package com.adbe.read;

import com.adbe.collections.AllowanceStore;
import com.adbe.collections.BalanceStore;
import com.adbe.config.CoreConfig;
import com.adbe.core.BalanceEngine;
import com.adbe.persistence.SnapshotManager;
import com.adbe.read.config.ReadServiceConfig;
import com.adbe.read.config.StandbyConfig;
import com.adbe.read.http.QueryHttpServer;
import com.adbe.read.query.QueryCodec;
import com.adbe.read.query.QueryType;
import com.adbe.read.query.ReadQueryGateway;
import com.adbe.telemetry.AtomicCounterSink;
import com.adbe.telemetry.CoreMetrics;
import com.adbe.telemetry.CounterSink;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import java.util.concurrent.atomic.AtomicReference;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

/**
 * A read node that operates independently of the Raft consensus protocol. It
 * connects to a cluster member's Aeron Archive, periodically downloads the
 * latest service snapshot, loads it into a {@link BalanceEngine}, and serves
 * reads over HTTP. It is never listed in {@code clusterMembers} and does not
 * vote or affect quorum.
 *
 * <p>Snapshot staleness is bounded by the poll interval; in steady state reads
 * reflect the most recent snapshot on the cluster Archive. Between snapshots
 * the standby state is frozen -- this is acceptable for the read use case
 * defined in ADR 0005.
 */
public final class StandbyReadNode implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 64;
    private static final int REQUEST_DRAIN_LIMIT = 64;

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final BalanceEngine engine;
    private final CoreMetrics metrics;
    private final ReadQueryGateway gateway;
    private final QueryHttpServer httpServer;
    private final StandbyConfig standbyConfig;
    private final SnapshotManager snapshotManager;
    private final AtomicReference<Thread> pollThread;
    private final AtomicReference<Thread> queryDrainer;
    private final UnsafeBuffer responseBuffer;
    private final MessageHandler queryHandler;
    private final AtomicReference<LiveLogSubscriber> liveLog;
    private volatile long snapshotLogPosition;

    private volatile boolean running = true;

    public StandbyReadNode(
            final StandbyConfig standbyConfig, final CoreConfig coreConfig, final ReadServiceConfig readConfig) {

        this.standbyConfig = standbyConfig;
        this.snapshotManager = new SnapshotManager();
        this.responseBuffer = new UnsafeBuffer(new byte[QueryCodec.maxMessageLength()]);
        this.queryHandler = this::onQuery;

        this.metrics = newMetrics();
        this.engine = new BalanceEngine(coreConfig, metrics);
        this.gateway = new ReadQueryGateway(readConfig.requestRingCapacity(), readConfig.responseRingCapacity());

        final String aeronDir = "build/standby-read/driver";
        this.mediaDriver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));

        this.archive = AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .controlRequestChannel(standbyConfig.archiveControlChannel())
                .controlResponseChannel("aeron:udp?endpoint=localhost:0")
                .controlRequestStreamId(standbyConfig.archiveControlStreamId()));

        loadLatestSnapshot();

        this.httpServer = new QueryHttpServer(gateway, readConfig);

        this.pollThread = new AtomicReference<>();
        this.queryDrainer = new AtomicReference<>();
        this.liveLog = new AtomicReference<>();
        startPolling();
        startQueryDrainer();
    }

    /** The bound HTTP port (useful when a port of 0 was requested in tests). */
    public int httpPort() {
        return httpServer.port();
    }

    public ReadQueryGateway gateway() {
        return gateway;
    }

    private void loadLatestSnapshot() {
        final long recordingId = findLatestSnapshotRecording();
        if (recordingId < 0) {
            return;
        }
        replaySnapshot(recordingId);
        snapshotLogPosition = snapshotManager.loadedLogPosition();

        if (standbyConfig.liveLogEnabled() && snapshotLogPosition > 0) {
            final LiveLogSubscriber subscriber = new LiveLogSubscriber(archive, engine, snapshotLogPosition);
            liveLog.set(subscriber);
            subscriber.start();
        }
    }

    private long findLatestSnapshotRecording() {
        final long[] latest = {-1L, -1L}; // recordingId, stopTimestamp

        final RecordingDescriptorConsumer consumer =
                (controlSessionId,
                        correlationId,
                        recordingId,
                        startTimestamp,
                        stopTimestamp,
                        startPosition,
                        stopPosition,
                        initialTermId,
                        segmentFileLength,
                        termBufferLength,
                        mtuLength,
                        sessionId,
                        streamId,
                        strippedChannel,
                        originalChannel,
                        sourceIdentity) -> {
                    if (streamId == standbyConfig.snapshotStreamId() && stopTimestamp > latest[1]) {
                        latest[0] = recordingId;
                        latest[1] = stopTimestamp;
                    }
                };

        archive.listRecordings(0L, 200, consumer);
        return latest[0];
    }

    private void replaySnapshot(final long recordingId) {
        final String replayChannel = "aeron:ipc?term-length=256k";
        final int replayStreamId = 42;
        final long replaySessionId =
                archive.startReplay(recordingId, 0, AeronArchive.NULL_LENGTH, replayChannel, replayStreamId);

        final Subscription subscription = aeron.addSubscription(replayChannel, replayStreamId);

        engine.beginSnapshotLoad(snapshotManager);

        final long deadline = System.currentTimeMillis() + standbyConfig.replayTimeoutMs();
        while (!snapshotManager.loadComplete() && System.currentTimeMillis() < deadline) {
            final int fragments = subscription.poll(fragmentHandler(), FRAGMENT_LIMIT);
            if (fragments == 0) {
                Thread.onSpinWait();
            }
        }

        subscription.close();

        if (!snapshotManager.loadComplete()) {
            throw new IllegalStateException("Snapshot replay timed out for recording " + recordingId);
        }

        engine.publishSizeGauges();
        System.out.printf(
                "Standby snapshot loaded: recordingId=%d balances=%d%n",
                recordingId, engine.balances().size());
    }

    private FragmentHandler fragmentHandler() {
        return (final DirectBuffer buffer, final int offset, final int length, final Header header) ->
                snapshotManager.onRecord(buffer, offset);
    }

    private static CoreMetrics newMetrics() {
        final int counterCount = CounterSink.Counter.COUNT;
        final int gaugeCount = CounterSink.Gauge.COUNT;
        final int totalSlots = counterCount + gaugeCount;
        final UnsafeBuffer valuesBuf = new UnsafeBuffer(BufferUtil.allocateDirectAligned(
                totalSlots * CountersManager.COUNTER_LENGTH, BitUtil.CACHE_LINE_LENGTH));
        final UnsafeBuffer metaBuf = new UnsafeBuffer(BufferUtil.allocateDirectAligned(
                totalSlots * CountersManager.METADATA_LENGTH, BitUtil.CACHE_LINE_LENGTH));
        final CountersManager cm = new CountersManager(metaBuf, valuesBuf);
        final AtomicCounter[] counters = new AtomicCounter[counterCount];
        final AtomicCounter[] gauges = new AtomicCounter[gaugeCount];
        for (int i = 0; i < counterCount; i++) {
            counters[i] = cm.newCounter("adbe.standby.counter." + i, CounterSink.TYPE_COUNTER);
        }
        for (int i = 0; i < gaugeCount; i++) {
            gauges[i] = cm.newCounter("adbe.standby.gauge." + i, CounterSink.TYPE_GAUGE);
        }
        return new CoreMetrics(new AtomicCounterSink(counters, gauges));
    }

    private void startPolling() {
        final Thread thread = new Thread(
                () -> {
                    while (running) {
                        try {
                            Thread.sleep(standbyConfig.pollIntervalMs());
                        } catch (final InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        if (!running) {
                            break;
                        }
                        try {
                            loadLatestSnapshot();
                        } catch (final Exception e) {
                            System.err.println("Standby snapshot poll failed: " + e.getMessage());
                        }
                    }
                },
                "adbe-standby-poll");
        thread.setDaemon(true);
        pollThread.set(thread);
        thread.start();
    }

    private void startQueryDrainer() {
        final Thread thread = new Thread(
                () -> {
                    while (running) {
                        gateway.readRequests(queryHandler, REQUEST_DRAIN_LIMIT);
                        Thread.onSpinWait();
                    }
                },
                "adbe-standby-query");
        thread.setDaemon(true);
        queryDrainer.set(thread);
        thread.start();
    }

    // --- Query handling (same logic as ReadModelService) -----------------

    private void onQuery(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length) {
        final long correlationId = QueryCodec.correlationId(buffer, index);
        final QueryType type = QueryCodec.queryType(buffer, index);
        switch (type) {
            case BALANCE -> answerBalances(correlationId, buffer, index, 1);
            case BATCH_BALANCE -> answerBalances(correlationId, buffer, index, QueryCodec.count(buffer, index));
            case ALLOWANCE -> answerAllowance(correlationId, buffer, index);
            case TOTAL_SUPPLY -> answerTotalSupply(correlationId);
        }
    }

    private void answerBalances(
            final long correlationId, final DirectBuffer request, final int reqIndex, final int accountCount) {
        final BalanceStore balances = engine.balances();
        QueryCodec.beginResponse(responseBuffer, correlationId, QueryType.BATCH_BALANCE, accountCount);
        for (int i = 0; i < accountCount; i++) {
            final long accountId = QueryCodec.operand(request, reqIndex, i);
            final long balance = balances.rawGet(accountId);
            final boolean exists = balance != BalanceStore.MISSING;
            QueryCodec.putEntry(responseBuffer, i, exists ? balance : 0L, exists);
        }
        gateway.offerResponse(responseBuffer, 0, QueryCodec.responseLength(accountCount));
    }

    private void answerAllowance(final long correlationId, final DirectBuffer request, final int reqIndex) {
        final AllowanceStore allowances = engine.allowances();
        final long ownerId = QueryCodec.operand(request, reqIndex, 0);
        final long delegateId = QueryCodec.operand(request, reqIndex, 1);
        final long allowance = allowances.get(ownerId, delegateId);
        QueryCodec.beginResponse(responseBuffer, correlationId, QueryType.ALLOWANCE, 1);
        QueryCodec.putEntry(responseBuffer, 0, allowance, true);
        gateway.offerResponse(responseBuffer, 0, QueryCodec.responseLength(1));
    }

    private void answerTotalSupply(final long correlationId) {
        final long supply = engine.balances().totalSupply();
        QueryCodec.beginResponse(responseBuffer, correlationId, QueryType.TOTAL_SUPPLY, 1);
        QueryCodec.putEntry(responseBuffer, 0, supply, true);
        gateway.offerResponse(responseBuffer, 0, QueryCodec.responseLength(1));
    }

    @Override
    public void close() {
        running = false;

        final LiveLogSubscriber sub = liveLog.getAndSet(null);
        if (sub != null) {
            sub.close();
        }

        final Thread qThread = queryDrainer.getAndSet(null);
        if (qThread != null) {
            qThread.interrupt();
            try {
                qThread.join(1000L);
            } catch (final InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }

        final Thread thread = pollThread.getAndSet(null);
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(1000L);
            } catch (final InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
        httpServer.close();
        archive.close();
        aeron.close();
        mediaDriver.close();
        gateway.close();
    }
}
