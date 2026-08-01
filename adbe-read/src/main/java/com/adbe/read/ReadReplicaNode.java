package com.adbe.read;

import com.adbe.collections.AllowanceStore;
import com.adbe.collections.BalanceStore;
import com.adbe.config.CoreConfig;
import com.adbe.core.BalanceEngine;
import com.adbe.persistence.SnapshotManager;
import com.adbe.protocol.MessageHeaderDecoder;
import com.adbe.protocol.SnapshotHeaderDecoder;
import com.adbe.read.config.ReadReplicaConfig;
import com.adbe.read.config.ReadServiceConfig;
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
import java.util.Arrays;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.MessageHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

/**
 * A read node that operates independently of the Raft consensus protocol. It
 * connects to a cluster member's Aeron Archive, follows the consensus log, loads
 * service snapshots as they appear, and serves reads over HTTP. It is never
 * listed in {@code clusterMembers} and does not vote or affect quorum.
 *
 * <p>Single-writer: a single Agrona {@link Agent} thread (driven by an
 * {@link AgentRunner}) owns the engine. That one thread drains HTTP queries from
 * the {@link ReadQueryGateway}, polls the live log, and loads new snapshots.
 * Because reads and writes to the engine's non-thread-safe stores always happen
 * on this one thread, no concurrency control is needed and readers always see a
 * consistent state.
 *
 * <p>The live log is followed from the last loaded snapshot position, or from
 * the start of the consensus log (position 0) when no snapshot has been loaded
 * yet, so the engine builds state immediately on a fresh cluster. Engine dedup
 * keeps re-application idempotent across a later snapshot load. Snapshot
 * staleness is bounded by the poll interval; a snapshot is loaded only when a
 * newer snapshot recording appears, so the engine is not clobbered on every poll
 * and live-log progress is preserved between snapshots. See ADR 0006 and 0007.
 */
public final class ReadReplicaNode implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 64;
    private static final int REQUEST_DRAIN_LIMIT = 64;

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final AeronArchive archive;
    private final BalanceEngine engine;
    private final CoreMetrics metrics;
    private final ReadQueryGateway gateway;
    private final QueryHttpServer httpServer;
    private final ReadReplicaConfig replicaConfig;
    private final SnapshotManager snapshotManager;
    private final UnsafeBuffer responseBuffer;
    private final MessageHandler queryHandler;
    private final FragmentHandler snapshotFragmentHandler;
    private final MessageHeaderDecoder validationHeader = new MessageHeaderDecoder();
    private final long[] snapshotCandidates = new long[64];

    private final AgentRunner agentRunner;

    // Agent-thread-owned state (never touched from any other thread).
    private long currentSnapshotRecordingId = -1L;
    private long snapshotLogPosition;
    private long nextSnapshotPollMs;
    private LiveLogSubscriber liveLog;
    private int candidateCount;
    private boolean snapshotLoadBegun;
    private boolean snapshotRejected;

    public ReadReplicaNode(
            final ReadReplicaConfig replicaConfig, final CoreConfig coreConfig, final ReadServiceConfig readConfig) {

        this.replicaConfig = replicaConfig;
        this.snapshotManager = new SnapshotManager();
        this.responseBuffer = new UnsafeBuffer(new byte[QueryCodec.maxMessageLength()]);
        this.queryHandler = this::onQuery;

        this.metrics = newMetrics();
        this.engine = new BalanceEngine(coreConfig, metrics);
        this.gateway = new ReadQueryGateway(readConfig.requestRingCapacity(), readConfig.responseRingCapacity());

        // Validates that the first record is an ADBE SnapshotHeader before the
        // engine is cleared, so a consensus-module snapshot recording (which
        // shares the snapshot stream but carries cluster-schema records) is
        // rejected without disturbing the current state.
        this.snapshotFragmentHandler =
                (final DirectBuffer buffer, final int offset, final int length, final io.aeron.logbuffer.Header h) -> {
                    if (!snapshotLoadBegun) {
                        validationHeader.wrap(buffer, offset);
                        if (validationHeader.templateId() != SnapshotHeaderDecoder.TEMPLATE_ID) {
                            snapshotRejected = true;
                            return;
                        }
                        engine.beginSnapshotLoad(snapshotManager);
                        snapshotLoadBegun = true;
                    }
                    snapshotManager.onRecord(buffer, offset);
                };

        final String aeronDir = "build/read-replica/driver";
        this.mediaDriver = MediaDriver.launch(new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
        this.aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));

        this.archive = AeronArchive.connect(new AeronArchive.Context()
                .aeron(aeron)
                .controlRequestChannel(replicaConfig.archiveControlChannel())
                .controlResponseChannel("aeron:udp?endpoint=" + replicaConfig.localHost() + ":0")
                .controlRequestStreamId(replicaConfig.archiveControlStreamId()));

        this.httpServer = new QueryHttpServer(gateway, readConfig);

        this.agentRunner = new AgentRunner(new BackoffIdleStrategy(), this::onAgentError, null, new Agent() {
            @Override
            public int doWork() {
                return doWorkCycle();
            }

            @Override
            public String roleName() {
                return "adbe-read-replica-agent";
            }
        });
        final Thread agentThread = new Thread(agentRunner, "adbe-read-replica-agent");
        agentThread.setDaemon(true);
        agentThread.start();
    }

    /** The bound HTTP port (useful when a port of 0 was requested in tests). */
    public int httpPort() {
        return httpServer.port();
    }

    public ReadQueryGateway gateway() {
        return gateway;
    }

    // --- Single-writer agent loop ----------------------------------------

    private int doWorkCycle() {
        int work = 0;
        work += gateway.readRequests(queryHandler, REQUEST_DRAIN_LIMIT);
        work += pollLiveLog();
        if (System.currentTimeMillis() >= nextSnapshotPollMs) {
            try {
                work += pollForNewSnapshot();
            } catch (final Exception e) {
                // A failed replay must not kill query serving; retry on the next poll.
                System.err.println("Read replica snapshot poll failed: " + e.getMessage());
            }
            nextSnapshotPollMs = System.currentTimeMillis() + replicaConfig.pollIntervalMs();
        }
        return work;
    }

    private void onAgentError(final Throwable throwable) {
        // Non-fatal: the AgentRunner keeps the agent running after an error, so a
        // transient failure (e.g. a replay hiccup) does not stop query serving.
        System.err.println("Read replica agent error: " + throwable);
    }

    private int pollLiveLog() {
        ensureLiveLog();
        if (liveLog == null) {
            return 0;
        }
        return liveLog.poll(FRAGMENT_LIMIT);
    }

    private int pollForNewSnapshot() {
        final int count = collectSnapshotCandidates();
        // Candidates are sorted ascending by recording id; try newest first.
        for (int i = count - 1; i >= 0; i--) {
            final long recordingId = snapshotCandidates[i];
            if (recordingId == currentSnapshotRecordingId) {
                continue;
            }
            if (tryLoadSnapshot(recordingId)) {
                currentSnapshotRecordingId = recordingId;
                snapshotLogPosition = snapshotManager.loadedLogPosition();
                restartLiveLog();
                return 1;
            }
        }
        return 0;
    }

    private boolean tryLoadSnapshot(final long recordingId) {
        try {
            replaySnapshot(recordingId);
            return true;
        } catch (final RuntimeException e) {
            // Not a loadable service snapshot (e.g. the consensus-module snapshot
            // recording) or a transient replay failure; leave current state intact.
            return false;
        }
    }

    private void restartLiveLog() {
        if (liveLog != null) {
            liveLog.close();
            liveLog = null;
        }
        // Recreated lazily by ensureLiveLog() from the updated snapshotLogPosition.
    }

    private void ensureLiveLog() {
        if (liveLog != null || !replicaConfig.liveLogEnabled()) {
            return;
        }
        // Follow from the last loaded snapshot position, or from the start of the
        // consensus log (position 0) when no snapshot has been loaded yet, so the
        // engine builds state immediately on a fresh cluster. Engine dedup keeps
        // re-application idempotent across a later snapshot load.
        final LiveLogSubscriber subscriber =
                new LiveLogSubscriber(archive, engine, snapshotLogPosition, replicaConfig.localHost());
        if (subscriber.connect()) {
            liveLog = subscriber;
        } else {
            subscriber.close();
        }
    }

    private int collectSnapshotCandidates() {
        candidateCount = 0;

        // A snapshot trigger produces two recordings on the snapshot stream: the
        // consensus-module snapshot (cluster-schema records) and the service
        // snapshot (ADBE snapshot records). Collect them all and let the caller
        // validate which one is a loadable service snapshot.
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
                    if (streamId == replicaConfig.snapshotStreamId() && candidateCount < snapshotCandidates.length) {
                        snapshotCandidates[candidateCount++] = recordingId;
                    }
                };

        archive.listRecordings(0L, 200, consumer);
        Arrays.sort(snapshotCandidates, 0, candidateCount);
        return candidateCount;
    }

    private void replaySnapshot(final long recordingId) {
        final int replayStreamId = 42;

        // The read replica runs its own media driver, so the replay must travel over
        // UDP (IPC is scoped to a single driver). Bind a subscription on an
        // ephemeral port, resolve the actual port, then tell the archive to
        // replay to that concrete endpoint.
        snapshotLoadBegun = false;
        snapshotRejected = false;

        final Subscription subscription =
                aeron.addSubscription("aeron:udp?endpoint=" + replicaConfig.localHost() + ":0", replayStreamId);
        try {
            final String replayChannel = "aeron:udp?endpoint=" + awaitResolvedEndpoint(subscription);
            archive.startReplay(recordingId, 0, AeronArchive.NULL_LENGTH, replayChannel, replayStreamId);

            final long deadline = System.currentTimeMillis() + replicaConfig.replayTimeoutMs();
            while (!snapshotRejected && !snapshotManager.loadComplete() && System.currentTimeMillis() < deadline) {
                final int fragments = subscription.poll(snapshotFragmentHandler, FRAGMENT_LIMIT);
                if (fragments == 0) {
                    Thread.onSpinWait();
                }
            }

            if (snapshotRejected) {
                throw new IllegalStateException("Recording " + recordingId + " is not a service snapshot");
            }
            if (!snapshotManager.loadComplete()) {
                throw new IllegalStateException("Snapshot replay timed out for recording " + recordingId);
            }
        } finally {
            subscription.close();
        }

        engine.publishSizeGauges();
        System.out.printf(
                "Read replica snapshot loaded: recordingId=%d balances=%d%n",
                recordingId, engine.balances().size());
    }

    private String awaitResolvedEndpoint(final Subscription subscription) {
        final long deadline = System.currentTimeMillis() + replicaConfig.replayTimeoutMs();
        while (System.currentTimeMillis() < deadline) {
            final String endpoint = subscription.resolvedEndpoint();
            if (endpoint != null) {
                return endpoint;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("Timed out resolving replay subscription endpoint");
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
            counters[i] = cm.newCounter("adbe.read.replica.counter." + i, CounterSink.TYPE_COUNTER);
        }
        for (int i = 0; i < gaugeCount; i++) {
            gauges[i] = cm.newCounter("adbe.read.replica.gauge." + i, CounterSink.TYPE_GAUGE);
        }
        return new CoreMetrics(new AtomicCounterSink(counters, gauges));
    }

    // --- Query handling (agent thread) -----------------

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
        agentRunner.close();

        if (liveLog != null) {
            liveLog.close();
            liveLog = null;
        }

        httpServer.close();
        archive.close();
        aeron.close();
        mediaDriver.close();
        gateway.close();
    }
}
