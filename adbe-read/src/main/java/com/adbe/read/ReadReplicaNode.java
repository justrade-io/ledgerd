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
 * connects to a cluster member's Aeron Archive, follows the committed consensus
 * log, loads service snapshots as they appear, and serves reads over HTTP. It is
 * never listed in {@code clusterMembers} and does not vote or affect quorum.
 *
 * <p>Single-writer: a single Agrona {@link Agent} thread (driven by an
 * {@link AgentRunner}) owns the engine. That one thread drains HTTP queries from
 * the {@link ReadQueryGateway}, polls the live log, and loads new snapshots.
 * Because reads and writes to the engine's non-thread-safe stores always happen
 * on this one thread, no concurrency control is needed and readers always see a
 * consistent state.
 *
 * <p>Multi-archive failover (ADR 0008): the node is configured with an ordered
 * list of Archive endpoints (one per cluster member) held by an
 * {@link ArchiveSource}. Every member records the committed consensus log to its
 * own Archive, so the node follows the first reachable endpoint and, on an
 * Archive error or a live-log liveness timeout, fails over to the next endpoint
 * (round-robin with backoff) while keeping its engine state. On switching source
 * the live log restarts from the last loaded snapshot position; the engine's
 * command-id dedup makes re-applying the already-seen prefix idempotent, so state
 * converges with no clobber. Snapshots are loaded ADVANCE-ONLY: a service
 * snapshot is applied only when its cluster-global {@code logPosition} advances
 * the replica's current position, so an older snapshot on a new source never
 * rolls state back. {@code /healthz} reports ok / stale and {@code /metrics}
 * exposes a failover counter so an orchestrator can detect a degraded replica.
 * See ADR 0006, 0007, and 0008.
 */
public final class ReadReplicaNode implements AutoCloseable {

    private static final int FRAGMENT_LIMIT = 64;
    private static final int REQUEST_DRAIN_LIMIT = 64;
    private static final long STARTUP_CONNECT_TIMEOUT_MS = 10_000L;
    private static final long LIVE_LOG_RECONNECT_BACKOFF_MS = 250L;
    private static final System.Logger LOG = System.getLogger(ReadReplicaNode.class.getName());

    private final MediaDriver mediaDriver;
    private final Aeron aeron;
    private final ArchiveSource source;
    private final BalanceEngine engine;
    private final CoreMetrics metrics;
    private final ReadQueryGateway gateway;
    private final QueryHttpServer httpServer;
    private final ReadReplicaConfig replicaConfig;
    private final SnapshotManager snapshotManager;
    private final ReplicationHealth health;
    private final UnsafeBuffer responseBuffer;
    private final MessageHandler queryHandler;
    private final FragmentHandler snapshotFragmentHandler;
    private final MessageHeaderDecoder validationHeader = new MessageHeaderDecoder();
    private final SnapshotHeaderDecoder snapshotHeaderDecoder = new SnapshotHeaderDecoder();
    private final long[] snapshotCandidates = new long[64];

    private final AgentRunner agentRunner;

    // Agent-thread-owned state (never touched from any other thread).
    private State state = State.CONNECTING;
    private long maxSnapshotRecordingId = -1L;
    private long appliedLogPosition;
    private long nextSnapshotPollMs;
    private long nextConnectAttemptMs;
    private long nextLiveLogConnectMs;
    private long lastActivityMs;
    private LiveLogSubscriber liveLog;
    private int candidateCount;
    private SnapshotDecision snapshotDecision = SnapshotDecision.PENDING;

    /** Replication state machine (ADR 0008). */
    private enum State {
        CONNECTING,
        FOLLOWING,
        DEGRADED
    }

    /** Outcome of inspecting a candidate snapshot recording's header. */
    private enum SnapshotDecision {
        PENDING,
        LOADING,
        SKIP
    }

    /** Result of a snapshot replay attempt. */
    private enum SnapshotResult {
        LOADED,
        SKIPPED,
        CORRUPT,
        FAILED
    }

    public ReadReplicaNode(
            final ReadReplicaConfig replicaConfig, final CoreConfig coreConfig, final ReadServiceConfig readConfig) {

        this.replicaConfig = replicaConfig;
        this.snapshotManager = new SnapshotManager();
        this.responseBuffer = new UnsafeBuffer(new byte[QueryCodec.maxMessageLength()]);
        this.queryHandler = this::onQuery;
        this.health = new ReplicationHealth();

        this.metrics = newMetrics();
        this.engine = new BalanceEngine(coreConfig, metrics);
        this.gateway = new ReadQueryGateway(readConfig.requestRingCapacity(), readConfig.responseRingCapacity());

        // The service snapshot recording is prefixed with cluster-schema framing
        // records (schema 111); the ADBE SnapshotHeader (ADBE schema, template 10)
        // follows them. Skip the framing, then apply the snapshot ADVANCE-ONLY:
        // peek the header's cluster-global logPosition and begin the load (which
        // clears the engine) only when it advances the current position, so an
        // older snapshot on a failover source never clobbers newer state.
        this.snapshotFragmentHandler =
                (final DirectBuffer buffer, final int offset, final int length, final io.aeron.logbuffer.Header h) -> {
                    if (snapshotDecision == SnapshotDecision.LOADING) {
                        snapshotManager.onRecord(buffer, offset);
                        return;
                    }
                    if (snapshotDecision != SnapshotDecision.PENDING) {
                        return; // SKIP: this recording does not advance state
                    }
                    validationHeader.wrap(buffer, offset);
                    if (validationHeader.schemaId() != MessageHeaderDecoder.SCHEMA_ID
                            || validationHeader.templateId() != SnapshotHeaderDecoder.TEMPLATE_ID) {
                        return; // cluster-schema framing; keep scanning
                    }
                    snapshotHeaderDecoder.wrap(
                            buffer,
                            offset + MessageHeaderDecoder.ENCODED_LENGTH,
                            validationHeader.blockLength(),
                            validationHeader.version());
                    if (snapshotHeaderDecoder.logPosition() <= appliedLogPosition) {
                        snapshotDecision = SnapshotDecision.SKIP;
                        return;
                    }
                    engine.beginSnapshotLoad(snapshotManager);
                    snapshotDecision = SnapshotDecision.LOADING;
                    snapshotManager.onRecord(buffer, offset);
                };

        final String aeronDir = replicaConfig.aeronDir();
        MediaDriver driver = null;
        Aeron aeronClient = null;
        QueryHttpServer server = null;
        AgentRunner runner = null;
        try {
            driver = MediaDriver.launch(new MediaDriver.Context()
                    .aeronDirectoryName(aeronDir)
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
            aeronClient = Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDir));
            server = new QueryHttpServer(gateway, readConfig, health);
            runner = new AgentRunner(new BackoffIdleStrategy(), this::onAgentError, null, new Agent() {
                @Override
                public int doWork() {
                    return doWorkCycle();
                }

                @Override
                public String roleName() {
                    return "adbe-read-replica-agent";
                }
            });

            this.mediaDriver = driver;
            this.aeron = aeronClient;
            this.source = new ArchiveSource(aeronClient, replicaConfig);
            this.httpServer = server;
            this.agentRunner = runner;
        } catch (final RuntimeException e) {
            // Do not leak the driver, client, HTTP server, or gateway dispatcher
            // thread if a later component fails to start.
            closeQuietly(runner, server, aeronClient, driver);
            gateway.close();
            throw e;
        }

        final Thread agentThread = new Thread(agentRunner, "adbe-read-replica-agent");
        agentThread.setDaemon(true);
        agentThread.start();

        awaitInitialConnect();
    }

    /**
     * Blocks (bounded) until the agent reports healthy, so a reachable cluster is
     * already being followed when the constructor returns. When no endpoint is
     * reachable the wait times out and the agent keeps retrying in the background.
     */
    private void awaitInitialConnect() {
        final long deadline = System.currentTimeMillis() + STARTUP_CONNECT_TIMEOUT_MS;
        while (!health.isHealthy() && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10L);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private static void closeQuietly(final AutoCloseable... resources) {
        for (final AutoCloseable resource : resources) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (final Exception ignored) {
                    // Best-effort cleanup after a failed startup.
                }
            }
        }
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
        int work = gateway.readRequests(queryHandler, REQUEST_DRAIN_LIMIT);
        switch (state) {
            case CONNECTING, DEGRADED -> work += attemptConnect();
            case FOLLOWING -> work += followCycle();
        }
        return work;
    }

    private void onAgentError(final Throwable throwable) {
        // Non-fatal: the AgentRunner keeps the agent running after an error, so a
        // transient failure (e.g. a replay hiccup) does not stop query serving.
        LOG.log(System.Logger.Level.ERROR, "Read replica agent error", throwable);
    }

    /** Tries to (re)connect to the current candidate Archive endpoint. */
    private int attemptConnect() {
        final long now = System.currentTimeMillis();
        if (now < nextConnectAttemptMs) {
            return 0;
        }
        if (source.connect()) {
            state = State.FOLLOWING;
            lastActivityMs = now;
            // A new source has its own recording ids (not comparable across
            // Archives); rescan its snapshots. The advance-only logPosition guard
            // is what protects state, not the recording id.
            maxSnapshotRecordingId = -1L;
            nextSnapshotPollMs = 0L;
            restartLiveLog();
            health.markHealthy(source.activeChannel(), appliedLogPosition);
            LOG.log(System.Logger.Level.INFO, "Read replica connected to archive: {0}", source.activeChannel());
            return 1;
        }
        // Endpoint unreachable: advance round-robin and back off.
        source.advance();
        nextConnectAttemptMs = now + replicaConfig.failoverBackoffMs();
        state = State.DEGRADED;
        health.markStale(source.activeChannel(), appliedLogPosition);
        return 0;
    }

    /** Follows the live log and polls for newer snapshots on the active source. */
    private int followCycle() {
        final long now = System.currentTimeMillis();
        int work = 0;
        try {
            work += pollLiveLog();
        } catch (final RuntimeException e) {
            LOG.log(System.Logger.Level.ERROR, "Read replica live log failed: {0}", e.getMessage());
            failover();
            return work;
        }

        if (now >= nextSnapshotPollMs) {
            try {
                work += pollForNewSnapshot();
                lastActivityMs = System.currentTimeMillis(); // a successful archive op
            } catch (final RuntimeException e) {
                // A control-session failure here means the source is dead; fail over
                // rather than retrying the same broken endpoint.
                LOG.log(System.Logger.Level.ERROR, "Read replica snapshot poll failed: {0}", e.getMessage());
                failover();
                return work;
            }
            nextSnapshotPollMs = System.currentTimeMillis() + replicaConfig.pollIntervalMs();
        }

        // Liveness backstop for a silently dead Archive (no exception, no
        // fragments). A successful snapshot poll counts as activity, so an idle
        // but healthy cluster does not false-positive.
        if (System.currentTimeMillis() - lastActivityMs > replicaConfig.liveLogLivenessTimeoutMs()) {
            LOG.log(System.Logger.Level.WARNING, "Read replica live log liveness timeout; failing over");
            failover();
            return work;
        }

        health.markHealthy(source.activeChannel(), appliedLogPosition);
        return work;
    }

    /** Drops the dead source and schedules a reconnect to the next endpoint. */
    private void failover() {
        source.advance();
        if (liveLog != null) {
            liveLog.close();
            liveLog = null;
        }
        health.recordFailover();
        state = State.DEGRADED;
        nextConnectAttemptMs = System.currentTimeMillis() + replicaConfig.failoverBackoffMs();
        health.markStale(source.activeChannel(), appliedLogPosition);
        LOG.log(
                System.Logger.Level.WARNING,
                "Read replica failing over to next archive: {0} (failovers={1})",
                source.activeChannel(),
                health.failovers());
    }

    private int pollLiveLog() {
        ensureLiveLog();
        if (liveLog == null) {
            return 0;
        }
        final int fragments = liveLog.poll(FRAGMENT_LIMIT);
        if (fragments > 0) {
            lastActivityMs = System.currentTimeMillis();
            final long position = liveLog.lastPosition();
            if (position > appliedLogPosition) {
                appliedLogPosition = position;
            }
        }
        if (liveLog.isReplayEnded()) {
            // The bounded replay ran to the end of the (momentarily idle) recording
            // and its image closed. Drop it and re-point a fresh replay from the
            // consumed position after a short backoff, so commits that land later
            // are still followed instead of being missed.
            liveLog.close();
            liveLog = null;
            nextLiveLogConnectMs = System.currentTimeMillis() + LIVE_LOG_RECONNECT_BACKOFF_MS;
        }
        return fragments;
    }

    private int pollForNewSnapshot() {
        final int count = collectSnapshotCandidates();
        // Candidates are sorted ascending by recording id; try newest first.
        for (int i = count - 1; i >= 0; i--) {
            final long recordingId = snapshotCandidates[i];
            if (recordingId <= maxSnapshotRecordingId) {
                continue; // already examined on this source
            }
            final SnapshotResult result = replaySnapshot(recordingId);
            if (result == SnapshotResult.LOADED) {
                maxSnapshotRecordingId = recordingId;
                final long loadedPosition = snapshotManager.loadedLogPosition();
                if (loadedPosition > appliedLogPosition) {
                    appliedLogPosition = loadedPosition;
                }
                restartLiveLog();
                return 1;
            }
            if (result == SnapshotResult.SKIPPED) {
                // Does not advance state; within a source snapshots advance
                // monotonically, so no older recording will either.
                maxSnapshotRecordingId = recordingId;
                return 0;
            }
            if (result == SnapshotResult.CORRUPT) {
                // Integrity check failed: skip this recording permanently on this
                // source and rebuild engine state from the start of the log.
                maxSnapshotRecordingId = recordingId;
                appliedLogPosition = 0L;
                health.recordIntegrityFailure();
                restartLiveLog();
                return 0;
            }
            // FAILED: transient; retry the newest recording on the next poll.
            return 0;
        }
        return 0;
    }

    private void restartLiveLog() {
        if (liveLog != null) {
            liveLog.close();
            liveLog = null;
        }
        // Reconnect immediately on the next cycle (a snapshot load or a failover
        // just moved the start position); the replay-ended backoff does not apply.
        nextLiveLogConnectMs = 0L;
        // Recreated lazily by ensureLiveLog() from the updated appliedLogPosition.
    }

    private void ensureLiveLog() {
        if (liveLog != null
                || !replicaConfig.liveLogEnabled()
                || !source.isConnected()
                || System.currentTimeMillis() < nextLiveLogConnectMs) {
            return;
        }
        // Follow from the position the engine has already applied up to. On a fresh
        // replica this is 0 (the start of the consensus log), so state builds
        // immediately; after a snapshot load or a failover it is the cluster-global
        // position consumed so far, which starts the replay at (or near) the live
        // head of the new source so it follows the active recording. Positions are
        // cluster-global (ADR 0008 fact A/C), so the boundary is valid on any
        // member's Archive; engine dedup keeps any re-delivered prefix idempotent.
        final LiveLogSubscriber subscriber =
                new LiveLogSubscriber(source.archive(), engine, appliedLogPosition, replicaConfig.localHost());
        if (subscriber.connect()) {
            liveLog = subscriber;
        } else {
            subscriber.close();
        }
    }

    private int collectSnapshotCandidates() {
        candidateCount = 0;

        // Collect the snapshot-stream recordings, newest first. The service
        // snapshot recording is prefixed with cluster-schema framing, which the
        // replay handler skips to reach the ADBE SnapshotHeader; the separate
        // consensus-module snapshot lives on a different stream and is not listed
        // here.
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

        source.archive().listRecordings(0L, 200, consumer);
        Arrays.sort(snapshotCandidates, 0, candidateCount);
        return candidateCount;
    }

    private SnapshotResult replaySnapshot(final long recordingId) {
        // The read replica runs its own media driver, so the replay must travel over
        // UDP (IPC is scoped to a single driver). Bind a subscription on an
        // ephemeral port, resolve the actual port, then tell the archive to
        // replay to that concrete endpoint.
        snapshotDecision = SnapshotDecision.PENDING;

        final Subscription subscription = aeron.addSubscription(
                "aeron:udp?endpoint=" + replicaConfig.localHost() + ":0", ReadStreams.SNAPSHOT_REPLAY);
        try {
            final String replayChannel = "aeron:udp?endpoint=" + awaitResolvedEndpoint(subscription);
            source.archive()
                    .startReplay(recordingId, 0, AeronArchive.NULL_LENGTH, replayChannel, ReadStreams.SNAPSHOT_REPLAY);

            final long deadline = System.currentTimeMillis() + replicaConfig.replayTimeoutMs();
            while (System.currentTimeMillis() < deadline) {
                if (snapshotDecision == SnapshotDecision.SKIP) {
                    return SnapshotResult.SKIPPED;
                }
                if (snapshotDecision == SnapshotDecision.LOADING && snapshotManager.loadComplete()) {
                    break;
                }
                final int fragments = subscription.poll(snapshotFragmentHandler, FRAGMENT_LIMIT);
                if (fragments == 0) {
                    Thread.onSpinWait();
                }
            }

            if (snapshotDecision == SnapshotDecision.SKIP) {
                return SnapshotResult.SKIPPED;
            }
            if (snapshotDecision != SnapshotDecision.LOADING || !snapshotManager.loadComplete()) {
                throw new IllegalStateException("Snapshot replay timed out for recording " + recordingId);
            }
        } catch (final RuntimeException e) {
            // Not a loadable service snapshot or a transient replay failure; leave
            // current state intact and retry on a later poll.
            return SnapshotResult.FAILED;
        } finally {
            subscription.close();
        }

        // A replayed snapshot that does not reconcile (sum(balances) != totalSupply)
        // must not be served; discard the partial load and rebuild from the log.
        if (!snapshotManager.verifyInvariant()) {
            engine.clearState();
            LOG.log(
                    System.Logger.Level.WARNING,
                    "Read replica snapshot integrity check failed: recordingId={0}",
                    recordingId);
            return SnapshotResult.CORRUPT;
        }

        engine.publishSizeGauges();
        LOG.log(
                System.Logger.Level.INFO,
                "Read replica snapshot loaded: recordingId={0} logPosition={1} balances={2}",
                recordingId,
                snapshotManager.loadedLogPosition(),
                engine.balances().size());
        return SnapshotResult.LOADED;
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
        source.close();
        aeron.close();
        mediaDriver.close();
        gateway.close();
    }
}
