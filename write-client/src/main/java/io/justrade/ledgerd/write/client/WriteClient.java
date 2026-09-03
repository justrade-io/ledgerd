package io.justrade.ledgerd.write.client;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.justrade.ledgerd.protocol.CommandEnvelopeEncoder;
import io.justrade.ledgerd.protocol.CommandResultDecoder;
import io.justrade.ledgerd.protocol.CommandType;
import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.MessageHeaderEncoder;
import io.justrade.ledgerd.protocol.TransferBatchEncoder;
import io.justrade.ledgerd.protocol.TransferBatchResultDecoder;
import io.justrade.ledgerd.write.client.config.ClientConfig;
import org.HdrHistogram.Histogram;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;

/**
 * Edge-side client for the LEDGERD core. Adds, on top of a raw Aeron cluster client:
 *
 * <ul>
 *   <li>leader-change handling (resends in-flight commands to the new leader);
 *   <li>idempotent retry that reuses the original command id, which is the
 *       precondition for the core's dedup guarantee;
 *   <li>asynchronous request/response correlation by command id;
 *   <li>explicit backpressure signalling to the caller (never a silent drop);
 *   <li>end-to-end latency measurement via HdrHistogram.
 * </ul>
 *
 * <p>Not thread-safe: {@link #submit} and {@link #poll} must be called from the
 * same thread. Steady-state submission is allocation-free (pending commands are
 * pooled).
 */
public final class WriteClient implements EgressListener, AutoCloseable {

    private static final float LOAD_FACTOR = 0.65f;

    private final ClientConfig config;
    private final ResultHandler handler;
    private final AeronCluster cluster;
    private final MediaDriver ownMediaDriver;
    private final NanoClock nanoClock;
    private final RetryPolicy retryPolicy;

    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final CommandEnvelopeEncoder envelopeEncoder = new CommandEnvelopeEncoder();
    private final TransferBatchEncoder transferBatchEncoder = new TransferBatchEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final CommandResultDecoder resultDecoder = new CommandResultDecoder();
    private final TransferBatchResultDecoder batchResultDecoder = new TransferBatchResultDecoder();

    private final Long2ObjectHashMap<PendingCommand> pending;
    private final PendingCommand[] pool;
    private final int[] freeStack;
    private int freeTop;

    private final Long2ObjectHashMap<PendingBatchCommand> pendingBatch;
    private final PendingBatchCommand[] batchPool;
    private final int[] batchFreeStack;
    private int batchFreeTop;

    private BatchResultHandler batchHandler = BatchResultHandler.NOOP;

    private final Histogram latencyHistogram = new Histogram(3_600_000_000_000L, 3);

    private long nextClientSeq;
    private long nextCommandIdLo = 1L;
    private long nextBatchIdLo = 1L;

    private long submitted;
    private long completed;
    private long expired;
    private long backpressureEvents;
    private int leaderChanges;
    private int leaderMemberId = -1;
    private boolean retransmitAll;

    public WriteClient(final ClientConfig config, final ResultHandler handler) {
        this(config, handler, SystemNanoClock.INSTANCE);
    }

    WriteClient(final ClientConfig config, final ResultHandler handler, final NanoClock nanoClock) {
        this.config = config;
        this.handler = handler;
        this.nanoClock = nanoClock;
        this.retryPolicy = new RetryPolicy(config.maxRetries());
        // Size the pending map so it never rehashes while the in-flight window
        // (bounded by maxInFlight) is populated: capacity * load factor must
        // cover maxInFlight entries.
        this.pending = new Long2ObjectHashMap<>(Math.max(16, config.maxInFlight() * 2), LOAD_FACTOR);
        this.pool = new PendingCommand[config.maxInFlight()];
        this.freeStack = new int[config.maxInFlight()];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new PendingCommand(i);
            freeStack[i] = pool.length - 1 - i;
        }
        this.freeTop = pool.length;

        this.pendingBatch = new Long2ObjectHashMap<>(Math.max(16, config.maxBatchInFlight() * 2), LOAD_FACTOR);
        final int batchBufferLength = MessageHeaderEncoder.ENCODED_LENGTH
                + TransferBatchEncoder.BLOCK_LENGTH
                + TransferBatchEncoder.LegsEncoder.HEADER_SIZE
                + TransferBatchEncoder.LegsEncoder.sbeBlockLength() * config.maxBatchSize();
        this.batchPool = new PendingBatchCommand[config.maxBatchInFlight()];
        this.batchFreeStack = new int[config.maxBatchInFlight()];
        for (int i = 0; i < batchPool.length; i++) {
            batchPool[i] = new PendingBatchCommand(i, batchBufferLength);
            batchFreeStack[i] = batchPool.length - 1 - i;
        }
        this.batchFreeTop = batchPool.length;

        MediaDriver embedded = null;
        String aeronDir = config.aeronDirectoryName();
        if (aeronDir == null) {
            embedded = MediaDriver.launchEmbedded(new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true));
            aeronDir = embedded.aeronDirectoryName();
        }
        this.ownMediaDriver = embedded;

        try {
            this.cluster = AeronCluster.connect(new AeronCluster.Context()
                    .egressListener(this)
                    .aeronDirectoryName(aeronDir)
                    .ingressChannel("aeron:udp")
                    .egressChannel(config.egressChannel())
                    .messageTimeoutNs(config.messageTimeoutNs())
                    .ingressEndpoints(config.ingressEndpoints()));
        } catch (final RuntimeException e) {
            if (embedded != null) {
                embedded.close();
            }
            throw e;
        }
    }

    /**
     * Submits a command on the default asset ({@code 0}). Convenience overload of
     * {@link #submit(CommandType, long, long, long, long, long)}.
     */
    public long submit(
            final CommandType type, final long accountA, final long accountB, final long accountC, final long amount) {
        return submit(type, 0L, accountA, accountB, accountC, amount);
    }

    /**
     * Encodes and submits a command, returning its low command-id word for
     * correlation. The command is retried automatically (reusing the same id)
     * until acknowledged, on timeout or leader change.
     *
     * @throws BackpressureException if the in-flight window is full; the caller
     *     must poll and retry rather than have the command silently dropped.
     */
    public long submit(
            final CommandType type,
            final long assetId,
            final long accountA,
            final long accountB,
            final long accountC,
            final long amount) {

        if (freeTop == 0) {
            backpressureEvents++;
            throw new BackpressureException("in-flight window full: " + config.maxInFlight());
        }

        final PendingCommand pc = pool[freeStack[--freeTop]];
        pc.inUse = true;
        pc.retries = 0;
        pc.commandIdHi = config.clientId();
        pc.commandIdLo = nextCommandIdLo++;

        envelopeEncoder
                .wrapAndApplyHeader(pc.buffer, 0, headerEncoder)
                .clientId(config.clientId())
                .clientSeq(nextClientSeq++)
                .commandIdHi(pc.commandIdHi)
                .commandIdLo(pc.commandIdLo)
                .commandType(type)
                .accountA(accountA)
                .accountB(accountB)
                .amount(amount)
                .correlationId(CommandEnvelopeEncoder.correlationIdNullValue())
                .accountC(accountC)
                .assetId(assetId);

        pc.length = MessageHeaderEncoder.ENCODED_LENGTH + envelopeEncoder.encodedLength();
        pc.submitNanos = nanoClock.nanoTime();
        pc.deadlineNanos = pc.submitNanos + config.retryBackoffNs();

        pending.put(pc.commandIdLo, pc);
        submitted++;
        offer(pc);
        return pc.commandIdLo;
    }

    /**
     * Encodes and submits a transfer batch, returning its low batch-id word for
     * correlation. The batch is retried automatically (reusing the same id) until
     * acknowledged, on timeout or leader change.
     *
     * @throws BackpressureException if the batch in-flight window is full
     * @throws IllegalArgumentException if {@code legs.length} exceeds
     *     {@code ClientConfig.maxBatchSize()}
     */
    public long submitTransferBatch(final TransferLeg[] legs) {
        if (legs.length > config.maxBatchSize()) {
            throw new IllegalArgumentException("batch legs exceed " + config.maxBatchSize() + ": " + legs.length);
        }
        if (batchFreeTop == 0) {
            backpressureEvents++;
            throw new BackpressureException("batch in-flight window full: " + config.maxBatchInFlight());
        }

        final PendingBatchCommand pc = batchPool[batchFreeStack[--batchFreeTop]];
        pc.inUse = true;
        pc.retries = 0;
        pc.batchIdHi = config.clientId();
        pc.batchIdLo = nextBatchIdLo++;

        transferBatchEncoder
                .wrapAndApplyHeader(pc.buffer, 0, headerEncoder)
                .clientId(config.clientId())
                .clientSeq(nextClientSeq++)
                .batchIdHi(pc.batchIdHi)
                .batchIdLo(pc.batchIdLo);
        final TransferBatchEncoder.LegsEncoder legsEncoder = transferBatchEncoder.legsCount(legs.length);
        for (final TransferLeg leg : legs) {
            legsEncoder
                    .next()
                    .fromId(leg.fromId())
                    .toId(leg.toId())
                    .amount(leg.amount())
                    .assetId(leg.assetId())
                    .linked(leg.linked() ? (short) 1 : (short) 0);
        }

        pc.length = MessageHeaderEncoder.ENCODED_LENGTH + transferBatchEncoder.encodedLength();
        pc.submitNanos = nanoClock.nanoTime();
        pc.deadlineNanos = pc.submitNanos + config.retryBackoffNs();

        pendingBatch.put(pc.batchIdLo, pc);
        submitted++;
        offerBatch(pc);
        return pc.batchIdLo;
    }

    /** Registers the callback that receives transfer-batch results. */
    public void setBatchResultHandler(final BatchResultHandler handler) {
        this.batchHandler = handler == null ? BatchResultHandler.NOOP : handler;
    }

    /**
     * Drives egress delivery and time-based retransmission. Call in a loop.
     *
     * @return an opaque work count (positive when progress was made).
     */
    public int poll() {
        int work = cluster.pollEgress();
        final long now = nanoClock.nanoTime();

        // Scan the preallocated pool rather than the map's value iterator so a
        // poll neither allocates nor risks concurrent modification when a result
        // callback recycles an entry mid-scan.
        for (int i = 0; i < pool.length; i++) {
            final PendingCommand pc = pool[i];
            switch (retryPolicy.evaluate(now, pc.deadlineNanos, pc.retries, retransmitAll, pc.inUse)) {
                case WAIT -> {
                    // not in use, or still within its backoff window
                }
                case EXPIRE -> expire(pc);
                case RETRY -> {
                    offer(pc);
                    pc.retries++;
                    pc.deadlineNanos = now + config.retryBackoffNs();
                    work++;
                }
            }
        }
        for (int i = 0; i < batchPool.length; i++) {
            final PendingBatchCommand pc = batchPool[i];
            switch (retryPolicy.evaluate(now, pc.deadlineNanos, pc.retries, retransmitAll, pc.inUse)) {
                case WAIT -> {
                    // not in use, or still within its backoff window
                }
                case EXPIRE -> expireBatch(pc);
                case RETRY -> {
                    offerBatch(pc);
                    pc.retries++;
                    pc.deadlineNanos = now + config.retryBackoffNs();
                    work++;
                }
            }
        }
        retransmitAll = false;
        return work;
    }

    private void expire(final PendingCommand pc) {
        pending.remove(pc.commandIdLo);
        handler.onExpired(pc.commandIdHi, pc.commandIdLo);
        expired++;
        release(pc);
    }

    private void expireBatch(final PendingBatchCommand pc) {
        pendingBatch.remove(pc.batchIdLo);
        batchHandler.onBatchExpired(pc.batchIdHi, pc.batchIdLo);
        expired++;
        releaseBatch(pc);
    }

    private void offer(final PendingCommand pc) {
        final long result = cluster.offer(pc.buffer, 0, pc.length);
        if (result < 0) {
            backpressureEvents++;
        }
    }

    private void offerBatch(final PendingBatchCommand pc) {
        final long result = cluster.offer(pc.buffer, 0, pc.length);
        if (result < 0) {
            backpressureEvents++;
        }
    }

    private void release(final PendingCommand pc) {
        pc.reset();
        freeStack[freeTop++] = pc.poolIndex;
    }

    private void releaseBatch(final PendingBatchCommand pc) {
        pc.reset();
        batchFreeStack[batchFreeTop++] = pc.poolIndex;
    }

    @Override
    public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {
        headerDecoder.wrap(buffer, offset);
        final int templateId = headerDecoder.templateId();
        final int bodyOffset = offset + MessageHeaderDecoder.ENCODED_LENGTH;

        if (templateId == CommandResultDecoder.TEMPLATE_ID) {
            resultDecoder.wrap(buffer, bodyOffset, headerDecoder.blockLength(), headerDecoder.version());

            final long commandIdLo = resultDecoder.commandIdLo();
            final PendingCommand pc = pending.remove(commandIdLo);
            final boolean hasBalance = resultDecoder.resultBalance() != CommandResultDecoder.resultBalanceNullValue();
            final boolean hasAllowance =
                    resultDecoder.resultAllowance() != CommandResultDecoder.resultAllowanceNullValue();

            if (pc != null) {
                final long elapsedNs = nanoClock.nanoTime() - pc.submitNanos;
                // Clamp so a result arriving after a long outage cannot throw out
                // of the poll loop (Histogram rejects values above highestTrackableValue).
                latencyHistogram.recordValue(Math.min(elapsedNs, latencyHistogram.getHighestTrackableValue()));
                release(pc);
                completed++;
            }

            handler.onResult(
                    resultDecoder.commandIdHi(),
                    commandIdLo,
                    resultDecoder.status(),
                    resultDecoder.resultBalance(),
                    hasBalance,
                    resultDecoder.resultAllowance(),
                    hasAllowance);
            return;
        }

        if (templateId == TransferBatchResultDecoder.TEMPLATE_ID) {
            batchResultDecoder.wrap(buffer, bodyOffset, headerDecoder.blockLength(), headerDecoder.version());

            final long batchIdLo = batchResultDecoder.batchIdLo();
            final PendingBatchCommand pc = pendingBatch.remove(batchIdLo);
            final TransferBatchResultDecoder.ResultsDecoder results = batchResultDecoder.results();
            final int count = results.count();
            final TransferLegResult[] legResults = new TransferLegResult[count];
            for (int i = 0; i < count; i++) {
                results.next();
                legResults[i] =
                        new TransferLegResult(results.status(), results.hasBalance() != 0, results.resultBalance());
            }

            if (pc != null) {
                final long elapsedNs = nanoClock.nanoTime() - pc.submitNanos;
                latencyHistogram.recordValue(Math.min(elapsedNs, latencyHistogram.getHighestTrackableValue()));
                releaseBatch(pc);
                completed++;
            }

            batchHandler.onBatchResult(batchResultDecoder.batchIdHi(), batchIdLo, legResults);
            return;
        }
    }

    @Override
    public void onNewLeader(
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final String ingressEndpoints) {
        this.leaderMemberId = leaderMemberId;
        this.leaderChanges++;
        this.retransmitAll = true;
    }

    public int pendingCount() {
        return pending.size();
    }

    public long submitted() {
        return submitted;
    }

    public long completed() {
        return completed;
    }

    /** Commands abandoned after exhausting {@code maxRetries}; each was reported via {@link ResultHandler#onExpired}. */
    public long expired() {
        return expired;
    }

    public long backpressureEvents() {
        return backpressureEvents;
    }

    public int leaderChanges() {
        return leaderChanges;
    }

    public int leaderMemberId() {
        final int fromCluster = cluster.leaderMemberId();
        return fromCluster >= 0 ? fromCluster : leaderMemberId;
    }

    /** End-to-end latency (submit to result) in nanoseconds; read from the poll thread. */
    public Histogram latencyHistogram() {
        return latencyHistogram;
    }

    @Override
    public void close() {
        cluster.close();
        if (ownMediaDriver != null) {
            ownMediaDriver.close();
        }
    }
}
