package io.justrade.ledgerd.read.client;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.justrade.ledgerd.protocol.MessageHeaderDecoder;
import io.justrade.ledgerd.protocol.MessageHeaderEncoder;
import io.justrade.ledgerd.protocol.QueryRequestEncoder;
import io.justrade.ledgerd.protocol.QueryResponseDecoder;
import io.justrade.ledgerd.protocol.QueryStatusCode;
import io.justrade.ledgerd.protocol.QueryType;
import io.justrade.ledgerd.read.client.config.ReadClientConfig;
import java.util.ArrayList;
import java.util.List;
import org.agrona.DirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * The read-side SDK: queries a running read replica's {@code QueryResponder}
 * over plain Aeron request/response streams. Consumes only the {@code protocol}
 * wire contract, never {@code core} or {@code read}, mirroring how
 * {@code WriteClient} stays decoupled from the engine.
 *
 * <p>Two API modes share one core:
 *
 * <ul>
 *   <li><b>Asynchronous</b>: {@code submit...} returns a {@code requestId}
 *       without blocking (throwing {@link BackpressureException} when the
 *       in-flight window is full); {@link #poll()} drives delivery and fires the
 *       registered {@link QueryListener}; unanswered queries are re-published
 *       idempotently (same request id) until answered or the retry budget is
 *       exhausted, at which point {@code onTimeout} fires.
 *   <li><b>Synchronous</b>: the {@code balance(...)}-style methods submit and
 *       block (driving {@link #poll()} themselves) until the matching response
 *       arrives or {@code messageTimeoutNs} elapses, throwing
 *       {@link QueryTimeoutException}.
 * </ul>
 *
 * <p>Queries are reads, so retries simply re-publish the same request id; a
 * response to an abandoned attempt is discarded. Results are eventually
 * consistent and each carries the replica's {@code appliedPosition} at answer
 * time.
 *
 * <p>Not thread-safe: query methods and {@link #poll()} must be called from a
 * single thread; listener callbacks run on that same thread.
 */
public final class ReadClient implements AutoCloseable {

    private static final float LOAD_FACTOR = 0.65f;
    private static final int RESPONSE_FRAGMENT_LIMIT = 64;
    private static final int REQUEST_BUFFER_CAPACITY = 16 * 1024;
    private static final int MAX_BATCH_ACCOUNTS = 512;
    private static final long[] EMPTY_ACCOUNTS = new long[0];
    private static final long RESOLVE_ENDPOINT_TIMEOUT_MS = 10_000L;
    // Size the socket buffers above the largest term buffer so response bursts never overflow the OS socket.
    private static final int SOCKET_RCVBUF_LENGTH = 16 * 1024 * 1024;
    private static final int SOCKET_SNDBUF_LENGTH = 16 * 1024 * 1024;

    private final ReadClientConfig config;
    private final NanoClock nanoClock;
    private final RetryPolicy retryPolicy;
    private final MediaDriver ownMediaDriver;
    private final Aeron aeron;
    private final Subscription responses;
    private final Publication requests;
    private final FragmentAssembler fragmentAssembler;
    private final String responseChannel;
    private final IdleStrategy idle = new BackoffIdleStrategy();

    private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
    private final QueryRequestEncoder requestEncoder = new QueryRequestEncoder();
    private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
    private final QueryResponseDecoder responseDecoder = new QueryResponseDecoder();

    private final Long2ObjectHashMap<PendingQuery> pending;
    private final PendingQuery[] pool;
    private final int[] freeStack;
    private int freeTop;

    private QueryListener listener = QueryListener.NONE;
    private long nextRequestId = 1L;
    private long submitted;
    private long completed;
    private long expired;
    private long backpressureEvents;
    private long lastOfferResult;
    private long lastAppliedPosition;

    // Single-threaded stack of sync delivery frames. A stack (not a single slot)
    // lets a listener callback issue a nested synchronous query without
    // overwriting the outer await's frame.
    private static final int MAX_SYNC_NESTING = 8;
    private final SyncFrame[] syncStack = new SyncFrame[MAX_SYNC_NESTING];
    private int syncDepth;

    /**
     * @param config query endpoints and timing; a {@code null}
     *     aeronDirectoryName launches an embedded media driver
     */
    public ReadClient(final ReadClientConfig config) {
        this(config, SystemNanoClock.INSTANCE);
    }

    /**
     * @param config query endpoints and timing; a {@code null}
     *     aeronDirectoryName launches an embedded media driver
     * @param nanoClock monotonic time source for retry and timeout budgets
     */
    public ReadClient(final ReadClientConfig config, final NanoClock nanoClock) {
        this.config = config;
        this.nanoClock = nanoClock;
        this.retryPolicy = new RetryPolicy(config.messageTimeoutNs(), config.maxRetries());
        this.pending = new Long2ObjectHashMap<>(Math.max(16, config.maxInFlight() * 2), LOAD_FACTOR);
        this.pool = new PendingQuery[config.maxInFlight()];
        this.freeStack = new int[config.maxInFlight()];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = new PendingQuery(i);
            freeStack[i] = pool.length - 1 - i;
        }
        this.freeTop = pool.length;

        MediaDriver embedded = null;
        String dir = config.aeronDirectoryName();
        if (dir == null) {
            embedded = MediaDriver.launchEmbedded(new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .dirDeleteOnStart(true)
                    .dirDeleteOnShutdown(true)
                    .socketRcvbufLength(SOCKET_RCVBUF_LENGTH)
                    .socketSndbufLength(SOCKET_SNDBUF_LENGTH));
            dir = embedded.aeronDirectoryName();
        }
        this.ownMediaDriver = embedded;
        Aeron aeronClient = null;
        Subscription sub = null;
        Publication pub = null;
        String channel = null;
        try {
            aeronClient = Aeron.connect(new Aeron.Context().aeronDirectoryName(dir));
            sub = aeronClient.addSubscription(config.responseChannel(), config.responseStreamId());
            channel = awaitResolvedEndpoint(sub);
            pub = aeronClient.addPublication(config.requestChannel(), config.requestStreamId());
        } catch (final RuntimeException e) {
            if (pub != null) {
                pub.close();
            }
            if (sub != null) {
                sub.close();
            }
            if (aeronClient != null) {
                aeronClient.close();
            }
            if (embedded != null) {
                embedded.close();
            }
            throw e;
        }
        this.aeron = aeronClient;
        this.responses = sub;
        this.requests = pub;
        this.fragmentAssembler = new FragmentAssembler(this::onResponse);
        this.responseChannel = channel;
    }

    /** Registers the asynchronous delivery listener; replaces any previous listener. */
    public void setListener(final QueryListener listener) {
        this.listener = listener == null ? QueryListener.NONE : listener;
    }

    // ============================ Asynchronous API ============================

    /** Submits a {@code BALANCE} query for {@code (assetId, accountId)}; returns its request id. */
    public long submitBalance(final long assetId, final long accountId) {
        return submit(QueryType.BALANCE, EMPTY_ACCOUNTS, encoder -> encoder.assetId(assetId)
                .accountId(accountId));
    }

    /** Submits a {@code BATCH_BALANCE} query for up to {@value #MAX_BATCH_ACCOUNTS} accounts. */
    public long submitBatchBalances(final long assetId, final long... accountIds) {
        if (accountIds.length > MAX_BATCH_ACCOUNTS) {
            throw new IllegalArgumentException(
                    "batch balance accounts exceed " + MAX_BATCH_ACCOUNTS + ": " + accountIds.length);
        }
        return submit(QueryType.BATCH_BALANCE, accountIds, encoder -> encoder.assetId(assetId));
    }

    /** Submits an {@code ALLOWANCE} query for {@code (assetId, ownerId, delegateId)}. */
    public long submitAllowance(final long assetId, final long ownerId, final long delegateId) {
        return submit(QueryType.ALLOWANCE, EMPTY_ACCOUNTS, encoder -> encoder.assetId(assetId)
                .ownerId(ownerId)
                .delegateId(delegateId));
    }

    /** Submits a {@code TOTAL_SUPPLY} query for {@code assetId}. */
    public long submitTotalSupply(final long assetId) {
        return submit(QueryType.TOTAL_SUPPLY, EMPTY_ACCOUNTS, encoder -> encoder.assetId(assetId));
    }

    /**
     * Drives response delivery, listener callbacks, and idempotent
     * retransmission of unanswered queries. Call in a loop.
     *
     * @return an opaque work count (positive when progress was made)
     */
    public int poll() {
        int work = responses.poll(fragmentAssembler, RESPONSE_FRAGMENT_LIMIT);
        work += retransmit(nanoClock.nanoTime());
        return work;
    }

    // ============================ Synchronous API ============================

    /** The balance of {@code (assetId, accountId)}; {@code found} is false when the account is unknown. */
    public BalanceResult balance(final long assetId, final long accountId) {
        return (BalanceResult) await(submitBalance(assetId, accountId));
    }

    /** The balances of up to {@value #MAX_BATCH_ACCOUNTS} accounts, in the requested order. */
    public List<BalanceResult> batchBalances(final long assetId, final long... accountIds) {
        return castBalances(await(submitBatchBalances(assetId, accountIds)));
    }

    /** The allowance for an {@code (assetId, ownerId, delegateId)} pair. */
    public AllowanceResult allowance(final long assetId, final long ownerId, final long delegateId) {
        return (AllowanceResult) await(submitAllowance(assetId, ownerId, delegateId));
    }

    /** The engine-wide total supply for {@code assetId}. */
    public TotalSupplyResult totalSupply(final long assetId) {
        return (TotalSupplyResult) await(submitTotalSupply(assetId));
    }

    // ============================ Stats and diagnostics ============================

    /** The cluster log position the read service had applied when answering the most recent query. */
    public long lastAppliedPosition() {
        return lastAppliedPosition;
    }

    /** Number of queries submitted. */
    public long submitted() {
        return submitted;
    }

    /** Number of queries answered. */
    public long completed() {
        return completed;
    }

    /** Number of queries expired on their retry budget. */
    public long expired() {
        return expired;
    }

    /** Number of times a submit hit the in-flight window or an offer was backpressured. */
    public long backpressureEvents() {
        return backpressureEvents;
    }

    /** The raw result of the most recent request offer (diagnostics). */
    public long lastOfferResult() {
        return lastOfferResult;
    }

    /** Number of queries currently awaiting a response. */
    public int pendingCount() {
        return pending.size();
    }

    // ============================ Core ============================

    @SuppressWarnings("unchecked")
    private static List<BalanceResult> castBalances(final Object value) {
        return (List<BalanceResult>) value;
    }

    @FunctionalInterface
    private interface RequestFiller {
        void fill(QueryRequestEncoder encoder);
    }

    private long submit(final QueryType type, final long[] accountIds, final RequestFiller filler) {
        if (freeTop == 0) {
            backpressureEvents++;
            throw new BackpressureException("query in-flight window full: " + config.maxInFlight());
        }
        final PendingQuery pq = pool[freeStack[--freeTop]];
        pq.inUse = true;
        pq.retries = 0;
        pq.requestId = nextRequestId();
        pq.type = type;
        pq.submittedNanos = nanoClock.nanoTime();

        requestEncoder
                .wrapAndApplyHeader(pq.buffer, 0, headerEncoder)
                .requestId(pq.requestId)
                .queryType(type)
                .assetId(QueryRequestEncoder.assetIdNullValue())
                .accountId(QueryRequestEncoder.accountIdNullValue())
                .ownerId(QueryRequestEncoder.ownerIdNullValue())
                .delegateId(QueryRequestEncoder.delegateIdNullValue())
                .responseStreamId(config.responseStreamId());
        filler.fill(requestEncoder);
        // The group is always encoded (even when empty) so the decoder can skip
        // it deterministically before reaching the varData block.
        final QueryRequestEncoder.AccountIdsEncoder ids = requestEncoder.accountIdsCount(accountIds.length);
        for (final long accountId : accountIds) {
            ids.next().accountId(accountId);
        }
        // varData is written last, preserving the SBE fixed -> group -> varData order.
        requestEncoder.responseChannel(responseChannel);
        pq.length = MessageHeaderEncoder.ENCODED_LENGTH + requestEncoder.encodedLength();
        pq.deadlineNanos = pq.submittedNanos + config.retryBackoffNs();

        pending.put(pq.requestId, pq);
        submitted++;
        offer(pq);
        return pq.requestId;
    }

    // Request id 0 is reserved (it never collides with a live id after a wrap,
    // since the id is always positive); wrap back to 1 rather than through 0.
    private long nextRequestId() {
        final long id = nextRequestId;
        nextRequestId = (nextRequestId == Long.MAX_VALUE) ? 1L : nextRequestId + 1L;
        return id;
    }

    private void offer(final PendingQuery pq) {
        lastOfferResult = requests.offer(pq.buffer, 0, pq.length);
        if (lastOfferResult < 0) {
            backpressureEvents++;
        }
    }

    private int retransmit(final long now) {
        if (freeTop == pool.length) {
            // Nothing in flight: the per-cycle pool scan would be pure waste.
            return 0;
        }
        int work = 0;
        for (int i = 0; i < pool.length; i++) {
            final PendingQuery pq = pool[i];
            // A delivering slot is mid-listener-callback (which may re-enter
            // poll via a nested synchronous query); it is released when the
            // callback returns, so it must not be expired here as well.
            final boolean ready = pq.inUse && !pq.delivering;
            switch (retryPolicy.evaluate(now, pq.deadlineNanos, pq.submittedNanos, pq.retries, ready)) {
                case WAIT -> {
                    // still within its backoff window; leave it in flight
                }
                case EXPIRE -> expire(pq);
                case RETRY -> {
                    offer(pq);
                    pq.retries++;
                    pq.deadlineNanos = now + config.retryBackoffNs();
                    work++;
                }
            }
        }
        return work;
    }

    private void expire(final PendingQuery pq) {
        pending.remove(pq.requestId);
        expired++;
        if (listener != QueryListener.NONE) {
            listener.onTimeout(pq.requestId, pq.type);
        }
        release(pq);
    }

    private void onResponse(final DirectBuffer buffer, final int offset, final int length, final Header header) {
        headerDecoder.wrap(buffer, offset);
        if (headerDecoder.schemaId() != MessageHeaderDecoder.SCHEMA_ID
                || headerDecoder.templateId() != QueryResponseDecoder.TEMPLATE_ID
                || length < MessageHeaderDecoder.ENCODED_LENGTH + headerDecoder.blockLength()) {
            return;
        }
        responseDecoder.wrap(
                buffer,
                offset + MessageHeaderDecoder.ENCODED_LENGTH,
                headerDecoder.blockLength(),
                headerDecoder.version());
        final long requestId = responseDecoder.requestId();
        final PendingQuery pq = pending.get(requestId);
        if (pq == null) {
            return; // Late response to an abandoned or already-completed query.
        }
        decodeResponse(pq);
        lastAppliedPosition = pq.appliedPosition;
        pending.remove(requestId);
        // The listener callback may re-enter poll() (a nested synchronous
        // query); the slot is off the pending map but not yet released, so mark
        // it delivering to keep a reentrant retransmit from expiring it and
        // releasing the pool slot twice.
        pq.delivering = true;
        final SyncFrame frame = syncFrameFor(requestId);
        if (pq.status == QueryStatusCode.UNSUPPORTED) {
            if (listener != QueryListener.NONE) {
                listener.onError(requestId, pq.type, pq.status);
            }
            if (frame != null) {
                frame.error = new QueryException(pq.status, "read service rejected query type: " + pq.type);
            }
        } else {
            deliver(pq);
            if (frame != null) {
                frame.value = pq.value;
                frame.delivered = true;
            }
        }
        release(pq);
        completed++;
    }

    private void deliver(final PendingQuery pq) {
        if (listener == QueryListener.NONE) {
            return;
        }
        switch (pq.type) {
            case BALANCE -> listener.onBalance(pq.requestId, (BalanceResult) pq.value);
            case BATCH_BALANCE -> listener.onBatchBalances(pq.requestId, castBalances(pq.value));
            case ALLOWANCE -> listener.onAllowance(pq.requestId, (AllowanceResult) pq.value);
            case TOTAL_SUPPLY -> listener.onTotalSupply(pq.requestId, (TotalSupplyResult) pq.value);
            default -> listener.onError(pq.requestId, pq.type, pq.status);
        }
    }

    /**
     * Blocks (driving {@link #poll()}) until the submitted query is delivered
     * or {@code messageTimeoutNs} elapses. The awaited request id is pushed on a
     * stack, so a listener callback that issues its own synchronous query during
     * {@link #poll()} does not corrupt this (outer) await.
     */
    @SuppressWarnings("unchecked")
    private <T> T await(final long requestId) {
        final SyncFrame frame = pushSyncFrame(requestId);
        final long deadline = nanoClock.nanoTime() + config.messageTimeoutNs();
        try {
            while (true) {
                poll();
                if (frame.error != null) {
                    final QueryException error = frame.error;
                    frame.error = null;
                    throw error;
                }
                if (frame.delivered) {
                    final T value = (T) frame.value;
                    frame.value = null;
                    frame.delivered = false;
                    return value;
                }
                if (nanoClock.nanoTime() >= deadline) {
                    // Release the abandoned query's window slot: a dead replica
                    // must not keep the slot occupied by a never-answered query.
                    cancel(requestId);
                    throw new QueryTimeoutException("no response for query requestId=" + requestId);
                }
                idle.idle(0);
            }
        } finally {
            popSyncFrame();
        }
    }

    private SyncFrame pushSyncFrame(final long requestId) {
        if (syncDepth == MAX_SYNC_NESTING) {
            throw new IllegalStateException("synchronous query nesting exceeds " + MAX_SYNC_NESTING);
        }
        SyncFrame frame = syncStack[syncDepth];
        if (frame == null) {
            frame = new SyncFrame();
            syncStack[syncDepth] = frame;
        }
        frame.requestId = requestId;
        frame.delivered = false;
        frame.value = null;
        frame.error = null;
        syncDepth++;
        return frame;
    }

    private void popSyncFrame() {
        syncDepth--;
    }

    private SyncFrame syncFrameFor(final long requestId) {
        if (syncDepth > 0 && syncStack[syncDepth - 1].requestId == requestId) {
            return syncStack[syncDepth - 1];
        }
        return null;
    }

    private void cancel(final long requestId) {
        final PendingQuery pq = pending.remove(requestId);
        if (pq != null) {
            release(pq);
        }
    }

    private void decodeResponse(final PendingQuery pq) {
        pq.status = responseDecoder.status();
        pq.appliedPosition = responseDecoder.appliedPosition();
        switch (responseDecoder.queryType()) {
            case BALANCE -> pq.value = new BalanceResult(
                    responseDecoder.accountId(),
                    responseDecoder.balance(),
                    responseDecoder.exists() != 0,
                    pq.appliedPosition);
            case BATCH_BALANCE -> pq.value = decodeBalances(pq);
            case ALLOWANCE -> pq.value = new AllowanceResult(
                    responseDecoder.ownerId(),
                    responseDecoder.delegateId(),
                    responseDecoder.allowance(),
                    pq.appliedPosition);
            case TOTAL_SUPPLY -> pq.value =
                    new TotalSupplyResult(responseDecoder.assetId(), responseDecoder.totalSupply(), pq.appliedPosition);
            default -> pq.status = QueryStatusCode.UNSUPPORTED;
        }
    }

    private List<BalanceResult> decodeBalances(final PendingQuery pq) {
        final ArrayList<BalanceResult> results = new ArrayList<>();
        final QueryResponseDecoder.BalancesDecoder balances = responseDecoder.balances();
        while (balances.hasNext()) {
            final QueryResponseDecoder.BalancesDecoder element = balances.next();
            results.add(new BalanceResult(
                    element.accountId(), element.balance(), element.exists() != 0, pq.appliedPosition));
        }
        return results;
    }

    private static String awaitResolvedEndpoint(final Subscription subscription) {
        final long deadline = System.currentTimeMillis() + RESOLVE_ENDPOINT_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            final String endpoint = subscription.resolvedEndpoint();
            if (endpoint != null) {
                return "aeron:udp?endpoint=" + endpoint;
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("timed out resolving the query response endpoint");
    }

    private void release(final PendingQuery pq) {
        pq.inUse = false;
        pq.delivering = false;
        pq.value = null;
        pq.status = QueryStatusCode.SUCCESS;
        pq.appliedPosition = 0L;
        pq.submittedNanos = 0L;
        freeStack[freeTop++] = pq.poolIndex;
    }

    @Override
    public void close() {
        requests.close();
        responses.close();
        aeron.close();
        if (ownMediaDriver != null) {
            ownMediaDriver.close();
        }
    }

    /** One frame of an in-progress synchronous await (see {@link #await}). */
    private static final class SyncFrame {
        long requestId;
        boolean delivered;
        Object value;
        QueryException error;
    }

    /** One in-flight query; pooled with a private request buffer so retransmits re-offer the same bytes. */
    private static final class PendingQuery {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[REQUEST_BUFFER_CAPACITY]);
        final int poolIndex;
        long requestId;
        QueryType type;
        int length;
        int retries;
        long deadlineNanos;
        long submittedNanos;
        boolean inUse;
        boolean delivering;
        Object value;
        QueryStatusCode status;
        long appliedPosition;

        PendingQuery(final int poolIndex) {
            this.poolIndex = poolIndex;
        }
    }
}
