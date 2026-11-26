package io.justrade.ledgerd.config;

import org.agrona.BitUtil;

/**
 * Immutable, preallocated capacity and tuning configuration for the core engine.
 *
 * <p>All capacities are validated to be powers of two so that hot-path index
 * arithmetic can use {@code & (capacity - 1)} instead of modulo.
 */
public final class CoreConfig {

    /**
     * How the service reacts when the domain event journal ring is full (ADR 0014).
     * Neither policy affects the deterministic state machine; they only change
     * whether a journal event is dropped.
     */
    public enum JournalOverflowPolicy {
        /** Drop the event and increment the overflow counter. Zero hot-path impact. */
        DROP,
        /**
         * Spin briefly to let the journaler drain, then drop as a fail-safe if the
         * ring is still full. Best-effort completeness at the cost of tail latency
         * under saturation; it never blocks consensus. Unconditional losslessness
         * (ingress backpressure) remains a deferred follow-up.
         */
        BLOCK
    }

    /** Default number of accounts preallocated in the balance map. */
    public static final int DEFAULT_ACCOUNT_CAPACITY = 1 << 20;

    /** Default number of allowance owners preallocated. */
    public static final int DEFAULT_ALLOWANCE_OWNER_CAPACITY = 1 << 16;

    /** Default per-owner delegate capacity. */
    public static final int DEFAULT_DELEGATE_CAPACITY = 1 << 4;

    /** Default number of distinct clients preallocated in the dedup table. */
    public static final int DEFAULT_DEDUP_CLIENT_CAPACITY = 1 << 16;

    /** Default dedup window size (most recent commands retained per client). */
    public static final int DEFAULT_DEDUP_WINDOW = 1 << 10;

    /** Default event-journal ring capacity in bytes (power of two, ADR 0011). */
    public static final int DEFAULT_EVENT_JOURNAL_CAPACITY = 1 << 20;

    /** Default maximum number of transfer legs accepted in one batch. */
    public static final int DEFAULT_MAX_BATCH_SIZE = 1 << 10;

    /** Default batch dedup window size (most recent batches retained per client). */
    public static final int DEFAULT_BATCH_DEDUP_WINDOW = 1 << 10;

    /**
     * Bytes a batch dedup ring spends per leg slot: one status byte, one
     * has-balance byte, and an eight-byte result balance (see {@code BatchDedupRing}).
     */
    public static final int BATCH_DEDUP_BYTES_PER_LEG = 10;

    /**
     * Per-client batch dedup footprint above which {@link #batchDedupSizingWarning()}
     * flags a likely misconfiguration (64 MiB). Guidance, not a hard limit.
     */
    public static final long BATCH_DEDUP_WARN_BYTES_PER_CLIENT = 64L << 20;

    private final int accountCapacity;
    private final int allowanceOwnerCapacity;
    private final int delegateCapacity;
    private final int dedupClientCapacity;
    private final int dedupWindow;
    private final boolean eventJournalEnabled;
    private final int eventJournalCapacity;
    private final int maxBatchSize;
    private final int batchDedupWindow;
    private final JournalOverflowPolicy journalOverflowPolicy;

    private CoreConfig(
            final int accountCapacity,
            final int allowanceOwnerCapacity,
            final int delegateCapacity,
            final int dedupClientCapacity,
            final int dedupWindow,
            final boolean eventJournalEnabled,
            final int eventJournalCapacity,
            final int maxBatchSize,
            final int batchDedupWindow,
            final JournalOverflowPolicy journalOverflowPolicy) {
        this.accountCapacity = requirePowerOfTwo(accountCapacity, "accountCapacity");
        this.allowanceOwnerCapacity = requirePowerOfTwo(allowanceOwnerCapacity, "allowanceOwnerCapacity");
        this.delegateCapacity = requirePowerOfTwo(delegateCapacity, "delegateCapacity");
        this.dedupClientCapacity = requirePowerOfTwo(dedupClientCapacity, "dedupClientCapacity");
        this.dedupWindow = requirePowerOfTwo(dedupWindow, "dedupWindow");
        this.eventJournalEnabled = eventJournalEnabled;
        this.eventJournalCapacity = requirePowerOfTwo(eventJournalCapacity, "eventJournalCapacity");
        this.maxBatchSize = requirePowerOfTwo(maxBatchSize, "maxBatchSize");
        this.batchDedupWindow = requirePowerOfTwo(batchDedupWindow, "batchDedupWindow");
        this.journalOverflowPolicy = journalOverflowPolicy == null ? JournalOverflowPolicy.DROP : journalOverflowPolicy;
    }

    /** Returns a configuration populated with the documented defaults. */
    public static CoreConfig defaults() {
        return new CoreConfig(
                DEFAULT_ACCOUNT_CAPACITY,
                DEFAULT_ALLOWANCE_OWNER_CAPACITY,
                DEFAULT_DELEGATE_CAPACITY,
                DEFAULT_DEDUP_CLIENT_CAPACITY,
                DEFAULT_DEDUP_WINDOW,
                false,
                DEFAULT_EVENT_JOURNAL_CAPACITY,
                DEFAULT_MAX_BATCH_SIZE,
                DEFAULT_BATCH_DEDUP_WINDOW,
                JournalOverflowPolicy.DROP);
    }

    /** Returns a configuration with explicit capacities; each must be a power of two. */
    public static CoreConfig of(
            final int accountCapacity,
            final int allowanceOwnerCapacity,
            final int delegateCapacity,
            final int dedupClientCapacity,
            final int dedupWindow) {
        return new CoreConfig(
                accountCapacity,
                allowanceOwnerCapacity,
                delegateCapacity,
                dedupClientCapacity,
                dedupWindow,
                false,
                DEFAULT_EVENT_JOURNAL_CAPACITY,
                DEFAULT_MAX_BATCH_SIZE,
                DEFAULT_BATCH_DEDUP_WINDOW,
                JournalOverflowPolicy.DROP);
    }

    /**
     * Returns a copy of this configuration with the domain event journal enabled
     * (ADR 0011), using a ring of {@code capacity} bytes (a power of two).
     */
    public CoreConfig withEventJournal(final int capacity) {
        return new CoreConfig(
                accountCapacity,
                allowanceOwnerCapacity,
                delegateCapacity,
                dedupClientCapacity,
                dedupWindow,
                true,
                capacity,
                maxBatchSize,
                batchDedupWindow,
                journalOverflowPolicy);
    }

    /**
     * Returns a copy of this configuration with the transfer-batch capacities set:
     * {@code maxBatchSize} legs per batch and a per-client batch dedup window of
     * {@code batchDedupWindow} (both powers of two).
     */
    public CoreConfig withBatch(final int maxBatchSize, final int batchDedupWindow) {
        return new CoreConfig(
                accountCapacity,
                allowanceOwnerCapacity,
                delegateCapacity,
                dedupClientCapacity,
                dedupWindow,
                eventJournalEnabled,
                eventJournalCapacity,
                maxBatchSize,
                batchDedupWindow,
                journalOverflowPolicy);
    }

    /**
     * Returns a copy of this configuration with the preallocated map capacities
     * set (each a power of two): per-asset account capacity, per-asset allowance
     * owner capacity, and the dedup client capacity. Used by the deploy knobs to
     * size the hot-path stores for a target account/asset workload.
     */
    public CoreConfig withCapacities(
            final int accountCapacity, final int allowanceOwnerCapacity, final int dedupClientCapacity) {
        return new CoreConfig(
                accountCapacity,
                allowanceOwnerCapacity,
                delegateCapacity,
                dedupClientCapacity,
                dedupWindow,
                eventJournalEnabled,
                eventJournalCapacity,
                maxBatchSize,
                batchDedupWindow,
                journalOverflowPolicy);
    }

    /**
     * Returns a copy of this configuration with the event-journal overflow policy
     * set (ADR 0014). The default is {@link JournalOverflowPolicy#DROP}.
     */
    public CoreConfig withJournalOverflowPolicy(final JournalOverflowPolicy policy) {
        return new CoreConfig(
                accountCapacity,
                allowanceOwnerCapacity,
                delegateCapacity,
                dedupClientCapacity,
                dedupWindow,
                eventJournalEnabled,
                eventJournalCapacity,
                maxBatchSize,
                batchDedupWindow,
                policy);
    }

    public JournalOverflowPolicy journalOverflowPolicy() {
        return journalOverflowPolicy;
    }

    public boolean eventJournalEnabled() {
        return eventJournalEnabled;
    }

    public int eventJournalCapacity() {
        return eventJournalCapacity;
    }

    public int accountCapacity() {
        return accountCapacity;
    }

    public int allowanceOwnerCapacity() {
        return allowanceOwnerCapacity;
    }

    public int delegateCapacity() {
        return delegateCapacity;
    }

    public int dedupClientCapacity() {
        return dedupClientCapacity;
    }

    public int dedupWindow() {
        return dedupWindow;
    }

    public int maxBatchSize() {
        return maxBatchSize;
    }

    public int batchDedupWindow() {
        return batchDedupWindow;
    }

    /**
     * Estimated memory a single client's batch dedup ring occupies:
     * {@code batchDedupWindow * maxBatchSize * }{@link #BATCH_DEDUP_BYTES_PER_LEG}.
     * One ring is allocated per client that submits batches.
     */
    public long batchDedupBytesPerClient() {
        return (long) batchDedupWindow * maxBatchSize * BATCH_DEDUP_BYTES_PER_LEG;
    }

    /**
     * Returns a sizing warning when the per-client batch dedup footprint exceeds
     * {@link #BATCH_DEDUP_WARN_BYTES_PER_CLIENT}, or {@code null} when it is within
     * budget. The dedup window only needs to cover the batches a client keeps
     * in flight, never {@code maxBatchSize}; equating the two multiplies the
     * per-client footprint and can exhaust a small heap (docs/CAPACITY-PLANNING.md).
     */
    public String batchDedupSizingWarning() {
        final long bytes = batchDedupBytesPerClient();
        if (bytes <= BATCH_DEDUP_WARN_BYTES_PER_CLIENT) {
            return null;
        }
        return "batch dedup footprint is ~" + (bytes >> 20) + " MiB per client (batchDedupWindow="
                + batchDedupWindow + ", maxBatchSize=" + maxBatchSize + "); size batchDedupWindow to the"
                + " number of batches a client keeps in flight, not maxBatchSize, to avoid exhausting the heap";
    }

    private static int requirePowerOfTwo(final int value, final String name) {
        if (value <= 0 || !BitUtil.isPowerOfTwo(value)) {
            throw new IllegalArgumentException(name + " must be a positive power of two, was: " + value);
        }
        return value;
    }
}
