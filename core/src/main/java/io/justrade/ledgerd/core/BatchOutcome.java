package io.justrade.ledgerd.core;

import io.justrade.ledgerd.protocol.EventCause;
import io.justrade.ledgerd.protocol.StatusCode;

/**
 * Mutable, reusable holder for the outcome of a {@code TransferBatch}: one
 * result per transfer leg plus the semantic domain events produced by the legs
 * that committed (ADR 0011).
 *
 * <p>A single instance is owned by the engine and reset before each batch, so
 * the batch hot path allocates nothing. Per-leg results and staged events live
 * in preallocated arrays sized by {@code maxBatchSize}; a transfer leg emits at
 * most three events (two balance changes and one transfer edge), matching
 * {@link CommandOutcome}'s recording but at batch scale.
 *
 * <p>Events for a linked chain are staged as the chain applies and are truncated
 * back to the chain's starting count when a later leg fails, so a rolled-back
 * chain never leaks events to the journal.
 */
public final class BatchOutcome {

    /** Number of events a single transfer leg emits. */
    public static final int EVENTS_PER_LEG = 3;

    private final int maxBatchSize;
    private final int maxEvents;

    private final byte[] statusValues;
    private final byte[] hasBalance;
    private final long[] resultBalance;
    private int legCount;

    private final CommandOutcome.EventKind[] eventKind;
    private final long[] eventAsset;
    private final long[] eventAccountA;
    private final long[] eventAccountB;
    private final long[] eventValueA;
    private final long[] eventValueB;
    private final EventCause[] eventCause;
    private int eventCount;

    public BatchOutcome(final int maxBatchSize) {
        this.maxBatchSize = maxBatchSize;
        this.maxEvents = maxBatchSize * EVENTS_PER_LEG;
        this.statusValues = new byte[maxBatchSize];
        this.hasBalance = new byte[maxBatchSize];
        this.resultBalance = new long[maxBatchSize];
        this.eventKind = new CommandOutcome.EventKind[maxEvents];
        this.eventAsset = new long[maxEvents];
        this.eventAccountA = new long[maxEvents];
        this.eventAccountB = new long[maxEvents];
        this.eventValueA = new long[maxEvents];
        this.eventValueB = new long[maxEvents];
        this.eventCause = new EventCause[maxEvents];
    }

    /** Clears the batch result for the next dispatch; {@code legCount} entries are written. */
    public void reset(final int legCount) {
        this.legCount = legCount;
        this.eventCount = 0;
    }

    public int legCount() {
        return legCount;
    }

    public void setLeg(final int index, final StatusCode status, final boolean balancePresent, final long balance) {
        statusValues[index] = (byte) status.value();
        hasBalance[index] = balancePresent ? (byte) 1 : (byte) 0;
        resultBalance[index] = balance;
    }

    public StatusCode legStatus(final int index) {
        return StatusCode.get(statusValues[index]);
    }

    public boolean legHasBalance(final int index) {
        return hasBalance[index] != 0;
    }

    public long legResultBalance(final int index) {
        return resultBalance[index];
    }

    /** Records the three domain events of one committed transfer leg. */
    public void addTransferEvents(
            final long assetId,
            final long from,
            final long to,
            final long fromNewBalance,
            final long toNewBalance,
            final long amount) {
        int e = eventCount;
        eventKind[e] = CommandOutcome.EventKind.BALANCE_CHANGED;
        eventAsset[e] = assetId;
        eventAccountA[e] = from;
        eventAccountB[e] = 0L;
        eventValueA[e] = fromNewBalance;
        eventValueB[e] = -amount;
        eventCause[e] = EventCause.TRANSFER_DEBIT;
        e++;

        eventKind[e] = CommandOutcome.EventKind.BALANCE_CHANGED;
        eventAsset[e] = assetId;
        eventAccountA[e] = to;
        eventAccountB[e] = 0L;
        eventValueA[e] = toNewBalance;
        eventValueB[e] = amount;
        eventCause[e] = EventCause.TRANSFER_CREDIT;
        e++;

        eventKind[e] = CommandOutcome.EventKind.TRANSFER;
        eventAsset[e] = assetId;
        eventAccountA[e] = from;
        eventAccountB[e] = to;
        eventValueA[e] = amount;
        eventValueB[e] = 0L;
        eventCause[e] = null;
        e++;

        eventCount = e;
    }

    /** Discards staged events back to {@code count}, rolling back a failed chain. */
    public void truncateEvents(final int count) {
        eventCount = count;
    }

    /** Number of domain events staged for the legs committed so far. */
    public int eventCount() {
        return eventCount;
    }

    /** The staged event at {@code index}; valid only for {@code index < eventCount()}. */
    public CommandOutcome.EventKind eventKind(final int index) {
        return eventKind[index];
    }

    public long eventAsset(final int index) {
        return eventAsset[index];
    }

    public long eventAccountA(final int index) {
        return eventAccountA[index];
    }

    public long eventAccountB(final int index) {
        return eventAccountB[index];
    }

    public long eventValueA(final int index) {
        return eventValueA[index];
    }

    public long eventValueB(final int index) {
        return eventValueB[index];
    }

    public EventCause eventCause(final int index) {
        return eventCause[index];
    }

    public int maxBatchSize() {
        return maxBatchSize;
    }

    // Package-private raw views so the engine can hand the per-leg arrays to the
    // batch dedup table without copying or allocating.
    byte[] statusValues() {
        return statusValues;
    }

    byte[] hasBalanceFlags() {
        return hasBalance;
    }

    long[] resultBalances() {
        return resultBalance;
    }
}
