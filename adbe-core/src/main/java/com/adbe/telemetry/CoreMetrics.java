package com.adbe.telemetry;

import com.adbe.telemetry.CounterSink.Counter;

/**
 * Single-writer counters for core observability. Owned and mutated only by the
 * clustered-service thread, so plain fields suffice; cross-thread readers should
 * treat values as eventually consistent snapshots.
 *
 * <p>Each increment is also mirrored to a {@link CounterSink}, allowing the
 * cluster to expose the same values off-heap for external readers without
 * disturbing the single-writer hot path. The default sink is a no-op.
 *
 * <p>No string formatting or allocation occurs here, honouring the hot-path
 * logging rules: the hot path only increments counters.
 */
public final class CoreMetrics {

    private final CounterSink sink;

    private long commandsProcessed;
    private long duplicatesDetected;
    private long insufficientBalance;
    private long insufficientAllowance;
    private long invalidAccount;
    private long overflow;
    private long invalidAmount;
    private long backpressureEvents;
    private long leaderElections;
    private long dedupEvicted;
    private long eventJournalOverflow;
    private long lastSnapshotWriteNanos;
    private long lastSnapshotReadNanos;
    private long balanceCount;
    private long allowanceOwnerCount;
    private long dedupClientCount;

    /** Creates metrics that only maintain in-heap counters (tests, raw engine). */
    public CoreMetrics() {
        this(CounterSink.NOOP);
    }

    /** Creates metrics that also mirror counters to {@code sink}. */
    public CoreMetrics(final CounterSink sink) {
        this.sink = sink;
    }

    public void onCommandProcessed() {
        commandsProcessed++;
        sink.increment(Counter.COMMANDS_PROCESSED);
    }

    public void onDuplicate() {
        duplicatesDetected++;
        sink.increment(Counter.DUPLICATES_DETECTED);
    }

    public void onInsufficientBalance() {
        insufficientBalance++;
        sink.increment(Counter.INSUFFICIENT_BALANCE);
    }

    public void onInsufficientAllowance() {
        insufficientAllowance++;
        sink.increment(Counter.INSUFFICIENT_ALLOWANCE);
    }

    public void onInvalidAccount() {
        invalidAccount++;
        sink.increment(Counter.INVALID_ACCOUNT);
    }

    public void onOverflow() {
        overflow++;
        sink.increment(Counter.OVERFLOW);
    }

    public void onInvalidAmount() {
        invalidAmount++;
        sink.increment(Counter.INVALID_AMOUNT);
    }

    public void onBackpressure() {
        backpressureEvents++;
        sink.increment(Counter.BACKPRESSURE_EVENTS);
    }

    public void onLeaderElection() {
        leaderElections++;
        sink.increment(Counter.LEADER_ELECTIONS);
    }

    // A fresh command whose bounded-window slot still held a different, older
    // sequence: that older sequence's dedup record is now gone, so a late retry
    // of it would re-apply. A rising count means the dedup window is too small.
    public void onDedupEvicted() {
        dedupEvicted++;
        sink.increment(Counter.DEDUP_EVICTED);
    }

    // The event-journal ring was full when the service thread tried to write a
    // domain event; the record was not enqueued. A rising count means the
    // journaler cannot keep up and the ring must be sized larger (ADR 0011).
    public void onEventJournalOverflow() {
        eventJournalOverflow++;
        sink.increment(Counter.EVENT_JOURNAL_OVERFLOW);
    }

    public void snapshotWriteNanos(final long nanos) {
        this.lastSnapshotWriteNanos = nanos;
        sink.set(CounterSink.Gauge.SNAPSHOT_WRITE_NANOS, nanos);
    }

    public void snapshotReadNanos(final long nanos) {
        this.lastSnapshotReadNanos = nanos;
        sink.set(CounterSink.Gauge.SNAPSHOT_READ_NANOS, nanos);
    }

    /** Publishes the current balance map size as a gauge. */
    public void balanceCount(final long count) {
        this.balanceCount = count;
        sink.set(CounterSink.Gauge.BALANCE_COUNT, count);
    }

    /** Publishes the current allowance owner count as a gauge. */
    public void allowanceOwnerCount(final long count) {
        this.allowanceOwnerCount = count;
        sink.set(CounterSink.Gauge.ALLOWANCE_OWNER_COUNT, count);
    }

    /** Publishes the current dedup client count as a gauge. */
    public void dedupClientCount(final long count) {
        this.dedupClientCount = count;
        sink.set(CounterSink.Gauge.DEDUP_CLIENT_COUNT, count);
    }

    public long commandsProcessed() {
        return commandsProcessed;
    }

    public long duplicatesDetected() {
        return duplicatesDetected;
    }

    public long insufficientBalance() {
        return insufficientBalance;
    }

    public long insufficientAllowance() {
        return insufficientAllowance;
    }

    public long invalidAccount() {
        return invalidAccount;
    }

    public long overflow() {
        return overflow;
    }

    public long invalidAmount() {
        return invalidAmount;
    }

    public long backpressureEvents() {
        return backpressureEvents;
    }

    public long leaderElections() {
        return leaderElections;
    }

    public long dedupEvicted() {
        return dedupEvicted;
    }

    public long eventJournalOverflow() {
        return eventJournalOverflow;
    }

    public long lastSnapshotWriteNanos() {
        return lastSnapshotWriteNanos;
    }

    public long lastSnapshotReadNanos() {
        return lastSnapshotReadNanos;
    }

    public long balanceCount() {
        return balanceCount;
    }

    public long allowanceOwnerCount() {
        return allowanceOwnerCount;
    }

    public long dedupClientCount() {
        return dedupClientCount;
    }
}
