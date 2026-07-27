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
    private long lastSnapshotWriteNanos;
    private long lastSnapshotReadNanos;

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

    public void snapshotWriteNanos(final long nanos) {
        this.lastSnapshotWriteNanos = nanos;
    }

    public void snapshotReadNanos(final long nanos) {
        this.lastSnapshotReadNanos = nanos;
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

    public long lastSnapshotWriteNanos() {
        return lastSnapshotWriteNanos;
    }

    public long lastSnapshotReadNanos() {
        return lastSnapshotReadNanos;
    }
}
