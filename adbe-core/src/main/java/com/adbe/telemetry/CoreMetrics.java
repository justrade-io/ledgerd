package com.adbe.telemetry;

/**
 * Single-writer counters for core observability. Owned and mutated only by the
 * clustered-service thread, so plain fields suffice; cross-thread readers should
 * treat values as eventually consistent snapshots.
 *
 * <p>No string formatting or allocation occurs here, honouring the hot-path
 * logging rules: the hot path only increments counters.
 */
public final class CoreMetrics {

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

    public void onCommandProcessed() {
        commandsProcessed++;
    }

    public void onDuplicate() {
        duplicatesDetected++;
    }

    public void onInsufficientBalance() {
        insufficientBalance++;
    }

    public void onInsufficientAllowance() {
        insufficientAllowance++;
    }

    public void onInvalidAccount() {
        invalidAccount++;
    }

    public void onOverflow() {
        overflow++;
    }

    public void onInvalidAmount() {
        invalidAmount++;
    }

    public void onBackpressure() {
        backpressureEvents++;
    }

    public void onLeaderElection() {
        leaderElections++;
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
