package io.justrade.ledgerd.launcher;

import io.aeron.cluster.ClusterControl;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.SleepingIdleStrategy;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.status.AtomicCounter;

/**
 * Periodically requests a cluster snapshot so the consensus log does not grow
 * without bound and a recovering node or a cold read replica can start from a
 * recent snapshot instead of replaying the whole log.
 *
 * <p>A snapshot is requested when either a time interval or a committed-command
 * interval elapses. The request is made by toggling the cluster control counter
 * to {@code SNAPSHOT}; only the leader acts on it, so running this scheduler on
 * every node is harmless. The snapshot itself is taken deterministically by the
 * consensus module at a committed log position, off the single-writer hot path.
 *
 * <p>Both intervals are opt-in: a value of zero disables that trigger. The
 * scheduling decision is a pure function ({@link #shouldSnapshot}) so it can be
 * tested without a running cluster.
 */
public final class SnapshotScheduler implements AutoCloseable {

    private final AtomicCounter controlToggle;
    private final LongSupplier committedCommands;
    private final long intervalNs;
    private final long intervalCommands;
    private final NanoClock clock;

    private long lastSnapshotNs;
    private long lastSnapshotCommands;

    private volatile boolean running;
    private Thread thread;

    public SnapshotScheduler(
            final AtomicCounter controlToggle,
            final LongSupplier committedCommands,
            final long intervalNs,
            final long intervalCommands) {
        this(controlToggle, committedCommands, intervalNs, intervalCommands, SystemNanoClock.INSTANCE);
    }

    SnapshotScheduler(
            final AtomicCounter controlToggle,
            final LongSupplier committedCommands,
            final long intervalNs,
            final long intervalCommands,
            final NanoClock clock) {
        if (intervalNs < 0 || intervalCommands < 0) {
            throw new IllegalArgumentException("intervals must be non-negative");
        }
        this.controlToggle = controlToggle;
        this.committedCommands = committedCommands;
        this.intervalNs = intervalNs;
        this.intervalCommands = intervalCommands;
        this.clock = clock;
        this.lastSnapshotNs = clock.nanoTime();
        this.lastSnapshotCommands = committedCommands.getAsLong();
    }

    /** Returns {@code true} when either enabled interval has elapsed. */
    static boolean shouldSnapshot(
            final long nowNs,
            final long lastNs,
            final long intervalNs,
            final long commands,
            final long lastCommands,
            final long intervalCommands) {
        final boolean byTime = intervalNs > 0 && nowNs - lastNs >= intervalNs;
        final boolean byCount = intervalCommands > 0 && commands - lastCommands >= intervalCommands;
        return byTime || byCount;
    }

    /**
     * Evaluates the policy once. When a snapshot is due it advances the baselines
     * and toggles the control counter, returning whether the request was accepted
     * (true only on the leader when no snapshot is already pending).
     */
    boolean tick() {
        final long now = clock.nanoTime();
        final long commands = committedCommands.getAsLong();
        if (!shouldSnapshot(now, lastSnapshotNs, intervalNs, commands, lastSnapshotCommands, intervalCommands)) {
            return false;
        }
        // Advance baselines whether or not this node is the leader, so a follower
        // does not re-toggle every tick; a new leader simply waits a full interval.
        lastSnapshotNs = now;
        lastSnapshotCommands = commands;
        return ClusterControl.ToggleState.SNAPSHOT.toggle(controlToggle);
    }

    /** Starts the background scheduler thread. */
    public void start() {
        if (intervalNs == 0 && intervalCommands == 0) {
            return;
        }
        running = true;
        thread = new Thread(this::run, "ledgerd-snapshot-scheduler");
        thread.setDaemon(true);
        thread.start();
    }

    private void run() {
        final SleepingIdleStrategy idle = new SleepingIdleStrategy(TimeUnit.MILLISECONDS.toNanos(100));
        while (running) {
            tick();
            idle.idle();
        }
    }

    @Override
    public void close() {
        running = false;
        if (thread != null) {
            thread.interrupt();
            try {
                thread.join(TimeUnit.SECONDS.toMillis(2));
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
