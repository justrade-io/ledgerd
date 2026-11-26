package io.justrade.ledgerd.launcher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.aeron.cluster.ClusterControl;
import java.util.concurrent.TimeUnit;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.junit.jupiter.api.Test;

/**
 * Verifies the snapshot scheduling policy (H2). The scheduling decision is a pure
 * function and the tick logic drives the real cluster control toggle, so both can
 * be checked without a running cluster: a due tick must toggle the control
 * counter to {@code SNAPSHOT} and advance its baselines so it does not re-toggle
 * on the next tick.
 */
final class SnapshotSchedulerTest {

    @Test
    void shouldSnapshotByTimeOnly() {
        final long interval = TimeUnit.SECONDS.toNanos(1);
        assertFalse(SnapshotScheduler.shouldSnapshot(500_000_000L, 0L, interval, 0L, 0L, 0L));
        assertTrue(SnapshotScheduler.shouldSnapshot(1_000_000_000L, 0L, interval, 0L, 0L, 0L));
    }

    @Test
    void shouldSnapshotByCommandsOnly() {
        assertFalse(SnapshotScheduler.shouldSnapshot(0L, 0L, 0L, 4L, 0L, 5L));
        assertTrue(SnapshotScheduler.shouldSnapshot(0L, 0L, 0L, 5L, 0L, 5L));
    }

    @Test
    void shouldNotSnapshotWhenBothIntervalsDisabled() {
        assertFalse(SnapshotScheduler.shouldSnapshot(Long.MAX_VALUE, 0L, 0L, Long.MAX_VALUE, 0L, 0L));
    }

    @Test
    void tickTogglesSnapshotOnCommandThresholdThenResets() {
        final CountersManager counters = newCountersManager();
        final AtomicCounter toggle = counters.newCounter("cluster-control-toggle");
        ClusterControl.ToggleState.reset(toggle);

        final long[] commands = {0L};
        final CachedNanoClock clock = new CachedNanoClock();
        clock.update(0L);
        final SnapshotScheduler scheduler = new SnapshotScheduler(toggle, () -> commands[0], 0L, 5L, clock);

        commands[0] = 4L;
        assertFalse(scheduler.tick());
        assertEquals(ClusterControl.ToggleState.NEUTRAL, ClusterControl.ToggleState.get(toggle));

        commands[0] = 5L;
        assertTrue(scheduler.tick());
        assertEquals(ClusterControl.ToggleState.SNAPSHOT, ClusterControl.ToggleState.get(toggle));

        // Baseline advanced to 5, so an unchanged count is no longer due.
        ClusterControl.ToggleState.reset(toggle);
        assertFalse(scheduler.tick());
        assertEquals(ClusterControl.ToggleState.NEUTRAL, ClusterControl.ToggleState.get(toggle));
    }

    @Test
    void tickTogglesSnapshotOnTimeThreshold() {
        final CountersManager counters = newCountersManager();
        final AtomicCounter toggle = counters.newCounter("cluster-control-toggle");
        ClusterControl.ToggleState.reset(toggle);

        final CachedNanoClock clock = new CachedNanoClock();
        clock.update(0L);
        final SnapshotScheduler scheduler =
                new SnapshotScheduler(toggle, () -> 0L, TimeUnit.SECONDS.toNanos(1), 0L, clock);

        clock.update(TimeUnit.MILLISECONDS.toNanos(500));
        assertFalse(scheduler.tick());

        clock.update(TimeUnit.SECONDS.toNanos(1));
        assertTrue(scheduler.tick());
        assertEquals(ClusterControl.ToggleState.SNAPSHOT, ClusterControl.ToggleState.get(toggle));
    }

    private static CountersManager newCountersManager() {
        final int capacity = 8;
        final UnsafeBuffer values = new UnsafeBuffer(new byte[capacity * CountersManager.COUNTER_LENGTH]);
        final UnsafeBuffer metadata = new UnsafeBuffer(new byte[capacity * CountersManager.METADATA_LENGTH]);
        return new CountersManager(metadata, values);
    }
}
