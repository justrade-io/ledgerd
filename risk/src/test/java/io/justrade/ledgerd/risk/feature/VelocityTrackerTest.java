package io.justrade.ledgerd.risk.feature;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class VelocityTrackerTest {

    @Test
    void warmupReportsZeroThenSpikesOnBurst() {
        final VelocityTracker tracker = new VelocityTracker(0.3, 3);
        final long account = 42L;

        // Baseline: roughly one transaction per second with mild jitter so the
        // account has a realistic non-zero rate variance.
        long t = 0L;
        for (int i = 0; i < 12; i++) {
            tracker.record(account, t);
            t += (i % 2 == 0) ? 900L : 1100L;
        }
        final double baselineZ = tracker.zScore(account);
        assertTrue(Math.abs(baselineZ) < 2.0, "steady traffic stays near baseline, was " + baselineZ);

        // Sudden burst: transactions 1ms apart. The first burst transaction is a
        // huge anomaly against the slow baseline.
        double peakZ = 0.0;
        for (int i = 0; i < 5; i++) {
            peakZ = Math.max(peakZ, tracker.record(account, t));
            t += 1L;
        }
        assertTrue(peakZ > 3.0, "burst produces a high z-score, peak was " + peakZ);
    }

    @Test
    void unseenAccountIsZero() {
        final VelocityTracker tracker = new VelocityTracker();
        assertEquals(0.0, tracker.zScore(99L));
        assertEquals(0.0, tracker.rate(99L));
        assertEquals(0, tracker.size());
    }
}
