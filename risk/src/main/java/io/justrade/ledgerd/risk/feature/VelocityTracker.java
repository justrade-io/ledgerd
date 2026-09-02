package io.justrade.ledgerd.risk.feature;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Per-account transaction-velocity tracker (ADR 0012). For each account it keeps
 * an exponentially weighted moving average of the instantaneous transaction rate
 * (events per second) together with an exponentially weighted variance, and
 * yields a live z-score of the newest transaction against the account's own
 * baseline.
 *
 * <p>A high positive z-score means the account is transacting far faster than its
 * recent history, the classic velocity anomaly signal.
 *
 * <p>Single-writer: {@link #record(long, long)} is called only on the event
 * follower's agent thread. Reads ({@link #zScore(long)}, {@link #rate(long)}) come
 * from the HTTP thread and see published {@code volatile} snapshots, so a read
 * never blocks or perturbs the writer.
 */
public final class VelocityTracker {

    /** Default smoothing factor for the EWMA (higher = more reactive). */
    public static final double DEFAULT_ALPHA = 0.2;

    /** Default number of samples before a z-score is reported (warm-up). */
    public static final int DEFAULT_MIN_SAMPLES = 5;

    // Instantaneous rate is 1000/dt; clamp dt to this floor to bound a burst spike.
    private static final long MIN_INTERVAL_MS = 1L;

    private final double alpha;
    private final int minSamples;
    private final ConcurrentHashMap<Long, Account> accounts = new ConcurrentHashMap<>();

    public VelocityTracker() {
        this(DEFAULT_ALPHA, DEFAULT_MIN_SAMPLES);
    }

    public VelocityTracker(final double alpha, final int minSamples) {
        if (alpha <= 0.0 || alpha >= 1.0) {
            throw new IllegalArgumentException("alpha must be in (0, 1)");
        }
        if (minSamples < 1) {
            throw new IllegalArgumentException("minSamples must be >= 1");
        }
        this.alpha = alpha;
        this.minSamples = minSamples;
    }

    /**
     * Records a transaction for {@code accountId} at leader time {@code timestampMs}
     * and updates the account's velocity baseline. Returns the z-score of this
     * transaction against the pre-update baseline (0 during warm-up).
     */
    public double record(final long accountId, final long timestampMs) {
        final Account account = accounts.computeIfAbsent(accountId, id -> new Account());
        return account.update(timestampMs, alpha, minSamples);
    }

    /** The most recent z-score published for {@code accountId} (0 if unseen). */
    public double zScore(final long accountId) {
        final Account account = accounts.get(accountId);
        return account == null ? 0.0 : account.zScore;
    }

    /** The current smoothed transaction rate (events/sec) for {@code accountId}. */
    public double rate(final long accountId) {
        final Account account = accounts.get(accountId);
        return account == null ? 0.0 : account.rate;
    }

    /** Number of accounts observed so far. */
    public int size() {
        return accounts.size();
    }

    /** Mutable per-account state; working fields touched only by the writer thread. */
    private static final class Account {
        private long lastTimestampMs = Long.MIN_VALUE;
        private long count;
        private double ewmaVar;

        // Published for concurrent dashboard reads (volatile double reads are atomic).
        private volatile double rate;
        private volatile double zScore;

        private double update(final long timestampMs, final double alpha, final int minSamples) {
            count++;
            if (lastTimestampMs == Long.MIN_VALUE) {
                lastTimestampMs = timestampMs;
                return 0.0;
            }
            final long dt = Math.max(MIN_INTERVAL_MS, timestampMs - lastTimestampMs);
            lastTimestampMs = timestampMs;
            final double instantRate = 1000.0 / dt;

            final double mean = rate;
            final double deviation = instantRate - mean;
            final double stdDev = Math.sqrt(ewmaVar);
            final double z = count > minSamples && stdDev > 0.0 ? deviation / stdDev : 0.0;

            // EW mean and EW variance (West, 1979 exponential form).
            rate = mean + alpha * deviation;
            ewmaVar = (1.0 - alpha) * (ewmaVar + alpha * deviation * deviation);
            zScore = z;
            return z;
        }
    }
}
