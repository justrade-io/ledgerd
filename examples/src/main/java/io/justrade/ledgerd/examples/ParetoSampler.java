package io.justrade.ledgerd.examples;

import java.util.Random;

/**
 * Deterministic two-bucket Pareto account sampler for the multi-account
 * benchmark drivers ({@link ParetoLoadGenerator}, {@link ParetoBatchGenerator}).
 *
 * <p>Models Pareto contention as a two-bucket approximation: the first
 * {@code hotAccounts} account ids receive {@code hotTrafficRatio} of the traffic,
 * and the remaining {@code coldAccounts} ids split the rest uniformly. Given the
 * same seed the sequence is identical, so a benchmark run is reproducible.
 *
 * <p>Examples-side counterpart of the test-only
 * {@code io.justrade.ledgerd.testkit.ParetoAccountSampler}; the examples module
 * must not depend on test fixtures, so the sampler is reimplemented here. Uses a
 * seeded {@link Random}; never part of the deterministic core hot path.
 */
public final class ParetoSampler {

    private final Random random;
    private final int hotAccounts;
    private final int coldAccounts;
    private final double hotTrafficRatio;

    /**
     * @param seed random seed for reproducibility
     * @param hotAccounts number of hot account ids ({@code 1..hotAccounts})
     * @param coldAccounts number of cold account ids ({@code hotAccounts+1..hotAccounts+coldAccounts})
     * @param hotTrafficRatio fraction of traffic routed to the hot bucket, in {@code [0, 1]}
     */
    public ParetoSampler(final long seed, final int hotAccounts, final int coldAccounts, final double hotTrafficRatio) {
        if (hotAccounts < 1) {
            throw new IllegalArgumentException("hotAccounts must be positive, was: " + hotAccounts);
        }
        if (coldAccounts < 1) {
            throw new IllegalArgumentException("coldAccounts must be positive, was: " + coldAccounts);
        }
        if (hotTrafficRatio < 0.0 || hotTrafficRatio > 1.0) {
            throw new IllegalArgumentException("hotTrafficRatio must be in [0, 1], was: " + hotTrafficRatio);
        }
        this.random = new Random(seed);
        this.hotAccounts = hotAccounts;
        this.coldAccounts = coldAccounts;
        this.hotTrafficRatio = hotTrafficRatio;
    }

    /** Returns a hot-skewed account id in {@code [1, hotAccounts + coldAccounts]}. */
    public long nextAccount() {
        if (random.nextDouble() < hotTrafficRatio) {
            return 1L + random.nextInt(hotAccounts);
        }
        return 1L + hotAccounts + random.nextInt(coldAccounts);
    }
}
