package com.adbe.bench;

import java.util.Random;

/**
 * Produces a deterministic pseudo-random wallet workload. Given the same seed,
 * account count, and mix, the exact same op stream is produced, so every backend
 * runs an identical sequence (a precondition for a fair comparison).
 *
 * <p>Amounts are kept small relative to the seeded initial balance so debits and
 * transfers never underflow; every op therefore succeeds on every backend and the
 * final total supply is a pure function of the op stream (see {@link Op#supplyDelta}).
 * This lives in benchmark code only, never in the deterministic core.
 */
public final class WorkloadGenerator {

    private static final long MIN_AMOUNT = 1L;
    private static final long MAX_AMOUNT = 1000L;

    private WorkloadGenerator() {}

    /**
     * @param count number of ops to generate
     * @param accounts account id space (ids are {@code 1..accounts})
     * @param mix relative weights for CREDIT, DEBIT, TRANSFER (any non-negative ints)
     * @param seed RNG seed for reproducibility
     */
    public static Op[] generate(final int count, final int accounts, final int[] mix, final long seed) {
        if (accounts < 2) {
            throw new IllegalArgumentException("need at least 2 accounts, was: " + accounts);
        }
        if (mix.length != 3) {
            throw new IllegalArgumentException("mix must have 3 weights (credit, debit, transfer)");
        }
        final int totalWeight = mix[0] + mix[1] + mix[2];
        if (totalWeight <= 0) {
            throw new IllegalArgumentException("mix weights must sum to a positive value");
        }

        final Random random = new Random(seed);
        final Op[] ops = new Op[count];
        for (int i = 0; i < count; i++) {
            final OpType type = pickType(random, mix, totalWeight);
            final long a = 1L + random.nextInt(accounts);
            final long amount = MIN_AMOUNT + random.nextInt((int) (MAX_AMOUNT - MIN_AMOUNT + 1));
            if (type == OpType.TRANSFER) {
                long b = 1L + random.nextInt(accounts);
                while (b == a) {
                    b = 1L + random.nextInt(accounts);
                }
                ops[i] = new Op(OpType.TRANSFER, a, b, amount);
            } else {
                ops[i] = new Op(type, a, 0L, amount);
            }
        }
        return ops;
    }

    private static OpType pickType(final Random random, final int[] mix, final int totalWeight) {
        final int roll = random.nextInt(totalWeight);
        if (roll < mix[0]) {
            return OpType.CREDIT;
        }
        if (roll < mix[0] + mix[1]) {
            return OpType.DEBIT;
        }
        return OpType.TRANSFER;
    }
}
