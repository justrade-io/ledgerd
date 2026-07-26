package com.adbe.util;

/**
 * Branch-light, allocation-free arithmetic helpers for 64-bit signed amounts.
 *
 * <p>Balances and allowances are non-negative {@code long} values with a fixed
 * scale. These helpers detect overflow without throwing, so callers can map the
 * condition to a status code rather than using exceptions for control flow.
 */
public final class Amounts {

    private Amounts() {}

    /**
     * Returns {@code true} if {@code a + b} would overflow signed 64-bit range.
     *
     * <p>Overflow occurs when both operands share a sign that differs from the
     * sign of the sum.
     */
    public static boolean addOverflows(final long a, final long b) {
        final long r = a + b;
        return ((a ^ r) & (b ^ r)) < 0;
    }

    /** Returns {@code true} if the amount is negative (invalid for a ledger operation). */
    public static boolean isNegative(final long amount) {
        return amount < 0L;
    }
}
