package com.adbe.collections;

import java.util.Arrays;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongLongConsumer;

/**
 * Single-writer store of account balances plus the running total supply.
 *
 * <p>Maintains the invariant {@code sum(balances) == totalSupply}. Absence of an
 * account is represented by {@link #MISSING}; a balance is always non-negative,
 * so {@link Long#MIN_VALUE} is a safe sentinel that can never be a real value.
 */
public final class BalanceStore {

    /** Sentinel returned by {@link #rawGet(long)} when the account does not exist. */
    public static final long MISSING = Long.MIN_VALUE;

    private static final float LOAD_FACTOR = 0.65f;

    private final Long2LongHashMap balances;
    private long totalSupply;
    private long[] sortScratch = new long[0];

    public BalanceStore(final int initialCapacity) {
        this.balances = new Long2LongHashMap(initialCapacity, LOAD_FACTOR, MISSING);
        this.totalSupply = 0L;
    }

    /** Returns {@code true} if the account exists. */
    public boolean exists(final long accountId) {
        return balances.get(accountId) != MISSING;
    }

    /** Returns the stored balance or {@link #MISSING} if the account does not exist. */
    public long rawGet(final long accountId) {
        return balances.get(accountId);
    }

    /** Sets the balance for an account, creating it if necessary. */
    public void set(final long accountId, final long balance) {
        balances.put(accountId, balance);
    }

    public long totalSupply() {
        return totalSupply;
    }

    /** Adjusts total supply; used by credit ({@code +}) and debit ({@code -}). */
    public void adjustTotalSupply(final long delta) {
        this.totalSupply += delta;
    }

    /** Restores total supply directly during snapshot load. */
    public void totalSupply(final long value) {
        this.totalSupply = value;
    }

    public int size() {
        return balances.size();
    }

    /** Removes all state; used before a snapshot load. */
    public void clear() {
        balances.clear();
        totalSupply = 0L;
    }

    /**
     * Emits every balance entry in ascending account-id order.
     *
     * <p>Deterministic iteration is mandatory for snapshots. This is a cold path
     * (snapshot only), so extracting and sorting keys is acceptable.
     */
    public void forEachSorted(final LongLongConsumer consumer) {
        final int size = balances.size();
        if (sortScratch.length < size) {
            sortScratch = new long[size];
        }
        final long[] keys = sortScratch;
        final int[] cursor = {0};
        balances.forEachLong((k, v) -> keys[cursor[0]++] = k);
        Arrays.sort(keys, 0, size);
        for (int i = 0; i < size; i++) {
            consumer.accept(keys[i], balances.get(keys[i]));
        }
    }
}
