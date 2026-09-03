package io.justrade.ledgerd.collections;

import java.util.Arrays;

/**
 * Fixed-capacity, allocation-free ring holding the most recent transfer-batch
 * results for one client, indexed by {@code clientSeq & (capacity - 1)}.
 *
 * <p>Each slot stores one batch result: the batch id plus a flattened run of
 * per-leg {@code (status, hasBalance, resultBalance)} tuples. The per-leg region
 * is fixed at {@code maxBatchSize}, so a slot's memory is contiguous and lookup
 * is O(1). As with {@link DedupRing}, retries reuse the same {@code clientSeq}
 * and are detected as duplicates.
 *
 * <p>Contract: {@code clientSeq} must never equal {@link #EMPTY} (all-ones).
 */
public final class BatchDedupRing {

    /** Reserved sentinel marking an unoccupied slot. */
    public static final long EMPTY = -1L;

    private final int mask;
    private final int maxBatchSize;

    private final long[] seqSlots;
    private final int[] legCounts;
    private final long[] batchIdHi;
    private final long[] batchIdLo;
    private final byte[] statusValues;
    private final byte[] hasBalance;
    private final long[] resultBalance;

    public BatchDedupRing(final int capacity, final int maxBatchSize) {
        this.mask = capacity - 1;
        this.maxBatchSize = maxBatchSize;
        this.seqSlots = new long[capacity];
        this.legCounts = new int[capacity];
        this.batchIdHi = new long[capacity];
        this.batchIdLo = new long[capacity];
        this.statusValues = new byte[capacity * maxBatchSize];
        this.hasBalance = new byte[capacity * maxBatchSize];
        this.resultBalance = new long[capacity * maxBatchSize];
        Arrays.fill(seqSlots, EMPTY);
    }

    /** Returns {@code true} if this sequence's slot currently holds that sequence. */
    public boolean contains(final long seq) {
        return seq != EMPTY && seqSlots[(int) (seq & mask)] == seq;
    }

    /** Stores (or overwrites) a batch result for a sequence. */
    public boolean put(
            final long seq,
            final long idHi,
            final long idLo,
            final int legCount,
            final byte[] status,
            final byte[] balancePresent,
            final long[] balance) {
        final int idx = (int) (seq & mask);
        final long prior = seqSlots[idx];
        final boolean evicted = prior != EMPTY && prior != seq;
        seqSlots[idx] = seq;
        legCounts[idx] = legCount;
        batchIdHi[idx] = idHi;
        batchIdLo[idx] = idLo;
        final int base = idx * maxBatchSize;
        for (int i = 0; i < legCount; i++) {
            statusValues[base + i] = status[i];
            hasBalance[base + i] = balancePresent[i];
            resultBalance[base + i] = balance[i];
        }
        return evicted;
    }

    public long batchIdHi(final long seq) {
        return batchIdHi[(int) (seq & mask)];
    }

    public long batchIdLo(final long seq) {
        return batchIdLo[(int) (seq & mask)];
    }

    public int legCount(final long seq) {
        return legCounts[(int) (seq & mask)];
    }

    public short status(final long seq, final int leg) {
        return statusValues[(int) (seq & mask) * maxBatchSize + leg];
    }

    public boolean hasBalance(final long seq, final int leg) {
        return hasBalance[(int) (seq & mask) * maxBatchSize + leg] != 0;
    }

    public long resultBalance(final long seq, final int leg) {
        return resultBalance[(int) (seq & mask) * maxBatchSize + leg];
    }

    /** Ring capacity (power of two). */
    public int capacity() {
        return mask + 1;
    }

    /**
     * Copies every occupied sequence into {@code dest} and returns the count.
     *
     * <p>Cold snapshot path only; {@code dest} must be at least {@link #capacity()}.
     */
    public int occupiedSeqs(final long[] dest) {
        int n = 0;
        for (final long slot : seqSlots) {
            if (slot != EMPTY) {
                dest[n++] = slot;
            }
        }
        return n;
    }
}
