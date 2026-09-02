package io.justrade.ledgerd.collections;

import java.util.Arrays;

/**
 * Fixed-capacity, allocation-free ring holding the most recent commands for one
 * client, indexed directly by {@code clientSeq & (capacity - 1)}.
 *
 * <p>Because {@code clientSeq} is monotonic per client, the slot for a sequence
 * is deterministic and lookup is O(1): the slot is occupied by that sequence iff
 * {@code seqSlots[seq & mask] == seq}. Retries reuse the same {@code clientSeq},
 * so they resolve to the same slot and are detected as duplicates.
 *
 * <p>Contract: {@code clientSeq} must never equal {@link #EMPTY} (all-ones),
 * which is reserved as the unoccupied-slot sentinel.
 */
public final class DedupRing {

    /** Reserved sentinel marking an unoccupied slot. */
    public static final long EMPTY = -1L;

    /** Flag bit indicating the cached result carried a balance value. */
    public static final int FLAG_HAS_BALANCE = 1;

    /** Flag bit indicating the cached result carried an allowance value. */
    public static final int FLAG_HAS_ALLOWANCE = 1 << 1;

    /** Flag bit indicating the cached result carried a reserved value. */
    public static final int FLAG_HAS_RESERVED = 1 << 2;

    private final int mask;
    private final long[] seqSlots;
    private final long[] commandIdHi;
    private final long[] commandIdLo;
    private final short[] statusValue;
    private final long[] resultBalance;
    private final long[] resultAllowance;
    private final long[] resultReserved;
    private final byte[] flags;

    public DedupRing(final int capacity) {
        this.mask = capacity - 1;
        this.seqSlots = new long[capacity];
        this.commandIdHi = new long[capacity];
        this.commandIdLo = new long[capacity];
        this.statusValue = new short[capacity];
        this.resultBalance = new long[capacity];
        this.resultAllowance = new long[capacity];
        this.resultReserved = new long[capacity];
        this.flags = new byte[capacity];
        Arrays.fill(seqSlots, EMPTY);
    }

    /** Returns {@code true} if this sequence's slot currently holds that sequence. */
    public boolean contains(final long seq) {
        return seq != EMPTY && seqSlots[(int) (seq & mask)] == seq;
    }

    /** Stores (or overwrites) the cached result for a sequence. */
    public boolean put(
            final long seq,
            final long idHi,
            final long idLo,
            final short status,
            final long balance,
            final boolean hasBalance,
            final long allowance,
            final boolean hasAllowance,
            final long reserved,
            final boolean hasReserved) {
        final int idx = (int) (seq & mask);
        final long prior = seqSlots[idx];
        final boolean evicted = prior != EMPTY && prior != seq;
        seqSlots[idx] = seq;
        commandIdHi[idx] = idHi;
        commandIdLo[idx] = idLo;
        statusValue[idx] = status;
        resultBalance[idx] = balance;
        resultAllowance[idx] = allowance;
        resultReserved[idx] = reserved;
        int f = 0;
        if (hasBalance) {
            f |= FLAG_HAS_BALANCE;
        }
        if (hasAllowance) {
            f |= FLAG_HAS_ALLOWANCE;
        }
        if (hasReserved) {
            f |= FLAG_HAS_RESERVED;
        }
        flags[idx] = (byte) f;
        return evicted;
    }

    public long commandIdHi(final long seq) {
        return commandIdHi[(int) (seq & mask)];
    }

    public long commandIdLo(final long seq) {
        return commandIdLo[(int) (seq & mask)];
    }

    public short status(final long seq) {
        return statusValue[(int) (seq & mask)];
    }

    public long resultBalance(final long seq) {
        return resultBalance[(int) (seq & mask)];
    }

    public long resultAllowance(final long seq) {
        return resultAllowance[(int) (seq & mask)];
    }

    public long resultReserved(final long seq) {
        return resultReserved[(int) (seq & mask)];
    }

    public boolean hasBalance(final long seq) {
        return (flags[(int) (seq & mask)] & FLAG_HAS_BALANCE) != 0;
    }

    public boolean hasAllowance(final long seq) {
        return (flags[(int) (seq & mask)] & FLAG_HAS_ALLOWANCE) != 0;
    }

    public boolean hasReserved(final long seq) {
        return (flags[(int) (seq & mask)] & FLAG_HAS_RESERVED) != 0;
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
