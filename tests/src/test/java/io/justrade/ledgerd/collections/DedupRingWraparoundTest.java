package io.justrade.ledgerd.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import net.jqwik.api.Assume;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import org.junit.jupiter.api.Test;

/**
 * The dedup slot index is {@code seq & (capacity - 1)}, which treats the 64-bit
 * {@code clientSeq} as an unsigned bit pattern. These tests exercise sequences
 * with the high bit set (values above {@code Long.MAX_VALUE} when read as
 * unsigned) to prove the index arithmetic and eviction behave the same across
 * the whole unsigned range, not just small sequences.
 */
class DedupRingWraparoundTest {

    private static final int CAPACITY = 16;

    private static boolean put(final DedupRing ring, final long seq) {
        return ring.put(seq, 0L, seq, (short) 0, seq, true, 0L, false, 0L, false);
    }

    @Test
    void highBitSequencesAreStoredAndFound() {
        // A fresh ring per sequence so each write lands in an empty slot, isolating
        // the index arithmetic from eviction on colliding slots.
        for (final long seq : new long[] {Long.MAX_VALUE, Long.MIN_VALUE, -2L, 0x8000000000000010L}) {
            final DedupRing ring = new DedupRing(CAPACITY);
            assertFalse(put(ring, seq), "first write into an empty slot never evicts");
            assertTrue(ring.contains(seq), "sequence must be found in its slot");
            assertEquals(seq, ring.commandIdLo(seq));
        }
    }

    @Test
    void sequencesAtUnsignedTopEvictOnSameSlot() {
        final DedupRing ring = new DedupRing(CAPACITY);
        // -18 and -2 both map to slot 14 (& 15); the later write evicts the earlier.
        final long older = -18L;
        final long newer = -2L;
        assertEquals(older & (CAPACITY - 1), newer & (CAPACITY - 1), "test precondition: same slot");

        assertFalse(put(ring, older));
        assertTrue(ring.contains(older));

        assertTrue(put(ring, newer), "a different sequence in an occupied slot evicts");
        assertTrue(ring.contains(newer));
        assertFalse(ring.contains(older), "the evicted sequence is no longer present");
    }

    @Test
    void tableLevelWraparoundEvictionIsReported() {
        final DedupTable table = new DedupTable(16, CAPACITY, CAPACITY, 8);
        final long clientId = 42L;
        final long older = Long.MIN_VALUE;
        final long newer = Long.MIN_VALUE + CAPACITY;

        assertFalse(store(table, clientId, older));
        assertTrue(table.ringFor(clientId).contains(older));

        assertTrue(store(table, clientId, newer), "same-slot store must report eviction");
        assertFalse(table.ringFor(clientId).contains(older));
        assertTrue(table.ringFor(clientId).contains(newer));
    }

    @Property
    void anySequenceIsFoundAfterStore(@ForAll final long seq) {
        Assume.that(seq != DedupRing.EMPTY);
        final DedupRing ring = new DedupRing(CAPACITY);
        put(ring, seq);
        assertTrue(ring.contains(seq));
    }

    @Property
    void sameSlotSuccessorEvictsPredecessor(@ForAll final long seq) {
        Assume.that(seq != DedupRing.EMPTY);
        final long successor = seq + CAPACITY;
        Assume.that(successor != DedupRing.EMPTY);
        Assume.that(successor != seq);

        final DedupRing ring = new DedupRing(CAPACITY);
        put(ring, seq);
        final boolean evicted = put(ring, successor);

        assertTrue(evicted);
        assertTrue(ring.contains(successor));
        assertFalse(ring.contains(seq));
    }

    private static boolean store(final DedupTable table, final long clientId, final long seq) {
        return table.store(clientId, seq, 0L, seq, (short) 0, seq, true, 0L, false, 0L, false);
    }
}
