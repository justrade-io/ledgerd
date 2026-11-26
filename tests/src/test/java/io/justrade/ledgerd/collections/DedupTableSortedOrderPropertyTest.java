package io.justrade.ledgerd.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

/**
 * The snapshot path emits dedup records through {@code forEachSorted} and
 * {@code forEachSortedBatch}, which must impose a total, deterministic order
 * over the hash-map-backed state; otherwise two replicas would write
 * byte-divergent snapshots. These properties assert the emitted order is
 * ascending by {@code (clientId, seq)} and identical across repeated iterations
 * of the same state.
 */
class DedupTableSortedOrderPropertyTest {

    private static DedupTable table() {
        return new DedupTable(64, 1024, 1024, 8);
    }

    @Property
    void commandRecordsAreEmittedAscendingAndStable(@ForAll("records") final List<long[]> records) {
        final DedupTable table = table();
        for (final long[] record : records) {
            table.store(record[0], record[1], 0L, record[1], (short) 0, 0L, false, 0L, false, 0L, false);
        }

        final List<long[]> first = collect(table);
        final List<long[]> second = collect(table);

        assertAscending(first);
        assertSameOrder(first, second);
    }

    @Property
    void batchRecordsAreEmittedAscendingAndStable(@ForAll("records") final List<long[]> records) {
        final DedupTable table = table();
        final byte[] status = {0};
        final byte[] hasBalance = {0};
        final long[] balance = {0L};
        for (final long[] record : records) {
            table.storeBatch(record[0], record[1], 0L, record[1], 1, status, hasBalance, balance);
        }

        final List<long[]> first = collectBatch(table);
        final List<long[]> second = collectBatch(table);

        assertAscending(first);
        assertSameOrder(first, second);
    }

    private static List<long[]> collect(final DedupTable table) {
        final List<long[]> out = new ArrayList<>();
        table.forEachSorted((clientId, seq, idHi, idLo, status, bal, hasBal, allow, hasAllow, res, hasRes) ->
                out.add(new long[] {clientId, seq}));
        return out;
    }

    private static List<long[]> collectBatch(final DedupTable table) {
        final List<long[]> out = new ArrayList<>();
        table.forEachSortedBatch((clientId, seq, idHi, idLo, ring) -> out.add(new long[] {clientId, seq}));
        return out;
    }

    private static void assertAscending(final List<long[]> records) {
        for (int i = 1; i < records.size(); i++) {
            final long[] prev = records.get(i - 1);
            final long[] curr = records.get(i);
            final boolean ordered = prev[0] < curr[0] || (prev[0] == curr[0] && prev[1] < curr[1]);
            assertTrue(ordered, "records must be strictly ascending by (clientId, seq)");
        }
    }

    private static void assertSameOrder(final List<long[]> first, final List<long[]> second) {
        assertEquals(first.size(), second.size());
        for (int i = 0; i < first.size(); i++) {
            assertEquals(first.get(i)[0], second.get(i)[0]);
            assertEquals(first.get(i)[1], second.get(i)[1]);
        }
    }

    @Provide
    Arbitrary<List<long[]>> records() {
        final Arbitrary<Long> clientId = Arbitraries.longs().between(0L, 7L);
        final Arbitrary<Long> seq = Arbitraries.longs().between(0L, 1_000_000L);
        final Arbitrary<long[]> pair = Combinators.combine(clientId, seq).as((c, s) -> new long[] {c, s});
        return pair.list().ofMaxSize(40);
    }
}
