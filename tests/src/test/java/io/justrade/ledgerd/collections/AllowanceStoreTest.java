package io.justrade.ledgerd.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class AllowanceStoreTest {

    private static AllowanceStore store() {
        return new AllowanceStore(64, 8);
    }

    @Test
    void absentAllowanceIsZero() {
        final AllowanceStore store = store();
        assertEquals(0L, store.get(0L, 1L, 2L));
    }

    @Test
    void setThenGetReturnsStoredValue() {
        final AllowanceStore store = store();
        store.set(0L, 1L, 2L, 500L);
        assertEquals(500L, store.get(0L, 1L, 2L));
    }

    @Test
    void revocationSetsAllowanceToZero() {
        final AllowanceStore store = store();
        store.set(0L, 1L, 2L, 500L);
        store.set(0L, 1L, 2L, 0L);
        assertEquals(0L, store.get(0L, 1L, 2L));
    }

    @Test
    void allowanceCapAtLongMaxIsPreserved() {
        final AllowanceStore store = store();
        store.set(0L, 1L, 2L, Long.MAX_VALUE);
        assertEquals(Long.MAX_VALUE, store.get(0L, 1L, 2L));
    }

    @Test
    void allowancesAreIsolatedAcrossAssets() {
        final AllowanceStore store = store();
        store.set(0L, 1L, 2L, 100L);
        assertEquals(100L, store.get(0L, 1L, 2L));
        assertEquals(0L, store.get(9L, 1L, 2L));
    }

    @Test
    void allowancesAreIsolatedAcrossOwnersAndDelegates() {
        final AllowanceStore store = store();
        store.set(0L, 1L, 2L, 100L);
        store.set(0L, 1L, 3L, 200L);
        store.set(0L, 5L, 2L, 300L);
        assertEquals(100L, store.get(0L, 1L, 2L));
        assertEquals(200L, store.get(0L, 1L, 3L));
        assertEquals(300L, store.get(0L, 5L, 2L));
    }

    @Test
    void ownerCountCountsAssetOwnerPairsNotDelegates() {
        final AllowanceStore store = store();
        // Two delegates under one (asset, owner) count as one owner pair.
        store.set(0L, 1L, 2L, 100L);
        store.set(0L, 1L, 3L, 200L);
        assertEquals(1, store.ownerCount());

        // A second owner on the same asset is a second pair.
        store.set(0L, 5L, 2L, 300L);
        assertEquals(2, store.ownerCount());

        // The same owner on a different asset is a distinct pair.
        store.set(9L, 1L, 2L, 400L);
        assertEquals(3, store.ownerCount());
    }

    @Test
    void clearRemovesAllState() {
        final AllowanceStore store = store();
        store.set(0L, 1L, 2L, 100L);
        store.clear();
        assertEquals(0L, store.get(0L, 1L, 2L));
        assertEquals(0, store.ownerCount());
    }

    @Test
    void forEachSortedEmitsAscendingByAssetOwnerDelegate() {
        final AllowanceStore store = store();
        // Insert deliberately out of order across all three key dimensions.
        store.set(9L, 5L, 7L, 1L);
        store.set(0L, 5L, 2L, 2L);
        store.set(0L, 1L, 3L, 3L);
        store.set(0L, 1L, 2L, 4L);
        store.set(9L, 5L, 1L, 5L);

        final List<long[]> seen = new ArrayList<>();
        store.forEachSorted((assetId, ownerId, delegateId, allowance) ->
                seen.add(new long[] {assetId, ownerId, delegateId, allowance}));

        assertEquals(5, seen.size());
        assertKey(seen.get(0), 0L, 1L, 2L, 4L);
        assertKey(seen.get(1), 0L, 1L, 3L, 3L);
        assertKey(seen.get(2), 0L, 5L, 2L, 2L);
        assertKey(seen.get(3), 9L, 5L, 1L, 5L);
        assertKey(seen.get(4), 9L, 5L, 7L, 1L);
    }

    private static void assertKey(
            final long[] record, final long assetId, final long ownerId, final long delegateId, final long allowance) {
        assertEquals(assetId, record[0]);
        assertEquals(ownerId, record[1]);
        assertEquals(delegateId, record[2]);
        assertEquals(allowance, record[3]);
    }
}
