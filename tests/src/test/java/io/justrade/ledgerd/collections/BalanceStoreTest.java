package io.justrade.ledgerd.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BalanceStoreTest {

    private static BalanceStore store() {
        return new BalanceStore(64);
    }

    @Test
    void absentAccountIsMissing() {
        final BalanceStore store = store();
        assertFalse(store.exists(0L, 1L));
        assertEquals(BalanceStore.MISSING, store.rawGet(0L, 1L));
    }

    @Test
    void setThenGetReturnsBalanceAndCreatesAccount() {
        final BalanceStore store = store();
        store.set(0L, 1L, 500L);
        assertTrue(store.exists(0L, 1L));
        assertEquals(500L, store.rawGet(0L, 1L));
        assertEquals(1, store.size());
    }

    @Test
    void reservedDefaultsToZeroAndIsSettable() {
        final BalanceStore store = store();
        assertEquals(0L, store.reserved(0L, 1L));
        store.setReserved(0L, 1L, 250L);
        assertEquals(250L, store.reserved(0L, 1L));
    }

    @Test
    void supplyTracksAdjustmentsPerAsset() {
        final BalanceStore store = store();
        store.adjustTotalSupply(0L, 1000L);
        store.adjustTotalSupply(0L, -400L);
        store.adjustTotalSupply(1L, 50L);
        assertEquals(600L, store.totalSupply(0L));
        assertEquals(50L, store.totalSupply(1L));
        assertEquals(650L, store.aggregateSupply());
    }

    @Test
    void singleAssetBurstThenAssetSwitchKeepsBucketsIsolated() {
        final BalanceStore store = store();
        // A burst of same-asset writes exercises the last-asset cache repeatedly.
        for (long account = 0; account < 32; account++) {
            store.set(0L, account, account * 10L);
        }
        // Writing a different asset displaces the cached bucket.
        store.set(7L, 100L, 999L);

        // Reads on the original asset must still resolve to the right bucket, and
        // the two assets must not leak into each other.
        for (long account = 0; account < 32; account++) {
            assertEquals(account * 10L, store.rawGet(0L, account));
            assertEquals(BalanceStore.MISSING, store.rawGet(7L, account));
        }
        assertEquals(999L, store.rawGet(7L, 100L));
        assertEquals(BalanceStore.MISSING, store.rawGet(0L, 100L));
        assertEquals(2, store.assetCount());
        assertEquals(33, store.size());
    }

    @Test
    void restoreAvailableToPriorValueRewindsBalance() {
        final BalanceStore store = store();
        store.set(0L, 1L, 100L);
        store.set(0L, 1L, 40L);
        store.restoreAvailable(0L, 1L, 100L);
        assertEquals(100L, store.rawGet(0L, 1L));
        assertEquals(1, store.size());
    }

    @Test
    void restoreAvailableToMissingRemovesAutoCreatedAccount() {
        final BalanceStore store = store();
        store.set(0L, 1L, 60L);
        assertEquals(1, store.size());
        store.restoreAvailable(0L, 1L, BalanceStore.MISSING);
        assertFalse(store.exists(0L, 1L));
        assertEquals(0, store.size());
    }

    @Test
    void setSupplyOverwritesRunningSupply() {
        final BalanceStore store = store();
        store.adjustTotalSupply(0L, 1000L);
        store.setSupply(0L, 250L);
        assertEquals(250L, store.totalSupply(0L));
    }

    @Test
    void clearRemovesAllState() {
        final BalanceStore store = store();
        store.set(0L, 1L, 100L);
        store.adjustTotalSupply(0L, 100L);
        store.clear();
        assertFalse(store.exists(0L, 1L));
        assertEquals(0, store.size());
        assertEquals(0, store.assetCount());
        assertEquals(0L, store.totalSupply(0L));
    }

    @Test
    void forEachSortedEmitsAscendingByAssetThenAccount() {
        final BalanceStore store = store();
        store.set(1L, 5L, 50L);
        store.set(0L, 9L, 90L);
        store.set(0L, 2L, 20L);
        store.setReserved(0L, 2L, 5L);

        final List<long[]> seen = new ArrayList<>();
        store.forEachSorted((assetId, accountId, balance, reserved) ->
                seen.add(new long[] {assetId, accountId, balance, reserved}));

        assertEquals(3, seen.size());
        assertRecord(seen.get(0), 0L, 2L, 20L, 5L);
        assertRecord(seen.get(1), 0L, 9L, 90L, 0L);
        assertRecord(seen.get(2), 1L, 5L, 50L, 0L);
    }

    @Test
    void forEachSupplySortedEmitsAscendingByAsset() {
        final BalanceStore store = store();
        store.adjustTotalSupply(5L, 500L);
        store.adjustTotalSupply(1L, 100L);
        store.adjustTotalSupply(3L, 300L);

        final List<long[]> seen = new ArrayList<>();
        store.forEachSupplySorted((assetId, totalSupply) -> seen.add(new long[] {assetId, totalSupply}));

        assertEquals(3, seen.size());
        assertEquals(1L, seen.get(0)[0]);
        assertEquals(3L, seen.get(1)[0]);
        assertEquals(5L, seen.get(2)[0]);
    }

    private static void assertRecord(
            final long[] record, final long assetId, final long accountId, final long balance, final long reserved) {
        assertEquals(assetId, record[0]);
        assertEquals(accountId, record[1]);
        assertEquals(balance, record[2]);
        assertEquals(reserved, record[3]);
    }
}
