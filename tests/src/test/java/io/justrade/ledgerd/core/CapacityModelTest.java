package io.justrade.ledgerd.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.justrade.ledgerd.collections.BalanceStore;
import io.justrade.ledgerd.collections.BatchDedupRing;
import io.justrade.ledgerd.collections.DedupRing;
import org.junit.jupiter.api.Test;
import org.openjdk.jol.info.GraphLayout;

/**
 * Verifies the byte factors behind the capacity-planning memory model
 * (docs/CAPACITY-PLANNING.md, sections 1 and 2) using JOL retained-size
 * measurement rather than reflection into private maps.
 *
 * <p>The model asserts that {@code Long2LongHashMap} backing arrays cost 16 B
 * per slot (parallel {@code keys[]} + {@code values[]}), that every touched asset
 * pays the same {@code accountCapacity}, that Agrona resizes past the 0.65 load
 * factor, and that the dedup rings cost 51 B per command slot and 10 B per batch
 * leg slot. This test drives the public constructors and observes the resulting
 * object graph, so it needs no production accessors.
 */
class CapacityModelTest {

    private static final float LOAD_FACTOR = 0.65f;

    private static long retainedBytes(final Object root) {
        return GraphLayout.parseInstance(root).totalSize();
    }

    @Test
    void oneTouchedAssetPreallocatesAccountCapacity() {
        final int capacity = 4096;
        final BalanceStore store = new BalanceStore(capacity);
        final long before = retainedBytes(store);

        store.set(0L, 1L, 100L);

        final long delta = retainedBytes(store) - before;
        // keys[] + values[] each hold `capacity` longs: 16 B per slot.
        assertTrue(delta >= capacity * 16L, "expected at least " + capacity * 16L + " bytes, was " + delta);
        assertTrue(delta < capacity * 16L + 512L, "unexpected overhead beyond 16 B/slot: " + delta);
    }

    @Test
    void everyAssetPaysTheSameCapacity() {
        final int capacity = 4096;
        final BalanceStore store = new BalanceStore(capacity);
        store.set(0L, 1L, 1L);
        final long oneAsset = retainedBytes(store);

        store.set(1L, 1L, 1L);

        final long delta = retainedBytes(store) - oneAsset;
        assertTrue(
                delta >= capacity * 16L, "second asset should preallocate " + capacity * 16L + " bytes, was " + delta);
        assertTrue(delta < capacity * 16L + 512L, "unexpected overhead for second asset: " + delta);
    }

    @Test
    void resizePastSafeFillDoublesBackingArrays() {
        final int capacity = 1024;
        final BalanceStore store = new BalanceStore(capacity);
        // Agrona resizes when size exceeds capacity * loadFactor (0.65).
        final int safeFill = (int) (capacity * LOAD_FACTOR);

        for (long account = 1; account <= safeFill - 2; account++) {
            store.set(0L, account, 1L);
        }
        final long belowThreshold = retainedBytes(store);

        for (long account = safeFill - 1; account <= safeFill + 2; account++) {
            store.set(0L, account, 1L);
        }
        final long crossedThreshold = retainedBytes(store);

        // Crossing the threshold doubles the backing arrays, so retained bytes
        // roughly double while the fixed overhead stays constant.
        assertTrue(
                crossedThreshold > belowThreshold * 1.5,
                "expected resize to roughly double retained bytes, before=" + belowThreshold + " after="
                        + crossedThreshold);
    }

    @Test
    void batchDedupRingMatchesTenBytesPerLegSlot() {
        final int window = 1024;
        final int maxBatchSize = 1024;
        final BatchDedupRing ring = new BatchDedupRing(window, maxBatchSize);

        // Fixed per-slot arrays: seqSlots (8) + legCounts (4) + batchIdHi (8) +
        // batchIdLo (8) = 28 B/slot. Leg region: status (1) + hasBalance (1) +
        // resultBalance (8) = 10 B per leg slot.
        final long expected = window * 28L + (long) window * maxBatchSize * 10L;

        assertEquals(expected, retainedBytes(ring), 256L, "batch ring must match 28 B/slot + 10 B/leg-slot");
    }

    @Test
    void dedupRingMatches51BytesPerSlot() {
        final int capacity = 1024;
        final DedupRing ring = new DedupRing(capacity);

        // seqSlots (8) + commandIdHi (8) + commandIdLo (8) + statusValue (2) +
        // resultBalance (8) + resultAllowance (8) + resultReserved (8) + flags (1).
        final long expected = 51L * capacity;

        assertEquals(expected, retainedBytes(ring), 256L, "dedup ring must match 51 B/slot");
    }
}
