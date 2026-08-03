package com.adbe.collections;

import java.util.Arrays;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Single-writer store of delegated-spending allowances.
 *
 * <p>Keyed by (owner, delegate) using a nested primitive map so that two 64-bit
 * identifiers are stored without lossy composite hashing. An absent allowance is
 * exactly zero. Owner maps are created lazily on first approval, which is a cold
 * onboarding event rather than a steady-state hot-path allocation.
 */
public final class AllowanceStore {

    // Sentinel for an absent delegate entry. Allowances are validated non-negative
    // before storage, so -1 can never collide with a real value.
    private static final long MISSING = -1L;
    private static final long NONE = 0L;
    private static final float LOAD_FACTOR = 0.65f;

    private final Long2ObjectHashMap<Long2LongHashMap> byOwner;
    private final int delegateCapacity;
    private long[] ownerScratch = new long[0];
    private long[] delegateScratch = new long[0];

    public AllowanceStore(final int ownerCapacity, final int delegateCapacity) {
        this.byOwner = new Long2ObjectHashMap<>(ownerCapacity, LOAD_FACTOR);
        this.delegateCapacity = delegateCapacity;
    }

    /** Returns the current allowance the delegate may spend on the owner's behalf. */
    public long get(final long ownerId, final long delegateId) {
        final Long2LongHashMap delegates = byOwner.get(ownerId);
        if (delegates == null) {
            return NONE;
        }
        final long value = delegates.get(delegateId);
        return value == MISSING ? NONE : value;
    }

    /** Overwrites the allowance (approve semantics). */
    public void set(final long ownerId, final long delegateId, final long value) {
        delegatesOf(ownerId).put(delegateId, value);
    }

    public int ownerCount() {
        return byOwner.size();
    }

    /** Removes all state; used before a snapshot load. */
    public void clear() {
        byOwner.clear();
    }

    private Long2LongHashMap delegatesOf(final long ownerId) {
        Long2LongHashMap delegates = byOwner.get(ownerId);
        if (delegates == null) {
            delegates = new Long2LongHashMap(delegateCapacity, LOAD_FACTOR, MISSING);
            byOwner.put(ownerId, delegates);
        }
        return delegates;
    }

    /**
     * Emits every allowance entry in ascending (owner, delegate) order.
     *
     * <p>Cold snapshot path: extracting and sorting keys is acceptable here.
     */
    public void forEachSorted(final AllowanceConsumer consumer) {
        final int ownerCount = byOwner.size();
        if (ownerScratch.length < ownerCount) {
            ownerScratch = new long[ownerCount];
        }
        final long[] owners = ownerScratch;
        final int[] ownerCursor = {0};
        byOwner.forEachLong((owner, delegates) -> owners[ownerCursor[0]++] = owner);
        Arrays.sort(owners, 0, ownerCount);

        for (int o = 0; o < ownerCount; o++) {
            final long owner = owners[o];
            final Long2LongHashMap delegates = byOwner.get(owner);
            final int delegateCount = delegates.size();
            if (delegateScratch.length < delegateCount) {
                delegateScratch = new long[delegateCount];
            }
            final long[] delegateIds = delegateScratch;
            final int[] delegateCursor = {0};
            delegates.forEachLong((delegate, value) -> delegateIds[delegateCursor[0]++] = delegate);
            Arrays.sort(delegateIds, 0, delegateCount);
            for (int d = 0; d < delegateCount; d++) {
                final long delegate = delegateIds[d];
                consumer.accept(owner, delegate, delegates.get(delegate));
            }
        }
    }

    /** Primitive callback for deterministic allowance iteration. */
    @FunctionalInterface
    public interface AllowanceConsumer {
        void accept(long ownerId, long delegateId, long allowance);
    }
}
