package io.justrade.ledgerd.collections;

import java.util.Arrays;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Single-writer store of delegated-spending allowances, scoped per asset.
 *
 * <p>Keyed by {@code (assetId, owner, delegate)} using nested primitive maps so
 * that the three 64-bit identifiers are stored without lossy composite hashing.
 * An allowance approved for one asset never authorizes spending another. An
 * absent allowance is exactly zero. Owner maps are created lazily on first
 * approval, which is a cold onboarding event rather than a steady-state hot-path
 * allocation.
 */
public final class AllowanceStore {

    // Sentinel for an absent delegate entry. Allowances are validated non-negative
    // before storage, so -1 can never collide with a real value.
    private static final long MISSING = -1L;
    private static final long NONE = 0L;
    private static final float LOAD_FACTOR = 0.65f;

    private final Long2ObjectHashMap<Long2ObjectHashMap<Long2LongHashMap>> byAsset;
    private final int ownerCapacity;
    private final int delegateCapacity;
    private long totalOwners;

    private long[] assetScratch = new long[0];
    private long[] ownerScratch = new long[0];
    private long[] delegateScratch = new long[0];

    public AllowanceStore(final int ownerCapacity, final int delegateCapacity) {
        this.byAsset = new Long2ObjectHashMap<>(16, LOAD_FACTOR);
        this.ownerCapacity = ownerCapacity;
        this.delegateCapacity = delegateCapacity;
    }

    /** Returns the current allowance the delegate may spend on the owner's behalf. */
    public long get(final long assetId, final long ownerId, final long delegateId) {
        final Long2ObjectHashMap<Long2LongHashMap> owners = byAsset.get(assetId);
        if (owners == null) {
            return NONE;
        }
        final Long2LongHashMap delegates = owners.get(ownerId);
        if (delegates == null) {
            return NONE;
        }
        final long value = delegates.get(delegateId);
        return value == MISSING ? NONE : value;
    }

    /** Overwrites the allowance (approve semantics). */
    public void set(final long assetId, final long ownerId, final long delegateId, final long value) {
        delegatesOf(assetId, ownerId).put(delegateId, value);
    }

    /** Total number of (asset, owner) pairs holding at least one allowance. */
    public int ownerCount() {
        return (int) totalOwners;
    }

    /** Removes all state; used before a snapshot load. */
    public void clear() {
        byAsset.clear();
        totalOwners = 0L;
    }

    private Long2LongHashMap delegatesOf(final long assetId, final long ownerId) {
        Long2ObjectHashMap<Long2LongHashMap> owners = byAsset.get(assetId);
        if (owners == null) {
            owners = new Long2ObjectHashMap<>(ownerCapacity, LOAD_FACTOR);
            byAsset.put(assetId, owners);
        }
        Long2LongHashMap delegates = owners.get(ownerId);
        if (delegates == null) {
            delegates = new Long2LongHashMap(delegateCapacity, LOAD_FACTOR, MISSING);
            owners.put(ownerId, delegates);
            totalOwners++;
        }
        return delegates;
    }

    /**
     * Emits every allowance entry in ascending {@code (assetId, owner, delegate)}
     * order.
     *
     * <p>Cold snapshot path: extracting and sorting keys is acceptable here.
     */
    public void forEachSorted(final AllowanceConsumer consumer) {
        final int assetCount = byAsset.size();
        if (assetScratch.length < assetCount) {
            assetScratch = new long[assetCount];
        }
        final long[] assets = assetScratch;
        final int[] assetCursor = {0};
        byAsset.forEachLong((asset, owners) -> assets[assetCursor[0]++] = asset);
        Arrays.sort(assets, 0, assetCount);

        for (int a = 0; a < assetCount; a++) {
            final long assetId = assets[a];
            final Long2ObjectHashMap<Long2LongHashMap> owners = byAsset.get(assetId);
            final int ownerCount = owners.size();
            if (ownerScratch.length < ownerCount) {
                ownerScratch = new long[ownerCount];
            }
            final long[] ownerIds = ownerScratch;
            final int[] ownerCursor = {0};
            owners.forEachLong((owner, delegates) -> ownerIds[ownerCursor[0]++] = owner);
            Arrays.sort(ownerIds, 0, ownerCount);

            for (int o = 0; o < ownerCount; o++) {
                final long owner = ownerIds[o];
                final Long2LongHashMap delegates = owners.get(owner);
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
                    consumer.accept(assetId, owner, delegate, delegates.get(delegate));
                }
            }
        }
    }

    /** Primitive callback for deterministic allowance iteration. */
    @FunctionalInterface
    public interface AllowanceConsumer {
        void accept(long assetId, long ownerId, long delegateId, long allowance);
    }
}
