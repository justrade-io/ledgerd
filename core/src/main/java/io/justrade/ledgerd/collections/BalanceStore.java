package io.justrade.ledgerd.collections;

import java.util.Arrays;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;

/**
 * Single-writer store of account balances per asset, plus the running total
 * supply per asset.
 *
 * <p>The logical key is the composite {@code (assetId, accountId)}. Both are
 * 64-bit, so a nested structure is used rather than lossy composite hashing:
 * each asset owns an {@link AssetBucket} holding its {@code available} and
 * (lazily) {@code reserved} account maps plus its running supply. A single
 * last-asset cache collapses the per-command asset resolution to a field compare
 * on the common case of a burst of commands for the same asset, so a single-
 * asset workload pays no extra hot-path lookup versus a flat map.
 *
 * <p>Each account carries two buckets: {@code available} (spendable) and
 * {@code reserved} (held by a two-phase reservation). The invariant is
 * {@code sum(available + reserved) == totalSupply} for every asset. Absence of
 * an account is {@link #MISSING}; a balance is always non-negative, so
 * {@link Long#MIN_VALUE} is a safe sentinel that can never be a real value.
 */
public final class BalanceStore {

    /** Sentinel returned by {@link #rawGet(long, long)} when the account does not exist. */
    public static final long MISSING = Long.MIN_VALUE;

    /** Default asset used when a command or snapshot record omits an asset id. */
    public static final long DEFAULT_ASSET = 0L;

    private static final float LOAD_FACTOR = 0.65f;

    /** Per-asset state: available balances, lazily-created reserved balances, and supply. */
    private static final class AssetBucket {
        private final Long2LongHashMap available;
        private Long2LongHashMap reserved;
        private long supply;

        AssetBucket(final int accountCapacity) {
            this.available = new Long2LongHashMap(accountCapacity, LOAD_FACTOR, MISSING);
        }

        Long2LongHashMap reserved(final int accountCapacity) {
            if (reserved == null) {
                reserved = new Long2LongHashMap(accountCapacity, LOAD_FACTOR, MISSING);
            }
            return reserved;
        }
    }

    private final Long2ObjectHashMap<AssetBucket> byAsset;
    private final int accountCapacity;
    private long totalAccounts;

    private long cachedAsset;
    private AssetBucket cachedBucket;

    private long[] assetScratch = new long[0];
    private long[] accountScratch = new long[0];

    public BalanceStore(final int initialCapacity) {
        this.accountCapacity = initialCapacity;
        this.byAsset = new Long2ObjectHashMap<>(16, LOAD_FACTOR);
    }

    private AssetBucket bucketFor(final long assetId) {
        if (cachedBucket != null && cachedAsset == assetId) {
            return cachedBucket;
        }
        final AssetBucket bucket = byAsset.get(assetId);
        if (bucket != null) {
            cachedAsset = assetId;
            cachedBucket = bucket;
        }
        return bucket;
    }

    private AssetBucket bucketForCreate(final long assetId) {
        if (cachedBucket != null && cachedAsset == assetId) {
            return cachedBucket;
        }
        AssetBucket bucket = byAsset.get(assetId);
        if (bucket == null) {
            bucket = new AssetBucket(accountCapacity);
            byAsset.put(assetId, bucket);
        }
        cachedAsset = assetId;
        cachedBucket = bucket;
        return bucket;
    }

    /** Returns {@code true} if the account exists for the asset. */
    public boolean exists(final long assetId, final long accountId) {
        return rawGet(assetId, accountId) != MISSING;
    }

    /** Returns the available balance or {@link #MISSING} if the account does not exist. */
    public long rawGet(final long assetId, final long accountId) {
        final AssetBucket bucket = bucketFor(assetId);
        return bucket == null ? MISSING : bucket.available.get(accountId);
    }

    /** Returns the reserved (held) balance for an account; zero if none. */
    public long reserved(final long assetId, final long accountId) {
        final AssetBucket bucket = bucketFor(assetId);
        if (bucket == null || bucket.reserved == null) {
            return 0L;
        }
        final long value = bucket.reserved.get(accountId);
        return value == MISSING ? 0L : value;
    }

    /** Sets the available balance for an account, creating it if necessary. */
    public void set(final long assetId, final long accountId, final long balance) {
        final long prior = bucketForCreate(assetId).available.put(accountId, balance);
        if (prior == MISSING) {
            totalAccounts++;
        }
    }

    /** Sets the reserved (held) balance for an account. */
    public void setReserved(final long assetId, final long accountId, final long value) {
        bucketForCreate(assetId).reserved(accountCapacity).put(accountId, value);
    }

    /**
     * Restores an account's available balance to a prior value captured before a
     * linked-chain apply. When {@code priorValue} is {@link #MISSING}, the account
     * was auto-created by the rolled-back chain and is removed, decrementing the
     * account count. Transfer legs only mutate {@code available}, so this is the
     * only undo primitive the batch path needs.
     */
    public void restoreAvailable(final long assetId, final long accountId, final long priorValue) {
        final AssetBucket bucket = bucketFor(assetId);
        if (bucket == null) {
            return;
        }
        if (priorValue == MISSING) {
            if (bucket.available.remove(accountId) != MISSING) {
                totalAccounts--;
            }
        } else {
            bucket.available.put(accountId, priorValue);
        }
    }

    /** Returns the running total supply for an asset; zero if the asset is unseen. */
    public long totalSupply(final long assetId) {
        final AssetBucket bucket = bucketFor(assetId);
        return bucket == null ? 0L : bucket.supply;
    }

    /** Adjusts total supply for an asset; used by credit ({@code +}) and debit ({@code -}). */
    public void adjustTotalSupply(final long assetId, final long delta) {
        bucketForCreate(assetId).supply += delta;
    }

    /** Restores an asset's total supply directly during snapshot load. */
    public void setSupply(final long assetId, final long value) {
        bucketForCreate(assetId).supply = value;
    }

    /** Total number of accounts across all assets. */
    public int size() {
        return (int) totalAccounts;
    }

    /** Number of distinct assets seen. */
    public int assetCount() {
        return byAsset.size();
    }

    /** Sum of every asset's total supply; cold snapshot-write path only. */
    public long aggregateSupply() {
        final long[] sum = {0L};
        byAsset.forEachLong((asset, bucket) -> sum[0] += bucket.supply);
        return sum[0];
    }

    /** Removes all state; used before a snapshot load. */
    public void clear() {
        byAsset.clear();
        totalAccounts = 0L;
        cachedAsset = 0L;
        cachedBucket = null;
    }

    /**
     * Emits every balance entry in ascending {@code (assetId, accountId)} order.
     *
     * <p>Deterministic iteration is mandatory for snapshots. This is a cold path
     * (snapshot only), so extracting and sorting keys is acceptable.
     */
    public void forEachSorted(final BalanceConsumer consumer) {
        final int assetCount = byAsset.size();
        if (assetScratch.length < assetCount) {
            assetScratch = new long[assetCount];
        }
        final long[] assets = assetScratch;
        final int[] assetCursor = {0};
        byAsset.forEachLong((asset, bucket) -> assets[assetCursor[0]++] = asset);
        Arrays.sort(assets, 0, assetCount);

        for (int a = 0; a < assetCount; a++) {
            final long assetId = assets[a];
            final AssetBucket bucket = byAsset.get(assetId);
            final Long2LongHashMap accounts = bucket.available;
            final int accountCount = accounts.size();
            if (accountScratch.length < accountCount) {
                accountScratch = new long[accountCount];
            }
            final long[] accountIds = accountScratch;
            final int[] cursor = {0};
            accounts.forEachLong((account, balance) -> accountIds[cursor[0]++] = account);
            Arrays.sort(accountIds, 0, accountCount);
            for (int i = 0; i < accountCount; i++) {
                final long accountId = accountIds[i];
                consumer.accept(assetId, accountId, accounts.get(accountId), reserved(assetId, accountId));
            }
        }
    }

    /** Emits every asset's total supply in ascending assetId order; cold snapshot path. */
    public void forEachSupplySorted(final SupplyConsumer consumer) {
        final int assetCount = byAsset.size();
        if (assetScratch.length < assetCount) {
            assetScratch = new long[assetCount];
        }
        final long[] assets = assetScratch;
        final int[] cursor = {0};
        byAsset.forEachLong((asset, bucket) -> assets[cursor[0]++] = asset);
        Arrays.sort(assets, 0, assetCount);
        for (int i = 0; i < assetCount; i++) {
            consumer.accept(assets[i], byAsset.get(assets[i]).supply);
        }
    }

    /** Primitive callback for deterministic balance iteration. */
    @FunctionalInterface
    public interface BalanceConsumer {
        void accept(long assetId, long accountId, long balance, long reserved);
    }

    /** Primitive callback for deterministic per-asset supply iteration. */
    @FunctionalInterface
    public interface SupplyConsumer {
        void accept(long assetId, long totalSupply);
    }
}
