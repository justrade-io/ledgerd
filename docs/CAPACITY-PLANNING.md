# Capacity Planning

How to size `CoreConfig` and the JVM heap for a target account/asset/client
workload, and how to map that onto EC2 instance shapes.

> Status: derived from the code and from the measured baseline in
> `deploy/aws/README.md`. The formulas are deterministic, but the worked tables
> are guidance, not a hard SLA. Validate before production using the plan in
> [Validation plan](#validation-plan).

## 1. The scaling model

Two facts from the code drive everything:

1. **`accountCapacity` is per asset, not total.** `BalanceStore` preallocates one
   `Long2LongHashMap(accountCapacity)` inside each `AssetBucket` the moment an
   asset is first touched (`BalanceStore.java:86`, `:42`). Every asset pays the
   same capacity, so one hot asset forces all assets to pay for its size:

   ```
   balanceMemory = assets * accountCapacity * 16 B
   ```

   This holds even if most assets hold only a handful of accounts. Size
   `accountCapacity` for the peak of the single hottest asset, not for the total
   account count and not uniformly.

2. **`clientId` is a write client, not an account.** The dedup table keys on the
   write-side SDK / gateway id, so dedup rings are per write client (a small,
   bounded count), while accounts live in the balance maps (16 B each). A million
   accounts do not create a million dedup rings.

The balance key is the composite `(assetId, accountId)` (ADR 0009), both 64-bit,
with no hard count limit. The real limits are preallocated memory and the
single-writer CPU ceiling.

## 2. Memory model

All stores are on-heap arrays (`new long[]` / `new byte[]`) and count against
`-Xmx`. Aeron term buffers, socket buffers, and the domain event ring are
off-heap (native) and count against total RAM but not heap.

```
heapLive ~=
    A * accountCapacity * 16 * (holdsEnabled ? 2 : 1)     (1) balance maps
  + A * allowanceOwnerCapacity * 12                        (2) allowance owner maps
  + 2 * dedupClientCapacity * 12                           (3) dedup client maps
  + CmdClients  * 51 * 1024                                (4) command dedup rings
  + BatchClients * batchDedupWindow * maxBatchSize * 10    (5) batch dedup rings
  + 4 * 1024 * 1024                                        (6) fixed scratch

heap = heapLive / 0.65      (ZGC headroom ~35%)
```

| Term | Source | Factor |
|------|--------|--------|
| (1) balance map | `Long2LongHashMap` = parallel `keys[]` + `values[]` | 16 B per slot; x2 when holds are used (lazy `reserved` map) |
| (2) allowance map | `Long2ObjectHashMap(ownerCapacity)` = 8 B key + 4 B ref (compressed oops) | 12 B per owner slot |
| (3) dedup client map | two `Long2ObjectHashMap(dedupClientCapacity)` | ~1.6 MB at the 2^16 default |
| (4) command ring | `DedupRing` = 5x`long[1024]` + `short[1024]` + `byte[1024]` | ~51 KB per write client |
| (5) batch ring | `BatchDedupRing` = `long[W*M]` + 2x`byte[W*M]` (`BatchDedupRing.java:40-42`) | 10 B per leg slot; W = batchDedupWindow, M = maxBatchSize |
| (6) scratch | `SnapshotManager`, `BatchOutcome`, `TransferBatchHandler` | ~2-4 MB fixed |

The reference sizes use compressed oops (4-byte references), which the JVM
enables by default while the heap stays under 32 GB. Keep the heap under ~28 GB
to retain it; above that, allowance and dedup maps grow ~33%.

### Load factor and safe fill

`BalanceStore` and `AllowanceStore` both use `LOAD_FACTOR = 0.65`
(`BalanceStore.java:37`). Agrona resizes when `size` exceeds
`capacity * loadFactor`, so the safe account count per asset is `0.65 * capacity`.

| accountCapacity | Bytes / asset (available) | Safe accounts / hottest asset |
|-----------------|---------------------------|-------------------------------|
| 2^16 = 65,536 | 1 MB | ~42K |
| 2^17 = 131,072 | 2 MB | ~85K |
| 2^18 = 262,144 | 4 MB | ~170K |
| 2^19 = 524,288 | 8 MB | ~340K |
| 2^20 = 1,048,576 (default) | 16 MB | ~680K |
| 2^21 = 2,097,152 | 32 MB | ~1.36M |
| 2^22 = 4,194,304 | 64 MB | ~2.72M |
| 2^23 = 8,388,608 | 128 MB | ~5.45M |

### Batch dedup ring is the wildcard

The batch ring is the one term that can exhaust a heap on its own, and it scales
with the number of write clients that submit batches, not with accounts:

| maxBatchSize x batchDedupWindow | Bytes per batch client |
|---------------------------------|------------------------|
| 256 x 1024 | ~2.5 MB |
| 1024 x 1024 | ~10 MB |
| 4096 x 1024 (deploy default) | ~40 MB |

`batchDedupWindow` only needs to cover a client's `maxBatchInFlight`, never
`maxBatchSize`; setting it to `maxBatchSize` (for example 4096 x 4096) OOMs a
small heap (`OPERATIONS.md`).

## 3. CPU model

The write hot path is single-writer: one `ClusteredServiceAgent` thread runs the
whole `BalanceEngine`. The measured ceiling is ~1.7M legs/s on that one thread
(`deploy/aws/README.md`). More vCPUs do not raise write throughput; they improve
tail latency (dedicated media-driver threads) and let the read replica or event
journaler run alongside.

| Media mode | Threads | Fits |
|------------|---------|------|
| `SHARED_NETWORK` | 2 | 4 vCPU (deploy default) |
| `DEDICATED` | 3 | 8+ vCPU |

```
4 vCPU  = media(2) + consensus(1) + service(1)          -> no journaler
8 vCPU  = media(3) + consensus(1) + service(1) + journaler(1) -> comfortable
```

## 4. Sizing method

1. Pick the peak account count of the single hottest asset, `P`.
2. `accountCapacity = pow2ceil(P / 0.65)`.
3. Balance memory = `assets * accountCapacity * 16` (x2 with holds).
4. Pick `maxBatchSize` and `batchDedupWindow` from the number of write clients
   that submit batches; batch memory = `batchClients * window * maxBatchSize * 10`.
5. `heap = heapLive / 0.65`, capped under ~28 GB for compressed oops; add 1-2 GB
   of native memory to the instance RAM budget for Aeron and the OS.

## 5. Server mapping

Assumptions: holds off, 32 single-command write clients, negligible allowances,
ZGC with live data under 65% of heap.

| Instance | Heap | accountCapacity | Peak / hottest asset | Example (assets x total, Pareto) | Batch clients @ 1024 | Est. live |
|----------|------|-----------------|----------------------|----------------------------------|----------------------|-----------|
| 4 vCPU / 8 GB | 2g (deploy default) | 2^20 (16 MB) | ~680K | 32 x ~2M | 16 (160 MB) | ~680 MB |
| 4 vCPU / 8 GB (account-heavy) | 3-4g | 2^21 (32 MB) | ~1.36M | 16 x ~1M | 16 (160 MB) | ~700 MB |
| 8 vCPU / 16 GB | 8g | 2^21 (32 MB) | ~1.36M | 64 x ~5M | 32 (320 MB) | ~2.4 GB |
| 8 vCPU / 16 GB (asset-heavy) | 8g | 2^20 (16 MB) | ~680K | 256 x ~10M | 32 (320 MB) | ~4.4 GB |
| 8 vCPU / 32 GB | 24g (cap < 32g) | 2^22 (64 MB) | ~2.72M | 128 x ~20M | 64 (640 MB) | ~8.8 GB |
| 8 vCPU / 32 GB (throughput) | 24g | 2^21 (32 MB) | ~1.36M | 128 x ~5M | 64 @ 4096 (2.5 GB) | ~6.6 GB |

"Peak / hottest asset" is the ceiling for a single asset, not the total. The
"Example" column distributes accounts Pareto-style (the pattern
`ParetoAccountSampler` models): the hottest asset approaches the ceiling while the
rest stay sparse.

## 6. Workload archetypes

| Segment | Assets | Peak accounts / hottest asset | Notes |
|---------|--------|-------------------------------|-------|
| Fintech wallet | 8-32 | 500K-1M | accountCapacity 2^20-2^21 |
| Marketplace | 10-50 | 100K-500K | currency + escrow + refund buckets |
| Gaming wallet | 10-100 | 1M+ | currency + item tokens, heavy tail |

## 7. Validation plan

The model is deterministic but the tables are unproven at scale. Validate each
claim before production. Existing harness covers some of it; the rest needs new
tests or benchmarks.

### What exists today

| Claim | Covered by | Command |
|-------|-----------|---------|
| Batch throughput ceiling ~1.7M legs/s | `BatchThroughputSoakTest` | `./gradlew soakTest` |
| Zero steady-state allocation | JMH `-prof gc`, `ParetoContentionSoakTest` GC bound | `./gradlew :core:jmh -Pjmh.profilers=gc` |
| Hot-path latency budget | `BalanceEngineBenchmark` + `jmhBudgetCheck` | `./gradlew :core:jmhBudgetCheck` |
| Snapshot round-trip at 16K accounts | `SnapshotBenchmark` (`accounts=1024,16384`) | `./gradlew :core:jmh` |
| Pareto hot/cold tail latency | `ParetoContentionSoakTest` (8 hot + 1000 cold) | `./gradlew soakTest` |

### Gaps to close (new work)

1. **Resize threshold and per-asset allocation.** A unit test driving
   `BalanceStore` and asserting `Long2LongHashMap.capacity()` / `resizeThreshold()`:
   one touched asset preallocates `accountCapacity`, inserting past
   `0.65 * accountCapacity` grows it. Verifies terms (1) and the safe-fill table.
2. **Account-scale sweep.** Extend the JMH harness (or add
   `CapacityScaleBenchmark`) with `@Param` account counts 100K / 500K / 1M,
   asserting `mapLookup` stays flat and `creditDispatch` does not cliff. Verifies
   the O(1) claim at scale.
3. **Asset-scale sweep.** `@Param` asset counts 1 / 8 / 32 / 128 with
   alternating asset ids, measuring the last-asset cache warm/cold delta. Verifies
   ADR 0009's single-extra-lookup claim and cache-miss cost.
4. **Memory footprint.** A JOL-based (`org.openjdk.jol`) or `Runtime`-measured
   test computing retained bytes for (assets x accountCapacity) and for
   `BatchDedupRing(batchDedupWindow, maxBatchSize)`, comparing against the 16 B /
   10 B factors. Verifies terms (1) and (5).
5. **Snapshot scale.** Extend `SnapshotBenchmark` `accounts` to 1M and add a
   multi-asset dimension, measuring write/read time and serialized size. Verifies
   snapshot cost at production account counts.
6. **Heap-constrained boot.** Launch a single-node cluster with a fixed `-Xmx`
   and a target config from the mapping table; assert it boots and applies a
   Pareto workload without OOM. Verifies the full `heap = heapLive / 0.65` formula
   end to end.

Suggested new files (kept out of the `check` gate where heavy):

```
tests/src/test/java/io/justrade/ledgerd/core/CapacityModelTest.java
core/src/jmh/java/io/justrade/ledgerd/bench/CapacityScaleBenchmark.java
```

Run order after adding them:

```bash
./gradlew test                                  # unit, includes CapacityModelTest
./gradlew :core:jmh -PquickBench                # smoke: scale sweep does not cliff
./gradlew :core:jmh -Pjmh.profilers=gc          # zero-allocation confirmation
./gradlew soakTest                              # tail latency + GC bound
```
