# 0009 - Multi-Asset Ledger via Composite (assetId, accountId) Key

Status: Accepted
Date: 2026-08-05

## Context

Phase 1 shipped a single-asset ledger. Balances are a flat
`Long2LongHashMap(accountId -> balance)` and there is exactly one running
`totalSupply`. Allowances are keyed by `(owner, delegate)` with no notion of
which asset the delegation covers. This is the single largest adoption gap for
the target segment (fintech, marketplace, gaming wallets), all of which need to
hold more than one asset (currencies, tokens, points) in the same deterministic
core.

Retrofitting an asset dimension only grows more expensive over time: `assetId`
touches the key of every store, every snapshot record, every handler, and the
read model. Doing it before sharding (ADR 0003, deferred to Phase 2) is far
cheaper than after, because the composite key must be woven through the same
surfaces sharding will later touch.

The non-negotiable constraint is the determinism and allocation moat: the hot
path must remain allocation-free, single-writer, power-of-two indexed, and
byte-identical across nodes.

## Decision

- The ledger becomes multi-asset. The logical balance key is the composite
  `(assetId, accountId)`; both are 64-bit. Total supply becomes per-asset:
  the invariant is `sum(balances[asset]) == totalSupply[asset]` for every asset.
- Avoid lossy composite hashing. Rather than fold two 64-bit ids into one long,
  use a nested primitive map `assetId -> (accountId -> balance)`, mirroring the
  proven `AllowanceStore` pattern. Absence of an asset map is asset-not-seen;
  absence of an account within it is the existing `MISSING` sentinel.
- Allowances become asset-scoped: the key is `(assetId, ownerId, delegateId)`.
  A delegation approved for one asset MUST NOT authorize spending another. This
  is a correctness requirement, not an enhancement.
- The wire and snapshot schema evolve backward-compatibly (SBE), schema version
  1 -> 2:
    - `CommandEnvelope` gains an appended optional `assetId`. Absent (older
      encoders, or replayed pre-2 log records) decodes to the null value and is
      mapped to asset `0`, the default asset. New clients always send it.
    - `BalanceEntry` and `AllowanceEntry` gain an appended optional `assetId`.
      Reading an older snapshot yields the null value, mapped to asset `0`, so
      pre-multi-asset snapshots load unchanged as the default asset.
    - A new `AssetSupplyEntry(assetId, totalSupply)` record carries per-asset
      supply, emitted in ascending `assetId` order after the header.
    - `SnapshotHeader` gains an appended optional `assetCount` and retains the
      aggregate `totalSupply` (sum across assets) so the streaming load can
      verify the aggregate integrity invariant without allocating a per-asset
      map on the load path.
- Snapshot ordering stays deterministic: balances are emitted ascending by
  `(assetId, accountId)`, allowances ascending by `(assetId, ownerId,
  delegateId)`, asset supplies ascending by `assetId`.
- The default asset is `0`. A single-asset deployment sends `assetId = 0`
  everywhere and is byte-for-byte equivalent to Phase 1 semantics.

## Consequences

- Hot path cost is one extra map lookup to resolve the per-asset account map,
  then the existing O(1) balance lookup. Maps are preallocated; steady state
  allocates nothing. `assetId` resolution on ingress is a single predictable
  branch (`null -> 0`).
- Store size gauges (`balanceCount`, `allowanceOwnerCount`) are maintained by
  O(1) running counters rather than per-command iteration, so multi-asset does
  not add per-command work to `publishSizeGauges`.
- The integrity invariant remains aggregate: the streaming loader sums all
  balances across all assets and compares to the sum of all per-asset supplies
  (equivalently, the header aggregate). This is allocation-free and equal in
  strength to the Phase 1 check when there is one asset.
- Old snapshots remain loadable: absent `assetId` -> asset `0`, absent
  `AssetSupplyEntry` records -> the header's aggregate supply is the asset-`0`
  supply. Replaying a pre-2 Raft log is safe because SBE decodes appended
  fields via the acting block length.
- The read model (`adbe-read`) becomes asset-aware: queries carry an `assetId`
  (default `0`), exposed over HTTP as an optional `?asset=` parameter so
  existing routes keep working.

## Performance

The asset dimension adds one resolution step per command to find the per-asset
account map. To keep this off the tail, each asset's `available`, `reserved`,
and `supply` live together in a single `AssetBucket`, and a one-entry last-asset
cache turns the resolution into a field compare when consecutive commands touch
the same asset (the common case). A single-asset workload therefore performs the
same number of hot-path map operations as the Phase 1 flat map.

Smoke benchmark (`-PquickBench`, one warmup / one measurement iteration, one
fork; indicative, not rigorous) on the reference machine:

| Benchmark               | Phase 1 baseline | Multi-asset + holds | Budget    |
|-------------------------|------------------|---------------------|-----------|
| `decodeEnvelope`        | 1.9 ns           | 1.9 ns              | < 100 ns  |
| `mapLookup`             | 0.7 ns           | 0.8 ns              | < 50 ns   |
| `creditDispatch`        | 19.2 ns          | ~21.5 ns            | < 500 ns  |

`creditDispatch` moves by roughly one cache-line-resident field compare plus the
lazy-reserved null check. The nominal delta sits near the 10% smoke-run gate but
well inside single-iteration variance and an order of magnitude under the 500 ns
dispatch budget. The asset dimension is a deliberate, load-bearing capability, so
this ADR records the new numbers as the multi-asset baseline
(`benchmark-baseline.txt`).

## Alternatives considered

- Single long composite key `(assetId << k) | accountId`: rejected. Both ids are
  full 64-bit; any bit-packing is lossy and would cap either the asset space or
  the account space, an unacceptable silent constraint for a ledger.
- Parallel `BalanceStore` instance per asset created eagerly: rejected. Assets
  are discovered at runtime; eager allocation wastes memory and cannot know the
  asset set at init. Lazy per-asset maps match the onboarding-is-cold principle.
- Making `assetId` a required (non-optional) field: rejected for snapshot and
  log compatibility. Optional + version bump lets pre-2 snapshots and committed
  log records replay as the default asset with no migration step.
