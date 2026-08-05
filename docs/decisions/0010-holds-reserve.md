# 0010 - Two-Phase Holds: RESERVE / CAPTURE / RELEASE

Status: Accepted
Date: 2026-08-05

## Context

Phase 1 spending is single-phase: a `DEBIT` or `TRANSFER` removes funds
immediately. The target segment (marketplace, gaming, payment authorization)
needs a two-phase model: earmark funds now, settle or return them later. A
marketplace holds a buyer's funds at order time and captures them on fulfilment
or releases them on cancellation; a game holds an entry stake and settles the
pot at match end. Without holds, integrators must emulate escrow with a shadow
account and non-atomic transfers, which is both error-prone and a determinism
hazard.

Holds also produce a rich, honest signal for the downstream AI risk layer
(reserve velocity, capture/release ratio, expiring holds), which single-phase
debits cannot express.

## Decision

- Split each account's balance into two buckets per asset: `available`
  (spendable) and `reserved` (held). The per-asset invariant becomes
  `sum(available + reserved) == totalSupply`. Reserved funds never leave the
  owner's total until captured.
- Add three commands (`CommandType` 7, 8, 9):
    - `RESERVE(asset, account, amount)` - move `amount` from `available` to
      `reserved` on one account. Fails `INSUFFICIENT_BALANCE` if `available` is
      too low, `INVALID_ACCOUNT` if the account does not exist.
    - `RELEASE(asset, account, amount)` - move `amount` from `reserved` back to
      `available`. Fails `INSUFFICIENT_RESERVED` if too little is held.
    - `CAPTURE(asset, from, to, amount)` - settle `amount` of `from`'s reserved
      funds into `to`'s available balance. Total supply is unchanged (funds move
      within the ledger). Capturing to self returns the held funds to available.
      Fails `INSUFFICIENT_RESERVED` if too little is held.
- Add a `StatusCode` value `INSUFFICIENT_RESERVED`.
- The `CommandResult`, the dedup cache, and the snapshot carry an optional
  `resultReserved` / `reserved` field so a reserving command's outcome is
  reported, deduplicated, and persisted exactly like balance and allowance.
- Reserved balances are stored in a lazily-created per-asset map inside the
  same `AssetBucket` as `available` (ADR 0009), so accounts that never reserve
  pay no reserved-map memory and the hot path is unaffected for non-hold
  commands.

## Consequences

- Total supply is conserved across the full reserve/release/capture lifecycle;
  the snapshot integrity check (`sum(available + reserved) == aggregate supply`)
  covers held funds.
- Snapshots serialise `reserved` as an optional field on `BalanceEntry`; a
  pre-holds snapshot decodes it as zero, so older snapshots load unchanged.
- `CAPTURE` is a settlement transfer, not a withdrawal: funds move from `from`'s
  reserved bucket to `to`'s available bucket. A withdrawal of held funds is
  expressed as `RELEASE` followed by `DEBIT`, or `CAPTURE` to a treasury
  account. This keeps every command supply-preserving except the explicit
  `CREDIT` / `DEBIT` mint/burn operations.
- Expiry and time-boxed holds are intentionally out of scope here. The core has
  no clock on the hot path; a future timer-driven auto-release would flow
  through the cluster timer service (deterministic), tracked separately.

## Alternatives considered

- A separate escrow account per hold: rejected. It multiplies account count,
  needs a non-atomic two-transfer protocol, and loses the clean
  available/reserved split that the risk layer wants.
- Encoding holds as negative available with a side flag: rejected. It corrupts
  the non-negative-balance invariant that lets `Long.MIN_VALUE` serve as the
  absent-account sentinel.
