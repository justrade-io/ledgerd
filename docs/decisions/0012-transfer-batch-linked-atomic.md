# 0012 - Transfer Batch with Linked Atomic Chains

Status: Accepted
Date: 2026-09-03

## Context

The command path is write-only and one command per message: a `TRANSFER` moves
funds between two accounts, and each message pays the full consensus/replication
cost once. Real ledger workloads need two things this does not provide:

1. **Throughput under load.** A burst of transfers benefits from amortizing the
   per-message consensus and egress cost over many legs, the same batching
   argument TigerBeetle makes for its `create_transfers` interface.
2. **Atomic multi-leg transactions.** Currency exchange, multi-debit /
   multi-credit, and split payments are all "one logical operation, several
   legs" that must be all-or-nothing, or a failed intermediate leg corrupts the
   ledger.

This is single-node atomicity. It is distinct from the cross-shard atomicity
deferred to Phase 2 (ADR 0003), and it is in fact the intra-node building block
that future cross-shard coordination will compose.

## Decision

- Add a `TransferBatch` wire message (template 3) and a `TransferBatchResult`
  (template 4), schema version 4 -> 5. A batch carries one `(clientId,
  clientSeq)` idempotency key and a group of transfer legs; each leg has
  `fromId`, `toId`, `amount`, `assetId`, and a `linked` flag. Additive only: no
  existing message changes, so pre-5 logs and snapshots replay unchanged.
- **Scope is transfer-only.** Linked atomicity applies only to transfer legs,
  not to arbitrary command types, mirroring TigerBeetle's transfer-only `linked`
  semantics. Allowance, reserve, and supply are never touched by a batch, which
  is what makes the rollback narrow and cheap.
- **Linked chains.** Contiguous runs of `linked` legs form an all-or-nothing
  chain; the last leg of a chain has `linked = false`. A trailing `linked` flag
  (chain overflow) and a batch larger than `maxBatchSize` are rejected with the
  new `INVALID_CHAIN` status. When a leg fails, the whole chain rolls back and
  every leg in the chain returns the first failure's status. Legs in other
  chains are unaffected.
- **Rollback via a narrow undo frame, not a general mutation log.** A transfer
  leg only mutates an account's `available` balance, so the engine records the
  two before-images it is about to overwrite and restores them in reverse on
  failure. A single `BalanceStore.restoreAvailable` primitive handles both
  value restoration and removal of an auto-created recipient.
- **Batch idempotency at the batch level.** A batch is one dedup unit at
  `(clientId, clientSeq)`; resubmission replays the cached per-leg results. The
  batch dedup ring is separate from the single-command dedup ring, so the
  single-command hot path is untouched.
- **Event journal.** Committed legs emit their staged transfer events; failed
  legs emit one `CommandRejectedEvent` each. Events are staged per chain and
  truncated on rollback, so a rolled-back chain never leaks events.

## Consequences

- The batch path allocates nothing in steady state: the leg scratch, undo frame,
  per-leg results, and event staging are all preallocated to `maxBatchSize`
  (a power of two, default 1024).
- `SnapshotManager` grows a `BatchDedupEntry` record (template 16) and a
  `batchDedupCount` header field, so batch idempotency survives a restart
  exactly as single-command idempotency does. The record buffer is sized to the
  largest possible batch dedup entry.
- The read replica replays batches through the same `BalanceEngine.processBatch`,
  so it derives identical state without a parallel code path.
- `write-client` gains `submitTransferBatch` with a pooled, verbatim-resend
  `PendingBatchCommand` and a `BatchResultHandler`.

## Performance

Smoke run (`-PquickBench`) on the reference machine:

| Benchmark              | batchSize | Score    | Note |
|------------------------|-----------|----------|------|
| `creditDispatch`       | -         | ~24.5 ns | unchanged from the ADR 0011 baseline |
| `batchApply`           | 16        | ~340 ns  | ~21 ns per leg |
| `batchApply`           | 256       | ~5.7 us  | ~22 ns per leg |
| `linkedChainApply`     | 16        | ~341 ns  | undo recording adds no material cost |

The single-command hot path is untouched, so the ADR 0011 baseline holds. Batch
apply scales linearly with leg count, which is the point: one message amortizes
the per-message consensus cost across the whole batch.

## Alternatives considered

- **General `CommandBatch` with a mutation log across every store.** Rejected.
  Atomicity over arbitrary command types (transfer + approve + reserve) is rarely
  needed and would require undo instrumentation in `BalanceStore`,
  `AllowanceStore`, and the supply accounting. Transfer-only chains keep the
  undo surface to a single `available` restore.
- **Batching without `linked`.** Rejected as the sole design. Independent-leg
  batching captures the throughput win but not the atomic multi-leg use case;
  both are needed and are delivered together by one message.
- **Compensating transfers as rollback.** Rejected. Compensation is a new
  business event that can itself fail; it is not sound for automatic
  all-or-nothing semantics. The undo frame is the correct, deterministic
  mechanism.
