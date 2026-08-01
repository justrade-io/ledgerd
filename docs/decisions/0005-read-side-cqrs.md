# 0005 - Read-Side: Eventually-Consistent Read Model over the Cluster

Status: Accepted
Date: 2026-07-29
Updated: 2026-08-01 (cluster-follower realization superseded by ADR 0007)

> Note: the cluster-follower realization described below (`ReadModelService`
> joining as a voting member) was removed by ADR 0007, which standardizes on the
> read replica node of ADR 0006. The rest of this ADR - the read bounded context,
> the HTTP query surface, the lock-free query gateway, and the
> eventually-consistent contract - remains in force.

## Context

Phase 1 shipped a write-only command path: every state change is a
`CommandEnvelope` committed through Raft and applied by `BalanceService` on a
single thread. There is no way to read a balance, an allowance, or the total
supply without submitting a command, and the `CommandResult` egress carries only
the acting account's balance (the sender), never the recipient's. A read model
built from egress alone would therefore be incomplete.

Real integrations need to read balances and allowances, often in batches, at a
much higher rate than they write. These reads do not need to be linearizable;
bounded staleness (a few microseconds to milliseconds behind the leader) is
acceptable for user-facing balance display.

## Decision

- Introduce a new module `adbe-read`, a read (CQRS query) bounded context. It may
  depend on `adbe-core` (unlike `adbe-client`, which ADR 0004 restricts to
  `adbe-protocol`), because it must apply the same deterministic command logic to
  reproduce state.
- The read model is a cluster follower. `ReadModelService` implements
  `io.aeron.cluster.service.ClusteredService` and composes the core
  `BalanceService`, delegating every cluster callback to it. Because it applies
  the identical committed log through the same `BalanceEngine`, its in-memory
  state is byte-identical to the leader's and complete (it observes both sides of
  every `TRANSFER`, unlike egress).
- The feed is normal cluster replication. A joining follower catches up via Aeron
  Archive log replay and then follows the live log; this is the managed
  realization of a replay-then-merge, so no raw log parsing is hand-rolled.
- Reads are served on the single service thread via
  `ClusteredService.doBackgroundWork(long nowNs)`. Query requests arrive from the
  HTTP boundary on a lock-free `ManyToOneRingBuffer`; the service thread drains
  them, looks up its own stores (single-writer safe, no concurrent map access),
  and writes answers to a `OneToOneRingBuffer`. A separate dispatcher thread
  completes the pending HTTP responses by correlation id.
- The external surface is HTTP REST via Netty at the boundary only: `GET
  /balance/{id}`, `POST /balances` (batch), `GET /allowance/{owner}/{delegate}`,
  `GET /supply`, `GET /healthz`. JSON and heap/pooled buffers are acceptable here
  because this is Edge code, never the deterministic core hot path.

## Consequences

- Reads are eventually consistent with bounded staleness. `GET /supply` reflects
  the follower's applied position, not necessarily the leader's latest command.
- The deterministic core (`adbe-core`) is unchanged. Read concerns live entirely
  in `adbe-read`; the query drain runs in `doBackgroundWork`, which cannot affect
  the replicated log or snapshots.
- A missing account returns HTTP 200 with `exists=false` (not 404) so batch
  responses stay uniform.
- Read scaling is achieved by running additional read-enabled followers; the
  design allows it but Phase 1 ships a single read node.

## Out of scope

- Strong / linearizable reads (no query through Raft).
- Sharding and cross-shard reads (deferred by ADR 0003).
- Authentication, authorization, and rate limiting on the read API (Edge
  concerns; a prerequisite before any production exposure).
- A shipped read client SDK; the REST surface is the Phase 1 contract.
