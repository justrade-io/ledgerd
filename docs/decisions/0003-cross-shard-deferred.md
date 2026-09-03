# 0003 - Sharding and Cross-Shard Atomicity Deferred to Phase 2

Status: Accepted
Date: 2026-07-26

## Context

PRD v2.0 (tmp/PRD.md sections 9.6, 20, 21; historical planning draft, not tracked in this repository) requires that Sharding and
Cross-Shard Atomicity be released together, because a `transfer` between two
accounts in different shards is a basic ledger use-case, not an edge case.
Open Question #3 (2PC vs Saga) must be answered before Phase 2 begins.

## Decision

- Phase 1 (this implementation) is single-shard only. No sharding, no
  cross-shard coordination, is implemented.
- Sharding and cross-shard atomicity are deferred to Phase 2 and MUST ship
  together. The choice between two-phase commit and saga is intentionally left
  open here and must be resolved in a dedicated ADR before Phase 2 coding.

## Consequences

- Phase 1 delivers full balance/allowance operations, idempotency, a reliable
  ACK protocol, controlled snapshots, and metrics for a single replicated state
  machine.
- The account-to-shard function `shard = hash(accountId) mod N` and any
  cross-shard protocol are out of scope until the follow-up ADR exists.

## Out of scope for Phase 1 (owned by other bounded contexts)

- Edge Gateway, authentication/authorization, client SDK reconnect.
- Long-term audit storage, analytics, reporting.
- Byzantine fault tolerance, multi-asset support, per-account rate limiting.
