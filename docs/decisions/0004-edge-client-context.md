# 0004 - Edge Client SDK is a Separate Bounded Context

Status: Accepted
Date: 2026-07-27

## Context

PRD v2.0 (tmp/PRD.md sections 2.1, 2.2) draws a hard boundary: the Core Service
executes a deterministic replicated state machine, while the Edge (Gateway, auth,
client SDK, retry policy) is a distinct bounded context that communicates with the
Core only through the fixed binary command contract.

A client SDK is nonetheless useful for driving and integrating with the Core. It
must not be allowed to leak Edge concerns (reconnect, retry, correlation,
backpressure policy) into the deterministic core.

## Decision

- Introduce a new module `adbe-client` that depends ONLY on `adbe-protocol`
  (the `CommandEnvelope` / `CommandResult` wire contract). It MUST NOT depend on
  `adbe-core`.
- `adbe-client` owns Edge-side concerns: leader-change handling, idempotent retry
  that reuses the original command id, asynchronous request/response correlation
  by command id, explicit backpressure signalling to the caller, and end-to-end
  latency measurement (HdrHistogram).
- Idempotent retry MUST reuse the same `commandId` on resend; this is the
  precondition for the Core's dedup guarantee (PRD 9.3, 9.4). The client never
  generates a new id for a retry.
- The client generates command ids and client sequence numbers (identifiers
  originate at the Edge, never in the Core, per PRD 8.3 and 14.1).

## Consequences

- The Core remains free of reconnect/retry/correlation logic and stays
  deterministic and Aeron-cluster-focused.
- The dependency direction is one-way: `adbe-client -> adbe-protocol`. Any change
  to retry, backpressure, or correlation policy is isolated to the Edge context.
- `adbe-client` is not a hot-path deterministic component; it may use the system
  clock for timeouts and latency measurement, which the Core may not.
- The test-only `ClusterTestClient` (in `adbe-tests` fixtures) remains a separate,
  minimal harness and is not the shipped client.

## Out of scope

- Authentication/authorization, rate limiting, and the public API surface remain
  Edge concerns outside this module.
- Multi-datacenter routing and read replicas are not addressed here.
