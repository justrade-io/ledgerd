# 0001 - Architecture: Deterministic Clustered Balance Engine

Status: Accepted
Date: 2026-07-26

## Context

LEDGERD Core must be a strongly consistent, ultra-low-latency balance and
delegated-spending engine. Traditional RDBMS ledgers suffer lock contention and
latency growth under load; single-node in-memory engines are fast but not fault
tolerant. See tmp/PRD.md and tmp/TDD.md (historical planning drafts, not tracked in this repository; /docs/decisions is the authoritative record).

## Decision

- Implement the core as a single Aeron `ClusteredService` (`BalanceService`)
  running on one `ClusteredServiceAgent` thread (single-writer, no locks).
- Keep all business logic in a cluster-independent `BalanceEngine` so it can be
  unit and replay tested without Aeron.
- Wire format and snapshots use SBE (module `protocol`), little-endian,
  fixed field order. Optional fields (`presence="optional"`) enable
  backward-compatible schema evolution, including snapshots.
- Idempotency is intrinsic: a per-client `DedupTable` of ring buffers caches the
  most recent N results and is included in snapshots.
- Money and allowances are 64-bit signed `long` with overflow checks that return
  `StatusCode.OVERFLOW` (no exceptions for control flow).
- Determinism is enforced by convention and a Checkstyle rule set
  (`core/config/checkstyle/determinism.xml`) banning clocks, randomness,
  unordered maps, `Optional`, `BigDecimal`, streams, and blocking primitives in
  the core.

## Module layout (hybrid)

- `protocol` - SBE schema and generated codecs (dependency-only).
- `core` - engine, handlers, dedup, snapshot, telemetry.
- `launcher` - Aeron component bootstrap.
- `tests` - unit, integration, and test fixtures (test-only cluster client).

Each module organises sources by the copilot-instructions package structure
(`config`, `codec`, `core`, `collections`, `persistence`, `telemetry`, `util`).

## Consequences

- No external I/O, no local clock, and no random/GUID generation in the core;
  identifiers arrive from the Edge via the command envelope.
- Cross-node byte-identical state is verifiable via deterministic snapshot
  serialization (keys sorted before emission).
- Aeron/Agrona 2.x require the JVM flags
  `--add-opens java.base/jdk.internal.misc=ALL-UNNAMED` and
  `--add-opens java.base/sun.nio.ch=ALL-UNNAMED`.
