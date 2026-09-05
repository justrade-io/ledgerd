# 0013 - Read-Side Graph Query Engine

Status: Proposed
Date: 2026-09-05

## Context

The read side answers `QueryRequest` messages with a fixed set of point
lookups: `BALANCE`, `BATCH_BALANCE`, `ALLOWANCE`, and `TOTAL_SUPPLY` (see
`QueryResponder` and `QueryType` in `protocol/src/main/resources/messages.xml`).
Each query reads one scalar or one flat batch from the replicated projections
(`BalanceStore`, `AllowanceStore`). This is sufficient for balance display, but
it is not sufficient for the two workloads the read side is expected to serve
next:

1. **General graph queries.** The domain is a graph: an `AllowanceStore` entry
   is a directed, weighted edge `owner -> delegate` scoped by asset, and an
   account is a node with balance properties. Users want pattern questions such
   as "who can spend on behalf of account A, directly or through a chain of
   delegations", "which delegates hold the largest aggregate allowance across
   owners", or "are there delegation cycles". None of these are expressible as
   point lookups, and adding one `QueryType` per question does not scale.
2. **Risk analysis.** Ledgerd needs concentration and exposure signals: blast
   radius of a compromised account, accounts with the highest fan-in (many
   owners delegating to one delegate), dormant allowances (owner holds zero
   balance but authority is still granted), and indirect delegation chains.
   These are graph traversal and aggregation problems, not scalar reads.

The reference design studied for this ADR is `codebase-memory-mcp`
(`tmp/codebase-memory-mcp`), a structural-analysis graph store. It models
source code as a graph of typed nodes and edges, exposes a read-only Cypher
subset, and computes risk as a function of hop distance from a seed, plus
precomputed fan-in hotspots. Its architecture is the template for this decision;
its implementation details (SQLite, text parser, string-typed results) are not
copied.

## Decision

1. **Introduce a read-side `GraphQueryEngine` in the `read` module.** It is a
   query-time, read-only layer over the existing replicated projections
   (`BalanceStore` + `AllowanceStore`). It does not introduce a second copy of
   ledger state and does not touch the deterministic `core` module. The engine
   runs on the read replica's single agent thread, exactly where `QueryResponder`
   runs today, so single-writer guarantees hold with no synchronization.

2. **Two query surfaces, one executor.** The engine exposes both surfaces that
   the two purposes require:
   - A **structured graph query model** (pattern + filter + projection) with a
     tabular result, for general graph queries. This is the "full query feature"
     surface.
   - A set of **precomputed risk views** (`trace_allowance`, `hotspots`,
     `reachability`, `delegation_cycles`, `dormant_allowances`,
     `concentration`) implemented on top of the same traversal primitives. This
     is the risk-analysis surface, mirroring `trace_path`, `arch_hotspots`, and
     the dead-code `NOT EXISTS` pattern of `codebase-memory-mcp`.

   A text Cypher-like language is explicitly out of scope for the first cut; the
   structured query model is the canonical form and is what the wire protocol
   carries. A text language can later compile into the same query model.

3. **Risk is hop distance plus fan-in.** Blast radius is classified by hop
   distance from a seed account: hop 1 = CRITICAL, hop 2 = HIGH, hop 3 =
   MEDIUM, hop 4+ = LOW, the exact mapping `codebase-memory-mcp` uses
   (`cbm_hop_to_risk`). Concentration is fan-in: top-N accounts by the number of
   distinct owners delegating to them, the exact `arch_hotspots` query.

4. **Derived, scoped graph.** The queryable graph is derived from the existing
   stores and does not require materializing a new store:
   - Node `Account(assetId, accountId)` with properties `balance` (available),
     `reserved`, and `exists`.
   - Node `Asset(assetId)` with property `totalSupply`.
   - Edge `ALLOWANCE(owner -> delegate, assetId)` with property `amount`.
   `TRANSFER` edges are out of scope for the first cut. They are derived from
   the engine's apply-time outcome (`CommandOutcome`/`BatchOutcome` events that
   the read replica already receives during log replay), not from the domain
   event journal (ADR 0011), which is an opt-in, lossy audit stream and the
   wrong completeness source for a query projection.

5. **Two additive capabilities, no core hot-path change.** Forward traversal
   needs a per-owner delegate iterator, and backward traversal needs a reverse
   (delegate -> owners) view. Forward iteration is an additive `forEachDelegate`
   accessor on `AllowanceStore` (cold read path, never the consensus hot path).
   The reverse view is an `AllowanceReverseIndex` projection rebuilt lazily by
   the read replica from `AllowanceStore.forEachSorted` and invalidated on every
   `appliedPosition` advance; it is not added to `core`, so the deterministic
   single-command and batch hot paths are untouched.

6. **Bounded execution with fail-closed semantics.** Every query carries a
   wall-clock budget, a row ceiling, and a traversal depth cap, all checked in
   the expansion loop. A query that exceeds a bound returns a `TRUNCATED` or
   `TIMEOUT` status plus an explicit warning, never a silently truncated or
   empty result. Unsupported query features fail with `UNSUPPORTED`, never with
   an empty result set.

The detailed interface, query model, executor algorithm, risk views, and wire
protocol are specified in `docs/graph-query-engine.md`.

## Consequences

- **Positive**: General pattern queries and risk analysis are expressible
  without adding a `QueryType` per question. The read side stops being a
  point-lookup-only surface.
- **Positive**: No change to the deterministic core, the consensus log, or the
  snapshot format. The engine reads projections that are already replicated and
  byte-identical to the leader's state (ADR 0005, 0007).
- **Positive**: Allocation and CPU are confined to the read replica and bounded
  per query, so a query cannot stall replication or starve the point-lookup path.
- **Negative**: Inbound (delegate -> owners) traversal requires the read replica
  to build and cache a reverse projection, which is extra memory proportional to
  the number of allowances and a one-time O(allowances) build cost per applied
  position.
- **Negative**: The first cut excludes `TRANSFER` edges, so time-based risk
  (transfer volume, velocity, recently-active accounts) is deferred until a
  read-side transfer-edge projection is added to the log replay path.
- **Neutral**: The structured query model is less ergonomic than a text query
  language. The `read-client` SDK absorbs this with a fluent builder.

## Alternatives considered

- **Extend `QueryType` point by point.** Add one enum value and one encoder
  branch per new question (`TOP_DELEGATES`, `REACHABLE`, etc.). Rejected: the
  query surface grows without bound, each question ships its own encoding and
  validation, and cross-question composition (filter + aggregate + order) is
  impossible. This is the approach the current API already takes and is exactly
  the ceiling this ADR removes.
- **Full text Cypher parser on the read replica.** Copy `codebase-memory-mcp`'s
  lexer/parser (about 5000 lines of C) into Java. Rejected for the first cut:
  parsing untrusted text on the agent thread is a CPU and attack-surface cost
  that buys ergonomics, not new capability. The structured query model delivers
  the same semantics; a text language can be layered on later and compile to the
  same AST.
- **A separate graph store (SQLite or an in-memory graph) replicated from the
  log.** Copy `codebase-memory-mcp`'s store tier. Rejected: it duplicates ledger
  state, doubles the memory footprint, and introduces a second consistency
  boundary to keep in sync with the projections. The projections already hold
  the graph; the engine only needs to traverse them.
- **Add the reverse index to `core`'s `AllowanceStore`.** Rejected: it would run
  on the deterministic consensus hot path for every allowance write, even though
  no write path consumes it. Keeping it in `read` preserves the ADR 0011 hot-path
  budget.
