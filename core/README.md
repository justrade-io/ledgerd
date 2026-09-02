# core

The deterministic balance and delegated-spending engine: the heart of LEDGERD
and its hot path. Given a decoded command and a leader timestamp, it applies the
command to the balance and allowance stores, produces a deterministic result, and
records domain events, all without allocating, locking, or reading a clock.

## Responsibility

- Overflow-checked 64-bit credit, debit, transfer, allowance, delegated
  transfer, and reserve operations with total-supply conservation (ADR 0010).
- Multi-asset balances and (assetId, owner, delegate) allowances (ADR 0009).
- Per-client dedup for exactly-once (idempotent) command processing.
- Deterministic streaming snapshots and the off-heap event journal ring
  (ADR 0011).
- Off-heap telemetry counters.

## Key classes

- [BalanceEngine.java](src/main/java/io/justrade/ledgerd/core/BalanceEngine.java) -
  command dispatch plus dedup (cluster-independent).
- [BalanceService.java](src/main/java/io/justrade/ledgerd/core/BalanceService.java) -
  the `ClusteredService` integration (decode, process, ACK, snapshot).
- [handlers/](src/main/java/io/justrade/ledgerd/core/handlers/) - credit, debit,
  transfer, approve, delegated transfer, and reserve command handlers.
- [collections/BalanceStore.java](src/main/java/io/justrade/ledgerd/collections/BalanceStore.java) -
  primitive balance map plus the total-supply invariant.
- [collections/AllowanceStore.java](src/main/java/io/justrade/ledgerd/collections/AllowanceStore.java) -
  nested primitive map keyed by (assetId, owner, delegate).
- [collections/DedupTable.java](src/main/java/io/justrade/ledgerd/collections/DedupTable.java) -
  per-client dedup rings for idempotency.
- [persistence/SnapshotManager.java](src/main/java/io/justrade/ledgerd/persistence/SnapshotManager.java) -
  streaming SBE snapshot write and load.
- [pipeline/EventJournalRing.java](src/main/java/io/justrade/ledgerd/pipeline/EventJournalRing.java) -
  off-heap SPSC ring for domain events (ADR 0011).
- [util/Amounts.java](src/main/java/io/justrade/ledgerd/util/Amounts.java) -
  overflow-checked 64-bit arithmetic.

## Hot path rules

This module is subject to the strictest rules: no locks, no boxing, no
`HashMap`, no streams, no `String.format`, no per-event allocation, no clock
calls, power-of-two capacities with `& (capacity - 1)` indexing. A Checkstyle
determinism overlay at `core/config/checkstyle/determinism.xml` enforces them.
See [../CONTRIBUTING.md](../CONTRIBUTING.md) and
`.github/copilot-instructions.md`.

## Using the engine directly

The engine has no Aeron dependency and can be driven from a decoded command:

```java
BalanceEngine engine = new BalanceEngine(CoreConfig.defaults(), new CoreMetrics());
CommandOutcome outcome = new CommandOutcome();
engine.process(commandDecoder, /* leader timestamp */ 1_000L, outcome);
```

## Benchmarks

JMH sources live under `src/jmh/`. Run the quick smoke set:

```bash
./gradlew :core:jmh -PquickBench
```

## Related

- Determinism rules: [../docs/decisions/0002-core-budget.md](../docs/decisions/0002-core-budget.md).
- Design: [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md).
