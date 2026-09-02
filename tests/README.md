# tests

Shared test fixtures and the cross-module test suites, organized by JUnit 5 tags
so each tier can run independently in the build and in CI.

## Tiers

- Unit and property tests (no tag): pure logic with deterministic clocks and no
  real network. Property tests use jqwik for sequence arithmetic and codec
  round-trips.
- Integration (`@Tag("integration")`): in-process single-node cluster.
- Cluster (`@Tag("cluster")`): multi-node Raft with election, warm restart, and
  catch-up.
- Fault (`@Tag("fault")`): leader-kill and failover, verifying exactly-once.
- Soak (`@Tag("soak")`): long-running steady-state runs with tail-latency and GC
  assertions.

## Fixtures

- [MultiNodeCluster.java](src/testFixtures/java/io/justrade/ledgerd/testkit/MultiNodeCluster.java) -
  in-process multi-node cluster harness.
- [ClusterTestClient.java](src/testFixtures/java/io/justrade/ledgerd/testkit/ClusterTestClient.java) -
  minimal Aeron Cluster client that matches results by command id.
- [CommandFixtures.java](src/testFixtures/java/io/justrade/ledgerd/testkit/CommandFixtures.java) -
  encode a `CommandEnvelope` and return a wrapped decoder.
- [InMemorySnapshot.java](src/testFixtures/java/io/justrade/ledgerd/testkit/InMemorySnapshot.java) -
  serialise/restore engine state via an in-memory record stream.
- [WorkloadGenerator.java](src/testFixtures/java/io/justrade/ledgerd/testkit/WorkloadGenerator.java) -
  deterministic pseudo-random command workload (seeded).

## Run

```bash
./gradlew test                   # unit + property
./gradlew integrationTest        # in-process single-node cluster
./gradlew clusterTest faultTest  # multi-node and fault-injection suites
```

Only `test` and `integrationTest` run in the default `check` gate.

## What to verify when contributing

- New logic has unit or property coverage.
- Behavior changes are exercised at the right tier (integration / cluster /
  fault).
- Replay determinism holds: a recorded session in produces a byte-identical
  session out.

## Related

- Contribution and CI gate: [../CONTRIBUTING.md](../CONTRIBUTING.md).
- Determinism: [../docs/decisions/](../docs/decisions/).
