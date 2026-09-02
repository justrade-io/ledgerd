# launcher

Bootstraps a single LEDGERD cluster node. It stands up the Aeron stack (media
driver, Archive, consensus) and hosts the balance `ClusteredService`, plus the
agent that drains the event journal to the Archive and an optional Prometheus
endpoint.

## Responsibility

- Launch and wire an Aeron media driver, Archive, and Cluster (consensus) for one
  node.
- Host the `core` balance service inside the cluster container.
- Run the journaler agent that records committed domain events to the Archive,
  off the consensus thread (ADR 0011).
- Expose the off-heap counters over HTTP in Prometheus text format.

## Key classes

- [ClusterLauncher.java](src/main/java/io/justrade/ledgerd/launcher/ClusterLauncher.java) -
  entry point; assembles and starts a node.
- [ClusterNode.java](src/main/java/io/justrade/ledgerd/launcher/ClusterNode.java) -
  the composed node lifecycle.
- [ClusterConfig.java](src/main/java/io/justrade/ledgerd/launcher/ClusterConfig.java) -
  endpoints and directories per node.
- [EventJournaler.java](src/main/java/io/justrade/ledgerd/launcher/EventJournaler.java) -
  drains the off-heap journal ring to the Archive.
- [MetricsHttpServer.java](src/main/java/io/justrade/ledgerd/launcher/MetricsHttpServer.java) -
  optional Prometheus `/metrics` and `/healthz` endpoint.

## Run

```bash
# Single-node localhost cluster
./gradlew :launcher:run

# With a config file overriding CoreConfig capacities
./gradlew :launcher:run --args="--config=production.properties"
```

The launch configuration sets the required `--add-opens` JVM flags
automatically.

## Configuration

- `CoreConfig` capacities via `ledgerd.core.*` (`--config=<file>` or
  `-Dledgerd.core.*`).
- Prometheus metrics port via `-Dledgerd.metricsPort=<port>`.

See [../docs/OPERATIONS.md](../docs/OPERATIONS.md) for the full reference.

## Related

- Consensus and determinism: [../docs/decisions/](../docs/decisions/).
- Deployment: [../docs/OPERATIONS.md](../docs/OPERATIONS.md).
