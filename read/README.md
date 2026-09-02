# read

The CQRS read replica. It follows a cluster member's Aeron Archive (committed
consensus log and service snapshots), rebuilds queryable projections, and
answers queries over a plain Aeron request/response protocol, all without
touching consensus.

## Responsibility

- Follow the live committed log and load service snapshots as they appear.
- Dedup on `(logPosition, eventIndex)` for exactly-once event application, even
  across a leader change (ADR 0008).
- Rebuild projections: per-account balances, per-asset total supply, and
  (assetId, owner, delegate) allowances.
- Answer `QueryRequest` messages with `QueryResponse` messages.

## Key classes

- [ReadServiceLauncher.java](src/main/java/io/justrade/ledgerd/read/ReadServiceLauncher.java) -
  entry point (`:read:run` main class); wires and runs the replica.
- [ReadReplicaNode.java](src/main/java/io/justrade/ledgerd/read/ReadReplicaNode.java) -
  the follower core; drives log consumption and query responses.
- [QueryResponder.java](src/main/java/io/justrade/ledgerd/read/QueryResponder.java) -
  serves queries over Aeron.
- [LiveLogSubscriber.java](src/main/java/io/justrade/ledgerd/read/LiveLogSubscriber.java) -
  consumes the committed consensus log.
- [ArchiveSource.java](src/main/java/io/justrade/ledgerd/read/ArchiveSource.java) -
  multi-archive endpoint with round-robin failover.
- [journal/](src/main/java/io/justrade/ledgerd/read/journal/) - domain event
  journal follower, subscriber, and standalone verifier (ADR 0011).

## Run

```bash
# Follow a member's archive and answer queries
LEDGERD_ARCHIVE_CHANNELS="aeron:udp?endpoint=localhost:20104" ./gradlew :read:run
```

## Configuration

- Archive channels via `LEDGERD_ARCHIVE_CHANNELS` (comma-separated, one per
  member) or `LEDGERD_ARCHIVE_CHANNEL` (single, legacy fallback).
- Query channel and stream id via `LEDGERD_QUERY_CHANNEL` (default
  `aeron:udp?endpoint=localhost:44000`) and `LEDGERD_QUERY_STREAM_ID` (default
  `300`).
- `CoreConfig` via `--config=<file>` or `-Dledgerd.core.*`.

See [../docs/OPERATIONS.md](../docs/OPERATIONS.md#8-read-service-aeron-query-api)
for the full reference.

## Related

- Read SDK: [../read-client/README.md](../read-client/README.md).
- Design: [../docs/ARCHITECTURE.md](../docs/ARCHITECTURE.md).
