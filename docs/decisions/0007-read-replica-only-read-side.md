# 0007 - Read Replica Only Read Side: Remove the Cluster-Follower Read Mode

Status: Accepted
Date: 2026-08-01
Updated: 2026-08-01 (terminology: renamed "standby" to "read replica" - the node is a read-only replica, not a failover standby)

## Context

ADR 0005 introduced the read-side CQRS model as a cluster follower:
`ReadModelService` joined the cluster as a full Raft voting member, composing
the core `BalanceService`. ADR 0006 added a second realization - standalone
**read replica** read nodes (`ReadReplicaNode`) that replicate state through the
Aeron Archive without participating in consensus, precisely because a follower
read node increases the quorum threshold and can bring the write cluster down if
lost.

Since ADR 0006, two read topologies shipped side by side: `ADBE_MODE=cluster`
(the follower) and `ADBE_MODE=read-replica` (the default). Maintaining both doubles
the read-side surface area, and the cluster mode keeps the very quorum coupling
that ADR 0006 was designed to remove. The follower mode is not used by any
deployment in this repository (the compose topology runs read replica nodes only).

Separately, the read replica node only began following the live consensus log *after*
loading a snapshot (`snapshotLogPosition > 0`). Because the write cluster takes
no periodic snapshots by default (Aeron's `snapshotInterval` defaults to
`Long.MAX_VALUE`) and Docker starts clean (`ADBE_CLEAN_START=true`), a freshly
started cluster produced no snapshot recording, so a read replica node served
empty reads indefinitely. The read replica read path was therefore not functional in
a running Docker deployment without an externally triggered snapshot.

## Decision

1. **Standardize on the read replica node.** Remove the cluster-follower read
   mode entirely: delete `ReadNode` and `projection/ReadModelService`, the
   `ADBE_MODE=cluster` branch of `ReadServiceLauncher`, and the cluster-mode
   documentation, environment variables, and Docker entrypoint path. `adbe-read`
   no longer depends on `adbe-launcher` (the Aeron Cluster codecs it still uses
   come from the `aeron` bundle). This supersedes the cluster-follower portion
   of ADR 0005; the rest of ADR 0005 (the HTTP query boundary, the lock-free
   `ReadQueryGateway`, the eventually-consistent contract) is unchanged.

2. **Follow the consensus log from position 0.** `ReadReplicaNode` now starts
   following the consensus log from the last loaded snapshot position, or from
   the start of the log (position 0) when no snapshot has been loaded yet. The
   engine therefore builds state immediately on a fresh cluster, with no
   externally triggered snapshot required. The engine's command-id dedup keeps
   re-application idempotent when a snapshot later loads and the live log
   restarts from the snapshot position. The write cluster is unchanged.

3. **Idiomatic Agrona agent.** The read replica's single-writer loop runs as an
   Agrona `Agent` driven by an `AgentRunner` (per the project concurrency
   rules), replacing the hand-rolled `Thread` + idle strategy. A failed snapshot
   replay is still caught and retried on the next cycle so it never stops query
   serving.

4. **Routable Archive call-back host.** The read replica binds its Archive-facing
   subscriptions (the control-response channel and snapshot / log replays) on a
   configurable `localHost` (default `localhost` for same-host runs). In Docker
   the entrypoint sets it to the container's own address so the Archive on
   another container can connect back - the same pattern the remote client uses
   for its egress endpoint.

5. **Single Docker Compose topology.** `docker-compose.yml` now brings up the
   3-node write cluster plus one read replica node (`adbe-read-0`) on one
   network. The separate `docker-compose.read.yml` is removed.

## Consequences

- **Positive**: One supported read topology. Read nodes never affect quorum and
  can be added, removed, or restarted independently of write availability.
- **Positive**: The read replica read path works on a fresh cluster with no snapshot
  tooling, so the Docker deployment serves real reads out of the box.
- **Positive**: Smaller read-side surface and one fewer module dependency
  (`adbe-read` no longer depends on `adbe-launcher`).
- **Negative**: A read replica that starts late replays the consensus log from
  position 0 until a snapshot bounds it. Snapshots, when present, still cap the
  replay; this is acceptable for the bounded-staleness contract of ADR 0005.
- **Negative**: Homogeneous read clusters (all members hosting the read service)
  are no longer supported. That topology was unused and re-introduced the quorum
  coupling ADR 0006 removed.

## Out of scope

- Multi-archive failover (the read replica still connects to a single Archive
  endpoint; retry across members remains deferred, as in ADR 0006).
- Periodic snapshot scheduling on the write cluster (not required now that the
  read replica follows from position 0; snapshots still occur on clean shutdown).
- Authentication or encryption on the Archive connection.
