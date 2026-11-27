# LEDGERD Jepsen

Distributed-systems verification for LEDGERD using [Jepsen](https://jepsen.io).
This is a standalone Leiningen project; it is not part of the Gradle build.

Milestone 3 / Sprint 5 (J1) is a no-nemesis smoke test: Jepsen workers credit a
single account through the real Aeron cluster and a conservation checker confirms
the history is well-formed and value is conserved. Sprint 6 (J2) adds network
partitions: a partition that splits the cluster must force a leader change
without losing an acknowledged command or applying one twice. Later sprints add
crash, pause, clock, and disk nemeses on top of this harness (see
`docs/ROADMAP.md`).

## How it works

- `ledgerd.db` installs and runs one LEDGERD node per DB node from the launcher
  distribution, over SSH, and waits for `/healthz`. Nodes run with lowered
  election and heartbeat timeouts (the launcher M6 knobs) so failover is fast
  enough for a nemesis cycle.
- `ledgerd.client` drives the real `write-client` over Java interop. Each worker
  owns a WriteClient with a unique `clientId`, in raw single-shot mode so an
  unacknowledged command surfaces as indeterminate (`:info`) instead of being
  masked by an idempotent retry.
- `ledgerd.checker` asserts value conservation on the credit counter.

## Prerequisites

- A Jepsen control node (this machine) with Leiningen and a JDK.
- A set of DB nodes reachable over SSH, each with a JDK on `PATH`, `curl`, and
  `iptables` (`iptables` is only needed once partition nemeses land). Jepsen
  needs root on the DB nodes for process and (later) network control.
- The LEDGERD artifacts built from the repository root:

  ```bash
  ./gradlew :launcher:distTar :write-client:jar :protocol:jar
  ```

  `ledgerd.db` uploads `launcher/build/distributions/launcher.tar` to each node;
  `project.clj` puts the write-client and protocol jars on the classpath.

## Running

From this directory, with a nodes file listing the DB hostnames (one per line):

```bash
lein run test \
  --nodes-file nodes.txt \
  --username root \
  --ssh-private-key ~/.ssh/id_rsa \
  --concurrency 10 \
  --time-limit 60 \
  --nemesis partition-halves
```

`--nemesis` accepts `none`, `partition-halves`, or `partition-ring`.

Results, histories, and the checker report are written under `store/`. Launch
the built-in web viewer with:

```bash
lein run serve
```
