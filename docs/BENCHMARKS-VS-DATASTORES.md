# Benchmark: LEDGERD vs PostgreSQL vs Redis

This is the public, reproducible datastore benchmark called for in the roadmap. It
runs one identical wallet workload (credit / debit / transfer over N accounts)
against LEDGERD, PostgreSQL, and Redis behind a common interface, and reports
throughput and end-to-end tail latency side by side.

The methodology, the workload, and - crucially - the fairness caveats are fixed in
[ADR 0013](decisions/0013-datastore-benchmark-methodology.md). Read it before
quoting any number: the three systems sit at different points on the durability,
consistency, and topology spectrum, so this is a **directional** comparison, not a
leaderboard.

## What is being compared

| Backend | Topology in the run | Durability (default) | Execution model |
| --- | --- | --- | --- |
| LEDGERD | 3-node Raft cluster (real consensus) | Replicated + Aeron Archive | Pipelined async, one client |
| PostgreSQL | Single node | WAL fsync per commit (`synchronous_commit=on`) | Synchronous, thread-per-op |
| Redis | Single node | In-memory, no persistence | Synchronous, thread-per-op |

LEDGERD is doing strictly more work per op (three replicas, durable log) than the
single-node datastores. Redis promises the least (no durability) and will look
fastest on latency partly for that reason. The point of the exercise is that a
deterministic, allocation-free, **replicated and durable** ledger stays
competitive on throughput and tight on tail latency.

## How to run

Requires a reachable Docker daemon (Postgres and Redis are provisioned via
Testcontainers; LEDGERD boots an in-process three-node cluster):

```bash
./gradlew :bench:run --args="--accounts=1000 --ops=20000 --warmup=5000 --concurrency=32"
```

Results are printed as a table and written to `bench/build/bench/results.csv`.

Arguments (all optional):

| Argument | Default | Meaning |
| --- | --- | --- |
| `--accounts` | 1000 | account id space (ids `1..N`) |
| `--ops` | 50000 | measured operations |
| `--warmup` | 10000 | warmup operations (discarded) |
| `--concurrency` | 64 | LEDGERD `maxInFlight` and PG/Redis worker/connection count |
| `--mix` | 40,30,30 | credit,debit,transfer weights |
| `--seed` | 42 | workload RNG seed (reproducibility) |
| `--initial` | 1000000000 | per-account starting balance |
| `--backends` | ledgerd,postgres,redis | subset to run |
| `--csv` | build/bench/results.csv | output path |

Point LEDGERD at an external cluster (instead of the in-process one) by exporting
`LEDGERD_INGRESS_ENDPOINTS`, for example the docker-compose stack's
`0=localhost:20100,...`.

## Sample results

Illustrative only - a developer laptop, `--accounts=1000 --ops=20000
--warmup=5000 --concurrency=32`. Numbers vary with hardware, Docker, and load;
regenerate locally rather than quoting these:

| backend | throughput (ops/s) | p50 (us) | p99 (us) | p99.9 (us) | max (us) |
| --- | --- | --- | --- | --- | --- |
| ledgerd | 53527 | 522.8 | 3137.5 | 5345.3 | 5357.6 |
| postgres | 28277 | 705.0 | 3651.6 | 5832.7 | 9855.0 |
| redis | 25153 | 288.0 | 906.8 | 1739.8 | 3070.0 |

Reading these fairly: LEDGERD leads throughput while being the only replicated,
durable backend; Redis has the lowest median latency (in-memory, no durability)
but is throughput-bound by the synchronous thread-per-op model at this concurrency;
Postgres pays a WAL fsync per op. Tail latency (p99.9) is the number the project
treats as the contract, not the mean.

## Correctness

The harness self-checks each backend after its run: Postgres and Redis assert their
total supply equals the value derived from the (seeded) op stream, and LEDGERD asserts
every submitted command completed with none dropped. A divergence fails the run
rather than reporting a meaningless number.

## What this is not

- Not a CI gate - it needs Docker and is wall-clock sensitive. The hot-path JMH
  micro-benchmarks in `core` / `read` remain the gating benchmarks.
- Not durability-matched - a fairer follow-up would enable Redis AOF `everysec` and
  tune Postgres. This first cut runs each store at its defaults and documents the
  asymmetry (see ADR 0013).
