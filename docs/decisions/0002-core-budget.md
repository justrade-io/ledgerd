# 0002 - Core Service Latency and Allocation Budget

Status: Accepted
Date: 2026-07-26

## Context

The engine targets low-latency, high-throughput workloads. Latency variance
(tail percentiles) is the contract, not the mean. See tmp/PRD.md section 6.2 and
19 (historical planning drafts, not tracked in this repository), and .github/copilot-instructions.md.

## Budget

| Metric                              | Target          |
|-------------------------------------|-----------------|
| Small message decode                | < 100 ns        |
| Primitive map lookup                | < 50 ns         |
| Command dispatch (in-process)       | < 500 ns        |
| End-to-end IPC p99                  | < 15 us         |
| Allocation in hot path              | 0 bytes / event |
| GC pause during operational window  | 0 ms            |
| Throughput (sustained)              | 200k - 1M ops/s |

## Verification

- JMH micro-benchmarks in `core/src/jmh` cover decode, map lookup, and
  dispatch. Run with `-prof gc` to confirm zero steady-state allocation.
- The CI gate runs `./gradlew jmh -PquickBench` as a smoke check; a > 10%
  regression on any percentile requires rollback or a new ADR.

## Notes

- Priority order: Correctness > Determinism > Tail Latency > Mean Latency >
  Throughput.
- Snapshot write time (runtime impact) and snapshot read time (recovery time)
  are measured and reported separately; they are not summed into one figure.
