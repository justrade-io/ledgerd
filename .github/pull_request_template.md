# Description

Summarise the change and the motivation. Link any relevant issue or ADR.

Closes #

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Performance change (hot path)
- [ ] Refactor (no behaviour change)
- [ ] Docs / build / CI

## CI gate

Confirm the full local gate passed, in order (see [CONTRIBUTING.md](../CONTRIBUTING.md)):

- [ ] `./gradlew spotlessApply`
- [ ] `./gradlew checkstyleMain checkstyleTest`
- [ ] `./gradlew compileJava` (with `-Werror`)
- [ ] `./gradlew test integrationTest`
- [ ] `./gradlew :core:jmh -PquickBench`

If clustering, failover, or snapshots are affected, also:

- [ ] `./gradlew clusterTest`
- [ ] `./gradlew faultTest`
- [ ] `./gradlew soakTest`
- [ ] N/A

## Performance (required for hot-path changes)

Hot-path operations: `decode`, `dispatch`, `lookup`, `encode`, snapshot
write/read. If this PR touches any of them, paste JMH before/after numbers
against `benchmark-baseline.txt`.

| Benchmark | Before | After | Delta |
|-----------|--------|-------|-------|
|           |        |       |       |

- [ ] No regression greater than 10% on any percentile, OR an ADR under
      `docs/decisions/` justifies the change.
- [ ] Zero steady-state allocation confirmed (`-Pjmh.profilers=gc`).
- [ ] N/A - this PR does not touch the hot path.

## Determinism

- [ ] The change preserves byte-identical state across nodes and reruns.
- [ ] No clocks, randomness, unordered maps, `Optional`, `BigDecimal`, streams,
      or blocking primitives were introduced into `core`.
- [ ] N/A - this PR does not touch `core`.

## Notes for reviewers

Anything reviewers should focus on, risks, or follow-ups.
