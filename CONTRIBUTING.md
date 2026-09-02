# Contributing to LEDGERD

Thanks for your interest in contributing. LEDGERD is a deterministic, allocation-
conscious, lock-free balance engine, so the bar for changes on the hot path is
high: correctness and tail-latency stability come before everything else.

## Prerequisites

- **JDK 21 LTS** (Temurin or equivalent). The Gradle toolchain is pinned to 21
  in `gradle.properties` (`targetJavaVersion=21`); do not use a different JDK.
- **Linux** is the supported and tested platform.
- Aeron/Agrona 2.x access internal JDK APIs, so every JVM that runs the engine
  (build, tests, launcher, examples) must pass these flags:

  ```
  --add-opens java.base/jdk.internal.misc=ALL-UNNAMED
  --add-opens java.base/sun.nio.ch=ALL-UNNAMED
  ```

  The Gradle build already applies them to test, launcher, and example JVMs; you
  only need them if you run a class by hand.

## The CI gate

Every change must pass the following sequence, in this order, with zero errors
and zero warnings before it is committed. This is exactly what CI enforces
(`.github/workflows/ci.yml`).

```bash
./gradlew spotlessApply          # 1. auto-fix formatting (run this first, not spotlessCheck)
./gradlew checkstyleMain checkstyleTest   # 2. zero violations
./gradlew compileJava            # 3. compiles with -Werror (warnings are errors)
./gradlew test integrationTest   # 4. unit + single-node integration tests
./gradlew :core:jmh -PquickBench     # 5. benchmark smoke run
```

If any step fails, fix it and re-run from step 1. CI runs `spotlessCheck` (non-
mutating); run `spotlessApply` locally first so formatting never trips CI.

## Opt-in test suites

Only `test` and `integrationTest` run in the default gate. The heavier suites
are opt-in because they are slower and timing-sensitive; run them locally when
your change touches clustering, failover, snapshots, or the hot path:

```bash
./gradlew clusterTest   # multi-node: leader election, catch-up replay, determinism
./gradlew faultTest     # kill the leader mid-flight, verify exactly-once failover
./gradlew soakTest      # sustained load: tail-latency budget and GC observation
```

## Benchmarks and the performance budget

Any change that touches a hot-path operation (`decode`, `dispatch`, `lookup`,
`encode`, snapshot write/read) must include JMH before/after numbers in the PR.

- Compare against `benchmark-baseline.txt` at the repo root.
- A regression greater than 10% on any hot-path benchmark is rejected unless it
  is justified by a new ADR under `docs/decisions/`.
- Confirm zero steady-state allocation with the GC profiler:

  ```bash
  ./gradlew :core:jmh -Pjmh.profilers=gc
  ```

See [docs/decisions/0002-core-budget.md](docs/decisions/0002-core-budget.md) for
the full budget. Priority order: Correctness > Determinism > Tail Latency > Mean
Latency > Throughput.

## The determinism gate

The core (`core`) is checked by a dedicated ruleset,
[core/config/checkstyle/determinism.xml](core/config/checkstyle/determinism.xml),
which fails the build if the state machine uses anything non-deterministic or
allocation-heavy on the hot path. It bans, among other things:

- `System.currentTimeMillis()` / `System.nanoTime()` (the only time source is the
  leader-assigned timestamp passed into the service);
- `Math.random()` / `UUID.randomUUID()` (identifiers are minted at the Edge);
- `java.util.HashMap` / `TreeMap` / `ConcurrentHashMap` (use Agrona primitive
  maps; snapshot iteration sorts keys explicitly for byte-identical output);
- `Optional`, `BigDecimal`, streams, `String.format`, and blocking primitives on
  the hot path.

If your change legitimately needs one of these, it almost certainly belongs
outside `core` (for example in `write-client`, `launcher`, `read`,
or `examples`, which are not deterministic hot-path modules).

## Coding conventions

- Follow the rules in [.github/copilot-instructions.md](.github/copilot-instructions.md);
  they encode the hot-path, concurrency, and memory-ordering requirements.
- ASCII only in code comments, Javadoc, and Markdown. No em-dashes, no emojis;
  use ` - ` instead.
- Non-trivial diagrams use Mermaid, never ASCII art.
- `docs/decisions/` is the architectural source of truth. Material design changes
  need an ADR there.

## Submitting a change

1. Create a topic branch.
2. Make the change and keep it minimal and focused.
3. Run the full CI gate locally (all five steps above).
4. For hot-path changes, attach JMH before/after numbers.
5. Open a pull request and fill in the PR template.
