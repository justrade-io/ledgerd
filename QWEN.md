# QWEN.md

> **CRITICAL:** At the start of every conversation, before any code changes, read
> `.github/copilot-instructions.md`. It contains the authoritative, machine-parseable
> coding directives for hot path, memory, concurrency, wire format, testing, and
> determinism. All code must comply with those rules regardless of what is summarized
> below.

## Project Identity

**ADBE** (Aeron Distributed Balance Engine) - a deterministic, replicated, in-memory balance and delegated-spending engine in Java, built on Aeron Cluster. Strong consistency and ultra-low latency for a core ledger: the single source of truth for balances and allowances.

- **Repository:** `brianpht/addendum` on GitHub
- **License:** MIT
- **Primary language:** Java
- **Toolchain:** JDK 21 LTS (Temurin), Linux only

## Tech Stack

| Layer | Technology |
|---|---|
| Transport / Cluster / Archive | Aeron 1.48.0 |
| Primitive collections / buffers | Agrona 2.2.1 |
| Wire format + codec generation | SBE 1.35.1 (Simple Binary Encoding) |
| Build system | Gradle (Kotlin DSL) with version catalog |
| Testing | JUnit 5.11.4, jqwik 1.9.2 (property-based) |
| Benchmarks | JMH 1.37 |
| Latency measurement | HdrHistogram 2.2.2 |
| Thread affinity | Affinity 3.23.3 (OpenHFT) |
| Linting / formatting | Checkstyle 10.20.1, Spotless (Palantir Java Format) |
| Read-side HTTP | Netty 4.1.136.Final |
| Containerisation | Docker (multi-stage `Dockerfile` + `docker-compose.yml`) |
| CI | GitHub Actions (`.github/workflows/ci.yml`) |

## Module Architecture

```
adbe-protocol/     SBE schema + generated flyweight codecs (zero-dependency wire contract)
adbe-core/         Deterministic state machine: engine, handlers, stores, dedup, snapshot, telemetry
adbe-launcher/     Aeron bootstrap: Media Driver, Archive, Consensus Module, service container, optional Prometheus endpoint
adbe-client/       Edge-side SDK: leader-change handling, idempotent retry, async correlation, backpressure (depends only on adbe-protocol)
adbe-read/         CQRS read side: standby (default) or cluster follower, answers balance/allowance/supply queries over HTTP (Netty)
adbe-tests/        Unit, property, integration, cluster, fault, and soak tests + testFixtures toolkit
adbe-examples/     Runnable examples (QuickStart, RemoteClient)
```

The hot path lives entirely in `adbe-core`. Everything else (client, launcher, read side, examples) is outside the deterministic boundary.

## Key Build / Test Commands

```bash
# Full CI gate (run in this order before committing)
./gradlew spotlessApply                              # auto-fix formatting
./gradlew checkstyleMain checkstyleTest              # zero violations required
./gradlew compileJava                                # compiles with -Werror
./gradlew test integrationTest                       # unit + single-node integration tests
./gradlew :adbe-core:jmh -PquickBench                # benchmark smoke run (no regression > 10%)

# Opt-in heavier test suites
./gradlew clusterTest    # multi-node: leader election, catch-up replay, determinism
./gradlew faultTest      # kill leader mid-flight, verify exactly-once failover
./gradlew soakTest       # sustained load with tail-latency budget + GC observation

# Aggregate Javadoc
./gradlew aggregateJavadoc
```

**JVM requirement:** All Gradle tasks, tests, launcher, and examples must pass:
```
--add-opens java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```
The build already applies these to test/launcher/example JVMs. Pass them manually if running a class by hand.

## CI Gate Sequence (Mandatory Before Committing)

Every change must pass, in order, with **zero errors and zero warnings:**

1. `./gradlew spotlessApply` (run this first, never `spotlessCheck` only)
2. `./gradlew checkstyleMain checkstyleTest`
3. `./gradlew compileJava` (warnings are errors via `-Werror`)
4. `./gradlew test integrationTest`
5. `./gradlew :adbe-core:jmh -PquickBench`
6. If any step fails: fix and re-run from step 1.

**Never push** commits, tags, or refs to any remote. All changes remain local.

## Coding Conventions

### Determinism (adbe-core only)

The `adbe-core` module is checked by a dedicated determinism ruleset (`adbe-core/config/checkstyle/determinism.xml`). The following are **forbidden** on the hot path and enforced by Checkstyle:

- `System.currentTimeMillis()` / `System.nanoTime()` - the only time source is the leader-assigned `timestamp` parameter
- `Math.random()` / `UUID.randomUUID()` - identifiers are minted at the Edge
- `java.util.HashMap` / `TreeMap` / `ConcurrentHashMap` - use Agrona primitive maps; snapshot iteration sorts keys explicitly
- `Optional<T>`, `BigDecimal`, Java streams, `String.format`, blocking primitives
- Exceptions for control flow - use status codes or sentinel values

### Hot-Path Rules (adbe-core)

- **Zero heap allocation** in steady state: no `new`, no boxing, no `StringBuilder.toString()`, no streams
- **Single-writer principle:** one thread owns all mutable state; no `synchronized`, `ReentrantLock`, or atomics on the hot path
- **Primitive collections only:** `Long2LongHashMap`, `Int2ObjectHashMap`, `LongArrayList` - use Agrona, never boxed keys/values
- **Power-of-two capacities** with bitwise indexing: `seq & (capacity - 1)`, never `seq % capacity`
- **Flyweight pattern:** SBE encoders/decoders wrap `UnsafeBuffer` / `DirectBuffer` per call; one instance per thread, reused
- **Preallocated buffers** at startup; pools sized at init, no runtime growth
- **Overflow-checked 64-bit arithmetic** (`Amounts.java`) - overflow returns `StatusCode.OVERFLOW`, never wraps silently
- **Fast/common branch first** in conditionals; cold/error paths in separate private methods so JIT keeps them out of inlined hot path

### Cross-Cutting

- **ASCII only** in code comments, Javadoc, and Markdown. No em-dashes, no emojis. Use ` - ` instead.
- **Mermaid** for all non-trivial diagrams. No ASCII art.
- **Palantir Java Format** via Spotless (auto-applied by `spotlessApply`).
- **Checkstyle** with zero warnings (maxWarnings=0), config at `config/checkstyle/`.
- Imports: Spotless removes unused imports automatically.
- Encoding: UTF-8 everywhere.
- **`/docs/decisions/`** is the architectural source of truth. Material design changes need an ADR there. Never use `/docs/sessions` as implementation rules.

## Testing Strategy

| Suite | Gradle task | JUnit tag | What it covers |
|---|---|---|---|
| Unit tests | `test` | (none) | Pure logic, deterministic clocks, no real network |
| Integration tests | `integrationTest` | `integration` | In-process single-node cluster with IPC Media Driver |
| Cluster tests | `clusterTest` | `cluster` | Multi-node: leader election, catch-up replay, determinism |
| Fault tests | `faultTest` | `fault` | Kill leader mid-flight, verify exactly-once failover |
| Soak tests | `soakTest` | `soak` | Sustained load within tail-latency budget, bounded-GC assertion (core zero-alloc is asserted by JMH `-prof gc`) |
| Property tests | `test` | (none) | jqwik: overflow matches `Math.addExact`, sequence arithmetic |
| JMH benchmarks | `jmh` | (none) | Hot-path micro-benchmarks with GC profiler |

Only `test` and `integrationTest` run in the default `check` gate. Use `testFixtures` from `adbe-tests` for test-only helpers (never depend on it from production modules).

## Performance Budget

Priority order: **Correctness > Determinism > Tail Latency > Mean Latency > Throughput**.

- Hot-path allocation: 0 bytes/event in steady state
- Envelope decode: < 100 ns
- Primitive map lookup: < 50 ns
- Command dispatch: < 500 ns
- Regression > 10% on any hot-path benchmark vs `benchmark-baseline.txt` requires justification via ADR

## Key Documents

| Document | Purpose |
|---|---|
| `README.md` | Project overview, features, quick start, architecture |
| `docs/ARCHITECTURE.md` | Full system design: modules, wire format, data flows, determinism rules |
| `docs/API-REFERENCE.md` | Client SDK, commands, status codes, read API, observability |
| `docs/OPERATIONS.md` | Snapshot management, node restart, deployment, Prometheus metrics |
| `docs/decisions/` | Architectural Decision Records (source of truth) |
| `CONTRIBUTING.md` | CI gate, determinism rules, benchmark process, PR template |
| `.github/copilot-instructions.md` | Machine-parseable coding directives (hot path, memory, concurrency, testing) |
| `adbe-core/config/checkstyle/determinism.xml` | Forbidden-constructs ruleset for the hot path |
| `benchmark-baseline.txt` | Committed baseline numbers for JMH diff comparison |

## Docker

```bash
docker compose up --build          # 3-node Raft cluster
docker compose run --rm client     # end-to-end smoke test
docker compose -f docker-compose.read.yml up --build  # read-side service
```
