// bench: a public, reproducible benchmark harness that runs the same
// logical wallet workload (credit / debit / transfer over N accounts) against
// LEDGERD (an in-process 3-node Aeron cluster driven by WriteClient), PostgreSQL
// (JDBC row-lock transactions), and Redis (Jedis + a Lua atomic transfer), then
// reports throughput and end-to-end tail latency (p50 / p99 / p99.9) side by
// side. See ADR 0013 and docs/BENCHMARKS-VS-DATASTORES.md.
//
// Like examples, this module is illustrative infrastructure, not the
// deterministic hot path: it may use the system clock, heap allocation, HashMap,
// streams, and blocking JDBC/Redis clients. Postgres and Redis are provisioned
// on demand via Testcontainers, so a run requires a reachable Docker daemon.

plugins {
    application
}

application {
    mainClass.set("io.justrade.ledgerd.bench.BenchmarkHarness")
    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    )
}

dependencies {
    implementation(project(":launcher"))
    implementation(project(":write-client"))
    implementation(project(":core"))
    implementation(project(":protocol"))
    implementation(libs.bundles.aeron)
    implementation(libs.hdrhistogram)
    implementation(libs.bundles.bench.datastores)
    // A concrete SLF4J binding so a failed Testcontainers run surfaces its cause
    // (see src/main/resources/simplelogger.properties, pinned to warnings).
    runtimeOnly(libs.slf4j.simple)
}

// Benchmark code is illustrative, not the production hot path: keep lint
// informative but non-fatal so the harness stays readable (matches examples).
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.remove("-Werror")
}
