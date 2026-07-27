// adbe-tests: deterministic replay, snapshot round-trip, idempotency, fault
// injection, and overflow tests. Also hosts the test-only cluster client harness
// via testFixtures (NOT a shipped Edge SDK).

plugins {
    `java-test-fixtures`
}

dependencies {
    testImplementation(project(":adbe-core"))
    testImplementation(project(":adbe-launcher"))
    testImplementation(project(":adbe-client"))
    testImplementation(libs.bundles.aeron)

    testFixturesApi(project(":adbe-protocol"))
    testFixturesApi(project(":adbe-core"))
    testFixturesApi(project(":adbe-launcher"))
    testFixturesApi(libs.bundles.aeron)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.jqwik)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Integration tests (in-process Media Driver) run under the integrationTest task.
val integrationTest by tasks.registering(Test::class) {
    description = "Runs integration tests with an in-process Aeron Media Driver."
    group = "verification"
    useJUnitPlatform {
        includeTags("integration")
    }
    shouldRunAfter(tasks.named("test"))
}

// Multi-node cluster tests (leader election, catch-up replay). Heavier and
// slower than single-node integration; opt-in, NOT wired into `check`.
val clusterTest by tasks.registering(Test::class) {
    description = "Runs multi-node Aeron cluster tests (leader election, catch-up)."
    group = "verification"
    useJUnitPlatform {
        includeTags("cluster")
    }
    shouldRunAfter(integrationTest)
}

// Fault-injection tests (kill-leader mid-ACK). Can be timing-sensitive; opt-in,
// NOT wired into `check`.
val faultTest by tasks.registering(Test::class) {
    description = "Runs fault-injection tests (leader kill, failover)."
    group = "verification"
    useJUnitPlatform {
        includeTags("fault")
    }
    shouldRunAfter(clusterTest)
}

// Long-running chaos/soak tests asserting zero-GC and tail-latency budgets.
// Opt-in only, NOT wired into `check`.
val soakTest by tasks.registering(Test::class) {
    description = "Runs long-running soak/chaos tests (zero-GC, tail latency)."
    group = "verification"
    useJUnitPlatform {
        includeTags("soak")
    }
    shouldRunAfter(faultTest)
}

tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("integration", "cluster", "fault", "soak")
    }
}

tasks.named("check") {
    dependsOn(integrationTest)
}

// Test code is not the production hot path; keep lint informative but non-fatal.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.remove("-Werror")
}
