// adbe-tests: deterministic replay, snapshot round-trip, idempotency, fault
// injection, and overflow tests. Also hosts the test-only cluster client harness
// via testFixtures (NOT a shipped Edge SDK).

plugins {
    `java-test-fixtures`
}

dependencies {
    testImplementation(project(":adbe-core"))
    testImplementation(project(":adbe-launcher"))
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

tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("integration")
    }
}

tasks.named("check") {
    dependsOn(integrationTest)
}

// Test code is not the production hot path; keep lint informative but non-fatal.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.remove("-Werror")
}
