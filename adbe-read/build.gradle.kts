// adbe-read: the CQRS read-side. Hosts a ReadModelService (a ClusteredService
// that composes the core BalanceService) on a cluster follower and serves
// eventually-consistent balance / allowance / total-supply reads over HTTP.
//
// Unlike the deterministic core (adbe-core), this module is an Edge / read
// bounded context: it may use the system clock, Netty, and heap allocation at
// the HTTP boundary. It must never perturb the single-writer service thread; it
// answers queries on that same thread via ClusteredService.doBackgroundWork.

plugins {
    application
    alias(libs.plugins.jmh)
}

application {
    mainClass.set("com.adbe.read.ReadServiceLauncher")
    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    )
}

dependencies {
    implementation(project(":adbe-core"))
    implementation(project(":adbe-launcher"))
    implementation(libs.bundles.aeron)
    implementation(libs.netty.codec.http)
}

jmh {
    jmhVersion.set(libs.versions.jmh.get())
    jvmArgs.set(listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    ))
    // -PquickBench: fast smoke run used by the CI gate.
    if (project.hasProperty("quickBench")) {
        warmupIterations.set(1)
        iterations.set(1)
        fork.set(1)
    }
}

// Benchmarks (and JMH-generated sources) are not the production hot path:
// keep lint informative and skip the -Werror / determinism gate for the jmh set.
tasks.withType<JavaCompile>().matching { it.name.contains("Jmh") }.configureEach {
    options.compilerArgs.remove("-Werror")
}
tasks.withType<Checkstyle>().matching { it.name.contains("Jmh") }.configureEach {
    enabled = false
}
