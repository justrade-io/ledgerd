// adbe-examples: runnable, end-to-end demonstrations of the engine. Depends on
// the launcher (to boot an in-process single-node cluster) and the client SDK
// (to submit commands and read results). This module is not part of the
// deterministic hot path and is excluded from the determinism checkstyle gate.

plugins {
    application
}

dependencies {
    implementation(project(":adbe-launcher"))
    implementation(project(":adbe-client"))
    implementation(project(":adbe-core"))
    implementation(project(":adbe-protocol"))
    implementation(libs.bundles.aeron)
}

application {
    mainClass.set("com.adbe.examples.QuickStartExample")
    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    )
}

// Example code is illustrative, not the production hot path: keep lint
// informative but non-fatal so demos stay readable.
tasks.withType<JavaCompile>().configureEach {
    options.compilerArgs.remove("-Werror")
}
