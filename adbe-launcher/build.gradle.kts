// adbe-launcher: bootstraps the Aeron components (Media Driver, Consensus Module,
// Archive, Clustered Service Container) that host a single BalanceService.

plugins {
    application
}

application {
    mainClass.set("com.adbe.launcher.ClusterLauncher")
    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    )
}

dependencies {
    implementation(project(":adbe-core"))
    implementation(libs.bundles.aeron)
    implementation(libs.affinity)
}
