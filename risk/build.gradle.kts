// risk: the AI risk substrate (ADR 0012). An Edge / analytics bounded
// context that follows the domain event journal (ADR 0011, stream 108) via
// read's EventJournalFollower and turns the event stream into live risk
// signals - per-account transaction-velocity z-scores and money-flow graph
// centrality - surfaced on a Netty HTTP dashboard.
//
// Like read, this module is NOT the deterministic core: it may use the
// system clock, heap allocation, HashMap, streams, and Netty. It only reads the
// journal; it never joins Raft and never affects quorum or the write path.

plugins {
    application
}

application {
    mainClass.set("io.justrade.ledgerd.risk.RiskServiceLauncher")
    applicationDefaultJvmArgs = listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    )
}

dependencies {
    implementation(project(":protocol"))
    implementation(project(":read"))
    implementation(libs.bundles.aeron)
    implementation(libs.netty.codec.http)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
