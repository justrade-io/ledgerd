// write-client: the Edge-side client SDK. It consumes ONLY the protocol wire
// contract (CommandEnvelope / CommandResult) and deliberately does NOT depend on
// core: the Edge is a separate bounded context (see
// docs/decisions/0004-edge-client-context.md). It adds leader-change handling,
// idempotent retry, async request/response correlation, and backpressure
// signalling on top of an Aeron cluster client.

dependencies {
    api(project(":protocol"))
    api(libs.aeron.cluster)
    api(libs.agrona)
    api(libs.hdrhistogram)
    implementation(libs.aeron.driver)

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
}
