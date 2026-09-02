// core: BalanceService (ClusteredService) plus dedup, snapshot, telemetry.
// This is the deterministic, allocation-free heart of the engine.

plugins {
    alias(libs.plugins.jmh)
}

dependencies {
    api(project(":protocol"))
    api(libs.aeron.cluster)
    api(libs.agrona)
    implementation(libs.hdrhistogram)

    testImplementation(libs.junit.jupiter)
    testImplementation(libs.jqwik)
    testRuntimeOnly(libs.junit.platform.launcher)
}

// Enforce the determinism / hot-path rule set for core sources.
checkstyle {
    configFile = layout.projectDirectory.file("config/checkstyle/determinism.xml").asFile
}

jmh {
    jmhVersion.set(libs.versions.jmh.get())
    jvmArgs.set(listOf(
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
    ))
    resultFormat.set("JSON")
    resultsFile.set(layout.buildDirectory.file("results/jmh/results.json"))
    // -Pjmh.profilers=gc attaches JMH profilers (e.g. the GC allocation profiler
    // used to confirm zero steady-state allocation on the hot path).
    if (project.hasProperty("jmh.profilers")) {
        profilers.set((project.property("jmh.profilers") as String)
            .split(",").map { it.trim() }.filter { it.isNotEmpty() })
    }
    // -PquickBench: fast smoke run used by the CI gate.
    if (project.hasProperty("quickBench")) {
        warmupIterations.set(1)
        iterations.set(1)
        fork.set(1)
    }
}

// F5: fail if a hot-path benchmark breaches the core latency budget (ADR 0002).
// This gates on the absolute budget the baseline documents, not raw deltas, so a
// noisy quickBench smoke run does not produce flaky failures.
tasks.register("jmhBudgetCheck") {
    description = "Verifies hot-path JMH benchmarks stay within the ADR 0002 latency budget."
    group = "verification"
    dependsOn("jmh")
    val resultsFile = layout.buildDirectory.file("results/jmh/results.json")
    doLast {
        val budgetsNs = mapOf(
            "decodeEnvelope" to 100.0,
            "mapLookup" to 50.0,
            "creditDispatch" to 500.0,
        )
        val file = resultsFile.get().asFile
        if (!file.exists()) {
            throw GradleException("JMH results not found at ${file.path}; run the jmh task first.")
        }
        @Suppress("UNCHECKED_CAST")
        val results = groovy.json.JsonSlurper().parse(file) as List<Map<String, Any>>
        val breaches = mutableListOf<String>()
        val seen = mutableSetOf<String>()
        results.forEach { entry ->
            val fqn = entry["benchmark"] as String
            val name = fqn.substringAfterLast('.')
            val budget = budgetsNs[name] ?: return@forEach
            val metric = entry["primaryMetric"] as Map<String, Any>
            val score = (metric["score"] as Number).toDouble()
            seen += name
            if (score > budget) {
                breaches += "$name: ${"%.2f".format(score)} ns > budget ${budget} ns"
            }
        }
        val missing = budgetsNs.keys - seen
        if (missing.isNotEmpty()) {
            throw GradleException("JMH budget check missing benchmarks: $missing")
        }
        if (breaches.isNotEmpty()) {
            throw GradleException("Hot-path latency budget exceeded (ADR 0002):\n  " + breaches.joinToString("\n  "))
        }
        logger.lifecycle("JMH budget check passed for: ${seen.joinToString(", ")}")
    }
}

// Benchmarks (and JMH-generated sources) are not the production hot path:
// keep lint informative and skip the determinism gate for the jmh source set.
tasks.withType<JavaCompile>().matching { it.name.contains("Jmh") }.configureEach {
    options.compilerArgs.remove("-Werror")
}
tasks.withType<Checkstyle>().matching { it.name.contains("Jmh") }.configureEach {
    enabled = false
}
