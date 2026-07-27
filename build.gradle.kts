import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.javadoc.Javadoc
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.external.javadoc.StandardJavadocDocletOptions

plugins {
    alias(libs.plugins.spotless) apply false
}

val targetJava = (property("targetJavaVersion") as String).toInt()
val checkstyleVersion = libs.versions.checkstyle.get()

// Aeron/Agrona 2.x access jdk.internal.misc.Unsafe and sun.nio.ch on modern JDKs.
val aeronJvmArgs = listOf(
    "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
    "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
)
extra["aeronJvmArgs"] = aeronJvmArgs

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "checkstyle")
    apply(plugin = "com.diffplug.spotless")

    repositories {
        mavenCentral()
    }

    extensions.configure<JavaPluginExtension> {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(targetJava))
        }
    }

    extensions.configure<org.gradle.api.plugins.quality.CheckstyleExtension> {
        toolVersion = checkstyleVersion
        configDirectory.set(rootProject.layout.projectDirectory.dir("config/checkstyle"))
        isIgnoreFailures = false
        maxWarnings = 0
    }

    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        java {
            target("src/**/*.java")
            targetExclude("**/build/**", "**/generated/**")
            palantirJavaFormat()
            removeUnusedImports()
            trimTrailingWhitespace()
            endWithNewline()
            // Enforce ASCII, no em-dashes per project rules is handled via checkstyle/review.
        }
    }

    tasks.withType<JavaCompile>().configureEach {
        options.encoding = "UTF-8"
        options.compilerArgs.addAll(listOf("-Xlint:all", "-Werror"))
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
        jvmArgs(aeronJvmArgs)
        testLogging {
            exceptionFormat = TestExceptionFormat.FULL
            events("passed", "skipped", "failed")
        }
    }

    // Generated SBE codecs and flyweights do not carry lint-clean Javadoc, so
    // disable doclint everywhere rather than fail the docs build on generated code.
    tasks.withType<Javadoc>().configureEach {
        options.encoding = "UTF-8"
        (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
    }
}

// Aggregated API documentation across all modules, published to GitHub Pages by
// .github/workflows/javadoc.yml. Runs on the same JDK 21 toolchain via the
// javadoc tool resolved from each subproject.
tasks.register<Javadoc>("aggregateJavadoc") {
    group = "documentation"
    description = "Generates a single aggregated Javadoc site across all modules."
    setDestinationDir(layout.buildDirectory.dir("docs/aggregateJavadoc").get().asFile)
    title = "ADBE - Aeron Distributed Balance Engine API"
    (options as StandardJavadocDocletOptions).apply {
        encoding = "UTF-8"
        addStringOption("Xdoclint:none", "-quiet")
    }

    subprojects.forEach { sp ->
        sp.plugins.withId("java") {
            val sourceSets = sp.extensions.getByType<SourceSetContainer>()
            val mainSourceSet = sourceSets.getByName("main")
            source(mainSourceSet.allJava)
            classpath += mainSourceSet.compileClasspath + mainSourceSet.output
            dependsOn(sp.tasks.named("compileJava"))
        }
    }
}
