group = "io.skjaere"

plugins {
    `java-library`
    `maven-publish`
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlinx.kover)
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    api(libs.ktor.network)
    api(libs.ktor.network.tls)
    api(libs.ktor.io)
    api(libs.micrometer.core)
    api(libs.rapidyenc)
    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.slf4j.api)

    testImplementation(libs.kotlin.test)
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.logback.classic)
    testImplementation(libs.mock.nntp.testcontainer)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
}

kotlin {
    jvmToolchain(17)
}

java {
    withSourcesJar()
}

tasks.withType<Test> {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
        }
    }
}

// Main targets JVM 17 so Android consumers can dex the classes. The mock-nntp-server
// test dependency is published for JVM 25 only, so test configurations resolve and
// run against a JDK 25 toolchain instead.
listOf("testCompileClasspath", "testRuntimeClasspath").forEach { name -> configurations.named(name) {
    attributes {
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, 25)
    }
} }

tasks.withType<Test>().configureEach {
    javaLauncher = javaToolchains.launcherFor {
        languageVersion = JavaLanguageVersion.of(25)
    }
}
