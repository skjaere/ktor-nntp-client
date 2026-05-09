plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.9.0"
}

rootProject.name = "ktor-nntp-client"

if (file("../yenc_kotlin_wrapper").exists()) {
    includeBuild("../yenc_kotlin_wrapper") {
        dependencySubstitution {
            substitute(module("com.github.skjaere:rapidyenc-kotlin-wrapper")).using(project(":"))
        }
    }
}
