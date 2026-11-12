plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "ktor-nntp-client"

// A checkout of the rapidyenc wrapper beside this one, under its repository's name or as yenc_kotlin_wrapper.
listOf("../rapidyenc-kotlin-wrapper", "../yenc_kotlin_wrapper").map(::file).firstOrNull { it.exists() }?.let { wrapper ->
    includeBuild(wrapper) {
        dependencySubstitution {
            substitute(module("com.github.skjaere:rapidyenc-kotlin-wrapper")).using(project(":"))
        }
    }
}
