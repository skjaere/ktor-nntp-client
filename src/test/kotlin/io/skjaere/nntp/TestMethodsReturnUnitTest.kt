package io.skjaere.nntp

import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.extension
import kotlin.io.path.invariantSeparatorsPathString
import kotlin.io.path.relativeTo
import kotlin.streams.asSequence
import kotlin.test.assertEquals

/**
 * JUnit 5 skips a @Test method that returns a value, without a word. In Kotlin that is
 * `fun x() = runBlocking { ...; assertFailsWith<E> { ... } }`, which returns the exception:
 * give such a test a block body or `runBlocking<Unit>`.
 */
class TestMethodsReturnUnitTest {

    @Test
    fun `no test method returns a value`() {
        val root = Path.of(javaClass.protectionDomain.codeSource.location.toURI())
        val skipped = Files.walk(root).use { paths ->
            paths.asSequence()
                .filter { it.extension == "class" }
                .map { it.relativeTo(root).invariantSeparatorsPathString.removeSuffix(".class").replace('/', '.') }
                .map { Class.forName(it, false, javaClass.classLoader) }
                .flatMap { it.declaredMethods.asSequence() }
                .filter { it.isAnnotationPresent(Test::class.java) && it.returnType != Void.TYPE }
                .map { "${it.declaringClass.simpleName}.${it.name}" }
                .sorted()
                .toList()
        }
        assertEquals(emptyList(), skipped, "JUnit never runs these tests")
    }
}
