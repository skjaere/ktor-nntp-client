package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.ServerSocket
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.system.measureTimeMillis
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Whether a connection goes back to the pool after a command on it was cut short (cancelled,
 * or failed in a block that caught the failure and carried on). It may only when the wire is
 * in step: what the server still owes has been read off, or else the connection is
 * discarded. Otherwise the next command reads an earlier command's answer as its own.
 */
class NntpPoolWireStateTest {

    private lateinit var serverSocket: ServerSocket
    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope

    @BeforeEach
    fun setUp() {
        serverSocket = ServerSocket(0)
        selectorManager = SelectorManager(Dispatchers.IO)
        poolScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    }

    @AfterEach
    fun tearDown() {
        poolScope.cancel()
        serverSocket.close()
        selectorManager.close()
    }

    /**
     * Greets every connection and hands each command to [answer] with the connection's number
     * (from 1). QUIT is answered here. Returns the number of connections accepted so far.
     */
    private fun launchServer(
        answer: (connection: Int, command: String, reader: BufferedReader, out: OutputStream) -> Unit
    ): AtomicInteger {
        val accepted = AtomicInteger()
        Thread {
            while (true) {
                val client = try {
                    serverSocket.accept()
                } catch (_: Exception) {
                    break
                }
                val number = accepted.incrementAndGet()
                Thread {
                    client.use { socket ->
                        try {
                            val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                            val out = socket.getOutputStream()
                            out.send("200 welcome")
                            while (true) {
                                val cmd = reader.readLine() ?: break
                                if (cmd == "QUIT") {
                                    out.send("205 bye")
                                    break
                                }
                                answer(number, cmd, reader, out)
                            }
                        } catch (_: Exception) {
                        }
                    }
                }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
        return accepted
    }

    private fun OutputStream.send(vararg lines: String) {
        write(lines.joinToString("") { "$it\r\n" }.toByteArray())
        flush()
    }

    private fun pool(
        commandRetries: Int = NntpClientPool.DEFAULT_COMMAND_RETRIES,
        readTimeoutMs: Long = NntpConnection.DEFAULT_READ_TIMEOUT_MS
    ) = NntpClientPool(
        host = "localhost",
        port = serverSocket.localPort,
        selectorManager = selectorManager,
        maxConnections = 1,
        scope = poolScope,
        keepaliveIntervalMs = 0,
        idleGracePeriodMs = 0,
        commandRetries = commandRetries,
        readTimeoutMs = readTimeoutMs
    )

    // --- Cancelled commands ---

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a STAT cancelled before its answer is read off and its connection reused`() = runBlocking<Unit> {
        val accepted = launchServer { _, cmd, _, out ->
            when (cmd) {
                "STAT <slow@test>" -> {
                    Thread.sleep(SLOW_ANSWER_MS)
                    out.send("223 0 <slow@test> article exists")
                }
                else -> out.send("430 no such article")
            }
        }
        val pool = pool()
        pool.wake()
        try {
            assertIs<StatResult.NotFound>(pool.stat("<warm@test>"))

            assertNull(withTimeoutOrNull(CANCEL_AFTER_MS) { pool.stat("<slow@test>") })

            val next = pool.stat("<missing@test>")
            assertIs<StatResult.NotFound>(next, "the next STAT read the cancelled STAT's answer: $next")
            assertEquals(1, accepted.get(), "the STAT's answer should have been read off and its connection reused")
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a HEAD cancelled mid-response does not leave the rest for the next command`() = runBlocking<Unit> {
        val accepted = launchServer { _, cmd, _, out ->
            when {
                cmd == "HEAD <head@test>" -> {
                    out.send("221 0 <head@test> head follows", "Subject: one")
                    Thread.sleep(SLOW_ANSWER_MS)
                    out.send("From: two", ".")
                }
                cmd.startsWith("STAT") -> out.send("223 0 <test@msg> article exists")
                else -> out.send("500 unknown command")
            }
        }
        val pool = pool()
        pool.wake()
        try {
            assertIs<StatResult.Found>(pool.stat("<test@msg>"))

            assertNull(withTimeoutOrNull(CANCEL_AFTER_MS) { pool.head("<head@test>") })

            val next = pool.stat("<test@msg>")
            assertIs<StatResult.Found>(next)
            assertEquals("<test@msg>", next.messageId)
            assertEquals(2, accepted.get(), "the STAT should have needed a new connection")
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a POST cancelled before its answer is read off and its connection reused`() = runBlocking<Unit> {
        val accepted = launchServer { _, cmd, reader, out ->
            when {
                cmd == "POST" -> {
                    out.send("340 send article")
                    while (reader.readLine() != ".") Unit
                    Thread.sleep(SLOW_ANSWER_MS)
                    out.send("240 article posted")
                }
                cmd.startsWith("STAT") -> out.send("430 no such article")
                else -> out.send("500 unknown command")
            }
        }
        val pool = pool()
        pool.wake()
        try {
            assertIs<StatResult.NotFound>(pool.stat("<warm@test>"))

            assertNull(
                withTimeoutOrNull(CANCEL_AFTER_MS) {
                    pool.withClient { it.post("Subject: test\r\n\r\nbody") }
                }
            )

            val next = runCatching { pool.stat("<missing@test>") }
            assertIs<StatResult.NotFound>(next.getOrNull(), "the next STAT read the POST's answer: $next")
            assertEquals(1, accepted.get(), "the POST's answer should have been read off and its connection reused")
        } finally {
            pool.close()
        }
    }

    /**
     * Reading off the go-ahead a cancelled POST still owes would leave the server waiting for
     * an article, taking the next command for its text.
     */
    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a POST cancelled before its go-ahead does not leave the server waiting for the article`() =
        runBlocking<Unit> {
            val accepted = launchServer { _, cmd, reader, out ->
                when {
                    cmd == "POST" -> {
                        Thread.sleep(SLOW_ANSWER_MS)
                        out.send("340 send article")
                        while (reader.readLine() != ".") Unit
                        out.send("240 article posted")
                    }
                    cmd.startsWith("STAT") -> out.send("223 0 <test@msg> article exists")
                    else -> out.send("500 unknown command")
                }
            }
            val pool = pool()
            pool.wake()
            try {
                assertIs<StatResult.Found>(pool.stat("<test@msg>"))

                assertNull(
                    withTimeoutOrNull(CANCEL_AFTER_MS) {
                        pool.withClient { it.post("Subject: test\r\n\r\nbody") }
                    }
                )

                val next = runCatching { withTimeoutOrNull(5_000) { pool.stat("<test@msg>") } }
                assertIs<StatResult.Found>(next.getOrNull(), "the next STAT got $next")
                assertEquals(2, accepted.get(), "the STAT should have needed a new connection")
            } finally {
                pool.close()
            }
        }

    // --- Cancelled pipelined STAT batches ---

    /**
     * Reading off the answers a cancelled batch still owes took until the server had sent them
     * all, up to the drain budget (NntpClientPool.DRAIN_TIMEOUT_MS), and the caller waited for
     * it: seconds on a server slow to answer. They're read off without the caller, and the
     * connection is discarded if they don't all come within the budget.
     */
    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a pipelined STAT batch cancelled with many answers owed returns at once and does not pool its connection`() =
        runBlocking<Unit> {
            val accepted = launchServer { connection, cmd, _, out ->
                val id = cmd.removePrefix("STAT ")
                // The first connection answers its first STAT at once and each later one slowly.
                if (connection == 1 && id != "<a0@test>") Thread.sleep(SLOW_PIPELINE_ANSWER_MS)
                out.send("223 0 $id article exists")
            }
            val pool = pool()
            pool.wake()
            try {
                val ids = (0 until 64).map { "<a$it@test>" }
                val elapsed = measureTimeMillis {
                    assertNull(withTimeoutOrNull(CANCEL_AFTER_MS) { pool.statPipelined(ids) })
                }
                assertTrue(
                    elapsed < CANCEL_AFTER_MS + 1_000,
                    "the cancelled batch held its caller for ${elapsed}ms with about 60 answers owed"
                )

                val next = pool.stat("<later@test>")
                assertEquals("<later@test>", assertIs<StatResult.Found>(next).messageId, "the next STAT got $next")
                assertEquals(2, accepted.get(), "the STAT should have needed a new connection")
            } finally {
                pool.close()
            }
        }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a pipelined STAT batch cancelled with a few answers owed is read off and its connection reused`() =
        runBlocking<Unit> {
            val accepted = launchServer { _, cmd, _, out ->
                val id = cmd.removePrefix("STAT ")
                if (id == "<a1@test>") Thread.sleep(SLOW_ANSWER_MS)
                out.send("223 0 $id article exists")
            }
            val pool = pool()
            pool.wake()
            try {
                val ids = (0 until 3).map { "<a$it@test>" }
                val elapsed = measureTimeMillis {
                    assertNull(withTimeoutOrNull(CANCEL_AFTER_MS) { pool.statPipelined(ids) })
                }
                assertTrue(elapsed < SLOW_ANSWER_MS, "the cancelled batch waited ${elapsed}ms for the answers it owed")

                val next = pool.stat("<later@test>")
                assertEquals("<later@test>", assertIs<StatResult.Found>(next).messageId, "the next STAT got $next")
                assertEquals(1, accepted.get(), "the answers owed should have been read off and the connection reused")
            } finally {
                pool.close()
            }
        }

    /**
     * An availability check stops at its first missing article with the rest of its batch
     * owed. A server that answers at once has them all on their way: they're read off and the
     * connection reused, not reconnected for every check that stops early.
     */
    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a pipelined STAT batch stopped at its first answer, the rest coming at once, keeps its connection`() =
        runBlocking<Unit> {
            val accepted = launchServer { _, cmd, _, out ->
                val id = cmd.removePrefix("STAT ")
                out.send(if (id == "<a0@test>") "430 no such article" else "223 0 $id article exists")
            }
            val pool = pool()
            pool.wake()
            try {
                val ids = (0 until 64).map { "<a$it@test>" }
                val elapsed = measureTimeMillis {
                    val check = launch {
                        val context = coroutineContext
                        pool.statPipelined(ids) { _, result ->
                            // As a check does at its first missing article.
                            if (result is StatResult.NotFound) context.cancel()
                            context.ensureActive()
                        }
                    }
                    check.join()
                    assertTrue(check.isCancelled)
                }
                assertTrue(elapsed < 1_000, "the stopped batch held its caller for ${elapsed}ms")

                val next = pool.stat("<later@test>")
                assertEquals("<later@test>", assertIs<StatResult.Found>(next).messageId, "the next STAT got $next")
                assertEquals(1, accepted.get(), "the answers owed should have been read off and the connection reused")
            } finally {
                pool.close()
            }
        }

    // --- Blocks that catch a command's failure or cancellation and carry on ---
    // These pools don't retry, so a connection pooled out of step fails the next command
    // instead of being replaced behind the caller's back.

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a pipelined STAT that fails mid-batch in a block that carries on does not pool its connection`() =
        runBlocking<Unit> {
            val accepted = launchServer { _, cmd, _, out ->
                when (cmd) {
                    "STAT <a@test>" -> out.send("223 0 <a@test> article exists")
                    "STAT <b@test>" -> out.send("garbage")
                    "STAT <later@test>" -> out.send("223 0 <later@test> article exists")
                    else -> out.send("430 no such article")
                }
            }
            val pool = pool(commandRetries = 0)
            pool.wake()
            try {
                val failure = pool.withClient { client ->
                    runCatching { client.statPipelined(listOf("<a@test>", "<b@test>", "<c@test>")) }.exceptionOrNull()
                }
                assertIs<NntpProtocolException>(failure)

                val next = pool.stat("<later@test>")
                assertIs<StatResult.Found>(next, "the next STAT read an answer the batch still owed: $next")
                assertEquals(2, accepted.get(), "the STAT should have needed a new connection")
            } finally {
                pool.close()
            }
        }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a read that timed out in a block that carries on does not pool its connection`() = runBlocking<Unit> {
        val accepted = launchServer { _, cmd, _, out ->
            when (cmd) {
                "STAT <stalled@test>" -> Thread.sleep(2_000)
                else -> out.send("223 0 <test@msg> article exists")
            }
        }
        val pool = pool(commandRetries = 0, readTimeoutMs = 300)
        pool.wake()
        try {
            val failure = pool.withClient { client ->
                runCatching { client.stat("<stalled@test>") }.exceptionOrNull()
            }
            assertIs<NntpConnectionException>(failure)

            val next = runCatching { pool.stat("<test@msg>") }
            assertIs<StatResult.Found>(next.getOrNull(), "the next STAT got $next")
            assertEquals(2, accepted.get(), "the STAT should have needed a new connection")
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a pipelined STAT cut short in a block that carries on does not pool its connection`() =
        runBlocking<Unit> {
            val accepted = launchServer { _, cmd, _, out ->
                when (cmd) {
                    "STAT <a@test>" -> out.send("223 0 <a@test> article exists")
                    "STAT <slow@test>" -> {
                        Thread.sleep(SLOW_ANSWER_MS)
                        out.send("223 0 <slow@test> article exists")
                    }
                    else -> out.send("430 no such article")
                }
            }
            val pool = pool(commandRetries = 0)
            pool.wake()
            try {
                val results = pool.withClient { client ->
                    withTimeoutOrNull(CANCEL_AFTER_MS) { client.statPipelined(listOf("<a@test>", "<slow@test>")) }
                }
                assertNull(results)

                val next = pool.stat("<missing@test>")
                assertIs<StatResult.NotFound>(next, "the next STAT read an answer the batch still owed: $next")
                assertEquals(2, accepted.get(), "the STAT should have needed a new connection")
            } finally {
                pool.close()
            }
        }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a STAT cut short in a block that carries on does not pool its connection`() = runBlocking<Unit> {
        val accepted = launchServer { _, cmd, _, out ->
            when (cmd) {
                "STAT <slow@test>" -> {
                    Thread.sleep(SLOW_ANSWER_MS)
                    out.send("223 0 <slow@test> article exists")
                }
                else -> out.send("430 no such article")
            }
        }
        val pool = pool(commandRetries = 0)
        pool.wake()
        try {
            val result = pool.withClient { client ->
                withTimeoutOrNull(CANCEL_AFTER_MS) { client.stat("<slow@test>") }
            }
            assertNull(result)

            val next = pool.stat("<missing@test>")
            assertIs<StatResult.NotFound>(next, "the next STAT read the cut-short STAT's answer: $next")
            assertEquals(2, accepted.get(), "the STAT should have needed a new connection")
        } finally {
            pool.close()
        }
    }

    /**
     * A 430 ends a block without its connection being discarded, as the wire is in step after
     * one; not so when the block had swallowed a failure before (here the 430 is the batch's).
     */
    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a block that swallowed a failure and then ends with a 430 does not pool its connection`() =
        runBlocking<Unit> {
            val accepted = launchServer { _, cmd, _, out ->
                when (cmd) {
                    "STAT <a@test>" -> out.send("223 0 <a@test> article exists")
                    "STAT <b@test>" -> out.send("garbage")
                    "STAT <later@test>" -> out.send("223 0 <later@test> article exists")
                    else -> out.send("430 no such article")
                }
            }
            val pool = pool(commandRetries = 0)
            pool.wake()
            try {
                assertFailsWith<ArticleNotFoundException> {
                    pool.withClient { client ->
                        runCatching { client.statPipelined(listOf("<a@test>", "<b@test>", "<c@test>")) }
                        client.article("<x@test>")
                    }
                }

                val next = pool.stat("<later@test>")
                assertIs<StatResult.Found>(next, "the next STAT read an answer still owed: $next")
                assertEquals(2, accepted.get(), "the STAT should have needed a new connection")
            } finally {
                pool.close()
            }
        }

    /** A command the server refused cleanly leaves the wire in step: its connection stays pooled. */
    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a command refused cleanly in a block that carries on keeps its connection`() = runBlocking<Unit> {
        val accepted = launchServer { _, cmd, _, out ->
            when {
                cmd.startsWith("GROUP") -> out.send("411 no such group")
                else -> out.send("223 0 <test@msg> article exists")
            }
        }
        val pool = pool(commandRetries = 0)
        pool.wake()
        try {
            val failure = pool.withClient { client -> runCatching { client.group("alt.missing") }.exceptionOrNull() }
            assertIs<NntpProtocolException>(failure)

            assertIs<StatResult.Found>(pool.stat("<test@msg>"))
            assertEquals(1, accepted.get(), "the connection should have been reused")
        } finally {
            pool.close()
        }
    }

    private companion object {
        // The server answers the slow command this long after reading it, and the caller gives
        // up on it sooner: a cancellation with the answer still owed. Well inside the pool's
        // drain budget (NntpClientPool.DRAIN_TIMEOUT_MS), so a drain gets the answer.
        const val SLOW_ANSWER_MS = 600L
        const val CANCEL_AFTER_MS = 200L

        // A server that answers each pipelined STAT this long after the one before: the 60 or
        // so a cancelled batch of 64 still owes would take it seconds, beyond the drain budget.
        const val SLOW_PIPELINE_ANSWER_MS = 100L
    }
}
