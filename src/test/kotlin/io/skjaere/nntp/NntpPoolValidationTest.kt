package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.skjaere.nntp.IdleSilenceServer.OnIdle
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * With validateAfterIdleMs, a connection nobody has used or checked for that long answers a DATE before it is lent: one
 * a NAT, a VPN or the provider dropped while it sat idle costs a short check and a connect, not a command's read
 * timeout, and the connections idle beside it are checked at once.
 */
@Timeout(30, unit = TimeUnit.SECONDS)
class NntpPoolValidationTest {

    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope
    private lateinit var callers: CoroutineScope

    @BeforeEach
    fun setUp() {
        selectorManager = SelectorManager(Dispatchers.IO)
        poolScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        callers = CoroutineScope(Dispatchers.IO + SupervisorJob())
    }

    @AfterEach
    fun tearDown() {
        callers.cancel()
        poolScope.cancel()
        selectorManager.close()
    }

    private fun pool(
        server: IdleSilenceServer,
        maxConnections: Int,
        readTimeoutMs: Long = NntpConnection.DEFAULT_READ_TIMEOUT_MS,
        validateAfterIdleMs: Long = 0,
        validateTimeoutMs: Long = NntpClientPool.DEFAULT_VALIDATE_TIMEOUT_MS,
    ) = NntpClientPool(
        host = "localhost",
        port = server.port,
        selectorManager = selectorManager,
        maxConnections = maxConnections,
        scope = poolScope,
        keepaliveIntervalMs = 0,
        idleGracePeriodMs = 0,
        readTimeoutMs = readTimeoutMs,
        validateAfterIdleMs = validateAfterIdleMs,
        validateTimeoutMs = validateTimeoutMs,
    )

    /** Opens [count] connections at once and puts them back, idle. */
    private suspend fun NntpClientPool.fillIdle(count: Int) {
        val acquired = AtomicInteger()
        val allAcquired = CompletableDeferred<Unit>()
        List(count) {
            callers.async {
                withClient { client ->
                    if (acquired.incrementAndGet() == count) allAcquired.complete(Unit)
                    allAcquired.await()
                    client.stat("<fill@test>")
                }
            }
        }.awaitAll()
    }

    /** [call]'s outcome and how long it took. */
    private suspend fun <T> timed(call: suspend () -> T): Pair<Result<T>, Long> {
        val started = System.nanoTime()
        val result = runCatching { call() }
        return result to (System.nanoTime() - started) / 1_000_000
    }

    /**
     * Lent unchecked, each silent idle connection cost the command its read timeout, and the retry met the next: the
     * command failed after two read timeouts.
     */
    @Test
    fun `a borrow checks connections idle past the threshold and moves on from silent ones to a new one`() =
        runBlocking<Unit> {
            IdleSilenceServer(idleAfterMs = 300).use { server ->
                val pool = pool(
                    server,
                    maxConnections = 2,
                    readTimeoutMs = 3_000,
                    validateAfterIdleMs = 100,
                    validateTimeoutMs = 300,
                )
                try {
                    pool.fillIdle(2)
                    delay(500)
                    val (answer, tookMs) = timed { pool.stat("<after@test>") }
                    assertIs<StatResult.Found>(answer.getOrNull(), "the STAT got $answer after $tookMs ms")
                    assertTrue(tookMs < 1_000, "the STAT took $tookMs ms")
                    assertEquals(3, server.accepted.get(), "the STAT should have been served on a third connection")
                } finally {
                    pool.close()
                }
            }
        }

    @Test
    fun `with the defaults a silent idle connection is lent unchecked and costs the read timeout`() =
        runBlocking<Unit> {
            IdleSilenceServer(idleAfterMs = 200).use { server ->
                val pool = pool(server, maxConnections = 1, readTimeoutMs = 500)
                try {
                    pool.fillIdle(1)
                    delay(400)
                    val (answer, tookMs) = timed { pool.stat("<after@test>") }
                    assertIs<StatResult.Found>(answer.getOrNull(), "the STAT got $answer")
                    assertTrue(tookMs >= 500, "the STAT took $tookMs ms, less than the read timeout")
                    assertEquals(0, server.commands.count { it.line == "DATE" }, "DATEs sent")
                    assertEquals(2, server.accepted.get(), "connections opened")
                } finally {
                    pool.close()
                }
            }
        }

    /** A connection closed while idle, or told off with a 400, fails its check at once: it costs a connect. */
    @ParameterizedTest
    @ValueSource(strings = ["RST", "FIN", "KICK"])
    fun `a connection closed while idle is dropped at once by its check`(onIdle: String) = runBlocking<Unit> {
        IdleSilenceServer(idleAfterMs = 200, onIdle = OnIdle.valueOf(onIdle)).use { server ->
            val pool = pool(server, maxConnections = 1, validateAfterIdleMs = 100)
            try {
                pool.fillIdle(1)
                delay(400)
                val (answer, tookMs) = timed { pool.stat("<after@test>") }
                assertIs<StatResult.Found>(answer.getOrNull(), "the STAT got $answer")
                // Far below the check's 2 s timeout.
                assertTrue(tookMs < 1_000, "the STAT took $tookMs ms")
                assertEquals(
                    listOf(0 to "STAT <fill@test>", 0 to "DATE", 1 to "STAT <after@test>"),
                    server.commands.map { it.connection to it.line },
                )
            } finally {
                pool.close()
            }
        }
    }

    @Test
    fun `a connection that answers its check is lent, and one used since is not checked again`() = runBlocking<Unit> {
        IdleSilenceServer().use { server ->
            val pool = pool(server, maxConnections = 1, validateAfterIdleMs = 100)
            try {
                pool.stat("<first@test>")
                delay(300)
                pool.stat("<second@test>")
                pool.stat("<third@test>")
                assertEquals(
                    listOf("STAT <first@test>", "DATE", "STAT <second@test>", "STAT <third@test>"),
                    server.commands.map { it.line },
                )
                assertEquals(1, server.accepted.get(), "connections opened")
            } finally {
                pool.close()
            }
        }
    }

    /**
     * Connections die together. Once a borrow's check finds one dead, the others idle as long are checked at once, and
     * the borrower doesn't wait for them: a new connection opens for it, and none for the dead ones nobody waits for.
     */
    @Test
    fun `once a check finds a connection dead, the others due one are checked at once`() = runBlocking<Unit> {
        IdleSilenceServer(idleAfterMs = 200).use { server ->
            val pool = pool(server, maxConnections = 3, validateAfterIdleMs = 100, validateTimeoutMs = CHECK_MS)
            try {
                pool.fillIdle(3)
                delay(400)
                val (answer, tookMs) = timed { pool.stat("<after@test>") }
                assertIs<StatResult.Found>(answer.getOrNull(), "the STAT got $answer")
                // One check's timeout and a connect, not three checks'.
                assertTrue(tookMs < CHECK_MS + 500, "the STAT took $tookMs ms")
                val statOn = server.commands.single { it.line == "STAT <after@test>" }.connection
                assertEquals(3, statOn, "the STAT's connection")

                delay(CHECK_MS + 300) // the other two checks give up
                val dates = server.commands.filter { it.line == "DATE" }
                assertEquals(3, dates.map { it.connection }.distinct().size, "connections checked")
                val first = dates.first()
                val others = dates.drop(1)
                assertTrue(
                    others.all { (it.atNanos - first.atNanos) / 1_000_000 >= CHECK_MS - 50 },
                    "the others were checked before the first check gave up",
                )
                assertTrue(
                    // One after another they would be a check's timeout apart.
                    (others.last().atNanos - others.first().atNanos) / 1_000_000 < CHECK_MS / 2,
                    "the others were checked one after another",
                )
                assertEquals(4, server.accepted.get(), "connections opened")
            } finally {
                pool.close()
            }
        }
    }

    /** A borrower cancelled while its check waits leaves no connection behind it, half-checked and still counted. */
    @Test
    fun `a borrow cancelled during its check frees the connection's slot`() = runBlocking<Unit> {
        IdleSilenceServer(idleAfterMs = 200).use { server ->
            val pool = pool(server, maxConnections = 1, validateAfterIdleMs = 100, validateTimeoutMs = 5_000)
            try {
                pool.fillIdle(1)
                delay(400)
                assertNull(withTimeoutOrNull(300) { pool.stat("<cancelled@test>") })
                // With the one slot still taken, the next STAT would wait for the acquire's 30 s.
                val (answer, tookMs) = timed { withTimeoutOrNull(1_000) { pool.stat("<next@test>") } }
                assertIs<StatResult.Found>(answer.getOrNull(), "the STAT got $answer after $tookMs ms")
                assertEquals(2, server.accepted.get(), "connections opened")
            } finally {
                pool.close()
            }
        }
    }

    /**
     * A borrower cancelled once its check had been answered, while it waited for the pool's lock, left the connection
     * neither idle nor lent and still counted: with one connection, every later command waited out the acquire's 30 s.
     */
    @Test
    fun `a borrow cancelled after its check was answered hands the connection back`() = runBlocking<Unit> {
        val dateReceived = CountDownLatch(1)
        val answerDate = CountDownLatch(1)
        val accepted = AtomicInteger()
        val server = ServerSocket(0)
        Thread {
            while (true) {
                val socket = runCatching { server.accept() }.getOrNull() ?: break
                accepted.incrementAndGet()
                Thread {
                    socket.use {
                        runCatching {
                            val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                            val out = socket.getOutputStream()
                            fun send(line: String) = out.run { write("$line\r\n".toByteArray()); flush() }
                            send("200 welcome")
                            while (true) {
                                val line = reader.readLine() ?: break
                                when {
                                    line == "DATE" -> {
                                        dateReceived.countDown()
                                        answerDate.await(5, TimeUnit.SECONDS)
                                        send("111 20261028120000")
                                    }
                                    line.startsWith("STAT ") ->
                                        send("223 0 ${line.removePrefix("STAT ")} article exists")
                                    else -> send("500 unknown command")
                                }
                            }
                        }
                    }
                }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
        val pool = NntpClientPool(
            host = "localhost", port = server.localPort, selectorManager = selectorManager, maxConnections = 1,
            scope = poolScope, keepaliveIntervalMs = 0, idleGracePeriodMs = 0, validateAfterIdleMs = 100,
        )
        val lock = NntpClientPool::class.java.getDeclaredField("poolMutex").apply { isAccessible = true }
            .get(pool) as Mutex
        try {
            pool.stat("<first@test>")
            delay(300)
            val borrow = callers.async { pool.stat("<cancelled@test>") }
            assertTrue(dateReceived.await(5, TimeUnit.SECONDS), "the borrow never checked its connection")
            // The check is answered while the pool's lock is held: the borrower waits for it, and is cancelled.
            lock.lock()
            try {
                answerDate.countDown()
                delay(300)
                borrow.cancel()
                delay(100)
            } finally {
                lock.unlock()
            }
            runCatching { borrow.await() }
            val next = withTimeoutOrNull(2_000) { pool.stat("<next@test>") }
            assertIs<StatResult.Found>(next, "the next STAT got $next")
            assertEquals(1, accepted.get(), "connections opened")
        } finally {
            pool.close()
            server.close()
        }
    }

    @Test
    fun `the check's timeout defaults to 2 s`() {
        assertEquals(2_000L, NntpClientPool.DEFAULT_VALIDATE_TIMEOUT_MS)
    }

    private companion object {
        const val CHECK_MS = 500L
    }
}
