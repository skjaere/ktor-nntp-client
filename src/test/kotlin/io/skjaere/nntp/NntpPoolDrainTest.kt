package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * A command its caller cancels mid-answer (a fetch's timeout, a seek) leaves the rest of the answer on the wire. It is
 * read off on the pool's scope, the connection held meanwhile: its caller goes on at once.
 */
@Timeout(20, unit = TimeUnit.SECONDS)
class NntpPoolDrainTest {

    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope

    @BeforeEach
    fun setUp() {
        selectorManager = SelectorManager(Dispatchers.IO)
        poolScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    }

    @AfterEach
    fun tearDown() {
        poolScope.cancel()
        selectorManager.close()
    }

    private fun pool(server: IdleSilenceServer) = NntpClientPool(
        host = "localhost",
        port = server.port,
        selectorManager = selectorManager,
        maxConnections = 1,
        scope = poolScope,
        keepaliveIntervalMs = 0,
        idleGracePeriodMs = 0,
    )

    private fun msSince(startNanos: Long) = (System.nanoTime() - startNanos) / 1_000_000

    /**
     * The drain after a cancelled BODY ran in the caller, up to its 3 s budget: a segment timeout on a server gone
     * silent cost the caller the timeout and 3 s more, a seek as long.
     */
    @Test
    fun `a BODY cancelled on a server that never answers returns at once and frees its slot when the drain gives up`() =
        runBlocking<Unit> {
            IdleSilenceServer(answerBody = false).use { server ->
                val pool = pool(server)
                try {
                    val started = System.nanoTime()
                    assertFailsWith<TimeoutCancellationException> {
                        withTimeout(CANCEL_AFTER_MS) { pool.withClient { it.bodyYencBytes("<never@test>") } }
                    }
                    val returnedMs = msSince(started)
                    assertTrue(returnedMs < RETURN_BOUND_MS, "the cancelled BODY held its caller for $returnedMs ms")

                    // The one connection is held while its drain waits, then discarded: the next command gets a
                    // new one.
                    val returned = System.nanoTime()
                    val next = withTimeoutOrNull(SLOT_BOUND_MS) { pool.stat("<next@test>") }
                    assertIs<StatResult.Found>(next, "the next STAT got $next after ${msSince(returned)} ms")
                    assertEquals(2, server.accepted.get(), "connections opened")
                } finally {
                    pool.close()
                }
            }
        }

    /**
     * A BODY cancelled right after it was sent, for an article the server answers 430, is still drained. When its
     * status line comes in two pieces and the cancel lands between them, the read cut short has taken the first.
     */
    @Test
    fun `a BODY cancelled between the two pieces of its 430 is read off and its connection kept`() = runBlocking<Unit> {
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
                            fun send(text: String) = out.run { write(text.toByteArray()); flush() }
                            send("200 welcome\r\n")
                            while (true) {
                                val line = reader.readLine() ?: break
                                when {
                                    line.startsWith("BODY ") -> {
                                        send("430 no such")
                                        Thread.sleep(SECOND_PIECE_AFTER_MS)
                                        send(" article\r\n")
                                    }
                                    line.startsWith("STAT ") ->
                                        send("223 0 ${line.removePrefix("STAT ")} article exists\r\n")
                                    else -> send("500 unknown command\r\n")
                                }
                            }
                        }
                    }
                }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
        val pool = NntpClientPool(
            host = "localhost", port = server.localPort, selectorManager = selectorManager, maxConnections = 1,
            scope = poolScope, keepaliveIntervalMs = 0, idleGracePeriodMs = 0,
        )
        try {
            val started = System.nanoTime()
            assertFailsWith<TimeoutCancellationException> {
                withTimeout(SECOND_PIECE_AFTER_MS / 2) { pool.withClient { it.bodyYencBytes("<missing@test>") } }
            }
            val returnedMs = msSince(started)
            assertTrue(returnedMs < RETURN_BOUND_MS, "the cancelled BODY held its caller for $returnedMs ms")
            val next = withTimeoutOrNull(SLOT_BOUND_MS) { pool.stat("<next@test>") }
            assertIs<StatResult.Found>(next, "the next STAT got $next")
            assertEquals(1, accepted.get(), "the 430 should have been read off and the connection kept")
        } finally {
            pool.close()
            server.close()
        }
    }

    private companion object {
        const val SECOND_PIECE_AFTER_MS = 400L

        const val CANCEL_AFTER_MS = 300L
        const val RETURN_BOUND_MS = 600L

        // The drain's 3 s budget (NntpClientPool.DRAIN_TIMEOUT_MS) and a connect, from the caller's return.
        const val SLOT_BOUND_MS = 3_500L
    }
}
