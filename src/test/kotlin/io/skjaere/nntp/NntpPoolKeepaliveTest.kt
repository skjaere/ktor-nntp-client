package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.system.measureTimeMillis
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The keepalive checks the connections that sat idle a whole interval, all at once, and hands each back as it
 * answers: a connection in use between ticks is never taken out, nor one that answered kept behind one that doesn't.
 */
@Timeout(30, unit = TimeUnit.SECONDS)
class NntpPoolKeepaliveTest {

    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope
    private lateinit var callers: CoroutineScope
    private lateinit var registry: SimpleMeterRegistry

    @BeforeEach
    fun setUp() {
        selectorManager = SelectorManager(Dispatchers.IO)
        poolScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        callers = CoroutineScope(Dispatchers.IO + SupervisorJob())
        registry = SimpleMeterRegistry()
        Metrics.addRegistry(registry)
    }

    @AfterEach
    fun tearDown() {
        Metrics.removeRegistry(registry)
        callers.cancel()
        poolScope.cancel()
        selectorManager.close()
    }

    private fun pool(
        server: IdleSilenceServer,
        maxConnections: Int,
        keepaliveIntervalMs: Long,
        keepaliveTimeoutMs: Long = NntpClientPool.KEEPALIVE_COMMAND_TIMEOUT_MS,
    ) = NntpClientPool(
        host = "localhost",
        port = server.port,
        selectorManager = selectorManager,
        maxConnections = maxConnections,
        scope = poolScope,
        keepaliveIntervalMs = keepaliveIntervalMs,
        idleGracePeriodMs = 0,
        keepaliveTimeoutMs = keepaliveTimeoutMs,
    )

    private fun gauge(server: IdleSilenceServer, name: String): Double? =
        registry.find(name).tag("pool.name", "localhost:${server.port}").gauge()?.value()

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

    private fun IdleSilenceServer.dates() = commands.filter { it.line == "DATE" }

    /**
     * The sweep used to take every idle connection out of the pool, the one in use between a caller's commands too,
     * and DATE them one after another, oldest first: behind each silent one the rest waited out its 10 s timeout, and
     * the caller with them, the pool being full.
     */
    @Test
    fun `a sweep does not hold the connection a caller keeps using behind cold ones gone silent`() = runBlocking<Unit> {
        IdleSilenceServer(idleAfterMs = SILENT_AFTER_MS).use { server ->
            val pool = pool(server, maxConnections = 3, keepaliveIntervalMs = INTERVAL_MS)
            try {
                pool.wake()
                pool.fillIdle(3)
                // The pool lends the connection returned last, so one stays hot and two go cold, and silent.
                val slowest = (1..STATS).maxOf { n ->
                    delay(STAT_EVERY_MS)
                    measureTimeMillis {
                        val answer = withTimeoutOrNull(STAT_BOUND_MS) { pool.stat("<hot$n@test>") }
                        assertNotNull(answer, "STAT $n waited more than $STAT_BOUND_MS ms for a connection")
                    }
                }
                assertTrue(slowest < STAT_BOUND_MS, "slowest STAT: $slowest ms")
                // Two sweeps have run, and the cold ones were checked.
                assertEquals(2, server.dates().map { it.connection }.distinct().size, "connections DATEd")
                assertEquals(3, server.accepted.get(), "connections opened")
            } finally {
                pool.close()
            }
        }
    }

    /** One after another, the DATEs behind a silent one waited out its timeout before they were even sent. */
    @Test
    fun `a sweep checks its connections at once and hands each back as it answers`() = runBlocking<Unit> {
        // Two of every three connections go silent once idle; the third stays healthy.
        IdleSilenceServer(idleAfterMs = SILENT_AFTER_MS, affected = 2, outOf = 3).use { server ->
            val pool = pool(server, maxConnections = 3, keepaliveIntervalMs = INTERVAL_MS)
            try {
                pool.wake()
                pool.fillIdle(3)
                // All three are cold by the second sweep at the latest.
                val dated = withTimeoutOrNull(3 * INTERVAL_MS + STAT_BOUND_MS) {
                    while (server.dates().size < 3) delay(10)
                }
                assertNotNull(dated, "DATEs sent: ${server.dates().map { it.connection }}")
                // The healthy one is back while the silent ones' DATEs still wait.
                val answer = withTimeoutOrNull(STAT_BOUND_MS) { pool.stat("<after@test>") }
                assertIs<StatResult.Found>(answer, "the STAT after the sweep got $answer")
                assertEquals(3, server.accepted.get(), "the healthy connection should have served the STAT")
            } finally {
                pool.close()
            }
        }
    }

    @Test
    fun `a sweep takes a connection whose DATE goes unanswered for keepaliveTimeoutMs for dead`() = runBlocking<Unit> {
        IdleSilenceServer(idleAfterMs = SILENT_AFTER_MS).use { server ->
            val pool = pool(server, maxConnections = 2, keepaliveIntervalMs = INTERVAL_MS, keepaliveTimeoutMs = 300)
            try {
                pool.wake()
                pool.fillIdle(2)
                val dated = withTimeoutOrNull(3 * INTERVAL_MS) { while (server.dates().size < 2) delay(10) }
                assertNotNull(dated, "DATEs sent: ${server.dates().map { it.connection }}")
                // Bound: the 300 ms timeout and then some, far below the 10 s default.
                val gone = withTimeoutOrNull(STAT_BOUND_MS) {
                    while (gauge(server, "nntp.pool.size") != 0.0) delay(10)
                }
                assertNotNull(gone, "pool size ${gauge(server, "nntp.pool.size")} a second after the DATEs")
                // Nobody asked for a connection meanwhile, so none was opened in their place.
                assertEquals(2, server.accepted.get(), "connections opened")
            } finally {
                pool.close()
            }
        }
    }

    @Test
    fun `the keepalive's defaults are as before`() {
        assertEquals(60_000L, NntpClientPool.DEFAULT_KEEPALIVE_INTERVAL_MS)
        assertEquals(10_000L, NntpClientPool.KEEPALIVE_COMMAND_TIMEOUT_MS)
    }

    private companion object {
        const val INTERVAL_MS = 600L
        const val SILENT_AFTER_MS = 400L
        const val STAT_EVERY_MS = 100L
        const val STAT_BOUND_MS = 1_000L

        // Two sweeps' worth of STATs, and some.
        const val STATS = 20
    }
}
