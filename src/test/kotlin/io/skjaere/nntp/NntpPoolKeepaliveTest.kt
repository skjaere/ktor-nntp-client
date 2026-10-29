package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
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
 * The keepalive checks, all at once, the connections nobody used in the last tenth of an interval, and hands each back
 * as it answers: one a caller keeps using stays theirs or comes back at once, none that answered is kept behind one
 * that doesn't, and none goes much over an interval unchecked.
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
        idleEvictionTimeoutMs: Long = NntpClientPool.DEFAULT_IDLE_EVICTION_TIMEOUT_MS,
    ) = NntpClientPool(
        host = "localhost",
        port = server.port,
        selectorManager = selectorManager,
        maxConnections = maxConnections,
        scope = poolScope,
        keepaliveIntervalMs = keepaliveIntervalMs,
        idleGracePeriodMs = 0,
        keepaliveTimeoutMs = keepaliveTimeoutMs,
        idleEvictionTimeoutMs = idleEvictionTimeoutMs,
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

    /** The connections that were sent QUIT. */
    private fun IdleSilenceServer.quitOn() = commands.filter { it.line == "QUIT" }.map { it.connection }.toSet()

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
                // Two sweeps have run, and the cold ones were checked. The hot one may have been too, when a tick
                // came more than a tenth of an interval after its last STAT: it answered, and was back at once.
                val hot = server.commands.filter { it.line.startsWith("STAT <hot") }.map { it.connection }.toSet()
                assertEquals(1, hot.size, "connections the STATs were served on")
                val dated = server.dates().map { it.connection }.toSet()
                assertEquals(setOf(0, 1, 2) - hot, dated - hot, "cold connections DATEd")
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

    /**
     * Any answer to a keepalive DATE counted: the "400 idle timeout" a server sends before it closes an idle connection
     * was taken for DATE's, and the connection kept until the next command on it failed.
     */
    @Test
    fun `a sweep discards a connection whose DATE gets any answer but 111`() = runBlocking<Unit> {
        IdleSilenceServer(idleAfterMs = SILENT_AFTER_MS, onIdle = OnIdle.KICK).use { server ->
            val pool = pool(server, maxConnections = 1, keepaliveIntervalMs = INTERVAL_MS)
            try {
                pool.wake()
                pool.fillIdle(1)
                val kicked = withTimeoutOrNull(3 * INTERVAL_MS) { while (server.silenced.get() == 0) delay(5) }
                assertNotNull(kicked, "the sweep never sent DATE")
                // Well before the next sweep, which would find the connection closed.
                delay(INTERVAL_MS / 3)
                assertEquals(0.0, gauge(server, "nntp.pool.idle"), "idle connections after the sweep")
                assertEquals(0.0, gauge(server, "nntp.pool.size"), "pool size after the sweep")
            } finally {
                pool.close()
            }
        }
    }

    /**
     * A sweep put the connections it checked back after the one in use, at the end the pool lends from: the next
     * borrow took a cold one, and eviction, which stops at the first connection used within idleEvictionTimeoutMs
     * from the oldest end, always met the one in use until the last sweep. A pool a caller kept using never shrank.
     */
    @Test
    fun `a pool a caller keeps using shrinks to one connection while the keepalive checks the others`() =
        runBlocking<Unit> {
            IdleSilenceServer().use { server ->
                val pool = pool(
                    server, maxConnections = 3, keepaliveIntervalMs = SHRINK_INTERVAL_MS,
                    idleEvictionTimeoutMs = EVICT_AFTER_MS,
                )
                try {
                    pool.wake()
                    pool.fillIdle(3)
                    var stats = 0
                    val shrunk = withTimeoutOrNull(SHRINK_BOUND_MS) {
                        while (gauge(server, "nntp.pool.size") != 1.0) {
                            delay(STAT_EVERY_MS)
                            pool.stat("<trickle${stats++}@test>")
                        }
                    }
                    assertNotNull(shrunk, "pool size ${gauge(server, "nntp.pool.size")} after $stats STATs")
                    // The cold ones were checked meanwhile.
                    assertTrue(server.dates().isNotEmpty(), "the keepalive never sent DATE")
                    // And the pool shrank by eviction, which sends QUIT, not by finding them dead, which doesn't. Not
                    // told by the connections opened: a STAT during the DATE an eviction's tick sends the warm one may
                    // open another, evicted in its turn. The QUITs go out just after the size drops.
                    val quit = withTimeoutOrNull(QUIT_BOUND_MS) { while (server.quitOn().size < 2) delay(10) }
                    assertNotNull(quit, "connections QUIT: ${server.quitOn()}")
                } finally {
                    pool.close()
                }
            }
        }

    /**
     * Only the connections idle a whole interval were checked. One given back just after a tick was a little short of
     * it at the next, and went nearly two intervals without a command: past a NAT or a provider that ends flows idle a
     * little over one interval, where checking every idle connection each tick had kept it.
     */
    @Test
    fun `a connection given back just after a tick is checked at the next`() = runBlocking<Unit> {
        IdleSilenceServer().use { server ->
            val pool = pool(server, maxConnections = 1, keepaliveIntervalMs = GAP_INTERVAL_MS)
            try {
                // The keepalive's ticks count from the wake, and the connection is given back just after it.
                pool.wake()
                pool.stat("<once@test>")
                val dated = withTimeoutOrNull(3 * GAP_INTERVAL_MS) { while (server.dates().isEmpty()) delay(10) }
                assertNotNull(dated, "the keepalive never sent DATE")
                val stat = server.commands.first { it.line.startsWith("STAT") }
                val gapMs = (server.dates().first().atNanos - stat.atNanos) / NANOS_PER_MS
                assertTrue(gapMs < GAP_BOUND_MS, "the first DATE came $gapMs ms after the connection's STAT")
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

        // Eviction under a trickle: three intervals of checks before the cold ones are due, and the bound well past.
        const val SHRINK_INTERVAL_MS = 300L
        const val EVICT_AFTER_MS = 1_000L
        const val SHRINK_BOUND_MS = 5_000L

        // The QUITs eviction sends: local round trips, the bound far past them.
        const val QUIT_BOUND_MS = 2_000L

        // An interval, and the bound on the first DATE after a connection's last command: half an interval over one,
        // and half short of two.
        const val GAP_INTERVAL_MS = 1_000L
        const val GAP_BOUND_MS = 1_500L
        const val NANOS_PER_MS = 1_000_000L
    }
}
