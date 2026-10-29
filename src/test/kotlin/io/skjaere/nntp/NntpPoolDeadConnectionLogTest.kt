package io.skjaere.nntp

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
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
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * When the keepalive or a borrow's check finds idle connections dead, the pool says so at INFO, where a log without
 * DEBUG lines shows it: how many, silent or closed, how long they had sat unused, and which check found them. At most
 * one line per pool every DEAD_REPORT_INTERVAL_MS, the ones found meanwhile summed into the next.
 */
@Timeout(30, unit = TimeUnit.SECONDS)
class NntpPoolDeadConnectionLogTest {

    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope
    private lateinit var callers: CoroutineScope
    private val logger = LoggerFactory.getLogger(NntpClientPool::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()
    private var level: Level? = null

    @BeforeEach
    fun setUp() {
        selectorManager = SelectorManager(Dispatchers.IO)
        poolScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        callers = CoroutineScope(Dispatchers.IO + SupervisorJob())
        appender.start()
        level = logger.level
        logger.level = Level.INFO
        logger.addAppender(appender)
    }

    @AfterEach
    fun tearDown() {
        logger.detachAppender(appender)
        logger.level = level
        callers.cancel()
        poolScope.cancel()
        selectorManager.close()
    }

    /** The INFO lines about dead connections, as numbers: found, silent, closed, by the keepalive, on borrow. */
    private fun deadLines(): List<List<Long>> = appender.list.toList()
        .filter { it.level == Level.INFO && "dead idle connection" in it.formattedMessage }
        .map { event ->
            val message = event.formattedMessage
            val found = Regex("""found (\d+) dead idle connection\(s\), (\d+) silent and (\d+) closed""")
                .find(message) ?: error("not the line's shape: $message")
            val by = Regex("""\((\d+) by the keepalive, (\d+) on borrow\)""").find(message)
                ?: error("not the line's shape: $message")
            (found.groupValues.drop(1) + by.groupValues.drop(1)).map(String::toLong)
        }

    /** How long the line's connections had sat unused, as it gives it: the least and the most. */
    private fun unusedFor(): List<LongRange> = appender.list.toList()
        .mapNotNull { Regex("""unused for (\d+)-(\d+)ms""").find(it.formattedMessage) }
        .map { it.groupValues[1].toLong()..it.groupValues[2].toLong() }

    private fun keepalivePool(server: IdleSilenceServer) = NntpClientPool(
        host = "localhost", port = server.port, selectorManager = selectorManager, maxConnections = 2,
        scope = poolScope, keepaliveIntervalMs = 300, idleGracePeriodMs = 0, keepaliveTimeoutMs = 300,
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

    @Test
    fun `a sweep that finds a connection gone silent says so at INFO`() = runBlocking<Unit> {
        IdleSilenceServer(idleAfterMs = 200).use { server ->
            val pool = keepalivePool(server)
            try {
                pool.wake()
                pool.fillIdle(1)
                val logged = withTimeoutOrNull(3_000) { while (deadLines().isEmpty()) delay(10) }
                assertNotNull(logged, "no INFO line about the dead connection")
                assertEquals(listOf(listOf(1L, 1L, 0L, 1L, 0L)), deadLines(), "the line's numbers")
                val unused = unusedFor().single()
                assertTrue(unused.first >= 300 && unused.last < 3_000, "unused for $unused ms")
            } finally {
                pool.close()
            }
        }
    }

    @Test
    fun `a sweep that finds every connection alive says nothing`() = runBlocking<Unit> {
        IdleSilenceServer().use { server ->
            val pool = keepalivePool(server)
            try {
                pool.wake()
                pool.fillIdle(2)
                val dated = withTimeoutOrNull(3_000) { while (server.commands.none { it.line == "DATE" }) delay(10) }
                assertNotNull(dated, "the keepalive never sent DATE")
                delay(300)
                assertEquals(emptyList(), deadLines(), "lines about dead connections")
            } finally {
                pool.close()
            }
        }
    }

    @Test
    fun `dead connections found within the interval are summed into the next line`() = runBlocking<Unit> {
        IdleSilenceServer(idleAfterMs = 200, onIdle = OnIdle.KICK).use { server ->
            val pool = NntpClientPool(
                host = "localhost", port = server.port, selectorManager = selectorManager, maxConnections = 3,
                scope = poolScope, keepaliveIntervalMs = 0, idleGracePeriodMs = 0, validateAfterIdleMs = 100,
            )
            pool.deadReportIntervalMs = INTERVAL_MS
            try {
                pool.fillIdle(3)
                delay(400)
                // The borrow's check finds one closed, and the other two, checked at once, too.
                pool.stat("<after@test>")
                delay(INTERVAL_MS / 2)
                assertEquals(listOf(listOf(1L, 0L, 1L, 0L, 1L)), deadLines(), "the lines within the interval")
                delay(INTERVAL_MS)
                assertEquals(
                    listOf(listOf(1L, 0L, 1L, 0L, 1L), listOf(2L, 0L, 2L, 0L, 2L)),
                    deadLines(),
                    "the lines once the interval is over",
                )
            } finally {
                pool.close()
            }
        }
    }

    @Test
    fun `at most one line every 10 s`() {
        assertEquals(10_000L, NntpClientPool.DEAD_REPORT_INTERVAL_MS)
    }

    private companion object {
        const val INTERVAL_MS = 1_000L
    }
}
