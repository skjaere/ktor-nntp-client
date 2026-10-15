package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.coroutines.CoroutineContext
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Connections the pool takes out of its idle list to check, evict or clean up are its own
 * until it hands them back or closes them. A cancellation in between (the keepalive is
 * cancelled by sleep and close, wake runs in the caller's coroutine) must not lose them:
 * never closed, and still counted against maxConnections.
 */
class NntpPoolCleanupTest {

    private lateinit var serverSocket: ServerSocket
    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope
    private lateinit var callers: CoroutineScope
    private lateinit var registry: SimpleMeterRegistry

    @BeforeEach
    fun setUp() {
        serverSocket = ServerSocket(0)
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
        serverSocket.close()
        selectorManager.close()
    }

    /**
     * Greets each connection once [greet] is counted down and answers STAT, QUIT and, if
     * [answerDate], DATE; an unanswered DATE counts [dateReceived] down instead. Counts [closed]
     * down as each connection ends. Returns the number of connections accepted so far.
     */
    private fun launchServer(
        closed: CountDownLatch,
        greet: CountDownLatch = CountDownLatch(0),
        answerDate: Boolean = true,
        dateReceived: CountDownLatch = CountDownLatch(1)
    ): AtomicInteger {
        val accepted = AtomicInteger()
        Thread {
            while (true) {
                val client = try {
                    serverSocket.accept()
                } catch (_: Exception) {
                    break
                }
                accepted.incrementAndGet()
                Thread {
                    client.use { socket ->
                        try {
                            val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                            val out = socket.getOutputStream()
                            greet.await()
                            out.write("200 welcome\r\n".toByteArray())
                            out.flush()
                            while (true) {
                                val cmd = reader.readLine() ?: break
                                val reply = when {
                                    cmd.startsWith("STAT") -> "223 0 <test@msg> article exists"
                                    cmd == "DATE" && !answerDate -> {
                                        dateReceived.countDown()
                                        continue
                                    }
                                    cmd == "DATE" -> "111 20260224120000"
                                    cmd == "QUIT" -> "205 bye"
                                    else -> "500 unknown command"
                                }
                                out.write("$reply\r\n".toByteArray())
                                out.flush()
                                if (cmd == "QUIT") break
                            }
                        } catch (_: Exception) {
                        }
                    }
                    closed.countDown()
                }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
        return accepted
    }

    private fun pool(
        maxConnections: Int,
        scope: CoroutineScope = poolScope,
        keepaliveIntervalMs: Long = 0,
        idleEvictionTimeoutMs: Long = 0
    ) = NntpClientPool(
        host = "localhost",
        port = serverSocket.localPort,
        selectorManager = selectorManager,
        maxConnections = maxConnections,
        scope = scope,
        keepaliveIntervalMs = keepaliveIntervalMs,
        idleGracePeriodMs = 0,
        idleEvictionTimeoutMs = idleEvictionTimeoutMs
    )

    private fun gauge(name: String): Double? =
        registry.find(name).tag("pool.name", "localhost:${serverSocket.localPort}").gauge()?.value()

    /** Opens [count] connections at once and puts them back, idle. */
    private suspend fun NntpClientPool.fillIdle(count: Int) {
        val acquired = AtomicInteger()
        val allAcquired = CompletableDeferred<Unit>()
        List(count) {
            callers.async {
                withClient { client ->
                    if (acquired.incrementAndGet() == count) allAcquired.complete(Unit)
                    allAcquired.await()
                    client.stat("<test@msg>")
                }
            }
        }.awaitAll()
    }

    private suspend fun awaitGauge(name: String, expected: Double) {
        withTimeout(5_000) {
            while (gauge(name) != expected) delay(10)
        }
    }

    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `a keepalive cancelled mid-sweep does not lose the connections it was checking`() = runBlocking<Unit> {
        val connections = 3
        val closed = CountDownLatch(connections)
        val dateReceived = CountDownLatch(1)
        launchServer(closed, answerDate = false, dateReceived = dateReceived)
        val pool = pool(maxConnections = connections, keepaliveIntervalMs = 200)
        try {
            pool.wake()
            pool.fillIdle(connections)
            // The sweep has taken the idle connections out of the pool and waits on a DATE.
            assertTrue(dateReceived.await(5, TimeUnit.SECONDS), "the keepalive never sent DATE")

            pool.sleep() // cancels the keepalive

            assertTrue(
                closed.await(5, TimeUnit.SECONDS),
                "${closed.count} of $connections connection(s) were never closed"
            )
            assertEquals(0.0, gauge("nntp.pool.size"), "pool size")
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `an eviction cancelled before its connections are closed still closes them`() = runBlocking<Unit> {
        val closed = CountDownLatch(2)
        launchServer(closed)
        // The pool's coroutines run here, one step at a time, so the test can cancel the
        // keepalive between its eviction taking a connection out and the QUIT starting.
        val steps = SteppingDispatcher()
        val stepScope = CoroutineScope(steps + SupervisorJob())
        val pool = pool(
            maxConnections = 2,
            scope = stepScope,
            keepaliveIntervalMs = 300,
            idleEvictionTimeoutMs = 1
        )
        try {
            var full = false
            steps.pauseAfter = {
                val size = gauge("nntp.pool.size")
                if (size == 2.0) full = true
                full && size == 1.0 // one evicted (the pool keeps at least one)
            }
            pool.wake()
            pool.fillIdle(2)
            assertTrue(steps.awaitPaused(5_000), "the keepalive never evicted a connection")

            pool.close() // cancels the keepalive, and closes the connection it kept
            steps.resume()

            assertTrue(closed.await(5, TimeUnit.SECONDS), "${closed.count} connection(s) were never closed")
        } finally {
            steps.resume()
            pool.close()
            stepScope.cancel()
            steps.shutdown()
        }
    }

    /**
     * A connect still in flight when the pool went to sleep parks its connection idle, and
     * wake closes it. The waking caller may be cancelled already: withClient wakes the pool
     * in its caller's coroutine.
     */
    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `a wake cancelled while it cleans up still closes the connections left from before the sleep`() =
        runBlocking<Unit> {
            val closed = CountDownLatch(1)
            val greet = CountDownLatch(1)
            val accepted = launchServer(closed, greet = greet)
            val pool = pool(maxConnections = 1)
            try {
                pool.wake()
                val waiting = callers.launch { pool.withClient { } }
                withTimeout(5_000) { while (accepted.get() == 0) delay(10) }
                waiting.cancelAndJoin()
                pool.sleep()
                greet.countDown()
                awaitGauge("nntp.pool.idle", 1.0) // parked while the pool sleeps

                callers.launch {
                    currentCoroutineContext().job.cancel()
                    pool.wake()
                }.join()

                assertTrue(closed.await(5, TimeUnit.SECONDS), "the connection left from before the sleep was never closed")
                assertEquals(0.0, gauge("nntp.pool.size"), "pool size")
            } finally {
                pool.close()
            }
        }

    /**
     * Runs coroutines one task at a time on its own thread. After a task for which
     * [pauseAfter] holds (once), it stops until [resume], so a test can act between two steps.
     */
    private class SteppingDispatcher : CoroutineDispatcher() {
        private val tasks = LinkedBlockingQueue<Runnable>()
        private val paused = CountDownLatch(1)
        private val resumed = CountDownLatch(1)

        @Volatile
        var pauseAfter: () -> Boolean = { false }

        private val worker = Thread {
            try {
                while (true) {
                    tasks.take().run()
                    if (paused.count > 0 && pauseAfter()) {
                        paused.countDown()
                        resumed.await()
                    }
                }
            } catch (_: InterruptedException) {
            }
        }.apply {
            isDaemon = true
            start()
        }

        override fun dispatch(context: CoroutineContext, block: Runnable) {
            tasks.add(block)
        }

        fun awaitPaused(timeoutMs: Long): Boolean = paused.await(timeoutMs, TimeUnit.MILLISECONDS)

        fun resume() = resumed.countDown()

        fun shutdown() = worker.interrupt()
    }
}
