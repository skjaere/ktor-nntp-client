package io.skjaere.nntp

import io.ktor.network.selector.*
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
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class NntpClientPoolMetricsTest {

    private lateinit var serverSocket: ServerSocket
    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope
    private lateinit var registry: SimpleMeterRegistry

    @BeforeEach
    fun setUp() {
        serverSocket = ServerSocket(0)
        selectorManager = SelectorManager(Dispatchers.IO)
        poolScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        registry = SimpleMeterRegistry()
        Metrics.addRegistry(registry)
    }

    @AfterEach
    fun tearDown() {
        Metrics.removeRegistry(registry)
        poolScope.cancel()
        serverSocket.close()
        selectorManager.close()
    }

    private fun launchCommandServer(
        maxAccepts: Int = Int.MAX_VALUE,
        acceptedSockets: CopyOnWriteArrayList<Socket> = CopyOnWriteArrayList()
    ): CopyOnWriteArrayList<Socket> {
        Thread {
            var accepted = 0
            while (accepted < maxAccepts) {
                try {
                    val client = serverSocket.accept()
                    acceptedSockets.add(client)
                    Thread {
                        try {
                            val reader = BufferedReader(InputStreamReader(client.getInputStream()))
                            val out = client.getOutputStream()
                            out.write("200 welcome\r\n".toByteArray())
                            out.flush()
                            while (!client.isClosed) {
                                val cmd = reader.readLine() ?: break
                                when {
                                    cmd == "DATE" -> {
                                        out.write("111 20260224120000\r\n".toByteArray())
                                        out.flush()
                                    }
                                    cmd.startsWith("STAT") -> {
                                        out.write("223 0 <test@msg> article exists\r\n".toByteArray())
                                        out.flush()
                                    }
                                    cmd == "QUIT" -> {
                                        out.write("205 bye\r\n".toByteArray())
                                        out.flush()
                                        break
                                    }
                                    else -> {
                                        out.write("500 unknown command\r\n".toByteArray())
                                        out.flush()
                                    }
                                }
                            }
                        } catch (_: Exception) {
                        }
                    }.start()
                    accepted++
                } catch (_: Exception) {
                    break
                }
            }
        }.start()
        return acceptedSockets
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `idle and active gauges reflect pool state`() = runBlocking {
        launchCommandServer(maxAccepts = 10)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 2,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0,
        )
        pool.wake()

        try {
            val poolName = "localhost:${serverSocket.localPort}"

            // Ensure both connections are established by checking out concurrently
            val gate = CompletableDeferred<Unit>()
            listOf(
                async { pool.withClient { gate.await() } },
                async { pool.withClient { gate.await() } }
            ).also { gate.complete(Unit) }.awaitAll()
            // Allow connections to settle into idle pool
            delay(100)

            val idleGauge = registry.find("nntp.pool.idle").tag("pool.name", poolName).gauge()
            val activeGauge = registry.find("nntp.pool.active").tag("pool.name", poolName).gauge()
            assertNotNull(idleGauge)
            assertNotNull(activeGauge)

            // Both idle
            assertEquals(2.0, idleGauge.value())
            assertEquals(0.0, activeGauge.value())

            pool.withClient { _ ->
                // one client checked out, one idle
                assertEquals(1.0, idleGauge.value())
                assertEquals(1.0, activeGauge.value())
            }

            // Back to all idle
            assertEquals(2.0, idleGauge.value())
            assertEquals(0.0, activeGauge.value())
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `waiter gauge increments when pool exhausted`() = runBlocking {
        launchCommandServer(maxAccepts = 10)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0,
        )
        pool.wake()

        try {
            val poolName = "localhost:${serverSocket.localPort}"
            // Per-priority gauges — sum across priorities for a total-waiters check.
            // Pool init pre-registers priority=0; ensurePriorityGauges fires for other
            // priorities the first time a client is acquired or a waiter enqueued at
            // that priority.
            fun totalWaiters(): Double =
                registry.find("nntp.pool.waiters")
                    .tag("pool.name", poolName)
                    .gauges()
                    .sumOf { it.value() }

            assertEquals(0.0, totalWaiters())

            val gate = CompletableDeferred<Unit>()
            val waiterReady = CompletableDeferred<Unit>()

            // Hold the only client
            val holder = async {
                pool.withClient {
                    waiterReady.complete(Unit)
                    gate.await()
                }
            }

            waiterReady.await()

            // Launch a waiter that will block
            val waiter = async {
                pool.withClient(priority = 1) {
                    it.stat("<test@msg>")
                }
            }

            delay(200) // let the waiter enqueue
            assertEquals(1.0, totalWaiters())

            // Release
            gate.complete(Unit)
            holder.await()
            waiter.await()

            assertEquals(0.0, totalWaiters())
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `acquire timer records duration with priority tag`() = runBlocking {
        launchCommandServer(maxAccepts = 10)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0,
        )
        pool.wake()

        try {
            val poolName = "localhost:${serverSocket.localPort}"

            pool.withClient(priority = 5) { _ -> }

            val timer = registry.find("nntp.pool.acquire")
                .tag("pool.name", poolName)
                .tag("priority", "5")
                .timer()
            assertNotNull(timer)
            assertEquals(1, timer.count())
            assertTrue(timer.totalTime(TimeUnit.NANOSECONDS) > 0)
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `sleeping gauge reflects sleep and wake state`() = runBlocking {
        // Accept: 1 initial + 1 after wake
        launchCommandServer(maxAccepts = 2)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0,
        )
        pool.wake()

        try {
            val poolName = "localhost:${serverSocket.localPort}"
            val sleepingGauge = registry.find("nntp.pool.sleeping").tag("pool.name", poolName).gauge()
            assertNotNull(sleepingGauge)

            assertEquals(0.0, sleepingGauge.value())

            pool.sleep()
            assertEquals(1.0, sleepingGauge.value())

            pool.wake()
            assertEquals(0.0, sleepingGauge.value())
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `close removes meters from registry`() = runBlocking {
        launchCommandServer(maxAccepts = 10)

        val poolName = "localhost:${serverSocket.localPort}"
        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0,
        )
        pool.wake()

        // Record an acquire timer
        pool.withClient(priority = 0) { _ -> }

        // Verify meters exist
        assertNotNull(registry.find("nntp.pool.idle").tag("pool.name", poolName).gauge())
        assertNotNull(registry.find("nntp.pool.acquire").tag("pool.name", poolName).timer())

        pool.close()

        // After close, meters should be removed
        val idleGauge = registry.find("nntp.pool.idle").tag("pool.name", poolName).gauge()
        val acquireTimer = registry.find("nntp.pool.acquire").tag("pool.name", poolName).timer()
        assertEquals(null, idleGauge)
        assertEquals(null, acquireTimer)
    }
}
