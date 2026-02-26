package io.skjaere.nntp

import io.ktor.network.selector.*
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
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
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class NntpClientPoolTest {

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
        serverSocket.close()
        selectorManager.close()
    }

    private fun buildYencArticle(data: ByteArray, filename: String): ByteArray {
        val encoded = RapidYenc.encode(data)
        val crc = RapidYenc.crc32(data)
        val sb = StringBuilder()
        sb.append("=ybegin line=128 size=${data.size} name=$filename\r\n")
        sb.append(String(encoded, Charsets.ISO_8859_1))
        if (!String(encoded, Charsets.ISO_8859_1).endsWith("\r\n")) {
            sb.append("\r\n")
        }
        sb.append("=yend size=${data.size} crc32=${crc.toString(16).padStart(8, '0')}\r\n")
        sb.append(".\r\n")
        return sb.toString().toByteArray(Charsets.ISO_8859_1)
    }

    private fun launchMultiConnectionServer(count: Int, articles: List<Pair<ByteArray, String>>) {
        Thread {
            repeat(count) { i ->
                try {
                    val client = serverSocket.accept()
                    Thread {
                        client.use { socket ->
                            val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                            val out = socket.getOutputStream()
                            out.write("200 welcome\r\n".toByteArray())
                            out.flush()
                            try {
                                val cmd = reader.readLine() ?: return@Thread
                                if (cmd.startsWith("BODY")) {
                                    val (data, filename) = articles[i]
                                    out.write("222 body follows\r\n".toByteArray())
                                    out.write(buildYencArticle(data, filename))
                                    out.flush()
                                }
                                Thread.sleep(5000)
                            } catch (_: Exception) {
                            }
                        }
                    }.start()
                } catch (_: Exception) {
                }
            }
        }.start()
    }

    /**
     * Launch a mock NNTP server that handles DATE and STAT commands,
     * and tracks accepted sockets for selective killing.
     */
    private fun launchCommandServer(
        maxAccepts: Int = Int.MAX_VALUE,
        acceptedSockets: CopyOnWriteArrayList<Socket> = CopyOnWriteArrayList(),
        killAfterAccept: Boolean = false,
        failStatOnce: AtomicBoolean = AtomicBoolean(false)
    ): CopyOnWriteArrayList<Socket> {
        Thread {
            var accepted = 0
            while (accepted < maxAccepts) {
                try {
                    val client = serverSocket.accept()
                    acceptedSockets.add(client)
                    if (killAfterAccept) {
                        client.close()
                        accepted++
                        continue
                    }
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
                                        if (failStatOnce.compareAndSet(true, false)) {
                                            // Close to simulate connection failure
                                            client.close()
                                            break
                                        }
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
    fun `bodyYencHeaders unlocks mutex - raw client, no supervisorScope`() = runBlocking {
        val data = "Test data".toByteArray()
        launchMultiConnectionServer(1, listOf(data to "test.bin"))

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        try {
            val headers = client.bodyYencHeaders("<test@msg>")
            assertEquals("test.bin", headers.name)

            delay(500) // let async cleanup finish
            assertFalse(client.connection.commandMutex.isLocked, "Mutex should be unlocked (raw, no supervisor)")
        } finally {
            client.close()
        }
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `bodyYencHeaders unlocks mutex - raw client, inside supervisorScope`() = runBlocking {
        val data = "Test data".toByteArray()
        launchMultiConnectionServer(1, listOf(data to "test.bin"))

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        try {
            supervisorScope {
                val headers = client.bodyYencHeaders("<test@msg>")
                assertEquals("test.bin", headers.name)
            }

            delay(500)
            assertFalse(client.connection.commandMutex.isLocked, "Mutex should be unlocked (raw, inside supervisor)")
        } finally {
            client.close()
        }
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `bodyYencHeaders twice through pool does not deadlock`() = runBlocking {
        val data1 = "First article data".toByteArray()
        val data2 = "Second article data".toByteArray()

        launchMultiConnectionServer(2, listOf(
            data1 to "first.bin",
            data2 to "second.bin"
        ))

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope
        )
        pool.wake()

        try {
            val headers1 = pool.bodyYencHeaders("<article-1@test>")
            assertEquals("first.bin", headers1.name)

            val headers2 = pool.bodyYencHeaders("<article-2@test>")
            assertEquals("second.bin", headers2.name)
        } finally {
            pool.close()
        }
    }

    // --- New tests for keepalive, retry, sleep/wake ---

    @Test
    @Timeout(30, unit = TimeUnit.SECONDS)
    fun `keepalive detects dead connection and reconnects`() = runBlocking {
        // Server accepts: initial connection + reconnection after keepalive detects dead
        val sockets = launchCommandServer(maxAccepts = 2)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 500,
            idleGracePeriodMs = 0  // disable auto-sleep
        )
        pool.wake()

        try {
            // Verify stat works initially
            val result = pool.stat("<test@msg>")
            assertTrue(result is StatResult.Found)

            // Kill the server-side socket to simulate dead connection
            delay(100)
            sockets.first().close()

            // Wait for keepalive to detect and schedule reconnect
            delay(1500)

            // Next operation should succeed via reconnected client
            val result2 = pool.stat("<test@msg>")
            assertTrue(result2 is StatResult.Found)
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `retry with different connection when first fails`() = runBlocking {
        // Pool size 2: first STAT fails (connection killed), retry gets second connection
        val failOnce = AtomicBoolean(false)
        // Accept: 2 initial + 1 reconnect
        launchCommandServer(maxAccepts = 3, failStatOnce = failOnce)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 2,
            scope = poolScope,
            keepaliveIntervalMs = 0,  // disable keepalive
            idleGracePeriodMs = 0
        )
        pool.wake()

        try {
            // Set up: next STAT on one connection will close the socket
            failOnce.set(true)

            // withClient should catch the NntpConnectionException, retry with the other client
            val result = pool.stat("<test@msg>")
            assertTrue(result is StatResult.Found)
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `both connections dead propagates exception`() = runBlocking {
        // Server accepts 2 initial connections, then kills all reconnect attempts
        val sockets = launchCommandServer(maxAccepts = 2)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 2,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0
        )
        pool.wake()

        // Kill both server-side sockets
        delay(100)
        sockets.forEach { it.close() }
        // Close the server so reconnects fail
        serverSocket.close()

        delay(200) // let TCP state settle

        assertFailsWith<NntpConnectionException> {
            pool.stat("<test@msg>")
        }

        pool.close()
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `pool size 1 waits for reconnect on retry`() = runBlocking {
        // Accept: 1 initial + 1 reconnect
        val failOnce = AtomicBoolean(false)
        launchCommandServer(maxAccepts = 2, failStatOnce = failOnce)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0
        )
        pool.wake()

        try {
            failOnce.set(true)

            // With pool size 1, retry gets the same (reconnecting) client.
            // ensureConnected() waits for reconnect to complete.
            val result = pool.stat("<test@msg>")
            assertTrue(result is StatResult.Found)
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `non-connection exceptions are not retried`() = runBlocking {
        // Server that responds 430 to STAT (article not found)
        Thread {
            repeat(1) {
                try {
                    val client = serverSocket.accept()
                    Thread {
                        try {
                            val reader = BufferedReader(InputStreamReader(client.getInputStream()))
                            val out = client.getOutputStream()
                            out.write("200 welcome\r\n".toByteArray())
                            out.flush()
                            while (!client.isClosed) {
                                val cmd = reader.readLine() ?: break
                                if (cmd.startsWith("STAT")) {
                                    out.write("430 no such article\r\n".toByteArray())
                                    out.flush()
                                } else {
                                    out.write("500 unknown\r\n".toByteArray())
                                    out.flush()
                                }
                            }
                        } catch (_: Exception) {
                        }
                    }.start()
                } catch (_: Exception) {
                }
            }
        }.start()

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0
        )
        pool.wake()

        try {
            // stat() returns StatResult.NotFound for 430, not an exception
            // But article() throws ArticleNotFoundException for 430
            // ArticleNotFoundException is NOT NntpConnectionException, so no retry
            assertFailsWith<ArticleNotFoundException> {
                pool.article("<nonexistent@msg>")
            }
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `grace period puts pool to sleep`() = runBlocking {
        val sockets = launchCommandServer(maxAccepts = 10)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 200,
            idleGracePeriodMs = 500
        )
        pool.wake()

        // Use withClient to ensure connection is established before checking
        pool.withClient { it.stat("<test@msg>") }
        assertEquals(1, sockets.size, "Should have 1 initial connection")

        // Wait for grace period + keepalive interval to trigger sleep
        delay(1500)

        // After sleeping, withClient should auto-wake creating a new server connection
        val result = pool.stat("<test@msg>")
        assertTrue(result is StatResult.Found)
        assertEquals(2, sockets.size, "Should have 2 connections total (initial + after wake)")

        pool.close()
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `auto-wake on withClient after sleep`() = runBlocking {
        // Accept: 1 initial + 1 after auto-wake
        launchCommandServer(maxAccepts = 2)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 200,
            idleGracePeriodMs = 500
        )
        pool.wake()

        // Wait for auto-sleep to trigger
        delay(1500)

        // Now call withClient — should auto-wake and transparently reconnect
        val result = pool.stat("<test@msg>")
        assertTrue(result is StatResult.Found)

        pool.close()
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `explicit sleep and wake`() = runBlocking {
        // Accept: 1 initial + 1 after wake
        val sockets = launchCommandServer(maxAccepts = 2)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0
        )
        pool.wake()

        // Verify working
        val result1 = pool.stat("<test@msg>")
        assertTrue(result1 is StatResult.Found)
        assertEquals(1, sockets.size, "Should have 1 initial connection")

        // Sleep
        pool.sleep()

        // Wake
        pool.wake()

        // Verify working after wake — server should have accepted a second connection
        val result2 = pool.stat("<test@msg>")
        assertTrue(result2 is StatResult.Found)
        assertEquals(2, sockets.size, "Should have 2 connections total (initial + after wake)")

        pool.close()
    }

    // --- Priority tests ---

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `higher priority waiters are served first when pool is exhausted`() = runBlocking {
        launchCommandServer(maxAccepts = 10)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0
        )
        pool.wake()

        try {
            val served = CopyOnWriteArrayList<String>()
            val gate = CompletableDeferred<Unit>()

            // Hold the only client
            val holder = async {
                pool.withClient {
                    gate.await() // hold until we release
                }
            }

            // Give the holder time to acquire
            delay(100)

            // Launch three waiters with different priorities
            val low = async {
                pool.withClient(priority = 1) {
                    served.add("low")
                }
            }
            delay(50) // ensure enqueue order
            val med = async {
                pool.withClient(priority = 5) {
                    served.add("med")
                }
            }
            delay(50)
            val high = async {
                pool.withClient(priority = 10) {
                    served.add("high")
                }
            }
            delay(50)

            // Release the holder
            gate.complete(Unit)
            holder.await()
            low.await()
            med.await()
            high.await()

            assertEquals(listOf("high", "med", "low"), served)
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `same priority waiters are served in FIFO order`() = runBlocking {
        launchCommandServer(maxAccepts = 10)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0
        )
        pool.wake()

        try {
            val served = CopyOnWriteArrayList<String>()
            val gate = CompletableDeferred<Unit>()

            val holder = async {
                pool.withClient {
                    gate.await()
                }
            }
            delay(100)

            val first = async {
                pool.withClient(priority = 0) {
                    served.add("first")
                }
            }
            delay(50)
            val second = async {
                pool.withClient(priority = 0) {
                    served.add("second")
                }
            }
            delay(50)
            val third = async {
                pool.withClient(priority = 0) {
                    served.add("third")
                }
            }
            delay(50)

            gate.complete(Unit)
            holder.await()
            first.await()
            second.await()
            third.await()

            assertEquals(listOf("first", "second", "third"), served)
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `cancelled waiter does not leak or block`() = runBlocking {
        launchCommandServer(maxAccepts = 10)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0
        )
        pool.wake()

        try {
            val gate = CompletableDeferred<Unit>()

            val holder = async {
                pool.withClient {
                    gate.await()
                }
            }
            delay(100)

            // Launch a waiter that we'll cancel
            val waiterJob = launch {
                pool.withClient(priority = 5) {
                    // should never get here
                }
            }
            delay(100)

            // Cancel the waiter while it's waiting
            waiterJob.cancel()
            delay(50)

            // Release the holder
            gate.complete(Unit)
            holder.await()

            // The client should not be leaked — subsequent calls should work
            val result = pool.stat("<test@msg>")
            assertTrue(result is StatResult.Found)
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `default priority is backward compatible`() = runBlocking {
        launchCommandServer(maxAccepts = 10)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0
        )
        pool.wake()

        try {
            // Call withClient without priority arg twice sequentially
            pool.withClient { client ->
                val result = client.stat("<test@msg>")
                assertTrue(result is StatResult.Found)
            }
            pool.withClient { client ->
                val result = client.stat("<test@msg>")
                assertTrue(result is StatResult.Found)
            }
        } finally {
            pool.close()
        }
    }
}
