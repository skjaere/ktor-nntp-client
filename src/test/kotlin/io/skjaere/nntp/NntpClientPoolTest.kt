package io.skjaere.nntp

import io.ktor.network.selector.*
import io.ktor.utils.io.cancel
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.toByteArray
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestCoroutineScheduler
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

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
        poolScope.cancel()
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


    /**
     * A server whose connections wait for [greet] before sending the welcome, so the pool's
     * connects stay in flight until the test lets them finish. Answers STAT and QUIT, and
     * counts [closed] down as each connection ends.
     */
    private fun launchGatedServer(greet: CountDownLatch, closed: CountDownLatch) {
        Thread {
            while (true) {
                val client = try {
                    serverSocket.accept()
                } catch (_: Exception) {
                    break
                }
                Thread {
                    client.use { socket ->
                        try {
                            val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                            val out = socket.getOutputStream()
                            greet.await()
                            out.write("200 welcome\r\n".toByteArray())
                            out.flush()
                            answerCommands(reader, out)
                        } catch (_: Exception) {
                        }
                    }
                    closed.countDown()
                }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
    }

    /** Answers STAT and QUIT on an open connection until the client quits or goes away. */
    private fun answerCommands(reader: BufferedReader, out: OutputStream) {
        while (true) {
            val cmd = reader.readLine() ?: break
            val reply = when {
                cmd.startsWith("STAT") -> "223 0 <test@msg> article exists"
                cmd == "QUIT" -> "205 bye"
                else -> "500 unknown command"
            }
            out.write("$reply\r\n".toByteArray())
            out.flush()
            if (cmd == "QUIT") break
        }
    }

    /**
     * Greets every connection, hands the first to [first] and answers STAT and QUIT on the
     * others. Returns the number of connections accepted so far.
     */
    private fun launchFirstConnectionServer(
        first: (Socket, BufferedReader, OutputStream) -> Unit
    ): AtomicInteger {
        val accepted = AtomicInteger()
        Thread {
            while (true) {
                val client = try {
                    serverSocket.accept()
                } catch (_: Exception) {
                    break
                }
                val isFirst = accepted.incrementAndGet() == 1
                Thread {
                    client.use { socket ->
                        try {
                            val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                            val out = socket.getOutputStream()
                            out.write("200 welcome\r\n".toByteArray())
                            out.flush()
                            if (isFirst) first(socket, reader, out) else answerCommands(reader, out)
                        } catch (_: Exception) {
                        }
                    }
                }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
        return accepted
    }

    /** The pool's private lock, so a test can hold it the way a concurrent pool operation does. */
    private fun NntpClientPool.poolMutex(): Mutex =
        NntpClientPool::class.java.getDeclaredField("poolMutex").apply { isAccessible = true }.get(this) as Mutex


    // --- New tests for keepalive, retry, sleep/wake ---

    @Test
    @Timeout(30, unit = TimeUnit.SECONDS)
    fun `keepalive detects dead connection and discards`() = runBlocking {
        // Server accepts: initial connection + replacement spawned after keepalive
        // discards the dead one (the next acquire opens a fresh client).
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
    @Timeout(45, unit = TimeUnit.SECONDS)
    fun `both connections dead propagates exception`() = runBlocking {
        // Server accepts 2 initial connections; the rest will fail (server closed below).
        // After both pooled clients are discarded, the next acquire spawns a fresh
        // connection via connectWithRetry — which retries 5x with exponential backoff
        // before giving up. Worst case ~30s of backoff + acquire timeout before the
        // final NntpConnectionException surfaces.
        val sockets = launchCommandServer(maxAccepts = 10)

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

        // Prime both connections by using them
        pool.stat("<test@msg>")
        pool.stat("<test@msg>")
        // Wait for both connections to be established
        delay(100)

        // Kill both server-side sockets and close the server
        sockets.forEach { it.close() }
        serverSocket.close()

        delay(200) // let TCP state settle

        assertFailsWith<NntpConnectionException> {
            pool.stat("<test@msg>")
        }

        pool.close()
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `pool size 1 acquires fresh connection on retry`() = runBlocking {
        // Accept: 1 initial + 1 fresh (after the first one is discarded on STAT failure)
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

            // With pool size 1, the failing STAT discards the only client; the
            // .retry inside withClient triggers a fresh acquire which spawns a
            // replacement connection. The retry transparently succeeds.
            val result = pool.stat("<test@msg>")
            assertTrue(result is StatResult.Found)
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `multiple retries succeeds when last attempt works`() = runBlocking {
        // Server that fails the first 2 STAT attempts (closes connection), then succeeds on the 3rd
        val failCount = java.util.concurrent.atomic.AtomicInteger(2)
        // Accept: connections for each attempt + reconnects
        launchCommandServer(maxAccepts = 6, failStatOnce = AtomicBoolean(false))

        // Override: use a custom server that fails N times
        serverSocket.close()
        serverSocket = ServerSocket(0)
        Thread {
            var accepted = 0
            while (accepted < 6) {
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
                                when {
                                    cmd == "DATE" -> {
                                        out.write("111 20260224120000\r\n".toByteArray())
                                        out.flush()
                                    }
                                    cmd.startsWith("STAT") -> {
                                        if (failCount.getAndDecrement() > 0) {
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

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 2,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0,
            commandRetries = 3  // allow up to 3 retries (4 total attempts)
        )
        pool.wake()

        try {
            // First 2 attempts fail, third succeeds — should not throw
            val result = pool.stat("<test@msg>")
            assertTrue(result is StatResult.Found)
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `exhausted retries propagates exception`() = runBlocking<Unit> {
        // Server that always kills the connection on STAT
        serverSocket.close()
        serverSocket = ServerSocket(0)
        val statCommands = AtomicInteger(0)
        Thread {
            var accepted = 0
            while (accepted < 10) {
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
                                when {
                                    cmd.startsWith("STAT") -> {
                                        statCommands.incrementAndGet()
                                        client.close()
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

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 2,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0,
            commandRetries = 2  // 3 total attempts, all will fail
        )
        pool.wake()

        try {
            assertFailsWith<NntpConnectionException> {
                pool.stat("<test@msg>")
            }
            assertEquals(3, statCommands.get())
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `non-connection exceptions are not retried`() = runBlocking<Unit> {
        // Server that responds 430 to ARTICLE (article not found)
        val articleCommands = AtomicInteger(0)
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
                                if (cmd.startsWith("ARTICLE")) {
                                    articleCommands.incrementAndGet()
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
            assertEquals(1, articleCommands.get())
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

    /**
     * An acquire cancelled while another coroutine holds the pool's lock skipped its cleanup
     * (a contended Mutex.lock throws at once in a cancelled coroutine) and left its waiter
     * queued with a live deferred. The connection opened for it was then completed into that
     * dead waiter and lost: never idle, never closed, still counted against maxConnections.
     * Every seek cancels the downloads waiting in acquire, so scrubbing could leak every slot.
     */
    @Test
    @Timeout(30, unit = TimeUnit.SECONDS)
    fun `acquires cancelled while the pool is locked do not lose the connections opened for them`() =
        runBlocking<Unit> { assertCancelledAcquiresKeepTheirConnections(timeOutFirst = false) }

    /** As above, for a caller cancelled while its timed-out acquire waits for the lock to clean up. */
    @Test
    @Timeout(30, unit = TimeUnit.SECONDS)
    fun `acquires cancelled while cleaning up after a timeout do not lose the connections opened for them`() =
        runBlocking<Unit> { assertCancelledAcquiresKeepTheirConnections(timeOutFirst = true) }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun assertCancelledAcquiresKeepTheirConnections(timeOutFirst: Boolean) = coroutineScope {
        val slots = 4
        val greet = CountDownLatch(1)
        val closed = CountDownLatch(slots)
        launchGatedServer(greet, closed)

        val registry = SimpleMeterRegistry()
        Metrics.addRegistry(registry)
        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = slots,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0
        )
        val poolName = "localhost:${serverSocket.localPort}"
        fun gauge(name: String) = registry.find(name).tag("pool.name", poolName).gauge()?.value()

        // The waiting callers run on a test dispatcher, so the test decides when their
        // cleanup runs (while the pool's lock is held) and when their acquire times out.
        val scheduler = TestCoroutineScheduler()
        val callers = CoroutineScope(StandardTestDispatcher(scheduler) + SupervisorJob())
        try {
            pool.wake()
            val waiting = List(slots) {
                callers.launch { pool.withClient { fail("a cancelled acquire was served") } }
            }
            scheduler.runCurrent()
            assertEquals(slots.toDouble(), gauge("nntp.pool.waiters"), "every caller should be waiting")

            val lock = pool.poolMutex()
            lock.lock()
            if (timeOutFirst) {
                scheduler.advanceTimeBy(NntpClientPool.ACQUIRE_TIMEOUT_MS)
                scheduler.runCurrent()
            }
            waiting.forEach { it.cancel() }
            scheduler.runCurrent()
            lock.unlock()
            scheduler.runCurrent()
            assertTrue(waiting.all { it.isCompleted }, "the cancelled callers should have finished")

            // The connections opened for the cancelled callers arrive now. Every slot must
            // still be usable, promptly: hold all of them at once.
            greet.countDown()
            val acquired = AtomicInteger()
            val allAcquired = CompletableDeferred<Unit>()
            val served = withTimeoutOrNull(5_000) {
                List(slots) {
                    async(Dispatchers.IO) {
                        pool.withClient { client ->
                            if (acquired.incrementAndGet() == slots) allAcquired.complete(Unit)
                            allAcquired.await()
                            client.stat("<test@msg>")
                        }
                    }
                }.awaitAll()
            }
            assertNotNull(
                served,
                "only ${acquired.get()} of $slots slots could be acquired: the connections for the " +
                    "rest went to cancelled waiters"
            )
            assertEquals(slots.toDouble(), gauge("nntp.pool.size"), "pool size")
            assertEquals(slots.toDouble(), gauge("nntp.pool.idle"), "idle connections")
            assertEquals(0.0, gauge("nntp.pool.connecting"), "connections neither idle nor in use")
            assertEquals(0.0, gauge("nntp.pool.waiters"), "queued waiters")
        } finally {
            greet.countDown()
            callers.cancel()
            pool.close()
            Metrics.removeRegistry(registry)
        }
        assertTrue(closed.await(5, TimeUnit.SECONDS), "${closed.count} connection(s) were never closed")
    }

    /**
     * Ktor 3's writer keeps a failure of its block in the channel and completes normally, so
     * when the yEnc pump failed mid-article (here the server resets the connection), the
     * pool's withClient saw its block return and put the dead connection back in the pool,
     * first in line for the next caller.
     */
    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a body pump that fails mid-article does not return its connection to the pool`() = runBlocking<Unit> {
        val bodyStarted = CountDownLatch(1)
        launchFirstConnectionServer { socket, reader, out ->
            reader.readLine() // BODY
            val article = buildYencArticle(ByteArray(64 * 1024) { (it % 256).toByte() }, "reset.bin")
            out.write("222 body follows\r\n".toByteArray())
            out.write(article, 0, article.size / 2)
            out.flush()
            bodyStarted.await()
            socket.setSoLinger(true, 0) // closes with a reset, mid-article
        }
        val registry = SimpleMeterRegistry()
        Metrics.addRegistry(registry)
        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0
        )
        val poolName = "localhost:${serverSocket.localPort}"
        fun gauge(name: String) = registry.find(name).tag("pool.name", poolName).gauge()?.value()
        pool.wake()
        try {
            pool.bodyYenc("<reset@test>").collect { event ->
                if (event is YencEvent.Body) {
                    bodyStarted.countDown()
                    try {
                        event.data.toByteArray()
                    } catch (_: IOException) {
                        // The reset reaches the reader; what matters here is the connection.
                    }
                }
            }
            assertEquals(0.0, gauge("nntp.pool.idle"), "the dead connection is idle in the pool")
            assertEquals(0.0, gauge("nntp.pool.size"), "pool size")
            assertTrue(pool.stat("<test@msg>") is StatResult.Found)
        } finally {
            pool.close()
            Metrics.removeRegistry(registry)
        }
    }

    /**
     * A reader that cancels the Body channel to stop early makes the pump's next write fail,
     * which Ktor 3's writer keeps in the channel, so withClient returned the connection with
     * the rest of the article still on the wire: the next command read yEnc data as its
     * status line.
     */
    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a body its reader cancels does not leave the rest of the article for the next command`() =
        runBlocking<Unit> {
            val bodyCancelled = CountDownLatch(1)
            val accepted = launchFirstConnectionServer { _, reader, out ->
                reader.readLine() // BODY
                val article = buildYencArticle(ByteArray(64 * 1024) { (it % 256).toByte() }, "early.bin")
                val third = article.size / 3
                out.write("222 body follows\r\n".toByteArray())
                out.write(article, 0, third)
                out.flush()
                bodyCancelled.await()
                // More of the body, which the pump fails to write into the cancelled channel.
                out.write(article, third, third)
                out.flush()
                // The rest goes out when the next command arrives, so a connection returned
                // mid-article reads it as that command's status line.
                reader.readLine() ?: return@launchFirstConnectionServer
                out.write(article, 2 * third, article.size - 2 * third)
                out.write("223 0 <test@msg> article exists\r\n".toByteArray())
                out.flush()
                answerCommands(reader, out)
            }
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
                pool.bodyYenc("<early@test>").collect { event ->
                    if (event is YencEvent.Body) {
                        event.data.cancel()
                        bodyCancelled.countDown()
                    }
                }
                val result = pool.stat("<test@msg>")
                assertTrue(result is StatResult.Found)
                assertEquals(2, accepted.get(), "the STAT should have needed a new connection")
            } finally {
                pool.close()
            }
        }

    /**
     * EOF before =yend ended the pump normally, so a connection closed halfway through an
     * article (by the server, or by TLS, which reports a FIN, a reset and a missing
     * close_notify alike as EOF) delivered a short segment as a whole one.
     */
    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a body cut short by the connection closing fails instead of ending early`() = runBlocking<Unit> {
        val data = ByteArray(64 * 1024) { (it % 256).toByte() }
        launchFirstConnectionServer { _, reader, out ->
            reader.readLine() // BODY
            val article = buildYencArticle(data, "short.bin")
            out.write("222 body follows\r\n".toByteArray())
            out.write(article, 0, article.size / 2)
            out.flush()
            // Returning closes the connection (a FIN), mid-article.
        }
        val registry = SimpleMeterRegistry()
        Metrics.addRegistry(registry)
        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0
        )
        val poolName = "localhost:${serverSocket.localPort}"
        fun gauge(name: String) = registry.find(name).tag("pool.name", poolName).gauge()?.value()
        pool.wake()
        try {
            var read: ByteArray? = null
            var failure: IOException? = null
            pool.bodyYenc("<short@test>").collect { event ->
                if (event is YencEvent.Body) {
                    try {
                        read = event.data.toByteArray()
                    } catch (e: IOException) {
                        failure = e
                    }
                }
            }
            assertNotNull(failure, "the body ended without an error after ${read?.size} of ${data.size} bytes")
            assertTrue(
                generateSequence<Throwable>(failure) { it.cause }.any { it is NntpConnectionException },
                "the reader should see the connection failure, got $failure"
            )
            assertEquals(0.0, gauge("nntp.pool.idle"), "the closed connection is idle in the pool")
            assertEquals(0.0, gauge("nntp.pool.size"), "pool size")
        } finally {
            pool.close()
            Metrics.removeRegistry(registry)
        }
    }

    /**
     * A connection closed after =yend but before the article terminator still delivers the
     * whole body (its CRC checked), but must not go back to the pool: the trailer drain used
     * to take EOF for the terminator and mark the wire clean.
     */
    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a body whose connection closes before the article terminator is delivered but not pooled`() =
        runBlocking<Unit> {
            val data = ByteArray(64 * 1024) { (it % 256).toByte() }
            launchFirstConnectionServer { _, reader, out ->
                reader.readLine() // BODY
                val article = buildYencArticle(data, "trailer.bin")
                out.write("222 body follows\r\n".toByteArray())
                out.write(article, 0, article.size - ".\r\n".length)
                out.flush()
                // Returning closes the connection before the terminator.
            }
            val registry = SimpleMeterRegistry()
            Metrics.addRegistry(registry)
            val pool = NntpClientPool(
                host = "localhost",
                port = serverSocket.localPort,
                selectorManager = selectorManager,
                maxConnections = 1,
                scope = poolScope,
                keepaliveIntervalMs = 0,
                idleGracePeriodMs = 0
            )
            val poolName = "localhost:${serverSocket.localPort}"
            fun gauge(name: String) = registry.find(name).tag("pool.name", poolName).gauge()?.value()
            pool.wake()
            try {
                var read: ByteArray? = null
                pool.bodyYenc("<trailer@test>").collect { event ->
                    if (event is YencEvent.Body) read = event.data.toByteArray()
                }
                assertContentEquals(data, read)
                assertEquals(0.0, gauge("nntp.pool.idle"), "the closed connection is idle in the pool")
                assertEquals(0.0, gauge("nntp.pool.size"), "pool size")
            } finally {
                pool.close()
                Metrics.removeRegistry(registry)
            }
        }

    @Test
    @Timeout(45, unit = TimeUnit.SECONDS)
    fun `currentSize decrements when all connection retries fail`() = runBlocking {
        // Server that immediately refuses connections
        serverSocket.close()
        serverSocket = ServerSocket(0)
        val closedPort = serverSocket.localPort
        serverSocket.close()

        val registry = SimpleMeterRegistry()
        Metrics.addRegistry(registry)
        try {
            val pool = NntpClientPool(
                host = "localhost",
                port = closedPort,
                selectorManager = selectorManager,
                maxConnections = 2,
                scope = poolScope,
                keepaliveIntervalMs = 0,
                idleGracePeriodMs = 0
            )
            pool.wake()

            val poolName = "localhost:$closedPort"

            // Trigger a connection attempt, then cancel the caller immediately.
            // This leaves the background launchConnection running.
            val job = launch {
                pool.withClient { delay(Long.MAX_VALUE) }
            }
            delay(200) // let acquire trigger launchConnection
            job.cancel()

            // connectWithRetry does up to 5 retries with exponential backoff (1+2+4+8+16=31s).
            // After all retries fail, launchConnection should decrement currentSize.
            delay(35_000)

            val sizeGauge = registry.find("nntp.pool.size").tag("pool.name", poolName).gauge()
            assertEquals(0.0, sizeGauge?.value() ?: -1.0, "currentSize should be 0 after failed connections")

            pool.close()
        } finally {
            Metrics.removeRegistry(registry)
        }
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `cancelled withClient does not leak connections`() = runBlocking {
        launchCommandServer(maxAccepts = 10)

        val registry = SimpleMeterRegistry()
        Metrics.addRegistry(registry)
        try {
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

            val poolName = "localhost:${serverSocket.localPort}"

            // Launch many concurrent withClient calls and cancel them
            repeat(5) {
                val job = launch {
                    pool.withClient {
                        delay(Long.MAX_VALUE) // block forever
                    }
                }
                delay(100)
                job.cancel()
                delay(50)
            }

            // All connections should be returned — pool should still work
            val result = pool.stat("<test@msg>")
            assertTrue(result is StatResult.Found)

            // idle + active should equal size (no leaked slots)
            val sizeGauge = registry.find("nntp.pool.size").tag("pool.name", poolName).gauge()
            val idleGauge = registry.find("nntp.pool.idle").tag("pool.name", poolName).gauge()
            val size = sizeGauge?.value() ?: -1.0
            val idle = idleGauge?.value() ?: -1.0
            assertTrue(idle > 0, "Should have idle connections, got idle=$idle size=$size")

            pool.close()
        } finally {
            Metrics.removeRegistry(registry)
        }
    }

    /**
     * Repro for the production bug where the next op on a pooled connection reads
     * yenc body bytes from a previous, cancelled-mid-stream op:
     *
     *   io.skjaere.nntp.NntpProtocolException: Invalid response code: <yenc-garbage>
     *       at io.skjaere.nntp.NntpResponseKt.parseResponseLine(NntpResponse.kt:61)
     *       at io.skjaere.nntp.NntpConnection.readResponse(NntpConnection.kt)
     *       at io.skjaere.nntp.NntpConnection.commandRaw(NntpConnection.kt)
     *       at io.skjaere.nntp.NntpClient$fetchBodyYenc$1.invokeSuspend(NntpClient.kt)
     *
     * Pool size = 1 forces both ops onto the same connection. The server sends a 222
     * + partial body, then waits on a gate; the test cancels the first op via
     * withTimeout while the body is mid-stream. After cancellation, the gate is
     * released so the server finishes the body — giving the pool's background drain
     * a chance to read the article terminator and return the connection clean.
     *
     * If drain works as designed, the second BODY succeeds.
     * If drain doesn't actually flush the wire (the bug we keep seeing in prod),
     * the second BODY's commandRaw throws NntpProtocolException reading yenc bytes.
     */
    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `cancelled body does not leak yenc bytes into next op on same pool`() = runBlocking {
        val article1 = ByteArray(8192) { (it % 256).toByte() }
        val article2 = "second article cleanly served".toByteArray()

        Thread {
            // Single accept — both client-side ops MUST share this socket.
            val socket = serverSocket.accept()
            socket.use { s ->
                val reader = BufferedReader(InputStreamReader(s.getInputStream()))
                val out = s.getOutputStream()
                out.write("200 welcome\r\n".toByteArray())
                out.flush()

                // First BODY: send 222 + partial body, pause mid-body for 800 ms,
                // then send the rest. The test cancels around 300 ms in (mid-pause)
                // so the drain has to read what arrives after the pause.
                val cmd1 = reader.readLine()
                check(cmd1 != null && cmd1.startsWith("BODY")) { "expected BODY, got '$cmd1'" }
                val full1 = buildYencArticle(article1, "first.bin")
                out.write("222 body follows\r\n".toByteArray())
                val firstChunkSize = 256
                out.write(full1, 0, firstChunkSize)
                out.flush()
                Thread.sleep(800)
                out.write(full1, firstChunkSize, full1.size - firstChunkSize)
                out.flush()

                // Second BODY
                val cmd2 = reader.readLine()
                check(cmd2 != null && cmd2.startsWith("BODY")) { "expected BODY, got '$cmd2'" }
                out.write("222 body follows\r\n".toByteArray())
                out.write(buildYencArticle(article2, "second.bin"))
                out.flush()
                // Keep the socket alive long enough for the test to finish the read.
                Thread.sleep(2_000)
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
            // First op: collect bodyYenc with a tight timeout so cancellation lands
            // while the server is paused mid-body. We expect the timeout to fire,
            // the inline drain to run, and the connection to come back clean.
            val firstThrew = AtomicBoolean(false)
            try {
                kotlinx.coroutines.withTimeout(300) {
                    pool.bodyYenc("<first@test>").collect { event ->
                        if (event is YencEvent.Body) {
                            // Read a bit so we know the writer is engaged with the wire,
                            // but don't drain — we want the cancellation to fire while
                            // the writer is still mid-stream.
                            val sink = ByteArray(64)
                            event.data.readAvailable(sink)
                        }
                    }
                }
            } catch (@Suppress("TooGenericExceptionCaught") _: Exception) {
                firstThrew.set(true)
            }
            assertTrue(firstThrew.get(), "first bodyYenc should have been cancelled by withTimeout")

            // Second op: must succeed. If the connection still has yenc bytes from
            // article1 pending, commandRaw will throw NntpProtocolException reading
            // the garbage as a response code.
            val secondBytes = ByteArray(article2.size)
            var secondOffset = 0
            pool.bodyYenc("<second@test>").collect { event ->
                if (event is YencEvent.Body) {
                    while (true) {
                        val n = event.data.readAvailable(
                            secondBytes, secondOffset, secondBytes.size - secondOffset
                        )
                        if (n == -1) break
                        secondOffset += n
                        if (secondOffset >= secondBytes.size) break
                    }
                }
            }
            assertEquals(
                String(article2),
                String(secondBytes.copyOf(secondOffset)),
                "second body should have decoded cleanly — no yenc bleed-through from cancelled first op"
            )
        } finally {
            pool.close()
        }
    }

    /**
     * Variant of the cancel-mid-body test that triggers cancellation via parent-scope
     * cancel rather than withTimeout. In production, streamSegments cancels the
     * downloader job from a finally block when the servlet output stream throws —
     * a different propagation path than withTimeout's TimeoutCancellationException.
     */
    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `cancelled body via parent scope does not leak yenc bytes into next op`() = runBlocking {
        val article1 = ByteArray(8192) { (it % 256).toByte() }
        val article2 = "second article cleanly served".toByteArray()

        Thread {
            val socket = serverSocket.accept()
            socket.use { s ->
                val reader = BufferedReader(InputStreamReader(s.getInputStream()))
                val out = s.getOutputStream()
                out.write("200 welcome\r\n".toByteArray())
                out.flush()

                val cmd1 = reader.readLine()
                check(cmd1 != null && cmd1.startsWith("BODY")) { "expected BODY, got '$cmd1'" }
                val full1 = buildYencArticle(article1, "first.bin")
                out.write("222 body follows\r\n".toByteArray())
                out.write(full1, 0, 256)
                out.flush()
                Thread.sleep(800)
                out.write(full1, 256, full1.size - 256)
                out.flush()

                val cmd2 = reader.readLine()
                check(cmd2 != null && cmd2.startsWith("BODY")) { "expected BODY, got '$cmd2'" }
                out.write("222 body follows\r\n".toByteArray())
                out.write(buildYencArticle(article2, "second.bin"))
                out.flush()
                Thread.sleep(2_000)
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
            val collectorReady = CompletableDeferred<Unit>()
            val collectorJob = launch {
                pool.bodyYenc("<first@test>").collect { event ->
                    if (event is YencEvent.Body) {
                        collectorReady.complete(Unit)
                        // Read a bit, then suspend so the server-side mid-body pause
                        // can hold us in the middle of the body stream until the
                        // parent scope is cancelled below.
                        val sink = ByteArray(64)
                        event.data.readAvailable(sink)
                        delay(Long.MAX_VALUE)
                    }
                }
            }
            collectorReady.await()
            // Cancel the parent scope — this is the propagation shape that happens
            // in streamSegments when the servlet output throws.
            collectorJob.cancel()
            collectorJob.join()

            // Second op on the same pool slot — must NOT see yenc bleed-through.
            val secondBytes = ByteArray(article2.size)
            var secondOffset = 0
            pool.bodyYenc("<second@test>").collect { event ->
                if (event is YencEvent.Body) {
                    while (true) {
                        val n = event.data.readAvailable(
                            secondBytes, secondOffset, secondBytes.size - secondOffset
                        )
                        if (n == -1) break
                        secondOffset += n
                        if (secondOffset >= secondBytes.size) break
                    }
                }
            }
            assertEquals(
                String(article2),
                String(secondBytes.copyOf(secondOffset)),
                "second body should have decoded cleanly — no yenc bleed-through"
            )
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

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `connectionsOpened counts the connections the pool opened, not reuses`() = runBlocking<Unit> {
        launchCommandServer(maxAccepts = 10)

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 2,
            scope = poolScope,
            keepaliveIntervalMs = 0,
            idleGracePeriodMs = 0
        )
        try {
            assertEquals(0, pool.connectionsOpened)
            // Two at once need two connections.
            val bothHeld = CompletableDeferred<Unit>()
            val holding = AtomicInteger()
            coroutineScope {
                repeat(2) {
                    launch {
                        pool.withClient {
                            if (holding.incrementAndGet() == 2) bothHeld.complete(Unit)
                            bothHeld.await()
                        }
                    }
                }
            }
            assertEquals(2, pool.connectionsOpened)
            // A later command reuses one of them.
            assertTrue(pool.stat("<test@msg>") is StatResult.Found)
            assertEquals(2, pool.connectionsOpened)
        } finally {
            pool.close()
        }
    }
}
