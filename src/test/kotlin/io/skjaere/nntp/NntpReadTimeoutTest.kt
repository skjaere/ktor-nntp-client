package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.ktor.utils.io.toByteArray
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The idle read timeout: a read that gets no bytes for `readTimeoutMs` fails the operation
 * with [NntpConnectionException], and the pool discards the connection. A response that
 * keeps arriving, however slowly, and an idle pooled connection are never cut off.
 */
class NntpReadTimeoutTest {

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

    /**
     * Accepts connections, greets each one unless [greet] is false, and hands it to [serve].
     * Returns the number of connections accepted so far.
     */
    private fun launchServer(
        greet: Boolean = true,
        serve: (BufferedReader, OutputStream) -> Unit
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
                            if (greet) {
                                out.write("200 welcome\r\n".toByteArray())
                                out.flush()
                            }
                            serve(reader, out)
                        } catch (_: Exception) {
                        }
                    }
                }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
        return accepted
    }

    /** Reads commands without ever answering, until the client goes away. */
    private fun ignoreCommands(reader: BufferedReader) {
        while (reader.readLine() != null) {
            // silent
        }
    }

    private fun pool(readTimeoutMs: Long) = NntpClientPool(
        host = "localhost",
        port = serverSocket.localPort,
        selectorManager = selectorManager,
        maxConnections = 1,
        scope = poolScope,
        keepaliveIntervalMs = 0,
        idleGracePeriodMs = 0,
        readTimeoutMs = readTimeoutMs
    )

    private fun poolSize() =
        registry.find("nntp.pool.size").tag("pool.name", "localhost:${serverSocket.localPort}").gauge()?.value()

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

    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `a STAT the server never answers fails after the read timeout and frees its slot`() = runBlocking<Unit> {
        launchServer { reader, _ -> ignoreCommands(reader) }
        val pool = pool(readTimeoutMs = 300)
        pool.wake()
        try {
            var failure: NntpConnectionException? = null
            val finished = withTimeoutOrNull(5_000) {
                try {
                    pool.stat("<test@msg>")
                } catch (e: NntpConnectionException) {
                    failure = e
                }
            }
            assertNotNull(finished, "the STAT was still waiting for an answer after 5 s")
            assertNotNull(failure, "the STAT should fail with a connection error")
            assertEquals(0.0, poolSize(), "the silent connections should be discarded, freeing their slots")
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `a BODY that stops mid-article fails after the read timeout`() = runBlocking<Unit> {
        launchServer { reader, out ->
            reader.readLine() // BODY
            val article = buildYencArticle(ByteArray(64 * 1024) { (it % 256).toByte() }, "stall.bin")
            out.write("222 body follows\r\n".toByteArray())
            out.write(article, 0, article.size / 2)
            out.flush()
            ignoreCommands(reader)
        }
        val pool = pool(readTimeoutMs = 300)
        pool.wake()
        try {
            var failure: IOException? = null
            val finished = withTimeoutOrNull(5_000) {
                pool.bodyYenc("<stall@test>").collect { event ->
                    if (event is YencEvent.Body) {
                        try {
                            event.data.toByteArray()
                        } catch (e: IOException) {
                            failure = e
                        }
                    }
                }
            }
            assertNotNull(finished, "the BODY was still waiting for the rest of the article after 5 s")
            assertTrue(
                generateSequence<Throwable>(failure) { it.cause }.any { it is NntpConnectionException },
                "the reader should see a connection failure, got $failure"
            )
            assertEquals(0.0, poolSize(), "the silent connection should be discarded, freeing its slot")
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `a BODY that keeps arriving is not cut off by the read timeout`() = runBlocking<Unit> {
        val data = ByteArray(64 * 1024) { (it % 256).toByte() }
        launchServer { reader, out ->
            reader.readLine() // BODY
            val article = buildYencArticle(data, "slow.bin")
            out.write("222 body follows\r\n".toByteArray())
            out.flush()
            // 30 pieces 50 ms apart: three read timeouts in all, but never more than 50 ms
            // without bytes.
            val piece = (article.size + 29) / 30
            for (start in article.indices step piece) {
                out.write(article, start, minOf(piece, article.size - start))
                out.flush()
                Thread.sleep(50)
            }
            ignoreCommands(reader)
        }
        val pool = pool(readTimeoutMs = 500)
        pool.wake()
        try {
            var read: ByteArray? = null
            pool.bodyYenc("<slow@test>").collect { event ->
                if (event is YencEvent.Body) read = event.data.toByteArray()
            }
            assertContentEquals(data, read)
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `a pipelined STAT batch that keeps answering is not cut off by the read timeout`() = runBlocking<Unit> {
        launchServer { reader, out ->
            while (true) {
                val cmd = reader.readLine() ?: break
                Thread.sleep(50)
                out.write("223 0 ${cmd.removePrefix("STAT ")} article exists\r\n".toByteArray())
                out.flush()
            }
        }
        val pool = pool(readTimeoutMs = 500)
        pool.wake()
        try {
            // 30 answers 50 ms apart: three read timeouts in all.
            val results = pool.statPipelined((0 until 30).map { "<a$it@x>" })
            assertEquals(30, results.size)
            assertTrue(results.all { it is StatResult.Found })
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `an idle pooled connection is not closed by the read timeout`() = runBlocking<Unit> {
        val accepted = launchServer { reader, out ->
            while (true) {
                val cmd = reader.readLine() ?: break
                out.write("223 0 ${cmd.removePrefix("STAT ")} article exists\r\n".toByteArray())
                out.flush()
            }
        }
        val pool = pool(readTimeoutMs = 200)
        pool.wake()
        try {
            assertTrue(pool.stat("<a@x>") is StatResult.Found)
            delay(600) // idle for three read timeouts, with no read pending
            assertTrue(pool.stat("<a@x>") is StatResult.Found)
            assertEquals(1, accepted.get(), "the second STAT should reuse the idle connection")
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `connect fails when the server never sends its welcome`() = runBlocking<Unit> {
        launchServer(greet = false) { reader, _ -> ignoreCommands(reader) }
        var failure: NntpConnectionException? = null
        val finished = withTimeoutOrNull(5_000) {
            try {
                NntpClient.connect("localhost", serverSocket.localPort, selectorManager, readTimeoutMs = 300).close()
            } catch (e: NntpConnectionException) {
                failure = e
            }
        }
        assertNotNull(finished, "connect was still waiting for the welcome after 5 s")
        assertNotNull(failure, "connect should fail with a connection error")
    }

    /**
     * A test dispatcher's virtual clock must not drive the timeout: under runTest a coroutine
     * timeout would expire at once while the read waits on the real network.
     */
    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `the read timeout runs in real time under a test dispatcher`() = runTest {
        launchServer(greet = false) { reader, out ->
            Thread.sleep(200)
            out.write("200 welcome\r\n".toByteArray())
            out.flush()
            ignoreCommands(reader)
        }
        val connection = NntpConnection.connect(
            "localhost", serverSocket.localPort, selectorManager, readTimeoutMs = 5_000
        )
        assertEquals(200, connection.welcomeResponse.code)
        connection.close()
    }
}
