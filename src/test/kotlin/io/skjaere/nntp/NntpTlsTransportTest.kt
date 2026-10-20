package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.ktor.utils.io.readLine
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.IOException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The connection over TLS as over plain TCP: whole articles read across many TLS records, a connection reused after a
 * body, the CRC check, the read timeout, the end of a connection (with TLS's close_notify or without), a body cancelled
 * mid-article and drained, and a close that fails a read waiting on it. Over TLS 1.2 here; [NntpTls13TransportTest]
 * runs them over 1.3, and [NntpTlsConscryptTransportTest] with Conscrypt's TLS, Android's, as the client's.
 */
@Timeout(30, unit = TimeUnit.SECONDS)
internal open class NntpTlsTransportTest {

    /** What the server speaks: TLS 1.2 here, 1.3 in [NntpTls13TransportTest]. */
    protected open val protocols: Array<String> = arrayOf("TLSv1.2")

    companion object {
        private lateinit var certificates: TestCertificates

        @JvmStatic
        @BeforeAll
        fun createCertificates() {
            certificates = TestCertificates.create(mapOf("localhost" to ("CN=localhost" to "dns:localhost")))
        }

        @JvmStatic
        @AfterAll
        fun deleteCertificates() {
            certificates.close()
        }
    }

    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope
    private lateinit var registry: SimpleMeterRegistry
    private val servers = mutableListOf<ScriptedTlsServer>()

    @BeforeEach
    fun setUp() {
        certificates.useAsDefaultTrustStore()
        selectorManager = SelectorManager(Dispatchers.IO)
        poolScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        registry = SimpleMeterRegistry()
        Metrics.addRegistry(registry)
    }

    @AfterEach
    fun tearDown() {
        servers.forEach { it.close() }
        Metrics.removeRegistry(registry)
        poolScope.cancel()
        selectorManager.close()
        certificates.restoreDefaultTrustStore()
    }

    protected fun server(
        greet: Boolean = true,
        serve: (ScriptedTlsServer.TlsSession) -> Unit = {},
    ) = ScriptedTlsServer(certificates.keyStore("localhost"), protocols, greet = greet, serve = serve).also { servers += it }

    protected suspend fun client(server: ScriptedTlsServer, readTimeoutMs: Long = NntpConnection.DEFAULT_READ_TIMEOUT_MS) =
        NntpClient.connect("localhost", server.port, selectorManager, useTls = true, readTimeoutMs = readTimeoutMs)

    private fun pool(server: ScriptedTlsServer, readTimeoutMs: Long = NntpConnection.DEFAULT_READ_TIMEOUT_MS) =
        NntpClientPool(
            host = "localhost", port = server.port, selectorManager = selectorManager, useTls = true,
            maxConnections = 1, scope = poolScope, keepaliveIntervalMs = 0, idleGracePeriodMs = 0,
            readTimeoutMs = readTimeoutMs
        )

    private fun poolSize(server: ScriptedTlsServer) =
        registry.find("nntp.pool.size").tag("pool.name", "localhost:${server.port}").gauge()?.value()

    /** A yEnc article as a server sends it after BODY: its 222, a part of a larger file, its =yend with [crc], the terminator. */
    protected fun article(data: ByteArray, crc: UInt = RapidYenc.crc32(data)): ByteArray {
        val encoded = String(RapidYenc.encode(data), Charsets.ISO_8859_1)
        return buildString {
            append("222 body follows\r\n")
            append("=ybegin part=1 total=1 line=128 size=${data.size} name=data.bin\r\n")
            append("=ypart begin=1 end=${data.size}\r\n")
            append(encoded)
            if (!encoded.endsWith("\r\n")) append("\r\n")
            append("=yend size=${data.size} part=1 pcrc32=${crc.toString(16).padStart(8, '0')}\r\n")
            append(".\r\n")
        }.toByteArray(Charsets.ISO_8859_1)
    }

    /** Answers BODY with each of [articles] in turn and STAT with 223, until the client goes away. */
    protected fun answering(vararg articles: ByteArray): (ScriptedTlsServer.TlsSession) -> Unit = { session ->
        val bodies = articles.iterator()
        while (true) {
            val command = session.reader.readLine() ?: break
            when {
                command.startsWith("BODY") -> session.out.write(bodies.next())
                command.startsWith("STAT") -> session.out.write("223 0 <a@b> exists\r\n".toByteArray())
                command == "QUIT" -> {
                    session.out.write("205 bye\r\n".toByteArray())
                    session.out.flush()
                    break
                }
                else -> session.out.write("500 unknown\r\n".toByteArray())
            }
            session.out.flush()
        }
    }

    private fun Throwable?.causes() = generateSequence(this) { it.cause }.toList()

    @Test
    fun `an article larger than many TLS records is read whole and its connection stays in step`() = runBlocking<Unit> {
        val data = Random(11).nextBytes(1_300_000)
        val server = server(serve = answering(article(data)))
        client(server).use { client ->
            assertContentEquals(data, client.bodyYencBytes("<big@test>").data)
            assertTrue(client.connection.isReusable)
            assertIs<StatResult.Found>(client.stat("<a@b>"))
        }
    }

    @Test
    fun `a pooled connection is reused after a body`() = runBlocking<Unit> {
        val first = Random(12).nextBytes(500_000)
        val second = Random(13).nextBytes(200_000)
        val server = server(serve = answering(article(first), article(second)))
        val pool = pool(server)
        try {
            assertContentEquals(first, pool.withClient { it.bodyYencBytes("<first@test>") }.data)
            assertContentEquals(second, pool.withClient { it.bodyYencBytes("<second@test>") }.data)
            assertIs<StatResult.Found>(pool.stat("<a@b>"))
            assertEquals(1, server.accepted, "connections: the bodies and the STAT should share one")
        } finally {
            pool.close()
        }
    }

    @Test
    fun `a body whose CRC does not match fails once read to its end and keeps its connection`() = runBlocking<Unit> {
        val damaged = Random(14).nextBytes(300_000)
        val next = Random(15).nextBytes(10_000)
        val server = server(serve = answering(article(damaged, crc = RapidYenc.crc32(damaged) xor 1u), article(next)))
        client(server).use { client ->
            val e = assertFailsWith<YencCrcMismatchException> { client.bodyYencBytes("<damaged@test>") }
            assertContentEquals(damaged, e.article?.data, "the mismatch should carry the article as it came")
            assertTrue(client.connection.isReusable, "the article was read to its end: the wire is in step")
            assertContentEquals(next, client.bodyYencBytes("<next@test>").data)
        }
    }

    /**
     * A short record (a status line) is decrypted as soon as it is in: only a long one still arriving waits for its
     * bytes to gather (NntpSocket's gathering), or every command over TLS would wait a millisecond for its answer.
     */
    @Test
    fun `short answers over TLS are read at once, not left to gather`() = runBlocking<Unit> {
        val server = server(serve = answering())
        client(server).use { client ->
            repeat(20) { client.stat("<a@b>") }
            val started = System.nanoTime()
            repeat(300) { assertIs<StatResult.Found>(client.stat("<a@b>")) }
            val tookMs = (System.nanoTime() - started) / 1_000_000
            assertTrue(tookMs < 300, "300 STATs took $tookMs ms, a millisecond or more each")
        }
    }

    @Test
    fun `a body that stops mid-article fails after the read timeout and frees its slot`() = runBlocking<Unit> {
        val whole = article(Random(16).nextBytes(64 * 1024))
        val server = server { session ->
            session.reader.readLine() // BODY
            session.out.write(whole, 0, whole.size / 2)
            session.out.flush()
            while (session.reader.readLine() != null) Unit
        }
        val pool = pool(server, readTimeoutMs = 300)
        try {
            var failure: Throwable? = null
            val finished = withTimeoutOrNull(5_000) {
                failure = runCatching { pool.withClient { it.bodyYencBytes("<stall@test>") } }.exceptionOrNull()
            }
            assertNotNull(finished, "the BODY was still waiting for the rest of the article after 5 s")
            assertTrue(failure.causes().any { it is NntpConnectionException }, "expected a connection failure, got $failure")
            assertEquals(0.0, poolSize(server), "the silent connections should be discarded, freeing their slots")
        } finally {
            pool.close()
        }
    }

    @Test
    fun `a STAT the server never answers fails after the read timeout`() = runBlocking<Unit> {
        val server = server { session -> while (session.reader.readLine() != null) Unit }
        client(server, readTimeoutMs = 300).use { client ->
            var failure: Throwable? = null
            val finished = withTimeoutOrNull(5_000) { failure = runCatching { client.stat("<a@b>") }.exceptionOrNull() }
            assertNotNull(finished, "the STAT was still waiting for an answer after 5 s")
            assertIs<NntpConnectionException>(failure)
            assertFalse(client.connection.isReusable)
        }
    }

    @Test
    fun `a body that keeps arriving is not cut off by the read timeout`() = runBlocking<Unit> {
        val data = Random(17).nextBytes(64 * 1024)
        val whole = article(data)
        val server = server { session ->
            session.reader.readLine() // BODY
            // 30 pieces 50 ms apart: three read timeouts in all, but never more than 50 ms without bytes.
            val piece = (whole.size + 29) / 30
            for (start in whole.indices step piece) {
                session.out.write(whole, start, minOf(piece, whole.size - start))
                session.out.flush()
                Thread.sleep(50)
            }
            while (session.reader.readLine() != null) Unit
        }
        client(server, readTimeoutMs = 500).use { client ->
            assertContentEquals(data, client.bodyYencBytes("<slow@test>").data)
        }
    }

    @Test
    fun `connect fails when the server completes the handshake but never sends its welcome`() = runBlocking<Unit> {
        val server = server(greet = false) { session -> while (session.reader.readLine() != null) Unit }
        var failure: Throwable? = null
        val finished = withTimeoutOrNull(5_000) {
            failure = runCatching { client(server, readTimeoutMs = 300).close() }.exceptionOrNull()
        }
        assertNotNull(finished, "connect was still waiting for the welcome after 5 s")
        assertIs<NntpConnectionException>(failure)
    }

    /** A test dispatcher's virtual clock must drive neither the handshake nor the read timeout. */
    @Test
    fun `a TLS connect works under a test dispatcher`() = runTest {
        val server = server(greet = false) { session ->
            Thread.sleep(200)
            session.out.write("200 welcome\r\n".toByteArray())
            session.out.flush()
            while (session.reader.readLine() != null) Unit
        }
        client(server, readTimeoutMs = 5_000).use { assertEquals(200, it.connection.welcomeResponse.code) }
    }

    @Test
    fun `a connection closed mid-article with close_notify fails the body`() = runBlocking<Unit> {
        val whole = article(Random(18).nextBytes(200_000))
        val server = server { session ->
            session.reader.readLine() // BODY
            session.out.write(whole, 0, whole.size / 2)
            session.out.flush()
            session.closeWithNotify()
        }
        client(server).use { client ->
            assertFailsWith<NntpConnectionException> { client.bodyYencBytes("<cut@test>") }
            assertFalse(client.connection.isReusable)
        }
    }

    /** Without close_notify the end may be an attack cutting the stream short; it is the connection's end all the same. */
    @Test
    fun `a connection closed mid-article without close_notify fails the body`() = runBlocking<Unit> {
        val whole = article(Random(19).nextBytes(200_000))
        val server = server { session ->
            session.reader.readLine() // BODY
            session.out.write(whole, 0, whole.size / 2)
            session.out.flush()
            Thread.sleep(100) // the bytes out before the FIN
            session.closeWithoutNotify()
        }
        client(server).use { client ->
            assertFailsWith<NntpConnectionException> { client.bodyYencBytes("<cut@test>") }
            assertFalse(client.connection.isReusable)
        }
    }

    @Test
    fun `a body whose connection closes mid-article is fetched again on another connection`() = runBlocking<Unit> {
        val data = Random(20).nextBytes(200_000)
        val whole = article(data)
        val connections = AtomicInteger()
        val server = server { session ->
            session.reader.readLine() // BODY
            if (connections.incrementAndGet() == 1) {
                // The first connection is cut a third of the way in.
                session.out.write(whole, 0, whole.size / 3)
                session.out.flush()
                Thread.sleep(100)
                session.closeWithoutNotify()
            } else {
                session.out.write(whole)
                session.out.flush()
                answering()(session) // QUIT, at the pool's close
            }
        }
        val pool = pool(server)
        try {
            assertContentEquals(data, pool.withClient { it.bodyYencBytes("<again@test>") }.data)
            assertEquals(2, server.accepted, "connections: the cut one, then the one that served the article")
        } finally {
            pool.close()
        }
    }

    @Test
    fun `a close_notify between commands fails the next one as a closed connection, and the pool reconnects`() =
        runBlocking<Unit> {
            val server = server { session ->
                session.reader.readLine() // STAT
                session.out.write("223 0 <a@b> exists\r\n".toByteArray())
                session.out.flush()
                session.closeWithNotify()
            }
            client(server).use { client ->
                assertIs<StatResult.Found>(client.stat("<a@b>"))
                val failure = runCatching { client.stat("<a@b>") }.exceptionOrNull()
                assertNotNull(failure, "a STAT after the server's close_notify succeeded")
                assertFalse(failure is CancellationException, "the STAT ended as cancelled: $failure")
                assertTrue(failure is NntpException || failure is IOException, "expected a connection failure, got $failure")
                assertFalse(client.connection.isReusable)
            }
            val pool = pool(server)
            try {
                assertIs<StatResult.Found>(pool.stat("<a@b>"))
                assertIs<StatResult.Found>(pool.stat("<a@b>"), "the second STAT, after the close_notify, on a new connection")
            } finally {
                pool.close()
            }
        }

    @Test
    fun `a body cancelled mid-article is drained and its connection reused`() = runBlocking<Unit> {
        val first = Random(21).nextBytes(300_000)
        val second = Random(22).nextBytes(20_000)
        val firstArticle = article(first)
        val server = server { session ->
            session.reader.readLine() // the first BODY
            session.out.write(firstArticle, 0, 4_096)
            session.out.flush()
            Thread.sleep(800) // the caller gives up at 300 ms
            session.out.write(firstArticle, 4_096, firstArticle.size - 4_096)
            session.out.flush()
            answering(article(second))(session)
        }
        val pool = pool(server)
        try {
            assertFailsWith<TimeoutCancellationException> {
                withTimeout(300) { pool.withClient { it.bodyYencBytes("<first@test>") } }
            }
            assertContentEquals(second, pool.withClient { it.bodyYencBytes("<second@test>") }.data)
            assertEquals(1, server.accepted, "connections: the drained one should have been reused")
        } finally {
            pool.close()
        }
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `a STAT waiting on a server that never answers ends as soon as its caller is cancelled`() = runBlocking<Unit> {
        val received = CountDownLatch(1)
        val server = server { session ->
            while (session.reader.readLine() != null) received.countDown()
        }
        val client = client(server)
        val callers = CoroutineScope(Dispatchers.IO + SupervisorJob())
        try {
            val pending = callers.async { client.stat("<a@b>") }
            assertTrue(received.await(5, TimeUnit.SECONDS), "the STAT never reached the server")
            Thread.sleep(100) // the STAT waits for its answer
            val started = System.nanoTime()
            pending.cancel()
            withTimeout(5_000) { pending.join() }
            val tookMs = (System.nanoTime() - started) / 1_000_000
            assertTrue(tookMs < 1_000, "the cancelled STAT ended $tookMs ms after its cancel")
            assertTrue(pending.isCancelled, "the STAT was not cancelled: ${pending.getCompletionExceptionOrNull()}")
        } finally {
            callers.cancel()
            client.close()
        }
    }

    /**
     * A connection's close leaves its TLS engine closed both ways with nothing left to send: what closing queued (its
     * close_notify, after a handshake) is taken out and dropped. Conscrypt frees its native state (the SSL, its buffers,
     * the session) only then; a close_notify left queued kept it until the engine was finalized.
     */
    @Test
    fun `a closed connection leaves its TLS engine closed with nothing left to send`() = runBlocking<Unit> {
        val server = server(serve = answering())
        val socket = NntpSocket.connect("localhost", server.port, NntpConnection.DEFAULT_READ_TIMEOUT_MS)
        val tls = PlatformTls.connect("localhost", server.port, verifyHostname = true, socket.read, socket.write)
        try {
            assertEquals("200 welcome", tls.read.readLine())
        } finally {
            socket.close()
            tls.release()
        }
        assertTrue(tls.released, "the engine isn't closed both ways, or has something left to send")
    }

    /**
     * TLS reads the socket into its record buffer and nothing else: the socket's own buffer for line reads (64 KB of
     * direct memory) is made only when a line is read from the socket itself, as on a plain connection.
     */
    @Test
    fun `a TLS connection makes no buffer for reading lines from its socket`() = runBlocking<Unit> {
        val data = Random(24).nextBytes(100_000)
        val server = server(serve = answering(article(data)))
        val socket = NntpSocket.connect("localhost", server.port, NntpConnection.DEFAULT_READ_TIMEOUT_MS)
        val tls = PlatformTls.connect("localhost", server.port, verifyHostname = true, socket.read, socket.write)
        try {
            assertEquals("200 welcome", tls.read.readLine())
            assertFalse(socket.read.hasLineBuffer, "the handshake and the welcome made it")
        } finally {
            socket.close()
            tls.release()
        }
    }

    @Test
    fun `closing a connection fails a body waiting on it instead of cancelling it`() = runBlocking<Unit> {
        val whole = article(Random(23).nextBytes(100_000))
        val bodySent = CountDownLatch(1)
        val server = server { session ->
            session.reader.readLine() // BODY
            session.out.write(whole, 0, whole.size / 2)
            session.out.flush()
            bodySent.countDown()
            while (session.reader.readLine() != null) Unit
        }
        val callers = CoroutineScope(Dispatchers.IO + SupervisorJob())
        try {
            val client = client(server)
            val pending = callers.async { client.bodyYencBytes("<waiting@test>") }
            assertTrue(bodySent.await(5, TimeUnit.SECONDS), "the server never sent the body")
            Thread.sleep(200) // the reader waits for the rest
            client.close()
            withTimeout(5_000) { pending.join() }
            val failure = pending.getCompletionExceptionOrNull()
            assertNotNull(failure, "the body was read whole from a closed connection")
            assertFalse(failure is CancellationException, "the body ended as cancelled: $failure")
            assertTrue(failure is IOException || failure is NntpException, "expected an I/O failure, got $failure")
        } finally {
            callers.cancel()
        }
    }
}
