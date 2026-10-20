package io.skjaere.nntp

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.ktor.network.selector.SelectorManager
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.slf4j.LoggerFactory
import java.security.cert.CertificateException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Over TLS the server's certificate must name the host connected to, and that host name is
 * sent as the server name (SNI), unless hostname verification is turned off. The test
 * certificates are self-signed and trusted through the JVM's default trust store, the one
 * the library checks certificates against.
 */
@OptIn(ExperimentalCoroutinesApi::class)
@Timeout(20, unit = TimeUnit.SECONDS)
class NntpTlsTest {

    companion object {
        private lateinit var certificates: TestCertificates

        @JvmStatic
        @BeforeAll
        fun createCertificates() {
            certificates = TestCertificates.create(
                mapOf(
                    "other-host" to ("CN=other.example" to "dns:other.example"),
                    "localhost" to ("CN=localhost" to "dns:localhost"),
                    "no-names" to ("CN=other.example" to null),
                    "loopback-address" to ("CN=127.0.0.1" to "ip:127.0.0.1"),
                )
            )
        }

        @JvmStatic
        @AfterAll
        fun deleteCertificates() {
            certificates.close()
        }
    }

    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope

    @BeforeEach
    fun setUp() {
        certificates.useAsDefaultTrustStore()
        selectorManager = SelectorManager(Dispatchers.IO)
        poolScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    }

    @AfterEach
    fun tearDown() {
        poolScope.cancel()
        selectorManager.close()
        certificates.restoreDefaultTrustStore()
    }

    private fun Throwable.causes() = generateSequence(this) { it.cause }.toList()

    private fun assertCertificateRefused(e: Throwable) {
        assertTrue(e.causes().any { it is CertificateException }, "the failure doesn't say why: ${e.causes()}")
    }

    @Test
    fun `a certificate that names another host is refused`() = runBlocking<Unit> {
        TlsNntpServer(certificates.keyStore("other-host")).use { server ->
            val e = assertFailsWith<NntpConnectionException> {
                NntpConnection.connect("localhost", server.port, selectorManager, useTls = true)
            }
            assertCertificateRefused(e)
        }
    }

    @Test
    fun `a certificate that names another host is accepted with hostname verification off`() = runBlocking<Unit> {
        TlsNntpServer(certificates.keyStore("other-host")).use { server ->
            NntpConnection.connect("localhost", server.port, selectorManager, useTls = true, verifyHostname = false)
                .use { assertEquals(200, it.welcomeResponse.code) }
        }
    }

    /** Hostname verification off still checks the certificate is trusted. */
    @Test
    fun `a certificate the trust store does not trust is refused with hostname verification off`() =
        runBlocking<Unit> {
            certificates.restoreDefaultTrustStore()
            TlsNntpServer(certificates.keyStore("localhost")).use { server ->
                val e = assertFailsWith<NntpConnectionException> {
                    NntpConnection.connect("localhost", server.port, selectorManager, useTls = true, verifyHostname = false)
                }
                assertCertificateRefused(e)
            }
        }

    @Test
    fun `a certificate that names the host is accepted`() = runBlocking<Unit> {
        TlsNntpServer(certificates.keyStore("localhost")).use { server ->
            NntpConnection.connect("localhost", server.port, selectorManager, useTls = true)
                .use { assertEquals(200, it.welcomeResponse.code) }
        }
    }

    @Test
    fun `the host name is sent as the server name (SNI)`() = runBlocking<Unit> {
        TlsNntpServer(certificates.keyStore("localhost")).use { server ->
            NntpClient.connect("localhost", server.port, selectorManager, useTls = true).close()
            assertEquals(listOf(listOf("localhost")), server.requestedServerNames, "server names asked for")
        }
    }

    @Test
    fun `with hostname verification off no server name is sent`() = runBlocking<Unit> {
        TlsNntpServer(certificates.keyStore("localhost")).use { server ->
            NntpClient.connect("localhost", server.port, selectorManager, useTls = true, verifyHostname = false).close()
            assertEquals(listOf(emptyList()), server.requestedServerNames, "server names asked for")
        }
    }

    /**
     * Ktor checks the host name against the certificate's DNS names only when there are
     * some: a certificate that lists none (a common name only, or IP addresses only) would
     * pass for any host.
     */
    @Test
    fun `a certificate that lists no host names is refused`() = runBlocking<Unit> {
        TlsNntpServer(certificates.keyStore("no-names")).use { server ->
            val e = assertFailsWith<NntpConnectionException> {
                NntpConnection.connect("localhost", server.port, selectorManager, useTls = true)
            }
            assertCertificateRefused(e)
        }
    }

    @Test
    fun `connecting by IP address accepts a certificate for that address and sends no server name`() =
        runBlocking<Unit> {
            TlsNntpServer(certificates.keyStore("loopback-address")).use { server ->
                NntpConnection.connect("127.0.0.1", server.port, selectorManager, useTls = true)
                    .use { assertEquals(200, it.welcomeResponse.code) }
                // RFC 6066 §3: an IP address is not a server name.
                assertEquals(listOf(emptyList()), server.requestedServerNames, "server names asked for")
            }
        }

    /** Ktor's own check passes any certificate that lists no IP addresses when given one. */
    @Test
    fun `connecting by IP address refuses a certificate that does not list that address`() = runBlocking<Unit> {
        TlsNntpServer(certificates.keyStore("localhost")).use { server ->
            val e = assertFailsWith<NntpConnectionException> {
                NntpConnection.connect("127.0.0.1", server.port, selectorManager, useTls = true)
            }
            assertCertificateRefused(e)
        }
    }

    @Test
    fun `a pool's connect to a server whose certificate names another host fails at once`() = runBlocking<Unit> {
        TlsNntpServer(certificates.keyStore("other-host")).use { server ->
            val pool = NntpClientPool(
                host = "localhost", port = server.port, selectorManager = selectorManager, useTls = true,
                maxConnections = 1, scope = poolScope, keepaliveIntervalMs = 0, idleGracePeriodMs = 0
            )
            try {
                val call = poolScope.async { pool.stat("<test@msg>") }
                assertNotNull(withTimeoutOrNull(5_000) { call.join() }, "the acquire still waits after 5 s")
                val failure = assertIs<NntpConnectionException>(call.getCompletionExceptionOrNull(), "the acquire's failure")
                assertCertificateRefused(failure)
                delay(1_500) // a retry used to come after 1 s
                assertEquals(1, server.accepted, "connections: a refused certificate was tried again")
            } finally {
                pool.close()
            }
        }
    }

    @Test
    fun `a pool with hostname verification off connects to a server whose certificate names another host`() =
        runBlocking<Unit> {
            TlsNntpServer(certificates.keyStore("other-host")).use { server ->
                val pool = NntpClientPool(
                    host = "localhost", port = server.port, selectorManager = selectorManager, useTls = true,
                    maxConnections = 1, scope = poolScope, keepaliveIntervalMs = 0, idleGracePeriodMs = 0,
                    verifyHostname = false
                )
                try {
                    assertIs<StatResult.Found>(pool.stat("<test@msg>"))
                } finally {
                    pool.close()
                }
            }
        }

    /** Answers STAT and QUIT on a [ScriptedTlsServer] connection until the client goes away. */
    private val answeringStat: (ScriptedTlsServer.TlsSession) -> Unit = { session ->
        while (true) {
            val command = session.reader.readLine() ?: break
            session.out.write((if (command == "QUIT") "205 bye\r\n" else "223 0 <a@b> exists\r\n").toByteArray())
            session.out.flush()
            if (command == "QUIT") break
        }
    }

    /** TLS 1.3, which the platform's TLS speaks and Ktor's doesn't. */
    @Test
    fun `the newest TLS version a server offers is used`() = runBlocking<Unit> {
        ScriptedTlsServer(certificates.keyStore("localhost"), protocols = arrayOf("TLSv1.2", "TLSv1.3")).use { server ->
            NntpClient.connect("localhost", server.port, selectorManager, useTls = true).close()
            assertEquals(listOf("TLSv1.3"), server.negotiated, "the protocol the handshake chose")
        }
    }

    @Test
    fun `a server that speaks only TLS 1_3 is reached`() = runBlocking<Unit> {
        ScriptedTlsServer(certificates.keyStore("localhost"), protocols = arrayOf("TLSv1.3"), serve = answeringStat).use { server ->
            NntpClient.connect("localhost", server.port, selectorManager, useTls = true).use { client ->
                assertIs<StatResult.Found>(client.stat("<a@b>"))
            }
        }
    }

    /** The handshake's reads are the connection's reads: a server that goes silent in it is given up on as in any other. */
    @Test
    fun `a server that never answers the handshake fails the connect after the read timeout`() = runBlocking<Unit> {
        ScriptedTlsServer(certificates.keyStore("localhost"), handshake = false).use { server ->
            var failure: Throwable? = null
            val finished = withTimeoutOrNull(5_000) {
                failure = runCatching {
                    NntpConnection.connect("localhost", server.port, selectorManager, useTls = true, readTimeoutMs = 300).close()
                }.exceptionOrNull()
            }
            assertNotNull(finished, "the connect was still waiting for the handshake after 5 s")
            assertIs<NntpConnectionException>(failure)
        }
    }

    /** What a provider's TLS chose is worth a line when looking into its speed, but one per pool, not one per connection. */
    @Test
    fun `a pool logs the protocol and suite its connections negotiated once`() = runBlocking<Unit> {
        val logger = LoggerFactory.getLogger(NntpClientPool::class.java) as ch.qos.logback.classic.Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        val level = logger.level
        logger.level = Level.DEBUG
        logger.addAppender(appender)
        try {
            val protocols = arrayOf("TLSv1.2", "TLSv1.3")
            ScriptedTlsServer(certificates.keyStore("localhost"), protocols = protocols, serve = answeringStat).use { server ->
                val pool = NntpClientPool(
                    host = "localhost", port = server.port, selectorManager = selectorManager, useTls = true,
                    maxConnections = 2, scope = poolScope, keepaliveIntervalMs = 0, idleGracePeriodMs = 0
                )
                try {
                    // Two connections, each held until both are open.
                    val bothOpen = CountDownLatch(2)
                    coroutineScope {
                        repeat(2) {
                            launch {
                                pool.withClient { client ->
                                    bothOpen.countDown()
                                    withContext(Dispatchers.IO) { bothOpen.await(5, TimeUnit.SECONDS) }
                                    client.stat("<a@b>")
                                }
                            }
                        }
                    }
                    assertEquals(2, server.accepted, "connections opened")
                    val protocol = server.negotiated.first()
                    val lines = appender.list.filter { it.level == Level.DEBUG }.map { it.formattedMessage }.filter { protocol in it }
                    assertEquals(1, lines.size, "lines naming the protocol: $lines")
                    assertTrue("TLS_" in lines.single(), "the line should name the suite: ${lines.single()}")
                } finally {
                    pool.close()
                }
            }
        } finally {
            logger.detachAppender(appender)
            logger.level = level
        }
    }

    @Test
    fun `a certificate's DNS name matches the host ignoring case and a trailing dot`() {
        assertTrue(dnsNameMatches("news.example.com", "news.example.com"))
        assertTrue(dnsNameMatches("News.Example.COM.", "news.example.com"))
        assertFalse(dnsNameMatches("news.example.com", "example.com"))
        assertFalse(dnsNameMatches("example.com", "news.example.com"))
    }

    @Test
    fun `a wildcard in a certificate's DNS name stands for one whole left-most label`() {
        assertTrue(dnsNameMatches("news.example.com", "*.example.com"))
        assertTrue(dnsNameMatches("NEWS.example.com", "*.Example.com"))
        assertFalse(dnsNameMatches("example.com", "*.example.com"))
        assertFalse(dnsNameMatches("a.news.example.com", "*.example.com"))
        assertFalse(dnsNameMatches("news.example.com", "n*.example.com"))
        assertFalse(dnsNameMatches("news.example.com", "news.*.com"))
        assertFalse(dnsNameMatches("example.com", "*.com"))
    }

    @Test
    fun `only an IPv4 or IPv6 literal is read as an IP address`() {
        assertEquals(java.net.InetAddress.getByName("127.0.0.1"), ipAddress("127.0.0.1"))
        assertEquals(java.net.InetAddress.getByName("::1"), ipAddress("::1"))
        assertEquals(java.net.InetAddress.getByName("::1"), ipAddress("[::1]"))
        assertEquals(java.net.InetAddress.getByName("::1"), ipAddress("0:0:0:0:0:0:0:1"))
        listOf("localhost", "news.example.com", "256.1.1.1", "1.2.3", "1.2.3.4.5", "1.2.3.x", "").forEach {
            assertEquals(null, ipAddress(it), "'$it' as an IP address")
        }
    }
}
