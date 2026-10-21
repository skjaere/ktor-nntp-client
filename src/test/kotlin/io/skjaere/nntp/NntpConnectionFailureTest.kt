package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * A connection that fails under a command fails it with an [NntpConnectionException], whatever ended it. One the server
 * or the network ended (a close in the middle of a line or of an article's data, a reset) is an
 * [NntpConnectionLostException], its cause the I/O error if there was one; one the read timeout or our own close ended
 * stays a plain [NntpConnectionException], as does a connect that fails.
 */
@Timeout(30, unit = TimeUnit.SECONDS)
class NntpConnectionFailureTest {

    private lateinit var serverSocket: ServerSocket
    private lateinit var selectorManager: SelectorManager
    private val callers = CoroutineScope(Dispatchers.IO + SupervisorJob())

    @BeforeEach
    fun setUp() {
        serverSocket = ServerSocket(0)
        selectorManager = SelectorManager(Dispatchers.IO)
    }

    @AfterEach
    fun tearDown() {
        callers.cancel()
        serverSocket.close()
        selectorManager.close()
    }

    /** Greets one connection, reads its first command, then does [then] with the socket. */
    private fun serveOnce(then: (Socket) -> Unit) {
        Thread {
            runCatching {
                serverSocket.accept().use { socket ->
                    val reader = BufferedReader(InputStreamReader(socket.getInputStream(), Charsets.ISO_8859_1))
                    socket.getOutputStream().apply {
                        write("200 welcome\r\n".toByteArray())
                        flush()
                    }
                    reader.readLine()
                    then(socket)
                }
            }
        }.apply { isDaemon = true }.start()
    }

    private suspend fun client(readTimeoutMs: Long = NntpConnection.DEFAULT_READ_TIMEOUT_MS) =
        NntpClient.connect("localhost", serverSocket.localPort, selectorManager, readTimeoutMs = readTimeoutMs)

    /** Sends a BODY's status line, its yEnc headers and a line of its data, the rest of the article not following. */
    private fun Socket.sendArticleStart() {
        getOutputStream().apply {
            write("222 0 <cut@test>\r\n=ybegin part=1 line=128 size=716800 name=x.bin\r\n=ypart begin=1 end=716800\r\n".toByteArray())
            write("0123456789abcdefghijklmnopqrstuvwxyz\r\n".toByteArray())
            flush()
        }
    }

    @Test
    fun `a server that closes after an article's =ybegin line fails the body as a lost connection`() = runBlocking<Unit> {
        serveOnce { socket ->
            socket.getOutputStream().apply {
                write("222 0 <cut@test>\r\n=ybegin part=1 line=128 size=716800 name=x.bin\r\n".toByteArray())
                flush()
            }
        }
        client().use { client ->
            val failure = assertFailsWith<NntpConnectionLostException> { client.bodyYencBytes("<cut@test>") }
            assertTrue(failure.cause is java.io.EOFException, "the cause should be the end of the stream: ${failure.cause}")
            assertFalse(client.connection.isReusable)
        }
    }

    @Test
    fun `a server that closes in the middle of an article's data fails the body as a lost connection`() = runBlocking<Unit> {
        serveOnce { socket -> socket.sendArticleStart() }
        client().use { client ->
            val failure = assertFailsWith<NntpConnectionLostException> { client.bodyYencBytes("<cut@test>") }
            // The socket's end: no I/O error, just no more bytes.
            assertNull(failure.cause, "a close in the data ends the stream without an I/O error: ${failure.cause}")
            assertFalse(client.connection.isReusable)
        }
    }

    @Test
    fun `a reset fails the command as a lost connection`() = runBlocking<Unit> {
        serveOnce { socket ->
            // Closed with SO_LINGER 0: the client gets a reset, not a FIN.
            socket.setSoLinger(true, 0)
        }
        client().use { client ->
            val failure = assertFailsWith<NntpConnectionLostException> { client.stat("<a@b>") }
            assertIs<java.io.IOException>(failure.cause)
            assertFalse(client.connection.isReusable)
        }
    }

    @Test
    fun `a reset in the middle of an article's data fails the body as a lost connection`() = runBlocking<Unit> {
        serveOnce { socket ->
            socket.sendArticleStart()
            socket.setSoLinger(true, 0)
        }
        client().use { client ->
            val failure = assertFailsWith<NntpConnectionLostException> { client.bodyYencBytes("<cut@test>") }
            assertIs<java.io.IOException>(failure.cause)
            assertFalse(client.connection.isReusable)
        }
    }

    /** A connect fails with an [NntpConnectionException] as its KDoc says, also when the greeting is cut off (K48). */
    @Test
    fun `a reset while the welcome is read fails the connect as a connection failure, not a lost connection`() = runBlocking<Unit> {
        Thread {
            runCatching {
                serverSocket.accept().use { socket ->
                    socket.getOutputStream().apply {
                        write("200 wel".toByteArray())
                        flush()
                    }
                    // Long enough for the client to have connected and wait for the rest of the line.
                    Thread.sleep(200)
                    socket.setSoLinger(true, 0)
                }
            }
        }.apply { isDaemon = true }.start()
        val failure = assertFailsWith<NntpConnectionException> { client() }
        assertFalse(failure is NntpConnectionLostException, "a connect that fails isn't a lost connection: $failure")
        assertIs<java.io.IOException>(failure.cause)
    }

    @Test
    fun `the read timeout fails the command as a connection failure, not a lost connection`() = runBlocking<Unit> {
        val answer = CountDownLatch(1)
        serveOnce { answer.await(10, TimeUnit.SECONDS) }
        try {
            client(readTimeoutMs = 300).use { client ->
                val failure = assertFailsWith<NntpConnectionException> { client.stat("<a@b>") }
                assertFalse(failure is NntpConnectionLostException, "the read timeout isn't a lost connection: $failure")
                assertFalse(client.connection.isReusable)
            }
        } finally {
            answer.countDown()
        }
    }

    @Test
    fun `a connection closed under a waiting STAT fails it as a closed connection, not a lost one`() = runBlocking<Unit> {
        val received = CountDownLatch(1)
        val answer = CountDownLatch(1)
        serveOnce {
            received.countDown()
            answer.await(10, TimeUnit.SECONDS)
        }
        val client = client()
        try {
            val pending = callers.async { client.stat("<a@b>") }
            assertTrue(received.await(5, TimeUnit.SECONDS), "the STAT never reached the server")
            Thread.sleep(100) // the STAT waits for its answer
            client.close()
            withTimeout(5_000) { pending.join() }
            val failure = pending.getCompletionExceptionOrNull()
            assertIs<NntpConnectionException>(failure, "expected a connection failure, got $failure")
            assertFalse(failure is NntpConnectionLostException, "our own close isn't a lost connection: $failure")
        } finally {
            answer.countDown()
            client.close()
        }
    }
}
