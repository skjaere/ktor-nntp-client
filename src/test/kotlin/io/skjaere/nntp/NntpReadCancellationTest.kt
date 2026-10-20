package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.BufferedReader
import java.io.File
import java.io.InputStreamReader
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * A read that waits for the server, and a connect that waits for it to accept, end as soon as their caller is cancelled
 * (a seek closes the stream whose downloads wait), long before the read timeout; a wait never holds a thread other
 * coroutines need: a caller on a single thread leaves it to them while it waits; and a closed connection keeps no
 * descriptor open.
 */
@OptIn(ExperimentalCoroutinesApi::class)
@Timeout(30, unit = TimeUnit.SECONDS)
class NntpReadCancellationTest {

    private lateinit var serverSocket: ServerSocket
    private lateinit var selectorManager: SelectorManager
    private val callers = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val held = mutableListOf<Socket>()

    @BeforeEach
    fun setUp() {
        serverSocket = ServerSocket(0)
        selectorManager = SelectorManager(Dispatchers.IO)
    }

    @AfterEach
    fun tearDown() {
        callers.cancel()
        held.forEach { runCatching { it.close() } }
        serverSocket.close()
        selectorManager.close()
    }

    /** Greets each connection, then reads its commands and counts [received] down for each, answering none. */
    private fun silentServer(received: CountDownLatch, answer: CountDownLatch? = null) {
        Thread {
            while (true) {
                val client = runCatching { serverSocket.accept() }.getOrNull() ?: break
                Thread {
                    client.use { socket ->
                        runCatching {
                            val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                            val out = socket.getOutputStream()
                            out.write("200 welcome\r\n".toByteArray())
                            out.flush()
                            while (reader.readLine() != null) {
                                received.countDown()
                                if (answer != null && answer.await(10, TimeUnit.SECONDS)) {
                                    out.write("111 20261020000000\r\n".toByteArray())
                                    out.flush()
                                }
                            }
                        }
                    }
                }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
    }

    private suspend fun connect() = NntpConnection.connect("localhost", serverSocket.localPort, selectorManager)

    @Test
    fun `a command waiting on a server that never answers ends as soon as its caller is cancelled`() = runBlocking<Unit> {
        val received = CountDownLatch(1)
        silentServer(received)
        val connection = connect()
        try {
            val pending = callers.async { connection.command("DATE") }
            assertTrue(received.await(5, TimeUnit.SECONDS), "DATE never reached the server")
            Thread.sleep(100) // the command waits for its answer
            val started = System.nanoTime()
            pending.cancel()
            withTimeout(5_000) { pending.join() }
            val tookMs = (System.nanoTime() - started) / 1_000_000
            assertTrue(tookMs < 1_000, "the cancelled command ended $tookMs ms after its cancel")
            assertIs<CancellationException>(pending.getCompletionExceptionOrNull())
        } finally {
            connection.close()
        }
    }

    @Test
    fun `a command waiting on a single thread ends as soon as it is cancelled`() = runBlocking<Unit> {
        val received = CountDownLatch(1)
        silentServer(received)
        val connection = connect()
        try {
            // runBlocking's own thread: the command's wait must not hold it, or the cancel below never runs.
            val pending = async { connection.command("DATE") }
            withTimeout(5_000) { while (received.count > 0) delay(10) }
            delay(100)
            val started = System.nanoTime()
            pending.cancel()
            withTimeout(5_000) { pending.join() }
            val tookMs = (System.nanoTime() - started) / 1_000_000
            assertTrue(tookMs < 1_000, "the cancelled command ended $tookMs ms after its cancel")
            assertTrue(pending.isCancelled, "the command was not cancelled: ${pending.getCompletionExceptionOrNull()}")
        } finally {
            connection.close()
        }
    }

    @Test
    fun `a command waiting on a single thread leaves the thread to its other coroutines`() = runBlocking<Unit> {
        val received = CountDownLatch(1)
        val answer = CountDownLatch(1)
        silentServer(received, answer)
        val connection = connect()
        try {
            // The server answers only once another coroutine on this thread lets it.
            launch {
                withTimeout(5_000) { while (received.count > 0) delay(10) }
                answer.countDown()
            }
            assertEquals(111, withTimeout(10_000) { connection.command("DATE") }.code)
        } finally {
            connection.close()
        }
    }

    @Test
    fun `a read that keeps finding bytes ends as soon as its caller is cancelled`() = runBlocking<Unit> {
        val sending = CountDownLatch(1)
        Thread {
            runCatching {
                serverSocket.accept().use { socket ->
                    val out = socket.getOutputStream()
                    out.write("200 welcome\r\n".toByteArray())
                    out.flush()
                    // As fast as the client takes it, until it goes away.
                    val chunk = ("a".repeat(126) + "\r\n").repeat(512).toByteArray()
                    while (true) {
                        out.write(chunk)
                        sending.countDown()
                    }
                }
            }
        }.apply { isDaemon = true }.start()
        val connection = connect()
        try {
            val pending = callers.async {
                val buffer = ByteArray(64 * 1024)
                // Slower than the server, as a reader starved of CPU is: the socket always holds bytes for the next read.
                while (connection.readAvailable(buffer) >= 0) Thread.sleep(2)
            }
            assertTrue(sending.await(5, TimeUnit.SECONDS), "the server never sent")
            delay(300) // reading all the while
            val started = System.nanoTime()
            pending.cancel()
            withTimeout(5_000) { pending.join() }
            val tookMs = (System.nanoTime() - started) / 1_000_000
            assertTrue(tookMs < 1_000, "the cancelled read ended $tookMs ms after its cancel")
            assertTrue(pending.isCancelled, "the read was not cancelled: ${pending.getCompletionExceptionOrNull()}")
        } finally {
            connection.close()
        }
    }

    @Test
    fun `closed connections keep no descriptors open, whether a read waited on them or not`() = runBlocking<Unit> {
        val descriptors = File("/proc/self/fd")
        assumeTrue(descriptors.isDirectory, "needs /proc/self/fd")
        silentServer(CountDownLatch(0))
        // The first connection makes what stays for the process (a selector provider, Ktor's selector).
        connect().close()
        delay(300) // and its server end closes
        val before = descriptors.list()!!.size
        repeat(10) { i ->
            val connection = connect()
            if (i % 2 == 0) {
                val pending = callers.async { connection.command("DATE") }
                delay(50) // the command waits for its answer
                connection.close()
                withTimeout(5_000) { pending.join() }
            } else {
                connection.close()
            }
        }
        // The server's ends are in this process too: they close as it reads each client's end.
        val settled = withTimeoutOrNull(5_000) {
            while (descriptors.list()!!.size > before) delay(50)
        }
        assertNotNull(settled, "descriptors open: ${descriptors.list()!!.size}, before the connections: $before")
    }

    @Test
    fun `a connect the server never accepts ends as soon as its caller is cancelled`() = runBlocking<Unit> {
        // A listener that accepts nothing and has room for one connection: once its queue is full, the kernel drops
        // the next connect's SYNs, and that connect waits.
        val full = ServerSocket(0, 1)
        try {
            val address = InetSocketAddress("localhost", full.localPort)
            repeat(8) {
                val socket = Socket()
                try {
                    socket.connect(address, 300)
                    held += socket
                } catch (_: SocketTimeoutException) {
                    socket.close()
                    return@repeat
                }
            }
            val pending = callers.async { NntpConnection.connect("localhost", full.localPort, selectorManager) }
            delay(300) // the connect waits for the server
            assertTrue(pending.isActive, "the connect ended before it was cancelled")
            val started = System.nanoTime()
            pending.cancel()
            withTimeout(5_000) { pending.join() }
            val tookMs = (System.nanoTime() - started) / 1_000_000
            assertTrue(tookMs < 1_000, "the cancelled connect ended $tookMs ms after its cancel")
            assertTrue(pending.isCancelled, "the connect was not cancelled: ${pending.getCompletionExceptionOrNull()}")
        } finally {
            full.close()
        }
    }
}
