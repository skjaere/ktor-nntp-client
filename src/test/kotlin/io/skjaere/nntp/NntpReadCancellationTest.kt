package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.asCoroutineDispatcher
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
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random
import kotlin.test.assertContentEquals
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
    fun `an article read whole ends as soon as its caller is cancelled while its bytes trickle in`() = runBlocking<Unit> {
        val sending = CountDownLatch(1)
        Thread {
            runCatching {
                serverSocket.accept().use { socket ->
                    val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                    val out = socket.getOutputStream()
                    out.write("200 welcome\r\n".toByteArray())
                    out.flush()
                    reader.readLine() // BODY
                    out.write("222 0 <slow@test>\r\n=ybegin line=128 size=10000000 name=slow.bin\r\n".toByteArray())
                    // A line of data every 10 ms, the article never finished: the reader reads it bit by bit.
                    val line = ("a".repeat(128) + "\r\n").toByteArray()
                    while (true) {
                        out.write(line)
                        out.flush()
                        sending.countDown()
                        Thread.sleep(10)
                    }
                }
            }
        }.apply { isDaemon = true }.start()
        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        try {
            val pending = callers.async { client.bodyYencBytes("<slow@test>") }
            assertTrue(sending.await(5, TimeUnit.SECONDS), "the server never sent the article")
            delay(300) // the article arrives a line at a time
            val started = System.nanoTime()
            pending.cancel()
            withTimeout(5_000) { pending.join() }
            val tookMs = (System.nanoTime() - started) / 1_000_000
            assertTrue(tookMs < 1_000, "the cancelled article ended $tookMs ms after its cancel")
            assertTrue(pending.isCancelled, "the article was not cancelled: ${pending.getCompletionExceptionOrNull()}")
        } finally {
            client.close()
        }
    }

    /**
     * A caller off Dispatchers.IO reads through a hop there. Cancelled after the hop has read an article's end but
     * before the caller resumes (its thread busy meanwhile), the hop's result is dropped; the bytes it took are the
     * drain's all the same, which finds the terminator at once and keeps the connection.
     */
    @Test
    fun `a body cancelled after the hop to IO read its end is drained at once and its connection kept`() = runBlocking<Unit> {
        val data = Random(7).nextBytes(64 * 1024)
        val article = article(data)
        val firstPart = article.size - 1_000
        val firstSent = CountDownLatch(1)
        val sendRest = CountDownLatch(1)
        val accepted = AtomicInteger()
        Thread {
            while (true) {
                val client = runCatching { serverSocket.accept() }.getOrNull() ?: break
                accepted.incrementAndGet()
                Thread {
                    client.use { socket ->
                        runCatching {
                            val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                            val out = socket.getOutputStream()
                            out.write("200 welcome\r\n".toByteArray())
                            out.flush()
                            var bodies = 0
                            while (true) {
                                val command = reader.readLine() ?: break
                                when {
                                    command.startsWith("BODY") && bodies++ == 0 -> {
                                        // All but the end, and the end once the test has the client's thread busy.
                                        out.write(article, 0, firstPart)
                                        out.flush()
                                        firstSent.countDown()
                                        sendRest.await(10, TimeUnit.SECONDS)
                                        out.write(article, firstPart, article.size - firstPart)
                                    }
                                    command.startsWith("BODY") -> out.write(article)
                                    command == "QUIT" -> out.write("205 bye\r\n".toByteArray())
                                    else -> out.write("500 unknown\r\n".toByteArray())
                                }
                                out.flush()
                            }
                        }
                    }
                }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
        val callerThread = Executors.newSingleThreadExecutor()
        val caller = callerThread.asCoroutineDispatcher()
        val pool = NntpClientPool(
            host = "localhost", port = serverSocket.localPort, selectorManager = selectorManager, maxConnections = 1,
            scope = callers, keepaliveIntervalMs = 0, idleGracePeriodMs = 0,
        )
        try {
            val pending = CoroutineScope(caller).async { pool.withClient { it.bodyYencBytes("<a@test>") } }
            assertTrue(firstSent.await(5, TimeUnit.SECONDS), "the server never sent the article")
            delay(300) // the client has the first part, and waits in its hop to IO for the rest
            val busy = CountDownLatch(1)
            callerThread.execute { busy.await(10, TimeUnit.SECONDS) }
            sendRest.countDown()
            delay(300) // the hop reads the article's end, and its result waits for the caller's thread
            pending.cancel()
            val started = System.nanoTime()
            busy.countDown()
            withTimeout(10_000) { pending.join() }
            val tookMs = (System.nanoTime() - started) / 1_000_000
            assertTrue(pending.isCancelled, "the article was not cancelled: ${pending.getCompletionExceptionOrNull()}")
            assertTrue(tookMs < 1_000, "the cancelled article ended $tookMs ms after its cancel: its drain missed the terminator")
            // The connection was kept: the next article comes over it.
            assertContentEquals(data, pool.withClient { it.bodyYencBytes("<a@test>") }.data)
            assertEquals(1, accepted.get(), "the connection was discarded and another one opened")
        } finally {
            pool.close()
            caller.close()
        }
    }

    /**
     * A caller off Dispatchers.IO connects through a hop there. Cancelled once the hop has connected but before the
     * caller resumes (its thread busy meanwhile), the hop's socket is dropped with its result: it must be closed, not
     * left open with its descriptors and the server's session.
     */
    @Test
    fun `a connect cancelled after the hop to IO connected closes its socket`() = runBlocking<Unit> {
        val callerThread = Executors.newSingleThreadExecutor()
        val caller = callerThread.asCoroutineDispatcher()
        val busy = CountDownLatch(1)
        try {
            // The caller's thread busy from the start: the connect runs up to its hop here, and its result waits.
            callerThread.execute { busy.await(10, TimeUnit.SECONDS) }
            val pending = CoroutineScope(caller).async(start = CoroutineStart.UNDISPATCHED) {
                NntpConnection.connect("localhost", serverSocket.localPort, selectorManager)
            }
            serverSocket.soTimeout = 5_000
            val accepted = serverSocket.accept()
            held += accepted
            delay(300) // the hop has connected, and its result waits for the caller's thread
            pending.cancel()
            busy.countDown()
            withTimeout(5_000) { pending.join() }
            assertTrue(pending.isCancelled, "the connect was not cancelled: ${pending.getCompletionExceptionOrNull()}")
            accepted.soTimeout = 3_000
            val read = try {
                accepted.getInputStream().read()
            } catch (_: SocketTimeoutException) {
                null
            }
            assertEquals(-1, read, "the client's socket is still open")
        } finally {
            busy.countDown()
            caller.close()
        }
    }

    /** A yEnc article as a server sends it after BODY: its 222, a part of a larger file, its =yend, the terminator. */
    private fun article(data: ByteArray): ByteArray {
        val encoded = String(RapidYenc.encode(data), Charsets.ISO_8859_1)
        return buildString {
            append("222 0 <a@test>\r\n")
            append("=ybegin part=1 total=1 line=128 size=${data.size} name=data.bin\r\n")
            append("=ypart begin=1 end=${data.size}\r\n")
            append(encoded)
            if (!encoded.endsWith("\r\n")) append("\r\n")
            append("=yend size=${data.size} part=1 pcrc32=${RapidYenc.crc32(data).toString(16).padStart(8, '0')}\r\n")
            append(".\r\n")
        }.toByteArray(Charsets.ISO_8859_1)
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
