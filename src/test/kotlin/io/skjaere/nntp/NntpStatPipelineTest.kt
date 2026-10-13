package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.ServerSocket
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

class NntpStatPipelineTest {
    private lateinit var serverSocket: ServerSocket
    private lateinit var selectorManager: SelectorManager

    @BeforeEach
    fun setUp() {
        serverSocket = ServerSocket(0)
        selectorManager = SelectorManager(Dispatchers.IO)
    }

    @AfterEach
    fun tearDown() {
        serverSocket.close()
        selectorManager.close()
    }

    private fun launchServer(handler: (BufferedReader, OutputStream) -> Unit) {
        Thread {
            serverSocket.accept().use { socket ->
                val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                val out = socket.getOutputStream()
                out.write("200 welcome\r\n".toByteArray())
                out.flush()
                runCatching { handler(reader, out) }
            }
        }.apply { isDaemon = true }.start()
    }

    /** "STAT <a7@x>" → 223 when the number is even, 430 when odd. */
    private fun answer(command: String): String {
        val id = command.removePrefix("STAT ").trim()
        val n = id.removePrefix("<a").substringBefore('@').toInt()
        return if (n % 2 == 0) "223 0 $id article exists\r\n" else "430 No Such Article Found\r\n"
    }

    private fun ids(n: Int) = (0 until n).map { "<a$it@x>" }

    @Test
    fun `commands are pipelined - the server reads all of them before answering any`() = runBlocking<Unit> {
        launchServer { reader, out ->
            val commands = (1..10).map { reader.readLine() }
            commands.forEach { out.write(answer(it).toByteArray()) }
            out.flush()
        }
        NntpClient.connect("localhost", serverSocket.localPort, selectorManager).use { client ->
            // Without pipelining the client would wait for the first answer forever.
            val results = withTimeout(5_000) { client.statPipelined(ids(10)) }
            assertEquals(List(10) { if (it % 2 == 0) "Found" else "NotFound" }, results.map { it::class.simpleName })
        }
    }

    @Test
    fun `results keep input order with a window smaller than the list`() = runBlocking<Unit> {
        launchServer { reader, out ->
            // Answer each command as it arrives.
            val received = CopyOnWriteArrayList<String>()
            repeat(200) {
                received += reader.readLine()
                out.write(answer(received.last()).toByteArray())
                out.flush()
            }
        }
        NntpClient.connect("localhost", serverSocket.localPort, selectorManager).use { client ->
            val results = withTimeout(10_000) { client.statPipelined(ids(200), window = 8) }
            assertEquals(200, results.size)
            results.forEachIndexed { i, r ->
                if (i % 2 == 0) assertEquals("<a$i@x>", assertIs<StatResult.Found>(r).messageId) else assertIs<StatResult.NotFound>(r)
            }
        }
    }

    @Test
    fun `an unexpected code fails the call only after every answer is read`() = runBlocking<Unit> {
        launchServer { reader, out ->
            val commands = (1..4).map { reader.readLine() }
            commands.forEachIndexed { i, c -> out.write((if (i == 1) "500 Command not recognized\r\n" else answer(c)).toByteArray()) }
            out.flush()
            val next = reader.readLine()
            out.write(answer(next).toByteArray())
            out.flush()
        }
        NntpClient.connect("localhost", serverSocket.localPort, selectorManager).use { client ->
            val e = assertFailsWith<NntpProtocolException> { withTimeout(5_000) { client.statPipelined(ids(4)) } }
            assertEquals(500, e.response?.code)
            // The wire is clean: the next command gets its own answer.
            assertIs<StatResult.Found>(withTimeout(5_000) { client.stat("<a6@x>") })
        }
    }

    @Test
    fun `cancelling mid-pipeline leaves answers that the drain reads off`() = runBlocking<Unit> {
        launchServer { reader, out ->
            val commands = (1..20).map { reader.readLine() }
            // Answer 5 now and the rest only after the client has given up.
            commands.take(5).forEach { out.write(answer(it).toByteArray()) }
            out.flush()
            Thread.sleep(500)
            commands.drop(5).forEach { out.write(answer(it).toByteArray()) }
            out.flush()
            val next = reader.readLine()
            out.write(answer(next).toByteArray())
            out.flush()
        }
        NntpClient.connect("localhost", serverSocket.localPort, selectorManager).use { client ->
            assertNull(withTimeoutOrNull(200) { client.statPipelined(ids(20)) })
            assertTrue(withTimeout(5_000) { client.connection.drainUntilArticleEnd() })
            val after = withTimeout(5_000) { client.stat("<a8@x>") }
            assertEquals("<a8@x>", assertIs<StatResult.Found>(after).messageId)
        }
    }

    @Test
    fun `progress is reported per answer, in order, while the pipeline runs`() = runBlocking<Unit> {
        launchServer { reader, out ->
            val commands = (1..30).map { reader.readLine() }
            // First 10 answers, a pause, then the rest: progress must be visible during the pause.
            commands.take(10).forEach { out.write(answer(it).toByteArray()) }
            out.flush()
            Thread.sleep(400)
            commands.drop(10).forEach { out.write(answer(it).toByteArray()) }
            out.flush()
        }
        val seen = CopyOnWriteArrayList<Pair<Int, StatResult>>()
        NntpClient.connect("localhost", serverSocket.localPort, selectorManager).use { client ->
            val job = async { client.statPipelined(ids(30)) { i, r -> seen += i to r } }
            delay(200)
            assertEquals(10, seen.size, "answers reported before the pause")
            val results = withTimeout(5_000) { job.await() }
            assertEquals((0 until 30).toList(), seen.map { it.first })
            assertEquals(results, seen.map { it.second })
        }
    }
}
