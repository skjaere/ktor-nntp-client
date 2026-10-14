package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
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

/** Message-id arguments: anything that can't be one (RFC 3977 §3.6) never reaches the wire. */
class NntpMessageIdTest {
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

    /** STAT of an even "<aN@x>" → 223, IHAVE → 435, anything else → 430. */
    private fun answer(command: String): String {
        val n = command.removePrefix("STAT <a").substringBefore('@').toIntOrNull()
        return when {
            n != null && n % 2 == 0 -> "223 0 ${command.removePrefix("STAT ")} article exists\r\n"
            command.startsWith("IHAVE ") -> "435 Article not wanted\r\n"
            else -> "430 No Such Article Found\r\n"
        }
    }

    /**
     * Runs [call] on a fresh connection, then STATs <a2@x> on it. Returns the lines the server
     * received and that last answer, which is only right if [call] left nothing on the wire.
     */
    private suspend fun afterCall(call: suspend (NntpClient) -> Unit): Pair<List<String>, StatResult> {
        val received = CopyOnWriteArrayList<String>()
        launchServer { reader, out ->
            generateSequence { reader.readLine() }.forEach { command ->
                received += command
                out.write(answer(command).toByteArray())
                out.flush()
            }
        }
        return NntpClient.connect("localhost", serverSocket.localPort, selectorManager).use { client ->
            withTimeout(5_000) {
                call(client)
                val next = client.stat("<a2@x>")
                received.toList() to next
            }
        }
    }

    private fun assertNothingSent(outcome: Pair<List<String>, StatResult>) {
        assertEquals(listOf("STAT <a2@x>"), outcome.first)
        assertEquals("<a2@x>", assertIs<StatResult.Found>(outcome.second).messageId)
    }

    @Test
    fun `command builders refuse anything that isn't a message-id`() {
        val builders = listOf<(String) -> String>(
            NntpCommand::article, NntpCommand::body, NntpCommand::head, NntpCommand::stat,
            NntpCommand::over, { NntpCommand.hdr("Subject", it) }, NntpCommand::ihave,
        )
        val bad = listOf(INJECTED, "<a@x>\nQUIT", "<a@x>\rQUIT", "<a\u0000@x>", "<a b@x>", "<a\t@x>", "a@x", "<a@x", "a@x>", "<>", "")
        builders.forEach { build ->
            bad.forEach { id -> assertFailsWith<IllegalArgumentException>("id ${id.toList()}") { build(id) } }
            assertEquals("<part1of9.abc\$x@news.example>", build("<part1of9.abc\$x@news.example>").substringAfterLast(' '))
        }
    }

    @Test
    fun `stat of an id with CR LF is NotFound and sends nothing`() = runBlocking<Unit> {
        assertNothingSent(afterCall { assertEquals(430, assertIs<StatResult.NotFound>(it.stat(INJECTED)).code) })
    }

    @Test
    fun `article of an id with CR LF is not found and sends nothing`() = runBlocking<Unit> {
        assertNothingSent(afterCall { assertFailsWith<ArticleNotFoundException> { it.article(INJECTED) } })
    }

    @Test
    fun `head of an id with CR LF is not found and sends nothing`() = runBlocking<Unit> {
        assertNothingSent(afterCall { assertFailsWith<ArticleNotFoundException> { it.head(INJECTED) } })
    }

    @Test
    fun `body of an id with CR LF is not found and sends nothing`() = runBlocking<Unit> {
        assertNothingSent(afterCall { assertFailsWith<ArticleNotFoundException> { it.body(INJECTED) } })
    }

    @Test
    fun `bodyYenc of an id with CR LF is not found and sends nothing`() = runBlocking<Unit> {
        assertNothingSent(afterCall { assertFailsWith<ArticleNotFoundException> { it.bodyYenc(INJECTED).collect() } })
    }

    @Test
    fun `over of an id with CR LF is not found and sends nothing`() = runBlocking<Unit> {
        assertNothingSent(afterCall { assertFailsWith<ArticleNotFoundException> { it.over(INJECTED) } })
    }

    @Test
    fun `hdr of an id with CR LF is not found and sends nothing`() = runBlocking<Unit> {
        assertNothingSent(afterCall { assertFailsWith<ArticleNotFoundException> { it.hdr("Subject", INJECTED) } })
    }

    @Test
    fun `ihave of an id with CR LF is refused and sends nothing`() = runBlocking<Unit> {
        assertNothingSent(afterCall { assertFailsWith<IllegalArgumentException> { it.ihave(INJECTED, "Subject: x\r\n\r\nbody") } })
    }

    private companion object {
        /** Written as is, a second command (STAT <a3@x>) would follow the first on the wire. */
        const val INJECTED = "<a1@x>\r\nSTAT <a3@x>"
    }
}
