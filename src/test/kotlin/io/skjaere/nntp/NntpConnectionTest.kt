package io.skjaere.nntp

import io.ktor.network.selector.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.ServerSocket
import java.net.Socket
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class NntpConnectionTest {

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

    private fun acceptAndHandle(handler: (BufferedReader, PrintWriter) -> Unit) {
        val client: Socket = serverSocket.accept()
        client.use { socket ->
            val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
            val writer = PrintWriter(socket.getOutputStream(), true)
            handler(reader, writer)
        }
    }

    @Test
    fun `connect reads welcome message`() = runTest {
        launch(Dispatchers.IO) {
            acceptAndHandle { _, writer ->
                writer.print("200 service available\r\n")
                writer.flush()
            }
        }

        val connection = NntpConnection.connect(
            "localhost", serverSocket.localPort, selectorManager
        )
        assertEquals(200, connection.welcomeResponse.code)
        assertEquals("service available", connection.welcomeResponse.message)
        connection.close()
    }

    @Test
    fun `connect rejects non-200 welcome`() = runTest {
        launch(Dispatchers.IO) {
            acceptAndHandle { _, writer ->
                writer.print("502 access denied\r\n")
                writer.flush()
            }
        }

        assertFailsWith<NntpProtocolException> {
            NntpConnection.connect(
                "localhost", serverSocket.localPort, selectorManager
            )
        }
    }

    @Test
    fun `command sends and receives single-line response`() = runTest {
        launch(Dispatchers.IO) {
            acceptAndHandle { reader, writer ->
                writer.print("200 welcome\r\n")
                writer.flush()
                val cmd = reader.readLine()
                assertEquals("DATE", cmd)
                writer.print("111 20260208120000\r\n")
                writer.flush()
            }
        }

        val connection = NntpConnection.connect(
            "localhost", serverSocket.localPort, selectorManager
        )
        val response = connection.command("DATE")
        assertEquals(111, response.code)
        assertEquals("20260208120000", response.message)
        connection.close()
    }

    @Test
    fun `commandMultiLine reads multi-line data with dot termination`() = runTest {
        launch(Dispatchers.IO) {
            acceptAndHandle { reader, writer ->
                writer.print("200 welcome\r\n")
                writer.flush()
                reader.readLine() // HELP command
                writer.print("100 help follows\r\n")
                writer.print("ARTICLE\r\n")
                writer.print("BODY\r\n")
                writer.print("HEAD\r\n")
                writer.print(".\r\n")
                writer.flush()
            }
        }

        val connection = NntpConnection.connect(
            "localhost", serverSocket.localPort, selectorManager
        )
        val (response, lines) = connection.commandMultiLine("HELP")
        assertEquals(100, response.code)
        assertEquals(listOf("ARTICLE", "BODY", "HEAD"), lines)
        connection.close()
    }

    @Test
    fun `commandMultiLine handles dot-unstuffing`() = runTest {
        launch(Dispatchers.IO) {
            acceptAndHandle { reader, writer ->
                writer.print("200 welcome\r\n")
                writer.flush()
                reader.readLine() // ARTICLE command
                writer.print("220 1 <msg@host> article follows\r\n")
                writer.print("Subject: Test\r\n")
                writer.print("\r\n")
                writer.print("Line one\r\n")
                writer.print("..dot-stuffed line\r\n")
                writer.print(".\r\n")
                writer.flush()
            }
        }

        val connection = NntpConnection.connect(
            "localhost", serverSocket.localPort, selectorManager
        )
        val (response, lines) = connection.commandMultiLine("ARTICLE <msg@host>")
        assertEquals(220, response.code)
        assertEquals(
            listOf("Subject: Test", "", "Line one", ".dot-stuffed line"),
            lines
        )
        connection.close()
    }

    @Test
    fun `commandMultiLine returns empty list for error response`() = runTest {
        launch(Dispatchers.IO) {
            acceptAndHandle { reader, writer ->
                writer.print("200 welcome\r\n")
                writer.flush()
                reader.readLine()
                writer.print("430 No Such Article\r\n")
                writer.flush()
            }
        }

        val connection = NntpConnection.connect(
            "localhost", serverSocket.localPort, selectorManager
        )
        val (response, lines) = connection.commandMultiLine("ARTICLE <nonexistent@host>")
        assertEquals(430, response.code)
        assertEquals(emptyList(), lines)
        connection.close()
    }

    /**
     * Regression for the wire-corruption bug behind the prod `NntpProtocolException:
     * Invalid response code: <binary garbage>` reports. drainUntilArticleEnd's
     * single-line fast-path checked whether the first \r\n-terminated chunk
     * looked like a 3-digit non-222 status code. yenc data lines occasionally
     * start with three ASCII digits + \r\n purely by chance; the drain would
     * declare the wire clean while body bytes were still queued, the connection
     * would be returned to the pool, and the next reader would read those body
     * bytes as the next response. Fix: drainUntilArticleEnd now skips the
     * single-line heuristic when markBodyResponseRead has already been called,
     * because we know the status line is past us and any further \r\n is just a
     * yenc inter-line terminator.
     *
     * Reproducer: simulate a body whose first chunk starts with "430\r\n…" —
     * exactly the kind of byte sequence the old code would mis-classify as a
     * single-line `430` response. The terminator `\r\n.\r\n` is later in the
     * stream; drain must keep reading until then.
     */
    @Test
    fun `drainUntilArticleEnd does not mis-classify body bytes as a 430 response after status line is read`() =
        runTest {
            // Server side: send welcome, accept BODY, respond 222 + body that begins
            // with "430\r\n..." (the bug repro), terminate properly with \r\n.\r\n.
            launch(Dispatchers.IO) {
                acceptAndHandle { reader, writer ->
                    writer.print("200 welcome\r\n")
                    writer.flush()
                    reader.readLine() // BODY <id>
                    // 222 ack, then a yenc-shaped body whose first data line is "430..."
                    // (followed by enough bytes to overrun firstLineProbe's 3-digit check),
                    // then a properly-terminated article ending.
                    writer.print("222 0 <abc@host> body follows\r\n")
                    writer.print("430 not really a status line\r\n")
                    writer.print("=ybegin part=1 line=128 size=10 name=x\r\n")
                    writer.print("=yend size=10 part=1\r\n")
                    writer.print(".\r\n")
                    writer.flush()
                }
            }

            val connection = NntpConnection.connect(
                "localhost", serverSocket.localPort, selectorManager
            )

            // Simulate the fetchBodyYenc handshake: mark in-progress, write BODY,
            // read the 222 status line, mark response-read, then drain to article-end.
            connection.markBodyInProgress()
            val response = connection.commandRaw("BODY <abc@host>")
            assertEquals(222, response.code)
            connection.markBodyResponseRead()

            // Drain MUST consume bytes through the actual `\r\n.\r\n` terminator —
            // it must NOT take the single-line fast-path on the first "430\r\n"
            // line, which is yenc body data after the response.
            val drained = connection.drainUntilArticleEnd()
            assertEquals(true, drained, "drain should report success, not a false 430-fast-path bail-out")
            connection.close()
        }
}
