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
}
