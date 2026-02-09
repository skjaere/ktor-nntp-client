package io.skjaere.nntp

import io.ktor.network.selector.*
import io.ktor.utils.io.*
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class NntpClientTest {

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

    private fun launchServer(handler: (BufferedReader, java.io.OutputStream) -> Unit) {
        Thread {
            val client = serverSocket.accept()
            client.use { socket ->
                val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                val out = socket.getOutputStream()
                out.write("200 welcome\r\n".toByteArray())
                out.flush()
                handler(reader, out)
            }
        }.start()
    }

    @Test
    fun `connect returns client with welcome response`() = runTest {
        launchServer { _, _ -> }
        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        assertEquals(200, client.connection.welcomeResponse.code)
        client.close()
    }

    @Test
    fun `group command parses response correctly`() = runTest {
        launchServer { reader, out ->
            val cmd = reader.readLine()
            assertTrue(cmd.startsWith("GROUP"))
            out.write("211 5 1 5 test.group\r\n".toByteArray())
            out.flush()
        }

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        val response = client.group("test.group")
        assertEquals(211, response.code)
        assertEquals(5L, response.count)
        assertEquals(1L, response.low)
        assertEquals(5L, response.high)
        assertEquals("test.group", response.name)
        client.close()
    }

    @Test
    fun `stat command parses response correctly`() = runTest {
        launchServer { reader, out ->
            reader.readLine()
            out.write("223 42 <msg@host> article exists\r\n".toByteArray())
            out.flush()
        }

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        val response = client.stat("<msg@host>")
        assertEquals(223, response.code)
        assertEquals(42L, response.articleNumber)
        assertEquals("<msg@host>", response.messageId)
        client.close()
    }

    @Test
    fun `article command retrieves full article`() = runTest {
        launchServer { reader, out ->
            reader.readLine()
            out.write("220 1 <msg@host> article follows\r\n".toByteArray())
            out.write("Subject: Test Article\r\n".toByteArray())
            out.write("From: test@example.com\r\n".toByteArray())
            out.write("\r\n".toByteArray())
            out.write("Article body line 1\r\n".toByteArray())
            out.write("Article body line 2\r\n".toByteArray())
            out.write(".\r\n".toByteArray())
            out.flush()
        }

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        val response = client.article("<msg@host>")
        assertEquals(220, response.code)
        assertEquals(1L, response.articleNumber)
        assertEquals("<msg@host>", response.messageId)
        assertEquals(5, response.content.size)
        assertEquals("Subject: Test Article", response.content[0])
        assertEquals("Article body line 2", response.content[4])
        client.close()
    }

    @Test
    fun `body command retrieves body only`() = runTest {
        launchServer { reader, out ->
            reader.readLine()
            out.write("222 1 <msg@host> body follows\r\n".toByteArray())
            out.write("Body line 1\r\n".toByteArray())
            out.write("Body line 2\r\n".toByteArray())
            out.write(".\r\n".toByteArray())
            out.flush()
        }

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        val response = client.body("<msg@host>")
        assertEquals(222, response.code)
        assertEquals(listOf("Body line 1", "Body line 2"), response.content)
        client.close()
    }

    @Test
    fun `head command retrieves headers only`() = runTest {
        launchServer { reader, out ->
            reader.readLine()
            out.write("221 1 <msg@host> head follows\r\n".toByteArray())
            out.write("Subject: Test\r\n".toByteArray())
            out.write("From: test@example.com\r\n".toByteArray())
            out.write(".\r\n".toByteArray())
            out.flush()
        }

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        val response = client.head("<msg@host>")
        assertEquals(221, response.code)
        assertEquals(listOf("Subject: Test", "From: test@example.com"), response.content)
        client.close()
    }

    @Test
    fun `help command returns command list`() = runTest {
        launchServer { reader, out ->
            reader.readLine()
            out.write("100 help follows\r\n".toByteArray())
            out.write("ARTICLE\r\n".toByteArray())
            out.write("BODY\r\n".toByteArray())
            out.write(".\r\n".toByteArray())
            out.flush()
        }

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        val lines = client.help()
        assertEquals(listOf("ARTICLE", "BODY"), lines)
        client.close()
    }

    @Test
    fun `date command returns server date`() = runTest {
        launchServer { reader, out ->
            reader.readLine()
            out.write("111 20260208120000\r\n".toByteArray())
            out.flush()
        }

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        val response = client.date()
        assertEquals(111, response.code)
        assertEquals("20260208120000", response.message)
        client.close()
    }

    @Test
    fun `quit command returns response`() = runTest {
        launchServer { reader, out ->
            reader.readLine()
            out.write("205 closing connection\r\n".toByteArray())
            out.flush()
        }

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        val response = client.quit()
        assertEquals(205, response.code)
        client.close()
    }

    @Test
    fun `authenticate with user and pass`() = runTest {
        launchServer { reader, out ->
            val userCmd = reader.readLine()
            assertTrue(userCmd.startsWith("AUTHINFO USER"))
            out.write("381 Password required\r\n".toByteArray())
            out.flush()

            val passCmd = reader.readLine()
            assertTrue(passCmd.startsWith("AUTHINFO PASS"))
            out.write("281 Authentication accepted\r\n".toByteArray())
            out.flush()
        }

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        val response = client.authenticate("user", "pass")
        assertEquals(281, response.code)
        client.close()
    }

    @Test
    fun `capabilities returns capability list`() = runTest {
        launchServer { reader, out ->
            reader.readLine()
            out.write("101 Capability list:\r\n".toByteArray())
            out.write("VERSION 2\r\n".toByteArray())
            out.write("READER\r\n".toByteArray())
            out.write("POST\r\n".toByteArray())
            out.write(".\r\n".toByteArray())
            out.flush()
        }

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        val caps = client.capabilities()
        assertEquals(listOf("VERSION 2", "READER", "POST"), caps)
        client.close()
    }

    @Test
    fun `bodyYenc decodes yenc body correctly`() = runTest {
        val originalData = "Yenc test data for NntpClient integration test!".toByteArray()
        val encoded = RapidYenc.encode(originalData)
        val crc = RapidYenc.crc32(originalData)

        launchServer { reader, out ->
            reader.readLine()
            out.write("222 body follows\r\n".toByteArray())
            out.write("=ybegin line=128 size=${originalData.size} name=yenctest.bin\r\n".toByteArray())
            out.write(encoded)
            if (!String(encoded, Charsets.ISO_8859_1).endsWith("\r\n")) {
                out.write("\r\n".toByteArray())
            }
            out.write("=yend size=${originalData.size} crc32=${crc.toString(16).padStart(8, '0')}\r\n".toByteArray())
            out.write(".\r\n".toByteArray())
            out.flush()
        }

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        val result = client.bodyYenc("<yenc@msg>")

        assertEquals("yenctest.bin", result.yencHeaders.name)
        assertEquals(originalData.size.toLong(), result.yencHeaders.size)

        val decoded = result.data.toByteArray()
        assertContentEquals(originalData, decoded)
        client.close()
    }

    @Test
    fun `bodyYenc with cancellation triggers reconnect`() = runBlocking {
        // Send partial yenc data so writer is still blocking when we cancel
        val partialData = ByteArray(1000) { (it % 256).toByte() }
        val partialEncoded = RapidYenc.encode(partialData)

        val multiServerSocket = ServerSocket(0)
        try {
            var connectionCount = 0
            val serverThread = Thread {
                while (connectionCount < 2) {
                    try {
                        val client = multiServerSocket.accept()
                        connectionCount++
                        val connNum = connectionCount
                        Thread {
                            client.use { socket ->
                                val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                                val out = socket.getOutputStream()
                                out.write("200 welcome\r\n".toByteArray())
                                out.flush()

                                if (connNum == 1) {
                                    // First connection: send headers + partial data, then STOP
                                    // (don't send =yend or .\r\n so writer blocks waiting for more)
                                    reader.readLine()
                                    out.write("222 body follows\r\n".toByteArray())
                                    out.write("=ybegin line=128 size=99999 name=large.bin\r\n".toByteArray())
                                    out.write(partialEncoded)
                                    out.flush()
                                    // Block until client disconnects
                                    try {
                                        Thread.sleep(30000)
                                    } catch (_: Exception) {
                                    }
                                } else {
                                    // Second connection (after reconnect): serve GROUP
                                    try {
                                        val cmd = reader.readLine()
                                        if (cmd != null && cmd.startsWith("GROUP")) {
                                            out.write("211 5 1 5 test.group\r\n".toByteArray())
                                            out.flush()
                                        }
                                    } catch (_: Exception) {
                                    }
                                }
                            }
                        }.start()
                    } catch (_: Exception) {
                        break
                    }
                }
            }
            serverThread.start()

            val client = NntpClient.connect("localhost", multiServerSocket.localPort, selectorManager)

            // Start bodyYenc - writer will block after reading partial data
            val result = client.bodyYenc("<large@msg>")

            // Read some decoded bytes
            val buf = ByteArray(100)
            result.data.readAvailable(buf)

            // Cancel the channel - writer coroutine will be cancelled
            // and scheduleReconnect() should be called in the finally block
            result.data.cancel()

            // Wait for reconnection (real I/O time)
            delay(2000)

            // Now try a command - should work after reconnection
            val groupResponse = client.group("test.group")
            assertEquals(211, groupResponse.code)
            assertEquals("test.group", groupResponse.name)

            client.close()
        } finally {
            multiServerSocket.close()
        }
    }

    @Test
    fun `concurrent bodyYenc on multiple connections`() = runTest {
        val originalData = "Concurrent test data".toByteArray()
        val encoded = RapidYenc.encode(originalData)
        val crc = RapidYenc.crc32(originalData)

        val servers = (1..3).map { ServerSocket(0) }
        val serverThreads = servers.map { server ->
            Thread {
                val client = server.accept()
                client.use { socket ->
                    val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                    val out = socket.getOutputStream()
                    out.write("200 welcome\r\n".toByteArray())
                    out.flush()

                    reader.readLine()
                    out.write("222 body follows\r\n".toByteArray())
                    out.write("=ybegin line=128 size=${originalData.size} name=concurrent.bin\r\n".toByteArray())
                    out.write(encoded)
                    if (!String(encoded, Charsets.ISO_8859_1).endsWith("\r\n")) {
                        out.write("\r\n".toByteArray())
                    }
                    out.write("=yend size=${originalData.size} crc32=${crc.toString(16).padStart(8, '0')}\r\n".toByteArray())
                    out.write(".\r\n".toByteArray())
                    out.flush()
                }
            }.also { it.start() }
        }

        try {
            val clients = servers.map { server ->
                NntpClient.connect("localhost", server.localPort, selectorManager)
            }

            // All three connections decode concurrently
            val results = coroutineScope {
                clients.map { client ->
                    async {
                        val result = client.bodyYenc("<concurrent@msg>")
                        result.data.toByteArray()
                    }
                }.map { it.await() }
            }

            results.forEach { decoded ->
                assertContentEquals(originalData, decoded)
            }

            clients.forEach { it.close() }
        } finally {
            servers.forEach { it.close() }
        }
    }

    @Test
    fun `listGroup returns article numbers`() = runTest {
        launchServer { reader, out ->
            reader.readLine()
            out.write("211 3 1 3 test.group\r\n".toByteArray())
            out.write("1\r\n".toByteArray())
            out.write("2\r\n".toByteArray())
            out.write("3\r\n".toByteArray())
            out.write(".\r\n".toByteArray())
            out.flush()
        }

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        val response = client.listGroup("test.group")
        assertEquals(3L, response.count)
        assertEquals(listOf(1L, 2L, 3L), response.articleNumbers)
        client.close()
    }

    @Test
    fun `group command throws on error response`() = runTest {
        launchServer { reader, out ->
            reader.readLine()
            out.write("411 No such group\r\n".toByteArray())
            out.flush()
        }

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        assertFailsWith<NntpProtocolException> {
            client.group("nonexistent.group")
        }
        client.close()
    }
}
