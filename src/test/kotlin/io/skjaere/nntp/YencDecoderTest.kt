package io.skjaere.nntp

import io.ktor.network.selector.*
import io.ktor.utils.io.*
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.PrintWriter
import java.net.ServerSocket
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class YencDecoderTest {

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

    private fun buildYencArticle(data: ByteArray, filename: String): ByteArray {
        val encoded = RapidYenc.encode(data)
        val crc = RapidYenc.crc32(data)
        val sb = StringBuilder()
        sb.append("=ybegin line=128 size=${data.size} name=$filename\r\n")
        sb.append(String(encoded, Charsets.ISO_8859_1))
        if (!String(encoded, Charsets.ISO_8859_1).endsWith("\r\n")) {
            sb.append("\r\n")
        }
        sb.append("=yend size=${data.size} crc32=${crc.toString(16).padStart(8, '0')}\r\n")
        sb.append(".\r\n")
        return sb.toString().toByteArray(Charsets.ISO_8859_1)
    }

    @Test
    fun `decode single-part yenc body`() = runTest {
        val originalData = "Hello, World! This is a test of yenc decoding.".toByteArray()

        launch(Dispatchers.IO) {
            val client = serverSocket.accept()
            client.use { socket ->
                val out = socket.getOutputStream()
                out.write("200 welcome\r\n".toByteArray())
                out.flush()

                // Read the BODY command
                val reader = socket.getInputStream().bufferedReader()
                reader.readLine()

                // Send response
                out.write("222 body follows\r\n".toByteArray())
                out.write(buildYencArticle(originalData, "test.txt"))
                out.flush()
            }
        }

        val connection = NntpConnection.connect(
            "localhost", serverSocket.localPort, selectorManager
        )
        val response = connection.commandRaw("BODY <test@msg>")
        assertEquals(222, response.code)

        val decoder = YencDecoder(this)
        val result = decoder.decode(connection, response)

        assertEquals("test.txt", result.yencHeaders.name)
        assertEquals(originalData.size.toLong(), result.yencHeaders.size)

        val decoded = result.data.toByteArray()
        assertContentEquals(originalData, decoded)
    }

    @Test
    fun `decode yenc body with multipart headers`() = runTest {
        val originalData = ByteArray(1000) { (it % 256).toByte() }

        launch(Dispatchers.IO) {
            val client = serverSocket.accept()
            client.use { socket ->
                val out = socket.getOutputStream()
                out.write("200 welcome\r\n".toByteArray())
                out.flush()

                val reader = socket.getInputStream().bufferedReader()
                reader.readLine()

                val encoded = RapidYenc.encode(originalData)
                val crc = RapidYenc.crc32(originalData)

                val body = buildString {
                    append("222 body follows\r\n")
                    append("=ybegin part=1 total=3 line=128 size=3000 name=archive.rar\r\n")
                    append("=ypart begin=1 end=1000\r\n")
                    append(String(encoded, Charsets.ISO_8859_1))
                    if (!String(encoded, Charsets.ISO_8859_1).endsWith("\r\n")) {
                        append("\r\n")
                    }
                    append("=yend size=${originalData.size} pcrc32=${crc.toString(16).padStart(8, '0')}\r\n")
                    append(".\r\n")
                }
                out.write(body.toByteArray(Charsets.ISO_8859_1))
                out.flush()
            }
        }

        val connection = NntpConnection.connect(
            "localhost", serverSocket.localPort, selectorManager
        )
        val response = connection.commandRaw("BODY <part1@msg>")

        val decoder = YencDecoder(this)
        val result = decoder.decode(connection, response)

        assertEquals("archive.rar", result.yencHeaders.name)
        assertEquals(1, result.yencHeaders.part)
        assertEquals(3, result.yencHeaders.total)
        assertEquals(1L, result.yencHeaders.partBegin)
        assertEquals(1000L, result.yencHeaders.partEnd)

        val decoded = result.data.toByteArray()
        assertContentEquals(originalData, decoded)
    }

    @Test
    fun `decode large yenc body across multiple chunks`() = runTest {
        // Use data larger than the 16KB buffer to test multi-chunk reading
        val originalData = ByteArray(50000) { (it % 256).toByte() }

        launch(Dispatchers.IO) {
            val client = serverSocket.accept()
            client.use { socket ->
                val out = socket.getOutputStream()
                out.write("200 welcome\r\n".toByteArray())
                out.flush()

                val reader = socket.getInputStream().bufferedReader()
                reader.readLine()

                out.write("222 body follows\r\n".toByteArray())
                out.write(buildYencArticle(originalData, "large.bin"))
                out.flush()
            }
        }

        val connection = NntpConnection.connect(
            "localhost", serverSocket.localPort, selectorManager
        )
        val response = connection.commandRaw("BODY <large@msg>")

        val decoder = YencDecoder(this)
        val result = decoder.decode(connection, response)

        assertEquals(50000L, result.yencHeaders.size)

        val decoded = result.data.toByteArray()
        assertContentEquals(originalData, decoded)
    }
}
