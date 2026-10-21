package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.util.concurrent.TimeUnit
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

/**
 * A line whose UTF-8 character reaches the connection in two reads, its first byte in one and the rest in the next, is
 * read whole. Before Ktor 3.5.2 (KTOR-9679) its readLine decoded each read's part of a line on its own, so the
 * character came out as two U+FFFD: in a file's name on =ybegin, in a subject in an overview.
 */
@Timeout(30, unit = TimeUnit.SECONDS)
class NntpUtf8LineTest {
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

    /**
     * Greets one connection and answers its command with [first], then, once the client has had time to read that
     * alone, with [rest]: what a line split between two TCP segments looks like to the reader.
     */
    private fun serveSplit(first: ByteArray, rest: ByteArray) {
        Thread {
            runCatching {
                serverSocket.accept().use { socket ->
                    val reader = BufferedReader(InputStreamReader(socket.getInputStream(), Charsets.ISO_8859_1))
                    val out = socket.getOutputStream()
                    out.write("200 welcome\r\n".toByteArray())
                    out.flush()
                    reader.readLine() ?: return@use
                    out.write(first)
                    out.flush()
                    Thread.sleep(SPLIT_PAUSE_MS)
                    out.write(rest)
                    out.flush()
                    reader.readLine()
                }
            }
        }.apply { isDaemon = true }.start()
    }

    private suspend fun client() = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)

    /** [text] in UTF-8, cut after the first byte of its first two-byte character: the line's two parts. */
    private fun splitInside(text: String): Pair<ByteArray, ByteArray> {
        val bytes = text.toByteArray(Charsets.UTF_8)
        val cut = bytes.indexOfFirst { (it.toInt() and 0xE0) == 0xC0 } + 1
        return bytes.copyOfRange(0, cut) to bytes.copyOfRange(cut, bytes.size)
    }

    @Test
    fun `a file name on =ybegin split inside a character is read whole`() = runBlocking<Unit> {
        val data = "Yenc data under a name split in two".toByteArray()
        val crc = RapidYenc.crc32(data).toString(16).padStart(8, '0')
        val (nameStart, nameEnd) = splitInside("Café Nocturne.mkv")
        val encoded = String(RapidYenc.encode(data), Charsets.ISO_8859_1).let { if (it.endsWith("\r\n")) it else "$it\r\n" }
        serveSplit(
            "222 body follows\r\n=ybegin line=128 size=${data.size} name=".toByteArray() + nameStart,
            nameEnd + "\r\n$encoded=yend size=${data.size} crc32=$crc\r\n.\r\n".toByteArray(Charsets.ISO_8859_1),
        )
        client().use { client ->
            val article = client.bodyYencBytes("<split@test>")
            assertEquals("Café Nocturne.mkv", article.headers.name)
            assertContentEquals(data, article.data)
        }
    }

    @Test
    fun `a line of a multi-line answer split inside a character is read whole`() = runBlocking<Unit> {
        val line = "1\tRéunion au port - S01E02\tposter@example.invalid\t20 Oct 2026 10:00:00 +0000\t<split@test>\t\t1024\t8"
        val (lineStart, lineEnd) = splitInside(line)
        serveSplit("224 overview follows\r\n".toByteArray() + lineStart, lineEnd + "\r\n.\r\n".toByteArray())
        client().use { client ->
            assertEquals(listOf(line), client.over(1L..1L))
        }
    }

    private companion object {
        // Long enough for the client's read to take the first part on its own, even on a busy machine.
        const val SPLIT_PAUSE_MS = 300L
    }
}
