package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.OutputStream
import java.lang.management.ManagementFactory
import java.net.ServerSocket
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * [NntpClient.bodyYencBytes]: an article decoded whole into one array. Streamed through the Body
 * channel and read back with toByteArray, a 3.84 MB segment of a 4K remux was copied into the
 * channel's buffers and again into an array: on a TV box's heap, eight at once kept the collector
 * running and playback hitched.
 */
class NntpWholeBodyTest {
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

    /** Answers each command [answers] gives, in order, on one connection; then waits for the client to hang up. */
    private fun serve(vararg answers: (OutputStream) -> Unit) {
        Thread {
            serverSocket.accept().use { socket ->
                val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                val out = socket.getOutputStream()
                out.write("200 welcome\r\n".toByteArray())
                out.flush()
                answers.forEach { answer ->
                    reader.readLine() ?: return@use
                    answer(out)
                    out.flush()
                }
                reader.readLine()
            }
        }.start()
    }

    /** A yEnc article as a server sends it after "222": [ypart] for one part of a larger file; [crc] what =yend claims. */
    private fun article(data: ByteArray, size: Long = data.size.toLong(), ypart: LongRange? = null, crc: UInt = RapidYenc.crc32(data)): ByteArray {
        val encoded = RapidYenc.encode(data)
        val text = buildString {
            append("222 body follows\r\n")
            if (ypart == null) append("=ybegin line=128 size=$size name=data.bin\r\n")
            else append("=ybegin part=2 total=9 line=128 size=${size * 9} name=data.bin\r\n=ypart begin=${ypart.first} end=${ypart.last}\r\n")
            append(String(encoded, Charsets.ISO_8859_1))
            if (!endsWith("\r\n")) append("\r\n")
            val crcName = if (ypart == null) "crc32" else "pcrc32"
            append("=yend size=${data.size} $crcName=${crc.toString(16).padStart(8, '0')}\r\n")
            append(".\r\n")
        }
        return text.toByteArray(Charsets.ISO_8859_1)
    }

    private suspend fun client() = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)

    @Test
    fun `an article is read whole, with its headers`() = runBlocking<Unit> {
        val data = "Yenc test data for a whole article".toByteArray()
        serve({ it.write(article(data)) })
        client().use { client ->
            val article = client.bodyYencBytes("<whole@test>")
            assertEquals("data.bin", article.headers.name)
            assertEquals(data.size.toLong(), article.headers.size)
            assertContentEquals(data, article.data)
            assertTrue(client.connection.isReusable)
        }
    }

    @Test
    fun `a part larger than the decoder's buffers is read whole`() = runBlocking<Unit> {
        val data = Random(1).nextBytes(1_300_000)
        serve({ it.write(article(data, ypart = 3_900_001L..5_200_000L)) })
        client().use { client ->
            val article = client.bodyYencBytes("<part@test>")
            assertEquals(3_900_001L, article.headers.partBegin)
            assertContentEquals(data, article.data)
        }
    }

    /** The headers size the array; what the body holds is what comes back, if they misstate it. */
    @Test
    fun `a body longer or shorter than its headers say comes back as it is`() = runBlocking<Unit> {
        val longer = Random(2).nextBytes(400_000)
        val shorter = Random(3).nextBytes(1_000)
        serve({ it.write(article(longer, size = 1_000)) }, { it.write(article(shorter, size = 400_000)) })
        client().use { client ->
            assertContentEquals(longer, client.bodyYencBytes("<longer@test>").data)
            assertContentEquals(shorter, client.bodyYencBytes("<shorter@test>").data)
        }
    }

    @Test
    fun `a missing article fails as not found and leaves the connection in step`() = runBlocking<Unit> {
        val data = "after the 430".toByteArray()
        serve({ it.write("430 No Such Article\r\n".toByteArray()) }, { it.write(article(data)) })
        client().use { client ->
            val e = assertFailsWith<ArticleNotFoundException> { client.bodyYencBytes("<missing@test>") }
            assertEquals(430, e.response?.code)
            assertTrue(client.connection.isReusable)
            assertContentEquals(data, client.bodyYencBytes("<next@test>").data)
        }
    }

    @Test
    fun `a connection closed mid-article fails as a connection failure and isn't reused`() = runBlocking<Unit> {
        val data = Random(4).nextBytes(300_000)
        val whole = article(data)
        serve({ out -> out.write(whole, 0, whole.size / 2); out.flush(); serverSocket.close(); throw java.io.IOException("hung up") })
        client().use { client ->
            assertFailsWith<NntpConnectionException> { client.bodyYencBytes("<cut@test>") }
            assertFalse(client.connection.isReusable)
        }
    }

    /**
     * Decoded straight into the array its headers size: about one body's worth allocated on the
     * calling thread, with the decoder's buffers, and no channel's buffers to copy it out of.
     */
    @Test
    fun `an article is decoded into one array`() = runBlocking<Unit> {
        val data = Random(6).nextBytes(4_000_000)
        val warmUp = Random(7).nextBytes(4_000_000)
        serve({ it.write(article(warmUp)) }, { it.write(article(data)) })
        val threads = ManagementFactory.getThreadMXBean() as com.sun.management.ThreadMXBean
        client().use { client ->
            // The first body loads and compiles what decoding needs: only the second is counted.
            client.bodyYencBytes("<warm-up@test>")
            val before = threads.getThreadAllocatedBytes(Thread.currentThread().id)
            val article = client.bodyYencBytes("<big@test>")
            val allocated = threads.getThreadAllocatedBytes(Thread.currentThread().id) - before
            assertContentEquals(data, article.data)
            assertTrue(allocated < data.size * 3 / 2, "$allocated bytes allocated for a ${data.size}-byte body")
        }
    }
}
