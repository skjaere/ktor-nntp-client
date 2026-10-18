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
import kotlin.test.assertNotNull
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

    /**
     * A yEnc article as a server sends it after "222": [ypart] for one part of a larger file; [crc] what
     * =yend claims the body's CRC is (a part's pcrc32, else its crc32; none if null), and [fileCrc] the
     * whole file's, which a part's =yend may give as its crc32.
     */
    private fun article(
        data: ByteArray,
        size: Long = data.size.toLong(),
        ypart: LongRange? = null,
        crc: UInt? = RapidYenc.crc32(data),
        fileCrc: UInt? = null,
    ): ByteArray {
        val encoded = RapidYenc.encode(data)
        fun hex(crc: UInt) = crc.toString(16).padStart(8, '0')
        val text = buildString {
            append("222 body follows\r\n")
            if (ypart == null) append("=ybegin line=128 size=$size name=data.bin\r\n")
            else append("=ybegin part=2 total=9 line=128 size=${size * 9} name=data.bin\r\n=ypart begin=${ypart.first} end=${ypart.last}\r\n")
            append(String(encoded, Charsets.ISO_8859_1))
            if (!endsWith("\r\n")) append("\r\n")
            append("=yend size=${data.size}")
            if (ypart != null) append(" part=2")
            crc?.let { append(if (ypart == null) " crc32=${hex(it)}" else " pcrc32=${hex(it)}") }
            fileCrc?.let { append(" crc32=${hex(it)}") }
            append("\r\n")
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

    /** The article is read to its end first, so the connection can go on. */
    @Test
    fun `a CRC mismatch fails once the article is read to its end`() = runBlocking<Unit> {
        val data = Random(5).nextBytes(10_000)
        val next = "the next article".toByteArray()
        serve({ it.write(article(data, crc = RapidYenc.crc32(data) xor 1u)) }, { it.write(article(next)) })
        client().use { client ->
            assertFailsWith<YencCrcMismatchException> { client.bodyYencBytes("<bad@test>") }
            assertContentEquals(next, client.bodyYencBytes("<next@test>").data)
        }
    }

    /** Read whole, the damaged copy comes with the failure: a caller that has no better one may use it. */
    @Test
    fun `a CRC mismatch carries the article it was read whole`() = runBlocking<Unit> {
        val data = Random(12).nextBytes(300_000)
        serve({ it.write(article(data, crc = RapidYenc.crc32(data) xor 1u)) })
        client().use { client ->
            val e = assertFailsWith<YencCrcMismatchException> { client.bodyYencBytes("<bad@test>") }
            assertEquals(RapidYenc.crc32(data) xor 1u, e.expected)
            assertEquals(RapidYenc.crc32(data), e.actual)
            val article = assertNotNull(e.article, "the mismatch should carry the article")
            assertEquals("data.bin", article.headers.name)
            assertContentEquals(data, article.data)
        }
    }

    @Test
    fun `a part whose pcrc32 doesn't match fails once it is read to its end`() = runBlocking<Unit> {
        val data = Random(8).nextBytes(10_000)
        val next = "the next article".toByteArray()
        serve(
            { it.write(article(data, ypart = 10_001L..20_000L, crc = RapidYenc.crc32(data) xor 1u, fileCrc = RapidYenc.crc32(data))) },
            { it.write(article(next)) },
        )
        client().use { client ->
            val e = assertFailsWith<YencCrcMismatchException> { client.bodyYencBytes("<bad-part@test>") }
            assertEquals(RapidYenc.crc32(data), e.actual)
            assertContentEquals(next, client.bodyYencBytes("<next@test>").data)
        }
    }

    /** A part's =yend may give the whole file's CRC as its crc32: only its pcrc32 is the part's. */
    @Test
    fun `a part is checked against its pcrc32, not the file's crc32`() = runBlocking<Unit> {
        val data = Random(9).nextBytes(10_000)
        val fileCrc = RapidYenc.crc32(data) xor 0x5a5a5a5au
        serve({ it.write(article(data, ypart = 10_001L..20_000L, fileCrc = fileCrc)) }, { it.write(article(data, ypart = 10_001L..20_000L, crc = null, fileCrc = fileCrc)) })
        client().use { client ->
            assertContentEquals(data, client.bodyYencBytes("<part@test>").data)
            assertContentEquals(data, client.bodyYencBytes("<part-without-pcrc32@test>").data)
        }
    }

    @Test
    fun `an article whose =yend gives no CRC is taken as it is`() = runBlocking<Unit> {
        val data = Random(10).nextBytes(10_000)
        serve({ it.write(article(data, crc = null)) })
        client().use { client ->
            assertContentEquals(data, client.bodyYencBytes("<no-crc@test>").data)
            assertTrue(client.connection.isReusable)
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
