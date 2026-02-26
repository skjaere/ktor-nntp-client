package io.skjaere.nntp

import io.ktor.network.selector.*
import io.ktor.utils.io.*
import io.skjaere.mocknntp.testcontainer.MockNntpServerContainer
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull

@Testcontainers
class NntpClientIntegrationTest {

    companion object {
        @Container
        @JvmStatic
        val mockServer = MockNntpServerContainer()
    }

    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope

    @BeforeEach
    fun setUp() = runBlocking {
        selectorManager = SelectorManager(Dispatchers.IO)
        poolScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        mockServer.client.clearExpectations()
    }

    @AfterEach
    fun tearDown() {
        selectorManager.close()
    }

    private suspend fun createPool(maxConnections: Int = 1): NntpClientPool {
        val pool = NntpClientPool(
            host = mockServer.nntpHost,
            port = mockServer.nntpPort,
            selectorManager = selectorManager,
            maxConnections = maxConnections,
            scope = poolScope,
            /*username = "username",
            password = "password"*/
        )
        pool.wake()
        return pool
    }

    @Test
    fun `bodyYencHeaders returns correct headers for single-part article`() = runBlocking {
        val originalData = "Hello, this is test content for yenc headers!".toByteArray()
        mockServer.client.addYencBodyExpectation(
            articleId = "<single-part@test>",
            data = originalData,
            filename = "testfile.bin"
        )

        val pool = createPool()
        pool.use {
            val headers = pool.bodyYencHeaders("<single-part@test>")

            assertEquals("testfile.bin", headers.name)
            assertEquals(originalData.size.toLong(), headers.size)
            assertEquals(128, headers.line)
            assertNull(headers.part)
            assertNull(headers.total)
            assertNull(headers.partBegin)
            assertNull(headers.partEnd)
        }
    }

    @Test
    fun `bodyYencHeaders with multipart article returns part info`() = runBlocking {
        val partData = ByteArray(1000) { (it % 256).toByte() }
        val encoded = RapidYenc.encode(partData)
        val crc = RapidYenc.crc32(partData)

        val rawYenc = buildString {
            append("=ybegin part=2 total=5 line=128 size=5000 name=archive.rar\r\n")
            append("=ypart begin=1001 end=2000\r\n")
            append(String(encoded, Charsets.ISO_8859_1))
            if (!String(encoded, Charsets.ISO_8859_1).endsWith("\r\n")) {
                append("\r\n")
            }
            append("=yend size=${partData.size} pcrc32=${crc.toString(16).padStart(8, '0')}\r\n")
        }.toByteArray(Charsets.ISO_8859_1)

        mockServer.client.addRawYencBodyExpectation("<multipart@test>", rawYenc)

        val pool = createPool()
        pool.use {
            val headers = pool.bodyYencHeaders("<multipart@test>")

            assertEquals("archive.rar", headers.name)
            assertEquals(5000L, headers.size)
            assertEquals(128, headers.line)
            assertEquals(2, headers.part)
            assertEquals(5, headers.total)
            assertEquals(1001L, headers.partBegin)
            assertEquals(2000L, headers.partEnd)
        }
    }

    @Test
    fun `bodyYencHeaders allows subsequent commands without manual delay`() = runBlocking {
        val data1 = "First article data".toByteArray()
        val data2 = "Second article data".toByteArray()
        mockServer.client.addYencBodyExpectation(
            articleId = "<article-1@test>",
            data = data1,
            filename = "first.bin"
        )
        mockServer.client.addYencBodyExpectation(
            articleId = "<article-2@test>",
            data = data2,
            filename = "second.bin"
        )

        val pool = createPool()
        pool.use {
            val headers1 = pool.bodyYencHeaders("<article-1@test>")
            assertEquals("first.bin", headers1.name)

            // No delay needed — pool waits for reconnection before reusing the client
            val headers2 = pool.bodyYencHeaders("<article-2@test>")
            assertEquals("second.bin", headers2.name)
            assertEquals(data2.size.toLong(), headers2.size)
        }
    }

    @Test
    fun `bodyYencHeaders then bodyYenc on different article`() = runBlocking {
        val headersData = "Data for headers check".toByteArray()
        val downloadData = "Data for full download".toByteArray()
        mockServer.client.addYencBodyExpectation(
            articleId = "<headers-only@test>",
            data = headersData,
            filename = "headers.bin"
        )
        mockServer.client.addYencBodyExpectation(
            articleId = "<full-download@test>",
            data = downloadData,
            filename = "download.bin"
        )

        val pool = createPool()
        pool.use {
            // First: just read headers (triggers reconnect internally)
            val headers = it.bodyYencHeaders("<headers-only@test>")
            assertEquals("headers.bin", headers.name)

            // No delay needed — pool handles reconnection transparently
            var yencHeaders: YencHeaders? = null
            var decoded: ByteArray? = null
            it.bodyYenc("<full-download@test>").collect { event ->
                when (event) {
                    is YencEvent.Headers -> yencHeaders = event.yencHeaders
                    is YencEvent.Body -> decoded = event.data.toByteArray()
                }
            }
            assertEquals("download.bin", yencHeaders!!.name)
            assertContentEquals(downloadData, decoded)
        }
    }
}
