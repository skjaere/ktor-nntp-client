package io.skjaere.nntp

import io.ktor.network.selector.*
import io.ktor.utils.io.*
import io.skjaere.mocknntp.testcontainer.MockNntpServerContainer
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
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

    @BeforeEach
    fun setUp() = runBlocking {
        selectorManager = SelectorManager(Dispatchers.IO)
        mockServer.client.clearExpectations()
    }

    @AfterEach
    fun tearDown() {
        selectorManager.close()
    }

    @Test
    fun `bodyYencHeaders returns correct headers for single-part article`() = runBlocking {
        val originalData = "Hello, this is test content for yenc headers!".toByteArray()
        mockServer.client.addYencBodyExpectation(
            articleId = "<single-part@test>",
            data = originalData,
            filename = "testfile.bin"
        )

        val client = NntpClient.connect(mockServer.nntpHost, mockServer.nntpPort, selectorManager)
        client.use { client ->
            val headers = client.bodyYencHeaders("<single-part@test>")

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

        val client = NntpClient.connect(mockServer.nntpHost, mockServer.nntpPort, selectorManager)
        client.use { client ->
            val headers = client.bodyYencHeaders("<multipart@test>")

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
    fun `bodyYencHeaders allows subsequent commands after reconnect`() = runBlocking {
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

        val client = NntpClient.connect(mockServer.nntpHost, mockServer.nntpPort, selectorManager)
        client.use { client ->
            val headers1 = client.bodyYencHeaders("<article-1@test>")
            assertEquals("first.bin", headers1.name)

            // Wait for background reconnect to complete
            delay(2000)

            val headers2 = client.bodyYencHeaders("<article-2@test>")
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

        val client = NntpClient.connect(mockServer.nntpHost, mockServer.nntpPort, selectorManager)
        client.use { client ->
            // First: just read headers
            val headers = client.bodyYencHeaders("<headers-only@test>")
            assertEquals("headers.bin", headers.name)

            // Wait for background reconnect
            delay(2000)

            // Then: full yenc download on a different article
            val result = client.bodyYenc("<full-download@test>")
            assertEquals("download.bin", result.yencHeaders.name)

            val decoded = result.data.toByteArray()
            assertContentEquals(downloadData, decoded)
        }
    }
}
