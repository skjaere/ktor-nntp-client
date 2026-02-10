package io.skjaere.nntp

import io.ktor.network.selector.*
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class NntpClientPoolTest {

    private lateinit var serverSocket: ServerSocket
    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope

    @BeforeEach
    fun setUp() {
        serverSocket = ServerSocket(0)
        selectorManager = SelectorManager(Dispatchers.IO)
        poolScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
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

    private fun launchMultiConnectionServer(count: Int, articles: List<Pair<ByteArray, String>>) {
        Thread {
            repeat(count) { i ->
                try {
                    val client = serverSocket.accept()
                    Thread {
                        client.use { socket ->
                            val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                            val out = socket.getOutputStream()
                            out.write("200 welcome\r\n".toByteArray())
                            out.flush()
                            try {
                                val cmd = reader.readLine() ?: return@Thread
                                if (cmd.startsWith("BODY")) {
                                    val (data, filename) = articles[i]
                                    out.write("222 body follows\r\n".toByteArray())
                                    out.write(buildYencArticle(data, filename))
                                    out.flush()
                                }
                                Thread.sleep(5000)
                            } catch (_: Exception) {
                            }
                        }
                    }.start()
                } catch (_: Exception) {
                }
            }
        }.start()
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `bodyYencHeaders unlocks mutex - raw client, no supervisorScope`() = runBlocking {
        val data = "Test data".toByteArray()
        launchMultiConnectionServer(1, listOf(data to "test.bin"))

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        try {
            val headers = client.bodyYencHeaders("<test@msg>")
            assertEquals("test.bin", headers.name)

            delay(500) // let async cleanup finish
            assertFalse(client.connection.commandMutex.isLocked, "Mutex should be unlocked (raw, no supervisor)")
        } finally {
            client.close()
        }
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `bodyYencHeaders unlocks mutex - raw client, inside supervisorScope`() = runBlocking {
        val data = "Test data".toByteArray()
        launchMultiConnectionServer(1, listOf(data to "test.bin"))

        val client = NntpClient.connect("localhost", serverSocket.localPort, selectorManager)
        try {
            supervisorScope {
                val headers = client.bodyYencHeaders("<test@msg>")
                assertEquals("test.bin", headers.name)
            }

            delay(500)
            assertFalse(client.connection.commandMutex.isLocked, "Mutex should be unlocked (raw, inside supervisor)")
        } finally {
            client.close()
        }
    }

    @Test
    @Timeout(10, unit = TimeUnit.SECONDS)
    fun `bodyYencHeaders twice through pool does not deadlock`() = runBlocking {
        val data1 = "First article data".toByteArray()
        val data2 = "Second article data".toByteArray()

        launchMultiConnectionServer(2, listOf(
            data1 to "first.bin",
            data2 to "second.bin"
        ))

        val pool = NntpClientPool(
            host = "localhost",
            port = serverSocket.localPort,
            selectorManager = selectorManager,
            maxConnections = 1,
            scope = poolScope
        )
        pool.connect()

        try {
            val headers1 = pool.bodyYencHeaders("<article-1@test>")
            assertEquals("first.bin", headers1.name)

            val headers2 = pool.bodyYencHeaders("<article-2@test>")
            assertEquals("second.bin", headers2.name)
        } finally {
            pool.close()
        }
    }
}
