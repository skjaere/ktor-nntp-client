package io.skjaere.nntp

import io.skjaere.nntp.IdleSilenceServer.OnIdle
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.Socket
import java.net.SocketException
import java.net.SocketTimeoutException
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The fake the dead-connection tests rest on does what its KDoc says, seen from a plain socket. */
@Timeout(20, unit = TimeUnit.SECONDS)
class IdleSilenceServerTest {

    /** A plain client: sends a line, reads one back, or times out after [ANSWER_WAIT_MS]. */
    private class Client(port: Int) : AutoCloseable {
        val socket = Socket("localhost", port).apply { soTimeout = ANSWER_WAIT_MS }
        private val reader = BufferedReader(InputStreamReader(socket.getInputStream(), Charsets.ISO_8859_1))
        val greeting: String? = reader.readLine()

        fun send(line: String): String? {
            socket.getOutputStream().apply { write("$line\r\n".toByteArray()); flush() }
            return reader.readLine()
        }

        fun read(): String? = reader.readLine()

        override fun close() = socket.close()
    }

    @Test
    fun `a connection idle past the threshold goes silent for good and stays open`() {
        IdleSilenceServer(idleAfterMs = IDLE_MS).use { server ->
            Client(server.port).use { client ->
                assertEquals("200 welcome", client.greeting)
                assertTrue(client.send("DATE")!!.startsWith("111 "))
                Thread.sleep(IDLE_MS + 100)

                // No answer, and no close either: the read waits out its timeout instead of seeing the end.
                assertFailsWith<SocketTimeoutException> { client.send("DATE") }
                assertFailsWith<SocketTimeoutException> { client.send("STAT <a@b>") }
                assertFalse(client.socket.isClosed)
                assertEquals(1, server.silenced.get())
                assertEquals(2, server.dropped.get())
            }
        }
    }

    @Test
    fun `answers keep a connection alive, a DATE's as any other`() {
        IdleSilenceServer(idleAfterMs = IDLE_MS).use { server ->
            Client(server.port).use { client ->
                repeat(8) {
                    Thread.sleep(IDLE_MS / 3)
                    assertTrue(client.send("DATE")!!.startsWith("111 "), "DATE $it")
                }
                assertEquals("223 0 <a@b> article exists", client.send("STAT <a@b>"))
                assertEquals(0, server.silenced.get())
            }
        }
    }

    @Test
    fun `an idle connection closed with RST resets the next read`() {
        IdleSilenceServer(idleAfterMs = IDLE_MS, onIdle = OnIdle.RST).use { server ->
            Client(server.port).use { client ->
                Thread.sleep(IDLE_MS + 100)
                val failure = assertFailsWith<SocketException> { client.send("DATE") }
                assertTrue("reset" in failure.message.orEmpty().lowercase(), "not a reset: $failure")
                assertEquals(1, server.silenced.get())
            }
        }
    }

    @Test
    fun `an idle connection closed with FIN ends the stream`() {
        IdleSilenceServer(idleAfterMs = IDLE_MS, onIdle = OnIdle.FIN).use { server ->
            Client(server.port).use { client ->
                Thread.sleep(IDLE_MS + 100)
                assertNull(client.send("DATE"))
                assertEquals(1, server.silenced.get())
            }
        }
    }

    @Test
    fun `an idle connection kicked gets a 400 and then the stream's end`() {
        IdleSilenceServer(idleAfterMs = IDLE_MS, onIdle = OnIdle.KICK).use { server ->
            Client(server.port).use { client ->
                Thread.sleep(IDLE_MS + 100)
                assertEquals("400 idle timeout", client.send("DATE"))
                assertNull(client.read())
                assertEquals(1, server.silenced.get())
            }
        }
    }

    @Test
    fun `only K of every M connections go dead`() {
        IdleSilenceServer(idleAfterMs = IDLE_MS, affected = 1, outOf = 2).use { server ->
            val clients = List(4) { Client(server.port) }
            try {
                Thread.sleep(IDLE_MS + 100)
                val answered = clients.map { client -> runCatching { client.send("DATE") }.getOrNull() != null }
                // Accept order is connect order here: each client connected after the last one's greeting.
                assertEquals(listOf(false, true, false, true), answered)
                assertEquals(4, server.accepted.get())
                assertEquals(2, server.silenced.get())
            } finally {
                clients.forEach { it.close() }
            }
        }
    }

    @Test
    fun `a BODY is answered 430, or never when bodies are off, and every command is noted`() {
        IdleSilenceServer().use { server ->
            Client(server.port).use { client ->
                assertEquals("430 no such article", client.send("BODY <a@b>"))
            }
            assertEquals(listOf(0 to "BODY <a@b>"), server.commands.map { it.connection to it.line })
        }
        IdleSilenceServer(answerBody = false).use { server ->
            Client(server.port).use { client ->
                assertFailsWith<SocketTimeoutException> { client.send("BODY <a@b>") }
                assertEquals(0, server.silenced.get())
            }
        }
    }

    private companion object {
        const val IDLE_MS = 300L
        const val ANSWER_WAIT_MS = 500
    }
}
