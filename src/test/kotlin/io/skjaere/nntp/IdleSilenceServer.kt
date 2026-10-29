package io.skjaere.nntp

import java.io.BufferedReader
import java.io.Closeable
import java.io.InputStreamReader
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * A fake NNTP server whose connections die after sitting idle, as a NAT, a VPN or a provider ends a flow nobody used
 * for a while. Each connection notes when its last answer ended (the greeting's, at first). A command that comes once
 * the connection has been idle [idleAfterMs] or longer finds it dead for good, in the way [onIdle] says. Any answer
 * keeps a connection alive, a DATE's as any other, as a keepalive refreshes a NAT's entry. Null [idleAfterMs]: none
 * die.
 *
 * Only [affected] of every [outOf] connections can die, by accept order from 0 (K of M): connection i when
 * `i % outOf < affected`.
 *
 * Answers STAT (found), DATE (111), QUIT, and BODY with 430, or never with [answerBody] false (a BODY the server sits
 * on); anything else with 500. On loopback, as through a NAT that dropped the flow, a silent connection's writes are
 * still taken: the client sees no answer, not an error.
 */
internal class IdleSilenceServer(
    private val idleAfterMs: Long? = null,
    private val onIdle: OnIdle = OnIdle.SILENT,
    private val affected: Int = 1,
    private val outOf: Int = 1,
    private val answerBody: Boolean = true,
) : Closeable {

    /** How a connection dies once idle. */
    enum class OnIdle {
        /** Reads and drops whatever comes, never writes, never closes. */
        SILENT,

        /** Closes with RST (SO_LINGER 0): the client's next read fails with a reset. */
        RST,

        /** Closes with FIN: the client's next read finds the stream's end. */
        FIN,

        /** Writes "400 idle timeout", then closes with FIN: the client reads the 400 as its command's answer. */
        KICK,
    }

    /** A command as read: on which connection (accept order, from 0), and when ([System.nanoTime]). */
    data class Command(val connection: Int, val line: String, val atNanos: Long)

    private val serverSocket = ServerSocket(0)
    private val sockets = CopyOnWriteArrayList<Socket>()

    val port: Int get() = serverSocket.localPort

    /** Connections accepted. */
    val accepted = AtomicInteger()

    /** Connections the idle rule ended: gone silent, or closed. */
    val silenced = AtomicInteger()

    /** Commands read and dropped on connections gone silent. */
    val dropped = AtomicInteger()

    /** Every command read, in the order read. */
    val commands = CopyOnWriteArrayList<Command>()

    init {
        Thread {
            while (true) {
                val socket = runCatching { serverSocket.accept() }.getOrNull() ?: break
                sockets += socket
                val index = accepted.getAndIncrement()
                Thread { serve(socket, index) }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
    }

    private fun serve(socket: Socket, index: Int) = socket.use {
        runCatching {
            val reader = BufferedReader(InputStreamReader(socket.getInputStream(), Charsets.ISO_8859_1))
            val out = socket.getOutputStream()
            var lastAnswerEnd = out.answer("200 welcome")
            var silent = false
            while (true) {
                val line = reader.readLine() ?: break
                commands += Command(index, line, System.nanoTime())
                if (silent) {
                    dropped.incrementAndGet()
                    continue
                }
                if (diesAt(index, lastAnswerEnd)) {
                    silenced.incrementAndGet()
                    when (onIdle) {
                        OnIdle.SILENT -> {
                            silent = true
                            dropped.incrementAndGet()
                            continue
                        }
                        OnIdle.RST -> socket.setSoLinger(true, 0)
                        OnIdle.FIN -> socket.shutdownOutput()
                        OnIdle.KICK -> {
                            out.answer("400 idle timeout")
                            socket.shutdownOutput()
                        }
                    }
                    break
                }
                val reply = when {
                    line.startsWith("STAT ") -> "223 0 ${line.removePrefix("STAT ")} article exists"
                    line == "DATE" -> "111 20261028120000"
                    line == "QUIT" -> "205 bye"
                    line.startsWith("BODY ") && !answerBody -> continue
                    line.startsWith("BODY ") -> "430 no such article"
                    else -> "500 unknown command"
                }
                lastAnswerEnd = out.answer(reply)
                if (line == "QUIT") break
            }
        }
    }

    /** Whether connection [index], whose last answer ended at [lastAnswerEnd], is dead for a command now. */
    private fun diesAt(index: Int, lastAnswerEnd: Long): Boolean {
        val after = idleAfterMs ?: return false
        return index % outOf < affected && System.nanoTime() - lastAnswerEnd >= after * NANOS_PER_MS
    }

    /** Writes [line] and returns when its writing ended. */
    private fun OutputStream.answer(line: String): Long {
        write("$line\r\n".toByteArray(Charsets.ISO_8859_1))
        flush()
        return System.nanoTime()
    }

    override fun close() {
        runCatching { serverSocket.close() }
        sockets.forEach { runCatching { it.close() } }
    }

    private companion object {
        const val NANOS_PER_MS = 1_000_000L
    }
}
