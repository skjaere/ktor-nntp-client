package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.network.tls.tls
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.readAvailable
import io.ktor.utils.io.readByte
import io.ktor.utils.io.readLine
import io.ktor.utils.io.writeStringUtf8
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.io.Closeable

/**
 * Owns exactly one socket for its lifetime. Open in [connect], close in [close]. Never
 * swaps the underlying socket — when an operation fails, the connection is dead and the
 * pool discards it in favour of a fresh one. This single-socket invariant is the whole
 * reason close paths fit on one screen now: there's no `var socket` to lose track of.
 *
 * Thread-safety: the pool gives one [NntpConnection] to one caller at a time
 * (single-owner via [NntpClientPool.withClient]), so this class has no internal mutex.
 */
class NntpConnection private constructor(
    private val host: String,
    private val port: Int,
    private val socket: Socket,
    private val readChannel: ByteReadChannel,
    private val writeChannel: ByteWriteChannel,
    val welcomeResponse: NntpResponse
) : Closeable {

    /**
     * Stable per-connection id used in log lines so we can correlate the lifecycle of
     * a single connection across cancellations, drains, and reuse — e.g. confirm that
     * the drain on conn=N completed before another caller sees conn=N. Process-local;
     * not stable across restarts.
     */
    val id: Long = NEXT_CONNECTION_ID.getAndIncrement()

    private var username: String? = null
    private var password: String? = null

    // True while a yenc BODY response is being streamed off the wire — i.e. between
    // [markBodyInProgress] (called once the 222 reply has been seen and decoder is
    // about to consume the article bytes) and [markBodyConsumed] (decoder finished
    // cleanly OR drain reached the article terminator). [drainUntilArticleEnd] uses
    // this to fast-path: if false, the wire is line-protocol clean and there's
    // nothing to drain. Volatile because the drain coroutine reads it from a
    // different coroutine than the one that toggled it.
    @Volatile
    private var bodyInProgress: Boolean = false

    internal fun markBodyInProgress() {
        bodyInProgress = true
        log.debug("conn={} markBodyInProgress", id)
    }

    internal fun markBodyConsumed() {
        bodyInProgress = false
        log.debug("conn={} markBodyConsumed", id)
    }

    companion object {
        private val log = LoggerFactory.getLogger(NntpConnection::class.java)
        private val NEXT_CONNECTION_ID = java.util.concurrent.atomic.AtomicLong(0)

        suspend fun connect(
            host: String,
            port: Int,
            selectorManager: SelectorManager,
            useTls: Boolean = false
        ): NntpConnection {
            val socket = openSocket(host, port, selectorManager, useTls)
            // Anything that throws between openSocket and a successful return must close
            // the socket; otherwise its FD is held by the JVM until process exit.
            try {
                val readChannel = socket.openReadChannel()
                val writeChannel = socket.openWriteChannel(autoFlush = true)
                val welcomeLine = readChannel.readLine()
                    ?: throw NntpConnectionException("Connection closed before welcome message")
                val welcomeResponse = parseResponseLine(welcomeLine)
                if (welcomeResponse.code != 200 && welcomeResponse.code != 201) {
                    throw NntpProtocolException(
                        "Unexpected welcome code: ${welcomeResponse.code}",
                        welcomeResponse
                    )
                }
                return NntpConnection(
                    host, port, socket, readChannel, writeChannel, welcomeResponse
                )
            } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
                runCatching { socket.socketContext.cancel() }
                runCatching { socket.close() }
                throw e
            }
        }

        private val tlsExceptionHandler = CoroutineExceptionHandler { _, throwable ->
            log.debug("TLS actor terminated: {}", throwable.message)
        }

        private suspend fun openSocket(
            host: String,
            port: Int,
            selectorManager: SelectorManager,
            useTls: Boolean
        ): Socket {
            log.debug("openSocket called with host='{}', port={}, useTls={}", host, port, useTls)
            val rawSocket = aSocket(selectorManager).tcp().connect(host, port)
            return if (useTls) {
                // TLS handshake can fail (cert mismatch, server abort, network); the raw
                // TCP socket below is already open and would otherwise leak.
                try {
                    rawSocket.tls(coroutineContext = Dispatchers.IO + tlsExceptionHandler)
                } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
                    runCatching { rawSocket.close() }
                    throw e
                }
            } else {
                rawSocket
            }
        }
    }

    /** AUTHINFO USER/PASS handshake. Caller has exclusive access via the pool. */
    private suspend fun performAuth(user: String, pass: String): NntpResponse {
        writeLine(NntpCommand.authinfoUser(user))
        val userResponse = readResponse()
        if (userResponse.code == 281) {
            username = user
            password = pass
            return userResponse
        }
        if (userResponse.code != 381) {
            throw NntpAuthenticationException(
                "AUTHINFO USER failed: ${userResponse.code} ${userResponse.message}",
                userResponse
            )
        }
        writeLine(NntpCommand.authinfoPass(pass))
        val passResponse = readResponse()
        if (passResponse.code != 281) {
            throw NntpAuthenticationException(
                "AUTHINFO PASS failed: ${passResponse.code} ${passResponse.message}",
                passResponse
            )
        }
        username = user
        password = pass
        return passResponse
    }

    internal suspend fun authenticate(username: String, password: String): NntpResponse =
        performAuth(username, password)

    /**
     * Send a command and read the status line. Any failure leaves the wire in an
     * indeterminate state — the caller (the pool's withClient) treats that as fatal
     * for this connection and discards it.
     */
    suspend fun command(cmd: String): NntpResponse {
        writeLine(cmd)
        return readResponse()
    }

    suspend fun commandMultiLine(cmd: String): Pair<NntpResponse, List<String>> {
        writeLine(cmd)
        val response = readResponse()
        return if (response.code in 100..299) {
            Pair(response, readMultiLineData())
        } else {
            Pair(response, emptyList())
        }
    }

    /**
     * Send a command and read the initial status line. The caller is responsible for
     * consuming any remaining data via [rawReadChannel] (used by the BODY/yenc streaming
     * path, where the body bytes follow the status line). With single-owner connections
     * there's no mutex to manage — the connection is held by the caller for the whole
     * operation.
     */
    suspend fun commandRaw(cmd: String): NntpResponse {
        writeLine(cmd)
        return readResponse()
    }

    suspend fun readResponse(): NntpResponse {
        val line = readChannel.readLine()
            ?: throw NntpConnectionException("Connection closed unexpectedly")
        return parseResponseLine(line)
    }

    suspend fun readMultiLineData(): List<String> {
        val lines = mutableListOf<String>()
        while (true) {
            val line = readChannel.readLine()
                ?: throw NntpConnectionException("Connection closed during multi-line read")
            if (line == ".") break
            // Dot-unstuffing: lines starting with ".." have leading dot removed
            if (line.startsWith("..")) {
                lines.add(line.substring(1))
            } else {
                lines.add(line)
            }
        }
        return lines
    }

    suspend fun readLine(): String {
        return readChannel.readLine()
            ?: throw NntpConnectionException("Connection closed unexpectedly")
    }

    /**
     * Read a line as raw bytes (ISO-8859-1), without UTF-8 interpretation.
     * Returns the line content without the trailing CRLF.
     * Use this for reading data that may contain non-ASCII bytes (e.g., yenc-encoded data).
     */
    suspend fun readRawLine(): ByteArray {
        val buffer = java.io.ByteArrayOutputStream()
        var prevByte: Int = -1
        while (true) {
            val byte = readChannel.readByte().toInt() and 0xFF
            if (prevByte == '\r'.code && byte == '\n'.code) {
                // Remove the trailing CR from buffer
                val result = buffer.toByteArray()
                return result.copyOf(result.size - 1)
            }
            buffer.write(byte)
            prevByte = byte
        }
    }

    val rawReadChannel: ByteReadChannel get() = readChannel

    suspend fun writeLine(line: String) {
        writeChannel.writeStringUtf8("$line\r\n")
    }

    /**
     * Recover a reusable connection after a body operation was cancelled with bytes
     * still in flight on the wire. Handles both shapes the server may have produced
     * (or be in the middle of producing) at cancellation time:
     *
     *  - 222 multi-line body response — read until the article terminator `\r\n.\r\n`
     *  - non-222 single-line response (430, 5xx, etc.) — read until end of that one
     *    line; no body terminator is coming
     *  - body data itself if the cancellation landed mid-body — read until terminator
     *
     * The first complete `\r\n`-terminated line tells us which shape we're in. If it
     * starts with three digits and isn't `222`, the server is done. Otherwise we keep
     * reading until we see `\r\n.\r\n`.
     *
     * @return true if the wire is now clean and the connection is reusable, false if
     *   the server closed mid-stream (EOF) without giving us a clean state.
     */
    suspend fun drainUntilArticleEnd(): Boolean {
        // Fast-path: protocol-state check. If no body operation is in flight, the
        // wire is line-protocol clean and there's nothing to drain. Set by
        // fetchBodyYenc BEFORE issuing the BODY command (so the flag is true even
        // for cancellations that land between the write and the response read) and
        // cleared either by the YencDecoder writer on natural body termination or by
        // this function on successful drain.
        if (!bodyInProgress) {
            log.debug("conn={} drain: bodyInProgress=false fast-path, wire is clean", id)
            return true
        }
        log.debug("conn={} drain: starting (bodyInProgress=true)", id)

        val buffer = ByteArray(8192)
        // Sliding window of just enough bytes to detect the 5-byte terminator across
        // chunk boundaries. Bounded — we never accumulate the whole body in memory.
        var window = ""
        // Until we see the first \r\n we don't yet know if this is a single-line
        // response or a multi-line body, so we accumulate the prefix to inspect the
        // first line in full. Capped at a few bytes — we only need the 3-digit code
        // and the line terminator.
        val firstLineProbe = StringBuilder()
        var firstLineDecided = false
        var totalRead = 0L

        while (true) {
            val n = readChannel.readAvailable(buffer)
            if (n == -1) {
                log.debug("conn={} drain: hit EOF after {} bytes (server closed)", id, totalRead)
                return false
            }
            totalRead += n
            val chunk = String(buffer, 0, n, Charsets.ISO_8859_1)

            if (!firstLineDecided) {
                firstLineProbe.append(chunk)
                val crlf = firstLineProbe.indexOf("\r\n")
                if (crlf >= 3) {
                    val code = firstLineProbe.substring(0, 3)
                    if (code.all { it.isDigit() } && code != "222") {
                        // Single-line response (430/5xx/etc.) — wire is clean once the
                        // line is consumed. We've already read past the \r\n so any
                        // bytes after firstLineProbe are surplus, but for a compliant
                        // NNTP server there shouldn't be any (no pipelining).
                        bodyInProgress = false
                        log.debug(
                            "conn={} drain: single-line {} response; wire clean after {} bytes",
                            id, code, totalRead
                        )
                        return true
                    }
                    firstLineDecided = true
                }
                // else: still accumulating the first line, keep reading.
            }

            window = (window + chunk).takeLast(8)
            if (window.contains("\r\n.\r\n")) {
                bodyInProgress = false
                log.debug("conn={} drain: success after {} bytes", id, totalRead)
                return true
            }
        }
    }

    /**
     * Cancel the actor coroutines, wait for them to settle, then close the SocketChannel
     * in a `finally` so the FD is released even if the join propagates a Throwable.
     *
     * Why join before close: under Ktor 3.x, [Socket.close] called immediately after
     * `socketContext.cancel()` can land while the read/write actor still holds the
     * channel — close() then returns without releasing the underlying FD, producing
     * orphan socket FDs (visible in /proc/<pid>/fd but not in /proc/net/tcp).
     */
    override fun close() {
        try {
            socket.socketContext.cancel()
            runBlocking { runCatching { socket.socketContext.join() } }
        } finally {
            runCatching { socket.close() }.onFailure {
                log.warn("Socket close failed for {}:{}: {}", host, port, it.message, it)
            }
        }
    }

    /** Graceful close: send QUIT (bounded by [timeoutMs]) before tearing the socket down. */
    suspend fun shutdown(timeoutMs: Long = DEFAULT_QUIT_TIMEOUT_MS) {
        try {
            withTimeoutOrNull(timeoutMs) {
                runCatching {
                    writeLine(NntpCommand.QUIT)
                    readResponse()
                }
            }
        } finally {
            close()
        }
    }
}

private const val DEFAULT_QUIT_TIMEOUT_MS = 5_000L
