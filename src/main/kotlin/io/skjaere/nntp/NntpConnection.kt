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
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.nio.channels.UnresolvedAddressException
import java.security.GeneralSecurityException
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

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
    val welcomeResponse: NntpResponse,
    private val readTimeoutMs: Long
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

    // Sub-state of [bodyInProgress]: `false` between BODY-command write and reading
    // its status line (the response line hasn't been read yet); `true` once we've
    // read the 222 ack and the wire is now carrying yenc body bytes. Lets
    // [drainUntilArticleEnd] tell apart "drain a pre-response BODY" (where the
    // first \r\n-terminated line is genuinely the status line) from "drain a
    // body-streaming BODY" (where any \r\n is just an inter-line yenc terminator).
    // Without this flag, a yenc body that happened to start with 3 ASCII digits
    // followed by \r\n would falsely match the single-line fast-path, return
    // "wire is clean" while body bytes were still queued, and pollute the
    // connection on its next reuse.
    @Volatile
    private var bodyResponseRead: Boolean = false

    // Timer sample paired with [bodyInProgress]. Started in markBodyInProgress (just
    // before the BODY command goes on the wire), stopped in markBodyConsumed (after
    // the article terminator is consumed, or immediately for a 430-style fast-path).
    // Records into [bodyDurationTimer]; captures full BODY round-trip including yenc
    // body bytes, unlike [ackLatencyTimer] which stops at the status line.
    @Volatile
    private var bodyStartSample: Timer.Sample? = null

    /**
     * Status lines still owed by the server for pipelined single-line commands (see
     * [NntpClient.statPipelined]). A cancellation mid-pipeline leaves these on the wire;
     * [drainUntilArticleEnd] reads them off before the connection is reused.
     */
    private var pendingStatusLines: Int = 0

    /**
     * What the server still owes for the command in flight ([command], [commandMultiLine],
     * [commandRaw]'s status line, POST/IHAVE, AUTHINFO), from just before it's written until
     * its response has been read. A cancellation in between leaves it on the wire;
     * [drainUntilArticleEnd] reads off a single-line command's status line and gives up on
     * anything else. The yEnc BODY path and pipelined STATs keep their own books
     * ([bodyInProgress], [pendingStatusLines]).
     */
    private var inFlight = InFlight.NONE

    private enum class InFlight {
        NONE,

        /** One status line and nothing after it. */
        STATUS_LINE,

        /**
         * A response with a data block, a POST's article, a login, or [commandRaw]'s status
         * line, which the caller follows up. No drain brings these back in step for sure: a
         * cancelled readLine drops the part of a line it had read, which may be the start of
         * the status line or of the terminator.
         */
        UNRECOVERABLE
    }

    internal fun markPipelined(lines: Int) {
        pendingStatusLines += lines
    }

    internal fun markPipelineAnswered() {
        if (pendingStatusLines > 0) pendingStatusLines--
    }

    /** Pipelined status lines the server still owes ([pendingStatusLines]). */
    internal val pipelinedAnswersOwed: Int get() = pendingStatusLines

    internal fun markBodyInProgress() {
        bodyInProgress = true
        bodyResponseRead = false
        bodyStartSample = Timer.start(registry)
        log.debug("conn={} markBodyInProgress", id)
    }

    /**
     * Mark the BODY response status line as read. Called by [io.skjaere.nntp.NntpClient.fetchBodyYenc]
     * immediately after `commandRaw(BODY ...)` returns — at that point any subsequent bytes
     * on the wire are body content, not a status line. Used by [drainUntilArticleEnd] to
     * skip the single-line-response heuristic when we're already past the status line.
     */
    internal fun markBodyResponseRead() {
        bodyResponseRead = true
        log.debug("conn={} markBodyResponseRead", id)
    }

    internal fun markBodyConsumed() {
        bodyInProgress = false
        bodyResponseRead = false
        bodyStartSample?.stop(bodyDurationTimer)
        bodyStartSample = null
        log.debug("conn={} markBodyConsumed", id)
    }

    // Why the wire is at a point nobody knows, if it is: a read or write failed (the socket
    // dead, a read timed out, a line that isn't a status line read as one), or the body pump
    // stopped before the article's end (see [markBodyFailed]). Never cleared: the connection
    // can't be reused or drained.
    @Volatile
    private var failure: Throwable? = null

    /** Records [cause] as this connection's [failure], unless it has one, and returns it to throw. */
    private fun failed(cause: Throwable): Throwable {
        if (failure == null) failure = cause
        return cause
    }

    /** Runs [block], recording anything it throws but a cancellation as the connection's [failure]. */
    private inline fun <T> failing(block: () -> T): T = try {
        block()
    } catch (e: CancellationException) {
        throw e
    } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
        throw failed(e)
    }

    /**
     * Records that the body pump failed with [cause]. Before the article's end, that leaves
     * the socket dead or the rest of the article on the wire at a point nobody knows (the
     * pump may have read bytes it never decoded), so the connection can't be reused or
     * drained. After it (a CRC mismatch) the wire is clean and this does nothing.
     */
    internal fun markBodyFailed(cause: Throwable) {
        if (!bodyInProgress) return
        failed(cause)
        log.debug("conn={} markBodyFailed: {}", id, cause.toString())
    }

    /**
     * False while the wire is out of step: a body unfinished (its pump failed or was stopped,
     * or never ran), a read or write failed ([failure]), pipelined STAT answers still owed, or
     * a command cut short ([inFlight]). A block can return with the connection in this state:
     * Ktor 3's writer keeps a pump failure in the Body channel and completes normally, and a
     * block may catch a command's failure or cancellation itself and carry on.
     */
    internal val isReusable: Boolean
        get() = !bodyInProgress && failure == null && pendingStatusLines == 0 && inFlight == InFlight.NONE

    companion object {
        private val log = LoggerFactory.getLogger(NntpConnection::class.java)
        private val NEXT_CONNECTION_ID = java.util.concurrent.atomic.AtomicLong(0)

        /** Default for how long a read waits for the server's next bytes (see [connect]). */
        const val DEFAULT_READ_TIMEOUT_MS = 30_000L

        // Fires read timeouts on a real clock. A coroutine timeout would follow the caller's
        // dispatcher, and on a test dispatcher's virtual clock it expires at once while a
        // read waits on the network.
        private val readTimeouts = ScheduledThreadPoolExecutor(1) { task ->
            Thread(task, "nntp-read-timeout").apply { isDaemon = true }
        }.apply { removeOnCancelPolicy = true }

        /**
         * Runs [read] on [channel], failing it with [NntpConnectionException] if it gets no
         * bytes for [timeoutMs]. On expiry the channel is cancelled, which ends the
         * connection: a server that went silent mid-response leaves the wire unusable.
         */
        private suspend fun <T> withReadTimeout(
            channel: ByteReadChannel,
            timeoutMs: Long,
            host: String,
            port: Int,
            read: suspend () -> T
        ): T {
            if (timeoutMs <= 0) return read()
            fun timedOut() = "No data from $host:$port for ${timeoutMs}ms"
            // Settled by whichever comes first: the read finishing or the timer cancelling it.
            val settled = AtomicBoolean(false)
            val timer = readTimeouts.schedule({
                if (settled.compareAndSet(false, true)) channel.cancel(NntpConnectionException(timedOut()))
            }, timeoutMs, TimeUnit.MILLISECONDS)
            val result = try {
                read()
            } catch (e: CancellationException) {
                settled.set(true)
                throw e
            } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
                if (settled.compareAndSet(false, true)) throw e
                throw NntpConnectionException(timedOut(), e)
            } finally {
                timer.cancel(false)
            }
            // The timer fired as the read finished: the channel is cancelled all the same.
            if (!settled.compareAndSet(false, true)) throw NntpConnectionException(timedOut())
            return result
        }

        /**
         * @param readTimeoutMs how long a read may wait without receiving a byte before the
         *   operation fails with [NntpConnectionException]: a stalled server, or a half-open
         *   TCP connection after a network change. An idle timeout, not a deadline: a long
         *   BODY or pipelined STAT batch that keeps receiving bytes never times out, and an
         *   idle connection with no read pending is never affected. 0 or less disables it.
         * @param verifyHostname with [useTls], check that the server's certificate names [host]
         *   (a host name, or an IP address) and send a host name as the server name (SNI). Off,
         *   the certificate only has to be trusted, whatever host it names, and no server name
         *   is sent: for a server reached by an address its certificate doesn't list.
         * @throws NntpConnectionException if the connection can't be opened: the host name
         *   doesn't resolve, the connect is refused or fails, or the TLS handshake fails.
         */
        suspend fun connect(
            host: String,
            port: Int,
            selectorManager: SelectorManager,
            useTls: Boolean = false,
            readTimeoutMs: Long = DEFAULT_READ_TIMEOUT_MS,
            verifyHostname: Boolean = true
        ): NntpConnection {
            val socket = try {
                openSocket(host, port, selectorManager, useTls, verifyHostname)
            } catch (e: UnresolvedAddressException) {
                throw NntpConnectionException("Unknown host $host", e)
            } catch (e: IOException) {
                throw NntpConnectionException("Could not connect to $host:$port: ${e.message}", e)
            } catch (e: GeneralSecurityException) {
                // Ktor's TLS handshake lets the trust manager's CertificateException through.
                throw NntpConnectionException("Could not connect to $host:$port: ${e.message}", e)
            }
            // Anything that throws between openSocket and a successful return must close
            // the socket; otherwise its FD is held by the JVM until process exit.
            try {
                val readChannel = socket.openReadChannel()
                val writeChannel = socket.openWriteChannel(autoFlush = true)
                val welcomeLine = withReadTimeout(readChannel, readTimeoutMs, host, port) { readChannel.readLine() }
                    ?: throw NntpConnectionException("Connection closed before welcome message")
                val welcomeResponse = parseResponseLine(welcomeLine)
                if (welcomeResponse.code != 200 && welcomeResponse.code != 201) {
                    throw NntpProtocolException(
                        "Unexpected welcome code: ${welcomeResponse.code}",
                        welcomeResponse
                    )
                }
                return NntpConnection(
                    host, port, socket, readChannel, writeChannel, welcomeResponse, readTimeoutMs
                )
            } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
                runCatching { socket.socketContext.cancel() }
                runCatching { socket.close() }
                throw e
            }
        }

        // Ktor's TLS actors run in this context. With CBC cipher suites they call
        // generateNonce(), which runBlocking-waits on a nonce producer that runs on
        // Dispatchers.Default. If the actors also ran on Default, a few concurrent handshakes
        // would occupy every Default worker (one per CPU core, so 4 on many phones) and the
        // producer could never run: a deadlock that surfaces as connect timeouts. IO threads
        // are meant for blocking calls and leave Default's workers free.
        private val tlsContext = Dispatchers.IO + CoroutineExceptionHandler { _, throwable ->
            log.debug("TLS actor terminated: {}", throwable.message)
        }

        private suspend fun openSocket(
            host: String,
            port: Int,
            selectorManager: SelectorManager,
            useTls: Boolean,
            verifyHostname: Boolean
        ): Socket {
            log.debug("openSocket called with host='{}', port={}, useTls={}", host, port, useTls)
            val rawSocket = aSocket(selectorManager).tcp().connect(host, port)
            return if (useTls) {
                // TLS handshake can fail (cert mismatch, server abort, network); the raw
                // TCP socket below is already open and would otherwise leak.
                try {
                    rawSocket.tls(coroutineContext = tlsContext) {
                        if (verifyHostname) {
                            // SNI names a host without a trailing dot; an IP address isn't one (RFC 6066 §3).
                            if (ipAddress(host) == null) serverName = host.trimEnd('.')
                            trustManager = HostnameVerifyingTrustManager(HostnameVerifyingTrustManager.platformDefault(), host)
                        }
                    }
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
        // A login cut short or refused leaves the session half-authenticated or not at all,
        // which no drain puts right.
        inFlight = InFlight.UNRECOVERABLE
        writeLine(NntpCommand.authinfoUser(user))
        val userResponse = readResponse()
        if (userResponse.code == 281) {
            username = user
            password = pass
            inFlight = InFlight.NONE
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
        inFlight = InFlight.NONE
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
        val sample = Timer.start(registry)
        val response = exchange(cmd)
        sample.stop(ackLatencyTimer(cmd))
        return response
    }

    suspend fun commandMultiLine(cmd: String): Pair<NntpResponse, List<String>> {
        val sample = Timer.start(registry)
        inFlight = InFlight.UNRECOVERABLE
        writeLine(cmd)
        val response = readResponse()
        // Stop the timer at the status line — that's "first byte of response".
        // The data lines that follow on a 1xx/2xx response are the body, not TTFB.
        sample.stop(ackLatencyTimer(cmd))
        val lines = if (response.code in 100..299) readMultiLineData() else emptyList()
        inFlight = InFlight.NONE
        return Pair(response, lines)
    }

    /**
     * Send a command and read the initial status line. The caller is responsible for
     * consuming any remaining data via [rawReadChannel] (used by the BODY/yenc streaming
     * path, where the body bytes follow the status line), and for keeping track of it: this
     * connection only knows whether the status line was read. With single-owner connections
     * there's no mutex to manage — the connection is held by the caller for the whole
     * operation.
     */
    suspend fun commandRaw(cmd: String): NntpResponse {
        val sample = Timer.start(registry)
        // What follows the status line depends on the command (a BODY's data, POST waiting for
        // its article), so a cancellation before it's read leaves nothing a drain can rely on.
        val response = exchange(cmd, InFlight.UNRECOVERABLE)
        sample.stop(ackLatencyTimer(cmd))
        return response
    }

    /** Writes [cmd] and reads its status line, owing [owed] in between ([inFlight]). */
    private suspend fun exchange(cmd: String, owed: InFlight = InFlight.STATUS_LINE): NntpResponse {
        inFlight = owed
        writeLine(cmd)
        val response = readResponse()
        inFlight = InFlight.NONE
        return response
    }

    /**
     * After POST's or IHAVE's go-ahead: sends [article] (dot-stuffed) and the terminator, and
     * reads the server's answer.
     */
    internal suspend fun sendArticle(article: String): NntpResponse {
        // Until the terminator is on its way, the server takes whatever follows as the article.
        inFlight = InFlight.UNRECOVERABLE
        writeLine(article)
        return exchange(".")
    }

    private val registry = Metrics.globalRegistry

    // Status-line ack latency: command write -> first byte of response status line.
    // Excludes any body that follows (use bodyDurationTimer for that). For BODY this
    // captures the server's command-acknowledgement delay only — *not* the time spent
    // streaming body bytes back.
    private fun ackLatencyTimer(cmd: String): Timer = Timer.builder("nntp.command.ack_latency")
        .description("Time from sending an NNTP command to receiving the first byte of the response status line")
        .publishPercentileHistogram()
        .tag("command", cmd.substringBefore(' ').uppercase())
        .register(registry)

    // Full BODY round-trip: command write -> article terminator consumed off the wire.
    // Started in markBodyInProgress, stopped in markBodyConsumed. Captures the
    // body-transfer phase that ack latency misses; together they bracket the slow-ack
    // vs slow-body failure modes when an upstream provider degrades.
    private val bodyDurationTimer: Timer = Timer.builder("nntp.body.duration")
        .description("Full BODY round-trip: command write to article terminator consumed off the wire")
        .publishPercentileHistogram()
        .tag("pool.name", "$host:$port")
        .register(registry)

    /** [withReadTimeout] with this connection's read timeout. A read that fails is its [failure]. */
    private suspend fun <T> timedRead(read: suspend () -> T): T =
        failing { withReadTimeout(readChannel, readTimeoutMs, host, port, read) }

    /** Reads what's available, up to [length] bytes, or -1 at EOF; bounded by the read timeout. */
    internal suspend fun readAvailable(dst: ByteArray, offset: Int = 0, length: Int = dst.size - offset): Int =
        timedRead { readChannel.readAvailable(dst, offset, length) }

    suspend fun readResponse(): NntpResponse {
        val line = timedRead { readChannel.readLine() }
            ?: throw failed(NntpConnectionException("Connection closed unexpectedly"))
        // Not a status line: whatever it belongs to, the wire is out of step.
        return failing { parseResponseLine(line) }
    }

    suspend fun readMultiLineData(): List<String> {
        val lines = mutableListOf<String>()
        while (true) {
            val line = timedRead { readChannel.readLine() }
                ?: throw failed(NntpConnectionException("Connection closed during multi-line read"))
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
        return timedRead { readChannel.readLine() }
            ?: throw failed(NntpConnectionException("Connection closed unexpectedly"))
    }

    /**
     * Read a line as raw bytes (ISO-8859-1), without UTF-8 interpretation.
     * Returns the line content without the trailing CRLF.
     * Use this for reading data that may contain non-ASCII bytes (e.g., yenc-encoded data).
     */
    suspend fun readRawLine(): ByteArray = timedRead {
        val buffer = java.io.ByteArrayOutputStream()
        var prevByte: Int = -1
        while (true) {
            val byte = readChannel.readByte().toInt() and 0xFF
            if (prevByte == '\r'.code && byte == '\n'.code) break
            buffer.write(byte)
            prevByte = byte
        }
        // Remove the trailing CR from buffer
        val result = buffer.toByteArray()
        result.copyOf(result.size - 1)
    }

    /** The socket's read channel. Reads through it aren't bounded by the read timeout. */
    val rawReadChannel: ByteReadChannel get() = readChannel

    suspend fun writeLine(line: String) {
        failing { writeChannel.writeStringUtf8("$line\r\n") }
    }

    /** Writes several command lines in one write (one flush), for pipelining. */
    suspend fun writeLines(lines: List<String>) {
        failing { writeChannel.writeStringUtf8(lines.joinToString("") { "$it\r\n" }) }
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
     * Outside a BODY, the status line a cancelled single-line command (STAT, GROUP, DATE...)
     * still owes is read off. A command with a data block (ARTICLE, HEAD, OVER, HDR...) or a
     * POST cut short can't be drained: see [InFlight.UNRECOVERABLE].
     *
     * @return true if the wire is now clean and the connection is reusable, false if
     *   the server closed mid-stream (EOF) without giving us a clean state, a read or
     *   write or the body pump had already failed ([failure]), or what's owed can't be
     *   drained.
     */
    suspend fun drainUntilArticleEnd(): Boolean {
        failure?.let {
            log.debug("conn={} drain: a read or write failed ({}), wire position unknown", id, it.toString())
            return false
        }
        // Pipelined STATs never overlap a BODY: read off the status lines still owed.
        while (pendingStatusLines > 0) {
            timedRead { readChannel.readLine() } ?: run {
                log.debug("conn={} drain: EOF with {} pipelined responses outstanding", id, pendingStatusLines)
                return false
            }
            pendingStatusLines--
        }
        // Fast-path: protocol-state check. If no body operation is in flight, the
        // wire is line-protocol clean once any other command's answer is read off. Set by
        // fetchBodyYenc BEFORE issuing the BODY command (so the flag is true even
        // for cancellations that land between the write and the response read) and
        // cleared either by the YencDecoder writer on natural body termination or by
        // this function on successful drain.
        if (!bodyInProgress) return drainInFlight()
        log.debug("conn={} drain: starting (bodyInProgress=true)", id)

        val buffer = ByteArray(8192)
        // Sliding window of just enough bytes to detect the 5-byte terminator across
        // chunk boundaries. Bounded — we never accumulate the whole body in memory.
        var window = ""
        // If the BODY command's status line has *already* been read, every subsequent
        // byte on the wire is yenc body — skip the single-line-response heuristic
        // entirely. Without this guard, yenc data lines that happen to start with 3
        // ASCII digits + \r\n would falsely match the heuristic, return "wire clean",
        // and leave body bytes queued for the next caller (the corrupt-response-line
        // symptom in production).
        val responseAlreadyRead = bodyResponseRead
        // Until we see the first \r\n we don't yet know if this is a single-line
        // response or a multi-line body, so we accumulate the prefix to inspect the
        // first line in full. Capped at a few bytes — we only need the 3-digit code
        // and the line terminator.
        val firstLineProbe = StringBuilder()
        var firstLineDecided = responseAlreadyRead
        var totalRead = 0L

        while (true) {
            val n = readAvailable(buffer)
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
                        bodyResponseRead = false
                        inFlight = InFlight.NONE
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
                bodyResponseRead = false
                // The BODY's status line, if a cancellation cut commandRaw short, was read above.
                inFlight = InFlight.NONE
                log.debug("conn={} drain: success after {} bytes", id, totalRead)
                return true
            }
        }
    }

    /** [drainUntilArticleEnd] outside a BODY: reads off the status line a command still owes. */
    private suspend fun drainInFlight(): Boolean = when (inFlight) {
        InFlight.NONE -> {
            log.debug("conn={} drain: nothing in flight, wire is clean", id)
            true
        }
        InFlight.STATUS_LINE -> {
            // Whole, or what's left of it after a cancelled read: either way it ends the answer.
            val line = timedRead { readChannel.readLine() }
            if (line == null) {
                log.debug("conn={} drain: EOF with a status line outstanding", id)
                false
            } else {
                inFlight = InFlight.NONE
                log.debug("conn={} drain: read off the status line owed: {}", id, line)
                true
            }
        }
        InFlight.UNRECOVERABLE -> {
            log.debug("conn={} drain: a multi-line response or article was cut short, wire position unknown", id)
            false
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
     *
     * A coroutine still reading or writing (the pool's close() closes connections callers
     * have checked out) fails with an I/O error whose cause says the connection was closed.
     * The socket's cancellation alone would reach it as a CancellationException, which it
     * would take for its own cancellation.
     */
    override fun close() {
        try {
            val closedCause = NntpConnectionException("Connection to $host:$port closed")
            readChannel.cancel(closedCause)
            writeChannel.cancel(closedCause)
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
