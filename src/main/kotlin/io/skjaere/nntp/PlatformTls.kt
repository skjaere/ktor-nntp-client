package io.skjaere.nntp

import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.ClosedByteChannelException
import io.ktor.utils.io.InternalAPI
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.CancellationException
import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.io.readAtMostTo
import kotlinx.io.transferFrom
import java.io.EOFException
import java.nio.ByteBuffer
import javax.net.ssl.SNIHostName
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLEngineResult
import javax.net.ssl.SSLEngineResult.HandshakeStatus
import javax.net.ssl.SSLEngineResult.Status
import javax.net.ssl.SSLException
import javax.net.ssl.X509TrustManager

/**
 * TLS from the platform (javax.net.ssl: Conscrypt on Android, SunJSSE on the JVM) over a TCP socket's channels,
 * [rawRead] and [rawWrite]. [read] is what the server sent, decrypted when its reader asks for more, in the reader's
 * own coroutine; [write] encrypts and sends what is written to it when flushed. Nothing runs between the socket and
 * the reader: Ktor's tls() put a coroutine and a channel there for each direction, one more hand-off per read, and
 * decrypted CBC records in Kotlin with an allocation per record (library issue K43).
 *
 * TLS 1.3 when the server offers it, else 1.2, with the platform's cipher suites. Reads and writes take turns, as a
 * connection's commands and answers do (the pool gives it to one caller at a time): a key update the server asks for
 * is answered from [read], through the buffers [write] uses.
 */
internal class PlatformTls private constructor(
    private val engine: SSLEngine,
    private val rawRead: BulkReadChannel,
    private val rawWrite: ByteWriteChannel,
) {
    // Records from the socket, one or more and maybe the start of the next. Each read from the socket takes what it
    // has, up to the buffer's room, so a burst of records is decrypted in one go.
    private var netIn: ByteBuffer = ByteBuffer.allocate(maxOf(engine.session.packetBufferSize, NET_BUFFER_SIZE)).flip()

    // A record's plaintext, as the engine decrypts it, before it joins [Reader]'s buffer.
    private var appIn: ByteBuffer = ByteBuffer.allocate(engine.session.applicationBufferSize)

    // Records to send, as the engine encrypts them.
    private var netOut: ByteBuffer = ByteBuffer.allocate(engine.session.packetBufferSize)

    // Plaintext to encrypt: [Writer]'s buffer, a record's worth at a time.
    private val appOut: ByteBuffer = ByteBuffer.allocate(MAX_PLAINTEXT_RECORD)

    // What has been decrypted and not yet read: [read]'s buffer.
    private val plain = Buffer()

    val read: BulkReadChannel = Reader()
    val write: ByteWriteChannel = Writer()

    /** The protocol and cipher suite the handshake chose, as "TLSv1.3 TLS_AES_128_GCM_SHA256". */
    val description: String get() = engine.session.let { "${it.protocol} ${it.cipherSuite}" }

    /**
     * Runs the handshake, each read from the socket in it bounded by the socket's read timeout as the connection's reads
     * are. The welcome or anything else the server sends with its last flight is kept for [read].
     */
    private suspend fun handshake() {
        engine.beginHandshake()
        while (true) {
            when (engine.handshakeStatus) {
                HandshakeStatus.NEED_WRAP -> wrapAndSend(EMPTY)
                HandshakeStatus.NEED_TASK -> runTasks()
                HandshakeStatus.FINISHED, HandshakeStatus.NOT_HANDSHAKING, null -> return
                // NEED_UNWRAP, and NEED_UNWRAP_AGAIN (DTLS only, and missing on Android, so never named here).
                else -> {
                    val result = unwrap()
                    when (result.status) {
                        Status.BUFFER_UNDERFLOW -> if (readSocket(want = 0) < 0) {
                            throw EOFException("The server closed the connection in the TLS handshake")
                        }
                        Status.CLOSED -> throw SSLException("The server ended TLS in the handshake")
                        else -> Unit
                    }
                }
            }
        }
    }

    /** Decrypts the next record in [netIn], if it holds a whole one, and moves its plaintext to [plain]. */
    private fun unwrap(): SSLEngineResult {
        while (true) {
            val result = engine.unwrap(netIn, appIn)
            if (result.status == Status.BUFFER_OVERFLOW) {
                // A record larger than the session said at the start: room for it, and try again.
                appIn = grow(appIn, engine.session.applicationBufferSize)
                continue
            }
            if (appIn.position() > 0) {
                appIn.flip()
                plain.transferFrom(appIn)
                appIn.clear()
            }
            if (result.handshakeStatus == HandshakeStatus.NEED_TASK) runTasks()
            return result
        }
    }

    /**
     * Reads what the socket has into [netIn], after the part of a record it holds, as much as there is room for: the
     * bytes read, or -1 at the end of the stream (a FIN or reset, TLS's close_notify missing). Fewer than [want], and
     * the socket lets more gather first ([BulkReadChannel.readAvailable]).
     */
    private suspend fun readSocket(want: Int): Int {
        netIn.compact()
        if (!netIn.hasRemaining()) netIn = grow(netIn, engine.session.packetBufferSize)
        val n = try {
            rawRead.readAvailable(netIn, want)
        } finally {
            netIn.flip()
        }
        return n
    }

    /**
     * What the record [netIn] begins with still lacks, by the length its header gives: only so much is worth a wake-up,
     * as the engine decrypts nothing before the record's end. Without its header, the header: a short record (a status
     * line, the welcome) is then read at once, and only a long one still arriving lets the rest gather.
     */
    private fun recordShortfall(): Int {
        val have = netIn.remaining()
        if (have < RECORD_HEADER) return RECORD_HEADER - have
        val at = netIn.position()
        val length = RECORD_HEADER + ((netIn.get(at + 3).toInt() and 0xFF) shl 8 or (netIn.get(at + 4).toInt() and 0xFF))
        return length - have
    }

    /**
     * Encrypts all of [source] into records and sends them; the handshake's messages when it is [EMPTY]. Each record is
     * flushed as it is written (writeFully flushes), so nothing waits for the next command.
     */
    private suspend fun wrapAndSend(source: ByteBuffer) {
        do {
            netOut.clear()
            val result = engine.wrap(source, netOut)
            when (result.status) {
                Status.BUFFER_OVERFLOW -> {
                    netOut = grow(netOut, engine.session.packetBufferSize)
                    continue
                }
                Status.CLOSED -> if (netOut.position() == 0) throw SSLException("TLS is closed")
                else -> Unit
            }
            netOut.flip()
            if (netOut.hasRemaining()) rawWrite.writeFully(netOut)
            if (result.handshakeStatus == HandshakeStatus.NEED_TASK) runTasks()
        } while (source.hasRemaining() || (source === EMPTY && engine.handshakeStatus == HandshakeStatus.NEED_WRAP))
    }

    // The JDK's engine leaves certificate checks and key exchange to tasks; Conscrypt runs them inside unwrap.
    private fun runTasks() {
        while (true) (engine.delegatedTask ?: return).run()
    }

    /**
     * Closes the engine both ways and takes out what closing queued (after a handshake, its close_notify) and drops it:
     * Conscrypt frees its native state (the SSL, its buffers, the session) once the engine has nothing left to send,
     * which a close_notify nobody takes out never is, so it waited for finalization. The connection's channels are
     * cancelled first, so no read or write is left to use the engine, and the socket is closed or about to be.
     */
    fun release() {
        engine.closeOutbound()
        try {
            engine.closeInbound()
        } catch (_: SSLException) {
            // No close_notify came: nothing to read anyway.
        }
        try {
            var wraps = 0
            while (!engine.isOutboundDone && wraps++ < MAX_CLOSING_WRAPS) {
                netOut.clear()
                if (engine.wrap(EMPTY, netOut).bytesProduced() == 0) break
            }
        } catch (_: SSLException) {
            // Closed for good already: nothing left to take out.
        }
    }

    /** Whether the engine is closed both ways with nothing left to send, as [release] leaves it. */
    internal val released: Boolean get() = engine.isInboundDone && engine.isOutboundDone

    /** What the server sent, decrypted as its reader asks for more. */
    @OptIn(InternalAPI::class)
    private inner class Reader : BulkReadChannel {
        // The end of the stream: the server's close_notify, or the socket's end without it.
        private var ended = false

        @Volatile
        private var closed: Throwable? = null

        override val closedCause: Throwable? get() = closed

        override val isClosedForRead: Boolean get() = closed != null || (ended && plain.exhausted())

        override val readBuffer: Source
            get() {
                closed?.let { throw it }
                return plain
            }

        override suspend fun awaitContent(min: Int): Boolean {
            closed?.let { throw it }
            while (plain.size < min && !ended) {
                if (!decryptMore()) ended = true
                closed?.let { throw it }
            }
            return plain.size >= min
        }

        /**
         * All that is decrypted, as [dst] has room for, decrypting more first if nothing is: -1 at the end. [want] needs
         * nothing here: a record is decrypted whole, its bytes let gather in the socket until then.
         */
        override suspend fun readAvailable(dst: ByteBuffer, want: Int): Int {
            if (!awaitContent(1)) return -1
            return plain.drainInto(dst)
        }

        /**
         * Decrypts the records [netIn] holds, reading from the socket until there is one: false at the end of the
         * stream. All the whole records read go in at once, so the reader gets bursts rather than a record at a time.
         */
        private suspend fun decryptMore(): Boolean {
            var produced = false
            while (true) {
                val result = unwrap()
                produced = produced || result.bytesProduced() > 0
                when (result.status) {
                    Status.CLOSED -> {
                        // close_notify: what came before it is the stream's last.
                        ended = true
                        return produced
                    }
                    Status.BUFFER_UNDERFLOW -> {
                        if (produced) return true
                        // Like Ktor's, a stream that ends without close_notify ends as at a FIN: the NNTP framing
                        // (the status line, the article terminator) says whether it was whole.
                        if (readSocket(want = recordShortfall()) < 0) return false
                    }
                    // A post-handshake message: a session ticket needs nothing, a key update may need an answer.
                    else -> if (result.handshakeStatus != HandshakeStatus.NOT_HANDSHAKING &&
                        engine.handshakeStatus == HandshakeStatus.NEED_WRAP
                    ) {
                        wrapAndSend(EMPTY)
                    }
                }
            }
        }

        override fun cancel(cause: Throwable?) {
            if (closed != null) return
            closed = when (cause) {
                is CancellationException -> CancellationException(cause.message, cause)
                else -> ClosedByteChannelException(cause)
            }
            // A read waiting on the socket fails with the cause, as one waiting on Ktor's TLS channel did.
            rawRead.cancel(cause)
        }
    }

    /** Encrypts what is written to it, a record at a time, and sends it at each flush, as autoFlush has it do. */
    @OptIn(InternalAPI::class)
    private inner class Writer : ByteWriteChannel {
        private val pending = Buffer()

        @Volatile
        private var closed: Throwable? = null

        override val autoFlush: Boolean get() = true

        override val isClosedForWrite: Boolean get() = closed != null

        override val closedCause: Throwable? get() = closed

        override val writeBuffer: Sink
            get() {
                closed?.let { throw it }
                return pending
            }

        override suspend fun flush() {
            closed?.let { throw it }
            while (!pending.exhausted()) {
                appOut.clear()
                pending.readAtMostTo(appOut)
                appOut.flip()
                wrapAndSend(appOut)
            }
        }

        override suspend fun flushAndClose() {
            try {
                flush()
                engine.closeOutbound()
                wrapAndSend(EMPTY) // close_notify
            } finally {
                closed = closed ?: ClosedByteChannelException()
                rawWrite.flushAndClose()
            }
        }

        override fun cancel(cause: Throwable?) {
            if (closed != null) return
            closed = when (cause) {
                is CancellationException -> CancellationException(cause.message, cause)
                else -> ClosedByteChannelException(cause)
            }
            rawWrite.cancel(cause)
        }
    }

    companion object {
        // What one read from the socket may bring: several records' worth.
        private const val NET_BUFFER_SIZE = 64 * 1024

        // TLS's largest record of plaintext (RFC 8446 §5.1).
        private const val MAX_PLAINTEXT_RECORD = 16 * 1024

        // A record's header: its type, version and length (RFC 8446 §5.1).
        private const val RECORD_HEADER = 5

        private val EMPTY: ByteBuffer = ByteBuffer.allocate(0)

        // What closing queues is an alert or two: more wraps than this would be an engine that never says it is done.
        private const val MAX_CLOSING_WRAPS = 4

        // TLS 1.2 and 1.3 only, as Ktor's spoke 1.2: never down to 1.0 or 1.1 with an old server.
        private val PROTOCOLS = setOf("TLSv1.3", "TLSv1.2")

        /** [buffer] with its content, in a buffer of at least [size] bytes and twice its capacity. */
        private fun grow(buffer: ByteBuffer, size: Int): ByteBuffer =
            ByteBuffer.allocate(maxOf(size, buffer.capacity() * 2)).put(buffer.flip())

        /**
         * TLS to [host]:[port] over [rawRead] and [rawWrite], after its handshake. With [verifyHostname] the certificate
         * must name [host] ([HostnameVerifyingTrustManager]) and a host name is sent as the server name (SNI); without it
         * the certificate only has to be trusted, and no server name is sent.
         */
        suspend fun connect(
            host: String,
            port: Int,
            verifyHostname: Boolean,
            rawRead: BulkReadChannel,
            rawWrite: ByteWriteChannel,
        ): PlatformTls {
            val trustManager: X509TrustManager = HostnameVerifyingTrustManager.platformDefault()
                .let { if (verifyHostname) HostnameVerifyingTrustManager(it, host) else it }
            val context = SSLContext.getInstance("TLS").apply { init(null, arrayOf(trustManager), null) }
            val name = host.trimEnd('.')
            // An engine given no host sends no server name; one given an address sends none either (RFC 6066 §3).
            val engine = if (verifyHostname) context.createSSLEngine(name, port) else context.createSSLEngine()
            engine.apply {
                useClientMode = true
                val protocols = supportedProtocols.filter { it in PROTOCOLS }
                if (protocols.isNotEmpty()) enabledProtocols = protocols.toTypedArray()
                // The JDK and Conscrypt send a name without a dot ("localhost") only if told to.
                if (verifyHostname && ipAddress(host) == null) {
                    serverName(name)?.let { sni -> sslParameters = sslParameters.apply { serverNames = listOf(sni) } }
                }
            }
            val tls = PlatformTls(engine, rawRead, rawWrite)
            try {
                tls.handshake()
            } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
                tls.release()
                throw e
            }
            return tls
        }

        // A name SNI can't carry (an underscore, say) is sent as none, as the platforms do for one they can't.
        private fun serverName(name: String): SNIHostName? = try {
            SNIHostName(name)
        } catch (_: IllegalArgumentException) {
            null
        }
    }
}
