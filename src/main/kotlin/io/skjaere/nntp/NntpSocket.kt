package io.skjaere.nntp

import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.ClosedByteChannelException
import io.ktor.utils.io.InternalAPI
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.io.readAtMostTo
import kotlinx.io.transferFrom
import java.io.IOException
import java.net.InetSocketAddress
import java.net.StandardSocketOptions
import java.nio.ByteBuffer
import java.nio.channels.CancelledKeyException
import java.nio.channels.ClosedSelectorException
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.SocketChannel
import kotlin.coroutines.ContinuationInterceptor
import kotlin.coroutines.CoroutineContext

/**
 * A TCP connection read and written in its caller's own thread. A read takes what the socket holds, and one that finds
 * nothing waits there, on a selector of the connection's own, until bytes come, the read timeout runs out, the caller's
 * coroutine is cancelled (what comes later, and what a read had taken when its caller was cancelled, is the next read's,
 * a drain's) or the connection is closed.
 *
 * Ktor's socket put a coroutine between the socket and the reader, woken by its selector's thread: every burst of bytes
 * (8 KB at most) cost a selector wake-up and two dispatches, and the reader's coroutine was resumed last, with a timer
 * task per read for the read timeout (library issue K42). Here the thread the kernel wakes is the reader's.
 *
 * A wait blocks its thread, so reads, writes and the connect run on [NntpDispatchers.IO], the library's view of
 * Dispatchers.IO: in place for a caller on it, else moved there. A caller on another dispatcher (Main, runBlocking's, a
 * test's, Dispatchers.IO itself) never has its thread blocked, nor a coroutine sharing that thread kept waiting, and
 * Dispatchers.IO's 64 threads are never held by waits. A coroutine started UNDISPATCHED runs on its starter's thread
 * until it first suspends, so one that reads at once holds its starter while it waits: it should yield first, as the
 * pool's drain does.
 */
internal class NntpSocket private constructor(
    private val channel: SocketChannel,
    private val selector: Selector,
    private val key: SelectionKey,
    private val host: String,
    private val port: Int,
    private val readTimeoutMs: Long,
) {
    val read: Reader = Reader()
    val write: ByteWriteChannel = Writer()

    // Ends a wait early, for a cancellation or a close. One for the connection, not one per wait.
    private val wake: (Throwable?) -> Unit = { runCatching { selector.wakeup() } }

    /**
     * Waits in this thread until the socket is ready for [ops], for [timeoutMs] at most (0: no limit): false when the
     * time ran out. Ends early with [closed]'s cause once there is one, or with [job]'s cancellation.
     */
    @OptIn(InternalCoroutinesApi::class)
    private fun awaitReady(ops: Int, job: Job?, timeoutMs: Long, closed: () -> Throwable?): Boolean {
        val handle = job?.invokeOnCompletion(onCancelling = true, invokeImmediately = true, handler = wake)
        try {
            if (key.interestOps() != ops) key.interestOps(ops)
            val deadline = if (timeoutMs > 0) System.nanoTime() + timeoutMs * NANOS_PER_MS else 0L
            while (true) {
                closed()?.let { throw it }
                job?.ensureActive()
                val waitMs = if (deadline == 0L) {
                    0L
                } else {
                    val left = deadline - System.nanoTime()
                    if (left <= 0) return false
                    (left + NANOS_PER_MS - 1) / NANOS_PER_MS
                }
                if (selector.select(waitMs) > 0) {
                    selector.selectedKeys().clear()
                    return true
                }
            }
        } catch (e: ClosedSelectorException) {
            throw closed() ?: e
        } catch (e: CancelledKeyException) {
            throw closed() ?: e
        } finally {
            handle?.dispose()
        }
    }

    /** Closes the socket: a read or write waiting on it ends with the cause its channel was cancelled with first. */
    fun close() {
        wake(null)
        runCatching { channel.close() }
        // After the channel: on the JVM and Android its descriptor is let go when the selector deregisters it.
        runCatching { selector.close() }
    }

    /** What the server sent: read from the socket as its reader asks for more, in the reader's own thread. */
    @OptIn(InternalAPI::class)
    inner class Reader : BulkReadChannel {
        // What was read from the socket and not yet taken, as Ktor's channel kept it.
        private val buffer = Buffer()

        // A read for [buffer]: what the socket holds, up to a burst's worth, in one call.
        private val socketIn: ByteBuffer = ByteBuffer.allocateDirect(READ_BUFFER_SIZE)

        // The end of the stream: the server closed its side.
        private var ended = false

        @Volatile
        private var closed: Throwable? = null

        private val closedNow: () -> Throwable? = { closed }

        override val closedCause: Throwable? get() = closed

        override val isClosedForRead: Boolean get() = closed != null || (ended && buffer.exhausted())

        override val readBuffer: Source
            get() {
                closed?.let { throw it }
                return buffer
            }

        override suspend fun awaitContent(min: Int): Boolean {
            closed?.let { throw it }
            while (buffer.size < min && !ended) {
                socketIn.clear()
                if (read(socketIn) < 0) {
                    ended = true
                } else {
                    socketIn.flip()
                    buffer.transferFrom(socketIn)
                }
            }
            return buffer.size >= min
        }

        /**
         * Reads into [dst] all this channel holds, or else what the socket has, waiting for it if it has nothing: the
         * bytes read, or -1 at the end of the stream. A bulk reader's way past [buffer]: the socket's bytes go straight
         * into [dst] (a decoder's direct buffer, TLS's record buffer), with no copy on the way.
         */
        override suspend fun readAvailable(dst: ByteBuffer, want: Int): Int {
            closed?.let { throw it }
            if (!buffer.exhausted()) return buffer.drainInto(dst)
            if (ended) return -1
            return read(dst, want).also { if (it < 0) ended = true }
        }

        private suspend fun read(dst: ByteBuffer, want: Int = 0): Int {
            val context = currentCoroutineContext()
            if (context.onIo()) return readSocket(dst, context[Job], want)
            val start = dst.position()
            return try {
                withContext(NntpDispatchers.IO) { readSocket(dst, coroutineContext[Job], want) }
            } catch (e: CancellationException) {
                // Cancelled once the hop had read, withContext drops what it returned, but the bytes are out of the
                // socket: they may hold an answer's end, which the drain after a cancel reads for.
                keep(dst, start)
                throw e
            }
        }

        /** Moves what a read put in [dst] from [start] on to [buffer], for the next read to take first, as if never read. */
        private fun keep(dst: ByteBuffer, start: Int) {
            val end = dst.position()
            if (end <= start) return
            buffer.transferFrom(dst.duplicate().position(start).limit(end))
            dst.position(start)
        }

        /**
         * What the socket holds, into [dst], waiting for some if it holds none: the bytes read, or -1 at its end. Fewer
         * than [want] in one go, and more are let gather ([gatherMore]).
         */
        private fun readSocket(dst: ByteBuffer, job: Job?, want: Int): Int {
            // Before anything is taken: a read that finds bytes waits for nothing, and would not see a cancel otherwise.
            job?.ensureActive()
            while (true) {
                val n = readNow(dst)
                if (n > 0) return if (n < want) gatherMore(dst, n) else n
                if (n < 0 || !dst.hasRemaining()) return n
                if (!awaitReady(SelectionKey.OP_READ, job, readTimeoutMs, closedNow)) {
                    // A server gone silent mid-answer leaves the wire unusable: this read and every later one fail.
                    val timedOut = NntpConnectionException("No data from $host:$port for ${readTimeoutMs}ms")
                    if (closed == null) closed = timedOut
                    throw timedOut
                }
            }
        }

        /**
         * After [got] bytes, fewer than the reader wants: waits [GATHER_MS] for more to come, then takes them, so one
         * wake-up brings what a millisecond's packets do (each woke the reader on its own on the emulator, 1.5 KB). The
         * bytes in all; an end seen meanwhile shows at the next read. Only a close ends the wait early: the bytes taken
         * may hold the end of the answer, and a cancel thrown now would leave them unseen. The next read sees it.
         */
        private fun gatherMore(dst: ByteBuffer, got: Int): Int {
            if (!dst.hasRemaining()) return got
            awaitReady(NO_OPS, null, GATHER_MS, closedNow)
            val more = readNow(dst)
            return if (more > 0) got + more else got
        }

        private fun readNow(dst: ByteBuffer): Int = try {
            channel.read(dst)
        } catch (e: IOException) {
            // Closed under the read (AsynchronousCloseException): the close's cause says why.
            throw closed ?: e
        }

        override fun cancel(cause: Throwable?) {
            if (closed != null) return
            closed = when (cause) {
                is CancellationException -> CancellationException(cause.message, cause)
                else -> ClosedByteChannelException(cause)
            }
            wake(null)
        }
    }

    /** Sends what is written to it at each flush, as autoFlush has it do, waiting in the writer's thread if need be. */
    @OptIn(InternalAPI::class)
    private inner class Writer : ByteWriteChannel {
        private val pending = Buffer()
        private val socketOut: ByteBuffer = ByteBuffer.allocateDirect(WRITE_BUFFER_SIZE)

        @Volatile
        private var closed: Throwable? = null

        private val closedNow: () -> Throwable? = { closed }

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
            if (pending.exhausted()) return
            val context = currentCoroutineContext()
            if (context.onIo()) send(context[Job]) else withContext(NntpDispatchers.IO) { send(coroutineContext[Job]) }
        }

        private fun send(job: Job?) {
            while (!pending.exhausted()) {
                socketOut.clear()
                pending.readAtMostTo(socketOut)
                socketOut.flip()
                while (socketOut.hasRemaining()) {
                    val n = try {
                        channel.write(socketOut)
                    } catch (e: IOException) {
                        throw closed ?: e
                    }
                    // A server that doesn't read waits as long as it takes, as Ktor's writer did.
                    if (n == 0) awaitReady(SelectionKey.OP_WRITE, job, 0, closedNow)
                }
            }
        }

        override suspend fun flushAndClose() {
            try {
                flush()
                runCatching { channel.shutdownOutput() }
            } finally {
                closed = closed ?: ClosedByteChannelException()
            }
        }

        override fun cancel(cause: Throwable?) {
            if (closed != null) return
            closed = when (cause) {
                is CancellationException -> CancellationException(cause.message, cause)
                else -> ClosedByteChannelException(cause)
            }
            wake(null)
        }
    }

    companion object {
        // What one read for a line-by-line reader may take: a burst's worth.
        private const val READ_BUFFER_SIZE = 64 * 1024

        // Commands, and TLS records of up to 16 KB.
        private const val WRITE_BUFFER_SIZE = 32 * 1024

        private const val NANOS_PER_MS = 1_000_000L

        // How long a read that brought fewer bytes than its reader wants lets more gather ([Reader.gatherMore]).
        private const val GATHER_MS = 1L

        // Interest in nothing: a wait that only the time or a close ends.
        private const val NO_OPS = 0

        /** Where a wait may block its thread: the library's own view of Dispatchers.IO, whose threads are for it. */
        private fun CoroutineContext.onIo(): Boolean = this[ContinuationInterceptor] === NntpDispatchers.IO

        /**
         * Connects to [host]:[port] (a name resolved here, as Ktor's socket did), waiting for the connect in an IO
         * thread until it succeeds, fails, or the caller is cancelled. [readTimeoutMs] bounds every wait for bytes later.
         */
        suspend fun connect(host: String, port: Int, readTimeoutMs: Long): NntpSocket {
            val context = currentCoroutineContext()
            if (context.onIo()) return open(host, port, readTimeoutMs, context[Job])
            var opened: NntpSocket? = null
            try {
                return withContext(NntpDispatchers.IO) { open(host, port, readTimeoutMs, coroutineContext[Job]).also { opened = it } }
            } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
                // Cancelled once the hop had connected, withContext drops the socket it returns: nobody else could close it.
                opened?.close()
                throw e
            }
        }

        private fun open(host: String, port: Int, readTimeoutMs: Long, job: Job?): NntpSocket {
            val channel = SocketChannel.open()
            var selector: Selector? = null
            try {
                channel.configureBlocking(false)
                // Commands are a line each and answered before the next: none waits to fill a packet.
                channel.setOption(StandardSocketOptions.TCP_NODELAY, true)
                selector = Selector.open()
                val key = channel.register(selector, SelectionKey.OP_CONNECT)
                val socket = NntpSocket(channel, selector, key, host, port, readTimeoutMs)
                if (!channel.connect(InetSocketAddress(host, port))) {
                    while (!channel.finishConnect()) socket.awaitReady(SelectionKey.OP_CONNECT, job, 0) { null }
                }
                key.interestOps(SelectionKey.OP_READ)
                return socket
            } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
                runCatching { channel.close() }
                selector?.let { runCatching { it.close() } }
                throw e
            }
        }
    }
}
