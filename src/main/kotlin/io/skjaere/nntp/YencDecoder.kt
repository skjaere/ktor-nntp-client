package io.skjaere.nntp

import io.ktor.utils.io.*
import io.skjaere.yenc.RapidYenc
import io.skjaere.yenc.RapidYencDecoderEnd
import io.skjaere.yenc.RapidYencDecoderState
import java.nio.ByteBuffer
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.ProducerScope

internal object YencDecoder {
    private const val BUFFER_SIZE = 131072

    // Decode-side buffers are 2× socket buffer to absorb (a) a partial article line
    // carried across iterations after `srcBuf.compact()` plus a fresh `BUFFER_SIZE`
    // socket read, and (b) the worst-case decoded output (yEnc encoded ≈ source size).
    private const val DECODE_BUFFER_SIZE = BUFFER_SIZE * 2

    /**
     * Stream a yenc-encoded BODY response into the producer scope. Caller has exclusive
     * ownership of [connection] for the whole BODY operation (one [NntpClientPool.withClient]
     * = one connection lifetime). If the headers can't be read, the exception propagates
     * up and the pool discards the connection. A failure of the body pump (socket error,
     * the reader cancelling the Body channel) doesn't propagate: Ktor 3's writer keeps it
     * in the Body channel, where the reader sees it, and completes normally. The pump marks
     * the connection instead ([NntpConnection.markBodyFailed]), and the pool discards it.
     */
    suspend fun ProducerScope<YencEvent>.decode(
        connection: NntpConnection
    ) {
        // Skip blank lines before =ybegin (real NNTP servers send header/body separator)
        var ybeginLine: String
        while (true) {
            ybeginLine = connection.readLine()
            if (ybeginLine.startsWith("=ybegin ")) break
            if (ybeginLine.isNotBlank()) {
                throw YencDecodingException("Expected =ybegin line, got: $ybeginLine")
            }
        }

        // Read next line as raw bytes to avoid UTF-8 corruption of yenc data
        val nextLineBytes = connection.readRawLine()
        val nextLineStr = String(nextLineBytes, Charsets.ISO_8859_1)

        val ypartLine: String?
        val firstDataBytes: ByteArray?

        if (nextLineStr.startsWith("=ypart ")) {
            ypartLine = nextLineStr
            firstDataBytes = null
        } else {
            ypartLine = null
            // First line of encoded data — preserve raw bytes with CRLF
            firstDataBytes = nextLineBytes + byteArrayOf('\r'.code.toByte(), '\n'.code.toByte())
        }

        val headers = YencHeaders.parse(ybeginLine, ypartLine)
        send(YencEvent.Headers(headers))

        val writerJob = launchWriterJob(connection, firstDataBytes)
        send(YencEvent.Body(writerJob.channel))
    }

    /**
     * Read pump for one BODY response. Reuses three buffers across the article instead
     * of allocating per-chunk:
     *
     *   - [readBuf]  — heap ByteArray sized to one socket read; reused indefinitely
     *   - [srcBuf]   — direct ByteBuffer fed to the yEnc decoder; carries any unconsumed
     *                  tail across iterations via [java.nio.ByteBuffer.compact]
     *   - [destBuf]  — direct ByteBuffer where rapidyenc writes decoded output
     *   - [writeBuf] — heap ByteArray for the ktor channel write, sized to destBuf
     *
     * The previous implementation called [RapidYenc.decodeIncremental] with a [ByteArray]
     * source, which the wrapper then copied into a fresh JNA `Memory` (raw `malloc`)
     * twice per call (src + dest workspaces), plus three small pointer/state holders,
     * plus another `Memory` per [RapidYenc.crc32] call. That allocator pressure was
     * invisible to NMT and `jvm.buffer.memory.used` and accumulated in glibc's per-thread
     * arenas (untracked anon RSS that never got returned to the OS).
     *
     * Routing through the direct-`ByteBuffer` overloads keeps the workspace footprint
     * inside JVM-tracked direct memory (capped via `-XX:MaxDirectMemorySize` if needed)
     * and reuses the same backing native pages for every chunk.
     */
    private fun ProducerScope<YencEvent>.launchWriterJob(
        connection: NntpConnection,
        firstDataBytes: ByteArray?
    ): WriterJob = writer(autoFlush = true) {
        try {
            pump(connection, firstDataBytes)
        } catch (e: CancellationException) {
            // The pool's withClient drains the rest of the article after a cancellation.
            throw e
        } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
            // The writer only stores this in the Body channel, so mark the connection for
            // the pool to see.
            connection.markBodyFailed(e)
            throw e
        }
    }

    private suspend fun WriterScope.pump(connection: NntpConnection, firstDataBytes: ByteArray?) {
        val readBuf = ByteArray(BUFFER_SIZE)
        val writeBuf = ByteArray(DECODE_BUFFER_SIZE)
        val srcBuf = ByteBuffer.allocateDirect(DECODE_BUFFER_SIZE)
        val destBuf = ByteBuffer.allocateDirect(DECODE_BUFFER_SIZE)
        var decoderState = RapidYencDecoderState.CRLF
        var crc = 0u
        var eof = false

        firstDataBytes?.let { srcBuf.put(it) }

        while (true) {
            // Top up srcBuf from the socket if there's room and we haven't seen EOF yet.
            // We don't bypass the read on the iteration where firstDataBytes was seeded —
            // it's fine to read more on top of it. The decoder will consume what it can
            // and compact the rest.
            if (!eof && srcBuf.position() < srcBuf.capacity()) {
                val readLimit = minOf(readBuf.size, srcBuf.capacity() - srcBuf.position())
                val n = connection.readAvailable(readBuf, 0, readLimit)
                if (n == -1) {
                    eof = true
                    if (srcBuf.position() == 0) throw closedMidArticle()
                } else if (n > 0) {
                    srcBuf.put(readBuf, 0, n)
                } else {
                    // n == 0: nothing immediately available; if we have buffered data,
                    // proceed to decode it; otherwise keep waiting.
                    if (srcBuf.position() == 0) continue
                }
            }

            srcBuf.flip()
            destBuf.clear()
            val result = RapidYenc.decodeIncremental(srcBuf, destBuf, decoderState)

            if (result.bytesWritten > 0) {
                destBuf.flip()
                val outLen = destBuf.remaining()
                // CRC over the still-direct buffer (no copy), then drain into a heap
                // ByteArray for the ktor channel write — ktor 3.x's stable cross-version
                // write surface is ByteArray-based, and the per-iteration heap copy here
                // is reusable + bounded so it's cheap relative to what we removed.
                crc = RapidYenc.crc32(destBuf, length = outLen, initCrc = crc)
                destBuf.get(writeBuf, 0, outLen)
                channel.writeFully(writeBuf, 0, outLen)
                // Hand the reader each chunk as it's decoded. Ktor 3's writer ignores autoFlush,
                // and its channel flushes by itself only every 1 MiB, more than a whole article:
                // without this the reader gets nothing until the pump ends. Still suspends while
                // the reader is 1 MiB behind.
                channel.flush()
            }

            decoderState = result.state

            when (result.end) {
                RapidYencDecoderEnd.CONTROL -> {
                    // Found =yend line — parse trailer for CRC validation. The unconsumed
                    // tail of srcBuf (from its current position to limit) is the bytes
                    // after =yend, including potentially the trailer line and the article
                    // terminator. Pull them out as a ByteArray so the rest of this branch
                    // can stay character-oriented.
                    val remaining = ByteArray(srcBuf.remaining())
                    srcBuf.get(remaining)
                    val remainingStr = String(remaining, Charsets.ISO_8859_1)
                    val yendLine = remainingStr.lines()
                        .firstOrNull { it.startsWith("=yend") }
                    val crcMismatch = if (yendLine != null) {
                        val trailer = YencTrailer.parse(yendLine)
                        val expectedCrc = trailer.pcrc32 ?: trailer.crc32
                        if (expectedCrc != null && expectedCrc != crc) expectedCrc to crc
                        else null
                    } else null

                    // Drain remaining bytes until the NNTP article terminator BEFORE
                    // throwing. By the time we get here we already have everything we
                    // need to detect the CRC mismatch; deferring the throw until after
                    // the wire is clean means the connection stays reusable and we
                    // don't need the pool to discard a perfectly healthy socket just
                    // because a single article had a bad CRC. (The pool's catch-all in
                    // withClient will still discard if drain itself fails — it only
                    // gives up the discard if everything below this line succeeds.)
                    if (drainUntilArticleEnd(connection, remaining)) {
                        // Wire is now line-protocol clean — flag the connection reusable.
                        // Done HERE rather than in fetchBodyYenc because the decoder body
                        // returns as soon as it has sent the Body event, while the writer
                        // (this coroutine) is still pumping body bytes off the wire. Flipping
                        // the flag in fetchBodyYenc therefore cleared it before the body
                        // was actually drained, and a cancellation in that window left
                        // yenc bytes pending for the next caller.
                        connection.markBodyConsumed()
                    } else {
                        // The connection closed after =yend but before the terminator: the
                        // body is whole, so deliver it, but the pool must discard the connection.
                        connection.markBodyFailed(
                            NntpConnectionException("Connection closed before the article terminator")
                        )
                    }

                    crcMismatch?.let { (expected, actual) ->
                        throw YencCrcMismatchException(
                            "CRC mismatch: expected ${expected.toString(16)}, got ${actual.toString(16)}",
                            expected = expected,
                            actual = actual
                        )
                    }
                    break
                }

                RapidYencDecoderEnd.ARTICLE -> {
                    // Found \r\n.\r\n — article end. Wire is clean.
                    connection.markBodyConsumed()
                    break
                }

                RapidYencDecoderEnd.NONE -> {
                    // Neither =yend nor the terminator, and nothing more will come: ending
                    // normally would hand the reader a short body as if it were whole.
                    if (eof) throw closedMidArticle()
                    // Move any unconsumed bytes to the front so the next iteration
                    // can append a fresh socket read after them.
                    srcBuf.compact()
                }
            }
        }
    }

    // EOF mid-article: the server closed the connection, or under TLS anything that ended
    // it (Ktor reports a FIN, a reset and a missing close_notify alike as EOF).
    private fun closedMidArticle() =
        NntpConnectionException("Connection closed mid-article, before =yend or the article terminator")

    /** Reads up to the article terminator; false if the connection closed before it. */
    private suspend fun drainUntilArticleEnd(
        connection: NntpConnection,
        alreadyRead: ByteArray
    ): Boolean {
        // Check if the article terminator is already in what we've read
        val str = String(alreadyRead, Charsets.ISO_8859_1)
        if (str.contains("\r\n.\r\n") || str.endsWith("\r\n.\r\n")) {
            return true
        }
        // Continue reading until we find the article end marker
        val buffer = ByteArray(BUFFER_SIZE)
        val accumulator = StringBuilder(str)
        while (true) {
            val bytesRead = connection.readAvailable(buffer)
            if (bytesRead == -1) return false
            accumulator.append(String(buffer, 0, bytesRead, Charsets.ISO_8859_1))
            if (accumulator.contains("\r\n.\r\n")) return true
        }
    }
}
