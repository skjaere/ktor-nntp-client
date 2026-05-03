package io.skjaere.nntp

import io.ktor.utils.io.*
import io.skjaere.yenc.RapidYenc
import io.skjaere.yenc.RapidYencDecoderEnd
import io.skjaere.yenc.RapidYencDecoderState
import kotlinx.coroutines.channels.ProducerScope

internal object YencDecoder {
    private const val BUFFER_SIZE = 131072

    /**
     * Stream a yenc-encoded BODY response into the producer scope. Caller has exclusive
     * ownership of [connection] for the whole BODY operation (one [NntpClientPool.withClient]
     * = one connection lifetime). If anything throws — protocol error, EOF mid-stream,
     * cancellation by the consumer — the exception propagates up; the pool sees the
     * throw, doesn't return the connection to the pool, and discards it.
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

    private fun ProducerScope<YencEvent>.launchWriterJob(
        connection: NntpConnection,
        firstDataBytes: ByteArray?
    ): WriterJob = writer(autoFlush = true) {
        val buffer = ByteArray(BUFFER_SIZE)
        var decoderState = RapidYencDecoderState.CRLF
        var crc = 0u
        var pendingBytes: ByteArray? = firstDataBytes

        while (true) {
            @Suppress("UNNECESSARY_NOT_NULL_ASSERTION")
            val rawChunk: ByteArray = if (pendingBytes != null) {
                val tmp = pendingBytes!!
                pendingBytes = null
                tmp
            } else {
                val bytesRead = connection.rawReadChannel.readAvailable(buffer)
                if (bytesRead == -1) break
                buffer.copyOf(bytesRead)
            }

            val result = RapidYenc.decodeIncremental(rawChunk, decoderState)

            if (result.data.isNotEmpty()) {
                channel.writeFully(result.data)
                crc = RapidYenc.crc32(result.data, initCrc = crc)
            }

            decoderState = result.state

            when (result.end) {
                RapidYencDecoderEnd.CONTROL -> {
                    // Found =yend line — parse trailer for CRC validation
                    val remaining = rawChunk.copyOfRange(
                        result.bytesConsumed.toInt(), rawChunk.size
                    )
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
                    drainUntilArticleEnd(connection, remaining)
                    // Wire is now line-protocol clean — flag the connection reusable.
                    // Done HERE rather than in fetchBodyYenc because the decoder body
                    // returns as soon as it has sent the Body event, while the writer
                    // (this coroutine) is still pumping body bytes off the wire. Flipping
                    // the flag in fetchBodyYenc therefore cleared it before the body
                    // was actually drained, and a cancellation in that window left
                    // yenc bytes pending for the next caller.
                    connection.markBodyConsumed()

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
                    // Continue reading
                }
            }
        }
    }

    private suspend fun drainUntilArticleEnd(
        connection: NntpConnection,
        alreadyRead: ByteArray
    ) {
        // Check if the article terminator is already in what we've read
        val str = String(alreadyRead, Charsets.ISO_8859_1)
        if (str.contains("\r\n.\r\n") || str.endsWith("\r\n.\r\n")) {
            return
        }
        // Continue reading until we find the article end marker
        val buffer = ByteArray(BUFFER_SIZE)
        val accumulator = StringBuilder(str)
        while (true) {
            val bytesRead = connection.rawReadChannel.readAvailable(buffer)
            if (bytesRead == -1) break
            accumulator.append(String(buffer, 0, bytesRead, Charsets.ISO_8859_1))
            if (accumulator.contains("\r\n.\r\n")) break
        }
    }
}
