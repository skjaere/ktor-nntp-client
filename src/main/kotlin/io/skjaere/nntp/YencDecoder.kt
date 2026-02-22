package io.skjaere.nntp

import io.ktor.utils.io.*
import io.skjaere.yenc.RapidYenc
import io.skjaere.yenc.RapidYencDecoderEnd
import io.skjaere.yenc.RapidYencDecoderState
import kotlinx.coroutines.channels.ProducerScope
import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean

internal object YencDecoder {
    private const val BUFFER_SIZE = 131072

    suspend fun ProducerScope<YencEvent>.decode(
        connection: NntpConnection
    ) {
        var mutexHandedToWriter = false
        try {
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
                // This is the first line of encoded data - preserve raw bytes with CRLF
                firstDataBytes = nextLineBytes + byteArrayOf('\r'.code.toByte(), '\n'.code.toByte())
            }

            val headers = YencHeaders.parse(ybeginLine, ypartLine)
            send(YencEvent.Headers(headers))

            // Launch writer as child of this ProducerScope (= channelFlow's scope)
            // The flag tracks whether the writer's Closeable.use cleanup already ran.
            // invokeOnCompletion is a fallback for when the writer is cancelled before
            // its lambda starts (so Closeable.use never executes).
            val writerCleanedUp = AtomicBoolean(false)
            val writerJob = launchWriterJob(connection, firstDataBytes, writerCleanedUp)

            writerJob.job.invokeOnCompletion { cause ->
                if (cause != null && !writerCleanedUp.get()) {
                    connection.scheduleReconnect()
                    connection.commandMutex.unlock()
                }
            }

            mutexHandedToWriter = true
            send(YencEvent.Body(writerJob.channel))
        } finally {
            if (!mutexHandedToWriter) {
                connection.scheduleReconnect()
                connection.commandMutex.unlock()
            }
        }
    }

    private fun ProducerScope<YencEvent>.launchWriterJob(
        connection: NntpConnection,
        firstDataBytes: ByteArray?,
        cleanedUp: AtomicBoolean
    ): WriterJob = writer(autoFlush = true) {
        var completed = false
        Closeable {
            cleanedUp.set(true)
            if (!completed) {
                connection.scheduleReconnect()
            }
            connection.commandMutex.unlock()
        }.use {
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
                        // Found =yend line - parse trailer for CRC validation
                        val remaining = rawChunk.copyOfRange(
                            result.bytesConsumed.toInt(), rawChunk.size
                        )
                        val remainingStr = String(remaining, Charsets.ISO_8859_1)
                        val yendLine = remainingStr.lines()
                            .firstOrNull { it.startsWith("=yend") }
                        if (yendLine != null) {
                            val trailer = YencTrailer.parse(yendLine)
                            val expectedCrc = trailer.pcrc32 ?: trailer.crc32
                            if (expectedCrc != null && expectedCrc != crc) {
                                throw YencCrcMismatchException(
                                    "CRC mismatch: expected ${expectedCrc.toString(16)}, got ${crc.toString(16)}",
                                    expected = expectedCrc,
                                    actual = crc
                                )
                            }
                        }
                        // Drain remaining bytes until NNTP article terminator
                        drainUntilArticleEnd(connection, remaining)
                        completed = true
                        break
                    }

                    RapidYencDecoderEnd.ARTICLE -> {
                        // Found \r\n.\r\n - article end
                        completed = true
                        break
                    }

                    RapidYencDecoderEnd.NONE -> {
                        // Continue reading
                    }
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
