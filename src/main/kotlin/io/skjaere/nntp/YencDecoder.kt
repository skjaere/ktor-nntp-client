package io.skjaere.nntp

import io.ktor.utils.io.*
import io.skjaere.yenc.RapidYenc
import io.skjaere.yenc.RapidYencDecoderEnd
import io.skjaere.yenc.RapidYencDecoderState
import kotlinx.coroutines.CoroutineScope
import java.io.Closeable

internal class YencDecoder(
    private val scope: CoroutineScope
) {
    companion object {
        private const val BUFFER_SIZE = 16384
    }

    suspend fun decode(
        connection: NntpConnection,
        response: NntpResponse
    ): YencBodyResult {
        // Read =ybegin line (ASCII-safe, so readLine is fine)
        val ybeginLine = connection.readLine()
        if (!ybeginLine.startsWith("=ybegin ")) {
            connection.commandMutex.unlock()
            throw YencDecodingException("Expected =ybegin line, got: $ybeginLine")
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

        val writerJob = ChannelWriterJob(scope, connection, firstDataBytes)

        return YencBodyResult(
            code = response.code,
            message = response.message,
            yencHeaders = headers,
            data = writerJob.channel
        )
    }

    private class ChannelWriterJob(
        scope: CoroutineScope,
        connection: NntpConnection,
        firstDataBytes: ByteArray?
    ) {
        val channel: ByteReadChannel

        init {
            val writerJob = scope.writer(autoFlush = true) {
                var completed = false
                Closeable {
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
            channel = writerJob.channel
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
}
