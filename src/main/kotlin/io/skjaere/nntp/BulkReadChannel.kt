package io.skjaere.nntp

import io.ktor.utils.io.ByteReadChannel
import kotlinx.io.Buffer
import kotlinx.io.readAtMostTo
import java.nio.ByteBuffer

/**
 * A connection's read channel that hands a bulk reader all it holds in one call. Ktor's readAvailable(ByteBuffer) takes
 * one segment of a channel's buffer, 8 KB at most, so a 16 KB TLS record came out in two reads, and the record buffer
 * TLS reads the socket into was filled 8 KB at a time.
 */
internal interface BulkReadChannel : ByteReadChannel {
    /**
     * Reads into [dst] all this channel holds, as much as [dst] has room for, waiting for some if it holds none: the
     * bytes read, or -1 at the end of the stream. When the socket brings fewer than [want] bytes, a reader that wants
     * that many before it does anything with them, more are let gather for a moment first ([NntpSocket.Reader]): a
     * wake-up then takes many packets rather than one or two.
     */
    suspend fun readAvailable(dst: ByteBuffer, want: Int = 0): Int
}

/** Moves what this buffer holds into [dst], as much as it has room for, segment after segment: the bytes moved. */
internal fun Buffer.drainInto(dst: ByteBuffer): Int {
    var moved = 0
    while (dst.hasRemaining() && !exhausted()) moved += readAtMostTo(dst)
    return moved
}
