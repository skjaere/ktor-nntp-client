package io.skjaere.nntp

import io.ktor.utils.io.*

data class NntpResponse(
    val code: Int,
    val message: String
)

data class ArticleResponse(
    val code: Int,
    val message: String,
    val articleNumber: Long,
    val messageId: String,
    val content: List<String>
)

data class StatResponse(
    val code: Int,
    val message: String,
    val articleNumber: Long,
    val messageId: String
)

data class GroupResponse(
    val code: Int,
    val message: String,
    val count: Long,
    val low: Long,
    val high: Long,
    val name: String
)

data class ListGroupResponse(
    val code: Int,
    val message: String,
    val count: Long,
    val low: Long,
    val high: Long,
    val name: String,
    val articleNumbers: List<Long>
)

data class YencBodyResult(
    val code: Int,
    val message: String,
    val yencHeaders: YencHeaders,
    val data: ByteReadChannel
)

internal fun parseResponseLine(line: String): NntpResponse {
    if (line.length < 3) {
        throw NntpProtocolException("Invalid response line: $line")
    }
    val code = line.substring(0, 3).toIntOrNull()
        ?: throw NntpProtocolException("Invalid response code: $line")
    val message = if (line.length > 4) line.substring(4) else ""
    return NntpResponse(code, message)
}

internal fun parseArticleResponseLine(line: String): Triple<Int, Long, String> {
    val parts = line.split(" ", limit = 4)
    val code = parts[0].toIntOrNull()
        ?: throw NntpProtocolException("Invalid response code: $line")
    val number = parts.getOrNull(1)?.toLongOrNull() ?: 0L
    val messageId = parts.getOrNull(2) ?: ""
    return Triple(code, number, messageId)
}

internal fun parseGroupResponseLine(line: String): GroupResponse {
    val parts = line.split(" ", limit = 5)
    val code = parts[0].toIntOrNull()
        ?: throw NntpProtocolException("Invalid response code: $line")
    val count = parts.getOrNull(1)?.toLongOrNull() ?: 0L
    val low = parts.getOrNull(2)?.toLongOrNull() ?: 0L
    val high = parts.getOrNull(3)?.toLongOrNull() ?: 0L
    val name = parts.getOrNull(4) ?: ""
    return GroupResponse(code, line.substringAfter(" "), count, low, high, name)
}
