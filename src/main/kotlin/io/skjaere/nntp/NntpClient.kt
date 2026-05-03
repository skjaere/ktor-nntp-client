package io.skjaere.nntp

import io.ktor.network.selector.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import java.io.Closeable

class NntpClient(
    val connection: NntpConnection
) : Closeable {

    companion object {
        private const val DEFAULT_QUIT_TIMEOUT_MS = 1000L

        suspend fun connect(
            host: String,
            port: Int,
            selectorManager: SelectorManager,
            useTls: Boolean = false
        ): NntpClient {
            val connection = NntpConnection.connect(host, port, selectorManager, useTls)
            return NntpClient(connection)
        }

        suspend fun connect(
            host: String,
            port: Int,
            selectorManager: SelectorManager,
            useTls: Boolean = false,
            username: String,
            password: String
        ): NntpClient {
            val client = connect(host, port, selectorManager, useTls)
            // The TCP+TLS+welcome handshake just succeeded; if AUTH fails (e.g. server
            // returns 502 "Too many connections" on AUTHINFO PASS) we own a live socket
            // that nothing else holds a reference to. Without this catch the JVM hangs
            // onto the FD until GC — exactly the leak shape the single-socket-for-life
            // refactor was meant to eliminate.
            try {
                client.authenticate(username, password)
            } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
                runCatching { client.close() }
                throw e
            }
            return client
        }
    }

    // --- Session commands ---

    suspend fun capabilities(): List<String> {
        val (response, lines) = connection.commandMultiLine(NntpCommand.CAPABILITIES)
        if (response.code != 101) {
            throw NntpProtocolException("CAPABILITIES failed: ${response.code} ${response.message}", response)
        }
        return lines
    }

    suspend fun modeReader(): NntpResponse {
        return connection.command(NntpCommand.MODE_READER)
    }

    suspend fun authenticate(username: String, password: String): NntpResponse {
        return connection.authenticate(username, password)
    }

    suspend fun quit(): NntpResponse {
        return connection.command(NntpCommand.QUIT)
    }

    // --- Article retrieval ---

    suspend fun article(messageId: String): ArticleResponse {
        return fetchArticle(NntpCommand.article(messageId), 220)
    }

    suspend fun article(number: Long): ArticleResponse {
        return fetchArticle(NntpCommand.article(number), 220)
    }

    suspend fun head(messageId: String): ArticleResponse {
        return fetchArticle(NntpCommand.head(messageId), 221)
    }

    suspend fun head(number: Long): ArticleResponse {
        return fetchArticle(NntpCommand.head(number), 221)
    }

    suspend fun body(messageId: String): ArticleResponse {
        return fetchArticle(NntpCommand.body(messageId), 222)
    }

    suspend fun body(number: Long): ArticleResponse {
        return fetchArticle(NntpCommand.body(number), 222)
    }

    suspend fun stat(messageId: String): StatResult {
        return fetchStat(NntpCommand.stat(messageId))
    }

    suspend fun stat(number: Long): StatResult {
        return fetchStat(NntpCommand.stat(number))
    }

    suspend fun next(): StatResult {
        return fetchStat(NntpCommand.NEXT)
    }

    suspend fun last(): StatResult {
        return fetchStat(NntpCommand.LAST)
    }

    // --- Yenc body ---

    fun bodyYenc(messageId: String): Flow<YencEvent> =
        fetchBodyYenc(NntpCommand.body(messageId))

    fun bodyYenc(number: Long): Flow<YencEvent> =
        fetchBodyYenc(NntpCommand.body(number))

    // --- Group selection ---

    suspend fun group(name: String): GroupResponse {
        val response = connection.command(NntpCommand.group(name))
        if (response.code != 211) {
            throw NntpProtocolException("GROUP failed: ${response.code} ${response.message}", response)
        }
        return parseGroupResponseLine("${response.code} ${response.message}")
    }

    suspend fun listGroup(name: String? = null, range: LongRange? = null): ListGroupResponse {
        val (response, lines) = connection.commandMultiLine(NntpCommand.listGroup(name, range))
        if (response.code != 211) {
            throw NntpProtocolException("LISTGROUP failed: ${response.code} ${response.message}", response)
        }
        val groupResponse = parseGroupResponseLine("${response.code} ${response.message}")
        val articleNumbers = lines.mapNotNull { it.toLongOrNull() }
        return ListGroupResponse(
            code = groupResponse.code,
            message = groupResponse.message,
            count = groupResponse.count,
            low = groupResponse.low,
            high = groupResponse.high,
            name = groupResponse.name,
            articleNumbers = articleNumbers
        )
    }

    // --- Listings ---

    suspend fun list(keyword: String? = null, vararg arguments: String): List<String> {
        val (response, lines) = connection.commandMultiLine(NntpCommand.list(keyword, *arguments))
        if (response.code != 215) {
            throw NntpProtocolException("LIST failed: ${response.code} ${response.message}", response)
        }
        return lines
    }

    suspend fun newGroups(date: String, time: String, gmt: Boolean = false): List<String> {
        val (response, lines) = connection.commandMultiLine(NntpCommand.newGroups(date, time, gmt))
        if (response.code != 231) {
            throw NntpProtocolException("NEWGROUPS failed: ${response.code} ${response.message}", response)
        }
        return lines
    }

    suspend fun newNews(wildmat: String, date: String, time: String, gmt: Boolean = false): List<String> {
        val (response, lines) = connection.commandMultiLine(NntpCommand.newNews(wildmat, date, time, gmt))
        if (response.code != 230) {
            throw NntpProtocolException("NEWNEWS failed: ${response.code} ${response.message}", response)
        }
        return lines
    }

    // --- Overview ---

    suspend fun over(range: LongRange): List<String> {
        val (response, lines) = connection.commandMultiLine(NntpCommand.over(range))
        if (response.code != 224) {
            throw NntpProtocolException("OVER failed: ${response.code} ${response.message}", response)
        }
        return lines
    }

    suspend fun over(messageId: String): List<String> {
        val (response, lines) = connection.commandMultiLine(NntpCommand.over(messageId))
        if (response.code != 224) {
            throw NntpProtocolException("OVER failed: ${response.code} ${response.message}", response)
        }
        return lines
    }

    suspend fun xover(range: LongRange): List<String> {
        val (response, lines) = connection.commandMultiLine(NntpCommand.xover(range))
        if (response.code != 224) {
            throw NntpProtocolException("XOVER failed: ${response.code} ${response.message}", response)
        }
        return lines
    }

    suspend fun hdr(field: String, range: LongRange): List<String> {
        val (response, lines) = connection.commandMultiLine(NntpCommand.hdr(field, range))
        if (response.code != 225) {
            throw NntpProtocolException("HDR failed: ${response.code} ${response.message}", response)
        }
        return lines
    }

    suspend fun hdr(field: String, messageId: String): List<String> {
        val (response, lines) = connection.commandMultiLine(NntpCommand.hdr(field, messageId))
        if (response.code != 225) {
            throw NntpProtocolException("HDR failed: ${response.code} ${response.message}", response)
        }
        return lines
    }

    suspend fun xhdr(field: String, range: LongRange): List<String> {
        val (response, lines) = connection.commandMultiLine(NntpCommand.xhdr(field, range))
        if (response.code != 221) {
            throw NntpProtocolException("XHDR failed: ${response.code} ${response.message}", response)
        }
        return lines
    }

    // --- Posting ---

    suspend fun post(article: String): NntpResponse =
        sendArticle(NntpCommand.POST, expectedContinueCode = 340, label = "POST", article = article)

    suspend fun ihave(messageId: String, article: String): NntpResponse =
        sendArticle(NntpCommand.ihave(messageId), expectedContinueCode = 335, label = "IHAVE", article = article)

    private suspend fun sendArticle(
        cmd: String,
        expectedContinueCode: Int,
        label: String,
        article: String
    ): NntpResponse {
        val initialResponse = connection.commandRaw(cmd)
        if (initialResponse.code != expectedContinueCode) {
            throw NntpProtocolException(
                "$label failed: ${initialResponse.code} ${initialResponse.message}",
                initialResponse
            )
        }
        connection.writeLine(dotStuff(article))
        connection.writeLine(".")
        return connection.readResponse()
    }

    // RFC 3977 §3.1.1: any line in a multi-line data block that begins with "." must
    // be prefixed with an additional "." so the server's unstuff pass recovers the
    // original bytes and does not mistake an in-body "." for the block terminator.
    private fun dotStuff(article: String): String =
        article.split("\r\n").joinToString("\r\n") { if (it.startsWith(".")) ".$it" else it }

    // --- Server info ---

    suspend fun help(): List<String> {
        val (response, lines) = connection.commandMultiLine(NntpCommand.HELP)
        if (response.code != 100) {
            throw NntpProtocolException("HELP failed: ${response.code} ${response.message}", response)
        }
        return lines
    }

    suspend fun date(): NntpResponse {
        return connection.command(NntpCommand.DATE)
    }

    override fun close() {
        connection.close()
    }

    /** Graceful close: send QUIT (bounded by [timeoutMs]) before tearing the socket down. */
    suspend fun shutdown(timeoutMs: Long = DEFAULT_QUIT_TIMEOUT_MS) {
        connection.shutdown(timeoutMs)
    }

    // --- Private helpers ---

    private suspend fun fetchArticle(cmd: String, expectedCode: Int): ArticleResponse {
        val (response, lines) = connection.commandMultiLine(cmd)
        if (response.code != expectedCode) {
            if (response.code == 430) {
                throw ArticleNotFoundException(
                    "Article not found: ${response.code} ${response.message}",
                    response
                )
            }
            throw NntpProtocolException(
                "Command failed: ${response.code} ${response.message}",
                response
            )
        }
        val (code, number, messageId) = parseArticleResponseLine("${response.code} ${response.message}")
        return ArticleResponse(code, response.message, number, messageId, lines)
    }

    private suspend fun fetchStat(cmd: String): StatResult {
        val response = connection.command(cmd)
        if (response.code == 430) {
            return StatResult.NotFound(response.code, response.message)
        }
        if (response.code != 223) {
            throw NntpProtocolException(
                "Command failed: ${response.code} ${response.message}",
                response
            )
        }
        val (code, number, messageId) = parseArticleResponseLine("${response.code} ${response.message}")
        return StatResult.Found(code, response.message, number, messageId)
    }

    private fun fetchBodyYenc(cmd: String): Flow<YencEvent> = channelFlow {
        // Set the flag BEFORE issuing the command. Once BODY is on the wire the server
        // WILL send a response (222+body, or 430/5xx single line); if a cancellation
        // lands between this writeLine and the readResponse below, we still have to
        // tell the drain that the wire is dirty. Setting the flag only AFTER receiving
        // 222 was a real bug — the cancellation between commandRaw returning and the
        // flag-set was a tight but reachable window in production.
        connection.markBodyInProgress()
        val response = connection.commandRaw(cmd)
        if (response.code != 222) {
            // Non-222 means the server has finished responding with a single line and
            // the wire is now clean. Clear the flag so the next op's drain (or
            // subsequent commandRaw) sees a clean state. Then surface the protocol
            // error.
            connection.markBodyConsumed()
            if (response.code == 430) {
                throw ArticleNotFoundException(
                    "Article not found: ${response.code} ${response.message}",
                    response
                )
            }
            throw NntpProtocolException(
                "BODY failed: ${response.code} ${response.message}",
                response
            )
        }
        // 222 received — body bytes about to flow. The matching markBodyConsumed runs
        // inside the YencDecoder writer once the article terminator is read off the
        // wire (NOT here, because decode returns as soon as it has sent the Body event
        // while the writer is still streaming body bytes asynchronously).
        with(YencDecoder) { decode(connection) }
    }
}
