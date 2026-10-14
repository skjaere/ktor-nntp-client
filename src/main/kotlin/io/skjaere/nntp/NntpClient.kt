package io.skjaere.nntp

import io.ktor.network.selector.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import java.io.Closeable

class NntpClient(
    val connection: NntpConnection
) : Closeable {

    companion object {
        private const val DEFAULT_QUIT_TIMEOUT_MS = 1000L

        // A lookup by an id that can't be one ([NntpCommand.isMessageId]) isn't sent: no article
        // has that id, so it gets the command's not-found outcome, as if the server said 430.
        private val NOT_A_MESSAGE_ID = NntpResponse(430, "Not a message-id, not sent")
        private val NOT_A_MESSAGE_ID_STAT = StatResult.NotFound(NOT_A_MESSAGE_ID.code, NOT_A_MESSAGE_ID.message)

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
        checkMessageId(messageId)
        return fetchArticle(NntpCommand.article(messageId), 220)
    }

    suspend fun article(number: Long): ArticleResponse {
        return fetchArticle(NntpCommand.article(number), 220)
    }

    suspend fun head(messageId: String): ArticleResponse {
        checkMessageId(messageId)
        return fetchArticle(NntpCommand.head(messageId), 221)
    }

    suspend fun head(number: Long): ArticleResponse {
        return fetchArticle(NntpCommand.head(number), 221)
    }

    suspend fun body(messageId: String): ArticleResponse {
        checkMessageId(messageId)
        return fetchArticle(NntpCommand.body(messageId), 222)
    }

    suspend fun body(number: Long): ArticleResponse {
        return fetchArticle(NntpCommand.body(number), 222)
    }

    suspend fun stat(messageId: String): StatResult {
        if (!NntpCommand.isMessageId(messageId)) return NOT_A_MESSAGE_ID_STAT
        return fetchStat(NntpCommand.stat(messageId))
    }

    /**
     * STATs many articles with pipelining (RFC 3977 §3.5): commands are sent in batches
     * with up to [window] awaiting an answer, and answers are read in order. One round
     * trip per batch instead of per article, for health checks over thousands of articles.
     *
     * Results are in the order of [messageIds]. A response other than 223/430 doesn't stop
     * the pipeline (its answers are still owed); the first such error is thrown at the end.
     *
     * [onResult] is called as each answer arrives, in order, with its index in [messageIds]:
     * for progress reporting, or to react to the first missing article without waiting for
     * the rest. It runs on the reading coroutine, so keep it cheap.
     *
     * An id that can't be a message-id ([NntpCommand.isMessageId]) is never sent: it gets a
     * [StatResult.NotFound] in its place, so it can't put extra commands on the wire and shift
     * the others' answers.
     */
    suspend fun statPipelined(
        messageIds: List<String>,
        window: Int = 64,
        onResult: ((index: Int, result: StatResult) -> Unit)? = null,
    ): List<StatResult> {
        require(window > 0) { "window must be > 0, got $window" }
        // Indices of the ids that go on the wire, in order; [sent] and [answered] count these.
        val toSend = messageIds.indices.filter { NntpCommand.isMessageId(messageIds[it]) }
        val results = ArrayList<StatResult>(messageIds.size)
        var sent = 0
        var answered = 0
        var firstError: NntpProtocolException? = null
        while (results.size < messageIds.size) {
            if (toSend.getOrNull(answered) != results.size) {
                onResult?.invoke(results.size, NOT_A_MESSAGE_ID_STAT)
                results += NOT_A_MESSAGE_ID_STAT
                continue
            }
            val outstanding = sent - answered
            // Top up once half the window has been answered: few large writes, never idle.
            if (sent < toSend.size && outstanding <= window / 2) {
                val lines = toSend.subList(sent, minOf(toSend.size, sent + window - outstanding))
                    .map { NntpCommand.stat(messageIds[it]) }
                connection.markPipelined(lines.size)
                connection.writeLines(lines)
                sent += lines.size
            }
            val response = connection.readResponse()
            connection.markPipelineAnswered()
            answered++
            val result = when (response.code) {
                430 -> StatResult.NotFound(response.code, response.message)
                223 -> {
                    val (code, number, messageId) = parseArticleResponseLine("${response.code} ${response.message}")
                    StatResult.Found(code, response.message, number, messageId)
                }
                else -> {
                    if (firstError == null) firstError = NntpProtocolException("STAT failed: ${response.code} ${response.message}", response)
                    StatResult.NotFound(response.code, response.message)
                }
            }
            onResult?.invoke(results.size, result)
            results += result
        }
        firstError?.let { throw it }
        return results
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
        if (NntpCommand.isMessageId(messageId)) fetchBodyYenc(NntpCommand.body(messageId))
        else flow { checkMessageId(messageId) }

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
        checkMessageId(messageId)
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
        checkMessageId(messageId)
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

    /** Throws the not-found of a lookup by [messageId], without sending it, when it can't be one. */
    private fun checkMessageId(messageId: String) {
        if (!NntpCommand.isMessageId(messageId)) {
            throw ArticleNotFoundException(
                "Article not found: ${NOT_A_MESSAGE_ID.code} ${NOT_A_MESSAGE_ID.message}",
                NOT_A_MESSAGE_ID
            )
        }
    }

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
        // The status line is now consumed off the wire; everything that follows
        // (if 222) is yenc body. Tell the connection so a subsequent
        // drainUntilArticleEnd doesn't try to re-parse a yenc data line as the
        // status line.
        connection.markBodyResponseRead()
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
