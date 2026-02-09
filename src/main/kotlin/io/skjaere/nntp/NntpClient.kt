package io.skjaere.nntp

import io.ktor.network.selector.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import java.io.Closeable

class NntpClient(
    val connection: NntpConnection,
    private val scope: CoroutineScope
) : Closeable {

    companion object {
        suspend fun connect(
            host: String,
            port: Int,
            selectorManager: SelectorManager,
            useTls: Boolean = false,
            scope: CoroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        ): NntpClient {
            val connection = NntpConnection.connect(host, port, selectorManager, useTls, scope)
            return NntpClient(connection, scope)
        }

        suspend fun connect(
            host: String,
            port: Int,
            selectorManager: SelectorManager,
            useTls: Boolean = false,
            username: String,
            password: String,
            scope: CoroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        ): NntpClient {
            val client = connect(host, port, selectorManager, useTls, scope)
            client.authenticate(username, password)
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
        val userResponse = connection.command(NntpCommand.authinfoUser(username))
        if (userResponse.code == 281) return userResponse
        if (userResponse.code != 381) {
            throw NntpAuthenticationException(
                "AUTHINFO USER failed: ${userResponse.code} ${userResponse.message}",
                userResponse
            )
        }
        val passResponse = connection.command(NntpCommand.authinfoPass(password))
        if (passResponse.code != 281) {
            throw NntpAuthenticationException(
                "AUTHINFO PASS failed: ${passResponse.code} ${passResponse.message}",
                passResponse
            )
        }
        return passResponse
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

    suspend fun stat(messageId: String): StatResponse {
        return fetchStat(NntpCommand.stat(messageId))
    }

    suspend fun stat(number: Long): StatResponse {
        return fetchStat(NntpCommand.stat(number))
    }

    suspend fun next(): StatResponse {
        return fetchStat(NntpCommand.NEXT)
    }

    suspend fun last(): StatResponse {
        return fetchStat(NntpCommand.LAST)
    }

    // --- Yenc body ---

    suspend fun bodyYenc(messageId: String): YencBodyResult {
        return fetchBodyYenc(NntpCommand.body(messageId))
    }

    suspend fun bodyYenc(number: Long): YencBodyResult {
        return fetchBodyYenc(NntpCommand.body(number))
    }

    // --- Yenc headers only ---

    suspend fun bodyYencHeaders(messageId: String): YencHeaders {
        return fetchBodyYencHeaders(NntpCommand.body(messageId))
    }

    suspend fun bodyYencHeaders(number: Long): YencHeaders {
        return fetchBodyYencHeaders(NntpCommand.body(number))
    }

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

    suspend fun post(article: String): NntpResponse {
        val initialResponse = connection.command(NntpCommand.POST)
        if (initialResponse.code != 340) {
            throw NntpProtocolException("POST failed: ${initialResponse.code} ${initialResponse.message}", initialResponse)
        }
        connection.writeLine(article)
        connection.writeLine(".")
        return connection.readResponse()
    }

    suspend fun ihave(messageId: String, article: String): NntpResponse {
        val initialResponse = connection.command(NntpCommand.ihave(messageId))
        if (initialResponse.code != 335) {
            throw NntpProtocolException("IHAVE failed: ${initialResponse.code} ${initialResponse.message}", initialResponse)
        }
        connection.writeLine(article)
        connection.writeLine(".")
        return connection.readResponse()
    }

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

    // --- Private helpers ---

    private suspend fun fetchArticle(cmd: String, expectedCode: Int): ArticleResponse {
        val (response, lines) = connection.commandMultiLine(cmd)
        if (response.code != expectedCode) {
            throw NntpProtocolException(
                "Command failed: ${response.code} ${response.message}",
                response
            )
        }
        val (code, number, messageId) = parseArticleResponseLine("${response.code} ${response.message}")
        return ArticleResponse(code, response.message, number, messageId, lines)
    }

    private suspend fun fetchStat(cmd: String): StatResponse {
        val response = connection.command(cmd)
        if (response.code != 223) {
            throw NntpProtocolException(
                "Command failed: ${response.code} ${response.message}",
                response
            )
        }
        val (code, number, messageId) = parseArticleResponseLine("${response.code} ${response.message}")
        return StatResponse(code, response.message, number, messageId)
    }

    private suspend fun fetchBodyYenc(cmd: String): YencBodyResult {
        val response = connection.commandRaw(cmd)
        if (response.code != 222) {
            connection.commandMutex.unlock()
            throw NntpProtocolException(
                "BODY failed: ${response.code} ${response.message}",
                response
            )
        }
        val decoder = YencDecoder(scope)
        return decoder.decode(connection, response)
    }

    private suspend fun fetchBodyYencHeaders(cmd: String): YencHeaders {
        val response = connection.commandRaw(cmd)
        if (response.code != 222) {
            connection.commandMutex.unlock()
            throw NntpProtocolException(
                "BODY failed: ${response.code} ${response.message}",
                response
            )
        }
        try {
            val ybeginLine = connection.readLine()
            if (!ybeginLine.startsWith("=ybegin ")) {
                throw YencDecodingException("Expected =ybegin line, got: $ybeginLine")
            }

            val nextLineBytes = connection.readRawLine()
            val nextLineStr = String(nextLineBytes, Charsets.ISO_8859_1)
            val ypartLine = if (nextLineStr.startsWith("=ypart ")) nextLineStr else null

            return YencHeaders.parse(ybeginLine, ypartLine)
        } finally {
            connection.scheduleReconnect()
            connection.commandMutex.unlock()
        }
    }
}
