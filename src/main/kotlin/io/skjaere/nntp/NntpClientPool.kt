package io.skjaere.nntp

import io.ktor.network.selector.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import java.io.Closeable

class NntpClientPool(
    private val host: String,
    private val port: Int,
    private val selectorManager: SelectorManager,
    private val useTls: Boolean = false,
    private val username: String? = null,
    private val password: String? = null,
    private val maxConnections: Int,
    private val scope: CoroutineScope
) : Closeable {

    private val available = Channel<NntpClient>(maxConnections)

    suspend fun connect() {
        repeat(maxConnections) {
            val client = if (username != null && password != null)
                NntpClient.connect(host, port, selectorManager, useTls, username, password, scope)
            else
                NntpClient.connect(host, port, selectorManager, useTls, scope)
            available.send(client)
        }
    }

    suspend fun <T> withClient(block: suspend (NntpClient) -> T): T {
        val client = available.receive()
        try {
            return supervisorScope {
                block(client)
            }
        } finally {
            withContext(NonCancellable) {
                try {
                    available.send(client)
                } catch (_: ClosedSendChannelException) {
                    // Pool was closed while returning — just close the client
                    runCatching { client.close() }
                }
            }
        }
    }

    // --- Delegated commands ---

    fun bodyYenc(messageId: String): Flow<YencEvent> = channelFlow {
        withClient { client ->
            client.bodyYenc(messageId).collect { event -> send(event) }
        }
    }

    fun bodyYenc(number: Long): Flow<YencEvent> = channelFlow {
        withClient { client ->
            client.bodyYenc(number).collect { event -> send(event) }
        }
    }

    suspend fun bodyYencHeaders(messageId: String): YencHeaders =
        withClient { it.bodyYencHeaders(messageId) }

    suspend fun bodyYencHeaders(number: Long): YencHeaders =
        withClient { it.bodyYencHeaders(number) }

    suspend fun group(name: String): GroupResponse =
        withClient { it.group(name) }

    suspend fun article(messageId: String): ArticleResponse =
        withClient { it.article(messageId) }

    suspend fun article(number: Long): ArticleResponse =
        withClient { it.article(number) }

    suspend fun head(messageId: String): ArticleResponse =
        withClient { it.head(messageId) }

    suspend fun head(number: Long): ArticleResponse =
        withClient { it.head(number) }

    suspend fun body(messageId: String): ArticleResponse =
        withClient { it.body(messageId) }

    suspend fun body(number: Long): ArticleResponse =
        withClient { it.body(number) }

    suspend fun stat(messageId: String): StatResult =
        withClient { it.stat(messageId) }

    suspend fun stat(number: Long): StatResult =
        withClient { it.stat(number) }

    override fun close() {
        available.close()
        val clients = mutableListOf<NntpClient>()
        while (true) {
            clients.add(available.tryReceive().getOrNull() ?: break)
        }
        clients.forEach { runCatching { it.close() } }
    }
}
