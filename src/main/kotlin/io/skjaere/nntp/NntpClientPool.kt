package io.skjaere.nntp

import io.ktor.network.selector.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import org.slf4j.LoggerFactory
import java.io.Closeable
import kotlin.coroutines.coroutineContext

class NntpClientPool(
    private val host: String,
    private val port: Int,
    private val selectorManager: SelectorManager,
    private val useTls: Boolean = false,
    private val username: String? = null,
    private val password: String? = null,
    private val maxConnections: Int,
    private val scope: CoroutineScope,
    private val keepaliveIntervalMs: Long = DEFAULT_KEEPALIVE_INTERVAL_MS,
    private val idleGracePeriodMs: Long = DEFAULT_IDLE_GRACE_PERIOD_MS
) : Closeable {

    companion object {
        const val DEFAULT_KEEPALIVE_INTERVAL_MS = 60_000L
        const val DEFAULT_IDLE_GRACE_PERIOD_MS = 300_000L // 5 minutes
    }

    private val logger = LoggerFactory.getLogger(NntpClientPool::class.java)
    private val available = Channel<NntpClient>(maxConnections)
    private var keepaliveJob: Job? = null

    @Volatile private var sleeping = false
    @Volatile private var lastActivityMs = System.currentTimeMillis()

    suspend fun connect() {
        repeat(maxConnections) {
            val client = if (username != null && password != null)
                NntpClient.connect(host, port, selectorManager, useTls, username, password, scope)
            else
                NntpClient.connect(host, port, selectorManager, useTls, scope)
            available.send(client)
        }
        sleeping = false
        lastActivityMs = System.currentTimeMillis()
        startKeepalive()
    }

    private fun startKeepalive() {
        if (keepaliveIntervalMs <= 0) return
        keepaliveJob?.cancel()
        keepaliveJob = scope.launch { keepaliveLoop() }
    }

    private suspend fun keepaliveLoop() {
        while (coroutineContext.isActive) {
            delay(keepaliveIntervalMs)
            if (sleeping) continue
            if (idleGracePeriodMs > 0 &&
                System.currentTimeMillis() - lastActivityMs > idleGracePeriodMs
            ) {
                logger.info("Pool idle for {}ms, going to sleep", idleGracePeriodMs)
                doSleep()
                continue
            }
            val clients = drainAvailable()
            for (client in clients) {
                try {
                    client.date()
                } catch (e: NntpConnectionException) {
                    logger.debug("Keepalive failed, scheduling reconnect: {}", e.message)
                    client.connection.scheduleReconnect()
                } catch (_: Exception) { }
                returnToPool(client)
            }
        }
    }

    private fun drainAvailable(): List<NntpClient> {
        val clients = mutableListOf<NntpClient>()
        while (true) {
            clients.add(available.tryReceive().getOrNull() ?: break)
        }
        return clients
    }

    suspend fun sleep() { doSleep() }

    private suspend fun doSleep() {
        if (sleeping) return
        sleeping = true
        keepaliveJob?.cancel()
        keepaliveJob = null
        val clients = drainAvailable()
        for (client in clients) {
            runCatching { client.close() }
        }
        logger.info("Pool is now sleeping ({} connections closed)", clients.size)
    }

    suspend fun wake() { doWake() }

    private suspend fun doWake() {
        if (!sleeping) return
        logger.info("Waking pool, reconnecting {} connections", maxConnections)
        sleeping = false
        // Drain any stale clients returned after sleep (from in-flight withClient calls)
        val stale = drainAvailable()
        stale.forEach { runCatching { it.close() } }
        repeat(maxConnections) {
            val client = if (username != null && password != null)
                NntpClient.connect(host, port, selectorManager, useTls, username, password, scope)
            else
                NntpClient.connect(host, port, selectorManager, useTls, scope)
            available.send(client)
        }
        lastActivityMs = System.currentTimeMillis()
        startKeepalive()
    }

    suspend fun <T> withClient(block: suspend (NntpClient) -> T): T {
        if (sleeping) doWake()
        lastActivityMs = System.currentTimeMillis()
        val client = available.receive()
        var returned = false
        try {
            return supervisorScope { block(client) }
        } catch (e: NntpConnectionException) {
            logger.debug("Connection failed, scheduling reconnect and retrying: {}", e.message)
            client.connection.scheduleReconnect()
            returnToPool(client)
            returned = true
            return retryWithDifferentClient(block)
        } finally {
            if (!returned) returnToPool(client)
        }
    }

    private suspend fun <T> retryWithDifferentClient(block: suspend (NntpClient) -> T): T {
        val client = available.receive()
        try {
            client.connection.ensureConnected()
            return supervisorScope { block(client) }
        } catch (e: NntpConnectionException) {
            logger.debug("Retry also failed, scheduling reconnect: {}", e.message)
            client.connection.scheduleReconnect()
            throw e
        } finally {
            returnToPool(client)
        }
    }

    private suspend fun returnToPool(client: NntpClient) {
        withContext(NonCancellable) {
            try {
                available.send(client)
            } catch (_: ClosedSendChannelException) {
                runCatching { client.close() }
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
        keepaliveJob?.cancel()
        keepaliveJob = null
        available.close()
        val clients = mutableListOf<NntpClient>()
        while (true) {
            clients.add(available.tryReceive().getOrNull() ?: break)
        }
        clients.forEach { runCatching { it.close() } }
    }
}
