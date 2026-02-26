package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import kotlin.collections.ArrayDeque
import kotlin.collections.List
import kotlin.collections.forEach
import kotlin.collections.toList
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.PriorityQueue

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

    private class PriorityWaiter(
        val priority: Int,
        val deferred: CompletableDeferred<NntpClient>,
        val sequenceNumber: Long
    ) : Comparable<PriorityWaiter> {
        override fun compareTo(other: PriorityWaiter): Int {
            // Higher priority first, then lower sequence number first (FIFO)
            val priorityCmp = other.priority.compareTo(this.priority)
            if (priorityCmp != 0) return priorityCmp
            return this.sequenceNumber.compareTo(other.sequenceNumber)
        }
    }

    private val logger = LoggerFactory.getLogger(NntpClientPool::class.java)
    private val poolMutex = Mutex()
    private val idleClients = ArrayDeque<NntpClient>()
    private val waiters = PriorityQueue<PriorityWaiter>()
    private var waiterSequence = 0L
    private var closed = false
    private var keepaliveJob: Job? = null

    @Volatile
    private var sleeping = false
    @Volatile
    private var lastActivityMs = System.currentTimeMillis()

    suspend fun connect() {
        repeat(maxConnections) {
            val client = if (username != null && password != null)
                NntpClient.connect(host, port, selectorManager, useTls, username, password, scope)
            else
                NntpClient.connect(host, port, selectorManager, useTls, scope)
            addClientToPool(client)
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
        while (currentCoroutineContext().isActive) {
            delay(keepaliveIntervalMs)
            if (sleeping) continue
            if (idleGracePeriodMs > 0 &&
                System.currentTimeMillis() - lastActivityMs > idleGracePeriodMs
            ) {
                logger.info("Pool idle for {}ms, going to sleep", idleGracePeriodMs)
                doSleep()
                continue
            }
            val clients = drainIdle()
            for (client in clients) {
                try {
                    client.date()
                } catch (e: NntpConnectionException) {
                    logger.debug("Keepalive failed, scheduling reconnect: {}", e.message)
                    client.connection.scheduleReconnect()
                } catch (_: Exception) {
                }
                returnToPool(client)
            }
        }
    }

    private suspend fun drainIdle(): List<NntpClient> {
        poolMutex.withLock {
            val clients = idleClients.toList()
            idleClients.clear()
            return clients
        }
    }

    suspend fun sleep() {
        doSleep()
    }

    private suspend fun doSleep() {
        if (sleeping) return
        sleeping = true
        keepaliveJob?.cancel()
        keepaliveJob = null
        val clients = drainIdle()
        for (client in clients) {
            runCatching { client.close() }
        }
        logger.info("Pool is now sleeping ({} connections closed)", clients.size)
    }

    suspend fun wake() {
        doWake()
    }

    private suspend fun doWake() {
        if (!sleeping) return
        logger.info("Waking pool, reconnecting {} connections", maxConnections)
        sleeping = false
        // Drain any stale clients returned after sleep (from in-flight withClient calls)
        val stale = drainIdle()
        stale.forEach { runCatching { it.close() } }
        repeat(maxConnections) {
            val client = if (username != null && password != null)
                NntpClient.connect(host, port, selectorManager, useTls, username, password, scope)
            else
                NntpClient.connect(host, port, selectorManager, useTls, scope)
            addClientToPool(client)
        }
        lastActivityMs = System.currentTimeMillis()
        startKeepalive()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun acquire(priority: Int = 0): NntpClient {
        val deferred = CompletableDeferred<NntpClient>()
        val waiter = poolMutex.withLock {
            val client = idleClients.removeFirstOrNull()
            if (client != null) {
                deferred.complete(client)
                return@withLock null
            }
            val w = PriorityWaiter(priority, deferred, waiterSequence++)
            waiters.add(w)
            w
        }
        if (waiter == null) return deferred.getCompleted()
        // Await outside the lock
        try {
            return deferred.await()
        } catch (e: CancellationException) {
            // On cancellation, remove from queue and handle race where client was already completed
            poolMutex.withLock { waiters.remove(waiter) }
            // If the deferred was completed before we could cancel, return the client to the pool
            if (deferred.isCompleted) {
                try {
                    addClientToPool(deferred.getCompleted())
                } catch (_: IllegalStateException) {
                    // deferred completed exceptionally, nothing to return
                }
            }
            throw e
        }
    }

    /**
     * Called under [poolMutex]: dispatch the client to the highest-priority waiter,
     * skipping cancelled waiters. If no waiters, park in [idleClients].
     */
    private fun dispatchOrPark(client: NntpClient) {
        while (true) {
            val waiter = waiters.poll() ?: break
            if (waiter.deferred.complete(client)) return
            // Waiter was cancelled, try next
        }
        idleClients.addLast(client)
    }

    private suspend fun addClientToPool(client: NntpClient) {
        poolMutex.withLock {
            dispatchOrPark(client)
        }
    }

    suspend fun <T> withClient(priority: Int = 0, block: suspend (NntpClient) -> T): T {
        if (sleeping) doWake()
        lastActivityMs = System.currentTimeMillis()
        val client = acquire(priority)
        var returned = false
        try {
            return supervisorScope { block(client) }
        } catch (e: NntpConnectionException) {
            logger.debug("Connection failed, scheduling reconnect and retrying: {}", e.message)
            client.connection.scheduleReconnect()
            returnToPool(client)
            returned = true
            return retryWithDifferentClient(priority, block)
        } finally {
            if (!returned) returnToPool(client)
        }
    }

    private suspend fun <T> retryWithDifferentClient(
        priority: Int,
        block: suspend (NntpClient) -> T
    ): T {
        val client = acquire(priority)
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
            poolMutex.withLock {
                if (closed) {
                    runCatching { client.close() }
                } else {
                    dispatchOrPark(client)
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
        keepaliveJob?.cancel()
        keepaliveJob = null
        runBlocking {
            poolMutex.withLock {
                closed = true
                // Complete all waiters exceptionally
                while (true) {
                    val waiter = waiters.poll() ?: break
                    waiter.deferred.completeExceptionally(
                        IllegalStateException("Pool closed")
                    )
                }
                // Close all idle clients
                for (client in idleClients) {
                    runCatching { client.close() }
                }
                idleClients.clear()
            }
        }
    }
}
