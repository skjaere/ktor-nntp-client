package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import kotlin.collections.ArrayDeque
import kotlin.time.Duration.Companion.milliseconds
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
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.nio.channels.UnresolvedAddressException
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
    private val idleGracePeriodMs: Long = DEFAULT_IDLE_GRACE_PERIOD_MS,
    private val idleEvictionTimeoutMs: Long = DEFAULT_IDLE_EVICTION_TIMEOUT_MS,
    private val commandRetries: Int = DEFAULT_COMMAND_RETRIES
) : Closeable {

    companion object {
        const val DEFAULT_KEEPALIVE_INTERVAL_MS = 60_000L
        const val DEFAULT_IDLE_GRACE_PERIOD_MS = 300_000L // 5 minutes
        const val DEFAULT_IDLE_EVICTION_TIMEOUT_MS = 180_000L // 3 minutes
        const val MIN_POOL_SIZE = 1  // always keep at least one connection
        const val CONNECT_RETRY_BASE_DELAY_MS = 1_000L
        const val CONNECT_RETRY_MAX_DELAY_MS = 30_000L
        const val CONNECT_MAX_RETRIES = 5L
        const val ACQUIRE_TIMEOUT_MS = 30_000L
        const val DEFAULT_COMMAND_RETRIES = 1
    }

    private data class IdleEntry(val client: NntpClient, val returnedAtMs: Long)

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
    private val idleClients = ArrayDeque<IdleEntry>()
    private val waiters = PriorityQueue<PriorityWaiter>()
    private var waiterSequence = 0L
    private var currentSize = 0  // total connections: idle + in-use + connecting
    private var closed = false
    private var keepaliveJob: Job? = null

    @Volatile
    private var sleeping = true

    @Volatile
    private var lastActivityMs = System.currentTimeMillis()

    private val poolName = "$host:$port"

    private val registry = Metrics.globalRegistry

    init {
        Gauge.builder("nntp.pool.idle", idleClients) { it.size.toDouble() }
            .tag("pool.name", poolName)
            .register(registry)
        Gauge.builder("nntp.pool.active", this) { (it.currentSize - it.idleClients.size).toDouble() }
            .tag("pool.name", poolName)
            .register(registry)
        Gauge.builder("nntp.pool.size", this) { it.currentSize.toDouble() }
            .tag("pool.name", poolName)
            .register(registry)
        Gauge.builder("nntp.pool.waiters", waiters) { it.size.toDouble() }
            .tag("pool.name", poolName)
            .register(registry)
        Gauge.builder("nntp.pool.sleeping", this) { if (it.sleeping) 1.0 else 0.0 }
            .tag("pool.name", poolName)
            .register(registry)
    }

    /**
     * Launch a single new connection in the background.
     * On success the client is dispatched to a waiter or parked idle.
     * On failure [currentSize] is decremented so the slot can be reused.
     * Must only be called after incrementing [currentSize] under [poolMutex].
     */
    private fun launchConnection() {
        scope.launch {
            val client: NntpClient? = try {
                connectWithRetry()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.warn("Connection attempt failed permanently: {}", e.message)
                null
            }
            if (client != null) {
                withContext(NonCancellable) { addClientToPool(client) }
            } else {
                withContext(NonCancellable) { poolMutex.withLock { currentSize-- } }
            }
        }
    }

    private suspend fun connectWithRetry(): NntpClient? {
        var delayMs = CONNECT_RETRY_BASE_DELAY_MS
        return flow {
            emit(
                if (username != null && password != null)
                    NntpClient.connect(host, port, selectorManager, useTls, username, password, scope)
                else
                    NntpClient.connect(host, port, selectorManager, useTls, scope)
            )
        }.retry(CONNECT_MAX_RETRIES) { e ->
            when (e) {
                is UnresolvedAddressException ->
                    logger.error("Failed to resolve host '{}:{}': {}", host, port, e.message)

                else ->
                    logger.warn("Failed to connect to {}:{}: {}", host, port, e.message)
            }
            delay(delayMs)
            delayMs = (delayMs * 2).coerceAtMost(CONNECT_RETRY_MAX_DELAY_MS)
            true
        }.firstOrNull()
    }

    private fun startKeepalive() {
        if (keepaliveIntervalMs <= 0) return
        keepaliveJob?.cancel()
        keepaliveJob = scope.launch { keepaliveLoop() }
    }

    private suspend fun keepaliveLoop() {
        while (currentCoroutineContext().isActive) {
            delay(keepaliveIntervalMs.milliseconds)
            if (sleeping) continue
            if (idleGracePeriodMs > 0 &&
                System.currentTimeMillis() - lastActivityMs > idleGracePeriodMs
            ) {
                logger.info("Pool idle for {}ms, going to sleep", idleGracePeriodMs)
                doSleep()
                continue
            }
            evictIdleConnections()
            val entries = drainIdle()
            for (entry in entries) {
                try {
                    entry.client.date()
                } catch (e: NntpConnectionException) {
                    logger.debug("Keepalive failed, scheduling reconnect: {}", e.message)
                    entry.client.connection.scheduleReconnect()
                } catch (_: Exception) {
                }
                // Preserve the original idle timestamp so keepalive itself doesn't reset eviction.
                returnToPool(entry.client, entry.returnedAtMs)
            }
        }
    }

    /**
     * Closes individual idle connections that have been sitting unused for longer than
     * [idleEvictionTimeoutMs], keeping at least [MIN_POOL_SIZE] connections. Unlike the
     * previous "shrink only when everything is idle" heuristic, this survives intermittent
     * background traffic (e.g. health-check probes) that would otherwise pin the pool at
     * its peak size forever.
     */
    private suspend fun evictIdleConnections() {
        if (idleEvictionTimeoutMs <= 0) return
        val now = System.currentTimeMillis()
        val evicted = poolMutex.withLock {
            val keepFloor = MIN_POOL_SIZE.coerceAtLeast(0)
            val removed = mutableListOf<IdleEntry>()
            // Oldest idle entries sit at the head of the deque (we addLast on return),
            // so scan from the front. Stop once the remaining pool would dip below the floor.
            while (idleClients.isNotEmpty() && currentSize > keepFloor) {
                val oldest = idleClients.first()
                if (now - oldest.returnedAtMs < idleEvictionTimeoutMs) break
                idleClients.removeFirst()
                currentSize--
                removed.add(oldest)
            }
            removed
        }
        for (entry in evicted) {
            runCatching { entry.client.close() }
        }
        if (evicted.isNotEmpty()) {
            logger.info(
                "Evicted {} idle connection(s) from pool (now {})",
                evicted.size, currentSize
            )
        }
    }

    private suspend fun drainIdle(): List<IdleEntry> {
        poolMutex.withLock {
            val entries = idleClients.toList()
            idleClients.clear()
            return entries
        }
    }

    suspend fun sleep() {
        doSleep()
    }

    private suspend fun doSleep() {
        val shouldSleep = poolMutex.withLock {
            if (sleeping) return@withLock false
            sleeping = true
            true
        }
        if (!shouldSleep) return
        keepaliveJob?.cancel()
        keepaliveJob = null
        val entries = drainIdle()
        for (entry in entries) {
            runCatching { entry.client.close() }
        }
        poolMutex.withLock { currentSize -= entries.size }
        logger.info("Pool is now sleeping ({} connections closed)", entries.size)
    }

    suspend fun wake() {
        doWake()
    }

    private suspend fun doWake() {
        val shouldWake = poolMutex.withLock {
            if (!sleeping) return@withLock false
            sleeping = false
            true
        }
        if (!shouldWake) return
        logger.info("Waking pool (connections will be created on demand, max={})", maxConnections)
        // Drain any stale clients returned after sleep (from in-flight withClient calls)
        val stale = drainIdle()
        stale.forEach { runCatching { it.client.close() } }
        lastActivityMs = System.currentTimeMillis()
        startKeepalive()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun acquire(priority: Int = 0): NntpClient {
        val sample = Timer.start(registry)
        val deferred = CompletableDeferred<NntpClient>()
        val waiter = poolMutex.withLock {
            val entry = idleClients.removeFirstOrNull()
            if (entry != null) {
                deferred.complete(entry.client)
                return@withLock null
            }
            val w = PriorityWaiter(priority, deferred, waiterSequence++)
            waiters.add(w)
            // Launch a new connection if we haven't reached the max
            if (currentSize < maxConnections) {
                currentSize++
                launchConnection()
            }
            w
        }
        if (waiter == null) {
            recordAcquire(sample, priority)
            return deferred.getCompleted()
        }
        // Await outside the lock with timeout
        try {
            val client = withTimeoutOrNull(ACQUIRE_TIMEOUT_MS.milliseconds) { deferred.await() }
            if (client != null) {
                recordAcquire(sample, priority)
                return client
            }
            // Timed out — remove waiter and fail
            poolMutex.withLock { waiters.remove(waiter) }
            if (deferred.isCompleted) {
                try {
                    addClientToPool(deferred.getCompleted())
                } catch (_: IllegalStateException) {
                    // deferred completed exceptionally
                }
            }
            throw NntpConnectionException(
                "Timed out waiting for connection to $host:$port (${ACQUIRE_TIMEOUT_MS}ms)"
            )
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

    private fun recordAcquire(sample: Timer.Sample, priority: Int) {
        sample.stop(
            Timer.builder("nntp.pool.acquire")
                .tag("pool.name", poolName)
                .tag("priority", priority.toString())
                .register(registry)
        )
    }

    /**
     * Called under [poolMutex]: dispatch the client to the highest-priority waiter,
     * skipping cancelled waiters. If no waiters, park in [idleClients] with the given
     * idle timestamp (defaults to now).
     */
    private fun dispatchOrPark(client: NntpClient, returnedAtMs: Long = System.currentTimeMillis()) {
        while (true) {
            val waiter = waiters.poll() ?: break
            if (waiter.deferred.complete(client)) return
            // Waiter was cancelled, try next
        }
        idleClients.addLast(IdleEntry(client, returnedAtMs))
    }

    private suspend fun addClientToPool(client: NntpClient) {
        poolMutex.withLock {
            if (closed) {
                currentSize--
                runCatching { client.close() }
            } else {
                dispatchOrPark(client)
            }
        }
    }

    suspend fun <T> withClient(block: suspend (NntpClient) -> T): T = withClient(0, block)

    suspend fun <T> withClient(priority: Int = 0, block: suspend (NntpClient) -> T): T {
        if (sleeping) doWake()
        lastActivityMs = System.currentTimeMillis()
        return supervisorScope {
            flow {
                var client: NntpClient? = null
                try {
                    client = acquire(priority)
                    client.connection.ensureConnected()
                    emit(block(client))
                } catch (e: NntpConnectionException) {
                    client?.connection?.scheduleReconnect()
                    throw e
                } catch (e: IOException) {
                    client?.connection?.scheduleReconnect()
                    throw e
                } finally {
                    client?.let { returnToPool(it) }
                }
            }.retry(commandRetries.toLong()) { e ->
                val retryable = e is NntpConnectionException || e is IOException
                if (retryable) {
                    logger.debug("Command failed ({}), retrying: {}", e::class.simpleName, e.message)
                }
                retryable
            }.first()
        }
    }

    private suspend fun returnToPool(client: NntpClient, returnedAtMs: Long = System.currentTimeMillis()) {
        withContext(NonCancellable) {
            poolMutex.withLock {
                if (closed) {
                    runCatching { client.close() }
                } else {
                    dispatchOrPark(client, returnedAtMs)
                }
            }
        }
    }

    // --- Delegated commands ---

    fun bodyYenc(messageId: String, priority: Int = 0): Flow<YencEvent> = channelFlow {
        withClient(priority) { client ->
            client.bodyYenc(messageId).collect { event -> send(event) }
        }
    }

    fun bodyYenc(number: Long, priority: Int = 0): Flow<YencEvent> = channelFlow {
        withClient(priority) { client ->
            client.bodyYenc(number).collect { event -> send(event) }
        }
    }

    suspend fun bodyYencHeaders(messageId: String, priority: Int = 0): YencHeaders =
        withClient(priority) { it.bodyYencHeaders(messageId) }

    suspend fun bodyYencHeaders(number: Long, priority: Int = 0): YencHeaders =
        withClient(priority) { it.bodyYencHeaders(number) }

    suspend fun group(name: String, priority: Int = 0): GroupResponse =
        withClient(priority) { it.group(name) }

    suspend fun article(messageId: String, priority: Int = 0): ArticleResponse =
        withClient(priority) { it.article(messageId) }

    suspend fun article(number: Long, priority: Int = 0): ArticleResponse =
        withClient(priority) { it.article(number) }

    suspend fun head(messageId: String, priority: Int = 0): ArticleResponse =
        withClient(priority) { it.head(messageId) }

    suspend fun head(number: Long, priority: Int = 0): ArticleResponse =
        withClient(priority) { it.head(number) }

    suspend fun body(messageId: String, priority: Int = 0): ArticleResponse =
        withClient(priority) { it.body(messageId) }

    suspend fun body(number: Long, priority: Int = 0): ArticleResponse =
        withClient(priority) { it.body(number) }

    suspend fun stat(messageId: String, priority: Int = 0): StatResult =
        withClient(priority) { it.stat(messageId) }

    suspend fun stat(number: Long, priority: Int = 0): StatResult =
        withClient(priority) { it.stat(number) }

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
                for (entry in idleClients) {
                    runCatching { entry.client.close() }
                }
                currentSize -= idleClients.size
                idleClients.clear()
            }
        }
        registry.find("nntp.pool.idle").tag("pool.name", poolName).gauge()?.id?.let(registry::remove)
        registry.find("nntp.pool.active").tag("pool.name", poolName).gauge()?.id?.let(registry::remove)
        registry.find("nntp.pool.size").tag("pool.name", poolName).gauge()?.id?.let(registry::remove)
        registry.find("nntp.pool.waiters").tag("pool.name", poolName).gauge()?.id?.let(registry::remove)
        registry.find("nntp.pool.sleeping").tag("pool.name", poolName).gauge()?.id?.let(registry::remove)
        registry.find("nntp.pool.acquire").tag("pool.name", poolName).timers().forEach { timer ->
            registry.remove(timer.id)
        }
    }
}
