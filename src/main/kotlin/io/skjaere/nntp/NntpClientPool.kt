package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import io.skjaere.nntp.NntpClientPool.Companion.MIN_POOL_SIZE
import kotlin.collections.ArrayDeque
import kotlin.collections.List
import kotlin.collections.forEach
import kotlin.collections.isNotEmpty
import kotlin.collections.map
import kotlin.collections.mutableListOf
import kotlin.collections.mutableSetOf
import kotlin.collections.plus
import kotlin.collections.toList
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
        const val KEEPALIVE_COMMAND_TIMEOUT_MS =
            10_000L // bound per-connection DATE so a hung socket can't stall eviction
        const val MIN_POOL_SIZE = 1  // always keep at least one connection
        const val CONNECT_RETRY_BASE_DELAY_MS = 1_000L
        const val CONNECT_RETRY_MAX_DELAY_MS = 30_000L
        const val CONNECT_MAX_RETRIES = 5L
        const val ACQUIRE_TIMEOUT_MS = 30_000L
        const val DEFAULT_COMMAND_RETRIES = 1

        // QUIT round-trip cap for graceful shutdowns at runtime (idle eviction, pool
        // sleep, defensive wake-time cleanup). Same budget as POOL_CLOSE_QUIT_TIMEOUT_MS
        // — the QUIT is a courtesy to the server's session counter, not a correctness
        // requirement, so a stuck peer must not slow the pool's bookkeeping.
        const val GRACEFUL_QUIT_TIMEOUT_MS = 1500L

        // Tight per-client timeout for the QUIT round-trip during pool.close(). Slightly above
        // the per-connection default so an unresponsive socket doesn't drag shutdown out — clients
        // run their QUITs in parallel, so this caps the overall shutdown wall time, not a serial sum.
        const val POOL_CLOSE_QUIT_TIMEOUT_MS = 1500L
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

    // Active clients are tracked as a map so returnToPool can recover the priority used at
    // acquire time and decrement the right gauge bucket. Mutated under poolMutex.
    private val activeClients = mutableMapOf<NntpClient, Int>()
    private val waiters = PriorityQueue<PriorityWaiter>()
    private var waiterSequence = 0L
    private var currentSize = 0  // total connections: idle + in-use + connecting
    private var closed = false
    private var keepaliveJob: Job? = null

    // Per-priority counters for the priority-tagged gauges. Updated atomically alongside
    // every add/remove of an active client or a waiter, so micrometer scrapes can read
    // them lock-free. We register a gauge lazily the first time we see a given priority,
    // tracked via [priorityGauges] to keep registration idempotent.
    private val activeByPriority = ConcurrentHashMap<Int, AtomicInteger>()
    private val waitersByPriority = ConcurrentHashMap<Int, AtomicInteger>()
    private val priorityGauges = ConcurrentHashMap.newKeySet<Int>()

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
        Gauge.builder("nntp.pool.size", this) { it.currentSize.toDouble() }
            .tag("pool.name", poolName)
            .register(registry)
        Gauge.builder("nntp.pool.sleeping", this) { if (it.sleeping) 1.0 else 0.0 }
            .tag("pool.name", poolName)
            .register(registry)
        // Pre-register the priority=0 active/waiters gauges so they exist from pool
        // init (other priorities are registered lazily on first use). These carry a
        // `priority` tag — registering an aggregate version with only `{pool.name}`
        // here would conflict with the per-priority versions at Prometheus export
        // time (Prometheus requires identical tag keys across all instances of a
        // metric, so the lazily-registered priority-tagged ones would silently
        // disappear from the scrape).
        ensurePriorityGauges(0)
    }

    private fun ensurePriorityGauges(priority: Int) {
        if (priorityGauges.add(priority)) {
            Gauge.builder("nntp.pool.active") {
                (activeByPriority[priority]?.get() ?: 0).toDouble()
            }
                .tag("pool.name", poolName)
                .tag("priority", priority.toString())
                .register(registry)
            Gauge.builder("nntp.pool.waiters") {
                (waitersByPriority[priority]?.get() ?: 0).toDouble()
            }
                .tag("pool.name", poolName)
                .tag("priority", priority.toString())
                .register(registry)
        }
    }

    private fun incActive(priority: Int) {
        ensurePriorityGauges(priority)
        activeByPriority.computeIfAbsent(priority) { AtomicInteger() }.incrementAndGet()
    }

    private fun decActive(priority: Int) {
        activeByPriority[priority]?.decrementAndGet()
    }

    private fun incWaiter(priority: Int) {
        ensurePriorityGauges(priority)
        waitersByPriority.computeIfAbsent(priority) { AtomicInteger() }.incrementAndGet()
    }

    private fun decWaiter(priority: Int) {
        waitersByPriority[priority]?.decrementAndGet()
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
                    NntpClient.connect(host, port, selectorManager, useTls, username, password)
                else
                    NntpClient.connect(host, port, selectorManager, useTls)
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
                System.currentTimeMillis() - lastActivityMs > idleGracePeriodMs &&
                // `lastActivityMs` ticks only at withClient entry, so it goes stale during
                // cache-hot streaming (callers short-circuit through the disk segment cache
                // and never enter withClient). With active checked-out connections in that
                // state, the timestamp is a stale proxy for pool busyness. Skip the sleep
                // path so we don't drop into the incoherent sleeping=1 / active>0 state.
                activeClients.isEmpty()
            ) {
                logger.info("Pool idle for {}ms, going to sleep", idleGracePeriodMs)
                doSleep()
                continue
            }
            evictIdleConnections()
            val entries = drainIdle()
            for (entry in entries) {
                var dead = false
                val completed = withTimeoutOrNull(KEEPALIVE_COMMAND_TIMEOUT_MS.milliseconds) {
                    try {
                        entry.client.date()
                        true
                    } catch (@Suppress("TooGenericExceptionCaught") e: Exception) {
                        logger.debug("Keepalive failed, discarding connection: {}", e.message)
                        dead = true
                        true
                    }
                }
                if (completed == null) {
                    // DATE hung — socket is half-broken. Discard so a single stuck connection
                    // can't block eviction or the grace-period check.
                    logger.debug(
                        "Keepalive DATE timed out after {}ms; discarding connection",
                        KEEPALIVE_COMMAND_TIMEOUT_MS
                    )
                    dead = true
                }
                if (dead) {
                    discardClient(entry.client)
                } else {
                    // Preserve the original idle timestamp so keepalive itself doesn't reset eviction.
                    returnToPool(entry.client, entry.returnedAtMs)
                }
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
        shutdownGracefully(evicted.map { it.client })
        if (evicted.isNotEmpty()) {
            logger.debug(
                "Evicted {} idle connection(s) from pool (now {})",
                evicted.size, currentSize
            )
        }
    }

    /**
     * QUIT-then-close a batch of healthy idle connections in parallel. Used by
     * eviction, sleep, and defensive wake-time cleanup paths — anywhere we own a
     * clean connection that we just don't need anymore. Sending QUIT lets the news
     * server free the session slot promptly, so we don't burn through the per-account
     * connection quota with stale half-acknowledged sessions on the server side.
     *
     * For broken/dirty connections (after a protocol error or cancelled body), use
     * [discardClient] / [NntpClient.close] directly — QUIT on a corrupt wire is
     * meaningless and can pile garbage on top of garbage.
     */
    private suspend fun shutdownGracefully(clients: Collection<NntpClient>) {
        if (clients.isEmpty()) return
        supervisorScope {
            clients.forEach { c ->
                launch { runCatching { c.shutdown(GRACEFUL_QUIT_TIMEOUT_MS) } }
            }
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
            // Belt-and-suspenders against any caller (keepalive, an explicit sleep() call,
            // a future code path) that might try to sleep while clients are checked out.
            // Sleeping in that state silently desyncs the gauges and the pool's slot count.
            if (activeClients.isNotEmpty()) {
                logger.debug("Refusing to sleep: {} client(s) still checked out", activeClients.size)
                return@withLock false
            }
            sleeping = true
            true
        }
        if (!shouldSleep) return
        // doSleep can be called from inside keepaliveLoop (it's how the grace-period
        // path triggers sleep), and cancelling keepaliveJob there cancels the very
        // coroutine running this code. NonCancellable lets the QUIT-and-close drain
        // run to completion; the loop body's `if (sleeping) continue` guard above
        // stops keepalive from doing any further work the next time it ticks.
        withContext(NonCancellable) {
            keepaliveJob?.cancel()
            keepaliveJob = null
            val entries = drainIdle()
            shutdownGracefully(entries.map { it.client })
            poolMutex.withLock { currentSize -= entries.size }
            logger.info("Pool is now sleeping ({} connections closed)", entries.size)
        }
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
        // Defensive cleanup: with returnToPool now honoring `sleeping`, idleClients should
        // already be empty here. Any stragglers get closed AND decremented so currentSize
        // doesn't leak slots across sleep/wake cycles.
        val stale = poolMutex.withLock {
            val entries = idleClients.toList()
            idleClients.clear()
            currentSize -= entries.size
            entries
        }
        shutdownGracefully(stale.map { it.client })
        lastActivityMs = System.currentTimeMillis()
        startKeepalive()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun acquire(priority: Int = 0): NntpClient {
        val sample = Timer.start(registry)
        val deferred = CompletableDeferred<NntpClient>()
        val waiter = poolMutex.withLock {
            // LIFO: take the most-recently-used connection. Under steady-state
            // trickle traffic, this keeps the "hot" connections refreshed at the
            // tail while older connections age out at the head.
            val entry = idleClients.removeLastOrNull()
            if (entry != null) {
                deferred.complete(entry.client)
                return@withLock null
            }
            val w = PriorityWaiter(priority, deferred, waiterSequence++)
            waiters.add(w)
            incWaiter(priority)
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
            // Timed out — remove waiter and fail. Decrement only if we actually pulled it
            // out of the queue ourselves; if dispatchOrPark already polled it, the dispatch
            // path already decremented.
            val stillQueued = poolMutex.withLock { waiters.remove(waiter) }
            if (stillQueued) decWaiter(priority)
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
            val stillQueued = poolMutex.withLock { waiters.remove(waiter) }
            if (stillQueued) decWaiter(priority)
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
            // Removed from queue → decrement gauge for that priority. Whether the deferred
            // accepts (live waiter) or rejects (cancelled), the waiter is no longer queued.
            decWaiter(waiter.priority)
            if (waiter.deferred.complete(client)) return
            // Waiter was cancelled, try next
        }
        idleClients.addLast(IdleEntry(client, returnedAtMs))
    }

    private suspend fun addClientToPool(client: NntpClient) {
        val orphaned = poolMutex.withLock {
            if (closed) {
                currentSize--
                true
            } else {
                dispatchOrPark(client)
                false
            }
        }
        // Pool was closed mid-launch — the client we just opened is healthy and
        // ours; QUIT it so the server can free the session immediately rather than
        // waiting on TCP timeout. Done outside the mutex so a slow QUIT doesn't
        // block other pool operations.
        if (orphaned) runCatching { client.shutdown(GRACEFUL_QUIT_TIMEOUT_MS) }
    }

    suspend fun <T> withClient(block: suspend (NntpClient) -> T): T = withClient(0, block)

    suspend fun <T> withClient(priority: Int = 0, block: suspend (NntpClient) -> T): T {
        if (sleeping) doWake()
        lastActivityMs = System.currentTimeMillis()
        return supervisorScope {
            flow<T> {
                var client: NntpClient? = null
                var dead = false
                // Set immediately after `block` returns successfully and BEFORE `emit`,
                // so we can distinguish:
                //   - mid-block cancellation  (blockCompleted=false → wire is corrupt → discard)
                //   - post-emit cancellation  (blockCompleted=true  → wire is clean  → return)
                // Post-emit cancellation is the common case — `.first()` below cancels the
                // flow on every successful op after emit. That must NOT mark the connection
                // dead, otherwise every successful command discards its slot.
                var blockCompleted = false
                try {
                    client = acquire(priority)
                    logger.debug(
                        "conn={} acquired by withClient (priority={})",
                        client.connection.id, priority
                    )
                    poolMutex.withLock { activeClients[client] = priority }
                    incActive(priority)
                    val result = block(client)
                    blockCompleted = true
                    emit(result)
                } catch (e: ArticleNotFoundException) {
                    // Server cleanly responded 430 to STAT/BODY — wire is line-protocol
                    // clean, connection stays reusable. The only "benign" exception
                    // type that block can produce; everything else gets the catch-all
                    // below and is treated as protocol-corrupting.
                    throw e
                } catch (e: CancellationException) {
                    // Mid-block cancellation (e.g. an outer withTimeout firing while a
                    // bodyYenc stream is still reading article bytes off the wire) leaves
                    // the wire in a half-read state. Drain inline — wrapped in
                    // NonCancellable so the parent cancellation doesn't kill the drain
                    // mid-read — to article-end before letting the cancellation propagate.
                    // If drain succeeds the connection stays reusable (the finally below
                    // returns it); if drain fails/times-out we mark dead so the finally
                    // discards instead.
                    //
                    // Inline (rather than handing off to a background coroutine) is the
                    // simpler design: the caller waits for the drain, but we don't have
                    // to reason about a race between the drain and the next acquire of
                    // the same connection. The benign post-emit case (blockCompleted=true)
                    // skips the drain entirely.
                    if (!blockCompleted) {
                        client?.let { c ->
                            logger.debug(
                                "conn={} cancellation mid-block, draining inline",
                                c.connection.id
                            )
                            val drained = withContext(NonCancellable) {
                                runCatching { c.connection.drainUntilArticleEnd() }
                                    .getOrDefault(false)
                            }
                            if (!drained) {
                                logger.debug(
                                    "conn={} drain failed; will discard in finally",
                                    c.connection.id
                                )
                                dead = true
                            }
                        }
                    }
                    throw e
                } catch (@Suppress("TooGenericExceptionCaught") e: Throwable) {
                    // Anything else — NntpConnectionException, IOException, NntpProtocolException,
                    // YencDecodingException, YencCrcMismatchException, decode/parse errors,
                    // unexpected runtime exceptions — leaves the wire state unknown. Discard
                    // so a polluted connection can't cascade through the pool. (NntpProtocol-
                    // Exception in particular surfaces when we read a response and get yenc
                    // body bytes from a previous op — without this catch-all the dirty
                    // connection would just be returned to the pool, and the next caller
                    // would also throw NntpProtocolException reading the *next* response,
                    // and so on.)
                    dead = true
                    client?.let { c ->
                        logger.debug(
                            "conn={} marking dead due to {}: {}",
                            c.connection.id, e::class.simpleName, e.message
                        )
                    }
                    throw e
                } finally {
                    client?.let {
                        if (dead) discardClient(it)
                        else returnToPool(it)
                    }
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

    /**
     * Connection failed mid-operation — close it and free its pool slot so the next
     * acquire spawns a fresh replacement. Replaces the old `scheduleReconnect`
     * machinery: the connection is single-socket-for-life, so death = discard.
     */
    private suspend fun discardClient(client: NntpClient) {
        withContext(NonCancellable) {
            poolMutex.withLock {
                val priority = activeClients.remove(client)
                if (priority != null) decActive(priority)
                currentSize--
                // If callers are queued waiting for a connection, spawn a replacement
                // so they're not left hanging until acquire-timeout. Without this kick,
                // a discarded slot stays vacant unless a new acquire happens to walk the
                // currentSize<max branch.
                if (waiters.isNotEmpty() && currentSize < maxConnections && !closed && !sleeping) {
                    currentSize++
                    launchConnection()
                }
            }
            runCatching { client.close() }
        }
    }

    private suspend fun returnToPool(client: NntpClient, returnedAtMs: Long = System.currentTimeMillis()) {
        withContext(NonCancellable) {
            val toClose = poolMutex.withLock {
                val priority = activeClients.remove(client)
                if (priority != null) decActive(priority)
                when {
                    closed -> client
                    // If the pool is sleeping, an in-flight withClient that finishes now must
                    // not park its client — otherwise doSleep leaves an open socket in idleClients
                    // and doWake cleans it up without decrementing currentSize, silently leaking a
                    // pool slot on every sleep/wake cycle with concurrent traffic.
                    sleeping -> {
                        currentSize--
                        client
                    }

                    else -> {
                        dispatchOrPark(client, returnedAtMs)
                        null
                    }
                }
            }
            if (toClose != null) runCatching { toClose.close() }
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
            val toShutdown = poolMutex.withLock {
                closed = true
                // Complete all waiters exceptionally — we own the dequeue so we also own
                // the per-priority decrement.
                while (true) {
                    val waiter = waiters.poll() ?: break
                    decWaiter(waiter.priority)
                    waiter.deferred.completeExceptionally(
                        IllegalStateException("Pool closed")
                    )
                }
                // Snapshot every client we own — both idle and currently checked-out.
                // Active clients are cancelled mid-operation; their owning withClient
                // unwinds via returnToPool which is a no-op once we've closed them here.
                val all = idleClients.map { it.client } + activeClients.keys.toList()
                currentSize -= idleClients.size
                idleClients.clear()
                all
            }
            // Best-effort QUIT in parallel, then hard-close. supervisorScope keeps one
            // bad client from sabotaging the rest of the drain.
            if (toShutdown.isNotEmpty()) {
                supervisorScope {
                    toShutdown.forEach { client ->
                        launch {
                            runCatching { client.shutdown(POOL_CLOSE_QUIT_TIMEOUT_MS) }
                        }
                    }
                }
            }
        }
        registry.find("nntp.pool.idle").tag("pool.name", poolName).gauge()?.id?.let(registry::remove)
        // active and waiters are now multi-tag (priority); clear every series for this pool
        registry.find("nntp.pool.active").tag("pool.name", poolName).gauges().forEach { registry.remove(it.id) }
        registry.find("nntp.pool.size").tag("pool.name", poolName).gauge()?.id?.let(registry::remove)
        registry.find("nntp.pool.waiters").tag("pool.name", poolName).gauges().forEach { registry.remove(it.id) }
        registry.find("nntp.pool.sleeping").tag("pool.name", poolName).gauge()?.id?.let(registry::remove)
        registry.find("nntp.pool.acquire").tag("pool.name", poolName).timers().forEach { timer ->
            registry.remove(timer.id)
        }
    }
}
