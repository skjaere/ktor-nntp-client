package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.Timer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
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
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.flow.retryWhen
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
import java.security.cert.CertificateException
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
    private val commandRetries: Int = DEFAULT_COMMAND_RETRIES,
    // How long a read waits for the server's next bytes; see NntpConnection.connect.
    private val readTimeoutMs: Long = NntpConnection.DEFAULT_READ_TIMEOUT_MS,
    // With useTls, check the certificate names host and send it as SNI; see NntpConnection.connect.
    private val verifyHostname: Boolean = true
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

        // After a connect fails in a way a retry can't change (isPermanentConnectFailure), the
        // pool opens no new connection for this long while it has another one, open or on its
        // way: the acquires queued behind it are served as connections come back. Otherwise each
        // queued acquire would start a login the server rejects, e.g. "502 Too many connections"
        // when the account allows fewer connections than maxConnections.
        const val CONNECT_REJECTED_BACKOFF_MS = 30_000L

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

        // Upper bound on the cancellation-recovery drain. drainUntilArticleEnd's job is to
        // read the rest of a half-consumed body so the connection can be reused — but it
        // runs under withContext(NonCancellable), so a server that stops sending bytes after
        // a cancelled fetch (article missing post-status, network glitch mid-body, etc.) will
        // pin the calling coroutine on `readAvailable` indefinitely. That coroutine still
        // holds the pool slot via activeClients, so each leaked drain permanently removes
        // one connection from circulation. A 3-second cap is well above the worst-case
        // drain on a healthy wire (~one yenc-segment worth of bytes ≈ 750 KB) and well below
        // any reasonable client patience for "we already cancelled this anyway".
        const val DRAIN_TIMEOUT_MS = 3_000L

        // Upper bound on a single TCP+TLS+welcome+AUTHINFO handshake. NntpConnection.connect
        // does `readChannel.readLine()` to consume the welcome banner, and NntpConnection's
        // performAuth does two more `readResponse()` -> readLine() calls for AUTHINFO USER
        // and AUTHINFO PASS — each bounded only by the idle read timeout (readTimeoutMs),
        // which a server trickling bytes never trips and which can be disabled. When the server
        // is alive at TCP level but slow/silent at the protocol level (account session limit
        // hit, throttling, post-handshake stall), these reads can suspend that long or
        // indefinitely, leaving the launchConnection coroutine pinned in "connecting" state
        // with the pool slot reserved. Connect-with-retry's exponential backoff doesn't help
        // here because the inner attempt doesn't return to be retried.
        //
        // 20 s is loose enough to cover worst-case healthy handshakes (TCP ~3 s + TLS ~1 s +
        // welcome <0.1 s + AUTHINFO round-trips <1 s = ~5 s healthy, ~10 s degraded) and
        // strict enough to keep one stalled peer from indefinitely consuming a slot.
        const val CONNECT_TIMEOUT_MS = 20_000L
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

    private val opened = AtomicLong()

    // Until when new connections wait after a rejected connect; see CONNECT_REJECTED_BACKOFF_MS.
    @Volatile
    private var connectRejectedUntilMs = 0L

    /**
     * Connections this pool has opened (connected, greeted and logged in) since it was created.
     * Read it before and after an operation to see how many connects the operation waited for.
     */
    val connectionsOpened: Long get() = opened.get()

    private val poolName = "$host:$port"

    private val registry = Metrics.globalRegistry

    init {
        Gauge.builder("nntp.pool.idle", idleClients) { it.size.toDouble() }
            .tag("pool.name", poolName)
            .register(registry)
        Gauge.builder("nntp.pool.size", this) { it.currentSize.toDouble() }
            .tag("pool.name", poolName)
            .register(registry)
        // Connections whose slot has been reserved (`currentSize` bumped) but that
        // haven't yet completed `launchConnection` — i.e. stuck in TLS handshake,
        // AUTHINFO, or the connect-retry backoff. Without this gauge the only way
        // to see "pool is wedged producing connections it can't finish" is the
        // derived value `size − idle − active`, which doesn't surface on a
        // single panel. Read is lock-free and may briefly race with the maps;
        // coerced to ≥0 to avoid transient negative values during mutation.
        Gauge.builder("nntp.pool.connecting", this) { pool ->
            (pool.currentSize - pool.idleClients.size - pool.activeClients.size)
                .coerceAtLeast(0).toDouble()
        }.tag("pool.name", poolName).register(registry)
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

    private fun recordAcquireFailure(priority: Int, reason: String) {
        Counter.builder("nntp.pool.acquire.failures")
            .tag("pool.name", poolName)
            .tag("priority", priority.toString())
            .tag("reason", reason)
            .register(registry)
            .increment()
    }

    private fun recordConnectAttempt(outcome: String) {
        Counter.builder("nntp.pool.connect.attempts")
            .tag("pool.name", poolName)
            .tag("outcome", outcome)
            .register(registry)
            .increment()
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
            val client = try {
                connectWithRetry()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.warn("Connection attempt failed permanently: {}", e.message)
                recordConnectAttempt("failure")
                withContext(NonCancellable) { connectFailed(e) }
                return@launch
            }
            opened.incrementAndGet()
            recordConnectAttempt("success")
            connectRejectedUntilMs = 0L
            withContext(NonCancellable) { addClientToPool(client) }
        }
    }

    /**
     * Frees the slot of a connect that failed for good with [cause]. If that leaves the pool
     * with no connection, open or on its way, nothing can serve the acquires still waiting:
     * they fail now with [cause] instead of timing out. While the pool has another connection
     * they wait for it.
     */
    private suspend fun connectFailed(cause: Exception) {
        poolMutex.withLock {
            currentSize--
            if (cause.isPermanentConnectFailure()) {
                connectRejectedUntilMs = System.currentTimeMillis() + CONNECT_REJECTED_BACKOFF_MS
            }
            if (currentSize > 0) return@withLock
            // We own the dequeue, so we also own the per-priority decrement. A waiter being
            // withdrawn has its deferred cancelled already: completing it fails, and withdraw
            // finds it gone from the queue.
            while (true) {
                val waiter = waiters.poll() ?: break
                decWaiter(waiter.priority)
                if (waiter.deferred.completeExceptionally(failureFor(cause))) {
                    recordAcquireFailure(waiter.priority, "connect_failed")
                }
            }
        }
    }

    /**
     * What an acquire whose connect failed with [cause] throws: a rejected login stays an
     * [NntpAuthenticationException], anything else is an [NntpConnectionException]. One for
     * each waiter, as they throw it from different coroutines.
     */
    private fun failureFor(cause: Exception): Exception {
        val message = cause.message ?: cause.toString()
        return when (cause) {
            is NntpAuthenticationException -> NntpAuthenticationException(message, cause.response, cause)
            is NntpConnectionException -> NntpConnectionException(message, cause)
            else -> NntpConnectionException("Could not connect to $host:$port: $message", cause)
        }
    }

    /**
     * Under [poolMutex]: whether a waiter may have a new connection opened for it. Not at the
     * maximum, nor while a rejected connect's back-off runs and another connection can serve
     * the waiter (see [CONNECT_REJECTED_BACKOFF_MS]).
     */
    private fun canOpenConnection(): Boolean =
        currentSize < maxConnections &&
            (currentSize == 0 || System.currentTimeMillis() >= connectRejectedUntilMs)

    /**
     * Connects, retrying with exponential back-off (1 s doubling, at most [CONNECT_MAX_RETRIES]
     * retries) a failure that may pass: refused, reset, timed out, a greeting other than 200/201.
     * A failure a retry can't change ([isPermanentConnectFailure]) is thrown at once.
     */
    private suspend fun connectWithRetry(): NntpClient {
        var delayMs = CONNECT_RETRY_BASE_DELAY_MS
        return flow {
            // Bound each individual connect attempt. The inner NntpClient.connect path has
            // three unbounded readLine() suspends (welcome + AUTHINFO USER ack + AUTHINFO
            // PASS ack); without this wrapper, a silent-but-alive server pins the attempt
            // forever and the existing .retry below never gets a chance to fire. On
            // expiry we surface as NntpConnectionException so the same retry path that
            // handles real connect failures (IO errors, EOF mid-handshake, etc.) treats
            // a timed-out attempt the same way.
            val client = withTimeoutOrNull(CONNECT_TIMEOUT_MS) {
                if (username != null && password != null)
                    NntpClient.connect(host, port, selectorManager, useTls, username, password, readTimeoutMs, verifyHostname)
                else
                    NntpClient.connect(host, port, selectorManager, useTls, readTimeoutMs, verifyHostname)
            } ?: throw NntpConnectionException(
                "Connect to $host:$port timed out after ${CONNECT_TIMEOUT_MS}ms"
            )
            emit(client)
        }.retry(CONNECT_MAX_RETRIES) { e ->
            if (e.isPermanentConnectFailure()) {
                logger.error("Failed to connect to {}:{}, not retrying: {}", host, port, e.message ?: e.toString())
                return@retry false
            }
            logger.warn("Failed to connect to {}:{}: {}", host, port, e.message)
            delay(delayMs)
            delayMs = (delayMs * 2).coerceAtMost(CONNECT_RETRY_MAX_DELAY_MS)
            true
        }.first()
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
            if (canOpenConnection()) {
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
        val client = try {
            withTimeoutOrNull(ACQUIRE_TIMEOUT_MS.milliseconds) { deferred.await() }
        } catch (e: CancellationException) {
            withdraw(waiter)
            throw e
        }
        if (client != null) {
            recordAcquire(sample, priority)
            return client
        }
        withdraw(waiter)
        recordAcquireFailure(priority, "timeout")
        throw NntpConnectionException(
            "Timed out waiting for connection to $host:$port (${ACQUIRE_TIMEOUT_MS}ms)"
        )
    }

    /**
     * Takes [waiter] back after its acquire timed out or was cancelled. Cancelling its
     * deferred first makes this atomic with [dispatchOrPark]: from then on complete() fails
     * and the dispatcher moves on, so no connection can be handed to a waiter nobody awaits.
     * If the deferred already held a client, it's ours and goes back to the pool. The rest
     * runs under NonCancellable: the caller is usually cancelled already, and a contended
     * Mutex.lock throws at once in a cancelled coroutine, which used to skip this cleanup.
     */
    @OptIn(ExperimentalCoroutinesApi::class)
    private suspend fun withdraw(waiter: PriorityWaiter) {
        waiter.deferred.cancel()
        val delivered = if (waiter.deferred.isCancelled) null else waiter.deferred.getCompleted()
        withContext(NonCancellable) {
            // Decrement only if we pulled it out of the queue ourselves; if dispatchOrPark
            // already polled it, the dispatch path already decremented.
            val stillQueued = poolMutex.withLock { waiters.remove(waiter) }
            if (stillQueued) decWaiter(waiter.priority)
            delivered?.let { addClientToPool(it) }
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

    /** [NntpClient.statPipelined] on one pooled connection, acquired at [priority]. */
    suspend fun statPipelined(
        messageIds: List<String>,
        priority: Int = 0,
        window: Int = 64,
        onResult: ((index: Int, result: StatResult) -> Unit)? = null,
    ): List<StatResult> = withClient(priority) { it.statPipelined(messageIds, window, onResult) }

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
                    // A yEnc body's pump runs in Ktor's writer, which keeps its failure in the
                    // Body channel and completes normally, so block can return with the body
                    // unfinished: the socket dead or the rest of the article on the wire. So can
                    // a block that caught a command's failure or cancellation itself and carried
                    // on, with that command's answer still owed or the socket dead.
                    if (!client.connection.isReusable) {
                        logger.debug(
                            "conn={} wire left out of step; will discard in finally",
                            client.connection.id
                        )
                        dead = true
                    }
                    blockCompleted = true
                    emit(result)
                } catch (e: ArticleNotFoundException) {
                    // Server cleanly responded 430 to STAT/BODY — wire is line-protocol
                    // clean, connection stays reusable. The only "benign" exception
                    // type that block can produce; everything else gets the catch-all
                    // below and is treated as protocol-corrupting. Unless block left the wire
                    // out of step before, as checked above for a normal return.
                    if (client?.connection?.isReusable == false) dead = true
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
                            // Nested withTimeoutOrNull inside NonCancellable: the outer
                            // context blocks PARENT cancellation from reaching us, but the
                            // timeout's own Job (a child of NonCancellable) can still cancel
                            // the inner readAvailable on expiry. Without this bound, a server
                            // that stops sending after a cancelled fetch pins the drain
                            // coroutine forever — and the connection along with it — because
                            // readAvailable just keeps waiting for bytes that will never come.
                            // On timeout we treat the wire as unrecoverable: mark dead so the
                            // finally below discards the client and frees its pool slot.
                            val drained = withContext(NonCancellable) {
                                runCatching {
                                    withTimeoutOrNull(DRAIN_TIMEOUT_MS) {
                                        c.connection.drainUntilArticleEnd()
                                    } ?: false
                                }.getOrDefault(false)
                            }
                            if (!drained) {
                                logger.debug(
                                    "conn={} drain failed or timed out after {}ms; will discard in finally",
                                    c.connection.id, DRAIN_TIMEOUT_MS,
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
            }.retryWhen { e, attempt ->
                // retryWhen rather than retry, which rejects commandRetries = 0. A connect that
                // failed for good (acquire passes its cause on) fails the same again.
                val retryable = attempt < commandRetries &&
                    (e is NntpConnectionException || e is IOException) && !e.isPermanentConnectFailure()
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
                if (waiters.isNotEmpty() && canOpenConnection() && !closed && !sleeping) {
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
        registry.find("nntp.pool.connecting").tag("pool.name", poolName).gauge()?.id?.let(registry::remove)
        registry.find("nntp.pool.waiters").tag("pool.name", poolName).gauges().forEach { registry.remove(it.id) }
        registry.find("nntp.pool.sleeping").tag("pool.name", poolName).gauge()?.id?.let(registry::remove)
        registry.find("nntp.pool.acquire").tag("pool.name", poolName).timers().forEach { timer ->
            registry.remove(timer.id)
        }
        registry.find("nntp.pool.acquire.failures").tag("pool.name", poolName)
            .counters().forEach { registry.remove(it.id) }
        registry.find("nntp.pool.connect.attempts").tag("pool.name", poolName)
            .counters().forEach { registry.remove(it.id) }
    }
}

// AUTHINFO answers that reject the login itself: 481 (rejected), 482 (out of sequence) and 502
// (not allowed, e.g. too many connections for the account). Asking again gets the same answer.
private val REJECTED_LOGIN_CODES = setOf(481, 482, 502)

/**
 * A connect failure a retry can't change: the server rejected the login, the host name doesn't
 * resolve, or TLS refused the server's certificate. Anything else (refused, reset, timed out, a
 * 400 greeting) may pass.
 */
private fun Throwable.isPermanentConnectFailure(): Boolean =
    (this is NntpAuthenticationException && response?.code in REJECTED_LOGIN_CODES) ||
        generateSequence(this) { it.cause }.any { it is UnresolvedAddressException || it is CertificateException }
