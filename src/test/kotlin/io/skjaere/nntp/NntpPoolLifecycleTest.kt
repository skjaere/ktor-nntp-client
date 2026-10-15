package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.job
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.BufferedReader
import java.io.Closeable
import java.io.InputStreamReader
import java.net.ServerSocket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/**
 * The pool's life: once closed it serves nothing and opens nothing, and the work in flight at
 * close ends promptly. A connect that finishes is never lost, whatever cancels it at the end.
 * Awake, it always has its keepalive.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class NntpPoolLifecycleTest {

    private lateinit var serverSocket: ServerSocket
    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope
    private lateinit var callers: CoroutineScope

    @BeforeEach
    fun setUp() {
        serverSocket = ServerSocket(0)
        selectorManager = SelectorManager(Dispatchers.IO)
        poolScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        callers = CoroutineScope(Dispatchers.IO + SupervisorJob())
    }

    @AfterEach
    fun tearDown() {
        callers.cancel()
        poolScope.cancel()
        serverSocket.close()
        selectorManager.close()
    }

    private class Server {
        val accepted = AtomicInteger()
        val closed = AtomicInteger()
        val statReceived = CountDownLatch(1)
    }

    /**
     * Accepts connections and greets them, or with [greet] false closes each at once. Answers
     * STAT unless [answerStat] is false (then counts [Server.statReceived] down and leaves it
     * unanswered), DATE and QUIT.
     */
    private fun launchServer(greet: Boolean = true, answerStat: Boolean = true): Server {
        val server = Server()
        Thread {
            while (true) {
                val client = try {
                    serverSocket.accept()
                } catch (_: Exception) {
                    break
                }
                server.accepted.incrementAndGet()
                Thread {
                    client.use { socket ->
                        try {
                            if (!greet) return@use
                            val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                            val out = socket.getOutputStream()
                            out.write("200 welcome\r\n".toByteArray())
                            out.flush()
                            while (true) {
                                val cmd = reader.readLine() ?: break
                                val reply = when {
                                    cmd.startsWith("STAT") && !answerStat -> {
                                        server.statReceived.countDown()
                                        continue
                                    }
                                    cmd.startsWith("STAT") -> "223 0 <test@msg> article exists"
                                    cmd == "DATE" -> "111 20260224120000"
                                    cmd == "QUIT" -> "205 bye"
                                    else -> "500 unknown command"
                                }
                                out.write("$reply\r\n".toByteArray())
                                out.flush()
                                if (cmd == "QUIT") break
                            }
                        } catch (_: Exception) {
                        }
                    }
                    server.closed.incrementAndGet()
                }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
        return server
    }

    private fun pool(
        maxConnections: Int = 1,
        keepaliveIntervalMs: Long = 0,
        idleGracePeriodMs: Long = 0
    ) = NntpClientPool(
        host = "localhost",
        port = serverSocket.localPort,
        selectorManager = selectorManager,
        maxConnections = maxConnections,
        scope = poolScope,
        keepaliveIntervalMs = keepaliveIntervalMs,
        idleGracePeriodMs = idleGracePeriodMs
    )

    /**
     * How [call] ended: its failure, or null if it succeeded. Fails the test if it hasn't ended
     * within [PROMPT_MS]; a cancellation counts as an ending.
     */
    private suspend fun endingOf(call: Deferred<*>): Throwable? {
        assertNotNull(
            withTimeoutOrNull(PROMPT_MS) { call.join() },
            "the call was still running ${PROMPT_MS}ms after the pool closed"
        )
        return call.getCompletionExceptionOrNull()
    }

    private fun assertPoolClosedFailure(failure: Throwable?) {
        assertNotNull(failure, "the call succeeded on a closed pool")
        assertFalse(failure is CancellationException, "the call ended as cancelled: $failure")
        assertTrue(
            failure is IllegalStateException && failure.message == "Pool closed",
            "expected IllegalStateException(\"Pool closed\"), got $failure"
        )
    }

    /** Waits until nothing the pool launched (connects, the keepalive) is running any more. */
    private suspend fun assertPoolScopeSettles(what: String) {
        val settled = withTimeoutOrNull(PROMPT_MS) {
            while (poolScope.coroutineContext.job.children.any()) delay(10)
        }
        assertNotNull(settled, "$what was still running ${PROMPT_MS}ms after the pool closed")
    }

    // --- K15: close ---

    @ParameterizedTest(name = "closed while asleep: {0}")
    @ValueSource(booleans = [false, true])
    @Timeout(30, unit = TimeUnit.SECONDS)
    fun `an acquire after close fails at once and opens no connection`(asleep: Boolean) = runBlocking<Unit> {
        val server = launchServer()
        val pool = pool(keepaliveIntervalMs = 100)
        pool.wake()
        pool.stat("<test@msg>")
        if (asleep) pool.sleep()
        pool.close()
        val acceptedAtClose = server.accepted.get()

        // On a sleeping pool withClient wakes it first, which must not bring it back to life.
        assertPoolClosedFailure(endingOf(callers.async { pool.stat("<test@msg>") }))

        delay(300) // a connect opened for the acquire would have been accepted by now
        assertEquals(acceptedAtClose, server.accepted.get(), "connections opened after close")
        assertPoolScopeSettles("a keepalive or connect")
    }

    @Test
    @Timeout(30, unit = TimeUnit.SECONDS)
    fun `a command in flight at close fails at once and nothing reconnects`() = runBlocking<Unit> {
        val server = launchServer(answerStat = false)
        val pool = pool()
        pool.wake()
        val inFlight = callers.async { pool.stat("<test@msg>") }
        assertTrue(server.statReceived.await(5, TimeUnit.SECONDS), "the STAT never reached the server")

        pool.close()

        assertPoolClosedFailure(endingOf(inFlight))
        delay(300)
        assertEquals(1, server.accepted.get(), "connections opened after close")
        assertEquals(1, server.closed.get(), "the in-flight connection should be closed")
        assertPoolScopeSettles("a connect")
    }

    @Test
    @Timeout(30, unit = TimeUnit.SECONDS)
    fun `a connect in flight at close is abandoned`() = runBlocking<Unit> {
        // Every connect ends before the greeting, a failure the pool retries after 1 s, 2 s, ...
        val server = launchServer(greet = false)
        val pool = pool()
        pool.wake()
        val waiting = callers.async { pool.withClient { } }
        withTimeoutOrNull(5_000) { while (server.accepted.get() == 0) delay(10) }
        assertEquals(1, server.accepted.get(), "the pool never connected")

        pool.close()

        assertPoolClosedFailure(endingOf(waiting))
        assertPoolScopeSettles("the connect")
        delay(1_500) // past the first retry's back-off
        assertEquals(1, server.accepted.get(), "connects after close")
    }

    // --- K16: the per-attempt connect timeout ---

    private class FakeClient : Closeable {
        @Volatile
        var closed = false

        override fun close() {
            closed = true
        }
    }

    /**
     * Stands in for a connect: it waits on the network (suspends), then reads the server's last
     * answer, and what's left (parsing it, building the client) runs without suspending. Here
     * that last step waits, without suspending, until this attempt is cancelled (its timeout
     * fired, or its caller was cancelled), and finishes all the same with [client].
     */
    private suspend fun finishAfterCancellation(client: FakeClient, started: CompletableDeferred<Unit>? = null): FakeClient {
        // A block that never suspends keeps its result past the timeout (withTimeout's
        // undispatched fast path); a real connect always suspends first.
        yield()
        val attempt = currentCoroutineContext().job
        started?.complete(Unit)
        val giveUpAt = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
        while (attempt.isActive && System.nanoTime() < giveUpAt) Thread.sleep(5)
        assertFalse(attempt.isActive, "the attempt was never cancelled")
        return client
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a connect that finishes as its time runs out keeps its client`() = runBlocking<Unit> {
        val client = FakeClient()
        // Off runBlocking's thread, so the timeout fires while the connect is still running.
        val connected = withContext(Dispatchers.Default) {
            connectWithin(timeoutMs = 50) { finishAfterCancellation(client) }
        }
        assertSame(client, connected, "the client connected as the time ran out was dropped")
        assertFalse(client.closed, "the client kept should be open")
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a connect that finishes as its caller is cancelled closes its client`() = runBlocking<Unit> {
        val client = FakeClient()
        val started = CompletableDeferred<Unit>()
        val attempt = callers.launch {
            connectWithin(timeoutMs = 60_000) { finishAfterCancellation(client, started) }
        }
        started.await()
        attempt.cancel()
        attempt.join()
        assertTrue(client.closed, "the client connected as its caller was cancelled was never closed")
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a connect that does not finish in time gives up`() = runBlocking<Unit> {
        val client = FakeClient()
        val connected = connectWithin(timeoutMs = 50) {
            delay(10_000)
            client
        }
        assertNull(connected, "a connect still running at its deadline should give up")
    }

    // --- K17: sleep and wake ---

    /** The pool's private lock, so a test can hold it the way a concurrent pool operation does. */
    private fun NntpClientPool.poolMutex(): Mutex =
        NntpClientPool::class.java.getDeclaredField("poolMutex").apply { isAccessible = true }.get(this) as Mutex

    /**
     * A sleep and a wake that cross. The sleep cancelled the keepalive after releasing the lock,
     * so it could cancel the keepalive a wake had just started: the pool stayed awake with no
     * keepalive to check its connections, evict them or put it back to sleep. Here the wake runs
     * (unconfined) inside the sleep's unlock, the narrowest form of a race between two threads.
     */
    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a wake that crosses a sleep keeps its keepalive`() = runBlocking<Unit> {
        val registry = SimpleMeterRegistry()
        Metrics.addRegistry(registry)
        fun sleeping() = registry.find("nntp.pool.sleeping")
            .tag("pool.name", "localhost:${serverSocket.localPort}").gauge()?.value()
        val pool = pool(keepaliveIntervalMs = 100, idleGracePeriodMs = 300)
        try {
            pool.wake()
            val lock = pool.poolMutex()
            lock.lock()
            // Both wait for the lock, the sleep first. This thread resumes the sleep; the sleep's
            // unlock resumes the wake, which runs there and then, before the sleep goes on.
            val sleep = launch(start = CoroutineStart.UNDISPATCHED) { pool.sleep() }
            val wake = launch(Dispatchers.Unconfined) { pool.wake() }
            lock.unlock()
            sleep.join()
            wake.join()
            assertEquals(0.0, sleeping(), "the wake came last: the pool should be awake")

            // An idle pool's keepalive puts it to sleep once the grace period has passed.
            val slept = withTimeoutOrNull(3_000) { while (sleeping() != 1.0) delay(20) }
            assertNotNull(slept, "the pool stayed awake with no keepalive to put it to sleep")
        } finally {
            pool.close()
            Metrics.removeRegistry(registry)
        }
    }

    private companion object {
        // Promptly: well under the pool's 30 s acquire timeout and 1 s first connect retry.
        const val PROMPT_MS = 2_000L
    }
}
