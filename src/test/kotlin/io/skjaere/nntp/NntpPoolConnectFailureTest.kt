package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.micrometer.core.instrument.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.ServerSocket
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * How the pool's connects fail. A failure a retry can't change (a rejected login, a host
 * name that doesn't resolve) is tried once; others are retried with a bounded back-off.
 */
class NntpPoolConnectFailureTest {

    private lateinit var serverSocket: ServerSocket
    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope
    private lateinit var callers: CoroutineScope
    private lateinit var registry: SimpleMeterRegistry

    @BeforeEach
    fun setUp() {
        serverSocket = ServerSocket(0)
        selectorManager = SelectorManager(Dispatchers.IO)
        poolScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        callers = CoroutineScope(Dispatchers.IO + SupervisorJob())
        registry = SimpleMeterRegistry()
        Metrics.addRegistry(registry)
    }

    @AfterEach
    fun tearDown() {
        Metrics.removeRegistry(registry)
        callers.cancel()
        poolScope.cancel()
        serverSocket.close()
        selectorManager.close()
    }

    /**
     * Greets every connection and answers AUTHINFO PASS with [passReply] for the n-th login
     * (from 1), STAT and QUIT. Returns the number of logins (AUTHINFO PASS) so far.
     */
    private fun launchLoginServer(passReply: (login: Int) -> String): AtomicInteger {
        val logins = AtomicInteger()
        Thread {
            while (true) {
                val client = try {
                    serverSocket.accept()
                } catch (_: Exception) {
                    break
                }
                Thread {
                    client.use { socket ->
                        try {
                            val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                            val out = socket.getOutputStream()
                            out.write("200 welcome\r\n".toByteArray())
                            out.flush()
                            while (true) {
                                val cmd = reader.readLine() ?: break
                                val reply = when {
                                    cmd.startsWith("AUTHINFO USER") -> "381 password required"
                                    cmd.startsWith("AUTHINFO PASS") -> passReply(logins.incrementAndGet())
                                    cmd.startsWith("STAT") -> "223 0 <test@msg> article exists"
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
                }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
        return logins
    }

    private fun pool(
        host: String = "localhost",
        port: Int = serverSocket.localPort,
        maxConnections: Int = 1,
        username: String? = "user",
        password: String? = "secret"
    ) = NntpClientPool(
        host = host,
        port = port,
        selectorManager = selectorManager,
        username = username,
        password = password,
        maxConnections = maxConnections,
        scope = poolScope,
        keepaliveIntervalMs = 0,
        idleGracePeriodMs = 0
    )

    /** Connects of the pool at [host]:[port] that ended with [outcome], retries included once. */
    private fun connects(outcome: String, host: String = "localhost", port: Int = serverSocket.localPort): Int =
        registry.find("nntp.pool.connect.attempts").tag("pool.name", "$host:$port").tag("outcome", outcome)
            .counter()?.count()?.toInt() ?: 0

    private suspend fun awaitUntil(timeoutMs: Long, message: () -> String, condition: () -> Boolean) {
        val met = withTimeoutOrNull(timeoutMs) {
            while (!condition()) delay(20)
            true
        }
        assertNotNull(met, message())
    }

    @ParameterizedTest
    @ValueSource(ints = [481, 482, 502])
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a rejected login is not retried`(code: Int) = runBlocking<Unit> {
        val logins = launchLoginServer { "$code Authentication rejected" }
        val pool = pool()
        try {
            callers.async { pool.withClient { } }
            awaitUntil(5_000, { "the pool still tries to log in after ${logins.get()} rejected logins" }) {
                connects("failure") == 1
            }
            delay(1_500) // the first retry used to come after 1 s
            assertEquals(1, logins.get(), "logins after a $code")
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a host name that does not resolve is not retried`() = runBlocking<Unit> {
        val host = "nntp-pool-test.invalid"
        val pool = pool(host = host, port = 119, username = null, password = null)
        try {
            callers.async { pool.withClient { } }
            awaitUntil(5_000, { "the pool still retries a host that does not resolve" }) {
                connects("failure", host, 119) == 1
            }
        } finally {
            pool.close()
        }
    }

    @Test
    @Timeout(15, unit = TimeUnit.SECONDS)
    fun `a connect that fails for a reason that can pass is retried`() = runBlocking<Unit> {
        val accepted = AtomicInteger()
        Thread {
            while (true) {
                val client = try {
                    serverSocket.accept()
                } catch (_: Exception) {
                    break
                }
                val first = accepted.incrementAndGet() == 1
                Thread {
                    client.use { socket ->
                        try {
                            val out = socket.getOutputStream()
                            if (first) {
                                out.write("400 too busy, try again later\r\n".toByteArray())
                                out.flush()
                                return@use
                            }
                            out.write("200 welcome\r\n".toByteArray())
                            out.flush()
                            val reader = BufferedReader(InputStreamReader(socket.getInputStream()))
                            while (true) {
                                val cmd = reader.readLine() ?: break
                                val reply = if (cmd.startsWith("STAT")) "223 0 <test@msg> article exists" else "205 bye"
                                out.write("$reply\r\n".toByteArray())
                                out.flush()
                                if (cmd == "QUIT") break
                            }
                        } catch (_: Exception) {
                        }
                    }
                }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
        val pool = pool(username = null, password = null)
        try {
            val result = pool.stat("<test@msg>")
            assertTrue(result is StatResult.Found, "STAT after a retried connect: $result")
            assertEquals(2, accepted.get(), "connections")
        } finally {
            pool.close()
        }
    }

    /**
     * Without back-off, a rejected connect frees its slot at once, so each acquire queued
     * behind the pool's open connection would start another login the server rejects: a
     * provider whose account allows fewer connections than the pool's maximum would see a
     * failed login per command.
     */
    @Test
    @Timeout(20, unit = TimeUnit.SECONDS)
    fun `a rejected login does not make the acquires queued behind an open connection log in again`() =
        runBlocking<Unit> {
            val logins = launchLoginServer { if (it == 1) "281 welcome" else "502 Too many connections" }
            val pool = pool(maxConnections = 4)
            try {
                val held = CompletableDeferred<Unit>()
                val release = CompletableDeferred<Unit>()
                val holder = callers.async {
                    pool.withClient {
                        held.complete(Unit)
                        release.await()
                        it.stat("<test@msg>")
                    }
                }
                held.await()

                val firstWave = List(3) { callers.async { pool.stat("<test@msg>") } }
                awaitUntil(5_000, { "rejected connects so far: ${connects("failure")} of 3" }) {
                    connects("failure") == 3
                }
                val secondWave = List(3) { callers.async { pool.stat("<test@msg>") } }
                delay(500)
                assertEquals(4, logins.get(), "logins: the second wave logged in again although a connection is open")

                release.complete(Unit)
                val results = withTimeoutOrNull(10_000) { (listOf(holder) + firstWave + secondWave).awaitAll() }
                assertNotNull(results, "the queued acquires were not served by the open connection")
                assertTrue(results.all { it is StatResult.Found }, "results: $results")
            } finally {
                pool.close()
            }
        }
}
