package io.skjaere.nntp

import io.ktor.network.selector.*
import io.ktor.utils.io.*
import io.skjaere.mocknntp.testcontainer.MockNntpServerContainer
import io.skjaere.yenc.RapidYenc
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull

@Testcontainers
class NntpClientIntegrationTest {

    companion object {
        @Container
        @JvmStatic
        val mockServer = MockNntpServerContainer()
    }

    private lateinit var selectorManager: SelectorManager
    private lateinit var poolScope: CoroutineScope

    @BeforeEach
    fun setUp() = runBlocking {
        selectorManager = SelectorManager(Dispatchers.IO)
        poolScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        mockServer.client.clearExpectations()
    }

    @AfterEach
    fun tearDown() {
        poolScope.cancel()
        selectorManager.close()
    }

    private suspend fun createPool(maxConnections: Int = 1): NntpClientPool {
        val pool = NntpClientPool(
            host = mockServer.nntpHost,
            port = mockServer.nntpPort,
            selectorManager = selectorManager,
            maxConnections = maxConnections,
            scope = poolScope,
            /*username = "username",
            password = "password"*/
        )
        pool.wake()
        return pool
    }


}
