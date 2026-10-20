package io.skjaere.nntp

import org.conscrypt.Conscrypt
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import java.security.Security
import javax.net.ssl.SSLContext
import kotlin.test.assertEquals

/**
 * [NntpTls13TransportTest] with Conscrypt's TLS as the client's, as on Android (the servers stay the JDK's): its engine
 * differs from the JDK's in what it asks for and when (a record's end, tasks it runs itself, what closing leaves to
 * send). Conscrypt's OpenJDK build, with BoringSSL as on Android; its provider first while these run.
 */
internal class NntpTlsConscryptTransportTest : NntpTls13TransportTest() {
    @Test
    fun `the client's TLS is Conscrypt's`() {
        assertEquals(conscrypt.name, SSLContext.getInstance("TLS").provider.name)
    }

    companion object {
        private val conscrypt = Conscrypt.newProvider()

        @JvmStatic
        @BeforeAll
        fun useConscrypt() {
            // The JDK's TLS (the servers') sets up its named groups once, with the first provider's EC parameters:
            // Conscrypt's would refuse the JDK's binary-field curves, so the JDK's go first.
            SSLContext.getInstance("TLS", "SunJSSE").apply { init(null, null, null) }.createSSLEngine()
            Security.insertProviderAt(conscrypt, 1)
        }

        @JvmStatic
        @AfterAll
        fun stopUsingConscrypt() {
            Security.removeProvider(conscrypt.name)
        }
    }
}
