package io.skjaere.nntp

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

/**
 * [NntpTlsTransportTest] over TLS 1.3, which the platform's TLS negotiates with any server that offers it: a provider's
 * most likely does. And what only 1.3 has, a key update.
 */
internal open class NntpTls13TransportTest : NntpTlsTransportTest() {
    override val protocols: Array<String> = arrayOf("TLSv1.3")

    /**
     * A server may ask for new keys in the middle of a connection (a JDK server does on reaching a key's limit). The
     * client answers from its read, as nothing else may write meanwhile, and goes on with new keys of its own: the rest
     * of the article comes whole, and the next command and its answer go through on the same connection.
     */
    @Test
    fun `a key update the server asks for mid-article is answered and the connection goes on`() = runBlocking<Unit> {
        val first = Random(31).nextBytes(400_000)
        val second = Random(32).nextBytes(30_000)
        val whole = article(first)
        val server = server { session ->
            session.reader.readLine() // the first BODY
            session.out.write(whole, 0, whole.size / 2)
            session.out.flush()
            session.updateKeys()
            session.out.write(whole, whole.size / 2, whole.size - whole.size / 2)
            session.out.flush()
            answering(article(second))(session)
        }
        client(server).use { client ->
            assertContentEquals(first, client.bodyYencBytes("<first@test>").data)
            assertContentEquals(second, client.bodyYencBytes("<second@test>").data, "the command after the key update")
        }
        assertEquals(listOf("TLSv1.3"), server.negotiated)
    }
}
