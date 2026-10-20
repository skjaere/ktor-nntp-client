package io.skjaere.nntp

import java.io.BufferedReader
import java.io.Closeable
import java.io.IOException
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.net.ssl.ExtendedSSLSession
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.SNIHostName
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLServerSocket
import javax.net.ssl.SSLSocket
import javax.net.ssl.SSLSocketFactory
import kotlin.test.fail

private const val PASSWORD = "changeit"

/**
 * Self-signed RSA certificates made with the test JDK's keytool, one keystore per alias,
 * and a trust store that trusts them all.
 */
internal class TestCertificates(private val dir: Path, certificates: Map<String, Pair<String, String?>>) : Closeable {

    /** The keystore holding [alias]'s key and certificate. */
    fun keyStore(alias: String): Path = dir.resolve("$alias.p12")

    val trustStore: Path = dir.resolve("trust.p12")

    init {
        // alias to (subject, subjectAltName extension or null for none)
        certificates.forEach { (alias, certificate) ->
            val (subject, san) = certificate
            keytool(
                listOf(
                    "-genkeypair", "-alias", alias, "-keyalg", "RSA", "-keysize", "2048", "-validity", "3650",
                    "-dname", subject, "-keystore", keyStore(alias).toString(), "-storetype", "PKCS12",
                    "-storepass", PASSWORD, "-keypass", PASSWORD
                ) + (san?.let { listOf("-ext", "SAN=$it") } ?: emptyList())
            )
            val cert = dir.resolve("$alias.cer")
            keytool("-exportcert", "-alias", alias, "-keystore", keyStore(alias).toString(), "-storepass", PASSWORD, "-file", cert.toString())
            keytool(
                "-importcert", "-noprompt", "-alias", alias, "-file", cert.toString(),
                "-keystore", trustStore.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD
            )
        }
    }

    private fun keytool(vararg args: String) = keytool(args.toList())

    private fun keytool(args: List<String>) {
        val keytool = Path.of(System.getProperty("java.home"), "bin", "keytool").toString()
        val process = ProcessBuilder(listOf(keytool) + args).redirectErrorStream(true).start()
        val output = process.inputStream.bufferedReader().readText()
        if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) fail("keytool ${args.first()} failed: $output")
    }

    private val saved = mutableMapOf<String, String?>()

    /**
     * Makes [trustStore] the JVM's default trust store, the one the library's TLS checks
     * certificates against, until [restoreDefaultTrustStore].
     */
    fun useAsDefaultTrustStore() {
        mapOf(
            "javax.net.ssl.trustStore" to trustStore.toString(),
            "javax.net.ssl.trustStoreType" to "PKCS12",
            "javax.net.ssl.trustStorePassword" to PASSWORD
        ).forEach { (key, value) ->
            saved[key] = System.getProperty(key)
            System.setProperty(key, value)
        }
    }

    fun restoreDefaultTrustStore() {
        saved.forEach { (key, value) -> if (value == null) System.clearProperty(key) else System.setProperty(key, value) }
        saved.clear()
    }

    override fun close() {
        dir.toFile().deleteRecursively()
    }

    companion object {
        fun create(certificates: Map<String, Pair<String, String?>>): TestCertificates =
            TestCertificates(Files.createTempDirectory("nntp-tls-test"), certificates)
    }
}

/**
 * A TLS 1.2 NNTP server presenting the certificate in [keyStore]. Greets each connection that
 * completes the handshake and answers STAT and QUIT. Records the server names (SNI) each
 * handshake asked for.
 */
internal class TlsNntpServer(keyStore: Path) : Closeable {

    private val serverSocket: SSLServerSocket

    /** For each connection that completed the handshake, the server names it asked for. */
    val requestedServerNames = CopyOnWriteArrayList<List<String>>()

    /** Connections accepted, handshake completed or not. */
    val accepted get() = acceptedCount.get()
    private val acceptedCount = java.util.concurrent.atomic.AtomicInteger()

    val port: Int get() = serverSocket.localPort

    init {
        val keys = KeyStore.getInstance("PKCS12").apply {
            Files.newInputStream(keyStore).use { load(it, PASSWORD.toCharArray()) }
        }
        val keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
            .apply { init(keys, PASSWORD.toCharArray()) }.keyManagers
        val context = SSLContext.getInstance("TLS").apply { init(keyManagers, null, null) }
        // Ktor's TLS client speaks TLS 1.2 only.
        serverSocket = (context.serverSocketFactory.createServerSocket(0) as SSLServerSocket)
            .apply { enabledProtocols = arrayOf("TLSv1.2") }
        Thread {
            while (true) {
                val socket = try {
                    serverSocket.accept() as SSLSocket
                } catch (_: IOException) {
                    break
                }
                acceptedCount.incrementAndGet()
                Thread { serve(socket) }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
    }

    private fun serve(socket: SSLSocket) {
        (socket as Socket).use {
            try {
                socket.startHandshake()
                requestedServerNames.add(
                    (socket.session as ExtendedSSLSession).requestedServerNames.map { name ->
                        (name as? SNIHostName)?.asciiName ?: name.toString()
                    }
                )
                val out = socket.outputStream
                out.write("200 welcome\r\n".toByteArray())
                out.flush()
                val reader = socket.inputStream.bufferedReader()
                while (true) {
                    val cmd = reader.readLine() ?: break
                    val reply = when {
                        cmd.startsWith("STAT") -> "223 0 <test@msg> article exists"
                        cmd == "QUIT" -> "205 bye"
                        else -> "500 unknown command"
                    }
                    out.write("$reply\r\n".toByteArray())
                    out.flush()
                    if (cmd == "QUIT") break
                }
            } catch (_: IOException) {
                // The client refused the certificate, or went away.
            }
        }
    }

    override fun close() {
        serverSocket.close()
    }
}

/**
 * A TLS NNTP server for the transport's tests, presenting the certificate in [keyStore] over [protocols]. Greets each
 * connection whose handshake completes (unless [greet] is false), then hands it to [serve], and closes it with TLS's
 * close_notify when [serve] returns. With [handshake] false it accepts connections and never answers them, the TLS
 * handshake included. TLS is layered over a plain socket, so a connection can also end without close_notify.
 */
internal class ScriptedTlsServer(
    keyStore: Path,
    private val protocols: Array<String> = arrayOf("TLSv1.2"),
    private val greet: Boolean = true,
    private val handshake: Boolean = true,
    private val serve: (TlsSession) -> Unit = {},
) : Closeable {

    /** One connection: what the client sent ([reader]) and the way back ([out]), over TLS. */
    class TlsSession(private val tls: SSLSocket, private val tcp: Socket) {
        val reader: BufferedReader = tls.inputStream.bufferedReader(Charsets.ISO_8859_1)
        val out: OutputStream = tls.outputStream

        /** Ends the connection as TLS means it to end: close_notify, then the TCP close. */
        fun closeWithNotify() = tls.close()

        /** Ends the TCP connection with no close_notify, as a server that crashed or a cut network does. */
        fun closeWithoutNotify() = tcp.close()

        /**
         * Over TLS 1.3, sends a KeyUpdate asking the client to update its keys too, as a JDK server does on reaching a
         * key's limit (jdk.tls.keyLimits); the server's own are new from here on.
         */
        fun updateKeys() = tls.startHandshake()
    }

    private val serverSocket = ServerSocket(0)
    private val factory: SSLSocketFactory

    /** Connections accepted, handshake completed or not. */
    val accepted get() = acceptedCount.get()
    private val acceptedCount = AtomicInteger()

    /** The protocol each completed handshake chose, in order: TLSv1.2 or TLSv1.3. */
    val negotiated = CopyOnWriteArrayList<String>()

    val port: Int get() = serverSocket.localPort

    init {
        val keys = KeyStore.getInstance("PKCS12").apply {
            Files.newInputStream(keyStore).use { load(it, PASSWORD.toCharArray()) }
        }
        val keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm())
            .apply { init(keys, PASSWORD.toCharArray()) }.keyManagers
        // The JDK's TLS, whatever provider a test puts first for the client (NntpTlsConscryptTransportTest's).
        factory = SSLContext.getInstance("TLS", "SunJSSE").apply { init(keyManagers, null, null) }.socketFactory
        Thread {
            while (true) {
                val tcp = try {
                    serverSocket.accept()
                } catch (_: IOException) {
                    break
                }
                acceptedCount.incrementAndGet()
                Thread { handle(tcp) }.apply { isDaemon = true }.start()
            }
        }.apply { isDaemon = true }.start()
    }

    private fun handle(tcp: Socket) {
        try {
            if (!handshake) {
                // Holds the connection open, reading what the client sends, until it goes away.
                tcp.use { it.getInputStream().readAllBytes() }
                return
            }
            val tls = (factory.createSocket(tcp, null, false) as SSLSocket).apply { enabledProtocols = protocols }
            tls.startHandshake()
            negotiated.add(tls.session.protocol)
            val session = TlsSession(tls, tcp)
            if (greet) {
                session.out.write("200 welcome\r\n".toByteArray())
                session.out.flush()
            }
            serve(session)
            tls.close()
        } catch (_: IOException) {
            // The client refused the certificate, or went away.
        } finally {
            runCatching { tcp.close() }
        }
    }

    override fun close() {
        serverSocket.close()
    }
}
