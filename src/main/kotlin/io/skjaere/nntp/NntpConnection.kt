package io.skjaere.nntp

import io.ktor.network.selector.SelectorManager
import io.ktor.network.sockets.Socket
import io.ktor.network.sockets.aSocket
import io.ktor.network.sockets.openReadChannel
import io.ktor.network.sockets.openWriteChannel
import io.ktor.network.tls.tls
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.ByteWriteChannel
import io.ktor.utils.io.readByte
import io.ktor.utils.io.readLine
import io.ktor.utils.io.writeStringUtf8
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.io.Closeable

class NntpConnection private constructor(
    private val host: String,
    private val port: Int,
    private val selectorManager: SelectorManager,
    private val useTls: Boolean,
    private var socket: Socket,
    private var readChannel: ByteReadChannel,
    private var writeChannel: ByteWriteChannel,
    val welcomeResponse: NntpResponse,
    private val scope: CoroutineScope
) : Closeable {

    internal val commandMutex = Mutex()
    private var reconnectJob: Job? = null
    private var username: String? = null
    private var password: String? = null
    private val logger = LoggerFactory.getLogger(NntpConnection::class.java)

    companion object {
        private val log = LoggerFactory.getLogger(NntpConnection::class.java)
        suspend fun connect(
            host: String,
            port: Int,
            selectorManager: SelectorManager,
            useTls: Boolean = false,
            scope: CoroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
        ): NntpConnection {
            val socket = openSocket(host, port, selectorManager, useTls)
            val readChannel = socket.openReadChannel()
            val writeChannel = socket.openWriteChannel(autoFlush = true)

            val welcomeLine = readChannel.readLine()
                ?: throw NntpConnectionException("Connection closed before welcome message")
            val welcomeResponse = parseResponseLine(welcomeLine)

            if (welcomeResponse.code != 200 && welcomeResponse.code != 201) {
                socket.close()
                throw NntpProtocolException(
                    "Unexpected welcome code: ${welcomeResponse.code}",
                    welcomeResponse
                )
            }

            return NntpConnection(
                host, port, selectorManager, useTls,
                socket, readChannel, writeChannel, welcomeResponse, scope
            )
        }

        private suspend fun openSocket(
            host: String,
            port: Int,
            selectorManager: SelectorManager,
            useTls: Boolean
        ): Socket {
            log.info("openSocket called with host='{}', port={}, useTls={}", host, port, useTls)
            val rawSocket = aSocket(selectorManager).tcp().connect(host, port)
            return if (useTls) {
                rawSocket.tls(coroutineContext = Dispatchers.IO)
            } else {
                rawSocket
            }
        }
    }

    /**
     * Perform the AUTHINFO USER/PASS handshake on the current channels.
     * Must only be called when the caller already owns the command mutex
     * or during reconnect (where no other command can run concurrently).
     */
    private suspend fun performAuth(user: String, pass: String): NntpResponse {
        writeLine(NntpCommand.authinfoUser(user))
        val userResponse = readResponse()
        if (userResponse.code == 281) {
            username = user
            password = pass
            return userResponse
        }
        if (userResponse.code != 381) {
            throw NntpAuthenticationException(
                "AUTHINFO USER failed: ${userResponse.code} ${userResponse.message}",
                userResponse
            )
        }
        writeLine(NntpCommand.authinfoPass(pass))
        val passResponse = readResponse()
        if (passResponse.code != 281) {
            throw NntpAuthenticationException(
                "AUTHINFO PASS failed: ${passResponse.code} ${passResponse.message}",
                passResponse
            )
        }
        username = user
        password = pass
        return passResponse
    }

    internal suspend fun authenticate(username: String, password: String): NntpResponse =
        commandMutex.withLock {
            ensureConnected()
            performAuth(username, password)
        }

    internal suspend fun ensureConnected() {
        reconnectJob?.join()
    }

    internal fun scheduleReconnect() {
        logger.info("scheduleReconnect called, host='{}', port={}", host, port)
        try {
            socket.close()
        } catch (_: Exception) {
            // Ignore close errors on poisoned socket
        }
        reconnectJob = scope.launch {
            logger.info("scheduleReconnect launching openSocket with host='{}', port={}", host, port)
            val newSocket = openSocket(host, port, selectorManager, useTls)
            readChannel = newSocket.openReadChannel()
            writeChannel = newSocket.openWriteChannel(autoFlush = true)
            socket = newSocket
            // Read and discard welcome message
            readChannel.readLine()

            // Re-authenticate if credentials are stored
            val user = username
            val pass = password
            if (user != null && pass != null) {
                performAuth(user, pass)
            }

            reconnectJob = null
        }
    }

    suspend fun command(cmd: String): NntpResponse = commandMutex.withLock {
        ensureConnected()
        writeLine(cmd)
        readResponse()
    }

    suspend fun commandMultiLine(cmd: String): Pair<NntpResponse, List<String>> = commandMutex.withLock {
        ensureConnected()
        writeLine(cmd)
        val response = readResponse()
        if (response.code in 100..299) {
            val data = readMultiLineData()
            Pair(response, data)
        } else {
            Pair(response, emptyList())
        }
    }

    /**
     * Send a command and read the initial status line.
     * The caller is responsible for consuming any remaining data from [rawReadChannel].
     * The command mutex is NOT released - the caller must release it via [commandMutex].
     */
    suspend fun commandRaw(cmd: String): NntpResponse {
        commandMutex.lock()
        ensureConnected()
        writeLine(cmd)
        return readResponse()
    }

    suspend fun readResponse(): NntpResponse {
        val line = readChannel.readLine()
            ?: throw NntpConnectionException("Connection closed unexpectedly")
        return parseResponseLine(line)
    }

    suspend fun readMultiLineData(): List<String> {
        val lines = mutableListOf<String>()
        while (true) {
            val line = readChannel.readLine()
                ?: throw NntpConnectionException("Connection closed during multi-line read")
            if (line == ".") break
            // Dot-unstuffing: lines starting with ".." have leading dot removed
            if (line.startsWith("..")) {
                lines.add(line.substring(1))
            } else {
                lines.add(line)
            }
        }
        return lines
    }

    suspend fun readLine(): String {
        return readChannel.readLine()
            ?: throw NntpConnectionException("Connection closed unexpectedly")
    }

    /**
     * Read a line as raw bytes (ISO-8859-1), without UTF-8 interpretation.
     * Returns the line content without the trailing CRLF.
     * Use this for reading data that may contain non-ASCII bytes (e.g., yenc-encoded data).
     */
    suspend fun readRawLine(): ByteArray {
        val buffer = java.io.ByteArrayOutputStream()
        var prevByte: Int = -1
        while (true) {
            val byte = readChannel.readByte().toInt() and 0xFF
            if (prevByte == '\r'.code && byte == '\n'.code) {
                // Remove the trailing CR from buffer
                val result = buffer.toByteArray()
                return result.copyOf(result.size - 1)
            }
            buffer.write(byte)
            prevByte = byte
        }
    }

    val rawReadChannel: ByteReadChannel get() = readChannel

    suspend fun writeLine(line: String) {
        writeChannel.writeStringUtf8("$line\r\n")
    }

    override fun close() {
        reconnectJob?.cancel()
        try {
            socket.close()
        } catch (_: Exception) {
            // Ignore
        }
    }
}
