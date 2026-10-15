# Ktor NNTP Client

[![CI](https://github.com/skjaere/ktor-nntp-client/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/skjaere/ktor-nntp-client/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/skjaere/ktor-nntp-client/branch/main/graph/badge.svg)](https://codecov.io/gh/skjaere/ktor-nntp-client)
[![](https://jitpack.io/v/skjaere/ktor-nntp-client.svg)](https://jitpack.io/#skjaere/ktor-nntp-client)

A Kotlin NNTP (Network News Transfer Protocol) client library built on Ktor's asynchronous socket API. Implements all RFC 3977 commands plus streaming yEnc body decoding via [rapidyenc-kotlin-wrapper](https://github.com/skjaere/rapidyenc-kotlin-wrapper).

## Features

- Full RFC 3977 NNTP command support
- Connection pool with lazy initialization, automatic reconnection, keepalive, sleep/wake, and priority scheduling
- Lazy connection creation -- connections are opened on demand up to `maxConnections`, not all at once
- Automatic pool shrinking -- excess idle connections are closed when demand drops
- Idle connection keepalive with configurable interval
- Automatic sleep after configurable idle grace period, with transparent wake-on-demand
- Dead connection detection -- the keepalive checks idle connections with `DATE`, and a read that gets no data for `readTimeoutMs` fails, so dead connections are discarded
- I/O error recovery -- TLS/socket exceptions trigger automatic reconnect and retry
- Flow-based streaming yEnc body decoding
- Pipelined `STAT` for checking many articles in few round trips
- SIMD-accelerated yEnc decoding via rapidyenc native library
- CRC32 validation for yEnc articles
- Coroutine-based async I/O with backpressure
- TLS support, with SNI and certificate hostname verification
- Credential storage for automatic re-authentication on reconnect

## Requirements

- Java 17+ (the tests run on JDK 25)
- Kotlin 2.3+

## Installation

Add to your `build.gradle.kts`:

```kotlin
repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    implementation("com.github.skjaere:ktor-nntp-client:v0.1.0")
}
```

## Usage

### Connection Pool (Recommended)

`NntpClientPool` manages a pool of connections with lazy initialization, automatic reconnection, and retry. This is the recommended way to use the library.

```kotlin
val selectorManager = SelectorManager(Dispatchers.IO)
val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

val pool = NntpClientPool(
    host = "news.example.com",
    port = 119,
    selectorManager = selectorManager,
    username = "user",
    password = "pass",
    maxConnections = 5,
    scope = scope,
    keepaliveIntervalMs = 60_000,   // send DATE every 60s to keep connections alive (default)
    idleGracePeriodMs = 300_000     // sleep after 5 minutes of inactivity (default)
)

// No connect() call needed -- connections are created lazily on first use
val group = pool.group("alt.binaries.test")
println("Articles: ${group.count}, range: ${group.low}-${group.high}")

pool.close()
```

Connections are created on demand as requests come in, up to `maxConnections`. When demand drops, excess idle connections are automatically closed over time. If a connection fails (server close, TLS error), the pool transparently reconnects and retries with a different connection (once by default; `commandRetries`, where `0` means no retry).

### Direct Connection

For simple use cases, you can create a single `NntpClient` directly:

```kotlin
val selectorManager = SelectorManager(Dispatchers.IO)
val client = NntpClient.connect("news.example.com", 119, selectorManager)

// With authentication
val client = NntpClient.connect(
    "news.example.com", 119, selectorManager,
    username = "user", password = "pass"
)

// With TLS
val client = NntpClient.connect("news.example.com", 563, selectorManager, useTls = true)
```

With TLS the server's certificate must be trusted and name the host you connect to (a host name, or an IP address), and the host name is sent as SNI. Pass `verifyHostname = false` (to `NntpClient.connect` or `NntpClientPool`) for a server reached by an address its certificate doesn't list: the certificate then only has to be trusted.

### Group Selection

```kotlin
val group = pool.group("alt.binaries.test")
println("Articles: ${group.count}, range: ${group.low}-${group.high}")
```

### Article Retrieval

```kotlin
// Full article (headers + body)
val article = pool.article("<message-id@host>")
println(article.content.joinToString("\n"))

// Headers only
val head = pool.head("<message-id@host>")

// Body only (text)
val body = pool.body(12345L)

// Check if article exists
when (val stat = pool.stat("<message-id@host>")) {
    is StatResult.Found -> println("Article ${stat.articleNumber}: ${stat.messageId}")
    is StatResult.NotFound -> println("Missing: ${stat.code}")
}

// Check many articles, pipelined on one connection; onResult sees each answer as it arrives
val results = pool.statPipelined(listOf("<a@host>", "<b@host>")) { index, result -> println("$index: $result") }
```

A lookup by an id that can't be a message-id (`<...>` with no whitespace or control characters) is never sent: it gets the command's not-found outcome (`StatResult.NotFound`, or `ArticleNotFoundException`), and the pool doesn't take a connection for it.

### yEnc Body Decoding (Streaming)

`bodyYenc()` returns a `Flow<YencEvent>` that emits a `Headers` event followed by a `Body` event containing a `ByteReadChannel` for streaming the decoded data. The channel gets the article as it is decoded:

```kotlin
pool.bodyYenc("<yenc-message-id@host>").collect { event ->
    when (event) {
        is YencEvent.Headers -> {
            println("Filename: ${event.yencHeaders.name}")
            println("Size: ${event.yencHeaders.size}")
        }
        is YencEvent.Body -> {
            // Stream decoded binary data
            val decoded = event.data.toByteArray()

            // Or process incrementally
            val buffer = ByteArray(8192)
            while (!event.data.isClosedForRead) {
                val read = event.data.readAvailable(buffer)
                if (read > 0) {
                    // Process buffer[0..read]
                }
            }
        }
    }
}
```

A connection that drops mid-article fails the read of the `Body` channel, which happens outside the pool's `withClient`, so the pool doesn't fetch the article again: the reader may already have part of it. To have it fetched again on another connection, read the body whole inside `withClient`:

```kotlin
val decoded: ByteArray? = pool.withClient { client ->
    var data: ByteArray? = null
    client.bodyYenc("<yenc-message-id@host>").collect { event ->
        if (event is YencEvent.Body) data = event.data.toByteArray()
    }
    data
}
```

### Concurrent Downloads

With the connection pool, concurrent downloads are handled automatically. Connections scale up to `maxConnections` as concurrent requests arrive:

```kotlin
val pool = NntpClientPool(
    host = "news.example.com",
    port = 119,
    selectorManager = selectorManager,
    maxConnections = 10,
    scope = scope
)

coroutineScope {
    messageIds.map { msgId ->
        async {
            pool.withClient { client ->
                var decoded: ByteArray? = null
                client.bodyYenc(msgId).collect { event ->
                    if (event is YencEvent.Body) {
                        decoded = event.data.toByteArray()
                    }
                }
                decoded!!
            }
        }
    }.awaitAll()
}
```

### Keepalive, Sleep/Wake, and Pool Shrinking

The pool periodically sends `DATE` commands to keep connections alive. If no activity occurs within the idle grace period, the pool automatically closes all connections (sleeps). The next `withClient` call transparently wakes the pool.

When demand drops, the pool shrinks: on each keepalive tick it closes the connections that have been idle for longer than `idleEvictionTimeoutMs` (3 minutes by default), keeping at least 1. New connections are created again on demand when traffic increases.

You can also control sleep/wake explicitly:

```kotlin
// Manually sleep -- closes all idle connections
pool.sleep()

// Manually wake -- re-enables the pool (connections created on demand)
pool.wake()
```

Set `keepaliveIntervalMs = 0` to disable keepalive, and `idleGracePeriodMs = 0` to disable automatic sleep. Automatic sleep and idle eviction run on the keepalive's ticks, so without keepalive the pool neither sleeps nor shrinks by itself.

### Read Timeout

A read that gets no bytes from the server for `readTimeoutMs` (30 s by default) fails with `NntpConnectionException`, and the pool discards the connection, so a stalled server or a connection left half-open by a network change can't hang a command. It is an idle timeout: a long `BODY` or pipelined `STAT` batch that keeps receiving bytes is never cut off, and idle pooled connections are not affected. Pass `readTimeoutMs` to `NntpClientPool` or `NntpClient.connect`; `0` disables it.

### Priority Scheduling

When all pool connections are in use, waiting callers are served by priority. Higher `Int` values mean higher priority. Within the same priority level, callers are served in FIFO order.

```kotlin
// Default priority (0) -- backward compatible
val result = pool.stat("<message-id@host>")

// Higher priority -- served before default-priority waiters
val urgent = pool.article("<important@host>", priority = 10)

// Also works with withClient directly
pool.withClient(priority = 5) { client ->
    client.group("alt.binaries.test")
    client.article(12345L)
}
```

### Other Commands

```kotlin
pool.close()
// Or with a direct client:
client.capabilities()
client.modeReader()
client.help()
client.date()
client.listGroup("alt.binaries.test", range = 1L..100L)
client.over(1L..100L)
client.quit()
client.close()
```

## API Reference

### NntpClientPool

| Method | Description |
|--------|-------------|
| `withClient(priority = 0, block)` | Borrow a client with optional priority, execute block, return client to pool |
| `sleep()` | Close all idle connections and stop keepalive |
| `wake()` | Re-enable the pool (connections created on demand) |
| `bodyYenc(messageId/number, priority = 0)` | Stream yEnc decoded body as `Flow<YencEvent>` |
| `group(name, priority = 0)` | Select newsgroup |
| `article(messageId/number, priority = 0)` | Retrieve full article |
| `head(messageId/number, priority = 0)` | Retrieve headers |
| `body(messageId/number, priority = 0)` | Retrieve body (text) |
| `stat(messageId/number, priority = 0)` | Check article exists |
| `statPipelined(messageIds, priority = 0, window = 64, onResult)` | Check many articles, pipelined on one connection |
| `connectionsOpened` | Connections opened since the pool was created |
| `close()` | Close all connections and cancel waiters |

### NntpClient

| Method | Description |
|--------|-------------|
| `connect(host, port, selectorManager)` | Create connection |
| `authenticate(username, password)` | AUTHINFO USER/PASS |
| `capabilities()` | List server capabilities |
| `modeReader()` | Switch to reader mode |
| `group(name)` | Select newsgroup |
| `listGroup(name?, range?)` | List article numbers |
| `article(messageId/number)` | Retrieve full article |
| `head(messageId/number)` | Retrieve headers |
| `body(messageId/number)` | Retrieve body (text) |
| `bodyYenc(messageId/number)` | Stream yEnc decoded body as `Flow<YencEvent>` |
| `stat(messageId/number)` | Check article exists |
| `statPipelined(messageIds, window = 64, onResult)` | Check many articles, pipelined |
| `next()` / `last()` | Navigate articles |
| `over(range/messageId)` | Overview data |
| `xover(range)` | Extended overview |
| `hdr(field, range/messageId)` | Header data |
| `xhdr(field, range)` | Extended header data |
| `list(keyword?, args)` | List newsgroups |
| `newGroups(date, time)` | New groups since date |
| `newNews(wildmat, date, time)` | New articles since date |
| `post(article)` | Post article |
| `ihave(messageId, article)` | Transfer article |
| `help()` | Server help |
| `date()` | Server date |
| `quit()` | Close session |

### Metrics

The pool exposes the following Micrometer meters (tagged with `pool.name`):

| Metric | Description |
|--------|-------------|
| `nntp.pool.idle` | Number of idle connections |
| `nntp.pool.active` | Number of connections currently in use (tagged by priority) |
| `nntp.pool.size` | Total connections (idle + active + connecting) |
| `nntp.pool.connecting` | Connections being opened (connect, TLS, login or retry back-off) |
| `nntp.pool.waiters` | Number of callers waiting for a connection (tagged by priority) |
| `nntp.pool.sleeping` | 1 if pool is sleeping, 0 otherwise |
| `nntp.pool.acquire` | Timer for connection acquire latency (tagged by priority) |
| `nntp.pool.acquire.failures` | Acquires that failed (tagged by priority and `reason`: `timeout`, `connect_failed`) |
| `nntp.pool.connect.attempts` | Connects that succeeded or failed for good (tagged by `outcome`) |
| `nntp.body.duration` | Timer for a whole yEnc `BODY`, command to article terminator |

Connections also record `nntp.command.ack_latency`, the time from sending a command to its status line, tagged by `command` (not by pool).

### Thread Safety

- `NntpClientPool` is safe for concurrent use from multiple coroutines
- The pool uses `supervisorScope` to isolate failures and `NonCancellable` to ensure connections are always returned
- When all connections are busy, waiting coroutines are dispatched by priority (higher first, FIFO within same priority). Cancelled waiters are cleaned up without leaking connections.
- A command cancelled before its answer was read (e.g. a cancelled `bodyYenc()` or `stat()`) has the rest of the answer read off before its connection is reused, for up to 3 s; a cancelled `statPipelined()` returns at once and its answers are read off on the pool's scope. A connection that can't be brought back in step that way (a multi-line answer cut short, answers that don't come within the 3 s) is closed, and a new one is opened on demand
- I/O errors (e.g., `ClosedWriteChannelException` from TLS) trigger automatic reconnect and retry with a different connection
- Each `NntpClient` instance is safe for sequential use from any coroutine
- A single `SelectorManager` can be shared across all connections

## Building

```bash
./gradlew build
./gradlew test
./gradlew publishToMavenLocal
```

## Dependencies

- Ktor Network 3.4.1
- rapidyenc-kotlin-wrapper 0.3.0
- Kotlin Coroutines 1.10.1
- Micrometer 1.14.4
