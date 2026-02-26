# Ktor NNTP Client

[![CI](https://github.com/skjaere/ktor-nntp-client/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/skjaere/ktor-nntp-client/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/skjaere/ktor-nntp-client/branch/main/graph/badge.svg)](https://codecov.io/gh/skjaere/ktor-nntp-client)
[![](https://jitpack.io/v/skjaere/ktor-nntp-client.svg)](https://jitpack.io/#skjaere/ktor-nntp-client)

A Kotlin NNTP (Network News Transfer Protocol) client library built on Ktor's asynchronous socket API. Implements all RFC 3977 commands plus streaming yEnc body decoding via [rapidyenc-kotlin-wrapper](https://github.com/skjaere/rapidyenc-kotlin-wrapper).

## Features

- Full RFC 3977 NNTP command support
- Connection pool with automatic reconnection, keepalive, sleep/wake, and priority scheduling
- Idle connection keepalive with configurable interval
- Automatic sleep after configurable idle grace period, with transparent wake-on-demand
- Flow-based streaming yEnc body decoding
- Lightweight yEnc header retrieval without downloading the full body
- SIMD-accelerated yEnc decoding via rapidyenc native library
- CRC32 validation for yEnc articles
- Coroutine-based async I/O with backpressure
- TLS support
- Credential storage for automatic re-authentication on reconnect

## Requirements

- Java 25+
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

`NntpClientPool` manages a pool of connections with automatic reconnection. This is the recommended way to use the library.

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
pool.connect()

// All commands are delegated through the pool
val group = pool.group("alt.binaries.test")
println("Articles: ${group.count}, range: ${group.low}-${group.high}")

pool.close()
```

After operations that consume partial data (like `bodyYencHeaders`), the pool automatically reconnects the underlying connection and re-authenticates before reusing it. Callers don't need to manage connection state.

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
val stat = pool.stat("<message-id@host>")
println("Article ${stat.articleNumber}: ${stat.messageId}")
```

### yEnc Body Decoding (Streaming)

`bodyYenc()` returns a `Flow<YencEvent>` that emits a `Headers` event followed by a `Body` event containing a `ByteReadChannel` for streaming the decoded data:

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

### yEnc Headers Only

Retrieve just the yEnc headers (filename, size, part info) from an article body without downloading or decoding the full content. The connection automatically reconnects in the background after the headers are read.

```kotlin
val headers = pool.bodyYencHeaders("<yenc-message-id@host>")

println("Filename: ${headers.name}")
println("Size: ${headers.size}")
println("Line length: ${headers.line}")

// Multipart articles also include part info
if (headers.part != null) {
    println("Part ${headers.part} of ${headers.total}")
    println("Byte range: ${headers.partBegin}-${headers.partEnd}")
}

// Subsequent commands work immediately — the pool handles reconnection transparently
val nextHeaders = pool.bodyYencHeaders("<another-message-id@host>")
```

### Concurrent Downloads

With the connection pool, concurrent downloads are handled automatically:

```kotlin
val pool = NntpClientPool(
    host = "news.example.com",
    port = 119,
    selectorManager = selectorManager,
    maxConnections = 10,
    scope = scope
)
pool.connect()

coroutineScope {
    messageIds.map { msgId ->
        async {
            var decoded: ByteArray? = null
            pool.bodyYenc(msgId).collect { event ->
                if (event is YencEvent.Body) {
                    decoded = event.data.toByteArray()
                }
            }
            decoded!!
        }
    }.awaitAll()
}
```

### Keepalive and Sleep/Wake

The pool periodically sends `DATE` commands to keep connections alive. If no activity occurs within the idle grace period, the pool automatically closes all connections (sleeps). The next `withClient` call transparently wakes the pool by reconnecting.

You can also control sleep/wake explicitly:

```kotlin
// Manually sleep — closes all idle connections
pool.sleep()

// Manually wake — reconnects all connections
pool.wake()
```

Set `keepaliveIntervalMs = 0` to disable keepalive, and `idleGracePeriodMs = 0` to disable automatic sleep.

### Priority Scheduling

When all pool connections are in use, waiting callers are served by priority. Higher `Int` values mean higher priority. Within the same priority level, callers are served in FIFO order.

```kotlin
// Default priority (0) — backward compatible
val result = pool.stat("<message-id@host>")

// Higher priority — served before default-priority waiters
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
| `connect()` | Initialize pool connections |
| `withClient(priority = 0, block)` | Borrow a client with optional priority, execute block, return client to pool |
| `sleep()` | Close all idle connections and stop keepalive |
| `wake()` | Reconnect all connections and restart keepalive |
| `bodyYenc(messageId/number, priority = 0)` | Stream yEnc decoded body as `Flow<YencEvent>` |
| `bodyYencHeaders(messageId/number, priority = 0)` | Retrieve yEnc headers only |
| `group(name, priority = 0)` | Select newsgroup |
| `article(messageId/number, priority = 0)` | Retrieve full article |
| `head(messageId/number, priority = 0)` | Retrieve headers |
| `body(messageId/number, priority = 0)` | Retrieve body (text) |
| `stat(messageId/number, priority = 0)` | Check article exists |
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
| `bodyYencHeaders(messageId/number)` | Retrieve yEnc headers only |
| `stat(messageId/number)` | Check article exists |
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

### Thread Safety

- `NntpClientPool` is safe for concurrent use from multiple coroutines
- The pool uses `supervisorScope` to isolate failures and `NonCancellable` to ensure connections are always returned
- When all connections are busy, waiting coroutines are dispatched by priority (higher first, FIFO within same priority). Cancelled waiters are cleaned up without leaking connections.
- After `bodyYencHeaders()` or a cancelled `bodyYenc()`, the connection automatically reconnects in the background and re-authenticates if credentials were provided
- Each `NntpClient` instance is safe for sequential use from any coroutine
- A single `SelectorManager` can be shared across all connections

## Building

```bash
./gradlew build
./gradlew test
./gradlew publishToMavenLocal
```

## Dependencies

- Ktor Network 3.4.0
- rapidyenc-kotlin-wrapper 0.1.0
- Kotlin Coroutines 1.10.1
