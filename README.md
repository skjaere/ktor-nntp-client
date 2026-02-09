# Ktor NNTP Client

A Kotlin NNTP (Network News Transfer Protocol) client library built on Ktor's asynchronous socket API. Implements all RFC 3977 commands plus streaming yEnc body decoding via [rapidyenc-kotlin-wrapper](https://github.com/skjaere/rapidyenc-kotlin-wrapper).

## Features

- Full RFC 3977 NNTP command support
- Streaming yEnc body decoding with `ByteReadChannel`
- Lightweight yEnc header retrieval without downloading the full body
- SIMD-accelerated yEnc decoding via rapidyenc native library
- CRC32 validation for yEnc articles
- Automatic reconnection on partial body read cancellation
- Coroutine-based async I/O with backpressure
- Thread-safe for concurrent connections (one `NntpClient` per connection)
- TLS support

## Requirements

- Java 25+
- Kotlin 2.3+

## Installation

Add to your `build.gradle.kts`:

```kotlin
repositories {
    mavenCentral()
    mavenLocal()
}

dependencies {
    implementation("io.skjaere:ktor-nntp-client:0.1.0")
}
```

Publish locally first:
```bash
./gradlew publishToMavenLocal
```

## Usage

### Basic Connection

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
val group = client.group("alt.binaries.test")
println("Articles: ${group.count}, range: ${group.low}-${group.high}")
```

### Article Retrieval

```kotlin
// Full article (headers + body)
val article = client.article("<message-id@host>")
println(article.content.joinToString("\n"))

// Headers only
val head = client.head("<message-id@host>")

// Body only (text)
val body = client.body(12345L)

// Check if article exists
val stat = client.stat("<message-id@host>")
println("Article ${stat.articleNumber}: ${stat.messageId}")
```

### yEnc Body Decoding (Streaming)

```kotlin
val result = client.bodyYenc("<yenc-message-id@host>")

// Access yEnc headers
println("Filename: ${result.yencHeaders.name}")
println("Size: ${result.yencHeaders.size}")

// Stream decoded binary data
val decoded = result.data.toByteArray()

// Or process incrementally
val buffer = ByteArray(8192)
while (!result.data.isClosedForRead) {
    val read = result.data.readAvailable(buffer)
    if (read > 0) {
        // Process buffer[0..read]
    }
}
```

### yEnc Headers Only

Retrieve just the yEnc headers (filename, size, part info) from an article body without downloading or decoding the full content. The connection automatically reconnects in the background after the headers are read.

```kotlin
val headers = client.bodyYencHeaders("<yenc-message-id@host>")

println("Filename: ${headers.name}")
println("Size: ${headers.size}")
println("Line length: ${headers.line}")

// Multipart articles also include part info
if (headers.part != null) {
    println("Part ${headers.part} of ${headers.total}")
    println("Byte range: ${headers.partBegin}-${headers.partEnd}")
}

// The connection is still usable — subsequent commands work after the background reconnect
val nextHeaders = client.bodyYencHeaders("<another-message-id@host>")
```

### Concurrent Downloads

```kotlin
val selectorManager = SelectorManager(Dispatchers.IO)
val clients = (1..10).map {
    NntpClient.connect("news.example.com", 119, selectorManager)
}

coroutineScope {
    messageIds.zip(clients).map { (msgId, client) ->
        async {
            val result = client.bodyYenc(msgId)
            result.data.toByteArray()
        }
    }.awaitAll()
}
```

### Other Commands

```kotlin
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
| `bodyYenc(messageId/number)` | Retrieve and decode yEnc body |
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

- Each `NntpClient` instance is safe for sequential use from any coroutine
- Multiple `NntpClient` instances can operate concurrently (separate connections)
- A single `SelectorManager` can be shared across all connections
- If a `bodyYenc()` consumer cancels mid-read, or after `bodyYencHeaders()` returns, the connection automatically reconnects in the background

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
