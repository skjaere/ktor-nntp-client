# Changelog

## [0.6.0](https://github.com/skjaere/ktor-nntp-client/compare/v0.5.1...v0.6.0) (2026-05-20)


### Features

* **metrics:** record TTFB on NNTP commands tagged by command verb
* **metrics:** rename command timer to ack_latency, add body.duration


### Bug Fixes

* **connection:** don't mis-classify yenc body as a 430 single-line response


### Performance Improvements

* **yenc:** use direct ByteBuffer overloads to avoid JNA Memory churn

## [0.5.1](https://github.com/skjaere/ktor-nntp-client/compare/v0.5.0...v0.5.1) (2026-04-25)


### Bug Fixes

* **deps:** bump rapidyenc-kotlin-wrapper to v0.2.0

## [0.5.0](https://github.com/skjaere/ktor-nntp-client/compare/v0.4.2...v0.5.0) (2026-04-23)


### Features

* configurable command retries with Flow.retry() in withClient
* lazy pool initialization with on-demand connections and automatic shrinking
* **pool:** LIFO idle reuse and bounded keepalive DATE
* **pool:** per-connection idle eviction


### Bug Fixes

* connection pool currentSize leak when connection retries are exhausted
* **connection:** retry reconnect on transient network errors, not just auth
* handle ClosedWriteChannelException from TLS layer in connection pool
* **nntp:** dot-stuff article body and hold mutex across post/ihave
* **pool:** stop leaking slots when withClient returns during sleep
* release commandRaw mutex inside commandRaw on failure

## [0.4.2](https://github.com/skjaere/ktor-nntp-client/compare/v0.4.1...v0.4.2) (2026-03-18)


### Bug Fixes

* gracefully cancel TLS coroutines on reconnect and close

## [0.4.1](https://github.com/skjaere/ktor-nntp-client/compare/v0.4.0...v0.4.1) (2026-03-13)


### Bug Fixes

* retry connections with backoff and add acquire timeout

## [0.4.0](https://github.com/skjaere/ktor-nntp-client/compare/v0.3.1...v0.4.0) (2026-02-26)


### Features

* supporting starting the pool is a sleeping state


### Bug Fixes

* fix: 
* adding metrics
* fixing race condition in sleep/awake
* initializing connections concurrently
* removing  redundant startSleeping parameter
* removing duplicate logger and reducing noise in logs

## [0.3.1](https://github.com/skjaere/ktor-nntp-client/compare/v0.3.0...v0.3.1) (2026-02-26)


### Bug Fixes

* supporting priority for delegated methods too

## [0.3.0](https://github.com/skjaere/ktor-nntp-client/compare/v0.2.0...v0.3.0) (2026-02-26)


### Features

* supporting pool lease priority

## [0.2.0](https://github.com/skjaere/ktor-nntp-client/compare/v0.1.2...v0.2.0) (2026-02-25)


### Features

* feat: 


### Bug Fixes

* increasing buffer size for YencDecoder
* inline release-please workflow instead of referencing reusable workflow
* making re-connecting thread-safe
* removing duplicated authentication code
* returning sealed interface from stat commands
* throwing ArticleNotFoundException on 430 responses

## [0.1.2](https://github.com/skjaere/ktor-nntp-client/compare/v0.1.1...v0.1.2) (2026-02-11)


### Bug Fixes

* adding error handling
* refactoring

## [0.1.1](https://github.com/skjaere/ktor-nntp-client/compare/v0.1.0...v0.1.1) (2026-02-10)


### Bug Fixes

* cleaning up dirty connections, and using a connection pool for the public API
* replacing usage of try/finally with .use{}

## [0.1.0](https://github.com/skjaere/ktor-nntp-client/compare/v0.0.1...v0.1.0) (2026-02-10)


### Features

* first release
* fixing release version


### Bug Fixes

* fixing CI
* fixing release version
* prepare first release
