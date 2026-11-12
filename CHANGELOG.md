# Changelog

## [0.8.0](https://github.com/skjaere/ktor-nntp-client/compare/v0.7.0...v0.8.0) (2026-11-12)


### Features

* **client:** read a yEnc article whole into one array ([288cc9e](https://github.com/skjaere/ktor-nntp-client/commit/288cc9e47cca7f4347ed40384dea968b14146a0e))
* **nntp:** tell a lost connection from one that can't be made ([fbf1a10](https://github.com/skjaere/ktor-nntp-client/commit/fbf1a100c328019138985dbac1b6ee77c4cdd3fc))
* **pool:** check a connection idle past validateAfterIdleMs before lending it ([adcf92e](https://github.com/skjaere/ktor-nntp-client/commit/adcf92e754564084abc1c34ef38b901bade8ba70))
* **pool:** count the connections the pool opens ([07739ba](https://github.com/skjaere/ktor-nntp-client/commit/07739ba4b98c7295a7e5705562b693eac0e300f2))
* **pool:** log the dead idle connections the checks find at INFO ([9ef17e2](https://github.com/skjaere/ktor-nntp-client/commit/9ef17e26542714f25b4b30dcd9157220cb1dfdca))
* **pool:** name a pool's metrics so pools on one server stay apart ([c3cdaab](https://github.com/skjaere/ktor-nntp-client/commit/c3cdaab62d8be0f2fe0e14ab1c4b133e719d3cfa))
* **stat:** pipelined STAT with per-answer progress ([e17b006](https://github.com/skjaere/ktor-nntp-client/commit/e17b0066ab1ed254fa3590804f6b0ce265ec06b3))


### Bug Fixes

* **build:** target JVM 17 for Android, keep tests on JDK 25 ([e199238](https://github.com/skjaere/ktor-nntp-client/commit/e199238a4b1d5cc6c211ca85574f50f9cd203162))
* **client:** never write an id that isn't a message-id to the wire ([8146f0b](https://github.com/skjaere/ktor-nntp-client/commit/8146f0b62cfc0f4ba84f2bb5e5f43a624475947d))
* **client:** OVER/HDR 430 is not-found; pool checks ids before acquiring ([68f3811](https://github.com/skjaere/ktor-nntp-client/commit/68f38111e658a24586adaffa96be808f97937df1))
* **connection:** drain or discard a command cut short by cancellation ([bd8b7d0](https://github.com/skjaere/ktor-nntp-client/commit/bd8b7d0e5110e6788e63d63f82a7737b3f25e5fc))
* **connection:** fail, not cancel, a command on a closed connection ([7576aa8](https://github.com/skjaere/ktor-nntp-client/commit/7576aa8baf15b25a4ead14c204f5a94628a53a19))
* **connection:** throw NntpConnectionException when the connect fails ([39b943b](https://github.com/skjaere/ktor-nntp-client/commit/39b943bedf5fb54667ac08ddcc0e0da18adaffdc))
* **connection:** time out reads that get no data (30 s by default) ([92084c1](https://github.com/skjaere/ktor-nntp-client/commit/92084c1b9b68a1f54059450520979b430c28db35))
* **nntp:** close a socket whose connect was cancelled after the hop connected it ([f6cbad5](https://github.com/skjaere/ktor-nntp-client/commit/f6cbad5132c08d05139d95cba92b61167f12159f))
* **nntp:** count a close right after a BODY's status line as a lost connection ([6851f65](https://github.com/skjaere/ktor-nntp-client/commit/6851f65ca3c469c023e288ec7b412e6afd873f4f))
* **nntp:** count a line cut off by the server's close as a lost connection ([6305ab8](https://github.com/skjaere/ktor-nntp-client/commit/6305ab805f9980c7e684d3706d631b708686578f))
* **nntp:** fail a command whose connection fails with NntpConnectionException ([ab3003d](https://github.com/skjaere/ktor-nntp-client/commit/ab3003deb8aea85169d76f3bcdd2c586f34f1d33))
* **nntp:** fail a connect whose greeting is cut off with NntpConnectionException ([ff3bea9](https://github.com/skjaere/ktor-nntp-client/commit/ff3bea9a00fda42b5665d66ed0230162708b3824))
* **nntp:** keep the bytes a cancelled hop to IO had read for the drain ([b4bccf1](https://github.com/skjaere/ktor-nntp-client/commit/b4bccf1fa7f3cd43ef2012441dd92fb67e900a2a))
* **nntp:** take out what closing TLS queued, so Conscrypt frees its state at close ([a2d87ce](https://github.com/skjaere/ktor-nntp-client/commit/a2d87ce6976950ef7eb9161cdb4dbd9a9d2b0fe5))
* **nntp:** wait for servers on the library's own view of Dispatchers.IO ([edd554e](https://github.com/skjaere/ktor-nntp-client/commit/edd554ec23668d9edbbf0bbb8dcd5e1942858381))
* **pool:** bound the cancellation-recovery drain so a silent server can't leak the slot ([9ba575c](https://github.com/skjaere/ktor-nntp-client/commit/9ba575c0719a95c8933d7ba3a124f4b2fcd59b40))
* **pool:** check every idle connection not in use at each keepalive tick ([9b4e596](https://github.com/skjaere/ktor-nntp-client/commit/9b4e59658eb54e1355cc66d1b7f4fe343bd0b9a5))
* **pool:** close the connections a cancelled keepalive or wake held ([9c40c5f](https://github.com/skjaere/ktor-nntp-client/commit/9c40c5f0724803476131ce56135c1247847de8d9))
* **pool:** discard a cancelled STAT batch that owes many answers ([481b66a](https://github.com/skjaere/ktor-nntp-client/commit/481b66adb48d39af763765c29df69114701a481a))
* **pool:** don't pool a connection a block left out of step ([8e98f97](https://github.com/skjaere/ktor-nntp-client/commit/8e98f9747128c7d8450613dc51ec1c70e6d1d540))
* **pool:** don't pool a connection whose yEnc body didn't finish ([22dff2d](https://github.com/skjaere/ktor-nntp-client/commit/22dff2df40fc7c4b67ec7ada1482eeae49bc9409))
* **pool:** don't retry a rejected login or an unknown host ([390aeff](https://github.com/skjaere/ktor-nntp-client/commit/390aeff35e421c4fc1d85afaa974d60d62d8ac23))
* **pool:** drain a cancelled command on the pool's scope, not the caller's ([3a26c7b](https://github.com/skjaere/ktor-nntp-client/commit/3a26c7b2226edcf7e95708c7bd1a4fccba95ef41))
* **pool:** fail acquires at once after close and stop its connects ([0a2d0d4](https://github.com/skjaere/ktor-nntp-client/commit/0a2d0d4789e1ab2322d7610bc32cb286f0926981))
* **pool:** fail waiting acquires at once when the pool can't connect ([8b490a8](https://github.com/skjaere/ktor-nntp-client/commit/8b490a889f28d8fcdb18b535e7dbc2f733ba6678))
* **pool:** give back the connections of acquires cancelled while the pool is busy ([3f57825](https://github.com/skjaere/ktor-nntp-client/commit/3f5782526ce8e739248ef3dc36dd8bd37cfc86bc))
* **pool:** give up a dead server's connect within 10 s, with its cause ([1df6163](https://github.com/skjaere/ktor-nntp-client/commit/1df616375b1bafc5c61f8e0cc02a6e1207f08f50))
* **pool:** keep a connection that completes as its connect times out ([d384568](https://github.com/skjaere/ktor-nntp-client/commit/d384568500c0c96e1750ce88b6dfde4aef854dc2))
* **pool:** keep idle connections in the order callers gave them back ([702813f](https://github.com/skjaere/ktor-nntp-client/commit/702813f002ee6b5b49b6174056e2584310a87e12))
* **pool:** keep the keepalive from holding connections behind silent ones ([2a9bb20](https://github.com/skjaere/ktor-nntp-client/commit/2a9bb2027cc23801bc797d2f18e5540942331d97))
* **pool:** keep the keepalive running when a wake races a sleep ([aa24ea5](https://github.com/skjaere/ktor-nntp-client/commit/aa24ea541566f3491dfb60f692159890beec2ed7))
* **pool:** make commandRetries = 0 mean no retry instead of failing ([b181f56](https://github.com/skjaere/ktor-nntp-client/commit/b181f56fd9d6deefc15b7ba6b200ae6d5d1325b5))
* **pool:** read off a cancelled STAT batch's answers without the caller ([9b05a21](https://github.com/skjaere/ktor-nntp-client/commit/9b05a2131d16ad941f4421bf5d25760fd348edc5))
* **pool:** send no login while a rejected account's back-off runs ([a70b223](https://github.com/skjaere/ktor-nntp-client/commit/a70b223541000958b3e35d5e62eb82cd39bc57a2))
* **pool:** take a connection whose DATE gets any answer but 111 for dead ([d391ded](https://github.com/skjaere/ktor-nntp-client/commit/d391dede2a253f39fca86d9a1975122a6f847800))
* **pool:** work in progress on the cancellation-recovery path ([ffe9212](https://github.com/skjaere/ktor-nntp-client/commit/ffe9212d630ed403e2372a6a28b8d9231f3f3d32))
* **tls:** check that the server's certificate names the host ([4da8323](https://github.com/skjaere/ktor-nntp-client/commit/4da8323134d2cb263a0e49daf042b3cf2d752468))
* **tls:** run TLS actors on Dispatchers.IO to avoid a nonce deadlock ([feb1810](https://github.com/skjaere/ktor-nntp-client/commit/feb18106225989c7e61a8fa8c2904e7d4eb1d2bd))
* **yenc:** compare an article's CRC with its =yend line's ([9ec027a](https://github.com/skjaere/ktor-nntp-client/commit/9ec027a625ce3b82e6b640c0a3dbcc39f61cfd37))
* **yenc:** fail a body cut short by the connection closing ([ed745d8](https://github.com/skjaere/ktor-nntp-client/commit/ed745d876cd82821a35ba26b0e3f27549cc1d41f))
* **yenc:** name the field of an unreadable number in a yEnc header (K55) ([c1aef6e](https://github.com/skjaere/ktor-nntp-client/commit/c1aef6ec6347132487d89d7be4462494b7028001))
* **yenc:** stream a body to its reader as it is decoded ([6a3a15e](https://github.com/skjaere/ktor-nntp-client/commit/6a3a15e6b53d9d5182982f6346ce10ef511bfdf6))


### Performance Improvements

* **nntp:** make the socket's line buffer only when a line is read from it ([be30768](https://github.com/skjaere/ktor-nntp-client/commit/be307682629f9fa1f3a9e2c986c18b51cb9a3290))
* **socket:** let a long answer's bytes gather before reading them ([0613336](https://github.com/skjaere/ktor-nntp-client/commit/061333647d20df93e2ec1ccc2d36803f81f150e1))
* **socket:** read a connection's socket in the reader's own thread ([60f5897](https://github.com/skjaere/ktor-nntp-client/commit/60f58976d4d54d522f3e79c14d83431b40e4d440))
* **tls:** use the platform's TLS in place of Ktor's tls() ([3092980](https://github.com/skjaere/ktor-nntp-client/commit/3092980b06fe9e53919cf1ba28af04a50fa8559f))
* **yenc:** decode a whole article in batches, not after every read ([aa8e610](https://github.com/skjaere/ktor-nntp-client/commit/aa8e610146f4ca9a68234c2a90fc558904abb40b))
* **yenc:** keep the decoder's buffers on the connection ([1a614aa](https://github.com/skjaere/ktor-nntp-client/commit/1a614aacab747addf7c632a79ae2309c33793f80))

## [0.7.0](https://github.com/skjaere/ktor-nntp-client/compare/v0.6.0...v0.7.0) (2026-05-26)


### Features

* **metrics:** pool connecting gauge, acquire-failure + connect-attempt counters

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
