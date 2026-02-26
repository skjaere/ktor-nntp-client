# Changelog

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
