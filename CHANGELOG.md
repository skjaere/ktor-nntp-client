# Changelog

## [0.4.0](https://github.com/skjaere/ktor-nntp-client/compare/v0.3.1...v0.4.0) (2026-02-26)


### Features

* supporting starting the pool is a sleeping state ([0145961](https://github.com/skjaere/ktor-nntp-client/commit/01459615c174ca6dbfc89a2eccfd134e25a00d04))


### Bug Fixes

* fix:  ([4b561ae](https://github.com/skjaere/ktor-nntp-client/commit/4b561ae3fe683eb4ca37efc148cb0391dc128ab0))
* adding metrics ([73d590a](https://github.com/skjaere/ktor-nntp-client/commit/73d590a529f681579d91cd942609cdca4690ec67))
* fixing race condition in sleep/awake ([79dcf22](https://github.com/skjaere/ktor-nntp-client/commit/79dcf220c75dda9394ca7a9ca2b6be65122ec47a))
* initializing connections concurrently ([ed19274](https://github.com/skjaere/ktor-nntp-client/commit/ed19274a43c0f88a4f59238552c667d4318c54e4))
* removing  redundant startSleeping parameter ([a3e951c](https://github.com/skjaere/ktor-nntp-client/commit/a3e951c4a4052ce43a333c4b0aa04bc4bb8f0243))
* removing duplicate logger and reducing noise in logs ([b52ca2b](https://github.com/skjaere/ktor-nntp-client/commit/b52ca2b81b278187da284e0a14f383717710d876))

## [0.3.1](https://github.com/skjaere/ktor-nntp-client/compare/v0.3.0...v0.3.1) (2026-02-26)


### Bug Fixes

* supporting priority for delegated methods too ([4a976d4](https://github.com/skjaere/ktor-nntp-client/commit/4a976d4dbc200da73d71056a568e96b78c7eece0))

## [0.3.0](https://github.com/skjaere/ktor-nntp-client/compare/v0.2.0...v0.3.0) (2026-02-26)


### Features

* supporting pool lease priority ([de79265](https://github.com/skjaere/ktor-nntp-client/commit/de7926561b68adc3ef097e01e71c94c9bd2470f1))

## [0.2.0](https://github.com/skjaere/ktor-nntp-client/compare/v0.1.2...v0.2.0) (2026-02-25)


### Features

* feat:  ([a24e299](https://github.com/skjaere/ktor-nntp-client/commit/a24e2997c6ff8a3d619c309f9561528c7c60077d))


### Bug Fixes

* increasing buffer size for YencDecoder ([a18bf6f](https://github.com/skjaere/ktor-nntp-client/commit/a18bf6f5fd9f4c925a5e5a7b3e75bb2899011223))
* inline release-please workflow instead of referencing reusable workflow ([22e14c0](https://github.com/skjaere/ktor-nntp-client/commit/22e14c0f216bf4a28182e1634a1bb981b7dfe6f6))
* making re-connecting thread-safe ([fce93c9](https://github.com/skjaere/ktor-nntp-client/commit/fce93c925e89760cdca85b04885281675e41a863))
* removing duplicated authentication code ([a56469a](https://github.com/skjaere/ktor-nntp-client/commit/a56469a76bd7332864fe52998bebd9ffef60ecce))
* returning sealed interface from stat commands ([bf596d9](https://github.com/skjaere/ktor-nntp-client/commit/bf596d94493f7dd5f3cbec039b19800e311186b4))
* throwing ArticleNotFoundException on 430 responses ([560a91b](https://github.com/skjaere/ktor-nntp-client/commit/560a91baae24301d8b24b968aa87dda2ead48b91))

## [0.1.2](https://github.com/skjaere/ktor-nntp-client/compare/v0.1.1...v0.1.2) (2026-02-11)


### Bug Fixes

* adding error handling ([6ca11ee](https://github.com/skjaere/ktor-nntp-client/commit/6ca11ee23305db9efd45223777d905bae461790b))
* refactoring ([3dd7059](https://github.com/skjaere/ktor-nntp-client/commit/3dd705991b97ee33deff12bc9b1c8103206e7478))

## [0.1.1](https://github.com/skjaere/ktor-nntp-client/compare/v0.1.0...v0.1.1) (2026-02-10)


### Bug Fixes

* cleaning up dirty connections, and using a connection pool for the public API ([079853b](https://github.com/skjaere/ktor-nntp-client/commit/079853b16b04ce52f0f6ad8363dea05ccf6a2e1e))
* replacing usage of try/finally with .use{} ([34e96cc](https://github.com/skjaere/ktor-nntp-client/commit/34e96cce551f477623f7a148b5650736e2d09260))

## [0.1.0](https://github.com/skjaere/ktor-nntp-client/compare/v0.0.1...v0.1.0) (2026-02-10)


### Features

* first release ([f4f80b4](https://github.com/skjaere/ktor-nntp-client/commit/f4f80b4fd991f9da24d9d91fa163ade33204c59b))
* fixing release version ([a3bf0d5](https://github.com/skjaere/ktor-nntp-client/commit/a3bf0d5e2fa8deade2b121a3eb21404b39e64466))


### Bug Fixes

* fixing CI ([9ff6676](https://github.com/skjaere/ktor-nntp-client/commit/9ff6676a9713a5cca4d46835e30a4d4f99bd6d50))
* fixing release version ([a504650](https://github.com/skjaere/ktor-nntp-client/commit/a5046506034dc1b35dd050795ed6b5be355627d6))
* prepare first release ([cc9e796](https://github.com/skjaere/ktor-nntp-client/commit/cc9e7968f31077b49939a3e733654d22063f7e79))
