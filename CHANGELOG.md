# Changelog

Notable changes per release. The safe Rust API follows ordinary semver
expectations. `sys` is tied to the bundled clickhouse-c revision, which
does not change without the parity, layout, packaging, and integration
checks passing; a revision bump is called out here with the ClickHouse
versions CI exercised.

## Unreleased

First release. Bindings for clickhouse-c
`87d30a2ce3580cded1c794c7579fb8be8ea1c00b`, tested against ClickHouse
25.x on Linux and macOS.

- `sys` covers every public symbol of `clickhouse.h`,
  `clickhouse-posix-io.h`, `clickhouse-compression.h`,
  `clickhouse-client.h`, and `clickhouse-async.h`, checked against the
  headers on every test run. `clickhouse-openssl.h` ships unbound.
- Native block decode and encode over the `Io` trait, zero-copy on the
  write side.
- Blocking `Client`, with per-query settings and parameters.
- `IolessClient`, the packet loop with no I/O in it, for any runtime.
- `AsyncClient` (feature `tokio`), generic over the transport.
- rustls TLS (feature `tls`) for both clients.
- LZ4 and ZSTD (features `lz4`, `zstd`), plus caller-supplied codecs.
- Unix only. MSRV 1.85.
