# clickhouse-c-rs

`clickhouse-c-rs` provides Rust bindings for
[`clickhouse-c`](https://github.com/ClickHouse/clickhouse-c), a C library for
ClickHouse Native format and native TCP protocol.

Crate provides three levels of access:

- safe types for reading and writing Native blocks
- blocking and Tokio clients for ClickHouse native TCP protocol
- direct access to C API through `clickhouse_c::sys`

Blocking APIs support custom transports through `Io`. `PosixIo` supports Unix
file descriptors, including sockets and pipes. `IolessClient` separates protocol
processing from transport I/O for use with other runtimes. Optional TLS support
uses rustls.

## Installation

```toml
[dependencies]
clickhouse-c-rs = "0.1"
```

Default configuration enables LZ4 compression and requires system liblz4
development files. Disable default features for a build that only requires C
compiler and libc:

```toml
[dependencies]
clickhouse-c-rs = { version = "0.1", default-features = false }
```

Available features:

| Feature | Description | System requirement |
|---|---|---|
| `lz4` | LZ4 compression, enabled by default | liblz4 |
| `zstd` | Zstandard compression | libzstd |
| `tokio` | Asynchronous client using Tokio | none |
| `tls` | Blocking and asynchronous TLS using rustls | none |

Asynchronous TLS requires both `tokio` and `tls`.

## Documentation

API documentation is available on
[`docs.rs`](https://docs.rs/clickhouse-c-rs). Start with `BlockReader` and
`BlockBuilder` for Native block streams, `Client` for blocking TCP connections,
`AsyncClient` for Tokio connections, or `IolessClient` for transport-independent
protocol processing.

Repository includes `clickhouse-c` as a git submodule. Initialize it before
building from a source checkout:

```sh
git submodule update --init
```

Set `CHC_INCLUDE_DIR` to build against another `clickhouse-c` checkout.

Library supports Unix platforms. Minimum supported Rust version is 1.85.

## License

Apache-2.0. See `clickhouse-c/LICENSE` for upstream library license.
