//! Rust bindings for [clickhouse-c], a header-only C client for the
//! ClickHouse Native wire format.
//!
//! Two entry points:
//!
//! * [`BlockReader`] / [`BlockBuilder`] over any [`Io`] backend: read or
//!   write Native blocks without going through the TCP packet loop.
//!   [`PosixIo`] covers a pipe or socket fd, which is enough to pipe
//!   `clickhouse local --format Native`; implement [`Io`] for anything
//!   else.
//! * [`Client`] over a connected TCP [`PosixIo`]: full Hello / Query /
//!   Data / EOS / Exception / Progress packet loop with optional LZ4 /
//!   ZSTD compression.
//! * With feature `tokio`, [`AsyncClient`] over `tokio::net::TcpStream`:
//!   same packet loop, driven by the caller's task without a worker
//!   thread.
//! * With feature `tls`, TLS over rustls: the blocking [`Client`] on a
//!   [`tls::TlsIo`] backend, and [`AsyncClient::connect_tls`].
//!
//! [`sys`] holds the raw unsafe FFI surface the safe layer wraps.
//!
//! [clickhouse-c]: https://github.com/ClickHouse/clickhouse-c

// FFI wrappers mirror C arities one-to-one; arg-count refactors would
// only push parameters into ad-hoc structs without earning anything.
#![allow(clippy::too_many_arguments)]

/// Revision of the bundled [clickhouse-c] the bindings were written
/// against, taken from `clickhouse-c/UPSTREAM` at build time.
///
/// [clickhouse-c]: https://github.com/ClickHouse/clickhouse-c
pub const UPSTREAM_REVISION: &str = env!("CHC_UPSTREAM_REVISION");

pub mod sys;

mod alloc;
#[cfg(feature = "tokio")]
mod async_client;
mod block;
mod builder;
mod client;
mod codec;
mod error;
mod io;
#[cfg(test)]
mod parity;
mod query;
#[cfg(feature = "tls")]
pub mod tls;
mod types;

pub use alloc::Allocator;
#[cfg(feature = "tokio")]
pub use async_client::AsyncClient;
pub use block::{Block, BlockOpts, BlockReader, Column, ColumnLayout, LowCardinalityView};
pub use builder::{BlockBuilder, ColumnBuilder};
pub use client::{
    Client, ClientOpts, Event, Exception, PacketKind, ProfileInfo, Progress, ServerInfo,
};
pub use codec::{Codec, Compression, cityhash128};
pub use error::{Error, ErrorKind, Result};
pub use io::{CancelToken, Io, PosixIo};
pub use query::{QueryOpts, QueryParam, QuerySetting};
pub use types::{Kind, TypeAst, TypeRef};
