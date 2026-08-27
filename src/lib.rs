//! Rust bindings for [clickhouse-c], a header-only C client for the
//! ClickHouse Native wire format.
//!
//! # Three layers
//!
//! 1. [`sys`] — the unsafe FFI surface. Every public function, struct, and
//!    constant of the vendored headers, checked against them on every test
//!    run so an upstream bump cannot drift past unnoticed. Reach here for
//!    anything the safe layer does not wrap.
//! 2. Safe protocol and block primitives — [`TypeAst`], [`Block`],
//!    [`BlockReader`], [`BlockBuilder`], [`Client`], [`Codec`],
//!    [`Allocator`]. Ownership, lifetimes, and the C destroy calls, and
//!    nothing else: no pooling, no retries, no row mapping.
//! 3. Optional transport adapters — [`PosixIo`] always, [`AsyncClient`]
//!    under feature `tokio`, [`tls`] under feature `tls`. All three sit on
//!    the [`Io`] trait, which a consumer can implement for any transport.
//!
//! # Entry points
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
//! # Safety model
//!
//! Soundness of the safe API rests on clickhouse-c, at the bundled
//! [`UPSTREAM_REVISION`], holding the invariants its headers document.
//! Where a cross-check costs a line it is made, and where a length could be
//! read from two fields the smaller wins, so a C-side bug truncates rather
//! than reading out of bounds.
//!
//! Decoding bounds every slice by the owning column's own row count, so the
//! accessors are safe on any input. Agreement *between* columns is not
//! checked: see [`Block::validate`] before walking a peer's block by its
//! offsets or dictionary keys.
//!
//! Self-referential C structs ([`PosixIo`], [`Codec`], `tls::TlsIo`) hand
//! out `Pin<Box<Self>>`, because the C side stores pointers back into them.
//!
//! Every owning handle is `Send`. The builders are also `Sync`, since a
//! shared reference to one only exposes reads; the connection handles are
//! not, matching clickhouse-c's single-threaded client. [`Allocator`] is
//! `Copy + Send + Sync`, being a stateless vtable.
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
