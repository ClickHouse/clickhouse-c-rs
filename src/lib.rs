//! Rust bindings for [clickhouse-c].
//!
//! Crate reads and writes ClickHouse Native blocks and implements native TCP
//! protocol. API has three layers:
//!
//! 1. [`sys`] exposes unsafe C API.
//! 2. [`BlockReader`], [`BlockBuilder`], [`Client`], and related types provide
//!    safe block and protocol operations.
//! 3. [`PosixIo`], [`AsyncClient`], and [`tls`] provide transport adapters.
//!
//! # Entry points
//!
//! * Use [`BlockReader`] and [`BlockBuilder`] for Native block streams over
//!   any [`Io`] implementation.
//! * Use [`Client`] for blocking native TCP protocol.
//! * Use [`IolessClient`] to process protocol bytes with caller-managed I/O.
//! * Enable `tokio` feature and use [`AsyncClient`] for asynchronous TCP.
//! * Enable `tls` feature and use [`tls::TlsIo`] or
//!   [`AsyncClient::connect_tls`] for rustls connections.
//!
//! # Safety model
//!
//! Safe API depends on invariants documented by bundled clickhouse-c revision,
//! available as [`UPSTREAM_REVISION`]. Slice lengths are bounded by owning C
//! values.
//!
//! Decoding does not automatically validate relationships between nested
//! columns. Call [`Block::validate`] before using offsets or dictionary keys
//! from untrusted input as indexes.
//!
//! Types containing self-references, including [`PosixIo`], [`Codec`], and
//! `tls::TlsIo`, return `Pin<Box<Self>>`. Owning handles implement `Send`.
//! Connection handles do not implement `Sync`.
//!
//! [clickhouse-c]: https://github.com/ClickHouse/clickhouse-c

// Keep C function signatures visible in FFI wrappers
#![allow(clippy::too_many_arguments)]

/// Bundled [clickhouse-c] revision from `clickhouse-c/UPSTREAM`.
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
mod ioless;
#[cfg(test)]
mod parity;
mod query;
#[cfg(feature = "tls")]
pub mod tls;
mod types;

pub use alloc::Allocator;
#[cfg(feature = "tokio")]
pub use async_client::{AsyncClient, AsyncTransport, BoxedAsyncClient};
pub use block::{Block, BlockOpts, BlockReader, Column, ColumnLayout, LowCardinalityView};
pub use builder::{BlockBuilder, ColumnBuilder};
pub use client::{
    Client, ClientOpts, Event, Exception, PacketKind, ProfileInfo, Progress, ServerInfo,
};
pub use codec::{Codec, Compression, cityhash128};
pub use error::{Error, ErrorKind, Result};
pub use io::{CancelToken, Io, PosixIo};
pub use ioless::{IolessClient, Step};
pub use query::{QueryOpts, QueryParam, QuerySetting};
pub use types::{Kind, TypeAst, TypeRef};
