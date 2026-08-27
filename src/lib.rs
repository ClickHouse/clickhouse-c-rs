//! Rust bindings for [clickhouse-c], a header-only C client for the
//! ClickHouse Native wire format.
//!
//! [`PosixIo`] over a pipe or socket fd, plus [`Block`] /
//! [`BlockBuilder`], reads and writes Native blocks without going
//! through the TCP packet loop. Suitable for piping into
//! `clickhouse local --format Native` or for talking to any peer that
//! speaks raw block frames.
//!
//! [`sys`] holds the raw unsafe FFI surface the safe layer wraps.
//!
//! [clickhouse-c]: https://github.com/ClickHouse/clickhouse-c

// FFI wrappers mirror C arities one-to-one; arg-count refactors would
// only push parameters into ad-hoc structs without earning anything.
#![allow(clippy::too_many_arguments)]

pub mod sys;

mod alloc;
mod block;
mod builder;
mod error;
mod io;
mod types;

pub use alloc::Allocator;
pub use block::{Block, BlockOpts, BlockReader, Column, ColumnLayout, LowCardinalityView};
pub use builder::{BlockBuilder, ColumnBuilder};
pub use error::{Error, ErrorKind, Result};
pub use io::{ClientIo, PosixIo};
pub use types::{Kind, TypeAst, TypeRef};
