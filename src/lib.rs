//! Rust bindings for [clickhouse-c], a header-only C client for the
//! ClickHouse Native wire format.
//!
//! [`sys`] holds the raw unsafe FFI surface; safe wrappers layer over it.
//!
//! [clickhouse-c]: https://github.com/ClickHouse/clickhouse-c

// FFI wrappers mirror C arities one-to-one; arg-count refactors would
// only push parameters into ad-hoc structs without earning anything.
#![allow(clippy::too_many_arguments)]

pub mod sys;
