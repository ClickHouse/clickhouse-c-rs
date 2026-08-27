//! Upstream-drift guard.
//!
//! `src/sys.rs` is hand-written, so a vendored-header bump that adds a
//! function or an enumerator would otherwise land silently: the new function
//! is simply unreachable, and the new enumerator makes `Kind::from_raw` and
//! friends start returning `None` for a value the server legitimately sends.
//!
//! `build.rs` scans the covered headers and `src/sys.rs` into
//! `$OUT_DIR/parity.rs`; these tests compare the two. Accepting a new upstream
//! symbol means editing the lists below, which is the point — the decision is
//! recorded rather than defaulted.
//!
//! Struct geometry is guarded separately, by `tests/layout.rs`.

use crate::block::ColumnLayout;
use crate::client::PacketKind;
use crate::types::Kind;

mod generated {
    include!(concat!(env!("OUT_DIR"), "/parity.rs"));
}

use generated::{HEADER_ENUMERATORS, HEADER_FUNCTIONS, SYS_FUNCTIONS};

/// Header functions `sys` deliberately leaves undeclared, with the reason.
const UNDECLARED: &[(&str, &str)] = &[(
    "chc_err_reset",
    "static inline in clickhouse.h, so there is no external symbol to link; \
     Rust passes a fresh chc_err::zeroed() per call instead",
)];

/// `sys` functions with no header declaration: the crate's own C shims in
/// `src/wrapper.c`, which exist because Rust cannot allocate clickhouse-c's
/// incomplete struct types by value.
const CRATE_SHIMS: &[&str] = &[
    "chc_rs_in_destroy",
    "chc_rs_in_new",
    "chc_rs_in_new_ioless",
    "chc_rs_monotonic_us",
];

/// Enumerators that are not wire values, so no safe variant maps to them.
fn is_sentinel(name: &str) -> bool {
    name.ends_with("_COUNT") || name.ends_with("_LAST")
}

fn enumerators(tag: &str) -> impl Iterator<Item = (&'static str, i64)> {
    HEADER_ENUMERATORS
        .iter()
        .filter(move |(t, name, _)| *t == tag && !is_sentinel(name))
        .map(|(_, name, value)| (*name, *value))
}

#[test]
fn sys_declares_every_public_header_function() {
    let missing: Vec<_> = HEADER_FUNCTIONS
        .iter()
        .filter(|f| !SYS_FUNCTIONS.contains(f))
        .filter(|f| !UNDECLARED.iter().any(|(name, _)| name == *f))
        .collect();
    assert!(
        missing.is_empty(),
        "clickhouse-c publishes functions src/sys.rs does not declare: {missing:?}\n\
         Declare them, or add them to UNDECLARED with a reason."
    );
}

#[test]
fn sys_declares_nothing_upstream_dropped() {
    let stale: Vec<_> = SYS_FUNCTIONS
        .iter()
        .filter(|f| !HEADER_FUNCTIONS.contains(f))
        .filter(|f| !CRATE_SHIMS.contains(f))
        .collect();
    assert!(
        stale.is_empty(),
        "src/sys.rs declares functions the vendored headers no longer publish: {stale:?}"
    );
}

#[test]
fn undeclared_list_stays_current() {
    for (name, _) in UNDECLARED {
        assert!(
            HEADER_FUNCTIONS.contains(name),
            "UNDECLARED lists {name}, which upstream no longer publishes"
        );
    }
}

#[test]
fn every_type_kind_maps_to_a_variant() {
    for (name, value) in enumerators("chc_kind") {
        assert!(
            Kind::from_raw(value as _).is_some(),
            "clickhouse-c added {name} to chc_kind; Kind has no variant for it, \
             so parsed types of that kind report None"
        );
    }
}

#[test]
fn every_column_layout_maps_to_a_variant() {
    for (name, value) in enumerators("chc_col_kind") {
        assert!(
            ColumnLayout::from_raw(value as _).is_some(),
            "clickhouse-c added {name} to chc_col_kind; ColumnLayout has no variant for it"
        );
    }
}

/// Packet kinds that never arrive from the server, so `PacketKind` has no
/// variant for them.
const CLIENT_ONLY_PACKETS: &[&str] = &["CHC_PKT_HELLO"];

#[test]
fn every_server_packet_kind_maps_to_a_variant() {
    for (name, value) in enumerators("chc_packet_kind") {
        if CLIENT_ONLY_PACKETS.contains(&name) {
            continue;
        }
        assert!(
            PacketKind::from_raw(value as _).is_some(),
            "clickhouse-c added {name} to chc_packet_kind; the client rejects that \
             packet as an unknown-protocol error"
        );
    }
}

/// The safe enums are `#[repr]`-tagged with the generated constants, so a
/// variant can only exist for a value upstream still publishes. This catches
/// the other direction: a variant kept alive by a stale hand-written constant.
#[test]
fn header_enumerators_cover_the_generated_constants() {
    assert!(
        enumerators("chc_kind").count() > 0,
        "no chc_kind enumerators scanned; the header scanner is broken"
    );
    assert!(
        enumerators("chc_packet_kind").count() > 0,
        "no chc_packet_kind enumerators scanned; the header scanner is broken"
    );
}
