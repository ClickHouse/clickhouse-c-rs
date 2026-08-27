//! Malformed wire input must produce errors, never a panic or worse.
//!
//! A real fuzzer belongs here eventually; this is the deterministic,
//! always-on version of the same idea. It takes a valid Native block, mutates
//! it every way a corrupt or hostile peer might, and asserts the decoder
//! declines rather than misbehaving. Run under a sanitizer it also covers the
//! C boundary:
//!
//! ```sh
//! CFLAGS="-fsanitize=address -fno-omit-frame-pointer -g" \
//! RUSTFLAGS="-Zsanitizer=address" \
//! ASAN_OPTIONS=detect_leaks=0:allocator_may_return_null=1 \
//! cargo +nightly test --target x86_64-unknown-linux-gnu --test malformed
//! ```
//!
//! `allocator_may_return_null` matters: a corrupt row count can ask
//! clickhouse-c for a hundreds-of-gigabytes null map, which is within the
//! `CHC_MAX_NUM_ROWS` ceiling. `malloc` refuses and the library reports
//! `ErrorKind::Oom`, but ASan aborts the process instead unless told to
//! behave like `malloc`.

use core::ffi::{c_int, c_void};
use core::marker::PhantomPinned;
use core::pin::Pin;

use clickhouse_c::{
    Allocator, BlockBuilder, BlockOpts, BlockReader, ClientOpts, ColumnBuilder, Io, IolessClient,
    Step, TypeAst, sys,
};

/// Read-only backend over a fixed byte slice: reads walk it, then report EOF.
struct Bytes {
    io: sys::chc_io,
    data: Vec<u8>,
    at: usize,
    _pin: PhantomPinned,
}

impl Bytes {
    fn new(data: Vec<u8>) -> Pin<Box<Self>> {
        let mut boxed = Box::pin(Self {
            io: sys::chc_io {
                ud: core::ptr::null_mut(),
                read: Some(read),
                write: None,
                check_cancel: None,
            },
            data,
            at: 0,
            _pin: PhantomPinned,
        });
        // SAFETY: sets a field at the pinned address; never moves out.
        unsafe {
            let this = boxed.as_mut().get_unchecked_mut();
            this.io.ud = (this as *mut Self).cast();
        }
        boxed
    }
}

// SAFETY: `io` is fully wired, `ud` back-points at the same pinned node, and
// `read` honours the vtable contract.
unsafe impl Io for Bytes {
    fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io {
        // SAFETY: field pointer; does not move self.
        unsafe { &mut self.get_unchecked_mut().io as *mut sys::chc_io }
    }
}

unsafe extern "C" fn read(
    ud: *mut c_void,
    buf: *mut c_void,
    len: usize,
    out_n: *mut usize,
    _err: *mut sys::chc_err,
) -> c_int {
    let io = unsafe { &mut *(ud as *mut Bytes) };
    let n = len.min(io.data.len() - io.at);
    if n > 0 {
        unsafe { core::ptr::copy_nonoverlapping(io.data[io.at..].as_ptr(), buf.cast::<u8>(), n) };
        io.at += n;
    }
    unsafe { *out_n = n };
    sys::CHC_OK
}

/// A valid block: two columns, a composite and a dictionary, so a mutated
/// byte can land in a type name, an offset array, a key, or a data slab.
fn valid_block() -> Vec<u8> {
    let alloc = Allocator::stdlib();
    let array_ty = TypeAst::parse("Array(Nullable(UInt32))", alloc).expect("array type");
    let lc_ty = TypeAst::parse("LowCardinality(String)", alloc).expect("lc type");

    let values: Vec<u8> = [10u32, 0, 30, 40]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    let null_map = [0u8, 1, 0, 0];
    let array_offsets = [2u64, 2, 4];
    let dict_offsets = [5u64, 9];
    let keys = [0u8, 1, 0];

    let leaf = ColumnBuilder::fixed(&values, 4, 4).expect("leaf");
    let nullable = leaf.nullable(&null_map).expect("nullable");
    let array = nullable.array(&array_offsets, 3).expect("array");
    let dict = ColumnBuilder::string(&dict_offsets, b"alphabeta", 2).expect("dict");
    let lc = dict.low_cardinality(1, &keys, 3).expect("lc");

    let mut builder = BlockBuilder::new();
    builder
        .append("nums", array_ty.view(), &array)
        .expect("append nums");
    builder
        .append("tag", lc_ty.view(), &lc)
        .expect("append tag");

    let mut sink = Sink::new();
    builder
        .write(sink.as_mut(), BlockOpts::default())
        .expect("write");
    sink.as_ref().get_ref().data.clone()
}

/// Write-only backend, used once to capture the valid encoding.
struct Sink {
    io: sys::chc_io,
    data: Vec<u8>,
    _pin: PhantomPinned,
}

impl Sink {
    fn new() -> Pin<Box<Self>> {
        let mut boxed = Box::pin(Self {
            io: sys::chc_io {
                ud: core::ptr::null_mut(),
                read: None,
                write: Some(write),
                check_cancel: None,
            },
            data: Vec::new(),
            _pin: PhantomPinned,
        });
        // SAFETY: sets a field at the pinned address; never moves out.
        unsafe {
            let this = boxed.as_mut().get_unchecked_mut();
            this.io.ud = (this as *mut Self).cast();
        }
        boxed
    }
}

// SAFETY: as for `Bytes`, with `write` in place of `read`.
unsafe impl Io for Sink {
    fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io {
        // SAFETY: field pointer; does not move self.
        unsafe { &mut self.get_unchecked_mut().io as *mut sys::chc_io }
    }
}

unsafe extern "C" fn write(
    ud: *mut c_void,
    buf: *const c_void,
    len: usize,
    _err: *mut sys::chc_err,
) -> c_int {
    let io = unsafe { &mut *(ud as *mut Sink) };
    io.data
        .extend_from_slice(unsafe { core::slice::from_raw_parts(buf.cast::<u8>(), len) });
    sys::CHC_OK
}

/// Decode `bytes`, then walk whatever came back. Returning at all is the
/// assertion; a panic or a sanitizer report is the failure.
fn decode_and_walk(bytes: Vec<u8>) {
    let alloc = Allocator::stdlib();
    let mut io = Bytes::new(bytes);
    let Ok(mut reader) = BlockReader::new(io.as_mut(), alloc, BlockOpts::default()) else {
        return;
    };
    while let Ok(Some(block)) = reader.read() {
        // Traverse the way a consumer would, including the accessors that
        // build slices from column-side lengths.
        if block.validate().is_err() {
            continue;
        }
        for i in 0..block.n_columns() {
            let _ = block.column_name(i);
            let _ = block.column_type(i).map(|t| t.format());
            let Some(col) = block.column(i) else { continue };
            walk(col);
        }
    }
}

fn walk(col: clickhouse_c::Column<'_>) {
    let _ = col.fixed();
    if let Some((offsets, data)) = col.string() {
        let mut start = 0usize;
        for &end in offsets {
            let end = (end as usize).min(data.len());
            let _ = &data[start.min(end)..end];
            start = end;
        }
    }
    let _ = col.null_map();
    let _ = col.array_offsets();
    if let Some(view) = col.low_cardinality() {
        let _ = view.keys;
        walk(view.dict);
    }
    if let Some(inner) = col.nullable_inner() {
        walk(inner);
    }
    if let Some(values) = col.array_values() {
        walk(values);
    }
    for i in 0..col.tuple_arity() {
        if let Some(child) = col.tuple_child(i) {
            walk(child);
        }
    }
}

/// Deterministic so a failure is reproducible from the seed alone.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

#[test]
fn every_truncation_is_rejected_cleanly() {
    let valid = valid_block();
    assert!(valid.len() > 32, "the fixture should be substantial");
    for cut in 0..valid.len() {
        decode_and_walk(valid[..cut].to_vec());
    }
}

#[test]
fn every_single_byte_corruption_is_survivable() {
    let valid = valid_block();
    // 0xFF turns a varint continuation on and blows up a length field, which
    // is the interesting direction; 0x00 and a bit flip cover the rest.
    for at in 0..valid.len() {
        for patch in [0x00u8, 0xFF, valid[at] ^ 0x01, valid[at] ^ 0x80] {
            let mut bytes = valid.clone();
            bytes[at] = patch;
            decode_and_walk(bytes);
        }
    }
}

#[test]
fn random_mutations_are_survivable() {
    let valid = valid_block();
    let mut rng = Rng(0x5eed_1234_abcd_ef01);
    for _ in 0..4000 {
        let mut bytes = valid.clone();
        let mutations = 1 + (rng.next() % 8) as usize;
        for _ in 0..mutations {
            let at = (rng.next() % bytes.len() as u64) as usize;
            bytes[at] = (rng.next() & 0xFF) as u8;
        }
        if rng.next() % 4 == 0 {
            bytes.truncate((rng.next() % bytes.len() as u64) as usize);
        }
        decode_and_walk(bytes);
    }
}

#[test]
fn pure_garbage_is_rejected_cleanly() {
    let mut rng = Rng(0x0bad_c0de_0bad_c0de);
    for len in [0usize, 1, 2, 7, 64, 512] {
        for _ in 0..200 {
            let bytes = (0..len).map(|_| (rng.next() & 0xFF) as u8).collect();
            decode_and_walk(bytes);
        }
    }
}

/// The same sweep through the packet parser, which frames blocks differently
/// and is what a network peer actually reaches.
#[test]
fn the_packet_parser_survives_garbage() {
    let mut rng = Rng(0xfeed_face_dead_beef);
    for _ in 0..500 {
        let mut core =
            IolessClient::new(&ClientOpts::new(), Allocator::stdlib(), None).expect("construct");
        // Past the Hello the machine is in the state a real connection reaches.
        assert!(matches!(
            core.handshake().expect("hello queued"),
            Step::NeedsInput
        ));
        let len = (rng.next() % 512) as usize;
        let bytes: Vec<u8> = (0..len).map(|_| (rng.next() & 0xFF) as u8).collect();
        if core.submit(&bytes).is_err() {
            continue;
        }
        // Either a decoded event, an error, or "need more" -- any is fine, a
        // panic is not.
        let _ = core.handshake();
        let _ = core.recv_event();
    }
}
