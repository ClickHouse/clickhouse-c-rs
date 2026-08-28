//! Custom in-memory [`Io`] implementation and Native round-trip tests.
//!
//! Tests encode and decode blocks through public transport interface without
//! external processes.

use core::ffi::{c_int, c_void};
use core::marker::PhantomPinned;
use core::pin::Pin;

use clickhouse_c::{
    Allocator, BlockBuilder, BlockOpts, BlockReader, ColumnBuilder, ColumnLayout, Io, Kind,
    TypeAst, sys,
};

/// In-memory transport backed by a byte vector.
///
/// Value is pinned because callback context points to this structure.
struct MemIo {
    io: sys::chc_io,
    buf: Vec<u8>,
    read_at: usize,
    _pin: PhantomPinned,
}

impl MemIo {
    fn new() -> Pin<Box<Self>> {
        let mut boxed = Box::pin(Self {
            io: sys::chc_io {
                ud: core::ptr::null_mut(),
                read: Some(mem_read),
                write: Some(mem_write),
                check_cancel: None,
            },
            buf: Vec::new(),
            read_at: 0,
            _pin: PhantomPinned,
        });
        // SAFETY: set context after address becomes stable
        unsafe {
            let this = boxed.as_mut().get_unchecked_mut();
            this.io.ud = (this as *mut Self).cast();
        }
        boxed
    }

    fn written(self: Pin<&Self>) -> &[u8] {
        &self.get_ref().buf
    }
}

// SAFETY: callback table and context remain valid within pinned MemIo
unsafe impl Io for MemIo {
    fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io {
        // SAFETY: returning field address does not move pinned value
        unsafe { &mut self.get_unchecked_mut().io as *mut sys::chc_io }
    }
}

/// Reads up to `len` bytes and reports zero at EOF.
unsafe extern "C" fn mem_read(
    ud: *mut c_void,
    buf: *mut c_void,
    len: usize,
    out_n: *mut usize,
    _err: *mut sys::chc_err,
) -> c_int {
    let io = unsafe { &mut *(ud as *mut MemIo) };
    let n = len.min(io.buf.len() - io.read_at);
    if n > 0 {
        unsafe {
            core::ptr::copy_nonoverlapping(io.buf[io.read_at..].as_ptr(), buf.cast::<u8>(), n)
        };
        io.read_at += n;
    }
    unsafe { *out_n = n };
    sys::CHC_OK
}

/// Appends all input bytes.
unsafe extern "C" fn mem_write(
    ud: *mut c_void,
    buf: *const c_void,
    len: usize,
    _err: *mut sys::chc_err,
) -> c_int {
    let io = unsafe { &mut *(ud as *mut MemIo) };
    io.buf
        .extend_from_slice(unsafe { core::slice::from_raw_parts(buf.cast::<u8>(), len) });
    sys::CHC_OK
}

/// Round-trips nested nullable array and LowCardinality string columns.
#[test]
fn composite_block_round_trips_through_a_custom_backend() {
    let alloc = Allocator::stdlib();
    let array_ty = TypeAst::parse("Array(Nullable(UInt32))", alloc).expect("array type");
    let lc_ty = TypeAst::parse("LowCardinality(String)", alloc).expect("lc type");

    // Three arrays contain four total nullable elements
    let values: Vec<u8> = [10u32, 0, 30, 40]
        .iter()
        .flat_map(|v| v.to_le_bytes())
        .collect();
    let null_map = [0u8, 1, 0, 0];
    let array_offsets = [2u64, 2, 4];

    // Keys select alpha, beta, and alpha
    let dict_data = b"alphabeta";
    let dict_offsets = [5u64, 9];
    let keys = [0u8, 1, 0];

    let leaf = ColumnBuilder::fixed(&values, 4, 4).expect("leaf");
    let nullable = leaf.nullable(&null_map).expect("nullable");
    let array = nullable.array(&array_offsets, 3).expect("array");
    let dict = ColumnBuilder::string(&dict_offsets, dict_data, 2).expect("dict");
    let lc = dict.low_cardinality(1, &keys, 3).expect("lc");

    let mut builder = BlockBuilder::new();
    builder
        .append("nums", array_ty.view(), &array)
        .expect("append nums");
    builder
        .append("tag", lc_ty.view(), &lc)
        .expect("append tag");

    let mut io = MemIo::new();
    builder
        .write(io.as_mut(), BlockOpts::default())
        .expect("write");
    assert!(!io.as_ref().written().is_empty());

    let mut reader = BlockReader::new(io.as_mut(), alloc, BlockOpts::default()).expect("reader");
    let block = reader.read().expect("read").expect("one block");
    block.validate().expect("validate");

    assert_eq!(block.n_rows(), 3);
    assert_eq!(block.n_columns(), 2);
    assert_eq!(block.column_name(0), Some(&b"nums"[..]));
    assert_eq!(
        block.column_type(0).and_then(|t| t.kind()),
        Some(Kind::Array)
    );

    let nums = block.column(0).expect("nums column");
    assert!(matches!(nums.layout(), Some(ColumnLayout::Array)));
    assert_eq!(nums.array_offsets(), Some(&array_offsets[..]));
    let inner = nums.array_values().expect("array values");
    assert_eq!(inner.null_map(), Some(&null_map[..]));
    let (elem_size, bytes) = inner
        .nullable_inner()
        .expect("nullable inner")
        .fixed()
        .expect("fixed");
    assert_eq!(elem_size, 4);
    assert_eq!(bytes, &values[..]);

    let tag = block.column(1).expect("tag column");
    let view = tag.low_cardinality().expect("lc view");
    assert_eq!(view.key_size, 1);
    assert_eq!(view.keys, &keys[..]);
    let (offsets, data) = view.dict.string().expect("dict strings");
    assert_eq!(offsets, &dict_offsets[..]);
    assert_eq!(data, &dict_data[..]);

    // Next read reaches EOF at block boundary
    assert!(reader.read().expect("eof read").is_none());
}

/// Verifies one reader preserves buffered data between consecutive blocks.
#[test]
fn successive_blocks_share_one_reader() {
    let alloc = Allocator::stdlib();
    let ty = TypeAst::parse("UInt32", alloc).expect("type");
    let mut io = MemIo::new();

    for chunk in [[1u32, 2].as_slice(), [3u32, 4, 5].as_slice()] {
        let bytes: Vec<u8> = chunk.iter().flat_map(|v| v.to_le_bytes()).collect();
        let col = ColumnBuilder::fixed(&bytes, 4, chunk.len()).expect("col");
        let mut builder = BlockBuilder::new();
        builder.append("x", ty.view(), &col).expect("append");
        builder
            .write(io.as_mut(), BlockOpts::default())
            .expect("write");
    }

    let mut reader = BlockReader::new(io.as_mut(), alloc, BlockOpts::default()).expect("reader");
    let mut seen = vec![];
    while let Some(block) = reader.read().expect("read") {
        let (_, bytes) = block.column(0).and_then(|c| c.fixed()).expect("fixed");
        seen.extend(
            bytes
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes(c.try_into().expect("u32"))),
        );
    }
    assert_eq!(seen, vec![1, 2, 3, 4, 5]);
}
