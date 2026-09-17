//! Custom in-memory [`Io`] implementation and Native round-trip tests.
//!
//! Tests encode and decode blocks through public transport interface without
//! external processes.

use core::ffi::{c_int, c_void};
use core::marker::PhantomPinned;
use core::pin::Pin;

use clickhouse_c::{
    Allocator, BlockBuilder, BlockOpts, BlockReader, ColumnBuilder, ColumnLayout, ErrorKind, Io,
    Kind, SliceIo, TypeAst, sys,
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

/// Re-emits a decoded column beside a locally built one, without visiting values.
#[test]
fn decoded_column_splices_beside_a_local_column() {
    let alloc = Allocator::stdlib();
    let map_ty = TypeAst::parse("Map(String, Nullable(String))", alloc).expect("map type");
    let id_ty = TypeAst::parse("UInt64", alloc).expect("id type");

    // Two rows hold three pairs: {a=>x, b=>NULL} and {c=>z}
    let key_data = b"abc";
    let key_offsets = [1u64, 2, 3];
    let val_data = b"xz";
    let val_offsets = [1u64, 1, 2];
    let val_nulls = [0u8, 1, 0];
    let entry_offsets = [2u64, 3];

    let keys = ColumnBuilder::string(&key_offsets, key_data, 3).expect("keys");
    let val_leaf = ColumnBuilder::string(&val_offsets, val_data, 3).expect("val leaf");
    let vals = val_leaf.nullable(&val_nulls).expect("vals");
    let children = [keys, vals];
    let mut ptrs = [core::ptr::null_mut(); 2];
    let tuple = ColumnBuilder::tuple(&children, &mut ptrs).expect("tuple");
    let map = tuple.array(&entry_offsets, 2).expect("map");

    let mut source = BlockBuilder::new();
    source.append("kv", map_ty.view(), &map).expect("append kv");
    let mut wio = MemIo::new();
    source
        .write(wio.as_mut(), BlockOpts::default())
        .expect("write source");
    let native = wio.as_ref().written().to_vec();

    // Decode partial block, then re-emit it beside a column built here
    let mut rio = SliceIo::new(&native);
    let mut reader = BlockReader::new(rio.as_mut(), alloc, BlockOpts::default()).expect("reader");
    let decoded = reader.read().expect("read").expect("one block");
    decoded.validate().expect("validate");
    assert!(reader.read().expect("eof read").is_none());
    drop(reader);

    let ids: Vec<u8> = [7u64, 9].iter().flat_map(|v| v.to_le_bytes()).collect();
    let id_col = ColumnBuilder::fixed(&ids, 8, 2).expect("id col");
    let mut merged = BlockBuilder::new();
    merged
        .append("id", id_ty.view(), &id_col)
        .expect("append id");
    merged
        .append_column("kv", map_ty.view(), decoded.column(0).expect("kv column"))
        .expect("append kv column");

    let mut mio = MemIo::new();
    merged
        .write(mio.as_mut(), BlockOpts::default())
        .expect("write merged");
    let mut reader = BlockReader::new(mio.as_mut(), alloc, BlockOpts::default()).expect("reader");
    let out = reader.read().expect("read").expect("one block");
    out.validate().expect("validate");

    assert_eq!(out.n_rows(), 2);
    assert_eq!(out.column_name(0), Some(&b"id"[..]));
    assert_eq!(out.column_name(1), Some(&b"kv"[..]));
    let kv = out.column(1).expect("kv");
    assert_eq!(kv.array_offsets(), Some(&entry_offsets[..]));
    let pairs = kv.array_values().expect("pairs");
    let (koff, kdata) = pairs.tuple_child(0).and_then(|c| c.string()).expect("keys");
    assert_eq!((koff, kdata), (&key_offsets[..], &key_data[..]));
    let vcol = pairs.tuple_child(1).expect("vals");
    assert_eq!(vcol.null_map(), Some(&val_nulls[..]));
}

/// Rejects a decoded column whose row count disagrees with the block.
#[test]
fn append_column_rejects_row_count_mismatch() {
    let alloc = Allocator::stdlib();
    let ty = TypeAst::parse("UInt32", alloc).expect("type");

    let two: Vec<u8> = [1u32, 2].iter().flat_map(|v| v.to_le_bytes()).collect();
    let col = ColumnBuilder::fixed(&two, 4, 2).expect("col");
    let mut source = BlockBuilder::new();
    source.append("x", ty.view(), &col).expect("append");
    let mut wio = MemIo::new();
    source
        .write(wio.as_mut(), BlockOpts::default())
        .expect("write");
    let native = wio.as_ref().written().to_vec();

    let mut rio = SliceIo::new(&native);
    let mut reader = BlockReader::new(rio.as_mut(), alloc, BlockOpts::default()).expect("reader");
    let decoded = reader.read().expect("read").expect("one block");

    let three: Vec<u8> = [1u32, 2, 3].iter().flat_map(|v| v.to_le_bytes()).collect();
    let local = ColumnBuilder::fixed(&three, 4, 3).expect("local");
    let mut merged = BlockBuilder::new();
    merged.append("y", ty.view(), &local).expect("append y");
    let err = merged
        .append_column("x", ty.view(), decoded.column(0).expect("x"))
        .expect_err("row count mismatch");
    assert!(err.to_string().contains("row count mismatch"), "{err}");
}

/// Builds a block holding one column of every decoded layout.
fn all_layouts(io: Pin<&mut MemIo>) {
    let alloc = Allocator::stdlib();
    let fixed_ty = TypeAst::parse("UInt32", alloc).expect("fixed type");
    let string_ty = TypeAst::parse("String", alloc).expect("string type");
    let nullable_ty = TypeAst::parse("Nullable(UInt32)", alloc).expect("nullable type");
    let array_ty = TypeAst::parse("Array(UInt32)", alloc).expect("array type");
    let tuple_ty = TypeAst::parse("Tuple(UInt32, String)", alloc).expect("tuple type");
    let lc_ty = TypeAst::parse("LowCardinality(String)", alloc).expect("lc type");

    let nums: Vec<u8> = [1u32, 2].iter().flat_map(|v| v.to_le_bytes()).collect();
    // Two empty strings leave the data slab empty
    let empty_offsets = [0u64, 0];
    let null_map = [0u8, 1];
    let array_offsets = [1u64, 2];
    let dict_offsets = [1u64, 2];
    let keys = [0u8, 1];

    let fixed = ColumnBuilder::fixed(&nums, 4, 2).expect("fixed");
    let string = ColumnBuilder::string(&empty_offsets, &[], 2).expect("string");
    let nullable = fixed.nullable(&null_map).expect("nullable");
    let array = fixed.array(&array_offsets, 2).expect("array");
    let dict = ColumnBuilder::string(&dict_offsets, b"pq", 2).expect("dict");
    let lc = dict.low_cardinality(1, &keys, 2).expect("lc");
    let children = [
        ColumnBuilder::fixed(&nums, 4, 2).expect("tuple fixed"),
        ColumnBuilder::string(&empty_offsets, &[], 2).expect("tuple string"),
    ];
    let mut ptrs = [core::ptr::null_mut(); 2];
    let tuple = ColumnBuilder::tuple(&children, &mut ptrs).expect("tuple");

    let mut builder = BlockBuilder::new();
    builder.append("f", fixed_ty.view(), &fixed).expect("f");
    builder.append("s", string_ty.view(), &string).expect("s");
    builder
        .append("n", nullable_ty.view(), &nullable)
        .expect("n");
    builder.append("a", array_ty.view(), &array).expect("a");
    builder.append("t", tuple_ty.view(), &tuple).expect("t");
    builder.append("l", lc_ty.view(), &lc).expect("l");
    builder.write(io, BlockOpts::default()).expect("write");
}

/// Every typed accessor answers None for a column of another layout, so a
/// caller cannot read one layout's slab through another's getter.
#[test]
fn accessors_reject_a_foreign_layout() {
    let alloc = Allocator::stdlib();
    let mut io = MemIo::new();
    all_layouts(io.as_mut());
    let mut reader = BlockReader::new(io.as_mut(), alloc, BlockOpts::default()).expect("reader");
    let block = reader.read().expect("read").expect("one block");
    block.validate().expect("validate");

    let by_name = |name: &[u8]| {
        (0..block.n_columns())
            .find(|&i| block.column_name(i) == Some(name))
            .and_then(|i| block.column(i))
            .expect("column")
    };
    let fixed = by_name(b"f");
    let string = by_name(b"s");
    let nullable = by_name(b"n");
    let array = by_name(b"a");
    let tuple = by_name(b"t");
    let lc = by_name(b"l");

    assert!(string.fixed().is_none());
    assert!(fixed.string().is_none());
    assert!(fixed.null_map().is_none());
    assert!(fixed.nullable_inner().is_none());
    assert!(fixed.array_offsets().is_none());
    assert!(fixed.array_values().is_none());
    assert!(fixed.low_cardinality().is_none());
    assert!(fixed.tuple_child(0).is_none());
    assert_eq!(fixed.tuple_arity(), 0);

    // Wrapper layouts expose only their own parts
    assert!(nullable.fixed().is_none());
    assert!(array.fixed().is_none());
    assert!(lc.string().is_none());
    assert!(tuple.tuple_child(2).is_none());
    assert_eq!(tuple.tuple_arity(), 2);
}

/// Rows of empty strings leave nothing to borrow, so data must read as empty
/// rather than as a slice over the allocation.
#[test]
fn empty_strings_expose_an_empty_slab() {
    let alloc = Allocator::stdlib();
    let mut io = MemIo::new();
    all_layouts(io.as_mut());
    let mut reader = BlockReader::new(io.as_mut(), alloc, BlockOpts::default()).expect("reader");
    let block = reader.read().expect("read").expect("one block");

    let string = block.column(1).expect("string column");
    let (offsets, data) = string.string().expect("string parts");
    assert_eq!(offsets, &[0, 0][..]);
    assert!(data.is_empty());
}

/// File framing carries no BlockInfo, so a decoded block reports neither an
/// overflow marker nor a two-level bucket.
#[test]
fn file_framing_leaves_block_info_unset() {
    let alloc = Allocator::stdlib();
    let mut io = MemIo::new();
    all_layouts(io.as_mut());
    let mut reader = BlockReader::new(io.as_mut(), alloc, BlockOpts::default()).expect("reader");
    let block = reader.read().expect("read").expect("one block");

    assert!(!block.is_overflows());
    assert_eq!(block.bucket_num(), 0);
}

#[test]
fn column_index_past_the_end_is_none() {
    let alloc = Allocator::stdlib();
    let mut io = MemIo::new();
    all_layouts(io.as_mut());
    let mut reader = BlockReader::new(io.as_mut(), alloc, BlockOpts::default()).expect("reader");
    let block = reader.read().expect("read").expect("one block");

    let past = block.n_columns();
    assert!(block.column_name(past).is_none());
    assert!(block.column_type(past).is_none());
    assert!(block.column(past).is_none());
}

/// A backend that installs no deadline reports a usage error rather than
/// silently ignoring the request.
#[test]
fn a_backend_without_deadlines_rejects_a_timeout() {
    let mut io = MemIo::new();
    let err = io
        .as_mut()
        .set_read_timeout(Some(core::time::Duration::from_secs(1)))
        .expect_err("no deadline support");
    assert_eq!(err.kind, ErrorKind::Usage);
    assert!(io.as_mut().set_read_timeout(None).is_err());
}

/// Slice transport hands bytes out in order and reports what it still holds.
#[test]
fn slice_transport_tracks_its_remaining_bytes() {
    let alloc = Allocator::stdlib();
    let mut wio = MemIo::new();
    all_layouts(wio.as_mut());
    let native = wio.as_ref().written().to_vec();

    let mut io = SliceIo::new(&native);
    assert_eq!(io.as_ref().remaining(), native.len());
    let mut reader = BlockReader::new(io.as_mut(), alloc, BlockOpts::default()).expect("reader");
    reader.read().expect("read").expect("one block");
    drop(reader);
    // Reader buffers ahead, so a read leaves nothing behind for this input
    assert_eq!(io.as_ref().remaining(), 0);
}

/// Slice transport is read-only; a write must fail instead of being dropped.
#[test]
fn slice_transport_refuses_writes() {
    let alloc = Allocator::stdlib();
    let ty = TypeAst::parse("UInt32", alloc).expect("type");
    let data = 1u32.to_le_bytes();
    let col = ColumnBuilder::fixed(&data, 4, 1).expect("col");
    let mut builder = BlockBuilder::new();
    builder.append("x", ty.view(), &col).expect("append");

    let mut io = SliceIo::new(&[]);
    let err = builder
        .write(io.as_mut(), BlockOpts::default())
        .expect_err("read-only transport");
    assert_eq!(err.kind, ErrorKind::Io);
    assert!(err.message.contains("read-only"), "{err}");
}

/// A block with no rows still describes its structure. It is what a server
/// sends ahead of an INSERT, and it carries names and types with no column
/// bodies to borrow.
#[test]
fn a_zero_row_block_carries_structure_without_columns() {
    let alloc = Allocator::stdlib();
    let types = [
        ("f", "UInt32"),
        ("s", "String"),
        ("n", "Nullable(UInt32)"),
        ("a", "Array(UInt32)"),
        ("l", "LowCardinality(String)"),
    ];
    let parsed: Vec<_> = types
        .iter()
        .map(|(_, ty)| TypeAst::parse(ty, alloc).expect("type"))
        .collect();

    let fixed = ColumnBuilder::fixed(&[], 4, 0).expect("fixed");
    let string = ColumnBuilder::string(&[], &[], 0).expect("string");
    let nullable = fixed.nullable(&[]).expect("nullable");
    let array = fixed.array(&[], 0).expect("array");
    let dict = ColumnBuilder::string(&[], &[], 0).expect("dict");
    let lc = dict.low_cardinality(1, &[], 0).expect("lc");
    let cols = [&fixed, &string, &nullable, &array, &lc];

    let mut builder = BlockBuilder::new();
    for ((name, _), (ty, col)) in types.iter().zip(parsed.iter().zip(cols)) {
        builder.append(name, ty.view(), col).expect(name);
    }
    let mut io = MemIo::new();
    builder
        .write(io.as_mut(), BlockOpts::default())
        .expect("write");

    let mut reader = BlockReader::new(io.as_mut(), alloc, BlockOpts::default()).expect("reader");
    let block = reader.read().expect("read").expect("one block");
    // Validation walks a block that publishes no column bodies
    block.validate().expect("validate");

    assert_eq!(block.n_rows(), 0);
    assert_eq!(block.n_columns(), types.len());
    for (i, (name, ty)) in types.iter().enumerate() {
        assert_eq!(block.column_name(i), Some(name.as_bytes()));
        assert_eq!(
            block.column_type(i).map(|t| t.format()).as_deref(),
            Some(*ty),
        );
        assert!(block.column(i).is_none(), "{name} published a body");
    }
}
