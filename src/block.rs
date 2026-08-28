//! Native block reading and column access.
//!
//! [`BlockReader`] reads block streams from an [`Io`] implementation. TCP
//! clients also return [`Block`] values for Data packets.

use core::pin::Pin;
use core::ptr::NonNull;
use core::slice;

use crate::alloc::Allocator;
use crate::error::{Result, check};
use crate::io::Io;
use crate::sys;
use crate::types::TypeRef;

/// Storage layout of a decoded column.
///
/// Several ClickHouse types can share a layout. Use
/// [`Block::column_type`] to determine logical type.
#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum ColumnLayout {
    /// Fixed-width values stored as contiguous little-endian bytes.
    Fixed = sys::CHC_COL_FIXED,
    /// Offsets and byte data used by `String` and string-encoded JSON types.
    String = sys::CHC_COL_STRING,
    /// Null map and dense inner column used by `Nullable(T)`.
    Nullable = sys::CHC_COL_NULLABLE,
    /// Offsets and element column used by `Array(T)`, and `Map(K, V)` as
    /// `Array(Tuple(K, V))`.
    Array = sys::CHC_COL_ARRAY,
    /// Parallel child columns used by tuples, nested values, geographic
    /// types, and QBit values.
    Tuple = sys::CHC_COL_TUPLE,
    /// Keys and dictionary column used by `LowCardinality(T)`.
    LowCardinality = sys::CHC_COL_LOW_CARDINALITY,
    /// Empty storage used by `Nothing` and `Nullable(Nothing)` inner columns.
    Nothing = sys::CHC_COL_NOTHING,
}

impl ColumnLayout {
    pub(crate) fn from_raw(k: sys::chc_col_kind) -> Option<Self> {
        Some(match k {
            sys::CHC_COL_FIXED => Self::Fixed,
            sys::CHC_COL_STRING => Self::String,
            sys::CHC_COL_NULLABLE => Self::Nullable,
            sys::CHC_COL_ARRAY => Self::Array,
            sys::CHC_COL_TUPLE => Self::Tuple,
            sys::CHC_COL_LOW_CARDINALITY => Self::LowCardinality,
            sys::CHC_COL_NOTHING => Self::Nothing,
            _ => return None,
        })
    }
}

/// Options that describe Native block framing.
///
/// Native files and native TCP protocol use different optional fields. TCP
/// values depend on negotiated server revision. Incorrect values prevent
/// block decoding.
#[derive(Clone, Copy, Default)]
pub struct BlockOpts {
    /// Includes 8-byte `BlockInfo` prefix used by TCP revision 51903 and later.
    pub has_block_info: bool,
    /// Includes custom serialization flag used by TCP revision 54454 and later.
    pub has_custom_serialization: bool,
    /// Read buffer size in bytes. Zero selects 8 KiB default.
    pub read_buffer_bytes: usize,
}

impl BlockOpts {
    pub(crate) fn to_raw(self) -> sys::chc_block_opts {
        sys::chc_block_opts {
            has_block_info: self.has_block_info,
            has_custom_serialization: self.has_custom_serialization,
            read_buffer_bytes: self.read_buffer_bytes,
        }
    }
}

/// Decoded Native block.
///
/// Value releases its memory with allocator used during decoding.
pub struct Block {
    raw: NonNull<sys::chc_block>,
    alloc: Allocator,
}

impl Block {
    /// Takes ownership of a raw block pointer.
    ///
    /// # Safety
    /// Caller must own `raw` and stop using it after this call. `alloc` must
    /// match allocator used to create block.
    pub(crate) unsafe fn from_raw(raw: *mut sys::chc_block, alloc: Allocator) -> Option<Self> {
        NonNull::new(raw).map(|raw| Self { raw, alloc })
    }

    pub fn n_rows(&self) -> usize {
        unsafe { sys::chc_block_n_rows(self.raw.as_ptr().cast_const()) }
    }

    pub fn n_columns(&self) -> usize {
        unsafe { sys::chc_block_n_columns(self.raw.as_ptr().cast_const()) }
    }

    /// Returns column name bytes without UTF-8 validation.
    pub fn column_name(&self, i: usize) -> Option<&[u8]> {
        let mut len = 0;
        let p = unsafe { sys::chc_block_column_name(self.raw.as_ptr().cast_const(), i, &mut len) };
        if p.is_null() {
            None
        } else {
            Some(unsafe { slice::from_raw_parts(p.cast::<u8>(), len) })
        }
    }

    pub fn column_type(&self, i: usize) -> Option<TypeRef<'_>> {
        let p = unsafe { sys::chc_block_column_type(self.raw.as_ptr().cast_const(), i) };
        if p.is_null() {
            None
        } else {
            Some(TypeRef {
                raw: p,
                _marker: core::marker::PhantomData,
            })
        }
    }

    pub fn column(&self, i: usize) -> Option<Column<'_>> {
        let p = unsafe { sys::chc_block_column(self.raw.as_ptr().cast_const(), i) };
        if p.is_null() {
            None
        } else {
            Some(Column {
                raw: p,
                _marker: core::marker::PhantomData,
            })
        }
    }

    /// Validates structural relationships within every column.
    ///
    /// Validation checks array offset order and LowCardinality dictionary
    /// indexes. Call this method before using offsets or keys from untrusted
    /// data as indexes.
    ///
    /// Runtime cost is proportional to row count.
    pub fn validate(&self) -> Result<()> {
        for i in 0..self.n_columns() {
            if let Some(col) = self.column(i) {
                col.validate()?;
            }
        }
        Ok(())
    }

    /// Returns server flag marking a block truncated by `max_rows_to_group_by`
    /// with `group_by_overflow_mode = 'any'`.
    pub fn is_overflows(&self) -> bool {
        unsafe { sys::chc_block_is_overflows(self.raw.as_ptr().cast_const()) }
    }

    /// Returns two-level aggregation bucket, or -1 when not applicable.
    pub fn bucket_num(&self) -> i32 {
        unsafe { sys::chc_block_bucket_num(self.raw.as_ptr().cast_const()) }
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        unsafe { sys::chc_block_destroy(self.raw.as_ptr(), self.alloc.as_ptr()) };
    }
}

unsafe impl Send for Block {}

/// Reads consecutive Native [`Block`] values from an [`Io`] implementation.
///
/// Reader retains buffered bytes between calls to [`read`](Self::read).
pub struct BlockReader<'io, I: Io + ?Sized> {
    raw: NonNull<sys::chc_in>,
    // `raw` retains pointer into pinned I/O value
    _io: Pin<&'io mut I>,
    // C reader retains allocator address until destruction
    alloc: Box<Allocator>,
    opts: sys::chc_block_opts,
}

impl<'io, I: Io + ?Sized> BlockReader<'io, I> {
    /// Creates a reader using framing described by `opts`.
    ///
    /// Use [`BlockOpts::default`] for output from `clickhouse local`.
    pub fn new(mut io: Pin<&'io mut I>, alloc: Allocator, opts: BlockOpts) -> Result<Self> {
        let raw_opts = opts.to_raw();
        // C reader retains this address
        let alloc = Box::new(alloc);
        let mut raw: *mut sys::chc_in = core::ptr::null_mut();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_rs_in_new(
                io.as_mut().io_ptr(),
                alloc.as_ptr(),
                raw_opts.read_buffer_bytes,
                &mut raw,
                &mut err,
            )
        };
        check(rc, &err)?;
        let raw = NonNull::new(raw).expect("chc_rs_in_new returned CHC_OK with null reader");
        Ok(Self {
            raw,
            _io: io,
            alloc,
            opts: raw_opts,
        })
    }

    /// Decodes next block. Returns `None` for EOF at a block boundary.
    pub fn read(&mut self) -> Result<Option<Block>> {
        let mut out: *mut sys::chc_block = core::ptr::null_mut();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_block_read(
                self.raw.as_ptr(),
                self.alloc.as_ptr(),
                &self.opts,
                &mut out,
                &mut err,
            )
        };
        check(rc, &err)?;
        Ok(NonNull::new(out).map(|raw| Block {
            raw,
            alloc: *self.alloc,
        }))
    }
}

impl<I: Io + ?Sized> Drop for BlockReader<'_, I> {
    fn drop(&mut self) {
        unsafe { sys::chc_rs_in_destroy(self.raw.as_ptr(), self.alloc.as_ptr()) };
    }
}

/// Borrowed view of a block column.
#[derive(Clone, Copy)]
pub struct Column<'b> {
    pub(crate) raw: *const sys::chc_column,
    pub(crate) _marker: core::marker::PhantomData<&'b sys::chc_column>,
}

impl<'b> Column<'b> {
    pub fn layout(&self) -> Option<ColumnLayout> {
        ColumnLayout::from_raw(unsafe { sys::chc_column_layout(self.raw) })
    }

    pub fn n_rows(&self) -> usize {
        unsafe { sys::chc_column_n_rows(self.raw) }
    }

    /// Validates array offsets and LowCardinality dictionary indexes.
    ///
    /// Validation includes nested columns and returns
    /// [`ErrorKind::Protocol`](crate::ErrorKind::Protocol) for invalid data.
    pub fn validate(&self) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_column_validate(self.raw, &mut err) };
        check(rc, &err)
    }

    /// Returns element width and little-endian data for a fixed-width column.
    pub fn fixed(&self) -> Option<(usize, &'b [u8])> {
        let Some(ColumnLayout::Fixed) = self.layout() else {
            return None;
        };
        let mut elem_size = 0usize;
        let ptr = unsafe { sys::chc_column_fixed_data(self.raw, &mut elem_size) };
        if ptr.is_null() {
            return None;
        }
        let n = self.n_rows().checked_mul(elem_size)?;
        let bytes = unsafe { slice::from_raw_parts(ptr.cast::<u8>(), n) };
        Some((elem_size, bytes))
    }

    /// Returns offsets and bytes for a string column.
    ///
    /// Each offset is exclusive end of corresponding row in host byte order.
    /// Layout also represents string-encoded JSON and LowCardinality
    /// dictionaries.
    pub fn string(&self) -> Option<(&'b [u64], &'b [u8])> {
        let Some(ColumnLayout::String) = self.layout() else {
            return None;
        };
        let n = self.n_rows();
        let offsets_ptr = unsafe { sys::chc_column_string_offsets(self.raw) };
        let data_ptr = unsafe { sys::chc_column_string_data(self.raw) };
        if offsets_ptr.is_null() || (data_ptr.is_null() && n > 0) {
            return None;
        }
        let offsets = unsafe { slice::from_raw_parts(offsets_ptr, n) };
        // Bound data by both final offset and recorded allocation size
        // SAFETY: String layout selects `str_` union member
        let capacity = unsafe { (*self.raw).payload.str_.bytes };
        let claimed = offsets.last().copied().unwrap_or(0) as usize;
        debug_assert!(
            offsets.windows(2).all(|w| w[0] <= w[1]) && claimed <= capacity,
            "clickhouse-c published string offsets outside its own data slab",
        );
        let data_len = claimed.min(capacity);
        let data = if data_len == 0 || data_ptr.is_null() {
            &[][..]
        } else {
            unsafe { slice::from_raw_parts(data_ptr, data_len) }
        };
        Some((offsets, data))
    }

    pub fn null_map(&self) -> Option<&'b [u8]> {
        let Some(ColumnLayout::Nullable) = self.layout() else {
            return None;
        };
        let p = unsafe { sys::chc_column_null_map(self.raw) };
        if p.is_null() {
            return None;
        }
        Some(unsafe { slice::from_raw_parts(p, self.n_rows()) })
    }

    pub fn nullable_inner(&self) -> Option<Column<'b>> {
        let p = unsafe { sys::chc_column_nullable_inner(self.raw) };
        if p.is_null() {
            None
        } else {
            Some(Column {
                raw: p,
                _marker: core::marker::PhantomData,
            })
        }
    }

    pub fn array_offsets(&self) -> Option<&'b [u64]> {
        let Some(ColumnLayout::Array) = self.layout() else {
            return None;
        };
        let p = unsafe { sys::chc_column_array_offsets(self.raw) };
        if p.is_null() {
            None
        } else {
            Some(unsafe { slice::from_raw_parts(p, self.n_rows()) })
        }
    }

    pub fn array_values(&self) -> Option<Column<'b>> {
        let p = unsafe { sys::chc_column_array_values(self.raw) };
        if p.is_null() {
            None
        } else {
            Some(Column {
                raw: p,
                _marker: core::marker::PhantomData,
            })
        }
    }

    pub fn tuple_arity(&self) -> usize {
        unsafe { sys::chc_column_tuple_arity(self.raw) }
    }

    pub fn tuple_child(&self, i: usize) -> Option<Column<'b>> {
        let p = unsafe { sys::chc_column_tuple_child(self.raw, i) };
        if p.is_null() {
            None
        } else {
            Some(Column {
                raw: p,
                _marker: core::marker::PhantomData,
            })
        }
    }

    /// Returns keys and dictionary for a LowCardinality column.
    pub fn low_cardinality(&self) -> Option<LowCardinalityView<'b>> {
        let Some(ColumnLayout::LowCardinality) = self.layout() else {
            return None;
        };
        let key_size = unsafe { sys::chc_column_lc_key_size(self.raw) };
        if key_size <= 0 {
            return None;
        }
        debug_assert!(
            matches!(key_size, 1 | 2 | 4 | 8),
            "clickhouse-c published LowCardinality key_size = {key_size}",
        );
        let keys_ptr = unsafe { sys::chc_column_lc_keys(self.raw) };
        let dict_ptr = unsafe { sys::chc_column_lc_dict(self.raw) };
        if keys_ptr.is_null() || dict_ptr.is_null() {
            return None;
        }
        let keys_len = self.n_rows().checked_mul(key_size as usize)?;
        let keys = unsafe { slice::from_raw_parts(keys_ptr.cast::<u8>(), keys_len) };
        Some(LowCardinalityView {
            key_size: key_size as usize,
            keys,
            dict: Column {
                raw: dict_ptr,
                _marker: core::marker::PhantomData,
            },
        })
    }
}

/// Borrowed parts of a LowCardinality column.
pub struct LowCardinalityView<'b> {
    /// Key width in bytes. Valid values are 1, 2, 4, and 8.
    pub key_size: usize,
    /// Raw keys in host byte order. Length is `n_rows * key_size`.
    /// Call [`Column::validate`] before using untrusted keys as indexes.
    pub keys: &'b [u8],
    /// Dictionary referenced by keys.
    pub dict: Column<'b>,
}
