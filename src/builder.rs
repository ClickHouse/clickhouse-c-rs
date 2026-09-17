//! Native block construction and writing.
//!
//! [`ColumnBuilder`] borrows source data without copying it. Wrapper columns
//! also borrow their child columns. [`BlockBuilder`] retains these borrows
//! until block is written.

use core::ffi::c_int;
use core::marker::PhantomData;
use core::pin::Pin;

use crate::block::{BlockOpts, Column};
use crate::error::{Error, ErrorKind, Result, check};
use crate::io::Io;
use crate::sys;
use crate::types::TypeRef;

/// Column descriptor over borrowed data.
///
/// Create leaf columns with [`fixed`](Self::fixed) or [`string`](Self::string).
/// Create composite columns with [`nullable`](Self::nullable),
/// [`array`](Self::array), [`low_cardinality`](Self::low_cardinality), or
/// [`tuple`](Self::tuple). Composite builders borrow their child builders:
///
/// ```ignore
/// let leaf = ColumnBuilder::fixed(values, 4, 3)?;
/// let nullable = leaf.nullable(null_map)?;
/// let array = nullable.array(offsets, 2)?;
/// block.append("v", ty.view(), &array)?;
/// ```
pub struct ColumnBuilder<'a> {
    // Wrapper variants contain pointers to borrowed child nodes
    node: sys::chc_column,
    _marker: PhantomData<&'a ()>,
}

impl<'a> ColumnBuilder<'a> {
    /// Creates a fixed-width column.
    ///
    /// `data` must contain at least `n_rows * elem_size` little-endian bytes.
    /// Additional bytes are ignored.
    pub fn fixed(data: &'a [u8], elem_size: usize, n_rows: usize) -> Result<Self> {
        if elem_size == 0 {
            return Err(usage("fixed column: elem_size must be nonzero"));
        }
        require_covers(
            "fixed data",
            data.len(),
            checked_len(n_rows, elem_size, "fixed data")?,
        )?;
        Ok(node(unsafe {
            sys::chc_build_fixed(data.as_ptr().cast(), elem_size, n_rows)
        }))
    }

    /// Creates a string column.
    ///
    /// `offsets[i]` contains exclusive end of row `i` in `data`, in host byte
    /// order. Same layout supports string-encoded JSON and LowCardinality
    /// dictionaries.
    pub fn string(offsets: &'a [u64], data: &'a [u8], n_rows: usize) -> Result<Self> {
        validate_string(offsets, data.len(), n_rows, "string")?;
        Ok(node(unsafe {
            sys::chc_build_string(offsets.as_ptr(), data.as_ptr(), n_rows)
        }))
    }

    /// Creates a nullable column around this column.
    ///
    /// `null_map[i] == 1` marks row `i` as null. Map length must equal inner
    /// row count. Inner column must contain a value for every row.
    pub fn nullable<'r>(&'r self, null_map: &'r [u8]) -> Result<ColumnBuilder<'r>> {
        require_len("nullable null map", null_map.len(), self.n_rows())?;
        let inner = self.node_ptr().cast_mut();
        Ok(node(unsafe {
            sys::chc_build_nullable(null_map.as_ptr(), inner)
        }))
    }

    /// Creates an array column using this column as element storage.
    ///
    /// `offsets[i]` contains cumulative exclusive end of row `i`. Final offset
    /// must equal row count of element column.
    pub fn array<'r>(&'r self, offsets: &'r [u64], n_rows: usize) -> Result<ColumnBuilder<'r>> {
        let inner_n = validate_offsets(offsets, n_rows, "array")?;
        require_len("array values", self.n_rows(), inner_n)?;
        let values = self.node_ptr().cast_mut();
        Ok(node(unsafe {
            sys::chc_build_array(offsets.as_ptr(), n_rows, values)
        }))
    }

    /// Creates a LowCardinality column using this column as dictionary.
    ///
    /// `key_size` must be 1, 2, 4, or 8 bytes. Each key indexes dictionary.
    /// `LowCardinality(Nullable(T))` uses dictionary entry zero and key zero
    /// for null rows.
    pub fn low_cardinality<'r>(
        &'r self,
        key_size: i32,
        keys: &'r [u8],
        n_rows: usize,
    ) -> Result<ColumnBuilder<'r>> {
        validate_low_cardinality_keys(keys, key_size, n_rows, self.n_rows())?;
        let dict = self.node_ptr().cast_mut();
        Ok(node(unsafe {
            sys::chc_build_lc(key_size as c_int, keys.as_ptr().cast(), n_rows, dict)
        }))
    }

    /// Creates a tuple column from `children`.
    ///
    /// All children must have same row count. `ptrs` provides temporary
    /// pointer storage and must have same length as `children`. Returned
    /// builder borrows both slices. Maps, nested values, and geographic
    /// types use tuple storage internally.
    pub fn tuple<'r>(
        children: &'r [ColumnBuilder<'a>],
        ptrs: &'r mut [*mut sys::chc_column],
    ) -> Result<ColumnBuilder<'r>> {
        let Some(first) = children.first() else {
            return Err(usage("tuple column: needs at least one child"));
        };
        if ptrs.len() != children.len() {
            return Err(usage(format!(
                "tuple ptr scratch length mismatch: {} vs {} children",
                ptrs.len(),
                children.len()
            )));
        }
        let n_rows = first.n_rows();
        for (i, child) in children.iter().enumerate() {
            if child.n_rows() != n_rows {
                return Err(usage(format!(
                    "tuple child {i} row count mismatch: {} vs {n_rows}",
                    child.n_rows()
                )));
            }
            ptrs[i] = child.node_ptr().cast_mut();
        }
        Ok(node(unsafe {
            sys::chc_build_tuple(ptrs.as_mut_ptr(), children.len())
        }))
    }

    /// Returns row count for this column.
    pub fn n_rows(&self) -> usize {
        self.node.n_rows
    }

    fn node_ptr(&self) -> *const sys::chc_column {
        &self.node
    }
}

// Bind C node to lifetime of data referenced by it
fn node<'x>(node: sys::chc_column) -> ColumnBuilder<'x> {
    ColumnBuilder {
        node,
        _marker: PhantomData,
    }
}

/// Native block assembled from borrowed column data.
pub struct BlockBuilder<'a> {
    // Raw builder points into this allocation
    cols: Vec<sys::chc_block_col>,
    raw: sys::chc_block_builder,
    n_rows: Option<usize>,
    _marker: PhantomData<&'a ()>,
}

impl<'a> Default for BlockBuilder<'a> {
    fn default() -> Self {
        Self::new()
    }
}

impl<'a> BlockBuilder<'a> {
    /// Creates an empty block. First appended column sets row count.
    pub fn new() -> Self {
        Self {
            cols: Vec::new(),
            raw: sys::chc_block_builder::zeroed(),
            n_rows: None,
            _marker: PhantomData,
        }
    }

    /// Adds a named column with ClickHouse type `ty`.
    ///
    /// Column must match `ty` and use same row count as other columns. Builder
    /// borrows column and its data until block is written.
    pub fn append(
        &mut self,
        name: &'a str,
        ty: TypeRef<'a>,
        col: &'a ColumnBuilder<'a>,
    ) -> Result<()> {
        self.push(name, ty, col.node_ptr(), col.n_rows())
    }

    fn push(
        &mut self,
        name: &'a str,
        ty: TypeRef<'a>,
        col: *const sys::chc_column,
        n_rows: usize,
    ) -> Result<()> {
        match self.n_rows {
            Some(prev) if prev != n_rows => {
                return Err(usage(format!(
                    "block_builder: row count mismatch ({prev} vs {n_rows})"
                )));
            }
            _ => self.n_rows = Some(n_rows),
        }
        self.cols.push(sys::chc_block_col {
            name: name.as_ptr().cast(),
            name_len: name.len(),
            type_: ty.raw,
            col,
        });
        // Refresh pointer after possible Vec reallocation
        self.raw.cols = self.cols.as_mut_ptr();
        self.raw.n_cols = self.cols.len();
        self.raw.n_rows = n_rows;
        Ok(())
    }

    /// Adds a named column backed by a decoded [`Column`].
    ///
    /// Re-emits a column read by [`BlockReader`](crate::BlockReader) without
    /// visiting its values. Builder borrows column tree, so owning
    /// [`Block`](crate::Block) must outlive builder. `ty` describes column as
    /// writer must encode it; writer rejects a tree that does not match.
    pub fn append_column(&mut self, name: &'a str, ty: TypeRef<'a>, col: Column<'a>) -> Result<()> {
        self.push(name, ty, col.raw, col.n_rows())
    }

    /// Writes block through an [`Io`] implementation.
    ///
    /// `opts` must describe expected framing. `clickhouse local` accepts
    /// [`BlockOpts::default`].
    ///
    /// [`BlockReader`]: crate::BlockReader
    pub fn write<I: Io + ?Sized>(&self, io: Pin<&mut I>, opts: BlockOpts) -> Result<()> {
        let raw_opts = opts.to_raw();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_block_write(io.io_ptr(), &self.raw, &raw_opts, &mut err) };
        check(rc, &err)
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *const sys::chc_block_builder {
        &self.raw
    }
}

fn usage(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Usage, message)
}

fn checked_len(count: usize, width: usize, label: &str) -> Result<usize> {
    count
        .checked_mul(width)
        .ok_or_else(|| usage(format!("{label} length overflow: {count} * {width}")))
}

// Row metadata lengths must exactly match declared row count
fn require_len(label: &str, actual: usize, expected: usize) -> Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(usage(format!(
            "{label} length mismatch: got {actual}, expected {expected}"
        )))
    }
}

// C reads required prefix and ignores extra bytes
fn require_covers(label: &str, actual: usize, needed: usize) -> Result<()> {
    if actual >= needed {
        Ok(())
    } else {
        Err(usage(format!(
            "{label} too short: got {actual}, need at least {needed}"
        )))
    }
}

fn validate_offsets(offsets: &[u64], n_rows: usize, label: &str) -> Result<usize> {
    require_len(&format!("{label} offsets"), offsets.len(), n_rows)?;
    let mut previous = 0;
    for (row, &end) in offsets.iter().enumerate() {
        if end < previous {
            return Err(usage(format!(
                "{label} offsets not monotonic at row {row}: {end} < {previous}"
            )));
        }
        previous = end;
    }
    // On a 32-bit target an offset past usize cannot be covered by any slab,
    // so saturate and let the length check reject it
    Ok(usize::try_from(previous).unwrap_or(usize::MAX))
}

fn validate_string(offsets: &[u64], data_len: usize, n_rows: usize, label: &str) -> Result<()> {
    let final_offset = validate_offsets(offsets, n_rows, label)?;
    require_covers(&format!("{label} data"), data_len, final_offset)
}

fn validate_low_cardinality_keys(
    keys: &[u8],
    key_size: i32,
    n_rows: usize,
    dict_n: usize,
) -> Result<()> {
    let read: fn(&[u8]) -> u64 = match key_size {
        1 => |key| u64::from(key[0]),
        2 => |key| u64::from(u16::from_ne_bytes(key.try_into().expect("key width"))),
        4 => |key| u64::from(u32::from_ne_bytes(key.try_into().expect("key width"))),
        8 => |key| u64::from_ne_bytes(key.try_into().expect("key width")),
        _ => {
            return Err(usage(format!(
                "LowCardinality key size must be 1, 2, 4, or 8, got {key_size}"
            )));
        }
    };
    let key_size = key_size as usize;
    require_len(
        "LowCardinality keys",
        keys.len(),
        checked_len(n_rows, key_size, "LowCardinality keys")?,
    )?;
    for (row, key) in keys.chunks_exact(key_size).enumerate() {
        let value = read(key);
        if value >= dict_n as u64 {
            return Err(usage(format!(
                "LowCardinality key out of range at row {row}: {value} >= {dict_n}"
            )));
        }
    }
    Ok(())
}

// Raw pointers only provide read access to borrowed data
unsafe impl Send for ColumnBuilder<'_> {}
unsafe impl Sync for ColumnBuilder<'_> {}

unsafe impl Send for BlockBuilder<'_> {}
// Shared references only permit read operations, including asynchronous send
unsafe impl Sync for BlockBuilder<'_> {}

#[cfg(test)]
mod tests {
    use super::{BlockBuilder, ColumnBuilder};
    use crate::sys;
    use crate::{Allocator, ErrorKind, TypeAst};

    fn kind(result: crate::Result<ColumnBuilder<'_>>) -> ErrorKind {
        result.err().expect("invalid column accepted").kind
    }

    #[test]
    fn a_zero_width_element_is_a_usage_error() {
        assert_eq!(kind(ColumnBuilder::fixed(&[0; 4], 0, 1)), ErrorKind::Usage);
    }

    // A corrupt row count must not wrap into a short length that passes
    #[test]
    fn row_count_times_width_must_not_overflow() {
        assert_eq!(
            kind(ColumnBuilder::fixed(&[0; 4], usize::MAX, 2)),
            ErrorKind::Usage,
        );
    }

    #[test]
    fn a_tuple_needs_children_and_matching_scratch() {
        let mut ptrs: [*mut sys::chc_column; 1] = [core::ptr::null_mut()];
        assert_eq!(kind(ColumnBuilder::tuple(&[], &mut ptrs)), ErrorKind::Usage);

        let one = ColumnBuilder::fixed(&[0; 4], 4, 1).expect("fixed");
        let two = ColumnBuilder::fixed(&[0; 8], 4, 2).expect("fixed");
        assert_eq!(
            kind(ColumnBuilder::tuple(&[one, two], &mut ptrs)),
            ErrorKind::Usage,
        );
    }

    #[test]
    fn tuple_children_must_agree_on_row_count() {
        let one = ColumnBuilder::fixed(&[0; 4], 4, 1).expect("fixed");
        let two = ColumnBuilder::fixed(&[0; 8], 4, 2).expect("fixed");
        let mut ptrs: [*mut sys::chc_column; 2] = [core::ptr::null_mut(); 2];
        assert_eq!(
            kind(ColumnBuilder::tuple(&[one, two], &mut ptrs)),
            ErrorKind::Usage,
        );
    }

    #[test]
    fn low_cardinality_keys_must_use_a_wire_width() {
        let dict = ColumnBuilder::string(&[0], &[], 1).expect("dict");
        for bad in [0, 3, 16, -1] {
            assert_eq!(
                kind(dict.low_cardinality(bad, &[0; 8], 1)),
                ErrorKind::Usage,
                "key size {bad}",
            );
        }
    }

    #[test]
    fn every_key_width_indexes_the_dictionary() {
        let offsets = [1u64, 2];
        let dict = ColumnBuilder::string(&offsets, b"ab", 2).expect("dict");
        for (key_size, keys) in [
            (1, vec![1u8]),
            (2, 1u16.to_ne_bytes().to_vec()),
            (4, 1u32.to_ne_bytes().to_vec()),
            (8, 1u64.to_ne_bytes().to_vec()),
        ] {
            dict.low_cardinality(key_size, &keys, 1)
                .unwrap_or_else(|e| panic!("key size {key_size}: {e}"));
        }
    }

    #[test]
    fn a_key_past_the_dictionary_is_rejected() {
        let offsets = [1u64];
        let dict = ColumnBuilder::string(&offsets, b"a", 1).expect("dict");
        for (key_size, keys) in [
            (1, vec![9u8]),
            (2, 9u16.to_ne_bytes().to_vec()),
            (4, 9u32.to_ne_bytes().to_vec()),
            (8, 9u64.to_ne_bytes().to_vec()),
        ] {
            assert_eq!(
                kind(dict.low_cardinality(key_size, &keys, 1)),
                ErrorKind::Usage,
                "key size {key_size}",
            );
        }
    }

    #[test]
    fn key_slab_length_must_match_the_row_count() {
        let offsets = [1u64];
        let dict = ColumnBuilder::string(&offsets, b"a", 1).expect("dict");
        assert_eq!(kind(dict.low_cardinality(4, &[0; 4], 2)), ErrorKind::Usage,);
        assert_eq!(
            kind(dict.low_cardinality(4, &[0; 4], usize::MAX)),
            ErrorKind::Usage,
        );
    }

    #[test]
    fn offsets_must_not_move_backwards() {
        let offsets = [2u64, 1];
        assert_eq!(
            kind(ColumnBuilder::string(&offsets, b"ab", 2)),
            ErrorKind::Usage,
        );
        let values = ColumnBuilder::fixed(&[0; 8], 4, 2).expect("fixed");
        assert_eq!(kind(values.array(&offsets, 2)), ErrorKind::Usage);
    }

    // An offset past the slab would index outside borrowed data
    #[test]
    fn offsets_must_stay_inside_their_slab() {
        let offsets = [u64::MAX];
        assert_eq!(
            kind(ColumnBuilder::string(&offsets, b"a", 1)),
            ErrorKind::Usage,
        );
    }

    #[test]
    fn a_null_map_must_cover_every_row() {
        let values = ColumnBuilder::fixed(&[0; 8], 4, 2).expect("fixed");
        assert_eq!(kind(values.nullable(&[0])), ErrorKind::Usage);
    }

    #[test]
    fn array_values_must_match_the_final_offset() {
        let values = ColumnBuilder::fixed(&[0; 8], 4, 2).expect("fixed");
        assert_eq!(kind(values.array(&[3], 1)), ErrorKind::Usage);
    }

    #[test]
    fn a_default_block_holds_no_columns() {
        let alloc = Allocator::stdlib();
        let ty = TypeAst::parse("UInt32", alloc).expect("UInt32");
        let data = 1u32.to_le_bytes();
        let col = ColumnBuilder::fixed(&data, 4, 1).expect("fixed");
        let mut bb = BlockBuilder::default();
        bb.append("x", ty.view(), &col).expect("append");
        let raw = unsafe { &*bb.as_ptr() };
        assert_eq!(raw.n_cols, 1);
        assert_eq!(raw.n_rows, 1);
    }
}
