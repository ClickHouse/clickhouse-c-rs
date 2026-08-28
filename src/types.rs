//! ClickHouse type parsing and inspection.

use core::ffi::c_char;
use core::ptr::NonNull;
use core::slice;

use crate::alloc::Allocator;
use crate::error::{Result, check};
use crate::sys;

/// ClickHouse type kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum Kind {
    Void = sys::CHC_VOID,
    Int8 = sys::CHC_INT8,
    Int16 = sys::CHC_INT16,
    Int32 = sys::CHC_INT32,
    Int64 = sys::CHC_INT64,
    Int128 = sys::CHC_INT128,
    Int256 = sys::CHC_INT256,
    UInt8 = sys::CHC_UINT8,
    UInt16 = sys::CHC_UINT16,
    UInt32 = sys::CHC_UINT32,
    UInt64 = sys::CHC_UINT64,
    UInt128 = sys::CHC_UINT128,
    UInt256 = sys::CHC_UINT256,
    Float32 = sys::CHC_FLOAT32,
    Float64 = sys::CHC_FLOAT64,
    BFloat16 = sys::CHC_BFLOAT16,
    Bool = sys::CHC_BOOL,
    Date = sys::CHC_DATE,
    Date32 = sys::CHC_DATE32,
    DateTime = sys::CHC_DATETIME,
    DateTime64 = sys::CHC_DATETIME64,
    Time = sys::CHC_TIME,
    Time64 = sys::CHC_TIME64,
    String = sys::CHC_STRING,
    FixedString = sys::CHC_FIXED_STRING,
    Decimal32 = sys::CHC_DECIMAL32,
    Decimal64 = sys::CHC_DECIMAL64,
    Decimal128 = sys::CHC_DECIMAL128,
    Decimal256 = sys::CHC_DECIMAL256,
    Uuid = sys::CHC_UUID,
    Ipv4 = sys::CHC_IPV4,
    Ipv6 = sys::CHC_IPV6,
    Enum8 = sys::CHC_ENUM8,
    Enum16 = sys::CHC_ENUM16,
    Nullable = sys::CHC_NULLABLE,
    Array = sys::CHC_ARRAY,
    Tuple = sys::CHC_TUPLE,
    Map = sys::CHC_MAP,
    Nested = sys::CHC_NESTED,
    LowCardinality = sys::CHC_LOW_CARDINALITY,
    Interval = sys::CHC_INTERVAL,
    Point = sys::CHC_POINT,
    Ring = sys::CHC_RING,
    Polygon = sys::CHC_POLYGON,
    MultiPolygon = sys::CHC_MULTI_POLYGON,
    Variant = sys::CHC_VARIANT,
    Dynamic = sys::CHC_DYNAMIC,
    Json = sys::CHC_JSON,
    Object = sys::CHC_OBJECT,
    AggregateFunction = sys::CHC_AGGREGATE_FUNCTION,
    SimpleAggregateFunction = sys::CHC_SIMPLE_AGGREGATE_FUNCTION,
    Nothing = sys::CHC_NOTHING,
    QBit = sys::CHC_QBIT,
}

impl Kind {
    pub(crate) fn from_raw(k: sys::chc_kind) -> Option<Self> {
        // Return None for values added to C API but not represented here
        Some(match k {
            sys::CHC_VOID => Self::Void,
            sys::CHC_INT8 => Self::Int8,
            sys::CHC_INT16 => Self::Int16,
            sys::CHC_INT32 => Self::Int32,
            sys::CHC_INT64 => Self::Int64,
            sys::CHC_INT128 => Self::Int128,
            sys::CHC_INT256 => Self::Int256,
            sys::CHC_UINT8 => Self::UInt8,
            sys::CHC_UINT16 => Self::UInt16,
            sys::CHC_UINT32 => Self::UInt32,
            sys::CHC_UINT64 => Self::UInt64,
            sys::CHC_UINT128 => Self::UInt128,
            sys::CHC_UINT256 => Self::UInt256,
            sys::CHC_FLOAT32 => Self::Float32,
            sys::CHC_FLOAT64 => Self::Float64,
            sys::CHC_BFLOAT16 => Self::BFloat16,
            sys::CHC_BOOL => Self::Bool,
            sys::CHC_DATE => Self::Date,
            sys::CHC_DATE32 => Self::Date32,
            sys::CHC_DATETIME => Self::DateTime,
            sys::CHC_DATETIME64 => Self::DateTime64,
            sys::CHC_TIME => Self::Time,
            sys::CHC_TIME64 => Self::Time64,
            sys::CHC_STRING => Self::String,
            sys::CHC_FIXED_STRING => Self::FixedString,
            sys::CHC_DECIMAL32 => Self::Decimal32,
            sys::CHC_DECIMAL64 => Self::Decimal64,
            sys::CHC_DECIMAL128 => Self::Decimal128,
            sys::CHC_DECIMAL256 => Self::Decimal256,
            sys::CHC_UUID => Self::Uuid,
            sys::CHC_IPV4 => Self::Ipv4,
            sys::CHC_IPV6 => Self::Ipv6,
            sys::CHC_ENUM8 => Self::Enum8,
            sys::CHC_ENUM16 => Self::Enum16,
            sys::CHC_NULLABLE => Self::Nullable,
            sys::CHC_ARRAY => Self::Array,
            sys::CHC_TUPLE => Self::Tuple,
            sys::CHC_MAP => Self::Map,
            sys::CHC_NESTED => Self::Nested,
            sys::CHC_LOW_CARDINALITY => Self::LowCardinality,
            sys::CHC_INTERVAL => Self::Interval,
            sys::CHC_POINT => Self::Point,
            sys::CHC_RING => Self::Ring,
            sys::CHC_POLYGON => Self::Polygon,
            sys::CHC_MULTI_POLYGON => Self::MultiPolygon,
            sys::CHC_VARIANT => Self::Variant,
            sys::CHC_DYNAMIC => Self::Dynamic,
            sys::CHC_JSON => Self::Json,
            sys::CHC_OBJECT => Self::Object,
            sys::CHC_AGGREGATE_FUNCTION => Self::AggregateFunction,
            sys::CHC_SIMPLE_AGGREGATE_FUNCTION => Self::SimpleAggregateFunction,
            sys::CHC_NOTHING => Self::Nothing,
            sys::CHC_QBIT => Self::QBit,
            _ => return None,
        })
    }
}

/// Parsed ClickHouse type.
///
/// Value releases its memory with allocator passed to [`parse`](Self::parse).
pub struct TypeAst {
    raw: NonNull<sys::chc_type>,
    alloc: Allocator,
}

impl TypeAst {
    /// Parses a ClickHouse type name such as `"Array(Nullable(UInt32))"`.
    pub fn parse(name: &str, alloc: Allocator) -> Result<Self> {
        let mut out: *mut sys::chc_type = core::ptr::null_mut();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_type_parse(
                name.as_ptr().cast::<c_char>(),
                name.len(),
                alloc.as_ptr(),
                &mut out,
                &mut err,
            )
        };
        check(rc, &err)?;
        Ok(Self {
            raw: NonNull::new(out).expect("chc_type_parse returned OK with NULL"),
            alloc,
        })
    }

    /// Returns a borrowed view of parsed type.
    pub fn view(&self) -> TypeRef<'_> {
        TypeRef {
            raw: self.raw.as_ptr().cast_const(),
            _marker: core::marker::PhantomData,
        }
    }
}

impl Drop for TypeAst {
    fn drop(&mut self) {
        unsafe { sys::chc_type_destroy(self.raw.as_ptr(), self.alloc.as_ptr()) };
    }
}

unsafe impl Send for TypeAst {}

/// Borrowed view of a ClickHouse type.
#[derive(Clone, Copy)]
pub struct TypeRef<'a> {
    pub(crate) raw: *const sys::chc_type,
    pub(crate) _marker: core::marker::PhantomData<&'a sys::chc_type>,
}

impl<'a> TypeRef<'a> {
    pub fn kind(&self) -> Option<Kind> {
        Kind::from_raw(unsafe { sys::chc_type_kind(self.raw) })
    }

    /// Returns number of child types.
    ///
    /// Child types include array elements, tuple fields, map keys and values,
    /// and inner types of `Nullable` and `LowCardinality`.
    pub fn n_children(&self) -> usize {
        unsafe { sys::chc_type_n_children(self.raw) }
    }

    pub fn child(&self, i: usize) -> Option<TypeRef<'a>> {
        let c = unsafe { sys::chc_type_child(self.raw, i) };
        if c.is_null() {
            None
        } else {
            Some(TypeRef {
                raw: c,
                _marker: core::marker::PhantomData,
            })
        }
    }

    /// Returns `N` for `FixedString(N)`, or zero for other types.
    pub fn fixed_size(&self) -> i32 {
        unsafe { sys::chc_type_fixed_size(self.raw) }
    }

    /// Returns byte width in a [`Fixed`](crate::ColumnLayout::Fixed) column.
    /// Returns zero for types without fixed-width storage.
    pub fn elem_size(&self) -> usize {
        unsafe { sys::chc_type_elem_size(self.raw) }
    }

    /// Returns `P` for `Decimal(P, S)`, or zero for other types.
    pub fn decimal_precision(&self) -> i32 {
        unsafe { sys::chc_type_decimal_precision(self.raw) }
    }

    /// Returns `S` for `Decimal(P, S)`, or zero for other types.
    pub fn decimal_scale(&self) -> i32 {
        unsafe { sys::chc_type_decimal_scale(self.raw) }
    }

    /// Returns subsecond precision for `DateTime64(S)` or `Time64(S)`.
    /// Returns zero for other types.
    pub fn datetime64_scale(&self) -> i32 {
        unsafe { sys::chc_type_datetime64_scale(self.raw) }
    }

    /// Returns `N` for `QBit(T, N)`, or zero for other types.
    ///
    /// A QBit column uses [`ColumnLayout::Tuple`] with one fixed column per
    /// bit in `T`. Columns use most significant bit first, and each row holds
    /// `ceil(N / 8)` bytes.
    ///
    /// [`ColumnLayout::Tuple`]: crate::ColumnLayout::Tuple
    /// [`qbit_element_size`]: TypeRef::qbit_element_size
    pub fn qbit_dimension(&self) -> usize {
        unsafe { sys::chc_type_qbit_dimension(self.raw) }
    }

    /// Returns bit width of `T` in `QBit(T, N)`, or zero for other types.
    /// Supported widths are 16, 32, and 64.
    pub fn qbit_element_size(&self) -> usize {
        unsafe { sys::chc_type_qbit_element_size(self.raw) }
    }

    /// Returns timezone bytes without UTF-8 validation.
    pub fn timezone(&self) -> Option<&'a [u8]> {
        let mut len = 0;
        let p = unsafe { sys::chc_type_timezone(self.raw, &mut len) };
        if p.is_null() {
            None
        } else {
            // SAFETY: C allocation remains valid for lifetime of borrowed type
            Some(unsafe { slice::from_raw_parts(p.cast::<u8>(), len) })
        }
    }

    /// Returns type name bytes without UTF-8 validation.
    pub fn name(&self) -> Option<&'a [u8]> {
        let mut len = 0;
        let p = unsafe { sys::chc_type_name(self.raw, &mut len) };
        if p.is_null() {
            None
        } else {
            Some(unsafe { slice::from_raw_parts(p.cast::<u8>(), len) })
        }
    }

    /// Returns number of entries for `Enum8` or `Enum16`.
    /// Returns zero for other types.
    pub fn enum_count(&self) -> usize {
        unsafe { sys::chc_type_enum_count(self.raw) }
    }

    /// Returns name and value for enum entry at `i`.
    /// Name bytes are not validated as UTF-8.
    pub fn enum_at(&self, i: usize) -> Option<(&'a [u8], i64)> {
        if i >= self.enum_count() {
            return None;
        }
        let mut name_ptr: *const c_char = core::ptr::null();
        let mut name_len: usize = 0;
        let mut value: i64 = 0;
        unsafe {
            sys::chc_type_enum_at(self.raw, i, &mut name_ptr, &mut name_len, &mut value);
        }
        if name_ptr.is_null() {
            return None;
        }
        Some((
            unsafe { slice::from_raw_parts(name_ptr.cast::<u8>(), name_len) },
            value,
        ))
    }

    /// Returns tuple field name bytes without UTF-8 validation.
    pub fn tuple_field_name(&self, i: usize) -> Option<&'a [u8]> {
        let mut len = 0;
        let p = unsafe { sys::chc_type_tuple_field_name(self.raw, i, &mut len) };
        if p.is_null() {
            None
        } else {
            Some(unsafe { slice::from_raw_parts(p.cast::<u8>(), len) })
        }
    }

    /// Formats type as a ClickHouse type name.
    ///
    /// Returns an empty string if required buffer length exceeds `usize`.
    pub fn format(&self) -> String {
        let needed = unsafe { sys::chc_type_format(self.raw, core::ptr::null_mut(), 0) };
        if needed == 0 {
            return String::new();
        }
        let Some(cap) = needed.checked_add(1) else {
            return String::new();
        };
        let mut buf = vec![0u8; cap];
        let _ =
            unsafe { sys::chc_type_format(self.raw, buf.as_mut_ptr().cast::<c_char>(), buf.len()) };
        buf.truncate(needed);
        String::from_utf8_lossy(&buf).into_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::{Kind, TypeAst};
    use crate::Allocator;

    fn parse(name: &str) -> TypeAst {
        TypeAst::parse(name, Allocator::stdlib()).expect(name)
    }

    #[test]
    fn qbit_metadata() {
        let ty = parse("QBit(Float32, 16)");
        let view = ty.view();
        assert_eq!(view.kind(), Some(Kind::QBit));
        assert_eq!(view.qbit_dimension(), 16);
        assert_eq!(view.qbit_element_size(), 32);
        assert_eq!(view.format(), "QBit(Float32, 16)");
        assert_eq!(view.child(0).and_then(|c| c.kind()), Some(Kind::Float32));
    }

    #[test]
    fn qbit_accessors_are_zero_off_qbit() {
        let ty = parse("Array(UInt32)");
        assert_eq!(ty.view().qbit_dimension(), 0);
        assert_eq!(ty.view().qbit_element_size(), 0);
    }

    // Unknown C discriminants must not convert to adjacent Rust variants
    #[test]
    fn unknown_discriminant_is_none() {
        assert_eq!(Kind::from_raw(i32::MAX), None);
        assert_eq!(Kind::from_raw(-1), None);
    }
}
