//! Compression codecs for native protocol frames.
//!
//! Built-in LZ4 and Zstandard codecs require corresponding crate features.
//! [`Codec::empty`] and [`Codec::from_raw`] support custom implementations.

use core::pin::Pin;

use crate::sys;

/// Compression algorithm used for native protocol frames.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum Compression {
    /// Disables compression and does not require a [`Codec`].
    #[default]
    None = sys::CHC_COMP_NONE,
    /// Uses LZ4 and requires corresponding callbacks, such as [`Codec::lz4`].
    Lz4 = sys::CHC_COMP_LZ4,
    /// Uses Zstandard and requires corresponding callbacks, such as
    /// [`Codec::zstd`].
    Zstd = sys::CHC_COMP_ZSTD,
}

/// Compression callbacks used by clickhouse-c.
///
/// Value is pinned because C code retains address of callback table.
pub struct Codec {
    raw: sys::chc_codec,
    _pin: core::marker::PhantomPinned,
}

impl Codec {
    /// Creates a codec without compression callbacks.
    ///
    /// Codec only supports [`Compression::None`] until required callbacks are
    /// installed through [`raw_mut`]. [`Client::init`](crate::Client::init)
    /// rejects missing callbacks.
    ///
    /// [`raw_mut`]: Codec::raw_mut
    pub fn empty() -> Pin<Box<Self>> {
        Box::pin(Self {
            raw: sys::chc_codec {
                ud: core::ptr::null_mut(),
                lz4_compress: None,
                lz4_decompress: None,
                zstd_compress: None,
                zstd_decompress: None,
                lz4_bound: None,
                zstd_bound: None,
            },
            _pin: core::marker::PhantomPinned,
        })
    }

    /// Creates a codec from a raw callback table.
    ///
    /// # Safety
    ///
    /// Each function pointer must match corresponding field signature. All
    /// callbacks required by selected [`Compression`] must be present. User
    /// data referenced by `ud` must remain valid while codec exists and from
    /// every thread that uses codec.
    pub unsafe fn from_raw(raw: sys::chc_codec) -> Pin<Box<Self>> {
        Box::pin(Self {
            raw,
            _pin: core::marker::PhantomPinned,
        })
    }

    /// Creates built-in LZ4 codec backed by system liblz4.
    #[cfg(feature = "lz4")]
    pub fn lz4() -> Pin<Box<Self>> {
        let mut b = Self::empty();
        unsafe {
            let this = b.as_mut().get_unchecked_mut();
            sys::chc_lz4_codec_init(&mut this.raw);
        }
        b
    }

    /// Creates built-in Zstandard codec backed by system libzstd.
    #[cfg(feature = "zstd")]
    pub fn zstd() -> Pin<Box<Self>> {
        let mut b = Self::empty();
        unsafe {
            let this = b.as_mut().get_unchecked_mut();
            sys::chc_zstd_codec_init(&mut this.raw);
        }
        b
    }

    /// Returns mutable access to raw callback table.
    ///
    /// # Safety
    ///
    /// C library calls installed function pointers without validation.
    ///
    /// * Each function pointer must match corresponding field signature.
    /// * All callbacks required by selected [`Compression`] must be present.
    ///   For example, LZ4 requires `lz4_compress`, `lz4_decompress`, and
    ///   `lz4_bound`.
    /// * Data referenced by `ud` must remain valid while codec exists and from
    ///   every thread that uses codec.
    pub unsafe fn raw_mut(self: Pin<&mut Self>) -> &mut sys::chc_codec {
        unsafe { &mut self.get_unchecked_mut().raw }
    }

    #[inline]
    pub(crate) fn as_ptr(self: Pin<&Self>) -> *const sys::chc_codec {
        &self.raw
    }

    /// Returns whether all callbacks required by `compression` are present.
    pub(crate) fn supports(self: Pin<&Self>, compression: Compression) -> bool {
        match compression {
            Compression::None => true,
            Compression::Lz4 => {
                self.raw.lz4_compress.is_some()
                    && self.raw.lz4_decompress.is_some()
                    && self.raw.lz4_bound.is_some()
            }
            Compression::Zstd => {
                self.raw.zstd_compress.is_some()
                    && self.raw.zstd_decompress.is_some()
                    && self.raw.zstd_bound.is_some()
            }
        }
    }
}

unsafe impl Send for Codec {}

/// Calculates CityHash128 and returns low and high words in wire order.
pub fn cityhash128(data: &[u8]) -> (u64, u64) {
    let mut lo = 0u64;
    let mut hi = 0u64;
    unsafe {
        sys::chc_cityhash128(data.as_ptr().cast(), data.len(), &mut lo, &mut hi);
    }
    (lo, hi)
}

#[cfg(test)]
mod tests {
    use core::ffi::{c_int, c_void};

    use super::{Codec, Compression};
    use crate::sys;

    #[test]
    fn empty_codec_supports_only_uncompressed() {
        let codec = Codec::empty();
        assert!(codec.as_ref().supports(Compression::None));
        assert!(!codec.as_ref().supports(Compression::Lz4));
        assert!(!codec.as_ref().supports(Compression::Zstd));
    }

    // Frame allocation requires bound callback before compression
    #[test]
    fn missing_bound_callback_is_not_support() {
        let mut codec = Codec::empty();
        unsafe {
            let raw = codec.as_mut().raw_mut();
            raw.lz4_compress = Some(stub_compress);
            raw.lz4_decompress = Some(stub_decompress);
        }
        assert!(!codec.as_ref().supports(Compression::Lz4));

        unsafe { codec.as_mut().raw_mut().lz4_bound = Some(stub_bound) };
        assert!(codec.as_ref().supports(Compression::Lz4));
    }

    // Test only checks whether callback is present
    unsafe extern "C" fn stub_compress(
        _ud: *mut c_void,
        _src: *const c_void,
        _src_len: usize,
        _dst: *mut c_void,
        _dst_cap: usize,
        _dst_n: *mut usize,
        _err: *mut sys::chc_err,
    ) -> c_int {
        sys::CHC_OK
    }

    unsafe extern "C" fn stub_decompress(
        _ud: *mut c_void,
        _src: *const c_void,
        _src_len: usize,
        _dst: *mut c_void,
        _original_size: usize,
        _err: *mut sys::chc_err,
    ) -> c_int {
        sys::CHC_OK
    }

    unsafe extern "C" fn stub_bound(src_len: usize) -> usize {
        src_len
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn built_in_lz4_fills_every_slot() {
        assert!(Codec::lz4().as_ref().supports(Compression::Lz4));
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn built_in_zstd_fills_every_slot() {
        assert!(Codec::zstd().as_ref().supports(Compression::Zstd));
    }
}
