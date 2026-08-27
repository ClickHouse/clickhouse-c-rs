//! Compression codec handles.
//!
//! [`Codec::lz4`] and [`Codec::zstd`] populate a [`Codec`] from
//! clickhouse-compression.h's adapters, each behind its own feature.
//! [`Codec::empty`] plus [`Codec::raw_mut`], or [`Codec::from_raw`], build one
//! from caller-supplied callbacks and stay available with no features at
//! all -- linking a compression library is a choice, not a prerequisite for
//! having a codec.

use core::pin::Pin;

use crate::sys;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(i32)]
pub enum Compression {
    #[default]
    None = sys::CHC_COMP_NONE,
    Lz4 = sys::CHC_COMP_LZ4,
    Zstd = sys::CHC_COMP_ZSTD,
}

/// Owns a `chc_codec`. Constructed via the codec-specific factory
/// (`Codec::lz4()`, `Codec::zstd()`) or by hand-filling [`raw_mut`].
///
/// The struct is pinned because compression code calls back into the
/// function-pointer table by address.
pub struct Codec {
    raw: sys::chc_codec,
    _pin: core::marker::PhantomPinned,
}

impl Codec {
    /// A codec with no callbacks installed.
    ///
    /// Only usable with [`Compression::None`] until [`raw_mut`] fills the
    /// slots a codec needs; [`Client::init`](crate::Client::init) rejects the
    /// mismatch rather than reaching a null call.
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

    /// Take a fully filled `chc_codec`.
    ///
    /// # Safety
    ///
    /// Same contract as [`raw_mut`](Codec::raw_mut): every installed function
    /// pointer must match its field's signature, the slots required by the
    /// [`Compression`] this codec is paired with must all be set, and any
    /// `ud` must outlive the [`Codec`] and be dereferenceable from every
    /// thread the codec is used from.
    pub unsafe fn from_raw(raw: sys::chc_codec) -> Pin<Box<Self>> {
        Box::pin(Self {
            raw,
            _pin: core::marker::PhantomPinned,
        })
    }

    #[cfg(feature = "lz4")]
    pub fn lz4() -> Pin<Box<Self>> {
        let mut b = Self::empty();
        unsafe {
            let this = b.as_mut().get_unchecked_mut();
            sys::chc_lz4_codec_init(&mut this.raw);
        }
        b
    }

    #[cfg(feature = "zstd")]
    pub fn zstd() -> Pin<Box<Self>> {
        let mut b = Self::empty();
        unsafe {
            let this = b.as_mut().get_unchecked_mut();
            sys::chc_zstd_codec_init(&mut this.raw);
        }
        b
    }

    /// Borrow the underlying `chc_codec` for manual fills (e.g. wiring a
    /// custom allocator-bound compression implementation).
    ///
    /// # Safety
    ///
    /// Caller installs raw function pointers the C library will invoke
    /// without further checks. To stay sound:
    ///
    /// * Each installed function pointer must match the exact signature
    ///   of the corresponding `chc_codec` field.
    /// * For any [`Compression`] the codec will be paired with at the
    ///   [`Client`](crate::Client), the relevant fields must be set —
    ///   e.g. `Compression::Lz4` needs `lz4_compress`, `lz4_decompress`,
    ///   `lz4_bound`. Leaving a required slot `None` reaches a null
    ///   call.
    /// * Any `ud` pointer stored on the codec must outlive the
    ///   [`Codec`] and remain dereferenceable from every thread the
    ///   codec is used from.
    pub unsafe fn raw_mut(self: Pin<&mut Self>) -> &mut sys::chc_codec {
        unsafe { &mut self.get_unchecked_mut().raw }
    }

    #[inline]
    pub(crate) fn as_ptr(self: Pin<&Self>) -> *const sys::chc_codec {
        &self.raw
    }

    /// Whether every slot `compression` reaches is filled. The bound
    /// callback sizes the compressed frame before compressing, so leaving it
    /// out is as fatal as leaving out the compressor.
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

/// City Hash 128 helper. Returns `(lo, hi)` matching the on-wire
/// frame-checksum layout.
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

    // A compressor with no bound callback would reach a null call while
    // sizing the frame, so it must not count as support.
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

    // Never called: `supports` only inspects which slots are filled.
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
