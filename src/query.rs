//! Per-query settings, parameters, and identity.
//!
//! [`QueryOpts`] is the safe form of `chc_query_opts`. Names and values are
//! `&str` and get NUL-terminated copies made at send time; an interior NUL
//! is rejected as [`ErrorKind::Usage`] rather than silently truncating the
//! string the server sees.

use core::ffi::c_char;
use std::ffi::CString;

use crate::error::{Error, ErrorKind, Result};
use crate::sys;

/// One entry in the query's `SETTINGS` list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuerySetting<'a> {
    pub name: &'a str,
    pub value: &'a str,
    /// Ask the server to reject the query instead of ignoring a setting it
    /// does not recognize.
    pub important: bool,
    /// A user-defined `custom_*` setting rather than a built-in one.
    pub custom: bool,
}

impl<'a> QuerySetting<'a> {
    pub const fn new(name: &'a str, value: &'a str) -> Self {
        Self {
            name,
            value,
            important: false,
            custom: false,
        }
    }

    pub const fn important(mut self) -> Self {
        self.important = true;
        self
    }

    pub const fn custom(mut self) -> Self {
        self.custom = true;
        self
    }
}

impl QuerySetting<'static> {
    /// `output_format_native_encode_types_in_binary_format = 0`.
    ///
    /// The block decoder reads printable type names off the wire. ClickHouse
    /// writes them as text by default, but pin the setting on every query so
    /// a server or session profile that flips it to binary cannot break
    /// decoding.
    pub const TEXT_TYPE_NAMES: Self =
        Self::new("output_format_native_encode_types_in_binary_format", "0");
}

/// One `{name:Type}` placeholder substitution.
///
/// `value` must be a **single-quoted** literal whatever the placeholder's
/// declared type: the server reads the parameter with `readQuoted` and then
/// parses the unquoted text as `Type`. So `{n:UInt8}` takes `'42'`, not `42`,
/// and a bare `42` is a server-side parse error. Escape an embedded quote or
/// backslash the usual way, and write NULL as `'\\N'`.
///
/// clickhouse-c's header comment describes the older `Field::restoreFromDump`
/// behaviour and is out of date on this point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryParam<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

impl<'a> QueryParam<'a> {
    pub const fn new(name: &'a str, value: &'a str) -> Self {
        Self { name, value }
    }
}

/// Everything [`Client::send_query_with`](crate::Client::send_query_with)
/// attaches to a query beyond the SQL text.
#[derive(Clone, Copy, Debug, Default)]
pub struct QueryOpts<'a> {
    /// Server-side query id. Empty means the server assigns one.
    pub query_id: Option<&'a str>,
    pub settings: &'a [QuerySetting<'a>],
    pub params: &'a [QueryParam<'a>],
}

impl<'a> QueryOpts<'a> {
    pub const fn new() -> Self {
        Self {
            query_id: None,
            settings: &[],
            params: &[],
        }
    }

    pub const fn query_id(mut self, id: &'a str) -> Self {
        self.query_id = Some(id);
        self
    }

    pub const fn settings(mut self, settings: &'a [QuerySetting<'a>]) -> Self {
        self.settings = settings;
        self
    }

    pub const fn params(mut self, params: &'a [QueryParam<'a>]) -> Self {
        self.params = params;
        self
    }
}

/// NUL-terminated copies of a [`QueryOpts`]'s strings plus the C arrays
/// pointing at them, kept together so the pointers stay valid for exactly as
/// long as the `chc_query_opts` that borrows them.
pub(crate) struct RawQueryOpts {
    // Each CString owns its own heap buffer, so the pointers handed to C stay
    // valid however this struct moves.
    _owned: Vec<CString>,
    settings: Vec<sys::chc_query_setting>,
    params: Vec<sys::chc_query_param>,
    raw: sys::chc_query_opts,
}

impl RawQueryOpts {
    pub(crate) fn new(opts: &QueryOpts<'_>) -> Result<Self> {
        let mut owned = Vec::with_capacity(2 * (opts.settings.len() + opts.params.len()));
        for s in opts.settings {
            owned.push(cstring("query setting name", s.name)?);
            owned.push(cstring("query setting value", s.value)?);
        }
        for p in opts.params {
            owned.push(cstring("query parameter name", p.name)?);
            owned.push(cstring("query parameter value", p.value)?);
        }

        let mut next = owned.iter().map(|c| c.as_ptr());
        let settings: Vec<_> = opts
            .settings
            .iter()
            .map(|s| sys::chc_query_setting {
                name: next.next().expect("one name per setting"),
                value: next.next().expect("one value per setting"),
                important: s.important,
                custom: s.custom,
            })
            .collect();
        let params: Vec<_> = opts
            .params
            .iter()
            .map(|_| sys::chc_query_param {
                name: next.next().expect("one name per param"),
                value: next.next().expect("one value per param"),
            })
            .collect();

        let (query_id, query_id_len) = match opts.query_id {
            // chc_query_opts takes query_id as pointer + length, so no NUL
            // terminator is needed and any byte is legal.
            Some(id) => (id.as_ptr().cast::<c_char>(), id.len()),
            None => (core::ptr::null(), 0),
        };
        let mut this = Self {
            _owned: owned,
            settings,
            params,
            raw: sys::chc_query_opts {
                query_id,
                query_id_len,
                settings: core::ptr::null(),
                n_settings: 0,
                params: core::ptr::null(),
                n_params: 0,
            },
        };
        // Vec buffers live on the heap, so their addresses survive the move
        // out of this function; only `raw` itself must not move afterwards,
        // which the &self borrow in as_ptr enforces.
        this.raw.settings = this.settings.as_ptr();
        this.raw.n_settings = this.settings.len();
        this.raw.params = this.params.as_ptr();
        this.raw.n_params = this.params.len();
        Ok(this)
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *const sys::chc_query_opts {
        &self.raw
    }
}

/// NUL-terminate `s`, rejecting an interior NUL. C would stop at the first
/// one, so the server would silently see a different string.
pub(crate) fn cstring(label: &str, s: &str) -> Result<CString> {
    CString::new(s).map_err(|e| {
        Error::new(
            ErrorKind::Usage,
            format!("{label} has an interior NUL at byte {}", e.nul_position()),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interior_nul_is_a_usage_error() {
        let settings = [QuerySetting::new("max_block_size", "8\u{0}192")];
        let Err(err) = RawQueryOpts::new(&QueryOpts::new().settings(&settings)) else {
            panic!("interior NUL accepted");
        };
        assert_eq!(err.kind, ErrorKind::Usage);
        assert!(err.message.contains("query setting value"), "{err}");
    }

    #[test]
    fn empty_opts_pass_null_arrays() {
        let raw = RawQueryOpts::new(&QueryOpts::new()).expect("empty");
        let c = unsafe { &*raw.as_ptr() };
        assert_eq!(c.n_settings, 0);
        assert_eq!(c.n_params, 0);
        assert!(c.query_id.is_null());
    }

    #[test]
    fn strings_reach_c_nul_terminated_and_in_order() {
        let settings = [
            QuerySetting::TEXT_TYPE_NAMES,
            QuerySetting::new("max_block_size", "1024").important(),
        ];
        let params = [QueryParam::new("n", "42")];
        let opts = QueryOpts::new()
            .query_id("q-1")
            .settings(&settings)
            .params(&params);
        let raw = RawQueryOpts::new(&opts).expect("build");
        let c = unsafe { &*raw.as_ptr() };

        assert_eq!(c.n_settings, 2);
        assert_eq!(c.n_params, 1);
        assert_eq!(c.query_id_len, 3);

        let cstr = |p| unsafe { core::ffi::CStr::from_ptr(p) }.to_str().expect("utf8");
        let first = unsafe { &*c.settings };
        assert_eq!(
            cstr(first.name),
            "output_format_native_encode_types_in_binary_format"
        );
        assert_eq!(cstr(first.value), "0");
        assert!(!first.important);

        let second = unsafe { &*c.settings.add(1) };
        assert_eq!(cstr(second.name), "max_block_size");
        assert_eq!(cstr(second.value), "1024");
        assert!(second.important);

        let param = unsafe { &*c.params };
        assert_eq!(cstr(param.name), "n");
        assert_eq!(cstr(param.value), "42");
    }
}
