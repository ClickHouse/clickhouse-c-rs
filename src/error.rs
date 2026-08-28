//! Error types returned by clickhouse-c-rs.

use core::ffi::c_int;

use crate::sys;

/// Category corresponding to a clickhouse-c `CHC_ERR_*` code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Transport operation failed or read deadline expired.
    Io,
    /// Stream ended before required data was available.
    Eof,
    /// Protocol data was invalid or unsupported.
    Protocol,
    /// Type name was invalid or column did not match its declared type.
    Type,
    /// Allocator returned a null pointer.
    Oom,
    /// [`CancelToken`](crate::CancelToken) was set before a read.
    Cancelled,
    /// Server returned an exception. See [`Error::server_code`] and
    /// [`Error::server_name`] for details.
    Server,
    /// API input was invalid.
    Usage,
    /// I/O-independent client requires more input. Blocking client does not
    /// return this category.
    WouldBlock,
    /// Error code is not represented by another variant.
    Other(c_int),
}

impl ErrorKind {
    pub(crate) fn from_code(code: c_int) -> Self {
        match code {
            sys::CHC_ERR_IO => Self::Io,
            sys::CHC_ERR_EOF => Self::Eof,
            sys::CHC_ERR_PROTOCOL => Self::Protocol,
            sys::CHC_ERR_TYPE => Self::Type,
            sys::CHC_ERR_OOM => Self::Oom,
            sys::CHC_ERR_CANCELLED => Self::Cancelled,
            sys::CHC_ERR_SERVER => Self::Server,
            sys::CHC_ERR_USAGE => Self::Usage,
            sys::CHC_WOULD_BLOCK => Self::WouldBlock,
            other => Self::Other(other),
        }
    }
}

/// Error returned by this crate.
#[derive(Debug, Clone, thiserror::Error)]
#[error("clickhouse-c: {kind:?}: {message}")]
pub struct Error {
    pub kind: ErrorKind,
    /// ClickHouse error code, or zero when `kind` is not [`ErrorKind::Server`].
    pub server_code: i32,
    /// Error message copied from clickhouse-c.
    pub message: String,
    /// Server exception name, empty when `kind` is not [`ErrorKind::Server`].
    pub server_name: String,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            server_code: 0,
            message: message.into(),
            server_name: String::new(),
        }
    }

    pub(crate) fn from_raw(code: c_int, e: &sys::chc_err) -> Self {
        Self {
            kind: ErrorKind::from_code(code),
            server_code: e.server_code,
            message: cstr_array_to_string(&e.msg),
            server_name: cstr_array_to_string(&e.server_name),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::new(ErrorKind::Io, e.to_string())
    }
}

/// Result type used by this crate.
pub type Result<T> = core::result::Result<T, Error>;

fn cstr_array_to_string(buf: &[core::ffi::c_char]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let bytes: &[u8] = unsafe { core::slice::from_raw_parts(buf.as_ptr().cast::<u8>(), end) };
    String::from_utf8_lossy(bytes).into_owned()
}

#[inline]
pub(crate) fn check(rc: c_int, err: &sys::chc_err) -> Result<()> {
    if rc == sys::CHC_OK {
        Ok(())
    } else {
        Err(Error::from_raw(rc, err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err_with(msg: &str) -> sys::chc_err {
        let mut e = sys::chc_err::zeroed();
        for (slot, b) in e.msg.iter_mut().zip(msg.bytes()) {
            *slot = b as core::ffi::c_char;
        }
        e
    }

    // Compressed receive can leave stale text in `err` after successful return
    #[test]
    fn ok_rc_ignores_stale_err() {
        let stale = err_with("ioless buffer drained");
        assert!(check(sys::CHC_OK, &stale).is_ok());
    }

    #[test]
    fn error_rc_surfaces_kind() {
        let e = err_with("bad handshake");
        let err = check(sys::CHC_ERR_PROTOCOL, &e).unwrap_err();
        assert_eq!(err.kind, ErrorKind::Protocol);
        assert_eq!(err.message, "bad handshake");
    }
}
