//! Error type wrapping clickhouse-c's `chc_err`.

use core::ffi::c_int;

use crate::sys;

/// Safe mirror of clickhouse-c's `CHC_ERR_*` codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Transport failure, or a read deadline elapsed.
    Io,
    /// Stream ended where more bytes were required.
    Eof,
    /// Wire bytes did not parse: bad framing, an unknown packet, a length
    /// that does not fit its buffer.
    Protocol,
    /// A type name did not parse, or a column did not match its type.
    Type,
    /// The allocator returned null.
    Oom,
    /// A [`CancelToken`](crate::CancelToken) was set before this read.
    Cancelled,
    /// The server sent an exception; `server_code` and `server_name` carry
    /// its identity.
    Server,
    /// The caller broke an API contract: mismatched row counts, a codec that
    /// cannot do the selected compression, an interior NUL in a C string.
    Usage,
    /// The ioless client needs more submitted bytes to make progress. Never
    /// produced by the blocking client.
    WouldBlock,
    /// A code this crate does not map, meaning the vendored headers grew one
    /// without a matching variant here.
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

/// Anything this crate can fail with.
#[derive(Debug, Clone, thiserror::Error)]
#[error("clickhouse-c: {kind:?}: {message}")]
pub struct Error {
    pub kind: ErrorKind,
    /// ClickHouse error code when `kind` is [`ErrorKind::Server`], else 0.
    pub server_code: i32,
    /// Copied out of the C `chc_err` buffer, which does not outlive the call.
    pub message: String,
    /// Server-side exception name when `kind` is [`ErrorKind::Server`].
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

/// `Result` with this crate's [`Error`].
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

    // The compressed-recv frame pump leaves a populated `err` message behind
    // on an otherwise-CHC_OK return; rc is authoritative, so that must stay
    // Ok. Regression guard for the bootstrap-drain "WouldBlock: ioless buffer
    // drained" leak.
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
