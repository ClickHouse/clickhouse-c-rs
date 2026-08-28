//! Rustls transport support.
//!
//! [`TlsIo`] implements blocking [`Io`] over a rustls connection.
//! [`default_config`] and [`config_with_roots`] create client configurations
//! for blocking and asynchronous clients. Module re-exports `rustls` for
//! custom root stores and client authentication.

use core::ffi::{c_int, c_void};
use core::marker::PhantomPinned;
use core::pin::Pin;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

pub use rustls;

use crate::error::{Error, ErrorKind, Result};
use crate::io::Io;
use crate::sys;

/// Creates client configuration using Mozilla webpki roots without client
/// authentication.
///
/// Use [`config_with_roots`] or rustls APIs for private certificate
/// authorities or mutual TLS.
pub fn default_config() -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    config_with_roots(roots)
}

/// Creates client configuration using provided roots without client
/// authentication.
pub fn config_with_roots(roots: rustls::RootCertStore) -> Arc<rustls::ClientConfig> {
    // Select provider independently of process-wide rustls configuration
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("aws-lc-rs supports the safe default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(config)
}

type RustlsStream = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

/// Blocking TLS [`Io`] implementation over an owned `TcpStream`.
///
/// Value is pinned because C callback table contains a pointer to its stream.
pub struct TlsIo {
    io: sys::chc_io,
    stream: RustlsStream,
    _pin: PhantomPinned,
}

impl TlsIo {
    /// Creates TLS transport over connected TCP socket.
    ///
    /// Peer is verified using `config` and `server_name`. Server name is also
    /// sent as SNI. Method completes TLS handshake before returning.
    pub fn connect(
        tcp: TcpStream,
        server_name: &str,
        config: Arc<rustls::ClientConfig>,
    ) -> Result<Pin<Box<Self>>> {
        let name =
            rustls::pki_types::ServerName::try_from(server_name.to_owned()).map_err(|_| {
                Error::new(
                    ErrorKind::Usage,
                    format!("invalid TLS server name: {server_name}"),
                )
            })?;
        let conn = rustls::ClientConnection::new(config, name)
            .map_err(|e| Error::new(ErrorKind::Io, format!("rustls client: {e}")))?;
        let mut stream = rustls::StreamOwned::new(conn, tcp);
        stream
            .conn
            .complete_io(&mut stream.sock)
            .map_err(|e| Error::new(ErrorKind::Io, format!("TLS handshake: {e}")))?;

        let mut boxed = Box::pin(Self {
            io: sys::chc_io {
                ud: core::ptr::null_mut(),
                read: Some(tls_read),
                write: Some(tls_write),
                check_cancel: None,
            },
            stream,
            _pin: PhantomPinned,
        });
        // Set callback context after address becomes stable
        unsafe {
            let this = boxed.as_mut().get_unchecked_mut();
            this.io.ud = (this as *mut Self).cast();
        }
        Ok(boxed)
    }
}

// SAFETY: callback table and context remain valid within pinned TlsIo
unsafe impl Io for TlsIo {
    fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io {
        // SAFETY: returning field address does not move pinned value
        unsafe { &mut self.get_unchecked_mut().io as *mut sys::chc_io }
    }

    fn set_read_timeout(self: Pin<&mut Self>, timeout: Option<core::time::Duration>) -> Result<()> {
        unsafe { self.get_unchecked_mut() }
            .stream
            .sock
            .set_read_timeout(timeout)?;
        Ok(())
    }
}

// C client uses pinned callback context from one thread at a time
unsafe impl Send for TlsIo {}

unsafe extern "C" fn tls_read(
    ud: *mut c_void,
    buf: *mut c_void,
    len: usize,
    out_n: *mut usize,
    err: *mut sys::chc_err,
) -> c_int {
    if len == 0 {
        unsafe { *out_n = 0 };
        return sys::CHC_OK;
    }
    let io = unsafe { &mut *(ud as *mut TlsIo) };
    let dst = unsafe { core::slice::from_raw_parts_mut(buf as *mut u8, len) };
    match io.stream.read(dst) {
        // C reader converts zero count to EOF when more bytes are required
        Ok(n) => {
            unsafe { *out_n = n };
            sys::CHC_OK
        }
        Err(e) => unsafe { set_err(err, sys::CHC_ERR_IO, &format!("tls read: {e}")) },
    }
}

unsafe extern "C" fn tls_write(
    ud: *mut c_void,
    buf: *const c_void,
    len: usize,
    err: *mut sys::chc_err,
) -> c_int {
    let io = unsafe { &mut *(ud as *mut TlsIo) };
    let src = unsafe { core::slice::from_raw_parts(buf as *const u8, len) };
    // Callback must write and flush complete input or fail
    match io.stream.write_all(src).and_then(|()| io.stream.flush()) {
        Ok(()) => sys::CHC_OK,
        Err(e) => unsafe { set_err(err, sys::CHC_ERR_IO, &format!("tls write: {e}")) },
    }
}

/// Copies null-terminated message into C error and returns `code`.
unsafe fn set_err(err: *mut sys::chc_err, code: c_int, msg: &str) -> c_int {
    if !err.is_null() {
        let e = unsafe { &mut *err };
        e.server_code = 0;
        let cap = e.msg.len();
        if cap > 0 {
            let n = msg.len().min(cap - 1);
            for (slot, b) in e.msg.iter_mut().zip(msg.as_bytes()[..n].iter()) {
                *slot = *b as core::ffi::c_char;
            }
            e.msg[n] = 0;
        }
    }
    code
}
