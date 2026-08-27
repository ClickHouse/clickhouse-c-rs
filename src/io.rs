//! I/O glue. The C library reads & writes through `chc_io`, a small
//! read/write/cancel vtable it owns.
//!
//! [`PosixIo`] wraps a raw fd via clickhouse-c's posix-io backend. It holds
//! the `chc_posix_io` state and the `chc_io` vtable it feeds as inline
//! fields and lets upstream `chc_posix_io_init` populate both — pointing
//! the vtable's `ud` back at the state. That back-pointer makes the node
//! self-referential, so it must not move while the
//! [`Client`](crate::Client) holds the vtable pointer: hence
//! [`PhantomPinned`] and the `Pin<Box<Self>>` the constructors return,
//! mirroring [`TlsIo`](crate::tls::TlsIo). The rest of the crate
//! ([`Client`](crate::Client), [`BlockReader`](crate::BlockReader),
//! [`BlockBuilder::write`](crate::BlockBuilder::write)) expresses the
//! borrow as `Pin<&mut PosixIo>`.
//!
//! Covers TCP sockets (production) and pipes (the `clickhouse local` test
//! path) without further plumbing.

use core::ffi::c_void;
use core::marker::{PhantomData, PhantomPinned};
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::sync::Arc;

use crate::error::{Error, ErrorKind, Result};
use crate::sys;

/// Byte transport clickhouse-c reads and writes through.
///
/// Hands out the `chc_io` vtable pointer C retains for the duration of the
/// operation; the implementor keeps that vtable at a fixed address, hence the
/// `Pin<&mut Self>` receiver. Used by [`Client`](crate::Client),
/// [`BlockReader`](crate::BlockReader), and
/// [`BlockBuilder::write`](crate::BlockBuilder::write) alike, so a backend
/// written once serves every path.
///
/// Implemented by [`PosixIo`] (plaintext fd) and, under feature `tls`, by
/// `tls::TlsIo` (rustls over a `TcpStream`). A custom backend -- OpenSSL via
/// `clickhouse-openssl.h`, an in-memory buffer, a caller's own event loop --
/// may implement it too, hence `unsafe`: the crate passes `io_ptr`'s return
/// straight to C without validating it.
///
/// # Safety
///
/// `io_ptr` must return a non-null pointer to a fully initialized `chc_io`
/// whose `read` / `write` (and `check_cancel`, if set) callbacks honor the
/// clickhouse-c vtable contract:
///
/// * `read` fills up to `len` bytes, stores the count in `out_n`, and
///   returns `CHC_OK`. `out_n == 0` means clean EOF.
/// * `write` writes all `len` bytes or fails.
/// * Both report failure by filling `*err` and returning a `CHC_ERR_*` code.
///
/// That pointer, and any state it back-references, must stay valid and fixed
/// in place for as long as the pinned `self` lives -- through the whole
/// lifetime of whatever retains it, which for a
/// [`Client`](crate::Client) is the entire connection. C dereferences it and
/// calls through it on whatever thread drives the operation, never
/// concurrently from two.
pub unsafe trait Io {
    /// Pointer to the `chc_io` vtable, valid while `self` is pinned alive.
    fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io;

    /// Set backend read timeout. Refresh before each operation when backend
    /// uses an absolute deadline.
    fn set_read_timeout(self: Pin<&mut Self>, _timeout: Option<Duration>) -> Result<()> {
        Err(Error::new(
            ErrorKind::Usage,
            "I/O backend does not support read timeouts",
        ))
    }
}

/// Cooperative cancellation flag for a [`PosixIo`].
///
/// Cloneable and shareable across threads: hand one clone to
/// [`PosixIo::new_cancellable`] and keep another to [`cancel`] with.
///
/// # What it does and does not interrupt
///
/// clickhouse-c checks the flag *before* each transport read, not during
/// one, so a read already parked in `read(2)` runs to completion. Pair the
/// token with [`PosixIo::set_read_timeout`] to bound that wait, and the
/// cancel is observed on the next attempt. Once set, reads fail with
/// [`ErrorKind::Cancelled`](crate::ErrorKind::Cancelled).
///
/// This is local: it stops *this* side reading. It sends nothing, so the
/// server keeps producing. To ask the server to stop, send the protocol
/// Cancel packet with [`Client::send_cancel`](crate::Client::send_cancel)
/// and drain to `EndOfStream`.
///
/// [`cancel`]: CancelToken::cancel
#[derive(Clone, Debug, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fail every later read on the [`PosixIo`] holding a clone of this
    /// token. One-way: there is no reset, because a half-consumed block
    /// leaves the stream unparseable anyway.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

/// Reads the flag `cancel_ud` points at. clickhouse-c calls this from
/// whatever thread drives the read.
unsafe extern "C" fn check_cancel_flag(ud: *mut c_void) -> bool {
    // SAFETY: ud is the AtomicBool inside the Arc the PosixIo keeps alive.
    unsafe { &*ud.cast::<AtomicBool>() }.load(Ordering::Relaxed)
}

pub struct PosixIo<'fd> {
    state: sys::chc_posix_io,
    io: sys::chc_io,
    /// `Some` when [`PosixIo::new_owned`] handed us the fd; dropping it
    /// closes the fd, after the owning [`Client`](crate::Client) has closed
    /// the `chc_client` that reads through it. `None` for the borrowed
    /// path: caller keeps the fd open for the duration of the `'fd`
    /// lifetime.
    #[allow(dead_code)]
    owned: Option<OwnedFd>,
    /// Keeps the flag `state.cancel_ud` points at alive for as long as C can
    /// call through it.
    #[allow(dead_code)]
    cancel: Option<CancelToken>,
    _fd: PhantomData<BorrowedFd<'fd>>,
    // io.ud back-points at `state`; the node must not move once wired.
    _pin: PhantomPinned,
}

impl<'fd> PosixIo<'fd> {
    /// Wrap a borrowed file descriptor. The caller keeps ownership and
    /// must keep it open for the duration of `'fd`. Closing the fd while
    /// the [`PosixIo`] still references it is a use-after-free at the
    /// kernel level (subsequent reads land in whatever the fd table
    /// reassigned the number to).
    pub fn new(fd: BorrowedFd<'fd>) -> Pin<Box<Self>> {
        Self::build(fd.as_raw_fd(), None, None)
    }

    /// Wrap a borrowed fd whose reads a [`CancelToken`] can abort. See the
    /// token's docs for what it interrupts.
    pub fn new_cancellable(fd: BorrowedFd<'fd>, cancel: CancelToken) -> Pin<Box<Self>> {
        Self::build(fd.as_raw_fd(), None, Some(cancel))
    }

    fn build(fd: RawFd, owned: Option<OwnedFd>, cancel: Option<CancelToken>) -> Pin<Box<Self>> {
        let mut boxed = Box::pin(Self {
            // Overwritten wholesale by chc_posix_io_init below; pre-filled
            // only to satisfy Rust's all-fields-init rule.
            state: sys::chc_posix_io {
                fd,
                check_cancel: None,
                cancel_ud: core::ptr::null_mut(),
                deadline_us: 0,
            },
            io: sys::chc_io {
                ud: core::ptr::null_mut(),
                read: None,
                write: None,
                check_cancel: None,
            },
            owned,
            cancel,
            _fd: PhantomData,
            _pin: PhantomPinned,
        });
        // Populates `state` + the `io` vtable with the posix read/write
        // callbacks and wires io.ud -> state at the pinned address.
        // SAFETY: only writes fields; never moves out of the pin.
        unsafe {
            let this = boxed.as_mut().get_unchecked_mut();
            // Point at the AtomicBool inside the Arc this node now owns, not
            // at the CancelToken wrapper, which moves with the struct.
            let (check, ud) = match &this.cancel {
                Some(token) => (
                    Some(check_cancel_flag as unsafe extern "C" fn(*mut c_void) -> bool),
                    Arc::as_ptr(&token.0).cast_mut().cast::<c_void>(),
                ),
                None => (None, core::ptr::null_mut()),
            };
            sys::chc_posix_io_init(&mut this.state, &mut this.io, fd, check, ud);
        }
        boxed
    }

    /// Bound subsequent blocking reads by an absolute `now + timeout`
    /// deadline; `None` clears it so reads block indefinitely (default).
    ///
    /// The deadline is absolute and shared by every later read, not a
    /// rolling per-read budget: refresh it before each operation that
    /// needs a fresh window. Once elapsed, reads fail with
    /// [`ErrorKind::Io`](crate::ErrorKind::Io) ("read timeout"); a
    /// `Some(ZERO)` timeout makes the next read time out immediately.
    pub fn set_read_timeout(self: Pin<&mut Self>, timeout: Option<Duration>) {
        let deadline_us = match timeout {
            None => 0,
            Some(d) => {
                let now = unsafe { sys::chc_rs_monotonic_us() };
                let add = i64::try_from(d.as_micros()).unwrap_or(i64::MAX);
                // Keep nonzero so a near-zero deadline never reads as "disabled".
                now.saturating_add(add).max(1)
            }
        };
        // SAFETY: writes the deadline field; does not move self.
        unsafe { sys::chc_posix_io_set_deadline(&mut self.get_unchecked_mut().state, deadline_us) };
    }
}

impl PosixIo<'static> {
    /// Take ownership of the fd. The fd is closed when the [`PosixIo`]
    /// drops — typically through the owning [`Client`](crate::Client),
    /// which keeps the `PosixIo` alive for its own lifetime.
    pub fn new_owned<F: Into<OwnedFd>>(fd: F) -> Pin<Box<Self>> {
        let fd = fd.into();
        let raw = fd.as_fd().as_raw_fd();
        Self::build(raw, Some(fd), None)
    }

    /// Take ownership of the fd and make its reads cancellable.
    pub fn new_owned_cancellable<F: Into<OwnedFd>>(fd: F, cancel: CancelToken) -> Pin<Box<Self>> {
        let fd = fd.into();
        let raw = fd.as_fd().as_raw_fd();
        Self::build(raw, Some(fd), Some(cancel))
    }
}

// SAFETY: `io` is a fully wired chc_io embedded in the pinned PosixIo, fed
// clickhouse-c's posix read/write callbacks by chc_posix_io_init; its `ud`
// back-points at the inline `state`, which stays at a fixed address behind
// the pinned Box for as long as the retaining Client lives.
unsafe impl<'fd> Io for PosixIo<'fd> {
    fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io {
        // Address of the inline vtable the caller retains. SAFETY: returns a
        // field pointer; does not move self.
        unsafe { &mut self.get_unchecked_mut().io as *mut sys::chc_io }
    }

    fn set_read_timeout(self: Pin<&mut Self>, timeout: Option<Duration>) -> Result<()> {
        Self::set_read_timeout(self, timeout);
        Ok(())
    }
}

// `state`/`io` are POD with no destructor; `owned` (if any) closes the fd
// when the node drops, after the owning Client has closed `chc_client`. No
// explicit Drop needed.

// chc_posix_io stores a non-thread-local fd; the kernel guarantees the
// safety of cross-thread fd use itself. The io.ud raw pointer (into this
// node) otherwise makes PosixIo !Send; it is dereferenced only from the C
// client's single-threaded read/write calls on whatever thread owns the
// Client, and stays valid behind the pinned Box.
unsafe impl<'fd> Send for PosixIo<'fd> {}
