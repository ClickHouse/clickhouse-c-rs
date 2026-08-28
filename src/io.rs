//! Blocking I/O interfaces used by clickhouse-c.
//!
//! [`Io`] provides access to a C callback table. [`PosixIo`] implements this
//! interface for Unix file descriptors, including sockets and pipes. I/O
//! values are pinned because C callback state can contain pointers to fields
//! within each value.

use core::ffi::c_void;
use core::marker::{PhantomData, PhantomPinned};
use core::pin::Pin;
use core::sync::atomic::{AtomicBool, Ordering};
use core::time::Duration;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};
use std::sync::Arc;

use crate::error::{Error, ErrorKind, Result};
use crate::sys;

/// Byte transport used by clickhouse-c.
///
/// [`Client`](crate::Client), [`BlockReader`](crate::BlockReader), and
/// [`BlockBuilder::write`](crate::BlockBuilder::write) use this interface.
///
/// Crate provides [`PosixIo`] and, with `tls` feature, `tls::TlsIo`. Custom
/// implementations can support other transports.
///
/// # Safety
///
/// [`io_ptr`](Self::io_ptr) must return a non-null pointer to initialized
/// `chc_io`. Its `read`, `write`, and optional `check_cancel` callbacks must
/// follow
/// clickhouse-c vtable contract:
///
/// * `read` stores at most `len` bytes, writes count to `out_n`, and returns
///   `CHC_OK`. A zero count indicates EOF.
/// * `write` writes all `len` bytes or returns an error.
/// * Both callbacks write `err` and return a `CHC_ERR_*` code on failure.
///
/// Returned pointer and referenced state must remain valid at fixed addresses
/// while `self` is pinned. Callbacks can run on any thread that uses transport,
/// but clickhouse-c does not call them concurrently.
pub unsafe trait Io {
    /// Returns pointer to callback table valid while `self` remains pinned.
    fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io;

    /// Sets read timeout for transport.
    ///
    /// Implementations using absolute deadlines may require a new call before
    /// each operation.
    fn set_read_timeout(self: Pin<&mut Self>, _timeout: Option<Duration>) -> Result<()> {
        Err(Error::new(
            ErrorKind::Usage,
            "I/O backend does not support read timeouts",
        ))
    }
}

/// Cooperative read cancellation for [`PosixIo`].
///
/// Pass one clone to [`PosixIo::new_cancellable`] and retain another for
/// [`cancel`](Self::cancel). clickhouse-c checks token before each read. It
/// does not interrupt a read already blocked in operating system. Use a read
/// timeout to limit that wait. Later reads return
/// [`ErrorKind::Cancelled`](crate::ErrorKind::Cancelled).
///
/// Token only stops local reads. Use
/// [`Client::send_cancel`](crate::Client::send_cancel) to request server-side
/// cancellation.
#[derive(Clone, Debug, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    /// Creates a token in active state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Cancels future reads for every [`PosixIo`] using a clone of this token.
    /// Cancellation cannot be reset.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Relaxed);
    }

    /// Returns whether any clone has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

/// Reads cancellation state for C callback.
unsafe extern "C" fn check_cancel_flag(ud: *mut c_void) -> bool {
    // SAFETY: PosixIo retains Arc containing this AtomicBool
    unsafe { &*ud.cast::<AtomicBool>() }.load(Ordering::Relaxed)
}

/// Blocking [`Io`] implementation for a Unix file descriptor.
pub struct PosixIo<'fd> {
    state: sys::chc_posix_io,
    io: sys::chc_io,
    /// Retains owned descriptor until C client has been closed
    #[allow(dead_code)]
    owned: Option<OwnedFd>,
    /// Retains cancellation state referenced by C callback
    #[allow(dead_code)]
    cancel: Option<CancelToken>,
    _fd: PhantomData<BorrowedFd<'fd>>,
    // io.ud points to state within this pinned value
    _pin: PhantomPinned,
}

impl<'fd> PosixIo<'fd> {
    /// Creates transport for a borrowed file descriptor.
    ///
    /// Descriptor must remain open for lifetime `'fd`.
    pub fn new(fd: BorrowedFd<'fd>) -> Pin<Box<Self>> {
        Self::build(fd.as_raw_fd(), None, None)
    }

    /// Creates cancellable transport for a borrowed file descriptor.
    pub fn new_cancellable(fd: BorrowedFd<'fd>, cancel: CancelToken) -> Pin<Box<Self>> {
        Self::build(fd.as_raw_fd(), None, Some(cancel))
    }

    fn build(fd: RawFd, owned: Option<OwnedFd>, cancel: Option<CancelToken>) -> Pin<Box<Self>> {
        let mut boxed = Box::pin(Self {
            // Replaced by chc_posix_io_init after pinning
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
        // Initialize callback table after address becomes stable
        unsafe {
            let this = boxed.as_mut().get_unchecked_mut();
            // Point to stable Arc allocation rather than movable wrapper
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

    /// Sets absolute deadline for subsequent blocking reads.
    ///
    /// `None` removes deadline. Timeout is calculated when this method is
    /// called and shared by later reads. Call again before each operation that
    /// requires a fresh timeout. Zero causes next read to time out immediately.
    pub fn set_read_timeout(self: Pin<&mut Self>, timeout: Option<Duration>) {
        let deadline_us = match timeout {
            None => 0,
            Some(d) => {
                let now = unsafe { sys::chc_rs_monotonic_us() };
                let add = i64::try_from(d.as_micros()).unwrap_or(i64::MAX);
                // Zero represents disabled deadline in C API
                now.saturating_add(add).max(1)
            }
        };
        // SAFETY: setter does not move pinned value
        unsafe { sys::chc_posix_io_set_deadline(&mut self.get_unchecked_mut().state, deadline_us) };
    }
}

impl PosixIo<'static> {
    /// Creates transport that owns and closes file descriptor.
    pub fn new_owned<F: Into<OwnedFd>>(fd: F) -> Pin<Box<Self>> {
        let fd = fd.into();
        let raw = fd.as_fd().as_raw_fd();
        Self::build(raw, Some(fd), None)
    }

    /// Creates cancellable transport that owns and closes file descriptor.
    pub fn new_owned_cancellable<F: Into<OwnedFd>>(fd: F, cancel: CancelToken) -> Pin<Box<Self>> {
        let fd = fd.into();
        let raw = fd.as_fd().as_raw_fd();
        Self::build(raw, Some(fd), Some(cancel))
    }
}

// SAFETY: initialized callback table and referenced state remain pinned together
unsafe impl<'fd> Io for PosixIo<'fd> {
    fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io {
        // SAFETY: returning field address does not move pinned value
        unsafe { &mut self.get_unchecked_mut().io as *mut sys::chc_io }
    }

    fn set_read_timeout(self: Pin<&mut Self>, timeout: Option<Duration>) -> Result<()> {
        Self::set_read_timeout(self, timeout);
        Ok(())
    }
}

// C client uses transport from one thread at a time, file descriptors support transfer
unsafe impl<'fd> Send for PosixIo<'fd> {}
