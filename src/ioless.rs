//! Transport-independent native protocol client.
//!
//! [`IolessClient`] processes protocol bytes without reading or writing a
//! transport. Callers submit received bytes and consume bytes queued for
//! sending. Operations return [`Step::NeedsInput`] when more data is required.

use core::pin::Pin;
use core::ptr::NonNull;
use core::slice;
use std::ffi::c_char;

use crate::alloc::Allocator;
use crate::builder::BlockBuilder;
use crate::client::{ClientOpts, Event, ServerInfo};
use crate::codec::Codec;
use crate::error::{Result, check};
use crate::sys;

/// Result of a protocol operation that may require more input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step<T> {
    Ready(T),
    /// Operation requires more bytes. Submit them with
    /// [`IolessClient::submit`] and repeat operation.
    NeedsInput,
}

impl<T> Step<T> {
    pub fn ready(self) -> Option<T> {
        match self {
            Self::Ready(v) => Some(v),
            Self::NeedsInput => None,
        }
    }

    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }
}

/// Native protocol client without transport I/O.
///
/// ```no_run
/// use clickhouse_c::{Allocator, ClientOpts, Event, IolessClient, Step};
/// use std::io::{Read, Write};
/// use std::net::TcpStream;
///
/// # fn main() -> clickhouse_c::Result<()> {
/// let mut sock = TcpStream::connect("localhost:9000")?;
/// let mut core = IolessClient::new(&ClientOpts::new(), Allocator::stdlib(), None)?;
/// let mut buf = [0u8; 8192];
///
/// // Send queued bytes before waiting for more input.
/// let mut pump = |core: &mut IolessClient, sock: &mut TcpStream| -> clickhouse_c::Result<()> {
///     while !core.pending_out().is_empty() {
///         let n = sock.write(core.pending_out())?;
///         core.consume_out(n);
///     }
///     let n = sock.read(&mut buf)?;
///     core.submit(&buf[..n])
/// };
///
/// while !core.handshake()?.is_ready() {
///     pump(&mut core, &mut sock)?;
/// }
///
/// core.send_query("SELECT 1", None)?;
/// loop {
///     match core.recv_event()? {
///         Step::Ready(Event::EndOfStream) => break,
///         Step::Ready(_) => {}
///         Step::NeedsInput => pump(&mut core, &mut sock)?,
///     }
/// }
/// # Ok(())
/// # }
/// ```
pub struct IolessClient {
    raw: NonNull<sys::chc_async_client>,
    // C client retains allocator address until destruction
    alloc: Box<Allocator>,
    _codec: Option<Pin<Box<Codec>>>,
}

impl IolessClient {
    /// Creates protocol client. Call [`handshake`](Self::handshake) before
    /// sending queries.
    pub fn new(
        opts: &ClientOpts,
        alloc: Allocator,
        codec: Option<Pin<Box<Codec>>>,
    ) -> Result<Self> {
        opts.validate_codec(codec.as_ref().map(|codec| codec.as_ref()))?;
        let codec_ptr = codec.as_ref().map(|c| c.as_ref().as_ptr());
        let raw_opts = opts.to_raw(codec_ptr)?;
        let alloc = Box::new(alloc);
        let mut out: *mut sys::chc_async_client = core::ptr::null_mut();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_async_client_init(&mut out, raw_opts.as_ptr(), alloc.as_ptr(), &mut err)
        };
        check(rc, &err)?;
        Ok(Self {
            raw: NonNull::new(out).expect("chc_async_client_init returned OK with NULL"),
            alloc,
            _codec: codec,
        })
    }

    /// Copies received transport bytes into protocol input buffer.
    pub fn submit(&mut self, bytes: &[u8]) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_async_submit(
                self.raw.as_ptr(),
                bytes.as_ptr().cast(),
                bytes.len(),
                &mut err,
            )
        };
        check(rc, &err)
    }

    /// Returns bytes waiting to be written to transport.
    ///
    /// Send methods append to this buffer without backpressure. Callers should
    /// limit queued operations according to their memory requirements.
    pub fn pending_out(&self) -> &[u8] {
        let mut ptr: *const u8 = core::ptr::null();
        let mut len = 0usize;
        unsafe { sys::chc_async_pending_out(self.raw.as_ptr(), &mut ptr, &mut len) };
        if ptr.is_null() || len == 0 {
            return &[];
        }
        // SAFETY: C buffer remains unchanged until next mutable operation
        unsafe { slice::from_raw_parts(ptr, len) }
    }

    /// Removes first `n` bytes after transport accepts them.
    ///
    /// Values larger than queued length remove all queued bytes.
    pub fn consume_out(&mut self, n: usize) {
        unsafe { sys::chc_async_consume_out(self.raw.as_ptr(), n) };
    }

    /// Advances Hello exchange. Repeat after sending output and submitting
    /// input until method returns [`Step::Ready`].
    pub fn handshake(&mut self) -> Result<Step<()>> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_async_handshake(self.raw.as_ptr(), &mut err) };
        step(rc, &err).map(|s| s.map_ready(|()| ()))
    }

    /// Queues a query.
    ///
    /// I/O-independent C API does not support query settings or parameters.
    pub fn send_query(&mut self, sql: &str, query_id: Option<&str>) -> Result<()> {
        let (qid, qid_len) = query_id
            .map(|q| (q.as_ptr().cast::<c_char>(), q.len()))
            .unwrap_or((core::ptr::null(), 0));
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_async_send_query(
                self.raw.as_ptr(),
                sql.as_ptr().cast::<c_char>(),
                sql.len(),
                qid,
                qid_len,
                &mut err,
            )
        };
        check(rc, &err)
    }

    /// Queues a Data block, or empty terminator when `builder` is `None`.
    pub fn send_data(&mut self, builder: Option<&BlockBuilder<'_>>) -> Result<()> {
        let bb_ptr = builder.map(|b| b.as_ptr()).unwrap_or(core::ptr::null());
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_async_send_data(self.raw.as_ptr(), bb_ptr, &mut err) };
        check(rc, &err)
    }

    /// Queues empty Data block that ends INSERT input.
    pub fn send_data_end(&mut self) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_async_send_data_end(self.raw.as_ptr(), &mut err) };
        check(rc, &err)
    }

    /// Decodes next server event from submitted bytes.
    ///
    /// Returned event owns block or exception payload.
    pub fn recv_event(&mut self) -> Result<Step<Event>> {
        let mut raw = sys::chc_packet::zeroed();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_async_recv_packet(self.raw.as_ptr(), &mut raw, &mut err) };
        if rc == sys::CHC_WOULD_BLOCK {
            return Ok(Step::NeedsInput);
        }
        if let Err(e) = check(rc, &err) {
            unsafe { sys::chc_async_packet_clear(self.raw.as_ptr(), &mut raw) };
            return Err(e);
        }
        let event = Event::from_raw(&mut raw, *self.alloc);
        unsafe { sys::chc_async_packet_clear(self.raw.as_ptr(), &mut raw) };
        event.map(Step::Ready)
    }

    /// Returns server identity information.
    ///
    /// Before handshake completes, value contains empty name and requested
    /// revision. Read value after [`handshake`](Self::handshake) returns
    /// [`Step::Ready`] to obtain server-provided information.
    pub fn server_info(&self) -> Option<ServerInfo> {
        let p = unsafe { sys::chc_async_server_info(self.raw.as_ptr().cast_const()) };
        if p.is_null() {
            None
        } else {
            Some(ServerInfo::from_raw(unsafe { &*p }))
        }
    }
}

impl<T> Step<T> {
    fn map_ready<U>(self, f: impl FnOnce(T) -> U) -> Step<U> {
        match self {
            Self::Ready(v) => Step::Ready(f(v)),
            Self::NeedsInput => Step::NeedsInput,
        }
    }
}

fn step(rc: i32, err: &sys::chc_err) -> Result<Step<()>> {
    if rc == sys::CHC_WOULD_BLOCK {
        Ok(Step::NeedsInput)
    } else {
        check(rc, err).map(Step::Ready)
    }
}

impl Drop for IolessClient {
    fn drop(&mut self) {
        unsafe { sys::chc_async_client_free(self.raw.as_ptr()) };
    }
}

// C client is uniquely owned and used from one thread at a time
unsafe impl Send for IolessClient {}
