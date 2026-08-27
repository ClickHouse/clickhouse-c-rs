//! Runtime-neutral protocol machine.
//!
//! [`IolessClient`] speaks the whole native TCP protocol and performs no I/O
//! at all. The caller hands it the bytes that arrived and takes the bytes it
//! wants sent, so the transport can be anything: `async-std`, `smol`,
//! `io_uring`, an embedded event loop, a `Vec<u8>` in a test.
//!
//! Every step that could run past the submitted bytes returns
//! [`Step::NeedsInput`] rather than blocking, and resumes mid-block once more
//! bytes arrive.
//!
//! The [`AsyncClient`](crate::AsyncClient) tokio adapter is this type plus a
//! byte pump; anything it does, a caller can do against another runtime.

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

/// A step that either finished or ran out of input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step<T> {
    Ready(T),
    /// The parser reached the end of the submitted bytes mid-item. Read more
    /// from the transport, hand them to [`IolessClient::submit`], and call the
    /// same method again.
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

/// The native protocol, with the socket left to the caller.
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
/// // Push everything queued, then read once. Flushing before the read is not
/// // optional: a step that reports NeedsInput has usually just queued the
/// // bytes the server is waiting on, and reading first deadlocks both sides.
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
    // The C side stores this address and calls through it until free, so it is
    // boxed rather than held inline where a move of `Self` would relocate it.
    alloc: Box<Allocator>,
    _codec: Option<Pin<Box<Codec>>>,
}

impl IolessClient {
    /// Build the machine. Does no I/O, so it cannot fail on the network; run
    /// [`handshake`](Self::handshake) next.
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

    /// Hand over bytes read from the transport. Copied into the parser's
    /// staging buffer, so `bytes` can be reused immediately.
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

    /// Bytes queued for the transport, empty when there is nothing to send.
    ///
    /// Sends never block or apply backpressure, so watch this length and stop
    /// issuing sends when it grows past what the application is willing to
    /// buffer.
    pub fn pending_out(&self) -> &[u8] {
        let mut ptr: *const u8 = core::ptr::null();
        let mut len = 0usize;
        unsafe { sys::chc_async_pending_out(self.raw.as_ptr(), &mut ptr, &mut len) };
        if ptr.is_null() || len == 0 {
            return &[];
        }
        // SAFETY: C owns the buffer and keeps it alive and unchanged until the
        // next call that mutates the machine, which needs &mut self.
        unsafe { slice::from_raw_parts(ptr, len) }
    }

    /// Drop the first `n` bytes of [`pending_out`](Self::pending_out) after
    /// the transport accepted them. A partial write is fine. `n` past what is
    /// queued is clamped.
    pub fn consume_out(&mut self, n: usize) {
        unsafe { sys::chc_async_consume_out(self.raw.as_ptr(), n) };
    }

    /// Advance the Hello exchange. Call until it reports
    /// [`Step::Ready`](Step::Ready), pumping bytes both ways in between.
    pub fn handshake(&mut self) -> Result<Step<()>> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_async_handshake(self.raw.as_ptr(), &mut err) };
        step(rc, &err).map(|s| s.map_ready(|()| ()))
    }

    /// Queue a query. Settings and parameters are not reachable here:
    /// clickhouse-c publishes no `chc_async_send_query_ex`.
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

    /// Queue a Data block, or the empty terminator with [`None`].
    pub fn send_data(&mut self, builder: Option<&BlockBuilder<'_>>) -> Result<()> {
        let bb_ptr = builder.map(|b| b.as_ptr()).unwrap_or(core::ptr::null());
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_async_send_data(self.raw.as_ptr(), bb_ptr, &mut err) };
        check(rc, &err)
    }

    /// Close an INSERT's data stream.
    pub fn send_data_end(&mut self) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_async_send_data_end(self.raw.as_ptr(), &mut err) };
        check(rc, &err)
    }

    /// Take the next server event out of the submitted bytes. Any block or
    /// exception payload is owned by the returned [`Event`].
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

    /// Identity the server sent.
    ///
    /// Unlike [`Client::server_info`](crate::Client::server_info), this
    /// returns `Some` before the handshake too: clickhouse-c hands back its
    /// own slot, whose name is empty and whose revision is seeded with the
    /// requested one so block framing is defined from the first byte. Read it
    /// after [`handshake`](Self::handshake) reports
    /// [`Step::Ready`](Step::Ready).
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

// The raw pointer is to a heap machine this handle uniquely owns; C only ever
// touches it from the thread calling in, and every mutating method takes
// &mut self.
unsafe impl Send for IolessClient {}
