//! TCP client. Wraps `chc_client_*` from `clickhouse-client.h`.
//!
//! The caller owns the socket (or any other transport) and supplies it
//! via [`PosixIo`]; this crate does not do TCP, DNS, or TLS itself.

use core::ffi::c_char;
use core::pin::Pin;
use core::ptr::NonNull;
use core::slice;
use core::time::Duration;
use std::ffi::CString;

use crate::alloc::Allocator;
use crate::block::Block;
use crate::builder::BlockBuilder;
use crate::codec::{Codec, Compression};
use crate::error::{Error, ErrorKind, Result, check};
use crate::io::Io;
use crate::query::{QueryOpts, RawQueryOpts, cstring};
use crate::sys;

/// Connection settings, sent in the Hello handshake.
///
/// The string fields are copied and NUL-terminated when the client
/// connects; an interior NUL is reported as
/// [`ErrorKind::Usage`](crate::ErrorKind::Usage) instead of silently
/// truncating what the server sees.
#[derive(Clone, Debug, Default)]
pub struct ClientOpts {
    client_name: Option<String>,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
    /// Client version reported to the server. Cosmetic: it shows up in
    /// `system.query_log`. Defaults to 0.0.0.
    pub client_version_major: u64,
    pub client_version_minor: u64,
    pub client_version_patch: u64,
    /// Native protocol revision to negotiate. 0 keeps clickhouse-c's own
    /// default, [`sys::CHC_CLIENT_DEFAULT_REVISION`]. The server caps the
    /// negotiated revision at its own, so asking for a newer one is safe;
    /// asking for an older one turns off the features it gates.
    pub client_revision: u64,
    pub compression: Compression,
    /// Internal read buffer. 0 selects clickhouse-c's 8 KiB default.
    pub read_buffer_bytes: usize,
}

impl ClientOpts {
    /// Defaults throughout: user `default`, database `default`, empty
    /// password, no compression.
    pub fn new() -> Self {
        Self::default()
    }

    /// Name reported to the server, default `"clickhouse-c"`.
    pub fn client_name(mut self, s: &str) -> Self {
        self.client_name = Some(s.to_owned());
        self
    }
    /// Default database for unqualified table names.
    pub fn database(mut self, s: &str) -> Self {
        self.database = Some(s.to_owned());
        self
    }
    /// Account to authenticate as.
    pub fn user(mut self, s: &str) -> Self {
        self.user = Some(s.to_owned());
        self
    }
    /// Sent in cleartext inside the Hello body: use TLS on an untrusted
    /// network.
    pub fn password(mut self, s: &str) -> Self {
        self.password = Some(s.to_owned());
        self
    }

    /// Set all three version components at once.
    pub fn client_version(mut self, major: u64, minor: u64, patch: u64) -> Self {
        self.client_version_major = major;
        self.client_version_minor = minor;
        self.client_version_patch = patch;
        self
    }

    /// Protocol revision to ask for; see [`client_revision`](Self::client_revision).
    pub fn client_revision(mut self, revision: u64) -> Self {
        self.client_revision = revision;
        self
    }

    /// Anything but [`Compression::None`] also needs a matching [`Codec`]
    /// passed to [`Client::init`].
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    pub(crate) fn to_raw(&self, codec: Option<*const sys::chc_codec>) -> Result<RawClientOpts> {
        let mut owned = Vec::with_capacity(4);
        let mut field = |label, value: &Option<String>| -> Result<*const c_char> {
            let Some(value) = value else {
                return Ok(core::ptr::null());
            };
            owned.push(cstring(label, value)?);
            Ok(owned.last().expect("just pushed").as_ptr())
        };
        let client_name = field("client name", &self.client_name)?;
        let database = field("database", &self.database)?;
        let user = field("user", &self.user)?;
        let password = field("password", &self.password)?;

        Ok(RawClientOpts {
            _owned: owned,
            raw: sys::chc_client_opts {
                client_name,
                client_version_major: self.client_version_major,
                client_version_minor: self.client_version_minor,
                client_version_patch: self.client_version_patch,
                client_revision: self.client_revision,
                database,
                user,
                password,
                compression: self.compression as i32,
                codec: codec.unwrap_or(core::ptr::null()),
                read_buffer_bytes: self.read_buffer_bytes,
            },
        })
    }

    pub(crate) fn validate_codec(&self, codec: Option<Pin<&Codec>>) -> Result<()> {
        if self.compression == Compression::None {
            return Ok(());
        }
        let codec = codec.ok_or_else(|| {
            Error::new(
                ErrorKind::Usage,
                format!("{:?} compression requires a codec", self.compression),
            )
        })?;
        if codec.supports(self.compression) {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::Usage,
                format!("codec does not support {:?} compression", self.compression),
            ))
        }
    }
}

/// `chc_client_opts` plus the NUL-terminated copies its pointers reference.
pub(crate) struct RawClientOpts {
    _owned: Vec<CString>,
    raw: sys::chc_client_opts,
}

impl RawClientOpts {
    #[inline]
    pub(crate) fn as_ptr(&self) -> *const sys::chc_client_opts {
        &self.raw
    }
}

/// Information sent by the server during the Hello handshake.
#[derive(Debug, Clone)]
pub struct ServerInfo {
    pub name: String,
    pub timezone: String,
    pub display_name: String,
    pub version_major: u64,
    pub version_minor: u64,
    pub version_patch: u64,
    pub revision: u64,
}

impl ServerInfo {
    pub(crate) fn from_raw(raw: &sys::chc_server_info) -> Self {
        Self {
            name: cstr_array_to_string(&raw.name),
            timezone: cstr_array_to_string(&raw.timezone),
            display_name: cstr_array_to_string(&raw.display_name),
            version_major: raw.version_major,
            version_minor: raw.version_minor,
            version_patch: raw.version_patch,
            revision: raw.revision,
        }
    }
}

fn cstr_array_to_string(buf: &[c_char]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    let bytes: &[u8] = unsafe { slice::from_raw_parts(buf.as_ptr().cast::<u8>(), end) };
    String::from_utf8_lossy(bytes).into_owned()
}

/// Open ClickHouse client. Owns the underlying `chc_client *`, the
/// [`Io`](crate::Io) backend it talks through, and (when compressed) its
/// [`Codec`]; Rust drop order guarantees the C-side back-pointers stay
/// valid through close.
pub struct Client<'fd> {
    raw: NonNull<sys::chc_client>,
    // `chc_client_init` stashes `c->al = al`, i.e. it stores a pointer
    // back into the `chc_alloc` struct we pass in; reads & writes go
    // through `c->al->{alloc,realloc,free}` on every subsequent call
    // and from `chc_client_close`. The `Box` heap-allocates the alloc
    // so its address stays stable across moves of `Self`. No `Pin` —
    // `Allocator: Unpin`, so a bare `Box` already gives all the
    // guarantee the C side needs.
    alloc: Box<Allocator>,
    _codec: Option<Pin<Box<Codec>>>,
    // Type-erased so the same `Client` carries either a plaintext
    // [`PosixIo`](crate::PosixIo) or a `tls::TlsIo`. `chc_client` retains
    // the `chc_io` pointer minted from this backend, so it must outlive
    // the client; the box keeps it pinned through `Drop`.
    io: Pin<Box<dyn Io + Send + 'fd>>,
}

impl<'fd> Client<'fd> {
    /// Performs Hello / HelloAck against the supplied I/O. Takes
    /// ownership of `io` and `codec` so the C-side back-pointers
    /// `c->io` / `c->codec` stay valid for the client's lifetime.
    ///
    /// `io` is any [`Io`] backend: [`PosixIo`](crate::PosixIo) for a
    /// plaintext fd, or `tls::TlsIo` (feature `tls`) for rustls over a
    /// `TcpStream`.
    ///
    /// `codec` may be `None` only when `opts.compression` is `None`.
    ///
    /// The `'fd` lifetime is the borrow on the file descriptor backing
    /// `io`. `Client<'fd>` cannot outlive that fd, so dropping the fd
    /// owner while the [`Client`] is still alive is a compile error:
    ///
    /// ```compile_fail
    /// use clickhouse_c::{Allocator, Client, ClientOpts, PosixIo};
    /// use std::net::TcpStream;
    /// use std::os::fd::AsFd;
    ///
    /// fn build() -> clickhouse_c::Result<Client<'static>> {
    ///     let sock = TcpStream::connect("localhost:9000")?;
    ///     let io = PosixIo::new(sock.as_fd());
    ///     // Client<'_> borrows `sock` through `io`; can't promote to
    ///     // 'static because `sock` dies at the end of this scope.
    ///     Client::init(&ClientOpts::new(), Allocator::stdlib(), io, None)
    /// }
    /// ```
    pub fn init<I: Io + Send + 'fd>(
        opts: &ClientOpts,
        alloc: Allocator,
        mut io: Pin<Box<I>>,
        codec: Option<Pin<Box<Codec>>>,
    ) -> Result<Self> {
        opts.validate_codec(codec.as_ref().map(|codec| codec.as_ref()))?;
        let codec_ptr = codec.as_ref().map(|c| c.as_ref().as_ptr());
        let raw_opts = opts.to_raw(codec_ptr)?;
        let alloc = Box::new(alloc);
        let mut out: *mut sys::chc_client = core::ptr::null_mut();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_client_init(
                &mut out,
                raw_opts.as_ptr(),
                alloc.as_ptr(),
                io.as_mut().io_ptr(),
                &mut err,
            )
        };
        check(rc, &err)?;
        Ok(Self {
            raw: NonNull::new(out).expect("chc_client_init returned OK with NULL"),
            alloc,
            _codec: codec,
            io,
        })
    }

    /// Identity the server sent during the handshake.
    pub fn server_info(&self) -> Option<ServerInfo> {
        let p = unsafe { sys::chc_client_server_info(self.raw.as_ptr().cast_const()) };
        if p.is_null() {
            None
        } else {
            Some(ServerInfo::from_raw(unsafe { &*p }))
        }
    }

    /// Set backend read timeout. Refresh before each operation when using
    /// [`PosixIo`](crate::PosixIo), whose timeout is an absolute deadline.
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.io.as_mut().set_read_timeout(timeout)
    }

    /// Send a query with no settings or parameters. The server applies
    /// whatever its profile defaults to, which for a Native consumer means
    /// trusting it not to have turned on binary type names; see
    /// [`QuerySetting::TEXT_TYPE_NAMES`](crate::QuerySetting::TEXT_TYPE_NAMES)
    /// and [`send_query_with`](Self::send_query_with).
    pub fn send_query(&mut self, sql: &str, query_id: Option<&str>) -> Result<()> {
        let (qid, qid_len) = query_id
            .map(|q| (q.as_ptr().cast::<c_char>(), q.len()))
            .unwrap_or((core::ptr::null(), 0));
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_client_send_query(
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

    /// Send a query with per-query settings and `{name:Type}` parameters.
    ///
    /// ```no_run
    /// use clickhouse_c::{QueryOpts, QueryParam, QuerySetting};
    /// # fn run(client: &mut clickhouse_c::Client<'_>) -> clickhouse_c::Result<()> {
    /// let settings = [
    ///     QuerySetting::TEXT_TYPE_NAMES,
    ///     QuerySetting::new("max_block_size", "8192"),
    /// ];
    /// let params = [QueryParam::new("cutoff", "100")];
    /// client.send_query_with(
    ///     "SELECT number FROM numbers(1000) WHERE number > {cutoff:UInt64}",
    ///     &QueryOpts::new().settings(&settings).params(&params),
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn send_query_with(&mut self, sql: &str, opts: &QueryOpts<'_>) -> Result<()> {
        let raw_opts = RawQueryOpts::new(opts)?;
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_client_send_query_ex(
                self.raw.as_ptr(),
                sql.as_ptr().cast::<c_char>(),
                sql.len(),
                raw_opts.as_ptr(),
                &mut err,
            )
        };
        check(rc, &err)
    }

    /// Send a Data block. Passing [`None`] writes the empty block that
    /// terminates a query / signals "no more INSERT rows".
    pub fn send_data(&mut self, builder: Option<&BlockBuilder<'_>>) -> Result<()> {
        let bb_ptr = builder.map(|b| b.as_ptr()).unwrap_or(core::ptr::null());
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_client_send_data(self.raw.as_ptr(), bb_ptr, &mut err) };
        check(rc, &err)
    }

    /// Send the protocol Cancel packet, asking the server to stop producing
    /// rows for the query in flight. Packets already on the wire still
    /// arrive, so keep draining until `EndOfStream`.
    ///
    /// This is the remote half of cancellation and needs a working
    /// connection. To stop *reading* -- because the caller is shutting down,
    /// or the peer went quiet -- use
    /// [`CancelToken`](crate::CancelToken), which fails local reads and
    /// sends nothing.
    pub fn send_cancel(&mut self) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_client_send_cancel(self.raw.as_ptr(), &mut err) };
        check(rc, &err)
    }

    /// Send a Ping. The server answers [`Event::Pong`]; useful for probing
    /// a connection between queries.
    pub fn send_ping(&mut self) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_client_send_ping(self.raw.as_ptr(), &mut err) };
        check(rc, &err)
    }

    /// Read the next server event, blocking until a full packet arrives.
    /// Any block / exception payload is owned by the returned [`Event`]
    /// and freed on drop.
    pub fn recv_event(&mut self) -> Result<Event> {
        let mut raw = sys::chc_packet::zeroed();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_client_recv_packet(self.raw.as_ptr(), &mut raw, &mut err) };
        if let Err(e) = check(rc, &err) {
            unsafe { sys::chc_packet_clear(self.raw.as_ptr(), &mut raw) };
            return Err(e);
        }
        let event = Event::from_raw(&mut raw, *self.alloc);
        unsafe { sys::chc_packet_clear(self.raw.as_ptr(), &mut raw) };
        event
    }
}

impl<'fd> Drop for Client<'fd> {
    fn drop(&mut self) {
        unsafe { sys::chc_client_close(self.raw.as_ptr()) };
    }
}

unsafe impl<'fd> Send for Client<'fd> {}

/// Server-side exception. Owning wrapper around the C `chc_exception`.
/// [`Drop`] calls `chc_exception_free`.
pub struct Exception {
    raw: NonNull<sys::chc_exception>,
    alloc: Allocator,
}

impl Exception {
    /// SAFETY: `raw` must point at a `chc_exception` owned by the caller;
    /// `alloc` must be the allocator it was created with.
    pub(crate) unsafe fn from_raw(raw: NonNull<sys::chc_exception>, alloc: Allocator) -> Self {
        Self { raw, alloc }
    }

    /// ClickHouse error code, matching `system.errors`.
    pub fn code(&self) -> i32 {
        unsafe { (*self.raw.as_ptr()).code }
    }

    /// Exception class name. Server-controlled bytes, not UTF-8-validated.
    pub fn name(&self) -> &[u8] {
        let r = unsafe { self.raw.as_ref() };
        cstr_bytes(r.name, r.name_len)
    }

    /// Human-readable message. Server-controlled bytes, not
    /// UTF-8-validated.
    pub fn display_text(&self) -> &[u8] {
        let r = unsafe { self.raw.as_ref() };
        cstr_bytes(r.display_text, r.display_text_len)
    }

    /// Server stack trace, empty unless the server was asked for one.
    /// Server-controlled bytes, not UTF-8-validated.
    pub fn stack_trace(&self) -> &[u8] {
        let r = unsafe { self.raw.as_ref() };
        cstr_bytes(r.stack_trace, r.stack_trace_len)
    }
}

impl Drop for Exception {
    fn drop(&mut self) {
        unsafe { sys::chc_exception_free(self.raw.as_ptr(), self.alloc.as_ptr()) };
    }
}

impl core::fmt::Debug for Exception {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Exception")
            .field("code", &self.code())
            .field("name", &String::from_utf8_lossy(self.name()))
            .field(
                "display_text",
                &String::from_utf8_lossy(self.display_text()),
            )
            .field("stack_trace", &String::from_utf8_lossy(self.stack_trace()))
            .finish()
    }
}

impl core::fmt::Display for Exception {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{} (code {}): {}",
            String::from_utf8_lossy(self.name()),
            self.code(),
            String::from_utf8_lossy(self.display_text()),
        )
    }
}

impl std::error::Error for Exception {}

unsafe impl Send for Exception {}

impl From<Exception> for Error {
    fn from(exc: Exception) -> Self {
        Self {
            kind: ErrorKind::Server,
            server_code: exc.code(),
            message: String::from_utf8_lossy(exc.display_text()).into_owned(),
            server_name: String::from_utf8_lossy(exc.name()).into_owned(),
        }
    }
}

fn cstr_bytes<'a>(ptr: *mut c_char, len: usize) -> &'a [u8] {
    if ptr.is_null() || len == 0 {
        return &[];
    }
    debug_assert!(
        len <= isize::MAX as usize,
        "clickhouse-c published exception field len = {len}",
    );
    unsafe { slice::from_raw_parts(ptr.cast::<u8>(), len) }
}

/// Server-to-client packet tags. `CHC_PKT_HELLO` has no variant: the Hello
/// exchange happens inside [`Client::init`], never as a received packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PacketKind {
    Data = sys::CHC_PKT_DATA,
    Exception = sys::CHC_PKT_EXCEPTION,
    Progress = sys::CHC_PKT_PROGRESS,
    Pong = sys::CHC_PKT_PONG,
    EndOfStream = sys::CHC_PKT_END_OF_STREAM,
    ProfileInfo = sys::CHC_PKT_PROFILE_INFO,
    Totals = sys::CHC_PKT_TOTALS,
    Extremes = sys::CHC_PKT_EXTREMES,
    Log = sys::CHC_PKT_LOG,
    TableColumns = sys::CHC_PKT_TABLE_COLUMNS,
    ProfileEvents = sys::CHC_PKT_PROFILE_EVENTS,
}

impl PacketKind {
    pub(crate) fn from_raw(k: sys::chc_packet_kind) -> Option<Self> {
        Some(match k {
            sys::CHC_PKT_DATA => Self::Data,
            sys::CHC_PKT_EXCEPTION => Self::Exception,
            sys::CHC_PKT_PROGRESS => Self::Progress,
            sys::CHC_PKT_PONG => Self::Pong,
            sys::CHC_PKT_END_OF_STREAM => Self::EndOfStream,
            sys::CHC_PKT_PROFILE_INFO => Self::ProfileInfo,
            sys::CHC_PKT_TOTALS => Self::Totals,
            sys::CHC_PKT_EXTREMES => Self::Extremes,
            sys::CHC_PKT_LOG => Self::Log,
            sys::CHC_PKT_TABLE_COLUMNS => Self::TableColumns,
            sys::CHC_PKT_PROFILE_EVENTS => Self::ProfileEvents,
            _ => return None,
        })
    }
}

/// Owned server event from the packet loop, shared by [`Client`] and the
/// async client. Any block / exception payload is owned here and freed on
/// drop. Reading the `kind`-selected union arm happens once, in
/// `Event::from_raw`, so consumers never touch the raw union.
pub enum Event {
    /// A result block, or an INSERT's expected-structure block.
    Data(Block),
    /// The `WITH TOTALS` row.
    Totals(Block),
    /// The `WITH EXTREMES` min/max rows.
    Extremes(Block),
    /// Server log lines, when `send_logs_level` asks for them.
    Log(Block),
    /// Per-query profile-event counters.
    ProfileEvents(Block),
    /// The query failed. Nothing more arrives on this query.
    Exception(Exception),
    /// Incremental read/write counters; sent repeatedly during a query.
    Progress(Progress),
    /// Row and byte totals, sent once near the end of a query.
    ProfileInfo(ProfileInfo),
    /// Reply to [`Client::send_ping`].
    Pong,
    /// The query is done. Stop draining.
    EndOfStream,
    /// Column metadata for an INSERT target. The payload is not decoded;
    /// the following Data block carries the same structure.
    TableColumns,
}

impl Event {
    /// Consume a recv'd packet, taking ownership of its payload. `raw`
    /// must come straight from `chc_*_recv_packet`; arms are read only
    /// after `kind` selects them, so no union read is unsound.
    pub(crate) fn from_raw(raw: &mut sys::chc_packet, alloc: Allocator) -> Result<Self> {
        let Some(kind) = PacketKind::from_raw(raw.kind) else {
            return Err(Error::new(
                ErrorKind::Protocol,
                format!("unknown server packet {}", raw.kind),
            ));
        };
        Ok(match kind {
            PacketKind::Data => Self::Data(take_block(raw, alloc)?),
            PacketKind::Totals => Self::Totals(take_block(raw, alloc)?),
            PacketKind::Extremes => Self::Extremes(take_block(raw, alloc)?),
            PacketKind::Log => Self::Log(take_block(raw, alloc)?),
            PacketKind::ProfileEvents => Self::ProfileEvents(take_block(raw, alloc)?),
            PacketKind::Exception => Self::Exception(take_exception(raw, alloc)?),
            PacketKind::Progress => {
                // SAFETY: kind selects the `progress` arm.
                Self::Progress(Progress::from_raw(unsafe { &raw.payload.progress }))
            }
            PacketKind::ProfileInfo => {
                // SAFETY: kind selects the `profile` arm.
                Self::ProfileInfo(ProfileInfo::from_raw(unsafe { &raw.payload.profile }))
            }
            PacketKind::Pong => Self::Pong,
            PacketKind::EndOfStream => Self::EndOfStream,
            PacketKind::TableColumns => Self::TableColumns,
        })
    }
}

fn take_block(raw: &mut sys::chc_packet, alloc: Allocator) -> Result<Block> {
    // SAFETY: caller's kind match selected the `block` arm.
    let p = unsafe { raw.payload.block };
    raw.payload.block = core::ptr::null_mut();
    // SAFETY: ownership transfer; alloc matches the recv'ing client.
    unsafe { Block::from_raw(p, alloc) }
        .ok_or_else(|| Error::new(ErrorKind::Protocol, "block packet missing block"))
}

fn take_exception(raw: &mut sys::chc_packet, alloc: Allocator) -> Result<Exception> {
    // SAFETY: caller's kind match selected the `exception` arm.
    let p = NonNull::new(unsafe { raw.payload.exception })
        .ok_or_else(|| Error::new(ErrorKind::Protocol, "exception packet missing exception"))?;
    raw.payload.exception = core::ptr::null_mut();
    // SAFETY: ownership transfer; alloc matches the recv'ing client.
    Ok(unsafe { Exception::from_raw(p, alloc) })
}

/// Incremental counters for the query in flight. Each packet reports a
/// delta, not a running total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    pub rows: u64,
    pub bytes: u64,
    pub total_rows: u64,
    pub written_rows: u64,
    pub written_bytes: u64,
}

impl Progress {
    fn from_raw(raw: &sys::chc_packet_progress) -> Self {
        Self {
            rows: raw.rows,
            bytes: raw.bytes,
            total_rows: raw.total_rows,
            written_rows: raw.written_rows,
            written_bytes: raw.written_bytes,
        }
    }
}

/// End-of-query totals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProfileInfo {
    pub rows: u64,
    pub blocks: u64,
    pub bytes: u64,
    pub rows_before_limit: u64,
    pub applied_limit: bool,
    pub calculated_rows_before_limit: bool,
}

impl ProfileInfo {
    fn from_raw(raw: &sys::chc_packet_profile) -> Self {
        Self {
            rows: raw.rows,
            blocks: raw.blocks,
            bytes: raw.bytes,
            rows_before_limit: raw.rows_before_limit,
            applied_limit: raw.applied_limit != 0,
            calculated_rows_before_limit: raw.calculated_rows_before_limit != 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ClientOpts;
    use crate::{Codec, Compression, ErrorKind};

    #[cfg(feature = "lz4")]
    #[test]
    fn compression_requires_matching_codec() {
        let missing = ClientOpts {
            compression: Compression::Lz4,
            ..ClientOpts::new()
        }
        .validate_codec(None)
        .expect_err("missing codec");
        assert_eq!(missing.kind, ErrorKind::Usage);

        let codec = Codec::lz4();
        let mismatch = ClientOpts {
            compression: Compression::Zstd,
            ..ClientOpts::new()
        }
        .validate_codec(Some(codec.as_ref()))
        .expect_err("mismatched codec");
        assert_eq!(mismatch.kind, ErrorKind::Usage);
    }
}
