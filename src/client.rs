//! Blocking ClickHouse native protocol client.
//!
//! Caller supplies a connected [`Io`] transport. Crate provides
//! [`PosixIo`](crate::PosixIo) for Unix file descriptors and
//! `tls::TlsIo` for rustls connections.

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

/// Client settings sent during Hello handshake.
///
/// String values are copied and null-terminated during connection. Interior
/// null bytes return [`ErrorKind::Usage`](crate::ErrorKind::Usage).
#[derive(Clone, Debug, Default)]
pub struct ClientOpts {
    client_name: Option<String>,
    database: Option<String>,
    user: Option<String>,
    password: Option<String>,
    /// Client version reported in `system.query_log`. Default is 0.0.0.
    pub client_version_major: u64,
    pub client_version_minor: u64,
    pub client_version_patch: u64,
    /// Requested native protocol revision. Zero uses
    /// [`sys::CHC_CLIENT_DEFAULT_REVISION`]. Server limits negotiated revision
    /// to its supported value.
    pub client_revision: u64,
    pub compression: Compression,
    /// Read buffer size in bytes. Zero selects clickhouse-c 8 KiB default.
    pub read_buffer_bytes: usize,
}

impl ClientOpts {
    /// Creates settings for `default` user and database, empty password, and
    /// no compression.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets client name. Default is `"clickhouse-c"`.
    pub fn client_name(mut self, s: &str) -> Self {
        self.client_name = Some(s.to_owned());
        self
    }
    /// Sets default database for unqualified table names.
    pub fn database(mut self, s: &str) -> Self {
        self.database = Some(s.to_owned());
        self
    }
    /// Sets account name used for authentication.
    pub fn user(mut self, s: &str) -> Self {
        self.user = Some(s.to_owned());
        self
    }
    /// Sets authentication password.
    ///
    /// Native protocol sends password as clear text inside Hello message. Use
    /// TLS when transport is not trusted.
    pub fn password(mut self, s: &str) -> Self {
        self.password = Some(s.to_owned());
        self
    }

    /// Sets reported client version.
    pub fn client_version(mut self, major: u64, minor: u64, patch: u64) -> Self {
        self.client_version_major = major;
        self.client_version_minor = minor;
        self.client_version_patch = patch;
        self
    }

    /// Sets requested protocol revision.
    pub fn client_revision(mut self, revision: u64) -> Self {
        self.client_revision = revision;
        self
    }

    /// Sets compression algorithm.
    ///
    /// Compressed connections require a matching [`Codec`] in [`Client::init`].
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

/// Owns raw client options and null-terminated strings referenced by them.
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

/// Server information received during Hello handshake.
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

/// Active blocking ClickHouse connection.
///
/// Client owns C connection, I/O transport, and optional compression codec.
pub struct Client<'fd> {
    raw: NonNull<sys::chc_client>,
    // C connection retains allocator address until close
    alloc: Box<Allocator>,
    _codec: Option<Pin<Box<Codec>>>,
    // C connection retains callback pointer into pinned transport
    io: Pin<Box<dyn Io + Send + 'fd>>,
}

impl<'fd> Client<'fd> {
    /// Creates client and completes Hello handshake using supplied transport.
    ///
    /// Method takes ownership of `io` and `codec`. `io` can be
    /// [`PosixIo`](crate::PosixIo), `tls::TlsIo`, or custom [`Io`]
    /// implementation.
    ///
    /// `codec` can be `None` only when compression is disabled.
    ///
    /// Server rejection returns [`ErrorKind::Server`] carrying exception code,
    /// class, and untruncated message.
    ///
    /// Lifetime `'fd` prevents client from outliving a borrowed file
    /// descriptor:
    ///
    /// ```compile_fail
    /// use clickhouse_c::{Allocator, Client, ClientOpts, PosixIo};
    /// use std::net::TcpStream;
    /// use std::os::fd::AsFd;
    ///
    /// fn build() -> clickhouse_c::Result<Client<'static>> {
    ///     let sock = TcpStream::connect("localhost:9000")?;
    ///     let io = PosixIo::new(sock.as_fd());
    ///     // Borrowed socket cannot produce Client<'static>.
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
        let mut exc: *mut sys::chc_exception = core::ptr::null_mut();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_client_init(
                &mut out,
                raw_opts.as_ptr(),
                alloc.as_ptr(),
                io.as_mut().io_ptr(),
                &mut exc,
                &mut err,
            )
        };
        if let Some(e) = take_handshake_exception(exc, *alloc) {
            return Err(e);
        }
        check(rc, &err)?;
        Ok(Self {
            raw: NonNull::new(out).expect("chc_client_init returned OK with NULL"),
            alloc,
            _codec: codec,
            io,
        })
    }

    /// Returns server information received during handshake.
    pub fn server_info(&self) -> Option<ServerInfo> {
        let p = unsafe { sys::chc_client_server_info(self.raw.as_ptr().cast_const()) };
        if p.is_null() {
            None
        } else {
            Some(ServerInfo::from_raw(unsafe { &*p }))
        }
    }

    /// Sets transport read timeout.
    ///
    /// [`PosixIo`](crate::PosixIo) uses an absolute deadline. Set timeout
    /// again before each operation that requires a fresh deadline.
    pub fn set_read_timeout(&mut self, timeout: Option<Duration>) -> Result<()> {
        self.io.as_mut().set_read_timeout(timeout)
    }

    /// Sends a query without settings or parameters.
    ///
    /// Server profile supplies all query settings. Use
    /// [`QuerySetting::TEXT_TYPE_NAMES`](crate::QuerySetting::TEXT_TYPE_NAMES)
    /// with [`send_query_with`](Self::send_query_with) when profile may enable
    /// binary type names.
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

    /// Sends a query with settings and `{name:Type}` parameters.
    ///
    /// ```no_run
    /// use clickhouse_c::{QueryOpts, QueryParam, QuerySetting};
    /// # fn run(client: &mut clickhouse_c::Client<'_>) -> clickhouse_c::Result<()> {
    /// let settings = [
    ///     QuerySetting::TEXT_TYPE_NAMES,
    ///     QuerySetting::new("max_block_size", "8192"),
    /// ];
    /// let params = [QueryParam::new("cutoff", "'100'")];
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

    /// Sends a Data block.
    ///
    /// `None` sends empty block that ends INSERT input.
    pub fn send_data(&mut self, builder: Option<&BlockBuilder<'_>>) -> Result<()> {
        let bb_ptr = builder.map(|b| b.as_ptr()).unwrap_or(core::ptr::null());
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_client_send_data(self.raw.as_ptr(), bb_ptr, &mut err) };
        check(rc, &err)
    }

    /// Sends protocol Cancel packet for active query.
    ///
    /// Continue receiving events until [`Event::EndOfStream`] because packets
    /// already sent by server can still arrive. Use [`CancelToken`](crate::CancelToken)
    /// to cancel local reads without sending a packet.
    pub fn send_cancel(&mut self) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_client_send_cancel(self.raw.as_ptr(), &mut err) };
        check(rc, &err)
    }

    /// Sends Ping packet. Server responds with [`Event::Pong`].
    pub fn send_ping(&mut self) -> Result<()> {
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_client_send_ping(self.raw.as_ptr(), &mut err) };
        check(rc, &err)
    }

    /// Reads next server event and blocks until complete packet arrives.
    ///
    /// Returned event owns block or exception payload.
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

/// Exception returned by ClickHouse server.
pub struct Exception {
    raw: NonNull<sys::chc_exception>,
    alloc: Allocator,
}

impl Exception {
    /// SAFETY: caller must own `raw`, and `alloc` must match its allocator
    pub(crate) unsafe fn from_raw(raw: NonNull<sys::chc_exception>, alloc: Allocator) -> Self {
        Self { raw, alloc }
    }

    /// Returns ClickHouse error code from `system.errors`.
    pub fn code(&self) -> i32 {
        unsafe { (*self.raw.as_ptr()).code }
    }

    /// Returns exception class name without UTF-8 validation.
    pub fn name(&self) -> &[u8] {
        let r = unsafe { self.raw.as_ref() };
        cstr_bytes(r.name, r.name_len)
    }

    /// Returns exception message without UTF-8 validation.
    pub fn display_text(&self) -> &[u8] {
        let r = unsafe { self.raw.as_ref() };
        cstr_bytes(r.display_text, r.display_text_len)
    }

    /// Returns server stack trace without UTF-8 validation.
    ///
    /// Value is empty unless query requested a stack trace.
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

/// Converts handshake rejection into an error.
///
/// clickhouse-c leaves `err` empty for handshake rejection and transfers
/// exception ownership instead.
pub(crate) fn take_handshake_exception(
    exc: *mut sys::chc_exception,
    alloc: Allocator,
) -> Option<Error> {
    // SAFETY: handshake transferred ownership of exception allocated by alloc
    NonNull::new(exc).map(|p| unsafe { Exception::from_raw(p, alloc) }.into())
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

/// Server packet kind.
///
/// Hello packets are handled during [`Client::init`] and do not have a variant.
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

/// Event received from server.
///
/// Event owns any block or exception payload.
pub enum Event {
    /// Result block or expected INSERT structure.
    Data(Block),
    /// Row produced by `WITH TOTALS`.
    Totals(Block),
    /// Minimum and maximum rows produced by `WITH EXTREMES`.
    Extremes(Block),
    /// Server log rows requested by `send_logs_level`.
    Log(Block),
    /// Per-query profile event counters.
    ProfileEvents(Block),
    /// Server exception that ends current query.
    Exception(Exception),
    /// Incremental read and write counters.
    Progress(Progress),
    /// Row and byte totals sent near query completion.
    ProfileInfo(ProfileInfo),
    /// Response to [`Client::send_ping`].
    Pong,
    /// Query completion marker.
    EndOfStream,
    /// INSERT target metadata. Payload is not decoded. Following Data block
    /// contains same structure.
    TableColumns,
}

impl Event {
    /// Converts received C packet and takes ownership of its payload.
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
                // SAFETY: packet kind selects progress union member
                Self::Progress(Progress::from_raw(unsafe { &raw.payload.progress }))
            }
            PacketKind::ProfileInfo => {
                // SAFETY: packet kind selects profile union member
                Self::ProfileInfo(ProfileInfo::from_raw(unsafe { &raw.payload.profile }))
            }
            PacketKind::Pong => Self::Pong,
            PacketKind::EndOfStream => Self::EndOfStream,
            PacketKind::TableColumns => Self::TableColumns,
        })
    }
}

fn take_block(raw: &mut sys::chc_packet, alloc: Allocator) -> Result<Block> {
    // SAFETY: caller matched block packet kind
    let p = unsafe { raw.payload.block };
    raw.payload.block = core::ptr::null_mut();
    // SAFETY: allocator belongs to client that received block
    unsafe { Block::from_raw(p, alloc) }
        .ok_or_else(|| Error::new(ErrorKind::Protocol, "block packet missing block"))
}

fn take_exception(raw: &mut sys::chc_packet, alloc: Allocator) -> Result<Exception> {
    // SAFETY: caller matched exception packet kind
    let p = NonNull::new(unsafe { raw.payload.exception })
        .ok_or_else(|| Error::new(ErrorKind::Protocol, "exception packet missing exception"))?;
    raw.payload.exception = core::ptr::null_mut();
    // SAFETY: allocator belongs to client that received exception
    Ok(unsafe { Exception::from_raw(p, alloc) })
}

/// Incremental query counters. Each packet contains a delta.
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

/// Query totals reported near completion.
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
    use crate::{Compression, ErrorKind};

    /// Compression always requires a codec, including builds without codecs
    #[test]
    fn compression_without_a_codec_is_a_usage_error() {
        let err = ClientOpts::new()
            .compression(Compression::Lz4)
            .validate_codec(None)
            .expect_err("missing codec");
        assert_eq!(err.kind, ErrorKind::Usage);
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn compression_requires_matching_codec() {
        use crate::Codec;

        let codec = Codec::lz4();
        let mismatch = ClientOpts::new()
            .compression(Compression::Zstd)
            .validate_codec(Some(codec.as_ref()))
            .expect_err("mismatched codec");
        assert_eq!(mismatch.kind, ErrorKind::Usage);
    }
}
