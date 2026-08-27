//! Drives the native protocol against a real server with no runtime at all:
//! a blocking `std::net::TcpStream` and a hand-written byte pump.
//!
//! The point is not that anyone would write it this way, but that everything
//! another runtime needs is public. Nothing here touches `tokio`, `sys`, or
//! `unsafe`.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;

use clickhouse_c::{
    Allocator, ClientOpts, Event, IolessClient, QuerySetting, Result, Step, TypeAst,
};
use common::{ChServer, TestResult, clickhouse_on_path};

/// The whole transport contract: push queued bytes out, pull bytes in.
struct Pump {
    sock: TcpStream,
    buf: Vec<u8>,
    /// Total bytes written, to prove partial writes are exercised.
    written: usize,
}

impl Pump {
    fn new(sock: TcpStream, read_chunk: usize) -> Self {
        Self {
            sock,
            buf: vec![0; read_chunk],
            written: 0,
        }
    }

    /// Write at most `limit` bytes per call, so `consume_out` is driven with
    /// partial counts rather than always draining the queue in one go.
    fn flush(&mut self, core: &mut IolessClient, limit: usize) -> Result<()> {
        loop {
            let pending = core.pending_out();
            if pending.is_empty() {
                return Ok(());
            }
            let take = pending.len().min(limit);
            let n = self.sock.write(&pending[..take])?;
            self.written += n;
            core.consume_out(n);
        }
    }

    fn fill(&mut self, core: &mut IolessClient) -> Result<()> {
        let n = self.sock.read(&mut self.buf)?;
        if n == 0 {
            return Err(clickhouse_c::Error::from(std::io::Error::other(
                "server closed the connection",
            )));
        }
        core.submit(&self.buf[..n])
    }
}

/// Deliberately tiny read and write chunks: the parser has to resume
/// mid-packet, which is the whole reason the ioless API exists.
const CHUNK: usize = 7;

fn drive<T>(
    core: &mut IolessClient,
    pump: &mut Pump,
    mut step: impl FnMut(&mut IolessClient) -> Result<Step<T>>,
) -> Result<T> {
    loop {
        match step(core)? {
            Step::Ready(value) => {
                pump.flush(core, CHUNK)?;
                return Ok(value);
            }
            // Flush before reading: a step that needs input has usually just
            // queued the bytes the server is waiting on, and skipping this
            // deadlocks both sides.
            Step::NeedsInput => {
                pump.flush(core, CHUNK)?;
                pump.fill(core)?;
            }
        }
    }
}

#[test]
fn a_hand_written_pump_runs_a_query() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("skipping: clickhouse not on PATH");
        return Ok(());
    }
    let server = ChServer::spawn()?;
    let sock = TcpStream::connect(("127.0.0.1", server.tcp_port))?;
    sock.set_nodelay(true).ok();

    let mut core = IolessClient::new(
        &ClientOpts::new().user("default").client_name("ioless-test"),
        Allocator::stdlib(),
        None,
    )?;
    // Pre-handshake the slot exists but is empty; the revision is seeded
    // with what the client asked for.
    let seeded = core.server_info().expect("slot always exists");
    assert!(seeded.name.is_empty());
    assert!(seeded.revision > 0);

    let mut pump = Pump::new(sock, CHUNK);
    drive(&mut core, &mut pump, |c| c.handshake())?;
    let info = core.server_info().expect("handshake completed");
    assert!(!info.name.is_empty());

    core.send_query("SELECT number FROM numbers(2000)", None)?;

    let mut total_rows = 0usize;
    let mut sum = 0u64;
    loop {
        match drive(&mut core, &mut pump, |c| c.recv_event())? {
            Event::EndOfStream => break,
            Event::Exception(exc) => return Err(Box::new(exc)),
            Event::Data(block) => {
                let Some((_, bytes)) = block.column(0).and_then(|c| c.fixed()) else {
                    continue;
                };
                total_rows += block.n_rows();
                sum += bytes
                    .chunks_exact(8)
                    .map(|c| u64::from_le_bytes(c.try_into().expect("u64")))
                    .sum::<u64>();
            }
            _ => {}
        }
    }

    assert_eq!(total_rows, 2000);
    assert_eq!(sum, (0..2000u64).sum::<u64>());
    // 2000 rows past a 7-byte read buffer means the parser resumed mid-block
    // many times over.
    assert!(pump.written > 0);
    Ok(())
}

#[test]
fn a_hand_written_pump_runs_an_insert() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("skipping: clickhouse not on PATH");
        return Ok(());
    }
    let server = ChServer::spawn()?;
    server.query("CREATE TABLE ioless (n UInt32) ENGINE = Memory")?;

    let sock = TcpStream::connect(("127.0.0.1", server.tcp_port))?;
    sock.set_nodelay(true).ok();
    let mut core = IolessClient::new(&ClientOpts::new(), Allocator::stdlib(), None)?;
    let mut pump = Pump::new(sock, CHUNK);
    drive(&mut core, &mut pump, |c| c.handshake())?;

    core.send_query("INSERT INTO ioless (n) VALUES", None)?;
    // The server answers with a header block describing the target, though
    // TableColumns and Progress can arrive first.
    loop {
        match drive(&mut core, &mut pump, |c| c.recv_event())? {
            Event::Data(header) => {
                assert_eq!(header.n_rows(), 0);
                assert_eq!(header.column_name(0), Some(&b"n"[..]));
                break;
            }
            Event::Exception(exc) => return Err(Box::new(exc)),
            Event::EndOfStream => return Err("no header block".into()),
            _ => {}
        }
    }

    let alloc = Allocator::stdlib();
    let ty = TypeAst::parse("UInt32", alloc)?;
    let values: Vec<u8> = (1u32..=4).flat_map(|v| v.to_le_bytes()).collect();
    let col = clickhouse_c::ColumnBuilder::fixed(&values, 4, 4)?;
    let mut block = clickhouse_c::BlockBuilder::new();
    block.append("n", ty.view(), &col)?;

    core.send_data(Some(&block))?;
    core.send_data(None)?;
    loop {
        match drive(&mut core, &mut pump, |c| c.recv_event())? {
            Event::EndOfStream => break,
            Event::Exception(exc) => return Err(Box::new(exc)),
            _ => {}
        }
    }

    assert_eq!(server.query("SELECT sum(n) FROM ioless")?, "10");
    Ok(())
}

/// `IolessClient` runs the same `ClientOpts` validation as the blocking
/// client, before any byte is queued.
#[test]
fn opts_are_validated_without_a_connection() {
    let Err(err) = IolessClient::new(
        &ClientOpts::new().database("bad\u{0}name"),
        Allocator::stdlib(),
        None,
    ) else {
        panic!("interior NUL must be rejected");
    };
    assert_eq!(err.kind, clickhouse_c::ErrorKind::Usage);
}

/// A fresh machine has queued its Hello but read nothing, so `pending_out` is
/// non-empty and `recv_event` needs input. No socket involved.
#[test]
fn a_fresh_machine_queues_hello_and_waits() {
    let mut core =
        IolessClient::new(&ClientOpts::new(), Allocator::stdlib(), None).expect("construct");
    assert!(matches!(
        core.handshake().expect("handshake step"),
        Step::NeedsInput
    ));
    assert!(!core.pending_out().is_empty(), "Hello should be queued");

    // Consuming more than is queued is clamped, not a panic.
    let queued = core.pending_out().len();
    core.consume_out(queued * 2);
    assert!(core.pending_out().is_empty());

    let _ = QuerySetting::TEXT_TYPE_NAMES; // settings are a blocking-client feature
}
