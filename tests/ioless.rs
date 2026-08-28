//! I/O-independent client tests using blocking TCP transport.
//!
//! Tests use only public safe API required by alternate runtimes.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;

use clickhouse_c::{
    Allocator, ClientOpts, Event, IolessClient, QuerySetting, Result, Step, TypeAst,
};
use common::{ChServer, TestResult, clickhouse_on_path};

/// Transfers queued output and submits received input.
struct Pump {
    sock: TcpStream,
    buf: Vec<u8>,
    /// Total bytes accepted by transport.
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

    /// Writes at most `limit` bytes to exercise partial output consumption.
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

/// Creates byte pump with small chunks to exercise incremental parsing.
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
            // Send queued protocol output before blocking for input
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
    // Initial server information contains requested revision
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
    // Small input chunks force parser to resume within block
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
    // Wait for INSERT structure Data block
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

/// Verifies client options before protocol output is queued.
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

/// Verifies initial Hello output and input requirement without transport.
#[test]
fn a_fresh_machine_queues_hello_and_waits() {
    let mut core =
        IolessClient::new(&ClientOpts::new(), Allocator::stdlib(), None).expect("construct");
    assert!(matches!(
        core.handshake().expect("handshake step"),
        Step::NeedsInput
    ));
    assert!(!core.pending_out().is_empty(), "Hello should be queued");

    // Consumption beyond queue length removes all output
    let queued = core.pending_out().len();
    core.consume_out(queued * 2);
    assert!(core.pending_out().is_empty());

    let _ = QuerySetting::TEXT_TYPE_NAMES; // I/O-independent API does not support settings
}
