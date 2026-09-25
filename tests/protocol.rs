//! Protocol packets that only a real server produces.
//!
//! Tests skip when `clickhouse` is unavailable.

mod common;

use std::net::TcpStream;
use std::time::Duration;

use clickhouse_c::{
    Allocator, Client, ClientOpts, ErrorKind, Event, PosixIo, QueryOpts, QuerySetting,
};
use common::{ChServer, TestResult, clickhouse_on_path};

fn connect(server: &ChServer) -> TestResult<Client<'static>> {
    let sock = TcpStream::connect(("127.0.0.1", server.tcp_port))?;
    Ok(Client::init(
        &ClientOpts::new(),
        Allocator::stdlib(),
        PosixIo::new_owned(sock),
        None,
    )?)
}

/// Counts every event kind seen through EndOfStream.
#[derive(Default)]
struct Seen {
    data: usize,
    totals: usize,
    extremes: usize,
    log: usize,
    profile_events: usize,
    profile_info: usize,
    progress: usize,
    table_columns: usize,
}

fn drain(client: &mut Client<'_>) -> TestResult<Seen> {
    let mut seen = Seen::default();
    loop {
        match client.recv_event()? {
            Event::EndOfStream => return Ok(seen),
            Event::Exception(e) => return Err(Box::new(e)),
            Event::Data(_) => seen.data += 1,
            Event::Totals(_) => seen.totals += 1,
            Event::Extremes(_) => seen.extremes += 1,
            Event::Log(_) => seen.log += 1,
            Event::ProfileEvents(_) => seen.profile_events += 1,
            Event::ProfileInfo(_) => seen.profile_info += 1,
            Event::Progress(_) => seen.progress += 1,
            Event::TableColumns => seen.table_columns += 1,
            Event::TimezoneUpdate => {}
            Event::Pong => panic!("unexpected Pong during a query"),
        }
    }
}

#[test]
fn ping_answers_with_pong() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    let server = ChServer::spawn()?;
    let mut client = connect(&server)?;
    client.send_ping()?;
    assert!(matches!(client.recv_event()?, Event::Pong));

    // Connection stays usable for a query after a ping
    client.send_query("SELECT 1", None)?;
    client.send_data(None)?;
    assert!(drain(&mut client)?.data > 0);
    Ok(())
}

/// `WITH TOTALS` and `extremes` each arrive as their own packet kind rather
/// than as extra rows in a Data block.
#[test]
fn totals_and_extremes_arrive_as_their_own_events() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    let server = ChServer::spawn()?;
    let mut client = connect(&server)?;
    let settings = [QuerySetting::new("extremes", "1")];
    client.send_query_with(
        "SELECT number % 2 AS k, count() AS n FROM numbers(10) GROUP BY k WITH TOTALS ORDER BY k",
        &QueryOpts::new().settings(&settings),
    )?;
    client.send_data(None)?;

    let seen = drain(&mut client)?;
    assert_eq!(seen.totals, 1, "expected one Totals packet");
    assert_eq!(seen.extremes, 1, "expected one Extremes packet");
    assert!(seen.profile_info > 0);
    Ok(())
}

/// `send_logs_level` makes the server stream its own log rows over the same
/// connection.
#[test]
fn server_logs_arrive_as_log_events() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    let server = ChServer::spawn()?;
    let mut client = connect(&server)?;
    let settings = [QuerySetting::new("send_logs_level", "trace")];
    client.send_query_with(
        "SELECT count() FROM numbers(1000)",
        &QueryOpts::new().settings(&settings),
    )?;
    client.send_data(None)?;

    let seen = drain(&mut client)?;
    assert!(seen.log > 0, "expected Log packets at trace level");
    Ok(())
}

/// Cancel asks the server to stop. Packets already in flight still arrive, and
/// a server may drop the connection rather than answer, so both a clean
/// EndOfStream and a transport error end the read loop.
#[test]
fn cancel_ends_a_long_query() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    let server = ChServer::spawn()?;
    let mut client = connect(&server)?;
    client.send_query("SELECT number FROM numbers(100000000000)", None)?;
    client.send_data(None)?;
    // One event proves the query is running before cancelling it
    client.recv_event()?;
    client.send_cancel()?;

    loop {
        match client.recv_event() {
            Ok(Event::EndOfStream) | Err(_) => return Ok(()),
            Ok(Event::Exception(e)) => return Err(Box::new(e)),
            Ok(_) => {}
        }
    }
}

/// A server exception carries every field its Debug and Display read.
#[test]
fn an_exception_reports_every_field() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    let server = ChServer::spawn()?;
    let mut client = connect(&server)?;
    client.send_query("SELECT * FROM definitely_missing_table", None)?;
    client.send_data(None)?;

    loop {
        match client.recv_event()? {
            Event::Exception(e) => {
                assert_ne!(e.code(), 0);
                assert!(!e.name().is_empty());
                assert!(!e.display_text().is_empty());
                // Server decides whether to calculate a text stack trace
                let _ = e.stack_trace();

                let debug = format!("{e:?}");
                assert!(debug.contains("Exception"), "{debug}");
                assert!(debug.contains(&e.code().to_string()), "{debug}");
                assert!(debug.contains("stack_trace"), "{debug}");

                let display = e.to_string();
                assert!(display.contains(&e.code().to_string()), "{display}");

                // Conversion keeps the server class and code
                let err = clickhouse_c::Error::from(e);
                assert_eq!(err.kind, ErrorKind::Server);
                assert_ne!(err.server_code, 0);
                assert!(!err.server_name.is_empty());
                return Ok(());
            }
            Event::EndOfStream => panic!("missing table ended without an exception"),
            _ => {}
        }
    }
}

/// A deadline on an idle connection surfaces as a transport error instead of
/// blocking forever.
#[test]
fn a_read_deadline_fails_an_idle_recv() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    let server = ChServer::spawn()?;
    let mut client = connect(&server)?;
    client.set_read_timeout(Some(Duration::from_millis(50)))?;
    let Err(err) = client.recv_event() else {
        panic!("idle connection produced an event");
    };
    assert_eq!(err.kind, ErrorKind::Io);

    // Clearing the deadline is accepted even after it fired
    client.set_read_timeout(None)?;
    Ok(())
}
