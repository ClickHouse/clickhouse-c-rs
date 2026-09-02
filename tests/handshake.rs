//! Handshake rejection tests using a temporary ClickHouse server.
//!
//! Tests skip when `clickhouse` is unavailable.

mod common;

use std::io::{Read, Write};
use std::net::TcpStream;

use clickhouse_c::{Allocator, Client, ClientOpts, Error, ErrorKind, IolessClient, PosixIo, Step};
use common::{ChServer, TestResult, clickhouse_on_path};

const MISSING_DB: &str = "no such database";

/// Server sends full exception text, so message is not truncated to the
/// `chc_err` buffer.
fn assert_rejected(err: Error) {
    assert_eq!(err.kind, ErrorKind::Server);
    assert_ne!(err.server_code, 0, "{err}");
    assert!(!err.server_name.is_empty(), "{err}");
    assert!(err.message.contains(MISSING_DB), "{err}");
}

#[test]
fn missing_database_rejects_blocking_handshake() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("skipping: clickhouse not on PATH");
        return Ok(());
    }
    let server = ChServer::spawn()?;
    let sock = TcpStream::connect(("127.0.0.1", server.tcp_port))?;
    let Err(err) = Client::init(
        &ClientOpts::new().database(MISSING_DB),
        Allocator::stdlib(),
        PosixIo::new_owned(sock),
        None,
    ) else {
        panic!("missing database must reject handshake");
    };
    assert_rejected(err);
    Ok(())
}

#[test]
fn missing_database_rejects_ioless_handshake() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("skipping: clickhouse not on PATH");
        return Ok(());
    }
    let server = ChServer::spawn()?;
    let mut sock = TcpStream::connect(("127.0.0.1", server.tcp_port))?;
    let mut core = IolessClient::new(
        &ClientOpts::new().database(MISSING_DB),
        Allocator::stdlib(),
        None,
    )?;
    let mut buf = [0u8; 4096];
    let err = loop {
        match core.handshake() {
            Ok(Step::Ready(())) => panic!("missing database must reject handshake"),
            Ok(Step::NeedsInput) => {}
            Err(err) => break err,
        }
        while !core.pending_out().is_empty() {
            let n = sock.write(core.pending_out())?;
            core.consume_out(n);
        }
        let n = sock.read(&mut buf)?;
        assert_ne!(n, 0, "server closed before sending exception");
        core.submit(&buf[..n])?;
    };
    assert_rejected(err);
    Ok(())
}
