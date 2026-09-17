//! Tokio client tests using a temporary ClickHouse server.
//!
//! Tests skip when `clickhouse` is unavailable.

mod common;

use core::pin::Pin;
use core::task::{Context, Poll};
use std::io;

use clickhouse_c::{
    AsyncClient, AsyncTransport, Block, BlockBuilder, BoxedAsyncClient, ClientOpts, ColumnBuilder,
    ErrorKind, Event, TypeAst,
};
use common::{ChServer, TestResult, clickhouse_on_path};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

async fn connect(server: &ChServer) -> clickhouse_c::Result<AsyncClient> {
    AsyncClient::connect(("127.0.0.1", server.tcp_port), ClientOpts::new(), None).await
}

async fn drain<S: AsyncTransport>(client: &mut AsyncClient<S>) -> TestResult {
    loop {
        match client.recv_event().await? {
            Event::EndOfStream => return Ok(()),
            Event::Exception(e) => return Err(boxed(e)),
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn async_insert_select_roundtrip() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    let server = ChServer::spawn()?;
    let mut client = connect(&server).await?;
    assert!(client.server_info().is_some());

    client
        .send_query(
            "CREATE TABLE async_roundtrip (id Int32, name String) ENGINE = Memory",
            None,
        )
        .await?;
    drain(&mut client).await?;

    client
        .send_query("INSERT INTO async_roundtrip FORMAT Native", None)
        .await?;
    let ids = [10i32, 20, 30];
    let id_bytes: Vec<u8> = ids.iter().flat_map(|v| v.to_le_bytes()).collect();
    let names = ["alpha", "beta", "gamma"];
    let (name_offsets, name_data) = string_column(&names);
    let alloc = clickhouse_c::Allocator::stdlib();
    let id_type = TypeAst::parse("Int32", alloc)?;
    let name_type = TypeAst::parse("String", alloc)?;
    let id_col = ColumnBuilder::fixed(&id_bytes, id_type.view().elem_size(), ids.len())?;
    let name_col = ColumnBuilder::string(&name_offsets, &name_data, names.len())?;
    let mut block = BlockBuilder::new();
    block.append("id", id_type.view(), &id_col)?;
    block.append("name", name_type.view(), &name_col)?;
    client.send_data(Some(&block)).await?;
    client.send_data_end().await?;
    drain(&mut client).await?;

    client
        .send_query("SELECT id, name FROM async_roundtrip ORDER BY id", None)
        .await?;
    let mut rows = Vec::new();
    loop {
        match client.recv_event().await? {
            Event::Data(block) => collect_rows(&block, &mut rows),
            Event::EndOfStream => break,
            Event::Exception(e) => return Err(boxed(e)),
            _ => {}
        }
    }

    assert_eq!(
        rows,
        vec![
            (10, "alpha".to_string()),
            (20, "beta".to_string()),
            (30, "gamma".to_string()),
        ]
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn async_bad_sql_returns_exception() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    let server = ChServer::spawn()?;
    let mut client = connect(&server).await?;
    client
        .send_query("SELECT * FROM definitely_missing_async_table", None)
        .await?;

    loop {
        match client.recv_event().await? {
            Event::Exception(e) => {
                assert_ne!(e.code(), 0);
                assert!(!e.display_text().is_empty());
                return Ok(());
            }
            Event::EndOfStream => panic!("bad SQL ended without exception"),
            _ => {}
        }
    }
}

/// Verifies transport erasure preserves active connection.
#[tokio::test(flavor = "current_thread")]
async fn async_boxed_client_runs_query() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    let server = ChServer::spawn()?;
    let mut client: BoxedAsyncClient = connect(&server).await?.boxed();
    assert!(client.server_info().is_some());
    client.send_query("SELECT 1", None).await?;
    drain(&mut client).await?;

    Ok(())
}

fn string_column(values: &[&str]) -> (Vec<u64>, Vec<u8>) {
    let mut offsets = Vec::with_capacity(values.len());
    let mut data = Vec::new();
    for value in values {
        data.extend_from_slice(value.as_bytes());
        offsets.push(data.len() as u64);
    }
    (offsets, data)
}

fn boxed<E>(e: E) -> Box<dyn std::error::Error>
where
    E: std::error::Error + 'static,
{
    Box::new(e)
}

fn collect_rows(block: &Block, rows: &mut Vec<(i32, String)>) {
    if block.n_rows() == 0 {
        return;
    }
    assert_eq!(block.n_columns(), 2);

    let (id_size, id_bytes) = block.column(0).and_then(|c| c.fixed()).expect("id column");
    assert_eq!(id_size, 4);

    let (name_offsets, name_data) = block
        .column(1)
        .and_then(|c| c.string())
        .expect("name column");

    for row in 0..block.n_rows() {
        let id_start = row * id_size;
        let id = i32::from_le_bytes(id_bytes[id_start..id_start + id_size].try_into().unwrap());
        let name_start = if row == 0 {
            0
        } else {
            name_offsets[row - 1] as usize
        };
        let name_end = name_offsets[row] as usize;
        rows.push((
            id,
            String::from_utf8(name_data[name_start..name_end].to_vec()).unwrap(),
        ));
    }
}

/// Transport that accepts every write and reports EOF on the first read.
struct ClosedReader;

impl AsyncRead for ClosedReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for ClosedReader {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Transport that never accepts a byte.
struct StalledWriter;

impl AsyncRead for StalledWriter {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl AsyncWrite for StalledWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Ok(0))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// A peer that closes before answering Hello must surface as EOF rather than
/// as an endless pump.
#[tokio::test(flavor = "current_thread")]
async fn a_closed_transport_ends_the_handshake() {
    let Err(err) = AsyncClient::handshake_on(ClosedReader, ClientOpts::new(), None).await else {
        panic!("handshake completed without a server");
    };
    assert_eq!(err.kind, ErrorKind::Eof);
}

/// A transport that accepts no bytes must fail instead of spinning on an
/// output queue that never drains.
#[tokio::test(flavor = "current_thread")]
async fn a_transport_that_writes_nothing_is_an_error() {
    let Err(err) = AsyncClient::handshake_on(StalledWriter, ClientOpts::new(), None).await else {
        panic!("handshake completed without a server");
    };
    assert_eq!(err.kind, ErrorKind::Io);
    assert!(err.message.contains("write returned zero"), "{err}");
}

/// Read buffer size comes from client options when set.
#[tokio::test(flavor = "current_thread")]
async fn a_custom_read_buffer_size_is_honoured() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    let server = ChServer::spawn()?;
    let mut opts = ClientOpts::new();
    opts.read_buffer_bytes = 64;
    let mut client = AsyncClient::connect(("127.0.0.1", server.tcp_port), opts, None).await?;

    // Small buffer forces several reads for one result
    client
        .send_query("SELECT number FROM numbers(4096)", None)
        .await?;
    drain(&mut client).await?;
    Ok(())
}

/// Transport-independent core stays reachable for callers that drive their own
/// I/O over an established connection.
#[tokio::test(flavor = "current_thread")]
async fn the_protocol_core_is_reachable() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    let server = ChServer::spawn()?;
    let mut client = connect(&server).await?;
    assert!(client.core().pending_out().is_empty());
    assert!(client.core().server_info().is_some());
    Ok(())
}

// Multi-thread Tokio requires method futures to implement Send
#[allow(dead_code)]
fn method_futures_are_send(mut c: AsyncClient, bb: BlockBuilder<'static>) {
    fn require_send<T: Send>(_: T) {}
    require_send(AsyncClient::connect(("h", 1u16), ClientOpts::new(), None));
    require_send(c.send_query("", None));
    require_send(c.send_data(Some(&bb)));
    require_send(c.send_data_end());
    require_send(c.recv_event());
}

// Custom Tokio transports use same protocol adapter
#[allow(dead_code)]
fn any_tokio_transport_works(pipe: tokio::io::DuplexStream) {
    fn require_send<T: Send>(_: T) {}
    require_send(AsyncClient::handshake_on(pipe, ClientOpts::new(), None));
}
