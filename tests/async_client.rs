//! Tokio async TCP client coverage over spawned `clickhouse server`.
//!
//! Skips when `clickhouse` is not on PATH.

mod common;

use clickhouse_c::{
    AsyncClient, AsyncTransport, Block, BlockBuilder, BoxedAsyncClient, ClientOpts, ColumnBuilder,
    Event, TypeAst,
};
use common::{ChServer, TestResult, clickhouse_on_path};

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

/// A consumer wanting plaintext or TLS behind one type boxes the
/// transport; the connection keeps working across the erasure.
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
