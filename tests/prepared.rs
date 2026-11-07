//! Prepared Data blocks against a temporary ClickHouse server.
//!
//! Tests skip when `clickhouse` is unavailable.

#![cfg(all(feature = "lz4", feature = "zstd"))]

mod common;

use std::net::TcpStream;

use clickhouse_c::{
    Allocator, BlockBuilder, Client, ClientOpts, Codec, ColumnBuilder, Compression, ErrorKind,
    Event, PosixIo, PreparedBlock, QueryOpts, TypeAst,
};
use common::{ChServer, TestResult, clickhouse_on_path};

/// Server answers in its own configured method, so decode both
fn codec(compression: Compression) -> Option<std::pin::Pin<Box<Codec>>> {
    if compression == Compression::None {
        return None;
    }
    let mut codec = Codec::empty();
    // SAFETY: built-in adapters fill matching slots with valid callbacks
    unsafe {
        let raw = codec.as_mut().raw_mut();
        clickhouse_c::sys::chc_lz4_codec_init(raw);
        clickhouse_c::sys::chc_zstd_codec_init(raw);
    }
    Some(codec)
}

fn connect(server: &ChServer, compression: Compression) -> TestResult<Client<'static>> {
    let sock = TcpStream::connect(("127.0.0.1", server.tcp_port))?;
    Ok(Client::init(
        &ClientOpts::new().compression(compression),
        Allocator::stdlib(),
        PosixIo::new_owned(sock),
        codec(compression),
    )?)
}

/// `n` rows of ids from `first` with repeating text
fn prepare(first: u64, n: usize, compression: Compression) -> TestResult<PreparedBlock> {
    let alloc = Allocator::stdlib();
    let (u64_ty, str_ty) = (
        TypeAst::parse("UInt64", alloc)?,
        TypeAst::parse("String", alloc)?,
    );
    let ids: Vec<u8> = (first..first + n as u64)
        .flat_map(u64::to_le_bytes)
        .collect();
    let mut data = Vec::new();
    let offsets: Vec<u64> = (0..n)
        .map(|i| {
            data.extend_from_slice(&b"walshadow"[..i % 9]);
            data.len() as u64
        })
        .collect();
    let id = ColumnBuilder::fixed(&ids, 8, n)?;
    let text = ColumnBuilder::string(&offsets, &data, n)?;
    let mut block = BlockBuilder::new();
    block.append("id", u64_ty.view(), &id)?;
    block.append("text", str_ty.view(), &text)?;
    let codec = codec(compression);
    Ok(block.prepare(compression, codec.as_ref().map(|c| c.as_ref()), alloc)?)
}

fn insert(client: &mut Client<'_>, blocks: &[PreparedBlock]) -> TestResult {
    client.send_query_with("INSERT INTO prepared (id, text) VALUES", &QueryOpts::new())?;
    loop {
        match client.recv_event()? {
            Event::Data(_) => break,
            Event::Exception(exc) => return Err(exc.into()),
            Event::EndOfStream => return Err("no header block".into()),
            _ => {}
        }
    }
    for b in blocks {
        client.send_prepared(b)?;
    }
    client.send_data(None)?;
    loop {
        match client.recv_event()? {
            Event::EndOfStream => return Ok(()),
            Event::Exception(exc) => return Err(exc.into()),
            _ => {}
        }
    }
}

/// Blocks prepared once insert under one query, then again on a fresh
/// connection, as a retried INSERT would, in every compression mode
#[test]
fn prepared_blocks_insert_and_resend() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("skipping: clickhouse not on PATH");
        return Ok(());
    }
    let server = ChServer::spawn()?;
    server.query("CREATE TABLE prepared (id UInt64, text String) ENGINE = Memory")?;
    for (i, compression) in [Compression::None, Compression::Lz4, Compression::Zstd]
        .into_iter()
        .enumerate()
    {
        // Larger than one 64 KiB compression frame
        let first = 1_000_000 * i as u64;
        let blocks = [
            prepare(first, 50_000, compression)?,
            prepare(first + 50_000, 3, compression)?,
        ];
        assert_eq!(blocks[0].n_rows(), 50_000);
        assert!(blocks[0].raw_len() > 65_535);
        if compression != Compression::None {
            assert!(blocks[0].as_bytes().len() < blocks[0].raw_len());
        }
        for _ in 0..2 {
            insert(&mut connect(&server, compression)?, &blocks)?;
        }
    }
    let text_len = |n: usize| (0..n).map(|i| i % 9).sum::<usize>();
    let got = server.query("SELECT count(), uniqExact(id), sum(length(text)) FROM prepared")?;
    assert_eq!(
        got.trim(),
        format!(
            "{}\t{}\t{}",
            3 * 2 * 50_003,
            3 * 50_003,
            3 * 2 * (text_len(50_000) + text_len(3))
        )
    );
    Ok(())
}

/// Connection negotiated with other compression refuses the block before
/// writing, leaving the connection usable
#[test]
fn mismatched_compression_rejects_without_writing() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("skipping: clickhouse not on PATH");
        return Ok(());
    }
    let server = ChServer::spawn()?;
    server.query("CREATE TABLE prepared (id UInt64, text String) ENGINE = Memory")?;
    let mut client = connect(&server, Compression::Lz4)?;
    let zstd = prepare(0, 10, Compression::Zstd)?;
    let err = client
        .send_prepared(&zstd)
        .expect_err("compression differs");
    assert_eq!(err.kind, ErrorKind::Usage);
    insert(&mut client, &[prepare(0, 10, Compression::Lz4)?])?;
    assert_eq!(server.query("SELECT count() FROM prepared")?.trim(), "10");
    Ok(())
}

/// Async sends write block bytes past the queue; mismatch still refuses
#[cfg(feature = "tokio")]
#[tokio::test(flavor = "current_thread")]
async fn async_prepared_blocks_insert() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("skipping: clickhouse not on PATH");
        return Ok(());
    }
    let server = ChServer::spawn()?;
    server.query("CREATE TABLE prepared (id UInt64, text String) ENGINE = Memory")?;
    let blocks = [
        prepare(0, 50_000, Compression::Lz4)?,
        prepare(50_000, 3, Compression::Lz4)?,
    ];
    let opts = ClientOpts::new().compression(Compression::Lz4);
    let addr = ("127.0.0.1", server.tcp_port);
    let mut client =
        clickhouse_c::AsyncClient::connect(addr, opts, codec(Compression::Lz4)).await?;
    let err = client
        .send_prepared(&prepare(0, 10, Compression::Zstd)?)
        .await
        .expect_err("compression differs");
    assert_eq!(err.kind, ErrorKind::Usage);
    client
        .send_query("INSERT INTO prepared (id, text) VALUES", None)
        .await?;
    while !matches!(client.recv_event().await?, Event::Data(_)) {}
    for b in &blocks {
        client.send_prepared(b).await?;
    }
    client.send_data_end().await?;
    loop {
        match client.recv_event().await? {
            Event::EndOfStream => break,
            Event::Exception(exc) => return Err(exc.into()),
            _ => {}
        }
    }
    assert_eq!(
        server.query("SELECT count() FROM prepared")?.trim(),
        "50003"
    );
    Ok(())
}

#[test]
fn compression_needs_a_matching_codec() {
    let alloc = Allocator::stdlib();
    let block = BlockBuilder::new();
    let lz4 = Codec::lz4();
    let err = block
        .prepare(Compression::Zstd, Some(lz4.as_ref()), alloc)
        .err()
        .expect("lz4 codec cannot zstd");
    assert_eq!(err.kind, ErrorKind::Usage);
    assert!(block.prepare(Compression::Lz4, None, alloc).is_err());
    let end = PreparedBlock::end(Compression::None, None, alloc).unwrap();
    assert_eq!(end.n_rows(), 0);
    assert!(!end.as_bytes().is_empty());
    assert!(end.capacity() >= end.as_bytes().len());
}
