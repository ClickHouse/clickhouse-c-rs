//! Validation, timeout, and cancellation tests using loopback TCP.

use std::net::{TcpListener, TcpStream};
use std::os::fd::AsFd;
use std::time::{Duration, Instant};

use clickhouse_c::{
    Allocator, BlockBuilder, BlockOpts, BlockReader, CancelToken, ColumnBuilder, ErrorKind,
    PosixIo, TypeAst,
};

/// Creates connected loopback TCP pair as writer and reader.
fn loopback_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let writer = TcpStream::connect(addr).expect("connect");
    let (reader, _) = listener.accept().expect("accept");
    (writer, reader)
}

#[test]
fn validate_accepts_roundtripped_block() {
    let alloc = Allocator::stdlib();
    let (writer, reader) = loopback_pair();

    let ty = TypeAst::parse("UInt32", alloc).expect("UInt32");
    let data: [u32; 4] = [1, 2, 3, 4];
    let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();

    let mut wio = PosixIo::new(writer.as_fd());
    let name = String::from("x");
    let col = ColumnBuilder::fixed(&bytes, ty.view().elem_size(), data.len()).expect("fixed");
    let mut bb = BlockBuilder::new();
    bb.append(&name, ty.view(), &col).expect("append");
    bb.write(wio.as_mut(), BlockOpts::default()).expect("write");
    // Small block fits socket buffer before reader starts
    drop(wio);
    drop(writer);

    let mut rio = PosixIo::new(reader.as_fd());
    let block = BlockReader::new(rio.as_mut(), alloc, BlockOpts::default())
        .expect("reader")
        .read()
        .expect("read")
        .expect("a block");

    block.validate().expect("block validates");
    assert_eq!(block.n_columns(), 1);
    assert_eq!(block.column_name(0), Some(&b"x"[..]));
    block
        .column(0)
        .expect("col 0")
        .validate()
        .expect("col validates");
}

#[test]
fn builder_rejects_inconsistent_slabs() {
    let alloc = Allocator::stdlib();
    let uint32 = TypeAst::parse("UInt32", alloc).expect("UInt32");
    let elem = uint32.view().elem_size();

    let err = ColumnBuilder::fixed(&[], elem, 1)
        .map(drop)
        .expect_err("short fixed data");
    assert_eq!(err.kind, ErrorKind::Usage);

    let err = ColumnBuilder::string(&[2, 1], &[0], 2)
        .map(drop)
        .expect_err("decreasing offsets");
    assert_eq!(err.kind, ErrorKind::Usage);

    let leaf = ColumnBuilder::fixed(&[], elem, 0).expect("empty leaf for array");
    let err = leaf
        .array(&[], 1)
        .map(drop)
        .expect_err("short array offsets");
    assert_eq!(err.kind, ErrorKind::Usage);
}

#[test]
fn builder_accepts_oversized_slabs() {
    let alloc = Allocator::stdlib();
    let uint32 = TypeAst::parse("UInt32", alloc).expect("UInt32");

    // Fixed column ignores bytes after required prefix
    let fixed = [1u8, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef];
    ColumnBuilder::fixed(&fixed, uint32.view().elem_size(), 1).expect("fixed slab with slack");

    // String column ignores data after final offset
    ColumnBuilder::string(&[1, 3], b"abcdefg", 2).expect("string slab with slack");
}

#[test]
fn read_timeout_fires_on_idle_socket() {
    let alloc = Allocator::stdlib();
    // Keep peer open without sending data or EOF
    let (writer, reader) = loopback_pair();

    let mut rio = PosixIo::new(reader.as_fd());
    rio.as_mut()
        .set_read_timeout(Some(Duration::from_millis(50)));

    let mut block_reader =
        BlockReader::new(rio.as_mut(), alloc, BlockOpts::default()).expect("reader");
    let start = Instant::now();
    let Err(err) = block_reader.read() else {
        panic!("idle read must hit the deadline, not return a block/EOF");
    };
    let elapsed = start.elapsed();
    drop(block_reader);

    assert_eq!(err.kind, ErrorKind::Io, "got {err:?}");
    assert!(
        elapsed < Duration::from_secs(2),
        "read blocked past the deadline: {elapsed:?}"
    );

    // Clear deadline for later reads
    rio.as_mut().set_read_timeout(None);
    drop(writer);
}

/// Writes one UInt32 block to socket.
fn write_block(sock: &TcpStream, values: &[u32]) {
    let alloc = Allocator::stdlib();
    let ty = TypeAst::parse("UInt32", alloc).expect("UInt32");
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    let col = ColumnBuilder::fixed(&bytes, 4, values.len()).expect("fixed");
    let mut bb = BlockBuilder::new();
    bb.append("x", ty.view(), &col).expect("append");
    let mut io = PosixIo::new(sock.as_fd());
    bb.write(io.as_mut(), BlockOpts::default()).expect("write");
}

#[test]
fn cancel_before_read_fails_without_touching_the_socket() {
    let alloc = Allocator::stdlib();
    let (writer, reader) = loopback_pair();
    write_block(&writer, &[1, 2, 3]);

    let cancel = CancelToken::new();
    cancel.cancel();
    assert!(cancel.is_cancelled());

    let mut rio = PosixIo::new_cancellable(reader.as_fd(), cancel);
    let mut block_reader =
        BlockReader::new(rio.as_mut(), alloc, BlockOpts::default()).expect("reader");
    let Err(err) = block_reader.read() else {
        panic!("a cancelled token must fail the read even with bytes waiting");
    };
    assert_eq!(err.kind, ErrorKind::Cancelled, "got {err:?}");
    drop(writer);
}

/// Verifies cancellation from another thread before next refill.
#[test]
fn cancel_stops_a_reader_between_blocks() {
    let alloc = Allocator::stdlib();
    let (writer, reader) = loopback_pair();
    write_block(&writer, &[7, 8]);

    let cancel = CancelToken::new();
    let mut rio = PosixIo::new_cancellable(reader.as_fd(), cancel.clone());
    let mut block_reader =
        BlockReader::new(rio.as_mut(), alloc, BlockOpts::default()).expect("reader");

    let first = block_reader
        .read()
        .expect("first read")
        .expect("one block on the wire");
    assert_eq!(first.n_rows(), 2);

    // Join cancellation thread before next read
    std::thread::spawn(move || cancel.cancel())
        .join()
        .expect("flipper thread");

    // Cancellation prevents next read from blocking without input
    let Err(err) = block_reader.read() else {
        panic!("cancelled reader must not return another block");
    };
    assert_eq!(err.kind, ErrorKind::Cancelled, "got {err:?}");
    drop(writer);
}

/// Verifies cancellation does not interrupt read already blocked in OS.
#[test]
fn cancel_during_a_parked_read_lands_on_the_next_attempt() {
    let alloc = Allocator::stdlib();
    let (writer, reader) = loopback_pair();

    let cancel = CancelToken::new();
    let mut rio = PosixIo::new_cancellable(reader.as_fd(), cancel.clone());
    // Set absolute deadline before reader borrows transport
    rio.as_mut()
        .set_read_timeout(Some(Duration::from_millis(100)));

    let flipper = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(20));
        cancel.cancel();
    });

    let mut block_reader =
        BlockReader::new(rio.as_mut(), alloc, BlockOpts::default()).expect("reader");
    let Err(parked) = block_reader.read() else {
        panic!("idle read must fail");
    };
    assert_eq!(
        parked.kind,
        ErrorKind::Io,
        "the parked read reports its own timeout, not the cancel: {parked:?}"
    );

    flipper.join().expect("flipper thread");
    let Err(next) = block_reader.read() else {
        panic!("cancelled reader must not return a block");
    };
    assert_eq!(next.kind, ErrorKind::Cancelled, "got {next:?}");
    drop(writer);
}
