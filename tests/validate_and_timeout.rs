//! Self-contained coverage for `Column`/`Block::validate`,
//! `PosixIo::set_read_timeout`, and `CancelToken`, over a loopback TCP pair
//! so no external `clickhouse` binary is needed.

use std::net::{TcpListener, TcpStream};
use std::os::fd::AsFd;
use std::time::{Duration, Instant};

use clickhouse_c::{
    Allocator, BlockBuilder, BlockOpts, BlockReader, CancelToken, ColumnBuilder, ErrorKind,
    PosixIo, TypeAst,
};

/// Connected (writer, reader) TCP pair on loopback.
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
    // Block is tiny, fits the socket buffer; closing flushes it to the reader.
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

    // 1 UInt32 row needs 4 bytes; hand in 8. Trailing slack is never read.
    let fixed = [1u8, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef];
    ColumnBuilder::fixed(&fixed, uint32.view().elem_size(), 1).expect("fixed slab with slack");

    // 2 string rows ending at offset 3; data buffer runs past it.
    ColumnBuilder::string(&[1, 3], b"abcdefg", 2).expect("string slab with slack");
}

#[test]
fn read_timeout_fires_on_idle_socket() {
    let alloc = Allocator::stdlib();
    // Keep the server end alive but silent: no bytes, no EOF.
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

    // Clearing the deadline restores blocking semantics for later reads.
    rio.as_mut().set_read_timeout(None);
    drop(writer);
}

/// Write one `UInt32` block of `values` into `sock`.
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

/// A reader mid-stream, cancelled from another thread. The flag is checked
/// before each refill, so the read that needs more bytes is the one that
/// fails.
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

    // Flip from another thread, joined before the next read so the test is
    // about the wiring, not about winning a race.
    std::thread::spawn(move || cancel.cancel())
        .join()
        .expect("flipper thread");

    // No second block is on the wire; without the token this would park in
    // read(2) forever.
    let Err(err) = block_reader.read() else {
        panic!("cancelled reader must not return another block");
    };
    assert_eq!(err.kind, ErrorKind::Cancelled, "got {err:?}");
    drop(writer);
}

/// Pins the documented limit: the flag is read between transport reads, so a
/// read already parked in `read(2)` runs to its deadline first and reports
/// the timeout. The cancel lands on the following attempt.
#[test]
fn cancel_during_a_parked_read_lands_on_the_next_attempt() {
    let alloc = Allocator::stdlib();
    let (writer, reader) = loopback_pair();

    let cancel = CancelToken::new();
    let mut rio = PosixIo::new_cancellable(reader.as_fd(), cancel.clone());
    // Absolute deadline, so it must be armed before BlockReader borrows the
    // backend.
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
