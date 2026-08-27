//! Compile check for the snippets in `docs/index.mdx`.
//!
//! Those docs are published separately, so nothing else here would catch them
//! drifting from the API. Each function below is the snippet with its
//! surrounding `fn` and imports filled in; most are never called, because
//! compiling is the assertion.

#![allow(dead_code)]

use std::net::TcpStream;
use std::os::fd::AsFd;
use std::process::{Command, Stdio};

use clickhouse_c::{
    Allocator, Block, BlockBuilder, BlockOpts, BlockReader, CancelToken, Client, ClientOpts, Codec,
    Column, ColumnBuilder, ColumnLayout, Compression, Event, Kind, PosixIo, QueryOpts, QueryParam,
    QuerySetting, TypeAst, TypeRef,
};

type DocResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn print_block(_block: &Block) {}

// ---- Connecting over TCP -------------------------------------------------

fn connecting_over_tcp() -> DocResult {
    let sock = TcpStream::connect("localhost:9000")?;
    sock.set_nodelay(true).ok();

    let opts = ClientOpts::new()
        .user("default")
        .password("")
        .database("default")
        .client_name("my-service");

    let mut client = Client::init(
        &opts,
        Allocator::stdlib(),
        PosixIo::new_owned(sock),
        None, // no codec: uncompressed
    )?;

    let info = client.server_info().expect("handshake completed");
    println!(
        "connected to {} {}.{}.{}",
        info.display_name, info.version_major, info.version_minor, info.version_patch
    );
    let _ = &mut client;
    Ok(())
}

// ---- Running a query -----------------------------------------------------

fn running_a_query(client: &mut Client<'_>) -> DocResult {
    let settings = [
        QuerySetting::TEXT_TYPE_NAMES,
        QuerySetting::new("max_block_size", "8192"),
        // `important` makes the server reject a setting it does not recognize
        // instead of ignoring it.
        QuerySetting::new("max_execution_time", "30").important(),
    ];
    let params = [QueryParam::new("cutoff", "'100'")];

    client.send_query_with(
        "SELECT number, toString(number * number) \
         FROM numbers(1000) WHERE number > {cutoff:UInt64}",
        &QueryOpts::new().settings(&settings).params(&params),
    )?;

    loop {
        match client.recv_event()? {
            Event::Data(block) => print_block(&block),
            Event::Exception(exc) => return Err(exc.into()),
            Event::EndOfStream => break,
            _ => {} // Progress, ProfileInfo, Log, ...
        }
    }
    Ok(())
}

// ---- Reading column data -------------------------------------------------

fn print_value(ty: TypeRef<'_>, col: Column<'_>, row: usize) {
    if let Some(ColumnLayout::Nullable) = col.layout() {
        if col.null_map().is_some_and(|m| m[row] == 1) {
            print!("\\N");
            return;
        }
        let (inner_ty, inner) = (ty.child(0), col.nullable_inner());
        if let (Some(inner_ty), Some(inner)) = (inner_ty, inner) {
            print_value(inner_ty, inner, row);
        }
        return;
    }

    match col.layout() {
        Some(ColumnLayout::Fixed) => {
            // A raw little-endian slab: slice the row out and convert, rather
            // than casting the pointer, so alignment is never an issue.
            let Some((elem_size, bytes)) = col.fixed() else {
                return;
            };
            let cell = &bytes[row * elem_size..][..elem_size];
            match ty.kind() {
                Some(Kind::UInt64) => {
                    print!("{}", u64::from_le_bytes(cell.try_into().unwrap()))
                }
                Some(Kind::Int32) => {
                    print!("{}", i32::from_le_bytes(cell.try_into().unwrap()))
                }
                Some(Kind::Float64) => {
                    print!("{}", f64::from_le_bytes(cell.try_into().unwrap()))
                }
                _ => {}
            }
        }
        Some(ColumnLayout::String) => {
            let Some((offsets, data)) = col.string() else {
                return;
            };
            let start = if row == 0 {
                0
            } else {
                offsets[row - 1] as usize
            };
            print!(
                "{}",
                String::from_utf8_lossy(&data[start..offsets[row] as usize])
            );
        }
        _ => {}
    }
}

// ---- Inserting data ------------------------------------------------------

fn inserting_data(client: &mut Client<'_>) -> DocResult {
    client.send_query_with(
        "INSERT INTO greetings (id, message) VALUES",
        &QueryOpts::new(),
    )?;

    // Wait for the header block: schema, zero rows.
    loop {
        match client.recv_event()? {
            Event::Data(_) => break,
            Event::Exception(exc) => return Err(exc.into()),
            Event::EndOfStream => return Err("no header block".into()),
            _ => {}
        }
    }

    let alloc = Allocator::stdlib();
    let u64_ty = TypeAst::parse("UInt64", alloc)?;
    let str_ty = TypeAst::parse("String", alloc)?;

    let ids: Vec<u8> = [1u64, 2, 3].iter().flat_map(|v| v.to_le_bytes()).collect();
    let id = ColumnBuilder::fixed(&ids, 8, 3)?;

    // String columns: cumulative exclusive end offsets over a packed slab.
    let offsets = [5u64, 11, 20];
    let bytes = b"hellobuenasgoedendag";
    let message = ColumnBuilder::string(&offsets, bytes, 3)?;

    let mut block = BlockBuilder::new();
    block.append("id", u64_ty.view(), &id)?;
    block.append("message", str_ty.view(), &message)?;

    client.send_data(Some(&block))?; // the populated block
    client.send_data(None)?; // empty block ends the INSERT

    loop {
        match client.recv_event()? {
            Event::EndOfStream => break,
            Event::Exception(exc) => return Err(exc.into()),
            _ => {}
        }
    }
    Ok(())
}

fn nesting_composites(
    values: &[u8],
    null_map: &[u8],
    array_offsets: &[u64],
    array_ty: &TypeAst,
) -> DocResult {
    let leaf = ColumnBuilder::fixed(values, 4, 4)?;
    let nullable = leaf.nullable(null_map)?; // borrows leaf
    let array = nullable.array(array_offsets, 3)?; // borrows nullable
    let mut block = BlockBuilder::new();
    block.append("v", array_ty.view(), &array)?; // borrows array until the write
    Ok(())
}

// ---- Reading Native without a server -------------------------------------

fn native_without_a_server() -> DocResult {
    let mut child = Command::new("clickhouse")
        .args([
            "local",
            "--format",
            "Native",
            "--output_format_native_encode_types_in_binary_format=0",
            "-q",
            "SELECT number FROM numbers(5)",
        ])
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().expect("piped");
    let mut io = PosixIo::new(stdout.as_fd());

    // One reader across every read: bytes pulled past a block boundary stay
    // buffered, so a multi-block result decodes without dropping the tail.
    let mut reader = BlockReader::new(io.as_mut(), Allocator::stdlib(), BlockOpts::default())?;
    while let Some(block) = reader.read()? {
        print_block(&block);
    }
    Ok(())
}

// ---- Compression ---------------------------------------------------------

fn compression(io: core::pin::Pin<Box<PosixIo<'static>>>) -> DocResult {
    let opts = ClientOpts::new()
        .user("default")
        .compression(Compression::Lz4);
    let client = Client::init(&opts, Allocator::stdlib(), io, Some(Codec::lz4()))?;
    drop(client);
    Ok(())
}

// ---- TLS -----------------------------------------------------------------

fn tls_client() -> DocResult {
    use clickhouse_c::tls;

    let tcp = TcpStream::connect(("myhost.clickhouse.cloud", 9440))?;
    tcp.set_nodelay(true).ok();

    // Verifies the chain and the SNI hostname against the Mozilla webpki roots.
    let io = tls::TlsIo::connect(tcp, "myhost.clickhouse.cloud", tls::default_config())?;
    let mut client = Client::init(
        &ClientOpts::new().user("default").password("…"),
        Allocator::stdlib(),
        io,
        None,
    )?;
    let _ = &mut client;
    Ok(())
}

// ---- Async client --------------------------------------------------------

async fn async_client() -> DocResult {
    use clickhouse_c::AsyncClient;

    let mut client =
        AsyncClient::connect(("127.0.0.1", 9000), ClientOpts::new().user("default"), None).await?;

    client
        .send_query("SELECT number FROM numbers(5)", None)
        .await?;
    loop {
        match client.recv_event().await? {
            Event::Data(block) => print_block(&block),
            Event::Exception(exc) => return Err(exc.into()),
            Event::EndOfStream => break,
            _ => {}
        }
    }
    Ok(())
}

async fn async_client_boxed_transport(secure: bool) -> DocResult {
    use clickhouse_c::{AsyncClient, BoxedAsyncClient, tls};

    let opts = ClientOpts::new().user("default").password("…");
    let mut client: BoxedAsyncClient = if secure {
        AsyncClient::connect_tls(
            ("myhost", 9440),
            "myhost",
            opts,
            None,
            tls::default_config(),
        )
        .await?
        .boxed()
    } else {
        AsyncClient::connect(("127.0.0.1", 9000), opts, None)
            .await?
            .boxed()
    };
    let _ = &mut client;
    Ok(())
}

// ---- Any other runtime ---------------------------------------------------

fn ioless_over_a_blocking_socket() -> DocResult {
    use clickhouse_c::{IolessClient, Step};
    use std::io::{Read, Write};

    let mut sock = TcpStream::connect("localhost:9000")?;
    let mut core = IolessClient::new(&ClientOpts::new(), Allocator::stdlib(), None)?;
    let mut buf = [0u8; 8192];

    // Push everything queued, then read once. Flushing before the read is not
    // optional: a step that reports NeedsInput has usually just queued the
    // bytes the server is waiting on, and reading first deadlocks both sides.
    let mut pump = |core: &mut IolessClient, sock: &mut TcpStream| -> clickhouse_c::Result<()> {
        while !core.pending_out().is_empty() {
            let n = sock.write(core.pending_out())?;
            core.consume_out(n);
        }
        let n = sock.read(&mut buf)?;
        core.submit(&buf[..n])
    };

    while !core.handshake()?.is_ready() {
        pump(&mut core, &mut sock)?;
    }

    core.send_query("SELECT number FROM numbers(5)", None)?;
    loop {
        match core.recv_event()? {
            Step::Ready(Event::EndOfStream) => break,
            Step::Ready(Event::Data(block)) => print_block(&block),
            Step::Ready(_) => {}
            Step::NeedsInput => pump(&mut core, &mut sock)?,
        }
    }
    Ok(())
}

// ---- Cancellation --------------------------------------------------------

fn cancellation(sock: TcpStream) {
    let cancel = CancelToken::new();
    let io = PosixIo::new_owned_cancellable(sock, cancel.clone());
    // ... from another task or thread:
    cancel.cancel();
    drop(io);
}

// ---- Allocator -----------------------------------------------------------

static ARENA: std::alloc::System = std::alloc::System;

fn custom_allocator() -> Allocator {
    Allocator::global(&ARENA)
}

/// The one snippet cheap enough to run: it needs no server, and it exercises
/// the reader path the docs' column table describes.
#[test]
fn column_table_accessors_match_the_docs() {
    let alloc = Allocator::stdlib();
    let ty = TypeAst::parse("String", alloc).expect("String");
    let offsets = [5u64, 11, 20];
    let data = b"hellobuenasgoedendag";
    let col = ColumnBuilder::string(&offsets, data, 3).expect("string column");
    let mut block = BlockBuilder::new();
    block.append("message", ty.view(), &col).expect("append");

    // Round-trip so `print_value` runs against a decoded column, not a
    // builder node.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let writer = TcpStream::connect(listener.local_addr().expect("addr")).expect("connect");
    let (reader, _) = listener.accept().expect("accept");
    let mut wio = PosixIo::new(writer.as_fd());
    block
        .write(wio.as_mut(), BlockOpts::default())
        .expect("write");
    drop(wio);
    drop(writer);

    let mut rio = PosixIo::new(reader.as_fd());
    let decoded = BlockReader::new(rio.as_mut(), alloc, BlockOpts::default())
        .expect("reader")
        .read()
        .expect("read")
        .expect("a block");

    let column = decoded.column(0).expect("column 0");
    let (got_offsets, got_data) = column.string().expect("string layout");
    assert_eq!(got_offsets, &offsets[..]);
    assert_eq!(got_data, &data[..]);
    for row in 0..decoded.n_rows() {
        print_value(decoded.column_type(0).expect("type"), column, row);
    }
    println!();
}
