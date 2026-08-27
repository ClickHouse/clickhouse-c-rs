# clickhouse-c-rs

Rust bindings for [clickhouse-c], a header-only C client for the
ClickHouse Native wire format. Two entry points:

- raw block frames over any transport implementing the `Io` trait
  (`PosixIo` covers a TCP socket or a pipe to `clickhouse local`)
- TCP packet loop (Hello / Query / Data / EOS / Exception / Progress)
  with optional LZ4 / ZSTD compression
- Tokio async TCP packet loop with feature `tokio`
- TLS (rustls) for both the blocking and async clients with feature `tls`

[clickhouse-c]: https://github.com/ClickHouse/clickhouse-c

```toml
[dependencies]
clickhouse-c-rs = "0.1"
```

No features are on by default, so the base crate needs only a C compiler
and libc. The `lz4` and `zstd` features each link a system library and
need its development package (`liblz4-dev` / `libzstd-dev` on Debian,
`lz4` / `zstd` on Homebrew); `build.rs` locates them with `pkg-config`.

## Architecture

1. **Vendored headers** under `clickhouse-c/`, pinned by
   `clickhouse-c/UPSTREAM`. Override location with
   `CHC_INCLUDE_DIR=<path>`.
2. **`src/wrapper.c`** — single TU that `#define`s `CHC_IMPLEMENTATION`
   & includes each header the configured features select. `build.rs`
   compiles it via the `cc` crate into a static library. LZ4 / ZSTD
   link separately under their feature flags.
3. **`src/sys.rs`** — FFI declarations for the public symbols &
   structs of `clickhouse.h`, `clickhouse-posix-io.h`,
   `clickhouse-compression.h`, `clickhouse-client.h`, and
   `clickhouse-async.h`. `src/parity.rs` checks that claim against the
   headers on every test run, so an upstream bump that adds a function
   or an enum member fails a test rather than going unnoticed.
   `clickhouse-openssl.h` is shipped but not bound: it is there for
   callers wiring their own OpenSSL `chc_io`. Integer constants from
   `enum` blocks
   (`chc_kind`, `chc_col_kind`, `chc_compression`, `chc_packet_kind`,
   error codes) & a couple of `#define`s are scanned out of the
   headers by `build.rs` into `$OUT_DIR/sys_constants.rs` & pulled in
   via `include!`.
4. **Safe wrappers** in `src/{error,alloc,io,types,block,builder,codec,client,query}.rs`.
   Each owning C struct gets a Drop impl that calls the matching
   `chc_*_destroy` / `_close` / `_free`. Borrowed views ride lifetimes
   tied to their owner.

## Safety model

**Trusted base.** Soundness of every non-`unsafe` API is conditional on
`clickhouse-c`, at the revision in `clickhouse-c/UPSTREAM`, holding the
invariants its headers document — chiefly that the `chc_column`-side
length counters (`n_rows`, `offsets.last()`, `name_len`) match the buffer
the same struct points at. Where a length is available from two fields the
smaller wins, so a C-side bug truncates rather than reading out of bounds;
where only a `debug_assert!` is possible it trips in debug builds and
release trusts the C side. `clickhouse_c::UPSTREAM_REVISION` reports the
bundled revision at runtime.

**Decoded blocks.** Decoding bounds every slice by the owning column's own
row count, so the accessors are safe on any input, however forged.
Agreement *between* columns — array offsets non-decreasing, LowCardinality
keys inside the dictionary — is not checked, because the cost is
proportional to row count and most readers never index by them. Code that
does should call `Block::validate` first on anything a peer sent.

**Allocators thread through every owning constructor.** `chc_alloc` is
a vtable. `Allocator` wraps it `Copy + Send + Sync`. `TypeAst` /
`Block` / `Client` each take an `Allocator` at construction & store it;
`Drop` calls the matching destroy with the same allocator the C side
used. `BlockBuilder` needs none — it owns caller-side `chc_block_col`
storage as a `Vec` and the C writer allocates nothing. `Client` boxes
its `Allocator` so the
heap address the C side stashes in `c->al` stays valid through every
later call & through `chc_client_close`.

**No-copy columns.** The writer builds each column as a `chc_column`
tree (`chc_build_fixed` / `_string` / `_nullable` / `_array` / `_tuple`
/ `_lc`) whose nodes retain raw pointers into caller-owned slabs; the
tree and slabs must outlive the write. Mirrored as `ColumnBuilder<'a>`:
leaves (`fixed`, `string`) compose with wrappers (`nullable`, `array`,
`low_cardinality`, `tuple`) to match any composite the reader emits,
e.g. `Array(Nullable(UInt32))`. Caller owns each node; wrappers borrow
child nodes and keep them immovable until the write completes.
`BlockBuilder::append(name, ty, col)` records one built column against a
`&'a str` name and `TypeRef<'a>`; the C writer checks the tree matches
`ty` structurally. Caller keeps inputs alive for `'a`.

**Self-referential C structs.** `chc_io` carries a pointer back into the
`chc_posix_io` state it was initialized from; `PosixIo` holds both inline
& lets `chc_posix_io_init` wire the back-pointer, so it is genuinely
pinned (`PhantomPinned`) — mirroring how `TlsIo` embeds a `chc_io` whose
`ud` points at its own rustls stream. `chc_codec` is addressed by
compression code calling into its function-pointer table, so `Codec` is
likewise pinned. All ship behind `Pin<Box<Self>>` & expose internals
through `Pin<&mut _>` / `Pin<&_>`: `PosixIo` for ownership-passing into
[`Client`] / [`Block`] / [`BlockBuilder`], `Codec` because it must not
move. `Codec::raw_mut` is `unsafe`: caller must
populate the function-pointer table to match the [`Compression`] the
codec is paired with.

**One `Io` trait for every path.** `Client`, `BlockReader`, and
`BlockBuilder::write` all take a `Pin<&mut impl Io>`, so a backend written
once serves all three. `PosixIo` and `tls::TlsIo` ship with the crate; a
consumer can add OpenSSL through `clickhouse-openssl.h`, an in-memory
buffer, or their own event loop by implementing the trait. It is `unsafe`
because `io_ptr`'s return goes straight to C: see `tests/custom_io.rs` for
a worked backend.

**`Client` owns its I/O + codec.** `chc_client` stashes raw pointers
to `chc_io` & `chc_codec` for the connection's lifetime; using
borrowed references would let safe code drop them out from under the C
side. `Client::init` takes `Pin<Box<PosixIo<'fd>>>` &
`Option<Pin<Box<Codec>>>` by ownership so the back-pointers stay valid
through `Drop`. `Client<'fd>` carries the fd lifetime; constructed via
`PosixIo::new(fd.as_fd())` it ties the client to a borrowed fd, or via
`PosixIo::new_owned(fd_owner)` it takes the fd and closes it on drop.

**C-side strings.** `chc_err.msg` is a fixed-size char buffer;
`Error::from_raw` copies it through `from_utf8_lossy` because the C
struct goes out of scope at the call boundary. `chc_exception` is a
heap chain in the C allocator; [`Exception`] is a thin owning wrapper
over the head pointer, accessors return `&[u8]` borrowed from C
memory, and `Drop` calls `chc_exception_free` to walk & release the
chain. Server-controlled text accessors on `Block` / `TypeRef`
(`column_name`, `name`, `timezone`, `enum_at`, `tuple_field_name`)
likewise return `&[u8]` so the UTF-8 question stays at the
consumer; `TypeRef::format` is the one place a `String` is materialized
& uses `from_utf8_lossy`.

**Packet payloads alias a union.** `chc_packet` is a `kind` tag plus a
`payload` union — `block`, `exception`, `progress` and `profile` share
one slot, mirroring the C header. Exactly one arm is live, selected by
`kind`; reading any other is UB. `chc_packet_payload` therefore makes
every read `unsafe`, and a single reader — `Event::from_raw`, shared by
the blocking `Client` and the async client — converts a recv'd packet
into an owned `Event`, reading each arm only inside its `kind` match. A
new `chc_packet` member must be a union arm, never a parallel struct
field: a field laid out past the union's offset reads zero for every
packet, silently turning exception payloads into NULL.

**Send / Sync.** Every owning handle is `Send`. `Client`, `Block`,
`TypeAst`, `Exception`, and the I/O backends are not `Sync`: each
`chc_client` is single-threaded upstream and the rest follow. The
exceptions are `Allocator` (a stateless function-pointer vtable) and
`BlockBuilder` / `ColumnBuilder`, where a shared reference only exposes
reads — which is what lets the async client hold a `&BlockBuilder` across
the `send_data` await. `AsyncClient` is `Send`, and its method futures
stay `Send` because no raw FFI pointer is held across an `.await`: each
`chc_async_*` call resolves the C-owned slice or pointer in a tight scope
and awaits only on the copied `&[u8]`.

## Quickstart

### Decode `clickhouse local`'s stdout

```rust
use clickhouse_c::{Allocator, BlockOpts, BlockReader, PosixIo};
use std::os::fd::AsFd;
use std::process::{Command, Stdio};

let mut child = Command::new("clickhouse")
    .args(["local", "--format", "Native",
           "--output_format_native_encode_types_in_binary_format=0",
           "-q", "SELECT number FROM numbers(5)"])
    .stdout(Stdio::piped())
    .spawn()?;
let stdout = child.stdout.take().unwrap();
let mut io = PosixIo::new(stdout.as_fd());

let alloc = Allocator::stdlib();
// One reader across all reads: bytes read past a block boundary stay
// buffered, so multi-block results decode without dropping the tail.
let mut reader = BlockReader::new(io.as_mut(), alloc, BlockOpts::default())?;
while let Some(block) = reader.read()? {
    // block.n_rows(), block.column(i).fixed() / .string() / ...
}
drop(reader);
drop(io);
drop(stdout);     // close pipe
child.wait()?;
```

`clickhouse local` emits Native without `BlockInfo` or
`has_custom_serialization`, so `BlockOpts::default()` is correct. TCP
needs both flags depending on negotiated server revision.

### Encode a block & feed it back in

```rust
use clickhouse_c::{Allocator, BlockBuilder, BlockOpts, ColumnBuilder, PosixIo, TypeAst};
use std::os::fd::AsFd;
use std::process::{Command, Stdio};

let mut child = Command::new("clickhouse")
    .args(["local", "--input-format", "Native", "--structure", "x UInt32",
           "-q", "SELECT sum(x) FROM table"])
    .stdin(Stdio::piped())
    .spawn()?;
let stdin = child.stdin.take().unwrap();
let mut io = PosixIo::new(stdin.as_fd());

let alloc = Allocator::stdlib();
let ty = TypeAst::parse("UInt32", alloc)?;
let data: Vec<u32> = (0..1000).collect();
let bytes: &[u8] = unsafe {
    core::slice::from_raw_parts(data.as_ptr().cast(), std::mem::size_of_val(&data[..]))
};

let mut bb = BlockBuilder::new();
let col = ColumnBuilder::fixed(bytes, ty.view().elem_size(), data.len())?;
bb.append("x", ty.view(), &col)?;
bb.write(io.as_mut(), BlockOpts::default())?;
drop(io);
drop(stdin);      // EOF for the child
child.wait()?;
```

ClickHouse Native is little-endian on the wire & `ColumnBuilder::fixed`
expects LE bytes. Big-endian hosts swap before build.

### TCP client

```rust
use clickhouse_c::{Allocator, Client, ClientOpts, Codec, Compression, Event, PosixIo};
use std::net::TcpStream;

let sock = TcpStream::connect("localhost:9000")?;
// `Client` will own the fd through `PosixIo::new_owned` and close it
// on drop. For a borrowed-fd variant, keep `sock` in scope and pass
// `PosixIo::new(sock.as_fd())` — `Client<'_>` then borrows from `sock`.
let io = PosixIo::new_owned(sock);

let codec = Codec::lz4();        // feature = "lz4"
let mut opts = ClientOpts::new()
    .database("default")
    .user("default")
    .password("");
opts.compression = Compression::Lz4;

let mut client = Client::init(&opts, Allocator::stdlib(), io, Some(codec))?;
// Refresh before each blocking operation to apply a fresh absolute deadline:
// client.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;

client.send_query("INSERT INTO t FORMAT Native", None)?;
// send one or more data blocks via client.send_data(Some(&bb)),
// then close the INSERT with the empty terminator:
client.send_data(None)?;

loop {
    match client.recv_event()? {
        Event::EndOfStream => break,
        Event::Exception(exc) => return Err(exc.into()),
        _ => {}
    }
}
```

### Per-query settings & parameters

`send_query_with` carries a `SETTINGS` list and `{name:Type}` parameters.
Pin `QuerySetting::TEXT_TYPE_NAMES` on every query: the decoder reads
printable type names off the wire, and a server profile that switches
them to binary would otherwise break decoding.

Parameter values are single-quoted whatever the placeholder type — the
server unquotes, then parses the text as `Type`.

```rust,ignore
use clickhouse_c::{QueryOpts, QueryParam, QuerySetting};

let settings = [
    QuerySetting::TEXT_TYPE_NAMES,
    QuerySetting::new("max_block_size", "8192"),
    // `important` makes the server reject a setting it does not know
    // instead of ignoring it.
    QuerySetting::new("max_execution_time", "30").important(),
];
let params = [QueryParam::new("cutoff", "'100'")];

client.send_query_with(
    "SELECT number FROM numbers(1000) WHERE number > {cutoff:UInt64}",
    &QueryOpts::new().settings(&settings).params(&params),
)?;
```

clickhouse-c publishes no `chc_async_send_query_ex`, so `AsyncClient` has
no counterpart yet.

### Cancellation

Two separate things, often wanted together:

- `CancelToken` fails local reads. Hand a clone to
  `PosixIo::new_cancellable`; clickhouse-c checks it before each transport
  read, so pair it with `set_read_timeout` to bound a read already parked
  in `read(2)`. Nothing goes over the wire.
- `Client::send_cancel` sends the protocol Cancel packet so the server
  stops producing. Packets already in flight still arrive, so keep
  draining to `EndOfStream`.

### TLS (feature `tls`)

rustls verifies the peer against `tls::default_config()` (Mozilla webpki
roots, no client auth). `rustls` is re-exported as `clickhouse_c::tls::rustls`
so callers can build a bespoke `ClientConfig` (private CA, mTLS) and pass it
in. The native secure port is `9440`.

Async — wraps the `tokio::net::TcpStream` in a TLS stream (also needs
feature `tokio`):

```rust,ignore
use clickhouse_c::{AsyncClient, ClientOpts};

let mut client = AsyncClient::connect_tls(
    ("myhost.clickhouse.cloud", 9440),
    "myhost.clickhouse.cloud",          // SNI + cert hostname
    ClientOpts::new().user("default").password("…"),
    None,                               // or Some(Codec::lz4())
    clickhouse_c::tls::default_config(),
).await?;
```

Blocking — `tls::TlsIo` is a `Io` backend over an owned `TcpStream`;
hand it to the same `Client::init` the plaintext path uses:

```rust,ignore
use clickhouse_c::{Allocator, Client, ClientOpts, tls};
use std::net::TcpStream;

let tcp = TcpStream::connect(("myhost.clickhouse.cloud", 9440))?;
tcp.set_nodelay(true).ok();
let io = tls::TlsIo::connect(tcp, "myhost.clickhouse.cloud", tls::default_config())?;
let mut client = Client::init(
    &ClientOpts::new().user("default").password("…"),
    Allocator::stdlib(),
    io,
    None,
)?;
```

## Feature flags

All off by default.

| Feature | Effect | Needs |
|---|---|---|
| `lz4`   | compile clickhouse-compression.h's LZ4 wrapper, link `-llz4`, expose `Codec::lz4()` | system `liblz4` |
| `tls`   | rustls TLS: `tls::TlsIo` backend for the blocking `Client`, `AsyncClient::connect_tls`, `tls::default_config()` (webpki roots) | `rustls`, `webpki-roots`, `tokio-rustls` |
| `tokio` | expose `AsyncClient` over `tokio::net::TcpStream` | `tokio` |
| `zstd`  | compile clickhouse-compression.h's ZSTD wrapper, link `-lzstd`, expose `Codec::zstd()` | system `libzstd` |

Async TLS needs both `tls` and `tokio`.

## Header vendoring

Headers live under `clickhouse-c/` so the crate builds straight from a
`git clone` or a published archive; `clickhouse-c/UPSTREAM` records the
repository and revision they came from. Build against an out-of-tree
checkout with:

```sh
CHC_INCLUDE_DIR=/abs/path/to/clickhouse-c cargo build
```

## Supported platforms

Unix only, and `build.rs` says so rather than failing in the C compiler.
`PosixIo` is the bundled transport and it needs `poll(2)`; the block and
client layers themselves are portable, so a Windows port is a matter of
writing a Windows `Io` backend. CI runs Linux and macOS on x86-64 and
aarch64.

ClickHouse Native is little-endian on the wire. Offsets and
LowCardinality keys are byte-swapped to host order at decode time; fixed
column data is not, so a big-endian host swaps multi-byte scalars itself
in both directions.

MSRV is 1.85, checked in CI.

## Non-goals

Mirrors upstream's list plus Rust-specific items:

- HTTP — wrap libcurl or a Rust HTTP client
- DNS, endpoint round-robin, pooling, retry / backoff — caller-driven;
  `PosixIo` only wraps a connected fd
- TLS beyond rustls — the `tls` feature ships a rustls backend
  (`tls::TlsIo` / `AsyncClient::connect_tls`); for a different stack the
  caller can still drive OpenSSL through a custom `chc_io`
  (`clickhouse-openssl.h`) or hand `connect_tls` a bespoke
  `rustls::ClientConfig`
- Threading — each `Client` is single-threaded, matching upstream
- Runtime-neutral Rust async — `AsyncClient` is Tokio-native; custom
  event loops can drive `chc_async_*` through `sys`
- `Variant` / `Dynamic` / `JSON` / `AggregateFunction` decoding —
  upstream excludes from v1 (25.x / 26.x wire format still shifting).
  A `ColumnBuilder::string` column under a `JSON` type covers the
  STRING-serialization write path

## License

Apache-2.0. Inherits clickhouse-c's license; see
`clickhouse-c/LICENSE`.
