//! TLS smoke test using an external ClickHouse endpoint.
//!
//! Test is ignored by default. Run explicitly:
//!
//! ```sh
//! CHC_TLS_HOST=... \
//! CHC_TLS_PASSWORD=... \
//! cargo test -p clickhouse-c-rs --features tls,tokio --test tls_live -- --ignored --nocapture
//! ```
//!
//! Environment variables are `CHC_TLS_HOST` (required), `CHC_TLS_PORT`
//! (default 9440), `CHC_TLS_USER` (default `default`), `CHC_TLS_PASSWORD`
//! (default empty), and `CHC_TLS_DB` (default `default`). Peer verification
//! uses public webpki roots from `tls::default_config()`.

use std::net::TcpStream;

use clickhouse_c::{Allocator, AsyncClient, Client, ClientOpts, Event};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct Endpoint {
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
}

fn endpoint() -> Option<Endpoint> {
    let host = std::env::var("CHC_TLS_HOST").ok()?;
    Some(Endpoint {
        host,
        port: std::env::var("CHC_TLS_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(9440),
        user: std::env::var("CHC_TLS_USER").unwrap_or_else(|_| "default".into()),
        password: std::env::var("CHC_TLS_PASSWORD").unwrap_or_default(),
        database: std::env::var("CHC_TLS_DB").unwrap_or_else(|_| "default".into()),
    })
}

fn opts(ep: &Endpoint) -> ClientOpts {
    ClientOpts::new()
        .database(&ep.database)
        .user(&ep.user)
        .password(&ep.password)
        .client_name("clickhouse-c-rs tls_live")
}

#[tokio::test]
#[ignore = "hits an external network endpoint; set CHC_TLS_HOST"]
async fn async_tls_live() -> TestResult {
    let Some(ep) = endpoint() else {
        eprintln!("CHC_TLS_HOST unset, skipping");
        return Ok(());
    };

    let mut client = AsyncClient::connect_tls(
        (ep.host.as_str(), ep.port),
        &ep.host,
        opts(&ep),
        None,
        clickhouse_c::tls::default_config(),
    )
    .await?;

    let info = client.server_info().expect("server info after handshake");
    eprintln!(
        "async connected: {} {}.{}.{}",
        info.name, info.version_major, info.version_minor, info.version_patch
    );

    client.send_query("SELECT toUInt64(42) AS x", None).await?;
    let mut got = None;
    loop {
        match client.recv_event().await? {
            Event::Data(block) => {
                if block.n_rows() == 1 {
                    let (_, bytes) = block.column(0).and_then(|c| c.fixed()).expect("col");
                    got = Some(u64::from_le_bytes(bytes[..8].try_into().unwrap()));
                }
            }
            Event::EndOfStream => break,
            Event::Exception(e) => return Err(e.into()),
            _ => {}
        }
    }
    assert_eq!(got, Some(42));
    Ok(())
}

#[tokio::test]
#[ignore = "hits an external network endpoint; set CHC_TLS_HOST"]
async fn sync_tls_live() -> TestResult {
    let Some(ep) = endpoint() else {
        eprintln!("CHC_TLS_HOST unset, skipping");
        return Ok(());
    };

    // Complete blocking client work without crossing await point
    let addr = format!("{}:{}", ep.host, ep.port);
    let tcp = TcpStream::connect(&addr)?;
    tcp.set_nodelay(true).ok();
    let io = clickhouse_c::tls::TlsIo::connect(tcp, &ep.host, clickhouse_c::tls::default_config())?;
    let mut client = Client::init(&opts(&ep), Allocator::stdlib(), io, None)?;

    let info = client.server_info().expect("server info after handshake");
    eprintln!(
        "sync connected: {} {}.{}.{}",
        info.name, info.version_major, info.version_minor, info.version_patch
    );

    client.send_query("SELECT toUInt64(42) AS x", None)?;
    let mut got = None;
    loop {
        match client.recv_event()? {
            Event::Data(block) => {
                if block.n_rows() == 1 {
                    let (_, bytes) = block.column(0).and_then(|c| c.fixed()).expect("col");
                    got = Some(u64::from_le_bytes(bytes[..8].try_into().unwrap()));
                }
            }
            Event::EndOfStream => break,
            Event::Exception(e) => return Err(e.into()),
            _ => {}
        }
    }
    assert_eq!(got, Some(42));
    Ok(())
}
