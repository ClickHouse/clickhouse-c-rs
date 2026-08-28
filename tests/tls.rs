//! TLS tests using temporary ClickHouse server and local certificate authority.
//!
//! Tests verify certificate chain and SNI hostname for blocking and
//! asynchronous clients. Tests skip when `clickhouse` or `openssl` is
//! unavailable.

mod common;

use std::io;
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;

use clickhouse_c::tls::{self, rustls};
use clickhouse_c::{Allocator, AsyncClient, Client, ClientOpts, Event};
use common::{ChServer, TestResult, clickhouse_on_path};

fn openssl_on_path() -> bool {
    Command::new("openssl")
        .arg("version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn openssl(args: &[&std::ffi::OsStr]) -> TestResult {
    let status = Command::new("openssl")
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if !status.success() {
        return Err(io::Error::other(format!("openssl {:?} failed", args[0])).into());
    }
    Ok(())
}

/// Creates certificate authority and signed server certificate.
///
/// Returns server certificate PEM, server key PEM, and CA certificate DER.
fn make_cert(dir: &Path) -> TestResult<(PathBuf, PathBuf, Vec<u8>)> {
    let p = |n: &str| dir.join(n);
    let oss = |path: &Path| path.as_os_str().to_owned();

    // Write server certificate extensions for OpenSSL
    let ext = p("leaf.ext");
    std::fs::write(
        &ext,
        "basicConstraints=critical,CA:FALSE\n\
         keyUsage=critical,digitalSignature,keyEncipherment\n\
         extendedKeyUsage=serverAuth\n\
         subjectAltName=DNS:localhost,IP:127.0.0.1\n",
    )?;

    // Generate self-signed certificate authority
    openssl(&[
        "req".as_ref(),
        "-x509".as_ref(),
        "-newkey".as_ref(),
        "rsa:2048".as_ref(),
        "-nodes".as_ref(),
        "-days".as_ref(),
        "1".as_ref(),
        "-subj".as_ref(),
        "/CN=clickhouse-c-rs test CA".as_ref(),
        "-keyout".as_ref(),
        &oss(&p("ca.key")),
        "-out".as_ref(),
        &oss(&p("ca.pem")),
    ])?;

    // Generate server key and signing request
    openssl(&[
        "req".as_ref(),
        "-newkey".as_ref(),
        "rsa:2048".as_ref(),
        "-nodes".as_ref(),
        "-subj".as_ref(),
        "/CN=localhost".as_ref(),
        "-keyout".as_ref(),
        &oss(&p("key.pem")),
        "-out".as_ref(),
        &oss(&p("leaf.csr")),
    ])?;

    // Sign server certificate with certificate authority
    openssl(&[
        "x509".as_ref(),
        "-req".as_ref(),
        "-in".as_ref(),
        &oss(&p("leaf.csr")),
        "-CA".as_ref(),
        &oss(&p("ca.pem")),
        "-CAkey".as_ref(),
        &oss(&p("ca.key")),
        "-CAcreateserial".as_ref(),
        "-days".as_ref(),
        "1".as_ref(),
        "-extfile".as_ref(),
        &oss(&ext),
        "-out".as_ref(),
        &oss(&p("cert.pem")),
    ])?;

    // Convert CA certificate for rustls root store
    openssl(&[
        "x509".as_ref(),
        "-in".as_ref(),
        &oss(&p("ca.pem")),
        "-outform".as_ref(),
        "DER".as_ref(),
        "-out".as_ref(),
        &oss(&p("ca.der")),
    ])?;

    let der = std::fs::read(p("ca.der"))?;
    Ok((p("cert.pem"), p("key.pem"), der))
}

/// Temporary TLS server and trusted certificate authority.
struct TlsServer {
    inner: ChServer,
    ca_der: Vec<u8>,
    _tmp: tempfile::TempDir,
}

impl TlsServer {
    fn spawn() -> TestResult<Self> {
        let tmp = tempfile::tempdir()?;
        let (cert_pem, key_pem, ca_der) = make_cert(tmp.path())?;
        Ok(Self {
            inner: ChServer::spawn_tls(&cert_pem, &key_pem)?,
            ca_der,
            _tmp: tmp,
        })
    }

    fn secure_port(&self) -> u16 {
        self.inner.secure_port.expect("spawned with a certificate")
    }
}

/// Creates rustls configuration with test CA as only root.
fn pinned_config(server: &TlsServer) -> TestResult<Arc<rustls::ClientConfig>> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(rustls::pki_types::CertificateDer::from(
        server.ca_der.clone(),
    ))?;
    Ok(tls::config_with_roots(roots))
}

fn skip() -> bool {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return true;
    }
    if !openssl_on_path() {
        eprintln!("openssl binary not found, skipping");
        return true;
    }
    false
}

#[tokio::test(flavor = "current_thread")]
async fn async_tls_roundtrip() -> TestResult {
    if skip() {
        return Ok(());
    }
    let server = TlsServer::spawn()?;
    let config = pinned_config(&server)?;

    let mut client = AsyncClient::connect_tls(
        ("127.0.0.1", server.secure_port()),
        "localhost",
        ClientOpts::new(),
        None,
        config,
    )
    .await?;
    assert!(client.server_info().is_some());

    client.send_query("SELECT toUInt64(42) AS x", None).await?;
    let mut got = None;
    loop {
        match client.recv_event().await? {
            Event::Data(block) => {
                if block.n_rows() == 1 {
                    let (_, bytes) = block.column(0).and_then(|c| c.fixed()).expect("x col");
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

#[tokio::test(flavor = "current_thread")]
async fn sync_tls_roundtrip() -> TestResult {
    if skip() {
        return Ok(());
    }
    let server = TlsServer::spawn()?;
    let config = pinned_config(&server)?;

    // Complete blocking client work without crossing await point
    let tcp = TcpStream::connect(("127.0.0.1", server.secure_port()))?;
    tcp.set_nodelay(true).ok();
    let io = tls::TlsIo::connect(tcp, "localhost", config)?;
    let mut client = Client::init(&ClientOpts::new(), Allocator::stdlib(), io, None)?;
    assert!(client.server_info().is_some());

    client.send_query("SELECT toUInt64(42) AS x", None)?;
    let mut got = None;
    loop {
        match client.recv_event()? {
            Event::Data(block) => {
                if block.n_rows() == 1 {
                    let (_, bytes) = block.column(0).and_then(|c| c.fixed()).expect("x col");
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
