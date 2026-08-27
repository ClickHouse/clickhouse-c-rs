//! Spawns a throwaway `clickhouse server` for the integration tests.
//!
//! Every test that needs a live server skips itself when `clickhouse` is not
//! on PATH, so a checkout without it still runs the rest of the suite.

#![allow(dead_code)]

use std::io;
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

pub fn clickhouse_on_path() -> bool {
    Command::new("clickhouse")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

pub struct ChServer {
    child: Child,
    pub tcp_port: u16,
    /// Set only when spawned with a certificate.
    pub secure_port: Option<u16>,
    _tmp: tempfile::TempDir,
}

impl ChServer {
    pub fn spawn() -> TestResult<Self> {
        Self::start(None, &[])
    }

    /// Spawn with extra `--name=value` server overrides.
    pub fn spawn_with(extra: &[&str]) -> TestResult<Self> {
        Self::start(None, extra)
    }

    /// Spawn with a native secure port terminating TLS with `cert` / `key`.
    pub fn spawn_tls(cert: &Path, key: &Path) -> TestResult<Self> {
        Self::start(Some((cert, key)), &[])
    }

    fn start(tls: Option<(&Path, &Path)>, extra: &[&str]) -> TestResult<Self> {
        let tmp = tempfile::tempdir()?;
        let data_dir = tmp.path().join("ch");
        let log_dir = tmp.path().join("ch-logs");
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(&log_dir)?;

        let tcp_port = free_port()?;
        let secure_port = tls.is_some().then(free_port).transpose()?;

        let mut args = vec![
            "server".to_string(),
            "--".to_string(),
            format!("--tcp_port={tcp_port}"),
            format!("--http_port={}", free_port()?),
            format!("--interserver_http_port={}", free_port()?),
            "--mysql_port=".to_string(),
            "--postgresql_port=".to_string(),
            "--grpc_port=".to_string(),
            "--prometheus.port=".to_string(),
            "--listen_host=127.0.0.1".to_string(),
            format!("--path={}/", data_dir.display()),
            format!("--logger.log={}/server.log", log_dir.display()),
            format!("--logger.errorlog={}/error.log", log_dir.display()),
            "--logger.level=warning".to_string(),
        ];
        if let (Some(port), Some((cert, key))) = (secure_port, tls) {
            args.push(format!("--tcp_port_secure={port}"));
            args.push(format!("--openSSL.server.certificateFile={}", cert.display()));
            args.push(format!("--openSSL.server.privateKeyFile={}", key.display()));
            args.push("--openSSL.server.verificationMode=none".to_string());
        }
        args.extend(extra.iter().map(|a| (*a).to_string()));

        let mut cmd = Command::new("clickhouse");
        cmd.args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        // Own process group: SYSTEM SHUTDOWN is the clean path, but a test
        // that panics mid-run should not leave a server parented to the
        // harness.
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }

        let server = Self {
            child: cmd.spawn()?,
            tcp_port,
            secure_port,
            _tmp: tmp,
        };
        server.wait_for_ready()?;
        Ok(server)
    }

    fn wait_for_ready(&self) -> TestResult {
        let addr = format!("127.0.0.1:{}", self.tcp_port).parse()?;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(60) {
            if TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok()
                && self.query("SELECT 1").is_ok()
            {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        Err(io::Error::other("clickhouse server did not become ready").into())
    }

    /// Run `sql` through the `clickhouse client` binary, out of band from the
    /// crate under test.
    pub fn query(&self, sql: &str) -> TestResult<String> {
        let out = Command::new("clickhouse")
            .args([
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                &self.tcp_port.to_string(),
                "--query",
                sql,
            ])
            .output()?;
        if !out.status.success() {
            return Err(io::Error::other(format!(
                "clickhouse query failed: {sql}, stderr={}",
                String::from_utf8_lossy(&out.stderr)
            ))
            .into());
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }
}

impl Drop for ChServer {
    fn drop(&mut self) {
        let _ = self.query("SYSTEM SHUTDOWN");
        for _ in 0..50 {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                Err(_) => break,
            }
        }
        // Kill the whole group: the server forks helpers that outlive a
        // plain kill on the parent pid.
        #[cfg(unix)]
        {
            let pgid = self.child.id() as i32;
            let _ = Command::new("kill")
                .args(["-KILL", &format!("-{pgid}")])
                .stderr(Stdio::null())
                .status();
        }
        #[cfg(not(unix))]
        {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

/// Bind port 0, read the assignment, release it. Racy in principle; in
/// practice the kernel does not hand the same ephemeral port straight back.
fn free_port() -> io::Result<u16> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    listener.local_addr().map(|a| a.port())
}
