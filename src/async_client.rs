//! Tokio client built on [`IolessClient`].

use core::pin::Pin;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::alloc::Allocator;
use crate::builder::BlockBuilder;
use crate::client::{ClientOpts, Event, ServerInfo};
use crate::codec::Codec;
use crate::error::{Error, ErrorKind, Result};
use crate::ioless::{IolessClient, Step};

const DEFAULT_READ_BUF_BYTES: usize = 8 * 1024;

/// Asynchronous byte stream used by [`AsyncClient`].
///
/// Any `AsyncRead + AsyncWrite + Unpin + Send` type implements this trait.
pub trait AsyncTransport: AsyncRead + AsyncWrite + Unpin + Send {}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncTransport for S {}

/// [`AsyncClient`] with transport type erased.
///
/// Use this alias when plaintext and TLS connections must share one type.
/// Dynamic dispatch applies to transport reads and writes.
pub type BoxedAsyncClient = AsyncClient<Box<dyn AsyncTransport>>;

/// Asynchronous ClickHouse native protocol client.
///
/// Client accepts any [`AsyncTransport`]. [`connect`](Self::connect) creates a
/// TCP connection. [`connect_tls`](Self::connect_tls) creates a rustls
/// connection when `tls` feature is enabled. [`boxed`](Self::boxed) erases
/// transport type.
pub struct AsyncClient<S = TcpStream> {
    core: IolessClient,
    stream: S,
    read_buf: Vec<u8>,
}

impl AsyncClient<TcpStream> {
    /// Connects over TCP and completes Hello handshake.
    ///
    /// Socket uses `TCP_NODELAY`.
    pub async fn connect<A>(
        addr: A,
        opts: ClientOpts,
        codec: Option<Pin<Box<Codec>>>,
    ) -> Result<Self>
    where
        A: ToSocketAddrs,
    {
        let sock = TcpStream::connect(addr).await?;
        sock.set_nodelay(true).ok();
        Self::handshake_on(sock, opts, codec).await
    }
}

#[cfg(feature = "tls")]
impl AsyncClient<tokio_rustls::client::TlsStream<TcpStream>> {
    /// Connects over TCP and TLS, then completes Hello handshake.
    ///
    /// TLS verifies peer using `config` and `domain`. Domain is also sent as
    /// SNI. Default configuration is available from
    /// [`tls::default_config`](crate::tls::default_config).
    pub async fn connect_tls<A>(
        addr: A,
        domain: &str,
        opts: ClientOpts,
        codec: Option<Pin<Box<Codec>>>,
        config: std::sync::Arc<rustls::ClientConfig>,
    ) -> Result<Self>
    where
        A: ToSocketAddrs,
    {
        let sock = TcpStream::connect(addr).await?;
        sock.set_nodelay(true).ok();
        let server_name =
            rustls::pki_types::ServerName::try_from(domain.to_owned()).map_err(|_| {
                Error::new(
                    ErrorKind::Usage,
                    format!("invalid TLS server name: {domain}"),
                )
            })?;
        let tls = tokio_rustls::TlsConnector::from(config)
            .connect(server_name, sock)
            .await
            .map_err(|e| Error::new(ErrorKind::Io, format!("TLS handshake: {e}")))?;
        Self::handshake_on(tls, opts, codec).await
    }
}

impl<S: AsyncTransport> AsyncClient<S> {
    /// Completes Hello handshake over an existing transport.
    pub async fn handshake_on(
        stream: S,
        opts: ClientOpts,
        codec: Option<Pin<Box<Codec>>>,
    ) -> Result<Self> {
        let read_buf_bytes = if opts.read_buffer_bytes == 0 {
            DEFAULT_READ_BUF_BYTES
        } else {
            opts.read_buffer_bytes
        };
        let mut client = Self {
            core: IolessClient::new(&opts, Allocator::stdlib(), codec)?,
            stream,
            read_buf: vec![0; read_buf_bytes],
        };
        client.pump_until_ready(|core| core.handshake()).await?;
        Ok(client)
    }

    pub async fn send_query(&mut self, sql: &str, query_id: Option<&str>) -> Result<()> {
        self.drain_out().await?;
        self.core.send_query(sql, query_id)?;
        self.drain_out().await
    }

    /// Sends a Data block, or empty terminator when `builder` is `None`.
    pub async fn send_data(&mut self, builder: Option<&BlockBuilder<'_>>) -> Result<()> {
        self.drain_out().await?;
        self.core.send_data(builder)?;
        self.drain_out().await
    }

    /// Sends empty Data block that ends INSERT input.
    pub async fn send_data_end(&mut self) -> Result<()> {
        self.drain_out().await?;
        self.core.send_data_end()?;
        self.drain_out().await
    }

    /// Waits for next server event.
    ///
    /// Returned event owns block or exception payload.
    pub async fn recv_event(&mut self) -> Result<Event> {
        let mut event = None;
        self.pump_until_ready(|core| {
            Ok(match core.recv_event()? {
                Step::Ready(e) => {
                    event = Some(e);
                    Step::Ready(())
                }
                Step::NeedsInput => Step::NeedsInput,
            })
        })
        .await?;
        Ok(event.expect("pump_until_ready only returns once the step stored an event"))
    }

    /// Returns server identity received during handshake.
    pub fn server_info(&self) -> Option<ServerInfo> {
        self.core.server_info()
    }

    /// Erases transport type without changing connection state.
    pub fn boxed(self) -> BoxedAsyncClient
    where
        S: 'static,
    {
        AsyncClient {
            core: self.core,
            stream: Box::new(self.stream),
            read_buf: self.read_buf,
        }
    }

    /// Returns mutable access to transport-independent protocol client.
    pub fn core(&mut self) -> &mut IolessClient {
        &mut self.core
    }

    async fn drain_out(&mut self) -> Result<()> {
        let mut wrote = false;
        loop {
            // Queue and stream are disjoint fields across await
            let buf = self.core.pending_out();
            if buf.is_empty() {
                break;
            }
            let n = self.stream.write(buf).await?;
            if n == 0 {
                return Err(Error::new(ErrorKind::Io, "transport write returned zero"));
            }
            self.core.consume_out(n);
            wrote = true;
        }
        // TLS can buffer partial record after poll_write reports progress
        if wrote {
            self.stream.flush().await?;
        }
        Ok(())
    }

    async fn pump_until_ready(
        &mut self,
        mut step: impl FnMut(&mut IolessClient) -> Result<Step<()>>,
    ) -> Result<()> {
        loop {
            self.drain_out().await?;
            match step(&mut self.core)? {
                Step::Ready(()) => return self.drain_out().await,
                Step::NeedsInput => {
                    self.drain_out().await?;
                    self.read_more().await?;
                }
            }
        }
    }

    async fn read_more(&mut self) -> Result<()> {
        let n = self.stream.read(&mut self.read_buf).await?;
        if n == 0 {
            return Err(Error::new(ErrorKind::Eof, "transport closed"));
        }
        self.core.submit(&self.read_buf[..n])
    }
}

#[cfg(test)]
mod tests {
    use super::{AsyncClient, Event};
    #[cfg(feature = "tls")]
    use super::{BoxedAsyncClient, TcpStream};
    use crate::builder::BlockBuilder;
    use crate::client::ClientOpts;

    #[test]
    fn async_client_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<AsyncClient>();
        assert_send::<Event>();
    }

    // Multi-thread Tokio requires method futures to implement Send
    #[allow(dead_code)]
    fn method_futures_are_send(mut c: AsyncClient, bb: BlockBuilder<'static>) {
        fn require_send<T: Send>(_: T) {}
        require_send(AsyncClient::connect(("h", 1u16), ClientOpts::new(), None));
        #[cfg(feature = "tls")]
        require_send(AsyncClient::connect_tls(
            ("h", 1u16),
            "h",
            ClientOpts::new(),
            None,
            crate::tls::default_config(),
        ));
        require_send(c.send_query("", None));
        require_send(c.send_data(Some(&bb)));
        require_send(c.send_data_end());
        require_send(c.recv_event());
    }

    // Plaintext and TLS clients must share erased type and Send futures
    #[cfg(feature = "tls")]
    #[allow(dead_code)]
    fn plaintext_and_tls_share_one_type(
        plain: AsyncClient,
        tls: AsyncClient<tokio_rustls::client::TlsStream<TcpStream>>,
    ) -> Vec<BoxedAsyncClient> {
        fn require_send<T: Send>(_: T) {}
        let mut erased = plain.boxed();
        require_send(erased.recv_event());
        vec![erased, tls.boxed()]
    }

    // Custom Tokio transports use same protocol adapter
    #[allow(dead_code)]
    fn any_tokio_transport_works(pipe: tokio::io::DuplexStream) {
        fn require_send<T: Send>(_: T) {}
        require_send(AsyncClient::handshake_on(pipe, ClientOpts::new(), None));
    }
}
