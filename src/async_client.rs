//! Tokio adapter over the runtime-neutral [`IolessClient`].
//!
//! This is a byte pump and nothing more: the protocol lives in
//! [`IolessClient`], which is why another runtime needs no code from here.

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

/// Transport [`AsyncClient`] drives: a stream it owns and pumps bytes
/// through. Blanket-implemented, so any `AsyncRead + AsyncWrite + Unpin +
/// Send` qualifies and nothing has to implement it by hand. The `Send`
/// bound keeps the client's method futures `Send`, which `tokio::spawn` on
/// a multi-thread runtime requires.
pub trait AsyncTransport: AsyncRead + AsyncWrite + Unpin + Send {}

impl<S: AsyncRead + AsyncWrite + Unpin + Send> AsyncTransport for S {}

/// [`AsyncClient`] with its transport type erased, so a plaintext and a TLS
/// connection are one type: storable in a struct field, a `Vec`, or
/// reassignable on reconnect without a hand-written enum delegating every
/// method.
///
/// Dispatch is dynamic per socket read and write only. The protocol work
/// and the method futures are untouched: no boxed futures, no allocation
/// per call.
pub type BoxedAsyncClient = AsyncClient<Box<dyn AsyncTransport>>;

/// Worker-free async ClickHouse client.
///
/// Generic over the transport, so a caller can bring their own
/// [`AsyncTransport`]: a TLS stream from another library, a proxied socket,
/// a duplex pipe in a test. [`connect`](Self::connect) and
/// [`connect_tls`](Self::connect_tls) are conveniences over the two common
/// ones. [`boxed`](Self::boxed) erases the transport type when plaintext and
/// TLS connections have to share one type.
pub struct AsyncClient<S = TcpStream> {
    core: IolessClient,
    stream: S,
    read_buf: Vec<u8>,
}

impl AsyncClient<TcpStream> {
    /// TCP-connect and run the Hello handshake. Sets `TCP_NODELAY`: Native is
    /// request/response, so Nagle only adds latency to the small writes
    /// between blocks.
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
    /// Connect over TLS: TCP-connect to `addr`, then rustls-handshake
    /// verifying the peer against `config` for `domain` (sent as SNI).
    /// `config` typically comes from
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
    /// Run the Hello handshake over an already-connected transport.
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

    /// Send a Data block, or the empty terminator with [`None`].
    pub async fn send_data(&mut self, builder: Option<&BlockBuilder<'_>>) -> Result<()> {
        self.drain_out().await?;
        self.core.send_data(builder)?;
        self.drain_out().await
    }

    /// Close an INSERT's data stream.
    pub async fn send_data_end(&mut self) -> Result<()> {
        self.drain_out().await?;
        self.core.send_data_end()?;
        self.drain_out().await
    }

    /// Await the next server event, pumping the socket as needed. Any block
    /// or exception payload is owned by the returned [`Event`].
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

    /// Identity the server sent during the handshake.
    pub fn server_info(&self) -> Option<ServerInfo> {
        self.core.server_info()
    }

    /// Box the transport behind a trait object; see [`BoxedAsyncClient`].
    /// The connection is untouched, so this is callable mid-stream.
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

    /// The protocol machine underneath, for anything the adapter does not
    /// expose.
    pub fn core(&mut self) -> &mut IolessClient {
        &mut self.core
    }

    async fn drain_out(&mut self) -> Result<()> {
        let mut wrote = false;
        loop {
            // Disjoint field borrows: the &[u8] into the machine's queue is
            // alive across the write, but only `self.stream` is borrowed
            // mutably, and a shared slice is Send so the future stays Send.
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
        // A TLS stream's poll_write may leave the tail of a record buffered in
        // rustls when the socket briefly back-pressures; flush forces it out
        // so the server is not left waiting on a half-sent Hello or query.
        // Skipped when nothing was written, so the recv path never flushes an
        // idle stream.
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
    use super::{AsyncClient, BoxedAsyncClient, Event};
    use crate::builder::BlockBuilder;
    use crate::client::ClientOpts;

    #[test]
    fn async_client_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<AsyncClient>();
        assert_send::<Event>();
    }

    // Compile-time guard: the method futures must be `Send`, not just
    // `AsyncClient` itself, or `tokio::spawn` on a multi-thread runtime
    // rejects them. A raw FFI pointer held across an await silently makes a
    // future `!Send` -- invisible to the live `current_thread` tests, so
    // assert it here where it costs nothing.
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

    // Erasure is the point of `boxed`: two transport types, one client
    // type, so a consumer holding either needs no delegating wrapper. The
    // erased futures must stay `Send` too.
    #[allow(dead_code)]
    fn boxed_clients_unify(
        plain: AsyncClient,
        tls_like: AsyncClient<tokio::io::DuplexStream>,
    ) -> Vec<BoxedAsyncClient> {
        fn require_send<T: Send>(_: T) {}
        let mut erased = plain.boxed();
        require_send(erased.recv_event());
        vec![erased, tls_like.boxed()]
    }

    // The consumer case for erasure: a config flag picks plaintext or TLS,
    // and one field holds whichever came back.
    #[cfg(feature = "tls")]
    #[allow(dead_code)]
    fn plaintext_and_tls_share_one_type(
        plain: AsyncClient,
        tls: AsyncClient<tokio_rustls::client::TlsStream<tokio::net::TcpStream>>,
    ) -> Vec<BoxedAsyncClient> {
        vec![plain.boxed(), tls.boxed()]
    }

    // The adapter is generic, so a caller can drive the protocol over any
    // tokio transport, not just the two the constructors cover.
    #[allow(dead_code)]
    fn any_tokio_transport_works(pipe: tokio::io::DuplexStream) {
        fn require_send<T: Send>(_: T) {}
        require_send(AsyncClient::handshake_on(pipe, ClientOpts::new(), None));
    }
}
