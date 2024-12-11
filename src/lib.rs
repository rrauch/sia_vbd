use crate::connection::tcp::TcpListener;
use crate::connection::unix::UnixListener;
use crate::connection::{Connection, Listener};
use crate::nbd::handshake::Handshaker;
use crate::nbd::{handler::Handler, Export};
use anyhow::anyhow;
use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use std::cmp::min;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

mod connection;
pub mod nbd;

pub struct Runner {
    listen_endpoint: ListenEndpoint,
    handshaker: Arc<Handshaker>,
}

enum ListenEndpoint {
    Tcp(String),
    Unix(PathBuf),
}

pub struct Builder {
    listen_endpoint: ListenEndpoint,
    exports: HashMap<String, Export>,
    default_export: Option<String>,
    structured_replies_disabled: bool,
    extended_headers_disabled: bool,
}

impl Builder {
    pub fn tcp<S: ToString>(host: S, port: u16) -> Self {
        Self {
            listen_endpoint: ListenEndpoint::Tcp(format!("{}:{}", host.to_string(), port)),
            exports: HashMap::default(),
            default_export: None,
            structured_replies_disabled: false,
            extended_headers_disabled: false,
        }
    }

    pub fn unix<P: AsRef<Path>>(socket_path: P) -> Self {
        Self {
            listen_endpoint: ListenEndpoint::Unix(socket_path.as_ref().to_path_buf()),
            exports: HashMap::default(),
            default_export: None,
            structured_replies_disabled: false,
            extended_headers_disabled: false,
        }
    }

    pub fn with_export<S: ToString, H: Handler + Send + Sync + 'static>(
        mut self,
        name: S,
        handler: H,
        force_read_only: bool,
    ) -> Self {
        let name = name.to_string();
        let export = Export::new(name.clone(), handler, force_read_only);
        self.exports.insert(name, export);
        self
    }

    pub fn with_default_export<S: ToString>(mut self, name: S) -> Self {
        self.default_export = Some(name.to_string());
        self
    }

    pub fn disable_structured_replies(mut self) -> Self {
        self.structured_replies_disabled = true;
        self.extended_headers_disabled = true;
        self
    }

    pub fn disable_extended_headers(mut self) -> Self {
        self.extended_headers_disabled = true;
        self
    }

    pub fn build(self) -> Runner {
        let handshaker = Handshaker::new(
            self.exports,
            self.default_export,
            self.structured_replies_disabled,
            self.extended_headers_disabled,
        );
        Runner {
            listen_endpoint: self.listen_endpoint,
            handshaker: Arc::new(handshaker),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ClientEndpoint {
    Tcp(SocketAddr),
    Unix(UnixAddr),
}

impl Display for ClientEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::Tcp(addr) => Display::fmt(addr, f),
            Self::Unix(addr) => Display::fmt(addr, f),
        }
    }
}

#[derive(Clone)]
pub struct UnixAddr(std::os::unix::net::SocketAddr);

impl Deref for UnixAddr {
    type Target = std::os::unix::net::SocketAddr;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<UnixAddr> for std::os::unix::net::SocketAddr {
    fn from(value: UnixAddr) -> Self {
        value.0
    }
}

impl Display for UnixAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Debug for UnixAddr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Runner {
    pub async fn run(&self) -> anyhow::Result<()> {
        match &self.listen_endpoint {
            ListenEndpoint::Tcp(addr) => {
                self._run(TcpListener::bind(addr.to_string()).await?)
                    .await?
            }
            ListenEndpoint::Unix(path) => {
                self._run(UnixListener::bind(path.to_path_buf()).await?)
                    .await?
            }
        };
        Ok(())
    }

    async fn _run<T: Listener>(&self, listener: T) -> anyhow::Result<()> {
        println!("Listening on {}", listener.addr());
        loop {
            let conn = listener.accept().await?;
            println!("New connection from {}", conn.client_endpoint());

            let handshaker = self.handshaker.clone();
            let client_endpoint = conn.client_endpoint().clone();
            let (rx, tx) = conn.into_split();

            tokio::spawn(async move {
                if let Err(error) =
                    nbd::new_connection(&handshaker, rx, tx, client_endpoint.clone()).await
                {
                    eprintln!("error {:?}, client endpoint {}", error, client_endpoint);
                }
                println!("connection closed for {}", client_endpoint);
            });
        }
    }
}

pub(crate) trait AsyncReadBytesExt: AsyncReadExt + Unpin {
    /// Reads exactly `n` bytes into a new buffer
    async fn get_exact(&mut self, n: usize) -> std::io::Result<Bytes> {
        let mut buf = BytesMut::zeroed(n);
        self.read_exact(buf.as_mut()).await?;
        Ok(buf.freeze())
    }

    /// Skips exactly `n` bytes from the reader
    async fn skip(&mut self, n: usize) -> std::io::Result<()> {
        let mut remaining = n;
        let mut buffer = [0u8; 8192];
        while remaining > 0 {
            let to_read = min(remaining, buffer.len());
            let bytes_read = self.read(&mut buffer[..to_read]).await?;
            if bytes_read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Reached EOF",
                ));
            }
            remaining -= bytes_read;
        }
        Ok(())
    }
}
impl<T: AsyncReadExt + ?Sized + Unpin> AsyncReadBytesExt for T {}

pub(crate) trait AsyncWriteBytesExt: AsyncWriteExt + Unpin {
    /// Write exactly `n` bytes of zeroes to the writer
    async fn write_zeroes(&mut self, n: usize) -> std::io::Result<()> {
        let mut remaining = n;
        let zeroes = [0u8; 8192];
        while remaining > 0 {
            let to_write = min(remaining, zeroes.len());
            let bytes_written = self.write(&zeroes[..to_write]).await?;
            if bytes_written == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Reached EOF",
                ));
            }
            remaining -= bytes_written;
        }
        Ok(())
    }
}
impl<T: AsyncWriteExt + ?Sized + Unpin> AsyncWriteBytesExt for T {}

#[derive(Debug)]
pub(crate) struct LimitedReader<R> {
    inner: Option<R>,
    remaining: usize,
    return_tx: Option<oneshot::Sender<(R, usize)>>,
}

impl<R> LimitedReader<R> {
    pub(crate) fn new(inner: R, limit: usize, return_tx: oneshot::Sender<(R, usize)>) -> Self
    where
        R: AsyncRead + Unpin + Send,
    {
        Self {
            inner: Some(inner),
            remaining: limit,
            return_tx: Some(return_tx),
        }
    }

    fn return_inner(&mut self) {
        if let Some(inner) = self.inner.take() {
            if let Some(return_tx) = self.return_tx.take() {
                let _ = return_tx.send((inner, self.remaining));
            }
        }
    }
}

impl<R: AsyncRead + Unpin + Send> AsyncRead for LimitedReader<R> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();

        if this.remaining == 0 {
            // finished already
            this.return_inner();
            return Poll::Ready(Ok(0));
        }

        let inner = match this.inner.as_mut() {
            Some(inner) => inner,
            None => {
                return Poll::Ready(Err(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    anyhow!("inner reader was None"),
                )));
            }
        };

        let max_read = min(buf.len(), this.remaining);
        let pinned_inner = Pin::new(inner);

        match pinned_inner.poll_read(cx, &mut buf[..max_read]) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(0)) => {
                // eof
                this.return_inner();
                Poll::Ready(Ok(0))
            }
            Poll::Ready(Ok(bytes_read)) => {
                this.remaining -= bytes_read;
                if this.remaining == 0 {
                    this.return_inner();
                }
                Poll::Ready(Ok(bytes_read))
            }
            Poll::Ready(Err(err)) => {
                this.return_inner();
                Poll::Ready(Err(err))
            }
        }
    }
}

impl<R> Drop for LimitedReader<R> {
    fn drop(&mut self) {
        self.return_inner();
    }
}
