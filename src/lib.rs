use crate::connection::tcp::TcpListener;
use crate::connection::{Connection, Listener};
use crate::nbd::handshake::Handshaker;
use crate::nbd::{block_device::BlockDevice, Export};
use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use once_cell::sync::Lazy;
use std::cmp::min;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

mod connection;
pub mod nbd;

static ZEROES: Lazy<Bytes> = Lazy::new(|| BytesMut::zeroed(1024 * 256).freeze());

pub struct Runner {
    listen_endpoint: ListenEndpoint,
    handshaker: Arc<Handshaker>,
}

enum ListenEndpoint {
    Tcp(String),
    #[cfg(unix)]
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

    #[cfg(unix)]
    pub fn unix<P: AsRef<Path>>(socket_path: P) -> Self {
        Self {
            listen_endpoint: ListenEndpoint::Unix(socket_path.as_ref().to_path_buf()),
            exports: HashMap::default(),
            default_export: None,
            structured_replies_disabled: false,
            extended_headers_disabled: false,
        }
    }

    pub fn with_export<S: ToString, H: BlockDevice + Send + Sync + 'static>(
        mut self,
        name: S,
        block_device: H,
        force_read_only: bool,
    ) -> Result<Self, anyhow::Error> {
        let name = name.to_string();
        let export = Export::new(name.clone(), block_device, force_read_only)?;
        self.exports.insert(name, export);
        Ok(self)
    }

    pub fn with_default_export<S: ToString>(mut self, name: S) -> Result<Self, anyhow::Error> {
        let name = name.to_string();
        if !self.exports.contains_key(&name) {
            anyhow::bail!("unknown export: {}", name);
        }
        self.default_export = Some(name.to_string());
        Ok(self)
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
    #[cfg(unix)]
    Unix(unix::UnixAddr),
}

impl Display for ClientEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::Tcp(addr) => Display::fmt(addr, f),
            #[cfg(unix)]
            Self::Unix(addr) => Display::fmt(addr, f),
        }
    }
}

impl Runner {
    pub async fn run(&self) -> anyhow::Result<()> {
        match &self.listen_endpoint {
            ListenEndpoint::Tcp(addr) => {
                self._run(TcpListener::bind(addr.to_string()).await?)
                    .await?
            }
            #[cfg(unix)]
            ListenEndpoint::Unix(path) => {
                self._run(connection::unix::UnixListener::bind(path.to_path_buf()).await?)
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
        let buffer_len = min(n, 1024 * 256);
        let mut buffer = BytesMut::zeroed(buffer_len);
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
        let zeroes = ZEROES.as_ref();
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
    inner: R,
    remaining: usize,
}

impl<R> LimitedReader<R> {
    pub(crate) fn new(inner: R, limit: usize) -> Self
    where
        R: AsyncRead + Unpin + Send,
    {
        Self {
            inner,
            remaining: limit,
        }
    }

    pub fn into_inner(self) -> (R, usize) {
        (self.inner, self.remaining)
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
            return Poll::Ready(Ok(0));
        }

        let inner = &mut this.inner;

        let max_read = min(buf.len(), this.remaining);
        let pinned_inner = Pin::new(inner);

        match pinned_inner.poll_read(cx, &mut buf[..max_read]) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(0)) => {
                // eof
                Poll::Ready(Ok(0))
            }
            Poll::Ready(Ok(bytes_read)) => {
                this.remaining -= bytes_read;
                Poll::Ready(Ok(bytes_read))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        }
    }
}

fn is_power_of_two(n: u32) -> bool {
    n != 0 && (n & (n - 1)) == 0
}

/// Returns the highest power of two that fits into `n`
fn highest_power_of_two(n: u32) -> u32 {
    if n == 0 {
        0
    } else {
        1 << (31 - n.leading_zeros())
    }
}

#[cfg(unix)]
mod unix {
    use std::fmt::{Debug, Display, Formatter};
    use std::ops::Deref;
    use std::os::unix::net::SocketAddr;

    #[derive(Clone)]
    pub struct UnixAddr(pub(crate) SocketAddr);

    impl Deref for UnixAddr {
        type Target = SocketAddr;
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
}
