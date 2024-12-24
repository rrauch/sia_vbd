use anyhow::anyhow;
use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use once_cell::sync::Lazy;
use std::cmp::min;
use std::fmt::{Debug, Display, Formatter};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

pub mod hash;
pub mod nbd;
pub mod vbd;

static ZEROES: Lazy<Bytes> = Lazy::new(|| BytesMut::zeroed(1024 * 256).freeze());

enum ListenEndpoint {
    Tcp(String),
    #[cfg(unix)]
    Unix(PathBuf),
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

#[derive(Debug)]
pub(crate) struct LimitedWriter<W> {
    inner: W,
    remaining: usize,
}

impl<W> LimitedWriter<W> {
    pub(crate) fn new(inner: W, limit: usize) -> Self
    where
        W: AsyncWrite + Unpin + Send,
    {
        Self {
            inner,
            remaining: limit,
        }
    }

    pub fn into_inner(self) -> (W, usize) {
        (self.inner, self.remaining)
    }
}

impl<W: AsyncWrite + Unpin + Send> AsyncWrite for LimitedWriter<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if this.remaining == 0 {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "cannot write beyond limit",
            )));
        }

        let inner = &mut this.inner;
        let pinned_inner = Pin::new(inner);

        let max_write = min(buf.len(), this.remaining);

        match pinned_inner.poll_write(cx, &buf[..max_write]) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(bytes_written)) => {
                this.remaining -= bytes_written;
                Poll::Ready(Ok(bytes_written))
            }
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let inner = &mut this.inner;
        let pinned_inner = Pin::new(inner);
        pinned_inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // don't close the inner writer, just flush it
        self.poll_flush(cx)
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
