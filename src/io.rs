use crate::{Etag, ZEROES};
use anyhow::anyhow;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncWrite, AsyncWriteExt};
use std::cmp::min;
use std::future::Future;
use std::io::{ErrorKind, SeekFrom};
use std::marker::PhantomData;
use std::ops::DerefMut;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_util::compat::TokioAsyncWriteCompatExt;

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
pub(crate) struct WrappedReader<T>(pub T);

impl<T, R> AsyncRead for WrappedReader<T>
where
    T: DerefMut<Target = R> + Send + Unpin,
    R: AsyncRead + Unpin + Send,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(self.0.deref_mut()).poll_read(cx, buf)
    }
}

impl<T, R> AsyncSeek for WrappedReader<T>
where
    T: DerefMut<Target = R> + Send + Unpin,
    R: AsyncSeek + Unpin + Send,
{
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        Pin::new(self.0.deref_mut()).poll_seek(cx, pos)
    }
}

#[derive(Debug)]
pub(crate) struct CountingReader<R> {
    inner: R,
    read: usize,
}

impl<R> CountingReader<R> {
    pub(crate) fn new(inner: R) -> Self
    where
        R: AsyncRead + Unpin + Send,
    {
        Self { inner, read: 0 }
    }

    pub fn read(&self) -> usize {
        self.read
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: AsyncRead + Unpin + Send> AsyncRead for CountingReader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_read(cx, buf).map(|res| {
            res.map(|n| {
                self.read += n;
                n
            })
        })
    }
}

#[derive(Debug)]
pub(crate) struct LimitedReader<R> {
    inner: Option<R>,
    remaining: usize,
}

impl<R> LimitedReader<R> {
    pub(crate) fn new(inner: R, limit: usize) -> Self
    where
        R: AsyncRead + Unpin + Send,
    {
        Self {
            inner: Some(inner),
            remaining: limit,
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
            this.inner.take();
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
                this.inner.take();
                Poll::Ready(Ok(0))
            }
            Poll::Ready(Ok(bytes_read)) => {
                this.remaining -= bytes_read;
                if this.remaining == 0 {
                    this.inner.take();
                }
                Poll::Ready(Ok(bytes_read))
            }
            Poll::Ready(Err(err)) => {
                this.inner.take();
                Poll::Ready(Err(err))
            }
        }
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

pub trait AsyncReadExtBuffered: AsyncRead {
    fn read_exact_buffered<'a, B: BufMut>(
        &'a mut self,
        buf: &'a mut B,
        n: usize,
    ) -> ReadBuffered<'a, Self, B>
    where
        Self: Unpin + Sized,
    {
        ReadBuffered {
            reader: self,
            buf,
            exact: Some(n),
            read: 0,
        }
    }

    fn read_all_buffered<'a, B: BufMut>(&'a mut self, buf: &'a mut B) -> ReadBuffered<'a, Self, B>
    where
        Self: Unpin + Sized,
    {
        ReadBuffered {
            reader: self,
            buf,
            exact: None,
            read: 0,
        }
    }
}

impl<T: AsyncRead + ?Sized> AsyncReadExtBuffered for T {}

pub struct ReadBuffered<'a, R, B> {
    reader: &'a mut R,
    buf: &'a mut B,
    exact: Option<usize>,
    read: usize,
}

impl<R: AsyncRead + Unpin, B: BufMut> Future for ReadBuffered<'_, R, B> {
    type Output = std::io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            if self.buf.remaining_mut() == 0 {
                return Poll::Ready(Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Not enough buffer space, increase read buffer",
                )));
            }

            let read_limit = match self.exact.as_ref() {
                Some(exact) => {
                    let remaining = *exact - self.read;
                    if remaining == 0 {
                        break;
                    }
                    Some(remaining)
                }
                None => None,
            };

            let uninit = self.buf.chunk_mut();
            let to_read = match read_limit {
                Some(n) => min(uninit.len(), n),
                None => uninit.len(),
            };

            if to_read == 0 {
                return Poll::Ready(Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Not enough buffer space, increase read buffer",
                )));
            }
            let slice = &mut uninit[..to_read];
            // Safety: We are writing exactly `to_read` bytes and will advance accordingly
            let buf = unsafe { std::slice::from_raw_parts_mut(slice.as_mut_ptr(), to_read) };
            match Pin::new(&mut self.reader).poll_read(cx, unsafe {
                std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, to_read)
            }) {
                Poll::Ready(Ok(0)) => {
                    break;
                }
                // Safety: We are advancing exactly as far as the underlying data has been written
                Poll::Ready(Ok(n)) => unsafe {
                    self.buf.advance_mut(n);
                    self.read += n;
                },
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }

        let res = match self.exact.as_ref() {
            Some(exact) if exact > &self.read => {
                Err(std::io::Error::new(ErrorKind::UnexpectedEof, "EOF reached"))
            }
            _ => Ok(self.read),
        };

        Poll::Ready(res)
    }
}

trait Readable {}
trait Writable {}

pub struct ReadOnly;
impl Readable for ReadOnly {}

pub struct ReadWrite;
impl Readable for ReadWrite {}
impl Writable for ReadWrite {}

pub(crate) struct TokioFile<M> {
    file: tokio_util::compat::Compat<tokio::fs::File>,
    path: PathBuf,
    _phantom_data: PhantomData<M>,
}

impl TokioFile<ReadWrite> {
    pub async fn create_new<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = tokio::fs::File::create_new(&path).await?;
        Ok(Self {
            file: file.compat_write(),
            path,
            _phantom_data: PhantomData::default(),
        })
    }
}

impl TokioFile<ReadOnly> {
    pub async fn open<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = tokio::fs::File::open(&path).await?;
        Ok(Self {
            file: file.compat_write(),
            path,
            _phantom_data: PhantomData::default(),
        })
    }
}

impl<M> TokioFile<M> {
    pub async fn etag(&self) -> std::io::Result<Etag> {
        let metadata = self.file.get_ref().metadata().await?;
        (&metadata).try_into()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl<M> AsRef<tokio::fs::File> for TokioFile<M> {
    fn as_ref(&self) -> &tokio::fs::File {
        self.file.get_ref()
    }
}

impl<M: Writable> AsMut<tokio::fs::File> for TokioFile<M> {
    fn as_mut(&mut self) -> &mut tokio::fs::File {
        self.file.get_mut()
    }
}

impl<M: Readable + Unpin> AsyncRead for TokioFile<M> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.file).poll_read(cx, buf)
    }
}

impl<M: Writable + Unpin> AsyncWrite for TokioFile<M> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.file).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.file).poll_close(cx)
    }
}

impl<M: Readable + Unpin> AsyncSeek for TokioFile<M> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.file).poll_seek(cx, pos)
    }
}
