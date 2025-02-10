use crate::ClientEndpoint;
use async_trait::async_trait;
use futures::AsyncRead;
use std::result;
use thiserror::Error;

pub mod read_reply {
    use crate::nbd::block_device::read_reply::Payload::Zeroes;
    use crate::nbd::transmission::fragment::Fragment;
    use async_trait::async_trait;
    use bytes::{Buf, Bytes, BytesMut};
    use futures::{AsyncWrite, AsyncWriteExt};
    use tokio::sync::mpsc;

    #[async_trait]
    pub trait PayloadWriter: Send + Sync {
        async fn write(
            self: Box<Self>,
            out: &mut (dyn AsyncWrite + Send + Unpin),
        ) -> std::io::Result<()>;
    }

    impl<T: PayloadWriter + Send + Sync + 'static> From<T> for Payload {
        fn from(value: T) -> Self {
            Payload::from_writer(value)
        }
    }

    pub(in crate::nbd) enum Payload {
        Writer(Box<dyn PayloadWriter>),
        Zeroes,
    }

    impl Payload {
        fn from_writer<T: PayloadWriter + Send + Sync + 'static>(writer: T) -> Self {
            Self::Writer(Box::new(writer))
        }
    }

    struct BufWriter<B> {
        inner: B,
    }

    impl<B: Buf + Send + Sync + 'static> BufWriter<B> {
        fn new(buf: B) -> Self {
            Self { inner: buf }
        }
    }

    #[async_trait]
    impl<B: Buf + Send + Sync + 'static> PayloadWriter for BufWriter<B> {
        async fn write(
            mut self: Box<Self>,
            out: &mut (dyn AsyncWrite + Send + Unpin),
        ) -> std::io::Result<()> {
            loop {
                let chunk = self.inner.chunk();
                if chunk.len() == 0 {
                    return Ok(());
                }
                out.write_all(chunk).await?;
                self.inner.advance(chunk.len());
            }
        }
    }

    impl From<Bytes> for Payload {
        fn from(value: Bytes) -> Self {
            BufWriter::new(value).into()
        }
    }

    impl From<BytesMut> for Payload {
        fn from(value: BytesMut) -> Self {
            value.freeze().into()
        }
    }

    impl From<Vec<u8>> for Payload {
        fn from(value: Vec<u8>) -> Self {
            Bytes::from(value).into()
        }
    }

    pub struct Queue {
        tx: mpsc::Sender<Fragment>,
    }

    impl Queue {
        pub(in crate::nbd) fn new(tx: mpsc::Sender<Fragment>) -> Self {
            Self { tx }
        }

        pub async fn zeroes(&mut self, offset: u64, length: u64) -> super::Result<()> {
            self.send_fragment(offset, length, Zeroes).await
        }

        #[allow(private_bounds)]
        pub async fn data<T: Into<Payload> + Send + Sync + 'static>(
            &mut self,
            offset: u64,
            length: u64,
            data: T,
        ) -> super::Result<()> {
            self.send_fragment(offset, length, data.into()).await
        }

        async fn send_fragment(
            &mut self,
            offset: u64,
            length: u64,
            data: Payload,
        ) -> super::Result<()> {
            self.tx
                .send(Fragment::new(offset, length, data))
                .await
                .map_err(|_| super::Error::ReadQueueError)?;
            Ok(())
        }
    }
}

#[derive(Debug, Clone)]
pub struct RequestContext {
    cookie: u64,
    client_endpoint: ClientEndpoint,
}

impl RequestContext {
    pub(super) fn new(cookie: u64, client_endpoint: ClientEndpoint) -> Self {
        Self {
            cookie,
            client_endpoint,
        }
    }

    pub fn cookie(&self) -> u64 {
        self.cookie
    }

    pub fn client_endpoint(&self) -> &ClientEndpoint {
        &self.client_endpoint
    }
}

#[derive(Debug, Clone)]
pub struct Options {
    /// Human readable description
    pub description: Option<String>,
    /// Size in bytes
    pub size: u64,
    /// Block Device is read-only
    pub read_only: bool,
    /// Block Device has characteristics of rotational media
    pub rotational: bool,
    /// `trim` is supported
    pub trim: bool,
    /// Fast zeroing is supported
    pub fast_zeroes: bool,
    /// Block Device can be resized
    pub resizable: bool,
    /// Ideal Block Size
    ///
    /// Must be a power of two and a multiple of [crate::nbd::MIN_BLOCK_SIZE]
    pub block_size: u32,
}

#[async_trait]
#[allow(unused_variables)]
pub trait BlockDevice {
    async fn options(&self) -> Options;

    async fn read(
        &self,
        offset: u64,
        length: u64,
        queue: &mut read_reply::Queue,
        _ctx: &RequestContext,
    ) -> Result<()>;

    async fn write(
        &self,
        offset: u64,
        length: u64,
        fua: bool,
        data: &mut (dyn AsyncRead + Send + Unpin),
        _ctx: &RequestContext,
    ) -> Result<()>;

    async fn write_zeroes(
        &self,
        offset: u64,
        length: u64,
        no_hole: bool,
        _ctx: &RequestContext,
    ) -> Result<()>;

    async fn flush(&self, _ctx: &RequestContext) -> Result<()>;

    async fn cache(&self, offset: u64, length: u64, _ctx: &RequestContext) -> Result<()> {
        Ok(())
    }

    async fn trim(&self, offset: u64, length: u64, _ctx: &RequestContext) -> Result<()> {
        unimplemented!("trim")
    }

    async fn resize(&self, new_size: u64, _ctx: &RequestContext) -> Result<()> {
        unimplemented!("resize")
    }

    async fn close(&mut self) -> Result<()>;
}

pub type Result<T> = result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("read queue closed prematurely")]
    ReadQueueError,
}
