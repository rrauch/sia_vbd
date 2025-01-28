use crate::serde::framed::{FramedStream, FramingSink, InnerReader, ReadFrame, WriteFrame};
use crate::serde::protos::frame;
use crate::serde::{framed, protos, Body, BodyType, Compressed, Compression, Compressor};
use crate::vbd::wal::{FileHeader as WalInfo, TxBegin, TxCommit};
use crate::vbd::{Block, BlockId, Cluster, ClusterId, Commit, CommitId, FixedSpecs, Position};
use crate::{AsyncReadExtBuffered, WrappedReader};
use bytes::{BufMut, Bytes, BytesMut};
use futures::future::BoxFuture;
use futures::io::BufReader;
use futures::lock::OwnedMutexGuard;
use futures::{AsyncRead, AsyncWrite, Sink, Stream};
use pin_project_lite::pin_project;
use prost::{DecodeError, Message};
use std::fmt::{Display, Formatter};
use std::io::ErrorKind;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{io, mem};
use thiserror::Error;

pub(crate) struct DecodedReadFrame<T: Send, R: InnerReader, B: Send> {
    header: T,
    body: Option<Body>,
    inner: ReadFrame<R>,
    fixed_specs: FixedSpecs,
    _phantom_data: PhantomData<B>,
}

impl<'a, T: Send, R: InnerReader + 'a, B: Send> DecodedReadFrame<T, R, B> {
    fn new(header: T, body: Option<Body>, inner: ReadFrame<R>, fixed_specs: FixedSpecs) -> Self {
        Self {
            header,
            body,
            inner,
            fixed_specs,
            _phantom_data: PhantomData::default(),
        }
    }

    pub fn position(&self) -> &Position<u64, u32> {
        self.inner.position()
    }

    pub fn header(&self) -> &T {
        &self.header
    }

    pub fn into_header(self) -> T {
        self.header
    }

    pub fn body(&self) -> Option<&Body> {
        self.body.as_ref()
    }

    async fn read_body_bytes(&mut self) -> Result<Bytes, DecodingError> {
        let body_len = self.inner.body_len();
        if body_len == 0 {
            return Err(DecodingError::MissingBody);
        }

        let body = self.body.as_ref().ok_or(DecodingError::MissingBody)?;
        let reader = self.inner.body().await?.ok_or(DecodingError::MissingBody)?;
        let (mut reader, body_len) = match body
            .compressed
            .as_ref()
            .map(|c| (&c.compression, c.uncompressed))
        {
            Some((&Compression::Zstd, uncompressed)) => (
                Box::new(async_compression::futures::bufread::ZstdDecoder::new(
                    BufReader::new(reader),
                )) as Box<dyn AsyncRead + Send + Unpin>,
                uncompressed as usize,
            ),
            None => (
                Box::new(reader) as Box<dyn AsyncRead + Send + Unpin>,
                body_len as usize,
            ),
        };

        let mut buf = BytesMut::with_capacity(body_len);
        reader.read_exact_buffered(&mut buf, body_len).await?;
        Ok(buf.freeze())
    }
}

impl<'a, R: InnerReader + 'a> DecodedReadFrame<BlockId, R, Bytes> {
    pub async fn read_body(&mut self) -> Result<Block, DecodingError> {
        let fixed_specs = self.fixed_specs.clone();
        let block = Block::from_bytes(&fixed_specs, self.read_body_bytes().await?);

        if &self.header != block.content_id() {
            return Err(BodyError::BlockIdMismatch(
                self.header.clone(),
                block.content_id().clone(),
            ))?;
        }

        Ok(block)
    }
}

impl<'a, R: InnerReader + 'a> DecodedReadFrame<ClusterId, R, Cluster> {
    pub async fn read_body(&mut self) -> Result<Cluster, DecodingError> {
        let mut bytes = self.read_body_bytes().await?;
        let proto_cluster = protos::Cluster::decode(&mut bytes)?;
        let cluster: Cluster = (proto_cluster, self.fixed_specs.clone()).try_into()?;
        if &self.header != cluster.content_id() {
            return Err(BodyError::ClusterIdMismatch(
                self.header.clone(),
                cluster.content_id().clone(),
            ))?;
        }
        Ok(cluster)
    }
}

impl<'a, R: InnerReader + 'a> DecodedReadFrame<CommitId, R, Commit> {
    pub async fn read_body(&mut self) -> Result<Commit, DecodingError> {
        let mut bytes = self.read_body_bytes().await?;
        let proto_commit = protos::Commit::decode(&mut bytes)?;
        let commit: Commit = (proto_commit, self.fixed_specs.clone()).try_into()?;
        if &self.header != commit.content_id() {
            return Err(BodyError::CommitIdMismatch(
                self.header.clone(),
                commit.content_id().clone(),
            ))?;
        }
        Ok(commit)
    }
}

pub(crate) struct DecodedStream<'a, T: InnerReader> {
    state: StreamState<'a, T>,
    buf: ReusableBuffer<{ (framed::MAX_HEADER_LEN + 256) as usize }>,
    fixed_specs: FixedSpecs,
}

enum StreamState<'a, T: InnerReader> {
    New(FramedStream<'a, T>),
    ReadingFrame(FramedStream<'a, T>),
    Decoding(
        FramedStream<'a, T>,
        BoxFuture<'a, Result<Decoded<WrappedReader<OwnedMutexGuard<T>>>, DecodingError>>,
    ),
    Done,
}

impl<'a, T: InnerReader + 'a> DecodedStream<'a, T> {
    pub fn from_stream(inner: FramedStream<'a, T>, fixed_specs: FixedSpecs) -> Self {
        Self {
            state: StreamState::New(inner),
            buf: ReusableBuffer::new(),
            fixed_specs,
        }
    }

    pub fn from_reader(reader: T, fixed_specs: FixedSpecs) -> Self {
        Self::from_stream(FramedStream::new(reader), fixed_specs)
    }

    fn decoding(
        mut frame: ReadFrame<WrappedReader<OwnedMutexGuard<T>>>,
        mut buf: BytesMut,
        fixed_specs: FixedSpecs,
    ) -> BoxFuture<'a, Result<Decoded<WrappedReader<OwnedMutexGuard<T>>>, DecodingError>> {
        Box::pin(async move {
            buf.clear();
            let mut reader = frame.header().await?;
            reader
                .read_exact_buffered(&mut buf, frame.header_len() as usize)
                .await?;
            let header = protos::frame::Header::decode(&mut buf)?;
            let body = header.body.map(|b| b.try_into()).transpose()?;
            if let Some(t) = header.r#type {
                Ok(match t {
                    frame::header::Type::Commit(commit) => {
                        let commit_id: CommitId = commit.try_into()?;
                        let frame = DecodedReadFrame::new(commit_id, body, frame, fixed_specs);
                        Decoded::Commit(frame)
                    }
                    frame::header::Type::Cluster(cluster) => {
                        let cluster_id: ClusterId = cluster.try_into()?;
                        let frame = DecodedReadFrame::new(cluster_id, body, frame, fixed_specs);
                        Decoded::Cluster(frame)
                    }
                    frame::header::Type::Block(block) => {
                        let block_id: BlockId = block.try_into()?;
                        let frame = DecodedReadFrame::new(block_id, body, frame, fixed_specs);
                        Decoded::Block(frame)
                    }
                    frame::header::Type::TxBegin(tx_begin) => {
                        let tx_begin: TxBegin = tx_begin.try_into()?;
                        let frame = DecodedReadFrame::new(tx_begin, body, frame, fixed_specs);
                        Decoded::TxBegin(frame)
                    }
                    frame::header::Type::TxCommit(tx_commit) => {
                        let tx_commit: TxCommit = tx_commit.try_into()?;
                        let frame = DecodedReadFrame::new(tx_commit, body, frame, fixed_specs);
                        Decoded::TxCommit(frame)
                    }
                    frame::header::Type::WalInfo(wal_info) => {
                        let wal_info: WalInfo = wal_info.try_into()?;
                        let frame = DecodedReadFrame::new(wal_info, body, frame, fixed_specs);
                        Decoded::WalInfo(frame)
                    }
                })
            } else {
                Err(DecodingError::MissingHeader)
            }
        })
    }
}

impl<'a, T: InnerReader + 'a> Stream for DecodedStream<'a, T> {
    type Item = Result<Decoded<WrappedReader<OwnedMutexGuard<T>>>, DecodingError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match mem::replace(&mut self.state, StreamState::Done) {
                StreamState::New(inner) => {
                    self.state = StreamState::ReadingFrame(inner);
                    continue;
                }
                StreamState::ReadingFrame(mut inner) => match Pin::new(&mut inner).poll_next(cx) {
                    Poll::Pending => {
                        self.state = StreamState::ReadingFrame(inner);
                        return Poll::Pending;
                    }
                    Poll::Ready(Some(Ok(frame))) => {
                        self.state = StreamState::Decoding(
                            inner,
                            Self::decoding(frame, self.buf.get(), self.fixed_specs.clone()),
                        );
                        continue;
                    }
                    Poll::Ready(Some(Err(err))) => {
                        return Poll::Ready(Some(Err(err.into())));
                    }
                    Poll::Ready(None) => {
                        return Poll::Ready(None);
                    }
                },
                StreamState::Decoding(inner, mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.state = StreamState::Decoding(inner, fut);
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(frame)) => {
                        self.state = StreamState::ReadingFrame(inner);
                        return Poll::Ready(Some(Ok(frame)));
                    }
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Some(Err(err.into())));
                    }
                },
                StreamState::Done => {
                    panic!("polled after completion");
                }
            }
        }
    }
}

#[derive(Error, Debug)]
pub(crate) enum DecodingError {
    #[error("Frame does not have a header")]
    MissingHeader,
    #[error("Frame does not have a body")]
    MissingBody,
    #[error(transparent)]
    BodyError(#[from] BodyError),
    #[error(transparent)]
    FrameError(#[from] frame::Error),
    #[error(transparent)]
    ProtoError(#[from] DecodeError),
    #[error(transparent)]
    WalHeaderError(#[from] crate::vbd::wal::HeaderError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub(crate) enum BodyError {
    #[error("BlockId Mismatch: [{0}] != [{1}]")]
    BlockIdMismatch(BlockId, BlockId),
    #[error("ClusterId Mismatch: [{0}] != [{1}]")]
    ClusterIdMismatch(ClusterId, ClusterId),
    #[error("CommitId Mismatch: [{0}] != [{1}]")]
    CommitIdMismatch(CommitId, CommitId),
}

pub(crate) enum Decoded<R: InnerReader> {
    TxBegin(DecodedReadFrame<TxBegin, R, ()>),
    TxCommit(DecodedReadFrame<TxCommit, R, ()>),
    Block(DecodedReadFrame<BlockId, R, Bytes>),
    Cluster(DecodedReadFrame<ClusterId, R, Cluster>),
    Commit(DecodedReadFrame<CommitId, R, Commit>),
    WalInfo(DecodedReadFrame<WalInfo, R, ()>),
}

impl<R: InnerReader> Decoded<R> {
    pub fn position(&self) -> &Position<u64, u32> {
        match &self {
            Self::TxBegin(f) => f.position(),
            Self::TxCommit(f) => f.position(),
            Self::Block(f) => f.position(),
            Self::Cluster(f) => f.position(),
            Self::Commit(f) => f.position(),
            Self::WalInfo(f) => f.position(),
        }
    }

    pub fn body(&self) -> Option<&Body> {
        match &self {
            Self::TxBegin(f) => f.body.as_ref(),
            Self::TxCommit(f) => f.body.as_ref(),
            Self::Block(f) => f.body.as_ref(),
            Self::Cluster(f) => f.body.as_ref(),
            Self::Commit(f) => f.body.as_ref(),
            Self::WalInfo(f) => f.body.as_ref(),
        }
    }
}

struct Converter {
    head_buf: ReusableBuffer<{ (framed::MAX_HEADER_LEN + 256) as usize }>,
    body_buf: ReusableBuffer<{ 1024 * 1024 }>,
    compressor: Option<Compressor>,
}

impl Converter {
    fn new(compressor: Option<Compressor>) -> Self {
        Self {
            head_buf: ReusableBuffer::new(),
            body_buf: ReusableBuffer::new(),
            compressor,
        }
    }

    fn compression(&self, size: u64) -> Option<Compression> {
        match self.compressor.as_ref() {
            Some(c) if size >= c.size_threshold() => Some(c.compression().clone()),
            _ => None,
        }
    }

    fn try_convert(
        &mut self,
        encodable: Encodable,
    ) -> Result<WriteFrame<'static>, prost::EncodeError> {
        let (header, mut body, compress) = match encodable {
            Encodable::TxBegin(tx_begin) => (
                frame::Header {
                    r#type: Some(frame::header::Type::TxBegin(tx_begin.into())),
                    body: None,
                },
                None,
                false,
            ),
            Encodable::TxCommit(tx_commit) => (
                frame::Header {
                    r#type: Some(frame::header::Type::TxCommit(tx_commit.into())),
                    body: None,
                },
                None,
                false,
            ),
            Encodable::Block(block) => {
                let compression = self.compression(block.len());
                let header = frame::Header {
                    r#type: Some(frame::header::Type::Block(block.content_id().into())),
                    body: Some(
                        (&Body {
                            body_type: BodyType::BlockContent,
                            compressed: compression.as_ref().map(|c| {
                                Compressed {
                                    compression: c.clone(),
                                    uncompressed: block.len(),
                                }
                                .into()
                            }),
                        })
                            .into(),
                    ),
                };
                (header, Some(block.data().clone()), compression.is_some())
            }
            Encodable::Cluster(cluster) => {
                let mut buf = self.body_buf.get();
                Into::<protos::Cluster>::into(cluster).encode(&mut buf)?;
                let bytes = buf.freeze();
                let compression = self.compression(bytes.len() as u64);
                let header = frame::Header {
                    r#type: Some(frame::header::Type::Cluster(cluster.content_id().into())),
                    body: Some(
                        (&Body {
                            body_type: BodyType::Cluster,
                            compressed: compression.as_ref().map(|c| {
                                Compressed {
                                    compression: c.clone(),
                                    uncompressed: bytes.len() as u64,
                                }
                                .into()
                            }),
                        })
                            .into(),
                    ),
                };
                (header, Some(bytes), compression.is_some())
            }
            Encodable::Commit(commit) => {
                let mut buf = self.body_buf.get();
                Into::<protos::Commit>::into(commit).encode(&mut buf)?;
                let bytes = buf.freeze();
                let compression = self.compression(bytes.len() as u64);
                let header = frame::Header {
                    r#type: Some(frame::header::Type::Commit(commit.content_id().into())),
                    body: Some(
                        (&Body {
                            body_type: BodyType::Commit,
                            compressed: compression.as_ref().map(|c| {
                                Compressed {
                                    compression: c.clone(),
                                    uncompressed: bytes.len() as u64,
                                }
                                .into()
                            }),
                        })
                            .into(),
                    ),
                };
                (header, Some(bytes), compression.is_some())
            }
            Encodable::WalInfo(wal_info) => (
                frame::Header {
                    r#type: Some(frame::header::Type::WalInfo(wal_info.into())),
                    body: None,
                },
                None,
                false,
            ),
        };
        let mut head_buf = self.head_buf.get();
        header.encode(&mut head_buf)?;
        let header = head_buf.freeze();

        if body.is_some() && compress {
            if let Some(compressor) = self.compressor.as_ref() {
                let uncompressed_body = body.take().unwrap();
                let compressed_body = compressor.compress(&uncompressed_body);

                body = Some(if compressed_body.len() < uncompressed_body.len() {
                    compressed_body
                } else {
                    uncompressed_body
                });
            }
        }

        Ok(match body {
            Some(body) => WriteFrame::full(header, body),
            None => WriteFrame::header_only(header),
        })
    }
}

pub(crate) enum Encodable<'a> {
    TxBegin(&'a TxBegin),
    TxCommit(&'a TxCommit),
    Block(&'a Block),
    Cluster(&'a Cluster),
    Commit(&'a Commit),
    WalInfo(&'a WalInfo),
}

impl<'a> From<&'a TxBegin> for Encodable<'a> {
    fn from(value: &'a TxBegin) -> Self {
        Encodable::TxBegin(value)
    }
}

impl<'a> From<&'a TxCommit> for Encodable<'a> {
    fn from(value: &'a TxCommit) -> Self {
        Encodable::TxCommit(value)
    }
}

impl<'a> From<&'a Block> for Encodable<'a> {
    fn from(value: &'a Block) -> Self {
        Encodable::Block(value)
    }
}

impl<'a> From<&'a Cluster> for Encodable<'a> {
    fn from(value: &'a Cluster) -> Self {
        Encodable::Cluster(value)
    }
}

impl<'a> From<&'a Commit> for Encodable<'a> {
    fn from(value: &'a Commit) -> Self {
        Encodable::Commit(value)
    }
}

impl<'a> From<&'a WalInfo> for Encodable<'a> {
    fn from(value: &'a WalInfo) -> Self {
        Encodable::WalInfo(value)
    }
}

impl<'a> Display for Encodable<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TxBegin(tx_begin) => {
                write!(
                    f,
                    "TxBegin[id={}, preceding_content_id={}, created={}]",
                    tx_begin.transaction_id, tx_begin.preceding_content_id, tx_begin.created
                )
            }
            Self::TxCommit(tx_commit) => {
                write!(
                    f,
                    "TxCommit[id={}, content_id={}, committed={}]",
                    tx_commit.transaction_id, tx_commit.content_id, tx_commit.committed
                )
            }
            Self::Block(block) => {
                write!(f, "Block[id={}, len={}]", block.content_id(), block.len())
            }
            Self::Cluster(cluster) => {
                write!(
                    f,
                    "Cluster[content_id={}, len={}]",
                    cluster.content_id(),
                    cluster.len()
                )
            }
            Self::Commit(commit) => {
                write!(
                    f,
                    "Commit[content_id={}, len={}]",
                    commit.content_id(),
                    commit.len()
                )
            }
            Self::WalInfo(wal_info) => {
                write!(
                    f,
                    "WalInfo[id={}, created={}, preceding_wal_id={:?}]",
                    wal_info.wal_id, wal_info.created, wal_info.preceding_wal_id
                )
            }
        }
    }
}

pub(crate) struct EncodingSinkBuilder<T> {
    inner: T,
    compressor: Option<Compressor>,
}

impl<T: AsyncWrite + Send> EncodingSinkBuilder<FramingSink<'_, T>> {
    pub fn from_writer(writer: T) -> Self {
        Self::from_sink(FramingSink::new(writer))
    }
}

impl<'a, T: Sink<WriteFrame<'a>> + Send> EncodingSinkBuilder<T> {
    pub fn from_sink(inner: T) -> Self {
        Self {
            inner,
            compressor: None,
        }
    }
}

impl<T> EncodingSinkBuilder<T> {
    pub fn with_compressor(mut self, compressor: Compressor) -> Self {
        self.compressor = Some(compressor);
        self
    }

    pub fn build(self) -> EncodingSink<T> {
        EncodingSink {
            inner: self.inner,
            converter: Converter::new(self.compressor),
        }
    }
}

pin_project! {
    pub(crate) struct EncodingSink<T> {
        #[pin]
        inner: T,
        converter: Converter,
    }
}

impl<T> EncodingSink<T> {
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T> Sink<Encodable<'_>> for EncodingSink<T>
where
    T: Sink<WriteFrame<'static>, Error = io::Error> + Send,
{
    type Error = T::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: Encodable) -> Result<(), Self::Error> {
        let this = self.project();
        let write_frame = this
            .converter
            .try_convert(item)
            .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;
        this.inner.start_send(write_frame)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.project().inner.poll_flush(cx)
    }
}

struct ReusableBuffer<const S: usize> {
    buf: Option<BytesMut>,
}

impl<const S: usize> ReusableBuffer<S> {
    fn new() -> Self {
        Self { buf: None }
    }

    fn get(&mut self) -> BytesMut {
        if self.buf.is_none() {
            self.buf = Some(BytesMut::with_capacity(S));
        }
        let buf = self.buf.as_mut().unwrap();
        buf.clear();

        if buf.remaining_mut() < S {
            let additional = S - buf.remaining_mut();
            if !buf.try_reclaim(additional) {
                buf.reserve(additional)
            }
        }
        buf.split()
    }
}

#[derive(Error, Debug)]
pub(crate) enum EncodingError {
    #[error(transparent)]
    FrameError(#[from] frame::Error),
    #[error(transparent)]
    ProtoError(#[from] DecodeError),
    #[error(transparent)]
    IoError(#[from] io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
