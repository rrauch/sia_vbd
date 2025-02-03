pub(crate) mod man;
pub(crate) mod protos;
pub(crate) mod reader;
pub(crate) mod writer;

use crate::vbd::{BlockId, ClusterId, CommitId, FixedSpecs, TypedUuid, VbdId};
use crate::Etag;
use chrono::{DateTime, Duration, Utc};
use futures::{AsyncRead, AsyncSeek, AsyncWrite};
use prost::DecodeError;
use std::collections::HashMap;
use std::future::Future;
use std::io::{Error, SeekFrom};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio_util::compat::TokioAsyncWriteCompatExt;

const MAGIC_NUMBER: &'static [u8; 16] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x57, 0x41, 0x4C, 0x00, 0x00, 0x01,
];

pub(crate) trait WalSink: AsyncWrite + AsyncSeek + Unpin + Send {
    fn len(&self) -> impl Future<Output = Result<u64, std::io::Error>> + Send;
    fn set_len(&mut self, len: u64) -> impl Future<Output = Result<(), std::io::Error>> + Send;
}

trait Readable {}
trait Writable {}

pub struct ReadOnly;
impl Readable for ReadOnly {}

pub struct ReadWrite;
impl Readable for ReadWrite {}
impl Writable for ReadWrite {}

pub(crate) struct TokioWalFile<M> {
    file: tokio_util::compat::Compat<tokio::fs::File>,
    path: PathBuf,
    _phantom_data: PhantomData<M>,
}

impl TokioWalFile<ReadWrite> {
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

impl TokioWalFile<ReadOnly> {
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

impl<M> TokioWalFile<M> {
    pub async fn etag(&self) -> std::io::Result<Etag> {
        let metadata = self.file.get_ref().metadata().await?;
        (&metadata).try_into()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl<M: Readable + Unpin> AsyncRead for TokioWalFile<M> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.file).poll_read(cx, buf)
    }
}

impl<M: Writable + Unpin> AsyncWrite for TokioWalFile<M> {
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

impl<M: Readable + Unpin> AsyncSeek for TokioWalFile<M> {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.file).poll_seek(cx, pos)
    }
}

impl WalSink for TokioWalFile<ReadWrite> {
    async fn len(&self) -> Result<u64, Error> {
        Ok(self.file.get_ref().metadata().await?.len())
    }

    async fn set_len(&mut self, len: u64) -> Result<(), Error> {
        self.file.get_mut().set_len(len).await
    }
}

pub trait WalSource: AsyncRead + AsyncSeek + Unpin + Send {}
impl<T: AsyncRead + AsyncSeek + Unpin + Send> WalSource for T {}

pub struct Wal {}
pub type WalId = TypedUuid<Wal>;
pub type WalReader = reader::WalReader<TokioWalFile<ReadOnly>>;
pub type WalWriter = writer::WalWriter<TokioWalFile<ReadWrite>>;

pub struct TxDetails {
    pub tx_id: TxId,
    pub wal_id: WalId,
    pub vbd_id: VbdId,
    pub commit_id: CommitId,
    pub preceding_commit_id: CommitId,
    pub created: DateTime<Utc>,
    pub committed: DateTime<Utc>,
    pub blocks: HashMap<BlockId, u64>,
    pub clusters: HashMap<ClusterId, u64>,
    pub commits: HashMap<CommitId, u64>,
}

impl TxDetails {
    pub fn duration(&self) -> Duration {
        self.committed - self.created
    }
}

#[derive(Clone)]
struct TxDetailBuilder {
    tx_id: TxId,
    wal_id: WalId,
    vbd_id: VbdId,
    preceding_commit_id: CommitId,
    created: DateTime<Utc>,
    blocks: HashMap<BlockId, u64>,
    clusters: HashMap<ClusterId, u64>,
    commits: HashMap<CommitId, u64>,
}

impl TxDetailBuilder {
    fn new(
        tx_id: TxId,
        wal_id: WalId,
        vbd_id: VbdId,
        preceding_commit_id: CommitId,
        created: DateTime<Utc>,
    ) -> Self {
        Self {
            tx_id,
            wal_id,
            vbd_id,
            preceding_commit_id,
            created,
            blocks: HashMap::default(),
            clusters: HashMap::default(),
            commits: HashMap::default(),
        }
    }

    fn build(self, commit: CommitId, committed: DateTime<Utc>) -> TxDetails {
        TxDetails {
            tx_id: self.tx_id,
            wal_id: self.wal_id,
            vbd_id: self.vbd_id,
            commit_id: commit,
            preceding_commit_id: self.preceding_commit_id,
            created: self.created,
            committed,
            blocks: self.blocks,
            clusters: self.clusters,
            commits: self.commits,
        }
    }
}

pub struct Tx_ {}

pub type TxId = TypedUuid<Tx_>;

pub(crate) struct TxBegin {
    pub transaction_id: TxId,
    pub preceding_content_id: CommitId,
    pub created: DateTime<Utc>,
}

pub(crate) struct TxCommit {
    pub transaction_id: TxId,
    pub content_id: CommitId,
    pub committed: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct FileHeader {
    pub wal_id: WalId,
    pub specs: FixedSpecs,
    pub created: DateTime<Utc>,
    pub preceding_wal_id: Option<WalId>,
}

#[derive(Error, Debug)]
pub(crate) enum WalError {
    /// WAL File Parse error
    #[error(transparent)]
    ParseError(#[from] ParseError),
    #[error(transparent)]
    DecodingError(#[from] crate::serde::encoded::DecodingError),
    /// WAL File Encode error
    #[error(transparent)]
    EncodeError(#[from] EncodeError),
    /// An `IO` error occurred reading from or writing to the wal
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    /// Rolling back the last transaction failed
    #[error(transparent)]
    RollbackError(#[from] RollbackError),
    /// Unexpected frame type found
    #[error("Frame type {exp} expected, but found different frame type")]
    UnexpectedFrameType { exp: String },
    /// Incorrect preceding commit found
    #[error(
        "Preceding commit with content id {exp} expected, but found {found} content id instead"
    )]
    IncorrectPrecedingCommit { exp: CommitId, found: CommitId },
    /// Dangling Transaction detected
    #[error("Dangling tx detected, tx [{tx_id}] never committed")]
    DanglingTxDetected { tx_id: TxId },
    #[error("TxId {exp} expected, but found {found} instead")]
    IncorrectTxId { exp: TxId, found: TxId },
    #[error("Incorrect type encountered")]
    IncorrectType,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub enum RollbackError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("transaction state unclear")]
    UnclearState,
}

#[derive(Error, Debug)]
pub enum EncodeError {
    /// Wal space insufficient
    #[error("Wal space insufficient, required {req} != {rem} remaining")]
    WalSpaceInsufficient { req: u64, rem: u64 },
    /// Max Wal size exceeded
    #[error("Maximum Wal size exceeded")]
    MaxWalSizeExceeded,
    /// Protobuf related parsing error
    #[error(transparent)]
    ProtoError(#[from] prost::EncodeError),
}

#[derive(Error, Debug)]
pub enum ParseError {
    /// Invalid Magic Number
    #[error("Invalid Magic Number")]
    InvalidMagicNumber,
    /// Header error
    #[error(transparent)]
    HeaderError(#[from] HeaderError),
    /// Frame error
    #[error(transparent)]
    FrameError(#[from] crate::serde::protos::frame::Error),
    /// Protobuf related parsing error
    #[error(transparent)]
    ProtoError(#[from] DecodeError),
}

#[derive(Error, Debug)]
pub enum HeaderError {
    /// File Id Missing or Invalid
    #[error("File Id Missing or Invalid")]
    FileIdInvalid,
    /// Vbd Specs Missing or Invalid
    #[error("Vbd Specs Missing or Invalid")]
    VbdSpecsInvalid,
    /// Created Missing or Invalid
    #[error("Creation Timestamp Missing or Invalid")]
    CreatedInvalid,
}

#[derive(Error, Debug)]
pub enum CommitFrameError {
    /// Content Id Invalid
    #[error("Content Id Invalid")]
    ContentIdInvalid,
    /// Transaction Id Invalid
    #[error("Transaction Id Invalid")]
    TransactionIdInvalid,
    /// Timestamp Invalid
    #[error("Timestamp Invalid")]
    TimestampInvalid,
}

#[derive(Error, Debug)]
pub enum BlockFrameError {
    /// Content Id Invalid
    #[error("Content Id Invalid")]
    ContentIdInvalid,
    /// Body Length Mismatch
    #[error("body length mismatch, {exp} != {found}")]
    BodyLengthMismatch { exp: u32, found: u32 },
}

#[derive(Error, Debug)]
pub enum ClusterFrameError {
    /// Content Id Invalid
    #[error("Content Id Invalid")]
    ContentIdInvalid,
}

#[derive(Error, Debug)]
pub enum StateFrameError {
    /// Content Id Invalid
    #[error("Content Id Invalid")]
    ContentIdInvalid,
}
