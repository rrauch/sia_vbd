mod protos;
pub(crate) mod reader;
pub(crate) mod writer;

use crate::vbd::{BlockId, ClusterId, Commit, FixedSpecs, Position, TypedUuid, VbdId};
use chrono::{DateTime, Duration, Utc};
use futures::{AsyncRead, AsyncSeek, AsyncWrite};
use prost::DecodeError;
use std::collections::HashMap;
use std::future::Future;
use std::io::{Error, SeekFrom};
use std::marker::PhantomData;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio_util::compat::TokioAsyncWriteCompatExt;

const PREAMBLE_LEN: usize = 14;
const VALID_HEADER_LEN: Range<u16> = 8..2048;
const VALID_BODY_LEN: Range<u32> = 0..1024 * 1024 * 100;
const VALID_PADDING_LEN: Range<u32> = 0..1024 * 1024 * 10;

const MAGIC_NUMBER: &'static [u8; 18] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x57, 0x41, 0x4C, 0x00, 0x00, 0x00,
    0x00, 0x01,
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

pub struct TokioWalFile<M> {
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
    pub fn as_file(&self) -> &tokio::fs::File {
        self.file.get_ref()
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

pub struct TxDetails {
    pub tx_id: TxId,
    pub wal_id: WalId,
    pub vbd_id: VbdId,
    pub commit: Commit,
    pub preceding_commit: Commit,
    pub created: DateTime<Utc>,
    pub committed: DateTime<Utc>,
    //pub position: Position<u64, u64>,
    pub blocks: HashMap<BlockId, Position<u64, u32>>,
    pub clusters: HashMap<ClusterId, Position<u64, u32>>,
    pub states: HashMap<Commit, Position<u64, u32>>,
}

impl TxDetails {
    pub fn duration(&self) -> Duration {
        self.committed - self.created
    }

    /*pub fn len(&self) -> u64 {
        self.position.length
    }*/
}

#[derive(Clone)]
struct TxDetailBuilder {
    tx_id: TxId,
    wal_id: WalId,
    vbd_id: VbdId,
    preceding_commit: Commit,
    created: DateTime<Utc>,
    blocks: HashMap<BlockId, Position<u64, u32>>,
    clusters: HashMap<ClusterId, Position<u64, u32>>,
    states: HashMap<Commit, Position<u64, u32>>,
}

impl TxDetailBuilder {
    fn new(
        tx_id: TxId,
        wal_id: WalId,
        vbd_id: VbdId,
        preceding_commit: Commit,
        created: DateTime<Utc>,
    ) -> Self {
        Self {
            tx_id,
            wal_id,
            vbd_id,
            preceding_commit,
            created,
            blocks: HashMap::default(),
            clusters: HashMap::default(),
            states: HashMap::default(),
        }
    }

    fn build(self, commit: Commit, committed: DateTime<Utc>) -> TxDetails {
        TxDetails {
            tx_id: self.tx_id,
            wal_id: self.wal_id,
            vbd_id: self.vbd_id,
            commit,
            preceding_commit: self.preceding_commit,
            created: self.created,
            committed,
            blocks: self.blocks,
            clusters: self.clusters,
            states: self.states,
        }
    }
}

pub struct Tx_ {}

pub type TxId = TypedUuid<Tx_>;

struct TxBegin {
    transaction_id: TxId,
    preceding_content_id: Commit,
    created: DateTime<Utc>,
}

struct TxCommit {
    transaction_id: TxId,
    content_id: Commit,
    committed: DateTime<Utc>,
}

struct Block {
    content_id: BlockId,
    length: u32,
}

#[derive(Debug)]
pub struct FileHeader {
    pub wal_id: WalId,
    pub specs: FixedSpecs,
    pub created: DateTime<Utc>,
    pub preceding_wal_id: Option<WalId>,
}

#[derive(Debug)]
struct Preamble {
    header: u16,
    padding1: u32,
    body: u32,
    padding2: u32,
}

impl Preamble {
    fn header_only<H: Into<u16>>(header: H) -> Self {
        Self {
            header: header.into(),
            padding1: 0,
            body: 0,
            padding2: 0,
        }
    }

    fn full<H: Into<u16>, P1: Into<Option<u32>>, B: Into<Option<u32>>, P2: Into<Option<u32>>>(
        header: H,
        padding1: P1,
        body: B,
        padding2: P2,
    ) -> Self {
        Self {
            header: header.into(),
            padding1: padding1.into().unwrap_or(0),
            body: body.into().unwrap_or(0),
            padding2: padding2.into().unwrap_or(0),
        }
    }

    fn content_len(&self) -> u64 {
        self.header as u64 + self.padding1 as u64 + self.body as u64 + self.padding2 as u64
    }
}

#[derive(Error, Debug)]
pub enum WalError {
    /// WAL File Parse error
    #[error("WAL File Parse error")]
    ParseError(#[from] ParseError),
    /// WAL File Encode error
    #[error("WAL File Encode error")]
    EncodeError(#[from] EncodeError),
    /// An `IO` error occurred reading from or writing to the wal
    #[error("io error")]
    IoError(#[from] std::io::Error),
    /// Rolling back the last transaction failed
    #[error("io error")]
    RollbackError(#[from] RollbackError),
    /// Unexpected frame type found
    #[error("Frame type {exp} expected, but found different frame type")]
    UnexpectedFrameType { exp: String },
    /// Incorrect preceding commit found
    #[error(
        "Preceding commit with content id {exp} expected, but found {found} content id instead"
    )]
    IncorrectPrecedingCommit { exp: Commit, found: Commit },
    /// Dangling Transaction detected
    #[error("Dangling tx detected, tx [{tx_id}] never committed")]
    DanglingTxDetected { tx_id: TxId },
    #[error("TxId {exp} expected, but found {found} instead")]
    IncorrectTxId { exp: TxId, found: TxId },
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub enum RollbackError {
    #[error("io error")]
    IoError(#[from] std::io::Error),
    #[error("transaction state unclear")]
    UnclearState,
}

#[derive(Error, Debug)]
pub enum EncodeError {
    /// Buffer size insufficient
    #[error("Buffer size too small, required {req} != {rem} remaining")]
    BufferTooSmall { req: usize, rem: usize },
    /// Wal space insufficient
    #[error("Wal space insufficient, required {req} != {rem} remaining")]
    WalSpaceInsufficient { req: u64, rem: u64 },
    /// Max Wal size exceeded
    #[error("Maximum Wal size exceeded")]
    MaxWalSizeExceeded,
    /// Protobuf related parsing error
    #[error("Protobuf related parsing error")]
    ProtoError(#[from] prost::EncodeError),
}

#[derive(Error, Debug)]
pub enum ParseError {
    /// Invalid Magic Number
    #[error("Invalid Magic Number")]
    InvalidMagicNumber,
    /// Preamble error
    #[error("Preamble error")]
    PreambleError(#[from] PreambleError),
    /// Header error
    #[error("Header error")]
    HeaderError(#[from] HeaderError),
    /// Frame error
    #[error("Frame error")]
    FrameError(#[from] FrameError),
    /// Buffer size insufficient
    #[error("Buffer size too small, required {req} != {rem} remaining")]
    BufferTooSmall { req: usize, rem: usize },
    /// Protobuf related parsing error
    #[error("Protobuf related parsing error")]
    ProtoError(#[from] DecodeError),
}

#[derive(Error, Debug)]
pub enum PreambleError {
    /// Invalid Preamble Length
    #[error("Invalid Preamble Length")]
    InvalidLength,
    /// Invalid Header Length
    #[error("header length {len} needs to be in range {min}-{max}")]
    InvalidHeaderLength { len: u16, min: u16, max: u16 },
    /// Invalid Body Length
    #[error("body length {len} needs to be in range {min}-{max}")]
    InvalidBodyLength { len: u32, min: u32, max: u32 },
    /// Invalid Padding Length
    #[error("padding length {len} needs to be in range {min}-{max}")]
    InvalidPaddingLength { len: u32, min: u32, max: u32 },
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
pub enum FrameError {
    /// Frame Type Invalid
    #[error("File Type missing or invalid")]
    FrameTypeInvalid,
    /// Commit Frame error
    #[error("Commit Frame error")]
    CommitFrameError(#[from] CommitFrameError),
    /// Block Frame error
    #[error("Block Frame error")]
    BlockFrameError(#[from] BlockFrameError),
    /// Cluster Frame error
    #[error("Cluster Frame error")]
    ClusterFrameError(#[from] ClusterFrameError),
    /// State Frame error
    #[error("State Frame error")]
    StateFrameError(#[from] StateFrameError),
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
