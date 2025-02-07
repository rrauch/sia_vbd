pub(crate) mod man;
pub(crate) mod protos;
pub(crate) mod reader;
pub(crate) mod writer;

use crate::io::{ReadOnly, ReadWrite, TokioFile};
use crate::vbd::{
    BlockId, BranchName, ClusterId, Commit, CommitId, FixedSpecs, IndexId, TypedUuid, VbdId,
};
use chrono::{DateTime, Duration, Utc};
use futures::{AsyncRead, AsyncSeek, AsyncWrite};
use prost::DecodeError;
use std::collections::HashMap;
use std::future::Future;
use std::io::Error;
use thiserror::Error;

const MAGIC_NUMBER: &'static [u8; 16] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x57, 0x41, 0x4C, 0x00, 0x00, 0x01,
];

pub(crate) trait WalSink: AsyncWrite + AsyncSeek + Unpin + Send {
    fn len(&self) -> impl Future<Output = Result<u64, std::io::Error>> + Send;
    fn set_len(&mut self, len: u64) -> impl Future<Output = Result<(), std::io::Error>> + Send;
}

impl WalSink for TokioFile<ReadWrite> {
    async fn len(&self) -> Result<u64, Error> {
        Ok(self.as_ref().metadata().await?.len())
    }

    async fn set_len(&mut self, len: u64) -> Result<(), Error> {
        self.as_mut().set_len(len).await
    }
}

pub(crate) trait WalSource: AsyncRead + AsyncSeek + Unpin + Send {}
impl<T: AsyncRead + AsyncSeek + Unpin + Send> WalSource for T {}

pub(crate) struct Wal {}
pub(crate) type WalId = TypedUuid<Wal>;
pub(crate) type WalReader = reader::WalReader<TokioFile<ReadOnly>>;
pub(crate) type WalWriter = writer::WalWriter<TokioFile<ReadWrite>>;

pub(crate) struct TxDetails {
    pub tx_id: TxId,
    pub wal_id: WalId,
    pub vbd_id: VbdId,
    pub branch: BranchName,
    pub commit: Commit,
    pub preceding_commit: Commit,
    pub created: DateTime<Utc>,
    pub blocks: HashMap<BlockId, u64>,
    pub clusters: HashMap<ClusterId, u64>,
    pub indices: HashMap<IndexId, u64>,
}

impl TxDetails {
    pub fn duration(&self) -> Duration {
        self.commit.committed().clone() - self.created
    }
}

#[derive(Clone)]
struct TxDetailBuilder {
    tx_id: TxId,
    wal_id: WalId,
    vbd_id: VbdId,
    branch: BranchName,
    preceding_commit: Commit,
    created: DateTime<Utc>,
    blocks: HashMap<BlockId, u64>,
    clusters: HashMap<ClusterId, u64>,
    indices: HashMap<IndexId, u64>,
}

impl TxDetailBuilder {
    fn new(
        tx_id: TxId,
        wal_id: WalId,
        vbd_id: VbdId,
        branch: BranchName,
        preceding_commit: Commit,
        created: DateTime<Utc>,
    ) -> Self {
        Self {
            tx_id,
            wal_id,
            vbd_id,
            branch,
            preceding_commit,
            created,
            blocks: HashMap::default(),
            clusters: HashMap::default(),
            indices: HashMap::default(),
        }
    }

    fn build(self, commit: Commit) -> TxDetails {
        TxDetails {
            tx_id: self.tx_id,
            wal_id: self.wal_id,
            vbd_id: self.vbd_id,
            branch: self.branch,
            commit,
            preceding_commit: self.preceding_commit,
            created: self.created,
            blocks: self.blocks,
            clusters: self.clusters,
            indices: self.indices,
        }
    }
}

pub struct Tx_ {}

pub type TxId = TypedUuid<Tx_>;

pub(crate) struct TxBegin {
    pub transaction_id: TxId,
    pub branch: BranchName,
    pub preceding_commit: Commit,
    pub created: DateTime<Utc>,
}

pub(crate) struct TxCommit {
    pub transaction_id: TxId,
    pub commit: Commit,
}

#[derive(Clone, Debug)]
pub(crate) struct FileHeader {
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
    #[error("Branch Name Invalid")]
    BranchInvalid,
    /// Content Id Invalid
    #[error("Commit Invalid")]
    CommitInvalid,
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
pub enum IndexFrameError {
    /// Content Id Invalid
    #[error("Content Id Invalid")]
    ContentIdInvalid,
}
