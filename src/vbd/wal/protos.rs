use crate::vbd::wal::HeaderError::{CreatedInvalid, FileIdInvalid, VbdSpecsInvalid};
use crate::vbd::wal::{BlockFrameError, FrameError, HeaderError, TxId, WalId};
use crate::vbd::{ClusterId, Commit};
use uuid::Uuid;

include!(concat!(env!("OUT_DIR"), "/protos/wal.rs"));

impl TryFrom<frame_header::TxBegin> for super::TxBegin {
    type Error = FrameError;

    fn try_from(value: frame_header::TxBegin) -> Result<Self, Self::Error> {
        use crate::vbd::wal::CommitFrameError::*;

        Ok(Self {
            transaction_id: value.transaction_id.ok_or(TransactionIdInvalid)?.into(),
            preceding_content_id: value
                .preceding_content_id
                .ok_or(ContentIdInvalid)?
                .try_into()
                .map_err(|_| ContentIdInvalid)?,

            created: value
                .created
                .ok_or(TimestampInvalid)?
                .try_into()
                .map_err(|_| TimestampInvalid)?,
        })
    }
}

impl From<&super::TxBegin> for frame_header::TxBegin {
    fn from(value: &super::TxBegin) -> Self {
        let mut begin = frame_header::TxBegin::default();
        begin.transaction_id = Some(value.transaction_id.into());
        begin.preceding_content_id = Some((&value.preceding_content_id.0).into());
        begin.created = Some(value.created.into());
        begin
    }
}

impl TryFrom<frame_header::TxCommit> for super::TxCommit {
    type Error = FrameError;

    fn try_from(value: frame_header::TxCommit) -> Result<Self, Self::Error> {
        use crate::vbd::wal::CommitFrameError::*;

        Ok(Self {
            transaction_id: value.transaction_id.ok_or(TransactionIdInvalid)?.into(),
            content_id: value
                .content_id
                .ok_or(ContentIdInvalid)?
                .try_into()
                .map_err(|_| ContentIdInvalid)?,
            committed: value
                .committed
                .ok_or(TimestampInvalid)?
                .try_into()
                .map_err(|_| TimestampInvalid)?,
        })
    }
}

impl From<&super::TxCommit> for frame_header::TxCommit {
    fn from(value: &super::TxCommit) -> Self {
        let mut commit = frame_header::TxCommit::default();
        commit.transaction_id = Some(value.transaction_id.into());
        commit.content_id = Some((&value.content_id.0).into());
        commit.committed = Some(value.committed.into());
        commit
    }
}

impl TryFrom<FileInfo> for super::FileHeader {
    type Error = HeaderError;

    fn try_from(value: FileInfo) -> Result<Self, Self::Error> {
        let wal_id = value.wal_file_id.map(|id| id.into()).ok_or(FileIdInvalid)?;
        let specs = value
            .specs
            .map(|s| s.try_into().ok())
            .flatten()
            .ok_or(VbdSpecsInvalid)?;
        let created = value
            .created
            .map(|c| c.try_into().map_err(|_| CreatedInvalid))
            .ok_or(CreatedInvalid)??;
        let preceding_wal_id = value.preceding_wal_file.map(|id| id.into());

        Ok(Self {
            wal_id,
            specs,
            created,
            preceding_wal_id,
        })
    }
}

impl From<&super::FileHeader> for FileInfo {
    fn from(value: &super::FileHeader) -> Self {
        FileInfo {
            wal_file_id: Some(value.wal_id.into()),
            specs: Some((&value.specs).into()),
            created: Some(value.created.into()),
            preceding_wal_file: value.preceding_wal_id.map(|i| i.into()),
        }
    }
}

impl From<WalId> for crate::protos::Uuid {
    fn from(value: WalId) -> Self {
        value.0.into()
    }
}

impl From<crate::protos::Uuid> for WalId {
    fn from(value: crate::protos::Uuid) -> Self {
        Into::<Uuid>::into(value).into()
    }
}

impl From<TxId> for crate::protos::Uuid {
    fn from(value: TxId) -> Self {
        value.0.into()
    }
}

impl From<crate::protos::Uuid> for TxId {
    fn from(value: crate::protos::Uuid) -> Self {
        Into::<Uuid>::into(value).into()
    }
}

impl From<&super::Block> for frame_header::Block {
    fn from(value: &super::Block) -> Self {
        Self {
            content_id: Some((&value.content_id).into()),
            length: value.length,
        }
    }
}

impl TryFrom<frame_header::Block> for super::Block {
    type Error = FrameError;

    fn try_from(value: frame_header::Block) -> Result<Self, Self::Error> {
        use BlockFrameError::*;

        Ok(super::Block {
            content_id: value
                .content_id
                .ok_or(ContentIdInvalid)?
                .try_into()
                .map_err(|_| ContentIdInvalid)?,
            length: value.length,
        })
    }
}

impl From<&ClusterId> for frame_header::Cluster {
    fn from(value: &ClusterId) -> Self {
        Self {
            content_id: Some(value.into()),
        }
    }
}

impl TryFrom<frame_header::Cluster> for ClusterId {
    type Error = FrameError;

    fn try_from(value: frame_header::Cluster) -> Result<Self, Self::Error> {
        use super::ClusterFrameError::*;

        Ok(value
            .content_id
            .ok_or(ContentIdInvalid)?
            .try_into()
            .map_err(|_| ContentIdInvalid)?)
    }
}

impl TryFrom<frame_header::State> for Commit {
    type Error = FrameError;

    fn try_from(value: frame_header::State) -> Result<Self, Self::Error> {
        use super::StateFrameError::*;

        Ok(value
            .content_id
            .ok_or(ContentIdInvalid)?
            .try_into()
            .map_err(|_| ContentIdInvalid)?)
    }
}

impl From<&super::super::Cluster> for Cluster {
    fn from(value: &super::super::Cluster) -> Self {
        Self {
            content_id: Some((&value.content_id).into()),
            block_ids: value.blocks.iter().map(|b| b.into()).collect::<Vec<_>>(),
        }
    }
}
