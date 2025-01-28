use crate::vbd::wal::HeaderError::{CreatedInvalid, FileIdInvalid, VbdSpecsInvalid};
use crate::vbd::wal::{HeaderError, TxId, WalId};
use uuid::Uuid;

include!(concat!(env!("OUT_DIR"), "/protos/wal.rs"));

impl TryFrom<crate::serde::protos::frame::header::TxBegin> for super::TxBegin {
    type Error = crate::serde::protos::frame::Error;

    fn try_from(value: crate::serde::protos::frame::header::TxBegin) -> Result<Self, Self::Error> {
        use crate::vbd::wal::CommitFrameError::*;

        Ok(Self {
            transaction_id: value.transaction_id.ok_or(TransactionIdInvalid)?.into(),
            preceding_content_id: value
                .preceding_cid
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

impl From<&super::TxBegin> for crate::serde::protos::frame::header::TxBegin {
    fn from(value: &super::TxBegin) -> Self {
        let mut begin = crate::serde::protos::frame::header::TxBegin::default();
        begin.transaction_id = Some(value.transaction_id.into());
        begin.preceding_cid = Some((&value.preceding_content_id.0).into());
        begin.created = Some(value.created.into());
        begin
    }
}

impl TryFrom<crate::serde::protos::frame::header::TxCommit> for super::TxCommit {
    type Error = crate::serde::protos::frame::Error;

    fn try_from(value: crate::serde::protos::frame::header::TxCommit) -> Result<Self, Self::Error> {
        use crate::vbd::wal::CommitFrameError::*;

        Ok(Self {
            transaction_id: value.transaction_id.ok_or(TransactionIdInvalid)?.into(),
            content_id: value
                .cid
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

impl From<&super::TxCommit> for crate::serde::protos::frame::header::TxCommit {
    fn from(value: &super::TxCommit) -> Self {
        let mut commit = crate::serde::protos::frame::header::TxCommit::default();
        commit.transaction_id = Some(value.transaction_id.into());
        commit.cid = Some((&value.content_id.0).into());
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

impl From<WalId> for crate::serde::protos::Uuid {
    fn from(value: WalId) -> Self {
        value.0.into()
    }
}

impl From<crate::serde::protos::Uuid> for WalId {
    fn from(value: crate::serde::protos::Uuid) -> Self {
        Into::<Uuid>::into(value).into()
    }
}

impl From<TxId> for crate::serde::protos::Uuid {
    fn from(value: TxId) -> Self {
        value.0.into()
    }
}

impl From<crate::serde::protos::Uuid> for TxId {
    fn from(value: crate::serde::protos::Uuid) -> Self {
        Into::<Uuid>::into(value).into()
    }
}
