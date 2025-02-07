use crate::wal::HeaderError::{CreatedInvalid, FileIdInvalid, VbdSpecsInvalid};
use crate::wal::{HeaderError, TxId, WalId};
use std::ops::Deref;
use uuid::Uuid;

include!(concat!(env!("OUT_DIR"), "/protos/wal.rs"));

impl TryFrom<crate::serde::protos::frame::header::TxBegin> for super::TxBegin {
    type Error = crate::serde::protos::frame::Error;

    fn try_from(value: crate::serde::protos::frame::header::TxBegin) -> Result<Self, Self::Error> {
        use crate::wal::CommitFrameError::*;

        Ok(Self {
            transaction_id: value.transaction_id.ok_or(TransactionIdInvalid)?.into(),
            branch: value.branch.try_into().map_err(|_| BranchInvalid)?,
            preceding_commit: value
                .preceding_commit
                .map(|c| c.try_into())
                .transpose()
                .map_err(|_| CommitInvalid)?
                .ok_or(CommitInvalid)?,
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
        begin.branch = value.branch.to_string();
        begin.preceding_commit = Some((&value.preceding_commit).into());
        begin.created = Some(value.created.into());
        begin
    }
}

impl TryFrom<crate::serde::protos::frame::header::TxCommit> for super::TxCommit {
    type Error = crate::serde::protos::frame::Error;

    fn try_from(value: crate::serde::protos::frame::header::TxCommit) -> Result<Self, Self::Error> {
        use crate::wal::CommitFrameError::*;

        Ok(Self {
            transaction_id: value.transaction_id.ok_or(TransactionIdInvalid)?.into(),
            commit: value
                .commit
                .map(|c| c.try_into())
                .transpose()
                .map_err(|_| CommitInvalid)?
                .ok_or(CommitInvalid)?,
        })
    }
}

impl From<&super::TxCommit> for crate::serde::protos::frame::header::TxCommit {
    fn from(value: &super::TxCommit) -> Self {
        let mut commit = crate::serde::protos::frame::header::TxCommit::default();
        commit.transaction_id = Some(value.transaction_id.into());
        commit.commit = Some((&value.commit).into());
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
        value.deref().into()
    }
}

impl From<crate::serde::protos::Uuid> for WalId {
    fn from(value: crate::serde::protos::Uuid) -> Self {
        Into::<Uuid>::into(value).into()
    }
}

impl From<TxId> for crate::serde::protos::Uuid {
    fn from(value: TxId) -> Self {
        value.deref().into()
    }
}

impl From<crate::serde::protos::Uuid> for TxId {
    fn from(value: crate::serde::protos::Uuid) -> Self {
        Into::<Uuid>::into(value).into()
    }
}
