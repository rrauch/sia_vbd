use crate::vbd::{ClusterId, CommitId};
use anyhow::anyhow;
use chrono::{DateTime, Utc};

include!(concat!(env!("OUT_DIR"), "/protos/common.rs"));

impl From<&uuid::Uuid> for Uuid {
    fn from(value: &uuid::Uuid) -> Self {
        let (most_significant, least_significant) = value.as_u64_pair();
        Self {
            most_significant,
            least_significant,
        }
    }
}

impl From<uuid::Uuid> for Uuid {
    fn from(value: uuid::Uuid) -> Self {
        From::from(&value)
    }
}

impl From<&Uuid> for uuid::Uuid {
    fn from(value: &Uuid) -> Self {
        Self::from_u64_pair(value.most_significant, value.least_significant)
    }
}

impl From<Uuid> for uuid::Uuid {
    fn from(value: Uuid) -> Self {
        From::from(&value)
    }
}

impl From<&crate::vbd::FixedSpecs> for FixedSpecs {
    fn from(value: &crate::vbd::FixedSpecs) -> Self {
        let mut v = Self {
            vbd_id: Some(value.vbd_id().into()),
            block_size: *value.block_size() as u32,
            cluster_size: *value.cluster_size() as u32,
            content_hash: 0,
            meta_hash: 0,
        };
        v.set_content_hash(value.content_hash().into());
        v.set_meta_hash(value.meta_hash().into());
        v
    }
}

impl TryFrom<FixedSpecs> for crate::vbd::FixedSpecs {
    type Error = anyhow::Error;

    fn try_from(value: FixedSpecs) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&FixedSpecs> for crate::vbd::FixedSpecs {
    type Error = anyhow::Error;

    fn try_from(value: &FixedSpecs) -> Result<Self, Self::Error> {
        Ok(Self::new(
            value.vbd_id.ok_or(anyhow!("vbd_id missing"))?.into(),
            (value.cluster_size as usize).try_into()?,
            (value.block_size as usize).try_into()?,
            value.content_hash().into(),
            value.meta_hash().into(),
        ))
    }
}

impl From<&DateTime<Utc>> for Timestamp {
    fn from(value: &DateTime<Utc>) -> Self {
        Self {
            seconds: value.timestamp(),
            nanos: value.timestamp_subsec_nanos(),
        }
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(value: DateTime<Utc>) -> Self {
        From::from(&value)
    }
}

impl TryFrom<&Timestamp> for DateTime<Utc> {
    type Error = anyhow::Error;

    fn try_from(value: &Timestamp) -> Result<Self, Self::Error> {
        Self::from_timestamp(value.seconds, value.nanos).ok_or(anyhow!(
            "Timestamp could not be turned into a DateTime<Utc>"
        ))
    }
}

impl TryFrom<Timestamp> for DateTime<Utc> {
    type Error = anyhow::Error;

    fn try_from(value: Timestamp) -> Result<Self, Self::Error> {
        TryFrom::try_from(&value)
    }
}

impl From<&crate::hash::Hash> for Hash {
    fn from(value: &crate::hash::Hash) -> Self {
        let mut hash = Self::default();
        hash.value = Vec::from(value.as_ref());
        hash.set_algo(value.algorithm().into());
        hash
    }
}

impl From<crate::hash::HashAlgorithm> for HashAlgorithm {
    fn from(value: crate::hash::HashAlgorithm) -> Self {
        match value {
            crate::hash::HashAlgorithm::Tent => HashAlgorithm::Tent,
            crate::hash::HashAlgorithm::Blake3 => HashAlgorithm::Blake3,
            crate::hash::HashAlgorithm::XXH3 => HashAlgorithm::Xxh3,
        }
    }
}

impl From<HashAlgorithm> for crate::hash::HashAlgorithm {
    fn from(value: HashAlgorithm) -> Self {
        match value {
            HashAlgorithm::Tent => crate::hash::HashAlgorithm::Tent,
            HashAlgorithm::Blake3 => crate::hash::HashAlgorithm::Blake3,
            HashAlgorithm::Xxh3 => crate::hash::HashAlgorithm::XXH3,
        }
    }
}

impl From<crate::hash::Hash> for Hash {
    fn from(value: crate::hash::Hash) -> Self {
        From::from(&value)
    }
}

impl TryFrom<Hash> for crate::hash::Hash {
    type Error = anyhow::Error;

    fn try_from(value: Hash) -> Result<Self, Self::Error> {
        (value.value.as_slice(), value.algo().into()).try_into()
    }
}

impl From<&super::Compression> for Compression {
    fn from(value: &super::Compression) -> Self {
        match value {
            super::Compression::Zstd => Compression {
                r#type: Some(compression::Type::Zstd(compression::Zstd {})),
            },
        }
    }
}

impl TryFrom<Compression> for super::Compression {
    type Error = anyhow::Error;

    fn try_from(value: Compression) -> Result<Self, Self::Error> {
        match value.r#type {
            Some(compression::Type::Zstd(_)) => Ok(super::Compression::Zstd),
            None => Err(anyhow!("compression type not set")),
        }
    }
}

impl From<&super::Compressed> for Compressed {
    fn from(value: &crate::serde::Compressed) -> Self {
        Compressed {
            compression: Some((&value.compression).into()),
            uncompressed: value.uncompressed,
        }
    }
}

impl TryFrom<Compressed> for super::Compressed {
    type Error = anyhow::Error;

    fn try_from(value: Compressed) -> Result<Self, Self::Error> {
        let compression = match value.compression {
            Some(compression) => compression.try_into(),
            None => Err(anyhow!("compression is not set")),
        }?;

        Ok(super::Compressed {
            compression,
            uncompressed: value.uncompressed,
        })
    }
}

impl TryFrom<(Cluster, crate::vbd::FixedSpecs)> for crate::vbd::Cluster {
    type Error = anyhow::Error;

    fn try_from((value, specs): (Cluster, crate::vbd::FixedSpecs)) -> Result<Self, Self::Error> {
        let cluster_id: ClusterId = value
            .cid
            .map(|c| c.try_into())
            .transpose()?
            .ok_or(frame::Error::ContentIdInvalid)?;

        let block_ids = value
            .block_ids
            .into_iter()
            .map(|b| b.try_into())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| frame::Error::ContentIdInvalid)?;

        let cluster =
            crate::vbd::ClusterMut::from_block_ids(block_ids.into_iter(), specs).finalize();

        if cluster.content_id() != &cluster_id {
            Err(anyhow!(
                "ClusterIds do not match: {} != {}",
                cluster.content_id(),
                cluster_id
            ))?;
        }

        Ok(cluster)
    }
}

impl TryFrom<(Commit, crate::vbd::FixedSpecs)> for crate::vbd::Commit {
    type Error = anyhow::Error;

    fn try_from((value, specs): (Commit, crate::vbd::FixedSpecs)) -> Result<Self, Self::Error> {
        let commit_id: CommitId = value
            .cid
            .map(|c| c.try_into())
            .transpose()?
            .ok_or(frame::Error::ContentIdInvalid)?;

        let cluster_ids = value
            .cluster_ids
            .into_iter()
            .map(|b| b.try_into())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| frame::Error::ContentIdInvalid)?;

        let commit =
            crate::vbd::CommitMut::from_cluster_ids(cluster_ids.into_iter(), specs).finalize();

        if commit.content_id() != &commit_id {
            Err(anyhow!(
                "CommitIds do not match: {} != {}",
                commit.content_id(),
                commit_id
            ))?;
        }

        Ok(commit)
    }
}

impl From<&crate::vbd::Cluster> for Cluster {
    fn from(value: &crate::vbd::Cluster) -> Self {
        Self {
            cid: Some(value.content_id().into()),
            block_ids: value.block_ids().map(|b| b.into()).collect::<Vec<_>>(),
        }
    }
}

impl From<&crate::vbd::Commit> for Commit {
    fn from(value: &crate::vbd::Commit) -> Self {
        Self {
            cid: Some(value.content_id().into()),
            cluster_ids: value.cluster_ids().map(|c| c.into()).collect(),
        }
    }
}

pub(crate) mod frame {
    use self::Error::*;
    use crate::serde::{BodyType, Compressed};
    use crate::vbd::{BlockId, ClusterId, CommitId};
    use crate::wal::{BlockFrameError, ClusterFrameError, CommitFrameError, StateFrameError};
    use thiserror::Error;

    include!(concat!(env!("OUT_DIR"), "/protos/frame.rs"));

    impl From<&BodyType> for body::Type {
        fn from(value: &BodyType) -> Self {
            match value {
                &BodyType::Commit => body::Type::CommitProto3,
                &BodyType::BlockContent => body::Type::BlockContent,
                &BodyType::Cluster => body::Type::ClusterProto3,
            }
        }
    }

    impl TryFrom<body::Type> for BodyType {
        type Error = Error;

        fn try_from(value: body::Type) -> Result<Self, Self::Error> {
            match value {
                body::Type::ClusterProto3 => Ok(BodyType::Cluster),
                body::Type::CommitProto3 => Ok(BodyType::Commit),
                body::Type::BlockContent => Ok(BodyType::BlockContent),
            }
        }
    }

    impl From<&super::super::Body> for Body {
        fn from(value: &crate::serde::Body) -> Self {
            let mut body = Body::default();
            body.set_type((&value.body_type).into());
            body.compressed = value.compressed.as_ref().map(|c| c.into());
            body
        }
    }

    impl TryFrom<Body> for super::super::Body {
        type Error = Error;

        fn try_from(value: Body) -> Result<Self, Self::Error> {
            let body_type: BodyType = value.r#type().try_into()?;
            let compressed = value
                .compressed
                .map(|c| Compressed::try_from(c))
                .transpose()
                .map_err(|_| Error::CompressionInvalid)?;
            Ok(super::super::Body {
                body_type,
                compressed,
            })
        }
    }

    impl From<&ClusterId> for header::Cluster {
        fn from(value: &ClusterId) -> Self {
            Self {
                cid: Some(value.into()),
            }
        }
    }

    impl TryFrom<header::Cluster> for ClusterId {
        type Error = Error;

        fn try_from(value: header::Cluster) -> Result<Self, Self::Error> {
            Ok(value
                .cid
                .ok_or(ContentIdInvalid)?
                .try_into()
                .map_err(|_| ContentIdInvalid)?)
        }
    }

    impl From<&CommitId> for header::Commit {
        fn from(value: &CommitId) -> Self {
            Self {
                cid: Some(value.into()),
            }
        }
    }

    impl TryFrom<header::Commit> for CommitId {
        type Error = Error;

        fn try_from(value: header::Commit) -> Result<Self, Self::Error> {
            Ok(value
                .cid
                .ok_or(ContentIdInvalid)?
                .try_into()
                .map_err(|_| ContentIdInvalid)?)
        }
    }

    impl From<&BlockId> for header::Block {
        fn from(value: &BlockId) -> Self {
            Self {
                cid: Some(value.into()),
            }
        }
    }

    impl TryFrom<header::Block> for BlockId {
        type Error = Error;

        fn try_from(value: header::Block) -> Result<Self, Self::Error> {
            use BlockFrameError::*;

            Ok(value
                .cid
                .ok_or(ContentIdInvalid)?
                .try_into()
                .map_err(|_| ContentIdInvalid)?)
        }
    }

    #[derive(Error, Debug)]
    pub enum Error {
        /// Frame Type Invalid
        #[error("File Type missing or invalid")]
        FrameTypeInvalid,
        #[error("Content Id missing or invalid")]
        ContentIdInvalid,
        #[error("Invalid compression")]
        CompressionInvalid,
        /// Commit Frame error
        #[error(transparent)]
        CommitFrameError(#[from] CommitFrameError),
        /// Block Frame error
        #[error(transparent)]
        BlockFrameError(#[from] BlockFrameError),
        /// Cluster Frame error
        #[error(transparent)]
        ClusterFrameError(#[from] ClusterFrameError),
        /// State Frame error
        #[error(transparent)]
        StateFrameError(#[from] StateFrameError),
    }
}
