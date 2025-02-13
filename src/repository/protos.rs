pub(crate) mod volume {
    use crate::inventory::chunk::ChunkIndexId;
    use anyhow::anyhow;
    use std::collections::BTreeMap;
    use std::ops::Deref;
    use uuid::Uuid;

    include!(concat!(env!("OUT_DIR"), "/protos/volume.rs"));

    impl From<&super::super::VolumeInfo> for VolumeInfo {
        fn from(value: &crate::repository::VolumeInfo) -> Self {
            VolumeInfo {
                specs: Some((&value.specs).into()),
                created: Some(value.created.into()),
                name: value.name.clone(),
            }
        }
    }

    impl TryFrom<VolumeInfo> for super::super::VolumeInfo {
        type Error = anyhow::Error;

        fn try_from(value: VolumeInfo) -> Result<Self, Self::Error> {
            Ok(Self {
                specs: value
                    .specs
                    .map(|s| s.try_into())
                    .transpose()?
                    .ok_or(anyhow!("spec is missing"))?,
                created: value
                    .created
                    .map(|c| c.try_into())
                    .transpose()?
                    .ok_or(anyhow!("created is missing"))?,
                name: value.name,
            })
        }
    }

    impl From<&super::super::BranchInfo> for BranchInfo {
        fn from(value: &super::super::BranchInfo) -> Self {
            BranchInfo {
                commit: Some((&value.commit).into()),
            }
        }
    }

    impl TryFrom<BranchInfo> for super::super::BranchInfo {
        type Error = anyhow::Error;

        fn try_from(value: BranchInfo) -> Result<Self, Self::Error> {
            Ok(super::super::BranchInfo {
                commit: value
                    .commit
                    .map(|c| c.try_into())
                    .transpose()?
                    .ok_or(anyhow!("commit is missing"))?,
            })
        }
    }

    impl From<&crate::inventory::chunk::ChunkEntry> for ChunkContent {
        fn from(value: &crate::inventory::chunk::ChunkEntry) -> Self {
            match value {
                crate::inventory::chunk::ChunkEntry::BlockId(id) => ChunkContent {
                    r#type: Some(chunk_content::Type::Block(chunk_content::Block {
                        cid: Some(id.into()),
                    })),
                },
                crate::inventory::chunk::ChunkEntry::ClusterId(id) => ChunkContent {
                    r#type: Some(chunk_content::Type::Cluster(chunk_content::Cluster {
                        cid: Some(id.into()),
                    })),
                },
                crate::inventory::chunk::ChunkEntry::IndexId(id) => ChunkContent {
                    r#type: Some(chunk_content::Type::Index(chunk_content::Index {
                        cid: Some(id.into()),
                    })),
                },
            }
        }
    }

    impl TryFrom<ChunkContent> for crate::inventory::chunk::ChunkEntry {
        type Error = anyhow::Error;

        fn try_from(value: ChunkContent) -> Result<Self, Self::Error> {
            Ok(match value.r#type.ok_or(anyhow!("type is none"))? {
                chunk_content::Type::Block(block) => crate::inventory::chunk::ChunkEntry::BlockId(
                    block
                        .cid
                        .map(|h| h.try_into())
                        .transpose()?
                        .ok_or(anyhow!("block id is missing"))?,
                ),
                chunk_content::Type::Cluster(cluster) => {
                    crate::inventory::chunk::ChunkEntry::ClusterId(
                        cluster
                            .cid
                            .map(|h| h.try_into())
                            .transpose()?
                            .ok_or(anyhow!("cluster id is missing"))?,
                    )
                }
                chunk_content::Type::Index(index) => crate::inventory::chunk::ChunkEntry::IndexId(
                    index
                        .cid
                        .map(|h| h.try_into())
                        .transpose()?
                        .ok_or(anyhow!("index id is missing"))?,
                ),
            })
        }
    }

    impl From<ChunkIndexId> for crate::serde::protos::Uuid {
        fn from(value: ChunkIndexId) -> Self {
            value.deref().into()
        }
    }

    impl From<crate::serde::protos::Uuid> for ChunkIndexId {
        fn from(value: crate::serde::protos::Uuid) -> Self {
            Into::<Uuid>::into(value).into()
        }
    }

    impl From<&crate::inventory::chunk::ChunkIndex> for ChunkIndexInfo {
        fn from(value: &crate::inventory::chunk::ChunkIndex) -> Self {
            Self {
                chunk_index_id: Some(value.id.into()),
                specs: Some((&value.specs).into()),
                created: Some((&value.created).into()),
            }
        }
    }

    impl TryFrom<ChunkIndexInfo> for crate::inventory::chunk::ChunkIndex {
        type Error = anyhow::Error;

        fn try_from(value: ChunkIndexInfo) -> Result<Self, Self::Error> {
            let id = value
                .chunk_index_id
                .map(|id| id.into())
                .ok_or(anyhow!("id is missing"))?;
            let specs = value
                .specs
                .map(|specs| specs.try_into())
                .transpose()?
                .ok_or(anyhow!("specs are missing"))?;
            let created = value
                .created
                .map(|c| c.try_into())
                .transpose()?
                .ok_or(anyhow!("created is missing"))?;

            Ok(crate::inventory::chunk::ChunkIndex {
                id,
                specs,
                created,
                chunks: vec![],
            })
        }
    }

    impl From<&crate::inventory::chunk::Chunk> for ChunkIndex {
        fn from(value: &crate::inventory::chunk::Chunk) -> Self {
            Self {
                chunk_id: Some(value.id().clone().into()),
                content: value
                    .content()
                    .into_iter()
                    .map(|(offset, entry)| (offset, entry.into()))
                    .collect(),
            }
        }
    }

    impl TryFrom<ChunkIndex> for crate::inventory::chunk::Chunk {
        type Error = anyhow::Error;

        fn try_from(index: ChunkIndex) -> Result<Self, Self::Error> {
            let id = index
                .chunk_id
                .map(|id| id.into())
                .ok_or(anyhow!("chunk_id is missing"))?;

            let content = index
                .content
                .into_iter()
                .map(|(offset, content)| {
                    Ok((
                        offset,
                        content
                            .try_into()
                            .map_err(|_| anyhow!("invalid content id"))?,
                    ))
                })
                .collect::<Result<BTreeMap<u64, crate::inventory::chunk::ChunkEntry>, Self::Error>>(
                )?;

            Ok(crate::inventory::chunk::Chunk::new(id, content.into_iter()))
        }
    }

    impl From<crate::serde::protos::Uuid> for super::super::ChunkId {
        fn from(value: crate::serde::protos::Uuid) -> Self {
            Into::<Uuid>::into(value).into()
        }
    }

    impl From<super::super::ChunkId> for crate::serde::protos::Uuid {
        fn from(value: super::super::ChunkId) -> Self {
            Into::<crate::serde::protos::Uuid>::into(value.as_ref()).into()
        }
    }
}
