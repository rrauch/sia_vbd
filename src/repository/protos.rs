pub(crate) mod volume {
    use crate::inventory::chunk::ManifestId;
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

    impl From<&super::super::TagInfo> for TagInfo {
        fn from(value: &super::super::TagInfo) -> Self {
            TagInfo {
                commit: Some((&value.commit).into()),
            }
        }
    }

    impl TryFrom<TagInfo> for super::super::TagInfo {
        type Error = anyhow::Error;

        fn try_from(value: TagInfo) -> Result<Self, Self::Error> {
            Ok(super::super::TagInfo {
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
                crate::inventory::chunk::ChunkEntry::SnapshotId(id) => ChunkContent {
                    r#type: Some(chunk_content::Type::Snapshot(chunk_content::Snapshot {
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
                chunk_content::Type::Snapshot(snapshot) => {
                    crate::inventory::chunk::ChunkEntry::SnapshotId(
                        snapshot
                            .cid
                            .map(|h| h.try_into())
                            .transpose()?
                            .ok_or(anyhow!("snapshot id is missing"))?,
                    )
                }
            })
        }
    }

    impl From<ManifestId> for crate::serde::protos::Uuid {
        fn from(value: ManifestId) -> Self {
            value.deref().into()
        }
    }

    impl From<crate::serde::protos::Uuid> for ManifestId {
        fn from(value: crate::serde::protos::Uuid) -> Self {
            Into::<Uuid>::into(value).into()
        }
    }

    impl From<&crate::inventory::chunk::Manifest> for Manifest {
        fn from(value: &crate::inventory::chunk::Manifest) -> Self {
            Self {
                manifest_id: Some(value.id.into()),
                specs: Some((&value.specs).into()),
                created: Some((&value.created).into()),
            }
        }
    }

    impl TryFrom<Manifest> for crate::inventory::chunk::Manifest {
        type Error = anyhow::Error;

        fn try_from(value: Manifest) -> Result<Self, Self::Error> {
            let id = value
                .manifest_id
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

            Ok(crate::inventory::chunk::Manifest {
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
