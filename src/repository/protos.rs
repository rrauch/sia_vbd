pub(crate) mod volume {
    use anyhow::anyhow;

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
}
