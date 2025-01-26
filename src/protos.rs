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
            vbd_id: Some(value.vbd_id.into()),
            block_size: *value.block_size as u32,
            cluster_size: *value.cluster_size as u32,
            content_hash: 0,
            meta_hash: 0,
        };
        v.set_content_hash(value.content_hash.into());
        v.set_meta_hash(value.meta_hash.into());
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
        Ok(Self {
            vbd_id: value.vbd_id.ok_or(anyhow!("vbd_id missing"))?.into(),
            block_size: (value.block_size as usize).try_into()?,
            cluster_size: (value.cluster_size as usize).try_into()?,
            content_hash: value.content_hash().into(),
            meta_hash: value.meta_hash().into(),
        })
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

/*impl TryFrom<Hash> for crate::hash::Hash {
    type Error = anyhow::Error;

    fn try_from(value: Hash) -> Result<Self, Self::Error> {
        let len = value.value.len();
        match value.algo() {
            HashAlgorithm::Tent => {
                if len == 20 {
                    Ok(crate::hash::Hash::Tent(value.value.try_into().unwrap()))
                } else {
                    Err(anyhow!(
                        "tent hash length incorrect, expected 20 bytes, but found {}",
                        len
                    ))
                }
            }
            HashAlgorithm::Blake3 => {
                if len == 32 {
                    Ok(crate::hash::Hash::Blake3(blake3::Hash::from_bytes(
                        value.value.try_into().unwrap(),
                    )))
                } else {
                    Err(anyhow!(
                        "blake3 hash length incorrect, expected 32 bytes, but found {}",
                        len
                    ))
                }
            }
            HashAlgorithm::Xxh3 => {
                if len == 16 {
                    Ok(crate::hash::Hash::XXH3(value.value.try_into().unwrap()))
                } else {
                    Err(anyhow!(
                        "blake3 hash length incorrect, expected 16 bytes, but found {}",
                        len
                    ))
                }
            }
        }
    }
}*/
