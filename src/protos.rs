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
        let algo = match value {
            crate::hash::Hash::Tent(_) => hash::Algorithm::Tent,
            crate::hash::Hash::Blake3(_) => hash::Algorithm::Blake3,
            crate::hash::Hash::XXH3(_) => hash::Algorithm::Xxh3,
        };
        hash.set_algo(algo);
        hash
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
        let len = value.value.len();
        match value.algo() {
            hash::Algorithm::Tent => {
                if len == 20 {
                    Ok(crate::hash::Hash::Tent(value.value.try_into().unwrap()))
                } else {
                    Err(anyhow!(
                        "tent hash length incorrect, expected 20 bytes, but found {}",
                        len
                    ))
                }
            }
            hash::Algorithm::Blake3 => {
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
            hash::Algorithm::Xxh3 => {
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
}
