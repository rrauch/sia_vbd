use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum HashAlgorithm {
    Tent,
    Blake3,
    XXH3,
}

impl Display for HashAlgorithm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl HashAlgorithm {
    /// Performs a one-shot hash of the input data.
    pub(crate) fn hash(&self, input: impl AsRef<[u8]>) -> Hash {
        match self {
            HashAlgorithm::Tent => Hash::Tent(tenthash::hash(input)),
            HashAlgorithm::Blake3 => Hash::Blake3(blake3::hash(input.as_ref())),
            HashAlgorithm::XXH3 => {
                Hash::XXH3(twox_hash::XxHash3_128::oneshot(input.as_ref()).to_be_bytes())
            }
        }
    }

    /// Creates a new hasher instance for incremental hashing.
    pub(crate) fn new(&self) -> Hasher {
        match self {
            HashAlgorithm::Tent => Hasher::Tent(tenthash::TentHash::new()),
            HashAlgorithm::Blake3 => Hasher::Blake3(blake3::Hasher::new()),
            HashAlgorithm::XXH3 => Hasher::XXH3(twox_hash::XxHash3_128::new()),
        }
    }

    pub(crate) fn as_str(&self) -> &str {
        match self {
            HashAlgorithm::Tent => "TentHash",
            HashAlgorithm::Blake3 => "BLAKE3",
            HashAlgorithm::XXH3 => "XXH128",
        }
    }
}

impl FromStr for HashAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "blake3" => Ok(HashAlgorithm::Blake3),
            "xxh128" => Ok(HashAlgorithm::XXH3),
            "tent" => Ok(HashAlgorithm::Tent),
            _ => Err(format!("'{}' is not a known hash algorithm", s)),
        }
    }
}

#[derive(Clone)]
pub(crate) enum Hasher {
    Tent(tenthash::TentHash),
    Blake3(blake3::Hasher),
    XXH3(twox_hash::XxHash3_128),
}

impl Hasher {
    /// Updates the hasher with additional data.
    pub fn update(&mut self, data: impl AsRef<[u8]>) {
        match self {
            Hasher::Tent(hasher) => hasher.update(data),
            Hasher::Blake3(hasher) => {
                hasher.update(data.as_ref());
            }
            Hasher::XXH3(hasher) => hasher.write(data.as_ref()),
        }
    }

    /// Finalizes the hashing process and returns the hash.
    pub fn finalize(self) -> Hash {
        match self {
            Hasher::Tent(hasher) => Hash::Tent(hasher.finalize()),
            Hasher::Blake3(hasher) => Hash::Blake3(hasher.finalize()),
            Hasher::XXH3(hasher) => Hash::XXH3(hasher.finish_128().to_be_bytes()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Hash {
    #[serde(rename = "t")]
    Tent([u8; 20]),
    #[serde(
        rename = "b",
        serialize_with = "serialize_blake3",
        deserialize_with = "deserialize_blake3"
    )]
    Blake3(blake3::Hash),
    #[serde(rename = "x")]
    XXH3([u8; 16]),
}

fn serialize_blake3<S: serde::Serializer>(value: &blake3::Hash, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_bytes(value.as_bytes())
}

fn deserialize_blake3<'de, D: serde::Deserializer<'de>>(d: D) -> Result<blake3::Hash, D::Error> {
    struct ByteVisitor;

    impl<'de> serde::de::Visitor<'de> for ByteVisitor {
        type Value = blake3::Hash;

        fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
            formatter.write_str("bytes")
        }

        fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if v.len() != 32 {
                return Err(E::custom("invalid byte length"));
            }
            Ok(blake3::Hash::from_bytes(v.try_into().unwrap()))
        }
    }

    d.deserialize_bytes(ByteVisitor)
}

impl Hash {
    pub fn algorithm(&self) -> HashAlgorithm {
        match &self {
            Hash::Tent(_) => HashAlgorithm::Tent,
            Hash::Blake3(_) => HashAlgorithm::Blake3,
            Hash::XXH3(_) => HashAlgorithm::XXH3,
        }
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bytes = match self {
            Hash::Tent(bytes) => bytes.as_slice(),
            Hash::Blake3(hash) => hash.as_bytes().as_slice(),
            Hash::XXH3(bytes) => bytes.as_slice(),
        };

        write!(f, "{}", hex::encode(bytes))
    }
}

impl AsRef<[u8]> for Hash {
    fn as_ref(&self) -> &[u8] {
        match self {
            Hash::Tent(bytes) => bytes.as_slice(),
            Hash::Blake3(hash) => hash.as_bytes().as_slice(),
            Hash::XXH3(bytes) => bytes.as_slice(),
        }
    }
}

impl TryFrom<(&str, HashAlgorithm)> for Hash {
    type Error = anyhow::Error;

    fn try_from((value, algo): (&str, HashAlgorithm)) -> Result<Self, Self::Error> {
        let bytes = hex::decode(value)?;
        (bytes.as_slice(), algo).try_into()
    }
}

impl TryFrom<(&[u8], HashAlgorithm)> for Hash {
    type Error = anyhow::Error;

    fn try_from((value, algo): (&[u8], HashAlgorithm)) -> Result<Self, Self::Error> {
        let len = value.len();
        match algo {
            HashAlgorithm::Tent => {
                if len == 20 {
                    Ok(Hash::Tent(value.try_into()?))
                } else {
                    Err(anyhow!(
                        "tent hash length incorrect, expected 20 bytes, but found {}",
                        len
                    ))
                }
            }
            HashAlgorithm::Blake3 => {
                if len == 32 {
                    Ok(Hash::Blake3(blake3::Hash::from_bytes(value.try_into()?)))
                } else {
                    Err(anyhow!(
                        "blake3 hash length incorrect, expected 32 bytes, but found {}",
                        len
                    ))
                }
            }
            HashAlgorithm::XXH3 => {
                if len == 16 {
                    Ok(Hash::XXH3(value.try_into()?))
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
