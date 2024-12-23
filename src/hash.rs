use bytes::Bytes;
use std::hash::Hash;

pub enum Hashable<'a> {
    Simple(&'a [u8], bool),
}

impl<'a> Iterator for Hashable<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Hashable::Simple(data, sent) => {
                if !*sent {
                    *sent = true;
                    Some(data)
                } else {
                    None
                }
            }
        }
    }
}

pub trait AsHashable {
    #[must_use]
    fn as_hashable(&self) -> Hashable;
}

impl AsHashable for &[u8] {
    fn as_hashable(&self) -> Hashable {
        Hashable::Simple(self, false)
    }
}

impl AsHashable for &Vec<u8> {
    fn as_hashable(&self) -> Hashable {
        Hashable::Simple(self.as_slice(), false)
    }
}

impl AsHashable for &Bytes {
    fn as_hashable(&self) -> Hashable {
        Hashable::Simple(self.as_ref(), false)
    }
}

pub trait Hasher: Send + Sync {
    type Hash: Clone + PartialEq + Eq + Hash + Send + Sync;

    fn hash(&self, input: impl AsHashable) -> Self::Hash;
}

pub struct TentHasher {}

impl Hasher for TentHasher {
    type Hash = [u8; 20];

    fn hash(&self, input: impl AsHashable) -> Self::Hash {
        let mut hasher = tenthash::TentHasher::new();
        input.as_hashable().for_each(|i| hasher.update(i.as_ref()));
        hasher.finalize()
    }
}

pub struct Blake3Hasher {}

impl Hasher for Blake3Hasher {
    type Hash = blake3::Hash;

    fn hash(&self, input: impl AsHashable) -> Self::Hash {
        let mut hasher = blake3::Hasher::new();
        input.as_hashable().for_each(|d| {
            hasher.update(d.as_ref());
        });
        hasher.finalize()
    }
}

pub struct XXH3Hasher {}

impl Hasher for XXH3Hasher {
    type Hash = u128;

    fn hash(&self, input: impl AsHashable) -> Self::Hash {
        let mut hasher = twox_hash::XxHash3_128::new();
        input.as_hashable().for_each(|d| hasher.write(d.as_ref()));
        hasher.finish_128()
    }
}
