use crate::hash::HashAlgorithm;
use bytes::{Bytes, BytesMut};
use once_cell::sync::Lazy;
use sqlx::{Pool, Sqlite};
use std::fmt::{Debug, Display, Formatter};
use std::fs::Metadata;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::SystemTime;

pub mod hash;
pub mod inventory;
pub mod io;
pub mod nbd;
pub mod repository;
pub mod serde;
pub mod vbd;
pub mod wal;

static ZEROES: Lazy<Bytes> = Lazy::new(|| BytesMut::zeroed(1024 * 256).freeze());

enum ListenEndpoint {
    Tcp(String),
    #[cfg(unix)]
    Unix(PathBuf),
}

#[derive(Debug, Clone)]
pub enum ClientEndpoint {
    Tcp(SocketAddr),
    #[cfg(unix)]
    Unix(unix::UnixAddr),
}

impl Display for ClientEndpoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::Tcp(addr) => Display::fmt(addr, f),
            #[cfg(unix)]
            Self::Unix(addr) => Display::fmt(addr, f),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SqlitePool {
    writer: Pool<Sqlite>,
    reader: Pool<Sqlite>,
}

impl SqlitePool {
    pub fn read(&self) -> &Pool<Sqlite> {
        &self.reader
    }

    pub fn write(&self) -> &Pool<Sqlite> {
        &self.writer
    }
}

fn is_power_of_two(n: u32) -> bool {
    n != 0 && (n & (n - 1)) == 0
}

/// Returns the highest power of two that fits into `n`
fn highest_power_of_two(n: u32) -> u32 {
    if n == 0 {
        0
    } else {
        1 << (31 - n.leading_zeros())
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct Etag {
    data: Bytes,
}

impl Etag {
    pub fn copy_from<T: AsRef<[u8]>>(input: T) -> Self {
        Self {
            data: Bytes::copy_from_slice(input.as_ref()),
        }
    }
}

impl From<Bytes> for Etag {
    fn from(value: Bytes) -> Self {
        Self { data: value }
    }
}

impl From<Vec<u8>> for Etag {
    fn from(value: Vec<u8>) -> Self {
        Self { data: value.into() }
    }
}

impl Display for Etag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let bytes = self.data.as_ref();
        for &byte in bytes {
            write!(f, "{:0>2x}", byte)?;
        }
        Ok(())
    }
}

impl Debug for Etag {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

impl AsRef<[u8]> for Etag {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

impl TryFrom<&Metadata> for Etag {
    type Error = Error;

    fn try_from(metadata: &Metadata) -> Result<Self, Self::Error> {
        let mut hasher = HashAlgorithm::XXH3.new();
        hasher.update(
            metadata
                .modified()?
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|_| Error::new(ErrorKind::InvalidData, "file last_modified before 1970"))?
                .as_nanos()
                .to_be_bytes(),
        );
        hasher.update(metadata.len().to_be_bytes());
        let hash = hasher.finalize();
        Ok(Etag::copy_from(hash.as_ref()))
    }
}

#[cfg(unix)]
mod unix {
    use std::fmt::{Debug, Display, Formatter};
    use std::ops::Deref;
    use std::os::unix::net::SocketAddr;

    #[derive(Clone)]
    pub struct UnixAddr(pub(crate) SocketAddr);

    impl Deref for UnixAddr {
        type Target = SocketAddr;
        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl From<UnixAddr> for SocketAddr {
        fn from(value: UnixAddr) -> Self {
            value.0
        }
    }

    impl Display for UnixAddr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }

    impl Debug for UnixAddr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }
}
