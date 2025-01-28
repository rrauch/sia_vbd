use crate::AsyncReadExtBuffered;
use anyhow::anyhow;
use async_compression::futures::bufread::ZstdEncoder;
use async_compression::Level;
use bytes::{Bytes, BytesMut};
use futures::io::Cursor;

pub(crate) mod encoded;
pub(crate) mod framed;
pub(crate) mod protos;

pub(crate) const PREAMBLE_LEN: usize = 8;

#[derive(Clone, Debug)]
pub enum Compression {
    Zstd,
}

#[derive(Clone, Debug)]
pub(crate) enum Compressor {
    ZStdCompressor {
        size_threshold: u64,
        compression_level: u16,
        compression: Compression,
    },
}

impl Compressor {
    pub fn size_threshold(&self) -> u64 {
        match &self {
            Self::ZStdCompressor { size_threshold, .. } => *size_threshold,
        }
    }

    pub fn compression(&self) -> &Compression {
        match &self {
            Self::ZStdCompressor { compression, .. } => compression,
        }
    }

    pub fn compress(&self, input: &Bytes) -> Bytes {
        match self {
            Self::ZStdCompressor {
                compression_level, ..
            } => zstd_compress(*compression_level, input),
        }
    }

    pub fn zstd<L: Into<Option<u16>>>(
        size_threshold: u64,
        compression_level: L,
    ) -> Result<Self, anyhow::Error> {
        let compression_level = compression_level
            .into()
            .map(|l| {
                if l >= 1 && l <= 22 {
                    Ok(l)
                } else {
                    Err(anyhow!(
                        "invalid zstd compression level, must be between 1-22"
                    ))
                }
            })
            .transpose()?
            .unwrap_or(3);

        Ok(Self::ZStdCompressor {
            size_threshold,
            compression_level,
            compression: Compression::Zstd,
        })
    }
}

fn zstd_compress(compression_level: u16, input: &Bytes) -> Bytes {
    let mut output = BytesMut::with_capacity(input.len());

    let cursor = Cursor::new(input);
    let mut compressor =
        ZstdEncoder::with_quality(cursor, Level::Precise(compression_level as i32));

    futures::executor::block_on(async {
        compressor
            .read_all_buffered(&mut output)
            .await
            .expect("writing into buffer can never fail")
    });

    output.freeze()
}

#[derive(Clone, Debug)]
pub(crate) struct Compressed {
    uncompressed: u64,
    compression: Compression,
}

impl Compressed {
    pub fn uncompressed_len(&self) -> u64 {
        self.uncompressed
    }

    pub fn compression(&self) -> &Compression {
        &self.compression
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum BodyType {
    BlockContent,
    Cluster,
    Commit,
}

#[derive(Clone, Debug)]
pub(crate) struct Body {
    body_type: BodyType,
    compressed: Option<Compressed>,
}

impl Body {
    pub fn body_type(&self) -> BodyType {
        self.body_type
    }

    pub fn compressed(&self) -> Option<&Compressed> {
        self.compressed.as_ref()
    }
}
