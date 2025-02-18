use crate::hash::HashAlgorithm;
use crate::io::AsyncReadExtBuffered;
use crate::repository::protos::volume::ChunkInfo;
use crate::serde;
use crate::serde::encoded::{Decoded, DecodedStream, EncodingSink, EncodingSinkBuilder};
use crate::serde::framed::{FramingSink, WriteFrame};
use crate::serde::{Compressor, PREAMBLE_LEN};
use crate::vbd::{
    Block, BlockId, BlockSize, Cluster, ClusterId, ClusterSize, FixedSpecs, Index, IndexId,
    Position, TypedUuid,
};
use anyhow::anyhow;
use async_tempfile::TempFile;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use futures::{io, AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWriteExt, SinkExt, StreamExt};
use prost::Message;
use std::collections::{BTreeMap, HashMap};
use std::io::SeekFrom;
use std::path::Path;
use std::sync::Arc;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::instrument;
use uuid::Uuid;

const CHUNK_MAGIC_NUMBER: &'static [u8; 16] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x43, 0x4E, 0x4B, 0x00, 0x00, 0x01,
];

#[derive(Clone)]
pub(crate) struct Chunk {
    inner: Arc<Inner>,
}

struct Inner {
    id: ChunkId,
    content: BTreeMap<u64, ChunkEntry>,
}

impl Chunk {
    pub fn new(id: ChunkId, content: impl Iterator<Item = (u64, ChunkEntry)>) -> Self {
        Self {
            inner: Arc::new(Inner {
                id,
                content: content.into_iter().collect(),
            }),
        }
    }

    pub fn from_chunk(other: Chunk, offset_adjustment: i64) -> Self {
        Chunk {
            inner: Arc::new(Inner {
                id: other.inner.id.clone(),
                content: other
                    .inner
                    .content
                    .iter()
                    .map(|(offset, entry)| {
                        (
                            (*offset as i64).saturating_add(offset_adjustment) as u64,
                            entry.clone(),
                        )
                    })
                    .collect(),
            }),
        }
    }

    pub fn id(&self) -> &ChunkId {
        &self.inner.id
    }

    pub fn content(&self) -> impl Iterator<Item = (u64, &ChunkEntry)> {
        self.inner
            .content
            .iter()
            .map(|(id, content)| (*id, content))
    }

    pub fn len(&self) -> usize {
        self.inner.content.len()
    }
}

#[derive(PartialEq, Eq, Clone)]
pub(crate) enum ChunkEntry {
    BlockId(BlockId),
    ClusterId(ClusterId),
    IndexId(IndexId),
}

pub(crate) enum ChunkContent {
    Block(Block),
    Cluster(Cluster),
    Index(Index),
}

pub(crate) type ChunkId = TypedUuid<Chunk>;

pub(super) struct ChunkWriter {
    max_size: u64,
    sink: EncodingSink<FramingSink<'static, tokio_util::compat::Compat<TempFile>>>,
    temp_file: TempFile,
    current_offset: u64,
    header: serde::protos::frame::Header,
    index: crate::repository::protos::volume::ChunkIndex,
}

impl ChunkWriter {
    pub async fn new(
        max_size: u64,
        temp_path: impl AsRef<Path>,
        specs: FixedSpecs,
    ) -> anyhow::Result<Self> {
        let temp_path = temp_path.as_ref();
        let sink = EncodingSinkBuilder::from_writer(
            Self::create_temp_file(temp_path, max_size)
                .await?
                .compat_write(),
        )
        .with_compressor(Compressor::zstd(1024, 3)?)
        .build();

        let chunk_id: ChunkId = Uuid::now_v7().into();
        let info = ChunkInfo {
            chunk_id: Some(chunk_id.into()),
            specs: Some((&specs).into()),
            created: Some(Utc::now().into()),
        };
        let mut body = serde::protos::frame::Body::default();
        body.set_type(serde::protos::frame::body::Type::ChunkIndexProto3);
        body.compressed = None;

        let header = serde::protos::frame::Header {
            r#type: Some(serde::protos::frame::header::Type::ChunkInfo(info)),
            body: Some(body),
        };

        Ok(Self {
            max_size,
            sink,
            temp_file: Self::create_temp_file(temp_path, max_size).await?,
            current_offset: 0,
            header,
            index: crate::repository::protos::volume::ChunkIndex {
                chunk_id: Some(chunk_id.into()),
                content: HashMap::default(),
            },
        })
    }

    async fn create_temp_file(path: impl AsRef<Path>, len: u64) -> anyhow::Result<TempFile> {
        let temp_file = TempFile::new_with_uuid_in(Uuid::now_v7(), path.as_ref()).await?;
        temp_file.set_len(len).await?;
        Ok(temp_file)
    }

    pub async fn append(&mut self, content: &ChunkContent) -> anyhow::Result<bool> {
        let (entry, encodable) = match content {
            ChunkContent::Block(block) => (
                ChunkEntry::BlockId(block.content_id().clone()),
                block.into(),
            ),
            ChunkContent::Cluster(cluster) => (
                ChunkEntry::ClusterId(cluster.content_id().clone()),
                cluster.into(),
            ),
            ChunkContent::Index(index) => (
                ChunkEntry::IndexId(index.content_id().clone()),
                index.into(),
            ),
        };

        let cc: crate::repository::protos::volume::ChunkContent = (&entry).into();

        if self.index.content.values().find(|e| *e == &cc).is_some() {
            //tracing::warn!("duplicate content found in chunk, skipping");
            return Ok(true);
        }

        let entry_offset = self.current_offset;
        self.sink.send(encodable).await?;
        self.current_offset = self.sink.as_mut().as_mut().stream_position().await?;
        self.index.content.insert(entry_offset, cc);

        let total_size = self.current_offset
            + self.header.encoded_len() as u64
            + self.index.encoded_len() as u64
            + PREAMBLE_LEN as u64
            + CHUNK_MAGIC_NUMBER.len() as u64;

        Ok(if total_size > self.max_size {
            // file is full
            let file = self.sink.as_mut().as_mut();
            file.seek(SeekFrom::Start(entry_offset)).await?;
            file.get_mut().set_len(entry_offset).await?;
            self.current_offset = entry_offset;
            self.index.content.remove(&entry_offset);
            false
        } else {
            true
        })
    }

    pub async fn finalize(
        mut self,
    ) -> anyhow::Result<(Chunk, u64, impl AsyncRead + AsyncSeek + Send + Sync + Unpin)> {
        self.sink
            .as_mut()
            .as_mut()
            .get_mut()
            .set_len(self.current_offset)
            .await?;
        self.sink.close().await?;

        let mut content_file = self
            .sink
            .into_inner()
            .into_inner()
            .into_inner()
            .open_ro()
            .await?
            .compat();
        content_file.seek(SeekFrom::Start(0)).await?;

        let mut final_file = self.temp_file.compat_write();
        final_file.write_all(CHUNK_MAGIC_NUMBER).await?;

        let mut buf = BytesMut::with_capacity(self.header.encoded_len());
        self.header.encode(&mut buf)?;
        let header = buf.freeze();

        let mut buf = BytesMut::with_capacity(self.index.encoded_len());
        self.index.encode(&mut buf)?;
        let body = buf.freeze();
        let frame = WriteFrame::full(header, body);

        let mut sink = FramingSink::new(&mut final_file);
        sink.send(frame).await?;
        sink.close().await?;
        io::copy(&mut content_file, &mut final_file).await?;
        let len = final_file.stream_position().await?;
        final_file.get_mut().set_len(len).await?;
        final_file.seek(SeekFrom::Start(0)).await?;
        let chunk = read_chunk_header(&mut final_file).await?;
        final_file.close().await?;

        Ok((
            chunk,
            len,
            final_file.into_inner().open_ro().await?.compat(),
        ))
    }

    pub fn len(&self) -> usize {
        self.index.content.len()
    }
}

pub(crate) trait ChunkSource: AsyncRead + AsyncSeek + Unpin + Send {}
impl<T: AsyncRead + AsyncSeek + Unpin + Send> ChunkSource for T {}

pub(crate) async fn read_chunk_header(mut io: impl ChunkSource) -> anyhow::Result<Chunk> {
    let (chunk, pos) = read_file_header(&mut io).await?;
    let first_frame_offset = pos.offset + pos.length as u64;
    Ok(Chunk::from_chunk(chunk, first_frame_offset as i64))
}

#[instrument(skip(io))]
async fn read_file_header<IO: ChunkSource>(
    mut io: IO,
) -> Result<(Chunk, Position<u64, u32>), anyhow::Error> {
    tracing::trace!("reading chunk header");
    let mut buf = BytesMut::with_capacity(CHUNK_MAGIC_NUMBER.len());
    io.read_exact_buffered(&mut buf, CHUNK_MAGIC_NUMBER.len())
        .await?;
    if buf.as_ref() != CHUNK_MAGIC_NUMBER {
        return Err(anyhow!("invalid magic number"))?;
    }

    let dummy_specs = FixedSpecs::new(
        Uuid::now_v7().into(),
        ClusterSize::Cs256,
        BlockSize::Bs64k,
        HashAlgorithm::Blake3,
        HashAlgorithm::Blake3,
    );

    let mut stream = DecodedStream::from_reader(io, dummy_specs);
    match stream.next().await.transpose()? {
        Some(Decoded::ChunkInfo(mut frame)) => {
            let body = frame.read_body().await?;
            Ok((body, frame.position().clone()))
        }
        Some(_) | None => Err(anyhow!(
            "invalid file format, expected chunk info as first frame but got something else"
        ))?,
    }
}

pub(crate) type ManifestId = TypedUuid<Manifest>;

#[derive(Clone)]
pub(crate) struct Manifest {
    pub id: ManifestId,
    pub specs: FixedSpecs,
    pub created: DateTime<Utc>,
    pub chunks: Vec<Chunk>,
}

impl Manifest {
    pub fn len(&self) -> usize {
        self.chunks.len()
    }
}
