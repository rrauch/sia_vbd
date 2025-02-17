pub mod fs;
pub(crate) mod protos;
pub mod renterd;

use crate::hash::HashAlgorithm;
use crate::inventory::chunk::{
    read_chunk_header, Chunk, ChunkContent, ChunkId, ChunkIndex, ChunkIndexId,
};
use crate::io::{AsyncReadExtBuffered, WrappedReader};
use crate::repository::fs::{FsRepository, FsVolume};
use crate::repository::renterd::{RenterdRepository, RenterdVolume};
use crate::serde::encoded::{Decoded, DecodedStream, Decoder, EncodingSinkBuilder};
use crate::serde::Compressor;
use crate::vbd::{
    Block, BlockSize, BranchName, Cluster, ClusterSize, Commit, CommitMut, FixedSpecs, Index, VbdId,
};
use crate::{now, Etag};
use anyhow::{anyhow, bail};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::io::Cursor;
use futures::lock::OwnedMutexGuard;
use futures::{
    AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, SinkExt, StreamExt, TryStreamExt,
};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::io::{ErrorKind, SeekFrom};
use std::sync::Mutex;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::instrument;
use uuid::Uuid;

const VOL_MAGIC_NUMBER: &'static [u8; 16] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x56, 0x4F, 0x4C, 0x00, 0x00, 0x01,
];

const BRA_MAGIC_NUMBER: &'static [u8; 16] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x42, 0x52, 0x41, 0x00, 0x00, 0x01,
];

const IDX_MAGIC_NUMBER: &'static [u8; 16] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x49, 0x44, 0x58, 0x00, 0x00, 0x01,
];

pub trait Repository: Send {
    type Volume: Volume + Send;
    //type Error: std::error::Error + Send + Sync;
    type Error: Send + Sync + Display + Debug;

    fn list(
        &self,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<VbdId, Self::Error>> + 'static, Self::Error>,
    > + Send;
    fn open(
        &self,
        vbd_id: &VbdId,
    ) -> impl Future<Output = Result<Self::Volume, Self::Error>> + Send;
    fn details(
        &self,
        vbd_id: &VbdId,
    ) -> impl Future<
        Output = Result<
            (
                VolumeInfo,
                impl Stream<Item = Result<(BranchName, Bytes), Self::Error>> + 'static,
            ),
            Self::Error,
        >,
    > + Send;
    fn create(
        &self,
        vbd_id: &VbdId,
        branch: &BranchName,
        volume_info: Bytes,
        initial_commit: Bytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
    fn delete(&self, vbd_id: &VbdId) -> impl Future<Output = Result<(), Self::Error>>;
}

pub(crate) trait Volume: Send {
    //type Error: std::error::Error + Send + Sync;
    type Error: Send + Sync + Display + Debug;

    fn read_info(&self) -> impl Future<Output = Result<Bytes, Self::Error>> + Send;

    fn chunks(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<(ChunkId, Etag), Self::Error>> + 'static,
            Self::Error,
        >,
    > + Send;

    fn chunk_indices(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<(ChunkIndexId, Etag), Self::Error>> + 'static,
            Self::Error,
        >,
    > + Send;

    fn read_chunk_index(
        &self,
        id: &ChunkIndexId,
    ) -> impl Future<Output = Result<impl Reader + 'static, Self::Error>> + Send;

    fn write_chunk_index(
        &self,
        id: &ChunkIndexId,
        content: impl AsyncRead + Send + Unpin + 'static,
    ) -> impl Future<Output = Result<Etag, Self::Error>> + Send;

    fn delete_chunk_index(
        &self,
        id: &ChunkIndexId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn read_chunk(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> impl Future<Output = Result<impl Reader + 'static, Self::Error>> + Send;

    fn write_chunk(
        &self,
        chunk_id: &ChunkId,
        len: u64,
        content: impl Reader + 'static,
    ) -> impl Future<Output = Result<Etag, Self::Error>> + Send;

    fn delete_chunk(
        &self,
        chunk_id: &ChunkId,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn branches(
        &self,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<BranchName, Self::Error>> + 'static, Self::Error>,
    > + Send;

    fn write_commit(
        &self,
        branch: &BranchName,
        commit: Bytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn read_commit(
        &self,
        branch: &BranchName,
    ) -> impl Future<Output = Result<Bytes, Self::Error>> + Send;
}

pub trait Stream: futures::Stream + Send + Unpin {}
impl<T> Stream for T where T: futures::Stream + Send + Unpin {}

pub(crate) trait Reader: AsyncRead + AsyncSeek + Send + Unpin {}
impl<T> Reader for T where T: AsyncRead + AsyncSeek + Send + Unpin {}

pub enum RepositoryHandler {
    FsRepo(FsRepository),
    RenterdRepo(RenterdRepository),
}

impl Display for RepositoryHandler {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FsRepo(repo) => repo.fmt(f),
            Self::RenterdRepo(repo) => repo.fmt(f),
        }
    }
}

impl RepositoryHandler {
    pub async fn list_volumes(
        &self,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<VbdId>> + 'static + use<'_>> {
        Ok(match self {
            Self::FsRepo(fs) => {
                Box::new(fs.list().await?) as Box<dyn Stream<Item = anyhow::Result<VbdId>>>
            }
            Self::RenterdRepo(renterd) => {
                Box::new(renterd.list().await?) as Box<dyn Stream<Item = anyhow::Result<VbdId>>>
            }
        })
    }

    pub async fn volume_details(
        &self,
        vbd_id: &VbdId,
    ) -> anyhow::Result<
        (
            VolumeInfo,
            impl Stream<Item = anyhow::Result<(BranchName, BranchInfo)>> + 'static + use<'_>,
        ),
        anyhow::Error,
    > {
        let (info, stream) = match self {
            Self::FsRepo(fs) => {
                let (info, stream) = fs.details(vbd_id).await?;
                (
                    info,
                    Box::new(stream) as Box<dyn Stream<Item = anyhow::Result<(BranchName, Bytes)>> + Unpin>,
                )
            }
            Self::RenterdRepo(renterd) => {
                let (info, stream) = renterd.details(vbd_id).await?;
                (
                    info,
                    Box::new(stream) as Box<dyn Stream<Item = anyhow::Result<(BranchName, Bytes)>> + Unpin>,
                )
            }
        };

        let specs = info.specs.clone();
        let stream = stream.then(move |r| {
            let specs = specs.clone();
            async move {
                match r {
                    Ok((name, data)) => read_branch(data, specs).await.map(|bi| (name, bi)),
                    Err(err) => Err(err),
                }
            }
        });

        Ok((info, Box::pin(stream)))
    }

    pub async fn open_volume(
        &self,
        vbd_id: &VbdId,
        branch: &BranchName,
    ) -> anyhow::Result<VolumeHandler> {
        match self {
            Self::FsRepo(fs) => {
                let fs_volume = fs.open(vbd_id).await?;
                Ok(VolumeHandler::new(fs_volume, branch.clone()).await?)
            }
            Self::RenterdRepo(renterd) => {
                let renterd_volume = renterd.open(vbd_id).await?;
                Ok(VolumeHandler::new(renterd_volume, branch.clone()).await?)
            }
        }
    }

    pub async fn delete_volume(&self, vbd_id: &VbdId) -> anyhow::Result<()> {
        Ok(match self {
            Self::FsRepo(fs) => fs.delete(vbd_id).await?,
            Self::RenterdRepo(renterd) => renterd.delete(vbd_id).await?,
        })
    }

    pub async fn create_volume(
        &self,
        name: impl Into<Option<&str>>,
        default_branch_name: &BranchName,
        size: usize,
        cluster_size: ClusterSize,
        block_size: BlockSize,
        content_hash: HashAlgorithm,
        meta_hash: HashAlgorithm,
    ) -> anyhow::Result<VbdId> {
        let name = name.into().map(|s| s.to_string());
        let cluster_size_bytes = *block_size * *cluster_size;
        let num_clusters = (size + cluster_size_bytes - 1) / cluster_size_bytes;
        let vbd_id: VbdId = Uuid::now_v7().into();
        let fixed_specs = FixedSpecs::new(
            vbd_id.clone(),
            cluster_size,
            block_size,
            content_hash,
            meta_hash,
        );
        let volume_info = VolumeInfo {
            specs: fixed_specs,
            created: now(),
            name,
        };
        let mut buf = BytesMut::zeroed(4096);
        let mut cursor = Cursor::new(buf.as_mut());
        write_volume(&mut cursor, &volume_info).await?;
        let len = cursor.position() as usize;
        drop(cursor);
        buf.truncate(len);
        let volume_bytes = buf.freeze();

        let commit = CommitMut::zeroed(volume_info.specs.clone(), num_clusters).finalize();
        let branch_info = BranchInfo { commit };
        let mut buf = BytesMut::zeroed(4096);
        let mut cursor = Cursor::new(buf.as_mut());
        write_branch(&mut cursor, &branch_info).await?;
        let len = cursor.position() as usize;
        drop(cursor);
        buf.truncate(len);
        let branch_bytes = buf.freeze();

        match self {
            Self::FsRepo(fs) => {
                fs.create(&vbd_id, default_branch_name, volume_bytes, branch_bytes)
                    .await?
            }
            Self::RenterdRepo(renterd) => {
                renterd
                    .create(&vbd_id, default_branch_name, volume_bytes, branch_bytes)
                    .await?
            }
        }

        Ok(vbd_id)
    }
}

impl From<FsRepository> for RepositoryHandler {
    fn from(value: FsRepository) -> Self {
        RepositoryHandler::FsRepo(value)
    }
}

impl From<RenterdRepository> for RepositoryHandler {
    fn from(value: RenterdRepository) -> Self {
        RepositoryHandler::RenterdRepo(value)
    }
}

pub struct VolumeHandler {
    wrapper: WrappedVolume,
    volume_info: VolumeInfo,
    branch_name: BranchName,
    branch_info: BranchInfo,
    chunk_reader: Mutex<Option<(ChunkId, Box<dyn Reader + 'static>)>>,
}

impl VolumeHandler {
    async fn new<V: Into<WrappedVolume>>(
        volume: V,
        branch_name: BranchName,
    ) -> anyhow::Result<Self> {
        let wrapper = volume.into();
        let volume_info = read_volume(wrapper.read_volume().await?).await?;
        let branch_info = read_branch(
            wrapper.read_branch(&branch_name).await?,
            volume_info.specs.clone(),
        )
        .await?;

        Ok(Self {
            wrapper,
            volume_info,
            branch_name,
            branch_info,
            chunk_reader: Mutex::new(None),
        })
    }

    pub async fn list_branches(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = (BranchName, BranchInfo)> + 'static> {
        let mut map = HashMap::new();
        for name in self.wrapper.list_branches().await? {
            let info = read_branch(
                self.wrapper.read_branch(&name).await?,
                self.volume_info.specs.clone(),
            )
            .await?;
            map.insert(name, info);
        }
        Ok(map.into_iter())
    }

    pub async fn list_chunks(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = (ChunkId, Etag)> + 'static> {
        self.wrapper.list_chunks().await
    }

    pub async fn chunk_details(&self, chunk_id: &ChunkId) -> anyhow::Result<Chunk> {
        let mut reader = self.wrapper.chunk_reader(chunk_id, 0).await?;
        Ok(read_chunk_header(&mut reader).await?)
    }

    pub async fn list_chunk_indices(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = (ChunkIndexId, Etag)> + 'static> {
        self.wrapper.list_chunk_indices().await
    }

    pub async fn chunk_index(&self, id: &ChunkIndexId) -> anyhow::Result<ChunkIndex> {
        let mut reader = self.wrapper.chunk_index_reader(id).await?;
        Ok(read_chunk_index(&mut reader, self.volume_info.specs.clone()).await?)
    }

    pub async fn update_chunk_index(&self, chunk_index: ChunkIndex) -> anyhow::Result<Etag> {
        let (reader, writer) = tokio::io::duplex(1024 * 64);
        let id = chunk_index.id.clone();
        tokio::task::spawn(async move {
            let mut writer = writer.compat_write();
            let _ = write_chunk_index(&chunk_index, &mut writer).await;
        });

        Ok(self.wrapper.write_chunk_index(&id, reader.compat()).await?)
    }

    pub async fn delete_chunk_index(&self, id: &ChunkIndexId) -> anyhow::Result<()> {
        Ok(self.wrapper.delete_chunk_index(id).await?)
    }

    pub fn volume_info(&self) -> &VolumeInfo {
        &self.volume_info
    }

    pub fn branch_info(&self) -> (&BranchName, &BranchInfo) {
        (&self.branch_name, &self.branch_info)
    }

    pub async fn update_branch_commit(&self, commit: &Commit) -> anyhow::Result<()> {
        let info = BranchInfo {
            commit: commit.clone(),
        };

        let mut buf = BytesMut::zeroed(4096);
        let mut cursor = Cursor::new(buf.as_mut());
        write_branch(&mut cursor, &info).await?;
        let len = cursor.position() as usize;
        drop(cursor);
        buf.truncate(len);
        let branch_bytes = buf.freeze();

        self.wrapper
            .write_branch(&self.branch_name, branch_bytes)
            .await?;
        Ok(())
    }

    pub async fn delete_chunk(&self, chunk_id: &ChunkId) -> anyhow::Result<()> {
        self.wrapper.delete_chunk(chunk_id).await
    }

    pub async fn block(&self, chunk_id: &ChunkId, offset: u64) -> anyhow::Result<Block> {
        if let ChunkContent::Block(block) = self.read_chunk_content(chunk_id, offset).await? {
            return Ok(block);
        }
        bail!("unexpected content found in chunk, expected block");
    }

    pub async fn cluster(&self, chunk_id: &ChunkId, offset: u64) -> anyhow::Result<Cluster> {
        if let ChunkContent::Cluster(cluster) = self.read_chunk_content(chunk_id, offset).await? {
            return Ok(cluster);
        }
        bail!("unexpected content found in chunk, expected cluster");
    }

    pub async fn index(&self, chunk_id: &ChunkId, offset: u64) -> anyhow::Result<Index> {
        if let ChunkContent::Index(index) = self.read_chunk_content(chunk_id, offset).await? {
            return Ok(index);
        }
        bail!("unexpected content found in chunk, expected index");
    }

    #[instrument(skip_all, fields(chunk_id = %chunk.id(), num_entries = chunk.len(), len))]
    pub async fn put_chunk(
        &self,
        chunk: &Chunk,
        len: u64,
        reader: impl Reader + 'static,
    ) -> anyhow::Result<Etag> {
        tracing::debug!("writing chunk to volume");
        Ok(self.wrapper.write_chunk(chunk, len, reader).await?)
    }

    async fn read_chunk_content(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> anyhow::Result<ChunkContent> {
        let specs = self.volume_info.specs.clone();
        let mut reader = self.chunk_reader(chunk_id, offset).await?;
        let content = {
            let decoded = read_chunk(specs, &mut reader).await?;
            try_convert_chunk(decoded).await?
        };
        {
            let mut lock = self.chunk_reader.lock().unwrap();
            *lock = Some((chunk_id.clone(), reader));
        }
        Ok(content)
    }

    async fn chunk_reader(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> anyhow::Result<Box<dyn Reader + 'static>> {
        let mut reader = None;
        if let Some((c, mut r)) = {
            let mut lock = self.chunk_reader.lock().unwrap();
            lock.take()
        } {
            if &c == chunk_id {
                if let Ok(_) = r.seek(SeekFrom::Start(offset)).await {
                    // still alive
                    reader = Some(r);
                }
            }
        }
        if let Some(reader) = reader.take() {
            return Ok(reader);
        }
        self.wrapper.chunk_reader(chunk_id, offset).await
    }
}

async fn write_chunk_index<IO: AsyncWrite + Send + Unpin>(
    chunk_index: &ChunkIndex,
    mut out: IO,
) -> anyhow::Result<()> {
    out.write_all(IDX_MAGIC_NUMBER).await?;
    let mut sink = EncodingSinkBuilder::from_writer(&mut out)
        .with_compressor(Compressor::zstd(1024, 3)?)
        .build();

    sink.send(chunk_index.into()).await?;

    for chunk in chunk_index.chunks.iter() {
        sink.send(chunk.into()).await?;
    }

    sink.close().await?;
    out.flush().await?;

    Ok(())
}

#[instrument(skip_all)]
async fn read_chunk<IO: Reader>(
    fixed_specs: FixedSpecs,
    reader: &mut IO,
) -> Result<Decoded<WrappedReader<OwnedMutexGuard<&mut IO>>>, anyhow::Error> {
    tracing::trace!("reading CHUNK from VOLUME");
    let decoder = Decoder::new(fixed_specs);
    Ok(decoder.read(reader).await?.ok_or(std::io::Error::new(
        ErrorKind::UnexpectedEof,
        "Unexpected Eof",
    ))?)
}

async fn try_convert_chunk<IO: Reader>(
    decoded: Decoded<WrappedReader<OwnedMutexGuard<&mut IO>>>,
) -> anyhow::Result<ChunkContent> {
    match decoded {
        Decoded::Block(mut frame) => {
            let block = frame.read_body().await?;
            Ok(ChunkContent::Block(block))
        }
        Decoded::Cluster(mut frame) => {
            let cluster = frame.read_body().await?;
            Ok(ChunkContent::Cluster(cluster))
        }
        Decoded::Index(mut frame) => {
            let commit = frame.read_body().await?;
            Ok(ChunkContent::Index(commit))
        }
        _ => {
            bail!("invalid content in chunk");
        }
    }
}

async fn read_volume(data: Bytes) -> anyhow::Result<VolumeInfo> {
    let mut io = Cursor::new(data);
    let mut buf = BytesMut::with_capacity(VOL_MAGIC_NUMBER.len());
    io.read_exact_buffered(&mut buf, VOL_MAGIC_NUMBER.len())
        .await?;
    if buf.as_ref() != VOL_MAGIC_NUMBER {
        bail!("invalid magic number");
    }

    let dummy = FixedSpecs::new(
        Uuid::now_v7().into(),
        ClusterSize::Cs256,
        BlockSize::Bs64k,
        HashAlgorithm::Blake3,
        HashAlgorithm::Blake3,
    );
    let decoder = Decoder::new(dummy);
    match decoder.read(io).await?.ok_or(anyhow!("unexpected eof"))? {
        Decoded::VolumeInfo(v) => Ok(v.into_header()),
        _ => Err(anyhow!("incorrect entry, expected volume_info")),
    }
}

async fn write_volume<IO: AsyncWrite + Send + Unpin>(
    mut io: IO,
    volume_info: &VolumeInfo,
) -> anyhow::Result<()> {
    io.write_all(VOL_MAGIC_NUMBER).await?;

    let mut sink = EncodingSinkBuilder::from_writer(&mut io).build();
    sink.send(volume_info.into()).await?;
    sink.close().await?;

    io.flush().await?;

    Ok(())
}

async fn read_chunk_index(
    reader: &mut impl Reader,
    fixed_specs: FixedSpecs,
) -> anyhow::Result<ChunkIndex> {
    let mut buf = BytesMut::with_capacity(IDX_MAGIC_NUMBER.len());
    reader
        .read_exact_buffered(&mut buf, IDX_MAGIC_NUMBER.len())
        .await?;
    if buf.as_ref() != IDX_MAGIC_NUMBER {
        bail!("invalid magic number");
    }

    let mut stream = DecodedStream::from_reader(reader, fixed_specs.clone());
    let mut chunk_index = match stream.try_next().await? {
        Some(Decoded::ChunkIndexInfo(chunk_index)) => {
            let chunk_index = chunk_index.into_header();
            if chunk_index.specs != fixed_specs {
                bail!("index appears to be for a different vbd");
            }
            chunk_index
        }
        _ => return Err(anyhow!("incorrect entry, expected chunk_index_info")),
    };

    while let Some(frame) = stream.try_next().await? {
        match frame {
            Decoded::Chunk(mut c) => {
                let chunk = c.read_body().await?;
                chunk_index.chunks.push(chunk);
            }
            _ => return Err(anyhow!("incorrect entry, expected chunk_info")),
        }
    }

    Ok(chunk_index)
}

async fn read_branch(data: Bytes, fixed_specs: FixedSpecs) -> anyhow::Result<BranchInfo> {
    let mut io = Cursor::new(data);
    let mut buf = BytesMut::with_capacity(BRA_MAGIC_NUMBER.len());
    io.read_exact_buffered(&mut buf, BRA_MAGIC_NUMBER.len())
        .await?;
    if buf.as_ref() != BRA_MAGIC_NUMBER {
        bail!("invalid magic number");
    }

    let decoder = Decoder::new(fixed_specs);
    match decoder.read(io).await?.ok_or(anyhow!("unexpected eof"))? {
        Decoded::BranchInfo(b) => Ok(b.into_header()),
        _ => Err(anyhow!("incorrect entry, expected branch_info")),
    }
}

async fn write_branch<IO: AsyncWrite + Send + Unpin>(
    mut io: IO,
    branch_info: &BranchInfo,
) -> anyhow::Result<()> {
    io.write_all(BRA_MAGIC_NUMBER).await?;

    let mut sink = EncodingSinkBuilder::from_writer(&mut io).build();
    sink.send(branch_info.into()).await?;
    sink.close().await?;

    io.flush().await?;

    Ok(())
}

pub enum WrappedVolume {
    FsVolume(FsVolume),
    RenterdVolume(RenterdVolume),
}

impl WrappedVolume {
    async fn read_volume(&self) -> anyhow::Result<Bytes> {
        Ok(match &self {
            Self::FsVolume(fs) => fs.read_info().await?,
            Self::RenterdVolume(renterd) => renterd.read_info().await?,
        })
    }

    async fn list_branches(&self) -> anyhow::Result<impl Iterator<Item = BranchName> + 'static> {
        Ok(match &self {
            Self::FsVolume(fs) => {
                let stream = fs.branches().await?;
                let res: Result<Vec<_>, <FsVolume as Volume>::Error> = stream.try_collect().await;
                res?.into_iter()
            }
            Self::RenterdVolume(renterd) => {
                let stream = renterd.branches().await?;
                let res: Result<Vec<_>, <RenterdVolume as Volume>::Error> =
                    stream.try_collect().await;
                res?.into_iter()
            }
        })
    }

    async fn list_chunks(&self) -> anyhow::Result<impl Iterator<Item = (ChunkId, Etag)> + 'static> {
        Ok(match &self {
            Self::FsVolume(fs) => {
                let stream = fs.chunks().await?;
                let res: Result<Vec<_>, <FsVolume as Volume>::Error> = stream.try_collect().await;
                res?.into_iter()
            }
            Self::RenterdVolume(renterd) => {
                let stream = renterd.chunks().await?;
                let res: Result<Vec<_>, <RenterdVolume as Volume>::Error> =
                    stream.try_collect().await;
                res?.into_iter()
            }
        })
    }

    async fn read_branch(&self, branch: &BranchName) -> anyhow::Result<Bytes> {
        Ok(match &self {
            Self::FsVolume(fs) => fs.read_commit(branch).await?,
            Self::RenterdVolume(renterd) => renterd.read_commit(branch).await?,
        })
    }

    async fn write_branch(&self, branch: &BranchName, content: Bytes) -> anyhow::Result<()> {
        Ok(match &self {
            Self::FsVolume(fs) => fs.write_commit(branch, content).await?,
            Self::RenterdVolume(renterd) => renterd.write_commit(branch, content).await?,
        })
    }

    async fn list_chunk_indices(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = (ChunkIndexId, Etag)> + 'static> {
        Ok(match &self {
            Self::FsVolume(fs) => {
                let stream = fs.chunk_indices().await?;
                let res: Result<Vec<_>, <FsVolume as Volume>::Error> = stream.try_collect().await;
                res?.into_iter()
            }
            Self::RenterdVolume(renterd) => {
                let stream = renterd.chunk_indices().await?;
                let res: Result<Vec<_>, <RenterdVolume as Volume>::Error> =
                    stream.try_collect().await;
                res?.into_iter()
            }
        })
    }

    async fn chunk_index_reader(
        &self,
        id: &ChunkIndexId,
    ) -> anyhow::Result<Box<dyn Reader + 'static>> {
        Ok(match &self {
            WrappedVolume::FsVolume(fs) => Box::new(fs.read_chunk_index(id).await?),
            WrappedVolume::RenterdVolume(renterd) => Box::new(renterd.read_chunk_index(id).await?),
        })
    }

    async fn write_chunk_index(
        &self,
        id: &ChunkIndexId,
        content: impl AsyncRead + Send + Unpin + 'static,
    ) -> anyhow::Result<Etag> {
        Ok(match &self {
            Self::FsVolume(fs) => fs.write_chunk_index(id, content).await?,
            Self::RenterdVolume(renterd) => renterd.write_chunk_index(id, content).await?,
        })
    }

    async fn delete_chunk_index(&self, id: &ChunkIndexId) -> anyhow::Result<()> {
        Ok(match &self {
            WrappedVolume::FsVolume(fs) => fs.delete_chunk_index(id).await?,
            WrappedVolume::RenterdVolume(renterd) => renterd.delete_chunk_index(id).await?,
        })
    }

    async fn delete_chunk(&self, chunk_id: &ChunkId) -> anyhow::Result<()> {
        Ok(match &self {
            WrappedVolume::FsVolume(fs) => fs.delete_chunk(chunk_id).await?,
            WrappedVolume::RenterdVolume(renterd) => renterd.delete_chunk(chunk_id).await?,
        })
    }

    async fn chunk_reader(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> anyhow::Result<Box<dyn Reader + 'static>> {
        Ok(match &self {
            Self::FsVolume(fs) => Box::new(fs.read_chunk(chunk_id, offset).await?),
            Self::RenterdVolume(renterd) => Box::new(renterd.read_chunk(chunk_id, offset).await?),
        })
    }

    async fn write_chunk(
        &self,
        chunk: &Chunk,
        len: u64,
        content: impl Reader + 'static,
    ) -> anyhow::Result<Etag> {
        Ok(match &self {
            Self::FsVolume(fs) => fs.write_chunk(chunk.id(), len, content).await?,
            Self::RenterdVolume(renterd) => renterd.write_chunk(chunk.id(), len, content).await?,
        })
    }
}

impl From<FsVolume> for WrappedVolume {
    fn from(value: FsVolume) -> Self {
        WrappedVolume::FsVolume(value)
    }
}

impl From<RenterdVolume> for WrappedVolume {
    fn from(value: RenterdVolume) -> Self {
        WrappedVolume::RenterdVolume(value)
    }
}

#[derive(Clone)]
pub struct VolumeInfo {
    pub specs: FixedSpecs,
    pub created: DateTime<Utc>,
    pub name: Option<String>,
}

#[derive(Clone)]
pub struct BranchInfo {
    pub commit: Commit,
}
