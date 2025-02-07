pub mod fs;
pub(crate) mod protos;

use crate::hash::HashAlgorithm;
use crate::io::{AsyncReadExtBuffered, WrappedReader};
use crate::repository::fs::{FsRepository, FsVolume};
use crate::serde::encoded::{Decoded, Decoder, EncodingSinkBuilder};
use crate::vbd::{
    Block, BlockSize, BranchName, Cluster, ClusterSize, Commit, CommitMut, FixedSpecs, Index,
    TypedUuid, VbdId,
};
use crate::{now, Etag};
use anyhow::{anyhow, bail};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::io::Cursor;
use futures::lock::OwnedMutexGuard;
use futures::{
    AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, SinkExt, TryStreamExt,
};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{Debug, Display};
use std::future::Future;
use std::io::{ErrorKind, SeekFrom};
use tracing::instrument;
use uuid::Uuid;

const VOL_MAGIC_NUMBER: &'static [u8; 16] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x56, 0x4F, 0x4C, 0x00, 0x00, 0x01,
];

const BRA_MAGIC_NUMBER: &'static [u8; 16] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x42, 0x52, 0x41, 0x00, 0x00, 0x01,
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
    fn chunk_details(
        &self,
        chunk_id: &ChunkId,
    ) -> impl Future<Output = Result<Chunk, Self::Error>> + Send;
    fn read_chunk(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> impl Future<Output = Result<impl Reader + 'static, Self::Error>> + Send;
    fn write_chunk(
        &self,
        chunk: &Chunk,
    ) -> impl Future<Output = Result<impl Writer<Ok = Etag> + 'static, Self::Error>> + Send;
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

pub(crate) trait Reader: AsyncRead + AsyncSeek + Send + Sync + Unpin {}
impl<T> Reader for T where T: AsyncRead + AsyncSeek + Send + Sync + Unpin {}

pub(crate) trait Writer: AsyncWrite + Send + Unpin {
    type Ok: Send;
    //type Error: std::error::Error + Send + Sync;
    type Error: Send + Sync + Display + Debug;

    fn finalize(self) -> impl Future<Output = Result<Self::Ok, Self::Error>> + Send;
    fn cancel(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

pub(crate) struct Chunk {
    id: ChunkId,
    content: BTreeMap<u64, ChunkContent>,
}

impl Chunk {
    pub fn id(&self) -> &ChunkId {
        &self.id
    }

    pub fn content(&self) -> impl Iterator<Item = (u64, &ChunkContent)> {
        self.content.iter().map(|(id, content)| (*id, content))
    }
}

pub(crate) enum ChunkContent {
    Block(Block),
    Cluster(Cluster),
    Index(Index),
}

pub(crate) type ChunkId = TypedUuid<Chunk>;

pub enum RepositoryHandler {
    FsRepo(FsRepository),
}

impl RepositoryHandler {
    pub async fn list_volumes(
        &self,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<VbdId>> + 'static + use<'_>> {
        Ok(match self {
            Self::FsRepo(fs) => Box::new(fs.list().await?),
        })
    }

    /*pub async fn list_branches(
        &self,
        vbd_id: &VbdId,
    ) -> anyhow::Result<impl Iterator<Item = String> + 'static> {
        Ok(match self {
            Self::FsRepo(fs) => {
                let fs_volume = fs.open(vbd_id).await?;
                let stream = fs_volume.branches().await?;
                let res: Result<Vec<_>, <FsVolume as Volume>::Error> = stream.try_collect().await;
                res?.into_iter()
            }
        })
    }*/

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
        }
    }

    pub async fn delete_volume(&self, vbd_id: &VbdId) -> anyhow::Result<()> {
        Ok(match self {
            Self::FsRepo(fs) => fs.delete(vbd_id).await?,
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
        }

        Ok(vbd_id)
    }
}

impl From<FsRepository> for RepositoryHandler {
    fn from(value: FsRepository) -> Self {
        RepositoryHandler::FsRepo(value)
    }
}

pub struct VolumeHandler {
    wrapper: WrappedVolume,
    volume_info: VolumeInfo,
    branch_name: BranchName,
    branch_info: BranchInfo,
    chunk_reader: Option<(ChunkId, Box<dyn Reader + 'static>)>,
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
            chunk_reader: None,
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

    pub fn volume_info(&self) -> &VolumeInfo {
        &self.volume_info
    }

    pub fn branch_info(&self) -> (&BranchName, &BranchInfo) {
        (&self.branch_name, &self.branch_info)
    }

    pub async fn chunk_details(&self, chunk_id: &ChunkId) -> anyhow::Result<Chunk> {
        self.wrapper.chunk_details(chunk_id).await
    }

    pub async fn delete_chunk(&self, chunk_id: &ChunkId) -> anyhow::Result<()> {
        self.wrapper.delete_chunk(chunk_id).await
    }

    pub async fn block(&mut self, chunk_id: &ChunkId, offset: u64) -> anyhow::Result<Block> {
        if let ChunkContent::Block(block) = self.read_chunk_content(chunk_id, offset).await? {
            return Ok(block);
        }
        bail!("unexpected content found in chunk, expected block");
    }

    pub async fn cluster(&mut self, chunk_id: &ChunkId, offset: u64) -> anyhow::Result<Cluster> {
        if let ChunkContent::Cluster(cluster) = self.read_chunk_content(chunk_id, offset).await? {
            return Ok(cluster);
        }
        bail!("unexpected content found in chunk, expected cluster");
    }

    pub async fn index(&mut self, chunk_id: &ChunkId, offset: u64) -> anyhow::Result<Index> {
        if let ChunkContent::Index(index) = self.read_chunk_content(chunk_id, offset).await? {
            return Ok(index);
        }
        bail!("unexpected content found in chunk, expected index");
    }

    async fn read_chunk_content(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> anyhow::Result<ChunkContent> {
        let specs = self.volume_info.specs.clone();
        let mut reader = self.chunk_reader(chunk_id, offset).await?;
        let content = {
            let decoded = read_chunk(specs, &mut reader).await?;
            try_convert_chunk(decoded).await?
        };
        self.chunk_reader = Some((chunk_id.clone(), reader));
        Ok(content)
    }

    async fn chunk_reader(
        &mut self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> anyhow::Result<Box<dyn Reader + 'static>> {
        let mut reader = None;
        if let Some((c, mut r)) = self.chunk_reader.take() {
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
}

impl WrappedVolume {
    async fn read_volume(&self) -> anyhow::Result<Bytes> {
        Ok(match &self {
            Self::FsVolume(fs) => fs.read_info().await?,
        })
    }

    async fn list_branches(&self) -> anyhow::Result<impl Iterator<Item = BranchName> + 'static> {
        Ok(match &self {
            Self::FsVolume(fs) => {
                let stream = fs.branches().await?;
                let res: Result<Vec<_>, <FsVolume as Volume>::Error> = stream.try_collect().await;
                res?.into_iter()
            }
        })
    }

    async fn read_branch(&self, branch: &BranchName) -> anyhow::Result<Bytes> {
        Ok(match &self {
            Self::FsVolume(fs) => fs.read_commit(branch).await?,
        })
    }

    async fn chunk_details(&self, chunk_id: &ChunkId) -> anyhow::Result<Chunk> {
        Ok(match &self {
            Self::FsVolume(fs) => fs.chunk_details(chunk_id).await?,
        })
    }

    async fn delete_chunk(&self, chunk_id: &ChunkId) -> anyhow::Result<()> {
        Ok(match &self {
            WrappedVolume::FsVolume(fs) => fs.delete_chunk(chunk_id).await?,
        })
    }

    async fn chunk_reader(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> anyhow::Result<Box<dyn Reader + 'static>> {
        Ok(match &self {
            Self::FsVolume(fs) => Box::new(fs.read_chunk(chunk_id, offset).await?),
        })
    }
}

impl From<FsVolume> for WrappedVolume {
    fn from(value: FsVolume) -> Self {
        WrappedVolume::FsVolume(value)
    }
}

#[derive(Clone)]
pub(crate) struct VolumeInfo {
    pub specs: FixedSpecs,
    pub created: DateTime<Utc>,
    pub name: Option<String>,
}

#[derive(Clone)]
pub(crate) struct BranchInfo {
    pub commit: Commit,
}
