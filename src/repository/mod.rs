pub mod fs;
pub(crate) mod protos;
pub mod renterd;

use crate::hash::HashAlgorithm;
use crate::inventory::chunk::{
    read_chunk_header, Chunk, ChunkContent, ChunkId, Manifest, ManifestId,
};
use crate::io::{AsyncReadExtBuffered, WrappedReader};
use crate::repository::fs::{FsRepository, FsVolume};
use crate::repository::renterd::{RenterdRepository, RenterdVolume};
use crate::serde::encoded::{Decoded, DecodedStream, Decoder, EncodingSinkBuilder};
use crate::serde::Compressor;
use crate::vbd::{
    Block, BlockSize, BranchName, Cluster, ClusterSize, Commit, CommitId, CommitMut, FixedSpecs,
    Snapshot, TagName, VbdId,
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
use std::fmt::{Debug, Display, Formatter};
use std::future::Future;
use std::io::{ErrorKind, SeekFrom};
use std::sync::Mutex;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use tracing::instrument;
use uuid::Uuid;

const VOLUME_MAGIC_NUMBER: &'static [u8; 16] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x56, 0x4F, 0x4C, 0x00, 0x00, 0x01,
];

const BRANCH_MAGIC_NUMBER: &'static [u8; 16] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x42, 0x52, 0x41, 0x00, 0x00, 0x01,
];

const TAG_MAGIC_NUMBER: &'static [u8; 16] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x54, 0x41, 0x47, 0x00, 0x00, 0x01,
];

const MANIFEST_MAGIC_NUMBER: &'static [u8; 16] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x4D, 0x41, 0x4E, 0x00, 0x00, 0x01,
];

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum CommitType {
    Branch(BranchName),
    Tag(TagName),
}

impl Display for CommitType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CommitType::Branch(b) => Display::fmt(&b, f),
            CommitType::Tag(t) => Display::fmt(&t, f),
        }
    }
}

impl From<BranchName> for CommitType {
    fn from(value: BranchName) -> Self {
        CommitType::Branch(value)
    }
}

impl From<TagName> for CommitType {
    fn from(value: TagName) -> Self {
        CommitType::Tag(value)
    }
}

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
                impl Stream<Item = Result<(CommitType, Bytes), Self::Error>> + 'static,
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
    fn write_commit(
        &self,
        vbd_id: &VbdId,
        commit_type: &CommitType,
        data: Bytes,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
    fn delete_commit(
        &self,
        vbd_id: &VbdId,
        commit_type: &CommitType,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
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

    fn manifests(
        &self,
    ) -> impl Future<
        Output = Result<
            impl Stream<Item = Result<(ManifestId, Etag), Self::Error>> + 'static,
            Self::Error,
        >,
    > + Send;

    fn read_manifest(
        &self,
        id: &ManifestId,
    ) -> impl Future<Output = Result<impl Reader + 'static, Self::Error>> + Send;

    fn write_manifest(
        &self,
        id: &ManifestId,
        content: impl AsyncRead + Send + Unpin + 'static,
    ) -> impl Future<Output = Result<Etag, Self::Error>> + Send;

    fn delete_manifest(
        &self,
        id: &ManifestId,
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

    fn commits(
        &self,
    ) -> impl Future<
        Output = Result<impl Stream<Item = Result<CommitType, Self::Error>> + 'static, Self::Error>,
    > + Send;

    fn write_commit(
        &self,
        commit_type: &CommitType,
        commit: Bytes,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn read_commit(
        &self,
        commit_type: &CommitType,
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
            impl Stream<Item = anyhow::Result<CommitInfo>> + 'static + use<'_>,
        ),
        anyhow::Error,
    > {
        let (info, stream) = match self {
            Self::FsRepo(fs) => {
                let (info, stream) = fs.details(vbd_id).await?;
                (
                    info,
                    Box::new(stream)
                        as Box<dyn Stream<Item = anyhow::Result<(CommitType, Bytes)>> + Unpin>,
                )
            }
            Self::RenterdRepo(renterd) => {
                let (info, stream) = renterd.details(vbd_id).await?;
                (
                    info,
                    Box::new(stream)
                        as Box<dyn Stream<Item = anyhow::Result<(CommitType, Bytes)>> + Unpin>,
                )
            }
        };

        let specs = info.specs.clone();
        let stream = stream.then(move |r| {
            let specs = specs.clone();
            async move {
                match r {
                    Ok((commit_type, data)) => read_commit(data, commit_type, specs).await,
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
        let commit_info = CommitInfo::Branch(default_branch_name.clone(), BranchInfo { commit });
        let mut buf = BytesMut::zeroed(4096);
        let mut cursor = Cursor::new(buf.as_mut());
        write_commit(&mut cursor, &commit_info).await?;
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

    pub async fn create_commit(
        &self,
        commit_type: &CommitType,
        volume_id: &VbdId,
        commit_id: &CommitId,
    ) -> anyhow::Result<CommitInfo> {
        let (_, mut commits) = self.volume_details(volume_id).await?;
        let mut commit = None;
        while let Some(commit_info) = commits.try_next().await? {
            match (&commit_info, commit_type) {
                (CommitInfo::Branch(existing, _), CommitType::Branch(branch_name))
                    if branch_name == existing =>
                {
                    bail!("branch {} already exists", branch_name);
                }
                (CommitInfo::Tag(existing, _), CommitType::Tag(tag_name))
                    if tag_name == existing =>
                {
                    bail!("tag {} already exists", tag_name);
                }
                _ => {}
            }
            if commit_info.commit().content_id() == commit_id {
                commit = Some(commit_info.commit().clone());
                break;
            }
        }
        let commit = commit.ok_or(anyhow!("commit {} not found", commit_id))?;
        let commit_info = match commit_type {
            CommitType::Branch(branch_name) => {
                CommitInfo::Branch(branch_name.clone(), BranchInfo { commit })
            }
            CommitType::Tag(tag_name) => CommitInfo::Tag(tag_name.clone(), TagInfo { commit }),
        };
        let mut buf = BytesMut::zeroed(4096);
        let mut cursor = Cursor::new(buf.as_mut());
        write_commit(&mut cursor, &commit_info).await?;
        let len = cursor.position() as usize;
        drop(cursor);
        buf.truncate(len);
        let branch_bytes = buf.freeze();

        match self {
            Self::FsRepo(fs) => {
                fs.write_commit(&volume_id, &commit_type, branch_bytes)
                    .await?
            }
            Self::RenterdRepo(renterd) => {
                renterd
                    .write_commit(&volume_id, &commit_type, branch_bytes)
                    .await?
            }
        }

        Ok(commit_info)
    }

    pub async fn delete_commit(
        &self,
        volume_id: &VbdId,
        commit_type: &CommitType,
    ) -> anyhow::Result<()> {
        match self {
            Self::FsRepo(fs) => fs.delete_commit(&volume_id, &commit_type).await,
            Self::RenterdRepo(renterd) => renterd.delete_commit(&volume_id, &commit_type).await,
        }
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
        let commit_type = CommitType::Branch(branch_name);
        let (branch_name, branch_info) = match read_commit(
            wrapper.read_commit(&commit_type).await?,
            commit_type,
            volume_info.specs.clone(),
        )
        .await?
        {
            CommitInfo::Branch(branch_name, branch_info) => Ok((branch_name, branch_info)),
            _ => Err(anyhow!("incorrect commit type")),
        }?;

        Ok(Self {
            wrapper,
            volume_info,
            branch_name,
            branch_info,
            chunk_reader: Mutex::new(None),
        })
    }

    pub async fn list_commits(&self) -> anyhow::Result<impl Iterator<Item = CommitInfo> + 'static> {
        let mut vec = vec![];
        for commit_type in self.wrapper.list_commits().await? {
            let data = self.wrapper.read_commit(&commit_type).await?;
            let info = read_commit(data, commit_type, self.volume_info.specs.clone()).await?;
            vec.push(info);
        }
        Ok(vec.into_iter())
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

    pub async fn list_manifests(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = (ManifestId, Etag)> + 'static> {
        self.wrapper.list_manifests().await
    }

    pub async fn manifest(&self, id: &ManifestId) -> anyhow::Result<Manifest> {
        let mut reader = self.wrapper.manifest_reader(id).await?;
        Ok(read_manifest(&mut reader, self.volume_info.specs.clone()).await?)
    }

    pub async fn update_manifest(&self, manifest: Manifest) -> anyhow::Result<Etag> {
        let (reader, writer) = tokio::io::duplex(1024 * 64);
        let id = manifest.id.clone();
        tokio::task::spawn(async move {
            let mut writer = writer.compat_write();
            let _ = write_manifest(&manifest, &mut writer).await;
        });

        Ok(self.wrapper.write_manifest(&id, reader.compat()).await?)
    }

    pub async fn delete_manifest(&self, id: &ManifestId) -> anyhow::Result<()> {
        Ok(self.wrapper.delete_manifest(id).await?)
    }

    pub fn volume_info(&self) -> &VolumeInfo {
        &self.volume_info
    }

    pub fn branch_info(&self) -> (&BranchName, &BranchInfo) {
        (&self.branch_name, &self.branch_info)
    }

    pub async fn update_branch_commit(&self, commit: &Commit) -> anyhow::Result<()> {
        let info = CommitInfo::Branch(
            self.branch_name.clone(),
            BranchInfo {
                commit: commit.clone(),
            },
        );

        let mut buf = BytesMut::zeroed(4096);
        let mut cursor = Cursor::new(buf.as_mut());
        write_commit(&mut cursor, &info).await?;
        let len = cursor.position() as usize;
        drop(cursor);
        buf.truncate(len);
        let branch_bytes = buf.freeze();

        self.wrapper
            .write_commit(&CommitType::Branch(self.branch_name.clone()), branch_bytes)
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

    pub async fn snapshot(&self, chunk_id: &ChunkId, offset: u64) -> anyhow::Result<Snapshot> {
        if let ChunkContent::Snapshot(snapshot) = self.read_chunk_content(chunk_id, offset).await? {
            return Ok(snapshot);
        }
        bail!("unexpected content found in chunk, expected snapshot");
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

async fn write_manifest<IO: AsyncWrite + Send + Unpin>(
    manifest: &Manifest,
    mut out: IO,
) -> anyhow::Result<()> {
    out.write_all(MANIFEST_MAGIC_NUMBER).await?;
    let mut sink = EncodingSinkBuilder::from_writer(&mut out)
        .with_compressor(Compressor::zstd(1024, 3)?)
        .build();

    sink.send(manifest.into()).await?;

    for chunk in manifest.chunks.iter() {
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
        Decoded::Snapshot(mut frame) => {
            let commit = frame.read_body().await?;
            Ok(ChunkContent::Snapshot(commit))
        }
        _ => {
            bail!("invalid content in chunk");
        }
    }
}

async fn read_volume(data: Bytes) -> anyhow::Result<VolumeInfo> {
    let mut io = Cursor::new(data);
    let mut buf = BytesMut::with_capacity(VOLUME_MAGIC_NUMBER.len());
    io.read_exact_buffered(&mut buf, VOLUME_MAGIC_NUMBER.len())
        .await?;
    if buf.as_ref() != VOLUME_MAGIC_NUMBER {
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
    io.write_all(VOLUME_MAGIC_NUMBER).await?;

    let mut sink = EncodingSinkBuilder::from_writer(&mut io).build();
    sink.send(volume_info.into()).await?;
    sink.close().await?;

    io.flush().await?;

    Ok(())
}

async fn read_manifest(
    reader: &mut impl Reader,
    fixed_specs: FixedSpecs,
) -> anyhow::Result<Manifest> {
    let mut buf = BytesMut::with_capacity(MANIFEST_MAGIC_NUMBER.len());
    reader
        .read_exact_buffered(&mut buf, MANIFEST_MAGIC_NUMBER.len())
        .await?;
    if buf.as_ref() != MANIFEST_MAGIC_NUMBER {
        bail!("invalid magic number");
    }

    let mut stream = DecodedStream::from_reader(reader, fixed_specs.clone());
    let mut manifest = match stream.try_next().await? {
        Some(Decoded::Manifest(manifest)) => {
            let manifest = manifest.into_header();
            if manifest.specs != fixed_specs {
                bail!("manifest appears to be for a different vbd");
            }
            manifest
        }
        _ => return Err(anyhow!("incorrect entry, expected manifest_info")),
    };

    while let Some(frame) = stream.try_next().await? {
        match frame {
            Decoded::Chunk(mut c) => {
                let chunk = c.read_body().await?;
                manifest.chunks.push(chunk);
            }
            _ => return Err(anyhow!("incorrect entry, expected chunk_info")),
        }
    }

    Ok(manifest)
}

async fn read_commit(
    data: Bytes,
    commit_type: CommitType,
    fixed_specs: FixedSpecs,
) -> anyhow::Result<CommitInfo> {
    let mut io = Cursor::new(data);
    let magic_number = match &commit_type {
        CommitType::Branch(_) => BRANCH_MAGIC_NUMBER,
        CommitType::Tag(_) => TAG_MAGIC_NUMBER,
    };
    let mut buf = BytesMut::with_capacity(magic_number.len());
    io.read_exact_buffered(&mut buf, magic_number.len()).await?;
    if buf.as_ref() != magic_number {
        bail!("invalid magic number");
    }

    let decoder = Decoder::new(fixed_specs);
    match decoder.read(io).await?.ok_or(anyhow!("unexpected eof"))? {
        Decoded::BranchInfo(b) => match commit_type {
            CommitType::Branch(branch_name) => Ok(CommitInfo::Branch(branch_name, b.into_header())),
            _ => Err(anyhow!("incorrect entry, expected branch_info")),
        },
        Decoded::TagInfo(t) => match commit_type {
            CommitType::Tag(tag_info) => Ok(CommitInfo::Tag(tag_info, t.into_header())),
            _ => Err(anyhow!("incorrect entry, expected tag_info")),
        },
        _ => Err(anyhow!("incorrect entry, commit info type entry")),
    }
}

async fn write_commit<IO: AsyncWrite + Send + Unpin>(
    mut io: IO,
    commit_info: &CommitInfo,
) -> anyhow::Result<()> {
    let (magic_number, encodable) = match commit_info {
        CommitInfo::Branch(_, branch_info) => (BRANCH_MAGIC_NUMBER, branch_info.into()),
        CommitInfo::Tag(_, tag_info) => (TAG_MAGIC_NUMBER, tag_info.into()),
    };
    io.write_all(magic_number).await?;

    let mut sink = EncodingSinkBuilder::from_writer(&mut io).build();
    sink.send(encodable).await?;
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

    async fn list_commits(&self) -> anyhow::Result<impl Iterator<Item = CommitType> + 'static> {
        Ok(match &self {
            Self::FsVolume(fs) => {
                let stream = fs.commits().await?;
                let res: Result<Vec<_>, <FsVolume as Volume>::Error> = stream.try_collect().await;
                res?.into_iter()
            }
            Self::RenterdVolume(renterd) => {
                let stream = renterd.commits().await?;
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

    async fn read_commit(&self, commit_type: &CommitType) -> anyhow::Result<Bytes> {
        Ok(match &self {
            Self::FsVolume(fs) => fs.read_commit(commit_type).await?,
            Self::RenterdVolume(renterd) => renterd.read_commit(commit_type).await?,
        })
    }

    async fn write_commit(&self, commit_type: &CommitType, content: Bytes) -> anyhow::Result<()> {
        Ok(match &self {
            Self::FsVolume(fs) => fs.write_commit(commit_type, content).await?,
            Self::RenterdVolume(renterd) => renterd.write_commit(commit_type, content).await?,
        })
    }

    async fn list_manifests(
        &self,
    ) -> anyhow::Result<impl Iterator<Item = (ManifestId, Etag)> + 'static> {
        Ok(match &self {
            Self::FsVolume(fs) => {
                let stream = fs.manifests().await?;
                let res: Result<Vec<_>, <FsVolume as Volume>::Error> = stream.try_collect().await;
                res?.into_iter()
            }
            Self::RenterdVolume(renterd) => {
                let stream = renterd.manifests().await?;
                let res: Result<Vec<_>, <RenterdVolume as Volume>::Error> =
                    stream.try_collect().await;
                res?.into_iter()
            }
        })
    }

    async fn manifest_reader(&self, id: &ManifestId) -> anyhow::Result<Box<dyn Reader + 'static>> {
        Ok(match &self {
            WrappedVolume::FsVolume(fs) => Box::new(fs.read_manifest(id).await?),
            WrappedVolume::RenterdVolume(renterd) => Box::new(renterd.read_manifest(id).await?),
        })
    }

    async fn write_manifest(
        &self,
        id: &ManifestId,
        content: impl AsyncRead + Send + Unpin + 'static,
    ) -> anyhow::Result<Etag> {
        Ok(match &self {
            Self::FsVolume(fs) => fs.write_manifest(id, content).await?,
            Self::RenterdVolume(renterd) => renterd.write_manifest(id, content).await?,
        })
    }

    async fn delete_manifest(&self, id: &ManifestId) -> anyhow::Result<()> {
        Ok(match &self {
            WrappedVolume::FsVolume(fs) => fs.delete_manifest(id).await?,
            WrappedVolume::RenterdVolume(renterd) => renterd.delete_manifest(id).await?,
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

#[derive(Clone)]
pub struct TagInfo {
    pub commit: Commit,
}

#[derive(Clone)]
pub enum CommitInfo {
    Branch(BranchName, BranchInfo),
    Tag(TagName, TagInfo),
}

impl CommitInfo {
    pub fn commit(&self) -> &Commit {
        match self {
            CommitInfo::Branch(_, info) => &info.commit,
            CommitInfo::Tag(_, info) => &info.commit,
        }
    }

    pub fn to_commit_type(&self) -> CommitType {
        match self {
            CommitInfo::Branch(name, _) => CommitType::Branch(name.clone()),
            CommitInfo::Tag(name, _) => CommitType::Tag(name.clone()),
        }
    }
}
