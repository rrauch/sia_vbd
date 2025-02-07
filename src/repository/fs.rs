use crate::io::{AsyncReadExtBuffered, ReadWrite, TokioFile};
use crate::repository::{Chunk, ChunkId, Reader, Repository, Stream, Volume, Writer};
use crate::vbd::{BranchName, VbdId};
use crate::Etag;
use anyhow::{anyhow, bail};
use bytes::{Bytes, BytesMut};
use futures::{AsyncSeekExt, AsyncWrite, AsyncWriteExt, StreamExt};
use std::future;
use std::io::{ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct FsRepository {
    root_dir: PathBuf,
}

impl FsRepository {
    pub async fn new(root_dir: impl AsRef<Path>) -> anyhow::Result<Self> {
        let root_dir = root_dir.as_ref().to_path_buf();
        if !is_dir(&root_dir).await? {
            bail!("{} not a directory", root_dir.display())
        }
        Ok(Self { root_dir })
    }
}

impl Repository for FsRepository {
    type Volume = FsVolume;
    type Error = anyhow::Error;

    async fn list(
        &self,
    ) -> Result<impl Stream<Item = Result<VbdId, Self::Error>> + 'static, Self::Error> {
        let stream =
            tokio_stream::wrappers::ReadDirStream::new(tokio::fs::read_dir(&self.root_dir).await?);
        let stream = stream
            .then(|r| async move {
                match r {
                    Ok(e) => {
                        if let Some(file_name) = e.file_name().to_str() {
                            match VbdId::try_from(file_name) {
                                Ok(vbd_id) => Ok(Some(vbd_id)),
                                Err(_) => Ok(None),
                            }
                        } else {
                            Ok(None)
                        }
                    }
                    Err(err) => Err(err.into()),
                }
            })
            .filter_map(|e| future::ready(e.transpose()));

        Ok(Box::pin(stream))
    }

    async fn open(&self, vbd_id: &VbdId) -> Result<Self::Volume, Self::Error> {
        let volume_dir = self.root_dir.join(format!("{}", vbd_id));
        if !is_dir(&volume_dir).await? {
            bail!("volume {} not found", vbd_id);
        }
        Ok(FsVolume::new(volume_dir))
    }

    async fn create(
        &self,
        vbd_id: &VbdId,
        branch: &BranchName,
        volume_info: Bytes,
        initial_commit: Bytes,
    ) -> Result<(), Self::Error> {
        let dir = self.root_dir.join(format!("{}", vbd_id));
        tokio::fs::create_dir(&dir).await?;
        let file = dir.join("sia_vbd.volume");
        let mut file = TokioFile::create_new(&file).await?;
        file.write_all(volume_info.as_ref()).await?;
        file.close().await?;
        let dir = dir.join("commits");
        tokio::fs::create_dir(&dir).await?;
        let file = dir.join(format!("{}.branch", branch.as_ref()));
        let mut file = TokioFile::create_new(&file).await?;
        file.write_all(initial_commit.as_ref()).await?;
        Ok(())
    }

    async fn delete(&self, vbd_id: &VbdId) -> Result<(), Self::Error> {
        let volume_dir = self.root_dir.join(format!("{}", &vbd_id));
        if !is_dir(&volume_dir).await? {
            bail!("invalid volume directory: {}", volume_dir.display());
        }
        let volume_file = volume_dir.join("sia_vbd.volume");
        if !is_file(&volume_file).await? {
            bail!("invalid volume file: {}", volume_file.display());
        }
        let commit_dir = volume_dir.join("commits");
        if !is_dir(&commit_dir).await? {
            bail!("invalid commits directory: {}", volume_dir.display());
        }
        let mut read_dir = tokio::fs::read_dir(&commit_dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();
            if !is_file(&path).await? {
                bail!("invalid directory entry found: {}", &path.display());
            }
            let file_name = path
                .file_name()
                .ok_or(anyhow!("invalid file name"))?
                .to_str()
                .ok_or(anyhow!("invalid file name"))?;
            if file_name.ends_with(".branch") || file_name.ends_with(".tag") {
                tokio::fs::remove_file(&path).await?;
            }
        }
        tokio::fs::remove_dir(&commit_dir).await?;

        let chunks_dir = volume_dir.join("chunks");
        let mut read_dir = tokio::fs::read_dir(&chunks_dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            let path = entry.path();
            if !entry.file_name().to_str().unwrap_or("").ends_with(".chunk")
                || !is_file(&path).await?
            {
                bail!("invalid directory entry found: {}", &path.display());
            }
            tokio::fs::remove_file(&path).await?;
        }
        tokio::fs::remove_dir(&chunks_dir).await?;

        tokio::fs::remove_file(&volume_file).await?;
        tokio::fs::remove_dir(&volume_dir).await?;
        Ok(())
    }
}

async fn etag(path: impl AsRef<Path>) -> Result<Etag, anyhow::Error> {
    let metadata = tokio::fs::metadata(path.as_ref()).await?;
    let etag = (&metadata).try_into()?;
    Ok(etag)
}

async fn is_dir(path: impl AsRef<Path>) -> Result<bool, anyhow::Error> {
    let metadata = tokio::fs::metadata(path.as_ref()).await?;
    Ok(metadata.is_dir())
}

async fn is_file(path: impl AsRef<Path>) -> Result<bool, anyhow::Error> {
    let metadata = tokio::fs::metadata(path.as_ref()).await?;
    Ok(metadata.is_file())
}

pub struct FsVolume {
    volume_dir: PathBuf,
    chunk_dir: PathBuf,
    commits_dir: PathBuf,
}

impl FsVolume {
    fn new(volume_dir: PathBuf) -> Self {
        Self {
            chunk_dir: volume_dir.join("chunks"),
            commits_dir: volume_dir.join("commits"),
            volume_dir,
        }
    }

    fn chunk_path(&self, chunk_id: &ChunkId) -> PathBuf {
        self.chunk_dir.join(format!("{}.chunk", chunk_id))
    }
}

const MAX_VOLUME_FILE_SIZE: u64 = 4096;
const MAX_COMMIT_FILE_SIZE: u64 = 4096;

impl Volume for FsVolume {
    type Error = anyhow::Error;

    async fn read_info(&self) -> Result<Bytes, Self::Error> {
        let file = self.volume_dir.join("sia_vbd.volume");
        read_file(&file, MAX_VOLUME_FILE_SIZE).await
    }

    async fn chunks(
        &self,
    ) -> Result<impl Stream<Item = Result<(ChunkId, Etag), Self::Error>> + 'static, Self::Error>
    {
        let stream =
            tokio_stream::wrappers::ReadDirStream::new(tokio::fs::read_dir(&self.chunk_dir).await?);
        let stream = stream
            .then(|r| async move {
                match r {
                    Ok(e) => {
                        if let Some(file_name) = e.file_name().to_str() {
                            if let Some(cid) = file_name.strip_suffix(".chunk") {
                                match ChunkId::try_from(cid) {
                                    Ok(chunk_id) => {
                                        if let Ok(etag) = etag(e.path()).await {
                                            Ok(Some((chunk_id, etag)))
                                        } else {
                                            Ok(None)
                                        }
                                    }
                                    Err(_) => Ok(None),
                                }
                            } else {
                                Ok(None)
                            }
                        } else {
                            Ok(None)
                        }
                    }
                    Err(err) => Err(err.into()),
                }
            })
            .filter_map(|e| future::ready(e.transpose()));

        Ok(Box::pin(stream))
    }

    async fn chunk_details(&self, chunk_id: &ChunkId) -> Result<Chunk, Self::Error> {
        let path = self.chunk_path(chunk_id);
        todo!()
    }

    async fn read_chunk(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> Result<impl Reader + 'static, Self::Error> {
        let path = self.chunk_path(chunk_id);
        let mut file = TokioFile::open(&path).await?;
        file.seek(SeekFrom::Start(offset)).await?;
        Ok(file)
    }

    async fn write_chunk(
        &self,
        chunk: &Chunk,
    ) -> Result<impl Writer<Ok = Etag> + 'static, Self::Error> {
        let path = self.chunk_path(chunk.id());
        Ok(ChunkWriter::new(TokioFile::create_new(&path).await?))
    }

    async fn delete_chunk(&self, chunk_id: &ChunkId) -> Result<(), Self::Error> {
        let path = self.chunk_path(chunk_id);
        if !is_file(&path).await? {
            bail!("chunk file {} not found", path.display());
        }
        tokio::fs::remove_file(&path).await?;
        Ok(())
    }

    async fn branches(
        &self,
    ) -> Result<impl Stream<Item = Result<BranchName, Self::Error>> + 'static, Self::Error> {
        let stream = tokio_stream::wrappers::ReadDirStream::new(
            tokio::fs::read_dir(&self.commits_dir).await?,
        );
        let stream = stream
            .then(|r| async move {
                match r {
                    Ok(e) => {
                        if let Some(file_name) = e.file_name().to_str() {
                            if let Some(branch) = file_name.strip_suffix(".branch") {
                                Ok(match branch.try_into() {
                                    Ok(branch) => Some(branch),
                                    Err(_) => None,
                                })
                            } else {
                                Ok(None)
                            }
                        } else {
                            Ok(None)
                        }
                    }
                    Err(err) => Err(err.into()),
                }
            })
            .filter_map(|e| future::ready(e.transpose()));

        Ok(Box::pin(stream))
    }

    async fn write_commit(&self, branch: &BranchName, commit: Bytes) -> Result<(), Self::Error> {
        let path = self.commits_dir.join(format!("{}.branch", branch.as_ref()));
        write_bak(&path, commit.as_ref()).await?;
        Ok(())
    }

    async fn read_commit(&self, branch: &BranchName) -> Result<Bytes, Self::Error> {
        let path = self.commits_dir.join(format!("{}.branch", branch.as_ref()));
        read_file(&path, MAX_COMMIT_FILE_SIZE).await
    }
}

async fn write_bak(final_path: &Path, data: &[u8]) -> Result<(), anyhow::Error> {
    let mut temp_path = final_path.to_path_buf();
    let mut bak_path = final_path.to_path_buf();
    let file_name = final_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or(anyhow!("invalid file name"))?;
    temp_path.set_file_name(format!("{}.incomplete", file_name));
    bak_path.set_file_name(format!("{}.bak", file_name));

    if tokio::fs::try_exists(&temp_path).await? {
        tokio::fs::remove_file(&temp_path).await?;
    }
    let mut file = TokioFile::create_new(&temp_path).await?;
    file.write_all(data).await?;
    file.close().await?;

    if tokio::fs::try_exists(&bak_path).await? {
        tokio::fs::remove_file(&bak_path).await?;
    }
    if tokio::fs::try_exists(&final_path).await? {
        tokio::fs::rename(&final_path, &bak_path).await?;
    }
    tokio::fs::rename(&temp_path, &final_path).await?;
    Ok(())
}

async fn read_file(path: &Path, max_file_size: u64) -> Result<Bytes, anyhow::Error> {
    let metadata = tokio::fs::metadata(path).await?;
    if !metadata.is_file() {
        bail!("entry is not a file");
    }
    if metadata.len() > max_file_size {
        bail!("max file size exceeded");
    }
    let mut buf = BytesMut::with_capacity(metadata.len() as usize);
    let mut file = TokioFile::open(path).await?;
    file.read_all_buffered(&mut buf).await?;
    Ok(buf.freeze())
}

pub(crate) struct ChunkWriter {
    file: TokioFile<ReadWrite>,
}

impl ChunkWriter {
    fn new(file: TokioFile<ReadWrite>) -> Self {
        Self { file }
    }
}

impl Writer for ChunkWriter {
    type Ok = Etag;
    type Error = anyhow::Error;

    async fn finalize(mut self) -> Result<Etag, Self::Error> {
        self.file.close().await?;
        let etag = etag(self.file.path()).await?;
        Ok(etag)
    }

    async fn cancel(mut self) -> Result<(), Self::Error> {
        self.file.close().await?;
        tokio::fs::remove_file(self.file.path()).await?;
        Ok(())
    }
}

impl AsyncWrite for ChunkWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.file).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(std::io::Error::new(
            ErrorKind::Unsupported,
            "writer must be closed via call to 'finalize' or 'cancel'",
        )))
    }
}
