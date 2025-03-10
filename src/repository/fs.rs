use crate::inventory::chunk::ManifestId;
use crate::io::{AsyncReadExtBuffered, TokioFile};
use crate::repository::{ChunkId, CommitType, Reader, Repository, Stream, Volume, VolumeInfo};
use crate::vbd::{BranchName, VbdId};
use crate::Etag;
use anyhow::{anyhow, bail};
use bytes::{Bytes, BytesMut};
use futures::io::Cursor;
use futures::{AsyncRead, AsyncSeekExt, AsyncWriteExt, StreamExt};
use std::fmt::{Display, Formatter};
use std::future;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};

pub struct FsRepository {
    root_dir: PathBuf,
}

impl FsRepository {
    pub fn new(root_dir: impl AsRef<Path>) -> Self {
        Self {
            root_dir: root_dir.as_ref().to_path_buf(),
        }
    }
}

impl Display for FsRepository {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Local File System [{}]", self.root_dir.display())
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

    async fn details(
        &self,
        vbd_id: &VbdId,
    ) -> Result<
        (
            VolumeInfo,
            impl Stream<Item = Result<(CommitType, Bytes), Self::Error>> + 'static,
        ),
        Self::Error,
    > {
        let volume_dir = self.root_dir.join(format!("{}", vbd_id));
        if !is_dir(&volume_dir).await? {
            bail!("volume {} not found", vbd_id);
        }
        let volume_info = read_volume_info(&volume_dir).await?;
        let volume_info = super::read_volume(volume_info).await?;
        let commits_dir = volume_dir.join("commits");

        let stream = list_commits(&commits_dir).await?.then(move |r| {
            let commits_dir = commits_dir.clone();
            async move {
                match r {
                    Ok(commit_type) => match read_commit(&commits_dir, &commit_type).await {
                        Ok(bytes) => Ok((commit_type, bytes)),
                        Err(err) => Err(err),
                    },
                    Err(err) => Err(err),
                }
            }
        });

        Ok((volume_info, Box::pin(stream)))
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
        let chunk_dir = dir.join("chunks");
        tokio::fs::create_dir(&chunk_dir).await?;
        let manifest_dir = dir.join("manifests");
        tokio::fs::create_dir(&manifest_dir).await?;
        let commit_dir = dir.join("commits");
        tokio::fs::create_dir(&commit_dir).await?;
        let file = commit_dir.join(format!("{}.branch", branch.as_ref()));
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

        let manifest_dir = volume_dir.join("manifests");
        if !is_dir(&manifest_dir).await? {
            bail!("invalid manifests directory: {}", volume_dir.display());
        }
        let mut read_dir = tokio::fs::read_dir(&manifest_dir).await?;
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
            if file_name.ends_with(".manifest") {
                tokio::fs::remove_file(&path).await?;
            }
        }
        tokio::fs::remove_dir(&manifest_dir).await?;

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

    async fn write_commit(
        &self,
        vbd_id: &VbdId,
        commit_type: &CommitType,
        data: Bytes,
    ) -> anyhow::Result<()> {
        let volume_dir = self.root_dir.join(format!("{}", &vbd_id));
        write_commit(&volume_dir.join("commits"), commit_type, data).await
    }

    async fn delete_commit(&self, vbd_id: &VbdId, commit_type: &CommitType) -> anyhow::Result<()> {
        let volume_dir = self.root_dir.join(format!("{}", &vbd_id));
        let commits_dir = volume_dir.join("commits");
        let path = match commit_type {
            CommitType::Branch(branch_name) => commits_dir.join(format!("{}.branch", branch_name)),
            CommitType::Tag(tag_name) => commits_dir.join(format!("{}.tag", tag_name)),
        };
        tokio::fs::remove_file(path).await?;
        Ok(())
    }
}

async fn etag(path: impl AsRef<Path>) -> Result<Etag, std::io::Error> {
    let metadata = tokio::fs::metadata(path.as_ref()).await?;
    let etag = (&metadata).try_into()?;
    Ok(etag)
}

async fn is_dir(path: impl AsRef<Path>) -> Result<bool, std::io::Error> {
    let metadata = tokio::fs::metadata(path.as_ref()).await?;
    Ok(metadata.is_dir())
}

async fn is_file(path: impl AsRef<Path>) -> Result<bool, std::io::Error> {
    let metadata = tokio::fs::metadata(path.as_ref()).await?;
    Ok(metadata.is_file())
}

pub struct FsVolume {
    volume_dir: PathBuf,
    manifest_dir: PathBuf,
    chunk_dir: PathBuf,
    commits_dir: PathBuf,
}

impl FsVolume {
    fn new(volume_dir: PathBuf) -> Self {
        Self {
            chunk_dir: volume_dir.join("chunks"),
            commits_dir: volume_dir.join("commits"),
            manifest_dir: volume_dir.join("manifests"),
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
        read_volume_info(&self.volume_dir).await
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

    async fn manifests(
        &self,
    ) -> Result<impl Stream<Item = Result<(ManifestId, Etag), Self::Error>> + 'static, Self::Error>
    {
        let stream = tokio_stream::wrappers::ReadDirStream::new(
            tokio::fs::read_dir(&self.manifest_dir).await?,
        );
        let stream = stream
            .then(|r| async move {
                match r {
                    Ok(e) => {
                        if let Some(file_name) = e.file_name().to_str() {
                            if let Some(id) = file_name.strip_suffix(".manifest") {
                                match ManifestId::try_from(id) {
                                    Ok(id) => {
                                        if let Ok(etag) = etag(e.path()).await {
                                            Ok(Some((id, etag)))
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

    async fn read_manifest(&self, id: &ManifestId) -> Result<impl Reader + 'static, Self::Error> {
        let path = self.manifest_dir.join(format!("{}.manifest", id));
        Ok(TokioFile::open(&path).await?)
    }

    async fn write_manifest(
        &self,
        id: &ManifestId,
        content: impl AsyncRead + Send + Unpin + 'static,
    ) -> Result<Etag, Self::Error> {
        let path = self.manifest_dir.join(format!("{}.manifest", id));
        write_file(&path, content).await?;
        let etag = etag(&path).await?;
        Ok(etag)
    }

    async fn delete_manifest(&self, id: &ManifestId) -> Result<(), Self::Error> {
        let path = self.manifest_dir.join(format!("{}.manifest", id));
        Ok(tokio::fs::remove_file(&path).await?)
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
        chunk_id: &ChunkId,
        len: u64,
        content: impl Reader + 'static,
    ) -> Result<Etag, Self::Error> {
        let path = self.chunk_path(chunk_id);
        let mut writer = TokioFile::create_new(&path).await?;
        match futures::io::copy(content, &mut writer).await {
            Ok(_) => {
                writer.close().await?;
                let success = match tokio::fs::metadata(&path).await {
                    Ok(metadata) => metadata.is_file() && metadata.len() == len,
                    Err(_) => false,
                };
                if !success {
                    let _ = tokio::fs::remove_file(&path).await;
                    bail!("chunk creation failed");
                }
                let etag = etag(&path).await?;
                Ok(etag)
            }
            Err(err) => {
                writer.close().await?;
                tokio::fs::remove_file(&path).await?;
                Err(err)?
            }
        }
    }

    async fn delete_chunk(&self, chunk_id: &ChunkId) -> Result<(), Self::Error> {
        let path = self.chunk_path(chunk_id);
        if !is_file(&path).await? {
            bail!("chunk file {} not found", path.display());
        }
        tokio::fs::remove_file(&path).await?;
        Ok(())
    }

    async fn commits(
        &self,
    ) -> Result<impl Stream<Item = Result<CommitType, Self::Error>> + 'static, Self::Error> {
        list_commits(&self.commits_dir).await
    }

    async fn write_commit(
        &self,
        commit_type: &CommitType,
        commit: Bytes,
    ) -> Result<(), Self::Error> {
        write_commit(&self.commits_dir, commit_type, commit).await
    }

    async fn read_commit(&self, commit_type: &CommitType) -> Result<Bytes, Self::Error> {
        read_commit(&self.commits_dir, commit_type).await
    }
}

async fn read_commit(commits_dir: &Path, commit_type: &CommitType) -> Result<Bytes, anyhow::Error> {
    let path = match commit_type {
        CommitType::Branch(branch_name) => commits_dir.join(format!("{}.branch", branch_name)),
        CommitType::Tag(tag_name) => commits_dir.join(format!("{}.tag", tag_name)),
    };
    read_file(&path, MAX_COMMIT_FILE_SIZE).await
}

async fn write_commit(
    commits_dir: &Path,
    commit_type: &CommitType,
    commit: Bytes,
) -> Result<(), anyhow::Error> {
    let path = match commit_type {
        CommitType::Branch(branch_name) => commits_dir.join(format!("{}.branch", branch_name)),
        CommitType::Tag(tag_name) => commits_dir.join(format!("{}.tag", tag_name)),
    };
    write_file(&path, Cursor::new(commit)).await?;
    Ok(())
}

async fn list_commits(
    commits_dir: &Path,
) -> Result<impl Stream<Item = Result<CommitType, anyhow::Error>> + 'static, anyhow::Error> {
    let stream =
        tokio_stream::wrappers::ReadDirStream::new(tokio::fs::read_dir(commits_dir).await?);
    let stream = stream
        .then(|r| async move {
            match r {
                Ok(e) => {
                    if let Some(file_name) = e.file_name().to_str() {
                        if let Some(branch) = file_name.strip_suffix(".branch") {
                            Ok(match branch.try_into() {
                                Ok(branch) => Some(CommitType::Branch(branch)),
                                Err(_) => None,
                            })
                        } else if let Some(tag) = file_name.strip_suffix(".tag") {
                            Ok(match tag.try_into() {
                                Ok(tag) => Some(CommitType::Tag(tag)),
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

async fn read_volume_info(volume_dir: &Path) -> Result<Bytes, anyhow::Error> {
    let file = volume_dir.join("sia_vbd.volume");
    read_file(&file, MAX_VOLUME_FILE_SIZE).await
}

async fn write_file(
    final_path: &Path,
    reader: impl AsyncRead + Send + Unpin + 'static,
) -> Result<(), anyhow::Error> {
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
    futures::io::copy(reader, &mut file).await?;
    file.close().await?;

    if tokio::fs::try_exists(&bak_path).await? {
        tokio::fs::remove_file(&bak_path).await?;
    }
    if tokio::fs::try_exists(&final_path).await? {
        tokio::fs::rename(&final_path, &bak_path).await?;
    }
    tokio::fs::rename(&temp_path, &final_path).await?;
    let _ = tokio::fs::remove_file(&bak_path).await;
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
