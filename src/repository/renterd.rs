use crate::hash::HashAlgorithm;
use crate::inventory::chunk::{ChunkId, ManifestId};
use crate::io::AsyncReadExtBuffered;
use crate::repository::{
    read_volume, Reader, Repository, Stream, Volume, VolumeInfo, VOLUME_MAGIC_NUMBER,
};
use crate::serde::encoded::{Decoded, Decoder};
use crate::vbd::{BlockSize, BranchName, ClusterSize, FixedSpecs, VbdId};
use crate::Etag;
use anyhow::{anyhow, bail};
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::io::Cursor;
use futures::{stream, AsyncRead, AsyncSeek, StreamExt, TryStreamExt};
use renterd_client::bus::object::{Metadata, RenameMode};
use std::fmt::{Debug, Display, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

const MAX_VOLUME_FILE_SIZE: u64 = 4096;
const MAX_COMMIT_FILE_SIZE: u64 = 4096;

pub struct RenterdRepository {
    renterd: renterd_client::Client,
    endpoint: Url,
    bucket: Bucket,
    path: Path,
}

impl Display for RenterdRepository {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "renterd [endpoint={}, bucket={}, path={}]",
            self.endpoint,
            self.bucket.as_ref(),
            self.path.as_ref()
        )
    }
}

impl RenterdRepository {
    pub fn new(renterd: renterd_client::Client, endpoint: Url, bucket: Bucket, path: Path) -> Self {
        Self {
            renterd,
            endpoint,
            bucket,
            path,
        }
    }

    fn volume_path(&self, vbd_id: &VbdId) -> anyhow::Result<Path> {
        Ok(self
            .path
            .try_join(format!("{}.volume", vbd_id).as_str(), true)?)
    }

    fn commits_path(&self, vbd_id: &VbdId) -> anyhow::Result<Path> {
        self.volume_path(vbd_id)?.try_join("commits", true)
    }
}

impl Repository for RenterdRepository {
    type Volume = RenterdVolume;
    type Error = anyhow::Error;

    async fn list(
        &self,
    ) -> Result<impl Stream<Item = Result<VbdId, Self::Error>> + 'static, Self::Error> {
        let stream = list_dir(&self.renterd, &self.bucket, &self.path)
            .await?
            .filter_map(|res| async {
                match res.map(|o| {
                    o.as_dir()
                        .map(|d| {
                            d.name()
                                .strip_suffix(".volume")
                                .map(|id| VbdId::try_from(id).ok())
                        })
                        .flatten()
                        .flatten()
                }) {
                    Ok(Some(id)) => Some(Ok(id)),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                }
            });
        Ok(Box::pin(stream))
    }

    async fn open(&self, vbd_id: &VbdId) -> Result<Self::Volume, Self::Error> {
        Ok(RenterdVolume::new(
            self.bucket.clone(),
            self.volume_path(vbd_id)?,
            self.renterd.clone(),
        )
        .await?)
    }

    async fn details(
        &self,
        vbd_id: &VbdId,
    ) -> Result<
        (
            VolumeInfo,
            impl Stream<Item = Result<(BranchName, Bytes), Self::Error>> + 'static,
        ),
        Self::Error,
    > {
        let volume_path = self
            .path
            .try_join(format!("{}.volume", vbd_id).as_str(), true)?;
        let volume_info_path = volume_path.try_join("sia_vbd.volume", false)?;
        let volume_info =
            read_volume(read_volume_info(&self.renterd, &volume_info_path, &self.bucket).await?)
                .await?;
        let commits_path = volume_path.try_join("commits", true)?;
        let renterd = self.renterd.clone();
        let bucket = self.bucket.clone();

        let stream = list_dir(&self.renterd, &self.bucket, &commits_path)
            .await?
            .filter_map(|res| async {
                match res.map(|o| {
                    o.as_file()
                        .map(|f| {
                            f.name()
                                .strip_suffix(".branch")
                                .map(|name| BranchName::try_from(name))
                        })
                        .flatten()
                }) {
                    Ok(Some(Ok(name))) => Some(Ok(name)),
                    Ok(None) => None,
                    Ok(Some(Err(err))) => Some(Err(err)),
                    Err(err) => Some(Err(err)),
                }
            })
            .then(move |r| {
                let renterd = renterd.clone();
                let bucket = bucket.clone();
                let commits_path = commits_path.clone();
                async move {
                    match r {
                        Ok(branch_name) => {
                            match commits_path
                                .try_join(format!("{}.branch", branch_name).as_str(), false)
                            {
                                Ok(path) => read_commit(&renterd, &path, &bucket)
                                    .await
                                    .map(|data| (branch_name, data)),
                                Err(err) => Err(err),
                            }
                        }
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
        let name = format!("{}.volume", vbd_id);
        let volume_dir = mkdir(&self.renterd, name.as_str(), &self.path, &self.bucket).await?;
        write_file(
            &self.renterd,
            volume_dir.path(),
            "sia_vbd.volume",
            &self.bucket,
            None,
            Cursor::new(volume_info),
            None,
            false,
        )
        .await?;
        let commits = mkdir(&self.renterd, "commits", volume_dir.path(), &self.bucket).await?;
        write_file(
            &self.renterd,
            commits.path(),
            format!("{}.branch", branch.as_ref()).as_str(),
            &self.bucket,
            None,
            Cursor::new(initial_commit),
            None,
            false,
        )
        .await?;
        let chunk_dir = mkdir(&self.renterd, "chunks", volume_dir.path(), &self.bucket).await?;
        for i in 0u8..255 {
            let name = format!("{:02x}", i);
            mkdir(&self.renterd, name.as_str(), chunk_dir.path(), &self.bucket).await?;
        }
        mkdir(&self.renterd, "manifests", volume_dir.path(), &self.bucket).await?;
        Ok(())
    }

    async fn delete(&self, vbd_id: &VbdId) -> Result<(), Self::Error> {
        let path = self.volume_path(vbd_id)?;
        self.renterd
            .worker()
            .object()
            .delete(&path, Some(self.bucket.to_string()), true)
            .await?;
        Ok(())
    }

    async fn write_branch(
        &self,
        vbd_id: &VbdId,
        branch_name: &BranchName,
        data: Bytes,
    ) -> anyhow::Result<()> {
        let len = data.len() as u64;
        write_file(
            &self.renterd,
            &self.commits_path(vbd_id)?,
            format!("{}.branch", branch_name.as_ref()).as_str(),
            &self.bucket,
            None,
            Cursor::new(data),
            Some(len),
            true,
        )
        .await?;
        Ok(())
    }

    async fn delete_branch(&self, vbd_id: &VbdId, branch_name: &BranchName) -> anyhow::Result<()> {
        let path = self
            .commits_path(vbd_id)?
            .try_join(format!("{}.branch", branch_name.as_ref()).as_str(), false)?;
        self.renterd
            .bus()
            .object()
            .delete(path, Some(self.bucket.to_string()), false)
            .await?;
        Ok(())
    }
}

pub struct RenterdVolume {
    renterd: renterd_client::Client,
    bucket: Bucket,
    specs: FixedSpecs,
    volume_info: File,
    chunks: Directory,
    manifests: Directory,
    commits: Directory,
}

impl RenterdVolume {
    async fn new(
        bucket: Bucket,
        path: Path,
        renterd: renterd_client::Client,
    ) -> anyhow::Result<Self> {
        let mut volume_info = None;
        let mut chunks = None;
        let mut commits = None;
        let mut manifests = None;

        let mut stream = list_dir(&renterd, &bucket, &path).await?;
        while let Some(res) = stream.next().await {
            let object = res?;
            match object {
                Object::File(file) => match file.name() {
                    "sia_vbd.volume" => {
                        volume_info = Some(file);
                    }
                    _ => continue,
                },
                Object::Directory(dir) => match dir.name() {
                    "chunks" => {
                        chunks = Some(dir);
                    }
                    "commits" => {
                        commits = Some(dir);
                    }
                    "manifests" => {
                        manifests = Some(dir);
                    }
                    _ => continue,
                },
            }
            if volume_info.is_some() && chunks.is_some() && commits.is_some() {
                break;
            }
        }

        if volume_info.is_none() {
            bail!("sia_vbd.volume not found in {}", path);
        }
        let volume_info = volume_info.unwrap();

        if chunks.is_none() {
            bail!("chunks not found in {}", path);
        }
        let chunks = chunks.unwrap();

        if commits.is_none() {
            bail!("commits not found in {}", path);
        }
        let commits = commits.unwrap();

        if manifests.is_none() {
            bail!("manifests not found in {}", path);
        }
        let manifests = manifests.unwrap();

        let mut reader = read_file(
            &renterd,
            volume_info.path(),
            &bucket,
            0,
            Option::from(MAX_VOLUME_FILE_SIZE),
        )
        .await?;

        let mut buf = BytesMut::with_capacity(VOLUME_MAGIC_NUMBER.len());
        reader
            .read_exact_buffered(&mut buf, VOLUME_MAGIC_NUMBER.len())
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
        let specs = match decoder
            .read(reader)
            .await?
            .ok_or(anyhow!("unexpected eof"))?
        {
            Decoded::VolumeInfo(v) => Ok(v.into_header()),
            _ => Err(anyhow!("incorrect entry, expected volume_info")),
        }?
        .specs;

        Ok(Self {
            renterd,
            bucket,
            specs,
            volume_info,
            manifests,
            chunks,
            commits,
        })
    }

    fn chunk_path(&self, chunk_id: &ChunkId) -> anyhow::Result<Path> {
        let bucket = (chunk_id.as_u128() & 255) as u8;
        let dir = self
            .chunks
            .path()
            .try_join(format!("{:02x}", bucket).as_str(), true)?;
        Ok(dir.try_join(format!("{}.chunk", chunk_id).as_str(), false)?)
    }

    fn commit_path(&self, branch_name: &BranchName) -> anyhow::Result<Path> {
        Ok(self
            .commits
            .path()
            .try_join(format!("{}.branch", branch_name.as_ref()).as_str(), false)?)
    }
}

impl Volume for RenterdVolume {
    type Error = anyhow::Error;

    async fn read_info(&self) -> Result<Bytes, Self::Error> {
        read_volume_info(&self.renterd, self.volume_info.path(), &self.bucket).await
    }

    async fn chunks(
        &self,
    ) -> Result<impl Stream<Item = Result<(ChunkId, Etag), Self::Error>> + 'static, Self::Error>
    {
        let mut streams = Vec::with_capacity(256);
        for i in 0u8..255 {
            let path = self
                .chunks
                .path()
                .try_join(format!("{:02x}", i).as_str(), true)?;
            let stream = list_dir(&self.renterd, &self.bucket, &path)
                .await?
                .filter_map(|res| async {
                    match res.map(|o| {
                        o.as_file()
                            .map(|f| {
                                f.name()
                                    .strip_suffix(".chunk")
                                    .map(|id| {
                                        ChunkId::try_from(id)
                                            .ok()
                                            .map(|c| (c, f.etag().map(|e| e.clone())))
                                    })
                                    .map(|opt| {
                                        opt.map(|(id, etag)| match etag {
                                            Some(etag) => Some((id, etag)),
                                            None => None,
                                        })
                                    })
                                    .flatten()
                            })
                            .flatten()
                            .flatten()
                    }) {
                        Ok(Some((id, etag))) => Some(Ok((id, etag))),
                        Ok(None) => None,
                        Err(err) => Some(Err(err)),
                    }
                });
            streams.push(Box::pin(stream));
        }

        Ok(Box::pin(futures::stream::select_all(streams)))
    }

    async fn manifests(
        &self,
    ) -> Result<impl Stream<Item = Result<(ManifestId, Etag), Self::Error>> + 'static, Self::Error>
    {
        let stream = list_dir(&self.renterd, &self.bucket, self.manifests.path())
            .await?
            .filter_map(|res| async {
                match res.map(|o| {
                    o.as_file()
                        .map(|f| {
                            f.name()
                                .strip_suffix(".manifest")
                                .map(|id| {
                                    ManifestId::try_from(id)
                                        .ok()
                                        .map(|c| (c, f.etag().map(|e| e.clone())))
                                })
                                .map(|opt| {
                                    opt.map(|(id, etag)| match etag {
                                        Some(etag) => Some((id, etag)),
                                        None => None,
                                    })
                                })
                                .flatten()
                        })
                        .flatten()
                        .flatten()
                }) {
                    Ok(Some((id, etag))) => Some(Ok((id, etag))),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                }
            });

        Ok(Box::pin(stream))
    }

    async fn read_manifest(&self, id: &ManifestId) -> Result<impl Reader + 'static, Self::Error> {
        let path = self
            .manifests
            .path()
            .try_join(format!("{}.manifest", id).as_str(), false)?;
        Ok(read_file(&self.renterd, &path, &self.bucket, 0, None).await?)
    }

    async fn write_manifest(
        &self,
        id: &ManifestId,
        content: impl AsyncRead + Send + Unpin + 'static,
    ) -> Result<Etag, Self::Error> {
        let file = write_file(
            &self.renterd,
            self.manifests.path(),
            format!("{}.manifest", id).as_str(),
            &self.bucket,
            None,
            content,
            None,
            true,
        )
        .await?;
        Ok(file.etag().ok_or(anyhow!("etag missing"))?.clone())
    }

    async fn delete_manifest(&self, id: &ManifestId) -> Result<(), Self::Error> {
        let path = self
            .manifests
            .path()
            .try_join(format!("{}.manifest", id).as_str(), false)?;
        self.renterd
            .worker()
            .object()
            .delete(&path, Some(self.bucket.to_string()), false)
            .await?;
        Ok(())
    }

    async fn read_chunk(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
    ) -> Result<impl Reader + 'static, Self::Error> {
        let path = self.chunk_path(&chunk_id)?;
        Ok(read_file(&self.renterd, &path, &self.bucket, offset, None).await?)
    }

    async fn write_chunk(
        &self,
        chunk_id: &ChunkId,
        len: u64,
        content: impl Reader + 'static,
    ) -> Result<Etag, Self::Error> {
        let path = self.chunk_path(chunk_id)?;
        let (parent, name) = match path.split() {
            (Some(parent), Some(name)) => (parent, name),
            _ => bail!("invalid path for chunk"),
        };

        let file = write_file(
            &self.renterd,
            &parent,
            name,
            &self.bucket,
            None,
            content,
            Some(len),
            false,
        )
        .await?;

        Ok(file.etag().ok_or(anyhow!("etag missing"))?.clone())
    }

    async fn delete_chunk(&self, chunk_id: &ChunkId) -> Result<(), Self::Error> {
        let path = self.chunk_path(chunk_id)?;
        self.renterd
            .worker()
            .object()
            .delete(&path, Some(self.bucket.to_string()), false)
            .await?;
        Ok(())
    }

    async fn branches(
        &self,
    ) -> Result<impl Stream<Item = Result<BranchName, Self::Error>> + 'static, Self::Error> {
        let stream = list_dir(&self.renterd, &self.bucket, self.commits.path())
            .await?
            .filter_map(|res| async {
                match res.map(|o| {
                    o.as_file()
                        .map(|f| {
                            f.name()
                                .strip_suffix(".branch")
                                .map(|name| BranchName::try_from(name).ok())
                        })
                        .flatten()
                        .flatten()
                }) {
                    Ok(Some(name)) => Some(Ok(name)),
                    Ok(None) => None,
                    Err(err) => Some(Err(err)),
                }
            });
        Ok(Box::pin(stream))
    }

    async fn write_commit(&self, branch: &BranchName, commit: Bytes) -> Result<(), Self::Error> {
        let path = self.commits.path();
        write_file(
            &self.renterd,
            &path,
            format!("{}.branch", branch.as_ref()).as_str(),
            &self.bucket,
            None,
            Cursor::new(commit),
            None,
            true,
        )
        .await?;

        Ok(())
    }

    async fn read_commit(&self, branch: &BranchName) -> Result<Bytes, Self::Error> {
        let path = self.commit_path(branch)?;
        read_commit(&self.renterd, &path, &self.bucket).await
    }
}

async fn read_commit(
    renterd: &renterd_client::Client,
    path: &Path,
    bucket: &Bucket,
) -> anyhow::Result<Bytes> {
    let mut reader = read_file(&renterd, &path, &bucket, 0, Some(MAX_COMMIT_FILE_SIZE)).await?;
    let mut buf = BytesMut::with_capacity(MAX_COMMIT_FILE_SIZE as usize);
    reader.read_all_buffered(&mut buf).await?;
    Ok(buf.freeze())
}

#[derive(PartialEq, Clone, Debug)]
pub enum Object {
    File(File),
    Directory(Directory),
}

impl Object {
    pub fn name(&self) -> &str {
        match self {
            Object::File(file) => file.name(),
            Object::Directory(dir) => dir.name(),
        }
    }

    pub fn as_file(&self) -> Option<&File> {
        match self {
            Object::File(file) => Some(file),
            Object::Directory(_) => None,
        }
    }

    pub fn as_dir(&self) -> Option<&Directory> {
        match self {
            Object::File(_) => None,
            Object::Directory(dir) => Some(dir),
        }
    }

    pub fn try_from(metadata: Metadata, parent: &Path) -> anyhow::Result<Self> {
        let name = match metadata.name.strip_prefix(parent.as_ref()) {
            Some(name) => name,
            None => bail!("path does not match name"),
        };

        let (name, is_dir) = match name.split_once('/') {
            None => (name, false),
            Some((name, suffix)) if suffix.is_empty() && !name.is_empty() => (name, true),
            _ => {
                bail!("invalid name: {}", name);
            }
        };

        let last_modified = metadata.mod_time.to_utc();
        let etag = metadata.etag.map(|s| Bytes::from(s).into());

        Ok(if is_dir {
            Object::Directory(Directory {
                name: name.to_string(),
                path: metadata.name.into(),
                parent: parent.clone(),
            })
        } else {
            Object::File(File {
                name: name.to_string(),
                size: metadata.size,
                last_modified,
                path: metadata.name.into(),
                etag,
                mime_type: metadata.mime_type,
                parent: parent.clone(),
            })
        })
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct File {
    name: String,
    path: Path,
    parent: Path,
    size: u64,
    last_modified: DateTime<Utc>,
    etag: Option<Etag>,
    mime_type: Option<String>,
}

impl File {
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn parent(&self) -> &Path {
        &self.parent
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn last_modified(&self) -> &DateTime<Utc> {
        &self.last_modified
    }

    pub fn etag(&self) -> Option<&Etag> {
        self.etag.as_ref()
    }

    pub fn mime_type(&self) -> Option<&str> {
        self.mime_type.as_ref().map(|s| s.as_str())
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct Directory {
    name: String,
    path: Path,
    parent: Path,
}

impl Directory {
    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn parent(&self) -> &Path {
        &self.parent
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct Bucket(Arc<String>);

impl AsRef<str> for Bucket {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for Bucket {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl From<String> for Bucket {
    fn from(value: String) -> Self {
        Self(Arc::new(value))
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct Path(Arc<String>);

impl Path {
    fn split(&self) -> (Option<Path>, Option<&str>) {
        let path = self.0.as_str();
        let path = path.strip_suffix('/').unwrap_or_else(|| path);

        let (parent, name) = match path.rsplit_once("/") {
            Some((parent, name)) => {
                let parent = Some(Path(Arc::new(format!("{}/", parent))));
                let name = if !name.is_empty() { Some(name) } else { None };
                (parent, name)
            }
            None => (None, None),
        };

        (parent, name)
    }

    fn is_dir(&self) -> bool {
        self.0.as_str().ends_with('/')
    }

    fn try_join(&self, name: &str, is_dir: bool) -> anyhow::Result<Self> {
        if name.contains("/") {
            bail!("invalid name");
        }
        if !self.as_ref().starts_with("/") {
            bail!("invalid parent");
        }
        let path = if is_dir {
            format!("{}{}/", self.as_ref(), name)
        } else {
            format!("{}{}", self.as_ref(), name)
        };

        Ok(Path(Arc::new(path)))
    }
}

impl AsRef<str> for Path {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl From<String> for Path {
    fn from(value: String) -> Self {
        Self(Arc::new(value))
    }
}

async fn list_dir(
    renterd: &renterd_client::Client,
    bucket: &Bucket,
    path: &Path,
) -> Result<impl Stream<Item = Result<Object, anyhow::Error>> + 'static, anyhow::Error> {
    let path = path.clone();
    let stream = renterd.bus().object().list(
        NonZeroUsize::new(100).unwrap(),
        Some(path.as_ref().to_string()),
        Some(bucket.as_ref().to_string()),
    )?;

    let stream = stream
        .try_filter_map(move |m| {
            let path = path.clone();
            async move {
                let res = m
                    .into_iter()
                    .filter(|m| {
                        // filter out all entries that do not belong to this directory
                        match m.name.as_str().strip_prefix(path.as_ref()) {
                            Some(name) if !name.is_empty() => match name.split_once('/') {
                                Some((name, suffix)) if !suffix.is_empty() && !name.is_empty() => {
                                    false
                                }
                                _ => true,
                            },
                            _ => false,
                        }
                    })
                    .map(|m| Object::try_from(m, &path))
                    .collect::<Vec<_>>();
                if res.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(res))
                }
            }
        })
        .flat_map(|res| stream::iter(res.unwrap_or_else(|_| vec![])));

    Ok(Box::pin(stream))
}

async fn mkdir(
    renterd: &renterd_client::Client,
    name: &str,
    parent: &Path,
    bucket: &Bucket,
) -> anyhow::Result<Directory> {
    let path = parent.try_join(name, true)?;

    if find_object(renterd, &path, bucket).await?.is_some() {
        bail!("{} already exists", path);
    }

    // a directory is created by uploading an empty file with a name ending in "/"
    renterd
        .worker()
        .object()
        .upload(
            path.as_ref(),
            None,
            None,
            Some(bucket.to_string()),
            Cursor::new(vec![]),
        )
        .await?;

    match find_object(renterd, &path, bucket).await? {
        Some(Object::Directory(dir)) => Ok(dir),
        Some(_) => Err(anyhow!("{} is a file, expected a directory", path)),
        None => Err(anyhow!("directory {} does not exist", path)),
    }
}

async fn find_object(
    renterd: &renterd_client::Client,
    path: &Path,
    bucket: &Bucket,
) -> anyhow::Result<Option<Object>> {
    let mut matches = renterd
        .bus()
        .object()
        .search(
            Some(path.to_string()),
            Some(bucket.to_string()),
            Some(0),
            Some(1),
        )
        .await?;
    if matches.is_empty() {
        return Ok(None);
    }
    let metadata = matches.remove(0);
    let (parent, _) = path.split();
    match parent {
        Some(parent) => Ok(Some(Object::try_from(metadata, &parent)?)),
        None => Err(anyhow!("unable to determine parent of object")),
    }
}

async fn write_file(
    renterd: &renterd_client::Client,
    parent: &Path,
    name: &str,
    bucket: &Bucket,
    metadata: Option<Vec<(String, String)>>,
    stream: impl AsyncRead + Send + Unpin + 'static,
    len: Option<u64>,
    overwrite_existing: bool,
) -> anyhow::Result<File> {
    let path = parent.try_join(name, false)?;
    let temp = parent.try_join(format!("{}.incomplete", name).as_str(), false)?;

    if !overwrite_existing && find_object(renterd, &path, bucket).await?.is_some() {
        bail!("{} already exists", path);
    }

    let _ = renterd
        .bus()
        .object()
        .delete(temp.as_ref(), Some(bucket.to_string()), false)
        .await;

    renterd
        .worker()
        .object()
        .upload(&temp, None, metadata, Some(bucket.to_string()), stream)
        .await?;

    if let Some(len) = len {
        match find_object(renterd, &temp, bucket).await? {
            Some(Object::File(file)) => {
                if file.size() != len {
                    bail!(
                        "file size mismatch, expected {} but found {}",
                        len,
                        file.size()
                    );
                }
            }
            _ => {
                bail!("unexpected object type");
            }
        }
    }

    let _ = renterd
        .bus()
        .object()
        .delete(path.as_ref(), Some(bucket.to_string()), false)
        .await;

    renterd
        .bus()
        .object()
        .rename(
            temp.to_string(),
            path.to_string(),
            bucket.to_string(),
            false,
            RenameMode::Single,
        )
        .await?;

    match find_object(renterd, &path, bucket).await? {
        Some(Object::File(file)) => Ok(file),
        Some(Object::Directory(_)) => Err(anyhow!("{} is a directory, expected a file", path)),
        None => Err(anyhow!("file {} does not exist", path)),
    }
}

async fn read_file(
    renterd: &renterd_client::Client,
    path: &Path,
    bucket: &Bucket,
    offset: u64,
    max_size: Option<u64>,
) -> anyhow::Result<impl AsyncRead + AsyncSeek + Send + Unpin + 'static> {
    let dl_object = renterd
        .worker()
        .object()
        .download(path.as_ref(), Some(bucket.to_string()))
        .await?
        .ok_or(anyhow!("renterd couldn't find the file"))?;

    if !dl_object.seekable {
        bail!("object is not seekable");
    }

    let len = dl_object.length.ok_or(anyhow!("file size is unknown"))?;

    if let Some(max_size) = max_size {
        if len > max_size {
            bail!(
                "file {} with file size {} exceeds maximum {}",
                path,
                len,
                max_size
            );
        }
    }

    Ok(dl_object.open_seekable_stream(offset).await?)
}

async fn read_volume_info(
    renterd: &renterd_client::Client,
    path: &Path,
    bucket: &Bucket,
) -> anyhow::Result<Bytes> {
    let mut reader = read_file(renterd, path, bucket, 0, Some(MAX_VOLUME_FILE_SIZE)).await?;
    let mut buf = BytesMut::with_capacity(MAX_VOLUME_FILE_SIZE as usize);
    reader.read_all_buffered(&mut buf).await?;
    Ok(buf.freeze())
}
