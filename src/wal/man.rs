use crate::io::TokioFile;
use crate::vbd::FixedSpecs;
use crate::wal::{WalId, WalReader, WalWriter};
use crate::Etag;
use std::path::{Path, PathBuf};
use tracing::instrument;
use uuid::Uuid;

pub(crate) struct WalMan {
    wal_dir: PathBuf,
    max_file_size: u64,
}

impl WalMan {
    #[instrument(skip_all)]
    pub fn new<P: AsRef<Path>>(wal_dir: P, max_file_size: u64) -> Self {
        assert!(max_file_size > 0);
        let wal_dir = wal_dir.as_ref();
        Self {
            wal_dir: wal_dir.to_path_buf(),
            max_file_size,
        }
    }

    pub(crate) async fn wal_files(&self) -> anyhow::Result<impl Iterator<Item = (WalId, Etag)>> {
        let mut dir = tokio::fs::read_dir(&self.wal_dir).await?;
        let mut wal_files = vec![];
        while let Some(entry) = dir.next_entry().await? {
            let wal_id = {
                let mut wal_id: Option<WalId> = None;
                let file_name = entry.file_name();
                if let Some(file_name) = file_name.to_str() {
                    if file_name.len() > 4 {
                        let (name, ext) = file_name.split_at(file_name.len() - 4);
                        if ext.eq_ignore_ascii_case(".wal") {
                            if let Ok(id) = name.try_into() {
                                wal_id = Some(id);
                            }
                        }
                    }
                }
                if let Some(wal_id) = wal_id {
                    wal_id
                } else {
                    continue;
                }
            };

            let metadata = entry.metadata().await?;
            let etag: Etag = (&metadata).try_into()?;
            wal_files.push((wal_id, etag));
        }
        Ok(wal_files.into_iter())
    }

    #[instrument(skip(self))]
    pub(crate) async fn open_reader(&self, wal_id: &WalId) -> anyhow::Result<WalReader> {
        let path = self.wal_dir.join(format!("{}.wal", wal_id));
        tracing::trace!(path = %path.display(), "opening wal reader");
        Ok(WalReader::new(TokioFile::open(&path).await?).await?)
    }

    #[instrument(skip_all)]
    pub(crate) async fn new_writer<T: Into<Option<WalId>>>(
        &self,
        preceding_wal_id: T,
        fixed_specs: FixedSpecs,
    ) -> anyhow::Result<WalWriter> {
        let wal_id: WalId = Uuid::now_v7().into();
        let path = self.wal_dir.join(format!("{}.wal", wal_id));
        tracing::debug!(path = %path.display(), "creating new wal writer");
        let file = TokioFile::create_new(self.wal_dir.join(format!("{}.wal", wal_id))).await?;
        let mut builder =
            WalWriter::builder(file, wal_id, fixed_specs).max_file_size(self.max_file_size);
        if let Some(wal_id) = preceding_wal_id.into() {
            builder = builder.preceding_wal_id(wal_id);
        }
        Ok(builder.build().await?)
    }

    pub(crate) async fn delete(&self, wal_id: &WalId) -> anyhow::Result<()> {
        let path = self.wal_dir.join(format!("{}.wal", wal_id));
        if tokio::fs::try_exists(&path).await? {
            tracing::info!(path = %path.display(), "deleting wal file");
            tokio::fs::remove_file(&path).await?;
        }
        Ok(())
    }
}
