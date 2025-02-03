use crate::vbd::WalReader;
use crate::wal::{TokioWalFile, WalId};
use crate::Etag;
use std::path::{Path, PathBuf};
use tracing::instrument;

pub(crate) struct WalMan {
    wal_dir: PathBuf,
}

impl WalMan {
    #[instrument(skip_all)]
    pub fn new<P: AsRef<Path>>(wal_dir: P) -> Self {
        let wal_dir = wal_dir.as_ref();
        Self {
            wal_dir: wal_dir.to_path_buf(),
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
        tracing::debug!(path = %path.display(), "opening wal reader");
        Ok(WalReader::new(TokioWalFile::open(&path).await?).await?)
    }
}
