use crate::hash::{Hash, HashAlgorithm};
use crate::repository::{BranchInfo, VolumeHandler};
use crate::vbd::{
    Block, BlockId, BlockSize, Cluster, ClusterId, ClusterMut, ClusterSize, Commit, FixedSpecs,
    Index, IndexId, IndexMut, VbdId,
};
use crate::wal::man::WalMan;
use crate::wal::{TxDetails, WalId};
use crate::{Etag, SqlitePool};
use anyhow::{anyhow, bail};
use chrono::DateTime;
use futures::{Stream, TryStreamExt};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, SqliteConnection};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::log::LevelFilter;
use tracing::{instrument, Instrument};

pub(crate) struct Inventory {
    specs: FixedSpecs,
    pool: SqlitePool,
    branch: String,
    current_commit: Commit,
    wal_man: Arc<WalMan>,
    volume: VolumeHandler,
}

enum Id<'a> {
    BlockId(OwnedOrBorrowed<'a, BlockId>),
    ClusterId(OwnedOrBorrowed<'a, ClusterId>),
    IndexId(OwnedOrBorrowed<'a, IndexId>),
}

impl<'a> Display for Id<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockId(id) => Display::fmt(id.deref(), f),
            Self::ClusterId(id) => Display::fmt(id.deref(), f),
            Self::IndexId(id) => Display::fmt(id.deref(), f),
        }
    }
}

impl<'a> Debug for Id<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockId(id) => Debug::fmt(id.deref(), f),
            Self::ClusterId(id) => Debug::fmt(id.deref(), f),
            Self::IndexId(id) => Debug::fmt(id.deref(), f),
        }
    }
}

impl<'a> Id<'a> {
    fn as_bytes(&'a self) -> &'a [u8] {
        match self {
            Self::BlockId(id) => id.as_ref(),
            Self::ClusterId(id) => id.as_ref(),
            Self::IndexId(id) => id.as_ref(),
        }
    }
}

enum OwnedOrBorrowed<'a, T> {
    Owned(T),
    Borrowed(&'a T),
}

impl<T> Deref for OwnedOrBorrowed<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match &self {
            Self::Owned(id) => id,
            Self::Borrowed(id) => *id,
        }
    }
}

impl From<BlockId> for Id<'static> {
    fn from(value: BlockId) -> Self {
        Self::BlockId(OwnedOrBorrowed::Owned(value))
    }
}

impl<'a> From<&'a BlockId> for Id<'a> {
    fn from(value: &'a BlockId) -> Self {
        Self::BlockId(OwnedOrBorrowed::Borrowed(value))
    }
}

impl From<ClusterId> for Id<'static> {
    fn from(value: ClusterId) -> Self {
        Self::ClusterId(OwnedOrBorrowed::Owned(value))
    }
}

impl<'a> From<&'a ClusterId> for Id<'a> {
    fn from(value: &'a ClusterId) -> Self {
        Self::ClusterId(OwnedOrBorrowed::Borrowed(value))
    }
}

impl From<IndexId> for Id<'static> {
    fn from(value: IndexId) -> Self {
        Self::IndexId(OwnedOrBorrowed::Owned(value))
    }
}

impl<'a> From<&'a IndexId> for Id<'a> {
    fn from(value: &'a IndexId) -> Self {
        Self::IndexId(OwnedOrBorrowed::Borrowed(value))
    }
}

impl Inventory {
    #[instrument(skip_all)]
    pub(super) async fn new(
        db_file: &Path,
        max_db_connections: u8,
        current_branch: impl AsRef<str>,
        wal_man: Arc<WalMan>,
        volume: VolumeHandler,
    ) -> Result<Self, anyhow::Error> {
        let current_branch = current_branch.as_ref().to_string();
        let specs = volume.volume_info().specs.clone();
        let branches = volume.list_branches().await?.collect::<HashMap<_, _>>();
        if !branches.contains_key(&current_branch) {
            bail!("branch {} not found", current_branch);
        }
        let pool = db_init(db_file, max_db_connections, &specs, &branches).await?;

        tracing::debug!("loading inventory");

        let remote_commit = branches
            .get(&current_branch)
            .map(|b| &b.commit)
            .unwrap()
            .clone();

        let local_commit = {
            let branch = current_branch.as_str();
            let r = sqlx::query!(
                "
                SELECT commit_id, preceding_commit_id, index_id, committed, num_clusters
                FROM commits WHERE branch = ?
                ",
                branch
            )
            .fetch_one(pool.read())
            .await?;
            Commit {
                content_id: Hash::try_from((r.commit_id.as_slice(), specs.meta_hash()))?.into(),
                preceding_commit: Hash::try_from((
                    r.preceding_commit_id.as_slice(),
                    specs.meta_hash(),
                ))?
                .into(),
                index: Hash::try_from((r.index_id.as_slice(), specs.meta_hash()))?.into(),
                committed: DateTime::from_timestamp_millis(r.committed)
                    .ok_or(anyhow!("invalid timestamp"))?,
                num_clusters: r.num_clusters as usize,
            }
        };

        let commit = if remote_commit > local_commit {
            tracing::info!(remote = %remote_commit.content_id(), local = %local_commit.content_id(), "remote branch is further ahead, updating local branch");
            let mut tx = pool.write().begin().await?;
            update_commit(&current_branch, &remote_commit, tx.as_mut()).await?;
            tx.commit().await?;
            remote_commit
        } else {
            local_commit
        };

        let mut this = Self {
            specs,
            pool,
            branch: current_branch,
            current_commit: commit,
            wal_man,
            volume,
        };

        this.sync_wal_files().await?;

        this.current_commit = this.current_commit.clone();

        tracing::debug!("inventory loaded");

        Ok(this)
    }

    pub fn specs(&self) -> &FixedSpecs {
        &self.specs
    }

    pub fn current_commit(&self) -> &Commit {
        &self.current_commit
    }

    pub fn branch(&self) -> &String {
        &self.branch
    }

    #[instrument[skip(self)]]
    pub async fn block_by_id(&self, block_id: &BlockId) -> anyhow::Result<Option<Block>> {
        if block_id == self.specs().zero_block().content_id() {
            return Ok(Some(self.specs().zero_block().clone()));
        }

        // check the local WALs
        if let Ok(Some(block)) = self.block_from_wal(block_id).await {
            return Ok(Some(block));
        }

        Ok(None)
    }

    #[instrument[skip(self)]]
    pub async fn cluster_by_id(&self, cluster_id: &ClusterId) -> anyhow::Result<Option<Cluster>> {
        if cluster_id == self.specs().zero_cluster().content_id() {
            return Ok(Some(self.specs().zero_cluster().clone()));
        }

        // try to load directly from database
        {
            let mut conn = self.pool.read().acquire().await?;
            if let Ok(Some(cluster)) = self.cluster_from_db(cluster_id, conn.as_mut()).await {
                return Ok(Some(cluster));
            }
        }

        // check the local WALs
        if let Ok(Some(cluster)) = self.cluster_from_wal(cluster_id).await {
            return Ok(Some(cluster));
        }

        Ok(None)
    }

    #[instrument[skip(self)]]
    pub async fn index_by_id(&self, index_id: &IndexId) -> anyhow::Result<Option<Index>> {
        for index in self.specs.zero_indices() {
            if index.content_id() == index_id {
                return Ok(Some(index.clone()));
            }
        }

        // try to load directly from database
        {
            let mut conn = self.pool.read().acquire().await?;
            if let Ok(Some(index)) = self.index_from_db(index_id, conn.as_mut()).await {
                return Ok(Some(index));
            }
        }

        // check the local WALs
        if let Ok(Some(index)) = self.index_from_wal(index_id).await {
            return Ok(Some(index));
        }

        Ok(None)
    }

    #[instrument[skip(self)]]
    async fn index_from_wal(&self, index_id: &IndexId) -> anyhow::Result<Option<Index>> {
        tracing::debug!("reading index from wal");
        for (wal_id, offsets) in {
            let mut conn = self.pool.read().acquire().await?;
            self.wal_offsets_for_id(index_id, conn.as_mut()).await?
        } {
            let mut wal_reader = match self.wal_man.open_reader(&wal_id).await {
                Ok(wal_reader) => wal_reader,
                Err(err) => {
                    tracing::error!(error = %err, wal_id = %wal_id, "opening wal reader failed");
                    continue;
                }
            };
            for offset in offsets {
                match wal_reader.index(index_id, offset).await {
                    Ok(index) => return Ok(Some(index)),
                    Err(err) => {
                        tracing::error!(error = %err, wal_id = %wal_id, "reading index failed");
                    }
                }
            }
        }
        Ok(None)
    }

    #[instrument[skip(self)]]
    async fn cluster_from_wal(&self, cluster_id: &ClusterId) -> anyhow::Result<Option<Cluster>> {
        tracing::debug!("reading cluster from wal");
        for (wal_id, offsets) in {
            let mut conn = self.pool.read().acquire().await?;
            self.wal_offsets_for_id(cluster_id, conn.as_mut()).await?
        } {
            let mut wal_reader = match self.wal_man.open_reader(&wal_id).await {
                Ok(wal_reader) => wal_reader,
                Err(err) => {
                    tracing::error!(error = %err, wal_id = %wal_id, "opening wal reader failed");
                    continue;
                }
            };
            for offset in offsets {
                match wal_reader.cluster(cluster_id, offset).await {
                    Ok(cluster) => return Ok(Some(cluster)),
                    Err(err) => {
                        tracing::error!(error = %err, wal_id = %wal_id, "reading cluster failed");
                    }
                }
            }
        }
        Ok(None)
    }

    #[instrument[skip(self)]]
    async fn block_from_wal(&self, block_id: &BlockId) -> anyhow::Result<Option<Block>> {
        for (wal_id, offsets) in {
            let mut conn = self.pool.read().acquire().await?;
            self.wal_offsets_for_id(block_id, conn.as_mut()).await?
        } {
            let mut wal_reader = match self.wal_man.open_reader(&wal_id).await {
                Ok(wal_reader) => wal_reader,
                Err(err) => {
                    tracing::error!(error = %err, wal_id = %wal_id, "opening wal reader failed");
                    continue;
                }
            };
            for offset in offsets {
                match wal_reader.block(block_id, offset).await {
                    Ok(block) => return Ok(Some(block)),
                    Err(err) => {
                        tracing::error!(error = %err, wal_id = %wal_id, "reading block failed");
                    }
                }
            }
        }
        Ok(None)
    }

    async fn wal_offsets_for_id(
        &self,
        id: impl Into<Id<'_>>,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<impl Iterator<Item = (WalId, Vec<u64>)>> {
        #[derive(Debug)]
        struct Row {
            wal_id: Vec<u8>,
            file_offset: i64,
        }

        fn try_convert(row: Row) -> anyhow::Result<(WalId, u64)> {
            Ok(WalId::try_from(row.wal_id.as_slice()).map(|w| (w, row.file_offset as u64))?)
        }

        let id = id.into();
        let id_slice = id.as_bytes();
        let mut stream = match id {
            Id::BlockId(_) => {
                sqlx::query_as!(
                    Row,
                    "
                    SELECT wal_id, file_offset FROM wal_content
                    WHERE content_type = 'B' AND block_id = ? AND index_id IS NULL AND cluster_id IS NULL
                    ",
                    id_slice
                ).fetch(conn)
            }
            Id::ClusterId(_) => {
                sqlx::query_as!(
                    Row,
                    "
                    SELECT wal_id, file_offset FROM wal_content
                    WHERE content_type = 'C' AND cluster_id = ? AND block_id IS NULL AND index_id IS NULL
                    ",
                    id_slice
                ).fetch(conn)
            }
            Id::IndexId(_) => {
                sqlx::query_as!(
                    Row,
                    "
                    SELECT wal_id, file_offset FROM wal_content
                    WHERE content_type = 'I' AND index_id = ? AND block_id IS NULL AND cluster_id IS NULL
                    ",
                    id_slice
                ).fetch(conn)
            }
        };

        let mut matches = HashMap::new();

        while let Some((wal_id, offset)) = stream
            .try_next()
            .await?
            .map(|r| try_convert(r))
            .transpose()?
        {
            if !matches.contains_key(&wal_id) {
                matches.insert(wal_id.clone(), Vec::default());
            }
            matches.get_mut(&wal_id).unwrap().push(offset);
        }

        Ok(matches.into_iter())
    }

    #[instrument[skip(self, conn)]]
    async fn index_from_db(
        &self,
        index_id: &IndexId,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<Option<Index>> {
        let cluster_ids = {
            let index_id = index_id.as_ref();
            sqlx::query!(
                "
                SELECT cluster_index, cluster_id FROM index_content WHERE index_id = ?;
                ",
                index_id
            )
            .fetch(conn)
            .map_err(|e| anyhow::Error::from(e))
            .try_filter_map(|r| async move {
                let idx = r.cluster_index as usize;
                Hash::try_from((r.cluster_id.as_slice(), self.specs.meta_hash()))
                    .map(|h| Some((idx, h.into())))
            })
            .try_collect::<Vec<(usize, ClusterId)>>()
            .await?
        };

        if cluster_ids.is_empty() {
            return Ok(None);
        }

        let mut index = IndexMut::from_index(
            self.specs().zero_index(cluster_ids.len()),
            self.specs.clone(),
        );
        for (idx, cluster_id) in cluster_ids {
            if idx >= index.clusters().len() {
                return Err(anyhow!("database entry for index [{}] invalid", index_id));
            }
            index.clusters()[idx] = cluster_id
        }
        let index = index.finalize();
        if index.content_id() == index_id {
            Ok(Some(index))
        } else {
            Err(anyhow!("database entry for index [{}] invalid", index_id))
        }
    }

    #[instrument[skip(self, conn)]]
    async fn cluster_from_db(
        &self,
        cluster_id: &ClusterId,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<Option<Cluster>> {
        let block_ids = {
            let cluster_id = cluster_id.as_ref();
            sqlx::query!(
                "
                SELECT block_index, block_id FROM cluster_content WHERE cluster_id = ?;
                ",
                cluster_id
            )
            .fetch(conn)
            .map_err(|e| anyhow::Error::from(e))
            .try_filter_map(|r| async move {
                let idx = r.block_index as usize;
                Hash::try_from((r.block_id.as_slice(), self.specs.meta_hash()))
                    .map(|h| Some((idx, h.into())))
            })
            .try_collect::<Vec<(usize, BlockId)>>()
            .await?
        };

        if block_ids.is_empty() {
            return Ok(None);
        }

        let mut cluster =
            ClusterMut::from_cluster(self.specs().zero_cluster().clone(), self.specs.clone());
        for (idx, block_id) in block_ids {
            if idx >= cluster.blocks().len() {
                return Err(anyhow!(
                    "database entry for cluster [{}] invalid",
                    cluster_id
                ));
            }
            cluster.blocks()[idx] = block_id
        }
        let cluster = cluster.finalize();
        if cluster.content_id() == cluster_id {
            Ok(Some(cluster))
        } else {
            Err(anyhow!(
                "database entry for cluster [{}] invalid",
                cluster_id
            ))
        }
    }

    #[instrument[skip(tx), fields(index = %index.content_id())]]
    async fn sync_index_content(
        index: &Index,
        zero_cluster_id: &ClusterId,
        tx: &mut SqliteConnection,
    ) -> anyhow::Result<u64> {
        let mut rows_affected = 0;
        let index_id = index.content_id().as_ref();
        let num_clusters = {
            let r = sqlx::query!(
                "
                SELECT COUNT(*) AS count FROM index_content WHERE index_id = ?;
                ",
                index_id
            )
            .fetch_one(&mut *tx)
            .await?;
            r.count as usize
        };
        if num_clusters == index.len() {
            // already synced
            return Ok(rows_affected);
        };

        if num_clusters > 0 {
            // invalid, delete
            rows_affected += sqlx::query!(
                "
                DELETE FROM index_content WHERE index_id = ?;
                ",
                index_id
            )
            .execute(&mut *tx)
            .await?
            .rows_affected();
        }

        let mut idx = 0;
        for cluster_id in index.cluster_ids() {
            if cluster_id != zero_cluster_id {
                let cluster_id = cluster_id.as_ref();
                rows_affected += sqlx::query!(
                    "
                    INSERT INTO index_content (index_id, cluster_index, cluster_id)
                    VALUES (?, ?, ?)
                    ",
                    index_id,
                    idx,
                    cluster_id,
                )
                .execute(&mut *tx)
                .await?
                .rows_affected();
            }
            idx += 1;
        }

        Ok(rows_affected)
    }

    #[instrument[skip(tx), fields(cluster = %cluster.content_id())]]
    async fn sync_cluster_content(
        cluster: &Cluster,
        zero_block_id: &BlockId,
        tx: &mut SqliteConnection,
    ) -> anyhow::Result<u64> {
        let mut rows_affected = 0;
        let num_blocks = {
            let cluster_id = cluster.content_id().as_ref();
            let r = sqlx::query!(
                "
                SELECT COUNT(*) AS count FROM cluster_content WHERE cluster_id = ?;
                ",
                cluster_id
            )
            .fetch_one(&mut *tx)
            .await?;
            r.count as usize
        };
        if num_blocks == cluster.len() {
            // already synced
            return Ok(rows_affected);
        };
        let cluster_id = cluster.content_id().as_ref();
        if num_blocks > 0 {
            // invalid, delete
            rows_affected += sqlx::query!(
                "
                DELETE FROM cluster_content WHERE cluster_id = ?;
                ",
                cluster_id
            )
            .execute(&mut *tx)
            .await?
            .rows_affected();
        }

        let mut idx = 0;
        for block_id in cluster.block_ids() {
            if block_id != zero_block_id {
                let block_id = block_id.as_ref();
                rows_affected += sqlx::query!(
                    "
                    INSERT INTO cluster_content (cluster_id, block_index, block_id)
                    VALUES (?, ?, ?)
                    ",
                    cluster_id,
                    idx,
                    block_id,
                )
                .execute(&mut *tx)
                .await?
                .rows_affected();
            }
            idx += 1;
        }

        Ok(rows_affected)
    }

    fn unused<'a>(
        tx: &'a mut SqliteConnection,
        specs: &'a FixedSpecs,
        zero_block_id: &'a BlockId,
        zero_cluster_id: &'a ClusterId,
        zero_index_ids: &'a Vec<IndexId>,
    ) -> impl Stream<Item = Result<Id<'static>, anyhow::Error>> + use<'a> {
        sqlx::query!(
            "
            SELECT block_id, NULL AS cluster_id, NULL AS index_id FROM known_blocks
                WHERE used = 0 AND available > 0
                UNION ALL
            SELECT NULL, cluster_id, NULL FROM known_clusters
                WHERE used = 0 AND available > 0
                UNION ALL
            SELECT NULL, NULL, index_id FROM known_indices
                WHERE used = 0 AND available > 0;
            "
        )
        .fetch(tx)
        .map_err(|e| anyhow::Error::from(e))
        .try_filter_map(move |r| async move {
            if let Some(block_id) = r.block_id {
                Hash::try_from((block_id.as_slice(), specs.content_hash()))
                    .map(|h| h.into())
                    .map(|b: BlockId| {
                        if &b == zero_block_id {
                            None
                        } else {
                            Some(b.into())
                        }
                    })
            } else if let Some(cluster_id) = r.cluster_id {
                Hash::try_from((cluster_id.as_slice(), specs.meta_hash()))
                    .map(|h| h.into())
                    .map(|c: ClusterId| {
                        if &c == zero_cluster_id {
                            None
                        } else {
                            Some(c.into())
                        }
                    })
            } else if let Some(index_id) = r.index_id {
                Hash::try_from((index_id.as_slice(), specs.meta_hash()))
                    .map(|h| h.into())
                    .map(|c: IndexId| {
                        if zero_index_ids.contains(&c) {
                            None
                        } else {
                            Some(c.into())
                        }
                    })
            } else {
                Err(anyhow!("invalid row"))
            }
        })
    }

    #[instrument[skip(tx, tx_details)]]
    async fn process_tx_details(
        tx_details: &TxDetails,
        tx: &mut SqliteConnection,
    ) -> anyhow::Result<u64> {
        let mut rows_affected = 0;
        let items = tx_details
            .blocks
            .iter()
            .map(|(b, offset)| (Id::from(b), *offset))
            .chain(
                tx_details
                    .clusters
                    .iter()
                    .map(|(c, offset)| (c.into(), *offset)),
            )
            .chain(
                tx_details
                    .indices
                    .iter()
                    .map(|(c, offset)| (c.into(), *offset)),
            );

        let wal_id = tx_details.wal_id.as_bytes().as_slice();

        for (id, offset) in items.into_iter() {
            let file_offset = offset as i64;
            let id_bytes = id.as_bytes();
            let (content_type, block_id, cluster_id, index_id) = match &id {
                Id::BlockId(_) => ("B", Some(id_bytes), None, None),
                Id::ClusterId(_) => ("C", None, Some(id_bytes), None),
                Id::IndexId(_) => ("I", None, None, Some(id_bytes)),
            };
            rows_affected += sqlx::query!(
            "
                    INSERT INTO wal_content (wal_id, file_offset, content_type, block_id, cluster_id, index_id)
                    VALUES (?, ?, ?, ?, ?, ?);
                    ",
            wal_id,
            file_offset,
            content_type,
            block_id,
            cluster_id,
            index_id
            )
                .execute(&mut *tx)
                .await?.rows_affected();
        }
        Ok(rows_affected)
    }

    #[instrument[skip_all]]
    pub async fn sync_wal_files(&mut self) -> anyhow::Result<()> {
        tracing::info!("syncing wal files");
        let wal_files = self.wal_man.wal_files().await?;

        let mut known_wal_files: HashMap<WalId, Etag> = HashMap::default();
        let mut tx = self.pool.write().begin().await?;
        for r in sqlx::query!("SELECT id, etag FROM wal_files")
            .fetch_all(tx.as_mut())
            .await?
            .into_iter()
        {
            let wal_id = match WalId::try_from(r.id.as_slice()) {
                Ok(wal_id) => wal_id,
                Err(err) => {
                    tracing::error!(error = %err, wal_id = ?r.id, "invalid wal_id found in database, removing");
                    sqlx::query!("DELETE FROM wal_files WHERE id = ?", r.id)
                        .execute(tx.as_mut())
                        .await?;
                    continue;
                }
            };
            known_wal_files.insert(wal_id, Etag::from(r.etag));
        }

        for (wal_id, etag) in wal_files {
            let mut in_sync = false;
            if let Some(known_etag) = known_wal_files.remove(&wal_id) {
                if &known_etag == &etag {
                    in_sync = true;
                }
            }
            if !in_sync {
                tracing::info!(wal_id = %wal_id, "wal file needs syncing");
                self.sync_wal_file(&wal_id, &etag, tx.as_mut()).await?;
            }
        }

        for obsolete in known_wal_files.into_keys() {
            tracing::debug!(
                wal_id = %obsolete,
                "removing obsolete wal file from inventory",
            );
            let id = obsolete.as_bytes().as_slice();
            sqlx::query!("DELETE FROM wal_files WHERE id = ?", id)
                .execute(tx.as_mut())
                .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    #[instrument(skip(self, tx), fields(wal_id = %wal_id))]
    async fn sync_wal_file(
        &mut self,
        wal_id: &WalId,
        etag: &Etag,
        tx: &mut SqliteConnection,
    ) -> anyhow::Result<u64> {
        tracing::debug!("syncing wal file");
        let mut wal_reader = self.wal_man.open_reader(wal_id).await?;

        let id = wal_id.as_bytes().as_slice();
        let etag = etag.as_ref();

        let rows_affected = sqlx::query!("DELETE FROM wal_files WHERE id = ?", id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        tracing::trace!(
            rows_affected,
            "deleted wal_file related entries from database"
        );
        let mut rows_affected = 0;
        rows_affected += sqlx::query!(
            "INSERT INTO wal_files (id, etag)
             VALUES (?, ?)
            ",
            id,
            etag
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();

        let mut clusters = HashMap::new();
        let mut indices = HashMap::new();

        {
            let mut stream = wal_reader.transactions(None).await?;
            while let Some(tx_details) = stream.try_next().await? {
                rows_affected += Self::process_tx_details(&tx_details, &mut *tx).await?;
                tx_details.clusters.into_iter().for_each(|(id, offset)| {
                    clusters.insert(id, offset);
                });
                tx_details.indices.into_iter().for_each(|(id, offset)| {
                    indices.insert(id, offset);
                });
            }
        }

        let zero_block_id = self.specs().zero_block().content_id().clone();
        let zero_cluster_id = self.specs().zero_cluster().content_id().clone();
        let zero_index_ids = {
            self.specs()
                .zero_indices()
                .map(|c| c.content_id().clone())
                .collect::<Vec<_>>()
        };

        // find any unused content that has been in this wal file
        for (id, offset) in Self::unused(
            &mut *tx,
            &self.specs,
            &zero_block_id,
            &zero_cluster_id,
            &zero_index_ids,
        )
        .try_filter_map(|id| {
            let clusters = &clusters;
            let indices = &indices;
            async move {
                Ok(match &id {
                    Id::BlockId(_) => None,
                    Id::ClusterId(cluster_id) => {
                        if let Some(offset) = clusters.get(cluster_id).map(|c| *c) {
                            Some((id, offset))
                        } else {
                            None
                        }
                    }
                    Id::IndexId(index_id) => {
                        if let Some(offset) = indices.get(index_id).map(|c| *c) {
                            Some((id, offset))
                        } else {
                            None
                        }
                    }
                })
            }
        })
        .try_collect::<Vec<_>>()
        .await?
        {
            match id {
                Id::BlockId(_) => {}
                Id::ClusterId(cluster_id) => {
                    let cluster = wal_reader.cluster(&cluster_id, offset).await?;
                    rows_affected +=
                        Self::sync_cluster_content(&cluster, &zero_block_id, &mut *tx).await?;
                }
                Id::IndexId(index_id) => {
                    let index = wal_reader.index(&index_id, offset).await?;
                    rows_affected +=
                        Self::sync_index_content(&index, &zero_cluster_id, &mut *tx).await?;
                }
            }
        }
        tracing::debug!(rows_affected, "wal file sync complete");
        Ok(rows_affected)
    }

    #[instrument[skip(self, tx_details), fields(wal_id = %wal_id)]]
    pub async fn update_wal(
        &mut self,
        tx_details: &TxDetails,
        wal_id: &WalId,
    ) -> anyhow::Result<()> {
        {
            let mut wal_reader = self.wal_man.open_reader(wal_id).await?;
            let id = tx_details.wal_id.as_bytes().as_slice();
            let etag = wal_reader.as_ref().etag().await?;
            let etag = etag.as_ref();
            let mut tx = self.pool.writer.begin().await?;
            sqlx::query!(
                "
                INSERT INTO wal_files (id, etag)
                VALUES (?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    etag = excluded.etag;
                ",
                id,
                etag,
            )
            .execute(tx.as_mut())
            .await?;

            Self::process_tx_details(tx_details, tx.as_mut()).await?;

            let zero_block_id = self.specs().zero_block().content_id().clone();
            let zero_cluster_id = self.specs().zero_cluster().content_id().clone();
            let zero_index_ids = {
                self.specs()
                    .zero_indices()
                    .map(|c| c.content_id().clone())
                    .collect::<Vec<_>>()
            };

            // find any unused content that has been in this wal update
            for id in Self::unused(
                tx.as_mut(),
                &self.specs,
                &zero_block_id,
                &zero_cluster_id,
                &zero_index_ids,
            )
            .try_filter_map(move |id| async move {
                Ok(
                    if match &id {
                        Id::BlockId(block_id) => tx_details.blocks.contains_key(block_id),
                        Id::ClusterId(cluster_id) => tx_details.clusters.contains_key(cluster_id),
                        Id::IndexId(index_id) => tx_details.indices.contains_key(index_id),
                    } {
                        Some(id)
                    } else {
                        None
                    },
                )
            })
            .try_collect::<Vec<_>>()
            .await?
            {
                match id {
                    Id::BlockId(_) => {}
                    Id::ClusterId(cluster_id) => {
                        if let Some(offset) = tx_details.clusters.get(&cluster_id) {
                            let cluster = wal_reader.cluster(&cluster_id, *offset).await?;
                            Self::sync_cluster_content(&cluster, &zero_block_id, tx.as_mut())
                                .await?;
                        }
                    }
                    Id::IndexId(index_id) => {
                        if let Some(offset) = tx_details.indices.get(&index_id) {
                            let index = wal_reader.index(&index_id, *offset).await?;
                            Self::sync_index_content(&index, &zero_cluster_id, tx.as_mut()).await?;
                        }
                    }
                }
            }

            update_commit(self.branch(), &tx_details.commit, tx.as_mut()).await?;

            tx.commit().await?;

            self.current_commit = tx_details.commit.clone();
            Ok(())
        }
    }
}

async fn update_commit<S: AsRef<str>>(
    branch: S,
    commit: &Commit,
    tx: &mut SqliteConnection,
) -> anyhow::Result<()> {
    let commit_id = commit.content_id().as_ref();
    let preceding_commit_id = commit.preceding_commit().as_ref();
    let index_id = commit.index().as_ref();
    let commited = commit.committed().timestamp_micros();
    let num_clusters = commit.num_clusters() as i64;
    let branch = branch.as_ref();
    sqlx::query!(
        "
        UPDATE commits SET
        commit_id = ?, preceding_commit_id = ?, index_id = ?, committed = ?, num_clusters = ?
        WHERE branch = ?
        ",
        commit_id,
        preceding_commit_id,
        index_id,
        commited,
        num_clusters,
        branch
    )
    .execute(&mut *tx)
    .await?;
    Ok(())
}

#[instrument[skip_all, fields(db_file = %db_file.display())]]
async fn db_init(
    db_file: &Path,
    max_connections: u8,
    specs: &FixedSpecs,
    branches: &HashMap<String, BranchInfo>,
) -> anyhow::Result<SqlitePool> {
    let writer = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with({
            SqliteConnectOptions::new()
                .create_if_missing(true)
                .filename(db_file)
                .log_statements(LevelFilter::Trace)
                .journal_mode(SqliteJournalMode::Wal)
                .foreign_keys(true)
                .pragma("recursive_triggers", "ON")
                .busy_timeout(Duration::from_millis(100))
                .shared_cache(true)
        })
        .await?;

    async { sqlx::migrate!("./migrations").run(&writer).await }
        .instrument(tracing::warn_span!("db_migration"))
        .await?;

    let mut tx = writer.begin().await?;

    // check config

    let vbd_id = specs.vbd_id();
    let vbd_id = vbd_id.as_bytes().as_slice();
    let cluster_size = *specs.cluster_size() as i64;
    let block_size = *specs.block_size() as i64;
    let content_hash = to_db_hash(specs.content_hash());
    let meta_hash = to_db_hash(specs.meta_hash());
    sqlx::query!(
        "
        INSERT INTO config (vbd_id, cluster_size, block_size, content_hash, meta_hash)
        SELECT ?, ?, ?, ?, ?
        WHERE NOT EXISTS (SELECT 1 FROM config);
        ",
        vbd_id,
        cluster_size,
        block_size,
        content_hash,
        meta_hash
    )
    .execute(tx.as_mut())
    .await?;

    let r = sqlx::query!(
        "
        SELECT vbd_id, cluster_size, block_size, content_hash, meta_hash
        FROM config;
        ",
    )
    .fetch_one(tx.as_mut())
    .await?;

    let vbd_id: VbdId = r.vbd_id.as_slice().try_into()?;
    let cluster_size: ClusterSize = (r.cluster_size as usize).try_into()?;
    let block_size: BlockSize = (r.block_size as usize).try_into()?;
    let content_hash: HashAlgorithm = try_from_db_hash(r.content_hash)?;
    let meta_hash: HashAlgorithm = try_from_db_hash(r.meta_hash)?;

    if &specs.vbd_id() != &vbd_id {
        bail!("vbd_id mismatch");
    }
    if &specs.cluster_size() != &cluster_size {
        bail!("cluster_size mismatch");
    }
    if &specs.block_size() != &block_size {
        bail!("block_size mismatch");
    }
    if &specs.content_hash() != &content_hash {
        bail!("content_hash mismatch");
    }
    if &specs.meta_hash() != &meta_hash {
        bail!("meta_hash mismatch");
    }

    let zero_block = specs.zero_block();
    let block_id = zero_block.content_id().as_ref();
    sqlx::query!(
        "
        INSERT INTO known_blocks (block_id, used, available)
        VALUES (?, 0, 1)
        ON CONFLICT(block_id) DO UPDATE SET available = 1;
        ",
        block_id,
    )
    .execute(tx.as_mut())
    .await?;

    let zero_cluster = specs.zero_cluster();
    let cluster_id = zero_cluster.content_id().as_ref();
    sqlx::query!(
        "
        INSERT INTO known_clusters (cluster_id, used, available)
        VALUES (?, 0, 1)
        ON CONFLICT(cluster_id) DO UPDATE SET available = 1;
                ",
        cluster_id
    )
    .execute(tx.as_mut())
    .await?;

    for num_clusters in branches.values().into_iter().map(|b| b.commit.num_clusters) {
        let zero_index = specs.zero_index(num_clusters);
        let index_id = zero_index.content_id().as_ref();
        sqlx::query!(
            "
            INSERT INTO known_indices (index_id, used, available)
            VALUES (?, 0, 1)
            ON CONFLICT(index_id) DO UPDATE SET available = 1;
            ",
            index_id
        )
        .execute(tx.as_mut())
        .await?;
    }

    let to_delete = sqlx::query!("SELECT branch FROM commits;")
        .map(|r| r.branch)
        .fetch_all(tx.as_mut())
        .await?
        .into_iter()
        .filter(|b| !branches.contains_key(b))
        .collect::<Vec<_>>();

    for branch in to_delete {
        sqlx::query!("DELETE FROM commits WHERE branch = ?;", branch)
            .execute(tx.as_mut())
            .await?;
    }

    for (branch, commit) in branches.iter().map(|(s, b)| (s, &b.commit)) {
        let commit_id = commit.content_id().as_ref();
        let preceding_commit_id = commit.preceding_commit().as_ref();
        let index_id = commit.index().as_ref();
        let committed = commit.committed().timestamp_micros();
        let num_clusters = commit.num_clusters() as i64;
        sqlx::query!(
            "
            INSERT INTO commits (branch, commit_id, preceding_commit_id, index_id, committed, num_clusters)
            SELECT ?, ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM commits WHERE branch = ?
            )
            ",
            branch,
            commit_id,
            preceding_commit_id,
            index_id,
            committed,
            num_clusters,
            branch
    )
            .execute(tx.as_mut())
            .await?;
    }

    tx.commit().await?;

    let reader = SqlitePoolOptions::new()
        .max_connections(max_connections as u32)
        .connect_with({
            SqliteConnectOptions::new()
                .create_if_missing(false)
                .filename(db_file)
                .log_statements(LevelFilter::Trace)
                .journal_mode(SqliteJournalMode::Wal)
                .foreign_keys(true)
                .pragma("recursive_triggers", "ON")
                .busy_timeout(Duration::from_millis(100))
                .shared_cache(true)
                .pragma("query_only", "ON")
        })
        .await?;

    Ok(SqlitePool { writer, reader })
}

fn to_db_hash(value: HashAlgorithm) -> &'static str {
    match value {
        HashAlgorithm::Tent => "TENT",
        HashAlgorithm::Blake3 => "BLAKE3",
        HashAlgorithm::XXH3 => "XXH3_128",
    }
}

fn try_from_db_hash(value: String) -> anyhow::Result<HashAlgorithm> {
    match value.as_str() {
        "TENT" => Ok(HashAlgorithm::Tent),
        "BLAKE3" => Ok(HashAlgorithm::Blake3),
        "XXH3_128" => Ok(HashAlgorithm::XXH3),
        _ => bail!("invalid hash algorithm value: {}", value),
    }
}
