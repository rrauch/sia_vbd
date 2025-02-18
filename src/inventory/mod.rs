pub(crate) mod chunk;
mod syncer;

use crate::hash::{Hash, HashAlgorithm};
use crate::inventory::chunk::{Chunk, ChunkEntry, ChunkId, Manifest, ManifestId};
use crate::inventory::syncer::Syncer;
use crate::repository::{BranchInfo, VolumeHandler};
use crate::vbd::{
    Block, BlockId, BlockSize, BranchName, Cluster, ClusterId, ClusterMut, ClusterSize, Commit,
    FixedSpecs, Snapshot, SnapshotId, SnapshotMut, VbdId,
};
use crate::wal::man::WalMan;
use crate::wal::{TxDetails, WalId};
use crate::{Etag, SqlitePool};
use anyhow::{anyhow, bail};
use async_tempfile::TempDir;
use chrono::DateTime;
use futures::TryStreamExt;
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
use uuid::Uuid;

pub(crate) struct Inventory {
    specs: FixedSpecs,
    pool: SqlitePool,
    branch: BranchName,
    current_commit: Commit,
    wal_man: Arc<WalMan>,
    volume: Arc<VolumeHandler>,
    syncer: Option<Syncer>,
}

enum Id<'a> {
    BlockId(OwnedOrBorrowed<'a, BlockId>),
    ClusterId(OwnedOrBorrowed<'a, ClusterId>),
    SnapshotId(OwnedOrBorrowed<'a, SnapshotId>),
}

impl<'a> Display for Id<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockId(id) => Display::fmt(id.deref(), f),
            Self::ClusterId(id) => Display::fmt(id.deref(), f),
            Self::SnapshotId(id) => Display::fmt(id.deref(), f),
        }
    }
}

impl<'a> Debug for Id<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockId(id) => Debug::fmt(id.deref(), f),
            Self::ClusterId(id) => Debug::fmt(id.deref(), f),
            Self::SnapshotId(id) => Debug::fmt(id.deref(), f),
        }
    }
}

impl<'a> Id<'a> {
    fn as_bytes(&'a self) -> &'a [u8] {
        match self {
            Self::BlockId(id) => id.as_ref(),
            Self::ClusterId(id) => id.as_ref(),
            Self::SnapshotId(id) => id.as_ref(),
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

impl From<SnapshotId> for Id<'static> {
    fn from(value: SnapshotId) -> Self {
        Self::SnapshotId(OwnedOrBorrowed::Owned(value))
    }
}

impl<'a> From<&'a SnapshotId> for Id<'a> {
    fn from(value: &'a SnapshotId) -> Self {
        Self::SnapshotId(OwnedOrBorrowed::Borrowed(value))
    }
}

impl Inventory {
    #[instrument(skip_all)]
    pub(super) async fn new(
        db_file: &Path,
        max_db_connections: u8,
        max_chunk_size: u64,
        current_branch: BranchName,
        wal_man: Arc<WalMan>,
        volume: VolumeHandler,
        initial_sync_delay: Duration,
        sync_interval: Duration,
    ) -> Result<Self, anyhow::Error> {
        let temp_dir = TempDir::new_with_uuid(Uuid::now_v7()).await?;

        let volume = Arc::new(volume);
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

        let local_commit =
            commit_from_db(&current_branch, &specs, &mut *pool.read().acquire().await?).await?;

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
            syncer: None,
        };

        this.sync_chunks().await?;
        this.sync_manifests().await?;
        this.sync_unindexed_chunk_content().await?;
        this.sync_wal_files().await?;
        this.sync_commits().await?;
        this.syncer = Some(Syncer::new(
            this.pool.clone(),
            this.wal_man.clone(),
            this.volume.clone(),
            this.specs.clone(),
            initial_sync_delay,
            sync_interval,
            this.branch.clone(),
            temp_dir,
            max_chunk_size,
        ));

        tracing::debug!("inventory loaded");

        Ok(this)
    }

    pub async fn close(mut self) -> anyhow::Result<()> {
        if let Some(syncer) = self.syncer.take() {
            syncer.close().await?;
        }
        Ok(())
    }

    pub fn specs(&self) -> &FixedSpecs {
        &self.specs
    }

    pub fn current_commit(&self) -> &Commit {
        &self.current_commit
    }

    pub fn branch(&self) -> &BranchName {
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

        // get from chunk
        if let Ok(Some(block)) = self.block_from_chunk(block_id).await {
            return Ok(Some(block));
        }

        Ok(None)
    }

    #[instrument[skip(self)]]
    pub async fn cluster_by_id(&self, cluster_id: &ClusterId) -> anyhow::Result<Option<Cluster>> {
        let mut conn = self.pool.read().acquire().await?;
        let res = Self::_cluster_by_id(
            &self.specs,
            &self.wal_man,
            &self.volume,
            cluster_id,
            conn.as_mut(),
        )
        .await?;
        conn.close().await?;
        Ok(res)
    }

    #[instrument[skip_all]]
    async fn _cluster_by_id(
        specs: &FixedSpecs,
        wal_man: &WalMan,
        volume: &VolumeHandler,
        cluster_id: &ClusterId,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<Option<Cluster>> {
        if cluster_id == specs.zero_cluster().content_id() {
            return Ok(Some(specs.zero_cluster().clone()));
        }

        // try to load directly from database
        {
            if let Ok(Some(cluster)) = Self::cluster_from_db(specs, cluster_id, &mut *conn).await {
                return Ok(Some(cluster));
            }
        }

        // check the local WALs
        if let Ok(Some(cluster)) = Self::cluster_from_wal(wal_man, cluster_id, &mut *conn).await {
            return Ok(Some(cluster));
        }

        // get from chunk
        if let Ok(Some(cluster)) = Self::cluster_from_chunk(volume, cluster_id, &mut *conn).await {
            return Ok(Some(cluster));
        }

        Ok(None)
    }

    #[instrument[skip(self)]]
    pub async fn snapshot_by_id(&self, snapshot_id: &SnapshotId) -> anyhow::Result<Option<Snapshot>> {
        let mut conn = self.pool.read().acquire().await?;
        let res = Self::_snapshot_by_id(
            &self.specs,
            &self.wal_man,
            &self.volume,
            snapshot_id,
            conn.as_mut(),
        )
        .await?;
        conn.close().await?;
        Ok(res)
    }

    #[instrument[skip_all]]
    async fn _snapshot_by_id(
        specs: &FixedSpecs,
        wal_man: &WalMan,
        volume: &VolumeHandler,
        snapshot_id: &SnapshotId,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<Option<Snapshot>> {
        for snapshot in specs.zero_snapshots() {
            if snapshot.content_id() == snapshot_id {
                return Ok(Some(snapshot.clone()));
            }
        }

        // try to load directly from database
        {
            if let Ok(Some(snapshot)) = Self::snapshot_from_db(specs, snapshot_id, &mut *conn).await
            {
                return Ok(Some(snapshot));
            }
        }

        // check the local WALs
        if let Ok(Some(snapshot)) = Self::snapshot_from_wal(wal_man, snapshot_id, &mut *conn).await
        {
            return Ok(Some(snapshot));
        }

        // get from chunk
        if let Ok(Some(snapshot)) = Self::snapshot_from_chunk(volume, snapshot_id, &mut *conn).await
        {
            return Ok(Some(snapshot));
        }

        Ok(None)
    }

    #[instrument[skip_all]]
    async fn snapshot_from_wal(
        wal_man: &WalMan,
        snapshot_id: &SnapshotId,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<Option<Snapshot>> {
        tracing::trace!("reading SNAPSHOT from WAL");
        for (wal_id, offsets) in Self::wal_offsets_for_id(snapshot_id, &mut *conn).await? {
            let mut wal_reader = match wal_man.open_reader(&wal_id).await {
                Ok(wal_reader) => wal_reader,
                Err(err) => {
                    tracing::error!(error = %err, wal_id = %wal_id, "opening wal reader failed");
                    continue;
                }
            };
            for offset in offsets {
                match wal_reader.snapshot(snapshot_id, offset).await {
                    Ok(snapshot) => return Ok(Some(snapshot)),
                    Err(err) => {
                        tracing::error!(error = %err, wal_id = %wal_id, offset, "reading snapshot failed");
                    }
                }
            }
        }
        Ok(None)
    }

    #[instrument[skip_all]]
    async fn snapshot_from_chunk(
        volume: &VolumeHandler,
        snapshot_id: &SnapshotId,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<Option<Snapshot>> {
        tracing::trace!("reading SNAPSHOT from CHUNK");
        let mut last_error = None;

        for (chunk_id, offsets) in Self::chunk_offsets_for_id(snapshot_id, &mut *conn).await? {
            for offset in offsets {
                match volume.snapshot(&chunk_id, offset).await {
                    Ok(snapshot) => return Ok(Some(snapshot)),
                    Err(err) => {
                        tracing::error!(error = %err, chunk_id = %chunk_id, offset, "reading snapshot failed");
                        last_error = Some(err);
                    }
                }
            }
        }
        if let Some(err) = last_error {
            Err(err)
        } else {
            Ok(None)
        }
    }

    #[instrument[skip_all]]
    async fn cluster_from_wal(
        wal_man: &WalMan,
        cluster_id: &ClusterId,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<Option<Cluster>> {
        tracing::trace!("reading cluster from wal");
        for (wal_id, offsets) in Self::wal_offsets_for_id(cluster_id, &mut *conn).await? {
            let mut wal_reader = match wal_man.open_reader(&wal_id).await {
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
                        tracing::error!(error = %err, wal_id = %wal_id, offset, "reading cluster failed");
                    }
                }
            }
        }
        Ok(None)
    }

    #[instrument[skip_all]]
    async fn cluster_from_chunk(
        volume: &VolumeHandler,
        cluster_id: &ClusterId,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<Option<Cluster>> {
        tracing::trace!("reading cluster from chunk");
        let mut last_error = None;

        for (chunk_id, offsets) in Self::chunk_offsets_for_id(cluster_id, &mut *conn).await? {
            for offset in offsets {
                match volume.cluster(&chunk_id, offset).await {
                    Ok(cluster) => return Ok(Some(cluster)),
                    Err(err) => {
                        tracing::error!(error = %err, chunk_id = %chunk_id, offset, "reading cluster failed");
                        last_error = Some(err);
                    }
                }
            }
        }
        if let Some(err) = last_error {
            Err(err)
        } else {
            Ok(None)
        }
    }

    #[instrument[skip(self)]]
    async fn block_from_wal(&self, block_id: &BlockId) -> anyhow::Result<Option<Block>> {
        for (wal_id, offsets) in {
            let mut conn = self.pool.read().acquire().await?;
            Self::wal_offsets_for_id(block_id, conn.as_mut()).await?
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
                        tracing::error!(error = %err, wal_id = %wal_id, offset, "reading block failed");
                    }
                }
            }
        }
        Ok(None)
    }

    #[instrument[skip(self)]]
    async fn block_from_chunk(&self, block_id: &BlockId) -> anyhow::Result<Option<Block>> {
        tracing::trace!("reading block from chunk");
        let mut last_error = None;

        for (chunk_id, offsets) in {
            let mut conn = self.pool.read().acquire().await?;
            Self::chunk_offsets_for_id(block_id, conn.as_mut()).await?
        } {
            for offset in offsets {
                match self.volume.block(&chunk_id, offset).await {
                    Ok(block) => return Ok(Some(block)),
                    Err(err) => {
                        tracing::error!(error = %err, chunk_id = %chunk_id, offset, "reading block failed");
                        last_error = Some(err);
                    }
                }
            }
        }
        if let Some(err) = last_error {
            Err(err)
        } else {
            Ok(None)
        }
    }

    async fn wal_offsets_for_id(
        id: impl Into<Id<'_>>,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<impl Iterator<Item = (WalId, Vec<u64>)>> {
        #[derive(Debug)]
        struct Row {
            wal_id: Option<Vec<u8>>,
            offset: i64,
        }

        fn try_convert(row: Row) -> anyhow::Result<(WalId, u64)> {
            Ok(
                WalId::try_from(row.wal_id.ok_or(anyhow!("wal_id is NULL"))?.as_slice())
                    .map(|w| (w, row.offset as u64))?,
            )
        }

        let id = id.into();
        let id_slice = id.as_bytes();
        let mut stream = match id {
            Id::BlockId(_) => {
                sqlx::query_as!(
                    Row,
                    "
                    SELECT wal_id, offset FROM available_content
                    WHERE source_type = 'W' AND content_type = 'B' AND block_id = ? AND snapshot_id IS NULL AND cluster_id IS NULL
                    ",
                    id_slice
                ).fetch(conn)
            }
            Id::ClusterId(_) => {
                sqlx::query_as!(
                    Row,
                    "
                    SELECT wal_id, offset FROM available_content
                    WHERE source_type = 'W' AND content_type = 'C' AND cluster_id = ? AND block_id IS NULL AND snapshot_id IS NULL
                    ",
                    id_slice
                ).fetch(conn)
            }
            Id::SnapshotId(_) => {
                sqlx::query_as!(
                    Row,
                    "
                    SELECT wal_id, offset FROM available_content
                    WHERE source_type = 'W' AND content_type = 'S' AND snapshot_id = ? AND block_id IS NULL AND cluster_id IS NULL
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

    async fn chunk_offsets_for_id(
        id: impl Into<Id<'_>>,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<impl Iterator<Item = (ChunkId, Vec<u64>)>> {
        #[derive(Debug)]
        struct Row {
            chunk_id: Option<Vec<u8>>,
            offset: i64,
        }

        fn try_convert(row: Row) -> anyhow::Result<(ChunkId, u64)> {
            Ok(
                ChunkId::try_from(row.chunk_id.ok_or(anyhow!("chunk_id is NULL"))?.as_slice())
                    .map(|w| (w, row.offset as u64))?,
            )
        }

        let id = id.into();
        let id_slice = id.as_bytes();
        let mut stream = match id {
            Id::BlockId(_) => {
                sqlx::query_as!(
                    Row,
                    "
                    SELECT chunk_id, offset FROM available_content
                    WHERE source_type = 'C' AND content_type = 'B' AND block_id = ? AND snapshot_id IS NULL AND cluster_id IS NULL
                    ",
                    id_slice
                ).fetch(conn)
            }
            Id::ClusterId(_) => {
                sqlx::query_as!(
                    Row,
                    "
                    SELECT chunk_id, offset FROM available_content
                    WHERE source_type = 'C' AND content_type = 'C' AND cluster_id = ? AND block_id IS NULL AND snapshot_id IS NULL
                    ",
                    id_slice
                ).fetch(conn)
            }
            Id::SnapshotId(_) => {
                sqlx::query_as!(
                    Row,
                    "
                    SELECT chunk_id, offset FROM available_content
                    WHERE source_type = 'C' AND content_type = 'S' AND snapshot_id = ? AND block_id IS NULL AND cluster_id IS NULL
                    ",
                    id_slice
                ).fetch(conn)
            }
        };

        let mut matches = HashMap::new();

        while let Some((chunk_id, offset)) = stream
            .try_next()
            .await?
            .map(|r| try_convert(r))
            .transpose()?
        {
            if !matches.contains_key(&chunk_id) {
                matches.insert(chunk_id.clone(), Vec::default());
            }
            matches.get_mut(&chunk_id).unwrap().push(offset);
        }

        Ok(matches.into_iter())
    }

    #[instrument[skip_all]]
    async fn snapshot_from_db(
        specs: &FixedSpecs,
        snapshot_id: &SnapshotId,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<Option<Snapshot>> {
        let cluster_ids = {
            let snapshot_id = snapshot_id.as_ref();
            sqlx::query!(
                "
                SELECT cluster_index, cluster_id FROM snapshot_content WHERE snapshot_id = ?;
                ",
                snapshot_id
            )
            .fetch(conn)
            .map_err(|e| anyhow::Error::from(e))
            .try_filter_map(|r| async move {
                let idx = r.cluster_index as usize;
                Hash::try_from((r.cluster_id.as_slice(), specs.meta_hash()))
                    .map(|h| Some((idx, h.into())))
            })
            .try_collect::<Vec<(usize, ClusterId)>>()
            .await?
        };

        if cluster_ids.is_empty() {
            return Ok(None);
        }

        let mut snapshot =
            SnapshotMut::from_snapshot(specs.zero_snapshot(cluster_ids.len()), specs.clone());
        for (idx, cluster_id) in cluster_ids {
            if idx >= snapshot.clusters().len() {
                return Err(anyhow!(
                    "database entry for snapshot [{}] invalid",
                    snapshot_id
                ));
            }
            snapshot.clusters()[idx] = cluster_id
        }
        let snapshot = snapshot.finalize();
        if snapshot.content_id() == snapshot_id {
            Ok(Some(snapshot))
        } else {
            Err(anyhow!(
                "database entry for snapshot [{}] invalid",
                snapshot_id
            ))
        }
    }

    #[instrument[skip_all]]
    async fn cluster_from_db(
        specs: &FixedSpecs,
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
                Hash::try_from((r.block_id.as_slice(), specs.meta_hash()))
                    .map(|h| Some((idx, h.into())))
            })
            .try_collect::<Vec<(usize, BlockId)>>()
            .await?
        };

        if block_ids.is_empty() {
            return Ok(None);
        }

        let mut cluster = ClusterMut::from_cluster(specs.zero_cluster().clone(), specs.clone());
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

    #[instrument[skip(tx), fields(snapshot = %snapshot.content_id())]]
    async fn sync_snapshot_content(
        snapshot: &Snapshot,
        zero_cluster_id: &ClusterId,
        tx: &mut SqliteConnection,
    ) -> anyhow::Result<u64> {
        let mut rows_affected = 0;
        let snapshot_id = snapshot.content_id().as_ref();

        let non_zero_clusters = snapshot
            .cluster_ids()
            .into_iter()
            .enumerate()
            .filter(|(_, b)| *b != zero_cluster_id)
            .collect::<Vec<_>>();

        if non_zero_clusters.is_empty() {
            return Ok(0);
        }

        let num_clusters = {
            let r = sqlx::query!(
                "
                SELECT COUNT(*) AS count FROM snapshot_content WHERE snapshot_id = ?;
                ",
                snapshot_id
            )
            .fetch_one(&mut *tx)
            .await?;
            r.count as usize
        };
        if num_clusters == non_zero_clusters.len() {
            // already synced
            return Ok(rows_affected);
        };

        if num_clusters > 0 {
            // invalid, delete
            rows_affected += sqlx::query!(
                "
                DELETE FROM snapshot_content WHERE snapshot_id = ?;
                ",
                snapshot_id
            )
            .execute(&mut *tx)
            .await?
            .rows_affected();
        }

        for (idx, cluster_id) in non_zero_clusters {
            let idx = idx as i32;
            let cluster_id = cluster_id.as_ref();
            rows_affected += sqlx::query!(
                "
                    INSERT INTO snapshot_content (snapshot_id, cluster_index, cluster_id)
                    VALUES (?, ?, ?)
                    ",
                snapshot_id,
                idx,
                cluster_id,
            )
            .execute(&mut *tx)
            .await?
            .rows_affected();
        }

        Ok(rows_affected)
    }

    #[instrument[skip(tx), fields(cluster = %cluster.content_id())]]
    async fn sync_cluster_content(
        cluster: &Cluster,
        zero_block_id: &BlockId,
        tx: &mut SqliteConnection,
    ) -> anyhow::Result<u64> {
        let non_zero_blocks = cluster
            .block_ids()
            .into_iter()
            .enumerate()
            .filter(|(_, b)| *b != zero_block_id)
            .collect::<Vec<_>>();

        if non_zero_blocks.is_empty() {
            return Ok(0);
        }

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
        if num_blocks == non_zero_blocks.len() {
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

        for (idx, block_id) in non_zero_blocks {
            let idx = idx as i32;
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

        Ok(rows_affected)
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
                    .snapshots
                    .iter()
                    .map(|(c, offset)| (c.into(), *offset)),
            );

        let wal_id = tx_details.wal_id.as_bytes().as_slice();
        {
            let branch = tx_details.branch.as_ref();
            let commit_id = tx_details.commit.content_id.as_ref();
            let preceding_commit_id = tx_details.commit.preceding_commit.as_ref();
            let snapshot_id = tx_details.commit.snapshot.as_ref();
            let committed = tx_details.commit.committed.timestamp_micros();
            let num_clusters = tx_details.commit.num_clusters as i64;
            sqlx::query!(
                "
                INSERT INTO wal_commits (wal_id, branch, commit_id, preceding_commit_id, snapshot_id, committed, num_clusters)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ",
                wal_id,
                branch,
                commit_id,
                preceding_commit_id,
                snapshot_id,
                committed,
                num_clusters
            ).execute(&mut *tx).await?;
        }

        for (id, offset) in items.into_iter() {
            let offset = offset as i64;
            let id_bytes = id.as_bytes();
            let (content_type, block_id, cluster_id, snapshot_id) = match &id {
                Id::BlockId(_) => ("B", Some(id_bytes), None, None),
                Id::ClusterId(_) => ("C", None, Some(id_bytes), None),
                Id::SnapshotId(_) => ("S", None, None, Some(id_bytes)),
            };
            rows_affected += sqlx::query!(
            "
                    INSERT INTO available_content (source_type, wal_id, offset, content_type, block_id, cluster_id, snapshot_id)
                    VALUES ('W', ?, ?, ?, ?, ?, ?);
                    ",
            wal_id,
            offset,
            content_type,
            block_id,
            cluster_id,
            snapshot_id
            )
                .execute(&mut *tx)
                .await?.rows_affected();
        }

        Ok(rows_affected)
    }

    #[instrument[skip_all]]
    pub async fn sync_unindexed_chunk_content(&mut self) -> anyhow::Result<()> {
        tracing::info!("syncing unindexed chunk content");
        let mut tx = self.pool.write().begin().await?;
        for chunk_id in sqlx::query!(
            "
            SELECT id FROM known_chunks WHERE indexed = 0 AND available > 0;
            "
        )
        .fetch_all(tx.as_mut())
        .await?
        .into_iter()
        .map(|r| ChunkId::try_from(r.id.as_slice()))
        {
            let chunk_id = chunk_id?;
            tracing::info!(chunk_id = %chunk_id, "chunk needs syncing");
            let chunk = self.volume.chunk_details(&chunk_id).await?;
            sync_chunk(&chunk, tx.as_mut()).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    #[instrument[skip_all]]
    pub async fn sync_manifests(&mut self) -> anyhow::Result<()> {
        tracing::info!("syncing manifests");
        let available_manifests = self
            .volume
            .list_manifests()
            .await?
            .collect::<HashMap<_, _>>();
        let mut tx = self.pool.write().begin().await?;

        let mut known_manifests: HashMap<ManifestId, Etag> = HashMap::default();
        for r in sqlx::query!("SELECT id, etag FROM manifests")
            .fetch_all(tx.as_mut())
            .await?
            .into_iter()
        {
            let id = match ManifestId::try_from(r.id.as_slice()) {
                Ok(id) => id,
                Err(err) => {
                    tracing::error!(error = %err, manifest_id = ?r.id, "invalid manifest_id found in database, removing");
                    sqlx::query!("DELETE FROM manifests WHERE id = ?", r.id)
                        .execute(tx.as_mut())
                        .await?;
                    continue;
                }
            };
            known_manifests.insert(id, Etag::from(r.etag));
        }

        for (id, etag) in available_manifests {
            let mut in_sync = false;
            if let Some(known_etag) = known_manifests.remove(&id) {
                if &known_etag == &etag {
                    in_sync = true;
                }
            }
            if !in_sync {
                tracing::info!(manifest_id = %id, "manifest needs syncing");
                let manifest = self.volume.manifest(&id).await?;
                sync_manifest(&manifest, &etag, tx.as_mut()).await?;
            }
        }

        for obsolete in known_manifests.into_keys() {
            tracing::debug!(
                manifest_id = %obsolete,
                "removing obsolete manifest from inventory",
            );
            let id = obsolete.as_bytes().as_slice();
            sqlx::query!("DELETE FROM manifests WHERE id = ?", id)
                .execute(tx.as_mut())
                .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    #[instrument[skip_all]]
    pub async fn sync_chunks(&mut self) -> anyhow::Result<()> {
        tracing::info!("syncing chunks");
        let available_chunks = self.volume.list_chunks().await?.collect::<HashMap<_, _>>();
        let mut tx = self.pool.write().begin().await?;

        let mut known_chunks: HashMap<ChunkId, Etag> = HashMap::default();
        for r in sqlx::query!("SELECT chunk_id, etag FROM chunk_files")
            .fetch_all(tx.as_mut())
            .await?
            .into_iter()
        {
            let chunk_id = match ChunkId::try_from(r.chunk_id.as_slice()) {
                Ok(chunk_id) => chunk_id,
                Err(err) => {
                    tracing::error!(error = %err, chunk_id = ?r.chunk_id, "invalid chunk_id found in database, removing");
                    sqlx::query!("DELETE FROM chunk_files WHERE chunk_id = ?", r.chunk_id)
                        .execute(tx.as_mut())
                        .await?;
                    continue;
                }
            };
            known_chunks.insert(chunk_id, Etag::from(r.etag));
        }

        for (chunk_id, etag) in available_chunks {
            let mut in_sync = false;
            if let Some(known_etag) = known_chunks.remove(&chunk_id) {
                if &known_etag == &etag {
                    in_sync = true;
                }
            }
            if !in_sync {
                sync_chunk_file(&chunk_id, &etag, tx.as_mut()).await?;
            }
        }

        for obsolete in known_chunks.into_keys() {
            tracing::debug!(
                chunk_id = %obsolete,
                "removing obsolete chunk from inventory",
            );
            let id = obsolete.as_bytes().as_slice();
            sqlx::query!("DELETE FROM chunk_files WHERE chunk_id = ?", id)
                .execute(tx.as_mut())
                .await?;
        }

        tx.commit().await?;

        Ok(())
    }

    #[instrument[skip_all]]
    async fn sync_commits(&mut self) -> anyhow::Result<()> {
        let mut tx = self.pool.write().begin().await?;
        Self::_sync_commits(&self.specs, &self.wal_man, &self.volume, tx.as_mut()).await?;
        tx.commit().await?;
        Ok(())
    }

    #[instrument[skip_all]]
    async fn _sync_commits(
        specs: &FixedSpecs,
        wal_man: &WalMan,
        volume: &VolumeHandler,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<()> {
        let hash_algo = specs.meta_hash();
        let zero_cluster_id = specs.zero_cluster().content_id().clone();
        let zero_block_id = specs.zero_block().content_id().clone();

        tracing::info!("syncing commits");

        let mut rows_deleted = sqlx::query!(
            "DELETE FROM snapshot_content WHERE snapshot_id NOT IN (SELECT DISTINCT snapshot_id FROM commits);"
        )
            .execute(&mut *conn)
            .await?
            .rows_affected();

        if rows_deleted > 0 {
            tracing::debug!(rows_deleted, "removed inactive snapshot_content rows");
        }

        let missing_snapshot_content = sqlx::query!(
            "
            SELECT DISTINCT snapshot_id FROM commits
            WHERE snapshot_id NOT IN (SELECT DISTINCT snapshot_id FROM snapshot_content);
            "
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|r| Hash::try_from((r.snapshot_id.as_slice(), hash_algo)).map(|h| h.into()))
        .collect::<Result<Vec<SnapshotId>, _>>()?;

        let mut rows_inserted = 0;
        for snapshot_id in missing_snapshot_content {
            if let Some(snapshot) =
                Self::_snapshot_by_id(specs, wal_man, volume, &snapshot_id, &mut *conn).await?
            {
                rows_inserted +=
                    Self::sync_snapshot_content(&snapshot, &zero_cluster_id, &mut *conn).await?;
            } else {
                tracing::warn!(snapshot_id = %snapshot_id, "snapshot for snapshot_id unavailable");
            }
        }

        let cluster_rows_deleted = sqlx::query!(
            "DELETE FROM cluster_content WHERE cluster_id NOT IN (SELECT DISTINCT cluster_id FROM snapshot_content);"
        )
            .execute(&mut *conn)
            .await?
            .rows_affected();

        if cluster_rows_deleted > 0 {
            tracing::debug!(
                cluster_rows_deleted,
                "removed inactive cluster_content rows"
            );
            rows_deleted += cluster_rows_deleted;
        }

        let missing_cluster_content = sqlx::query!(
            "
            SELECT DISTINCT cluster_id FROM snapshot_content
            WHERE cluster_id NOT IN (SELECT DISTINCT cluster_id FROM cluster_content);
            "
        )
        .fetch_all(&mut *conn)
        .await?
        .into_iter()
        .map(|r| Hash::try_from((r.cluster_id.as_slice(), hash_algo)).map(|h| h.into()))
        .collect::<Result<Vec<ClusterId>, _>>()?;

        for cluster_id in missing_cluster_content {
            if let Some(cluster) =
                Self::_cluster_by_id(specs, wal_man, volume, &cluster_id, &mut *conn).await?
            {
                rows_inserted +=
                    Self::sync_cluster_content(&cluster, &zero_block_id, &mut *conn).await?;
            } else {
                tracing::warn!(cluster_id = %cluster_id, "cluster for cluster_id unavailable");
            }
        }

        if rows_deleted > 0 || rows_inserted > 0 {
            tracing::debug!(rows_inserted, rows_deleted, "commits synced");
        }
        Ok(())
    }

    #[instrument[skip_all]]
    async fn sync_wal_files(&mut self) -> anyhow::Result<()> {
        tracing::info!("syncing wal files");
        let wal_files = self.wal_man.wal_files().await?;

        let mut known_wal_files: HashMap<WalId, Etag> = HashMap::default();
        let mut tx = self.pool.write().begin().await?;
        sqlx::query!("UPDATE wal_files SET active = 0")
            .execute(tx.as_mut())
            .await?;
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

        let mut newer_commit = None;

        // sync commits
        // this updates all branch commits where the wal commits contain newer commits
        if sqlx::query!(
            "
            WITH branches AS (
                SELECT name
                FROM commits
                WHERE type = 'B'
            ),
            latest_wc AS (
                SELECT wc.branch,
                    wc.commit_id,
                    wc.preceding_commit_id,
                    wc.snapshot_id,
                    wc.committed,
                    wc.num_clusters
                FROM wal_commits wc
                JOIN branches b ON wc.branch = b.name
                WHERE wc.committed = (
                    SELECT MAX(committed)
                    FROM wal_commits
                    WHERE branch = wc.branch
                )
            )
            UPDATE commits
            SET commit_id = latest_wc.commit_id,
                preceding_commit_id = latest_wc.preceding_commit_id,
                snapshot_id = latest_wc.snapshot_id,
                committed = latest_wc.committed,
                num_clusters = latest_wc.num_clusters
            FROM latest_wc
            WHERE commits.name = latest_wc.branch
              AND commits.type = 'B'
              AND latest_wc.committed > commits.committed;
            "
        )
        .execute(tx.as_mut())
        .await?
        .rows_affected()
            > 0
        {
            let commit = commit_from_db(&self.branch, &self.specs, tx.as_mut()).await?;
            if &commit > &self.current_commit {
                newer_commit = Some(commit);
            }
        }
        tx.commit().await?;

        if let Some(commit) = newer_commit {
            tracing::info!(wal = %commit.content_id(), local = %self.current_commit.content_id(), "latest wal commit is further ahead, updating local branch");
            self.current_commit = commit;
        }

        Ok(())
    }

    #[instrument(skip(self, tx), fields(wal_id = %wal_id))]
    async fn sync_wal_file(
        &mut self,
        wal_id: &WalId,
        etag: &Etag,
        tx: &mut SqliteConnection,
    ) -> anyhow::Result<u64> {
        tracing::debug!(wal_id = %wal_id, "syncing wal file");
        let mut wal_reader = self.wal_man.open_reader(wal_id).await?;

        let id = wal_id.as_bytes().as_slice();
        let etag = etag.as_ref();
        let created = wal_reader.header().created.timestamp_micros();

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
            "INSERT INTO wal_files (id, etag, created, active)
             VALUES (?, ?, ?, 0)
            ",
            id,
            etag,
            created,
        )
        .execute(&mut *tx)
        .await?
        .rows_affected();

        let mut stream = wal_reader.transactions(None).await?;
        while let Some(tx_details) = stream.try_next().await? {
            rows_affected += Self::process_tx_details(&tx_details, &mut *tx).await?;
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
            let wal_reader = self.wal_man.open_reader(wal_id).await?;
            let id = tx_details.wal_id.as_bytes().as_slice();
            let etag = wal_reader.as_ref().etag().await?;
            let etag = etag.as_ref();
            let created = wal_reader.header().created.timestamp_micros();
            let mut tx = self.pool.writer.begin().await?;
            sqlx::query!(
                "
                INSERT INTO wal_files (id, etag, created, active)
                VALUES (?, ?, ?, 1)
                ON CONFLICT(id) DO UPDATE SET
                    etag = excluded.etag,
                    created = excluded.created,
                    active = 1;
                ",
                id,
                etag,
                created
            )
            .execute(tx.as_mut())
            .await?;

            Self::process_tx_details(tx_details, tx.as_mut()).await?;
            update_commit(self.branch(), &tx_details.commit, tx.as_mut()).await?;
            Self::_sync_commits(&self.specs, &self.wal_man, &self.volume, tx.as_mut()).await?;

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
    let snapshot_id = commit.snapshot().as_ref();
    let commited = commit.committed().timestamp_micros();
    let num_clusters = commit.num_clusters() as i64;
    let branch = branch.as_ref();
    sqlx::query!(
        "
        UPDATE commits SET
        commit_id = ?, preceding_commit_id = ?, snapshot_id = ?, committed = ?, num_clusters = ?
        WHERE name = ? and type = 'B'
        ",
        commit_id,
        preceding_commit_id,
        snapshot_id,
        commited,
        num_clusters,
        branch
    )
    .execute(&mut *tx)
    .await?;
    Ok(())
}

async fn commit_from_db(
    branch: &BranchName,
    specs: &FixedSpecs,
    tx: &mut SqliteConnection,
) -> anyhow::Result<Commit> {
    let branch = branch.as_ref();
    let r = sqlx::query!(
        "
        SELECT commit_id, preceding_commit_id, snapshot_id, committed, num_clusters
        FROM commits WHERE name = ? AND type = 'B'
        ",
        branch
    )
    .fetch_one(&mut *tx)
    .await?;
    Ok(Commit {
        content_id: Hash::try_from((r.commit_id.as_slice(), specs.meta_hash()))?.into(),
        preceding_commit: Hash::try_from((r.preceding_commit_id.as_slice(), specs.meta_hash()))?
            .into(),
        snapshot: Hash::try_from((r.snapshot_id.as_slice(), specs.meta_hash()))?.into(),
        committed: DateTime::from_timestamp_micros(r.committed)
            .ok_or(anyhow!("invalid timestamp"))?,
        num_clusters: r.num_clusters as usize,
    })
}

#[instrument[skip_all, fields(db_file = %db_file.display())]]
async fn db_init(
    db_file: &Path,
    max_connections: u8,
    specs: &FixedSpecs,
    branches: &HashMap<BranchName, BranchInfo>,
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
        INSERT INTO known_content (content_type, block_id, used, chunk_avail, wal_avail)
        VALUES ('B', ?, 0, 1, 0)
        ON CONFLICT(block_id) DO UPDATE SET chunk_avail = 1;
        ",
        block_id,
    )
    .execute(tx.as_mut())
    .await?;

    let zero_cluster = specs.zero_cluster();
    let cluster_id = zero_cluster.content_id().as_ref();
    sqlx::query!(
        "
        INSERT INTO known_content (content_type, cluster_id, used, chunk_avail, wal_avail)
        VALUES ('C', ?, 0, 1, 0)
        ON CONFLICT(cluster_id) DO UPDATE SET chunk_avail = 1;
                ",
        cluster_id
    )
    .execute(tx.as_mut())
    .await?;

    for num_clusters in branches.values().into_iter().map(|b| b.commit.num_clusters) {
        let zero_snapshot = specs.zero_snapshot(num_clusters);
        let snapshot_id = zero_snapshot.content_id().as_ref();
        sqlx::query!(
            "
            INSERT INTO known_content (content_type, snapshot_id, used, chunk_avail, wal_avail)
            VALUES ('S', ?, 0, 1, 0)
            ON CONFLICT(snapshot_id) DO UPDATE SET chunk_avail = 1;
            ",
            snapshot_id
        )
        .execute(tx.as_mut())
        .await?;
    }

    let to_delete = sqlx::query!("SELECT name FROM commits where type = 'B';")
        .map(|r| r.name)
        .fetch_all(tx.as_mut())
        .await?
        .into_iter()
        .filter(|b| {
            match TryInto::<BranchName>::try_into(b) {
                Ok(b) => !branches.contains_key(&b),
                Err(_) => {
                    // invalid branch name, delete
                    true
                }
            }
        })
        .collect::<Vec<_>>();

    for branch in to_delete {
        sqlx::query!("DELETE FROM commits WHERE name = ? AND type = 'B';", branch)
            .execute(tx.as_mut())
            .await?;
    }

    // delete locked commits
    sqlx::query!("DELETE FROM commits WHERE type = 'LB'")
        .execute(tx.as_mut())
        .await?;

    for (branch, commit) in branches.iter().map(|(s, b)| (s, &b.commit)) {
        let commit_id = commit.content_id().as_ref();
        let preceding_commit_id = commit.preceding_commit().as_ref();
        let snapshot_id = commit.snapshot().as_ref();
        let committed = commit.committed().timestamp_micros();
        let num_clusters = commit.num_clusters() as i64;
        let branch = branch.as_ref();
        sqlx::query!(
            "
            INSERT INTO commits (name, type, commit_id, preceding_commit_id, snapshot_id, committed, num_clusters)
            SELECT ?, 'B', ?, ?, ?, ?, ?
            WHERE NOT EXISTS (
                SELECT 1 FROM commits WHERE name = ? and type = 'B'
            )
            ",
            branch,
            commit_id,
            preceding_commit_id,
            snapshot_id,
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

async fn sync_manifest(
    manifest: &Manifest,
    etag: &Etag,
    tx: &mut SqliteConnection,
) -> anyhow::Result<()> {
    tracing::debug!(manifest_id = %&manifest.id, "syncing manifest");

    let id = manifest.id.as_bytes().as_slice();
    let etag = etag.as_ref();

    let rows_affected = sqlx::query!("DELETE FROM manifests WHERE id = ?", id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if rows_affected > 0 {
        tracing::trace!(
            rows_affected,
            "deleted manifest related entries from database"
        );
    }

    sqlx::query!(
        "INSERT INTO manifests (id, etag)
             VALUES (?, ?)
            ",
        id,
        etag
    )
    .execute(&mut *tx)
    .await?;

    for chunk in manifest.chunks.iter() {
        let chunk_id = chunk.id().as_bytes().as_slice();
        sqlx::query!(
            "
            INSERT INTO manifest_content (manifest_id, chunk_id)
            VALUES (?, ?)
            ",
            id,
            chunk_id,
        )
        .execute(&mut *tx)
        .await?;

        if chunk.len() > 0 {
            sync_chunk(&chunk, tx).await?;
        }
    }

    Ok(())
}

async fn sync_chunk(chunk: &Chunk, tx: &mut SqliteConnection) -> anyhow::Result<()> {
    tracing::debug!(chunk_id = %chunk.id(), "syncing chunk");

    let id = chunk.id().as_bytes().as_slice();

    let rows_affected = sqlx::query!("DELETE FROM available_content WHERE chunk_id = ?", id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if rows_affected > 0 {
        tracing::trace!(rows_affected, "deleted chunk related entries from database");
    }

    let chunk_file_exists = sqlx::query!(
        "SELECT count(*) AS num FROM chunk_files WHERE chunk_id = ?",
        id
    )
    .fetch_one(&mut *tx)
    .await?
    .num > 0;

    if chunk_file_exists {
        insert_chunk_content(chunk, tx).await?;
    }

    Ok(())
}

async fn insert_chunk_content(chunk: &Chunk, tx: &mut SqliteConnection) -> anyhow::Result<()> {
    let id = chunk.id().as_bytes().as_slice();

    for (offset, content) in chunk.content().into_iter() {
        let offset = offset as i64;
        let (content_type, block_id, cluster_id, snapshot_id) = match content {
            ChunkEntry::BlockId(block_id) => ("B", Some(block_id.as_ref()), None, None),
            ChunkEntry::ClusterId(cluster_id) => ("C", None, Some(cluster_id.as_ref()), None),
            ChunkEntry::SnapshotId(snapshot_id) => ("S", None, None, Some(snapshot_id.as_ref())),
        };

        sqlx::query!(
            "
            INSERT INTO available_content (source_type, chunk_id, offset, content_type, block_id, cluster_id, snapshot_id)
            VALUES ('C', ?, ?, ?, ?, ?, ?)
            ",
            id,
            offset,
            content_type,
            block_id,
            cluster_id,
            snapshot_id,
        ).execute(&mut *tx).await?;
    }

    Ok(())
}

async fn sync_chunk_file(
    id: &ChunkId,
    etag: &Etag,
    tx: &mut SqliteConnection,
) -> anyhow::Result<()> {
    tracing::debug!(chunk_id = %id, "syncing chunk");

    let id = id.as_bytes().as_slice();
    let etag = etag.as_ref();

    let rows_affected = sqlx::query!("DELETE FROM chunk_files WHERE chunk_id = ?", id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if rows_affected > 0 {
        tracing::trace!(rows_affected, "deleted chunk related entries from database");
    }

    sqlx::query!(
        "INSERT INTO chunk_files (chunk_id, etag)
             VALUES (?, ?)
            ",
        id,
        etag
    )
    .execute(&mut *tx)
    .await?;

    Ok(())
}
