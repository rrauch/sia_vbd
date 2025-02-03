use crate::hash::{Hash, HashAlgorithm};
use crate::vbd::{
    Block, BlockId, Cluster, ClusterId, ClusterMut, Commit, CommitId, CommitMut, FixedSpecs,
    WalReader,
};
use crate::wal::man::WalMan;
use crate::wal::{TokioWalFile, TxDetails, WalId};
use crate::{Etag, SqlitePool};
use anyhow::{anyhow, bail};
use arc_swap::ArcSwap;
use futures::{Stream, TryStreamExt};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{ConnectOptions, SqliteConnection};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::iter;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::log::LevelFilter;
use tracing::{instrument, Instrument};

pub(crate) struct Inventory {
    specs: FixedSpecs,
    pool: SqlitePool,
    num_clusters: usize,
    zero_block: Block,
    zero_cluster: Cluster,
    zero_commits: ArcSwap<HashMap<usize, Commit>>,
    branch: String,
    current_commit: Commit,
    wal_man: Arc<WalMan>,
}

enum Id<'a> {
    BlockId(OwnedOrBorrowed<'a, BlockId>),
    ClusterId(OwnedOrBorrowed<'a, ClusterId>),
    CommitId(OwnedOrBorrowed<'a, CommitId>),
}

impl<'a> Display for Id<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockId(id) => Display::fmt(id.deref(), f),
            Self::ClusterId(id) => Display::fmt(id.deref(), f),
            Self::CommitId(id) => Display::fmt(id.deref(), f),
        }
    }
}

impl<'a> Debug for Id<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BlockId(id) => Debug::fmt(id.deref(), f),
            Self::ClusterId(id) => Debug::fmt(id.deref(), f),
            Self::CommitId(id) => Debug::fmt(id.deref(), f),
        }
    }
}

impl<'a> Id<'a> {
    fn as_bytes(&'a self) -> &'a [u8] {
        match self {
            Self::BlockId(id) => id.as_ref(),
            Self::ClusterId(id) => id.as_ref(),
            Self::CommitId(id) => id.as_ref(),
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

impl From<CommitId> for Id<'static> {
    fn from(value: CommitId) -> Self {
        Self::CommitId(OwnedOrBorrowed::Owned(value))
    }
}

impl<'a> From<&'a CommitId> for Id<'a> {
    fn from(value: &'a CommitId) -> Self {
        Self::CommitId(OwnedOrBorrowed::Borrowed(value))
    }
}

impl Inventory {
    #[instrument(skip(wal_man), fields(db_file = %db_file.display(), branch = branch.as_ref()
    ))]
    pub(super) async fn create(
        db_file: &Path,
        max_db_connections: u8,
        specs: &FixedSpecs,
        num_clusters: usize,
        branch: impl AsRef<str>,
        wal_man: Arc<WalMan>,
    ) -> Result<Self, anyhow::Error> {
        tracing::debug!("creating new inventory");
        let db_path_exists = tokio::fs::try_exists(db_file).await?;
        if db_path_exists {
            bail!("database file at {} already exists", db_file.display());
        }
        let (pool, new_specs) = db_init(db_file, max_db_connections, Some(specs)).await?;
        if &new_specs != specs {
            bail!("specs do not match");
        }
        let specs = new_specs;
        let (zero_block, zero_cluster, zero_commit) = Self::calc_zeroed(num_clusters, &specs);

        let branch = branch.as_ref().to_string();

        let mut tx = pool.writer.begin().await?;

        {
            let block_id = zero_block.content_id().as_ref();
            sqlx::query!(
                "
                INSERT INTO known_blocks (block_id, used, available)
                VALUES (?, 0, 1);
                ",
                block_id
            )
            .execute(tx.as_mut())
            .await?;
        }
        {
            let cluster_id = zero_cluster.content_id().as_ref();
            sqlx::query!(
                "
                INSERT INTO known_clusters (cluster_id, used, available)
                VALUES (?, 0, 1);
                ",
                cluster_id
            )
            .execute(tx.as_mut())
            .await?;
        }

        {
            let commit_id = zero_commit.content_id().as_ref();
            sqlx::query!(
                "
                INSERT INTO known_commits (commit_id, used, available)
                VALUES (?, 0, 1);
                ",
                commit_id
            )
            .execute(tx.as_mut())
            .await?;

            let name = branch.as_str();
            let num_clusters = num_clusters as i64;
            sqlx::query!(
                "
                INSERT INTO branches (name, commit_id, num_clusters)
                VALUES (?, ?, ?);
                ",
                name,
                commit_id,
                num_clusters,
            )
            .execute(tx.as_mut())
            .await?;
        }

        Self::sync_cluster_content(&zero_cluster, zero_block.content_id(), tx.as_mut()).await?;
        Self::sync_commit_content(&zero_commit, zero_cluster.content_id(), tx.as_mut()).await?;

        tx.commit().await?;
        tracing::info!("new inventory created");
        Ok(Self {
            specs,
            pool,
            branch,
            num_clusters,
            zero_block,
            zero_cluster,
            zero_commits: ArcSwap::from_pointee(
                iter::once((num_clusters, zero_commit.clone()))
                    .into_iter()
                    .collect(),
            ),
            current_commit: zero_commit,
            wal_man,
        })
    }

    #[instrument(skip(wal_man), fields(db_file = %db_file.display(), branch = branch.as_ref()
    ))]
    pub(super) async fn load(
        db_file: &Path,
        max_db_connections: u8,
        branch: impl AsRef<str>,
        wal_man: Arc<WalMan>,
    ) -> Result<Self, anyhow::Error> {
        let (pool, specs) = db_init(db_file, max_db_connections, None).await?;
        let branch = branch.as_ref();

        tracing::debug!("loading inventory");

        let (commit_id, num_clusters) = {
            let r = sqlx::query!(
                "
                SELECT commit_id, num_clusters FROM branches WHERE name = ?
                ",
                branch
            )
            .fetch_one(pool.read())
            .await?;
            (
                Hash::try_from((r.commit_id.as_slice(), specs.meta_hash()))?.into(),
                r.num_clusters as usize,
            )
        };

        let (zero_block, zero_cluster, zero_commit) = Self::calc_zeroed(num_clusters, &specs);

        let mut this = Self {
            specs,
            pool,
            branch: branch.to_string(),
            num_clusters,
            zero_block,
            zero_cluster,
            zero_commits: ArcSwap::from_pointee(
                iter::once((num_clusters, zero_commit.clone()))
                    .into_iter()
                    .collect(),
            ),
            current_commit: zero_commit,
            wal_man,
        };

        this.sync_wal_files().await?;

        this.current_commit = this
            .commit_by_id(&commit_id)
            .await?
            .ok_or_else(|| anyhow!("branch commit [{}] is unknown", &commit_id))?;

        tracing::debug!("inventory loaded");

        Ok(this)
    }

    fn calc_zeroed(num_clusters: usize, specs: &FixedSpecs) -> (Block, Cluster, Commit) {
        let zero_block = Block::zeroed(&specs);
        let zero_cluster = ClusterMut::zeroed(specs.clone(), zero_block.content_id()).finalize();
        let zero_commit =
            CommitMut::zeroed(specs.clone(), zero_cluster.content_id(), num_clusters).finalize();
        (zero_block, zero_cluster, zero_commit)
    }

    pub fn specs(&self) -> &FixedSpecs {
        &self.specs
    }

    pub fn zero_block(&self) -> &Block {
        &self.zero_block
    }

    pub fn zero_cluster(&self) -> &Cluster {
        &self.zero_cluster
    }

    pub fn zero_commit(&self) -> Commit {
        let guard = self.zero_commits.load();
        if let Some(commit) = guard.get(&self.num_clusters) {
            return commit.clone();
        }
        let mut map = guard.as_ref().clone();
        drop(guard);

        let commit = CommitMut::zeroed(
            self.specs.clone(),
            self.zero_cluster.content_id(),
            self.num_clusters,
        )
        .finalize();
        map.insert(self.num_clusters, commit.clone());
        self.zero_commits.store(Arc::new(map));
        commit
    }

    pub fn num_clusters(&self) -> usize {
        self.num_clusters
    }

    pub fn current_commit(&self) -> &Commit {
        &self.current_commit
    }

    pub fn branch(&self) -> &String {
        &self.branch
    }

    #[instrument[skip(self)]]
    pub async fn block_by_id(&self, block_id: &BlockId) -> anyhow::Result<Option<Block>> {
        if block_id == self.zero_block.content_id() {
            return Ok(Some(self.zero_block.clone()));
        }

        // check the local WALs
        if let Ok(Some(block)) = self.block_from_wal(block_id).await {
            return Ok(Some(block));
        }

        Ok(None)
    }

    #[instrument[skip(self)]]
    pub async fn cluster_by_id(&self, cluster_id: &ClusterId) -> anyhow::Result<Option<Cluster>> {
        if cluster_id == self.zero_cluster.content_id() {
            return Ok(Some(self.zero_cluster.clone()));
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
    pub async fn commit_by_id(&self, commit_id: &CommitId) -> anyhow::Result<Option<Commit>> {
        if self.current_commit.content_id() == commit_id {
            return Ok(Some(self.current_commit.clone()));
        }
        {
            let guard = self.zero_commits.load();
            for commit in guard.values() {
                if commit.content_id() == commit_id {
                    return Ok(Some(commit.clone()));
                }
            }
        }

        // try to load directly from database
        {
            let mut conn = self.pool.read().acquire().await?;
            if let Ok(Some(commit)) = self.commit_from_db(commit_id, conn.as_mut()).await {
                return Ok(Some(commit));
            }
        }

        // check the local WALs
        if let Ok(Some(commit)) = self.commit_from_wal(commit_id).await {
            return Ok(Some(commit));
        }

        Ok(None)
    }

    #[instrument[skip(self)]]
    async fn commit_from_wal(&self, commit_id: &CommitId) -> anyhow::Result<Option<Commit>> {
        tracing::debug!("reading commit from wal");
        for (wal_id, offsets) in {
            let mut conn = self.pool.read().acquire().await?;
            self.wal_offsets_for_id(commit_id, conn.as_mut()).await?
        } {
            let mut wal_reader = match self.wal_man.open_reader(&wal_id).await {
                Ok(wal_reader) => wal_reader,
                Err(err) => {
                    tracing::error!(error = %err, wal_id = %wal_id, "opening wal reader failed");
                    continue;
                }
            };
            for offset in offsets {
                match wal_reader.commit(commit_id, offset).await {
                    Ok(commit) => return Ok(Some(commit)),
                    Err(err) => {
                        tracing::error!(error = %err, wal_id = %wal_id, "reading commit failed");
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
        tracing::debug!("reading block from wal");
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
                    WHERE content_type = 'B' AND block_id = ? AND commit_id IS NULL AND cluster_id IS NULL
                    ",
                    id_slice
                ).fetch(conn)
            }
            Id::ClusterId(_) => {
                sqlx::query_as!(
                    Row,
                    "
                    SELECT wal_id, file_offset FROM wal_content
                    WHERE content_type = 'C' AND cluster_id = ? AND block_id IS NULL AND commit_id IS NULL
                    ",
                    id_slice
                ).fetch(conn)
            }
            Id::CommitId(_) => {
                sqlx::query_as!(
                    Row,
                    "
                    SELECT wal_id, file_offset FROM wal_content
                    WHERE content_type = 'S' AND commit_id = ? AND block_id IS NULL AND cluster_id IS NULL
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
    async fn commit_from_db(
        &self,
        commit_id: &CommitId,
        conn: &mut SqliteConnection,
    ) -> anyhow::Result<Option<Commit>> {
        let cluster_ids = {
            let commit_id = commit_id.as_ref();
            sqlx::query!(
                "
                SELECT cluster_index, cluster_id FROM commit_content WHERE commit_id = ?;
                ",
                commit_id
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

        let mut commit = CommitMut::from_commit(self.zero_commit(), self.specs.clone());
        for (idx, cluster_id) in cluster_ids {
            if idx >= commit.clusters().len() {
                return Err(anyhow!("database entry for commit [{}] invalid", commit_id));
            }
            commit.clusters()[idx] = cluster_id
        }
        let commit = commit.finalize();
        if commit.content_id() == commit_id {
            Ok(Some(commit))
        } else {
            Err(anyhow!("database entry for commit [{}] invalid", commit_id))
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

        let mut cluster = ClusterMut::from_cluster(self.zero_cluster().clone(), self.specs.clone());
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

    #[instrument[skip(tx), fields(commit = %commit.content_id())]]
    async fn sync_commit_content(
        commit: &Commit,
        zero_cluster_id: &ClusterId,
        tx: &mut SqliteConnection,
    ) -> anyhow::Result<u64> {
        let mut rows_affected = 0;
        let commit_id = commit.content_id().as_ref();
        let num_clusters = {
            let r = sqlx::query!(
                "
                SELECT COUNT(*) AS count FROM commit_content WHERE commit_id = ?;
                ",
                commit_id
            )
            .fetch_one(&mut *tx)
            .await?;
            r.count as usize
        };
        if num_clusters == commit.len() {
            // already synced
            return Ok(rows_affected);
        };

        if num_clusters > 0 {
            // invalid, delete
            rows_affected += sqlx::query!(
                "
                DELETE FROM commit_content WHERE commit_id = ?;
                ",
                commit_id
            )
            .execute(&mut *tx)
            .await?
            .rows_affected();
        }

        let mut idx = 0;
        for cluster_id in commit.cluster_ids() {
            if cluster_id != zero_cluster_id {
                let cluster_id = cluster_id.as_ref();
                rows_affected += sqlx::query!(
                    "
                    INSERT INTO commit_content (commit_id, cluster_index, cluster_id)
                    VALUES (?, ?, ?)
                    ",
                    commit_id,
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
        zero_commit_ids: &'a Vec<CommitId>,
    ) -> impl Stream<Item = Result<Id<'static>, anyhow::Error>> + use<'a> {
        sqlx::query!(
            "
            SELECT block_id, NULL AS cluster_id, NULL AS commit_id FROM known_blocks
                WHERE used = 0 AND available > 0
                UNION ALL
            SELECT NULL, cluster_id, NULL FROM known_clusters
                WHERE used = 0 AND available > 0
                UNION ALL
            SELECT NULL, NULL, commit_id FROM known_commits
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
            } else if let Some(commit_id) = r.commit_id {
                Hash::try_from((commit_id.as_slice(), specs.meta_hash()))
                    .map(|h| h.into())
                    .map(|c: CommitId| {
                        if zero_commit_ids.contains(&c) {
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
                    .commits
                    .iter()
                    .map(|(c, offset)| (c.into(), *offset)),
            );

        let wal_id = tx_details.wal_id.as_bytes().as_slice();

        for (id, offset) in items.into_iter() {
            let file_offset = offset as i64;
            let id_bytes = id.as_bytes();
            let (content_type, block_id, cluster_id, commit_id) = match &id {
                Id::BlockId(_) => ("B", Some(id_bytes), None, None),
                Id::ClusterId(_) => ("C", None, Some(id_bytes), None),
                Id::CommitId(_) => ("S", None, None, Some(id_bytes)),
            };
            rows_affected += sqlx::query!(
            "
                    INSERT INTO wal_content (wal_id, file_offset, content_type, block_id, cluster_id, commit_id)
                    VALUES (?, ?, ?, ?, ?, ?);
                    ",
            wal_id,
            file_offset,
            content_type,
            block_id,
            cluster_id,
            commit_id
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
                    tracing::error!(wal_id = ?r.id, "invalid wal_id found in database, removing");
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
        let mut commits = HashMap::new();

        {
            let mut stream = wal_reader.transactions(None).await?;
            while let Some(tx_details) = stream.try_next().await? {
                rows_affected += Self::process_tx_details(&tx_details, &mut *tx).await?;
                tx_details.clusters.into_iter().for_each(|(id, offset)| {
                    clusters.insert(id, offset);
                });
                tx_details.commits.into_iter().for_each(|(id, offset)| {
                    commits.insert(id, offset);
                });
            }
        }

        let zero_block_id = self.zero_block().content_id().clone();
        let zero_cluster_id = self.zero_cluster().content_id().clone();
        let zero_commit_ids = {
            self.zero_commits
                .load()
                .values()
                .into_iter()
                .map(|c| c.content_id().clone())
                .collect::<Vec<_>>()
        };

        // find any unused content that has been in this wal file
        for (id, offset) in Self::unused(
            &mut *tx,
            &self.specs,
            &zero_block_id,
            &zero_cluster_id,
            &zero_commit_ids,
        )
        .try_filter_map(|id| {
            let clusters = &clusters;
            let commits = &commits;
            async move {
                Ok(match &id {
                    Id::BlockId(block_id) => None,
                    Id::ClusterId(cluster_id) => {
                        if let Some(offset) = clusters.get(cluster_id).map(|c| *c) {
                            Some((id, offset))
                        } else {
                            None
                        }
                    }
                    Id::CommitId(commit_id) => {
                        if let Some(offset) = commits.get(commit_id).map(|c| *c) {
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
                Id::CommitId(commit_id) => {
                    let commit = wal_reader.commit(&commit_id, offset).await?;
                    rows_affected +=
                        Self::sync_commit_content(&commit, &zero_cluster_id, &mut *tx).await?;
                }
            }
        }
        tracing::debug!(rows_affected, "wal file sync complete");
        Ok(rows_affected)
    }

    #[instrument[skip(self, tx_details), fields(wal_file = %wal_file.as_ref().display())]]
    pub async fn update_wal<P: AsRef<Path>>(
        &mut self,
        tx_details: &TxDetails,
        wal_file: P,
    ) -> anyhow::Result<()> {
        {
            let mut wal_reader = WalReader::new(TokioWalFile::open(wal_file).await?).await?;
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

            let zero_block_id = self.zero_block().content_id().clone();
            let zero_cluster_id = self.zero_cluster().content_id().clone();
            let zero_commit_ids = {
                self.zero_commits
                    .load()
                    .values()
                    .into_iter()
                    .map(|c| c.content_id().clone())
                    .collect::<Vec<_>>()
            };

            // find any unused content that has been in this wal update
            for id in Self::unused(
                tx.as_mut(),
                &self.specs,
                &zero_block_id,
                &zero_cluster_id,
                &zero_commit_ids,
            )
            .try_filter_map(move |id| async move {
                Ok(
                    if match &id {
                        Id::BlockId(block_id) => tx_details.blocks.contains_key(block_id),
                        Id::ClusterId(cluster_id) => tx_details.clusters.contains_key(cluster_id),
                        Id::CommitId(commit_id) => tx_details.commits.contains_key(commit_id),
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
                    Id::CommitId(commit_id) => {
                        if let Some(offset) = tx_details.commits.get(&commit_id) {
                            let commit = wal_reader.commit(&commit_id, *offset).await?;
                            Self::sync_commit_content(&commit, &zero_cluster_id, tx.as_mut())
                                .await?;
                        }
                    }
                }
            }

            let commit_id = tx_details.commit_id.as_ref();
            let num_clusters = self.num_clusters as i64;
            let name = self.branch.as_str();
            sqlx::query!(
                "
                UPDATE branches SET commit_id = ?, num_clusters = ? WHERE name = ?
                ",
                commit_id,
                num_clusters,
                name
            )
            .execute(tx.as_mut())
            .await?;

            tx.commit().await?;

            self.current_commit = self
                .commit_by_id(&tx_details.commit_id)
                .await?
                .ok_or_else(|| anyhow!("commit [{}] not found", tx_details.commit_id))?;
            Ok(())
        }
    }
}

#[instrument[fields(db_file = %db_file.display())]]
async fn db_init(
    db_file: &Path,
    max_connections: u8,
    specs: Option<&FixedSpecs>,
) -> anyhow::Result<(SqlitePool, FixedSpecs)> {
    let new = specs.is_some();
    let writer = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with({
            SqliteConnectOptions::new()
                .create_if_missing(new)
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
    if let Some(specs) = specs {
        // check config
        {
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
        }
    }

    let r = sqlx::query!(
        "
        SELECT vbd_id, cluster_size, block_size, content_hash, meta_hash
        FROM config;
        ",
    )
    .fetch_one(tx.as_mut())
    .await?;

    let specs = FixedSpecs::new(
        r.vbd_id.as_slice().try_into()?,
        (r.cluster_size as usize).try_into()?,
        (r.block_size as usize).try_into()?,
        try_from_db_hash(r.content_hash)?,
        try_from_db_hash(r.meta_hash)?,
    );

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

    Ok((SqlitePool { writer, reader }, specs))
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
