use crate::hash::Hash;
use crate::inventory::chunk::{
    Chunk, ChunkContent, ChunkEntry, ChunkId, ChunkWriter, Manifest, ManifestId,
};
use crate::inventory::{commit_from_db, sync_chunk, sync_chunk_file, sync_manifest};
use crate::repository::{CommitType, VolumeHandler};
use crate::vbd::{BlockId, BranchName, ClusterId, Commit, FixedSpecs, SnapshotId};
use crate::wal::man::WalMan;
use crate::wal::WalId;
use crate::SqlitePool;
use anyhow::{anyhow, bail};
use async_tempfile::TempDir;
use chrono::Utc;
use futures::TryStreamExt;
use sqlx::SqliteConnection;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, DropGuard};
use uuid::Uuid;

pub(super) struct Syncer {
    pool: SqlitePool,
    wal_man: Arc<WalMan>,
    volume: Arc<VolumeHandler>,
    specs: FixedSpecs,
    branch_name: BranchName,
    temp_dir: Arc<TempDir>,
    max_chunk_size: u64,
    task_handle: JoinHandle<(Option<ChunkWriter>, Option<Commit>)>,
    ct: CancellationToken,
    _drop_guard: DropGuard,
}

impl Syncer {
    pub fn new(
        pool: SqlitePool,
        wal_man: Arc<WalMan>,
        volume: Arc<VolumeHandler>,
        specs: FixedSpecs,
        initial_delay: Duration,
        run_interval: Duration,
        branch_name: BranchName,
        temp_dir: TempDir,
        max_chunk_size: u64,
    ) -> Self {
        let ct = CancellationToken::new();
        let _drop_guard = ct.clone().drop_guard();
        let temp_dir = Arc::new(temp_dir);

        let task_handle = tokio::spawn({
            let pool = pool.clone();
            let wal_man = wal_man.clone();
            let volume = volume.clone();
            let specs = specs.clone();
            let branch_name = branch_name.clone();
            let temp_dir = temp_dir.clone();
            let ct = ct.clone();
            async move {
                sync_loop(
                    pool,
                    wal_man,
                    volume,
                    specs,
                    initial_delay,
                    run_interval,
                    branch_name,
                    temp_dir,
                    ct,
                    max_chunk_size,
                )
                .await
            }
        });

        Self {
            pool,
            wal_man,
            volume,
            specs,
            branch_name,
            temp_dir,
            max_chunk_size,
            task_handle,
            ct,
            _drop_guard,
        }
    }

    pub(crate) async fn close(self) -> anyhow::Result<()> {
        tracing::debug!("closing syncer");
        self.ct.cancel();
        let (mut chunk_writer, mut incomplete_commit) = self.task_handle.await?;

        pack_chunks(
            &self.branch_name,
            &self.pool,
            &self.wal_man,
            &self.volume,
            &self.specs,
            &self.temp_dir,
            self.max_chunk_size,
            &mut chunk_writer,
            &mut incomplete_commit,
            true,
        )
        .await?;

        tracing::debug!("syncer closed successfully");
        Ok(())
    }
}

async fn sync_loop(
    pool: SqlitePool,
    wal_man: Arc<WalMan>,
    volume: Arc<VolumeHandler>,
    specs: FixedSpecs,
    initial_delay: Duration,
    run_interval: Duration,
    branch_name: BranchName,
    temp_dir: Arc<TempDir>,
    ct: CancellationToken,
    max_chunk_size: u64,
) -> (Option<ChunkWriter>, Option<Commit>) {
    tokio::select! {
        _ = tokio::time::sleep(initial_delay) => {},
        _ = ct.cancelled() => {
            return (None, None);
        }
    }

    let mut chunk_writer = None;
    let mut incomplete_commit = None;

    if let Err(err) = pack_chunks(
        &branch_name,
        &pool,
        &wal_man,
        &volume,
        &specs,
        &temp_dir,
        max_chunk_size,
        &mut chunk_writer,
        &mut incomplete_commit,
        true,
    )
    .await
    {
        tracing::error!(error = %err, "error packing chunks");
    }

    loop {
        if ct.is_cancelled() {
            return (chunk_writer, incomplete_commit);
        }

        if let Err(err) = delete_expendable_wal_files(&pool, &wal_man).await {
            tracing::error!(error = %err, "error deleting expendable wal files");
        }

        if ct.is_cancelled() {
            return (chunk_writer, incomplete_commit);
        }

        tokio::select! {
            _ = tokio::time::sleep(run_interval) => {},
            _ = ct.cancelled() => {
                return (chunk_writer, incomplete_commit);
            }
        }

        if let Err(err) = pack_chunks(
            &branch_name,
            &pool,
            &wal_man,
            &volume,
            &specs,
            &temp_dir,
            max_chunk_size,
            &mut chunk_writer,
            &mut incomplete_commit,
            false,
        )
        .await
        {
            tracing::error!(error = %err, "error packing chunks");
        }

        if ct.is_cancelled() {
            return (chunk_writer, incomplete_commit);
        }

        if let Err(err) = gc(&pool, &volume).await {
            tracing::error!(error = %err, "error during garbage collection");
        }
    }
}

async fn gc(pool: &SqlitePool, volume: &VolumeHandler) -> anyhow::Result<()> {
    tracing::debug!("deleting obsolete chunks");
    let mut chunks_deleted = 0;
    loop {
        let mut tx = pool.write().begin().await?;
        match sqlx::query!("SELECT chunk_id FROM chunk_files WHERE critical = 0 LIMIT 1")
            .fetch_optional(tx.as_mut())
            .await?
            .map(|r| TryInto::<ChunkId>::try_into(r.chunk_id.as_slice()))
            .transpose()?
        {
            Some(chunk_id) => {
                tracing::debug!(chunk_id = %chunk_id, "deleting obsolete chunk");
                let id = chunk_id.as_bytes().as_slice();
                let rows_affected = sqlx::query!("DELETE FROM chunk_files WHERE chunk_id = ?", id)
                    .execute(tx.as_mut())
                    .await?
                    .rows_affected();
                if rows_affected > 0 {
                    tracing::trace!(rows_affected, "database entry deleted");
                }
                volume.delete_chunk(&chunk_id).await?;
                tx.commit().await?;
                chunks_deleted += 1;
            }
            None => {
                tx.commit().await?;
                break;
            }
        }
    }
    if chunks_deleted > 0 {
        tracing::info!(chunks_deleted, "deleted obsolete chunks");
    }

    let mut manifests_deleted = 0;
    tracing::debug!("deleting obsolete manifests");
    loop {
        let mut tx = pool.write().begin().await?;
        match sqlx::query!("SELECT id FROM manifests WHERE prunable >= entries LIMIT 1")
            .fetch_optional(tx.as_mut())
            .await?
            .map(|r| TryInto::<ManifestId>::try_into(r.id.as_slice()))
            .transpose()?
        {
            Some(manifest_id) => {
                tracing::debug!(manifest_id = %manifest_id, "deleting obsolete manifest");
                let id = manifest_id.as_bytes().as_slice();
                let rows_affected = sqlx::query!("DELETE FROM manifests WHERE id = ?", id)
                    .execute(tx.as_mut())
                    .await?
                    .rows_affected();
                if rows_affected > 0 {
                    tracing::trace!(rows_affected, "database entry deleted");
                }
                volume.delete_manifest(&manifest_id).await?;
                tx.commit().await?;
                manifests_deleted += 1;
            }
            None => {
                tx.commit().await?;
                break;
            }
        }
    }
    if manifests_deleted > 0 {
        tracing::info!(manifests_deleted, "deleted obsolete manifests");
    }
    Ok(())
}

async fn delete_expendable_wal_files(pool: &SqlitePool, wal_man: &WalMan) -> anyhow::Result<()> {
    tracing::debug!("deleting expendable wal files");
    let mut deleted = 0;
    let expendable = sqlx::query!("SELECT id FROM wal_files WHERE critical = 0 AND active = 0;")
        .fetch_all(pool.read())
        .await?
        .into_iter()
        .map(|r| TryInto::<WalId>::try_into(r.id.as_slice()))
        .collect::<Result<Vec<_>, _>>()?;

    for wal_id in expendable {
        let mut tx = pool.write().begin().await?;
        let id = wal_id.as_bytes().as_slice();
        if sqlx::query!(
            "DELETE FROM wal_files WHERE id = ? AND active = 0 AND critical = 0",
            id
        )
        .execute(tx.as_mut())
        .await?
        .rows_affected()
            > 0
        {
            wal_man.delete(&wal_id).await?;
            deleted += 1;
        }
        tx.commit().await?;
    }

    if deleted > 0 {
        tracing::info!(deleted_wal_files = deleted, "expendable wal files deleted");
    }

    Ok(())
}

async fn pack_chunks(
    branch: &BranchName,
    pool: &SqlitePool,
    wal_man: &WalMan,
    volume: &VolumeHandler,
    specs: &FixedSpecs,
    temp_dir: &TempDir,
    max_size: u64,
    chunk_writer: &mut Option<ChunkWriter>,
    incomplete_commit: &mut Option<Commit>,
    flush: bool,
) -> anyhow::Result<()> {
    tracing::debug!("packing chunks");
    let mut tx = pool.write().begin().await?;
    let commit = commit_from_db(&CommitType::from(branch.clone()), specs, tx.as_mut()).await?;
    {
        let commit_id = commit.content_id().as_ref();
        let preceding_commit_id = commit.preceding_commit().as_ref();
        let snapshot_id = commit.snapshot().as_ref();
        let committed = commit.committed().timestamp_micros();
        let num_clusters = commit.num_clusters() as i64;
        let branch = branch.as_ref();

        sqlx::query!("DELETE FROM commits WHERE name = ? and type = 'LB'", branch)
            .execute(tx.as_mut())
            .await?;

        // Lock the current commit
        sqlx::query!(
            "
            INSERT INTO commits (name, type, commit_id, preceding_commit_id, snapshot_id, committed, num_clusters)
            SELECT ?, 'LB', ?, ?, ?, ?, ?
            ",
            branch,
            commit_id,
            preceding_commit_id,
            snapshot_id,
            committed,
            num_clusters,
    )
            .execute(tx.as_mut())
            .await?;
    }

    let content = find_packable_content(commit.snapshot(), tx.as_mut(), specs).await?;
    tx.commit().await?;

    for (wal_id, content) in content.into_iter() {
        let mut wal_reader = wal_man.open_reader(&wal_id).await?;
        for (wal_offset, entry) in content.into_iter() {
            let chunk_content = match entry {
                ChunkEntry::BlockId(block_id) => {
                    ChunkContent::Block(wal_reader.block(&block_id, wal_offset).await?)
                }
                ChunkEntry::ClusterId(cluster_id) => {
                    ChunkContent::Cluster(wal_reader.cluster(&cluster_id, wal_offset).await?)
                }
                ChunkEntry::SnapshotId(snapshot_id) => {
                    ChunkContent::Snapshot(wal_reader.snapshot(&snapshot_id, wal_offset).await?)
                }
            };

            if chunk_writer.is_none() {
                *chunk_writer = Some(ChunkWriter::new(max_size, temp_dir, specs.clone()).await?);
            }

            if !chunk_writer
                .as_mut()
                .unwrap()
                .append(&chunk_content)
                .await?
            {
                // chunk is full
                let (chunk, len, reader) = chunk_writer.take().unwrap().finalize().await?;
                let etag = volume.put_chunk(&chunk, len, reader).await?;
                let mut tx = pool.write().begin().await?;
                sync_chunk_file(chunk.id(), &etag, tx.as_mut()).await?;
                sync_chunk(&chunk, tx.as_mut()).await?;
                tx.commit().await?;
            }
        }
    }

    if flush {
        if chunk_writer.is_some() {
            let (chunk, len, reader) = chunk_writer.take().unwrap().finalize().await?;
            let etag = volume.put_chunk(&chunk, len, reader).await?;
            let mut tx = pool.write().begin().await?;
            sync_chunk_file(chunk.id(), &etag, tx.as_mut()).await?;
            sync_chunk(&chunk, tx.as_mut()).await?;
            tx.commit().await?;
        }
    }

    if flush {
        // update index
        let mut conn = pool.read().acquire().await?;
        let mut stream = sqlx::query!(
            "SELECT chunk_id, offset, block_id, cluster_id, snapshot_id
                FROM available_content
                WHERE chunk_id IN (SELECT id
                   FROM known_chunks
                   WHERE indexed = 0 AND available > 0
               );
            "
        )
        .fetch(conn.as_mut());

        let mut chunks = HashMap::new();
        while let Some(r) = stream.try_next().await? {
            if r.chunk_id.is_none() {
                continue;
            }
            if let Ok(chunk_id) = ChunkId::try_from(r.chunk_id.unwrap().as_slice()) {
                if !chunks.contains_key(&chunk_id) {
                    chunks.insert(chunk_id, BTreeMap::new());
                }
                let offset = r.offset as u64;
                if let Some(block_id) = r
                    .block_id
                    .map(|b| {
                        Hash::try_from((b.as_slice(), specs.content_hash()))
                            .ok()
                            .map(|h| BlockId::try_from(h).ok())
                    })
                    .flatten()
                    .flatten()
                {
                    chunks
                        .get_mut(&chunk_id)
                        .unwrap()
                        .insert(offset, ChunkEntry::BlockId(block_id));
                }
                if let Some(cluster_id) = r
                    .cluster_id
                    .map(|b| {
                        Hash::try_from((b.as_slice(), specs.content_hash()))
                            .ok()
                            .map(|h| ClusterId::try_from(h).ok())
                    })
                    .flatten()
                    .flatten()
                {
                    chunks
                        .get_mut(&chunk_id)
                        .unwrap()
                        .insert(offset, ChunkEntry::ClusterId(cluster_id));
                }
                if let Some(snapshot_id) = r
                    .snapshot_id
                    .map(|b| {
                        Hash::try_from((b.as_slice(), specs.content_hash()))
                            .ok()
                            .map(|h| SnapshotId::try_from(h).ok())
                    })
                    .flatten()
                    .flatten()
                {
                    chunks
                        .get_mut(&chunk_id)
                        .unwrap()
                        .insert(offset, ChunkEntry::SnapshotId(snapshot_id));
                }
            }
        }
        drop(stream);
        conn.close().await?;

        let manifest = Manifest {
            id: Uuid::now_v7().into(),
            specs: specs.clone(),
            created: Utc::now(),
            chunks: chunks
                .into_iter()
                .map(|(chunk_id, entries)| Chunk::new(chunk_id, entries.into_iter()))
                .collect(),
        };

        let num_chunks = manifest.len();
        if num_chunks > 0 {
            let etag = volume.update_manifest(manifest.clone()).await?;
            let mut tx = pool.write().begin().await?;
            sync_manifest(&manifest, &etag, tx.as_mut()).await?;
            tx.commit().await?;
            tracing::debug!(manifest_id = %&manifest.id, chunks = num_chunks, "manifest updated");
        }
    }

    if chunk_writer.is_some() {
        // not completed yet
        *incomplete_commit = Some(commit);
    } else {
        incomplete_commit.take();
        volume.update_branch_commit(&commit).await?;
        let branch = branch.as_ref();
        let mut tx = pool.write().begin().await?;
        sqlx::query!("DELETE FROM commits WHERE name = ? and type = 'LB'", branch)
            .execute(tx.as_mut())
            .await?;
        tx.commit().await?;
    }

    Ok(())
}

async fn find_packable_content(
    snapshot_id: &SnapshotId,
    conn: &mut SqliteConnection,
    specs: &FixedSpecs,
) -> anyhow::Result<HashMap<WalId, Vec<(u64, ChunkEntry)>>> {
    let content_hash = specs.content_hash();
    let meta_hash = specs.meta_hash();
    let snapshot_id = snapshot_id.as_ref();

    #[derive(Debug)]
    struct Row {
        wal_id: Option<Vec<u8>>,
        block_id: Option<Vec<u8>>,
        cluster_id: Option<Vec<u8>>,
        snapshot_id: Option<Vec<u8>>,
        offset: i64,
    }

    let try_convert = |row: Row| -> anyhow::Result<(WalId, u64, ChunkEntry)> {
        let wal_id = WalId::try_from(row.wal_id.ok_or(anyhow!("missing wal_id"))?.as_slice())?;
        let offset = row.offset as u64;
        let entry = if row.block_id.is_some() {
            ChunkEntry::BlockId(
                Hash::try_from((row.block_id.unwrap().as_slice(), content_hash))?.into(),
            )
        } else if row.cluster_id.is_some() {
            ChunkEntry::ClusterId(
                Hash::try_from((row.cluster_id.unwrap().as_slice(), meta_hash))?.into(),
            )
        } else if row.snapshot_id.is_some() {
            ChunkEntry::SnapshotId(
                Hash::try_from((row.snapshot_id.unwrap().as_slice(), meta_hash))?.into(),
            )
        } else {
            bail!("invalid row returned, no content id found");
        };
        Ok((wal_id, offset, entry))
    };

    let mut content = HashMap::new();

    for (wal_id, offset, entry) in sqlx::query_as!(
        Row,
        "
WITH clusters AS (SELECT DISTINCT cluster_id
                  FROM snapshot_content
                  WHERE snapshot_id = ?),
     blocks AS (SELECT DISTINCT block_id
                FROM cluster_content
                WHERE cluster_id IN (SELECT cluster_id FROM clusters)),
     filtered_content AS (SELECT block_id, cluster_id, snapshot_id
                          FROM known_content
                          WHERE (
                              block_id IN (SELECT block_id FROM blocks) OR
                              cluster_id IN (SELECT cluster_id FROM clusters) OR
                              snapshot_id = ?
                              )
                            AND chunk_avail = 0
                            AND wal_avail > 0),
     desired_ids AS (SELECT block_id AS id
                     FROM filtered_content
                     UNION
                     SELECT cluster_id
                     FROM filtered_content
                     UNION
                     SELECT snapshot_id
                     FROM filtered_content)
SELECT wal_id, offset, cluster_id, block_id, snapshot_id
FROM available_content
WHERE block_id IN (SELECT id FROM desired_ids)
   OR cluster_id IN (SELECT id FROM desired_ids)
   OR snapshot_id IN (SELECT id FROM desired_ids);
        ",
        snapshot_id,
        snapshot_id
    )
    .fetch_all(&mut *conn)
    .await?
    .into_iter()
    .map(|r| try_convert(r))
    .collect::<Result<Vec<_>, _>>()?
    {
        if !content.contains_key(&wal_id) {
            content.insert(wal_id.clone(), vec![]);
        }
        content.get_mut(&wal_id).unwrap().push((offset, entry));
    }

    Ok(content)
}
