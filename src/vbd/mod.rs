use crate::hash::{Hash, HashAlgorithm};
use crate::inventory::Inventory;
use crate::io::{ReadWrite, TokioFile};
use crate::repository::VolumeHandler;
use crate::wal;
use crate::wal::man::WalMan;
use crate::wal::{RollbackError, WalError, WalReader, WalWriter};
use anyhow::{anyhow, bail};
use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use std::cmp::{min, Ordering};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hasher;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut, Range};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::{Mutex, RwLock};
use tracing::instrument;
use uuid::Uuid;

pub mod nbd_device;

const BS16K: usize = 16 * 1024;
const BS64K: usize = 64 * 1024;
const BS256K: usize = 256 * 1024;

pub struct ContentId<T>(Hash, PhantomData<T>);

impl<T> From<Hash> for ContentId<T> {
    fn from(value: Hash) -> Self {
        ContentId(value, PhantomData::default())
    }
}

impl<T> Clone for ContentId<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}

impl<T> Display for ContentId<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl<T> Debug for ContentId<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl<T> std::hash::Hash for ContentId<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl<T> PartialEq for ContentId<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<T> Eq for ContentId<T> {}

impl<T> Deref for ContentId<T> {
    type Target = Hash;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct TypedUuid<T>(Uuid, PhantomData<T>);

impl<T> From<Uuid> for TypedUuid<T> {
    fn from(value: Uuid) -> Self {
        TypedUuid(value, PhantomData::default())
    }
}

impl<T> TryFrom<&str> for TypedUuid<T> {
    type Error = uuid::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Uuid::parse_str(value).map(|uuid| uuid.into())
    }
}

impl<T> TryFrom<&[u8]> for TypedUuid<T> {
    type Error = uuid::Error;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Uuid::from_slice(value).map(|uuid| uuid.into())
    }
}

impl<T> Clone for TypedUuid<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}

impl<T> Copy for TypedUuid<T> {}

impl<T> Display for TypedUuid<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl<T> Debug for TypedUuid<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl<T> std::hash::Hash for TypedUuid<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl<T> PartialEq for TypedUuid<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl<T> Eq for TypedUuid<T> {}

impl<T> Deref for TypedUuid<T> {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub type VbdId = TypedUuid<VirtualBlockDevice>;

type WalTx = wal::writer::Tx<TokioFile<ReadWrite>>;

pub struct VirtualBlockDevice {
    config: Arc<Config>,
    state: State,
    inventory: Arc<RwLock<Inventory>>,
    wal_man: Arc<WalMan>,
}

struct Config {
    specs: FixedSpecs,
    zero_cluster: Cluster,
    zero_block: Block,
    max_tx_size: u64,
    max_write_buffer: usize,
    wal_dir: PathBuf,
    max_wal_size: u64,
    db_file: PathBuf,
    max_db_connections: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixedSpecs {
    inner: Arc<FixedSpecsInner>,
}

impl FixedSpecs {
    pub fn new(
        vbd_id: VbdId,
        cluster_size: ClusterSize,
        block_size: BlockSize,
        content_hash: HashAlgorithm,
        meta_hash: HashAlgorithm,
    ) -> Self {
        let dummy = FixedSpecs {
            inner: Arc::new(FixedSpecsInner {
                vbd_id: vbd_id.clone(),
                cluster_size: cluster_size.clone(),
                block_size: block_size.clone(),
                content_hash: content_hash.clone(),
                meta_hash: meta_hash.clone(),
                zero_block: None,
                zero_cluster: None,
                zero_indices: ArcSwap::from_pointee(HashMap::default()),
            }),
        };

        let zero_block = Block::zeroed(&dummy);

        let dummy = FixedSpecs {
            inner: Arc::new(FixedSpecsInner {
                vbd_id: dummy.inner.vbd_id.clone(),
                cluster_size: dummy.inner.cluster_size.clone(),
                block_size: dummy.inner.block_size.clone(),
                content_hash: dummy.inner.content_hash.clone(),
                meta_hash: dummy.inner.meta_hash.clone(),
                zero_block: Some(zero_block.clone()),
                zero_cluster: None,
                zero_indices: ArcSwap::from_pointee(HashMap::default()),
            }),
        };

        let zero_cluster = ClusterMut::zeroed(dummy).finalize();

        Self {
            inner: Arc::new(FixedSpecsInner {
                vbd_id,
                cluster_size,
                block_size,
                content_hash,
                meta_hash,
                zero_block: Some(zero_block),
                zero_cluster: Some(zero_cluster),
                zero_indices: ArcSwap::from_pointee(HashMap::default()),
            }),
        }
    }

    pub fn vbd_id(&self) -> VbdId {
        self.inner.vbd_id
    }

    pub fn cluster_size(&self) -> ClusterSize {
        self.inner.cluster_size
    }

    pub fn block_size(&self) -> BlockSize {
        self.inner.block_size
    }

    pub fn content_hash(&self) -> HashAlgorithm {
        self.inner.content_hash
    }

    pub fn meta_hash(&self) -> HashAlgorithm {
        self.inner.meta_hash
    }

    pub fn zero_block(&self) -> Block {
        self.inner.zero_block.as_ref().unwrap().clone()
    }

    pub fn zero_cluster(&self) -> Cluster {
        self.inner.zero_cluster.as_ref().unwrap().clone()
    }

    pub fn zero_index(&self, num_clusters: usize) -> Index {
        let guard = self.inner.zero_indices.load();
        if let Some(index) = guard.get(&num_clusters) {
            return index.clone();
        }
        let mut map = guard.as_ref().clone();
        drop(guard);

        let index = IndexMut::zeroed(self.clone(), num_clusters).finalize();
        map.insert(num_clusters, index.clone());
        self.inner.zero_indices.store(Arc::new(map));
        index
    }

    pub fn zero_indices(&self) -> impl Iterator<Item = Index> {
        let vec = self
            .inner
            .zero_indices
            .load()
            .values()
            .into_iter()
            .map(|c| c.clone())
            .collect::<Vec<_>>();
        vec.into_iter()
    }
}

struct FixedSpecsInner {
    vbd_id: VbdId,
    cluster_size: ClusterSize,
    block_size: BlockSize,
    content_hash: HashAlgorithm,
    meta_hash: HashAlgorithm,
    zero_block: Option<Block>,
    zero_cluster: Option<Cluster>,
    zero_indices: ArcSwap<HashMap<usize, Index>>,
}

impl PartialEq for FixedSpecsInner {
    fn eq(&self, other: &Self) -> bool {
        &self.vbd_id == &other.vbd_id
            && &self.cluster_size == &other.cluster_size
            && &self.block_size == &other.block_size
            && &self.content_hash == &other.content_hash
            && &self.meta_hash == &other.meta_hash
    }
}

impl Eq for FixedSpecsInner {}

impl Debug for FixedSpecsInner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}, {}, {}, {}]",
            *self.cluster_size, *self.block_size, self.content_hash, &self.meta_hash
        )
    }
}

enum State {
    Committed(Committed),
    Uncommitted(Uncommitted),
    Poisoned,
}

impl State {
    fn commit(&self) -> &Commit {
        match &self {
            State::Committed(state) => &state.commit,
            State::Uncommitted(state) => &state.previous_commit,
            State::Poisoned => unreachable!("poisoned state"),
        }
    }

    fn index(&self) -> &Index {
        match &self {
            State::Committed(state) => &state.index,
            State::Uncommitted(state) => &state.previous_index,
            State::Poisoned => unreachable!("poisoned state"),
        }
    }
}

struct Committed {
    commit: Commit,
    index: Index,
    inventory: Arc<RwLock<Inventory>>,
    wal: Option<WalWriter>,
}

impl Committed {
    async fn begin(
        mut self,
        config: Arc<Config>,
        wal_man: Arc<WalMan>,
    ) -> Result<Uncommitted, (BlockError, Self)> {
        async fn prepare_wal(
            mut wal: Option<WalWriter>,
            wal_man: &WalMan,
            config: &Config,
        ) -> Result<WalWriter, BlockError> {
            let mut preceding_wal_id = None;
            if let Some(existing_wal) = wal.take() {
                if existing_wal.remaining().await? <= config.max_tx_size {
                    preceding_wal_id = Some(existing_wal.id().clone());
                } else {
                    wal = Some(existing_wal);
                }
            };

            if wal.is_none() {
                // start a new wal
                wal = Some(
                    wal_man
                        .new_writer(preceding_wal_id, config.specs.clone())
                        .await?,
                );
            }

            Ok(wal.expect("wal should be Some"))
        }

        let wal = match prepare_wal(self.wal.take(), &wal_man, &config).await {
            Ok(wal) => wal,
            Err(err) => {
                return Err((err, self));
            }
        };

        match Uncommitted::new(
            wal,
            self.commit.clone(),
            self.index.clone(),
            config,
            self.inventory.clone(),
            wal_man,
        )
        .await
        {
            Ok(u) => Ok(u),
            Err((err, wal)) => {
                let wal = wal.ok();
                Err((
                    err,
                    Self {
                        commit: self.commit,
                        index: self.index,
                        wal,
                        inventory: self.inventory,
                    },
                ))
            }
        }
    }
}

struct ModifiedData {
    unflushed_blocks: HashMap<(usize, usize), Option<Bytes>>,
    clusters: HashMap<usize, HashMap<usize, BlockId>>,
}

enum ModifiedBlock {
    Data(Bytes),
    BlockId(BlockId),
    Zeroed,
}

impl ModifiedData {
    fn get(&self, cluster_no: usize, block_no: usize) -> Option<ModifiedBlock> {
        if let Some(b) = self
            .unflushed_blocks
            .get(&(cluster_no, block_no))
            .map(|b| b.as_ref())
        {
            return Some(match b {
                Some(data) => ModifiedBlock::Data(data.clone()),
                None => ModifiedBlock::Zeroed,
            });
        };

        if let Some(block_id) = self
            .clusters
            .get(&cluster_no)
            .map(|m| m.get(&block_no))
            .flatten()
        {
            return Some(ModifiedBlock::BlockId(block_id.clone()));
        }

        None
    }

    fn put(&mut self, cluster_no: usize, block_no: usize, data: Option<Bytes>) {
        self.unflushed_blocks.insert((cluster_no, block_no), data);
    }

    #[instrument(skip(self, config, wal, wal_blocks), fields(tx_id = %wal.id()))]
    async fn flush(
        &mut self,
        config: &Config,
        wal: &mut WalTx,
        wal_blocks: &mut HashMap<BlockId, u64>,
    ) -> Result<(), BlockError> {
        tracing::debug!("flushing active tx");
        for ((cluster_no, block_no), data) in self.unflushed_blocks.drain() {
            let block = data.map(|d| Block::from_bytes(&config.specs, d));
            let block_id = match block {
                Some(block) => {
                    let block_id = block.content_id.clone();
                    wal_blocks.insert(block_id.clone(), wal.put(&block).await?);
                    block_id
                }
                None => config.zero_block.content_id.clone(),
            };

            if !self.clusters.contains_key(&cluster_no) {
                self.clusters.insert(cluster_no, HashMap::default());
            }
            self.clusters
                .get_mut(&cluster_no)
                .unwrap()
                .insert(block_no, block_id);
        }
        tracing::debug!("flushing complete");
        Ok(())
    }
}

struct Uncommitted {
    previous_commit: Commit,
    previous_index: Index,
    config: Arc<Config>,
    last_modified: Instant,
    data: ModifiedData,
    wal_tx: WalTx,
    wal_reader: Mutex<Option<WalReader>>,
    wal_blocks: HashMap<BlockId, u64>,
    inventory: Arc<RwLock<Inventory>>,
    wal_man: Arc<WalMan>,
}

impl Uncommitted {
    async fn new(
        wal: WalWriter,
        previous_commit: Commit,
        previous_index: Index,
        config: Arc<Config>,
        inventory: Arc<RwLock<Inventory>>,
        wal_man: Arc<WalMan>,
    ) -> Result<Self, (BlockError, Result<WalWriter, RollbackError>)> {
        let wal_tx = match wal.begin(previous_commit.clone(), config.max_tx_size).await {
            Ok(wal) => wal,
            Err((e, wal)) => {
                return Err((e.into(), wal));
            }
        };

        Ok(Self {
            previous_commit,
            previous_index,
            config,
            data: ModifiedData {
                unflushed_blocks: HashMap::default(),
                clusters: HashMap::default(),
            },
            last_modified: Instant::now(),
            wal_tx,
            wal_blocks: HashMap::default(),
            wal_reader: Mutex::new(None),
            inventory,
            wal_man,
        })
    }

    #[instrument(skip(self))]
    async fn get(
        &self,
        cluster_no: usize,
        block_no: usize,
    ) -> Result<Option<Option<Bytes>>, WalError> {
        if let Some(block) = self.data.get(cluster_no, block_no) {
            return Ok(Some(match block {
                ModifiedBlock::Data(data) => Some(data),
                ModifiedBlock::Zeroed => None,
                ModifiedBlock::BlockId(block_id) => {
                    if &block_id == self.config.zero_block.content_id() {
                        None
                    } else {
                        if let Some(offset) = self.wal_blocks.get(&block_id) {
                            let mut lock = self.wal_reader.lock().await;
                            if lock.is_none() {
                                *lock =
                                    Some(self.wal_man.open_reader(&self.wal_tx.wal_id()).await?);
                            }

                            let wal_reader = lock.as_mut().unwrap();
                            wal_reader
                                .block(&block_id, *offset)
                                .await
                                .map(|b| Some(b.data))?
                        } else {
                            tracing::warn!(block_id = %block_id, "uncommited block not found in wal");
                            let lock = self.inventory.read().await;
                            lock.block_by_id(&block_id)
                                .await?
                                .map(|b| Some(b.data))
                                .flatten()
                        }
                    }
                }
            }));
        }
        Ok(None)
    }

    #[instrument(skip(self, data))]
    async fn put(
        &mut self,
        cluster_no: usize,
        block_no: usize,
        data: Bytes,
    ) -> Result<(), BlockError> {
        if self.buffered_size() + data.len() >= self.config.max_write_buffer {
            // time to flush to wal
            self.data
                .flush(&self.config, &mut self.wal_tx, &mut self.wal_blocks)
                .await?;
        }
        self.data.put(cluster_no, block_no, Some(data));
        self.last_modified = Instant::now();
        Ok(())
    }

    fn delete(&mut self, cluster_no: usize, block_no: usize) {
        self.data.put(cluster_no, block_no, None);
        self.last_modified = Instant::now();
    }

    fn buffered_size(&self) -> usize {
        self.data
            .unflushed_blocks
            .values()
            .map(|b| b.as_ref().map(|b| b.len()).unwrap_or(0))
            .sum()
    }

    fn is_full(&self) -> bool {
        //self.wal_tx.remaining() == 0
        self.wal_tx.remaining() <= self.buffered_size() as u64 + 1024 * 1024
    }

    #[instrument(skip(self), fields(tx_id = %self.wal_tx.id()))]
    async fn try_commit(mut self) -> Result<Committed, (BlockError, Committed)> {
        async fn apply_changes(
            mut modified_data: ModifiedData,
            previous_index: &Index,
            inventory: &Inventory,
            config: &Config,
            wal: &mut WalTx,
            wal_blocks: &mut HashMap<BlockId, u64>,
        ) -> Result<Index, BlockError> {
            modified_data.flush(config, wal, wal_blocks).await?;
            let mut index = IndexMut::from_index(previous_index.clone(), config.specs.clone());

            let mut non_zero_clusters = 0;

            for (cluster_no, modified_blocks) in modified_data.clusters.drain() {
                let cluster_id = previous_index
                    .clusters
                    .get(cluster_no)
                    .expect("cluster no in bounds");

                let cluster = inventory.cluster_by_id(cluster_id).await?.ok_or_else(|| {
                    BlockError::ClusterNoFound {
                        cluster_id: cluster_id.clone(),
                    }
                })?;

                let mut cluster = ClusterMut::from_cluster(cluster, config.specs.clone());
                for (idx, block_id) in modified_blocks.into_iter() {
                    cluster.blocks[idx] = block_id;
                }
                let cluster = cluster.finalize();

                if cluster.content_id() != config.zero_cluster.content_id() {
                    wal.put(&cluster).await?;
                    non_zero_clusters += 1;
                }
                index.clusters[cluster_no] = cluster.content_id.clone();
            }

            let index = index.finalize();
            if non_zero_clusters > 0 {
                wal.put(&index).await?;
            }

            Ok(index)
        }
        tracing::debug!("commit started");
        let mut inventory = self.inventory.write().await;

        let (new_commit, new_index) = match apply_changes(
            self.data,
            &self.previous_index,
            inventory.deref_mut(),
            &self.config,
            &mut self.wal_tx,
            &mut self.wal_blocks,
        )
        .await
        {
            Ok(index) => {
                let mut commit =
                    CommitMut::from_commit(self.config.specs.clone(), &self.previous_commit);
                commit.update_num_clusters(index.len());
                commit.update_index(index.content_id());
                let commit = commit.finalize();
                (commit, index)
            }
            Err(err) => {
                tracing::error!(error = %err, "commit failed, beginning rollback");
                // rolling back
                let wal = match self.wal_tx.rollback().await {
                    Ok(wal) => {
                        tracing::debug!("rollback succeeded");
                        Some(wal)
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "rollback failed");
                        None
                    }
                };
                drop(inventory);
                return Err((
                    err,
                    Committed {
                        commit: self.previous_commit,
                        index: self.previous_index,
                        wal,
                        inventory: self.inventory,
                    },
                ));
            }
        };

        match self.wal_tx.commit(&new_commit).await {
            Ok((wal, tx_details)) => {
                tracing::debug!(commit = %new_commit.content_id(), "wal commit succeeded");
                if let Err(err) = inventory.update_wal(&tx_details, wal.id()).await {
                    tracing::error!(error = %err, commit = %new_commit.content_id(), "wal sync failed after commit");
                }
                Ok({
                    drop(inventory);
                    Committed {
                        commit: new_commit,
                        index: new_index,
                        wal: Some(wal),
                        inventory: self.inventory,
                    }
                })
            }
            Err((e, rbr)) => {
                tracing::error!(commit = %new_commit.content_id(), error = %e, "commit failed");
                let wal = rbr.ok();
                drop(inventory);
                Err((
                    e.into(),
                    Committed {
                        commit: self.previous_commit,
                        index: self.previous_index,
                        wal,
                        inventory: self.inventory,
                    },
                ))
            }
        }
    }
}

impl VirtualBlockDevice {
    pub async fn new(
        max_write_buffer: usize,
        wal_dir: impl AsRef<Path>,
        max_wal_size: u64,
        max_tx_size: u64,
        db_file: impl AsRef<Path>,
        max_db_connections: u8,
        branch: BranchName,
        volume: VolumeHandler,
    ) -> Result<Self, anyhow::Error> {
        let max_tx_size = min(max_tx_size, max_wal_size - 1024 * 1024 * 10);
        if max_tx_size < 1024 * 1024 * 10 {
            bail!("max_tx_size too small");
        }

        let wal_dir = wal_dir.as_ref();
        match tokio::fs::try_exists(wal_dir).await {
            Ok(exists) if exists => {}
            _ => bail!(
                "WAL path {} is not a directory or does not exist",
                wal_dir.display()
            ),
        }

        let wal_man = Arc::new(WalMan::new(&wal_dir, max_wal_size));
        let name = volume.volume_info().name.clone();
        let inventory = Arc::new(RwLock::new(
            Inventory::new(
                db_file.as_ref(),
                max_db_connections,
                branch.clone(),
                wal_man.clone(),
                volume,
            )
            .await?,
        ));

        let lock = inventory.read().await;
        let zero_block = lock.specs().zero_block();
        let zero_cluster = lock.specs().zero_cluster();
        let config = Config {
            specs: lock.specs().clone(),
            zero_cluster,
            zero_block,
            max_tx_size,
            max_write_buffer,
            wal_dir: wal_dir.into(),
            max_wal_size,
            db_file: db_file.as_ref().to_path_buf(),
            max_db_connections,
        };

        let commit = lock.current_commit().clone();
        let index = lock
            .index_by_id(commit.index())
            .await?
            .ok_or(anyhow!("unable to load index [{}]", commit.index()))?;
        drop(lock);

        eprintln!("vbd id: {}", config.specs.vbd_id());
        if let Some(name) = name {
            eprintln!("vbd name: {}", name);
        }
        eprintln!("branch: {}", branch);
        eprintln!("commit: {} @ {}", commit.content_id(), commit.committed);
        eprintln!("index: {} @ {} clusters", index.content_id(), index.clusters.len());

        Ok(Self {
            config: Arc::new(config),
            state: State::Committed(Committed {
                commit,
                index,
                wal: None,
                inventory: inventory.clone(),
            }),
            inventory,
            wal_man,
        })
    }

    fn calc_cluster_block(&self, block_no: usize) -> Result<(usize, usize), BlockError> {
        match self.calc_clusters_blocks(block_no..(block_no + 1))? {
            vec if vec.len() == 1 => {
                let (cluster_no, range) = vec.into_iter().next().unwrap();
                if range.len() != 1 {
                    panic!("calc_clusters_blocks needs to return exactly 1 block")
                }
                Ok((cluster_no, range.start))
            }
            _ => panic!("calc_clusters_blocks needs to return exactly 1 match"),
        }
    }

    fn calc_clusters_blocks(
        &self,
        blocks: Range<usize>,
    ) -> Result<Vec<(usize, Range<usize>)>, BlockError> {
        let cluster_size = *self.config.specs.cluster_size();
        let mut clustered_blocks: Vec<(usize, Range<usize>)> = Vec::new();

        for block_no in blocks {
            let cluster_no = block_no / cluster_size;
            let relative_block_no = block_no % cluster_size;

            if cluster_no >= self.state.index().clusters.len() || relative_block_no >= cluster_size
            {
                return Err(BlockError::OutOfRange { block_no });
            }

            if let Some(last) = clustered_blocks.last_mut() {
                if last.0 == cluster_no {
                    last.1.end = relative_block_no + 1;
                    continue;
                }
            }

            clustered_blocks.push((cluster_no, relative_block_no..(relative_block_no + 1)));
        }

        Ok(clustered_blocks)
    }

    #[instrument(skip(self))]
    pub async fn get(&self, block_no: usize) -> Result<Option<Bytes>, BlockError> {
        tracing::trace!("get called");
        let (cluster_no, relative_block_no) = self.calc_cluster_block(block_no)?;

        // check pending transaction first
        if let State::Uncommitted(uncommitted) = &self.state {
            if let Some(data) = uncommitted.get(cluster_no, relative_block_no).await? {
                // return the pending data
                return Ok(data);
            }
        }

        let cluster_id = self.state.index().clusters.get(cluster_no).unwrap().clone();

        if &cluster_id == self.config.zero_cluster.content_id() {
            // the whole cluster is empty
            return Ok(None);
        }

        let lock = self.inventory.read().await;

        let cluster = lock
            .cluster_by_id(&cluster_id)
            .await?
            .ok_or_else(|| BlockError::ClusterNoFound { cluster_id })?;

        let block_id = &cluster
            .blocks
            .get(relative_block_no)
            .map(|id| id.clone())
            .expect("block_no in range");

        if block_id == self.config.zero_block.content_id() {
            // this is an empty block
            return Ok(None);
        }

        Ok(Some(
            lock.block_by_id(&block_id)
                .await?
                .ok_or_else(|| BlockError::BlockNotFound {
                    block_id: block_id.clone(),
                })?
                .data,
        ))
    }

    #[instrument(skip(self, data))]
    pub async fn put(&mut self, block_no: usize, data: Bytes) -> Result<(), BlockError> {
        tracing::trace!("put called");
        let (cluster_no, relative_block_no) = self.calc_cluster_block(block_no)?;
        if all_zeroes(data.as_ref()) {
            // use delete instead
            self.delete(block_no..(block_no + 1)).await?;
        } else {
            let uncommitted = self.prepare_writing().await?;
            uncommitted.put(cluster_no, relative_block_no, data).await?;
        }
        Ok(())
    }

    #[instrument(skip(self), fields(blocks = ?blocks))]
    pub async fn delete(&mut self, blocks: Range<usize>) -> Result<(), BlockError> {
        tracing::trace!("delete called");
        let clusters = self.calc_clusters_blocks(blocks)?;
        let uncommitted = self.prepare_writing().await?;

        for (cluster_no, blocks) in clusters {
            for block_no in blocks {
                uncommitted.delete(cluster_no, block_no);
            }
        }
        Ok(())
    }

    async fn prepare_writing(&mut self) -> Result<&mut Uncommitted, BlockError> {
        if let State::Uncommitted(state) = &self.state {
            if state.is_full() {
                self.commit().await?;
            }
        }

        if let State::Committed(_) = &self.state {
            match std::mem::replace(&mut self.state, State::Poisoned) {
                State::Committed(state) => {
                    match state.begin(self.config.clone(), self.wal_man.clone()).await {
                        Ok(uncommited) => {
                            self.state = State::Uncommitted(uncommited);
                        }
                        Err((e, commited)) => {
                            self.state = State::Committed(commited);
                            return Err(e);
                        }
                    }
                }
                _ => unreachable!(),
            }
        }

        Ok(match &mut self.state {
            State::Uncommitted(state) => state,
            _ => unreachable!(),
        })
    }

    async fn commit(&mut self) -> Result<(), BlockError> {
        if let State::Uncommitted(_) = self.state {
            match std::mem::replace(&mut self.state, State::Poisoned) {
                State::Uncommitted(state) => match state.try_commit().await {
                    Ok(committed) => self.state = State::Committed(committed),
                    Err((err, prev_commit)) => {
                        self.state = State::Committed(prev_commit);
                        return Err(err);
                    }
                },
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    #[instrument(skip(self))]
    pub async fn flush(&mut self) -> Result<(), BlockError> {
        tracing::debug!("flush called");
        // commit the pending transaction if there is one
        self.commit().await?;
        Ok(())
    }

    pub fn id(&self) -> VbdId {
        self.config.specs.vbd_id()
    }

    pub fn last_commit_id(&self) -> &CommitId {
        self.state.commit().content_id()
    }

    pub fn is_dirty(&self) -> bool {
        match &self.state {
            State::Committed(_) => false,
            State::Uncommitted(_) => true,
            _ => unreachable!("poisoned state"),
        }
    }

    pub fn block_size(&self) -> usize {
        *self.config.specs.block_size()
    }

    pub fn blocks(&self) -> usize {
        *self.config.specs.cluster_size() * self.state.index().len()
    }

    pub fn cluster_size(&self) -> usize {
        *self.config.specs.cluster_size()
    }

    pub fn total_size(&self) -> usize {
        self.blocks() * *self.config.specs.block_size()
    }
}

pub type ClusterId = ContentId<Cluster>;

#[derive(Clone)]
pub struct Block {
    content_id: BlockId,
    data: Bytes,
}

impl Block {
    pub fn from_bytes(specs: &FixedSpecs, bytes: impl Into<Bytes>) -> Self {
        let bytes = bytes.into();
        Self {
            content_id: Self::calc_content_id(specs, &bytes),
            data: bytes,
        }
    }

    pub fn zeroed(specs: &FixedSpecs) -> Self {
        Self::from_bytes(specs, BytesMut::zeroed(*specs.block_size()).freeze())
    }

    fn calc_content_id(specs: &FixedSpecs, bytes: &Bytes) -> BlockId {
        let mut hasher = specs.content_hash().new();
        hasher.update("--sia_vbd block hash v1 start--\n".as_bytes());
        hasher.update("vbd_id: ".as_bytes());
        hasher.update(specs.vbd_id().as_bytes());
        hasher.update("\nblock_size: ".as_bytes());
        hasher.update(bytes.len().to_be_bytes());
        hasher.update("hash_algorithm: ".as_bytes());
        hasher.update(specs.content_hash().as_str().as_bytes());
        hasher.update("\n content: \n".as_bytes());
        hasher.update(&bytes);
        hasher.update("\n--sia_vbd block hash v1 end--".as_bytes());
        hasher.finalize().into()
    }

    pub fn content_id(&self) -> &BlockId {
        &self.content_id
    }

    pub fn len(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn data(&self) -> &Bytes {
        &self.data
    }
}

#[derive(Clone)]
pub(crate) struct ClusterMut {
    blocks: Vec<BlockId>,
    specs: FixedSpecs,
}

impl ClusterMut {
    pub fn zeroed(specs: FixedSpecs) -> Self {
        let zero_block_id = specs.zero_block().content_id().clone();
        Self {
            blocks: (0..*specs.cluster_size())
                .map(|_| zero_block_id.clone())
                .collect(),
            specs,
        }
    }

    pub fn from_cluster(cluster: Cluster, specs: FixedSpecs) -> Self {
        Self {
            blocks: Arc::try_unwrap(cluster.blocks)
                .unwrap_or_else(|blocks| blocks.as_ref().clone()),
            specs,
        }
    }

    pub fn from_block_ids<I: Iterator<Item = BlockId>>(ids: I, specs: FixedSpecs) -> Self {
        Self {
            blocks: ids.into_iter().collect(),
            specs,
        }
    }

    pub fn blocks(&mut self) -> &mut Vec<BlockId> {
        &mut self.blocks
    }

    fn calc_content_id(&self) -> ClusterId {
        let mut hasher = self.specs.meta_hash().new();
        hasher.update("--sia_vbd cluster hash v1 start--\n".as_bytes());
        hasher.update("vbd_id: ".as_bytes());
        hasher.update(self.specs.vbd_id().as_bytes());
        hasher.update("\nnumber_of_blocks: ".as_bytes());
        hasher.update(self.blocks.len().to_be_bytes());
        hasher.update("\nblock_size: ");
        hasher.update(self.specs.block_size().to_be_bytes());
        hasher.update("\nhash_algorithm: ".as_bytes());
        hasher.update(self.specs.meta_hash().as_str().as_bytes());
        hasher.update("\n content: ".as_bytes());
        self.blocks.iter().enumerate().for_each(|(i, id)| {
            hasher.update("\n--block entry start--\n".as_bytes());
            hasher.update("block_no: ".as_bytes());
            hasher.update(i.to_be_bytes());
            hasher.update("\n block_id: ".as_bytes());
            hasher.update(id.as_ref());
            hasher.update("\n--block entry end--".as_bytes());
        });
        hasher.update("\n--sia_vbd cluster hash v1 end--".as_bytes());
        hasher.finalize().into()
    }

    pub fn finalize(self) -> Cluster {
        self.into()
    }
}

impl From<ClusterMut> for Cluster {
    fn from(value: ClusterMut) -> Self {
        let content_id = value.calc_content_id();
        Cluster {
            content_id,
            blocks: Arc::new(value.blocks),
        }
    }
}

#[derive(Clone)]
pub struct Cluster {
    content_id: ClusterId,
    blocks: Arc<Vec<BlockId>>,
}

impl Cluster {
    pub fn content_id(&self) -> &ClusterId {
        &self.content_id
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    pub fn block_ids(&self) -> impl Iterator<Item = &BlockId> {
        self.blocks.iter()
    }
}

#[derive(Clone)]
pub struct Index {
    content_id: IndexId,
    clusters: Arc<Vec<ClusterId>>,
}

impl Index {
    pub fn content_id(&self) -> &IndexId {
        &self.content_id
    }

    pub fn len(&self) -> usize {
        self.clusters.len()
    }

    pub fn cluster_ids(&self) -> impl Iterator<Item = &ClusterId> {
        self.clusters.iter()
    }
}

#[derive(Clone)]
pub(crate) struct IndexMut {
    clusters: Vec<ClusterId>,
    specs: FixedSpecs,
}

impl IndexMut {
    pub fn zeroed(specs: FixedSpecs, num_clusters: usize) -> Self {
        assert!(num_clusters > 0);
        let zero_cluster_id = specs.zero_cluster().content_id().clone();
        Self {
            clusters: (0..num_clusters).map(|_| zero_cluster_id.clone()).collect(),
            specs,
        }
    }

    pub fn from_index(index: Index, specs: FixedSpecs) -> Self {
        Self {
            clusters: Arc::try_unwrap(index.clusters)
                .unwrap_or_else(|clusters| clusters.as_ref().clone()),
            specs,
        }
    }

    pub fn from_cluster_ids<I: Iterator<Item = ClusterId>>(ids: I, specs: FixedSpecs) -> Self {
        Self {
            clusters: ids.into_iter().collect(),
            specs,
        }
    }

    pub fn clusters(&mut self) -> &mut Vec<ClusterId> {
        &mut self.clusters
    }

    fn calc_content_id(&self) -> IndexId {
        let mut hasher = self.specs.meta_hash().new();
        hasher.update("--sia_vbd index hash v1 start--\n".as_bytes());
        hasher.update("vbd_id: ".as_bytes());
        hasher.update(&self.specs.vbd_id().as_bytes());
        hasher.update("\nblock_size: ".as_bytes());
        hasher.update(&self.specs.block_size().to_be_bytes());
        hasher.update("\ncluster_size: ".as_bytes());
        hasher.update(&self.specs.cluster_size().to_be_bytes());
        hasher.update("number_of_clusters: ".as_bytes());
        hasher.update(&self.clusters.len().to_be_bytes());
        hasher.update("\ncontent: ".as_bytes());
        self.clusters.iter().enumerate().for_each(|(i, c)| {
            hasher.update("\n--cluster entry start--\n".as_bytes());
            hasher.update("cluster no: ".as_bytes());
            hasher.update(i.to_be_bytes());
            hasher.update("\n cluster hash: ".as_bytes());
            hasher.update(c.as_ref());
            hasher.update("\n--cluster entry end--".as_bytes());
        });
        hasher.update("\n--sia_vbd index hash v1 end--".as_bytes());
        hasher.finalize().into()
    }

    pub fn finalize(self) -> Index {
        self.into()
    }
}

impl From<IndexMut> for Index {
    fn from(value: IndexMut) -> Self {
        let content_id = value.calc_content_id();
        Index {
            content_id,
            clusters: Arc::new(value.clusters),
        }
    }
}

pub type IndexId = ContentId<Index>;

pub type CommitId = ContentId<Commit>;

#[derive(Clone, PartialEq, Eq)]
pub struct Commit {
    pub(crate) content_id: CommitId,
    pub(crate) preceding_commit: CommitId,
    pub(crate) committed: DateTime<Utc>,
    pub(crate) index: IndexId,
    pub(crate) num_clusters: usize,
}

impl Commit {
    pub fn content_id(&self) -> &CommitId {
        &self.content_id
    }

    pub fn preceding_commit(&self) -> &CommitId {
        &self.preceding_commit
    }

    pub fn committed(&self) -> &DateTime<Utc> {
        &self.committed
    }

    pub fn index(&self) -> &IndexId {
        &self.index
    }

    pub fn num_clusters(&self) -> usize {
        self.num_clusters
    }
}

impl PartialOrd for Commit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.committed.partial_cmp(&other.committed)
    }
}

#[derive(Clone)]
pub(crate) struct CommitMut {
    preceding_commit: CommitId,
    timestamp: DateTime<Utc>,
    index: IndexId,
    num_clusters: usize,
    specs: FixedSpecs,
}

impl CommitMut {
    pub fn zeroed(specs: FixedSpecs, num_clusters: usize) -> Self {
        let preceding_commit = specs
            .meta_hash()
            .hash(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
            .into();
        let zero_index = specs.zero_index(num_clusters);

        Self {
            preceding_commit,
            timestamp: Utc::now(),
            index: zero_index.content_id,
            num_clusters,
            specs,
        }
    }

    pub fn from_commit(specs: FixedSpecs, commit: &Commit) -> Self {
        Self {
            preceding_commit: commit.content_id.clone(),
            timestamp: commit.committed.clone(),
            index: commit.index.clone(),
            num_clusters: commit.num_clusters,
            specs,
        }
    }

    pub fn update_index(&mut self, index: &IndexId) {
        self.index = index.clone();
    }

    pub fn update_num_clusters(&mut self, num_clusters: usize) {
        self.num_clusters = num_clusters;
    }

    fn calc_content_id(&self) -> CommitId {
        let mut hasher = self.specs.meta_hash().new();
        hasher.update("--sia_vbd commit hash v1 start--\n".as_bytes());
        hasher.update("vbd_id: ".as_bytes());
        hasher.update(&self.specs.vbd_id().as_bytes());
        hasher.update("\nblock_size: ".as_bytes());
        hasher.update(&self.specs.block_size().to_be_bytes());
        hasher.update("\ncluster_size: ".as_bytes());
        hasher.update(&self.specs.cluster_size().to_be_bytes());
        hasher.update("number_of_clusters: ".as_bytes());
        hasher.update(&self.num_clusters.to_be_bytes());
        hasher.update("\ncontent: ".as_bytes());
        hasher.update("\nindex hash: ".as_bytes());
        hasher.update(self.index.as_ref());
        hasher.update("\n preceding index hash: ".as_bytes());
        hasher.update(self.preceding_commit.as_ref());
        hasher.update("\n timestamp: ".as_bytes());
        hasher.update(self.timestamp.timestamp_micros().to_be_bytes());
        hasher.update("\n--sia_vbd commit hash v1 end--".as_bytes());
        hasher.finalize().into()
    }

    pub fn finalize(mut self) -> Commit {
        self.timestamp = Utc::now();
        self.into()
    }
}

impl From<CommitMut> for Commit {
    fn from(value: CommitMut) -> Self {
        let content_id = value.calc_content_id();
        Commit {
            content_id,
            preceding_commit: value.preceding_commit,
            committed: value.timestamp,
            index: value.index,
            num_clusters: value.num_clusters,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct BranchName(String);

impl AsRef<str> for BranchName {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

impl Display for BranchName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.to_string())
    }
}

impl TryFrom<String> for BranchName {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if is_valid_branch_name(&value) {
            Ok(BranchName(value))
        } else {
            Err(anyhow!("invalid branch name"))
        }
    }
}

impl TryFrom<&String> for BranchName {
    type Error = anyhow::Error;

    fn try_from(value: &String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

impl TryFrom<&str> for BranchName {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if is_valid_branch_name(value) {
            Ok(BranchName(value.to_string()))
        } else {
            Err(anyhow!("invalid branch name"))
        }
    }
}

fn is_valid_branch_name(s: impl AsRef<str>) -> bool {
    let s = s.as_ref();
    if s.is_empty() || s.len() > 255 {
        return false;
    }
    if s.starts_with('.') || s.ends_with('.') {
        return false;
    }
    if s.contains("..") {
        return false;
    }
    s.chars()
        .all(|c| c.is_alphanumeric() || c == '-' || c == '.' || c == '_')
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum ClusterSize {
    Cs256,
}

impl TryFrom<usize> for ClusterSize {
    type Error = anyhow::Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        match value {
            256 => Ok(ClusterSize::Cs256),
            _ => Err(anyhow!("invalid cluster size: {}", value)),
        }
    }
}

impl Deref for ClusterSize {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        match self {
            ClusterSize::Cs256 => &(256),
        }
    }
}

impl FromStr for ClusterSize {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "256" => Ok(ClusterSize::Cs256),
            _ => Err(anyhow!("'{}' not a valid or supported cluster size.", s)),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum BlockSize {
    Bs16k,
    Bs64k,
    Bs256k,
}

impl TryFrom<usize> for BlockSize {
    type Error = anyhow::Error;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        match value {
            BS16K => Ok(BlockSize::Bs16k),
            BS64K => Ok(BlockSize::Bs64k),
            BS256K => Ok(BlockSize::Bs256k),
            _ => Err(anyhow!("unsupported block size: {}", value)),
        }
    }
}

impl Deref for BlockSize {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        match self {
            BlockSize::Bs16k => &BS16K,
            BlockSize::Bs64k => &BS64K,
            BlockSize::Bs256k => &BS256K,
        }
    }
}

impl FromStr for BlockSize {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "16" => Ok(BlockSize::Bs16k),
            "64" => Ok(BlockSize::Bs64k),
            "256" => Ok(BlockSize::Bs256k),
            _ => Err(anyhow!("'{}' not a valid or supported block size.", s)),
        }
    }
}

pub type BlockId = ContentId<Block>;

#[derive(Clone, Debug)]
pub struct Position<O, L> {
    pub offset: O,
    pub length: L,
}

impl<O, L> Position<O, L> {
    pub fn new(offset: O, length: L) -> Self {
        Self { offset, length }
    }
}

/// Returns true if the supplied byte slice only contains `0x00`
fn all_zeroes(buffer: &[u8]) -> bool {
    let len = buffer.len();
    let ptr = buffer.as_ptr();
    let num_words = len / 8;
    unsafe {
        for i in 0..num_words {
            if *(ptr.add(i * 8) as *const u64) != 0 {
                return false;
            }
        }
    }
    buffer[num_words * 8..].iter().all(|&b| b == 0)
}

#[derive(Error, Debug)]
pub(crate) enum BlockError {
    #[error("data length {data_len} does not correspond to block size {block_size}")]
    InvalidDataLength { block_size: usize, data_len: usize },
    #[error("the given block number {block_no} is out of range")]
    OutOfRange { block_no: usize },
    #[error("block with id {block_id} not found")]
    BlockNotFound { block_id: BlockId },
    #[error("cluster with id {cluster_id} not found")]
    ClusterNoFound { cluster_id: ClusterId },
    #[error(transparent)]
    WalError(#[from] WalError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

mod protos {
    use crate::hash::Hash;
    use crate::vbd::{BlockId, ClusterId, Commit, CommitId, IndexId, VbdId};
    use anyhow::anyhow;
    use uuid::Uuid;

    impl From<VbdId> for crate::serde::protos::Uuid {
        fn from(value: VbdId) -> Self {
            value.0.into()
        }
    }

    impl From<crate::serde::protos::Uuid> for VbdId {
        fn from(value: crate::serde::protos::Uuid) -> Self {
            Into::<Uuid>::into(value).into()
        }
    }

    impl From<IndexId> for crate::serde::protos::Hash {
        fn from(value: IndexId) -> Self {
            value.0.into()
        }
    }

    impl From<&IndexId> for crate::serde::protos::Hash {
        fn from(value: &IndexId) -> Self {
            (&value.0).into()
        }
    }

    impl TryFrom<crate::serde::protos::Hash> for IndexId {
        type Error = <Hash as TryFrom<crate::serde::protos::Hash>>::Error;

        fn try_from(value: crate::serde::protos::Hash) -> Result<Self, Self::Error> {
            TryInto::<Hash>::try_into(value).map(|h| h.into())
        }
    }

    impl From<&BlockId> for crate::serde::protos::Hash {
        fn from(value: &BlockId) -> Self {
            (&value.0).into()
        }
    }

    impl TryFrom<crate::serde::protos::Hash> for BlockId {
        type Error = <Hash as TryFrom<crate::serde::protos::Hash>>::Error;

        fn try_from(value: crate::serde::protos::Hash) -> Result<Self, Self::Error> {
            TryInto::<Hash>::try_into(value).map(|h| h.into())
        }
    }

    impl From<&ClusterId> for crate::serde::protos::Hash {
        fn from(value: &ClusterId) -> Self {
            (&value.0).into()
        }
    }

    impl TryFrom<crate::serde::protos::Hash> for ClusterId {
        type Error = <Hash as TryFrom<crate::serde::protos::Hash>>::Error;

        fn try_from(value: crate::serde::protos::Hash) -> Result<Self, Self::Error> {
            TryInto::<Hash>::try_into(value).map(|h| h.into())
        }
    }

    impl TryFrom<crate::serde::protos::Hash> for CommitId {
        type Error = <Hash as TryFrom<crate::serde::protos::Hash>>::Error;

        fn try_from(value: crate::serde::protos::Hash) -> Result<Self, Self::Error> {
            TryInto::<Hash>::try_into(value).map(|h| h.into())
        }
    }

    impl From<&CommitId> for crate::serde::protos::Hash {
        fn from(value: &CommitId) -> Self {
            (&value.0).into()
        }
    }

    impl TryFrom<crate::serde::protos::Commit> for Commit {
        type Error = anyhow::Error;

        fn try_from(value: crate::serde::protos::Commit) -> Result<Self, Self::Error> {
            Ok(Self {
                content_id: value
                    .cid
                    .map(|c| c.try_into())
                    .transpose()?
                    .ok_or(anyhow!("Invalid commit message"))?,
                preceding_commit: value
                    .preceding_cid
                    .map(|c| c.try_into())
                    .transpose()?
                    .ok_or(anyhow!("invalid commit message"))?,
                committed: value
                    .committed
                    .map(|c| c.try_into())
                    .transpose()?
                    .ok_or(anyhow!("invalid commit message"))?,
                index: value
                    .index_id
                    .map(|i| i.try_into())
                    .transpose()?
                    .ok_or(anyhow!("invalid commit message"))?,
                num_clusters: value.clusters as usize,
            })
        }
    }
}
