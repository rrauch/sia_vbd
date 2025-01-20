pub mod nbd_device;
pub(crate) mod wal;

use crate::hash::{Hash, HashAlgorithm};
use crate::vbd::wal::{
    Position, RollbackError, TokioFileWal, WalError, WalId, WalReader, WalWriter,
};
use anyhow::{anyhow, bail};
use bytes::{Bytes, BytesMut};
use std::cmp::min;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hasher;
use std::marker::PhantomData;
use std::ops::{Deref, Range};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_util::compat::TokioAsyncReadCompatExt;
use uuid::Uuid;

const BS16K: usize = 16 * 1024;
const BS64K: usize = 64 * 1024;
const BS256K: usize = 256 * 1024;

pub(crate) struct ContentId<T>(Hash, PhantomData<T>);

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

pub type Commit = ContentId<State>;
pub type VbdId = TypedUuid<VirtualBlockDevice>;

pub struct VirtualBlockDevice {
    config: Arc<Config>,
    state: State,
}

struct Config {
    specs: FixedSpecs,
    zero_cluster_id: ClusterId,
    zero_block: Block,
    max_tx_size: u64,
    max_write_buffer: usize,
    wal_dir: PathBuf,
    max_wal_size: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct FixedSpecs {
    pub vbd_id: VbdId,
    pub cluster_size: ClusterSize,
    pub block_size: BlockSize,
    pub content_hash: HashAlgorithm,
    pub meta_hash: HashAlgorithm,
}

pub(crate) enum State {
    Committed(Committed),
    Uncommitted(Uncommitted),
    Poisoned,
}

#[derive(Clone)]
struct Clusters {
    clusters: Vec<ClusterId>,
    cluster_map: HashMap<ClusterId, Arc<Cluster>>,
    blocks: HashMap<BlockId, Block>,
}

impl From<(&Vec<ClusterId>, &FixedSpecs)> for Commit {
    fn from((clusters, specs): (&Vec<ClusterId>, &FixedSpecs)) -> Self {
        let mut hasher = specs.meta_hash.new();
        hasher.update("--sia_vbd commit hash v1 start--\n".as_bytes());
        hasher.update("uuid: ".as_bytes());
        hasher.update(specs.vbd_id.as_bytes());
        hasher.update("\nblock_size: ".as_bytes());
        hasher.update(specs.block_size.to_be_bytes());
        hasher.update("\ncluster_size: ".as_bytes());
        hasher.update(specs.cluster_size.to_be_bytes());
        hasher.update("number_of_clusters: ".as_bytes());
        hasher.update(clusters.len().to_be_bytes());
        hasher.update("\ncontent: ".as_bytes());
        clusters.iter().enumerate().for_each(|(i, c)| {
            hasher.update("\n--cluster entry start--\n".as_bytes());
            hasher.update("cluster no: ".as_bytes());
            hasher.update(i.to_be_bytes());
            hasher.update("\n cluster hash: ".as_bytes());
            hasher.update(c.as_ref());
            hasher.update("\n--cluster entry end--".as_bytes());
        });
        hasher.update("\n--sia_vbd commit hash v1 end--".as_bytes());
        hasher.finalize().into()
    }
}

impl Clusters {
    fn calc_commit(&self, config: &Config) -> Commit {
        (&self.clusters, &config.specs).into()
    }

    async fn get(&self, cluster_no: usize) -> Result<Arc<Cluster>, BlockError> {
        let cluster_id = self
            .clusters
            .get(cluster_no)
            .expect("cluster_no to be in range 0");
        Ok(self
            .cluster_map
            .get(cluster_id)
            .map(|c| c.clone())
            .ok_or_else(|| BlockError::ClusterNoFound {
                cluster_id: cluster_id.clone(),
            })?)
    }
}

struct Committed {
    commit: Commit,
    clusters: Clusters,
    wal: Option<(WalWriter<TokioFileWal>, PathBuf)>,
}

impl Committed {
    async fn begin(mut self, config: Arc<Config>) -> Result<Uncommitted, (BlockError, Self)> {
        async fn prepare_wal(
            mut wal: Option<(WalWriter<TokioFileWal>, PathBuf)>,
            config: &Config,
        ) -> Result<(WalWriter<TokioFileWal>, PathBuf), BlockError> {
            let mut previous_wal_id = None;
            if let Some((existing_wal, wal_path)) = wal.take() {
                if existing_wal.remaining().await? <= config.max_tx_size {
                    previous_wal_id = Some(existing_wal.id().clone());
                } else {
                    wal = Some((existing_wal, wal_path));
                }
            };

            if wal.is_none() {
                // start a new wal
                let wal_id: WalId = Uuid::now_v7().into();
                let wal_path = config.wal_dir.join(format!("{}.wal", wal_id));
                let wal_file = tokio::fs::File::create_new(&wal_path).await?;
                wal = Some((
                    WalWriter::new(
                        TokioFileWal::new(wal_file),
                        wal_id,
                        previous_wal_id,
                        config.max_wal_size,
                        config.specs.clone(),
                    )
                    .await?,
                    wal_path,
                ));
            }

            Ok(wal.expect("wal should be Some"))
        }

        let (wal, wal_path) = match prepare_wal(self.wal.take(), &config).await {
            Ok(wal) => wal,
            Err(err) => {
                return Err((err, self));
            }
        };

        match Uncommitted::new(wal, wal_path, self.commit.clone(), self.clusters, config).await {
            Ok(u) => Ok(u),
            Err((err, (clusters, wal))) => {
                let wal = wal.ok();
                Err((
                    err,
                    Self {
                        commit: self.commit,
                        clusters,
                        wal,
                    },
                ))
            }
        }
    }
}

struct Uncommitted {
    previous_commit: Commit,
    committed_clusters: Clusters,
    config: Arc<Config>,
    last_modified: Instant,
    data: ModifiedData,
    wal_tx: wal::Tx<TokioFileWal>,
    wal_path: PathBuf,
    wal_reader: Mutex<Option<WalReader<tokio_util::compat::Compat<tokio::fs::File>>>>,
    wal_blocks: HashMap<BlockId, Position<u64, u32>>,
}

struct ModifiedData {
    unflushed_blocks: HashMap<(usize, usize), Option<Bytes>>,
    clusters: HashMap<usize, ClusterMut>,
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
            .map(|c| c.blocks.get(block_no))
            .flatten()
        {
            return Some(ModifiedBlock::BlockId(block_id.clone()));
        }

        None
    }

    fn put(&mut self, cluster_no: usize, block_no: usize, data: Option<Bytes>) {
        self.unflushed_blocks.insert((cluster_no, block_no), data);
    }

    async fn flush(
        &mut self,
        config: &Config,
        committed_clusters: &Clusters,
        wal: &mut wal::Tx<TokioFileWal>,
        wal_blocks: &mut HashMap<BlockId, Position<u64, u32>>,
    ) -> Result<(), BlockError> {
        for ((cluster_no, block_no), data) in self.unflushed_blocks.drain() {
            let block = data.map(|d| Block::new(&config.specs, d)).transpose()?;

            let block_id = match block {
                Some(block) => {
                    let block_id = block.content_id.clone();
                    wal_blocks.insert(
                        block_id.clone(),
                        wal.block(block.content_id(), &block.data).await?,
                    );
                    block_id
                }
                None => config.zero_block.content_id.clone(),
            };

            if !self.clusters.contains_key(&cluster_no) {
                let c = committed_clusters.get(cluster_no).await?;
                self.clusters.insert(
                    cluster_no,
                    ClusterMut::from_cluster(c.as_ref().clone(), config.specs.clone()),
                );
            }

            let cluster = self.clusters.get_mut(&cluster_no).unwrap();
            cluster.blocks[block_no] = block_id;
        }
        Ok(())
    }
}

impl Uncommitted {
    async fn new(
        wal: WalWriter<TokioFileWal>,
        wal_path: PathBuf,
        previous_commit: Commit,
        committed_clusters: Clusters,
        config: Arc<Config>,
    ) -> Result<
        Self,
        (
            BlockError,
            (
                Clusters,
                Result<(WalWriter<TokioFileWal>, PathBuf), RollbackError>,
            ),
        ),
    > {
        let wal_tx = match wal.begin(previous_commit.clone(), config.max_tx_size).await {
            Ok(wal) => wal,
            Err((e, wal)) => {
                return Err((e.into(), (committed_clusters, wal.map(|w| (w, wal_path)))));
            }
        };

        Ok(Self {
            previous_commit,
            committed_clusters,
            config,
            data: ModifiedData {
                unflushed_blocks: HashMap::default(),
                clusters: HashMap::default(),
            },
            last_modified: Instant::now(),
            wal_tx,
            wal_blocks: HashMap::default(),
            wal_path,
            wal_reader: Mutex::new(None),
        })
    }

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
                    if let Some(pos) = self.wal_blocks.get(&block_id) {
                        let mut lock = self.wal_reader.lock().await;
                        if lock.is_none() {
                            *lock = Some(
                                WalReader::new(
                                    tokio::fs::File::open(&self.wal_path).await?.compat(),
                                )
                                .await?,
                            )
                        }

                        let wal_reader = lock.as_mut().unwrap();
                        wal_reader.block(&block_id, pos).await.map(|b| Some(b.data))?
                    } else {
                        todo!()
                    }
                }
            }));
        }
        Ok(None)
    }

    async fn put(
        &mut self,
        cluster_no: usize,
        block_no: usize,
        data: Bytes,
    ) -> Result<(), BlockError> {
        if self.buffered_size() + data.len() >= self.config.max_write_buffer {
            // time to flush to wal
            self.data
                .flush(
                    &self.config,
                    &self.committed_clusters,
                    &mut self.wal_tx,
                    &mut self.wal_blocks,
                )
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
        self.wal_tx.remaining() == 0
    }

    async fn try_commit(mut self) -> Result<Committed, (BlockError, Committed)> {
        async fn apply_changes(
            mut modified_data: ModifiedData,
            committed_clusters: &Clusters,
            config: &Config,
            wal: &mut wal::Tx<TokioFileWal>,
            wal_blocks: &mut HashMap<BlockId, Position<u64, u32>>,
        ) -> Result<(Commit, Clusters), BlockError> {
            modified_data
                .flush(config, committed_clusters, wal, wal_blocks)
                .await?;
            let mut new_clusters = committed_clusters.clone();

            for (cluster_no, cluster) in modified_data.clusters.drain() {
                let cluster = cluster.finalize();
                wal.cluster(&cluster).await?;
                new_clusters.clusters[cluster_no] = cluster.content_id.clone();
                if !new_clusters.cluster_map.contains_key(cluster.content_id()) {
                    new_clusters
                        .cluster_map
                        .insert(cluster.content_id.clone(), Arc::new(cluster));
                }
            }

            let commit = new_clusters.calc_commit(config);
            wal.state(&commit, new_clusters.clusters.iter()).await?;

            Ok((commit, new_clusters))
        }

        let (new_commit, clusters) = match apply_changes(
            self.data,
            &self.committed_clusters,
            &self.config,
            &mut self.wal_tx,
            &mut self.wal_blocks,
        )
        .await
        {
            Ok(c) => c,
            Err(err) => {
                // rolling back
                let wal = match self.wal_tx.rollback().await {
                    Ok(wal) => Some(wal),
                    Err(_re) => {
                        //todo: logging
                        None
                    }
                };
                return Err((
                    err,
                    Committed {
                        commit: self.previous_commit,
                        clusters: self.committed_clusters,
                        wal: wal.map(|w| (w, self.wal_path)),
                    },
                ));
            }
        };

        match self.wal_tx.commit(&new_commit).await {
            Ok((wal, tx_details)) => {
                match wal
                    .io()
                    .as_file()
                    .metadata()
                    .await
                    .map(|m| (m.len(), m.modified().expect("access to last_modified")))
                {
                    Ok((file_size, file_last_modified)) => {
                        //todo: tx_details
                    }
                    Err(err) => {
                        todo!()
                    }
                }
                Ok(Committed {
                    commit: new_commit,
                    clusters,
                    wal: Some((wal, self.wal_path)),
                })
            }
            Err((e, rbr)) => {
                let wal = rbr.ok();
                Err((
                    e.into(),
                    Committed {
                        commit: self.previous_commit,
                        clusters: self.committed_clusters,
                        wal: wal.map(|w| (w, self.wal_path)),
                    },
                ))
            }
        }
    }

    async fn rollback(self) -> Committed {
        let wal = self.wal_tx.rollback().await.ok();
        Committed {
            commit: self.previous_commit,
            clusters: self.committed_clusters,
            wal: wal.map(|w| (w, self.wal_path)),
        }
    }
}

impl VirtualBlockDevice {
    pub async fn new(
        vbd_id: VbdId,
        cluster_size: ClusterSize,
        num_clusters: usize,
        block_size: BlockSize,
        content_hash: HashAlgorithm,
        meta_hash: HashAlgorithm,
        max_write_buffer: usize,
        wal_dir: impl AsRef<Path>,
        max_wal_size: u64,
        max_tx_size: u64,
    ) -> Result<Self, anyhow::Error> {
        assert!(num_clusters > 0);
        let max_tx_size = min(max_tx_size, max_wal_size - 1024 * 1024 * 10);
        if max_tx_size < 1024 * 1024 * 10 {
            bail!("max_tx_size too small");
        }

        let specs = FixedSpecs {
            vbd_id,
            cluster_size,
            block_size,
            content_hash,
            meta_hash,
        };

        let zero_block = Block::new(&specs, BytesMut::zeroed(*block_size).freeze())?;

        let zero_block_id = zero_block.content_id().clone();

        let blocks = [(zero_block_id.clone(), zero_block.clone())]
            .into_iter()
            .collect();

        let zero_cluster = Arc::new(ClusterMut::new_empty(specs.clone(), zero_block_id).finalize());
        let zero_cluster_id = zero_cluster.content_id().clone();
        let clusters = (0..num_clusters).map(|_| zero_cluster_id.clone()).collect();
        let cluster_map = [(zero_cluster_id.clone(), zero_cluster.clone())]
            .into_iter()
            .collect();

        let wal_dir = wal_dir.as_ref();
        let meta_data = tokio::fs::metadata(wal_dir).await?;
        if !meta_data.is_dir() {
            bail!(
                "Path {} is not a directory or does not exist",
                wal_dir.display()
            );
        }

        let config = Config {
            specs,
            zero_cluster_id,
            zero_block,
            max_tx_size,
            max_write_buffer,
            wal_dir: wal_dir.into(),
            max_wal_size,
        };

        let clusters = Clusters {
            clusters,
            cluster_map,
            blocks,
        };

        let commit = clusters.calc_commit(&config);

        eprintln!("vbd id: {}", config.specs.vbd_id);
        eprintln!("commit: {}", &commit);

        Ok(Self {
            config: Arc::new(config),
            state: State::Committed(Committed {
                commit,
                clusters,
                wal: None,
            }),
        })
    }

    fn clusters(&self) -> &Clusters {
        match &self.state {
            State::Committed(s) => &s.clusters,
            State::Uncommitted(s) => &s.committed_clusters,
            State::Poisoned => unreachable!("poisoned state"),
        }
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
        let cluster_size = *self.config.specs.cluster_size;
        let mut clustered_blocks: Vec<(usize, Range<usize>)> = Vec::new();

        for block_no in blocks {
            let cluster_no = block_no / cluster_size;
            let relative_block_no = block_no % cluster_size;

            if cluster_no >= self.clusters().clusters.len() || relative_block_no >= cluster_size {
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

    pub async fn get(&self, block_no: usize) -> Result<Option<Bytes>, BlockError> {
        let (cluster_no, relative_block_no) = self.calc_cluster_block(block_no)?;

        // check pending transaction first
        if let State::Uncommitted(uncommitted) = &self.state {
            if let Some(data) = uncommitted.get(cluster_no, relative_block_no).await? {
                // return the pending data
                return Ok(data);
            }
        }

        let cluster_id = self.clusters().clusters[cluster_no].clone();

        if cluster_id == self.config.zero_cluster_id {
            // the whole cluster is empty
            return Ok(None);
        }

        let block_id = self
            .clusters()
            .cluster_map
            .get(&cluster_id)
            .ok_or_else(|| BlockError::ClusterNoFound { cluster_id })?
            .blocks
            .get(relative_block_no)
            .expect("block_no in range");

        if block_id == self.config.zero_block.content_id() {
            // this is an empty block
            return Ok(None);
        }

        Ok(Some(
            self.clusters()
                .blocks
                .get(block_id)
                .ok_or_else(|| {
                    println!(
                        "foo: {} ({}, {}): {}",
                        block_no, cluster_no, relative_block_no, block_id
                    );
                    BlockError::BlockNotFound {
                        block_id: block_id.clone(),
                    }
                })
                .map(|block| block.data.clone())?,
        ))
    }

    pub async fn put(&mut self, block_no: usize, data: Bytes) -> Result<(), BlockError> {
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

    pub async fn delete(&mut self, blocks: Range<usize>) -> Result<(), BlockError> {
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
                State::Committed(state) => match state.begin(self.config.clone()).await {
                    Ok(uncommited) => {
                        self.state = State::Uncommitted(uncommited);
                    }
                    Err((e, commited)) => {
                        self.state = State::Committed(commited);
                        return Err(e);
                    }
                },
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

    pub async fn flush(&mut self) -> Result<(), BlockError> {
        // commit the pending transaction if there is one
        self.commit().await?;
        Ok(())
    }

    pub fn id(&self) -> VbdId {
        self.config.specs.vbd_id
    }

    pub fn last_commit(&self) -> &Commit {
        match &self.state {
            State::Committed(c) => &c.commit,
            State::Uncommitted(u) => &u.previous_commit,
            _ => unreachable!("poisoned state"),
        }
    }

    pub fn is_dirty(&self) -> bool {
        match &self.state {
            State::Committed(_) => false,
            State::Uncommitted(_) => true,
            _ => unreachable!("poisoned state"),
        }
    }

    pub fn block_size(&self) -> usize {
        *self.config.specs.block_size
    }

    pub fn blocks(&self) -> usize {
        *self.config.specs.cluster_size * self.clusters().clusters.len()
    }

    pub fn cluster_size(&self) -> usize {
        *self.config.specs.cluster_size
    }

    pub fn total_size(&self) -> usize {
        self.blocks() * *self.config.specs.block_size
    }
}

pub(crate) type ClusterId = ContentId<Cluster>;

#[derive(Clone)]
pub(crate) struct Block {
    content_id: BlockId,
    data: Bytes,
}

impl Block {
    fn new(specs: &FixedSpecs, data: Bytes) -> Result<Self, BlockError> {
        let content_id = BlockId::new(specs, &data)?;
        Ok(Self { content_id, data })
    }

    pub fn content_id(&self) -> &BlockId {
        &self.content_id
    }
}

#[derive(Clone)]
struct ClusterMut {
    blocks: Vec<BlockId>,
    specs: FixedSpecs,
}

impl ClusterMut {
    pub fn new_empty(specs: FixedSpecs, zero_block_id: BlockId) -> Self {
        Self {
            blocks: (0..*specs.cluster_size)
                .map(|_| zero_block_id.clone())
                .collect(),
            specs,
        }
    }

    pub fn from_cluster(cluster: Cluster, specs: FixedSpecs) -> Self {
        Self {
            blocks: cluster.blocks,
            specs,
        }
    }

    pub fn from_block_ids<I: Iterator<Item = BlockId>>(ids: I, specs: FixedSpecs) -> Self {
        Self {
            blocks: ids.into_iter().collect(),
            specs,
        }
    }

    fn calc_content_id(&self) -> ClusterId {
        let mut hasher = self.specs.meta_hash.new();
        hasher.update("--sia_vbd cluster hash v1 start--\n".as_bytes());
        hasher.update("uuid: ".as_bytes());
        hasher.update(self.specs.vbd_id.as_bytes());
        hasher.update("\nnumber_of_blocks: ".as_bytes());
        hasher.update(self.blocks.len().to_be_bytes());
        hasher.update("\nblock_size: ");
        hasher.update(self.specs.block_size.to_be_bytes());
        hasher.update("\nhash_algorithm: ".as_bytes());
        hasher.update(self.specs.meta_hash.as_str().as_bytes());
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
            blocks: value.blocks,
        }
    }
}

#[derive(Clone)]
pub(crate) struct Cluster {
    content_id: ClusterId,
    blocks: Vec<BlockId>,
}

impl Cluster {
    pub fn content_id(&self) -> &ClusterId {
        &self.content_id
    }
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

type BlockId = ContentId<Block>;

impl BlockId {
    fn new<D: AsRef<[u8]>>(specs: &FixedSpecs, data: D) -> Result<Self, BlockError> {
        let block_size = *specs.block_size;
        let data = data.as_ref();
        if data.len() != block_size {
            return Err(BlockError::InvalidDataLength {
                block_size,
                data_len: data.len(),
            });
        }

        let mut hasher = specs.content_hash.new();
        hasher.update("--sia_vbd block hash v1 start--\n".as_bytes());
        hasher.update("uuid: ".as_bytes());
        hasher.update(specs.vbd_id.as_bytes());
        hasher.update("\nblock_size: ".as_bytes());
        hasher.update(block_size.to_be_bytes());
        hasher.update("hash_algorithm: ".as_bytes());
        hasher.update(specs.content_hash.as_str().as_bytes());
        hasher.update("\n content: \n".as_bytes());
        hasher.update(data);
        hasher.update("\n--sia_vbd block hash v1 end--".as_bytes());

        Ok(Self(hasher.finalize(), PhantomData::default()))
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
enum BlockError {
    #[error("data length {data_len} does not correspond to block size {block_size}")]
    InvalidDataLength { block_size: usize, data_len: usize },
    #[error("the given block number {block_no} is out of range")]
    OutOfRange { block_no: usize },
    #[error("block with id {block_id} not found")]
    BlockNotFound { block_id: BlockId },
    #[error("cluster with id {cluster_id} not found")]
    ClusterNoFound { cluster_id: ClusterId },
    #[error("WAL error")]
    WalError(#[from] wal::WalError),
    #[error("IO error")]
    IoError(#[from] std::io::Error),
}

mod protos {
    use crate::hash::Hash;
    use crate::vbd::{BlockId, ClusterId, Commit, VbdId};
    use uuid::Uuid;

    impl From<VbdId> for crate::protos::Uuid {
        fn from(value: VbdId) -> Self {
            value.0.into()
        }
    }

    impl From<crate::protos::Uuid> for VbdId {
        fn from(value: crate::protos::Uuid) -> Self {
            Into::<Uuid>::into(value).into()
        }
    }

    impl From<Commit> for crate::protos::Hash {
        fn from(value: Commit) -> Self {
            value.0.into()
        }
    }

    impl From<&Commit> for crate::protos::Hash {
        fn from(value: &Commit) -> Self {
            (&value.0).into()
        }
    }

    impl TryFrom<crate::protos::Hash> for Commit {
        type Error = <Hash as TryFrom<crate::protos::Hash>>::Error;

        fn try_from(value: crate::protos::Hash) -> Result<Self, Self::Error> {
            TryInto::<Hash>::try_into(value).map(|h| h.into())
        }
    }

    impl From<&BlockId> for crate::protos::Hash {
        fn from(value: &BlockId) -> Self {
            (&value.0).into()
        }
    }

    impl TryFrom<crate::protos::Hash> for BlockId {
        type Error = <Hash as TryFrom<crate::protos::Hash>>::Error;

        fn try_from(value: crate::protos::Hash) -> Result<Self, Self::Error> {
            TryInto::<Hash>::try_into(value).map(|h| h.into())
        }
    }

    impl From<&ClusterId> for crate::protos::Hash {
        fn from(value: &ClusterId) -> Self {
            (&value.0).into()
        }
    }

    impl TryFrom<crate::protos::Hash> for ClusterId {
        type Error = <Hash as TryFrom<crate::protos::Hash>>::Error;

        fn try_from(value: crate::protos::Hash) -> Result<Self, Self::Error> {
            TryInto::<Hash>::try_into(value).map(|h| h.into())
        }
    }
}
