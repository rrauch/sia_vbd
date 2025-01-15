pub mod nbd_device;
pub(crate) mod wal;

use crate::hash::{Hash, HashAlgorithm};
use crate::vbd::wal::{RollbackError, TokioFileWal, WalId, WalWriter};
use anyhow::bail;
use bytes::{Bytes, BytesMut};
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
use uuid::Uuid;

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
        Debug::fmt(&self.0, f)
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

struct Transaction {
    max_blocks: usize,
    blocks: HashMap<(usize, usize), Option<Bytes>>,
    last_modified: Instant,
    wal_tx: wal::Tx<TokioFileWal>,
}

impl Transaction {
    pub async fn new(
        max_blocks: usize,
        wal: WalWriter<TokioFileWal>,
        preceding_commit: Commit,
        max_tx_size: u64,
    ) -> Result<Self, (BlockError, Result<WalWriter<TokioFileWal>, RollbackError>)> {
        let wal_tx = wal
            .begin(preceding_commit, max_tx_size)
            .await
            .map_err(|(e, wal)| (e.into(), wal))?;

        Ok(Self {
            max_blocks,
            blocks: HashMap::default(),
            last_modified: Instant::now(),
            wal_tx,
        })
    }

    pub fn get(&self, cluster_no: usize, block_no: usize) -> Option<Option<Bytes>> {
        self.blocks
            .get(&(cluster_no, block_no))
            .map(|b| b.as_ref().map(|b| b.clone()))
    }

    pub fn put(&mut self, cluster_no: usize, block_no: usize, data: Bytes) {
        self.blocks.insert((cluster_no, block_no), Some(data));
        self.last_modified = Instant::now();
    }

    pub fn delete(&mut self, cluster_no: usize, block_no: usize) {
        self.blocks.insert((cluster_no, block_no), None);
        self.last_modified = Instant::now();
    }

    pub fn is_full(&self) -> bool {
        self.blocks.len() >= self.max_blocks
    }
}

pub type Commit = ContentId<State>;
pub type VbdId = TypedUuid<VirtualBlockDevice>;

pub struct VirtualBlockDevice {
    state: State,
}

pub(crate) enum State {
    Committed(Committed),
    Uncommitted(Uncommitted),
    Poisoned,
}

#[derive(Clone)]
struct Shared {
    id: VbdId,
    cluster_size: ClusterSize,
    block_size: BlockSize,
    content_hash: HashAlgorithm,
    meta_hash: HashAlgorithm,
    clusters: Vec<ClusterId>,
    cluster_map: HashMap<ClusterId, Arc<Cluster>>,
    zero_cluster_id: ClusterId,
    zero_block: Block,
    blocks: HashMap<BlockId, Block>,
    tx_max_blocks: usize,
    wal_dir: PathBuf,
    max_wal_size: u64,
}

impl Shared {
    fn calc_commit(&self) -> Commit {
        let mut hasher = self.meta_hash.new();
        hasher.update("--sia_vbd commit hash v1 start--\n".as_bytes());
        hasher.update("uuid: ".as_bytes());
        hasher.update(self.id.as_bytes());
        hasher.update("\nblock_size: ".as_bytes());
        hasher.update(self.block_size.to_be_bytes());
        hasher.update("\ncluster_size: ".as_bytes());
        hasher.update(self.cluster_size.to_be_bytes());
        hasher.update("number_of_clusters: ".as_bytes());
        hasher.update(self.clusters.len().to_be_bytes());
        hasher.update("\ncontent: ".as_bytes());
        self.clusters.iter().enumerate().for_each(|(i, c)| {
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

struct Committed {
    commit: Commit,
    shared: Shared,
    wal: Option<WalWriter<TokioFileWal>>,
}

struct Uncommitted {
    previous_commit: Commit,
    shared: Shared,
    tx: Transaction,
}

impl Committed {
    async fn begin(mut self) -> Result<Uncommitted, (BlockError, Self)> {
        async fn prepare_wal<P: AsRef<Path>>(
            mut wal: Option<WalWriter<TokioFileWal>>,
            wal_dir: P,
            required_disk_space: u64,
            max_wal_size: u64,
            vbd_id: &VbdId,
        ) -> Result<WalWriter<TokioFileWal>, BlockError> {
            let mut previous_wal_id = None;
            if let Some(existing_wal) = wal.take() {
                if existing_wal.remaining().await? < required_disk_space {
                    previous_wal_id = Some(existing_wal.id().clone());
                } else {
                    wal = Some(existing_wal);
                }
            };

            if wal.is_none() {
                // start a new wal
                let wal_id: WalId = Uuid::now_v7().into();
                let wal_file =
                    tokio::fs::File::create_new(wal_dir.as_ref().join(format!("{}.wal", wal_id)))
                        .await?;
                wal = Some(
                    WalWriter::new(
                        TokioFileWal::new(wal_file),
                        wal_id,
                        vbd_id.clone(),
                        previous_wal_id,
                        max_wal_size,
                    )
                    .await?,
                );
            }

            Ok(wal.expect("wal should be Some"))
        }
        let required_disk_space =
            self.shared.tx_max_blocks as u64 * self.shared.block_size as u64 + 1024 * 1024 * 10;

        let wal = match prepare_wal(
            self.wal.take(),
            &self.shared.wal_dir,
            required_disk_space,
            self.shared.max_wal_size,
            &self.shared.id,
        )
        .await
        {
            Ok(wal) => wal,
            Err(err) => {
                return Err((err, self));
            }
        };

        let tx = match Transaction::new(
            self.shared.tx_max_blocks,
            wal,
            self.commit.clone(),
            required_disk_space,
        )
        .await
        {
            Ok(tx) => tx,
            Err((err, wal)) => {
                self.wal = wal.ok();
                return Err((err, self));
            }
        };

        Ok(Uncommitted {
            previous_commit: self.commit.clone(),
            tx,
            shared: self.shared,
        })
    }
}

impl Uncommitted {
    async fn try_commit(mut self) -> Result<Committed, (BlockError, Committed)> {
        async fn apply_changes(
            shared: &mut Shared,
            blocks: HashMap<(usize, usize), Option<Bytes>>,
            wal: &mut wal::Tx<TokioFileWal>,
        ) -> Result<Commit, BlockError> {
            let mut clustered_changes = HashMap::<usize, Vec<(usize, Option<Bytes>)>>::new();

            blocks
                .into_iter()
                .for_each(|((cluster_no, block_no), data)| {
                    clustered_changes
                        .entry(cluster_no)
                        .or_default()
                        .push((block_no, data))
                });

            let mut clusters = Vec::with_capacity(clustered_changes.len());

            for (cluster_no, changed_blocks) in clustered_changes {
                let cluster_id = &shared.clusters[cluster_no];
                let c = shared
                    .cluster_map
                    .get(cluster_id)
                    .ok_or_else(|| BlockError::ClusterNoFound {
                        cluster_id: cluster_id.clone(),
                    })
                    .map(|c| c.clone())?;

                let mut cluster_mut = ClusterMut::from_cluster(
                    Arc::try_unwrap(c).unwrap_or_else(|c| c.as_ref().clone()),
                    shared.id,
                    shared.block_size,
                    shared.meta_hash,
                );

                for (block_no, data) in changed_blocks {
                    let mut is_zero = false;
                    let block = match data {
                        Some(data) => {
                            Block::new(shared.id, shared.block_size, shared.content_hash, data)?
                        }
                        None => {
                            // the block was deleted
                            // return the zero block
                            is_zero = true;
                            shared.zero_block.clone()
                        }
                    };

                    let block_id = block.content_id().clone();

                    let data = if !is_zero { Some(&block.data) } else { None };
                    wal.block(block_id.clone(), data).await?;

                    // only insert if not yet present
                    shared
                        .blocks
                        .entry(block_id.clone())
                        .or_insert_with(|| block);
                    cluster_mut.blocks[block_no] = block_id;
                }
                clusters.push((cluster_no, cluster_mut.finalize()));
            }

            for (cluster_no, new_cluster) in clusters {
                let cluster_id = new_cluster.content_id().clone();

                wal.cluster(&new_cluster).await?;

                // only insert if not yet present
                shared
                    .cluster_map
                    .entry(cluster_id.clone())
                    .or_insert_with(|| Arc::new(new_cluster));
                shared.clusters[cluster_no] = cluster_id;
            }

            Ok(shared.calc_commit())
        }

        let mut new_shared = self.shared.clone();

        let new_commit = match apply_changes(
            &mut new_shared,
            std::mem::take(&mut self.tx.blocks),
            &mut self.tx.wal_tx,
        )
        .await
        {
            Ok(commit) => commit,
            Err(err) => {
                // rolling back
                let wal = match self.tx.wal_tx.rollback().await {
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
                        shared: self.shared,
                        wal,
                    },
                ));
            }
        };

        match self.tx.wal_tx.commit(new_commit.clone()).await {
            Ok((wal, _tx_details)) => Ok(Committed {
                commit: new_commit,
                shared: new_shared,
                wal: Some(wal),
            }),
            Err((e, rbr)) => {
                let wal = rbr.ok();
                Err((
                    e.into(),
                    Committed {
                        commit: self.previous_commit,
                        shared: self.shared,
                        wal,
                    },
                ))
            }
        }
    }

    async fn rollback(self) -> Committed {
        let wal = self.tx.wal_tx.rollback().await.ok();
        Committed {
            commit: self.previous_commit,
            shared: self.shared,
            wal,
        }
    }
}

impl VirtualBlockDevice {
    pub async fn new(
        id: VbdId,
        cluster_size: ClusterSize,
        num_clusters: usize,
        block_size: BlockSize,
        content_hash: HashAlgorithm,
        meta_hash: HashAlgorithm,
        max_buffer_size: usize,
        wal_dir: impl AsRef<Path>,
        max_wal_size: u64,
    ) -> Result<Self, anyhow::Error> {
        assert!(num_clusters > 0);

        let zero_block = Block::new(
            id,
            block_size,
            content_hash,
            BytesMut::zeroed(*block_size).freeze(),
        )?;

        let zero_block_id = zero_block.content_id().clone();

        let blocks = [(zero_block_id.clone(), zero_block.clone())]
            .into_iter()
            .collect();

        let zero_cluster = Arc::new(
            ClusterMut::new_empty(id, block_size, cluster_size, meta_hash, zero_block_id)
                .finalize(),
        );
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

        let shared = Shared {
            id,
            cluster_size,
            block_size,
            content_hash,
            meta_hash,
            clusters,
            cluster_map,
            zero_cluster_id,
            zero_block,
            blocks,
            tx_max_blocks: (max_buffer_size / *block_size) + 1,
            wal_dir: wal_dir.into(),
            max_wal_size,
        };

        let commit = shared.calc_commit();

        eprintln!("vbd id: {}", id);
        eprintln!("commit: {}", &commit);

        Ok(Self {
            state: State::Committed(Committed {
                commit,
                shared,
                wal: None,
            }),
        })
    }

    fn shared(&self) -> &Shared {
        match &self.state {
            State::Committed(s) => &s.shared,
            State::Uncommitted(s) => &s.shared,
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
        let cluster_size = *self.shared().cluster_size;
        let mut clustered_blocks: Vec<(usize, Range<usize>)> = Vec::new();

        for block_no in blocks {
            let cluster_no = block_no / cluster_size;
            let relative_block_no = block_no % cluster_size;

            if cluster_no >= self.shared().clusters.len() || relative_block_no >= cluster_size {
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

    pub fn get(&self, block_no: usize) -> Result<Option<Bytes>, BlockError> {
        let (cluster_no, relative_block_no) = self.calc_cluster_block(block_no)?;

        // check pending transaction first
        if let State::Uncommitted(state) = &self.state {
            if let Some(data) = state.tx.get(cluster_no, relative_block_no) {
                // return the pending data
                return Ok(data);
            }
        }

        let cluster_id = self.shared().clusters[cluster_no].clone();

        if cluster_id == self.shared().zero_cluster_id {
            // the whole cluster is empty
            return Ok(None);
        }

        let block_id = self
            .shared()
            .cluster_map
            .get(&cluster_id)
            .ok_or_else(|| BlockError::ClusterNoFound { cluster_id })?
            .blocks
            .get(relative_block_no)
            .expect("block_no in range");

        if block_id == self.shared().zero_block.content_id() {
            // this is an empty block
            return Ok(None);
        }

        Ok(Some(
            self.shared()
                .blocks
                .get(block_id)
                .ok_or_else(|| BlockError::BlockNotFound {
                    block_id: block_id.clone(),
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
            let tx = self.prepare_tx().await?;
            tx.put(cluster_no, relative_block_no, data);
        }
        Ok(())
    }

    pub async fn delete(&mut self, blocks: Range<usize>) -> Result<(), BlockError> {
        let clusters = self.calc_clusters_blocks(blocks)?;
        let tx = self.prepare_tx().await?;

        for (cluster_no, blocks) in clusters {
            for block_no in blocks {
                tx.delete(cluster_no, block_no);
            }
        }
        Ok(())
    }

    async fn prepare_tx(&mut self) -> Result<&mut Transaction, BlockError> {
        if let State::Uncommitted(state) = &self.state {
            if state.tx.is_full() {
                self.commit().await?;
            }
        }

        if let State::Committed(_) = &self.state {
            match std::mem::replace(&mut self.state, State::Poisoned) {
                State::Committed(state) => match state.begin().await {
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
            State::Uncommitted(state) => &mut state.tx,
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
        self.shared().id
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
        *self.shared().block_size
    }

    pub fn blocks(&self) -> usize {
        let shared = self.shared();
        *shared.cluster_size * shared.clusters.len()
    }

    pub fn cluster_size(&self) -> usize {
        *self.shared().cluster_size
    }

    pub fn total_size(&self) -> usize {
        self.blocks() * *self.shared().block_size
    }
}

type ClusterId = ContentId<Cluster>;

#[derive(Clone)]
pub(crate) struct Block {
    content_id: BlockId,
    data: Bytes,
}

impl Block {
    fn new(
        vbd_id: VbdId,
        block_size: BlockSize,
        hash: HashAlgorithm,
        data: Bytes,
    ) -> Result<Self, BlockError> {
        let content_id = BlockId::new(vbd_id, block_size, &data, hash)?;
        Ok(Self { content_id, data })
    }

    fn content_id(&self) -> &BlockId {
        &self.content_id
    }
}

#[derive(Clone)]
struct ClusterMut {
    vbd_id: VbdId,
    block_size: BlockSize,
    blocks: Vec<BlockId>,
    hash_algorithm: HashAlgorithm,
}

impl ClusterMut {
    pub fn new_empty(
        vbd_id: VbdId,
        block_size: BlockSize,
        cluster_size: ClusterSize,
        hash_algorithm: HashAlgorithm,
        zero_block_id: BlockId,
    ) -> Self {
        Self {
            vbd_id,
            block_size,
            blocks: (0..*cluster_size).map(|_| zero_block_id.clone()).collect(),
            hash_algorithm,
        }
    }

    pub fn from_cluster(
        cluster: Cluster,
        vbd_id: VbdId,
        block_size: BlockSize,
        hash_algorithm: HashAlgorithm,
    ) -> Self {
        Self {
            vbd_id,
            block_size,
            blocks: cluster.blocks,
            hash_algorithm,
        }
    }

    fn calc_content_id(&self) -> ClusterId {
        let mut hasher = self.hash_algorithm.new();
        hasher.update("--sia_vbd cluster hash v1 start--\n".as_bytes());
        hasher.update("uuid: ".as_bytes());
        hasher.update(self.vbd_id.as_bytes());
        hasher.update("\nnumber_of_blocks: ".as_bytes());
        hasher.update(self.blocks.len().to_be_bytes());
        hasher.update("\nblock_size: ");
        hasher.update(self.block_size.to_be_bytes());
        hasher.update("\nhash_algorithm: ".as_bytes());
        hasher.update(self.hash_algorithm.as_str().as_bytes());
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
struct Cluster {
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

impl Deref for ClusterSize {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        match self {
            ClusterSize::Cs256 => &(256),
        }
    }
}

impl FromStr for ClusterSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "256" => Ok(ClusterSize::Cs256),
            _ => Err(format!("'{}' not a valid or supported cluster size.", s)),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum BlockSize {
    Bs16k,
    Bs64k,
    Bs256k,
}

impl Deref for BlockSize {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        match self {
            BlockSize::Bs16k => &(16 * 1024),
            BlockSize::Bs64k => &(64 * 1024),
            BlockSize::Bs256k => &(256 * 1024),
        }
    }
}

impl FromStr for BlockSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "16" => Ok(BlockSize::Bs16k),
            "64" => Ok(BlockSize::Bs64k),
            "256" => Ok(BlockSize::Bs256k),
            _ => Err(format!("'{}' not a valid or supported block size.", s)),
        }
    }
}

type BlockId = ContentId<Block>;

impl BlockId {
    fn new<D: AsRef<[u8]>>(
        vbd_id: VbdId,
        block_size: BlockSize,
        data: D,
        hash_algorithm: HashAlgorithm,
    ) -> Result<Self, BlockError> {
        let block_size = *block_size;
        let data = data.as_ref();
        if data.len() != block_size {
            return Err(BlockError::InvalidDataLength {
                block_size,
                data_len: data.len(),
            });
        }

        let mut hasher = hash_algorithm.new();
        hasher.update("--sia_vbd block hash v1 start--\n".as_bytes());
        hasher.update("uuid: ".as_bytes());
        hasher.update(vbd_id.as_bytes());
        hasher.update("\nblock_size: ".as_bytes());
        hasher.update(block_size.to_be_bytes());
        hasher.update("hash_algorithm: ".as_bytes());
        hasher.update(hash_algorithm.as_str().as_bytes());
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
