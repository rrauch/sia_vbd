pub mod nbd_device;

use crate::hash::{Hash, HashAlgorithm};
use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hasher;
use std::marker::PhantomData;
use std::ops::{Deref, Range};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use thiserror::Error;
use uuid::Uuid;

struct ContentId<T>(Hash, PhantomData<T>);

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

struct Transaction {
    id: Uuid,
    created: Instant,
    max_blocks: usize,
    last_modified: Instant,
    blocks: HashMap<(usize, usize), Option<Bytes>>,
}

impl Transaction {
    pub fn new(max_blocks: usize) -> Self {
        Self {
            id: Uuid::now_v7(),
            created: Instant::now(),
            max_blocks,
            last_modified: Instant::now(),
            blocks: HashMap::default(),
        }
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

pub struct VirtualBlockDevice {
    state: State,
}

enum State {
    Committed(Committed),
    Uncommitted(Uncommitted),
    Poisoned,
}

struct Shared {
    uuid: Uuid,
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
}

impl Shared {
    fn calc_commit(&self) -> Commit {
        let mut hasher = self.meta_hash.new();
        hasher.update("--sia_vbd commit hash v1 start--\n".as_bytes());
        hasher.update("uuid: ".as_bytes());
        hasher.update(self.uuid.as_bytes());
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
}

struct Uncommitted {
    previous_commit: Commit,
    shared: Shared,
    tx: Transaction,
}

impl Committed {
    fn begin(self) -> Uncommitted {
        Uncommitted {
            previous_commit: self.commit,
            tx: Transaction::new(self.shared.tx_max_blocks),
            shared: self.shared,
        }
    }
}

impl Uncommitted {
    fn try_commit(mut self) -> Result<Committed, (Committed, BlockError)> {
        let mut clustered_changes = HashMap::<usize, Vec<(usize, Option<Bytes>)>>::new();
        self.tx
            .blocks
            .into_iter()
            .for_each(|((cluster_no, block_no), data)| {
                clustered_changes
                    .entry(cluster_no)
                    .or_default()
                    .push((block_no, data))
            });

        let mut clusters = Vec::with_capacity(clustered_changes.len());

        for (cluster_no, changed_blocks) in clustered_changes {
            let cluster_id = &self.shared.clusters[cluster_no];
            let c = match self
                .shared
                .cluster_map
                .get(cluster_id)
                .ok_or_else(|| BlockError::ClusterNoFound {
                    cluster_id: cluster_id.clone(),
                })
                .map(|c| c.clone())
            {
                Ok(cluster) => cluster,
                Err(err) => {
                    return Err((
                        Committed {
                            commit: self.previous_commit,
                            shared: self.shared,
                        },
                        err,
                    ))
                }
            };

            let mut cluster_mut = ClusterMut::from_cluster(
                Arc::try_unwrap(c).unwrap_or_else(|c| c.as_ref().clone()),
                self.shared.uuid,
                self.shared.block_size,
                self.shared.meta_hash,
            );

            for (block_no, data) in changed_blocks {
                let res = match data {
                    Some(data) => Block::new(
                        self.shared.uuid,
                        self.shared.block_size,
                        self.shared.content_hash,
                        data,
                    ),
                    None => {
                        // the block was deleted
                        // return the zero block
                        Ok(self.shared.zero_block.clone())
                    }
                };
                let block = match res {
                    Ok(block) => block,
                    Err(err) => {
                        return Err((
                            Committed {
                                commit: self.previous_commit,
                                shared: self.shared,
                            },
                            err,
                        ))
                    }
                };

                let block_id = block.content_id().clone();
                // only insert if not yet present
                self.shared
                    .blocks
                    .entry(block_id.clone())
                    .or_insert_with(|| block);
                cluster_mut.blocks[block_no] = block_id;
            }
            clusters.push((cluster_no, cluster_mut.finalize()));
        }

        for (cluster_no, new_cluster) in clusters {
            let cluster_id = new_cluster.content_id().clone();
            // only insert if not yet present
            self.shared
                .cluster_map
                .entry(cluster_id.clone())
                .or_insert_with(|| Arc::new(new_cluster));
            self.shared.clusters[cluster_no] = cluster_id;
        }

        let age = Instant::now() - self.tx.created;
        eprintln!("committed tx {} ({} ms)", self.tx.id, age.as_millis());

        let new_commit = self.shared.calc_commit();
        if &new_commit != &self.previous_commit {
            eprintln!("commit: {} -> {}", &self.previous_commit, &new_commit);
        }

        Ok(Committed {
            commit: new_commit,
            shared: self.shared,
        })
    }

    fn rollback(self) -> Committed {
        Committed {
            commit: self.previous_commit,
            shared: self.shared,
        }
    }
}

impl VirtualBlockDevice {
    pub fn new(
        uuid: Uuid,
        cluster_size: ClusterSize,
        num_clusters: usize,
        block_size: BlockSize,
        content_hash: HashAlgorithm,
        meta_hash: HashAlgorithm,
        max_buffer_size: usize,
    ) -> Result<Self, anyhow::Error> {
        assert!(num_clusters > 0);

        let zero_block = Block::new(
            uuid,
            block_size,
            content_hash,
            BytesMut::zeroed(*block_size).freeze(),
        )?;

        let zero_block_id = zero_block.content_id().clone();

        let blocks = [(zero_block_id.clone(), zero_block.clone())]
            .into_iter()
            .collect();

        let zero_cluster = Arc::new(
            ClusterMut::new_empty(uuid, block_size, cluster_size, meta_hash, zero_block_id)
                .finalize(),
        );
        let zero_cluster_id = zero_cluster.content_id().clone();
        let clusters = (0..num_clusters).map(|_| zero_cluster_id.clone()).collect();
        let cluster_map = [(zero_cluster_id.clone(), zero_cluster.clone())]
            .into_iter()
            .collect();

        let shared = Shared {
            uuid,
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
        };

        let commit = shared.calc_commit();

        eprintln!("vbd uuid: {}", uuid);
        eprintln!("commit: {}", &commit);

        Ok(Self {
            state: State::Committed(Committed { commit, shared }),
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

    pub fn put(&mut self, block_no: usize, data: Bytes) -> Result<(), BlockError> {
        let (cluster_no, relative_block_no) = self.calc_cluster_block(block_no)?;
        if all_zeroes(data.as_ref()) {
            // use delete instead
            self.delete(block_no..(block_no + 1))?;
        } else {
            let tx = self.prepare_tx()?;
            tx.put(cluster_no, relative_block_no, data);
        }
        Ok(())
    }

    pub fn delete(&mut self, blocks: Range<usize>) -> Result<(), BlockError> {
        let clusters = self.calc_clusters_blocks(blocks)?;
        let tx = self.prepare_tx()?;

        for (cluster_no, blocks) in clusters {
            for block_no in blocks {
                tx.delete(cluster_no, block_no);
            }
        }
        Ok(())
    }

    fn prepare_tx(&mut self) -> Result<&mut Transaction, BlockError> {
        if let State::Uncommitted(state) = &self.state {
            if state.tx.is_full() {
                self.commit()?;
            }
        }

        if let State::Committed(_) = &self.state {
            match std::mem::replace(&mut self.state, State::Poisoned) {
                State::Committed(state) => {
                    self.state = State::Uncommitted(state.begin());
                }
                _ => unreachable!(),
            }
        }

        Ok(match &mut self.state {
            State::Uncommitted(state) => &mut state.tx,
            _ => unreachable!(),
        })
    }

    fn commit(&mut self) -> Result<(), BlockError> {
        if let State::Uncommitted(_) = self.state {
            match std::mem::replace(&mut self.state, State::Poisoned) {
                State::Uncommitted(state) => match state.try_commit() {
                    Ok(committed) => self.state = State::Committed(committed),
                    Err((prev_commit, err)) => {
                        self.state = State::Committed(prev_commit);
                        return Err(err);
                    }
                },
                _ => unreachable!(),
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), BlockError> {
        // commit the pending transaction if there is one
        self.commit()?;
        Ok(())
    }

    pub fn uuid(&self) -> Uuid {
        self.shared().uuid
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
        uuid: Uuid,
        block_size: BlockSize,
        hash: HashAlgorithm,
        data: Bytes,
    ) -> Result<Self, BlockError> {
        let content_id = BlockId::new(uuid, block_size, &data, hash)?;
        Ok(Self { content_id, data })
    }

    fn content_id(&self) -> &BlockId {
        &self.content_id
    }
}

#[derive(Clone)]
struct ClusterMut {
    uuid: Uuid,
    block_size: BlockSize,
    blocks: Vec<BlockId>,
    hash_algorithm: HashAlgorithm,
}

impl ClusterMut {
    pub fn new_empty(
        uuid: Uuid,
        block_size: BlockSize,
        cluster_size: ClusterSize,
        hash_algorithm: HashAlgorithm,
        zero_block_id: BlockId,
    ) -> Self {
        Self {
            uuid,
            block_size,
            blocks: (0..*cluster_size).map(|_| zero_block_id.clone()).collect(),
            hash_algorithm,
        }
    }

    pub fn from_cluster(
        cluster: Cluster,
        uuid: Uuid,
        block_size: BlockSize,
        hash_algorithm: HashAlgorithm,
    ) -> Self {
        Self {
            uuid,
            block_size,
            blocks: cluster.blocks,
            hash_algorithm,
        }
    }

    fn calc_content_id(&self) -> ClusterId {
        let mut hasher = self.hash_algorithm.new();
        hasher.update("--sia_vbd cluster hash v1 start--\n".as_bytes());
        hasher.update("uuid: ".as_bytes());
        hasher.update(self.uuid.as_bytes());
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
        uuid: Uuid,
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
        hasher.update(uuid.as_bytes());
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
}
