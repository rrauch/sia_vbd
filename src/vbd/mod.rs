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
    max_blocks: usize,
    last_modified: Instant,
    blocks: HashMap<(usize, usize), Option<Bytes>>,
}

impl Transaction {
    pub fn new(max_blocks: usize) -> Self {
        Self {
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

type StateId = ContentId<State>;

pub struct State {
    uuid: Uuid,
    content_id: Option<StateId>,
    cluster_size: ClusterSize,
    zero_cluster: Arc<Cluster>,
    zero_cluster_id: ClusterId,
    cluster_map: HashMap<ClusterId, Arc<Cluster>>,
    clusters: Vec<ClusterId>,
    cluster_hash: HashAlgorithm,
    block_size: BlockSize,
    block_hash: HashAlgorithm,
    zero_block: Block,
    blocks: HashMap<BlockId, Block>,
    hash: HashAlgorithm,
    pending_tx: Option<Transaction>,
    tx_max_blocks: usize,
}

impl State {
    pub fn new(
        uuid: Uuid,
        cluster_size: ClusterSize,
        num_clusters: usize,
        cluster_hash: HashAlgorithm,
        block_size: BlockSize,
        block_hash: HashAlgorithm,
        hash: HashAlgorithm,
        max_buffer_size: usize,
    ) -> Result<Self, anyhow::Error> {
        assert!(num_clusters > 0);

        let zero_block = Block::new(
            uuid,
            block_size,
            block_hash,
            BytesMut::zeroed(*block_size).freeze(),
        )?;

        let zero_block_id = zero_block.content_id().clone();

        let blocks = [(zero_block_id.clone(), zero_block.clone())]
            .into_iter()
            .collect();

        let zero_cluster = Arc::new(
            ClusterMut::new_empty(uuid, block_size, cluster_size, cluster_hash, zero_block_id)
                .finalize(),
        );
        let zero_cluster_id = zero_cluster.content_id().clone();
        let clusters = (0..num_clusters).map(|_| zero_cluster_id.clone()).collect();
        let cluster_map = [(zero_cluster_id.clone(), zero_cluster.clone())]
            .into_iter()
            .collect();

        let mut state = Self {
            uuid,
            content_id: None,
            cluster_size,
            zero_cluster,
            zero_cluster_id,
            cluster_map,
            clusters,
            cluster_hash,
            block_size,
            block_hash,
            zero_block,
            blocks,
            hash,
            pending_tx: None,
            tx_max_blocks: (max_buffer_size / *block_size) + 1,
        };
        state.refresh_content_id();

        Ok(state)
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
        let cluster_size = *self.cluster_size;
        let mut clustered_blocks: Vec<(usize, Range<usize>)> = Vec::new();

        for block_no in blocks {
            let cluster_no = block_no / cluster_size;
            let relative_block_no = block_no % cluster_size;

            if cluster_no >= self.clusters.len() || relative_block_no >= cluster_size {
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
        if let Some(tx) = &self.pending_tx {
            if let Some(data) = tx.get(cluster_no, relative_block_no) {
                // return the pending data
                return Ok(data);
            }
        }

        let cluster_id = self.clusters[cluster_no].clone();

        if cluster_id == self.zero_cluster_id {
            // the whole cluster is empty
            return Ok(None);
        }

        let block_id = self
            .cluster_map
            .get(&cluster_id)
            .ok_or_else(|| BlockError::ClusterNoFound { cluster_id })?
            .blocks
            .get(relative_block_no)
            .expect("block_no in range");

        if block_id == self.zero_block.content_id() {
            // this is an empty block
            return Ok(None);
        }

        Ok(Some(
            self.blocks
                .get(block_id)
                .ok_or_else(|| BlockError::BlockNotFound {
                    block_id: block_id.clone(),
                })
                .map(|block| block.data.clone())?,
        ))
    }

    pub fn put(&mut self, block_no: usize, data: Bytes) -> Result<(), BlockError> {
        let (cluster_no, relative_block_no) = self.calc_cluster_block(block_no)?;
        self.prepare_tx()?;
        if all_zeroes(data.as_ref()) {
            // use delete instead
            self.delete(block_no..(block_no + 1))?;
        } else {
            let tx = self.pending_tx.as_mut().unwrap();
            tx.put(cluster_no, relative_block_no, data);
        }
        Ok(())
    }

    pub fn delete(&mut self, blocks: Range<usize>) -> Result<(), BlockError> {
        let clusters = self.calc_clusters_blocks(blocks)?;

        self.prepare_tx()?;

        let tx = self.pending_tx.as_mut().unwrap();
        for (cluster_no, blocks) in clusters {
            for block_no in blocks {
                tx.delete(cluster_no, block_no);
            }
        }

        Ok(())
    }

    fn prepare_tx(&mut self) -> Result<(), BlockError> {
        if self
            .pending_tx
            .as_ref()
            .map(|tx| tx.is_full())
            .unwrap_or(false)
        {
            // the current tx is full, commit now
            self.commit()?;
        }

        if self.pending_tx.is_none() {
            self.pending_tx = Some(Transaction::new(self.tx_max_blocks));
        }

        Ok(())
    }

    fn commit(&mut self) -> Result<(), BlockError> {
        if let Some(tx) = self.pending_tx.take() {
            let mut clusters = HashMap::<usize, Vec<(usize, Option<Bytes>)>>::new();
            tx.blocks
                .into_iter()
                .for_each(|((cluster_no, block_no), data)| {
                    clusters
                        .entry(cluster_no)
                        .or_default()
                        .push((block_no, data))
                });

            for (cluster_no, changed_blocks) in clusters {
                let cluster_id = &self.clusters[cluster_no];
                let c = self
                    .cluster_map
                    .get(cluster_id)
                    .ok_or_else(|| BlockError::ClusterNoFound {
                        cluster_id: cluster_id.clone(),
                    })
                    .map(|c| c.clone())?;

                let mut cluster_mut = ClusterMut::from_cluster(
                    Arc::try_unwrap(c).unwrap_or_else(|c| c.as_ref().clone()),
                    self.uuid,
                    self.block_size,
                    self.cluster_hash,
                );

                for (block_no, data) in changed_blocks {
                    let block = match data {
                        Some(data) => {
                            Block::new(self.uuid, self.block_size, self.block_hash, data)?
                        }
                        None => {
                            // the block was deleted
                            // return the zero block
                            self.zero_block.clone()
                        }
                    };
                    let block_id = block.content_id().clone();
                    // only insert if not yet present
                    self.blocks.entry(block_id.clone()).or_insert_with(|| block);
                    cluster_mut.blocks[block_no] = block_id;
                }
                let new_cluster = cluster_mut.finalize();
                let cluster_id = new_cluster.content_id().clone();
                // only insert if not yet present
                self.cluster_map
                    .entry(cluster_id.clone())
                    .or_insert_with(|| Arc::new(new_cluster));
                self.clusters[cluster_no] = cluster_id;
            }
            let prev = self.content_id().clone();
            self.refresh_content_id();
            let current = self.content_id();
            if current != &prev {
                eprintln!("content_id: {} -> {}", prev, current);
            }
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), BlockError> {
        // commit the pending transaction if there is one
        self.commit()?;
        Ok(())
    }

    pub fn content_id(&self) -> &StateId {
        self.content_id.as_ref().unwrap()
    }

    fn refresh_content_id(&mut self) {
        self.content_id = Some(self.calc_content_id());
    }

    fn calc_content_id(&self) -> StateId {
        let mut hasher = self.hash.new();
        hasher.update("--sia_vbd state hash v1 start--\n".as_bytes());
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
        hasher.update("\n--sia_vbd state hash v1 end--".as_bytes());
        hasher.finalize().into()
    }

    pub fn block_size(&self) -> usize {
        *self.block_size
    }

    pub fn total_size(&self) -> usize {
        *self.block_size * *self.cluster_size * self.clusters.len()
    }
}

type ClusterId = ContentId<Cluster>;

#[derive(Clone)]
pub(crate) struct Block {
    content_id: BlockId,
    data: Bytes,
}

impl Block {
    pub fn new(
        uuid: Uuid,
        block_size: BlockSize,
        hash: HashAlgorithm,
        data: Bytes,
    ) -> Result<Self, BlockError> {
        let content_id = BlockId::new(uuid, block_size, &data, hash)?;
        Ok(Self { content_id, data })
    }

    pub fn content_id(&self) -> &BlockId {
        &self.content_id
    }

    pub fn data(&self) -> &Bytes {
        &self.data
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

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum BlockSize {
    Bs64k,
    Bs256k,
}

impl Deref for BlockSize {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        match self {
            BlockSize::Bs64k => &(64 * 1024),
            BlockSize::Bs256k => &(256 * 1024),
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

pub fn foo() {
    //let uuid = Uuid::now_v7();
    let uuid = Uuid::from_str("019408c6-9ad0-7e31-b272-042c3d01c68c").unwrap();

    let mut state = State::new(
        uuid,
        ClusterSize::Cs256,
        16 * 1024,
        HashAlgorithm::Blake3,
        BlockSize::Bs64k,
        HashAlgorithm::XXH3,
        HashAlgorithm::Blake3,
        1024 * 1024 * 100,
    )
    .unwrap();

    println!("uuid: {}", state.uuid);
    println!("content_id: {}", state.content_id());
    println!("block_size: {}", state.block_size());
    println!("---");

    let mut bytes = BytesMut::zeroed(*BlockSize::Bs64k);
    bytes[0] = 0xFF;
    bytes[1] = 0xFF;
    bytes[2] = 0xFF;
    bytes[3] = 0xFF;
    let bytes = bytes.freeze();

    state.put(0, bytes.clone()).unwrap();
    println!("content_id: {}", state.content_id());

    state.put(1009, bytes.clone()).unwrap();
    println!("content_id: {}", state.content_id());

    let b1 = state.get(0).unwrap();
    let b2 = state.get(1).unwrap();
    let b3 = state.get(1009).unwrap();
    let b4 = state.get(333).unwrap();
    let b5 = state.get(5555).unwrap();

    state.flush().unwrap();
    println!("content_id: {}", state.content_id());

    state.put(888, bytes.clone()).unwrap();
    state.flush().unwrap();
    println!("content_id: {}", state.content_id());

    state.delete(800..900).unwrap();
    state.flush().unwrap();
    println!("content_id: {}", state.content_id());

    println!("");
}
