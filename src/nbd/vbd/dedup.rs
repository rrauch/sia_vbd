use crate::nbd::block_device::read_reply::Queue;
use crate::nbd::block_device::{BlockDevice, Options, RequestContext};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt};
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::{sleep_until, Instant};

const MIN_BLOCK_SIZE: u32 = 4096;

pub trait Hasher: Send + Sync {
    type Hash: Clone + PartialEq + Eq + Hash + Send + Sync;

    fn hash(&self, data: impl AsRef<[u8]>) -> Self::Hash;
}

pub struct TentHasher {}

impl Hasher for TentHasher {
    type Hash = [u8; 20];

    fn hash(&self, data: impl AsRef<[u8]>) -> Self::Hash {
        tenthash::hash(data)
    }
}

pub struct Blake3Hasher {}

impl Hasher for Blake3Hasher {
    type Hash = blake3::Hash;

    fn hash(&self, data: impl AsRef<[u8]>) -> Self::Hash {
        blake3::hash(data.as_ref())
    }
}

pub struct XXH3Hasher {}

impl Hasher for XXH3Hasher {
    type Hash = u128;

    fn hash(&self, data: impl AsRef<[u8]>) -> Self::Hash {
        twox_hash::XxHash3_128::oneshot(data.as_ref())
    }
}

struct Block<T: Hasher> {
    hash: T::Hash,
    data: Bytes,
    _phantom_data: PhantomData<T>,
}

impl<T: Hasher> Clone for Block<T> {
    fn clone(&self) -> Self {
        Self {
            hash: self.hash.clone(),
            data: self.data.clone(),
            _phantom_data: self._phantom_data.clone(),
        }
    }
}

impl<T: Hasher> Block<T> {
    fn new(hash: T::Hash, data: Bytes) -> Self {
        Self {
            hash,
            data,
            _phantom_data: PhantomData::default(),
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

struct DataContainer<T: Hasher> {
    index: HashMap<usize, T::Hash>,
    data_blocks: HashMap<T::Hash, Block<T>>,
    _phantom_data: PhantomData<T>,
}

impl<T: Hasher> DataContainer<T> {
    fn get(&self, block_no: usize) -> Option<Block<T>> {
        self.index
            .get(&block_no)
            .map(|hash| self.data_blocks.get(hash).map(|b| b.clone()))
            .flatten()
    }

    fn remove(&mut self, block_no: usize) {
        self.index.remove(&block_no);
    }

    fn insert(&mut self, block_no: usize, block: Block<T>) {
        self.index.insert(block_no, block.hash.clone());
        if !self.data_blocks.contains_key(&block.hash) {
            self.data_blocks.insert(block.hash.clone(), block);
        }
    }

    fn gc(&mut self) {
        let in_use = self.index.values().unique().collect::<HashSet<_>>();
        let before = self.data_blocks.len();
        self.data_blocks.retain(|hash, _| in_use.contains(&hash));
        let removed = before - self.data_blocks.len();

        if removed > 0 {
            eprintln!("gc: removed {} unused data blocks", removed);
        }
    }
}

pub struct DedupDevice<T: Hasher> {
    block_size: u32,
    num_blocks: usize,
    description: Option<String>,
    data_blocks: Arc<RwLock<DataContainer<T>>>,
    zero_block: Bytes,
    zero_hash: T::Hash,
    hasher: T,
    gc_task: JoinHandle<()>,
    stats_task: JoinHandle<()>,
}

impl<T: Hasher + 'static> DedupDevice<T> {
    pub fn new(
        block_size: u32,
        num_blocks: usize,
        hasher: T,
        gc_interval: Duration,
        description: Option<impl ToString>,
    ) -> Self {
        assert!(block_size >= MIN_BLOCK_SIZE);
        assert!(num_blocks > 0);

        let zero_block = BytesMut::zeroed(block_size as usize).freeze();
        let zero_hash = hasher.hash(zero_block.as_ref());

        let data_blocks = Arc::new(RwLock::new(DataContainer {
            index: HashMap::with_capacity(num_blocks),
            data_blocks: HashMap::default(),
            _phantom_data: PhantomData::default(),
        }));

        let gc_task = {
            let data_blocks = data_blocks.clone();
            tokio::spawn(async move {
                let mut next_gc = Instant::now() + gc_interval;

                loop {
                    tokio::select! {
                        _ = sleep_until(next_gc) => {
                            // regular gc
                            let mut lock = data_blocks.write().unwrap();
                            lock.gc();
                            next_gc = Instant::now() + gc_interval;
                        },
                        _ = tokio::time::sleep(Duration::from_secs(10)) => {
                            // check if under pressure
                            let under_pressure = {
                                let lock = data_blocks.read().unwrap();
                                lock.data_blocks.len() > num_blocks
                            };
                            if under_pressure {
                                let mut lock = data_blocks.write().unwrap();
                                lock.gc();
                                next_gc = Instant::now() + gc_interval;
                            }
                        }
                    }
                }
            })
        };

        let stats_task = {
            let data_blocks = data_blocks.clone();
            tokio::spawn(async move {
                let mut used = 0;
                let mut unique = 0;

                loop {
                    let (current_used, current_unique) = {
                        let lock = data_blocks.read().unwrap();
                        (lock.index.len(), lock.data_blocks.len())
                    };

                    if current_used != used || current_unique != unique {
                        used = current_used;
                        unique = current_unique;

                        let dedup_factor = used as f64 / unique as f64;
                        let bytes_used = unique as u64 * block_size as u64;
                        let max_bytes = used as u64 * block_size as u64;
                        let bytes_saved: i64 = max_bytes as i64 - bytes_used as i64;

                        eprintln!(
                            "stats: {}/{} unique blocks used - dedup factor: {:.2}x",
                            unique, used, dedup_factor
                        );
                        eprintln!(
                            "stats: {} / {} bytes used ({} bytes saved)",
                            bytes_used,
                            num_blocks * block_size as usize,
                            bytes_saved
                        )
                    }
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
            })
        };

        Self {
            block_size,
            num_blocks,
            description: description.map(|s| s.to_string()),
            hasher,
            gc_task,
            stats_task,
            data_blocks,
            zero_block,
            zero_hash,
        }
    }
}

impl<T: Hasher> Drop for DedupDevice<T> {
    fn drop(&mut self) {
        self.gc_task.abort();
        self.stats_task.abort();
    }
}

#[async_trait]
impl<T: Hasher> BlockDevice for DedupDevice<T> {
    fn options(&self) -> Options {
        Options {
            block_size: self.block_size,
            size: (self.block_size as u64).saturating_mul(self.num_blocks as u64),
            read_only: false,
            resizable: false,
            trim: true,
            rotational: false,
            description: self.description.clone(),
            fast_zeroes: true,
        }
    }

    async fn read(
        &self,
        offset: u64,
        length: u64,
        queue: &mut Queue,
        _ctx: &RequestContext,
    ) -> crate::nbd::block_device::Result<()> {
        let blocks = calculate_affected_blocks(
            self.block_size as u64,
            self.num_blocks as u64,
            offset,
            length,
        );

        for block in blocks.start_block..blocks.end_block + 1 {
            let mut start = 0;
            let mut end = self.block_size as u64;
            if block == blocks.start_block {
                start = blocks.start_offset;
            }
            if block == blocks.end_block {
                end = blocks.end_offset;
            }

            let (abs_offset, length) =
                calculate_absolute_offset(self.block_size as u64, block, start, end);

            let bytes = {
                let lock = self.data_blocks.read().unwrap();

                lock.get(block as usize).map(|b| {
                    if start != 0 || end != b.len() as u64 {
                        b.data.slice(start as usize..end as usize)
                    } else {
                        b.data
                    }
                })
            };

            if let Some(bytes) = bytes {
                queue.data(abs_offset, length, bytes).await.unwrap();
            } else {
                queue.zeroes(abs_offset, length).await.unwrap();
            }
        }

        Ok(())
    }

    async fn write(
        &self,
        offset: u64,
        length: u64,
        _fua: bool,
        data: &mut (dyn AsyncRead + Send + Unpin),
        _ctx: &RequestContext,
    ) -> crate::nbd::block_device::Result<()> {
        let blocks = calculate_affected_blocks(
            self.block_size as u64,
            self.num_blocks as u64,
            offset,
            length,
        );

        for block in blocks.start_block..blocks.end_block + 1 {
            let mut buf = {
                let lock = self.data_blocks.read().unwrap();
                lock.index
                    .get(&(block as usize))
                    .map(|h| lock.data_blocks.get(h))
                    .flatten()
                    .map(|b| BytesMut::from(b.data.as_ref()))
                    .or_else(|| Some(BytesMut::from(self.zero_block.as_ref())))
                    .unwrap()
            };

            let mut start = 0;
            let mut end = self.block_size as usize;
            if block == blocks.start_block {
                start = blocks.start_offset as usize;
            }
            if block == blocks.end_block {
                end = blocks.end_offset as usize;
            }
            data.read_exact(&mut buf[start..end]).await?;
            let hash = self.hasher.hash(buf.as_ref());
            let all_zeroes = hash == self.zero_hash;
            {
                let mut lock = self.data_blocks.write().unwrap();
                if all_zeroes {
                    lock.remove(block as usize);
                } else {
                    lock.insert(block as usize, Block::new(hash, buf.freeze()));
                }
            }
        }

        Ok(())
    }

    async fn write_zeroes(
        &self,
        offset: u64,
        length: u64,
        _no_hole: bool,
        _ctx: &RequestContext,
    ) -> crate::nbd::block_device::Result<()> {
        let blocks = calculate_affected_blocks(
            self.block_size as u64,
            self.num_blocks as u64,
            offset,
            length,
        );
        let mut lock = self.data_blocks.write().unwrap();

        for block in blocks.start_block..blocks.end_block + 1 {
            let mut start = 0;
            let mut end = self.block_size as usize;
            if block == blocks.start_block {
                start = blocks.start_offset as usize;
            }
            if block == blocks.end_block {
                end = blocks.end_offset as usize;
            }
            let length = end - start;
            if length == self.block_size as usize {
                // full block
                lock.remove(block as usize);
            } else {
                if let Some(mut buf) = lock
                    .get(block as usize)
                    .map(|b| BytesMut::from(b.data.as_ref()))
                {
                    let to_zero = &mut buf[start..end];
                    to_zero.copy_from_slice(&self.zero_block.as_ref()[..length]);
                    let buf = buf.freeze();
                    let hash = self.hasher.hash(buf.as_ref());
                    lock.insert(block as usize, Block::new(hash, buf));
                }
            }
        }

        Ok(())
    }

    async fn flush(&self, _ctx: &RequestContext) -> crate::nbd::block_device::Result<()> {
        // do nothing
        Ok(())
    }

    async fn trim(
        &self,
        offset: u64,
        length: u64,
        ctx: &RequestContext,
    ) -> crate::nbd::block_device::Result<()> {
        self.write_zeroes(offset, length, false, ctx).await?;
        Ok(())
    }
}

struct AffectedBlocks {
    start_block: u64,
    start_offset: u64,
    end_block: u64,
    end_offset: u64,
}

fn calculate_affected_blocks(
    block_size: u64,
    num_blocks: u64,
    offset: u64,
    length: u64,
) -> AffectedBlocks {
    // Calculate the starting block
    let start_block = offset / block_size;

    // Calculate the starting offset within the start block
    let start_offset = offset % block_size;

    // Calculate the absolute end position
    let end_absolute = offset + length;

    // Calculate the ending block
    let mut end_block = end_absolute / block_size;

    // Calculate the ending offset within the end block
    let mut end_offset = end_absolute % block_size;

    // Adjust if end_offset is exactly at a block boundary
    if end_offset == 0 && end_block > 0 {
        end_block -= 1;
        end_offset = block_size;
    }

    // Ensure end_block does not exceed the total number of blocks
    if end_block >= num_blocks {
        end_block = num_blocks - 1;
        end_offset = block_size;
    }

    AffectedBlocks {
        start_block,
        start_offset,
        end_block,
        end_offset,
    }
}

fn calculate_absolute_offset(block_size: u64, block: u64, start: u64, end: u64) -> (u64, u64) {
    assert!(start <= end);
    assert!(end <= block_size);
    let offset = block * block_size + start;
    let length = end - start;
    (offset, length)
}
