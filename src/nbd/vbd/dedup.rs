use crate::hash::{Hash, HashAlgorithm};
use crate::nbd::block_device::read_reply::Queue;
use crate::nbd::block_device::{BlockDevice, Options, RequestContext};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::task::JoinHandle;

const MIN_BLOCK_SIZE: u32 = 4096;

#[derive(Clone)]
struct Block {
    hash: Hash,
    data: Bytes,
}

impl Block {
    fn new(hash: Hash, data: Bytes) -> Self {
        Self { hash, data }
    }

    fn len(&self) -> usize {
        self.data.len()
    }
}

struct DataContainer {
    index: HashMap<usize, Hash>,
    data_blocks: HashMap<Hash, (Block, usize)>,
}

impl DataContainer {
    fn get(&self, block_no: usize) -> Option<Block> {
        self.index
            .get(&block_no)
            .map(|hash| self.data_blocks.get(hash).map(|(b, _)| b.clone()))
            .flatten()
    }

    fn remove(&mut self, block_no: usize) {
        if let Some(hash) = self.index.remove(&block_no) {
            self.gc(&hash);
        }
    }

    fn gc(&mut self, hash: &Hash) {
        if let Some((_, refcount)) = self.data_blocks.get_mut(hash) {
            *refcount = refcount.saturating_sub(1);
            if *refcount == 0 {
                self.data_blocks.remove(hash);
            }
        }
    }

    fn insert(&mut self, block_no: usize, block: Block) {
        let previous = self.index.insert(block_no, block.hash.clone());

        self.data_blocks
            .entry(block.hash.clone())
            .and_modify(|(_, refcount)| *refcount += 1)
            .or_insert_with(|| (block, 1));

        if let Some(hash) = previous {
            self.gc(&hash);
        }
    }
}

pub struct DedupDevice {
    block_size: u32,
    num_blocks: usize,
    description: Option<String>,
    data_blocks: Arc<RwLock<DataContainer>>,
    zero_block: Bytes,
    zero_hash: Hash,
    hasher: HashAlgorithm,
    stats_task: JoinHandle<()>,
}

impl DedupDevice {
    pub fn new(
        block_size: u32,
        num_blocks: usize,
        hasher: HashAlgorithm,
        description: Option<impl ToString>,
    ) -> Self {
        assert!(block_size >= MIN_BLOCK_SIZE);
        assert!(num_blocks > 0);

        let zero_block = BytesMut::zeroed(block_size as usize).freeze();
        let zero_hash = hasher.hash(zero_block.as_ref());

        let data_blocks = Arc::new(RwLock::new(DataContainer {
            index: HashMap::with_capacity(num_blocks),
            data_blocks: HashMap::default(),
        }));

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
            stats_task,
            data_blocks,
            zero_block,
            zero_hash,
        }
    }
}

impl Drop for DedupDevice {
    fn drop(&mut self) {
        self.stats_task.abort();
    }
}

#[async_trait]
impl BlockDevice for DedupDevice {
    async fn options(&self) -> Options {
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
                    .map(|(b, _)| BytesMut::from(b.data.as_ref()))
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

    async fn close(&mut self) -> crate::nbd::block_device::Result<()> {
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
