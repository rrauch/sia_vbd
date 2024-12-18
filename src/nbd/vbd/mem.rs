use crate::nbd::block_device::read_reply::Queue;
use crate::nbd::block_device::Result;
use crate::nbd::block_device::{BlockDevice, Options, RequestContext};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt};
use std::collections::HashMap;
use std::sync::RwLock;

const MIN_BLOCK_SIZE: u32 = 512;

pub struct MemDevice {
    block_size: u32,
    num_blocks: usize,
    data_blocks: RwLock<HashMap<usize, Bytes>>,
    description: Option<String>,
    zeroes: Bytes,
}

impl MemDevice {
    pub fn new(block_size: u32, num_blocks: usize, description: Option<impl ToString>) -> Self {
        assert!(block_size > MIN_BLOCK_SIZE);
        Self {
            block_size,
            num_blocks,
            data_blocks: RwLock::new(HashMap::with_capacity(num_blocks)),
            description: description.map(|s| s.to_string()),
            zeroes: BytesMut::zeroed(block_size as usize).freeze(),
        }
    }
}

#[async_trait]
impl BlockDevice for MemDevice {
    fn options(&self) -> Options {
        Options {
            block_size: Some((MIN_BLOCK_SIZE, self.block_size)),
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
    ) -> Result<()> {
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
                lock.get(&(block as usize))
                    .map(|b| b.slice(start as usize..end as usize))
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
    ) -> Result<()> {
        let blocks = calculate_affected_blocks(
            self.block_size as u64,
            self.num_blocks as u64,
            offset,
            length,
        );

        for block in blocks.start_block..blocks.end_block + 1 {
            let mut buf = {
                let lock = self.data_blocks.read().unwrap();
                lock.get(&(block as usize))
                    .map(|b| BytesMut::from(b.as_ref()))
                    .or_else(|| Some(BytesMut::zeroed(self.block_size as usize)))
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

            {
                let mut lock = self.data_blocks.write().unwrap();
                if all_zeroes(&buf) {
                    lock.remove(&(block as usize));
                } else {
                    lock.insert(block as usize, buf.freeze());
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
    ) -> Result<()> {
        let blocks = calculate_affected_blocks(
            self.block_size as u64,
            self.num_blocks as u64,
            offset,
            length,
        );
        let zeroes = self.zeroes.clone();

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
                lock.remove(&(block as usize));
            } else {
                if let Some(mut buf) = lock
                    .get(&(block as usize))
                    .map(|b| BytesMut::from(b.as_ref()))
                {
                    let to_zero = &mut buf[start..end];
                    to_zero.copy_from_slice(&zeroes.as_ref()[..length]);
                    lock.insert(block as usize, buf.freeze());
                }
            }
        }

        Ok(())
    }

    async fn flush(&self, _ctx: &RequestContext) -> Result<()> {
        // do nothing
        Ok(())
    }

    async fn trim(&self, offset: u64, length: u64, ctx: &RequestContext) -> Result<()> {
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

    /*let ptr = buffer.as_ptr();
    let end = unsafe { ptr.add(buffer.len()) };
    let mut current = ptr;
    unsafe {
        while current < end {
            if *current != 0 {
                return false;
            }
            current = current.add(1);
        }
    }
    true*/
}
