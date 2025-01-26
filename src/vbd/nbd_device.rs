use crate::nbd::block_device::read_reply::Queue;
use crate::nbd::block_device::{BlockDevice, Error, Options, RequestContext};
use crate::vbd::{BlockError, VirtualBlockDevice};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt};
use std::io::ErrorKind;
use std::ops::Range;
use tokio::sync::RwLock;

pub struct NbdDevice {
    vbd: RwLock<VirtualBlockDevice>,
    block_size: u64,
    zero_block: Bytes,
    block_calc: BlockCalc,
}

impl NbdDevice {
    pub fn new(vbd: VirtualBlockDevice) -> Self {
        let block_size = vbd.block_size() as u64;
        let num_blocks = vbd.blocks() as u64;
        Self {
            vbd: tokio::sync::RwLock::new(vbd),
            block_size,
            zero_block: BytesMut::zeroed(block_size as usize).freeze(),
            block_calc: BlockCalc {
                block_size,
                num_blocks,
            },
        }
    }
}

#[async_trait]
impl BlockDevice for NbdDevice {
    async fn options(&self) -> Options {
        let vbd = self.vbd.read().await;

        Options {
            block_size: vbd.block_size() as u32,
            description: Some(format!("sia_vbd {}", vbd.id())),
            fast_zeroes: true,
            read_only: false,
            resizable: false,
            rotational: false,
            trim: true,
            size: vbd.total_size() as u64,
        }
    }

    async fn read(
        &self,
        offset: u64,
        length: u64,
        queue: &mut Queue,
        _ctx: &RequestContext,
    ) -> crate::nbd::block_device::Result<()> {
        let blocks = self.block_calc.blocks(offset, length);

        for block in blocks.into_iter() {
            let abs_offset = self
                .block_calc
                .absolute_offset(block.block_no, block.range.start);

            let length = block.range.end - block.range.start;
            assert!(length > 0 && length <= self.block_size);

            let bytes = {
                let lock = self.vbd.read().await;

                lock.get(block.block_no as usize).await?.map(|b| {
                    if block.partial {
                        b.slice((block.range.start as usize)..(block.range.end as usize))
                    } else {
                        b
                    }
                })
            };

            if let Some(bytes) = bytes {
                queue.data(abs_offset, length, bytes).await?;
            } else {
                queue.zeroes(abs_offset, length).await?;
            }
        }

        Ok(())
    }

    async fn write(
        &self,
        offset: u64,
        length: u64,
        fua: bool,
        data: &mut (dyn AsyncRead + Send + Unpin),
        _ctx: &RequestContext,
    ) -> crate::nbd::block_device::Result<()> {
        let blocks = self.block_calc.blocks(offset, length);
        let mut data_written = false;

        if let Some((_last_block, _)) = &blocks.last_if_partial {
            //todo: prepare last block
        }

        for block in blocks.into_iter() {
            let mut buf = if !block.partial {
                // this is a full block
                None
            } else {
                // partial block, get from backend first
                let lock = self.vbd.read().await;
                lock.get(block.block_no as usize)
                    .await?
                    .map(|b| BytesMut::from(b))
            }
            .unwrap_or(BytesMut::zeroed(self.block_size as usize));

            let range = (block.range.start as usize)..(block.range.end as usize);

            // write data into buffer
            data.read_exact(&mut buf[range]).await?;

            let bytes = buf.freeze();

            let mut lock = self.vbd.write().await;
            lock.put(block.block_no as usize, bytes).await?;
            data_written = true;
        }

        if data_written && fua {
            let mut lock = self.vbd.write().await;
            lock.flush().await?;
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
        let mut blocks = self.block_calc.blocks(offset, length);

        if !blocks.full.is_empty() {
            // full blocks can be deleted
            let mut lock = self.vbd.write().await;
            lock.delete((blocks.full.start as usize)..(blocks.full.end as usize))
                .await?;

            // clear full range
            blocks.full = 0..0;
        }

        for block in blocks.into_iter() {
            let mut buf = {
                let lock = self.vbd.read().await;
                lock.get(block.block_no as usize)
                    .await?
                    .map(|b| BytesMut::from(b))
            }
            .unwrap_or(BytesMut::zeroed(self.block_size as usize));

            let range = (block.range.start as usize)..(block.range.end as usize);
            let to_zero = &mut buf[range];
            to_zero.copy_from_slice(&self.zero_block.as_ref()[..to_zero.len()]);
            let mut lock = self.vbd.write().await;
            lock.put(block.block_no as usize, buf.freeze()).await?;
        }

        Ok(())
    }

    async fn flush(&self, _ctx: &RequestContext) -> crate::nbd::block_device::Result<()> {
        let mut state = self.vbd.write().await;
        state.flush().await?;
        Ok(())
    }

    async fn trim(
        &self,
        offset: u64,
        length: u64,
        ctx: &RequestContext,
    ) -> crate::nbd::block_device::Result<()> {
        self.write_zeroes(offset, length, true, ctx).await
    }
}

impl From<BlockError> for Error {
    fn from(value: BlockError) -> Self {
        match value {
            BlockError::ClusterNoFound { cluster_id } => Self::IoError(std::io::Error::new(
                ErrorKind::NotFound,
                format!("cluster with id {} not found", cluster_id),
            )),
            BlockError::OutOfRange { block_no } => Self::IoError(std::io::Error::new(
                ErrorKind::InvalidInput,
                format!("block {} out of range", block_no),
            )),
            BlockError::InvalidDataLength {
                data_len,
                block_size,
            } => Self::IoError(std::io::Error::new(
                ErrorKind::InvalidData,
                format!("data length {} != {}", data_len, block_size),
            )),
            BlockError::BlockNotFound { block_id } => Self::IoError(std::io::Error::new(
                ErrorKind::NotFound,
                format!("block with id {} not found", block_id),
            )),
            BlockError::WalError(e) => Self::IoError(std::io::Error::new(
                ErrorKind::Other,
                format!("wal related error: {}", e),
            )),
            BlockError::IoError(e) => Self::IoError(e),
            BlockError::Other(e) => Self::IoError(std::io::Error::new(
                ErrorKind::Other,
                format!("other error: {}", e),
            )),
        }
    }
}

struct BlockCalc {
    block_size: u64,
    num_blocks: u64,
}

impl BlockCalc {
    fn blocks(&self, offset: u64, length: u64) -> AffectedBlocks {
        // Calculate the starting block
        let start_block = offset / self.block_size;

        // Calculate the starting offset within the start block
        let start_offset = offset % self.block_size;

        // Calculate the absolute end position
        let end_absolute = offset + length;

        // Calculate the ending block
        let mut end_block = end_absolute / self.block_size;

        // Calculate the ending offset within the end block
        let mut end_offset = end_absolute % self.block_size;

        // Adjust if end_offset is exactly at a block boundary
        if end_offset == 0 && end_block > 0 {
            end_block -= 1;
            end_offset = self.block_size;
        }

        // Ensure end_block does not exceed the total number of blocks
        if end_block >= self.num_blocks {
            end_block = self.num_blocks - 1;
            end_offset = self.block_size;
        }

        AffectedBlocks::new(
            self.block_size,
            start_block,
            start_offset,
            end_block,
            end_offset,
        )
    }

    fn absolute_offset(&self, block: u64, block_offset: u64) -> u64 {
        assert!(block_offset < self.block_size);
        block * self.block_size + block_offset
    }
}

struct AffectedBlocks {
    block_size: u64,
    first_if_partial: Option<(u64, Range<u64>)>,
    full: Range<u64>,
    last_if_partial: Option<(u64, Range<u64>)>,
}

struct AffectedBlock {
    block_no: u64,
    range: Range<u64>,
    partial: bool,
}

impl AffectedBlock {
    fn new_full(block_no: u64, block_size: u64) -> Self {
        Self {
            block_no,
            range: 0..block_size,
            partial: false,
        }
    }

    fn new_partial(block_no: u64, range: Range<u64>) -> Self {
        Self {
            block_no,
            range,
            partial: true,
        }
    }
}

struct BlockIterator {
    idx: u64,
    block_size: u64,
    blocks: AffectedBlocks,
}

impl Iterator for BlockIterator {
    type Item = AffectedBlock;

    fn next(&mut self) -> Option<Self::Item> {
        let mut item = None;

        if self.blocks.full.contains(&self.idx) {
            item = Some(AffectedBlock::new_full(self.idx, self.block_size));
        }

        if item.is_none() {
            match self.blocks.first_if_partial.take() {
                Some((block_no, range)) if block_no == self.idx => {
                    item = Some(AffectedBlock::new_partial(block_no, range));
                    self.idx = block_no;
                }
                _ => {}
            }
        }

        if item.is_none() {
            match self.blocks.last_if_partial.take() {
                Some((block_no, range)) if block_no == self.idx => {
                    item = Some(AffectedBlock::new_partial(block_no, range));
                    self.idx = block_no;
                }
                _ => {}
            }
        }

        self.idx += 1;
        item
    }
}

impl AffectedBlocks {
    fn new(
        block_size: u64,
        start_block: u64,
        start_offset: u64,
        end_block: u64,
        end_offset: u64,
    ) -> Self {
        let mut full = start_block..end_block + 1;

        let mut first_if_partial = None;
        if start_offset != 0 {
            first_if_partial = Some((start_block, start_offset..block_size));
            full.start += 1;
        }

        let mut last_if_partial = None;
        if end_offset != block_size {
            if end_block == start_block {
                // single, partial block detected
                let start_offset = match first_if_partial {
                    Some((_, range)) => range.start,
                    _ => {
                        full.start += 1;
                        0
                    }
                };
                first_if_partial = Some((end_block, start_offset..end_offset));
            } else {
                last_if_partial = Some((end_block, 0..end_offset));
                full.end -= 1;
            }
        }

        Self {
            block_size,
            first_if_partial,
            full,
            last_if_partial,
        }
    }

    fn into_iter(self) -> BlockIterator {
        let idx = match self.first_if_partial.as_ref() {
            Some((block_no, _)) => *block_no,
            None => self.full.start,
        };

        BlockIterator {
            idx,
            block_size: self.block_size,
            blocks: self,
        }
    }
}
