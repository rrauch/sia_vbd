use crate::nbd::block_device::read_reply::Queue;
use crate::nbd::block_device::{BlockDevice, Error, Options, RequestContext};
use crate::vbd::{BlockError, Structure};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt};
use std::io::ErrorKind;
use std::sync::RwLock;

pub struct NbdDevice {
    structure: RwLock<Structure>,
    block_size: u64,
    num_blocks: u64,
    zero_block: Bytes,
}

impl NbdDevice {
    pub fn new(structure: Structure) -> Self {
        let block_size = *structure.block_size as u64;
        let num_blocks = *structure.cluster_size * structure.clusters.len();
        Self {
            structure: RwLock::new(structure),
            block_size,
            num_blocks: num_blocks as u64,
            zero_block: BytesMut::zeroed(block_size as usize).freeze(),
        }
    }
}

#[async_trait]
impl BlockDevice for NbdDevice {
    fn options(&self) -> Options {
        let structure = self.structure.read().unwrap();

        Options {
            block_size: self.block_size as u32,
            description: Some(format!("sia_vbd {}", structure.uuid)),
            fast_zeroes: true,
            read_only: false,
            resizable: false,
            rotational: false,
            trim: true,
            size: structure.total_size() as u64,
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
                let lock = self.structure.read().unwrap();

                lock.get(block as usize)?.map(|b| {
                    if start != 0 || end != b.len() as u64 {
                        b.slice(start as usize..end as usize)
                    } else {
                        b
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
        fua: bool,
        data: &mut (dyn AsyncRead + Send + Unpin),
        _ctx: &RequestContext,
    ) -> crate::nbd::block_device::Result<()> {
        let blocks = calculate_affected_blocks(self.block_size, self.num_blocks, offset, length);
        let mut data_written = false;

        for block in blocks.start_block..blocks.end_block + 1 {
            let mut start = 0 as usize;
            let mut end = self.block_size as usize;
            if block == blocks.start_block {
                start = blocks.start_offset as usize;
            }
            if block == blocks.end_block {
                end = blocks.end_offset as usize;
            }

            let mut buf = if start == 0 && end >= self.block_size as usize {
                // this is a full block
                None
            } else {
                // partial block, get from backend first
                let lock = self.structure.read().unwrap();
                lock.get(block as usize)?.map(|b| BytesMut::from(b))
            }
            .unwrap_or(BytesMut::zeroed(self.block_size as usize));

            // write data into buffer
            data.read_exact(&mut buf[start..end]).await?;

            let bytes = buf.freeze();

            let mut lock = self.structure.write().unwrap();
            lock.put(block as usize, bytes)?;
            data_written = true;
        }

        if data_written && fua {
            let mut lock = self.structure.write().unwrap();
            lock.flush()?;
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
        let blocks = calculate_affected_blocks(self.block_size, self.num_blocks, offset, length);

        let mut full_blocks = blocks.start_block as usize..blocks.end_block as usize;
        let mut partial = vec![];

        if blocks.start_offset != 0 {
            full_blocks.start += 1;
            let end = if blocks.start_block != blocks.end_block {
                self.block_size
            } else {
                blocks.end_offset
            };
            partial.push((
                blocks.start_block as usize,
                blocks.start_offset as usize,
                end as usize,
            ));
        }

        if !full_blocks.is_empty() {
            if blocks.end_offset != self.block_size {
                full_blocks.end -= 1;
                partial.push((blocks.end_block as usize, 0, blocks.end_offset as usize));
            }
        }

        let mut partial_blocks = vec![];

        for (block, start, end) in partial {
            if let Some(mut buf) = {
                let lock = self.structure.read().unwrap();
                lock.get(block as usize)?.map(|b| BytesMut::from(b))
            } {
                let to_zero = &mut buf[start..end];
                to_zero.copy_from_slice(&self.zero_block.as_ref()[..to_zero.len()]);
                partial_blocks.push((block, buf.freeze()));
            }
        }

        if !partial_blocks.is_empty() || !full_blocks.is_empty() {
            let mut lock = self.structure.write().unwrap();
            for (block, bytes) in partial_blocks {
                lock.put(block, bytes)?;
            }
            if !full_blocks.is_empty() {
                lock.delete(full_blocks.start..=full_blocks.end - 1)?;
            }
        }

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

    async fn flush(&self, _ctx: &RequestContext) -> crate::nbd::block_device::Result<()> {
        let mut structure = self.structure.write().unwrap();
        structure.flush()?;
        Ok(())
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
        }
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
