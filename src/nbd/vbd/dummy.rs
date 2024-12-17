use crate::nbd::block_device::{read_reply, BlockDevice, Options, RequestContext};
use crate::AsyncReadBytesExt;
use async_trait::async_trait;
use futures::{AsyncRead, AsyncWrite};

pub struct DummyBlockDevice {
    description: Option<String>,
    size: u64,
    read_only: bool,
}

impl DummyBlockDevice {
    pub fn new(description: Option<impl ToString>, size: u64, read_only: bool) -> Self {
        Self {
            description: description.map(|s| s.to_string()),
            size,
            read_only,
        }
    }
}

#[async_trait]
impl BlockDevice for DummyBlockDevice {
    fn options(&self) -> Options {
        eprintln!("options requested");
        Options {
            description: self.description.clone(),
            size: self.size,
            read_only: self.read_only,
            trim: true,
            rotational: false,
            fast_zeroes: true,
            resizable: false,
            block_size: Some((1024 * 4, 1024 * 256)),
        }
    }

    async fn read(
        &self,
        offset: u64,
        length: u64,
        queue: &mut read_reply::Queue,
        _ctx: &RequestContext,
    ) {
        queue
            .zeroes(offset, length)
            .await
            .expect("queue closed prematurely");

        /*queue
        .data(offset, length, BytesMut::zeroed(length as usize))
        .await
        .expect("queue closed prematurely");*/

        /*queue
        .data(
            offset,
            length,
            DummyWriter {
                length: length as usize,
            },
        )
        .await
        .expect("queue closed prematurely");*/

        /*queue
        .error(offset, length, Error::ReadQueueError)
        .await
        .expect("queue closed prematurely");*/
    }

    async fn write(
        &self,
        offset: u64,
        length: u64,
        _fua: bool,
        data: &mut (dyn AsyncRead + Send + Unpin),
        _ctx: &RequestContext,
    ) -> crate::nbd::block_device::Result<()> {
        data.skip(length as usize).await?;
        eprintln!("wrote {} bytes at offset {}", length, offset);
        Ok(())
    }

    async fn write_zeroes(
        &self,
        offset: u64,
        length: u64,
        _no_hole: bool,
        _ctx: &RequestContext,
    ) -> crate::nbd::block_device::Result<()> {
        eprintln!("zeroed {} bytes at offset {}", length, offset);
        Ok(())
    }

    async fn flush(&self, _ctx: &RequestContext) -> crate::nbd::block_device::Result<()> {
        eprintln!("flush called");
        Ok(())
    }

    async fn trim(
        &self,
        offset: u64,
        length: u64,
        _ctx: &RequestContext,
    ) -> crate::nbd::block_device::Result<()> {
        eprintln!("trim called for offset {} and length {}", offset, length);
        Ok(())
    }
}

struct DummyWriter {
    length: usize,
}

#[async_trait]
impl read_reply::PayloadWriter for DummyWriter {
    async fn write(
        self: Box<Self>,
        out: &mut (dyn AsyncWrite + Send + Unpin),
    ) -> std::io::Result<()> {
        use crate::AsyncWriteBytesExt;
        out.write_zeroes(self.length).await
    }
}
