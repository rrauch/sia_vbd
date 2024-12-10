use crate::{AsyncReadBytesExt, AsyncWriteBytesExt};
use async_trait::async_trait;
use futures::{AsyncRead, AsyncWrite};
use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub struct RequestContext {
    cookie: u64,
    client_addr: SocketAddr,
}

impl RequestContext {
    pub(super) fn new(cookie: u64, client_addr: SocketAddr) -> Self {
        Self {
            cookie,
            client_addr,
        }
    }

    pub fn cookie(&self) -> u64 {
        self.cookie
    }

    pub fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }
}

#[derive(Debug, Clone)]
pub struct Options {
    /// Human readable description
    pub description: Option<String>,
    /// Size in bytes
    pub size: u64,
    /// Block Device is read-only
    pub read_only: bool,
    /// Block Device has characteristics of rotational media
    pub rotational: bool,
    /// `trim` is supported
    pub trim: bool,
    /// Fast zeroing is supported
    pub fast_zeroes: bool,
    /// Block Device can be resized
    pub resizable: bool,
    /// Block size preferences
    pub block_size: Option<(u32, u32)>,
}

#[async_trait]
#[allow(unused_variables)]
pub trait Handler {
    fn options(&self) -> Options;

    async fn read(
        &self,
        offset: u64,
        length: u64,
        out: &mut (dyn AsyncWrite + Send + Unpin),
        ctx: &RequestContext,
    ) -> std::io::Result<()>;

    async fn write(
        &self,
        offset: u64,
        length: u64,
        fua: bool,
        data: &mut (dyn AsyncRead + Send + Unpin),
        ctx: &RequestContext,
    ) -> std::io::Result<()>;

    async fn write_zeroes(
        &self,
        offset: u64,
        length: u64,
        no_hole: bool,
        ctx: &RequestContext,
    ) -> std::io::Result<()>;

    async fn flush(&self, ctx: &RequestContext) -> std::io::Result<()>;

    async fn trim(&self, offset: u64, length: u64, ctx: &RequestContext) -> std::io::Result<()> {
        unimplemented!("trim")
    }

    async fn resize(&self, new_size: u64, ctx: &RequestContext) -> std::io::Result<()> {
        unimplemented!("resize")
    }
}

pub struct DummyHandler {
    description: Option<String>,
    size: u64,
    read_only: bool,
}

impl DummyHandler {
    pub fn new(description: Option<impl ToString>, size: u64, read_only: bool) -> Self {
        Self {
            description: description.map(|s| s.to_string()),
            size,
            read_only,
        }
    }
}

#[async_trait]
impl Handler for DummyHandler {
    fn options(&self) -> Options {
        println!("info");
        Options {
            description: self.description.clone(),
            size: self.size,
            read_only: self.read_only,
            trim: true,
            rotational: false,
            fast_zeroes: true,
            resizable: false,
            block_size: Some((1024, 1024 * 16)),
        }
    }

    async fn read(
        &self,
        offset: u64,
        length: u64,
        out: &mut (dyn AsyncWrite + Send + Unpin),
        _ctx: &RequestContext,
    ) -> std::io::Result<()> {
        out.write_zeroes(length as usize).await?;
        eprintln!("read {} bytes at offset {}", length, offset);
        Ok(())
    }

    async fn write(
        &self,
        offset: u64,
        length: u64,
        _fua: bool,
        data: &mut (dyn AsyncRead + Send + Unpin),
        _ctx: &RequestContext,
    ) -> std::io::Result<()> {
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
    ) -> std::io::Result<()> {
        eprintln!("zeroed {} bytes at offset {}", length, offset);
        Ok(())
    }

    async fn flush(&self, _ctx: &RequestContext) -> std::io::Result<()> {
        eprintln!("flush called");
        Ok(())
    }

    async fn trim(&self, offset: u64, length: u64, _ctx: &RequestContext) -> std::io::Result<()> {
        eprintln!("trim called for offset {} and length {}", offset, length);
        Ok(())
    }
}
