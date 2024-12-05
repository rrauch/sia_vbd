mod handshake;
mod transmission;

use futures::{AsyncRead, AsyncWrite};
use std::net::SocketAddr;

const MAX_PAYLOAD_LEN: u32 = 32 * 1024 * 1024; // 32 MiB

/*pub(crate) async fn new_connection<RX: AsyncRead + Unpin, TX: Sink<Bytes> + Unpin>(
    mut rx: RX,
    mut tx: TX,
    addr: SocketAddr,
) -> std::io::Result<()>
where
    std::io::Error: From<<TX as Sink<Bytes>>::Error>,
{
    handshake::process(&mut rx, &mut tx, &addr).await?;
    todo!()
}*/

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TransmissionMode {
    Simple,
    Structured,
    Extended, // extended is a superset of structured
}

pub(crate) async fn new_connection<RX: AsyncRead + Unpin, TX: AsyncWrite + Unpin>(
    mut rx: RX,
    mut tx: TX,
    addr: SocketAddr,
) -> anyhow::Result<()> {
    if let Some(handler) = handshake::process(&mut rx, &mut tx, &addr).await? {
        handler.process(rx, tx).await?;
    } else {
        // handshake ended without intent to proceed to transmission
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct Export {
    /// Internal Identifier of the export
    name: String,
    /// Human readable description
    description: Option<String>,
    /// Size in bytes
    size: u64,
    /// Block Device is read-only
    read_only: bool,
    /// Block Device has characteristics of rotational media
    rotational: bool,
    /// `trim` is supported
    trim: bool,
    /// Fast zeroing is supported
    fast_zeroes: bool,
    /// Block Device can be resized
    resizable: bool,

    /// Block size preferences
    block_size: Option<(u32, u32)>,
}
