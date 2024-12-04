mod handshake;
mod transmission;

use futures::{AsyncRead, AsyncWrite};
use std::net::SocketAddr;

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
}

pub(crate) async fn new_connection<RX: AsyncRead + Unpin, TX: AsyncWrite + Unpin>(
    mut rx: RX,
    mut tx: TX,
    addr: SocketAddr,
) -> anyhow::Result<()> {
    let handler = handshake::process(&mut rx, &mut tx, &addr).await?;
    handler.process(rx, tx).await?;
    Ok(())
}

#[derive(Debug, Clone)]
struct Export {
    name: String,
    size: u64,
    read_only: bool,
    resizable: bool,
    rotational: bool,
    trim: bool,
}
