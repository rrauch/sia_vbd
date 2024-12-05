use bytes::{Bytes, BytesMut};
use futures::AsyncReadExt;
use std::cmp::min;
use tokio::net::TcpListener;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

mod nbd;

pub async fn run(addr: &str) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on {}", listener.local_addr()?);
    loop {
        let (socket, addr) = listener.accept().await?;
        println!("New connection from {}", addr);

        tokio::spawn(async move {
            let (rx, tx) = socket.into_split();
            let rx = rx.compat();
            //let tx = FramedWrite::new(tx, BytesCodec::new());
            let tx = tx.compat_write();

            if let Err(error) = nbd::new_connection(rx, tx, addr.clone()).await {
                eprintln!("error {:?}, client address {}", error, addr);
            }
            println!("connection closed for {}", addr);
        });
    }
}

pub(crate) trait AsyncReadBytesExt: AsyncReadExt + Unpin {
    /// Reads exactly `n` bytes into a new buffer
    async fn get_exact(&mut self, n: usize) -> std::io::Result<Bytes> {
        let mut buf = BytesMut::zeroed(n);
        self.read_exact(buf.as_mut()).await?;
        Ok(buf.freeze())
    }

    /// Skips exactly `n` bytes from the reader
    async fn skip(&mut self, n: usize) -> std::io::Result<()> {
        //todo: find option that does not need allocating
        let mut remaining = n;
        let mut buffer = [0u8; 8192];
        while remaining > 0 {
            let to_read = min(remaining, buffer.len());
            let bytes_read = self.read(&mut buffer[..to_read]).await?;
            if bytes_read == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Reached EOF",
                ));
            }
            remaining -= bytes_read;
        }
        Ok(())
    }
}
// Blanket implementation for all types that implement AsyncReadExt
impl<T: AsyncReadExt + ?Sized + Unpin> AsyncReadBytesExt for T {}

/*pub(crate) trait AsyncWriteBytesExt: AsyncWriteExt + Unpin {
}
// Blanket implementation for all types that implement AsyncReadExt
impl<T: AsyncWriteExt + ?Sized + Unpin> AsyncWriteBytesExt for T {}*/
