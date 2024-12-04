use byteorder::{BigEndian, ByteOrder};
use bytes::{Bytes, BytesMut};
use futures::AsyncReadExt;
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
    /// Reads a `u16` in network byte order from the reader
    async fn get_u16(&mut self) -> std::io::Result<u16> {
        let mut buf = [0u8; 2];
        self.read_exact(&mut buf).await?;
        Ok(BigEndian::read_u16(&buf))
    }

    /// Reads a `u32` in network byte order from the reader
    async fn get_u32(&mut self) -> std::io::Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf).await?;
        Ok(BigEndian::read_u32(&buf))
    }

    /// Reads a `u64` in network byte order from the reader
    async fn get_u64(&mut self) -> std::io::Result<u64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf).await?;
        Ok(BigEndian::read_u64(&buf))
    }

    /// Reads exactly `n` bytes into a new buffer
    async fn get_exact(&mut self, n: usize) -> std::io::Result<Bytes> {
        let mut buf = BytesMut::zeroed(n);
        self.read_exact(buf.as_mut()).await?;
        Ok(buf.freeze())
    }
}
// Blanket implementation for all types that implement AsyncReadExt
impl<T: AsyncReadExt + ?Sized + Unpin> AsyncReadBytesExt for T {}

pub(crate) trait BlockDevice {

}