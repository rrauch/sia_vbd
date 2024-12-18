use crate::ClientEndpoint;
use async_trait::async_trait;
use futures::{AsyncRead, AsyncWrite};
use std::fmt::Display;

#[async_trait]
pub(super) trait Listener: Sized {
    type BindAddr;
    type ListenAddr: Display;
    type Conn: Connection;

    async fn bind(addr: Self::BindAddr) -> anyhow::Result<Self>;

    async fn accept(&self) -> anyhow::Result<Self::Conn>;

    fn addr(&self) -> &Self::ListenAddr;
}

#[async_trait]
pub(super) trait Connection: Send {
    type Reader: AsyncRead + Send + Unpin + 'static;
    type Writer: AsyncWrite + Send + Unpin + 'static;

    fn client_endpoint(&self) -> &ClientEndpoint;
    fn into_split(self) -> (Self::Reader, Self::Writer);
}

pub(super) mod tcp {
    use crate::connection::{Connection, Listener};
    use crate::ClientEndpoint;
    use async_trait::async_trait;
    use std::net::SocketAddr;
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::{TcpListener as TokioTcpListener, TcpStream};
    use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

    pub(crate) struct TcpListener {
        inner: TokioTcpListener,
        addr: SocketAddr,
    }

    #[async_trait]
    impl Listener for TcpListener {
        type BindAddr = String;
        type ListenAddr = SocketAddr;
        type Conn = TcpConnection;

        async fn bind(addr: Self::BindAddr) -> anyhow::Result<Self> {
            let inner = TokioTcpListener::bind(addr).await?;
            let addr = inner.local_addr()?;
            Ok(Self { inner, addr })
        }

        async fn accept(&self) -> anyhow::Result<Self::Conn> {
            let (stream, addr) = self.inner.accept().await?;
            stream
                .set_nodelay(true)
                .expect("failed to set no_delay for tcp connection");
            Ok(TcpConnection {
                inner: stream,
                client_endpoint: ClientEndpoint::Tcp(addr),
            })
        }

        fn addr(&self) -> &Self::ListenAddr {
            &self.addr
        }
    }

    pub(crate) struct TcpConnection {
        inner: TcpStream,
        client_endpoint: ClientEndpoint,
    }

    #[async_trait]
    impl Connection for TcpConnection {
        type Reader = Compat<OwnedReadHalf>;
        type Writer = Compat<OwnedWriteHalf>;

        fn client_endpoint(&self) -> &ClientEndpoint {
            &self.client_endpoint
        }

        fn into_split(self) -> (Self::Reader, Self::Writer) {
            let (rx, tx) = self.inner.into_split();
            (rx.compat(), tx.compat_write())
        }
    }
}

#[cfg(unix)]
pub(super) mod unix {
    use crate::connection::{Connection, Listener};
    use crate::unix::UnixAddr;
    use crate::ClientEndpoint;
    use async_trait::async_trait;
    use std::path::PathBuf;
    use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::{UnixListener as TokioUnixListener, UnixStream};
    use tokio_util::compat::{Compat, TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

    pub(crate) struct UnixListener {
        inner: TokioUnixListener,
        addr: UnixAddr,
    }

    #[async_trait]
    impl Listener for UnixListener {
        type BindAddr = PathBuf;
        type ListenAddr = UnixAddr;
        type Conn = UnixConnection;

        async fn bind(addr: Self::BindAddr) -> anyhow::Result<Self> {
            let inner = TokioUnixListener::bind(addr)?;
            let addr = inner.local_addr()?;
            Ok(Self {
                inner,
                addr: UnixAddr(addr.into()),
            })
        }

        async fn accept(&self) -> anyhow::Result<Self::Conn> {
            let (stream, addr) = self.inner.accept().await?;
            Ok(UnixConnection {
                inner: stream,
                client_endpoint: ClientEndpoint::Unix(UnixAddr(addr.into())),
            })
        }

        fn addr(&self) -> &Self::ListenAddr {
            &self.addr
        }
    }

    pub(crate) struct UnixConnection {
        inner: UnixStream,
        client_endpoint: ClientEndpoint,
    }

    #[async_trait]
    impl Connection for UnixConnection {
        type Reader = Compat<OwnedReadHalf>;
        type Writer = Compat<OwnedWriteHalf>;

        fn client_endpoint(&self) -> &ClientEndpoint {
            &self.client_endpoint
        }

        fn into_split(self) -> (Self::Reader, Self::Writer) {
            let (rx, tx) = self.inner.into_split();
            (rx.compat(), tx.compat_write())
        }
    }
}
