pub mod block_device;
mod connection;
mod handshake;
mod transmission;
pub mod vbd;

use crate::{is_power_of_two, ClientEndpoint, ListenEndpoint};
use anyhow::bail;
use block_device::{BlockDevice, Options};
use connection::tcp::TcpListener;
use connection::{Connection, Listener};
use futures::{AsyncRead, AsyncWrite};
use handshake::Handshaker;
use std::cmp::min;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::Semaphore;
use tokio_util::sync::{CancellationToken, DropGuard};

const MAX_PAYLOAD_LEN: u32 = 32 * 1024 * 1024; // 32 MiB

// for optimal compatibility this should not be changed
const MIN_BLOCK_SIZE: u32 = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TransmissionMode {
    Simple,
    Structured,
    Extended, // extended is a superset of structured
}

async fn new_connection<
    RX: AsyncRead + Unpin + Send + 'static,
    TX: AsyncWrite + Unpin + Send + 'static,
>(
    handshaker: &Handshaker,
    mut rx: RX,
    mut tx: TX,
    client_endpoint: ClientEndpoint,
) -> anyhow::Result<()> {
    if let Some(handler) = handshaker
        .process(&mut rx, &mut tx, &client_endpoint)
        .await?
    {
        handler.process(rx, tx).await?;
    } else {
        // handshake ended without intent to proceed to transmission
    }

    Ok(())
}

#[derive(Clone)]
struct Export {
    name: String,
    forced_read_only: bool,
    block_device: Arc<dyn BlockDevice + Send + Sync + 'static>,
    options: Arc<Mutex<Arc<Options>>>,
    connection_count: Arc<Mutex<usize>>,
}

impl Export {
    async fn new<T: BlockDevice + Send + Sync + 'static>(
        name: String,
        block_device: T,
        forced_read_only: bool,
    ) -> Result<Self, ExportError> {
        let block_device = Arc::new(block_device);
        let options = block_device.options().await;
        let min = MIN_BLOCK_SIZE;
        let preferred = options.block_size;

        if min > preferred {
            return Err(BlockSizeError::MinSizeExceedsPreferred { min, preferred }.into());
        }
        if preferred > MAX_PAYLOAD_LEN {
            return Err(BlockSizeError::PreferredSizeTooLarge { size: preferred }.into());
        }
        if preferred % min != 0 {
            return Err(BlockSizeError::PreferredSizeNotMultipleOfMin { min, preferred }.into());
        }
        if !is_power_of_two(min) {
            return Err(BlockSizeError::NotPowerOfTwo { size: min }.into());
        }
        if !is_power_of_two(preferred) {
            return Err(BlockSizeError::NotPowerOfTwo { size: preferred }.into());
        }

        let options = Arc::new(Mutex::new(Arc::new(options)));

        Ok(Self {
            name,
            forced_read_only,
            block_device,
            options,
            connection_count: Arc::new(Mutex::new(0)),
        })
    }

    pub fn read_only(&self) -> bool {
        if self.forced_read_only {
            true
        } else {
            self.options.lock().unwrap().read_only
        }
    }

    pub fn options(&self) -> Arc<Options> {
        self.options.lock().unwrap().clone()
    }

    pub fn update_options(&self, new_options: Options) {
        let mut lock = self.options.lock().unwrap();
        *lock = Arc::new(new_options);
    }

    pub fn increase_connection_count(&self) -> usize {
        let mut lock = self.connection_count.lock().unwrap();
        *lock += 1;
        *lock
    }

    pub fn decrease_connection_count(&self) -> usize {
        let mut lock = self.connection_count.lock().unwrap();
        *lock = lock.saturating_sub(1);
        *lock
    }

    fn into_inner(self) -> Arc<dyn BlockDevice + Send + Sync + 'static> {
        self.block_device
    }
}

#[derive(Error, Debug)]
enum ExportError {
    #[error(transparent)]
    InvalidBlockSize(#[from] BlockSizeError),
}

pub struct Builder {
    listen_endpoint: ListenEndpoint,
    exports: HashMap<String, Export>,
    default_export: Option<String>,
    structured_replies_disabled: bool,
    extended_headers_disabled: bool,
    max_simultaneous_connections: u32,
}

impl Builder {
    pub fn tcp<S: ToString>(host: S, port: u16) -> Self {
        Self {
            listen_endpoint: ListenEndpoint::Tcp(format!("{}:{}", host.to_string(), port)),
            exports: HashMap::default(),
            default_export: None,
            structured_replies_disabled: false,
            extended_headers_disabled: false,
            max_simultaneous_connections: u32::MAX,
        }
    }

    #[cfg(unix)]
    pub fn unix<P: AsRef<Path>>(socket_path: P) -> Self {
        Self {
            listen_endpoint: ListenEndpoint::Unix(socket_path.as_ref().to_path_buf()),
            exports: HashMap::default(),
            default_export: None,
            structured_replies_disabled: false,
            extended_headers_disabled: false,
            max_simultaneous_connections: u32::MAX,
        }
    }

    pub async fn with_export<S: ToString, H: BlockDevice + Send + Sync + 'static>(
        mut self,
        name: S,
        block_device: H,
        force_read_only: bool,
    ) -> Result<Self, anyhow::Error> {
        let name = name.to_string();
        let export = Export::new(name.clone(), block_device, force_read_only).await?;
        self.exports.insert(name, export);
        Ok(self)
    }

    pub fn with_default_export<S: ToString>(mut self, name: S) -> Result<Self, anyhow::Error> {
        let name = name.to_string();
        if !self.exports.contains_key(&name) {
            anyhow::bail!("unknown export: {}", name);
        }
        self.default_export = Some(name.to_string());
        Ok(self)
    }

    pub fn disable_structured_replies(mut self) -> Self {
        self.structured_replies_disabled = true;
        self.extended_headers_disabled = true;
        self
    }

    pub fn disable_extended_headers(mut self) -> Self {
        self.extended_headers_disabled = true;
        self
    }

    pub fn max_connections(mut self, max_simultaneous_connections: u32) -> Self {
        self.max_simultaneous_connections = max_simultaneous_connections;
        self
    }

    pub fn build(self) -> (Runner, RunGuard) {
        let shutdown_ct = CancellationToken::new();
        let guard = shutdown_ct.clone().drop_guard();
        let handshaker = Handshaker::new(
            self.exports,
            self.default_export,
            self.structured_replies_disabled,
            self.extended_headers_disabled,
            shutdown_ct.clone(),
        );
        (
            Runner {
                listen_endpoint: self.listen_endpoint,
                handshaker: Some(Arc::new(handshaker)),
                shutdown_ct,
                max_connections: self.max_simultaneous_connections,
                conn_permits: Arc::new(Semaphore::new(self.max_simultaneous_connections as usize)),
            },
            guard,
        )
    }
}

pub type RunGuard = DropGuard;

pub struct Runner {
    listen_endpoint: ListenEndpoint,
    handshaker: Option<Arc<Handshaker>>,
    shutdown_ct: CancellationToken,
    max_connections: u32,
    conn_permits: Arc<Semaphore>,
}

impl Runner {
    pub async fn run(&mut self) -> anyhow::Result<()> {
        let res = self._run().await;
        self.shutdown_ct.cancel();
        // waiting for shutdown to complete
        let _ = self.conn_permits.acquire_many(self.max_connections).await?;
        println!("all connections closed");
        let mut handshaker = match self.handshaker.take() {
            Some(handshaker) => handshaker,
            None => {
                bail!("handshaker gone");
            }
        };
        let wait_deadline = Instant::now() + Duration::from_secs(10);
        println!("waiting for all tasks to finish");
        let mut hs = None;
        loop {
            match Arc::try_unwrap(handshaker) {
                Ok(success) => {
                    hs = Some(success);
                    break;
                }
                Err(hs) => {
                    handshaker = hs;
                }
            }
            let now = Instant::now();
            if now >= wait_deadline {
                break;
            }
            let wait_duration =
                Duration::from_millis(min((wait_deadline - now).as_millis() as u64, 100));
            tokio::time::sleep(wait_duration).await;
        }
        if hs.is_none() {
            bail!("waiting for tasks timed out");
        }
        let hs = hs.unwrap();
        println!("closing all exports");
        hs.close().await;
        println!("shutdown complete");
        res
    }

    async fn _run(&mut self) -> anyhow::Result<()> {
        match &self.listen_endpoint {
            ListenEndpoint::Tcp(addr) => {
                self.accept(TcpListener::bind(addr.to_string()).await?)
                    .await?
            }
            #[cfg(unix)]
            ListenEndpoint::Unix(path) => {
                self.accept(connection::unix::UnixListener::bind(path.to_path_buf()).await?)
                    .await?
            }
        };
        Ok(())
    }

    async fn accept<T: Listener>(&mut self, listener: T) -> anyhow::Result<()> {
        println!("Listening on {}", listener.addr());
        let ct = self.shutdown_ct.clone();
        loop {
            let permit = tokio::select! {
                permit = self.conn_permits.clone().acquire_owned () => {
                    permit
                }
                _ = ct.cancelled() => {
                    println!("not accepting any more connections");
                    return Ok(());
                }
            }?;

            let conn = tokio::select! {
                res = listener.accept() => {
                    res
                }
                _ = ct.cancelled() => {
                    println!("not accepting any more connections");
                    return Ok(());
                }
            }?;

            println!("New connection from {}", conn.client_endpoint());

            let handshaker = self.handshaker.clone();
            let client_endpoint = conn.client_endpoint().clone();
            let (rx, tx) = conn.into_split();
            tokio::spawn(async move {
                if let Err(error) = new_connection(
                    handshaker.as_ref().unwrap(),
                    rx,
                    tx,
                    client_endpoint.clone(),
                )
                .await
                {
                    eprintln!("error {:?}, client endpoint {}", error, client_endpoint);
                }
                println!("connection closed for {}", client_endpoint);
                drop(permit);
            });
        }
    }
}

#[derive(Error, Debug)]
enum BlockSizeError {
    /// Min Block Size exceeds preferred size
    #[error("minimum block size {min} must not exceed preferred size {preferred}")]
    MinSizeExceedsPreferred { min: u32, preferred: u32 },
    /// Preferred Block Size exceeds max size
    #[error("preferred block size {size} exceeds maximum of {max} bytes", max = MAX_PAYLOAD_LEN)]
    PreferredSizeTooLarge { size: u32 },
    /// Preferred Bock Size needs to be a multiple of Min Block Size
    #[error("preferred block size {preferred} must be a multiple of minimum block size {min}")]
    PreferredSizeNotMultipleOfMin { min: u32, preferred: u32 },
    #[error("block size {size} is not a power of two")]
    NotPowerOfTwo { size: u32 },
}
