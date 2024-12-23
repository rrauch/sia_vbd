pub mod block_device;
mod connection;
mod handshake;
mod transmission;
pub mod vbd;

use crate::{is_power_of_two, ClientEndpoint, ListenEndpoint};
use block_device::{BlockDevice, Options};
use connection::tcp::TcpListener;
use connection::{Connection, Listener};
use futures::{AsyncRead, AsyncWrite};
use handshake::Handshaker;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use thiserror::Error;

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
}

impl Export {
    fn new<T: BlockDevice + Send + Sync + 'static>(
        name: String,
        block_device: T,
        forced_read_only: bool,
    ) -> Result<Self, ExportError> {
        let block_device = Arc::new(block_device);
        let options = block_device.options();
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
}

impl Builder {
    pub fn tcp<S: ToString>(host: S, port: u16) -> Self {
        Self {
            listen_endpoint: ListenEndpoint::Tcp(format!("{}:{}", host.to_string(), port)),
            exports: HashMap::default(),
            default_export: None,
            structured_replies_disabled: false,
            extended_headers_disabled: false,
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
        }
    }

    pub fn with_export<S: ToString, H: BlockDevice + Send + Sync + 'static>(
        mut self,
        name: S,
        block_device: H,
        force_read_only: bool,
    ) -> Result<Self, anyhow::Error> {
        let name = name.to_string();
        let export = Export::new(name.clone(), block_device, force_read_only)?;
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

    pub fn build(self) -> Runner {
        let handshaker = Handshaker::new(
            self.exports,
            self.default_export,
            self.structured_replies_disabled,
            self.extended_headers_disabled,
        );
        Runner {
            listen_endpoint: self.listen_endpoint,
            handshaker: Arc::new(handshaker),
        }
    }
}

pub struct Runner {
    listen_endpoint: ListenEndpoint,
    handshaker: Arc<Handshaker>,
}

impl Runner {
    pub async fn run(&self) -> anyhow::Result<()> {
        match &self.listen_endpoint {
            ListenEndpoint::Tcp(addr) => {
                self._run(TcpListener::bind(addr.to_string()).await?)
                    .await?
            }
            #[cfg(unix)]
            ListenEndpoint::Unix(path) => {
                self._run(connection::unix::UnixListener::bind(path.to_path_buf()).await?)
                    .await?
            }
        };
        Ok(())
    }

    async fn _run<T: Listener>(&self, listener: T) -> anyhow::Result<()> {
        println!("Listening on {}", listener.addr());
        loop {
            let conn = listener.accept().await?;
            println!("New connection from {}", conn.client_endpoint());

            let handshaker = self.handshaker.clone();
            let client_endpoint = conn.client_endpoint().clone();
            let (rx, tx) = conn.into_split();

            tokio::spawn(async move {
                if let Err(error) =
                    new_connection(&handshaker, rx, tx, client_endpoint.clone()).await
                {
                    eprintln!("error {:?}, client endpoint {}", error, client_endpoint);
                }
                println!("connection closed for {}", client_endpoint);
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
