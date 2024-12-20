pub mod block_device;
pub(crate) mod handshake;
mod transmission;
pub mod vbd;

use crate::nbd::block_device::{BlockDevice, Options};
use crate::nbd::handshake::Handshaker;
use crate::{is_power_of_two, ClientEndpoint};
use futures::{AsyncRead, AsyncWrite};
use std::sync::{Arc, Mutex};
use thiserror::Error;

const MAX_PAYLOAD_LEN: u32 = 33 * 1024 * 1024; // 33 MiB

// for optimal compatibility this should not be changed
const MIN_BLOCK_SIZE: u32 = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TransmissionMode {
    Simple,
    Structured,
    Extended, // extended is a superset of structured
}

pub(crate) async fn new_connection<
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
pub(crate) struct Export {
    name: String,
    forced_read_only: bool,
    block_device: Arc<dyn BlockDevice + Send + Sync + 'static>,
    options: Arc<Mutex<Arc<Options>>>,
}

impl Export {
    pub fn new<T: BlockDevice + Send + Sync + 'static>(
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
pub(super) enum ExportError {
    #[error(transparent)]
    InvalidBlockSize(#[from] BlockSizeError),
}

#[derive(Error, Debug)]
pub(super) enum BlockSizeError {
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
