pub mod handler;
pub(crate) mod handshake;
mod transmission;

use crate::nbd::handler::{Handler, Options};
use crate::nbd::handshake::Handshaker;
use crate::ClientEndpoint;
use futures::{AsyncRead, AsyncWrite};
use std::sync::{Arc, Mutex};

const MAX_PAYLOAD_LEN: u32 = 32 * 1024 * 1024; // 32 MiB

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
    handler: Arc<dyn Handler + Send + Sync + 'static>,
    options: Arc<Mutex<Arc<Options>>>,
}

impl Export {
    pub fn new<T: Handler + Send + Sync + 'static>(
        name: String,
        handler: T,
        forced_read_only: bool,
    ) -> Self {
        let handler = Arc::new(handler);
        let options = Arc::new(Mutex::new(Arc::new(handler.options())));

        Self {
            name,
            forced_read_only,
            handler,
            options,
        }
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
