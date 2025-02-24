pub(super) mod fragment;
mod read;
mod reply;
mod request;

use crate::io::{AsyncReadBytesExt, CountingReader, LimitedReader, WrappedReader};
use crate::nbd::block_device::RequestContext;
use crate::nbd::transmission::reply::ErrorType;
use crate::nbd::transmission::request::{ReadError, RequestId};
use crate::nbd::{Export, TransmissionMode};
use crate::ClientEndpoint;
use futures::lock::Mutex;
use futures::{AsyncRead, AsyncWrite};
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Command {
    Read {
        offset: u64,
        length: u64,
        dont_fragment: bool,
    },
    Write {
        offset: u64,
        length: u64,
        fua: bool,
    },
    Disconnect,
    Flush,
    Trim {
        offset: u64,
        length: u64,
    },
    Cache {
        offset: u64,
        length: u64,
    },
    WriteZeroes {
        offset: u64,
        length: u64,
        no_hole: bool,
        fast_only: bool,
    },
    BlockStatus,
    Resize {
        length: u64,
    },
}

pub(super) struct TransmissionHandler {
    export: Export,
    transmission_mode: TransmissionMode,
    client_endpoint: ClientEndpoint,
    shutdown_ct: CancellationToken,
}

impl TransmissionHandler {
    pub(super) fn new(
        export: Export,
        transmission_mode: TransmissionMode,
        client_endpoint: ClientEndpoint,
        shutdown_ct: CancellationToken,
    ) -> Self {
        Self {
            export,
            transmission_mode,
            client_endpoint,
            shutdown_ct,
        }
    }

    pub(super) async fn process<
        RX: AsyncRead + Unpin + Send + 'static,
        TX: AsyncWrite + Unpin + Send + 'static,
    >(
        &self,
        rx: RX,
        tx: TX,
    ) -> Result<(), NbdError> {
        self.export.increase_connection_count();
        let res = self._process(rx, tx).await;
        if self.export.decrease_connection_count() == 0 && !self.export.read_only() {
            // this is the end of the last connection
            // issue final flush
            let ctx = RequestContext::new(69234560174454211, self.client_endpoint.clone());
            let _ = self.export.block_device.flush(&ctx).await;
        }
        res
    }

    async fn _process<
        RX: AsyncRead + Unpin + Send + 'static,
        TX: AsyncWrite + Unpin + Send + 'static,
    >(
        &self,
        mut rx: RX,
        tx: TX,
    ) -> Result<(), NbdError> {
        let shutdown_ct = self.shutdown_ct.clone();
        let ct = CancellationToken::new();
        let _drop_guard = ct.clone().drop_guard();

        let transmission_mode = self.transmission_mode;
        let writer = Arc::new(reply::Writer::new(tx, transmission_mode));

        let mut request_reader = request::Reader::new(transmission_mode);
        let read_handler = read::ReadCommandHandler::new(
            transmission_mode,
            self.export.block_device.clone(),
            self.client_endpoint.clone(),
            writer.clone(),
            ct.clone(),
        );

        loop {
            let req = tokio::select! {
                req = request_reader.read_next(&mut rx) => {
                    req?
                },
                _ = ct.cancelled() => {
                    eprintln!("connection cancelled");
                    break;
                },
                _ = shutdown_ct.cancelled() => {
                    eprintln!("shutdown signal received, prepare shutdown");
                    writer.io_error(
                        ServerShutdown,
                        //todo: clarify what request id should be sent
                        RequestId {
                            cookie: 0,
                            offset: 0,
                        }
                    ).await?;
                    break;
                }
            };
            let ctx = RequestContext::new(req.id.cookie, self.client_endpoint.clone());
            let mut payload_remaining = req.payload_length as usize;

            let block_device = &self.export.block_device;
            let read_only = self.export.read_only();
            let info = self.export.options();

            match req.command {
                Command::Read {
                    offset,
                    length,
                    dont_fragment,
                } => {
                    read_handler.handle(offset, length, dont_fragment, req.id, ctx.clone());
                }
                Command::Write {
                    offset,
                    length,
                    fua,
                } if !read_only => {
                    let arc_rx = Arc::new(Mutex::new(CountingReader::new(rx)));
                    let lock = arc_rx.clone().lock_owned().await;
                    let mut payload =
                        LimitedReader::new(WrappedReader(lock), req.payload_length as usize);
                    let block_device = block_device.clone();
                    let req_id = req.id;
                    let writer = writer.clone();

                    tokio::spawn(async move {
                        match block_device
                            .write(offset, length, fua, &mut payload, &ctx)
                            .await
                        {
                            Ok(()) => {
                                let _ = writer.done(req_id).await;
                            }
                            Err(err) => {
                                let _ = writer.io_error(&err, req_id).await;
                            }
                        }
                    });

                    let _ = arc_rx.lock().await;

                    let counting = Arc::into_inner(arc_rx)
                        .expect("exclusive arc_rx")
                        .into_inner();
                    let read = counting.read();
                    rx = counting.into_inner();
                    payload_remaining = (req.payload_length as usize) - read;
                }
                Command::WriteZeroes {
                    offset,
                    length,
                    fast_only,
                    no_hole,
                } if !read_only && ((fast_only && info.fast_zeroes) || (!fast_only)) => {
                    let block_device = block_device.clone();
                    let req_id = req.id;
                    let writer = writer.clone();
                    tokio::spawn(async move {
                        match block_device
                            .write_zeroes(offset, length, no_hole, &ctx)
                            .await
                        {
                            Ok(()) => {
                                let _ = writer.done(req_id).await;
                            }
                            Err(err) => {
                                let _ = writer.io_error(&err, req_id).await;
                            }
                        }
                    });
                }
                Command::Flush if !read_only => {
                    let block_device = block_device.clone();
                    let req_id = req.id;
                    let writer = writer.clone();
                    tokio::spawn(async move {
                        match block_device.flush(&ctx).await {
                            Ok(()) => {
                                let _ = writer.done(req_id).await;
                            }
                            Err(err) => {
                                let _ = writer.io_error(&err, req_id).await;
                            }
                        }
                    });
                }
                Command::Resize { length } if info.resizable && !read_only => {
                    let block_device = block_device.clone();
                    let req_id = req.id;
                    let writer = writer.clone();
                    let export = self.export.clone();
                    tokio::spawn(async move {
                        match block_device.resize(length, &ctx).await {
                            Ok(()) => {
                                // update the exports info
                                export.update_options(block_device.options().await);
                                let _ = writer.done(req_id).await;
                            }
                            Err(err) => {
                                let _ = writer.io_error(&err, req_id).await;
                            }
                        }
                    });
                }
                Command::Trim { offset, length } if info.trim && !read_only => {
                    let block_device = block_device.clone();
                    let req_id = req.id;
                    let writer = writer.clone();
                    tokio::spawn(async move {
                        match block_device.trim(offset, length, &ctx).await {
                            Ok(()) => {
                                let _ = writer.done(req_id).await;
                            }
                            Err(err) => {
                                let _ = writer.io_error(&err, req_id).await;
                            }
                        }
                    });
                }
                Command::Cache { offset, length } => {
                    let block_device = block_device.clone();
                    let req_id = req.id;
                    let writer = writer.clone();
                    tokio::spawn(async move {
                        match block_device.cache(offset, length, &ctx).await {
                            Ok(()) => {
                                let _ = writer.done(req_id).await;
                            }
                            Err(err) => {
                                let _ = writer.io_error(&err, req_id).await;
                            }
                        }
                    });
                }
                Command::Disconnect => {
                    eprintln!("client sent disconnect request");
                    break;
                }
                _ => {
                    // unsupported command
                    writer
                        .unsupported(
                            format!("Command {:?} is unsupported", req.command).to_string(),
                            req.id,
                        )
                        .await?;
                }
            }

            if payload_remaining > 0 {
                // discard any unprocessed payload data
                rx.skip(payload_remaining).await?;
                eprintln!(
                    "warning: {} bytes of unread payload were discarded",
                    payload_remaining
                );
            }
        }

        // cleanly shutdown the writer

        writer.shutdown().await;

        Ok(())
    }
}

struct ServerShutdown;

impl From<ServerShutdown> for ErrorType {
    fn from(_: ServerShutdown) -> Self {
        ErrorType::Shutdown
    }
}

impl Display for ServerShutdown {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "server shutdown imminent")
    }
}

#[derive(Error, Debug)]
pub(super) enum NbdError {
    /// A request related error occurred when reading data from the client
    #[error(transparent)]
    RequestError(#[from] ReadError),
    /// An `IO` error occurred reading from or writing to the client
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    /// The connection had to be terminated
    #[error("termination received")]
    Termination,
    /// Other error, with optional details
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
