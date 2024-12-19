use crate::nbd::block_device::read_reply::{Payload as ReadReplyPayload, Queue as ReadReplyQueue};
use crate::nbd::block_device::{BlockDevice, RequestContext};
use crate::nbd::transmission::fragment::{Fragment, FragmentPotential, FragmentProcessor};
use crate::nbd::transmission::reply::{Write, WriteCommand, Writer};
use crate::nbd::transmission::request::RequestId;
use crate::nbd::TransmissionMode;
use crate::ClientEndpoint;
use std::collections::VecDeque;
use std::io::{Error, ErrorKind};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::{CancellationToken, DropGuard};

pub(super) struct ReadCommandHandler {
    transmission_mode: TransmissionMode,
    block_device: Arc<dyn BlockDevice + Send + Sync + 'static>,
    client_endpoint: ClientEndpoint,
    writer: Arc<Writer>,
    cancellation_token: CancellationToken,
    _drop_guard: DropGuard,
}

impl ReadCommandHandler {
    pub fn new(
        transmission_mode: TransmissionMode,
        block_device: Arc<dyn BlockDevice + Send + Sync + 'static>,
        client_endpoint: ClientEndpoint,
        writer: Arc<Writer>,
    ) -> Self {
        let ct = CancellationToken::new();
        let drop_guard = ct.clone().drop_guard();
        Self {
            transmission_mode,
            block_device,
            client_endpoint,
            writer,
            cancellation_token: ct,
            _drop_guard: drop_guard,
        }
    }

    pub fn handle(
        &self,
        offset: u64,
        length: u64,
        dont_fragment: bool,
        request_id: RequestId,
        ctx: RequestContext,
    ) {
        let block_device = self.block_device.clone();
        let req_id = request_id;
        let writer = self.writer.clone();
        let transmission_mode = self.transmission_mode;
        let ct = self.cancellation_token.clone();
        tokio::spawn(async move {
            let (data_tx, mut fragments) = mpsc::channel::<Fragment>(4);
            let mut read_q = ReadReplyQueue::new(data_tx);
            let data_sent = Arc::new(Mutex::new(false));
            let dont_fragment = transmission_mode == TransmissionMode::Simple || dont_fragment;

            let sender_task: JoinHandle<Result<(), std::io::Error>> = {
                let data_sent = data_sent.clone();
                let writer = writer.clone();
                task::spawn(async move {
                    let mut processor = FragmentProcessor::new(offset, length, dont_fragment);
                    let mut multi_tx: Option<mpsc::Sender<Write>> = None;
                    let mut done = processor.is_complete();
                    let mut buffer = VecDeque::new();
                    let mut sent = false;
                    let mut idle_duration = Duration::from_secs(60);

                    while !done {
                        let sleep_deadline = sleep(idle_duration);
                        tokio::pin!(sleep_deadline);

                        tokio::select! {
                            msg = fragments.recv() => {
                                if let Some(fragment) = msg {
                                    processor
                                        .insert(fragment)
                                        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

                                    while let Ok(fragment) = fragments.try_recv() {
                                        processor.insert(fragment).map_err(|e| {
                                            Error::new(ErrorKind::InvalidData, e.to_string())
                                        })?;
                                    }

                                    while let Some(fragment) = processor.next(FragmentPotential::Complete) {
                                        buffer.push_back(fragment);
                                    }

                                    //todo: check if this is actually improves things or not
                                    if buffer.is_empty() && dont_fragment {
                                        while let Some(fragment) = processor.next(FragmentPotential::Incomplete) {
                                            buffer.push_back(fragment);
                                        }
                                    }
                                } else {
                                    // channel is closed
                                    done = true;
                                    processor.drain().for_each(|f| buffer.push_back(f));
                                }
                            }
                            _ = &mut sleep_deadline => {
                                while let Ok(fragment) = fragments.try_recv() {
                                    processor.insert(fragment).map_err(|e| {
                                        Error::new(ErrorKind::InvalidData, e.to_string())
                                    })?;
                                }

                                while let Some(fragment) = processor.next(FragmentPotential::Complete) {
                                    buffer.push_back(fragment);
                                }
                                if buffer.is_empty() {
                                    if let Some(fragment) = processor.next(FragmentPotential::Incomplete) {
                                        buffer.push_back(fragment);
                                    }
                                }
                            }
                        }
                        let is_complete = processor.is_complete();

                        if !done && is_complete {
                            // all fragments for reply accounted for
                            done = true;
                        }

                        if done && !is_complete {
                            return Err(Error::new(
                                ErrorKind::UnexpectedEof,
                                "read fragment channel closed prematurely",
                            ));
                        }

                        if !buffer.is_empty() {
                            let single_fragment_reply = !sent && done && buffer.len() == 1;
                            if !single_fragment_reply && dont_fragment && multi_tx.is_none() {
                                let (tx, rx) = mpsc::channel(3);
                                writer
                                    .send_stream(ReceiverStream::new(rx))
                                    .await
                                    .map_err(|e| {
                                        Error::new(ErrorKind::BrokenPipe, e.to_string())
                                    })?;
                                multi_tx = Some(tx);
                            }

                            let mut fragments_left = buffer.len();
                            while let Some(fragment) = buffer.pop_front() {
                                fragments_left -= 1;

                                let command = match fragment.payload {
                                    ReadReplyPayload::Writer(writer) => WriteCommand::Read {
                                        chunk_offset: fragment.offset,
                                        chunk_length: fragment.length,
                                        request_length: length,
                                        dont_fragment,
                                        writer,
                                    },
                                    ReadReplyPayload::Zeroes => WriteCommand::Zeroes {
                                        chunk_offset: fragment.offset,
                                        chunk_length: fragment.length,
                                        dont_fragment,
                                        request_length: length,
                                    },
                                };

                                let write =
                                    Write::new(command, req_id, !sent, done && fragments_left == 0);

                                if let Some(multi_tx) = multi_tx.as_mut() {
                                    multi_tx.send(write).await.map_err(|e| {
                                        Error::new(ErrorKind::BrokenPipe, e.to_string())
                                    })?;
                                } else {
                                    writer.send(write).await.map_err(|e| {
                                        Error::new(ErrorKind::BrokenPipe, e.to_string())
                                    })?;
                                }

                                if !sent {
                                    sent = true;
                                    *data_sent.lock().unwrap() = true;
                                }
                            }
                        }
                        if !processor.is_empty() && !dont_fragment {
                            idle_duration = Duration::from_millis(5);
                        } else {
                            idle_duration = Duration::from_secs(60);
                        }
                    }
                    Ok(())
                })
            };

            let mut err = None;

            tokio::select! {
                res = block_device.read(offset, length, &mut read_q, &ctx) => {
                    if let Err(e) = res {
                        // error occurred in block device
                        err = Some(e);
                        sender_task.abort();
                    }
                }
                _ = ct.cancelled() => {
                    // stop everything immediately
                    sender_task.abort();
                }
            }

            let mut data_sent = { *data_sent.lock().unwrap() };
            let mut error_sent = false;

            let mut shutdown = false;
            if let Some(err) = err {
                if data_sent {
                    shutdown = true;
                } else {
                    let _ = writer.io_error(&err, req_id).await;
                    error_sent = true;
                    data_sent = true;
                }
            }

            match sender_task.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    if !error_sent {
                        if data_sent {
                            // some data has already been sent
                            // need to shut down the connection
                            //ct.cancel();
                            shutdown = true;
                        }
                        if let Err(_) = writer.io_error(&err, req_id).await {
                            // can't even send the error
                            // shut down connection
                            //ct.cancel();
                            shutdown = true;
                        }
                    }
                }
                Err(_) => {
                    if !error_sent {
                        // something is very off
                        // shut down connection
                        shutdown = true;
                    }
                }
            }

            if shutdown {
                ct.cancel();
            }
        });
    }
}
