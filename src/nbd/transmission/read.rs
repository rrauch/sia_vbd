use crate::nbd::block_device::read_reply::{
    Chunk as ReadReplyChunk, Payload as ReadReplyPayload, Queue as ReadReplyQueue,
};
use crate::nbd::block_device::{BlockDevice, RequestContext};
use crate::nbd::transmission::reply::{Write, WriteCommand, Writer};
use crate::nbd::transmission::request::RequestId;
use crate::nbd::{block_device, TransmissionMode};
use crate::ClientEndpoint;
use futures::SinkExt;
use rangemap::RangeMap;
use std::io::{Error, ErrorKind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::{CancellationToken, DropGuard, PollSender};

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
            let (data_tx, mut chunks) = mpsc::channel::<ReadReplyChunk>(2);
            let mut read_q = ReadReplyQueue::new(
                PollSender::new(data_tx).sink_map_err(|_| block_device::Error::ReadQueueError),
            );
            let data_sent = Arc::new(AtomicBool::new(false));

            let sender_task: JoinHandle<Result<(), std::io::Error>> = {
                let data_sent = data_sent.clone();
                let writer = writer.clone();
                task::spawn(async move {
                    let dont_fragment =
                        transmission_mode == TransmissionMode::Simple || dont_fragment;
                    let mut ranges = RangeMap::new();
                    ranges.insert(offset..offset + length, false);
                    let mut chunk_buffer = Vec::new();
                    let mut multi_tx: Option<mpsc::Sender<Write>> = None;
                    let mut done = false;
                    while let Some(chunk) = chunks.recv().await {
                        let next_chunk = if dont_fragment {
                            chunk_buffer.push(chunk);
                            // look for the lowest unsent offset
                            let next_offset = ranges
                                .iter()
                                .find_map(
                                    |(range, sent)| if *sent { None } else { Some(range.start) },
                                )
                                .ok_or(Error::new(
                                    ErrorKind::InvalidData,
                                    "more chunks received while range already fully written",
                                ))?;
                            // find a chunk for the offset
                            chunk_buffer
                                .iter()
                                .position(|c| c.offset == next_offset)
                                .map(|i| chunk_buffer.swap_remove(i))
                        } else {
                            Some(chunk)
                        };
                        if let Some(chunk) = next_chunk {
                            ranges.insert(chunk.offset..chunk.offset + chunk.length, true);
                            done = ranges.len() == 1; // indicates if the full range has been sent yet
                            let command = match chunk.result {
                                Ok(ReadReplyPayload::Writer(writer)) => WriteCommand::Read {
                                    chunk_offset: chunk.offset,
                                    chunk_length: chunk.length,
                                    request_length: length,
                                    dont_fragment,
                                    writer,
                                },
                                Ok(ReadReplyPayload::Zeroes) => WriteCommand::Zeroes {
                                    chunk_offset: chunk.offset,
                                    chunk_length: chunk.length,
                                    dont_fragment,
                                    request_length: length,
                                },
                                Err(err) => {
                                    // an error always ends the read command
                                    done = true;
                                    WriteCommand::Error {
                                        error_type: (&err).into(),
                                        offset: None,
                                        msg: Some(err.to_string()),
                                    }
                                }
                            };
                            let is_first = !data_sent.load(Ordering::SeqCst);
                            let is_only = is_first && done;

                            let write = Write::new(command, req_id, is_first, done);

                            if is_only {
                                writer.send(write).await.map_err(|e| {
                                    Error::new(ErrorKind::BrokenPipe, e.to_string())
                                })?;
                            } else {
                                if multi_tx.is_none() {
                                    // this is the first chunk of a multi-chunk reply
                                    let (tx, rx) = mpsc::channel(2);
                                    writer.send_stream(ReceiverStream::new(rx)).await.map_err(
                                        |e| Error::new(ErrorKind::BrokenPipe, e.to_string()),
                                    )?;
                                    multi_tx = Some(tx);
                                }
                                multi_tx.as_ref().unwrap().send(write).await.map_err(|e| {
                                    Error::new(ErrorKind::BrokenPipe, e.to_string())
                                })?;
                            }
                            data_sent.store(true, Ordering::SeqCst);
                        }
                        if done {
                            break;
                        }
                    }

                    if !done {
                        // handler stopped sending chunks before the request was fulfilled
                        return Err(Error::new(
                            ErrorKind::UnexpectedEof,
                            "handler stopped before sending all chunks",
                        ));
                    }

                    Ok(())
                })
            };

            tokio::select! {
                _ = block_device.read(offset, length, &mut read_q, &ctx) => {
                    // do nothing
                }
                _ = ct.cancelled() => {
                    // stop everything immediately
                    sender_task.abort();
                    return;
                }
            }

            match sender_task.await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    if data_sent.load(Ordering::SeqCst) {
                        // some data has already been sent
                        // need to shut down the connection
                        ct.cancel();
                    }
                    if let Err(_) = writer.io_error(&err, req_id).await {
                        // can't even send the error
                        // shut down connection
                        ct.cancel();
                    }
                }
                Err(_) => {
                    // something is very off
                    // shut down connection
                    ct.cancel();
                }
            }
        });
    }
}
