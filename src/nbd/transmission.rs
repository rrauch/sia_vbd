use crate::nbd::handler::{Data, DataWriter, ReadChunk, RequestContext};
use crate::nbd::transmission::NbdError::Termination;
use crate::nbd::{Export, TransmissionMode, MAX_PAYLOAD_LEN};
use crate::{AsyncReadBytesExt, AsyncWriteBytesExt, ClientEndpoint, LimitedReader};
use anyhow::anyhow;
use bitflags::bitflags;
use bytes::{Buf, BufMut};
use compact_bytes::CompactBytes;
use derivative::Derivative;
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, SinkExt};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use rangemap::RangeMap;
use std::fmt::Debug;
use std::io::{Error, ErrorKind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::task;
use tokio::task::JoinHandle;
use tokio_util::sync::{CancellationToken, PollSender};

const REQUEST_MAGIC: u32 = 0x25609513;
const EXTENDED_REQUEST_MAGIC: u32 = 0x21e41c71;

const SIMPLE_REPLY_MAGIC: u32 = 0x67446698;
const STRUCTURED_REPLY_MAGIC: u32 = 0x668e33ef;
const EXTENDED_REPLY_MAGIC: u32 = 0x6e8a278c;

bitflags! {
    /// Command flags, received from client
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct CommandFlags: u16 {
        /// `Force Unit Access`
        ///
        /// Valid for all write commands
        const FUA = 1 << 0;
        /// Don't create holes
        ///
        /// Valid during `NBD_CMD_WRITE_ZEROES` only
        const NO_HOLE = 1 << 1;
        /// Don't fragment
        ///
        /// Valid during `NBD_CMD_READ` only
        const DF = 1 << 2;
        /// Only one extent per metadata context
        ///
        /// Valid during `NBD_CMD_BLOCK_STATUS` only
        const REQ_ONE = 1 << 3;
        /// Fail if not fast
        ///
        /// Valid during `NBD_CMD_WRITE_ZEROES` only
        const FAST_ZERO = 1 << 4;
        /// Payload length
        ///
        /// Experimental; only with `EXTENDED_HEADERS`
        const PAYLOAD_LEN = 1 << 5;
    }
}

bitflags! {
    /// Structured Reply flags, sent by the server
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct StructuredReplyFlags: u16 {
        /// Reply is complete
        ///
        /// Only set this on the last chunk of a structured request response.
        /// Indicates no more chunks will follow.
        /// Indicates the successful completion of the request
        /// if no previous errors where sent during the reply.
        const DONE = 1 << 0;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Reply {
    /// Indicates a request was handled successfully
    ///
    /// **MUST** always be used with the `NBD_REPLY_FLAG_DONE` bit set.
    /// Valid as a reply to any request.
    None,
    /// Content chunk with `offset` and `length`
    ///
    /// The data **MUST** lie within the bounds of the requested range.
    /// May be used more than once unless `NBD_CMD_FLAG_DF` was set.
    /// Valid for `NBD_CMD_READ` only.
    OffsetData(u64, u64),
    /// Empty chunk (all zeroes) with `offset` and `length`
    ///
    /// Contains no actual data as content is all zeroes.
    /// The range **MUST** lie within the bounds of the requested range
    /// and **MUST NOT** overlap with any previously sent chunks within the same reply.
    /// Valid for `NBD_CMD_READ` only.
    OffsetHole(u64, u32),
    BlockStatus,
    ExtendedBlockStatus,
    /// *SHOULD NOT* be sent more than once per reply.
    /// A mandatory [ErrorType] as to be supplied.
    /// Can contain an optional error message. The message length **MUST NOT** exceed 4096 bytes.
    /// *Note*: does not automatically indicate the completion of the reply. The `NBD_REPLY_FLAG_DONE`
    /// flag still has to be set if this completes the reply.
    Error(ErrorType, Option<String>),
    /// Error with an additional offset indicator
    ///
    /// Similar to [ReplyError::Error], but with an additional offset.
    /// Valid as a reply to:
    /// `NBD_CMD_READ`, `NBD_CMD_WRITE`, `NBD_CMD_TRIM`, `NBD_CMD_CACHE`, `NBD_CMD_WRITE_ZEROES`,
    /// and `NBD_CMD_BLOCK_STATUS`.
    OffsetError(ErrorType, u64, Option<String>),
}

impl Reply {
    async fn serialize(
        self,
        tx: &mut (impl AsyncWrite + Unpin),
        transmission_mode: TransmissionMode,
        cookie: u64,
        offset: u64,
        done: bool,
    ) -> std::io::Result<()> {
        use Reply::*;

        let mut buf = [0u8; 32];
        let (magic, header_len) = match transmission_mode {
            TransmissionMode::Simple => (SIMPLE_REPLY_MAGIC, 16),
            TransmissionMode::Structured => (STRUCTURED_REPLY_MAGIC, 20),
            TransmissionMode::Extended => (EXTENDED_REPLY_MAGIC, 32),
        };
        let mut header = &mut buf[..header_len];
        header.put_u32(magic);

        if transmission_mode == TransmissionMode::Simple {
            // Simple responses are very limited
            let error = match self {
                Error(error_type, _) | OffsetError(error_type, _, _) => error_type.into(),
                _ => 0, // not an error
            };
            header.put_u32(error);
            header.put_u64(cookie);
            tx.write_all(&buf[..header_len]).await?;
            return Ok(());
        }

        let mut flags = StructuredReplyFlags::empty();
        if done {
            // last reply for this request
            flags |= StructuredReplyFlags::DONE;
        }
        header.put_u16(flags.bits());

        let rep_type: u16 = match &self {
            None => 0,
            OffsetData(_, _) => 1,
            OffsetHole(_, _) => 2,
            BlockStatus => 5,
            ExtendedBlockStatus => 6,
            Error(_, _) => (1 << 15) | (1),
            OffsetError(_, _, _) => (1 << 15) | (2),
        };
        header.put_u16(rep_type);
        header.put_u64(cookie);

        let mut payload = CompactBytes::default();

        let additional_length = match self {
            None => 0,
            OffsetData(offset, length) => {
                payload.extend_from_slice(offset.to_be_bytes().as_slice());
                length
            }
            OffsetHole(offset, length) => {
                payload.extend_from_slice(offset.to_be_bytes().as_slice());
                payload.extend_from_slice(length.to_be_bytes().as_slice());
                0
            }
            BlockStatus => unimplemented!("NBD_REPLY_TYPE_BLOCK_STATUS is unimplemented"),
            ExtendedBlockStatus => {
                unimplemented!("NBD_REPLY_TYPE_BLOCK_STATUS_EXT is unimplemented")
            }
            Error(error_type, msg) => {
                payload.extend_from_slice(Into::<u32>::into(error_type).to_be_bytes().as_slice());
                let msg = msg.map(|s| s.into_bytes()).unwrap_or_default();
                payload.extend_from_slice((msg.len() as u16).to_be_bytes().as_slice());
                if msg.len() > 0 {
                    payload.extend_from_slice(msg.as_slice());
                }
                0
            }
            OffsetError(error_type, offset, msg) => {
                payload.extend_from_slice(Into::<u32>::into(error_type).to_be_bytes().as_slice());
                let msg = msg.map(|s| s.into_bytes()).unwrap_or_default();
                payload.extend_from_slice((msg.len() as u16).to_be_bytes().as_slice());
                payload.extend_from_slice(offset.to_be_bytes().as_slice());
                if msg.len() > 0 {
                    payload.extend_from_slice(msg.as_slice());
                }
                0
            }
        };
        let length = payload.len() as u64 + additional_length;

        match transmission_mode {
            TransmissionMode::Structured => {
                header.put_u32(length as u32);
            }
            TransmissionMode::Extended => {
                header.put_u64(offset);
                header.put_u64(length);
            }
            TransmissionMode::Simple => {
                unreachable!()
            }
        }
        tx.write_all(&buf[..header_len]).await?;

        if payload.len() > 0 {
            tx.write_all(payload.as_slice()).await?;
        }

        Ok(())
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[repr(u32)]
/// `ErrorType` used with [ReplyError].
enum ErrorType {
    NotPermitted = 1u32,
    IoError = 5u32,
    NoMemory = 12u32,
    InvalidArgument = 22u32,
    NoSpaceLeft = 28u32,
    Overflow = 75u32,
    NotSupported = 95u32,
    /// Server is shutting down
    Shutdown = 108u32,
}

impl From<&std::io::Error> for ErrorType {
    fn from(err: &Error) -> Self {
        use std::io::ErrorKind;
        use ErrorType::*;

        match err.kind() {
            ErrorKind::PermissionDenied => NotPermitted,
            ErrorKind::OutOfMemory => NoMemory,
            ErrorKind::InvalidInput => InvalidArgument,
            ErrorKind::Unsupported => NotSupported,
            _ => IoError,
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[repr(u16)]
/// Client request types.
///
/// Replies **MUST** be in `Structured Reply` form if negotiated during `Handshake`.
enum RequestType {
    /// Reads `length` bytes at specified `offset`
    Read = 0u16,
    /// Writes to specified `offset`
    ///
    /// The server *MAY* reply before fully commiting
    /// the data **UNLESS** `NBD_CMD_FLAG_FUA` is set.
    Write = 1u16,
    /// The server **MUST** first handle all outstanding requests before
    /// orderly shutting down the connection. Any further requests *SHOULD* be ignored.
    Disconnect = 2u16,
    /// The server **MUST** fully commit any and all outstanding writes before replying.
    Flush = 3u16,
    /// A hint that `length` bytes at `offset` are not required any more.
    ///
    /// The server *MAY* discard the data, however the client **MUST NOT**
    /// expect this to be the case.
    Trim = 4u16,
    /// A hint that the client plans to access `length` bytes at `offset` soon.
    ///
    /// The server *MAY* use this information to make preparations ahead of time.
    Cache = 5u16,
    /// A request to zero out `length` bytes at `offset`.
    ///
    /// The server **MUST** zero out the data as requested.
    /// The server *MAY* reply before fully commiting
    /// the changes **UNLESS** `NBD_CMD_FLAG_FUA` is set.
    WriteZeroes = 6u16,
    BlockStatus = 7u16,
    Resize = 8u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct RequestId {
    cookie: u64,
    offset: u64,
}

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

struct Request {
    id: RequestId,
    command: Command,
    payload_length: u32,
}

impl Request {
    fn deserialize(
        mut buf: impl Buf,
        transmission_mode: TransmissionMode,
    ) -> Result<Self, RequestError> {
        use RequestError::*;
        let magic = buf.get_u32();
        match magic {
            REQUEST_MAGIC => {
                if transmission_mode == TransmissionMode::Extended {
                    return Err(ExtendedRequestHeaderRequired);
                }
                // all good
            }
            EXTENDED_REQUEST_MAGIC => {
                if transmission_mode != TransmissionMode::Extended {
                    return Err(ExtendedRequestHeaderNotAllowed);
                }
                // all good
            }
            _ => {
                // invalid header received from client
                return Err(InvalidRequestHeader);
            }
        }

        let flags = CommandFlags::from_bits(buf.get_u16()).ok_or(InvalidCommandFlags)?;
        let req_type = RequestType::try_from(buf.get_u16()).map_err(|_| UnknownRequestType)?;
        let cookie = buf.get_u64();
        let offset = buf.get_u64();
        let id = RequestId { cookie, offset };

        let length;
        if transmission_mode == TransmissionMode::Extended {
            // extended requests have a 64-bit length field
            length = buf.get_u64();
        } else {
            // compact requests have a 32-bit length field
            length = buf.get_u32() as u64;
        }

        let payload_length = match transmission_mode {
            TransmissionMode::Simple | TransmissionMode::Structured => {
                match req_type {
                    RequestType::Write => length as u32,
                    _ => {
                        // only `Write` commands can have a payload in non-extended mode
                        0
                    }
                }
            }
            TransmissionMode::Extended => {
                // In extended mode the `NBD_CMD_FLAG_PAYLOAD_LEN` indicates whether the header
                // length is a payload length or an effect length
                if flags.contains(CommandFlags::PAYLOAD_LEN) {
                    length as u32
                } else {
                    0
                }
            }
        };

        if payload_length > MAX_PAYLOAD_LEN {
            return Err(Overflow)?;
        }

        let command = match req_type {
            RequestType::Read => {
                let dont_fragment = if transmission_mode == TransmissionMode::Simple {
                    // simple mode does not support reply fragmentation
                    true
                } else {
                    flags.contains(CommandFlags::DF)
                };
                Command::Read {
                    offset,
                    length,
                    dont_fragment,
                }
            }
            RequestType::Write => Command::Write {
                offset,
                length,
                fua: flags.contains(CommandFlags::FUA),
            },
            RequestType::Disconnect => Command::Disconnect,
            RequestType::Flush => Command::Flush,
            RequestType::Trim => Command::Trim { offset, length },
            RequestType::Cache => Command::Cache { offset, length },
            RequestType::WriteZeroes => Command::WriteZeroes {
                offset,
                length,
                fast_only: flags.contains(CommandFlags::FAST_ZERO),
                no_hole: flags.contains(CommandFlags::NO_HOLE),
            },
            RequestType::BlockStatus => Command::BlockStatus,
            RequestType::Resize => Command::Resize { length },
        };

        Ok(Request {
            id,
            command,
            payload_length,
        })
    }
}

enum WriteEntry {
    Single(Write),
    Multi(mpsc::Receiver<Write>),
}

#[derive(Debug)]
struct Write {
    command: WriteCommand,
    is_first: bool,
    done: bool,
    request_id: RequestId,
}

impl Write {
    /// This is a single chunk reply,
    /// no other chunks have been sent before and no other chunks
    /// will be sent later for this particular reply.
    fn is_single_chunk_reply(&self) -> bool {
        self.is_first && self.done
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
enum WriteCommand {
    Done,
    Error {
        error_type: ErrorType,
        offset: Option<u64>,
        msg: Option<String>,
    },
    Zeroes {
        chunk_offset: u64,
        chunk_length: u64,
        request_length: u64,
        dont_fragment: bool,
    },
    Read {
        chunk_offset: u64,
        chunk_length: u64,
        request_length: u64,
        dont_fragment: bool,
        #[derivative(Debug = "ignore")]
        writer: Box<dyn DataWriter>,
    },
}

trait SenderExt {
    async fn done(&self, req_id: RequestId) -> anyhow::Result<()>;
    async fn io_error(&self, err: std::io::Error, req_id: RequestId) -> anyhow::Result<()>;
    async fn unsupported(&self, msg: String, req_id: RequestId) -> anyhow::Result<()>;
}

impl SenderExt for mpsc::Sender<WriteEntry> {
    async fn done(&self, req_id: RequestId) -> anyhow::Result<()> {
        self.send(WriteEntry::Single(Write {
            command: WriteCommand::Done,
            done: true,
            request_id: req_id,
            is_first: true,
        }))
        .await?;
        Ok(())
    }

    async fn io_error(&self, err: Error, req_id: RequestId) -> anyhow::Result<()> {
        let msg = err.to_string();
        let error_type = (&err).into();
        self.send(WriteEntry::Single(Write {
            command: WriteCommand::Error {
                offset: None,
                msg: Some(msg),
                error_type,
            },
            done: true,
            request_id: req_id,
            is_first: true,
        }))
        .await?;
        Ok(())
    }

    async fn unsupported(&self, msg: String, req_id: RequestId) -> anyhow::Result<()> {
        self.send(WriteEntry::Single(Write {
            command: WriteCommand::Error {
                offset: None,
                msg: Some(msg),
                error_type: ErrorType::NotSupported,
            },
            done: true,
            request_id: req_id,
            is_first: true,
        }))
        .await?;
        Ok(())
    }
}

pub(super) struct TransmissionHandler {
    export: Export,
    transmission_mode: TransmissionMode,
    client_endpoint: ClientEndpoint,
}

impl TransmissionHandler {
    pub(super) fn new(
        export: Export,
        transmission_mode: TransmissionMode,
        client_endpoint: ClientEndpoint,
    ) -> Self {
        Self {
            export,
            transmission_mode,
            client_endpoint,
        }
    }

    pub(super) async fn process<
        RX: AsyncRead + Unpin + Send + 'static,
        TX: AsyncWrite + Unpin + Send + 'static,
    >(
        &self,
        mut rx: RX,
        tx: TX,
    ) -> Result<(), NbdError> {
        use RequestError::*;

        let ct = CancellationToken::new();
        let _drop_guard = ct.clone().drop_guard();

        let transmission_mode = self.transmission_mode;
        let (writer, receiver) = mpsc::channel(10);

        // start the writer
        let write_handle = {
            let ct = ct.clone();
            tokio::spawn(async move { write_loop(tx, transmission_mode, receiver, ct).await })
        };

        let mut buf = [0u8; 32];
        loop {
            // read the next request header
            // the length depends on the transmission mode
            let header_len = if self.transmission_mode == TransmissionMode::Extended {
                // extended mode allows only extended requests
                buf.len()
            } else {
                // request has to be a regular request
                28
            };

            let buf = tokio::select! {
                res = rx.read_exact(&mut buf[..header_len]) => {
                    res?;
                    &buf[..header_len]
                }
                _ = ct.cancelled() => {
                    // close connection immediately
                    return Err(Termination);
                }
            };

            let req = Request::deserialize(buf, self.transmission_mode)?;
            let ctx = RequestContext::new(req.id.cookie, self.client_endpoint.clone());
            let mut payload_remaining = req.payload_length as usize;

            let handler = &self.export.handler;
            let read_only = self.export.read_only();
            let info = self.export.options();

            match req.command {
                Command::Read {
                    offset,
                    length,
                    dont_fragment,
                } => {
                    let handler = handler.clone();
                    let req_id = req.id;
                    let writer = writer.clone();
                    let transmission_mode = self.transmission_mode;
                    let ct = ct.clone();

                    tokio::spawn(async move {
                        let (data_tx, mut data_rx) = mpsc::channel::<ReadChunk>(2);
                        let mut data_tx = PollSender::new(data_tx).sink_map_err(|_| ());
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
                                while let Some(chunk) = data_rx.recv().await {
                                    let next_chunk = if dont_fragment {
                                        chunk_buffer.push(chunk);
                                        // look for the lowest unsent offset
                                        let next_offset = ranges.iter().find_map(|(range, sent)| if *sent {
                                            None
                                        } else {
                                            Some(range.start)
                                        }).ok_or(Error::new(ErrorKind::InvalidData, "more chunks received while range already fully written"))?;
                                        // find a chunk for the offset
                                        chunk_buffer
                                            .iter()
                                            .position(|c| c.offset == next_offset)
                                            .map(|i| chunk_buffer.swap_remove(i))
                                    } else {
                                        Some(chunk)
                                    };
                                    if let Some(chunk) = next_chunk {
                                        ranges.insert(
                                            chunk.offset..chunk.offset + chunk.length,
                                            true,
                                        );
                                        done = ranges.len() == 1; // indicates if the full range has been sent yet
                                        let command = match chunk.result {
                                            Ok(Data::Content(writer)) => WriteCommand::Read {
                                                chunk_offset: chunk.offset,
                                                chunk_length: chunk.length,
                                                request_length: length,
                                                dont_fragment,
                                                writer,
                                            },
                                            Ok(Data::Zeroes) => WriteCommand::Zeroes {
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

                                        let write = Write {
                                            request_id: req_id,
                                            is_first,
                                            done,
                                            command,
                                        };

                                        if is_only {
                                            writer.send(WriteEntry::Single(write)).await.map_err(
                                                |e| {
                                                    Error::new(ErrorKind::BrokenPipe, e.to_string())
                                                },
                                            )?;
                                        } else {
                                            if multi_tx.is_none() {
                                                // this is the first chunk of a multi-chunk reply
                                                let (tx, rx) = mpsc::channel(2);
                                                writer.send(WriteEntry::Multi(rx)).await.map_err(
                                                    |e| {
                                                        Error::new(
                                                            ErrorKind::BrokenPipe,
                                                            e.to_string(),
                                                        )
                                                    },
                                                )?;
                                                multi_tx = Some(tx);
                                            }
                                            multi_tx.as_ref().unwrap().send(write).await.map_err(
                                                |e| {
                                                    Error::new(ErrorKind::BrokenPipe, e.to_string())
                                                },
                                            )?;
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
                            _ = handler.read(offset, length, &mut data_tx, &ctx) => {
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
                                if let Err(_) = writer.io_error(err, req_id).await {
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
                Command::Write {
                    offset,
                    length,
                    fua,
                } if !read_only => {
                    if req.payload_length == 0 {
                        return Err(InvalidInput)?;
                    }
                    let (reader_tx, reader_rx) = oneshot::channel();
                    let mut payload =
                        LimitedReader::new(rx, req.payload_length as usize, reader_tx);
                    let handler = handler.clone();
                    let req_id = req.id;
                    let writer = writer.clone();

                    tokio::spawn(async move {
                        match handler.write(offset, length, fua, &mut payload, &ctx).await {
                            Ok(()) => {
                                let _ = writer.done(req_id).await;
                            }
                            Err(err) => {
                                let _ = writer.io_error(err, req_id).await;
                            }
                        }
                    });

                    let remaining;
                    (rx, remaining) = reader_rx
                        .await
                        .map_err(|_| anyhow!("input reader was not returned"))?;
                    payload_remaining = remaining;
                }
                Command::WriteZeroes {
                    offset,
                    length,
                    fast_only,
                    no_hole,
                } if !read_only && ((fast_only && info.fast_zeroes) || (!fast_only)) => {
                    let handler = handler.clone();
                    let req_id = req.id;
                    let writer = writer.clone();
                    tokio::spawn(async move {
                        match handler.write_zeroes(offset, length, no_hole, &ctx).await {
                            Ok(()) => {
                                let _ = writer.done(req_id).await;
                            }
                            Err(err) => {
                                let _ = writer.io_error(err, req_id).await;
                            }
                        }
                    });
                }
                Command::Flush if !read_only => {
                    let handler = handler.clone();
                    let req_id = req.id;
                    let writer = writer.clone();
                    tokio::spawn(async move {
                        match handler.flush(&ctx).await {
                            Ok(()) => {
                                let _ = writer.done(req_id).await;
                            }
                            Err(err) => {
                                let _ = writer.io_error(err, req_id).await;
                            }
                        }
                    });
                }
                Command::Resize { length } if info.resizable && !read_only => {
                    let handler = handler.clone();
                    let req_id = req.id;
                    let writer = writer.clone();
                    let export = self.export.clone();
                    tokio::spawn(async move {
                        match handler.resize(length, &ctx).await {
                            Ok(()) => {
                                // update the exports info
                                export.update_options(handler.options());
                                let _ = writer.done(req_id).await;
                            }
                            Err(err) => {
                                let _ = writer.io_error(err, req_id).await;
                            }
                        }
                    });
                }
                Command::Trim { offset, length } if info.trim && !read_only => {
                    let handler = handler.clone();
                    let req_id = req.id;
                    let writer = writer.clone();
                    tokio::spawn(async move {
                        match handler.trim(offset, length, &ctx).await {
                            Ok(()) => {
                                let _ = writer.done(req_id).await;
                            }
                            Err(err) => {
                                let _ = writer.io_error(err, req_id).await;
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

        // close the sender & wait for the writer to finish
        drop(writer);
        if let Ok(res) = write_handle.await {
            res?;
        }

        Ok(())
    }
}

async fn write_loop<TX: AsyncWrite + Unpin + Send + 'static>(
    mut tx: TX,
    transmission_mode: TransmissionMode,
    mut rx: mpsc::Receiver<WriteEntry>,
    ct: CancellationToken,
) -> anyhow::Result<TX> {
    let _drop_guard = ct.clone().drop_guard();
    loop {
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(entry) => {
                        match entry {
                            WriteEntry::Single(w) => {
                                write(&mut tx, transmission_mode, w).await?;
                            },
                            WriteEntry::Multi(mut rx) => {
                                while let Some(w) = rx.recv().await {
                                    write(&mut tx, transmission_mode, w).await?;
                                }
                            }
                        }
                    },
                    None => break,
                }
            },
            _ = ct.cancelled() => {
                break;
            }
        }
    }
    Ok(tx)
}

async fn write<TX: AsyncWrite + Unpin + Send + 'static>(
    mut tx: &mut TX,
    transmission_mode: TransmissionMode,
    write: Write,
) -> std::io::Result<()> {
    eprintln!(
        "writing {:?} to output, transmission_mode: {:?}",
        write, transmission_mode
    );
    match write.command {
        WriteCommand::Done => {
            if transmission_mode == TransmissionMode::Simple && !write.is_first {
                // ignore for simple mode if not the first chunk
            } else {
                send_done(transmission_mode, write.request_id, &mut tx).await?;
            }
        }
        WriteCommand::Error {
            error_type,
            offset,
            msg,
        } => {
            if transmission_mode == TransmissionMode::Simple && !write.is_first {
                // the spec says to disconnect immediately in this case
                return Err(Error::new(
                    ErrorKind::Other,
                    "error received after data has been written in simple transfer mode",
                ));
            } else {
                send_error(
                    transmission_mode,
                    error_type,
                    offset,
                    msg,
                    write.request_id,
                    &mut tx,
                )
                .await?;
            }
        }
        WriteCommand::Zeroes {
            chunk_offset,
            chunk_length,
            request_length,
            dont_fragment,
        } => {
            if transmission_mode != TransmissionMode::Simple
                && (!dont_fragment || write.is_single_chunk_reply())
            {
                // Structured replies are allowed, this is ideal
                Reply::OffsetHole(chunk_offset, chunk_length as u32)
                    .serialize(
                        &mut tx,
                        transmission_mode,
                        write.request_id.cookie,
                        write.request_id.offset,
                        write.done,
                    )
                    .await?;
                return Ok(());
            }

            // If this is the first chunk for this reply, send the header
            if write.is_first {
                Reply::OffsetData(write.request_id.offset, request_length)
                    .serialize(
                        &mut tx,
                        transmission_mode,
                        write.request_id.cookie,
                        write.request_id.offset,
                        write.done,
                    )
                    .await?;
            }

            // Write the payload
            tx.write_zeroes(chunk_length as usize).await?;
        }
        WriteCommand::Read {
            chunk_offset,
            chunk_length,
            request_length,
            dont_fragment,
            writer,
        } => {
            let (write_offset, write_length) =
                if transmission_mode == TransmissionMode::Simple || dont_fragment {
                    (write.request_id.offset, request_length)
                } else {
                    (chunk_offset, chunk_length)
                };

            if (transmission_mode == TransmissionMode::Simple || dont_fragment) && !write.is_first {
                // dont write a header in this case
            } else {
                Reply::OffsetData(write_offset, write_length)
                    .serialize(
                        &mut tx,
                        transmission_mode,
                        write.request_id.cookie,
                        write.request_id.offset,
                        write.done,
                    )
                    .await?;
            }
            writer.write(&mut tx).await?
        }
    }
    Ok(())
}

async fn send_done(
    transmission_mode: TransmissionMode,
    req_id: RequestId,
    tx: &mut (impl AsyncWrite + Unpin),
) -> std::io::Result<()> {
    Reply::None
        .serialize(tx, transmission_mode, req_id.cookie, req_id.offset, true)
        .await?;
    tx.flush().await?;
    Ok(())
}

async fn send_error(
    transmission_mode: TransmissionMode,
    error: ErrorType,
    offset: Option<u64>,
    msg: Option<String>,
    req_id: RequestId,
    tx: &mut (impl AsyncWrite + Unpin),
) -> std::io::Result<()> {
    let reply = match offset {
        Some(offset) => {
            // error at specific offset
            Reply::OffsetError(error, offset, msg)
        }
        None => {
            // general error
            Reply::Error(error, msg)
        }
    };
    reply
        .serialize(tx, transmission_mode, req_id.cookie, req_id.offset, true)
        .await?;
    tx.flush().await?;
    Ok(())
}

#[derive(Error, Debug)]
pub(super) enum NbdError {
    /// A request related error occurred when reading data from the client
    #[error("client request error")]
    RequestError(#[from] RequestError),
    /// An `IO` error occurred reading from or writing to the client
    #[error("client io error")]
    IoError(#[from] std::io::Error),
    /// The connection had to be terminated
    #[error("termination received")]
    Termination,
    /// Other error, with optional details
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub(super) enum RequestError {
    /// The client sent an invalid request header
    #[error("invalid request header")]
    InvalidRequestHeader,
    /// The client did not send an extended request header when required
    #[error("extended request header required")]
    ExtendedRequestHeaderRequired,
    /// The client sent an extended request header when it was not negotiated
    #[error("extended request header not allowed")]
    ExtendedRequestHeaderNotAllowed,
    /// The client sent invalid command flags
    #[error("invalid command flags")]
    InvalidCommandFlags,
    /// The client sent an unknown request type
    #[error("unknown request type")]
    UnknownRequestType,
    /// The client sent a payload that exceeds MAX_PAYLOAD_LEN
    #[error("payload > {} bytes", MAX_PAYLOAD_LEN)]
    Overflow,
    /// The client sent an invalid input value
    #[error("invalid input")]
    InvalidInput,
}
