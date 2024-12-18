use crate::nbd::block_device::read_reply::PayloadWriter;
use crate::nbd::block_device::Error as BlockDeviceError;
use crate::nbd::transmission::request::RequestId;
use crate::nbd::TransmissionMode;
use crate::AsyncWriteBytesExt;
use bitflags::bitflags;
use bytes::BufMut;
use compact_bytes::CompactBytes;
use derivative::Derivative;
use futures::StreamExt;
use futures::{AsyncWrite, AsyncWriteExt, Stream};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::io::{Error as IoError, ErrorKind};
use std::sync::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const SIMPLE_REPLY_MAGIC: u32 = 0x67446698;
const STRUCTURED_REPLY_MAGIC: u32 = 0x668e33ef;
const EXTENDED_REPLY_MAGIC: u32 = 0x6e8a278c;

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

pub(super) struct Writer {
    task_handle: Mutex<Option<JoinHandle<()>>>,
    sender: mpsc::Sender<WriteEntry>,
    ct: CancellationToken,
}

#[derive(Debug)]
pub(super) struct Write {
    command: WriteCommand,
    is_first: bool,
    done: bool,
    request_id: RequestId,
}

impl Write {
    pub fn new(command: WriteCommand, request_id: RequestId, is_first: bool, done: bool) -> Self {
        Self {
            command,
            is_first,
            done,
            request_id,
        }
    }
    /// This is a single chunk reply,
    /// no other chunks have been sent before and no other chunks
    /// will be sent later for this particular reply.
    fn is_single_chunk_reply(&self) -> bool {
        self.is_first && self.done
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(super) enum WriteCommand {
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
        writer: Box<dyn PayloadWriter>,
    },
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[repr(u32)]
/// `ErrorType` used with [ReplyError].
pub(super) enum ErrorType {
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

impl From<&BlockDeviceError> for ErrorType {
    fn from(value: &BlockDeviceError) -> Self {
        match value {
            BlockDeviceError::IoError(io_error) => io_error.into(),
            BlockDeviceError::ReadQueueError => ErrorType::IoError,
        }
    }
}

impl From<&IoError> for ErrorType {
    fn from(err: &IoError) -> Self {
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

enum WriteEntry {
    Single(Write),
    Multi(Box<dyn Stream<Item = Write> + Send + Sync + Unpin>),
}

impl Writer {
    pub fn new<TX: AsyncWrite + Unpin + Send + 'static>(
        tx: TX,
        transmission_mode: TransmissionMode,
    ) -> Self {
        let ct = CancellationToken::new();
        let (sender, rx) = mpsc::channel(32);
        let task_handle = {
            let ct = ct.clone();
            tokio::spawn(async move {
                let _ = write_loop(tx, transmission_mode, rx, ct).await;
            })
        };
        Self {
            task_handle: Mutex::new(Some(task_handle)),
            sender,
            ct,
        }
    }

    pub async fn shutdown(&self) {
        self.ct.cancel();
        let task_handle = {
            let mut lock = self.task_handle.lock().unwrap();
            lock.take()
        };

        if let Some(task_handle) = task_handle {
            let _ = task_handle.await;
        }
    }

    pub async fn send(&self, entry: Write) -> anyhow::Result<()> {
        self.sender.send(WriteEntry::Single(entry)).await?;
        Ok(())
    }

    pub async fn send_stream<T: Stream<Item = Write> + Send + Sync + Unpin + 'static>(
        &self,
        stream: T,
    ) -> anyhow::Result<()> {
        self.sender
            .send(WriteEntry::Multi(Box::new(stream)))
            .await?;
        Ok(())
    }

    pub async fn done(&self, req_id: RequestId) -> anyhow::Result<()> {
        self.sender
            .send(WriteEntry::Single(Write {
                command: WriteCommand::Done,
                done: true,
                request_id: req_id,
                is_first: true,
            }))
            .await?;
        Ok(())
    }

    pub async fn io_error<E: Into<ErrorType> + ToString>(
        &self,
        err: E,
        req_id: RequestId,
    ) -> anyhow::Result<()> {
        let msg = err.to_string();
        let error_type = err.into();
        self.sender
            .send(WriteEntry::Single(Write {
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

    pub async fn unsupported(&self, msg: String, req_id: RequestId) -> anyhow::Result<()> {
        self.sender
            .send(WriteEntry::Single(Write {
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

async fn write_loop<TX: AsyncWrite + Unpin + Send + 'static>(
    mut tx: TX,
    transmission_mode: TransmissionMode,
    mut rx: mpsc::Receiver<WriteEntry>,
    ct: CancellationToken,
) -> anyhow::Result<TX> {
    let mut shutdown = false;
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
                                while let Some(w) = rx.next().await {
                                    write(&mut tx, transmission_mode, w).await?;
                                }
                            }
                        }
                        tx.flush().await?;
                    },
                    None => {
                        // sender closed
                        break
                    },
                }
            },
            _ = ct.cancelled(), if !shutdown => {
                // clean shutdown
                rx.close();
                shutdown = true;
            }
        }
    }
    let _ = tx.flush().await;
    Ok(tx)
}

async fn write<TX: AsyncWrite + Unpin + Send + 'static>(
    mut tx: &mut TX,
    transmission_mode: TransmissionMode,
    write: Write,
) -> std::io::Result<()> {
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
                return Err(IoError::new(
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
                        true, // this is a defragmented, single reply, so done need to be true
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
    Ok(())
}

impl Drop for Writer {
    fn drop(&mut self) {
        if let Ok(mut lock) = self.task_handle.lock() {
            if let Some(task_handle) = lock.take() {
                task_handle.abort();
            }
        }
    }
}
