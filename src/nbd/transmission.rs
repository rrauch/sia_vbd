use crate::nbd::TransmissionMode;
use crate::AsyncReadBytesExt;
use anyhow::anyhow;
use bitflags::bitflags;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::net::SocketAddr;
use thiserror::Error;

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

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[repr(u16)]
/// `ReplyType` is only used when structured responses are active.
enum ReplyType {
    /// Indicates a request was handled successfully
    ///
    /// **MUST** always be used with the `NBD_REPLY_FLAG_DONE` bit set.
    /// Valid as a reply to any request.
    None = 0u16,
    /// Content chunk with `offset` and `length`
    ///
    /// The data **MUST** lie within the bounds of the requested range.
    /// May be used more than once unless `NBD_CMD_FLAG_DF` was set.
    /// Valid for `NBD_CMD_READ` only.
    OffsetData = 1u16,
    /// Empty chunk (all zeroes) with `offset` and `length`
    ///
    /// Contains no actual data as content is all zeroes.
    /// The range **MUST** lie within the bounds of the requested range
    /// and **MUST NOT** overlap with any previously sent chunks within the same reply.
    /// Valid for `NBD_CMD_READ` only.
    OffsetHole = 2u16,
    BlockStatus = 5u16,
    ExtendedBlockStatus = 6u16,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[repr(u16)]
/// `ReplyError` is only used when structured responses are active.
enum ReplyError {
    /// *SHOULD NOT* be sent more than once per reply.
    /// A mandatory [ErrorType] as to be supplied.
    /// Can contain an optional error message. The message length **MUST NOT** exceed 4096 bytes.
    /// *Note*: does not automatically indicate the completion of the reply. The `NBD_REPLY_FLAG_DONE`
    /// flag still has to be set if this completes the reply.
    Error = (1 << 15) | (1),
    /// Error with an additional offset indicator
    ///
    /// Similar to [ReplyError::Error], but with an additional offset.
    /// Valid as a reply to:
    /// `NBD_CMD_READ`, `NBD_CMD_WRITE`, `NBD_CMD_TRIM`, `NBD_CMD_CACHE`, `NBD_CMD_WRITE_ZEROES`,
    /// and `NBD_CMD_BLOCK_STATUS`.
    OffsetError = (1 << 15) | (2),
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

pub(super) struct TransmissionHandler {
    export_name: String,
    transmission_mode: TransmissionMode,
    client_addr: SocketAddr,
}

impl TransmissionHandler {
    pub(super) fn new(
        export_name: String,
        transmission_mode: TransmissionMode,
        client_addr: SocketAddr,
    ) -> Self {
        Self {
            export_name,
            transmission_mode,
            client_addr,
        }
    }

    pub(super) async fn process<RX: AsyncRead + Unpin, TX: AsyncWrite + Unpin>(
        &self,
        mut rx: RX,
        mut tx: TX,
    ) -> Result<(), NbdError> {
        use RequestError::*;

        if self.transmission_mode != TransmissionMode::Simple {
            return Err(anyhow!("only simple mode is currently supported"))?;
        }

        loop {
            // read the header of the next request
            if rx.get_u32().await? != REQUEST_MAGIC {
                // invalid header received from client
                return Err(InvalidRequestHeader)?;
            }
            let flags = CommandFlags::from_bits(rx.get_u16().await?).ok_or(InvalidCommandFlags)?;
            let req_type =
                RequestType::try_from(rx.get_u16().await?).map_err(|_| InvalidRequestType)?;
            let cookie = rx.get_u64().await?;
            let offset = rx.get_u64().await?;
            let length = rx.get_u32().await?;

            match req_type {
                RequestType::Read => {
                    self.read(length, cookie, &mut tx).await?;
                    eprintln!("read {} bytes at offset {}", length, offset);
                }
                RequestType::Write => {
                    let payload = rx.get_exact(length as usize).await?;
                    self.write(payload, flags.contains(CommandFlags::FUA), cookie, &mut tx)
                        .await?;
                    eprintln!("wrote {} bytes at offset {}", length, offset);
                }
                RequestType::Flush => {
                    self.flush(cookie, &mut tx).await?;
                    eprintln!("flush request received");
                }
                RequestType::Disconnect => {
                    eprintln!("client sent disconnect request");
                    break;
                }
                _ => return Err(anyhow!("request type {:?} unimplemented", req_type).into()),
            }
        }

        Ok(())
    }

    // stub
    async fn read(
        &self,
        length: u32,
        cookie: u64,
        tx: &mut (impl AsyncWrite + Unpin),
    ) -> std::io::Result<()> {
        let data = BytesMut::zeroed(length as usize).freeze();
        let mut len = BytesMut::with_capacity(4);
        len.put_u32(data.len() as u32);
        let header = Self::reply_header_simple(0, cookie);
        tx.write_all(header.as_ref()).await?;
        tx.write_all(&data).await?;
        Ok(())
    }

    // stub
    async fn write(
        &self,
        _payload: Bytes,
        _fua: bool,
        cookie: u64,
        tx: &mut (impl AsyncWrite + Unpin),
    ) -> std::io::Result<()> {
        let header = Self::reply_header_simple(0, cookie);
        tx.write_all(header.as_ref()).await?;
        Ok(())
    }

    // stub
    async fn flush(&self, cookie: u64, tx: &mut (impl AsyncWrite + Unpin)) -> std::io::Result<()> {
        let header = Self::reply_header_simple(0, cookie);
        tx.write_all(header.as_ref()).await?;
        Ok(())
    }

    fn reply_header_simple(error: u32, cookie: u64) -> Bytes {
        let mut header = BytesMut::with_capacity(16);
        header.put_u32(SIMPLE_REPLY_MAGIC);
        header.put_u32(error);
        header.put_u64(cookie);
        header.freeze()
    }

    /*async fn _send_reply(
        &self,
        tx: &mut (impl AsyncWrite + Unpin),
        error: u32,
        cookie: u64,
        data: Option<Bytes>,
    ) -> std::io::Result<()> {
        let data_len = data.as_ref().map(|b| b.len()).unwrap_or(0);
        let mut resp = match self.transmission_mode {
            TransmissionMode::Simple => {
                let mut resp = BytesMut::with_capacity(16 + data_len);
                resp.put_u32(SIMPLE_REPLY_MAGIC);
                resp
            },
            TransmissionMode::Structured => {
                let mut resp = BytesMut::with_capacity(20 + data_len);
                resp.put_u32(STRUCTURED_REPLY_MAGIC);
                resp
            }
        };
        resp.put_u32(option_type);
        resp.put_u32(resp_type);
        resp.put_u32(data_len as u32);
        if let Some(data) = data {
            resp.put(data);
        }
        let resp = resp.freeze();
        tx.write_all(resp.as_ref()).await?;
        tx.flush().await?;
        Ok(())
    }*/
}

#[derive(Error, Debug)]
pub(super) enum NbdError {
    /// A request related error occurred when reading data from the client
    #[error("client request error")]
    RequestError(#[from] RequestError),
    /// An `IO` error occurred reading from or writing to the client
    #[error("client io error")]
    IoError(#[from] std::io::Error),
    /// Other error, with optional details
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub(super) enum RequestError {
    /// The client sent an invalid request header
    #[error("invalid request header")]
    InvalidRequestHeader,
    /// The client sent invalid command flags
    #[error("invalid command flags")]
    InvalidCommandFlags,
    /// The client sent an
    #[error("invalid command flags")]
    InvalidRequestType,
}
