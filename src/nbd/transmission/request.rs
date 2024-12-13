use crate::nbd::transmission::Command;
use crate::nbd::{TransmissionMode, MAX_PAYLOAD_LEN};
use bitflags::bitflags;
use bytes::{Buf, BytesMut};
use futures::{AsyncRead, AsyncReadExt};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use thiserror::Error;

const REQUEST_MAGIC: u32 = 0x25609513;
const EXTENDED_REQUEST_MAGIC: u32 = 0x21e41c71;

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
pub(super) struct RequestId {
    pub(super) cookie: u64,
    pub(super) offset: u64,
}

pub(super) struct Request {
    pub(super) id: RequestId,
    pub(super) command: Command,
    pub(super) payload_length: u32,
}

pub(super) struct Reader {
    buf: BytesMut,
    transmission_mode: TransmissionMode,
}

impl Reader {
    pub fn new(transmission_mode: TransmissionMode) -> Self {
        Self {
            buf: BytesMut::zeroed(32),
            transmission_mode,
        }
    }

    pub async fn read_next<RX: AsyncRead + Unpin>(
        &mut self,
        rx: &mut RX,
    ) -> Result<Request, ReadError> {
        // read the next request header
        // the length depends on the transmission mode
        let header_len = if self.transmission_mode == TransmissionMode::Extended {
            // extended mode allows only extended requests
            32
        } else {
            // request has to be a regular request
            28
        };
        let buf = self.buf.as_mut();
        let buf = {
            rx.read_exact(&mut buf[..header_len]).await?;
            &buf[..header_len]
        };

        Ok(deserialize(buf, self.transmission_mode)?)
    }
}

fn deserialize(
    mut buf: impl Buf,
    transmission_mode: TransmissionMode,
) -> Result<Request, RequestError> {
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

    if req_type == RequestType::Write && payload_length == 0 {
        return Err(InvalidInput)?;
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

#[derive(Error, Debug)]
pub(in crate::nbd) enum ReadError {
    /// A request related error occurred when reading data from the client
    #[error("client request error")]
    RequestError(#[from] RequestError),
    /// An `IO` error occurred while reading from the client
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

#[derive(Error, Debug)]
pub(in crate::nbd) enum RequestError {
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
