use super::{Export, TransmissionMode};
use crate::nbd::transmission::TransmissionHandler;
use crate::AsyncReadBytesExt;
use anyhow::anyhow;
use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use once_cell::sync::Lazy;
use std::cmp::PartialEq;
use std::io::ErrorKind;
use std::net::SocketAddr;
use thiserror::Error;

const NBD_MAGIC: &[u8; 8] = b"NBDMAGIC";
const I_HAVE_OPT: &[u8; 8] = b"IHAVEOPT";
const REPLY_MAGIC: u64 = 0x003e889045565a9;

const MAX_DATA_LEN: usize = 8 * 1024;
const MAX_NAME_LEN: usize = 1024;

static INITIAL_SERVER_MESSAGE: Lazy<Bytes> = Lazy::new(|| {
    let mut b = BytesMut::with_capacity(18);
    b.put(&NBD_MAGIC[..]);
    b.put(&I_HAVE_OPT[..]);
    b.put_u16(ServerFlags::FIXED_NEWSTYLE.bits() | ServerFlags::NO_ZEROES.bits());
    b.freeze()
});

static LEGACY_ZEROED_BYTES: Lazy<Bytes> = Lazy::new(|| {
    let mut b = BytesMut::zeroed(124);
    b.freeze()
});

bitflags! {
    /// Initial Handshake Flags, sent by server
    ///
    /// New-style handshake flags control what will happen during handshake phase.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct ServerFlags: u16 {
        /// Fixed newstyle protocol
        const FIXED_NEWSTYLE = 1 << 0;
        /// End handshake without zeroes
        const NO_ZEROES = 1 << 1;
    }
}

bitflags! {
    /// Initial Handshake Flags, received from client
    ///
    /// Need to match the server sent ones to continue handshake
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct ClientFlags: u32 {
        /// Fixed newstyle protocol
        const FIXED_NEWSTYLE = 1 << 0;
        /// End handshake without zeroes
        const NO_ZEROES = 1 << 1;
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[repr(u32)]
enum OptionRequestType {
    ExportName = 1u32,
    Abort = 2u32,
    List = 3u32,
    StartTls = 5u32,
    Info = 6u32,
    Go = 7u32,
    StructuredReply = 8u32,
    ListMetaContext = 9u32,
    SetMetaContext = 10u32,
    ExtendedHeaders = 11u32,
}

#[derive(Debug)]
enum OptionRequest {
    /// `None` indicates a "default" export
    ExportName(Option<String>),
    Abort,
    List,
    StartTls,
    Info(Option<String>, Vec<u16>),
    Go(Option<String>, Vec<u16>),
    StructuredReply,
    ListMetaContext,
    SetMetaContext,
    ExtendedHeaders,
}

impl TryFrom<(&OptionRequestType, Option<Bytes>)> for OptionRequest {
    type Error = OptionError;
    fn try_from(value: (&OptionRequestType, Option<Bytes>)) -> Result<Self, Self::Error> {
        use DataError::*;
        use OptionError::*;
        use OptionRequest::*;

        let (option_type, data) = value;
        match option_type {
            OptionRequestType::ExportName => {
                if data.is_none() {
                    // Special "default" case as explained in the spec
                    return Ok(ExportName(None));
                }
                // Regular, named export
                let data = data.unwrap();

                if data.len() > MAX_NAME_LEN {
                    return Err(NameExceedsMaxSize)?;
                }

                let name = String::from_utf8(data.into()).map_err(|e| Other(e.into()))?;
                if !is_sane_export_name(&name) {
                    return Err(InvalidExportName)?;
                }
                Ok(ExportName(Some(name)))
            }
            OptionRequestType::Abort => Ok(Abort),
            OptionRequestType::List => {
                if data.is_some() {
                    // The spec defines requests LIST requests with data
                    // SHOULD be rejected.
                    Err(SuperfluousData)
                } else {
                    Ok(List)
                }
            }
            OptionRequestType::StartTls => {
                if data.is_some() {
                    // Requests with data MUST be rejected according
                    // to the spec.
                    Err(SuperfluousData)
                } else {
                    Ok(StartTls)
                }
            }
            OptionRequestType::Info | OptionRequestType::Go => {
                let mut data = match data {
                    Some(data) => data,
                    None => return Err(MissingData),
                };
                let name_len = data.get_u32() as usize;
                if name_len > MAX_NAME_LEN || name_len > (data.len() - 2) {
                    return Err(NameExceedsMaxSize)?;
                }
                let mut name = None; // empty name is allowed by the spec
                if name_len > 0 {
                    let submitted_name = std::str::from_utf8(&data.as_ref()[..name_len])
                        .map_err(|e| Other(e.into()))?
                        .to_string();
                    data.advance(name_len);

                    if !is_sane_export_name(&submitted_name) {
                        return Err(InvalidExportName)?;
                    }
                    name = Some(submitted_name);
                }

                let info_req_num = data.get_u16() as usize;
                let mut expected_remaining_data = info_req_num * 2;

                if expected_remaining_data != data.len() {
                    return Err(UnexpectedAmountOfData)?;
                }

                //todo: find out how to parse the information request items
                // for now they are just returned as a `u16`
                let mut items = Vec::with_capacity(info_req_num);
                while expected_remaining_data > 0 {
                    items.push(data.get_u16());
                    expected_remaining_data -= 1;
                }

                if let OptionRequestType::Go = option_type {
                    Ok(Go(name, items))
                } else {
                    Ok(Info(name, items))
                }
            }
            OptionRequestType::StructuredReply => {
                if data.is_some() {
                    // Requests with data SHOULD be rejected according
                    // to the spec.
                    Err(SuperfluousData)
                } else {
                    Ok(StructuredReply)
                }
            }
            OptionRequestType::ListMetaContext => {
                //todo: data parsing currently unimplemented
                Ok(ListMetaContext)
            }
            OptionRequestType::SetMetaContext => {
                //todo: data parsing currently unimplemented
                Ok(SetMetaContext)
            }
            OptionRequestType::ExtendedHeaders => {
                //todo: data parsing currently unimplemented
                Ok(ExtendedHeaders)
            }
        }
    }
}

fn is_sane_export_name(name: &str) -> bool {
    if name.starts_with(char::is_whitespace) {
        return false;
    }
    if name.ends_with(char::is_whitespace) {
        return false;
    }
    !name.is_empty()
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[repr(u32)]
enum OptionResponseType {
    /// Data sending finished
    Ack = 1u32,
    /// Export description
    Server = 2u32,
    Info = 3u32,
    MetaContext = 4u32,
}

bitflags! {
    /// Response Error Types
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct ErrorType: u32 {
        /// Unknown option
        const UNSUP = 1 | 1 << 31;
        /// Server denied
        const POLICY = 2 | 1 << 31;
        /// Invalid length
        const INVALID = 3 | 1 << 31;
        /// Not compiled in
        const PLATFORM = 4 | 1 << 31;
        /// TLS required
        const TLS_REQD = 5 | 1 << 31;
        /// Export unknown
        const UNKNOWN = 6 | 1 << 31;
        /// Server shutting down
        const SHUTDOWN = 7 | 1 << 31;
        /// Need INFO_BLOCK_SIZE
        const BLOCK_SIZE_REQD = 8 | 1 << 31;
        /// Payload size overflow
        const TOO_BIG = 9 | 1 << 31;
        /// Need extended headers
        const EXT_HEADER_REQD = 10 | 1 << 31;
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, TryFromPrimitive, IntoPrimitive,
)]
#[repr(u16)]
enum InfoType {
    Export = 0u16,
    Name = 1u16,
    Description = 2u16,
    BlockSize = 3u16,
}

bitflags! {
    /// Transmission Flags
    ///
    /// This field is sent by the server after option haggling
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct TransmissionFlags: u16 {
        /// Flags are set
        const HAS_FLAGS = 1 << 0;
        /// Device is read-only
        const READ_ONLY = 1 << 1;
        /// Sending FLUSH is supported
        const SEND_FLUSH = 1 << 2;
        /// Sending FUA (Force Unit Access) is supported
        const SEND_FUA = 1 << 3;
        /// Use elevator algorithm - rotational media
        const ROTATIONAL = 1 << 4;
        /// Sending TRIM (discard) is supported
        const SEND_TRIM = 1 << 5;
        /// Sending WRITE_ZEROES is supported
        const SEND_WRITE_ZEROES = 1 << 6;
        /// Sending DF (Do not Fragment) is supported
        const SEND_DF = 1 << 7;
        /// Multi-client cache consistent
        const CAN_MULTI_CONN = 1 << 8;
        /// Sending RESIZE is supported
        const SEND_RESIZE = 1 << 9;
        /// Send CACHE (prefetch) is supported
        const SEND_CACHE = 1 << 10;
        /// FAST_ZERO flag for WRITE_ZEROES
        const SEND_FAST_ZERO = 1 << 11;
        /// PAYLOAD flag for BLOCK_STATUS
        const BLOCK_STAT_PAYLOAD = 1 << 12;
    }
}

#[derive(Debug, Clone)]
struct NegotiationContext {
    export: Option<Export>,
    transition_format: TransitionFormat,
    /// if set to true, `InfoFormat::ExportName` won't have trailing 124 bytes of zeroes
    no_zeroes: bool,
    /// indicated whether [TransmissionMode::Structured] was negotiated
    structured_reply: bool,
}

impl Default for NegotiationContext {
    fn default() -> Self {
        Self {
            export: Default::default(),
            transition_format: TransitionFormat::ExportName,
            no_zeroes: false,
            structured_reply: false,
        }
    }
}

/// Defines which format to use to transition from `handshake` to `transmission` phase
///
/// Depending on which option was used during negotiation (`NBD_OPT_EXPORT_NAME` or `NBD_OPT_GO`)
/// a different response format is required.
#[derive(Debug, Clone)]
enum TransitionFormat {
    ExportName,
    Go,
}

impl NegotiationContext {
    fn new() -> Self {
        Self::default()
    }

    fn is_valid(&self) -> bool {
        if self.export.is_none() {
            return false;
        }
        true
    }
}

pub(super) async fn process<RX: AsyncRead + Unpin, TX: AsyncWrite + Unpin>(
    rx: &mut RX,
    tx: &mut TX,
    addr: &SocketAddr,
) -> Result<TransmissionHandler, NbdError> {
    // Send the initial magic header & handshake flags
    tx.write_all(INITIAL_SERVER_MESSAGE.as_ref()).await?;
    tx.flush().await?;

    // Read the flags the client has responded with
    let client_hs_flags = ClientFlags::from_bits(rx.get_u32().await?)
        .ok_or(HandshakeError::InvalidClientHandshake)?;

    // Server & Client Handshake flags have to match before we can proceed
    if client_hs_flags != ClientFlags::FIXED_NEWSTYLE | ClientFlags::NO_ZEROES {
        return Err(HandshakeError::HandshakeFailed)?;
    }

    // Handshake looks good, proceed to option negotiation
    let mut ctx = NegotiationContext::new();
    loop {
        // Check for the next option header
        let buf = rx.get_exact(I_HAVE_OPT.len()).await?;
        if &buf.as_ref()[..] != I_HAVE_OPT {
            return Err(NegotiationError::InvalidInput(anyhow!(
                "option header missing"
            )))?;
        }
        let option_type = rx.get_u32().await?;
        // Option header found, read details
        let option_len = rx.get_u32().await? as usize;
        if option_len > MAX_DATA_LEN {
            return Err(NegotiationError::from(OptionError::from(
                DataError::Overflow,
            )))?;
        }
        let mut option_data = None;
        if option_len > 0 {
            option_data = Some(rx.get_exact(option_len).await?);
        }

        match process_option_request(&mut ctx, option_type, option_data) {
            Err(NegotiationError::ExportUnavailableImmediateDisconnect) => {
                // do not respond to the client, return immediately
                return Err(NegotiationError::ExportUnavailableImmediateDisconnect)?;
            }
            Err(NegotiationError::ExportUnavailable) => {
                // the requested export does not exist or access has been denied
                send_error(
                    tx,
                    option_type,
                    ErrorType::UNKNOWN,
                    "requested export is unknown or unavailable".to_string(),
                )
                .await?;
            }
            Err(NegotiationError::InvalidOption(OptionError::UnsupportedOption)) => {
                // this server does not support the option the client has requested
                send_error(tx, option_type, ErrorType::UNSUP, None).await?;
            }
            Err(e) => {
                // all other errors lead to disconnect
                return Err(e)?;
            }
            Ok(NextAction::Abort) => {
                send_ack(tx, option_type).await?;
                return Err(std::io::Error::new(
                    ErrorKind::ConnectionAborted,
                    "client sent abort",
                ))?;
            }
            Ok(NextAction::ProceedToNextPhase) => {
                // we are done with option negotiation
                // transition to transmission phase
                break;
            }
            Ok(NextAction::ReadMore) => {
                send_ack(tx, option_type).await?;
            }
        }
        // more options need to be read
    }

    if !ctx.is_valid() {
        // something went wrong during negotiation, abort
        match ctx.transition_format {
            TransitionFormat::ExportName => {
                // `NBD_OPT_EXPORT_NAME` does not support errors,
                // so this is a no-op
            }
            TransitionFormat::Go => {
                send_error(
                    tx,
                    OptionRequestType::Go.into(),
                    ErrorType::UNKNOWN,
                    "Negotiated options are invalid".to_string(),
                )
                .await?;
            }
        }
        return Err(NegotiationError::NegotiationFailed)?;
    }

    // ready to move to transmission phase

    let export = ctx.export.unwrap();
    let mut flags = TransmissionFlags::HAS_FLAGS;
    if export.read_only {
        flags |= TransmissionFlags::READ_ONLY;
    } else {
        flags |= TransmissionFlags::SEND_FLUSH;
    }
    if export.resizable {
        flags |= TransmissionFlags::SEND_RESIZE;
    }
    if export.rotational {
        flags |= TransmissionFlags::ROTATIONAL;
    }
    if export.trim {
        flags |= TransmissionFlags::SEND_TRIM;
    }

    let mut export_payload = BytesMut::with_capacity(10);
    export_payload.put_u64(export.size);
    export_payload.put_u16(flags.bits());
    let export_payload = export_payload.freeze();

    match ctx.transition_format {
        TransitionFormat::Go => {
            let mut info_export_payload = BytesMut::with_capacity(12);
            info_export_payload.put_u16(InfoType::Export.into());
            info_export_payload.put(export_payload);
            let info_export_payload = info_export_payload.freeze();
            send_info(tx, OptionRequestType::Go.into(), info_export_payload).await?;

            // finalize transition by sending a `NBD_REP_ACK`
            send_ack(tx, OptionRequestType::Go.into()).await?;
        }
        TransitionFormat::ExportName => {
            tx.write_all(export_payload.as_ref()).await?;
            if !ctx.no_zeroes {
                // append unused zeroes to response
                tx.write_all(LEGACY_ZEROED_BYTES.as_ref()).await?;
            }
            tx.flush().await?;
        }
    }

    Ok(TransmissionHandler::new(
        export.name,
        match ctx.structured_reply {
            true => TransmissionMode::Structured,
            false => TransmissionMode::Simple,
        },
        addr.clone(),
    ))
}

async fn send_error<TX: AsyncWrite + Unpin, M: Into<Option<String>>>(
    tx: &mut TX,
    option_type: u32,
    error_type: ErrorType,
    msg: M,
) -> std::io::Result<()> {
    _send_reply(
        tx,
        option_type,
        error_type.bits(),
        msg.into().map(|s| s.into()),
    )
    .await
}

async fn send_ack<TX: AsyncWrite + Unpin>(tx: &mut TX, option_type: u32) -> std::io::Result<()> {
    _send_reply(tx, option_type, OptionResponseType::Ack.into(), None).await
}

async fn send_info<TX: AsyncWrite + Unpin>(
    tx: &mut TX,
    option_type: u32,
    payload: Bytes,
) -> std::io::Result<()> {
    _send_reply(
        tx,
        option_type,
        OptionResponseType::Info.into(),
        Some(payload),
    )
    .await
}

async fn _send_reply<TX: AsyncWrite + Unpin>(
    tx: &mut TX,
    option_type: u32,
    resp_type: u32,
    data: Option<Bytes>,
) -> std::io::Result<()> {
    let data_len = data.as_ref().map(|b| b.len()).unwrap_or(0);
    let mut resp = BytesMut::with_capacity(20 + data_len);
    resp.put_u64(REPLY_MAGIC);
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
}

fn process_option_request(
    ctx: &mut NegotiationContext,
    option_type: u32,
    option_data: Option<Bytes>,
) -> Result<NextAction, NegotiationError> {
    //use DataError::*;
    use NegotiationError::*;
    use OptionError::*;

    let option_type = OptionRequestType::try_from(option_type).map_err(|_| UnsupportedOption)?;

    let option_request = TryInto::<OptionRequest>::try_into((&option_type, option_data))?;
    // Looking good, the option request seems known

    match option_request {
        OptionRequest::ExportName(name) => {
            if let Some(export) = find_export(name) {
                ctx.export = Some(export);
                ctx.transition_format = TransitionFormat::ExportName;
                return Ok(NextAction::ProceedToNextPhase);
            }
            Err(ExportUnavailableImmediateDisconnect)
        }
        OptionRequest::Abort => Ok(NextAction::Abort),
        OptionRequest::Go(name, data) => {
            if !data.is_empty() {
                //todo: unsupported
                return Err(UnsupportedOption)?;
            }
            if let Some(export) = find_export(name) {
                ctx.export = Some(export);
                ctx.transition_format = TransitionFormat::Go;
                return Ok(NextAction::ProceedToNextPhase);
            }
            Err(ExportUnavailable)
        }
        _ => {
            // no other options are supported right now
            Err(UnsupportedOption)?
        }
    }
}

fn find_export(name: Option<String>) -> Option<Export> {
    //todo: this is a stub
    if let Some(name) = name {
        if name == "sia_vbd" {
            return Some(Export {
                name,
                read_only: false,
                trim: true,
                rotational: false,
                size: 1024 * 1024 * 1024 * 10, // 10 GiB
                resizable: false,
            });
        }
    }
    None
}

enum NextAction {
    ReadMore,
    ProceedToNextPhase,
    Abort,
}

#[derive(Error, Debug)]
pub(super) enum NbdError {
    /// A protocol related error occurred during the handshake phase
    #[error("handshake error")]
    HandshakeFailure(#[from] HandshakeError),
    /// An error occurred during the option negotiation phase
    #[error("negotiation error")]
    NegotiationFailure(#[from] NegotiationError),
    /// An `IO` error occurred reading from or writing to the client
    #[error("client io error")]
    IoError(#[from] std::io::Error),
}

#[derive(Error, Debug)]
pub(super) enum HandshakeError {
    /// The handshake could not be read
    #[error("invalid client handshake received")]
    InvalidClientHandshake,
    /// The client handshake flags do not match the servers'
    #[error("handshake failed due to client / server flag mismatch")]
    HandshakeFailed,
}

#[derive(Error, Debug)]
pub(super) enum NegotiationError {
    /// The client supplied input could not be understood, with details
    #[error(transparent)]
    InvalidInput(#[from] anyhow::Error),
    /// An error related to the supplied client request option
    #[error("invalid option")]
    InvalidOption(#[from] OptionError),
    /// The requested export cannot be found or access is denied
    #[error("export unavailable")]
    ExportUnavailable,
    /// The requested export cannot be found or access is denied.
    /// No further communication should be sent to the client.
    /// Disconnect immediately
    ///
    /// This is only for the `NBD_OPT_EXPORT_NAME` option that does not support error handling.
    #[error("export unavailable, disconnect immediately")]
    ExportUnavailableImmediateDisconnect,
    /// The client and server could not come to a valid negotiation result
    #[error("client/server negotiation failed")]
    NegotiationFailed,
}

#[derive(Error, Debug)]
enum OptionError {
    /// The option is either unknown or unsupported
    #[error("unsupported option")]
    UnsupportedOption,
    /// Option requires mandatory data, but the data is missing
    #[error("missing mandatory option data")]
    MissingData,
    /// The option data was invalid, with details
    #[error("invalid option data")]
    InvalidData(#[from] DataError),
    /// Option does not support or allow data, but data was supplied nonetheless
    #[error("superfluous option data supplied")]
    SuperfluousData,
}

#[derive(Error, Debug)]
enum DataError {
    /// Data size exceed `MAX_DATA_LEN`
    #[error("data size exceeds maximum of {max} bytes", max = MAX_DATA_LEN)]
    Overflow,
    /// Name length exceeds `MAX_NAME_LEN`
    #[error("name length exceeds maximum of {max} bytes", max = MAX_NAME_LEN)]
    NameExceedsMaxSize,
    /// Export Name format is invalid
    #[error("export name format is invalid")]
    InvalidExportName,
    /// Data size is not what was excepted
    #[error("unexpected data size")]
    UnexpectedAmountOfData,
    /// Other data error, with optional details
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
