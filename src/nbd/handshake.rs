use super::{Export, TransmissionMode, MAX_PAYLOAD_LEN};
use crate::nbd::transmission::TransmissionHandler;
use crate::{highest_power_of_two, AsyncReadBytesExt, ClientEndpoint};
use bitflags::bitflags;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use once_cell::sync::Lazy;
use std::cmp::{max, PartialEq};
use std::collections::HashMap;
use thiserror::Error;

const NBD_MAGIC: u64 = 0x4e42444d41474943; // NBDMAGIC;
const I_HAVE_OPT: u64 = 0x49484156454F5054; // IHAVEOPT;
const REPLY_MAGIC: u64 = 0x3e889045565a9;

const MAX_DATA_LEN: usize = 8 * 1024;
const MAX_NAME_LEN: usize = 1024;

static INITIAL_SERVER_MESSAGE: Lazy<Bytes> = Lazy::new(|| {
    let mut b = BytesMut::with_capacity(18);
    b.put_u64(NBD_MAGIC);
    b.put_u64(I_HAVE_OPT);
    b.put_u16(ServerFlags::FIXED_NEWSTYLE.bits() | ServerFlags::NO_ZEROES.bits());
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
    Info(Option<String>, Vec<InfoType>),
    Go(Option<String>, Vec<InfoType>),
    StructuredReply,
    ListMetaContext,
    SetMetaContext,
    ExtendedHeaders,
}

impl OptionRequest {
    fn as_req_type(&self) -> OptionRequestType {
        match self {
            Self::ExportName(_) => OptionRequestType::ExportName,
            Self::Abort => OptionRequestType::Abort,
            Self::List => OptionRequestType::List,
            Self::StartTls => OptionRequestType::StartTls,
            Self::Info(_, _) => OptionRequestType::Info,
            Self::Go(_, _) => OptionRequestType::Go,
            Self::StructuredReply => OptionRequestType::StructuredReply,
            Self::ListMetaContext => OptionRequestType::ListMetaContext,
            Self::SetMetaContext => OptionRequestType::SetMetaContext,
            Self::ExtendedHeaders => OptionRequestType::ExtendedHeaders,
        }
    }

    fn deserialize(
        option_type: OptionRequestType,
        data: Option<impl Buf>,
    ) -> Result<Self, OptionError> {
        use DataError::*;
        use OptionError::*;
        use OptionRequest::*;

        match option_type {
            OptionRequestType::ExportName => {
                if data.is_none() {
                    // Special "default" case as explained in the spec
                    return Ok(ExportName(None));
                }
                // Regular, named export
                let mut data = data.unwrap();

                if data.remaining() > MAX_NAME_LEN {
                    return Err(NameExceedsMaxSize)?;
                }

                let name = String::from_utf8(data.copy_to_bytes(data.remaining()).to_vec())
                    .map_err(|e| Other(e.into()))?;
                if !is_sane_export_name(&name) {
                    return Err(InvalidExportName)?;
                }
                Ok(ExportName(Some(name)))
            }
            OptionRequestType::Abort => Ok(Abort),
            OptionRequestType::List => {
                if data.is_some() {
                    // The spec defines LIST requests with data
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
                if name_len > MAX_NAME_LEN || name_len > (data.remaining() - 2) {
                    return Err(NameExceedsMaxSize)?;
                }
                let mut name = None; // empty name is allowed by the spec
                if name_len > 0 {
                    let submitted_name = String::from_utf8(data.copy_to_bytes(name_len).to_vec())
                        .map_err(|e| Other(e.into()))?;

                    if !is_sane_export_name(&submitted_name) {
                        return Err(InvalidExportName)?;
                    }
                    name = Some(submitted_name);
                }

                let info_req_num = data.get_u16() as usize;
                if info_req_num * 2 != data.remaining() {
                    return Err(UnexpectedAmountOfData)?;
                }

                let mut items = Vec::with_capacity(info_req_num);
                for _ in 0..info_req_num {
                    if let Ok(info_type) = InfoType::try_from(data.get_u16()) {
                        items.push(info_type);
                    }
                    // any unknown info types should be ignored as per the spec
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

pub(crate) struct Handshaker {
    exports: HashMap<String, Export>,
    default_export: Option<String>,
    structured_replies_disabled: bool,
    extended_headers_disabled: bool,
}

impl Handshaker {
    pub fn new<I: IntoIterator<Item = (String, Export)>>(
        exports: I,
        default_export: Option<String>,
        structured_replies_disabled: bool,
        extended_headers_disabled: bool,
    ) -> Self {
        Self {
            exports: exports.into_iter().collect(),
            default_export,
            structured_replies_disabled,
            extended_headers_disabled: if structured_replies_disabled {
                // automatically disable extended headers if structured replies are disabled
                true
            } else {
                extended_headers_disabled
            },
        }
    }

    pub(super) async fn process<RX: AsyncRead + Unpin, TX: AsyncWrite + Unpin>(
        &self,
        rx: &mut RX,
        tx: &mut TX,
        client_endpoint: &ClientEndpoint,
    ) -> Result<Option<TransmissionHandler>, NbdError> {
        use RequestError::*;

        // Send the initial magic header & handshake flags
        tx.write_all(INITIAL_SERVER_MESSAGE.as_ref()).await?;
        tx.flush().await?;

        // Read the flags the client has responded with
        let mut buf = [0u8; 4];
        rx.read_exact(&mut buf).await?;
        let client_hs_flags = ClientFlags::from_bits(buf.as_slice().get_u32())
            .ok_or(HandshakeError::InvalidClientHandshake)?;

        // Server & Client Handshake flags have to match before we can proceed
        if client_hs_flags != ClientFlags::FIXED_NEWSTYLE | ClientFlags::NO_ZEROES {
            return Err(HandshakeError::HandshakeFailed)?;
        }

        // Handshake looks good, proceed to option negotiation
        let mut transmission_mode = TransmissionMode::Simple;
        let mut abort = false;

        let mut buf = [0u8; 16];
        loop {
            // read the next request header
            rx.read_exact(&mut buf).await?;
            let mut buf = &buf[..];

            let magic = buf.get_u64();
            if magic != I_HAVE_OPT {
                return Err(InvalidOptionHeader)?;
            }

            let option_type = buf.get_u32();
            let option_len = buf.get_u32() as usize;
            if option_len > MAX_DATA_LEN {
                return Err(InvalidOption(OptionError::from(DataError::Overflow)))?;
            }

            let mut option_data = None;
            if option_len > 0 {
                option_data = Some(rx.get_exact(option_len).await?);
            }

            let option_type = OptionRequestType::try_from(option_type)
                .map_err(|_| InvalidOption(OptionError::UnsupportedOption(option_type)))?;

            let option_request = match OptionRequest::deserialize(option_type, option_data) {
                Ok(req) => req,
                Err(OptionError::UnsupportedOption(option_type)) => {
                    // this server does not support the option the client has requested
                    send_error(tx, option_type, ErrorType::UNSUP, None).await?;
                    // don't disconnect, read the next request instead
                    continue;
                }
                Err(e) => {
                    // option could not be read, disconnect
                    return Err(NbdError::InvalidRequest(e.into()));
                }
            };

            match option_request {
                OptionRequest::ExportName(name) => {
                    if let Some(export) = self.find_export(name) {
                        // export ok, inform client and proceed to transmission phase
                        tx.write_all(to_export_payload(&export, transmission_mode).as_ref())
                            .await?;
                        return Ok(Some(TransmissionHandler::new(
                            export,
                            transmission_mode,
                            client_endpoint.clone(),
                        )));
                    }
                    // export is unavailable, but client does not support error handling
                    // do not respond to the client, end conversation immediately
                    return Ok(None);
                }
                OptionRequest::Abort => {
                    // client requested to end the session
                    abort = true;
                }
                OptionRequest::Info(ref name, ref info_types)
                | OptionRequest::Go(ref name, ref info_types) => {
                    if let Some(export) = self.find_export(name.as_ref()) {
                        for info_type in info_types {
                            match info_type {
                                InfoType::Name => {
                                    let name_bytes = export.name.as_bytes();
                                    let mut payload = BytesMut::with_capacity(name_bytes.len() + 2);
                                    payload.put_u16(InfoType::Name.into());
                                    payload.put(name_bytes);
                                    send_info(
                                        tx,
                                        option_request.as_req_type().into(),
                                        payload.freeze(),
                                    )
                                    .await?;
                                }
                                InfoType::Description => {
                                    let options = export.options();
                                    if let Some(desc_bytes) =
                                        options.description.as_ref().map(|s| s.as_bytes())
                                    {
                                        let mut payload =
                                            BytesMut::with_capacity(desc_bytes.len() + 2);
                                        payload.put_u16(InfoType::Description.into());
                                        payload.put(desc_bytes);
                                        send_info(
                                            tx,
                                            option_request.as_req_type().into(),
                                            payload.freeze(),
                                        )
                                        .await?;
                                    }
                                }
                                InfoType::BlockSize => {
                                    let options = export.options();
                                    if let Some((min, preferred)) = options.block_size {
                                        let mut payload = BytesMut::with_capacity(14);
                                        payload.put_u16(InfoType::BlockSize.into());
                                        payload.put_u32(min);
                                        payload.put_u32(preferred);

                                        // max block size must be a multiple of min, a power of two
                                        // and not exceed (MAX_PAYLOAD_LEN + headers)
                                        let max_block_size = highest_power_of_two(
                                            ((MAX_PAYLOAD_LEN - 33) / min) * min,
                                        );
                                        payload.put_u32(max(max_block_size, preferred));

                                        send_info(
                                            tx,
                                            option_request.as_req_type().into(),
                                            payload.freeze(),
                                        )
                                        .await?;
                                    }
                                }
                                InfoType::Export => {
                                    // ignore, will always be sent anyway
                                }
                            }
                        }
                        // always send export info, even if not explicitly requested
                        let mut info_export_payload = BytesMut::with_capacity(12);
                        info_export_payload.put_u16(InfoType::Export.into());
                        info_export_payload.put(to_export_payload(&export, transmission_mode));
                        let info_export_payload = info_export_payload.freeze();
                        send_info(tx, option_request.as_req_type().into(), info_export_payload)
                            .await?;

                        if option_request.as_req_type() == OptionRequestType::Go {
                            // send Ack and move to transmission phase
                            send_ack(tx, OptionRequestType::Go.into()).await?;
                            return Ok(Some(TransmissionHandler::new(
                                export,
                                transmission_mode,
                                client_endpoint.clone(),
                            )));
                        }
                    }
                    // the requested export does not exist or access has been denied
                    send_error(
                        tx,
                        option_request.as_req_type().into(),
                        ErrorType::UNKNOWN,
                        "requested export is unknown or unavailable".to_string(),
                    )
                    .await?;
                    continue;
                }
                OptionRequest::StructuredReply if !self.structured_replies_disabled => {
                    transmission_mode = TransmissionMode::Structured;
                }
                OptionRequest::ExtendedHeaders if !self.extended_headers_disabled => {
                    transmission_mode = TransmissionMode::Extended;
                }
                OptionRequest::List => {
                    for export_name in self.exports.keys() {
                        send_server(tx, option_request.as_req_type().into(), export_name.clone())
                            .await?;
                    }
                }
                _ => {
                    // no other options are supported right now
                    send_error(
                        tx,
                        option_request.as_req_type().into(),
                        ErrorType::UNSUP,
                        None,
                    )
                    .await?;
                    continue;
                }
            }
            send_ack(tx, option_request.as_req_type()).await?;
            if abort {
                return Ok(None);
            }
        }
    }

    fn find_export<S: AsRef<str>>(&self, name: Option<S>) -> Option<Export> {
        let name = match name.as_ref() {
            Some(name) => name.as_ref(),
            None => match self.default_export.as_ref() {
                Some(name) => name.as_str(),
                None => return None,
            },
        };
        self.exports.get(name).map(|exp| exp.clone())
    }
}

fn to_export_payload(export: &Export, transmission_mode: TransmissionMode) -> Bytes {
    let mut flags = TransmissionFlags::HAS_FLAGS;
    if export.read_only() {
        flags |= TransmissionFlags::READ_ONLY;
    } else {
        flags |= TransmissionFlags::SEND_FLUSH;
        flags |= TransmissionFlags::SEND_FUA;
    };
    let options = export.options();
    if options.rotational {
        flags |= TransmissionFlags::ROTATIONAL;
    }
    if options.trim {
        flags |= TransmissionFlags::SEND_TRIM;
    }
    if options.resizable && transmission_mode == TransmissionMode::Extended {
        flags |= TransmissionFlags::SEND_RESIZE;
    }

    flags |= TransmissionFlags::SEND_WRITE_ZEROES;
    if options.fast_zeroes {
        flags |= TransmissionFlags::SEND_FAST_ZERO;
    }

    if transmission_mode != TransmissionMode::Simple {
        flags |= TransmissionFlags::SEND_DF;
    }

    flags |= TransmissionFlags::CAN_MULTI_CONN;
    flags |= TransmissionFlags::SEND_CACHE;

    let mut export_payload = BytesMut::with_capacity(10);
    export_payload.put_u64(options.size);
    export_payload.put_u16(flags.bits());
    export_payload.freeze()
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

async fn send_ack<TX: AsyncWrite + Unpin>(
    tx: &mut TX,
    option_type: OptionRequestType,
) -> std::io::Result<()> {
    _send_reply(tx, option_type.into(), OptionResponseType::Ack.into(), None).await
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

async fn send_server<TX: AsyncWrite + Unpin>(
    tx: &mut TX,
    option_type: u32,
    export_name: String,
) -> std::io::Result<()> {
    let name_bytes = export_name.as_bytes();
    let mut payload = BytesMut::with_capacity(name_bytes.len() + 4);
    payload.put_u32(name_bytes.len() as u32);
    payload.put(name_bytes);
    _send_reply(
        tx,
        option_type,
        OptionResponseType::Server.into(),
        Some(payload.freeze()),
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

#[derive(Error, Debug)]
pub(super) enum NbdError {
    /// A protocol related error occurred during the handshake phase
    #[error("handshake error")]
    HandshakeFailure(#[from] HandshakeError),
    /// Request was not valid
    #[error("invalid request")]
    InvalidRequest(#[from] RequestError),
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
pub(super) enum RequestError {
    /// Option header is invalid
    #[error("option header invalid")]
    InvalidOptionHeader,
    /// An error related to the supplied client request option
    #[error("invalid option")]
    InvalidOption(#[from] OptionError),
    /// An `IO` error occurred reading from the client
    #[error("client io error")]
    IoError(#[from] std::io::Error),
}

#[derive(Error, Debug)]
pub(super) enum OptionError {
    /// The option is either unknown or unsupported
    #[error("unsupported option")]
    UnsupportedOption(u32),
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
pub(super) enum DataError {
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
