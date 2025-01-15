use protos::frame_header::Type as ProtoFrameType;
use protos::FileInfo as ProtoFileInfo;
use protos::FrameHeader as ProtoFrameHeader;
use std::future::Future;

use crate::vbd::wal::ParseError::InvalidMagicNumber;
use crate::vbd::wal::PreambleError::{
    InvalidBodyLength, InvalidHeaderLength, InvalidLength, InvalidPaddingLength,
};
use crate::vbd::{BlockId, ClusterId, Commit, TypedUuid, VbdId};
use crate::{AsyncReadExtBuffered, AsyncWriteBytesExt};
use async_scoped::TokioScope;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::{DateTime, Duration, Utc};
use futures::future::BoxFuture;
use futures::{AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt, Stream};
use prost::{DecodeError, Message};
use std::io::{Error, SeekFrom};
use std::mem;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use uuid::Uuid;

const PREAMBLE_LEN: usize = 14;
const VALID_HEADER_LEN: Range<u16> = 8..2048;
const VALID_BODY_LEN: Range<u32> = 0..1024 * 1024 * 100;
const VALID_PADDING_LEN: Range<u32> = 0..1024 * 1024 * 10;

const MAGIC_NUMBER: &'static [u8; 18] = &[
    0x00, 0xFF, 0x73, 0x69, 0x61, 0x5F, 0x76, 0x62, 0x64, 0x20, 0x57, 0x41, 0x4C, 0x00, 0x00, 0x00,
    0x00, 0x01,
];

pub trait WalSink: AsyncWrite + AsyncSeek + Unpin + Send {
    fn len(&self) -> impl Future<Output = Result<u64, std::io::Error>> + Send;
    fn set_len(&mut self, len: u64) -> impl Future<Output = Result<(), std::io::Error>> + Send;
}

pub struct TokioFileWal {
    file: tokio_util::compat::Compat<tokio::fs::File>,
}

impl TokioFileWal {
    pub fn new(file: tokio::fs::File) -> Self {
        Self {
            file: file.compat_write(),
        }
    }

    pub fn into_inner(self) -> tokio::fs::File {
        self.file.into_inner()
    }
}

impl AsyncWrite for TokioFileWal {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.file).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.file).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.file).poll_close(cx)
    }
}

impl AsyncSeek for TokioFileWal {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.file).poll_seek(cx, pos)
    }
}

impl WalSink for TokioFileWal {
    async fn len(&self) -> Result<u64, Error> {
        Ok(self.file.get_ref().metadata().await?.len())
    }

    async fn set_len(&mut self, len: u64) -> Result<(), Error> {
        self.file.get_mut().set_len(len).await
    }
}

pub struct WalWriter<IO> {
    id: WalId,
    io: IO,
    offset: usize,
    header: FileHeader,
    max_size: u64,
}

impl<IO: WalSink> WalWriter<IO> {
    pub async fn new(
        mut io: IO,
        wal_id: WalId,
        vbd_id: VbdId,
        preceding_wal_id: Option<WalId>,
        max_size: u64,
    ) -> Result<Self, WalError> {
        let header = FileHeader {
            wal_id,
            preceding_wal_id,
            created: Utc::now(),
            vbd_id,
        };

        let mut offset = 0usize;
        io.write_all(MAGIC_NUMBER).await?;
        offset += MAGIC_NUMBER.len();

        let fi = Into::<ProtoFileInfo>::into(&header);
        let mut buf = BytesMut::with_capacity(PREAMBLE_LEN + fi.encoded_len());

        offset += encode_preamble(&Preamble::header_only(fi.encoded_len() as u16), &mut buf)?;

        fi.encode(&mut buf)
            .map_err(|e| Into::<EncodeError>::into(e))?;
        offset += fi.encoded_len();

        io.write_all(&buf).await?;
        io.flush().await?;

        Ok(Self {
            id: wal_id,
            io,
            header,
            offset,
            max_size,
        })
    }

    pub fn id(&self) -> &WalId {
        &self.id
    }

    pub async fn remaining(&self) -> Result<u64, WalError> {
        let len = self.io.len().await?;
        if len > self.max_size {
            return Err(EncodeError::MaxWalSizeExceeded)?;
        }
        Ok(self.max_size - len)
    }

    pub async fn begin(
        self,
        preceding_commit: Commit,
        reserve_space: u64,
    ) -> Result<Tx<IO>, (WalError, Result<Self, RollbackError>)> {
        if let Err(err) = {
            match self.remaining().await {
                Ok(remaining) => {
                    if remaining < reserve_space {
                        Err(EncodeError::WalSpaceInsufficient {
                            req: reserve_space,
                            rem: remaining,
                        }
                        .into())
                    } else {
                        Ok(())
                    }
                }
                Err(e) => Err(e),
            }
        } {
            return Err((err, Ok(self)));
        }

        Ok(Tx::new(
            Uuid::now_v7().into(),
            self.header.wal_id,
            self.header.vbd_id,
            preceding_commit,
            Utc::now(),
            reserve_space,
            self,
        )
        .await?)
    }
}

pub enum TxContent {
    Block(BlockId, Position<u64, u32>),
    Cluster(ClusterId, Position<u64, u32>),
    State(Commit, Position<u64, u32>),
}

pub struct TxDetails {
    pub tx_id: TxId,
    pub wal_id: WalId,
    pub vbd_id: VbdId,
    pub commit: Commit,
    pub preceding_commit: Commit,
    pub created: DateTime<Utc>,
    pub commited: DateTime<Utc>,
    pub position: Position<u64, u64>,
}

impl TxDetails {
    pub fn duration(&self) -> Duration {
        self.commited - self.created
    }

    pub fn len(&self) -> u64 {
        self.position.length
    }
}

pub struct Tx_ {}

type TxId = TypedUuid<Tx_>;

pub struct Tx<IO: WalSink> {
    id: TxId,
    wal_id: WalId,
    vbd_id: VbdId,
    created: DateTime<Utc>,
    initial_len: u64,
    len: u64,
    position: u64,
    writer: Option<WalWriter<IO>>,
    buf: BytesMut,
    preceding_commit: Commit,
}

impl<IO: WalSink> Tx<IO> {
    async fn new(
        id: TxId,
        wal_id: WalId,
        vbd_id: VbdId,
        preceding_commit: Commit,
        created: DateTime<Utc>,
        reserve: u64,
        mut writer: WalWriter<IO>,
    ) -> Result<Self, (WalError, Result<WalWriter<IO>, RollbackError>)> {
        async fn prepare_wal<IO: WalSink>(
            writer: &mut WalWriter<IO>,
            reserve: u64,
        ) -> Result<u64, WalError> {
            let initial_len = writer.io.len().await?;
            writer.io.set_len(initial_len + reserve).await?;
            writer.io.seek(SeekFrom::Start(initial_len)).await?;
            Ok(initial_len)
        }

        let initial_len = match prepare_wal(&mut writer, reserve).await {
            Ok(l) => l,
            Err(e) => {
                return Err((e, Err(RollbackError::UnclearState)));
            }
        };

        let mut this = Self {
            id,
            wal_id,
            vbd_id,
            created: created.clone(),
            initial_len,
            len: initial_len + reserve,
            position: initial_len,
            writer: Some(writer),
            buf: BytesMut::with_capacity(VALID_HEADER_LEN.end as usize + PREAMBLE_LEN),
            preceding_commit: preceding_commit.clone(),
        };

        if let Err(e) = this.write_tx_begin(preceding_commit, created).await {
            if let Some(mut wal) = this.writer.take() {
                return match _rollback(&mut wal, initial_len).await {
                    Ok(()) => Err((e, Ok(wal))),
                    Err(re) => Err((e, Err(re))),
                };
            }
            return Err((e, Err(RollbackError::UnclearState)));
        }

        Ok(this)
    }

    async fn write_tx_begin(
        &mut self,
        preceding_commit: Commit,
        created: DateTime<Utc>,
    ) -> Result<(), WalError> {
        let frame = WriteFrame::TxBegin(TxBegin {
            transaction_id: self.id,
            preceding_content_id: preceding_commit,
            created,
        });
        self.write_frame(frame).await?;
        Ok(())
    }

    async fn write_frame(&mut self, frame: WriteFrame<'_>) -> Result<Position<u64, u16>, WalError> {
        let buf = &mut self.buf;
        buf.clear();
        let header_len = encode_frame(&mut *buf, frame)? - PREAMBLE_LEN;
        let remaining = self.len - self.position;
        if remaining < buf.len() as u64 {
            return Err(EncodeError::WalSpaceInsufficient {
                req: buf.len() as u64,
                rem: remaining,
            })?;
        }
        self.writer.as_mut().unwrap().io.write_all(&buf).await?;
        let header_pos = Position::new(self.position + PREAMBLE_LEN as u64, header_len as u16);
        self.position += buf.len() as u64;
        Ok(header_pos)
    }

    async fn write_body(
        &mut self,
        body: impl AsRef<[u8]>,
        padding1: u32,
        padding2: u32,
    ) -> Result<Position<u64, u32>, WalError> {
        let body = body.as_ref();
        let total_len = body.len() as u64 + padding1 as u64 + padding2 as u64;
        let remaining = self.len - self.position;
        if total_len > remaining {
            return Err(EncodeError::WalSpaceInsufficient {
                req: total_len,
                rem: remaining,
            })?;
        }
        if padding1 > 0 {
            self.writer
                .as_mut()
                .unwrap()
                .io
                .write_zeroes(padding1 as usize)
                .await?;
        }
        self.writer.as_mut().unwrap().io.write_all(body).await?;
        if padding2 > 0 {
            self.writer
                .as_mut()
                .unwrap()
                .io
                .write_zeroes(padding2 as usize)
                .await?;
        }
        let body_pos = Position::new(self.position + padding1 as u64, body.len() as u32);
        self.position += total_len;
        Ok(body_pos)
    }

    pub async fn block(&mut self, block_id: BlockId, data: Option<&Bytes>) -> Result<(), WalError> {
        let block = Block {
            content_id: block_id,
            length: data.map(|d| d.len() as u32).unwrap_or(0),
        };
        let frame = WriteFrame::Block((block, None, None));
        self.write_frame(frame).await?;
        if let Some(data) = data {
            self.write_body(data, 0, 0).await?;
        }
        Ok(())
    }

    pub async fn cluster(&mut self, cluster: &super::Cluster) -> Result<(), WalError> {
        let cluster = cluster.into();
        let frame = WriteFrame::Cluster(&cluster);
        self.write_frame(frame).await?;
        self.write_body(cluster.encode_to_vec(), 0, 0).await?;
        Ok(())
    }

    pub async fn state(
        &mut self,
        content_id: &Commit,
        cluster_ids: impl Iterator<Item = &ClusterId>,
    ) -> Result<(), WalError> {
        let state = protos::State {
            content_id: Some(content_id.into()),
            cluster_ids: cluster_ids.into_iter().map(|c| c.into()).collect(),
        };
        let frame = WriteFrame::State(&state);
        self.write_frame(frame).await?;
        self.write_body(state.encode_to_vec(), 0, 0).await?;
        Ok(())
    }

    pub async fn commit(
        mut self,
        content_id: Commit,
    ) -> Result<(WalWriter<IO>, TxDetails), (WalError, Result<WalWriter<IO>, RollbackError>)> {
        let commited = Utc::now();
        let frame = WriteFrame::TxCommit(TxCommit {
            transaction_id: self.id,
            content_id: content_id.clone(),
            commited: commited.clone(),
        });

        async fn write_commit<IO: WalSink>(
            tx: &mut Tx<IO>,
            frame: WriteFrame<'_>,
            position: u64,
        ) -> Result<(), WalError> {
            tx.write_frame(frame).await?;
            tx.writer.as_mut().unwrap().io.set_len(position).await?;
            tx.writer.as_mut().unwrap().io.flush().await?;
            Ok(())
        }
        let position = self.position;
        if let Err(err) = write_commit(&mut self, frame, position).await {
            // error during commit, rolling back
            if let Err(re) = _rollback(&mut self.writer.as_mut().unwrap(), self.initial_len).await {
                return Err((err, Err(re)));
            }
            return Err((err, Ok(self.writer.take().unwrap())));
        }
        //todo
        eprintln!("commited tx: {}", self.id);

        Ok((
            self.writer.take().unwrap(),
            TxDetails {
                tx_id: self.id,
                wal_id: self.wal_id,
                vbd_id: self.vbd_id,
                commit: content_id,
                preceding_commit: self.preceding_commit.clone(),
                created: self.created,
                commited,
                position: Position::new(self.initial_len, self.position - self.initial_len),
            },
        ))
    }

    pub async fn rollback(mut self) -> Result<WalWriter<IO>, RollbackError> {
        match self.writer.take() {
            Some(mut wal) => {
                _rollback(&mut wal, self.initial_len).await?;
                Ok(wal)
            }
            None => Err(RollbackError::UnclearState),
        }
    }
}

async fn _rollback<IO: WalSink>(
    wal: &mut WalWriter<IO>,
    position: u64,
) -> Result<(), RollbackError> {
    wal.io.seek(SeekFrom::Start(position)).await?;
    wal.io.set_len(position).await?;
    wal.io.flush().await?;
    Ok(())
}

impl<IO: WalSink> Drop for Tx<IO> {
    fn drop(&mut self) {
        if let Some(mut wal) = self.writer.take() {
            TokioScope::scope_and_block(|s| {
                s.spawn(_rollback(&mut wal, self.initial_len));
            });
        }
    }
}

pub trait WalSource: AsyncRead + AsyncSeek + Unpin + Send {}
impl<T: AsyncRead + AsyncSeek + Unpin + Send> WalSource for T {}

pub struct WalReader<IO> {
    io: IO,
    header: FileHeader,
    first_frame_offset: u64,
}

impl<IO: WalSource> WalReader<IO> {
    pub async fn new(mut io: IO) -> Result<Self, WalError> {
        let header = read_file_header(&mut io).await?;

        Ok(Self {
            io,
            header: header.header,
            first_frame_offset: header.next_frame_offset,
        })
    }

    pub async fn frames(&mut self) -> FrameStream<&mut IO> {
        FrameStream::new(&mut self.io, self.first_frame_offset)
    }
}

pub struct FrameStream<'a, IO: WalSource> {
    state: FrameStreamState<'a, IO>,
}

enum FrameStreamState<'a, IO: WalSource> {
    New(u64, IO, BytesMut),
    Seeking(u64, IO, BytesMut),
    Reading(BoxFuture<'a, (Result<ReadFrameItem, WalError>, IO, BytesMut)>),
    Done,
}

impl<'a, IO: WalSource + 'a> FrameStream<'a, IO> {
    fn new(io: IO, first_frame_offset: u64) -> Self {
        Self {
            state: FrameStreamState::New(
                first_frame_offset,
                io,
                BytesMut::zeroed(VALID_HEADER_LEN.end as usize),
            ),
        }
    }

    fn read_frame(
        mut io: IO,
        mut buf: BytesMut,
    ) -> BoxFuture<'a, (Result<ReadFrameItem, WalError>, IO, BytesMut)> {
        Box::pin(async move {
            buf.clear();
            let res = read_frame(&mut io, &mut buf).await;
            (res, io, buf)
        })
    }
}

impl<'a, IO: WalSource + 'a> Stream for FrameStream<'a, IO> {
    type Item = Result<ReadFrameItem, WalError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match mem::replace(&mut self.state, FrameStreamState::Done) {
                FrameStreamState::New(position, io, buf) => {
                    self.state = FrameStreamState::Seeking(position, io, buf);
                    continue;
                }
                FrameStreamState::Seeking(position, mut io, buf) => {
                    match Pin::new(&mut io).poll_seek(cx, SeekFrom::Start(position)) {
                        Poll::Pending => {
                            self.state = FrameStreamState::Seeking(position, io, buf);
                            return Poll::Pending;
                        }
                        Poll::Ready(Ok(actual_position)) => {
                            if position == actual_position {
                                self.state = FrameStreamState::Reading(Self::read_frame(io, buf));
                                continue;
                            } else {
                                self.state = FrameStreamState::Seeking(position, io, buf);
                                continue;
                            }
                        }
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(Some(Err(e.into())));
                        }
                    }
                }
                FrameStreamState::Reading(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.state = FrameStreamState::Reading(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready((Ok(res), io, buf)) => {
                        self.state = FrameStreamState::Seeking(res.next_frame_offset(), io, buf);
                        return Poll::Ready(Some(Ok(res)));
                    }
                    Poll::Ready((Err(err), _, _)) => {
                        return match err {
                            WalError::IoError(err)
                                if err.kind() == std::io::ErrorKind::UnexpectedEof =>
                            {
                                Poll::Ready(None)
                            }
                            _ => Poll::Ready(Some(Err(err))),
                        };
                    }
                },
                FrameStreamState::Done => {
                    panic!("polled after completion");
                }
            }
        }
    }
}

impl<IO> WalReader<IO> {
    pub fn header(&self) -> &FileHeader {
        &self.header
    }
}

pub struct Position<O, L> {
    pub offset: O,
    pub length: L,
}

impl<O, L> Position<O, L> {
    fn new(offset: O, length: L) -> Self {
        Self { offset, length }
    }
}

pub struct Wal {}
pub type WalId = TypedUuid<Wal>;

pub struct FileHeader {
    pub wal_id: WalId,
    pub vbd_id: VbdId,
    pub created: DateTime<Utc>,
    pub preceding_wal_id: Option<WalId>,
}

fn parse_file_preamble<B: Buf>(
    buf: &mut B,
    mut offset: u64,
) -> Result<(Position<u64, u16>, u64), ParseError> {
    if buf.remaining() < 32 {
        return Err(InvalidLength)?;
    }

    if buf.take(MAGIC_NUMBER.len()).chunk() != MAGIC_NUMBER {
        return Err(InvalidMagicNumber)?;
    }
    buf.advance(MAGIC_NUMBER.len());
    offset += MAGIC_NUMBER.len() as u64 + PREAMBLE_LEN as u64;
    let preamble = parse_preamble(buf)?;
    let first_frame_offset = offset + preamble.content_len();

    Ok((Position::new(offset, preamble.header), first_frame_offset))
}

async fn read_file_header<IO: WalSource>(mut io: IO) -> Result<ReadFrame<FileHeader>, WalError> {
    let start_offset = io.stream_position().await?;
    let mut buf = BytesMut::with_capacity(VALID_HEADER_LEN.end as usize);
    io.read_exact_buffered(&mut buf, MAGIC_NUMBER.len() + PREAMBLE_LEN)
        .await?;
    let (header_pos, next_frame_at) = parse_file_preamble(&mut buf, start_offset)?;
    buf.clear();
    io.seek(SeekFrom::Start(header_pos.offset)).await?;
    io.read_exact_buffered(&mut buf, header_pos.length as usize)
        .await?;
    Ok(ReadFrame {
        header: parse_file_header(buf)?,
        body: None,
        next_frame_offset: next_frame_at,
    })
}

fn parse_file_header<B: Buf>(buf: B) -> Result<FileHeader, ParseError> {
    Ok(ProtoFileInfo::decode(buf)?.try_into()?)
}

async fn read_frame<IO: WalSource, B: BufMut + Buf>(
    mut io: IO,
    mut buf: B,
) -> Result<ReadFrameItem, WalError> {
    let mut offset = io.stream_position().await?;
    if buf.remaining_mut() < PREAMBLE_LEN {
        return Err(ParseError::BufferTooSmall {
            req: PREAMBLE_LEN,
            rem: buf.remaining_mut(),
        })?;
    }
    io.read_exact_buffered(&mut buf, PREAMBLE_LEN).await?;
    offset += PREAMBLE_LEN as u64;
    let preamble = parse_preamble(&mut buf)?;
    let body = if preamble.body == 0 {
        None
    } else {
        Some(Position::new(
            offset + preamble.header as u64 + preamble.padding1 as u64,
            preamble.body,
        ))
    };
    let header_len = preamble.header as usize;

    if buf.remaining_mut() < header_len {
        return Err(ParseError::BufferTooSmall {
            req: header_len,
            rem: buf.remaining_mut(),
        })?;
    }
    io.read_exact_buffered(&mut buf, header_len).await?;
    Ok(parse_frame(
        &mut buf,
        body,
        offset + preamble.content_len(),
    )?)
}

fn parse_frame<B: Buf>(
    buf: &mut B,
    body: Option<Position<u64, u32>>,
    next_frame_at: u64,
) -> Result<ReadFrameItem, ParseError> {
    let proto_frame_header = ProtoFrameHeader::decode(buf)?;
    match proto_frame_header.r#type {
        Some(ProtoFrameType::TxBegin(begin)) => Ok(ReadFrameItem::TxBegin(ReadFrame {
            header: begin.try_into()?,
            body,
            next_frame_offset: next_frame_at,
        })),
        Some(ProtoFrameType::TxCommit(commit)) => Ok(ReadFrameItem::TxCommit(ReadFrame {
            header: commit.try_into()?,
            body,
            next_frame_offset: next_frame_at,
        })),
        Some(ProtoFrameType::Block(block)) => {
            let block = TryInto::<Block>::try_into(block)?;
            let body_length = body.as_ref().map(|p| p.length).unwrap_or(0);
            if body_length != block.length {
                return Err(FrameError::BlockFrameError(
                    BlockFrameError::BodyLengthMismatch {
                        exp: body_length,
                        found: body_length,
                    },
                ))?;
            }
            Ok(ReadFrameItem::Block(ReadFrame {
                header: block,
                body,
                next_frame_offset: next_frame_at,
            }))
        }
        Some(ProtoFrameType::Cluster(cluster)) => Ok(ReadFrameItem::Cluster(ReadFrame {
            header: cluster.try_into()?,
            body,
            next_frame_offset: next_frame_at,
        })),
        Some(ProtoFrameType::State(state)) => Ok(ReadFrameItem::State(ReadFrame {
            header: state.try_into()?,
            body,
            next_frame_offset: next_frame_at,
        })),
        None => Err(FrameError::FrameTypeInvalid)?,
    }
}

fn encode_frame<B: BufMut>(mut buf: B, frame: WriteFrame) -> Result<usize, EncodeError> {
    Ok(match frame {
        WriteFrame::TxBegin(begin) => {
            let begin = Into::<protos::frame_header::TxBegin>::into(&begin);
            let preamble = Preamble::header_only(begin.encoded_len() as u16);
            let mut len = encode_preamble(&preamble, &mut buf)?;
            begin.encode(&mut buf)?;
            len += begin.encoded_len();
            len
        }
        WriteFrame::TxCommit(commit) => {
            let commit = Into::<protos::frame_header::TxCommit>::into(&commit);
            let preamble = Preamble::header_only(commit.encoded_len() as u16);
            let mut len = encode_preamble(&preamble, &mut buf)?;
            commit.encode(&mut buf)?;
            len += commit.encoded_len();
            len
        }
        WriteFrame::Block((block, padding1, padding2)) => {
            let block = Into::<protos::frame_header::Block>::into(&block);
            let preamble = Preamble::full(
                block.encoded_len() as u16,
                padding1,
                Some(block.length),
                padding2,
            );
            let mut len = encode_preamble(&preamble, &mut buf)?;
            block.encode(&mut buf)?;
            len += block.encoded_len();
            len
        }
        WriteFrame::Cluster(cluster) => {
            let header = protos::frame_header::Cluster {
                content_id: cluster.content_id.clone(),
            };
            let preamble = Preamble::full(
                header.encoded_len() as u16,
                None,
                cluster.encoded_len() as u32,
                None,
            );
            let mut len = encode_preamble(&preamble, &mut buf)?;
            cluster.encode(&mut buf)?;
            len += cluster.encoded_len();
            len
        }
        WriteFrame::State(state) => {
            let header = protos::frame_header::State {
                content_id: state.content_id.clone(),
            };
            let preamble = Preamble::full(
                header.encoded_len() as u16,
                None,
                state.encoded_len() as u32,
                None,
            );
            let mut len = encode_preamble(&preamble, &mut buf)?;
            state.encode(&mut buf)?;
            len += state.encoded_len();
            len
        }
    })
}

pub enum ReadFrameItem {
    TxBegin(ReadFrame<TxBegin>),
    TxCommit(ReadFrame<TxCommit>),
    Block(ReadFrame<Block>),
    Cluster(ReadFrame<ClusterId>),
    State(ReadFrame<Commit>),
}

impl ReadFrameItem {
    fn next_frame_offset(&self) -> u64 {
        match self {
            ReadFrameItem::TxBegin(frame) => frame.next_frame_offset,
            ReadFrameItem::TxCommit(frame) => frame.next_frame_offset,
            ReadFrameItem::Block(frame) => frame.next_frame_offset,
            ReadFrameItem::Cluster(frame) => frame.next_frame_offset,
            ReadFrameItem::State(frame) => frame.next_frame_offset,
        }
    }
}

pub struct TxBegin {
    transaction_id: TxId,
    preceding_content_id: Commit,
    created: DateTime<Utc>,
}

pub struct TxCommit {
    transaction_id: TxId,
    content_id: Commit,
    commited: DateTime<Utc>,
}

struct Block {
    content_id: BlockId,
    length: u32,
}

struct State {
    content_id: Commit,
    clusters: Vec<ClusterId>,
}

pub struct ReadFrame<T> {
    header: T,
    body: Option<Position<u64, u32>>,
    next_frame_offset: u64,
}

enum WriteFrame<'a> {
    TxBegin(TxBegin),
    TxCommit(TxCommit),
    Block((Block, Option<u32>, Option<u32>)),
    Cluster(&'a protos::Cluster),
    State(&'a protos::State),
}

struct Preamble {
    header: u16,
    padding1: u32,
    body: u32,
    padding2: u32,
}

impl Preamble {
    fn header_only<H: Into<u16>>(header: H) -> Self {
        Self {
            header: header.into(),
            padding1: 0,
            body: 0,
            padding2: 0,
        }
    }

    fn full<H: Into<u16>, P1: Into<Option<u32>>, B: Into<Option<u32>>, P2: Into<Option<u32>>>(
        header: H,
        padding1: P1,
        body: B,
        padding2: P2,
    ) -> Self {
        Self {
            header: header.into(),
            padding1: padding1.into().unwrap_or(0),
            body: body.into().unwrap_or(0),
            padding2: padding2.into().unwrap_or(0),
        }
    }

    fn content_len(&self) -> u64 {
        self.header as u64 + self.padding1 as u64 + self.body as u64 + self.padding2 as u64
    }
}

fn parse_preamble<B: Buf>(buf: &mut B) -> Result<Preamble, ParseError> {
    if buf.remaining() < PREAMBLE_LEN {
        return Err(InvalidLength)?;
    }
    let header = buf.get_u16();
    if !VALID_HEADER_LEN.contains(&header) {
        return Err(InvalidHeaderLength {
            len: header,
            min: VALID_HEADER_LEN.start,
            max: VALID_HEADER_LEN.end,
        })?;
    }

    let padding1 = buf.get_u32();
    if !VALID_PADDING_LEN.contains(&padding1) {
        return Err(InvalidPaddingLength {
            len: padding1,
            min: VALID_PADDING_LEN.start,
            max: VALID_PADDING_LEN.end,
        })?;
    }

    let body = buf.get_u32();
    if !VALID_BODY_LEN.contains(&body) {
        return Err(InvalidBodyLength {
            len: body,
            min: VALID_BODY_LEN.start,
            max: VALID_BODY_LEN.end,
        })?;
    }

    let padding2 = buf.get_u32();
    if !VALID_PADDING_LEN.contains(&padding2) {
        return Err(InvalidPaddingLength {
            len: padding2,
            min: VALID_PADDING_LEN.start,
            max: VALID_PADDING_LEN.end,
        })?;
    }

    Ok(Preamble {
        header,
        padding1,
        body,
        padding2,
    })
}

fn encode_preamble<B: BufMut>(preamble: &Preamble, mut buf: B) -> Result<usize, EncodeError> {
    if buf.remaining_mut() < PREAMBLE_LEN {
        return Err(EncodeError::BufferTooSmall {
            req: PREAMBLE_LEN,
            rem: buf.remaining_mut(),
        });
    }

    buf.put_u16(preamble.header);
    buf.put_u32(preamble.padding1);
    buf.put_u32(preamble.body);
    buf.put_u32(preamble.padding2);

    Ok(PREAMBLE_LEN)
}

#[derive(Error, Debug)]
pub enum WalError {
    /// WAL File Parse error
    #[error("WAL File Parse error")]
    ParseError(#[from] ParseError),
    /// WAL File Encode error
    #[error("WAL File Encode error")]
    EncodeError(#[from] EncodeError),
    /// An `IO` error occurred reading from or writing to the wal
    #[error("io error")]
    IoError(#[from] std::io::Error),
    /// Rolling back the last transaction failed
    #[error("io error")]
    RollbackError(#[from] RollbackError),
}

#[derive(Error, Debug)]
pub enum RollbackError {
    #[error("io error")]
    IoError(#[from] std::io::Error),
    #[error("transaction state unclear")]
    UnclearState,
}

#[derive(Error, Debug)]
pub enum EncodeError {
    /// Buffer size insufficient
    #[error("Buffer size too small, required {req} != {rem} remaining")]
    BufferTooSmall { req: usize, rem: usize },
    /// Wal space insufficient
    #[error("Wal space insufficient, required {req} != {rem} remaining")]
    WalSpaceInsufficient { req: u64, rem: u64 },
    /// Max Wal size exceeded
    #[error("Maximum Wal size exceeded")]
    MaxWalSizeExceeded,
    /// Protobuf related parsing error
    #[error("Protobuf related parsing error")]
    ProtoError(#[from] prost::EncodeError),
}

#[derive(Error, Debug)]
pub enum ParseError {
    /// Invalid Magic Number
    #[error("Invalid Magic Number")]
    InvalidMagicNumber,
    /// Preamble error
    #[error("Preamble error")]
    PreambleError(#[from] PreambleError),
    /// Header error
    #[error("Header error")]
    HeaderError(#[from] HeaderError),
    /// Frame error
    #[error("Frame error")]
    FrameError(#[from] FrameError),
    /// Buffer size insufficient
    #[error("Buffer size too small, required {req} != {rem} remaining")]
    BufferTooSmall { req: usize, rem: usize },
    /// Protobuf related parsing error
    #[error("Protobuf related parsing error")]
    ProtoError(#[from] DecodeError),
}

#[derive(Error, Debug)]
pub enum PreambleError {
    /// Invalid Preamble Length
    #[error("Invalid Preamble Length")]
    InvalidLength,
    /// Invalid Header Length
    #[error("header length {len} needs to be in range {min}-{max}")]
    InvalidHeaderLength { len: u16, min: u16, max: u16 },
    /// Invalid Body Length
    #[error("body length {len} needs to be in range {min}-{max}")]
    InvalidBodyLength { len: u32, min: u32, max: u32 },
    /// Invalid Padding Length
    #[error("padding length {len} needs to be in range {min}-{max}")]
    InvalidPaddingLength { len: u32, min: u32, max: u32 },
}

#[derive(Error, Debug)]
pub enum HeaderError {
    /// File Id Missing or Invalid
    #[error("File Id Missing or Invalid")]
    FileIdInvalid,
    /// Vbd Id Missing or Invalid
    #[error("Vbd Id Missing or Invalid")]
    VbdIdInvalid,
    /// Created Missing or Invalid
    #[error("Creation Timestamp Missing or Invalid")]
    CreatedInvalid,
}

#[derive(Error, Debug)]
pub enum FrameError {
    /// Frame Type Invalid
    #[error("File Type missing or invalid")]
    FrameTypeInvalid,
    /// Commit Frame error
    #[error("Commit Frame error")]
    CommitFrameError(#[from] CommitFrameError),
    /// Block Frame error
    #[error("Block Frame error")]
    BlockFrameError(#[from] BlockFrameError),
    /// Cluster Frame error
    #[error("Cluster Frame error")]
    ClusterFrameError(#[from] ClusterFrameError),
    /// State Frame error
    #[error("State Frame error")]
    StateFrameError(#[from] StateFrameError),
}

#[derive(Error, Debug)]
pub enum CommitFrameError {
    /// Content Id Invalid
    #[error("Content Id Invalid")]
    ContentIdInvalid,
    /// Transaction Id Invalid
    #[error("Transaction Id Invalid")]
    TransactionIdInvalid,
    /// Timestamp Invalid
    #[error("Timestamp Invalid")]
    TimestampInvalid,
}

#[derive(Error, Debug)]
pub enum BlockFrameError {
    /// Content Id Invalid
    #[error("Content Id Invalid")]
    ContentIdInvalid,
    /// Body Length Mismatch
    #[error("body length mismatch, {exp} != {found}")]
    BodyLengthMismatch { exp: u32, found: u32 },
}

#[derive(Error, Debug)]
pub enum ClusterFrameError {
    /// Content Id Invalid
    #[error("Content Id Invalid")]
    ContentIdInvalid,
}

#[derive(Error, Debug)]
pub enum StateFrameError {
    /// Content Id Invalid
    #[error("Content Id Invalid")]
    ContentIdInvalid,
}

mod protos {
    use crate::vbd::wal::HeaderError::{CreatedInvalid, FileIdInvalid, VbdIdInvalid};
    use crate::vbd::wal::{BlockFrameError, FrameError, HeaderError, TxId, WalId};
    use crate::vbd::{ClusterId, Commit};
    use uuid::Uuid;

    include!(concat!(env!("OUT_DIR"), "/protos/wal.rs"));

    impl TryFrom<frame_header::TxBegin> for super::TxBegin {
        type Error = FrameError;

        fn try_from(value: frame_header::TxBegin) -> Result<Self, Self::Error> {
            use crate::vbd::wal::CommitFrameError::*;

            Ok(Self {
                transaction_id: value.transaction_id.ok_or(TransactionIdInvalid)?.into(),
                preceding_content_id: value
                    .preceding_content_id
                    .ok_or(ContentIdInvalid)?
                    .try_into()
                    .map_err(|_| ContentIdInvalid)?,

                created: value
                    .created
                    .ok_or(TimestampInvalid)?
                    .try_into()
                    .map_err(|_| TimestampInvalid)?,
            })
        }
    }

    impl From<&super::TxBegin> for frame_header::TxBegin {
        fn from(value: &super::TxBegin) -> Self {
            let mut begin = frame_header::TxBegin::default();
            begin.transaction_id = Some(value.transaction_id.into());
            begin.preceding_content_id = Some((&value.preceding_content_id.0).into());
            begin.created = Some(value.created.into());
            begin
        }
    }

    impl TryFrom<frame_header::TxCommit> for super::TxCommit {
        type Error = FrameError;

        fn try_from(value: frame_header::TxCommit) -> Result<Self, Self::Error> {
            use crate::vbd::wal::CommitFrameError::*;

            Ok(Self {
                transaction_id: value.transaction_id.ok_or(TransactionIdInvalid)?.into(),
                content_id: value
                    .content_id
                    .ok_or(ContentIdInvalid)?
                    .try_into()
                    .map_err(|_| ContentIdInvalid)?,
                commited: value
                    .committed
                    .ok_or(TimestampInvalid)?
                    .try_into()
                    .map_err(|_| TimestampInvalid)?,
            })
        }
    }

    impl From<&super::TxCommit> for frame_header::TxCommit {
        fn from(value: &super::TxCommit) -> Self {
            let mut commit = frame_header::TxCommit::default();
            commit.transaction_id = Some(value.transaction_id.into());
            commit.content_id = Some((&value.content_id.0).into());
            commit.committed = Some(value.commited.into());
            commit
        }
    }

    impl TryFrom<FileInfo> for super::FileHeader {
        type Error = HeaderError;

        fn try_from(value: FileInfo) -> Result<Self, Self::Error> {
            let wal_id = value.wal_file_id.map(|id| id.into()).ok_or(FileIdInvalid)?;
            let vbd_id = value.vbd_id.map(|id| id.into()).ok_or(VbdIdInvalid)?;
            let created = value
                .created
                .map(|c| c.try_into().map_err(|_| CreatedInvalid))
                .ok_or(CreatedInvalid)??;
            let preceding_wal_id = value.preceding_wal_file.map(|id| id.into());

            Ok(Self {
                wal_id,
                vbd_id,
                created,
                preceding_wal_id,
            })
        }
    }

    impl From<&super::FileHeader> for FileInfo {
        fn from(value: &super::FileHeader) -> Self {
            FileInfo {
                wal_file_id: Some(value.wal_id.into()),
                vbd_id: Some(value.vbd_id.into()),
                created: Some(value.created.into()),
                preceding_wal_file: value.preceding_wal_id.map(|i| i.into()),
            }
        }
    }

    impl From<WalId> for crate::protos::Uuid {
        fn from(value: WalId) -> Self {
            value.0.into()
        }
    }

    impl From<crate::protos::Uuid> for WalId {
        fn from(value: crate::protos::Uuid) -> Self {
            Into::<Uuid>::into(value).into()
        }
    }

    impl From<TxId> for crate::protos::Uuid {
        fn from(value: TxId) -> Self {
            value.0.into()
        }
    }

    impl From<crate::protos::Uuid> for TxId {
        fn from(value: crate::protos::Uuid) -> Self {
            Into::<Uuid>::into(value).into()
        }
    }

    impl From<&super::Block> for frame_header::Block {
        fn from(value: &super::Block) -> Self {
            Self {
                content_id: Some((&value.content_id).into()),
                length: value.length,
            }
        }
    }

    impl TryFrom<frame_header::Block> for super::Block {
        type Error = FrameError;

        fn try_from(value: frame_header::Block) -> Result<Self, Self::Error> {
            use BlockFrameError::*;

            Ok(super::Block {
                content_id: value
                    .content_id
                    .ok_or(ContentIdInvalid)?
                    .try_into()
                    .map_err(|_| ContentIdInvalid)?,
                length: value.length,
            })
        }
    }

    impl From<&ClusterId> for frame_header::Cluster {
        fn from(value: &ClusterId) -> Self {
            Self {
                content_id: Some(value.into()),
            }
        }
    }

    impl TryFrom<frame_header::Cluster> for ClusterId {
        type Error = FrameError;

        fn try_from(value: frame_header::Cluster) -> Result<Self, Self::Error> {
            use super::ClusterFrameError::*;

            Ok(value
                .content_id
                .ok_or(ContentIdInvalid)?
                .try_into()
                .map_err(|_| ContentIdInvalid)?)
        }
    }

    impl TryFrom<frame_header::State> for Commit {
        type Error = FrameError;

        fn try_from(value: frame_header::State) -> Result<Self, Self::Error> {
            use super::StateFrameError::*;

            Ok(value
                .content_id
                .ok_or(ContentIdInvalid)?
                .try_into()
                .map_err(|_| ContentIdInvalid)?)
        }
    }

    impl From<&super::super::Cluster> for Cluster {
        fn from(value: &super::super::Cluster) -> Self {
            Self {
                content_id: Some((&value.content_id).into()),
                block_ids: value.blocks.iter().map(|b| b.into()).collect::<Vec<_>>(),
            }
        }
    }
}
