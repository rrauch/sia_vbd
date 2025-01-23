use crate::vbd::wal::protos::Cluster;
use crate::vbd::wal::ParseError::InvalidMagicNumber;
use crate::vbd::wal::PreambleError::{
    InvalidBodyLength, InvalidHeaderLength, InvalidLength, InvalidPaddingLength,
};
use crate::vbd::wal::{
    protos, Block, BlockFrameError, FileHeader, FrameError, ParseError, Preamble, TxBegin,
    TxCommit, TxDetailBuilder, TxDetails, WalError, WalId, WalSource, MAGIC_NUMBER, PREAMBLE_LEN,
    VALID_BODY_LEN, VALID_HEADER_LEN, VALID_PADDING_LEN,
};
use crate::vbd::{BlockError, BlockId, ClusterId, ClusterMut, Commit, FixedSpecs, Position, VbdId};
use crate::AsyncReadExtBuffered;
use anyhow::anyhow;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use either::Either;
use futures::future::BoxFuture;
use futures::{AsyncSeekExt, Stream, StreamExt};
use prost::Message;
use std::fmt::{Display, Formatter};
use std::io::SeekFrom;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::instrument;

pub struct WalReader<IO> {
    io: IO,
    header: FileHeader,
    first_frame_offset: u64,
}

impl<IO: WalSource> WalReader<IO> {
    #[instrument(skip(io))]
    pub async fn new(mut io: IO) -> Result<Self, WalError> {
        let header = read_file_header(&mut io).await?;

        Ok(Self {
            io,
            header: header.header,
            first_frame_offset: header.next_frame_offset,
        })
    }

    pub fn transactions(&mut self, preceding_content_id: Option<Commit>) -> TxStream<&mut IO> {
        TxStream::new(
            &mut self.io,
            self.first_frame_offset,
            preceding_content_id,
            self.header.wal_id.clone(),
            &self.header.specs,
        )
    }

    #[instrument(skip(self))]
    async fn read(&mut self, pos: &Position<u64, u32>) -> Result<Bytes, std::io::Error> {
        tracing::trace!(offset = pos.offset, len = pos.length, "reading from WAL");
        self.io.seek(SeekFrom::Start(pos.offset)).await?;
        let mut buf = BytesMut::with_capacity(pos.length as usize);
        self.io
            .read_exact_buffered(&mut buf, pos.length as usize)
            .await?;
        Ok(buf.freeze())
    }

    #[instrument(skip(self))]
    pub async fn block(
        &mut self,
        block_id: &BlockId,
        pos: &Position<u64, u32>,
    ) -> Result<crate::vbd::Block, WalError> {
        tracing::debug!("reading BLOCK from WAL");
        if pos.length as usize != *self.header.specs.block_size {
            Err(anyhow!(
                "Incorrect block size, found [{}] but expected [{}]",
                pos.length,
                *self.header.specs.block_size
            ))?;
        }

        let specs = self.header.specs.clone();
        let block = crate::vbd::Block::new(&specs, self.read(pos).await?)
            .map_err(|e| <BlockError as Into<anyhow::Error>>::into(e))?;
        if &block.content_id != block_id {
            Err(anyhow!(
                "BlockIds do not match: {} != {}",
                block.content_id,
                block_id
            ))?;
        }
        Ok(block)
    }

    #[instrument(skip(self))]
    pub async fn cluster(
        &mut self,
        cluster_id: &ClusterId,
        pos: &Position<u64, u32>,
    ) -> Result<crate::vbd::Cluster, WalError> {
        tracing::debug!("reading CLUSTER from WAL");
        let proto_cluster = Cluster::decode(self.read(pos).await?)
            .map_err(|p| WalError::ParseError(ParseError::ProtoError(p)))?;

        if proto_cluster.block_ids.len() != *self.header.specs.cluster_size {
            Err(anyhow!(
                "Incorrect cluster size, found [{}] but expected [{}]",
                proto_cluster.block_ids.len(),
                *self.header.specs.cluster_size
            ))?;
        }

        let block_ids = proto_cluster
            .block_ids
            .into_iter()
            .map(|h| <crate::protos::Hash as TryInto<BlockId>>::try_into(h))
            .collect::<Result<Vec<_>, _>>()?;
        let cluster =
            ClusterMut::from_block_ids(block_ids.into_iter(), self.header.specs.clone()).finalize();
        if &cluster.content_id != cluster_id {
            Err(anyhow!(
                "ClusterIds do not match: {} != {}",
                cluster.content_id,
                cluster_id
            ))?;
        }
        Ok(cluster)
    }

    #[instrument(skip(self))]
    pub async fn state(
        &mut self,
        commit: &Commit,
        pos: &Position<u64, u32>,
    ) -> Result<Vec<ClusterId>, WalError> {
        tracing::debug!("reading STATE from WAL");
        let state = protos::State::decode(self.read(pos).await?)
            .map_err(|p| WalError::ParseError(ParseError::ProtoError(p)))?;

        let cluster_ids = state
            .cluster_ids
            .into_iter()
            .map(|h| <crate::protos::Hash as TryInto<ClusterId>>::try_into(h))
            .collect::<Result<Vec<_>, _>>()?;

        let state_commit: Commit = (&cluster_ids, &self.header.specs).into();

        if &state_commit != commit {
            Err(anyhow!(
                "Commits do not match: {} != {}",
                state_commit,
                commit
            ))?;
        }

        Ok(cluster_ids)
    }
}

impl<IO> WalReader<IO> {
    pub fn header(&self) -> &FileHeader {
        &self.header
    }
}

pub struct TxStream<'a, IO: WalSource> {
    state: TxStreamState<'a, IO>,
    wal_id: WalId,
    specs: &'a FixedSpecs,
}

impl<'a, IO: WalSource + 'a> TxStream<'a, IO> {
    fn new(
        io: IO,
        first_frame_offset: u64,
        preceding_cid: Option<Commit>,
        wal_id: WalId,
        specs: &'a FixedSpecs,
    ) -> Self {
        Self {
            state: TxStreamState::New(FrameStream::new(io, first_frame_offset), preceding_cid),
            wal_id,
            specs,
        }
    }

    fn next_tx_begin(
        mut frame_stream: FrameStream<'a, IO>,
        preceding_cid: Option<Commit>,
        wal_id: WalId,
        vbd_id: VbdId,
    ) -> BoxFuture<
        'a,
        (
            Result<Option<TxDetailBuilder>, WalError>,
            FrameStream<'a, IO>,
        ),
    > {
        Box::pin(async move {
            let res = match frame_stream.next().await {
                Some(Ok(ReadFrameItem::TxBegin(tx_begin))) => match match preceding_cid {
                    Some(preceding_cid) => {
                        if &preceding_cid == &tx_begin.header.preceding_content_id {
                            Ok(tx_begin)
                        } else {
                            Err(WalError::IncorrectPrecedingCommit {
                                exp: preceding_cid,
                                found: tx_begin.header.preceding_content_id.clone(),
                            })
                        }
                    }
                    None => Ok(tx_begin),
                } {
                    Ok(tx_begin) => Ok(Some(TxDetailBuilder::new(
                        tx_begin.header.transaction_id,
                        wal_id,
                        vbd_id,
                        tx_begin.header.preceding_content_id,
                        tx_begin.header.created,
                    ))),
                    Err(err) => Err(err),
                },
                Some(Ok(_)) => Err(WalError::UnexpectedFrameType {
                    exp: "TxBegin".to_string(),
                }),
                Some(Err(err)) => Err(err),
                None => Ok(None),
            };
            (res, frame_stream)
        })
    }
    fn next_frame(
        mut frame_stream: FrameStream<'a, IO>,
        mut tx_builder: TxDetailBuilder,
    ) -> BoxFuture<
        'a,
        (
            Result<Either<TxDetailBuilder, TxDetails>, WalError>,
            FrameStream<'a, IO>,
        ),
    > {
        Box::pin(async move {
            let res = match frame_stream.next().await {
                Some(Ok(frame)) => match frame {
                    ReadFrameItem::TxBegin(_) => Err(WalError::DanglingTxDetected {
                        tx_id: tx_builder.tx_id,
                    }),
                    ReadFrameItem::TxCommit(tx_commit) => {
                        if &tx_commit.header.transaction_id != &tx_builder.tx_id {
                            Err(WalError::IncorrectTxId {
                                exp: tx_builder.tx_id,
                                found: tx_commit.header.transaction_id,
                            })
                        } else {
                            Ok(Either::Right(tx_builder.build(
                                tx_commit.header.content_id,
                                tx_commit.header.committed,
                            )))
                        }
                    }
                    ReadFrameItem::Block(block) => {
                        if let Some(body) = block.body {
                            tx_builder.blocks.insert(block.header.content_id, body);
                        }
                        Ok(Either::Left(tx_builder))
                    }
                    ReadFrameItem::Cluster(cluster) => {
                        if let Some(body) = cluster.body {
                            tx_builder.clusters.insert(cluster.header, body);
                        }
                        Ok(Either::Left(tx_builder))
                    }
                    ReadFrameItem::State(state) => {
                        if let Some(body) = state.body {
                            tx_builder.states.insert(state.header, body);
                        }
                        Ok(Either::Left(tx_builder))
                    }
                },
                Some(Err(err)) => Err(err),
                None => Err(WalError::DanglingTxDetected {
                    tx_id: tx_builder.tx_id,
                }),
            };
            (res, frame_stream)
        })
    }
}

enum TxStreamState<'a, IO: WalSource> {
    New(FrameStream<'a, IO>, Option<Commit>),
    AwaitingNextTxBegin(
        BoxFuture<
            'a,
            (
                Result<Option<TxDetailBuilder>, WalError>,
                FrameStream<'a, IO>,
            ),
        >,
    ),
    AwaitingNextFrame(
        BoxFuture<
            'a,
            (
                Result<Either<TxDetailBuilder, TxDetails>, WalError>,
                FrameStream<'a, IO>,
            ),
        >,
    ),
    Done,
}

impl<'a, IO: WalSource + 'a> Stream for TxStream<'a, IO> {
    type Item = Result<TxDetails, WalError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match mem::replace(&mut self.state, TxStreamState::Done) {
                TxStreamState::New(frame_stream, preceding_cid) => {
                    self.state = TxStreamState::AwaitingNextTxBegin(Self::next_tx_begin(
                        frame_stream,
                        preceding_cid,
                        self.wal_id.clone(),
                        self.specs.vbd_id.clone(),
                    ));
                    continue;
                }
                TxStreamState::AwaitingNextTxBegin(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.state = TxStreamState::AwaitingNextTxBegin(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready((Ok(Some(tx_builder)), frame_stream)) => {
                        self.state = TxStreamState::AwaitingNextFrame(Self::next_frame(
                            frame_stream,
                            tx_builder,
                        ));
                        continue;
                    }
                    Poll::Ready((Ok(None), _)) => return Poll::Ready(None),
                    Poll::Ready((Err(err), _)) => return Poll::Ready(Some(Err(err))),
                },
                TxStreamState::AwaitingNextFrame(mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.state = TxStreamState::AwaitingNextFrame(fut);
                        return Poll::Pending;
                    }
                    Poll::Ready((Ok(Either::Left(tx_builder)), frame_stream)) => {
                        self.state = TxStreamState::AwaitingNextFrame(Self::next_frame(
                            frame_stream,
                            tx_builder,
                        ));
                        continue;
                    }
                    Poll::Ready((Ok(Either::Right(tx_details)), frame_stream)) => {
                        self.state = TxStreamState::AwaitingNextTxBegin(Self::next_tx_begin(
                            frame_stream,
                            Some(tx_details.commit.clone()),
                            self.wal_id.clone(),
                            self.specs.vbd_id.clone(),
                        ));
                        return Poll::Ready(Some(Ok(tx_details)));
                    }
                    Poll::Ready((Err(err), _)) => return Poll::Ready(Some(Err(err))),
                },
                TxStreamState::Done => {
                    panic!("polled after completion");
                }
            }
        }
    }
}

struct FrameStream<'a, IO: WalSource> {
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

#[instrument(skip(io))]
async fn read_file_header<IO: WalSource>(mut io: IO) -> Result<ReadFrame<FileHeader>, WalError> {
    tracing::trace!("reading wal header");
    let start_offset = io.stream_position().await?;
    let mut buf = BytesMut::with_capacity(VALID_HEADER_LEN.end as usize);
    io.read_exact_buffered(&mut buf, MAGIC_NUMBER.len() + PREAMBLE_LEN)
        .await?;
    let (header_pos, next_frame_at) = parse_file_preamble(&mut buf, start_offset)?;
    buf.clear();
    io.seek(SeekFrom::Start(header_pos.offset)).await?;
    io.read_exact_buffered(&mut buf, header_pos.length as usize)
        .await?;
    let header = parse_file_header(buf)?;
    tracing::debug!(header = ?header,  "wal header read");
    Ok(ReadFrame {
        header,
        body: None,
        next_frame_offset: next_frame_at,
    })
}

fn parse_file_header<B: Buf>(buf: B) -> Result<FileHeader, ParseError> {
    Ok(protos::FileInfo::decode(buf)?.try_into()?)
}

#[instrument(skip_all)]
async fn read_frame<IO: WalSource, B: BufMut + Buf>(
    mut io: IO,
    mut buf: B,
) -> Result<ReadFrameItem, WalError> {
    let mut offset = io.stream_position().await?;
    tracing::trace!(offset = offset, "reading frame");
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
    tracing::trace!(header_len = header_len, body = ?body, "preamble read");

    if buf.remaining_mut() < header_len {
        return Err(ParseError::BufferTooSmall {
            req: header_len,
            rem: buf.remaining_mut(),
        })?;
    }
    io.read_exact_buffered(&mut buf, header_len).await?;
    Ok(decode_frame(
        &mut buf,
        body,
        offset + preamble.content_len(),
    )?)
}

#[instrument(skip(buf))]
fn decode_frame<B: Buf>(
    buf: &mut B,
    body: Option<Position<u64, u32>>,
    next_frame_at: u64,
) -> Result<ReadFrameItem, ParseError> {
    tracing::trace!("decoding frame");
    let proto_frame_header = protos::FrameHeader::decode(buf)?;
    let res = match proto_frame_header.r#type {
        Some(protos::frame_header::Type::TxBegin(begin)) => Ok(ReadFrameItem::TxBegin(ReadFrame {
            header: begin.try_into()?,
            body,
            next_frame_offset: next_frame_at,
        })),
        Some(protos::frame_header::Type::TxCommit(commit)) => {
            Ok(ReadFrameItem::TxCommit(ReadFrame {
                header: commit.try_into()?,
                body,
                next_frame_offset: next_frame_at,
            }))
        }
        Some(protos::frame_header::Type::Block(block)) => {
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
        Some(protos::frame_header::Type::Cluster(cluster)) => {
            Ok(ReadFrameItem::Cluster(ReadFrame {
                header: cluster.try_into()?,
                body,
                next_frame_offset: next_frame_at,
            }))
        }
        Some(protos::frame_header::Type::State(state)) => Ok(ReadFrameItem::State(ReadFrame {
            header: state.try_into()?,
            body,
            next_frame_offset: next_frame_at,
        })),
        None => Err(FrameError::FrameTypeInvalid)?,
    };
    match &res {
        Ok(res) => {
            tracing::trace!(frame = %res, "frame decoded");
        }
        Err(err) => tracing::error!(error = %err, "error decoding frame"),
    }
    res
}

enum ReadFrameItem {
    TxBegin(ReadFrame<TxBegin>),
    TxCommit(ReadFrame<TxCommit>),
    Block(ReadFrame<Block>),
    Cluster(ReadFrame<ClusterId>),
    State(ReadFrame<Commit>),
}

impl Display for ReadFrameItem {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::TxBegin(frame) => {
                write!(
                    f,
                    "TxBegin[id={}, preceding_content_id={}, created={}]",
                    frame.header.transaction_id,
                    frame.header.preceding_content_id,
                    frame.header.created
                )
            }
            Self::TxCommit(frame) => {
                write!(
                    f,
                    "TxCommit[id={}, content_id={}, committed={}]",
                    frame.header.transaction_id, frame.header.content_id, frame.header.committed
                )
            }
            Self::Block(frame) => {
                write!(
                    f,
                    "Block[id={}, length={}]",
                    frame.header.content_id, frame.header.length
                )
            }
            Self::Cluster(frame) => {
                write!(f, "Cluster[content_id={}]", frame.header)
            }
            Self::State(frame) => {
                write!(f, "State[content_id={}]", frame.header)
            }
        }
    }
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

pub struct ReadFrame<T> {
    header: T,
    body: Option<Position<u64, u32>>,
    next_frame_offset: u64,
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
