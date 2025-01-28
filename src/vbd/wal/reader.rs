use crate::hash::HashAlgorithm;
use crate::serde::encoded::{Decoded, DecodedStream};
use crate::vbd::wal::ParseError::InvalidMagicNumber;

use crate::vbd::wal::{
    FileHeader, HeaderError, ParseError, TxDetailBuilder, TxDetails, WalError, WalId, WalSource,
    MAGIC_NUMBER,
};
use crate::vbd::{
    BlockId, BlockSize, ClusterId, ClusterSize, Commit, CommitId, FixedSpecs, Position, VbdId,
};
use crate::{AsyncReadExtBuffered, WrappedReader};
use anyhow::anyhow;
use bytes::BytesMut;
use either::Either;
use futures::future::BoxFuture;
use futures::lock::OwnedMutexGuard;
use futures::{AsyncSeekExt, Stream, StreamExt};
use std::io::{ErrorKind, SeekFrom};
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::instrument;
use uuid::Uuid;

pub struct WalReader<IO> {
    io: IO,
    header: FileHeader,
    first_frame_offset: u64,
}

impl<IO: WalSource> WalReader<IO> {
    #[instrument(skip(io))]
    pub async fn new(mut io: IO) -> Result<Self, WalError> {
        let (header, pos) = read_file_header(&mut io).await?;

        Ok(Self {
            io,
            header,
            first_frame_offset: pos.offset + pos.length as u64,
        })
    }

    pub async fn transactions(
        &mut self,
        preceding_content_id: Option<CommitId>,
    ) -> Result<TxStream<&mut IO>, WalError> {
        self.io
            .seek(SeekFrom::Start(self.first_frame_offset))
            .await?;
        Ok(TxStream::new(
            &mut self.io,
            preceding_content_id,
            self.header.wal_id.clone(),
            self.header.specs.clone(),
        ))
    }

    #[instrument(skip(self))]
    async fn read(
        &mut self,
        pos: &Position<u64, u32>,
    ) -> Result<Decoded<WrappedReader<OwnedMutexGuard<&mut IO>>>, WalError> {
        tracing::trace!(offset = pos.offset, len = pos.length, "reading from WAL");
        self.io.seek(SeekFrom::Start(pos.offset)).await?;
        Ok(
            DecodedStream::from_reader(&mut self.io, self.header.specs.clone())
                .next()
                .await
                .transpose()?
                .ok_or(std::io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "Unexpected Eof",
                ))?,
        )
    }

    #[instrument(skip(self))]
    pub async fn block(
        &mut self,
        block_id: &BlockId,
        pos: &Position<u64, u32>,
    ) -> Result<crate::vbd::Block, WalError> {
        tracing::debug!("reading BLOCK from WAL");
        let mut frame = match self.read(pos).await? {
            Decoded::Block(frame) => frame,
            _ => {
                return Err(WalError::IncorrectType);
            }
        };

        if block_id != frame.header() {
            Err(anyhow!(
                "BlockIds do not match: {} != {}",
                frame.header(),
                block_id
            ))?;
        }

        Ok(frame.read_body().await?)
    }

    #[instrument(skip(self))]
    pub async fn cluster(
        &mut self,
        cluster_id: &ClusterId,
        pos: &Position<u64, u32>,
    ) -> Result<crate::vbd::Cluster, WalError> {
        tracing::debug!("reading CLUSTER from WAL");
        let mut frame = match self.read(pos).await? {
            Decoded::Cluster(frame) => frame,
            _ => {
                return Err(WalError::IncorrectType);
            }
        };

        if frame.header() != cluster_id {
            Err(anyhow!(
                "ClusterIds do not match: {} != {}",
                frame.header(),
                cluster_id
            ))?;
        }

        Ok(frame.read_body().await?)
    }

    #[instrument(skip(self))]
    pub async fn commit(
        &mut self,
        commit_id: &CommitId,
        pos: &Position<u64, u32>,
    ) -> Result<Commit, WalError> {
        tracing::debug!("reading COMMIT from WAL");
        let mut frame = match self.read(pos).await? {
            Decoded::Commit(frame) => frame,
            _ => {
                return Err(WalError::IncorrectType);
            }
        };

        if frame.header() != commit_id {
            Err(anyhow!(
                "Commit Ids do not match: {} != {}",
                frame.header(),
                commit_id
            ))?;
        }

        Ok(frame.read_body().await?)
    }
}

impl<IO> WalReader<IO> {
    pub fn header(&self) -> &FileHeader {
        &self.header
    }
}

impl<IO> AsRef<IO> for WalReader<IO> {
    fn as_ref(&self) -> &IO {
        &self.io
    }
}

pub(crate) struct TxStream<'a, IO: WalSource> {
    state: TxStreamState<'a, IO>,
    wal_id: WalId,
    specs: FixedSpecs,
}

impl<'a, IO: WalSource + 'a> TxStream<'a, IO> {
    fn new(io: IO, preceding_cid: Option<CommitId>, wal_id: WalId, specs: FixedSpecs) -> Self {
        Self {
            state: TxStreamState::New(DecodedStream::from_reader(io, specs.clone()), preceding_cid),
            wal_id,
            specs,
        }
    }

    fn next_tx_begin(
        mut stream: DecodedStream<'a, IO>,
        preceding_cid: Option<CommitId>,
        wal_id: WalId,
        vbd_id: VbdId,
    ) -> BoxFuture<
        'a,
        (
            Result<Option<TxDetailBuilder>, WalError>,
            DecodedStream<'a, IO>,
        ),
    > {
        Box::pin(async move {
            let res = match stream.next().await {
                Some(Ok(Decoded::TxBegin(tx_begin))) => match match preceding_cid {
                    Some(preceding_cid) => {
                        if &preceding_cid == &tx_begin.header().preceding_content_id {
                            Ok(tx_begin)
                        } else {
                            Err(WalError::IncorrectPrecedingCommit {
                                exp: preceding_cid,
                                found: tx_begin.header().preceding_content_id.clone(),
                            })
                        }
                    }
                    None => Ok(tx_begin),
                } {
                    Ok(tx_begin) => {
                        let tx_begin = tx_begin.into_header();
                        Ok(Some(TxDetailBuilder::new(
                            tx_begin.transaction_id,
                            wal_id,
                            vbd_id,
                            tx_begin.preceding_content_id,
                            tx_begin.created,
                        )))
                    }
                    Err(err) => Err(err),
                },
                Some(Ok(_)) => Err(WalError::UnexpectedFrameType {
                    exp: "TxBegin".to_string(),
                }),
                Some(Err(err)) => Err(err.into()),
                None => Ok(None),
            };
            (res, stream)
        })
    }
    fn next_frame(
        mut stream: DecodedStream<'a, IO>,
        mut tx_builder: TxDetailBuilder,
    ) -> BoxFuture<
        'a,
        (
            Result<Either<TxDetailBuilder, TxDetails>, WalError>,
            DecodedStream<'a, IO>,
        ),
    > {
        Box::pin(async move {
            let res = match stream.next().await {
                Some(Ok(frame)) => match frame {
                    Decoded::TxBegin(_) => Err(WalError::DanglingTxDetected {
                        tx_id: tx_builder.tx_id,
                    }),
                    Decoded::TxCommit(frame) => {
                        let tx_commit = frame.into_header();
                        if &tx_commit.transaction_id != &tx_builder.tx_id {
                            Err(WalError::IncorrectTxId {
                                exp: tx_builder.tx_id,
                                found: tx_commit.transaction_id,
                            })
                        } else {
                            Ok(Either::Right(
                                tx_builder.build(tx_commit.content_id, tx_commit.committed),
                            ))
                        }
                    }
                    Decoded::Block(frame) => {
                        if let Some(_) = frame.body() {
                            let pos = frame.position().clone();
                            tx_builder.blocks.insert(frame.into_header(), pos);
                        }
                        Ok(Either::Left(tx_builder))
                    }
                    Decoded::Cluster(frame) => {
                        if let Some(_) = frame.body() {
                            let pos = frame.position().clone();
                            tx_builder.clusters.insert(frame.into_header(), pos);
                        }
                        Ok(Either::Left(tx_builder))
                    }
                    Decoded::Commit(frame) => {
                        if let Some(_) = frame.body() {
                            let pos = frame.position().clone();
                            tx_builder.commits.insert(frame.into_header(), pos);
                        }
                        Ok(Either::Left(tx_builder))
                    }
                    _ => Err(WalError::UnexpectedFrameType {
                        exp: "TxBegin, TxCommit, Block, Cluster, Commit".to_string(),
                    }),
                },
                Some(Err(err)) => Err(err.into()),
                None => Err(WalError::DanglingTxDetected {
                    tx_id: tx_builder.tx_id,
                }),
            };
            (res, stream)
        })
    }
}

enum TxStreamState<'a, IO: WalSource> {
    New(DecodedStream<'a, IO>, Option<CommitId>),
    AwaitingNextTxBegin(
        BoxFuture<
            'a,
            (
                Result<Option<TxDetailBuilder>, WalError>,
                DecodedStream<'a, IO>,
            ),
        >,
    ),
    AwaitingNextFrame(
        BoxFuture<
            'a,
            (
                Result<Either<TxDetailBuilder, TxDetails>, WalError>,
                DecodedStream<'a, IO>,
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
                            Some(tx_details.commit_id.clone()),
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

#[instrument(skip(io))]
async fn read_file_header<IO: WalSource>(
    mut io: IO,
) -> Result<(FileHeader, Position<u64, u32>), WalError> {
    tracing::trace!("reading wal header");
    let mut buf = BytesMut::with_capacity(MAGIC_NUMBER.len());
    io.read_exact_buffered(&mut buf, MAGIC_NUMBER.len()).await?;
    if buf.as_ref() != MAGIC_NUMBER {
        return Err(InvalidMagicNumber)?;
    }

    let dummy_specs = FixedSpecs {
        vbd_id: Uuid::now_v7().into(),
        cluster_size: ClusterSize::Cs256,
        block_size: BlockSize::Bs64k,
        content_hash: HashAlgorithm::Blake3,
        meta_hash: HashAlgorithm::Blake3,
    };
    let mut stream = DecodedStream::from_reader(io, dummy_specs);
    match stream.next().await.transpose()? {
        Some(Decoded::WalInfo(frame)) => Ok((frame.header().clone(), frame.position().clone())),
        Some(_) | None => Err(ParseError::HeaderError(HeaderError::FileIdInvalid))?,
    }
}
