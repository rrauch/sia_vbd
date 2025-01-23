use crate::vbd::wal::{
    protos, Block, EncodeError, FileHeader, Preamble, RollbackError, TxBegin, TxCommit,
    TxDetailBuilder, TxDetails, TxId, WalError, WalId, WalSink, MAGIC_NUMBER, PREAMBLE_LEN,
    VALID_HEADER_LEN,
};
use crate::vbd::{BlockId, ClusterId, Commit, FixedSpecs, Position, VbdId};
use crate::AsyncWriteBytesExt;
use async_scoped::TokioScope;
use bytes::{BufMut, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use futures::{AsyncSeekExt, AsyncWriteExt};
use prost::Message;
use std::fmt::{Display, Formatter};
use std::io::SeekFrom;
use tracing::instrument;
use uuid::Uuid;

pub(crate) struct WalWriter<IO> {
    id: WalId,
    io: IO,
    offset: usize,
    header: FileHeader,
    max_size: u64,
}

impl<IO: WalSink> WalWriter<IO> {
    #[instrument(skip(io))]
    pub async fn new(
        mut io: IO,
        wal_id: WalId,
        preceding_wal_id: Option<WalId>,
        max_size: u64,
        specs: FixedSpecs,
    ) -> Result<Self, WalError> {
        let header = FileHeader {
            wal_id,
            preceding_wal_id,
            created: Utc::now(),
            specs,
        };

        tracing::debug!("creating new wal");

        let mut offset = 0usize;
        io.write_all(MAGIC_NUMBER).await?;
        offset += MAGIC_NUMBER.len();

        let fi = Into::<protos::FileInfo>::into(&header);
        let mut buf = BytesMut::with_capacity(PREAMBLE_LEN + fi.encoded_len());

        offset += encode_preamble(&Preamble::header_only(fi.encoded_len() as u16), &mut buf)?;

        fi.encode(&mut buf)
            .map_err(|e| Into::<EncodeError>::into(e))?;
        offset += fi.encoded_len();

        io.write_all(&buf).await?;
        io.flush().await?;

        tracing::info!("new wal created");

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

    #[instrument(skip(self), fields(wal_id = %self.id, preceding_commit = %preceding_commit))]
    pub async fn begin(
        self,
        preceding_commit: Commit,
        reserve_space: u64,
    ) -> Result<Tx<IO>, (WalError, Result<Self, RollbackError>)> {
        tracing::debug!("starting new transaction");
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
            tracing::error!(error = %err, "starting new transaction failed");
            return Err((err, Ok(self)));
        }

        let tx = Tx::new(
            Uuid::now_v7().into(),
            self.header.wal_id,
            self.header.specs.vbd_id,
            preceding_commit,
            Utc::now(),
            reserve_space,
            self,
        )
        .await?;
        tracing::info!(id = %tx.id(), "new transaction started");
        Ok(tx)
    }
}

impl<IO> AsRef<IO> for WalWriter<IO> {
    fn as_ref(&self) -> &IO {
        &self.io
    }
}

pub struct Tx<IO: WalSink> {
    initial_len: u64,
    len: u64,
    position: u64,
    writer: Option<WalWriter<IO>>,
    buf: BytesMut,
    builder: TxDetailBuilder,
}

impl<IO: WalSink> AsRef<IO> for Tx<IO> {
    fn as_ref(&self) -> &IO {
        self.writer
            .as_ref()
            .map(|w| &w.io)
            .expect("writer to be set")
    }
}

impl<IO: WalSink> Tx<IO> {
    #[instrument(skip(writer))]
    async fn new(
        id: TxId,
        wal_id: WalId,
        vbd_id: VbdId,
        preceding_commit: Commit,
        created: DateTime<Utc>,
        max_len: u64,
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
        tracing::debug!("preparing wal");
        let initial_len = match prepare_wal(&mut writer, max_len).await {
            Ok(l) => l,
            Err(e) => {
                return Err((e, Err(RollbackError::UnclearState)));
            }
        };

        let tx_detail_builder = TxDetailBuilder::new(
            id,
            wal_id,
            vbd_id,
            preceding_commit.clone(),
            created.clone(),
        );

        let mut this = Self {
            initial_len,
            len: initial_len + max_len,
            position: initial_len,
            writer: Some(writer),
            buf: BytesMut::with_capacity(VALID_HEADER_LEN.end as usize + PREAMBLE_LEN),
            builder: tx_detail_builder,
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
        tracing::debug!("new transaction started");
        Ok(this)
    }

    pub fn id(&self) -> TxId {
        self.builder.tx_id
    }

    async fn write_tx_begin(
        &mut self,
        preceding_commit: Commit,
        created: DateTime<Utc>,
    ) -> Result<(), WalError> {
        let frame = WriteFrame::TxBegin(TxBegin {
            transaction_id: self.id(),
            preceding_content_id: preceding_commit,
            created,
        });
        self.write_frame(frame).await?;
        Ok(())
    }

    #[instrument(skip(self), fields(frame = %frame))]
    async fn write_frame(&mut self, frame: WriteFrame<'_>) -> Result<Position<u64, u16>, WalError> {
        let buf = &mut self.buf;
        buf.clear();
        tracing::trace!("encoding frame");
        let header_len = encode_frame(&mut *buf, frame)? - PREAMBLE_LEN;
        let remaining = self.len - self.position;
        if remaining < buf.len() as u64 {
            return Err(EncodeError::WalSpaceInsufficient {
                req: buf.len() as u64,
                rem: remaining,
            })?;
        }
        tracing::trace!(
            offset = self.position,
            encoded_length = buf.len(),
            "writing frame"
        );
        self.writer.as_mut().unwrap().io.write_all(&buf).await?;
        let header_pos = Position::new(self.position + PREAMBLE_LEN as u64, header_len as u16);
        self.position += buf.len() as u64;
        Ok(header_pos)
    }

    #[instrument(skip_all)]
    async fn write_body(
        &mut self,
        body: impl AsRef<[u8]>,
        padding1: u32,
        padding2: u32,
    ) -> Result<Position<u64, u32>, WalError> {
        let body = body.as_ref();
        let total_len = body.len() as u64 + padding1 as u64 + padding2 as u64;
        tracing::trace!(offset = self.position, length = total_len, "writing body");
        let remaining = self.remaining();
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

    pub fn remaining(&self) -> u64 {
        self.len - self.position
    }

    #[instrument(skip_all, fields(block_id = %block_id, data_len = data.len()))]
    pub async fn block(
        &mut self,
        block_id: &BlockId,
        data: &Bytes,
    ) -> Result<Position<u64, u32>, WalError> {
        if let Some(pos) = self.builder.blocks.get(block_id) {
            return Ok(pos.clone());
        }
        tracing::debug!("writing BLOCK to wal");
        let block = Block {
            content_id: block_id.clone(),
            length: data.len() as u32,
        };
        let frame = WriteFrame::Block((block, None, None));
        self.write_frame(frame).await?;
        let pos = self.write_body(data, 0, 0).await?;
        self.builder.blocks.insert(block_id.clone(), pos.clone());
        Ok(pos)
    }

    #[instrument(skip_all, fields(cluster_id = %cluster.content_id))]
    pub async fn cluster(
        &mut self,
        cluster: &crate::vbd::Cluster,
    ) -> Result<Position<u64, u32>, WalError> {
        if let Some(pos) = self.builder.clusters.get(cluster.content_id()) {
            return Ok(pos.clone());
        }
        tracing::debug!("writing CLUSTER to wal");
        let cluster_id = cluster.content_id.clone();
        let cluster = cluster.into();
        let frame = WriteFrame::Cluster((&cluster, &cluster_id));
        self.write_frame(frame).await?;
        let pos = self.write_body(cluster.encode_to_vec(), 0, 0).await?;
        self.builder.clusters.insert(cluster_id, pos.clone());
        Ok(pos)
    }

    #[instrument(skip_all, fields(content_id = %content_id))]
    pub async fn state(
        &mut self,
        content_id: &Commit,
        cluster_ids: impl Iterator<Item = &ClusterId>,
    ) -> Result<Position<u64, u32>, WalError> {
        if let Some(pos) = self.builder.states.get(content_id) {
            return Ok(pos.clone());
        }
        tracing::debug!("writing STATE to wal");
        let commit = content_id.clone();
        let state = protos::State {
            content_id: Some(content_id.into()),
            cluster_ids: cluster_ids.into_iter().map(|c| c.into()).collect(),
        };
        let frame = WriteFrame::State((&state, &commit));
        self.write_frame(frame).await?;
        let pos = self.write_body(state.encode_to_vec(), 0, 0).await?;
        self.builder.states.insert(commit, pos.clone());
        Ok(pos)
    }

    #[instrument(skip_all, fields(tx_id = %self.builder.tx_id, wal_id = %self.builder.wal_id, vbd_id = %self.builder.vbd_id, content_id = %content_id
    ))]
    pub async fn commit(
        mut self,
        content_id: &Commit,
    ) -> Result<(WalWriter<IO>, TxDetails), (WalError, Result<WalWriter<IO>, RollbackError>)> {
        let committed = Utc::now();
        let frame = WriteFrame::TxCommit(TxCommit {
            transaction_id: self.id(),
            content_id: content_id.clone(),
            committed: committed.clone(),
        });
        tracing::trace!("starting commit");
        async fn write_commit<IO: WalSink>(
            tx: &mut Tx<IO>,
            frame: WriteFrame<'_>,
        ) -> Result<(), WalError> {
            tx.write_frame(frame).await?;
            tx.writer.as_mut().unwrap().io.set_len(tx.position).await?;
            tx.writer.as_mut().unwrap().io.flush().await?;
            Ok(())
        }
        if let Err(err) = write_commit(&mut self, frame).await {
            // error during commit, rolling back
            if let Err(re) = _rollback(&mut self.writer.as_mut().unwrap(), self.initial_len).await {
                return Err((err, Err(re)));
            }
            return Err((err, Ok(self.writer.take().unwrap())));
        }

        tracing::info!("transaction committed");

        Ok((
            self.writer.take().unwrap(),
            self.builder.clone().build(content_id.clone(), committed),
        ))
    }

    #[instrument(skip_all, fields(tx_id = %self.builder.tx_id, wal_id = %self.builder.wal_id, vbd_id = %self.builder.vbd_id))]
    pub async fn rollback(mut self) -> Result<WalWriter<IO>, RollbackError> {
        tracing::info!("rolling back transaction");
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
    #[instrument(skip_all, fields(tx_id = %self.builder.tx_id, wal_id = %self.builder.wal_id, vbd_id = %self.builder.vbd_id))]
    fn drop(&mut self) {
        if let Some(mut wal) = self.writer.take() {
            tracing::warn!("transaction dropped, rolling back");
            TokioScope::scope_and_block(|s| {
                s.spawn(async move {
                    if let Err(err) = _rollback(&mut wal, self.initial_len).await {
                        tracing::error!(error = %err, "rollback failure");
                    }
                });
            });
        }
    }
}

fn encode_frame<B: BufMut>(mut buf: B, frame: WriteFrame) -> Result<usize, EncodeError> {
    let (header, preamble) = {
        match frame {
            WriteFrame::TxBegin(begin) => {
                let header = protos::FrameHeader {
                    r#type: Some(protos::frame_header::Type::TxBegin((&begin).into())),
                };
                let preamble = Preamble::header_only(header.encoded_len() as u16);
                (header, preamble)
            }
            WriteFrame::TxCommit(commit) => {
                let header = protos::FrameHeader {
                    r#type: Some(protos::frame_header::Type::TxCommit((&commit).into())),
                };
                let preamble = Preamble::header_only(header.encoded_len() as u16);
                (header, preamble)
            }
            WriteFrame::Block((block, padding1, padding2)) => {
                let header = protos::FrameHeader {
                    r#type: Some(protos::frame_header::Type::Block((&block).into())),
                };
                let preamble = Preamble::full(
                    header.encoded_len() as u16,
                    padding1,
                    Some(block.length),
                    padding2,
                );
                (header, preamble)
            }
            WriteFrame::Cluster((cluster, _)) => {
                let header = protos::FrameHeader {
                    r#type: Some(protos::frame_header::Type::Cluster(
                        protos::frame_header::Cluster {
                            content_id: cluster.content_id.clone(),
                        },
                    )),
                };
                let preamble = Preamble::full(
                    header.encoded_len() as u16,
                    None,
                    cluster.encoded_len() as u32,
                    None,
                );
                (header, preamble)
            }
            WriteFrame::State((state, _)) => {
                let header = protos::FrameHeader {
                    r#type: Some(protos::frame_header::Type::State(
                        protos::frame_header::State {
                            content_id: state.content_id.clone(),
                        },
                    )),
                };
                let preamble = Preamble::full(
                    header.encoded_len() as u16,
                    None,
                    state.encoded_len() as u32,
                    None,
                );
                (header, preamble)
            }
        }
    };
    let mut len = encode_preamble(&preamble, &mut buf)?;
    header.encode(&mut buf)?;
    len += header.encoded_len();
    Ok(len)
}

enum WriteFrame<'a> {
    TxBegin(TxBegin),
    TxCommit(TxCommit),
    Block((Block, Option<u32>, Option<u32>)),
    Cluster((&'a protos::Cluster, &'a ClusterId)),
    State((&'a protos::State, &'a Commit)),
}

impl<'a> Display for WriteFrame<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TxBegin(tx_begin) => {
                write!(
                    f,
                    "TxBegin[id={}, preceding_content_id={}, created={}]",
                    tx_begin.transaction_id, tx_begin.preceding_content_id, tx_begin.created
                )
            }
            Self::TxCommit(tx_commit) => {
                write!(
                    f,
                    "TxCommit[id={}, content_id={}, committed={}]",
                    tx_commit.transaction_id, tx_commit.content_id, tx_commit.committed
                )
            }
            Self::Block((block, _, _)) => {
                write!(f, "Block[id={}, length={}]", block.content_id, block.length)
            }
            Self::Cluster((_, id)) => {
                write!(f, "Cluster[content_id={}]", id)
            }
            Self::State((_, id)) => {
                write!(f, "State[content_id={}]", id)
            }
        }
    }
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
