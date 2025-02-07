use crate::serde::encoded::{Encodable, Encoder, EncodingSinkBuilder};
use crate::vbd::{Block, BranchName, Cluster, Commit, FixedSpecs, Index, VbdId};
use crate::wal::{
    EncodeError, FileHeader, RollbackError, TxBegin, TxCommit, TxDetailBuilder, TxDetails, TxId,
    WalError, WalId, WalSink, MAGIC_NUMBER,
};
use async_scoped::TokioScope;
use chrono::{DateTime, Utc};
use futures::{AsyncSeekExt, AsyncWriteExt, SinkExt};
use std::io::SeekFrom;
use tracing::instrument;
use uuid::Uuid;
use crate::now;

const MAX_WAL_FILE_SIZE: u64 = 1024 * 1024 * 128;

pub(super) struct WalWriterBuilder<IO> {
    io: IO,
    id: WalId,
    max_size: u64,
    specs: FixedSpecs,
    preceding_wal_id: Option<WalId>,
}

impl<IO: WalSink> WalWriterBuilder<IO> {
    pub fn max_file_size(mut self, max_size: u64) -> Self {
        assert!(max_size > 0);
        self.max_size = max_size;
        self
    }

    pub fn preceding_wal_id(mut self, wal_id: WalId) -> Self {
        self.preceding_wal_id = Some(wal_id);
        self
    }

    pub async fn build(self) -> Result<WalWriter<IO>, WalError> {
        WalWriter::new(
            self.io,
            self.id,
            self.preceding_wal_id,
            self.max_size,
            self.specs,
        )
        .await
    }
}

pub(crate) struct WalWriter<IO> {
    id: WalId,
    io: IO,
    header: FileHeader,
    max_size: u64,
}

impl<IO: WalSink> WalWriter<IO> {
    pub(super) fn builder(io: IO, id: WalId, specs: FixedSpecs) -> WalWriterBuilder<IO> {
        WalWriterBuilder {
            io,
            id,
            specs,
            max_size: MAX_WAL_FILE_SIZE,
            preceding_wal_id: None,
        }
    }

    #[instrument(skip(io))]
    async fn new(
        mut io: IO,
        wal_id: WalId,
        preceding_wal_id: Option<WalId>,
        max_size: u64,
        specs: FixedSpecs,
    ) -> Result<Self, WalError> {
        let header = FileHeader {
            wal_id,
            preceding_wal_id,
            created: now(),
            specs,
        };

        tracing::debug!("creating new wal");

        io.write_all(MAGIC_NUMBER).await?;

        let mut sink = EncodingSinkBuilder::from_writer(&mut io).build();
        sink.send((&header).into()).await?;
        sink.close().await?;

        io.flush().await?;

        tracing::info!("new wal created");

        Ok(Self {
            id: wal_id,
            io,
            header,
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

    #[instrument(skip(self), fields(wal_id = %self.id, preceding_commit = %preceding_commit.content_id()))]
    pub async fn begin(
        self,
        branch: BranchName,
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
            self.header.specs.vbd_id(),
            branch,
            preceding_commit,
            now(),
            reserve_space,
            self,
            Encoder::new(None),
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
    wal_id: WalId,
    branch: BranchName,
    initial_len: u64,
    len: u64,
    position: u64,
    writer: Option<WalWriter<IO>>,
    builder: TxDetailBuilder,
    encoder: Encoder,
}

enum Puttable<'a> {
    Block(&'a Block),
    Cluster(&'a Cluster),
    Index(&'a Index),
}

impl<'a> From<Puttable<'a>> for Encodable<'a> {
    fn from(value: Puttable<'a>) -> Self {
        match value {
            Puttable::Block(block) => Encodable::Block(block),
            Puttable::Cluster(cluster) => Encodable::Cluster(cluster),
            Puttable::Index(index) => Encodable::Index(index),
        }
    }
}

impl<'a> From<&'a Block> for Puttable<'a> {
    fn from(value: &'a Block) -> Self {
        Puttable::Block(value)
    }
}

impl<'a> From<&'a Cluster> for Puttable<'a> {
    fn from(value: &'a Cluster) -> Self {
        Puttable::Cluster(value)
    }
}

impl<'a> From<&'a Index> for Puttable<'a> {
    fn from(value: &'a Index) -> Self {
        Puttable::Index(value)
    }
}

impl<IO: WalSink> Tx<IO> {
    #[instrument(skip(writer, encoder, preceding_commit), fields(branch = %branch, preceding_commit = %preceding_commit.content_id()))]
    async fn new(
        id: TxId,
        wal_id: WalId,
        vbd_id: VbdId,
        branch: BranchName,
        preceding_commit: Commit,
        created: DateTime<Utc>,
        max_len: u64,
        mut writer: WalWriter<IO>,
        encoder: Encoder,
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
            wal_id.clone(),
            vbd_id,
            branch.clone(),
            preceding_commit.clone(),
            created.clone(),
        );

        let mut this = Self {
            wal_id,
            branch,
            initial_len,
            len: initial_len + max_len,
            position: initial_len,
            writer: Some(writer),
            builder: tx_detail_builder,
            encoder,
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

    pub fn wal_id(&self) -> WalId {
        self.wal_id
    }

    pub fn branch(&self) -> &BranchName {
        &self.branch
    }

    async fn write_tx_begin(
        &mut self,
        preceding_commit: Commit,
        created: DateTime<Utc>,
    ) -> Result<(), WalError> {
        let tx_begin = TxBegin {
            transaction_id: self.id(),
            branch: self.branch().clone(),
            preceding_commit,
            created,
        };
        self.write_frame(&tx_begin).await?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn write_frame<'a, T: Into<Encodable<'a>>>(&mut self, input: T) -> Result<u64, WalError> {
        tracing::trace!("encoding frame");
        let out = &mut self.writer.as_mut().unwrap().io;
        let pos = self.encoder.encode(input, &mut *out).await?;
        self.position = out.stream_position().await?;
        Ok(pos.offset)
    }

    pub fn remaining(&self) -> u64 {
        self.len - self.position
    }

    #[instrument(skip_all)]
    pub async fn put<'a, T: Into<Puttable<'a>>>(&mut self, value: T) -> Result<u64, WalError> {
        let value = value.into();
        Ok(match &value {
            Puttable::Block(block) => {
                if let Some(offset) = self.builder.blocks.get(block.content_id()) {
                    return Ok(*offset);
                }
                let id = block.content_id().clone();
                tracing::trace!("writing BLOCK [{}] to wal", &id);
                let pos = self.write_frame(value).await?;
                self.builder.blocks.insert(id, pos.clone());
                pos
            }
            Puttable::Cluster(cluster) => {
                if let Some(pos) = self.builder.clusters.get(cluster.content_id()) {
                    return Ok(pos.clone());
                }
                let id = cluster.content_id().clone();
                tracing::trace!("writing CLUSTER [{}] to wal", &id);
                let pos = self.write_frame(value).await?;
                self.builder.clusters.insert(id, pos.clone());
                pos
            }
            Puttable::Index(commit) => {
                if let Some(pos) = self.builder.indices.get(commit.content_id()) {
                    return Ok(pos.clone());
                }
                let id = commit.content_id().clone();
                tracing::trace!("writing COMMIT [{}] to wal", &id);
                let pos = self.write_frame(value).await?;
                self.builder.indices.insert(id, pos.clone());
                pos
            }
        })
    }

    #[instrument(skip_all, fields(tx_id = %self.builder.tx_id, wal_id = %self.builder.wal_id, vbd_id = %self.builder.vbd_id, commit = %commit.content_id()
    ))]
    pub async fn commit(
        mut self,
        commit: &Commit,
    ) -> Result<(WalWriter<IO>, TxDetails), (WalError, Result<WalWriter<IO>, RollbackError>)> {
        let tx_commit = TxCommit {
            transaction_id: self.id(),
            commit: commit.clone(),
        };
        tracing::trace!("starting commit");
        async fn write_commit<IO: WalSink>(
            tx: &mut Tx<IO>,
            tx_commit: &TxCommit,
        ) -> Result<(), WalError> {
            tx.write_frame(tx_commit).await?;
            tx.writer.as_mut().unwrap().io.set_len(tx.position).await?;
            tx.writer.as_mut().unwrap().io.flush().await?;
            Ok(())
        }
        if let Err(err) = write_commit(&mut self, &tx_commit).await {
            // error during commit, rolling back
            if let Err(re) = _rollback(&mut self.writer.as_mut().unwrap(), self.initial_len).await {
                return Err((err, Err(re)));
            }
            return Err((err, Ok(self.writer.take().unwrap())));
        }

        tracing::info!("transaction committed");

        Ok((
            self.writer.take().unwrap(),
            self.builder.clone().build(commit.clone()),
        ))
    }

    #[instrument(skip_all, fields(tx_id = %self.builder.tx_id, wal_id = %self.builder.wal_id, vbd_id = %self.builder.vbd_id))]
    pub async fn rollback(mut self) -> Result<WalWriter<IO>, RollbackError> {
        tracing::warn!("rolling back transaction");
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
