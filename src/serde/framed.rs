use crate::serde::PREAMBLE_LEN;
use crate::vbd::Position;
use crate::io::{AsyncReadExtBuffered, LimitedReader, WrappedReader};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use crc::Crc;
use futures::future::BoxFuture;
use futures::lock::{Mutex, OwnedMutexGuard};
use futures::ready;
use futures::{AsyncRead, AsyncSeek, AsyncSeekExt, AsyncWrite, Sink, Stream};
use pin_project_lite::pin_project;
use std::io::{ErrorKind, Result, SeekFrom};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::{io, mem};

pub(super) const MAX_HEADER_LEN: u16 = 1024 * 4;
const MAX_PAYLOAD_LEN: u32 = 1024 * 1024 * 16;

pub(crate) trait InnerReader: AsyncRead + AsyncSeek + Unpin + Send {}
impl<T: AsyncRead + AsyncSeek + Unpin + Send> InnerReader for T {}

pub(crate) struct FramedStream<'a, T> {
    state: StreamState<'a, T>,
}

enum StreamState<'a, T> {
    New(T),
    AwaitingStreamPosition(BytesMut, BoxFuture<'a, Result<(u64, T)>>),
    ReadingPreamble(u64, BoxFuture<'a, Result<Option<(u32, u16, T, BytesMut)>>>),
    Seeking(u64, T, BytesMut),
    Waiting(u64, BytesMut, BoxFuture<'a, T>),
    Done,
}

impl<'a, T: InnerReader + 'a> FramedStream<'a, T> {
    pub fn new(reader: T) -> Self {
        Self {
            state: StreamState::New(reader),
        }
    }

    fn read_preamble(
        mut reader: T,
        mut buf: BytesMut,
    ) -> BoxFuture<'a, Result<Option<(u32, u16, T, BytesMut)>>> {
        Box::pin(async move {
            buf.clear();
            match reader.read_exact_buffered(&mut buf, PREAMBLE_LEN).await {
                Ok(_) => {
                    let calculated_crc = crc(&buf[..6]);
                    let payload_len = buf.get_u32();
                    let header_len = buf.get_u16();
                    let crc = buf.get_u16();

                    if crc != calculated_crc {
                        return Err(std::io::Error::new(
                            ErrorKind::InvalidData,
                            "crc checksum mismatch",
                        ));
                    }

                    if header_len > MAX_HEADER_LEN {
                        return Err(std::io::Error::new(
                            ErrorKind::InvalidData,
                            format!(
                                "header length [{}] exceeds maximum [{}]",
                                header_len, MAX_HEADER_LEN
                            ),
                        ));
                    }

                    if payload_len > MAX_PAYLOAD_LEN {
                        return Err(std::io::Error::new(
                            ErrorKind::InvalidData,
                            format!(
                                "payload length [{}] exceeds maximum [{}]",
                                payload_len, MAX_PAYLOAD_LEN
                            ),
                        ));
                    }

                    Ok(Some((payload_len, header_len, reader, buf)))
                }
                Err(err) => {
                    if err.kind() == ErrorKind::UnexpectedEof {
                        if buf.is_empty() {
                            return Ok(None);
                        }
                    }
                    Err(err)
                }
            }
        })
    }

    fn reader_position(mut reader: T) -> BoxFuture<'a, Result<(u64, T)>> {
        Box::pin(async move {
            let pos = reader.stream_position().await?;
            Ok((pos, reader))
        })
    }

    fn await_reader(locked_reader: Arc<Mutex<T>>) -> BoxFuture<'a, T> {
        Box::pin(async move {
            let _ = locked_reader.lock().await;
            Arc::into_inner(locked_reader)
                .expect("exclusive locked_reader")
                .into_inner()
        })
    }
}

impl<'a, T: InnerReader + 'a> Stream for FramedStream<'a, T> {
    type Item = Result<ReadFrame<WrappedReader<OwnedMutexGuard<T>>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match mem::replace(&mut self.state, StreamState::Done) {
                StreamState::New(reader) => {
                    let fut = Self::reader_position(reader);
                    self.state =
                        StreamState::AwaitingStreamPosition(BytesMut::with_capacity(6), fut);
                    continue;
                }
                StreamState::AwaitingStreamPosition(buf, mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.state = StreamState::AwaitingStreamPosition(buf, fut);
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok((pos, reader))) => {
                        self.state =
                            StreamState::ReadingPreamble(pos, Self::read_preamble(reader, buf));
                        continue;
                    }
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Some(Err(err)));
                    }
                },
                StreamState::ReadingPreamble(pos, mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.state = StreamState::ReadingPreamble(pos, fut);
                        return Poll::Pending;
                    }
                    Poll::Ready(Ok(None)) => {
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Ok(Some((payload_len, header_len, reader, buf)))) => {
                        let payload_pos = pos + PREAMBLE_LEN as u64;
                        let header = Position {
                            offset: payload_pos,
                            length: header_len,
                        };
                        let next_frame_at = payload_pos + payload_len as u64;
                        let body_start = header.offset + header_len as u64;
                        let body_len = (next_frame_at - body_start) as u32;
                        let body = if body_len > 0 {
                            Some(Position {
                                offset: body_start,
                                length: body_len,
                            })
                        } else {
                            None
                        };
                        let reader = Arc::new(Mutex::new(reader));
                        let lock = reader
                            .clone()
                            .try_lock_owned()
                            .expect("reader should not be locked");

                        self.state =
                            StreamState::Waiting(next_frame_at, buf, Self::await_reader(reader));

                        return Poll::Ready(Some(Ok(ReadFrame::new(
                            WrappedReader(lock),
                            Position::new(pos, PREAMBLE_LEN as u32 + payload_len),
                            header,
                            body,
                        ))));
                    }
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Some(Err(err)));
                    }
                },
                StreamState::Waiting(pos, buf, mut fut) => match fut.as_mut().poll(cx) {
                    Poll::Ready(reader) => {
                        self.state = StreamState::Seeking(pos, reader, buf);
                        continue;
                    }
                    Poll::Pending => {
                        self.state = StreamState::Waiting(pos, buf, fut);
                        return Poll::Pending;
                    }
                },
                StreamState::Seeking(pos, mut reader, buf) => {
                    match Pin::new(&mut reader).poll_seek(cx, SeekFrom::Start(pos)) {
                        Poll::Pending => {
                            self.state = StreamState::Seeking(pos, reader, buf);
                            return Poll::Pending;
                        }
                        Poll::Ready(Ok(actual_position)) => {
                            if pos == actual_position {
                                self.state = StreamState::ReadingPreamble(
                                    pos,
                                    Self::read_preamble(reader, buf),
                                );
                                continue;
                            } else {
                                self.state = StreamState::Seeking(pos, reader, buf);
                                continue;
                            }
                        }
                        Poll::Ready(Err(e)) => {
                            return Poll::Ready(Some(Err(e.into())));
                        }
                    }
                }
                StreamState::Done => {
                    panic!("polled after completion");
                }
            }
        }
    }
}

pub struct ReadFrame<T: InnerReader> {
    position: Position<u64, u32>,
    reader: Arc<Mutex<T>>,
    header: Position<u64, u16>,
    body: Option<Position<u64, u32>>,
}

impl<T: InnerReader> ReadFrame<T> {
    fn new(
        reader: T,
        position: Position<u64, u32>,
        header: Position<u64, u16>,
        body: impl Into<Option<Position<u64, u32>>>,
    ) -> Self {
        Self {
            position,
            reader: Arc::new(Mutex::new(reader)),
            header,
            body: body.into(),
        }
    }

    pub fn position(&self) -> &Position<u64, u32> {
        &self.position
    }

    pub fn header_len(&self) -> u16 {
        self.header.length
    }

    async fn reader(&mut self, offset: u64, len: usize) -> Result<impl AsyncRead + Send + Unpin> {
        let lock = self.reader.clone().lock_owned().await;
        let mut reader = WrappedReader(lock);
        reader.seek(SeekFrom::Start(offset)).await?;
        Ok(LimitedReader::new(reader, len))
    }

    pub async fn header(&mut self) -> Result<impl AsyncRead + Send + Unpin> {
        self.reader(self.header.offset, self.header.length as usize)
            .await
    }

    pub fn body_len(&self) -> u32 {
        self.body.as_ref().map(|b| b.length).unwrap_or(0)
    }

    pub async fn body(&mut self) -> Result<Option<impl AsyncRead + Send + Unpin>> {
        let (offset, len) = match self.body.as_ref() {
            Some(pos) => (pos.offset, pos.length),
            None => return Ok(None),
        };
        Ok(Some(self.reader(offset, len as usize).await?))
    }
}

pub(crate) trait FrameBuf: Buf + Send + Unpin {}
impl<T: Buf + Send + Unpin> FrameBuf for T {}

pub struct WriteFrame<'a> {
    header: Box<dyn FrameBuf + 'a>,
    body: Option<Box<dyn FrameBuf + 'a>>,
}

impl<'a> WriteFrame<'a> {
    pub fn header_only(header: impl FrameBuf + 'a) -> Self {
        Self {
            header: Box::new(header),
            body: None,
        }
    }

    pub fn full(header: impl FrameBuf + 'a, body: impl FrameBuf + 'a) -> Self {
        Self {
            header: Box::new(header),
            body: Some(Box::new(body)),
        }
    }
}

struct SinkFrame<'a> {
    preamble: Option<Bytes>,
    header: Option<Box<dyn FrameBuf + 'a>>,
    body: Option<Box<dyn FrameBuf + 'a>>,
}

impl<'a> From<WriteFrame<'a>> for SinkFrame<'a> {
    fn from(value: WriteFrame<'a>) -> Self {
        let mut preamble = BytesMut::with_capacity(PREAMBLE_LEN);
        let header_len = value.header.remaining() as u16;
        let body_len = value
            .body
            .as_ref()
            .map(|b| b.remaining() as u32)
            .unwrap_or(0);
        preamble.put_u32(header_len as u32 + body_len);
        preamble.put_u16(header_len);
        preamble.put_u16(crc(&preamble));

        let preamble = preamble.freeze();
        Self {
            preamble: Some(preamble),
            header: Some(value.header),
            body: value.body,
        }
    }
}

pin_project! {
    pub(crate) struct FramingSink<'a, T> {
        #[pin]
        writer: T,
        buffer: Option<SinkFrame<'a>>,
    }
}

impl<T> FramingSink<'_, T> {
    pub fn into_inner(self) -> T {
        self.writer
    }
}

impl<T: AsyncWrite + Send> FramingSink<'_, T> {
    pub fn new(writer: T) -> Self {
        Self {
            writer,
            buffer: None,
        }
    }
}

impl<T: AsyncWrite + Send> FramingSink<'_, T> {
    fn poll_flush_buffer(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        let mut this = self.project();

        if let Some(buffer) = this.buffer {
            if let Some(preamble) = buffer.preamble.as_mut() {
                ready!(Self::poll_flush_buffer_item(
                    this.writer.as_mut(),
                    preamble,
                    cx
                ))?;
                buffer.preamble = None;
            }
            if let Some(header) = buffer.header.as_mut() {
                ready!(Self::poll_flush_buffer_item(
                    this.writer.as_mut(),
                    header,
                    cx
                ))?;
                buffer.header = None;
            }
            if let Some(body) = buffer.body.as_mut() {
                ready!(Self::poll_flush_buffer_item(this.writer.as_mut(), body, cx))?;
                buffer.body = None;
            }
        }
        *this.buffer = None;
        Poll::Ready(Ok(()))
    }

    fn poll_flush_buffer_item<B: Buf>(
        mut writer: Pin<&mut T>,
        mut buf: B,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), io::Error>> {
        loop {
            let chunk = buf.chunk();
            if chunk.len() == 0 {
                break;
            }
            let written = ready!(writer.as_mut().poll_write(cx, chunk))?;
            buf.advance(written);
        }
        Poll::Ready(Ok(()))
    }
}

impl<'a, T: AsyncWrite + Send> Sink<WriteFrame<'a>> for FramingSink<'a, T> {
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        ready!(self.poll_flush_buffer(cx))?;
        Poll::Ready(Ok(()))
    }

    fn start_send(
        self: Pin<&mut Self>,
        item: WriteFrame<'a>,
    ) -> std::result::Result<(), Self::Error> {
        debug_assert!(self.buffer.is_none());
        *self.project().buffer = Some(item.into());
        Ok(())
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush_buffer(cx))?;
        ready!(self.project().writer.poll_flush(cx))?;
        Poll::Ready(Ok(()))
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush_buffer(cx))?;
        ready!(self.project().writer.poll_close(cx))?;
        Poll::Ready(Ok(()))
    }
}

fn crc<T: AsRef<[u8]>>(input: T) -> u16 {
    static CRC: OnceLock<Crc<u16>> = OnceLock::new();
    let mut digest = CRC
        .get_or_init(|| Crc::<u16>::new(&crc::CRC_16_KERMIT))
        .digest();
    digest.update(input.as_ref());
    digest.finalize()
}
