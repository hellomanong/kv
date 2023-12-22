use std::{
    marker::PhantomData,
    pin::Pin,
    task::{ready, Poll},
};

use bytes::BytesMut;
use futures::{Future, FutureExt, Sink, Stream};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::{network::frame::read_frame, KvError};

use super::frame::FrameCoder;

pub struct ProstStream<S, In, Out> {
    stream: S,
    wbuf: BytesMut,
    rbuf: BytesMut,
    written: usize,
    _in: PhantomData<In>,
    _out: PhantomData<Out>,
}

impl<S, In, Out> Stream for ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    In: Unpin + Send + FrameCoder,
    Out: Unpin + Send,
{
    type Item = Result<In, KvError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        assert!(self.rbuf.len() == 0);

        let mut rest = self.rbuf.split_off(0);
        let fut = read_frame(&mut self.stream, &mut rest);
        // {
        //     let mut f = Box::pin(fut);
        //     let mut f2 = Pin::new(&mut f);
        //     ready!(f2.poll(cx))?;
        // }
        ready!(Box::pin(fut).poll_unpin(cx))?; //这一段跟上面是一样的意思
        self.rbuf.unsplit(rest);
        Poll::Ready(Some(In::decode_frame(&mut self.rbuf)))
    }
}

impl<S, In, Out> Sink<&Out> for ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
    In: Unpin + Send,
    Out: Unpin + Send + FrameCoder,
{
    type Error = KvError;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: std::pin::Pin<&mut Self>, item: &Out) -> Result<(), Self::Error> {
        item.encode_frame(&mut self.wbuf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        let this = self.get_mut();
        while this.written != this.wbuf.len() {
            let n = ready!(Pin::new(&mut this.stream).poll_write(cx, &this.wbuf[this.written..]))?;
            this.written += n;
        }

        this.wbuf.clear();
        this.written = 0;

        ready!(Pin::new(&mut this.stream).poll_flush(cx)?);

        Poll::Ready(Ok(()))
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;
        ready!(Pin::new(&mut self.stream).poll_shutdown(cx))?;
        Poll::Ready(Ok(()))
    }
}

impl<S, In, Out> ProstStream<S, In, Out>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            wbuf: BytesMut::new(),
            rbuf: BytesMut::new(),
            written: 0,
            _in: PhantomData::default(),
            _out: PhantomData::default(),
        }
    }
}

impl<S, Req, Res> Unpin for ProstStream<S, Req, Res> where S: Unpin {}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use bytes::BytesMut;
    use crate::{utils::DummyStream, CommandRequest, CommandResponse};

    use super::ProstStream;
    use futures::prelude::*;

    #[tokio::test]
    async fn prost_stream_should_work() -> Result<()> {
        let stream = DummyStream{buf: BytesMut::new()};
        let mut p_stream: ProstStream<DummyStream, CommandRequest, CommandRequest> =  ProstStream::new(stream);
        let cmd = CommandRequest::new_hget("t1", "k1");
        p_stream.send(&cmd).await?;

        if let Some(Ok(s)) = p_stream.next().await {
            assert_eq!(s, cmd);
        } else {
            assert!(false);
        }

        Ok(())
    }
}
