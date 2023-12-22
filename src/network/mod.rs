use std::os::unix::process;

use axum::async_trait;
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use log::info;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::{CommandRequest, CommandResponse, KvError, Service};

use self::{
    frame::{read_frame, FrameCoder},
    stream::ProstStream, stream_result::StreamResult,
};
mod frame;
pub mod multiplex;
pub mod stream;
pub mod tls;
mod stream_result;

#[async_trait]
pub trait AppStream {
    type InnerStream;
    async fn open_stream(&mut self) -> Result<ProstClientStream<Self::InnerStream>, KvError>;
}

pub struct ProstServerStream<S> {
    inner: ProstStream<S, CommandRequest, CommandResponse>,
    service: Service,
}

pub struct ProstClientStream<S> {
    inner: ProstStream<S, CommandResponse, CommandRequest>,
}

impl<S> ProstServerStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(stream: S, service: Service) -> Self {
        Self {
            inner: ProstStream::new(stream),
            service,
        }
    }

    pub async fn process(mut self) -> Result<(), KvError> {
        // let mut buf = BytesMut::new();
        let stream = &mut self.inner;
        while let Some(Ok(cmd)) = stream.next().await {
            info!("Got a new command: {cmd:?}");
            let mut res = self.service.execute(cmd);
            while let Some(data) = res.next().await {
                stream.send(&data).await?;
            }
        }

        // while let Ok(_) = read_frame(&mut self.inner, &mut buf).await {
        //     let cmd = CommandRequest::decode_frame(&mut buf)?;
        //     let res = self.service.execute(cmd);
        //     buf.clear();
        //     res.encode_frame(&mut buf)?;
        //     self.inner.write_all_buf(&mut buf).await?;
        //     buf.clear();
        // }

        Ok(())
    }
}

impl<S> ProstClientStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    pub fn new(stream: S) -> Self {
        Self {
            inner: ProstStream::new(stream),
        }
    }

    pub async fn execute_unary(&mut self, cmd: CommandRequest) -> Result<CommandResponse, KvError> {
        // let mut buf = BytesMut::new();
        // cmd.encode_frame(&mut buf)?;
        // self.inner.write_all_buf(&mut buf).await?;
        // buf.clear();

        // read_frame(&mut self.inner, &mut buf).await?;
        // let res = CommandResponse::decode_frame(&mut buf)?;

        self.inner.send(&cmd).await?;
        match self.inner.next().await {
            Some(cmd) => cmd,
            None => Err(KvError::Internal("Didn't get any response".into())),
        }
    }

    pub async fn execute_streaming(self, cmd: &CommandRequest) -> Result<StreamResult, KvError> {
        let mut stream = self.inner;
        stream.send(cmd).await?;
        stream.close().await?;
        StreamResult::new(stream).await
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use anyhow::Result;
    use bytes::Bytes;
    use tokio::net::{TcpListener, TcpStream};

    use crate::{
        assert_res_ok, network::ProstClientStream, CommandRequest, MemTable, Service, ServiceInner,
        Value,
    };

    use super::ProstServerStream;

    #[tokio::test]
    async fn client_server_basic_communication_should_work() -> Result<()> {
        let addr = start_server().await.unwrap();
        let stream = TcpStream::connect(addr).await.unwrap();
        let mut client = ProstClientStream::new(stream);
        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        let res = client.execute_unary(cmd).await.unwrap();

        assert_res_ok(&res, &[Value::default()], &[]);
        let cmd = CommandRequest::new_hget("t1", "k1");
        let res = client.execute_unary(cmd).await.unwrap();
        assert_res_ok(&res, &["v1".into()], &[]);
        Ok(())
    }

    #[tokio::test]
    async fn client_server_compression_should_work() -> anyhow::Result<()> {
        let addr = start_server().await?;
        let stream = TcpStream::connect(addr).await?;
        let mut client = ProstClientStream::new(stream);
        let v: Value = Bytes::from(vec![0u8; 16384]).into();
        let cmd = CommandRequest::new_hset("t2", "k2", v.clone().into());
        let res = client.execute_unary(cmd).await?;
        assert_res_ok(&res, &[Value::default()], &[]);
        let cmd = CommandRequest::new_hget("t2", "k2");
        let res = client.execute_unary(cmd).await?;
        assert_res_ok(&res, &[v.into()], &[]);
        Ok(())
    }

    async fn start_server() -> Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let service: Service = ServiceInner::new(MemTable::new()).into();
                let server = ProstServerStream::new(stream, service);
                tokio::spawn(server.process());
            }
        });
        Ok(addr)
    }
}

#[cfg(test)]
pub mod utils {
    use bytes::{BufMut, BytesMut};
    use tokio::io::{AsyncRead, AsyncWrite};

    #[derive(Default)]
    pub struct DummyStream {
        pub buf: BytesMut,
    }

    impl AsyncRead for DummyStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let len = buf.capacity();
            let data = self.get_mut().buf.split_to(len);
            buf.put_slice(&data);
            std::task::Poll::Ready(Ok(()))
        }
    }

    impl AsyncWrite for DummyStream {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            self.as_mut().buf.put_slice(buf);
            std::task::Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
}
