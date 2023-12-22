use axum::async_trait;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_yamux::{session::SessionType, Config, Session, StreamHandle};
use tracing::instrument;

use crate::{AppStream, KvError, ProstClientStream};

pub struct YamuxSession<S> {
    pub conn: Session<S>,
}

impl<S> YamuxSession<S>
where
    S: AsyncRead + AsyncWrite + Send + Unpin,
{
    pub fn new_client(stream: S, config: Option<Config>) -> Self {
        let mut config = config.unwrap_or_default();
        Self {
            conn: Session::new_client(stream, config),
        }
    }

    pub fn new_server(stream: S, config: Option<Config>) -> Self {
        let mut config = config.unwrap_or_default();
        Self {
            conn: Session::new_server(stream, config),
        }
    }

    pub async fn open_stream(&mut self) -> Result<StreamHandle, KvError> {
        let stream = self.conn.open_stream()?;
        Ok(stream)
    }
}

#[async_trait]
impl<S> AppStream for YamuxSession<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    type InnerStream = StreamHandle;

    #[instrument(skip_all)]
    async fn open_stream(&mut self) -> Result<ProstClientStream<Self::InnerStream>, KvError> {
        let stream = self.conn.open_stream()?;
        Ok(ProstClientStream::new(stream))
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::*;
    use crate::{
        assert_res_ok, tls::tls_utils, tls::TlsServerAcceptor, utils, utils::DummyStream,
        CommandRequest, KvError, MemTable, ProstClientStream, ProstServerStream, Service,
        ServiceInner, Storage,
    };
    use anyhow::Result;
    use bytes::BytesMut;
    use futures::StreamExt;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    use tokio_rustls::server;
    use tracing::{info, warn};
    use tracing_test::traced_test;

    pub async fn start_server_with<Store>(
        addr: &str,
        tls: TlsServerAcceptor,
        store: Store,
    ) -> Result<SocketAddr, KvError>
    where
        Store: Storage,
        Service: From<ServiceInner<Store>>,
    {
        let listener = TcpListener::bind(addr).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let service: Service = ServiceInner::new(store).into();

        tokio::spawn(async move {
            loop {
                let (stream, _addr) = listener.accept().await.unwrap();
                let stream = tls.accept(stream).await.unwrap();
                let svc: Service = service.clone();
                tokio::spawn(async move {
                    let mut conn = YamuxSession::new_server(stream, None);
                    while let Ok(y_stream) = conn.conn.next().await.unwrap() {
                        let svc = svc.clone();
                        tokio::spawn(async move {
                            let stream = ProstServerStream::new(y_stream, svc.clone());
                            stream.process().await.unwrap();
                        });
                    }
                });
            }
        });

        Ok(addr)
    }

    #[tokio::test]
    async fn yamux_ctrl_creation_should_work() -> Result<()> {
        let s = utils::DummyStream {
            buf: BytesMut::new(),
        };
        let mut conn = YamuxSession::new_client(s, None);
        let stream = conn.open_stream().await;

        assert!(stream.is_ok());
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn yamux_ctrl_client_server_should_work() -> Result<()> {
        // 创建使用了 TLS 的 yamux server
        let acceptor: TlsServerAcceptor = tls_utils::tls_acceptor(false)?;
        let addr = start_server_with("127.0.0.1:0", acceptor, MemTable::new())
            .await
            .unwrap();

        let connector = tls_utils::tls_connector(false)?;
        let stream = TcpStream::connect(addr).await?;
        let stream = connector.connect(stream).await?;
        // 创建使用了 TLS 的 yamux client
        let mut client = YamuxSession::new_client(stream, None);

        // 从 client ctrl 中打开一个新的 yamux stream
        let mut stream = client.open_stream().await.unwrap();

        tokio::spawn(async move {
            loop {
                match client.conn.next().await {
                    Some(Ok(_)) => (),
                    Some(Err(e)) => {
                        info!("{}", e);
                        break;
                    }
                    None => {
                        info!("closed");
                        break;
                    }
                }
            }
        });

        let cmd = CommandRequest::new_hset("t1", "k1", "v1".into());
        let mut p_stream: ProstClientStream<StreamHandle> = ProstClientStream::new(stream);
        let res = p_stream.execute_unary(cmd).await.unwrap();

        let cmd = CommandRequest::new_hget("t1", "k1");
        let res = p_stream.execute_unary(cmd).await.unwrap();
        assert_res_ok(&res, &["v1".into()], &[]);

        Ok(())
    }
}
