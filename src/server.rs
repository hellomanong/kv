use anyhow::Result;
use futures::StreamExt;
use kv::{
    multiplex::YamuxSession, tls::TlsServerAcceptor, MemTable, ProstServerStream, Service,
    ServiceInner,
};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let server_cert = include_str!("../fixtures/server.cert");
    let server_key = include_str!("../fixtures/server.key");
    let accepter = TlsServerAcceptor::new(server_cert, server_key, None)?;

    let addr = "127.0.0.1:12345";
    let listener = TcpListener::bind(addr).await?;

    let db = MemTable::new();
    let service: Service = ServiceInner::new(db).into();

    loop {
        let tls = accepter.clone();
        let (stream, _) = listener.accept().await?;
        let stream = tls.accept(stream).await?;
        let sv = service.clone();
        tokio::spawn(async move {
            let mut session = YamuxSession::new_server(stream, None);
            while let Some(Ok(y_stream)) = session.conn.next().await {
                let sv = sv.clone();
                let pcs = ProstServerStream::new(y_stream, sv);
                tokio::spawn(async move { pcs.process().await });
            }
        });
    }
}
