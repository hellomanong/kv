#[warn(unused_imports)]
use anyhow::Result;
use async_prost::AsyncProstStream;
use tokio::net::TcpListener;
use tracing::info;
use kv::{CommandRequest, CommandResponse, MemTable, Service, ServiceInner};
use futures::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let addr = "127.0.0.1:9527";
    let listener = TcpListener::bind(addr).await?;

    info!("Start listening on {addr}");

    let service: Service = ServiceInner::new(MemTable::new()).into();

    loop {
        let (stream, addr) = listener.accept().await?;
        info!("Client {addr:?} connected");
        let sv = service.clone();
        tokio::spawn(async move {
            let mut stream =
                AsyncProstStream::<_, CommandRequest, CommandResponse, _>::from(stream).for_async();

            while let Some(Ok(cmd)) = stream.next().await {
                info!("Got a new command: {:?}", cmd);
                let mut res = sv.execute(cmd);
                let data = res.next().await.unwrap();
                // stream.send(&data).await.unwrap();
            }
            info!("Client {addr:?} disconnected");
        });
    }
}