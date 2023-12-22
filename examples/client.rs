use tokio::net::TcpStream;
use anyhow::Result;
use async_prost::AsyncProstStream;
use futures::{SinkExt, StreamExt};
use tracing::info;
use kv::{CommandRequest, CommandResponse};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:9527";
    let stream = TcpStream::connect(addr).await?;

    let mut client = AsyncProstStream::<_, CommandResponse, CommandRequest, _>::from(stream).for_async();
    let cmd = CommandRequest::new_hset("table1", "hello", "world".into());
    client.send(cmd).await?;

    if let Some(Ok(data)) = client.next().await {
        info!("Got response {data:?}");
    }

    Ok(())
}