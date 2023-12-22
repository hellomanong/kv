use std::time::Duration;

use anyhow::Result;
use futures::{StreamExt, TryFutureExt};
use kv::{multiplex::YamuxSession, tls::TlsClientConnector, CommandRequest, ProstClientStream};
use tokio::{net::TcpStream, time};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let addr = "127.0.0.1:12345";
    let stream = TcpStream::connect(addr).await?;

    let ca = include_str!("../fixtures/ca.cert");
    let connector = TlsClientConnector::new("kvserver.acme.inc", None, Some(ca))?;
    let stream = connector.connect(stream).await?;

    let mut session = YamuxSession::new_client(stream, None);
    let stream = session.open_stream().await?;
    let stream2 = session.open_stream().await?;

    tokio::spawn(async move {
        loop {
            match session.conn.next().await {
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

    tokio::spawn(async move {
        let cmd = CommandRequest::new_subscriber("hello");
        let client = ProstClientStream::new(stream2);
        let mut res = client.execute_streaming(&cmd).await.unwrap();
        info!("Got id = {:?}", res.id);
        while let Some(Ok(v)) = &mut res.next().await {
            info!("---------------value:{v:?}");
        }
    });

    time::sleep(Duration::from_secs(1)).await;

    let mut client = ProstClientStream::new(stream);
    let cmd = CommandRequest::new_hset("table1", "hello", "world".into());
    let res = client.execute_unary(cmd).await?;
    info!("Got response hset {res:?}");

    let cmd = CommandRequest::new_hget("table1", "hello");
    let res = client.execute_unary(cmd).await?;
    info!("Got response hget {res:?}");

    let cmd = CommandRequest::new_publish("hello", "222222".into());
    client.execute_unary(cmd).await?;

    time::sleep(Duration::from_secs(10)).await;

    Ok(())
}
