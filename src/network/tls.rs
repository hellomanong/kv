use std::{
    borrow::BorrowMut,
    fs::File,
    io::{BufReader, Cursor},
    path::Path,
    sync::Arc,
};

use axum::{extract::path, http::header::Keys};
use bytes::BytesMut;
use tokio::io::{self, AsyncRead, AsyncWrite};
use tokio_rustls::{
    rustls::{
        internal::pemfile::{self, certs, rsa_private_keys},
        AllowAnyAnonymousOrAuthenticatedClient, Certificate, ClientConfig, NoClientAuth,
        PrivateKey, RootCertStore, ServerConfig,
    },
    webpki::DNSNameRef,
    TlsAcceptor, TlsConnector,
};

use tokio_rustls::{client::TlsStream as ClientTlsStream, server::TlsStream as ServerTlsStream};

use crate::KvError;
const ALPN_KV: &str = "kv";
#[derive(Clone)]
pub struct TlsServerAcceptor {
    inner: Arc<ServerConfig>,
}

#[derive(Clone)]
pub struct TlsClientConnector {
    pub config: Arc<ClientConfig>,
    pub domain: Arc<String>,
}

impl TlsClientConnector {
    pub fn new(
        domain: impl Into<String>,
        identity: Option<(&str, &str)>,
        server_ca: Option<&str>,
    ) -> Result<Self, KvError> {
        let mut config = ClientConfig::new();

        if let Some((cart, key)) = identity {
            let certs = load_certs(cart)?;
            let key = load_key(key)?;

            config.set_single_client_cert(certs, key)?;
        }

        config.root_store = match rustls_native_certs::load_native_certs() {
            Ok(store) | Err((Some(store), _)) => store,
            Err((None, error)) => return Err(error.into()),
        };

        if let Some(cert) = server_ca {
            let mut buf = Cursor::new(cert);
            config.root_store.add_pem_file(&mut buf).unwrap();
        }

        Ok(Self {
            config: Arc::new(config),
            domain: Arc::new(domain.into()),
        })
    }

    pub async fn connect<S>(&self, stream: S) -> Result<ClientTlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let dns = DNSNameRef::try_from_ascii_str(self.domain.as_str())
            .map_err(|_| KvError::Internal("Invalid DNS name".into()))?;

        let stream = TlsConnector::from(self.config.clone())
            .connect(dns, stream)
            .await?;

        Ok(stream)
    }
}

impl TlsServerAcceptor {
    pub fn new(cert: &str, key: &str, client_ca: Option<&str>) -> Result<Self, KvError> {
        let certs = load_certs(cert)?;
        let key = load_key(key)?;

        let mut config = match client_ca {
            None => ServerConfig::new(NoClientAuth::new()),
            Some(cert_path) => {
                let mut cert = Cursor::new(cert_path);
                let mut client_root_cert_store = RootCertStore::empty();
                client_root_cert_store
                    .add_pem_file(&mut cert)
                    .map_err(|_| KvError::CertifcateParseError("CA".into(), "cert".into()))?;

                let client_auth =
                    AllowAnyAnonymousOrAuthenticatedClient::new(client_root_cert_store);
                ServerConfig::new(client_auth)
            }
        };

        config
            .set_single_cert(certs, key)
            .map_err(|_| KvError::CertifcateParseError("server".into(), "cert".into()));

        config.set_protocols(&[Vec::from(&ALPN_KV[..])]);

        Ok(Self {
            inner: Arc::new(config),
        })
    }

    pub async fn accept<S>(&self, stream: S) -> Result<ServerTlsStream<S>, KvError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let accepter = TlsAcceptor::from(self.inner.clone());
        Ok(accepter.accept(stream).await?)
    }
}

fn load_certs(cert: &str) -> Result<Vec<Certificate>, KvError> {
    let mut cert: Cursor<&str> = Cursor::new(cert);
    certs(&mut cert).map_err(|_| KvError::CertifcateParseError("server".into(), "cert".into()))
}

fn load_key(key: &str) -> Result<PrivateKey, KvError> {
    // let file = File::open(key)?;
    // let mut reader = BufReader::new(file);
    let mut cursor = Cursor::new(key);

    // 先尝试用 PKCS8 加载私钥
    if let Ok(mut keys) = pemfile::pkcs8_private_keys(&mut cursor) {
        if !keys.is_empty() {
            return Ok(keys.remove(0));
        }
    }

    rsa_private_keys(&mut cursor).map_or(
        Err(KvError::CertifcateParseError(
            "private".into(),
            "keys".into(),
        )),
        |mut keys| Ok(keys.remove(0)),
    )
}

#[cfg(test)]
mod tests {
    use std::{io::BufReader, net::SocketAddr};

    use bytes::{BufMut, BytesMut};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };
    use tracing::{info, warn};

    use super::TlsServerAcceptor;
    use crate::network::tls::TlsClientConnector;

    const CA_CERT: &str = include_str!("../../fixtures/ca.cert");
    const CLIENT_CERT: &str = include_str!("../../fixtures/client.cert");
    const CLIENT_KEY: &str = include_str!("../../fixtures/client.key");
    const SERVER_CERT: &str = include_str!("../../fixtures/server.cert");
    const SERVER_KEY: &str = include_str!("../../fixtures/server.key");

    #[tokio::test]
    async fn tls_should_work() -> anyhow::Result<()> {
        // tracing_subscriber::fmt::init();
        let ca = Some(CA_CERT);
        let addr = start_server(None).await?;
        let connector = TlsClientConnector::new("kvserver.acme.inc", None, ca).unwrap();

        let connect = TcpStream::connect(addr).await.unwrap();

        let mut stream = connector.connect(connect).await.unwrap();
        stream.write_all(b"hello world").await.unwrap();
        let mut buf = BytesMut::new();
        stream.read_buf(&mut buf).await.unwrap();
        assert_eq!(buf, BytesMut::from("hello world"));
        Ok(())
    }

    #[tokio::test]
    async fn tls_with_client_cert_should_work() -> anyhow::Result<()> {
        let ca = Some(CA_CERT);
        let addr = start_server(ca).await?;
        let connector = TlsClientConnector::new("kvserver.acme.inc", Some((CLIENT_CERT, CLIENT_KEY)), ca).unwrap();

        let connect = TcpStream::connect(addr).await.unwrap();

        let mut stream = connector.connect(connect).await.unwrap();
        stream.write_all(b"hello world").await.unwrap();
        let mut buf = BytesMut::new();
        stream.read_buf(&mut buf).await.unwrap();
        assert_eq!(buf, BytesMut::from("hello world"));

        Ok(())
    }

    async fn start_server(ca: Option<&str>) -> anyhow::Result<SocketAddr> {
        let accepter = TlsServerAcceptor::new(SERVER_CERT, SERVER_KEY, ca).unwrap();
        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = echo.local_addr().unwrap();
        info!(
            "----------------------------addr:{}------------------------",
            addr
        );
        tokio::spawn(async move {
            let (tcp_stream, addr) = echo.accept().await.unwrap();
            let mut stream = accepter.accept(tcp_stream).await.unwrap();

            let mut buf = BytesMut::new();
            stream.read_buf(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();

            info!("==================================================");
        });

        Ok(addr)
    }
}


#[cfg(test)]
pub mod tls_utils {
    use crate::{KvError};

    use super::{TlsClientConnector, TlsServerAcceptor};

    const CA_CERT: &str = include_str!("../../fixtures/ca.cert");
    const CLIENT_CERT: &str = include_str!("../../fixtures/client.cert");
    const CLIENT_KEY: &str = include_str!("../../fixtures/client.key");
    const SERVER_CERT: &str = include_str!("../../fixtures/server.cert");
    const SERVER_KEY: &str = include_str!("../../fixtures/server.key");

    pub fn tls_connector(client_cert: bool) -> Result<TlsClientConnector, KvError> {
        let ca = Some(CA_CERT);
        let client_identity = Some((CLIENT_CERT, CLIENT_KEY));

        match client_cert {
            false => TlsClientConnector::new("kvserver.acme.inc", None, ca),
            true => TlsClientConnector::new("kvserver.acme.inc", client_identity, ca),
        }
    }

    pub fn tls_acceptor(client_cert: bool) -> Result<TlsServerAcceptor, KvError> {
        let ca = Some(CA_CERT);
        match client_cert {
            true => TlsServerAcceptor::new(SERVER_CERT, SERVER_KEY, ca),
            false => TlsServerAcceptor::new(SERVER_CERT, SERVER_KEY, None),
        }
    }
}