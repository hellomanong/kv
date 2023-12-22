use std::io;

use thiserror::Error;
use tokio_rustls::rustls::{Certificate, TLSError};
use crate::Value;


#[derive(Error, Debug)]
pub enum KvError {
    #[error("Not found for table:{0}, key:{1}")]
    NotFound(String, String),
    #[error("Cannot parse command: `{0}`")]
    InvalidCommand(String),
    #[error("Cannot convert value {0:?} to {1}")]
    ConvertError(String, &'static str),
    #[error("Cannot process command {0} with table: {1}, key: {2}, value: {3}")]
    StorageError(&'static str, String, String, String),
    #[error("Failed to encode protobuf message, err info: {0}")]
    EncodeError(#[from] prost::EncodeError),
    #[error("Failed to decode protobuf message, err info: {0}")]
    DecodeError(#[from] prost::DecodeError),
    #[error("Internal error: {0}")]
    Internal(String),
    #[error("Failed to access sled db")]
    SledError(#[from] sled::Error),
    #[error("Frame is largar than max size")]
    FrameError,
    #[error("I/O error")]
    IoError(#[from] io::Error),
    #[error("Certifcate parse error server:{0}, cert:{1}")]
    CertifcateParseError(String, String),
    #[error("Config TLS failed")]
    TLSError(#[from] TLSError),
    #[error("Connect open stream failed")]
    ConnectError(#[from] tokio_yamux::Error)
}