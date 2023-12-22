use std::error::Error;
use std::io;
use std::io::ErrorKind::NotFound;
use axum::http;
use log::error;
use thiserror::Error;
use crate::Data::{Hello, NoMsg, Unknown};

fn main() {
    let a = Hello("world".into());
    println!("{a}-----{Unknown}");
    println!("{NoMsg}");
    let b = io::Error::new(NotFound, "33333");
    // let c = Data::from(b);
    let c = Data::From{err: b};
    println!("{c}----source:{:?}", c.source());

    let a: anyhow::Error = c.into();

    println!("{a}")
}

#[derive(Error, Debug)]
pub enum Data {
    #[error("this is err hello {0}")]
    Hello(String),
    #[error("this is unknown")]
    Unknown,
    #[error("")]
    NoMsg,
    #[error("this is from {err}")]
    From{#[source] err: io::Error},
    #[error("this is from2 {0}")]
    From2(#[from] http::Error),
}