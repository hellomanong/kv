use std::{
    fmt::UpperExp,
    io::{Read, Write},
};

use bytes::{Buf, BufMut, BytesMut};
use flate2::{bufread::GzDecoder, write::GzEncoder, Compression};
use prost::Message;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::{CommandRequest, CommandResponse, KvError};

pub const LEN_LEN: usize = 4;
const MAX_FRAME: usize = 2 * 1024 * 1024 * 1024;
const COMPRESSION_LIMINT: usize = 1436;
const COMPRESSION_BIT: usize = 1 << 31;

pub trait FrameCoder
where
    Self: Message + Sized + Default,
{
    fn encode_frame(&self, buf: &mut BytesMut) -> Result<(), KvError> {
        let size = self.encoded_len();
        if size >= MAX_FRAME {
            return Err(KvError::FrameError);
        }

        buf.put_u32(size as _);

        if size > COMPRESSION_LIMINT {
            let mut bufTmp = Vec::with_capacity(size);
            self.encode(&mut bufTmp);

            let payload = buf.split_off(LEN_LEN);
            buf.clear();

            let mut encoder = GzEncoder::new(payload.writer(), Compression::default());
            encoder.write_all(&bufTmp[..])?;

            let payload = encoder.finish()?.into_inner();
            buf.put_u32((payload.len() | COMPRESSION_BIT) as _);

            buf.unsplit(payload);
        } else {
            self.encode(buf)?
        }

        Ok(())
    }

    fn decode_frame(buf: &mut BytesMut) -> Result<Self, KvError> {
        let header = buf.get_u32() as usize;
        let (len, compressed) = decode_header(header);

        if compressed {
            let mut decoder = GzDecoder::new(&buf[..len]);
            let mut bufTmp = Vec::with_capacity(len * 2);
            decoder.read_to_end(&mut bufTmp)?;
            buf.advance(len);

            Ok(Self::decode(&bufTmp[..bufTmp.len()])?)
        } else {
            let msg = Self::decode(&buf[..len])?;
            buf.advance(len);
            Ok(msg)
        }
    }
}

impl FrameCoder for CommandRequest {}
impl FrameCoder for CommandResponse {}

fn decode_header(header: usize) -> (usize, bool) {
    let len = header & !COMPRESSION_BIT;
    let compressed = header & COMPRESSION_BIT == COMPRESSION_BIT;
    (len, compressed)
}

pub async fn read_frame<S>(stream: &mut S, buf: &mut BytesMut) -> Result<(), KvError>
where
    S: AsyncRead + Unpin + Send,
{
    let header = stream.read_u32().await? as usize;
    let (len, compressed) = decode_header(header);

    buf.reserve(LEN_LEN + len);

    buf.put_u32(header as _);

    unsafe { buf.advance_mut(len) }

    stream.read_exact(&mut buf[LEN_LEN..]).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use tokio::io::AsyncRead;
    use log::info;

    use crate::{CommandRequest, CommandResponse, Value, utils};

    use super::{read_frame, FrameCoder, COMPRESSION_BIT, COMPRESSION_LIMINT};
    use crate::utils::DummyStream;
    
    #[test]
    fn command_request_encode_decode_should_work() {
        // tracing_subscriber::fmt::try_init();
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hget("t1", "k1");
        cmd.encode_frame(&mut buf).unwrap();
        assert_eq!(is_compressed(&buf), false);
        let cmd1 = CommandRequest::decode_frame(&mut buf).unwrap();
        assert_eq!(cmd, cmd1);
    }

    #[test]
    fn command_response_encode_decode_should_work() {
        // tracing_subscriber::fmt::try_init();
        let mut buf = BytesMut::new();

        let values: Vec<Value> = vec![1.into(), "hello".into(), b"world".into()];
        let res: CommandResponse = values.into();
        res.encode_frame(&mut buf).unwrap();
        assert_eq!(is_compressed(&buf), false);
        let res1 = CommandResponse::decode_frame(&mut buf).unwrap();
        info!("---------------3333333333333333333---------------");
        assert_eq!(res, res1);
    }

    #[test]
    fn command_response_compressed_encode_decode_should_work() {
        let mut buf = BytesMut::new();
        let value: Value = Bytes::from(vec![0u8; COMPRESSION_LIMINT + 1]).into();
        let res: CommandResponse = value.into();

        res.encode_frame(&mut buf).unwrap();
        assert_eq!(is_compressed(&buf), true);

        let res1 = CommandResponse::decode_frame(&mut buf).unwrap();
        assert_eq!(res, res1);
    }

    fn is_compressed(data: &[u8]) -> bool {
        if let &[v] = &data[..1] {
            v >> 7 == 1
        } else {
            false
        }
    }

    #[tokio::test]
    async fn read_frame_should_work() {
        let mut buf = BytesMut::new();
        let cmd = CommandRequest::new_hget("t1", "k1");
        cmd.encode_frame(&mut buf).unwrap();
        let mut stream = DummyStream { buf };

        let mut tmp = BytesMut::new();
        read_frame(&mut stream, &mut tmp).await.unwrap();

        let cmd1 = CommandRequest::decode_frame(&mut tmp).unwrap();

        assert_eq!(cmd, cmd1);
    }
}
