use std::io;

use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder, ZlibDecoder};
use futures_util::TryStreamExt;
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::body::RespBody;
use crate::error::Error;

pub(crate) fn maybe_decompress(body: RespBody, encoding: Option<&str>) -> RespBody {
    let reader = |body: RespBody| {
        StreamReader::new(
            body.into_data_stream()
                .map_err(err_to_io as fn(Error) -> io::Error),
        )
    };
    match encoding {
        Some("gzip" | "x-gzip") => {
            let mut decoder = GzipDecoder::new(reader(body));
            decoder.multiple_members(true);
            wrap(decoder)
        }
        Some("br") => wrap(BrotliDecoder::new(reader(body))),
        Some("deflate" | "x-deflate") => wrap(ZlibDecoder::new(reader(body))),
        _ => body,
    }
}

fn wrap<D>(dec: D) -> RespBody
where
    D: tokio::io::AsyncRead + Send + Sync + Unpin + 'static,
{
    StreamBody::new(
        ReaderStream::with_capacity(dec, 64 * 1024)
            .map_ok(Frame::data)
            .map_err(|e| Error::Body(e.to_string())),
    )
    .boxed()
}

fn err_to_io(e: Error) -> io::Error {
    io::Error::other(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_compression::tokio::write::GzipEncoder;
    use bytes::Bytes;
    use http_body_util::Full;
    use tokio::io::AsyncWriteExt;

    async fn gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = GzipEncoder::new(Vec::new());
        encoder.write_all(data).await.unwrap();
        encoder.shutdown().await.unwrap();
        encoder.into_inner()
    }

    #[tokio::test]
    async fn gzip_decodes_every_member() {
        let mut data = gzip(b"hello ").await;
        data.extend(gzip(b"world").await);
        let body: RespBody = Full::new(Bytes::from(data))
            .map_err(|never| match never {})
            .boxed();
        let decoded = maybe_decompress(body, Some("gzip"))
            .collect()
            .await
            .unwrap()
            .to_bytes();
        assert_eq!(&decoded[..], b"hello world");
    }
}
