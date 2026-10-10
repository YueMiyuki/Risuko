use std::io;

use async_compression::tokio::bufread::{BrotliDecoder, GzipDecoder, ZlibDecoder};
use futures_util::TryStreamExt;
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use tokio_util::io::{ReaderStream, StreamReader};

use crate::body::RespBody;
use crate::error::Error;

pub(crate) fn maybe_decompress(body: RespBody, encoding: Option<&str>) -> RespBody {
    let enc = encoding.map(|s| s.trim().to_ascii_lowercase());
    let reader = |body: RespBody| {
        StreamReader::new(
            body.into_data_stream()
                .map_err(err_to_io as fn(Error) -> io::Error),
        )
    };
    match enc.as_deref() {
        Some("gzip" | "x-gzip") => wrap(GzipDecoder::new(reader(body))),
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
