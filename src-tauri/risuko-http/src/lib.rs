mod body;
mod client;
mod connector;
mod cookies;
mod decompress;
pub mod doh;
mod error;
mod into_url;
mod proxy;
pub mod redirect;
mod request;
mod resolver;
mod response;

pub use body::ReqBody;
pub use client::{Client, ClientBuilder};
pub use connector::{
    datagram_source_matches, BoxedIo, ProxyAssociation, ProxyConnector, ProxyDatagram,
    ProxyDatagramSource,
};
pub use cookies::{CookieStore, Jar};
pub use doh::{DohConfig, DohResolver};
pub use error::{Error, Result};
pub use into_url::IntoUrl;
pub use proxy::{NoProxy, Proxy};
pub use redirect::Policy;
pub use request::RequestBuilder;
pub use resolver::{set_global_resolver, Addrs, GaiResolver, Resolve, Resolving};
pub use response::Response;

pub use http::header;
pub use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, Uri};
pub use url::Url;

pub fn file_stream_body_with_progress<F>(
    path: std::path::PathBuf,
    _content_length: u64,
    on_progress: F,
    cancel: Option<tokio_util::sync::CancellationToken>,
) -> ReqBody
where
    F: Fn(u64) + Send + Sync + 'static,
{
    file_stream_body_range_inner(
        path,
        0,
        None,
        Some(std::sync::Arc::new(on_progress)),
        cancel,
    )
}

pub fn file_stream_body_range_with_progress<F>(
    path: std::path::PathBuf,
    offset: u64,
    len: u64,
    on_progress: F,
    cancel: Option<tokio_util::sync::CancellationToken>,
) -> ReqBody
where
    F: Fn(u64) + Send + Sync + 'static,
{
    file_stream_body_range_inner(
        path,
        offset,
        Some(len),
        Some(std::sync::Arc::new(on_progress)),
        cancel,
    )
}

fn file_stream_body_range_inner(
    path: std::path::PathBuf,
    offset: u64,
    content_length: Option<u64>,
    on_progress: Option<std::sync::Arc<dyn Fn(u64) + Send + Sync + 'static>>,
    cancel: Option<tokio_util::sync::CancellationToken>,
) -> ReqBody {
    use futures_util::stream::TryStreamExt;
    use http_body::Frame;
    use http_body_util::{combinators::BoxBody, StreamBody};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    let verified_length = match std::fs::metadata(&path) {
        Ok(meta) => {
            let remaining = meta.len().saturating_sub(offset);
            Some(match content_length {
                Some(n) => n.min(remaining),
                None => remaining,
            })
        }
        Err(_) => content_length,
    };
    let take = verified_length;

    ReqBody::from_stream(
        move || {
            let path = path.clone();
            let on_progress = on_progress.clone();
            let cancel = cancel.clone();
            let counter = Arc::new(AtomicU64::new(0));
            let stream = async_stream::try_stream! {
                use tokio::io::{AsyncReadExt, AsyncSeekExt};
                let mut file = tokio::fs::File::open(&path).await
                    .map_err(|e| Error::Body(format!("open {}: {e}", path.display())))?;
                let file_size = file.metadata().await
                    .map_err(|e| Error::Body(format!("stat {}: {e}", path.display())))?
                    .len();
                let remaining = file_size.saturating_sub(offset);
                let effective_take = match take {
                    Some(n) if n > remaining => {
                        Err(Error::Body(format!(
                            "requested range {n} bytes from offset {offset} but only {remaining} available in {}",
                            path.display()
                        )))?;
                        unreachable!()
                    }
                    Some(n) => Some(n.min(remaining)),
                    None => None,
                };
                if offset > 0 {
                    file.seek(std::io::SeekFrom::Start(offset)).await
                        .map_err(|e| Error::Body(format!("seek {} to {offset}: {e}", path.display())))?;
                }
                let reader: Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin> = match effective_take {
                    Some(n) => Box::new(file.take(n)),
                    None => Box::new(file),
                };
                let reader = tokio_util::io::ReaderStream::with_capacity(reader, 256 * 1024);
                let mut reader = std::pin::pin!(reader);
                use futures_util::StreamExt;
                while let Some(chunk) = reader.next().await {
                    if let Some(ref c) = cancel {
                        if c.is_cancelled() {
                            Err(Error::Body("cancelled".into()))?;
                        }
                    }
                    let bytes = chunk.map_err(|e| Error::Body(e.to_string()))?;
                    if let Some(ref cb) = on_progress {
                        let sent = counter.fetch_add(bytes.len() as u64, Ordering::Relaxed)
                            + bytes.len() as u64;
                        cb(sent);
                    }
                    yield bytes;
                }
            };
            let frame_stream = stream.map_ok(Frame::data);
            BoxBody::new(StreamBody::new(frame_stream))
        },
        verified_length,
    )
}
