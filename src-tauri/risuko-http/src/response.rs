use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use http::{HeaderMap, StatusCode, Version};
use http_body_util::BodyExt;
use serde::de::DeserializeOwned;
use url::Url;

use crate::body::RespBody;
use crate::error::{Error, Result};

pub struct Response {
    status: StatusCode,
    version: Version,
    headers: HeaderMap,
    url: Url,
    body: Option<RespBody>,
}

impl Response {
    pub(crate) fn new(
        status: StatusCode,
        version: Version,
        headers: HeaderMap,
        url: Url,
        body: RespBody,
    ) -> Self {
        Self {
            status,
            version,
            headers,
            url,
            body: Some(body),
        }
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    pub fn content_length(&self) -> Option<u64> {
        self.headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse().ok())
    }

    pub fn error_for_status(self) -> Result<Self> {
        if self.status.is_client_error() || self.status.is_server_error() {
            Err(Error::Status(self.status))
        } else {
            Ok(self)
        }
    }

    pub async fn bytes(mut self) -> Result<Bytes> {
        let body = self
            .body
            .take()
            .ok_or_else(|| Error::Body("body already consumed".into()))?;
        let collected = body
            .collect()
            .await
            .map_err(|e| Error::Body(e.to_string()))?;
        Ok(collected.to_bytes())
    }

    pub async fn bytes_limited(mut self, max: usize) -> Result<Bytes> {
        let too_big = || Error::Body(format!("body exceeds {max} bytes"));
        if self.content_length().is_some_and(|n| n > max as u64) {
            return Err(too_big());
        }
        let body = self
            .body
            .take()
            .ok_or_else(|| Error::Body("body already consumed".into()))?;
        let mut stream = body.into_data_stream();
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if out.len() + chunk.len() > max {
                return Err(too_big());
            }
            out.extend_from_slice(&chunk);
        }
        Ok(out.into())
    }

    pub async fn snippet(mut self, max: usize) -> String {
        let Some(body) = self.body.take() else {
            return String::new();
        };
        let mut stream = body.into_data_stream();
        let mut out = Vec::new();
        while let Some(Ok(chunk)) = stream.next().await {
            let room = max - out.len();
            out.extend_from_slice(&chunk[..chunk.len().min(room)]);
            if out.len() >= max {
                break;
            }
        }
        String::from_utf8_lossy(&out).into_owned()
    }

    pub async fn text_limited(self, max: usize) -> Result<String> {
        let bytes = self.bytes_limited(max).await?;
        String::from_utf8(bytes.into()).map_err(|e| Error::Decode(e.to_string()))
    }

    pub(crate) async fn drain_bounded(mut self) {
        const LIMIT: usize = 64 * 1024;
        if let Some(body) = self.body.take() {
            let mut stream = body.into_data_stream();
            let mut seen = 0usize;
            while let Some(Ok(chunk)) = stream.next().await {
                seen += chunk.len();
                if seen >= LIMIT {
                    break;
                }
            }
        }
    }

    pub async fn text(self) -> Result<String> {
        let bytes = self.bytes().await?;
        String::from_utf8(bytes.into()).map_err(|e| Error::Decode(e.to_string()))
    }

    pub async fn json<T: DeserializeOwned>(self) -> Result<T> {
        let bytes = self.bytes().await?;
        serde_json::from_slice(&bytes).map_err(|e| Error::Decode(e.to_string()))
    }

    pub fn bytes_stream(mut self) -> impl Stream<Item = Result<Bytes>> + Send + 'static {
        let body = self.body.take().expect("body already consumed");
        body.into_data_stream()
    }
}

impl std::fmt::Debug for Response {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status)
            .field("version", &self.version)
            .field("url", &self.url.as_str())
            .finish()
    }
}
