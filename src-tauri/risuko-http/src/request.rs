use std::time::Duration;

use bytes::Bytes;
use http::header::{HeaderMap, HeaderName, HeaderValue};
use http::Method;
use serde::Serialize;
use url::Url;

use crate::body::ReqBody;
use crate::client::Client;
use crate::error::{Error, Result};
use crate::response::Response;

pub struct RequestBuilder {
    pub(crate) client: Client,
    pub(crate) method: Method,
    pub(crate) url: Result<Url>,
    pub(crate) headers: HeaderMap,
    pub(crate) body: ReqBody,
    pub(crate) timeout: Option<Duration>,
}

impl RequestBuilder {
    pub fn new(client: Client, method: Method, url: Result<Url>) -> Self {
        Self {
            client,
            method,
            url,
            headers: HeaderMap::new(),
            body: ReqBody::Empty,
            timeout: None,
        }
    }

    pub fn header<K, V>(mut self, key: K, value: V) -> Self
    where
        HeaderName: TryFrom<K>,
        HeaderValue: TryFrom<V>,
        <HeaderName as TryFrom<K>>::Error: Into<http::Error>,
        <HeaderValue as TryFrom<V>>::Error: Into<http::Error>,
    {
        match (HeaderName::try_from(key), HeaderValue::try_from(value)) {
            (Ok(k), Ok(v)) => {
                self.headers.insert(k, v);
            }
            _ => tracing::warn!("ignoring invalid header"),
        }
        self
    }

    pub fn headers(mut self, headers: HeaderMap) -> Self {
        for (k, v) in headers.iter() {
            self.headers.append(k.clone(), v.clone());
        }
        self
    }

    pub fn timeout(mut self, t: Duration) -> Self {
        self.timeout = Some(t);
        self
    }

    pub fn body<B: Into<Bytes>>(mut self, body: B) -> Self {
        self.body = ReqBody::from_bytes(body);
        self
    }

    pub fn stream_body(mut self, body: ReqBody) -> Self {
        self.body = body;
        self
    }

    pub fn json<T: Serialize>(mut self, value: &T) -> Self {
        match serde_json::to_vec(value) {
            Ok(bytes) => {
                self.body = ReqBody::from_bytes(Bytes::from(bytes));
                self.headers.insert(
                    http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
            }
            Err(e) => self.url = Err(Error::Encode(e.to_string())),
        }
        self
    }

    pub fn query<T: Serialize + ?Sized>(mut self, q: &T) -> Self {
        if let Ok(ref mut url) = self.url {
            match serde_urlencoded::to_string(q) {
                Ok(s) if s.is_empty() => {}
                Ok(s) => {
                    let existing = url.query().unwrap_or("");
                    let merged = if existing.is_empty() {
                        s
                    } else {
                        format!("{existing}&{s}")
                    };
                    url.set_query(Some(&merged));
                }
                Err(e) => {
                    self.url = Err(Error::Encode(e.to_string()));
                }
            }
        }
        self
    }

    pub async fn send(self) -> Result<Response> {
        let client = self.client.clone();
        client.execute(self).await
    }
}
