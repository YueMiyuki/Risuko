use std::sync::Arc;

use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Full};

use crate::error::Error;

pub type StreamBodyFactory = Arc<dyn Fn() -> BoxBody<Bytes, Error> + Send + Sync + 'static>;

#[derive(Clone)]
pub enum ReqBody {
    Empty,
    Bytes(Bytes),
    Stream {
        factory: StreamBodyFactory,
        content_length: Option<u64>,
    },
}

impl std::fmt::Debug for ReqBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReqBody::Empty => f.write_str("ReqBody::Empty"),
            ReqBody::Bytes(b) => f.debug_tuple("ReqBody::Bytes").field(&b.len()).finish(),
            ReqBody::Stream { content_length, .. } => f
                .debug_struct("ReqBody::Stream")
                .field("content_length", content_length)
                .finish(),
        }
    }
}

impl ReqBody {
    pub fn from_bytes(b: impl Into<Bytes>) -> Self {
        ReqBody::Bytes(b.into())
    }

    pub fn from_stream<F>(factory: F, content_length: Option<u64>) -> Self
    where
        F: Fn() -> BoxBody<Bytes, Error> + Send + Sync + 'static,
    {
        ReqBody::Stream {
            factory: Arc::new(factory),
            content_length,
        }
    }

    pub fn content_length(&self) -> Option<u64> {
        match self {
            ReqBody::Empty => Some(0),
            ReqBody::Bytes(b) => Some(b.len() as u64),
            ReqBody::Stream { content_length, .. } => *content_length,
        }
    }

    pub(crate) fn into_hyper_body(self) -> BoxBody<Bytes, Error> {
        match self {
            ReqBody::Empty => http_body_util::Empty::<Bytes>::new()
                .map_err(|never| match never {})
                .boxed(),
            ReqBody::Bytes(b) => Full::new(b).map_err(|never| match never {}).boxed(),
            ReqBody::Stream { factory, .. } => factory(),
        }
    }
}

pub type RespBody = http_body_util::combinators::BoxBody<Bytes, Error>;
