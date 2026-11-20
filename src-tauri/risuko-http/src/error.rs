use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid url: {0}")]
    Url(String),
    #[error("invalid header value: {0}")]
    Header(String),
    #[error("builder error: {0}")]
    Builder(String),
    #[error("connection error: {0}")]
    Connect(String),
    #[error("SOCKS5 required for UDP proxying")]
    Socks5Required,
    #[error("proxy authentication failed: {0}")]
    ProxyAuthentication(String),
    #[error("proxy protocol error: {0}")]
    ProxyProtocol(String),
    #[error("proxy timeout: {0}")]
    ProxyTimeout(String),
    #[error("request error: {0}")]
    Request(String),
    #[error("redirect error: {0}")]
    Redirect(String),
    #[error("body error: {0}")]
    Body(String),
    #[error("decode error: {0}")]
    Decode(String),
    #[error("encode error: {0}")]
    Encode(String),
    #[error("dns error: no records found")]
    NoRecords,
    #[error("timeout")]
    Timeout,
    #[error("HTTP status {0}")]
    Status(http::StatusCode),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls error: {0}")]
    Tls(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<url::ParseError> for Error {
    fn from(e: url::ParseError) -> Self {
        Error::Url(e.to_string())
    }
}

impl From<http::Error> for Error {
    fn from(e: http::Error) -> Self {
        Error::Builder(e.to_string())
    }
}

impl From<hyper::Error> for Error {
    fn from(e: hyper::Error) -> Self {
        Error::Request(e.to_string())
    }
}

impl From<hyper_util::client::legacy::Error> for Error {
    fn from(e: hyper_util::client::legacy::Error) -> Self {
        let mut message = e.to_string();
        let mut source = std::error::Error::source(&e);
        while let Some(cause) = source {
            if let Some(error) = cause.downcast_ref::<Error>() {
                return crate::connector::clone_route_error(error);
            }
            message.push_str(": ");
            message.push_str(&cause.to_string());
            source = cause.source();
        }
        Error::Request(message)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Decode(e.to_string())
    }
}

impl From<http::header::InvalidHeaderName> for Error {
    fn from(e: http::header::InvalidHeaderName) -> Self {
        Error::Header(e.to_string())
    }
}

impl From<http::header::InvalidHeaderValue> for Error {
    fn from(e: http::header::InvalidHeaderValue) -> Self {
        Error::Header(e.to_string())
    }
}
