pub mod http;
pub mod udp;

use std::net::SocketAddr;
use std::time::Duration;

use super::core::Id20;

/// Common "announce" parameters sent to every tracker
#[derive(Debug, Clone)]
pub struct AnnounceRequest {
    pub info_hash: Id20,
    pub peer_id: Id20,
    pub key: u32,
    pub port: u16,
    pub uploaded: u64,
    pub downloaded: u64,
    pub left: u64,
    pub event: AnnounceEvent,
    pub num_want: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnnounceEvent {
    None,
    Started,
    Stopped,
    Completed,
}

impl AnnounceEvent {
    pub fn as_str(self) -> &'static str {
        match self {
            AnnounceEvent::None => "",
            AnnounceEvent::Started => "started",
            AnnounceEvent::Stopped => "stopped",
            AnnounceEvent::Completed => "completed",
        }
    }
}

#[derive(Debug, Clone)]
pub struct AnnounceResponse {
    pub interval: Duration,
    pub peers: Vec<SocketAddr>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScrapeResponse {
    pub complete: u32,
    pub downloaded: u32,
    pub incomplete: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum TrackerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("http: {0}")]
    Http(String),
    #[error("bencode: {0}")]
    Bencode(#[from] super::bencode::Error),
    #[error("tracker rejected request: {0}")]
    Rejected(String),
    #[error("unsupported scheme: {0}")]
    UnsupportedScheme(String),
    #[error("timeout")]
    Timeout,
    #[error("url: {0}")]
    Url(String),
}

pub(crate) fn is_valid_endpoint(endpoint: SocketAddr) -> bool {
    !endpoint.ip().is_unspecified()
        && !endpoint.ip().is_multicast()
        && !matches!(endpoint, SocketAddr::V4(v4) if v4.ip().is_broadcast())
        && endpoint.port() != 0
}

/// Dispatch a single announce to a tracker URL; returns the parsed response
pub async fn announce(
    url: &str,
    req: &AnnounceRequest,
    timeout: Duration,
) -> Result<AnnounceResponse, TrackerError> {
    announce_with_proxy(url, req, timeout, None).await
}

pub async fn announce_with_proxy(
    url: &str,
    req: &AnnounceRequest,
    timeout: Duration,
    proxy: Option<&risuko_http::ProxyConnector>,
) -> Result<AnnounceResponse, TrackerError> {
    if url.starts_with("http://") || url.starts_with("https://") {
        tokio::time::timeout(timeout, http::announce_with_proxy(url, req, proxy))
            .await
            .map_err(|_| TrackerError::Timeout)?
    } else if url.starts_with("udp://") {
        tokio::time::timeout(timeout, udp::announce_with_proxy(url, req, proxy))
            .await
            .map_err(|_| TrackerError::Timeout)?
    } else {
        Err(TrackerError::UnsupportedScheme(url.to_string()))
    }
}

pub async fn announce_with_proxy_and_source(
    url: &str,
    req: &AnnounceRequest,
    timeout: Duration,
    proxy: Option<&risuko_http::ProxyConnector>,
    source: Option<SocketAddr>,
) -> Result<AnnounceResponse, TrackerError> {
    if url.starts_with("http://") || url.starts_with("https://") {
        tokio::time::timeout(
            timeout,
            http::announce_with_proxy_and_source(url, req, proxy, source),
        )
        .await
        .map_err(|_| TrackerError::Timeout)?
    } else if url.starts_with("udp://") {
        tokio::time::timeout(
            timeout,
            udp::announce_with_proxy_and_source(url, req, proxy, source),
        )
        .await
        .map_err(|_| TrackerError::Timeout)?
    } else {
        Err(TrackerError::UnsupportedScheme(url.to_string()))
    }
}

pub async fn scrape_udp(
    url: &str,
    info_hashes: &[Id20],
    timeout: Duration,
) -> Result<Vec<ScrapeResponse>, TrackerError> {
    tokio::time::timeout(timeout, udp::scrape(url, info_hashes, None))
        .await
        .map_err(|_| TrackerError::Timeout)?
}

pub async fn scrape_udp_with_proxy(
    url: &str,
    info_hashes: &[Id20],
    timeout: Duration,
    proxy: Option<&risuko_http::ProxyConnector>,
) -> Result<Vec<ScrapeResponse>, TrackerError> {
    tokio::time::timeout(
        timeout,
        udp::scrape_with_proxy(url, info_hashes, proxy, None),
    )
    .await
    .map_err(|_| TrackerError::Timeout)?
}

#[cfg(test)]
mod tests {
    use super::is_valid_endpoint;

    #[test]
    fn rejects_broadcast_peer_endpoints() {
        assert!(!is_valid_endpoint("255.255.255.255:6881".parse().unwrap()));
        assert!(is_valid_endpoint("192.0.2.1:6881".parse().unwrap()));
    }
}
