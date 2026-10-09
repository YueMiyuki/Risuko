pub mod http;
pub mod obfuscation;
pub mod udp;

use std::future::Future;
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
    /// BEP 8 obfuscated announce, for HTTP trackers in `obfuscate-announce-list`
    pub obfuscate: bool,
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

/// Address family an announce is pinned to (BEP 7)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressFamily {
    V4,
    V6,
}

impl AddressFamily {
    pub fn matches(self, addr: &SocketAddr) -> bool {
        addr.is_ipv4() == (self == Self::V4)
    }

    /// Wildcard bind address of this family
    pub fn unspecified(self) -> SocketAddr {
        match self {
            Self::V4 => SocketAddr::from(([0, 0, 0, 0], 0)),
            Self::V6 => SocketAddr::from(([0u16; 8], 0)),
        }
    }
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
    announce_with_proxy_and_source(url, req, timeout, proxy, None).await
}

/// How long the slower family may still answer after the other succeeds
const FAMILY_ANNOUNCE_GRACE: Duration = Duration::from_secs(3);

/// BEP 7: without a fixed source address, announce once per address family and merge the answers; a configured source or a proxied route keeps a single announce
pub async fn announce_with_proxy_and_source(
    url: &str,
    req: &AnnounceRequest,
    timeout: Duration,
    proxy: Option<&risuko_http::ProxyConnector>,
    source: Option<SocketAddr>,
) -> Result<AnnounceResponse, TrackerError> {
    let proxied = proxy.is_some_and(|proxy| routes_via_proxy(url, proxy));
    if source.is_some() || proxied {
        return announce_once(url, req, timeout, proxy, source).await;
    }
    join_family_announces(
        announce_family(url, req, timeout, proxy, AddressFamily::V4),
        announce_family(url, req, timeout, proxy, AddressFamily::V6),
    )
    .await
}

/// Whether this tracker's route goes through the proxy; `no_proxy` matches are contacted direct
fn routes_via_proxy(url: &str, proxy: &risuko_http::ProxyConnector) -> bool {
    if url.starts_with("udp://") {
        udp::routes_via_proxy(url, proxy)
    } else {
        http::routes_via_proxy(url, proxy)
    }
}

/// Once one family succeeds the other only gets `FAMILY_ANNOUNCE_GRACE`; after a failure it keeps its own timeout
async fn join_family_announces(
    v4: impl Future<Output = Result<AnnounceResponse, TrackerError>>,
    v6: impl Future<Output = Result<AnnounceResponse, TrackerError>>,
) -> Result<AnnounceResponse, TrackerError> {
    async fn rest(
        first: &Result<AnnounceResponse, TrackerError>,
        other: impl Future<Output = Result<AnnounceResponse, TrackerError>>,
    ) -> Result<AnnounceResponse, TrackerError> {
        if first.is_err() {
            return other.await;
        }
        tokio::time::timeout(FAMILY_ANNOUNCE_GRACE, other)
            .await
            .unwrap_or(Err(TrackerError::Timeout))
    }

    tokio::pin!(v4, v6);
    tokio::select! {
        v4 = &mut v4 => {
            let v6 = rest(&v4, v6).await;
            merge_family_responses(v4, v6)
        }
        v6 = &mut v6 => {
            let v4 = rest(&v6, v4).await;
            merge_family_responses(v4, v6)
        }
    }
}

async fn announce_family(
    url: &str,
    req: &AnnounceRequest,
    timeout: Duration,
    proxy: Option<&risuko_http::ProxyConnector>,
    family: AddressFamily,
) -> Result<AnnounceResponse, TrackerError> {
    let attempt = async {
        if url.starts_with("http://") || url.starts_with("https://") {
            http::announce_for_family(url, req, family, proxy).await
        } else if url.starts_with("udp://") {
            udp::announce_for_family(url, req, family).await
        } else {
            Err(TrackerError::UnsupportedScheme(url.to_string()))
        }
    };
    tokio::time::timeout(timeout, attempt)
        .await
        .map_err(|_| TrackerError::Timeout)?
}

/// Union of both families' peers, the shorter interval and the larger counts; fails only when both fail
fn merge_family_responses(
    v4: Result<AnnounceResponse, TrackerError>,
    v6: Result<AnnounceResponse, TrackerError>,
) -> Result<AnnounceResponse, TrackerError> {
    match (v4, v6) {
        (Ok(mut a), Ok(b)) => {
            for peer in b.peers {
                if !a.peers.contains(&peer) {
                    a.peers.push(peer);
                }
            }
            a.interval = a.interval.min(b.interval);
            a.seeders = a.seeders.max(b.seeders);
            a.leechers = a.leechers.max(b.leechers);
            Ok(a)
        }
        (Ok(a), Err(_)) | (Err(_), Ok(a)) => Ok(a),
        (Err(e), Err(_)) => Err(e),
    }
}

async fn announce_once(
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
    use super::*;

    fn response(peers: &[&str], interval: u64, seeders: u32) -> AnnounceResponse {
        AnnounceResponse {
            interval: Duration::from_secs(interval),
            peers: peers.iter().map(|p| p.parse().unwrap()).collect(),
            seeders: Some(seeders),
            leechers: Some(1),
        }
    }

    #[tokio::test]
    async fn dual_stack_tracker_gets_one_announce_per_family() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let Ok(v4) = TcpListener::bind("127.0.0.1:0").await else {
            return;
        };
        let port = v4.local_addr().unwrap().port();
        let Ok(v6) = TcpListener::bind(("::1", port)).await else {
            return; // no IPv6 loopback here
        };
        let resolved: Vec<SocketAddr> = tokio::net::lookup_host(("localhost", port))
            .await
            .map(|addrs| addrs.collect())
            .unwrap_or_default();
        if !(resolved.iter().any(SocketAddr::is_ipv4) && resolved.iter().any(SocketAddr::is_ipv6)) {
            return; // `localhost` isn't dual-stack on this host
        }
        // Each family's tracker hands out a peer of its own family
        let serve = |listener: TcpListener, peers: Vec<u8>, key: &'static [u8]| async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = vec![0u8; 4096];
            let n = socket.read(&mut request).await.unwrap();
            let body = crate::bencode::encode_to_vec(&crate::bencode::Value::Dict(vec![
                (b"interval".to_vec(), crate::bencode::Value::Int(900)),
                (key.to_vec(), crate::bencode::Value::Bytes(peers)),
            ]));
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(head.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
            String::from_utf8_lossy(&request[..n]).contains("key=7")
        };
        let mut v6_peer = std::net::Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)
            .octets()
            .to_vec();
        v6_peer.extend_from_slice(&6882u16.to_be_bytes());
        let t4 = tokio::spawn(serve(v4, vec![198, 51, 100, 9, 0x1a, 0xe1], b"peers"));
        let t6 = tokio::spawn(serve(v6, v6_peer, b"peers6"));

        let req = AnnounceRequest {
            info_hash: Id20([3u8; 20]),
            peer_id: Id20([4u8; 20]),
            key: 7,
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 1,
            event: AnnounceEvent::Started,
            num_want: 50,
            obfuscate: false,
        };
        let url = format!("http://localhost:{port}/announce");
        let response =
            announce_with_proxy_and_source(&url, &req, Duration::from_secs(10), None, None)
                .await
                .unwrap();
        assert!(t4.await.unwrap(), "IPv4 announce carries the shared key");
        assert!(t6.await.unwrap(), "IPv6 announce carries the shared key");
        assert_eq!(response.peers.len(), 2);
        assert!(response.peers.iter().any(SocketAddr::is_ipv4));
        assert!(response.peers.iter().any(SocketAddr::is_ipv6));
    }

    #[test]
    fn family_announces_merge_peers_and_tolerate_one_failure() {
        let merged = merge_family_responses(
            Ok(response(&["198.51.100.1:1"], 1800, 3)),
            Ok(response(&["[2001:db8::1]:1", "198.51.100.1:1"], 900, 5)),
        )
        .unwrap();
        assert_eq!(merged.peers.len(), 2);
        assert_eq!(merged.interval, Duration::from_secs(900));
        assert_eq!(merged.seeders, Some(5));

        let only_v4 = merge_family_responses(
            Ok(response(&["198.51.100.1:1"], 60, 1)),
            Err(TrackerError::Timeout),
        )
        .unwrap();
        assert_eq!(only_v4.peers.len(), 1);
        assert!(
            merge_family_responses(Err(TrackerError::Timeout), Err(TrackerError::Timeout)).is_err()
        );
        assert!(AddressFamily::V6.matches(&"[::1]:1".parse().unwrap()));
        assert!(AddressFamily::V4.unspecified().ip().is_unspecified());
    }

    #[tokio::test(start_paused = true)]
    async fn one_family_success_waits_only_a_grace_period_for_the_other() {
        let started = tokio::time::Instant::now();
        let merged = join_family_announces(std::future::pending(), async {
            Ok(response(&["[2001:db8::1]:1"], 60, 1))
        })
        .await
        .unwrap();
        assert_eq!(merged.peers.len(), 1);
        assert_eq!(started.elapsed(), FAMILY_ANNOUNCE_GRACE);

        // A failed family leaves the other its own timeout
        let started = tokio::time::Instant::now();
        let slow = async {
            tokio::time::sleep(Duration::from_secs(20)).await;
            Ok(response(&["198.51.100.1:1"], 60, 1))
        };
        let merged = join_family_announces(slow, async { Err(TrackerError::Timeout) })
            .await
            .unwrap();
        assert_eq!(merged.peers.len(), 1);
        assert_eq!(started.elapsed(), Duration::from_secs(20));
    }

    #[test]
    fn only_a_proxied_route_keeps_a_single_announce() {
        use risuko_http::{Proxy, ProxyConnector};

        let proxy = ProxyConnector::from_proxy(
            Proxy::all("socks5://127.0.0.1:1080")
                .unwrap()
                .with_bypass("direct.example"),
        );
        assert!(routes_via_proxy("http://tracker.example/announce", &proxy));
        assert!(routes_via_proxy("udp://tracker.example:6969", &proxy));
        assert!(!routes_via_proxy("https://direct.example/announce", &proxy));
        assert!(!routes_via_proxy(
            "udp://tracker.direct.example:6969/announce",
            &proxy
        ));

        // A TCP-only proxy leaves UDP trackers direct
        let tcp_only = proxy.with_udp_proxy(Some(ProxyConnector::direct()));
        assert!(routes_via_proxy(
            "http://tracker.example/announce",
            &tcp_only
        ));
        assert!(!routes_via_proxy("udp://tracker.example:6969", &tcp_only));
        assert!(!routes_via_proxy(
            "http://tracker.example/announce",
            &ProxyConnector::direct()
        ));
    }

    #[test]
    fn rejects_broadcast_peer_endpoints() {
        assert!(!is_valid_endpoint("255.255.255.255:6881".parse().unwrap()));
        assert!(is_valid_endpoint("192.0.2.1:6881".parse().unwrap()));
    }
}
