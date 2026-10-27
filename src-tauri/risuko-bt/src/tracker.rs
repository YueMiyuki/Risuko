pub mod http;
pub mod obfuscation;
pub mod udp;

use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use super::core::Id20;
use super::external_ip::{ExternalIp, Route, Source, Voter};

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
    pub obfuscate: bool,
    pub tracker_id: Option<Vec<u8>>,
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
    pub min_interval: Option<Duration>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub tracker_id: Option<Vec<u8>>,
    pub warning: Option<String>,
    pub external_ip: Option<IpAddr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryIn {
    Never,
    After(Duration),
}

#[derive(Debug, thiserror::Error)]
pub enum TrackerError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("http: {0}")]
    Http(String),
    #[error("bencode: {0}")]
    Bencode(#[from] super::bencode::Error),
    #[error("tracker rejected request: {reason}")]
    Rejected {
        reason: String,
        retry_in: Option<RetryIn>,
    },
    #[error("unsupported scheme: {0}")]
    UnsupportedScheme(String),
    #[error("timeout")]
    Timeout,
    #[error("url: {0}")]
    Url(String),
}

impl TrackerError {
    pub(crate) fn rejected(reason: impl Into<String>) -> Self {
        Self::Rejected {
            reason: reason.into(),
            retry_in: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressFamily {
    V4,
    V6,
}

impl AddressFamily {
    pub fn matches(self, addr: &SocketAddr) -> bool {
        addr.is_ipv4() == (self == Self::V4)
    }

    pub fn unspecified(self) -> SocketAddr {
        match self {
            Self::V4 => SocketAddr::from(([0, 0, 0, 0], 0)),
            Self::V6 => SocketAddr::from(([0u16; 8], 0)),
        }
    }
}

pub(crate) fn redacted_tracker_url(url: &str) -> String {
    let Ok(parsed) = url::Url::parse(url) else {
        return "<invalid-url>".to_string();
    };
    let Some(host) = parsed.host_str() else {
        return "<invalid-url>".to_string();
    };
    match parsed.port() {
        Some(port) => format!("{}://{host}:{port}", parsed.scheme()),
        None => format!("{}://{host}", parsed.scheme()),
    }
}

pub(crate) fn is_valid_endpoint(endpoint: SocketAddr) -> bool {
    !endpoint.ip().is_unspecified()
        && !endpoint.ip().is_multicast()
        && !matches!(endpoint, SocketAddr::V4(v4) if v4.ip().is_broadcast())
        && endpoint.port() != 0
}

pub async fn announce_with_proxy(
    url: &str,
    req: &AnnounceRequest,
    timeout: Duration,
    proxy: Option<&risuko_http::ProxyConnector>,
) -> Result<AnnounceResponse, TrackerError> {
    announce_with_proxy_and_source(url, req, timeout, proxy, None).await
}

const FAMILY_ANNOUNCE_GRACE: Duration = Duration::from_secs(5);

pub(crate) const STOPPED_ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(7);

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

fn routes_via_proxy(url: &str, proxy: &risuko_http::ProxyConnector) -> bool {
    if url.starts_with("udp://") {
        udp::routes_via_proxy(url, proxy)
    } else {
        http::routes_via_proxy(url, proxy)
    }
}

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
            Err(TrackerError::UnsupportedScheme(redacted_tracker_url(url)))
        }
    };
    let response = tokio::time::timeout(timeout, attempt)
        .await
        .map_err(|_| TrackerError::Timeout)??;
    note_external_ip(ExternalIp::shared(), url, proxy, &response);
    Ok(response)
}

fn tracker_voter(url: &str) -> Option<Voter> {
    let parsed = url::Url::parse(url).ok()?;
    let host = match parsed.host()? {
        url::Host::Domain(name) => url::Host::parse(name).ok()?,
        host => host.to_owned(),
    };
    if !super::webseed::is_allowed_host(&host) {
        return None;
    }
    match host {
        url::Host::Ipv4(ip) => Some(Voter::Ip(IpAddr::V4(ip))),
        url::Host::Ipv6(ip) => Some(Voter::Ip(IpAddr::V6(ip))),
        url::Host::Domain(_) => Some(Voter::Host),
    }
}

fn note_external_ip(
    external: &ExternalIp,
    url: &str,
    proxy: Option<&risuko_http::ProxyConnector>,
    response: &AnnounceResponse,
) {
    let Some(observed) = response.external_ip else {
        return;
    };
    let route = if proxy.is_some_and(|proxy| routes_via_proxy(url, proxy)) {
        Route::Proxied
    } else {
        Route::Direct
    };
    if let Some(voter) = tracker_voter(url) {
        external.vote(observed, Source::Tracker, voter, route);
    }
}

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
            a.min_interval = a.min_interval.max(b.min_interval);
            a.seeders = a.seeders.max(b.seeders);
            a.leechers = a.leechers.max(b.leechers);
            a.tracker_id = a.tracker_id.or(b.tracker_id);
            a.warning = a.warning.or(b.warning);
            a.external_ip = a.external_ip.or(b.external_ip);
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
    let attempt = async {
        if url.starts_with("http://") || url.starts_with("https://") {
            http::announce_with_proxy_and_source(url, req, proxy, source).await
        } else if url.starts_with("udp://") {
            udp::announce_with_proxy_and_source(url, req, proxy, source).await
        } else {
            Err(TrackerError::UnsupportedScheme(redacted_tracker_url(url)))
        }
    };
    let response = tokio::time::timeout(timeout, attempt)
        .await
        .map_err(|_| TrackerError::Timeout)??;
    note_external_ip(ExternalIp::shared(), url, proxy, &response);
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(peers: &[&str], interval: u64, seeders: u32) -> AnnounceResponse {
        AnnounceResponse {
            interval: Duration::from_secs(interval),
            peers: peers.iter().map(|p| p.parse().unwrap()).collect(),
            min_interval: None,
            seeders: Some(seeders),
            leechers: Some(1),
            tracker_id: None,
            warning: None,
            external_ip: None,
        }
    }

    #[tokio::test]
    async fn tracker_errors_never_embed_the_announce_url() {
        let secret = "SECRETPASSKEY";
        let mut req = AnnounceRequest {
            info_hash: Id20([3u8; 20]),
            peer_id: Id20([4u8; 20]),
            key: 7,
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 0,
            event: AnnounceEvent::None,
            num_want: 50,
            obfuscate: false,
            tracker_id: None,
        };
        let urls = [
            format!("ws://t.example/{secret}/announce?passkey={secret}"),
            format!("udp://t.example/{secret}/announce?passkey={secret}"),
            format!("udp://t.example:port/{secret}?passkey={secret}"),
            format!("udp://t.example:0/{secret}?passkey={secret}"),
            format!("udp://:6969/{secret}?passkey={secret}"),
        ];
        let sources = [None, Some(SocketAddr::from(([127, 0, 0, 1], 0)))];
        for obfuscate in [false, true] {
            req.obfuscate = obfuscate;
            for url in &urls {
                for source in sources {
                    let err = announce_with_proxy_and_source(
                        url,
                        &req,
                        Duration::from_secs(5),
                        None,
                        source,
                    )
                    .await
                    .unwrap_err();
                    let shown = err.to_string();
                    assert!(
                        !shown.contains(secret) && !shown.contains("passkey"),
                        "{url} leaked into: {shown}"
                    );
                }
            }
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
            return;
        };
        let resolved: Vec<SocketAddr> = tokio::net::lookup_host(("localhost", port))
            .await
            .map(|addrs| addrs.collect())
            .unwrap_or_default();
        if !(resolved.iter().any(SocketAddr::is_ipv4) && resolved.iter().any(SocketAddr::is_ipv6)) {
            return;
        }
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
            tracker_id: None,
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
    async fn slower_family_inside_the_grace_still_contributes_its_peers() {
        let slow_v6 = async {
            tokio::time::sleep(FAMILY_ANNOUNCE_GRACE - Duration::from_secs(1)).await;
            Ok(response(&["[2001:db8::1]:1"], 60, 1))
        };
        let merged =
            join_family_announces(async { Ok(response(&["198.51.100.1:1"], 60, 1)) }, slow_v6)
                .await
                .unwrap();
        assert_eq!(merged.peers.len(), 2);
    }

    #[test]
    fn merged_family_responses_keep_the_first_tracker_id_and_warning() {
        let mut v4 = response(&["198.51.100.1:1"], 60, 1);
        let mut v6 = response(&["[2001:db8::1]:1"], 60, 1);
        v6.tracker_id = Some(b"six".to_vec());
        v6.warning = Some("slow".into());
        let merged = merge_family_responses(Ok(v4.clone()), Ok(v6.clone())).unwrap();
        assert_eq!(merged.tracker_id, Some(b"six".to_vec()));
        assert_eq!(merged.warning.as_deref(), Some("slow"));
        v4.tracker_id = Some(b"four".to_vec());
        let merged = merge_family_responses(Ok(v4), Ok(v6)).unwrap();
        assert_eq!(merged.tracker_id, Some(b"four".to_vec()));
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

    fn reporting(ip: &str) -> AnnounceResponse {
        AnnounceResponse {
            external_ip: Some(ip.parse().unwrap()),
            ..response(&[], 60, 1)
        }
    }

    #[test]
    fn tracker_reported_addresses_vote_once_per_tracker() {
        let external = ExternalIp::new();
        let reported = reporting("93.184.216.34");
        for _ in 0..8 {
            note_external_ip(&external, "http://one.example/announce", None, &reported);
            note_external_ip(&external, "https://ONE.example:8443/a?k=1", None, &reported);
        }
        assert_eq!(external.external_v4(), None);
        note_external_ip(&external, "http://8.8.4.4:6969/announce", None, &reported);
        note_external_ip(&external, "http://8.8.4.4:6970/announce", None, &reported);
        assert_eq!(external.external_v4(), None);
        note_external_ip(&external, "http://1.1.1.1:6969/announce", None, &reported);
        assert_eq!(
            external.external_v4(),
            Some("93.184.216.34".parse().unwrap())
        );
    }

    #[test]
    fn a_farm_of_tracker_hostnames_cannot_elect_an_address_alone() {
        let external = ExternalIp::new();
        let reported = reporting("93.184.216.34");
        for index in 0..100 {
            let url = format!("http://t{index}.evil.example/announce");
            note_external_ip(&external, &url, None, &reported);
        }
        assert_eq!(external.external_v4(), None);
        note_external_ip(&external, "http://8.8.4.4:6969/announce", None, &reported);
        assert_eq!(external.external_v4(), None);
        note_external_ip(&external, "http://1.1.1.1:6969/announce", None, &reported);
        assert_eq!(
            external.external_v4(),
            Some("93.184.216.34".parse().unwrap())
        );
    }

    #[test]
    fn trackers_on_private_or_internal_hosts_do_not_vote() {
        let external = ExternalIp::new();
        let reported = reporting("93.184.216.34");
        for url in [
            "http://127.0.0.1:6969/announce",
            "http://10.0.0.5/announce",
            "http://192.168.1.2:8080/announce",
            "http://[::1]:6969/announce",
            "http://localhost:6969/announce",
            "http://tracker.local/announce",
            "http://tracker.internal/announce",
            "http://singlelabel/announce",
            "not a url",
        ] {
            note_external_ip(&external, url, None, &reported);
        }
        for url in [
            "http://a.example/announce",
            "http://b.example/announce",
            "http://c.example/announce",
        ] {
            let before = external.external_v4();
            note_external_ip(&external, url, None, &response(&[], 60, 1));
            assert_eq!(external.external_v4(), before);
        }
        assert_eq!(external.addresses(), Default::default());
    }

    #[test]
    fn udp_tracker_hosts_are_read_like_http_ones() {
        let ip = |raw: &str| Some(Voter::Ip(raw.parse().unwrap()));
        assert_eq!(tracker_voter("udp://8.8.4.4:6969/announce"), ip("8.8.4.4"));
        assert_eq!(
            tracker_voter("udp://[2001:4860::8888]:6969/announce"),
            ip("2001:4860::8888")
        );
        assert_eq!(
            tracker_voter("udp://Tracker.Example:6969/announce"),
            Some(Voter::Host)
        );
        for url in [
            "udp://127.0.0.1:6969/announce",
            "udp://192.168.1.2:6969",
            "udp://0x7f.1:6969",
            "udp://2130706433:6969",
            "udp://[::1]:6969",
            "udp://LOCALHOST:6969",
            "udp://tracker.LOCAL:6969",
        ] {
            assert_eq!(tracker_voter(url), None, "{url}");
        }

        let external = ExternalIp::new();
        let reported = reporting("93.184.216.34");
        for url in [
            "udp://8.8.4.4:6969/announce",
            "udp://1.1.1.1:6969/announce",
            "udp://9.9.9.9:6969/announce",
        ] {
            note_external_ip(&external, url, None, &reported);
        }
        assert_eq!(
            external.external_v4(),
            Some("93.184.216.34".parse().unwrap())
        );
    }

    #[test]
    fn proxied_tracker_announces_never_vote() {
        use risuko_http::{Proxy, ProxyConnector};

        let external = ExternalIp::new();
        let reported = reporting("93.184.216.34");
        let proxy = ProxyConnector::from_proxy(Proxy::all("socks5://127.0.0.1:1080").unwrap());
        for url in [
            "http://8.8.4.4/announce",
            "http://1.1.1.1/announce",
            "http://9.9.9.9/announce",
            "http://151.101.1.69/announce",
        ] {
            note_external_ip(&external, url, Some(&proxy), &reported);
        }
        assert_eq!(external.addresses(), Default::default());
        for url in [
            "http://8.8.4.4/announce",
            "http://1.1.1.1/announce",
            "http://9.9.9.9/announce",
        ] {
            note_external_ip(&external, url, Some(&ProxyConnector::direct()), &reported);
        }
        assert_eq!(
            external.external_v4(),
            Some("93.184.216.34".parse().unwrap())
        );
    }

    #[test]
    fn trackers_that_bypass_the_proxy_still_vote() {
        use risuko_http::{Proxy, ProxyConnector};

        let external = ExternalIp::new();
        let reported = reporting("93.184.216.34");
        let proxy = ProxyConnector::from_proxy(
            Proxy::all("socks5://127.0.0.1:1080")
                .unwrap()
                .with_bypass("8.8.4.4,1.1.1.1,9.9.9.9"),
        );
        for url in [
            "http://8.8.4.4/announce",
            "http://1.1.1.1/announce",
            "http://9.9.9.9/announce",
        ] {
            note_external_ip(&external, url, Some(&proxy), &reported);
        }
        assert_eq!(
            external.external_v4(),
            Some("93.184.216.34".parse().unwrap())
        );
    }

    #[test]
    fn dual_stack_merge_keeps_an_external_ip() {
        let v4 = reporting("93.184.216.34");
        let v6 = reporting("2a00:1450:4001:80b::200e");
        let merged = merge_family_responses(Ok(v4.clone()), Ok(v6.clone())).unwrap();
        assert_eq!(merged.external_ip, v4.external_ip);
        let merged = merge_family_responses(Ok(response(&[], 60, 1)), Ok(v6.clone())).unwrap();
        assert_eq!(merged.external_ip, v6.external_ip);
    }

    #[test]
    fn redacted_tracker_urls_keep_only_scheme_host_and_port() {
        assert_eq!(
            redacted_tracker_url("https://t.example:8443/AbC123passkey/announce?passkey=zzz"),
            "https://t.example:8443"
        );
        assert_eq!(
            redacted_tracker_url("udp://tracker.example:6969/announce"),
            "udp://tracker.example:6969"
        );
        assert_eq!(
            redacted_tracker_url("http://user:secret@[2001:db8::1]/x?k=v"),
            "http://[2001:db8::1]"
        );
        assert_eq!(redacted_tracker_url("AbC123passkey"), "<invalid-url>");
    }

    #[test]
    fn rejects_broadcast_peer_endpoints() {
        assert!(!is_valid_endpoint("255.255.255.255:6881".parse().unwrap()));
        assert!(is_valid_endpoint("192.0.2.1:6881".parse().unwrap()));
    }
}
