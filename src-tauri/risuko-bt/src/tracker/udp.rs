use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use rand::RngExt;
use risuko_http::ProxyDatagramSource;
use tokio::net::{lookup_host, UdpSocket};
use tokio::task::JoinSet;
use tokio::time::{timeout, timeout_at, Instant};

use super::{is_valid_endpoint, AnnounceRequest, AnnounceResponse, TrackerError};

mod be {
    pub fn read_u32(b: &[u8]) -> u32 {
        u32::from_be_bytes(b[..4].try_into().unwrap())
    }
    pub fn read_u64(b: &[u8]) -> u64 {
        u64::from_be_bytes(b[..8].try_into().unwrap())
    }
    pub fn write_u16(b: &mut [u8], v: u16) {
        b[..2].copy_from_slice(&v.to_be_bytes());
    }
    pub fn write_u32(b: &mut [u8], v: u32) {
        b[..4].copy_from_slice(&v.to_be_bytes());
    }
    pub fn write_u64(b: &mut [u8], v: u64) {
        b[..8].copy_from_slice(&v.to_be_bytes());
    }
}

const PROTOCOL_ID: u64 = 0x41727101980;
const ACTION_CONNECT: u32 = 0;
const ACTION_ANNOUNCE: u32 = 1;
const ACTION_ERROR: u32 = 3;
const RETRANSMIT_ATTEMPTS: u32 = 4;
const MAX_ENDPOINTS_PER_FAMILY: usize = 4;
const ENDPOINT_STAGGER: Duration = Duration::from_millis(250);
const ENDPOINT_FAILOVER: Duration = Duration::from_secs(6);
const RECV_BUFFER_LEN: usize = 2048;

fn retransmit_timeout(attempt: u32) -> Duration {
    Duration::from_secs(3u64 << attempt.min(3))
}

pub async fn announce_with_proxy_and_source(
    url: &str,
    req: &AnnounceRequest,
    proxy: Option<&risuko_http::ProxyConnector>,
    source: Option<SocketAddr>,
) -> Result<AnnounceResponse, TrackerError> {
    reject_obfuscation(req, url)?;
    let (host, port) = parse_udp_url(url)?;
    let url_data: Arc<[u8]> = udp_url_data(url).into();
    let bypasses_proxy = proxy.is_some_and(|proxy| is_bypassed(proxy, &host, port));
    let source_is_concrete = source.is_some_and(|source| !source.ip().is_unspecified());
    if let Some(proxy) =
        proxy.filter(|proxy| proxy.has_proxy() && !(source_is_concrete && bypasses_proxy))
    {
        let socket = proxy
            .bind_udp_with_bypass()
            .await
            .map_err(|e| TrackerError::Http(e.to_string()))?;
        return announce_endpoint_proxy(socket, &host, port, req, &url_data).await;
    }
    let targets = dedupe_endpoints(lookup_host((host.as_str(), port)).await?)
        .into_iter()
        .filter(|target| {
            source.is_none_or(|source| {
                source.ip().is_unspecified() || source.is_ipv4() == target.is_ipv4()
            })
        })
        .collect::<Vec<_>>();
    race_endpoints(targets, &host, req, source, url_data).await
}

pub async fn announce_for_family(
    url: &str,
    req: &AnnounceRequest,
    family: super::AddressFamily,
) -> Result<AnnounceResponse, TrackerError> {
    reject_obfuscation(req, url)?;
    let (host, port) = parse_udp_url(url)?;
    let url_data: Arc<[u8]> = udp_url_data(url).into();
    let targets = dedupe_endpoints(lookup_host((host.as_str(), port)).await?)
        .into_iter()
        .filter(|target| family.matches(target))
        .collect::<Vec<_>>();
    race_endpoints(targets, &host, req, None, url_data).await
}

pub(super) fn routes_via_proxy(url: &str, proxy: &risuko_http::ProxyConnector) -> bool {
    proxy.udp_proxy().is_some()
        && !parse_udp_url(url).is_ok_and(|(host, port)| is_bypassed(proxy, &host, port))
}

fn is_bypassed(proxy: &risuko_http::ProxyConnector, host: &str, port: u16) -> bool {
    proxy
        .udp_no_proxy()
        .or_else(|| proxy.no_proxy())
        .is_some_and(|no_proxy| no_proxy.matches_host_port(host, Some(port)))
}

fn reject_obfuscation(req: &AnnounceRequest, url: &str) -> Result<(), TrackerError> {
    if req.obfuscate {
        return Err(TrackerError::UnsupportedScheme(format!(
            "{}: BEP 8 obfuscation needs an HTTP tracker",
            super::redacted_tracker_url(url)
        )));
    }
    Ok(())
}

async fn race_endpoints(
    targets: Vec<SocketAddr>,
    host: &str,
    req: &AnnounceRequest,
    source: Option<SocketAddr>,
    url_data: Arc<[u8]>,
) -> Result<AnnounceResponse, TrackerError> {
    race_endpoints_staggered(
        targets,
        host,
        req,
        source,
        url_data,
        ENDPOINT_STAGGER,
        ENDPOINT_FAILOVER,
    )
    .await
}

async fn race_endpoints_staggered(
    targets: Vec<SocketAddr>,
    host: &str,
    req: &AnnounceRequest,
    source: Option<SocketAddr>,
    url_data: Arc<[u8]>,
    stagger: Duration,
    failover: Duration,
) -> Result<AnnounceResponse, TrackerError> {
    if targets.is_empty() {
        return Err(TrackerError::Url(format!("no DNS result for {host}")));
    }
    let mut pending = limit_per_family(targets).into_iter();
    let mut attempts = JoinSet::new();
    let mut last_error = None;
    let mut give_up = None;
    loop {
        if attempts.is_empty() {
            let Some(target) = pending.next() else { break };
            attempts.spawn(connect_endpoint(target, source));
        }
        let result = tokio::select! {
            result = attempts.join_next() => result,
            _ = tokio::time::sleep(stagger), if !pending.as_slice().is_empty() => {
                if let Some(target) = pending.next() {
                    attempts.spawn(connect_endpoint(target, source));
                }
                continue;
            }
            _ = tokio::time::sleep_until(give_up.unwrap_or_else(Instant::now)), if give_up.is_some() => break,
        };
        match result {
            Some(Ok(Ok((sock, conn_id)))) => {
                let transport = Direct(&sock);
                let announcing = announce(&transport, conn_id, req, &url_data);
                let outcome = if attempts.is_empty() && pending.as_slice().is_empty() {
                    announcing.await
                } else {
                    timeout(failover, announcing)
                        .await
                        .unwrap_or(Err(TrackerError::Timeout))
                };
                match outcome {
                    Ok(response) => return Ok(response),
                    Err(error) => {
                        keep_tracker_answer(&mut last_error, error);
                        give_up = Some(Instant::now() + failover);
                    }
                }
            }
            Some(Ok(Err(error))) => keep_tracker_answer(&mut last_error, error),
            Some(Err(error)) if error.is_cancelled() => {}
            Some(Err(error)) => keep_tracker_answer(
                &mut last_error,
                TrackerError::Io(std::io::Error::other(format!(
                    "UDP tracker endpoint task failed: {error}"
                ))),
            ),
            None => {}
        }
    }
    Err(last_error
        .unwrap_or_else(|| TrackerError::Url(format!("no usable DNS endpoint for {host}"))))
}

fn keep_tracker_answer(last: &mut Option<TrackerError>, error: TrackerError) {
    if !matches!(last, Some(TrackerError::Rejected { .. })) {
        *last = Some(error);
    }
}

fn limit_per_family(targets: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let (mut v4, mut v6) = (0usize, 0usize);
    targets
        .into_iter()
        .filter(|target| {
            let taken = if target.is_ipv4() { &mut v4 } else { &mut v6 };
            *taken += 1;
            *taken <= MAX_ENDPOINTS_PER_FAMILY
        })
        .collect()
}

fn dedupe_endpoints(endpoints: impl IntoIterator<Item = SocketAddr>) -> Vec<SocketAddr> {
    let mut seen = HashSet::new();
    endpoints
        .into_iter()
        .filter(|endpoint| is_valid_endpoint(*endpoint) && seen.insert(*endpoint))
        .collect()
}

async fn connect_endpoint(
    target: SocketAddr,
    source: Option<SocketAddr>,
) -> Result<(UdpSocket, u64), TrackerError> {
    let sock = UdpSocket::bind(direct_bind_addr(target, source)).await?;
    sock.connect(target).await?;
    let conn_id = connect(&Direct(&sock)).await?;
    Ok((sock, conn_id))
}

fn direct_bind_addr(target: SocketAddr, source: Option<SocketAddr>) -> SocketAddr {
    let compatible = source
        .filter(|source| !source.ip().is_unspecified() && source.is_ipv4() == target.is_ipv4());
    compatible
        .map(|mut source| {
            source.set_port(0);
            source
        })
        .unwrap_or_else(|| {
            if target.is_ipv6() {
                SocketAddr::from(([0u16; 8], 0))
            } else {
                SocketAddr::from(([0, 0, 0, 0], 0))
            }
        })
}

async fn announce_endpoint_proxy(
    sock: risuko_http::ProxyDatagram,
    host: &str,
    port: u16,
    req: &AnnounceRequest,
    url_data: &[u8],
) -> Result<AnnounceResponse, TrackerError> {
    let transport = Relayed {
        socket: &sock,
        host,
        port,
    };
    let conn_id = connect(&transport).await?;
    announce(&transport, conn_id, req, url_data).await
}

trait Datagrams {
    fn family_is_ipv6(&self) -> Option<bool>;
    async fn send(&self, payload: &[u8]) -> Result<(), TrackerError>;
    async fn recv(&self, buf: &mut [u8]) -> Option<(usize, Option<ProxyDatagramSource>)>;
}

struct Direct<'a>(&'a UdpSocket);

impl Datagrams for Direct<'_> {
    fn family_is_ipv6(&self) -> Option<bool> {
        self.0.peer_addr().ok().map(|peer| peer.is_ipv6())
    }

    async fn send(&self, payload: &[u8]) -> Result<(), TrackerError> {
        self.0.send(payload).await?;
        Ok(())
    }

    async fn recv(&self, buf: &mut [u8]) -> Option<(usize, Option<ProxyDatagramSource>)> {
        self.0.recv(buf).await.ok().map(|n| (n, None))
    }
}

struct Relayed<'a> {
    socket: &'a risuko_http::ProxyDatagram,
    host: &'a str,
    port: u16,
}

impl Datagrams for Relayed<'_> {
    fn family_is_ipv6(&self) -> Option<bool> {
        self.host.parse::<IpAddr>().ok().map(|ip| ip.is_ipv6())
    }

    async fn send(&self, payload: &[u8]) -> Result<(), TrackerError> {
        self.socket
            .send_to_host(payload, self.host, self.port)
            .await
            .map_err(|e| TrackerError::Http(e.to_string()))?;
        Ok(())
    }

    async fn recv(&self, buf: &mut [u8]) -> Option<(usize, Option<ProxyDatagramSource>)> {
        loop {
            let (n, from) = self.socket.recv_from_target(buf).await.ok()?;
            if reply_source_matches(self.host, self.port, &from) {
                return Some((n, Some(from)));
            }
        }
    }
}

fn reply_source_matches(host: &str, port: u16, from: &ProxyDatagramSource) -> bool {
    let expected = host.parse::<IpAddr>().ok();
    match from {
        ProxyDatagramSource::Ip(address) => {
            address.ip().is_unspecified()
                || address.port() == 0
                || (address.port() == port
                    && expected.is_none_or(|ip| ip.to_canonical() == address.ip().to_canonical()))
        }
        ProxyDatagramSource::Host(name, from_port) => {
            *from_port == port
                && match (expected, name.parse::<IpAddr>()) {
                    (Some(ip), Ok(actual)) => ip.to_canonical() == actual.to_canonical(),
                    (None, Err(_)) => name
                        .trim_end_matches('.')
                        .eq_ignore_ascii_case(host.trim_end_matches('.')),
                    _ => true,
                }
        }
    }
}

async fn exchange<T: Datagrams>(
    transport: &T,
    payload: &[u8],
    action: u32,
    txn: u32,
    buf: &mut [u8],
    accept: impl Fn(usize, Option<&ProxyDatagramSource>) -> bool,
) -> Result<(usize, Option<ProxyDatagramSource>), TrackerError> {
    for attempt in 0..RETRANSMIT_ATTEMPTS {
        transport.send(payload).await?;
        let deadline = Instant::now() + retransmit_timeout(attempt);
        while let Ok(Some((n, from))) = timeout_at(deadline, transport.recv(buf)).await {
            if n < 8 || be::read_u32(&buf[4..8]) != txn {
                continue;
            }
            let reply_action = be::read_u32(&buf[0..4]);
            if reply_action == ACTION_ERROR {
                return Err(TrackerError::rejected(read_error(&buf[8..n])));
            }
            if reply_action == action && accept(n, from.as_ref()) {
                return Ok((n, from));
            }
        }
    }
    Err(TrackerError::Timeout)
}

async fn connect<T: Datagrams>(transport: &T) -> Result<u64, TrackerError> {
    let txn = rand::rng().random::<u32>();
    let mut req = [0u8; 16];
    be::write_u64(&mut req[0..8], PROTOCOL_ID);
    be::write_u32(&mut req[8..12], ACTION_CONNECT);
    be::write_u32(&mut req[12..16], txn);
    let mut buf = [0u8; RECV_BUFFER_LEN];
    exchange(transport, &req, ACTION_CONNECT, txn, &mut buf, |n, _| {
        n >= 16
    })
    .await?;
    Ok(be::read_u64(&buf[8..16]))
}

async fn announce<T: Datagrams>(
    transport: &T,
    conn_id: u64,
    req: &AnnounceRequest,
    url_data: &[u8],
) -> Result<AnnounceResponse, TrackerError> {
    let (body, txn) = build_announce_body(conn_id, req, url_data);
    let family = transport.family_is_ipv6();
    let mut buf = vec![0u8; announce_response_buffer_len(family != Some(false), req.num_want)];
    let (n, from) = exchange(
        transport,
        &body,
        ACTION_ANNOUNCE,
        txn,
        &mut buf,
        |n, from| n >= 20 && (n - 20) % peer_stride(reply_is_ipv6(family, from)) == 0,
    )
    .await?;
    if n >= buf.len().min(MAX_UDP_PAYLOAD) {
        tracing::warn!("UDP tracker announce response may be truncated");
    }
    Ok(parse_announce_response(
        &buf[..n],
        reply_is_ipv6(family, from.as_ref()),
    ))
}

fn reply_is_ipv6(family: Option<bool>, from: Option<&ProxyDatagramSource>) -> bool {
    family.unwrap_or_else(
        || matches!(from, Some(ProxyDatagramSource::Ip(address)) if address.is_ipv6()),
    )
}

fn peer_stride(is_ipv6: bool) -> usize {
    if is_ipv6 {
        18
    } else {
        6
    }
}

fn announce_response_buffer_len(is_ipv6: bool, num_want: u32) -> usize {
    const HEADER: usize = 20;
    const MIN_BUFFER_LEN: usize = 2048;
    HEADER
        .saturating_add(peer_stride(is_ipv6).saturating_mul(num_want as usize))
        .clamp(MIN_BUFFER_LEN, MAX_UDP_PAYLOAD)
}

const MAX_UDP_PAYLOAD: usize = 65_507;

const OPTION_URL_DATA: u8 = 0x2;

fn build_announce_body(conn_id: u64, req: &AnnounceRequest, url_data: &[u8]) -> (Vec<u8>, u32) {
    let txn = rand::rng().random::<u32>();
    let mut body = vec![0u8; 98];
    be::write_u64(&mut body[0..8], conn_id);
    be::write_u32(&mut body[8..12], ACTION_ANNOUNCE);
    be::write_u32(&mut body[12..16], txn);
    body[16..36].copy_from_slice(req.info_hash.as_bytes());
    body[36..56].copy_from_slice(req.peer_id.as_bytes());
    be::write_u64(&mut body[56..64], req.downloaded);
    be::write_u64(&mut body[64..72], req.left);
    be::write_u64(&mut body[72..80], req.uploaded);
    be::write_u32(&mut body[80..84], event_code(req));
    be::write_u32(&mut body[84..88], 0);
    be::write_u32(&mut body[88..92], req.key);
    be::write_u32(&mut body[92..96], req.num_want);
    be::write_u16(&mut body[96..98], req.port);
    for chunk in url_data.chunks(u8::MAX as usize) {
        body.push(OPTION_URL_DATA);
        body.push(chunk.len() as u8);
        body.extend_from_slice(chunk);
    }
    (body, txn)
}

fn udp_url_data(url: &str) -> Vec<u8> {
    let Some(rest) = url.strip_prefix("udp://") else {
        return Vec::new();
    };
    let rest = rest.split('#').next().unwrap_or(rest);
    match rest.find(['/', '?']) {
        Some(start) => rest.as_bytes()[start..].to_vec(),
        None => Vec::new(),
    }
}

fn parse_announce_response(buf: &[u8], is_ipv6: bool) -> AnnounceResponse {
    let interval = be::read_u32(&buf[8..12]).max(1) as u64;
    let leechers = be::read_u32(&buf[12..16]);
    let seeders = be::read_u32(&buf[16..20]);
    let mut peers = Vec::new();
    if is_ipv6 {
        for chunk in buf[20..].chunks_exact(18) {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&chunk[0..16]);
            let ip = std::net::Ipv6Addr::from(octets);
            let port = u16::from_be_bytes([chunk[16], chunk[17]]);
            let endpoint = SocketAddr::new(IpAddr::V6(ip), port);
            if is_valid_endpoint(endpoint) {
                peers.push(endpoint);
            }
        }
    } else {
        for chunk in buf[20..].chunks_exact(6) {
            let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            let endpoint = SocketAddr::new(IpAddr::V4(ip), port);
            if is_valid_endpoint(endpoint) {
                peers.push(endpoint);
            }
        }
    }
    AnnounceResponse {
        interval: Duration::from_secs(interval),
        peers,
        min_interval: None,
        seeders: Some(seeders),
        leechers: Some(leechers),
        tracker_id: None,
        warning: None,
        external_ip: None,
    }
}

fn event_code(req: &AnnounceRequest) -> u32 {
    match req.event {
        super::AnnounceEvent::None => 0,
        super::AnnounceEvent::Completed => 1,
        super::AnnounceEvent::Started => 2,
        super::AnnounceEvent::Stopped => 3,
    }
}

fn read_error(tail: &[u8]) -> String {
    String::from_utf8_lossy(tail).to_string()
}

fn parse_udp_url(url: &str) -> Result<(String, u16), TrackerError> {
    let shown = || super::redacted_tracker_url(url);
    let rest = url
        .strip_prefix("udp://")
        .ok_or_else(|| TrackerError::UnsupportedScheme(shown()))?;
    let rest = rest.split('/').next().unwrap_or(rest);
    let rest = rest.split('?').next().unwrap_or(rest);
    let (host, port) = match rest.rsplit_once(':') {
        Some((h, p)) => (
            h.trim_start_matches('[').trim_end_matches(']').to_string(),
            p.parse::<u16>()
                .map_err(|_| TrackerError::Url(format!("bad port in {}", shown())))?,
        ),
        None => return Err(TrackerError::Url(format!("missing port in {}", shown()))),
    };
    if host.is_empty() || port == 0 {
        return Err(TrackerError::Url(format!(
            "invalid endpoint in {}",
            shown()
        )));
    }
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_data_option_carries_path_and_query() {
        assert_eq!(
            udp_url_data("udp://tracker.example.com:80/dir?a=b&c=d#frag"),
            b"/dir?a=b&c=d"
        );
        assert_eq!(udp_url_data("udp://tracker.example.com:80?k=1"), b"?k=1");
        assert!(udp_url_data("udp://[::1]:2710").is_empty());

        use crate::core::Id20;
        let req = AnnounceRequest {
            info_hash: Id20([1u8; 20]),
            peer_id: Id20([2u8; 20]),
            key: 7,
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 0,
            event: super::super::AnnounceEvent::None,
            num_want: 50,
            obfuscate: false,
            tracker_id: None,
        };
        let (plain, _) = build_announce_body(1, &req, b"");
        assert_eq!(plain.len(), 98);
        let long = vec![b'x'; 300];
        let (body, _) = build_announce_body(1, &req, &long);
        assert_eq!(&body[98..100], &[OPTION_URL_DATA, 255]);
        assert_eq!(&body[355..357], &[OPTION_URL_DATA, 45]);
        assert_eq!(body.len(), 98 + 2 + 255 + 2 + 45);
    }

    #[test]
    fn parses_udp_url() {
        assert_eq!(
            parse_udp_url("udp://tracker.example.com:1337/announce").unwrap(),
            ("tracker.example.com".to_string(), 1337)
        );
        assert_eq!(
            parse_udp_url("udp://[::1]:2710").unwrap(),
            ("::1".to_string(), 2710)
        );
    }

    #[test]
    fn dns_endpoints_are_deduplicated_without_reordering() {
        let v4: SocketAddr = "192.0.2.10:80".parse().unwrap();
        let v6: SocketAddr = "[2001:db8::10]:80".parse().unwrap();
        let other: SocketAddr = "192.0.2.11:80".parse().unwrap();
        assert_eq!(
            dedupe_endpoints([v4, v6, v4, other, v6]),
            vec![v4, v6, other]
        );
    }

    #[test]
    fn rejects_wrong_scheme() {
        assert!(parse_udp_url("http://x:1").is_err());
        assert!(parse_udp_url("udp://tracker.example:0").is_err());
    }

    #[test]
    fn rejects_invalid_discovered_endpoints() {
        let valid: SocketAddr = "192.0.2.1:80".parse().unwrap();
        let unspecified: SocketAddr = "0.0.0.0:80".parse().unwrap();
        let multicast: SocketAddr = "224.0.0.1:80".parse().unwrap();
        let zero_port: SocketAddr = "192.0.2.2:0".parse().unwrap();
        assert_eq!(
            dedupe_endpoints([valid, unspecified, multicast, zero_port]),
            vec![valid]
        );
    }

    #[test]
    fn drops_invalid_compact_peers() {
        let mut response = Vec::new();
        response.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
        response.extend_from_slice(&42u32.to_be_bytes());
        response.extend_from_slice(&1800u32.to_be_bytes());
        response.extend_from_slice(&3u32.to_be_bytes());
        response.extend_from_slice(&7u32.to_be_bytes());
        response.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
        response.extend_from_slice(&[192, 0, 2, 1, 0, 80]);

        let parsed = parse_announce_response(&response, false);
        assert_eq!(parsed.peers, vec!["192.0.2.1:80".parse().unwrap()]);
    }

    #[test]
    fn announce_response_buffer_fits_requested_ipv6_peers() {
        assert_eq!(announce_response_buffer_len(false, 200), 2048);
        assert_eq!(announce_response_buffer_len(true, 200), 3620);
    }

    #[test]
    fn parses_proxied_ipv6_compact_peers() {
        let mut response = Vec::new();
        response.extend_from_slice(&ACTION_ANNOUNCE.to_be_bytes());
        response.extend_from_slice(&42u32.to_be_bytes());
        response.extend_from_slice(&1800u32.to_be_bytes());
        response.extend_from_slice(&3u32.to_be_bytes());
        response.extend_from_slice(&7u32.to_be_bytes());
        response.extend_from_slice(&std::net::Ipv6Addr::LOCALHOST.octets());
        response.extend_from_slice(&6881u16.to_be_bytes());

        let parsed = parse_announce_response(&response, true);
        assert_eq!(parsed.peers, vec!["[::1]:6881".parse().unwrap()]);
    }

    #[test]
    fn direct_bind_uses_matching_source_family() {
        let v4_target: SocketAddr = "192.0.2.1:6969".parse().unwrap();
        let v4_source: SocketAddr = "192.0.2.7:0".parse().unwrap();
        assert_eq!(
            direct_bind_addr(v4_target, Some(v4_source)),
            "192.0.2.7:0".parse().unwrap()
        );

        let v6_target: SocketAddr = "[2001:db8::1]:6969".parse().unwrap();
        assert_eq!(
            direct_bind_addr(v6_target, Some(v4_source)),
            "[::]:0".parse().unwrap()
        );
    }

    #[test]
    fn retransmit_timeout_doubles_then_caps() {
        assert_eq!(retransmit_timeout(0), Duration::from_secs(3));
        assert_eq!(retransmit_timeout(1), Duration::from_secs(6));
        assert_eq!(retransmit_timeout(2), Duration::from_secs(12));
        assert_eq!(retransmit_timeout(3), Duration::from_secs(24));
        assert_eq!(retransmit_timeout(9), Duration::from_secs(24));
    }

    #[test]
    fn stopped_announces_outlast_one_lost_datagram_in_each_phase() {
        let one_loss_per_phase = retransmit_timeout(0) * 2;
        assert!(super::super::STOPPED_ANNOUNCE_TIMEOUT > one_loss_per_phase);
    }

    #[test]
    fn endpoint_fan_out_is_capped_per_family_in_resolver_order() {
        let v4 = |i: u8| SocketAddr::from(([192, 0, 2, i], 6969));
        let v6 = |i: u16| SocketAddr::from(([0x2001, 0xdb8, 0, 0, 0, 0, 0, i], 6969));
        let mut targets = vec![v6(1)];
        targets.extend((1..=30).map(v4));
        targets.extend((2..=9).map(v6));
        let limited = limit_per_family(targets);
        assert_eq!(limited.iter().filter(|t| t.is_ipv4()).count(), 4);
        assert_eq!(limited.iter().filter(|t| t.is_ipv6()).count(), 4);
        assert_eq!(limited[0], v6(1));
        assert_eq!(limited[1], v4(1));
        assert_eq!(limited[4], v4(4));
    }

    #[test]
    fn reply_source_must_match_an_ip_literal_tracker() {
        let from = |text: &str| ProxyDatagramSource::Ip(text.parse().unwrap());
        assert!(reply_source_matches(
            "192.0.2.1",
            6969,
            &from("192.0.2.1:6969")
        ));
        assert!(!reply_source_matches(
            "192.0.2.1",
            6969,
            &from("192.0.2.9:6969")
        ));
        assert!(!reply_source_matches(
            "192.0.2.1",
            6969,
            &from("192.0.2.1:6970")
        ));
        assert!(reply_source_matches(
            "192.0.2.1",
            6969,
            &from("[::ffff:192.0.2.1]:6969")
        ));
        assert!(reply_source_matches(
            "2001:db8::1",
            6969,
            &from("[2001:db8::1]:6969")
        ));
        assert!(!reply_source_matches(
            "2001:db8::1",
            6969,
            &from("[2001:db8::2]:6969")
        ));
        assert!(reply_source_matches("192.0.2.1", 6969, &from("0.0.0.0:0")));
    }

    #[test]
    fn reply_source_is_not_compared_when_the_tracker_is_a_hostname() {
        let ip = ProxyDatagramSource::Ip("203.0.113.5:6969".parse().unwrap());
        assert!(reply_source_matches("tracker.test", 6969, &ip));
        assert!(!reply_source_matches("tracker.test", 6970, &ip));
        let named = |name: &str, port| ProxyDatagramSource::Host(name.into(), port);
        assert!(reply_source_matches(
            "tracker.test",
            6969,
            &named("Tracker.TEST.", 6969)
        ));
        assert!(!reply_source_matches(
            "tracker.test",
            6969,
            &named("other.test", 6969)
        ));
        assert!(reply_source_matches(
            "192.0.2.1",
            6969,
            &named("tracker.test", 6969)
        ));
    }

    #[test]
    fn peer_family_follows_the_tracker_literal_before_the_reply_source() {
        let v6_source = ProxyDatagramSource::Ip("[2001:db8::1]:6969".parse().unwrap());
        let host_source = ProxyDatagramSource::Host("tracker.test".into(), 6969);
        assert!(reply_is_ipv6(Some(true), Some(&host_source)));
        assert!(!reply_is_ipv6(Some(false), Some(&v6_source)));
        assert!(reply_is_ipv6(None, Some(&v6_source)));
        assert!(!reply_is_ipv6(None, Some(&host_source)));
        assert!(!reply_is_ipv6(None, None));
    }

    #[test]
    fn announce_response_reports_swarm_counts_in_wire_order() {
        let response = announce_reply(1);
        let parsed = parse_announce_response(&response, false);
        assert_eq!(parsed.leechers, Some(2));
        assert_eq!(parsed.seeders, Some(5));
        assert_eq!(parsed.interval, Duration::from_secs(1800));
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Seen {
        Connect,
        Announce,
    }

    struct MockTracker {
        addr: SocketAddr,
        log: Arc<std::sync::Mutex<Vec<Seen>>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl MockTracker {
        fn count(&self, kind: Seen) -> usize {
            self.log
                .lock()
                .unwrap()
                .iter()
                .filter(|seen| **seen == kind)
                .count()
        }

        fn total(&self) -> usize {
            self.log.lock().unwrap().len()
        }
    }

    impl Drop for MockTracker {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn mock_tracker(
        respond: impl Fn(Seen, u32, u64, SocketAddr) -> Vec<Vec<u8>> + Send + 'static,
    ) -> MockTracker {
        mock_tracker_with(None, respond).await
    }

    async fn mock_tracker_with(
        reply_socket: Option<UdpSocket>,
        respond: impl Fn(Seen, u32, u64, SocketAddr) -> Vec<Vec<u8>> + Send + 'static,
    ) -> MockTracker {
        delayed_mock_tracker(reply_socket, |_| Duration::ZERO, respond).await
    }

    async fn delayed_mock_tracker(
        reply_socket: Option<UdpSocket>,
        delay: impl Fn(Seen) -> Duration + Send + 'static,
        respond: impl Fn(Seen, u32, u64, SocketAddr) -> Vec<Vec<u8>> + Send + 'static,
    ) -> MockTracker {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let addr = socket.local_addr().unwrap();
        let sender = reply_socket.map(Arc::new).unwrap_or_else(|| socket.clone());
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = log.clone();
        let task = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            loop {
                let Ok((n, from)) = socket.recv_from(&mut buf).await else {
                    return;
                };
                let (kind, txn, conn_id) = if n == 16 && be::read_u64(&buf[..8]) == PROTOCOL_ID {
                    (Seen::Connect, be::read_u32(&buf[12..16]), 0)
                } else if n >= 98 {
                    (
                        Seen::Announce,
                        be::read_u32(&buf[12..16]),
                        be::read_u64(&buf[..8]),
                    )
                } else {
                    continue;
                };
                seen.lock().unwrap().push(kind);
                let wait = delay(kind);
                for datagram in respond(kind, txn, conn_id, from) {
                    if wait.is_zero() {
                        let _ = sender.send_to(&datagram, from).await;
                    } else {
                        let sender = sender.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(wait).await;
                            let _ = sender.send_to(&datagram, from).await;
                        });
                    }
                }
            }
        });
        MockTracker { addr, log, task }
    }

    fn datagram(action: u32, txn: u32, tail: &[u8]) -> Vec<u8> {
        let mut out = action.to_be_bytes().to_vec();
        out.extend_from_slice(&txn.to_be_bytes());
        out.extend_from_slice(tail);
        out
    }

    fn connect_reply(txn: u32, id: u64) -> Vec<u8> {
        datagram(ACTION_CONNECT, txn, &id.to_be_bytes())
    }

    fn announce_reply(txn: u32) -> Vec<u8> {
        let mut tail = Vec::new();
        tail.extend_from_slice(&1800u32.to_be_bytes());
        tail.extend_from_slice(&2u32.to_be_bytes());
        tail.extend_from_slice(&5u32.to_be_bytes());
        tail.extend_from_slice(&[198, 51, 100, 7, 0x1a, 0xe1]);
        datagram(ACTION_ANNOUNCE, txn, &tail)
    }

    fn error_reply(txn: u32, text: &str) -> Vec<u8> {
        datagram(ACTION_ERROR, txn, text.as_bytes())
    }

    fn bound_id(from: SocketAddr) -> u64 {
        0x5EED_0000_0000_0000 | u64::from(from.port())
    }

    fn honest(kind: Seen, txn: u32, conn_id: u64, from: SocketAddr) -> Vec<Vec<u8>> {
        match kind {
            Seen::Connect => vec![connect_reply(txn, bound_id(from))],
            Seen::Announce if conn_id == bound_id(from) => vec![announce_reply(txn)],
            Seen::Announce => Vec::new(),
        }
    }

    fn announce_req() -> AnnounceRequest {
        AnnounceRequest {
            info_hash: crate::core::Id20([1u8; 20]),
            peer_id: crate::core::Id20([2u8; 20]),
            key: 7,
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 1,
            event: super::super::AnnounceEvent::Started,
            num_want: 50,
            obfuscate: false,
            tracker_id: None,
        }
    }

    fn rejection(error: &TrackerError) -> Option<&str> {
        match error {
            TrackerError::Rejected { reason, .. } => Some(reason),
            _ => None,
        }
    }

    async fn within<T>(secs: u64, future: impl std::future::Future<Output = T>) -> T {
        timeout(Duration::from_secs(secs), future)
            .await
            .expect("operation did not finish in time")
    }

    async fn announce_endpoint(
        target: SocketAddr,
        req: &AnnounceRequest,
        source: Option<SocketAddr>,
        url_data: &[u8],
    ) -> Result<AnnounceResponse, TrackerError> {
        let (sock, conn_id) = connect_endpoint(target, source).await?;
        announce(&Direct(&sock), conn_id, req, url_data).await
    }

    async fn settle() {
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    #[tokio::test]
    async fn every_announce_connects_from_its_own_socket() {
        let tracker = mock_tracker(honest).await;
        for _ in 0..2 {
            let response = within(
                10,
                announce_endpoint(tracker.addr, &announce_req(), None, b""),
            )
            .await
            .unwrap();
            assert_eq!(response.peers.len(), 1);
        }
        assert_eq!(tracker.count(Seen::Connect), 2);
        assert_eq!(tracker.count(Seen::Announce), 2);
    }

    #[tokio::test]
    async fn rejected_announce_is_reported_after_a_single_announce() {
        let tracker = mock_tracker(|kind, txn, _, from| match kind {
            Seen::Connect => vec![connect_reply(txn, bound_id(from))],
            Seen::Announce => vec![error_reply(txn, "torrent not registered")],
        })
        .await;
        let error = within(
            10,
            announce_endpoint(tracker.addr, &announce_req(), None, b""),
        )
        .await
        .unwrap_err();
        assert_eq!(rejection(&error), Some("torrent not registered"));
        settle().await;
        assert_eq!(tracker.count(Seen::Connect), 1);
        assert_eq!(tracker.count(Seen::Announce), 1);
    }

    #[tokio::test]
    async fn rejected_announce_over_a_proxy_socket_is_not_repeated() {
        let tracker = mock_tracker(|kind, txn, _, from| match kind {
            Seen::Connect => vec![connect_reply(txn, bound_id(from))],
            Seen::Announce => vec![error_reply(txn, "torrent not registered")],
        })
        .await;
        let socket = risuko_http::ProxyConnector::direct()
            .bind_udp()
            .await
            .unwrap();
        let error = within(
            10,
            announce_endpoint_proxy(
                socket,
                "127.0.0.1",
                tracker.addr.port(),
                &announce_req(),
                b"",
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(rejection(&error), Some("torrent not registered"));
        settle().await;
        assert_eq!(tracker.count(Seen::Connect), 1);
        assert_eq!(tracker.count(Seen::Announce), 1);
    }

    #[tokio::test]
    async fn error_packet_with_a_foreign_transaction_id_is_ignored() {
        let tracker = mock_tracker(|kind, txn, conn_id, from| match kind {
            Seen::Connect => vec![
                error_reply(txn ^ 1, "forged"),
                connect_reply(txn, bound_id(from)),
            ],
            Seen::Announce => {
                let mut replies = vec![error_reply(txn ^ 1, "forged")];
                replies.extend(honest(kind, txn, conn_id, from));
                replies
            }
        })
        .await;
        let response = within(
            10,
            announce_endpoint(tracker.addr, &announce_req(), None, b""),
        )
        .await
        .unwrap();
        assert_eq!(response.peers.len(), 1);
    }

    #[tokio::test]
    async fn foreign_datagrams_do_not_trigger_retransmissions() {
        let tracker = mock_tracker(|kind, txn, conn_id, from| {
            let mut replies: Vec<Vec<u8>> = (1..=8)
                .map(|salt| datagram(ACTION_ANNOUNCE, txn ^ salt, &[0u8; 12]))
                .collect();
            if kind == Seen::Announce {
                replies.push(connect_reply(txn, 0));
            }
            replies.extend(honest(kind, txn, conn_id, from));
            replies
        })
        .await;
        let response = within(
            10,
            announce_endpoint(tracker.addr, &announce_req(), None, b""),
        )
        .await
        .unwrap();
        assert_eq!(response.peers.len(), 1);
        settle().await;
        assert_eq!(tracker.count(Seen::Connect), 1);
        assert_eq!(tracker.count(Seen::Announce), 1);
    }

    #[tokio::test]
    async fn connect_error_text_is_reported_in_full() {
        let text = "connection rate limited please slow down";
        let tracker = mock_tracker(move |_, txn, _, _| vec![error_reply(txn, text)]).await;
        let error = within(
            10,
            announce_endpoint(tracker.addr, &announce_req(), None, b""),
        )
        .await
        .unwrap_err();
        assert_eq!(rejection(&error), Some(text));
        assert_eq!(tracker.count(Seen::Connect), 1);
    }

    #[tokio::test]
    async fn padded_connect_reply_is_accepted_on_the_proxy_path() {
        let tracker = mock_tracker(|kind, txn, conn_id, from| match kind {
            Seen::Connect => {
                let mut reply = connect_reply(txn, bound_id(from));
                reply.extend_from_slice(&[0u8; 4]);
                vec![reply]
            }
            Seen::Announce => honest(kind, txn, conn_id, from),
        })
        .await;
        let socket = risuko_http::ProxyConnector::direct()
            .bind_udp()
            .await
            .unwrap();
        let response = within(
            10,
            announce_endpoint_proxy(
                socket,
                "127.0.0.1",
                tracker.addr.port(),
                &announce_req(),
                b"",
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.peers.len(), 1);
    }

    #[tokio::test]
    async fn proxy_path_ignores_replies_from_another_endpoint() {
        let impostor = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let tracker = mock_tracker_with(Some(impostor), honest).await;
        let socket = risuko_http::ProxyConnector::direct()
            .bind_udp()
            .await
            .unwrap();
        let outcome = timeout(
            Duration::from_secs(1),
            announce_endpoint_proxy(
                socket,
                "127.0.0.1",
                tracker.addr.port(),
                &announce_req(),
                b"",
            ),
        )
        .await;
        assert!(outcome.is_err(), "accepted a reply from a foreign source");
    }

    #[tokio::test]
    async fn announce_fan_out_starts_one_endpoint_at_a_time() {
        let mut trackers = Vec::new();
        for _ in 0..12 {
            trackers.push(mock_tracker(honest).await);
        }
        let targets = trackers.iter().map(|tracker| tracker.addr).collect();
        let url_data: Arc<[u8]> = Vec::new().into();
        let response = within(
            10,
            race_endpoints_staggered(
                targets,
                "tracker.test",
                &announce_req(),
                None,
                url_data,
                Duration::from_secs(60),
                ENDPOINT_FAILOVER,
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.peers.len(), 1);
        settle().await;
        let contacted = trackers
            .iter()
            .filter(|tracker| tracker.total() > 0)
            .count();
        assert_eq!(contacted, 1, "{contacted} endpoints were contacted");
    }

    #[tokio::test]
    async fn only_the_first_endpoint_to_connect_receives_the_announce() {
        let slow_connect = delayed_mock_tracker(
            None,
            |seen| match seen {
                Seen::Connect => Duration::from_millis(600),
                Seen::Announce => Duration::ZERO,
            },
            honest,
        )
        .await;
        let slow_announce = delayed_mock_tracker(
            None,
            |seen| match seen {
                Seen::Connect => Duration::ZERO,
                Seen::Announce => Duration::from_millis(1200),
            },
            honest,
        )
        .await;
        let url_data: Arc<[u8]> = Vec::new().into();
        let response = within(
            10,
            race_endpoints_staggered(
                vec![slow_connect.addr, slow_announce.addr],
                "tracker.test",
                &announce_req(),
                None,
                url_data,
                Duration::from_millis(20),
                ENDPOINT_FAILOVER,
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.peers.len(), 1);
        settle().await;
        assert_eq!(slow_announce.count(Seen::Announce), 1);
        assert_eq!(slow_connect.count(Seen::Announce), 0);
    }

    #[tokio::test]
    async fn a_failed_announce_on_the_winning_endpoint_falls_over_to_the_next_one() {
        let rejecting = mock_tracker(|kind, txn, _, from| match kind {
            Seen::Connect => vec![connect_reply(txn, bound_id(from))],
            Seen::Announce => vec![error_reply(txn, "overloaded")],
        })
        .await;
        let live = mock_tracker(honest).await;
        let url_data: Arc<[u8]> = Vec::new().into();
        let response = within(
            10,
            race_endpoints_staggered(
                vec![rejecting.addr, live.addr],
                "tracker.test",
                &announce_req(),
                None,
                url_data,
                Duration::from_secs(60),
                ENDPOINT_FAILOVER,
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.peers.len(), 1);
        assert_eq!(rejecting.count(Seen::Announce), 1);
        assert_eq!(live.count(Seen::Announce), 1);
    }

    #[tokio::test]
    async fn the_announce_error_is_reported_when_the_other_endpoints_stay_silent() {
        let rejecting = mock_tracker(|kind, txn, _, from| match kind {
            Seen::Connect => vec![connect_reply(txn, bound_id(from))],
            Seen::Announce => vec![error_reply(txn, "overloaded")],
        })
        .await;
        let silent = mock_tracker(|_, _, _, _| Vec::new()).await;
        let url_data: Arc<[u8]> = Vec::new().into();
        let error = within(
            10,
            race_endpoints_staggered(
                vec![silent.addr, rejecting.addr],
                "tracker.test",
                &announce_req(),
                None,
                url_data,
                Duration::from_millis(20),
                Duration::from_millis(300),
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(rejection(&error), Some("overloaded"), "{error}");
    }

    #[test]
    fn a_kept_rejection_is_not_replaced_by_transport_errors() {
        let mut last = None;
        keep_tracker_answer(&mut last, TrackerError::Timeout);
        assert!(matches!(last, Some(TrackerError::Timeout)));
        keep_tracker_answer(&mut last, TrackerError::rejected("busy"));
        keep_tracker_answer(&mut last, TrackerError::Timeout);
        keep_tracker_answer(
            &mut last,
            TrackerError::Io(std::io::Error::from(std::io::ErrorKind::ConnectionRefused)),
        );
        assert_eq!(rejection(last.as_ref().unwrap()), Some("busy"));
    }

    #[tokio::test]
    async fn a_winner_that_drops_the_announce_hands_over_to_an_endpoint_already_connecting() {
        let drops_announce = delayed_mock_tracker(
            None,
            |seen| match seen {
                Seen::Connect => Duration::from_millis(100),
                Seen::Announce => Duration::ZERO,
            },
            |kind, txn, _, from| match kind {
                Seen::Connect => vec![connect_reply(txn, bound_id(from))],
                Seen::Announce => Vec::new(),
            },
        )
        .await;
        let slow_connect = delayed_mock_tracker(
            None,
            |seen| match seen {
                Seen::Connect => Duration::from_millis(400),
                Seen::Announce => Duration::ZERO,
            },
            honest,
        )
        .await;
        let url_data: Arc<[u8]> = Vec::new().into();
        let response = within(
            10,
            race_endpoints_staggered(
                vec![drops_announce.addr, slow_connect.addr],
                "tracker.test",
                &announce_req(),
                None,
                url_data,
                Duration::from_millis(20),
                Duration::from_millis(300),
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.peers.len(), 1);
        assert_eq!(drops_announce.count(Seen::Announce), 1);
        assert_eq!(slow_connect.count(Seen::Connect), 1);
        assert_eq!(slow_connect.count(Seen::Announce), 1);
    }

    #[tokio::test]
    async fn a_lone_endpoint_keeps_its_full_announce_retransmit_schedule() {
        let slow = delayed_mock_tracker(
            None,
            |seen| match seen {
                Seen::Connect => Duration::ZERO,
                Seen::Announce => Duration::from_millis(700),
            },
            honest,
        )
        .await;
        let url_data: Arc<[u8]> = Vec::new().into();
        let response = within(
            10,
            race_endpoints_staggered(
                vec![slow.addr],
                "tracker.test",
                &announce_req(),
                None,
                url_data,
                Duration::from_millis(20),
                Duration::from_millis(300),
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.peers.len(), 1);
    }

    #[tokio::test]
    async fn a_silent_first_endpoint_does_not_block_the_next_one() {
        let silent = mock_tracker(|_, _, _, _| Vec::new()).await;
        let live = mock_tracker(honest).await;
        let url_data: Arc<[u8]> = Vec::new().into();
        let response = within(
            10,
            race_endpoints(
                vec![silent.addr, live.addr],
                "tracker.test",
                &announce_req(),
                None,
                url_data,
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.peers.len(), 1);
        assert!(silent.total() > 0);
    }
}
