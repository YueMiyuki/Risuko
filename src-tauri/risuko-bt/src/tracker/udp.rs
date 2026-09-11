//! UDP tracker (BEP-15): client sends `Connect` (magic) to get a 64-bit `connection_id`, then `Announce` quoting it to get peers + interval + seeders/leechers; retransmits shortened to 3 tries to fit our async budget

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use rand::RngExt;
use tokio::net::{lookup_host, UdpSocket};
use tokio::task::JoinSet;
use tokio::time::timeout;

use super::{is_valid_endpoint, AnnounceRequest, AnnounceResponse, ScrapeResponse, TrackerError};

/// Big-endian read/write helpers over byte slices, replacing `byteorder`; each maps 1:1 to `std` be-bytes and slices are sized exactly by the caller so `try_into` never fails
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
const ACTION_SCRAPE: u32 = 2;
const ACTION_ERROR: u32 = 3;
const CONNECTION_TTL: Duration = Duration::from_secs(60);
const RETRANSMIT_ATTEMPTS: u32 = 4;

fn retransmit_timeout(attempt: u32) -> Duration {
    Duration::from_secs(15u64.saturating_mul(1u64 << attempt.min(2)))
}

#[derive(Clone, Copy)]
struct CachedConnection {
    id: u64,
    created: std::time::Instant,
}

#[derive(Clone, Copy, Hash, PartialEq, Eq)]
struct ConnectionCacheKey {
    target: SocketAddr,
    source: SocketAddr,
}

static CONNECTION_CACHE: LazyLock<Mutex<HashMap<ConnectionCacheKey, CachedConnection>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub async fn announce(url: &str, req: &AnnounceRequest) -> Result<AnnounceResponse, TrackerError> {
    announce_with_proxy(url, req, None).await
}

pub async fn announce_with_proxy(
    url: &str,
    req: &AnnounceRequest,
    proxy: Option<&risuko_http::ProxyConnector>,
) -> Result<AnnounceResponse, TrackerError> {
    announce_with_proxy_and_source(url, req, proxy, None).await
}

pub async fn announce_with_proxy_and_source(
    url: &str,
    req: &AnnounceRequest,
    proxy: Option<&risuko_http::ProxyConnector>,
    source: Option<SocketAddr>,
) -> Result<AnnounceResponse, TrackerError> {
    let (host, port) = parse_udp_url(url)?;
    let bypasses_proxy = proxy
        .and_then(|proxy| proxy.udp_no_proxy().or_else(|| proxy.no_proxy()))
        .is_some_and(|no_proxy| no_proxy.matches_host_port(&host, Some(port)));
    let source_is_concrete = source.is_some_and(|source| !source.ip().is_unspecified());
    if let Some(proxy) =
        proxy.filter(|proxy| proxy.has_proxy() && !(source_is_concrete && bypasses_proxy))
    {
        let socket = proxy
            .bind_udp_with_bypass()
            .await
            .map_err(|e| TrackerError::Http(e.to_string()))?;
        return announce_endpoint_proxy(socket, &host, port, req).await;
    }
    let targets = dedupe_endpoints(lookup_host((host.as_str(), port)).await?)
        .into_iter()
        .filter(|target| {
            source.is_none_or(|source| {
                source.ip().is_unspecified() || source.is_ipv4() == target.is_ipv4()
            })
        })
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return Err(TrackerError::Url(format!("no DNS result for {host}")));
    }

    let mut attempts = JoinSet::new();
    for target in targets {
        let req = req.clone();
        attempts.spawn(async move { announce_endpoint(target, &req, source).await });
    }

    let mut last_error = None;
    while let Some(result) = attempts.join_next().await {
        match result {
            Ok(Ok(response)) => {
                attempts.abort_all();
                return Ok(response);
            }
            Ok(Err(error)) => last_error = Some(error),
            Err(error) => {
                last_error = Some(TrackerError::Io(std::io::Error::other(format!(
                    "UDP tracker endpoint task failed: {error}"
                ))));
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| TrackerError::Url(format!("no usable DNS endpoint for {host}"))))
}

const MAX_SCRAPE_HASHES_PER_REQUEST: usize = 74;

pub async fn scrape(
    url: &str,
    info_hashes: &[super::super::core::Id20],
    source: Option<SocketAddr>,
) -> Result<Vec<ScrapeResponse>, TrackerError> {
    let mut results = Vec::with_capacity(info_hashes.len());
    for batch in info_hashes.chunks(MAX_SCRAPE_HASHES_PER_REQUEST) {
        results.extend(scrape_direct_batch(url, batch, source).await?);
    }
    Ok(results)
}

async fn scrape_direct_batch(
    url: &str,
    info_hashes: &[super::super::core::Id20],
    source: Option<SocketAddr>,
) -> Result<Vec<ScrapeResponse>, TrackerError> {
    if info_hashes.is_empty() {
        return Ok(Vec::new());
    }
    if info_hashes.len() > MAX_SCRAPE_HASHES_PER_REQUEST {
        return Err(TrackerError::Url(
            "UDP scrape batch exceeds 74 info-hashes".into(),
        ));
    }
    let (host, port) = parse_udp_url(url)?;
    let targets = dedupe_endpoints(lookup_host((host.as_str(), port)).await?)
        .into_iter()
        .filter(|target| {
            source.is_none_or(|source| {
                source.ip().is_unspecified() || source.is_ipv4() == target.is_ipv4()
            })
        })
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return Err(TrackerError::Url(format!("no DNS result for {host}")));
    }

    let mut last_error = None;
    for target in targets {
        match scrape_direct_endpoint(target, info_hashes, source).await {
            Ok(response) => return Ok(response),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or(TrackerError::Timeout))
}

async fn scrape_direct_endpoint(
    target: SocketAddr,
    info_hashes: &[super::super::core::Id20],
    source: Option<SocketAddr>,
) -> Result<Vec<ScrapeResponse>, TrackerError> {
    let bind_addr = direct_bind_addr(target, source);
    let cache_key = ConnectionCacheKey {
        target,
        source: bind_addr,
    };
    let sock = UdpSocket::bind(bind_addr).await?;
    sock.connect(target).await?;

    let conn_id = match cached_connection(cache_key) {
        Some(id) => id,
        None => {
            let id = connect(&sock).await?;
            cache_connection(cache_key, id);
            id
        }
    };
    match scrape_inner(&sock, conn_id, info_hashes).await {
        Ok(response) => Ok(response),
        Err(TrackerError::Timeout | TrackerError::Rejected(_)) => {
            invalidate_connection(cache_key);
            let id = connect(&sock).await?;
            cache_connection(cache_key, id);
            scrape_inner(&sock, id, info_hashes).await
        }
        Err(error) => Err(error),
    }
}

async fn scrape_inner(
    sock: &UdpSocket,
    conn_id: u64,
    info_hashes: &[super::super::core::Id20],
) -> Result<Vec<ScrapeResponse>, TrackerError> {
    let txn = rand::rng().random::<u32>();
    let mut body = Vec::with_capacity(16 + info_hashes.len() * 20);
    body.extend_from_slice(&conn_id.to_be_bytes());
    body.extend_from_slice(&ACTION_SCRAPE.to_be_bytes());
    body.extend_from_slice(&txn.to_be_bytes());
    for hash in info_hashes {
        body.extend_from_slice(hash.as_bytes());
    }
    let mut buf = vec![0u8; 8 + info_hashes.len() * 12];
    for attempt in 0..RETRANSMIT_ATTEMPTS {
        sock.send(&body).await?;
        match timeout(retransmit_timeout(attempt), sock.recv(&mut buf)).await {
            Ok(Ok(n)) if n >= 8 => {
                let action = be::read_u32(&buf[..4]);
                let rtxn = be::read_u32(&buf[4..8]);
                if rtxn != txn {
                    continue;
                }
                if action == ACTION_ERROR {
                    return Err(TrackerError::Rejected(read_error(&buf[8..n])));
                }
                let expected = 8 + info_hashes.len() * 12;
                if action != ACTION_SCRAPE || n != expected {
                    continue;
                }
                let mut out = Vec::with_capacity(info_hashes.len());
                for chunk in buf[8..expected].chunks_exact(12) {
                    out.push(ScrapeResponse {
                        complete: be::read_u32(&chunk[0..4]),
                        downloaded: be::read_u32(&chunk[4..8]),
                        incomplete: be::read_u32(&chunk[8..12]),
                    });
                }
                return Ok(out);
            }
            _ => {}
        }
    }
    Err(TrackerError::Timeout)
}

pub async fn scrape_with_proxy(
    url: &str,
    info_hashes: &[super::super::core::Id20],
    proxy: Option<&risuko_http::ProxyConnector>,
    source: Option<SocketAddr>,
) -> Result<Vec<ScrapeResponse>, TrackerError> {
    let Some(proxy) = proxy else {
        return scrape(url, info_hashes, source).await;
    };
    if !proxy.has_proxy() {
        return scrape(url, info_hashes, source).await;
    }
    if source.is_some_and(|source| !source.ip().is_unspecified()) {
        let (host, port) = parse_udp_url(url)?;
        let bypasses_proxy = proxy
            .udp_no_proxy()
            .or_else(|| proxy.no_proxy())
            .is_some_and(|no_proxy| no_proxy.matches_host_port(&host, Some(port)));
        if bypasses_proxy {
            return scrape(url, info_hashes, source).await;
        }
    }
    let mut results = Vec::with_capacity(info_hashes.len());
    for batch in info_hashes.chunks(MAX_SCRAPE_HASHES_PER_REQUEST) {
        results.extend(scrape_proxy_batch(url, batch, proxy).await?);
    }
    Ok(results)
}

async fn scrape_proxy_batch(
    url: &str,
    info_hashes: &[super::super::core::Id20],
    proxy: &risuko_http::ProxyConnector,
) -> Result<Vec<ScrapeResponse>, TrackerError> {
    if info_hashes.is_empty() {
        return Ok(Vec::new());
    }
    if info_hashes.len() > MAX_SCRAPE_HASHES_PER_REQUEST {
        return Err(TrackerError::Url(
            "UDP scrape batch exceeds 74 info-hashes".into(),
        ));
    }
    let (host, port) = parse_udp_url(url)?;
    let sock = proxy
        .bind_udp_with_bypass()
        .await
        .map_err(|e| TrackerError::Http(e.to_string()))?;
    let conn_id = connect_socket(&sock, &host, port).await?;
    let txn = rand::rng().random::<u32>();
    let mut body = Vec::with_capacity(16 + info_hashes.len() * 20);
    body.extend_from_slice(&conn_id.to_be_bytes());
    body.extend_from_slice(&ACTION_SCRAPE.to_be_bytes());
    body.extend_from_slice(&txn.to_be_bytes());
    for hash in info_hashes {
        body.extend_from_slice(hash.as_bytes());
    }
    let mut buf = vec![0u8; 8 + info_hashes.len() * 12];
    for attempt in 0..RETRANSMIT_ATTEMPTS {
        sock.send_to_host(&body, &host, port)
            .await
            .map_err(|e| TrackerError::Http(e.to_string()))?;
        match timeout(retransmit_timeout(attempt), sock.recv_from_target(&mut buf)).await {
            Ok(Ok((n, _))) if n >= 8 => {
                let action = be::read_u32(&buf[..4]);
                let rtxn = be::read_u32(&buf[4..8]);
                if rtxn != txn {
                    continue;
                }
                if action == ACTION_ERROR {
                    return Err(TrackerError::Rejected(read_error(&buf[8..n])));
                }
                let expected = 8 + info_hashes.len() * 12;
                if action != ACTION_SCRAPE || n != expected {
                    continue;
                }
                return Ok(buf[8..expected]
                    .chunks_exact(12)
                    .map(|chunk| ScrapeResponse {
                        complete: be::read_u32(&chunk[0..4]),
                        downloaded: be::read_u32(&chunk[4..8]),
                        incomplete: be::read_u32(&chunk[8..12]),
                    })
                    .collect());
            }
            _ => {}
        }
    }
    Err(TrackerError::Timeout)
}

fn dedupe_endpoints(endpoints: impl IntoIterator<Item = SocketAddr>) -> Vec<SocketAddr> {
    let mut seen = HashSet::new();
    endpoints
        .into_iter()
        .filter(|endpoint| is_valid_endpoint(*endpoint) && seen.insert(*endpoint))
        .collect()
}

async fn announce_endpoint(
    target: SocketAddr,
    req: &AnnounceRequest,
    source: Option<SocketAddr>,
) -> Result<AnnounceResponse, TrackerError> {
    let bind_addr = direct_bind_addr(target, source);
    let cache_key = ConnectionCacheKey {
        target,
        source: bind_addr,
    };
    let sock = UdpSocket::bind(bind_addr).await?;
    sock.connect(target).await?;

    let conn_id = match cached_connection(cache_key) {
        Some(id) => id,
        None => {
            let id = connect(&sock).await?;
            cache_connection(cache_key, id);
            id
        }
    };
    match announce_inner(&sock, conn_id, req).await {
        Ok(response) => Ok(response),
        Err(TrackerError::Timeout | TrackerError::Rejected(_)) => {
            invalidate_connection(cache_key);
            let id = connect(&sock).await?;
            cache_connection(cache_key, id);
            announce_inner(&sock, id, req).await
        }
        Err(error) => Err(error),
    }
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

fn cached_connection(key: ConnectionCacheKey) -> Option<u64> {
    let mut cache = CONNECTION_CACHE.lock().ok()?;
    cache.retain(|_, item| item.created.elapsed() < CONNECTION_TTL);
    cache.get(&key).map(|item| item.id)
}

fn cache_connection(key: ConnectionCacheKey, id: u64) {
    if let Ok(mut cache) = CONNECTION_CACHE.lock() {
        cache.retain(|_, item| item.created.elapsed() < CONNECTION_TTL);
        cache.insert(
            key,
            CachedConnection {
                id,
                created: std::time::Instant::now(),
            },
        );
    }
}

fn invalidate_connection(key: ConnectionCacheKey) {
    if let Ok(mut cache) = CONNECTION_CACHE.lock() {
        cache.remove(&key);
    }
}

async fn announce_endpoint_proxy(
    sock: risuko_http::ProxyDatagram,
    host: &str,
    port: u16,
    req: &AnnounceRequest,
) -> Result<AnnounceResponse, TrackerError> {
    let conn_id = connect_socket(&sock, host, port).await?;
    let is_ipv6 = host.parse::<IpAddr>().is_ok_and(|ip| ip.is_ipv6());
    match announce_inner_socket(&sock, conn_id, host, port, req, is_ipv6).await {
        Ok(response) => Ok(response),
        Err(TrackerError::Timeout | TrackerError::Rejected(_)) => {
            let id = connect_socket(&sock, host, port).await?;
            announce_inner_socket(&sock, id, host, port, req, is_ipv6).await
        }
        Err(error) => Err(error),
    }
}

async fn connect(sock: &UdpSocket) -> Result<u64, TrackerError> {
    let mut buf = [0u8; 16];
    let txn = rand::rng().random::<u32>();
    let mut req = [0u8; 16];
    be::write_u64(&mut req[0..8], PROTOCOL_ID);
    be::write_u32(&mut req[8..12], ACTION_CONNECT);
    be::write_u32(&mut req[12..16], txn);

    for attempt in 0..RETRANSMIT_ATTEMPTS {
        sock.send(&req).await?;
        match timeout(retransmit_timeout(attempt), sock.recv(&mut buf)).await {
            Ok(Ok(n)) if n >= 8 => {
                let action = be::read_u32(&buf[0..4]);
                let rtxn = be::read_u32(&buf[4..8]);
                if action == ACTION_ERROR {
                    return Err(TrackerError::Rejected(read_error(&buf[8..n])));
                }
                if n != 16 || action != ACTION_CONNECT || rtxn != txn {
                    continue;
                }
                return Ok(be::read_u64(&buf[8..16]));
            }
            _ => continue,
        }
    }
    Err(TrackerError::Timeout)
}

async fn connect_socket(
    sock: &risuko_http::ProxyDatagram,
    host: &str,
    port: u16,
) -> Result<u64, TrackerError> {
    let mut buf = [0u8; 2048];
    let txn = rand::rng().random::<u32>();
    let mut req = [0u8; 16];
    be::write_u64(&mut req[0..8], PROTOCOL_ID);
    be::write_u32(&mut req[8..12], ACTION_CONNECT);
    be::write_u32(&mut req[12..16], txn);
    for attempt in 0..RETRANSMIT_ATTEMPTS {
        sock.send_to_host(&req, host, port)
            .await
            .map_err(|e| TrackerError::Http(e.to_string()))?;
        match timeout(retransmit_timeout(attempt), sock.recv_from_target(&mut buf)).await {
            Ok(Ok((n, _))) if n >= 8 => {
                let action = be::read_u32(&buf[0..4]);
                let rtxn = be::read_u32(&buf[4..8]);
                if action == ACTION_ERROR {
                    return Err(TrackerError::Rejected(read_error(&buf[8..n])));
                }
                if n == 16 && action == ACTION_CONNECT && rtxn == txn {
                    return Ok(be::read_u64(&buf[8..16]));
                }
            }
            _ => {}
        }
    }
    Err(TrackerError::Timeout)
}

async fn announce_inner_socket(
    sock: &risuko_http::ProxyDatagram,
    conn_id: u64,
    host: &str,
    port: u16,
    req: &AnnounceRequest,
    is_ipv6: bool,
) -> Result<AnnounceResponse, TrackerError> {
    let (body, txn) = build_announce_body(conn_id, req);
    let mut buf = vec![0u8; announce_response_buffer_len(true, req.num_want)];
    for attempt in 0..RETRANSMIT_ATTEMPTS {
        sock.send_to_host(&body, host, port)
            .await
            .map_err(|e| TrackerError::Http(e.to_string()))?;
        match timeout(retransmit_timeout(attempt), sock.recv_from_target(&mut buf)).await {
            Ok(Ok((n, from))) if n >= 8 => {
                let effective_max = buf.len().min(MAX_UDP_PAYLOAD);
                if n >= effective_max {
                    tracing::warn!("UDP tracker announce response may be truncated");
                }
                let action = be::read_u32(&buf[0..4]);
                let rtxn = be::read_u32(&buf[4..8]);
                if action == ACTION_ERROR {
                    return Err(TrackerError::Rejected(read_error(&buf[8..n])));
                }
                let source_is_ipv6 = matches!(
                    from,
                    risuko_http::ProxyDatagramSource::Ip(address)
                        if address.is_ipv6()
                );
                let response_is_ipv6 = is_ipv6 || source_is_ipv6;
                let stride = if response_is_ipv6 { 18 } else { 6 };
                if n >= 20 && (n - 20) % stride == 0 && action == ACTION_ANNOUNCE && rtxn == txn {
                    return Ok(parse_announce_response(&buf[..n], response_is_ipv6));
                }
            }
            _ => {}
        }
    }
    Err(TrackerError::Timeout)
}

async fn announce_inner(
    sock: &UdpSocket,
    conn_id: u64,
    req: &AnnounceRequest,
) -> Result<AnnounceResponse, TrackerError> {
    let (body, txn) = build_announce_body(conn_id, req);

    let is_ipv6 = sock.peer_addr().map(|a| a.is_ipv6()).unwrap_or(false);
    let mut buf = vec![0u8; announce_response_buffer_len(is_ipv6, req.num_want)];
    for attempt in 0..RETRANSMIT_ATTEMPTS {
        sock.send(&body).await?;
        match timeout(retransmit_timeout(attempt), sock.recv(&mut buf)).await {
            Ok(Ok(n)) if n >= 8 => {
                let effective_max = buf.len().min(MAX_UDP_PAYLOAD);
                if n >= effective_max {
                    tracing::warn!("UDP tracker announce response may be truncated");
                }
                let action = be::read_u32(&buf[0..4]);
                let rtxn = be::read_u32(&buf[4..8]);
                if action == ACTION_ERROR {
                    return Err(TrackerError::Rejected(read_error(&buf[8..n])));
                }
                let stride = if is_ipv6 { 18 } else { 6 };
                if n < 20 || (n - 20) % stride != 0 || action != ACTION_ANNOUNCE || rtxn != txn {
                    continue;
                }
                return Ok(parse_announce_response(&buf[..n], is_ipv6));
            }
            _ => continue,
        }
    }
    Err(TrackerError::Timeout)
}

fn announce_response_buffer_len(is_ipv6: bool, num_want: u32) -> usize {
    const HEADER: usize = 20;
    const MIN_BUFFER_LEN: usize = 2048;
    let stride: usize = if is_ipv6 { 18 } else { 6 };
    HEADER
        .saturating_add(stride.saturating_mul(num_want as usize))
        .clamp(MIN_BUFFER_LEN, MAX_UDP_PAYLOAD)
}

const MAX_UDP_PAYLOAD: usize = 65_507;

fn build_announce_body(conn_id: u64, req: &AnnounceRequest) -> ([u8; 98], u32) {
    let txn = rand::rng().random::<u32>();
    let mut body = [0u8; 98];
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
    (body, txn)
}

fn parse_announce_response(buf: &[u8], is_ipv6: bool) -> AnnounceResponse {
    let interval = be::read_u32(&buf[8..12]).max(1) as u64;
    let leechers = be::read_u32(&buf[12..16]);
    let seeders = be::read_u32(&buf[16..20]);
    let mut peers = Vec::new();
    if is_ipv6 {
        // BEP-15 IPv6 extension: 18-byte compact peer entries (16 addr + 2 port)
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
        seeders: Some(seeders),
        leechers: Some(leechers),
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
    let rest = url
        .strip_prefix("udp://")
        .ok_or_else(|| TrackerError::UnsupportedScheme(url.to_string()))?;
    // Trim trailing path like `/announce` and any query string; UDP trackers ignore both
    let rest = rest.split('/').next().unwrap_or(rest);
    let rest = rest.split('?').next().unwrap_or(rest);
    let (host, port) = match rest.rsplit_once(':') {
        Some((h, p)) => (
            h.trim_start_matches('[').trim_end_matches(']').to_string(),
            p.parse::<u16>()
                .map_err(|_| TrackerError::Url(format!("bad port in {url}")))?,
        ),
        None => return Err(TrackerError::Url(format!("missing port in {url}"))),
    };
    if host.is_empty() || port == 0 {
        return Err(TrackerError::Url(format!("invalid endpoint in {url}")));
    }
    Ok((host, port))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn scrape_splits_large_requests_at_the_protocol_batch_limit() {
        let hashes = vec![crate::core::Id20([7u8; 20]); 149];
        let sizes: Vec<usize> = hashes
            .chunks(MAX_SCRAPE_HASHES_PER_REQUEST)
            .map(|batch| batch.len())
            .collect();
        assert_eq!(sizes, vec![74, 74, 1]);
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
    fn cached_connection_keeps_zero_and_isolates_source_routes() {
        let target: SocketAddr = "192.0.2.1:6969".parse().unwrap();
        let first = ConnectionCacheKey {
            target,
            source: "192.0.2.7:0".parse().unwrap(),
        };
        let second = ConnectionCacheKey {
            target,
            source: "192.0.2.8:0".parse().unwrap(),
        };

        cache_connection(first, 0);
        assert_eq!(cached_connection(first), Some(0));
        assert_eq!(cached_connection(second), None);
        invalidate_connection(first);
    }

    #[test]
    fn bep15_retransmit_timeout_doubles_then_caps() {
        assert_eq!(retransmit_timeout(0), Duration::from_secs(15));
        assert_eq!(retransmit_timeout(1), Duration::from_secs(30));
        assert_eq!(retransmit_timeout(2), Duration::from_secs(60));
        assert_eq!(retransmit_timeout(3), Duration::from_secs(60));
    }
}
