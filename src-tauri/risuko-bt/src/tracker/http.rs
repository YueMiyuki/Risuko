//! HTTP / HTTPS tracker (BEP-3 + BEP-23 compact peer list)

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use percent_encoding::{percent_encode, NON_ALPHANUMERIC};

use super::super::bencode::{decode_all_external, DecodeLimits, Value};
use super::{is_valid_endpoint, AnnounceRequest, AnnounceResponse, TrackerError};

/// RFC 3986 unreserved + `-_.~` are safe in a URL without percent-encoding
const TRACKER_RESPONSE_LIMITS: DecodeLimits = DecodeLimits::new(2 * 1024 * 1024, 64, 262_144);

const QUERY_SAFE: &percent_encoding::AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

fn client() -> &'static risuko_http::Client {
    static CLIENT: OnceLock<risuko_http::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        risuko_http::Client::builder()
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(4)
            .build()
            .expect("build risuko-http client")
    })
}

fn source_client(source: SocketAddr) -> Result<risuko_http::Client, TrackerError> {
    static CLIENTS: OnceLock<Mutex<HashMap<SocketAddr, risuko_http::Client>>> = OnceLock::new();

    let source = tracker_source_addr(source);
    let mut clients = CLIENTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| TrackerError::Http("source client cache lock poisoned".into()))?;
    if let Some(client) = clients.get(&source) {
        return Ok(client.clone());
    }

    let client = risuko_http::Client::builder()
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(4)
        .local_socket_address(source)
        .build()
        .map_err(|e| TrackerError::Http(e.to_string()))?;
    clients.insert(source, client.clone());
    Ok(client)
}

fn tracker_source_addr(mut source: SocketAddr) -> SocketAddr {
    source.set_port(0);
    source
}

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
    let source = source.filter(|source| !source.ip().is_unspecified());
    let client = match (proxy, source) {
        (Some(proxy), _) => proxy_client(proxy, source)?,
        (None, Some(source)) => source_client(source)?,
        (None, None) => client().clone(),
    };
    fetch_announce(&client, url, req).await
}

/// Announce from one address family only (BEP 7); a proxy still routes redirects away from a bypassed tracker
pub async fn announce_for_family(
    url: &str,
    req: &AnnounceRequest,
    family: super::AddressFamily,
    proxy: Option<&risuko_http::ProxyConnector>,
) -> Result<AnnounceResponse, TrackerError> {
    let client = match proxy.filter(|proxy| proxy.proxy().is_some()) {
        Some(proxy) => proxy_client(proxy, Some(family.unspecified()))?,
        None => source_client(family.unspecified())?,
    };
    fetch_announce(&client, url, req).await
}

/// Whether the client tunnels `url` through the TCP proxy rather than connecting direct
pub(super) fn routes_via_proxy(url: &str, proxy: &risuko_http::ProxyConnector) -> bool {
    proxy.proxy().is_some()
        && !url::Url::parse(url).is_ok_and(|url| {
            proxy
                .no_proxy()
                .is_some_and(|no_proxy| no_proxy.matches_url(&url))
        })
}

/// `source` pins direct connections only; proxy control connections pick their own
fn proxy_client(
    proxy: &risuko_http::ProxyConnector,
    source: Option<SocketAddr>,
) -> Result<risuko_http::Client, TrackerError> {
    let mut builder = risuko_http::Client::builder()
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(4);
    if let Some(route) = proxy.proxy() {
        builder = builder.proxy(route);
    }
    if let Some(bypass) = proxy.no_proxy() {
        builder = builder.no_proxy(bypass);
    }
    if let Some(source) = source {
        builder = builder.local_socket_address(tracker_source_addr(source));
    }
    builder
        .build()
        .map_err(|e| TrackerError::Http(e.to_string()))
}

async fn fetch_announce(
    client: &risuko_http::Client,
    url: &str,
    req: &AnnounceRequest,
) -> Result<AnnounceResponse, TrackerError> {
    let query = build_query(req);
    // Append to existing query string if the URL already has one
    let sep = if url.contains('?') { '&' } else { '?' };
    let full = format!("{}{}{}", url, sep, query);
    let bytes = client
        .get(&full)
        .send()
        .await
        .map_err(|e| TrackerError::Http(e.to_string()))?
        .error_for_status()
        .map_err(|e| TrackerError::Http(e.to_string()))?
        .bytes()
        .await
        .map_err(|e| TrackerError::Http(e.to_string()))?;
    parse_response(&bytes, req)
}

fn build_query(req: &AnnounceRequest) -> String {
    let pid = percent_encode(req.peer_id.as_bytes(), QUERY_SAFE);
    // BEP 8: `sha_ih` replaces `info_hash` and the port is obscured
    let (hash_param, hash, port) = if req.obfuscate {
        (
            "sha_ih",
            super::obfuscation::sha_ih(&req.info_hash),
            super::obfuscation::obscure_port(&req.info_hash, req.port),
        )
    } else {
        ("info_hash", req.info_hash, req.port)
    };
    let hash = percent_encode(hash.as_bytes(), QUERY_SAFE);
    let mut s = format!(
        "{hash_param}={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact=1&numwant={}&key={}",
        hash, pid, port, req.uploaded, req.downloaded, req.left, req.num_want, req.key
    );
    let ev = req.event.as_str();
    if !ev.is_empty() {
        s.push_str("&event=");
        s.push_str(ev);
    }
    s
}

/// Compact peer field as plaintext, decrypting BEP 8 responses
fn reveal_peers<'a>(
    raw: &'a [u8],
    stride: usize,
    req: &AnnounceRequest,
    response: &Value,
) -> Option<std::borrow::Cow<'a, [u8]>> {
    if !req.obfuscate {
        return Some(std::borrow::Cow::Borrowed(raw));
    }
    // Outer `None` is a window parameter outside `u32`, which drops the field instead of wrapping
    let window = |key: &[u8]| match response.get(key).and_then(Value::as_int) {
        Some(n) => u32::try_from(n).ok().map(Some),
        None => Some(None),
    };
    super::obfuscation::deobfuscate_peers(
        &req.info_hash,
        response.get(b"iv").and_then(Value::as_bytes),
        window(b"i")?,
        window(b"n")?,
        raw,
        stride,
    )
    .map(std::borrow::Cow::Owned)
}

fn parse_response(bytes: &[u8], req: &AnnounceRequest) -> Result<AnnounceResponse, TrackerError> {
    let value = decode_all_external(bytes, TRACKER_RESPONSE_LIMITS)?;
    value
        .as_dict()
        .ok_or_else(|| TrackerError::Rejected("response not a dict".into()))?;

    if let Some(reason) = value.get(b"failure reason").and_then(|v| v.as_str()) {
        return Err(TrackerError::Rejected(reason.to_string()));
    }

    let interval = value
        .get(b"interval")
        .and_then(|v| v.as_int())
        .unwrap_or(1800)
        .max(1) as u64;

    let seeders = value
        .get(b"complete")
        .and_then(|v| v.as_int())
        .and_then(|n| u32::try_from(n).ok());
    let leechers = value
        .get(b"incomplete")
        .and_then(|v| v.as_int())
        .and_then(|n| u32::try_from(n).ok());

    let mut peers = Vec::new();
    let mut seen = HashSet::new();
    // Compact IPv4 (BEP-23): 6 bytes per peer
    if let Some(raw) = value
        .get(b"peers")
        .and_then(|v| v.as_bytes())
        .and_then(|raw| reveal_peers(raw, 6, req, &value))
    {
        for chunk in raw.chunks_exact(6) {
            let ip = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
            let port = u16::from_be_bytes([chunk[4], chunk[5]]);
            let endpoint = SocketAddr::new(IpAddr::V4(ip), port);
            if is_valid_endpoint(endpoint) && seen.insert(endpoint) {
                peers.push(endpoint);
            }
        }
    } else if let Some(list) = value
        .get(b"peers")
        .and_then(|v| v.as_list())
        .filter(|_| !req.obfuscate)
    {
        // Dictionary model (BEP-3 original): each entry is a dict with `ip` and `port` keys
        for entry in list {
            if entry.as_dict().is_some() {
                let ip = entry.get(b"ip").and_then(|v| v.as_str());
                let port = entry.get(b"port").and_then(|v| v.as_int());
                if let (Some(ip), Some(port)) = (ip, port) {
                    if let Ok(ip) = ip.parse::<IpAddr>() {
                        if let Ok(port) = u16::try_from(port) {
                            let endpoint = SocketAddr::new(ip, port);
                            if is_valid_endpoint(endpoint) && seen.insert(endpoint) {
                                peers.push(endpoint);
                            }
                        }
                    }
                }
            }
        }
    }

    // Compact IPv6 (BEP-7): 18 bytes per peer
    if let Some(raw) = value
        .get(b"peers6")
        .and_then(|v| v.as_bytes())
        .and_then(|raw| reveal_peers(raw, 18, req, &value))
    {
        for chunk in raw.chunks_exact(18) {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&chunk[..16]);
            let ip = Ipv6Addr::from(octets);
            let port = u16::from_be_bytes([chunk[16], chunk[17]]);
            let endpoint = SocketAddr::new(IpAddr::V6(ip), port);
            if is_valid_endpoint(endpoint) && seen.insert(endpoint) {
                peers.push(endpoint);
            }
        }
    }

    Ok(AnnounceResponse {
        interval: Duration::from_secs(interval),
        peers,
        seeders,
        leechers,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bencode::{encode_to_vec, Value};
    use crate::core::Id20;

    fn req() -> AnnounceRequest {
        AnnounceRequest {
            info_hash: Id20([1u8; 20]),
            peer_id: Id20([2u8; 20]),
            key: 0x01020304,
            port: 6881,
            uploaded: 0,
            downloaded: 0,
            left: 100,
            event: super::super::AnnounceEvent::Started,
            num_want: 50,
            obfuscate: false,
        }
    }

    #[test]
    fn query_includes_required_fields() {
        let q = build_query(&req());
        for k in [
            "info_hash=",
            "peer_id=",
            "port=6881",
            "compact=1",
            "event=started",
        ] {
            assert!(q.contains(k), "missing {k} in {q}");
        }
        assert!(q.contains("key=16909060"));
        assert!(!q.contains("ipv4="));
        assert!(!q.contains("ipv6="));
    }

    #[test]
    fn parses_compact_v4_response() {
        let peers_bin: Vec<u8> = vec![
            1, 2, 3, 4, 0x1a, 0xe1, // 1.2.3.4:6881
            5, 6, 7, 8, 0x1a, 0xe2, // 5.6.7.8:6882
        ];
        let body = encode_to_vec(&Value::Dict(vec![
            (b"interval".to_vec(), Value::Int(60)),
            (b"peers".to_vec(), Value::Bytes(peers_bin)),
        ]));
        let r = parse_response(&body, &req()).unwrap();
        assert_eq!(r.interval, Duration::from_secs(60));
        assert_eq!(r.peers.len(), 2);
        assert_eq!(r.peers[0].port(), 6881);
    }

    #[test]
    fn preserves_tracker_peer_order_while_deduplicating() {
        let peers_bin: Vec<u8> = vec![
            5, 6, 7, 8, 0x1a, 0xe2, // 5.6.7.8:6882
            1, 2, 3, 4, 0x1a, 0xe1, // 1.2.3.4:6881
            5, 6, 7, 8, 0x1a, 0xe2, // duplicate
            255, 255, 255, 255, 0x1a, 0xe3, // broadcast
        ];
        let body = encode_to_vec(&Value::Dict(vec![
            (b"interval".to_vec(), Value::Int(60)),
            (b"peers".to_vec(), Value::Bytes(peers_bin)),
        ]));

        let response = parse_response(&body, &req()).unwrap();
        assert_eq!(
            response.peers,
            vec![
                "5.6.7.8:6882".parse().unwrap(),
                "1.2.3.4:6881".parse().unwrap(),
            ]
        );
    }

    #[test]
    fn tracker_source_keeps_ipv6_scope_id() {
        let source = SocketAddr::V6(std::net::SocketAddrV6::new(
            Ipv6Addr::LOCALHOST,
            43123,
            0,
            7,
        ));

        assert_eq!(
            tracker_source_addr(source),
            SocketAddr::V6(std::net::SocketAddrV6::new(Ipv6Addr::LOCALHOST, 0, 0, 7))
        );
    }

    #[test]
    fn parses_compact_peers6_and_deduplicates_response_endpoints() {
        let ip = Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 7);
        let mut peers6 = ip.octets().to_vec();
        peers6.extend_from_slice(&6881u16.to_be_bytes());
        peers6.extend_from_slice(&Ipv6Addr::UNSPECIFIED.octets());
        peers6.extend_from_slice(&6882u16.to_be_bytes());
        let body = encode_to_vec(&Value::Dict(vec![
            (b"interval".to_vec(), Value::Int(60)),
            (b"peers6".to_vec(), Value::Bytes(peers6)),
            (
                b"peers".to_vec(),
                Value::List(vec![Value::Dict(vec![
                    (b"ip".to_vec(), Value::Bytes(ip.to_string().into_bytes())),
                    (b"port".to_vec(), Value::Int(6881)),
                ])]),
            ),
        ]));
        let response = parse_response(&body, &req()).unwrap();
        assert_eq!(response.peers, vec![SocketAddr::new(IpAddr::V6(ip), 6881)]);
    }

    #[test]
    fn expanded_peer_fallback_accepts_ipv6_and_rejects_bad_endpoints() {
        let body = encode_to_vec(&Value::Dict(vec![
            (b"interval".to_vec(), Value::Int(60)),
            (
                b"peers".to_vec(),
                Value::List(vec![
                    Value::Dict(vec![
                        (b"ip".to_vec(), Value::Bytes(b"2001:db8::8".to_vec())),
                        (b"port".to_vec(), Value::Int(6881)),
                    ]),
                    Value::Dict(vec![
                        (b"ip".to_vec(), Value::Bytes(b"::".to_vec())),
                        (b"port".to_vec(), Value::Int(6882)),
                    ]),
                    Value::Dict(vec![
                        (b"ip".to_vec(), Value::Bytes(b"192.0.2.8".to_vec())),
                        (b"port".to_vec(), Value::Int(0)),
                    ]),
                ]),
            ),
        ]));
        let response = parse_response(&body, &req()).unwrap();
        assert_eq!(response.peers, vec!["[2001:db8::8]:6881".parse().unwrap()]);
    }

    #[test]
    fn accepts_unsorted_response_and_trailing_whitespace() {
        let body = b"d5:peers0:8:intervali60ee\r\n";
        let response = parse_response(body, &req()).unwrap();

        assert_eq!(response.interval, Duration::from_secs(60));
        assert!(response.peers.is_empty());
    }

    #[tokio::test]
    async fn obfuscated_announce_sends_sha_ih_and_decrypts_peers() {
        use super::super::obfuscation::{deobfuscate_peers, obscure_port, sha_ih};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut request = req();
        request.obfuscate = true;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/announce", listener.local_addr().unwrap());
        let info_hash = request.info_hash;
        let tracker = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = socket.read(&mut buf).await.unwrap();
            let head = String::from_utf8_lossy(&buf[..n]).to_string();
            // Tracker-side obfuscation of two peers, keyed with an IV
            let plain = [198u8, 51, 100, 2, 0x1a, 0xe1, 203, 0, 113, 5, 0x1a, 0xe2];
            let iv = b"rotating-iv".to_vec();
            let peers = deobfuscate_peers(&info_hash, Some(&iv), None, None, &plain, 6).unwrap();
            let body = encode_to_vec(&Value::Dict(vec![
                (b"interval".to_vec(), Value::Int(600)),
                (b"iv".to_vec(), Value::Bytes(iv)),
                (b"peers".to_vec(), Value::Bytes(peers)),
            ]));
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
            head
        });

        let response = super::super::announce_with_proxy_and_source(
            &url,
            &request,
            Duration::from_secs(10),
            None,
            None,
        )
        .await
        .unwrap();
        let head = tracker.await.unwrap();
        let expected_sha = percent_encode(sha_ih(&info_hash).as_bytes(), QUERY_SAFE).to_string();
        assert!(head.contains(&format!("sha_ih={expected_sha}")));
        assert!(!head.contains("info_hash="), "BEP 8 forbids sending both");
        let obscured = obscure_port(&info_hash, request.port);
        assert!(head.contains(&format!("&port={obscured}&")));
        assert_eq!(
            response.peers,
            vec![
                "198.51.100.2:6881".parse::<SocketAddr>().unwrap(),
                "203.0.113.5:6882".parse().unwrap(),
            ]
        );
    }

    #[test]
    fn obfuscated_window_outside_u32_drops_the_peer_list() {
        use super::super::obfuscation::deobfuscate_peers;

        let mut request = req();
        request.obfuscate = true;
        let plain = [198u8, 51, 100, 2, 0x1a, 0xe1, 203, 0, 113, 5, 0x1a, 0xe2];
        let iv = b"rotating-iv".to_vec();
        let peers =
            deobfuscate_peers(&request.info_hash, Some(&iv), None, None, &plain, 6).unwrap();
        let body = |window: Option<(&[u8], i64)>| {
            let mut dict = vec![
                (b"interval".to_vec(), Value::Int(600)),
                (b"iv".to_vec(), Value::Bytes(iv.clone())),
                (b"peers".to_vec(), Value::Bytes(peers.clone())),
            ];
            dict.extend(window.map(|(key, n)| (key.to_vec(), Value::Int(n))));
            encode_to_vec(&Value::Dict(dict))
        };
        let parsed = |window| parse_response(&body(window), &request).unwrap().peers;

        assert_eq!(parsed(None).len(), 2);
        assert!(parsed(Some((b"i", -1))).is_empty());
        assert!(parsed(Some((b"n", 1 << 32))).is_empty());
    }

    #[test]
    fn propagates_failure_reason() {
        let body = encode_to_vec(&Value::Dict(vec![(
            b"failure reason".to_vec(),
            Value::Bytes(b"nope".to_vec()),
        )]));
        let err = parse_response(&body, &req()).unwrap_err();
        assert!(matches!(err, TrackerError::Rejected(ref m) if m == "nope"));
    }
}
