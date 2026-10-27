use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use percent_encoding::{percent_encode, NON_ALPHANUMERIC};

use super::super::bencode::{decode_all_external, DecodeLimits, Value};
use super::{is_valid_endpoint, AnnounceRequest, AnnounceResponse, RetryIn, TrackerError};

const TRACKER_RESPONSE_LIMITS: DecodeLimits = DecodeLimits::new(2 * 1024 * 1024, 64, 262_144);
const MAX_RETRY_IN: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const MAX_TRACKER_ID_LEN: usize = 256;
const MAX_WARNING_LEN: usize = 512;
const MAX_RESPONSE_PEERS: usize = 1000;

const QUERY_SAFE: &percent_encoding::AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

fn client_builder() -> risuko_http::ClientBuilder {
    risuko_http::Client::builder()
        .user_agent(crate::core::peer_id::CLIENT_VERSION)
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(5))
        .pool_max_idle_per_host(4)
}

fn client() -> &'static risuko_http::Client {
    static CLIENT: OnceLock<risuko_http::Client> = OnceLock::new();
    CLIENT.get_or_init(|| client_builder().build().expect("build risuko-http client"))
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

    let client = client_builder()
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

pub(super) fn routes_via_proxy(url: &str, proxy: &risuko_http::ProxyConnector) -> bool {
    proxy.proxy().is_some()
        && !url::Url::parse(url).is_ok_and(|url| {
            proxy
                .no_proxy()
                .is_some_and(|no_proxy| no_proxy.matches_url(&url))
        })
}

fn proxy_client(
    proxy: &risuko_http::ProxyConnector,
    source: Option<SocketAddr>,
) -> Result<risuko_http::Client, TrackerError> {
    let mut builder = client_builder();
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

const MAX_ANNOUNCE_BYTES: usize = 2 * 1024 * 1024;

async fn fetch_announce(
    client: &risuko_http::Client,
    url: &str,
    req: &AnnounceRequest,
) -> Result<AnnounceResponse, TrackerError> {
    let query = build_query(req);
    let sep = if url.contains('?') { '&' } else { '?' };
    let full = format!("{}{}{}", url, sep, query);
    let resp = client
        .get(&full)
        .send()
        .await
        .map_err(|e| TrackerError::Http(e.to_string()))?;
    let status = resp.status();
    if !status.is_success() {
        if let Ok(body) = resp.bytes_limited(MAX_ANNOUNCE_BYTES).await {
            if let Err(rejected @ TrackerError::Rejected { .. }) = parse_response(&body, req) {
                return Err(rejected);
            }
        }
        return Err(TrackerError::Http(format!("HTTP status {status}")));
    }
    let bytes = resp
        .bytes_limited(MAX_ANNOUNCE_BYTES)
        .await
        .map_err(|e| TrackerError::Http(e.to_string()))?;
    let response = parse_response(&bytes, req)?;
    if let Some(warning) = &response.warning {
        tracing::debug!(
            "tracker {} warning: {warning:?}",
            super::redacted_tracker_url(url)
        );
    }
    Ok(response)
}

fn build_query(req: &AnnounceRequest) -> String {
    let pid = percent_encode(req.peer_id.as_bytes(), QUERY_SAFE);
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
    if let Some(tracker_id) = &req.tracker_id {
        s.push_str("&trackerid=");
        s.extend(percent_encode(tracker_id, QUERY_SAFE));
    }
    s
}

fn parse_retry_in(value: &Value) -> Option<RetryIn> {
    let minutes = match value {
        Value::Int(minutes) => u64::try_from(*minutes).ok()?,
        Value::Bytes(text) => {
            let text = std::str::from_utf8(text).ok()?.trim();
            if text.eq_ignore_ascii_case("never") {
                return Some(RetryIn::Never);
            }
            text.parse::<u64>().ok()?
        }
        _ => return None,
    };
    (minutes > 0)
        .then(|| RetryIn::After(Duration::from_secs(minutes.saturating_mul(60)).min(MAX_RETRY_IN)))
}

fn reveal_peers<'a>(
    raw: &'a [u8],
    stride: usize,
    req: &AnnounceRequest,
    response: &Value,
) -> Option<std::borrow::Cow<'a, [u8]>> {
    if !req.obfuscate {
        return Some(std::borrow::Cow::Borrowed(raw));
    }
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
        .ok_or_else(|| TrackerError::rejected("response not a dict"))?;

    if let Some(reason) = value.get(b"failure reason") {
        return Err(TrackerError::Rejected {
            reason: reason
                .as_bytes()
                .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                .unwrap_or_else(|| "unspecified failure".into()),
            retry_in: value.get(b"retry in").and_then(parse_retry_in),
        });
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
    if let Some(raw) = value
        .get(b"peers")
        .and_then(|v| v.as_bytes())
        .and_then(|raw| reveal_peers(raw, 6, req, &value))
    {
        for chunk in raw.chunks_exact(6) {
            if peers.len() >= MAX_RESPONSE_PEERS {
                break;
            }
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
        for entry in list {
            if peers.len() >= MAX_RESPONSE_PEERS {
                break;
            }
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

    if let Some(raw) = value
        .get(b"peers6")
        .and_then(|v| v.as_bytes())
        .and_then(|raw| reveal_peers(raw, 18, req, &value))
    {
        for chunk in raw.chunks_exact(18) {
            if peers.len() >= MAX_RESPONSE_PEERS {
                break;
            }
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

    let min_interval = value
        .get(b"min interval")
        .and_then(|v| v.as_int())
        .and_then(|n| u64::try_from(n).ok())
        .filter(|&n| n > 0)
        .map(Duration::from_secs);

    let external_ip = value
        .get(b"external ip")
        .and_then(Value::as_bytes)
        .and_then(crate::net::ip_from_bytes);

    Ok(AnnounceResponse {
        interval: Duration::from_secs(interval),
        peers,
        min_interval,
        seeders,
        leechers,
        external_ip,
        tracker_id: value
            .get(b"tracker id")
            .and_then(|v| v.as_bytes())
            .filter(|id| !id.is_empty() && id.len() <= MAX_TRACKER_ID_LEN)
            .map(<[u8]>::to_vec),
        warning: value
            .get(b"warning message")
            .and_then(|v| v.as_bytes())
            .map(|bytes| {
                String::from_utf8_lossy(&bytes[..bytes.len().min(MAX_WARNING_LEN)]).into_owned()
            }),
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
            tracker_id: None,
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
        let peers_bin: Vec<u8> = vec![1, 2, 3, 4, 0x1a, 0xe1, 5, 6, 7, 8, 0x1a, 0xe2];
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
    fn parses_external_ip_of_four_or_sixteen_bytes() {
        let parse = |external: Option<Value>| {
            let mut fields = vec![
                (b"interval".to_vec(), Value::Int(60)),
                (b"peers".to_vec(), Value::Bytes(Vec::new())),
            ];
            if let Some(external) = external {
                fields.push((b"external ip".to_vec(), external));
            }
            let body = encode_to_vec(&Value::Dict(fields));
            parse_response(&body, &req()).unwrap().external_ip
        };
        assert_eq!(
            parse(Some(Value::Bytes(vec![93, 184, 216, 34]))),
            Some("93.184.216.34".parse().unwrap())
        );
        let v6: Ipv6Addr = "2a00:1450:4001:80b::200e".parse().unwrap();
        assert_eq!(
            parse(Some(Value::Bytes(v6.octets().to_vec()))),
            Some(IpAddr::V6(v6))
        );
        for length in [0usize, 1, 3, 5, 6, 8, 15, 17, 18] {
            assert_eq!(
                parse(Some(Value::Bytes(vec![93; length]))),
                None,
                "{length}"
            );
        }
        assert_eq!(parse(Some(Value::Int(5))), None);
        assert_eq!(parse(None), None);
    }

    #[test]
    fn external_ip_is_ignored_in_a_failure_response() {
        let body = encode_to_vec(&Value::Dict(vec![
            (b"failure reason".to_vec(), Value::Bytes(b"nope".to_vec())),
            (
                b"external ip".to_vec(),
                Value::Bytes(vec![93, 184, 216, 34]),
            ),
        ]));
        assert!(parse_response(&body, &req()).is_err());
    }

    #[test]
    fn preserves_tracker_peer_order_while_deduplicating() {
        let peers_bin: Vec<u8> = vec![
            5, 6, 7, 8, 0x1a, 0xe2, 1, 2, 3, 4, 0x1a, 0xe1, 5, 6, 7, 8, 0x1a, 0xe2, 255, 255, 255,
            255, 0x1a, 0xe3,
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
        assert!(matches!(err, TrackerError::Rejected { ref reason, .. } if reason == "nope"));
    }

    fn failure_body(extra: Vec<(&str, Value)>) -> Vec<u8> {
        let mut dict = vec![(
            b"failure reason".to_vec(),
            Value::Bytes(b"unregistered torrent".to_vec()),
        )];
        dict.extend(
            extra
                .into_iter()
                .map(|(key, value)| (key.as_bytes().to_vec(), value)),
        );
        encode_to_vec(&Value::Dict(dict))
    }

    fn retry_hint(value: Value) -> Option<RetryIn> {
        match parse_response(&failure_body(vec![("retry in", value)]), &req()).unwrap_err() {
            TrackerError::Rejected { retry_in, .. } => retry_in,
            other => panic!("unexpected error {other:?}"),
        }
    }

    #[test]
    fn non_utf8_failure_reason_is_a_rejection() {
        let body = encode_to_vec(&Value::Dict(vec![(
            b"failure reason".to_vec(),
            Value::Bytes(vec![0xff, 0xfe, b'x']),
        )]));
        let err = parse_response(&body, &req()).unwrap_err();
        assert!(matches!(err, TrackerError::Rejected { ref reason, .. } if reason.ends_with('x')));
    }

    #[test]
    fn failure_reason_of_an_unexpected_type_is_a_rejection() {
        let body = encode_to_vec(&Value::Dict(vec![(
            b"failure reason".to_vec(),
            Value::Int(1),
        )]));
        let err = parse_response(&body, &req()).unwrap_err();
        assert!(matches!(err, TrackerError::Rejected { .. }));
    }

    #[test]
    fn failure_response_carries_the_retry_in_hint() {
        let minutes = |m: u64| Some(RetryIn::After(Duration::from_secs(m * 60)));
        assert_eq!(retry_hint(Value::Int(15)), minutes(15));
        assert_eq!(retry_hint(Value::Bytes(b"30".to_vec())), minutes(30));
        assert_eq!(
            retry_hint(Value::Bytes(b"never".to_vec())),
            Some(RetryIn::Never)
        );
        assert_eq!(
            retry_hint(Value::Bytes(b"Never".to_vec())),
            Some(RetryIn::Never)
        );
        assert_eq!(retry_hint(Value::Int(0)), None);
        assert_eq!(retry_hint(Value::Int(-5)), None);
        assert_eq!(retry_hint(Value::Bytes(b"soon".to_vec())), None);
        assert_eq!(retry_hint(Value::List(Vec::new())), None);
        assert_eq!(
            retry_hint(Value::Int(i64::MAX)),
            Some(RetryIn::After(MAX_RETRY_IN))
        );
        let err = parse_response(&failure_body(Vec::new()), &req()).unwrap_err();
        assert!(matches!(err, TrackerError::Rejected { retry_in: None, .. }));
    }

    #[test]
    fn success_response_surfaces_swarm_counts_tracker_id_and_warning() {
        let body = encode_to_vec(&Value::Dict(vec![
            (b"interval".to_vec(), Value::Int(900)),
            (b"complete".to_vec(), Value::Int(7)),
            (b"incomplete".to_vec(), Value::Int(3)),
            (b"tracker id".to_vec(), Value::Bytes(vec![b'a', 0xff, b'b'])),
            (
                b"warning message".to_vec(),
                Value::Bytes(b"slow down".to_vec()),
            ),
        ]));
        let response = parse_response(&body, &req()).unwrap();
        assert_eq!(response.seeders, Some(7));
        assert_eq!(response.leechers, Some(3));
        assert_eq!(response.tracker_id, Some(vec![b'a', 0xff, b'b']));
        assert_eq!(response.warning.as_deref(), Some("slow down"));
    }

    #[test]
    fn min_interval_is_surfaced_only_when_positive() {
        let min = |value: Value| {
            let body = encode_to_vec(&Value::Dict(vec![
                (b"interval".to_vec(), Value::Int(900)),
                (b"min interval".to_vec(), value),
            ]));
            parse_response(&body, &req()).unwrap().min_interval
        };
        assert_eq!(min(Value::Int(3600)), Some(Duration::from_secs(3600)));
        assert_eq!(min(Value::Int(0)), None);
        assert_eq!(min(Value::Int(-1)), None);
        assert_eq!(min(Value::Bytes(b"60".to_vec())), None);
        let body = encode_to_vec(&Value::Dict(vec![(b"interval".to_vec(), Value::Int(900))]));
        assert_eq!(parse_response(&body, &req()).unwrap().min_interval, None);
    }

    #[test]
    fn oversized_peer_lists_are_capped() {
        let compact = |count: usize| -> Vec<u8> {
            (0..count)
                .flat_map(|n| {
                    let n = n as u32 + 1;
                    let ip = [10, (n >> 16) as u8, (n >> 8) as u8, n as u8];
                    [ip[0], ip[1], ip[2], ip[3], 0x1a, 0xe1]
                })
                .collect()
        };
        let body = |key: &[u8], peers: Vec<u8>| {
            encode_to_vec(&Value::Dict(vec![
                (b"interval".to_vec(), Value::Int(900)),
                (key.to_vec(), Value::Bytes(peers)),
            ]))
        };
        let capped = parse_response(&body(b"peers", compact(50_000)), &req()).unwrap();
        assert_eq!(capped.peers.len(), MAX_RESPONSE_PEERS);
        let small = parse_response(&body(b"peers", compact(10)), &req()).unwrap();
        assert_eq!(small.peers.len(), 10);

        let mut v6 = Vec::new();
        for n in 0..50_000u32 {
            let mut octets = [0u8; 16];
            octets[0] = 0x20;
            octets[12..16].copy_from_slice(&(n + 1).to_be_bytes());
            v6.extend_from_slice(&octets);
            v6.extend_from_slice(&[0x1a, 0xe1]);
        }
        let capped_v6 = parse_response(&body(b"peers6", v6), &req()).unwrap();
        assert_eq!(capped_v6.peers.len(), MAX_RESPONSE_PEERS);
    }

    #[test]
    fn oversized_warning_messages_are_truncated() {
        let warning = |bytes: Vec<u8>| {
            let body = encode_to_vec(&Value::Dict(vec![
                (b"interval".to_vec(), Value::Int(900)),
                (b"warning message".to_vec(), Value::Bytes(bytes)),
            ]));
            parse_response(&body, &req()).unwrap().warning.unwrap()
        };
        assert_eq!(warning(b"short".to_vec()), "short");
        assert_eq!(warning(vec![b'w'; MAX_WARNING_LEN]).len(), MAX_WARNING_LEN);
        assert_eq!(warning(vec![b'w'; 1 << 20]).len(), MAX_WARNING_LEN);
    }

    #[test]
    fn empty_or_oversized_tracker_id_is_ignored() {
        let id = |bytes: Vec<u8>| {
            let body = encode_to_vec(&Value::Dict(vec![
                (b"interval".to_vec(), Value::Int(900)),
                (b"tracker id".to_vec(), Value::Bytes(bytes)),
            ]));
            parse_response(&body, &req()).unwrap().tracker_id
        };
        assert_eq!(id(Vec::new()), None);
        assert_eq!(id(vec![b'x'; MAX_TRACKER_ID_LEN + 1]), None);
        assert_eq!(
            id(vec![b'x'; MAX_TRACKER_ID_LEN]),
            Some(vec![b'x'; MAX_TRACKER_ID_LEN])
        );
    }

    #[test]
    fn query_echoes_the_tracker_id() {
        let mut request = req();
        request.tracker_id = Some(b"id 1&x\xff".to_vec());
        assert!(build_query(&request).contains("&trackerid=id%201%26x%FF"));
        assert!(!build_query(&req()).contains("trackerid"));
    }

    async fn record_requests(count: usize) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/announce", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut heads = Vec::new();
            for _ in 0..count {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                heads.push(String::from_utf8_lossy(&buf[..n]).to_string());
                let body =
                    encode_to_vec(&Value::Dict(vec![(b"interval".to_vec(), Value::Int(900))]));
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(head.as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
            heads
        });
        (url, task)
    }

    #[tokio::test]
    async fn every_tracker_client_sends_a_user_agent() {
        let (url, task) = record_requests(3).await;
        let source: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let clients = [
            client().clone(),
            source_client(source).unwrap(),
            proxy_client(&risuko_http::ProxyConnector::direct(), None).unwrap(),
        ];
        for client in &clients {
            fetch_announce(client, &url, &req()).await.unwrap();
        }
        let expected =
            format!("user-agent: {}", crate::core::peer_id::CLIENT_VERSION).to_ascii_lowercase();
        for head in task.await.unwrap() {
            assert!(head.to_ascii_lowercase().contains(&expected), "{head}");
        }
    }
}
