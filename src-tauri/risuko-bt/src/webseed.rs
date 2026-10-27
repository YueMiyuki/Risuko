use bytes::{Bytes, BytesMut};
use futures_util::{Stream, StreamExt};
use std::collections::HashSet;
use std::net::IpAddr;
use tokio::time::Instant;
use url::{Host, Url};

use crate::bencode::Value;
use crate::limiter::Throttle;
pub const DEFAULT_MAX_RANGE_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_URL_LIST_ENTRIES: usize = 64;
const BODY_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const BODY_MIN_RATE_WINDOW: std::time::Duration = std::time::Duration::from_secs(20);
const BODY_MIN_RATE_BYTES: u64 = 2 * 1024 * 20;

pub fn is_allowed_destination(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !ip.is_unspecified()
                && !ip.is_loopback()
                && !ip.is_private()
                && !ip.is_link_local()
                && !ip.is_multicast()
                && !ip.is_broadcast()
                && !crate::net::is_shared_cgnat(ip)
        }
        IpAddr::V6(ip) => {
            if ip.is_unspecified() || ip.is_loopback() {
                return false;
            }
            if let Some(ipv4) = ip.to_ipv4() {
                return is_allowed_destination(IpAddr::V4(ipv4));
            }
            let segments = ip.segments();
            !ip.is_unicast_link_local()
                && !ip.is_unique_local()
                && !ip.is_multicast()
                && (segments[0] & 0xffc0) != 0xfec0
        }
    }
}

const INTERNAL_DOMAIN_SUFFIXES: [&str; 6] = [
    "localhost",
    "local",
    "internal",
    "localdomain",
    "lan",
    "home.arpa",
];

pub(crate) fn has_internal_suffix(name: &str) -> bool {
    INTERNAL_DOMAIN_SUFFIXES.iter().any(|suffix| {
        name.strip_suffix(suffix)
            .is_some_and(|rest| rest.is_empty() || rest.ends_with('.'))
    })
}

pub(crate) fn is_allowed_webseed_url(url: &Url) -> bool {
    url.host().is_some_and(|host| is_allowed_host(&host))
}

pub(crate) fn is_allowed_host<S: AsRef<str>>(host: &Host<S>) -> bool {
    match host {
        Host::Ipv4(ip) => is_allowed_destination(IpAddr::V4(*ip)),
        Host::Ipv6(ip) => is_allowed_destination(IpAddr::V6(*ip)),
        Host::Domain(name) => {
            let name = name.as_ref().trim_end_matches('.');
            name.contains('.') && !has_internal_suffix(name)
        }
    }
}

pub(crate) fn redirect_policy() -> risuko_http::Policy {
    risuko_http::Policy::limited(3).allow_only(is_allowed_webseed_url)
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum WebSeedError {
    #[error("invalid WebSeed URL: {0}")]
    InvalidUrl(String),
    #[error("WebSeed URL is not an HTTP(S) URL")]
    UnsupportedScheme,
    #[error("invalid byte range: {0}")]
    InvalidRange(String),
    #[error("WebSeed returned HTTP status {0}, expected 206")]
    UnexpectedStatus(u16),
    #[error("missing or malformed Content-Range header")]
    MissingContentRange,
    #[error("WebSeed response body is too large ({actual} bytes, limit {limit})")]
    BodyTooLarge { actual: u64, limit: u64 },
    #[error("WebSeed response body length {actual} does not match range length {expected}")]
    BodyLength { actual: u64, expected: u64 },
    #[error("Content-Range does not match requested range")]
    RangeMismatch,
    #[error("Content-Range total length {actual} does not match expected {expected}")]
    TotalLengthMismatch { actual: u64, expected: u64 },
    #[error("WebSeed response body is too slow")]
    BodyTooSlow,
    #[error("WebSeed HTTP request failed: {0}")]
    Http(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
    pub total: u64,
}

#[derive(Debug, Clone)]
pub struct RangeResponse {
    pub body: Bytes,
    pub etag: Option<String>,
}

impl ByteRange {
    pub fn len(self) -> u64 {
        if self.is_empty() {
            0
        } else {
            self.end - self.start + 1
        }
    }

    pub fn is_empty(self) -> bool {
        self.start > self.end
    }
}

pub fn parse_url_list(value: &Value) -> Vec<String> {
    let values = match value {
        Value::Bytes(_) => std::slice::from_ref(value),
        Value::List(items) => items.as_slice(),
        _ => return Vec::new(),
    };

    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for item in values {
        if out.len() >= MAX_URL_LIST_ENTRIES {
            break;
        }
        let Some(raw) = item.as_str() else {
            continue;
        };
        let raw = raw.trim();
        let Ok(mut url) = Url::parse(raw) else {
            continue;
        };
        if !matches!(url.scheme(), "http" | "https") || !is_allowed_webseed_url(&url) {
            continue;
        }
        url.set_fragment(None);
        let normalized = url.to_string();
        if seen.insert(normalized.clone()) {
            out.push(normalized);
        }
    }
    out
}

pub fn parse_base_url(raw: &str) -> Result<Url, WebSeedError> {
    let mut url = Url::parse(raw.trim()).map_err(|e| WebSeedError::InvalidUrl(e.to_string()))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return Err(WebSeedError::UnsupportedScheme);
    }
    url.set_fragment(None);
    Ok(url)
}

pub fn build_file_url(
    base: &str,
    torrent_name: &str,
    path: &[String],
    single_file_mode: bool,
) -> Result<Url, WebSeedError> {
    if torrent_name.is_empty() {
        return Err(WebSeedError::InvalidUrl("empty torrent name".into()));
    }
    let mut url = parse_base_url(base)?;
    let root_form = url.path().ends_with('/');

    let mut segments: Vec<&str> = Vec::new();
    if single_file_mode {
        if root_form {
            segments.push(torrent_name);
        }
    } else {
        segments.push(torrent_name);
        segments.extend(path.iter().map(String::as_str));
    }

    if !segments.is_empty() {
        let mut path_segments = url
            .path_segments_mut()
            .map_err(|_| WebSeedError::InvalidUrl("URL cannot contain path segments".into()))?;
        path_segments.pop_if_empty();
        for segment in segments {
            if segment.is_empty() || segment == "." || segment == ".." {
                return Err(WebSeedError::InvalidUrl("unsafe path component".into()));
            }
            path_segments.push(segment);
        }
    }
    Ok(url)
}

pub fn validate_range(
    start: u64,
    end: u64,
    total: u64,
    max_bytes: u64,
) -> Result<ByteRange, WebSeedError> {
    if total == 0 || start > end || end >= total {
        return Err(WebSeedError::InvalidRange(format!(
            "bytes {start}-{end}/{total}"
        )));
    }
    let range = ByteRange { start, end, total };
    if range.len() > max_bytes {
        return Err(WebSeedError::BodyTooLarge {
            actual: range.len(),
            limit: max_bytes,
        });
    }
    Ok(range)
}

pub fn parse_content_range(value: &str) -> Result<ByteRange, WebSeedError> {
    let (unit, value) = value
        .trim()
        .split_once(char::is_whitespace)
        .ok_or(WebSeedError::MissingContentRange)?;
    if !unit.eq_ignore_ascii_case("bytes") {
        return Err(WebSeedError::MissingContentRange);
    }
    let value = value.trim();
    let (range, total) = value
        .split_once('/')
        .ok_or(WebSeedError::MissingContentRange)?;
    let total = total
        .trim()
        .parse::<u64>()
        .map_err(|_| WebSeedError::MissingContentRange)?;
    let (start, end) = range
        .trim()
        .split_once('-')
        .ok_or(WebSeedError::MissingContentRange)?;
    let start = start
        .trim()
        .parse::<u64>()
        .map_err(|_| WebSeedError::MissingContentRange)?;
    let end = end
        .trim()
        .parse::<u64>()
        .map_err(|_| WebSeedError::MissingContentRange)?;
    if total == 0 || start > end || end >= total {
        return Err(WebSeedError::MissingContentRange);
    }
    Ok(ByteRange { start, end, total })
}

pub fn validate_range_response(
    status: u16,
    content_range: Option<&str>,
    body_len: u64,
    requested: ByteRange,
    max_body: u64,
) -> Result<ByteRange, WebSeedError> {
    if status != 206 {
        return Err(WebSeedError::UnexpectedStatus(status));
    }
    if body_len > max_body {
        return Err(WebSeedError::BodyTooLarge {
            actual: body_len,
            limit: max_body,
        });
    }
    let returned = parse_content_range(content_range.ok_or(WebSeedError::MissingContentRange)?)?;
    if returned.total != requested.total {
        return Err(WebSeedError::TotalLengthMismatch {
            actual: returned.total,
            expected: requested.total,
        });
    }
    if returned.start != requested.start || returned.end != requested.end {
        return Err(WebSeedError::RangeMismatch);
    }
    let expected = returned.len();
    if body_len != expected {
        return Err(WebSeedError::BodyLength {
            actual: body_len,
            expected,
        });
    }
    Ok(returned)
}

async fn collect_body<S, E>(
    mut stream: S,
    requested: ByteRange,
    max_body: u64,
    throttle: &Throttle,
) -> Result<Bytes, WebSeedError>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: std::fmt::Display,
{
    let mut collected = BytesMut::with_capacity(requested.len().min(max_body) as usize);
    let mut window_start = Instant::now();
    let mut window_bytes = 0u64;
    loop {
        let chunk = match tokio::time::timeout(BODY_IDLE_TIMEOUT, stream.next()).await {
            Ok(Some(chunk)) => chunk.map_err(|e| WebSeedError::Http(e.to_string()))?,
            Ok(None) => break,
            Err(_) => return Err(WebSeedError::Http("response body stalled".into())),
        };
        let next_len = collected.len().saturating_add(chunk.len());
        if next_len as u64 > max_body {
            return Err(WebSeedError::BodyTooLarge {
                actual: next_len as u64,
                limit: max_body,
            });
        }
        collected.extend_from_slice(&chunk);
        window_bytes += chunk.len() as u64;
        let throttled_at = Instant::now();
        throttle.acquire(chunk.len()).await;
        window_start += throttled_at.elapsed();
        if window_start.elapsed() >= BODY_MIN_RATE_WINDOW {
            if window_bytes < BODY_MIN_RATE_BYTES && (collected.len() as u64) < requested.len() {
                return Err(WebSeedError::BodyTooSlow);
            }
            window_start = Instant::now();
            window_bytes = 0;
        }
    }
    Ok(collected.freeze())
}

pub async fn fetch_range_response(
    client: &risuko_http::Client,
    url: &Url,
    requested: ByteRange,
    max_body: u64,
    throttle: &Throttle,
) -> Result<RangeResponse, WebSeedError> {
    let range_header = format!("bytes={}-{}", requested.start, requested.end);
    let response = client
        .get(url.clone())
        .header("Range", range_header)
        .header("Accept-Encoding", "identity")
        .send()
        .await
        .map_err(|e| WebSeedError::Http(e.to_string()))?;

    let status = response.status().as_u16();
    let content_range = response
        .headers()
        .get("content-range")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let etag = response
        .headers()
        .get("etag")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if status != 206 {
        validate_range_response(status, content_range.as_deref(), 0, requested, max_body)?;
    }
    if let Some(length) = response.content_length() {
        if length > max_body {
            return Err(WebSeedError::BodyTooLarge {
                actual: length,
                limit: max_body,
            });
        }
    }
    let body = collect_body(response.bytes_stream(), requested, max_body, throttle).await?;
    validate_range_response(
        status,
        content_range.as_deref(),
        body.len() as u64,
        requested,
        max_body,
    )?;
    Ok(RangeResponse { body, etag })
}

pub fn is_retryable_status(status: u16) -> bool {
    matches!(status, 403 | 408 | 425 | 429 | 500..=599)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_scalar_and_list_url_list() {
        let scalar = Value::Bytes(b"https://mirror.example/file".to_vec());
        assert_eq!(parse_url_list(&scalar), vec!["https://mirror.example/file"]);

        let with_fragment = Value::Bytes(b"https://mirror.example/file#piece".to_vec());
        assert_eq!(
            parse_url_list(&with_fragment),
            vec!["https://mirror.example/file"]
        );

        let list = Value::List(vec![
            Value::Bytes(b"https://mirror.example/".to_vec()),
            Value::Bytes(b"ftp://mirror.example/file".to_vec()),
            Value::Bytes(b"https://mirror.example/".to_vec()),
            Value::Bytes(vec![0xff]),
        ]);
        assert_eq!(parse_url_list(&list), vec!["https://mirror.example/"]);
    }

    #[test]
    fn bounds_url_list_and_deduplicates_in_order() {
        let mut values = vec![Value::Bytes(b"https://mirror.example/first".to_vec())];
        values.extend(
            (0..MAX_URL_LIST_ENTRIES + 4)
                .map(|i| Value::Bytes(format!("https://mirror.example/{i}").into_bytes())),
        );
        values.push(Value::Bytes(b"https://mirror.example/first".to_vec()));
        let urls = parse_url_list(&Value::List(values));
        assert_eq!(urls.len(), MAX_URL_LIST_ENTRIES);
        assert_eq!(
            urls.first().map(String::as_str),
            Some("https://mirror.example/first")
        );
        assert_eq!(urls[1], "https://mirror.example/0");
    }

    #[test]
    fn rejects_internal_webseed_destinations() {
        for raw in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.1.1",
            "::1",
            "::127.0.0.1",
            "::ffff:127.0.0.1",
            "100.64.0.1",
            "::ffff:100.64.0.1",
            "::100.64.0.1",
            "fc00::1",
            "fec0::1",
        ] {
            assert!(
                !is_allowed_destination(raw.parse().unwrap()),
                "accepted {raw}"
            );
        }
        assert!(is_allowed_destination("192.0.2.1".parse().unwrap()));
        assert!(is_allowed_destination("::192.0.2.1".parse().unwrap()));
    }

    #[test]
    fn builds_single_and_multi_file_urls() {
        let path = vec!["dir".into(), "a file.bin".into()];
        let single =
            build_file_url("https://mirror.example/pub/", "payload.bin", &[], true).unwrap();
        assert_eq!(single.as_str(), "https://mirror.example/pub/payload.bin");

        let direct = build_file_url(
            "https://mirror.example/payload.bin?download=1",
            "payload.bin",
            &[],
            true,
        )
        .unwrap();
        assert_eq!(
            direct.as_str(),
            "https://mirror.example/payload.bin?download=1"
        );

        let fragment = build_file_url(
            "https://mirror.example/payload.bin#download",
            "payload.bin",
            &[],
            true,
        )
        .unwrap();
        assert_eq!(fragment.as_str(), "https://mirror.example/payload.bin");

        let multi = build_file_url("https://mirror.example/pub/", "root", &path, false).unwrap();
        assert_eq!(
            multi.as_str(),
            "https://mirror.example/pub/root/dir/a%20file.bin"
        );
    }

    #[test]
    fn appends_missing_slash_to_multi_file_base() {
        let path = vec!["a".to_string()];
        let url = build_file_url("https://mirror.example/root", "root", &path, false).unwrap();
        assert_eq!(url.as_str(), "https://mirror.example/root/root/a");

        let query =
            build_file_url("https://mirror.example/pub/set?k=1", "root", &path, false).unwrap();
        assert_eq!(query.as_str(), "https://mirror.example/pub/set/root/a?k=1");

        let bare = build_file_url("https://mirror.example", "root", &path, false).unwrap();
        assert_eq!(bare.as_str(), "https://mirror.example/root/a");
    }

    #[test]
    fn validates_content_range_and_body() {
        let request = validate_range(0, 15, 100, DEFAULT_MAX_RANGE_BYTES).unwrap();
        let got = validate_range_response(
            206,
            Some("bytes 0-15/100"),
            16,
            request,
            DEFAULT_MAX_RANGE_BYTES,
        )
        .unwrap();
        assert_eq!(got, request);

        assert_eq!(
            validate_range_response(
                200,
                Some("bytes 0-15/100"),
                16,
                request,
                DEFAULT_MAX_RANGE_BYTES,
            )
            .unwrap_err(),
            WebSeedError::UnexpectedStatus(200)
        );
        assert_eq!(
            validate_range_response(
                206,
                Some("bytes 0-14/100"),
                15,
                request,
                DEFAULT_MAX_RANGE_BYTES,
            )
            .unwrap_err(),
            WebSeedError::RangeMismatch
        );
    }

    #[test]
    fn rejects_internal_webseed_hosts() {
        for raw in [
            "http://127.0.0.1:6800/jsonrpc",
            "http://[::1]/x",
            "http://10.0.0.5/x",
            "http://192.168.1.1/x",
            "http://169.254.169.254/latest/meta-data/",
            "http://[::ffff:127.0.0.1]/x",
            "http://2130706433/x",
            "http://0x7f.1/x",
            "http://localhost/x",
            "http://localhost./x",
            "http://api.localhost/x",
            "http://printer.local/x",
            "http://metadata.google.internal/x",
            "http://router.home.arpa/x",
            "http://home.arpa/x",
            "http://home.arpa./x",
            "http://nas/x",
        ] {
            assert!(
                !is_allowed_webseed_url(&Url::parse(raw).unwrap()),
                "accepted {raw}"
            );
        }
        for raw in [
            "http://192.0.2.1/x",
            "https://mirror.example/x",
            "https://downloads.example.co.uk:8443/a",
            "https://[2001:db8::1]/x",
        ] {
            assert!(
                is_allowed_webseed_url(&Url::parse(raw).unwrap()),
                "rejected {raw}"
            );
        }
    }

    #[test]
    fn url_list_drops_internal_webseed_hosts() {
        let entry = |raw: &str| Value::Bytes(raw.as_bytes().to_vec());
        let list = Value::List(vec![
            entry("http://127.0.0.1:6800/jsonrpc"),
            entry("http://192.168.1.1/x"),
            entry("http://169.254.169.254/latest/meta-data/"),
            entry("http://localhost/x"),
            entry("https://mirror.example/pub/"),
        ]);
        assert_eq!(parse_url_list(&list), vec!["https://mirror.example/pub/"]);
    }

    #[tokio::test]
    async fn webseed_redirects_to_internal_hosts_are_not_followed() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let first = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_port = first.local_addr().unwrap().port();
        let second = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let second_port = second.local_addr().unwrap().port();
        let origin = tokio::spawn(async move {
            let (mut socket, _) = first.accept().await.unwrap();
            let mut request = vec![0u8; 2048];
            let _ = socket.read(&mut request).await.unwrap();
            let response = format!(
                "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:{second_port}/jsonrpc\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });

        let client = risuko_http::Client::builder()
            .redirect(redirect_policy())
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .unwrap();
        let url = Url::parse(&format!("http://127.0.0.1:{first_port}/file")).unwrap();
        let requested = validate_range(0, 15, 16, DEFAULT_MAX_RANGE_BYTES).unwrap();
        let error = fetch_range_response(
            &client,
            &url,
            requested,
            DEFAULT_MAX_RANGE_BYTES,
            &Throttle::unlimited(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, WebSeedError::Http(_)), "{error}");
        tokio::time::timeout(std::time::Duration::from_secs(5), origin)
            .await
            .expect("origin was never contacted")
            .unwrap();
        let contacted =
            tokio::time::timeout(std::time::Duration::from_millis(300), second.accept()).await;
        assert!(contacted.is_err(), "redirect target was contacted");
    }

    fn paced_stream(
        chunks: usize,
        chunk_len: usize,
        gap: std::time::Duration,
    ) -> std::pin::Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send>> {
        Box::pin(futures_util::stream::unfold(
            0usize,
            move |sent| async move {
                if sent == chunks {
                    return None;
                }
                if !gap.is_zero() {
                    tokio::time::sleep(gap).await;
                }
                Some((Ok(Bytes::from(vec![0u8; chunk_len])), sent + 1))
            },
        ))
    }

    #[tokio::test(start_paused = true)]
    async fn trickling_body_is_aborted_as_too_slow() {
        let requested = validate_range(0, (1 << 20) - 1, 1 << 20, DEFAULT_MAX_RANGE_BYTES).unwrap();
        let stream = paced_stream(1 << 20, 1, std::time::Duration::from_secs(25));
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            collect_body(
                stream,
                requested,
                DEFAULT_MAX_RANGE_BYTES,
                &Throttle::unlimited(),
            ),
        )
        .await
        .expect("a trickling body was never aborted");
        assert_eq!(result.unwrap_err(), WebSeedError::BodyTooSlow);
    }

    #[tokio::test(start_paused = true)]
    async fn bodies_above_the_minimum_rate_are_collected() {
        let one_second = std::time::Duration::from_secs(1);
        let requested =
            validate_range(0, (800 << 10) - 1, 1 << 20, DEFAULT_MAX_RANGE_BYTES).unwrap();
        let body = collect_body(
            paced_stream(40, 20 << 10, one_second),
            requested,
            DEFAULT_MAX_RANGE_BYTES,
            &Throttle::unlimited(),
        )
        .await
        .unwrap();
        assert_eq!(body.len(), 800 << 10);
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_but_steady_link_still_completes_a_large_range() {
        let one_second = std::time::Duration::from_secs(1);
        let requested =
            validate_range(0, (400 << 10) - 1, 1 << 20, DEFAULT_MAX_RANGE_BYTES).unwrap();
        let body = collect_body(
            paced_stream(40, 10 << 10, one_second),
            requested,
            DEFAULT_MAX_RANGE_BYTES,
            &Throttle::unlimited(),
        )
        .await
        .unwrap();
        assert_eq!(body.len(), 400 << 10);
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_applies_per_chunk_without_counting_as_slowness() {
        let throttle = Throttle::unlimited();
        throttle.task().set_limit(8 << 10);
        let requested =
            validate_range(0, (200 << 10) - 1, 1 << 20, DEFAULT_MAX_RANGE_BYTES).unwrap();
        let started = tokio::time::Instant::now();
        let body = collect_body(
            paced_stream(25, 8 << 10, std::time::Duration::ZERO),
            requested,
            DEFAULT_MAX_RANGE_BYTES,
            &throttle,
        )
        .await
        .unwrap();
        assert_eq!(body.len(), 200 << 10);
        assert!(started.elapsed() >= std::time::Duration::from_secs(20));
    }

    #[test]
    fn rejects_oversized_ranges_and_truncated_bodies() {
        assert_eq!(
            ByteRange {
                start: 2,
                end: 1,
                total: 10,
            }
            .len(),
            0
        );
        assert!(matches!(
            validate_range(0, 100, 101, 100),
            Err(WebSeedError::BodyTooLarge { .. })
        ));
        let request = validate_range(0, 9, 10, 100).unwrap();
        assert!(matches!(
            validate_range_response(206, Some("bytes 0-9/10"), 9, request, 100),
            Err(WebSeedError::BodyLength { .. })
        ));
    }
}
