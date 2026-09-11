//! BEP-19 WebSeed metadata, URL construction and HTTP range validation

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use std::net::IpAddr;
use url::Url;

use crate::bencode::Value;
pub const DEFAULT_MAX_RANGE_BYTES: u64 = 16 * 1024 * 1024;

pub fn is_allowed_destination(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            !ip.is_unspecified()
                && !ip.is_loopback()
                && !ip.is_private()
                && !ip.is_link_local()
                && !ip.is_multicast()
                && !ip.is_broadcast()
        }
        IpAddr::V6(ip) => {
            if ip.is_unspecified() || ip.is_loopback() {
                return false;
            }
            if let Some(ipv4) = ip.to_ipv4() {
                return is_allowed_destination(IpAddr::V4(ipv4));
            }
            !ip.is_unicast_link_local()
                && !ip.is_unique_local()
                && !ip.is_multicast()
        }
    }
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum WebSeedError {
    #[error("invalid WebSeed URL: {0}")]
    InvalidUrl(String),
    #[error("multi-file WebSeed URL must end with '/'")]
    MultiFileBaseMissingSlash,
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
    #[error("WebSeed HTTP request failed: {0}")]
    Http(String),
}

/// A validated inclusive HTTP byte range
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

/// Parse BEP-19's top-level `url-list` value
pub fn parse_url_list(value: &Value) -> Vec<String> {
    let values = match value {
        Value::Bytes(_) => std::slice::from_ref(value),
        Value::List(items) => items.as_slice(),
        _ => return Vec::new(),
    };

    let mut out = Vec::new();
    for item in values {
        let Some(raw) = item.as_str() else {
            continue;
        };
        let raw = raw.trim();
        let Ok(mut url) = Url::parse(raw) else {
            continue;
        };
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            continue;
        }
        url.set_fragment(None);
        let normalized = url.to_string();
        if !out.iter().any(|existing| existing == &normalized) {
            out.push(normalized);
        }
    }
    out
}

/// Validate and normalize one WebSeed base URL
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
        if !root_form {
            return Err(WebSeedError::MultiFileBaseMissingSlash);
        }
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

pub async fn fetch_range(
    client: &risuko_http::Client,
    url: &Url,
    requested: ByteRange,
    max_body: u64,
) -> Result<Bytes, WebSeedError> {
    Ok(fetch_range_response(client, url, requested, max_body)
        .await?
        .body)
}

pub async fn fetch_range_response(
    client: &risuko_http::Client,
    url: &Url,
    requested: ByteRange,
    max_body: u64,
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
        validate_range_response(
            status,
            content_range.as_deref(),
            0,
            requested,
            max_body,
        )?;
    }
    if let Some(length) = response.content_length() {
        if length > max_body {
            return Err(WebSeedError::BodyTooLarge {
                actual: length,
                limit: max_body,
            });
        }
    }
    let mut stream = response.bytes_stream();
    let mut collected = BytesMut::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| WebSeedError::Http(e.to_string()))?;
        let next_len = collected.len().saturating_add(chunk.len());
        if next_len as u64 > max_body {
            return Err(WebSeedError::BodyTooLarge {
                actual: next_len as u64,
                limit: max_body,
            });
        }
        collected.extend_from_slice(&chunk);
    }
    let body = collected.freeze();
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
    matches!(status, 408 | 425 | 429 | 500 | 502 | 503 | 504)
}

pub fn is_permanent_status(status: u16) -> bool {
    matches!(status, 400 | 401 | 403 | 404 | 405 | 410 | 416)
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
    fn rejects_internal_webseed_destinations() {
        for raw in [
            "127.0.0.1",
            "10.0.0.1",
            "169.254.1.1",
            "::1",
            "::127.0.0.1",
            "::ffff:127.0.0.1",
            "fc00::1",
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
    fn rejects_non_root_multi_file_base() {
        let err = build_file_url("https://mirror.example/root", "root", &[], false).unwrap_err();
        assert_eq!(err, WebSeedError::MultiFileBaseMissingSlash);
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
