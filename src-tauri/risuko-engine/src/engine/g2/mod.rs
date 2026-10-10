use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::engine::gnutella::download::{run_urn_fetch, UrnFetch};
use crate::engine::gnutella::types::{split_uri, url_decode, GnutellaError};
use crate::engine::options::EngineOptions;

pub struct G2Link {
    pub host: String,
    pub port: u16,
    pub urn: Option<String>,
    pub file_name: String,
    pub file_size: u64,
}

pub fn is_g2_uri(uri: &str) -> bool {
    let lower = uri.trim().to_ascii_lowercase();
    lower.starts_with("g2://")
}

pub fn parse_g2_uri(uri: &str) -> Option<G2Link> {
    let s = uri.trim();
    let rest = s
        .strip_prefix("g2://")
        .or_else(|| s.strip_prefix("G2://"))?;
    let (host, port, path, query) = split_uri(rest)?;
    let mut urn: Option<String> = None;
    if let Some(rest) = path.strip_prefix("/sha1/") {
        urn = Some(format!("urn:sha1:{}", rest.trim_end_matches('/')));
    }
    let mut file_name = String::new();
    let mut file_size: u64 = 0;
    for part in query.split('&') {
        if let Some(rest) = part.strip_prefix("dn=") {
            file_name = url_decode(rest);
        } else if let Some(rest) = part.strip_prefix("xl=") {
            file_size = rest.parse().unwrap_or(0);
        } else if let Some(rest) = part.strip_prefix("urn=") {
            if urn.is_none() {
                if rest.starts_with("urn:sha1:") {
                    urn = Some(rest.to_string());
                } else if rest.len() == 32
                    && rest
                        .bytes()
                        .all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'2'..=b'7'))
                {
                    urn = Some(format!("urn:sha1:{}", rest.to_ascii_uppercase()));
                }
            }
        }
    }
    Some(G2Link {
        host,
        port,
        urn,
        file_name,
        file_size,
    })
}

pub async fn run_g2_download(
    uri: &str,
    dir: &str,
    opts: &EngineOptions,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
    cancel_token: CancellationToken,
) -> Result<PathBuf, GnutellaError> {
    if !is_g2_uri(uri) {
        return Err(GnutellaError::InvalidUri(format!("not a G2 URI: {uri}")));
    }
    let proxy = opts.p2p_proxy_connector().map_err(GnutellaError::Network)?;
    let link =
        parse_g2_uri(uri).ok_or_else(|| GnutellaError::InvalidUri("invalid g2 URI".into()))?;
    let urn = link
        .urn
        .as_deref()
        .ok_or_else(|| GnutellaError::InvalidUri("G2 URI missing sha1/urn".into()))?;
    if link.file_size == 0 {
        return Err(GnutellaError::InvalidUri("G2 URI missing xl/size".into()));
    }
    total.store(link.file_size, Ordering::Relaxed);
    if cancel_token.is_cancelled() {
        return Err(GnutellaError::Network("cancelled".into()));
    }
    run_urn_fetch(
        UrnFetch {
            host: &link.host,
            port: link.port,
            n2r_path: "/uri-res/N2R",
            urn,
            file_size: link.file_size,
            file_name: &link.file_name,
            default_name: "g2-download",
            dir,
        },
        completed,
        speed,
        connections,
        cancel_token,
        proxy,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn detects() {
        assert!(is_g2_uri("g2://h:6346/sha1/ABC?xl=10&dn=x"));
        assert!(!is_g2_uri("gnutella://"));
    }
    #[test]
    fn parses() {
        let l = parse_g2_uri("g2://peer.example.com:6346/sha1/ABCDEF?xl=42&dn=foo.bin").unwrap();
        assert_eq!(l.host, "peer.example.com");
        assert_eq!(l.port, 6346);
        assert_eq!(l.urn.as_deref(), Some("urn:sha1:ABCDEF"));
        assert_eq!(l.file_size, 42);
        assert_eq!(l.file_name, "foo.bin");
    }
}
