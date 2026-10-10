#[derive(Debug, thiserror::Error)]
pub enum GnutellaError {
    #[error("invalid URI: {0}")]
    InvalidUri(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("network: {0}")]
    Network(String),
    #[error("no source has the requested file")]
    NoSource,
}

pub struct GnutellaLink {
    pub host: String,
    pub port: u16,
    pub urn: Option<String>,
    pub file_name: String,
    pub file_size: u64,
    pub n2r_path: String,
}

pub fn is_gnutella_uri(uri: &str) -> bool {
    let lower = uri.trim().to_ascii_lowercase();
    lower.starts_with("gnutella://") || lower.starts_with("gnet://")
}

pub fn parse_gnutella_uri(uri: &str) -> Option<GnutellaLink> {
    let s = uri.trim();
    let lower = s.to_ascii_lowercase();
    let prefix_len = if lower.starts_with("gnutella://") {
        "gnutella://".len()
    } else if lower.starts_with("gnet://") {
        "gnet://".len()
    } else {
        return None;
    };
    let (host, port, path, query) = split_uri(&s[prefix_len..])?;

    let mut file_name = String::new();
    let mut file_size: u64 = 0;
    let mut urn: Option<String> = None;
    if !query.is_empty() {
        for part in query.split('&') {
            if let Some(rest) = part.strip_prefix("urn=") {
                urn = Some(rest.to_string());
            } else if let Some(rest) = part.strip_prefix("dn=") {
                file_name = url_decode(rest);
            } else if let Some(rest) = part.strip_prefix("xl=") {
                file_size = rest.parse().unwrap_or(0);
            } else if part.starts_with("urn:sha1:") || part.starts_with("urn:bitprint:") {
                urn = Some(part.to_string());
            }
        }
    }
    if file_name.is_empty() {
        if let Some(name) = path.rsplit('/').next() {
            file_name = url_decode(name);
        }
    }
    Some(GnutellaLink {
        host,
        port,
        urn,
        file_name,
        file_size,
        n2r_path: path.to_string(),
    })
}

pub(crate) fn url_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(&s.replace('+', " "))
        .decode_utf8_lossy()
        .to_string()
}

pub(crate) fn split_uri(rest: &str) -> Option<(String, u16, &str, &str)> {
    let (authority, path_query) = match rest.find(['/', '?']) {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/"),
    };
    let (host, port) = if let Some(inner) = authority.strip_prefix('[') {
        let close = inner.find(']')?;
        let port = match &inner[close + 1..] {
            "" => 6346,
            tail => tail.strip_prefix(':')?.parse().ok()?,
        };
        (inner[..close].to_string(), port)
    } else if let Some(idx) = authority.rfind(':') {
        (
            authority[..idx].to_string(),
            authority[idx + 1..].parse().ok()?,
        )
    } else {
        (authority.to_string(), 6346)
    };
    if host.is_empty() {
        return None;
    }
    let (path, query) = match path_query.find('?') {
        Some(idx) => (&path_query[..idx], &path_query[idx + 1..]),
        None => (path_query, ""),
    };
    Some((host, port, path, query))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn detects() {
        assert!(is_gnutella_uri(
            "gnutella://host:6346/uri-res/N2R?urn:sha1:ABC"
        ));
        assert!(is_gnutella_uri("gnet://x"));
        assert!(!is_gnutella_uri("ed2k://"));
    }
    #[test]
    fn parses_n2r() {
        let l = parse_gnutella_uri(
            "gnutella://peer.example.com:6346/uri-res/N2R?urn:sha1:PLSTQHKO5F2F5OJG6DNCKEXNV6YLQ47A",
        )
        .unwrap();
        assert_eq!(l.host, "peer.example.com");
        assert_eq!(l.port, 6346);
        assert_eq!(l.n2r_path, "/uri-res/N2R");
        assert!(l.urn.unwrap().starts_with("urn:sha1:"));
    }
    #[test]
    fn splits_ipv6_and_rejects_empty_host() {
        let (h, p, path, q) = split_uri("[::1]:7000/uri-res/N2R?x=1").unwrap();
        assert_eq!(
            (h.as_str(), p, path, q),
            ("::1", 7000, "/uri-res/N2R", "x=1")
        );
        let (h, p, _, _) = split_uri("[2001:db8::1]/a").unwrap();
        assert_eq!((h.as_str(), p), ("2001:db8::1", 6346));
        assert!(split_uri(":80/a").is_none());
        assert!(split_uri("h:bad/a").is_none());
    }
    #[test]
    fn query_directly_after_authority_is_not_host() {
        let (h, p, path, q) = split_uri("host:7000?urn=urn:sha1:ABC&xl=5").unwrap();
        assert_eq!(
            (h.as_str(), p, path, q),
            ("host", 7000, "", "urn=urn:sha1:ABC&xl=5")
        );
        let (h, p, _, q) = split_uri("[::1]?dn=a").unwrap();
        assert_eq!((h.as_str(), p, q), ("::1", 6346, "dn=a"));
        assert!(split_uri("host:bad?x=1").is_none());
        let l =
            parse_gnutella_uri("gnutella://peer:6346?urn=urn:sha1:ABC&dn=a+b.bin&xl=10").unwrap();
        assert_eq!((l.host.as_str(), l.port), ("peer", 6346));
        assert_eq!(l.urn.as_deref(), Some("urn:sha1:ABC"));
        assert_eq!((l.file_name.as_str(), l.file_size), ("a b.bin", 10));
    }
}
