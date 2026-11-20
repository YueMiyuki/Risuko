use std::time::{SystemTime, UNIX_EPOCH};

pub use risuko_util::is_windows_device_name;
pub(crate) use risuko_util::safe_filename;

pub(crate) const ERROR_SNIPPET_BYTES: usize = 512;

pub(crate) const RESPONSE_BODY_LIMIT: usize = 64 * 1024;

pub(crate) fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn atomic_saturating_sub(counter: &std::sync::atomic::AtomicU64, amount: u64) {
    use std::sync::atomic::Ordering;
    let mut current = counter.load(Ordering::Relaxed);
    while let Err(actual) = counter.compare_exchange_weak(
        current,
        current.saturating_sub(amount),
        Ordering::Relaxed,
        Ordering::Relaxed,
    ) {
        current = actual;
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn reserve_unique_path(
    dir: &std::path::Path,
    name: &str,
) -> Result<std::path::PathBuf, String> {
    let (stem, ext) = match name.rfind('.') {
        Some(dot) if dot > 0 => (&name[..dot], &name[dot..]),
        _ => (name, ""),
    };
    for n in 0u32.. {
        let candidate = if n == 0 {
            dir.join(name)
        } else {
            dir.join(format!("{stem}.{n}{ext}"))
        };
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(_) => return Ok(candidate),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(format!("Failed to reserve output file: {e}")),
        }
    }
    Err("Failed to reserve output filename".to_string())
}

pub(crate) fn dedup_path(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    let candidate = dir.join(name);
    if !candidate.exists() {
        return candidate;
    }

    let (stem, ext) = match name.rfind('.') {
        Some(dot) if dot > 0 => (&name[..dot], &name[dot..]),
        _ => (name, ""),
    };

    for n in 1u32.. {
        let numbered = if ext.is_empty() {
            format!("{stem}.{n}")
        } else {
            format!("{stem}.{n}{ext}")
        };
        let path = dir.join(&numbered);
        if !path.exists() {
            return path;
        }
    }
    candidate
}

pub(crate) fn is_cross_device_error(e: &std::io::Error) -> bool {
    if e.kind() == std::io::ErrorKind::CrossesDevices {
        return true;
    }
    match e.raw_os_error() {
        #[cfg(unix)]
        Some(18) => true,
        #[cfg(windows)]
        Some(17) => true,
        _ => false,
    }
}

pub(crate) fn http_proxy_from_options(
    options: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<risuko_http::ProxyConnector>, String> {
    let server = options
        .get("all-proxy")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .trim();
    if server.is_empty() {
        return Ok(None);
    }
    let proxy = risuko_http::Proxy::all(server)
        .map_err(|error| format!("Invalid HTTP profile proxy: {error}"))?;
    let bypass = options
        .get("no-proxy")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    Ok(Some(
        risuko_http::ProxyConnector::from_proxy(proxy)
            .with_no_proxy(risuko_http::NoProxy::parse(bypass)),
    ))
}

pub(crate) fn redact_url(raw: &str) -> String {
    let trimmed = raw.trim_start();
    if trimmed
        .get(..5)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("ed2k:"))
    {
        return raw.to_string();
    }
    let Ok(url) = url::Url::parse(raw) else {
        return "<unparsable url>".to_string();
    };
    if url.scheme() == "magnet" {
        let mut hash = None;
        let mut trackers = 0usize;
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "xt" if hash.is_none() => hash = Some(value.into_owned()),
                "tr" => trackers += 1,
                _ => {}
            }
        }
        return format!(
            "magnet:?xt={} ({trackers} trackers)",
            hash.unwrap_or_default()
        );
    }
    if url.cannot_be_a_base() {
        return format!("{}:<redacted>", url.scheme());
    }
    let mut out = format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default());
    if let Some(port) = url.port() {
        out.push(':');
        out.push_str(&port.to_string());
    }
    out.push_str(url.path());
    if url.query().is_some_and(|query| !query.is_empty()) {
        out.push_str("?<redacted>");
    }
    out
}

pub(crate) fn redact_urls(raw: &[String]) -> Vec<String> {
    raw.iter().map(|uri| redact_url(uri)).collect()
}

#[cfg(test)]
mod tests {
    use super::{redact_url, redact_urls};

    #[test]
    fn redact_url_strips_user_info_query_and_fragment() {
        assert_eq!(
            redact_url("https://user:secret@example.com:8443/a/b.zip?token=abc&x=1#frag"),
            "https://example.com:8443/a/b.zip?<redacted>"
        );
        assert_eq!(
            redact_url("ftp://anonymous:pw@ftp.example.org/pub/x"),
            "ftp://ftp.example.org/pub/x"
        );
    }

    #[test]
    fn redact_url_keeps_plain_urls() {
        assert_eq!(
            redact_url("http://example.com/file.iso"),
            "http://example.com/file.iso"
        );
        assert_eq!(redact_url("http://[::1]:8080/a?"), "http://[::1]:8080/a");
    }

    #[test]
    fn redact_url_reduces_magnet_links_to_hash_and_tracker_count() {
        let hash = "cab507494d02ebb1178b38f2e9d7be299c86b862";
        let magnet = format!(
            "magnet:?xt=urn:btih:{hash}&dn=Some+Name&tr=https%3A%2F%2Ft.example%2Fannounce%3Fpasskey%3Dsecret&tr=udp%3A%2F%2Fu.example%3A6969"
        );
        let redacted = redact_url(&magnet);
        assert_eq!(redacted, format!("magnet:?xt=urn:btih:{hash} (2 trackers)"));
        assert!(!redacted.contains("passkey"));
        assert!(!redacted.contains("secret"));
        assert!(!redacted.contains("Some"));
        assert_eq!(
            redact_url(&format!("magnet:?xt=urn:btih:{hash}")),
            format!("magnet:?xt=urn:btih:{hash} (0 trackers)")
        );
    }

    #[test]
    fn redact_url_leaves_ed2k_links_untouched() {
        let link = "ed2k://|file|name.bin|123|0123456789ABCDEF0123456789ABCDEF|/";
        assert_eq!(redact_url(link), link);
    }

    #[test]
    fn redact_url_reports_unparsable_input() {
        assert_eq!(redact_url("not a url"), "<unparsable url>");
        assert_eq!(redact_url(""), "<unparsable url>");
    }

    #[test]
    fn redact_urls_maps_every_entry() {
        let raw = vec![
            "https://u:p@a.example/x?t=1".to_string(),
            "http://b.example/y".to_string(),
        ];
        assert_eq!(
            redact_urls(&raw),
            vec![
                "https://a.example/x?<redacted>".to_string(),
                "http://b.example/y".to_string()
            ]
        );
    }
}
