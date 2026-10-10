use std::io::{BufRead, BufReader, Read};
use std::sync::{Arc, Mutex};

use http::HeaderValue;
use url::Url;

pub trait CookieStore: Send + Sync {
    fn set_cookies(&self, cookie_headers: &mut dyn Iterator<Item = &HeaderValue>, url: &Url);
    fn cookies(&self, url: &Url) -> Option<HeaderValue>;
}

pub struct Jar {
    inner: Mutex<cookie_store::CookieStore>,
}

impl Default for Jar {
    fn default() -> Self {
        Self {
            inner: Mutex::new(cookie_store::CookieStore::default()),
        }
    }
}

impl Jar {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_cookie_str(&self, cookie_str: &str, url: &Url) {
        if let Ok(parsed) = cookie::Cookie::parse(cookie_str) {
            if let Ok(mut store) = self.inner.lock() {
                let _ = store.insert_raw(&parsed, url);
            }
        }
    }

    pub fn load_netscape<R: Read>(&self, reader: R) -> std::io::Result<usize> {
        let mut count = 0usize;
        let buf = BufReader::new(reader);
        for line in buf.lines() {
            let line = line?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let payload = if let Some(rest) = trimmed.strip_prefix("#HttpOnly_") {
                rest
            } else if let Some(rest) = trimmed.strip_prefix("# HttpOnly_") {
                rest
            } else if trimmed.starts_with('#') {
                continue;
            } else {
                trimmed
            };
            let cols: Vec<&str> = payload.split('\t').collect();
            if cols.len() < 7 {
                if cols.len() > 1 {
                    tracing::warn!(
                        "cookies.txt: skipping malformed line ({} tab-separated fields, need 7)",
                        cols.len()
                    );
                }
                continue;
            }
            let domain_field = cols[0];
            let include_subdomains = cols[1].eq_ignore_ascii_case("TRUE");
            let path = cols[2];
            let secure = cols[3].eq_ignore_ascii_case("TRUE");
            let expires_raw = cols[4].trim();
            let name = cols[5];
            let value = cols[6];
            if name.is_empty() {
                continue;
            }
            let expires_epoch: Option<i64> = if expires_raw.is_empty() || expires_raw == "0" {
                None
            } else {
                match expires_raw.parse::<i64>() {
                    Ok(v) => {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0);
                        if v <= now {
                            continue;
                        }
                        Some(v)
                    }
                    Err(_) => None,
                }
            };
            let bare_domain = domain_field.trim_start_matches('.');
            let scheme = if secure { "https" } else { "http" };
            let url_str = format!("{scheme}://{bare_domain}{path}");
            let Ok(url) = Url::parse(&url_str) else {
                continue;
            };
            let mut builder = cookie::Cookie::build((name, value)).path(path);
            if include_subdomains {
                builder = builder.domain(bare_domain);
            }
            if secure {
                builder = builder.secure(true);
            }
            if let Some(epoch) = expires_epoch {
                builder =
                    builder.expires(cookie::time::OffsetDateTime::from_unix_timestamp(epoch).ok());
            }
            if let Ok(mut store) = self.inner.lock() {
                let _ = store.insert_raw(&builder.build(), &url);
            }
            count += 1;
        }
        Ok(count)
    }
}

impl CookieStore for Jar {
    fn set_cookies(&self, cookies: &mut dyn Iterator<Item = &HeaderValue>, url: &Url) {
        let Ok(mut store) = self.inner.lock() else {
            return;
        };
        for hv in cookies {
            let Ok(s) = hv.to_str() else { continue };
            let Ok(parsed) = cookie::Cookie::parse(s) else {
                continue;
            };
            let _ = store.insert_raw(&parsed, url);
        }
    }

    fn cookies(&self, url: &Url) -> Option<HeaderValue> {
        let store = self.inner.lock().ok()?;
        let s = store
            .matches(url)
            .into_iter()
            .map(|c| format!("{}={}", c.name(), c.value()))
            .collect::<Vec<_>>()
            .join("; ");
        if s.is_empty() {
            return None;
        }
        HeaderValue::from_str(&s).ok()
    }
}

pub(crate) type SharedJar = Arc<dyn CookieStore>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_simple_cookie() {
        let jar = Jar::new();
        let url = Url::parse("https://example.com/").unwrap();
        jar.add_cookie_str("session=abc; Path=/; Domain=example.com", &url);
        let header = CookieStore::cookies(&jar, &url).expect("cookie should match");
        assert_eq!(header.to_str().unwrap(), "session=abc");
    }

    #[test]
    fn netscape_format_loads() {
        let txt = "# Netscape HTTP Cookie File\n\
                   .example.com\tTRUE\t/\tFALSE\t0\tfoo\tbar\n\
                   #HttpOnly_.example.com\tTRUE\t/\tFALSE\t0\thid\tval\n";
        let jar = Jar::new();
        let n = jar.load_netscape(txt.as_bytes()).unwrap();
        assert_eq!(n, 2);
        let url = Url::parse("http://example.com/").unwrap();
        let header = CookieStore::cookies(&jar, &url).expect("cookie should match");
        let s = header.to_str().unwrap();
        assert!(s.contains("foo=bar"));
        assert!(s.contains("hid=val"));
    }

    #[test]
    fn secure_cookie_not_sent_over_http() {
        let jar = Jar::new();
        let https = Url::parse("https://example.com/").unwrap();
        jar.add_cookie_str("k=v; Path=/; Domain=example.com; Secure", &https);
        let http = Url::parse("http://example.com/").unwrap();
        assert!(CookieStore::cookies(&jar, &http).is_none());
        assert!(CookieStore::cookies(&jar, &https).is_some());
    }
}
