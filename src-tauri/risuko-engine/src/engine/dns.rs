use std::net::IpAddr;
use std::sync::Mutex;

use serde_json::{Map, Value};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DohSettings {
    pub enabled: bool,
    pub url: String,
    pub bootstrap: Vec<IpAddr>,
    pub fallback: bool,
}

impl DohSettings {
    pub fn from_options(opts: &Map<String, Value>) -> Self {
        let enabled =
            crate::config::parse_keep_seeding_option(opts.get("doh-enable")).unwrap_or(false);
        let url = opts
            .get("doh-url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let bootstrap = opts
            .get("doh-bootstrap")
            .and_then(|v| v.as_str())
            .map(parse_bootstrap)
            .unwrap_or_default();
        let fallback =
            crate::config::parse_keep_seeding_option(opts.get("doh-fallback")).unwrap_or(true);
        Self {
            enabled,
            url,
            bootstrap,
            fallback,
        }
    }

    fn is_active(&self) -> bool {
        self.enabled && !self.url.is_empty()
    }
}

fn parse_bootstrap(s: &str) -> Vec<IpAddr> {
    s.split([',', ' ', '\t', '\r', '\n'])
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .filter_map(|p| p.parse::<IpAddr>().ok())
        .collect()
}

static APPLIED: Mutex<Option<DohSettings>> = Mutex::new(None);

fn redacted_url_host(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_string()))
        .unwrap_or_else(|| "<invalid-url>".to_string())
}

pub fn apply_settings(settings: &DohSettings) {
    let mut guard = APPLIED.lock().unwrap_or_else(|e| e.into_inner());

    if guard.as_ref() == Some(settings) {
        return;
    }

    if settings.is_active() {
        let cfg = risuko_http::DohConfig {
            url: settings.url.clone(),
            bootstrap: settings.bootstrap.clone(),
            fallback: settings.fallback,
        };
        match risuko_http::DohResolver::new(cfg) {
            Ok(resolver) => {
                risuko_http::set_global_resolver(Some(std::sync::Arc::new(resolver)));
                tracing::info!(
                    "DoH enabled: endpoint={} bootstrap={} fallback={}",
                    redacted_url_host(&settings.url),
                    settings.bootstrap.len(),
                    settings.fallback,
                );
            }
            Err(e) => {
                tracing::warn!("DoH config invalid ({}); leaving DNS resolver unchanged", e);
                return;
            }
        }
    } else {
        risuko_http::set_global_resolver(None);
        if settings.enabled {
            tracing::warn!("DoH enabled but no endpoint URL set; using system DNS");
        } else {
            tracing::info!("DoH disabled: using system DNS");
        }
    }

    *guard = Some(settings.clone());
}

pub fn apply_from_options(opts: &Map<String, Value>) {
    apply_settings(&DohSettings::from_options(opts));
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_bootstrap_mixed_separators() {
        let ips = parse_bootstrap("1.1.1.1, 2606:4700:4700::1111\n8.8.8.8");
        assert_eq!(ips.len(), 3);
        assert!(ips.contains(&"1.1.1.1".parse().unwrap()));
        assert!(ips.contains(&"8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn parse_bootstrap_skips_garbage() {
        let ips = parse_bootstrap("1.1.1.1, not-an-ip, 9.9.9.9");
        assert_eq!(ips.len(), 2);
    }

    #[test]
    fn settings_defaults() {
        let s = DohSettings::from_options(&Map::new());
        assert!(!s.enabled);
        assert!(s.fallback);
        assert!(!s.is_active());
    }

    #[test]
    fn settings_from_options() {
        let opts = json!({
            "doh-enable": true,
            "doh-url": "  https://cloudflare-dns.com/dns-query  ",
            "doh-bootstrap": "1.1.1.1, 1.0.0.1",
            "doh-fallback": false
        });
        let s = DohSettings::from_options(opts.as_object().unwrap());
        assert!(s.enabled);
        assert_eq!(s.url, "https://cloudflare-dns.com/dns-query");
        assert_eq!(s.bootstrap.len(), 2);
        assert!(!s.fallback);
        assert!(s.is_active());
    }

    #[test]
    fn settings_enabled_without_url_is_inactive() {
        let opts = json!({ "doh-enable": true, "doh-url": "" });
        let s = DohSettings::from_options(opts.as_object().unwrap());
        assert!(s.enabled);
        assert!(!s.is_active());
    }
}
