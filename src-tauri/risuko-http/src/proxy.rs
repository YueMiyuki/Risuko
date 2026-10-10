use crate::error::{Error, Result};
use base64::engine::general_purpose::STANDARD as B64_STANDARD;
use base64::Engine as _;
use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use url::Url;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NoProxy {
    entries: Vec<NoProxyEntry>,
    normalized: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum NoProxyEntry {
    Any,
    Host {
        host: String,
        port: Option<u16>,
    },
    Ip {
        addr: IpAddr,
        port: Option<u16>,
    },
    Network {
        addr: IpAddr,
        prefix: u8,
        port: Option<u16>,
    },
}

impl NoProxy {
    pub fn normalize(value: impl AsRef<str>) -> String {
        Self::parse(value).normalized().to_string()
    }

    pub fn parse(value: impl AsRef<str>) -> Self {
        let mut entries = Vec::new();
        let mut seen = HashSet::new();
        for raw in value.as_ref().split([',', '\r', '\n']) {
            let Some(entry) = parse_entry(raw.trim()) else {
                continue;
            };
            if seen.insert(entry.clone()) {
                entries.push(entry);
            }
        }

        let normalized = entries
            .iter()
            .map(format_entry)
            .collect::<Vec<_>>()
            .join(",");
        Self {
            entries,
            normalized,
        }
    }

    pub fn normalized(&self) -> &str {
        &self.normalized
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn matches_url(&self, url: &Url) -> bool {
        let Some(host) = url.host_str() else {
            return false;
        };
        self.matches_host_port(host, url.port_or_known_default())
    }

    pub fn matches_host_port(&self, host: &str, port: Option<u16>) -> bool {
        let host = normalize_host(host);
        if host.is_empty() {
            return false;
        }

        let ip = host.parse::<IpAddr>().ok();
        self.entries.iter().any(|entry| match entry {
            NoProxyEntry::Any => true,
            NoProxyEntry::Host {
                host: rule,
                port: rule_port,
            } => {
                rule_port_matches(*rule_port, port) && (host == *rule || is_subdomain(&host, rule))
            }
            NoProxyEntry::Ip {
                addr,
                port: rule_port,
            } => rule_port_matches(*rule_port, port) && ip == Some(*addr),
            NoProxyEntry::Network {
                addr,
                prefix,
                port: rule_port,
            } => {
                rule_port_matches(*rule_port, port)
                    && ip.is_some_and(|candidate| ip_in_network(candidate, *addr, *prefix))
            }
        })
    }

    pub(crate) fn matches_ip(&self, ip: IpAddr, port: Option<u16>) -> bool {
        if self.entries.is_empty() {
            return false;
        }
        if self
            .entries
            .iter()
            .any(|entry| matches!(entry, NoProxyEntry::Host { .. }))
        {
            return self.matches_host_port(&ip.to_string(), port);
        }
        self.entries.iter().any(|entry| match entry {
            NoProxyEntry::Any => true,
            NoProxyEntry::Host { .. } => false,
            NoProxyEntry::Ip {
                addr,
                port: rule_port,
            } => rule_port_matches(*rule_port, port) && ip == *addr,
            NoProxyEntry::Network {
                addr,
                prefix,
                port: rule_port,
            } => rule_port_matches(*rule_port, port) && ip_in_network(ip, *addr, *prefix),
        })
    }
}

impl std::fmt::Display for NoProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.normalized)
    }
}

fn is_subdomain(host: &str, rule: &str) -> bool {
    host.len() > rule.len()
        && host.ends_with(rule)
        && host.as_bytes()[host.len() - rule.len() - 1] == b'.'
}

fn rule_port_matches(rule: Option<u16>, actual: Option<u16>) -> bool {
    rule.is_none() || rule == actual
}

fn normalize_host(host: &str) -> String {
    host.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .trim_start_matches('.')
        .trim_end_matches('.')
        .to_ascii_lowercase()
}

fn parse_entry(raw: &str) -> Option<NoProxyEntry> {
    if raw.is_empty() {
        return None;
    }
    if raw == "*" {
        return Some(NoProxyEntry::Any);
    }

    let (host_part, port) = split_host_port(raw)?;
    if host_part.is_empty() || host_part.contains(['\\', '?', '#', '@']) {
        return None;
    }
    if raw.starts_with('[')
        && host_part
            .split_once('/')
            .map(|(addr, _)| addr)
            .unwrap_or(&host_part)
            .parse::<IpAddr>()
            .is_err()
    {
        return None;
    }

    if let Some((addr_text, prefix_text)) = host_part.split_once('/') {
        if addr_text.is_empty() || prefix_text.is_empty() || prefix_text.contains('/') {
            return None;
        }
        let addr = addr_text.parse::<IpAddr>().ok()?;
        let max_prefix = match addr {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        let prefix = prefix_text.parse::<u8>().ok()?;
        if prefix > max_prefix {
            return None;
        }
        let network = mask_ip(addr, prefix);
        return Some(NoProxyEntry::Network {
            addr: network,
            prefix,
            port,
        });
    }

    let normalized = normalize_host(&host_part);
    if normalized.is_empty() || normalized == "*" {
        return None;
    }

    if let Ok(addr) = host_part.parse::<IpAddr>() {
        return Some(NoProxyEntry::Ip { addr, port });
    }

    if host_part.contains('.')
        && host_part
            .split('.')
            .all(|label| !label.is_empty() && label.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return None;
    }

    if normalized.contains(':') {
        return None;
    }

    if normalized.len() > 253
        || normalized.split('.').any(|label| {
            label.is_empty()
                || label.len() > 63
                || !label
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
                || label.starts_with('-')
                || label.ends_with('-')
        })
    {
        return None;
    }

    Some(NoProxyEntry::Host {
        host: normalized,
        port,
    })
}

fn split_host_port(raw: &str) -> Option<(String, Option<u16>)> {
    if raw.starts_with('[') {
        let close = raw.find(']')?;
        let host = &raw[1..close];
        let rest = &raw[close + 1..];
        if rest.is_empty() {
            return Some((host.to_string(), None));
        }
        if let Some(port_text) = rest.strip_prefix(':') {
            return Some((host.to_string(), Some(parse_port(port_text)?)));
        }
        let cidr = rest.strip_prefix('/')?;
        let (prefix, port) = if let Some((prefix, port_text)) = cidr.rsplit_once(':') {
            (prefix, Some(parse_port(port_text)?))
        } else {
            (cidr, None)
        };
        if prefix.is_empty() {
            return None;
        }
        return Some((format!("{host}/{prefix}"), port));
    }

    if let Some(slash) = raw.find('/') {
        let (network, suffix) = raw.split_at(slash);
        let suffix = &suffix[1..];
        if let Some((prefix, port_text)) = suffix.rsplit_once(':') {
            if !port_text.is_empty() {
                return Some((format!("{network}/{prefix}"), Some(parse_port(port_text)?)));
            }
        }
        return Some((raw.to_string(), None));
    }

    if raw.matches(':').count() == 1 {
        let (host, port_text) = raw.rsplit_once(':')?;
        return Some((host.to_string(), Some(parse_port(port_text)?)));
    }

    Some((raw.to_string(), None))
}

fn parse_port(text: &str) -> Option<u16> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let port = text.parse::<u16>().ok()?;
    (port > 0).then_some(port)
}

fn mask_ip(addr: IpAddr, prefix: u8) -> IpAddr {
    match addr {
        IpAddr::V4(v4) => {
            let bits = u32::from(v4);
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            IpAddr::V4(Ipv4Addr::from(bits & mask))
        }
        IpAddr::V6(v6) => {
            let bits = u128::from(v6);
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            IpAddr::V6(Ipv6Addr::from(bits & mask))
        }
    }
}

fn ip_in_network(candidate: IpAddr, network: IpAddr, prefix: u8) -> bool {
    match (candidate, network) {
        (IpAddr::V4(candidate), IpAddr::V4(network)) => {
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            (u32::from(candidate) & mask) == (u32::from(network) & mask)
        }
        (IpAddr::V6(candidate), IpAddr::V6(network)) => {
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            (u128::from(candidate) & mask) == (u128::from(network) & mask)
        }
        _ => false,
    }
}

fn format_entry(entry: &NoProxyEntry) -> String {
    match entry {
        NoProxyEntry::Any => "*".to_string(),
        NoProxyEntry::Host { host, port } => format_host_port(host, *port),
        NoProxyEntry::Ip { addr, port } => format_ip_port(*addr, *port),
        NoProxyEntry::Network { addr, prefix, port } => {
            let base = match addr {
                IpAddr::V4(v4) => v4.to_string(),
                IpAddr::V6(v6) if port.is_some() => format!("[{v6}]"),
                IpAddr::V6(v6) => v6.to_string(),
            };
            if let Some(port) = port {
                format!("{base}/{prefix}:{port}")
            } else {
                format!("{base}/{prefix}")
            }
        }
    }
}

fn format_host_port(host: &str, port: Option<u16>) -> String {
    match port {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }
}

fn format_ip_port(addr: IpAddr, port: Option<u16>) -> String {
    let host = match (addr, port) {
        (IpAddr::V4(v4), _) => v4.to_string(),
        (IpAddr::V6(v6), Some(_)) => format!("[{v6}]"),
        (IpAddr::V6(v6), None) => v6.to_string(),
    };
    format_host_port(&host, port)
}

#[derive(Clone)]
pub struct Proxy {
    pub(crate) url: Url,
    pub(crate) scheme: ProxyScheme,
    pub(crate) no_proxy: Option<Arc<NoProxy>>,
}

impl std::fmt::Debug for Proxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Proxy")
            .field("url", &self.redacted_url())
            .field("scheme", &self.scheme)
            .field("no_proxy", &self.no_proxy)
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProxyScheme {
    Http,
    Socks5 { resolve_locally: bool },
}

impl Proxy {
    pub(crate) fn redacted_url(&self) -> String {
        let mut url = self.url.clone();
        let _ = url.set_username("");
        let _ = url.set_password(None);
        url.to_string()
    }

    pub fn all<U: AsRef<str>>(url: U) -> Result<Self> {
        let url = Url::parse(url.as_ref()).map_err(|e| Error::Url(e.to_string()))?;
        let scheme = match url.scheme() {
            "http" => ProxyScheme::Http,
            "https" => {
                return Err(Error::Url(
                    "https:// proxies are not yet supported; use http:// or socks5://".into(),
                ))
            }
            "socks5" => ProxyScheme::Socks5 {
                resolve_locally: true,
            },
            "socks5h" => ProxyScheme::Socks5 {
                resolve_locally: false,
            },
            other => return Err(Error::Url(format!("unsupported proxy scheme: {other}"))),
        };
        Ok(Self {
            url,
            scheme,
            no_proxy: None,
        })
    }
    pub fn with_no_proxy(mut self, no_proxy: NoProxy) -> Self {
        self.no_proxy = Some(Arc::new(no_proxy));
        self
    }
    pub fn with_bypass<U: AsRef<str>>(self, bypass: U) -> Self {
        self.with_no_proxy(NoProxy::parse(bypass))
    }

    pub fn all_with_bypass<U: AsRef<str>, B: AsRef<str>>(url: U, bypass: B) -> Result<Self> {
        Ok(Self::all(url)?.with_bypass(bypass))
    }

    pub(crate) fn url(&self) -> &Url {
        &self.url
    }

    pub(crate) fn scheme(&self) -> &ProxyScheme {
        &self.scheme
    }

    pub(crate) fn http_basic_authorization(&self) -> Option<String> {
        if !matches!(self.scheme, ProxyScheme::Http) || self.url.username().is_empty() {
            return None;
        }
        let user = percent_decode_str(self.url.username());
        let pass = percent_decode_str(self.url.password().unwrap_or_default());
        let credentials = format!("{user}:{pass}");
        Some(format!(
            "Basic {}",
            B64_STANDARD.encode(credentials.as_bytes())
        ))
    }
}

pub(crate) fn percent_decode_str(value: &str) -> String {
    percent_encoding::percent_decode_str(value)
        .decode_utf8_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_ip_agrees_with_matches_host_port() {
        let bypass = NoProxy::parse("10.0.0.0/8,[2001:db8::]/32,192.168.1.5:80,host.example");
        for (ip, port) in [
            ("10.1.2.3", 1),
            ("11.1.2.3", 1),
            ("2001:db8::1", 9),
            ("2001:db9::1", 9),
            ("192.168.1.5", 80),
            ("192.168.1.5", 81),
        ] {
            let ip: IpAddr = ip.parse().unwrap();
            assert_eq!(
                bypass.matches_ip(ip, Some(port)),
                bypass.matches_host_port(&ip.to_string(), Some(port)),
                "{ip}:{port}"
            );
        }
        assert!(!NoProxy::default().matches_ip("10.0.0.1".parse().unwrap(), Some(1)));
    }

    #[test]
    fn subdomain_match_requires_label_boundary() {
        let bypass = NoProxy::parse("example.com");
        assert!(bypass.matches_host_port("a.example.com", None));
        assert!(!bypass.matches_host_port("badexample.com", None));
    }

    #[test]
    fn normalizes_hosts_ports_and_duplicates() {
        let bypass =
            NoProxy::parse(" .Example.COM,example.com, EXAMPLE.com:443\n[2001:DB8::1]:8443 ");
        assert_eq!(
            bypass.normalized(),
            "example.com,example.com:443,[2001:db8::1]:8443"
        );
    }

    #[test]
    fn drops_invalid_entries() {
        let bypass = NoProxy::parse(
            "bad host,*.example.com,[example.com],example.com:0,example.com:nope,10.0.0.0/33,2001:db8::/129,001.002.003.004",
        );
        assert!(bypass.is_empty());
    }

    #[test]
    fn host_matches_exact_and_subdomains_but_not_suffix_spoof() {
        let bypass = NoProxy::parse("example.com");
        assert!(bypass.matches_host_port("example.com", Some(443)));
        assert!(bypass.matches_host_port("a.example.com", Some(443)));
        assert!(!bypass.matches_host_port("notexample.com", Some(443)));
    }

    #[test]
    fn port_restriction_is_honored() {
        let bypass = NoProxy::parse("example.com:8080");
        assert!(bypass.matches_host_port("example.com", Some(8080)));
        assert!(!bypass.matches_host_port("example.com", Some(8081)));
        assert!(!bypass.matches_host_port("example.com", None));
    }

    #[test]
    fn signed_port_is_rejected() {
        let bypass = NoProxy::parse("example.com:+80,example.org:8080");
        assert_eq!(bypass.normalized(), "example.org:8080");
        assert!(!bypass.matches_host_port("example.com", Some(80)));
        assert!(bypass.matches_host_port("example.org", Some(8080)));
    }

    #[test]
    fn ipv4_and_ipv6_and_cidr_match() {
        let bypass = NoProxy::parse("127.0.0.1,2001:db8::1,10.10.0.0/16,2001:db8:abcd::/48");
        assert!(bypass.matches_host_port("127.0.0.1", Some(1)));
        assert!(bypass.matches_host_port("[2001:DB8::1]", Some(1)));
        assert!(bypass.matches_host_port("10.10.42.7", Some(1)));
        assert!(bypass.matches_host_port("2001:db8:abcd::42", Some(1)));
        assert!(!bypass.matches_host_port("10.11.0.1", Some(1)));
    }

    #[test]
    fn bracketed_ipv6_cidr_port_is_supported() {
        let bypass = NoProxy::parse("[2001:db8::]/32:8443");
        assert_eq!(bypass.normalized(), "[2001:db8::]/32:8443");
        assert!(bypass.matches_host_port("2001:db8::42", Some(8443)));
        assert!(!bypass.matches_host_port("2001:db8::42", Some(443)));
    }

    #[test]
    fn localhost_and_loopback_require_explicit_bypass() {
        let bypass = NoProxy::default();
        assert!(!bypass.matches_host_port("localhost", Some(1)));
        assert!(!bypass.matches_host_port("api.localhost", Some(1)));
        assert!(!bypass.matches_host_port("127.42.1.2", Some(1)));
        assert!(!bypass.matches_host_port("::1", Some(1)));

        let bypass = NoProxy::parse("localhost,127.0.0.0/8,::1");
        assert!(bypass.matches_host_port("localhost", Some(1)));
        assert!(bypass.matches_host_port("127.42.1.2", Some(1)));
        assert!(bypass.matches_host_port("::1", Some(1)));
    }

    #[test]
    fn wildcard_matches_everything() {
        let bypass = NoProxy::parse("*");
        assert!(bypass.matches_host_port("example.net", Some(443)));
    }

    #[test]
    fn http_proxy_basic_auth_decodes_url_credentials() {
        let proxy = Proxy::all("http://user%40name:p%3Ass@proxy.example:8080").unwrap();
        assert_eq!(
            proxy.http_basic_authorization().as_deref(),
            Some("Basic dXNlckBuYW1lOnA6c3M=")
        );
        let socks = Proxy::all("socks5://proxy.example:1080").unwrap();
        assert!(socks.http_basic_authorization().is_none());
    }

    #[test]
    fn proxy_debug_redacts_credentials() {
        let proxy = Proxy::all("http://user:secret@proxy.example:8080").unwrap();
        let debug = format!("{proxy:?}");
        assert!(!debug.contains("secret"));
        assert!(debug.contains("proxy.example"));
    }

    #[test]
    fn url_uses_known_default_port() {
        let bypass = NoProxy::parse("example.com:80");
        let url = Url::parse("http://example.com/path").unwrap();
        assert!(bypass.matches_url(&url));
    }
}
