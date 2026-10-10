use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::client::{Client, ClientBuilder};
use crate::error::{Error, Result};
use crate::resolver::{Addrs, GaiResolver, Resolve, Resolving};

const MIN_TTL: Duration = Duration::from_secs(30);
const MAX_TTL: Duration = Duration::from_secs(3600);
const NEGATIVE_TTL: Duration = Duration::from_secs(5);
const QUERY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct DohConfig {
    pub url: String,
    pub bootstrap: Vec<IpAddr>,
    pub fallback: bool,
}

struct CacheEntry {
    addrs: Vec<IpAddr>,
    nxdomain: bool,
    expires: Instant,
}

#[derive(Clone)]
pub struct DohResolver {
    inner: Arc<Inner>,
}

struct Inner {
    url: String,
    fallback: bool,
    client: Client,
    gai: GaiResolver,
    cache: RwLock<HashMap<String, CacheEntry>>,
    next_id: AtomicU16,
}

impl DohResolver {
    pub fn new(cfg: DohConfig) -> Result<Self> {
        let url = url::Url::parse(&cfg.url).map_err(|e| Error::Url(e.to_string()))?;
        if url.scheme() != "https" {
            return Err(Error::Url(format!(
                "DoH endpoint must be https://, got {}",
                url.scheme()
            )));
        }
        let endpoint_host = url
            .host_str()
            .ok_or_else(|| Error::Url("DoH endpoint missing host".into()))?
            .to_string();

        let bootstrap = Arc::new(BootstrapResolver {
            host: endpoint_host,
            ips: cfg.bootstrap.clone(),
            gai: GaiResolver,
        });
        let client = ClientBuilder::new()
            .resolver_arc(bootstrap)
            .connect_timeout(QUERY_TIMEOUT)
            .timeout(QUERY_TIMEOUT)
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(2)
            .build()?;

        Ok(Self {
            inner: Arc::new(Inner {
                url: cfg.url,
                fallback: cfg.fallback,
                client,
                gai: GaiResolver,
                cache: RwLock::new(HashMap::new()),
                next_id: AtomicU16::new(1),
            }),
        })
    }

    fn cache_get(&self, host: &str) -> Option<(Vec<IpAddr>, bool)> {
        let cache = self.inner.cache.read().ok()?;
        let entry = cache.get(host)?;
        if entry.expires > Instant::now() {
            return Some((entry.addrs.clone(), entry.nxdomain));
        }
        drop(cache);
        if let Ok(mut cache) = self.inner.cache.write() {
            cache.remove(host);
        }
        None
    }

    fn cache_put(&self, host: &str, addrs: Vec<IpAddr>, nxdomain: bool, ttl: Duration) {
        if let Ok(mut cache) = self.inner.cache.write() {
            let now = Instant::now();
            cache.retain(|_, entry| entry.expires > now);

            cache.insert(
                host.to_string(),
                CacheEntry {
                    addrs,
                    nxdomain,
                    expires: Instant::now() + ttl,
                },
            );
        }
    }

    async fn query_one(&self, host: &str, qtype: u16) -> Result<(Vec<IpAddr>, Duration)> {
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let msg = encode_query(id, host, qtype)?;
        let resp = self
            .inner
            .client
            .post(self.inner.url.as_str())
            .header("content-type", "application/dns-message")
            .header("accept", "application/dns-message")
            .body(msg)
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            return Err(Error::Status(status));
        }
        let body = resp.bytes_limited(64 * 1024).await?;
        parse_response(&body, qtype)
    }

    async fn resolve_doh(&self, host: &str) -> Result<(Vec<IpAddr>, Duration)> {
        let (v4, v6) = tokio::join!(
            self.query_one(host, TYPE_A),
            self.query_one(host, TYPE_AAAA)
        );

        let mut addrs = Vec::new();
        let mut ttl = MAX_TTL;
        let mut last_err: Option<Error> = None;

        for res in [v6, v4] {
            match res {
                Ok((ips, t)) => {
                    if !ips.is_empty() {
                        ttl = ttl.min(t);
                    }
                    addrs.extend(ips);
                }
                Err(Error::NoRecords) => {}
                Err(e) => last_err = Some(e),
            }
        }

        if addrs.is_empty() {
            return Err(last_err.unwrap_or(Error::NoRecords));
        }

        Ok((addrs, ttl.clamp(MIN_TTL, MAX_TTL)))
    }
}

impl Resolve for DohResolver {
    fn resolve(&self, host: &str) -> Resolving {
        let this = self.clone();
        let host = host.to_string();
        Box::pin(async move {
            if let Ok(ip) = host.parse::<IpAddr>() {
                return Ok(ips_to_addrs(vec![ip]));
            }

            if let Some((cached, nxdomain)) = this.cache_get(&host) {
                if cached.is_empty() {
                    if this.inner.fallback && !nxdomain {
                        return this.inner.gai.resolve(&host).await;
                    }
                    return Err(Error::NoRecords);
                }
                return Ok(ips_to_addrs(cached));
            }

            match this.resolve_doh(&host).await {
                Ok((addrs, ttl)) => {
                    this.cache_put(&host, addrs.clone(), false, ttl);
                    Ok(ips_to_addrs(addrs))
                }
                Err(e) => {
                    let nxdomain = matches!(e, Error::NoRecords);
                    this.cache_put(&host, Vec::new(), nxdomain, NEGATIVE_TTL);
                    if this.inner.fallback && !nxdomain {
                        tracing::warn!("DoH resolve failed for {host}: {e}; using system DNS");
                        this.inner.gai.resolve(&host).await
                    } else {
                        Err(e)
                    }
                }
            }
        })
    }
}

fn ips_to_addrs(ips: Vec<IpAddr>) -> Addrs {
    let v: Vec<SocketAddr> = ips.into_iter().map(|ip| SocketAddr::new(ip, 0)).collect();
    Box::new(v.into_iter()) as Addrs
}

struct BootstrapResolver {
    host: String,
    ips: Vec<IpAddr>,
    gai: GaiResolver,
}

impl Resolve for BootstrapResolver {
    fn resolve(&self, host: &str) -> Resolving {
        if host.eq_ignore_ascii_case(&self.host) && !self.ips.is_empty() {
            let addrs = ips_to_addrs(self.ips.clone());
            return Box::pin(async move { Ok(addrs) });
        }
        self.gai.resolve(host)
    }
}

const TYPE_A: u16 = 1;
const TYPE_AAAA: u16 = 28;
const CLASS_IN: u16 = 1;

fn encode_query(id: u16, host: &str, qtype: u16) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(host.len() + 32);
    buf.extend_from_slice(&id.to_be_bytes());
    buf.extend_from_slice(&0x0100u16.to_be_bytes());
    buf.extend_from_slice(&1u16.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes());

    for label in host.trim_end_matches('.').split('.') {
        if label.is_empty() {
            return Err(Error::Url(format!("invalid hostname for DoH: {host}")));
        }
        if label.len() > 63 {
            return Err(Error::Url(format!("DNS label too long in {host}")));
        }
        buf.push(label.len() as u8);
        buf.extend_from_slice(label.as_bytes());
    }
    buf.push(0);

    buf.extend_from_slice(&qtype.to_be_bytes());
    buf.extend_from_slice(&CLASS_IN.to_be_bytes());
    Ok(buf)
}

fn parse_response(buf: &[u8], want_type: u16) -> Result<(Vec<IpAddr>, Duration)> {
    if buf.len() < 12 {
        return Err(Error::Decode("DNS response too short".into()));
    }
    let flags = u16::from_be_bytes([buf[2], buf[3]]);
    let rcode = flags & 0x000F;
    if rcode == 3 {
        return Err(Error::NoRecords);
    }
    if rcode != 0 {
        return Err(Error::Decode(format!("DNS rcode {rcode}")));
    }
    let qdcount = u16::from_be_bytes([buf[4], buf[5]]);
    let ancount = u16::from_be_bytes([buf[6], buf[7]]);

    let mut pos = 12usize;

    for _ in 0..qdcount {
        pos = skip_name(buf, pos)?;
        pos = pos
            .checked_add(4)
            .filter(|&p| p <= buf.len())
            .ok_or_else(|| Error::Decode("truncated question".into()))?;
    }

    let mut addrs = Vec::new();
    let mut min_ttl = MAX_TTL;

    for _ in 0..ancount {
        pos = skip_name(buf, pos)?;
        if pos + 10 > buf.len() {
            return Err(Error::Decode("truncated answer header".into()));
        }
        let rtype = u16::from_be_bytes([buf[pos], buf[pos + 1]]);
        let ttl = u32::from_be_bytes([buf[pos + 4], buf[pos + 5], buf[pos + 6], buf[pos + 7]]);
        let rdlength = u16::from_be_bytes([buf[pos + 8], buf[pos + 9]]) as usize;
        pos += 10;
        if pos + rdlength > buf.len() {
            return Err(Error::Decode("truncated rdata".into()));
        }
        let rdata = &buf[pos..pos + rdlength];

        if rtype == want_type {
            match (want_type, rdlength) {
                (TYPE_A, 4) => {
                    let octets: [u8; 4] = rdata.try_into().unwrap();
                    addrs.push(IpAddr::V4(Ipv4Addr::from(octets)));
                    min_ttl = min_ttl.min(Duration::from_secs(ttl as u64));
                }
                (TYPE_AAAA, 16) => {
                    let octets: [u8; 16] = rdata.try_into().unwrap();
                    addrs.push(IpAddr::V6(Ipv6Addr::from(octets)));
                    min_ttl = min_ttl.min(Duration::from_secs(ttl as u64));
                }
                _ => {}
            }
        }
        pos += rdlength;
    }

    Ok((addrs, min_ttl))
}

fn skip_name(buf: &[u8], mut pos: usize) -> Result<usize> {
    loop {
        let len = *buf
            .get(pos)
            .ok_or_else(|| Error::Decode("name past end".into()))?;
        match len & 0xC0 {
            0x00 => {
                if len == 0 {
                    return Ok(pos + 1);
                }
                pos = pos
                    .checked_add(1 + len as usize)
                    .filter(|&p| p <= buf.len())
                    .ok_or_else(|| Error::Decode("label past end".into()))?;
            }
            0xC0 => {
                if pos + 2 > buf.len() {
                    return Err(Error::Decode("truncated pointer".into()));
                }
                return Ok(pos + 2);
            }
            _ => return Err(Error::Decode("invalid label length".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_query_basic() {
        let q = encode_query(0x1234, "example.com", TYPE_A).unwrap();
        assert_eq!(&q[0..2], &[0x12, 0x34]);
        assert_eq!(&q[2..4], &[0x01, 0x00]);
        assert_eq!(&q[4..6], &[0x00, 0x01]);
        assert_eq!(q[12], 7);
        assert_eq!(&q[13..20], b"example");
        assert_eq!(q[20], 3);
        assert_eq!(&q[21..24], b"com");
        assert_eq!(q[24], 0);
        assert_eq!(&q[25..27], &[0x00, 0x01]);
        assert_eq!(&q[27..29], &[0x00, 0x01]);
    }

    #[test]
    fn encode_query_rejects_empty_label() {
        assert!(encode_query(1, "a..b", TYPE_A).is_err());
    }

    #[test]
    fn encode_query_trailing_dot_ok() {
        let a = encode_query(1, "example.com", TYPE_A).unwrap();
        let b = encode_query(1, "example.com.", TYPE_A).unwrap();
        assert_eq!(a, b);
    }

    fn build_a_response(host: &str, ips: &[(Ipv4Addr, u32)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0x1234u16.to_be_bytes());
        buf.extend_from_slice(&0x8180u16.to_be_bytes());
        buf.extend_from_slice(&1u16.to_be_bytes());
        buf.extend_from_slice(&(ips.len() as u16).to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        for label in host.split('.') {
            buf.push(label.len() as u8);
            buf.extend_from_slice(label.as_bytes());
        }
        buf.push(0);
        buf.extend_from_slice(&TYPE_A.to_be_bytes());
        buf.extend_from_slice(&CLASS_IN.to_be_bytes());
        for (ip, ttl) in ips {
            buf.extend_from_slice(&0xC00Cu16.to_be_bytes());
            buf.extend_from_slice(&TYPE_A.to_be_bytes());
            buf.extend_from_slice(&CLASS_IN.to_be_bytes());
            buf.extend_from_slice(&ttl.to_be_bytes());
            buf.extend_from_slice(&4u16.to_be_bytes());
            buf.extend_from_slice(&ip.octets());
        }
        buf
    }

    #[test]
    fn parse_response_single_a() {
        let resp = build_a_response("example.com", &[(Ipv4Addr::new(93, 184, 216, 34), 300)]);
        let (addrs, ttl) = parse_response(&resp, TYPE_A).unwrap();
        assert_eq!(addrs, vec![IpAddr::V4(Ipv4Addr::new(93, 184, 216, 34))]);
        assert_eq!(ttl, Duration::from_secs(300));
    }

    #[test]
    fn parse_response_multiple_a_min_ttl() {
        let resp = build_a_response(
            "example.com",
            &[
                (Ipv4Addr::new(1, 1, 1, 1), 600),
                (Ipv4Addr::new(2, 2, 2, 2), 120),
            ],
        );
        let (addrs, ttl) = parse_response(&resp, TYPE_A).unwrap();
        assert_eq!(addrs.len(), 2);
        assert_eq!(ttl, Duration::from_secs(120));
    }

    #[test]
    fn parse_response_filters_other_family() {
        let resp = build_a_response("example.com", &[(Ipv4Addr::new(1, 1, 1, 1), 300)]);
        let (addrs, _) = parse_response(&resp, TYPE_AAAA).unwrap();
        assert!(addrs.is_empty());
    }

    #[test]
    fn parse_response_rejects_servfail() {
        let mut resp = build_a_response("example.com", &[]);
        resp[3] |= 0x02;
        assert!(parse_response(&resp, TYPE_A).is_err());
    }

    #[test]
    fn parse_response_maps_nxdomain_to_no_records() {
        let mut resp = build_a_response("example.com", &[]);
        resp[3] |= 0x03;
        assert!(matches!(
            parse_response(&resp, TYPE_A),
            Err(Error::NoRecords)
        ));
    }

    #[test]
    fn parse_response_too_short() {
        assert!(parse_response(&[0u8; 4], TYPE_A).is_err());
    }

    #[test]
    fn ips_to_addrs_uses_port_zero() {
        let addrs: Vec<SocketAddr> = ips_to_addrs(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]).collect();
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0].port(), 0);
    }

    #[test]
    fn new_rejects_non_https() {
        let cfg = DohConfig {
            url: "http://insecure.example/dns-query".into(),
            bootstrap: vec![],
            fallback: true,
        };
        assert!(DohResolver::new(cfg).is_err());
    }
}
