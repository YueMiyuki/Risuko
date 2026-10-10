use std::collections::HashSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use futures_util::{future, stream, Stream, StreamExt};

use super::hash::{Id20, Id32};

const PEER_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONCURRENT_PEER_LOOKUPS: usize = 8;

#[derive(Debug, thiserror::Error)]
pub enum MagnetError {
    #[error("magnet: expected scheme magnet:, got {0:?}")]
    BadScheme(String),
    #[error("magnet: missing info-hash (xt=urn:btih:.. or xt=urn:btmh:..)")]
    MissingInfoHash,
    #[error("magnet: bad info-hash: {0}")]
    BadInfoHash(String),
    #[error("magnet: parse error: {0}")]
    Parse(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MagnetPeer {
    Addr(SocketAddr),
    Host(String, u16),
}

impl MagnetPeer {
    fn parse(raw: &str) -> Option<Self> {
        let raw = raw.trim();
        if let Ok(addr) = raw.parse::<SocketAddr>() {
            return (addr.port() != 0).then_some(Self::Addr(addr));
        }
        let (host, port) = raw.rsplit_once(':')?;
        let port = port.parse::<u16>().ok().filter(|port| *port != 0)?;
        if host.is_empty() || host.contains([':', '[', ']', '/', ' ']) {
            return None;
        }
        Some(Self::Host(host.to_ascii_lowercase(), port))
    }

    pub async fn resolve(&self) -> Vec<SocketAddr> {
        match self {
            Self::Addr(addr) => vec![*addr],
            Self::Host(host, port) => tokio::time::timeout(
                PEER_LOOKUP_TIMEOUT,
                tokio::net::lookup_host((host.as_str(), *port)),
            )
            .await
            .ok()
            .and_then(Result::ok)
            .map(|addrs| addrs.collect())
            .unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Magnet {
    info_hash: Id20,
    info_hash_v1: Option<Id20>,
    info_hash_v2: Option<Id32>,
    pub trackers: Vec<String>,
    pub display_name: Option<String>,
    pub select_only: Option<Vec<usize>>,
    pub peers: Vec<MagnetPeer>,
}

impl Magnet {
    pub fn info_hash(&self) -> Id20 {
        self.info_hash
    }

    pub fn info_hash_v1(&self) -> Option<Id20> {
        self.info_hash_v1
    }

    pub fn info_hash_v2(&self) -> Option<Id32> {
        self.info_hash_v2
    }

    pub fn parse(input: &str) -> Result<Self, MagnetError> {
        let input = input.trim();
        if input.len() == 40 {
            if let Ok(id) = Id20::from_str(input) {
                return Ok(Self {
                    info_hash: id,
                    info_hash_v1: Some(id),
                    info_hash_v2: None,
                    trackers: vec![],
                    display_name: None,
                    select_only: None,
                    peers: vec![],
                });
            }
        }

        let url =
            url::Url::parse(input).map_err(|e| MagnetError::Parse(format!("invalid URL: {e}")))?;
        if url.scheme() != "magnet" {
            return Err(MagnetError::BadScheme(url.scheme().to_owned()));
        }

        let mut info_hash_v1: Option<Id20> = None;
        let mut info_hash_v2: Option<Id32> = None;
        let mut trackers = Vec::new();
        let mut display_name: Option<String> = None;
        let mut select_only: Vec<usize> = Vec::new();
        let mut peers: Vec<MagnetPeer> = Vec::new();

        for (k, v) in url.query_pairs() {
            match &*k {
                "xt" => {
                    if let Some(rest) = v.strip_prefix("urn:btih:") {
                        let id = Id20::from_str(rest.trim())
                            .map_err(|_| MagnetError::BadInfoHash(rest.into()))?;
                        info_hash_v1 = Some(id);
                    } else if let Some(rest) = v.strip_prefix("urn:btmh:") {
                        let raw = hex::decode(rest.trim())
                            .map_err(|_| MagnetError::BadInfoHash(rest.into()))?;
                        if raw.len() != 34 || raw[0] != 0x12 || raw[1] != 0x20 {
                            return Err(MagnetError::BadInfoHash(format!(
                                "btmh requires sha-256 multihash (0x12 0x20 + 32 bytes), got {} bytes",
                                raw.len()
                            )));
                        }
                        let id = Id32::from_slice(&raw[2..])
                            .map_err(|_| MagnetError::BadInfoHash(rest.into()))?;
                        info_hash_v2 = Some(id);
                    }
                }
                "tr" => trackers.push(v.into_owned()),
                "dn" => display_name = Some(v.into_owned()),
                "x.pe" => {
                    const MAX_MAGNET_PEERS: usize = 64;
                    if peers.len() < MAX_MAGNET_PEERS {
                        if let Some(peer) = MagnetPeer::parse(&v) {
                            if !peers.contains(&peer) {
                                peers.push(peer);
                            }
                        }
                    }
                }
                "so" => {
                    const MAX_SO_TOTAL: usize = 100_000;
                    for part in v.split(',') {
                        let part = part.trim();
                        if part.is_empty() {
                            continue;
                        }
                        match part.split_once('-') {
                            Some((a, b)) => {
                                let a: usize = a.parse().map_err(|_| {
                                    MagnetError::Parse(format!("bad so range {part}"))
                                })?;
                                let b: usize = b.parse().map_err(|_| {
                                    MagnetError::Parse(format!("bad so range {part}"))
                                })?;
                                const MAX_SO_RANGE: usize = 100_000;
                                if b < a || b.saturating_sub(a) >= MAX_SO_RANGE {
                                    return Err(MagnetError::Parse(format!(
                                        "so range too large: {part}"
                                    )));
                                }
                                if select_only.len().saturating_add(b - a + 1) > MAX_SO_TOTAL {
                                    return Err(MagnetError::Parse(
                                        "so selection too large".into(),
                                    ));
                                }
                                select_only.extend(a..=b);
                            }
                            None => {
                                let i: usize = part
                                    .parse()
                                    .map_err(|_| MagnetError::Parse(format!("bad so {part}")))?;
                                if select_only.len() >= MAX_SO_TOTAL {
                                    return Err(MagnetError::Parse(
                                        "so selection too large".into(),
                                    ));
                                }
                                select_only.push(i);
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        let info_hash = match (info_hash_v1, info_hash_v2) {
            (Some(v1), _) => v1,
            (None, Some(v2)) => v2.truncate_to_id20(),
            (None, None) => return Err(MagnetError::MissingInfoHash),
        };

        Ok(Self {
            info_hash,
            info_hash_v1,
            info_hash_v2,
            trackers,
            display_name,
            select_only: (!select_only.is_empty()).then_some(select_only),
            peers,
        })
    }

    pub fn resolve_peers(&self) -> impl Stream<Item = SocketAddr> + Send + 'static {
        let mut seen = HashSet::new();
        stream::iter(self.peers.clone())
            .map(|peer| async move { peer.resolve().await })
            .buffer_unordered(MAX_CONCURRENT_PEER_LOOKUPS)
            .flat_map(stream::iter)
            .filter(move |addr| future::ready(seen.insert(*addr)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_hex() {
        let m =
            Magnet::parse("magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&dn=Hello")
                .unwrap();
        assert_eq!(
            m.info_hash.to_hex(),
            "cab507494d02ebb1178b38f2e9d7be299c86b862"
        );
        assert_eq!(m.display_name.as_deref(), Some("Hello"));
    }

    #[test]
    fn parse_base32() {
        let m = Magnet::parse("magnet:?xt=urn:btih:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA").unwrap();
        assert_eq!(m.info_hash.0, [0u8; 20]);
    }

    #[test]
    fn parse_trackers_and_so() {
        let m = Magnet::parse(
            "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&tr=http://a/announce&tr=udp://b:80&so=0,2-4",
        )
        .unwrap();
        assert_eq!(m.trackers.len(), 2);
        assert_eq!(m.select_only.as_deref(), Some(&[0usize, 2, 3, 4][..]));
    }

    #[test]
    fn parse_x_pe_peers() {
        let m = Magnet::parse(
            "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862\
             &x.pe=203.0.113.5:6881&x.pe=%5B2001:db8::1%5D:51413&x.pe=Peer.Example:7000\
             &x.pe=bad&x.pe=203.0.113.5:0&x.pe=203.0.113.5:6881",
        )
        .unwrap();
        assert_eq!(
            m.peers,
            vec![
                MagnetPeer::Addr("203.0.113.5:6881".parse().unwrap()),
                MagnetPeer::Addr("[2001:db8::1]:51413".parse().unwrap()),
                MagnetPeer::Host("peer.example".into(), 7000),
            ]
        );
    }

    #[tokio::test]
    async fn resolve_peers_streams_deduplicated_addresses() {
        let mut m = Magnet::parse(
            "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862\
             &x.pe=203.0.113.5:6881&x.pe=127.0.0.1:7000&x.pe=localhost:7000",
        )
        .unwrap();
        m.peers.push(MagnetPeer::Host("127.0.0.1".into(), 7000));
        let addrs: Vec<SocketAddr> = m.resolve_peers().collect().await;
        let loopback: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        assert!(addrs.contains(&"203.0.113.5:6881".parse().unwrap()));
        assert_eq!(addrs.iter().filter(|a| **a == loopback).count(), 1);
    }

    #[test]
    fn parse_bare_hex() {
        let m = Magnet::parse("cab507494d02ebb1178b38f2e9d7be299c86b862").unwrap();
        assert_eq!(m.trackers.len(), 0);
    }

    #[test]
    fn reject_missing_xt() {
        assert!(matches!(
            Magnet::parse("magnet:?dn=nothing"),
            Err(MagnetError::MissingInfoHash),
        ));
    }

    #[test]
    fn parse_v2_btmh() {
        let mut multihash = vec![0x12u8, 0x20];
        multihash.extend_from_slice(&[0u8; 32]);
        let hex = hex::encode(multihash);
        let m = Magnet::parse(&format!("magnet:?xt=urn:btmh:{hex}")).unwrap();
        assert!(m.info_hash_v1().is_none());
        let v2 = m.info_hash_v2().unwrap();
        assert_eq!(v2.0, [0u8; 32]);
        assert_eq!(m.info_hash().0, [0u8; 20]);
    }

    #[test]
    fn parse_hybrid_magnet() {
        let mut multihash = vec![0x12u8, 0x20];
        multihash.extend_from_slice(&[0xabu8; 32]);
        let v2hex = hex::encode(multihash);
        let v1hex = "cab507494d02ebb1178b38f2e9d7be299c86b862";
        let m = Magnet::parse(&format!("magnet:?xt=urn:btih:{v1hex}&xt=urn:btmh:{v2hex}")).unwrap();
        let v1 = m.info_hash_v1().unwrap();
        assert_eq!(v1.to_hex(), v1hex);
        assert_eq!(m.info_hash_v2().unwrap().0, [0xabu8; 32]);
        assert_eq!(m.info_hash(), v1);
    }

    #[test]
    fn reject_btmh_wrong_multihash_code() {
        let mut bad = vec![0x11u8, 0x14];
        bad.extend_from_slice(&[0u8; 20]);
        let hex = hex::encode(bad);
        assert!(matches!(
            Magnet::parse(&format!("magnet:?xt=urn:btmh:{hex}")),
            Err(MagnetError::BadInfoHash(_)),
        ));
    }

    #[test]
    fn so_total_is_capped() {
        let base = "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862";
        let many = ["0-99998"; 3].join(",");
        assert!(Magnet::parse(&format!("{base}&so={many}")).is_err());
        assert!(Magnet::parse(&format!("{base}&so=0-99998")).is_ok());
    }

    #[test]
    fn so_total_is_capped_across_repeated_fields() {
        let base = "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862";
        assert!(Magnet::parse(&format!("{base}&so=0-99998&so=0-99998")).is_err());
        let spam = "&so=0-99998".repeat(50);
        assert!(Magnet::parse(&format!("{base}{spam}")).is_err());
    }

    #[test]
    fn repeated_so_fields_are_merged() {
        let base = "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862";
        let m = Magnet::parse(&format!("{base}&so=0,2&so=&so=5-6")).unwrap();
        assert_eq!(m.select_only.as_deref(), Some(&[0usize, 2, 5, 6][..]));
        assert!(Magnet::parse(&format!("{base}&so="))
            .unwrap()
            .select_only
            .is_none());
    }
}
