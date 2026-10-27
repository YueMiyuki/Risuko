use std::collections::HashSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use futures_util::{future, stream, Stream, StreamExt};

use super::hash::{Id20, Id32};

const PEER_LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CONCURRENT_PEER_LOOKUPS: usize = 8;
const MAX_MAGNET_TRACKERS: usize = 64;
const MAX_MAGNET_WEB_SEEDS: usize = 32;
const MAX_SO_TOTAL: usize = 100_000;

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
    pub web_seeds: Vec<String>,
    pub display_name: Option<String>,
    pub select_only: Option<Vec<usize>>,
    pub peers: Vec<MagnetPeer>,
}

fn strip_prefix_ignore_case<'a>(value: &'a str, prefix: &str) -> Option<&'a str> {
    let head = value.get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix)
        .then(|| &value[prefix.len()..])
}

fn parse_select_only(value: &str, out: &mut Vec<usize>) -> Option<()> {
    const MAX_SO_RANGE: usize = 100_000;
    for part in value.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (first, last) = match part.split_once('-') {
            Some((a, b)) => (a.parse::<usize>().ok()?, b.parse::<usize>().ok()?),
            None => {
                let i = part.parse::<usize>().ok()?;
                (i, i)
            }
        };
        if last < first || last - first >= MAX_SO_RANGE {
            return None;
        }
        if out.len().saturating_add(last - first + 1) > MAX_SO_TOTAL {
            return None;
        }
        out.extend(first..=last);
    }
    Some(())
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
                    web_seeds: vec![],
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
        let mut web_seeds: Vec<String> = Vec::new();
        let mut display_name: Option<String> = None;
        let mut select_only: Vec<usize> = Vec::new();
        let mut so_invalid = false;
        let mut peers: Vec<MagnetPeer> = Vec::new();

        for (k, v) in url.query_pairs() {
            match &*k {
                "xt" => {
                    if let Some(rest) = strip_prefix_ignore_case(&v, "urn:btih:") {
                        let id = Id20::from_str(rest.trim())
                            .map_err(|_| MagnetError::BadInfoHash(rest.into()))?;
                        info_hash_v1 = Some(id);
                    } else if let Some(rest) = strip_prefix_ignore_case(&v, "urn:btmh:") {
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
                "tr" => {
                    for url in crate::torrent::split_tracker_list(&v) {
                        if trackers.len() < MAX_MAGNET_TRACKERS
                            && !trackers.iter().any(|known| known == url)
                        {
                            trackers.push(url.to_string());
                        }
                    }
                }
                "ws" => {
                    let url = v.trim();
                    if !url.is_empty()
                        && web_seeds.len() < MAX_MAGNET_WEB_SEEDS
                        && !web_seeds.iter().any(|known| known == url)
                    {
                        web_seeds.push(url.to_string());
                    }
                }
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
                "so" if !so_invalid => {
                    so_invalid = parse_select_only(&v, &mut select_only).is_none();
                    if so_invalid {
                        select_only.clear();
                        tracing::debug!("ignoring invalid magnet so= selection");
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

        select_only.sort_unstable();
        select_only.dedup();
        Ok(Self {
            info_hash,
            info_hash_v1,
            info_hash_v2,
            trackers,
            web_seeds,
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
        assert!(Magnet::parse(&format!("{base}&so={many}"))
            .unwrap()
            .select_only
            .is_none());
        assert!(Magnet::parse(&format!("{base}&so=0-99998"))
            .unwrap()
            .select_only
            .is_some());
    }

    #[test]
    fn so_total_is_capped_across_repeated_fields() {
        let base = "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862";
        let m = Magnet::parse(&format!("{base}&so=0-99998&so=0-99998")).unwrap();
        assert!(m.select_only.is_none());
        let spam = "&so=0-99998".repeat(50);
        assert!(Magnet::parse(&format!("{base}{spam}"))
            .unwrap()
            .select_only
            .is_none());
    }

    #[test]
    fn invalid_so_drops_the_selection_instead_of_rejecting_the_magnet() {
        let base = "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862";
        for so in ["abc", "0-100000", "5-2", "1,x,3", "1-", "-3"] {
            let m = Magnet::parse(&format!("{base}&so={so}")).unwrap();
            assert!(m.select_only.is_none(), "so={so}");
        }
        let m = Magnet::parse(&format!("{base}&so=1,2&so=oops&so=4")).unwrap();
        assert!(m.select_only.is_none());
        assert_eq!(m.info_hash().to_hex(), &base[20..]);
    }

    #[test]
    fn select_only_is_sorted_and_deduplicated() {
        let base = "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862";
        let m = Magnet::parse(&format!("{base}&so=5,1-3,2,5&so=0")).unwrap();
        assert_eq!(m.select_only.as_deref(), Some(&[0usize, 1, 2, 3, 5][..]));
    }

    #[test]
    fn urn_prefixes_match_case_insensitively() {
        let hex = "cab507494d02ebb1178b38f2e9d7be299c86b862";
        let lower = Magnet::parse(&format!("magnet:?xt=urn:btih:{hex}")).unwrap();
        let upper = Magnet::parse(&format!("magnet:?xt=urn:BTIH:{}", hex.to_uppercase())).unwrap();
        assert_eq!(upper.info_hash(), lower.info_hash());
        let mixed = Magnet::parse(&format!("magnet:?xt=Urn:BtIh:{hex}")).unwrap();
        assert_eq!(mixed.info_hash(), lower.info_hash());

        let mut multihash = vec![0x12u8, 0x20];
        multihash.extend_from_slice(&[7u8; 32]);
        let v2 = Magnet::parse(&format!("magnet:?xt=URN:BTMH:{}", hex::encode(multihash))).unwrap();
        assert_eq!(v2.info_hash_v2().unwrap().0, [7u8; 32]);
    }

    #[test]
    fn non_ascii_xt_values_do_not_panic_the_prefix_match() {
        assert!(matches!(
            Magnet::parse("magnet:?xt=urn:bt\u{e9}h:abc"),
            Err(MagnetError::MissingInfoHash)
        ));
        assert!(matches!(
            Magnet::parse("magnet:?xt=%E2%82%AC%E2%82%AC%E2%82%AC%E2%82%AC"),
            Err(MagnetError::MissingInfoHash)
        ));
    }

    #[test]
    fn tracker_list_is_deduplicated_and_capped() {
        let base = "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862";
        let many: String = (0..5_000)
            .map(|i| format!("&tr=udp://t{i}.example:80/announce"))
            .collect();
        let m = Magnet::parse(&format!("{base}{many}")).unwrap();
        assert_eq!(m.trackers.len(), MAX_MAGNET_TRACKERS);
        assert_eq!(m.trackers[0], "udp://t0.example:80/announce");

        let dup =
            Magnet::parse(&format!("{base}&tr=http://a/x&tr=http://a/x&tr=http://b/x")).unwrap();
        assert_eq!(dup.trackers, ["http://a/x", "http://b/x"]);
    }

    #[test]
    fn ws_parameters_are_collected_deduplicated_and_capped() {
        let base = "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862";
        let m = Magnet::parse(&format!(
            "{base}&ws=http%3A%2F%2Fa.example%2Ffile&ws=http%3A%2F%2Fa.example%2Ffile&ws=&ws=https://b.example/"
        ))
        .unwrap();
        assert_eq!(m.web_seeds, ["http://a.example/file", "https://b.example/"]);

        let many: String = (0..500)
            .map(|i| format!("&ws=http://w{i}.example/"))
            .collect();
        let m = Magnet::parse(&format!("{base}{many}")).unwrap();
        assert_eq!(m.web_seeds.len(), MAX_MAGNET_WEB_SEEDS);
    }

    #[test]
    fn comma_joined_tracker_values_count_against_the_cap() {
        let base = "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862";
        let joined = (0..5_000)
            .map(|i| format!("udp://t{i}.example:80/announce"))
            .collect::<Vec<_>>()
            .join(",");
        let m = Magnet::parse(&format!("{base}&tr={joined}")).unwrap();
        assert_eq!(m.trackers.len(), MAX_MAGNET_TRACKERS);
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
