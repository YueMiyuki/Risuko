use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::watch;

const MIN_VOTER_NETWORKS: usize = 3;
const MAX_CANDIDATES: usize = 32;
const MAX_VOTER_NETWORKS: usize = 64;
const AGE_INTERVAL: Duration = Duration::from_secs(60 * 60);
const MAX_AGE_STEPS: u32 = 8;
const MAX_UPNP_GATEWAYS: usize = 8;
const UPNP_TTL: Duration = Duration::from_secs(3 * 60 * 60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Peer,
    Tracker,
    Dht,
    Upnp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Direct,
    Proxied,
}

impl Route {
    pub fn of(proxy: Option<&risuko_http::ProxyConnector>) -> Self {
        if proxy.is_some_and(|proxy| proxy.has_proxy()) {
            Route::Proxied
        } else {
            Route::Direct
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Voter {
    Ip(IpAddr),
    Host,
}

impl Voter {
    fn network(self) -> Option<Network> {
        match self {
            Voter::Ip(ip) => match ip.to_canonical() {
                ip if !is_public_address(ip) => None,
                IpAddr::V4(ip) => {
                    let octets = ip.octets();
                    Some(Network::V4([octets[0], octets[1], octets[2]]))
                }
                IpAddr::V6(ip) => {
                    let octets = ip.octets();
                    let mut prefix = [0u8; 6];
                    prefix.copy_from_slice(&octets[..6]);
                    Some(Network::V6(prefix))
                }
            },
            Voter::Host => Some(Network::Host),
        }
    }
}

impl From<IpAddr> for Voter {
    fn from(ip: IpAddr) -> Self {
        Voter::Ip(ip)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Network {
    V4([u8; 3]),
    V6([u8; 6]),
    Host,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExternalAddresses {
    pub v4: Option<Ipv4Addr>,
    pub v6: Option<Ipv6Addr>,
}

impl ExternalAddresses {
    pub fn contains(&self, ip: IpAddr) -> bool {
        match ip.to_canonical() {
            IpAddr::V4(ip) => self.v4 == Some(ip),
            IpAddr::V6(ip) => self.v6 == Some(ip),
        }
    }
}

pub fn is_public_address(ip: IpAddr) -> bool {
    crate::dht::public_ip(ip) && crate::webseed::is_allowed_destination(ip)
}

#[derive(Debug)]
struct Candidate {
    addr: IpAddr,
    networks: Vec<Network>,
    touched: u64,
}

#[derive(Debug)]
struct Gateway {
    id: Voter,
    addr: IpAddr,
    backed: Option<Instant>,
}

#[derive(Debug, Default)]
struct Family {
    elected: Option<IpAddr>,
    gateways: Vec<Gateway>,
    candidates: Vec<Candidate>,
    clock: u64,
}

impl Family {
    fn backed(&self, addr: IpAddr) -> bool {
        self.gateways
            .iter()
            .any(|gateway| gateway.addr == addr && gateway.backed.is_some())
    }

    fn strength(&self, addr: IpAddr) -> usize {
        let networks = self
            .candidates
            .iter()
            .find(|candidate| candidate.addr == addr)
            .map_or(0, |candidate| candidate.networks.len());
        if self.backed(addr) {
            networks.max(MIN_VOTER_NETWORKS)
        } else {
            networks
        }
    }

    fn protected(&self, addr: IpAddr) -> bool {
        self.elected == Some(addr) || self.backed(addr)
    }

    fn cast(&mut self, addr: IpAddr, network: Network) -> bool {
        self.clock += 1;
        if let Some(candidate) = self
            .candidates
            .iter_mut()
            .find(|candidate| candidate.addr == addr && candidate.networks.contains(&network))
        {
            candidate.touched = self.clock;
            return false;
        }
        for candidate in &mut self.candidates {
            if candidate.addr != addr {
                candidate.networks.retain(|voted| *voted != network);
            }
        }
        self.candidates
            .retain(|candidate| candidate.addr == addr || !candidate.networks.is_empty());
        let index = match self
            .candidates
            .iter()
            .position(|candidate| candidate.addr == addr)
        {
            Some(index) => index,
            None => {
                if self.candidates.len() >= MAX_CANDIDATES && !self.evict_for_newcomer() {
                    return true;
                }
                self.candidates.push(Candidate {
                    addr,
                    networks: Vec::new(),
                    touched: self.clock,
                });
                self.candidates.len() - 1
            }
        };
        let candidate = &mut self.candidates[index];
        candidate.touched = self.clock;
        if candidate.networks.len() >= MAX_VOTER_NETWORKS {
            candidate.networks.remove(0);
        }
        candidate.networks.push(network);
        true
    }

    fn evict_for_newcomer(&mut self) -> bool {
        let victim = self
            .candidates
            .iter()
            .enumerate()
            .min_by_key(|(_, candidate)| {
                (
                    self.protected(candidate.addr),
                    candidate.networks.len(),
                    candidate.touched,
                )
            })
            .map(|(index, _)| index);
        match victim {
            Some(index)
                if !self.protected(self.candidates[index].addr)
                    && self.candidates[index].networks.len() <= 1 =>
            {
                self.candidates.swap_remove(index);
                true
            }
            _ => false,
        }
    }

    fn report_upnp(&mut self, addr: IpAddr, id: Voter, now: Instant) {
        let previous = match self.gateways.iter_mut().find(|gateway| gateway.id == id) {
            Some(gateway) => {
                gateway.backed = Some(now);
                Some(std::mem::replace(&mut gateway.addr, addr))
            }
            None => {
                if self.gateways.len() >= MAX_UPNP_GATEWAYS {
                    self.gateways.remove(0);
                }
                self.gateways.push(Gateway {
                    id,
                    addr,
                    backed: Some(now),
                });
                None
            }
        };
        if previous == Some(addr) {
            return;
        }
        if let Some(stale) = previous {
            self.candidates.retain(|candidate| candidate.addr != stale);
        }
        let wins = self
            .elected
            .is_none_or(|current| current == addr || self.strength(addr) >= self.strength(current));
        if wins {
            self.elected = Some(addr);
        }
    }

    fn reelect(&mut self) {
        let mut best: Option<(IpAddr, usize)> = None;
        for addr in self
            .candidates
            .iter()
            .map(|candidate| candidate.addr)
            .chain(self.gateways.iter().map(|gateway| gateway.addr))
        {
            let strength = self.strength(addr);
            if strength < MIN_VOTER_NETWORKS {
                continue;
            }
            if best.is_none_or(|(_, top)| strength > top) {
                best = Some((addr, strength));
            }
        }
        let Some((addr, strength)) = best else {
            return;
        };
        match self.elected {
            Some(current) if current == addr || strength <= self.strength(current) => {}
            _ => self.elected = Some(addr),
        }
    }

    fn halve(&mut self, now: Instant) {
        for candidate in &mut self.candidates {
            let keep = candidate.networks.len() / 2;
            let drop = candidate.networks.len() - keep;
            candidate.networks.drain(..drop);
        }
        self.candidates
            .retain(|candidate| !candidate.networks.is_empty());
        for gateway in &mut self.gateways {
            if gateway
                .backed
                .is_some_and(|at| now.saturating_duration_since(at) >= UPNP_TTL)
            {
                gateway.backed = None;
            }
        }
    }
}

#[derive(Debug)]
struct State {
    v4: Family,
    v6: Family,
    aged: Instant,
}

impl State {
    fn family_mut(&mut self, addr: IpAddr) -> &mut Family {
        match addr {
            IpAddr::V4(_) => &mut self.v4,
            IpAddr::V6(_) => &mut self.v6,
        }
    }

    fn age(&mut self, now: Instant) -> bool {
        let mut steps = 0;
        while now.saturating_duration_since(self.aged) >= AGE_INTERVAL {
            self.aged += AGE_INTERVAL;
            self.v4.halve(now);
            self.v6.halve(now);
            steps += 1;
            if steps >= MAX_AGE_STEPS {
                self.aged = now;
                break;
            }
        }
        steps > 0
    }

    fn snapshot(&self) -> ExternalAddresses {
        ExternalAddresses {
            v4: match self.v4.elected {
                Some(IpAddr::V4(ip)) => Some(ip),
                _ => None,
            },
            v6: match self.v6.elected {
                Some(IpAddr::V6(ip)) => Some(ip),
                _ => None,
            },
        }
    }
}

struct Inner {
    state: Mutex<State>,
    published: watch::Sender<ExternalAddresses>,
}

#[derive(Clone)]
pub struct ExternalIp {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ExternalIp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalIp")
            .field("addresses", &self.addresses())
            .finish()
    }
}

impl Default for ExternalIp {
    fn default() -> Self {
        Self::new()
    }
}

impl ExternalIp {
    pub fn new() -> Self {
        Self::starting_at(Instant::now())
    }

    fn starting_at(now: Instant) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    v4: Family::default(),
                    v6: Family::default(),
                    aged: now,
                }),
                published: watch::Sender::new(ExternalAddresses::default()),
            }),
        }
    }

    pub fn shared() -> &'static ExternalIp {
        static SHARED: OnceLock<ExternalIp> = OnceLock::new();
        SHARED.get_or_init(ExternalIp::new)
    }

    pub fn vote(&self, observed: IpAddr, source: Source, voter: impl Into<Voter>, route: Route) {
        self.vote_at(Instant::now(), observed, source, voter.into(), route);
    }

    fn vote_at(&self, now: Instant, observed: IpAddr, source: Source, voter: Voter, route: Route) {
        let observed = observed.to_canonical();
        if !is_public_address(observed) {
            return;
        }
        let network = if source == Source::Upnp {
            None
        } else if route == Route::Proxied {
            return;
        } else if let Some(network) = voter.network() {
            Some(network)
        } else {
            return;
        };
        let mut state = self.inner.state.lock();
        let aged = state.age(now);
        let family = state.family_mut(observed);
        let changed = match network {
            Some(network) => family.cast(observed, network),
            None => {
                family.report_upnp(observed, voter, now);
                true
            }
        };
        if !changed && !aged {
            return;
        }
        family.reelect();
        let snapshot = state.snapshot();
        self.inner.published.send_if_modified(|current| {
            let changed = *current != snapshot;
            *current = snapshot;
            changed
        });
    }

    pub fn addresses(&self) -> ExternalAddresses {
        *self.inner.published.borrow()
    }

    pub fn external_v4(&self) -> Option<Ipv4Addr> {
        self.addresses().v4
    }

    pub fn subscribe(&self) -> watch::Receiver<ExternalAddresses> {
        self.inner.published.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn voter(index: u8) -> IpAddr {
        IpAddr::from([23, index, 0, 1])
    }

    fn voter6(index: u16) -> IpAddr {
        IpAddr::V6(Ipv6Addr::new(0x2a00, index, 0, 0, 0, 0, 0, 1))
    }

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    fn peer_votes(ext: &ExternalIp, observed: IpAddr, voters: impl IntoIterator<Item = IpAddr>) {
        for voter in voters {
            ext.vote(observed, Source::Peer, voter, Route::Direct);
        }
    }

    fn candidates(ext: &ExternalIp) -> (usize, usize) {
        let state = ext.inner.state.lock();
        (state.v4.candidates.len(), state.v6.candidates.len())
    }

    #[test]
    fn three_distinct_voter_networks_elect_an_address() {
        let ext = ExternalIp::new();
        let seen = ip("93.184.216.34");
        peer_votes(&ext, seen, [voter(1), voter(2)]);
        assert_eq!(ext.external_v4(), None);
        ext.vote(seen, Source::Dht, voter(3), Route::Direct);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
        assert_eq!(ext.addresses().v6, None);
    }

    #[test]
    fn peer_tracker_and_dht_sources_all_count_toward_the_quorum() {
        let ext = ExternalIp::new();
        let seen = ip("93.184.216.34");
        ext.vote(seen, Source::Peer, voter(1), Route::Direct);
        ext.vote(seen, Source::Tracker, Voter::Host, Route::Direct);
        assert_eq!(ext.external_v4(), None);
        ext.vote(seen, Source::Dht, voter(3), Route::Direct);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn one_ipv4_24_network_cannot_stuff_the_ballot() {
        let ext = ExternalIp::new();
        let seen = ip("93.184.216.34");
        peer_votes(
            &ext,
            seen,
            (1..=200).map(|host| IpAddr::from([23, 1, 0, host])),
        );
        assert_eq!(ext.external_v4(), None);
        peer_votes(&ext, seen, [IpAddr::from([23, 1, 1, 9]), voter(3)]);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn one_ipv6_48_network_cannot_stuff_the_ballot() {
        let ext = ExternalIp::new();
        let seen = ip("2a00:1450:4001:80b::200e");
        for tail in 1..=200u16 {
            let voter = IpAddr::V6(Ipv6Addr::new(0x2a00, 7, 1, tail, 0, 0, 0, 1));
            ext.vote(seen, Source::Peer, voter, Route::Direct);
        }
        assert_eq!(ext.addresses().v6, None);
        peer_votes(&ext, seen, [voter6(8), voter6(9)]);
        assert_eq!(ext.addresses().v6, Some(seen.to_string().parse().unwrap()));
        assert_eq!(ext.external_v4(), None);
    }

    #[test]
    fn repeated_votes_from_one_network_count_once() {
        let ext = ExternalIp::new();
        let seen = ip("93.184.216.34");
        for _ in 0..10 {
            peer_votes(&ext, seen, [voter(1), voter(2)]);
        }
        assert_eq!(ext.external_v4(), None);
    }

    #[test]
    fn ipv4_mapped_voters_share_the_ipv4_network_buckets() {
        let ext = ExternalIp::new();
        let seen = ip("93.184.216.34");
        let mapped = |index: u8| ip(&format!("::ffff:23.{index}.0.1"));
        peer_votes(&ext, seen, [mapped(1), mapped(2)]);
        assert_eq!(ext.external_v4(), None);
        peer_votes(&ext, seen, [voter(1), voter(2)]);
        assert_eq!(ext.external_v4(), None);
        peer_votes(&ext, seen, [mapped(3)]);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn mapped_observed_addresses_vote_in_the_ipv4_family() {
        let ext = ExternalIp::new();
        let seen = ip("::ffff:93.184.216.34");
        peer_votes(&ext, seen, [voter(1), voter(2), voter(3)]);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
        assert_eq!(ext.addresses().v6, None);
    }

    #[test]
    fn non_public_observed_addresses_never_win() {
        let ext = ExternalIp::new();
        for text in [
            "127.0.0.1",
            "0.0.0.0",
            "10.1.2.3",
            "172.16.5.5",
            "192.168.1.20",
            "169.254.10.10",
            "100.64.0.9",
            "100.127.255.1",
            "192.0.2.7",
            "198.51.100.7",
            "203.0.113.7",
            "224.0.0.251",
            "255.255.255.255",
            "::",
            "::1",
            "fe80::1",
            "fc00::1",
            "fd12:3456::1",
            "2001:db8::1",
            "ff02::1",
            "::ffff:10.0.0.1",
            "::ffff:192.168.1.1",
        ] {
            for source in [Source::Peer, Source::Tracker, Source::Dht, Source::Upnp] {
                peer_votes(&ext, ip(text), (1..=6).map(voter));
                ext.vote(ip(text), source, voter(9), Route::Direct);
            }
        }
        assert_eq!(ext.addresses(), ExternalAddresses::default());
        assert_eq!(candidates(&ext), (0, 0));
    }

    #[test]
    fn non_public_voters_are_ignored() {
        let ext = ExternalIp::new();
        let seen = ip("93.184.216.34");
        peer_votes(
            &ext,
            seen,
            [
                ip("127.0.0.2"),
                ip("127.0.1.2"),
                ip("127.0.2.2"),
                ip("10.0.0.1"),
                ip("192.168.1.1"),
                ip("100.64.1.1"),
                ip("fe80::2"),
                ip("fd00::2"),
            ],
        );
        assert_eq!(ext.external_v4(), None);
        assert_eq!(candidates(&ext), (0, 0));
    }

    #[test]
    fn a_single_upnp_vote_elects_a_public_address() {
        let ext = ExternalIp::new();
        let gateway = ip("192.168.1.1");
        ext.vote(ip("10.9.8.7"), Source::Upnp, gateway, Route::Direct);
        ext.vote(ip("100.70.1.1"), Source::Upnp, gateway, Route::Direct);
        ext.vote(ip("0.0.0.0"), Source::Upnp, gateway, Route::Direct);
        assert_eq!(ext.external_v4(), None);
        ext.vote(ip("93.184.216.34"), Source::Upnp, gateway, Route::Direct);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn a_non_public_upnp_report_leaves_the_election_alone() {
        let ext = ExternalIp::new();
        let gateway = ip("192.168.1.1");
        ext.vote(ip("93.184.216.34"), Source::Upnp, gateway, Route::Direct);
        ext.vote(ip("100.70.1.1"), Source::Upnp, gateway, Route::Direct);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn a_gateway_address_change_switches_the_election_and_drops_the_old_tally() {
        let ext = ExternalIp::new();
        let (gateway, old, new) = (ip("192.168.1.1"), ip("93.184.216.34"), ip("8.8.4.4"));
        ext.vote(old, Source::Upnp, gateway, Route::Direct);
        peer_votes(&ext, old, (1..=6).map(voter));
        ext.vote(ip("198.18.0.1"), Source::Upnp, gateway, Route::Direct);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
        ext.vote(new, Source::Upnp, gateway, Route::Direct);
        assert_eq!(ext.external_v4(), Some("8.8.4.4".parse().unwrap()));
        peer_votes(&ext, old, (1..=3).map(voter));
        assert_eq!(ext.external_v4(), Some("8.8.4.4".parse().unwrap()));
        peer_votes(&ext, old, [voter(4)]);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn a_new_upnp_report_is_elected_over_a_weakly_corroborated_incumbent() {
        let ext = ExternalIp::new();
        peer_votes(&ext, ip("93.184.216.34"), (1..=3).map(voter));
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
        ext.vote(
            ip("8.8.4.4"),
            Source::Upnp,
            ip("192.168.1.1"),
            Route::Direct,
        );
        assert_eq!(ext.external_v4(), Some("8.8.4.4".parse().unwrap()));
    }

    #[test]
    fn a_new_upnp_report_does_not_displace_a_better_corroborated_incumbent() {
        let ext = ExternalIp::new();
        peer_votes(&ext, ip("93.184.216.34"), (1..=5).map(voter));
        ext.vote(
            ip("8.8.4.4"),
            Source::Upnp,
            ip("192.168.1.1"),
            Route::Direct,
        );
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn peers_overturn_a_contradicted_upnp_address() {
        let ext = ExternalIp::new();
        let (gateway, upnp, real) = (ip("192.168.1.1"), ip("8.8.4.4"), ip("93.184.216.34"));
        ext.vote(upnp, Source::Upnp, gateway, Route::Direct);
        assert_eq!(ext.external_v4(), Some("8.8.4.4".parse().unwrap()));
        peer_votes(&ext, real, (1..=3).map(voter));
        assert_eq!(ext.external_v4(), Some("8.8.4.4".parse().unwrap()));
        peer_votes(&ext, real, [voter(4)]);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn an_unchanged_upnp_renewal_does_not_reclaim_an_overturned_election() {
        let ext = ExternalIp::new();
        let (gateway, upnp, real) = (ip("192.168.1.1"), ip("8.8.4.4"), ip("93.184.216.34"));
        ext.vote(upnp, Source::Upnp, gateway, Route::Direct);
        peer_votes(&ext, real, (1..=6).map(voter));
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
        for _ in 0..5 {
            ext.vote(upnp, Source::Upnp, gateway, Route::Direct);
        }
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn disagreeing_gateways_do_not_force_a_switch_on_every_renewal() {
        let ext = ExternalIp::new();
        let (first, second) = (ip("192.168.1.1"), ip("192.168.1.2"));
        let (a, b) = (ip("8.8.4.4"), ip("151.101.1.69"));
        ext.vote(a, Source::Upnp, first, Route::Direct);
        ext.vote(b, Source::Upnp, second, Route::Direct);
        assert_eq!(ext.external_v4(), Some("151.101.1.69".parse().unwrap()));
        for _ in 0..5 {
            ext.vote(a, Source::Upnp, first, Route::Direct);
            assert_eq!(ext.external_v4(), Some("151.101.1.69".parse().unwrap()));
            ext.vote(b, Source::Upnp, second, Route::Direct);
            assert_eq!(ext.external_v4(), Some("151.101.1.69".parse().unwrap()));
        }
    }

    #[test]
    fn a_gateway_returning_after_an_outage_with_the_same_address_is_not_forced() {
        let t0 = Instant::now();
        let ext = ExternalIp::starting_at(t0);
        let (gateway, upnp, real) = (ip("192.168.1.1"), ip("8.8.4.4"), ip("93.184.216.34"));
        ext.vote_at(t0, upnp, Source::Upnp, gateway.into(), Route::Direct);
        let later = t0 + UPNP_TTL + AGE_INTERVAL;
        for index in 10..=12u8 {
            ext.vote_at(
                later,
                real,
                Source::Peer,
                voter(index).into(),
                Route::Direct,
            );
        }
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
        ext.vote_at(later, upnp, Source::Upnp, gateway.into(), Route::Direct);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn a_challenger_needs_strictly_more_networks_than_the_incumbent() {
        let ext = ExternalIp::new();
        let (old, new) = (ip("93.184.216.34"), ip("8.8.4.4"));
        peer_votes(&ext, old, (1..=3).map(voter));
        peer_votes(&ext, new, (10..=12).map(voter));
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
        peer_votes(&ext, new, [voter(13)]);
        assert_eq!(ext.external_v4(), Some("8.8.4.4".parse().unwrap()));
        peer_votes(&ext, old, [voter(20)]);
        assert_eq!(ext.external_v4(), Some("8.8.4.4".parse().unwrap()));
        peer_votes(&ext, old, [voter(21)]);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn a_challenger_below_the_threshold_never_replaces_the_incumbent() {
        let ext = ExternalIp::new();
        peer_votes(&ext, ip("93.184.216.34"), [voter(1)]);
        peer_votes(&ext, ip("93.184.216.34"), [voter(2), voter(3)]);
        peer_votes(&ext, ip("8.8.4.4"), [voter(10), voter(11)]);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn families_elect_independently() {
        let ext = ExternalIp::new();
        peer_votes(&ext, ip("93.184.216.34"), (1..=3).map(voter));
        peer_votes(&ext, ip("2a00:1450:4001:80b::200e"), (1..=3).map(voter6));
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
        assert_eq!(
            ext.addresses().v6,
            Some("2a00:1450:4001:80b::200e".parse().unwrap())
        );
        assert!(ext.addresses().contains(ip("::ffff:93.184.216.34")));
        assert!(!ext.addresses().contains(ip("8.8.8.8")));
    }

    #[test]
    fn candidate_and_voter_tallies_stay_bounded() {
        let ext = ExternalIp::new();
        for octet in 0..=255u8 {
            let seen = IpAddr::from([45, octet, 0, 1]);
            ext.vote(seen, Source::Peer, voter(octet), Route::Direct);
        }
        assert!(candidates(&ext).0 <= MAX_CANDIDATES);

        let busy = ip("93.184.216.34");
        for index in 0..=255u8 {
            for tail in 0..2u8 {
                ext.vote(
                    busy,
                    Source::Peer,
                    IpAddr::from([60, index, tail, 1]),
                    Route::Direct,
                );
            }
        }
        let state = ext.inner.state.lock();
        assert!(state.v4.candidates.len() <= MAX_CANDIDATES);
        assert!(state
            .v4
            .candidates
            .iter()
            .all(|candidate| candidate.networks.len() <= MAX_VOTER_NETWORKS));
        assert_eq!(state.v4.elected, Some(busy));
    }

    #[test]
    fn the_least_voted_candidate_is_evicted_but_the_incumbent_and_heavier_ones_stay() {
        let ext = ExternalIp::new();
        let (incumbent, heavy) = (ip("93.184.216.34"), ip("8.8.4.4"));
        peer_votes(&ext, incumbent, (1..=3).map(voter));
        peer_votes(&ext, heavy, (11..=12).map(voter));
        for octet in 0..64u8 {
            ext.vote(
                IpAddr::from([45, octet, 0, 1]),
                Source::Peer,
                voter(100 + octet),
                Route::Direct,
            );
        }
        let state = ext.inner.state.lock();
        assert!(state.v4.candidates.len() <= MAX_CANDIDATES);
        let has = |addr: IpAddr| state.v4.candidates.iter().any(|c| c.addr == addr);
        assert!(has(incumbent) && has(heavy));
        assert_eq!(state.v4.elected, Some(incumbent));
    }

    #[test]
    fn one_voter_network_flooding_addresses_never_holds_more_than_one_candidate() {
        let ext = ExternalIp::new();
        let real = ip("93.184.216.34");
        peer_votes(&ext, real, [voter(1), voter(2)]);
        for round in 0..4u8 {
            for octet in 0..=255u8 {
                ext.vote(
                    IpAddr::from([45, round, octet, 1]),
                    Source::Peer,
                    voter(50),
                    Route::Direct,
                );
            }
        }
        assert_eq!(candidates(&ext).0, 2);
        peer_votes(&ext, real, [voter(3)]);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn a_network_changing_its_answer_moves_its_vote_to_the_new_address() {
        let ext = ExternalIp::new();
        let (old, new) = (ip("93.184.216.34"), ip("8.8.4.4"));
        peer_votes(&ext, old, (1..=3).map(voter));
        peer_votes(&ext, new, (4..=5).map(voter));
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
        peer_votes(&ext, new, [voter(1)]);
        assert_eq!(ext.external_v4(), Some("8.8.4.4".parse().unwrap()));
        let state = ext.inner.state.lock();
        let tally = |addr: IpAddr| {
            state
                .v4
                .candidates
                .iter()
                .find(|c| c.addr == addr)
                .map_or(0, |c| c.networks.len())
        };
        assert_eq!((tally(old), tally(new)), (2, 3));
    }

    #[test]
    fn tallies_halve_every_hour_so_a_real_address_change_is_picked_up() {
        let t0 = Instant::now();
        let ext = ExternalIp::starting_at(t0);
        let (old, new) = (ip("93.184.216.34"), ip("8.8.4.4"));
        let cast = |at: Instant, seen: IpAddr, voters: &mut dyn Iterator<Item = u8>| {
            for index in voters {
                ext.vote_at(at, seen, Source::Peer, voter(index).into(), Route::Direct);
            }
        };
        cast(t0, old, &mut (1..=40));
        cast(t0, new, &mut (100..=139));
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
        let hour = t0 + AGE_INTERVAL + Duration::from_secs(1);
        cast(hour, new, &mut (140..=145));
        assert_eq!(ext.external_v4(), Some("8.8.4.4".parse().unwrap()));
    }

    #[test]
    fn a_decayed_incumbent_stays_elected_until_someone_replaces_it() {
        let t0 = Instant::now();
        let ext = ExternalIp::starting_at(t0);
        let seen = ip("93.184.216.34");
        for index in 1..=4u8 {
            ext.vote_at(t0, seen, Source::Peer, voter(index).into(), Route::Direct);
        }
        let later = t0 + AGE_INTERVAL * 20;
        ext.vote_at(
            later,
            ip("8.8.4.4"),
            Source::Peer,
            voter(9).into(),
            Route::Direct,
        );
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
        for index in 10..=12u8 {
            ext.vote_at(
                later,
                ip("8.8.4.4"),
                Source::Peer,
                voter(index).into(),
                Route::Direct,
            );
        }
        assert_eq!(ext.external_v4(), Some("8.8.4.4".parse().unwrap()));
    }

    #[test]
    fn a_stale_upnp_report_stops_backing_its_address() {
        let t0 = Instant::now();
        let ext = ExternalIp::starting_at(t0);
        let gateway = voter(1);
        ext.vote_at(
            t0,
            ip("93.184.216.34"),
            Source::Upnp,
            gateway.into(),
            Route::Direct,
        );
        let later = t0 + UPNP_TTL + AGE_INTERVAL;
        for index in 10..=12u8 {
            ext.vote_at(
                later,
                ip("8.8.4.4"),
                Source::Peer,
                voter(index).into(),
                Route::Direct,
            );
        }
        assert_eq!(ext.external_v4(), Some("8.8.4.4".parse().unwrap()));
    }

    #[test]
    fn votes_over_a_proxied_route_never_elect_an_address() {
        let ext = ExternalIp::new();
        let exit = ip("93.184.216.34");
        for source in [Source::Peer, Source::Tracker, Source::Dht] {
            for index in 1..=20u8 {
                ext.vote(exit, source, voter(index), Route::Proxied);
            }
        }
        assert_eq!(ext.addresses(), ExternalAddresses::default());
        assert_eq!(candidates(&ext), (0, 0));
    }

    #[test]
    fn proxied_votes_do_not_help_a_direct_candidate_reach_the_quorum() {
        let ext = ExternalIp::new();
        let seen = ip("93.184.216.34");
        peer_votes(&ext, seen, [voter(1), voter(2)]);
        for index in 3..=20u8 {
            ext.vote(seen, Source::Peer, voter(index), Route::Proxied);
        }
        assert_eq!(ext.external_v4(), None);
    }

    #[test]
    fn upnp_still_elects_while_proxied() {
        let ext = ExternalIp::new();
        ext.vote(
            ip("93.184.216.34"),
            Source::Upnp,
            ip("192.168.1.1"),
            Route::Proxied,
        );
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn route_follows_whether_a_proxy_is_configured() {
        use risuko_http::{Proxy, ProxyConnector};

        assert_eq!(Route::of(None), Route::Direct);
        assert_eq!(Route::of(Some(&ProxyConnector::direct())), Route::Direct);
        let proxy = ProxyConnector::from_proxy(Proxy::all("socks5://127.0.0.1:1080").unwrap());
        assert_eq!(Route::of(Some(&proxy)), Route::Proxied);
        let udp_only = ProxyConnector::direct().with_udp_proxy(Some(proxy));
        assert_eq!(Route::of(Some(&udp_only)), Route::Proxied);
    }

    #[test]
    fn all_hostname_voters_together_count_as_one_network() {
        let ext = ExternalIp::new();
        let seen = ip("93.184.216.34");
        for _ in 0..200 {
            ext.vote(seen, Source::Tracker, Voter::Host, Route::Direct);
        }
        assert_eq!(ext.external_v4(), None);
        peer_votes(&ext, seen, [voter(1)]);
        assert_eq!(ext.external_v4(), None);
        peer_votes(&ext, seen, [voter(2)]);
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
    }

    #[test]
    fn subscribers_hear_about_changes_only() {
        let ext = ExternalIp::new();
        let mut rx = ext.subscribe();
        assert!(!rx.has_changed().unwrap());
        peer_votes(&ext, ip("93.184.216.34"), [voter(1), voter(2)]);
        assert!(!rx.has_changed().unwrap());
        peer_votes(&ext, ip("93.184.216.34"), [voter(3)]);
        assert!(rx.has_changed().unwrap());
        assert_eq!(
            rx.borrow_and_update().v4,
            Some("93.184.216.34".parse().unwrap())
        );
        peer_votes(&ext, ip("93.184.216.34"), [voter(4), voter(5)]);
        assert!(!rx.has_changed().unwrap());
    }

    #[test]
    fn clones_and_the_shared_instance_see_the_same_election() {
        let ext = ExternalIp::new();
        let clone = ext.clone();
        peer_votes(&clone, ip("93.184.216.34"), (1..=3).map(voter));
        assert_eq!(ext.external_v4(), Some("93.184.216.34".parse().unwrap()));
        assert!(std::ptr::eq(ExternalIp::shared(), ExternalIp::shared()));
    }
}
