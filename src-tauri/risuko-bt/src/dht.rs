use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::RngExt;
use tokio::net::{lookup_host, UdpSocket};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;

use risuko_http::{ProxyAssociation, ProxyDatagramSource};

use super::bencode::{decode_all_external, encode_to_vec, DecodeLimits, Value};
use super::core::peer_id::{parse_version_part, PEER_ID_PREFIX};
use super::core::Id20;
use super::external_ip::{ExternalAddresses, ExternalIp, Route, Source};
use super::net::host_prefix;

mod node_id;

use node_id::{initial_node_id, node_id_valid, plan_node_id, IdPlan};

type GetPeersReply = (
    DhtTarget,
    Option<Id20>,
    Vec<SocketAddr>,
    Vec<(Id20, SocketAddr)>,
    Option<Vec<u8>>,
);

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum DhtTarget {
    Addr(SocketAddr),
    Host(String, u16),
}

type GetPeersResponseBody = (
    Option<Id20>,
    Vec<SocketAddr>,
    Vec<(Id20, SocketAddr)>,
    Option<Vec<u8>>,
);

const K: usize = 8;
const ALPHA: usize = 3;
const V6_RETRY_AFTER: Duration = Duration::from_secs(120);
const QUERY_TIMEOUT: Duration = Duration::from_secs(4);
const MAX_ROUND_QUERIES: usize = 50;
const MAX_KRPC_DATAGRAM: usize = 16 * 1024;
const KRPC_DECODE_LIMITS: DecodeLimits = DecodeLimits::new(MAX_KRPC_DATAGRAM, 16, 1024);
const QUERY_RATE_PER_SEC: f64 = 5.0;
const QUERY_BURST: f64 = 10.0;
const QUERY_BLOCK: Duration = Duration::from_secs(5 * 60);
const MAX_TRACKED_QUERY_IPS: usize = 4096;
const REPLY_BYTES_PER_SEC: f64 = 32.0 * 1024.0;
const PEER_STORE_TTL: Duration = Duration::from_secs(30 * 60);
const TOKEN_ROTATION: Duration = Duration::from_secs(5 * 60);
const MAX_STORED_PEERS: usize = 2048;
const MAX_PEERS_PER_HASH: usize = 256;
const MAX_PEERS_PER_IP: usize = 4;
const ROUTING_STALE: Duration = Duration::from_secs(15 * 60);
const MAX_PERSISTED_ROUTES: usize = 4096;
const ROUTING_REFRESH_INTERVAL: Duration = Duration::from_secs(2 * 60);
const ROUTING_PERSIST_INTERVAL: Duration = Duration::from_secs(5 * 60);
const REFRESH_PINGS_PER_TICK: usize = 64;
const REFRESH_PINGS_IN_FLIGHT: usize = 16;
const PORT_PING_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_PORT_PINGED: usize = 1024;
const MAX_ROUTER_ADDRS: usize = 256;
const MAX_KRPC_RESPONSE_BYTES: usize = 1024;
const CROSS_FAMILY_MIN_NODES: usize = K * 2;

const fn version_byte(part: &str) -> u8 {
    let value = parse_version_part(part);
    if value > 255 {
        255
    } else {
        value as u8
    }
}

const DHT_VERSION: [u8; 4] = [
    PEER_ID_PREFIX[1],
    PEER_ID_PREFIX[2],
    version_byte(env!("CARGO_PKG_VERSION_MAJOR")),
    version_byte(env!("CARGO_PKG_VERSION_MINOR")),
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Want {
    V4,
    V6,
    Both,
}

fn want_families(target_v6: Option<bool>, v6_usable: bool, live4: usize, live6: usize) -> Want {
    match target_v6 {
        Some(false) if v6_usable && live6 < CROSS_FAMILY_MIN_NODES => Want::Both,
        Some(false) => Want::V4,
        Some(true) if live4 < CROSS_FAMILY_MIN_NODES => Want::Both,
        Some(true) => Want::V6,
        None if v6_usable => Want::Both,
        None => Want::V4,
    }
}

#[derive(Debug, Clone)]
struct StoredPeer {
    addr: SocketAddr,
    seen: Instant,
}

#[derive(Debug)]
struct TokenState {
    current: [u8; 16],
    previous: [u8; 16],
    rotated: Instant,
}

impl TokenState {
    fn new() -> Self {
        let mut current = [0u8; 16];
        rand::rng().fill(&mut current);
        Self {
            current,
            previous: current,
            rotated: Instant::now(),
        }
    }

    fn rotate_if_needed(&mut self) {
        let elapsed = self.rotated.elapsed();
        if elapsed < TOKEN_ROTATION {
            return;
        }
        if elapsed >= TOKEN_ROTATION * 2 {
            rand::rng().fill(&mut self.current);
            rand::rng().fill(&mut self.previous);
            self.rotated = Instant::now();
            return;
        }
        self.previous = self.current;
        rand::rng().fill(&mut self.current);
        self.rotated += TOKEN_ROTATION;
    }

    fn token_for(&mut self, addr: SocketAddr) -> Vec<u8> {
        self.rotate_if_needed();
        token_for_secret(&self.current, addr)
    }

    fn valid(&mut self, addr: SocketAddr, token: &[u8]) -> bool {
        self.rotate_if_needed();
        token == token_for_secret(&self.current, addr).as_slice()
            || token == token_for_secret(&self.previous, addr).as_slice()
    }
}

#[derive(Debug)]
struct InboundDhtState {
    routing: Arc<Mutex<RoutingTable>>,
    routing6: Arc<Mutex<RoutingTable>>,
    peers: Mutex<HashMap<Id20, Vec<StoredPeer>>>,
    tokens: Mutex<TokenState>,
    private_hashes: Mutex<HashSet<Id20>>,
}

impl InboundDhtState {
    #[cfg(test)]
    fn new(our_id: Id20, routing: Arc<Mutex<RoutingTable>>) -> Self {
        Self::with_routing6(routing, Arc::new(Mutex::new(RoutingTable::new(our_id))))
    }

    fn with_routing6(
        routing: Arc<Mutex<RoutingTable>>,
        routing6: Arc<Mutex<RoutingTable>>,
    ) -> Self {
        Self {
            routing,
            routing6,
            peers: Mutex::new(HashMap::new()),
            tokens: Mutex::new(TokenState::new()),
            private_hashes: Mutex::new(HashSet::new()),
        }
    }

    fn table(&self, v6: bool) -> &Mutex<RoutingTable> {
        if v6 {
            &self.routing6
        } else {
            &self.routing
        }
    }

    fn node_id(&self, v6: bool) -> Id20 {
        self.table(v6).lock().our_id
    }

    fn token(&self, addr: SocketAddr) -> Vec<u8> {
        self.tokens.lock().token_for(addr)
    }

    fn valid_token(&self, addr: SocketAddr, token: &[u8]) -> bool {
        self.tokens.lock().valid(addr, token)
    }

    fn get_peers(&self, hash: Id20, family: Option<bool>) -> Vec<SocketAddr> {
        let now = Instant::now();
        let mut peers = self.peers.lock();
        let Some(entries) = peers.get_mut(&hash) else {
            return Vec::new();
        };
        entries.retain(|peer| now.duration_since(peer.seen) <= PEER_STORE_TTL);
        let result = entries
            .iter()
            .rev()
            .filter(|peer| {
                matches!(
                    (family, peer.addr),
                    (Some(true), SocketAddr::V6(_)) | (Some(false), SocketAddr::V4(_)) | (None, _)
                )
            })
            .map(|peer| peer.addr)
            .collect();
        let empty = entries.is_empty();
        if empty {
            peers.remove(&hash);
        }
        result
    }

    fn add_peer(&self, hash: Id20, addr: SocketAddr) {
        let private_hashes = self.private_hashes.lock();
        if private_hashes.contains(&hash) {
            return;
        }
        let mut peers = self.peers.lock();
        let entries = peers.entry(hash).or_default();
        if let Some(existing) = entries.iter_mut().find(|peer| peer.addr == addr) {
            existing.seen = Instant::now();
            return;
        }
        let same_ip: Vec<usize> = entries
            .iter()
            .enumerate()
            .filter(|(_, peer)| peer.addr.ip() == addr.ip())
            .map(|(idx, _)| idx)
            .collect();
        if same_ip.len() >= MAX_PEERS_PER_IP {
            if let Some(idx) = same_ip.into_iter().min_by_key(|idx| entries[*idx].seen) {
                entries.remove(idx);
            }
        }
        if entries.len() >= MAX_PEERS_PER_HASH {
            if let Some(idx) = entries
                .iter()
                .enumerate()
                .min_by_key(|(_, peer)| peer.seen)
                .map(|(idx, _)| idx)
            {
                entries.remove(idx);
            }
        }
        entries.push(StoredPeer {
            addr,
            seen: Instant::now(),
        });
        while peers.values().map(Vec::len).sum::<usize>() > MAX_STORED_PEERS {
            let oldest = peers
                .iter()
                .flat_map(|(hash, entries)| {
                    entries
                        .iter()
                        .enumerate()
                        .map(move |(idx, peer)| (*hash, idx, peer.seen))
                })
                .min_by_key(|(_, _, seen)| *seen);
            if let Some((hash, idx, _)) = oldest {
                if let Some(entries) = peers.get_mut(&hash) {
                    entries.remove(idx);
                    if entries.is_empty() {
                        peers.remove(&hash);
                    }
                }
            } else {
                break;
            }
        }
        drop(private_hashes);
    }

    fn set_private(&self, hash: Id20, private: bool) {
        let mut hashes = self.private_hashes.lock();
        if private {
            hashes.insert(hash);
            self.peers.lock().remove(&hash);
        } else {
            hashes.remove(&hash);
        }
    }

    fn is_private(&self, hash: &Id20) -> bool {
        self.private_hashes.lock().contains(hash)
    }

    fn add_routing(&self, id: Id20, addr: SocketAddr) {
        if public_dht_endpoint(addr) {
            self.table(addr.is_ipv6()).lock().add_with_liveness(
                id,
                addr,
                RoutingLiveness::Questionable,
            );
        }
    }

    fn closest_nodes(
        &self,
        target: &Id20,
        n: usize,
        want4: bool,
        want6: bool,
    ) -> Vec<(Id20, SocketAddr)> {
        let mut nodes = if want4 {
            self.routing.lock().closest_good_nodes(target, n)
        } else {
            Vec::new()
        };
        if want6 {
            nodes.extend(self.routing6.lock().closest_good_nodes(target, n));
        }
        nodes
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DhtOptions {
    pub port: Option<u16>,
    pub routing_state_path: Option<PathBuf>,
}

#[derive(Default)]
struct SharedDhtState {
    dht: Option<Arc<Dht>>,
    proxy_requested: Option<bool>,
    options: DhtOptions,
}

static SHARED_DHT: std::sync::OnceLock<tokio::sync::Mutex<SharedDhtState>> =
    std::sync::OnceLock::new();

fn shared_dht_cell() -> &'static tokio::sync::Mutex<SharedDhtState> {
    SHARED_DHT.get_or_init(|| tokio::sync::Mutex::new(SharedDhtState::default()))
}

fn shared_dht_install_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

pub const DEFAULT_BOOTSTRAP: &[&str] = &[
    "router.bittorrent.com:6881",
    "router.utorrent.com:6881",
    "dht.transmissionbt.com:6881",
    "dht.libtorrent.org:25401",
    "router.bitcomet.com:6881",
];

fn bootstrap_targets() -> Vec<DhtTarget> {
    DEFAULT_BOOTSTRAP
        .iter()
        .filter_map(|raw| {
            let (host, port) = raw.rsplit_once(':')?;
            let port = port.parse().ok()?;
            Some(DhtTarget::Host(
                host.trim_matches(['[', ']']).to_string(),
                port,
            ))
        })
        .collect()
}

fn target_is_v6(target: &DhtTarget) -> bool {
    matches!(target, DhtTarget::Addr(addr) if addr.is_ipv6())
}

struct LookupCandidate {
    target: DhtTarget,
    verified: bool,
}

fn target_id_verified(id: &Id20, target: &DhtTarget) -> bool {
    matches!(target, DhtTarget::Addr(addr) if node_id_valid(id, addr.ip()))
}

fn next_dispatch(
    shortlist: &BTreeMap<Id20, LookupCandidate>,
    queried: &mut HashSet<DhtTarget>,
) -> Vec<DhtTarget> {
    let (mut window4, mut window6) = (0usize, 0usize);
    let mut window = Vec::new();
    for candidate in shortlist.values() {
        let used = if target_is_v6(&candidate.target) {
            &mut window6
        } else {
            &mut window4
        };
        if *used >= K * 2 {
            continue;
        }
        *used += 1;
        window.push(candidate);
    }
    window.sort_by_key(|candidate| !candidate.verified);
    let mut chosen = Vec::new();
    for candidate in window {
        if queried.insert(candidate.target.clone()) {
            chosen.push(candidate.target.clone());
            if chosen.len() >= ALPHA {
                break;
            }
        }
    }
    chosen
}

#[derive(Clone, Debug)]
struct AnnounceTarget {
    target: DhtTarget,
    token: Vec<u8>,
    verified: bool,
}

type AnnounceTargets = BTreeMap<(Id20, bool), AnnounceTarget>;

fn record_announce_target(
    targets: &mut AnnounceTargets,
    distance: Id20,
    from: DhtTarget,
    token: Vec<u8>,
    verified: bool,
) {
    let v6 = target_is_v6(&from);
    targets.insert(
        (distance, v6),
        AnnounceTarget {
            target: from,
            token,
            verified,
        },
    );
    let same_family: Vec<(Id20, bool)> = targets
        .keys()
        .filter(|(_, family)| *family == v6)
        .copied()
        .collect();
    if same_family.len() > K * 2 {
        if let Some(farthest) = same_family.last() {
            targets.remove(farthest);
        }
    }
}

fn select_announce_targets(targets: AnnounceTargets, allow_v6: bool) -> Vec<(DhtTarget, Vec<u8>)> {
    let (mut v4, mut v6) = (Vec::new(), Vec::new());
    for ((_, is_v6), entry) in targets {
        if !is_v6 {
            v4.push(entry);
        } else if allow_v6 {
            v6.push(entry);
        }
    }
    [v4, v6]
        .into_iter()
        .flat_map(|mut family| {
            family.sort_by_key(|entry| !entry.verified);
            family.into_iter().take(K)
        })
        .map(|entry| (entry.target, entry.token))
        .collect()
}

fn dht_targets_match(expected: &DhtTarget, actual: &DhtTarget) -> bool {
    match (expected, actual) {
        (DhtTarget::Addr(expected), DhtTarget::Addr(actual)) => expected == actual,
        (
            DhtTarget::Host(expected_host, expected_port),
            DhtTarget::Host(actual_host, actual_port),
        ) => expected_port == actual_port && expected_host.eq_ignore_ascii_case(actual_host),
        (DhtTarget::Host(..), DhtTarget::Addr(..)) => true,
        _ => false,
    }
}

pub struct Dht {
    sock: Option<Arc<UdpSocket>>,
    sock6: Option<Arc<UdpSocket>>,
    proxy_datagram: Option<Arc<ProxyAssociation>>,
    external: ExternalIp,
    pending: Arc<Mutex<PendingMap>>,
    routing: Arc<Mutex<RoutingTable>>,
    routing6: Arc<Mutex<RoutingTable>>,
    bootstrap_hosts: Mutex<Vec<DhtTarget>>,
    router_addrs: Mutex<HashSet<SocketAddr>>,
    port_pinged: Mutex<HashMap<SocketAddr, Instant>>,
    server: Arc<InboundDhtState>,
    reader_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    reader6_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    lookup_handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    refresh_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    identity_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    routing_state_path: Option<PathBuf>,
    saved_ids: SavedIds,
    shutdown: AtomicBool,
    announce_v6: AtomicBool,
    v6_unreachable_since: Mutex<Option<Instant>>,
    regeneration_interval: Duration,
}

#[derive(Debug, Default)]
struct ExternalFollow {
    regenerated_v4: bool,
    regenerated_v6: bool,
    retry_after: Option<Duration>,
}

async fn follow_external_addresses(
    dht: Weak<Dht>,
    mut updates: watch::Receiver<ExternalAddresses>,
) {
    loop {
        let addresses = *updates.borrow_and_update();
        let retry_after = {
            let Some(dht) = dht.upgrade() else {
                return;
            };
            if dht.shutdown.load(Ordering::Acquire) {
                return;
            }
            let outcome = dht.follow_external(addresses, Instant::now());
            for (v6, regenerated) in [
                (false, outcome.regenerated_v4),
                (true, outcome.regenerated_v6 && dht.ipv6_usable()),
            ] {
                if !regenerated {
                    continue;
                }
                tracing::debug!(
                    v6,
                    "DHT node id regenerated for the elected external address"
                );
                let dht = dht.clone();
                tokio::spawn(async move { dht.bootstrap_for(dht.node_id(v6)).await });
            }
            outcome.retry_after
        };
        let changed = match retry_after {
            Some(wait) => tokio::time::timeout(wait, updates.changed())
                .await
                .unwrap_or(Ok(())),
            None => updates.changed().await,
        };
        if changed.is_err() {
            return;
        }
    }
}

pub struct DhtRouteSwap {
    previous: Option<Arc<Dht>>,
    previous_proxy_requested: Option<bool>,
    previous_options: DhtOptions,
    next: Option<Arc<Dht>>,
}

impl DhtRouteSwap {
    pub fn next(&self) -> Option<Arc<Dht>> {
        self.next.clone()
    }

    pub async fn commit(self) {
        let owns_current = {
            let guard = shared_dht_cell().lock().await;
            match (&guard.dht, &self.next) {
                (None, None) => true,
                (Some(current), Some(next)) => Arc::ptr_eq(current, next),
                _ => false,
            }
        };
        if owns_current {
            if let Some(previous) = self.previous {
                let kept = self
                    .next
                    .as_ref()
                    .is_some_and(|next| Arc::ptr_eq(next, &previous));
                if !kept {
                    previous.shutdown().await;
                }
            }
        } else {
            tracing::debug!("ignoring stale DHT route commit");
            if let Some(next) = self.next {
                next.shutdown().await;
            }
        }
    }

    pub async fn rollback(self) -> Option<Arc<Dht>> {
        let restored = {
            let mut guard = shared_dht_cell().lock().await;
            let owns_current = match (&guard.dht, &self.next) {
                (None, None) => true,
                (Some(current), Some(next)) => Arc::ptr_eq(current, next),
                _ => false,
            };
            if !owns_current {
                tracing::debug!("ignoring stale DHT route rollback");
                let current = guard.dht.clone();
                drop(guard);
                if let Some(next) = self.next {
                    next.shutdown().await;
                }
                return current;
            }
            let current = guard.dht.take();
            guard.dht = self.previous.clone();
            guard.proxy_requested = self.previous_proxy_requested;
            guard.options = self.previous_options.clone();
            (current, guard.dht.clone())
        };

        if let Some(current) = restored.0 {
            if self
                .previous
                .as_ref()
                .is_none_or(|previous| !Arc::ptr_eq(previous, &current))
            {
                current.shutdown().await;
            }
        }
        restored.1
    }
}

impl std::fmt::Debug for Dht {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dht").finish_non_exhaustive()
    }
}

impl Drop for Dht {
    fn drop(&mut self) {
        if !self.shutdown.swap(true, Ordering::AcqRel) {
            self.persist_configured_routing_state();
        }
        self.stop_tasks();
    }
}

impl Dht {
    fn stop_tasks(&self) {
        self.abort_lookups();
        for slot in [
            &self.reader_handle,
            &self.reader6_handle,
            &self.refresh_handle,
            &self.identity_handle,
        ] {
            if let Some(handle) = slot.lock().take() {
                handle.abort();
            }
        }
    }

    fn abort_lookups(&self) {
        let handles = std::mem::take(&mut *self.lookup_handles.lock());
        for handle in handles {
            handle.abort();
        }
    }

    pub fn cancel_lookups(&self) {
        self.abort_lookups();
    }

    pub async fn persist_routing_state(&self) {
        let Some(path) = self.routing_state_path.clone() else {
            return;
        };
        let routes = self.routing_snapshot();
        if routes.is_empty() {
            return;
        }
        let ids = self.persisted_ids();
        let log_path = path.clone();
        match tokio::task::spawn_blocking(move || save_routing_state_file(&path, &routes, &ids))
            .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => {
                tracing::warn!(path = %log_path.display(), "failed to persist DHT routing state: {error}");
            }
            Err(error) => {
                tracing::warn!(path = %log_path.display(), "DHT routing state persistence task failed: {error}");
            }
        }
    }

    pub async fn shutdown(&self) {
        self.persist_routing_state().await;
        self.shutdown.store(true, Ordering::Release);
        self.stop_tasks();
    }
}

type PendingMap = std::collections::HashMap<Vec<u8>, PendingEntry>;

#[derive(Clone)]
struct PendingToken(Arc<()>);

impl PendingToken {
    fn new() -> Self {
        Self(Arc::new(()))
    }

    fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

struct PendingEntry {
    tx: oneshot::Sender<KrpcResponse>,
    target: DhtTarget,
    resolved_addrs: Option<Vec<SocketAddr>>,
    token: PendingToken,
}

struct PendingGuard {
    pending: Arc<Mutex<PendingMap>>,
    txn: Vec<u8>,
    token: PendingToken,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        let mut pending = self.pending.lock();
        let should_remove = pending
            .get(&self.txn)
            .is_some_and(|entry| entry.token.matches(&self.token));
        if should_remove {
            pending.remove(&self.txn);
        }
    }
}

struct KrpcResponse {
    from: DhtTarget,
    body: Value,
}

impl Dht {
    async fn send_packet(&self, packet: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "DHT runtime was reconfigured",
            ));
        }
        if let Some(proxy) = &self.proxy_datagram {
            return proxy
                .send_to(packet, target)
                .await
                .map_err(|error| std::io::Error::other(error.to_string()));
        }
        match target {
            SocketAddr::V4(_) => {
                self.sock
                    .as_ref()
                    .ok_or_else(|| std::io::Error::other("DHT IPv4 socket unavailable"))?
                    .send_to(packet, target)
                    .await
            }
            SocketAddr::V6(_) => match &self.sock6 {
                Some(socket) => {
                    let result = socket.send_to(packet, target).await;
                    self.note_v6_send(&result);
                    result
                }
                None => Err(std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "IPv6 DHT socket unavailable",
                )),
            },
        }
    }

    fn note_v6_send(&self, result: &std::io::Result<usize>) {
        use std::io::ErrorKind::{AddrNotAvailable, HostUnreachable, NetworkUnreachable};
        match result {
            Ok(_) => *self.v6_unreachable_since.lock() = None,
            Err(e)
                if matches!(
                    e.kind(),
                    NetworkUnreachable | HostUnreachable | AddrNotAvailable
                ) =>
            {
                *self.v6_unreachable_since.lock() = Some(Instant::now());
            }
            Err(_) => {}
        }
    }

    fn ipv6_usable(&self) -> bool {
        if self.proxy_datagram.is_some() {
            return true;
        }
        if self.sock6.is_none() {
            return false;
        }
        match *self.v6_unreachable_since.lock() {
            Some(since) => since.elapsed() >= V6_RETRY_AFTER,
            None => true,
        }
    }

    async fn send_packet_host(
        &self,
        packet: &[u8],
        host: &str,
        port: u16,
    ) -> std::io::Result<usize> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "DHT runtime was reconfigured",
            ));
        }
        if let Some(proxy) = &self.proxy_datagram {
            if !proxy_host_allowed(host) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("host {host} is not a public DHT router"),
                ));
            }
            return proxy
                .send_to_host(packet, host, port)
                .await
                .map_err(|error| std::io::Error::other(error.to_string()));
        }
        let targets = lookup_host((host, port))
            .await?
            .filter(|target| public_dht_endpoint(*target))
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("no public addresses for {host}"),
            ));
        }

        let mut last_error = None;
        for target in targets {
            let result = match target {
                SocketAddr::V4(_) => {
                    self.sock
                        .as_ref()
                        .ok_or_else(|| std::io::Error::other("DHT IPv4 socket unavailable"))?
                        .send_to(packet, target)
                        .await
                }
                SocketAddr::V6(_) => match &self.sock6 {
                    Some(socket) => {
                        let result = socket.send_to(packet, target).await;
                        self.note_v6_send(&result);
                        result
                    }
                    None => Err(std::io::Error::new(
                        std::io::ErrorKind::AddrNotAvailable,
                        "IPv6 DHT socket unavailable",
                    )),
                },
            };
            match result {
                Ok(n) => return Ok(n),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.unwrap_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("no addresses for {host}"),
            )
        }))
    }

    async fn send_target(&self, packet: &[u8], target: &DhtTarget) -> std::io::Result<usize> {
        match target {
            DhtTarget::Addr(addr) => self.send_packet(packet, *addr).await,
            DhtTarget::Host(host, port) => self.send_packet_host(packet, host, *port).await,
        }
    }

    pub async fn current_shared() -> Option<Arc<Dht>> {
        shared_dht_cell().lock().await.dht.clone()
    }

    #[cfg(test)]
    pub async fn shared() -> Option<Arc<Dht>> {
        if let Some(dht) = Self::current_shared().await {
            return Some(dht);
        }

        if shared_dht_cell().lock().await.proxy_requested == Some(true) {
            return None;
        }
        Self::shared_with_proxy(None, DhtOptions::default()).await
    }

    #[cfg(test)]
    pub async fn replace_shared_with_proxy(
        proxy: Option<risuko_http::ProxyConnector>,
    ) -> Option<Arc<Dht>> {
        let swap = Self::prepare_shared_with_proxy(proxy, DhtOptions::default())
            .await
            .ok()?;
        let next = swap.next();
        swap.commit().await;
        next
    }

    pub async fn prepare_shared_with_proxy(
        proxy: Option<risuko_http::ProxyConnector>,
        options: DhtOptions,
    ) -> Result<DhtRouteSwap, String> {
        let _install_guard = shared_dht_install_lock().lock().await;
        let requested_proxy = proxy.is_some();
        let (previous, previous_proxy_requested, previous_options) = {
            let mut guard = shared_dht_cell().lock().await;
            let snapshot = (
                guard.dht.clone(),
                guard.proxy_requested,
                guard.options.clone(),
            );
            guard.proxy_requested = Some(requested_proxy);
            snapshot
        };
        let keeps_route = !requested_proxy
            && previous_proxy_requested == Some(false)
            && previous_options == options
            && previous
                .as_ref()
                .is_some_and(|dht| !dht.shutdown.load(Ordering::Acquire));
        if keeps_route {
            return Ok(DhtRouteSwap {
                next: previous.clone(),
                previous,
                previous_proxy_requested,
                previous_options,
            });
        }
        let next = match Self::spawn_warm(proxy, options.clone(), previous.as_ref()).await {
            Ok(dht) => Some(dht),
            Err(error) => {
                let mut guard = shared_dht_cell().lock().await;
                guard.dht = previous.clone();
                guard.proxy_requested = previous_proxy_requested.or(Some(requested_proxy));
                return Err(error.to_string());
            }
        };
        {
            let mut guard = shared_dht_cell().lock().await;
            guard.dht = next.clone();
            guard.options = options;
        }

        if let Some(previous) = previous.as_ref() {
            previous.cancel_lookups();
        }

        Ok(DhtRouteSwap {
            previous,
            previous_proxy_requested,
            previous_options,
            next,
        })
    }

    async fn spawn_warm(
        proxy: Option<risuko_http::ProxyConnector>,
        options: DhtOptions,
        previous: Option<&Arc<Dht>>,
    ) -> std::io::Result<Arc<Dht>> {
        let preserved = previous
            .map(|dht| dht.routing_snapshot())
            .unwrap_or_default();
        let dht = Dht::spawn_with_options(proxy, options).await?;
        dht.add_bootstrap_nodes(preserved);
        let warm = dht.clone();
        tokio::spawn(async move { warm.bootstrap().await });
        Ok(dht)
    }

    pub async fn shared_with_proxy(
        proxy: Option<risuko_http::ProxyConnector>,
        options: DhtOptions,
    ) -> Option<Arc<Dht>> {
        let _install_guard = shared_dht_install_lock().lock().await;
        let requested_proxy = proxy.is_some();
        let (previous, previous_proxy_requested) = {
            let guard = shared_dht_cell().lock().await;
            if guard.proxy_requested == Some(requested_proxy)
                && guard.options == options
                && guard
                    .dht
                    .as_ref()
                    .is_some_and(|dht| !dht.shutdown.load(Ordering::Acquire))
            {
                return guard.dht.clone();
            }
            if guard.proxy_requested == Some(true) && !requested_proxy && guard.dht.is_none() {
                return None;
            }
            let previous = guard.dht.clone();
            let previous_proxy_requested = guard.proxy_requested;
            (previous, previous_proxy_requested)
        };
        let next = match Self::spawn_warm(proxy, options.clone(), previous.as_ref()).await {
            Ok(dht) => Some(dht),
            Err(error) => {
                let mut guard = shared_dht_cell().lock().await;
                guard.dht = previous.clone();
                guard.proxy_requested = previous_proxy_requested.or(Some(requested_proxy));
                tracing::warn!(
                    proxied = requested_proxy,
                    "DHT runtime initialization failed: {error}"
                );
                None
            }
        };
        if next.is_some() {
            let mut guard = shared_dht_cell().lock().await;
            guard.proxy_requested = Some(requested_proxy);
            guard.options = options;
            guard.dht = next.clone();
        }

        if next.is_some() {
            if let Some(previous) = previous {
                previous.cancel_lookups();
                previous.shutdown().await;
            }
        }
        next
    }

    #[cfg(test)]
    pub async fn spawn() -> std::io::Result<Arc<Self>> {
        Self::spawn_with_options(None, DhtOptions::default()).await
    }

    #[cfg(test)]
    pub async fn spawn_with_routing_state(
        state_path: impl Into<PathBuf>,
    ) -> std::io::Result<Arc<Self>> {
        let options = DhtOptions {
            routing_state_path: Some(state_path.into()),
            ..Default::default()
        };
        Self::spawn_with_options(None, options).await
    }

    pub(crate) async fn spawn_with_options(
        proxy: Option<risuko_http::ProxyConnector>,
        options: DhtOptions,
    ) -> std::io::Result<Arc<Self>> {
        Self::spawn_with_external(
            proxy,
            options,
            ExternalIp::shared().clone(),
            node_id::MIN_REGENERATION_INTERVAL,
        )
        .await
    }

    async fn spawn_with_external(
        proxy: Option<risuko_http::ProxyConnector>,
        options: DhtOptions,
        external: ExternalIp,
        regeneration_interval: Duration,
    ) -> std::io::Result<Arc<Self>> {
        let DhtOptions {
            port,
            routing_state_path,
        } = options;
        let proxy_datagram = match proxy {
            Some(connector) => {
                let has_explicit_bypass = connector
                    .udp_no_proxy()
                    .is_some_and(|matcher| !matcher.is_empty());
                let result = if has_explicit_bypass {
                    connector.bind_udp_with_bypass().await
                } else {
                    connector.bind_udp().await
                };
                let datagram = result.map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::Unsupported, error.to_string())
                })?;
                Some(Arc::new(ProxyAssociation::new(
                    connector,
                    has_explicit_bypass,
                    datagram,
                )))
            }
            None => None,
        };

        let (sock, sock6) = if proxy_datagram.is_none() {
            let (sock, sock6) = bind_direct_sockets(port).await?;
            (Some(sock), sock6)
        } else {
            (None, None)
        };
        let pending: Arc<Mutex<PendingMap>> = Arc::new(Mutex::new(Default::default()));

        let mut loaded = RoutingState::default();
        if let Some(path) = routing_state_path.clone() {
            let log_path = path.clone();
            match tokio::task::spawn_blocking(move || load_routing_state_file(&path)).await {
                Ok(Ok(state)) => loaded = state,
                Ok(Err(error)) => {
                    tracing::warn!(path = %log_path.display(), "failed to load DHT routing state: {error}");
                }
                Err(error) => {
                    tracing::warn!(path = %log_path.display(), "DHT routing state load task failed: {error}");
                }
            }
        }
        let proxied = proxy_datagram.is_some();
        let elected = external.addresses();
        let fallback_id = random_id();
        let routing = Arc::new(Mutex::new(RoutingTable::new(initial_node_id(
            loaded.ids.v4,
            elected.v4.map(IpAddr::V4),
            proxied,
            fallback_id,
        ))));
        let routing6 = Arc::new(Mutex::new(RoutingTable::new(initial_node_id(
            loaded.ids.v6,
            elected.v6.map(IpAddr::V6),
            proxied,
            fallback_id,
        ))));
        for (id, addr) in loaded.contacts {
            let table = if addr.is_ipv6() { &routing6 } else { &routing };
            table
                .lock()
                .add_with_liveness(id, addr, RoutingLiveness::Questionable);
        }
        let server = Arc::new(InboundDhtState::with_routing6(
            routing.clone(),
            routing6.clone(),
        ));
        let this = Arc::new(Self {
            sock: sock.clone(),
            sock6: sock6.clone(),
            proxy_datagram: proxy_datagram.clone(),
            external: external.clone(),
            pending: pending.clone(),
            routing,
            routing6,
            bootstrap_hosts: Mutex::new(Vec::new()),
            router_addrs: Mutex::new(HashSet::new()),
            port_pinged: Mutex::new(HashMap::new()),
            server: server.clone(),
            reader_handle: Mutex::new(None),
            reader6_handle: Mutex::new(None),
            lookup_handles: Mutex::new(Vec::new()),
            refresh_handle: Mutex::new(None),
            identity_handle: Mutex::new(None),
            routing_state_path,
            saved_ids: loaded.ids,
            shutdown: AtomicBool::new(false),
            announce_v6: AtomicBool::new(false),
            v6_unreachable_since: Mutex::new(None),
            regeneration_interval,
        });

        let reader_sock = sock.clone();
        let pending_reader = pending.clone();
        let reader_handle = if let Some(datagram) = proxy_datagram.clone() {
            tokio::spawn(async move { proxy_reader_loop(datagram, pending_reader).await })
        } else {
            let server_reader = server.clone();
            let external = external.clone();
            tokio::spawn(async move {
                reader_loop(
                    reader_sock.expect("direct DHT socket"),
                    pending_reader,
                    server_reader,
                    external,
                )
                .await
            })
        };
        *this.reader_handle.lock() = Some(reader_handle);

        if proxy_datagram.is_none() {
            if let Some(s6) = sock6 {
                let pending6 = pending.clone();
                let server6 = server.clone();
                let external6 = external.clone();
                let reader6_handle =
                    tokio::spawn(
                        async move { reader_loop(s6, pending6, server6, external6).await },
                    );
                *this.reader6_handle.lock() = Some(reader6_handle);
            }
        }

        let weak = Arc::downgrade(&this);
        let refresh_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(ROUTING_REFRESH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            let mut last_persist = tokio::time::Instant::now();
            loop {
                interval.tick().await;
                let Some(dht) = weak.upgrade() else {
                    return;
                };
                if dht.shutdown.load(Ordering::Acquire) {
                    return;
                }
                if last_persist.elapsed() >= ROUTING_PERSIST_INTERVAL {
                    last_persist = tokio::time::Instant::now();
                    dht.persist_routing_state().await;
                }
                if dht.proxy_datagram.is_some() {
                    continue;
                }
                let refresh_target = dht.next_refresh_target();
                let mut stale = dht.refresh_routing(REFRESH_PINGS_PER_TICK).into_iter();
                let mut pings: JoinSet<()> = JoinSet::new();
                loop {
                    while pings.len() < REFRESH_PINGS_IN_FLIGHT {
                        let Some((id, addr)) = stale.next() else {
                            break;
                        };
                        let dht = dht.clone();
                        pings.spawn(async move {
                            dht.ping_node(addr, Some(id)).await;
                        });
                    }
                    if pings.join_next().await.is_none() {
                        break;
                    }
                }
                if let Some(target) = refresh_target {
                    dht.bootstrap_for(target).await;
                }
            }
        });
        *this.refresh_handle.lock() = Some(refresh_handle);

        if !proxied {
            let identity_handle = tokio::spawn(follow_external_addresses(
                Arc::downgrade(&this),
                external.subscribe(),
            ));
            *this.identity_handle.lock() = Some(identity_handle);
        }

        tracing::debug!(
            "DHT started: id={}, bootstrap={} nodes, ipv6={}",
            this.node_id(false),
            DEFAULT_BOOTSTRAP.len(),
            this.sock6.is_some(),
        );
        Ok(this)
    }

    fn table(&self, v6: bool) -> &Mutex<RoutingTable> {
        if v6 {
            &self.routing6
        } else {
            &self.routing
        }
    }

    fn node_id(&self, v6: bool) -> Id20 {
        self.table(v6).lock().our_id
    }

    fn node_id_for(&self, target: &DhtTarget) -> Id20 {
        self.node_id(target_is_v6(target))
    }

    pub fn node_id_compliant(&self) -> Option<bool> {
        if self.proxy_datagram.is_some() {
            return None;
        }
        let elected = self.external.external_v4()?;
        Some(node_id_valid(&self.node_id(false), IpAddr::V4(elected)))
    }

    fn follow_external(&self, addresses: ExternalAddresses, now: Instant) -> ExternalFollow {
        let proxied = self.proxy_datagram.is_some();
        let mut outcome = ExternalFollow::default();
        for (v6, elected) in [
            (false, addresses.v4.map(IpAddr::V4)),
            (true, addresses.v6.map(IpAddr::V6)),
        ] {
            let plan = self.table(v6).lock().follow_external_address(
                elected,
                proxied,
                self.regeneration_interval,
                now,
            );
            match plan {
                IdPlan::Keep => {}
                IdPlan::Replace(_) if v6 => outcome.regenerated_v6 = true,
                IdPlan::Replace(_) => outcome.regenerated_v4 = true,
                IdPlan::Wait(wait) => {
                    outcome.retry_after = Some(outcome.retry_after.map_or(wait, |w| w.min(wait)));
                }
            }
        }
        outcome
    }

    pub fn get_peers_stream(
        self: &Arc<Self>,
        info_hash: Id20,
        budget: Duration,
        announce_port: Option<u16>,
    ) -> mpsc::UnboundedReceiver<SocketAddr> {
        self.get_peers_announced(info_hash, budget, announce_port).0
    }

    pub fn get_peers_announced(
        self: &Arc<Self>,
        info_hash: Id20,
        budget: Duration,
        announce_port: Option<u16>,
    ) -> (mpsc::UnboundedReceiver<SocketAddr>, oneshot::Receiver<bool>) {
        let (tx, rx) = mpsc::unbounded_channel::<SocketAddr>();
        let (done_tx, done_rx) = oneshot::channel();
        if self.server.is_private(&info_hash) {
            return (rx, done_rx);
        }
        let this = self.clone();
        let closed = tx.clone();
        let handle = tokio::spawn(async move {
            tokio::select! {
                finished = tokio::time::timeout(
                    budget,
                    this.iterative_get_peers(info_hash, tx, announce_port),
                ) => {
                    let _ = done_tx.send(finished.unwrap_or(false));
                }
                _ = closed.closed() => {}
            }
        });
        let mut handles = self.lookup_handles.lock();
        handles.retain(|handle| !handle.is_finished());
        if self.shutdown.load(Ordering::Acquire) {
            handle.abort();
        } else {
            handles.push(handle);
        }
        (rx, done_rx)
    }

    async fn iterative_get_peers(
        self: Arc<Self>,
        info_hash: Id20,
        peer_tx: mpsc::UnboundedSender<SocketAddr>,
        announce_port: Option<u16>,
    ) -> bool {
        if self.server.is_private(&info_hash) {
            return false;
        }
        let mut seed_seen = HashSet::new();
        let mut targets: Vec<DhtTarget> = self
            .closest_routing(&info_hash, K * 2)
            .into_iter()
            .map(DhtTarget::Addr)
            .filter(|target| seed_seen.insert(target.clone()))
            .collect();
        let mut used_bootstrap = targets.len() < K;
        if used_bootstrap {
            let hosts = self.bootstrap_hosts.lock().clone();
            targets.extend(
                bootstrap_targets()
                    .into_iter()
                    .chain(hosts)
                    .filter(|target| seed_seen.insert(target.clone())),
            );
        }
        if targets.is_empty() {
            tracing::debug!("dht: no bootstrap nodes available");
            return false;
        }

        let mut shortlist: BTreeMap<Id20, LookupCandidate> = BTreeMap::new();
        let mut queried: HashSet<DhtTarget> = HashSet::new();
        let mut peers_seen: HashSet<SocketAddr> = HashSet::new();
        let mut announce_targets: AnnounceTargets = BTreeMap::new();

        targets.truncate(MAX_ROUND_QUERIES);
        queried.extend(targets.iter().cloned());

        let mut futs: JoinSet<(DhtTarget, Option<GetPeersReply>)> = JoinSet::new();
        for target in targets {
            if self.server.is_private(&info_hash) {
                return false;
            }
            let this = self.clone();
            futs.spawn(async move {
                let reply = this.query_get_peers(target.clone(), info_hash).await;
                (target, reply)
            });
        }

        let mut total_peers = 0usize;
        let mut total_nodes = 0usize;
        let mut rounds_without_progress = 0usize;

        loop {
            if self.server.is_private(&info_hash) {
                return false;
            }
            let Some(joined) = futs.join_next().await else {
                break;
            };
            let (failed_target, res) = match joined {
                Ok((target, res)) => (Some(target), res),
                Err(_) => (None, None),
            };
            let ipv6_ok = self.ipv6_usable();

            if let Some((from, responder_id, peers, nodes, token)) = res {
                let mut new_peers = 0usize;
                for p in peers {
                    if peers_seen.insert(p) {
                        new_peers += 1;
                        if peer_tx.send(p).is_err() {
                            return false;
                        }
                    }
                }
                total_peers += new_peers;

                let node_id = responder_id.unwrap_or_else(|| pseudo_id_target(&from));
                let responder_public = match &from {
                    DhtTarget::Addr(addr) => public_dht_endpoint(*addr),
                    DhtTarget::Host(_, _) => true,
                };
                let is_router = matches!(&from, DhtTarget::Addr(addr) if self.router_addrs.lock().contains(addr));
                let responder_verified =
                    responder_id.is_some() && target_id_verified(&node_id, &from);
                if responder_public && !is_router {
                    shortlist.insert(
                        node_id.distance(&info_hash),
                        LookupCandidate {
                            target: from.clone(),
                            verified: responder_verified,
                        },
                    );
                    if responder_id.is_some() {
                        if let DhtTarget::Addr(from) = &from {
                            self.add_routing(node_id, *from);
                        }
                    }
                }
                if responder_public {
                    if let Some(tok) = token {
                        record_announce_target(
                            &mut announce_targets,
                            node_id.distance(&info_hash),
                            from,
                            tok,
                            responder_verified,
                        );
                    }
                }

                let mut progressed = false;
                for (nid, naddr) in &nodes {
                    if !public_dht_endpoint(*naddr) || (naddr.is_ipv6() && !ipv6_ok) {
                        continue;
                    }
                    total_nodes += 1;
                    let d = nid.distance(&info_hash);
                    if let std::collections::btree_map::Entry::Vacant(e) = shortlist.entry(d) {
                        e.insert(LookupCandidate {
                            target: DhtTarget::Addr(*naddr),
                            verified: node_id_valid(nid, naddr.ip()),
                        });
                        progressed = true;
                    }
                    self.add_routing_questionable(*nid, *naddr);
                }

                while shortlist.len() > K * 4 {
                    shortlist.pop_last();
                }

                if progressed || new_peers > 0 {
                    rounds_without_progress = 0;
                } else {
                    rounds_without_progress += 1;
                }
            } else if let Some(target) = failed_target {
                shortlist.retain(|_, candidate| candidate.target != target);
            }

            let to_dispatch = next_dispatch(&shortlist, &mut queried);
            let dispatched = to_dispatch.len();
            for target in to_dispatch {
                if self.server.is_private(&info_hash) {
                    return false;
                }
                let this = self.clone();
                futs.spawn(async move {
                    let reply = this.query_get_peers(target.clone(), info_hash).await;
                    (target, reply)
                });
            }

            if dispatched == 0 && futs.is_empty() && !used_bootstrap && shortlist.is_empty() {
                used_bootstrap = true;
                let fallback: Vec<DhtTarget> = bootstrap_targets()
                    .into_iter()
                    .chain(self.bootstrap_hosts.lock().iter().cloned())
                    .collect();
                for target in fallback {
                    if queried.insert(target.clone()) {
                        let this = self.clone();
                        futs.spawn(async move {
                            let reply = this.query_get_peers(target.clone(), info_hash).await;
                            (target, reply)
                        });
                    }
                }
            }
            if dispatched == 0 && futs.is_empty() {
                break;
            }
            if rounds_without_progress > 20 && peers_seen.len() >= 40 {
                break;
            }
        }

        tracing::debug!(
            "dht get_peers: peers={} nodes_learned={} nodes_queried={}",
            total_peers,
            total_nodes,
            queried.len()
        );

        let mut announced = false;
        if let Some(port) = announce_port.filter(|_| self.proxy_datagram.is_none()) {
            let allow_v6 = self.announce_v6();
            for (addr, token) in select_announce_targets(announce_targets, allow_v6) {
                if self.server.is_private(&info_hash) {
                    return false;
                }
                let txn = random_transaction_id();
                let our_id = self.node_id_for(&addr);
                let pkt = build_announce_peer(&txn, &our_id, &info_hash, port, &token);
                if self.server.is_private(&info_hash) {
                    return false;
                }
                announced |= self.send_target(&pkt, &addr).await.is_ok();
            }
        }
        announced
    }

    async fn query_get_peers(
        self: Arc<Self>,
        target: DhtTarget,
        info_hash: Id20,
    ) -> Option<GetPeersReply> {
        if self.server.is_private(&info_hash) {
            return None;
        }
        let via_host = matches!(target, DhtTarget::Host(..));
        let (target, resolved_addrs) = match target {
            DhtTarget::Host(host, port) if self.proxy_datagram.is_some() => {
                if !proxy_host_allowed(&host) {
                    return None;
                }
                (DhtTarget::Host(host, port), None)
            }
            DhtTarget::Host(host, port) => {
                let addresses =
                    tokio::time::timeout(QUERY_TIMEOUT, lookup_host((host.as_str(), port)))
                        .await
                        .ok()?
                        .ok()?
                        .filter(|address| public_dht_endpoint(*address))
                        .filter(|address| match address {
                            SocketAddr::V4(_) => self.sock.is_some(),
                            SocketAddr::V6(_) => self.ipv6_usable(),
                        })
                        .collect::<Vec<_>>();
                if addresses.is_empty() {
                    return None;
                }
                {
                    let mut routers = self.router_addrs.lock();
                    if routers.len() + addresses.len() <= MAX_ROUTER_ADDRS {
                        routers.extend(addresses.iter().copied());
                    }
                }
                (DhtTarget::Addr(addresses[0]), Some(addresses))
            }
            target => (target, None),
        };
        let (txn, rx, _guard) = self.register_transaction(target.clone(), resolved_addrs);
        let families = want_families(
            match &target {
                DhtTarget::Addr(addr) => Some(addr.is_ipv6()),
                DhtTarget::Host(..) => None,
            },
            self.ipv6_usable(),
            self.routing.lock().live_len_up_to(CROSS_FAMILY_MIN_NODES),
            self.routing6.lock().live_len_up_to(CROSS_FAMILY_MIN_NODES),
        );
        let packet = build_get_peers_bytes(
            &txn,
            &self.node_id_for(&target),
            &info_hash,
            families,
            self.proxy_datagram.is_some(),
        );
        if self.server.is_private(&info_hash) {
            return None;
        }
        let send_res = self.send_target(&packet, &target).await;
        if send_res.is_err() {
            return None;
        }
        let resp = match tokio::time::timeout(QUERY_TIMEOUT, rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => return None,
            Err(_) => {
                if let DhtTarget::Addr(addr) = target {
                    self.record_failure(addr);
                }
                return None;
            }
        };
        if let (true, DhtTarget::Addr(addr)) = (via_host, &resp.from) {
            let mut routers = self.router_addrs.lock();
            if routers.len() < MAX_ROUTER_ADDRS {
                routers.insert(*addr);
            }
        }
        parse_get_peers_response(&resp.body)
            .map(|(rid, peers, nodes, token)| (resp.from, rid, peers, nodes, token))
    }

    async fn ping_node(self: &Arc<Self>, target: SocketAddr, expected_id: Option<Id20>) -> bool {
        if self.proxy_datagram.is_some() {
            return false;
        }
        let target_kind = DhtTarget::Addr(target);
        let (txn, rx, _guard) = self.register_transaction(target_kind.clone(), Some(vec![target]));
        let packet = build_ping_bytes(&txn, &self.node_id(target.is_ipv6()));
        if self.send_target(&packet, &target_kind).await.is_err() {
            return false;
        }
        let response = match tokio::time::timeout(QUERY_TIMEOUT, rx).await {
            Ok(Ok(response)) => response,
            Ok(Err(_)) => return false,
            Err(_) => {
                self.record_failure(target);
                return false;
            }
        };
        let id = response
            .body
            .get(b"r")
            .and_then(|value| value.get(b"id"))
            .and_then(Value::as_bytes)
            .and_then(|bytes| Id20::from_slice(bytes).ok());
        match id {
            Some(id) if expected_id.is_none_or(|expected| expected == id) => {
                self.add_routing(id, target);
                true
            }
            Some(id) => {
                self.add_routing(id, target);
                false
            }
            None => false,
        }
    }

    fn register_transaction(
        &self,
        target: DhtTarget,
        resolved_addrs: Option<Vec<SocketAddr>>,
    ) -> (Vec<u8>, oneshot::Receiver<KrpcResponse>, PendingGuard) {
        let (tx, rx) = oneshot::channel();
        let mut map = self.pending.lock();
        let mut txn = random_transaction_id().to_vec();
        while map.contains_key(&txn) {
            txn = random_transaction_id().to_vec();
        }
        let token = PendingToken::new();
        map.insert(
            txn.clone(),
            PendingEntry {
                tx,
                target,
                resolved_addrs,
                token: token.clone(),
            },
        );
        let guard = PendingGuard {
            pending: self.pending.clone(),
            txn: txn.clone(),
            token,
        };
        (txn, rx, guard)
    }

    #[cfg(test)]
    pub(crate) fn active_lookup_count(&self) -> usize {
        self.lookup_handles
            .lock()
            .iter()
            .filter(|handle| !handle.is_finished())
            .count()
    }

    pub fn routing_table_len(&self) -> usize {
        self.routing.lock().live_len() + self.routing6.lock().live_len()
    }

    fn add_routing(&self, id: Id20, addr: SocketAddr) {
        if valid_routing_addr(addr) {
            self.table(addr.is_ipv6()).lock().add(id, addr);
        }
    }

    fn add_routing_questionable(&self, id: Id20, addr: SocketAddr) {
        if valid_routing_addr(addr) {
            self.table(addr.is_ipv6()).lock().add_with_liveness(
                id,
                addr,
                RoutingLiveness::Questionable,
            );
        }
    }

    fn record_failure(&self, addr: SocketAddr) {
        self.table(addr.is_ipv6()).lock().record_failure(addr);
    }

    fn closest_routing(&self, target: &Id20, n: usize) -> Vec<SocketAddr> {
        let mut nodes = self.routing.lock().closest_nodes(target, n);
        if self.ipv6_usable() {
            nodes.extend(self.routing6.lock().closest_nodes(target, n));
        }
        nodes.sort_by_key(|(id, _)| id.distance(target));
        nodes.truncate(n);
        nodes.into_iter().map(|(_, addr)| addr).collect()
    }

    pub fn routing_snapshot(&self) -> Vec<(Id20, SocketAddr)> {
        let mut routes = self.routing.lock().snapshot();
        routes.extend(self.routing6.lock().snapshot());
        routes
    }

    pub fn save_routing_state(&self, path: impl AsRef<Path>) -> std::io::Result<usize> {
        save_routing_state_file(
            path.as_ref(),
            &self.routing_snapshot(),
            &self.persisted_ids(),
        )
    }

    fn persisted_ids(&self) -> SavedIds {
        if self.proxy_datagram.is_some() {
            return self.saved_ids;
        }
        SavedIds {
            v4: Some(self.node_id(false)),
            v6: Some(self.node_id(true)),
        }
    }

    fn persist_configured_routing_state(&self) {
        let Some(path) = self.routing_state_path.as_deref() else {
            return;
        };
        let routes = self.routing_snapshot();
        if routes.is_empty() {
            return;
        }
        if let Err(error) = save_routing_state_file(path, &routes, &self.persisted_ids()) {
            tracing::warn!(path = %path.display(), "failed to persist DHT routing state: {error}");
        }
    }

    fn next_refresh_target(&self) -> Option<Id20> {
        let now = Instant::now();
        let target = self.routing.lock().stale_refresh_target(now);
        if target.is_some() || !self.ipv6_usable() {
            return target;
        }
        self.routing6.lock().stale_refresh_target(now)
    }

    pub fn refresh_routing(&self, limit: usize) -> Vec<(Id20, SocketAddr)> {
        let now = Instant::now();
        let mut routes = self.routing.lock().refresh_stale(now, limit);
        let remaining = limit.saturating_sub(routes.len());
        routes.extend(self.routing6.lock().refresh_stale(now, remaining));
        routes
    }

    #[cfg(test)]
    pub fn local_port(&self) -> Option<u16> {
        self.sock
            .as_ref()
            .and_then(|socket| socket.local_addr().ok())
            .map(|addr| addr.port())
    }

    pub fn local_port_for(&self, ipv6: bool) -> Option<u16> {
        let socket = if ipv6 {
            self.sock6.as_ref()
        } else {
            self.sock.as_ref()
        }?;
        socket.local_addr().ok().map(|addr| addr.port())
    }

    pub fn set_private(&self, info_hash: Id20, private: bool) {
        self.server.set_private(info_hash, private);
    }

    pub fn set_announce_v6(&self, enabled: bool) {
        self.announce_v6.store(enabled, Ordering::Release);
    }

    pub fn announce_v6(&self) -> bool {
        self.announce_v6.load(Ordering::Acquire)
    }

    pub fn add_bootstrap_nodes<I>(&self, nodes: I)
    where
        I: IntoIterator<Item = (Id20, SocketAddr)>,
    {
        for (id, addr) in nodes {
            self.add_routing_questionable(id, addr);
        }
    }

    pub fn add_bootstrap_hosts<I>(&self, hosts: I)
    where
        I: IntoIterator<Item = (String, u16)>,
    {
        let mut targets = self.bootstrap_hosts.lock();
        for (host, port) in hosts {
            if host.is_empty() || port == 0 {
                continue;
            }
            let target = DhtTarget::Host(host, port);
            if !targets.contains(&target) {
                targets.push(target);
            }
        }
        targets.truncate(MAX_ROUND_QUERIES);
    }

    pub fn add_node(self: &Arc<Self>, addr: SocketAddr) {
        if self.proxy_datagram.is_some()
            || self.shutdown.load(Ordering::Acquire)
            || !public_dht_endpoint(addr)
        {
            return;
        }
        {
            let now = Instant::now();
            let mut seen = self.port_pinged.lock();
            if seen.len() >= MAX_PORT_PINGED {
                seen.retain(|_, at| now.duration_since(*at) < PORT_PING_TTL);
                if seen.len() >= MAX_PORT_PINGED {
                    return;
                }
            }
            match seen.get(&addr) {
                Some(at) if now.duration_since(*at) < PORT_PING_TTL => return,
                _ => {
                    seen.insert(addr, now);
                }
            }
        }
        let this = self.clone();
        tokio::spawn(async move {
            this.ping_node(addr, None).await;
        });
    }

    pub async fn bootstrap(self: &Arc<Self>) {
        self.bootstrap_for(self.node_id(false)).await;
    }

    async fn bootstrap_for(self: &Arc<Self>, target: Id20) {
        let mut rx = self.get_peers_stream(target, Duration::from_secs(15), None);
        while rx.recv().await.is_some() {}
    }
}

fn valid_routing_addr(addr: SocketAddr) -> bool {
    addr.port() != 0
        && !addr.ip().is_unspecified()
        && !addr.ip().is_multicast()
        && !matches!(addr, SocketAddr::V4(v4) if v4.ip().is_broadcast())
        && !matches!(addr, SocketAddr::V6(v6) if v6.ip().to_ipv4_mapped().is_some())
}

fn proxy_host_allowed(host: &str) -> bool {
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    if let Ok(ip) = host.trim_matches(['[', ']']).parse::<IpAddr>() {
        return public_ip(ip);
    }
    if !host.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'.')
    }) {
        return false;
    }
    let mut labels = host.rsplit('.');
    let tld = labels.next().unwrap_or_default();
    if tld.is_empty()
        || tld.starts_with("0x")
        || tld.bytes().all(|byte| byte.is_ascii_digit())
        || !host.contains('.')
        || labels.any(str::is_empty)
    {
        return false;
    }
    !crate::webseed::has_internal_suffix(&host)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct SavedIds {
    v4: Option<Id20>,
    v6: Option<Id20>,
}

#[derive(Debug, Default)]
struct RoutingState {
    contacts: Vec<(Id20, SocketAddr)>,
    ids: SavedIds,
}

const NODE_ID_V4_KEY: &str = "node-id4";
const NODE_ID_V6_KEY: &str = "node-id6";

fn load_routing_state_file(path: &Path) -> std::io::Result<RoutingState> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RoutingState::default())
        }
        Err(error) => return Err(error),
    };
    let mut state = RoutingState::default();
    let mut seen = HashSet::new();
    for line in text
        .lines()
        .skip_while(|line| *line == "risuko-dht-routing-v1")
    {
        let Some((id_hex, addr_text)) = line.split_once('\t') else {
            continue;
        };
        let slot = match id_hex {
            NODE_ID_V4_KEY => Some(&mut state.ids.v4),
            NODE_ID_V6_KEY => Some(&mut state.ids.v6),
            _ => None,
        };
        if let Some(slot) = slot {
            if let Ok(id) = addr_text.parse::<Id20>() {
                *slot = Some(id);
            }
            continue;
        }
        if state.contacts.len() >= MAX_PERSISTED_ROUTES {
            break;
        }
        let Ok(id_bytes) = hex::decode(id_hex) else {
            continue;
        };
        let Ok(id) = Id20::from_slice(&id_bytes) else {
            continue;
        };
        let Ok(addr) = addr_text.parse::<SocketAddr>() else {
            continue;
        };
        if public_dht_endpoint(addr) && seen.insert((id, addr)) {
            state.contacts.push((id, addr));
        }
    }
    Ok(state)
}

fn save_routing_state_file(
    path: &Path,
    contacts: &[(Id20, SocketAddr)],
    ids: &SavedIds,
) -> std::io::Result<usize> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let mut data = String::from("risuko-dht-routing-v1\n");
    for (key, id) in [(NODE_ID_V4_KEY, ids.v4), (NODE_ID_V6_KEY, ids.v6)] {
        if let Some(id) = id {
            data.push_str(key);
            data.push('\t');
            data.push_str(&id.to_hex());
            data.push('\n');
        }
    }
    let mut written = 0usize;
    let mut seen = HashSet::new();
    for (id, addr) in contacts {
        if written >= MAX_PERSISTED_ROUTES {
            break;
        }
        if valid_routing_addr(*addr) && seen.insert((*id, *addr)) {
            data.push_str(&id.to_hex());
            data.push('\t');
            data.push_str(&addr.to_string());
            data.push('\n');
            written += 1;
        }
    }
    static WRITE_LOCK: Mutex<()> = Mutex::new(());
    let _write = WRITE_LOCK.lock();
    let temporary = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&temporary)?;
        use std::io::Write;
        file.write_all(data.as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&temporary, path)?;
    Ok(written)
}

async fn bind_direct_sockets(
    port: Option<u16>,
) -> std::io::Result<(Arc<UdpSocket>, Option<Arc<UdpSocket>>)> {
    let wanted = port.filter(|port| *port != 0);
    let sock = match wanted {
        Some(port) => match UdpSocket::bind((Ipv4Addr::UNSPECIFIED, port)).await {
            Ok(sock) => sock,
            Err(error) => {
                tracing::warn!("dht: udp/{port} unavailable ({error}); using a random port");
                UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?
            }
        },
        None => UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?,
    };
    let ports: &[u16] = match wanted {
        Some(port) => &[port, 0],
        None => &[0],
    };
    let sock6 = match crate::net::bind_first_available("dht: udp6", ports, |port| async move {
        crate::net::bind_udp_v6_only(port)
    })
    .await
    {
        Ok(sock6) => Some(Arc::new(sock6)),
        Err(error) => {
            tracing::debug!("dht: no ipv6 socket: {error}");
            None
        }
    };
    Ok((Arc::new(sock), sock6))
}

const BUCKET_SIZE: usize = K;

const MAX_QUERY_FAILURES: u8 = 2;

const MAX_RESPONSE_PEERS: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoutingLiveness {
    Good,
    Questionable,
    Bad,
}

#[derive(Debug, Clone)]
struct RoutingNode {
    id: Id20,
    addr: SocketAddr,
    last_seen: Instant,
    liveness: RoutingLiveness,
    failures: u8,
    verified: bool,
}

#[derive(Debug)]
struct RoutingBucket {
    prefix: [u8; 20],
    prefix_len: u8,
    nodes: Vec<RoutingNode>,
    last_changed: Instant,
}

impl RoutingBucket {
    fn root() -> Self {
        Self {
            prefix: [0; 20],
            prefix_len: 0,
            nodes: Vec::with_capacity(BUCKET_SIZE),
            last_changed: Instant::now(),
        }
    }

    fn bit(bytes: &[u8; 20], index: u8) -> u8 {
        (bytes[index as usize / 8] >> (7 - (index % 8))) & 1
    }

    fn matches(&self, distance: &Id20) -> bool {
        let bytes = distance.as_bytes();
        (0..self.prefix_len).all(|bit| Self::bit(bytes, bit) == Self::bit(&self.prefix, bit))
    }

    fn contains_ours(&self) -> bool {
        (0..self.prefix_len).all(|bit| Self::bit(&self.prefix, bit) == 0)
    }

    fn child(&self, bit: u8) -> Self {
        let mut prefix = self.prefix;
        let byte = self.prefix_len as usize / 8;
        let shift = 7 - (self.prefix_len % 8);
        if bit == 0 {
            prefix[byte] &= !(1 << shift);
        } else {
            prefix[byte] |= 1 << shift;
        }
        Self {
            prefix,
            prefix_len: self.prefix_len + 1,
            nodes: Vec::with_capacity(BUCKET_SIZE),
            last_changed: self.last_changed,
        }
    }
}

#[derive(Debug)]
pub(crate) struct RoutingTable {
    our_id: Id20,
    id_changed: Option<Instant>,
    buckets: Vec<RoutingBucket>,
}

impl RoutingTable {
    fn new(our_id: Id20) -> Self {
        Self {
            our_id,
            id_changed: None,
            buckets: vec![RoutingBucket::root()],
        }
    }

    fn follow_external_address(
        &mut self,
        elected: Option<IpAddr>,
        proxied: bool,
        interval: Duration,
        now: Instant,
    ) -> IdPlan {
        let plan = plan_node_id(
            &self.our_id,
            elected,
            proxied,
            self.id_changed,
            interval,
            now,
        );
        if let IdPlan::Replace(id) = plan {
            self.set_node_id(id, now);
        }
        plan
    }

    fn set_node_id(&mut self, our_id: Id20, now: Instant) {
        let mut nodes: Vec<RoutingNode> = std::mem::take(&mut self.buckets)
            .into_iter()
            .flat_map(|bucket| bucket.nodes)
            .filter(|node| node.liveness != RoutingLiveness::Bad)
            .collect();
        nodes.sort_by_key(|node| {
            (
                node.liveness != RoutingLiveness::Good,
                !node.verified,
                std::cmp::Reverse(node.last_seen),
            )
        });
        self.our_id = our_id;
        self.id_changed = Some(now);
        self.buckets = vec![RoutingBucket::root()];
        for node in nodes {
            self.insert_node(node);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.nodes.len()).sum()
    }

    fn live_len(&self) -> usize {
        self.live_len_up_to(usize::MAX)
    }

    fn live_len_up_to(&self, cap: usize) -> usize {
        self.buckets
            .iter()
            .flat_map(|b| b.nodes.iter())
            .filter(|n| n.liveness != RoutingLiveness::Bad)
            .take(cap)
            .count()
    }

    fn bucket_index(&self, distance: &Id20) -> Option<usize> {
        self.buckets
            .iter()
            .position(|bucket| bucket.matches(distance))
    }

    fn split_bucket(&mut self, index: usize) {
        let bucket = self.buckets.remove(index);
        debug_assert!(bucket.prefix_len < 160);
        let mut zero = bucket.child(0);
        let mut one = bucket.child(1);
        for node in bucket.nodes {
            let distance = self.our_id.distance(&node.id);
            if RoutingBucket::bit(distance.as_bytes(), bucket.prefix_len) == 0 {
                zero.nodes.push(node);
            } else {
                one.nodes.push(node);
            }
        }
        self.buckets.insert(index, one);
        self.buckets.insert(index, zero);
    }

    fn add(&mut self, id: Id20, addr: SocketAddr) {
        self.add_with_liveness(id, addr, RoutingLiveness::Good);
    }

    fn add_with_liveness(&mut self, id: Id20, addr: SocketAddr, liveness: RoutingLiveness) {
        let distance = self.our_id.distance(&id);
        if distance == Id20([0; 20]) {
            return;
        }
        let now = Instant::now();
        if let Some(idx) = self.bucket_index(&distance) {
            let bucket = &mut self.buckets[idx];
            if let Some(position) = bucket.nodes.iter().position(|n| n.id == id) {
                if liveness != RoutingLiveness::Good {
                    return;
                }
                let existing = &mut bucket.nodes[position];
                if host_prefix(existing.addr.ip()) == host_prefix(addr.ip()) {
                    existing.addr = addr;
                    existing.last_seen = now;
                    existing.liveness = RoutingLiveness::Good;
                    existing.failures = 0;
                    bucket.last_changed = now;
                    return;
                }
                if existing.liveness != RoutingLiveness::Bad {
                    return;
                }
                bucket.nodes.remove(position);
            }
        }
        if !self.make_room_for_host(id, addr, liveness) {
            return;
        }
        self.insert_node(RoutingNode {
            id,
            addr,
            last_seen: now,
            liveness,
            failures: 0,
            verified: node_id_valid(&id, addr.ip()),
        });
    }

    fn insert_node(&mut self, fresh: RoutingNode) {
        let distance = self.our_id.distance(&fresh.id);
        if distance == Id20([0; 20]) {
            return;
        }
        let now = Instant::now();
        loop {
            let Some(idx) = self.bucket_index(&distance) else {
                return;
            };
            let bucket = &mut self.buckets[idx];
            if bucket.nodes.len() < BUCKET_SIZE {
                bucket.nodes.push(fresh);
                bucket.last_changed = now;
                return;
            }
            if bucket.contains_ours() && bucket.prefix_len < 160 {
                self.split_bucket(idx);
                continue;
            }
            let oldest_questionable = |unverified_only: bool| {
                bucket
                    .nodes
                    .iter()
                    .enumerate()
                    .filter(|(_, node)| {
                        node.liveness == RoutingLiveness::Questionable
                            && !(unverified_only && node.verified)
                    })
                    .min_by_key(|(_, node)| node.last_seen)
                    .map(|(idx, _)| idx)
            };
            let replace_idx = bucket
                .nodes
                .iter()
                .position(|node| node.liveness == RoutingLiveness::Bad)
                .or_else(|| {
                    if fresh.verified {
                        oldest_questionable(true)
                    } else {
                        None
                    }
                })
                .or_else(|| {
                    if fresh.liveness == RoutingLiveness::Good {
                        oldest_questionable(false)
                    } else {
                        None
                    }
                });
            if let Some(replace_idx) = replace_idx {
                bucket.nodes[replace_idx] = fresh;
                bucket.last_changed = now;
            }
            return;
        }
    }

    fn make_room_for_host(
        &mut self,
        id: Id20,
        addr: SocketAddr,
        liveness: RoutingLiveness,
    ) -> bool {
        let mut conflict = None;
        for (bucket_idx, bucket) in self.buckets.iter().enumerate() {
            for (node_idx, node) in bucket.nodes.iter().enumerate() {
                if node.id == id || host_prefix(node.addr.ip()) != host_prefix(addr.ip()) {
                    continue;
                }
                let replaceable = node.liveness == RoutingLiveness::Bad
                    || (liveness == RoutingLiveness::Good
                        && (node.liveness == RoutingLiveness::Questionable || node.addr == addr));
                if !replaceable {
                    return false;
                }
                conflict = Some((bucket_idx, node_idx));
            }
        }
        if let Some((bucket_idx, node_idx)) = conflict {
            self.buckets[bucket_idx].nodes.remove(node_idx);
        }
        true
    }

    fn stale_refresh_target(&mut self, now: Instant) -> Option<Id20> {
        let idx = self
            .buckets
            .iter()
            .enumerate()
            .filter(|(_, bucket)| now.duration_since(bucket.last_changed) >= ROUTING_STALE)
            .min_by_key(|(_, bucket)| bucket.last_changed)
            .map(|(idx, _)| idx)?;
        let bucket = &mut self.buckets[idx];
        bucket.last_changed = now;
        if bucket.contains_ours() {
            return Some(self.our_id);
        }
        let mut distance = [0u8; 20];
        rand::rng().fill(&mut distance[..]);
        for bit in 0..bucket.prefix_len {
            let mask = 1u8 << (7 - bit % 8);
            let byte = &mut distance[bit as usize / 8];
            if RoutingBucket::bit(&bucket.prefix, bit) == 1 {
                *byte |= mask;
            } else {
                *byte &= !mask;
            }
        }
        Some(self.our_id.distance(&Id20(distance)))
    }

    fn record_failure(&mut self, addr: SocketAddr) {
        for bucket in &mut self.buckets {
            for node in &mut bucket.nodes {
                if node.addr != addr || node.liveness == RoutingLiveness::Bad {
                    continue;
                }
                node.failures = node.failures.saturating_add(1);
                node.liveness = if node.failures >= MAX_QUERY_FAILURES {
                    RoutingLiveness::Bad
                } else {
                    RoutingLiveness::Questionable
                };
                return;
            }
        }
    }

    fn refresh_stale(&mut self, now: Instant, limit: usize) -> Vec<(Id20, SocketAddr)> {
        let mut out = Vec::new();
        for bucket in &mut self.buckets {
            for node in &mut bucket.nodes {
                if out.len() >= limit {
                    return out;
                }
                if node.liveness == RoutingLiveness::Bad {
                    continue;
                }
                if now.duration_since(node.last_seen) >= ROUTING_STALE {
                    node.liveness = match node.liveness {
                        RoutingLiveness::Good => RoutingLiveness::Questionable,
                        RoutingLiveness::Questionable => RoutingLiveness::Bad,
                        RoutingLiveness::Bad => RoutingLiveness::Bad,
                    };
                    node.last_seen = now;
                    out.push((node.id, node.addr));
                }
            }
        }
        out
    }

    fn snapshot(&self) -> Vec<(Id20, SocketAddr)> {
        self.buckets
            .iter()
            .flat_map(|bucket| bucket.nodes.iter())
            .filter(|node| node.liveness != RoutingLiveness::Bad)
            .map(|node| (node.id, node.addr))
            .collect()
    }

    fn closest_nodes(&self, target: &Id20, n: usize) -> Vec<(Id20, SocketAddr)> {
        self.closest_matching(target, n, |node| node.liveness != RoutingLiveness::Bad)
    }

    fn closest_good_nodes(&self, target: &Id20, n: usize) -> Vec<(Id20, SocketAddr)> {
        self.closest_matching(target, n, |node| node.liveness == RoutingLiveness::Good)
    }

    fn closest_matching(
        &self,
        target: &Id20,
        n: usize,
        keep: impl Fn(&RoutingNode) -> bool,
    ) -> Vec<(Id20, SocketAddr)> {
        let mut all: Vec<(Id20, Id20, SocketAddr)> = self
            .buckets
            .iter()
            .flat_map(|bucket| bucket.nodes.iter())
            .filter(|node| keep(node))
            .map(|node| (node.id.distance(target), node.id, node.addr))
            .collect();
        if all.len() > n {
            all.select_nth_unstable_by_key(n, |(dist, _, _)| *dist);
            all.truncate(n);
        }
        all.sort_unstable_by_key(|(dist, _, _)| *dist);
        all.into_iter().map(|(_, id, addr)| (id, addr)).collect()
    }
}

#[cfg(test)]
pub fn parse_compact_nodes(bytes: &[u8], v6: bool) -> Vec<(Id20, SocketAddr)> {
    let width = if v6 { 38 } else { 26 };
    bytes
        .chunks_exact(width)
        .filter_map(|chunk| {
            let id = Id20::from_slice(&chunk[..20]).ok()?;
            let addr = if v6 {
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&chunk[20..36]);
                let ip = Ipv6Addr::from(octets);
                let port = u16::from_be_bytes([chunk[36], chunk[37]]);
                SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0))
            } else {
                let ip = Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]);
                let port = u16::from_be_bytes([chunk[24], chunk[25]]);
                SocketAddr::V4(SocketAddrV4::new(ip, port))
            };
            valid_routing_addr(addr).then_some((id, addr))
        })
        .collect()
}

fn random_id() -> Id20 {
    let mut b = [0u8; 20];
    rand::rng().fill(&mut b[..]);
    Id20::from_slice(&b).expect("20 bytes")
}

fn random_transaction_id() -> [u8; 8] {
    let mut txn = [0u8; 8];
    rand::rng().fill(&mut txn);
    txn
}

fn pseudo_id(addr: SocketAddr) -> Id20 {
    use sha1::{Digest, Sha1};
    let mut h = Sha1::new();
    match addr {
        SocketAddr::V4(a) => {
            h.update(a.ip().octets());
            h.update(a.port().to_be_bytes());
        }
        SocketAddr::V6(a) => {
            h.update(a.ip().octets());
            h.update(a.port().to_be_bytes());
        }
    }
    Id20::from_slice(&h.finalize()[..20]).unwrap()
}

fn pseudo_id_target(target: &DhtTarget) -> Id20 {
    match target {
        DhtTarget::Addr(addr) => pseudo_id(*addr),
        DhtTarget::Host(host, port) => {
            use sha1::{Digest, Sha1};
            let mut h = Sha1::new();
            h.update(host.as_bytes());
            h.update(port.to_be_bytes());
            Id20::from_slice(&h.finalize()[..20]).unwrap()
        }
    }
}

fn build_announce_peer(
    txn: &[u8],
    our_id: &Id20,
    info_hash: &Id20,
    port: u16,
    token: &[u8],
) -> Vec<u8> {
    let args = Value::Dict(vec![
        (b"id".to_vec(), Value::Bytes(our_id.as_bytes().to_vec())),
        (b"implied_port".to_vec(), Value::Int(0)),
        (
            b"info_hash".to_vec(),
            Value::Bytes(info_hash.as_bytes().to_vec()),
        ),
        (b"port".to_vec(), Value::Int(port as i64)),
        (b"token".to_vec(), Value::Bytes(token.to_vec())),
    ]);
    let msg = Value::Dict(vec![
        (b"a".to_vec(), args),
        (b"q".to_vec(), Value::Bytes(b"announce_peer".to_vec())),
        (b"t".to_vec(), Value::Bytes(txn.to_vec())),
        (b"v".to_vec(), Value::Bytes(DHT_VERSION.to_vec())),
        (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
    ]);
    encode_to_vec(&msg)
}

#[cfg(test)]
fn build_get_peers(txn: u16, our_id: &Id20, info_hash: &Id20) -> Vec<u8> {
    build_get_peers_bytes(&txn.to_be_bytes(), our_id, info_hash, Want::Both, false)
}

fn build_ping_bytes(txn: &[u8], our_id: &Id20) -> Vec<u8> {
    let args = Value::Dict(vec![(
        b"id".to_vec(),
        Value::Bytes(our_id.as_bytes().to_vec()),
    )]);
    encode_to_vec(&Value::Dict(vec![
        (b"a".to_vec(), args),
        (b"q".to_vec(), Value::Bytes(b"ping".to_vec())),
        (b"t".to_vec(), Value::Bytes(txn.to_vec())),
        (b"v".to_vec(), Value::Bytes(DHT_VERSION.to_vec())),
        (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
    ]))
}

fn build_get_peers_bytes(
    txn: &[u8],
    our_id: &Id20,
    info_hash: &Id20,
    families: Want,
    read_only: bool,
) -> Vec<u8> {
    let mut want = Vec::new();
    if families != Want::V6 {
        want.push(Value::Bytes(b"n4".to_vec()));
    }
    if families != Want::V4 {
        want.push(Value::Bytes(b"n6".to_vec()));
    }
    let args = Value::Dict(vec![
        (b"id".to_vec(), Value::Bytes(our_id.as_bytes().to_vec())),
        (
            b"info_hash".to_vec(),
            Value::Bytes(info_hash.as_bytes().to_vec()),
        ),
        (b"want".to_vec(), Value::List(want)),
    ]);
    let mut fields = vec![
        (b"a".to_vec(), args),
        (b"q".to_vec(), Value::Bytes(b"get_peers".to_vec())),
        (b"t".to_vec(), Value::Bytes(txn.to_vec())),
        (b"v".to_vec(), Value::Bytes(DHT_VERSION.to_vec())),
        (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
    ];
    if read_only {
        fields.push((b"ro".to_vec(), Value::Int(1)));
    }
    encode_to_vec(&Value::Dict(fields))
}

fn parse_get_peers_response(body: &Value) -> Option<GetPeersResponseBody> {
    let r_val = body.get(b"r")?;
    r_val.as_dict()?;
    let responder_id = r_val
        .get(b"id")
        .and_then(|v| v.as_bytes())
        .and_then(|b| Id20::from_slice(b).ok());
    let mut peers: Vec<SocketAddr> = Vec::new();
    if let Some(values) = r_val.get(b"values").and_then(|v| v.as_list()) {
        for v in values {
            if let Some(b) = v.as_bytes() {
                match b.len() {
                    6 => {
                        let ip = Ipv4Addr::new(b[0], b[1], b[2], b[3]);
                        let port = u16::from_be_bytes([b[4], b[5]]);
                        let addr = SocketAddr::V4(SocketAddrV4::new(ip, port));
                        if public_dht_endpoint(addr) {
                            peers.push(addr);
                        }
                    }
                    18 => {
                        let mut o = [0u8; 16];
                        o.copy_from_slice(&b[..16]);
                        let ip = Ipv6Addr::from(o);
                        let port = u16::from_be_bytes([b[16], b[17]]);
                        let addr = SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0));
                        if public_dht_endpoint(addr) {
                            peers.push(addr);
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    let mut nodes: Vec<(Id20, SocketAddr)> = Vec::new();
    if let Some(n) = r_val.get(b"nodes").and_then(|v| v.as_bytes()) {
        for chunk in n.chunks_exact(26) {
            let id = Id20::from_slice(&chunk[..20]).ok()?;
            let ip = Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]);
            let port = u16::from_be_bytes([chunk[24], chunk[25]]);
            let addr = SocketAddr::V4(SocketAddrV4::new(ip, port));
            if valid_routing_addr(addr) {
                nodes.push((id, addr));
            }
        }
    }
    if let Some(n6) = r_val.get(b"nodes6").and_then(|v| v.as_bytes()) {
        for chunk in n6.chunks_exact(38) {
            let id = match Id20::from_slice(&chunk[..20]) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let mut o = [0u8; 16];
            o.copy_from_slice(&chunk[20..36]);
            let ip = Ipv6Addr::from(o);
            let port = u16::from_be_bytes([chunk[36], chunk[37]]);
            let addr = SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0));
            if valid_routing_addr(addr) {
                nodes.push((id, addr));
            }
        }
    }
    let token = r_val
        .get(b"token")
        .and_then(|v| v.as_bytes())
        .map(|b| b.to_vec());
    Some((responder_id, peers, nodes, token))
}

pub(crate) fn public_dht_endpoint(addr: SocketAddr) -> bool {
    valid_routing_addr(addr) && public_ip(addr.ip())
}

pub(crate) fn public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => public_dht_ipv4(ip),
        IpAddr::V6(ip) => {
            if ip.to_ipv4().is_some() {
                return false;
            }
            let segments = ip.segments();
            let documentation =
                segments[0] == 0x2001 && (segments[1] == 0x0db8 || segments[1] == 0x0002);
            let orchid = segments[0] == 0x2001
                && ((segments[1] & 0xfff0) == 0x0010 || (segments[1] & 0xfff0) == 0x0020);
            let discard_only =
                segments[0] == 0x0100 && segments[1..4].iter().all(|segment| *segment == 0);
            !ip.is_unspecified()
                && !ip.is_multicast()
                && !ip.is_loopback()
                && !ip.is_unicast_link_local()
                && !ip.is_unique_local()
                && !documentation
                && !orchid
                && !discard_only
        }
    }
}

fn public_dht_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    octets[0] != 0
        && !ip.is_unspecified()
        && !ip.is_loopback()
        && !ip.is_private()
        && !ip.is_link_local()
        && !ip.is_broadcast()
        && !ip.is_multicast()
        && !ip.is_documentation()
        && !crate::net::is_shared_cgnat(ip)
        && !(octets[0] == 192
            && ((octets[1] == 0 && octets[2] == 0)
                || (octets[1] == 31 && octets[2] == 196)
                || (octets[1] == 52 && octets[2] == 193)
                || (octets[1] == 88 && octets[2] == 99)
                || (octets[1] == 175 && octets[2] == 48)))
        && !(octets[0] == 198 && (18..=19).contains(&octets[1]))
        && octets[0] < 240
}

fn token_for_secret(secret: &[u8; 16], addr: SocketAddr) -> Vec<u8> {
    use sha1::{Digest, Sha1};
    let mut digest = Sha1::new();
    digest.update(secret);
    match addr {
        SocketAddr::V4(a) => digest.update(a.ip().octets()),
        SocketAddr::V6(a) => digest.update(a.ip().octets()),
    }
    digest.finalize()[..8].to_vec()
}

fn compact_peer(addr: SocketAddr) -> Vec<u8> {
    let mut out = Vec::with_capacity(match addr {
        SocketAddr::V4(_) => 6,
        SocketAddr::V6(_) => 18,
    });
    match addr {
        SocketAddr::V4(a) => out.extend_from_slice(&a.ip().octets()),
        SocketAddr::V6(a) => out.extend_from_slice(&a.ip().octets()),
    }
    out.extend_from_slice(&addr.port().to_be_bytes());
    out
}

fn compact_nodes(nodes: impl IntoIterator<Item = (Id20, SocketAddr)>, v6: bool) -> Vec<u8> {
    let width = if v6 { 38 } else { 26 };
    let mut out = Vec::new();
    for (id, addr) in nodes {
        if v6 != addr.is_ipv6() {
            continue;
        }
        out.reserve(width);
        out.extend_from_slice(id.as_bytes());
        out.extend_from_slice(&compact_peer(addr));
    }
    out
}

fn parse_want(args: &Value, from: SocketAddr) -> (bool, bool) {
    let Some(want) = args.get(b"want").and_then(Value::as_list) else {
        return (from.is_ipv4(), from.is_ipv6());
    };
    let mut n4 = false;
    let mut n6 = false;
    for item in want {
        match item.as_bytes() {
            Some(b"n4") => n4 = true,
            Some(b"n6") => n6 = true,
            _ => {}
        }
    }
    (n4, n6)
}

fn krpc_error(
    tid: &[u8],
    code: i64,
    message: &'static [u8],
    requester: Option<SocketAddr>,
) -> Vec<u8> {
    let mut fields = vec![
        (
            b"e".to_vec(),
            Value::List(vec![Value::Int(code), Value::Bytes(message.to_vec())]),
        ),
        (b"t".to_vec(), Value::Bytes(tid.to_vec())),
        (b"v".to_vec(), Value::Bytes(DHT_VERSION.to_vec())),
        (b"y".to_vec(), Value::Bytes(b"e".to_vec())),
    ];
    if let Some(requester) = requester {
        fields.push((b"ip".to_vec(), Value::Bytes(compact_requester(requester))));
    }
    encode_to_vec(&Value::Dict(fields))
}

#[allow(clippy::ptr_arg)]
fn bounded_response_bytes(response: &mut Vec<(Vec<u8>, Value)>) -> Vec<u8> {
    if let Some((_, Value::Dict(body))) = response.iter_mut().find(|(key, _)| key == b"r") {
        let max_values = MAX_KRPC_RESPONSE_BYTES / 6;
        for (key, width) in [
            (b"values".as_slice(), 6usize),
            (b"nodes".as_slice(), 26usize),
            (b"nodes6".as_slice(), 38usize),
        ] {
            let Some(index) = body.iter().position(|(field, _)| field == key) else {
                continue;
            };
            match &mut body[index].1 {
                Value::List(values) if key == b"values" => values.truncate(max_values),
                Value::Bytes(bytes) => {
                    let max_bytes = (MAX_KRPC_RESPONSE_BYTES / width) * width;
                    bytes.truncate(bytes.len().min(max_bytes) / width * width);
                }
                _ => {
                    body.remove(index);
                }
            }
        }
    }
    loop {
        let owned = Value::Dict(std::mem::take(response));
        let encoded = encode_to_vec(&owned);
        let Value::Dict(restored) = owned else {
            return encoded;
        };
        *response = restored;
        if encoded.len() <= MAX_KRPC_RESPONSE_BYTES {
            return encoded;
        }
        let mut trimmed = false;
        if let Some((_, Value::Dict(body))) = response.iter_mut().find(|(key, _)| key == b"r") {
            for key in [
                b"values".as_slice(),
                b"nodes6".as_slice(),
                b"nodes".as_slice(),
            ] {
                let Some(index) = body.iter().position(|(field, _)| field == key) else {
                    continue;
                };
                match &mut body[index].1 {
                    Value::List(values) if !values.is_empty() => {
                        values.truncate(values.len() / 2);
                        if values.is_empty() {
                            body.remove(index);
                        }
                    }
                    Value::Bytes(bytes) if !bytes.is_empty() => {
                        let width = if key == b"nodes6" { 38 } else { 26 };
                        bytes.truncate((bytes.len() / width / 2) * width);
                        if bytes.is_empty() {
                            body.remove(index);
                        }
                    }
                    _ => {
                        body.remove(index);
                    }
                }
                trimmed = true;
                break;
            }
        }
        if !trimmed {
            return encoded;
        }
    }
}

fn build_query_response(
    msg: &Value,
    from: SocketAddr,
    server: &InboundDhtState,
) -> Option<Vec<u8>> {
    let tid = msg.get(b"t").and_then(Value::as_bytes)?;
    if tid.is_empty() || tid.len() > 32 {
        return None;
    }
    let Some(query) = msg.get(b"q").and_then(Value::as_bytes) else {
        return Some(krpc_error(tid, 203, b"missing query", None));
    };
    let Some(args) = msg.get(b"a").filter(|a| a.as_dict().is_some()) else {
        return Some(krpc_error(tid, 203, b"missing arguments", None));
    };
    let remote_id = args
        .get(b"id")
        .and_then(Value::as_bytes)
        .and_then(|bytes| Id20::from_slice(bytes).ok());
    let Some(remote_id) = remote_id else {
        return Some(krpc_error(tid, 203, b"invalid id", None));
    };
    let read_only = msg.get(b"ro").and_then(Value::as_int) == Some(1);
    if !read_only {
        server.add_routing(remote_id, from);
    }

    let our_id = server.node_id(from.is_ipv6());
    let mut response = vec![
        (
            b"r".to_vec(),
            Value::Dict(vec![(
                b"id".to_vec(),
                Value::Bytes(our_id.as_bytes().to_vec()),
            )]),
        ),
        (b"t".to_vec(), Value::Bytes(tid.to_vec())),
        (b"v".to_vec(), Value::Bytes(DHT_VERSION.to_vec())),
        (b"y".to_vec(), Value::Bytes(b"r".to_vec())),
    ];
    match query {
        b"ping" => {}
        b"find_node" => {
            let target = args
                .get(b"target")
                .and_then(Value::as_bytes)
                .and_then(|bytes| Id20::from_slice(bytes).ok());
            let Some(target) = target else {
                return Some(krpc_error(tid, 203, b"invalid target", Some(from)));
            };
            let (want4, want6) = parse_want(args, from);
            let nodes = server.closest_nodes(&target, K, want4, want6);
            let mut body = vec![(b"id".to_vec(), Value::Bytes(our_id.as_bytes().to_vec()))];
            if want4 {
                body.push((
                    b"nodes".to_vec(),
                    Value::Bytes(compact_nodes(nodes.iter().copied(), false)),
                ));
            }
            if want6 {
                body.push((
                    b"nodes6".to_vec(),
                    Value::Bytes(compact_nodes(nodes.iter().copied(), true)),
                ));
            }
            response[0] = (b"r".to_vec(), Value::Dict(body));
        }
        b"get_peers" => {
            let hash = args
                .get(b"info_hash")
                .and_then(Value::as_bytes)
                .and_then(|bytes| Id20::from_slice(bytes).ok());
            let Some(hash) = hash else {
                return Some(krpc_error(tid, 203, b"invalid info_hash", Some(from)));
            };
            let (want4, want6) = parse_want(args, from);
            let peers = server.get_peers(hash, Some(from.is_ipv6()));
            let nodes = server.closest_nodes(&hash, K, want4, want6);
            let mut body = vec![
                (b"id".to_vec(), Value::Bytes(our_id.as_bytes().to_vec())),
                (b"token".to_vec(), Value::Bytes(server.token(from))),
            ];
            let values: Vec<Value> = peers
                .into_iter()
                .take(MAX_RESPONSE_PEERS)
                .map(|addr| Value::Bytes(compact_peer(addr)))
                .collect();
            if !values.is_empty() {
                body.push((b"values".to_vec(), Value::List(values)));
            }
            if want4 {
                body.push((
                    b"nodes".to_vec(),
                    Value::Bytes(compact_nodes(nodes.iter().copied(), false)),
                ));
            }
            if want6 {
                body.push((
                    b"nodes6".to_vec(),
                    Value::Bytes(compact_nodes(nodes.iter().copied(), true)),
                ));
            }
            response[0] = (b"r".to_vec(), Value::Dict(body));
        }
        b"announce_peer" => {
            let hash = args
                .get(b"info_hash")
                .and_then(Value::as_bytes)
                .and_then(|bytes| Id20::from_slice(bytes).ok());
            let token = args.get(b"token").and_then(Value::as_bytes);
            let Some(hash) = hash else {
                return Some(krpc_error(tid, 203, b"invalid info_hash", Some(from)));
            };
            let Some(token) = token else {
                return Some(krpc_error(tid, 203, b"missing token", Some(from)));
            };
            if !server.valid_token(from, token) {
                return Some(krpc_error(tid, 203, b"invalid token", Some(from)));
            }
            let implied = args
                .get(b"implied_port")
                .and_then(Value::as_int)
                .unwrap_or(0);
            let implied = implied != 0;
            let port = if implied {
                from.port()
            } else {
                args.get(b"port")
                    .and_then(Value::as_int)
                    .and_then(|p| u16::try_from(p).ok())
                    .unwrap_or(0)
            };
            if port == 0 {
                return Some(krpc_error(tid, 203, b"invalid port", Some(from)));
            }
            server.add_peer(hash, SocketAddr::new(from.ip(), port));
        }
        _ => return Some(krpc_error(tid, 204, b"method unknown", Some(from))),
    }
    response.push((b"ip".to_vec(), Value::Bytes(compact_requester(from))));
    Some(bounded_response_bytes(&mut response))
}

fn compact_requester(from: SocketAddr) -> Vec<u8> {
    compact_peer(crate::net::canonical_addr(from))
}

fn reported_external_ip(body: &Value) -> Option<IpAddr> {
    let bytes = body.get(b"ip")?.as_bytes()?;
    match bytes.len() {
        6 | 18 => crate::net::ip_from_bytes(&bytes[..bytes.len() - 2]),
        _ => None,
    }
}

fn note_reported_ip(external: &ExternalIp, body: &Value, from: SocketAddr) {
    if !public_dht_endpoint(from) {
        return;
    }
    if let Some(observed) = reported_external_ip(body) {
        external.vote(observed, Source::Dht, from.ip(), Route::Direct);
    }
}

async fn reader_loop(
    sock: Arc<UdpSocket>,
    pending: Arc<Mutex<PendingMap>>,
    server: Arc<InboundDhtState>,
    external: ExternalIp,
) {
    let mut buf = vec![0u8; MAX_KRPC_DATAGRAM];
    let mut limiter = InboundLimiter::new(Instant::now());
    let mut errors = 0u32;
    loop {
        let (n, from) = match sock.recv_from(&mut buf).await {
            Ok(x) => {
                errors = 0;
                x
            }
            Err(e) => {
                // Windows reports ICMP port-unreachable as recv errors, none of which end the socket
                tracing::trace!("dht recv error: {e}");
                errors += 1;
                if errors > 8 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                continue;
            }
        };
        let Ok(msg) = decode_all_external(&buf[..n], KRPC_DECODE_LIMITS) else {
            if let Some(reply) =
                malformed_query_reply(&buf[..n], from.ip(), &mut limiter, Instant::now())
            {
                let _ = sock.send_to(&reply, from).await;
            }
            continue;
        };
        let Some(ty) = msg.get(b"y").and_then(|v| v.as_bytes()) else {
            continue;
        };
        if ty == b"q" {
            if !limiter.allow(from.ip(), Instant::now()) {
                continue;
            }
            if let Some(reply) = build_query_response(&msg, from, &server) {
                if limiter.take_reply_budget(reply.len(), Instant::now()) {
                    let _ = sock.send_to(&reply, from).await;
                }
            }
            continue;
        }
        if ty != b"r" && ty != b"e" {
            continue;
        }
        let Some(tid) = msg.get(b"t").and_then(|v| v.as_bytes()) else {
            continue;
        };
        let is_response = ty == b"r";
        let txn = tid.to_vec();
        deliver_reply(&pending, &external, &txn, is_response, msg, from);
    }
}

fn deliver_reply(
    pending: &Mutex<PendingMap>,
    external: &ExternalIp,
    txn: &[u8],
    is_response: bool,
    msg: Value,
    from: SocketAddr,
) -> bool {
    let answered = {
        let mut guard = pending.lock();
        let matches = guard.get(txn).is_some_and(|entry| {
            entry
                .resolved_addrs
                .as_ref()
                .is_some_and(|addresses| addresses.contains(&from))
                || (entry.resolved_addrs.is_none()
                    && dht_targets_match(&entry.target, &DhtTarget::Addr(from)))
        });
        if matches {
            guard.remove(txn)
        } else {
            None
        }
    };
    let Some(entry) = answered else {
        return false;
    };
    if is_response {
        note_reported_ip(external, &msg, from);
    }
    let _ = entry.tx.send(KrpcResponse {
        from: DhtTarget::Addr(from),
        body: msg,
    });
    true
}

fn is_query_packet(packet: &[u8]) -> bool {
    packet.windows(6).any(|w| w == b"1:y1:q")
}

fn malformed_query_reply(
    packet: &[u8],
    ip: IpAddr,
    limiter: &mut InboundLimiter,
    now: Instant,
) -> Option<Vec<u8>> {
    if !is_query_packet(packet) || !limiter.allow(ip, now) {
        return None;
    }
    let tid = recover_transaction_id(packet)?;
    let reply = krpc_error(&tid, 203, b"invalid bencode", None);
    limiter.take_reply_budget(reply.len(), now).then_some(reply)
}

struct InboundLimiter {
    ips: HashMap<IpAddr, QueryBucket>,
    reply_tokens: f64,
    reply_refilled: Instant,
}

struct QueryBucket {
    tokens: f64,
    refilled: Instant,
    blocked_until: Option<Instant>,
}

impl InboundLimiter {
    fn new(now: Instant) -> Self {
        Self {
            ips: HashMap::new(),
            reply_tokens: REPLY_BYTES_PER_SEC,
            reply_refilled: now,
        }
    }

    fn allow(&mut self, ip: IpAddr, now: Instant) -> bool {
        let ip = host_prefix(ip);
        if self.ips.len() >= MAX_TRACKED_QUERY_IPS && !self.ips.contains_key(&ip) {
            self.make_room(now);
        }
        let bucket = self.ips.entry(ip).or_insert(QueryBucket {
            tokens: QUERY_BURST,
            refilled: now,
            blocked_until: None,
        });
        if let Some(until) = bucket.blocked_until {
            if now < until {
                return false;
            }
            bucket.blocked_until = None;
            bucket.tokens = QUERY_BURST;
        }
        let elapsed = now.duration_since(bucket.refilled).as_secs_f64();
        bucket.refilled = now;
        bucket.tokens = (bucket.tokens + elapsed * QUERY_RATE_PER_SEC).min(QUERY_BURST);
        if bucket.tokens < 1.0 {
            bucket.blocked_until = Some(now + QUERY_BLOCK);
            return false;
        }
        bucket.tokens -= 1.0;
        true
    }

    fn make_room(&mut self, now: Instant) {
        self.ips.retain(|_, b| {
            b.blocked_until.is_some_and(|t| t > now)
                || now.duration_since(b.refilled) < Duration::from_secs(10)
        });
        if self.ips.len() < MAX_TRACKED_QUERY_IPS {
            return;
        }
        let mut by_age: Vec<((bool, Instant), IpAddr)> = self
            .ips
            .iter()
            .map(|(ip, bucket)| {
                let blocked = bucket.blocked_until.is_some_and(|t| t > now);
                ((blocked, bucket.refilled), *ip)
            })
            .collect();
        let evict = (MAX_TRACKED_QUERY_IPS / 8).max(1).min(by_age.len());
        by_age.select_nth_unstable_by_key(evict - 1, |(rank, _)| *rank);
        for (_, ip) in &by_age[..evict] {
            self.ips.remove(ip);
        }
    }

    fn take_reply_budget(&mut self, bytes: usize, now: Instant) -> bool {
        let elapsed = now.duration_since(self.reply_refilled).as_secs_f64();
        self.reply_refilled = now;
        self.reply_tokens =
            (self.reply_tokens + elapsed * REPLY_BYTES_PER_SEC).min(REPLY_BYTES_PER_SEC);
        if self.reply_tokens < bytes as f64 {
            return false;
        }
        self.reply_tokens -= bytes as f64;
        true
    }
}

fn recover_transaction_id(packet: &[u8]) -> Option<Vec<u8>> {
    let marker = b"1:t";
    let start = packet.windows(marker.len()).position(|w| w == marker)? + marker.len();
    let mut colon = start;
    while colon < packet.len() && packet[colon].is_ascii_digit() {
        colon += 1;
    }
    if colon == start || colon >= packet.len() || packet[colon] != b':' {
        return None;
    }
    let len = std::str::from_utf8(&packet[start..colon])
        .ok()?
        .parse::<usize>()
        .ok()?;
    if len == 0 || len > 32 || colon + 1 + len > packet.len() {
        return None;
    }
    Some(packet[colon + 1..colon + 1 + len].to_vec())
}

async fn proxy_reader_loop(sock: Arc<ProxyAssociation>, pending: Arc<Mutex<PendingMap>>) {
    let mut buf = vec![0u8; MAX_KRPC_DATAGRAM];
    loop {
        let (n, source) = match sock.recv_from_target(&mut buf).await {
            Ok(x) => x,
            Err(error) => {
                tracing::debug!("dht proxy reader stopped: {error}");
                return;
            }
        };
        let from = match &source {
            ProxyDatagramSource::Ip(address) => DhtTarget::Addr(*address),
            ProxyDatagramSource::Host(host, port) => DhtTarget::Host(host.clone(), *port),
        };
        let Ok(msg) = decode_all_external(&buf[..n], KRPC_DECODE_LIMITS) else {
            continue;
        };
        let Some(ty) = msg.get(b"y").and_then(|v| v.as_bytes()) else {
            continue;
        };
        if ty != b"r" && ty != b"e" {
            continue;
        }
        let Some(tid) = msg.get(b"t").and_then(|v| v.as_bytes()) else {
            continue;
        };
        let txn = tid.to_vec();
        let expected = pending.lock().get(&txn).map(|entry| entry.target.clone());
        let matches = match (&expected, &source) {
            (Some(DhtTarget::Addr(target)), ProxyDatagramSource::Host(..)) => {
                risuko_http::datagram_source_matches(&source, *target)
            }
            (Some(expected), _) => dht_targets_match(expected, &from),
            (None, _) => false,
        };
        if matches {
            let mut guard = pending.lock();
            if let Some(entry) = guard.get(&txn) {
                if expected
                    .as_ref()
                    .is_some_and(|target| target == &entry.target)
                {
                    if let Some(entry) = guard.remove(&txn) {
                        let _ = entry.tx.send(KrpcResponse { from, body: msg });
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::eventually;

    #[test]
    fn get_peers_wants_v6_only_when_usable() {
        let id = Id20::from_slice(&[7u8; 20]).unwrap();
        let want_of = |v6: bool| {
            let want = if v6 { Want::Both } else { Want::V4 };
            let pkt = build_get_peers_bytes(b"ab", &id, &id, want, false);
            let msg = decode_all(&pkt).unwrap();
            let list = msg
                .get(b"a")
                .unwrap()
                .get(b"want")
                .unwrap()
                .as_list()
                .unwrap();
            list.iter()
                .filter_map(|v| v.as_bytes().map(<[u8]>::to_vec))
                .collect::<Vec<_>>()
        };
        assert_eq!(want_of(true), vec![b"n4".to_vec(), b"n6".to_vec()]);
        assert_eq!(want_of(false), vec![b"n4".to_vec()]);
    }
    use crate::bencode::decode_all;
    use std::io::Write;

    static SHARED_TEST_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
        std::sync::OnceLock::new();

    async fn reset_shared_state() {
        let previous = {
            let mut state = shared_dht_cell().lock().await;
            state.proxy_requested = None;
            state.options = DhtOptions::default();
            state.dht.take()
        };
        if let Some(previous) = previous {
            previous.shutdown().await;
        }
    }

    async fn within_shared_test(test: impl std::future::Future<Output = ()>) {
        tokio::time::timeout(Duration::from_secs(10), test)
            .await
            .expect("shared DHT test timed out");
    }

    #[tokio::test]
    async fn proxied_dht_failure_blocks_implicit_direct_creation() {
        let lock = SHARED_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
        let _guard = lock.lock().await;
        reset_shared_state().await;

        let proxy = risuko_http::ProxyConnector::from_proxy(
            risuko_http::Proxy::all("http://127.0.0.1:8080").unwrap(),
        );
        assert!(Dht::replace_shared_with_proxy(Some(proxy)).await.is_none());
        assert!(Dht::current_shared().await.is_none());
        assert!(Dht::shared().await.is_none());

        reset_shared_state().await;
    }

    #[tokio::test]
    async fn dht_route_swap_surfaces_unavailable_http_udp_route() {
        let lock = SHARED_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
        let _guard = lock.lock().await;
        reset_shared_state().await;

        let previous = Dht::replace_shared_with_proxy(None)
            .await
            .expect("direct DHT should bind");
        let proxy = risuko_http::ProxyConnector::from_proxy(
            risuko_http::Proxy::all("http://127.0.0.1:8080").unwrap(),
        );

        let error = match Dht::prepare_shared_with_proxy(Some(proxy), DhtOptions::default()).await {
            Ok(_) => panic!("HTTP UDP limitation must fail a checked route swap"),
            Err(error) => error,
        };
        assert!(error.to_ascii_lowercase().contains("socks5 required"));
        let restored = Dht::current_shared()
            .await
            .expect("failed route preparation must preserve the old DHT");
        assert!(Arc::ptr_eq(&restored, &previous));
        assert!(Dht::shared().await.is_some());
        drop(previous);

        reset_shared_state().await;
    }

    #[test]
    fn bootstrap_nodes_remain_host_targets_until_sent() {
        let targets = bootstrap_targets();
        assert_eq!(targets.len(), DEFAULT_BOOTSTRAP.len());
        assert!(matches!(
            targets.first(),
            Some(DhtTarget::Host(host, 6881)) if host == "router.bittorrent.com"
        ));
    }

    #[test]
    fn routing_table_inserts_dedupes_and_indexes_distance() {
        let me = Id20::from_slice(&[0u8; 20]).unwrap();
        let mut rt = RoutingTable::new(me);
        let n1 = Id20::from_slice(&[1u8; 20]).unwrap();
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
        rt.add(n1, addr);
        rt.add(n1, addr);
        assert_eq!(rt.len(), 1);
        let n2 = Id20::from_slice(&[2u8; 20]).unwrap();
        rt.add(n2, "127.0.0.2:1".parse().unwrap());
        assert_eq!(rt.len(), 2);
        rt.add(me, addr);
        assert_eq!(rt.len(), 2);
    }

    #[test]
    fn routing_endpoint_validation_rejects_non_routable_addresses() {
        for raw in [
            "0.0.0.0:6881",
            "255.255.255.255:6881",
            "224.0.0.1:6881",
            "127.0.0.1:0",
        ] {
            let addr: SocketAddr = raw.parse().unwrap();
            assert!(!valid_routing_addr(addr), "accepted {raw}");
        }
        assert!(valid_routing_addr("192.0.2.1:6881".parse().unwrap()));
    }

    #[test]
    fn routing_table_caps_bucket_at_k() {
        let me = Id20::from_slice(&[0u8; 20]).unwrap();
        let mut rt = RoutingTable::new(me);
        for i in 1..=20u8 {
            let mut id = [0u8; 20];
            id[0] = 0x80;
            id[19] = i;
            rt.add(
                Id20::from_slice(&id).unwrap(),
                SocketAddr::from(([127, 0, 0, i], 1)),
            );
        }
        assert_eq!(rt.len(), BUCKET_SIZE);
    }

    #[test]
    fn routing_refresh_marks_stale_contacts_before_evicting_them() {
        let me = Id20::from_slice(&[0u8; 20]).unwrap();
        let mut rt = RoutingTable::new(me);
        let mut id = [0u8; 20];
        id[0] = 0x80;
        id[19] = 1;
        rt.add(
            Id20::from_slice(&id).unwrap(),
            "127.0.0.1:6881".parse().unwrap(),
        );
        rt.buckets[0].nodes[0].last_seen = Instant::now() - ROUTING_STALE;

        let first = rt.refresh_stale(Instant::now(), 1);
        assert_eq!(first.len(), 1);
        assert_eq!(
            rt.buckets[0].nodes[0].liveness,
            RoutingLiveness::Questionable
        );

        rt.buckets[0].nodes[0].last_seen = Instant::now() - ROUTING_STALE;
        let second = rt.refresh_stale(Instant::now(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(rt.buckets[0].nodes[0].liveness, RoutingLiveness::Bad);
        assert!(rt.refresh_stale(Instant::now(), 1).is_empty());
        assert!(rt.snapshot().is_empty());
    }

    #[test]
    fn malformed_packet_transaction_recovery_is_bounded() {
        assert_eq!(
            recover_transaction_id(b"d1:t2:ab1:y1:q"),
            Some(b"ab".to_vec())
        );
        assert_eq!(recover_transaction_id(b"d1:t0:1:y1:q"), None);
        assert_eq!(
            recover_transaction_id(b"d1:t33:0123456789012345678901234567890121:y1:q"),
            None
        );
        assert_eq!(recover_transaction_id(b"d1:tnope"), None);
    }

    #[test]
    fn routing_state_file_round_trips_and_ignores_invalid_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/dht-routing.state");
        let first = Id20::from_slice(&[1u8; 20]).unwrap();
        let second = Id20::from_slice(&[2u8; 20]).unwrap();
        let contacts = vec![
            (first, "8.8.8.8:6881".parse().unwrap()),
            (second, "[2001:4860:4860::8888]:6881".parse().unwrap()),
        ];
        assert_eq!(
            save_routing_state_file(&path, &contacts, &SavedIds::default()).unwrap(),
            2
        );
        let private = [
            "127.0.0.1:6881",
            "10.1.2.3:6881",
            "192.168.1.9:6881",
            "[::1]:6881",
            "[fd00::1]:6881",
            "[::ffff:8.8.4.4]:6881",
        ];
        let mut extra = String::from("not-a-contact\n0011\t0.0.0.0:1\n");
        for addr in private {
            extra.push_str(&format!("{}\t{addr}\n", Id20([3u8; 20]).to_hex()));
        }
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(extra.as_bytes())
            .unwrap();

        assert_eq!(load_routing_state_file(&path).unwrap().contacts, contacts);
    }

    #[tokio::test]
    async fn configured_routing_state_loads_and_saves_on_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        let first = Id20::from_slice(&[1u8; 20]).unwrap();
        let addr: SocketAddr = "8.8.8.8:6881".parse().unwrap();
        save_routing_state_file(&path, &[(first, addr)], &SavedIds::default()).unwrap();

        let dht = Dht::spawn_with_routing_state(path.clone()).await.unwrap();
        assert_eq!(dht.routing_table_len(), 1);
        let second = Id20::from_slice(&[2u8; 20]).unwrap();
        dht.add_bootstrap_nodes([(second, "[2001:4860:4860::8888]:6881".parse().unwrap())]);
        dht.shutdown().await;
        let restored = load_routing_state_file(&path).unwrap().contacts;
        assert!(restored.contains(&(first, addr)));
        assert!(restored.iter().any(|(id, _)| *id == second));
    }

    #[test]
    fn outbound_transaction_ids_are_opaque_eight_byte_values() {
        assert_eq!(random_transaction_id().len(), 8);
    }

    #[test]
    fn get_peers_packet_is_bencoded_krpc_query() {
        let our_id = Id20::from_slice(&[0u8; 20]).unwrap();
        let info_hash = Id20::from_slice(&[1u8; 20]).unwrap();
        let packet = build_get_peers(0xBEEF, &our_id, &info_hash);
        let decoded = decode_all(&packet).unwrap();
        assert_eq!(
            decoded.get(b"q").and_then(|v| v.as_bytes()),
            Some(b"get_peers" as &[u8])
        );
        assert_eq!(
            decoded.get(b"y").and_then(|v| v.as_bytes()),
            Some(b"q" as &[u8])
        );
        assert_eq!(
            decoded.get(b"t").and_then(|v| v.as_bytes()),
            Some(&[0xBE, 0xEF][..])
        );
        let a = decoded.get(b"a").unwrap().as_dict().unwrap();
        assert_eq!(a.len(), 3);
    }

    #[test]
    fn announce_peer_packet_is_bencoded_krpc_query() {
        let our_id = Id20::from_slice(&[0u8; 20]).unwrap();
        let info_hash = Id20::from_slice(&[1u8; 20]).unwrap();
        let packet = build_announce_peer(b"opaque-tx", &our_id, &info_hash, 6881, b"tok");
        let decoded = decode_all(&packet).unwrap();
        assert_eq!(
            decoded.get(b"q").and_then(|v| v.as_bytes()),
            Some(b"announce_peer" as &[u8])
        );
        assert_eq!(
            decoded.get(b"y").and_then(|v| v.as_bytes()),
            Some(b"q" as &[u8])
        );
        assert_eq!(
            decoded.get(b"t").and_then(|v| v.as_bytes()),
            Some(b"opaque-tx" as &[u8])
        );
        let a = Value::Dict(decoded.get(b"a").unwrap().as_dict().unwrap().to_vec());
        assert_eq!(a.get(b"port").and_then(|v| v.as_int()), Some(6881));
        assert_eq!(
            a.get(b"token").and_then(|v| v.as_bytes()),
            Some(b"tok" as &[u8])
        );
        assert_eq!(
            a.get(b"info_hash").and_then(|v| v.as_bytes()),
            Some(&[1u8; 20][..])
        );
    }

    #[test]
    fn parse_response_extracts_peers_and_nodes() {
        let peer_bytes: Vec<u8> = vec![1, 2, 3, 4, (5678u16 >> 8) as u8, (5678u16 & 0xff) as u8];
        let mut node_bytes = vec![0x22u8; 20];
        node_bytes.extend_from_slice(&[9, 8, 7, 6]);
        node_bytes.extend_from_slice(&11111u16.to_be_bytes());

        let r = Value::Dict(vec![
            (b"id".to_vec(), Value::Bytes(vec![0u8; 20])),
            (b"nodes".to_vec(), Value::Bytes(node_bytes)),
            (b"token".to_vec(), Value::Bytes(b"abcd".to_vec())),
            (
                b"values".to_vec(),
                Value::List(vec![Value::Bytes(peer_bytes)]),
            ),
        ]);
        let body = Value::Dict(vec![
            (b"r".to_vec(), r),
            (b"t".to_vec(), Value::Bytes(b"aa".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"r".to_vec())),
        ]);
        let (_id, peers, nodes, token) = parse_get_peers_response(&body).unwrap();
        assert_eq!(token.as_deref(), Some(b"abcd" as &[u8]));
        assert_eq!(peers.len(), 1);
        assert_eq!(
            peers[0],
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 5678))
        );
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes[0].1,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(9, 8, 7, 6), 11111))
        );
    }

    #[test]
    fn parse_response_extracts_v6_peers_and_nodes6() {
        let mut peer6: Vec<u8> = Vec::with_capacity(18);
        let peer_ip6: Ipv6Addr = "2001:4860:4860::8888".parse().unwrap();
        peer6.extend_from_slice(&peer_ip6.octets());
        peer6.extend_from_slice(&6881u16.to_be_bytes());
        let mut node6 = vec![0x33u8; 20];
        let ip6: Ipv6Addr = "2001:db8::1".parse().unwrap();
        node6.extend_from_slice(&ip6.octets());
        node6.extend_from_slice(&12345u16.to_be_bytes());

        let r = Value::Dict(vec![
            (b"id".to_vec(), Value::Bytes(vec![0u8; 20])),
            (b"nodes6".to_vec(), Value::Bytes(node6)),
            (b"values".to_vec(), Value::List(vec![Value::Bytes(peer6)])),
        ]);
        let body = Value::Dict(vec![
            (b"r".to_vec(), r),
            (b"t".to_vec(), Value::Bytes(b"bb".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"r".to_vec())),
        ]);
        let (_id, peers, nodes, _token) = parse_get_peers_response(&body).unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(
            peers[0],
            SocketAddr::V6(SocketAddrV6::new(peer_ip6, 6881, 0, 0))
        );
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            nodes[0].1,
            SocketAddr::V6(SocketAddrV6::new(ip6, 12345, 0, 0))
        );
    }

    #[test]
    fn get_peers_packet_includes_want_n4_n6() {
        let our_id = Id20::from_slice(&[0u8; 20]).unwrap();
        let info_hash = Id20::from_slice(&[1u8; 20]).unwrap();
        let packet = build_get_peers(0xABCD, &our_id, &info_hash);
        let decoded = decode_all(&packet).unwrap();
        let a = decoded.get(b"a").unwrap().as_dict().unwrap();
        let a_val = Value::Dict(a.to_vec());
        let want = a_val.get(b"want").unwrap().as_list().unwrap();
        let labels: Vec<&[u8]> = want.iter().filter_map(|v| v.as_bytes()).collect();
        assert!(labels.contains(&(b"n4" as &[u8])));
        assert!(labels.contains(&(b"n6" as &[u8])));
    }

    #[test]
    fn pending_guard_drop_does_not_remove_reused_transaction() {
        let pending: Arc<Mutex<PendingMap>> = Arc::new(Mutex::new(Default::default()));
        let txn = 0x1234u16.to_be_bytes().to_vec();
        let target: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let (old_tx, _old_rx) = oneshot::channel();
        let (new_tx, _new_rx) = oneshot::channel();
        let old_token = PendingToken::new();
        let new_token = PendingToken::new();

        pending.lock().insert(
            txn.clone(),
            PendingEntry {
                tx: old_tx,
                target: DhtTarget::Addr(target),
                resolved_addrs: None,
                token: old_token.clone(),
            },
        );
        let guard = PendingGuard {
            pending: pending.clone(),
            txn: txn.clone(),
            token: old_token,
        };
        pending.lock().remove(&txn);
        pending.lock().insert(
            txn.clone(),
            PendingEntry {
                tx: new_tx,
                target: DhtTarget::Addr(target),
                resolved_addrs: None,
                token: new_token,
            },
        );

        drop(guard);

        assert!(pending.lock().contains_key(&txn));
    }

    #[test]
    fn read_only_querier_is_answered_but_not_routed() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let from: SocketAddr = "8.8.8.8:6000".parse().unwrap();
        for (read_only, routed) in [(true, 0), (false, 1)] {
            let state =
                InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
            let mut fields = vec![
                (
                    b"a".to_vec(),
                    Value::Dict(vec![(b"id".to_vec(), Value::Bytes(vec![7u8; 20]))]),
                ),
                (b"q".to_vec(), Value::Bytes(b"ping".to_vec())),
                (b"t".to_vec(), Value::Bytes(b"ro".to_vec())),
                (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
            ];
            if read_only {
                fields.push((b"ro".to_vec(), Value::Int(1)));
            }
            let reply = build_query_response(&Value::Dict(fields), from, &state).unwrap();
            assert_eq!(
                decode_all(&reply)
                    .unwrap()
                    .get(b"y")
                    .and_then(Value::as_bytes),
                Some(b"r" as &[u8])
            );
            assert_eq!(state.routing.lock().len(), routed, "read_only={read_only}");
        }
    }

    fn query_from(method: &[u8], args: Vec<(Vec<u8>, Value)>) -> Value {
        let mut all = vec![(b"id".to_vec(), Value::Bytes(vec![7u8; 20]))];
        all.extend(args);
        Value::Dict(vec![
            (b"a".to_vec(), Value::Dict(all)),
            (b"q".to_vec(), Value::Bytes(method.to_vec())),
            (b"t".to_vec(), Value::Bytes(b"ip".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        ])
    }

    #[test]
    fn every_query_response_names_the_requester_in_a_top_level_ip_key() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let hash = Value::Bytes(vec![5u8; 20]);
        let token = Value::Bytes(state.token("8.8.8.8:6000".parse().unwrap()));
        let queries = [
            query_from(b"ping", vec![]),
            query_from(b"find_node", vec![(b"target".to_vec(), hash.clone())]),
            query_from(b"get_peers", vec![(b"info_hash".to_vec(), hash.clone())]),
            query_from(
                b"announce_peer",
                vec![
                    (b"info_hash".to_vec(), hash),
                    (b"port".to_vec(), Value::Int(6881)),
                    (b"token".to_vec(), token),
                ],
            ),
        ];
        let from: SocketAddr = "8.8.8.8:6000".parse().unwrap();
        for query in &queries {
            let reply = decode_all(&build_query_response(query, from, &state).unwrap()).unwrap();
            assert_eq!(
                reply.get(b"y").and_then(Value::as_bytes),
                Some(b"r" as &[u8])
            );
            assert_eq!(
                reply.get(b"ip").and_then(Value::as_bytes),
                Some([8u8, 8, 8, 8, 0x17, 0x70].as_slice())
            );
            assert!(reply.get(b"r").and_then(|r| r.get(b"ip")).is_none());
        }
    }

    #[test]
    fn the_requester_ip_is_six_bytes_for_v4_and_eighteen_for_v6() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let ping = query_from(b"ping", vec![]);
        let ip_of = |from: &str| {
            let reply =
                decode_all(&build_query_response(&ping, from.parse().unwrap(), &state).unwrap())
                    .unwrap();
            reply
                .get(b"ip")
                .and_then(Value::as_bytes)
                .map(<[u8]>::to_vec)
                .unwrap()
        };
        let v6 = ip_of("[2a00:1450:4001:80b::200e]:6881");
        assert_eq!(v6.len(), 18);
        assert_eq!(&v6[16..], &6881u16.to_be_bytes());
        assert_eq!(ip_of("[::ffff:8.8.4.4]:6881"), vec![8, 8, 4, 4, 0x1a, 0xe1]);
    }

    #[test]
    fn errors_after_the_id_check_carry_the_ip_key_and_trimmed_responses_stay_valid() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let from: SocketAddr = "[2a00:1450:4001:80b::200e]:6881".parse().unwrap();
        let ip_len = |bytes: &[u8]| {
            decode_all(bytes)
                .unwrap()
                .get(b"ip")
                .and_then(Value::as_bytes)
                .map(<[u8]>::len)
        };
        let post_id_errors = [
            query_from(b"nope", vec![]),
            query_from(b"find_node", vec![]),
            query_from(b"get_peers", vec![]),
            query_from(
                b"announce_peer",
                vec![(b"info_hash".to_vec(), Value::Bytes(vec![5u8; 20]))],
            ),
        ];
        for query in &post_id_errors {
            let bytes = build_query_response(query, from, &state).unwrap();
            let reply = decode_all(&bytes).unwrap();
            assert_eq!(
                reply.get(b"y").and_then(Value::as_bytes),
                Some(b"e".as_slice())
            );
            assert_eq!(ip_len(&bytes), Some(18));
        }
        let no_method = Value::Dict(vec![
            (b"t".to_vec(), Value::Bytes(b"ip".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        ]);
        let no_args = Value::Dict(vec![
            (b"q".to_vec(), Value::Bytes(b"ping".to_vec())),
            (b"t".to_vec(), Value::Bytes(b"ip".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        ]);
        let bad_id = Value::Dict(vec![
            (
                b"a".to_vec(),
                Value::Dict(vec![(b"id".to_vec(), Value::Bytes(vec![7u8; 3]))]),
            ),
            (b"q".to_vec(), Value::Bytes(b"ping".to_vec())),
            (b"t".to_vec(), Value::Bytes(b"ip".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        ]);
        for query in [no_method, no_args, bad_id] {
            let bytes = build_query_response(&query, from, &state).unwrap();
            let reply = decode_all(&bytes).unwrap();
            assert_eq!(
                reply.get(b"y").and_then(Value::as_bytes),
                Some(b"e".as_slice())
            );
            assert_eq!(ip_len(&bytes), None);
        }
        for index in 1..=60u8 {
            let id = Id20::from_slice(&[index; 20]).unwrap();
            state.add_routing(id, SocketAddr::from(([8, index, 8, 8], 6881)));
            state.add_routing(
                id,
                SocketAddr::new(
                    IpAddr::V6(Ipv6Addr::new(0x2a00, u16::from(index), 0, 0, 0, 0, 0, 1)),
                    6881,
                ),
            );
        }
        let hash = Value::Bytes(vec![5u8; 20]);
        let mut args = vec![(b"info_hash".to_vec(), hash)];
        args.push((
            b"want".to_vec(),
            Value::List(vec![
                Value::Bytes(b"n4".to_vec()),
                Value::Bytes(b"n6".to_vec()),
            ]),
        ));
        let bytes = build_query_response(&query_from(b"get_peers", args), from, &state).unwrap();
        assert!(bytes.len() <= MAX_KRPC_RESPONSE_BYTES);
        assert_eq!(
            decode_all(&bytes)
                .unwrap()
                .get(b"ip")
                .and_then(Value::as_bytes)
                .map(<[u8]>::len),
            Some(18)
        );
    }

    #[test]
    fn reported_ip_accepts_only_compact_v4_and_v6_endpoints() {
        let body = |ip: Option<Value>| {
            Value::Dict(ip.map(|v| vec![(b"ip".to_vec(), v)]).unwrap_or_default())
        };
        assert_eq!(
            reported_external_ip(&body(Some(Value::Bytes(vec![
                93, 184, 216, 34, 0x1a, 0xe1
            ])))),
            Some("93.184.216.34".parse().unwrap())
        );
        let mut v6 = Ipv6Addr::new(0x2a00, 0x1450, 0x4001, 0x80b, 0, 0, 0, 0x200e)
            .octets()
            .to_vec();
        v6.extend_from_slice(&6881u16.to_be_bytes());
        assert_eq!(
            reported_external_ip(&body(Some(Value::Bytes(v6)))),
            Some("2a00:1450:4001:80b::200e".parse().unwrap())
        );
        for bad in [
            Value::Bytes(vec![93, 184, 216, 34]),
            Value::Bytes(vec![0; 5]),
            Value::Bytes(vec![0; 16]),
            Value::Bytes(Vec::new()),
            Value::Int(5),
        ] {
            assert_eq!(reported_external_ip(&body(Some(bad))), None);
        }
        assert_eq!(reported_external_ip(&body(None)), None);
    }

    fn pending_for(
        target: SocketAddr,
        txn: &[u8],
    ) -> (Mutex<PendingMap>, oneshot::Receiver<KrpcResponse>) {
        let (tx, rx) = oneshot::channel();
        let pending = Mutex::new(PendingMap::new());
        pending.lock().insert(
            txn.to_vec(),
            PendingEntry {
                tx,
                target: DhtTarget::Addr(target),
                resolved_addrs: None,
                token: PendingToken::new(),
            },
        );
        (pending, rx)
    }

    fn reply_reporting(txn: &[u8], ip: Option<[u8; 6]>) -> Value {
        let mut fields = vec![
            (
                b"r".to_vec(),
                Value::Dict(vec![(b"id".to_vec(), Value::Bytes(vec![3u8; 20]))]),
            ),
            (b"t".to_vec(), Value::Bytes(txn.to_vec())),
            (b"y".to_vec(), Value::Bytes(b"r".to_vec())),
        ];
        if let Some(ip) = ip {
            fields.push((b"ip".to_vec(), Value::Bytes(ip.to_vec())));
        }
        Value::Dict(fields)
    }

    #[test]
    fn replies_to_our_queries_from_public_nodes_vote_for_the_reported_address() {
        let external = ExternalIp::new();
        let reported = [93, 184, 216, 34, 0x1a, 0xe1];
        for octet in [8u8, 9, 11] {
            let from = SocketAddr::from(([octet, 8, 8, 8], 6881));
            let (pending, mut rx) = pending_for(from, b"aa");
            let msg = reply_reporting(b"aa", Some(reported));
            assert!(deliver_reply(&pending, &external, b"aa", true, msg, from));
            assert!(rx.try_recv().is_ok());
        }
        assert_eq!(
            external.external_v4(),
            Some("93.184.216.34".parse().unwrap())
        );
    }

    #[test]
    fn unmatched_private_error_and_empty_replies_never_vote() {
        let external = ExternalIp::new();
        let reported = [93, 184, 216, 34, 0x1a, 0xe1];
        let public = |a: u8, b: u8| SocketAddr::from(([a, b, 8, 8], 6881));
        let attempt = |from: SocketAddr,
                       target: SocketAddr,
                       reply_txn: &[u8],
                       is_response: bool,
                       ip: Option<[u8; 6]>| {
            let (pending, _rx) = pending_for(target, b"aa");
            let msg = reply_reporting(reply_txn, ip);
            deliver_reply(&pending, &external, reply_txn, is_response, msg, from)
        };
        for from in [public(8, 1), public(9, 2), public(11, 3)] {
            assert!(!attempt(from, from, b"zz", true, Some(reported)));
            assert!(!attempt(from, public(99, 9), b"aa", true, Some(reported)));
            assert!(attempt(from, from, b"aa", false, Some(reported)));
            assert!(attempt(from, from, b"aa", true, Some([10, 0, 0, 5, 0, 1])));
            assert!(attempt(from, from, b"aa", true, None));
        }
        for private in [
            "127.0.0.1:6881",
            "10.1.2.3:6881",
            "192.168.0.7:6881",
            "100.64.0.9:6881",
        ] {
            let from: SocketAddr = private.parse().unwrap();
            assert!(attempt(from, from, b"aa", true, Some(reported)));
        }
        assert_eq!(external.addresses(), Default::default());
    }

    #[test]
    fn inbound_ping_and_unknown_query() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let remote = Id20::from_slice(&[7u8; 20]).unwrap();
        let ping = Value::Dict(vec![
            (
                b"a".to_vec(),
                Value::Dict(vec![(
                    b"id".to_vec(),
                    Value::Bytes(remote.as_bytes().to_vec()),
                )]),
            ),
            (b"q".to_vec(), Value::Bytes(b"ping".to_vec())),
            (b"t".to_vec(), Value::Bytes(b"abc".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        ]);
        let reply = build_query_response(&ping, "8.8.8.8:6000".parse().unwrap(), &state).unwrap();
        let reply = decode_all(&reply).unwrap();
        assert_eq!(
            reply.get(b"t").and_then(Value::as_bytes),
            Some(b"abc" as &[u8])
        );
        assert_eq!(
            reply.get(b"y").and_then(Value::as_bytes),
            Some(b"r" as &[u8])
        );
        assert_eq!(state.routing.lock().len(), 1);

        let mut unknown = ping.clone();
        if let Value::Dict(items) = &mut unknown {
            if let Some((_, value)) = items.iter_mut().find(|(key, _)| key == b"q") {
                *value = Value::Bytes(b"no_such_method".to_vec());
            }
        }
        let reply = decode_all(
            &build_query_response(&unknown, "127.0.0.1:6000".parse().unwrap(), &state).unwrap(),
        )
        .unwrap();
        assert_eq!(
            reply.get(b"y").and_then(Value::as_bytes),
            Some(b"e" as &[u8])
        );
        assert_eq!(
            reply
                .get(b"e")
                .and_then(Value::as_list)
                .and_then(|e| e.first())
                .and_then(Value::as_int),
            Some(204)
        );
    }

    #[test]
    fn inbound_get_peers_token_and_announce() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let remote = Id20::from_slice(&[7u8; 20]).unwrap();
        let hash = Id20::from_slice(&[3u8; 20]).unwrap();
        let from: SocketAddr = "127.0.0.1:6000".parse().unwrap();
        let get = Value::Dict(vec![
            (
                b"a".to_vec(),
                Value::Dict(vec![
                    (b"id".to_vec(), Value::Bytes(remote.as_bytes().to_vec())),
                    (
                        b"info_hash".to_vec(),
                        Value::Bytes(hash.as_bytes().to_vec()),
                    ),
                ]),
            ),
            (b"q".to_vec(), Value::Bytes(b"get_peers".to_vec())),
            (b"t".to_vec(), Value::Bytes(b"g1".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        ]);
        let body = decode_all(&build_query_response(&get, from, &state).unwrap()).unwrap();
        let token = body
            .get(b"r")
            .and_then(|r| r.get(b"token"))
            .and_then(Value::as_bytes)
            .unwrap()
            .to_vec();
        let announce = Value::Dict(vec![
            (
                b"a".to_vec(),
                Value::Dict(vec![
                    (b"id".to_vec(), Value::Bytes(remote.as_bytes().to_vec())),
                    (
                        b"info_hash".to_vec(),
                        Value::Bytes(hash.as_bytes().to_vec()),
                    ),
                    (b"port".to_vec(), Value::Int(6881)),
                    (b"token".to_vec(), Value::Bytes(token)),
                ]),
            ),
            (b"q".to_vec(), Value::Bytes(b"announce_peer".to_vec())),
            (b"t".to_vec(), Value::Bytes(b"a1".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        ]);
        let reply = decode_all(&build_query_response(&announce, from, &state).unwrap()).unwrap();
        assert_eq!(
            reply.get(b"y").and_then(Value::as_bytes),
            Some(b"r" as &[u8])
        );
        assert_eq!(
            state.get_peers(hash, None),
            vec!["127.0.0.1:6881".parse().unwrap()]
        );
    }

    #[test]
    fn inbound_get_peers_values_follow_transport_not_want() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let remote = Id20::from_slice(&[7u8; 20]).unwrap();
        let hash = Id20::from_slice(&[3u8; 20]).unwrap();
        state.add_peer(hash, "192.0.2.1:6881".parse().unwrap());
        state.add_peer(hash, "[2001:db8::1]:6881".parse().unwrap());

        let request = Value::Dict(vec![
            (
                b"a".to_vec(),
                Value::Dict(vec![
                    (b"id".to_vec(), Value::Bytes(remote.as_bytes().to_vec())),
                    (
                        b"info_hash".to_vec(),
                        Value::Bytes(hash.as_bytes().to_vec()),
                    ),
                    (
                        b"want".to_vec(),
                        Value::List(vec![Value::Bytes(b"n6".to_vec())]),
                    ),
                ]),
            ),
            (b"q".to_vec(), Value::Bytes(b"get_peers".to_vec())),
            (b"t".to_vec(), Value::Bytes(b"g6".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        ]);
        let reply = decode_all(
            &build_query_response(&request, "127.0.0.1:6000".parse().unwrap(), &state).unwrap(),
        )
        .unwrap();
        let response = reply.get(b"r").unwrap();
        let values = response.get(b"values").and_then(Value::as_list).unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].as_bytes().unwrap().len(), 6);
        assert!(response.get(b"nodes6").is_some());
        assert!(response.get(b"nodes").is_none());
    }

    #[test]
    fn inbound_get_peers_response_stays_within_udp_budget() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let remote = Id20::from_slice(&[7u8; 20]).unwrap();
        let hash = Id20::from_slice(&[3u8; 20]).unwrap();
        for port in 1..2000u16 {
            state.add_peer(
                hash,
                SocketAddr::new(Ipv4Addr::new(192, 0, 2, 1).into(), port),
            );
        }
        let request = Value::Dict(vec![
            (
                b"a".to_vec(),
                Value::Dict(vec![
                    (b"id".to_vec(), Value::Bytes(remote.as_bytes().to_vec())),
                    (
                        b"info_hash".to_vec(),
                        Value::Bytes(hash.as_bytes().to_vec()),
                    ),
                    (
                        b"want".to_vec(),
                        Value::List(vec![Value::Bytes(b"n4".to_vec())]),
                    ),
                ]),
            ),
            (b"q".to_vec(), Value::Bytes(b"get_peers".to_vec())),
            (b"t".to_vec(), Value::Bytes(b"aa".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        ]);
        let encoded =
            build_query_response(&request, "127.0.0.1:6881".parse().unwrap(), &state).unwrap();
        assert!(encoded.len() <= MAX_KRPC_RESPONSE_BYTES);
        assert!(decode_all(&encoded).unwrap().get(b"r").is_some());
    }

    #[test]
    fn private_hashes_are_not_stored() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let hash = Id20::from_slice(&[3u8; 20]).unwrap();
        state.set_private(hash, true);
        state.add_peer(hash, "127.0.0.1:6881".parse().unwrap());
        assert!(state.get_peers(hash, None).is_empty());
    }

    #[tokio::test]
    async fn a_private_hash_never_runs_a_lookup_or_reports_an_announce() {
        let dht = Dht::spawn().await.expect("bind DHT sockets");
        let hash = Id20::from_slice(&[3u8; 20]).unwrap();
        dht.set_private(hash, true);
        let (mut peers, sent) = dht.get_peers_announced(hash, Duration::from_secs(5), Some(6881));
        let first = tokio::time::timeout(Duration::from_secs(5), peers.recv())
            .await
            .expect("the stream closes at once");
        assert_eq!(first, None);
        let sent = tokio::time::timeout(Duration::from_secs(5), sent)
            .await
            .expect("the outcome settles at once");
        assert!(!sent.unwrap_or(false));
        assert_eq!(dht.active_lookup_count(), 0);
    }

    #[test]
    fn unknown_get_peers_does_not_create_empty_store_bucket() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let hash = Id20::from_slice(&[3u8; 20]).unwrap();

        assert!(state.get_peers(hash, None).is_empty());
        assert!(state.peers.lock().is_empty());
    }

    #[test]
    fn private_hashes_follow_normal_dht_response_without_storage() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let remote = Id20::from_slice(&[7u8; 20]).unwrap();
        let hash = Id20::from_slice(&[3u8; 20]).unwrap();
        let from: SocketAddr = "8.8.8.8:6000".parse().unwrap();
        state.set_private(hash, true);

        let token = state.token(from);
        for (query, tid) in [
            (b"get_peers".as_slice(), b"g1".as_slice()),
            (b"announce_peer".as_slice(), b"a1".as_slice()),
        ] {
            let request = Value::Dict(vec![
                (
                    b"a".to_vec(),
                    Value::Dict(vec![
                        (b"id".to_vec(), Value::Bytes(remote.as_bytes().to_vec())),
                        (
                            b"info_hash".to_vec(),
                            Value::Bytes(hash.as_bytes().to_vec()),
                        ),
                        (b"port".to_vec(), Value::Int(6881)),
                        (b"token".to_vec(), Value::Bytes(token.clone())),
                    ]),
                ),
                (b"q".to_vec(), Value::Bytes(query.to_vec())),
                (b"t".to_vec(), Value::Bytes(tid.to_vec())),
                (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
            ]);
            let reply = decode_all(&build_query_response(&request, from, &state).unwrap()).unwrap();
            assert_eq!(
                reply.get(b"y").and_then(Value::as_bytes),
                Some(b"r" as &[u8])
            );
            assert!(reply.get(b"e").is_none());
        }
        assert_eq!(state.routing.lock().len(), 1);
        assert!(state.peers.lock().is_empty());
    }

    #[test]
    fn public_dht_endpoint_rejects_cgnat_and_special_use_ranges() {
        for ip in [
            "0.0.0.1",
            "100.64.0.1",
            "100.127.255.254",
            "100::1",
            "192.0.0.1",
            "192.31.196.1",
            "192.52.193.1",
            "192.88.99.1",
            "192.175.48.1",
            "198.18.0.1",
            "239.255.255.1",
            "240.0.0.1",
        ] {
            assert!(
                !public_dht_endpoint(SocketAddr::new(ip.parse().unwrap(), 6881)),
                "special-use address {ip}"
            );
        }
        assert!(public_dht_endpoint("8.8.8.8:6881".parse().unwrap()));
    }

    #[test]
    fn announce_peer_treats_any_nonzero_implied_port_as_implied() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let remote = Id20::from_slice(&[7u8; 20]).unwrap();
        let hash = Id20::from_slice(&[3u8; 20]).unwrap();
        let from: SocketAddr = "8.8.4.4:6000".parse().unwrap();
        for (implied, expected_port) in [(0, 6881), (1, 6000), (2, 6000), (-1, 6000)] {
            let state =
                InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
            let token = state.token(from);
            let request = Value::Dict(vec![
                (
                    b"a".to_vec(),
                    Value::Dict(vec![
                        (b"id".to_vec(), Value::Bytes(remote.as_bytes().to_vec())),
                        (b"implied_port".to_vec(), Value::Int(implied)),
                        (
                            b"info_hash".to_vec(),
                            Value::Bytes(hash.as_bytes().to_vec()),
                        ),
                        (b"port".to_vec(), Value::Int(6881)),
                        (b"token".to_vec(), Value::Bytes(token)),
                    ]),
                ),
                (b"q".to_vec(), Value::Bytes(b"announce_peer".to_vec())),
                (b"t".to_vec(), Value::Bytes(b"imp".to_vec())),
                (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
            ]);
            let reply = decode_all(&build_query_response(&request, from, &state).unwrap()).unwrap();
            assert_eq!(
                reply.get(b"y").and_then(Value::as_bytes),
                Some(b"r" as &[u8]),
                "implied_port={implied}"
            );
            assert_eq!(
                state.get_peers(hash, None),
                vec![SocketAddr::new(from.ip(), expected_port)],
                "implied_port={implied}"
            );
        }
    }

    #[test]
    fn compact_node_parser_rejects_bad_tail_and_zero_port() {
        let id = [1u8; 20];
        let mut bytes = id.to_vec();
        bytes.extend_from_slice(&[1, 2, 3, 4, 0x1a, 0xe1]);
        bytes.extend_from_slice(&[0xff]);
        assert_eq!(parse_compact_nodes(&bytes, false).len(), 1);
        bytes[24] = 0;
        bytes[25] = 0;
        assert!(parse_compact_nodes(&bytes[..26], false).is_empty());
    }

    #[tokio::test]
    async fn udp_runtime_answers_ping() {
        let dht = Dht::spawn().await.expect("bind DHT sockets");
        let port = dht.local_port().expect("IPv4 DHT port");
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let remote = Id20::from_slice(&[4u8; 20]).unwrap();
        let request = Value::Dict(vec![
            (
                b"a".to_vec(),
                Value::Dict(vec![(
                    b"id".to_vec(),
                    Value::Bytes(remote.as_bytes().to_vec()),
                )]),
            ),
            (b"q".to_vec(), Value::Bytes(b"ping".to_vec())),
            (b"t".to_vec(), Value::Bytes(b"zz".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        ]);
        client
            .send_to(&encode_to_vec(&request), (Ipv4Addr::LOCALHOST, port))
            .await
            .unwrap();
        let mut buf = [0u8; 512];
        let (n, _) = tokio::time::timeout(Duration::from_secs(1), client.recv_from(&mut buf))
            .await
            .unwrap()
            .unwrap();
        let response = decode_all(&buf[..n]).unwrap();
        assert_eq!(
            response.get(b"y").and_then(Value::as_bytes),
            Some(b"r" as &[u8])
        );
        assert_eq!(
            response.get(b"t").and_then(Value::as_bytes),
            Some(b"zz" as &[u8])
        );
        dht.shutdown().await;
    }

    #[test]
    fn inbound_limiter_blocks_bursts_and_recovers() {
        let t0 = Instant::now();
        let mut limiter = InboundLimiter::new(t0);
        let ip: IpAddr = "192.0.2.9".parse().unwrap();
        let allowed = (0..50).filter(|_| limiter.allow(ip, t0)).count();
        assert_eq!(allowed, QUERY_BURST as usize);
        assert!(!limiter.allow(ip, t0 + Duration::from_secs(60)));
        assert!(limiter.allow(ip, t0 + QUERY_BLOCK + Duration::from_secs(1)));
        assert!(limiter.allow("192.0.2.10".parse().unwrap(), t0));
        assert!(limiter.take_reply_budget(1024, t0));
        assert!(!limiter.take_reply_budget(64 * 1024, t0));
    }

    #[test]
    fn malformed_query_replies_share_the_global_reply_budget() {
        let t0 = Instant::now();
        let packet = b"d1:t2:ab1:y1:qXe";
        let ip: IpAddr = "192.0.2.9".parse().unwrap();

        let mut limiter = InboundLimiter::new(t0);
        assert!(malformed_query_reply(packet, ip, &mut limiter, t0).is_some());
        assert!(malformed_query_reply(b"d1:t2:ab1:y1:rXe", ip, &mut limiter, t0).is_none());

        let mut limiter = InboundLimiter::new(t0);
        assert!(limiter.take_reply_budget(REPLY_BYTES_PER_SEC as usize, t0));
        let other: IpAddr = "192.0.2.10".parse().unwrap();
        assert!(malformed_query_reply(packet, other, &mut limiter, t0).is_none());
        assert!(
            malformed_query_reply(packet, other, &mut limiter, t0 + Duration::from_secs(1))
                .is_some()
        );
    }

    #[test]
    fn announce_targets_keep_one_entry_per_family_for_a_shared_distance() {
        let distance = Id20::from_slice(&[5u8; 20]).unwrap();
        let v4 = DhtTarget::Addr("198.51.100.1:6881".parse().unwrap());
        let v6 = DhtTarget::Addr("[2001:db8::1]:6881".parse().unwrap());
        let mut targets = AnnounceTargets::new();
        record_announce_target(&mut targets, distance, v4.clone(), vec![4], false);
        record_announce_target(&mut targets, distance, v6.clone(), vec![6], false);
        assert_eq!(targets.len(), 2);
        let first = &targets[&(distance, false)];
        assert_eq!((&first.target, &first.token), (&v4, &vec![4]));
        let second = &targets[&(distance, true)];
        assert_eq!((&second.target, &second.token), (&v6, &vec![6]));
    }

    #[test]
    fn announce_targets_prune_each_family_independently() {
        let mut targets = AnnounceTargets::new();
        for i in 0..(K * 2 + 4) as u8 {
            let mut id = [0u8; 20];
            id[19] = i + 1;
            let distance = Id20::from_slice(&id).unwrap();
            let v4 = DhtTarget::Addr(SocketAddr::from(([198, 51, 100, i + 1], 6881)));
            record_announce_target(&mut targets, distance, v4, vec![i], false);
        }
        let far = Id20::from_slice(&[0xffu8; 20]).unwrap();
        let v6 = DhtTarget::Addr("[2001:db8::1]:6881".parse().unwrap());
        record_announce_target(&mut targets, far, v6, vec![9], false);

        let v4_count = targets.keys().filter(|(_, v6)| !*v6).count();
        let v6_count = targets.keys().filter(|(_, v6)| *v6).count();
        assert_eq!(v4_count, K * 2);
        assert_eq!(v6_count, 1);
    }

    #[tokio::test]
    async fn seed_selection_skips_ipv6_contacts_during_backoff() {
        let dht = Dht::spawn().await.expect("bind DHT sockets");
        let near = Id20::from_slice(&[1u8; 20]).unwrap();
        let v4: SocketAddr = "198.51.100.7:6881".parse().unwrap();
        let v6: SocketAddr = "[2001:db8::7]:6881".parse().unwrap();
        dht.add_routing(near, v4);
        dht.add_routing(Id20::from_slice(&[2u8; 20]).unwrap(), v6);
        *dht.v6_unreachable_since.lock() = Some(Instant::now());

        let seeds = dht.closest_routing(&near, K * 2);
        assert_eq!(seeds, vec![v4]);
        dht.shutdown().await;
    }

    #[tokio::test]
    async fn refresh_routing_respects_the_limit_across_both_tables() {
        let dht = Dht::spawn().await.expect("bind DHT sockets");
        for i in 1..=3u8 {
            let mut id = [0u8; 20];
            id[0] = 0x80;
            id[19] = i;
            dht.add_routing(
                Id20::from_slice(&id).unwrap(),
                SocketAddr::from(([198, 51, 100, i], 6881)),
            );
            id[0] = 0x40;
            dht.add_routing(
                Id20::from_slice(&id).unwrap(),
                format!("[2001:db8:{i}::1]:6881").parse().unwrap(),
            );
        }
        for table in [&dht.routing, &dht.routing6] {
            for bucket in &mut table.lock().buckets {
                for node in &mut bucket.nodes {
                    node.last_seen = Instant::now() - ROUTING_STALE;
                }
            }
        }

        let refreshed = dht.refresh_routing(4);
        assert_eq!(refreshed.len(), 4);
        assert_eq!(refreshed.iter().filter(|(_, a)| a.is_ipv6()).count(), 1);
        assert_eq!(dht.refresh_routing(4).len(), 2);
        dht.shutdown().await;
    }

    #[test]
    fn peer_store_caps_per_ip_and_per_hash() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let hash = Id20::from_slice(&[3u8; 20]).unwrap();
        let other = Id20::from_slice(&[4u8; 20]).unwrap();
        state.add_peer(other, "198.51.100.1:1000".parse().unwrap());
        for port in 1000..1100 {
            state.add_peer(hash, SocketAddr::from(([203, 0, 113, 5], port)));
        }
        assert_eq!(state.get_peers(hash, None).len(), MAX_PEERS_PER_IP);
        for i in 0..1000u32 {
            let b = i.to_be_bytes();
            state.add_peer(hash, SocketAddr::from(([10, b[1], b[2], b[3]], 6881)));
        }
        assert_eq!(state.get_peers(hash, None).len(), MAX_PEERS_PER_HASH);
        assert_eq!(state.get_peers(other, None).len(), 1);
    }

    #[test]
    fn undecodable_non_query_packets_get_no_error_reply() {
        assert!(is_query_packet(b"d1:t2:ab1:y1:q"));
        assert!(!is_query_packet(b"d1:t2:ab1:y1:r"));
    }

    #[test]
    fn read_only_flag_is_set_for_proxied_queries() {
        let id = Id20::from_slice(&[1u8; 20]).unwrap();
        let ro = decode_all(&build_get_peers_bytes(b"ab", &id, &id, Want::V4, true)).unwrap();
        assert_eq!(ro.get(b"ro").and_then(Value::as_int), Some(1));
        let rw = decode_all(&build_get_peers_bytes(b"ab", &id, &id, Want::V4, false)).unwrap();
        assert!(rw.get(b"ro").is_none());
    }

    #[test]
    fn routing_ignores_hearsay_and_keeps_fresh_nodes_in_full_buckets() {
        let our_id = Id20([0u8; 20]);
        let far = |n: u8| {
            let mut id = [0u8; 20];
            id[0] = 0x80;
            id[19] = n;
            Id20(id)
        };
        let addr = |n: u8| SocketAddr::from(([8, 8, 8, n], 6881));
        let mut table = RoutingTable::new(our_id);
        for n in 1..=BUCKET_SIZE as u8 {
            table.add(far(n), addr(n));
        }
        table.add_with_liveness(far(100), addr(100), RoutingLiveness::Questionable);
        table.add_with_liveness(far(1), addr(200), RoutingLiveness::Questionable);
        table.add(far(101), addr(101));
        let nodes: Vec<_> = table.buckets.iter().flat_map(|b| b.nodes.iter()).collect();
        assert_eq!(nodes.len(), BUCKET_SIZE);
        assert!(nodes.iter().all(|n| n.liveness == RoutingLiveness::Good));
        assert!(nodes.iter().any(|n| n.id == far(1) && n.addr == addr(1)));

        let bucket = table
            .buckets
            .iter_mut()
            .find(|b| !b.nodes.is_empty())
            .unwrap();
        bucket.nodes[0].liveness = RoutingLiveness::Bad;
        let dead = bucket.nodes[0].id;
        table.add_with_liveness(dead, addr(250), RoutingLiveness::Questionable);
        let state = table
            .buckets
            .iter()
            .flat_map(|b| b.nodes.iter())
            .find(|n| n.id == dead)
            .map(|n| n.liveness);
        assert_eq!(state, Some(RoutingLiveness::Bad));
    }

    #[test]
    fn tokens_expire_after_ten_minutes_when_the_node_was_idle() {
        let addr: SocketAddr = "8.8.8.8:6000".parse().unwrap();
        let mut tokens = TokenState::new();
        let token = tokens.token_for(addr);
        assert!(tokens.valid(addr, &token));
        let Some(rotated) = Instant::now().checked_sub(TOKEN_ROTATION * 4) else {
            return;
        };
        tokens.rotated = rotated;
        assert!(!tokens.valid(addr, &token));
    }

    #[test]
    fn token_rotation_keeps_its_schedule_after_a_late_call() {
        let addr: SocketAddr = "8.8.8.8:6000".parse().unwrap();
        let mut tokens = TokenState::new();
        let token = tokens.token_for(addr);
        tokens.rotated = Instant::now() - Duration::from_secs(9 * 60);
        assert!(tokens.valid(addr, &token));
        tokens.rotated -= Duration::from_secs(61);
        assert!(!tokens.valid(addr, &token));
    }

    #[test]
    fn ipv4_mapped_ipv6_contacts_are_dropped_from_responses() {
        let mapped: Ipv6Addr = "::ffff:8.8.8.8".parse().unwrap();
        let global: Ipv6Addr = "2001:4860:4860::8888".parse().unwrap();
        let mut nodes6 = Vec::new();
        let mut values = Vec::new();
        for ip in [mapped, global] {
            nodes6.extend_from_slice(&[0x44u8; 20]);
            nodes6.extend_from_slice(&ip.octets());
            nodes6.extend_from_slice(&6881u16.to_be_bytes());
            let mut peer = ip.octets().to_vec();
            peer.extend_from_slice(&6881u16.to_be_bytes());
            values.push(Value::Bytes(peer));
        }
        let body = Value::Dict(vec![(
            b"r".to_vec(),
            Value::Dict(vec![
                (b"id".to_vec(), Value::Bytes(vec![0u8; 20])),
                (b"nodes6".to_vec(), Value::Bytes(nodes6)),
                (b"values".to_vec(), Value::List(values)),
            ]),
        )]);
        let (_, peers, nodes, _) = parse_get_peers_response(&body).unwrap();
        let expected = SocketAddr::V6(SocketAddrV6::new(global, 6881, 0, 0));
        assert_eq!(peers, vec![expected]);
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].1, expected);
        assert!(!public_dht_endpoint(
            "[::ffff:8.8.8.8]:6881".parse().unwrap()
        ));
        assert!(!public_dht_endpoint("[::8.8.8.8]:6881".parse().unwrap()));
    }

    fn far_id(first: u8, last: u8) -> Id20 {
        let mut id = [0u8; 20];
        id[0] = first;
        id[19] = last;
        Id20(id)
    }

    #[test]
    fn routing_table_keeps_one_node_per_ip_across_buckets() {
        let mut table = RoutingTable::new(Id20([0u8; 20]));
        let host: SocketAddr = "8.8.8.8:6881".parse().unwrap();
        for last in 1..=BUCKET_SIZE as u8 {
            table.add(far_id(0x80, last), host);
        }
        table.add(far_id(0x40, 1), host);
        assert_eq!(table.len(), 1);

        let other: SocketAddr = "9.9.9.9:6881".parse().unwrap();
        table.add(far_id(0x80, 100), other);
        table.add(
            far_id(0x80, 101),
            "[2001:4860:4860::8888]:6881".parse().unwrap(),
        );
        assert_eq!(table.len(), 3);
    }

    #[test]
    fn routing_table_groups_ipv6_contacts_by_prefix() {
        let mut table = RoutingTable::new(Id20([0u8; 20]));
        for last in 1..=4u8 {
            table.add(
                far_id(0x80, last),
                format!("[2001:4860:4860::{last}]:6881").parse().unwrap(),
            );
        }
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn verified_response_from_the_same_endpoint_replaces_the_old_id() {
        let mut table = RoutingTable::new(Id20([0u8; 20]));
        let host: SocketAddr = "8.8.8.8:6881".parse().unwrap();
        table.add(far_id(0x80, 1), host);
        table.add(far_id(0x80, 2), host);
        assert_eq!(table.snapshot(), vec![(far_id(0x80, 2), host)]);
    }

    #[test]
    fn same_ip_on_another_port_does_not_displace_a_good_node() {
        let mut table = RoutingTable::new(Id20([0u8; 20]));
        let first: SocketAddr = "8.8.8.8:6881".parse().unwrap();
        let second: SocketAddr = "8.8.8.8:6882".parse().unwrap();
        table.add(far_id(0x80, 1), first);
        table.add(far_id(0x80, 2), second);
        table.add_with_liveness(far_id(0x80, 3), second, RoutingLiveness::Questionable);
        assert_eq!(table.snapshot(), vec![(far_id(0x80, 1), first)]);
    }

    #[test]
    fn questionable_node_is_replaced_by_a_verified_node_on_the_same_ip() {
        let mut table = RoutingTable::new(Id20([0u8; 20]));
        let first: SocketAddr = "8.8.8.8:6881".parse().unwrap();
        let second: SocketAddr = "8.8.8.8:6882".parse().unwrap();
        table.add_with_liveness(far_id(0x80, 1), first, RoutingLiveness::Questionable);
        table.add(far_id(0x80, 2), second);
        assert_eq!(table.snapshot(), vec![(far_id(0x80, 2), second)]);
    }

    #[test]
    fn good_node_is_not_repointed_to_a_claimant_address() {
        let mut table = RoutingTable::new(Id20([0u8; 20]));
        let real: SocketAddr = "8.8.8.8:6881".parse().unwrap();
        let attacker: SocketAddr = "9.9.9.9:6881".parse().unwrap();
        let victim = far_id(0x80, 1);
        table.add(victim, real);
        table.add(victim, attacker);
        assert_eq!(table.snapshot(), vec![(victim, real)]);

        table.buckets[0].nodes[0].liveness = RoutingLiveness::Bad;
        table.add(victim, attacker);
        assert_eq!(table.snapshot(), vec![(victim, attacker)]);
    }

    #[test]
    fn inbound_replies_do_not_relay_unverified_nodes() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let find_node = |from: &str, id: u8| {
            let request = Value::Dict(vec![
                (
                    b"a".to_vec(),
                    Value::Dict(vec![
                        (b"id".to_vec(), Value::Bytes(vec![id; 20])),
                        (b"target".to_vec(), Value::Bytes(vec![3u8; 20])),
                    ]),
                ),
                (b"q".to_vec(), Value::Bytes(b"find_node".to_vec())),
                (b"t".to_vec(), Value::Bytes(b"fn".to_vec())),
                (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
            ]);
            let reply =
                decode_all(&build_query_response(&request, from.parse().unwrap(), &state).unwrap())
                    .unwrap();
            let nodes = reply
                .get(b"r")
                .and_then(|r| r.get(b"nodes"))
                .and_then(Value::as_bytes)
                .unwrap()
                .to_vec();
            parse_compact_nodes(&nodes, false)
        };
        assert!(find_node("8.8.8.8:6000", 7).is_empty());
        assert!(find_node("9.9.9.9:6000", 8).is_empty());
        assert_eq!(state.routing.lock().len(), 2);

        let verified: SocketAddr = "1.1.1.1:6881".parse().unwrap();
        state.routing.lock().add(Id20([5u8; 20]), verified);
        let nodes = find_node("9.9.9.9:6000", 8);
        assert_eq!(nodes, vec![(Id20([5u8; 20]), verified)]);
    }

    #[test]
    fn inbound_limiter_groups_ipv6_sources_by_prefix() {
        let t0 = Instant::now();
        let mut limiter = InboundLimiter::new(t0);
        let allowed = (0..100u16)
            .filter(|n| {
                let ip: IpAddr = format!("2001:4860:1:2::{n:x}").parse().unwrap();
                limiter.allow(ip, t0)
            })
            .count();
        assert_eq!(allowed, QUERY_BURST as usize);
        assert!(limiter.allow("2001:4860:1:3::1".parse().unwrap(), t0));
    }

    #[test]
    fn inbound_limiter_admits_new_sources_when_the_table_is_full() {
        let t0 = Instant::now();
        let mut limiter = InboundLimiter::new(t0);
        for n in 0..MAX_TRACKED_QUERY_IPS as u32 {
            let ip = IpAddr::V4(Ipv4Addr::from(0x0800_0000 + n));
            for _ in 0..QUERY_BURST as usize + 1 {
                limiter.allow(ip, t0);
            }
        }
        assert!(limiter.allow("203.0.113.9".parse().unwrap(), t0));
        assert!(limiter.ips.len() <= MAX_TRACKED_QUERY_IPS);
    }

    #[test]
    fn a_full_limiter_table_evicts_unblocked_sources_before_blocked_ones() {
        let t0 = Instant::now();
        let mut limiter = InboundLimiter::new(t0);
        let blocked: IpAddr = "192.0.2.9".parse().unwrap();
        for _ in 0..QUERY_BURST as usize + 1 {
            limiter.allow(blocked, t0);
        }
        let later = t0 + Duration::from_secs(5);
        for n in 1..MAX_TRACKED_QUERY_IPS as u32 {
            limiter.allow(IpAddr::V4(Ipv4Addr::from(0x0800_0000 + n)), later);
        }
        assert_eq!(limiter.ips.len(), MAX_TRACKED_QUERY_IPS);
        assert!(limiter.allow("203.0.113.9".parse().unwrap(), later));
        assert!(!limiter.allow(blocked, later));
    }

    #[test]
    fn version_bytes_follow_the_package_version_and_clamp_at_255() {
        assert_eq!(version_byte("0"), 0);
        assert_eq!(version_byte("254"), 254);
        assert_eq!(version_byte("4000"), 255);
        let major = parse_version_part(env!("CARGO_PKG_VERSION_MAJOR")).min(255) as u8;
        let minor = parse_version_part(env!("CARGO_PKG_VERSION_MINOR")).min(255) as u8;
        assert_eq!(DHT_VERSION[2..], [major, minor]);
    }

    #[test]
    fn repeated_failures_demote_a_node_to_questionable_then_bad() {
        let mut table = RoutingTable::new(Id20([0u8; 20]));
        let addr: SocketAddr = "8.8.8.8:6881".parse().unwrap();
        let id = far_id(0x80, 1);
        let target = far_id(0x80, 9);
        table.add(id, addr);
        assert_eq!(table.closest_good_nodes(&target, K).len(), 1);

        table.record_failure(addr);
        assert!(table.closest_good_nodes(&target, K).is_empty());
        assert_eq!(table.closest_nodes(&target, K).len(), 1);

        table.add(id, addr);
        table.record_failure(addr);
        assert_eq!(table.closest_nodes(&target, K).len(), 1);
        table.record_failure(addr);
        assert!(table.closest_nodes(&target, K).is_empty());
        assert!(table.snapshot().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn unanswered_lookups_and_pings_demote_the_target() {
        let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = silent.local_addr().unwrap();
        let dht = Dht::spawn().await.expect("bind DHT sockets");
        let id = far_id(0x80, 1);
        let hash = far_id(0x80, 9);
        dht.add_routing(id, addr);
        assert_eq!(dht.routing.lock().closest_good_nodes(&hash, K).len(), 1);

        assert!(dht
            .clone()
            .query_get_peers(DhtTarget::Addr(addr), hash)
            .await
            .is_none());
        assert!(dht.routing.lock().closest_good_nodes(&hash, K).is_empty());
        assert_eq!(dht.routing.lock().closest_nodes(&hash, K).len(), 1);

        assert!(!dht.ping_node(addr, Some(id)).await);
        assert!(dht.routing.lock().closest_nodes(&hash, K).is_empty());
        dht.shutdown().await;
    }

    #[test]
    fn announce_targets_skip_ipv6_without_a_listener() {
        let mut targets = AnnounceTargets::new();
        for i in 0..(K + 2) as u8 {
            let mut id = [0u8; 20];
            id[19] = i + 1;
            let v4 = DhtTarget::Addr(SocketAddr::from(([198, 51, 100, i + 1], 6881)));
            record_announce_target(&mut targets, Id20(id), v4, vec![4], false);
            id[18] = 1;
            let v6 = DhtTarget::Addr(
                format!("[2001:4860:4860::{:x}]:6881", i + 1)
                    .parse()
                    .unwrap(),
            );
            record_announce_target(&mut targets, Id20(id), v6, vec![6], false);
        }
        let without = select_announce_targets(targets.clone(), false);
        assert_eq!(without.len(), K);
        assert!(without.iter().all(|(target, _)| !target_is_v6(target)));
        let with = select_announce_targets(targets, true);
        assert_eq!(with.len(), K * 2);
        assert_eq!(with.iter().filter(|(t, _)| target_is_v6(t)).count(), K);
    }

    fn free_udp_port() -> u16 {
        std::net::UdpSocket::bind("0.0.0.0:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[tokio::test]
    async fn configured_port_is_bound_on_both_address_families() {
        let port = free_udp_port();
        let options = DhtOptions {
            port: Some(port),
            ..Default::default()
        };
        let dht = Dht::spawn_with_options(None, options).await.unwrap();
        assert_eq!(dht.local_port_for(false), Some(port));
        if dht.sock6.is_some() {
            assert_eq!(dht.local_port_for(true), Some(port));
        }
        dht.shutdown().await;
    }

    #[tokio::test]
    async fn busy_configured_port_falls_back_to_an_ephemeral_one() {
        let holder = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let port = holder.local_addr().unwrap().port();
        let options = DhtOptions {
            port: Some(port),
            ..Default::default()
        };
        let dht = Dht::spawn_with_options(None, options).await.unwrap();
        let bound = dht.local_port_for(false).unwrap();
        assert_ne!(bound, port);
        assert_ne!(bound, 0);
        dht.shutdown().await;
    }

    #[tokio::test]
    async fn shared_dht_restores_routing_state_after_a_restart() {
        let lock = SHARED_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
        let _guard = lock.lock().await;
        reset_shared_state().await;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        let options = DhtOptions {
            routing_state_path: Some(path.clone()),
            ..Default::default()
        };
        let contact = (
            Id20::from_slice(&[2u8; 20]).unwrap(),
            "8.8.8.8:6881".parse::<SocketAddr>().unwrap(),
        );

        let dht = Dht::shared_with_proxy(None, options.clone()).await.unwrap();
        dht.add_bootstrap_nodes([contact]);
        dht.persist_routing_state().await;
        assert!(path.exists());
        reset_shared_state().await;

        let restarted = Dht::shared_with_proxy(None, options).await.unwrap();
        assert!(restarted.routing_snapshot().contains(&contact));
        reset_shared_state().await;
    }

    #[tokio::test]
    async fn installing_the_shared_dht_with_other_options_rebuilds_it() {
        let lock = SHARED_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
        let _guard = lock.lock().await;
        reset_shared_state().await;

        let first = Dht::shared_with_proxy(None, DhtOptions::default())
            .await
            .unwrap();
        let again = Dht::shared_with_proxy(None, DhtOptions::default())
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &again));
        let port = free_udp_port();
        let configured = DhtOptions {
            port: Some(port),
            ..Default::default()
        };
        let rebuilt = Dht::shared_with_proxy(None, configured).await.unwrap();
        assert!(!Arc::ptr_eq(&first, &rebuilt));
        assert_eq!(rebuilt.local_port_for(false), Some(port));
        reset_shared_state().await;
    }

    #[tokio::test]
    async fn route_swap_keeps_the_configured_routing_state_path() {
        let lock = SHARED_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
        let _guard = lock.lock().await;
        reset_shared_state().await;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        let options = DhtOptions {
            routing_state_path: Some(path.clone()),
            ..Default::default()
        };
        Dht::shared_with_proxy(None, options.clone()).await.unwrap();
        let rebuilt = DhtOptions {
            port: Some(free_udp_port()),
            ..options
        };
        let swap = Dht::prepare_shared_with_proxy(None, rebuilt.clone())
            .await
            .unwrap();
        let next = swap.next().unwrap();
        assert_eq!(next.routing_state_path.as_ref(), Some(&path));
        assert_eq!(next.local_port_for(false), rebuilt.port);
        swap.commit().await;
        reset_shared_state().await;
    }

    #[tokio::test]
    async fn a_route_swap_prepared_before_the_session_installs_the_configured_options() {
        let lock = SHARED_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
        let _guard = lock.lock().await;
        reset_shared_state().await;
        within_shared_test(async {
            let dir = tempfile::tempdir().unwrap();
            let options = DhtOptions {
                port: Some(free_udp_port()),
                routing_state_path: Some(dir.path().join("dht-routing.state")),
            };
            let swap = Dht::prepare_shared_with_proxy(None, options.clone())
                .await
                .unwrap();
            swap.commit().await;

            let reused = Dht::shared_with_proxy(None, options.clone()).await.unwrap();
            assert_eq!(reused.local_port_for(false), options.port);
            assert_eq!(reused.routing_state_path, options.routing_state_path);
            assert_eq!(shared_dht_cell().lock().await.options, options);
        })
        .await;
        reset_shared_state().await;
    }

    #[tokio::test]
    async fn a_rolled_back_route_swap_restores_the_previous_options() {
        let lock = SHARED_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
        let _guard = lock.lock().await;
        reset_shared_state().await;
        within_shared_test(async {
            let original = DhtOptions {
                port: Some(free_udp_port()),
                ..Default::default()
            };
            let previous = Dht::shared_with_proxy(None, original.clone())
                .await
                .unwrap();
            let swap = Dht::prepare_shared_with_proxy(
                None,
                DhtOptions {
                    port: Some(free_udp_port()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            let restored = swap.rollback().await.unwrap();
            assert!(Arc::ptr_eq(&restored, &previous));
            assert_eq!(shared_dht_cell().lock().await.options, original);
        })
        .await;
        reset_shared_state().await;
    }

    #[tokio::test]
    async fn a_direct_to_direct_route_swap_keeps_the_running_dht_and_its_port() {
        let lock = SHARED_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
        let _guard = lock.lock().await;
        reset_shared_state().await;
        within_shared_test(async {
            let options = DhtOptions {
                port: Some(free_udp_port()),
                ..Default::default()
            };
            let previous = Dht::shared_with_proxy(None, options.clone()).await.unwrap();
            let swap = Dht::prepare_shared_with_proxy(None, options.clone())
                .await
                .unwrap();
            let next = swap.next().unwrap();
            assert!(Arc::ptr_eq(&next, &previous));
            assert_eq!(next.local_port_for(false), options.port);
            swap.commit().await;
            assert!(!previous.shutdown.load(Ordering::Acquire));

            let swap = Dht::prepare_shared_with_proxy(None, options).await.unwrap();
            let restored = swap.rollback().await.unwrap();
            assert!(Arc::ptr_eq(&restored, &previous));
            assert!(!previous.shutdown.load(Ordering::Acquire));
            assert!(Arc::ptr_eq(
                &Dht::current_shared().await.unwrap(),
                &previous
            ));
        })
        .await;
        reset_shared_state().await;
    }

    #[tokio::test(start_paused = true)]
    async fn routing_state_is_saved_periodically() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        let options = DhtOptions {
            routing_state_path: Some(path.clone()),
            ..Default::default()
        };
        let dht = Dht::spawn_with_options(None, options).await.unwrap();
        dht.add_bootstrap_nodes([(
            Id20::from_slice(&[2u8; 20]).unwrap(),
            "8.8.8.8:6881".parse().unwrap(),
        )]);
        assert!(!path.exists());

        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(ROUTING_PERSIST_INTERVAL + ROUTING_REFRESH_INTERVAL).await;
        for _ in 0..500 {
            if path.exists() {
                break;
            }
            tokio::task::yield_now().await;
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(path.exists());
        assert_eq!(load_routing_state_file(&path).unwrap().contacts.len(), 1);
        drop(dht);
    }

    #[tokio::test]
    async fn empty_routing_table_does_not_overwrite_saved_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        let contact = (
            Id20::from_slice(&[1u8; 20]).unwrap(),
            "8.8.8.8:6881".parse::<SocketAddr>().unwrap(),
        );
        save_routing_state_file(&path, &[contact], &SavedIds::default()).unwrap();
        let before = std::fs::read(&path).unwrap();

        let dht = Dht::spawn_with_routing_state(path.clone()).await.unwrap();
        dht.routing.lock().buckets[0].nodes.clear();
        dht.routing6.lock().buckets[0].nodes.clear();
        dht.shutdown().await;
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[tokio::test]
    async fn corrupt_routing_state_does_not_prevent_startup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        std::fs::write(&path, [0xff, 0xfe, 0x00, 0x9f]).unwrap();
        let dht = Dht::spawn_with_routing_state(path).await.unwrap();
        assert_eq!(dht.routing_table_len(), 0);
        dht.shutdown().await;
    }

    #[test]
    fn proxied_hostnames_must_not_look_local() {
        for host in [
            "router.bittorrent.com",
            "dht.libtorrent.org",
            "xn--bcher-kva.example",
            "Router.BitTorrent.com.",
            "8.8.8.8",
            "2001:4860:4860::8888",
        ] {
            assert!(proxy_host_allowed(host), "rejected {host}");
        }
        for host in [
            "",
            "localhost",
            "localhost.",
            "printer.local",
            "nas.lan",
            "svc.internal",
            "router.home.arpa",
            "intranet",
            "127.0.0.1",
            "10.0.0.7",
            "192.168.1.1",
            "::1",
            "fd00::1",
            "::ffff:127.0.0.1",
            "127.1",
            "10.1",
            "0177.0.0.1",
            "127.000.000.001",
            "0x7f.0.0.1",
            "127.0.0.0x1",
            "1.2.3.4.5",
            "a..example.com",
            ".example.com",
            "exa mple.com",
            "fe80::1%eth0",
            "bücher.example",
        ] {
            assert!(!proxy_host_allowed(host), "accepted {host}");
        }
    }

    #[tokio::test]
    async fn proxied_lookup_sends_router_hostnames_to_the_proxy_unresolved() {
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;

        let relay = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let relay_addr = relay.local_addr().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        let router_ip = Ipv4Addr::new(8, 8, 8, 8);
        let (seen_tx, seen_rx) = oneshot::channel::<String>();
        let server = tokio::spawn(async move {
            let (mut control, _) = listener.accept().await.unwrap();
            crate::test_util::socks5_grant_udp_associate(&mut control, relay_addr.port()).await;

            let mut frame = [0u8; 1024];
            let (n, source) = relay.recv_from(&mut frame).await.unwrap();
            assert_eq!(frame[3], 3, "hostname must stay a domain address");
            let host_len = usize::from(frame[4]);
            let host = String::from_utf8(frame[5..5 + host_len].to_vec()).unwrap();
            let payload = &frame[7 + host_len..n];
            let txn = decode_all(payload)
                .unwrap()
                .get(b"t")
                .and_then(Value::as_bytes)
                .unwrap()
                .to_vec();
            let _ = seen_tx.send(host);

            let reply = encode_to_vec(&Value::Dict(vec![
                (
                    b"r".to_vec(),
                    Value::Dict(vec![(b"id".to_vec(), Value::Bytes(vec![9u8; 20]))]),
                ),
                (b"t".to_vec(), Value::Bytes(txn)),
                (b"y".to_vec(), Value::Bytes(b"r".to_vec())),
            ]));
            let mut response = vec![0, 0, 0, 1];
            response.extend_from_slice(&router_ip.octets());
            response.extend_from_slice(&6881u16.to_be_bytes());
            response.extend_from_slice(&reply);
            relay.send_to(&response, source).await.unwrap();

            let mut one = [0u8; 1];
            let _ = control.read(&mut one).await;
        });

        let connector = risuko_http::ProxyConnector::from_proxy(
            risuko_http::Proxy::all(format!("socks5h://{proxy_addr}")).unwrap(),
        );
        let dht = Dht::spawn_with_options(Some(connector), DhtOptions::default())
            .await
            .unwrap();
        let reply = tokio::time::timeout(
            Duration::from_secs(3),
            dht.clone().query_get_peers(
                DhtTarget::Host("router.unresolvable.invalid".into(), 6881),
                Id20([1u8; 20]),
            ),
        )
        .await
        .expect("proxied lookup timed out");
        let seen = tokio::time::timeout(Duration::from_secs(1), seen_rx)
            .await
            .expect("hostname never reached the proxy")
            .unwrap();
        assert_eq!(seen, "router.unresolvable.invalid");
        let (from, ..) = reply.expect("response through the proxy");
        assert_eq!(from, DhtTarget::Addr(SocketAddr::from((router_ip, 6881))));
        assert!(dht
            .router_addrs
            .lock()
            .contains(&SocketAddr::from((router_ip, 6881))));
        dht.shutdown().await;
        drop(dht);
        let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
    }

    #[test]
    fn outgoing_queries_and_responses_carry_the_client_version() {
        let id = Id20([1u8; 20]);
        let version = |packet: Vec<u8>| {
            decode_all(&packet)
                .unwrap()
                .get(b"v")
                .and_then(Value::as_bytes)
                .map(<[u8]>::to_vec)
        };
        let expected = Some(DHT_VERSION.to_vec());
        assert_eq!(&DHT_VERSION[..2], b"RS");
        assert_eq!(version(build_ping_bytes(b"ab", &id)), expected);
        assert_eq!(
            version(build_get_peers_bytes(b"ab", &id, &id, Want::Both, false)),
            expected
        );
        assert_eq!(
            version(build_announce_peer(b"ab", &id, &id, 6881, b"tok")),
            expected
        );
        assert_eq!(version(krpc_error(b"ab", 203, b"x", None)), expected);

        let our_id = Id20([9u8; 20]);
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let ping = Value::Dict(vec![
            (
                b"a".to_vec(),
                Value::Dict(vec![(b"id".to_vec(), Value::Bytes(vec![7u8; 20]))]),
            ),
            (b"q".to_vec(), Value::Bytes(b"ping".to_vec())),
            (b"t".to_vec(), Value::Bytes(b"ab".to_vec())),
            (b"v".to_vec(), Value::Bytes(b"LT\x01\x02".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        ]);
        let reply = build_query_response(&ping, "8.8.8.8:6000".parse().unwrap(), &state).unwrap();
        assert_eq!(version(reply), expected);
    }

    #[test]
    fn steady_state_lookups_ask_each_node_for_its_own_family() {
        let sparse = 0;
        let full = CROSS_FAMILY_MIN_NODES;
        let want = |target_v6: Option<bool>, v6_usable: bool, live4: usize, live6: usize| {
            want_families(target_v6, v6_usable, live4, live6)
        };
        assert_eq!(want(Some(false), true, full, full), Want::V4);
        assert_eq!(want(Some(true), true, full, full), Want::V6);
        assert_eq!(want(Some(false), true, full, sparse), Want::Both);
        assert_eq!(want(Some(true), true, sparse, full), Want::Both);
        assert_eq!(want(Some(false), false, full, sparse), Want::V4);
        assert_eq!(want(None, true, full, full), Want::Both);
        assert_eq!(want(None, false, full, full), Want::V4);
    }

    #[test]
    fn get_peers_want_list_follows_the_requested_families() {
        let id = Id20([1u8; 20]);
        let labels = |want: Want| {
            let pkt = build_get_peers_bytes(b"ab", &id, &id, want, false);
            decode_all(&pkt)
                .unwrap()
                .get(b"a")
                .unwrap()
                .get(b"want")
                .unwrap()
                .as_list()
                .unwrap()
                .iter()
                .filter_map(|v| v.as_bytes().map(<[u8]>::to_vec))
                .collect::<Vec<_>>()
        };
        assert_eq!(labels(Want::V4), vec![b"n4".to_vec()]);
        assert_eq!(labels(Want::V6), vec![b"n6".to_vec()]);
        assert_eq!(labels(Want::Both), vec![b"n4".to_vec(), b"n6".to_vec()]);
    }

    #[test]
    fn idle_buckets_are_refreshed_inside_their_own_range() {
        let our_id = Id20([0x11u8; 20]);
        let mut table = RoutingTable::new(our_id);
        let far = |first: u8, last: u8| {
            let mut distance = [0u8; 20];
            distance[0] = first;
            distance[19] = last;
            our_id.distance(&Id20(distance))
        };
        for last in 1..=BUCKET_SIZE as u8 {
            table.add(far(0x80, last), SocketAddr::from(([8, 8, 8, last], 6881)));
        }
        for last in 1..=BUCKET_SIZE as u8 {
            table.add(far(0x40, last), SocketAddr::from(([9, 9, 9, last], 6881)));
        }
        table.add(far(0x01, 1), SocketAddr::from(([1, 1, 1, 1], 6881)));
        assert!(table.buckets.len() > 2);
        assert_eq!(table.stale_refresh_target(Instant::now()), None);

        let later = Instant::now() + ROUTING_STALE * 2;
        let mut refreshed = Vec::new();
        while let Some(target) = table.stale_refresh_target(later) {
            let distance = our_id.distance(&target);
            let owners: Vec<usize> = table
                .buckets
                .iter()
                .enumerate()
                .filter(|(_, bucket)| bucket.matches(&distance))
                .map(|(idx, _)| idx)
                .collect();
            assert_eq!(owners.len(), 1);
            refreshed.push(owners[0]);
        }
        refreshed.sort_unstable();
        refreshed.dedup();
        assert_eq!(refreshed.len(), table.buckets.len());
    }

    #[test]
    fn refresh_of_the_bucket_holding_our_id_is_a_self_lookup() {
        let our_id = Id20([0x22u8; 20]);
        let mut table = RoutingTable::new(our_id);
        table.buckets[0].last_changed = Instant::now() - ROUTING_STALE;
        assert_eq!(table.stale_refresh_target(Instant::now()), Some(our_id));
        assert_eq!(table.stale_refresh_target(Instant::now()), None);
    }

    #[test]
    fn adding_a_node_marks_its_bucket_as_changed() {
        let mut table = RoutingTable::new(Id20([0u8; 20]));
        let Some(long_ago) = Instant::now().checked_sub(ROUTING_STALE * 2) else {
            return;
        };
        table.buckets[0].last_changed = long_ago;
        table.add(far_id(0x80, 1), "8.8.8.8:6881".parse().unwrap());
        assert_eq!(table.stale_refresh_target(Instant::now()), None);
    }

    fn routed(table: &RoutingTable, id: &Id20) -> Option<RoutingNode> {
        table
            .buckets
            .iter()
            .flat_map(|bucket| bucket.nodes.iter())
            .find(|node| node.id == *id)
            .cloned()
    }

    fn id_at_distance(from: &Id20, first: u8, last: u8) -> Id20 {
        let mut distance = [0u8; 20];
        distance[0] = first;
        distance[19] = last;
        from.distance(&Id20(distance))
    }

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    #[test]
    fn routing_entries_are_verified_when_the_id_matches_the_source_ip() {
        let mut table = RoutingTable::new(Id20([0u8; 20]));
        let compliant_ip = ip("93.184.216.34");
        let compliant = node_id::generate_node_id(compliant_ip);
        table.add(compliant, SocketAddr::new(compliant_ip, 6881));
        let random = far_id(0x80, 1);
        table.add(random, "8.8.8.8:6881".parse().unwrap());
        let local = far_id(0x80, 2);
        table.add(local, "192.168.1.9:6881".parse().unwrap());

        assert!(routed(&table, &compliant).unwrap().verified);
        assert!(!routed(&table, &random).unwrap().verified);
        assert!(routed(&table, &local).unwrap().verified);
    }

    fn full_far_bucket(our_id: Id20, liveness: &[RoutingLiveness]) -> (RoutingTable, Vec<Id20>) {
        let mut table = RoutingTable::new(our_id);
        let mut ids = Vec::new();
        for (index, liveness) in liveness.iter().enumerate() {
            let id = id_at_distance(&our_id, 0x80, index as u8 + 1);
            table.add_with_liveness(
                id,
                SocketAddr::from(([8, 8, 8, index as u8 + 1], 6881)),
                *liveness,
            );
            ids.push(id);
        }
        assert_eq!(table.len(), liveness.len());
        (table, ids)
    }

    fn verified_newcomer() -> (Id20, SocketAddr, Id20) {
        let ip = ip("93.184.216.34");
        let newcomer = node_id::derive_node_id(ip, [7u8; 20]);
        let mut table_id = newcomer.0;
        table_id[0] ^= 0x80;
        (newcomer, SocketAddr::new(ip, 6881), Id20(table_id))
    }

    #[test]
    fn a_verified_newcomer_replaces_the_oldest_unverified_questionable_entry() {
        let (newcomer, addr, our_id) = verified_newcomer();
        let mut states = vec![RoutingLiveness::Good; 4];
        states.extend([RoutingLiveness::Questionable; 4]);
        let (mut table, ids) = full_far_bucket(our_id, &states);

        table.add_with_liveness(newcomer, addr, RoutingLiveness::Questionable);

        assert_eq!(table.len(), BUCKET_SIZE);
        assert!(routed(&table, &newcomer).unwrap().verified);
        assert!(routed(&table, &ids[4]).is_none());
        for id in ids[..4].iter().chain(&ids[5..]) {
            assert!(routed(&table, id).is_some());
        }
    }

    #[test]
    fn an_unverified_newcomer_does_not_replace_a_questionable_entry() {
        let (_, _, our_id) = verified_newcomer();
        let mut states = vec![RoutingLiveness::Good; 4];
        states.extend([RoutingLiveness::Questionable; 4]);
        let (mut table, ids) = full_far_bucket(our_id, &states);

        let stranger = id_at_distance(&our_id, 0x80, 99);
        table.add_with_liveness(
            stranger,
            "9.9.9.9:6881".parse().unwrap(),
            RoutingLiveness::Questionable,
        );

        assert!(routed(&table, &stranger).is_none());
        assert!(ids.iter().all(|id| routed(&table, id).is_some()));
    }

    #[test]
    fn a_verified_newcomer_never_evicts_an_unverified_good_entry() {
        let (newcomer, addr, our_id) = verified_newcomer();
        let (mut table, ids) = full_far_bucket(our_id, &[RoutingLiveness::Good; BUCKET_SIZE]);

        table.add_with_liveness(newcomer, addr, RoutingLiveness::Questionable);

        assert!(routed(&table, &newcomer).is_none());
        assert!(ids.iter().all(|id| routed(&table, id).is_some()));
    }

    #[test]
    fn a_bad_entry_is_replaced_before_any_unverified_questionable_one() {
        let (newcomer, addr, our_id) = verified_newcomer();
        let mut states = vec![RoutingLiveness::Good; 5];
        states.extend([RoutingLiveness::Questionable, RoutingLiveness::Bad]);
        states.push(RoutingLiveness::Questionable);
        let (mut table, ids) = full_far_bucket(our_id, &states);

        table.add_with_liveness(newcomer, addr, RoutingLiveness::Questionable);

        assert!(routed(&table, &ids[6]).is_none());
        assert!(routed(&table, &ids[5]).is_some());
        assert!(routed(&table, &ids[7]).is_some());
        assert!(routed(&table, &newcomer).is_some());
    }

    #[test]
    fn a_good_unverified_newcomer_still_replaces_the_oldest_questionable_entry() {
        let our_id = Id20([0u8; 20]);
        let mut states = vec![RoutingLiveness::Good; 7];
        states.push(RoutingLiveness::Questionable);
        let (mut table, ids) = full_far_bucket(our_id, &states);

        let fresh = id_at_distance(&our_id, 0x80, 99);
        table.add(fresh, "9.9.9.9:6881".parse().unwrap());

        assert!(routed(&table, &ids[7]).is_none());
        assert!(routed(&table, &fresh).is_some());
    }

    #[test]
    fn following_an_elected_address_regenerates_the_table_id_once_per_change() {
        let start = Instant::now();
        let interval = node_id::MIN_REGENERATION_INTERVAL;
        let mut table = RoutingTable::new(Id20([0x10; 20]));
        let first = ip("93.184.216.34");

        let plan = table.follow_external_address(Some(first), false, interval, start);
        assert!(matches!(plan, IdPlan::Replace(id) if id == table.our_id));
        assert!(node_id_valid(&table.our_id, first));
        assert_eq!(table.id_changed, Some(start));

        let derived = table.our_id;
        assert_eq!(
            table.follow_external_address(Some(first), false, interval, start),
            IdPlan::Keep
        );
        assert_eq!(table.our_id, derived);

        let second = ip("151.101.1.69");
        let soon = start + Duration::from_secs(30);
        assert!(matches!(
            table.follow_external_address(Some(second), false, interval, soon),
            IdPlan::Wait(_)
        ));
        assert_eq!(table.our_id, derived);

        let later = start + interval;
        assert!(matches!(
            table.follow_external_address(Some(second), false, interval, later),
            IdPlan::Replace(_)
        ));
        assert!(node_id_valid(&table.our_id, second));
        assert_eq!(
            table.follow_external_address(Some(second), false, interval, later),
            IdPlan::Keep
        );
    }

    #[test]
    fn a_proxied_table_never_follows_an_elected_address() {
        let mut table = RoutingTable::new(Id20([0x10; 20]));
        let plan = table.follow_external_address(
            Some(ip("93.184.216.34")),
            true,
            node_id::MIN_REGENERATION_INTERVAL,
            Instant::now(),
        );
        assert_eq!(plan, IdPlan::Keep);
        assert_eq!(table.our_id, Id20([0x10; 20]));
        assert_eq!(table.id_changed, None);
    }

    #[test]
    fn rebuilding_for_a_new_node_id_keeps_every_node_and_its_state() {
        let mut table = RoutingTable::new(Id20([0x10; 20]));
        let mut expected = Vec::new();
        for index in 0..BUCKET_SIZE as u8 {
            let id = Id20([0x20 + index; 20]);
            let addr = SocketAddr::from(([8, 8, 8, index + 1], 6881));
            if index % 2 == 0 {
                table.add(id, addr);
            } else {
                table.add_with_liveness(id, addr, RoutingLiveness::Questionable);
            }
            expected.push(id);
        }
        table.record_failure(SocketAddr::from(([8, 8, 8, 2], 6881)));
        let before: Vec<RoutingNode> = expected
            .iter()
            .map(|id| routed(&table, id).unwrap())
            .collect();

        let elected = ip("93.184.216.34");
        let plan = table.follow_external_address(
            Some(elected),
            false,
            node_id::MIN_REGENERATION_INTERVAL,
            Instant::now(),
        );
        assert!(matches!(plan, IdPlan::Replace(_)));

        assert_eq!(table.len(), BUCKET_SIZE);
        for old in before {
            let node = routed(&table, &old.id).expect("node survives the rebuild");
            assert_eq!(node.addr, old.addr);
            assert_eq!(node.liveness, old.liveness);
            assert_eq!(node.failures, old.failures);
            assert_eq!(node.last_seen, old.last_seen);
            assert_eq!(node.verified, old.verified);
        }
        let probe = id_at_distance(&table.our_id, 0x80, 1);
        table.add(probe, "9.9.9.9:6881".parse().unwrap());
        assert!(routed(&table, &probe).is_some());
    }

    #[test]
    fn a_rebuild_drops_bad_nodes_and_never_files_a_node_under_our_own_id() {
        let mut table = RoutingTable::new(Id20([0x10; 20]));
        let dead = Id20([0x21; 20]);
        let new_id = Id20([0x31; 20]);
        table.add(Id20([0x20; 20]), "8.8.8.1:6881".parse().unwrap());
        table.add(dead, "8.8.8.2:6881".parse().unwrap());
        table.add(new_id, "8.8.8.3:6881".parse().unwrap());
        let dead_addr: SocketAddr = "8.8.8.2:6881".parse().unwrap();
        table.record_failure(dead_addr);
        table.record_failure(dead_addr);

        table.set_node_id(new_id, Instant::now());

        assert_eq!(table.len(), 1);
        assert!(routed(&table, &dead).is_none());
        assert!(routed(&table, &new_id).is_none());
    }

    #[test]
    fn a_rebuild_keeps_good_nodes_ahead_of_questionable_ones_when_a_bucket_overflows() {
        let mut table = RoutingTable::new(Id20([0u8; 20]));
        let mut good = Vec::new();
        let mut doubtful = Vec::new();
        for index in 1..=12u8 {
            let id = far_id(index, 0);
            let addr = SocketAddr::from(([8, 8, 8, index], 6881));
            if index <= 6 {
                table.add(id, addr);
                good.push(id);
            } else {
                table.add_with_liveness(id, addr, RoutingLiveness::Questionable);
                doubtful.push(id);
            }
        }
        assert_eq!(table.len(), 12);

        table.set_node_id(Id20([0xff; 20]), Instant::now());

        assert_eq!(table.len(), BUCKET_SIZE);
        assert!(good.iter().all(|id| routed(&table, id).is_some()));
        assert_eq!(
            doubtful
                .iter()
                .filter(|id| routed(&table, id).is_some())
                .count(),
            2
        );
    }

    #[test]
    fn announce_targets_prefer_verified_nodes_among_the_closest_set() {
        let mut targets = AnnounceTargets::new();
        for index in 0..(K * 2) as u8 {
            let mut id = [0u8; 20];
            id[19] = index + 1;
            let target = DhtTarget::Addr(SocketAddr::from(([198, 51, 100, index + 1], 6881)));
            record_announce_target(
                &mut targets,
                Id20(id),
                target,
                vec![index],
                index >= K as u8,
            );
        }

        let chosen = select_announce_targets(targets, false);

        assert_eq!(chosen.len(), K);
        assert!(chosen.iter().all(|(_, token)| token[0] >= K as u8));
    }

    #[test]
    fn announce_targets_fill_with_the_closest_unverified_nodes() {
        let mut targets = AnnounceTargets::new();
        for index in 0..(K * 2) as u8 {
            let mut id = [0u8; 20];
            id[19] = index + 1;
            let target = DhtTarget::Addr(SocketAddr::from(([198, 51, 100, index + 1], 6881)));
            let verified = index >= (K * 2 - 3) as u8;
            record_announce_target(&mut targets, Id20(id), target, vec![index], verified);
        }

        let mut tokens: Vec<u8> = select_announce_targets(targets, false)
            .into_iter()
            .map(|(_, token)| token[0])
            .collect();
        tokens.sort_unstable();

        assert_eq!(tokens, vec![0, 1, 2, 3, 4, 13, 14, 15]);
    }

    #[test]
    fn announce_targets_are_chosen_per_family() {
        let mut targets = AnnounceTargets::new();
        for index in 0..(K + 2) as u8 {
            let mut id = [0u8; 20];
            id[19] = index + 1;
            let v4 = DhtTarget::Addr(SocketAddr::from(([198, 51, 100, index + 1], 6881)));
            record_announce_target(&mut targets, Id20(id), v4, vec![4], index >= 2);
            let v6 = DhtTarget::Addr(
                format!("[2001:4860:4860::{:x}]:6881", index + 1)
                    .parse()
                    .unwrap(),
            );
            record_announce_target(&mut targets, Id20(id), v6, vec![6], false);
        }

        let chosen = select_announce_targets(targets, true);

        assert_eq!(chosen.len(), K * 2);
        assert_eq!(chosen.iter().filter(|(t, _)| target_is_v6(t)).count(), K);
    }

    fn lookup_candidates(
        count: u8,
        verified: impl Fn(u8) -> bool,
    ) -> BTreeMap<Id20, LookupCandidate> {
        (1..=count)
            .map(|index| {
                let mut distance = [0u8; 20];
                distance[19] = index;
                (
                    Id20(distance),
                    LookupCandidate {
                        target: DhtTarget::Addr(SocketAddr::from(([198, 51, 100, index], 6881))),
                        verified: verified(index),
                    },
                )
            })
            .collect()
    }

    fn octets(targets: &[DhtTarget]) -> Vec<u8> {
        targets
            .iter()
            .map(|target| match target {
                DhtTarget::Addr(SocketAddr::V4(addr)) => addr.ip().octets()[3],
                other => panic!("unexpected target {other:?}"),
            })
            .collect()
    }

    #[test]
    fn lookups_query_verified_candidates_before_unverified_ones_in_the_window() {
        let shortlist = lookup_candidates(8, |index| index >= 7);
        let mut queried = HashSet::new();

        let first = next_dispatch(&shortlist, &mut queried);
        assert_eq!(octets(&first), vec![7, 8, 1]);

        let second = next_dispatch(&shortlist, &mut queried);
        assert_eq!(octets(&second), vec![2, 3, 4]);
    }

    #[test]
    fn lookups_do_not_promote_verified_candidates_outside_the_closest_window() {
        let shortlist = lookup_candidates((K * 2 + 4) as u8, |index| index as usize > K * 2);
        let mut queried = HashSet::new();

        let first = next_dispatch(&shortlist, &mut queried);

        assert_eq!(octets(&first), vec![1, 2, 3]);
    }

    #[test]
    fn lookups_skip_candidates_that_were_already_queried() {
        let shortlist = lookup_candidates(4, |_| false);
        let mut queried = HashSet::new();
        queried.insert(DhtTarget::Addr(SocketAddr::from(([198, 51, 100, 1], 6881))));

        let chosen = next_dispatch(&shortlist, &mut queried);

        assert_eq!(octets(&chosen), vec![2, 3, 4]);
        assert!(next_dispatch(&shortlist, &mut queried).is_empty());
    }

    #[test]
    fn routing_state_round_trips_the_node_ids_under_the_v1_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        let contact = (
            Id20([1u8; 20]),
            "8.8.8.8:6881".parse::<SocketAddr>().unwrap(),
        );
        let ids = SavedIds {
            v4: Some(Id20([0xa1; 20])),
            v6: Some(Id20([0xb2; 20])),
        };
        save_routing_state_file(&path, &[contact], &ids).unwrap();

        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().next(), Some("risuko-dht-routing-v1"));
        let restored = load_routing_state_file(&path).unwrap();
        assert_eq!(restored.ids, ids);
        assert_eq!(restored.contacts, vec![contact]);
    }

    #[test]
    fn routing_state_without_node_ids_loads_with_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        let contact = (
            Id20([1u8; 20]),
            "8.8.8.8:6881".parse::<SocketAddr>().unwrap(),
        );
        save_routing_state_file(&path, &[contact], &SavedIds::default()).unwrap();
        std::fs::write(
            &path,
            format!(
                "risuko-dht-routing-v1\nnode-id4\tnot-hex\nnode-id6\t00\n{}\t{}\n",
                contact.0.to_hex(),
                contact.1
            ),
        )
        .unwrap();

        let restored = load_routing_state_file(&path).unwrap();
        assert_eq!(restored.ids, SavedIds::default());
        assert_eq!(restored.contacts, vec![contact]);
    }

    #[test]
    fn queriers_with_mismatched_ids_are_still_answered_and_routed_as_unverified() {
        let our_id = Id20([9u8; 20]);
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let from: SocketAddr = "8.8.8.8:6000".parse().unwrap();

        let reply = build_query_response(&query_from(b"ping", vec![]), from, &state).unwrap();

        assert_eq!(
            decode_all(&reply)
                .unwrap()
                .get(b"y")
                .and_then(Value::as_bytes),
            Some(b"r" as &[u8])
        );
        let node = routed(&state.routing.lock(), &Id20([7u8; 20])).unwrap();
        assert!(!node.verified);
    }

    #[test]
    fn compliant_queriers_are_routed_as_verified() {
        let our_id = Id20([9u8; 20]);
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let ip = ip("93.184.216.34");
        let compliant = node_id::generate_node_id(ip);
        let query = Value::Dict(vec![
            (
                b"a".to_vec(),
                Value::Dict(vec![(b"id".to_vec(), Value::Bytes(compliant.0.to_vec()))]),
            ),
            (b"q".to_vec(), Value::Bytes(b"ping".to_vec())),
            (b"t".to_vec(), Value::Bytes(b"ok".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        ]);

        build_query_response(&query, SocketAddr::new(ip, 6000), &state).unwrap();

        assert!(routed(&state.routing.lock(), &compliant).unwrap().verified);
    }

    #[test]
    fn replies_carry_the_node_id_of_the_queriers_family() {
        let id4 = Id20([4u8; 20]);
        let id6 = Id20([6u8; 20]);
        let state = InboundDhtState::with_routing6(
            Arc::new(Mutex::new(RoutingTable::new(id4))),
            Arc::new(Mutex::new(RoutingTable::new(id6))),
        );
        let ping = query_from(b"ping", vec![]);
        let replied_id = |from: &str| {
            let reply = build_query_response(&ping, from.parse().unwrap(), &state).unwrap();
            decode_all(&reply)
                .unwrap()
                .get(b"r")
                .and_then(|r| r.get(b"id"))
                .and_then(Value::as_bytes)
                .map(<[u8]>::to_vec)
        };
        assert_eq!(replied_id("8.8.8.8:6000"), Some(id4.0.to_vec()));
        assert_eq!(
            replied_id("[2001:4860:4860::8888]:6000"),
            Some(id6.0.to_vec())
        );
    }

    #[tokio::test]
    async fn outgoing_packets_use_the_node_id_of_the_target_family() {
        let dht = Dht::spawn().await.expect("bind DHT sockets");
        let v6_id = Id20([0x66; 20]);
        dht.routing6.lock().set_node_id(v6_id, Instant::now());
        let v4_id = dht.node_id(false);

        assert_ne!(v4_id, v6_id);
        let v4 = DhtTarget::Addr("8.8.8.8:6881".parse().unwrap());
        let v6 = DhtTarget::Addr("[2001:4860:4860::8888]:6881".parse().unwrap());
        assert_eq!(dht.node_id_for(&v4), v4_id);
        assert_eq!(dht.node_id_for(&v6), v6_id);
        assert_eq!(
            dht.node_id_for(&DhtTarget::Host("router.example.com".into(), 6881)),
            v4_id
        );
        dht.shutdown().await;
    }

    async fn direct_dht(path: &Path, external: &ExternalIp) -> Arc<Dht> {
        direct_dht_with_interval(path, external, node_id::MIN_REGENERATION_INTERVAL).await
    }

    async fn direct_dht_with_interval(
        path: &Path,
        external: &ExternalIp,
        interval: Duration,
    ) -> Arc<Dht> {
        let options = DhtOptions {
            routing_state_path: Some(path.to_path_buf()),
            ..Default::default()
        };
        tokio::time::timeout(
            Duration::from_secs(5),
            Dht::spawn_with_external(None, options, external.clone(), interval),
        )
        .await
        .expect("spawn timed out")
        .expect("bind DHT sockets")
    }

    fn vote_upnp(external: &ExternalIp, address: &str, route: Route) {
        let address = ip(address);
        external.vote(address, Source::Upnp, address, route);
    }

    #[tokio::test]
    async fn a_direct_dht_regenerates_its_node_id_and_keeps_its_nodes_on_election() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        let external = ExternalIp::new();
        let dht = direct_dht(&path, &external).await;
        let nodes: Vec<(Id20, SocketAddr)> = (1..=3u8)
            .map(|index| {
                (
                    Id20([index; 20]),
                    SocketAddr::from(([8, 8, 8, index], 6881)),
                )
            })
            .collect();
        dht.add_bootstrap_nodes(nodes.clone());
        let before = dht.node_id(false);
        let before6 = dht.node_id(true);
        assert_eq!(dht.node_id_compliant(), None);

        vote_upnp(&external, "93.184.216.34", Route::Direct);
        eventually("a compliant node id", || {
            dht.node_id_compliant() == Some(true)
        })
        .await;

        assert_ne!(dht.node_id(false), before);
        assert_eq!(dht.node_id(true), before6);
        let snapshot = dht.routing_snapshot();
        for node in &nodes {
            assert!(snapshot.contains(node));
        }
        dht.shutdown().await;
    }

    #[tokio::test]
    async fn the_node_id_is_persisted_and_validated_against_the_elected_address_on_load() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        let external = ExternalIp::new();
        vote_upnp(&external, "93.184.216.34", Route::Direct);
        let dht = direct_dht(&path, &external).await;
        let derived = dht.node_id(false);
        assert_eq!(dht.node_id_compliant(), Some(true));
        dht.add_bootstrap_nodes([(Id20([5u8; 20]), "8.8.8.8:6881".parse().unwrap())]);
        dht.shutdown().await;
        assert_eq!(
            load_routing_state_file(&path).unwrap().ids.v4,
            Some(derived)
        );

        let same_path = dir.path().join("same.state");
        std::fs::copy(&path, &same_path).unwrap();
        let same = direct_dht(&same_path, &external).await;
        assert_eq!(same.node_id(false), derived);
        same.shutdown().await;

        let unelected_path = dir.path().join("unelected.state");
        std::fs::copy(&path, &unelected_path).unwrap();
        let unelected = direct_dht(&unelected_path, &ExternalIp::new()).await;
        assert_eq!(unelected.node_id(false), derived);
        assert_eq!(unelected.node_id_compliant(), None);
        unelected.shutdown().await;

        let moved_path = dir.path().join("moved.state");
        std::fs::copy(&path, &moved_path).unwrap();
        let moved_external = ExternalIp::new();
        vote_upnp(&moved_external, "151.101.1.69", Route::Direct);
        let moved = direct_dht(&moved_path, &moved_external).await;
        assert_ne!(moved.node_id(false), derived);
        assert_eq!(moved.node_id_compliant(), Some(true));
        moved.shutdown().await;
    }

    #[tokio::test]
    async fn a_changed_election_regenerates_after_the_interval_without_a_second_event() {
        let interval = Duration::from_secs(1);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        let external = ExternalIp::new();
        let dht = direct_dht_with_interval(&path, &external, interval).await;

        vote_upnp(&external, "93.184.216.34", Route::Direct);
        eventually("a compliant node id", || {
            dht.node_id_compliant() == Some(true)
        })
        .await;
        let derived = dht.node_id(false);
        let first_change = dht.routing.lock().id_changed.expect("first regeneration");

        vote_upnp(&external, "151.101.1.69", Route::Direct);
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(dht.node_id(false), derived);

        eventually("a compliant node id", || {
            dht.node_id_compliant() == Some(true)
        })
        .await;
        assert_ne!(dht.node_id(false), derived);
        let second_change = dht.routing.lock().id_changed.expect("second regeneration");
        assert!(second_change.duration_since(first_change) >= interval);
        dht.shutdown().await;
    }

    #[tokio::test]
    async fn following_the_election_reports_which_families_were_regenerated() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        let dht = direct_dht(&path, &ExternalIp::new()).await;
        let v4 = Ipv4Addr::new(93, 184, 216, 34);
        let v6 = "2001:db8::1".parse::<Ipv6Addr>().unwrap();
        let before = (dht.node_id(false), dht.node_id(true));

        let only_v6 = dht.follow_external(
            ExternalAddresses {
                v4: None,
                v6: Some(v6),
            },
            Instant::now(),
        );
        assert!(only_v6.regenerated_v6 && !only_v6.regenerated_v4);
        assert_eq!(dht.node_id(false), before.0);
        assert_ne!(dht.node_id(true), before.1);
        assert!(node_id_valid(&dht.node_id(true), IpAddr::V6(v6)));

        let only_v4 = dht.follow_external(
            ExternalAddresses {
                v4: Some(v4),
                v6: Some(v6),
            },
            Instant::now(),
        );
        assert!(only_v4.regenerated_v4 && !only_v4.regenerated_v6);
        assert!(node_id_valid(&dht.node_id(false), IpAddr::V4(v4)));
        dht.shutdown().await;
    }

    async fn fake_socks5_association() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        use tokio::io::AsyncReadExt;
        use tokio::net::TcpListener;

        let relay = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let relay_port = relay.local_addr().unwrap().port();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let _relay = relay;
            let (mut control, _) = listener.accept().await.unwrap();
            crate::test_util::socks5_grant_udp_associate(&mut control, relay_port).await;
            let mut one = [0u8; 1];
            let _ = control.read(&mut one).await;
        });
        (proxy_addr, handle)
    }

    #[tokio::test]
    async fn a_proxied_dht_keeps_its_random_id_after_a_upnp_only_election() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        let elected = ip("93.184.216.34");
        let derived = node_id::generate_node_id(elected);
        let contact = (
            Id20([1u8; 20]),
            "8.8.8.8:6881".parse::<SocketAddr>().unwrap(),
        );
        let saved = SavedIds {
            v4: Some(derived),
            v6: None,
        };
        save_routing_state_file(&path, &[contact], &saved).unwrap();
        let external = ExternalIp::new();
        vote_upnp(&external, "93.184.216.34", Route::Proxied);
        assert_eq!(
            external.external_v4(),
            Some(Ipv4Addr::new(93, 184, 216, 34))
        );

        let (proxy_addr, server) = fake_socks5_association().await;
        let connector = risuko_http::ProxyConnector::from_proxy(
            risuko_http::Proxy::all(format!("socks5h://{proxy_addr}")).unwrap(),
        );
        let options = DhtOptions {
            routing_state_path: Some(path.clone()),
            ..Default::default()
        };
        let dht = tokio::time::timeout(
            Duration::from_secs(5),
            Dht::spawn_with_external(
                Some(connector),
                options,
                external.clone(),
                node_id::MIN_REGENERATION_INTERVAL,
            ),
        )
        .await
        .expect("spawn timed out")
        .expect("proxied DHT");

        let random = dht.node_id(false);
        assert_ne!(random, derived);
        assert!(!node_id_valid(&random, elected));
        assert_eq!(dht.node_id(true), random);
        assert!(dht.identity_handle.lock().is_none());
        assert_eq!(dht.node_id_compliant(), None);

        vote_upnp(&external, "151.101.1.69", Route::Proxied);
        let outcome = dht.follow_external(external.addresses(), Instant::now());
        assert!(!outcome.regenerated_v4 && !outcome.regenerated_v6);
        assert!(outcome.retry_after.is_none());
        assert_eq!(dht.node_id(false), random);
        assert_eq!(dht.node_id(true), random);

        dht.persist_routing_state().await;
        assert_eq!(load_routing_state_file(&path).unwrap().ids, saved);

        dht.shutdown().await;
        server.abort();
    }
}
