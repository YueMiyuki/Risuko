//! Minimal BEP-5 DHT

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::RngExt;
use tokio::net::{lookup_host, UdpSocket};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;

use risuko_http::{ProxyDatagram, ProxyDatagramSource};

use super::bencode::{decode_all_external, encode_to_vec, DecodeLimits, Value};
use super::core::Id20;

/// Decoded `get_peers` reply: source addr, responder id (if present), peer list, and learned (id, addr) nodes
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

/// Body fields parsed from a `get_peers` response (no source addr)
type GetPeersResponseBody = (
    Option<Id20>,
    Vec<SocketAddr>,
    Vec<(Id20, SocketAddr)>,
    Option<Vec<u8>>,
);

const K: usize = 8;
const ALPHA: usize = 3;
const QUERY_TIMEOUT: Duration = Duration::from_secs(4);
const MAX_ROUND_QUERIES: usize = 50;
const KRPC_DECODE_LIMITS: DecodeLimits = DecodeLimits::new(2048, 16, 1024);
const PEER_STORE_TTL: Duration = Duration::from_secs(30 * 60);
const TOKEN_ROTATION: Duration = Duration::from_secs(5 * 60);
const MAX_STORED_PEERS: usize = 2048;
const ROUTING_STALE: Duration = Duration::from_secs(15 * 60);
const MAX_PERSISTED_ROUTES: usize = 4096;
const ROUTING_REFRESH_INTERVAL: Duration = Duration::from_secs(15 * 60);
const MAX_KRPC_RESPONSE_BYTES: usize = 1024;

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
        if self.rotated.elapsed() < TOKEN_ROTATION {
            return;
        }
        self.previous = self.current;
        rand::rng().fill(&mut self.current);
        self.rotated = Instant::now();
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
    our_id: Id20,
    routing: Arc<Mutex<RoutingTable>>,
    routing6: Arc<Mutex<RoutingTable>>,
    peers: Mutex<HashMap<Id20, Vec<StoredPeer>>>,
    tokens: Mutex<TokenState>,
    private_hashes: Mutex<HashSet<Id20>>,
}

impl InboundDhtState {
    #[cfg(test)]
    fn new(our_id: Id20, routing: Arc<Mutex<RoutingTable>>) -> Self {
        Self::with_routing6(
            our_id,
            routing,
            Arc::new(Mutex::new(RoutingTable::new(our_id))),
        )
    }

    fn with_routing6(
        our_id: Id20,
        routing: Arc<Mutex<RoutingTable>>,
        routing6: Arc<Mutex<RoutingTable>>,
    ) -> Self {
        Self {
            our_id,
            routing,
            routing6,
            peers: Mutex::new(HashMap::new()),
            tokens: Mutex::new(TokenState::new()),
            private_hashes: Mutex::new(HashSet::new()),
        }
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
        if self.private_hashes.lock().contains(&hash) {
            return;
        }
        let mut peers = self.peers.lock();
        let entries = peers.entry(hash).or_default();
        if let Some(existing) = entries.iter_mut().find(|peer| peer.addr == addr) {
            existing.seen = Instant::now();
            return;
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
        if !valid_routing_addr(addr) {
            return;
        }
        if addr.is_ipv6() {
            self.routing6
                .lock()
                .add_with_liveness(id, addr, RoutingLiveness::Questionable);
        } else {
            self.routing
                .lock()
                .add_with_liveness(id, addr, RoutingLiveness::Questionable);
        }
    }

    fn closest_nodes(&self, target: &Id20, n: usize) -> Vec<(Id20, SocketAddr)> {
        let mut nodes = self.routing.lock().closest_nodes(target, n);
        nodes.extend(self.routing6.lock().closest_nodes(target, n));
        nodes.sort_by_key(|(id, _)| id.distance(target));
        nodes.truncate(n);
        nodes
    }
}

/// Process-wide DHT ownership and route state
#[derive(Default)]
struct SharedDhtState {
    dht: Option<Arc<Dht>>,
    proxy_requested: Option<bool>,
    last_error: Option<String>,
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

fn dht_targets_match(expected: &DhtTarget, actual: &DhtTarget) -> bool {
    match (expected, actual) {
        (DhtTarget::Addr(expected), DhtTarget::Addr(actual)) => expected == actual,
        (
            DhtTarget::Host(expected_host, expected_port),
            DhtTarget::Host(actual_host, actual_port),
        ) => expected_port == actual_port && expected_host.eq_ignore_ascii_case(actual_host),
        // Proxy readers may report the hostname-form source used in a SOCKS5
        // domain request as an IP address. Direct requests are normalized to
        // Addr before registration and therefore never use this fallback.
        (DhtTarget::Host(..), DhtTarget::Addr(..)) => true,
        _ => false,
    }
}

/// A live DHT node; holds bound UDP sockets (v4 always, v6 if available) and a background reader task per socket that routes responses to pending queries by transaction id
pub struct Dht {
    sock: Option<Arc<UdpSocket>>,
    sock6: Option<Arc<UdpSocket>>,
    proxy_datagram: Option<Arc<ProxyDatagram>>,
    our_id: Id20,
    pending: Arc<Mutex<PendingMap>>,
    routing: Arc<Mutex<RoutingTable>>,
    routing6: Arc<Mutex<RoutingTable>>,
    bootstrap_hosts: Mutex<Vec<DhtTarget>>,
    server: Arc<InboundDhtState>,
    reader_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    reader6_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    lookup_handles: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    refresh_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    routing_state_path: Option<PathBuf>,
    shutdown: AtomicBool,
}

pub struct DhtRouteSwap {
    previous: Option<Arc<Dht>>,
    previous_proxy_requested: Option<bool>,
    previous_error: Option<String>,
    next: Option<Arc<Dht>>,
}

impl DhtRouteSwap {
    pub fn next(&self) -> Option<Arc<Dht>> {
        self.next.clone()
    }

    /// Finalize the route transition and stop the old runtime.
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
                previous.shutdown().await;
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
            guard.last_error = self.previous_error.clone();
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
        self.persist_configured_routing_state();
        self.shutdown.store(true, Ordering::Release);
        self.abort_lookups();
        if let Some(h) = self.reader_handle.lock().take() {
            h.abort();
        }
        if let Some(h) = self.reader6_handle.lock().take() {
            h.abort();
        }
        if let Some(h) = self.refresh_handle.lock().take() {
            h.abort();
        }
    }
}

impl Dht {
    fn abort_lookups(&self) {
        let handles = std::mem::take(&mut *self.lookup_handles.lock());
        for handle in handles {
            handle.abort();
        }
    }

    /// Stop iterative lookups while keeping this runtime available for a
    /// possible route rollback.
    pub fn cancel_lookups(&self) {
        self.abort_lookups();
    }

    pub async fn shutdown(&self) {
        if let Some(path) = self.routing_state_path.clone() {
            let routes = self.routing_snapshot();
            let log_path = path.clone();
            let result = tokio::task::spawn_blocking(move || save_routing_state_file(&path, &routes)).await;
            match result {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => {
                    tracing::warn!(path = %log_path.display(), "failed to persist DHT routing state: {error}");
                }
                Err(error) => {
                    tracing::warn!(path = %log_path.display(), "DHT routing state persistence task failed: {error}");
                }
            }
        }
        self.shutdown.store(true, Ordering::Release);
        self.abort_lookups();
        if let Some(handle) = self.reader_handle.lock().take() {
            handle.abort();
        }
        if let Some(handle) = self.reader6_handle.lock().take() {
            handle.abort();
        }
        if let Some(handle) = self.refresh_handle.lock().take() {
            handle.abort();
        }
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

/// Removes its transaction id from `pending` on drop, keeping aborted lookup tasks from leaving orphaned pending entries
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
                Some(socket) => socket.send_to(packet, target).await,
                None => Err(std::io::Error::new(
                    std::io::ErrorKind::AddrNotAvailable,
                    "IPv6 DHT socket unavailable",
                )),
            },
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
        let resolved = lookup_host((host, port)).await?.collect::<Vec<_>>();
        let targets = resolved
            .iter()
            .copied()
            .filter(|target| public_dht_endpoint(*target))
            .collect::<Vec<_>>();
        if targets.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("no public addresses for {host}"),
            ));
        }
        if let Some(proxy) = &self.proxy_datagram {
            if targets.len() != resolved.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("host {host} resolves to a non-public address"),
                ));
            }
            return proxy
                .send_to_host(packet, host, port)
                .await
                .map_err(|error| std::io::Error::other(error.to_string()));
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
                    Some(socket) => socket.send_to(packet, target).await,
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

    pub async fn shared() -> Option<Arc<Dht>> {
        if let Some(dht) = Self::current_shared().await {
            return Some(dht);
        }

        if shared_dht_cell().lock().await.proxy_requested == Some(true) {
            return None;
        }
        Self::shared_with_proxy(None).await
    }

    pub async fn shared_with_proxy(proxy: Option<risuko_http::ProxyConnector>) -> Option<Arc<Dht>> {
        Self::install_shared(proxy, false).await
    }

    pub async fn replace_shared_with_proxy(
        proxy: Option<risuko_http::ProxyConnector>,
    ) -> Option<Arc<Dht>> {
        let swap = Self::prepare_shared_with_proxy(proxy).await.ok()?;
        let next = swap.next();
        swap.commit().await;
        next
    }

    pub async fn replace_shared_with_proxy_checked(
        proxy: Option<risuko_http::ProxyConnector>,
    ) -> Result<Option<Arc<Dht>>, String> {
        let swap = Self::prepare_shared_with_proxy(proxy).await?;
        let next = swap.next();
        swap.commit().await;
        Ok(next)
    }

    pub async fn prepare_shared_with_proxy(
        proxy: Option<risuko_http::ProxyConnector>,
    ) -> Result<DhtRouteSwap, String> {
        let _install_guard = shared_dht_install_lock().lock().await;
        let requested_proxy = proxy.is_some();
        let (previous, previous_proxy_requested, previous_error) = {
            let mut guard = shared_dht_cell().lock().await;
            let snapshot = (
                guard.dht.clone(),
                guard.proxy_requested,
                guard.last_error.clone(),
            );
            guard.proxy_requested = Some(requested_proxy);
            guard.last_error = None;
            snapshot
        };
        let preserved_routes = previous
            .as_ref()
            .map(|dht| dht.routing_snapshot())
            .unwrap_or_default();
        let next = match Dht::spawn_with_proxy(proxy).await {
            Ok(dht) => {
                dht.add_bootstrap_nodes(preserved_routes);
                let warm = dht.clone();
                tokio::spawn(async move { warm.bootstrap().await });
                Some(dht)
            }
            Err(error) => {
                let message = error.to_string();
                let mut guard = shared_dht_cell().lock().await;
                guard.dht = previous.clone();
                guard.proxy_requested = previous_proxy_requested.or(Some(requested_proxy));
                guard.last_error = Some(message.clone());
                return Err(message);
            }
        };
        shared_dht_cell().lock().await.dht = next.clone();

        if let Some(previous) = previous.as_ref() {
            // A route swap may take time to rebuild the surrounding runtime;
            // stop old lookups immediately so they cannot keep announcing on
            // the previous proxy while the swap is in progress.
            previous.cancel_lookups();
        }

        Ok(DhtRouteSwap {
            previous,
            previous_proxy_requested,
            previous_error,
            next,
        })
    }

    async fn install_shared(
        proxy: Option<risuko_http::ProxyConnector>,
        force: bool,
    ) -> Option<Arc<Dht>> {
        let _install_guard = shared_dht_install_lock().lock().await;
        let requested_proxy = proxy.is_some();
        let (previous, previous_proxy_requested) = {
            let mut guard = shared_dht_cell().lock().await;
            if !force && guard.proxy_requested == Some(requested_proxy) && guard.dht.is_some() {
                return guard.dht.clone();
            }
            if !force
                && guard.proxy_requested == Some(true)
                && !requested_proxy
                && guard.dht.is_none()
            {
                return None;
            }
            let previous = guard.dht.clone();
            let previous_proxy_requested = guard.proxy_requested;
            guard.last_error = None;
            (previous, previous_proxy_requested)
        };
        let preserved_routes = previous
            .as_ref()
            .map(|dht| dht.routing_snapshot())
            .unwrap_or_default();

        let next = match Dht::spawn_with_proxy(proxy).await {
            Ok(dht) => {
                dht.add_bootstrap_nodes(preserved_routes);
                let warm = dht.clone();
                tokio::spawn(async move { warm.bootstrap().await });
                Some(dht)
            }
            Err(error) => {
                let mut guard = shared_dht_cell().lock().await;
                guard.dht = previous.clone();
                guard.proxy_requested = previous_proxy_requested.or(Some(requested_proxy));
                guard.last_error = Some(error.to_string());
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

    pub async fn spawn() -> std::io::Result<Arc<Self>> {
        Self::spawn_with_proxy(None).await
    }

    pub async fn spawn_with_proxy(
        proxy: Option<risuko_http::ProxyConnector>,
    ) -> std::io::Result<Arc<Self>> {
        Self::spawn_with_proxy_and_routing_state(proxy, None).await
    }
    pub async fn spawn_with_routing_state(
        state_path: impl Into<PathBuf>,
    ) -> std::io::Result<Arc<Self>> {
        Self::spawn_with_proxy_and_routing_state(None, Some(state_path.into())).await
    }
    pub async fn spawn_with_proxy_and_routing_state(
        proxy: Option<risuko_http::ProxyConnector>,
        routing_state_path: Option<PathBuf>,
    ) -> std::io::Result<Arc<Self>> {
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
                Some(Arc::new(result.map_err(|error| {
                    std::io::Error::new(std::io::ErrorKind::Unsupported, error.to_string())
                })?))
            }
            None => None,
        };

        let (sock, sock6) = if proxy_datagram.is_none() {
            let sock = Some(Arc::new(UdpSocket::bind("0.0.0.0:0").await?));
            let sock6 = match UdpSocket::bind("[::]:0").await {
                Ok(s) => Some(Arc::new(s)),
                Err(e) => {
                    tracing::debug!("dht: no ipv6 socket: {e}");
                    None
                }
            };
            (sock, sock6)
        } else {
            (None, None)
        };
        let our_id = random_id();
        let pending: Arc<Mutex<PendingMap>> = Arc::new(Mutex::new(Default::default()));

        let routing = Arc::new(Mutex::new(RoutingTable::new(our_id)));
        let routing6 = Arc::new(Mutex::new(RoutingTable::new(our_id)));
        if let Some(path) = routing_state_path.as_deref() {
            for (id, addr) in load_routing_state_file(path)? {
                if addr.is_ipv6() {
                    routing6
                        .lock()
                        .add_with_liveness(id, addr, RoutingLiveness::Questionable);
                } else {
                    routing
                        .lock()
                        .add_with_liveness(id, addr, RoutingLiveness::Questionable);
                }
            }
        }
        let server = Arc::new(InboundDhtState::with_routing6(
            our_id,
            routing.clone(),
            routing6.clone(),
        ));
        let this = Arc::new(Self {
            sock: sock.clone(),
            sock6: sock6.clone(),
            proxy_datagram: proxy_datagram.clone(),
            our_id,
            pending: pending.clone(),
            routing,
            routing6,
            bootstrap_hosts: Mutex::new(Vec::new()),
            server: server.clone(),
            reader_handle: Mutex::new(None),
            reader6_handle: Mutex::new(None),
            lookup_handles: Mutex::new(Vec::new()),
            refresh_handle: Mutex::new(None),
            routing_state_path,
            shutdown: AtomicBool::new(false),
        });

        let reader_sock = sock.clone();
        let pending_reader = pending.clone();
        let reader_handle = if let Some(datagram) = proxy_datagram.clone() {
            tokio::spawn(async move { proxy_reader_loop(datagram, pending_reader).await })
        } else {
            let server_reader = server.clone();
            tokio::spawn(async move {
                reader_loop(
                    reader_sock.expect("direct DHT socket"),
                    pending_reader,
                    server_reader,
                )
                .await
            })
        };
        *this.reader_handle.lock() = Some(reader_handle);

        if proxy_datagram.is_none() {
            if let Some(s6) = sock6 {
                let pending6 = pending.clone();
                let server6 = server.clone();
                let reader6_handle =
                    tokio::spawn(async move { reader_loop(s6, pending6, server6).await });
                *this.reader6_handle.lock() = Some(reader6_handle);
            }
        }

        let weak = Arc::downgrade(&this);
        let refresh_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(ROUTING_REFRESH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            loop {
                interval.tick().await;
                let Some(dht) = weak.upgrade() else {
                    return;
                };
                if dht.shutdown.load(Ordering::Acquire) {
                    return;
                }
                if dht.proxy_datagram.is_some() {
                    // Proxied DHT sockets cannot issue the direct refresh pings
                    // needed to validate liveness transitions.
                    continue;
                }
                let stale = dht.refresh_routing(K);
                for (id, addr) in stale {
                    let _ = dht.ping_node(addr, id).await;
                }
            }
        });
        *this.refresh_handle.lock() = Some(refresh_handle);

        tracing::debug!(
            "DHT started: id={}, bootstrap={} nodes, ipv6={}",
            hex::encode(our_id.as_bytes()),
            DEFAULT_BOOTSTRAP.len(),
            this.sock6.is_some(),
        );
        Ok(this)
    }

    /// Start an iterative `get_peers` lookup and stream discovered peers on the returned channel until `budget` elapses or the lookup converges; when `announce_port` is set, also re-publishes us to the DHT (BEP-5 `announce_peer`) on the closest write-token nodes so other clients searching this info-hash can dial us on that BT listen port
    pub fn get_peers_stream(
        self: &Arc<Self>,
        info_hash: Id20,
        budget: Duration,
        announce_port: Option<u16>,
    ) -> mpsc::UnboundedReceiver<SocketAddr> {
        let (tx, rx) = mpsc::unbounded_channel::<SocketAddr>();
        if self.server.is_private(&info_hash) {
            return rx;
        }
        let this = self.clone();
        let handle = tokio::spawn(async move {
            let _ = tokio::time::timeout(
                budget,
                this.iterative_get_peers(info_hash, tx, announce_port),
            )
            .await;
        });
        let mut handles = self.lookup_handles.lock();
        handles.retain(|handle| !handle.is_finished());
        if self.shutdown.load(Ordering::Acquire) {
            handle.abort();
        } else {
            handles.push(handle);
        }
        rx
    }

    async fn iterative_get_peers(
        self: Arc<Self>,
        info_hash: Id20,
        peer_tx: mpsc::UnboundedSender<SocketAddr>,
        announce_port: Option<u16>,
    ) {
        if self.server.is_private(&info_hash) {
            return;
        }
        let mut targets: Vec<DhtTarget> = self
            .closest_routing(&info_hash, K * 2)
            .into_iter()
            .map(DhtTarget::Addr)
            .collect();
        targets.extend(bootstrap_targets());
        targets.extend(self.bootstrap_hosts.lock().iter().cloned());
        if targets.is_empty() {
            tracing::debug!("dht: no bootstrap nodes available");
            return;
        }

        let mut shortlist: BTreeMap<Id20, DhtTarget> = BTreeMap::new();
        let mut queried: HashSet<DhtTarget> = HashSet::new();
        let mut peers_seen: HashSet<SocketAddr> = HashSet::new();
        let mut announce_targets: BTreeMap<Id20, (DhtTarget, Vec<u8>)> = BTreeMap::new();

        let mut seed_seen = HashSet::new();
        targets.retain(|target| seed_seen.insert(target.clone()));
        targets.truncate(MAX_ROUND_QUERIES);
        queried.extend(targets.iter().cloned());

        let mut futs: JoinSet<Option<GetPeersReply>> = JoinSet::new();
        for target in targets {
            if self.server.is_private(&info_hash) {
                return;
            }
            let this = self.clone();
            futs.spawn(async move { this.query_get_peers(target, info_hash).await });
        }

        let mut total_peers = 0usize;
        let mut total_nodes = 0usize;
        let mut rounds_without_progress = 0usize;

        loop {
            if self.server.is_private(&info_hash) {
                return;
            }
            let Some(joined) = futs.join_next().await else {
                break;
            };
            let res = joined.ok().flatten();

            let Some((from, responder_id, peers, nodes, token)) = res else {
                continue;
            };

            // Emit peers immediately
            let mut new_peers = 0usize;
            for p in peers {
                if peers_seen.insert(p) {
                    new_peers += 1;
                    if peer_tx.send(p).is_err() {
                        return;
                    }
                }
            }
            total_peers += new_peers;

            // Record the responder as a live DHT node, preferring the id it reported in its KRPC response and falling back to a pseudo-id only if omitted; the real id keeps XOR distance accurate, which matters for lookup convergence
            let node_id = responder_id.unwrap_or_else(|| pseudo_id_target(&from));
            let responder_public = match &from {
                DhtTarget::Addr(addr) => public_dht_endpoint(*addr),
                DhtTarget::Host(_, _) => true,
            };
            if responder_public {
                shortlist.insert(node_id.distance(&info_hash), from.clone());
                if responder_id.is_some() {
                    if let DhtTarget::Addr(from) = &from {
                        self.add_routing(node_id, *from);
                    }
                }
            }
            if responder_public {
                if let Some(tok) = token {
                    announce_targets.insert(node_id.distance(&info_hash), (from, tok));
                    while announce_targets.len() > K * 2 {
                        announce_targets.pop_last();
                    }
                }
            }

            // Merge any learned nodes into the shortlist
            let mut progressed = false;
            for (nid, naddr) in &nodes {
                if !public_dht_endpoint(*naddr) {
                    continue;
                }
                total_nodes += 1;
                let d = nid.distance(&info_hash);
                if let std::collections::btree_map::Entry::Vacant(e) = shortlist.entry(d) {
                    e.insert(DhtTarget::Addr(*naddr));
                    progressed = true;
                }
                self.add_routing_questionable(*nid, *naddr);
            }

            // Trim to K * 2 to keep memory bounded
            while shortlist.len() > K * 4 {
                shortlist.pop_last();
            }

            if progressed || new_peers > 0 {
                rounds_without_progress = 0;
            } else {
                rounds_without_progress += 1;
            }

            // Queue up to ALPHA new queries from the closest unvisited
            let mut dispatched = 0usize;
            let mut to_dispatch: Vec<DhtTarget> = Vec::new();
            for (_d, target) in shortlist.iter().take(K * 2) {
                let target = target.clone();
                if queried.insert(target.clone()) {
                    to_dispatch.push(target);
                    dispatched += 1;
                    if dispatched >= ALPHA {
                        break;
                    }
                }
            }
            for target in to_dispatch {
                if self.server.is_private(&info_hash) {
                    return;
                }
                let this = self.clone();
                futs.spawn(async move { this.query_get_peers(target, info_hash).await });
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

        // BEP-5 announce_peer: publish ourselves on the closest token-bearing nodes so other clients doing get_peers for this info-hash discover us and can open inbound connections; fire-and-forget, we don't need the ack
        if let Some(port) = announce_port {
            for (_d, (addr, token)) in announce_targets.into_iter().take(K) {
                if self.server.is_private(&info_hash) {
                    return;
                }
                let txn = random_transaction_id();
                let pkt = build_announce_peer(&txn, &self.our_id, &info_hash, port, &token);
                if self.server.is_private(&info_hash) {
                    return;
                }
                let _ = self.send_target(&pkt, &addr).await;
            }
        }
    }

    async fn query_get_peers(
        self: Arc<Self>,
        target: DhtTarget,
        info_hash: Id20,
    ) -> Option<GetPeersReply> {
        if self.server.is_private(&info_hash) {
            return None;
        }
        let (target, resolved_addrs) = match target {
            DhtTarget::Host(host, port) => {
                // Resolve bootstrap names before sending any traffic and only
                // retain publicly routable addresses. Using the validated
                // numeric endpoint for proxy requests also prevents a proxy
                // from resolving the same hostname to a private address.
                let addresses = lookup_host((host.as_str(), port))
                    .await
                    .ok()?
                    .filter(|address| public_dht_endpoint(*address))
                    .filter(|address| {
                        self.proxy_datagram.is_some()
                            || match address {
                                SocketAddr::V4(_) => self.sock.is_some(),
                                SocketAddr::V6(_) => self.sock6.is_some(),
                            }
                    })
                    .collect::<Vec<_>>();
                if addresses.is_empty() {
                    return None;
                }
                (DhtTarget::Addr(addresses[0]), Some(addresses))
            }
            target => (target, None),
        };
        let (txn, rx, _guard) = self.register_transaction(target.clone(), resolved_addrs);
        let packet = build_get_peers_bytes(&txn, &self.our_id, &info_hash);
        if self.server.is_private(&info_hash) {
            return None;
        }
        // `_guard` removes `txn` from `pending`
        let send_res = self.send_target(&packet, &target).await;
        if send_res.is_err() {
            return None;
        }
        let resp = match tokio::time::timeout(QUERY_TIMEOUT, rx).await {
            Ok(Ok(r)) => r,
            _ => {
                return None;
            }
        };
        parse_get_peers_response(&resp.body)
            .map(|(rid, peers, nodes, token)| (resp.from, rid, peers, nodes, token))
    }

    async fn ping_node(self: &Arc<Self>, target: SocketAddr, expected_id: Id20) -> bool {
        if self.proxy_datagram.is_some() {
            return false;
        }
        let target_kind = DhtTarget::Addr(target);
        let (txn, rx, _guard) = self.register_transaction(target_kind.clone(), Some(vec![target]));
        let packet = build_ping_bytes(&txn, &self.our_id);
        if self.send_target(&packet, &target_kind).await.is_err() {
            return false;
        }
        let Ok(Ok(response)) = tokio::time::timeout(QUERY_TIMEOUT, rx).await else {
            return false;
        };
        let id = response
            .body
            .get(b"r")
            .and_then(|value| value.get(b"id"))
            .and_then(Value::as_bytes)
            .and_then(|bytes| Id20::from_slice(bytes).ok());
        match id {
            Some(id) if id == expected_id => {
                self.add_routing(id, target);
                true
            }
            _ => false,
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

    /// Number of unique nodes currently held in the Kademlia routing table; a coarse health signal for DHT bootstrap progress
    pub fn routing_table_len(&self) -> usize {
        self.routing.lock().len() + self.routing6.lock().len()
    }

    fn add_routing(&self, id: Id20, addr: SocketAddr) {
        if !valid_routing_addr(addr) {
            return;
        }
        if addr.is_ipv6() {
            self.routing6.lock().add(id, addr);
        } else {
            self.routing.lock().add(id, addr);
        }
    }

    fn add_routing_questionable(&self, id: Id20, addr: SocketAddr) {
        if !valid_routing_addr(addr) {
            return;
        }
        if addr.is_ipv6() {
            self.routing6
                .lock()
                .add_with_liveness(id, addr, RoutingLiveness::Questionable);
        } else {
            self.routing
                .lock()
                .add_with_liveness(id, addr, RoutingLiveness::Questionable);
        }
    }

    fn closest_routing(&self, target: &Id20, n: usize) -> Vec<SocketAddr> {
        let mut nodes = self.routing.lock().closest_nodes(target, n);
        nodes.extend(self.routing6.lock().closest_nodes(target, n));
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
        save_routing_state_file(path.as_ref(), &self.routing_snapshot())
    }

    pub fn load_routing_state(&self, path: impl AsRef<Path>) -> std::io::Result<usize> {
        let contacts = load_routing_state_file(path.as_ref())?;
        let count = contacts.len();
        self.add_bootstrap_nodes(contacts);
        Ok(count)
    }

    fn persist_configured_routing_state(&self) {
        if let Some(path) = self.routing_state_path.as_deref() {
            if let Err(error) = self.save_routing_state(path) {
                tracing::warn!(path = %path.display(), "failed to persist DHT routing state: {error}");
            }
        }
    }

    pub fn refresh_routing(&self, limit: usize) -> Vec<(Id20, SocketAddr)> {
        let now = Instant::now();
        let mut routes = self.routing.lock().refresh_stale(now, limit);
        if routes.len() < limit {
            routes.extend(
                self.routing6
                    .lock()
                    .refresh_stale(now, limit - routes.len()),
            );
        }
        routes
    }

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

    pub fn peer_store_len(&self) -> usize {
        self.server.peers.lock().values().map(Vec::len).sum()
    }

    pub fn set_private(&self, info_hash: Id20, private: bool) {
        self.server.set_private(info_hash, private);
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
        I: IntoIterator<Item = (Id20, String, u16)>,
    {
        let mut targets = self.bootstrap_hosts.lock();
        for (_id, host, port) in hosts {
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

    /// BEP-5 PORT support
    pub fn add_node(&self, addr: SocketAddr) {
        self.add_routing_questionable(pseudo_id(addr), addr);
    }

    /// Warm the routing table by iteratively looking up our own id (bootstrap nodes respond with contacts closest to us, which populates a fresh table), returning once the lookup converges or `budget` elapses
    pub async fn bootstrap(self: &Arc<Self>) {
        let target = self.our_id;
        // We discard discovered peers; we only care about the side-effect node population in the routing table
        let mut rx = self.get_peers_stream(target, Duration::from_secs(15), None);
        while rx.recv().await.is_some() {}
    }
}

fn valid_routing_addr(addr: SocketAddr) -> bool {
    addr.port() != 0
        && !addr.ip().is_unspecified()
        && !addr.ip().is_multicast()
        && !matches!(addr, SocketAddr::V4(v4) if v4.ip().is_broadcast())
}

fn load_routing_state_file(path: &Path) -> std::io::Result<Vec<(Id20, SocketAddr)>> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    };
    let mut contacts = Vec::new();
    let mut seen = HashSet::new();
    for line in text
        .lines()
        .skip_while(|line| *line == "risuko-dht-routing-v1")
    {
        if contacts.len() >= MAX_PERSISTED_ROUTES {
            break;
        }
        let Some((id_hex, addr_text)) = line.split_once('\t') else {
            continue;
        };
        let Ok(id_bytes) = hex::decode(id_hex) else {
            continue;
        };
        let Ok(id) = Id20::from_slice(&id_bytes) else {
            continue;
        };
        let Ok(addr) = addr_text.parse::<SocketAddr>() else {
            continue;
        };
        if valid_routing_addr(addr) && seen.insert((id, addr)) {
            contacts.push((id, addr));
        }
    }
    Ok(contacts)
}

fn save_routing_state_file(path: &Path, contacts: &[(Id20, SocketAddr)]) -> std::io::Result<usize> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let mut data = String::from("risuko-dht-routing-v1\n");
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

const BUCKET_SIZE: usize = K;

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
}

#[derive(Debug)]
struct RoutingBucket {
    prefix: [u8; 20],
    prefix_len: u8,
    nodes: Vec<RoutingNode>,
}

impl RoutingBucket {
    fn root() -> Self {
        Self {
            prefix: [0; 20],
            prefix_len: 0,
            nodes: Vec::with_capacity(BUCKET_SIZE),
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
        }
    }
}

#[derive(Debug)]
pub(crate) struct RoutingTable {
    our_id: Id20,
    buckets: Vec<RoutingBucket>,
}

impl RoutingTable {
    fn new(our_id: Id20) -> Self {
        Self {
            our_id,
            buckets: vec![RoutingBucket::root()],
        }
    }

    fn len(&self) -> usize {
        self.buckets.iter().map(|b| b.nodes.len()).sum()
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
        loop {
            let Some(idx) = self.bucket_index(&distance) else {
                return;
            };
            let bucket = &mut self.buckets[idx];
            if let Some(existing) = bucket.nodes.iter_mut().find(|n| n.id == id) {
                if existing.liveness == RoutingLiveness::Good && liveness != RoutingLiveness::Good {
                    return;
                }
                existing.addr = addr;
                existing.last_seen = now;
                if liveness == RoutingLiveness::Good {
                    existing.liveness = RoutingLiveness::Good;
                } else if existing.liveness != RoutingLiveness::Good {
                    existing.liveness = liveness;
                }
                return;
            }
            if bucket.nodes.len() < BUCKET_SIZE {
                bucket.nodes.push(RoutingNode {
                    id,
                    addr,
                    last_seen: now,
                    liveness,
                });
                return;
            }
            if bucket.contains_ours() && bucket.prefix_len < 160 {
                self.split_bucket(idx);
                continue;
            }
            if let Some(replace_idx) = bucket
                .nodes
                .iter()
                .position(|node| node.liveness == RoutingLiveness::Bad)
            {
                bucket.nodes[replace_idx] = RoutingNode {
                    id,
                    addr,
                    last_seen: now,
                    liveness,
                };
            } else if let Some(stalest) = bucket.nodes.iter_mut().min_by_key(|node| node.last_seen)
            {
                stalest.liveness = RoutingLiveness::Questionable;
            }
            return;
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
        let mut all: Vec<(Id20, Id20, SocketAddr)> = self
            .buckets
            .iter()
            .flat_map(|bucket| bucket.nodes.iter())
            .filter(|node| node.liveness != RoutingLiveness::Bad)
            .map(|node| (node.id.distance(target), node.id, node.addr))
            .collect();
        all.sort_by_key(|(dist, _, _)| *dist);
        all.into_iter()
            .take(n)
            .map(|(_, id, addr)| (id, addr))
            .collect()
    }
}

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
            valid_dht_endpoint(addr).then_some((id, addr))
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
    // Stable per-address id for shortlist ordering. Not a real DHT id
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

/// Build a BEP-5 `announce_peer` query; the `token` must be one we received from this node's prior `get_peers` response, otherwise it rejects us
fn build_announce_peer(
    txn: &[u8],
    our_id: &Id20,
    info_hash: &Id20,
    port: u16,
    token: &[u8],
) -> Vec<u8> {
    // Dict keys must be bencode-sorted: id, implied_port, info_hash, port, token
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
        (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
    ]);
    encode_to_vec(&msg)
}

#[cfg(test)]
fn build_get_peers(txn: u16, our_id: &Id20, info_hash: &Id20) -> Vec<u8> {
    build_get_peers_bytes(&txn.to_be_bytes(), our_id, info_hash)
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
        (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
    ]))
}

fn build_get_peers_bytes(txn: &[u8], our_id: &Id20, info_hash: &Id20) -> Vec<u8> {
    let args = Value::Dict(vec![
        (b"id".to_vec(), Value::Bytes(our_id.as_bytes().to_vec())),
        (
            b"info_hash".to_vec(),
            Value::Bytes(info_hash.as_bytes().to_vec()),
        ),
        // BEP-32: request both v4 and v6 contacts; nodes that don't understand `want` ignore it, so this is always safe to send
        (
            b"want".to_vec(),
            Value::List(vec![
                Value::Bytes(b"n4".to_vec()),
                Value::Bytes(b"n6".to_vec()),
            ]),
        ),
    ]);
    let msg = Value::Dict(vec![
        (b"a".to_vec(), args),
        (b"q".to_vec(), Value::Bytes(b"get_peers".to_vec())),
        (b"t".to_vec(), Value::Bytes(txn.to_vec())),
        (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
    ]);
    encode_to_vec(&msg)
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
                        if valid_dht_endpoint(addr) {
                            peers.push(addr);
                        }
                    }
                    18 => {
                        let mut o = [0u8; 16];
                        o.copy_from_slice(&b[..16]);
                        let ip = Ipv6Addr::from(o);
                        let port = u16::from_be_bytes([b[16], b[17]]);
                        let addr = SocketAddr::V6(SocketAddrV6::new(ip, port, 0, 0));
                        if valid_dht_endpoint(addr) {
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
            if valid_dht_endpoint(addr) {
                nodes.push((id, addr));
            }
        }
    }
    if let Some(n6) = r_val.get(b"nodes6").and_then(|v| v.as_bytes()) {
        // Each compact v6 node: 20 bytes id + 16 bytes ipv6 + 2 bytes port
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
            if valid_dht_endpoint(addr) {
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

fn valid_dht_endpoint(addr: SocketAddr) -> bool {
    addr.port() != 0
        && !addr.ip().is_unspecified()
        && !addr.ip().is_multicast()
        && !matches!(addr, SocketAddr::V4(v4) if v4.ip().is_broadcast())
}

pub(crate) fn public_dht_endpoint(addr: SocketAddr) -> bool {
    if !valid_dht_endpoint(addr) {
        return false;
    }
    match addr.ip() {
        IpAddr::V4(ip) => public_dht_ipv4(ip),
        IpAddr::V6(ip) => {
            // IPv4-compatible and IPv4-mapped IPv6 addresses carry an IPv4
            // endpoint, so apply the IPv4 policy before accepting the address.
            if let Some(ipv4) = ip.to_ipv4() {
                return public_dht_endpoint(SocketAddr::new(
                    ipv4.into(),
                    addr.port(),
                ));
            }
            let segments = ip.segments();
            // Documentation and benchmarking prefixes are not publicly
            // routable DHT endpoints even though they are global unicast.
            let documentation = segments[0] == 0x2001
                && (segments[1] == 0x0db8 || segments[1] == 0x0002);
            let orchid = segments[0] == 0x2001
                && ((segments[1] & 0xfff0) == 0x0010
                    || (segments[1] & 0xfff0) == 0x0020);
            !ip.is_loopback()
                && !ip.is_unicast_link_local()
                && !ip.is_unique_local()
                && !documentation
                && !orchid
        }
    }
}

fn public_dht_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    // Ipv4Addr::is_private does not include CGNAT, reserved, benchmarking,
    // or several IANA special-purpose ranges.
    octets[0] != 0
        && !ip.is_unspecified()
        && !ip.is_loopback()
        && !ip.is_private()
        && !ip.is_link_local()
        && !ip.is_broadcast()
        && !ip.is_multicast()
        && !ip.is_documentation()
        && !(octets[0] == 100 && (64..=127).contains(&octets[1]))
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

fn krpc_error(tid: &[u8], code: i64, message: &'static [u8]) -> Vec<u8> {
    encode_to_vec(&Value::Dict(vec![
        (
            b"e".to_vec(),
            Value::List(vec![Value::Int(code), Value::Bytes(message.to_vec())]),
        ),
        (b"t".to_vec(), Value::Bytes(tid.to_vec())),
        (b"y".to_vec(), Value::Bytes(b"e".to_vec())),
    ]))
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
        let encoded = encode_to_vec(&Value::Dict(response.clone()));
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
        return Some(krpc_error(tid, 203, b"missing query"));
    };
    let Some(args) = msg.get(b"a").and_then(Value::as_dict) else {
        return Some(krpc_error(tid, 203, b"missing arguments"));
    };
    let args = Value::Dict(args.to_vec());
    let remote_id = args
        .get(b"id")
        .and_then(Value::as_bytes)
        .and_then(|bytes| Id20::from_slice(bytes).ok());
    let Some(remote_id) = remote_id else {
        return Some(krpc_error(tid, 203, b"invalid id"));
    };
    server.add_routing(remote_id, from);

    let mut response = vec![
        (
            b"r".to_vec(),
            Value::Dict(vec![(
                b"id".to_vec(),
                Value::Bytes(server.our_id.as_bytes().to_vec()),
            )]),
        ),
        (b"t".to_vec(), Value::Bytes(tid.to_vec())),
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
                return Some(krpc_error(tid, 203, b"invalid target"));
            };
            let (want4, want6) = parse_want(&args, from);
            let nodes = server.closest_nodes(&target, K * 2);
            let mut body = vec![(
                b"id".to_vec(),
                Value::Bytes(server.our_id.as_bytes().to_vec()),
            )];
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
                return Some(krpc_error(tid, 203, b"invalid info_hash"));
            };
            let (want4, want6) = parse_want(&args, from);
            let peers = server.get_peers(hash, Some(from.is_ipv6()));
            let nodes = server.closest_nodes(&hash, K * 2);
            let mut body = vec![
                (
                    b"id".to_vec(),
                    Value::Bytes(server.our_id.as_bytes().to_vec()),
                ),
                (b"token".to_vec(), Value::Bytes(server.token(from))),
            ];
            let values: Vec<Value> = peers
                .into_iter()
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
                return Some(krpc_error(tid, 203, b"invalid info_hash"));
            };
            let Some(token) = token else {
                return Some(krpc_error(tid, 203, b"missing token"));
            };
            if !server.valid_token(from, token) {
                return Some(krpc_error(tid, 203, b"invalid token"));
            }
            let implied = args
                .get(b"implied_port")
                .and_then(Value::as_int)
                .unwrap_or(0);
            if implied != 0 && implied != 1 {
                return Some(krpc_error(tid, 203, b"invalid implied_port"));
            }
            let implied = implied == 1;
            let port = if implied {
                from.port()
            } else {
                args.get(b"port")
                    .and_then(Value::as_int)
                    .and_then(|p| u16::try_from(p).ok())
                    .unwrap_or(0)
            };
            if port == 0 {
                return Some(krpc_error(tid, 203, b"invalid port"));
            }
            server.add_peer(hash, SocketAddr::new(from.ip(), port));
        }
        _ => return Some(krpc_error(tid, 204, b"method unknown")),
    }
    Some(bounded_response_bytes(&mut response))
}

async fn reader_loop(
    sock: Arc<UdpSocket>,
    pending: Arc<Mutex<PendingMap>>,
    server: Arc<InboundDhtState>,
) {
    let mut buf = vec![0u8; 2048];
    loop {
        let (n, from) = match sock.recv_from(&mut buf).await {
            Ok(x) => x,
            Err(_) => return,
        };
        let Ok(msg) = decode_all_external(&buf[..n], KRPC_DECODE_LIMITS) else {
            if let Some(tid) = recover_transaction_id(&buf[..n]) {
                let reply = krpc_error(&tid, 203, b"invalid bencode");
                let _ = sock.send_to(&reply, from).await;
            }
            continue;
        };
        let Some(ty) = msg.get(b"y").and_then(|v| v.as_bytes()) else {
            continue;
        };
        if ty == b"q" {
            if let Some(reply) = build_query_response(&msg, from, &server) {
                let _ = sock.send_to(&reply, from).await;
            }
            continue;
        }
        if ty != b"r" && ty != b"e" {
            continue;
        }
        let Some(tid) = msg.get(b"t").and_then(|v| v.as_bytes()) else {
            continue;
        };
        let txn = tid.to_vec();
        let mut guard = pending.lock();
        if let Some(entry) = guard.get(&txn) {
            let matches = entry
                .resolved_addrs
                .as_ref()
                .is_some_and(|addresses| addresses.contains(&from))
                || (entry.resolved_addrs.is_none()
                    && dht_targets_match(&entry.target, &DhtTarget::Addr(from)));
            if matches {
                if let Some(entry) = guard.remove(&txn) {
                    let _ = entry.tx.send(KrpcResponse {
                        from: DhtTarget::Addr(from),
                        body: msg,
                    });
                }
            }
            // Mismatch: ignore the packet, leave entry for the real responder
        }
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

async fn proxy_reader_loop(sock: Arc<ProxyDatagram>, pending: Arc<Mutex<PendingMap>>) {
    let mut buf = vec![0u8; 2048];
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
    use crate::bencode::decode_all;
    use std::io::Write;

    // Shared-DHT tests mutate process-wide state
    static SHARED_TEST_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
        std::sync::OnceLock::new();

    async fn reset_shared_state() {
        let previous = {
            let mut state = shared_dht_cell().lock().await;
            state.proxy_requested = None;
            state.last_error = None;
            state.dht.take()
        };
        if let Some(previous) = previous {
            previous.shutdown().await;
        }
    }

    #[tokio::test]
    async fn proxied_dht_failure_blocks_implicit_direct_creation() {
        let lock = SHARED_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
        let _guard = lock.lock().await;
        reset_shared_state().await;

        // HTTP CONNECT can carry TCP but must fail DHT's UDP association
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

        let error = match Dht::prepare_shared_with_proxy(Some(proxy)).await {
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
        rt.add(n1, addr); // dedup → still 1
        assert_eq!(rt.len(), 1);
        let n2 = Id20::from_slice(&[2u8; 20]).unwrap();
        rt.add(n2, addr);
        assert_eq!(rt.len(), 2);
        // Inserting our own id is a no-op
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
        // All ids share the most-significant bit set (byte[0] = 0x80) so bit_pos == 0 for the entire batch and every entry lands in bucket 0, which must cap at K
        for i in 1..=20u8 {
            let mut id = [0u8; 20];
            id[0] = 0x80;
            id[19] = i;
            rt.add(
                Id20::from_slice(&id).unwrap(),
                "127.0.0.1:1".parse().unwrap(),
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
            (first, "127.0.0.1:6881".parse().unwrap()),
            (second, "[::1]:6881".parse().unwrap()),
        ];
        assert_eq!(save_routing_state_file(&path, &contacts).unwrap(), 2);
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"not-a-contact\n0011\t0.0.0.0:1\n")
            .unwrap();

        assert_eq!(load_routing_state_file(&path).unwrap(), contacts);
    }

    #[tokio::test]
    async fn configured_routing_state_loads_and_saves_on_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dht-routing.state");
        let first = Id20::from_slice(&[1u8; 20]).unwrap();
        let addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        save_routing_state_file(&path, &[(first, addr)]).unwrap();

        let dht = Dht::spawn_with_routing_state(path.clone()).await.unwrap();
        assert_eq!(dht.routing_table_len(), 1);
        let second = Id20::from_slice(&[2u8; 20]).unwrap();
        dht.add_bootstrap_nodes([(second, "[::1]:6881".parse().unwrap())]);
        dht.shutdown().await;
        let restored = load_routing_state_file(&path).unwrap();
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
        // values: [6-byte peer for 1.2.3.4:5678]; nodes: 26 bytes (id=0x22... ip=9.8.7.6 port=11111)
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
        // 18-byte compact v6 peer: ip=::1 port=6881
        let mut peer6: Vec<u8> = Vec::with_capacity(18);
        peer6.extend_from_slice(&Ipv6Addr::LOCALHOST.octets());
        peer6.extend_from_slice(&6881u16.to_be_bytes());
        // 38-byte compact v6 node: id=0x33... ip=2001:db8::1 port=12345
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
            SocketAddr::V6(SocketAddrV6::new(Ipv6Addr::LOCALHOST, 6881, 0, 0))
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
        let reply = build_query_response(&ping, "127.0.0.1:6000".parse().unwrap(), &state).unwrap();
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

        // `want` asks for IPv6 routing nodes, but the KRPC packet itself was
        // sent over IPv4. BEP-32 requires 6-byte IPv4 peer values here.
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
        let from: SocketAddr = "127.0.0.1:6000".parse().unwrap();
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
            assert_eq!(reply.get(b"y").and_then(Value::as_bytes), Some(b"r" as &[u8]));
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
    fn announce_peer_rejects_invalid_implied_port_value() {
        let our_id = Id20::from_slice(&[9u8; 20]).unwrap();
        let state = InboundDhtState::new(our_id, Arc::new(Mutex::new(RoutingTable::new(our_id))));
        let remote = Id20::from_slice(&[7u8; 20]).unwrap();
        let hash = Id20::from_slice(&[3u8; 20]).unwrap();
        let from: SocketAddr = "127.0.0.1:6000".parse().unwrap();
        let token = state.token(from);
        let request = Value::Dict(vec![
            (
                b"a".to_vec(),
                Value::Dict(vec![
                    (b"id".to_vec(), Value::Bytes(remote.as_bytes().to_vec())),
                    (b"implied_port".to_vec(), Value::Int(2)),
                    (
                        b"info_hash".to_vec(),
                        Value::Bytes(hash.as_bytes().to_vec()),
                    ),
                    (b"port".to_vec(), Value::Int(6881)),
                    (b"token".to_vec(), Value::Bytes(token)),
                ]),
            ),
            (b"q".to_vec(), Value::Bytes(b"announce_peer".to_vec())),
            (b"t".to_vec(), Value::Bytes(b"bad".to_vec())),
            (b"y".to_vec(), Value::Bytes(b"q".to_vec())),
        ]);
        let reply = decode_all(&build_query_response(&request, from, &state).unwrap()).unwrap();
        assert_eq!(
            reply.get(b"y").and_then(Value::as_bytes),
            Some(b"e" as &[u8])
        );
        assert!(state.get_peers(hash, None).is_empty());
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
}
