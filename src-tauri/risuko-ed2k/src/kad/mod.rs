pub mod routing;
pub mod state;
pub mod wire;

use std::collections::HashSet;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::future::{BoxFuture, FutureExt};
use futures_util::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot, watch, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;

use risuko_http::{ProxyAssociation, ProxyDatagramSource};

use self::routing::{
    is_public_ipv4, KadId, LookupConfig, LookupTracker, NodeId, RoutingTable, SourceSet,
};
use self::wire::{
    build_bootstrap_request, build_hello_request, build_routing_request,
    build_source_search_request, parse_bootstrap_response, parse_hello, parse_pong,
    parse_routing_request, parse_routing_response_with_limit, parse_source_search_request,
    parse_source_search_response, KadPacket, KadSourceRecord, KadWireContact, WireError,
    OP_BOOTSTRAP_REQ, OP_BOOTSTRAP_RES, OP_HELLO_REQ, OP_HELLO_RES, OP_PING, OP_PONG,
    OP_ROUTING_REQ, OP_ROUTING_RES, OP_SEARCH_RES, OP_SEARCH_SOURCE_REQ, TAG_SOURCE_UDP_PORT,
};

const KAD_VERSION: u8 = 0x08;
const MIN_SOURCE_SEARCH_KAD_VERSION: u8 = 3;
const MAX_BOOTSTRAP_SEEDS: usize = 32;
const MAX_BOOTSTRAP_PROBES: usize = 8;
const MAX_SOURCE_QUERIES: usize = 32;
const SOURCE_CHANNEL_CAPACITY: usize = 300;
const MAX_SOURCE_RESPONSE_PACKETS: usize = 6;
const SOURCE_RESPONSE_IDLE: Duration = Duration::from_millis(250);
const MAX_LIVENESS_PROBES: usize = routing::ALPHA;
const STATE_CHECKPOINT_DEBOUNCE: Duration = Duration::from_secs(2);
const WARM_ROUTING_WINDOW_MS: u64 = 10 * 60 * 1000;

#[derive(Debug, Error)]
pub enum KadError {
    #[error("Kad UDP bind failed: {0}")]
    Bind(#[source] std::io::Error),
    #[error("Kad UDP I/O failed: {0}")]
    Io(#[source] std::io::Error),
    #[error("Kad wire error: {0}")]
    Wire(#[from] WireError),
    #[error("Kad state error: {0}")]
    State(#[source] std::io::Error),
    #[error("Kad lookup cancelled")]
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct KadConfig {
    pub config_dir: PathBuf,
    pub bind_addr: Ipv4Addr,
    pub udp_port: u16,
    pub tcp_port: u16,
    pub proxy: Option<risuko_http::ProxyConnector>,
    pub lookup: LookupConfig,
    bootstrap_seeds: Option<Vec<SocketAddrV4>>,
    #[cfg(test)]
    allow_private_contacts: bool,
}

impl KadConfig {
    pub fn new(config_dir: impl Into<PathBuf>, udp_port: u16, tcp_port: u16) -> Self {
        Self {
            config_dir: config_dir.into(),
            bind_addr: Ipv4Addr::UNSPECIFIED,
            udp_port,
            tcp_port,
            proxy: None,
            lookup: LookupConfig::default(),
            bootstrap_seeds: None,
            #[cfg(test)]
            allow_private_contacts: false,
        }
    }

    pub fn with_proxy(mut self, proxy: Option<risuko_http::ProxyConnector>) -> Self {
        self.proxy = proxy;
        self
    }

    #[cfg(test)]
    fn with_bootstrap_seeds_for_test(mut self, seeds: Vec<SocketAddrV4>) -> Self {
        self.bootstrap_seeds = Some(seeds);
        self
    }

    #[cfg(test)]
    fn with_loopback_contacts_for_test(mut self) -> Self {
        self.allow_private_contacts = true;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum KadState {
    Disabled,
    Bootstrapping,
    Searching,
    Ready,
    Timeout,
    Error,
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KadHealthSnapshot {
    pub enabled: bool,
    #[serde(default)]
    pub bound: bool,
    pub state: KadState,
    pub udp_port: u16,
    pub node_id: String,
    pub routing_contacts: usize,
    pub cached_contacts: usize,
    pub last_bootstrap_at_ms: Option<u64>,
    pub last_lookup_at_ms: Option<u64>,
    pub last_lookup_success: Option<bool>,
    pub last_error: Option<String>,
}

impl KadHealthSnapshot {
    pub fn disabled(port: u16) -> Self {
        Self {
            enabled: false,
            bound: false,
            state: KadState::Disabled,
            udp_port: port,
            node_id: String::new(),
            routing_contacts: 0,
            cached_contacts: 0,
            last_bootstrap_at_ms: None,
            last_lookup_at_ms: None,
            last_lookup_success: None,
            last_error: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KadLookupStatus {
    pub state: KadState,
    pub queried_nodes: usize,
    pub discovered_sources: usize,
    pub contacts: usize,
    pub error: Option<String>,
}

impl Default for KadLookupStatus {
    fn default() -> Self {
        Self {
            state: KadState::Bootstrapping,
            queried_nodes: 0,
            discovered_sources: 0,
            contacts: 0,
            error: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct KadSource {
    pub client_hash: KadId,
    pub addr: SocketAddrV4,
    pub source_type: u8,
}

pub struct KadLookup {
    pub sources: mpsc::Receiver<KadSource>,
    pub status: watch::Receiver<KadLookupStatus>,
    completion: JoinHandle<Result<(), KadError>>,
}

impl KadLookup {
    pub fn into_parts(
        self,
    ) -> (
        mpsc::Receiver<KadSource>,
        watch::Receiver<KadLookupStatus>,
        JoinHandle<Result<(), KadError>>,
    ) {
        (self.sources, self.status, self.completion)
    }

    pub async fn finish(self) -> Result<(), KadError> {
        self.completion.await.unwrap_or(Err(KadError::Cancelled))
    }
}

const MAX_SHORTLIST: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeState {
    Unqueried,
    InFlight,
    Responded,
    Failed,
}

struct ShortlistEntry {
    contact: routing::Contact,
    state: NodeState,
    source_sent: bool,
}

struct Shortlist {
    target: KadId,
    entries: Vec<ShortlistEntry>,
    known: HashSet<NodeId>,
}

impl Shortlist {
    fn new(target: KadId, contacts: Vec<routing::Contact>) -> Self {
        let mut list = Self {
            target,
            entries: Vec::new(),
            known: HashSet::new(),
        };
        for contact in contacts {
            list.add(contact);
        }
        list
    }

    fn add(&mut self, contact: routing::Contact) -> bool {
        if !self.known.insert(contact.id) {
            return false;
        }
        self.entries.push(ShortlistEntry {
            contact,
            state: NodeState::Unqueried,
            source_sent: false,
        });
        let target = self.target;
        self.entries.sort_by(|left, right| {
            routing::compare_distance(&target, &left.contact.id.0, &right.contact.id.0)
        });
        self.entries.truncate(MAX_SHORTLIST);
        true
    }

    fn window(&mut self) -> impl Iterator<Item = &mut ShortlistEntry> {
        self.entries
            .iter_mut()
            .filter(|entry| entry.state != NodeState::Failed)
            .take(routing::K)
    }

    fn set_state(&mut self, id: NodeId, state: NodeState) {
        if let Some(entry) = self.entries.iter_mut().find(|entry| entry.contact.id == id) {
            entry.state = state;
        }
    }

    fn next_unqueried(&mut self) -> Option<routing::Contact> {
        self.window()
            .find(|entry| entry.state == NodeState::Unqueried)
            .map(|entry| {
                entry.state = NodeState::InFlight;
                entry.contact.clone()
            })
    }

    fn has_pending_routing(&mut self) -> bool {
        self.window()
            .any(|entry| entry.state == NodeState::Unqueried)
    }

    fn next_source_candidate(&mut self) -> Option<routing::Contact> {
        self.window()
            .find(|entry| {
                entry.state == NodeState::Responded
                    && !entry.source_sent
                    && entry.contact.version >= MIN_SOURCE_SEARCH_KAD_VERSION
            })
            .map(|entry| {
                entry.source_sent = true;
                entry.contact.clone()
            })
    }
}

enum LookupReply {
    Routing(NodeId, Result<KadPacket, KadError>),
    Source(Result<Vec<KadPacket>, KadError>),
}

#[derive(Default)]
struct SourceSink {
    seen_ids: HashSet<KadId>,
    seen_addrs: SourceSet,
}

#[derive(Clone, Copy)]
enum RequestExpectation {
    Bootstrap,
    Hello,
    Routing { target: KadId, max_contacts: usize },
    Source(KadId),
    Pong,
}

impl RequestExpectation {
    fn from_request(packet: &KadPacket) -> Result<Self, KadError> {
        match packet.opcode {
            OP_BOOTSTRAP_REQ => Ok(Self::Bootstrap),
            OP_HELLO_REQ => Ok(Self::Hello),
            OP_ROUTING_REQ => {
                let (kind, target, _) =
                    parse_routing_request(&packet.payload).map_err(KadError::Wire)?;
                let max_contacts = if kind == 2 { 2 } else { 32 };
                Ok(Self::Routing {
                    target,
                    max_contacts,
                })
            }
            OP_SEARCH_SOURCE_REQ => {
                let (target, _, _) =
                    parse_source_search_request(&packet.payload).map_err(KadError::Wire)?;
                Ok(Self::Source(target))
            }
            OP_PING => Ok(Self::Pong),
            _ => Err(KadError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "unsupported Kad request opcode",
            ))),
        }
    }

    fn matches(self, packet: &KadPacket) -> bool {
        match self {
            Self::Bootstrap => packet.opcode == OP_BOOTSTRAP_RES,
            Self::Hello => packet.opcode == OP_HELLO_RES,
            Self::Pong => packet.opcode == OP_PONG && parse_pong(&packet.payload).is_ok(),
            Self::Routing {
                target,
                max_contacts,
            } => {
                packet.opcode == OP_ROUTING_RES
                    && parse_routing_response_with_limit(&packet.payload, max_contacts)
                        .is_ok_and(|response| response.target == target)
            }
            Self::Source(target) => {
                packet.opcode == OP_SEARCH_RES
                    && parse_source_search_response(&packet.payload)
                        .is_ok_and(|response| response.target == target)
            }
        }
    }
}

struct KadRuntime {
    socket: Mutex<Option<Arc<KadSocket>>>,
    pending: Arc<Mutex<Vec<PendingRequest>>>,
    next_request_id: AtomicU64,
    dispatcher: Mutex<Option<JoinHandle<()>>>,
    routing: Mutex<RoutingTable>,
    node_id: NodeId,
    config: KadConfig,
    config_dir: PathBuf,
    shutdown: CancellationToken,
    health: RwLock<KadHealthSnapshot>,
    seeds: Vec<SocketAddrV4>,
    seed_cursor: AtomicUsize,
    liveness: Mutex<HashSet<NodeId>>,
    active_tasks: Arc<KadTaskTracker>,
    checkpoint_tx: mpsc::Sender<()>,
    checkpoint_worker: Mutex<Option<JoinHandle<()>>>,
}

enum KadSocket {
    Direct(Arc<UdpSocket>),
    Proxied(Arc<ProxyAssociation>),
}

impl KadSocket {
    async fn send_to(&self, payload: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        match self {
            Self::Direct(socket) => socket.send_to(payload, target).await,
            Self::Proxied(socket) => socket
                .send_to(payload, target)
                .await
                .map_err(|error| std::io::Error::other(error.to_string())),
        }
    }

    async fn recv_from(&self, buffer: &mut [u8]) -> std::io::Result<(usize, ProxyDatagramSource)> {
        match self {
            Self::Direct(socket) => socket
                .recv_from(buffer)
                .await
                .map(|(length, source)| (length, ProxyDatagramSource::Ip(source))),
            Self::Proxied(socket) => socket
                .recv_from_target(buffer)
                .await
                .map_err(|error| std::io::Error::other(error.to_string())),
        }
    }

    fn local_port(&self) -> std::io::Result<u16> {
        match self {
            Self::Direct(socket) => Ok(socket.local_addr()?.port()),
            Self::Proxied(socket) => socket
                .local_addr()
                .map(|address| address.port())
                .map_err(|error| std::io::Error::other(error.to_string())),
        }
    }
}

fn kad_source_matches(target: SocketAddr, source: &ProxyDatagramSource) -> bool {
    risuko_http::datagram_source_matches(source, target)
}

struct KadTaskTracker {
    state: parking_lot::Mutex<KadTaskTrackerState>,
    active_tx: watch::Sender<usize>,
}

#[derive(Default)]
struct KadTaskTrackerState {
    closing: bool,
    active: usize,
}

struct KadTaskGuard(Arc<KadTaskTracker>);

impl KadTaskTracker {
    fn new() -> Arc<Self> {
        let (active_tx, _) = watch::channel(0);
        Arc::new(Self {
            state: parking_lot::Mutex::new(KadTaskTrackerState::default()),
            active_tx,
        })
    }

    fn try_start(self: &Arc<Self>) -> Option<KadTaskGuard> {
        let mut state = self.state.lock();
        if state.closing {
            return None;
        }
        state.active += 1;
        self.active_tx.send_replace(state.active);
        Some(KadTaskGuard(self.clone()))
    }

    async fn close_and_wait(&self) {
        let mut active = self.active_tx.subscribe();
        {
            let mut state = self.state.lock();
            state.closing = true;
        }
        while *active.borrow() != 0 {
            let _ = active.changed().await;
        }
    }

    fn finish(&self) {
        let mut state = self.state.lock();
        debug_assert!(state.active > 0, "Kad task tracker underflow");
        if state.active == 0 {
            return;
        }
        state.active -= 1;
        self.active_tx.send_replace(state.active);
    }

    #[cfg(test)]
    fn active_count(&self) -> usize {
        self.state.lock().active
    }
}

impl Drop for KadTaskGuard {
    fn drop(&mut self) {
        self.0.finish();
    }
}

struct PendingRequest {
    id: u64,
    target: SocketAddr,
    expectation: RequestExpectation,
    response: PendingResponse,
}

enum PendingResponse {
    Single(oneshot::Sender<KadPacket>),
    Stream(mpsc::Sender<KadPacket>),
}

pub struct KadService {
    runtime: Arc<KadRuntime>,
}

impl KadService {
    pub async fn bind(mut config: KadConfig) -> Result<Arc<Self>, KadError> {
        if config.udp_port == 0 {
            return Err(KadError::Bind(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Kad UDP port must be in 1..=65535",
            )));
        }
        let loaded = state::load(&config.config_dir, None).map_err(KadError::State)?;
        let needs_initial_save = loaded.kind != state::StateLoadKind::Existing;
        let loaded = if needs_initial_save {
            let config_dir = config.config_dir.clone();
            tokio::task::spawn_blocking(move || {
                state::save(&config_dir, loaded.node_id, &loaded.contacts)?;
                Ok::<_, std::io::Error>(loaded)
            })
            .await
            .map_err(|error| {
                KadError::State(std::io::Error::other(format!(
                    "initial Kad state persistence task failed: {error}"
                )))
            })?
            .map_err(KadError::State)?
        } else {
            loaded
        };
        let socket = match config.proxy.clone() {
            Some(proxy) => {
                let has_explicit_bypass = proxy
                    .udp_no_proxy()
                    .is_some_and(|matcher| !matcher.is_empty());
                let with_bypass = proxy.supports_udp() || has_explicit_bypass;
                let datagram = if with_bypass {
                    proxy.bind_udp_with_bypass().await
                } else {
                    proxy.bind_udp().await
                }
                .map_err(|error| {
                    KadError::Bind(std::io::Error::new(
                        std::io::ErrorKind::Unsupported,
                        error.to_string(),
                    ))
                })?;
                KadSocket::Proxied(Arc::new(ProxyAssociation::new(
                    proxy,
                    with_bypass,
                    datagram,
                )))
            }
            None => KadSocket::Direct(Arc::new(
                UdpSocket::bind(SocketAddrV4::new(config.bind_addr, config.udp_port))
                    .await
                    .map_err(KadError::Bind)?,
            )),
        };
        let bound_port = match &socket {
            KadSocket::Direct(_) => socket.local_port().map_err(KadError::Bind)?,
            KadSocket::Proxied(_) => config.udp_port,
        };
        config.udp_port = bound_port;
        let node_id = loaded.node_id;
        let mut routing = RoutingTable::new(node_id);
        for contact in loaded.contacts {
            let _ = routing.insert(contact);
        }
        let cached_contacts = routing.len();
        let seeds = config.bootstrap_seeds.clone().unwrap_or_else(bundled_seeds);
        let health = KadHealthSnapshot {
            enabled: true,
            bound: true,
            state: KadState::Bootstrapping,
            udp_port: bound_port,
            node_id: node_id.to_string(),
            routing_contacts: cached_contacts,
            cached_contacts,
            last_bootstrap_at_ms: None,
            last_lookup_at_ms: None,
            last_lookup_success: None,
            last_error: None,
        };
        let socket = Arc::new(socket);
        let pending = Arc::new(Mutex::new(Vec::new()));
        let shutdown = CancellationToken::new();
        let (checkpoint_tx, checkpoint_rx) = mpsc::channel(1);
        let dispatcher = tokio::spawn(run_dispatcher(
            socket.clone(),
            pending.clone(),
            shutdown.clone(),
        ));
        let runtime = Arc::new(KadRuntime {
            socket: Mutex::new(Some(socket)),
            pending,
            next_request_id: AtomicU64::new(1),
            dispatcher: Mutex::new(Some(dispatcher)),
            routing: Mutex::new(routing),
            node_id,
            config_dir: config.config_dir.clone(),
            config,
            shutdown,
            health: RwLock::new(health),
            seeds,
            seed_cursor: AtomicUsize::new(0),
            liveness: Mutex::new(HashSet::new()),
            active_tasks: KadTaskTracker::new(),
            checkpoint_tx,
            checkpoint_worker: Mutex::new(None),
        });
        let checkpoint_worker = tokio::spawn(run_checkpoint_worker(runtime.clone(), checkpoint_rx));
        *runtime.checkpoint_worker.lock().await = Some(checkpoint_worker);
        Ok(Arc::new(Self { runtime }))
    }

    pub fn node_id(&self) -> NodeId {
        self.runtime.node_id
    }

    pub fn udp_port(&self) -> u16 {
        self.runtime.config.udp_port
    }

    pub fn advertised_udp_port(&self) -> Option<u16> {
        self.runtime
            .config
            .proxy
            .is_none()
            .then_some(self.runtime.config.udp_port)
    }

    pub fn lookup_sources(
        self: &Arc<Self>,
        file_hash: KadId,
        file_size: u64,
        cancel: CancellationToken,
    ) -> KadLookup {
        self.lookup_sources_inner(file_hash, file_size, None, cancel, SOURCE_CHANNEL_CAPACITY)
    }

    pub fn lookup_sources_for_client(
        self: &Arc<Self>,
        file_hash: KadId,
        file_size: u64,
        client_hash: KadId,
        cancel: CancellationToken,
    ) -> KadLookup {
        self.lookup_sources_inner(
            file_hash,
            file_size,
            Some(client_hash),
            cancel,
            SOURCE_CHANNEL_CAPACITY,
        )
    }

    #[cfg(test)]
    fn lookup_sources_with_capacity_for_test(
        self: &Arc<Self>,
        file_hash: KadId,
        file_size: u64,
        cancel: CancellationToken,
        source_channel_capacity: usize,
    ) -> KadLookup {
        self.lookup_sources_inner(
            file_hash,
            file_size,
            None,
            cancel,
            source_channel_capacity.max(1),
        )
    }

    fn lookup_sources_inner(
        self: &Arc<Self>,
        file_hash: KadId,
        file_size: u64,
        client_hash: Option<KadId>,
        cancel: CancellationToken,
        source_channel_capacity: usize,
    ) -> KadLookup {
        let (source_tx, source_rx) = mpsc::channel(source_channel_capacity);
        let (status_tx, status_rx) = watch::channel(KadLookupStatus::default());
        let Some(task_guard) = self.runtime.active_tasks.try_start() else {
            let status = KadLookupStatus {
                state: KadState::Stopped,
                ..KadLookupStatus::default()
            };
            let _ = status_tx.send(status);
            return KadLookup {
                sources: source_rx,
                status: status_rx,
                completion: tokio::spawn(async { Err(KadError::Cancelled) }),
            };
        };
        let service = self.clone();
        let completion = tokio::spawn(async move {
            let _task_guard = task_guard;
            service
                .run_lookup(
                    file_hash,
                    file_size,
                    client_hash,
                    cancel,
                    source_tx,
                    status_tx,
                )
                .await
        });
        KadLookup {
            sources: source_rx,
            status: status_rx,
            completion,
        }
    }

    pub async fn health_snapshot(&self) -> KadHealthSnapshot {
        self.runtime.health.read().await.clone()
    }

    pub async fn shutdown(&self) {
        self.runtime.shutdown.cancel();
        self.runtime.active_tasks.close_and_wait().await;
        if let Some(dispatcher) = self.runtime.dispatcher.lock().await.take() {
            let _ = dispatcher.await;
        }
        if let Some(worker) = self.runtime.checkpoint_worker.lock().await.take() {
            let _ = worker.await;
        }
        self.runtime.socket.lock().await.take();
        persist_runtime_state(&self.runtime).await;
        let contact_count = self.runtime.routing.lock().await.len();
        let mut health = self.runtime.health.write().await;
        health.state = KadState::Stopped;
        health.bound = false;
        health.routing_contacts = contact_count;
    }

    async fn routing_is_warm(&self) -> bool {
        let (last_at, success) = {
            let health = self.runtime.health.read().await;
            (health.last_lookup_at_ms, health.last_lookup_success)
        };
        let recent = last_at.is_some_and(|at| now_ms().saturating_sub(at) < WARM_ROUTING_WINDOW_MS);
        recent && success == Some(true) && self.runtime.routing.lock().await.len() >= routing::K
    }

    fn lookup_is_cancelled(&self, cancel: &CancellationToken) -> bool {
        cancel.is_cancelled() || self.runtime.shutdown.is_cancelled()
    }

    async fn run_lookup(
        self: Arc<Self>,
        file_hash: KadId,
        file_size: u64,
        client_hash: Option<KadId>,
        cancel: CancellationToken,
        source_tx: mpsc::Sender<KadSource>,
        status_tx: watch::Sender<KadLookupStatus>,
    ) -> Result<(), KadError> {
        let started = Instant::now();
        let deadline = started + self.runtime.config.lookup.clone().bounded().deadline;
        let mut status = KadLookupStatus::default();
        if self.lookup_is_cancelled(&cancel) {
            status.state = KadState::Stopped;
            let _ = status_tx.send(status);
            return Ok(());
        }
        let warm = self.routing_is_warm().await;
        self.begin_lookup_health().await;

        let result = match if warm {
            Ok(())
        } else {
            self.bootstrap_if_needed(&cancel, deadline, &mut status, &status_tx)
                .await
        } {
            Ok(()) => {
                status.state = KadState::Searching;
                let _ = status_tx.send(status.clone());
                self.search_sources(
                    file_hash,
                    file_size,
                    client_hash,
                    &cancel,
                    deadline,
                    &mut status,
                    &status_tx,
                    &source_tx,
                )
                .await
            }
            Err(error) => Err(error),
        };

        match result {
            Ok(()) => {
                status.state = KadState::Ready;
                self.set_health_state(KadState::Ready, None).await;
                self.update_lookup_health(true, None).await;
            }
            Err(KadError::Cancelled) => {
                status.state = KadState::Stopped;
            }
            Err(error) => {
                let timed_out = matches!(
                    &error,
                    KadError::Io(io_error)
                        if io_error.kind() == std::io::ErrorKind::TimedOut
                ) || started.elapsed()
                    >= self.runtime.config.lookup.clone().bounded().deadline;
                status.state = if timed_out {
                    KadState::Timeout
                } else {
                    KadState::Error
                };
                status.error = Some(error.to_string());
                self.set_health_state(status.state, status.error.clone())
                    .await;
                self.update_lookup_health(false, status.error.clone()).await;
                let _ = status_tx.send(status.clone());
                return Err(error);
            }
        }
        let _ = status_tx.send(status);
        Ok(())
    }

    async fn bootstrap_if_needed(
        self: &Arc<Self>,
        cancel: &CancellationToken,
        deadline: Instant,
        status: &mut KadLookupStatus,
        status_tx: &watch::Sender<KadLookupStatus>,
    ) -> Result<(), KadError> {
        let mut cached_targets: Vec<_> = self
            .runtime
            .routing
            .lock()
            .await
            .contacts()
            .into_iter()
            .filter(|contact| contact.addr.port() != 0)
            .take(routing::K)
            .map(|contact| contact.udp_addr())
            .collect();
        cached_targets.sort_unstable();
        cached_targets.dedup();
        let mut successful = self
            .probe_bootstrap_targets(cached_targets, cancel, deadline, status, status_tx)
            .await?;

        if self.lookup_is_cancelled(cancel) {
            return Err(KadError::Cancelled);
        }

        if successful < routing::K {
            let seed_targets = self.rotated_seed_targets();
            successful += self
                .probe_bootstrap_targets(seed_targets, cancel, deadline, status, status_tx)
                .await?;
        }

        if self.lookup_is_cancelled(cancel) {
            return Err(KadError::Cancelled);
        }
        if successful == 0 {
            return Err(KadError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Kad bootstrap produced no responses",
            )));
        }
        Ok(())
    }

    async fn probe_bootstrap_targets(
        self: &Arc<Self>,
        targets: Vec<SocketAddrV4>,
        cancel: &CancellationToken,
        deadline: Instant,
        status: &mut KadLookupStatus,
        status_tx: &watch::Sender<KadLookupStatus>,
    ) -> Result<usize, KadError> {
        let mut unique_targets = Vec::with_capacity(targets.len());
        let mut seen = HashSet::new();
        for target in targets {
            if seen.insert(target) {
                unique_targets.push(target);
            }
        }
        if unique_targets.is_empty() {
            return Ok(0);
        }

        let alpha = self.runtime.config.lookup.clone().bounded().alpha;
        let mut targets = unique_targets.into_iter();
        let mut successful = 0usize;
        while !self.lookup_is_cancelled(cancel) && Instant::now() < deadline {
            let mut round = FuturesUnordered::new();
            for target in targets.by_ref().take(alpha) {
                let request = build_bootstrap_request();
                round.push(async move {
                    let result = self.request(target, request, cancel, deadline).await;
                    (target, result)
                });
            }
            if round.is_empty() {
                break;
            }

            let mut hello_targets = Vec::new();
            while let Some((target, response)) = round.next().await {
                let Ok(packet) = response else {
                    continue;
                };
                if packet.opcode != OP_BOOTSTRAP_RES {
                    continue;
                }
                let Ok((mut peer, discovered)) = parse_bootstrap_response(&packet.payload) else {
                    continue;
                };
                let peer_addr = SocketAddrV4::new(*target.ip(), target.port());
                peer.ip = *peer_addr.ip();
                peer.udp_port = peer_addr.port();
                if !self.is_valid_wire_contact(&peer) {
                    continue;
                }
                successful += 1;
                self.insert_wire_contact(peer).await;
                for contact in discovered {
                    self.insert_wire_contact(contact).await;
                }
                hello_targets.push(peer_addr);
                status.contacts = self.runtime.routing.lock().await.len();
                let mut health = self.runtime.health.write().await;
                health.routing_contacts = status.contacts;
                health.last_bootstrap_at_ms = Some(now_ms());
                let _ = status_tx.send(status.clone());
            }
            let mut hellos = FuturesUnordered::new();
            for target in hello_targets {
                hellos.push(self.hello_contact(target, cancel, deadline));
            }
            while hellos.next().await.is_some() {}
        }
        if self.lookup_is_cancelled(cancel) {
            return Err(KadError::Cancelled);
        }
        Ok(successful)
    }

    fn rotated_seed_targets(&self) -> Vec<SocketAddrV4> {
        let seed_count = self.runtime.seeds.len();
        if seed_count == 0 {
            return Vec::new();
        }
        let start = self
            .runtime
            .seed_cursor
            .fetch_add(MAX_BOOTSTRAP_PROBES, Ordering::Relaxed)
            % seed_count;
        (0..seed_count.min(MAX_BOOTSTRAP_PROBES))
            .map(|offset| self.runtime.seeds[(start + offset) % seed_count])
            .collect()
    }

    async fn hello_contact(
        self: &Arc<Self>,
        target: SocketAddrV4,
        cancel: &CancellationToken,
        deadline: Instant,
    ) {
        let request = build_hello_request(
            &self.runtime.node_id.0,
            self.advertised_udp_port().unwrap_or(0),
            self.runtime.config.tcp_port,
            KAD_VERSION,
        );
        let Ok(packet) = self.request(target, request, cancel, deadline).await else {
            return;
        };
        let Ok((id, tcp_port, version, tags)) = parse_hello(&packet.payload) else {
            return;
        };
        let udp_port = tags
            .iter()
            .find_map(|tag| {
                tag.id_value(TAG_SOURCE_UDP_PORT)
                    .and_then(|_| tag.get_uint())
            })
            .and_then(|value| u16::try_from(value).ok())
            .filter(|port| *port != 0)
            .unwrap_or(target.port());
        self.insert_wire_contact(KadWireContact {
            id,
            ip: *target.ip(),
            udp_port,
            tcp_port,
            version,
        })
        .await;
    }

    async fn search_sources(
        self: &Arc<Self>,
        file_hash: KadId,
        file_size: u64,
        client_hash: Option<KadId>,
        cancel: &CancellationToken,
        deadline: Instant,
        status: &mut KadLookupStatus,
        status_tx: &watch::Sender<KadLookupStatus>,
        source_tx: &mpsc::Sender<KadSource>,
    ) -> Result<(), KadError> {
        let target = NodeId(file_hash);
        let initial = self
            .runtime
            .routing
            .lock()
            .await
            .closest_with_replacements(target, routing::MAX_LOOKUP_QUERIES);
        if initial.is_empty() {
            return Err(KadError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "Kad routing table is empty",
            )));
        }

        let lookup = self.runtime.config.lookup.clone().bounded();
        let alpha = lookup.alpha;
        let source_limit = lookup.max_sources.min(MAX_SOURCE_QUERIES);
        let mut shortlist = Shortlist::new(file_hash, initial);
        let mut tracker = LookupTracker::new(&lookup);
        let mut sink = SourceSink::default();
        let mut source_queries = 0usize;
        let mut routing_inflight = 0usize;
        let mut source_inflight = 0usize;
        let mut inflight: FuturesUnordered<BoxFuture<'_, LookupReply>> = FuturesUnordered::new();
        let mut abort = None;
        let mut deadline_hit = false;

        loop {
            if abort.is_none() && !deadline_hit {
                if self.lookup_is_cancelled(cancel) {
                    abort = Some(KadError::Cancelled);
                } else if Instant::now() >= deadline {
                    deadline_hit = shortlist.has_pending_routing();
                }
            }
            if abort.is_none() && !deadline_hit && Instant::now() < deadline {
                while routing_inflight < alpha {
                    let Some(contact) = shortlist.next_unqueried() else {
                        break;
                    };
                    if !tracker.mark_queried(contact.id) {
                        shortlist.set_state(contact.id, NodeState::Unqueried);
                        break;
                    }
                    routing_inflight += 1;
                    status.queried_nodes = tracker.queried_count();
                    let _ = status_tx.send(status.clone());
                    let request = build_routing_request(2, &file_hash, &contact.id.0);
                    let addr = contact.udp_addr();
                    inflight.push(
                        async move {
                            let result = self.request(addr, request, cancel, deadline).await;
                            LookupReply::Routing(contact.id, result)
                        }
                        .boxed(),
                    );
                }
                while source_inflight < alpha && source_queries < source_limit {
                    let Some(contact) = shortlist.next_source_candidate() else {
                        break;
                    };
                    source_queries += 1;
                    source_inflight += 1;
                    let request = build_source_search_request(&file_hash, file_size, 0);
                    let addr = contact.udp_addr();
                    inflight.push(
                        async move {
                            let result = self.source_request(addr, request, cancel, deadline).await;
                            LookupReply::Source(result)
                        }
                        .boxed(),
                    );
                }
            }
            let Some(reply) = inflight.next().await else {
                break;
            };
            match reply {
                LookupReply::Routing(id, result) => {
                    routing_inflight -= 1;
                    match result {
                        Ok(packet) => {
                            shortlist.set_state(id, NodeState::Responded);
                            if packet.opcode != OP_ROUTING_RES {
                                continue;
                            }
                            let Ok(response) =
                                parse_routing_response_with_limit(&packet.payload, 2)
                            else {
                                continue;
                            };
                            for wire_contact in response.contacts {
                                if !self.is_valid_wire_contact(&wire_contact) {
                                    continue;
                                }
                                let candidate = wire_contact.to_contact();
                                self.insert_wire_contact(wire_contact).await;
                                shortlist.add(candidate);
                            }
                        }
                        Err(KadError::Cancelled) => abort = Some(KadError::Cancelled),
                        Err(_) => {
                            shortlist.set_state(id, NodeState::Failed);
                            self.runtime.routing.lock().await.mark_failed(id);
                        }
                    }
                }
                LookupReply::Source(result) => {
                    source_inflight -= 1;
                    match result {
                        Ok(packets) if abort.is_none() => {
                            if let Err(error) = self
                                .deliver_source_packets(
                                    packets,
                                    file_hash,
                                    client_hash,
                                    lookup.max_sources,
                                    &mut sink,
                                    status,
                                    status_tx,
                                    source_tx,
                                    cancel,
                                )
                                .await
                            {
                                abort = Some(error);
                            }
                        }
                        Err(KadError::Cancelled) => abort = Some(KadError::Cancelled),
                        _ => {}
                    }
                }
            }
        }

        if let Some(error) = abort {
            return Err(error);
        }
        if self.lookup_is_cancelled(cancel) {
            return Err(KadError::Cancelled);
        }
        if deadline_hit && status.discovered_sources == 0 {
            return Err(KadError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Kad lookup deadline exceeded",
            )));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn deliver_source_packets(
        &self,
        packets: Vec<KadPacket>,
        file_hash: KadId,
        client_hash: Option<KadId>,
        max_sources: usize,
        sink: &mut SourceSink,
        status: &mut KadLookupStatus,
        status_tx: &watch::Sender<KadLookupStatus>,
        source_tx: &mpsc::Sender<KadSource>,
        cancel: &CancellationToken,
    ) -> Result<(), KadError> {
        for packet in packets {
            if packet.opcode != OP_SEARCH_RES {
                continue;
            }
            let response = match parse_source_search_response(&packet.payload) {
                Ok(response) if response.target == file_hash => response,
                _ => continue,
            };
            for source in response.sources {
                if source.id == [0; 16]
                    || client_hash.is_some_and(|client_hash| source.id == client_hash)
                {
                    continue;
                }
                let Some(addr) = usable_source(&source) else {
                    continue;
                };
                if status.discovered_sources >= max_sources {
                    break;
                }
                if !sink.seen_ids.insert(source.id) {
                    continue;
                }
                if !sink.seen_addrs.insert(addr) {
                    continue;
                }
                let delivered = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(KadError::Cancelled),
                    _ = self.runtime.shutdown.cancelled() => return Err(KadError::Cancelled),
                    result = source_tx.send(KadSource {
                        client_hash: source.id,
                        addr,
                        source_type: source.source_type().unwrap_or(0),
                    }) => result,
                };
                if delivered.is_err() || self.lookup_is_cancelled(cancel) {
                    return Err(KadError::Cancelled);
                }
                status.discovered_sources += 1;
                let _ = status_tx.send(status.clone());
            }
        }
        Ok(())
    }

    async fn source_request(
        &self,
        target: SocketAddrV4,
        packet: KadPacket,
        cancel: &CancellationToken,
        deadline: Instant,
    ) -> Result<Vec<KadPacket>, KadError> {
        let expectation = RequestExpectation::from_request(&packet)?;
        if !matches!(expectation, RequestExpectation::Source(_)) {
            return Err(KadError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "source request has an unexpected opcode",
            )));
        }
        let lookup = self.runtime.config.lookup.clone().bounded();
        let encoded = packet.encode();
        let mut last_error = None;

        for _ in 0..=lookup.retries {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            if cancel.is_cancelled() || self.runtime.shutdown.is_cancelled() {
                return Err(KadError::Cancelled);
            }

            let (request_id, mut response_rx) =
                self.register_pending_stream(target, expectation).await;
            let socket = self
                .runtime
                .socket
                .lock()
                .await
                .clone()
                .ok_or(KadError::Cancelled);
            let socket = match socket {
                Ok(socket) => socket,
                Err(error) => {
                    self.remove_pending(request_id).await;
                    return Err(error);
                }
            };
            if let Err(error) = socket.send_to(&encoded, SocketAddr::V4(target)).await {
                self.remove_pending(request_id).await;
                return Err(KadError::Io(error));
            }

            let mut packets = Vec::new();
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                let wait = if packets.is_empty() {
                    lookup.request_timeout.min(remaining)
                } else {
                    SOURCE_RESPONSE_IDLE.min(remaining)
                };
                if wait.is_zero() {
                    break;
                }
                let result = tokio::select! {
                    _ = cancel.cancelled() => {
                        self.remove_pending(request_id).await;
                        return Err(KadError::Cancelled);
                    }
                    _ = self.runtime.shutdown.cancelled() => {
                        self.remove_pending(request_id).await;
                        return Err(KadError::Cancelled);
                    }
                    result = timeout(wait, response_rx.recv()) => result,
                };
                match result {
                    Ok(Some(response)) => {
                        packets.push(response);
                        if packets.len() >= MAX_SOURCE_RESPONSE_PACKETS {
                            break;
                        }
                    }
                    Ok(None) => {
                        last_error = Some(KadError::Io(std::io::Error::new(
                            std::io::ErrorKind::ConnectionAborted,
                            "Kad response dispatcher stopped",
                        )));
                        break;
                    }
                    Err(_) => break,
                }
            }
            self.remove_pending(request_id).await;
            if cancel.is_cancelled() || self.runtime.shutdown.is_cancelled() {
                return Err(KadError::Cancelled);
            }
            if !packets.is_empty() {
                return Ok(packets);
            }
            if last_error.is_none() {
                last_error = Some(KadError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "Kad source response timed out",
                )));
            }
        }

        Err(last_error.unwrap_or_else(|| {
            KadError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Kad lookup deadline exceeded",
            ))
        }))
    }

    async fn request(
        &self,
        target: SocketAddrV4,
        packet: KadPacket,
        cancel: &CancellationToken,
        deadline: Instant,
    ) -> Result<KadPacket, KadError> {
        let expectation = RequestExpectation::from_request(&packet)?;
        let lookup = self.runtime.config.lookup.clone().bounded();
        let encoded = packet.encode();
        let mut last_timeout = None;
        for _ in 0..=lookup.retries {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            if cancel.is_cancelled() || self.runtime.shutdown.is_cancelled() {
                return Err(KadError::Cancelled);
            }
            let (request_id, mut response_rx) = self.register_pending(target, expectation).await;
            if cancel.is_cancelled() || self.runtime.shutdown.is_cancelled() {
                self.remove_pending(request_id).await;
                return Err(KadError::Cancelled);
            }
            let socket = self
                .runtime
                .socket
                .lock()
                .await
                .clone()
                .ok_or(KadError::Cancelled);
            let socket = match socket {
                Ok(socket) => socket,
                Err(error) => {
                    self.remove_pending(request_id).await;
                    return Err(error);
                }
            };
            if let Err(error) = socket.send_to(&encoded, SocketAddr::V4(target)).await {
                self.remove_pending(request_id).await;
                return Err(KadError::Io(error));
            }

            let attempt_deadline = Instant::now() + lookup.request_timeout.min(remaining);
            let wait = attempt_deadline
                .saturating_duration_since(Instant::now())
                .min(deadline.saturating_duration_since(Instant::now()));
            let result = tokio::select! {
                _ = cancel.cancelled() => {
                    self.remove_pending(request_id).await;
                    return Err(KadError::Cancelled);
                }
                _ = self.runtime.shutdown.cancelled() => {
                    self.remove_pending(request_id).await;
                    return Err(KadError::Cancelled);
                }
                result = timeout(wait, &mut response_rx) => result,
            };
            match result {
                Ok(Ok(response)) => return Ok(response),
                Ok(Err(_)) if cancel.is_cancelled() || self.runtime.shutdown.is_cancelled() => {
                    self.remove_pending(request_id).await;
                    return Err(KadError::Cancelled);
                }
                Ok(Err(_)) => {
                    self.remove_pending(request_id).await;
                    last_timeout = Some(KadError::Io(std::io::Error::new(
                        std::io::ErrorKind::ConnectionAborted,
                        "Kad response dispatcher stopped",
                    )));
                }
                Err(_) => {
                    self.remove_pending(request_id).await;
                    last_timeout = Some(KadError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "Kad request timed out",
                    )));
                }
            }
        }
        Err(last_timeout.unwrap_or_else(|| {
            KadError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Kad lookup deadline exceeded",
            ))
        }))
    }

    async fn register_pending(
        &self,
        target: SocketAddrV4,
        expectation: RequestExpectation,
    ) -> (u64, oneshot::Receiver<KadPacket>) {
        let id = self.runtime.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (response, receiver) = oneshot::channel();
        self.runtime.pending.lock().await.push(PendingRequest {
            id,
            target: SocketAddr::V4(target),
            expectation,
            response: PendingResponse::Single(response),
        });
        (id, receiver)
    }

    async fn register_pending_stream(
        &self,
        target: SocketAddrV4,
        expectation: RequestExpectation,
    ) -> (u64, mpsc::Receiver<KadPacket>) {
        let id = self.runtime.next_request_id.fetch_add(1, Ordering::Relaxed);
        let (response, receiver) = mpsc::channel(MAX_SOURCE_RESPONSE_PACKETS);
        self.runtime.pending.lock().await.push(PendingRequest {
            id,
            target: SocketAddr::V4(target),
            expectation,
            response: PendingResponse::Stream(response),
        });
        (id, receiver)
    }

    async fn remove_pending(&self, request_id: u64) {
        self.runtime
            .pending
            .lock()
            .await
            .retain(|entry| entry.id != request_id);
    }

    fn is_valid_wire_contact(&self, contact: &KadWireContact) -> bool {
        #[cfg(test)]
        if self.runtime.config.allow_private_contacts {
            return contact
                .to_contact()
                .is_valid_for_routing_allow_private(self.runtime.node_id);
        }
        is_valid_kad_contact(contact, self.runtime.node_id)
    }

    async fn insert_wire_contact(self: &Arc<Self>, contact: KadWireContact) -> bool {
        if !self.is_valid_wire_contact(&contact) {
            return false;
        }
        let candidate = contact.to_contact();
        let mut table = self.runtime.routing.lock().await;
        let probe = table.liveness_probe_target(&candidate);
        let inserted = table.insert(candidate);
        drop(table);
        if !matches!(
            inserted,
            routing::InsertResult::Rejected | routing::InsertResult::SelfContact
        ) {
            let _ = self.runtime.checkpoint_tx.try_send(());
        }
        if matches!(inserted, routing::InsertResult::Rejected) {
            if let Some(contact) = probe {
                self.schedule_liveness_probe(contact).await;
            }
        }
        true
    }

    async fn schedule_liveness_probe(self: &Arc<Self>, contact: routing::Contact) {
        {
            let mut probes = self.runtime.liveness.lock().await;
            if probes.len() >= MAX_LIVENESS_PROBES || !probes.insert(contact.id) {
                return;
            }
        }

        let Some(task_guard) = self.runtime.active_tasks.try_start() else {
            self.runtime.liveness.lock().await.remove(&contact.id);
            return;
        };
        let service = self.clone();
        tokio::spawn(async move {
            let _task_guard = task_guard;
            let lookup = service.runtime.config.lookup.clone().bounded();
            let probe_window = lookup
                .request_timeout
                .checked_mul((lookup.retries + 1) as u32)
                .unwrap_or(lookup.request_timeout)
                .saturating_add(Duration::from_millis(50));
            let cancel = service.runtime.shutdown.child_token();
            let result = service
                .request(
                    contact.udp_addr(),
                    self::wire::build_ping(),
                    &cancel,
                    Instant::now() + probe_window,
                )
                .await;

            if !matches!(result, Err(KadError::Cancelled)) {
                let mut table = service.runtime.routing.lock().await;
                let changed = match result {
                    Ok(_) => table.mark_alive(contact.id),
                    Err(_) => table.remove_if_unchanged(&contact).is_some(),
                };
                drop(table);
                if changed {
                    let _ = service.runtime.checkpoint_tx.try_send(());
                }
            }
            service.runtime.liveness.lock().await.remove(&contact.id);
        });
    }

    async fn set_health_state(&self, state: KadState, error: Option<String>) {
        // Lock order routing -> health everywhere; reversing can deadlock against bootstrap
        let routing_contacts = self.runtime.routing.lock().await.len();
        let mut health = self.runtime.health.write().await;
        health.state = state;
        health.last_error = error;
        health.routing_contacts = routing_contacts;
    }

    async fn begin_lookup_health(&self) {
        let routing_contacts = self.runtime.routing.lock().await.len();
        let mut health = self.runtime.health.write().await;
        health.routing_contacts = routing_contacts;
        if routing_contacts == 0 || health.state != KadState::Ready {
            health.state = KadState::Bootstrapping;
            health.last_error = None;
        }
    }

    async fn update_lookup_health(&self, success: bool, error: Option<String>) {
        let mut health = self.runtime.health.write().await;
        health.last_lookup_at_ms = Some(now_ms());
        health.last_lookup_success = Some(success);
        health.last_error = error;
    }
}

async fn run_dispatcher(
    socket: Arc<KadSocket>,
    pending: Arc<Mutex<Vec<PendingRequest>>>,
    shutdown: CancellationToken,
) {
    let mut buffer = vec![0u8; wire::MAX_DATAGRAM_SIZE];
    let mut consecutive_errors = 0u32;
    loop {
        let received = tokio::select! {
            _ = shutdown.cancelled() => break,
            result = socket.recv_from(&mut buffer) => result,
        };
        let (length, source) = match received {
            Ok(received) => {
                consecutive_errors = 0;
                received
            }
            Err(error) => {
                // Transient errors (ICMP port-unreachable surfacing as ECONNRESET, ENOBUFS) must not end Kad
                consecutive_errors += 1;
                tracing::debug!("[ed2k-kad] recv_from failed: {}", error);
                if consecutive_errors > 8 {
                    tokio::select! {
                        _ = shutdown.cancelled() => break,
                        _ = sleep(Duration::from_millis(100)) => {}
                    }
                }
                continue;
            }
        };

        let candidates = {
            let entries = pending.lock().await;
            entries
                .iter()
                .filter(|entry| kad_source_matches(entry.target, &source))
                .map(|entry| entry.id)
                .collect::<HashSet<_>>()
        };
        if candidates.is_empty() {
            continue;
        }
        let Ok(packet) = KadPacket::decode(&buffer[..length]) else {
            continue;
        };
        let matching_ids = {
            let entries = pending.lock().await;
            entries
                .iter()
                .filter(|entry| {
                    candidates.contains(&entry.id) && entry.expectation.matches(&packet)
                })
                .map(|entry| entry.id)
                .collect::<HashSet<_>>()
        };

        let (oneshot_senders, stream_senders) = {
            let mut entries = pending.lock().await;
            let mut oneshot_senders = Vec::new();
            let mut stream_senders = Vec::new();
            let mut index = 0;
            while index < entries.len() {
                if matching_ids.contains(&entries[index].id) {
                    match &entries[index].response {
                        PendingResponse::Single(_) => {
                            let entry = entries.swap_remove(index);
                            if let PendingResponse::Single(sender) = entry.response {
                                oneshot_senders.push(sender);
                            }
                        }
                        PendingResponse::Stream(sender) if sender.is_closed() => {
                            entries.swap_remove(index);
                        }
                        PendingResponse::Stream(sender) => {
                            stream_senders.push(sender.clone());
                            index += 1;
                        }
                    }
                } else {
                    index += 1;
                }
            }
            if oneshot_senders.is_empty() && stream_senders.is_empty() {
                continue;
            }
            (oneshot_senders, stream_senders)
        };
        for sender in oneshot_senders {
            let _ = sender.send(packet.clone());
        }
        for sender in stream_senders {
            let _ = sender.try_send(packet.clone());
        }
    }

    pending.lock().await.clear();
}

async fn run_checkpoint_worker(runtime: Arc<KadRuntime>, mut signals: mpsc::Receiver<()>) {
    loop {
        let signal = tokio::select! {
            _ = runtime.shutdown.cancelled() => return,
            signal = signals.recv() => signal,
        };
        if signal.is_none() {
            return;
        }
        loop {
            tokio::select! {
                _ = runtime.shutdown.cancelled() => return,
                _ = sleep(STATE_CHECKPOINT_DEBOUNCE) => break,
                signal = signals.recv() => {
                    if signal.is_none() {
                        return;
                    }
                }
            }
        }
        if runtime.shutdown.is_cancelled() {
            return;
        }
        persist_runtime_state(&runtime).await;
    }
}

async fn persist_runtime_state(runtime: &KadRuntime) {
    let config_dir = runtime.config_dir.clone();
    let node_id = runtime.node_id;
    let contacts = runtime.routing.lock().await.contacts();
    let result =
        tokio::task::spawn_blocking(move || state::save(&config_dir, node_id, &contacts)).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!("[ed2k-kad] failed to persist state: {}", error);
        }
        Err(error) => {
            tracing::warn!("[ed2k-kad] state checkpoint task failed: {}", error);
        }
    }
}

fn bundled_seeds() -> Vec<SocketAddrV4> {
    #[derive(Deserialize)]
    struct SeedFile {
        version: u32,
        seeds: Vec<Seed>,
    }
    #[derive(Deserialize)]
    struct Seed {
        ip: Ipv4Addr,
        udp_port: u16,
    }
    let parsed: SeedFile = serde_json::from_str(include_str!("seeds.json")).unwrap_or(SeedFile {
        version: 1,
        seeds: Vec::new(),
    });
    if parsed.version != 1 {
        return Vec::new();
    }
    parsed
        .seeds
        .into_iter()
        .filter(|seed| seed.udp_port != 0 && is_public_ipv4(seed.ip))
        .take(MAX_BOOTSTRAP_SEEDS)
        .map(|seed| SocketAddrV4::new(seed.ip, seed.udp_port))
        .collect()
}

fn usable_source(source: &KadSourceRecord) -> Option<SocketAddrV4> {
    if source.id == [0; 16] {
        return None;
    }
    let source_type = source.source_type()?;
    if !matches!(source_type, 1 | 4) {
        return None;
    }
    let addr = source.direct_addr()?;
    (addr.port() != 0 && is_public_ipv4(*addr.ip())).then_some(addr)
}

fn is_valid_kad_contact(contact: &KadWireContact, local_id: NodeId) -> bool {
    contact.to_contact().is_valid_for_routing(local_id)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::routing::Contact;
    use super::*;
    use std::net::UdpSocket as StdUdpSocket;
    use std::sync::atomic::AtomicUsize;

    use tokio::sync::Barrier;
    use tokio::time::sleep;

    #[derive(Default)]
    struct LoopbackEvents {
        bootstrap: AtomicUsize,
        hello: AtomicUsize,
        routing: AtomicUsize,
        source: AtomicUsize,
    }

    fn free_loopback_port() -> u16 {
        let socket = StdUdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)).unwrap();
        socket.local_addr().unwrap().port()
    }

    #[test]
    fn domain_form_proxy_source_never_triggers_local_resolution() {
        let target: SocketAddr = "127.0.0.1:4662".parse().unwrap();
        assert!(!kad_source_matches(
            target,
            &ProxyDatagramSource::Host("localhost".to_string(), 4662)
        ));
        assert!(kad_source_matches(
            target,
            &ProxyDatagramSource::Host("127.0.0.1".to_string(), 4662)
        ));
    }

    async fn test_service(lookup: LookupConfig) -> (Arc<KadService>, tempfile::TempDir, u16) {
        let directory = tempfile::tempdir().unwrap();
        let port = free_loopback_port();
        let mut config = KadConfig::new(directory.path(), port, 4662)
            .with_bootstrap_seeds_for_test(Vec::new())
            .with_loopback_contacts_for_test();
        config.bind_addr = Ipv4Addr::LOCALHOST;
        config.lookup = lookup;
        let service = KadService::bind(config).await.unwrap();
        (service, directory, port)
    }

    async fn add_loopback_contact(service: &KadService, id: KadId, addr: SocketAddrV4) {
        let result = service
            .runtime
            .routing
            .lock()
            .await
            .insert_for_test(Contact::new(id, addr, 4662, KAD_VERSION));
        assert!(matches!(
            result,
            routing::InsertResult::Inserted | routing::InsertResult::Updated
        ));
    }

    fn bootstrap_response(node_id: KadId) -> KadPacket {
        bootstrap_response_with(node_id, 4662, KAD_VERSION)
    }

    fn bootstrap_response_with(node_id: KadId, tcp_port: u16, version: u8) -> KadPacket {
        let mut payload = Vec::with_capacity(21);
        payload.extend_from_slice(&node_id);
        payload.extend_from_slice(&tcp_port.to_le_bytes());
        payload.push(version);
        payload.extend_from_slice(&0u16.to_le_bytes());
        KadPacket::new(OP_BOOTSTRAP_RES, payload)
    }

    fn direct_source(id: KadId, addr: SocketAddrV4) -> KadSourceRecord {
        KadSourceRecord {
            id,
            tags: vec![
                wire::KadTag::uint(wire::TAG_SOURCE_TYPE, 1),
                wire::KadTag::uint(
                    wire::TAG_SOURCE_IP,
                    u32::from_be_bytes(addr.ip().octets()) as u64,
                ),
                wire::KadTag::uint(wire::TAG_SOURCE_PORT, addr.port() as u64),
            ],
        }
    }

    async fn run_loopback_node(
        socket: UdpSocket,
        node_id: KadId,
        sources: Vec<SocketAddrV4>,
        events: Arc<LoopbackEvents>,
        routing_observed: Option<mpsc::Sender<()>>,
        routing_barrier: Option<Arc<Barrier>>,
        shutdown: CancellationToken,
    ) {
        let mut buffer = vec![0u8; wire::MAX_DATAGRAM_SIZE];
        loop {
            let received = tokio::select! {
                _ = shutdown.cancelled() => break,
                result = socket.recv_from(&mut buffer) => result,
            };
            let Ok((length, remote)) = received else {
                break;
            };
            let Ok(packet) = KadPacket::decode(&buffer[..length]) else {
                continue;
            };
            let responses = match packet.opcode {
                OP_BOOTSTRAP_REQ => {
                    events.bootstrap.fetch_add(1, Ordering::Relaxed);
                    vec![bootstrap_response(node_id)]
                }
                OP_HELLO_REQ => {
                    events.hello.fetch_add(1, Ordering::Relaxed);
                    vec![wire::build_hello_response(
                        &node_id,
                        socket.local_addr().unwrap().port(),
                        4662,
                        KAD_VERSION,
                    )]
                }
                OP_ROUTING_REQ => {
                    events.routing.fetch_add(1, Ordering::Relaxed);
                    if let Some(observed) = &routing_observed {
                        let _ = observed.send(()).await;
                    }
                    if let Some(barrier) = &routing_barrier {
                        barrier.wait().await;
                    }
                    let Ok((_, target, requested_node_id)) =
                        wire::parse_routing_request(&packet.payload)
                    else {
                        continue;
                    };
                    if requested_node_id != node_id {
                        continue;
                    }
                    vec![wire::build_routing_response(&target, &[])]
                }
                OP_SEARCH_SOURCE_REQ => {
                    events.source.fetch_add(1, Ordering::Relaxed);
                    let Ok((target, _, _)) = wire::parse_source_search_request(&packet.payload)
                    else {
                        continue;
                    };
                    sources
                        .iter()
                        .enumerate()
                        .map(|(index, source)| {
                            wire::build_source_search_response(
                                &target,
                                &[direct_source(
                                    [node_id[0].wrapping_add(1 + index as u8); 16],
                                    *source,
                                )],
                            )
                        })
                        .collect()
                }
                OP_PING => vec![KadPacket::new(OP_PONG, 4672u16.to_le_bytes().to_vec())],
                _ => continue,
            };
            for response in responses {
                let _ = socket.send_to(&response.encode(), remote).await;
            }
        }
    }

    async fn run_bootstrap_only_node(
        socket: UdpSocket,
        node_id: KadId,
        tcp_port: u16,
        version: u8,
        requests: Arc<AtomicUsize>,
        shutdown: CancellationToken,
    ) {
        let mut buffer = vec![0u8; wire::MAX_DATAGRAM_SIZE];
        loop {
            let received = tokio::select! {
                _ = shutdown.cancelled() => break,
                result = socket.recv_from(&mut buffer) => result,
            };
            let Ok((length, remote)) = received else {
                break;
            };
            let Ok(packet) = KadPacket::decode(&buffer[..length]) else {
                continue;
            };
            if packet.opcode != OP_BOOTSTRAP_REQ {
                continue;
            }
            requests.fetch_add(1, Ordering::Relaxed);
            let response = bootstrap_response_with(node_id, tcp_port, version);
            let _ = socket.send_to(&response.encode(), remote).await;
        }
    }

    struct ScriptedNode {
        id: KadId,
        referrals: Vec<KadWireContact>,
        silent_routing: bool,
        source: Option<SocketAddrV4>,
        compress_sources: bool,
        routing_gate: Option<Arc<Barrier>>,
        routing_delay: Duration,
        events: Arc<LoopbackEvents>,
    }

    impl ScriptedNode {
        fn new(id: KadId) -> Self {
            Self {
                id,
                referrals: Vec::new(),
                silent_routing: false,
                source: None,
                compress_sources: false,
                routing_gate: None,
                routing_delay: Duration::ZERO,
                events: Arc::new(LoopbackEvents::default()),
            }
        }
    }

    fn compressed(packet: &KadPacket) -> Vec<u8> {
        use std::io::Write;
        let mut encoder = flate2::write::ZlibEncoder::new(
            vec![wire::KAD_PROTOCOL_COMPRESSED, packet.opcode],
            flate2::Compression::fast(),
        );
        encoder.write_all(&packet.payload).unwrap();
        encoder.finish().unwrap()
    }

    fn loopback_wire_contact(id: KadId, addr: SocketAddrV4) -> KadWireContact {
        KadWireContact {
            id,
            ip: *addr.ip(),
            udp_port: addr.port(),
            tcp_port: 4662,
            version: KAD_VERSION,
        }
    }

    async fn spawn_scripted_node(
        node: ScriptedNode,
        shutdown: CancellationToken,
    ) -> (SocketAddrV4, JoinHandle<()>) {
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let addr = match socket.local_addr().unwrap() {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => unreachable!(),
        };
        let handle = tokio::spawn(async move {
            let mut buffer = vec![0u8; wire::MAX_DATAGRAM_SIZE];
            loop {
                let received = tokio::select! {
                    _ = shutdown.cancelled() => break,
                    result = socket.recv_from(&mut buffer) => result,
                };
                let Ok((length, remote)) = received else {
                    break;
                };
                let Ok(packet) = KadPacket::decode(&buffer[..length]) else {
                    continue;
                };
                let events = &node.events;
                let datagrams = match packet.opcode {
                    OP_BOOTSTRAP_REQ => {
                        events.bootstrap.fetch_add(1, Ordering::Relaxed);
                        vec![bootstrap_response(node.id).encode()]
                    }
                    OP_HELLO_REQ => {
                        events.hello.fetch_add(1, Ordering::Relaxed);
                        vec![
                            wire::build_hello_response(&node.id, addr.port(), 4662, KAD_VERSION)
                                .encode(),
                        ]
                    }
                    OP_ROUTING_REQ => {
                        events.routing.fetch_add(1, Ordering::Relaxed);
                        if node.silent_routing {
                            continue;
                        }
                        sleep(node.routing_delay).await;
                        if let Some(gate) = &node.routing_gate {
                            gate.wait().await;
                        }
                        let Ok((_, target, _)) = wire::parse_routing_request(&packet.payload)
                        else {
                            continue;
                        };
                        vec![wire::build_routing_response(&target, &node.referrals).encode()]
                    }
                    OP_SEARCH_SOURCE_REQ => {
                        events.source.fetch_add(1, Ordering::Relaxed);
                        let (Some(source), Ok((target, _, _))) = (
                            node.source,
                            wire::parse_source_search_request(&packet.payload),
                        ) else {
                            continue;
                        };
                        let response = wire::build_source_search_response(
                            &target,
                            &[direct_source([node.id[0].wrapping_add(1); 16], source)],
                        );
                        if node.compress_sources {
                            vec![compressed(&response)]
                        } else {
                            vec![response.encode()]
                        }
                    }
                    OP_PING => {
                        vec![KadPacket::new(OP_PONG, 4672u16.to_le_bytes().to_vec()).encode()]
                    }
                    _ => continue,
                };
                for datagram in datagrams {
                    let _ = socket.send_to(&datagram, remote).await;
                }
            }
        });
        (addr, handle)
    }

    fn fast_lookup() -> LookupConfig {
        LookupConfig {
            request_timeout: Duration::from_millis(300),
            deadline: Duration::from_secs(10),
            retries: 0,
            ..LookupConfig::default()
        }
    }

    #[tokio::test]
    async fn lookup_converges_after_the_closest_nodes_have_responded() {
        let stop = CancellationToken::new();
        let hash = [0x44; 16];
        let mut seed = ScriptedNode::new([0x80; 16]);
        let mut near_a = ScriptedNode::new([0x45; 16]);
        let mut near_b = ScriptedNode::new([0x46; 16]);
        for node in [&mut seed, &mut near_a, &mut near_b] {
            node.source = Some(SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 4662));
        }
        let (a_events, b_events, seed_events) = (
            near_a.events.clone(),
            near_b.events.clone(),
            seed.events.clone(),
        );
        let (a_addr, a_task) = spawn_scripted_node(near_a, stop.clone()).await;
        let (b_addr, b_task) = spawn_scripted_node(near_b, stop.clone()).await;
        seed.referrals = vec![
            loopback_wire_contact([0x45; 16], a_addr),
            loopback_wire_contact([0x46; 16], b_addr),
        ];
        let (seed_addr, seed_task) = spawn_scripted_node(seed, stop.clone()).await;
        let (service, _directory, _) = test_service(fast_lookup()).await;
        add_loopback_contact(&service, [0x80; 16], seed_addr).await;

        let lookup = service.lookup_sources(hash, 1, CancellationToken::new());
        let (_sources, status, completion) = lookup.into_parts();
        timeout(Duration::from_secs(5), completion)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        for events in [&seed_events, &a_events, &b_events] {
            assert_eq!(events.routing.load(Ordering::Relaxed), 1);
            assert_eq!(events.source.load(Ordering::Relaxed), 1);
        }
        assert_eq!(status.borrow().queried_nodes, 3);
        assert_eq!(status.borrow().state, KadState::Ready);

        service.shutdown().await;
        stop.cancel();
        for task in [a_task, b_task, seed_task] {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn timed_out_node_is_not_queried_again_within_one_lookup() {
        let stop = CancellationToken::new();
        let mut silent = ScriptedNode::new([0x45; 16]);
        silent.silent_routing = true;
        let silent_events = silent.events.clone();
        let (silent_addr, silent_task) = spawn_scripted_node(silent, stop.clone()).await;
        let referral = loopback_wire_contact([0x45; 16], silent_addr);
        let mut first = ScriptedNode::new([0x80; 16]);
        first.referrals = vec![referral.clone()];
        let mut second = ScriptedNode::new([0x81; 16]);
        second.referrals = vec![referral];
        second.routing_delay = Duration::from_millis(700);
        let second_events = second.events.clone();
        let (first_addr, first_task) = spawn_scripted_node(first, stop.clone()).await;
        let (second_addr, second_task) = spawn_scripted_node(second, stop.clone()).await;
        let (service, _directory, _) = test_service(fast_lookup()).await;
        add_loopback_contact(&service, [0x80; 16], first_addr).await;
        add_loopback_contact(&service, [0x81; 16], second_addr).await;

        let lookup = service.lookup_sources([0x44; 16], 1, CancellationToken::new());
        let (_sources, _status, completion) = lookup.into_parts();
        timeout(Duration::from_secs(5), completion)
            .await
            .unwrap()
            .unwrap()
            .unwrap();

        assert_eq!(second_events.routing.load(Ordering::Relaxed), 1);
        assert_eq!(silent_events.routing.load(Ordering::Relaxed), 1);
        assert_eq!(silent_events.source.load(Ordering::Relaxed), 0);

        service.shutdown().await;
        stop.cancel();
        for task in [silent_task, first_task, second_task] {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn sources_stream_while_a_slow_node_is_still_pending() {
        let stop = CancellationToken::new();
        let gate = Arc::new(Barrier::new(2));
        let mut slow = ScriptedNode::new([0x45; 16]);
        slow.routing_gate = Some(gate.clone());
        let mut fast = ScriptedNode::new([0x46; 16]);
        fast.source = Some(SocketAddrV4::new(Ipv4Addr::new(8, 8, 4, 4), 4662));
        let (slow_addr, slow_task) = spawn_scripted_node(slow, stop.clone()).await;
        let (fast_addr, fast_task) = spawn_scripted_node(fast, stop.clone()).await;
        let (service, _directory, _) = test_service(LookupConfig {
            request_timeout: Duration::from_secs(5),
            ..fast_lookup()
        })
        .await;
        add_loopback_contact(&service, [0x45; 16], slow_addr).await;
        add_loopback_contact(&service, [0x46; 16], fast_addr).await;

        let lookup = service.lookup_sources([0x44; 16], 1, CancellationToken::new());
        let (mut sources, _status, completion) = lookup.into_parts();
        let source = timeout(Duration::from_secs(2), sources.recv())
            .await
            .expect("source should arrive before the slow node answers")
            .unwrap();
        assert_eq!(
            source.addr,
            SocketAddrV4::new(Ipv4Addr::new(8, 8, 4, 4), 4662)
        );
        assert!(!completion.is_finished());
        gate.wait().await;
        completion.await.unwrap().unwrap();

        service.shutdown().await;
        stop.cancel();
        slow_task.await.unwrap();
        fast_task.await.unwrap();
    }

    #[tokio::test]
    async fn compressed_source_response_is_delivered() {
        let stop = CancellationToken::new();
        let mut node = ScriptedNode::new([0x80; 16]);
        node.source = Some(SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 4662));
        node.compress_sources = true;
        let (addr, task) = spawn_scripted_node(node, stop.clone()).await;
        let (service, _directory, _) = test_service(fast_lookup()).await;
        add_loopback_contact(&service, [0x80; 16], addr).await;

        let lookup = service.lookup_sources([0x44; 16], 1, CancellationToken::new());
        let (mut sources, _status, completion) = lookup.into_parts();
        completion.await.unwrap().unwrap();
        let source = timeout(Duration::from_secs(1), sources.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            source.addr,
            SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 4662)
        );

        service.shutdown().await;
        stop.cancel();
        task.await.unwrap();
    }

    #[test]
    fn bundled_seed_asset_is_bounded() {
        assert!(bundled_seeds().len() <= MAX_BOOTSTRAP_SEEDS);
    }

    #[tokio::test]
    async fn bind_fails_when_the_initial_identity_cannot_be_persisted() {
        let directory = tempfile::tempdir().unwrap();
        let blocking_file = directory.path().join("not-a-directory");
        std::fs::write(&blocking_file, b"x").unwrap();
        let mut config = KadConfig::new(blocking_file.join("kad"), free_loopback_port(), 4662)
            .with_bootstrap_seeds_for_test(Vec::new());
        config.bind_addr = Ipv4Addr::LOCALHOST;

        assert!(matches!(
            KadService::bind(config).await,
            Err(KadError::State(_))
        ));
    }

    #[test]
    fn bootstrap_responder_validation_requires_a_usable_kad_contact() {
        let local_id = NodeId([0x11; 16]);
        let valid = KadWireContact {
            id: [0x22; 16],
            ip: Ipv4Addr::new(8, 8, 8, 8),
            udp_port: 4672,
            tcp_port: 4662,
            version: KAD_VERSION,
        };

        assert!(is_valid_kad_contact(&valid, local_id));

        let invalid_version = KadWireContact {
            version: 1,
            ..valid.clone()
        };
        assert!(!is_valid_kad_contact(&invalid_version, local_id));

        let udp_only = KadWireContact {
            tcp_port: 0,
            ..valid.clone()
        };
        assert!(is_valid_kad_contact(&udp_only, local_id));

        let missing_udp_port = KadWireContact {
            udp_port: 0,
            ..valid.clone()
        };
        assert!(!is_valid_kad_contact(&missing_udp_port, local_id));

        let private_endpoint = KadWireContact {
            ip: Ipv4Addr::new(10, 0, 0, 1),
            ..valid.clone()
        };
        assert!(!is_valid_kad_contact(&private_endpoint, local_id));

        let self_endpoint = KadWireContact {
            id: local_id.0,
            ..valid
        };
        assert!(!is_valid_kad_contact(&self_endpoint, local_id));
    }

    #[test]
    fn source_filter_rejects_callback_and_private_records() {
        let source = KadSourceRecord {
            id: [1; 16],
            tags: vec![
                wire::KadTag::uint(wire::TAG_SOURCE_TYPE, 3),
                wire::KadTag::uint(wire::TAG_SOURCE_IP, u32::from_be_bytes([8, 8, 8, 8]) as u64),
                wire::KadTag::uint(wire::TAG_SOURCE_PORT, 4662),
            ],
        };
        assert!(usable_source(&source).is_none());

        let mut zero_id = source.clone();
        zero_id.tags[0] = wire::KadTag::uint(wire::TAG_SOURCE_TYPE, 1);
        zero_id.tags[1] =
            wire::KadTag::uint(wire::TAG_SOURCE_IP, u32::from_le_bytes([8, 8, 8, 8]) as u64);
        zero_id.tags[2] = wire::KadTag::uint(wire::TAG_SOURCE_PORT, 4662);
        zero_id.id = [0; 16];
        assert!(usable_source(&zero_id).is_none());
    }

    #[test]
    fn source_filter_rejects_malformed_required_tags() {
        let bool_port = KadSourceRecord {
            id: [1; 16],
            tags: vec![
                wire::KadTag::uint(wire::TAG_SOURCE_TYPE, 1),
                wire::KadTag::uint(wire::TAG_SOURCE_IP, u32::from_be_bytes([8, 8, 8, 8]) as u64),
                wire::KadTag::id(wire::TAG_SOURCE_PORT, wire::KadTagValue::Bool(true)),
            ],
        };
        assert!(usable_source(&bool_port).is_none());

        let duplicate_type = KadSourceRecord {
            id: [2; 16],
            tags: vec![
                wire::KadTag::uint(wire::TAG_SOURCE_TYPE, 1),
                wire::KadTag::uint(wire::TAG_SOURCE_TYPE, 1),
                wire::KadTag::uint(wire::TAG_SOURCE_IP, u32::from_be_bytes([8, 8, 4, 4]) as u64),
                wire::KadTag::uint(wire::TAG_SOURCE_PORT, 4662),
            ],
        };
        assert!(usable_source(&duplicate_type).is_none());
    }

    #[test]
    fn find_value_expectation_rejects_an_oversized_routing_reply() {
        let target = [0x22; 16];
        let request = build_routing_request(2, &target, &[0x80; 16]);
        let expectation = RequestExpectation::from_request(&request).unwrap();
        let contacts = (1..=3)
            .map(|index| KadWireContact {
                id: [index; 16],
                ip: Ipv4Addr::new(8, 8, 8, index),
                udp_port: 4672,
                tcp_port: 4662,
                version: KAD_VERSION,
            })
            .collect::<Vec<_>>();
        let oversized = wire::build_routing_response(&target, &contacts);
        let bounded = wire::build_routing_response(&target, &contacts[..2]);

        assert!(!expectation.matches(&oversized));
        assert!(expectation.matches(&bounded));
    }

    #[tokio::test]
    async fn source_lookup_skips_contacts_before_kad_version_three() {
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let node_addr = match socket.local_addr().unwrap() {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => unreachable!(),
        };
        let events = Arc::new(LoopbackEvents::default());
        let node_cancel = CancellationToken::new();
        let node = tokio::spawn(run_loopback_node(
            socket,
            [0x80; 16],
            vec![SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 4662)],
            events.clone(),
            None,
            None,
            node_cancel.clone(),
        ));
        let (service, _directory, _) = test_service(LookupConfig {
            request_timeout: Duration::from_millis(250),
            deadline: Duration::from_secs(2),
            retries: 0,
            ..LookupConfig::default()
        })
        .await;
        let inserted = service
            .runtime
            .routing
            .lock()
            .await
            .insert_for_test(Contact::new([0x80; 16], node_addr, 4662, 2));
        assert!(matches!(
            inserted,
            routing::InsertResult::Inserted | routing::InsertResult::Updated
        ));

        let cancel = CancellationToken::new();
        let (source_tx, _source_rx) = mpsc::channel(1);
        let (status_tx, _status_rx) = watch::channel(KadLookupStatus::default());
        let mut status = KadLookupStatus::default();
        service
            .search_sources(
                [0x44; 16],
                1234,
                None,
                &cancel,
                Instant::now() + Duration::from_secs(1),
                &mut status,
                &status_tx,
                &source_tx,
            )
            .await
            .unwrap();

        assert_eq!(events.routing.load(Ordering::Relaxed), 1);
        assert_eq!(events.source.load(Ordering::Relaxed), 0);

        service.shutdown().await;
        node_cancel.cancel();
        node.await.unwrap();
    }

    #[tokio::test]
    async fn bootstrap_tries_bundled_seeds_after_an_unusable_cached_response() {
        let bad_socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let bad_addr = match bad_socket.local_addr().unwrap() {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => unreachable!(),
        };
        let bad_requests = Arc::new(AtomicUsize::new(0));
        let bad_shutdown = CancellationToken::new();
        let bad_node = tokio::spawn(run_bootstrap_only_node(
            bad_socket,
            [0x70; 16],
            4662,
            1,
            bad_requests.clone(),
            bad_shutdown.clone(),
        ));

        let seed_socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let seed_addr = match seed_socket.local_addr().unwrap() {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => unreachable!(),
        };
        let seed_events = Arc::new(LoopbackEvents::default());
        let seed_shutdown = CancellationToken::new();
        let seed_node = tokio::spawn(run_loopback_node(
            seed_socket,
            [0x80; 16],
            Vec::new(),
            seed_events.clone(),
            None,
            None,
            seed_shutdown.clone(),
        ));

        let directory = tempfile::tempdir().unwrap();
        let mut config = KadConfig::new(directory.path(), free_loopback_port(), 4662)
            .with_bootstrap_seeds_for_test(vec![seed_addr])
            .with_loopback_contacts_for_test();
        config.bind_addr = Ipv4Addr::LOCALHOST;
        config.lookup = LookupConfig {
            alpha: 1,
            request_timeout: Duration::from_millis(250),
            deadline: Duration::from_secs(2),
            retries: 0,
            ..LookupConfig::default()
        };
        let service = KadService::bind(config).await.unwrap();
        add_loopback_contact(&service, [0x70; 16], bad_addr).await;

        let cancel = CancellationToken::new();
        let (status_tx, _status_rx) = watch::channel(KadLookupStatus::default());
        let mut status = KadLookupStatus::default();
        service
            .bootstrap_if_needed(
                &cancel,
                Instant::now() + Duration::from_secs(2),
                &mut status,
                &status_tx,
            )
            .await
            .expect("a valid seed should recover bootstrap");

        assert_eq!(bad_requests.load(Ordering::Relaxed), 1);
        assert_eq!(seed_events.bootstrap.load(Ordering::Relaxed), 1);

        service.shutdown().await;
        bad_shutdown.cancel();
        seed_shutdown.cancel();
        bad_node.await.unwrap();
        seed_node.await.unwrap();
    }

    #[tokio::test]
    async fn loopback_lookup_bootstraps_hello_routes_and_delivers_sources() {
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let node_addr = match socket.local_addr().unwrap() {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => unreachable!(),
        };
        let events = Arc::new(LoopbackEvents::default());
        let node_cancel = CancellationToken::new();
        let node = tokio::spawn(run_loopback_node(
            socket,
            [0x80; 16],
            vec![SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 4662)],
            events.clone(),
            None,
            None,
            node_cancel.clone(),
        ));
        let (service, _directory, _) = test_service(LookupConfig {
            request_timeout: Duration::from_millis(250),
            deadline: Duration::from_secs(3),
            retries: 0,
            ..LookupConfig::default()
        })
        .await;
        add_loopback_contact(&service, [0x80; 16], node_addr).await;

        let hash = [0x44; 16];
        let lookup = service.lookup_sources(hash, 1234, CancellationToken::new());
        let (mut sources, status, completion) = lookup.into_parts();
        completion.await.unwrap().unwrap();
        let source = timeout(Duration::from_secs(1), sources.recv())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            source.addr,
            SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 4662)
        );
        assert_eq!(status.borrow().state, KadState::Ready);
        assert_eq!(status.borrow().queried_nodes, 1);
        assert_eq!(status.borrow().discovered_sources, 1);
        assert_eq!(events.bootstrap.load(Ordering::Relaxed), 1);
        assert_eq!(events.hello.load(Ordering::Relaxed), 1);
        assert_eq!(events.routing.load(Ordering::Relaxed), 1);
        assert_eq!(events.source.load(Ordering::Relaxed), 1);

        service.shutdown().await;
        node_cancel.cancel();
        node.await.unwrap();
    }

    #[tokio::test]
    async fn lookup_for_client_rejects_its_own_ed2k_source_hash() {
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let node_addr = match socket.local_addr().unwrap() {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => unreachable!(),
        };
        let events = Arc::new(LoopbackEvents::default());
        let node_cancel = CancellationToken::new();
        let node = tokio::spawn(run_loopback_node(
            socket,
            [0x80; 16],
            vec![SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 4662)],
            events.clone(),
            None,
            None,
            node_cancel.clone(),
        ));
        let (service, _directory, _) = test_service(LookupConfig {
            request_timeout: Duration::from_millis(250),
            deadline: Duration::from_secs(3),
            retries: 0,
            ..LookupConfig::default()
        })
        .await;
        add_loopback_contact(&service, [0x80; 16], node_addr).await;

        let lookup = service.lookup_sources_for_client(
            [0x44; 16],
            1234,
            [0x81; 16],
            CancellationToken::new(),
        );
        let (mut sources, status, completion) = lookup.into_parts();
        completion.await.unwrap().unwrap();

        assert!(timeout(Duration::from_secs(1), sources.recv())
            .await
            .unwrap()
            .is_none());
        assert_eq!(status.borrow().discovered_sources, 0);
        assert_eq!(events.source.load(Ordering::Relaxed), 1);

        service.shutdown().await;
        node_cancel.cancel();
        node.await.unwrap();
    }

    #[tokio::test]
    async fn loopback_lookup_collects_split_source_response_datagrams() {
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let node_addr = match socket.local_addr().unwrap() {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => unreachable!(),
        };
        let events = Arc::new(LoopbackEvents::default());
        let node_cancel = CancellationToken::new();
        let node = tokio::spawn(run_loopback_node(
            socket,
            [0x90; 16],
            vec![
                SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 4662),
                SocketAddrV4::new(Ipv4Addr::new(8, 8, 4, 4), 4662),
            ],
            events.clone(),
            None,
            None,
            node_cancel.clone(),
        ));
        let (service, _directory, _) = test_service(LookupConfig {
            request_timeout: Duration::from_millis(250),
            deadline: Duration::from_secs(3),
            retries: 0,
            ..LookupConfig::default()
        })
        .await;
        add_loopback_contact(&service, [0x90; 16], node_addr).await;

        let lookup = service.lookup_sources([0x55; 16], 1234, CancellationToken::new());
        let (mut sources, status, completion) = lookup.into_parts();
        completion.await.unwrap().unwrap();
        let mut discovered = Vec::new();
        while let Some(source) = sources.recv().await {
            discovered.push(source.addr);
        }

        assert_eq!(discovered.len(), 2);
        assert!(discovered.contains(&SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 4662,)));
        assert!(discovered.contains(&SocketAddrV4::new(Ipv4Addr::new(8, 8, 4, 4), 4662,)));
        assert_eq!(status.borrow().discovered_sources, 2);
        assert_eq!(events.source.load(Ordering::Relaxed), 1);

        service.shutdown().await;
        node_cancel.cancel();
        node.await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_cancels_lookup_blocked_on_a_full_source_receiver() {
        let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let node_addr = match socket.local_addr().unwrap() {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => unreachable!(),
        };
        let events = Arc::new(LoopbackEvents::default());
        let node_cancel = CancellationToken::new();
        let node = tokio::spawn(run_loopback_node(
            socket,
            [0x80; 16],
            vec![
                SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 4662),
                SocketAddrV4::new(Ipv4Addr::new(8, 8, 4, 4), 4662),
            ],
            events,
            None,
            None,
            node_cancel.clone(),
        ));
        let (service, _directory, _) = test_service(LookupConfig {
            request_timeout: Duration::from_millis(250),
            deadline: Duration::from_secs(3),
            retries: 0,
            ..LookupConfig::default()
        })
        .await;
        add_loopback_contact(&service, [0x80; 16], node_addr).await;

        let lookup = service.lookup_sources_with_capacity_for_test(
            [0x55; 16],
            1234,
            CancellationToken::new(),
            1,
        );
        let (sources, mut status, completion) = lookup.into_parts();
        timeout(Duration::from_secs(1), async {
            loop {
                status
                    .changed()
                    .await
                    .expect("lookup status sender should remain open");
                if status.borrow().discovered_sources == 1 {
                    break;
                }
            }
        })
        .await
        .expect("first source should fill the bounded receiver");

        timeout(Duration::from_secs(1), service.shutdown())
            .await
            .expect("shutdown should cancel a source send blocked on receiver capacity");
        assert_eq!(status.borrow().state, KadState::Stopped);
        assert!(completion.await.unwrap().is_ok());
        drop(sources);

        node_cancel.cancel();
        node.await.unwrap();
    }

    #[tokio::test]
    async fn loopback_routing_requests_use_the_full_alpha_window() {
        let events = Arc::new(LoopbackEvents::default());
        let node_cancel = CancellationToken::new();
        let barrier = Arc::new(Barrier::new(routing::ALPHA + 1));
        let (routing_tx, mut routing_rx) = mpsc::channel(routing::ALPHA);
        let mut nodes = Vec::new();
        let mut addresses = Vec::new();
        for index in 0..routing::ALPHA {
            let socket = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
                .await
                .unwrap();
            let addr = match socket.local_addr().unwrap() {
                SocketAddr::V4(addr) => addr,
                SocketAddr::V6(_) => unreachable!(),
            };
            addresses.push(addr);
            nodes.push(tokio::spawn(run_loopback_node(
                socket,
                [0x80 + index as u8; 16],
                vec![SocketAddrV4::new(
                    Ipv4Addr::new(8, 8, 4, index as u8 + 1),
                    4662,
                )],
                events.clone(),
                Some(routing_tx.clone()),
                Some(barrier.clone()),
                node_cancel.clone(),
            )));
        }
        drop(routing_tx);
        let (service, _directory, _) = test_service(LookupConfig {
            alpha: routing::ALPHA,
            max_queries: routing::ALPHA,
            max_sources: routing::ALPHA,
            request_timeout: Duration::from_secs(2),
            deadline: Duration::from_secs(5),
            retries: 0,
        })
        .await;
        for (index, addr) in addresses.into_iter().enumerate() {
            add_loopback_contact(&service, [0x80 + index as u8; 16], addr).await;
        }

        let lookup = service.lookup_sources([0x22; 16], 99, CancellationToken::new());
        let (mut sources, mut status, completion) = lookup.into_parts();
        timeout(Duration::from_millis(500), async {
            for _ in 0..routing::ALPHA {
                routing_rx.recv().await.unwrap();
            }
        })
        .await
        .expect("all alpha routing requests should be sent before a reply");
        timeout(Duration::from_millis(500), async {
            loop {
                status
                    .changed()
                    .await
                    .expect("lookup status sender should remain open");
                if status.borrow().state == KadState::Searching
                    && status.borrow().queried_nodes == routing::ALPHA
                {
                    break;
                }
            }
        })
        .await
        .expect("status watcher should report routing progress before replies");
        barrier.wait().await;
        completion.await.unwrap().unwrap();
        let mut discovered = Vec::new();
        while let Ok(Some(source)) = timeout(Duration::from_millis(50), sources.recv()).await {
            discovered.push(source.addr);
        }
        assert_eq!(status.borrow().queried_nodes, routing::ALPHA);
        assert_eq!(discovered.len(), routing::ALPHA);

        service.shutdown().await;
        node_cancel.cancel();
        for node in nodes {
            node.await.unwrap();
        }
    }

    #[tokio::test]
    async fn shutdown_tracker_waits_for_active_kad_work() {
        let tracker = KadTaskTracker::new();
        let guard = tracker
            .try_start()
            .expect("new Kad work should register before shutdown");
        let close = tracker.close_and_wait();
        tokio::pin!(close);

        assert!(
            timeout(Duration::from_millis(25), &mut close)
                .await
                .is_err(),
            "shutdown must wait for tracked Kad work"
        );

        drop(guard);
        timeout(Duration::from_secs(1), &mut close)
            .await
            .expect("tracker should become idle after work finishes");
        assert_eq!(tracker.active_count(), 0);
    }

    #[tokio::test]
    async fn lookup_lifetime_is_tracked_until_cancellation_completes() {
        let (service, _directory, _port) = test_service(LookupConfig {
            request_timeout: Duration::from_secs(1),
            deadline: Duration::from_secs(2),
            retries: 0,
            ..LookupConfig::default()
        })
        .await;
        add_loopback_contact(
            &service,
            [0x44; 16],
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4672),
        )
        .await;

        let cancel = CancellationToken::new();
        let lookup = service.lookup_sources([0x33; 16], 1, cancel.clone());
        assert_eq!(service.runtime.active_tasks.active_count(), 1);

        cancel.cancel();
        let (_sources, _status, completion) = lookup.into_parts();
        assert!(completion.await.unwrap().is_ok());
        assert_eq!(service.runtime.active_tasks.active_count(), 0);

        service.shutdown().await;
    }

    #[tokio::test]
    async fn loopback_timeout_cancellation_and_shutdown_release_the_socket() {
        let blackhole = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let blackhole_addr = match blackhole.local_addr().unwrap() {
            SocketAddr::V4(addr) => addr,
            SocketAddr::V6(_) => unreachable!(),
        };
        let (service, _directory, port) = test_service(LookupConfig {
            request_timeout: Duration::from_millis(40),
            deadline: Duration::from_millis(150),
            retries: 0,
            ..LookupConfig::default()
        })
        .await;

        let cancel = CancellationToken::new();
        let timed_out = service
            .request(
                blackhole_addr,
                wire::build_ping(),
                &cancel,
                Instant::now() + Duration::from_millis(100),
            )
            .await;
        assert!(
            matches!(timed_out, Err(KadError::Io(error)) if error.kind() == std::io::ErrorKind::TimedOut)
        );
        assert!(service.runtime.pending.lock().await.is_empty());

        let request_cancel = CancellationToken::new();
        let request_service = service.clone();
        let request_cancel_for_task = request_cancel.clone();
        let cancelled = tokio::spawn(async move {
            request_service
                .request(
                    blackhole_addr,
                    wire::build_ping(),
                    &request_cancel_for_task,
                    Instant::now() + Duration::from_secs(2),
                )
                .await
        });
        sleep(Duration::from_millis(10)).await;
        request_cancel.cancel();
        assert!(matches!(cancelled.await.unwrap(), Err(KadError::Cancelled)));
        assert!(service.runtime.pending.lock().await.is_empty());

        let shutdown_service = service.clone();
        let interrupted_by_shutdown = tokio::spawn(async move {
            let no_cancel = CancellationToken::new();
            shutdown_service
                .request(
                    blackhole_addr,
                    wire::build_ping(),
                    &no_cancel,
                    Instant::now() + Duration::from_secs(2),
                )
                .await
        });
        sleep(Duration::from_millis(10)).await;
        service.shutdown().await;
        assert!(matches!(
            interrupted_by_shutdown.await.unwrap(),
            Err(KadError::Cancelled)
        ));
        assert!(service.runtime.pending.lock().await.is_empty());
        drop(blackhole);
        let rebound = UdpSocket::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)).await;
        assert!(rebound.is_ok(), "Kad shutdown should release the UDP port");
    }

    #[tokio::test]
    async fn cancelled_lookup_does_not_record_a_service_timeout() {
        let (service, _directory, _port) = test_service(LookupConfig::default()).await;
        add_loopback_contact(
            &service,
            [0x44; 16],
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4672),
        )
        .await;
        {
            let mut health = service.runtime.health.write().await;
            health.state = KadState::Ready;
            health.routing_contacts = 1;
        }
        let cancel = CancellationToken::new();
        cancel.cancel();

        let lookup = service.lookup_sources([0x33; 16], 1, cancel);
        let (_sources, status, completion) = lookup.into_parts();
        assert!(completion.await.unwrap().is_ok());
        assert_eq!(status.borrow().state, KadState::Stopped);

        let health = service.health_snapshot().await;
        assert_eq!(health.state, KadState::Ready);
        assert_ne!(health.last_error.as_deref(), Some("cancelled"));

        service.shutdown().await;
    }
}
