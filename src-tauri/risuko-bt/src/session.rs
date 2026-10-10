use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock as ParkingRwLock};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex as TokioMutex, RwLock};

use super::api::TorrentIdOrHash;
use super::blocklist::{BlockList, BlocklistApplyResult};
use super::core::metainfo::parse_torrent;
use super::core::{generate_peer_id, Id20, Lengths};
use super::peer::{KnownInfoHash, PeerCommand, PeerEvent, PeerHandle};
use super::torrent::{spawn as spawn_torrent, ManagedTorrent, TorrentCommand, TorrentInit};

#[derive(Default)]
struct PrivateTorrentOwners {
    count: usize,
    dhts: Vec<std::sync::Weak<super::dht::Dht>>,
}

static PRIVATE_TORRENT_OWNERS: LazyLock<Mutex<HashMap<Id20, PrivateTorrentOwners>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn remember_private_dht(entry: &mut PrivateTorrentOwners, dht: Arc<super::dht::Dht>) {
    let weak = Arc::downgrade(&dht);
    if !entry.dhts.iter().any(|known| known.ptr_eq(&weak)) {
        entry.dhts.push(weak);
    }
}

fn claim_private_hashes(dht: Option<Arc<super::dht::Dht>>, hashes: impl IntoIterator<Item = Id20>) {
    let mut owners = PRIVATE_TORRENT_OWNERS.lock();
    for hash in hashes {
        let entry = owners.entry(hash).or_default();
        entry.count += 1;
        if let Some(dht) = dht.as_ref() {
            remember_private_dht(entry, dht.clone());
            dht.set_private(hash, true);
        }
    }
}

fn mark_private_hashes(dht: Option<Arc<super::dht::Dht>>, hashes: impl IntoIterator<Item = Id20>) {
    let Some(dht) = dht else {
        return;
    };
    let mut owners = PRIVATE_TORRENT_OWNERS.lock();
    for hash in hashes {
        let Some(entry) = owners.get_mut(&hash) else {
            continue;
        };
        remember_private_dht(entry, dht.clone());
        dht.set_private(hash, true);
    }
}

fn release_private_hashes(hashes: impl IntoIterator<Item = Id20>) {
    let mut owners = PRIVATE_TORRENT_OWNERS.lock();
    for hash in hashes {
        let Some(entry) = owners.get_mut(&hash) else {
            continue;
        };
        entry.count = entry.count.saturating_sub(1);
        if entry.count == 0 {
            let entry = owners.remove(&hash).expect("private owner entry exists");
            for dht in entry.dhts {
                if let Some(dht) = dht.upgrade() {
                    dht.set_private(hash, false);
                }
            }
        }
    }
}

struct PrivateHashClaimGuard {
    hashes: Option<Vec<Id20>>,
}

impl PrivateHashClaimGuard {
    fn claim(dht: Option<Arc<super::dht::Dht>>, hashes: Vec<Id20>) -> Self {
        claim_private_hashes(dht, hashes.clone());
        Self {
            hashes: Some(hashes),
        }
    }

    fn disarm(&mut self) {
        self.hashes = None;
    }
}

impl Drop for PrivateHashClaimGuard {
    fn drop(&mut self) {
        if let Some(hashes) = self.hashes.take() {
            release_private_hashes(hashes);
        }
    }
}

struct PrivateHashReleaseGuard {
    hashes: Option<Vec<Id20>>,
}

impl PrivateHashReleaseGuard {
    fn new(hashes: Vec<Id20>) -> Self {
        Self {
            hashes: Some(hashes),
        }
    }

    fn release(&mut self) {
        if let Some(hashes) = self.hashes.take() {
            release_private_hashes(hashes);
        }
    }
}

impl Drop for PrivateHashReleaseGuard {
    fn drop(&mut self) {
        self.release();
    }
}

#[derive(Debug, Clone, Copy)]
pub struct UpnpStatus {
    pub enabled: bool,
    pub mapping_count: usize,
    pub discovery_attempts: usize,
}

#[derive(Clone, Debug, Default)]
pub struct ListenerOptions {
    pub listen_addr: Option<SocketAddr>,
    pub enable_upnp_port_forwarding: bool,
    pub upnp_lease: Option<std::time::Duration>,
    pub listen_ipv6: bool,
}

#[derive(Clone, Debug, Default)]
pub struct SessionOptions {
    pub disable_dht: bool,
    pub listen: Option<ListenerOptions>,
    pub max_outstanding_requests_per_peer: Option<usize>,
    pub max_peers_per_torrent: Option<usize>,
    pub max_connections: Option<usize>,
    pub hash_fail_ban_strikes: Option<u8>,
    pub download_limiter: Option<Arc<super::limiter::RateLimiter>>,
    pub upload_limiter: Option<Arc<super::limiter::RateLimiter>>,
    pub disable_local_service_discovery: bool,
    pub encryption: super::peer::EncryptionPolicy,
    pub p2p_proxy: Option<risuko_http::ProxyConnector>,
}

#[derive(Clone, Debug)]
pub struct AddTorrentOptions {
    pub output_folder: Option<String>,
    pub trackers: Option<Vec<String>>,
    pub only_files: Option<Vec<usize>>,
    pub create_subfolder: bool,
    pub initial_peers: Vec<std::net::SocketAddr>,
    pub initial_tracker_peers: Vec<std::net::SocketAddr>,
    pub p2p_proxy: Option<risuko_http::ProxyConnector>,
    pub p2p_proxy_is_task_override: bool,
    pub download_limit: u64,
    pub upload_limit: u64,
}

impl Default for AddTorrentOptions {
    fn default() -> Self {
        Self {
            output_folder: None,
            trackers: None,
            only_files: None,
            create_subfolder: true,
            initial_peers: Vec::new(),
            initial_tracker_peers: Vec::new(),
            p2p_proxy: None,
            p2p_proxy_is_task_override: false,
            download_limit: 0,
            upload_limit: 0,
        }
    }
}

pub fn split_initial_peer_sources(
    private_torrent: bool,
    peers: Vec<SocketAddr>,
    tracker_peers: Vec<SocketAddr>,
) -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    if private_torrent {
        return (Vec::new(), tracker_peers);
    }
    let tracker_set: std::collections::HashSet<_> = tracker_peers.iter().copied().collect();
    (
        peers
            .into_iter()
            .filter(|peer| !tracker_set.contains(peer))
            .collect(),
        tracker_peers,
    )
}

pub enum AddTorrent {
    TorrentFileBytes(Bytes),
}

pub enum AddTorrentResponse {
    Added(usize, Arc<ManagedTorrent>),
    AlreadyManaged(usize, Arc<ManagedTorrent>),
}

pub struct Session {
    output_dir: PathBuf,
    opts: SessionOptions,
    p2p_proxy: RwLock<Option<risuko_http::ProxyConnector>>,
    peer_id: Id20,
    listen_port: u16,
    tracker_source_addr: Option<SocketAddr>,
    utp: Option<Arc<super::utp::UtpSocket>>,
    download_limiter: Arc<super::limiter::RateLimiter>,
    upload_limiter: Arc<super::limiter::RateLimiter>,
    inner: Mutex<SessionInner>,
    accept_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    accept6_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    utp_accept_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    upnp_handle: Mutex<Option<super::upnp::UpnpHandle>>,
    lsd: Mutex<Option<Arc<super::lsd::LocalServiceDiscovery>>>,
    lsd_router_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    dht: Mutex<Option<Arc<super::dht::Dht>>>,
    blocklist: Arc<ParkingRwLock<BlockList>>,
    blocklist_apply: TokioMutex<()>,
    conn_budget: Arc<super::conn_budget::ConnBudget>,
}

impl Drop for Session {
    fn drop(&mut self) {
        let private_hashes: Vec<Id20> = self
            .inner
            .get_mut()
            .torrents
            .values()
            .filter_map(|handle| handle.metadata.load_full())
            .filter(|meta| meta.info.private)
            .flat_map(|meta| meta.announce_infohashes())
            .collect();
        release_private_hashes(private_hashes);
        if let Some(h) = self.accept_handle.lock().take() {
            h.abort();
        }
        if let Some(h) = self.accept6_handle.lock().take() {
            h.abort();
        }
        if let Some(h) = self.utp_accept_handle.lock().take() {
            h.abort();
        }
        if let Some(h) = self.lsd_router_handle.lock().take() {
            h.abort();
        }
        if let Some(utp) = self.utp.take() {
            utp.shutdown();
        }
        let _ = self.upnp_handle.lock().take();
        let _ = self.lsd.lock().take();
        let _ = self.dht.lock().take();
    }
}

struct SessionInner {
    torrents: HashMap<usize, Arc<ManagedTorrent>>,
    by_hash: HashMap<Id20, usize>,
    next_id: usize,
}

impl Session {
    pub async fn new_with_opts(
        output_dir: PathBuf,
        opts: SessionOptions,
    ) -> std::io::Result<Arc<Self>> {
        std::fs::create_dir_all(&output_dir)?;
        raise_nofile_limit();
        let peer_id = generate_peer_id();

        let listen_addr = opts
            .listen
            .as_ref()
            .and_then(|l| l.listen_addr)
            .unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
        let listener = TcpListener::bind(listen_addr).await?;
        let bound_listen_addr = listener.local_addr()?;
        let local_port = bound_listen_addr.port();
        let tracker_source_addr = direct_tracker_source_addr(listen_addr, bound_listen_addr);
        tracing::info!("session listening on port {local_port}");

        let utp = match super::utp::UtpSocket::bind_with_proxy(
            SocketAddr::from(([0, 0, 0, 0], local_port)),
            opts.p2p_proxy.clone(),
        )
        .await
        {
            Ok(s) => {
                tracing::info!("µTP listening on udp/{}", s.local_addr().port());
                Some(s)
            }
            Err(e) => {
                tracing::warn!("µTP: bind udp/{local_port} failed ({e}); trying ephemeral");
                match super::utp::UtpSocket::bind_with_proxy(
                    SocketAddr::from(([0, 0, 0, 0], 0)),
                    opts.p2p_proxy.clone(),
                )
                .await
                {
                    Ok(s) => {
                        tracing::info!("µTP listening on udp/{}", s.local_addr().port());
                        Some(s)
                    }
                    Err(e2) => {
                        tracing::warn!("µTP: disabled (bind failed: {e2})");
                        None
                    }
                }
            }
        };

        let upnp_handle = if opts
            .listen
            .as_ref()
            .map(|l| l.enable_upnp_port_forwarding)
            .unwrap_or(false)
        {
            let lease = opts
                .listen
                .as_ref()
                .and_then(|l| l.upnp_lease)
                .unwrap_or(std::time::Duration::from_secs(300));
            let mappings = upnp_mapping_specs(
                local_port,
                utp.as_ref().map(|socket| socket.local_addr().port()),
            );
            Some(super::upnp::UpnpPortForwarder::new(mappings, lease).spawn())
        } else {
            None
        };

        let download_limiter = opts
            .download_limiter
            .clone()
            .unwrap_or_else(|| Arc::new(super::limiter::RateLimiter::unlimited()));
        let upload_limiter = opts
            .upload_limiter
            .clone()
            .unwrap_or_else(|| Arc::new(super::limiter::RateLimiter::unlimited()));

        let conn_budget = super::conn_budget::ConnBudget::new(super::conn_budget::session_limit(
            opts.max_connections,
        ));
        tracing::debug!("peer connection budget: {}", conn_budget.limit());
        let initial_p2p_proxy = opts.p2p_proxy.clone();
        super::core::peer_id::set_session_peer_id(local_port, peer_id);
        let session = Arc::new(Self {
            output_dir,
            opts,
            p2p_proxy: RwLock::new(initial_p2p_proxy),
            peer_id,
            listen_port: local_port,
            tracker_source_addr,
            utp,
            download_limiter,
            upload_limiter,
            inner: Mutex::new(SessionInner {
                torrents: HashMap::new(),
                by_hash: HashMap::new(),
                next_id: 1,
            }),
            accept_handle: Mutex::new(None),
            accept6_handle: Mutex::new(None),
            utp_accept_handle: Mutex::new(None),
            upnp_handle: Mutex::new(upnp_handle),
            lsd: Mutex::new(None),
            lsd_router_handle: Mutex::new(None),
            dht: Mutex::new(None),
            blocklist: Arc::new(ParkingRwLock::new(BlockList::default())),
            blocklist_apply: TokioMutex::new(()),
            conn_budget,
        });

        if !session.opts.disable_local_service_discovery {
            match super::lsd::LocalServiceDiscovery::spawn(local_port, Vec::new()) {
                Ok((svc, mut rx)) => {
                    let weak = Arc::downgrade(&session);
                    let router = tokio::spawn(async move {
                        while let Some((ih, addr)) = rx.recv().await {
                            let Some(s) = weak.upgrade() else {
                                return;
                            };
                            let Some(handle) = s.get(TorrentIdOrHash::Hash(ih)) else {
                                continue;
                            };
                            let is_private = handle
                                .metadata
                                .load()
                                .as_ref()
                                .is_some_and(|meta| meta.info.private);
                            if !is_private {
                                let _ = handle
                                    .cmd_tx()
                                    .send(super::torrent::TorrentCommand::AddPeer(
                                        super::torrent::PeerCandidate {
                                            addr,
                                            source: super::torrent::PeerSource::Lsd,
                                        },
                                    ))
                                    .await;
                            }
                        }
                    });
                    *session.lsd.lock() = Some(svc);
                    *session.lsd_router_handle.lock() = Some(router);
                }
                Err(e) => tracing::warn!("lsd: not started: {e}"),
            }
        }

        if !session.opts.disable_dht {
            match super::dht::Dht::shared_with_proxy(session.opts.p2p_proxy.clone()).await {
                Some(dht) => *session.dht.lock() = Some(dht),
                None => tracing::warn!("dht: not started"),
            }
        }

        let weak = Arc::downgrade(&session);
        let accept_handle = tokio::spawn(run_accept_loop(listener, weak));
        *session.accept_handle.lock() = Some(accept_handle);

        if let Some(utp) = session.utp.clone() {
            let weak_utp = Arc::downgrade(&session);
            let h = tokio::spawn(run_utp_accept_loop(utp, weak_utp));
            *session.utp_accept_handle.lock() = Some(h);
        }

        let listen_v6 = session
            .opts
            .listen
            .as_ref()
            .map(|l| l.listen_ipv6)
            .unwrap_or(false);
        if listen_v6 {
            let v6_addr: SocketAddr = (std::net::Ipv6Addr::UNSPECIFIED, local_port).into();
            match bind_v6_listener(v6_addr) {
                Ok(std_listener) => match TcpListener::from_std(std_listener) {
                    Ok(listener6) => {
                        tracing::info!("session v6 listening on port {local_port}");
                        let weak6 = Arc::downgrade(&session);
                        let accept6 = tokio::spawn(run_accept_loop(listener6, weak6));
                        *session.accept6_handle.lock() = Some(accept6);
                    }
                    Err(e) => tracing::warn!("session v6 tokio convert failed: {e}"),
                },
                Err(e) => tracing::warn!("session v6 bind failed: {e}"),
            }
        }

        Ok(session)
    }

    async fn route_inbound_peer(
        self: &Arc<Self>,
        addr: SocketAddr,
        mut handle: PeerHandle,
        mut event_rx: mpsc::Receiver<PeerEvent>,
    ) {
        let Some(bind) = handle.bind.take() else {
            handle.io_abort.abort();
            return;
        };
        let Some(first) = event_rx.recv().await else {
            return;
        };
        let (info_hash, reserved, peer_id, encrypted, utp) = match &first {
            PeerEvent::Handshook {
                info_hash,
                reserved,
                peer_id,
                encrypted,
                utp,
            } => (*info_hash, *reserved, *peer_id, *encrypted, *utp),
            _ => return,
        };
        if self.blocklist.read().contains(addr.ip()) {
            handle.io_abort.abort();
            let _ = handle.tx.try_send(PeerCommand::Disconnect);
            return;
        }
        if self.conn_budget.is_full() {
            handle.io_abort.abort();
            let _ = handle.tx.try_send(PeerCommand::Disconnect);
            return;
        }
        let Some(t) = self.get(TorrentIdOrHash::Hash(info_hash)) else {
            handle.io_abort.abort();
            let _ = handle.tx.try_send(PeerCommand::Disconnect);
            return;
        };
        handle.gate.set(t.limits.down.clone());
        let _ = t
            .cmd_tx()
            .send(TorrentCommand::AddInboundPeer {
                addr,
                cmd_tx: handle.tx,
                piece_tx: handle.piece_tx,
                bind,
                reserved,
                peer_id,
                io_abort: handle.io_abort,
                encrypted,
                utp,
            })
            .await;
    }

    pub fn set_download_limit(&self, bps: u64) {
        self.download_limiter.set_limit(bps);
    }

    pub fn set_upload_limit(&self, bps: u64) {
        self.upload_limiter.set_limit(bps);
    }

    pub fn set_max_connections(&self, max: Option<usize>) {
        let limit = super::conn_budget::session_limit(max);
        self.conn_budget.set_limit(limit);
        tracing::debug!("peer connection budget: {limit}");
    }

    pub fn max_connections(&self) -> usize {
        self.conn_budget.limit()
    }

    pub fn listen_port(&self) -> u16 {
        self.listen_port
    }

    pub fn utp_socket(&self) -> Option<Arc<super::utp::UtpSocket>> {
        self.utp.clone()
    }

    pub fn conn_budget(&self) -> Arc<super::conn_budget::ConnBudget> {
        self.conn_budget.clone()
    }

    async fn utp_for_route(
        &self,
        proxy: Option<risuko_http::ProxyConnector>,
        task_override: bool,
    ) -> Option<Arc<super::utp::UtpSocket>> {
        if !task_override {
            return self.utp_socket();
        }
        match super::utp::UtpSocket::bind_with_proxy(SocketAddr::from(([0, 0, 0, 0], 0)), proxy)
            .await
        {
            Ok(socket) => Some(socket),
            Err(error) => {
                tracing::warn!("task µTP route unavailable: {error}");
                None
            }
        }
    }

    pub fn lsd_active(&self) -> bool {
        self.lsd.lock().is_some()
    }

    pub fn dht_active(&self) -> bool {
        self.dht.lock().is_some()
    }

    pub fn dht_routing_table_len(&self) -> usize {
        self.dht
            .lock()
            .as_ref()
            .map(|d| d.routing_table_len())
            .unwrap_or(0)
    }

    pub fn upnp_status(&self) -> UpnpStatus {
        let guard = self.upnp_handle.lock();
        let enabled = guard.is_some();
        let mapping_count = guard.as_ref().map(|h| h.mapping_count()).unwrap_or(0);
        let discovery_attempts = guard.as_ref().map(|h| h.discovery_attempts()).unwrap_or(0);
        UpnpStatus {
            enabled,
            mapping_count,
            discovery_attempts,
        }
    }

    pub async fn add_torrent(
        self: &Arc<Self>,
        which: AddTorrent,
        opts: Option<AddTorrentOptions>,
    ) -> Result<AddTorrentResponse, String> {
        let opts = opts.unwrap_or_default();
        let AddTorrent::TorrentFileBytes(bytes) = which;
        let meta = parse_torrent(&bytes).map_err(|e| format!("parse torrent: {e}"))?;
        self.add_from_meta(meta, opts).await
    }

    async fn existing_live_torrent(
        &self,
        info_hash: Id20,
    ) -> Result<Option<(usize, Arc<ManagedTorrent>)>, String> {
        let existing = {
            let inner = self.inner.lock();
            inner
                .by_hash
                .get(&info_hash)
                .copied()
                .and_then(|id| inner.torrents.get(&id).cloned().map(|t| (id, t)))
        };
        let Some((id, handle)) = existing else {
            return Ok(None);
        };
        if !managed_needs_rescan(&handle).await {
            return Ok(Some((id, handle)));
        }
        tracing::info!(
            "dropping stale torrent id={id} ({}) to re-verify local files",
            info_hash.to_hex()
        );
        match self.delete(TorrentIdOrHash::Id(id), false).await {
            Ok(()) => {}
            Err(e) if e == "not found" => {}
            Err(e) => return Err(e),
        }
        Ok(None)
    }

    pub async fn add_from_meta(
        self: &Arc<Self>,
        meta: super::core::TorrentMeta,
        opts: AddTorrentOptions,
    ) -> Result<AddTorrentResponse, String> {
        let info = meta.info.clone();
        if let Some((id, t)) = self.existing_live_torrent(meta.info_hash).await? {
            return Ok(AddTorrentResponse::AlreadyManaged(id, t));
        }
        {
            let inner = self.inner.lock();
            if let Some(&id) = inner.by_hash.get(&meta.info_hash) {
                if inner.torrents.contains_key(&id) {
                    if let Some(t) = inner.torrents.get(&id).cloned() {
                        return Ok(AddTorrentResponse::AlreadyManaged(id, t));
                    }
                }
                return Err("torrent for this info-hash is already being added".to_string());
            }
        }

        let root_dir = opts
            .output_folder
            .map(PathBuf::from)
            .unwrap_or_else(|| self.output_dir.clone());
        let create_subfolder = opts.create_subfolder;
        let root_dir = if info.single_file_mode || !create_subfolder {
            root_dir
        } else {
            root_dir.join(&*super::core::metainfo::fs_component(&info.name))
        };

        let lengths = Lengths::new(info.total_length(), info.piece_length)
            .map_err(|e| format!("bad lengths: {e}"))?;
        let mut meta = meta;
        let expanded = extra_tracker_tier(opts.trackers, info.private);
        if !expanded.is_empty() {
            meta.announce_list.push(expanded);
        }
        let id = {
            let mut inner = self.inner.lock();
            if let Some(&existing) = inner.by_hash.get(&meta.info_hash) {
                if let Some(t) = inner.torrents.get(&existing).cloned() {
                    return Ok(AddTorrentResponse::AlreadyManaged(existing, t));
                }
                return Err("torrent for this info-hash is already being added".to_string());
            }
            let id = inner.next_id;
            inner.next_id += 1;
            inner.by_hash.insert(meta.info_hash, id);
            id
        };
        let verifier = match crate::core::PieceVerifier::from_meta(&meta) {
            Ok(v) => v,
            Err(e) => {
                let mut inner = self.inner.lock();
                if inner.by_hash.get(&meta.info_hash) == Some(&id) {
                    inner.by_hash.remove(&meta.info_hash);
                }
                return Err(format!("build verifier: {e}"));
            }
        };
        // v2 reserved bit only for pure-v2; Xunlei peers drop it asserted on a v1 info_hash
        let advertise_v2 = matches!(meta.meta_version, crate::core::metainfo::MetaVersion::V2);
        let profile_proxy = self.p2p_proxy.read().await.clone();
        let route_proxy = if opts.p2p_proxy_is_task_override {
            opts.p2p_proxy.clone()
        } else {
            opts.p2p_proxy.clone().or(profile_proxy)
        };
        let init = TorrentInit {
            meta: meta.clone(),
            lengths,
            root_dir,
            only_files: opts.only_files,
            max_outstanding_per_peer: self.opts.max_outstanding_requests_per_peer,
            max_peers: self.opts.max_peers_per_torrent,
            hash_fail_ban_strikes: self.opts.hash_fail_ban_strikes,
            encryption: self.opts.encryption,
            advertise_v2,
            verifier,
            create_subfolder,
            utp: self
                .utp_for_route(route_proxy.clone(), opts.p2p_proxy_is_task_override)
                .await,
            limits: super::limiter::TorrentLimits {
                down: super::limiter::Throttle::new(
                    self.download_limiter.clone(),
                    Arc::new(super::limiter::RateLimiter::new(opts.download_limit)),
                ),
                up: super::limiter::Throttle::new(
                    self.upload_limiter.clone(),
                    Arc::new(super::limiter::RateLimiter::new(opts.upload_limit)),
                ),
            },
            dht: if info.private {
                None
            } else {
                self.dht.lock().clone()
            },
            p2p_proxy: route_proxy,
            p2p_proxy_is_task_override: opts.p2p_proxy_is_task_override,
            tracker_source_addr: self.tracker_source_addr,
            blocklist: self.blocklist.clone(),
            conn_budget: self.conn_budget.clone(),
        };
        let mut private_claim = info.private.then(|| {
            PrivateHashClaimGuard::claim(self.dht.lock().clone(), meta.announce_infohashes())
        });
        let handle = match spawn_torrent(id, init, self.peer_id, self.listen_port).await {
            Ok(h) => h,
            Err(e) => {
                let mut inner = self.inner.lock();
                if inner.by_hash.get(&meta.info_hash) == Some(&id) {
                    inner.by_hash.remove(&meta.info_hash);
                }
                return Err(format!("spawn torrent: {e}"));
            }
        };
        if let Some(claim) = private_claim.as_mut() {
            claim.disarm();
        }
        {
            let mut inner = self.inner.lock();
            inner.torrents.insert(id, handle.clone());
        }
        if !opts.initial_peers.is_empty() || !opts.initial_tracker_peers.is_empty() {
            let cmd_tx = handle.cmd_tx();
            let peers = opts.initial_peers;
            let tracker_peers = opts.initial_tracker_peers;
            tracing::info!(
                "Seeding torrent id={id} with {} tracker and {} manual peers",
                tracker_peers.len(),
                peers.len(),
            );
            tokio::spawn(async move {
                for addr in tracker_peers {
                    if cmd_tx
                        .send(super::torrent::TorrentCommand::AddPeer(
                            super::torrent::PeerCandidate {
                                addr,
                                source: super::torrent::PeerSource::Tracker,
                            },
                        ))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                for addr in peers {
                    if cmd_tx
                        .send(super::torrent::TorrentCommand::AddPeer(
                            super::torrent::PeerCandidate {
                                addr,
                                source: super::torrent::PeerSource::Manual,
                            },
                        ))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            });
        }
        if !info.private {
            if let Some(lsd) = self.lsd.lock().as_ref() {
                for hash in meta.announce_infohashes() {
                    lsd.add_infohash(hash);
                }
            }
        }
        Ok(AddTorrentResponse::Added(id, handle))
    }

    pub async fn reconfigure_p2p_proxy(
        &self,
        proxy: Option<risuko_http::ProxyConnector>,
        dht: Option<Arc<super::dht::Dht>>,
    ) -> Result<(), String> {
        let old_proxy = self.p2p_proxy.read().await.clone();
        let old_dht = self.dht.lock().clone();
        let handles: Vec<Arc<ManagedTorrent>> =
            self.with_torrents(|iter| iter.map(|(_, handle)| handle).collect());
        *self.p2p_proxy.write().await = proxy.clone();
        if let Some(utp) = self.utp.clone() {
            utp.reconfigure_proxy(proxy.clone()).await;
        }
        *self.dht.lock() = dht.clone();
        if dht.is_some() {
            for handle in &handles {
                if let Some(meta) = handle.metadata.load().as_ref() {
                    if meta.info.private {
                        mark_private_hashes(dht.clone(), meta.announce_infohashes());
                    }
                }
            }
        }

        for handle in &handles {
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            let replace_proxy = !handle.p2p_proxy_is_task_override;
            if let Err(error) = handle
                .cmd_tx()
                .send(TorrentCommand::ReconfigureP2p {
                    proxy: proxy.clone(),
                    replace_proxy,
                    dht: dht.clone(),
                    ack: ack_tx,
                })
                .await
            {
                self.rollback_p2p_route(&handles, old_proxy.clone(), old_dht.clone())
                    .await;
                return Err(format!(
                    "torrent {} route update failed: {error}",
                    handle.id
                ));
            }
            if let Err(error) = ack_rx.await {
                self.rollback_p2p_route(&handles, old_proxy.clone(), old_dht.clone())
                    .await;
                return Err(format!(
                    "torrent {} route update interrupted: {error}",
                    handle.id
                ));
            }
        }
        Ok(())
    }

    async fn rollback_p2p_route(
        &self,
        handles: &[Arc<ManagedTorrent>],
        proxy: Option<risuko_http::ProxyConnector>,
        dht: Option<Arc<super::dht::Dht>>,
    ) {
        for handle in handles {
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            let replace_proxy = !handle.p2p_proxy_is_task_override;
            let send_result = handle
                .cmd_tx()
                .send(TorrentCommand::ReconfigureP2p {
                    proxy: proxy.clone(),
                    replace_proxy,
                    dht: dht.clone(),
                    ack: ack_tx,
                })
                .await;
            if let Err(error) = send_result {
                tracing::error!(
                    torrent = handle.id,
                    "failed to send P2P route rollback: {error}"
                );
                continue;
            }
            if let Err(error) = ack_rx.await {
                tracing::error!(
                    torrent = handle.id,
                    "P2P route rollback acknowledgement failed: {error}"
                );
            }
        }
        if let Some(utp) = self.utp.clone() {
            utp.reconfigure_proxy(proxy.clone()).await;
        }
        *self.p2p_proxy.write().await = proxy;
        *self.dht.lock() = dht;
    }

    pub fn with_torrents<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&mut dyn Iterator<Item = (usize, Arc<ManagedTorrent>)>) -> T,
    {
        let snapshot: Vec<(usize, Arc<ManagedTorrent>)> = self
            .inner
            .lock()
            .torrents
            .iter()
            .map(|(id, h)| (*id, h.clone()))
            .collect();
        let mut iter = snapshot.into_iter();
        f(&mut iter)
    }

    pub fn get(&self, which: TorrentIdOrHash) -> Option<Arc<ManagedTorrent>> {
        let inner = self.inner.lock();
        match which {
            TorrentIdOrHash::Id(id) => inner.torrents.get(&id).cloned(),
            TorrentIdOrHash::Hash(h) => inner
                .by_hash
                .get(&h)
                .and_then(|id| inner.torrents.get(id))
                .cloned()
                .or_else(|| {
                    inner
                        .torrents
                        .values()
                        .find(|handle| {
                            handle
                                .metadata
                                .load()
                                .as_ref()
                                .is_some_and(|meta| meta.announce_infohashes().contains(&h))
                        })
                        .cloned()
                }),
        }
    }

    pub async fn pause(&self, handle: &Arc<ManagedTorrent>) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .cmd_tx()
            .send(TorrentCommand::Pause(tx))
            .await
            .map_err(|e| e.to_string())?;
        rx.await.map_err(|e| e.to_string())
    }

    pub async fn pause_if_halted(&self, handle: &Arc<ManagedTorrent>) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .cmd_tx()
            .send(TorrentCommand::PauseIfHalted(tx))
            .await
            .map_err(|e| e.to_string())?;
        rx.await.map_err(|e| e.to_string())
    }

    pub async fn unpause(&self, handle: &Arc<ManagedTorrent>) -> Result<(), String> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .cmd_tx()
            .send(TorrentCommand::Unpause(tx))
            .await
            .map_err(|e| e.to_string())?;
        rx.await.map_err(|e| e.to_string())
    }

    pub async fn delete(&self, which: TorrentIdOrHash, with_files: bool) -> Result<(), String> {
        let handle = {
            let mut inner = self.inner.lock();
            let id = match which {
                TorrentIdOrHash::Id(id) => id,
                TorrentIdOrHash::Hash(hash) => *inner
                    .by_hash
                    .get(&hash)
                    .ok_or_else(|| "not found".to_string())?,
            };
            inner
                .torrents
                .remove(&id)
                .ok_or_else(|| "not found".to_string())?
        };
        let is_private = handle
            .metadata
            .load()
            .as_ref()
            .is_some_and(|meta| meta.info.private);
        if !is_private {
            if let Some(lsd) = self.lsd.lock().as_ref() {
                let hashes = handle
                    .metadata
                    .load()
                    .as_ref()
                    .map_or_else(|| vec![handle.info_hash], |meta| meta.announce_infohashes());
                for hash in hashes {
                    lsd.remove_infohash(hash);
                }
            }
        }
        let private_hashes = handle
            .metadata
            .load()
            .as_ref()
            .filter(|meta| meta.info.private)
            .map(|meta| meta.announce_infohashes());
        let mut private_release = private_hashes.map(PrivateHashReleaseGuard::new);
        let file_paths: Option<Vec<(PathBuf, bool)>> = if with_files {
            let create_subfolder = handle.create_subfolder;
            let root = handle.root_dir.clone();
            handle
                .with_metadata(|meta| {
                    let name_empty = meta.info.name.is_empty();
                    let parts_dir = (
                        super::storage::parts_dir_for(&root, &handle.info_hash.to_hex()),
                        true,
                    );
                    if meta.info.single_file_mode && !name_empty {
                        vec![
                            (
                                root.join(&*super::core::metainfo::fs_component(&meta.info.name)),
                                false,
                            ),
                            parts_dir,
                        ]
                    } else if !meta.info.single_file_mode && create_subfolder && !name_empty {
                        vec![(root, true)]
                    } else {
                        meta.info
                            .iter_file_details()
                            .filter(|f| !f.padding)
                            .filter_map(|f| {
                                let mut p = root.clone();
                                for c in f.filename.split('/') {
                                    if !super::core::metainfo::is_safe_component(c) {
                                        return None;
                                    }
                                    p.push(&*super::core::metainfo::fs_component(c));
                                }
                                Some((p, false))
                            })
                            .chain(std::iter::once(parts_dir))
                            .collect()
                    }
                })
                .ok()
        } else {
            None
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        if let Err(error) = handle.cmd_tx().send(TorrentCommand::Stop(tx)).await {
            let mut inner = self.inner.lock();
            if inner.by_hash.get(&handle.info_hash) == Some(&handle.id) {
                inner.by_hash.remove(&handle.info_hash);
            }
            return Err(error.to_string());
        }
        let _ = rx.await;
        {
            let mut inner = self.inner.lock();
            if inner.by_hash.get(&handle.info_hash) == Some(&handle.id) {
                inner.by_hash.remove(&handle.info_hash);
            }
        }
        if is_private {
            if let Some(release) = private_release.as_mut() {
                release.release();
            }
        }
        if let Some(paths) = file_paths {
            for (p, is_dir) in paths {
                let res = if is_dir {
                    tokio::fs::remove_dir_all(&p).await
                } else {
                    tokio::fs::remove_file(&p).await
                };
                match res {
                    Ok(()) => tracing::info!("deleted torrent data: {}", p.display()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => tracing::warn!("failed to delete {}: {}", p.display(), e),
                }
                if p.parent()
                    .and_then(|parent| parent.file_name())
                    .is_some_and(|name| name == super::storage::PARTS_DIR)
                {
                    if let Some(parent) = p.parent() {
                        let _ = tokio::fs::remove_dir(parent).await;
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn shutdown(&self) {
        let handles: Vec<Arc<ManagedTorrent>> =
            self.inner.lock().torrents.values().cloned().collect();
        let stops = handles.iter().map(|handle| async move {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if handle.cmd_tx().send(TorrentCommand::Stop(tx)).await.is_ok() {
                let _ = rx.await;
            }
        });
        if tokio::time::timeout(SHUTDOWN_TIMEOUT, futures_util::future::join_all(stops))
            .await
            .is_err()
        {
            tracing::warn!("torrent shutdown exceeded {SHUTDOWN_TIMEOUT:?}");
        }
        {
            let mut inner = self.inner.lock();
            inner.torrents.clear();
            inner.by_hash.clear();
        }
        let private_hashes: Vec<Id20> = handles
            .iter()
            .filter_map(|handle| handle.metadata.load_full())
            .filter(|meta| meta.info.private)
            .flat_map(|meta| meta.announce_infohashes())
            .collect();
        release_private_hashes(private_hashes);
    }

    pub async fn add_peer(&self, info_hash: Id20, addr: SocketAddr) -> Result<(), String> {
        let handle = self
            .get(TorrentIdOrHash::Hash(info_hash))
            .ok_or_else(|| "torrent not found".to_string())?;
        handle
            .cmd_tx()
            .send(TorrentCommand::AddPeer(super::torrent::PeerCandidate {
                addr,
                source: super::torrent::PeerSource::Manual,
            }))
            .await
            .map_err(|e| e.to_string())
    }

    pub async fn add_trackers(&self, info_hash: Id20, urls: Vec<String>) -> Result<usize, String> {
        let handle = self
            .get(TorrentIdOrHash::Hash(info_hash))
            .ok_or_else(|| "torrent not found".to_string())?;
        let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
        handle
            .cmd_tx()
            .send(TorrentCommand::AddTrackers { urls, ack: ack_tx })
            .await
            .map_err(|e| e.to_string())?;
        ack_rx.await.map_err(|e| e.to_string())
    }

    pub async fn set_peer_blocklist(&self, entries: Vec<String>) -> BlocklistApplyResult {
        let _apply_guard = self.blocklist_apply.lock().await;
        let rules = super::blocklist::PreparedRules::parse(&entries);
        let mut meta = self.blocklist.write().replace_prepared(rules);
        let handles: Vec<Arc<ManagedTorrent>> =
            self.with_torrents(|iter| iter.map(|(_, handle)| handle.clone()).collect());
        for handle in handles {
            let (ack_tx, ack_rx) = tokio::sync::oneshot::channel();
            if handle
                .cmd_tx()
                .send(TorrentCommand::ApplyBlocklist { ack: ack_tx })
                .await
                .is_err()
            {
                continue;
            }
            if let Ok((disconnected, removed)) = ack_rx.await {
                meta.disconnected_peers = meta.disconnected_peers.saturating_add(disconnected);
                meta.removed_peers = meta.removed_peers.saturating_add(removed);
            }
        }
        meta
    }
}

async fn managed_needs_rescan(handle: &ManagedTorrent) -> bool {
    let stats = handle.stats();
    if stats.finished {
        return true;
    }
    if stats.progress_bytes == 0 {
        return false;
    }
    match handle
        .with_metadata(|meta| super::storage::FilesystemStorage::new(&meta.info, &handle.root_dir))
    {
        Ok(storage) => !storage.has_existing_payload_files().await,
        Err(_) => false,
    }
}

fn upnp_mapping_specs(tcp_port: u16, utp_port: Option<u16>) -> Vec<(u16, super::upnp::MapProto)> {
    let mut mappings = vec![(tcp_port, crate::upnp::MapProto::Tcp)];
    if let Some(utp_port) = utp_port {
        mappings.push((utp_port, crate::upnp::MapProto::Udp));
    }
    mappings
}

fn direct_tracker_source_addr(configured: SocketAddr, bound: SocketAddr) -> Option<SocketAddr> {
    let ip = if configured.ip().is_unspecified() {
        return None;
    } else {
        bound.ip()
    };
    Some(SocketAddr::new(ip, 0))
}

fn bind_v6_listener(addr: SocketAddr) -> std::io::Result<std::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let sock = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
    sock.set_only_v6(true)?;
    // SO_REUSEADDR on Windows permits port hijacking, so skip it
    #[cfg(not(target_os = "windows"))]
    sock.set_reuse_address(true)?;
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    sock.listen(128)?;
    Ok(sock.into())
}

fn known_infohashes(s: &Session) -> Vec<KnownInfoHash> {
    s.inner
        .lock()
        .torrents
        .values()
        .flat_map(|handle| {
            let meta = handle.metadata.load();
            let advertise_dht = meta.as_ref().is_none_or(|meta| !meta.info.private);
            let mut hashes = vec![handle.info_hash];
            if let Some(meta) = meta.as_ref() {
                for hash in meta.announce_infohashes() {
                    if !hashes.contains(&hash) {
                        hashes.push(hash);
                    }
                }
            }
            let serves_v2 = handle.advertise_v2.load(Ordering::Relaxed);
            let v1_hash = meta.as_ref().and_then(|meta| meta.info_hashes().v1);
            hashes.into_iter().map(move |info_hash| KnownInfoHash {
                info_hash,
                advertise_v2: serves_v2 && Some(info_hash) != v1_hash,
                advertise_dht,
                ext_handshake_builder: Some(handle.ext_handshake_builder.clone()),
            })
        })
        .collect()
}

const SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

const MAX_INBOUND_HANDSHAKES: usize = 64;

fn extra_tracker_tier(extra: Option<Vec<String>>, private: bool) -> Vec<String> {
    match extra {
        Some(extra) if !private => super::torrent::split_tracker_lists(extra),
        _ => Vec::new(),
    }
}

fn raise_nofile_limit() {
    #[cfg(unix)]
    {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            const WANT: libc::rlim_t = 8192;
            let mut lim = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            // SAFETY: `lim` is a valid out pointer for the duration of the call
            if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } != 0 {
                return;
            }
            let target = lim.rlim_max.min(WANT);
            if target <= lim.rlim_cur {
                return;
            }
            lim.rlim_cur = target;
            // SAFETY: `lim` is a valid in pointer for the duration of the call
            if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &lim) } != 0 {
                tracing::debug!("could not raise RLIMIT_NOFILE to {target}");
            }
        });
    }
}

async fn run_accept_loop(listener: TcpListener, weak: std::sync::Weak<Session>) {
    let handshakes = Arc::new(tokio::sync::Semaphore::new(MAX_INBOUND_HANDSHAKES));
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let _ = stream.set_nodelay(true);
                let Some(s) = weak.upgrade() else {
                    return;
                };
                if s.blocklist.read().contains(addr.ip()) || s.conn_budget.is_full() {
                    continue;
                }
                let Ok(permit) = handshakes.clone().try_acquire_owned() else {
                    continue;
                };
                tokio::spawn(async move {
                    let allowed = known_infohashes(&s);
                    let policy = s.opts.encryption;
                    let res = super::peer::accept_deferred(
                        stream,
                        s.peer_id,
                        allowed,
                        std::time::Duration::from_secs(30),
                        policy,
                    )
                    .await;
                    drop(permit);
                    match res {
                        Ok((handle, rx)) => {
                            s.route_inbound_peer(addr, handle, rx).await;
                        }
                        Err(e) => {
                            tracing::debug!("inbound peer handshake failed: {e}")
                        }
                    }
                });
            }
            Err(e) => {
                if super::conn_budget::is_fd_exhaustion(&e) {
                    tracing::warn!("accept backing off, out of file descriptors: {e}");
                } else {
                    tracing::debug!("accept failed: {e}");
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
}

async fn run_utp_accept_loop(utp: Arc<super::utp::UtpSocket>, weak: std::sync::Weak<Session>) {
    let handshakes = Arc::new(tokio::sync::Semaphore::new(MAX_INBOUND_HANDSHAKES));
    loop {
        let stream = match utp.accept().await {
            Ok(s) => s,
            Err(_) => return,
        };
        let Some(s) = weak.upgrade() else {
            return;
        };
        let addr = stream.peer_addr();
        if s.blocklist.read().contains(addr.ip()) || s.conn_budget.is_full() {
            continue;
        }
        let Ok(permit) = handshakes.clone().try_acquire_owned() else {
            continue;
        };
        tokio::spawn(async move {
            let allowed = known_infohashes(&s);
            if allowed.is_empty() {
                return;
            }
            let res = super::peer::accept_utp_deferred(
                stream,
                s.peer_id,
                allowed,
                std::time::Duration::from_secs(30),
                s.opts.encryption,
            )
            .await;
            drop(permit);
            match res {
                Ok((handle, rx)) => {
                    s.route_inbound_peer(addr, handle, rx).await;
                }
                Err(e) => {
                    tracing::debug!("inbound µTP peer handshake failed: {e}");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_torrents_ignore_extra_trackers() {
        let extra = || Some(vec!["udp://a:1, udp://b:2\nudp://a:1".to_string()]);
        assert_eq!(
            extra_tracker_tier(extra(), false),
            ["udp://a:1", "udp://b:2"]
        );
        assert!(extra_tracker_tier(extra(), true).is_empty());
    }

    #[test]
    fn upnp_specs_map_tcp_and_actual_utp_ports() {
        assert_eq!(
            upnp_mapping_specs(41_000, Some(42_000)),
            vec![
                (41_000, crate::upnp::MapProto::Tcp),
                (42_000, crate::upnp::MapProto::Udp),
            ]
        );
        assert_eq!(
            upnp_mapping_specs(41_000, None),
            vec![(41_000, crate::upnp::MapProto::Tcp)]
        );
    }

    #[test]
    fn tracker_source_uses_explicit_listener_ip_and_ignores_wildcards() {
        let bound: SocketAddr = "192.0.2.7:43123".parse().unwrap();
        assert_eq!(
            direct_tracker_source_addr("192.0.2.7:0".parse().unwrap(), bound),
            Some("192.0.2.7:0".parse().unwrap())
        );
        assert_eq!(
            direct_tracker_source_addr("0.0.0.0:0".parse().unwrap(), bound),
            None
        );
    }
}
