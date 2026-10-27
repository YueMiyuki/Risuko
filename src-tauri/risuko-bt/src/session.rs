use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
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
    pub dht: super::dht::DhtOptions,
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
    utp6_accept_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
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
        if let Some(h) = self.utp6_accept_handle.lock().take() {
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
        super::core::peer_id::clear_session_peer_id(self.listen_port, self.peer_id);
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

        let utp4 = match super::utp::UtpSocket::bind_with_proxy(
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
        let utp4_port = utp4.as_ref().map(|socket| socket.local_addr().port());
        let listen_v6 = opts.listen.as_ref().is_some_and(|l| l.listen_ipv6);
        let utp6 = if listen_v6 {
            bind_utp_v6(local_port, utp4_port, opts.p2p_proxy.clone()).await
        } else {
            None
        };
        let utp = match (utp4, utp6) {
            (Some(v4), Some(v6)) => {
                v4.attach_v6(v6);
                Some(v4)
            }
            (v4, v6) => v4.or(v6),
        };

        let dht = if opts.disable_dht {
            None
        } else {
            let dht =
                super::dht::Dht::shared_with_proxy(opts.p2p_proxy.clone(), opts.dht.clone()).await;
            if dht.is_none() {
                tracing::warn!("dht: not started");
            }
            dht
        };

        let upnp_handle = if opts
            .listen
            .as_ref()
            .map(|l| l.enable_upnp_port_forwarding)
            .unwrap_or(false)
        {
            let mappings = upnp_mapping_specs(
                local_port,
                utp4_port,
                dht.as_ref().and_then(|dht| dht.local_port_for(false)),
            );
            Some(super::upnp::UpnpPortForwarder::new(mappings, upnp_lease(&opts)).spawn())
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
        let blocklist = Arc::new(ParkingRwLock::new(BlockList::default()));
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
            utp6_accept_handle: Mutex::new(None),
            upnp_handle: Mutex::new(upnp_handle),
            lsd: Mutex::new(None),
            lsd_router_handle: Mutex::new(None),
            dht: Mutex::new(dht),
            blocklist,
            blocklist_apply: TokioMutex::new(()),
            conn_budget,
        });
        if let Some(utp) = session.utp.as_ref() {
            let weak = Arc::downgrade(&session);
            utp.set_inbound_filter(move |ip| {
                weak.upgrade()
                    .is_some_and(|session| session.admits_inbound(ip))
            });
        }

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

        let limits = HandshakeLimits::new();
        let weak = Arc::downgrade(&session);
        let accept_handle = tokio::spawn(run_accept_loop(listener, weak, limits.clone()));
        *session.accept_handle.lock() = Some(accept_handle);

        if let Some(utp) = session.utp.clone() {
            if let Some(utp6) = utp.v6_socket() {
                let weak_utp6 = Arc::downgrade(&session);
                let h = tokio::spawn(run_utp_accept_loop(utp6, weak_utp6, limits.clone()));
                *session.utp6_accept_handle.lock() = Some(h);
            }
            let weak_utp = Arc::downgrade(&session);
            let h = tokio::spawn(run_utp_accept_loop(utp, weak_utp, limits.clone()));
            *session.utp_accept_handle.lock() = Some(h);
        }

        if listen_v6 {
            let v6_addr: SocketAddr = (std::net::Ipv6Addr::UNSPECIFIED, local_port).into();
            match bind_v6_listener(v6_addr) {
                Ok(std_listener) => match TcpListener::from_std(std_listener) {
                    Ok(listener6) => {
                        tracing::info!("session v6 listening on port {local_port}");
                        let weak6 = Arc::downgrade(&session);
                        let accept6 =
                            tokio::spawn(run_accept_loop(listener6, weak6, limits.clone()));
                        *session.accept6_handle.lock() = Some(accept6);
                    }
                    Err(e) => tracing::warn!("session v6 tokio convert failed: {e}"),
                },
                Err(e) => tracing::warn!("session v6 bind failed: {e}"),
            }
        }
        let v6_listening = session.accept6_handle.lock().is_some();
        if let Some(dht) = session.dht.lock().as_ref() {
            dht.set_announce_v6(v6_listening);
        }

        Ok(session)
    }

    fn admits_inbound(&self, ip: IpAddr) -> bool {
        !self.blocklist.read().contains(ip) && !self.conn_budget.is_full()
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
        let torrent = self
            .admits_inbound(addr.ip())
            .then(|| self.get(TorrentIdOrHash::Hash(info_hash)))
            .flatten();
        let Some(t) = torrent else {
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
            Ok(socket) => {
                socket.refuse_inbound();
                Some(socket)
            }
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

    pub fn dht_local_port(&self) -> Option<u16> {
        self.dht.lock().as_ref()?.local_port_for(false)
    }

    pub fn dht_routing_table_len(&self) -> usize {
        self.dht
            .lock()
            .as_ref()
            .map(|d| d.routing_table_len())
            .unwrap_or(0)
    }

    pub fn dht_node_id_compliant(&self) -> Option<bool> {
        self.dht.lock().as_ref()?.node_id_compliant()
    }

    pub fn external_addresses(&self) -> super::external_ip::ExternalAddresses {
        super::external_ip::ExternalIp::shared().addresses()
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
        append_extra_tracker_tier(&mut meta, extra_tracker_tier(opts.trackers, info.private));
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
        self.refresh_upnp_mappings(dht.as_ref());
        if let Some(next) = dht.as_ref() {
            next.set_announce_v6(self.accept6_handle.lock().is_some());
        }
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
        self.refresh_upnp_mappings(dht.as_ref());
        *self.dht.lock() = dht;
    }

    fn refresh_upnp_mappings(&self, dht: Option<&Arc<super::dht::Dht>>) {
        let mut upnp = self.upnp_handle.lock();
        let Some(current) = upnp.as_ref() else {
            return;
        };
        let utp4_port = self
            .utp
            .as_ref()
            .map(|utp| utp.local_addr())
            .filter(SocketAddr::is_ipv4)
            .map(|addr| addr.port());
        let mappings = upnp_mapping_specs(
            self.listen_port,
            utp4_port,
            dht.and_then(|dht| dht.local_port_for(false)),
        );
        if current.ports() != mappings.as_slice() {
            *upnp =
                Some(super::upnp::UpnpPortForwarder::new(mappings, upnp_lease(&self.opts)).spawn());
        }
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
        let dht = self.dht.lock().clone();
        if let Some(dht) = dht {
            dht.persist_routing_state().await;
        }
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

fn upnp_lease(opts: &SessionOptions) -> std::time::Duration {
    opts.listen
        .as_ref()
        .and_then(|l| l.upnp_lease)
        .unwrap_or(std::time::Duration::from_secs(300))
}

fn upnp_mapping_specs(
    tcp_port: u16,
    utp_port: Option<u16>,
    dht_port: Option<u16>,
) -> Vec<(u16, super::upnp::MapProto)> {
    let mut mappings = vec![(tcp_port, crate::upnp::MapProto::Tcp)];
    for port in [utp_port, dht_port].into_iter().flatten() {
        if !mappings.contains(&(port, crate::upnp::MapProto::Udp)) {
            mappings.push((port, crate::upnp::MapProto::Udp));
        }
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

async fn bind_utp_v6(
    local_port: u16,
    utp4_port: Option<u16>,
    proxy: Option<risuko_http::ProxyConnector>,
) -> Option<Arc<super::utp::UtpSocket>> {
    let mut ports = vec![local_port];
    ports.extend(utp4_port.filter(|port| *port != local_port));
    ports.push(0);
    let bound = match crate::net::bind_first_available("µTP v6: udp6", &ports, |port| {
        super::utp::UtpSocket::bind_v6_only_with_proxy(port, proxy.clone())
    })
    .await
    {
        Ok(socket) => socket,
        Err(e) => {
            tracing::debug!("µTP v6: disabled (bind failed: {e})");
            return None;
        }
    };
    tracing::info!("µTP listening on udp6/{}", bound.local_addr().port());
    Some(bound)
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

const MAX_INBOUND_HANDSHAKES: usize = 256;
const MAX_INBOUND_HANDSHAKES_PER_IP: usize = 8;
const INBOUND_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Clone)]
struct HandshakeLimits {
    slots: Arc<super::utp::slots::IpSlots>,
    timeout: std::time::Duration,
}

impl HandshakeLimits {
    fn new() -> Self {
        Self {
            slots: super::utp::slots::IpSlots::new(
                MAX_INBOUND_HANDSHAKES,
                MAX_INBOUND_HANDSHAKES_PER_IP,
            ),
            timeout: INBOUND_HANDSHAKE_TIMEOUT,
        }
    }
}

async fn within_handshake_deadline<T>(
    timeout: std::time::Duration,
    handshake: impl std::future::Future<Output = std::io::Result<T>>,
) -> std::io::Result<T> {
    tokio::time::timeout(timeout, handshake)
        .await
        .unwrap_or_else(|_| {
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "inbound handshake deadline",
            ))
        })
}

fn append_extra_tracker_tier(meta: &mut super::core::TorrentMeta, mut extra: Vec<String>) {
    let mut tiers = super::torrent::declared_tracker_tiers(meta);
    extra.retain(|url| !tiers.iter().flatten().any(|known| known == url));
    if extra.is_empty() {
        return;
    }
    tiers.push(extra);
    meta.announce_list = tiers;
}

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

async fn run_accept_loop(
    listener: TcpListener,
    weak: std::sync::Weak<Session>,
    limits: HandshakeLimits,
) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let _ = stream.set_nodelay(true);
                let Some(s) = weak.upgrade() else {
                    return;
                };
                if !s.admits_inbound(addr.ip()) {
                    continue;
                }
                let Some(slot) = limits.slots.try_acquire(addr.ip()) else {
                    continue;
                };
                let deadline = limits.timeout;
                tokio::spawn(async move {
                    let allowed = known_infohashes(&s);
                    let policy = s.opts.encryption;
                    let res = within_handshake_deadline(
                        deadline,
                        super::peer::accept_deferred(stream, s.peer_id, allowed, deadline, policy),
                    )
                    .await;
                    drop(slot);
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

async fn run_utp_accept_loop(
    utp: Arc<super::utp::UtpSocket>,
    weak: std::sync::Weak<Session>,
    limits: HandshakeLimits,
) {
    loop {
        let stream = match utp.accept().await {
            Ok(s) => s,
            Err(_) => return,
        };
        let Some(s) = weak.upgrade() else {
            stream.abort();
            return;
        };
        let addr = stream.peer_addr();
        if !s.admits_inbound(addr.ip()) {
            stream.abort();
            continue;
        }
        let Some(slot) = limits.slots.try_acquire(addr.ip()) else {
            stream.abort();
            continue;
        };
        let allowed = known_infohashes(&s);
        if allowed.is_empty() {
            stream.abort();
            continue;
        }
        let deadline = limits.timeout;
        let abort_arm = stream.arm_abort_on_drop();
        tokio::spawn(async move {
            let res = within_handshake_deadline(
                deadline,
                super::peer::accept_utp_deferred(
                    stream,
                    s.peer_id,
                    allowed,
                    deadline,
                    s.opts.encryption,
                ),
            )
            .await;
            drop(slot);
            match res {
                Ok((handle, rx)) => {
                    abort_arm.disarm();
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
    use crate::test_util::ipv6_loopback_available;

    #[test]
    fn private_torrents_ignore_extra_trackers() {
        let extra = || Some(vec!["udp://a:1, udp://b:2\nudp://a:1".to_string()]);
        assert_eq!(
            extra_tracker_tier(extra(), false),
            ["udp://a:1", "udp://b:2"]
        );
        assert!(extra_tracker_tier(extra(), true).is_empty());
    }

    fn announce_only_meta(
        announce: &str,
        announce_list: Vec<Vec<&str>>,
    ) -> super::super::core::TorrentMeta {
        use crate::bencode::{encode_to_vec, Value};
        let tier = |urls: &[&str]| {
            Value::List(
                urls.iter()
                    .map(|u| Value::Bytes(u.as_bytes().to_vec()))
                    .collect(),
            )
        };
        let mut top = vec![
            (
                b"announce".to_vec(),
                Value::Bytes(announce.as_bytes().to_vec()),
            ),
            (
                b"info".to_vec(),
                Value::Dict(vec![
                    (b"length".to_vec(), Value::Int(1)),
                    (b"name".to_vec(), Value::Bytes(b"x".to_vec())),
                    (b"piece length".to_vec(), Value::Int(16384)),
                    (b"pieces".to_vec(), Value::Bytes(vec![0u8; 20])),
                ]),
            ),
        ];
        if !announce_list.is_empty() {
            top.push((
                b"announce-list".to_vec(),
                Value::List(announce_list.iter().map(|t| tier(t)).collect()),
            ));
        }
        crate::core::metainfo::parse_torrent(&encode_to_vec(&Value::Dict(top))).unwrap()
    }

    #[test]
    fn extra_trackers_keep_the_torrents_own_announce_url() {
        let extras = || vec!["udp://x:1".to_string(), "http://a/announce".to_string()];
        let mut meta = announce_only_meta("http://a/announce", Vec::new());
        append_extra_tracker_tier(&mut meta, extras());
        let tiers = crate::torrent::declared_tracker_tiers(&meta);
        assert_eq!(tiers[0], ["http://a/announce"]);
        assert_eq!(tiers[1], ["udp://x:1"]);
        assert_eq!(tiers.len(), 2);
        assert_eq!(meta.announce_list, tiers);

        let mut listed =
            announce_only_meta("http://ignored/announce", vec![vec!["http://b/announce"]]);
        append_extra_tracker_tier(&mut listed, extras());
        assert_eq!(
            listed.announce_list,
            vec![
                vec!["http://b/announce".to_string()],
                vec!["udp://x:1".to_string(), "http://a/announce".to_string()],
            ]
        );

        let mut bare = announce_only_meta("http://a/announce", Vec::new());
        append_extra_tracker_tier(&mut bare, Vec::new());
        assert!(bare.announce_list.is_empty());
    }

    #[test]
    fn announce_is_used_when_the_announce_list_holds_only_empty_tiers() {
        let mut meta = announce_only_meta("http://a/announce", vec![vec![]]);
        assert_eq!(
            crate::torrent::declared_tracker_tiers(&meta),
            vec![vec!["http://a/announce".to_string()]]
        );
        append_extra_tracker_tier(&mut meta, vec!["udp://x:1".to_string()]);
        assert_eq!(meta.announce_list.len(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn rerouting_a_paused_torrent_does_not_start_dht_lookups() {
        let tmp = tempfile::tempdir().unwrap();
        let session = Session::new_with_opts(
            tmp.path().to_path_buf(),
            SessionOptions {
                disable_dht: true,
                disable_local_service_discovery: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let meta = announce_only_meta("http://127.0.0.1:1/announce", Vec::new());
        let AddTorrentResponse::Added(_, handle) = session
            .add_from_meta(meta, AddTorrentOptions::default())
            .await
            .unwrap()
        else {
            panic!("expected Added");
        };
        session.pause(&handle).await.unwrap();
        let dht = super::super::dht::Dht::spawn().await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            session.reconfigure_p2p_proxy(None, Some(dht.clone())),
        )
        .await
        .expect("reconfigure did not finish")
        .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert_eq!(dht.active_lookup_count(), 0);
        dht.cancel_lookups();
        session.shutdown().await;
    }

    #[test]
    fn upnp_specs_map_tcp_and_actual_udp_ports() {
        assert_eq!(
            upnp_mapping_specs(41_000, Some(42_000), Some(43_000)),
            vec![
                (41_000, crate::upnp::MapProto::Tcp),
                (42_000, crate::upnp::MapProto::Udp),
                (43_000, crate::upnp::MapProto::Udp),
            ]
        );
        assert_eq!(
            upnp_mapping_specs(41_000, Some(42_000), Some(42_000)),
            vec![
                (41_000, crate::upnp::MapProto::Tcp),
                (42_000, crate::upnp::MapProto::Udp),
            ]
        );
        assert_eq!(
            upnp_mapping_specs(41_000, None, Some(43_000)),
            vec![
                (41_000, crate::upnp::MapProto::Tcp),
                (43_000, crate::upnp::MapProto::Udp),
            ]
        );
        assert_eq!(
            upnp_mapping_specs(41_000, None, None),
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

    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpStream, UdpSocket};

    use crate::utp::packet::{PacketType, UtpHeader};
    use crate::utp::slots::IpSlots;

    fn limits(max_total: usize, max_per_ip: usize, timeout: Duration) -> HandshakeLimits {
        HandshakeLimits {
            slots: IpSlots::new(max_total, max_per_ip),
            timeout,
        }
    }

    #[test]
    fn handshake_limits_group_ipv6_sources_by_their_prefix() {
        let limits = HandshakeLimits::new();
        let in_prefix = |i: usize| -> std::net::IpAddr {
            format!("2001:db8:7:9:{:x}::{:x}", i + 1, i + 1)
                .parse()
                .unwrap()
        };
        let held: Vec<_> = (0..MAX_INBOUND_HANDSHAKES_PER_IP)
            .map(|i| {
                limits
                    .slots
                    .try_acquire(in_prefix(i))
                    .expect("room in prefix")
            })
            .collect();
        assert!(limits
            .slots
            .try_acquire(in_prefix(MAX_INBOUND_HANDSHAKES_PER_IP))
            .is_none());
        assert!(limits
            .slots
            .try_acquire("2001:db8:7:a::1".parse().unwrap())
            .is_some());
        drop(held);
    }

    async fn quiet_session() -> (tempfile::TempDir, Arc<Session>) {
        let tmp = tempfile::tempdir().unwrap();
        let opts = SessionOptions {
            disable_dht: true,
            disable_local_service_discovery: true,
            ..Default::default()
        };
        let session = Session::new_with_opts(tmp.path().to_path_buf(), opts)
            .await
            .unwrap();
        (tmp, session)
    }

    async fn host_torrent(session: &Arc<Session>) -> Id20 {
        use crate::bencode::{encode_to_vec, Value};
        let info = Value::Dict(vec![
            (b"length".to_vec(), Value::Int(1024)),
            (b"name".to_vec(), Value::Bytes(b"inbound-test".to_vec())),
            (b"piece length".to_vec(), Value::Int(1024)),
            (b"pieces".to_vec(), Value::Bytes(vec![0u8; 20])),
        ]);
        let bytes = encode_to_vec(&Value::Dict(vec![(b"info".to_vec(), info)]));
        match session
            .add_torrent(
                AddTorrent::TorrentFileBytes(bytes.into()),
                Some(AddTorrentOptions::default()),
            )
            .await
            .unwrap()
        {
            AddTorrentResponse::Added(_, handle)
            | AddTorrentResponse::AlreadyManaged(_, handle) => handle.info_hash,
        }
    }

    async fn assert_closed_promptly(stream: &mut TcpStream) {
        let mut buf = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .expect("connection was kept open");
        assert!(matches!(read, Ok(0) | Err(_)));
    }

    #[tokio::test]
    async fn idle_inbound_connections_are_dropped_at_the_handshake_deadline() {
        let (_tmp, session) = quiet_session().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(run_accept_loop(
            listener,
            Arc::downgrade(&session),
            limits(8, 8, Duration::from_millis(300)),
        ));
        let mut idle = TcpStream::connect(addr).await.unwrap();
        assert_closed_promptly(&mut idle).await;
        accept.abort();
    }

    #[tokio::test]
    async fn one_address_cannot_hold_every_inbound_handshake_slot() {
        let (_tmp, session) = quiet_session().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(run_accept_loop(
            listener,
            Arc::downgrade(&session),
            limits(8, 2, Duration::from_secs(30)),
        ));
        let mut first = TcpStream::connect(addr).await.unwrap();
        let mut second = TcpStream::connect(addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut third = TcpStream::connect(addr).await.unwrap();
        assert_closed_promptly(&mut third).await;
        let mut buf = [0u8; 1];
        for held in [&mut first, &mut second] {
            assert!(
                tokio::time::timeout(Duration::from_millis(200), held.read(&mut buf))
                    .await
                    .is_err(),
                "handshakes within the share keep their slot"
            );
        }
        accept.abort();
    }

    fn raw_utp(packet_type: PacketType, id: u16, seq_nr: u16, ack_nr: u16) -> UtpHeader {
        UtpHeader {
            packet_type,
            connection_id: id,
            timestamp_micros: 0,
            timestamp_diff_micros: 0,
            wnd_size: 65536,
            seq_nr,
            ack_nr,
            selective_ack: None,
        }
    }

    async fn assert_utp_peer_is_reset(session: &Arc<Session>, limits: HandshakeLimits) {
        let utp = super::super::utp::UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let server = utp.local_addr();
        let accept = tokio::spawn(run_utp_accept_loop(utp, Arc::downgrade(session), limits));
        let raw = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        raw.send_to(&raw_utp(PacketType::Syn, 40, 1, 0).encode(&[]), server)
            .await
            .unwrap();
        let mut buf = [0u8; 2048];
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), raw.recv_from(&mut buf))
            .await
            .expect("no syn-ack")
            .unwrap();
        let (state, _) = UtpHeader::decode(&buf[..n]).unwrap();
        let data = raw_utp(PacketType::Data, 41, 2, state.seq_nr.wrapping_sub(1));
        raw.send_to(&data.encode(&[0x13; 20]), server)
            .await
            .unwrap();

        let deadline = tokio::time::Instant::now() + Duration::from_millis(2500);
        let mut seen = Vec::new();
        while let Ok(Ok((n, _))) = tokio::time::timeout_at(deadline, raw.recv_from(&mut buf)).await
        {
            seen.push(UtpHeader::decode(&buf[..n]).unwrap().0.packet_type);
        }
        accept.abort();
        assert_eq!(
            seen.iter().filter(|ty| **ty == PacketType::Reset).count(),
            1,
            "{seen:?}"
        );
        assert!(!seen.contains(&PacketType::Fin), "{seen:?}");
    }

    #[tokio::test]
    async fn inbound_utp_peers_are_reset_once_when_no_torrent_is_hosted() {
        let (_tmp, session) = quiet_session().await;
        assert_utp_peer_is_reset(&session, limits(8, 8, Duration::from_secs(10))).await;
    }

    #[tokio::test]
    async fn inbound_utp_peers_that_stall_the_handshake_are_reset_not_closed_gracefully() {
        let (_tmp, session) = quiet_session().await;
        host_torrent(&session).await;
        assert_utp_peer_is_reset(&session, limits(8, 8, Duration::from_millis(300))).await;
    }

    #[tokio::test]
    async fn blocklisted_inbound_utp_peers_are_reset_once() {
        let (_tmp, session) = quiet_session().await;
        host_torrent(&session).await;
        session.blocklist.write().ban("127.0.0.1".parse().unwrap());
        assert_utp_peer_is_reset(&session, limits(8, 8, Duration::from_secs(10))).await;
    }

    #[tokio::test]
    async fn the_session_utp_gate_refuses_syns_from_blocklisted_sources() {
        let (_tmp, session) = quiet_session().await;
        let port = session
            .utp_socket()
            .expect("utp socket")
            .local_addr()
            .port();
        let server = SocketAddr::from(([127, 0, 0, 1], port));
        let reply_to_syn = |conn_id: u16, wait: Duration| async move {
            let raw = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            raw.send_to(&raw_utp(PacketType::Syn, conn_id, 1, 0).encode(&[]), server)
                .await
                .unwrap();
            let mut buf = [0u8; 2048];
            let (n, _) = tokio::time::timeout(wait, raw.recv_from(&mut buf))
                .await
                .ok()?
                .ok()?;
            Some(UtpHeader::decode(&buf[..n]).unwrap().0.packet_type)
        };
        session.blocklist.write().ban("127.0.0.1".parse().unwrap());
        assert_eq!(reply_to_syn(70, Duration::from_millis(300)).await, None);
        *session.blocklist.write() = BlockList::default();
        assert_eq!(
            reply_to_syn(80, Duration::from_secs(2)).await,
            Some(PacketType::State)
        );
    }

    #[tokio::test]
    async fn inbound_utp_peers_without_a_handshake_slot_are_reset_once() {
        let (_tmp, session) = quiet_session().await;
        host_torrent(&session).await;
        assert_utp_peer_is_reset(&session, limits(0, 0, Duration::from_secs(10))).await;
    }

    async fn ipv6_session() -> Option<(tempfile::TempDir, Arc<Session>)> {
        if !ipv6_loopback_available() {
            eprintln!("skipping: no IPv6 loopback on this host");
            return None;
        }
        let tmp = tempfile::tempdir().unwrap();
        let opts = SessionOptions {
            disable_dht: true,
            disable_local_service_discovery: true,
            listen: Some(ListenerOptions {
                listen_ipv6: true,
                ..Default::default()
            }),
            ..Default::default()
        };
        let session = Session::new_with_opts(tmp.path().to_path_buf(), opts)
            .await
            .unwrap();
        Some((tmp, session))
    }

    #[tokio::test]
    async fn utp_stays_ipv4_only_unless_listen_ipv6_is_set() {
        let (_tmp, session) = quiet_session().await;
        assert!(session.utp_socket().unwrap().v6_socket().is_none());
        assert!(session.utp6_accept_handle.lock().is_none());

        let tmp = tempfile::tempdir().unwrap();
        let session = Session::new_with_opts(
            tmp.path().to_path_buf(),
            SessionOptions {
                disable_dht: true,
                disable_local_service_discovery: true,
                listen: Some(ListenerOptions::default()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert!(session.utp_socket().unwrap().v6_socket().is_none());
        assert!(session.utp6_accept_handle.lock().is_none());
        let loopback: SocketAddr = "[::1]:9".parse().unwrap();
        let err = session
            .utp_socket()
            .unwrap()
            .connect_timeout(loopback, Duration::from_secs(2))
            .await
            .expect_err("a v4-only session must not dial v6 peers");
        assert_eq!(err.kind(), std::io::ErrorKind::AddrNotAvailable);
    }

    #[tokio::test]
    async fn listen_ipv6_adds_a_v6only_utp_socket_next_to_the_v4_one() {
        let Some((_tmp, session)) = ipv6_session().await else {
            return;
        };
        let utp = session.utp_socket().unwrap();
        let v6 = utp.v6_socket().expect("no v6 uTP socket");
        assert!(utp.local_addr().is_ipv4());
        assert!(v6.local_addr().is_ipv6());
        assert_ne!(v6.local_addr().port(), 0);
        assert!(session.utp6_accept_handle.lock().is_some());
        assert_eq!(
            utp.v6_local_addr().map(|addr| addr.port()),
            Some(v6.local_addr().port())
        );
    }

    fn free_udp_port() -> u16 {
        std::net::UdpSocket::bind("[::1]:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[tokio::test]
    async fn v6_utp_binding_prefers_the_listen_port_then_the_v4_port_then_ephemeral() {
        if !ipv6_loopback_available() {
            eprintln!("skipping: no IPv6 loopback on this host");
            return;
        }
        let held = bind_utp_v6(0, None, None).await.expect("v6 bind");
        let taken = held.local_addr().port();
        let held_too = bind_utp_v6(0, None, None).await.expect("v6 bind");
        let also_taken = held_too.local_addr().port();
        assert_ne!(taken, also_taken);

        let mut landed_on_listen_port = false;
        for _ in 0..8 {
            let (listen, v4) = (free_udp_port(), free_udp_port());
            let socket = bind_utp_v6(listen, Some(v4), None).await.expect("v6 bind");
            if socket.local_addr().port() == listen {
                landed_on_listen_port = true;
                break;
            }
        }
        assert!(landed_on_listen_port, "a free listen port was never used");

        let mut landed_on_v4_port = false;
        for _ in 0..8 {
            let v4 = free_udp_port();
            let socket = bind_utp_v6(taken, Some(v4), None).await.expect("v6 bind");
            if socket.local_addr().port() == v4 {
                landed_on_v4_port = true;
                break;
            }
        }
        assert!(landed_on_v4_port, "a free v4 port was never used");

        let fallback = bind_utp_v6(taken, Some(also_taken), None)
            .await
            .expect("ephemeral fallback");
        let port = fallback.local_addr().port();
        assert!(port != taken && port != also_taken);
        let fallback = bind_utp_v6(taken, None, None)
            .await
            .expect("ephemeral fallback");
        assert_ne!(fallback.local_addr().port(), taken);
    }

    #[tokio::test]
    async fn v6_and_v4_utp_sockets_share_a_port_without_colliding() {
        let Some((_tmp, session)) = ipv6_session().await else {
            return;
        };
        let utp = session.utp_socket().unwrap();
        let v6 = utp.v6_socket().unwrap();
        if utp.local_addr().port() != v6.local_addr().port() {
            eprintln!("skipping: the v6 port was already taken on this host");
            return;
        }
        let v4_client = super::super::utp::UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let v6_client = super::super::utp::UtpSocket::bind("[::1]:0".parse().unwrap())
            .await
            .unwrap();
        let port = utp.local_addr().port();
        tokio::time::timeout(Duration::from_secs(10), async {
            v4_client
                .connect(SocketAddr::from(([127, 0, 0, 1], port)))
                .await
                .expect("v4 dial");
            v6_client
                .connect(SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, port)))
                .await
                .expect("v6 dial");
        })
        .await
        .expect("dials timed out");
    }

    #[tokio::test]
    async fn an_inbound_utp_peer_completes_the_handshake_over_ipv6_through_the_accept_loop() {
        let Some((_tmp, session)) = ipv6_session().await else {
            return;
        };
        let info_hash = host_torrent(&session).await;
        let port = session
            .utp_socket()
            .unwrap()
            .v6_local_addr()
            .unwrap()
            .port();
        let client = super::super::utp::UtpSocket::bind("[::1]:0".parse().unwrap())
            .await
            .unwrap();
        let mut stream = tokio::time::timeout(
            Duration::from_secs(5),
            client.connect(SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, port))),
        )
        .await
        .expect("v6 connect timed out")
        .unwrap();
        let hello = crate::wire::Handshake::new_with_features(
            info_hash,
            Id20([5u8; 20]),
            false,
            true,
            true,
        );
        stream.write_all(&hello.to_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        let mut reply = [0u8; crate::wire::HANDSHAKE_LEN];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut reply))
            .await
            .expect("no handshake reply")
            .unwrap();
        assert_eq!(&reply[28..48], info_hash.as_bytes());
    }

    #[tokio::test]
    async fn outbound_utp_dials_to_v6_peers_leave_through_the_v6_socket() {
        let Some((_tmp, session)) = ipv6_session().await else {
            return;
        };
        let peer = super::super::utp::UtpSocket::bind("[::1]:0".parse().unwrap())
            .await
            .unwrap();
        let peer_addr = peer.local_addr();
        let utp = session.utp_socket().unwrap();
        let accept = tokio::spawn(async move { peer.accept().await });
        let mut dialed = tokio::time::timeout(Duration::from_secs(5), utp.connect(peer_addr))
            .await
            .expect("v6 dial timed out")
            .expect("v6 dial failed");
        dialed.write_all(b"hi").await.unwrap();
        dialed.flush().await.unwrap();
        let inbound = tokio::time::timeout(Duration::from_secs(5), accept)
            .await
            .expect("peer saw no connection")
            .unwrap()
            .unwrap();
        assert_eq!(
            inbound.peer_addr().port(),
            utp.v6_local_addr().unwrap().port()
        );
        assert!(inbound.peer_addr().is_ipv6());
        assert_eq!(dialed.peer_addr(), peer_addr);
    }

    #[tokio::test]
    async fn an_inbound_utp_peer_completes_the_handshake_through_the_accept_loop() {
        let (_tmp, session) = quiet_session().await;
        let info_hash = host_torrent(&session).await;
        let port = session.utp_socket().unwrap().local_addr().port();
        let client = super::super::utp::UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let mut stream = client
            .connect(SocketAddr::from(([127, 0, 0, 1], port)))
            .await
            .unwrap();
        let hello = crate::wire::Handshake::new_with_features(
            info_hash,
            Id20([5u8; 20]),
            false,
            true,
            true,
        );
        stream.write_all(&hello.to_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        let mut reply = [0u8; crate::wire::HANDSHAKE_LEN];
        tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut reply))
            .await
            .expect("no handshake reply")
            .unwrap();
        assert_eq!(&reply[28..48], info_hash.as_bytes());
    }
}
