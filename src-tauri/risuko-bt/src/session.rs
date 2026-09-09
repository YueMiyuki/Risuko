//! Session: orchestrates all torrents, the TCP listener, and persistence

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use parking_lot::{Mutex, RwLock as ParkingRwLock};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex as TokioMutex, RwLock};

use super::api::TorrentIdOrHash;
use super::blocklist::{BlockList, BlocklistApplyResult};
use super::core::metainfo::{parse_torrent, FileDetails};
use super::core::{generate_peer_id, Id20, Lengths};
use super::peer::{KnownInfoHash, PeerCommand, PeerEvent, PeerHandle};
use super::torrent::{spawn as spawn_torrent, ManagedTorrent, TorrentCommand, TorrentInit};

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
    pub upload_rate_limit: Option<u64>,
    pub disable_local_service_discovery: bool,
    pub encryption: super::peer::EncryptionPolicy,
    pub p2p_proxy: Option<risuko_http::ProxyConnector>,
}

#[derive(Clone, Debug)]
pub struct AddTorrentOptions {
    pub output_folder: Option<String>,
    pub trackers: Option<Vec<String>>,
    pub only_files: Option<Vec<usize>>,
    pub list_only: bool,
    pub create_subfolder: bool,
    pub initial_peers: Vec<std::net::SocketAddr>,
    pub initial_tracker_peers: Vec<std::net::SocketAddr>,
    pub p2p_proxy: Option<risuko_http::ProxyConnector>,
    pub p2p_proxy_is_task_override: bool,
}

impl Default for AddTorrentOptions {
    fn default() -> Self {
        Self {
            output_folder: None,
            trackers: None,
            only_files: None,
            list_only: false,
            create_subfolder: true,
            initial_peers: Vec::new(),
            initial_tracker_peers: Vec::new(),
            p2p_proxy: None,
            p2p_proxy_is_task_override: false,
        }
    }
}

pub enum AddTorrent {
    TorrentFileBytes(Bytes),
    Url(String),
}

pub enum AddTorrentResponse {
    Added(usize, Arc<ManagedTorrent>),
    AlreadyManaged(usize, Arc<ManagedTorrent>),
    ListOnly(ListOnlyResponse),
}

pub struct ListOnlyResponse {
    pub info: super::core::ValidatedTorrentMetaV1Info,
    pub files: Vec<FileDetails>,
}

pub struct Session {
    output_dir: PathBuf,
    opts: SessionOptions,
    p2p_proxy: RwLock<Option<risuko_http::ProxyConnector>>,
    peer_id: Id20,
    listen_port: u16,
    tracker_source_addr: Option<SocketAddr>,
    utp: Option<Arc<super::utp::UtpSocket>>,
    upload_limiter: Option<Arc<super::limiter::UploadLimiter>>,
    inner: Mutex<SessionInner>,
    accept_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    accept6_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    utp_accept_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    upnp_handle: Mutex<Option<super::upnp::UpnpHandle>>,
    lsd: Mutex<Option<Arc<super::lsd::LocalServiceDiscovery>>>,
    lsd_router_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    dht: Mutex<Option<Arc<super::dht::Dht>>>,
    dht_bootstrap_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    blocklist: Arc<ParkingRwLock<BlockList>>,
    blocklist_apply: TokioMutex<()>,
}

impl Drop for Session {
    fn drop(&mut self) {
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
        if let Some(h) = self.dht_bootstrap_handle.lock().take() {
            h.abort();
        }
        if let Some(utp) = self.utp.take() {
            utp.shutdown();
        }
        // Dropping the UpnpHandle / LSD service / DHT triggers their cleanup
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

        let upload_limiter = opts
            .upload_rate_limit
            .map(|r| Arc::new(super::limiter::UploadLimiter::new(r)));

        let initial_p2p_proxy = opts.p2p_proxy.clone();
        let session = Arc::new(Self {
            output_dir,
            opts,
            p2p_proxy: RwLock::new(initial_p2p_proxy),
            peer_id,
            listen_port: local_port,
            tracker_source_addr,
            utp,
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
            dht_bootstrap_handle: Mutex::new(None),
            blocklist: Arc::new(ParkingRwLock::new(BlockList::default())),
            blocklist_apply: TokioMutex::new(()),
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
                            // BEP-27
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
            // Reuse the process-wide warm DHT (also used by magnet resolution and each torrent's get_peers poller) rather than a session-local one; shared() spawns + bootstraps a single long-lived instance on first use
            match super::dht::Dht::shared_with_proxy(session.opts.p2p_proxy.clone()).await {
                Some(dht) => *session.dht.lock() = Some(dht),
                None => tracing::warn!("dht: not started"),
            }
        }

        // Hold a Weak reference inside the accept loop so it doesn't keep the session alive; when the last external Arc is dropped the session's Drop aborts the spawned task via `accept_handle`
        let weak = Arc::downgrade(&session);
        let accept_handle = tokio::spawn(run_accept_loop(listener, weak));
        *session.accept_handle.lock() = Some(accept_handle);

        // Inbound µTP (BEP-29) accept loop, only spawned when µTP bound; drains the socket's accept queue so inbound SYNs become real peers instead of leaking queued streams + driver tasks, giving inbound µTP connectivity on par with the TCP listener
        if let Some(utp) = session.utp.clone() {
            let weak_utp = Arc::downgrade(&session);
            let h = tokio::spawn(run_utp_accept_loop(utp, weak_utp));
            *session.utp_accept_handle.lock() = Some(h);
        }

        // Optional v6 listener on the same port; failure to bind is not fatal, so log and continue with v4 only
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
        handle: PeerHandle,
        mut event_rx: mpsc::Receiver<PeerEvent>,
    ) {
        // Consuming the Handshook event here is fine: adopting a peer does not depend on it being re-delivered to the torrent loop
        let Some(first) = event_rx.recv().await else {
            return;
        };
        let (info_hash, reserved, peer_id) = match &first {
            PeerEvent::Handshook {
                info_hash,
                reserved,
                peer_id,
                ..
            } => (*info_hash, *reserved, *peer_id),
            _ => return,
        };
        if self.blocklist.read().contains(addr.ip()) {
            handle.io_abort.abort();
            let _ = handle.tx.try_send(PeerCommand::Disconnect);
            return;
        }
        let target = {
            let inner = self.inner.lock();
            inner
                .by_hash
                .get(&info_hash)
                .and_then(|id| inner.torrents.get(id).cloned())
        };
        let Some(t) = target else {
            // Peer handshook for a torrent that is no longer managed, close
            handle.io_abort.abort();
            let _ = handle.tx.try_send(PeerCommand::Disconnect);
            return;
        };
        let _ = t
            .cmd_tx()
            .send(TorrentCommand::AddInboundPeer {
                addr,
                cmd_tx: handle.tx,
                event_rx,
                reserved,
                peer_id,
                io_abort: handle.io_abort,
            })
            .await;
    }

    pub fn listen_port(&self) -> u16 {
        self.listen_port
    }

    pub fn utp_socket(&self) -> Option<Arc<super::utp::UtpSocket>> {
        self.utp.clone()
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

    /// True if Local Service Discovery is currently spawned and running
    pub fn lsd_active(&self) -> bool {
        self.lsd.lock().is_some()
    }

    /// True if a DHT node is currently running for this session
    pub fn dht_active(&self) -> bool {
        self.dht.lock().is_some()
    }

    /// Number of nodes currently held in the DHT routing table; returns 0 when the DHT is disabled or has not yet learned any contacts
    pub fn dht_routing_table_len(&self) -> usize {
        self.dht
            .lock()
            .as_ref()
            .map(|d| d.routing_table_len())
            .unwrap_or(0)
    }

    /// Snapshot of UPnP forwarder state: whether it was enabled at startup and the count of currently confirmed router-side mappings (0 if not enabled or no router responded yet)
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
        match which {
            AddTorrent::TorrentFileBytes(bytes) => {
                let meta = parse_torrent(&bytes).map_err(|e| format!("parse torrent: {e}"))?;
                self.add_from_meta(meta, opts).await
            }
            AddTorrent::Url(url) => {
                let extra_trackers = opts.trackers.clone().unwrap_or_default();
                let profile_proxy = self.p2p_proxy.read().await.clone();
                let route_proxy = if opts.p2p_proxy_is_task_override {
                    opts.p2p_proxy.clone()
                } else {
                    opts.p2p_proxy.clone().or(profile_proxy)
                };
                let route_utp = self
                    .utp_for_route(route_proxy.clone(), opts.p2p_proxy_is_task_override)
                    .await;
                let resolved = super::magnet::resolve_with_port_and_utp_and_proxy(
                    &url,
                    &extra_trackers,
                    self.listen_port,
                    std::time::Duration::from_secs(120),
                    self.opts.encryption,
                    route_utp,
                    route_proxy,
                )
                .await?;
                let torrent_bytes = super::magnet::synth_torrent_bytes(
                    &resolved.info_bytes,
                    &resolved.trackers,
                    &resolved.piece_layers,
                );
                let meta = parse_torrent(&torrent_bytes)
                    .map_err(|e| format!("parse synthesized torrent: {e}"))?;
                let mut opts = opts;
                let tracker_peers = resolved.tracker_peers;
                opts.initial_tracker_peers
                    .extend(tracker_peers.iter().copied());
                if !meta.info.private {
                    // Public torrents may use all resolver output
                    let tracker_set: std::collections::HashSet<_> =
                        tracker_peers.iter().copied().collect();
                    opts.initial_peers.extend(
                        resolved
                            .peers
                            .into_iter()
                            .filter(|peer| !tracker_set.contains(peer)),
                    );
                }
                self.add_from_meta(meta, opts).await
            }
        }
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
        if opts.list_only {
            let files: Vec<FileDetails> = info.iter_file_details().collect();
            return Ok(AddTorrentResponse::ListOnly(ListOnlyResponse {
                info,
                files,
            }));
        }

        if let Some((id, t)) = self.existing_live_torrent(meta.info_hash).await? {
            return Ok(AddTorrentResponse::AlreadyManaged(id, t));
        }
        {
            let inner = self.inner.lock();
            if let Some(&id) = inner.by_hash.get(&meta.info_hash) {
                if inner.torrents.contains_key(&id) {
                    // Lost the race to another add that finished spawning after existing_live_torrent ran; reuse that handle rather than allocating a duplicate id
                    if let Some(t) = inner.torrents.get(&id).cloned() {
                        return Ok(AddTorrentResponse::AlreadyManaged(id, t));
                    }
                }
                // by_hash is claimed but torrents has no entry yet: another add_from_meta for this info-hash reserved the id and is still spawning; bail out instead of racing past to allocate a duplicate id (which would spawn a second torrent and break the first task's rollback guard)
                return Err("torrent for this info-hash is already being added".to_string());
            }
        }

        let root_dir = opts
            .output_folder
            .map(PathBuf::from)
            .unwrap_or_else(|| self.output_dir.clone());
        // For multi-file torrents, files are laid out under <output>/<name>/ when `create_subfolder` is set (default), else directly under <output>/; single-file torrents always store the file directly under <output>/
        let create_subfolder = opts.create_subfolder;
        let root_dir = if info.single_file_mode || !create_subfolder {
            root_dir
        } else {
            root_dir.join(&info.name)
        };

        let lengths = Lengths::new(info.total_length(), info.piece_length)
            .map_err(|e| format!("bad lengths: {e}"))?;
        let mut meta = meta;
        if let Some(extra) = opts.trackers {
            let mut expanded = Vec::new();
            for raw in extra {
                for part in raw
                    .split([',', '\n', '\r'])
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    if !expanded.iter().any(|x: &String| x == part) {
                        expanded.push(part.to_string());
                    }
                }
            }
            if !expanded.is_empty() {
                meta.announce_list.push(expanded);
            }
        }
        // Reserve an id and claim the info-hash atomically so a concurrent add_from_meta cannot race past the duplicate check above; if spawn fails we roll the reservation back
        let id = {
            let mut inner = self.inner.lock();
            if let Some(&existing) = inner.by_hash.get(&meta.info_hash) {
                if let Some(t) = inner.torrents.get(&existing).cloned() {
                    return Ok(AddTorrentResponse::AlreadyManaged(existing, t));
                }
                // Reserved by a concurrent add that is still spawning; don't allocate a duplicate id / overwrite the existing reservation
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
        // Reserved-bit advertisement is set only for *pure-v2* torrents (no v1 hash); hybrid torrents connect via the v1 info-hash and we intentionally don't assert the BEP-52 v2 bit there since empirically some swarms (notably CN Thunder/Xunlei clients) close the connection right after the BT handshake when v2 is asserted on a v1 info_hash
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
            encryption: self.opts.encryption,
            advertise_v2,
            verifier,
            create_subfolder,
            utp: self
                .utp_for_route(route_proxy.clone(), opts.p2p_proxy_is_task_override)
                .await,
            upload_limiter: self.upload_limiter.clone(),
            dht: if info.private {
                None
            } else {
                self.dht.lock().clone()
            },
            p2p_proxy: route_proxy,
            p2p_proxy_is_task_override: opts.p2p_proxy_is_task_override,
            tracker_source_addr: self.tracker_source_addr,
            blocklist: self.blocklist.clone(),
        };
        let handle = match spawn_torrent(id, init, self.peer_id, self.listen_port).await {
            Ok(h) => h,
            Err(e) => {
                // Roll back the reservation so a retry can succeed
                let mut inner = self.inner.lock();
                if inner.by_hash.get(&meta.info_hash) == Some(&id) {
                    inner.by_hash.remove(&meta.info_hash);
                }
                return Err(format!("spawn torrent: {e}"));
            }
        };
        {
            let mut inner = self.inner.lock();
            inner.torrents.insert(id, handle.clone());
        }
        if info.private {
            if let Some(dht) = self.dht.lock().clone() {
                for hash in meta.announce_infohashes() {
                    dht.set_private(hash, true);
                }
            }
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
                lsd.add_infohash(meta.info_hash);
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
        if let Some(next_dht) = dht.as_ref() {
            for handle in &handles {
                if let Some(meta) = handle.metadata.load().as_ref() {
                    if meta.info.private {
                        for hash in meta.announce_infohashes() {
                            next_dht.set_private(hash, true);
                        }
                    }
                }
            }
        }

        let mut changed = Vec::with_capacity(handles.len());
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
            changed.push(Arc::clone(handle));
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
                .cloned(),
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
        let handle = self.get(which).ok_or_else(|| "not found".to_string())?;
        // Capture file paths for optional deletion before stopping the torrent (the torrent loop owns storage and drops it on Stop)
        let file_paths: Option<Vec<(PathBuf, bool)>> = if with_files {
            let create_subfolder = handle.create_subfolder;
            // Use the torrent's resolved root_dir (which honors a per-torrent `opts.output_folder` override) rather than the session-wide `output_dir`; for grouped multi-file layouts this root already points at `<output>/<name>/`, for flat / single-file at the parent directory
            let root = handle.root_dir.clone();
            handle
                .with_metadata(|meta| {
                    let name_empty = meta.info.name.is_empty();
                    if meta.info.single_file_mode && !name_empty {
                        // Single-file: file lives directly under root
                        vec![(root.join(&meta.info.name), false)]
                    } else if !meta.info.single_file_mode && create_subfolder && !name_empty {
                        // Multi-file grouped: root_dir IS the torrent folder, safe to remove wholesale
                        vec![(root, true)]
                    } else {
                        // Flat layout, or any case with an empty torrent name (defensive): enumerate per-file paths under root so we never `remove_dir_all` the parent
                        meta.info
                            .iter_file_details()
                            .filter_map(|f| {
                                let mut p = root.clone();
                                // Defense-in-depth: metainfo parsing already rejects unsafe path components, but never join a `..`/`.`/empty/root component here so an upstream regression can't turn deletion into an arbitrary-path removal
                                for c in f.filename.split('/') {
                                    if c.is_empty() || c == "." || c == ".." {
                                        return None;
                                    }
                                    p.push(c);
                                }
                                Some((p, false))
                            })
                            .collect()
                    }
                })
                .ok()
        } else {
            None
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .cmd_tx()
            .send(TorrentCommand::Stop(tx))
            .await
            .map_err(|e| e.to_string())?;
        let _ = rx.await;
        {
            let mut inner = self.inner.lock();
            inner.torrents.remove(&handle.id);
            inner.by_hash.remove(&handle.info_hash);
        }
        let is_private = handle
            .metadata
            .load()
            .as_ref()
            .is_some_and(|meta| meta.info.private);
        if is_private {
            if let Some(dht) = self.dht.lock().clone() {
                if let Some(meta) = handle.metadata.load().as_ref() {
                    for hash in meta.announce_infohashes() {
                        dht.set_private(hash, false);
                    }
                }
            }
        }
        if !is_private {
            if let Some(lsd) = self.lsd.lock().as_ref() {
                lsd.remove_infohash(handle.info_hash);
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
            }
        }
        Ok(())
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
        let mut meta = {
            let mut list = self.blocklist.write();
            list.replace(&entries)
        };
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

/// Bind a TCP listener on an IPv6 address with `IPV6_V6ONLY` set to avoid dual-stack conflicts when an IPv4 listener already bound the same port
fn bind_v6_listener(addr: SocketAddr) -> std::io::Result<std::net::TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    let sock = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
    sock.set_only_v6(true)?;
    // On Unix, SO_REUSEADDR allows rebinding a port in TIME_WAIT (desirable); on Windows it has different semantics (permits multiple processes to bind the same port, enabling hijacking), so skip it there
    #[cfg(not(target_os = "windows"))]
    sock.set_reuse_address(true)?;
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())?;
    sock.listen(128)?;
    Ok(sock.into())
}

/// Snapshot of every managed torrent's info-hash + v2 flag + ext-handshake builder, handed to the connection layer as the inbound allow-list
fn known_infohashes(s: &Session) -> Vec<KnownInfoHash> {
    s.inner
        .lock()
        .torrents
        .values()
        .map(|handle| KnownInfoHash {
            info_hash: handle.info_hash,
            advertise_v2: handle.advertise_v2.load(Ordering::Relaxed),
            advertise_dht: handle
                .metadata
                .load()
                .as_ref()
                .is_none_or(|meta| !meta.info.private),
            ext_handshake_builder: Some(handle.ext_handshake_builder.clone()),
        })
        .collect()
}

/// Shared inbound accept loop, parameterised on a `Weak<Session>` so the task does not keep the session alive; the session's Drop aborts it
async fn run_accept_loop(listener: TcpListener, weak: std::sync::Weak<Session>) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                let _ = stream.set_nodelay(true);
                let Some(s) = weak.upgrade() else {
                    return;
                };
                if s.blocklist.read().contains(addr.ip()) {
                    continue;
                }
                tokio::spawn(async move {
                    let allowed = known_infohashes(&s);
                    let policy = s.opts.encryption;
                    let res = super::peer::accept(
                        stream,
                        s.peer_id,
                        allowed,
                        std::time::Duration::from_secs(30),
                        policy,
                    )
                    .await;
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
                tracing::debug!("accept failed: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
}

/// Inbound µTP (BEP-29) accept loop; mirrors [`run_accept_loop`] but over the shared µTP endpoint where each accepted connection runs the plaintext BT responder handshake (µTP carries no MSE layer) and is routed to its torrent by info-hash; parameterised on a `Weak<Session>` so it doesn't keep the session alive and the session's Drop aborts it via `utp_accept_handle`
async fn run_utp_accept_loop(utp: Arc<super::utp::UtpSocket>, weak: std::sync::Weak<Session>) {
    loop {
        let stream = match utp.accept().await {
            Ok(s) => s,
            // The endpoint closed (last Arc dropped); nothing more to accept
            Err(_) => return,
        };
        let Some(s) = weak.upgrade() else {
            return;
        };
        let addr = stream.peer_addr();
        if s.blocklist.read().contains(addr.ip()) {
            continue;
        }
        tokio::spawn(async move {
            let allowed = known_infohashes(&s);
            // No managed torrents—nothing this peer could be after, so drop the stream (its driver tears the connection down)
            if allowed.is_empty() {
                return;
            }
            let res = super::peer::accept_utp_plaintext(
                stream,
                s.peer_id,
                allowed,
                std::time::Duration::from_secs(30),
            )
            .await;
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
