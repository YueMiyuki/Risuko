use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures_util::FutureExt;
use risuko_bt as bt;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::future::Future;
use std::net::{Ipv4Addr, SocketAddr};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{watch, Mutex};
use tokio_util::sync::CancellationToken;

use super::options::TASK_P2P_PROXY_OVERRIDE_KEY;

/// BitTorrent session tuning passed from the user/system config; all fields are optional, missing entries fall back to `risuko-bt` defaults
#[derive(Clone, Debug, Default)]
pub struct BtTuning {
    pub max_outstanding_per_peer: Option<usize>,
    pub max_peers_per_torrent: Option<usize>,
    pub upload_rate_limit: Option<u64>,
    pub enable_upnp: Option<bool>,
    pub upnp_lease: Option<Duration>,
    /// Accepts "plaintext", "prefer", or "require". Anything else is ignored
    pub encryption_policy: Option<String>,
    pub listen_ipv6: Option<bool>,
    pub enable_lsd: Option<bool>,
    /// Process-wide P2P route for shared DHT and torrent-owned connections.
    pub p2p_proxy: Option<risuko_http::ProxyConnector>,
}

/// Read-only BT session diagnostics used by the `/health` panel
pub struct BtHealthSnapshot {
    pub listen_port: u16,
    pub lsd_active: bool,
    pub upnp_enabled: bool,
    pub upnp_mappings: usize,
    pub upnp_attempts: usize,
    pub torrents: usize,
    pub dht_active: bool,
    pub dht_nodes: usize,
}

#[derive(Debug)]
struct ResolvedMagnetMeta {
    bytes: Arc<[u8]>,
    peers: Arc<[SocketAddr]>,
    tracker_peers: Arc<[SocketAddr]>,
}

struct CachedMagnetMeta {
    meta: Arc<ResolvedMagnetMeta>,
    expires_at: Instant,
}

type SharedMagnetResult = Result<Arc<ResolvedMagnetMeta>, Arc<str>>;

const MAGNET_ROUTE_CHANGED: &str = "P2P proxy profile changed; magnet resolution cancelled";

struct InFlightMagnet {
    generation: u64,
    sender: watch::Sender<Option<SharedMagnetResult>>,
}

struct MagnetMetaCacheState {
    completed: HashMap<[u8; 20], CachedMagnetMeta>,
    in_flight: HashMap<[u8; 20], InFlightMagnet>,
    generation: u64,
    cancellation: CancellationToken,
}

impl Default for MagnetMetaCacheState {
    fn default() -> Self {
        Self {
            completed: HashMap::new(),
            in_flight: HashMap::new(),
            generation: 0,
            cancellation: CancellationToken::new(),
        }
    }
}

#[derive(Clone, Default)]
struct MagnetMetaCache {
    state: Arc<Mutex<MagnetMetaCacheState>>,
}

const MAGNET_META_CACHE_TTL: Duration = Duration::from_secs(120);

impl MagnetMetaCache {
    async fn get_or_resolve<F, Fut>(
        &self,
        key: [u8; 20],
        resolver: F,
    ) -> Result<Arc<ResolvedMagnetMeta>, String>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<ResolvedMagnetMeta, String>> + Send + 'static,
    {
        let receiver = {
            let mut state = self.state.lock().await;
            let now = Instant::now();
            state.completed.retain(|_, entry| entry.expires_at > now);

            if let Some(entry) = state.completed.get(&key) {
                tracing::info!(
                    "Magnet metadata cache hit ({} peers)",
                    entry.meta.peers.len()
                );
                return Ok(entry.meta.clone());
            }

            if let Some(in_flight) = state.in_flight.get(&key) {
                tracing::info!("Joining in-flight magnet metadata resolution");
                in_flight.sender.subscribe()
            } else {
                let (sender, receiver) = watch::channel(None);
                let generation = state.generation;
                let cancellation = state.cancellation.clone();
                state.in_flight.insert(
                    key,
                    InFlightMagnet {
                        generation,
                        sender: sender.clone(),
                    },
                );

                let cache = self.clone();
                tokio::spawn(async move {
                    let result = tokio::select! {
                        result = AssertUnwindSafe(async move { resolver().await }).catch_unwind() => {
                            match result {
                                Ok(result) => result.map(Arc::new).map_err(Arc::<str>::from),
                                Err(_) => {
                                    tracing::warn!("Magnet metadata resolver panicked");
                                    Err(Arc::<str>::from("Magnet metadata resolver panicked"))
                                }
                            }
                        }
                        _ = cancellation.cancelled() => {
                            Err(Arc::<str>::from(MAGNET_ROUTE_CHANGED))
                        }
                    };

                    let result = {
                        let mut state = cache.state.lock().await;
                        let current_generation = state.generation == generation;
                        if !current_generation {
                            Err(Arc::<str>::from(MAGNET_ROUTE_CHANGED))
                        } else {
                            if let Ok(meta) = &result {
                                state.completed.insert(
                                    key,
                                    CachedMagnetMeta {
                                        meta: meta.clone(),
                                        expires_at: Instant::now() + MAGNET_META_CACHE_TTL,
                                    },
                                );
                            }
                            if state
                                .in_flight
                                .get(&key)
                                .is_some_and(|entry| entry.generation == generation)
                            {
                                state.in_flight.remove(&key);
                            }
                            result
                        }
                    };

                    let _ = sender.send(Some(result));
                });

                receiver
            }
        };

        Self::await_result(receiver).await
    }

    /// Return the current route generation and its cancellation token as one
    /// snapshot. Reload invalidates both while holding the same state lock,
    /// so callers can safely reject a resolver that was queued before a
    /// profile swap but only started afterward.
    async fn route_snapshot(&self) -> (u64, CancellationToken) {
        let state = self.state.lock().await;
        (state.generation, state.cancellation.clone())
    }

    async fn route_snapshot_for(
        &self,
        expected_generation: u64,
    ) -> Result<CancellationToken, String> {
        let state = self.state.lock().await;
        if state.generation != expected_generation {
            return Err(MAGNET_ROUTE_CHANGED.to_string());
        }
        Ok(state.cancellation.clone())
    }

    async fn invalidate(&self) {
        let senders = {
            let mut state = self.state.lock().await;
            state.generation = state.generation.wrapping_add(1);
            state.completed.clear();
            state.cancellation.cancel();
            state.cancellation = CancellationToken::new();
            state
                .in_flight
                .drain()
                .map(|(_, entry)| entry.sender)
                .collect::<Vec<_>>()
        };

        for sender in senders {
            let _ = sender.send(Some(Err(Arc::<str>::from(MAGNET_ROUTE_CHANGED))));
        }
    }

    async fn await_result(
        mut receiver: watch::Receiver<Option<SharedMagnetResult>>,
    ) -> Result<Arc<ResolvedMagnetMeta>, String> {
        loop {
            if let Some(result) = receiver.borrow().clone() {
                return result.map_err(|error| error.to_string());
            }
            receiver
                .changed()
                .await
                .map_err(|_| "Magnet metadata resolver stopped unexpectedly".to_string())?;
        }
    }
}

/// BitTorrent download management via the in-tree `risuko-bt` engine
#[derive(Clone)]
pub struct TorrentEngine {
    session: Option<Arc<bt::Session>>,
    output_dir: PathBuf,
    magnet_cache: MagnetMetaCache,
    p2p_route_lock: Arc<Mutex<()>>,
}

impl TorrentEngine {
    pub async fn new_with_tuning(output_dir: &Path, tuning: BtTuning) -> Result<Self, String> {
        std::fs::create_dir_all(output_dir)
            .map_err(|e| format!("Failed to create torrent output dir: {}", e))?;

        let encryption = encryption_policy_from_str(tuning.encryption_policy.as_deref());

        let session = bt::Session::new_with_opts(
            output_dir.to_path_buf(),
            bt::SessionOptions {
                listen: Some(bt::ListenerOptions {
                    listen_addr: Some((Ipv4Addr::UNSPECIFIED, 0).into()),
                    enable_upnp_port_forwarding: tuning.enable_upnp.unwrap_or(true),
                    upnp_lease: tuning.upnp_lease,
                    listen_ipv6: tuning.listen_ipv6.unwrap_or(false),
                }),
                max_outstanding_requests_per_peer: tuning.max_outstanding_per_peer,
                max_peers_per_torrent: tuning.max_peers_per_torrent,
                upload_rate_limit: tuning.upload_rate_limit,
                disable_local_service_discovery: !tuning.enable_lsd.unwrap_or(true),
                encryption,
                p2p_proxy: tuning.p2p_proxy.clone(),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| format!("Failed to create torrent session: {}", e))?;

        tracing::info!(
            "Torrent engine initialized, output_dir={}",
            output_dir.display()
        );

        Ok(Self {
            session: Some(session),
            output_dir: output_dir.to_path_buf(),
            magnet_cache: MagnetMetaCache::default(),
            p2p_route_lock: Arc::new(Mutex::new(())),
        })
    }

    fn get_session(&self) -> Result<&Arc<bt::Session>, String> {
        self.session
            .as_ref()
            .ok_or_else(|| "Torrent engine not initialized".to_string())
    }

    pub fn list_managed_torrents(&self) -> Vec<(usize, String)> {
        let Some(session) = self.session.as_ref() else {
            return Vec::new();
        };
        session.with_torrents(|iter| {
            iter.map(|(id, handle)| (id, handle.info_hash().to_hex()))
                .collect()
        })
    }

    /// Snapshot of BitTorrent session health for the `/health` panel; `None` when the torrent engine has been torn down
    pub fn health_snapshot(&self) -> Option<BtHealthSnapshot> {
        let session = self.session.as_ref()?;
        let upnp = session.upnp_status();
        Some(BtHealthSnapshot {
            listen_port: session.listen_port(),
            lsd_active: session.lsd_active(),
            upnp_enabled: upnp.enabled,
            upnp_mappings: upnp.mapping_count,
            upnp_attempts: upnp.discovery_attempts,
            torrents: session.with_torrents(|i| i.count()),
            dht_active: session.dht_active(),
            dht_nodes: session.dht_routing_table_len(),
        })
    }

    pub async fn set_peer_blocklist(
        &self,
        entries: Vec<String>,
    ) -> Result<bt::BlocklistApplyResult, String> {
        Ok(self.get_session()?.set_peer_blocklist(entries).await)
    }

    fn parse_select_files(options: &Map<String, Value>) -> Option<Vec<usize>> {
        let raw = options.get("select-file").and_then(|v| v.as_str())?.trim();
        if raw.is_empty() {
            return None;
        }
        let indices: Vec<usize> = raw
            .split(',')
            .filter_map(|s| {
                let s = s.trim();
                if s.is_empty() {
                    return None;
                }
                s.parse::<usize>()
                    .ok()
                    .and_then(|i| if i >= 1 { Some(i - 1) } else { None })
            })
            .collect();
        if indices.is_empty() {
            None
        } else {
            Some(indices)
        }
    }

    pub async fn add_torrent_bytes(
        &self,
        data: &[u8],
        options: &Map<String, Value>,
    ) -> Result<TorrentHandle, String> {
        self.add_torrent_bytes_with_peers(data, options, Vec::new())
            .await
    }

    pub async fn add_torrent_bytes_with_peers(
        &self,
        data: &[u8],
        options: &Map<String, Value>,
        initial_peers: Vec<std::net::SocketAddr>,
    ) -> Result<TorrentHandle, String> {
        self.add_torrent_bytes_with_peer_sources(data, options, initial_peers, Vec::new())
            .await
    }

    async fn add_torrent_bytes_with_peer_sources(
        &self,
        data: &[u8],
        options: &Map<String, Value>,
        initial_peers: Vec<std::net::SocketAddr>,
        initial_tracker_peers: Vec<std::net::SocketAddr>,
    ) -> Result<TorrentHandle, String> {
        let session = self.get_session()?;

        let dir = options
            .get("dir")
            .and_then(|v| v.as_str())
            .unwrap_or(self.output_dir.to_str().unwrap_or("."));

        let trackers = Self::parse_trackers(options);
        let task_p2p_proxy = p2p_proxy_from_options(options)?;
        let task_p2p_proxy_is_override = options
            .get(TASK_P2P_PROXY_OVERRIDE_KEY)
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let only_files = Self::parse_select_files(options);
        let create_subfolder = options
            .get("bt-create-subfolder")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        let add_opts = bt::AddTorrentOptions {
            output_folder: Some(dir.to_string()),
            trackers: if trackers.is_empty() {
                None
            } else {
                Some(trackers)
            },
            only_files,
            list_only: false,
            create_subfolder,
            initial_peers,
            initial_tracker_peers,
            p2p_proxy: task_p2p_proxy,
            p2p_proxy_is_task_override: task_p2p_proxy_is_override,
        };

        tracing::info!("Adding torrent bytes ({} bytes) to dir={}", data.len(), dir);

        let response = session
            .add_torrent(
                bt::AddTorrent::TorrentFileBytes(data.to_vec().into()),
                Some(add_opts),
            )
            .await
            .map_err(|e| format!("Failed to add torrent: {}", e))?;

        let handle = extract_handle(response)?;
        tracing::info!(
            "Torrent added: id={}, info_hash={:?}",
            handle.id,
            handle.info_hash
        );
        Ok(handle)
    }

    pub async fn resolve_and_add_magnet(
        &self,
        magnet_uri: &str,
        options: &Map<String, Value>,
        timeout_secs: u64,
    ) -> Result<TorrentHandle, String> {
        let (_, cancellation) = self.magnet_cache.route_snapshot().await;
        self.resolve_and_add_magnet_with_cancellation(
            magnet_uri,
            options,
            timeout_secs,
            cancellation,
        )
        .await
    }

    /// Resolve and add a magnet only when it belongs to the supplied route
    /// generation. The generation check and cancellation-token capture happen
    /// under the cache lock, which is also used by route invalidation.
    pub async fn resolve_and_add_magnet_at_generation(
        &self,
        magnet_uri: &str,
        options: &Map<String, Value>,
        timeout_secs: u64,
        expected_generation: u64,
    ) -> Result<TorrentHandle, String> {
        let cancellation = self
            .magnet_cache
            .route_snapshot_for(expected_generation)
            .await?;
        self.resolve_and_add_magnet_with_cancellation(
            magnet_uri,
            options,
            timeout_secs,
            cancellation,
        )
        .await
    }

    pub async fn magnet_route_generation(&self) -> u64 {
        self.magnet_cache.route_snapshot().await.0
    }

    async fn resolve_and_add_magnet_with_cancellation(
        &self,
        magnet_uri: &str,
        options: &Map<String, Value>,
        timeout_secs: u64,
        cancellation: CancellationToken,
    ) -> Result<TorrentHandle, String> {
        let (bytes, peers, tracker_peers) = self
            .resolve_magnet_bytes_with_cancellation(
                magnet_uri,
                options,
                timeout_secs,
                cancellation.clone(),
            )
            .await?;

        if cancellation.is_cancelled() {
            return Err(MAGNET_ROUTE_CHANGED.to_string());
        }

        save_torrent_metadata_if_enabled(&bytes, options, &self.output_dir).await;

        if cancellation.is_cancelled() {
            return Err(MAGNET_ROUTE_CHANGED.to_string());
        }

        tracing::info!(
            "Magnet resolved with {} discovered peers; seeding download",
            peers.len()
        );
        let _route_guard = self.p2p_route_lock.lock().await;
        if cancellation.is_cancelled() {
            return Err(MAGNET_ROUTE_CHANGED.to_string());
        }
        let meta = bt::parse_torrent(&bytes)
            .map_err(|e| format!("Failed to parse resolved metadata: {e}"))?;
        let tracker_set: std::collections::HashSet<_> =
            tracker_peers.iter().copied().collect();
        let initial_peers = if meta.info.private {
            Vec::new()
        } else {
            peers
                .into_iter()
                .filter(|peer| !tracker_set.contains(peer))
                .collect()
        };
        self.add_torrent_bytes_with_peer_sources(
            &bytes,
            options,
            initial_peers,
            tracker_peers,
        )
            .await
    }

    pub async fn resolve_magnet(
        &self,
        magnet_uri: &str,
        options: &Map<String, Value>,
        timeout_secs: u64,
    ) -> Result<Vec<TorrentFileInfo>, String> {
        let session = self.get_session()?;

        if let Ok(magnet) = bt::Magnet::parse(magnet_uri) {
            if let Some(handle) = session.get(bt::TorrentIdOrHash::Hash(magnet.info_hash())) {
                if let Ok(files) = handle.with_metadata(|meta| extract_file_details(&meta.info)) {
                    tracing::info!(
                        "Magnet already managed, resolved from session ({} files)",
                        files.len()
                    );
                    return Ok(files);
                }
            }
        }

        tracing::info!("Resolving magnet metadata: {}", magnet_uri);
        let start = std::time::Instant::now();
        let (_, cancellation) = self.magnet_cache.route_snapshot().await;
        let (torrent_bytes, _, _) = self
            .resolve_magnet_bytes_with_cancellation(
                magnet_uri,
                options,
                timeout_secs,
                cancellation.clone(),
            )
            .await?;
        if cancellation.is_cancelled() {
            return Err(MAGNET_ROUTE_CHANGED.to_string());
        }
        let meta = bt::parse_torrent(&torrent_bytes)
            .map_err(|e| format!("Failed to parse resolved metadata: {}", e))?;
        let files = extract_file_details(&meta.info);
        tracing::info!(
            "Magnet metadata resolved in {:?} ({} files)",
            start.elapsed(),
            files.len()
        );
        Ok(files)
    }

    async fn resolve_magnet_bytes_with_cancellation(
        &self,
        magnet_uri: &str,
        options: &Map<String, Value>,
        timeout_secs: u64,
        cancellation: CancellationToken,
    ) -> Result<(Vec<u8>, Vec<SocketAddr>, Vec<SocketAddr>), String> {
        let info_hash_key = bt::Magnet::parse(magnet_uri)
            .ok()
            .map(|m| *m.info_hash().as_bytes());
        let trackers = Self::parse_trackers(options);
        let enc = encryption_policy_from_str(
            options.get("bt-encryption-policy").and_then(|v| v.as_str()),
        );
        let session = self.get_session()?;
        let listen_port = session.listen_port();
        let task_proxy_override = options
            .get(TASK_P2P_PROXY_OVERRIDE_KEY)
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let p2p_proxy = p2p_proxy_from_options(options)?;
        let utp = if task_proxy_override {
            match bt::utp::UtpSocket::bind_with_proxy(
                SocketAddr::from(([0, 0, 0, 0], 0)),
                p2p_proxy.clone(),
            )
            .await
            {
                Ok(socket) => Some(socket),
                Err(error) => {
                    tracing::warn!("task µTP route unavailable during magnet resolve: {error}");
                    None
                }
            }
        } else {
            session.utp_socket()
        };
        let magnet_uri = magnet_uri.to_string();

        let resolve = move || async move {
            Self::resolve_magnet_uncached(
                magnet_uri,
                trackers,
                listen_port,
                timeout_secs,
                enc,
                utp,
                p2p_proxy,
            )
            .await
        };

        let resolved = if task_proxy_override {
            tokio::select! {
                result = resolve() => {
                    let result = result?;
                    if cancellation.is_cancelled() {
                        return Err(MAGNET_ROUTE_CHANGED.to_string());
                    }
                    Arc::new(result)
                },
                _ = cancellation.cancelled() => return Err(MAGNET_ROUTE_CHANGED.to_string()),
            }
        } else {
            match info_hash_key {
                Some(key) => self.magnet_cache.get_or_resolve(key, resolve).await?,
                None => {
                    tokio::select! {
                        result = resolve() => {
                            let result = result?;
                            if cancellation.is_cancelled() {
                                return Err(MAGNET_ROUTE_CHANGED.to_string());
                            }
                            Arc::new(result)
                        },
                        _ = cancellation.cancelled() => return Err(MAGNET_ROUTE_CHANGED.to_string()),
                    }
                }
            }
        };

        if cancellation.is_cancelled() {
            return Err(MAGNET_ROUTE_CHANGED.to_string());
        }

        Ok((
            resolved.bytes.to_vec(),
            resolved.peers.to_vec(),
            resolved.tracker_peers.to_vec(),
        ))
    }

    async fn resolve_magnet_uncached(
        magnet_uri: String,
        trackers: Vec<String>,
        listen_port: u16,
        timeout_secs: u64,
        enc: bt::EncryptionPolicy,
        utp: Option<Arc<bt::utp::UtpSocket>>,
        p2p_proxy: Option<risuko_http::ProxyConnector>,
    ) -> Result<ResolvedMagnetMeta, String> {
        let resolved = tokio::time::timeout(
            Duration::from_secs(timeout_secs),
            bt::magnet::resolve_with_port_and_utp_and_proxy(
                &magnet_uri,
                &trackers,
                listen_port,
                Duration::from_secs(timeout_secs),
                enc,
                utp,
                p2p_proxy,
            ),
        )
        .await
        .map_err(|_| "Timed out resolving magnet metadata".to_string())?
        .map_err(|e| format!("Failed to resolve magnet: {}", e))?;

        Ok(ResolvedMagnetMeta {
            bytes: Arc::from(
                bt::magnet::synth_torrent_bytes(
                    &resolved.info_bytes,
                    &resolved.trackers,
                    &resolved.piece_layers,
                )
                .into_boxed_slice(),
            ),
            peers: Arc::from(resolved.peers.into_boxed_slice()),
            tracker_peers: Arc::from(resolved.tracker_peers.into_boxed_slice()),
        })
    }

    fn parse_trackers(options: &Map<String, Value>) -> Vec<String> {
        let raw = options
            .get("bt-tracker")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for part in raw.split([',', '\n', '\r']) {
            let t = part.trim();
            if t.is_empty() {
                continue;
            }
            if seen.insert(t.to_string()) {
                out.push(t.to_string());
            }
        }
        out
    }

    pub fn get_torrent_stats(&self, torrent_id: usize) -> Option<TorrentStats> {
        let session = self.session.as_ref()?;
        let handle = session.get(bt::TorrentIdOrHash::Id(torrent_id))?;
        let stats = handle.stats();

        let (download_speed, upload_speed, num_peers, peers, num_seeders) = match &stats.live {
            Some(live) => {
                let count = live.snapshot.peer_stats.live;
                let dl = (live.download_speed.mbps * 1_048_576.0) as u64;
                let ul = (live.upload_speed.mbps * 1_048_576.0) as u64;
                let mapped: Vec<PeerSnapshot> = stats
                    .peers
                    .iter()
                    .map(|p| PeerSnapshot {
                        addr: p.addr,
                        bitfield: p.bitfield.clone(),
                        am_choking: p.am_choking,
                        am_interested: p.am_interested,
                        peer_choking: p.peer_choking,
                        peer_interested: p.peer_interested,
                        seeder: p.seeder,
                        peer_id: p.peer_id,
                        client: p.client.clone(),
                        downloaded: p.downloaded,
                        uploaded: p.uploaded,
                        dl_speed: p.dl_speed,
                        up_speed: p.up_speed,
                        incoming: p.incoming,
                        snubbed: p.snubbed,
                        progress: p.progress,
                        optimistic_unchoke: p.optimistic_unchoke,
                    })
                    .collect();
                let seeders = mapped.iter().filter(|p| p.seeder).count() as u32;
                (dl, ul, count, mapped, seeders)
            }
            None => (0, 0, 0, Vec::new(), 0),
        };

        let name = handle.name();

        let metadata_payload = handle.metadata.load();
        let metadata = metadata_payload.as_ref().map(|meta| {
            let total_pieces = (meta.info.pieces.len() / 20) as u32;
            TorrentMetadataInfo {
                piece_length: meta.info.piece_length,
                num_pieces: total_pieces,
                comment: meta.comment.clone(),
                creation_date: meta.creation_date,
                announce_list: build_announce_list(meta),
            }
        });
        let file_details = metadata_payload
            .as_ref()
            .map(|meta| extract_file_details(&meta.info));
        let single_file_mode = metadata_payload
            .as_ref()
            .map(|meta| meta.info.single_file_mode)
            .unwrap_or(false);

        Some(TorrentStats {
            total_bytes: stats.total_bytes,
            downloaded_bytes: stats.progress_bytes,
            uploaded_bytes: stats.uploaded_bytes,
            download_speed,
            upload_speed,
            num_peers,
            num_seeders,
            is_finished: stats.finished,
            name,
            file_progress: stats.file_progress,
            file_details,
            resolved_root: Some(handle.root_dir.to_string_lossy().into_owned()),
            single_file_mode,
            peers,
            metadata,
        })
    }

    pub async fn pause(&self, torrent_id: usize) -> Result<(), String> {
        let session = self.get_session()?;
        let handle = session
            .get(bt::TorrentIdOrHash::Id(torrent_id))
            .ok_or("Torrent not found")?;
        session
            .pause(&handle)
            .await
            .map_err(|e| format!("Failed to pause: {}", e))
    }

    pub async fn unpause(&self, torrent_id: usize) -> Result<(), String> {
        let session = self.get_session()?;
        let handle = session
            .get(bt::TorrentIdOrHash::Id(torrent_id))
            .ok_or("Torrent not found")?;
        session
            .unpause(&handle)
            .await
            .map_err(|e| format!("Failed to unpause: {}", e))
    }

    pub async fn add_trackers(
        &self,
        torrent_id: usize,
        urls: Vec<String>,
    ) -> Result<usize, String> {
        let session = self.get_session()?;
        let handle = session
            .get(bt::TorrentIdOrHash::Id(torrent_id))
            .ok_or("Torrent not found")?;
        session
            .add_trackers(handle.info_hash(), urls)
            .await
            .map_err(|e| format!("Failed to add trackers: {}", e))
    }

    pub async fn reconfigure_p2p_proxy(
        &self,
        proxy: Option<risuko_http::ProxyConnector>,
        dht: Option<Arc<bt::dht::Dht>>,
    ) -> Result<(), String> {
        let _route_guard = self.p2p_route_lock.lock().await;
        let session = self.get_session()?;
        session.reconfigure_p2p_proxy(proxy, dht).await
    }

    pub async fn invalidate_magnet_resolutions(&self) {
        let _route_guard = self.p2p_route_lock.lock().await;
        self.magnet_cache.invalidate().await;
    }

    /// Drop a torrent from the bt session; `with_files=true` also wipes the on-disk payload, `with_files=false` keeps files but still releases the `by_hash` reservation so re-adding the same magnet isn't blocked as `AlreadyManaged`
    pub async fn remove(&self, torrent_id: usize, with_files: bool) -> Result<(), String> {
        let session = self.get_session()?;
        session
            .delete(bt::TorrentIdOrHash::Id(torrent_id), with_files)
            .await
            .map_err(|e| format!("Failed to remove torrent: {}", e))
    }

    pub async fn shutdown(&mut self) {
        if let Some(session) = self.session.take() {
            drop(session);
        }
    }
}

fn extract_handle(response: bt::AddTorrentResponse) -> Result<TorrentHandle, String> {
    match response {
        bt::AddTorrentResponse::Added(id, handle)
        | bt::AddTorrentResponse::AlreadyManaged(id, handle) => Ok(TorrentHandle {
            id,
            info_hash: Some(handle.info_hash().to_hex()),
            info_hash_v2: handle.info_hash_v2().map(|h| h.to_hex()),
            meta_version: handle.meta_version().map(|s| s.to_string()),
        }),
        bt::AddTorrentResponse::ListOnly(_) => {
            Err("Torrent was added in list-only mode".to_string())
        }
    }
}

fn extract_file_details(info: &bt::ValidatedTorrentMetaV1Info) -> Vec<TorrentFileInfo> {
    info.iter_file_details()
        .enumerate()
        .map(|(idx, d)| TorrentFileInfo {
            index: idx,
            path: d.filename.to_string(),
            length: d.len,
        })
        .collect()
}

/// Aggregate `announce` and `announce_list` into the BEP-12 nested-tier shape the frontend expects (`string[][]`), falling back to a single tier with the primary announce URL when no list is present
fn build_announce_list(meta: &bt::TorrentMeta) -> Vec<Vec<String>> {
    if !meta.announce_list.is_empty() {
        return meta
            .announce_list
            .iter()
            .map(|tier| tier.to_vec())
            .filter(|tier: &Vec<String>| !tier.is_empty())
            .collect();
    }
    match &meta.announce {
        Some(url) if !url.is_empty() => vec![vec![url.clone()]],
        _ => Vec::new(),
    }
}

pub struct TorrentHandle {
    pub id: usize,
    pub info_hash: Option<String>,
    pub info_hash_v2: Option<String>,
    pub meta_version: Option<String>,
}

pub struct TorrentFileInfo {
    pub index: usize,
    pub path: String,
    pub length: u64,
}

pub struct TorrentStats {
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub download_speed: u64,
    pub upload_speed: u64,
    pub num_peers: u32,
    pub num_seeders: u32,
    pub is_finished: bool,
    pub name: Option<String>,
    pub file_progress: Vec<u64>,
    pub file_details: Option<Vec<TorrentFileInfo>>,
    pub resolved_root: Option<String>,
    pub single_file_mode: bool,
    pub peers: Vec<PeerSnapshot>,
    pub metadata: Option<TorrentMetadataInfo>,
}

pub struct PeerSnapshot {
    pub addr: std::net::SocketAddr,
    /// Raw bitfield bytes; manager hex-encodes for the RPC payload
    pub bitfield: std::sync::Arc<[u8]>,
    pub am_choking: bool,
    pub am_interested: bool,
    pub peer_choking: bool,
    pub peer_interested: bool,
    pub seeder: bool,
    pub peer_id: Option<[u8; 20]>,
    pub client: Option<String>,
    pub downloaded: u64,
    pub uploaded: u64,
    pub dl_speed: u64,
    pub up_speed: u64,
    pub incoming: bool,
    pub snubbed: bool,
    pub progress: f64,
    pub optimistic_unchoke: bool,
}

pub struct MagnetInfo {
    pub info_hash: String,
    pub info_hash_v2: Option<String>,
    pub display_name: Option<String>,
}

pub struct TorrentMetadataInfo {
    pub piece_length: u32,
    pub num_pieces: u32,
    pub comment: Option<String>,
    pub creation_date: Option<i64>,
    pub announce_list: Vec<Vec<String>>,
}

pub fn is_magnet_uri(uri: &str) -> bool {
    uri.trim().to_lowercase().starts_with("magnet:")
}

pub fn is_thunder_uri(uri: &str) -> bool {
    let trimmed = uri.trim();
    let Some(scheme_end) = trimmed.find("://") else {
        return false;
    };
    trimmed[..scheme_end].eq_ignore_ascii_case("thunder")
}

fn is_zero_prefixed_suffix(value: &str, suffix: &str) -> bool {
    value
        .strip_suffix(suffix)
        .is_some_and(|prefix| prefix.bytes().all(|byte| byte == b'0'))
}

fn thunder_ampersand_entity_len(value: &str) -> Option<usize> {
    if value
        .get(..5)
        .is_some_and(|entity| entity.eq_ignore_ascii_case("&amp;"))
    {
        return Some(5);
    }

    let end = value.find(';')?;
    let code = value.get(..=end)?.strip_prefix("&#")?.strip_suffix(';')?;
    let is_ampersand = if let Some(hex) = code.strip_prefix(['x', 'X']) {
        is_zero_prefixed_suffix(hex, "26")
    } else {
        is_zero_prefixed_suffix(code, "38")
    };
    is_ampersand.then_some(end + 1)
}

fn thunder_unknown_entity_len(value: &str) -> Option<usize> {
    let end = value.find(';')?;
    let nested_ampersand = value.get(1..end)?.find('&')?;
    let name_end = nested_ampersand + 1;
    let name = value.get(1..name_end)?;
    if name.is_empty()
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'#')
    {
        return None;
    }

    Some(end + 1)
}

fn decode_thunder_ampersands(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    let mut rest = value;

    while let Some(start) = rest.find('&') {
        result.push_str(&rest[..start]);
        let entity = &rest[start..];
        if let Some(entity_len) = thunder_ampersand_entity_len(entity) {
            result.push('&');
            rest = &entity[entity_len..];
        } else if let Some(entity_len) = thunder_unknown_entity_len(entity) {
            result.push_str(&entity[..entity_len]);
            rest = &entity[entity_len..];
        } else {
            result.push('&');
            rest = &entity[1..];
        }
    }

    result.push_str(rest);
    result
}

fn is_valid_thunder_base64(value: &str) -> bool {
    let mut padding = 0;
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'+' | b'/' if padding == 0 => {}
            b'=' if padding < 2 => padding += 1,
            _ => return false,
        }
    }
    !value.is_empty()
}

pub fn decode_thunder_uri(uri: &str) -> Option<String> {
    let trimmed = uri.trim();
    let scheme_end = trimmed.find("://")?;
    if !is_thunder_uri(trimmed) {
        return None;
    }

    let mut encoded: String = trimmed[scheme_end + 3..]
        .trim_end_matches([')', '.'])
        .chars()
        .map(|ch| match ch {
            '-' => '+',
            '_' => '/',
            _ => ch,
        })
        .collect();
    if !is_valid_thunder_base64(&encoded) {
        return None;
    }

    let padding = (4 - (encoded.len() % 4)) % 4;
    encoded.extend(std::iter::repeat_n('=', padding));
    let bytes = STANDARD.decode(encoded.as_bytes()).ok()?;
    let decoded = String::from_utf8(bytes).ok()?;
    let wrapped = decoded.strip_prefix("AA")?.strip_suffix("ZZ")?.trim();
    if wrapped.is_empty() {
        return None;
    }

    let normalized = decode_thunder_ampersands(wrapped);
    Some(normalized)
}

pub fn inspect_magnet(uri: &str) -> Result<MagnetInfo, String> {
    let magnet = bt::Magnet::parse(uri).map_err(|e| e.to_string())?;
    Ok(MagnetInfo {
        info_hash: magnet.info_hash().to_hex(),
        info_hash_v2: magnet.info_hash_v2().map(|h| h.to_hex()),
        display_name: magnet.display_name.clone(),
    })
}
async fn save_torrent_metadata_if_enabled(
    bytes: &[u8],
    options: &Map<String, Value>,
    output_dir: &Path,
) {
    let save_metadata = options
        .get("bt-save-metadata")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    if !save_metadata {
        return;
    }
    let dir = options
        .get("dir")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| output_dir.to_str().unwrap_or("."));
    if let Ok(meta) = bt::parse_torrent(bytes) {
        let path = Path::new(dir).join(format!("{}.torrent", meta.info_hash.to_hex()));
        if let Some(parent) = path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                tracing::warn!("Failed to create metadata dir {}: {}", parent.display(), e);
            }
        }
        match tokio::fs::write(&path, bytes).await {
            Ok(()) => tracing::info!("Saved torrent metadata to {}", path.display()),
            Err(e) => tracing::warn!(
                "Failed to save torrent metadata to {}: {}",
                path.display(),
                e
            ),
        }
    }
}

/// Map an optional config string to a concrete BitTorrent encryption policy; unknown/missing values fall back to `Prefer` (MSE first, plaintext fallback) matching the system default
fn encryption_policy_from_str(s: Option<&str>) -> bt::EncryptionPolicy {
    match s {
        Some("plaintext") => bt::EncryptionPolicy::PlaintextOnly,
        Some("require") => bt::EncryptionPolicy::RequireEncryption,
        _ => bt::EncryptionPolicy::Prefer,
    }
}

pub(crate) fn p2p_proxy_from_options(
    options: &Map<String, Value>,
) -> Result<Option<risuko_http::ProxyConnector>, String> {
    let tcp_server = options
        .get("p2p-proxy")
        .and_then(Value::as_str)
        .unwrap_or("");
    let tcp_bypass = options
        .get("p2p-no-proxy")
        .and_then(Value::as_str)
        .unwrap_or("");
    let udp_server = options
        .get("p2p-udp-proxy")
        .and_then(Value::as_str)
        .unwrap_or("");
    let udp_bypass = options
        .get("p2p-udp-no-proxy")
        .and_then(Value::as_str)
        .unwrap_or("");
    if tcp_server.trim().is_empty() && udp_server.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(super::options::build_p2p_proxy_connector(
        tcp_server, tcp_bypass, udp_server, udp_bypass,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn resolved_meta(byte: u8) -> ResolvedMagnetMeta {
        ResolvedMagnetMeta {
            bytes: Arc::from(vec![byte].into_boxed_slice()),
            peers: Arc::from(Vec::<SocketAddr>::new().into_boxed_slice()),
            tracker_peers: Arc::from(Vec::<SocketAddr>::new().into_boxed_slice()),
        }
    }

    #[tokio::test]
    async fn magnet_cache_singleflights_concurrent_callers() {
        let cache = MagnetMetaCache::default();
        let key = [7; 20];
        let calls = Arc::new(AtomicUsize::new(0));

        let first_calls = calls.clone();
        let first = cache.get_or_resolve(key, move || async move {
            first_calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(25)).await;
            Ok(resolved_meta(42))
        });

        let second_calls = calls.clone();
        let second = cache.get_or_resolve(key, move || async move {
            second_calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(25)).await;
            Ok(resolved_meta(42))
        });

        let (first, second) = tokio::join!(first, second);
        assert_eq!(first.unwrap().bytes.as_ref(), &[42]);
        assert_eq!(second.unwrap().bytes.as_ref(), &[42]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let cached_calls = calls.clone();
        let cached = cache
            .get_or_resolve(key, move || async move {
                cached_calls.fetch_add(1, Ordering::SeqCst);
                Ok(resolved_meta(99))
            })
            .await
            .unwrap();
        assert_eq!(cached.bytes.as_ref(), &[42]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn magnet_cache_failure_does_not_poison_retry() {
        let cache = MagnetMetaCache::default();
        let key = [9; 20];

        let error = cache
            .get_or_resolve(key, || async { Err("first attempt failed".to_string()) })
            .await
            .unwrap_err();
        assert_eq!(error, "first attempt failed");

        let retried = cache
            .get_or_resolve(key, || async { Ok(resolved_meta(11)) })
            .await
            .unwrap();
        assert_eq!(retried.bytes.as_ref(), &[11]);
    }

    #[tokio::test]
    async fn magnet_cache_panic_does_not_leave_in_flight_entry() {
        let cache = MagnetMetaCache::default();
        let key = [4; 20];

        let error = tokio::time::timeout(Duration::from_secs(1), async {
            cache
                .get_or_resolve(key, || async {
                    panic!("resolver boom");
                    #[allow(unreachable_code)]
                    Ok(resolved_meta(0))
                })
                .await
        })
        .await
        .expect("panicked resolver should not hang")
        .unwrap_err();
        assert_eq!(error, "Magnet metadata resolver panicked");

        let retried = tokio::time::timeout(Duration::from_secs(1), async {
            cache
                .get_or_resolve(key, || async { Ok(resolved_meta(12)) })
                .await
        })
        .await
        .expect("retry after resolver panic should not hang")
        .unwrap();
        assert_eq!(retried.bytes.as_ref(), &[12]);
    }

    #[tokio::test]
    async fn magnet_cache_invalidation_cancels_old_route_and_clears_entries() {
        let cache = MagnetMetaCache::default();
        let key = [3; 20];
        let started = Arc::new(tokio::sync::Notify::new());
        let started_resolver = started.clone();
        let pending = tokio::spawn({
            let cache = cache.clone();
            async move {
                cache
                    .get_or_resolve(key, move || async move {
                        started_resolver.notify_one();
                        tokio::time::sleep(Duration::from_secs(60)).await;
                        Ok(resolved_meta(1))
                    })
                    .await
            }
        });

        tokio::time::timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("metadata resolver did not start");
        cache.invalidate().await;
        let error = tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .expect("in-flight lookup was not cancelled")
            .expect("lookup task panicked")
            .expect_err("cancelled lookup unexpectedly succeeded");
        assert!(error.contains("P2P proxy profile changed"));

        let refreshed = cache
            .get_or_resolve(key, || async { Ok(resolved_meta(2)) })
            .await
            .unwrap();
        assert_eq!(refreshed.bytes.as_ref(), &[2]);
    }

    #[tokio::test]
    async fn delayed_resolver_cannot_adopt_the_new_route_generation() {
        let cache = MagnetMetaCache::default();
        let (old_generation, old_cancellation) = cache.route_snapshot().await;

        // Model a resolver that was spawned before the reload, then delayed
        // until after invalidation has installed a fresh cancellation token.
        cache.invalidate().await;
        assert!(old_cancellation.is_cancelled());

        let error = cache
            .route_snapshot_for(old_generation)
            .await
            .expect_err("old resolver must not adopt the new route token");
        assert_eq!(error, MAGNET_ROUTE_CHANGED);

        let (new_generation, new_cancellation) = cache.route_snapshot().await;
        assert_ne!(new_generation, old_generation);
        assert!(!new_cancellation.is_cancelled());
    }

    #[test]
    fn parse_trackers_splits_newlines_and_commas() {
        let mut opts = Map::new();
        opts.insert(
            "bt-tracker".into(),
            json!("udp://a:1/announce\n\nudp://b:2/announce,http://c:3/announce\r\nudp://a:1/announce"),
        );
        assert_eq!(
            TorrentEngine::parse_trackers(&opts),
            vec![
                "udp://a:1/announce".to_string(),
                "udp://b:2/announce".to_string(),
                "http://c:3/announce".to_string(),
            ]
        );
    }

    #[test]
    fn encryption_policy_known_values() {
        assert!(matches!(
            encryption_policy_from_str(Some("plaintext")),
            bt::EncryptionPolicy::PlaintextOnly
        ));
        assert!(matches!(
            encryption_policy_from_str(Some("prefer")),
            bt::EncryptionPolicy::Prefer
        ));
        assert!(matches!(
            encryption_policy_from_str(Some("require")),
            bt::EncryptionPolicy::RequireEncryption
        ));
    }

    #[test]
    fn encryption_policy_unknown_falls_back_to_prefer() {
        assert!(matches!(
            encryption_policy_from_str(None),
            bt::EncryptionPolicy::Prefer
        ));
        assert!(matches!(
            encryption_policy_from_str(Some("garbage")),
            bt::EncryptionPolicy::Prefer
        ));
        // case-sensitive: uppercase is not recognised
        assert!(matches!(
            encryption_policy_from_str(Some("REQUIRE")),
            bt::EncryptionPolicy::Prefer
        ));
    }

    #[test]
    fn decodes_thunder_magnet_and_html_query_separator() {
        let encoded = base64::engine::general_purpose::STANDARD.encode(
            "AAmagnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&amp;dn=ExampleZZ",
        );
        let uri = decode_thunder_uri(&format!("THUNDER://{encoded}"));
        assert_eq!(
            uri.as_deref(),
            Some("magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&dn=Example")
        );
    }

    #[test]
    fn decodes_thunder_numeric_ampersand_entities_and_trailing_punctuation() {
        let encoded = base64::engine::general_purpose::STANDARD.encode(
            "AAmagnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&#x000026;dn=ExampleZZ",
        );
        assert_eq!(
            decode_thunder_uri(&format!("thunder://{encoded}).")),
            Some(
                "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&dn=Example"
                    .to_string()
            )
        );
    }

    #[test]
    fn decodes_thunder_ampersands_after_existing_query_parameters() {
        let encoded = base64::engine::general_purpose::STANDARD.encode(
            "AAmagnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&tr=udp%3A%2F%2Ftracker.example%3A80%2Fannounce&amp;dn=ExampleZZ",
        );
        assert_eq!(
            decode_thunder_uri(&format!("thunder://{encoded}")),
            Some(
                "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&tr=udp%3A%2F%2Ftracker.example%3A80%2Fannounce&dn=Example"
                    .to_string()
            )
        );
    }

    #[test]
    fn preserves_unknown_thunder_entity_with_nested_ampersand() {
        let encoded = base64::engine::general_purpose::STANDARD
            .encode("AAmagnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&foo&amp;barZZ");
        assert_eq!(
            decode_thunder_uri(&format!("thunder://{encoded}")),
            Some(
                "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&foo&amp;bar"
                    .to_string()
            )
        );
    }

    #[test]
    fn decodes_thunder_payload_with_partial_padding() {
        let uri = "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&dn=Examplex";
        let encoded = base64::engine::general_purpose::STANDARD.encode(format!("AA{uri}ZZ"));
        assert!(encoded.ends_with("=="));
        let partial = encoded.strip_suffix('=').unwrap();
        let unpadded = encoded.trim_end_matches('=');

        assert_eq!(
            decode_thunder_uri(&format!("thunder://{partial}")),
            Some(uri.to_string())
        );
        assert_eq!(
            decode_thunder_uri(&format!("thunder://{unpadded}")),
            Some(uri.to_string())
        );
    }

    #[test]
    fn decodes_thunder_payload_with_mixed_base64_alphabets() {
        let uri = "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&dn=\u{0BFF}";
        let encoded = base64::engine::general_purpose::STANDARD.encode(format!("AA{uri}ZZ"));
        assert!(encoded.contains('+'));
        assert!(encoded.contains('/'));
        let mixed = encoded.replacen('+', "-", 1);

        assert_eq!(
            decode_thunder_uri(&format!("thunder://{mixed}")),
            Some(uri.to_string())
        );

        let url_safe = encoded.replacen('/', "_", 1);
        assert_eq!(
            decode_thunder_uri(&format!("thunder://{url_safe}")),
            Some(uri.to_string())
        );
    }

    #[test]
    fn rejects_thunder_payload_with_undocumented_trailing_characters() {
        let encoded = base64::engine::general_purpose::STANDARD
            .encode("AAmagnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862ZZ");

        assert_eq!(decode_thunder_uri(&format!("thunder://{encoded}!")), None);
    }

    #[test]
    fn rejects_thunder_payload_without_envelope() {
        let encoded = base64::engine::general_purpose::STANDARD.encode("ABC");
        assert_eq!(decode_thunder_uri(&format!("thunder://{encoded}")), None);
    }
}
