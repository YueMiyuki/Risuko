//! Per-torrent state machine

pub mod stats;

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use rand::RngExt;
use sha1::{Digest, Sha1};

use super::blocklist::BlockList;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::AbortHandle;
use tokio::time::{interval, MissedTickBehavior};

use super::core::{supports_v2_wire, Id20, Lengths, MerkleProofTable, PieceVerifier, TorrentMeta};
use super::peer::{
    connect_prefer_utp, connect_with_utp_fallback, PeerCommand, PeerEvent, SpawnPeer,
};
use super::piece::{ChunkTracker, PieceTracker};
use super::storage::{FileSet, FilesystemStorage};
use super::tracker::{AnnounceEvent, AnnounceRequest};
use super::utp::UtpSocket;
use super::wire::extended::{
    build_holepunch, holepunch_err, holepunch_type, parse_holepunch, ut_metadata_data,
    ut_metadata_type, ExtHandshake, HolepunchMsg, EXT_HANDSHAKE_ID,
};
use super::wire::{Message, MessageEncoder};

pub use stats::{
    AggregatedLiveStats, LiveStats, PeerSnapshot, Snapshot, SpeedSample, TorrentStats,
};

const DEFAULT_MAX_OUTSTANDING_PER_PEER: usize = 6;
const DEFAULT_ADAPTIVE_MAX_OUTSTANDING_PER_PEER: usize = 96;
const ABSOLUTE_MAX_OUTSTANDING_PER_PEER: usize = 256;
const TORRENT_REQUEST_BUDGET: usize = 1024;
const DEFAULT_MAX_PEERS: usize = 100;
const MAX_PENDING_DIALS: usize = 48;
const PRIORITY_DIAL_RESERVE: usize = 12;
const MAX_PEER_BACKLOG: usize = 1024;
const DIAL_RETRY_DELAY: Duration = Duration::from_secs(20);
const USEFUL_PEER_REDIAL_DELAY: Duration = Duration::from_secs(8);
const MAX_DIAL_RETRIES: usize = 512;
const OUTBOUND_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const USEFUL_PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(12);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const TRACKER_ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(30);
const TRACKER_STOPPED_TIMEOUT: Duration = Duration::from_secs(5);
const TRACKER_SHUTDOWN_GRACE: Duration = Duration::from_secs(8);
const TRACKER_MIN_INTERVAL: Duration = Duration::from_secs(60);
const TRACKER_MAX_INTERVAL: Duration = Duration::from_secs(30 * 60);

const PEER_IDLE_TIMEOUT: Duration = Duration::from_secs(180);

const SNUB_EVICTION_TIMEOUT: Duration = Duration::from_secs(60);
const PEER_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(5);

const UPLOAD_SLOTS: usize = 4;
const CHOKE_EVAL_INTERVAL: Duration = Duration::from_secs(10);
const OPTIMISTIC_ROTATE_INTERVAL: Duration = Duration::from_secs(30);
const PIPELINE_TARGET_SECS: f32 = 4.0;
const PIPELINE_RATE_WINDOW: Duration = Duration::from_secs(2);
const PIPELINE_SLOW_START_CAP: usize = 64;
const PIPELINE_ADDITIVE_GROWTH: usize = 8;

const PEX_INTERVAL: Duration = Duration::from_secs(60);
const MAX_PEX_ADDED_PER_MSG: usize = 50;

const OUR_UT_METADATA_ID: u8 = 3;
const OUR_UT_PEX_ID: u8 = 4;
const OUR_UT_HOLEPUNCH_ID: u8 = 5;
const MAX_PEX_SOURCE_ENTRIES: usize = 4096;
const META_PIECE_SIZE: usize = 16 * 1024;
const MAX_FAST_PIECES: usize = 10;
const MAX_SUGGESTED_PIECES: usize = 64;
const WEBSEED_MAX_WORKERS: usize = 4;
const WEBSEED_MAX_PIECES_PER_JOB: usize = 8;
const WEBSEED_MAX_RANGE_BYTES: u64 = super::webseed::DEFAULT_MAX_RANGE_BYTES;
const WEBSEED_TIMEOUT: Duration = Duration::from_secs(30);

pub struct TorrentInit {
    pub meta: TorrentMeta,
    pub lengths: Lengths,
    pub root_dir: PathBuf,
    pub only_files: Option<Vec<usize>>,
    pub max_outstanding_per_peer: Option<usize>,
    pub max_peers: Option<usize>,
    pub encryption: super::peer::EncryptionPolicy,
    pub advertise_v2: bool,
    pub verifier: PieceVerifier,
    pub create_subfolder: bool,
    pub utp: Option<Arc<UtpSocket>>,
    pub upload_limiter: Option<Arc<crate::limiter::UploadLimiter>>,
    pub dht: Option<Arc<super::dht::Dht>>,
    pub p2p_proxy: Option<risuko_http::ProxyConnector>,
    pub p2p_proxy_is_task_override: bool,
    pub tracker_source_addr: Option<SocketAddr>,
    pub blocklist: Arc<RwLock<BlockList>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PeerSource {
    Manual,
    Tracker,
    Dht,
    Lsd,
    Pex,
    Holepunch,
    WebSeed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PeerCandidate {
    pub addr: SocketAddr,
    pub source: PeerSource,
}

#[derive(Clone, Default)]
struct GenerationToken {
    current: Arc<AtomicU64>,
}

impl GenerationToken {
    fn new() -> Self {
        Self {
            current: Arc::new(AtomicU64::new(0)),
        }
    }

    fn current(&self) -> u64 {
        self.current.load(Ordering::Acquire)
    }

    fn bump(&self) -> u64 {
        self.current
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1)
    }

    fn is_current(&self, generation: u64) -> bool {
        self.current() == generation
    }
}

#[derive(Debug, Clone, Copy)]
struct PeerEnvelope {
    generation: u64,
    candidate: PeerCandidate,
}

fn peer_source_allowed(
    private_torrent: bool,
    source: PeerSource,
    addr: SocketAddr,
    tracker_authorized: &HashSet<SocketAddr>,
) -> bool {
    !private_torrent || source == PeerSource::Tracker || tracker_authorized.contains(&addr)
}

#[derive(Debug)]
pub enum TorrentCommand {
    AddPeer(PeerCandidate),
    AddInboundPeer {
        addr: SocketAddr,
        cmd_tx: mpsc::Sender<PeerCommand>,
        event_rx: mpsc::Receiver<PeerEvent>,
        reserved: [u8; 8],
        peer_id: Id20,
        io_abort: AbortHandle,
    },
    AddTrackers {
        urls: Vec<String>,
        ack: oneshot::Sender<usize>,
    },
    Pause(oneshot::Sender<()>),
    Unpause(oneshot::Sender<()>),
    ReconfigureP2p {
        proxy: Option<risuko_http::ProxyConnector>,
        replace_proxy: bool,
        dht: Option<Arc<super::dht::Dht>>,
        ack: oneshot::Sender<()>,
    },
    ApplyBlocklist {
        ack: oneshot::Sender<(u32, u32)>,
    },
    Stop(oneshot::Sender<()>),
}

pub struct ManagedTorrent {
    pub id: usize,
    pub info_hash: Id20,
    pub name: Option<String>,
    pub metadata: ArcSwapOption<TorrentMeta>,
    pub create_subfolder: bool,
    pub root_dir: PathBuf,
    pub advertise_v2: Arc<AtomicBool>,
    pub ext_handshake_builder: crate::peer::ExtHandshakeBuilder,
    pub p2p_proxy_is_task_override: bool,
    pub(crate) cmd_tx: mpsc::Sender<TorrentCommand>,
    pub(crate) stats: Arc<Mutex<TorrentStats>>,
}

impl ManagedTorrent {
    pub fn info_hash(&self) -> Id20 {
        self.info_hash
    }
    pub fn info_hash_v2(&self) -> Option<crate::core::hash::Id32> {
        self.metadata.load().as_ref().and_then(|m| m.info_hash_v2)
    }
    pub fn meta_version(&self) -> Option<&'static str> {
        self.metadata
            .load()
            .as_ref()
            .map(|m| m.meta_version.as_str())
    }
    pub fn name(&self) -> Option<String> {
        self.name.clone()
    }
    pub fn stats(&self) -> TorrentStats {
        let mut s = self.stats.lock().clone();
        s.refresh_live();
        s
    }
    pub fn with_metadata<T>(&self, f: impl FnOnce(&TorrentMeta) -> T) -> Result<T, &'static str> {
        match self.metadata.load().as_ref() {
            Some(m) => Ok(f(m.as_ref())),
            None => Err("metadata not yet available"),
        }
    }
    pub(crate) fn cmd_tx(&self) -> mpsc::Sender<TorrentCommand> {
        self.cmd_tx.clone()
    }
}

pub async fn spawn(
    id: usize,
    init: TorrentInit,
    our_peer_id: Id20,
    listen_port: u16,
) -> std::io::Result<Arc<ManagedTorrent>> {
    // `only_files` reaches TorrentInit, but the scheduler still fetches every piece; warn so callers don't think a subset-only download is active
    if init.only_files.is_some() {
        tracing::warn!(
            "torrent {id}: selective download (only_files) is not yet supported; \
             downloading all files"
        );
    }
    let info_hash = init.meta.info_hash;
    let name = Some(init.meta.info.name.clone());
    let (cmd_tx, cmd_rx) = mpsc::channel::<TorrentCommand>(64);
    let file_lens: Vec<u64> = init.meta.info.iter_file_details().map(|f| f.len).collect();
    let stats = Arc::new(Mutex::new(TorrentStats::initial(
        init.lengths.total_length(),
        file_lens,
    )));
    let meta_arc = Arc::new(init.meta.clone());
    let metadata_swap = ArcSwapOption::new(Some(meta_arc));
    let ext_handshake_builder: crate::peer::ExtHandshakeBuilder = {
        let metadata_size = init.meta.info_bytes.len() as u64;
        let private_torrent = init.meta.info.private;
        std::sync::Arc::new(move |peer_ip: std::net::IpAddr| {
            let mut hs =
                ExtHandshake::new_outgoing(OUR_UT_METADATA_ID, OUR_UT_PEX_ID, Some(metadata_size))
                    .with_yourip(peer_ip)
                    .with_port(listen_port);
            if private_torrent {
                hs.supported.remove(b"ut_pex".as_slice());
            } else {
                hs = hs.with_holepunch(OUR_UT_HOLEPUNCH_ID);
            }
            MessageEncoder::encode(&Message::Extended {
                ext_id: EXT_HANDSHAKE_ID,
                payload: hs.encode(),
            })
        })
    };
    let advertise_v2_flag = Arc::new(AtomicBool::new(init.advertise_v2));
    let handle = Arc::new(ManagedTorrent {
        id,
        info_hash,
        name,
        metadata: metadata_swap,
        create_subfolder: init.create_subfolder,
        root_dir: init.root_dir.clone(),
        advertise_v2: Arc::clone(&advertise_v2_flag),
        ext_handshake_builder,
        p2p_proxy_is_task_override: init.p2p_proxy_is_task_override,
        cmd_tx,
        stats: stats.clone(),
    });
    tokio::spawn(torrent_loop(
        id,
        init,
        our_peer_id,
        listen_port,
        cmd_rx,
        stats,
        advertise_v2_flag,
        handle.ext_handshake_builder.clone(),
    ));
    Ok(handle)
}

struct Peer {
    addr: SocketAddr,
    cmd_tx: mpsc::Sender<PeerCommand>,
    bitfield: Vec<u8>,
    am_choking: bool,
    am_interested: bool,
    peer_choking: bool,
    peer_interested: bool,
    outstanding: Vec<(u32, u32, u32)>,
    max_outstanding: usize,
    delivered_window: u32,
    window_start: Instant,
    delivered_since_growth: usize,
    slow_start: bool,

    snubbing: bool,
    last_recv: Instant,
    snub_since: Option<Instant>,
    consecutive_rejects: u32,
    reqq_cap: Option<usize>,
    their_ut_metadata_id: Option<u8>,
    their_ut_holepunch_id: Option<u8>,
    downloaded_window: u64,
    uploaded_window: Arc<AtomicU64>,
    downloaded_total: u64,
    uploaded_total: Arc<AtomicU64>,
    last_snap_downloaded: u64,
    last_snap_uploaded: u64,
    dl_speed: u64,
    up_speed: u64,
    peer_id: Option<Id20>,
    client: Option<String>,
    optimistic_unchoke: bool,
    their_ut_pex_id: Option<u8>,
    pex_sent: HashSet<SocketAddr>,
    outbound: bool,
    supports_fast: bool,
    initial_availability: Option<InitialAvailability>,
    suggested_pieces: HashSet<u32>,
    remote_allowed_fast: HashSet<u32>,
    sent_allowed_fast: HashSet<u32>,
    responded_requests: HashSet<(u32, u32, u32)>,
    canceled_requests: HashSet<(u32, u32, u32)>,
    io_abort: Option<AbortHandle>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitialAvailability {
    Bitfield,
    HaveAll,
    HaveNone,
}

impl Peer {
    #[allow(clippy::too_many_arguments)]
    fn connected(
        addr: SocketAddr,
        cmd_tx: mpsc::Sender<PeerCommand>,
        bitfield_bytes: usize,
        max_outstanding: usize,
        pipeline_cap: usize,
        outbound: bool,
        supports_fast: bool,
        peer_id: Option<Id20>,
    ) -> Self {
        Self {
            addr,
            cmd_tx,
            bitfield: vec![0u8; bitfield_bytes],
            am_choking: true,
            am_interested: false,
            peer_choking: true,
            peer_interested: false,
            outstanding: Vec::new(),
            max_outstanding,
            delivered_window: 0,
            window_start: Instant::now(),
            delivered_since_growth: 0,
            slow_start: max_outstanding < pipeline_cap.min(PIPELINE_SLOW_START_CAP),
            snubbing: false,
            last_recv: Instant::now(),
            snub_since: None,
            consecutive_rejects: 0,
            reqq_cap: None,
            their_ut_metadata_id: None,
            their_ut_holepunch_id: None,
            downloaded_window: 0,
            uploaded_window: Arc::new(AtomicU64::new(0)),
            downloaded_total: 0,
            uploaded_total: Arc::new(AtomicU64::new(0)),
            last_snap_downloaded: 0,
            last_snap_uploaded: 0,
            dl_speed: 0,
            up_speed: 0,
            peer_id,
            client: None,
            optimistic_unchoke: false,
            their_ut_pex_id: None,
            pex_sent: HashSet::new(),
            outbound,
            supports_fast,
            initial_availability: None,
            suggested_pieces: HashSet::new(),
            remote_allowed_fast: HashSet::new(),
            sent_allowed_fast: HashSet::new(),
            responded_requests: HashSet::new(),
            canceled_requests: HashSet::new(),
            io_abort: None,
        }
    }
}

const REJECT_SNUB_THRESHOLD: u32 = 8;

struct VerifyResult {
    piece_index: u32,
    write_failed: bool,
    verify_ok: bool,
}

struct WebSeedBatchResult {
    generation: u64,
    pieces: Vec<VerifyResult>,
}

#[derive(Clone)]
struct WebSeedContext {
    client: risuko_http::Client,
    bases: Arc<Vec<String>>,
    disabled: Arc<Mutex<HashSet<usize>>>,
    etags: Arc<Mutex<HashMap<(usize, usize), String>>>,
    info: Arc<super::core::ValidatedTorrentMetaV1Info>,
    layout: FileSet,
    storage: Arc<FilesystemStorage>,
    verifier: PieceVerifier,
    lengths: Lengths,
    generation: GenerationToken,
}

impl WebSeedContext {
    fn has_available_mirror(&self) -> bool {
        self.disabled.lock().len() < self.bases.len()
    }
}

struct PieceAssembly {
    buf: Vec<u8>,
    received_chunks: HashSet<u32>,
    received_bytes: u32,
    expected_bytes: u32,
    completed: bool,
}

#[allow(clippy::too_many_arguments)]
async fn torrent_loop(
    torrent_id: usize,
    init: TorrentInit,
    our_peer_id: Id20,
    listen_port: u16,
    mut cmd_rx: mpsc::Receiver<TorrentCommand>,
    stats: Arc<Mutex<TorrentStats>>,
    advertise_v2_flag: Arc<AtomicBool>,
    ext_handshake_builder: crate::peer::ExtHandshakeBuilder,
) {
    let info = Arc::new(init.meta.info.clone());
    let info_hash = init.meta.info_hash;
    let info_bytes: Arc<Vec<u8>> = Arc::new(init.meta.info_bytes.clone());
    let lengths = init.lengths;
    let encryption = init.encryption;
    let utp = init.utp.clone();
    let outbound_utp = utp.clone();
    let upload_limiter = init.upload_limiter.clone();
    let private_torrent = init.meta.info.private;
    let mut dht = if private_torrent {
        None
    } else {
        init.dht.clone()
    };
    let mut p2p_proxy = init.p2p_proxy.clone();
    let blocklist = init.blocklist.clone();
    let supports_v2 = supports_v2_wire(&init.meta);
    let verifier = init.verifier;
    let hash_tables: Option<Arc<Vec<MerkleProofTable>>> = {
        if let PieceVerifier::V2Merkle { ref tables, .. } = verifier {
            Some(Arc::clone(tables))
        } else if supports_v2 {
            if let Some(ref v2) = init.meta.info_v2 {
                // Hybrid torrent: build Merkle tables from the meta's piece_layers
                let mut tbls = Vec::with_capacity(v2.files.len());
                let mut ok = true;
                for f in &v2.files {
                    let layer = init
                        .meta
                        .piece_layers
                        .get(&f.pieces_root)
                        .map(|v| v.as_slice())
                        .unwrap_or(&[]);
                    match super::core::MerkleProofTable::from_layer_bytes(
                        f.pieces_root,
                        f.length,
                        v2.piece_length,
                        layer,
                    ) {
                        Ok(t) => tbls.push(t),
                        Err(e) => {
                            tracing::warn!("hybrid torrent {info_hash}: could not build Merkle table for serving: {e}");
                            ok = false;
                            break;
                        }
                    }
                }
                if ok {
                    Some(Arc::new(tbls))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    };
    let serve_v2_layers = supports_v2 && hash_tables.is_some();
    let advertise_v2 = init.advertise_v2 && serve_v2_layers;
    advertise_v2_flag.store(advertise_v2, Ordering::Relaxed);

    let (pipeline_floor, pipeline_cap) = pipeline_bounds(init.max_outstanding_per_peer);
    let max_peers = init.max_peers.unwrap_or(DEFAULT_MAX_PEERS).max(1);
    tracing::info!(
        target: "diag",
        "torrent pipeline config: max_outstanding_per_peer={:?} -> floor={} cap={} request_budget={} max_peers={}",
        init.max_outstanding_per_peer,
        pipeline_floor,
        pipeline_cap,
        TORRENT_REQUEST_BUDGET,
        max_peers
    );
    let storage = Arc::new(FilesystemStorage::new(&info, &init.root_dir));
    let mut piece_tracker = PieceTracker::new(lengths);
    let mut chunk_tracker = ChunkTracker::new(lengths);
    let mut piece_assemblies: HashMap<u32, PieceAssembly> = HashMap::new();
    if storage.has_existing_payload_files().await {
        scan_existing_pieces(&verifier, &storage, &lengths, &mut piece_tracker).await;
    }
    if let Err(e) = storage.preallocate().await {
        tracing::warn!("preallocate failed for {info_hash}: {e}");
    }
    {
        let mut s = stats.lock();
        s.file_progress = compute_file_progress(&piece_tracker, &lengths, storage.layout());
        s.progress_bytes = s.file_progress.iter().sum();
        s.finished = piece_tracker.is_complete();
    }

    let announce_hashes = if serve_v2_layers {
        init.meta.announce_infohashes()
    } else {
        vec![info_hash]
    };
    let generation = GenerationToken::new();
    let (peer_src_tx, mut peer_addr_rx) = mpsc::channel::<PeerEnvelope>(256);
    let mut tracker_tiers = collect_tracker_tiers(&init.meta);
    let mut tracker_urls = flatten_tracker_tiers(&tracker_tiers);
    let tracker_info_hashes = announce_hashes.clone();
    let tracker_key = rand::rng().random::<u32>();
    let tracker_state = Arc::new(Mutex::new(TrackerLifecycle::default()));
    let (tracker_tier_tx, mut tracker_tier_rx) = mpsc::channel::<()>(4);
    let mut tracker_tasks = spawn_tracker_tier_pollers(
        peer_src_tx.clone(),
        tracker_tiers.clone(),
        tracker_info_hashes.clone(),
        our_peer_id,
        tracker_key,
        listen_port,
        Arc::clone(&stats),
        p2p_proxy.clone(),
        Arc::clone(&tracker_state),
        private_torrent.then_some(tracker_tier_tx.clone()),
        init.tracker_source_addr,
        generation.clone(),
    );

    let (peer_event_tx, mut peer_event_rx) = mpsc::channel::<(u32, PeerEvent)>(8192);

    let mut peers: HashMap<u32, Peer> = HashMap::new();
    let mut next_pid: u32 = 1;
    let mut known_addrs: HashSet<SocketAddr> = HashSet::new();
    let mut tracker_authorized: HashSet<SocketAddr> = HashSet::new();
    // BEP-55
    let mut pex_source: HashMap<SocketAddr, u32> = HashMap::new();
    let mut holepunch_attempted: HashSet<SocketAddr> = HashSet::new();
    let mut pending_dials: HashMap<u32, SocketAddr> = HashMap::new();
    let mut peer_backlog: VecDeque<SocketAddr> = VecDeque::new();
    let mut priority_backlog: VecDeque<SocketAddr> = VecDeque::new();
    let mut dial_retries: VecDeque<(SocketAddr, Instant)> = VecDeque::new();
    let mut useful_redials: VecDeque<(SocketAddr, Instant)> = VecDeque::new();
    let mut useful_peers: HashMap<SocketAddr, usize> = HashMap::new();
    let registry_scope = Arc::new(());
    let mut paused = false;
    let mut tick = interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_tick = Instant::now();
    let mut last_keepalive = Instant::now();
    let mut last_peer_snapshot = Instant::now() - PEER_SNAPSHOT_INTERVAL;
    let mut last_choke_eval = Instant::now();
    let mut last_window_reset = Instant::now();
    let mut last_optimistic_rotate = Instant::now();
    let mut optimistic_pid: Option<u32> = None;
    let mut choke_dirty = false;
    let mut last_pex = Instant::now();
    let mut bytes_this_tick = (0u64, 0u64);
    let upload_tick: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let mut write_tasks: tokio::task::JoinSet<VerifyResult> = tokio::task::JoinSet::new();
    let mut webseed_tasks: tokio::task::JoinSet<WebSeedBatchResult> = tokio::task::JoinSet::new();
    let mut webseed_inflight: HashSet<u32> = HashSet::new();
    let mut webseed_ctx = if !private_torrent && !init.meta.url_list.is_empty() {
        match build_webseed_client(p2p_proxy.as_ref()) {
            Ok(client) => Some(WebSeedContext {
                client,
                bases: Arc::new(init.meta.url_list.clone()),
                disabled: Arc::new(Mutex::new(HashSet::new())),
                etags: Arc::new(Mutex::new(HashMap::new())),
                info: Arc::clone(&info),
                layout: storage.layout().clone(),
                storage: Arc::clone(&storage),
                verifier: verifier.clone(),
                lengths,
                generation: generation.clone(),
            }),
            Err(e) => {
                tracing::warn!("torrent {info_hash}: unable to initialize WebSeed client: {e}");
                None
            }
        }
    } else {
        None
    };
    let mut outbound_tasks: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    let mut outbound_aborts: HashMap<u32, AbortHandle> = HashMap::new();
    let mut dht_poll_handle: Option<tokio::task::JoinHandle<()>> = dht.clone().map(|initial_dht| {
        initial_dht.set_private(info_hash, private_torrent);
        initial_dht.add_bootstrap_nodes(init.meta.bootstrap_nodes.clone());
        initial_dht.add_bootstrap_hosts(init.meta.bootstrap_hosts.clone());
        spawn_dht_poller(
            initial_dht,
            info_hash,
            listen_port,
            peer_src_tx.clone(),
            generation.clone(),
        )
    });

    let stop_ack = 'torrent: loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else {
                    tracing::debug!("torrent {torrent_id} command channel closed; shutting down");
                    generation.bump();
                    break 'torrent None;
                };
                match cmd {
                TorrentCommand::AddPeer(PeerCandidate { addr, source }) => {
                    if source == PeerSource::Tracker {
                        tracker_authorized.insert(addr);
                    }
                    if !peer_source_allowed(private_torrent, source, addr, &tracker_authorized) {
                        continue;
                    }
                    if enqueue_peer_candidate(
                        addr,
                        useful_peers.contains_key(&addr),
                        &mut priority_backlog,
                        &mut peer_backlog,
                        &mut dial_retries,
                        &mut useful_redials,
                        &mut known_addrs,
                        &blocklist,
                    ) && !paused {
                        drain_peer_backlog(
                            &mut priority_backlog,
                            &mut peer_backlog,
                            &mut dial_retries,
                            &mut useful_redials,
                            &mut pending_dials,
                            &mut known_addrs,
                            &useful_peers,
                            &holepunch_attempted,
                            &mut next_pid,
                            peers.len(),
                            max_peers,
                            torrent_id,
                            &registry_scope,
                            info_hash,
                            our_peer_id,
                            &peer_event_tx,
                            encryption,
                            advertise_v2,
                            !private_torrent,
                            &ext_handshake_builder,
                            &outbound_utp,
                            &p2p_proxy,
                            &mut outbound_tasks,
                            &mut outbound_aborts,
                            &blocklist,
                        );
                    }
                }
                TorrentCommand::AddInboundPeer { addr, cmd_tx, event_rx, reserved, peer_id, io_abort } => {
                    if blocklist.read().contains(addr.ip())
                        || !peer_source_allowed(
                            private_torrent,
                            PeerSource::Manual,
                            addr,
                            &tracker_authorized,
                        )
                    {
                        io_abort.abort();
                        let _ = cmd_tx.try_send(PeerCommand::Disconnect);
                    } else if !paused
                        && peers.len() < max_peers
                        && !known_addrs.contains(&addr)
                        && super::magnet::is_dialable_peer_addr(addr)
                    {
                        known_addrs.insert(addr);
                        let pid = next_pid; next_pid += 1;
                        adopt_inbound_peer(
                            pid,
                            addr,
                            cmd_tx,
                            event_rx,
                            peer_event_tx.clone(),
                            &mut peers,
                            &lengths,
                            &mut piece_tracker,
                            pipeline_floor,
                            pipeline_cap,
                            reserved,
                            Some(peer_id),
                            io_abort,
                            info_hash,
                        )
                        .await;
                    } else {
                        enqueue_peer_candidate(
                            addr,
                            useful_peers.contains_key(&addr),
                            &mut priority_backlog,
                            &mut peer_backlog,
                            &mut dial_retries,
                            &mut useful_redials,
                            &mut known_addrs,
                            &blocklist,
                        );
                        io_abort.abort();
                        let _ = cmd_tx.try_send(PeerCommand::Disconnect);
                    }
                }
                TorrentCommand::Pause(ack) => {
                    generation.bump();
                    paused = true;
                    tracker_tasks.shutdown(false).await;
                    if let Some(handle) = dht_poll_handle.take() {
                        handle.abort();
                    }
                    while peer_addr_rx.try_recv().is_ok() {}
                    let mut paused_candidates = pause_teardown_live_peers(
                        &mut peers,
                        &mut useful_peers,
                        &mut piece_tracker,
                        &mut chunk_tracker,
                        &mut piece_assemblies,
                        &lengths,
                        pipeline_floor,
                        pipeline_cap,
                    );
                    paused_candidates.extend(pending_dials.drain().map(|(_, addr)| addr));
                    outbound_tasks.shutdown().await;
                    outbound_aborts.clear();
                    webseed_tasks.shutdown().await;
                    for idx in webseed_inflight.drain() {
                        if let Ok(vpi) = lengths.validate_piece(idx) {
                            piece_tracker.clear_in_flight(vpi);
                        }
                    }
                    for (_, cmd_tx, registry_addr, io_abort) in
                        peer_registry::drain_scope(torrent_id, &registry_scope)
                    {
                        paused_candidates.push(registry_addr);
                        abort_peer_io(io_abort.as_ref(), &cmd_tx);
                    }

                    for addr in paused_candidates {
                        if useful_peers.contains_key(&addr) {
                            priority_backlog.push_back(addr);
                        } else {
                            peer_backlog.push_back(addr);
                        }
                    }
                    refresh_peer_queue_state(
                        &mut priority_backlog,
                        &mut peer_backlog,
                        &mut dial_retries,
                        &mut useful_redials,
                        &peers,
                        &pending_dials,
                        &mut known_addrs,
                    );
                    pex_source.clear();
                    holepunch_attempted.clear();
                    {
                        let mut s = stats.lock();
                        s.live_stats.snapshot.peer_stats.live = 0;
                        s.peers.clear();
                    }
                    while let Some(result) = write_tasks.join_next().await {
                        match result {
                            Ok(vr) => {
                                process_verify_result(
                                    vr,
                                    &lengths,
                                    &mut piece_tracker,
                                    &mut chunk_tracker,
                                    &mut peers,
                                    &storage,
                                    &stats,
                                    &mut piece_assemblies,
                                )
                                .await;
                            }
                            Err(e) if !e.is_cancelled() => {
                                tracing::warn!("piece write/verify task failed during pause: {e}");
                            }
                            Err(_) => {}
                        }
                    }
                    // Release the cached file descriptors
                    if let Err(e) = storage.close_handles().await {
                        tracing::warn!("failed to close storage handles on pause: {e}");
                    }
                    let _ = ack.send(());
                }
                TorrentCommand::Unpause(ack) => {
                    paused = false;
                    if tracker_tasks.is_empty() {
                        tracker_tasks = spawn_tracker_tier_pollers(
                            peer_src_tx.clone(),
                            tracker_tiers.clone(),
                            tracker_info_hashes.clone(),
                            our_peer_id,
                            tracker_key,
                            listen_port,
                            Arc::clone(&stats),
                            p2p_proxy.clone(),
                            Arc::clone(&tracker_state),
                            private_torrent.then_some(tracker_tier_tx.clone()),
                            init.tracker_source_addr,
                            generation.clone(),
                        );
                    }
                    if dht_poll_handle.is_none() {
                        if let Some(dht) = dht.clone() {
                            dht_poll_handle = Some(spawn_dht_poller(
                                dht,
                                info_hash,
                                listen_port,
                                peer_src_tx.clone(),
                                generation.clone(),
                            ));
                        }
                    }
                    // Resume immediately rather than waiting up to one tick
                    drain_peer_backlog(
                        &mut priority_backlog,
                        &mut peer_backlog,
                        &mut dial_retries,
                        &mut useful_redials,
                        &mut pending_dials,
                        &mut known_addrs,
                        &useful_peers,
                        &holepunch_attempted,
                        &mut next_pid,
                        peers.len(),
                        max_peers,
                        torrent_id,
                        &registry_scope,
                        info_hash,
                        our_peer_id,
                        &peer_event_tx,
                        encryption,
                        advertise_v2,
                        !private_torrent,
                            &ext_handshake_builder,
                            &outbound_utp,
                            &p2p_proxy,
                            &mut outbound_tasks,
                            &mut outbound_aborts,
                            &blocklist,
                        );
                    let _ = ack.send(());
                }
                TorrentCommand::ReconfigureP2p { proxy, replace_proxy, dht: next_dht, ack } => {
                    generation.bump();
                    if replace_proxy {
                        p2p_proxy = proxy;
                    }
                    webseed_tasks.shutdown().await;
                    for idx in webseed_inflight.drain() {
                        if let Ok(vpi) = lengths.validate_piece(idx) {
                            piece_tracker.clear_in_flight(vpi);
                        }
                    }
                    if replace_proxy {
                        webseed_ctx = if !private_torrent && !init.meta.url_list.is_empty() {
                            build_webseed_client(p2p_proxy.as_ref()).ok().map(|client| WebSeedContext {
                                client,
                                bases: Arc::new(init.meta.url_list.clone()),
                                disabled: Arc::new(Mutex::new(HashSet::new())),
                                etags: Arc::new(Mutex::new(HashMap::new())),
                                info: Arc::clone(&info),
                                layout: storage.layout().clone(),
                                storage: Arc::clone(&storage),
                                verifier: verifier.clone(),
                                lengths,
                                generation: generation.clone(),
                            })
                        } else {
                            None
                        };
                    }
                    if let Some(handle) = dht_poll_handle.take() {
                        handle.abort();
                    }
                    while peer_addr_rx.try_recv().is_ok() {}
                    if private_torrent {
                        if let Some(next_dht) = next_dht.as_ref() {
                            next_dht.set_private(info_hash, true);
                        }
                    }
                    dht = if private_torrent { None } else { next_dht };
                    if let Some(next_dht) = dht.clone() {
                        next_dht.set_private(info_hash, private_torrent);
                        dht_poll_handle = Some(spawn_dht_poller(
                            next_dht,
                            info_hash,
                            listen_port,
                            peer_src_tx.clone(),
                            generation.clone(),
                        ));
                    }
                    if replace_proxy {
                        tracker_tasks.shutdown(false).await;
                    }
                    // The manager normally sends this command while paused;
                    // defer new announces until the matching Unpause so the
                    // route swap cannot race a stale peer dial.
                    let _ = ack.send(());
                }
                TorrentCommand::AddTrackers { urls, ack } => {
                    let new_urls = normalize_tracker_urls(urls, &tracker_urls);
                    let added = new_urls.len();
                    for url in &new_urls {
                        tracker_urls.push(url.clone());
                    }
                    if !new_urls.is_empty() {
                        generation.bump();
                        webseed_tasks.shutdown().await;
                        for idx in webseed_inflight.drain() {
                            if let Ok(vpi) = lengths.validate_piece(idx) {
                                piece_tracker.clear_in_flight(vpi);
                            }
                        }
                        tracker_tiers.push(new_urls.clone());
                        tracker_state.lock().append_tier(&new_urls);
                        if tracker_tasks.is_empty() {
                            if !paused {
                                tracker_tasks = spawn_tracker_tier_pollers(
                                    peer_src_tx.clone(),
                                    tracker_tiers.clone(),
                                    tracker_info_hashes.clone(),
                                    our_peer_id,
                                    tracker_key,
                                    listen_port,
                                    Arc::clone(&stats),
                                    p2p_proxy.clone(),
                                    Arc::clone(&tracker_state),
                                    private_torrent.then_some(tracker_tier_tx.clone()),
                                    init.tracker_source_addr,
                                    generation.clone(),
                                );
                            }
                        } else {
                            tracker_tasks.shutdown(false).await;
                            if !paused {
                                tracker_tasks = spawn_tracker_tier_pollers(
                                    peer_src_tx.clone(),
                                    tracker_tiers.clone(),
                                    tracker_info_hashes.clone(),
                                    our_peer_id,
                                    tracker_key,
                                    listen_port,
                                    Arc::clone(&stats),
                                    p2p_proxy.clone(),
                                    Arc::clone(&tracker_state),
                                    private_torrent.then_some(tracker_tier_tx.clone()),
                                    init.tracker_source_addr,
                                    generation.clone(),
                                );
                            }
                        }
                    }
                    let _ = ack.send(added);
                }
                TorrentCommand::ApplyBlocklist { ack } => {
                    let (disconnected, removed) = apply_blocklist_to_torrent(
                        &blocklist,
                        &mut peers,
                        &mut pending_dials,
                        &mut outbound_aborts,
                        &mut known_addrs,
                        &mut priority_backlog,
                        &mut peer_backlog,
                        &mut dial_retries,
                        &mut useful_redials,
                        &mut useful_peers,
                        &mut pex_source,
                        &mut holepunch_attempted,
                        &mut piece_tracker,
                        &mut chunk_tracker,
                        &mut piece_assemblies,
                        &lengths,
                        torrent_id,
                        &registry_scope,
                    );
                    choke_dirty = true;
                    let _ = ack.send((disconnected, removed));
                }
                TorrentCommand::Stop(ack) => {
                    generation.bump();
                    break 'torrent Some(ack)
                },
                }
            },
            Some(envelope) = peer_addr_rx.recv() => {
                if !generation.is_current(envelope.generation) {
                    continue;
                }
                let PeerCandidate { addr, source } = envelope.candidate;
                if source == PeerSource::Tracker {
                    tracker_authorized.insert(addr);
                }
                if !peer_source_allowed(private_torrent, source, addr, &tracker_authorized) {
                    continue;
                }
                if enqueue_peer_candidate(
                    addr,
                    useful_peers.contains_key(&addr),
                    &mut priority_backlog,
                    &mut peer_backlog,
                    &mut dial_retries,
                    &mut useful_redials,
                    &mut known_addrs,
                    &blocklist,
                ) && !paused {
                    drain_peer_backlog(
                        &mut priority_backlog,
                        &mut peer_backlog,
                        &mut dial_retries,
                        &mut useful_redials,
                        &mut pending_dials,
                        &mut known_addrs,
                        &useful_peers,
                        &holepunch_attempted,
                        &mut next_pid,
                        peers.len(),
                        max_peers,
                        torrent_id,
                        &registry_scope,
                        info_hash,
                        our_peer_id,
                        &peer_event_tx,
                        encryption,
                        advertise_v2,
                        !private_torrent,
                            &ext_handshake_builder,
                            &outbound_utp,
                            &p2p_proxy,
                            &mut outbound_tasks,
                            &mut outbound_aborts,
                            &blocklist,
                        );
                }
            }
            Some(()) = tracker_tier_rx.recv(), if private_torrent => {
                generation.bump();
                tracker_authorized.clear();
                while peer_addr_rx.try_recv().is_ok() {}
                priority_backlog.clear();
                peer_backlog.clear();
                dial_retries.clear();
                useful_redials.clear();
                for (pid, addr) in pending_dials.drain() {
                    if let Some(abort) = outbound_aborts.remove(&pid) {
                        abort.abort();
                    }
                    known_addrs.remove(&addr);
                }
                for peer in peers.values() {
                    let _ = peer.cmd_tx.try_send(PeerCommand::Disconnect);
                }
            }
            Some((pid, ev)) = peer_event_rx.recv() => {
                let kick = process_peer_event(
                    torrent_id, info_hash, init.meta.info.private, pid, ev, &registry_scope, paused, &mut peers, &mut piece_tracker, &mut chunk_tracker,
                    &mut piece_assemblies,
                    &lengths, &storage, &stats, &mut bytes_this_tick,
                    &upload_tick,
                    &mut write_tasks,
                    &mut pending_dials,
                    &mut outbound_aborts,
                    &mut known_addrs,
                    &mut priority_backlog,
                    &mut peer_backlog,
                    &mut dial_retries,
                    &mut useful_redials,
                    &mut useful_peers,
                    &mut pex_source, &mut holepunch_attempted,
                    &peer_src_tx,
                    &generation,
                    &verifier,
                    &info_bytes,
                    hash_tables.as_deref().map(|v| &**v),
                    pipeline_floor,
                    pipeline_cap,
                    max_peers,
                    &mut choke_dirty,
                    &upload_limiter,
                    dht.as_ref(),
                    &blocklist,
                    &tracker_authorized,
                ).await;
                if kick && !paused {
                    drive_peer(pid, &mut peers, &mut piece_tracker, &mut chunk_tracker).await;
                }
            }
            result = webseed_tasks.join_next(), if !webseed_tasks.is_empty() => {
                match result {
                    Some(Ok(batch)) => {
                        if !generation.is_current(batch.generation) {
                            continue;
                        }
                        for vr in batch.pieces {
                            webseed_inflight.remove(&vr.piece_index);
                            process_verify_result(
                                vr, &lengths, &mut piece_tracker,
                                &mut chunk_tracker, &mut peers, &storage, &stats,
                                &mut piece_assemblies,
                            ).await;
                        }
                        if !paused {
                            drive_requests(&mut peers, &mut piece_tracker, &mut chunk_tracker).await;
                        }
                    }
                    Some(Err(e)) if !e.is_cancelled() => {
                        tracing::warn!("WebSeed worker failed: {e}");
                    }
                    Some(Err(_)) | None => {}
                }
            }
            result = write_tasks.join_next(), if !write_tasks.is_empty() => {
                match result {
                    Some(Ok(vr)) => {
                        process_verify_result(
                            vr, &lengths, &mut piece_tracker,
                            &mut chunk_tracker, &mut peers, &storage, &stats,
                            &mut piece_assemblies,
                        ).await;
                        if !paused {
                            drive_requests(&mut peers, &mut piece_tracker, &mut chunk_tracker).await;
                        }
                    }
                    Some(Err(e)) if !e.is_cancelled() => {
                        tracing::warn!("piece write/verify task failed: {e}");
                    }
                    Some(Err(_)) | None => {}
                }
            }
            result = outbound_tasks.join_next(), if !outbound_tasks.is_empty() => {
                if let Some(Err(e)) = result {
                    if !e.is_cancelled() {
                        tracing::warn!("outbound peer task failed: {e}");
                    }
                }
                if !paused {
                    drain_peer_backlog(
                        &mut priority_backlog,
                        &mut peer_backlog,
                        &mut dial_retries,
                        &mut useful_redials,
                        &mut pending_dials,
                        &mut known_addrs,
                        &useful_peers,
                        &holepunch_attempted,
                        &mut next_pid,
                        peers.len(),
                        max_peers,
                        torrent_id,
                        &registry_scope,
                        info_hash,
                        our_peer_id,
                        &peer_event_tx,
                        encryption,
                        advertise_v2,
                        !private_torrent,
                            &ext_handshake_builder,
                            &outbound_utp,
                            &p2p_proxy,
                            &mut outbound_tasks,
                            &mut outbound_aborts,
                            &blocklist,
                        );
                }
            }
            _ = tick.tick() => {
                let now = Instant::now();
                let dt = now.duration_since(last_tick).as_secs_f32().max(0.001);
                last_tick = now;
                if now.duration_since(last_keepalive) >= Duration::from_secs(90) {
                    last_keepalive = now;
                    for p in peers.values() {
                        let _ = p.cmd_tx.try_send(PeerCommand::Send(Message::KeepAlive));
                    }
                }
                let reclaimed = chunk_tracker.reclaim_stale(REQUEST_TIMEOUT);
                let mut reclaimed_requests = 0usize;
                if !reclaimed.is_empty() {
                    let mut unblocked_pieces: HashSet<u32> = HashSet::new();
                    for r in &reclaimed {
                        if let Some(p) = peers.get_mut(&r.peer) {
                            if !p.snubbing {
                                p.snubbing = true;
                                p.snub_since = Some(Instant::now());
                                let peer_cap = p.reqq_cap.unwrap_or(pipeline_cap).min(pipeline_cap);
                                let peer_floor = pipeline_floor.min(peer_cap);
                                p.max_outstanding = shrink_pipeline(p.max_outstanding, peer_floor);
                                p.delivered_window = 0;
                                p.window_start = Instant::now();
                                p.delivered_since_growth = 0;
                                p.slow_start = false;
                            }
                        }
                        unblocked_pieces.insert(r.piece);
                        for p in peers.values_mut() {
                            let before = p.outstanding.len();
                            p.outstanding
                                .retain(|&(pi, be, _)| !(pi == r.piece && be == r.begin));
                            reclaimed_requests += before - p.outstanding.len();
                        }
                    }
                    for pi in unblocked_pieces {
                        if let Ok(vpi) = lengths.validate_piece(pi) {
                            piece_tracker.clear_in_flight(vpi);
                        }
                    }
                }
                {
                    let mut to_evict: Vec<u32> = Vec::new();
                    for (&pid, p) in peers.iter() {
                        if now.duration_since(p.last_recv) > PEER_IDLE_TIMEOUT {
                            to_evict.push(pid);
                            continue;
                        }
                        if let Some(snub_at) = p.snub_since {
                            if now.duration_since(snub_at) > SNUB_EVICTION_TIMEOUT {
                                to_evict.push(pid);
                            }
                        }
                    }
                    let mut evicted_any = false;
                    for pid in to_evict {
                        if let Some(p) = peers.remove(&pid) {
                            evicted_any = true;
                            if p.downloaded_total > 0 {
                                useful_peers.insert(
                                    p.addr,
                                    p.max_outstanding.clamp(pipeline_floor, pipeline_cap),
                                );
                            }
                            let known_useful = useful_peers.contains_key(&p.addr);
                            schedule_peer_retry(
                                p.addr,
                                known_useful,
                                &mut dial_retries,
                                &mut useful_redials,
                            );
                            pex_source.retain(|_, relay| *relay != pid);
                            holepunch_attempted.remove(&p.addr);
                            let _ = p.cmd_tx.try_send(PeerCommand::Disconnect);
                            release_peer_scheduler_state(
                                pid,
                                &p.bitfield,
                                &mut piece_tracker,
                                &mut chunk_tracker,
                                &mut piece_assemblies,
                                &lengths,
                            );
                            choke_dirty = true;
                        }
                    }
                    if evicted_any {
                        refresh_peer_queue_state(
                            &mut priority_backlog,
                            &mut peer_backlog,
                            &mut dial_retries,
                            &mut useful_redials,
                            &peers,
                            &pending_dials,
                            &mut known_addrs,
                        );
                    }
                }
                if choke_dirty || now.duration_since(last_choke_eval) >= CHOKE_EVAL_INTERVAL {
                    let scheduled = now.duration_since(last_window_reset) >= CHOKE_EVAL_INTERVAL;
                    choke_dirty = false;
                    last_choke_eval = now;
                    if now.duration_since(last_optimistic_rotate) >= OPTIMISTIC_ROTATE_INTERVAL {
                        last_optimistic_rotate = now;
                        let mut interested: Vec<u32> = peers
                            .iter()
                            .filter(|(_, p)| p.peer_interested)
                            .map(|(&pid, _)| pid)
                            .collect();
                        interested.sort_unstable();
                        optimistic_pid = next_optimistic(&interested, optimistic_pid);
                    }
                    run_choke_eval(
                        &mut peers,
                        piece_tracker.is_complete(),
                        optimistic_pid,
                        scheduled,
                    );
                    if scheduled {
                        last_window_reset = now;
                    }
                }
                if !private_torrent && now.duration_since(last_pex) >= PEX_INTERVAL {
                    last_pex = now;
                    let current: HashSet<SocketAddr> = peers.values().map(|p| p.addr).collect();
                    for p in peers.values_mut() {
                        let Some(pex_id) = p.their_ut_pex_id else {
                            continue;
                        };
                        let added: Vec<SocketAddr> = current
                            .iter()
                            .filter(|a| **a != p.addr && !p.pex_sent.contains(a))
                            .take(MAX_PEX_ADDED_PER_MSG)
                            .copied()
                            .collect();
                        let dropped: Vec<SocketAddr> = p
                            .pex_sent
                            .iter()
                            .filter(|a| !current.contains(a))
                            .take(MAX_PEX_ADDED_PER_MSG)
                            .copied()
                            .collect();
                        if added.is_empty() && dropped.is_empty() {
                            continue;
                        }
                        for a in &dropped {
                            p.pex_sent.remove(a);
                        }
                        p.pex_sent.extend(added.iter().copied());
                        let payload = super::wire::extended::build_ut_pex(&added, &dropped);
                        let _ = p.cmd_tx.try_send(PeerCommand::Send(Message::Extended {
                            ext_id: pex_id,
                            payload,
                        }));
                    }
                }
                let peer_snaps = if now.duration_since(last_peer_snapshot) >= PEER_SNAPSHOT_INTERVAL {
                    let dt = now.duration_since(last_peer_snapshot).as_secs_f64().max(0.001);
                    last_peer_snapshot = now;
                    let total_pieces = lengths.total_pieces() as usize;
                    for p in peers.values_mut() {
                        let uploaded = p.uploaded_total.load(Ordering::Relaxed);
                        p.dl_speed = ((p.downloaded_total.saturating_sub(p.last_snap_downloaded))
                            as f64
                            / dt) as u64;
                        p.up_speed =
                            ((uploaded.saturating_sub(p.last_snap_uploaded)) as f64 / dt) as u64;
                        p.last_snap_downloaded = p.downloaded_total;
                        p.last_snap_uploaded = uploaded;
                    }
                    Some(
                        peers
                            .values()
                            .map(|p| {
                                let seeder = peer_bitfield_is_full(&p.bitfield, total_pieces);
                                stats::PeerSnapshot {
                                    addr: p.addr,
                                    bitfield: Arc::<[u8]>::from(p.bitfield.as_slice()),
                                    am_choking: p.am_choking,
                                    am_interested: p.am_interested,
                                    peer_choking: p.peer_choking,
                                    peer_interested: p.peer_interested,
                                    seeder,
                                    peer_id: p.peer_id.map(|id| id.0),
                                    client: p.client.clone(),
                                    downloaded: p.downloaded_total,
                                    uploaded: p.uploaded_total.load(Ordering::Relaxed),
                                    dl_speed: p.dl_speed,
                                    up_speed: p.up_speed,
                                    incoming: !p.outbound,
                                    snubbed: p.snubbing,
                                    progress: peer_bitfield_progress(&p.bitfield, total_pieces),
                                    optimistic_unchoke: p.optimistic_unchoke,
                                }
                            })
                            .collect(),
                    )
                } else {
                    None
                };
                {
                    let mut s = stats.lock();
                    let upload_dt = upload_tick.swap(0, Ordering::Relaxed);
                    bytes_this_tick.1 = upload_dt;
                    s.live_stats.update(bytes_this_tick.0, bytes_this_tick.1, dt);
                    s.live_stats.snapshot.peer_stats.live = peers.len() as u32;
                    if let Some(peer_snaps) = peer_snaps {
                        s.peers = peer_snaps;
                    }
                }
                let active_request_peers = peers
                    .values()
                    .filter(|peer| request_peer_eligible(peer))
                    .count();
                let outstanding_requests: usize =
                    peers.values().map(|peer| peer.outstanding.len()).sum();
                let snubbed_peers = peers.values().filter(|peer| peer.snubbing).count();
                let max_pipeline = peers
                    .values()
                    .map(|peer| peer.max_outstanding)
                    .max()
                    .unwrap_or(0);
                let max_effective_pipeline = peers
                    .values()
                    .filter(|peer| request_peer_eligible(peer))
                    .map(|peer| {
                        effective_request_limit(peer.max_outstanding, active_request_peers)
                    })
                    .max()
                    .unwrap_or(0);
                tracing::debug!(
                    target: "diag",
                    "TICK summary peers={} pending_dials={} known={} backlog={} endgame={} dl_bytes_tick={} ul_bytes_tick={} pending_chunks={} outstanding_requests={} active_request_peers={} snubbed_peers={} max_pipeline={} max_effective_pipeline={} reclaimed_requests={} request_budget={} dt_ms={:.0}",
                    peers.len(),
                    pending_dials.len(),
                    known_addrs.len(),
                    peer_backlog.len(),
                    chunk_tracker.endgame(),
                    bytes_this_tick.0,
                    bytes_this_tick.1,
                    chunk_tracker.pending_chunks(),
                    outstanding_requests,
                    active_request_peers,
                    snubbed_peers,
                    max_pipeline,
                    max_effective_pipeline,
                    reclaimed_requests,
                    TORRENT_REQUEST_BUDGET,
                    f64::from(dt) * 1000.0
                );
                bytes_this_tick = (0, 0);
                if !paused {
                    drain_peer_backlog(
                        &mut priority_backlog,
                        &mut peer_backlog,
                        &mut dial_retries,
                        &mut useful_redials,
                        &mut pending_dials,
                        &mut known_addrs,
                        &useful_peers,
                        &holepunch_attempted,
                        &mut next_pid,
                        peers.len(),
                        max_peers,
                        torrent_id,
                        &registry_scope,
                        info_hash,
                        our_peer_id,
                        &peer_event_tx,
                        encryption,
                        advertise_v2,
                        !private_torrent,
                            &ext_handshake_builder,
                            &outbound_utp,
                            &p2p_proxy,
                            &mut outbound_tasks,
                            &mut outbound_aborts,
                            &blocklist,
                    );
                    drive_requests(&mut peers, &mut piece_tracker, &mut chunk_tracker).await;
                    if let Some(ctx) = webseed_ctx.as_ref() {
                        schedule_webseed_workers(
                            &mut webseed_tasks,
                            &mut webseed_inflight,
                            &mut piece_tracker,
                            ctx,
                        );
                    }
                }
            }
        }
    };

    if let Some(handle) = dht_poll_handle.take() {
        handle.abort();
    }

    for (pid, peer) in peers.drain() {
        abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
        release_peer_scheduler_state(
            pid,
            &peer.bitfield,
            &mut piece_tracker,
            &mut chunk_tracker,
            &mut piece_assemblies,
            &lengths,
        );
    }

    outbound_tasks.shutdown().await;
    webseed_tasks.shutdown().await;
    webseed_inflight.clear();
    for (_, cmd_tx, _, io_abort) in peer_registry::drain_scope(torrent_id, &registry_scope) {
        abort_peer_io(io_abort.as_ref(), &cmd_tx);
    }
    pending_dials.clear();

    tracker_tasks.shutdown(true).await;

    priority_backlog.clear();
    peer_backlog.clear();
    dial_retries.clear();
    useful_redials.clear();
    useful_peers.clear();
    known_addrs.clear();
    pex_source.clear();
    holepunch_attempted.clear();

    while let Some(result) = write_tasks.join_next().await {
        match result {
            Ok(vr) => {
                process_verify_result(
                    vr,
                    &lengths,
                    &mut piece_tracker,
                    &mut chunk_tracker,
                    &mut peers,
                    &storage,
                    &stats,
                    &mut piece_assemblies,
                )
                .await;
            }
            Err(e) if !e.is_cancelled() => {
                tracing::warn!("piece write/verify task failed during shutdown: {e}");
            }
            Err(_) => {}
        }
    }

    if let Err(e) = storage.close_handles().await {
        tracing::warn!("failed to close storage handles on shutdown: {e}");
    }
    {
        let mut stats = stats.lock();
        stats.live_stats.snapshot.peer_stats.live = 0;
        stats.peers.clear();
    }
    if let Some(ack) = stop_ack {
        let _ = ack.send(());
    }
}

fn spawn_dht_poller(
    dht: Arc<super::dht::Dht>,
    info_hash: Id20,
    listen_port: u16,
    peer_tx: mpsc::Sender<PeerEnvelope>,
    generation: GenerationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let mut peers =
                dht.get_peers_stream(info_hash, Duration::from_secs(60), Some(listen_port));
            while let Some(addr) = peers.recv().await {
                let candidate_generation = generation.current();
                if peer_tx
                    .send(PeerEnvelope {
                        generation: candidate_generation,
                        candidate: PeerCandidate {
                            addr,
                            source: PeerSource::Dht,
                        },
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_secs(120)).await;
        }
    })
}

mod peer_registry {
    use super::*;
    use std::sync::LazyLock;
    use std::sync::Mutex as StdMutex;

    struct RegistryEntry {
        scope: Arc<()>,
        tx: mpsc::Sender<PeerCommand>,
        addr: SocketAddr,
        io_abort: Option<AbortHandle>,
    }

    type PeerCmdRegistry = StdMutex<HashMap<(usize, u32), RegistryEntry>>;
    static REG: LazyLock<PeerCmdRegistry> = LazyLock::new(|| StdMutex::new(HashMap::new()));

    pub fn put(
        torrent_id: usize,
        pid: u32,
        scope: &Arc<()>,
        tx: mpsc::Sender<PeerCommand>,
        addr: SocketAddr,
        io_abort: Option<AbortHandle>,
    ) {
        REG.lock().unwrap().insert(
            (torrent_id, pid),
            RegistryEntry {
                scope: scope.clone(),
                tx,
                addr,
                io_abort,
            },
        );
    }

    pub fn take(
        torrent_id: usize,
        pid: u32,
        scope: &Arc<()>,
    ) -> Option<(mpsc::Sender<PeerCommand>, SocketAddr, Option<AbortHandle>)> {
        let mut reg = REG.lock().unwrap();
        let key = (torrent_id, pid);
        if !reg
            .get(&key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.scope, scope))
        {
            return None;
        }
        reg.remove(&key)
            .map(|entry| (entry.tx, entry.addr, entry.io_abort))
    }

    pub fn remove(torrent_id: usize, pid: u32, scope: &Arc<()>) {
        let mut reg = REG.lock().unwrap();
        let key = (torrent_id, pid);
        if reg
            .get(&key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.scope, scope))
        {
            reg.remove(&key);
        }
    }

    pub fn drain_scope(
        torrent_id: usize,
        scope: &Arc<()>,
    ) -> Vec<(
        u32,
        mpsc::Sender<PeerCommand>,
        SocketAddr,
        Option<AbortHandle>,
    )> {
        let mut reg = REG.lock().unwrap();
        let keys = reg
            .iter()
            .filter_map(|(&(entry_torrent_id, pid), entry)| {
                (entry_torrent_id == torrent_id && Arc::ptr_eq(&entry.scope, scope))
                    .then_some((entry_torrent_id, pid))
            })
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter_map(|key| {
                reg.remove(&key)
                    .map(|entry| (key.1, entry.tx, entry.addr, entry.io_abort))
            })
            .collect()
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_outbound_peer(
    torrent_id: usize,
    pid: u32,
    addr: SocketAddr,
    registry_scope: Arc<()>,
    info_hash: Id20,
    our_peer_id: Id20,
    event_tx: mpsc::Sender<(u32, PeerEvent)>,
    encryption: crate::peer::EncryptionPolicy,
    advertise_v2: bool,
    advertise_dht: bool,
    ext_handshake_builder: Option<crate::peer::ExtHandshakeBuilder>,
    utp: Option<Arc<UtpSocket>>,
    prefer_utp: bool,
    known_useful: bool,
    proxy: Option<risuko_http::ProxyConnector>,
) {
    let spawn = SpawnPeer {
        addr,
        info_hash,
        our_peer_id,
        connect_timeout: if known_useful {
            USEFUL_PEER_CONNECT_TIMEOUT
        } else {
            OUTBOUND_CONNECT_TIMEOUT
        },
        // Post-handshake piece IO; MSE/handshake phases use connect_timeout
        read_timeout: Duration::from_secs(120),
        encryption,
        advertise_v2,
        advertise_dht,
        ext_handshake_builder,
        proxy,
    };
    if known_useful {
        tracing::debug!("redialing useful peer {addr} TCP-first with µTP fallback");
    }
    let connect_result = if prefer_utp {
        connect_prefer_utp(spawn, utp).await
    } else {
        connect_with_utp_fallback(spawn, utp).await
    };
    match connect_result {
        Ok((handle, mut rx)) => {
            peer_registry::put(
                torrent_id,
                pid,
                &registry_scope,
                handle.tx.clone(),
                handle.addr,
                Some(handle.io_abort),
            );
            while let Some(ev) = rx.recv().await {
                if event_tx.send((pid, ev)).await.is_err() {
                    break;
                }
            }
            peer_registry::remove(torrent_id, pid, &registry_scope);
        }
        Err(e) => {
            let _ = event_tx
                .send((
                    pid,
                    PeerEvent::Disconnected {
                        reason: format!("connect: {e}"),
                    },
                ))
                .await;
        }
    }
}

fn fast_bit(reserved: &[u8; 8]) -> bool {
    let (b, m) = super::wire::handshake::reserved::FAST;
    reserved[b] & m != 0
}

fn dht_bit(reserved: &[u8; 8]) -> bool {
    let (b, m) = super::wire::handshake::reserved::DHT;
    reserved[b] & m != 0
}

fn allowed_fast_set(info_hash: &Id20, addr: SocketAddr, total_pieces: u32) -> Vec<u32> {
    if total_pieces == 0 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(MAX_FAST_PIECES.min(total_pieces as usize));
    let mut seed = Vec::with_capacity(36);
    match addr {
        SocketAddr::V4(v4) => {
            let octets = v4.ip().octets();
            seed.extend_from_slice(&[octets[0], octets[1], octets[2], 0]);
        }
        SocketAddr::V6(v6) => seed.extend_from_slice(&v6.ip().octets()),
    }
    seed.extend_from_slice(info_hash.as_bytes());
    while out.len() < MAX_FAST_PIECES.min(total_pieces as usize) {
        let mut h = Sha1::new();
        h.update(&seed);
        seed = h.finalize().to_vec();
        for chunk in seed.chunks_exact(4) {
            let index = u32::from_be_bytes(chunk.try_into().expect("sha1 chunk")) % total_pieces;
            if !out.contains(&index) {
                out.push(index);
                if out.len() == MAX_FAST_PIECES.min(total_pieces as usize) {
                    break;
                }
            }
        }
    }
    out
}

fn initial_availability_message(
    bitfield: &[u8],
    total_pieces: u32,
    supports_fast: bool,
) -> Option<Message> {
    if !supports_fast {
        return should_send_initial_bitfield(bitfield)
            .then(|| Message::Bitfield(Bytes::copy_from_slice(bitfield)));
    }

    let has_any = bitfield.iter().any(|byte| *byte != 0);
    if !has_any {
        return Some(Message::HaveNone);
    }
    let has_all = (0..total_pieces as usize).all(|index| {
        bitfield
            .get(index / 8)
            .is_some_and(|byte| byte & (1 << (7 - (index % 8))) != 0)
    });
    if has_all {
        Some(Message::HaveAll)
    } else {
        Some(Message::Bitfield(Bytes::copy_from_slice(bitfield)))
    }
}

async fn send_initial_availability(
    cmd_tx: &mpsc::Sender<PeerCommand>,
    bitfield: &[u8],
    total_pieces: u32,
    supports_fast: bool,
) {
    if let Some(message) = initial_availability_message(bitfield, total_pieces, supports_fast) {
        let _ = cmd_tx.send(PeerCommand::Send(message)).await;
    }
}

fn accept_initial_availability(peer: &mut Peer, availability: InitialAvailability) -> bool {
    if peer.initial_availability.is_some() {
        return false;
    }
    peer.initial_availability = Some(availability);
    true
}

async fn send_allowed_fast(peer: &mut Peer, info_hash: &Id20, total_pieces: u32) {
    if !peer.supports_fast {
        return;
    }
    for index in allowed_fast_set(info_hash, peer.addr, total_pieces) {
        peer.sent_allowed_fast.insert(index);
        let _ = peer
            .cmd_tx
            .send(PeerCommand::Send(Message::AllowedFast(index)))
            .await;
    }
}

async fn send_fast_reject(peer: &Peer, index: u32, begin: u32, length: u32) {
    if peer.supports_fast {
        let _ = peer
            .cmd_tx
            .send(PeerCommand::Send(Message::RejectRequest {
                index,
                begin,
                length,
            }))
            .await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn adopt_inbound_peer(
    pid: u32,
    addr: SocketAddr,
    cmd_tx: mpsc::Sender<PeerCommand>,
    mut event_rx: mpsc::Receiver<PeerEvent>,
    fwd_tx: mpsc::Sender<(u32, PeerEvent)>,
    peers: &mut HashMap<u32, Peer>,
    lengths: &Lengths,
    piece_tracker: &mut PieceTracker,
    pipeline_floor: usize,
    pipeline_cap: usize,
    reserved: [u8; 8],
    peer_id: Option<Id20>,
    io_abort: AbortHandle,
    info_hash: Id20,
) {
    let supports_fast = fast_bit(&reserved);
    let mut peer = Peer::connected(
        addr,
        cmd_tx.clone(),
        lengths.piece_bitfield_bytes(),
        pipeline_floor,
        pipeline_cap,
        false,
        supports_fast,
        peer_id,
    );
    peer.io_abort = Some(io_abort);
    peers.insert(pid, peer);
    let bf = piece_tracker.bitfield();
    send_initial_availability(&cmd_tx, &bf, lengths.total_pieces(), supports_fast).await;
    if let Some(peer) = peers.get_mut(&pid) {
        send_allowed_fast(peer, &info_hash, lengths.total_pieces()).await;
    }
    tokio::spawn(async move {
        while let Some(ev) = event_rx.recv().await {
            if fwd_tx.send((pid, ev)).await.is_err() {
                break;
            }
        }
    });
}

#[allow(clippy::too_many_arguments)]
async fn process_peer_event(
    torrent_id: usize,
    info_hash: Id20,
    private_torrent: bool,
    pid: u32,
    ev: PeerEvent,
    registry_scope: &Arc<()>,
    paused: bool,
    peers: &mut HashMap<u32, Peer>,
    piece_tracker: &mut PieceTracker,
    chunk_tracker: &mut ChunkTracker,
    piece_assemblies: &mut HashMap<u32, PieceAssembly>,
    lengths: &Lengths,
    storage: &Arc<FilesystemStorage>,
    stats: &Arc<Mutex<TorrentStats>>,
    bytes_this_tick: &mut (u64, u64),
    upload_tick: &Arc<AtomicU64>,
    write_tasks: &mut tokio::task::JoinSet<VerifyResult>,
    pending_dials: &mut HashMap<u32, SocketAddr>,
    outbound_aborts: &mut HashMap<u32, AbortHandle>,
    known_addrs: &mut HashSet<SocketAddr>,
    priority_backlog: &mut VecDeque<SocketAddr>,
    peer_backlog: &mut VecDeque<SocketAddr>,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
    useful_peers: &mut HashMap<SocketAddr, usize>,
    // BEP-55: addr gossiped via PEX -> the relay pid that gossiped it
    pex_source: &mut HashMap<SocketAddr, u32>,
    // BEP-55: targets we've already asked a relay to rendezvous
    holepunch_attempted: &mut HashSet<SocketAddr>,
    peer_src_tx: &mpsc::Sender<PeerEnvelope>,
    generation: &GenerationToken,
    verifier: &PieceVerifier,
    info_bytes: &Arc<Vec<u8>>,
    hash_tables: Option<&[MerkleProofTable]>,
    pipeline_floor: usize,
    pipeline_cap: usize,
    max_peers: usize,
    choke_dirty: &mut bool,
    upload_limiter: &Option<Arc<crate::limiter::UploadLimiter>>,
    dht: Option<&Arc<crate::dht::Dht>>,
    blocklist: &RwLock<BlockList>,
    tracker_authorized: &HashSet<SocketAddr>,
) -> bool {
    if paused {
        match ev {
            PeerEvent::Handshook { .. } => {
                pending_dials.remove(&pid);
                cancel_peer_io(pid, None, outbound_aborts, torrent_id, registry_scope);
            }
            PeerEvent::Disconnected { .. } => {
                pending_dials.remove(&pid);
                outbound_aborts.remove(&pid);
                peer_registry::remove(torrent_id, pid, registry_scope);
            }
            PeerEvent::Message(_) => {}
        }
        return false;
    }

    let mut kick = false;
    match ev {
        PeerEvent::Handshook {
            encrypted,
            reserved,
            peer_id,
            ..
        } => {
            if !peers.contains_key(&pid) {
                if let Some((cmd_tx, registry_addr, io_abort)) =
                    peer_registry::take(torrent_id, pid, registry_scope)
                {
                    let addr = pending_dials.remove(&pid).unwrap_or(registry_addr);

                    if blocklist.read().contains(addr.ip())
                        || (private_torrent && !tracker_authorized.contains(&addr))
                    {
                        reject_handshook_peer(
                            pid,
                            &cmd_tx,
                            io_abort,
                            outbound_aborts,
                            torrent_id,
                            registry_scope,
                        );
                        known_addrs.remove(&addr);
                        return false;
                    }

                    if peers.len() >= max_peers {
                        let known_useful = useful_peers.contains_key(&addr);
                        schedule_peer_retry(addr, known_useful, dial_retries, useful_redials);
                        reject_handshook_peer(
                            pid,
                            &cmd_tx,
                            io_abort,
                            outbound_aborts,
                            torrent_id,
                            registry_scope,
                        );
                        refresh_peer_queue_state(
                            priority_backlog,
                            peer_backlog,
                            dial_retries,
                            useful_redials,
                            peers,
                            pending_dials,
                            known_addrs,
                        );
                        tracing::debug!(
                            "peer {addr} handshook after cap {max_peers} filled; scheduled retry"
                        );
                        return false;
                    }
                    let initial_window = useful_peers
                        .get(&addr)
                        .copied()
                        .unwrap_or(pipeline_floor)
                        .clamp(pipeline_floor, pipeline_cap);
                    if initial_window > pipeline_floor {
                        tracing::debug!(
                            target: "diag",
                            "pipeline RESUME pid={pid} addr={addr} {pipeline_floor}->{initial_window}"
                        );
                    }
                    tracing::debug!(
                        "peer connected: {addr} (encrypted={encrypted}, peers={}/{max_peers})",
                        peers.len() + 1
                    );
                    let supports_fast = fast_bit(&reserved);
                    let mut peer = Peer::connected(
                        addr,
                        cmd_tx.clone(),
                        lengths.piece_bitfield_bytes(),
                        initial_window,
                        pipeline_cap,
                        true,
                        supports_fast,
                        Some(peer_id),
                    );
                    peer.io_abort = io_abort;
                    peers.insert(pid, peer);
                    let bf = piece_tracker.bitfield();
                    send_initial_availability(&cmd_tx, &bf, lengths.total_pieces(), supports_fast)
                        .await;
                    if let Some(peer) = peers.get_mut(&pid) {
                        send_allowed_fast(peer, &info_hash, lengths.total_pieces()).await;
                        if !private_torrent {
                            if let Some(port) =
                                dht.and_then(|node| node.local_port_for(peer.addr.is_ipv6()))
                            {
                                if dht_bit(&reserved) {
                                    let _ = peer
                                        .cmd_tx
                                        .send(PeerCommand::Send(Message::Port(port)))
                                        .await;
                                }
                            }
                        }
                    }
                }
            }
        }
        PeerEvent::Message(msg) => {
            let Some(peer) = peers.get_mut(&pid) else {
                return false;
            };
            peer.last_recv = Instant::now();
            match &msg {
                Message::Piece { index, begin, data } => tracing::trace!(
                    target: "diag",
                    "RX {} Piece(i={index} b={begin} len={}) am_interested={} peer_choking={} am_choking={}",
                    peer.addr,
                    data.len(),
                    peer.am_interested,
                    peer.peer_choking,
                    peer.am_choking
                ),
                _ if tracing::enabled!(target: "diag", tracing::Level::DEBUG) => {
                    let kind = match &msg {
                        Message::Have { piece_index } => format!("Have({piece_index})"),
                        Message::Bitfield(b) => {
                            let set: u32 = b.iter().map(|x| x.count_ones()).sum();
                            format!("Bitfield(len={} set_bits={})", b.len(), set)
                        }
                        Message::Unknown { id, payload } => {
                            format!("Unknown(id={id} len={})", payload.len())
                        }
                        other => format!("{other:?}"),
                    };
                    tracing::debug!(
                        target: "diag",
                        "RX {} {kind} am_interested={} peer_choking={} am_choking={}",
                        peer.addr, peer.am_interested, peer.peer_choking, peer.am_choking
                    );
                }
                _ => {}
            }
            match msg {
                Message::Choke => {
                    peer.peer_choking = true;
                    peer.delivered_since_growth = 0;
                }
                Message::Unchoke => {
                    peer.peer_choking = false;
                    kick = true;
                }
                Message::Interested => {
                    peer.peer_interested = true;
                    *choke_dirty = true;
                }
                Message::NotInterested => peer.peer_interested = false,
                Message::Have { piece_index } => {
                    if let Ok(vpi) = lengths.validate_piece(piece_index) {
                        piece_tracker.update_peer_have(&mut peer.bitfield, vpi);
                    }
                    send_interested_if_useful(peer, piece_tracker).await;
                    kick = true;
                }
                Message::Bitfield(bytes) => {
                    if peer.supports_fast
                        && !accept_initial_availability(peer, InitialAvailability::Bitfield)
                    {
                        let _ = peer.cmd_tx.try_send(PeerCommand::Disconnect);
                        return false;
                    }
                    piece_tracker.replace_peer_bitfield(&mut peer.bitfield, &bytes);
                    send_interested_if_useful(peer, piece_tracker).await;
                    kick = true;
                }
                Message::HaveAll => {
                    if !peer.supports_fast {
                        let _ = peer.cmd_tx.try_send(PeerCommand::Disconnect);
                        return false;
                    }
                    if !accept_initial_availability(peer, InitialAvailability::HaveAll) {
                        let _ = peer.cmd_tx.try_send(PeerCommand::Disconnect);
                        return false;
                    }
                    let all = vec![0xff; peer.bitfield.len()];
                    piece_tracker.replace_peer_bitfield(&mut peer.bitfield, &all);
                    send_interested_if_useful(peer, piece_tracker).await;
                    kick = true;
                }
                Message::HaveNone => {
                    if !peer.supports_fast {
                        let _ = peer.cmd_tx.try_send(PeerCommand::Disconnect);
                        return false;
                    }
                    if !accept_initial_availability(peer, InitialAvailability::HaveNone) {
                        let _ = peer.cmd_tx.try_send(PeerCommand::Disconnect);
                        return false;
                    }
                    piece_tracker.replace_peer_bitfield(&mut peer.bitfield, &[]);
                }
                Message::RejectRequest {
                    index,
                    begin,
                    length,
                } => {
                    if !peer.supports_fast {
                        let _ = peer.cmd_tx.try_send(PeerCommand::Disconnect);
                        return false;
                    }
                    if let Some(req_idx) = peer
                        .outstanding
                        .iter()
                        .position(|&(p, b, l)| p == index && b == begin && l == length)
                    {
                        peer.outstanding.swap_remove(req_idx);
                        if let Ok(vpi) = lengths.validate_piece(index) {
                            chunk_tracker.reject_chunk(vpi, begin / super::core::CHUNK_SIZE, pid);
                            piece_tracker.clear_in_flight(vpi);
                        }
                        peer.consecutive_rejects = peer.consecutive_rejects.saturating_add(1);
                        let peer_cap = peer.reqq_cap.unwrap_or(pipeline_cap).min(pipeline_cap);
                        let peer_floor = pipeline_floor.min(peer_cap);
                        peer.max_outstanding = shrink_pipeline(peer.max_outstanding, peer_floor);
                        peer.delivered_since_growth = 0;
                        peer.slow_start = false;
                        if peer.consecutive_rejects >= REJECT_SNUB_THRESHOLD && !peer.snubbing {
                            peer.snubbing = true;
                            peer.snub_since = Some(Instant::now());
                            peer.delivered_window = 0;
                            peer.window_start = Instant::now();
                        } else if !peer.snubbing {
                            kick = true;
                        }
                    } else {
                        let _ = peer.cmd_tx.try_send(PeerCommand::Disconnect);
                        return false;
                    }
                }
                Message::SuggestPiece(index) => {
                    if !peer.supports_fast {
                        let _ = peer.cmd_tx.try_send(PeerCommand::Disconnect);
                        return false;
                    }
                    if lengths.validate_piece(index).is_ok()
                        && !piece_tracker.has_local(lengths.validate_piece(index).unwrap())
                        && peer.suggested_pieces.len() < MAX_SUGGESTED_PIECES
                    {
                        peer.suggested_pieces.insert(index);
                        kick = true;
                    }
                }
                Message::AllowedFast(index) => {
                    if !peer.supports_fast {
                        let _ = peer.cmd_tx.try_send(PeerCommand::Disconnect);
                        return false;
                    }
                    if lengths.validate_piece(index).is_ok()
                        && peer.remote_allowed_fast.len() < MAX_SUGGESTED_PIECES
                    {
                        peer.remote_allowed_fast.insert(index);
                        kick = true;
                    }
                }
                Message::Request {
                    index,
                    begin,
                    length,
                } => {
                    let request_key = (index, begin, length);
                    if peer.responded_requests.contains(&request_key) {
                        return false;
                    }
                    if peer.responded_requests.len() >= 4096 {
                        peer.responded_requests.clear();
                    }
                    peer.responded_requests.insert(request_key);
                    let Ok(vpi) = lengths.validate_piece(index) else {
                        send_fast_reject(peer, index, begin, length).await;
                        return false;
                    };
                    let fast_allowed =
                        peer.supports_fast && peer.sent_allowed_fast.contains(&index);
                    if !piece_tracker.has_local(vpi) || (peer.am_choking && !fast_allowed) {
                        send_fast_reject(peer, index, begin, length).await;
                        return false;
                    }
                    if length == 0 || length > 1024 * 1024 {
                        send_fast_reject(peer, index, begin, length).await;
                        return false;
                    }
                    let piece_len = lengths.piece_length_of(vpi) as u64;
                    if (begin as u64).saturating_add(length as u64) > piece_len {
                        send_fast_reject(peer, index, begin, length).await;
                        return false;
                    }
                    let offset = lengths.piece_offset(vpi) + begin as u64;
                    // Offload the disk read + send to a task; awaiting on the main loop here would stall every download peer for the duration of every upload `read_at`
                    let storage = storage.clone();
                    let cmd_tx = peer.cmd_tx.clone();
                    let stats = stats.clone();
                    let upload_tick = Arc::clone(upload_tick);
                    let peer_uploaded = Arc::clone(&peer.uploaded_window);
                    let peer_uploaded_total = Arc::clone(&peer.uploaded_total);
                    let upload_len = length as u64;
                    let limiter = upload_limiter.clone();
                    let supports_fast = peer.supports_fast;
                    tokio::spawn(async move {
                        if let Some(l) = &limiter {
                            l.acquire(upload_len).await;
                        }
                        let mut buf = vec![0u8; length as usize];
                        if storage.read_at(offset, &mut buf).await.is_err() {
                            if supports_fast {
                                let _ = cmd_tx
                                    .send(PeerCommand::Send(Message::RejectRequest {
                                        index,
                                        begin,
                                        length,
                                    }))
                                    .await;
                            }
                            return;
                        }
                        if cmd_tx
                            .send(PeerCommand::Send(Message::Piece {
                                index,
                                begin,
                                data: Bytes::from(buf),
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        // Only credit the upload after both the disk read and the send to the peer succeeded; crediting before would over-report on read errors or when the peer's command channel was closed mid-flight
                        upload_tick.fetch_add(upload_len, Ordering::Relaxed);
                        peer_uploaded.fetch_add(upload_len, Ordering::Relaxed);
                        peer_uploaded_total.fetch_add(upload_len, Ordering::Relaxed);
                        stats.lock().uploaded_bytes += upload_len;
                    });
                }
                Message::Piece { index, begin, data } => {
                    let Ok(vpi) = lengths.validate_piece(index) else {
                        let _ = peer.cmd_tx.try_send(PeerCommand::Disconnect);
                        return false;
                    };
                    // Only accept pieces that match an outstanding request; reject unsolicited or mismatched frames
                    let requested = peer.outstanding.iter().position(|&(p, b, l)| {
                        p == index && b == begin && l as usize == data.len()
                    });
                    let Some(req_idx) = requested else {
                        let late_key =
                            (index, begin, u32::try_from(data.len()).unwrap_or(u32::MAX));
                        if peer.canceled_requests.remove(&late_key) {
                            return false;
                        }
                        let _ = peer.cmd_tx.try_send(PeerCommand::Disconnect);
                        return false;
                    };
                    // Reject if data extends past the piece boundary
                    let piece_len = lengths.piece_length_of(vpi);
                    if (begin as u64).saturating_add(data.len() as u64) > piece_len as u64 {
                        let _ = peer.cmd_tx.try_send(PeerCommand::Disconnect);
                        return false;
                    }
                    peer.outstanding.swap_remove(req_idx);
                    // Clear the snubbing flag the moment any chunk arrives: this makes snubbing a soft, self-healing back-off rather than a permanent demotion, so a peer that timed out earlier this session rejoins the request rotation as soon as it proves it can still deliver bytes
                    peer.snubbing = false;
                    peer.snub_since = None;
                    peer.consecutive_rejects = 0;
                    peer.downloaded_window += data.len() as u64;
                    peer.downloaded_total += data.len() as u64;
                    peer.delivered_window += 1;
                    peer.delivered_since_growth = peer.delivered_since_growth.saturating_add(1);
                    let peer_cap = peer.reqq_cap.unwrap_or(pipeline_cap).min(pipeline_cap);
                    let peer_floor = pipeline_floor.min(peer_cap);
                    let window = peer.window_start.elapsed();
                    match pipeline_adjustment(
                        peer.slow_start,
                        peer.delivered_since_growth,
                        peer.delivered_window,
                        window,
                        peer_floor,
                        peer_cap,
                        peer.max_outstanding,
                    ) {
                        Some(PipelineAdjustment::SlowStart { target, finished }) => {
                            tracing::debug!(
                                target: "diag",
                                "pipeline SLOW_START pid={pid} addr={} {}->{target}",
                                peer.addr, peer.max_outstanding
                            );
                            peer.max_outstanding = target;
                            peer.delivered_since_growth = 0;
                            if finished {
                                peer.slow_start = false;
                                peer.delivered_window = 0;
                                peer.window_start = Instant::now();
                            }
                        }
                        Some(PipelineAdjustment::Rate { target }) => {
                            let current = peer.max_outstanding;
                            if target != current {
                                tracing::debug!(
                                    target: "diag",
                                    "pipeline TARGET pid={pid} addr={} {current}->{target}",
                                    peer.addr
                                );
                            }
                            peer.max_outstanding = target;
                            peer.delivered_since_growth = 0;
                            peer.delivered_window = 0;
                            peer.window_start = Instant::now();
                        }
                        None => {}
                    }
                    if piece_tracker.has_local(vpi) {
                        kick = true;
                        return kick;
                    }

                    // Accumulate the chunk in an in-memory piece buffer instead of hitting disk per chunk; the previous `storage.write_at(...).await` here serialised every download peer through one disk write per 16 KiB chunk
                    let assembly = piece_assemblies
                        .entry(index)
                        .or_insert_with(|| PieceAssembly {
                            buf: vec![0u8; piece_len as usize],
                            received_chunks: HashSet::new(),
                            received_bytes: 0,
                            expected_bytes: piece_len,
                            completed: false,
                        });
                    let chunk_index = begin / super::core::CHUNK_SIZE;
                    let begin_usz = begin as usize;
                    let end_usz = begin_usz + data.len();
                    let chunk_len = data.len();
                    let accepted = !assembly.completed
                        && end_usz <= assembly.buf.len()
                        && assembly.received_chunks.insert(chunk_index);
                    if accepted {
                        assembly.buf[begin_usz..end_usz].copy_from_slice(&data);
                        assembly.received_bytes += chunk_len as u32;
                        bytes_this_tick.0 += chunk_len as u64;
                    }
                    if accepted && chunk_tracker.endgame() {
                        cancel_outstanding(peers, pid, index, begin, chunk_len as u32);
                    }
                    let cinfo = super::core::ChunkInfo {
                        piece_index: vpi,
                        chunk_index: begin / super::core::CHUNK_SIZE,
                        absolute_index: 0,
                        size: data.len() as u32,
                        offset: begin,
                    };
                    let piece_done = chunk_tracker.mark_received(cinfo);
                    kick = true;
                    if piece_done {
                        let Some(assembly) = piece_assemblies.get_mut(&index) else {
                            return kick;
                        };
                        if assembly.completed {
                            return kick;
                        }
                        if assembly.received_bytes != assembly.expected_bytes {
                            // Missing bytes from a peer that disconnected before all chunks arrived: hash will fail anyway, so just reset and re-request
                            piece_assemblies.remove(&index);
                            chunk_tracker.reset_piece(vpi);
                            maybe_clear_endgame(chunk_tracker);
                            return kick;
                        }
                        assembly.completed = true;
                        let buf = std::mem::take(&mut assembly.buf);
                        let poff = lengths.piece_offset(vpi);
                        let storage = storage.clone();
                        let verifier = verifier.clone();
                        write_tasks.spawn(async move {
                            let buf: bytes::Bytes = buf.into();
                            let bytes_for_verify = buf.clone();
                            let verify_handle = tokio::task::spawn_blocking(move || {
                                verifier.verify(index, &bytes_for_verify).is_ok()
                            });
                            // If the write fails (ENOSPC, EIO, …) the on-disk data is incomplete — signal that to the torrent loop so it doesn't mark the piece local
                            let write_failed = storage.write_at_owned(poff, buf).await.is_err();
                            let verify_ok = verify_handle.await.unwrap_or(false);
                            VerifyResult {
                                piece_index: index,
                                write_failed,
                                verify_ok,
                            }
                        });
                    }
                }
                Message::HashRequest {
                    pieces_root,
                    base_layer,
                    index,
                    length,
                    proof_layers,
                } => {
                    let response = build_hash_response(
                        hash_tables,
                        pieces_root,
                        base_layer,
                        index,
                        length,
                        proof_layers,
                    );
                    let _ = peer.cmd_tx.try_send(PeerCommand::Send(response));
                }
                Message::Hashes { .. } | Message::HashReject { .. } => {
                    // Discard: no outstanding HASH_REQUEST to correlate
                }
                // BEP-10 extended messages we handle: the handshake (`ext_id == 0`) records the peer's `ut_metadata` id so later REQUESTs validate; `ut_metadata` REQUESTs (`ext_id == OUR_UT_METADATA_ID`) serve a 16 KiB block of our raw info dict or REJECT out-of-range pieces; `ut_pex` (`ext_id == OUR_UT_PEX_ID`) is BEP-11 peer exchange feeding gossiped peers into the dial path
                Message::Extended { ext_id, payload } => {
                    if ext_id == EXT_HANDSHAKE_ID {
                        if let Some(peer_ext) = ExtHandshake::decode(&payload) {
                            peer.their_ut_metadata_id = peer_ext.ut_metadata_id();
                            peer.their_ut_holepunch_id = peer_ext.ut_holepunch_id();
                            peer.their_ut_pex_id = peer_ext.ut_pex_id();
                            if peer.client.is_none() {
                                peer.client = peer_ext.client;
                            }

                            if let Some(reqq) = peer_ext.reqq {
                                let reqq_cap = (reqq as usize).max(1).min(pipeline_cap);
                                if reqq_cap < peer.max_outstanding {
                                    tracing::debug!(
                                        target: "diag",
                                        "pipeline REQQ pid={pid} addr={} {}->{reqq_cap} (peer reqq={reqq})",
                                        peer.addr, peer.max_outstanding
                                    );
                                    peer.max_outstanding = reqq_cap;
                                    peer.delivered_since_growth = 0;
                                }
                                peer.reqq_cap = Some(reqq_cap);
                                peer.slow_start =
                                    peer.max_outstanding < reqq_cap.min(PIPELINE_SLOW_START_CAP);
                            }
                        }
                    } else if ext_id == OUR_UT_METADATA_ID {
                        serve_ut_metadata(peer, &payload, info_bytes);
                    } else if ext_id == OUR_UT_PEX_ID && !private_torrent {
                        if let Some(update) = super::wire::extended::parse_ut_pex_full(&payload) {
                            for addr in update.dropped {
                                pex_source.remove(&addr);
                                priority_backlog.retain(|queued| *queued != addr);
                                peer_backlog.retain(|queued| *queued != addr);
                                dial_retries.retain(|(queued, _)| *queued != addr);
                                useful_redials.retain(|(queued, _)| *queued != addr);
                                if !peers.values().any(|peer| peer.addr == addr)
                                    && !pending_dials.values().any(|queued| *queued == addr)
                                {
                                    known_addrs.remove(&addr);
                                }
                            }
                            for (addr, _flags) in update.added {
                                if pex_source.len() < MAX_PEX_SOURCE_ENTRIES
                                    || pex_source.contains_key(&addr)
                                {
                                    pex_source.insert(addr, pid);
                                }
                                let _ = peer_src_tx.try_send(PeerEnvelope {
                                    generation: generation.current(),
                                    candidate: PeerCandidate {
                                        addr,
                                        source: PeerSource::Pex,
                                    },
                                });
                            }
                        }
                    } else if ext_id == OUR_UT_HOLEPUNCH_ID && !private_torrent {
                        // BEP-55 hole punching
                        let from_addr = peer.addr;
                        let from_hp_id = peer.their_ut_holepunch_id;
                        let from_cmd = peer.cmd_tx.clone();
                        if let Some(hp) = parse_holepunch(&payload) {
                            handle_holepunch(
                                hp,
                                from_addr,
                                from_hp_id,
                                &from_cmd,
                                peers,
                                pending_dials,
                                priority_backlog,
                                peer_backlog,
                                dial_retries,
                                useful_redials,
                                known_addrs,
                                holepunch_attempted,
                                blocklist,
                            );
                        }
                    }
                }
                Message::Port(port) => {
                    if !private_torrent {
                        if let (Some(dht), true) = (dht, port != 0) {
                            dht.add_node(SocketAddr::new(peer.addr.ip(), port));
                        }
                    }
                }
                _ => {}
            }
        }
        PeerEvent::Disconnected { reason } => {
            let pending_addr = pending_dials.remove(&pid);
            let was_pending_dial = pending_addr.is_some();
            let dead = peers.remove(&pid);
            let addr = pending_addr.or_else(|| dead.as_ref().map(|peer| peer.addr));
            let useful_bytes = dead.as_ref().map_or(0, |peer| peer.downloaded_total);
            let useful_window = dead
                .as_ref()
                .map_or(pipeline_floor, |peer| peer.max_outstanding);

            if let Some(a) = addr {
                tracing::debug!("peer {a} disconnected: {reason}");
                pex_source.retain(|_, relay| *relay != pid);
                if was_pending_dial && !private_torrent {
                    try_initiate_holepunch(a, pex_source, holepunch_attempted, peers);
                }

                if useful_bytes > 0 && (!private_torrent || tracker_authorized.contains(&a)) {
                    useful_peers.insert(a, useful_window.clamp(pipeline_floor, pipeline_cap));
                }
                let known_useful = useful_bytes > 0 || useful_peers.contains_key(&a);
                if !private_torrent || tracker_authorized.contains(&a) {
                    schedule_peer_retry(a, known_useful, dial_retries, useful_redials);
                } else {
                    known_addrs.remove(&a);
                }
                if known_useful && (!private_torrent || tracker_authorized.contains(&a)) {
                    tracing::debug!(
                        "scheduled useful peer {a} redial in {}s (delivered {useful_bytes} bytes)",
                        USEFUL_PEER_REDIAL_DELAY.as_secs()
                    );
                }
            }

            peer_registry::remove(torrent_id, pid, registry_scope);
            outbound_aborts.remove(&pid);
            if let Some(dead) = dead {
                *choke_dirty = true;
                release_peer_scheduler_state(
                    pid,
                    &dead.bitfield,
                    piece_tracker,
                    chunk_tracker,
                    piece_assemblies,
                    lengths,
                );
            }
            refresh_peer_queue_state(
                priority_backlog,
                peer_backlog,
                dial_retries,
                useful_redials,
                peers,
                pending_dials,
                known_addrs,
            );
        }
    }
    kick
}

#[allow(clippy::too_many_arguments)]
async fn process_verify_result(
    vr: VerifyResult,
    lengths: &Lengths,
    piece_tracker: &mut PieceTracker,
    chunk_tracker: &mut ChunkTracker,
    peers: &mut HashMap<u32, Peer>,
    storage: &Arc<FilesystemStorage>,
    stats: &Arc<Mutex<TorrentStats>>,
    piece_assemblies: &mut HashMap<u32, PieceAssembly>,
) {
    let Ok(vpi) = lengths.validate_piece(vr.piece_index) else {
        return;
    };
    piece_tracker.clear_in_flight(vpi);
    piece_assemblies.remove(&vr.piece_index);
    if vr.write_failed {
        tracing::warn!(
            "piece {} disk write failed; will re-request",
            vr.piece_index
        );
        cancel_piece_outstanding(peers, vr.piece_index);
        chunk_tracker.reset_piece(vpi);
        maybe_clear_endgame(chunk_tracker);
        return;
    }
    // Verification already completed in the per-piece write task (see write_tasks.spawn in process_peer_event); hashing here would re-serialise every piece completion through the torrent loop's select arm and stall all peer events for the hash duration
    if vr.verify_ok {
        let became_local = mark_verified_piece_local(piece_tracker, vpi);
        // Piece is verified + on disk; its dense chunk state is no longer needed, and dropping keeps `release_peer` and `pending_chunks` bounded by the working set of in-flight pieces rather than the torrent's lifetime piece count
        chunk_tracker.forget_piece(vpi);
        // Tell every peer to stop sending the rest of this piece; without it, endgame duplicates of the remaining chunks keep arriving (silently dropped by the has_local guard) and saturate downstream during the final pieces
        cancel_piece_outstanding(peers, vr.piece_index);
        broadcast_have(peers, vr.piece_index).await;
        let mut s = stats.lock();
        if became_local {
            add_piece_progress(&mut s, lengths, storage.layout(), vpi);
        }
        s.finished = piece_tracker.is_complete();
    } else {
        tracing::debug!("piece {} verify failed", vr.piece_index);
        // Same reasoning as write_failed: stale chunks from the prior attempt would interleave with the re-request
        cancel_piece_outstanding(peers, vr.piece_index);
        chunk_tracker.reset_piece(vpi);
        maybe_clear_endgame(chunk_tracker);
    }
}

fn build_webseed_client(
    proxy: Option<&risuko_http::ProxyConnector>,
) -> Result<risuko_http::Client, String> {
    let builder = risuko_http::Client::builder()
        .timeout(WEBSEED_TIMEOUT)
        .connect_timeout(Duration::from_secs(8))
        .redirect(risuko_http::Policy::limited(3))
        .pool_max_idle_per_host(WEBSEED_MAX_WORKERS);
    #[cfg(not(test))]
    let builder = builder.direct_address_filter(super::webseed::is_allowed_destination);
    let mut builder = builder
        .gzip(false)
        .brotli(false)
        .deflate(false);
    if let Some(proxy) = proxy {
        if let Some(route) = proxy.proxy() {
            builder = builder.proxy(route.clone());
        }
        if let Some(bypass) = proxy.no_proxy() {
            builder = builder.no_proxy(bypass.clone());
        }
    }
    builder.build().map_err(|e| e.to_string())
}

fn webseed_piece_spans(layout: &FileSet, offset: u64, len: u64) -> Vec<(usize, u64, u64)> {
    layout
        .spans_for(offset, len)
        .map(|span| (span.file_index, span.file_offset, span.len))
        .collect()
}

fn webseed_failed_batch(generation: u64, piece_indices: &[u32]) -> WebSeedBatchResult {
    WebSeedBatchResult {
        generation,
        pieces: piece_indices
            .iter()
            .copied()
            .map(|piece_index| VerifyResult {
                piece_index,
                write_failed: false,
                verify_ok: false,
            })
            .collect(),
    }
}

async fn fetch_webseed_batch(ctx: WebSeedContext, piece_indices: Vec<u32>) -> WebSeedBatchResult {
    let generation = ctx.generation.current();
    let Some(&first_index) = piece_indices.first() else {
        return WebSeedBatchResult {
            generation,
            pieces: Vec::new(),
        };
    };
    let Some(&last_index) = piece_indices.last() else {
        return WebSeedBatchResult {
            generation,
            pieces: Vec::new(),
        };
    };
    let Ok(first) = ctx.lengths.validate_piece(first_index) else {
        return webseed_failed_batch(generation, &piece_indices);
    };
    let Ok(last) = ctx.lengths.validate_piece(last_index) else {
        return webseed_failed_batch(generation, &piece_indices);
    };
    if piece_indices
        .windows(2)
        .any(|pair| pair[1] != pair[0].saturating_add(1))
    {
        return webseed_failed_batch(generation, &piece_indices);
    }

    let offset = ctx.lengths.piece_offset(first);
    let batch_len = ctx
        .lengths
        .piece_offset(last)
        .saturating_add(ctx.lengths.piece_length_of(last) as u64)
        .saturating_sub(offset);
    let spans = webseed_piece_spans(&ctx.layout, offset, batch_len);
    if spans.is_empty() {
        return webseed_failed_batch(generation, &piece_indices);
    }
    for mirror_idx in 0..ctx.bases.len() {
        if ctx.disabled.lock().contains(&mirror_idx) {
            continue;
        }
        let mut batch = vec![0u8; batch_len as usize];
        let mut cursor = 0usize;
        let mut mirror_ok = true;
        for (file_idx, file_offset, span_len) in &spans {
            let file = &ctx.layout.files()[*file_idx];
            let url = match super::webseed::build_file_url(
                &ctx.bases[mirror_idx],
                &ctx.info.name,
                &ctx.info.files[*file_idx].path,
                ctx.info.single_file_mode,
            ) {
                Ok(url) => url,
                Err(_) => {
                    ctx.disabled.lock().insert(mirror_idx);
                    mirror_ok = false;
                    break;
                }
            };
            let mut span_cursor = 0u64;
            while span_cursor < *span_len {
                let request_len = (*span_len - span_cursor).min(WEBSEED_MAX_RANGE_BYTES);
                let Some(start) = file_offset.checked_add(span_cursor) else {
                    ctx.disabled.lock().insert(mirror_idx);
                    mirror_ok = false;
                    break;
                };
                let requested = match super::webseed::validate_range(
                    start,
                    start + request_len - 1,
                    file.length,
                    WEBSEED_MAX_RANGE_BYTES,
                ) {
                    Ok(requested) => requested,
                    Err(_) => {
                        ctx.disabled.lock().insert(mirror_idx);
                        mirror_ok = false;
                        break;
                    }
                };
                let mut fetched = None;
                for attempt in 0..3u32 {
                    match super::webseed::fetch_range_response(
                        &ctx.client,
                        &url,
                        requested,
                        WEBSEED_MAX_RANGE_BYTES,
                    )
                    .await
                    {
                        Ok(response) => {
                            let representation_changed = {
                                let mut etags = ctx.etags.lock();
                                let etag_key = (mirror_idx, *file_idx);
                                match response.etag {
                                    Some(etag) => {
                                        if etags
                                            .get(&etag_key)
                                            .is_some_and(|previous| previous != &etag)
                                        {
                                            true
                                        } else {
                                            etags.entry(etag_key).or_insert(etag);
                                            false
                                        }
                                    }
                                    None => etags.contains_key(&etag_key),
                                }
                            };
                            if representation_changed {
                                ctx.disabled.lock().insert(mirror_idx);
                                mirror_ok = false;
                                break;
                            }
                            fetched = Some(response.body);
                            break;
                        }
                        Err(super::webseed::WebSeedError::UnexpectedStatus(status))
                            if super::webseed::is_retryable_status(status) && attempt < 2 =>
                        {
                            tokio::time::sleep(Duration::from_millis(100 * (attempt + 1) as u64))
                                .await;
                        }
                        Err(super::webseed::WebSeedError::Http(_)) if attempt < 2 => {
                            tokio::time::sleep(Duration::from_millis(100 * (attempt + 1) as u64))
                                .await;
                        }
                        Err(super::webseed::WebSeedError::UnexpectedStatus(status))
                            if super::webseed::is_permanent_status(status) =>
                        {
                            ctx.disabled.lock().insert(mirror_idx);
                            mirror_ok = false;
                            break;
                        }
                        Err(super::webseed::WebSeedError::Http(_)) => {
                            mirror_ok = false;
                            break;
                        }
                        Err(_) => {
                            ctx.disabled.lock().insert(mirror_idx);
                            mirror_ok = false;
                            break;
                        }
                    }
                }
                let Some(bytes) = fetched else {
                    break;
                };
                let end_cursor = cursor.saturating_add(bytes.len());
                if end_cursor > batch.len() || bytes.len() as u64 != request_len {
                    ctx.disabled.lock().insert(mirror_idx);
                    mirror_ok = false;
                    break;
                }
                batch[cursor..end_cursor].copy_from_slice(&bytes);
                cursor = end_cursor;
                span_cursor += request_len;
            }
            if !mirror_ok || span_cursor != *span_len {
                break;
            }
        }
        if !mirror_ok || cursor != batch.len() {
            continue;
        }

        let mut pieces = Vec::with_capacity(piece_indices.len());
        let mut piece_cursor = 0usize;
        for piece_index in &piece_indices {
            let Ok(vpi) = ctx.lengths.validate_piece(*piece_index) else {
                return webseed_failed_batch(generation, &piece_indices);
            };
            let piece_len = ctx.lengths.piece_length_of(vpi) as usize;
            let end = piece_cursor.saturating_add(piece_len);
            let Some(piece) = batch.get(piece_cursor..end) else {
                return webseed_failed_batch(generation, &piece_indices);
            };
            if ctx.verifier.verify(*piece_index, piece).is_err() {
                ctx.disabled.lock().insert(mirror_idx);
                mirror_ok = false;
                break;
            }
            let write_failed = ctx
                .storage
                .write_at_owned(ctx.lengths.piece_offset(vpi), Bytes::copy_from_slice(piece))
                .await
                .is_err();
            pieces.push(VerifyResult {
                piece_index: *piece_index,
                write_failed,
                verify_ok: !write_failed,
            });
            piece_cursor = end;
        }
        if mirror_ok && piece_cursor == batch.len() {
            return WebSeedBatchResult { generation, pieces };
        }
    }
    webseed_failed_batch(generation, &piece_indices)
}

fn schedule_webseed_workers(
    tasks: &mut tokio::task::JoinSet<WebSeedBatchResult>,
    inflight: &mut HashSet<u32>,
    piece_tracker: &mut PieceTracker,
    ctx: &WebSeedContext,
) {
    if tasks.len() >= WEBSEED_MAX_WORKERS || !ctx.has_available_mirror() {
        return;
    }
    let mut candidates: Vec<u32> = piece_tracker
        .choose_missing_pieces_by_index()
        .into_iter()
        .map(|piece| piece.get())
        .collect();
    candidates.sort_unstable();
    candidates.retain(|piece| !inflight.contains(piece));
    for batch in contiguous_webseed_batches(&candidates, WEBSEED_MAX_PIECES_PER_JOB) {
        if tasks.len() >= WEBSEED_MAX_WORKERS {
            break;
        }
        for piece_index in &batch {
            let Ok(vpi) = ctx.lengths.validate_piece(*piece_index) else {
                continue;
            };
            inflight.insert(*piece_index);
            piece_tracker.mark_in_flight(vpi);
        }
        let worker_ctx = ctx.clone();
        tasks.spawn(async move { fetch_webseed_batch(worker_ctx, batch).await });
    }
}

fn contiguous_webseed_batches(candidates: &[u32], max_pieces: usize) -> Vec<Vec<u32>> {
    let mut batches = Vec::new();
    let mut batch = Vec::new();
    for &piece in candidates {
        let adjacent = batch
            .last()
            .is_some_and(|previous: &u32| piece == previous.saturating_add(1));
        if !batch.is_empty() && (!adjacent || batch.len() == max_pieces.max(1)) {
            batches.push(std::mem::take(&mut batch));
        }
        batch.push(piece);
    }
    if !batch.is_empty() {
        batches.push(batch);
    }
    batches
}

/// Clear the endgame flag once the working set grows back above the activation threshold; endgame is otherwise a one-way ratchet (set when `pending_chunks() <= 64`, never cleared) that would keep pieces re-queued via `reset_piece` after a hash/write failure off the cheap sequential-scan path in `next_chunk` and force every peer through the `choose_piece_excluding` fallback for the rest of the download
fn maybe_clear_endgame(chunk_tracker: &mut ChunkTracker) {
    if chunk_tracker.endgame() && chunk_tracker.pending_chunks() > 64 {
        chunk_tracker.set_endgame(false);
    }
}

/// Handle an inbound BEP-55 ut_holepunch message
#[allow(clippy::too_many_arguments)]
fn handle_holepunch(
    hp: HolepunchMsg,
    from_addr: SocketAddr,
    from_hp_id: Option<u8>,
    from_cmd: &mpsc::Sender<PeerCommand>,
    peers: &HashMap<u32, Peer>,
    pending_dials: &HashMap<u32, SocketAddr>,
    priority_backlog: &mut VecDeque<SocketAddr>,
    peer_backlog: &mut VecDeque<SocketAddr>,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
    known_addrs: &mut HashSet<SocketAddr>,
    holepunch_targets: &mut HashSet<SocketAddr>,
    blocklist: &RwLock<BlockList>,
) {
    match hp.msg_type {
        holepunch_type::CONNECT => {
            if blocklist.read().contains(hp.addr.ip()) {
                return;
            }
            let active_addrs = peers
                .values()
                .map(|peer| peer.addr)
                .chain(pending_dials.values().copied())
                .collect::<HashSet<_>>();
            if promote_holepunch_candidate(
                hp.addr,
                &active_addrs,
                priority_backlog,
                peer_backlog,
                dial_retries,
                useful_redials,
                known_addrs,
            ) {
                holepunch_targets.insert(hp.addr);
            }
        }
        holepunch_type::RENDEZVOUS => {
            // A peer that did not advertise the extension
            let Some(from_hp_id) = from_hp_id else {
                return;
            };
            // We are the relay
            enum Relay {
                Connect(mpsc::Sender<PeerCommand>, u8),
                Err(u32),
            }
            let action = if hp.addr == from_addr {
                Relay::Err(holepunch_err::NO_SELF)
            } else if !super::magnet::is_dialable_peer_addr(hp.addr) {
                Relay::Err(holepunch_err::NO_SUCH_PEER)
            } else {
                match peers.values().find(|p| p.addr == hp.addr) {
                    Some(t) => match t.their_ut_holepunch_id {
                        Some(thp) => Relay::Connect(t.cmd_tx.clone(), thp),
                        None => Relay::Err(holepunch_err::NO_SUPPORT),
                    },
                    None => Relay::Err(holepunch_err::NOT_CONNECTED),
                }
            };
            match action {
                Relay::Connect(target_cmd, target_hp) => {
                    // Tell the target to connect to the initiator
                    let _ = target_cmd.try_send(PeerCommand::Send(Message::Extended {
                        ext_id: target_hp,
                        payload: build_holepunch(holepunch_type::CONNECT, from_addr, 0),
                    }));
                    // and tell the initiator to connect to the target
                    let _ = from_cmd.try_send(PeerCommand::Send(Message::Extended {
                        ext_id: from_hp_id,
                        payload: build_holepunch(holepunch_type::CONNECT, hp.addr, 0),
                    }));
                }
                Relay::Err(code) => {
                    let _ = from_cmd.try_send(PeerCommand::Send(Message::Extended {
                        ext_id: from_hp_id,
                        payload: build_holepunch(holepunch_type::ERROR, hp.addr, code),
                    }));
                }
            }
        }
        holepunch_type::ERROR => {
            tracing::debug!(
                target: "diag",
                "holepunch ERROR from {from_addr} target={} code={}",
                hp.addr, hp.err_code
            );
        }
        _ => {}
    }
}

fn promote_holepunch_candidate(
    addr: SocketAddr,
    active_addrs: &HashSet<SocketAddr>,
    priority_backlog: &mut VecDeque<SocketAddr>,
    peer_backlog: &mut VecDeque<SocketAddr>,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
    known_addrs: &mut HashSet<SocketAddr>,
) -> bool {
    normalize_peer_queues(
        priority_backlog,
        peer_backlog,
        dial_retries,
        useful_redials,
        active_addrs,
        known_addrs,
    );
    if !super::magnet::is_dialable_peer_addr(addr) || active_addrs.contains(&addr) {
        return false;
    }

    priority_backlog.retain(|candidate| *candidate != addr);
    peer_backlog.retain(|candidate| *candidate != addr);
    dial_retries.retain(|(candidate, _)| *candidate != addr);
    useful_redials.retain(|(candidate, _)| *candidate != addr);

    let queued =
        priority_backlog.len() + peer_backlog.len() + dial_retries.len() + useful_redials.len();
    if queued >= MAX_PEER_BACKLOG {
        let evicted = peer_backlog
            .pop_back()
            .or_else(|| dial_retries.pop_back().map(|(candidate, _)| candidate))
            .or_else(|| useful_redials.pop_back().map(|(candidate, _)| candidate))
            .or_else(|| priority_backlog.pop_back());
        if evicted.is_none() {
            return false;
        }
    }

    priority_backlog.push_front(addr);
    normalize_peer_queues(
        priority_backlog,
        peer_backlog,
        dial_retries,
        useful_redials,
        active_addrs,
        known_addrs,
    );
    true
}

fn normalize_peer_queues(
    priority_backlog: &mut VecDeque<SocketAddr>,
    peer_backlog: &mut VecDeque<SocketAddr>,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
    active_addrs: &HashSet<SocketAddr>,
    known_addrs: &mut HashSet<SocketAddr>,
) {
    let old_priority = std::mem::take(priority_backlog);
    let old_useful_redials = std::mem::take(useful_redials);
    let old_dial_retries = std::mem::take(dial_retries);
    let old_backlog = std::mem::take(peer_backlog);

    let mut seen = active_addrs.clone();
    let mut queued = 0usize;
    let mut accept = |addr: SocketAddr| {
        if queued >= MAX_PEER_BACKLOG
            || !super::magnet::is_dialable_peer_addr(addr)
            || !seen.insert(addr)
        {
            false
        } else {
            queued += 1;
            true
        }
    };

    for addr in old_priority {
        if accept(addr) {
            priority_backlog.push_back(addr);
        }
    }
    for (addr, due) in old_useful_redials {
        if accept(addr) {
            useful_redials.push_back((addr, due));
        }
    }
    for (addr, due) in old_dial_retries {
        if accept(addr) {
            dial_retries.push_back((addr, due));
        }
    }
    for addr in old_backlog {
        if accept(addr) {
            peer_backlog.push_back(addr);
        }
    }

    known_addrs.clear();
    known_addrs.extend(seen);
}

fn refresh_peer_queue_state(
    priority_backlog: &mut VecDeque<SocketAddr>,
    peer_backlog: &mut VecDeque<SocketAddr>,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
    peers: &HashMap<u32, Peer>,
    pending_dials: &HashMap<u32, SocketAddr>,
    known_addrs: &mut HashSet<SocketAddr>,
) {
    let active_addrs = peers
        .values()
        .map(|peer| peer.addr)
        .chain(pending_dials.values().copied())
        .collect::<HashSet<_>>();
    normalize_peer_queues(
        priority_backlog,
        peer_backlog,
        dial_retries,
        useful_redials,
        &active_addrs,
        known_addrs,
    );
}

#[allow(clippy::too_many_arguments)]
fn enqueue_peer_candidate(
    addr: SocketAddr,
    priority: bool,
    priority_backlog: &mut VecDeque<SocketAddr>,
    peer_backlog: &mut VecDeque<SocketAddr>,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
    known_addrs: &mut HashSet<SocketAddr>,
    blocklist: &RwLock<BlockList>,
) -> bool {
    if blocklist.read().contains(addr.ip()) {
        return false;
    }
    if !super::magnet::is_dialable_peer_addr(addr) || known_addrs.contains(&addr) {
        return false;
    }

    let queued =
        priority_backlog.len() + peer_backlog.len() + dial_retries.len() + useful_redials.len();
    if queued >= MAX_PEER_BACKLOG {
        if !priority {
            return false;
        }
        let evicted = peer_backlog
            .pop_back()
            .or_else(|| dial_retries.pop_back().map(|(candidate, _)| candidate))
            .or_else(|| useful_redials.pop_back().map(|(candidate, _)| candidate))
            .or_else(|| priority_backlog.pop_back());
        let Some(evicted) = evicted else {
            return false;
        };
        known_addrs.remove(&evicted);
    }

    if priority {
        priority_backlog.push_back(addr);
    } else {
        peer_backlog.push_back(addr);
    }
    known_addrs.insert(addr);
    true
}

fn schedule_peer_retry(
    addr: SocketAddr,
    useful: bool,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
) {
    if useful {
        if useful_redials.len() < MAX_DIAL_RETRIES {
            useful_redials.push_back((addr, Instant::now() + USEFUL_PEER_REDIAL_DELAY));
        }
    } else if dial_retries.len() < MAX_DIAL_RETRIES {
        dial_retries.push_back((addr, Instant::now() + DIAL_RETRY_DELAY));
    }
}

fn dial_slot_available(
    live_peers: usize,
    pending_dials: usize,
    max_peers: usize,
    pending_limit: usize,
) -> bool {
    live_peers.saturating_add(pending_dials) < max_peers && pending_dials < pending_limit
}

#[allow(clippy::too_many_arguments)]
fn drain_peer_backlog(
    priority_backlog: &mut VecDeque<SocketAddr>,
    peer_backlog: &mut VecDeque<SocketAddr>,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
    pending_dials: &mut HashMap<u32, SocketAddr>,
    known_addrs: &mut HashSet<SocketAddr>,
    useful_peers: &HashMap<SocketAddr, usize>,
    holepunch_targets: &HashSet<SocketAddr>,
    next_pid: &mut u32,
    live_peers: usize,
    max_peers: usize,
    torrent_id: usize,
    registry_scope: &Arc<()>,
    info_hash: Id20,
    our_peer_id: Id20,
    peer_event_tx: &mpsc::Sender<(u32, PeerEvent)>,
    encryption: crate::peer::EncryptionPolicy,
    advertise_v2: bool,
    advertise_dht: bool,
    ext_handshake_builder: &crate::peer::ExtHandshakeBuilder,
    utp: &Option<Arc<UtpSocket>>,
    proxy: &Option<risuko_http::ProxyConnector>,
    outbound_tasks: &mut tokio::task::JoinSet<()>,
    outbound_aborts: &mut HashMap<u32, AbortHandle>,
    blocklist: &RwLock<BlockList>,
) {
    // Promote due useful-peer redials into the priority backlog
    let now = Instant::now();
    while useful_redials.front().is_some_and(|(_, due)| *due <= now) {
        let Some((addr, _)) = useful_redials.pop_front() else {
            break;
        };
        if known_addrs.contains(&addr) {
            priority_backlog.push_front(addr);
        }
    }

    while dial_retries.front().is_some_and(|(_, due)| *due <= now) {
        let Some((addr, _)) = dial_retries.pop_front() else {
            break;
        };
        if known_addrs.contains(&addr) {
            peer_backlog.push_back(addr);
        }
    }

    let spawn_one = |addr: SocketAddr,
                     pending_dials: &mut HashMap<u32, SocketAddr>,
                     next_pid: &mut u32,
                     outbound_tasks: &mut tokio::task::JoinSet<()>,
                     outbound_aborts: &mut HashMap<u32, AbortHandle>| {
        let pid = *next_pid;
        *next_pid += 1;
        pending_dials.insert(pid, addr);
        let known_useful = useful_peers.contains_key(&addr);
        let prefer_utp = holepunch_targets.contains(&addr);
        let abort = outbound_tasks.spawn(run_outbound_peer(
            torrent_id,
            pid,
            addr,
            registry_scope.clone(),
            info_hash,
            our_peer_id,
            peer_event_tx.clone(),
            encryption,
            advertise_v2,
            advertise_dht,
            Some(ext_handshake_builder.clone()),
            utp.clone(),
            prefer_utp,
            known_useful,
            proxy.clone(),
        ));
        outbound_aborts.insert(pid, abort);
    };

    while dial_slot_available(
        live_peers,
        pending_dials.len(),
        max_peers,
        MAX_PENDING_DIALS,
    ) {
        let Some(addr) = priority_backlog.pop_front() else {
            break;
        };
        if !known_addrs.contains(&addr) {
            continue;
        }
        if blocklist.read().contains(addr.ip()) {
            known_addrs.remove(&addr);
            continue;
        }
        spawn_one(
            addr,
            pending_dials,
            next_pid,
            outbound_tasks,
            outbound_aborts,
        );
    }

    // Cold backlog leaves PRIORITY_DIAL_RESERVE free when the swarm is thin
    let cold_cap = if live_peers < 8 {
        MAX_PENDING_DIALS.saturating_sub(PRIORITY_DIAL_RESERVE)
    } else {
        MAX_PENDING_DIALS
    };
    while dial_slot_available(live_peers, pending_dials.len(), max_peers, cold_cap) {
        let Some(addr) = peer_backlog.pop_front() else {
            break;
        };
        if !known_addrs.contains(&addr) {
            continue;
        }
        if blocklist.read().contains(addr.ip()) {
            known_addrs.remove(&addr);
            continue;
        }
        spawn_one(
            addr,
            pending_dials,
            next_pid,
            outbound_tasks,
            outbound_aborts,
        );
    }
}

/// On a failed direct dial to `target`, ask the peer that gossiped it via PEX to perform a BEP-55 rendezvous
fn try_initiate_holepunch(
    target: SocketAddr,
    pex_source: &HashMap<SocketAddr, u32>,
    holepunch_attempted: &mut HashSet<SocketAddr>,
    peers: &HashMap<u32, Peer>,
) {
    if holepunch_attempted.contains(&target) {
        return;
    }
    // Only peers we learned via PEX have a known relay; tracker/DHT peers that fail to connect have no rendezvous path we can use
    let Some(&relay_pid) = pex_source.get(&target) else {
        return;
    };
    let Some(relay) = peers.get(&relay_pid) else {
        return; // the gossiper has since disconnected
    };
    let Some(relay_hp) = relay.their_ut_holepunch_id else {
        return; // relay doesn't support holepunch
    };
    if relay
        .cmd_tx
        .try_send(PeerCommand::Send(Message::Extended {
            ext_id: relay_hp,
            payload: build_holepunch(holepunch_type::RENDEZVOUS, target, 0),
        }))
        .is_ok()
    {
        holepunch_attempted.insert(target);
        tracing::debug!(
            target: "diag",
            "holepunch RENDEZVOUS initiate target={target} via relay pid={relay_pid}"
        );
    }
}

async fn send_interested_if_useful(peer: &mut Peer, piece_tracker: &mut PieceTracker) {
    let useful = piece_tracker.choose_piece(&peer.bitfield).is_some();
    let set_bits: u32 = peer.bitfield.iter().map(|x| x.count_ones()).sum();
    tracing::debug!(
        target: "diag",
        "send_interested_if_useful {} am_interested={} useful={} my_bitfield_set={}",
        peer.addr, peer.am_interested, useful, set_bits
    );
    if !peer.am_interested && useful {
        peer.am_interested = true;
        let _ = peer
            .cmd_tx
            .send(PeerCommand::Send(Message::Interested))
            .await;
    }
}

async fn broadcast_have(peers: &mut HashMap<u32, Peer>, piece_index: u32) {
    for p in peers.values_mut() {
        let _ = p
            .cmd_tx
            .try_send(PeerCommand::Send(Message::Have { piece_index }));
    }
}

fn request_peer_eligible(peer: &Peer) -> bool {
    (!peer.peer_choking || !peer.remote_allowed_fast.is_empty())
        && peer.am_interested
        && !peer.snubbing
}

fn effective_request_limit(adaptive_limit: usize, active_request_peers: usize) -> usize {
    if active_request_peers == 0 {
        return 0;
    }
    let fair_share = (TORRENT_REQUEST_BUDGET / active_request_peers).max(1);
    adaptive_limit.min(fair_share)
}

async fn drive_requests(
    peers: &mut HashMap<u32, Peer>,
    piece_tracker: &mut PieceTracker,
    chunk_tracker: &mut ChunkTracker,
) {
    let pids: Vec<u32> = peers.keys().copied().collect();
    for pid in pids {
        drive_peer(pid, peers, piece_tracker, chunk_tracker).await;
    }
}

fn pipeline_bounds(configured: Option<usize>) -> (usize, usize) {
    let cap = configured
        .map(|value| value.clamp(1, ABSOLUTE_MAX_OUTSTANDING_PER_PEER))
        .unwrap_or(DEFAULT_ADAPTIVE_MAX_OUTSTANDING_PER_PEER);
    (DEFAULT_MAX_OUTSTANDING_PER_PEER.min(cap), cap)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PipelineAdjustment {
    SlowStart { target: usize, finished: bool },
    Rate { target: usize },
}

#[allow(clippy::too_many_arguments)]
fn pipeline_adjustment(
    slow_start: bool,
    delivered_since_growth: usize,
    delivered_chunks: u32,
    window: Duration,
    floor: usize,
    cap: usize,
    current: usize,
) -> Option<PipelineAdjustment> {
    if slow_start {
        let target = pipeline_slow_start_target(delivered_since_growth, current, cap)?;
        return Some(PipelineAdjustment::SlowStart {
            target,
            finished: target >= cap.min(PIPELINE_SLOW_START_CAP),
        });
    }
    (window >= PIPELINE_RATE_WINDOW).then(|| PipelineAdjustment::Rate {
        target: pipeline_target(delivered_chunks, window, floor, cap, current),
    })
}

fn pipeline_slow_start_target(
    delivered_since_growth: usize,
    current: usize,
    cap: usize,
) -> Option<usize> {
    let probe_cap = cap.min(PIPELINE_SLOW_START_CAP);
    if current >= probe_cap || delivered_since_growth < current {
        return None;
    }
    let target = current.saturating_mul(2).min(probe_cap);
    (target > current).then_some(target)
}

fn pipeline_target(
    delivered_chunks: u32,
    window: Duration,
    floor: usize,
    cap: usize,
    current: usize,
) -> usize {
    let secs = window.as_secs_f32().max(0.001);
    let rate = delivered_chunks as f32 / secs; // chunks per second
    let raw = ((rate * PIPELINE_TARGET_SECS) as usize).clamp(floor, cap);
    if raw > current {
        let step = if current >= PIPELINE_SLOW_START_CAP {
            PIPELINE_ADDITIVE_GROWTH
        } else {
            (current / 2).max(PIPELINE_ADDITIVE_GROWTH)
        };
        current.saturating_add(step).min(raw).min(cap).max(floor)
    } else {
        raw
    }
}

fn shrink_pipeline(current: usize, floor: usize) -> usize {
    (current / 2).max(floor)
}

fn next_optimistic(sorted_candidates: &[u32], current: Option<u32>) -> Option<u32> {
    if sorted_candidates.is_empty() {
        return None;
    }
    let next = match current {
        Some(cur) => sorted_candidates
            .iter()
            .copied()
            .find(|&pid| pid > cur)
            .unwrap_or(sorted_candidates[0]),
        None => sorted_candidates[0],
    };
    Some(next)
}

fn select_unchoked(
    mut candidates: Vec<(u32, u64)>,
    optimistic: Option<u32>,
    slots: usize,
) -> HashSet<u32> {
    let mut selected: HashSet<u32> = HashSet::new();
    if let Some(pid) = optimistic.filter(|pid| candidates.iter().any(|&(c, _)| c == *pid)) {
        selected.insert(pid);
    }
    // Rate desc, pid asc as a deterministic tiebreak
    candidates.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    for (pid, _) in candidates {
        if selected.len() >= slots {
            break;
        }
        selected.insert(pid);
    }
    selected
}

fn run_choke_eval(
    peers: &mut HashMap<u32, Peer>,
    seeding: bool,
    optimistic: Option<u32>,
    reset_windows: bool,
) {
    let candidates: Vec<(u32, u64)> = peers
        .iter()
        .filter(|(_, p)| p.peer_interested)
        .map(|(&pid, p)| {
            let rate = if seeding {
                p.uploaded_window.load(Ordering::Relaxed)
            } else {
                p.downloaded_window
            };
            (pid, rate)
        })
        .collect();
    let unchoke = select_unchoked(candidates, optimistic, UPLOAD_SLOTS);
    for (&pid, p) in peers.iter_mut() {
        let should_unchoke = unchoke.contains(&pid);
        if should_unchoke
            && p.am_choking
            && p.cmd_tx
                .try_send(PeerCommand::Send(Message::Unchoke))
                .is_ok()
        {
            p.am_choking = false;
        } else if !should_unchoke
            && !p.am_choking
            && p.cmd_tx.try_send(PeerCommand::Send(Message::Choke)).is_ok()
        {
            p.am_choking = true;
        }
        p.optimistic_unchoke = should_unchoke && !p.am_choking && optimistic == Some(pid);
        if reset_windows {
            p.downloaded_window = 0;
            p.uploaded_window.store(0, Ordering::Relaxed);
        }
    }
}

/// Send a `Cancel` to every peer (other than `except_pid`) that has the chunk `(index, begin, length)` outstanding, removing the entry from each peer's local Vec; best-effort (if a peer's command channel is full or closed we skip it, the chunk just arrives and is dedup-dropped), and this is the only thing that keeps endgame mode from saturating downstream with duplicate blocks at the very end of a download
fn cancel_outstanding(
    peers: &mut HashMap<u32, Peer>,
    except_pid: u32,
    index: u32,
    begin: u32,
    length: u32,
) {
    for (&other_pid, other) in peers.iter_mut() {
        if other_pid == except_pid {
            continue;
        }
        let Some(slot) = other
            .outstanding
            .iter()
            .position(|&(p, b, l)| p == index && b == begin && l == length)
        else {
            continue;
        };
        // Drop our local record first so a duplicate Piece response is rejected as unsolicited even if the peer ignores Cancel
        other.outstanding.swap_remove(slot);
        remember_canceled_request(other, (index, begin, length));
        let _ = other.cmd_tx.try_send(PeerCommand::Send(Message::Cancel {
            index,
            begin,
            length,
        }));
    }
}

/// Send `Cancel` for every outstanding chunk request matching `piece_index` across every peer; called when the piece is no longer needed — either verified successfully (other peers' endgame duplicates would now be wasted bytes) or failed verification / disk write and about to be re-requested from scratch (delivering stale chunks from the previous attempt would inflate received_bytes past the expected_bytes guard, forcing a second reset)
fn cancel_piece_outstanding(peers: &mut HashMap<u32, Peer>, piece_index: u32) {
    for other in peers.values_mut() {
        let mut i = 0;
        while i < other.outstanding.len() {
            let (p, begin, length) = other.outstanding[i];
            if p == piece_index {
                other.outstanding.swap_remove(i);
                remember_canceled_request(other, (p, begin, length));
                let _ = other.cmd_tx.try_send(PeerCommand::Send(Message::Cancel {
                    index: piece_index,
                    begin,
                    length,
                }));
            } else {
                i += 1;
            }
        }
    }
}

fn remember_canceled_request(peer: &mut Peer, request: (u32, u32, u32)) {
    const MAX_CANCELED_REQUESTS: usize = 4096;
    if peer.canceled_requests.len() >= MAX_CANCELED_REQUESTS {
        peer.canceled_requests.clear();
    }
    peer.canceled_requests.insert(request);
}

/// Pipeline requests for a single peer up to its adaptive `max_outstanding`; called both on the tick and inline after events that can unblock a peer (Unchoke, Bitfield, Have, Piece), since without the inline calls throughput is capped at `max_outstanding * CHUNK_SIZE / tick_interval`
async fn drive_peer(
    pid: u32,
    peers: &mut HashMap<u32, Peer>,
    piece_tracker: &mut PieceTracker,
    chunk_tracker: &mut ChunkTracker,
) {
    let active_request_peers = peers
        .values()
        .filter(|peer| request_peer_eligible(peer))
        .count();
    let outstanding_requests: usize = peers.values().map(|peer| peer.outstanding.len()).sum();
    let remaining_request_budget = TORRENT_REQUEST_BUDGET.saturating_sub(outstanding_requests);
    let Some(peer) = peers.get_mut(&pid) else {
        return;
    };
    if !request_peer_eligible(peer) || remaining_request_budget == 0 {
        return;
    }
    let max_outstanding = effective_request_limit(peer.max_outstanding, active_request_peers);
    let mut request_slots = remaining_request_budget;

    let mut exhausted: HashSet<u32> = HashSet::new();
    let mut requestable_pieces = piece_tracker.choose_requestable_pieces(&peer.bitfield, pid);
    requestable_pieces
        .sort_by_key(|piece| (!peer.suggested_pieces.contains(&piece.get()), piece.get()));
    let mut requestable_idx = 0usize;
    let mut endgame_pieces: Option<Vec<super::core::ValidPieceIndex>> = None;
    let mut endgame_idx = 0usize;
    let mut current_piece: Option<super::core::ValidPieceIndex> = None;
    while peer.outstanding.len() < max_outstanding && request_slots > 0 {
        let piece = match current_piece {
            Some(piece) => piece,
            None => {
                let mut selected = None;
                loop {
                    if let Some(piece) = requestable_pieces.get(requestable_idx).copied() {
                        requestable_idx += 1;
                        if !exhausted.contains(&piece.get())
                            && (!peer.peer_choking
                                || peer.remote_allowed_fast.contains(&piece.get()))
                        {
                            selected = Some(piece);
                            break;
                        }
                        continue;
                    }

                    if !chunk_tracker.endgame() && chunk_tracker.pending_chunks() <= 64 {
                        chunk_tracker.set_endgame(true);
                    }
                    if chunk_tracker.endgame() {
                        let pieces = endgame_pieces.get_or_insert_with(|| {
                            piece_tracker.choose_pieces(&peer.bitfield, pid)
                        });
                        if let Some(piece) = pieces.get(endgame_idx).copied() {
                            endgame_idx += 1;
                            if !exhausted.contains(&piece.get())
                                && (!peer.peer_choking
                                    || peer.remote_allowed_fast.contains(&piece.get()))
                            {
                                selected = Some(piece);
                                break;
                            }
                            continue;
                        }
                    }
                    break;
                }
                let Some(piece) = selected else {
                    break;
                };
                piece
            }
        };
        match chunk_tracker.next_chunk(piece, pid) {
            Some(chunk) => {
                current_piece = Some(piece);
                let info = chunk.info;
                let prior = chunk.prior_state;
                // try_send + rollback so a single peer with a full writer queue cannot block the entire main loop; send().await on a full channel here would freeze every other download peer
                use tokio::sync::mpsc::error::TrySendError;
                let req = Message::Request {
                    index: info.piece_index.get(),
                    begin: info.offset,
                    length: info.size,
                };
                match peer.cmd_tx.try_send(PeerCommand::Send(req)) {
                    Ok(()) => {
                        peer.outstanding
                            .push((info.piece_index.get(), info.offset, info.size));
                        request_slots -= 1;
                    }
                    Err(TrySendError::Full(_)) => {
                        // Roll the chunk back so a different peer (or this one on the next tick) can pick it up
                        chunk_tracker.unrequest_chunk(info.piece_index, info.chunk_index, prior);
                        break;
                    }
                    Err(TrySendError::Closed(_)) => {
                        chunk_tracker.unrequest_chunk(info.piece_index, info.chunk_index, prior);
                        break;
                    }
                }
            }
            None => {
                current_piece = None;
                // All chunks of this piece are already requested or received (under endgame, also already requested by THIS peer); mark in_flight so non-endgame `choose_requestable_piece` skips it, and record it locally so the endgame fallback path doesn't pick it again next loop iteration
                piece_tracker.mark_in_flight(piece);
                exhausted.insert(piece.get());
                continue;
            }
        }
    }
}

async fn scan_existing_pieces(
    verifier: &PieceVerifier,
    storage: &Arc<FilesystemStorage>,
    lengths: &Lengths,
    piece_tracker: &mut PieceTracker,
) {
    // Sequential scanning of every piece blocks the torrent loop before any peer can connect — many seconds of dead time for a 50 GB torrent; hash pieces in parallel batches so disk reads + verify overlap
    use tokio::task::JoinSet;
    let total = lengths.total_pieces();
    if total == 0 {
        return;
    }
    // Cap memory rather than parallelism: the previous fixed concurrency=16 allocated 16 * piece_length bytes per batch, which on multi-MiB pieces could push hundreds of MiB through this scan; derive the batch size from a byte budget so each batch allocates at most ~MAX_SCAN_BYTES, still capping at 16 to avoid saturating the spawn_blocking pool on tiny-piece torrents
    const MAX_SCAN_BYTES: usize = 64 * 1024 * 1024;
    let plen_hint = lengths
        .validate_piece(0)
        .map(|vpi| lengths.piece_length_of(vpi) as usize)
        .unwrap_or(0)
        .max(1);
    let concurrency = (MAX_SCAN_BYTES / plen_hint).clamp(1, 16);
    let mut idx: u32 = 0;
    while idx < total {
        let mut set: JoinSet<Option<u32>> = JoinSet::new();
        let chunk_end = (idx + concurrency as u32).min(total);
        for i in idx..chunk_end {
            let Ok(vpi) = lengths.validate_piece(i) else {
                continue;
            };
            let plen = lengths.piece_length_of(vpi) as usize;
            let offset = lengths.piece_offset(vpi);
            let storage = storage.clone();
            let verifier = verifier.clone();
            set.spawn(async move {
                let mut buf = vec![0u8; plen];
                if storage.read_at(offset, &mut buf).await.is_err() {
                    return None;
                }
                let ok = tokio::task::spawn_blocking(move || verifier.verify(i, &buf).is_ok())
                    .await
                    .ok()?;
                if ok {
                    Some(i)
                } else {
                    None
                }
            });
        }
        while let Some(res) = set.join_next().await {
            if let Ok(Some(i)) = res {
                if let Ok(vpi) = lengths.validate_piece(i) {
                    piece_tracker.set_local(vpi, true);
                }
            }
        }
        idx = chunk_end;
    }
}

fn mark_verified_piece_local(pt: &mut PieceTracker, vpi: super::core::ValidPieceIndex) -> bool {
    let was_local = pt.has_local(vpi);
    pt.set_local(vpi, true);
    !was_local
}

fn add_piece_progress(
    stats: &mut TorrentStats,
    lengths: &Lengths,
    layout: &FileSet,
    vpi: super::core::ValidPieceIndex,
) {
    let offset = lengths.piece_offset(vpi);
    let len = lengths.piece_length_of(vpi) as u64;
    stats.progress_bytes = stats.progress_bytes.saturating_add(len);
    if stats.file_progress.len() < layout.files().len() {
        stats.file_progress.resize(layout.files().len(), 0);
    }
    for span in layout.spans_for(offset, len) {
        stats.file_progress[span.file_index] =
            stats.file_progress[span.file_index].saturating_add(span.len);
    }
}

/// Distribute the bytes of every completed piece across the files it overlaps; used to populate `TorrentStats::file_progress` so per-file completion can be reported in the UI
fn compute_file_progress(
    pt: &PieceTracker,
    lengths: &Lengths,
    layout: &crate::storage::FileSet,
) -> Vec<u64> {
    let mut per_file = vec![0u64; layout.files().len()];
    for idx in 0..lengths.total_pieces() {
        let Ok(vpi) = lengths.validate_piece(idx) else {
            continue;
        };
        if !pt.has_local(vpi) {
            continue;
        }
        let offset = lengths.piece_offset(vpi);
        let len = lengths.piece_length_of(vpi) as u64;
        for span in layout.spans_for(offset, len) {
            per_file[span.file_index] += span.len;
        }
    }
    per_file
}

fn peer_bitfield_progress(bitfield: &[u8], total_pieces: usize) -> f64 {
    if total_pieces == 0 {
        return 0.0;
    }
    let full_bytes = total_pieces / 8;
    let mut ones: u64 = bitfield[..full_bytes.min(bitfield.len())]
        .iter()
        .map(|b| u64::from(b.count_ones()))
        .sum();
    let trailing_bits = total_pieces % 8;
    if trailing_bits > 0 {
        if let Some(&b) = bitfield.get(full_bytes) {
            let mask = 0xffu8 << (8 - trailing_bits);
            ones += u64::from((b & mask).count_ones());
        }
    }
    ones as f64 / total_pieces as f64
}

fn abort_peer_io(io_abort: Option<&AbortHandle>, cmd_tx: &mpsc::Sender<PeerCommand>) {
    if let Some(handle) = io_abort {
        handle.abort();
    }
    let _ = cmd_tx.try_send(PeerCommand::Disconnect);
}

fn reject_handshook_peer(
    pid: u32,
    cmd_tx: &mpsc::Sender<PeerCommand>,
    io_abort: Option<AbortHandle>,
    outbound_aborts: &mut HashMap<u32, AbortHandle>,
    torrent_id: usize,
    registry_scope: &Arc<()>,
) {
    abort_peer_io(io_abort.as_ref(), cmd_tx);
    cancel_peer_io(pid, None, outbound_aborts, torrent_id, registry_scope);
}

#[allow(clippy::too_many_arguments)]
fn pause_teardown_live_peers(
    peers: &mut HashMap<u32, Peer>,
    useful_peers: &mut HashMap<SocketAddr, usize>,
    piece_tracker: &mut PieceTracker,
    chunk_tracker: &mut ChunkTracker,
    piece_assemblies: &mut HashMap<u32, PieceAssembly>,
    lengths: &Lengths,
    pipeline_floor: usize,
    pipeline_cap: usize,
) -> Vec<SocketAddr> {
    let mut paused_candidates = Vec::with_capacity(peers.len());
    for (pid, p) in peers.drain() {
        if p.downloaded_total > 0 {
            useful_peers.insert(
                p.addr,
                p.max_outstanding.clamp(pipeline_floor, pipeline_cap),
            );
        }
        paused_candidates.push(p.addr);
        abort_peer_io(p.io_abort.as_ref(), &p.cmd_tx);
        release_peer_scheduler_state(
            pid,
            &p.bitfield,
            piece_tracker,
            chunk_tracker,
            piece_assemblies,
            lengths,
        );
    }
    paused_candidates
}

fn cancel_peer_io(
    pid: u32,
    cmd_tx: Option<&mpsc::Sender<PeerCommand>>,
    outbound_aborts: &mut HashMap<u32, AbortHandle>,
    torrent_id: usize,
    registry_scope: &Arc<()>,
) {
    if let Some(handle) = outbound_aborts.remove(&pid) {
        handle.abort();
    }
    if let Some((registry_tx, _, io_abort)) = peer_registry::take(torrent_id, pid, registry_scope) {
        if let Some(handle) = io_abort {
            handle.abort();
        }
        let _ = registry_tx.try_send(PeerCommand::Disconnect);
    }
    if let Some(tx) = cmd_tx {
        let _ = tx.try_send(PeerCommand::Disconnect);
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_blocklist_to_torrent(
    blocklist: &RwLock<BlockList>,
    peers: &mut HashMap<u32, Peer>,
    pending_dials: &mut HashMap<u32, SocketAddr>,
    outbound_aborts: &mut HashMap<u32, AbortHandle>,
    known_addrs: &mut HashSet<SocketAddr>,
    priority_backlog: &mut VecDeque<SocketAddr>,
    peer_backlog: &mut VecDeque<SocketAddr>,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
    useful_peers: &mut HashMap<SocketAddr, usize>,
    pex_source: &mut HashMap<SocketAddr, u32>,
    holepunch_attempted: &mut HashSet<SocketAddr>,
    piece_tracker: &mut PieceTracker,
    chunk_tracker: &mut ChunkTracker,
    piece_assemblies: &mut HashMap<u32, PieceAssembly>,
    lengths: &Lengths,
    torrent_id: usize,
    registry_scope: &Arc<()>,
) -> (u32, u32) {
    let blocked = blocklist.read();
    if blocked.is_empty() {
        return (0, 0);
    }
    let is_blocked = |addr: SocketAddr| blocked.contains(addr.ip());

    let mut disconnected = 0u32;
    let to_kick: Vec<u32> = peers
        .iter()
        .filter(|(_, p)| is_blocked(p.addr))
        .map(|(&pid, _)| pid)
        .collect();
    for pid in to_kick {
        if let Some(p) = peers.remove(&pid) {
            if let Some(handle) = &p.io_abort {
                handle.abort();
            }
            cancel_peer_io(
                pid,
                Some(&p.cmd_tx),
                outbound_aborts,
                torrent_id,
                registry_scope,
            );
            known_addrs.remove(&p.addr);
            useful_peers.remove(&p.addr);
            pex_source.retain(|addr, relay| *relay != pid && !is_blocked(*addr));
            holepunch_attempted.remove(&p.addr);
            release_peer_scheduler_state(
                pid,
                &p.bitfield,
                piece_tracker,
                chunk_tracker,
                piece_assemblies,
                lengths,
            );
            disconnected += 1;
        }
    }

    let mut removed = 0u32;
    let mut purge_queue = |queue: &mut VecDeque<SocketAddr>| {
        let before = queue.len();
        queue.retain(|addr| {
            if is_blocked(*addr) {
                known_addrs.remove(addr);
                false
            } else {
                true
            }
        });
        removed += (before - queue.len()) as u32;
    };
    purge_queue(priority_backlog);
    purge_queue(peer_backlog);

    let mut purge_timed = |queue: &mut VecDeque<(SocketAddr, Instant)>| {
        let before = queue.len();
        queue.retain(|(addr, _)| {
            if is_blocked(*addr) {
                known_addrs.remove(addr);
                false
            } else {
                true
            }
        });
        removed += (before - queue.len()) as u32;
    };
    purge_timed(dial_retries);
    purge_timed(useful_redials);

    pending_dials.retain(|pid, addr| {
        if is_blocked(*addr) {
            known_addrs.remove(addr);
            cancel_peer_io(*pid, None, outbound_aborts, torrent_id, registry_scope);
            removed += 1;
            false
        } else {
            true
        }
    });

    useful_peers.retain(|addr, _| !is_blocked(*addr));
    pex_source.retain(|addr, _| !is_blocked(*addr));
    holepunch_attempted.retain(|addr| !is_blocked(*addr));
    // Drop banned addrs from the "already seen" set so a later unban can redial them from tracker/PEX
    known_addrs.retain(|addr| !is_blocked(*addr));
    drop(blocked);
    (disconnected, removed)
}

/// True when a peer's bitfield reports every piece — a full seeder
fn peer_bitfield_is_full(bitfield: &[u8], total_pieces: usize) -> bool {
    if total_pieces == 0 {
        return false;
    }
    let needed_bytes = total_pieces.div_ceil(8);
    if bitfield.len() < needed_bytes {
        return false;
    }
    let full_bytes = total_pieces / 8;
    if bitfield[..full_bytes].iter().any(|b| *b != 0xff) {
        return false;
    }
    let trailing_bits = total_pieces % 8;
    if trailing_bits == 0 {
        return true;
    }
    let mask: u8 = 0xffu8 << (8 - trailing_bits);
    bitfield[full_bytes] & mask == mask
}

fn should_send_initial_bitfield(bitfield: &[u8]) -> bool {
    bitfield.iter().any(|b| *b != 0)
}

/// Split and dedupe tracker URL strings the same way AddTrackers does
pub(crate) fn normalize_tracker_urls<I, S>(incoming: I, existing: &[String]) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut out = Vec::new();
    for raw in incoming {
        for part in raw
            .as_ref()
            .split([',', '\n', '\r'])
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            if !existing.iter().any(|x| x == part) && !out.iter().any(|x: &String| x == part) {
                out.push(part.to_string());
            }
        }
    }
    out
}

fn collect_tracker_tiers(meta: &TorrentMeta) -> Vec<Vec<String>> {
    let source = if !meta.announce_list.is_empty() {
        meta.announce_list.clone()
    } else {
        meta.announce
            .as_deref()
            .map(|raw| {
                raw.split([',', '\n', '\r'])
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .collect()
    };

    let mut seen = HashSet::new();
    source
        .into_iter()
        .map(|tier| {
            let mut urls = tier
                .into_iter()
                .filter(|url| seen.insert(url.clone()))
                .collect::<Vec<_>>();
            let mut rng = rand::rng();
            for i in (1..urls.len()).rev() {
                let j = (rand::RngExt::random::<u64>(&mut rng) as usize) % (i + 1);
                urls.swap(i, j);
            }
            urls
        })
        .filter(|tier: &Vec<String>| !tier.is_empty())
        .collect()
}

fn flatten_tracker_tiers(tiers: &[Vec<String>]) -> Vec<String> {
    tiers.iter().flatten().cloned().collect()
}

struct TrackerPollers {
    shutdown_tx: watch::Sender<Option<bool>>,
    tasks: tokio::task::JoinSet<()>,
}

impl TrackerPollers {
    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    async fn shutdown(&mut self, announce_stopped: bool) {
        let _ = self.shutdown_tx.send(Some(announce_stopped));
        let deadline = tokio::time::Instant::now() + TRACKER_SHUTDOWN_GRACE;

        while !self.tasks.is_empty() {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, self.tasks.join_next()).await {
                Ok(Some(Ok(()))) => {}
                Ok(Some(Err(e))) if e.is_cancelled() => {}
                Ok(Some(Err(e))) => tracing::warn!("tracker task failed during stop: {e}"),
                Ok(None) => return,
                Err(_) => break,
            }
        }

        if !self.tasks.is_empty() {
            tracing::warn!(
                "tracker shutdown exceeded {:?}; aborting {} poller(s)",
                TRACKER_SHUTDOWN_GRACE,
                self.tasks.len()
            );
            self.tasks.abort_all();
            while let Some(result) = self.tasks.join_next().await {
                if let Err(e) = result {
                    if !e.is_cancelled() {
                        tracing::warn!("tracker task failed after abort: {e}");
                    }
                }
            }
        }
    }
}

fn clamp_tracker_interval(interval: Duration) -> Duration {
    interval.clamp(TRACKER_MIN_INTERVAL, TRACKER_MAX_INTERVAL)
}

fn tracker_retry_delay(consecutive_failures: u32) -> Duration {
    const BACKOFF_SECS: [u64; 5] = [15, 30, 60, 120, 300];
    let idx = consecutive_failures.saturating_sub(1) as usize;
    Duration::from_secs(BACKOFF_SECS[idx.min(BACKOFF_SECS.len() - 1)])
}

#[cfg(test)]
fn tracker_event_for_attempt(
    pending: AnnounceEvent,
    finished: bool,
    sent_completed: bool,
) -> AnnounceEvent {
    if matches!(pending, AnnounceEvent::None) && finished && !sent_completed {
        AnnounceEvent::Completed
    } else {
        pending
    }
}

#[cfg(test)]
fn tracker_event_after_success(sent: AnnounceEvent, sent_completed: &mut bool) -> AnnounceEvent {
    if matches!(sent, AnnounceEvent::Completed) {
        *sent_completed = true;
    }
    AnnounceEvent::None
}

fn tracker_request(
    info_hash: Id20,
    peer_id: Id20,
    key: u32,
    port: u16,
    stats: &Arc<Mutex<TorrentStats>>,
    event: AnnounceEvent,
    num_want: u32,
) -> AnnounceRequest {
    let (uploaded, downloaded, left) = {
        let stats = stats.lock();
        (
            stats.uploaded_bytes,
            stats.progress_bytes,
            stats.total_bytes.saturating_sub(stats.progress_bytes),
        )
    };
    AnnounceRequest {
        info_hash,
        peer_id,
        key,
        port,
        uploaded,
        downloaded,
        left,
        event,
        num_want,
    }
}

#[derive(Clone)]
struct TrackerUrlState {
    url: String,
    next_at: Instant,
    failures: u32,
    started_sent: bool,
    completed_sent: bool,
}

#[derive(Clone)]
struct TrackerRunState {
    tiers: Vec<Vec<TrackerUrlState>>,
    active_tier: usize,
    active_url: usize,
    last_successful_tier: Option<usize>,
}

#[derive(Default)]
struct TrackerLifecycle {
    by_info_hash: HashMap<Id20, TrackerRunState>,
}

impl TrackerLifecycle {
    fn run_state(&mut self, info_hash: Id20, tiers: &[Vec<String>]) -> &mut TrackerRunState {
        self.by_info_hash.entry(info_hash).or_insert_with(|| {
            let now = Instant::now();
            TrackerRunState {
                tiers: tiers
                    .iter()
                    .map(|tier| {
                        tier.iter()
                            .cloned()
                            .map(|url| TrackerUrlState {
                                url,
                                next_at: now,
                                failures: 0,
                                started_sent: false,
                                completed_sent: false,
                            })
                            .collect()
                    })
                    .collect(),
                active_tier: 0,
                active_url: 0,
                last_successful_tier: None,
            }
        })
    }

    fn append_tier(&mut self, urls: &[String]) {
        if urls.is_empty() {
            return;
        }
        let now = Instant::now();
        for state in self.by_info_hash.values_mut() {
            state.tiers.push(
                urls.iter()
                    .cloned()
                    .map(|url| TrackerUrlState {
                        url,
                        next_at: now,
                        failures: 0,
                        started_sent: false,
                        completed_sent: false,
                    })
                    .collect(),
            );
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_tracker_tier_poller(
    tx: mpsc::Sender<PeerEnvelope>,
    tiers: Vec<Vec<String>>,
    info_hash: Id20,
    peer_id: Id20,
    key: u32,
    port: u16,
    stats: Arc<Mutex<TorrentStats>>,
    mut shutdown: watch::Receiver<Option<bool>>,
    proxy: Option<risuko_http::ProxyConnector>,
    lifecycle: Arc<Mutex<TrackerLifecycle>>,
    tier_change_tx: Option<mpsc::Sender<()>>,
    tracker_source_addr: Option<SocketAddr>,
    generation: GenerationToken,
) {
    if tiers.is_empty() {
        return;
    }

    'poll: loop {
        if shutdown.borrow().is_some() {
            break;
        }
        let selected = {
            let mut lifecycle = lifecycle.lock();
            let state = lifecycle.run_state(info_hash, &tiers);
            if state.tiers.is_empty() {
                break;
            }
            if state.active_tier >= state.tiers.len() {
                state.active_tier = 0;
                state.active_url = 0;
            }
            if state.tiers[state.active_tier].is_empty() {
                state.active_tier = (state.active_tier + 1) % state.tiers.len();
                state.active_url = 0;
            }
            let (index, selected) = state.tiers[state.active_tier]
                .iter()
                .enumerate()
                .min_by_key(|(_, entry)| entry.next_at)
                .expect("non-empty tracker tier");
            state.active_url = index;
            selected.clone()
        };
        let wait = selected.next_at.saturating_duration_since(Instant::now());
        if !wait.is_zero() {
            tokio::select! {
                biased;
                _ = shutdown.changed() => break 'poll,
                _ = tokio::time::sleep(wait) => {}
            }
            continue;
        }

        let finished = stats.lock().finished;
        let event = if !selected.started_sent {
            AnnounceEvent::Started
        } else if finished && !selected.completed_sent {
            AnnounceEvent::Completed
        } else {
            AnnounceEvent::None
        };
        let req = tracker_request(info_hash, peer_id, key, port, &stats, event, 200);
        let request_generation = generation.current();
        let result = tokio::select! {
            biased;
            _ = shutdown.changed() => break 'poll,
            result = super::tracker::announce_with_proxy_and_source(
                &selected.url,
                &req,
                TRACKER_ANNOUNCE_TIMEOUT,
                proxy.as_ref(),
                tracker_source_addr,
            ) => result,
        };
        match result {
            Ok(resp) => {
                tracing::info!(target: "diag", "tracker ANNOUNCE ok url={} event={event:?} peers={} interval_s={}", selected.url, resp.peers.len(), resp.interval.as_secs());
                let changed_tier = {
                    let mut lifecycle = lifecycle.lock();
                    let state = lifecycle.run_state(info_hash, &tiers);
                    let selected_tier = state.active_tier;
                    let tier = &mut state.tiers[state.active_tier];
                    if let Some(index) = tier.iter().position(|entry| entry.url == selected.url) {
                        let mut updated = tier.remove(index);
                        updated.failures = 0;
                        if event == AnnounceEvent::Started {
                            updated.started_sent = true;
                        } else if event == AnnounceEvent::Completed {
                            updated.completed_sent = true;
                        }
                        updated.next_at = Instant::now() + clamp_tracker_interval(resp.interval);
                        tier.insert(0, updated);
                        state.active_url = 0;
                    }
                    state
                        .last_successful_tier
                        .replace(selected_tier)
                        .is_some_and(|previous| previous != selected_tier)
                };
                if changed_tier {
                    if let Some(tx) = &tier_change_tx {
                        let _ = tx.try_send(());
                    }
                }
                for addr in resp.peers {
                    let sent = tokio::select! {
                        biased;
                        _ = shutdown.changed() => break 'poll,
                        sent = tx.send(PeerEnvelope {
                            generation: request_generation,
                            candidate: PeerCandidate { addr, source: PeerSource::Tracker },
                        }) => sent,
                    };
                    if sent.is_err() {
                        break 'poll;
                    }
                }
            }
            Err(e) => {
                let changed_tier = {
                    let mut lifecycle = lifecycle.lock();
                    let state = lifecycle.run_state(info_hash, &tiers);
                    let tier_index = state.active_tier;
                    let previous_tier = tier_index;
                    if let Some(index) = state.tiers[tier_index]
                        .iter()
                        .position(|entry| entry.url == selected.url)
                    {
                        let entry = &mut state.tiers[tier_index][index];
                        entry.failures = entry.failures.saturating_add(1);
                        let delay = tracker_retry_delay(entry.failures);
                        entry.next_at = Instant::now() + delay;
                        tracing::info!(target: "diag", "tracker ANNOUNCE fail url={} event={event:?} retry_s={} err={e}", selected.url, delay.as_secs());
                        state.active_url = index;
                        let tier_failed = state.tiers[tier_index]
                            .iter()
                            .all(|entry| entry.failures > 0);
                        if tier_failed {
                            state.active_tier = (state.active_tier + 1) % state.tiers.len();
                            state.active_url = 0;
                            let now = Instant::now();
                            for entry in &mut state.tiers[state.active_tier] {
                                entry.failures = 0;
                                entry.next_at = now;
                            }
                        }
                    }
                    state.active_tier != previous_tier
                };
                if changed_tier {
                    if let Some(tx) = &tier_change_tx {
                        let _ = tx.try_send(());
                    }
                }
            }
        }
    }

    if !(*shutdown.borrow()).unwrap_or(true) {
        return;
    }
    let started = lifecycle
        .lock()
        .run_state(info_hash, &tiers)
        .tiers
        .iter()
        .flatten()
        .filter(|state| state.started_sent)
        .cloned()
        .collect::<Vec<_>>();
    for state in started {
        let stopped = tracker_request(
            info_hash,
            peer_id,
            key,
            port,
            &stats,
            AnnounceEvent::Stopped,
            0,
        );
        match super::tracker::announce_with_proxy_and_source(
            &state.url,
            &stopped,
            TRACKER_STOPPED_TIMEOUT,
            proxy.as_ref(),
            tracker_source_addr,
        )
        .await
        {
            Ok(_) => tracing::debug!("tracker STOPPED ok url={}", state.url),
            Err(e) => tracing::debug!("tracker STOPPED failed url={}: {e}", state.url),
        }
    }
}

#[cfg(test)]
fn spawn_tracker_pollers(
    tx: mpsc::Sender<PeerEnvelope>,
    trackers: Vec<String>,
    info_hashes: Vec<Id20>,
    peer_id: Id20,
    port: u16,
    stats: Arc<Mutex<TorrentStats>>,
    proxy: Option<risuko_http::ProxyConnector>,
) -> TrackerPollers {
    let tiers = if trackers.is_empty() {
        Vec::new()
    } else {
        vec![trackers]
    };
    let key = info_hashes
        .first()
        .map(|h| u32::from_be_bytes(h.0[..4].try_into().unwrap()))
        .unwrap_or_default();
    spawn_tracker_tier_pollers(
        tx,
        tiers,
        info_hashes,
        peer_id,
        key,
        port,
        stats,
        proxy,
        Arc::new(Mutex::new(TrackerLifecycle::default())),
        None,
        None,
        GenerationToken::new(),
    )
}

#[allow(clippy::too_many_arguments)]
fn spawn_tracker_tier_pollers(
    tx: mpsc::Sender<PeerEnvelope>,
    tiers: Vec<Vec<String>>,
    info_hashes: Vec<Id20>,
    peer_id: Id20,
    key: u32,
    port: u16,
    stats: Arc<Mutex<TorrentStats>>,
    proxy: Option<risuko_http::ProxyConnector>,
    lifecycle: Arc<Mutex<TrackerLifecycle>>,
    tier_change_tx: Option<mpsc::Sender<()>>,
    tracker_source_addr: Option<SocketAddr>,
    generation: GenerationToken,
) -> TrackerPollers {
    let (shutdown_tx, shutdown_rx) = watch::channel(None);
    let mut tasks = tokio::task::JoinSet::new();
    for info_hash in info_hashes {
        if !tiers.is_empty() {
            tasks.spawn(run_tracker_tier_poller(
                tx.clone(),
                tiers.clone(),
                info_hash,
                peer_id,
                key,
                port,
                Arc::clone(&stats),
                shutdown_rx.clone(),
                proxy.clone(),
                Arc::clone(&lifecycle),
                tier_change_tx.clone(),
                tracker_source_addr,
                generation.clone(),
            ));
        }
    }
    TrackerPollers { shutdown_tx, tasks }
}

fn release_peer_scheduler_state(
    pid: u32,
    bitfield: &[u8],
    piece_tracker: &mut PieceTracker,
    chunk_tracker: &mut ChunkTracker,
    piece_assemblies: &mut HashMap<u32, PieceAssembly>,
    lengths: &Lengths,
) {
    piece_tracker.remove_peer_bitfield(bitfield);
    let freed = chunk_tracker.release_peer(pid);
    for piece_idx in freed {
        if let Ok(vpi) = lengths.validate_piece(piece_idx) {
            piece_tracker.clear_in_flight(vpi);
        }
        let should_drop = piece_assemblies
            .get(&piece_idx)
            .is_none_or(|a| a.received_chunks.is_empty());
        if should_drop {
            piece_assemblies.remove(&piece_idx);
        }
    }
}

/// Build a `HASHES` / `HashReject` reply for an inbound BEP 52 `HASH_REQUEST`
fn build_hash_response(
    tables: Option<&[MerkleProofTable]>,
    pieces_root: [u8; 32],
    base_layer: u32,
    index: u32,
    length: u32,
    proof_layers: u32,
) -> Message {
    let reject = || Message::HashReject {
        pieces_root,
        base_layer,
        index,
        length,
        proof_layers,
    };

    let Some(tables) = tables else {
        return reject();
    };

    // Only support the "entire piece layer at once, no proof" shape
    if index != 0 || proof_layers != 0 {
        return reject();
    }

    let root = super::core::Id32(pieces_root);
    let Some(table) = tables.iter().find(|t| t.file_root == root) else {
        return reject();
    };
    if base_layer != table.piece_layer_base() || length != table.piece_layer_padded_len() {
        return reject();
    }
    let Some(payload) = table.serve_full_piece_layer() else {
        return reject();
    };
    Message::Hashes {
        pieces_root,
        base_layer,
        index,
        length,
        proof_layers,
        hashes: bytes::Bytes::from(payload),
    }
}

/// Serve a single inbound BEP-9 `ut_metadata` request; the peer's payload is a bencoded `{msg_type, piece}` dict (DATA / REQUEST / REJECT) and we only act on REQUEST — for in-range pieces reply with DATA carrying the corresponding 16 KiB block of `info_bytes`, otherwise REJECT
fn serve_ut_metadata(peer: &Peer, payload: &Bytes, info_bytes: &Arc<Vec<u8>>) {
    let Some(msg) = super::wire::extended::parse_ut_metadata(payload.clone()) else {
        return;
    };
    if msg.msg_type != ut_metadata_type::REQUEST {
        // We never initiate a request from the torrent loop, so DATA / REJECT replies here are unsolicited — ignore
        return;
    }
    let total = info_bytes.len();
    let total_pieces = total.div_ceil(META_PIECE_SIZE);
    let piece = msg.piece;
    if piece < 0 || (piece as usize) >= total_pieces {
        // Only send REJECT when the peer has told us which ext_id to use; before their handshake, sending on an id they don't recognise is useless and potentially confusing
        if let Some(their_id) = peer.their_ut_metadata_id {
            let reject = super::wire::extended::ut_metadata_reject(piece);
            let _ = peer.cmd_tx.try_send(PeerCommand::Send(Message::Extended {
                ext_id: their_id,
                payload: reject,
            }));
        }
        return;
    }
    let Some(their_id) = peer.their_ut_metadata_id else {
        // Peer asked for ut_metadata without first telling us which ext id to use; drop silently — a well-behaved client always sends its handshake first
        return;
    };
    let start = piece as usize * META_PIECE_SIZE;
    let end = (start + META_PIECE_SIZE).min(total);
    let block = &info_bytes[start..end];
    let data = ut_metadata_data(piece, total as i64, block);
    // try_send: never block the torrent loop on a slow peer's writer queue
    let _ = peer.cmd_tx.try_send(PeerCommand::Send(Message::Extended {
        ext_id: their_id,
        payload: data,
    }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::merkle::{compute_root, hash_block, MerkleProofTable, BLOCK_SIZE};
    use crate::core::Id32;
    use crate::core::TorrentMetaInfo;
    use crate::core::ValidatedTorrentMetaV1Info;
    use std::path::Path;

    fn test_peer(port: u16) -> SocketAddr {
        SocketAddr::from(([203, 0, 113, 1], port))
    }

    #[test]
    fn enqueue_skips_blocked_peer() {
        let mut list = BlockList::default();
        list.replace(&[test_peer(6881).ip().to_string()]);
        let mut priority_backlog = VecDeque::new();
        let mut peer_backlog = VecDeque::new();
        let mut dial_retries = VecDeque::new();
        let mut useful_redials = VecDeque::new();
        let mut known_addrs = HashSet::new();
        assert!(!enqueue_peer_candidate(
            test_peer(6881),
            false,
            &mut priority_backlog,
            &mut peer_backlog,
            &mut dial_retries,
            &mut useful_redials,
            &mut known_addrs,
            &RwLock::new(list),
        ));
        assert!(priority_backlog.is_empty());
        assert!(peer_backlog.is_empty());
        assert!(known_addrs.is_empty());
    }

    #[test]
    fn peer_queue_normalization_deduplicates_by_precedence_and_rebuilds_known_set() {
        let now = Instant::now();
        let active_a = test_peer(10_001);
        let active_b = test_peer(10_002);
        let priority_a = test_peer(10_003);
        let priority_b = test_peer(10_004);
        let useful = test_peer(10_005);
        let retry = test_peer(10_006);
        let cold = test_peer(10_007);
        let mut active = HashSet::from([active_a, active_b]);
        let mut priority_backlog = VecDeque::from([priority_a, priority_b, active_a]);
        let mut useful_redials = VecDeque::from([
            (priority_b, now + Duration::from_secs(1)),
            (useful, now + Duration::from_secs(2)),
        ]);
        let mut dial_retries = VecDeque::from([
            (useful, now + Duration::from_secs(3)),
            (retry, now + Duration::from_secs(4)),
        ]);
        let mut peer_backlog = VecDeque::from([retry, cold, active_b]);
        let mut known_addrs = HashSet::from([test_peer(19_999)]);

        normalize_peer_queues(
            &mut priority_backlog,
            &mut peer_backlog,
            &mut dial_retries,
            &mut useful_redials,
            &active,
            &mut known_addrs,
        );

        assert_eq!(priority_backlog, VecDeque::from([priority_a, priority_b]));
        assert_eq!(
            useful_redials
                .iter()
                .map(|(addr, _)| *addr)
                .collect::<Vec<_>>(),
            vec![useful]
        );
        assert_eq!(
            dial_retries
                .iter()
                .map(|(addr, _)| *addr)
                .collect::<Vec<_>>(),
            vec![retry]
        );
        assert_eq!(peer_backlog, VecDeque::from([cold]));
        active.extend([priority_a, priority_b, useful, retry, cold]);
        assert_eq!(known_addrs, active);
    }

    #[test]
    fn peer_queue_normalization_caps_combined_unique_candidates() {
        let mut priority_backlog = VecDeque::new();
        let mut peer_backlog = (0..MAX_PEER_BACKLOG + 100)
            .map(|i| test_peer(20_000 + i as u16))
            .collect::<VecDeque<_>>();
        let mut dial_retries = VecDeque::new();
        let mut useful_redials = VecDeque::new();
        let active = HashSet::from([test_peer(10_001)]);
        let mut known_addrs = HashSet::new();

        normalize_peer_queues(
            &mut priority_backlog,
            &mut peer_backlog,
            &mut dial_retries,
            &mut useful_redials,
            &active,
            &mut known_addrs,
        );

        let queued =
            priority_backlog.len() + peer_backlog.len() + dial_retries.len() + useful_redials.len();
        assert_eq!(queued, MAX_PEER_BACKLOG);
        assert_eq!(known_addrs.len(), MAX_PEER_BACKLOG + active.len());
        assert!(active.is_subset(&known_addrs));
    }

    #[test]
    fn holepunch_promotion_moves_delayed_candidate_to_priority_front() {
        let now = Instant::now();
        let active = HashSet::from([test_peer(11_001)]);
        let existing_priority = test_peer(11_002);
        let target = test_peer(11_003);
        let cold = test_peer(11_004);
        let mut priority_backlog = VecDeque::from([existing_priority]);
        let mut peer_backlog = VecDeque::from([cold]);
        let mut dial_retries = VecDeque::from([(target, now + Duration::from_secs(30))]);
        let mut useful_redials = VecDeque::from([(target, now + Duration::from_secs(10))]);
        let mut known_addrs = HashSet::new();

        assert!(promote_holepunch_candidate(
            target,
            &active,
            &mut priority_backlog,
            &mut peer_backlog,
            &mut dial_retries,
            &mut useful_redials,
            &mut known_addrs,
        ));

        assert_eq!(
            priority_backlog,
            VecDeque::from([target, existing_priority])
        );
        assert!(!peer_backlog.contains(&target));
        assert!(!dial_retries.iter().any(|(addr, _)| *addr == target));
        assert!(!useful_redials.iter().any(|(addr, _)| *addr == target));
        let expected = active
            .iter()
            .copied()
            .chain(priority_backlog.iter().copied())
            .chain(peer_backlog.iter().copied())
            .chain(dial_retries.iter().map(|(addr, _)| *addr))
            .chain(useful_redials.iter().map(|(addr, _)| *addr))
            .collect::<HashSet<_>>();
        assert_eq!(known_addrs, expected);
    }

    #[test]
    fn holepunch_promotion_does_not_redial_active_or_pending_target() {
        let now = Instant::now();
        let target = test_peer(12_001);
        let active = HashSet::from([target, test_peer(12_002)]);
        let mut priority_backlog = VecDeque::from([target, test_peer(12_003)]);
        let mut peer_backlog = VecDeque::from([target, test_peer(12_004)]);
        let mut dial_retries = VecDeque::from([(target, now + Duration::from_secs(30))]);
        let mut useful_redials = VecDeque::from([(target, now + Duration::from_secs(10))]);
        let mut known_addrs = HashSet::new();

        assert!(!promote_holepunch_candidate(
            target,
            &active,
            &mut priority_backlog,
            &mut peer_backlog,
            &mut dial_retries,
            &mut useful_redials,
            &mut known_addrs,
        ));

        assert!(!priority_backlog.contains(&target));
        assert!(!peer_backlog.contains(&target));
        assert!(!dial_retries.iter().any(|(addr, _)| *addr == target));
        assert!(!useful_redials.iter().any(|(addr, _)| *addr == target));
        let expected = active
            .iter()
            .copied()
            .chain(priority_backlog.iter().copied())
            .chain(peer_backlog.iter().copied())
            .chain(dial_retries.iter().map(|(addr, _)| *addr))
            .chain(useful_redials.iter().map(|(addr, _)| *addr))
            .collect::<HashSet<_>>();
        assert_eq!(known_addrs, expected);
    }

    #[test]
    fn holepunch_connect_skips_blocked_target() {
        let target = test_peer(14_001);
        let mut list = BlockList::default();
        list.replace(&[target.ip().to_string()]);
        let (from_cmd, _rx) = mpsc::channel(1);
        let peers = HashMap::new();
        let pending_dials = HashMap::new();
        let mut priority_backlog = VecDeque::new();
        let mut peer_backlog = VecDeque::new();
        let mut dial_retries = VecDeque::new();
        let mut useful_redials = VecDeque::new();
        let mut known_addrs = HashSet::new();
        let mut holepunch_targets = HashSet::new();

        handle_holepunch(
            HolepunchMsg {
                msg_type: holepunch_type::CONNECT,
                addr: target,
                err_code: 0,
            },
            test_peer(14_002),
            None,
            &from_cmd,
            &peers,
            &pending_dials,
            &mut priority_backlog,
            &mut peer_backlog,
            &mut dial_retries,
            &mut useful_redials,
            &mut known_addrs,
            &mut holepunch_targets,
            &RwLock::new(list),
        );

        assert!(priority_backlog.is_empty());
        assert!(peer_backlog.is_empty());
        assert!(dial_retries.is_empty());
        assert!(useful_redials.is_empty());
        assert!(known_addrs.is_empty());
        assert!(holepunch_targets.is_empty());
    }

    #[test]
    fn holepunch_connect_marks_target_for_utp_first_dial() {
        let target = test_peer(14_003);
        let (from_cmd, _rx) = mpsc::channel(1);
        let peers = HashMap::new();
        let pending_dials = HashMap::new();
        let mut priority_backlog = VecDeque::new();
        let mut peer_backlog = VecDeque::new();
        let mut dial_retries = VecDeque::new();
        let mut useful_redials = VecDeque::new();
        let mut known_addrs = HashSet::new();
        let mut holepunch_targets = HashSet::new();

        handle_holepunch(
            HolepunchMsg {
                msg_type: holepunch_type::CONNECT,
                addr: target,
                err_code: 0,
            },
            test_peer(14_004),
            None,
            &from_cmd,
            &peers,
            &pending_dials,
            &mut priority_backlog,
            &mut peer_backlog,
            &mut dial_retries,
            &mut useful_redials,
            &mut known_addrs,
            &mut holepunch_targets,
            &RwLock::new(BlockList::default()),
        );

        assert_eq!(priority_backlog, VecDeque::from([target]));
        assert!(holepunch_targets.contains(&target));
    }

    #[test]
    fn holepunch_promotion_preserves_cap_and_evicts_cold_tail() {
        let now = Instant::now();
        let existing_priority = test_peer(13_001);
        let useful = test_peer(13_002);
        let retry = test_peer(13_003);
        let mut priority_backlog = VecDeque::from([existing_priority]);
        let mut useful_redials = VecDeque::from([(useful, now + Duration::from_secs(10))]);
        let mut dial_retries = VecDeque::from([(retry, now + Duration::from_secs(30))]);
        let mut peer_backlog = (0..MAX_PEER_BACKLOG - 3)
            .map(|i| test_peer(20_000 + i as u16))
            .collect::<VecDeque<_>>();
        let evicted = *peer_backlog.back().expect("full combined queue");
        let target = test_peer(45_001);
        let active = HashSet::from([test_peer(13_004)]);
        let mut known_addrs = HashSet::new();

        assert!(promote_holepunch_candidate(
            target,
            &active,
            &mut priority_backlog,
            &mut peer_backlog,
            &mut dial_retries,
            &mut useful_redials,
            &mut known_addrs,
        ));

        assert_eq!(priority_backlog.front(), Some(&target));
        assert!(!known_addrs.contains(&evicted));
        let queued =
            priority_backlog.len() + peer_backlog.len() + dial_retries.len() + useful_redials.len();
        assert_eq!(queued, MAX_PEER_BACKLOG);
        let expected = active
            .iter()
            .copied()
            .chain(priority_backlog.iter().copied())
            .chain(peer_backlog.iter().copied())
            .chain(dial_retries.iter().map(|(addr, _)| *addr))
            .chain(useful_redials.iter().map(|(addr, _)| *addr))
            .collect::<HashSet<_>>();
        assert_eq!(known_addrs, expected);
    }

    #[test]
    fn priority_peer_displaces_lowest_priority_cold_candidate_when_full() {
        let mut priority_backlog = VecDeque::new();
        let mut peer_backlog = (0..MAX_PEER_BACKLOG)
            .map(|i| test_peer(30_000 + i as u16))
            .collect::<VecDeque<_>>();
        let evicted = *peer_backlog.back().expect("full backlog");
        let mut dial_retries = VecDeque::new();
        let mut useful_redials = VecDeque::new();
        let mut known_addrs = peer_backlog.iter().copied().collect::<HashSet<_>>();
        let priority = test_peer(45_000);

        assert!(enqueue_peer_candidate(
            priority,
            true,
            &mut priority_backlog,
            &mut peer_backlog,
            &mut dial_retries,
            &mut useful_redials,
            &mut known_addrs,
            &RwLock::new(BlockList::default()),
        ));

        assert_eq!(priority_backlog, VecDeque::from([priority]));
        assert_eq!(peer_backlog.len(), MAX_PEER_BACKLOG - 1);
        assert!(known_addrs.contains(&priority));
        assert!(!known_addrs.contains(&evicted));
        assert_eq!(known_addrs.len(), MAX_PEER_BACKLOG);
    }

    #[test]
    fn dial_slots_count_pending_handshakes_against_peer_cap() {
        assert!(!dial_slot_available(99, 1, 100, MAX_PENDING_DIALS));
        assert!(!dial_slot_available(52, 48, 100, MAX_PENDING_DIALS));
        assert!(dial_slot_available(51, 47, 100, MAX_PENDING_DIALS));
        assert!(!dial_slot_available(0, 36, 100, 36));
    }

    #[test]
    fn pipeline_slow_start_requires_a_full_successful_turnover() {
        assert_eq!(pipeline_slow_start_target(5, 6, 256), None);
        assert_eq!(pipeline_slow_start_target(6, 6, 256), Some(12));
    }

    #[test]
    fn pipeline_slow_start_doubles_and_clamps_to_probe_or_peer_cap() {
        assert_eq!(pipeline_slow_start_target(12, 12, 256), Some(24));
        assert_eq!(pipeline_slow_start_target(24, 24, 256), Some(48));
        assert_eq!(pipeline_slow_start_target(48, 48, 256), Some(64));
        assert_eq!(pipeline_slow_start_target(6, 6, 10), Some(10));
        assert_eq!(pipeline_slow_start_target(64, 64, 256), None);
        assert_eq!(pipeline_slow_start_target(10, 10, 10), None);
    }

    #[test]
    fn pipeline_target_tracks_delivery_rate() {
        assert_eq!(pipeline_target(128, Duration::from_secs(2), 6, 256, 6), 14);
        assert_eq!(pipeline_target(2, Duration::from_secs(2), 6, 256, 6), 6);
        assert_eq!(pipeline_target(20, Duration::from_secs(2), 6, 256, 6), 14);
        assert_eq!(pipeline_target(128, Duration::from_secs(2), 32, 32, 32), 32);
        assert_eq!(pipeline_target(128, Duration::from_secs(2), 6, 256, 14), 22);
        assert_eq!(pipeline_target(128, Duration::from_secs(2), 6, 256, 64), 72);
        assert_eq!(pipeline_target(128, Duration::from_secs(2), 6, 256, 72), 80);
    }

    #[test]
    fn slow_start_and_rate_control_are_mutually_exclusive_per_piece() {
        let elapsed = Duration::from_secs(2);
        assert_eq!(
            pipeline_adjustment(true, 6, 128, elapsed, 6, 256, 6),
            Some(PipelineAdjustment::SlowStart {
                target: 12,
                finished: false,
            })
        );
        assert_eq!(
            pipeline_adjustment(false, 64, 128, elapsed, 6, 256, 64),
            Some(PipelineAdjustment::Rate { target: 72 })
        );
    }

    #[test]
    fn configured_outstanding_is_cap_not_floor() {
        let (floor, cap) = pipeline_bounds(Some(128));
        assert_eq!((floor, cap), (6, 128));
        assert_ne!(floor, cap);
        assert_eq!(shrink_pipeline(cap, floor), 64);
        assert_eq!(shrink_pipeline(64, floor), 32);
    }

    #[test]
    fn adaptive_outstanding_uses_a_lower_default_but_keeps_explicit_hard_cap() {
        assert_eq!(pipeline_bounds(None), (6, 96));
        assert_eq!(pipeline_bounds(Some(256)), (6, 256));
        assert_eq!(pipeline_bounds(Some(512)), (6, 256));
        assert_eq!(pipeline_bounds(Some(1)), (1, 1));
    }

    #[test]
    fn torrent_request_budget_is_divided_fairly_across_active_peers() {
        assert_eq!(effective_request_limit(256, 1), 256);
        assert_eq!(effective_request_limit(256, 2), 256);
        assert_eq!(effective_request_limit(256, 18), 56);
        assert_eq!(effective_request_limit(256, 100), 10);
        assert_eq!(effective_request_limit(96, 18), 56);
        assert_eq!(effective_request_limit(96, 0), 0);
    }

    #[test]
    fn pipeline_depth_shrinks_after_stall() {
        // The reclaim path applies exactly this on each snub transition
        assert_eq!(shrink_pipeline(256, 6), 128);
        assert_eq!(shrink_pipeline(128, 6), 64);
        assert_eq!(shrink_pipeline(8, 6), 6);
        assert_eq!(shrink_pipeline(6, 6), 6);
    }

    fn make_v2_tables(num_pieces: usize, piece_length: u32) -> (Vec<MerkleProofTable>, Id32) {
        let blocks_per_piece = piece_length / BLOCK_SIZE;
        let total = num_pieces as u64 * piece_length as u64;
        let mut data = vec![0u8; total as usize];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(31).wrapping_add(7);
        }
        // Per-piece root: SHA-256 Merkle subtree over 16 KiB blocks, zero-padded to `blocks_per_piece`
        let piece_roots: Vec<Id32> = data
            .chunks(piece_length as usize)
            .map(|p| {
                let mut leaves: Vec<Id32> = p.chunks(BLOCK_SIZE as usize).map(hash_block).collect();
                let target = blocks_per_piece as usize;
                if leaves.len() < target {
                    leaves.resize(target, Id32([0u8; 32]));
                }
                if target == 1 {
                    leaves[0]
                } else {
                    compute_root(&leaves)
                }
            })
            .collect();
        let file_root = if piece_roots.len() == 1 {
            piece_roots[0]
        } else {
            compute_root(&piece_roots)
        };
        let mut layer_bytes = Vec::with_capacity(piece_roots.len() * 32);
        for r in &piece_roots {
            layer_bytes.extend_from_slice(&r.0);
        }
        let table =
            MerkleProofTable::from_layer_bytes(file_root, total, piece_length, &layer_bytes)
                .expect("build table");
        (vec![table], file_root)
    }

    fn two_file_layout() -> (Lengths, FileSet) {
        let info = ValidatedTorrentMetaV1Info {
            name: "root".into(),
            piece_length: 10,
            pieces: vec![0; 20 * 3],
            private: false,
            files: vec![
                TorrentMetaInfo {
                    path: vec!["a".into()],
                    length: 15,
                },
                TorrentMetaInfo {
                    path: vec!["b".into()],
                    length: 15,
                },
            ],
            single_file_mode: false,
        };
        let lengths = Lengths::new(30, 10).unwrap();
        let layout = FileSet::from_meta(&info, Path::new("/tmp"));
        (lengths, layout)
    }

    #[test]
    fn tracker_intervals_and_failures_are_bounded() {
        assert_eq!(
            clamp_tracker_interval(Duration::from_secs(1)),
            TRACKER_MIN_INTERVAL
        );
        assert_eq!(
            clamp_tracker_interval(Duration::from_secs(24 * 60 * 60)),
            TRACKER_MAX_INTERVAL
        );
        assert_eq!(tracker_retry_delay(1), Duration::from_secs(15));
        assert_eq!(tracker_retry_delay(2), Duration::from_secs(30));
        assert_eq!(tracker_retry_delay(5), Duration::from_secs(300));
        assert_eq!(tracker_retry_delay(99), Duration::from_secs(300));
    }

    #[test]
    fn tracker_started_and_completed_remain_pending_until_success() {
        let mut sent_completed = false;
        let mut pending = AnnounceEvent::Started;

        assert_eq!(
            tracker_event_for_attempt(pending, false, sent_completed),
            AnnounceEvent::Started
        );
        pending = tracker_event_after_success(pending, &mut sent_completed);
        assert_eq!(pending, AnnounceEvent::None);

        pending = tracker_event_for_attempt(pending, true, sent_completed);
        assert_eq!(pending, AnnounceEvent::Completed);
        assert_eq!(
            tracker_event_for_attempt(pending, true, sent_completed),
            AnnounceEvent::Completed
        );
        pending = tracker_event_after_success(pending, &mut sent_completed);
        assert_eq!(pending, AnnounceEvent::None);
        assert!(sent_completed);
        assert_eq!(
            tracker_event_for_attempt(pending, true, sent_completed),
            AnnounceEvent::None
        );
    }

    #[test]
    fn verified_piece_progress_is_idempotent() {
        let (lengths, layout) = two_file_layout();
        let mut stats = TorrentStats::initial(
            lengths.total_length(),
            layout.files().iter().map(|f| f.length).collect(),
        );
        let mut tracker = PieceTracker::new(lengths);
        let piece = tracker.lengths().validate_piece(1).unwrap();

        if mark_verified_piece_local(&mut tracker, piece) {
            add_piece_progress(&mut stats, tracker.lengths(), &layout, piece);
        }
        assert_eq!(stats.progress_bytes, 10);
        assert_eq!(stats.file_progress, vec![5, 5]);

        if mark_verified_piece_local(&mut tracker, piece) {
            add_piece_progress(&mut stats, tracker.lengths(), &layout, piece);
        }
        assert_eq!(stats.progress_bytes, 10);
        assert_eq!(stats.file_progress, vec![5, 5]);
    }

    #[test]
    fn peer_registry_scope_prevents_cross_session_take() {
        let torrent_id = 7;
        let pid = 3;
        let addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let scope_a = Arc::new(());
        let scope_b = Arc::new(());
        let (tx, _rx) = mpsc::channel(1);

        peer_registry::put(torrent_id, pid, &scope_a, tx, addr, None);

        assert!(peer_registry::take(torrent_id, pid, &scope_b).is_none());
        assert!(peer_registry::take(torrent_id, pid, &scope_a).is_some());
    }

    #[test]
    fn peer_registry_drain_scope_is_isolated_by_torrent_and_generation() {
        let torrent_a = 91_001;
        let torrent_b = 91_002;
        let scope_a = Arc::new(());
        let scope_b = Arc::new(());
        let addr_a = test_peer(51_001);
        let addr_b = test_peer(51_002);
        let addr_other_torrent = test_peer(51_003);
        let (tx_a, _rx_a) = mpsc::channel(1);
        let (tx_b, _rx_b) = mpsc::channel(1);
        let (tx_other, _rx_other) = mpsc::channel(1);

        peer_registry::put(torrent_a, 1, &scope_a, tx_a, addr_a, None);
        peer_registry::put(torrent_a, 2, &scope_b, tx_b, addr_b, None);
        peer_registry::put(torrent_b, 1, &scope_a, tx_other, addr_other_torrent, None);

        let drained = peer_registry::drain_scope(torrent_a, &scope_a);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].0, 1);
        assert_eq!(drained[0].2, addr_a);
        assert!(peer_registry::take(torrent_a, 1, &scope_a).is_none());
        assert!(peer_registry::take(torrent_a, 2, &scope_b).is_some());
        assert!(peer_registry::take(torrent_b, 1, &scope_a).is_some());
    }

    #[tokio::test]
    async fn apply_blocklist_cancels_peers_when_command_channel_is_full() {
        let mut list = BlockList::default();
        list.replace(&["127.0.0.1".into()]);
        let blocklist = RwLock::new(list);

        let live_addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let pending_addr: SocketAddr = "127.0.0.1:6882".parse().unwrap();
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(PeerCommand::Send(Message::KeepAlive)).unwrap();
        let extra = tx.clone();

        let mut peers = HashMap::new();
        peers.insert(
            1,
            Peer::connected(live_addr, tx, 1, 1, 1, true, false, None),
        );

        let (pending_tx, _pending_rx) = mpsc::channel(1);
        pending_tx
            .try_send(PeerCommand::Send(Message::KeepAlive))
            .unwrap();
        let pending_extra = pending_tx.clone();
        let pending_io = tokio::spawn(async move {
            pending_extra.closed().await;
        });

        let mut pending_dials = HashMap::new();
        pending_dials.insert(2, pending_addr);
        let mut known_addrs = HashSet::from([live_addr, pending_addr]);
        let mut priority_backlog = VecDeque::new();
        let mut peer_backlog = VecDeque::new();
        let mut dial_retries = VecDeque::new();
        let mut useful_redials = VecDeque::new();
        let mut useful_peers = HashMap::new();
        let mut pex_source = HashMap::new();
        let mut holepunch_attempted = HashSet::new();
        let (lengths, _) = two_file_layout();
        let mut piece_tracker = PieceTracker::new(lengths);
        let mut chunk_tracker = ChunkTracker::new(lengths);
        let mut piece_assemblies = HashMap::new();
        let registry_scope = Arc::new(());
        peer_registry::put(
            42,
            2,
            &registry_scope,
            pending_tx,
            pending_addr,
            Some(pending_io.abort_handle()),
        );

        let mut outbound_tasks: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        let mut outbound_aborts = HashMap::new();
        outbound_aborts.insert(
            1,
            outbound_tasks.spawn(async move {
                extra.closed().await;
            }),
        );
        outbound_aborts.insert(2, outbound_tasks.spawn(std::future::pending::<()>()));

        let (disconnected, removed) = apply_blocklist_to_torrent(
            &blocklist,
            &mut peers,
            &mut pending_dials,
            &mut outbound_aborts,
            &mut known_addrs,
            &mut priority_backlog,
            &mut peer_backlog,
            &mut dial_retries,
            &mut useful_redials,
            &mut useful_peers,
            &mut pex_source,
            &mut holepunch_attempted,
            &mut piece_tracker,
            &mut chunk_tracker,
            &mut piece_assemblies,
            &lengths,
            42,
            &registry_scope,
        );

        assert_eq!(disconnected, 1);
        assert_eq!(removed, 1);
        assert!(peers.is_empty());
        assert!(pending_dials.is_empty());
        assert!(!known_addrs.contains(&live_addr));
        assert!(!known_addrs.contains(&pending_addr));
        assert!(outbound_aborts.is_empty());
        assert!(peer_registry::take(42, 2, &registry_scope).is_none());

        for _ in 0..2 {
            let joined = tokio::time::timeout(Duration::from_secs(1), outbound_tasks.join_next())
                .await
                .expect("blocklist abort should finish the outbound task");
            let result = joined.expect("joinset should still have a task");
            assert!(result.is_ok() || result.unwrap_err().is_cancelled());
        }

        let pending_io = tokio::time::timeout(Duration::from_secs(1), pending_io)
            .await
            .expect("blocklist abort should finish the pending outbound I/O task");
        assert!(pending_io.is_ok() || pending_io.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn apply_blocklist_aborts_inbound_peer_when_command_channel_is_full() {
        let mut list = BlockList::default();
        list.replace(&["127.0.0.1".into()]);
        let blocklist = RwLock::new(list);

        let live_addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(PeerCommand::Send(Message::KeepAlive)).unwrap();
        let extra = tx.clone();

        let mut peer = Peer::connected(live_addr, tx, 1, 1, 1, false, false, None);
        let io_task = tokio::spawn(async move {
            extra.closed().await;
        });
        peer.io_abort = Some(io_task.abort_handle());

        let mut peers = HashMap::new();
        peers.insert(1, peer);

        let mut pending_dials = HashMap::new();
        let mut known_addrs = HashSet::from([live_addr]);
        let mut priority_backlog = VecDeque::new();
        let mut peer_backlog = VecDeque::new();
        let mut dial_retries = VecDeque::new();
        let mut useful_redials = VecDeque::new();
        let mut useful_peers = HashMap::new();
        let mut pex_source = HashMap::new();
        let mut holepunch_attempted = HashSet::new();
        let (lengths, _) = two_file_layout();
        let mut piece_tracker = PieceTracker::new(lengths);
        let mut chunk_tracker = ChunkTracker::new(lengths);
        let mut piece_assemblies = HashMap::new();
        let registry_scope = Arc::new(());
        let mut outbound_aborts = HashMap::new();

        let (disconnected, removed) = apply_blocklist_to_torrent(
            &blocklist,
            &mut peers,
            &mut pending_dials,
            &mut outbound_aborts,
            &mut known_addrs,
            &mut priority_backlog,
            &mut peer_backlog,
            &mut dial_retries,
            &mut useful_redials,
            &mut useful_peers,
            &mut pex_source,
            &mut holepunch_attempted,
            &mut piece_tracker,
            &mut chunk_tracker,
            &mut piece_assemblies,
            &lengths,
            42,
            &registry_scope,
        );

        assert_eq!(disconnected, 1);
        assert_eq!(removed, 0);
        assert!(peers.is_empty());
        assert!(!known_addrs.contains(&live_addr));
        assert!(outbound_aborts.is_empty());

        let joined = tokio::time::timeout(Duration::from_secs(1), io_task)
            .await
            .expect("blocklist abort should finish the inbound I/O task");
        assert!(joined.is_ok() || joined.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn handshake_rejection_aborts_outbound_task_when_command_channel_is_full() {
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(PeerCommand::Send(Message::KeepAlive)).unwrap();
        let io_extra = tx.clone();
        let outbound_extra = tx.clone();

        let io_task = tokio::spawn(async move {
            io_extra.closed().await;
        });
        let mut outbound_tasks: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
        let mut outbound_aborts = HashMap::new();
        outbound_aborts.insert(
            1,
            outbound_tasks.spawn(async move {
                outbound_extra.closed().await;
            }),
        );
        let registry_scope = Arc::new(());

        reject_handshook_peer(
            1,
            &tx,
            Some(io_task.abort_handle()),
            &mut outbound_aborts,
            42,
            &registry_scope,
        );

        assert!(outbound_aborts.is_empty());

        let joined = tokio::time::timeout(Duration::from_secs(1), io_task)
            .await
            .expect("handshake rejection should finish the peer I/O task");
        assert!(joined.is_ok() || joined.unwrap_err().is_cancelled());

        let outbound = tokio::time::timeout(Duration::from_secs(1), outbound_tasks.join_next())
            .await
            .expect("handshake rejection should finish the outbound task")
            .expect("joinset should still have a task");
        assert!(outbound.is_ok() || outbound.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn pause_aborts_inbound_peer_when_command_channel_is_full() {
        let live_addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(PeerCommand::Send(Message::KeepAlive)).unwrap();
        let extra = tx.clone();

        let mut peer = Peer::connected(live_addr, tx, 1, 1, 1, false, false, None);
        let io_task = tokio::spawn(async move {
            extra.closed().await;
        });
        peer.io_abort = Some(io_task.abort_handle());

        let mut peers = HashMap::new();
        peers.insert(1, peer);
        let mut useful_peers = HashMap::new();
        let (lengths, _) = two_file_layout();
        let mut piece_tracker = PieceTracker::new(lengths);
        let mut chunk_tracker = ChunkTracker::new(lengths);
        let mut piece_assemblies = HashMap::new();

        let paused_candidates = pause_teardown_live_peers(
            &mut peers,
            &mut useful_peers,
            &mut piece_tracker,
            &mut chunk_tracker,
            &mut piece_assemblies,
            &lengths,
            1,
            1,
        );

        assert!(peers.is_empty());
        assert_eq!(paused_candidates, vec![live_addr]);

        let joined = tokio::time::timeout(Duration::from_secs(1), io_task)
            .await
            .expect("pause should finish the inbound I/O task");
        assert!(joined.is_ok() || joined.unwrap_err().is_cancelled());
    }

    #[test]
    fn build_hash_response_serves_full_piece_layer() {
        let (tables, root) = make_v2_tables(4, 64 * 1024);
        let base = tables[0].piece_layer_base();
        let length = tables[0].piece_layer_padded_len();
        let expected = tables[0].serve_full_piece_layer().unwrap();

        match build_hash_response(Some(&tables), root.0, base, 0, length, 0) {
            Message::Hashes {
                pieces_root,
                base_layer,
                index,
                length: l,
                proof_layers,
                hashes,
            } => {
                assert_eq!(pieces_root, root.0);
                assert_eq!(base_layer, base);
                assert_eq!(index, 0);
                assert_eq!(l, length);
                assert_eq!(proof_layers, 0);
                assert_eq!(hashes.as_ref(), expected.as_slice());
            }
            other => panic!("expected Hashes, got {other:?}"),
        }
    }

    #[test]
    fn initial_bitfield_is_suppressed_when_empty() {
        assert!(!should_send_initial_bitfield(&[]));
        assert!(!should_send_initial_bitfield(&[0]));
        assert!(!should_send_initial_bitfield(&[0, 0, 0]));
    }

    #[test]
    fn initial_bitfield_is_sent_when_any_piece_is_local() {
        assert!(should_send_initial_bitfield(&[0x80]));
        assert!(should_send_initial_bitfield(&[0, 0x01]));
    }

    #[test]
    fn fast_initial_availability_covers_empty_full_and_partial_swarms() {
        assert!(matches!(
            initial_availability_message(&[0], 4, true),
            Some(Message::HaveNone)
        ));
        assert!(matches!(
            initial_availability_message(&[0b1111_0000], 4, true),
            Some(Message::HaveAll)
        ));
        assert!(matches!(
            initial_availability_message(&[0b1010_0000], 4, true),
            Some(Message::Bitfield(_))
        ));
        assert!(initial_availability_message(&[0], 4, false).is_none());
    }

    #[test]
    fn fast_peer_rejects_duplicate_initial_availability() {
        let (cmd_tx, _cmd_rx) = mpsc::channel(1);
        let mut peer = Peer::connected(test_peer(52_001), cmd_tx, 1, 1, 1, true, true, None);
        assert!(accept_initial_availability(
            &mut peer,
            InitialAvailability::Bitfield
        ));
        assert!(!accept_initial_availability(
            &mut peer,
            InitialAvailability::HaveAll
        ));
    }

    #[test]
    fn build_hash_response_rejects_unknown_root() {
        let (tables, _root) = make_v2_tables(4, 64 * 1024);
        let bogus = [0xAAu8; 32];
        match build_hash_response(Some(&tables), bogus, 2, 0, 4, 0) {
            Message::HashReject { pieces_root, .. } => assert_eq!(pieces_root, bogus),
            _ => panic!("expected HashReject"),
        }
    }

    #[test]
    fn build_hash_response_hybrid_torrent_served_via_tables() {
        // Hybrid torrents use V1Sha1 for piece verification but must still serve BEP-52 hash requests; simulate this by passing Some(tables) with a V1Sha1 verifier — the function no longer looks at the verifier at all
        let (tables, root) = make_v2_tables(4, 64 * 1024);
        let base = tables[0].piece_layer_base();
        let length = tables[0].piece_layer_padded_len();
        let expected = tables[0].serve_full_piece_layer().unwrap();

        match build_hash_response(Some(&tables), root.0, base, 0, length, 0) {
            Message::Hashes { hashes, .. } => {
                assert_eq!(
                    hashes.as_ref(),
                    expected.as_slice(),
                    "hybrid torrent should serve piece layer via explicit tables"
                );
            }
            other => panic!("expected Hashes for hybrid torrent, got {other:?}"),
        }
    }

    #[test]
    fn build_hash_response_rejects_when_no_tables() {
        // Pure-v1 torrent: tables = None → always HashReject
        match build_hash_response(None, [0u8; 32], 0, 0, 1, 0) {
            Message::HashReject { .. } => {}
            _ => panic!("expected HashReject for None tables"),
        }
    }

    #[test]
    fn build_hash_response_rejects_partial_request() {
        let (tables, root) = make_v2_tables(4, 64 * 1024);
        // proof_layers > 0 → reject
        match build_hash_response(Some(&tables), root.0, 2, 0, 4, 1) {
            Message::HashReject { .. } => {}
            _ => panic!("expected HashReject for proof_layers != 0"),
        }
        // wrong base_layer → reject
        match build_hash_response(Some(&tables), root.0, 99, 0, 4, 0) {
            Message::HashReject { .. } => {}
            _ => panic!("expected HashReject for wrong base_layer"),
        }
    }

    #[test]
    fn build_hash_response_rejects_v1_verifier() {
        // Passing None tables → HashReject regardless of other params
        match build_hash_response(None, [0u8; 32], 0, 0, 1, 0) {
            Message::HashReject { .. } => {}
            _ => panic!("expected HashReject for None (v1-only) tables"),
        }
    }

    #[test]
    fn choke_slots_respect_limit_and_optimistic() {
        let candidates: Vec<(u32, u64)> =
            vec![(1, 100), (2, 500), (3, 300), (4, 50), (5, 400), (6, 200)];
        let set = select_unchoked(candidates.clone(), None, 4);
        assert_eq!(set.len(), 4);
        assert!(set.contains(&2) && set.contains(&5) && set.contains(&3) && set.contains(&6));

        let set = select_unchoked(candidates, Some(4), 4);
        assert_eq!(set.len(), 4);
        assert!(set.contains(&4), "optimistic peer must hold a slot");
        assert!(set.contains(&2) && set.contains(&5) && set.contains(&3));
        assert!(!set.contains(&6), "slowest regular peer loses its slot");
    }

    #[test]
    fn optimistic_rotation_is_round_robin() {
        let c = [1u32, 3, 7];
        assert_eq!(next_optimistic(&c, None), Some(1));
        assert_eq!(next_optimistic(&c, Some(1)), Some(3));
        assert_eq!(next_optimistic(&c, Some(3)), Some(7));
        assert_eq!(next_optimistic(&c, Some(7)), Some(1));
        assert_eq!(next_optimistic(&[], Some(7)), None);
    }

    #[test]
    fn normalize_tracker_urls_dedupes_against_existing_and_self() {
        let existing = vec!["udp://a.example:80/announce".to_string()];
        let added = normalize_tracker_urls(
            vec![
                "udp://a.example:80/announce".to_string(),
                "http://b.example/announce\nhttp://b.example/announce".to_string(),
                "http://c.example/announce,udp://d.example:6969/announce".to_string(),
            ],
            &existing,
        );
        assert_eq!(
            added,
            vec![
                "http://b.example/announce".to_string(),
                "http://c.example/announce".to_string(),
                "udp://d.example:6969/announce".to_string(),
            ]
        );
    }

    #[test]
    fn allowed_fast_set_is_stable_bounded_and_in_range() {
        let hash = Id20::new([7; 20]);
        let addr = "192.0.2.1:6881".parse().unwrap();
        let first = allowed_fast_set(&hash, addr, 17);
        assert_eq!(first, allowed_fast_set(&hash, addr, 17));
        assert!(first.len() <= MAX_FAST_PIECES);
        assert!(first.iter().all(|index| *index < 17));
        assert!(first.iter().collect::<HashSet<_>>().len() == first.len());
    }

    #[test]
    fn lifecycle_generation_invalidates_stale_worker_results() {
        let token = GenerationToken::new();
        let first = token.current();
        assert!(token.is_current(first));
        let second = token.bump();
        assert!(!token.is_current(first));
        assert!(token.is_current(second));
    }

    #[test]
    fn tracker_lifecycle_preserves_events_and_promoted_url_across_restart() {
        let hash = Id20::new([9; 20]);
        let tiers = vec![vec![
            "https://one.example/announce".to_string(),
            "https://two.example/announce".to_string(),
        ]];
        let mut lifecycle = TrackerLifecycle::default();
        {
            let state = lifecycle.run_state(hash, &tiers);
            state.active_url = 1;
            state.tiers[0][1].started_sent = true;
            let promoted = state.tiers[0].remove(1);
            state.tiers[0].insert(0, promoted);
            state.active_url = 0;
        }
        let restored = lifecycle.run_state(hash, &tiers);
        assert_eq!(restored.tiers[0][0].url, "https://two.example/announce");
        assert!(restored.tiers[0][0].started_sent);
        lifecycle.append_tier(&["https://new.example/announce".to_string()]);
        assert_eq!(lifecycle.run_state(hash, &tiers).tiers.len(), 2);
    }

    #[tokio::test]
    async fn add_trackers_after_shutdown_uses_a_fresh_poller_set() {
        let (tx, _rx) = mpsc::channel(8);
        let stats = Arc::new(Mutex::new(TorrentStats::initial(0, vec![])));
        let peer_id = Id20::new([0; 20]);
        let hashes = vec![Id20::new([1; 20])];
        let mut pollers = spawn_tracker_pollers(
            tx.clone(),
            Vec::new(),
            hashes.clone(),
            peer_id,
            6881,
            Arc::clone(&stats),
            None,
        );
        // Keep a subscriber so shutdown can latch; an empty set drops the
        // original receiver before send() otherwise.
        let _keep_alive = pollers.shutdown_tx.subscribe();
        pollers.shutdown(false).await;
        assert!(pollers.is_empty());
        assert!(pollers.shutdown_tx.borrow().is_some());

        pollers = spawn_tracker_pollers(tx, Vec::new(), hashes, peer_id, 6881, stats, None);
        assert!(pollers.is_empty());
        assert!(
            pollers.shutdown_tx.borrow().is_none(),
            "recreated pollers must not inherit the previous shutdown latch"
        );
    }

    #[test]
    fn webseed_scheduler_groups_bounded_contiguous_piece_jobs() {
        let jobs = contiguous_webseed_batches(&[0, 1, 2, 3, 5, 6, 9], 3);
        assert_eq!(jobs, vec![vec![0, 1, 2], vec![3], vec![5, 6], vec![9]]);
    }

    #[test]
    fn webseed_batch_maps_one_contiguous_range_across_files() {
        let info = ValidatedTorrentMetaV1Info {
            name: "bundle".into(),
            piece_length: 4,
            pieces: vec![0; 60],
            private: false,
            files: vec![
                TorrentMetaInfo {
                    path: vec!["first.bin".into()],
                    length: 5,
                },
                TorrentMetaInfo {
                    path: vec!["second.bin".into()],
                    length: 7,
                },
            ],
            single_file_mode: false,
        };
        let layout = FileSet::from_meta(&info, Path::new("/tmp/risuko-webseed-test"));
        assert_eq!(
            webseed_piece_spans(&layout, 0, 12),
            vec![(0, 0, 5), (1, 0, 7)]
        );
    }

    #[tokio::test]
    async fn webseed_batch_fetches_adjacent_pieces_in_one_http_range() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let payload = b"abcdefghijkl";
        let mut piece_hashes = Vec::new();
        for piece in payload.chunks(4) {
            piece_hashes.extend_from_slice(crate::core::hash::sha1(piece).as_bytes());
        }
        let info = Arc::new(ValidatedTorrentMetaV1Info {
            name: "payload.bin".into(),
            piece_length: 4,
            pieces: piece_hashes.clone(),
            private: false,
            files: vec![TorrentMetaInfo {
                path: vec!["payload.bin".into()],
                length: payload.len() as u64,
            }],
            single_file_mode: true,
        });
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(FilesystemStorage::new(&info, tmp.path()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = socket.read(&mut buf).await.unwrap();
                assert_ne!(read, 0);
                request.extend_from_slice(&buf[..read]);
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET /payload.bin HTTP/1.1\r\n"));
            assert!(request.contains("range: bytes=0-11\r\n"), "{request:?}");
            socket
                .write_all(
                    b"HTTP/1.1 206 Partial Content\r\nContent-Length: 12\r\nContent-Range: bytes 0-11/12\r\nETag: \"batch-v1\"\r\nConnection: close\r\n\r\nabcdefghijkl",
                )
                .await
                .unwrap();
        });

        let lengths = Lengths::new(payload.len() as u64, 4).unwrap();
        let ctx = WebSeedContext {
            client: build_webseed_client(None).unwrap(),
            bases: Arc::new(vec![format!("http://{addr}/")]),
            disabled: Arc::new(Mutex::new(HashSet::new())),
            etags: Arc::new(Mutex::new(HashMap::new())),
            layout: storage.layout().clone(),
            info,
            storage: Arc::clone(&storage),
            verifier: PieceVerifier::V1Sha1 {
                pieces: Arc::new(piece_hashes),
            },
            lengths,
            generation: GenerationToken::new(),
        };

        let result = fetch_webseed_batch(ctx, vec![0, 1, 2]).await;
        assert_eq!(result.pieces.len(), 3);
        assert!(result
            .pieces
            .iter()
            .all(|piece| piece.verify_ok && !piece.write_failed));
        server.await.unwrap();
        let mut stored = [0u8; 12];
        storage.read_at(0, &mut stored).await.unwrap();
        assert_eq!(&stored, payload);
    }

    #[tokio::test]
    async fn scan_existing_pieces_recovers_complete_piece_when_sibling_file_is_missing() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let a_bytes: Vec<u8> = (0u8..15).collect();
        let piece0 = crate::core::hash::sha1(&a_bytes[..10]);
        let mut pieces = Vec::with_capacity(60);
        pieces.extend_from_slice(piece0.as_bytes());
        pieces.extend_from_slice(&[0u8; 40]);

        let info = ValidatedTorrentMetaV1Info {
            name: "root".into(),
            piece_length: 10,
            pieces,
            private: false,
            files: vec![
                TorrentMetaInfo {
                    path: vec!["a".into()],
                    length: 15,
                },
                TorrentMetaInfo {
                    path: vec!["b".into()],
                    length: 15,
                },
            ],
            single_file_mode: false,
        };
        let storage = Arc::new(FilesystemStorage::new(&info, root));
        tokio::fs::write(root.join("a"), &a_bytes).await.unwrap();
        assert!(
            storage.has_existing_payload_files().await,
            "a complete piece in one file must still schedule a recovery scan"
        );

        let lengths = Lengths::new(30, 10).unwrap();
        let verifier = PieceVerifier::V1Sha1 {
            pieces: Arc::new(info.pieces.clone()),
        };
        let mut piece_tracker = PieceTracker::new(lengths);
        scan_existing_pieces(&verifier, &storage, &lengths, &mut piece_tracker).await;

        let v0 = lengths.validate_piece(0).unwrap();
        let v1 = lengths.validate_piece(1).unwrap();
        assert!(
            piece_tracker.has_local(v0),
            "piece fully contained in the present file must be recovered"
        );
        assert!(
            !piece_tracker.has_local(v1),
            "piece that spans the missing file must stay outstanding"
        );
    }

    #[test]
    fn private_peer_policy_allows_tracker_only() {
        let tracker = test_peer(15_001);
        let dht = test_peer(15_002);
        let auth = HashSet::new();
        assert!(peer_source_allowed(
            true,
            PeerSource::Tracker,
            tracker,
            &auth
        ));
        assert!(!peer_source_allowed(true, PeerSource::Dht, dht, &auth));
        assert!(!peer_source_allowed(true, PeerSource::Manual, dht, &auth));
    }

    #[test]
    fn private_peer_policy_allows_reconnect_to_authorized_endpoint() {
        let addr = test_peer(15_003);
        let auth = HashSet::from([addr]);
        assert!(peer_source_allowed(true, PeerSource::Pex, addr, &auth));
        assert!(peer_source_allowed(true, PeerSource::Manual, addr, &auth));
    }

    #[test]
    fn public_peer_policy_accepts_all_sources() {
        let addr = test_peer(15_004);
        let auth = HashSet::new();
        for source in [
            PeerSource::Manual,
            PeerSource::Tracker,
            PeerSource::Dht,
            PeerSource::Lsd,
            PeerSource::Pex,
            PeerSource::Holepunch,
            PeerSource::WebSeed,
        ] {
            assert!(peer_source_allowed(false, source, addr, &auth));
        }
    }
}
