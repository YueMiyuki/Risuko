pub mod stats;

use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use bytes::{Bytes, BytesMut};
use parking_lot::{Mutex, RwLock};
use rand::RngExt;
use sha1::{Digest, Sha1};

use super::blocklist::BlockList;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};
use tokio::task::AbortHandle;
use tokio::time::{interval, MissedTickBehavior};

use super::core::{build_v2_tables, Id20, Lengths, MerkleProofTable, PieceVerifier, TorrentMeta};
use super::limiter::{tightest, Throttle, TorrentLimits};
use super::peer::{
    connect_prefer_utp, connect_with_utp_fallback, PeerCommand, PeerEvent, PeerEventSink, SpawnPeer,
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
const DEFAULT_ADAPTIVE_MAX_OUTSTANDING_PER_PEER: usize = 500;
const ABSOLUTE_MAX_OUTSTANDING_PER_PEER: usize = 500;
const TORRENT_REQUEST_BUDGET: usize = 2048;
const DEFAULT_MAX_PEERS: usize = 100;
const MAX_PENDING_DIALS: usize = 48;
const PRIORITY_DIAL_RESERVE: usize = 12;
const MAX_PEER_BACKLOG: usize = 1024;
const DIAL_RETRY_DELAY: Duration = Duration::from_secs(20);
const MAX_DIAL_FAILURES: u8 = 3;
const USEFUL_PEER_REDIAL_DELAY: Duration = Duration::from_secs(8);
const MAX_DIAL_RETRIES: usize = 512;
const MAX_USEFUL_PEERS: usize = 1024;
const UNPRODUCTIVE_PEER_LIFETIME: Duration = Duration::from_secs(30);
const OUTBOUND_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const USEFUL_PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(12);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(8);
const TRACKER_ANNOUNCE_TIMEOUT: Duration = Duration::from_secs(30);
const TRACKER_STOPPED_TIMEOUT: Duration = Duration::from_secs(5);
const TRACKER_SHUTDOWN_GRACE: Duration = Duration::from_secs(8);
const TRACKER_MAX_CONCURRENT_ANNOUNCES: usize = 8;
const TRACKER_MIN_INTERVAL: Duration = Duration::from_secs(60);
const TRACKER_MAX_INTERVAL: Duration = Duration::from_secs(30 * 60);

const PEER_IDLE_TIMEOUT: Duration = Duration::from_secs(180);

const SNUB_EVICTION_TIMEOUT: Duration = Duration::from_secs(60);
const PEER_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(5);

const PEER_QUALITY_INTERVAL: Duration = Duration::from_secs(15);
const PEER_USELESS_AFTER: Duration = Duration::from_secs(120);
const MAX_QUALITY_EVICTIONS: usize = 2;

const MIN_UPLOAD_SLOTS: usize = 4;
const MAX_UPLOAD_SLOTS: usize = 12;
const SLOT_RATE_STEP_BPS: u64 = 1024;
const CHOKE_EVAL_INTERVAL: Duration = Duration::from_secs(10);
const OPTIMISTIC_ROTATE_INTERVAL: Duration = Duration::from_secs(30);
const PIPELINE_TARGET_SECS: f32 = 4.0;
const PIPELINE_RATE_WINDOW: Duration = Duration::from_secs(1);
const PIPELINE_SLOW_START_MIN_GAIN: f32 = 1.25;
const PIPELINE_ADDITIVE_GROWTH: usize = 8;

const DHT_POLL_INTERVAL: Duration = Duration::from_secs(120);
const DHT_RELAXED_INTERVAL: Duration = Duration::from_secs(15 * 60);
const PEX_INTERVAL: Duration = Duration::from_secs(60);
const PEX_MIN_INBOUND_GAP: Duration = Duration::from_secs(20);
const MAX_PEX_ADDED_PER_MSG: usize = 50;

const OUR_UT_METADATA_ID: u8 = 3;
const OUR_UT_PEX_ID: u8 = 4;
const OUR_UT_HOLEPUNCH_ID: u8 = 5;
const MAX_PEX_SOURCE_ENTRIES: usize = 4096;
const MAX_HOLEPUNCH_ENTRIES: usize = 4096;
const META_PIECE_SIZE: usize = 16 * 1024;
const MAX_FAST_PIECES: usize = 10;
const MAX_LEAF_HASH_JOBS: usize = 4;
const MAX_SUGGESTED_PIECES: usize = 64;
const WEBSEED_MAX_WORKERS: usize = 4;
const WEBSEED_MAX_PIECES_PER_JOB: usize = 8;
const WEBSEED_MAX_RANGE_BYTES: u64 = super::webseed::DEFAULT_MAX_RANGE_BYTES;
const WEBSEED_TIMEOUT: Duration = Duration::from_secs(30);
const WEBSEED_RETRY_BASE: Duration = Duration::from_secs(5);
const WEBSEED_RETRY_MAX: Duration = Duration::from_secs(600);
const MAX_QUEUED_DISK_BYTES: u64 = 64 * 1024 * 1024;

pub struct TorrentInit {
    pub meta: TorrentMeta,
    pub lengths: Lengths,
    pub root_dir: PathBuf,
    pub only_files: Option<Vec<usize>>,
    pub max_outstanding_per_peer: Option<usize>,
    pub max_peers: Option<usize>,
    pub hash_fail_ban_strikes: Option<u8>,
    pub encryption: super::peer::EncryptionPolicy,
    pub advertise_v2: bool,
    pub verifier: PieceVerifier,
    pub create_subfolder: bool,
    pub utp: Option<Arc<UtpSocket>>,
    pub limits: TorrentLimits,
    pub dht: Option<Arc<super::dht::Dht>>,
    pub p2p_proxy: Option<risuko_http::ProxyConnector>,
    pub p2p_proxy_is_task_override: bool,
    pub tracker_source_addr: Option<SocketAddr>,
    pub blocklist: Arc<RwLock<BlockList>>,
    pub conn_budget: Arc<super::conn_budget::ConnBudget>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PeerSource {
    Manual,
    Tracker,
    Dht,
    Lsd,
    Pex,
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
    mse_first: bool,
}

const MAX_MSE_FIRST_ADDRS: usize = 4096;

fn dial_policy(
    policy: crate::peer::EncryptionPolicy,
    mse_capable: bool,
) -> crate::peer::EncryptionPolicy {
    use crate::peer::EncryptionPolicy;
    if mse_capable && policy == EncryptionPolicy::Prefer {
        EncryptionPolicy::PreferEncrypted
    } else {
        policy
    }
}

fn peer_source_allowed(
    private_torrent: bool,
    source: PeerSource,
    addr: SocketAddr,
    tracker_authorized: &HashSet<SocketAddr>,
) -> bool {
    !private_torrent || source == PeerSource::Tracker || tracker_authorized.contains(&addr)
}

fn inbound_peer_allowed(
    private_torrent: bool,
    addr: SocketAddr,
    tracker_authorized: &HashSet<SocketAddr>,
) -> bool {
    !private_torrent
        || tracker_authorized.contains(&addr)
        || tracker_authorized.iter().any(|a| a.ip() == addr.ip())
}

fn request_timeout_for(limit_bps: u64, queued_chunks: usize) -> Duration {
    if limit_bps == 0 {
        return REQUEST_TIMEOUT;
    }
    let drain = queued_chunks as f64 * f64::from(crate::core::CHUNK_SIZE) / limit_bps as f64;
    REQUEST_TIMEOUT + Duration::from_secs_f64(drain.min(120.0))
}

#[derive(Debug)]
pub enum TorrentCommand {
    AddPeer(PeerCandidate),
    AddInboundPeer {
        addr: SocketAddr,
        cmd_tx: mpsc::Sender<PeerCommand>,
        piece_tx: mpsc::Sender<Message>,
        bind: oneshot::Sender<PeerEventSink>,
        reserved: [u8; 8],
        peer_id: Id20,
        io_abort: AbortHandle,
        encrypted: bool,
        utp: bool,
    },
    AddTrackers {
        urls: Vec<String>,
        ack: oneshot::Sender<usize>,
    },
    SetOnlyFiles {
        files: Option<Vec<usize>>,
        ack: oneshot::Sender<Result<(), String>>,
    },
    Pause(oneshot::Sender<()>),
    PauseIfHalted(oneshot::Sender<()>),
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
    pub(crate) limits: TorrentLimits,
}

impl ManagedTorrent {
    pub fn info_hash(&self) -> Id20 {
        self.info_hash
    }
    pub fn set_download_limit(&self, bps: u64) {
        self.limits.down.task().set_limit(bps);
    }
    pub fn set_upload_limit(&self, bps: u64) {
        self.limits.up.task().set_limit(bps);
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
        self.stats.lock().clone()
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

    pub async fn set_only_files(&self, files: Option<Vec<usize>>) -> Result<(), String> {
        let (ack, done) = oneshot::channel();
        self.cmd_tx
            .send(TorrentCommand::SetOnlyFiles { files, ack })
            .await
            .map_err(|e| e.to_string())?;
        done.await.map_err(|e| e.to_string())?
    }
}

fn wanted_pieces(layout: &FileSet, lengths: &Lengths, only_files: Option<&[usize]>) -> Vec<bool> {
    let total = lengths.total_pieces() as usize;
    let Some(only_files) = only_files else {
        return vec![true; total];
    };
    let mut wanted = vec![false; total];
    let piece_length = lengths.piece_length() as u64;
    for &idx in only_files {
        let Some(file) = layout.files().get(idx) else {
            continue;
        };
        if file.padding || file.length == 0 {
            continue;
        }
        let first = (file.offset / piece_length) as usize;
        let last = ((file.offset + file.length - 1) / piece_length) as usize;
        for flag in wanted.iter_mut().take(last.min(total - 1) + 1).skip(first) {
            *flag = true;
        }
    }
    wanted
}

fn selected_file_set(only_files: Option<&[usize]>) -> Option<HashSet<usize>> {
    only_files.map(|files| files.iter().copied().collect())
}

async fn apply_storage_selection(
    storage: &FilesystemStorage,
    only_files: Option<Vec<usize>>,
) -> (Option<Vec<usize>>, Result<(), String>) {
    let selected = selected_file_set(only_files.as_deref());
    let mut failed = HashSet::new();
    let mut errors = Vec::new();
    for idx in storage.set_selection(selected.as_ref()).await {
        if let Err(e) = storage.promote_file(idx).await {
            let path = &storage.layout().files()[idx].path;
            errors.push(format!("{} ({e})", path.display()));
            failed.insert(idx);
        }
    }
    if failed.is_empty() {
        return (only_files, Ok(()));
    }
    let in_effect = match only_files {
        Some(files) => files
            .into_iter()
            .filter(|idx| !failed.contains(idx))
            .collect(),
        None => (0..storage.layout().files().len())
            .filter(|idx| !failed.contains(idx))
            .collect(),
    };
    (
        Some(in_effect),
        Err(format!(
            "part-file data could not move, left unselected: {}",
            errors.join("; ")
        )),
    )
}

pub async fn spawn(
    id: usize,
    init: TorrentInit,
    our_peer_id: Id20,
    listen_port: u16,
) -> std::io::Result<Arc<ManagedTorrent>> {
    let info_hash = init.meta.info_hash;
    let name = Some(init.meta.info.name.clone());
    let (cmd_tx, cmd_rx) = mpsc::channel::<TorrentCommand>(64);
    let file_lens: Vec<u64> = init.meta.info.iter_file_details().map(|f| f.len).collect();
    let wanted = wanted_pieces(
        &FileSet::from_meta(&init.meta.info, &init.root_dir),
        &init.lengths,
        init.only_files.as_deref(),
    );
    let left_bytes = PieceTracker::bytes_of(&init.lengths, |i| wanted[i]);
    let stats = Arc::new(Mutex::new(TorrentStats::initial(
        init.lengths.total_length(),
        left_bytes,
        file_lens,
    )));
    let meta_arc = Arc::new(init.meta.clone());
    let metadata_swap = ArcSwapOption::new(Some(meta_arc));
    let upload_only_flag = Arc::new(AtomicBool::new(false));
    let ext_handshake_builder: crate::peer::ExtHandshakeBuilder = {
        let ext = OurExtHandshake {
            listen_port,
            metadata_size: init.meta.info_bytes.len() as u64,
            private: init.meta.info.private,
            prefers_encryption: init.encryption != super::peer::EncryptionPolicy::PlaintextOnly,
        };
        let upload_only_flag = Arc::clone(&upload_only_flag);
        std::sync::Arc::new(move |peer_ip: std::net::IpAddr| {
            let hs = ext.build(peer_ip, upload_only_flag.load(Ordering::Relaxed));
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
        limits: init.limits.clone(),
    });
    tokio::spawn(torrent_loop(
        id,
        init,
        our_peer_id,
        listen_port,
        cmd_rx,
        stats,
        advertise_v2_flag,
        upload_only_flag,
        handle.ext_handshake_builder.clone(),
    ));
    Ok(handle)
}

struct OurExtHandshake {
    listen_port: u16,
    metadata_size: u64,
    private: bool,
    prefers_encryption: bool,
}

impl OurExtHandshake {
    fn build(&self, peer_ip: IpAddr, upload_only: bool) -> ExtHandshake {
        let mut hs =
            ExtHandshake::new_outgoing(OUR_UT_METADATA_ID, OUR_UT_PEX_ID, Some(self.metadata_size))
                .with_yourip(peer_ip)
                .with_port(self.listen_port)
                .with_reqq(UPLOAD_REQQ)
                .with_encryption_preference(self.prefers_encryption);
        if self.private {
            hs.supported.remove(b"ut_pex".as_slice());
        } else {
            hs = hs.with_holepunch(OUR_UT_HOLEPUNCH_ID);
        }
        hs.upload_only = upload_only;
        hs
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct PeerLink {
    encrypted: bool,
    utp: bool,
}

mod pex_flag {
    pub const ENCRYPTION: u8 = 0x01;
    pub const SEED: u8 = 0x02;
    pub const UTP: u8 = 0x04;
    pub const HOLEPUNCH: u8 = 0x08;
    pub const REACHABLE: u8 = 0x10;
}

struct Peer {
    addr: SocketAddr,
    cmd_tx: mpsc::Sender<PeerCommand>,
    piece_tx: Option<mpsc::Sender<Message>>,
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
    slow_start_rate: f32,
    connected_at: Instant,
    optimistic_tried: bool,

    snubbing: bool,
    last_recv: Instant,
    snub_since: Option<Instant>,
    consecutive_rejects: u32,
    reqq_cap: Option<usize>,
    their_ut_metadata_id: Option<u8>,
    their_ut_holepunch_id: Option<u8>,
    downloaded_window: u64,
    uploaded_window: Arc<AtomicU64>,
    down_ewma: u64,
    up_ewma: u64,
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
    listen_addr: Option<SocketAddr>,
    link: PeerLink,
    prefers_encryption: bool,
    upload_only: bool,
    ext_seen: bool,
    pending_haves: Vec<u32>,
    last_pex_in: Option<Instant>,
    outbound: bool,
    supports_fast: bool,
    initial_availability: Option<InitialAvailability>,
    suggested_pieces: HashSet<u32>,
    remote_allowed_fast: HashSet<u32>,
    choke_rejected: HashSet<(u32, u32)>,
    sent_allowed_fast: HashSet<u32>,
    pending_uploads: Arc<Mutex<PendingUploads>>,
    canceled_requests: CanceledRequests,
    last_piece: Option<u32>,
    io_abort: Option<AbortHandle>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InitialAvailability {
    Bitfield,
    HaveAll,
    HaveNone,
}

impl Peer {
    fn pex_flags(&self, total_pieces: usize) -> u8 {
        let mut flags = 0;
        if self.link.encrypted || self.prefers_encryption {
            flags |= pex_flag::ENCRYPTION;
        }
        if self.upload_only || peer_bitfield_is_full(&self.bitfield, total_pieces) {
            flags |= pex_flag::SEED;
        }
        if self.link.utp {
            flags |= pex_flag::UTP;
        }
        if self.their_ut_holepunch_id.is_some() {
            flags |= pex_flag::HOLEPUNCH;
        }
        if self.outbound {
            flags |= pex_flag::REACHABLE;
        }
        flags
    }

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
            piece_tx: None,
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
            slow_start: max_outstanding < pipeline_cap,
            slow_start_rate: 0.0,
            connected_at: Instant::now(),
            optimistic_tried: false,
            snubbing: false,
            last_recv: Instant::now(),
            snub_since: None,
            consecutive_rejects: 0,
            reqq_cap: None,
            their_ut_metadata_id: None,
            their_ut_holepunch_id: None,
            downloaded_window: 0,
            uploaded_window: Arc::new(AtomicU64::new(0)),
            down_ewma: 0,
            up_ewma: 0,
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
            listen_addr: outbound.then_some(addr),
            link: PeerLink::default(),
            prefers_encryption: false,
            upload_only: false,
            ext_seen: false,
            pending_haves: Vec::new(),
            last_pex_in: None,
            outbound,
            supports_fast,
            initial_availability: None,
            suggested_pieces: HashSet::new(),
            remote_allowed_fast: HashSet::new(),
            choke_rejected: HashSet::new(),
            sent_allowed_fast: HashSet::new(),
            pending_uploads: Arc::new(Mutex::new(PendingUploads::default())),
            canceled_requests: CanceledRequests::default(),
            last_piece: None,
            io_abort: None,
        }
    }
}

impl Peer {
    fn shrink_window(&mut self, floor: usize, cap: usize) {
        let peer_cap = self.reqq_cap.unwrap_or(cap).min(cap);
        self.max_outstanding = shrink_pipeline(self.max_outstanding, floor.min(peer_cap));
        self.delivered_since_growth = 0;
        self.slow_start = false;
    }

    fn mark_snubbed(&mut self) {
        let now = Instant::now();
        self.snubbing = true;
        self.snub_since = Some(now);
        self.delivered_window = 0;
        self.window_start = now;
    }
}

fn release_webseed_inflight(
    inflight: &mut HashSet<u32>,
    piece_tracker: &mut PieceTracker,
    lengths: &Lengths,
) {
    for idx in inflight.drain() {
        if let Ok(vpi) = lengths.validate_piece(idx) {
            piece_tracker.clear_in_flight(vpi);
        }
    }
}

const REJECT_SNUB_THRESHOLD: u32 = 8;
const MAX_CHOKE_REJECTED: usize = 512;
const MAX_PENDING_UPLOAD_BYTES: u64 = 8 * 1024 * 1024;
const UPLOAD_REQQ: u32 = (MAX_PENDING_UPLOAD_BYTES / (16 * 1024)) as u32;
const MAX_REQUEST_LEN: u32 = 128 * 1024;

#[derive(Default)]
struct PendingUploads {
    keys: HashSet<(u32, u32, u32)>,
    bytes: u64,
}

impl PendingUploads {
    fn insert(&mut self, key: (u32, u32, u32)) -> bool {
        let added = self.keys.insert(key);
        if added {
            self.bytes += u64::from(key.2);
        }
        added
    }

    fn remove(&mut self, key: &(u32, u32, u32)) -> bool {
        let removed = self.keys.remove(key);
        if removed {
            self.bytes -= u64::from(key.2);
        }
        removed
    }

    fn contains(&self, key: &(u32, u32, u32)) -> bool {
        self.keys.contains(key)
    }

    fn retain(&mut self, mut keep: impl FnMut(&(u32, u32, u32)) -> bool) {
        let bytes = &mut self.bytes;
        self.keys.retain(|key| {
            let kept = keep(key);
            if !kept {
                *bytes -= u64::from(key.2);
            }
            kept
        });
    }
}

struct VerifyResult {
    piece_index: u32,
    write_error: Option<String>,
    verify_ok: bool,
    from_webseed: bool,
    contributors: Vec<IpAddr>,
    failed_copy: Option<Arc<Vec<ChunkDigest>>>,
    culprits: Vec<IpAddr>,
}

type ChunkDigest = (Option<IpAddr>, [u8; 20]);

const MAX_FAILED_COPIES: usize = 128;

#[derive(Default)]
struct FailedCopies {
    map: HashMap<u32, Arc<Vec<ChunkDigest>>>,
    order: VecDeque<u32>,
}

impl FailedCopies {
    fn get(&self, piece: u32) -> Option<Arc<Vec<ChunkDigest>>> {
        self.map.get(&piece).cloned()
    }

    fn insert(&mut self, piece: u32, copy: Arc<Vec<ChunkDigest>>) {
        if self.map.insert(piece, copy).is_none() {
            self.order.push_back(piece);
        }
        while self.map.len() > MAX_FAILED_COPIES {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.map.remove(&oldest);
        }
    }

    fn remove(&mut self, piece: u32) {
        if self.map.remove(&piece).is_some() {
            self.order.retain(|p| *p != piece);
        }
    }
}

type SharedFailedCopies = Arc<Mutex<FailedCopies>>;

fn chunk_digests(buf: &[u8], sources: &[Option<IpAddr>]) -> Vec<ChunkDigest> {
    buf.chunks(super::core::CHUNK_SIZE as usize)
        .zip(sources.iter().copied())
        .map(|(chunk, ip)| (ip, *crate::core::hash::sha1(chunk).as_bytes()))
        .collect()
}

fn differing_sources(failed: &[ChunkDigest], good: &[ChunkDigest]) -> Vec<IpAddr> {
    let mut out: Vec<IpAddr> = failed
        .iter()
        .zip(good)
        .filter(|(f, g)| f.1 != g.1)
        .filter_map(|(f, _)| f.0)
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

struct CorruptionTracker {
    ban_after: Option<u8>,
    strikes: HashMap<IpAddr, u8>,
    copies: SharedFailedCopies,
}

impl CorruptionTracker {
    fn new(ban_after: Option<u8>) -> Self {
        Self {
            ban_after: ban_after.map(|n| n.max(1)),
            strikes: HashMap::new(),
            copies: Arc::new(Mutex::new(FailedCopies::default())),
        }
    }

    fn copies_handle(&self) -> Option<SharedFailedCopies> {
        self.ban_after.map(|_| self.copies.clone())
    }

    fn record_result(&mut self, vr: &VerifyResult) -> Vec<IpAddr> {
        if self.ban_after.is_none() {
            return Vec::new();
        }
        if vr.verify_ok {
            self.copies.lock().remove(vr.piece_index);
            return self.record_strikes(&vr.culprits);
        }
        if vr.from_webseed {
            return Vec::new();
        }
        match vr.contributors.as_slice() {
            [only] => self.record_strikes(std::slice::from_ref(only)),
            _ => {
                if let Some(copy) = &vr.failed_copy {
                    self.copies.lock().insert(vr.piece_index, copy.clone());
                }
                Vec::new()
            }
        }
    }

    fn record_strikes(&mut self, ips: &[IpAddr]) -> Vec<IpAddr> {
        let Some(ban_after) = self.ban_after else {
            return Vec::new();
        };
        let mut ban = Vec::new();
        for ip in ips {
            let strikes = self.strikes.entry(*ip).or_insert(0);
            *strikes = strikes.saturating_add(1);
            if *strikes >= ban_after {
                ban.push(*ip);
            }
        }
        for ip in &ban {
            self.strikes.remove(ip);
        }
        if self.strikes.len() > MAX_PEX_SOURCE_ENTRIES {
            self.strikes.clear();
        }
        ban
    }
}

struct DiskQueue {
    tasks: tokio::task::JoinSet<VerifyResult>,
    sizes: HashMap<tokio::task::Id, u64>,
    queued_bytes: u64,
    limit: u64,
    write_failures: u32,
    halted: Option<String>,
}

fn disk_queue_limit(piece_length: u32) -> u64 {
    MAX_QUEUED_DISK_BYTES.max(u64::from(piece_length).saturating_mul(3))
}

const MAX_WRITE_FAILURES: u32 = 3;

impl DiskQueue {
    fn new(limit: u64) -> Self {
        Self {
            tasks: tokio::task::JoinSet::new(),
            sizes: HashMap::new(),
            queued_bytes: 0,
            limit: limit.max(1),
            write_failures: 0,
            halted: None,
        }
    }

    fn record_write(&mut self, result: &VerifyResult) -> Option<String> {
        match &result.write_error {
            Some(error) => {
                self.write_failures = self.write_failures.saturating_add(1);
                if self.write_failures >= MAX_WRITE_FAILURES && self.halted.is_none() {
                    self.halted = Some(error.clone());
                    return self.halted.clone();
                }
            }
            None if result.verify_ok => self.write_failures = 0,
            None => {}
        }
        None
    }

    fn resume(&mut self) {
        self.write_failures = 0;
        self.halted = None;
    }

    fn spawn<F>(&mut self, bytes: u64, task: F)
    where
        F: std::future::Future<Output = VerifyResult> + Send + 'static,
    {
        let id = self.tasks.spawn(task).id();
        self.sizes.insert(id, bytes);
        self.queued_bytes += bytes;
    }

    fn is_full(&self) -> bool {
        self.halted.is_some() || self.queued_bytes >= self.limit
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    fn settle(
        &mut self,
        joined: Result<(tokio::task::Id, VerifyResult), tokio::task::JoinError>,
    ) -> Result<VerifyResult, tokio::task::JoinError> {
        let id = match &joined {
            Ok((id, _)) => *id,
            Err(e) => e.id(),
        };
        if let Some(bytes) = self.sizes.remove(&id) {
            self.queued_bytes -= bytes;
        }
        joined.map(|(_, result)| result)
    }

    async fn join_next(&mut self) -> Option<Result<VerifyResult, tokio::task::JoinError>> {
        let joined = self.tasks.join_next_with_id().await?;
        Some(self.settle(joined))
    }

    fn try_join_next(&mut self) -> Option<Result<VerifyResult, tokio::task::JoinError>> {
        let joined = self.tasks.try_join_next_with_id()?;
        Some(self.settle(joined))
    }
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
    disabled_files: Arc<Mutex<HashSet<(usize, usize)>>>,
    backoff: Arc<Mutex<HashMap<usize, (u32, Instant)>>>,
    etags: Arc<Mutex<HashMap<(usize, usize), String>>>,
    info: Arc<super::core::ValidatedTorrentMetaV1Info>,
    layout: FileSet,
    storage: Arc<FilesystemStorage>,
    verifier: PieceVerifier,
    lengths: Lengths,
    generation: GenerationToken,
    throttle: Throttle,
}

impl WebSeedContext {
    fn mirror_usable(&self, idx: usize) -> bool {
        !self.disabled.lock().contains(&idx)
            && self
                .backoff
                .lock()
                .get(&idx)
                .is_none_or(|&(_, retry_at)| Instant::now() >= retry_at)
    }

    fn has_available_mirror(&self) -> bool {
        (0..self.bases.len()).any(|idx| self.mirror_usable(idx))
    }

    fn disable(&self, idx: usize) {
        self.disabled.lock().insert(idx);
    }

    fn disable_file(&self, idx: usize, file: usize) {
        self.disabled_files.lock().insert((idx, file));
    }

    fn range_servable(&self, offset: u64, len: u64) -> bool {
        let disabled = self.disabled_files.lock();
        if disabled.is_empty() {
            return true;
        }
        let files: Vec<usize> = self
            .layout
            .spans_for(offset, len)
            .map(|span| span.file_index)
            .filter(|&file| !self.layout.files()[file].padding)
            .collect();
        (0..self.bases.len()).any(|mirror| {
            self.mirror_usable(mirror)
                && files
                    .iter()
                    .all(|&file| !disabled.contains(&(mirror, file)))
        })
    }

    fn pieces_servable(&self, pieces: &[u32]) -> bool {
        let (Some(&first), Some(&last)) = (pieces.first(), pieces.last()) else {
            return false;
        };
        let (Ok(first), Ok(last)) = (
            self.lengths.validate_piece(first),
            self.lengths.validate_piece(last),
        ) else {
            return false;
        };
        let offset = self.lengths.piece_offset(first);
        let end = self.lengths.piece_offset(last) + u64::from(self.lengths.piece_length_of(last));
        self.range_servable(offset, end - offset)
    }

    fn back_off(&self, idx: usize) {
        let mut backoff = self.backoff.lock();
        if backoff
            .get(&idx)
            .is_some_and(|&(_, retry_at)| Instant::now() < retry_at)
        {
            return;
        }
        let streak = backoff
            .get(&idx)
            .map_or(1, |&(streak, _)| streak.saturating_add(1));
        let delay = WEBSEED_RETRY_BASE
            .saturating_mul(1u32 << (streak - 1).min(7))
            .min(WEBSEED_RETRY_MAX);
        backoff.insert(idx, (streak, Instant::now() + delay));
    }

    fn mirror_succeeded(&self, idx: usize) {
        self.backoff.lock().remove(&idx);
    }
}

struct PieceAssembly {
    buf: Vec<u8>,
    sources: Vec<Option<IpAddr>>,
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
    upload_only_flag: Arc<AtomicBool>,
    ext_handshake_builder: crate::peer::ExtHandshakeBuilder,
) {
    let info = Arc::new(init.meta.info.clone());
    let info_hash = init.meta.info_hash;
    let info_bytes: Arc<Vec<u8>> = Arc::new(init.meta.info_bytes.clone());
    let lengths = init.lengths;
    let encryption = init.encryption;
    let utp = init.utp.clone();
    let outbound_utp = utp.clone();
    let upload_limiter = init.limits.up.clone();
    let recv_throttle = init.limits.down.clone();
    let private_torrent = init.meta.info.private;
    let mut dht = if private_torrent {
        None
    } else {
        init.dht.clone()
    };
    let mut p2p_proxy = init.p2p_proxy.clone();
    let blocklist = init.blocklist.clone();
    let verifier = init.verifier;
    let (supports_v2, hash_tables): (bool, Option<Arc<Vec<MerkleProofTable>>>) =
        if let PieceVerifier::V2Merkle { ref tables, .. } = verifier {
            (true, Some(Arc::clone(tables)))
        } else {
            match build_v2_tables(&init.meta) {
                Some(Ok(tables)) => (true, Some(Arc::new(tables))),
                Some(Err(e)) => {
                    tracing::warn!(
                        "hybrid torrent {info_hash}: could not build Merkle table for serving: {e}"
                    );
                    (false, None)
                }
                None => (false, None),
            }
        };
    let serve_v2_layers = supports_v2 && hash_tables.is_some();
    let leaf_hash_jobs = Arc::new(Semaphore::new(MAX_LEAF_HASH_JOBS));
    let advertise_v2 = init.advertise_v2 && serve_v2_layers;
    advertise_v2_flag.store(serve_v2_layers, Ordering::Relaxed);

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
    let storage = Arc::new(
        FilesystemStorage::new(&info, &init.root_dir).with_parts_dir(
            super::storage::parts_dir_for(&init.root_dir, &info_hash.to_hex()),
        ),
    );
    let mut piece_tracker = PieceTracker::new(lengths);
    let (mut only_files, applied) =
        apply_storage_selection(&storage, init.only_files.clone()).await;
    if let Err(e) = applied {
        tracing::warn!("file selection for {info_hash}: {e}");
    }
    piece_tracker.set_wanted(wanted_pieces(
        storage.layout(),
        &lengths,
        only_files.as_deref(),
    ));
    let mut chunk_tracker = ChunkTracker::new(lengths);
    let mut piece_assemblies: HashMap<u32, PieceAssembly> = HashMap::new();
    let (scan_tx, scan_rx) = mpsc::channel::<Vec<u32>>(16);
    let mut scan_rx = Some(scan_rx);
    let mut scanning = true;
    let mut scan_task = Some({
        let verifier = verifier.clone();
        let storage = Arc::clone(&storage);
        tokio::spawn(async move {
            if storage.has_existing_payload_files().await {
                scan_existing_pieces(&verifier, &storage, &lengths, scan_tx).await;
            }
        })
    });
    {
        let mut s = stats.lock();
        s.file_progress = Arc::new(vec![0; storage.layout().files().len()]);
        s.progress_bytes = 0;
        s.left_bytes = piece_tracker.bytes_left();
        s.finished = false;
    }

    let announce_hashes = if serve_v2_layers {
        init.meta.announce_infohashes()
    } else {
        vec![info_hash]
    };
    let generation = GenerationToken::new();
    let (peer_src_tx, mut peer_addr_rx) = mpsc::channel::<PeerEnvelope>(256);
    let mut tracker_tiers = collect_obfuscated_tiers(&init.meta);
    let obfuscated_tiers = tracker_tiers.len();
    tracker_tiers.extend(collect_tracker_tiers(&init.meta));
    let mut tracker_urls = flatten_tracker_tiers(&tracker_tiers);
    let tracker_info_hashes = announce_hashes.clone();
    let tracker_key = rand::rng().random::<u32>();
    let tracker_state = Arc::new(Mutex::new(TrackerLifecycle::default()));
    let (tracker_tier_tx, mut tracker_tier_rx) = mpsc::channel::<()>(4);
    let mut tracker_tasks = spawn_tracker_tier_pollers(
        peer_src_tx.clone(),
        Vec::new(),
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
        obfuscated_tiers,
    );

    let (peer_event_tx, mut peer_event_rx) = mpsc::channel::<(u32, PeerEvent)>(8192);
    let mut conn_lease = super::conn_budget::BudgetLease::new(init.conn_budget.clone());

    let mut peers: HashMap<u32, Peer> = HashMap::new();
    let mut next_pid: u32 = 1;
    let mut known_addrs: HashSet<SocketAddr> = HashSet::new();
    let mut tracker_authorized: HashSet<SocketAddr> = HashSet::new();
    let mut pex_source: HashMap<SocketAddr, u32> = HashMap::new();
    let mut holepunch_attempted: HashSet<SocketAddr> = HashSet::new();
    let mut mse_first_addrs: HashSet<SocketAddr> = HashSet::new();
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
    let mut last_quality_pass = Instant::now();
    let mut last_optimistic_rotate = Instant::now();
    let mut optimistic_pid: Option<u32> = None;
    let mut choke_dirty = false;
    let mut last_finished = false;
    let our_ext = OurExtHandshake {
        listen_port,
        metadata_size: init.meta.info_bytes.len() as u64,
        private: private_torrent,
        prefers_encryption: encryption != super::peer::EncryptionPolicy::PlaintextOnly,
    };
    let mut last_pex = Instant::now();
    let mut bytes_this_tick = (0u64, 0u64);
    let upload_tick: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let mut corruption = CorruptionTracker::new(init.hash_fail_ban_strikes);
    let failed_copies = corruption.copies_handle();
    let mut seen_ban_revision = blocklist.read().ban_revision();
    let mut dial_failures: HashMap<SocketAddr, u8> = HashMap::new();
    let mut write_tasks = DiskQueue::new(disk_queue_limit(lengths.piece_length()));
    let mut webseed_tasks: tokio::task::JoinSet<WebSeedBatchResult> = tokio::task::JoinSet::new();
    let mut webseed_inflight: HashSet<u32> = HashSet::new();
    let mut webseed_ctx = if !private_torrent && !init.meta.url_list.is_empty() {
        match build_webseed_client(p2p_proxy.as_ref()) {
            Ok(client) => Some(WebSeedContext {
                client,
                bases: Arc::new(init.meta.url_list.clone()),
                disabled: Arc::new(Mutex::new(HashSet::new())),
                disabled_files: Arc::new(Mutex::new(HashSet::new())),
                backoff: Arc::new(Mutex::new(HashMap::new())),
                etags: Arc::new(Mutex::new(HashMap::new())),
                info: Arc::clone(&info),
                layout: storage.layout().clone(),
                storage: Arc::clone(&storage),
                verifier: verifier.clone(),
                lengths,
                generation: generation.clone(),
                throttle: recv_throttle.clone(),
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
    let dht_relaxed = Arc::new(AtomicBool::new(false));
    let mut dht_poll_handle: Option<tokio::task::JoinHandle<()>> = dht.clone().map(|initial_dht| {
        initial_dht.set_private(info_hash, private_torrent);
        initial_dht.add_bootstrap_nodes(init.meta.bootstrap_nodes.clone());
        initial_dht.add_bootstrap_hosts(init.meta.bootstrap_hosts.clone());
        spawn_dht_poller(
            initial_dht,
            announce_hashes.clone(),
            listen_port,
            peer_src_tx.clone(),
            generation.clone(),
            Arc::clone(&dht_relaxed),
        )
    });

    let stop_ack = 'torrent: loop {
        conn_lease.sync(peers.len() + pending_dials.len());
        tokio::select! {
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else {
                    tracing::debug!("torrent {torrent_id} command channel closed; shutting down");
                    generation.bump();
                    break 'torrent None;
                };
                match cmd {
                TorrentCommand::AddPeer(PeerCandidate { addr, source }) => {
                    if private_torrent && source == PeerSource::Tracker {
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
                            &mse_first_addrs,
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
                            &recv_throttle,
                            &mut outbound_tasks,
                            &mut outbound_aborts,
                            &blocklist,
                            &mut conn_lease,
                        );
                    }
                }
                TorrentCommand::AddInboundPeer { addr, cmd_tx, piece_tx, bind, reserved, peer_id, io_abort, encrypted, utp } => {
                    let duplicate = resolve_duplicate(&peers, addr.ip(), Some(peer_id), our_peer_id, false);
                    if blocklist.read().contains(addr.ip())
                        || !inbound_peer_allowed(private_torrent, addr, &tracker_authorized)
                        || matches!(duplicate, DuplicateAction::RejectNew)
                    {
                        io_abort.abort();
                        let _ = cmd_tx.try_send(PeerCommand::Disconnect);
                    } else if !paused
                        && peers.len() < max_peers
                        && !known_addrs.contains(&addr)
                        && super::magnet::is_dialable_peer_addr(addr)
                        && conn_lease.try_reserve()
                    {
                        known_addrs.insert(addr);
                        if let DuplicateAction::ReplaceOld(old) = duplicate {
                            for old_pid in old {
                                if let Some(old_peer) = peers.get(&old_pid) {
                                    abort_peer_io(old_peer.io_abort.as_ref(), &old_peer.cmd_tx);
                                }
                            }
                        }
                        let pid = next_pid; next_pid += 1;
                        adopt_inbound_peer(
                            pid,
                            addr,
                            cmd_tx,
                            Some(piece_tx),
                            bind,
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
                            PeerLink { encrypted, utp },
                            dht_port_for(dht.as_ref(), private_torrent, addr),
                        )
                        .await;
                    } else {
                        if utp {
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
                        }
                        io_abort.abort();
                        let _ = cmd_tx.try_send(PeerCommand::Disconnect);
                    }
                }
                TorrentCommand::PauseIfHalted(ack) if paused || write_tasks.halted.is_none() => {
                    let _ = ack.send(());
                }
                TorrentCommand::Pause(ack) | TorrentCommand::PauseIfHalted(ack) => {
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
                    release_webseed_inflight(&mut webseed_inflight, &mut piece_tracker, &lengths);
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
                        s.live_mut().snapshot.peer_stats.live = 0;
                        s.clear_peers();
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
                                 &mut corruption, &blocklist,
                                )
                                .await;
                            }
                            Err(e) if !e.is_cancelled() => {
                                tracing::warn!("piece write/verify task failed during pause: {e}");
                            }
                            Err(_) => {}
                        }
                    }
                    if let Err(e) = storage.close_handles().await {
                        tracing::warn!("failed to close storage handles on pause: {e}");
                    }
                    let _ = ack.send(());
                }
                TorrentCommand::Unpause(ack) => {
                    paused = false;
                    write_tasks.resume();
                    stats.lock().error = None;
                    tracker_state.lock().announce_now(false);
                    if !scanning && tracker_tasks.is_empty() {
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
                            obfuscated_tiers,
                        );
                    }
                    if dht_poll_handle.is_none() {
                        if let Some(dht) = dht.clone() {
                            dht_poll_handle = Some(spawn_dht_poller(
                                dht,
                                announce_hashes.clone(),
                                listen_port,
                                peer_src_tx.clone(),
                                generation.clone(),
                                Arc::clone(&dht_relaxed),
                            ));
                        }
                    }
                    drain_peer_backlog(
                        &mut priority_backlog,
                        &mut peer_backlog,
                        &mut dial_retries,
                        &mut useful_redials,
                        &mut pending_dials,
                        &mut known_addrs,
                        &useful_peers,
                        &holepunch_attempted,
                        &mse_first_addrs,
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
                            &recv_throttle,
                            &mut outbound_tasks,
                            &mut outbound_aborts,
                            &blocklist,
                            &mut conn_lease,
                        );
                    let _ = ack.send(());
                }
                TorrentCommand::ReconfigureP2p { proxy, replace_proxy, dht: next_dht, ack } => {
                    generation.bump();
                    if replace_proxy {
                        p2p_proxy = proxy;
                    }
                    webseed_tasks.shutdown().await;
                    release_webseed_inflight(&mut webseed_inflight, &mut piece_tracker, &lengths);
                    if replace_proxy {
                        webseed_ctx = if !private_torrent && !init.meta.url_list.is_empty() {
                            build_webseed_client(p2p_proxy.as_ref()).ok().map(|client| WebSeedContext {
                                client,
                                bases: Arc::new(init.meta.url_list.clone()),
                                disabled: Arc::new(Mutex::new(HashSet::new())),
                                disabled_files: Arc::new(Mutex::new(HashSet::new())),
                                backoff: Arc::new(Mutex::new(HashMap::new())),
                                etags: Arc::new(Mutex::new(HashMap::new())),
                                info: Arc::clone(&info),
                                layout: storage.layout().clone(),
                                storage: Arc::clone(&storage),
                                verifier: verifier.clone(),
                                lengths,
                                generation: generation.clone(),
                                throttle: recv_throttle.clone(),
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
                            announce_hashes.clone(),
                            listen_port,
                            peer_src_tx.clone(),
                            generation.clone(),
                            Arc::clone(&dht_relaxed),
                        ));
                    }
                    if replace_proxy {
                        tracker_tasks.shutdown(false).await;
                    }
                    let _ = ack.send(());
                }
                TorrentCommand::SetOnlyFiles { files, ack } => {
                    let applied;
                    (only_files, applied) = apply_storage_selection(&storage, files).await;
                    piece_tracker.set_wanted(wanted_pieces(
                        storage.layout(),
                        &lengths,
                        only_files.as_deref(),
                    ));
                    drop_unwanted_partial_pieces(
                        &piece_tracker,
                        &mut chunk_tracker,
                        &mut piece_assemblies,
                        &lengths,
                    );
                    {
                        let mut s = stats.lock();
                        s.left_bytes = piece_tracker.bytes_left();
                        s.finished = !scanning && piece_tracker.is_complete();
                    }
                    for peer in peers.values_mut() {
                        refresh_peer_needs(peer, &piece_tracker, &lengths);
                    }
                    let _ = ack.send(applied);
                }
                TorrentCommand::AddTrackers { urls, ack } => {
                    let new_urls = normalize_tracker_urls(urls, &tracker_urls);
                    let added = new_urls.len();
                    for url in &new_urls {
                        tracker_urls.push(url.clone());
                    }
                    if !new_urls.is_empty() {
                        tracker_tiers.push(new_urls.clone());
                        tracker_state.lock().append_tier(&new_urls);
                        if tracker_tasks.is_empty() && !(paused || scanning) {
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
                                obfuscated_tiers,
                            );
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
                    storage.invalidate_read_cache();
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
                if envelope.mse_first {
                    if mse_first_addrs.len() >= MAX_MSE_FIRST_ADDRS {
                        mse_first_addrs.clear();
                    }
                    mse_first_addrs.insert(addr);
                }
                if private_torrent && source == PeerSource::Tracker {
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
                        &mse_first_addrs,
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
                            &recv_throttle,
                            &mut outbound_tasks,
                            &mut outbound_aborts,
                            &blocklist,
                            &mut conn_lease,
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
                    abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
                }
            }
            found = recv_scan_batch(&mut scan_rx) => {
                match found {
                    Some(found) => {
                        let mut newly_local = Vec::with_capacity(found.len());
                        {
                            let mut s = stats.lock();
                            for index in found {
                                let Ok(vpi) = lengths.validate_piece(index) else {
                                    continue;
                                };
                                if mark_verified_piece_local(&mut piece_tracker, vpi) {
                                    add_piece_progress(&mut s, &lengths, storage.layout(), vpi);
                                    newly_local.push(index);
                                }
                            }
                            s.left_bytes = piece_tracker.bytes_left();
                        }
                        if !peers.is_empty() {
                            for index in newly_local {
                                broadcast_have(&mut peers, index);
                            }
                        }
                    }
                    None => {
                        scan_rx = None;
                        if let Some(task) = scan_task.take() {
                            let _ = task.await;
                        }
                        if let Err(e) = storage
                            .preallocate_selected(selected_file_set(only_files.as_deref()).as_ref())
                            .await
                        {
                            tracing::warn!("preallocate failed for {info_hash}: {e}");
                        }
                        scanning = false;
                        {
                            let mut s = stats.lock();
                            s.left_bytes = piece_tracker.bytes_left();
                            s.finished = piece_tracker.is_complete();
                        }
                        piece_tracker.refresh_order();
                        for peer in peers.values_mut() {
                            refresh_peer_needs(peer, &piece_tracker, &lengths);
                        }
                        if !paused {
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
                                    obfuscated_tiers,
                                );
                            }
                            drain_peer_backlog(
                                &mut priority_backlog,
                                &mut peer_backlog,
                                &mut dial_retries,
                                &mut useful_redials,
                                &mut pending_dials,
                                &mut known_addrs,
                                &useful_peers,
                                &holepunch_attempted,
                                &mse_first_addrs,
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
                                &recv_throttle,
                                &mut outbound_tasks,
                                &mut outbound_aborts,
                                &blocklist,
                                &mut conn_lease,
                            );
                        }
                    }
                }
            }
            Some((pid, ev)) = peer_event_rx.recv() => {
                let kick = process_peer_event(
                    torrent_id, info_hash, init.meta.info.private, pid, ev, our_peer_id, &registry_scope, paused, &mut peers, &mut piece_tracker, &mut chunk_tracker,
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
                    &mut dial_failures,
                    &mut pex_source, &mut holepunch_attempted,
                    &peer_src_tx,
                    &generation,
                    &verifier,
                    failed_copies.as_ref(),
                    &info_bytes,
                    hash_tables.as_deref().map(|v| &**v),
                    &leaf_hash_jobs,
                    pipeline_floor,
                    pipeline_cap,
                    max_peers,
                    &mut choke_dirty,
                    &upload_limiter,
                    dht.as_ref(),
                    &blocklist,
                    &tracker_authorized,
                ).await;
                if kick && !(paused || scanning) && !write_tasks.is_full() {
                    drive_peer(pid, &mut peers, &mut piece_tracker, &mut chunk_tracker, &webseed_inflight);
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
                            if vr.verify_ok && vr.write_error.is_none() {
                                if let Ok(vpi) = lengths.validate_piece(vr.piece_index) {
                                    bytes_this_tick.0 += u64::from(lengths.piece_length_of(vpi));
                                }
                            }
                            if let Some(error) = write_tasks.record_write(&vr) {
                                tracing::error!("torrent {info_hash}: disk writes keep failing, downloading stopped: {error}");
                                stats.lock().error = Some(format!("Disk write failed: {error}"));
                            }
                            process_verify_result(
                                vr, &lengths, &mut piece_tracker,
                                &mut chunk_tracker, &mut peers, &storage, &stats,
                                &mut piece_assemblies,
                             &mut corruption, &blocklist,
                            ).await;
                        }
                        if !(paused || scanning) {
                            if !write_tasks.is_full() {
                                drive_requests(&mut peers, &mut piece_tracker, &mut chunk_tracker, &webseed_inflight);
                            }
                            if let Some(ctx) = webseed_ctx.as_ref() {
                                schedule_webseed_workers(
                                    &mut webseed_tasks,
                                    &mut webseed_inflight,
                                    &mut piece_tracker,
                                    &chunk_tracker,
                                    ctx,
                                    write_tasks.halted.is_some(),
                                );
                            }
                        }
                    }
                    Some(Err(e)) if !e.is_cancelled() => {
                        tracing::warn!("WebSeed worker failed: {e}");
                    }
                    Some(Err(_)) | None => {}
                }
            }
            result = write_tasks.join_next(), if !write_tasks.is_empty() => {
                let mut next = result;
                let mut verified_any = false;
                while let Some(result) = next {
                    match result {
                        Ok(vr) => {
                            if let Some(error) = write_tasks.record_write(&vr) {
                                tracing::error!("torrent {info_hash}: disk writes keep failing, downloading stopped: {error}");
                                stats.lock().error = Some(format!("Disk write failed: {error}"));
                            }
                            process_verify_result(
                                vr, &lengths, &mut piece_tracker,
                                &mut chunk_tracker, &mut peers, &storage, &stats,
                                &mut piece_assemblies,
                             &mut corruption, &blocklist,
                            ).await;
                            verified_any = true;
                        }
                        Err(e) if !e.is_cancelled() => {
                            tracing::warn!("piece write/verify task failed: {e}");
                        }
                        Err(_) => {}
                    }
                    next = write_tasks.try_join_next();
                }
                if verified_any && !(paused || scanning) && !write_tasks.is_full() {
                    drive_requests(&mut peers, &mut piece_tracker, &mut chunk_tracker, &webseed_inflight);
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
                        &mse_first_addrs,
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
                            &recv_throttle,
                            &mut outbound_tasks,
                            &mut outbound_aborts,
                            &blocklist,
                            &mut conn_lease,
                        );
                }
            }
            _ = tick.tick() => {
                let now = Instant::now();
                storage.trim_read_cache();
                let ban_revision = blocklist.read().ban_revision();
                if ban_revision != seen_ban_revision {
                    seen_ban_revision = ban_revision;
                    let list = blocklist.read();
                    for peer in peers.values() {
                        if list.contains(peer.addr.ip()) {
                            abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
                        }
                    }
                }
                let dt = now.duration_since(last_tick).as_secs_f32().max(0.001);
                last_tick = now;
                if now.duration_since(last_keepalive) >= Duration::from_secs(90) {
                    last_keepalive = now;
                    for p in peers.values() {
                        let _ = p.cmd_tx.try_send(PeerCommand::Send(Message::KeepAlive));
                    }
                }
                let queued_chunks: usize = peers.values().map(|p| p.outstanding.len()).sum();
                let request_timeout = request_timeout_for(
                    tightest(
                        recv_throttle.global().limit_bps(),
                        recv_throttle.task().limit_bps(),
                    ),
                    queued_chunks,
                );
                let reclaimed = chunk_tracker.reclaim_stale(request_timeout);
                let mut reclaimed_requests = 0usize;
                if !reclaimed.is_empty() {
                    let mut unblocked_pieces: HashSet<u32> = HashSet::new();
                    for r in &reclaimed {
                        if let Some(p) = peers.get_mut(&r.peer) {
                            if !p.snubbing {
                                p.mark_snubbed();
                                p.shrink_window(pipeline_floor, pipeline_cap);
                            }
                            if let Some(slot) = p
                                .outstanding
                                .iter()
                                .position(|&(pi, be, _)| pi == r.piece && be == r.begin)
                            {
                                let (index, begin, length) = p.outstanding.swap_remove(slot);
                                remember_canceled_request(p, (index, begin, length));
                                let _ = p.cmd_tx.try_send(PeerCommand::Send(Message::Cancel {
                                    index,
                                    begin,
                                    length,
                                }));
                                reclaimed_requests += 1;
                            }
                        }
                        unblocked_pieces.insert(r.piece);
                    }
                    for pi in unblocked_pieces {
                        if let Ok(vpi) = lengths.validate_piece(pi) {
                            piece_tracker.clear_in_flight(vpi);
                        }
                    }
                }
                {
                    let mut to_evict: Vec<u32> = Vec::new();
                    let mut recycled: HashSet<u32> = HashSet::new();
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
                    if now.duration_since(last_quality_pass) >= PEER_QUALITY_INTERVAL {
                        last_quality_pass = now;
                        let candidates_waiting =
                            !priority_backlog.is_empty() || !peer_backlog.is_empty();
                        if candidates_waiting && peers.len() * 10 >= max_peers * 9 {
                            for pid in redundant_peers(
                                &peers,
                                piece_tracker.is_complete(),
                                now,
                                lengths.total_pieces() as usize,
                                MAX_QUALITY_EVICTIONS,
                            ) {
                                if !to_evict.contains(&pid) {
                                    recycled.insert(pid);
                                    to_evict.push(pid);
                                }
                            }
                        }
                    }
                    let mut evicted_any = false;
                    for pid in to_evict {
                        if let Some(p) = peers.remove(&pid) {
                            evicted_any = true;
                            if let Some(dial_addr) = p.listen_addr {
                                if p.downloaded_total > 0 {
                                    remember_useful_peer(
                                        &mut useful_peers,
                                        dial_addr,
                                        p.max_outstanding.clamp(pipeline_floor, pipeline_cap),
                                    );
                                }
                                let known_useful = useful_peers.contains_key(&dial_addr);
                                let idle = p.downloaded_total == 0
                                    && p.uploaded_total.load(Ordering::Relaxed) == 0;
                                let mut failures = if idle {
                                    bump_dial_failures(&mut dial_failures, dial_addr)
                                } else {
                                    0
                                };
                                if recycled.contains(&pid) {
                                    failures = failures.max(2);
                                }
                                if known_useful || failures < MAX_DIAL_FAILURES {
                                    schedule_peer_retry(
                                        dial_addr,
                                        known_useful,
                                        failures,
                                        &mut dial_retries,
                                        &mut useful_redials,
                                    );
                                }
                            }
                            pex_source.retain(|_, relay| *relay != pid);
                            holepunch_attempted.remove(&p.addr);
                            abort_peer_io(p.io_abort.as_ref(), &p.cmd_tx);
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
                let finished_now = stats.lock().finished;
                if finished_now != last_finished {
                    last_finished = finished_now;
                    upload_only_flag.store(finished_now, Ordering::Relaxed);
                    if finished_now {
                        tracker_state.lock().announce_now(true);
                    }
                    for p in peers.values().filter(|p| p.ext_seen) {
                        let _ = p.cmd_tx.try_send(PeerCommand::Send(Message::Extended {
                            ext_id: EXT_HANDSHAKE_ID,
                            payload: our_ext.build(p.addr.ip(), finished_now).encode(),
                        }));
                    }
                }
                dht_relaxed.store(
                    finished_now || peers.len() * 2 >= max_peers,
                    Ordering::Relaxed,
                );
                for p in peers.values_mut() {
                    if !p.pending_haves.is_empty() {
                        flush_pending_haves(p);
                    }
                }
                let full_choke_eval = now.duration_since(last_choke_eval) >= CHOKE_EVAL_INTERVAL;
                if choke_dirty || full_choke_eval {
                    choke_dirty = false;
                    let optimistic_lost = optimistic_pid
                        .is_some_and(|pid| !peers.get(&pid).is_some_and(|p| p.peer_interested));
                    if optimistic_lost
                        || (full_choke_eval
                            && now.duration_since(last_optimistic_rotate)
                                >= OPTIMISTIC_ROTATE_INTERVAL)
                    {
                        last_optimistic_rotate = now;
                        optimistic_pid = choose_optimistic(&mut peers, optimistic_pid);
                    }
                    if full_choke_eval {
                        last_choke_eval = now;
                        run_choke_eval(
                            &mut peers,
                            piece_tracker.is_complete(),
                            optimistic_pid,
                            true,
                        );
                    } else {
                        fill_choke_slots(&mut peers, piece_tracker.is_complete(), optimistic_pid);
                    }
                }
                if !private_torrent && now.duration_since(last_pex) >= PEX_INTERVAL {
                    last_pex = now;
                    let total_pieces = lengths.total_pieces() as usize;
                    let current: HashMap<SocketAddr, u8> = peers
                        .values()
                        .filter_map(|p| p.listen_addr.map(|a| (a, p.pex_flags(total_pieces))))
                        .collect();
                    for p in peers.values_mut() {
                        let Some(pex_id) = p.their_ut_pex_id else {
                            continue;
                        };
                        let added: Vec<(SocketAddr, u8)> = current
                            .iter()
                            .filter(|(a, _)| {
                                **a != p.addr
                                    && Some(**a) != p.listen_addr
                                    && !p.pex_sent.contains(a)
                            })
                            .take(MAX_PEX_ADDED_PER_MSG)
                            .map(|(a, flags)| (*a, *flags))
                            .collect();
                        let dropped: Vec<SocketAddr> = p
                            .pex_sent
                            .iter()
                            .filter(|a| !current.contains_key(a))
                            .take(MAX_PEX_ADDED_PER_MSG)
                            .copied()
                            .collect();
                        if added.is_empty() && dropped.is_empty() {
                            continue;
                        }
                        for a in &dropped {
                            p.pex_sent.remove(a);
                        }
                        p.pex_sent.extend(added.iter().map(|(a, _)| *a));
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
                    s.session_downloaded = s.session_downloaded.saturating_add(bytes_this_tick.0);
                    s.live_mut().update(bytes_this_tick.0, bytes_this_tick.1, dt);
                    s.live_mut().snapshot.peer_stats.live = peers.len() as u32;
                    if let Some(peer_snaps) = peer_snaps {
                        s.set_peers(peer_snaps);
                    }
                }
                if tracing::enabled!(target: "diag", tracing::Level::DEBUG) {
                    let active_request_peers = peers
                        .values()
                        .filter(|peer| request_peer_eligible(peer))
                        .count();
                    let RequestLoad {
                        demand,
                        outstanding: outstanding_requests,
                    } = RequestLoad::measure(&peers);
                    let snubbed_peers = peers.values().filter(|peer| peer.snubbing).count();
                    let max_pipeline = peers
                        .values()
                        .map(|peer| peer.max_outstanding)
                        .max()
                        .unwrap_or(0);
                    let max_effective_pipeline = peers
                        .values()
                        .filter(|peer| request_peer_eligible(peer))
                        .map(|peer| effective_request_limit(peer.max_outstanding, demand))
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
                }
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
                        &mse_first_addrs,
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
                            &recv_throttle,
                            &mut outbound_tasks,
                            &mut outbound_aborts,
                            &blocklist,
                            &mut conn_lease,
                    );
                }
                if !(paused || scanning) {
                    piece_tracker.refresh_order();
                    if !write_tasks.is_full() {
                        drive_requests(&mut peers, &mut piece_tracker, &mut chunk_tracker, &webseed_inflight);
                    }
                    if let Some(ctx) = webseed_ctx.as_ref() {
                        schedule_webseed_workers(
                            &mut webseed_tasks,
                            &mut webseed_inflight,
                            &mut piece_tracker,
                            &chunk_tracker,
                            ctx,
                            write_tasks.halted.is_some(),
                        );
                    }
                }
            }
        }
    };

    if let Some(handle) = dht_poll_handle.take() {
        handle.abort();
    }
    if let Some(task) = scan_task.take() {
        task.abort();
        let _ = task.await;
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

    priority_backlog.clear();
    peer_backlog.clear();
    dial_retries.clear();
    useful_redials.clear();
    useful_peers.clear();
    known_addrs.clear();
    pex_source.clear();
    holepunch_attempted.clear();

    let drain_writes = async {
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
                        &mut corruption,
                        &blocklist,
                    )
                    .await;
                }
                Err(e) if !e.is_cancelled() => {
                    tracing::warn!("piece write/verify task failed during shutdown: {e}");
                }
                Err(_) => {}
            }
        }
    };
    tokio::join!(tracker_tasks.shutdown(true), drain_writes);

    if let Err(e) = storage.close_handles().await {
        tracing::warn!("failed to close storage handles on shutdown: {e}");
    }
    {
        let mut stats = stats.lock();
        stats.live_mut().snapshot.peer_stats.live = 0;
        stats.clear_peers();
    }
    if let Some(ack) = stop_ack {
        let _ = ack.send(());
    }
}

fn spawn_dht_poller(
    dht: Arc<super::dht::Dht>,
    info_hashes: Vec<Id20>,
    listen_port: u16,
    peer_tx: mpsc::Sender<PeerEnvelope>,
    generation: GenerationToken,
    relaxed: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            for &info_hash in &info_hashes {
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
                            mse_first: false,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            let (base, jitter) = if relaxed.load(Ordering::Relaxed) {
                (DHT_RELAXED_INTERVAL, 60)
            } else {
                (DHT_POLL_INTERVAL, 30)
            };
            let jitter = rand::rng().random_range(0..=jitter);
            tokio::time::sleep(base + Duration::from_secs(jitter)).await;
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
        piece_tx: Option<mpsc::Sender<Message>>,
        addr: SocketAddr,
        io_abort: Option<AbortHandle>,
    }

    type RegistryKey = (usize, usize, u32);
    type PeerCmdRegistry = StdMutex<HashMap<RegistryKey, RegistryEntry>>;
    static REG: LazyLock<PeerCmdRegistry> = LazyLock::new(|| StdMutex::new(HashMap::new()));

    fn key(scope: &Arc<()>, torrent_id: usize, pid: u32) -> RegistryKey {
        (Arc::as_ptr(scope) as usize, torrent_id, pid)
    }

    #[cfg(test)]
    pub fn put(
        torrent_id: usize,
        pid: u32,
        scope: &Arc<()>,
        tx: mpsc::Sender<PeerCommand>,
        addr: SocketAddr,
        io_abort: Option<AbortHandle>,
    ) {
        put_with_lane(torrent_id, pid, scope, tx, None, addr, io_abort);
    }

    pub fn put_with_lane(
        torrent_id: usize,
        pid: u32,
        scope: &Arc<()>,
        tx: mpsc::Sender<PeerCommand>,
        piece_tx: Option<mpsc::Sender<Message>>,
        addr: SocketAddr,
        io_abort: Option<AbortHandle>,
    ) {
        REG.lock().unwrap().insert(
            key(scope, torrent_id, pid),
            RegistryEntry {
                scope: scope.clone(),
                tx,
                piece_tx,
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
        take_with_lane(torrent_id, pid, scope).map(|(tx, _, addr, io_abort)| (tx, addr, io_abort))
    }

    #[allow(clippy::type_complexity)]
    pub fn take_with_lane(
        torrent_id: usize,
        pid: u32,
        scope: &Arc<()>,
    ) -> Option<(
        mpsc::Sender<PeerCommand>,
        Option<mpsc::Sender<Message>>,
        SocketAddr,
        Option<AbortHandle>,
    )> {
        REG.lock()
            .unwrap()
            .remove(&key(scope, torrent_id, pid))
            .map(|entry| (entry.tx, entry.piece_tx, entry.addr, entry.io_abort))
    }

    pub fn remove(torrent_id: usize, pid: u32, scope: &Arc<()>) {
        REG.lock().unwrap().remove(&key(scope, torrent_id, pid));
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
        let scope_key = Arc::as_ptr(scope) as usize;
        let keys = reg
            .iter()
            .filter(|(&(entry_scope, entry_torrent_id, _), entry)| {
                entry_scope == scope_key
                    && entry_torrent_id == torrent_id
                    && Arc::ptr_eq(&entry.scope, scope)
            })
            .map(|(key, _)| *key)
            .collect::<Vec<_>>();
        keys.into_iter()
            .filter_map(|key| {
                reg.remove(&key)
                    .map(|entry| (key.2, entry.tx, entry.addr, entry.io_abort))
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
    recv_throttle: Throttle,
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
        read_timeout: Duration::from_secs(120),
        encryption,
        advertise_v2,
        advertise_dht,
        ext_handshake_builder,
        proxy,
        deferred: true,
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
        Ok((mut handle, mut rx)) => {
            handle.gate.set(recv_throttle);
            peer_registry::put_with_lane(
                torrent_id,
                pid,
                &registry_scope,
                handle.tx.clone(),
                Some(handle.piece_tx.clone()),
                handle.addr,
                Some(handle.io_abort.clone()),
            );
            let handshook = rx.recv().await;
            let Some(bind) = handle.bind.take() else {
                return;
            };
            let delivered = match handshook {
                Some(ev) => event_tx.send((pid, ev)).await.is_ok(),
                None => false,
            };
            if !delivered || bind.send(PeerEventSink::new(pid, event_tx)).is_err() {
                peer_registry::remove(torrent_id, pid, &registry_scope);
            }
        }
        Err(e) => {
            let reason = if crate::conn_budget::is_fd_exhaustion(&e) {
                crate::conn_budget::FD_EXHAUSTED_REASON.to_string()
            } else {
                format!("connect: {e}")
            };
            let _ = event_tx
                .send((pid, PeerEvent::Disconnected { reason }))
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

fn send_initial_availability(
    cmd_tx: &mpsc::Sender<PeerCommand>,
    bitfield: &[u8],
    total_pieces: u32,
    supports_fast: bool,
) {
    if let Some(message) = initial_availability_message(bitfield, total_pieces, supports_fast) {
        let _ = cmd_tx.try_send(PeerCommand::Send(message));
    }
}

fn accept_initial_availability(peer: &mut Peer, availability: InitialAvailability) -> bool {
    if peer.initial_availability.is_some() {
        return false;
    }
    peer.initial_availability = Some(availability);
    true
}

fn send_allowed_fast(peer: &mut Peer, info_hash: &Id20, total_pieces: u32) {
    if !peer.supports_fast {
        return;
    }
    for index in allowed_fast_set(info_hash, peer.addr, total_pieces) {
        peer.sent_allowed_fast.insert(index);
        let _ = peer
            .cmd_tx
            .try_send(PeerCommand::Send(Message::AllowedFast(index)));
    }
}

fn send_fast_reject(peer: &Peer, index: u32, begin: u32, length: u32) {
    if peer.supports_fast {
        let _ = peer
            .cmd_tx
            .try_send(PeerCommand::Send(Message::RejectRequest {
                index,
                begin,
                length,
            }));
    }
}

struct UploadJob {
    pending: Arc<Mutex<PendingUploads>>,
    storage: Arc<FilesystemStorage>,
    lane: UploadLane,
    stats: Arc<Mutex<TorrentStats>>,
    upload_tick: Arc<AtomicU64>,
    peer_uploaded: Arc<AtomicU64>,
    peer_uploaded_total: Arc<AtomicU64>,
    limiter: Throttle,
    supports_fast: bool,
    key: (u32, u32, u32),
    offset: u64,
    extent: Option<(u64, usize)>,
}

async fn serve_upload(job: UploadJob) {
    let UploadJob {
        pending,
        storage,
        lane,
        stats,
        upload_tick,
        peer_uploaded,
        peer_uploaded_total,
        limiter,
        supports_fast,
        key: request_key,
        offset,
        extent,
    } = job;
    let (index, begin, length) = request_key;
    let upload_len = length as u64;
    limiter.acquire(upload_len as usize).await;
    if !pending.lock().contains(&request_key) {
        return;
    }
    let Ok(permit) = lane.reserve().await else {
        pending.lock().remove(&request_key);
        return;
    };
    if !pending.lock().contains(&request_key) {
        return;
    }
    let read = match extent {
        Some((ext_start, ext_len)) => {
            storage
                .read_block_cached(ext_start, ext_len, offset, length as usize)
                .await
        }
        None => storage
            .read_owned(offset, length as usize)
            .await
            .map(Bytes::from),
    };
    let Ok(data) = read else {
        if pending.lock().remove(&request_key) && supports_fast {
            permit.send(Message::RejectRequest {
                index,
                begin,
                length,
            });
        }
        return;
    };
    if !pending.lock().remove(&request_key) {
        return;
    }
    permit.send(Message::Piece { index, begin, data });
    upload_tick.fetch_add(upload_len, Ordering::Relaxed);
    peer_uploaded.fetch_add(upload_len, Ordering::Relaxed);
    peer_uploaded_total.fetch_add(upload_len, Ordering::Relaxed);
    stats.lock().uploaded_bytes += upload_len;
}

enum UploadLane {
    Piece(mpsc::Sender<Message>),
    Control(mpsc::Sender<PeerCommand>),
}

enum UploadSlot {
    Piece(mpsc::OwnedPermit<Message>),
    Control(mpsc::OwnedPermit<PeerCommand>),
}

impl UploadLane {
    fn new(cmd_tx: &mpsc::Sender<PeerCommand>, piece_tx: Option<&mpsc::Sender<Message>>) -> Self {
        match piece_tx {
            Some(tx) => Self::Piece(tx.clone()),
            None => Self::Control(cmd_tx.clone()),
        }
    }

    async fn reserve(&self) -> Result<UploadSlot, ()> {
        match self {
            Self::Piece(tx) => tx
                .clone()
                .reserve_owned()
                .await
                .map(UploadSlot::Piece)
                .map_err(|_| ()),
            Self::Control(tx) => tx
                .clone()
                .reserve_owned()
                .await
                .map(UploadSlot::Control)
                .map_err(|_| ()),
        }
    }
}

impl UploadSlot {
    fn send(self, msg: Message) {
        match self {
            Self::Piece(permit) => {
                permit.send(msg);
            }
            Self::Control(permit) => {
                permit.send(PeerCommand::Send(msg));
            }
        }
    }
}

fn send_reliably(cmd_tx: &mpsc::Sender<PeerCommand>, cmd: PeerCommand) {
    if let Err(mpsc::error::TrySendError::Full(cmd)) = cmd_tx.try_send(cmd) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let cmd_tx = cmd_tx.clone();
            handle.spawn(async move {
                let _ = cmd_tx.send(cmd).await;
            });
        }
    }
}

fn send_owed_reject(cmd_tx: &mpsc::Sender<PeerCommand>, index: u32, begin: u32, length: u32) {
    send_reliably(
        cmd_tx,
        PeerCommand::Send(Message::RejectRequest {
            index,
            begin,
            length,
        }),
    );
}

fn dht_port_for(
    dht: Option<&Arc<crate::dht::Dht>>,
    private_torrent: bool,
    addr: SocketAddr,
) -> Option<u16> {
    if private_torrent {
        return None;
    }
    dht.and_then(|node| node.local_port_for(addr.is_ipv6()))
}

fn send_dht_port(peer: &Peer, reserved: &[u8; 8], port: Option<u16>) {
    if let Some(port) = port.filter(|_| dht_bit(reserved)) {
        let _ = peer.cmd_tx.try_send(PeerCommand::Send(Message::Port(port)));
    }
}

fn greet_peer(
    peers: &mut HashMap<u32, Peer>,
    pid: u32,
    piece_tracker: &PieceTracker,
    lengths: &Lengths,
    info_hash: &Id20,
    reserved: &[u8; 8],
    dht_port: Option<u16>,
) {
    let Some(peer) = peers.get_mut(&pid) else {
        return;
    };
    let bf = piece_tracker.bitfield();
    send_initial_availability(
        &peer.cmd_tx,
        &bf,
        lengths.total_pieces(),
        peer.supports_fast,
    );
    send_allowed_fast(peer, info_hash, lengths.total_pieces());
    send_dht_port(peer, reserved, dht_port);
}

enum DuplicateAction {
    Keep,
    RejectNew,
    ReplaceOld(Vec<u32>),
}

fn resolve_duplicate(
    peers: &HashMap<u32, Peer>,
    ip: IpAddr,
    peer_id: Option<Id20>,
    our_peer_id: Id20,
    new_outbound: bool,
) -> DuplicateAction {
    let Some(id) = peer_id.filter(|id| *id != our_peer_id) else {
        return DuplicateAction::Keep;
    };
    let existing: Vec<u32> = peers
        .iter()
        .filter(|(_, p)| p.peer_id == Some(id) && p.addr.ip() == ip)
        .map(|(&pid, _)| pid)
        .collect();
    if existing.is_empty() {
        return DuplicateAction::Keep;
    }
    let prefer_outbound = our_peer_id.0 < id.0;
    if new_outbound == prefer_outbound
        && existing
            .iter()
            .all(|pid| peers[pid].outbound != new_outbound)
    {
        DuplicateAction::ReplaceOld(existing)
    } else {
        DuplicateAction::RejectNew
    }
}

#[allow(clippy::too_many_arguments)]
async fn adopt_inbound_peer(
    pid: u32,
    addr: SocketAddr,
    cmd_tx: mpsc::Sender<PeerCommand>,
    piece_tx: Option<mpsc::Sender<Message>>,
    bind: oneshot::Sender<PeerEventSink>,
    event_tx: mpsc::Sender<(u32, PeerEvent)>,
    peers: &mut HashMap<u32, Peer>,
    lengths: &Lengths,
    piece_tracker: &mut PieceTracker,
    pipeline_floor: usize,
    pipeline_cap: usize,
    reserved: [u8; 8],
    peer_id: Option<Id20>,
    io_abort: AbortHandle,
    info_hash: Id20,
    link: PeerLink,
    dht_port: Option<u16>,
) {
    if bind.send(PeerEventSink::new(pid, event_tx)).is_err() {
        io_abort.abort();
        return;
    }
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
    peer.piece_tx = piece_tx;
    peer.link = link;
    peers.insert(pid, peer);
    greet_peer(
        peers,
        pid,
        piece_tracker,
        lengths,
        &info_hash,
        &reserved,
        dht_port,
    );
}

#[allow(clippy::too_many_arguments)]
async fn process_peer_event(
    torrent_id: usize,
    info_hash: Id20,
    private_torrent: bool,
    pid: u32,
    ev: PeerEvent,
    our_peer_id: Id20,
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
    write_tasks: &mut DiskQueue,
    pending_dials: &mut HashMap<u32, SocketAddr>,
    outbound_aborts: &mut HashMap<u32, AbortHandle>,
    known_addrs: &mut HashSet<SocketAddr>,
    priority_backlog: &mut VecDeque<SocketAddr>,
    peer_backlog: &mut VecDeque<SocketAddr>,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
    useful_peers: &mut HashMap<SocketAddr, usize>,
    dial_failures: &mut HashMap<SocketAddr, u8>,
    pex_source: &mut HashMap<SocketAddr, u32>,
    holepunch_attempted: &mut HashSet<SocketAddr>,
    peer_src_tx: &mpsc::Sender<PeerEnvelope>,
    generation: &GenerationToken,
    verifier: &PieceVerifier,
    failed_copies: Option<&SharedFailedCopies>,
    info_bytes: &Arc<Vec<u8>>,
    hash_tables: Option<&[MerkleProofTable]>,
    leaf_hash_jobs: &Arc<Semaphore>,
    pipeline_floor: usize,
    pipeline_cap: usize,
    max_peers: usize,
    choke_dirty: &mut bool,
    upload_limiter: &Throttle,
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
            utp,
            ..
        } => {
            if !peers.contains_key(&pid) {
                if let Some((cmd_tx, piece_tx, registry_addr, io_abort)) =
                    peer_registry::take_with_lane(torrent_id, pid, registry_scope)
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

                    match resolve_duplicate(peers, addr.ip(), Some(peer_id), our_peer_id, true) {
                        DuplicateAction::Keep => {}
                        DuplicateAction::RejectNew => {
                            reject_handshook_peer(
                                pid,
                                &cmd_tx,
                                io_abort,
                                outbound_aborts,
                                torrent_id,
                                registry_scope,
                            );
                            if !peers
                                .values()
                                .any(|p| p.addr == addr || p.listen_addr == Some(addr))
                            {
                                known_addrs.remove(&addr);
                            }
                            return false;
                        }
                        DuplicateAction::ReplaceOld(old) => {
                            for old_pid in old {
                                if let Some(old_peer) = peers.get(&old_pid) {
                                    abort_peer_io(old_peer.io_abort.as_ref(), &old_peer.cmd_tx);
                                }
                            }
                        }
                    }

                    if peers.len() >= max_peers {
                        let known_useful = useful_peers.contains_key(&addr);
                        schedule_peer_retry(addr, known_useful, 0, dial_retries, useful_redials);
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
                    peer.piece_tx = piece_tx;
                    peer.link = PeerLink { encrypted, utp };
                    peers.insert(pid, peer);
                    greet_peer(
                        peers,
                        pid,
                        piece_tracker,
                        lengths,
                        &info_hash,
                        &reserved,
                        dht_port_for(dht, private_torrent, addr),
                    );
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
                    if !peer.supports_fast {
                        release_choked_requests(pid, peer, piece_tracker, chunk_tracker, lengths);
                    }
                }
                Message::Unchoke => {
                    peer.peer_choking = false;
                    peer.choke_rejected.clear();
                    if peer.slow_start {
                        peer.window_start = Instant::now();
                        peer.delivered_since_growth = 0;
                        peer.slow_start_rate = 0.0;
                    }
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
                        send_interested_if_useful(peer, piece_tracker.is_useful(vpi));
                    }
                    kick = true;
                }
                Message::Bitfield(bytes) => {
                    if peer.supports_fast
                        && !accept_initial_availability(peer, InitialAvailability::Bitfield)
                    {
                        abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
                        return false;
                    }
                    piece_tracker.replace_peer_bitfield(&mut peer.bitfield, &bytes);
                    send_interested_if_useful(peer, piece_tracker.any_useful(&peer.bitfield));
                    kick = true;
                }
                Message::HaveAll => {
                    if !peer.supports_fast {
                        abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
                        return false;
                    }
                    if !accept_initial_availability(peer, InitialAvailability::HaveAll) {
                        abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
                        return false;
                    }
                    let all = vec![0xff; peer.bitfield.len()];
                    piece_tracker.replace_peer_bitfield(&mut peer.bitfield, &all);
                    send_interested_if_useful(peer, piece_tracker.any_useful(&peer.bitfield));
                    kick = true;
                }
                Message::HaveNone => {
                    if !peer.supports_fast {
                        abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
                        return false;
                    }
                    if !accept_initial_availability(peer, InitialAvailability::HaveNone) {
                        abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
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
                        abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
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
                            if chunk_tracker.has_missing(vpi) {
                                piece_tracker.clear_in_flight(vpi);
                            }
                        }
                        if peer.peer_choking {
                            if peer.choke_rejected.len() < MAX_CHOKE_REJECTED {
                                peer.choke_rejected.insert((index, begin));
                            }
                        } else {
                            peer.consecutive_rejects = peer.consecutive_rejects.saturating_add(1);
                            peer.shrink_window(pipeline_floor, pipeline_cap);
                        }
                        if peer.consecutive_rejects >= REJECT_SNUB_THRESHOLD && !peer.snubbing {
                            peer.mark_snubbed();
                        } else if !peer.snubbing && !peer.peer_choking {
                            kick = true;
                        }
                    } else if !peer.canceled_requests.remove(&(index, begin, length)) {
                        abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
                        return false;
                    }
                }
                Message::SuggestPiece(index) => {
                    if !peer.supports_fast {
                        abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
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
                        abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
                        return false;
                    }
                    if lengths
                        .validate_piece(index)
                        .is_ok_and(|vpi| piece_tracker.is_useful(vpi))
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
                    let Ok(vpi) = lengths.validate_piece(index) else {
                        send_fast_reject(peer, index, begin, length);
                        return false;
                    };
                    let fast_allowed =
                        peer.supports_fast && peer.sent_allowed_fast.contains(&index);
                    if !piece_tracker.has_local(vpi) || (peer.am_choking && !fast_allowed) {
                        send_fast_reject(peer, index, begin, length);
                        return false;
                    }
                    if length == 0 || length > MAX_REQUEST_LEN {
                        send_fast_reject(peer, index, begin, length);
                        return false;
                    }
                    let piece_len = lengths.piece_length_of(vpi) as u64;
                    if (begin as u64).saturating_add(length as u64) > piece_len {
                        send_fast_reject(peer, index, begin, length);
                        return false;
                    }
                    {
                        let mut pending = peer.pending_uploads.lock();
                        if pending.contains(&request_key) {
                            return false;
                        }
                        if pending.bytes + u64::from(length) > MAX_PENDING_UPLOAD_BYTES {
                            drop(pending);
                            send_fast_reject(peer, index, begin, length);
                            return false;
                        }
                        pending.insert(request_key);
                    }
                    let piece_off = lengths.piece_offset(vpi);
                    let offset = piece_off + begin as u64;
                    let extent = crate::read_cache::extent_for(
                        piece_off,
                        piece_len,
                        begin as u64,
                        length as u64,
                    );
                    let pending = Arc::clone(&peer.pending_uploads);
                    let storage = storage.clone();
                    let lane = UploadLane::new(&peer.cmd_tx, peer.piece_tx.as_ref());
                    let stats = stats.clone();
                    let upload_tick = Arc::clone(upload_tick);
                    let peer_uploaded = Arc::clone(&peer.uploaded_window);
                    let peer_uploaded_total = Arc::clone(&peer.uploaded_total);
                    let limiter = upload_limiter.clone();
                    let supports_fast = peer.supports_fast;
                    tokio::spawn(serve_upload(UploadJob {
                        pending,
                        storage,
                        lane,
                        stats,
                        upload_tick,
                        peer_uploaded,
                        peer_uploaded_total,
                        limiter,
                        supports_fast,
                        key: request_key,
                        offset,
                        extent,
                    }));
                }
                Message::Piece { index, begin, data } => {
                    let Ok(vpi) = lengths.validate_piece(index) else {
                        abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
                        return false;
                    };
                    let requested = peer.outstanding.iter().position(|&(p, b, l)| {
                        p == index && b == begin && l as usize == data.len()
                    });
                    let Some(req_idx) = requested else {
                        let late_key =
                            (index, begin, u32::try_from(data.len()).unwrap_or(u32::MAX));
                        if peer.canceled_requests.remove(&late_key) {
                            peer.snubbing = false;
                            peer.snub_since = None;
                            return false;
                        }
                        abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
                        return false;
                    };
                    let piece_len = lengths.piece_length_of(vpi);
                    if (begin as u64).saturating_add(data.len() as u64) > piece_len as u64 {
                        abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
                        return false;
                    }
                    peer.outstanding.swap_remove(req_idx);
                    peer.snubbing = false;
                    peer.snub_since = None;
                    peer.consecutive_rejects = 0;
                    peer.choke_rejected.clear();
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
                            let rate = slow_start_rate(peer.delivered_since_growth, window);
                            let plateau = slow_start_plateaued(peer.slow_start_rate, rate);
                            tracing::debug!(
                                target: "diag",
                                "pipeline SLOW_START pid={pid} addr={} {}->{target} plateau={plateau}",
                                peer.addr, peer.max_outstanding
                            );
                            peer.slow_start_rate = rate;
                            peer.delivered_since_growth = 0;
                            peer.delivered_window = 0;
                            peer.window_start = Instant::now();
                            if plateau {
                                peer.slow_start = false;
                            } else {
                                peer.max_outstanding = target;
                                if finished {
                                    peer.slow_start = false;
                                }
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

                    let source_ip = peer.addr.ip();
                    let assembly = piece_assemblies
                        .entry(index)
                        .or_insert_with(|| PieceAssembly {
                            buf: vec![0u8; piece_len as usize],
                            sources: vec![
                                None;
                                piece_len.div_ceil(super::core::CHUNK_SIZE) as usize
                            ],
                            completed: false,
                        });
                    let chunk_index = begin / super::core::CHUNK_SIZE;
                    let begin_usz = begin as usize;
                    let end_usz = begin_usz + data.len();
                    let chunk_len = data.len();
                    let accepted = !assembly.completed
                        && end_usz <= assembly.buf.len()
                        && matches!(assembly.sources.get(chunk_index as usize), Some(None));
                    if accepted {
                        assembly.sources[chunk_index as usize] = Some(source_ip);
                        assembly.buf[begin_usz..end_usz].copy_from_slice(&data);
                        bytes_this_tick.0 += chunk_len as u64;
                    }
                    if accepted && chunk_tracker.endgame() {
                        cancel_outstanding(peers, pid, index, begin, chunk_len as u32);
                    }
                    let cinfo = super::core::ChunkInfo {
                        piece_index: vpi,
                        chunk_index: begin / super::core::CHUNK_SIZE,
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
                        if assembly.sources.iter().any(Option::is_none) {
                            piece_assemblies.remove(&index);
                            chunk_tracker.reset_piece(vpi);
                            piece_tracker.clear_in_flight(vpi);
                            return kick;
                        }
                        assembly.completed = true;
                        let mut contributors: Vec<IpAddr> =
                            assembly.sources.iter().flatten().copied().collect();
                        contributors.sort_unstable();
                        contributors.dedup();
                        let sources = if failed_copies.is_some() {
                            assembly.sources.clone()
                        } else {
                            Vec::new()
                        };
                        let failed_copies = failed_copies.cloned();
                        let buf = std::mem::take(&mut assembly.buf);
                        let poff = lengths.piece_offset(vpi);
                        let storage = storage.clone();
                        let verifier = verifier.clone();
                        write_tasks.spawn(u64::from(piece_len), async move {
                            let buf: bytes::Bytes = buf.into();
                            let to_check = buf.clone();
                            let prior = failed_copies
                                .as_ref()
                                .and_then(|copies| copies.lock().get(index));
                            let multi_source = contributors.len() > 1;
                            let (verify_ok, digests) = tokio::task::spawn_blocking(move || {
                                let ok = verifier.verify(index, &to_check).is_ok();
                                let wanted = if ok { prior.is_some() } else { multi_source };
                                let digests = (wanted && !sources.is_empty())
                                    .then(|| chunk_digests(&to_check, &sources));
                                (ok, digests.map(|d| (d, prior)))
                            })
                            .await
                            .unwrap_or((false, None));
                            let mut failed_copy = None;
                            let mut culprits = Vec::new();
                            if let Some((digests, prior)) = digests {
                                match (verify_ok, prior) {
                                    (true, Some(prior)) => {
                                        culprits = differing_sources(&prior, &digests);
                                    }
                                    (false, _) => failed_copy = Some(Arc::new(digests)),
                                    _ => {}
                                }
                            }
                            let write_error = if verify_ok {
                                storage
                                    .write_at_owned(poff, buf)
                                    .await
                                    .err()
                                    .map(|e| e.to_string())
                            } else {
                                None
                            };
                            VerifyResult {
                                piece_index: index,
                                write_error,
                                verify_ok,
                                from_webseed: false,
                                contributors,
                                failed_copy,
                                culprits,
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
                    if base_layer == 0 {
                        let request = LeafHashRequest {
                            pieces_root,
                            index,
                            length,
                            proof_layers,
                        };
                        serve_leaf_hash_request(
                            request,
                            hash_tables,
                            leaf_hash_jobs,
                            storage,
                            piece_tracker,
                            lengths,
                            peer.cmd_tx.clone(),
                        );
                    } else {
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
                }
                Message::Hashes { .. } | Message::HashReject { .. } => {}
                Message::Extended { ext_id, payload } => {
                    if ext_id == EXT_HANDSHAKE_ID {
                        if let Some(peer_ext) = ExtHandshake::decode(&payload) {
                            peer.their_ut_metadata_id = peer_ext.ut_metadata_id();
                            peer.their_ut_holepunch_id = peer_ext.ut_holepunch_id();
                            peer.their_ut_pex_id = peer_ext.ut_pex_id();
                            peer.prefers_encryption = peer_ext.prefers_encryption;
                            peer.upload_only = peer_ext.upload_only;
                            peer.ext_seen = true;
                            if let (false, Some(port)) = (peer.outbound, peer_ext.port) {
                                if port != 0 {
                                    peer.listen_addr = Some(SocketAddr::new(peer.addr.ip(), port));
                                }
                            }
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
                                peer.slow_start = peer.max_outstanding < reqq_cap;
                            }
                        }
                    } else if ext_id == OUR_UT_METADATA_ID {
                        serve_ut_metadata(peer, &payload, info_bytes);
                    } else if ext_id == OUR_UT_PEX_ID && !private_torrent {
                        let now = Instant::now();
                        let flooding = peer
                            .last_pex_in
                            .is_some_and(|last| now.duration_since(last) < PEX_MIN_INBOUND_GAP);
                        let update = if flooding {
                            None
                        } else {
                            peer.last_pex_in = Some(now);
                            super::wire::extended::parse_ut_pex_full(&payload)
                        };
                        if let Some(update) = update {
                            for addr in update.dropped {
                                pex_source.remove(&addr);
                            }
                            let complete = piece_tracker.is_complete();
                            for (addr, flags) in update.added {
                                if complete && flags & pex_flag::SEED != 0 {
                                    continue;
                                }
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
                                    mse_first: false,
                                });
                            }
                        }
                    } else if ext_id == OUR_UT_HOLEPUNCH_ID && !private_torrent {
                        let from_addr = peer.listen_addr.unwrap_or(peer.addr);
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
                Message::Cancel {
                    index,
                    begin,
                    length,
                } => {
                    let dropped = peer.pending_uploads.lock().remove(&(index, begin, length));
                    if dropped && peer.supports_fast {
                        send_owed_reject(&peer.cmd_tx, index, begin, length);
                    }
                }
                Message::Port(port) if !private_torrent => {
                    if let (Some(dht), true) = (dht, port != 0) {
                        dht.add_node(SocketAddr::new(peer.addr.ip(), port));
                    }
                }
                _ => {}
            }
        }
        PeerEvent::Disconnected { reason } => {
            let pending_addr = pending_dials.remove(&pid);
            let was_pending_dial = pending_addr.is_some();
            let fd_backpressure =
                was_pending_dial && reason == super::conn_budget::FD_EXHAUSTED_REASON;
            let dead = peers.remove(&pid);
            let addr = pending_addr.or_else(|| dead.as_ref().map(|peer| peer.addr));
            let useful_bytes = dead.as_ref().map_or(0, |peer| peer.downloaded_total);
            let useful_window = dead
                .as_ref()
                .map_or(pipeline_floor, |peer| peer.max_outstanding);

            if let Some(a) = addr {
                tracing::debug!("peer {a} disconnected: {reason}");
                if dead.is_some() {
                    pex_source.retain(|_, relay| *relay != pid);
                }
                if was_pending_dial && !fd_backpressure && !private_torrent {
                    try_initiate_holepunch(a, pex_source, holepunch_attempted, peers);
                }
            }
            let dial_addr =
                pending_addr.or_else(|| dead.as_ref().and_then(|peer| peer.listen_addr));
            if let Some(a) = dial_addr {
                let unproductive = dead.as_ref().is_some_and(|peer| {
                    peer.downloaded_total == 0
                        && peer.uploaded_total.load(Ordering::Relaxed) == 0
                        && peer.connected_at.elapsed() < UNPRODUCTIVE_PEER_LIFETIME
                });
                let failures = settle_dial_failures(
                    dial_failures,
                    a,
                    fd_backpressure,
                    was_pending_dial || unproductive,
                );
                let allowed = !private_torrent || tracker_authorized.contains(&a);
                if useful_bytes > 0 && allowed {
                    remember_useful_peer(
                        useful_peers,
                        a,
                        useful_window.clamp(pipeline_floor, pipeline_cap),
                    );
                }
                let known_useful = useful_bytes > 0 || useful_peers.contains_key(&a);
                if allowed && (known_useful || failures < MAX_DIAL_FAILURES) {
                    schedule_peer_retry(a, known_useful, failures, dial_retries, useful_redials);
                    if known_useful {
                        tracing::debug!(
                            "scheduled useful peer {a} redial (delivered {useful_bytes} bytes, {failures} failed dials)"
                        );
                    }
                } else {
                    if failures >= MAX_DIAL_FAILURES {
                        dial_failures.remove(&a);
                        useful_peers.remove(&a);
                    }
                    known_addrs.remove(&a);
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
    corruption: &mut CorruptionTracker,
    blocklist: &RwLock<BlockList>,
) {
    let Ok(vpi) = lengths.validate_piece(vr.piece_index) else {
        return;
    };
    piece_tracker.clear_in_flight(vpi);
    let ban = corruption.record_result(&vr);
    if !ban.is_empty() {
        ban_peers(&ban, blocklist, peers);
        tracing::warn!(
            "piece {} hash check: banned {} peer(s) that sent corrupt data",
            vr.piece_index,
            ban.len()
        );
    }
    if vr.from_webseed && (vr.write_error.is_some() || !vr.verify_ok) {
        return;
    }
    piece_assemblies.remove(&vr.piece_index);
    if vr.write_error.is_some() {
        tracing::warn!(
            "piece {} disk write failed; will re-request",
            vr.piece_index
        );
        cancel_piece_outstanding(peers, vr.piece_index);
        chunk_tracker.reset_piece(vpi);
        reinterest_holders(peers, vr.piece_index);
        return;
    }
    if vr.verify_ok {
        let became_local = mark_verified_piece_local(piece_tracker, vpi);
        chunk_tracker.forget_piece(vpi);
        cancel_piece_outstanding(peers, vr.piece_index);
        broadcast_have(peers, vr.piece_index);
        for peer in peers.values_mut() {
            peer.remote_allowed_fast.remove(&vr.piece_index);
            peer.suggested_pieces.remove(&vr.piece_index);
        }
        if became_local && piece_tracker.is_complete() {
            for peer in peers.values_mut() {
                refresh_interest(peer, piece_tracker);
            }
        }
        let mut s = stats.lock();
        if became_local {
            add_piece_progress(&mut s, lengths, storage.layout(), vpi);
        }
        s.left_bytes = piece_tracker.bytes_left();
        s.finished = piece_tracker.is_complete();
    } else {
        tracing::debug!("piece {} verify failed", vr.piece_index);
        cancel_piece_outstanding(peers, vr.piece_index);
        chunk_tracker.reset_piece(vpi);
        reinterest_holders(peers, vr.piece_index);
    }
}

fn ban_peers(ban: &[IpAddr], blocklist: &RwLock<BlockList>, peers: &HashMap<u32, Peer>) {
    {
        let mut list = blocklist.write();
        for ip in ban {
            list.ban(*ip);
        }
    }
    for peer in peers.values() {
        if ban.contains(&peer.addr.ip()) {
            abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
        }
    }
}

fn build_webseed_client(
    proxy: Option<&risuko_http::ProxyConnector>,
) -> Result<risuko_http::Client, String> {
    let builder = risuko_http::Client::builder()
        .timeout(WEBSEED_TIMEOUT)
        .connect_timeout(Duration::from_secs(8))
        .redirect(risuko_http::Policy::limited(3))
        .pool_max_idle_per_host(WEBSEED_MAX_WORKERS)
        .http1_only(true);
    #[cfg(not(test))]
    let builder = builder.direct_address_filter(super::webseed::is_allowed_destination);
    let mut builder = builder.gzip(false).brotli(false).deflate(false);
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
                write_error: None,
                verify_ok: false,
                from_webseed: true,
                contributors: Vec::new(),
                failed_copy: None,
                culprits: Vec::new(),
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
    let mirrors = ctx.bases.len();
    let first_mirror = first_index as usize % mirrors.max(1);
    for mirror_idx in (0..mirrors).map(|k| (first_mirror + k) % mirrors) {
        if !ctx.mirror_usable(mirror_idx) {
            continue;
        }
        let missing_file = {
            let disabled = ctx.disabled_files.lock();
            spans.iter().any(|(file, _, _)| {
                !ctx.layout.files()[*file].padding && disabled.contains(&(mirror_idx, *file))
            })
        };
        if missing_file {
            continue;
        }
        let mut batch = BytesMut::zeroed(batch_len as usize);
        let mut cursor = 0usize;
        let mut mirror_ok = true;
        for (file_idx, file_offset, span_len) in &spans {
            let file = &ctx.layout.files()[*file_idx];
            if file.padding {
                cursor = cursor.saturating_add(*span_len as usize);
                continue;
            }
            let url = match super::webseed::build_file_url(
                &ctx.bases[mirror_idx],
                &ctx.info.name,
                &ctx.info.files[*file_idx].path,
                ctx.info.single_file_mode,
            ) {
                Ok(url) => url,
                Err(_) => {
                    ctx.disable(mirror_idx);
                    mirror_ok = false;
                    break;
                }
            };
            let mut span_cursor = 0u64;
            while span_cursor < *span_len {
                let request_len = (*span_len - span_cursor).min(WEBSEED_MAX_RANGE_BYTES);
                let Some(start) = file_offset.checked_add(span_cursor) else {
                    ctx.disable(mirror_idx);
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
                        ctx.disable(mirror_idx);
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
                                ctx.disable(mirror_idx);
                                mirror_ok = false;
                                break;
                            }
                            fetched = Some(response.body);
                            break;
                        }
                        Err(super::webseed::WebSeedError::UnexpectedStatus(404 | 410 | 416)) => {
                            ctx.disable_file(mirror_idx, *file_idx);
                            mirror_ok = false;
                            break;
                        }
                        Err(super::webseed::WebSeedError::UnexpectedStatus(status))
                            if super::webseed::is_retryable_status(status) =>
                        {
                            if attempt == 2 {
                                ctx.back_off(mirror_idx);
                                mirror_ok = false;
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(100 * (attempt + 1) as u64))
                                .await;
                        }
                        Err(super::webseed::WebSeedError::Http(_)) => {
                            if attempt == 2 {
                                ctx.back_off(mirror_idx);
                                mirror_ok = false;
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(100 * (attempt + 1) as u64))
                                .await;
                        }
                        Err(_) => {
                            ctx.disable(mirror_idx);
                            mirror_ok = false;
                            break;
                        }
                    }
                }
                let Some(bytes) = fetched else {
                    break;
                };
                ctx.throttle.acquire(bytes.len()).await;
                let end_cursor = cursor.saturating_add(bytes.len());
                if end_cursor > batch.len() || bytes.len() as u64 != request_len {
                    ctx.disable(mirror_idx);
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
        ctx.mirror_succeeded(mirror_idx);

        let batch = batch.freeze();
        let mut pieces = Vec::with_capacity(piece_indices.len());
        let mut piece_cursor = 0usize;
        for &piece_index in &piece_indices {
            let Ok(vpi) = ctx.lengths.validate_piece(piece_index) else {
                break;
            };
            let piece_len = ctx.lengths.piece_length_of(vpi) as usize;
            let end = piece_cursor.saturating_add(piece_len);
            if end > batch.len() {
                break;
            }
            let piece = batch.slice(piece_cursor..end);
            let to_check = piece.clone();
            let verifier = ctx.verifier.clone();
            let verified = tokio::task::spawn_blocking(move || {
                verifier.verify(piece_index, &to_check).is_ok()
            })
            .await
            .unwrap_or(false);
            if !verified {
                ctx.disable(mirror_idx);
                break;
            }
            let write_error = ctx
                .storage
                .write_at_owned(ctx.lengths.piece_offset(vpi), piece)
                .await
                .err()
                .map(|e| e.to_string());
            let failed = write_error.is_some();
            pieces.push(VerifyResult {
                piece_index,
                verify_ok: !failed,
                write_error,
                from_webseed: true,
                contributors: Vec::new(),
                failed_copy: None,
                culprits: Vec::new(),
            });
            if failed {
                break;
            }
            piece_cursor = end;
        }
        if pieces.is_empty() {
            continue;
        }
        let mut rest = webseed_failed_batch(generation, &piece_indices[pieces.len()..]).pieces;
        pieces.append(&mut rest);
        return WebSeedBatchResult { generation, pieces };
    }
    webseed_failed_batch(generation, &piece_indices)
}

fn schedule_webseed_workers(
    tasks: &mut tokio::task::JoinSet<WebSeedBatchResult>,
    inflight: &mut HashSet<u32>,
    piece_tracker: &mut PieceTracker,
    chunk_tracker: &ChunkTracker,
    ctx: &WebSeedContext,
    halted: bool,
) {
    if halted || tasks.len() >= WEBSEED_MAX_WORKERS || !ctx.has_available_mirror() {
        return;
    }
    let candidates: Vec<u32> = piece_tracker
        .choose_missing_pieces()
        .into_iter()
        .map(|piece| piece.get())
        .filter(|piece| {
            !inflight.contains(piece)
                && !chunk_tracker.has_live_requests(*piece)
                && ctx.pieces_servable(&[*piece])
        })
        .collect();
    let per_job = webseed_pieces_per_job(ctx.lengths.piece_length());
    for batch in contiguous_webseed_batches(&candidates, per_job) {
        if tasks.len() >= WEBSEED_MAX_WORKERS {
            break;
        }
        let batch = if ctx.pieces_servable(&batch) {
            batch
        } else {
            batch.into_iter().take(1).collect()
        };
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

fn webseed_pieces_per_job(piece_length: u32) -> usize {
    ((WEBSEED_MAX_RANGE_BYTES / u64::from(piece_length).max(1)) as usize)
        .clamp(1, WEBSEED_MAX_PIECES_PER_JOB)
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
            if holepunch_targets.len() >= MAX_HOLEPUNCH_ENTRIES
                && !holepunch_targets.contains(&hp.addr)
            {
                return;
            }
            if blocklist.read().contains(hp.addr.ip()) {
                return;
            }
            let active_addrs = peers
                .values()
                .flat_map(|peer| [Some(peer.addr), peer.listen_addr])
                .flatten()
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
            let Some(from_hp_id) = from_hp_id else {
                return;
            };
            enum Relay {
                Connect(mpsc::Sender<PeerCommand>, u8),
                Err(u32),
            }
            let action = if hp.addr == from_addr {
                Relay::Err(holepunch_err::NO_SELF)
            } else if !super::magnet::is_dialable_peer_addr(hp.addr) {
                Relay::Err(holepunch_err::NO_SUCH_PEER)
            } else {
                match peers
                    .values()
                    .find(|p| p.addr == hp.addr || p.listen_addr == Some(hp.addr))
                {
                    Some(t) => match t.their_ut_holepunch_id {
                        Some(thp) => Relay::Connect(t.cmd_tx.clone(), thp),
                        None => Relay::Err(holepunch_err::NO_SUPPORT),
                    },
                    None => Relay::Err(holepunch_err::NOT_CONNECTED),
                }
            };
            match action {
                Relay::Connect(target_cmd, target_hp) => {
                    let _ = target_cmd.try_send(PeerCommand::Send(Message::Extended {
                        ext_id: target_hp,
                        payload: build_holepunch(holepunch_type::CONNECT, from_addr, 0),
                    }));
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
        .flat_map(|peer| [Some(peer.addr), peer.listen_addr])
        .flatten()
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

fn bump_dial_failures(dial_failures: &mut HashMap<SocketAddr, u8>, addr: SocketAddr) -> u8 {
    if dial_failures.len() >= MAX_PEER_BACKLOG && !dial_failures.contains_key(&addr) {
        dial_failures.clear();
    }
    let failures = dial_failures.entry(addr).or_insert(0);
    *failures = failures.saturating_add(1);
    *failures
}

fn settle_dial_failures(
    dial_failures: &mut HashMap<SocketAddr, u8>,
    addr: SocketAddr,
    fd_backpressure: bool,
    failed: bool,
) -> u8 {
    if fd_backpressure {
        0
    } else if failed {
        bump_dial_failures(dial_failures, addr)
    } else {
        dial_failures.remove(&addr);
        0
    }
}

fn remember_useful_peer(
    useful_peers: &mut HashMap<SocketAddr, usize>,
    addr: SocketAddr,
    window: usize,
) {
    if useful_peers.len() >= MAX_USEFUL_PEERS && !useful_peers.contains_key(&addr) {
        if let Some(victim) = useful_peers.keys().next().copied() {
            useful_peers.remove(&victim);
        }
    }
    useful_peers.insert(addr, window);
}

fn schedule_peer_retry(
    addr: SocketAddr,
    useful: bool,
    failures: u8,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
) {
    let backoff = 3u32.pow(u32::from(failures.min(MAX_DIAL_FAILURES)));
    let insert = |queue: &mut VecDeque<(SocketAddr, Instant)>, delay: Duration| {
        if queue.len() < MAX_DIAL_RETRIES {
            let due = Instant::now() + delay;
            let at = queue.partition_point(|(_, queued)| *queued <= due);
            queue.insert(at, (addr, due));
        }
    };
    if useful {
        insert(useful_redials, USEFUL_PEER_REDIAL_DELAY * backoff);
    } else {
        insert(dial_retries, DIAL_RETRY_DELAY * backoff);
    }
}

fn dial_slot_available(
    live_peers: usize,
    pending_dials: usize,
    max_peers: usize,
    pending_limit: usize,
    session_has_room: bool,
) -> bool {
    session_has_room
        && live_peers.saturating_add(pending_dials) < max_peers
        && pending_dials < pending_limit
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
    mse_first: &HashSet<SocketAddr>,
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
    recv_throttle: &Throttle,
    outbound_tasks: &mut tokio::task::JoinSet<()>,
    outbound_aborts: &mut HashMap<u32, AbortHandle>,
    blocklist: &RwLock<BlockList>,
    conn_lease: &mut super::conn_budget::BudgetLease,
) {
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
            dial_policy(encryption, mse_first.contains(&addr)),
            advertise_v2,
            advertise_dht,
            Some(ext_handshake_builder.clone()),
            utp.clone(),
            prefer_utp,
            known_useful,
            proxy.clone(),
            recv_throttle.clone(),
        ));
        outbound_aborts.insert(pid, abort);
    };

    while dial_slot_available(
        live_peers,
        pending_dials.len(),
        max_peers,
        MAX_PENDING_DIALS,
        !conn_lease.is_full(),
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
        if !conn_lease.try_reserve() {
            priority_backlog.push_front(addr);
            break;
        }
        spawn_one(
            addr,
            pending_dials,
            next_pid,
            outbound_tasks,
            outbound_aborts,
        );
    }

    let cold_cap = if live_peers < 8 {
        MAX_PENDING_DIALS.saturating_sub(PRIORITY_DIAL_RESERVE)
    } else {
        MAX_PENDING_DIALS
    };
    while dial_slot_available(
        live_peers,
        pending_dials.len(),
        max_peers,
        cold_cap,
        !conn_lease.is_full(),
    ) {
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
        if !conn_lease.try_reserve() {
            peer_backlog.push_front(addr);
            break;
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

fn try_initiate_holepunch(
    target: SocketAddr,
    pex_source: &HashMap<SocketAddr, u32>,
    holepunch_attempted: &mut HashSet<SocketAddr>,
    peers: &HashMap<u32, Peer>,
) {
    if holepunch_attempted.contains(&target) {
        return;
    }
    let Some(&relay_pid) = pex_source.get(&target) else {
        return;
    };
    let Some(relay) = peers.get(&relay_pid) else {
        return;
    };
    let Some(relay_hp) = relay.their_ut_holepunch_id else {
        return;
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

fn send_interested_if_useful(peer: &mut Peer, useful: bool) {
    if tracing::enabled!(target: "diag", tracing::Level::DEBUG) {
        let set_bits: u32 = peer.bitfield.iter().map(|x| x.count_ones()).sum();
        tracing::debug!(
            target: "diag",
            "send_interested_if_useful {} am_interested={} useful={} peer_bitfield_set={}",
            peer.addr, peer.am_interested, useful, set_bits
        );
    }
    if !peer.am_interested
        && useful
        && peer
            .cmd_tx
            .try_send(PeerCommand::Send(Message::Interested))
            .is_ok()
    {
        peer.am_interested = true;
    }
}

fn reinterest_holders(peers: &mut HashMap<u32, Peer>, piece_index: u32) {
    for peer in peers.values_mut() {
        if bitfield_has(&peer.bitfield, piece_index) {
            send_interested_if_useful(peer, true);
        }
    }
}

fn refresh_peer_needs(peer: &mut Peer, piece_tracker: &PieceTracker, lengths: &Lengths) {
    refresh_interest(peer, piece_tracker);
    peer.remote_allowed_fast.retain(|&index| {
        lengths
            .validate_piece(index)
            .is_ok_and(|vpi| piece_tracker.is_useful(vpi))
    });
}

fn refresh_interest(peer: &mut Peer, piece_tracker: &PieceTracker) {
    let useful = piece_tracker.any_useful(&peer.bitfield);
    if useful == peer.am_interested {
        return;
    }
    let message = if useful {
        Message::Interested
    } else {
        Message::NotInterested
    };
    if peer.cmd_tx.try_send(PeerCommand::Send(message)).is_ok() {
        peer.am_interested = useful;
    }
}

fn broadcast_have(peers: &mut HashMap<u32, Peer>, piece_index: u32) {
    for p in peers.values_mut() {
        if bitfield_has(&p.bitfield, piece_index) {
            continue;
        }
        if !p.pending_haves.is_empty()
            || p.cmd_tx
                .try_send(PeerCommand::Send(Message::Have { piece_index }))
                .is_err()
        {
            p.pending_haves.push(piece_index);
        }
    }
}

fn flush_pending_haves(peer: &mut Peer) {
    let mut done = 0;
    for &piece_index in &peer.pending_haves {
        if !bitfield_has(&peer.bitfield, piece_index)
            && peer
                .cmd_tx
                .try_send(PeerCommand::Send(Message::Have { piece_index }))
                .is_err()
        {
            break;
        }
        done += 1;
    }
    peer.pending_haves.drain(..done);
}

fn request_peer_eligible(peer: &Peer) -> bool {
    (!peer.peer_choking || !peer.remote_allowed_fast.is_empty())
        && peer.am_interested
        && (!peer.snubbing || peer.outstanding.is_empty())
}

fn effective_request_limit(adaptive_limit: usize, demand: usize) -> usize {
    if demand <= TORRENT_REQUEST_BUDGET {
        return adaptive_limit;
    }
    (adaptive_limit * TORRENT_REQUEST_BUDGET / demand).max(1)
}

struct RequestLoad {
    demand: usize,
    outstanding: usize,
}

impl RequestLoad {
    fn measure(peers: &HashMap<u32, Peer>) -> Self {
        let mut load = Self {
            demand: 0,
            outstanding: 0,
        };
        for peer in peers.values() {
            if request_peer_eligible(peer) {
                load.demand += if peer.snubbing {
                    1
                } else {
                    peer.max_outstanding
                };
            }
            load.outstanding += peer.outstanding.len();
        }
        load
    }
}

fn drive_requests(
    peers: &mut HashMap<u32, Peer>,
    piece_tracker: &mut PieceTracker,
    chunk_tracker: &mut ChunkTracker,
    webseed_inflight: &HashSet<u32>,
) {
    let mut load = RequestLoad::measure(peers);
    let pids: Vec<u32> = peers.keys().copied().collect();
    for pid in pids {
        drive_peer_with(
            pid,
            peers,
            piece_tracker,
            chunk_tracker,
            webseed_inflight,
            &mut load,
        );
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
            finished: target >= cap,
        });
    }
    (window >= PIPELINE_RATE_WINDOW).then(|| PipelineAdjustment::Rate {
        target: pipeline_target(delivered_chunks, window, floor, cap, current),
    })
}

fn slow_start_rate(delivered_chunks: usize, window: Duration) -> f32 {
    delivered_chunks as f32 / window.as_secs_f32().max(0.001)
}

fn slow_start_plateaued(previous_rate: f32, rate: f32) -> bool {
    previous_rate > 0.0 && rate < previous_rate * PIPELINE_SLOW_START_MIN_GAIN
}

fn pipeline_slow_start_target(
    delivered_since_growth: usize,
    current: usize,
    cap: usize,
) -> Option<usize> {
    if current >= cap || delivered_since_growth < current {
        return None;
    }
    let target = current.saturating_mul(2).min(cap);
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
    let rate = delivered_chunks as f32 / secs;
    let raw = ((rate * PIPELINE_TARGET_SECS) as usize).clamp(floor, cap);
    if raw > current {
        current
            .saturating_add(current.max(PIPELINE_ADDITIVE_GROWTH))
            .min(raw)
            .min(cap)
            .max(floor)
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

fn adaptive_slots(sorted_rates: &[u64]) -> usize {
    let step = SLOT_RATE_STEP_BPS * CHOKE_EVAL_INTERVAL.as_secs();
    let mut threshold = step;
    let mut slots = 0usize;
    for &rate in sorted_rates {
        if rate < threshold {
            break;
        }
        threshold = threshold.saturating_add(step);
        slots += 1;
    }
    slots.clamp(MIN_UPLOAD_SLOTS, MAX_UPLOAD_SLOTS)
}

fn slots_for(candidates: &[(u32, u64)]) -> usize {
    let mut rates: Vec<u64> = candidates.iter().map(|&(_, r)| r).collect();
    rates.sort_unstable_by(|a, b| b.cmp(a));
    adaptive_slots(&rates)
}

fn select_unchoked(
    mut candidates: Vec<(u32, u64)>,
    optimistic: Option<u32>,
    slots: usize,
) -> HashSet<u32> {
    let mut selected: HashSet<u32> = HashSet::new();
    if let Some(pid) = optimistic.filter(|pid| candidates.iter().any(|&(c, _)| c == *pid)) {
        selected.insert(pid);
        candidates.retain(|&(c, _)| c != pid);
    }
    candidates.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    selected.extend(candidates.into_iter().take(slots).map(|(pid, _)| pid));
    selected
}

fn choke_rate(peer: &Peer, seeding: bool) -> u64 {
    if seeding {
        (peer.up_ewma + peer.uploaded_window.load(Ordering::Relaxed)) / 2
    } else if peer.snubbing {
        0
    } else {
        (peer.down_ewma + peer.downloaded_window) / 2
    }
}

fn choose_optimistic(peers: &mut HashMap<u32, Peer>, current: Option<u32>) -> Option<u32> {
    let mut interested: Vec<u32> = peers
        .iter()
        .filter(|(_, p)| p.peer_interested)
        .map(|(&pid, _)| pid)
        .collect();
    interested.sort_unstable();
    let fresh = interested
        .iter()
        .rev()
        .copied()
        .find(|pid| peers.get(pid).is_some_and(|p| !p.optimistic_tried));
    let pick = fresh.or_else(|| next_optimistic(&interested, current));
    if let Some(peer) = pick.and_then(|pid| peers.get_mut(&pid)) {
        peer.optimistic_tried = true;
    }
    pick
}

fn set_choked(peer: &mut Peer, choke: bool) {
    let message = if choke {
        Message::Choke
    } else {
        Message::Unchoke
    };
    if peer.am_choking != choke && peer.cmd_tx.try_send(PeerCommand::Send(message)).is_ok() {
        peer.am_choking = choke;
        if choke {
            drop_choked_uploads(peer);
        }
    }
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
        .map(|(&pid, p)| (pid, choke_rate(p, seeding)))
        .collect();
    let slots = slots_for(&candidates);
    let unchoke = select_unchoked(candidates, optimistic, slots);
    for (&pid, p) in peers.iter_mut() {
        set_choked(p, !unchoke.contains(&pid));
        p.optimistic_unchoke = !p.am_choking && optimistic == Some(pid);
        if reset_windows {
            p.down_ewma = (p.down_ewma + p.downloaded_window) / 2;
            p.up_ewma = (p.up_ewma + p.uploaded_window.load(Ordering::Relaxed)) / 2;
            p.downloaded_window = 0;
            p.uploaded_window.store(0, Ordering::Relaxed);
        }
    }
}

fn fill_choke_slots(peers: &mut HashMap<u32, Peer>, seeding: bool, optimistic: Option<u32>) {
    let ranked: Vec<(u32, u64)> = peers
        .iter()
        .filter(|(_, p)| p.peer_interested)
        .map(|(&pid, p)| (pid, choke_rate(p, seeding)))
        .collect();
    let slots = slots_for(&ranked);
    let used = peers
        .iter()
        .filter(|(&pid, p)| !p.am_choking && p.peer_interested && optimistic != Some(pid))
        .count();
    let mut free = slots.saturating_sub(used);
    let mut waiting: Vec<(u32, u64)> = ranked
        .into_iter()
        .filter(|(pid, _)| peers.get(pid).is_some_and(|p| p.am_choking))
        .collect();
    waiting.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    if let Some(pos) = waiting.iter().position(|&(pid, _)| optimistic == Some(pid)) {
        let (pid, _) = waiting.remove(pos);
        if let Some(p) = peers.get_mut(&pid) {
            set_choked(p, false);
            p.optimistic_unchoke = !p.am_choking;
        }
    }
    for (pid, _) in waiting {
        if free == 0 {
            break;
        }
        if let Some(p) = peers.get_mut(&pid) {
            set_choked(p, false);
            if !p.am_choking {
                p.optimistic_unchoke = false;
                free -= 1;
            }
        }
    }
}

fn redundant_peers(
    peers: &HashMap<u32, Peer>,
    complete: bool,
    now: Instant,
    total_pieces: usize,
    limit: usize,
) -> Vec<u32> {
    let mut found: Vec<(Instant, u32)> = peers
        .iter()
        .filter(|(_, p)| {
            if !p.outstanding.is_empty() || p.peer_interested {
                return false;
            }
            if complete {
                return p.upload_only || peer_bitfield_is_full(&p.bitfield, total_pieces);
            }
            now.duration_since(p.connected_at) >= PEER_USELESS_AFTER
                && p.downloaded_total == 0
                && (p.peer_choking || !p.am_interested)
        })
        .map(|(&pid, p)| (p.connected_at, pid))
        .collect();
    found.sort_unstable();
    found.into_iter().take(limit).map(|(_, pid)| pid).collect()
}

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
        other.outstanding.swap_remove(slot);
        remember_canceled_request(other, (index, begin, length));
        let _ = other.cmd_tx.try_send(PeerCommand::Send(Message::Cancel {
            index,
            begin,
            length,
        }));
    }
}

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
    peer.canceled_requests.insert(request);
}

#[derive(Default)]
struct CanceledRequests {
    keys: HashSet<(u32, u32, u32)>,
    order: VecDeque<(u32, u32, u32)>,
}

impl CanceledRequests {
    const MAX: usize = 4096;

    fn insert(&mut self, key: (u32, u32, u32)) {
        if self.keys.insert(key) {
            self.order.push_back(key);
        }
        while self.keys.len() > Self::MAX {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.keys.remove(&oldest);
        }
        if self.order.len() > 2 * Self::MAX {
            let keys = &self.keys;
            self.order.retain(|key| keys.contains(key));
        }
    }

    fn remove(&mut self, key: &(u32, u32, u32)) -> bool {
        self.keys.remove(key)
    }
}

fn drive_peer(
    pid: u32,
    peers: &mut HashMap<u32, Peer>,
    piece_tracker: &mut PieceTracker,
    chunk_tracker: &mut ChunkTracker,
    webseed_inflight: &HashSet<u32>,
) {
    let mut load = RequestLoad::measure(peers);
    drive_peer_with(
        pid,
        peers,
        piece_tracker,
        chunk_tracker,
        webseed_inflight,
        &mut load,
    );
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PickStage {
    Preferred,
    Partial,
    Rarest,
    Endgame,
    Done,
}

fn bitfield_has(bitfield: &[u8], index: u32) -> bool {
    let i = index as usize;
    bitfield
        .get(i / 8)
        .is_some_and(|b| b & (1 << (7 - (i % 8))) != 0)
}

fn drive_peer_with(
    pid: u32,
    peers: &mut HashMap<u32, Peer>,
    piece_tracker: &mut PieceTracker,
    chunk_tracker: &mut ChunkTracker,
    webseed_inflight: &HashSet<u32>,
    load: &mut RequestLoad,
) {
    let remaining_request_budget = TORRENT_REQUEST_BUDGET.saturating_sub(load.outstanding);
    let Some(peer) = peers.get_mut(&pid) else {
        return;
    };
    if !request_peer_eligible(peer) || remaining_request_budget == 0 {
        return;
    }
    let max_outstanding = if peer.snubbing {
        1
    } else {
        effective_request_limit(peer.max_outstanding, load.demand)
    };
    if peer.outstanding.len() >= max_outstanding {
        return;
    }
    let endgame = piece_tracker.requestable_remaining() == 0;
    chunk_tracker.set_endgame(endgame);
    let choked = peer.peer_choking;
    let mut request_slots = remaining_request_budget;

    let mut stage = PickStage::Preferred;
    let mut stage_list: Vec<u32> = if choked {
        peer.remote_allowed_fast.iter().copied().collect()
    } else {
        peer.last_piece
            .into_iter()
            .chain(peer.suggested_pieces.iter().copied())
            .collect()
    };
    let mut stage_idx = 0usize;
    let mut cursor = 0usize;
    let mut exhausted: HashSet<u32> = HashSet::new();
    let mut current: Option<super::core::ValidPieceIndex> = None;
    let mut nothing_to_pick = false;
    while peer.outstanding.len() < max_outstanding && request_slots > 0 {
        let piece = match current {
            Some(piece) => piece,
            None => {
                let next = loop {
                    let index = match stage {
                        PickStage::Preferred | PickStage::Partial | PickStage::Endgame => {
                            if let Some(&index) = stage_list.get(stage_idx) {
                                stage_idx += 1;
                                index
                            } else {
                                stage_idx = 0;
                                stage_list.clear();
                                stage = match stage {
                                    PickStage::Preferred if choked => PickStage::Done,
                                    PickStage::Preferred => {
                                        stage_list.extend(chunk_tracker.partial_pieces());
                                        PickStage::Partial
                                    }
                                    PickStage::Partial => PickStage::Rarest,
                                    _ => PickStage::Done,
                                };
                                continue;
                            }
                        }
                        PickStage::Rarest => {
                            match piece_tracker.next_requestable(&peer.bitfield, &mut cursor) {
                                Some(piece) => piece.get(),
                                None => {
                                    stage = if endgame {
                                        stage_list.extend(chunk_tracker.contested_pieces());
                                        stage_list.extend(webseed_inflight.iter().copied());
                                        if !stage_list.is_empty() {
                                            let len = stage_list.len();
                                            stage_list.rotate_left(pid as usize % len);
                                        }
                                        PickStage::Endgame
                                    } else {
                                        PickStage::Done
                                    };
                                    continue;
                                }
                            }
                        }
                        PickStage::Done => break None,
                    };
                    if exhausted.contains(&index) || !bitfield_has(&peer.bitfield, index) {
                        continue;
                    }
                    let Ok(piece) = piece_tracker.lengths().validate_piece(index) else {
                        continue;
                    };
                    if piece_tracker.is_requestable(piece)
                        || (endgame && piece_tracker.is_useful(piece))
                    {
                        break Some(piece);
                    }
                };
                let Some(piece) = next else {
                    nothing_to_pick = true;
                    break;
                };
                piece
            }
        };
        let outstanding = &peer.outstanding;
        let rejected = &peer.choke_rejected;
        let asked = |chunk: u32| {
            let begin = chunk * super::core::CHUNK_SIZE;
            (choked && rejected.contains(&(piece.get(), begin)))
                || outstanding
                    .iter()
                    .any(|&(index, b, _)| index == piece.get() && b == begin)
        };
        match chunk_tracker.next_chunk_skipping(piece, pid, asked) {
            Some(chunk) => {
                let info = chunk.info;
                // try_send + rollback so one full writer queue cannot block the main loop
                let req = Message::Request {
                    index: info.piece_index.get(),
                    begin: info.offset,
                    length: info.size,
                };
                if peer.cmd_tx.try_send(PeerCommand::Send(req)).is_err() {
                    chunk_tracker.unrequest_chunk(
                        info.piece_index,
                        info.chunk_index,
                        chunk.prior_state,
                    );
                    if chunk_tracker.has_missing(piece) {
                        piece_tracker.clear_in_flight(piece);
                    }
                    break;
                }
                peer.outstanding
                    .push((info.piece_index.get(), info.offset, info.size));
                peer.last_piece = Some(piece.get());
                request_slots -= 1;
                load.outstanding += 1;
                current = Some(piece);
                if !chunk_tracker.has_missing(piece) {
                    piece_tracker.mark_in_flight(piece);
                    if !endgame {
                        current = None;
                        exhausted.insert(piece.get());
                    }
                }
            }
            None => {
                current = None;
                if !chunk_tracker.has_missing(piece) {
                    piece_tracker.mark_in_flight(piece);
                }
                exhausted.insert(piece.get());
            }
        }
    }
    if nothing_to_pick
        && peer.outstanding.is_empty()
        && !piece_tracker.any_useful(&peer.bitfield)
        && peer
            .cmd_tx
            .try_send(PeerCommand::Send(Message::NotInterested))
            .is_ok()
    {
        peer.am_interested = false;
    }
}

fn release_choked_requests(
    pid: u32,
    peer: &mut Peer,
    piece_tracker: &mut PieceTracker,
    chunk_tracker: &mut ChunkTracker,
    lengths: &Lengths,
) {
    for request in std::mem::take(&mut peer.outstanding) {
        remember_canceled_request(peer, request);
        let (index, begin, _) = request;
        if let Ok(vpi) = lengths.validate_piece(index) {
            chunk_tracker.reject_chunk(vpi, begin / super::core::CHUNK_SIZE, pid);
            if chunk_tracker.has_missing(vpi) {
                piece_tracker.clear_in_flight(vpi);
            }
        }
    }
}

fn drop_choked_uploads(peer: &Peer) {
    peer.pending_uploads
        .lock()
        .retain(|&(index, begin, length)| {
            if !peer.supports_fast {
                return false;
            }
            if peer.sent_allowed_fast.contains(&index) {
                return true;
            }
            send_owed_reject(&peer.cmd_tx, index, begin, length);
            false
        });
}

async fn recv_scan_batch(rx: &mut Option<mpsc::Receiver<Vec<u32>>>) -> Option<Vec<u32>> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

async fn scan_existing_pieces(
    verifier: &PieceVerifier,
    storage: &Arc<FilesystemStorage>,
    lengths: &Lengths,
    found: mpsc::Sender<Vec<u32>>,
) {
    use tokio::task::JoinSet;
    const MAX_SCAN_BYTES: usize = 64 * 1024 * 1024;
    const MAX_BATCH: usize = 64;
    const BATCH_INTERVAL: Duration = Duration::from_millis(250);
    let total = lengths.total_pieces();
    if total == 0 {
        return;
    }
    let plen_hint = lengths
        .validate_piece(0)
        .map(|vpi| lengths.piece_length_of(vpi) as usize)
        .unwrap_or(0)
        .max(1);
    let concurrency = (MAX_SCAN_BYTES / plen_hint).clamp(1, 16);
    let mut set: JoinSet<Option<u32>> = JoinSet::new();
    let mut next: u32 = 0;
    let mut batch: Vec<u32> = Vec::new();
    let mut last_flush = Instant::now();
    loop {
        while next < total && set.len() < concurrency {
            let i = next;
            next += 1;
            let Ok(vpi) = lengths.validate_piece(i) else {
                continue;
            };
            let plen = lengths.piece_length_of(vpi) as usize;
            let offset = lengths.piece_offset(vpi);
            let storage = storage.clone();
            let verifier = verifier.clone();
            set.spawn(async move {
                let buf = storage.read_owned(offset, plen).await.ok()?;
                let ok = tokio::task::spawn_blocking(move || verifier.verify(i, &buf).is_ok())
                    .await
                    .ok()?;
                ok.then_some(i)
            });
        }
        let Some(res) = set.join_next().await else {
            break;
        };
        if let Ok(Some(i)) = res {
            batch.push(i);
        }
        if !batch.is_empty() && (batch.len() >= MAX_BATCH || last_flush.elapsed() >= BATCH_INTERVAL)
        {
            if found.send(std::mem::take(&mut batch)).await.is_err() {
                return;
            }
            last_flush = Instant::now();
        }
    }
    if !batch.is_empty() {
        let _ = found.send(batch).await;
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
    let file_progress = Arc::make_mut(&mut stats.file_progress);
    if file_progress.len() < layout.files().len() {
        file_progress.resize(layout.files().len(), 0);
    }
    for span in layout.spans_for(offset, len) {
        file_progress[span.file_index] = file_progress[span.file_index].saturating_add(span.len);
    }
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
        if let Some(dial_addr) = p.listen_addr {
            if p.downloaded_total > 0 {
                useful_peers.insert(
                    dial_addr,
                    p.max_outstanding.clamp(pipeline_floor, pipeline_cap),
                );
            }
            paused_candidates.push(dial_addr);
        }
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
    known_addrs.retain(|addr| !is_blocked(*addr));
    drop(blocked);
    (disconnected, removed)
}

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

pub fn split_tracker_list(raw: &str) -> impl Iterator<Item = &str> {
    raw.split([',', '\n', '\r'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

pub fn split_tracker_lists<I, S>(raw: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    normalize_tracker_urls(raw, &[])
}

pub(crate) fn normalize_tracker_urls<I, S>(incoming: I, existing: &[String]) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut seen: HashSet<String> = existing.iter().cloned().collect();
    let mut out = Vec::new();
    for raw in incoming {
        for part in split_tracker_list(raw.as_ref()) {
            if seen.insert(part.to_string()) {
                out.push(part.to_string());
            }
        }
    }
    out
}

fn shuffle_tier(urls: &mut [String]) {
    let mut rng = rand::rng();
    for i in (1..urls.len()).rev() {
        let j = (rand::RngExt::random::<u64>(&mut rng) as usize) % (i + 1);
        urls.swap(i, j);
    }
}

fn collect_tracker_tiers(meta: &TorrentMeta) -> Vec<Vec<String>> {
    let source = if !meta.announce_list.is_empty() {
        meta.announce_list.clone()
    } else {
        meta.announce
            .as_deref()
            .map(|raw| {
                split_tracker_list(raw)
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
            shuffle_tier(&mut urls);
            urls
        })
        .filter(|tier: &Vec<String>| !tier.is_empty())
        .collect()
}

fn flatten_tracker_tiers(tiers: &[Vec<String>]) -> Vec<String> {
    let mut seen = HashSet::new();
    tiers
        .iter()
        .flatten()
        .filter(|url| seen.insert(url.as_str()))
        .cloned()
        .collect()
}

fn collect_obfuscated_tiers(meta: &TorrentMeta) -> Vec<Vec<String>> {
    let mut seen = HashSet::new();
    meta.obfuscate_announce_list
        .iter()
        .map(|tier| {
            let mut urls: Vec<String> = tier
                .iter()
                .filter(|url| url.starts_with("http://") || url.starts_with("https://"))
                .filter(|url| seen.insert((*url).clone()))
                .cloned()
                .collect();
            shuffle_tier(&mut urls);
            urls
        })
        .filter(|tier| !tier.is_empty())
        .collect()
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

fn announce_event(entry: &TrackerUrlState, finished: bool) -> AnnounceEvent {
    if !entry.started_sent {
        AnnounceEvent::Started
    } else if finished && !entry.completed_sent {
        AnnounceEvent::Completed
    } else {
        AnnounceEvent::None
    }
}

#[allow(clippy::too_many_arguments)]
fn tracker_request(
    info_hash: Id20,
    peer_id: Id20,
    key: u32,
    port: u16,
    stats: &Arc<Mutex<TorrentStats>>,
    event: AnnounceEvent,
    num_want: u32,
    obfuscate: bool,
) -> AnnounceRequest {
    let (uploaded, downloaded, left) = {
        let stats = stats.lock();
        (
            stats.uploaded_bytes,
            stats.session_downloaded,
            stats.left_bytes,
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
        obfuscate,
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

struct TrackerLifecycle {
    by_info_hash: HashMap<Id20, TrackerRunState>,
    wake: watch::Sender<u64>,
    appended: Vec<Vec<String>>,
}

impl Default for TrackerLifecycle {
    fn default() -> Self {
        Self {
            by_info_hash: HashMap::new(),
            wake: watch::channel(0).0,
            appended: Vec::new(),
        }
    }
}

impl TrackerLifecycle {
    fn subscribe(&self) -> watch::Receiver<u64> {
        self.wake.subscribe()
    }

    fn announce_now(&mut self, completion: bool) {
        let now = Instant::now();
        for state in self.by_info_hash.values_mut() {
            for entry in state.tiers.iter_mut().flatten() {
                if entry.started_sent
                    && entry.failures == 0
                    && entry.next_at > now
                    && (!completion || !entry.completed_sent)
                {
                    entry.next_at = now;
                }
            }
        }
        self.wake.send_modify(|n| *n = n.wrapping_add(1));
    }

    fn run_state(&mut self, info_hash: Id20, tiers: &[Vec<String>]) -> &mut TrackerRunState {
        let appended = &self.appended;
        self.by_info_hash.entry(info_hash).or_insert_with(|| {
            let now = Instant::now();
            let late = appended.iter().filter(|tier| !tiers.contains(tier));
            TrackerRunState {
                tiers: tiers
                    .iter()
                    .chain(late)
                    .map(|tier| tracker_url_states(tier, now))
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
        self.appended.push(urls.to_vec());
        for state in self.by_info_hash.values_mut() {
            state.tiers.push(tracker_url_states(urls, now));
        }
        self.wake.send_modify(|n| *n = n.wrapping_add(1));
    }
}

fn tracker_url_states(urls: &[String], next_at: Instant) -> Vec<TrackerUrlState> {
    urls.iter()
        .map(|url| TrackerUrlState {
            url: url.clone(),
            next_at,
            failures: 0,
            started_sent: false,
            completed_sent: false,
        })
        .collect()
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
    obfuscated_tiers: usize,
) {
    if tiers.is_empty() {
        return;
    }
    let mut wake = lifecycle.lock().subscribe();

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
            (selected.clone(), state.active_tier < obfuscated_tiers)
        };
        let (selected, obfuscate) = selected;
        let wait = selected.next_at.saturating_duration_since(Instant::now());
        if !wait.is_zero() {
            tokio::select! {
                biased;
                _ = shutdown.changed() => break 'poll,
                _ = wake.changed() => {}
                _ = tokio::time::sleep(wait) => {}
            }
            continue;
        }

        let finished = stats.lock().finished;
        let event = announce_event(&selected, finished);
        let req = tracker_request(info_hash, peer_id, key, port, &stats, event, 200, obfuscate);
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
                let finished_now = stats.lock().finished;
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
                            updated.completed_sent |= finished;
                        } else if event == AnnounceEvent::Completed {
                            updated.completed_sent = true;
                        }
                        updated.next_at = if finished_now && !updated.completed_sent {
                            Instant::now()
                        } else {
                            Instant::now() + clamp_tracker_interval(resp.interval)
                        };
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
                            mse_first: obfuscate,
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
        .enumerate()
        .flat_map(|(tier, states)| {
            states
                .iter()
                .filter(|state| state.started_sent)
                .map(move |state| (state.url.clone(), tier < obfuscated_tiers))
        })
        .collect::<Vec<_>>();
    let mut stops = tokio::task::JoinSet::new();
    for (url, obfuscate) in started {
        let stopped = tracker_request(
            info_hash,
            peer_id,
            key,
            port,
            &stats,
            AnnounceEvent::Stopped,
            0,
            obfuscate,
        );
        let proxy = proxy.clone();
        stops.spawn(async move {
            match super::tracker::announce_with_proxy_and_source(
                &url,
                &stopped,
                TRACKER_STOPPED_TIMEOUT,
                proxy.as_ref(),
                tracker_source_addr,
            )
            .await
            {
                Ok(_) => tracing::debug!("tracker STOPPED ok url={url}"),
                Err(e) => tracing::debug!("tracker STOPPED failed url={url}: {e}"),
            }
        });
    }
    while stops.join_next().await.is_some() {}
}

type TrackerAnnounceDone = (
    usize,
    usize,
    AnnounceEvent,
    Result<super::tracker::AnnounceResponse, super::tracker::TrackerError>,
    u64,
    bool,
);

#[allow(clippy::too_many_arguments)]
async fn run_tracker_swarm_poller(
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
    _tier_change_tx: Option<mpsc::Sender<()>>,
    tracker_source_addr: Option<SocketAddr>,
    generation: GenerationToken,
    obfuscated_tiers: usize,
) {
    if tiers.is_empty() {
        return;
    }
    let mut wake = lifecycle.lock().subscribe();
    let mut inflight: HashSet<(usize, usize)> = HashSet::new();
    let mut announces: tokio::task::JoinSet<TrackerAnnounceDone> = tokio::task::JoinSet::new();

    'poll: loop {
        if shutdown.borrow().is_some() {
            break;
        }
        let finished = stats.lock().finished;
        let mut next_due: Option<Instant> = None;
        {
            let now = Instant::now();
            let mut lifecycle = lifecycle.lock();
            let state = lifecycle.run_state(info_hash, &tiers);
            for (ti, tier) in state.tiers.iter().enumerate() {
                for (ui, entry) in tier.iter().enumerate() {
                    if inflight.contains(&(ti, ui)) {
                        continue;
                    }
                    if entry.next_at > now || inflight.len() >= TRACKER_MAX_CONCURRENT_ANNOUNCES {
                        if entry.next_at > now {
                            next_due =
                                Some(next_due.map_or(entry.next_at, |d| d.min(entry.next_at)));
                        }
                        continue;
                    }
                    let event = announce_event(entry, finished);
                    let obfuscate = ti < obfuscated_tiers;
                    let req = tracker_request(
                        info_hash, peer_id, key, port, &stats, event, 200, obfuscate,
                    );
                    let url = entry.url.clone();
                    let proxy = proxy.clone();
                    let request_generation = generation.current();
                    inflight.insert((ti, ui));
                    announces.spawn(async move {
                        let result = super::tracker::announce_with_proxy_and_source(
                            &url,
                            &req,
                            TRACKER_ANNOUNCE_TIMEOUT,
                            proxy.as_ref(),
                            tracker_source_addr,
                        )
                        .await;
                        if let Ok(resp) = &result {
                            tracing::info!(target: "diag", "tracker ANNOUNCE ok url={url} event={event:?} peers={} interval_s={}", resp.peers.len(), resp.interval.as_secs());
                        } else if let Err(e) = &result {
                            tracing::info!(target: "diag", "tracker ANNOUNCE fail url={url} event={event:?} err={e}");
                        }
                        (ti, ui, event, result, request_generation, finished)
                    });
                }
            }
        }

        let sleep_for = next_due
            .map(|d| d.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_secs(60));
        let done = tokio::select! {
            biased;
            _ = shutdown.changed() => break 'poll,
            Some(joined) = announces.join_next(), if !announces.is_empty() => joined,
            _ = wake.changed() => continue 'poll,
            _ = tokio::time::sleep(sleep_for) => continue 'poll,
        };
        let Ok((ti, ui, event, result, request_generation, finished_at_send)) = done else {
            continue;
        };
        inflight.remove(&(ti, ui));
        match result {
            Ok(resp) => {
                let finished_now = stats.lock().finished;
                {
                    let mut lifecycle = lifecycle.lock();
                    let state = lifecycle.run_state(info_hash, &tiers);
                    if let Some(entry) = state.tiers.get_mut(ti).and_then(|t| t.get_mut(ui)) {
                        entry.failures = 0;
                        if event == AnnounceEvent::Started {
                            entry.started_sent = true;
                            entry.completed_sent |= finished_at_send;
                        } else if event == AnnounceEvent::Completed {
                            entry.completed_sent = true;
                        }
                        entry.next_at = if finished_now && !entry.completed_sent {
                            Instant::now()
                        } else {
                            Instant::now() + clamp_tracker_interval(resp.interval)
                        };
                    }
                }
                for addr in resp.peers {
                    let sent = tokio::select! {
                        biased;
                        _ = shutdown.changed() => break 'poll,
                        sent = tx.send(PeerEnvelope {
                            generation: request_generation,
                            candidate: PeerCandidate { addr, source: PeerSource::Tracker },
                            mse_first: ti < obfuscated_tiers,
                        }) => sent,
                    };
                    if sent.is_err() {
                        break 'poll;
                    }
                }
            }
            Err(_) => {
                let mut lifecycle = lifecycle.lock();
                let state = lifecycle.run_state(info_hash, &tiers);
                if let Some(entry) = state.tiers.get_mut(ti).and_then(|t| t.get_mut(ui)) {
                    entry.failures = entry.failures.saturating_add(1);
                    entry.next_at = Instant::now() + tracker_retry_delay(entry.failures);
                }
            }
        }
    }
    drop(announces);

    if !(*shutdown.borrow()).unwrap_or(true) {
        return;
    }
    let started = lifecycle
        .lock()
        .run_state(info_hash, &tiers)
        .tiers
        .iter()
        .enumerate()
        .flat_map(|(tier, states)| {
            states
                .iter()
                .filter(|state| state.started_sent)
                .map(move |state| (state.url.clone(), tier < obfuscated_tiers))
        })
        .collect::<Vec<_>>();
    let mut stops = tokio::task::JoinSet::new();
    for (url, obfuscate) in started {
        let stopped = tracker_request(
            info_hash,
            peer_id,
            key,
            port,
            &stats,
            AnnounceEvent::Stopped,
            0,
            obfuscate,
        );
        let proxy = proxy.clone();
        stops.spawn(async move {
            match super::tracker::announce_with_proxy_and_source(
                &url,
                &stopped,
                TRACKER_STOPPED_TIMEOUT,
                proxy.as_ref(),
                tracker_source_addr,
            )
            .await
            {
                Ok(_) => tracing::debug!("tracker STOPPED ok url={url}"),
                Err(e) => tracing::debug!("tracker STOPPED failed url={url}: {e}"),
            }
        });
    }
    while stops.join_next().await.is_some() {}
}

#[allow(clippy::too_many_arguments)]
async fn run_tracker_poller(
    private: bool,
    tx: mpsc::Sender<PeerEnvelope>,
    tiers: Vec<Vec<String>>,
    info_hash: Id20,
    peer_id: Id20,
    key: u32,
    port: u16,
    stats: Arc<Mutex<TorrentStats>>,
    shutdown: watch::Receiver<Option<bool>>,
    proxy: Option<risuko_http::ProxyConnector>,
    lifecycle: Arc<Mutex<TrackerLifecycle>>,
    tier_change_tx: Option<mpsc::Sender<()>>,
    tracker_source_addr: Option<SocketAddr>,
    generation: GenerationToken,
    obfuscated_tiers: usize,
) {
    macro_rules! run {
        ($poller:ident) => {
            $poller(
                tx,
                tiers,
                info_hash,
                peer_id,
                key,
                port,
                stats,
                shutdown,
                proxy,
                lifecycle,
                tier_change_tx,
                tracker_source_addr,
                generation,
                obfuscated_tiers,
            )
            .await
        };
    }
    if private {
        run!(run_tracker_tier_poller)
    } else {
        run!(run_tracker_swarm_poller)
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
        0,
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
    obfuscated_tiers: usize,
) -> TrackerPollers {
    let (shutdown_tx, shutdown_rx) = watch::channel(None);
    let mut tasks = tokio::task::JoinSet::new();
    for info_hash in info_hashes {
        if !tiers.is_empty() {
            let private = tier_change_tx.is_some();
            tasks.spawn(run_tracker_poller(
                private,
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
                obfuscated_tiers,
            ));
        }
    }
    TrackerPollers { shutdown_tx, tasks }
}

fn drop_unwanted_partial_pieces(
    piece_tracker: &PieceTracker,
    chunk_tracker: &mut ChunkTracker,
    piece_assemblies: &mut HashMap<u32, PieceAssembly>,
    lengths: &Lengths,
) {
    let stale: Vec<u32> = chunk_tracker
        .partial_pieces()
        .filter(|&idx| !chunk_tracker.has_live_requests(idx))
        .filter(|&idx| {
            lengths
                .validate_piece(idx)
                .is_ok_and(|vpi| !piece_tracker.is_wanted(vpi))
        })
        .collect();
    for idx in stale {
        if let Ok(vpi) = lengths.validate_piece(idx) {
            chunk_tracker.forget_piece(vpi);
        }
        piece_assemblies.remove(&idx);
    }
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
            .is_none_or(|a| a.sources.iter().all(Option::is_none));
        if should_drop {
            piece_assemblies.remove(&piece_idx);
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct LeafHashRequest {
    pieces_root: [u8; 32],
    index: u32,
    length: u32,
    proof_layers: u32,
}

impl LeafHashRequest {
    fn reject(self) -> Message {
        Message::HashReject {
            pieces_root: self.pieces_root,
            base_layer: 0,
            index: self.index,
            length: self.length,
            proof_layers: self.proof_layers,
        }
    }
}

fn v2_file_offset(
    tables: &[MerkleProofTable],
    layout: &FileSet,
    root: super::core::Id32,
) -> Option<(usize, u64)> {
    let k = tables.iter().position(|t| t.file_root == root)?;
    let file = layout
        .files()
        .iter()
        .filter(|f| !f.padding && f.length > 0)
        .nth(k)?;
    let table = &tables[k];
    (file.length == table.file_length && file.offset.is_multiple_of(table.piece_length as u64))
        .then_some((k, file.offset))
}

fn serve_leaf_hash_request(
    request: LeafHashRequest,
    tables: Option<&[MerkleProofTable]>,
    jobs: &Arc<Semaphore>,
    storage: &Arc<FilesystemStorage>,
    piece_tracker: &PieceTracker,
    lengths: &Lengths,
    cmd_tx: mpsc::Sender<PeerCommand>,
) {
    let plan = (|| {
        let tables = tables?;
        let (k, file_offset) = v2_file_offset(
            tables,
            storage.layout(),
            super::core::Id32(request.pieces_root),
        )?;
        let table = &tables[k];
        let pieces =
            table.leaf_request_pieces(request.index, request.length, request.proof_layers)?;
        let first_global = (file_offset / table.piece_length as u64) as u32;
        let all_local = pieces.iter().all(|p| {
            lengths
                .validate_piece(first_global + p)
                .is_ok_and(|vpi| piece_tracker.has_local(vpi))
        });
        all_local.then_some((k, file_offset, pieces))
    })();
    let permit = plan
        .as_ref()
        .and_then(|_| Arc::clone(jobs).try_acquire_owned().ok());
    let (Some((k, file_offset, pieces)), Some(permit)) = (plan, permit) else {
        let _ = cmd_tx.try_send(PeerCommand::Send(request.reject()));
        return;
    };
    let Some(table) = tables.and_then(|t| t.get(k)).cloned() else {
        let _ = cmd_tx.try_send(PeerCommand::Send(request.reject()));
        return;
    };
    let storage = Arc::clone(storage);
    tokio::spawn(async move {
        let _permit = permit;
        let mut piece_blocks = HashMap::with_capacity(pieces.len());
        for p in pieces {
            let start = p as u64 * table.piece_length as u64;
            let len = (table.file_length - start).min(table.piece_length as u64) as usize;
            let Ok(data) = storage.read_owned(file_offset + start, len).await else {
                let _ = cmd_tx.send(PeerCommand::Send(request.reject())).await;
                return;
            };
            let blocks = tokio::task::spawn_blocking(move || {
                data.chunks(super::core::merkle::BLOCK_SIZE as usize)
                    .map(super::core::merkle::hash_block)
                    .collect::<Vec<_>>()
            })
            .await
            .unwrap_or_default();
            piece_blocks.insert(p, blocks);
        }
        let message = match table.answer_leaf_request(
            request.index,
            request.length,
            request.proof_layers,
            &piece_blocks,
        ) {
            Some(hashes) => Message::Hashes {
                pieces_root: request.pieces_root,
                base_layer: 0,
                index: request.index,
                length: request.length,
                proof_layers: request.proof_layers,
                hashes: Bytes::from(hashes),
            },
            None => request.reject(),
        };
        let _ = cmd_tx.send(PeerCommand::Send(message)).await;
    });
}

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

    let root = super::core::Id32(pieces_root);
    let Some(table) = tables.iter().find(|t| t.file_root == root) else {
        return reject();
    };
    let Some(payload) = table.hashes_for_request(base_layer, index, length, proof_layers) else {
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

fn serve_ut_metadata(peer: &Peer, payload: &Bytes, info_bytes: &Arc<Vec<u8>>) {
    let Some(msg) = super::wire::extended::parse_ut_metadata(payload.clone()) else {
        return;
    };
    if msg.msg_type != ut_metadata_type::REQUEST {
        return;
    }
    let total = info_bytes.len();
    let total_pieces = total.div_ceil(META_PIECE_SIZE);
    let piece = msg.piece;
    if piece < 0 || (piece as usize) >= total_pieces {
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
        return;
    };
    let start = piece as usize * META_PIECE_SIZE;
    let end = (start + META_PIECE_SIZE).min(total);
    let block = &info_bytes[start..end];
    let data = ut_metadata_data(piece, total as i64, block);
    let _ = peer.cmd_tx.try_send(PeerCommand::Send(Message::Extended {
        ext_id: their_id,
        payload: data,
    }));
}

#[cfg(test)]
mod tests {
    #[test]
    fn request_timeout_grows_only_under_a_download_cap() {
        assert_eq!(request_timeout_for(0, 500), REQUEST_TIMEOUT);
        let t = request_timeout_for(100 * 1024, 64);
        assert!(t > REQUEST_TIMEOUT + Duration::from_secs(9), "{t:?}");
        assert!(t < REQUEST_TIMEOUT + Duration::from_secs(11), "{t:?}");
        assert!(request_timeout_for(1, 10_000) <= REQUEST_TIMEOUT + Duration::from_secs(120));
    }

    use super::*;
    use crate::core::merkle::{
        compute_root, hash_block, piece_layer_root, MerkleProofTable, BLOCK_SIZE,
    };
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
    fn holepunch_connect_stops_at_the_target_cap() {
        let target = test_peer(14_101);
        let (from_cmd, _rx) = mpsc::channel(1);
        let peers = HashMap::new();
        let pending_dials = HashMap::new();
        let mut priority_backlog = VecDeque::new();
        let mut peer_backlog = VecDeque::new();
        let mut dial_retries = VecDeque::new();
        let mut useful_redials = VecDeque::new();
        let mut known_addrs = HashSet::new();
        let mut holepunch_targets: HashSet<SocketAddr> = (0..MAX_HOLEPUNCH_ENTRIES)
            .map(|n| SocketAddr::from(([203, 0, 113, 1], 20_000 + n as u16)))
            .collect();

        handle_holepunch(
            HolepunchMsg {
                msg_type: holepunch_type::CONNECT,
                addr: target,
                err_code: 0,
            },
            test_peer(14_102),
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

        assert!(priority_backlog.is_empty());
        assert_eq!(holepunch_targets.len(), MAX_HOLEPUNCH_ENTRIES);
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
    fn mse_capable_peers_are_dialed_encrypted_first() {
        use crate::peer::EncryptionPolicy;
        assert_eq!(
            dial_policy(EncryptionPolicy::Prefer, true),
            EncryptionPolicy::PreferEncrypted
        );
        assert_eq!(
            dial_policy(EncryptionPolicy::Prefer, false),
            EncryptionPolicy::Prefer
        );
        assert_eq!(
            dial_policy(EncryptionPolicy::PlaintextOnly, true),
            EncryptionPolicy::PlaintextOnly
        );
        assert_eq!(
            dial_policy(EncryptionPolicy::RequireEncryption, true),
            EncryptionPolicy::RequireEncryption
        );
    }

    #[test]
    fn obfuscated_trackers_form_the_leading_tiers() {
        use crate::bencode::{encode_to_vec, Value};
        let tier = |urls: &[&str]| {
            Value::List(
                urls.iter()
                    .map(|u| Value::Bytes(u.as_bytes().to_vec()))
                    .collect(),
            )
        };
        let torrent = encode_to_vec(&Value::Dict(vec![
            (
                b"announce-list".to_vec(),
                Value::List(vec![tier(&["http://a/announce", "udp://c:80"])]),
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
            (
                b"obfuscate-announce-list".to_vec(),
                Value::List(vec![tier(&["http://a/announce", "udp://b:80"])]),
            ),
        ]));
        let meta = crate::core::metainfo::parse_torrent(&torrent).unwrap();
        let obfuscated = collect_obfuscated_tiers(&meta);
        assert_eq!(obfuscated, vec![vec!["http://a/announce".to_string()]]);
        let mut tiers = obfuscated;
        tiers.extend(collect_tracker_tiers(&meta));
        assert_eq!(tiers.len(), 2);
        assert_eq!(
            flatten_tracker_tiers(&tiers)
                .iter()
                .filter(|u| u.as_str() == "http://a/announce")
                .count(),
            1
        );
    }

    #[test]
    fn deselecting_a_file_drops_its_idle_partial_pieces() {
        let lengths = Lengths::new(
            4 * crate::core::CHUNK_SIZE as u64,
            2 * crate::core::CHUNK_SIZE,
        )
        .unwrap();
        let mut pieces = PieceTracker::new(lengths);
        let mut chunks = ChunkTracker::new(lengths);
        let p0 = lengths.validate_piece(0).unwrap();
        let p1 = lengths.validate_piece(1).unwrap();
        let first = chunks.next_chunk(p0, 1).unwrap();
        chunks.mark_received(first.info);
        let _live = chunks.next_chunk(p1, 2).unwrap();
        let mut assemblies = HashMap::from([(
            0,
            PieceAssembly {
                buf: vec![0; 2 * crate::core::CHUNK_SIZE as usize],
                sources: vec![Some("192.0.2.9".parse().unwrap()), None],
                completed: false,
            },
        )]);

        drop_unwanted_partial_pieces(&pieces, &mut chunks, &mut assemblies, &lengths);
        assert!(chunks.is_tracked(0) && chunks.is_tracked(1));

        pieces.set_wanted(vec![false, false]);
        drop_unwanted_partial_pieces(&pieces, &mut chunks, &mut assemblies, &lengths);
        assert!(!chunks.is_tracked(0));
        assert!(assemblies.is_empty());
        assert!(chunks.is_tracked(1));
    }

    #[test]
    fn wanted_pieces_cover_selected_files_only() {
        let info = ValidatedTorrentMetaV1Info {
            name: "sel".into(),
            piece_length: 10,
            pieces: vec![0u8; 3 * 20],
            private: false,
            files: vec![
                TorrentMetaInfo {
                    path: vec!["a".into()],
                    length: 15,
                    padding: false,
                },
                TorrentMetaInfo {
                    path: vec![".pad".into(), "5".into()],
                    length: 5,
                    padding: true,
                },
                TorrentMetaInfo {
                    path: vec!["b".into()],
                    length: 10,
                    padding: false,
                },
            ],
            single_file_mode: false,
        };
        let lengths = Lengths::new(30, 10).unwrap();
        let layout = FileSet::from_meta(&info, Path::new("/tmp"));
        assert_eq!(wanted_pieces(&layout, &lengths, None), vec![true; 3]);
        assert_eq!(
            wanted_pieces(&layout, &lengths, Some(&[2])),
            vec![false, false, true]
        );
        assert_eq!(
            wanted_pieces(&layout, &lengths, Some(&[0])),
            vec![true, true, false]
        );
        assert_eq!(
            wanted_pieces(&layout, &lengths, Some(&[1, 9])),
            vec![false; 3]
        );
        let wanted = wanted_pieces(&layout, &lengths, Some(&[2]));
        assert_eq!(PieceTracker::bytes_of(&lengths, |i| wanted[i]), 10);
        assert_eq!(info.selectable_file_indices(), vec![0, 2]);
    }

    #[tokio::test]
    async fn files_whose_part_file_data_cannot_move_stay_unselected() {
        let info = ValidatedTorrentMetaV1Info {
            name: "root".into(),
            piece_length: 10,
            pieces: vec![0; 20 * 3],
            private: false,
            files: vec![
                TorrentMetaInfo {
                    path: vec!["a".into()],
                    length: 15,
                    padding: false,
                },
                TorrentMetaInfo {
                    path: vec!["sub".into(), "b".into()],
                    length: 15,
                    padding: false,
                },
            ],
            single_file_mode: false,
        };
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let storage = FilesystemStorage::new(&info, &root)
            .with_parts_dir(crate::storage::parts_dir_for(&root, "abcd"));
        let (in_effect, applied) = apply_storage_selection(&storage, Some(vec![0])).await;
        assert_eq!(in_effect, Some(vec![0]));
        assert!(applied.is_ok());

        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("sub"), b"").unwrap();
        let (in_effect, applied) = apply_storage_selection(&storage, None).await;
        assert_eq!(in_effect, Some(vec![0]));
        let err = applied.unwrap_err();
        assert!(
            err.contains(&root.join("sub").join("b").display().to_string()),
            "error names the file path: {err}"
        );
        let lengths = Lengths::new(30, 10).unwrap();
        assert_eq!(
            wanted_pieces(storage.layout(), &lengths, in_effect.as_deref()),
            vec![true, true, false]
        );

        std::fs::remove_file(root.join("sub")).unwrap();
        let (in_effect, applied) = apply_storage_selection(&storage, None).await;
        assert_eq!(in_effect, None);
        assert!(applied.is_ok());
    }

    #[test]
    fn pex_flags_describe_the_connection() {
        let (tx, _rx) = mpsc::channel(1);
        let mut peer = Peer::connected(test_peer(14_020), tx, 1, 1, 1, true, false, None);
        assert_eq!(peer.pex_flags(8), pex_flag::REACHABLE);
        peer.link = PeerLink {
            encrypted: true,
            utp: true,
        };
        peer.their_ut_holepunch_id = Some(5);
        peer.bitfield = vec![0xff];
        assert_eq!(
            peer.pex_flags(8),
            pex_flag::ENCRYPTION
                | pex_flag::SEED
                | pex_flag::UTP
                | pex_flag::HOLEPUNCH
                | pex_flag::REACHABLE
        );
        let (tx, _rx) = mpsc::channel(1);
        let mut inbound = Peer::connected(test_peer(14_021), tx, 1, 1, 1, false, false, None);
        inbound.prefers_encryption = true;
        inbound.upload_only = true;
        assert_eq!(inbound.pex_flags(8), pex_flag::ENCRYPTION | pex_flag::SEED);
    }

    #[test]
    fn holepunch_rendezvous_reaches_inbound_peer_by_listen_addr() {
        let target_listen = test_peer(14_011);
        let (target_tx, mut target_rx) = mpsc::channel(4);
        let mut target = Peer::connected(test_peer(51_000), target_tx, 1, 1, 1, false, false, None);
        target.listen_addr = Some(target_listen);
        target.their_ut_holepunch_id = Some(9);
        let peers = HashMap::from([(1u32, target)]);
        let initiator = SocketAddr::from(([198, 51, 100, 7], 14_012));
        let (from_cmd, mut from_rx) = mpsc::channel(4);

        handle_holepunch(
            HolepunchMsg {
                msg_type: holepunch_type::RENDEZVOUS,
                addr: target_listen,
                err_code: 0,
            },
            initiator,
            Some(7),
            &from_cmd,
            &peers,
            &HashMap::new(),
            &mut VecDeque::new(),
            &mut VecDeque::new(),
            &mut VecDeque::new(),
            &mut VecDeque::new(),
            &mut HashSet::new(),
            &mut HashSet::new(),
            &RwLock::new(BlockList::default()),
        );

        let connect_to = |cmd: PeerCommand, ext: u8| match cmd {
            PeerCommand::Send(Message::Extended { ext_id, payload }) if ext_id == ext => {
                let msg = parse_holepunch(&payload).expect("holepunch payload");
                assert_eq!(msg.msg_type, holepunch_type::CONNECT);
                msg.addr
            }
            other => panic!("expected holepunch CONNECT, got {other:?}"),
        };
        assert_eq!(connect_to(target_rx.try_recv().unwrap(), 9), initiator);
        assert_eq!(connect_to(from_rx.try_recv().unwrap(), 7), target_listen);
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
        assert!(!dial_slot_available(99, 1, 100, MAX_PENDING_DIALS, true));
        assert!(!dial_slot_available(52, 48, 100, MAX_PENDING_DIALS, true));
        assert!(dial_slot_available(51, 47, 100, MAX_PENDING_DIALS, true));
        assert!(!dial_slot_available(51, 47, 100, MAX_PENDING_DIALS, false));
        assert!(!dial_slot_available(0, 36, 100, 36, true));
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
        assert_eq!(pipeline_slow_start_target(48, 48, 256), Some(96));
        assert_eq!(pipeline_slow_start_target(250, 250, 500), Some(500));
        assert_eq!(pipeline_slow_start_target(6, 6, 10), Some(10));
        assert_eq!(pipeline_slow_start_target(500, 500, 500), None);
        assert_eq!(pipeline_slow_start_target(10, 10, 10), None);
    }

    #[test]
    fn pipeline_target_tracks_delivery_rate() {
        assert_eq!(pipeline_target(128, Duration::from_secs(2), 6, 256, 6), 14);
        assert_eq!(pipeline_target(2, Duration::from_secs(2), 6, 256, 6), 6);
        assert_eq!(pipeline_target(20, Duration::from_secs(2), 6, 256, 6), 14);
        assert_eq!(pipeline_target(128, Duration::from_secs(2), 32, 32, 32), 32);
        assert_eq!(pipeline_target(128, Duration::from_secs(2), 6, 256, 14), 28);
        assert_eq!(
            pipeline_target(128, Duration::from_secs(2), 6, 256, 64),
            128
        );
        assert_eq!(
            pipeline_target(128, Duration::from_secs(2), 6, 256, 160),
            256
        );
        assert_eq!(pipeline_target(40, Duration::from_secs(2), 6, 256, 64), 80);
        assert_eq!(pipeline_target(10, Duration::from_secs(2), 6, 256, 64), 20);
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
            Some(PipelineAdjustment::Rate { target: 128 })
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
        assert_eq!(pipeline_bounds(None), (6, 500));
        assert_eq!(pipeline_bounds(Some(256)), (6, 256));
        assert_eq!(pipeline_bounds(Some(1024)), (6, 500));
        assert_eq!(pipeline_bounds(Some(1)), (1, 1));
    }

    #[tokio::test]
    async fn failed_webseed_result_keeps_peer_progress_on_the_piece() {
        let piece_length = 2 * crate::core::CHUNK_SIZE;
        let info = ValidatedTorrentMetaV1Info {
            name: "w.bin".into(),
            piece_length,
            pieces: vec![0; 20],
            private: false,
            files: vec![TorrentMetaInfo {
                path: vec!["w.bin".into()],
                length: piece_length as u64,
                padding: false,
            }],
            single_file_mode: true,
        };
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(FilesystemStorage::new(&info, tmp.path()));
        let lengths = Lengths::new(piece_length as u64, piece_length).unwrap();
        let piece = lengths.validate_piece(0).unwrap();
        let mut pieces = PieceTracker::new(lengths);
        let mut chunks = ChunkTracker::new(lengths);
        let first = chunks.next_chunk(piece, 7).unwrap();
        let _second = chunks.next_chunk(piece, 7).unwrap();
        chunks.mark_received(first.info);
        let mut assemblies = HashMap::from([(
            0,
            PieceAssembly {
                buf: vec![0; piece_length as usize],
                sources: vec![Some("192.0.2.9".parse().unwrap()), None],
                completed: false,
            },
        )]);
        let stats = Arc::new(Mutex::new(TorrentStats::initial(
            piece_length as u64,
            piece_length as u64,
            vec![piece_length as u64],
        )));
        let mut peers = HashMap::new();
        let failed = |from_webseed| VerifyResult {
            piece_index: 0,
            write_error: None,
            verify_ok: false,
            from_webseed,
            contributors: vec!["192.0.2.9".parse().unwrap()],
            failed_copy: None,
            culprits: Vec::new(),
        };
        let mut corruption = CorruptionTracker::new(Some(1));
        let blocklist = RwLock::new(BlockList::default());

        process_verify_result(
            failed(true),
            &lengths,
            &mut pieces,
            &mut chunks,
            &mut peers,
            &storage,
            &stats,
            &mut assemblies,
            &mut corruption,
            &blocklist,
        )
        .await;
        assert!(assemblies.contains_key(&0));
        assert_eq!(chunks.contested_pieces().collect::<Vec<_>>(), vec![0]);
        assert!(!chunks.has_missing(piece));

        process_verify_result(
            failed(false),
            &lengths,
            &mut pieces,
            &mut chunks,
            &mut peers,
            &storage,
            &stats,
            &mut assemblies,
            &mut corruption,
            &blocklist,
        )
        .await;
        assert!(!assemblies.contains_key(&0));
        assert!(chunks.has_missing(piece));
        assert!(blocklist.read().contains("192.0.2.9".parse().unwrap()));
    }

    #[test]
    fn canceled_requests_forget_the_oldest_first() {
        let mut canceled = CanceledRequests::default();
        for i in 0..=CanceledRequests::MAX as u32 {
            canceled.insert((i, 0, 16384));
        }
        assert!(!canceled.remove(&(0, 0, 16384)));
        assert!(canceled.remove(&(1, 0, 16384)));
        assert!(canceled.remove(&(CanceledRequests::MAX as u32, 0, 16384)));
        for round in 0..3 * CanceledRequests::MAX as u32 {
            let key = (round, 1, 16384);
            canceled.insert(key);
            canceled.remove(&key);
        }
        assert!(canceled.order.len() <= 2 * CanceledRequests::MAX + 1);
    }

    fn piece_result(
        verify_ok: bool,
        contributors: &[IpAddr],
        failed_copy: Option<Arc<Vec<ChunkDigest>>>,
        culprits: Vec<IpAddr>,
    ) -> VerifyResult {
        VerifyResult {
            piece_index: 3,
            write_error: None,
            verify_ok,
            from_webseed: false,
            contributors: contributors.to_vec(),
            failed_copy,
            culprits,
        }
    }

    #[test]
    fn sole_contributor_is_struck_at_once_and_banned_at_the_limit() {
        let mut corruption = CorruptionTracker::new(Some(2));
        let a: IpAddr = "192.0.2.1".parse().unwrap();
        assert!(corruption
            .record_result(&piece_result(false, &[a], None, Vec::new()))
            .is_empty());
        assert_eq!(
            corruption.record_result(&piece_result(false, &[a], None, Vec::new())),
            vec![a]
        );
        assert!(corruption.strikes.is_empty());
    }

    #[test]
    fn multi_source_failure_strikes_only_the_differing_chunk_source() {
        let mut corruption = CorruptionTracker::new(Some(1));
        let a: IpAddr = "192.0.2.1".parse().unwrap();
        let b: IpAddr = "192.0.2.2".parse().unwrap();
        let chunk = super::super::core::CHUNK_SIZE as usize;
        let good = vec![7u8; 2 * chunk];
        let mut bad = good.clone();
        bad[chunk + 5] ^= 0xff;
        let sources = [Some(a), Some(b)];
        let failed = Arc::new(chunk_digests(&bad, &sources));
        assert!(corruption
            .record_result(&piece_result(
                false,
                &[a, b],
                Some(failed.clone()),
                Vec::new()
            ))
            .is_empty());
        assert!(corruption.strikes.is_empty());
        let prior = corruption.copies.lock().get(3).expect("record kept");
        let culprits = differing_sources(&prior, &chunk_digests(&good, &sources));
        assert_eq!(culprits, vec![b]);
        assert_eq!(
            corruption.record_result(&piece_result(true, &[a, b], None, culprits)),
            vec![b]
        );
        assert!(corruption.copies.lock().get(3).is_none());
    }

    #[test]
    fn webseed_success_drops_the_record_without_strikes() {
        let mut corruption = CorruptionTracker::new(Some(1));
        let a: IpAddr = "192.0.2.1".parse().unwrap();
        let b: IpAddr = "192.0.2.2".parse().unwrap();
        let copy = Arc::new(vec![(Some(a), [0u8; 20])]);
        corruption.record_result(&piece_result(false, &[a, b], Some(copy), Vec::new()));
        assert!(corruption.copies.lock().get(3).is_some());
        let mut ok = piece_result(true, &[], None, Vec::new());
        ok.from_webseed = true;
        assert!(corruption.record_result(&ok).is_empty());
        assert!(corruption.copies.lock().get(3).is_none());
    }

    #[test]
    fn failed_copy_records_are_bounded_and_evict_the_oldest() {
        let mut copies = FailedCopies::default();
        for piece in 0..=MAX_FAILED_COPIES as u32 {
            copies.insert(piece, Arc::new(Vec::new()));
        }
        assert!(copies.get(0).is_none());
        assert!(copies.get(1).is_some());
        assert_eq!(copies.map.len(), MAX_FAILED_COPIES);
    }

    #[test]
    fn hash_failures_never_ban_when_disabled() {
        let mut corruption = CorruptionTracker::new(None);
        let a: IpAddr = "192.0.2.1".parse().unwrap();
        assert!(corruption.copies_handle().is_none());
        for _ in 0..10 {
            assert!(corruption
                .record_result(&piece_result(false, &[a], None, Vec::new()))
                .is_empty());
            assert!(corruption.record_strikes(&[a]).is_empty());
        }
        assert!(corruption.strikes.is_empty());
    }

    #[test]
    fn ban_from_another_torrent_changes_the_revision_peers_are_swept_on() {
        let shared = RwLock::new(BlockList::default());
        let seen = shared.read().ban_revision();
        shared.write().ban("192.0.2.4".parse().unwrap());
        assert_ne!(shared.read().ban_revision(), seen);
        assert!(shared.read().contains("192.0.2.4".parse().unwrap()));
    }

    #[tokio::test]
    async fn repeated_write_failures_halt_requests_until_resumed() {
        let mut queue = DiskQueue::new(u64::MAX);
        let outcome = |write_error: Option<&str>| VerifyResult {
            piece_index: 0,
            write_error: write_error.map(str::to_string),
            verify_ok: true,
            from_webseed: false,
            contributors: Vec::new(),
            failed_copy: None,
            culprits: Vec::new(),
        };
        for _ in 1..MAX_WRITE_FAILURES {
            assert_eq!(
                queue.record_write(&outcome(Some("No space left on device"))),
                None
            );
        }
        assert_eq!(queue.record_write(&outcome(None)), None);
        for _ in 1..MAX_WRITE_FAILURES {
            assert_eq!(
                queue.record_write(&outcome(Some("No space left on device"))),
                None
            );
        }
        assert!(!queue.is_full());
        assert_eq!(
            queue
                .record_write(&outcome(Some("No space left on device")))
                .as_deref(),
            Some("No space left on device")
        );
        assert!(queue.is_full());
        queue.resume();
        assert!(!queue.is_full());
    }

    #[tokio::test]
    async fn disk_queue_pauses_at_its_byte_limit_and_releases_failed_tasks() {
        let mut queue = DiskQueue::new(100);
        let done = |piece_index| VerifyResult {
            piece_index,
            write_error: None,
            verify_ok: true,
            from_webseed: false,
            contributors: Vec::new(),
            failed_copy: None,
            culprits: Vec::new(),
        };
        queue.spawn(60, async move { done(1) });
        assert!(!queue.is_full());
        queue.spawn(60, async move { done(2) });
        assert!(queue.is_full());
        queue.spawn(30, async move { panic!("write task failed") });
        let mut joined = 0;
        while let Some(result) = queue.join_next().await {
            joined += 1;
            let _ = result;
        }
        assert_eq!(joined, 3);
        assert_eq!(queue.queued_bytes, 0);
        assert!(queue.sizes.is_empty());
        assert!(!queue.is_full());
        assert!(queue.is_empty());
    }

    #[test]
    fn torrent_request_budget_is_shared_in_proportion_to_demand() {
        assert_eq!(effective_request_limit(256, 256), 256);
        assert_eq!(effective_request_limit(256, TORRENT_REQUEST_BUDGET), 256);
        assert_eq!(effective_request_limit(256, 256 + 49 * 6), 256);
        assert_eq!(effective_request_limit(256, 4 * TORRENT_REQUEST_BUDGET), 64);
        assert_eq!(effective_request_limit(6, 100 * TORRENT_REQUEST_BUDGET), 1);
    }

    #[test]
    fn pipeline_depth_shrinks_after_stall() {
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
            piece_layer_root(&piece_roots, blocks_per_piece)
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

    #[test]
    fn v2_file_offset_skips_empty_v1_entries() {
        let piece_length = BLOCK_SIZE;
        let (mut tables, _) = make_v2_tables(2, piece_length);
        let (second, second_root) = make_v2_tables(3, piece_length);
        tables.extend(second);
        let file = |name: &str, length: u64| TorrentMetaInfo {
            path: vec![name.into()],
            length,
            padding: false,
        };
        let info = ValidatedTorrentMetaV1Info {
            name: "hybrid".into(),
            piece_length,
            pieces: vec![0; 20 * 5],
            private: false,
            files: vec![
                file("a", 2 * piece_length as u64),
                file("empty", 0),
                file("b", 3 * piece_length as u64),
            ],
            single_file_mode: false,
        };
        let layout = FileSet::from_meta(&info, Path::new("/tmp"));
        assert_eq!(
            v2_file_offset(&tables, &layout, second_root),
            Some((1, 2 * piece_length as u64))
        );
    }

    #[tokio::test]
    async fn leaf_hash_requests_past_the_job_cap_are_rejected() {
        let piece_length = BLOCK_SIZE;
        let (tables, root) = make_v2_tables(2, piece_length);
        let info = ValidatedTorrentMetaV1Info {
            name: "leaf.bin".into(),
            piece_length,
            pieces: vec![0; 20 * 2],
            private: false,
            files: vec![TorrentMetaInfo {
                path: vec!["leaf.bin".into()],
                length: 2 * piece_length as u64,
                padding: false,
            }],
            single_file_mode: true,
        };
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(FilesystemStorage::new(&info, tmp.path()));
        let lengths = Lengths::new(2 * piece_length as u64, piece_length).unwrap();
        let mut tracker = PieceTracker::new(lengths);
        for i in 0..2 {
            tracker.set_local(lengths.validate_piece(i).unwrap(), true);
        }
        let request = LeafHashRequest {
            pieces_root: root.0,
            index: 0,
            length: 2,
            proof_layers: 0,
        };
        let jobs = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(4);
        let serve = |tx| {
            serve_leaf_hash_request(
                request,
                Some(&tables),
                &jobs,
                &storage,
                &tracker,
                &lengths,
                tx,
            )
        };

        serve(tx.clone());
        assert_eq!(jobs.available_permits(), 0);
        assert!(rx.try_recv().is_err());
        serve(tx);
        assert!(matches!(
            rx.try_recv(),
            Ok(PeerCommand::Send(Message::HashReject { .. }))
        ));
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
                    padding: false,
                },
                TorrentMetaInfo {
                    path: vec!["b".into()],
                    length: 15,
                    padding: false,
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
    fn announce_event_follows_started_then_completed() {
        let mut entry =
            tracker_url_states(&["https://t.example/announce".to_string()], Instant::now())
                .remove(0);
        assert_eq!(announce_event(&entry, false), AnnounceEvent::Started);
        assert_eq!(announce_event(&entry, true), AnnounceEvent::Started);
        entry.started_sent = true;
        assert_eq!(announce_event(&entry, false), AnnounceEvent::None);
        assert_eq!(announce_event(&entry, true), AnnounceEvent::Completed);
        entry.completed_sent = true;
        assert_eq!(announce_event(&entry, true), AnnounceEvent::None);
    }

    #[test]
    fn verified_piece_progress_is_idempotent() {
        let (lengths, layout) = two_file_layout();
        let mut stats = TorrentStats::initial(
            lengths.total_length(),
            lengths.total_length(),
            layout.files().iter().map(|f| f.length).collect(),
        );
        let mut tracker = PieceTracker::new(lengths);
        let piece = tracker.lengths().validate_piece(1).unwrap();

        if mark_verified_piece_local(&mut tracker, piece) {
            add_piece_progress(&mut stats, tracker.lengths(), &layout, piece);
        }
        assert_eq!(stats.progress_bytes, 10);
        assert_eq!(*stats.file_progress, vec![5, 5]);

        if mark_verified_piece_local(&mut tracker, piece) {
            add_piece_progress(&mut stats, tracker.lengths(), &layout, piece);
        }
        assert_eq!(stats.progress_bytes, 10);
        assert_eq!(*stats.file_progress, vec![5, 5]);
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
    fn peer_registry_sessions_with_equal_ids_do_not_clobber_each_other() {
        let (torrent_id, pid) = (7_001, 0);
        let scope_a = Arc::new(());
        let scope_b = Arc::new(());
        let addr_a: SocketAddr = "127.0.0.1:6001".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.1:6002".parse().unwrap();
        let (tx_a, _rx_a) = mpsc::channel(1);
        let (tx_b, _rx_b) = mpsc::channel(1);

        peer_registry::put(torrent_id, pid, &scope_a, tx_a, addr_a, None);
        peer_registry::put(torrent_id, pid, &scope_b, tx_b, addr_b, None);

        assert_eq!(
            peer_registry::take(torrent_id, pid, &scope_a).map(|(_, addr, _)| addr),
            Some(addr_a)
        );
        assert_eq!(
            peer_registry::take(torrent_id, pid, &scope_b).map(|(_, addr, _)| addr),
            Some(addr_b)
        );
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
        let live_addr: SocketAddr = "127.0.0.1:50123".parse().unwrap();
        let listen_addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let (tx, _rx) = mpsc::channel(1);
        tx.try_send(PeerCommand::Send(Message::KeepAlive)).unwrap();
        let extra = tx.clone();

        let mut peer = Peer::connected(live_addr, tx, 1, 1, 1, false, false, None);
        peer.listen_addr = Some(listen_addr);
        let io_task = tokio::spawn(async move {
            extra.closed().await;
        });
        peer.io_abort = Some(io_task.abort_handle());
        let (silent_tx, _silent_rx) = mpsc::channel(1);
        let silent = Peer::connected(
            "127.0.0.1:50124".parse().unwrap(),
            silent_tx,
            1,
            1,
            1,
            false,
            false,
            None,
        );

        let mut peers = HashMap::new();
        peers.insert(1, peer);
        peers.insert(2, silent);
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
        assert_eq!(paused_candidates, vec![listen_addr]);

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

    fn driven_peer(
        pieces: u32,
    ) -> (
        HashMap<u32, Peer>,
        PieceTracker,
        ChunkTracker,
        mpsc::Receiver<PeerCommand>,
    ) {
        let lengths = Lengths::new(u64::from(pieces) * 16 * 1024, 16 * 1024).unwrap();
        let mut piece_tracker = PieceTracker::new(lengths);
        let chunk_tracker = ChunkTracker::new(lengths);
        let (tx, rx) = mpsc::channel(64);
        let mut peer = Peer::connected(test_peer(52_100), tx, 1, 4, 8, true, true, None);
        peer.am_interested = true;
        peer.peer_choking = false;
        peer.bitfield = vec![0xff];
        piece_tracker.add_peer_bitfield(&[0xff]);
        let mut peers = HashMap::new();
        peers.insert(1, peer);
        (peers, piece_tracker, chunk_tracker, rx)
    }

    #[test]
    fn snubbed_peer_keeps_a_single_probe_request() {
        let (mut peers, mut pt, mut ct, _rx) = driven_peer(2);
        peers.get_mut(&1).unwrap().snubbing = true;
        drive_peer(1, &mut peers, &mut pt, &mut ct, &HashSet::new());
        assert_eq!(peers[&1].outstanding.len(), 1);
        assert!(!request_peer_eligible(&peers[&1]));
        drive_peer(1, &mut peers, &mut pt, &mut ct, &HashSet::new());
        assert_eq!(peers[&1].outstanding.len(), 1);
    }

    #[test]
    fn choke_rejected_chunks_are_not_asked_again_until_unchoke() {
        let (mut peers, mut pt, mut ct, _rx) = driven_peer(1);
        let peer = peers.get_mut(&1).unwrap();
        peer.peer_choking = true;
        peer.remote_allowed_fast.insert(0);
        drive_peer(1, &mut peers, &mut pt, &mut ct, &HashSet::new());
        assert_eq!(peers[&1].outstanding, vec![(0, 0, 16 * 1024)]);
        let peer = peers.get_mut(&1).unwrap();
        peer.outstanding.clear();
        let vpi = pt.lengths().validate_piece(0).unwrap();
        ct.reject_chunk(vpi, 0, 1);
        peer.choke_rejected.insert((0, 0));
        drive_peer(1, &mut peers, &mut pt, &mut ct, &HashSet::new());
        assert!(peers[&1].outstanding.is_empty());
        let peer = peers.get_mut(&1).unwrap();
        peer.peer_choking = false;
        peer.choke_rejected.clear();
        drive_peer(1, &mut peers, &mut pt, &mut ct, &HashSet::new());
        assert_eq!(peers[&1].outstanding.len(), 1);
    }

    #[tokio::test]
    async fn owed_rejects_survive_a_full_channel() {
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(PeerCommand::Disconnect).unwrap();
        send_owed_reject(&tx, 3, 0, 16384);
        assert!(matches!(rx.recv().await, Some(PeerCommand::Disconnect)));
        assert!(matches!(
            rx.recv().await,
            Some(PeerCommand::Send(Message::RejectRequest { index: 3, .. }))
        ));
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
        match build_hash_response(None, [0u8; 32], 0, 0, 1, 0) {
            Message::HashReject { .. } => {}
            _ => panic!("expected HashReject for None tables"),
        }
    }

    #[test]
    fn build_hash_response_rejects_partial_request() {
        let (tables, root) = make_v2_tables(4, 64 * 1024);
        match build_hash_response(Some(&tables), root.0, 2, 0, 4, 2) {
            Message::HashReject { .. } => {}
            _ => panic!("expected HashReject for proofs past the root"),
        }
        match build_hash_response(Some(&tables), root.0, 2, 2, 2, 1) {
            Message::Hashes { hashes, .. } => assert_eq!(hashes.len(), 3 * 32),
            other => panic!("expected Hashes for a proven range, got {other:?}"),
        }
        match build_hash_response(Some(&tables), root.0, 99, 0, 4, 0) {
            Message::HashReject { .. } => {}
            _ => panic!("expected HashReject for wrong base_layer"),
        }
    }

    #[test]
    fn build_hash_response_rejects_v1_verifier() {
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

        let set = select_unchoked(candidates.clone(), Some(4), 4);
        assert_eq!(set.len(), 5);
        assert!(set.contains(&4));
        assert!(set.contains(&2) && set.contains(&5) && set.contains(&3) && set.contains(&6));

        let set = select_unchoked(candidates, Some(2), 4);
        assert_eq!(set.len(), 5);
        assert!(set.contains(&2) && set.contains(&5) && set.contains(&3) && set.contains(&6));
        assert!(set.contains(&1) || set.contains(&4));
    }

    #[test]
    fn ties_are_broken_by_lowest_pid() {
        let set = select_unchoked(vec![(9, 5), (3, 5), (7, 5), (1, 5), (4, 5)], None, 4);
        assert_eq!(set, HashSet::from([1, 3, 4, 7]));
    }

    #[test]
    fn adaptive_slots_grow_and_shrink_within_bounds() {
        let step = SLOT_RATE_STEP_BPS * CHOKE_EVAL_INTERVAL.as_secs();
        assert_eq!(adaptive_slots(&[]), MIN_UPLOAD_SLOTS);
        assert_eq!(adaptive_slots(&[0, 0, 0]), MIN_UPLOAD_SLOTS);
        let fast = [8 * step; 7];
        assert_eq!(adaptive_slots(&fast), 7);
        let mixed = [
            8 * step,
            8 * step,
            8 * step,
            8 * step,
            8 * step,
            step / 2,
            step / 2,
        ];
        assert_eq!(adaptive_slots(&mixed), 5);
        assert_eq!(
            adaptive_slots(&[8 * step, 8 * step, step / 2]),
            MIN_UPLOAD_SLOTS
        );
        assert_eq!(adaptive_slots(&[1000 * step; 40]), MAX_UPLOAD_SLOTS);
    }

    #[test]
    fn rank_blends_history_with_the_current_window() {
        let (mut old, _rx1) = choke_peer(true, true);
        old.down_ewma = 1000;
        old.downloaded_window = 0;
        let (mut fresh, _rx2) = choke_peer(true, true);
        fresh.downloaded_window = 100;
        assert!(choke_rate(&old, false) > choke_rate(&fresh, false));
        fresh.snubbing = true;
        assert_eq!(choke_rate(&fresh, false), 0);
    }

    #[test]
    fn freed_slots_go_to_choked_interested_peers() {
        let mut peers = HashMap::new();
        let mut rxs = Vec::new();
        for pid in 1..=7u32 {
            let (mut peer, rx) = choke_peer(true, true);
            peer.downloaded_window = u64::from(pid) * 10;
            peers.insert(pid, peer);
            rxs.push(rx);
        }
        fill_choke_slots(&mut peers, false, Some(1));
        let unchoked: Vec<u32> = (1..=7).filter(|pid| !peers[pid].am_choking).collect();
        assert_eq!(unchoked, vec![1, 4, 5, 6, 7]);
        assert!(peers[&1].optimistic_unchoke);
        peers.remove(&7);
        fill_choke_slots(&mut peers, false, Some(1));
        let unchoked: Vec<u32> = (1..=6).filter(|pid| !peers[pid].am_choking).collect();
        assert_eq!(unchoked, vec![1, 3, 4, 5, 6]);
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
    fn slow_start_continues_until_the_rate_stops_growing() {
        assert!(!slow_start_plateaued(0.0, 10.0));
        assert!(!slow_start_plateaued(100.0, 200.0));
        assert!(slow_start_plateaued(100.0, 110.0));
        assert!(slow_start_plateaued(100.0, 60.0));
        assert_eq!(slow_start_rate(50, Duration::from_millis(500)), 100.0);
    }

    #[test]
    fn private_torrents_accept_inbound_peers_by_tracker_ip() {
        let tracker_peer = test_peer(51_413);
        let authorized: HashSet<SocketAddr> = [tracker_peer].into_iter().collect();
        let ephemeral = SocketAddr::new(tracker_peer.ip(), 60_001);
        assert!(inbound_peer_allowed(true, ephemeral, &authorized));
        assert!(inbound_peer_allowed(true, tracker_peer, &authorized));
        let stranger: SocketAddr = "198.51.100.9:6881".parse().unwrap();
        assert!(!inbound_peer_allowed(true, stranger, &authorized));
        assert!(inbound_peer_allowed(false, stranger, &authorized));
    }

    fn choke_peer(interested: bool, choking: bool) -> (Peer, mpsc::Receiver<PeerCommand>) {
        let (tx, rx) = mpsc::channel(64);
        let mut peer = Peer::connected(test_peer(52_200), tx, 1, 4, 8, true, true, None);
        peer.peer_interested = interested;
        peer.am_choking = choking;
        (peer, rx)
    }

    #[test]
    fn interest_events_only_fill_free_slots() {
        let mut peers = HashMap::new();
        let mut rxs = Vec::new();
        for pid in 1..=6u32 {
            let (mut peer, rx) = choke_peer(true, true);
            peer.downloaded_window = u64::from(pid) * 10;
            peers.insert(pid, peer);
            rxs.push(rx);
        }
        for pid in [1u32, 2, 3] {
            peers.get_mut(&pid).unwrap().am_choking = false;
        }
        fill_choke_slots(&mut peers, false, None);
        let unchoked: Vec<u32> = (1..=6).filter(|pid| !peers[pid].am_choking).collect();
        assert_eq!(unchoked, vec![1, 2, 3, 6]);
        fill_choke_slots(&mut peers, false, None);
        assert_eq!((1..=6).filter(|pid| !peers[pid].am_choking).count(), 4);
    }

    #[test]
    fn optimistic_pick_prefers_a_peer_without_a_turn() {
        let mut peers = HashMap::new();
        let mut rxs = Vec::new();
        for pid in 1..=3u32 {
            let (peer, rx) = choke_peer(true, true);
            peers.insert(pid, peer);
            rxs.push(rx);
        }
        peers.get_mut(&1).unwrap().optimistic_tried = true;
        assert_eq!(choose_optimistic(&mut peers, Some(1)), Some(3));
        assert_eq!(choose_optimistic(&mut peers, Some(3)), Some(2));
        assert_eq!(choose_optimistic(&mut peers, Some(2)), Some(3));
        assert_eq!(choose_optimistic(&mut peers, Some(3)), Some(1));
    }

    #[test]
    fn useless_peers_are_recycled_oldest_first() {
        let now = Instant::now();
        let old = now - PEER_USELESS_AFTER - Duration::from_secs(5);
        let mut peers = HashMap::new();
        let mut rxs = Vec::new();
        let mut add = |pid: u32, age: Instant, tweak: &dyn Fn(&mut Peer)| {
            let (mut peer, rx) = choke_peer(false, true);
            peer.connected_at = age;
            tweak(&mut peer);
            peers.insert(pid, peer);
            rxs.push(rx);
        };
        add(1, old, &|_| {});
        add(2, old - Duration::from_secs(10), &|_| {});
        add(3, now, &|_| {});
        add(4, old, &|p| p.downloaded_total = 1);
        add(5, old, &|p| p.peer_interested = true);
        assert_eq!(redundant_peers(&peers, false, now, 8, 8), vec![2, 1]);
        assert_eq!(redundant_peers(&peers, false, now, 8, 1), vec![2]);
        peers.get_mut(&3).unwrap().upload_only = true;
        assert_eq!(redundant_peers(&peers, true, now, 8, 8), vec![3]);
    }

    #[test]
    fn interest_is_withdrawn_when_the_peer_has_nothing_left_for_us() {
        let (mut peers, mut pt, mut ct, mut rx) = driven_peer(1);
        let vpi = pt.lengths().validate_piece(0).unwrap();
        pt.set_local(vpi, true);
        drive_peer(1, &mut peers, &mut pt, &mut ct, &HashSet::new());
        assert!(!peers[&1].am_interested);
        assert!(matches!(
            rx.try_recv(),
            Ok(PeerCommand::Send(Message::NotInterested))
        ));
    }

    #[test]
    fn interest_stays_while_the_needed_piece_is_in_flight_elsewhere() {
        let (mut peers, mut pt, mut ct, mut rx) = driven_peer(1);
        let vpi = pt.lengths().validate_piece(0).unwrap();
        pt.mark_in_flight(vpi);
        drive_peer(1, &mut peers, &mut pt, &mut ct, &HashSet::new());
        assert!(peers[&1].am_interested);
        assert!(rx.try_recv().is_err());
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

    #[test]
    fn appended_tier_reaches_a_poller_that_builds_its_state_late() {
        let hash = Id20::new([4; 20]);
        let old = vec![vec!["https://one.example/announce".to_string()]];
        let added = vec!["https://new.example/announce".to_string()];
        let mut lifecycle = TrackerLifecycle::default();
        let wake = lifecycle.subscribe();
        lifecycle.append_tier(&added);
        assert!(wake.has_changed().unwrap());
        assert_eq!(lifecycle.run_state(hash, &old).tiers.len(), 2);
        let mut fresh = TrackerLifecycle::default();
        fresh.append_tier(&added);
        let both = vec![old[0].clone(), added.clone()];
        assert_eq!(fresh.run_state(hash, &both).tiers.len(), 2);
    }

    #[tokio::test]
    async fn public_torrent_announces_to_every_tier_concurrently() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let hung = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hung_addr = hung.local_addr().unwrap();
        let hung_task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = hung.accept().await {
                held.push(socket);
            }
        });
        let live = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live_addr = live.local_addr().unwrap();
        let live_task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = live.accept().await {
                let mut buf = [0u8; 2048];
                let _ = socket.read(&mut buf).await;
                let body: &[u8] = b"d8:intervali1800e5:peers6:\x7f\x00\x00\x01\x1a\xe1e";
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(body).await;
            }
        });
        let (tx, mut rx) = mpsc::channel(8);
        let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
        let mut pollers = spawn_tracker_tier_pollers(
            tx,
            vec![
                vec![format!("http://{hung_addr}/announce")],
                vec![format!("http://{live_addr}/announce")],
            ],
            vec![Id20::new([1; 20])],
            Id20::new([0; 20]),
            1,
            6881,
            stats,
            None,
            Arc::new(Mutex::new(TrackerLifecycle::default())),
            None,
            None,
            GenerationToken::new(),
            0,
        );
        let envelope = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("live tracker must answer while the first tier hangs")
            .expect("peer channel open");
        assert_eq!(envelope.candidate.addr.port(), 6881);
        pollers.shutdown(false).await;
        hung_task.abort();
        live_task.abort();
    }

    async fn recording_tracker() -> (
        SocketAddr,
        mpsc::UnboundedReceiver<String>,
        tokio::task::JoinHandle<()>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (line_tx, line_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..n]);
                let _ = line_tx.send(request.lines().next().unwrap_or_default().to_string());
                let body: &[u8] = b"d8:intervali1800e5:peers0:e";
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(body).await;
            }
        });
        (addr, line_rx, task)
    }

    async fn announce_lines(
        private: bool,
        finished_at_start: bool,
        finish_later: bool,
    ) -> Vec<String> {
        let (addr, mut lines, server) = recording_tracker().await;
        let (tx, _rx) = mpsc::channel(8);
        let (tier_tx, _tier_rx) = mpsc::channel(1);
        let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
        stats.lock().finished = finished_at_start;
        let lifecycle = Arc::new(Mutex::new(TrackerLifecycle::default()));
        let mut pollers = spawn_tracker_tier_pollers(
            tx,
            vec![vec![format!("http://{addr}/announce")]],
            vec![Id20::new([1; 20])],
            Id20::new([0; 20]),
            1,
            6881,
            Arc::clone(&stats),
            None,
            Arc::clone(&lifecycle),
            private.then_some(tier_tx),
            None,
            GenerationToken::new(),
            0,
        );
        let first = tokio::time::timeout(Duration::from_secs(5), lines.recv())
            .await
            .expect("started announce")
            .expect("line");
        let mut seen = vec![first];
        if finish_later {
            stats.lock().finished = true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        lifecycle.lock().announce_now(finish_later);
        if let Ok(Some(line)) = tokio::time::timeout(Duration::from_secs(3), lines.recv()).await {
            seen.push(line);
        }
        pollers.shutdown(false).await;
        server.abort();
        seen
    }

    #[tokio::test]
    async fn torrent_complete_at_start_owes_no_completed_event() {
        for private in [false, true] {
            let seen = announce_lines(private, true, false).await;
            assert!(seen[0].contains("event=started"), "{seen:?}");
            assert_eq!(seen.len(), 2, "{seen:?}");
            assert!(!seen[1].contains("event="), "{seen:?}");
        }
    }

    #[tokio::test]
    async fn completion_during_the_session_announces_completed_at_once() {
        for private in [false, true] {
            let seen = announce_lines(private, false, true).await;
            assert!(seen[0].contains("event=started"), "{seen:?}");
            assert_eq!(seen.len(), 2, "{seen:?}");
            assert!(seen[1].contains("event=completed"), "{seen:?}");
        }
    }

    #[test]
    fn retries_are_queued_in_due_order() {
        let addr = |n: u16| test_peer(53_000 + n);
        let mut retries = VecDeque::new();
        let mut useful = VecDeque::new();
        schedule_peer_retry(addr(1), false, 3, &mut retries, &mut useful);
        schedule_peer_retry(addr(2), false, 0, &mut retries, &mut useful);
        schedule_peer_retry(addr(3), false, 1, &mut retries, &mut useful);
        let order: Vec<_> = retries.iter().map(|(a, _)| *a).collect();
        assert_eq!(order, vec![addr(2), addr(3), addr(1)]);
        schedule_peer_retry(addr(4), true, 1, &mut retries, &mut useful);
        schedule_peer_retry(addr(5), true, 0, &mut retries, &mut useful);
        assert_eq!(useful.front().map(|(a, _)| *a), Some(addr(5)));
    }

    #[test]
    fn fd_exhaustion_is_not_charged_as_a_dial_failure() {
        let mut failures = HashMap::new();
        let a = test_peer(53_101);
        assert_eq!(settle_dial_failures(&mut failures, a, true, true), 0);
        assert!(failures.is_empty());
        assert_eq!(settle_dial_failures(&mut failures, a, false, true), 1);
        assert_eq!(settle_dial_failures(&mut failures, a, true, true), 0);
        assert_eq!(failures.get(&a), Some(&1));
        assert_eq!(settle_dial_failures(&mut failures, a, false, false), 0);
        assert!(failures.is_empty());
    }

    #[test]
    fn dial_failures_and_useful_peers_stay_bounded() {
        let mut failures = HashMap::new();
        let a = test_peer(53_100);
        assert_eq!(bump_dial_failures(&mut failures, a), 1);
        assert_eq!(bump_dial_failures(&mut failures, a), 2);
        let mut useful = HashMap::new();
        for n in 0..(MAX_USEFUL_PEERS as u32 + 50) {
            let addr = SocketAddr::from(([10, 0, (n >> 8) as u8, n as u8], 6881));
            remember_useful_peer(&mut useful, addr, 8);
        }
        assert_eq!(useful.len(), MAX_USEFUL_PEERS);
    }

    #[test]
    fn duplicate_peer_ids_keep_the_link_the_lower_id_initiated() {
        let mk = |outbound: bool, id: Id20| {
            let (tx, _rx) = mpsc::channel(4);
            Peer::connected(test_peer(53_200), tx, 1, 4, 8, outbound, true, Some(id))
        };
        let low = Id20::new([1; 20]);
        let high = Id20::new([9; 20]);
        let mut peers = HashMap::new();
        peers.insert(1, mk(true, high));
        assert!(matches!(
            resolve_duplicate(&peers, test_peer(1).ip(), Some(high), low, false),
            DuplicateAction::RejectNew
        ));
        assert!(matches!(
            resolve_duplicate(&peers, test_peer(1).ip(), Some(high), Id20::new([200; 20]), false),
            DuplicateAction::ReplaceOld(old) if old == vec![1]
        ));
        assert!(matches!(
            resolve_duplicate(&peers, test_peer(1).ip(), Some(high), low, true),
            DuplicateAction::RejectNew
        ));
        assert!(matches!(
            resolve_duplicate(&peers, test_peer(1).ip(), None, low, false),
            DuplicateAction::Keep
        ));
        assert!(matches!(
            resolve_duplicate(&peers, test_peer(1).ip(), Some(low), low, false),
            DuplicateAction::Keep
        ));
    }

    #[tokio::test]
    async fn haves_that_miss_a_full_queue_are_flushed_later() {
        let (tx, mut rx) = mpsc::channel(1);
        let mut peer = Peer::connected(test_peer(53_300), tx, 1, 4, 8, true, true, None);
        peer.bitfield = vec![0b0100_0000];
        let mut peers = HashMap::new();
        peers.insert(1, peer);
        for piece in [0, 1, 2, 3] {
            broadcast_have(&mut peers, piece);
        }
        assert_eq!(peers[&1].pending_haves, vec![2, 3]);
        assert!(matches!(
            rx.try_recv(),
            Ok(PeerCommand::Send(Message::Have { piece_index: 0 }))
        ));
        let peer = peers.get_mut(&1).unwrap();
        flush_pending_haves(peer);
        assert_eq!(peer.pending_haves, vec![3]);
        assert!(matches!(
            rx.try_recv(),
            Ok(PeerCommand::Send(Message::Have { piece_index: 2 }))
        ));
        flush_pending_haves(peer);
        assert!(matches!(
            rx.try_recv(),
            Ok(PeerCommand::Send(Message::Have { piece_index: 3 }))
        ));
        assert!(peer.pending_haves.is_empty());
    }

    #[test]
    fn extended_handshake_advertises_upload_only_once_complete() {
        let ext = OurExtHandshake {
            listen_port: 6881,
            metadata_size: 100,
            private: false,
            prefers_encryption: true,
        };
        let ip = IpAddr::from([192, 0, 2, 1]);
        assert!(
            !ExtHandshake::decode(&ext.build(ip, false).encode())
                .unwrap()
                .upload_only
        );
        assert!(
            ExtHandshake::decode(&ext.build(ip, true).encode())
                .unwrap()
                .upload_only
        );
    }

    #[test]
    fn announce_now_wakes_only_trackers_owed_the_event() {
        let mut lifecycle = TrackerLifecycle::default();
        let hash = Id20::new([1; 20]);
        let urls = vec![vec![
            "http://a/announce".to_string(),
            "http://b/announce".to_string(),
        ]];
        let far = Instant::now() + Duration::from_secs(1000);
        {
            let state = lifecycle.run_state(hash, &urls);
            for entry in state.tiers[0].iter_mut() {
                entry.started_sent = true;
                entry.next_at = far;
            }
            state.tiers[0][1].completed_sent = true;
        }
        let wake = lifecycle.subscribe();
        lifecycle.announce_now(true);
        let state = lifecycle.run_state(hash, &urls);
        assert!(state.tiers[0][0].next_at <= Instant::now());
        assert_eq!(state.tiers[0][1].next_at, far);
        assert!(wake.has_changed().unwrap());
    }

    #[tokio::test]
    async fn add_trackers_after_shutdown_uses_a_fresh_poller_set() {
        let (tx, _rx) = mpsc::channel(8);
        let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
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
                    padding: false,
                },
                TorrentMetaInfo {
                    path: vec!["second.bin".into()],
                    length: 7,
                    padding: false,
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
                padding: false,
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
            disabled_files: Arc::new(Mutex::new(HashSet::new())),
            backoff: Arc::new(Mutex::new(HashMap::new())),
            etags: Arc::new(Mutex::new(HashMap::new())),
            layout: storage.layout().clone(),
            info,
            storage: Arc::clone(&storage),
            verifier: PieceVerifier::V1Sha1 {
                pieces: Arc::new(piece_hashes),
            },
            lengths,
            generation: GenerationToken::new(),
            throttle: Throttle::unlimited(),
        };

        let result = fetch_webseed_batch(ctx, vec![0, 1, 2]).await;
        assert_eq!(result.pieces.len(), 3);
        assert!(result
            .pieces
            .iter()
            .all(|piece| piece.verify_ok && piece.write_error.is_none()));
        server.await.unwrap();
        let mut stored = [0u8; 12];
        storage.read_at(0, &mut stored).await.unwrap();
        assert_eq!(&stored, payload);
    }

    #[tokio::test]
    async fn mirror_missing_one_file_keeps_serving_the_others() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let a = b"aaaaaaaa";
        let b = b"bbbbbbbb";
        let mut piece_hashes = Vec::new();
        piece_hashes.extend_from_slice(crate::core::hash::sha1(a).as_bytes());
        piece_hashes.extend_from_slice(crate::core::hash::sha1(b).as_bytes());
        let file = |name: &str| TorrentMetaInfo {
            path: vec![name.into()],
            length: 8,
            padding: false,
        };
        let info = Arc::new(ValidatedTorrentMetaV1Info {
            name: "root".into(),
            piece_length: 8,
            pieces: piece_hashes.clone(),
            private: false,
            files: vec![file("a"), file("b")],
            single_file_mode: false,
        });
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(FilesystemStorage::new(&info, tmp.path()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 2048];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..n]).to_string();
                let reply: &[u8] = if request.starts_with("GET /root/a ") {
                    b"HTTP/1.1 206 Partial Content\r\nContent-Length: 8\r\nContent-Range: bytes 0-7/8\r\nConnection: close\r\n\r\naaaaaaaa"
                } else {
                    b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                };
                let _ = socket.write_all(reply).await;
            }
        });
        let ctx = WebSeedContext {
            client: build_webseed_client(None).unwrap(),
            bases: Arc::new(vec![format!("http://{addr}/")]),
            disabled: Arc::new(Mutex::new(HashSet::new())),
            disabled_files: Arc::new(Mutex::new(HashSet::new())),
            backoff: Arc::new(Mutex::new(HashMap::new())),
            etags: Arc::new(Mutex::new(HashMap::new())),
            layout: storage.layout().clone(),
            info,
            storage: Arc::clone(&storage),
            verifier: PieceVerifier::V1Sha1 {
                pieces: Arc::new(piece_hashes),
            },
            lengths: Lengths::new(16, 8).unwrap(),
            generation: GenerationToken::new(),
            throttle: Throttle::unlimited(),
        };

        assert!(ctx.pieces_servable(&[1]));
        let missing = fetch_webseed_batch(ctx.clone(), vec![1]).await;
        assert!(!missing.pieces[0].verify_ok);
        assert!(
            ctx.mirror_usable(0),
            "one missing file must not retire the mirror"
        );
        assert!(!ctx.pieces_servable(&[1]));
        assert!(!ctx.pieces_servable(&[0, 1]));
        assert!(ctx.pieces_servable(&[0]));
        let served = fetch_webseed_batch(ctx, vec![0]).await;
        assert!(served.pieces[0].verify_ok);
        server.abort();
    }

    #[test]
    fn disk_queue_limit_holds_at_least_three_pieces() {
        assert_eq!(disk_queue_limit(16 * 1024), MAX_QUEUED_DISK_BYTES);
        let huge = 64 * 1024 * 1024;
        assert_eq!(disk_queue_limit(huge), 3 * u64::from(huge));
        let mut queue = DiskQueue::new(disk_queue_limit(huge));
        queue.queued_bytes = u64::from(huge);
        assert!(!queue.is_full());
    }

    #[test]
    fn webseed_status_classes() {
        for status in [403, 500, 503, 507, 509, 522, 429] {
            assert!(super::super::webseed::is_retryable_status(status));
        }
        for status in [400, 401, 404, 410, 416] {
            assert!(!super::super::webseed::is_retryable_status(status));
        }
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
                    padding: false,
                },
                TorrentMetaInfo {
                    path: vec!["b".into()],
                    length: 15,
                    padding: false,
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
        let (tx, mut rx) = mpsc::channel(16);
        scan_existing_pieces(&verifier, &storage, &lengths, tx).await;
        while let Some(found) = rx.recv().await {
            for index in found {
                piece_tracker.set_local(lengths.validate_piece(index).unwrap(), true);
            }
        }

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
        ] {
            assert!(peer_source_allowed(false, source, addr, &auth));
        }
    }
}

#[cfg(test)]
mod tracker_list_tests {
    use super::*;

    #[test]
    fn split_tracker_lists_dedupes_in_first_seen_order() {
        let lists = ["udp://a:1, udp://b:2\nudp://a:1", "\r\nudp://c:3,udp://b:2"];
        assert_eq!(
            split_tracker_lists(lists),
            ["udp://a:1", "udp://b:2", "udp://c:3"]
        );
        assert!(split_tracker_lists([" , \n"]).is_empty());
    }

    async fn upload_fixture(
        piece: usize,
        payload: &[u8],
    ) -> (
        tempfile::TempDir,
        Arc<FilesystemStorage>,
        Arc<Mutex<PendingUploads>>,
        mpsc::Sender<Message>,
        mpsc::Receiver<Message>,
    ) {
        use crate::bencode::{encode_to_vec, Value};
        let info = Value::Dict(vec![
            (b"length".to_vec(), Value::Int(payload.len() as i64)),
            (b"name".to_vec(), Value::Bytes(b"f.bin".to_vec())),
            (b"piece length".to_vec(), Value::Int(piece as i64)),
            (
                b"pieces".to_vec(),
                Value::Bytes(vec![0; 20 * payload.len().div_ceil(piece)]),
            ),
        ]);
        let bytes = encode_to_vec(&Value::Dict(vec![(b"info".to_vec(), info)]));
        let meta = crate::core::metainfo::parse_torrent(&bytes).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(FilesystemStorage::new(&meta.info, tmp.path()));
        storage.write_at(0, payload).await.unwrap();
        let (tx, rx) = mpsc::channel(64);
        (
            tmp,
            storage,
            Arc::new(Mutex::new(PendingUploads::default())),
            tx,
            rx,
        )
    }

    fn upload_job(
        storage: &Arc<FilesystemStorage>,
        pending: &Arc<Mutex<PendingUploads>>,
        tx: &mpsc::Sender<Message>,
        stats: &Arc<Mutex<TorrentStats>>,
        begin: u32,
        piece_len: u64,
    ) -> UploadJob {
        let key = (0, begin, 16384);
        pending.lock().insert(key);
        UploadJob {
            pending: pending.clone(),
            storage: storage.clone(),
            lane: UploadLane::Piece(tx.clone()),
            stats: stats.clone(),
            upload_tick: Arc::new(AtomicU64::new(0)),
            peer_uploaded: Arc::new(AtomicU64::new(0)),
            peer_uploaded_total: Arc::new(AtomicU64::new(0)),
            limiter: Throttle::unlimited(),
            supports_fast: true,
            key,
            offset: u64::from(begin),
            extent: crate::read_cache::extent_for(0, piece_len, u64::from(begin), 16384),
        }
    }

    #[tokio::test]
    async fn serving_a_whole_piece_costs_one_storage_read_per_window() {
        let piece = 256 * 1024;
        let payload: Vec<u8> = (0..piece).map(|i| (i % 251) as u8).collect();
        let (_tmp, storage, pending, tx, mut rx) = upload_fixture(piece, &payload).await;
        let stats = Arc::new(Mutex::new(TorrentStats::initial(piece as u64, 0, vec![])));
        for blk in 0..16u32 {
            let job = upload_job(&storage, &pending, &tx, &stats, blk * 16384, piece as u64);
            serve_upload(job).await;
            let Some(Message::Piece { begin, data, .. }) = rx.recv().await else {
                panic!("expected a piece");
            };
            assert_eq!(begin, blk * 16384);
            assert_eq!(&data[..], &payload[begin as usize..begin as usize + 16384]);
        }
        assert_eq!(
            storage.read_cache_loads(),
            piece as u64 / crate::read_cache::WINDOW
        );
        assert_eq!(stats.lock().uploaded_bytes, 16 * 16384);
    }

    #[tokio::test]
    async fn cancelled_upload_is_not_sent_or_credited() {
        let piece = 64 * 1024;
        let payload = vec![7u8; piece];
        let (_tmp, storage, pending, tx, mut rx) = upload_fixture(piece, &payload).await;
        let stats = Arc::new(Mutex::new(TorrentStats::initial(piece as u64, 0, vec![])));
        let job = upload_job(&storage, &pending, &tx, &stats, 0, piece as u64);
        let tick = job.upload_tick.clone();
        assert!(pending.lock().remove(&job.key));
        serve_upload(job).await;
        assert!(rx.try_recv().is_err());
        assert_eq!(tick.load(Ordering::Relaxed), 0);
        assert_eq!(storage.read_cache_loads(), 0);
        assert_eq!(stats.lock().uploaded_bytes, 0);
    }
}
