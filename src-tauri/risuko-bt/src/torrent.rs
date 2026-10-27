pub mod stats;

use std::cmp::Reverse;
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

use super::core::{
    build_v2_tables, Id20, Lengths, MerkleProofTable, PieceVerifier, TorrentMeta, VerifyError,
};
use super::external_ip::{ExternalAddresses, ExternalIp, Route, Source};
use super::limiter::{tightest, RateLimiter, Throttle, TorrentLimits};
use super::net::canonical_addr;
use super::peer::{
    connect_prefer_utp, connect_with_utp_fallback, PeerCommand, PeerEvent, PeerEventSink, SpawnPeer,
};
use super::peer_priority::PeerRanker;
use super::piece::{ChunkTracker, PieceTracker};
use super::storage::{FileSet, FilesystemStorage};
use super::tracker::{AnnounceEvent, AnnounceRequest, AnnounceResponse, RetryIn, TrackerError};
use super::utp::UtpSocket;
use super::wire::extended::{
    build_holepunch, holepunch_err, holepunch_type, parse_holepunch, ut_metadata_data,
    ut_metadata_type, ExtHandshake, HolepunchMsg, EXT_HANDSHAKE_ID, EXT_NAME_UT_HOLEPUNCH,
    EXT_NAME_UT_METADATA, EXT_NAME_UT_PEX,
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
const MAX_PEX_BACKLOG: usize = MAX_PEER_BACKLOG * 3 / 4;
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
const TRACKER_SHUTDOWN_GRACE: Duration =
    super::tracker::STOPPED_ANNOUNCE_TIMEOUT.saturating_add(Duration::from_secs(1));
const TRACKER_MAX_CONCURRENT_ANNOUNCES: usize = 8;
const TRACKER_MIN_INTERVAL: Duration = Duration::from_secs(60);
const TRACKER_MAX_INTERVAL: Duration = Duration::from_secs(2 * 60 * 60);
const TRACKER_TRANSPORT_RETRY_MAX: Duration = Duration::from_secs(10 * 60);

const PEER_IDLE_TIMEOUT: Duration = Duration::from_secs(180);

const SNUB_EVICTION_TIMEOUT: Duration = Duration::from_secs(60);
const PEER_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(5);

const PEER_QUALITY_INTERVAL: Duration = Duration::from_secs(15);
const PEER_USELESS_AFTER: Duration = Duration::from_secs(120);
const MAX_QUALITY_EVICTIONS: usize = 2;
const EVICTION_TIE_WINDOW: Duration = PEER_QUALITY_INTERVAL;
const MAX_DHT_BURST: usize = 128;

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
const DHT_LOOKUP_BUDGET: Duration = Duration::from_secs(60);
const DHT_RELAXED_INTERVAL: Duration = Duration::from_secs(15 * 60);
const DHT_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(15 * 60);
const PEX_INTERVAL: Duration = Duration::from_secs(60);
const PEX_MIN_INBOUND_GAP: Duration = Duration::from_secs(20);
const MAX_PEX_ADDED_PER_MSG: usize = 50;
const PEX_ACCEPT_BURST: u32 = 200;
const PEX_ACCEPT_REFILL: Duration = Duration::from_secs(6);

const OUR_UT_METADATA_ID: u8 = 3;
const OUR_UT_PEX_ID: u8 = 4;
const OUR_UT_HOLEPUNCH_ID: u8 = 5;
const MAX_PEX_SOURCE_ENTRIES: usize = 4096;
const MAX_HOLEPUNCH_ENTRIES: usize = 4096;
const HOLEPUNCH_TTL: Duration = Duration::from_secs(300);
const HOLEPUNCH_UNANSWERED_TTL: Duration = Duration::from_secs(20);
const HOLEPUNCH_CONNECT_BURST: u32 = 6;
const HOLEPUNCH_CONNECT_REFILL: Duration = Duration::from_secs(10);
const HOLEPUNCH_UNSOLICITED_BURST: u32 = 8;
const HOLEPUNCH_UNSOLICITED_REFILL: Duration = Duration::from_secs(5);
const MAX_IP_BUDGETS: usize = 1024;
const META_PIECE_SIZE: usize = 16 * 1024;
const MAX_FAST_PIECES: usize = 10;
const PIECE_FRAME_OVERHEAD: usize = 13;
const MIN_PIECE_FRAME_COST: usize = 64;
const MAX_LEAF_HASH_JOBS: usize = 4;
const MAX_PEER_QUEUED_LEAF_JOBS: usize = 32;
const LEAF_HASH_CACHE_BYTES: usize = 4 * 1024 * 1024;
const LEAF_HASH_READ_BPS: u64 = 64 * 1024 * 1024;
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
const MAX_TRACKER_AUTHORIZED: usize = 4096;

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

fn authorize_tracker_peer(
    authorized: &mut HashSet<SocketAddr>,
    known_addrs: &HashSet<SocketAddr>,
    addr: SocketAddr,
) {
    if authorized.len() >= MAX_TRACKER_AUTHORIZED && !authorized.contains(&addr) {
        authorized.retain(|kept| known_addrs.contains(kept));
        if authorized.len() >= MAX_TRACKER_AUTHORIZED {
            authorized.clear();
        }
    }
    authorized.insert(addr);
}

fn peer_source_allowed(
    private_torrent: bool,
    source: PeerSource,
    addr: SocketAddr,
    tracker_authorized: &HashSet<SocketAddr>,
) -> bool {
    !private_torrent || source == PeerSource::Tracker || tracker_authorized.contains(&addr)
}

fn advertised_listen_addr(
    private_torrent: bool,
    ip: std::net::IpAddr,
    port: u16,
    tracker_authorized: &HashSet<SocketAddr>,
) -> Option<SocketAddr> {
    let addr = SocketAddr::new(ip, port);
    (port != 0 && (!private_torrent || tracker_authorized.contains(&addr))).then_some(addr)
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
) -> (Option<Vec<usize>>, Result<(), String>, Vec<usize>) {
    let selected = selected_file_set(only_files.as_deref());
    let mut failed = HashSet::new();
    let mut promoted = Vec::new();
    let mut errors = Vec::new();
    for idx in storage.set_selection(selected.as_ref()).await {
        match storage.promote_file(idx).await {
            Ok(()) => promoted.push(idx),
            Err(e) => {
                let path = &storage.layout().files()[idx].path;
                errors.push(format!("{} ({e})", path.display()));
                failed.insert(idx);
            }
        }
    }
    if failed.is_empty() {
        return (only_files, Ok(()), promoted);
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
        promoted,
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
            holepunch: holepunch_supported(init.meta.info.private, init.utp.as_ref(), listen_port),
            prefers_encryption: init.encryption != super::peer::EncryptionPolicy::PlaintextOnly,
        };
        let upload_only_flag = Arc::clone(&upload_only_flag);
        std::sync::Arc::new(move |peer_ip: std::net::IpAddr| {
            let hs = ext.build(
                peer_ip,
                upload_only_flag.load(Ordering::Relaxed).then_some(true),
            );
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
    holepunch: bool,
    prefers_encryption: bool,
}

impl OurExtHandshake {
    fn build(&self, peer_ip: IpAddr, upload_only: Option<bool>) -> ExtHandshake {
        let mut hs =
            ExtHandshake::new_outgoing(OUR_UT_METADATA_ID, OUR_UT_PEX_ID, Some(self.metadata_size))
                .with_yourip(peer_ip)
                .with_port(self.listen_port)
                .with_reqq(UPLOAD_REQQ)
                .with_encryption_preference(self.prefers_encryption);
        if self.private {
            hs.supported.remove(b"ut_pex".as_slice());
        }
        if self.holepunch {
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

#[derive(Debug, Clone)]
struct TokenBucket {
    tokens: f32,
    refilled: Instant,
    cap: f32,
    per_token: Duration,
}

impl TokenBucket {
    fn new(cap: u32, per_token: Duration, now: Instant) -> Self {
        Self {
            tokens: cap as f32,
            refilled: now,
            cap: cap as f32,
            per_token,
        }
    }

    fn take(&mut self, want: u32, now: Instant) -> u32 {
        let elapsed = now.saturating_duration_since(self.refilled);
        self.refilled = self.refilled.max(now);
        self.tokens =
            (self.tokens + elapsed.as_secs_f32() / self.per_token.as_secs_f32()).min(self.cap);
        let granted = (self.tokens as u32).min(want);
        self.tokens -= granted as f32;
        granted
    }

    fn is_full(&self, now: Instant) -> bool {
        let elapsed = now.saturating_duration_since(self.refilled);
        self.tokens + elapsed.as_secs_f32() / self.per_token.as_secs_f32() >= self.cap
    }
}

struct IpBudget {
    pex: TokenBucket,
    connect: TokenBucket,
    last_pex_in: Option<Instant>,
    touched: Instant,
}

impl IpBudget {
    fn new(now: Instant) -> Self {
        Self {
            pex: TokenBucket::new(PEX_ACCEPT_BURST, PEX_ACCEPT_REFILL, now),
            connect: TokenBucket::new(HOLEPUNCH_CONNECT_BURST, HOLEPUNCH_CONNECT_REFILL, now),
            last_pex_in: None,
            touched: now,
        }
    }

    fn open_pex_gap(&mut self, now: Instant) -> bool {
        if self
            .last_pex_in
            .is_some_and(|last| now.saturating_duration_since(last) < PEX_MIN_INBOUND_GAP)
        {
            return false;
        }
        self.last_pex_in = Some(now);
        true
    }

    fn is_settled(&self, now: Instant) -> bool {
        self.pex.is_full(now)
            && self.connect.is_full(now)
            && self
                .last_pex_in
                .is_none_or(|last| now.saturating_duration_since(last) >= PEX_MIN_INBOUND_GAP)
    }
}

fn evict_oldest<K: Copy + Eq + std::hash::Hash, V>(
    map: &mut HashMap<K, V>,
    cap: usize,
    age: impl Fn(&V) -> Instant,
) {
    if map.len() < cap {
        return;
    }
    if let Some(oldest) = map.iter().min_by_key(|(_, v)| age(v)).map(|(k, _)| *k) {
        map.remove(&oldest);
    }
}

#[derive(Default)]
struct IpBudgets {
    entries: HashMap<IpAddr, IpBudget>,
}

impl IpBudgets {
    fn entry(&mut self, ip: IpAddr, now: Instant) -> &mut IpBudget {
        let ip = ip.to_canonical();
        if self.entries.len() >= MAX_IP_BUDGETS && !self.entries.contains_key(&ip) {
            self.prune(now);
            evict_oldest(&mut self.entries, MAX_IP_BUDGETS, |budget| budget.touched);
        }
        let budget = self.entries.entry(ip).or_insert_with(|| IpBudget::new(now));
        budget.touched = now;
        budget
    }

    fn prune(&mut self, now: Instant) {
        self.entries.retain(|_, budget| !budget.is_settled(now));
    }
}

#[derive(Debug, Clone, Copy)]
struct HolepunchAttempt {
    at: Instant,
    relay: u32,
    answered: bool,
}

#[derive(Debug)]
struct HolepunchState {
    enabled: bool,
    v6: bool,
    own_ports: [u16; 3],
    initiated: HashMap<SocketAddr, HolepunchAttempt>,
    targets: HashMap<SocketAddr, Instant>,
    unsolicited: TokenBucket,
}

impl Default for HolepunchState {
    fn default() -> Self {
        Self::new(false, 0, None)
    }
}

impl HolepunchState {
    fn new(enabled: bool, listen_port: u16, utp_port: Option<u16>) -> Self {
        Self {
            enabled,
            v6: false,
            own_ports: [listen_port, utp_port.unwrap_or(0), 0],
            initiated: HashMap::new(),
            targets: HashMap::new(),
            unsolicited: TokenBucket::new(
                HOLEPUNCH_UNSOLICITED_BURST,
                HOLEPUNCH_UNSOLICITED_REFILL,
                Instant::now(),
            ),
        }
    }

    fn with_v6_port(mut self, port: Option<u16>) -> Self {
        self.v6 = port == Some(self.own_ports[0]);
        self.own_ports[2] = port.unwrap_or(0);
        self
    }

    fn can_initiate(&self, target: &SocketAddr, now: Instant) -> bool {
        (self.v6 || !target.ip().to_canonical().is_ipv6())
            && self.initiated.get(target).is_none_or(|attempt| {
                let ttl = if attempt.answered {
                    HOLEPUNCH_TTL
                } else {
                    HOLEPUNCH_UNANSWERED_TTL
                };
                now.saturating_duration_since(attempt.at) >= ttl
            })
    }

    fn admit_unsolicited(&mut self, now: Instant) -> bool {
        self.unsolicited.take(1, now) == 1
    }

    fn note_initiated(&mut self, target: SocketAddr, relay: u32, now: Instant) {
        if self.initiated.len() >= MAX_HOLEPUNCH_ENTRIES && !self.initiated.contains_key(&target) {
            self.prune(now);
            evict_oldest(&mut self.initiated, MAX_HOLEPUNCH_ENTRIES, |attempt| {
                attempt.at
            });
        }
        self.initiated.insert(
            target,
            HolepunchAttempt {
                at: now,
                relay,
                answered: false,
            },
        );
    }

    fn take_answer(&mut self, target: &SocketAddr, relay: u32, now: Instant) -> bool {
        let open = |attempt: &HolepunchAttempt| {
            attempt.relay == relay
                && !attempt.answered
                && now.saturating_duration_since(attempt.at) < HOLEPUNCH_TTL
        };
        let key = match self.initiated.get(target) {
            Some(attempt) if open(attempt) => *target,
            _ => {
                let ip = target.ip().to_canonical();
                let matched = self
                    .initiated
                    .iter()
                    .filter(|(addr, attempt)| addr.ip().to_canonical() == ip && open(attempt))
                    .min_by_key(|(_, attempt)| attempt.at)
                    .map(|(addr, _)| *addr);
                match matched {
                    Some(addr) => addr,
                    None => return false,
                }
            }
        };
        self.initiated
            .get_mut(&key)
            .map(|attempt| attempt.answered = true)
            .is_some()
    }

    fn note_target(&mut self, target: SocketAddr, now: Instant) {
        if self.targets.len() >= MAX_HOLEPUNCH_ENTRIES && !self.targets.contains_key(&target) {
            self.prune(now);
            evict_oldest(&mut self.targets, MAX_HOLEPUNCH_ENTRIES, |at| *at);
        }
        self.targets.insert(target, now);
    }

    fn is_target(&self, target: &SocketAddr, now: Instant) -> bool {
        self.targets
            .get(target)
            .is_some_and(|at| now.saturating_duration_since(*at) < HOLEPUNCH_TTL)
    }

    fn is_own_endpoint(
        &self,
        addr: SocketAddr,
        their_view_of_us: Option<IpAddr>,
        elected: &ExternalAddresses,
    ) -> bool {
        addr.port() != 0
            && self.own_ports.contains(&addr.port())
            && (their_view_of_us.is_some_and(|ip| ip.to_canonical() == addr.ip().to_canonical())
                || elected.contains(addr.ip()))
    }

    fn drop_relay(&mut self, relay: u32) {
        self.initiated
            .retain(|_, attempt| attempt.relay != relay || attempt.answered);
    }

    fn forget(&mut self, addr: &SocketAddr) {
        self.initiated.remove(addr);
        self.targets.remove(addr);
    }

    fn retain(&mut self, mut keep: impl FnMut(&SocketAddr) -> bool) {
        self.initiated.retain(|addr, _| keep(addr));
        self.targets.retain(|addr, _| keep(addr));
    }

    fn clear(&mut self) {
        self.initiated.clear();
        self.targets.clear();
    }

    fn prune(&mut self, now: Instant) {
        self.initiated
            .retain(|_, attempt| now.saturating_duration_since(attempt.at) < HOLEPUNCH_TTL);
        self.targets
            .retain(|_, at| now.saturating_duration_since(*at) < HOLEPUNCH_TTL);
    }
}

fn holepunch_supported(private: bool, utp: Option<&Arc<UtpSocket>>, listen_port: u16) -> bool {
    !private && utp.is_some_and(|socket| socket.local_addr().port() == listen_port)
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
    their_yourip: Option<IpAddr>,
    route: Route,
    listen_addr: Option<SocketAddr>,
    link: PeerLink,
    prefers_encryption: bool,
    upload_only: bool,
    ext_seen: bool,
    pending_haves: Vec<u32>,
    outbound: bool,
    supports_fast: bool,
    initial_availability: Option<InitialAvailability>,
    suggested_pieces: HashSet<u32>,
    remote_allowed_fast: HashSet<u32>,
    choke_rejected: HashSet<(u32, u32)>,
    reject_backoff: HashMap<u32, Instant>,
    sent_allowed_fast: HashSet<u32>,
    fast_served: HashMap<u32, u64>,
    leaf_slots: Arc<Semaphore>,
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
        let connected_at = Instant::now();
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
            connected_at,
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
            their_yourip: None,
            route: Route::Direct,
            listen_addr: outbound.then_some(addr),
            link: PeerLink::default(),
            prefers_encryption: false,
            upload_only: false,
            ext_seen: false,
            pending_haves: Vec::new(),
            outbound,
            supports_fast,
            initial_availability: None,
            suggested_pieces: HashSet::new(),
            remote_allowed_fast: HashSet::new(),
            choke_rejected: HashSet::new(),
            reject_backoff: HashMap::new(),
            sent_allowed_fast: HashSet::new(),
            fast_served: HashMap::new(),
            leaf_slots: Arc::new(Semaphore::new(MAX_PEER_QUEUED_LEAF_JOBS)),
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

    fn back_off_piece(&mut self, index: u32) {
        let now = Instant::now();
        if self.reject_backoff.len() >= MAX_CHOKE_REJECTED {
            self.reject_backoff.retain(|_, until| *until > now);
        }
        if self.reject_backoff.len() < MAX_CHOKE_REJECTED {
            self.reject_backoff
                .insert(index, now + REJECT_RETRY_BACKOFF);
        }
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
const REJECT_RETRY_BACKOFF: Duration = Duration::from_secs(20);
const MAX_PENDING_UPLOAD_BYTES: u64 = 8 * 1024 * 1024;
const UPLOAD_REQQ: u32 = (MAX_PENDING_UPLOAD_BYTES / (16 * 1024)) as u32;
const MAX_PENDING_UPLOAD_REQUESTS: usize = UPLOAD_REQQ as usize;
const MAX_REQUEST_LEN: u32 = 128 * 1024;

#[derive(Default)]
struct PendingUploads {
    keys: HashSet<(u32, u32, u32)>,
    live: usize,
    live_bytes: u64,
    owed: VecDeque<(u32, u32, u32)>,
}

impl PendingUploads {
    fn admit(&mut self, key: (u32, u32, u32)) -> bool {
        let full = self.live >= MAX_PENDING_UPLOAD_REQUESTS
            || self.live_bytes + u64::from(key.2) > MAX_PENDING_UPLOAD_BYTES;
        !self.keys.contains(&key) && !full && self.insert(key)
    }

    fn insert(&mut self, key: (u32, u32, u32)) -> bool {
        let added = self.keys.insert(key);
        if added {
            self.live += 1;
            self.live_bytes += u64::from(key.2);
        }
        added
    }

    fn reserve_deferred(&mut self) -> bool {
        if self.live >= MAX_PENDING_UPLOAD_REQUESTS {
            return false;
        }
        self.live += 1;
        true
    }

    fn remove(&mut self, key: &(u32, u32, u32)) -> bool {
        self.keys.remove(key)
    }

    fn contains(&self, key: &(u32, u32, u32)) -> bool {
        self.keys.contains(key)
    }

    fn retain(&mut self, keep: impl FnMut(&(u32, u32, u32)) -> bool) {
        self.keys.retain(keep);
    }
}

struct LiveUpload {
    pending: Arc<Mutex<PendingUploads>>,
    bytes: u64,
}

impl Drop for LiveUpload {
    fn drop(&mut self) {
        let mut pending = self.pending.lock();
        pending.live = pending.live.saturating_sub(1);
        pending.live_bytes = pending.live_bytes.saturating_sub(self.bytes);
    }
}

struct VerifyResult {
    piece_index: u32,
    write_error: Option<String>,
    verify_ok: bool,
    from_webseed: bool,
    hash_conflict: bool,
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

fn chunk_digests(buf: &[u8], sources: &[Option<ChunkSource>]) -> Vec<ChunkDigest> {
    buf.chunks(super::core::CHUNK_SIZE as usize)
        .zip(sources.iter().copied())
        .map(|(chunk, source)| {
            (
                source.and_then(ChunkSource::peer),
                *crate::core::hash::sha1(chunk).as_bytes(),
            )
        })
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
        if self.ban_after.is_none() || vr.hash_conflict {
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
        let halt = match &result.write_error {
            Some(error) => {
                self.write_failures = self.write_failures.saturating_add(1);
                (self.write_failures >= MAX_WRITE_FAILURES)
                    .then(|| format!("Disk write failed: {error}"))
            }
            None if result.hash_conflict => Some(format!(
                "Piece {} matches its v1 hash but not its v2 hash; the torrent's hashes disagree",
                result.piece_index
            )),
            None if result.verify_ok => {
                self.write_failures = 0;
                None
            }
            None => None,
        };
        if halt.is_some() && self.halted.is_none() {
            self.halted = halt;
            return self.halted.clone();
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChunkSource {
    Padding,
    Peer(IpAddr),
}

impl ChunkSource {
    fn peer(self) -> Option<IpAddr> {
        match self {
            Self::Peer(ip) => Some(ip),
            Self::Padding => None,
        }
    }
}

struct PieceAssembly {
    buf: Vec<u8>,
    sources: Vec<Option<ChunkSource>>,
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
        if let Some(tables) = verifier.serving_tables() {
            (true, Some(tables))
        } else if matches!(verifier, PieceVerifier::Hybrid { .. }) {
            tracing::warn!("hybrid torrent {info_hash}: v2 hashes unusable for serving");
            (false, None)
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
    let leaf_pool = LeafHashPool::new(MAX_LEAF_HASH_JOBS, upload_limiter.clone());
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
    let mut piece_tracker = PieceTracker::new(lengths).with_order_salt(rand::rng().random::<u64>());
    let (mut only_files, applied, _) =
        apply_storage_selection(&storage, init.only_files.clone()).await;
    if let Err(e) = applied {
        tracing::warn!("file selection for {info_hash}: {e}");
    }
    piece_tracker.set_wanted(wanted_pieces(
        storage.layout(),
        &lengths,
        only_files.as_deref(),
    ));
    let mut chunk_tracker =
        ChunkTracker::new(lengths).with_padding(storage.layout().padding_ranges());
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
    let (mut tracker_tiers, obfuscated_tiers) = collect_announce_tiers(&init.meta);
    let mut tracker_urls = flatten_tracker_tiers(&tracker_tiers);
    let tracker_info_hashes = announce_hashes.clone();
    let tracker_key = rand::rng().random::<u32>();
    let tracker_state = Arc::new(Mutex::new(TrackerLifecycle::default()));
    let (tracker_tier_tx, mut tracker_tier_rx) = mpsc::channel::<oneshot::Sender<()>>(4);
    let tracker_ctx = TrackerCtx {
        tx: peer_src_tx.clone(),
        peer_id: our_peer_id,
        key: tracker_key,
        port: listen_port,
        stats: Arc::clone(&stats),
        proxy: None,
        lifecycle: Arc::clone(&tracker_state),
        tier_change_tx: private_torrent.then_some(tracker_tier_tx.clone()),
        source_addr: init.tracker_source_addr,
        generation: generation.clone(),
        obfuscated_tiers,
        ranker_for: PeerRanker::observed,
    };
    let mut tracker_tasks = TrackerPollers::idle();

    let (peer_event_tx, mut peer_event_rx) = mpsc::channel::<(u32, PeerEvent)>(8192);
    let mut conn_lease = super::conn_budget::BudgetLease::new(init.conn_budget.clone());

    let mut peers: HashMap<u32, Peer> = HashMap::new();
    let mut next_pid: u32 = 1;
    let mut known_addrs: HashSet<SocketAddr> = HashSet::new();
    let mut tracker_authorized: HashSet<SocketAddr> = HashSet::new();
    let mut pex_source: HashMap<SocketAddr, u32> = HashMap::new();
    let mut holepunch = HolepunchState::new(
        holepunch_supported(private_torrent, utp.as_ref(), listen_port),
        listen_port,
        utp.as_ref().map(|socket| socket.local_addr().port()),
    )
    .with_v6_port(
        utp.as_ref()
            .and_then(|socket| socket.v6_local_addr())
            .map(|addr| addr.port()),
    );
    let mut ip_budgets = IpBudgets::default();
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
    let mut inbound_evictions: Vec<u32> = Vec::new();
    let mut last_optimistic_rotate = Instant::now();
    let mut optimistic_pid: Option<u32> = None;
    let mut choke_dirty = false;
    let mut last_finished = false;
    let our_ext = OurExtHandshake {
        listen_port,
        metadata_size: init.meta.info_bytes.len() as u64,
        private: private_torrent,
        holepunch: holepunch.enabled,
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
    let mut tracker_stop: Option<tokio::task::JoinHandle<()>> = None;
    let mut dht_poll_handle: Option<tokio::task::JoinHandle<()>> = dht.clone().map(|initial_dht| {
        initial_dht.set_private(info_hash, private_torrent);
        initial_dht.add_bootstrap_nodes(init.meta.bootstrap_nodes.clone());
        initial_dht.add_bootstrap_hosts(init.meta.bootstrap_hosts.clone());
        spawn_dht_poller(
            initial_dht,
            announce_hashes.clone(),
            listen_port,
            Route::of(p2p_proxy.as_ref()),
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
                        authorize_tracker_peer(&mut tracker_authorized, &known_addrs, addr);
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
                            &holepunch,
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
                    let rejected = blocklist.read().contains(addr.ip())
                        || matches!(duplicate, DuplicateAction::RejectNew);
                    let admitted = (!rejected
                        && !paused
                        && !inbound_addr_taken(
                            addr,
                            &known_addrs,
                            &peers,
                            &pending_dials,
                            &holepunch,
                            Instant::now(),
                        )
                        && super::magnet::is_dialable_peer_addr(addr))
                    .then(|| {
                        inbound_room(
                            &peers,
                            &mut inbound_evictions,
                            max_peers,
                            paused,
                            RecycleScope {
                                complete: piece_tracker.is_complete(),
                                now: Instant::now(),
                                total_pieces: lengths.total_pieces() as usize,
                                ranker: &PeerRanker::observed(Route::of(p2p_proxy.as_ref()), listen_port),
                            },
                        )
                    })
                    .filter(|room| !room.crowded || room.victim.is_some())
                    .filter(|_| conn_lease.try_reserve());
                    if rejected {
                        io_abort.abort();
                        let _ = cmd_tx.try_send(PeerCommand::Disconnect);
                    } else if let Some(InboundRoom { victim, .. }) = admitted {
                        inbound_evictions.extend(victim);
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
                            Route::of(p2p_proxy.as_ref()),
                        )
                        .await;
                        unqueue_peer_addr(
                            addr,
                            &mut priority_backlog,
                            &mut peer_backlog,
                            &mut dial_retries,
                            &mut useful_redials,
                        );
                    } else {
                        if utp && (!private_torrent || tracker_authorized.contains(&addr)) {
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
                    let mut stopping = std::mem::replace(&mut tracker_tasks, TrackerPollers::idle());
                    stopping.signal_shutdown(true);
                    let previous = tracker_stop.take();
                    let done = tracker_state.lock().begin_stop();
                    tracker_stop = Some(tokio::spawn(async move {
                        let _done = done;
                        if let Some(previous) = previous {
                            let _ = previous.await;
                        }
                        stopping.join().await;
                    }));
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
                    holepunch.clear();
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
                    {
                        let mut lifecycle = tracker_state.lock();
                        lifecycle.retry_failed_now();
                        lifecycle.announce_now(false);
                    }
                    if !scanning && tracker_tasks.is_empty() {
                        tracker_tasks = spawn_tracker_tier_pollers(
                            TrackerCtx {
                                proxy: p2p_proxy.clone(),
                                ..tracker_ctx.clone()
                            },
                            tracker_tiers.clone(),
                            &tracker_info_hashes,
                        );
                    }
                    if dht_poll_handle.is_none() {
                        if let Some(dht) = dht.clone() {
                            dht_poll_handle = Some(spawn_dht_poller(
                                dht,
                                announce_hashes.clone(),
                                listen_port,
                                Route::of(p2p_proxy.as_ref()),
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
                        &holepunch,
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
                        if !paused {
                            dht_poll_handle = Some(spawn_dht_poller(
                                next_dht,
                                announce_hashes.clone(),
                                listen_port,
                                Route::of(p2p_proxy.as_ref()),
                                peer_src_tx.clone(),
                                generation.clone(),
                                Arc::clone(&dht_relaxed),
                            ));
                        }
                    }
                    if replace_proxy {
                        tracker_tasks.shutdown(false).await;
                        tracker_state.lock().retry_failed_now();
                        if !paused && !scanning {
                            tracker_tasks = spawn_tracker_tier_pollers(
                                TrackerCtx {
                                    proxy: p2p_proxy.clone(),
                                    ..tracker_ctx.clone()
                                },
                                tracker_tiers.clone(),
                                &tracker_info_hashes,
                            );
                        }
                    }
                    let _ = ack.send(());
                }
                TorrentCommand::SetOnlyFiles { files, ack } => {
                    let applied;
                    let promoted;
                    (only_files, applied, promoted) =
                        apply_storage_selection(&storage, files).await;
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
                    if !scanning {
                        let file_progress = Arc::clone(&stats.lock().file_progress);
                        storage
                            .settle_promoted_files(&promoted, &file_progress)
                            .await;
                    }
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
                    let new_urls = if private_torrent {
                        Vec::new()
                    } else {
                        normalize_tracker_urls(urls, &tracker_urls)
                    };
                    let added = new_urls.len();
                    for url in &new_urls {
                        tracker_urls.push(url.clone());
                    }
                    if !new_urls.is_empty() {
                        tracker_tiers.push(new_urls.clone());
                        tracker_state.lock().append_tier(&new_urls);
                        if tracker_tasks.is_empty() && !(paused || scanning) {
                            tracker_tasks = spawn_tracker_tier_pollers(
                                TrackerCtx {
                                    proxy: p2p_proxy.clone(),
                                    ..tracker_ctx.clone()
                                },
                                tracker_tiers.clone(),
                                &tracker_info_hashes,
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
                        &mut holepunch,
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
                    authorize_tracker_peer(&mut tracker_authorized, &known_addrs, addr);
                }
                if !peer_source_allowed(private_torrent, source, addr, &tracker_authorized) {
                    continue;
                }
                let known_useful = useful_peers.contains_key(&addr);
                if queue_sourced_candidate(
                    addr,
                    source,
                    known_useful,
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
                        &holepunch,
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
            Some(tier_ack) = tracker_tier_rx.recv(), if private_torrent => {
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
                let _ = tier_ack.send(());
            }
            found = recv_scan_batch(&mut scan_rx) => {
                match found {
                    Some(found) => {
                        let mut newly_local = Vec::with_capacity(found.len());
                        let mut completed_files = Vec::new();
                        {
                            let mut s = stats.lock();
                            for index in found {
                                let Ok(vpi) = lengths.validate_piece(index) else {
                                    continue;
                                };
                                if mark_verified_piece_local(&mut piece_tracker, vpi) {
                                    completed_files.extend(add_piece_progress(
                                        &mut s,
                                        &lengths,
                                        storage.layout(),
                                        vpi,
                                    ));
                                    newly_local.push(index);
                                }
                            }
                            s.left_bytes = piece_tracker.bytes_left();
                        }
                        storage.files_completed(&completed_files).await;
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
                                    TrackerCtx {
                                        proxy: p2p_proxy.clone(),
                                        ..tracker_ctx.clone()
                                    },
                                    tracker_tiers.clone(),
                                    &tracker_info_hashes,
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
                                &holepunch,
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
                    &mut pex_source, &mut holepunch, &mut ip_budgets,
                    &peer_src_tx,
                    &generation,
                    &verifier,
                    failed_copies.as_ref(),
                    &info_bytes,
                    hash_tables.as_ref(),
                    &leaf_pool,
                    pipeline_floor,
                    pipeline_cap,
                    max_peers,
                    &mut choke_dirty,
                    &upload_limiter,
                    dht.as_ref(),
                    &blocklist,
                    &tracker_authorized,
                    Route::of(p2p_proxy.as_ref()),
                    listen_port,
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
                                tracing::error!("torrent {info_hash}: downloading stopped: {error}");
                                stats.lock().error = Some(error);
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
                                tracing::error!("torrent {info_hash}: downloading stopped: {error}");
                                stats.lock().error = Some(error);
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
                        &holepunch,
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
                    for pid in inbound_evictions.drain(..) {
                        if peers.contains_key(&pid) && !to_evict.contains(&pid) {
                            recycled.insert(pid);
                            to_evict.push(pid);
                        }
                    }
                    if now.duration_since(last_quality_pass) >= PEER_QUALITY_INTERVAL {
                        last_quality_pass = now;
                        let candidates_waiting =
                            !priority_backlog.is_empty() || !peer_backlog.is_empty();
                        if candidates_waiting && peers.len() * 10 >= max_peers * 9 {
                            let scope = RecycleScope {
                                complete: piece_tracker.is_complete(),
                                now,
                                total_pieces: lengths.total_pieces() as usize,
                                ranker: &PeerRanker::observed(Route::of(p2p_proxy.as_ref()), listen_port),
                            };
                            for pid in redundant_peers(&peers, scope, MAX_QUALITY_EVICTIONS) {
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
                            holepunch.drop_relay(pid);
                            holepunch.forget(&p.addr);
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
                            payload: our_ext.build(p.addr.ip(), Some(finished_now)).encode(),
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
                    flush_owed_rejects(p);
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
                    holepunch.prune(now);
                    ip_budgets.prune(now);
                    send_pex_updates(&mut peers, lengths.total_pieces() as usize);
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
                                    client: p.client.clone().or_else(|| {
                                        p.peer_id.and_then(|id| {
                                            super::core::peer_id::client_name_from_peer_id(&id.0)
                                        })
                                    }),
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
                        &holepunch,
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
    holepunch.clear();

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
    let pending_stop = async {
        if let Some(stopping) = tracker_stop.take() {
            let _ = stopping.await;
        }
    };
    tokio::join!(tracker_tasks.shutdown(true), pending_stop, drain_writes);

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

fn dht_announce_due(last_announce: Option<Instant>, now: Instant) -> bool {
    last_announce.is_none_or(|at| now.saturating_duration_since(at) >= DHT_ANNOUNCE_INTERVAL)
}

fn dht_announce_targets(
    info_hashes: &[Id20],
    last_announce: &[Option<Instant>],
    listen_port: u16,
    now: Instant,
) -> Vec<(Id20, Option<u16>)> {
    info_hashes
        .iter()
        .zip(last_announce)
        .map(|(&info_hash, &at)| (info_hash, dht_announce_due(at, now).then_some(listen_port)))
        .collect()
}

fn record_dht_announces(
    last_announce: &mut [Option<Instant>],
    targets: &[(Id20, Option<u16>)],
    announced: &[bool],
    now: Instant,
) {
    for ((last, (_, port)), &sent) in last_announce.iter_mut().zip(targets).zip(announced) {
        if sent && port.is_some() {
            *last = Some(now);
        }
    }
}

async fn dht_poll_round(
    targets: &[(Id20, Option<u16>)],
    peer_tx: &mpsc::Sender<PeerEnvelope>,
    generation: &GenerationToken,
    order: impl Fn(&mut [SocketAddr]),
    lookup: impl Fn(Id20, Option<u16>) -> (mpsc::UnboundedReceiver<SocketAddr>, oneshot::Receiver<bool>),
) -> Option<Vec<bool>> {
    let mut announced = Vec::with_capacity(targets.len());
    let mut burst = Vec::new();
    for &(info_hash, announce_port) in targets {
        let (mut peers, sent) = lookup(info_hash, announce_port);
        while let Some(addr) = peers.recv().await {
            burst.push(addr);
            while burst.len() < MAX_DHT_BURST {
                let Ok(next) = peers.try_recv() else { break };
                burst.push(next);
            }
            order(&mut burst);
            for addr in burst.drain(..) {
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
                    return None;
                }
            }
        }
        announced.push(sent.await.unwrap_or(false));
    }
    Some(announced)
}

fn spawn_dht_poller(
    dht: Arc<super::dht::Dht>,
    info_hashes: Vec<Id20>,
    listen_port: u16,
    route: Route,
    peer_tx: mpsc::Sender<PeerEnvelope>,
    generation: GenerationToken,
    relaxed: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_announce: Vec<Option<Instant>> = vec![None; info_hashes.len()];
        loop {
            let targets =
                dht_announce_targets(&info_hashes, &last_announce, listen_port, Instant::now());
            let Some(announced) = dht_poll_round(
                &targets,
                &peer_tx,
                &generation,
                |burst| PeerRanker::observed(route, listen_port).order_batch(burst),
                |info_hash, port| dht.get_peers_announced(info_hash, DHT_LOOKUP_BUDGET, port),
            )
            .await
            else {
                return;
            };
            record_dht_announces(&mut last_announce, &targets, &announced, Instant::now());
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

    pub struct PeerLane {
        pub tx: mpsc::Sender<PeerCommand>,
        pub piece_tx: Option<mpsc::Sender<Message>>,
        pub addr: SocketAddr,
        pub io_abort: Option<AbortHandle>,
        pub route: Route,
    }

    struct RegistryEntry {
        scope: Arc<()>,
        lane: PeerLane,
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
        put_with_lane(
            torrent_id,
            pid,
            scope,
            PeerLane {
                tx,
                piece_tx: None,
                addr,
                io_abort,
                route: Route::Direct,
            },
        );
    }

    pub fn put_with_lane(torrent_id: usize, pid: u32, scope: &Arc<()>, lane: PeerLane) {
        REG.lock().unwrap().insert(
            key(scope, torrent_id, pid),
            RegistryEntry {
                scope: scope.clone(),
                lane,
            },
        );
    }

    pub fn take(
        torrent_id: usize,
        pid: u32,
        scope: &Arc<()>,
    ) -> Option<(mpsc::Sender<PeerCommand>, SocketAddr, Option<AbortHandle>)> {
        take_with_lane(torrent_id, pid, scope).map(|lane| (lane.tx, lane.addr, lane.io_abort))
    }

    pub fn take_with_lane(torrent_id: usize, pid: u32, scope: &Arc<()>) -> Option<PeerLane> {
        REG.lock()
            .unwrap()
            .remove(&key(scope, torrent_id, pid))
            .map(|entry| entry.lane)
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
                    .map(|entry| (key.2, entry.lane.tx, entry.lane.addr, entry.lane.io_abort))
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
    let route = Route::of(proxy.as_ref());
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
                peer_registry::PeerLane {
                    tx: handle.tx.clone(),
                    piece_tx: Some(handle.piece_tx.clone()),
                    addr: handle.addr,
                    io_abort: Some(handle.io_abort.clone()),
                    route,
                },
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
    match addr.ip().to_canonical() {
        IpAddr::V4(v4) => {
            let octets = v4.octets();
            seed.extend_from_slice(&[octets[0], octets[1], octets[2], 0]);
        }
        IpAddr::V6(v6) => {
            let octets = v6.octets();
            seed.extend_from_slice(&octets[..8]);
            seed.extend_from_slice(&[0; 8]);
        }
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

fn admit_upload_request(
    peer: &mut Peer,
    piece_tracker: &PieceTracker,
    lengths: &Lengths,
    key: (u32, u32, u32),
) -> Option<(u64, u64, LiveUpload)> {
    let (index, begin, length) = key;
    if peer.pending_uploads.lock().contains(&key) {
        return None;
    }
    let metered = peer.am_choking;
    let checked = (|| {
        let vpi = lengths.validate_piece(index).ok()?;
        let fast_allowed = peer.supports_fast && peer.sent_allowed_fast.contains(&index);
        if !piece_tracker.has_local(vpi) || (metered && !fast_allowed) {
            return None;
        }
        let piece_len = lengths.piece_length_of(vpi) as u64;
        let served = peer.fast_served.get(&index).copied().unwrap_or(0);
        let fits = length != 0
            && length <= MAX_REQUEST_LEN
            && (begin as u64).saturating_add(length as u64) <= piece_len
            && !(metered && served + u64::from(length) > piece_len);
        (fits && peer.pending_uploads.lock().admit(key)).then_some((vpi, piece_len))
    })();
    let Some((vpi, piece_len)) = checked else {
        send_fast_reject(peer, index, begin, length);
        return None;
    };
    let live = LiveUpload {
        pending: Arc::clone(&peer.pending_uploads),
        bytes: u64::from(length),
    };
    if metered {
        *peer.fast_served.entry(index).or_default() += u64::from(length);
    }
    Some((lengths.piece_offset(vpi), piece_len, live))
}

fn cancel_upload_request(peer: &mut Peer, key: (u32, u32, u32)) -> bool {
    let (index, begin, length) = key;
    let dropped = peer.pending_uploads.lock().remove(&key);
    if dropped {
        if let Some(served) = peer.fast_served.get_mut(&index) {
            *served = served.saturating_sub(u64::from(length));
        }
        if peer.supports_fast
            && !send_cancel_reject(&peer.pending_uploads, &peer.cmd_tx, index, begin, length)
        {
            abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
        }
    }
    dropped
}

fn send_cancel_reject(
    pending: &Arc<Mutex<PendingUploads>>,
    cmd_tx: &mpsc::Sender<PeerCommand>,
    index: u32,
    begin: u32,
    length: u32,
) -> bool {
    let cmd = PeerCommand::Send(Message::RejectRequest {
        index,
        begin,
        length,
    });
    let queued = send_reliably_holding(cmd_tx, cmd, || {
        pending.lock().reserve_deferred().then(|| LiveUpload {
            pending: Arc::clone(pending),
            bytes: 0,
        })
    });
    if queued {
        return true;
    }
    let mut pending = pending.lock();
    if pending.owed.len() >= MAX_PENDING_UPLOAD_REQUESTS {
        return false;
    }
    pending.owed.push_back((index, begin, length));
    true
}

fn flush_owed_rejects(peer: &Peer) {
    let mut pending = peer.pending_uploads.lock();
    while let Some(&(index, begin, length)) = pending.owed.front() {
        let reject = Message::RejectRequest {
            index,
            begin,
            length,
        };
        if peer.cmd_tx.try_send(PeerCommand::Send(reject)).is_err() {
            break;
        }
        pending.owed.pop_front();
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
    live: LiveUpload,
}

fn piece_frame_cost(length: usize) -> usize {
    (length + PIECE_FRAME_OVERHEAD).max(MIN_PIECE_FRAME_COST)
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
        live: _live,
    } = job;
    let (index, begin, length) = request_key;
    let upload_len = length as u64;
    limiter.acquire(piece_frame_cost(length as usize)).await;
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
    send_reliably_holding(cmd_tx, cmd, || Some(()));
}

fn send_reliably_holding<G: Send + 'static>(
    cmd_tx: &mpsc::Sender<PeerCommand>,
    cmd: PeerCommand,
    guard: impl FnOnce() -> Option<G>,
) -> bool {
    let cmd = match cmd_tx.try_send(cmd) {
        Err(mpsc::error::TrySendError::Full(cmd)) => cmd,
        _ => return true,
    };
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        return false;
    };
    let Some(guard) = guard() else {
        return false;
    };
    let cmd_tx = cmd_tx.clone();
    handle.spawn(async move {
        let _guard = guard;
        let _ = cmd_tx.send(cmd).await;
    });
    true
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
    route: Route,
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
    peer.route = route;
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
    holepunch: &mut HolepunchState,
    ip_budgets: &mut IpBudgets,
    peer_src_tx: &mpsc::Sender<PeerEnvelope>,
    generation: &GenerationToken,
    verifier: &PieceVerifier,
    failed_copies: Option<&SharedFailedCopies>,
    info_bytes: &Arc<Vec<u8>>,
    hash_tables: Option<&Arc<Vec<MerkleProofTable>>>,
    leaf_pool: &LeafHashPool,
    pipeline_floor: usize,
    pipeline_cap: usize,
    max_peers: usize,
    choke_dirty: &mut bool,
    upload_limiter: &Throttle,
    dht: Option<&Arc<crate::dht::Dht>>,
    blocklist: &RwLock<BlockList>,
    tracker_authorized: &HashSet<SocketAddr>,
    address_route: Route,
    listen_port: u16,
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
                if let Some(peer_registry::PeerLane {
                    tx: cmd_tx,
                    piece_tx,
                    addr: registry_addr,
                    io_abort,
                    route,
                }) = peer_registry::take_with_lane(torrent_id, pid, registry_scope)
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
                    peer.route = route;
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
                    peer.reject_backoff.clear();
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
                    let outcome = apply_request_reject(
                        pid,
                        peer,
                        piece_tracker,
                        chunk_tracker,
                        lengths,
                        (index, begin, length),
                        (pipeline_floor, pipeline_cap),
                    );
                    match outcome {
                        Some(redrive) => kick |= redrive,
                        None => {
                            abort_peer_io(peer.io_abort.as_ref(), &peer.cmd_tx);
                            return false;
                        }
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
                    let Some((piece_off, piece_len, live)) =
                        admit_upload_request(peer, piece_tracker, lengths, request_key)
                    else {
                        return false;
                    };
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
                        live,
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
                            sources: chunk_tracker
                                .padding_chunks(vpi)
                                .into_iter()
                                .map(|pad| pad.then_some(ChunkSource::Padding))
                                .collect(),
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
                        assembly.sources[chunk_index as usize] = Some(ChunkSource::Peer(source_ip));
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
                        let mut contributors: Vec<IpAddr> = assembly
                            .sources
                            .iter()
                            .flatten()
                            .filter_map(|source| source.peer())
                            .collect();
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
                            let (verify_ok, hash_conflict, digests) =
                                tokio::task::spawn_blocking(move || {
                                    let verdict = verifier.verify(index, &to_check);
                                    let ok = verdict.is_ok();
                                    let conflict =
                                        matches!(verdict, Err(VerifyError::InconsistentHashes));
                                    let wanted = if ok {
                                        prior.is_some()
                                    } else {
                                        multi_source && !conflict
                                    };
                                    let digests = (wanted && !sources.is_empty())
                                        .then(|| chunk_digests(&to_check, &sources));
                                    (ok, conflict, digests.map(|d| (d, prior)))
                                })
                                .await
                                .unwrap_or((false, false, None));
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
                                hash_conflict,
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
                            leaf_pool,
                            &peer.leaf_slots,
                            storage,
                            piece_tracker,
                            peer.cmd_tx.clone(),
                        );
                    } else {
                        let response = build_hash_response(
                            hash_tables.map(|tables| tables.as_slice()),
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
                            merge_ext_handshake(peer, &peer_ext);
                            vote_yourip(
                                ExternalIp::shared(),
                                peer.addr.ip(),
                                &peer_ext,
                                peer.route,
                            );
                            peer.ext_seen = true;
                            if let (false, Some(port)) = (peer.outbound, peer_ext.port) {
                                if let Some(listen) = advertised_listen_addr(
                                    private_torrent,
                                    peer.addr.ip(),
                                    port,
                                    tracker_authorized,
                                ) {
                                    peer.listen_addr = Some(listen);
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
                        let candidates = ingest_pex(
                            pid,
                            peer.addr.ip(),
                            &payload,
                            piece_tracker.is_complete(),
                            known_addrs,
                            ip_budgets,
                            pex_source,
                            Instant::now(),
                            &PeerRanker::observed(address_route, listen_port),
                        );
                        for addr in candidates {
                            let _ = peer_src_tx.try_send(PeerEnvelope {
                                generation: generation.current(),
                                candidate: PeerCandidate {
                                    addr,
                                    source: PeerSource::Pex,
                                },
                                mse_first: false,
                            });
                        }
                    } else if ext_id == OUR_UT_HOLEPUNCH_ID {
                        dispatch_holepunch(
                            pid,
                            &payload,
                            peers,
                            pending_dials,
                            priority_backlog,
                            peer_backlog,
                            dial_retries,
                            useful_redials,
                            known_addrs,
                            holepunch,
                            ip_budgets,
                            Instant::now(),
                            blocklist,
                        );
                    }
                }
                Message::Cancel {
                    index,
                    begin,
                    length,
                } => {
                    cancel_upload_request(peer, (index, begin, length));
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

            let live_elsewhere = pending_addr.is_some_and(|a| {
                peers
                    .values()
                    .any(|p| p.addr == a || p.listen_addr == Some(a))
            });
            if let Some(a) = addr {
                tracing::debug!("peer {a} disconnected: {reason}");
                if dead.is_some() {
                    pex_source.retain(|_, relay| *relay != pid);
                    holepunch.drop_relay(pid);
                }
                if was_pending_dial && !fd_backpressure && !live_elsewhere {
                    try_initiate_holepunch(a, pex_source, holepunch, peers, Instant::now());
                }
            }
            let dial_addr = if live_elsewhere {
                None
            } else {
                pending_addr.or_else(|| dead.as_ref().and_then(|peer| peer.listen_addr))
            };
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
        if became_local {
            let complete = piece_tracker.is_complete();
            for peer in peers.values_mut() {
                if complete || (peer.am_interested && bitfield_has(&peer.bitfield, vr.piece_index))
                {
                    refresh_interest(peer, piece_tracker);
                }
            }
        }
        let completed_files = {
            let mut s = stats.lock();
            let completed = if became_local {
                add_piece_progress(&mut s, lengths, storage.layout(), vpi)
            } else {
                Vec::new()
            };
            s.left_bytes = piece_tracker.bytes_left();
            completed
        };
        storage.files_completed(&completed_files).await;
        stats.lock().finished = piece_tracker.is_complete();
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
        .redirect(super::webseed::redirect_policy())
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
                hash_conflict: false,
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
                        &ctx.throttle,
                    )
                    .await
                    {
                        Ok(response) => {
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
                        Err(super::webseed::WebSeedError::BodyTooSlow) => {
                            ctx.back_off(mirror_idx);
                            mirror_ok = false;
                            break;
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
            let verdict =
                tokio::task::spawn_blocking(move || verifier.verify(piece_index, &to_check))
                    .await
                    .unwrap_or(Err(VerifyError::HashMismatch));
            if matches!(verdict, Err(VerifyError::InconsistentHashes)) {
                pieces.push(VerifyResult {
                    piece_index,
                    write_error: None,
                    verify_ok: false,
                    from_webseed: true,
                    hash_conflict: true,
                    contributors: Vec::new(),
                    failed_copy: None,
                    culprits: Vec::new(),
                });
                break;
            }
            if verdict.is_err() {
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
                hash_conflict: false,
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

struct HolepunchSender<'a> {
    pid: u32,
    addr: SocketAddr,
    hp_id: Option<u8>,
    their_view_of_us: Option<IpAddr>,
    elected: ExternalAddresses,
    cmd: &'a mpsc::Sender<PeerCommand>,
}

#[allow(clippy::too_many_arguments)]
fn handle_holepunch(
    hp: HolepunchMsg,
    from: &HolepunchSender,
    peers: &HashMap<u32, Peer>,
    pending_dials: &HashMap<u32, SocketAddr>,
    priority_backlog: &mut VecDeque<SocketAddr>,
    peer_backlog: &mut VecDeque<SocketAddr>,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
    known_addrs: &mut HashSet<SocketAddr>,
    holepunch: &mut HolepunchState,
    budget: &mut TokenBucket,
    now: Instant,
    blocklist: &RwLock<BlockList>,
) {
    let Some(from_hp_id) = from.hp_id else {
        return;
    };
    match hp.msg_type {
        holepunch_type::CONNECT => {
            let target = canonical_addr(hp.addr);
            let solicited = holepunch.take_answer(&target, from.pid, now);
            if !solicited && budget.take(1, now) == 0 {
                return;
            }
            if !peer_supplied_dialable(from.addr.ip(), target)
                || blocklist.read().contains(target.ip())
            {
                return;
            }
            if !solicited && !holepunch.admit_unsolicited(now) {
                return;
            }
            let active_addrs = peers
                .values()
                .flat_map(|peer| [Some(peer.addr), peer.listen_addr])
                .flatten()
                .chain(pending_dials.values().copied())
                .collect::<HashSet<_>>();
            if promote_holepunch_candidate(
                target,
                &active_addrs,
                priority_backlog,
                peer_backlog,
                dial_retries,
                useful_redials,
                known_addrs,
            ) {
                holepunch.note_target(target, now);
            }
        }
        holepunch_type::RENDEZVOUS => {
            enum Relay {
                Connect(mpsc::Sender<PeerCommand>, u8, SocketAddr),
                Err(u32),
            }
            let action = if hp.addr == from.addr
                || holepunch.is_own_endpoint(hp.addr, from.their_view_of_us, &from.elected)
            {
                Relay::Err(holepunch_err::NO_SELF)
            } else if !super::magnet::is_dialable_peer_addr(hp.addr) {
                Relay::Err(holepunch_err::NO_SUCH_PEER)
            } else {
                match peers
                    .iter()
                    .find(|(_, p)| p.addr == hp.addr || p.listen_addr == Some(hp.addr))
                {
                    Some((&target_pid, _)) if target_pid == from.pid => {
                        Relay::Err(holepunch_err::NO_SELF)
                    }
                    Some((_, t)) => match t.their_ut_holepunch_id {
                        Some(thp) => Relay::Connect(t.cmd_tx.clone(), thp, holepunch_endpoint(t)),
                        None => Relay::Err(holepunch_err::NO_SUPPORT),
                    },
                    None => Relay::Err(holepunch_err::NOT_CONNECTED),
                }
            };
            match action {
                Relay::Connect(target_cmd, target_hp, target_endpoint) => {
                    let _ = target_cmd.try_send(PeerCommand::Send(Message::Extended {
                        ext_id: target_hp,
                        payload: build_holepunch(holepunch_type::CONNECT, from.addr, 0),
                    }));
                    let _ = from.cmd.try_send(PeerCommand::Send(Message::Extended {
                        ext_id: from_hp_id,
                        payload: build_holepunch(holepunch_type::CONNECT, target_endpoint, 0),
                    }));
                }
                Relay::Err(code) => {
                    let _ = from.cmd.try_send(PeerCommand::Send(Message::Extended {
                        ext_id: from_hp_id,
                        payload: build_holepunch(holepunch_type::ERROR, hp.addr, code),
                    }));
                }
            }
        }
        holepunch_type::ERROR => {
            tracing::debug!(
                target: "diag",
                "holepunch ERROR from {} target={} code={}",
                from.addr, hp.addr, hp.err_code
            );
        }
        _ => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch_holepunch(
    pid: u32,
    payload: &[u8],
    peers: &HashMap<u32, Peer>,
    pending_dials: &HashMap<u32, SocketAddr>,
    priority_backlog: &mut VecDeque<SocketAddr>,
    peer_backlog: &mut VecDeque<SocketAddr>,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
    known_addrs: &mut HashSet<SocketAddr>,
    holepunch: &mut HolepunchState,
    ip_budgets: &mut IpBudgets,
    now: Instant,
    blocklist: &RwLock<BlockList>,
) {
    if !holepunch.enabled {
        return;
    }
    let Some(peer) = peers.get(&pid) else {
        return;
    };
    let Some(hp) = parse_holepunch(payload) else {
        return;
    };
    let sender = HolepunchSender {
        pid,
        addr: holepunch_endpoint(peer),
        hp_id: peer.their_ut_holepunch_id,
        their_view_of_us: peer.their_yourip,
        elected: ExternalIp::shared().addresses(),
        cmd: &peer.cmd_tx,
    };
    let budget = &mut ip_budgets.entry(peer.addr.ip(), now).connect;
    handle_holepunch(
        hp,
        &sender,
        peers,
        pending_dials,
        priority_backlog,
        peer_backlog,
        dial_retries,
        useful_redials,
        known_addrs,
        holepunch,
        budget,
        now,
        blocklist,
    );
}

#[allow(clippy::too_many_arguments)]
fn ingest_pex(
    pid: u32,
    source_ip: IpAddr,
    payload: &[u8],
    complete: bool,
    known_addrs: &HashSet<SocketAddr>,
    ip_budgets: &mut IpBudgets,
    pex_source: &mut HashMap<SocketAddr, u32>,
    now: Instant,
    ranker: &PeerRanker,
) -> Vec<SocketAddr> {
    let ip_budget = ip_budgets.entry(source_ip, now);
    if !ip_budget.open_pex_gap(now) {
        return Vec::new();
    }
    let Some(update) = super::wire::extended::parse_ut_pex_full(payload) else {
        return Vec::new();
    };
    for addr in update.dropped {
        let addr = canonical_addr(addr);
        if pex_source.get(&addr) == Some(&pid) {
            pex_source.remove(&addr);
        }
    }
    let mut admitted: Vec<SocketAddr> = admit_pex_contacts(
        source_ip,
        update.added,
        complete,
        known_addrs,
        &mut ip_budget.pex,
        now,
    )
    .into_iter()
    .map(|(addr, _)| {
        if pex_source.len() < MAX_PEX_SOURCE_ENTRIES || pex_source.contains_key(&addr) {
            pex_source.insert(addr, pid);
        }
        addr
    })
    .collect();
    ranker.order_batch(&mut admitted);
    admitted
}

#[allow(clippy::too_many_arguments)]
fn queue_sourced_candidate(
    addr: SocketAddr,
    source: PeerSource,
    known_useful: bool,
    priority_backlog: &mut VecDeque<SocketAddr>,
    peer_backlog: &mut VecDeque<SocketAddr>,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
    known_addrs: &mut HashSet<SocketAddr>,
    blocklist: &RwLock<BlockList>,
) -> bool {
    let queued =
        priority_backlog.len() + peer_backlog.len() + dial_retries.len() + useful_redials.len();
    candidate_fits_source_cap(source, known_useful, queued)
        && enqueue_peer_candidate(
            addr,
            known_useful,
            priority_backlog,
            peer_backlog,
            dial_retries,
            useful_redials,
            known_addrs,
            blocklist,
        )
}

fn holepunch_endpoint(peer: &Peer) -> SocketAddr {
    if peer.link.utp {
        peer.addr
    } else {
        peer.listen_addr.unwrap_or(peer.addr)
    }
}

fn inbound_addr_taken(
    addr: SocketAddr,
    known_addrs: &HashSet<SocketAddr>,
    peers: &HashMap<u32, Peer>,
    pending_dials: &HashMap<u32, SocketAddr>,
    holepunch: &HolepunchState,
    now: Instant,
) -> bool {
    if !known_addrs.contains(&addr) {
        return false;
    }
    if peers
        .values()
        .any(|p| p.addr == addr || p.listen_addr == Some(addr))
    {
        return true;
    }
    !(holepunch.is_target(&addr, now) || pending_dials.values().any(|dialing| *dialing == addr))
}

fn admit_pex_contacts(
    source: IpAddr,
    added: Vec<(SocketAddr, u8)>,
    complete: bool,
    known_addrs: &HashSet<SocketAddr>,
    budget: &mut TokenBucket,
    now: Instant,
) -> Vec<(SocketAddr, u8)> {
    let mut seen_ips = HashSet::new();
    added
        .into_iter()
        .map(|(addr, flags)| (canonical_addr(addr), flags))
        .filter(|(addr, flags)| {
            !(complete && flags & pex_flag::SEED != 0)
                && peer_supplied_dialable(source, *addr)
                && (addr.ip().is_loopback() || seen_ips.insert(addr.ip()))
                && (known_addrs.contains(addr) || budget.take(1, now) == 1)
        })
        .collect()
}

fn send_pex_updates(peers: &mut HashMap<u32, Peer>, total_pieces: usize) {
    let current: HashMap<SocketAddr, u8> = peers
        .values()
        .filter_map(|p| {
            p.listen_addr
                .map(|a| (canonical_addr(a), p.pex_flags(total_pieces)))
        })
        .collect();
    for p in peers.values_mut() {
        let Some(pex_id) = p.their_ut_pex_id else {
            continue;
        };
        let recipient_ip = p.addr.ip();
        let recipient_addrs = [
            Some(canonical_addr(p.addr)),
            p.listen_addr.map(canonical_addr),
        ];
        let added: Vec<(SocketAddr, u8)> = current
            .iter()
            .filter(|(a, _)| {
                !recipient_addrs.contains(&Some(**a))
                    && !p.pex_sent.contains(a)
                    && peer_supplied_dialable(recipient_ip, **a)
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
        let payload = super::wire::extended::build_ut_pex(&added, &dropped);
        if p.cmd_tx
            .try_send(PeerCommand::Send(Message::Extended {
                ext_id: pex_id,
                payload,
            }))
            .is_ok()
        {
            for a in &dropped {
                p.pex_sent.remove(a);
            }
            p.pex_sent.extend(added.iter().map(|(a, _)| *a));
        }
    }
}

fn peer_supplied_dialable(source: IpAddr, addr: SocketAddr) -> bool {
    let addr = canonical_addr(addr);
    super::magnet::is_dialable_peer_addr(addr)
        && crate::net::reach(addr.ip()) <= crate::net::reach(source)
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

fn unqueue_peer_addr(
    addr: SocketAddr,
    priority_backlog: &mut VecDeque<SocketAddr>,
    peer_backlog: &mut VecDeque<SocketAddr>,
    dial_retries: &mut VecDeque<(SocketAddr, Instant)>,
    useful_redials: &mut VecDeque<(SocketAddr, Instant)>,
) {
    priority_backlog.retain(|queued| *queued != addr);
    peer_backlog.retain(|queued| *queued != addr);
    dial_retries.retain(|(queued, _)| *queued != addr);
    useful_redials.retain(|(queued, _)| *queued != addr);
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

fn candidate_fits_source_cap(source: PeerSource, known_useful: bool, queued: usize) -> bool {
    source != PeerSource::Pex || known_useful || queued < MAX_PEX_BACKLOG
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
    holepunch: &HolepunchState,
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
        let prefer_utp = holepunch.is_target(&addr, now);
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
    holepunch: &mut HolepunchState,
    peers: &HashMap<u32, Peer>,
    now: Instant,
) {
    if !holepunch.enabled || !holepunch.can_initiate(&target, now) {
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
        holepunch.note_initiated(target, relay_pid, now);
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

#[derive(Clone, Copy)]
struct RecycleScope<'a> {
    complete: bool,
    now: Instant,
    total_pieces: usize,
    ranker: &'a PeerRanker,
}

fn redundant_peers(peers: &HashMap<u32, Peer>, scope: RecycleScope, limit: usize) -> Vec<u32> {
    let RecycleScope {
        complete,
        now,
        total_pieces,
        ranker,
    } = scope;
    let mut found: Vec<(Instant, u32, SocketAddr)> = peers
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
        .map(|(&pid, p)| (p.connected_at, pid, p.listen_addr.unwrap_or(p.addr)))
        .collect();
    found.sort_unstable_by_key(|&(connected_at, pid, _)| (connected_at, pid));
    let ranks: Option<Vec<u32>> = found
        .iter()
        .map(|&(_, _, addr)| ranker.priority(addr))
        .collect();
    let Some(ranks) = ranks else {
        return found
            .into_iter()
            .take(limit)
            .map(|(_, pid, _)| pid)
            .collect();
    };
    let mut keyed: Vec<_> = found
        .into_iter()
        .zip(ranks)
        .map(|((connected_at, pid, _), rank)| {
            let age = now.saturating_duration_since(connected_at);
            let window = age.as_secs() / EVICTION_TIE_WINDOW.as_secs();
            (Reverse(window), rank, connected_at, pid)
        })
        .collect();
    keyed.sort_unstable();
    keyed
        .into_iter()
        .take(limit)
        .map(|(_, _, _, pid)| pid)
        .collect()
}

fn inbound_eviction_victim(
    peers: &HashMap<u32, Peer>,
    queued: &[u32],
    scope: RecycleScope,
) -> Option<u32> {
    if !scope.complete {
        return None;
    }
    redundant_peers(peers, scope, queued.len() + 1)
        .into_iter()
        .find(|pid| !queued.contains(pid))
}

struct InboundRoom {
    crowded: bool,
    victim: Option<u32>,
}

fn inbound_room(
    peers: &HashMap<u32, Peer>,
    evictions: &mut Vec<u32>,
    max_peers: usize,
    paused: bool,
    scope: RecycleScope,
) -> InboundRoom {
    evictions.retain(|pid| peers.contains_key(pid));
    let crowded = peers.len().saturating_sub(evictions.len()) >= max_peers;
    let victim = if crowded && !paused {
        inbound_eviction_victim(peers, evictions, scope)
    } else {
        None
    };
    InboundRoom { crowded, victim }
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
    let now = Instant::now();
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
                    if exhausted.contains(&index)
                        || !bitfield_has(&peer.bitfield, index)
                        || peer
                            .reject_backoff
                            .get(&index)
                            .is_some_and(|&until| now < until)
                    {
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

pub(crate) fn vote_yourip(
    external: &ExternalIp,
    peer_ip: IpAddr,
    ext: &ExtHandshake,
    route: Route,
) {
    if let Some(seen) = ext.yourip {
        external.vote(seen, Source::Peer, peer_ip, route);
    }
}

fn merge_ext_handshake(peer: &mut Peer, ext: &ExtHandshake) {
    let merged = |current: Option<u8>, name: &[u8]| match ext.advertised_id(name) {
        None => current,
        Some(0) => None,
        Some(id) => Some(id),
    };
    peer.their_ut_metadata_id = merged(peer.their_ut_metadata_id, EXT_NAME_UT_METADATA);
    peer.their_ut_holepunch_id = merged(peer.their_ut_holepunch_id, EXT_NAME_UT_HOLEPUNCH);
    peer.their_ut_pex_id = merged(peer.their_ut_pex_id, EXT_NAME_UT_PEX);
    if let Some(prefers) = ext.prefers_encryption {
        peer.prefers_encryption = prefers;
    }
    if let Some(upload_only) = ext.upload_only {
        peer.upload_only = upload_only;
    }
    if ext.yourip.is_some() {
        peer.their_yourip = ext.yourip;
    }
}

fn apply_request_reject(
    pid: u32,
    peer: &mut Peer,
    piece_tracker: &mut PieceTracker,
    chunk_tracker: &mut ChunkTracker,
    lengths: &Lengths,
    key: (u32, u32, u32),
    pipeline: (usize, usize),
) -> Option<bool> {
    let (index, begin, _) = key;
    let Some(req_idx) = peer.outstanding.iter().position(|&request| request == key) else {
        return peer.canceled_requests.remove(&key).then_some(false);
    };
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
        peer.shrink_window(pipeline.0, pipeline.1);
        peer.back_off_piece(index);
    }
    if peer.consecutive_rejects >= REJECT_SNUB_THRESHOLD && !peer.snubbing {
        peer.mark_snubbed();
        return Some(false);
    }
    Some(!peer.snubbing && !peer.peer_choking)
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
) -> Vec<usize> {
    let offset = lengths.piece_offset(vpi);
    let len = lengths.piece_length_of(vpi) as u64;
    stats.progress_bytes = stats.progress_bytes.saturating_add(len);
    let file_progress = Arc::make_mut(&mut stats.file_progress);
    if file_progress.len() < layout.files().len() {
        file_progress.resize(layout.files().len(), 0);
    }
    let mut completed = Vec::new();
    for span in layout.spans_for(offset, len) {
        let length = layout.files()[span.file_index].length;
        let done = &mut file_progress[span.file_index];
        let before = *done;
        *done = before.saturating_add(span.len);
        if before < length && *done >= length {
            completed.push(span.file_index);
        }
    }
    completed
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
    holepunch: &mut HolepunchState,
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
            holepunch.drop_relay(pid);
            holepunch.forget(&p.addr);
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
    holepunch.retain(|addr| !is_blocked(*addr));
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

pub(crate) fn declared_tracker_tiers(meta: &TorrentMeta) -> Vec<Vec<String>> {
    if meta.announce_list.iter().any(|tier| !tier.is_empty()) {
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
    }
}

fn collect_tracker_tiers(meta: &TorrentMeta) -> Vec<Vec<String>> {
    let source = declared_tracker_tiers(meta);

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

fn collect_announce_tiers(meta: &TorrentMeta) -> (Vec<Vec<String>>, usize) {
    let mut tiers = collect_obfuscated_tiers(meta);
    let obfuscated = tiers.len();
    let mut plain = collect_tracker_tiers(meta);
    for tier in &mut plain {
        tier.retain(|url| !tiers.iter().flatten().any(|known| known == url));
    }
    tiers.extend(plain.into_iter().filter(|tier| !tier.is_empty()));
    (tiers, obfuscated)
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

type TierChangeTx = mpsc::Sender<oneshot::Sender<()>>;
type RankerFor = fn(Route, u16) -> PeerRanker;

struct TrackerPollers {
    shutdown_tx: watch::Sender<Option<bool>>,
    tasks: tokio::task::JoinSet<()>,
}

impl TrackerPollers {
    fn idle() -> Self {
        Self {
            shutdown_tx: watch::channel(None).0,
            tasks: tokio::task::JoinSet::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    fn signal_shutdown(&self, announce_stopped: bool) {
        let _ = self.shutdown_tx.send(Some(announce_stopped));
    }

    async fn shutdown(&mut self, announce_stopped: bool) {
        self.signal_shutdown(announce_stopped);
        self.join().await;
    }

    async fn join(&mut self) {
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

fn clamp_tracker_interval(interval: Duration, min_interval: Option<Duration>) -> Duration {
    let floor = min_interval.map_or(TRACKER_MIN_INTERVAL, |min| {
        min.clamp(TRACKER_MIN_INTERVAL, TRACKER_MAX_INTERVAL)
    });
    interval.clamp(floor, TRACKER_MAX_INTERVAL)
}

fn tracker_retry_delay(consecutive_failures: u32) -> Duration {
    const BACKOFF_SECS: [u64; 9] = [15, 30, 60, 120, 300, 600, 1200, 2400, 3600];
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
#[derive(Clone)]
struct TrackerCtx {
    tx: mpsc::Sender<PeerEnvelope>,
    peer_id: Id20,
    key: u32,
    port: u16,
    stats: Arc<Mutex<TorrentStats>>,
    proxy: Option<risuko_http::ProxyConnector>,
    lifecycle: Arc<Mutex<TrackerLifecycle>>,
    tier_change_tx: Option<TierChangeTx>,
    source_addr: Option<SocketAddr>,
    generation: GenerationToken,
    obfuscated_tiers: usize,
    ranker_for: RankerFor,
}

impl TrackerCtx {
    fn request(
        &self,
        info_hash: Id20,
        event: AnnounceEvent,
        num_want: u32,
        obfuscate: bool,
        tracker_id: Option<Vec<u8>>,
    ) -> AnnounceRequest {
        let (uploaded, downloaded, left) = {
            let stats = self.stats.lock();
            (
                stats.uploaded_bytes,
                stats.session_downloaded,
                stats.left_bytes,
            )
        };
        AnnounceRequest {
            info_hash,
            peer_id: self.peer_id,
            key: self.key,
            port: self.port,
            uploaded,
            downloaded,
            left,
            event,
            num_want,
            obfuscate,
            tracker_id,
        }
    }

    async fn announce(
        &self,
        url: &str,
        req: &AnnounceRequest,
        timeout: Duration,
    ) -> Result<AnnounceResponse, TrackerError> {
        super::tracker::announce_with_proxy_and_source(
            url,
            req,
            timeout,
            self.proxy.as_ref(),
            self.source_addr,
        )
        .await
    }

    async fn forward_peers(
        &self,
        shutdown: &mut watch::Receiver<Option<bool>>,
        mut peers: Vec<SocketAddr>,
        generation: u64,
        mse_first: bool,
    ) -> bool {
        (self.ranker_for)(Route::of(self.proxy.as_ref()), self.port).order_batch(&mut peers);
        for addr in peers {
            let sent = tokio::select! {
                biased;
                _ = shutdown.changed() => return false,
                sent = self.tx.send(PeerEnvelope {
                    generation,
                    candidate: PeerCandidate { addr, source: PeerSource::Tracker },
                    mse_first,
                }) => sent,
            };
            if sent.is_err() {
                return false;
            }
        }
        true
    }
}

#[derive(Clone)]
struct TrackerUrlState {
    url: String,
    next_at: Instant,
    min_next_at: Instant,
    failures: u32,
    started_sent: bool,
    completed_sent: bool,
    disabled: bool,
    tracker_id: Option<Vec<u8>>,
    seeders: Option<u32>,
    leechers: Option<u32>,
}

impl TrackerUrlState {
    fn is_working(&self) -> bool {
        self.started_sent && self.failures == 0
    }

    fn record_success(
        &mut self,
        event: AnnounceEvent,
        finished_at_send: bool,
        finished_now: bool,
        resp: &AnnounceResponse,
        now: Instant,
    ) {
        self.failures = 0;
        if event == AnnounceEvent::Started {
            self.started_sent = true;
            self.completed_sent |= finished_at_send;
        } else if event == AnnounceEvent::Completed {
            self.completed_sent = true;
        }
        self.min_next_at = now
            + resp
                .min_interval
                .map_or(Duration::ZERO, |min| min.min(TRACKER_MAX_INTERVAL));
        self.next_at = if finished_now && !self.completed_sent {
            now
        } else {
            now + clamp_tracker_interval(resp.interval, resp.min_interval)
        };
        if let Some(id) = &resp.tracker_id {
            self.tracker_id = Some(id.clone());
        }
        self.seeders = resp.seeders;
        self.leechers = resp.leechers;
    }

    fn record_failure(&mut self, error: &TrackerError, now: Instant) -> Option<Duration> {
        self.failures = self.failures.saturating_add(1);
        let backoff = tracker_retry_delay(self.failures);
        let delay = match error {
            TrackerError::UnsupportedScheme(_)
            | TrackerError::Rejected {
                retry_in: Some(RetryIn::Never),
                ..
            } => {
                self.disabled = true;
                return None;
            }
            TrackerError::Rejected {
                retry_in: Some(RetryIn::After(wait)),
                ..
            } => {
                self.min_next_at = self.min_next_at.max(now + *wait);
                backoff.max(*wait)
            }
            TrackerError::Io(_) | TrackerError::Timeout | TrackerError::Http(_) => {
                backoff.min(TRACKER_TRANSPORT_RETRY_MAX)
            }
            _ => backoff,
        };
        self.next_at = now + delay;
        Some(delay)
    }
}

#[derive(Clone)]
struct TrackerRunState {
    tiers: Vec<Vec<TrackerUrlState>>,
    last_successful_tier: Option<usize>,
}

#[derive(Debug, PartialEq, Eq)]
enum TrackerPick {
    Due { tier: usize, index: usize },
    Idle(Option<Instant>),
}

impl TrackerRunState {
    fn pick(&self, now: Instant) -> TrackerPick {
        // BEP 27 allows switching trackers only when the current one fails
        let current = self.last_successful_tier.filter(|&tier| {
            self.tiers
                .get(tier)
                .is_some_and(|urls| urls.iter().any(TrackerUrlState::is_working))
        });
        let mut earliest: Option<Instant> = None;
        for (tier, urls) in self.tiers.iter().enumerate().skip(current.unwrap_or(0)) {
            for (index, entry) in urls.iter().enumerate() {
                if entry.disabled {
                    continue;
                }
                if entry.next_at <= now {
                    return TrackerPick::Due { tier, index };
                }
                earliest = Some(earliest.map_or(entry.next_at, |at| at.min(entry.next_at)));
                if entry.is_working() {
                    return TrackerPick::Idle(earliest);
                }
            }
        }
        TrackerPick::Idle(earliest)
    }

    #[allow(clippy::too_many_arguments)]
    fn settle_tier_success(
        &mut self,
        tier: usize,
        url: &str,
        event: AnnounceEvent,
        finished_at_send: bool,
        finished_now: bool,
        resp: &AnnounceResponse,
        now: Instant,
    ) -> bool {
        if let Some(urls) = self.tiers.get_mut(tier) {
            if let Some(index) = urls.iter().position(|entry| entry.url == url) {
                let mut entry = urls.remove(index);
                entry.record_success(event, finished_at_send, finished_now, resp, now);
                urls.insert(0, entry);
            }
        }
        self.last_successful_tier
            .replace(tier)
            .is_some_and(|previous| previous != tier)
    }

    fn settle_tier_failure(
        &mut self,
        tier: usize,
        url: &str,
        error: &TrackerError,
        now: Instant,
    ) -> Option<Duration> {
        self.tiers
            .get_mut(tier)?
            .iter_mut()
            .find(|entry| entry.url == url)?
            .record_failure(error, now)
    }
}

struct TrackerLifecycle {
    by_info_hash: HashMap<Id20, TrackerRunState>,
    wake: watch::Sender<u64>,
    appended: Vec<Vec<String>>,
    stopping: Option<watch::Receiver<()>>,
}

impl Default for TrackerLifecycle {
    fn default() -> Self {
        Self {
            by_info_hash: HashMap::new(),
            wake: watch::channel(0).0,
            appended: Vec::new(),
            stopping: None,
        }
    }
}

impl TrackerLifecycle {
    fn subscribe(&self) -> watch::Receiver<u64> {
        self.wake.subscribe()
    }

    fn begin_stop(&mut self) -> watch::Sender<()> {
        let (done_tx, done_rx) = watch::channel(());
        self.stopping = Some(done_rx);
        done_tx
    }

    fn pending_stop(&self) -> Option<watch::Receiver<()>> {
        self.stopping.clone()
    }

    fn announce_now(&mut self, completion: bool) {
        let now = Instant::now();
        for state in self.by_info_hash.values_mut() {
            for entry in state.tiers.iter_mut().flatten() {
                if entry.is_working()
                    && entry.next_at > now
                    && (!completion || !entry.completed_sent)
                {
                    let earliest = if completion {
                        now
                    } else {
                        now.max(entry.min_next_at)
                    };
                    entry.next_at = entry.next_at.min(earliest);
                }
            }
        }
        self.wake.send_modify(|n| *n = n.wrapping_add(1));
    }

    fn retry_failed_now(&mut self) {
        let now = Instant::now();
        for state in self.by_info_hash.values_mut() {
            for entry in state.tiers.iter_mut().flatten() {
                if !entry.disabled && entry.failures > 0 {
                    entry.next_at = entry.next_at.min(now.max(entry.min_next_at));
                }
            }
        }
        self.wake.send_modify(|n| *n = n.wrapping_add(1));
    }

    fn swarm_counts(&self) -> (Option<u32>, Option<u32>) {
        self.by_info_hash
            .values()
            .flat_map(|state| state.tiers.iter().flatten())
            .fold((None, None), |(seeders, leechers), entry| {
                (seeders.max(entry.seeders), leechers.max(entry.leechers))
            })
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
            min_next_at: next_at,
            failures: 0,
            started_sent: false,
            completed_sent: false,
            disabled: false,
            tracker_id: None,
            seeders: None,
            leechers: None,
        })
        .collect()
}

fn redacted_tracker_error(error: &TrackerError, url: &str) -> String {
    error
        .to_string()
        .replace(url, &super::tracker::redacted_tracker_url(url))
}

fn log_announce_ok(shown: &str, event: AnnounceEvent, resp: &AnnounceResponse) {
    tracing::info!(target: "diag", "tracker ANNOUNCE ok url={shown} event={event:?} peers={} interval_s={}", resp.peers.len(), resp.interval.as_secs());
    if let Some(warning) = &resp.warning {
        tracing::info!(target: "diag", "tracker ANNOUNCE warning url={shown}: {warning:?}");
    }
}

fn publish_swarm_counts(lifecycle: &Mutex<TrackerLifecycle>, stats: &Mutex<TorrentStats>) {
    let (seeders, leechers) = lifecycle.lock().swarm_counts();
    let mut stats = stats.lock();
    stats.swarm_seeders = seeders;
    stats.swarm_leechers = leechers;
}

async fn announce_stopped(ctx: &TrackerCtx, tiers: &[Vec<String>], info_hash: Id20) {
    let now = Instant::now();
    let started = {
        let mut lifecycle = ctx.lifecycle.lock();
        let mut started = Vec::new();
        for (tier, urls) in lifecycle
            .run_state(info_hash, tiers)
            .tiers
            .iter_mut()
            .enumerate()
        {
            for entry in urls.iter_mut().filter(|entry| entry.started_sent) {
                started.push((
                    entry.url.clone(),
                    tier < ctx.obfuscated_tiers,
                    entry.tracker_id.clone(),
                ));
                entry.started_sent = false;
                if entry.failures == 0 {
                    entry.next_at = now;
                }
            }
        }
        started
    };
    let mut stops = tokio::task::JoinSet::new();
    for (url, obfuscate, tracker_id) in started {
        let stopped = ctx.request(info_hash, AnnounceEvent::Stopped, 0, obfuscate, tracker_id);
        let ctx = ctx.clone();
        stops.spawn(async move {
            let shown = super::tracker::redacted_tracker_url(&url);
            match ctx
                .announce(&url, &stopped, super::tracker::STOPPED_ANNOUNCE_TIMEOUT)
                .await
            {
                Ok(_) => tracing::debug!("tracker STOPPED ok url={shown}"),
                Err(e) => tracing::debug!(
                    "tracker STOPPED failed url={shown}: {}",
                    redacted_tracker_error(&e, &url)
                ),
            }
        });
    }
    while stops.join_next().await.is_some() {}
}

async fn run_tracker_tier_poller(
    ctx: &TrackerCtx,
    tiers: &[Vec<String>],
    info_hash: Id20,
    shutdown: &mut watch::Receiver<Option<bool>>,
) {
    let mut wake = ctx.lifecycle.lock().subscribe();

    loop {
        if shutdown.borrow().is_some() {
            break;
        }
        let picked = {
            let mut lifecycle = ctx.lifecycle.lock();
            let state = lifecycle.run_state(info_hash, tiers);
            if state.tiers.is_empty() {
                break;
            }
            match state.pick(Instant::now()) {
                TrackerPick::Due { tier, index } => Ok((tier, state.tiers[tier][index].clone())),
                TrackerPick::Idle(until) => Err(until),
            }
        };
        let (tier, selected) = match picked {
            Ok(picked) => picked,
            Err(until) => {
                let wait = until.map(|at| at.saturating_duration_since(Instant::now()));
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    _ = wake.changed() => {}
                    _ = async {
                        match wait {
                            Some(wait) => tokio::time::sleep(wait).await,
                            None => std::future::pending().await,
                        }
                    } => {}
                }
                continue;
            }
        };
        let obfuscate = tier < ctx.obfuscated_tiers;
        let shown = super::tracker::redacted_tracker_url(&selected.url);

        let finished = ctx.stats.lock().finished;
        let event = announce_event(&selected, finished);
        let req = ctx.request(
            info_hash,
            event,
            200,
            obfuscate,
            selected.tracker_id.clone(),
        );
        let request_generation = ctx.generation.current();
        let result = tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            result = ctx.announce(&selected.url, &req, TRACKER_ANNOUNCE_TIMEOUT) => result,
        };
        match result {
            Ok(resp) => {
                log_announce_ok(&shown, event, &resp);
                let finished_now = ctx.stats.lock().finished;
                let changed_tier = ctx
                    .lifecycle
                    .lock()
                    .run_state(info_hash, tiers)
                    .settle_tier_success(
                        tier,
                        &selected.url,
                        event,
                        finished,
                        finished_now,
                        &resp,
                        Instant::now(),
                    );
                publish_swarm_counts(&ctx.lifecycle, &ctx.stats);
                let request_generation = match (&ctx.tier_change_tx, changed_tier) {
                    (Some(tier_tx), true) => {
                        let (ack_tx, ack_rx) = oneshot::channel();
                        tokio::select! {
                            biased;
                            _ = shutdown.changed() => break,
                            _ = async {
                                if tier_tx.send(ack_tx).await.is_ok() {
                                    let _ = ack_rx.await;
                                }
                            } => {}
                        }
                        ctx.generation.current()
                    }
                    _ => request_generation,
                };
                if !ctx
                    .forward_peers(shutdown, resp.peers, request_generation, obfuscate)
                    .await
                {
                    break;
                }
            }
            Err(e) => {
                let retry = ctx
                    .lifecycle
                    .lock()
                    .run_state(info_hash, tiers)
                    .settle_tier_failure(tier, &selected.url, &e, Instant::now());
                tracing::info!(target: "diag", "tracker ANNOUNCE fail url={shown} event={event:?} retry_s={} err={}", retry.map_or_else(|| "never".to_string(), |delay| delay.as_secs().to_string()), redacted_tracker_error(&e, &selected.url));
            }
        }
    }
}

type TrackerAnnounceDone = (
    usize,
    usize,
    AnnounceEvent,
    Result<AnnounceResponse, TrackerError>,
    u64,
    bool,
);

async fn run_tracker_swarm_poller(
    ctx: &TrackerCtx,
    tiers: &[Vec<String>],
    info_hash: Id20,
    shutdown: &mut watch::Receiver<Option<bool>>,
) {
    let mut wake = ctx.lifecycle.lock().subscribe();
    let mut inflight: HashSet<(usize, usize)> = HashSet::new();
    let mut announces: tokio::task::JoinSet<TrackerAnnounceDone> = tokio::task::JoinSet::new();

    loop {
        if shutdown.borrow().is_some() {
            break;
        }
        let finished = ctx.stats.lock().finished;
        let mut next_due: Option<Instant> = None;
        {
            let now = Instant::now();
            let mut lifecycle = ctx.lifecycle.lock();
            let state = lifecycle.run_state(info_hash, tiers);
            for (ti, tier) in state.tiers.iter().enumerate() {
                for (ui, entry) in tier.iter().enumerate() {
                    if entry.disabled || inflight.contains(&(ti, ui)) {
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
                    let obfuscate = ti < ctx.obfuscated_tiers;
                    let req =
                        ctx.request(info_hash, event, 200, obfuscate, entry.tracker_id.clone());
                    let url = entry.url.clone();
                    let request_generation = ctx.generation.current();
                    let ctx = ctx.clone();
                    inflight.insert((ti, ui));
                    announces.spawn(async move {
                        let result = ctx.announce(&url, &req, TRACKER_ANNOUNCE_TIMEOUT).await;
                        let shown = super::tracker::redacted_tracker_url(&url);
                        match &result {
                            Ok(resp) => log_announce_ok(&shown, event, resp),
                            Err(e) => {
                                tracing::info!(target: "diag", "tracker ANNOUNCE fail url={shown} event={event:?} err={}", redacted_tracker_error(e, &url));
                            }
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
            _ = shutdown.changed() => break,
            Some(joined) = announces.join_next(), if !announces.is_empty() => joined,
            _ = wake.changed() => continue,
            _ = tokio::time::sleep(sleep_for) => continue,
        };
        let Ok((ti, ui, event, result, request_generation, finished_at_send)) = done else {
            continue;
        };
        inflight.remove(&(ti, ui));
        match result {
            Ok(resp) => {
                let finished_now = ctx.stats.lock().finished;
                {
                    let mut lifecycle = ctx.lifecycle.lock();
                    let state = lifecycle.run_state(info_hash, tiers);
                    if let Some(entry) = state.tiers.get_mut(ti).and_then(|t| t.get_mut(ui)) {
                        entry.record_success(
                            event,
                            finished_at_send,
                            finished_now,
                            &resp,
                            Instant::now(),
                        );
                    }
                }
                publish_swarm_counts(&ctx.lifecycle, &ctx.stats);
                let mse_first = ti < ctx.obfuscated_tiers;
                if !ctx
                    .forward_peers(shutdown, resp.peers, request_generation, mse_first)
                    .await
                {
                    break;
                }
            }
            Err(e) => {
                let mut lifecycle = ctx.lifecycle.lock();
                let state = lifecycle.run_state(info_hash, tiers);
                if let Some(entry) = state.tiers.get_mut(ti).and_then(|t| t.get_mut(ui)) {
                    entry.record_failure(&e, Instant::now());
                }
            }
        }
    }
}

async fn run_tracker_poller(
    ctx: TrackerCtx,
    tiers: Vec<Vec<String>>,
    info_hash: Id20,
    mut shutdown: watch::Receiver<Option<bool>>,
) {
    if tiers.is_empty() {
        return;
    }
    let pending_stop = ctx.lifecycle.lock().pending_stop();
    if let Some(mut pending_stop) = pending_stop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => return,
            _ = pending_stop.changed() => {}
        }
    }
    if ctx.tier_change_tx.is_some() {
        run_tracker_tier_poller(&ctx, &tiers, info_hash, &mut shutdown).await;
    } else {
        run_tracker_swarm_poller(&ctx, &tiers, info_hash, &mut shutdown).await;
    }
    if (*shutdown.borrow()).unwrap_or(true) {
        announce_stopped(&ctx, &tiers, info_hash).await;
    }
}

fn spawn_tracker_tier_pollers(
    ctx: TrackerCtx,
    tiers: Vec<Vec<String>>,
    info_hashes: &[Id20],
) -> TrackerPollers {
    let (shutdown_tx, shutdown_rx) = watch::channel(None);
    let mut tasks = tokio::task::JoinSet::new();
    if !tiers.is_empty() {
        for &info_hash in info_hashes {
            tasks.spawn(run_tracker_poller(
                ctx.clone(),
                tiers.clone(),
                info_hash,
                shutdown_rx.clone(),
            ));
        }
    }
    TrackerPollers { shutdown_tx, tasks }
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
        TrackerCtx {
            tx,
            peer_id,
            key,
            port,
            stats,
            proxy,
            lifecycle: Arc::new(Mutex::new(TrackerLifecycle::default())),
            tier_change_tx: None,
            source_addr: None,
            generation: GenerationToken::new(),
            obfuscated_tiers: 0,
            ranker_for: PeerRanker::observed,
        },
        tiers,
        &info_hashes,
    )
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
        let should_drop = piece_assemblies.get(&piece_idx).is_none_or(|a| {
            a.sources
                .iter()
                .flatten()
                .all(|source| *source == ChunkSource::Padding)
        });
        if should_drop {
            piece_assemblies.remove(&piece_idx);
        }
    }
}

type LeafBlocks = Arc<[super::core::Id32]>;

#[derive(Default)]
struct LeafHashCache {
    entries: HashMap<(usize, u32), (LeafBlocks, u64)>,
    order: VecDeque<((usize, u32), u64)>,
    bytes: usize,
    clock: u64,
}

impl LeafHashCache {
    fn get(&mut self, key: (usize, u32)) -> Option<LeafBlocks> {
        let (blocks, used) = self.entries.get_mut(&key)?;
        self.clock += 1;
        *used = self.clock;
        let blocks = Arc::clone(blocks);
        self.order.push_back((key, self.clock));
        self.compact();
        Some(blocks)
    }

    fn insert(&mut self, key: (usize, u32), blocks: LeafBlocks) {
        let size = blocks.len() * 32;
        if size > LEAF_HASH_CACHE_BYTES {
            return;
        }
        if let Some((old, _)) = self.entries.remove(&key) {
            self.bytes -= old.len() * 32;
        }
        while self.bytes + size > LEAF_HASH_CACHE_BYTES {
            let Some((oldest, stamp)) = self.order.pop_front() else {
                break;
            };
            if self
                .entries
                .get(&oldest)
                .is_some_and(|(_, used)| *used == stamp)
            {
                if let Some((evicted, _)) = self.entries.remove(&oldest) {
                    self.bytes -= evicted.len() * 32;
                }
            }
        }
        self.clock += 1;
        self.bytes += size;
        self.entries.insert(key, (blocks, self.clock));
        self.order.push_back((key, self.clock));
        self.compact();
    }

    fn compact(&mut self) {
        if self.order.len() > 2 * self.entries.len() + 64 {
            let entries = &self.entries;
            self.order
                .retain(|(key, stamp)| entries.get(key).is_some_and(|(_, used)| used == stamp));
        }
    }
}

#[derive(Clone)]
struct LeafHashPool {
    jobs: Arc<Semaphore>,
    cache: Arc<Mutex<LeafHashCache>>,
    limiter: Throttle,
    reads: Arc<RateLimiter>,
}

impl LeafHashPool {
    fn new(jobs: usize, limiter: Throttle) -> Self {
        Self {
            jobs: Arc::new(Semaphore::new(jobs)),
            cache: Arc::new(Mutex::new(LeafHashCache::default())),
            limiter,
            reads: Arc::new(RateLimiter::new(LEAF_HASH_READ_BPS)),
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
    tables: Option<&Arc<Vec<MerkleProofTable>>>,
    pool: &LeafHashPool,
    peer_slots: &Arc<Semaphore>,
    storage: &Arc<FilesystemStorage>,
    piece_tracker: &PieceTracker,
    cmd_tx: mpsc::Sender<PeerCommand>,
) {
    let lengths = piece_tracker.lengths();
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
        all_local.then(|| (Arc::clone(tables), k, file_offset, pieces))
    })();
    let peer_permit = plan
        .as_ref()
        .and_then(|_| Arc::clone(peer_slots).try_acquire_owned().ok());
    let (Some((tables, k, file_offset, pieces)), Some(peer_permit)) = (plan, peer_permit) else {
        let _ = cmd_tx.try_send(PeerCommand::Send(request.reject()));
        return;
    };
    let storage = Arc::clone(storage);
    let pool = pool.clone();
    tokio::spawn(async move {
        let _peer_permit = peer_permit;
        let table = &tables[k];
        let mut piece_blocks = HashMap::with_capacity(pieces.len());
        let mut missing = Vec::new();
        {
            let mut cache = pool.cache.lock();
            for p in pieces {
                match cache.get((k, p)) {
                    Some(blocks) => {
                        piece_blocks.insert(p, blocks);
                    }
                    None => missing.push(p),
                }
            }
        }
        if !missing.is_empty() {
            let loaded: Option<Vec<(u32, LeafBlocks)>> = 'load: {
                let Ok(_permit) = Arc::clone(&pool.jobs).acquire_owned().await else {
                    return;
                };
                if cmd_tx.is_closed() {
                    return;
                }
                let mut loaded = Vec::with_capacity(missing.len());
                for p in missing {
                    let cached = pool.cache.lock().get((k, p));
                    let blocks = match cached {
                        Some(blocks) => blocks,
                        None => {
                            let start = p as u64 * table.piece_length as u64;
                            let len =
                                (table.file_length - start).min(table.piece_length as u64) as usize;
                            pool.reads.acquire(len).await;
                            let Ok(data) = storage.read_owned(file_offset + start, len).await
                            else {
                                break 'load None;
                            };
                            let blocks = tokio::task::spawn_blocking(move || {
                                data.chunks(super::core::merkle::BLOCK_SIZE as usize)
                                    .map(super::core::merkle::hash_block)
                                    .collect::<Vec<_>>()
                            })
                            .await
                            .unwrap_or_default();
                            let blocks: LeafBlocks = Arc::from(blocks);
                            pool.cache.lock().insert((k, p), Arc::clone(&blocks));
                            blocks
                        }
                    };
                    loaded.push((p, blocks));
                }
                Some(loaded)
            };
            let Some(loaded) = loaded else {
                let _ = cmd_tx.send(PeerCommand::Send(request.reject())).await;
                return;
            };
            piece_blocks.extend(loaded);
        }
        let message = match table.answer_leaf_request(
            request.index,
            request.length,
            request.proof_layers,
            &piece_blocks,
        ) {
            Some(hashes) => {
                pool.limiter.acquire(hashes.len()).await;
                Message::Hashes {
                    pieces_root: request.pieces_root,
                    base_layer: 0,
                    index: request.index,
                    length: request.length,
                    proof_layers: request.proof_layers,
                    hashes: Bytes::from(hashes),
                }
            }
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
    fn tracker_ctx(
        tx: mpsc::Sender<PeerEnvelope>,
        stats: Arc<Mutex<TorrentStats>>,
        lifecycle: Arc<Mutex<TrackerLifecycle>>,
        tier_change_tx: Option<TierChangeTx>,
    ) -> TrackerCtx {
        TrackerCtx {
            tx,
            peer_id: Id20::new([0; 20]),
            key: 1,
            port: 6881,
            stats,
            proxy: None,
            lifecycle,
            tier_change_tx,
            source_addr: None,
            generation: GenerationToken::new(),
            obfuscated_tiers: 0,
            ranker_for: PeerRanker::observed,
        }
    }

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
    use crate::test_util::eventually;
    use std::path::Path;

    fn test_peer(port: u16) -> SocketAddr {
        SocketAddr::from(([203, 0, 113, 1], port))
    }

    #[test]
    fn tracker_authorization_stays_bounded_and_keeps_known_peers() {
        let mut authorized = HashSet::new();
        let kept = test_peer(1);
        let known = HashSet::from([kept]);
        authorize_tracker_peer(&mut authorized, &known, kept);
        for n in 0..(MAX_TRACKER_AUTHORIZED as u32 * 3) {
            let addr = SocketAddr::from(([10, (n >> 16) as u8, (n >> 8) as u8, n as u8], 6881));
            authorize_tracker_peer(&mut authorized, &known, addr);
            assert!(authorized.len() <= MAX_TRACKER_AUTHORIZED);
            assert!(authorized.contains(&addr));
        }
        assert!(authorized.contains(&kept));
        let crowded: HashSet<SocketAddr> = (0..MAX_TRACKER_AUTHORIZED as u32)
            .map(|n| SocketAddr::from(([10, 1, (n >> 8) as u8, n as u8], 6881)))
            .collect();
        let mut authorized = crowded.clone();
        authorize_tracker_peer(&mut authorized, &crowded, test_peer(2));
        assert_eq!(authorized, HashSet::from([test_peer(2)]));
    }

    #[test]
    fn dht_announce_peer_is_repeated_only_every_fifteen_minutes() {
        let t0 = Instant::now();
        let minutes = |m: u64| t0 + Duration::from_secs(m * 60);
        assert!(dht_announce_due(None, t0));
        assert!(!dht_announce_due(Some(t0), minutes(2)));
        assert!(!dht_announce_due(Some(t0), minutes(14)));
        assert!(dht_announce_due(Some(t0), minutes(15)));
        assert!(dht_announce_due(Some(t0), minutes(40)));
        assert!(!dht_announce_due(Some(minutes(5)), t0));
    }

    async fn dht_round_with(
        announces: Vec<Option<bool>>,
        peers_per_lookup: usize,
    ) -> (Option<Vec<bool>>, Vec<SocketAddr>) {
        let targets: Vec<(Id20, Option<u16>)> = (0..announces.len() as u8)
            .map(|i| (Id20([i; 20]), Some(6881)))
            .collect();
        let (peer_tx, mut peer_rx) = mpsc::channel(16);
        let generation = GenerationToken::new();
        let outcomes = std::cell::RefCell::new(announces.into_iter());
        let announced = tokio::time::timeout(
            Duration::from_secs(60),
            dht_poll_round(
                &targets,
                &peer_tx,
                &generation,
                |_| {},
                |_, port| {
                    assert_eq!(port, Some(6881));
                    let outcome = outcomes.borrow_mut().next().unwrap();
                    let (tx, rx) = mpsc::unbounded_channel();
                    let (done_tx, done_rx) = oneshot::channel();
                    for _ in 0..peers_per_lookup {
                        tx.send(test_peer(6881)).unwrap();
                    }
                    drop(tx);
                    if let Some(sent) = outcome {
                        done_tx.send(sent).unwrap();
                    }
                    (rx, done_rx)
                },
            ),
        )
        .await
        .expect("round did not finish");
        drop(peer_tx);
        let mut found = Vec::new();
        while let Some(envelope) = peer_rx.recv().await {
            assert_eq!(envelope.candidate.source, PeerSource::Dht);
            found.push(envelope.candidate.addr);
        }
        (announced, found)
    }

    #[tokio::test(start_paused = true)]
    async fn a_dht_round_reports_which_lookups_sent_an_announce() {
        let (announced, found) = dht_round_with(vec![Some(true)], 1).await;
        assert_eq!(announced, Some(vec![true]));
        assert_eq!(found.len(), 1);

        let (announced, _) = dht_round_with(vec![Some(false)], 3).await;
        assert_eq!(
            announced,
            Some(vec![false]),
            "peers alone are not an announce"
        );

        let (announced, _) = dht_round_with(vec![None], 0).await;
        assert_eq!(announced, Some(vec![false]), "a lookup that never settled");

        let (announced, found) = dht_round_with(vec![Some(true), Some(true)], 1).await;
        assert_eq!(announced, Some(vec![true, true]));
        assert_eq!(found.len(), 2);

        let (announced, _) = dht_round_with(vec![Some(true), Some(false)], 1).await;
        assert_eq!(announced, Some(vec![true, false]));
    }

    #[test]
    fn a_hybrid_hash_that_failed_to_announce_does_not_make_its_sibling_repeat() {
        let hashes = [Id20([1; 20]), Id20([2; 20])];
        let t0 = Instant::now();
        let minutes = |m: u64| t0 + Duration::from_secs(m * 60);
        let mut last = vec![None; 2];

        let targets = dht_announce_targets(&hashes, &last, 6881, t0);
        assert_eq!(targets, [(hashes[0], Some(6881)), (hashes[1], Some(6881))]);
        record_dht_announces(&mut last, &targets, &[true, false], t0);
        assert_eq!(last, [Some(t0), None]);

        let targets = dht_announce_targets(&hashes, &last, 6881, minutes(2));
        assert_eq!(targets, [(hashes[0], None), (hashes[1], Some(6881))]);
        record_dht_announces(&mut last, &targets, &[true, true], minutes(2));
        assert_eq!(last, [Some(t0), Some(minutes(2))]);

        let targets = dht_announce_targets(&hashes, &last, 6881, minutes(16));
        assert_eq!(targets, [(hashes[0], Some(6881)), (hashes[1], None)]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_dht_burst_is_dialed_highest_priority_first() {
        let ranker = elected_ranker();
        let pool = priority_pool(12);
        let mut burst = pool.clone();
        burst.reverse();
        let (tx, rx) = mpsc::unbounded_channel();
        for addr in &burst {
            tx.send(*addr).unwrap();
        }
        drop(tx);
        let (done_tx, done_rx) = oneshot::channel();
        done_tx.send(true).unwrap();
        let lookup = std::cell::RefCell::new(Some((rx, done_rx)));
        let (peer_tx, mut peer_rx) = mpsc::channel(32);
        let announced = tokio::time::timeout(
            Duration::from_secs(60),
            dht_poll_round(
                &[(Id20([1; 20]), None)],
                &peer_tx,
                &GenerationToken::new(),
                |burst| ranker.order_batch(burst),
                |_, _| lookup.borrow_mut().take().unwrap(),
            ),
        )
        .await
        .expect("round did not finish");
        assert_eq!(announced, Some(vec![true]));
        drop(peer_tx);
        let mut found = Vec::new();
        while let Some(envelope) = peer_rx.recv().await {
            found.push(envelope.candidate.addr);
        }
        assert_eq!(found, pool);
    }

    #[tokio::test(start_paused = true)]
    async fn a_streamed_dht_peer_is_never_held_back_to_fill_a_burst() {
        let ranker = elected_ranker();
        let pool = priority_pool(2);
        let (tx, rx) = mpsc::unbounded_channel();
        let (done_tx, done_rx) = oneshot::channel();
        let lookup = std::cell::RefCell::new(Some((rx, done_rx)));
        let (peer_tx, mut peer_rx) = mpsc::channel(8);
        let generation = GenerationToken::new();
        let round = dht_poll_round(
            &[(Id20([1; 20]), None)],
            &peer_tx,
            &generation,
            |burst| ranker.order_batch(burst),
            |_, _| lookup.borrow_mut().take().unwrap(),
        );
        let drive = async {
            for addr in [pool[1], pool[0]] {
                tx.send(addr).unwrap();
                let envelope = tokio::time::timeout(Duration::from_secs(5), peer_rx.recv())
                    .await
                    .expect("peer was held back")
                    .expect("channel closed");
                assert_eq!(envelope.candidate.addr, addr);
            }
            drop(tx);
            done_tx.send(true).unwrap();
        };
        let (announced, ()) = tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(round, drive)
        })
        .await
        .expect("round did not finish");
        assert_eq!(announced, Some(vec![true]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_dht_round_stops_when_the_torrent_stops_listening() {
        let (peer_tx, peer_rx) = mpsc::channel(1);
        drop(peer_rx);
        let announced = tokio::time::timeout(
            Duration::from_secs(60),
            dht_poll_round(
                &[(Id20([1; 20]), None)],
                &peer_tx,
                &GenerationToken::new(),
                |_| {},
                |_, _| {
                    let (tx, rx) = mpsc::unbounded_channel();
                    tx.send(test_peer(6881)).unwrap();
                    (rx, oneshot::channel().1)
                },
            ),
        )
        .await
        .expect("round did not finish");
        assert_eq!(announced, None);
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

    struct HolepunchFixture {
        peers: HashMap<u32, Peer>,
        pending_dials: HashMap<u32, SocketAddr>,
        priority_backlog: VecDeque<SocketAddr>,
        peer_backlog: VecDeque<SocketAddr>,
        dial_retries: VecDeque<(SocketAddr, Instant)>,
        useful_redials: VecDeque<(SocketAddr, Instant)>,
        known_addrs: HashSet<SocketAddr>,
        holepunch: HolepunchState,
        blocklist: RwLock<BlockList>,
        now: Instant,
    }

    impl HolepunchFixture {
        fn new() -> Self {
            Self {
                peers: HashMap::new(),
                pending_dials: HashMap::new(),
                priority_backlog: VecDeque::new(),
                peer_backlog: VecDeque::new(),
                dial_retries: VecDeque::new(),
                useful_redials: VecDeque::new(),
                known_addrs: HashSet::new(),
                holepunch: HolepunchState::new(true, 6881, Some(6881)),
                blocklist: RwLock::new(BlockList::default()),
                now: Instant::now(),
            }
        }

        fn handle(&mut self, hp: HolepunchMsg, from: &HolepunchSender, budget: &mut TokenBucket) {
            handle_holepunch(
                hp,
                from,
                &self.peers,
                &self.pending_dials,
                &mut self.priority_backlog,
                &mut self.peer_backlog,
                &mut self.dial_retries,
                &mut self.useful_redials,
                &mut self.known_addrs,
                &mut self.holepunch,
                budget,
                self.now,
                &self.blocklist,
            );
        }

        fn queued(&self) -> usize {
            self.priority_backlog.len()
                + self.peer_backlog.len()
                + self.dial_retries.len()
                + self.useful_redials.len()
        }
    }

    fn connect_msg(addr: SocketAddr) -> HolepunchMsg {
        HolepunchMsg {
            msg_type: holepunch_type::CONNECT,
            addr,
            err_code: 0,
        }
    }

    fn rendezvous_msg(addr: SocketAddr) -> HolepunchMsg {
        HolepunchMsg {
            msg_type: holepunch_type::RENDEZVOUS,
            addr,
            err_code: 0,
        }
    }

    fn hp_sender(
        pid: u32,
        addr: SocketAddr,
        cmd: &mpsc::Sender<PeerCommand>,
    ) -> HolepunchSender<'_> {
        HolepunchSender {
            pid,
            addr,
            hp_id: Some(7),
            their_view_of_us: None,
            elected: ExternalAddresses::default(),
            cmd,
        }
    }

    fn holepunch_budget(now: Instant) -> TokenBucket {
        TokenBucket::new(HOLEPUNCH_CONNECT_BURST, HOLEPUNCH_CONNECT_REFILL, now)
    }

    fn sent_holepunch(cmd: PeerCommand) -> (u8, HolepunchMsg) {
        match cmd {
            PeerCommand::Send(Message::Extended { ext_id, payload }) => (
                ext_id,
                parse_holepunch(&payload).expect("holepunch payload"),
            ),
            other => panic!("expected holepunch message, got {other:?}"),
        }
    }

    #[test]
    fn holepunch_connect_evicts_the_oldest_target_at_the_cap() {
        let mut fx = HolepunchFixture::new();
        let target = test_peer(14_101);
        let stale = SocketAddr::from(([203, 0, 113, 1], 20_000));
        for n in 0..MAX_HOLEPUNCH_ENTRIES {
            fx.holepunch.note_target(
                SocketAddr::from(([203, 0, 113, 1], 20_000 + n as u16)),
                fx.now + Duration::from_millis(n as u64),
            );
        }
        let (cmd, _rx) = mpsc::channel(1);
        let mut budget = holepunch_budget(fx.now);

        fx.handle(
            connect_msg(target),
            &hp_sender(1, test_peer(14_102), &cmd),
            &mut budget,
        );

        assert_eq!(fx.priority_backlog, VecDeque::from([target]));
        assert_eq!(fx.holepunch.targets.len(), MAX_HOLEPUNCH_ENTRIES);
        assert!(fx.holepunch.targets.contains_key(&target));
        assert!(!fx.holepunch.targets.contains_key(&stale));
    }

    #[test]
    fn holepunch_connect_skips_blocked_target() {
        let mut fx = HolepunchFixture::new();
        let target = test_peer(14_001);
        fx.blocklist.write().replace(&[target.ip().to_string()]);
        let (cmd, _rx) = mpsc::channel(1);
        let mut budget = holepunch_budget(fx.now);

        fx.handle(
            connect_msg(target),
            &hp_sender(1, test_peer(14_002), &cmd),
            &mut budget,
        );

        assert_eq!(fx.queued(), 0);
        assert!(fx.known_addrs.is_empty());
        assert!(fx.holepunch.targets.is_empty());
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
        let (tiers, obfuscated_count) = collect_announce_tiers(&meta);
        assert_eq!(obfuscated_count, 1);
        assert_eq!(
            tiers,
            vec![
                vec!["http://a/announce".to_string()],
                vec!["udp://c:80".to_string()],
            ]
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
                sources: vec![Some(ChunkSource::Peer("192.0.2.9".parse().unwrap())), None],
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
                    executable: false,
                },
                TorrentMetaInfo {
                    path: vec![".pad".into(), "5".into()],
                    length: 5,
                    padding: true,
                    executable: false,
                },
                TorrentMetaInfo {
                    path: vec!["b".into()],
                    length: 10,
                    padding: false,
                    executable: false,
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
                    executable: false,
                },
                TorrentMetaInfo {
                    path: vec!["sub".into(), "b".into()],
                    length: 15,
                    padding: false,
                    executable: false,
                },
            ],
            single_file_mode: false,
        };
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let storage = FilesystemStorage::new(&info, &root)
            .with_parts_dir(crate::storage::parts_dir_for(&root, "abcd"));
        let (in_effect, applied, promoted) = apply_storage_selection(&storage, Some(vec![0])).await;
        assert_eq!(in_effect, Some(vec![0]));
        assert!(applied.is_ok());
        assert!(promoted.is_empty());

        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("sub"), b"").unwrap();
        let (in_effect, applied, promoted) = apply_storage_selection(&storage, None).await;
        assert_eq!(in_effect, Some(vec![0]));
        assert!(promoted.is_empty());
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
        let (in_effect, applied, promoted) = apply_storage_selection(&storage, None).await;
        assert_eq!(in_effect, None);
        assert!(applied.is_ok());
        assert_eq!(promoted, vec![1]);
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
        let mut fx = HolepunchFixture::new();
        let target_listen = test_peer(14_011);
        let (target_tx, mut target_rx) = mpsc::channel(4);
        let mut target = Peer::connected(test_peer(51_000), target_tx, 1, 1, 1, false, false, None);
        target.listen_addr = Some(target_listen);
        target.their_ut_holepunch_id = Some(9);
        fx.peers.insert(1, target);
        let initiator = SocketAddr::from(([198, 51, 100, 7], 14_012));
        let (from_cmd, mut from_rx) = mpsc::channel(4);
        let mut budget = holepunch_budget(fx.now);

        fx.handle(
            rendezvous_msg(target_listen),
            &hp_sender(2, initiator, &from_cmd),
            &mut budget,
        );

        let (ext, msg) = sent_holepunch(target_rx.try_recv().unwrap());
        assert_eq!(
            (ext, msg.msg_type, msg.addr),
            (9, holepunch_type::CONNECT, initiator)
        );
        let (ext, msg) = sent_holepunch(from_rx.try_recv().unwrap());
        assert_eq!(
            (ext, msg.msg_type, msg.addr),
            (7, holepunch_type::CONNECT, target_listen)
        );
    }

    #[test]
    fn holepunch_connect_marks_target_for_utp_first_dial() {
        let mut fx = HolepunchFixture::new();
        let target = test_peer(14_003);
        let (cmd, _rx) = mpsc::channel(1);
        let mut budget = holepunch_budget(fx.now);

        fx.handle(
            connect_msg(target),
            &hp_sender(1, test_peer(14_004), &cmd),
            &mut budget,
        );

        assert_eq!(fx.priority_backlog, VecDeque::from([target]));
        assert!(fx.holepunch.is_target(&target, fx.now));
    }

    #[test]
    fn holepunch_connect_flood_from_one_peer_is_rate_limited() {
        let mut fx = HolepunchFixture::new();
        fx.peer_backlog = (0..MAX_PEER_BACKLOG)
            .map(|i| SocketAddr::from(([203, 0, 113, 2], 10_000 + i as u16)))
            .collect();
        let preloaded = fx.peer_backlog.iter().copied().collect::<HashSet<_>>();
        fx.known_addrs = preloaded.clone();
        let (cmd, _rx) = mpsc::channel(1);
        let sender = hp_sender(1, test_peer(14_200), &cmd);
        let mut budget = holepunch_budget(fx.now);

        for n in 0..1100u32 {
            let target = SocketAddr::from(([198, 51, 100, (n % 200) as u8 + 1], 20_000 + n as u16));
            fx.handle(connect_msg(target), &sender, &mut budget);
        }

        let accepted = fx.priority_backlog.len();
        assert_eq!(accepted, HOLEPUNCH_CONNECT_BURST as usize);
        assert_eq!(fx.holepunch.targets.len(), accepted);
        assert!(fx.queued() <= MAX_PEER_BACKLOG);
        let survivors = fx
            .peer_backlog
            .iter()
            .filter(|addr| preloaded.contains(addr))
            .count();
        assert_eq!(survivors, MAX_PEER_BACKLOG - accepted);
    }

    #[test]
    fn holepunch_connect_budget_refills_over_time() {
        let mut fx = HolepunchFixture::new();
        let (cmd, _rx) = mpsc::channel(1);
        let sender = hp_sender(1, test_peer(14_210), &cmd);
        let mut budget = holepunch_budget(fx.now);
        let target = |n: u16| SocketAddr::from(([198, 51, 100, 9], 30_000 + n));

        for n in 0..HOLEPUNCH_CONNECT_BURST as u16 + 2 {
            fx.handle(connect_msg(target(n)), &sender, &mut budget);
        }
        assert_eq!(fx.priority_backlog.len(), HOLEPUNCH_CONNECT_BURST as usize);

        fx.now += HOLEPUNCH_CONNECT_REFILL;
        fx.handle(connect_msg(target(500)), &sender, &mut budget);
        fx.handle(connect_msg(target(501)), &sender, &mut budget);
        assert_eq!(
            fx.priority_backlog.len(),
            HOLEPUNCH_CONNECT_BURST as usize + 1
        );
        assert_eq!(fx.priority_backlog.front(), Some(&target(500)));
    }

    #[test]
    fn reconnecting_does_not_refill_the_holepunch_connect_budget() {
        let mut fx = HolepunchFixture::new();
        let mut budgets = IpBudgets::default();
        let source = test_peer(14_260);
        let (cmd, _rx) = mpsc::channel(1);
        let target =
            |pid: u32, n: u16| SocketAddr::from(([198, 51, 100, 9], 20_000 + pid as u16 * 100 + n));
        let connect_all = |fx: &mut HolepunchFixture,
                           budgets: &mut IpBudgets,
                           pid: u32,
                           ip: IpAddr,
                           count: u16| {
            let sender = hp_sender(pid, source, &cmd);
            for n in 0..count {
                let budget = &mut budgets.entry(ip, fx.now).connect;
                fx.handle(connect_msg(target(pid, n)), &sender, budget);
            }
        };

        let (tx, _peer_rx) = mpsc::channel(1);
        fx.peers
            .insert(1, Peer::connected(source, tx, 1, 1, 1, false, false, None));
        connect_all(&mut fx, &mut budgets, 1, source.ip(), 20);
        assert_eq!(fx.priority_backlog.len(), HOLEPUNCH_CONNECT_BURST as usize);

        fx.peers.remove(&1);
        let mapped = match source.ip() {
            IpAddr::V4(v4) => IpAddr::V6(v4.to_ipv6_mapped()),
            IpAddr::V6(_) => unreachable!(),
        };
        for pid in 2..30u32 {
            let (tx, _peer_rx) = mpsc::channel(1);
            fx.peers.insert(
                pid,
                Peer::connected(source, tx, 1, 1, 1, false, false, None),
            );
            let ip = if pid % 2 == 0 { source.ip() } else { mapped };
            connect_all(&mut fx, &mut budgets, pid, ip, 20);
            fx.peers.remove(&pid);
        }

        assert_eq!(fx.priority_backlog.len(), HOLEPUNCH_CONNECT_BURST as usize);
        assert_eq!(budgets.entries.len(), 1);

        fx.now += HOLEPUNCH_CONNECT_REFILL;
        connect_all(&mut fx, &mut budgets, 40, source.ip(), 5);
        assert_eq!(
            fx.priority_backlog.len(),
            HOLEPUNCH_CONNECT_BURST as usize + 1
        );
    }

    #[test]
    fn unsolicited_holepunch_connects_share_one_torrent_wide_budget() {
        let mut fx = HolepunchFixture::new();
        let mut budgets = IpBudgets::default();
        let (cmd, _rx) = mpsc::channel(1);
        let target = |n: u16| SocketAddr::from(([198, 51, 100, 9], 34_000 + n));
        let mut send = |fx: &mut HolepunchFixture, n: u16| {
            let source = SocketAddr::from(([203, 0, 113, (n % 200) as u8 + 1], 40_000 + n));
            let sender = hp_sender(u32::from(n) + 1, source, &cmd);
            let budget = &mut budgets.entry(source.ip(), fx.now).connect;
            fx.handle(connect_msg(target(n)), &sender, budget);
        };

        for n in 0..64 {
            send(&mut fx, n);
        }
        assert_eq!(
            fx.priority_backlog.len(),
            HOLEPUNCH_UNSOLICITED_BURST as usize
        );
        assert_eq!(
            fx.holepunch.targets.len(),
            HOLEPUNCH_UNSOLICITED_BURST as usize
        );

        fx.now += HOLEPUNCH_UNSOLICITED_REFILL;
        send(&mut fx, 100);
        send(&mut fx, 101);
        assert_eq!(
            fx.priority_backlog.len(),
            HOLEPUNCH_UNSOLICITED_BURST as usize + 1
        );
        assert_eq!(fx.priority_backlog.front(), Some(&target(100)));
    }

    #[test]
    fn solicited_and_invalid_connects_leave_the_torrent_wide_budget_alone() {
        let mut fx = HolepunchFixture::new();
        let (cmd, _rx) = mpsc::channel(1);
        let relay = hp_sender(5, test_peer(14_270), &cmd);
        let public = hp_sender(6, test_peer(14_271), &cmd);
        let target = |n: u16| SocketAddr::from(([198, 51, 100, 11], 35_000 + n));

        for n in 0..HOLEPUNCH_UNSOLICITED_BURST as u16 * 2 {
            let mut budget = holepunch_budget(fx.now);
            fx.handle(
                connect_msg(SocketAddr::from(([10, 0, 0, 1], 6881 + n))),
                &public,
                &mut budget,
            );
        }
        assert_eq!(fx.queued(), 0);

        for n in 0..HOLEPUNCH_UNSOLICITED_BURST as u16 * 2 {
            fx.holepunch.note_initiated(target(n), 5, fx.now);
            let mut budget = holepunch_budget(fx.now);
            fx.handle(connect_msg(target(n)), &relay, &mut budget);
        }
        assert_eq!(
            fx.priority_backlog.len(),
            HOLEPUNCH_UNSOLICITED_BURST as usize * 2
        );

        let mut budget = holepunch_budget(fx.now);
        for n in 0..HOLEPUNCH_UNSOLICITED_BURST as u16 + 2 {
            fx.handle(connect_msg(target(1000 + n)), &public, &mut budget);
        }
        assert_eq!(
            fx.priority_backlog.len(),
            HOLEPUNCH_UNSOLICITED_BURST as usize * 2 + HOLEPUNCH_CONNECT_BURST as usize
        );
    }

    #[test]
    fn reconnecting_does_not_reopen_the_pex_gap_or_refill_the_pex_budget() {
        let t0 = Instant::now();
        let ip = IpAddr::from([198, 51, 100, 30]);
        let mut budgets = IpBudgets::default();

        let first = budgets.entry(ip, t0);
        assert!(first.open_pex_gap(t0));
        assert_eq!(first.pex.take(PEX_ACCEPT_BURST, t0), PEX_ACCEPT_BURST);

        let reconnected = t0 + Duration::from_secs(1);
        let again = budgets.entry(ip, reconnected);
        assert!(!again.open_pex_gap(reconnected));
        assert_eq!(again.pex.take(1, reconnected), 0);

        let after_gap = t0 + PEX_MIN_INBOUND_GAP;
        let again = budgets.entry(ip, after_gap);
        assert!(again.open_pex_gap(after_gap));
        assert_eq!(again.pex.take(PEX_ACCEPT_BURST, after_gap), 3);
        assert_eq!(budgets.entries.len(), 1);
    }

    #[test]
    fn ip_budgets_stay_bounded_and_are_pruned_only_once_settled() {
        let t0 = Instant::now();
        let ip = |n: usize| IpAddr::from([198, 51, (n / 250) as u8, (n % 250) as u8 + 1]);
        let mut budgets = IpBudgets::default();
        for n in 0..MAX_IP_BUDGETS + 10 {
            let at = t0 + Duration::from_millis(n as u64);
            budgets.entry(ip(n), at).connect.take(1, at);
        }
        assert_eq!(budgets.entries.len(), MAX_IP_BUDGETS);
        assert!(!budgets.entries.contains_key(&ip(0)));
        assert!(budgets.entries.contains_key(&ip(MAX_IP_BUDGETS + 9)));

        budgets.prune(t0 + Duration::from_secs(1));
        assert_eq!(budgets.entries.len(), MAX_IP_BUDGETS);

        let drained = ip(MAX_IP_BUDGETS + 9);
        let at = t0 + Duration::from_secs(2);
        budgets.entry(drained, at).pex.take(PEX_ACCEPT_BURST, at);
        budgets.prune(t0 + Duration::from_secs(30));
        assert_eq!(budgets.entries.len(), 1);
        assert!(budgets.entries.contains_key(&drained));

        budgets.prune(at + PEX_ACCEPT_REFILL * PEX_ACCEPT_BURST + Duration::from_secs(1));
        assert!(budgets.entries.is_empty());
    }

    #[test]
    fn holepunch_messages_from_peers_without_the_extension_are_ignored() {
        let mut fx = HolepunchFixture::new();
        let (target_tx, mut target_rx) = mpsc::channel(4);
        let target_addr = test_peer(14_221);
        let mut target = Peer::connected(target_addr, target_tx, 1, 1, 1, true, false, None);
        target.their_ut_holepunch_id = Some(9);
        fx.peers.insert(1, target);
        let (cmd, mut rx) = mpsc::channel(4);
        let mut sender = hp_sender(2, test_peer(14_220), &cmd);
        sender.hp_id = None;
        let mut budget = holepunch_budget(fx.now);

        fx.handle(connect_msg(test_peer(14_222)), &sender, &mut budget);
        fx.handle(rendezvous_msg(target_addr), &sender, &mut budget);

        assert_eq!(fx.queued(), 0);
        assert!(target_rx.try_recv().is_err());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn answered_rendezvous_connect_skips_the_budget_exactly_once() {
        let mut fx = HolepunchFixture::new();
        let (cmd, _rx) = mpsc::channel(1);
        let relay = hp_sender(5, test_peer(14_230), &cmd);
        let other = hp_sender(6, test_peer(14_231), &cmd);
        let mut exhausted = holepunch_budget(fx.now);
        assert_eq!(
            exhausted.take(HOLEPUNCH_CONNECT_BURST, fx.now),
            HOLEPUNCH_CONNECT_BURST
        );
        let target = SocketAddr::from(([198, 51, 100, 20], 31_000));
        fx.holepunch.note_initiated(target, 5, fx.now);

        fx.handle(connect_msg(target), &other, &mut exhausted);
        assert_eq!(fx.queued(), 0);

        fx.handle(connect_msg(target), &relay, &mut exhausted);
        assert_eq!(fx.priority_backlog, VecDeque::from([target]));

        fx.priority_backlog.clear();
        fx.known_addrs.clear();
        fx.handle(connect_msg(target), &relay, &mut exhausted);
        assert_eq!(fx.queued(), 0);
    }

    #[test]
    fn holepunch_connect_ignores_local_targets_from_public_relays() {
        let (cmd, _rx) = mpsc::channel(1);
        let local_targets = [
            SocketAddr::from(([127, 0, 0, 1], 6881)),
            SocketAddr::from(([192, 168, 0, 5], 6881)),
            SocketAddr::from(([10, 1, 2, 3], 6881)),
            SocketAddr::from(([172, 16, 0, 9], 6881)),
            SocketAddr::from(([100, 64, 0, 1], 6881)),
            "[::1]:6881".parse().unwrap(),
            "[fc00::7]:6881".parse().unwrap(),
            "[::ffff:127.0.0.1]:6881".parse().unwrap(),
            "[::ffff:192.168.9.9]:6881".parse().unwrap(),
        ];
        let mut fx = HolepunchFixture::new();
        let public = hp_sender(1, test_peer(14_240), &cmd);
        for target in local_targets {
            let mut budget = holepunch_budget(fx.now);
            fx.handle(connect_msg(target), &public, &mut budget);
        }
        assert_eq!(fx.queued(), 0);
        assert!(fx.holepunch.targets.is_empty());

        let lan = hp_sender(2, SocketAddr::from(([192, 168, 0, 9], 6881)), &cmd);
        let mut budget = holepunch_budget(fx.now);
        fx.handle(connect_msg(local_targets[1]), &lan, &mut budget);
        fx.handle(connect_msg(local_targets[0]), &lan, &mut budget);
        assert_eq!(fx.priority_backlog, VecDeque::from([local_targets[1]]));

        let loopback = hp_sender(3, SocketAddr::from(([127, 0, 0, 1], 6882)), &cmd);
        fx.handle(connect_msg(local_targets[0]), &loopback, &mut budget);
        assert_eq!(fx.priority_backlog.front(), Some(&local_targets[0]));
    }

    #[test]
    fn holepunch_connect_canonicalizes_mapped_targets() {
        let mut fx = HolepunchFixture::new();
        let (cmd, _rx) = mpsc::channel(1);
        let mut budget = holepunch_budget(fx.now);

        fx.handle(
            connect_msg("[::ffff:198.51.100.80]:6881".parse().unwrap()),
            &hp_sender(1, test_peer(14_250), &cmd),
            &mut budget,
        );

        let canonical = SocketAddr::from(([198, 51, 100, 80], 6881));
        assert_eq!(fx.priority_backlog, VecDeque::from([canonical]));
        assert!(fx.holepunch.is_target(&canonical, fx.now));
    }

    #[test]
    fn peer_supplied_addresses_follow_the_locality_of_their_source() {
        let public = IpAddr::from([203, 0, 113, 5]);
        let lan = IpAddr::from([192, 168, 1, 2]);
        let loopback = IpAddr::from([127, 0, 0, 1]);
        let addr = |s: &str| s.parse::<SocketAddr>().unwrap();

        assert!(peer_supplied_dialable(public, addr("198.51.100.4:6881")));
        assert!(peer_supplied_dialable(public, addr("[2001:db8::4]:6881")));
        for local in [
            "127.0.0.1:6881",
            "10.0.0.5:6881",
            "172.31.255.1:6881",
            "192.168.0.9:6881",
            "100.127.1.1:6881",
            "0.1.2.3:6881",
            "[::1]:6881",
            "[fd12::1]:6881",
            "[::ffff:10.0.0.1]:6881",
        ] {
            assert!(!peer_supplied_dialable(public, addr(local)), "{local}");
        }
        assert!(peer_supplied_dialable(
            public,
            addr("[::ffff:198.51.100.4]:6881")
        ));
        assert!(peer_supplied_dialable(lan, addr("192.168.0.9:6881")));
        assert!(peer_supplied_dialable(lan, addr("[fd12::1]:6881")));
        assert!(peer_supplied_dialable(lan, addr("198.51.100.4:6881")));
        assert!(!peer_supplied_dialable(lan, addr("127.0.0.1:6881")));
        assert!(!peer_supplied_dialable(lan, addr("[::1]:6881")));
        assert!(peer_supplied_dialable(loopback, addr("127.0.0.1:6882")));
        assert!(peer_supplied_dialable(
            loopback,
            addr("[::ffff:127.0.0.1]:6882")
        ));
        assert!(!peer_supplied_dialable(
            public,
            addr("[::ffff:127.0.0.1]:6882")
        ));
        assert!(peer_supplied_dialable(
            IpAddr::from([100, 64, 3, 3]),
            addr("10.0.0.5:6881")
        ));
        assert!(!peer_supplied_dialable(public, addr("198.51.100.4:0")));
        assert!(!peer_supplied_dialable(public, addr("169.254.1.1:6881")));
        assert!(!peer_supplied_dialable(lan, addr("162.158.0.1:6881")));
    }

    #[test]
    fn pex_contacts_drop_local_addresses_for_public_sources() {
        let now = Instant::now();
        let mut budget = TokenBucket::new(PEX_ACCEPT_BURST, PEX_ACCEPT_REFILL, now);
        let added = vec![
            (SocketAddr::from(([127, 0, 0, 1], 6881)), 0),
            (SocketAddr::from(([10, 0, 0, 5], 6881)), 0),
            (SocketAddr::from(([192, 168, 0, 9], 6881)), 0),
            (SocketAddr::from(([198, 51, 100, 4], 6881)), 0),
        ];

        let public = admit_pex_contacts(
            IpAddr::from([203, 0, 113, 1]),
            added.clone(),
            false,
            &HashSet::new(),
            &mut budget,
            now,
        );
        assert_eq!(public, vec![added[3]]);

        let lan = admit_pex_contacts(
            IpAddr::from([192, 168, 0, 2]),
            added.clone(),
            false,
            &HashSet::new(),
            &mut budget,
            now,
        );
        assert_eq!(lan, added[1..].to_vec());

        let loopback = admit_pex_contacts(
            IpAddr::from([127, 0, 0, 1]),
            added.clone(),
            false,
            &HashSet::new(),
            &mut budget,
            now,
        );
        assert_eq!(loopback, added);
    }

    #[test]
    fn pex_contacts_keep_one_port_per_ip() {
        let now = Instant::now();
        let mut budget = TokenBucket::new(PEX_ACCEPT_BURST, PEX_ACCEPT_REFILL, now);
        let victim = IpAddr::from([198, 51, 100, 9]);
        let mut added: Vec<(SocketAddr, u8)> = (0..50u16)
            .map(|port| (SocketAddr::new(victim, 7000 + port), 0))
            .collect();
        added.push((SocketAddr::from(([198, 51, 100, 10], 6881)), 0));

        let admitted = admit_pex_contacts(
            IpAddr::from([203, 0, 113, 1]),
            added,
            false,
            &HashSet::new(),
            &mut budget,
            now,
        );

        assert_eq!(
            admitted,
            vec![
                (SocketAddr::new(victim, 7000), 0),
                (SocketAddr::from(([198, 51, 100, 10], 6881)), 0)
            ]
        );
    }

    #[test]
    fn pex_contacts_from_loopback_sources_keep_every_port() {
        let now = Instant::now();
        let mut budget = TokenBucket::new(PEX_ACCEPT_BURST, PEX_ACCEPT_REFILL, now);
        let added: Vec<(SocketAddr, u8)> = (0..5u16)
            .map(|port| (SocketAddr::from(([127, 0, 0, 1], 7000 + port)), 0))
            .collect();

        let admitted = admit_pex_contacts(
            IpAddr::from([127, 0, 0, 1]),
            added.clone(),
            false,
            &HashSet::new(),
            &mut budget,
            now,
        );

        assert_eq!(admitted, added);
    }

    #[test]
    fn pex_contacts_skip_seeds_once_the_torrent_is_complete() {
        let now = Instant::now();
        let mut budget = TokenBucket::new(PEX_ACCEPT_BURST, PEX_ACCEPT_REFILL, now);
        let seed = (SocketAddr::from(([198, 51, 100, 4], 6881)), pex_flag::SEED);
        let leecher = (SocketAddr::from(([198, 51, 100, 5], 6881)), 0);

        let source = IpAddr::from([203, 0, 113, 1]);
        assert_eq!(
            admit_pex_contacts(
                source,
                vec![seed, leecher],
                true,
                &HashSet::new(),
                &mut budget,
                now
            ),
            vec![leecher]
        );
        assert_eq!(
            admit_pex_contacts(
                source,
                vec![seed, leecher],
                false,
                &HashSet::new(),
                &mut budget,
                now
            ),
            vec![seed, leecher]
        );
    }

    #[test]
    fn pex_budget_is_only_spent_on_unknown_contacts() {
        let now = Instant::now();
        let mut budget = TokenBucket::new(PEX_ACCEPT_BURST, PEX_ACCEPT_REFILL, now);
        assert_eq!(budget.take(PEX_ACCEPT_BURST, now), PEX_ACCEPT_BURST);
        let known: Vec<(SocketAddr, u8)> = (1..=50u8)
            .map(|host| (SocketAddr::from(([198, 51, 100, host], 6881)), 0))
            .collect();
        let fresh: Vec<(SocketAddr, u8)> = (1..=5u8)
            .map(|host| (SocketAddr::from(([198, 51, 101, host], 6881)), 0))
            .collect();
        let known_addrs = known.iter().map(|(addr, _)| *addr).collect::<HashSet<_>>();
        let mut added = known.clone();
        added.extend(fresh);

        let admitted = admit_pex_contacts(
            IpAddr::from([203, 0, 113, 1]),
            added,
            false,
            &known_addrs,
            &mut budget,
            now,
        );

        assert_eq!(admitted, known);
    }

    #[test]
    fn pex_acceptance_is_budgeted_per_source() {
        let now = Instant::now();
        let mut budget = TokenBucket::new(PEX_ACCEPT_BURST, PEX_ACCEPT_REFILL, now);
        let source = IpAddr::from([203, 0, 113, 1]);
        let batch = |round: u8| -> Vec<(SocketAddr, u8)> {
            (0..50u8)
                .map(|host| {
                    (
                        SocketAddr::from(([198, 51, 100 + round, host + 1], 6881)),
                        0,
                    )
                })
                .collect()
        };

        let mut total = 0;
        for round in 0..20u8 {
            total += admit_pex_contacts(
                source,
                batch(round),
                false,
                &HashSet::new(),
                &mut budget,
                now,
            )
            .len();
        }
        assert_eq!(total, PEX_ACCEPT_BURST as usize);

        let later = now + PEX_ACCEPT_REFILL * 10;
        assert_eq!(
            admit_pex_contacts(
                source,
                batch(30),
                false,
                &HashSet::new(),
                &mut budget,
                later
            )
            .len(),
            10
        );
    }

    #[test]
    fn token_bucket_refills_gradually_and_never_exceeds_its_cap() {
        let now = Instant::now();
        let mut bucket = TokenBucket::new(4, Duration::from_secs(10), now);
        assert_eq!(bucket.take(10, now), 4);
        assert_eq!(bucket.take(1, now), 0);
        assert_eq!(bucket.take(1, now + Duration::from_secs(9)), 0);
        assert_eq!(bucket.take(1, now + Duration::from_secs(10)), 1);
        assert_eq!(bucket.take(10, now + Duration::from_secs(10_000)), 4);
        assert_eq!(bucket.take(1, now), 0);
    }

    #[test]
    fn pex_bookkeeping_waits_for_a_delivered_message() {
        let neighbour_addr = SocketAddr::from(([198, 51, 100, 30], 6881));
        let gone_addr = SocketAddr::from(([198, 51, 100, 31], 6881));
        let (tx, mut rx) = mpsc::channel(1);
        let mut recipient = Peer::connected(test_peer(14_300), tx, 1, 1, 1, true, false, None);
        recipient.their_ut_pex_id = Some(4);
        recipient.pex_sent.insert(gone_addr);
        recipient
            .cmd_tx
            .try_send(PeerCommand::Send(Message::KeepAlive))
            .unwrap();
        let (other_tx, _other_rx) = mpsc::channel(1);
        let neighbour = Peer::connected(neighbour_addr, other_tx, 1, 1, 1, true, false, None);
        let mut peers = HashMap::from([(1u32, recipient), (2u32, neighbour)]);

        send_pex_updates(&mut peers, 8);

        assert!(!peers[&1].pex_sent.contains(&neighbour_addr));
        assert!(peers[&1].pex_sent.contains(&gone_addr));
        assert!(matches!(
            rx.try_recv(),
            Ok(PeerCommand::Send(Message::KeepAlive))
        ));

        send_pex_updates(&mut peers, 8);

        assert_eq!(peers[&1].pex_sent, HashSet::from([neighbour_addr]));
        let Ok(PeerCommand::Send(Message::Extended { ext_id: 4, payload })) = rx.try_recv() else {
            panic!("expected a ut_pex message");
        };
        let update = crate::wire::extended::parse_ut_pex_full(&payload).unwrap();
        assert_eq!(
            update.added.iter().map(|(a, _)| *a).collect::<Vec<_>>(),
            [neighbour_addr]
        );
        assert_eq!(update.dropped, [gone_addr]);
    }

    #[test]
    fn pex_advertises_mapped_listen_addresses_as_plain_ipv4() {
        let mapped: SocketAddr = "[::ffff:198.51.100.44]:6881".parse().unwrap();
        let plain = SocketAddr::from(([198, 51, 100, 44], 6881));
        let (recipient_tx, mut rx) = mpsc::channel(4);
        let mut recipient =
            Peer::connected(test_peer(14_315), recipient_tx, 1, 1, 1, true, false, None);
        recipient.their_ut_pex_id = Some(4);
        let (tx, _rx) = mpsc::channel(1);
        let mut neighbour = Peer::connected(mapped, tx, 1, 1, 1, false, false, None);
        neighbour.listen_addr = Some(mapped);
        let mut peers = HashMap::from([(1u32, recipient), (2u32, neighbour)]);

        send_pex_updates(&mut peers, 8);

        let Ok(PeerCommand::Send(Message::Extended { payload, .. })) = rx.try_recv() else {
            panic!("expected a ut_pex message");
        };
        let update = crate::wire::extended::parse_ut_pex_full(&payload).unwrap();
        assert_eq!(
            update.added.iter().map(|(a, _)| *a).collect::<Vec<_>>(),
            [plain]
        );
        assert_eq!(peers[&1].pex_sent, HashSet::from([plain]));
    }

    #[test]
    fn pex_withholds_local_contacts_from_public_recipients() {
        let lan_addr = SocketAddr::from(([192, 168, 1, 5], 6881));
        let public_addr = SocketAddr::from(([198, 51, 100, 40], 6881));
        let (public_tx, mut public_rx) = mpsc::channel(4);
        let mut public = Peer::connected(test_peer(14_310), public_tx, 1, 1, 1, true, false, None);
        public.their_ut_pex_id = Some(4);
        let (lan_tx, mut lan_rx) = mpsc::channel(4);
        let mut lan = Peer::connected(
            SocketAddr::from(([192, 168, 1, 9], 6881)),
            lan_tx,
            1,
            1,
            1,
            true,
            false,
            None,
        );
        lan.their_ut_pex_id = Some(4);
        let (a_tx, _a_rx) = mpsc::channel(1);
        let lan_peer = Peer::connected(lan_addr, a_tx, 1, 1, 1, true, false, None);
        let (b_tx, _b_rx) = mpsc::channel(1);
        let public_peer = Peer::connected(public_addr, b_tx, 1, 1, 1, true, false, None);
        let mut peers = HashMap::from([(1u32, public), (2, lan), (3, lan_peer), (4, public_peer)]);

        send_pex_updates(&mut peers, 8);

        let contacts = |rx: &mut mpsc::Receiver<PeerCommand>| {
            let Ok(PeerCommand::Send(Message::Extended { payload, .. })) = rx.try_recv() else {
                panic!("expected a ut_pex message");
            };
            let mut added = crate::wire::extended::parse_ut_pex_full(&payload)
                .unwrap()
                .added
                .into_iter()
                .map(|(a, _)| a)
                .collect::<Vec<_>>();
            added.sort();
            added
        };
        assert!(contacts(&mut public_rx).iter().all(|a| *a != lan_addr));
        assert!(peers[&1].pex_sent.contains(&public_addr));
        assert!(!peers[&1].pex_sent.contains(&lan_addr));
        let seen_by_lan = contacts(&mut lan_rx);
        assert!(seen_by_lan.contains(&lan_addr) && seen_by_lan.contains(&public_addr));
    }

    #[test]
    fn holepunch_rendezvous_naming_the_relay_returns_no_self() {
        let mut fx = HolepunchFixture::new();
        let initiator = SocketAddr::from(([198, 51, 100, 7], 14_251));
        let relay_public = IpAddr::from([203, 0, 113, 9]);
        let (cmd, mut rx) = mpsc::channel(4);
        let mut budget = holepunch_budget(fx.now);
        let mut sender = hp_sender(2, initiator, &cmd);
        sender.their_view_of_us = Some(relay_public);

        fx.handle(
            rendezvous_msg(SocketAddr::new(relay_public, 6881)),
            &sender,
            &mut budget,
        );
        let (ext, msg) = sent_holepunch(rx.try_recv().unwrap());
        assert_eq!(
            (ext, msg.msg_type, msg.err_code),
            (7, holepunch_type::ERROR, holepunch_err::NO_SELF)
        );

        sender.their_view_of_us = Some("::ffff:203.0.113.9".parse().unwrap());
        fx.handle(
            rendezvous_msg(SocketAddr::new(relay_public, 6881)),
            &sender,
            &mut budget,
        );
        assert_eq!(
            sent_holepunch(rx.try_recv().unwrap()).1.err_code,
            holepunch_err::NO_SELF
        );

        fx.handle(
            rendezvous_msg(SocketAddr::new(relay_public, 7777)),
            &sender,
            &mut budget,
        );
        assert_eq!(
            sent_holepunch(rx.try_recv().unwrap()).1.err_code,
            holepunch_err::NOT_CONNECTED
        );
    }

    #[test]
    fn holepunch_rendezvous_naming_our_elected_external_endpoint_returns_no_self() {
        let mut fx = HolepunchFixture::new();
        let initiator = SocketAddr::from(([198, 51, 100, 7], 14_252));
        let (cmd, mut rx) = mpsc::channel(4);
        let mut budget = holepunch_budget(fx.now);
        let external = ExternalIp::new();
        for octet in 1..=3u8 {
            external.vote(
                IpAddr::from([93, 184, 216, 34]),
                Source::Peer,
                IpAddr::from([23, octet, 0, 1]),
                Route::Direct,
            );
        }
        let mut sender = hp_sender(2, initiator, &cmd);
        sender.elected = external.addresses();
        assert_eq!(sender.their_view_of_us, None);

        let own = SocketAddr::from(([93, 184, 216, 34], 6881));
        fx.handle(rendezvous_msg(own), &sender, &mut budget);
        let (ext, msg) = sent_holepunch(rx.try_recv().unwrap());
        assert_eq!(
            (ext, msg.msg_type, msg.err_code),
            (7, holepunch_type::ERROR, holepunch_err::NO_SELF)
        );

        fx.handle(
            rendezvous_msg(SocketAddr::from(([93, 184, 216, 34], 7777))),
            &sender,
            &mut budget,
        );
        assert_eq!(
            sent_holepunch(rx.try_recv().unwrap()).1.err_code,
            holepunch_err::NOT_CONNECTED
        );
        fx.handle(
            rendezvous_msg(SocketAddr::from(([93, 184, 216, 35], 6881))),
            &sender,
            &mut budget,
        );
        assert_eq!(
            sent_holepunch(rx.try_recv().unwrap()).1.err_code,
            holepunch_err::NOT_CONNECTED
        );

        sender.elected = ExternalAddresses::default();
        fx.handle(rendezvous_msg(own), &sender, &mut budget);
        assert_eq!(
            sent_holepunch(rx.try_recv().unwrap()).1.err_code,
            holepunch_err::NOT_CONNECTED
        );
    }

    #[test]
    fn holepunch_rendezvous_naming_the_initiator_returns_no_self() {
        let mut fx = HolepunchFixture::new();
        let initiator = SocketAddr::from(([198, 51, 100, 7], 14_261));
        let (cmd, mut rx) = mpsc::channel(4);
        let mut budget = holepunch_budget(fx.now);

        fx.handle(
            rendezvous_msg(initiator),
            &hp_sender(2, initiator, &cmd),
            &mut budget,
        );

        let (ext, msg) = sent_holepunch(rx.try_recv().unwrap());
        assert_eq!(
            (ext, msg.msg_type, msg.err_code),
            (7, holepunch_type::ERROR, holepunch_err::NO_SELF)
        );
    }

    #[test]
    fn holepunch_rendezvous_does_not_relay_a_peer_to_itself_by_listen_addr() {
        let mut fx = HolepunchFixture::new();
        let observed = SocketAddr::from(([198, 51, 100, 7], 50_000));
        let listen = SocketAddr::from(([198, 51, 100, 7], 6881));
        let (cmd, mut rx) = mpsc::channel(4);
        let mut initiator = Peer::connected(observed, cmd.clone(), 1, 1, 1, false, false, None);
        initiator.listen_addr = Some(listen);
        initiator.their_ut_holepunch_id = Some(7);
        fx.peers.insert(2, initiator);
        let mut budget = holepunch_budget(fx.now);

        fx.handle(
            rendezvous_msg(listen),
            &hp_sender(2, observed, &cmd),
            &mut budget,
        );

        let (_, msg) = sent_holepunch(rx.try_recv().unwrap());
        assert_eq!(
            (msg.msg_type, msg.err_code),
            (holepunch_type::ERROR, holepunch_err::NO_SELF)
        );
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn holepunch_introduces_utp_initiators_by_their_observed_endpoint() {
        let mut fx = HolepunchFixture::new();
        let observed = SocketAddr::from(([198, 51, 100, 7], 50_000));
        let listen = SocketAddr::from(([198, 51, 100, 7], 6881));
        let target_addr = SocketAddr::from(([198, 51, 100, 8], 6881));
        let (cmd, _rx) = mpsc::channel(4);
        let mut initiator = Peer::connected(observed, cmd.clone(), 1, 1, 1, false, false, None);
        initiator.listen_addr = Some(listen);
        initiator.their_ut_holepunch_id = Some(7);
        assert_eq!(holepunch_endpoint(&initiator), listen);
        initiator.link.utp = true;
        assert_eq!(holepunch_endpoint(&initiator), observed);

        let (target_tx, mut target_rx) = mpsc::channel(4);
        let mut target = Peer::connected(target_addr, target_tx, 1, 1, 1, true, false, None);
        target.their_ut_holepunch_id = Some(9);
        fx.peers.insert(1, target);
        let mut budget = holepunch_budget(fx.now);

        fx.handle(
            rendezvous_msg(target_addr),
            &hp_sender(2, holepunch_endpoint(&initiator), &cmd),
            &mut budget,
        );

        let (ext, msg) = sent_holepunch(target_rx.try_recv().unwrap());
        assert_eq!(
            (ext, msg.msg_type, msg.addr),
            (9, holepunch_type::CONNECT, observed)
        );
    }

    fn relayed_target(
        fx: &mut HolepunchFixture,
        utp: bool,
    ) -> (SocketAddr, SocketAddr, mpsc::Receiver<PeerCommand>) {
        let observed = SocketAddr::from(([198, 51, 100, 8], 50_000));
        let listen = SocketAddr::from(([198, 51, 100, 8], 6881));
        let (target_tx, target_rx) = mpsc::channel(4);
        let mut target = Peer::connected(observed, target_tx, 1, 1, 1, false, false, None);
        target.listen_addr = Some(listen);
        target.link.utp = utp;
        target.their_ut_holepunch_id = Some(9);
        fx.peers.insert(1, target);
        (observed, listen, target_rx)
    }

    #[test]
    fn holepunch_hands_the_initiator_a_utp_targets_observed_endpoint() {
        let mut fx = HolepunchFixture::new();
        let (observed, listen, mut target_rx) = relayed_target(&mut fx, true);
        let initiator_addr = SocketAddr::from(([198, 51, 100, 7], 6881));
        let (cmd, mut initiator_rx) = mpsc::channel(4);
        let mut budget = holepunch_budget(fx.now);

        fx.handle(
            rendezvous_msg(listen),
            &hp_sender(2, initiator_addr, &cmd),
            &mut budget,
        );

        let (ext, msg) = sent_holepunch(initiator_rx.try_recv().unwrap());
        assert_eq!(
            (ext, msg.msg_type, msg.addr),
            (7, holepunch_type::CONNECT, observed)
        );
        let (ext, msg) = sent_holepunch(target_rx.try_recv().unwrap());
        assert_eq!(
            (ext, msg.msg_type, msg.addr),
            (9, holepunch_type::CONNECT, initiator_addr)
        );
    }

    #[test]
    fn holepunch_hands_the_initiator_a_tcp_targets_listen_endpoint() {
        let mut fx = HolepunchFixture::new();
        let (observed, listen, _target_rx) = relayed_target(&mut fx, false);
        let (cmd, mut initiator_rx) = mpsc::channel(4);
        let mut budget = holepunch_budget(fx.now);

        for requested in [listen, observed] {
            fx.handle(
                rendezvous_msg(requested),
                &hp_sender(2, SocketAddr::from(([198, 51, 100, 7], 6881)), &cmd),
                &mut budget,
            );
            let (_, msg) = sent_holepunch(initiator_rx.try_recv().unwrap());
            assert_eq!((msg.msg_type, msg.addr), (holepunch_type::CONNECT, listen));
        }
    }

    #[test]
    fn a_relayed_connect_answers_the_rendezvous_even_with_the_observed_port() {
        let t0 = Instant::now();
        let requested = SocketAddr::from(([198, 51, 100, 8], 6881));
        let observed = SocketAddr::from(([198, 51, 100, 8], 50_000));
        let elsewhere = SocketAddr::from(([198, 51, 100, 9], 50_000));
        let mut state = HolepunchState::new(true, 6881, None);
        state.note_initiated(requested, 1, t0);

        assert!(!state.take_answer(&observed, 2, t0), "another relay");
        assert!(!state.take_answer(&elsewhere, 1, t0), "another host");
        assert!(state.take_answer(&observed, 1, t0));
        assert!(!state.take_answer(&observed, 1, t0), "one-shot");
        assert!(!state.take_answer(&requested, 1, t0), "one-shot");
        assert!(!state.can_initiate(&requested, t0 + HOLEPUNCH_UNANSWERED_TTL));

        let mut stale = HolepunchState::new(true, 6881, None);
        stale.note_initiated(requested, 1, t0);
        assert!(!stale.take_answer(&observed, 1, t0 + HOLEPUNCH_TTL));
    }

    #[test]
    fn an_inbound_peer_adopted_from_a_queued_punch_target_leaves_every_queue() {
        let mut fx = HolepunchFixture::new();
        let target = SocketAddr::from(([198, 51, 100, 8], 50_000));
        let (cmd, _rx) = mpsc::channel(4);
        let mut budget = holepunch_budget(fx.now);
        fx.handle(
            connect_msg(target),
            &hp_sender(1, SocketAddr::from(([198, 51, 100, 7], 6881)), &cmd),
            &mut budget,
        );
        assert_eq!(fx.priority_backlog, VecDeque::from([target]));
        assert!(fx.holepunch.is_target(&target, fx.now));
        assert!(!inbound_addr_taken(
            target,
            &fx.known_addrs,
            &fx.peers,
            &fx.pending_dials,
            &fx.holepunch,
            fx.now
        ));

        let (peer_tx, _peer_rx) = mpsc::channel(4);
        fx.peers.insert(
            5,
            Peer::connected(target, peer_tx, 1, 1, 1, false, false, None),
        );
        fx.dial_retries.push_back((target, fx.now));
        fx.useful_redials.push_back((target, fx.now));
        fx.peer_backlog.push_back(target);
        fx.known_addrs.insert(target);
        unqueue_peer_addr(
            target,
            &mut fx.priority_backlog,
            &mut fx.peer_backlog,
            &mut fx.dial_retries,
            &mut fx.useful_redials,
        );

        assert_eq!(fx.queued(), 0);
        assert!(fx.known_addrs.contains(&target));
        assert!(inbound_addr_taken(
            target,
            &fx.known_addrs,
            &fx.peers,
            &fx.pending_dials,
            &fx.holepunch,
            fx.now
        ));
    }

    #[test]
    fn extended_handshake_omits_ut_holepunch_without_holepunch_support() {
        let build = |holepunch| {
            let ext = OurExtHandshake {
                holepunch,
                listen_port: 6881,
                metadata_size: 100,
                private: false,
                prefers_encryption: true,
            };
            ExtHandshake::decode(&ext.build(IpAddr::from([192, 0, 2, 1]), None).encode()).unwrap()
        };
        assert_eq!(build(true).ut_holepunch_id(), Some(OUR_UT_HOLEPUNCH_ID));
        assert_eq!(build(false).ut_holepunch_id(), None);
        assert!(build(false).ut_pex_id().is_some());
    }

    #[tokio::test]
    async fn holepunch_needs_a_utp_socket_on_the_listen_port() {
        tokio::time::timeout(Duration::from_secs(10), async {
            let socket = UtpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .unwrap();
            let port = socket.local_addr().port();
            assert!(holepunch_supported(false, Some(&socket), port));
            assert!(!holepunch_supported(
                false,
                Some(&socket),
                port.wrapping_add(1)
            ));
            assert!(!holepunch_supported(false, None, port));
            assert!(!holepunch_supported(true, Some(&socket), port));
        })
        .await
        .expect("uTP bind timed out");
    }

    #[tokio::test]
    async fn holepunch_for_v4_peers_does_not_depend_on_the_v6_socket_port() {
        if !crate::test_util::ipv6_loopback_available() {
            eprintln!("skipping: no IPv6 loopback on this host");
            return;
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            let v4 = UtpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
                .await
                .unwrap();
            let v6 = UtpSocket::bind_v6_only_with_proxy(0, None).await.unwrap();
            let v6_port = v6.local_addr().port();
            v4.attach_v6(v6);
            let v4_port = v4.local_addr().port();
            assert_eq!(v4.v6_local_addr().map(|addr| addr.port()), Some(v6_port));
            assert!(holepunch_supported(false, Some(&v4), v4_port));
            if v6_port != v4_port {
                assert!(
                    !holepunch_supported(false, Some(&v4), v6_port),
                    "the v4 socket must sit on the advertised port"
                );
            }
        })
        .await
        .expect("uTP bind timed out");
    }

    #[test]
    fn holepunch_only_targets_v6_peers_when_a_v6_utp_socket_exists() {
        let v6_target = SocketAddr::from((
            std::net::Ipv6Addr::from([0x2001, 0xdb8, 0, 0, 0, 0, 0, 5]),
            6881,
        ));
        let v4_target = SocketAddr::from(([198, 51, 100, 50], 6881));
        let mapped_target = SocketAddr::from((
            std::net::Ipv6Addr::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 198, 51, 100, 51]),
            6881,
        ));
        let now = Instant::now();
        let without = HolepunchState::new(true, 6881, Some(6881));
        assert!(without.can_initiate(&v4_target, now));
        assert!(without.can_initiate(&mapped_target, now));
        assert!(!without.can_initiate(&v6_target, now));

        let with = HolepunchState::new(true, 6881, Some(6881)).with_v6_port(Some(6881));
        assert!(with.can_initiate(&v6_target, now));
        assert!(with.can_initiate(&v4_target, now));
    }

    #[test]
    fn a_v6_socket_off_the_listen_port_keeps_v4_holepunch_but_skips_v6_targets() {
        let v6_target = SocketAddr::from((
            std::net::Ipv6Addr::from([0x2001, 0xdb8, 0, 0, 0, 0, 0, 5]),
            6881,
        ));
        let v4_target = SocketAddr::from(([198, 51, 100, 50], 6881));
        let now = Instant::now();
        let off_port = HolepunchState::new(true, 6881, Some(6881)).with_v6_port(Some(51_413));
        assert!(off_port.enabled);
        assert!(off_port.can_initiate(&v4_target, now));
        assert!(!off_port.can_initiate(&v6_target, now));
    }

    #[test]
    fn holepunch_recognises_our_own_v6_utp_port_as_ours() {
        let ours = IpAddr::from([0x2001, 0xdb8, 0, 0, 0, 0, 0, 9]);
        let elected = ExternalAddresses::default();
        let on = |state: &HolepunchState, port: u16| {
            state.is_own_endpoint(SocketAddr::new(ours, port), Some(ours), &elected)
        };
        let plain = HolepunchState::new(true, 6881, Some(6881));
        assert!(on(&plain, 6881));
        assert!(!on(&plain, 51_413));
        let with_v6 = HolepunchState::new(true, 6881, Some(6881)).with_v6_port(Some(51_413));
        assert!(on(&with_v6, 51_413));
        assert!(!on(&with_v6, 40_000));
    }

    #[test]
    fn disabled_holepunch_never_initiates() {
        let target = SocketAddr::from(([198, 51, 100, 50], 6881));
        let (relay_tx, mut relay_rx) = mpsc::channel(4);
        let mut relay = Peer::connected(test_peer(14_320), relay_tx, 1, 1, 1, true, false, None);
        relay.their_ut_holepunch_id = Some(9);
        let peers = HashMap::from([(1u32, relay)]);
        let pex_source = HashMap::from([(target, 1u32)]);
        let mut holepunch = HolepunchState::new(false, 6881, None);

        try_initiate_holepunch(target, &pex_source, &mut holepunch, &peers, Instant::now());

        assert!(relay_rx.try_recv().is_err());
        assert!(holepunch.initiated.is_empty());
    }

    #[test]
    fn an_unanswered_rendezvous_is_retried_after_the_short_expiry() {
        let target = SocketAddr::from(([198, 51, 100, 51], 6881));
        let (relay_tx, mut relay_rx) = mpsc::channel(8);
        let mut relay = Peer::connected(test_peer(14_330), relay_tx, 1, 1, 1, true, false, None);
        relay.their_ut_holepunch_id = Some(9);
        let peers = HashMap::from([(1u32, relay)]);
        let pex_source = HashMap::from([(target, 1u32)]);
        let mut holepunch = HolepunchState::new(true, 6881, None);
        let t0 = Instant::now();

        try_initiate_holepunch(target, &pex_source, &mut holepunch, &peers, t0);
        let (ext, msg) = sent_holepunch(relay_rx.try_recv().unwrap());
        assert_eq!(
            (ext, msg.msg_type, msg.addr),
            (9, holepunch_type::RENDEZVOUS, target)
        );

        try_initiate_holepunch(
            target,
            &pex_source,
            &mut holepunch,
            &peers,
            t0 + Duration::from_secs(1),
        );
        assert!(relay_rx.try_recv().is_err());

        try_initiate_holepunch(
            target,
            &pex_source,
            &mut holepunch,
            &peers,
            t0 + HOLEPUNCH_UNANSWERED_TTL,
        );
        assert!(relay_rx.try_recv().is_ok());

        holepunch.forget(&target);
        try_initiate_holepunch(
            target,
            &pex_source,
            &mut holepunch,
            &peers,
            t0 + HOLEPUNCH_UNANSWERED_TTL,
        );
        assert!(relay_rx.try_recv().is_ok());
    }

    #[test]
    fn a_failed_rendezvous_is_retried_at_every_scheduled_dial_retry() {
        let target = SocketAddr::from(([198, 51, 100, 52], 6881));
        let (relay_tx, mut relay_rx) = mpsc::channel(8);
        let mut relay = Peer::connected(test_peer(14_340), relay_tx, 1, 1, 1, true, false, None);
        relay.their_ut_holepunch_id = Some(9);
        let peers = HashMap::from([(1u32, relay)]);
        let pex_source = HashMap::from([(target, 1u32)]);
        let mut holepunch = HolepunchState::new(true, 6881, None);
        let mut now = Instant::now();

        try_initiate_holepunch(target, &pex_source, &mut holepunch, &peers, now);
        assert!(relay_rx.try_recv().is_ok());

        for failures in 1..MAX_DIAL_FAILURES {
            let mut dial_retries = VecDeque::new();
            let mut useful_redials = VecDeque::new();
            let before = Instant::now();
            schedule_peer_retry(
                target,
                false,
                failures,
                &mut dial_retries,
                &mut useful_redials,
            );
            let (_, due) = dial_retries.pop_front().expect("retry is scheduled");
            now += due.saturating_duration_since(before);

            try_initiate_holepunch(target, &pex_source, &mut holepunch, &peers, now);
            let (_, msg) = sent_holepunch(
                relay_rx
                    .try_recv()
                    .unwrap_or_else(|_| panic!("no RENDEZVOUS after failure {failures}")),
            );
            assert_eq!(msg.msg_type, holepunch_type::RENDEZVOUS);
        }
    }

    #[test]
    fn an_answered_rendezvous_is_not_retried_within_the_full_ttl() {
        let t0 = Instant::now();
        let target = SocketAddr::from(([198, 51, 100, 53], 6881));
        let mut state = HolepunchState::new(true, 6881, None);
        state.note_initiated(target, 1, t0);
        assert!(state.take_answer(&target, 1, t0));

        assert!(!state.can_initiate(&target, t0 + HOLEPUNCH_UNANSWERED_TTL));
        assert!(!state.can_initiate(&target, t0 + DIAL_RETRY_DELAY * 9));
        assert!(state.can_initiate(&target, t0 + HOLEPUNCH_TTL));
    }

    #[test]
    fn holepunch_tables_expire_and_never_exceed_their_cap() {
        let t0 = Instant::now();
        let mut state = HolepunchState::new(true, 6881, None);
        let addr = |n: usize| {
            SocketAddr::from((
                [198, 51, 100, (n / 60_000) as u8 + 1],
                (n % 60_000) as u16 + 1,
            ))
        };
        for n in 0..MAX_HOLEPUNCH_ENTRIES + 10 {
            let at = t0 + Duration::from_millis(n as u64);
            state.note_initiated(addr(n), 1, at);
            state.note_target(addr(n), at);
        }
        assert_eq!(state.initiated.len(), MAX_HOLEPUNCH_ENTRIES);
        assert_eq!(state.targets.len(), MAX_HOLEPUNCH_ENTRIES);
        assert!(!state.initiated.contains_key(&addr(0)));
        assert!(!state.targets.contains_key(&addr(9)));
        assert!(state.targets.contains_key(&addr(10)));

        let last = addr(MAX_HOLEPUNCH_ENTRIES + 9);
        assert!(state.is_target(&last, t0 + HOLEPUNCH_TTL));
        assert!(!state.is_target(&last, t0 + HOLEPUNCH_TTL + Duration::from_secs(10)));
        assert!(state.can_initiate(&last, t0 + HOLEPUNCH_TTL + Duration::from_secs(10)));

        state.prune(t0 + HOLEPUNCH_TTL + Duration::from_secs(10));
        assert!(state.initiated.is_empty() && state.targets.is_empty());
    }

    #[test]
    fn inbound_connections_from_a_dialed_or_punched_endpoint_are_adopted() {
        let now = Instant::now();
        let addr = SocketAddr::from(([198, 51, 100, 60], 6881));
        let mut known = HashSet::from([addr]);
        let mut peers = HashMap::new();
        let mut pending = HashMap::new();
        let mut holepunch = HolepunchState::new(true, 6881, None);

        assert!(!inbound_addr_taken(
            SocketAddr::from(([198, 51, 100, 61], 6881)),
            &known,
            &peers,
            &pending,
            &holepunch,
            now
        ));
        assert!(inbound_addr_taken(
            addr, &known, &peers, &pending, &holepunch, now
        ));

        pending.insert(4u32, addr);
        assert!(!inbound_addr_taken(
            addr, &known, &peers, &pending, &holepunch, now
        ));

        pending.clear();
        holepunch.note_target(addr, now);
        assert!(!inbound_addr_taken(
            addr, &known, &peers, &pending, &holepunch, now
        ));
        assert!(inbound_addr_taken(
            addr,
            &known,
            &peers,
            &pending,
            &holepunch,
            now + HOLEPUNCH_TTL
        ));

        let (tx, _rx) = mpsc::channel(1);
        peers.insert(1u32, Peer::connected(addr, tx, 1, 1, 1, false, false, None));
        pending.insert(4u32, addr);
        assert!(inbound_addr_taken(
            addr, &known, &peers, &pending, &holepunch, now
        ));

        known.clear();
        assert!(!inbound_addr_taken(
            addr, &known, &peers, &pending, &holepunch, now
        ));
    }

    #[test]
    fn pex_candidates_leave_backlog_room_for_other_sources() {
        let mut priority_backlog = VecDeque::new();
        let mut peer_backlog = VecDeque::new();
        let mut dial_retries = VecDeque::new();
        let mut useful_redials = VecDeque::new();
        let mut known_addrs = HashSet::new();
        let blocklist = RwLock::new(BlockList::default());

        for n in 0..MAX_PEER_BACKLOG as u32 + 100 {
            let addr = SocketAddr::from(([198, 51, 100, (n % 250) as u8 + 1], 10_000 + n as u16));
            queue_sourced_candidate(
                addr,
                PeerSource::Pex,
                false,
                &mut priority_backlog,
                &mut peer_backlog,
                &mut dial_retries,
                &mut useful_redials,
                &mut known_addrs,
                &blocklist,
            );
        }
        assert_eq!(peer_backlog.len(), MAX_PEX_BACKLOG);

        let tracker = SocketAddr::from(([192, 0, 2, 77], 6881));
        let mut queue = |addr, source, known_useful| {
            queue_sourced_candidate(
                addr,
                source,
                known_useful,
                &mut priority_backlog,
                &mut peer_backlog,
                &mut dial_retries,
                &mut useful_redials,
                &mut known_addrs,
                &blocklist,
            )
        };
        let late_pex = SocketAddr::from(([192, 0, 2, 78], 6881));
        assert!(!queue(late_pex, PeerSource::Pex, false));
        assert!(queue(tracker, PeerSource::Tracker, false));
        let useful_pex = SocketAddr::from(([192, 0, 2, 79], 6881));
        assert!(queue(useful_pex, PeerSource::Pex, true));
        assert_eq!(priority_backlog.back(), Some(&useful_pex));
        assert_eq!(peer_backlog.back(), Some(&tracker));
    }

    fn pex_message(addrs: &[SocketAddr], dropped: &[SocketAddr]) -> Bytes {
        let added: Vec<(SocketAddr, u8)> = addrs.iter().map(|addr| (*addr, 0)).collect();
        super::super::wire::extended::build_ut_pex(&added, dropped)
    }

    struct PexIntake {
        known_addrs: HashSet<SocketAddr>,
        budgets: IpBudgets,
        sources: HashMap<SocketAddr, u32>,
        now: Instant,
    }

    impl PexIntake {
        fn new() -> Self {
            Self {
                known_addrs: HashSet::new(),
                budgets: IpBudgets::default(),
                sources: HashMap::new(),
                now: Instant::now(),
            }
        }

        fn ingest(&mut self, pid: u32, payload: &[u8]) -> Vec<SocketAddr> {
            self.ingest_ranked(pid, payload, &PeerRanker::default())
        }

        fn ingest_ranked(
            &mut self,
            pid: u32,
            payload: &[u8],
            ranker: &PeerRanker,
        ) -> Vec<SocketAddr> {
            let source = IpAddr::from([198, 51, 100, 7]);
            self.now += PEX_MIN_INBOUND_GAP;
            ingest_pex(
                pid,
                source,
                payload,
                false,
                &self.known_addrs,
                &mut self.budgets,
                &mut self.sources,
                self.now,
                ranker,
            )
        }
    }

    #[test]
    fn pex_intake_remembers_the_relay_and_spaces_messages_apart() {
        let mut intake = PexIntake::new();
        let a = SocketAddr::from(([198, 51, 100, 21], 6881));
        let b = SocketAddr::from(([198, 51, 100, 22], 6881));
        assert_eq!(intake.ingest(3, &pex_message(&[a, b], &[])), vec![a, b]);
        assert_eq!(intake.sources.get(&a), Some(&3));
        assert_eq!(intake.sources.get(&b), Some(&3));

        let c = SocketAddr::from(([198, 51, 100, 23], 6881));
        let payload = pex_message(&[c], &[]);
        let too_soon = ingest_pex(
            3,
            IpAddr::from([198, 51, 100, 7]),
            &payload,
            false,
            &intake.known_addrs,
            &mut intake.budgets,
            &mut intake.sources,
            intake.now + Duration::from_secs(1),
            &PeerRanker::default(),
        );
        assert!(too_soon.is_empty());
        assert!(!intake.sources.contains_key(&c));
    }

    #[test]
    fn pex_contacts_are_queued_highest_priority_first_and_every_relay_is_remembered() {
        let pool = priority_pool(12);
        let mut wire = pool.clone();
        wire.reverse();
        let mut intake = PexIntake::new();
        let ordered = intake.ingest_ranked(5, &pex_message(&wire, &[]), &elected_ranker());
        assert_eq!(ordered, pool);
        assert!(pool.iter().all(|addr| intake.sources.get(addr) == Some(&5)));

        let mut unranked = PexIntake::new();
        assert_eq!(unranked.ingest(5, &pex_message(&wire, &[])), wire);
    }

    #[test]
    fn pex_intake_spends_the_source_addresses_shared_budget() {
        let mut intake = PexIntake::new();
        let mut accepted = 0;
        let mut last_batch = 0;
        for message in 0..5u8 {
            let addrs: Vec<SocketAddr> = (1..=MAX_PEX_ADDED_PER_MSG as u8)
                .map(|n| SocketAddr::from(([198, 51, 100 + message, n], 6881)))
                .collect();
            last_batch = intake.ingest(1, &pex_message(&addrs, &[])).len();
            accepted += last_batch;
        }
        assert!(last_batch < MAX_PEX_ADDED_PER_MSG, "{last_batch}");
        assert!(accepted > PEX_ACCEPT_BURST as usize, "{accepted}");
        assert!(accepted < 5 * MAX_PEX_ADDED_PER_MSG, "{accepted}");
    }

    #[test]
    fn a_dropped_contact_only_clears_the_relay_that_announced_it() {
        let mut intake = PexIntake::new();
        let plain = SocketAddr::from(([198, 51, 100, 31], 6881));
        let mapped = SocketAddr::from((
            std::net::Ipv6Addr::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 198, 51, 100, 32]),
            6881,
        ));
        let canonical = canonical_addr(mapped);
        assert_ne!(mapped, canonical);
        intake.ingest(3, &pex_message(&[plain, mapped], &[]));
        assert_eq!(intake.sources.get(&canonical), Some(&3));

        intake.ingest(4, &pex_message(&[], &[plain, mapped]));
        assert_eq!(intake.sources.get(&plain), Some(&3));
        assert_eq!(intake.sources.get(&canonical), Some(&3));

        intake.ingest(3, &pex_message(&[], &[plain, mapped]));
        assert!(intake.sources.is_empty());
    }

    fn holepunch_relay(fx: &mut HolepunchFixture, addr: SocketAddr) {
        let (cmd, rx) = mpsc::channel(4);
        std::mem::forget(rx);
        let mut relay = Peer::connected(addr, cmd, 1, 1, 1, false, false, None);
        relay.their_ut_holepunch_id = Some(7);
        fx.peers.insert(2, relay);
    }

    fn dispatch_connect(fx: &mut HolepunchFixture, budgets: &mut IpBudgets, target: SocketAddr) {
        let payload = build_holepunch(holepunch_type::CONNECT, target, 0);
        dispatch_holepunch(
            2,
            &payload,
            &fx.peers,
            &fx.pending_dials,
            &mut fx.priority_backlog,
            &mut fx.peer_backlog,
            &mut fx.dial_retries,
            &mut fx.useful_redials,
            &mut fx.known_addrs,
            &mut fx.holepunch,
            budgets,
            fx.now,
            &fx.blocklist,
        );
    }

    #[test]
    fn the_holepunch_dispatcher_spends_the_relay_addresses_connect_budget() {
        let mut fx = HolepunchFixture::new();
        holepunch_relay(&mut fx, SocketAddr::from(([198, 51, 100, 7], 6881)));
        let mut budgets = IpBudgets::default();
        for n in 0..HOLEPUNCH_CONNECT_BURST as u8 + 2 {
            dispatch_connect(
                &mut fx,
                &mut budgets,
                SocketAddr::from(([198, 51, 100, 50 + n], 6881)),
            );
        }
        assert_eq!(fx.priority_backlog.len(), HOLEPUNCH_CONNECT_BURST as usize);
        let relay_ip = IpAddr::from([198, 51, 100, 7]);
        assert!(!budgets.entry(relay_ip, fx.now).connect.is_full(fx.now));
        assert_eq!(budgets.entry(relay_ip, fx.now).connect.take(1, fx.now), 0);
    }

    #[test]
    fn a_disabled_holepunch_dispatcher_ignores_connects() {
        let mut fx = HolepunchFixture::new();
        fx.holepunch = HolepunchState::new(false, 6881, Some(6881));
        holepunch_relay(&mut fx, SocketAddr::from(([198, 51, 100, 7], 6881)));
        let mut budgets = IpBudgets::default();
        dispatch_connect(
            &mut fx,
            &mut budgets,
            SocketAddr::from(([198, 51, 100, 50], 6881)),
        );
        assert_eq!(fx.queued(), 0);
        assert!(budgets.entries.is_empty());
    }

    #[test]
    fn the_extended_handshake_records_how_the_peer_sees_us() {
        let (tx, _rx) = mpsc::channel(1);
        let mut peer = Peer::connected(test_peer(52_401), tx, 1, 1, 1, true, true, None);
        let seen = IpAddr::from([203, 0, 113, 9]);
        let with_ip = ExtHandshake::decode(
            &ExtHandshake::new_outgoing(2, 3, None)
                .with_yourip(seen)
                .encode(),
        )
        .unwrap();
        merge_ext_handshake(&mut peer, &with_ip);
        assert_eq!(peer.their_yourip, Some(seen));
        merge_ext_handshake(&mut peer, &ext_handshake_from(vec![], vec![]));
        assert_eq!(peer.their_yourip, Some(seen));
    }

    #[test]
    fn a_peers_yourip_casts_one_peer_vote_from_its_network() {
        let external = ExternalIp::new();
        let seen = IpAddr::from([93, 184, 216, 34]);
        let handshake = |yourip: Option<IpAddr>| {
            let mut hs = ExtHandshake::new_outgoing(2, 3, None);
            if let Some(yourip) = yourip {
                hs = hs.with_yourip(yourip);
            }
            ExtHandshake::decode(&hs.encode()).unwrap()
        };
        let voters = [[23, 1, 0, 9], [23, 2, 0, 9], [23, 3, 0, 9]].map(IpAddr::from);

        vote_yourip(&external, voters[0], &handshake(Some(seen)), Route::Direct);
        vote_yourip(&external, voters[0], &handshake(Some(seen)), Route::Direct);
        vote_yourip(&external, voters[1], &handshake(None), Route::Direct);
        vote_yourip(
            &external,
            voters[1],
            &handshake(Some(IpAddr::from([192, 168, 1, 5]))),
            Route::Direct,
        );
        vote_yourip(&external, voters[1], &handshake(Some(seen)), Route::Direct);
        assert_eq!(external.external_v4(), None);

        vote_yourip(&external, voters[2], &handshake(Some(seen)), Route::Direct);
        assert_eq!(
            external.external_v4(),
            Some("93.184.216.34".parse().unwrap())
        );
    }

    #[test]
    fn a_peers_yourip_over_a_proxied_route_never_elects() {
        use risuko_http::{Proxy, ProxyConnector};

        let external = ExternalIp::new();
        let exit = IpAddr::from([93, 184, 216, 34]);
        let hs = ExtHandshake::decode(
            &ExtHandshake::new_outgoing(2, 3, None)
                .with_yourip(exit)
                .encode(),
        )
        .unwrap();
        let proxy = ProxyConnector::from_proxy(Proxy::all("socks5://127.0.0.1:1080").unwrap());
        for octet in 1..=10u8 {
            vote_yourip(
                &external,
                IpAddr::from([23, octet, 0, 9]),
                &hs,
                Route::of(Some(&proxy)),
            );
        }
        assert_eq!(external.addresses(), Default::default());
        for octet in 1..=3u8 {
            vote_yourip(
                &external,
                IpAddr::from([23, octet, 0, 9]),
                &hs,
                Route::of(Some(&ProxyConnector::direct())),
            );
        }
        assert_eq!(
            external.external_v4(),
            Some("93.184.216.34".parse().unwrap())
        );
    }

    #[test]
    fn peer_registry_carries_the_route_the_connection_was_dialed_on() {
        let (torrent_id, pid) = (7_002, 0);
        let addr: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        let scope = Arc::new(());
        let (tx, _rx) = mpsc::channel(1);
        peer_registry::put_with_lane(
            torrent_id,
            pid,
            &scope,
            peer_registry::PeerLane {
                tx,
                piece_tx: None,
                addr,
                io_abort: None,
                route: Route::Proxied,
            },
        );
        let lane = peer_registry::take_with_lane(torrent_id, pid, &scope).unwrap();
        assert_eq!(lane.route, Route::Proxied);

        let (tx, _rx) = mpsc::channel(1);
        peer_registry::put(torrent_id, pid, &scope, tx, addr, None);
        let lane = peer_registry::take_with_lane(torrent_id, pid, &scope).unwrap();
        assert_eq!(lane.route, Route::Direct);
    }

    #[tokio::test]
    async fn adopted_peers_keep_their_connection_route_for_yourip_votes() {
        let (lengths, _) = two_file_layout();
        let mut piece_tracker = PieceTracker::new(lengths);
        let mut peers = HashMap::new();
        let external = ExternalIp::new();
        let seen = IpAddr::from([93, 184, 216, 34]);
        let hs = ExtHandshake::decode(
            &ExtHandshake::new_outgoing(2, 3, None)
                .with_yourip(seen)
                .encode(),
        )
        .unwrap();
        let io = tokio::spawn(std::future::pending::<()>());
        let mut keep_alive = Vec::new();
        for pid in 1..=13u32 {
            let route = if pid <= 10 {
                Route::Proxied
            } else {
                Route::Direct
            };
            let (cmd_tx, cmd_rx) = mpsc::channel(64);
            let (bind_tx, bind_rx) = oneshot::channel();
            let (event_tx, event_rx) = mpsc::channel(1);
            keep_alive.push((cmd_rx, bind_rx, event_rx));
            tokio::time::timeout(
                Duration::from_secs(5),
                adopt_inbound_peer(
                    pid,
                    SocketAddr::from(([23, pid as u8, 0, 9], 6881)),
                    cmd_tx,
                    None,
                    bind_tx,
                    event_tx,
                    &mut peers,
                    &lengths,
                    &mut piece_tracker,
                    1,
                    4,
                    [0; 8],
                    Some(Id20([7; 20])),
                    io.abort_handle(),
                    Id20([9; 20]),
                    PeerLink::default(),
                    None,
                    route,
                ),
            )
            .await
            .unwrap();
            vote_yourip(&external, peers[&pid].addr.ip(), &hs, peers[&pid].route);
            if pid == 10 {
                assert_eq!(external.addresses(), Default::default());
            }
        }
        assert_eq!(
            external.external_v4(),
            Some("93.184.216.34".parse().unwrap())
        );
        io.abort();
    }

    #[test]
    fn relay_departure_releases_unanswered_rendezvous_only() {
        let t0 = Instant::now();
        let mut state = HolepunchState::new(true, 6881, None);
        let waiting = SocketAddr::from(([198, 51, 100, 70], 6881));
        let answered = SocketAddr::from(([198, 51, 100, 71], 6881));
        let other = SocketAddr::from(([198, 51, 100, 72], 6881));
        state.note_initiated(waiting, 3, t0);
        state.note_initiated(answered, 3, t0);
        state.note_initiated(other, 4, t0);
        assert!(state.take_answer(&answered, 3, t0));

        state.drop_relay(3);

        assert!(state.can_initiate(&waiting, t0));
        assert!(!state.can_initiate(&answered, t0));
        assert!(!state.can_initiate(&other, t0));
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
    fn tracker_batches_dial_by_priority_without_overtaking_older_batches_or_useful_peers() {
        let ranker = elected_ranker();
        let pool = priority_pool(20);
        let useful = pool[19];
        let mut older: Vec<SocketAddr> = pool[10..19].to_vec();
        let mut newer: Vec<SocketAddr> = pool[..10].to_vec();
        older.reverse();
        newer.reverse();
        ranker.order_batch(&mut older);
        ranker.order_batch(&mut newer);
        assert_eq!(older, pool[10..19]);
        assert_eq!(newer, pool[..10]);

        let mut priority_backlog = VecDeque::new();
        let mut peer_backlog = VecDeque::new();
        let mut dial_retries = VecDeque::new();
        let mut useful_redials = VecDeque::new();
        let mut known_addrs = HashSet::new();
        let blocklist = RwLock::new(BlockList::default());
        let mut queue = |addr, priority| {
            assert!(enqueue_peer_candidate(
                addr,
                priority,
                &mut priority_backlog,
                &mut peer_backlog,
                &mut dial_retries,
                &mut useful_redials,
                &mut known_addrs,
                &blocklist,
            ));
        };
        for addr in &older {
            queue(*addr, false);
        }
        queue(useful, true);
        for addr in &newer {
            queue(*addr, false);
        }

        assert_eq!(priority_backlog, VecDeque::from([useful]));
        let expected: VecDeque<SocketAddr> = older.iter().chain(&newer).copied().collect();
        assert_eq!(peer_backlog, expected);
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
                executable: false,
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
                sources: vec![Some(ChunkSource::Peer("192.0.2.9".parse().unwrap())), None],
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
            hash_conflict: false,
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

    #[tokio::test]
    async fn verified_piece_withdraws_interest_from_a_choking_peer_with_nothing_left() {
        let piece_length = crate::core::CHUNK_SIZE;
        let info = ValidatedTorrentMetaV1Info {
            name: "i.bin".into(),
            piece_length,
            pieces: vec![0; 40],
            private: false,
            files: vec![TorrentMetaInfo {
                path: vec!["i.bin".into()],
                length: 2 * piece_length as u64,
                padding: false,
                executable: false,
            }],
            single_file_mode: true,
        };
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(FilesystemStorage::new(&info, tmp.path()));
        let lengths = Lengths::new(2 * piece_length as u64, piece_length).unwrap();
        let mut pieces = PieceTracker::new(lengths);
        let mut chunks = ChunkTracker::new(lengths);
        let stats = Arc::new(Mutex::new(TorrentStats::initial(
            2 * piece_length as u64,
            2 * piece_length as u64,
            vec![2 * piece_length as u64],
        )));
        let (tx, mut rx) = mpsc::channel(8);
        let mut holder = Peer::connected(test_peer(52_301), tx, 1, 4, 8, true, true, None);
        holder.am_interested = true;
        holder.bitfield = vec![0b0100_0000];
        let (tx, mut other_rx) = mpsc::channel(8);
        let mut bystander = Peer::connected(test_peer(52_302), tx, 1, 4, 8, true, true, None);
        bystander.am_interested = true;
        bystander.bitfield = vec![0b1000_0000];
        pieces.add_peer_bitfield(&holder.bitfield);
        pieces.add_peer_bitfield(&bystander.bitfield);
        let mut peers = HashMap::from([(1, holder), (2, bystander)]);
        let mut assemblies = HashMap::new();
        let mut corruption = CorruptionTracker::new(None);

        process_verify_result(
            VerifyResult {
                piece_index: 1,
                write_error: None,
                verify_ok: true,
                from_webseed: false,
                hash_conflict: false,
                contributors: Vec::new(),
                failed_copy: None,
                culprits: Vec::new(),
            },
            &lengths,
            &mut pieces,
            &mut chunks,
            &mut peers,
            &storage,
            &stats,
            &mut assemblies,
            &mut corruption,
            &RwLock::new(BlockList::default()),
        )
        .await;

        assert!(!peers[&1].am_interested);
        let mut sent = Vec::new();
        while let Ok(PeerCommand::Send(message)) = rx.try_recv() {
            sent.push(message);
        }
        assert!(sent.iter().any(|m| matches!(m, Message::NotInterested)));
        assert!(peers[&2].am_interested);
        while let Ok(PeerCommand::Send(message)) = other_rx.try_recv() {
            assert!(!matches!(message, Message::NotInterested));
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_completed_x_file_is_executable_before_the_torrent_reports_finished() {
        use std::os::unix::fs::PermissionsExt;
        let piece_length = crate::core::CHUNK_SIZE;
        let info = ValidatedTorrentMetaV1Info {
            name: "run.sh".into(),
            piece_length,
            pieces: vec![0; 20],
            private: false,
            files: vec![TorrentMetaInfo {
                path: vec!["run.sh".into()],
                length: piece_length as u64,
                padding: false,
                executable: true,
            }],
            single_file_mode: true,
        };
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("run.sh");
        std::fs::write(&path, vec![0u8; piece_length as usize]).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let storage = Arc::new(FilesystemStorage::new(&info, tmp.path()));
        let lengths = Lengths::new(piece_length as u64, piece_length).unwrap();
        let mut pieces = PieceTracker::new(lengths);
        let mut chunks = ChunkTracker::new(lengths);
        let stats = Arc::new(Mutex::new(TorrentStats::initial(
            piece_length as u64,
            piece_length as u64,
            vec![piece_length as u64],
        )));
        let mut peers = HashMap::new();
        let mut assemblies = HashMap::new();
        let mut corruption = CorruptionTracker::new(None);

        process_verify_result(
            VerifyResult {
                piece_index: 0,
                write_error: None,
                verify_ok: true,
                from_webseed: false,
                hash_conflict: false,
                contributors: Vec::new(),
                failed_copy: None,
                culprits: Vec::new(),
            },
            &lengths,
            &mut pieces,
            &mut chunks,
            &mut peers,
            &storage,
            &stats,
            &mut assemblies,
            &mut corruption,
            &RwLock::new(BlockList::default()),
        )
        .await;

        assert!(stats.lock().finished);
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_ne!(mode & 0o111, 0, "mode {mode:o}");
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
            hash_conflict: false,
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
        let sources = [Some(ChunkSource::Peer(a)), Some(ChunkSource::Peer(b))];
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
    async fn a_hybrid_hash_conflict_halts_the_torrent_without_striking_peers() {
        let conflict = VerifyResult {
            piece_index: 7,
            write_error: None,
            verify_ok: false,
            from_webseed: false,
            hash_conflict: true,
            contributors: vec!["192.0.2.9".parse().unwrap()],
            failed_copy: None,
            culprits: Vec::new(),
        };
        let mut corruption = CorruptionTracker::new(Some(1));
        assert!(corruption.record_result(&conflict).is_empty());
        assert!(corruption.strikes.is_empty());
        let mut queue = DiskQueue::new(u64::MAX);
        let halted = queue
            .record_write(&conflict)
            .expect("a hash conflict stops downloading");
        assert!(halted.contains("Piece 7"), "{halted}");
        assert!(queue.is_full());
        assert_eq!(queue.record_write(&conflict), None);
    }

    #[tokio::test]
    async fn repeated_write_failures_halt_requests_until_resumed() {
        let mut queue = DiskQueue::new(u64::MAX);
        let outcome = |write_error: Option<&str>| VerifyResult {
            piece_index: 0,
            write_error: write_error.map(str::to_string),
            verify_ok: true,
            from_webseed: false,
            hash_conflict: false,
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
            Some("Disk write failed: No space left on device")
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
            hash_conflict: false,
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

    fn v2_table_data(total: u64) -> Vec<u8> {
        let mut data = vec![0u8; total as usize];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(31).wrapping_add(7);
        }
        data
    }

    fn make_v2_tables(num_pieces: usize, piece_length: u32) -> (Vec<MerkleProofTable>, Id32) {
        let blocks_per_piece = piece_length / BLOCK_SIZE;
        let total = num_pieces as u64 * piece_length as u64;
        let data = v2_table_data(total);
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
            executable: false,
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

    async fn leaf_hash_fixture(
        pieces: u32,
    ) -> (
        tempfile::TempDir,
        Arc<Vec<MerkleProofTable>>,
        Id32,
        Arc<FilesystemStorage>,
        Lengths,
        PieceTracker,
    ) {
        let piece_length = 4 * BLOCK_SIZE;
        let total = u64::from(pieces) * u64::from(piece_length);
        let (tables, root) = make_v2_tables(pieces as usize, piece_length);
        let info = ValidatedTorrentMetaV1Info {
            name: "leaf.bin".into(),
            piece_length,
            pieces: vec![0; 20 * pieces as usize],
            private: false,
            files: vec![TorrentMetaInfo {
                path: vec!["leaf.bin".into()],
                length: total,
                padding: false,
                executable: false,
            }],
            single_file_mode: true,
        };
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(FilesystemStorage::new(&info, tmp.path()));
        storage.write_at(0, &v2_table_data(total)).await.unwrap();
        let lengths = Lengths::new(total, piece_length).unwrap();
        let mut tracker = PieceTracker::new(lengths);
        for i in 0..pieces {
            tracker.set_local(lengths.validate_piece(i).unwrap(), true);
        }
        (tmp, Arc::new(tables), root, storage, lengths, tracker)
    }

    fn leaf_request(root: Id32) -> LeafHashRequest {
        LeafHashRequest {
            pieces_root: root.0,
            index: 0,
            length: 2,
            proof_layers: 0,
        }
    }

    async fn next_leaf_reply(rx: &mut mpsc::Receiver<PeerCommand>) -> Message {
        let reply = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("reply in time");
        let Some(PeerCommand::Send(message)) = reply else {
            panic!("expected a message, got {reply:?}");
        };
        message
    }

    #[tokio::test]
    async fn repeated_leaf_hash_requests_for_one_piece_do_not_reread_the_disk() {
        let (_tmp, tables, root, storage, _lengths, tracker) = leaf_hash_fixture(2).await;
        let pool = LeafHashPool::new(1, Throttle::unlimited());
        let slots = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(8);
        let mut answers = Vec::new();
        for _ in 0..3 {
            serve_leaf_hash_request(
                leaf_request(root),
                Some(&tables),
                &pool,
                &slots,
                &storage,
                &tracker,
                tx.clone(),
            );
            let Message::Hashes { hashes, .. } = next_leaf_reply(&mut rx).await else {
                panic!("expected hashes");
            };
            answers.push(hashes);
            storage
                .write_at(0, &vec![0u8; 8 * BLOCK_SIZE as usize])
                .await
                .unwrap();
        }
        assert_eq!(answers[0], answers[1]);
        assert_eq!(answers[0], answers[2]);
    }

    #[tokio::test]
    async fn a_peer_at_its_leaf_queue_cap_is_rejected_while_other_peers_still_wait() {
        let (_tmp, tables, root, storage, _lengths, tracker) = leaf_hash_fixture(2).await;
        let pool = LeafHashPool::new(0, Throttle::unlimited());
        let flooder_slots = Arc::new(Semaphore::new(1));
        let honest_slots = Arc::new(Semaphore::new(1));
        let (flood_tx, mut flood_rx) = mpsc::channel(8);
        let (honest_tx, mut honest_rx) = mpsc::channel(8);
        let serve = |slots: &Arc<Semaphore>, tx: &mpsc::Sender<PeerCommand>| {
            serve_leaf_hash_request(
                leaf_request(root),
                Some(&tables),
                &pool,
                slots,
                &storage,
                &tracker,
                tx.clone(),
            )
        };

        serve(&flooder_slots, &flood_tx);
        serve(&flooder_slots, &flood_tx);
        assert!(matches!(
            flood_rx.try_recv(),
            Ok(PeerCommand::Send(Message::HashReject { .. }))
        ));
        serve(&honest_slots, &honest_tx);
        assert!(flood_rx.try_recv().is_err());
        assert!(honest_rx.try_recv().is_err());

        pool.jobs.add_permits(2);
        assert!(matches!(
            next_leaf_reply(&mut flood_rx).await,
            Message::Hashes { .. }
        ));
        assert!(matches!(
            next_leaf_reply(&mut honest_rx).await,
            Message::Hashes { .. }
        ));
    }

    #[tokio::test]
    async fn a_peer_can_queue_a_whole_window_of_leaf_hash_requests_behind_a_busy_pool() {
        let (_tmp, tables, root, storage, _lengths, tracker) = leaf_hash_fixture(2).await;
        let pool = LeafHashPool::new(0, Throttle::unlimited());
        let (cmd_tx, _cmd_rx) = mpsc::channel(1);
        let peer = Peer::connected(
            "127.0.0.1:6881".parse().unwrap(),
            cmd_tx,
            1,
            8,
            8,
            true,
            false,
            None,
        );
        let (tx, mut rx) = mpsc::channel(256);
        let window = MAX_PEER_QUEUED_LEAF_JOBS;
        let serve = || {
            serve_leaf_hash_request(
                leaf_request(root),
                Some(&tables),
                &pool,
                &peer.leaf_slots,
                &storage,
                &tracker,
                tx.clone(),
            )
        };
        for _ in 0..window {
            serve();
        }
        assert!(rx.try_recv().is_err(), "a pipelined request was rejected");
        serve();
        assert!(matches!(
            rx.try_recv(),
            Ok(PeerCommand::Send(Message::HashReject { .. }))
        ));
        pool.jobs.add_permits(window);
        for _ in 0..window {
            assert!(matches!(
                next_leaf_reply(&mut rx).await,
                Message::Hashes { .. }
            ));
        }
    }

    fn serve_leaf(
        pool: &LeafHashPool,
        slots: &Arc<Semaphore>,
        tx: &mpsc::Sender<PeerCommand>,
        tables: &Arc<Vec<MerkleProofTable>>,
        root: Id32,
        storage: &Arc<FilesystemStorage>,
        tracker: &PieceTracker,
    ) {
        serve_leaf_hash_request(
            leaf_request(root),
            Some(tables),
            pool,
            slots,
            storage,
            tracker,
            tx.clone(),
        );
    }

    async fn assert_no_leaf_reply(rx: &mut mpsc::Receiver<PeerCommand>) {
        assert!(tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .is_err());
    }

    #[tokio::test]
    async fn leaf_hash_replies_are_charged_to_the_upload_limiter() {
        let (_tmp, tables, root, storage, _lengths, tracker) = leaf_hash_fixture(2).await;
        let global = Arc::new(RateLimiter::new(1));
        let limiter = Throttle::new(Arc::clone(&global), Arc::new(RateLimiter::unlimited()));
        let pool = LeafHashPool::new(1, limiter);
        let slots = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(8);
        serve_leaf(&pool, &slots, &tx, &tables, root, &storage, &tracker);
        assert_no_leaf_reply(&mut rx).await;
        global.set_limit(0);
        assert!(matches!(
            next_leaf_reply(&mut rx).await,
            Message::Hashes { .. }
        ));
    }

    #[tokio::test]
    async fn cached_leaf_hash_replies_are_charged_to_the_upload_limiter() {
        let (_tmp, tables, root, storage, _lengths, tracker) = leaf_hash_fixture(2).await;
        let global = Arc::new(RateLimiter::unlimited());
        let limiter = Throttle::new(Arc::clone(&global), Arc::new(RateLimiter::unlimited()));
        let pool = LeafHashPool::new(1, limiter);
        let slots = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(8);
        serve_leaf(&pool, &slots, &tx, &tables, root, &storage, &tracker);
        assert!(matches!(
            next_leaf_reply(&mut rx).await,
            Message::Hashes { .. }
        ));
        global.set_limit(1);
        serve_leaf(&pool, &slots, &tx, &tables, root, &storage, &tracker);
        assert_no_leaf_reply(&mut rx).await;
        global.set_limit(0);
        assert!(matches!(
            next_leaf_reply(&mut rx).await,
            Message::Hashes { .. }
        ));
    }

    #[tokio::test]
    async fn leaf_hash_disk_reads_do_not_spend_the_upload_budget() {
        let (_tmp, tables, root, storage, _lengths, tracker) = leaf_hash_fixture(2).await;
        let global = Arc::new(RateLimiter::new(1024));
        let limiter = Throttle::new(global, Arc::new(RateLimiter::unlimited()));
        let pool = LeafHashPool::new(1, limiter);
        let slots = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(8);
        serve_leaf(&pool, &slots, &tx, &tables, root, &storage, &tracker);
        assert!(matches!(
            next_leaf_reply(&mut rx).await,
            Message::Hashes { .. }
        ));
    }

    #[tokio::test]
    async fn leaf_hash_disk_reads_are_bounded_by_their_own_budget() {
        let (_tmp, tables, root, storage, _lengths, tracker) = leaf_hash_fixture(2).await;
        let pool = LeafHashPool::new(1, Throttle::unlimited());
        pool.reads.set_limit(1);
        let slots = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(8);
        serve_leaf(&pool, &slots, &tx, &tables, root, &storage, &tracker);
        assert_no_leaf_reply(&mut rx).await;
        pool.reads.set_limit(0);
        assert!(matches!(
            next_leaf_reply(&mut rx).await,
            Message::Hashes { .. }
        ));
    }

    #[tokio::test]
    async fn cached_leaf_hashes_need_neither_a_pool_permit_nor_a_read_budget() {
        let (_tmp, tables, root, storage, _lengths, tracker) = leaf_hash_fixture(2).await;
        let pool = LeafHashPool::new(1, Throttle::unlimited());
        let slots = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(8);
        serve_leaf(&pool, &slots, &tx, &tables, root, &storage, &tracker);
        assert!(matches!(
            next_leaf_reply(&mut rx).await,
            Message::Hashes { .. }
        ));
        pool.jobs.acquire().await.unwrap().forget();
        pool.reads.set_limit(1);
        serve_leaf(&pool, &slots, &tx, &tables, root, &storage, &tracker);
        assert!(matches!(
            next_leaf_reply(&mut rx).await,
            Message::Hashes { .. }
        ));
    }

    #[tokio::test]
    async fn leaf_hash_pool_permit_is_not_held_while_the_reply_waits_for_the_peer() {
        let (_tmp, tables, root, storage, _lengths, tracker) = leaf_hash_fixture(2).await;
        let pool = LeafHashPool::new(1, Throttle::unlimited());
        let slots = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(1);
        assert!(tx.try_send(PeerCommand::Disconnect).is_ok());
        serve_leaf(&pool, &slots, &tx, &tables, root, &storage, &tracker);
        tokio::time::timeout(Duration::from_secs(5), async {
            while pool.cache.lock().entries.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("piece is read and hashed");
        assert_eq!(pool.jobs.available_permits(), 1);
        assert!(matches!(rx.recv().await, Some(PeerCommand::Disconnect)));
        assert!(matches!(
            next_leaf_reply(&mut rx).await,
            Message::Hashes { .. }
        ));
    }

    #[tokio::test]
    async fn parked_leaf_hash_requests_share_the_table_instead_of_copying_it() {
        let (_tmp, tables, root, storage, _lengths, tracker) = leaf_hash_fixture(2).await;
        let pool = LeafHashPool::new(0, Throttle::unlimited());
        let slots = Arc::new(Semaphore::new(1));
        let (tx, mut rx) = mpsc::channel(8);
        serve_leaf(&pool, &slots, &tx, &tables, root, &storage, &tracker);
        tokio::task::yield_now().await;
        assert_eq!(Arc::strong_count(&tables), 2);
        pool.jobs.add_permits(1);
        assert!(matches!(
            next_leaf_reply(&mut rx).await,
            Message::Hashes { .. }
        ));
    }

    #[test]
    fn leaf_hash_cache_evicts_the_least_recently_used_piece_within_its_byte_budget() {
        let mut cache = LeafHashCache::default();
        let blocks = |n: usize| -> LeafBlocks { Arc::from(vec![Id32([0u8; 32]); n]) };
        let per_entry = LEAF_HASH_CACHE_BYTES / 32 / 2;
        cache.insert((0, 0), blocks(per_entry));
        cache.insert((0, 1), blocks(per_entry));
        assert!(cache.get((0, 0)).is_some());
        cache.insert((0, 2), blocks(per_entry));
        assert!(cache.get((0, 1)).is_none());
        assert!(cache.get((0, 0)).is_some());
        assert!(cache.get((0, 2)).is_some());
        assert!(cache.bytes <= LEAF_HASH_CACHE_BYTES);
        cache.insert((1, 0), blocks(LEAF_HASH_CACHE_BYTES / 32 + 1));
        assert!(cache.get((1, 0)).is_none());
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
                    executable: false,
                },
                TorrentMetaInfo {
                    path: vec!["b".into()],
                    length: 15,
                    padding: false,
                    executable: false,
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
        let secs = Duration::from_secs;
        assert_eq!(clamp_tracker_interval(secs(1), None), TRACKER_MIN_INTERVAL);
        assert_eq!(
            clamp_tracker_interval(secs(24 * 60 * 60), None),
            TRACKER_MAX_INTERVAL
        );
        assert_eq!(clamp_tracker_interval(secs(3600), None), secs(3600));
        assert_eq!(clamp_tracker_interval(secs(7200), None), secs(7200));
        assert_eq!(
            clamp_tracker_interval(secs(900), Some(secs(3600))),
            secs(3600)
        );
        assert_eq!(clamp_tracker_interval(secs(900), Some(secs(10))), secs(900));
        assert_eq!(
            clamp_tracker_interval(secs(900), Some(secs(365 * 24 * 60 * 60))),
            TRACKER_MAX_INTERVAL
        );
        assert_eq!(tracker_retry_delay(1), secs(15));
        assert_eq!(tracker_retry_delay(2), secs(30));
        assert_eq!(tracker_retry_delay(5), secs(300));
        assert!(tracker_retry_delay(12) >= secs(30 * 60));
        assert_eq!(tracker_retry_delay(99), secs(3600));
        let mut previous = Duration::ZERO;
        for failures in 1..=20 {
            let delay = tracker_retry_delay(failures);
            assert!(delay >= previous);
            previous = delay;
        }
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
        let mut holepunch = HolepunchState::default();
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
            &mut holepunch,
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
        let mut holepunch = HolepunchState::default();
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
            &mut holepunch,
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

    fn upload_peer(fast: bool) -> (Peer, PieceTracker, Lengths, mpsc::Receiver<PeerCommand>) {
        let lengths = Lengths::new(4 * 64 * 1024, 64 * 1024).unwrap();
        let mut piece_tracker = PieceTracker::new(lengths);
        piece_tracker.set_local(lengths.validate_piece(0).unwrap(), true);
        let (tx, rx) = mpsc::channel(8192);
        let mut peer = Peer::connected(test_peer(52_200), tx, 1, 4, 8, true, fast, None);
        peer.am_choking = false;
        (peer, piece_tracker, lengths, rx)
    }

    fn drain_rejects(rx: &mut mpsc::Receiver<PeerCommand>) -> usize {
        let mut rejects = 0;
        while let Ok(cmd) = rx.try_recv() {
            if matches!(cmd, PeerCommand::Send(Message::RejectRequest { .. })) {
                rejects += 1;
            }
        }
        rejects
    }

    #[test]
    fn upload_requests_are_capped_by_count_as_well_as_bytes() {
        let (mut peer, pt, lengths, mut rx) = upload_peer(true);
        let sent = MAX_PENDING_UPLOAD_REQUESTS as u32 * 4;
        let held: Vec<_> = (0..sent)
            .filter_map(|begin| admit_upload_request(&mut peer, &pt, &lengths, (0, begin, 1)))
            .collect();
        assert_eq!(held.len(), MAX_PENDING_UPLOAD_REQUESTS);
        assert_eq!(
            peer.pending_uploads.lock().keys.len(),
            MAX_PENDING_UPLOAD_REQUESTS
        );
        assert_eq!(
            drain_rejects(&mut rx),
            sent as usize - MAX_PENDING_UPLOAD_REQUESTS
        );
    }

    #[test]
    fn request_and_cancel_pairs_cannot_outrun_the_live_job_cap() {
        let (mut peer, pt, lengths, mut rx) = upload_peer(true);
        peer.am_choking = true;
        peer.sent_allowed_fast.insert(0);
        let sent = MAX_PENDING_UPLOAD_REQUESTS as u32 * 4;
        let mut held = Vec::new();
        for begin in 0..sent {
            let key = (0, begin, 1);
            if let Some((_, _, live)) = admit_upload_request(&mut peer, &pt, &lengths, key) {
                held.push(live);
            }
            cancel_upload_request(&mut peer, key);
        }
        assert_eq!(held.len(), MAX_PENDING_UPLOAD_REQUESTS);
        assert!(peer.pending_uploads.lock().keys.is_empty());
        assert_eq!(peer.pending_uploads.lock().live, held.len());
        assert_eq!(drain_rejects(&mut rx), sent as usize);
        drop(held);
        assert_eq!(peer.pending_uploads.lock().live, 0);
        assert!(admit_upload_request(&mut peer, &pt, &lengths, (0, 0, 1)).is_some());
    }

    #[tokio::test]
    async fn a_reject_owed_while_every_upload_slot_is_busy_goes_out_on_the_next_flush() {
        let (mut peer, pt, lengths, _unused) = upload_peer(true);
        let (cmd_tx, mut rx) = mpsc::channel(1);
        peer.cmd_tx = cmd_tx;
        let held: Vec<_> = (0..MAX_PENDING_UPLOAD_REQUESTS as u32)
            .map(|begin| {
                admit_upload_request(&mut peer, &pt, &lengths, (0, begin, 1))
                    .expect("slot")
                    .2
            })
            .collect();
        assert!(peer.cmd_tx.try_send(PeerCommand::Disconnect).is_ok());
        assert!(cancel_upload_request(&mut peer, (0, 0, 1)));
        assert_eq!(peer.pending_uploads.lock().owed.len(), 1);
        assert!(matches!(rx.try_recv(), Ok(PeerCommand::Disconnect)));
        flush_owed_rejects(&peer);
        assert!(matches!(
            rx.try_recv(),
            Ok(PeerCommand::Send(Message::RejectRequest {
                index: 0,
                begin: 0,
                length: 1
            }))
        ));
        assert!(peer.pending_uploads.lock().owed.is_empty());
        drop(held);
    }

    #[tokio::test]
    async fn deferred_cancel_rejects_hold_a_live_slot_and_are_still_delivered() {
        let (mut peer, pt, lengths, _unused) = upload_peer(true);
        let (cmd_tx, mut rx) = mpsc::channel(1);
        assert!(cmd_tx.try_send(PeerCommand::Disconnect).is_ok());
        peer.cmd_tx = cmd_tx;
        let mut admitted = 0;
        for begin in 0..MAX_PENDING_UPLOAD_REQUESTS as u32 * 4 {
            let key = (0, begin, 1);
            if admit_upload_request(&mut peer, &pt, &lengths, key).is_some() {
                admitted += 1;
            }
            cancel_upload_request(&mut peer, key);
        }
        assert_eq!(admitted, MAX_PENDING_UPLOAD_REQUESTS);
        assert_eq!(
            peer.pending_uploads.lock().live,
            MAX_PENDING_UPLOAD_REQUESTS
        );
        let mut received = 0;
        tokio::time::timeout(Duration::from_secs(5), async {
            while peer.pending_uploads.lock().live != 0 {
                rx.recv().await;
                received += 1;
            }
        })
        .await
        .expect("deferred rejects drain once the peer reads");
        while rx.try_recv().is_ok() {
            received += 1;
        }
        assert_eq!(received, 1 + MAX_PENDING_UPLOAD_REQUESTS);
    }

    #[test]
    fn cancelled_upload_bytes_stay_charged_until_the_job_ends() {
        let (mut peer, pt, lengths, _rx) = upload_peer(true);
        let key = (0, 0, 64 * 1024);
        let mut held = Vec::new();
        for _ in 0..MAX_PENDING_UPLOAD_REQUESTS * 2 {
            let Some((_, _, live)) = admit_upload_request(&mut peer, &pt, &lengths, key) else {
                break;
            };
            held.push(live);
            cancel_upload_request(&mut peer, key);
        }
        assert_eq!(
            held.len() as u64 * u64::from(key.2),
            MAX_PENDING_UPLOAD_BYTES
        );
        assert_eq!(
            peer.pending_uploads.lock().live_bytes,
            MAX_PENDING_UPLOAD_BYTES
        );
        held.pop();
        assert!(admit_upload_request(&mut peer, &pt, &lengths, key).is_some());
    }

    #[test]
    fn a_duplicate_of_a_pending_upload_request_is_ignored_not_rejected() {
        let (mut fast, pt, lengths, mut rx) = upload_peer(true);
        assert!(admit_upload_request(&mut fast, &pt, &lengths, (0, 0, 16384)).is_some());
        assert!(admit_upload_request(&mut fast, &pt, &lengths, (0, 0, 16384)).is_none());
        assert_eq!(drain_rejects(&mut rx), 0);
        assert_eq!(fast.pending_uploads.lock().keys.len(), 1);

        let (mut plain, pt, lengths, mut rx) = upload_peer(false);
        assert!(admit_upload_request(&mut plain, &pt, &lengths, (0, 0, 16384)).is_some());
        assert!(admit_upload_request(&mut plain, &pt, &lengths, (0, 0, 16384)).is_none());
        assert_eq!(drain_rejects(&mut rx), 0);
    }

    #[test]
    fn choked_peer_gets_one_copy_of_each_allowed_fast_piece() {
        let (mut peer, pt, lengths, mut rx) = upload_peer(true);
        peer.am_choking = true;
        peer.sent_allowed_fast.insert(0);
        for chunk in 0..4u32 {
            let key = (0, chunk * 16384, 16384);
            assert!(admit_upload_request(&mut peer, &pt, &lengths, key).is_some());
            assert!(peer.pending_uploads.lock().remove(&key));
        }
        assert_eq!(drain_rejects(&mut rx), 0);
        assert!(admit_upload_request(&mut peer, &pt, &lengths, (0, 0, 16384)).is_none());
        assert_eq!(drain_rejects(&mut rx), 1);
        peer.am_choking = false;
        assert!(admit_upload_request(&mut peer, &pt, &lengths, (0, 0, 16384)).is_some());
    }

    #[test]
    fn canceled_allowed_fast_request_gives_its_budget_back() {
        let (mut peer, pt, lengths, _rx) = upload_peer(true);
        peer.am_choking = true;
        peer.sent_allowed_fast.insert(0);
        let key = (0, 0, 65536);
        assert!(admit_upload_request(&mut peer, &pt, &lengths, key).is_some());
        assert!(cancel_upload_request(&mut peer, key));
        assert!(admit_upload_request(&mut peer, &pt, &lengths, key).is_some());
    }

    fn sent_requests(rx: &mut mpsc::Receiver<PeerCommand>) -> Vec<(u32, u32)> {
        let mut out = Vec::new();
        while let Ok(cmd) = rx.try_recv() {
            if let PeerCommand::Send(Message::Request { index, begin, .. }) = cmd {
                out.push((index, begin));
            }
        }
        out
    }

    #[test]
    fn rejected_piece_is_not_re_requested_from_an_unchoked_peer_until_the_backoff_ends() {
        let (mut peers, mut pt, mut ct, mut rx) = driven_peer(3);
        let lengths = *pt.lengths();
        drive_peer(1, &mut peers, &mut pt, &mut ct, &HashSet::new());
        assert_eq!(sent_requests(&mut rx).len(), 3);
        let peer = peers.get_mut(&1).unwrap();
        peer.last_piece = Some(0);
        let kick = apply_request_reject(1, peer, &mut pt, &mut ct, &lengths, (0, 0, 16384), (4, 8));
        assert_eq!(kick, Some(true));

        drive_peer(1, &mut peers, &mut pt, &mut ct, &HashSet::new());
        assert!(sent_requests(&mut rx).is_empty());
        assert_eq!(peers[&1].outstanding.len(), 2);

        let peer = peers.get_mut(&1).unwrap();
        for until in peer.reject_backoff.values_mut() {
            *until = Instant::now();
        }
        drive_peer(1, &mut peers, &mut pt, &mut ct, &HashSet::new());
        assert_eq!(sent_requests(&mut rx), vec![(0, 0)]);
    }

    #[test]
    fn unexpected_reject_is_a_protocol_error_but_a_canceled_one_is_not() {
        let (mut peers, mut pt, mut ct, _rx) = driven_peer(2);
        let lengths = *pt.lengths();
        let peer = peers.get_mut(&1).unwrap();
        assert_eq!(
            apply_request_reject(1, peer, &mut pt, &mut ct, &lengths, (0, 0, 16384), (4, 8)),
            None
        );
        remember_canceled_request(peer, (0, 0, 16384));
        assert_eq!(
            apply_request_reject(1, peer, &mut pt, &mut ct, &lengths, (0, 0, 16384), (4, 8)),
            Some(false)
        );
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
    fn private_torrents_only_redial_inbound_peers_the_tracker_listed() {
        let listed = test_peer(51_413);
        let authorized: HashSet<SocketAddr> = [listed].into_iter().collect();
        let stranger: SocketAddr = "198.51.100.9:6881".parse().unwrap();
        let advertised = |private, addr: SocketAddr| {
            advertised_listen_addr(private, addr.ip(), addr.port(), &authorized)
        };
        assert_eq!(advertised(true, listed), Some(listed));
        assert_eq!(advertised(true, stranger), None);
        assert_eq!(advertised(false, stranger), Some(stranger));
        assert_eq!(
            advertised_listen_addr(false, stranger.ip(), 0, &authorized),
            None
        );
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

    fn recycle(complete: bool, now: Instant, ranker: &PeerRanker) -> RecycleScope<'_> {
        RecycleScope {
            complete,
            now,
            total_pieces: 8,
            ranker,
        }
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
        assert_eq!(
            redundant_peers(&peers, recycle(false, now, &PeerRanker::default()), 8),
            vec![2, 1]
        );
        assert_eq!(
            redundant_peers(&peers, recycle(false, now, &PeerRanker::default()), 1),
            vec![2]
        );
        peers.get_mut(&3).unwrap().upload_only = true;
        assert_eq!(
            redundant_peers(&peers, recycle(true, now, &PeerRanker::default()), 8),
            vec![3]
        );
    }

    #[test]
    fn finished_torrent_makes_room_for_a_leecher_by_dropping_the_oldest_redundant_peer() {
        let now = Instant::now();
        let mut peers = HashMap::new();
        let mut rxs = Vec::new();
        for pid in 1..=4u32 {
            let (mut peer, rx) = choke_peer(false, true);
            peer.upload_only = pid != 4;
            peer.connected_at = now - Duration::from_secs(u64::from(10 * (5 - pid)));
            peers.insert(pid, peer);
            rxs.push(rx);
        }
        assert_eq!(
            inbound_eviction_victim(&peers, &[], recycle(true, now, &PeerRanker::default())),
            Some(1)
        );
        assert_eq!(
            inbound_eviction_victim(&peers, &[1], recycle(true, now, &PeerRanker::default())),
            Some(2)
        );
        assert_eq!(
            inbound_eviction_victim(
                &peers,
                &[1, 2, 3],
                recycle(true, now, &PeerRanker::default())
            ),
            None
        );
        assert_eq!(
            inbound_eviction_victim(&peers, &[], recycle(false, now, &PeerRanker::default())),
            None
        );
        peers.get_mut(&1).unwrap().peer_interested = true;
        assert_eq!(
            inbound_eviction_victim(&peers, &[], recycle(true, now, &PeerRanker::default())),
            Some(2)
        );
    }

    fn crowded_swarm(
        count: u32,
        age: Duration,
    ) -> (HashMap<u32, Peer>, Vec<mpsc::Receiver<PeerCommand>>) {
        let mut peers = HashMap::new();
        let mut rxs = Vec::new();
        for pid in 1..=count {
            let (mut peer, rx) = choke_peer(false, true);
            peer.upload_only = true;
            peer.connected_at = Instant::now() - age - Duration::from_secs(u64::from(pid));
            peers.insert(pid, peer);
            rxs.push(rx);
        }
        (peers, rxs)
    }

    #[test]
    fn a_downloading_torrent_never_drops_a_peer_for_an_inbound_arrival() {
        let (peers, _rxs) = crowded_swarm(4, PEER_USELESS_AFTER + Duration::from_secs(60));
        let mut evictions = Vec::new();
        assert!(!redundant_peers(
            &peers,
            recycle(false, Instant::now(), &PeerRanker::default()),
            8
        )
        .is_empty());
        let room = inbound_room(
            &peers,
            &mut evictions,
            4,
            false,
            recycle(false, Instant::now(), &PeerRanker::default()),
        );
        assert!(room.crowded);
        assert_eq!(room.victim, None);
        let room = inbound_room(
            &peers,
            &mut evictions,
            4,
            false,
            recycle(true, Instant::now(), &PeerRanker::default()),
        );
        assert!(room.crowded);
        assert!(room.victim.is_some());
    }

    #[test]
    fn a_paused_or_roomy_torrent_picks_no_inbound_victim() {
        let (peers, _rxs) = crowded_swarm(4, Duration::from_secs(600));
        let mut evictions = Vec::new();
        let paused = inbound_room(
            &peers,
            &mut evictions,
            4,
            true,
            recycle(true, Instant::now(), &PeerRanker::default()),
        );
        assert!(paused.crowded && paused.victim.is_none());
        let roomy = inbound_room(
            &peers,
            &mut evictions,
            5,
            false,
            recycle(true, Instant::now(), &PeerRanker::default()),
        );
        assert!(!roomy.crowded && roomy.victim.is_none());
    }

    #[test]
    fn queued_evictions_free_room_only_while_their_peers_are_still_connected() {
        let (mut peers, _rxs) = crowded_swarm(4, Duration::from_secs(600));
        let mut evictions = Vec::new();
        let room = inbound_room(
            &peers,
            &mut evictions,
            4,
            false,
            recycle(true, Instant::now(), &PeerRanker::default()),
        );
        let victim = room.victim.expect("a finished torrent frees a slot");
        evictions.push(victim);
        let pending = inbound_room(
            &peers,
            &mut evictions,
            4,
            false,
            recycle(true, Instant::now(), &PeerRanker::default()),
        );
        assert!(!pending.crowded);
        assert_eq!(evictions, vec![victim]);

        peers.remove(&victim);
        evictions.push(victim);
        let gone = inbound_room(
            &peers,
            &mut evictions,
            3,
            false,
            recycle(true, Instant::now(), &PeerRanker::default()),
        );
        assert!(evictions.is_empty());
        assert!(gone.crowded);
        assert!(gone.victim.is_some());
    }

    fn elected_ranker() -> PeerRanker {
        PeerRanker::new(
            ExternalAddresses {
                v4: Some(std::net::Ipv4Addr::new(123, 213, 32, 10)),
                v6: None,
            },
            6881,
        )
    }

    fn priority_pool(count: u8) -> Vec<SocketAddr> {
        let ranker = elected_ranker();
        let mut pool: Vec<SocketAddr> = (0..count)
            .map(|n| SocketAddr::from(([98, 40 + n, 54, 32], 6881)))
            .collect();
        pool.sort_by_key(|addr| Reverse(ranker.priority(*addr).unwrap()));
        let distinct: HashSet<u32> = pool
            .iter()
            .map(|addr| ranker.priority(*addr).unwrap())
            .collect();
        assert_eq!(distinct.len(), pool.len());
        pool
    }

    fn redundant_swarm(
        addrs: &[SocketAddr],
        connected: Instant,
    ) -> (HashMap<u32, Peer>, Vec<mpsc::Receiver<PeerCommand>>) {
        let mut peers = HashMap::new();
        let mut rxs = Vec::new();
        for (index, addr) in addrs.iter().enumerate() {
            let (mut peer, rx) = choke_peer(false, true);
            peer.addr = *addr;
            peer.listen_addr = Some(*addr);
            peer.upload_only = true;
            peer.connected_at = connected + Duration::from_millis(index as u64);
            peers.insert(index as u32 + 1, peer);
            rxs.push(rx);
        }
        (peers, rxs)
    }

    #[test]
    fn redundant_peers_of_one_age_window_are_recycled_lowest_priority_first() {
        let ranker = elected_ranker();
        let now = Instant::now();
        let mut addrs = priority_pool(6);
        addrs.swap(0, 5);
        addrs.swap(2, 3);
        let (peers, _rxs) = redundant_swarm(&addrs, now - Duration::from_secs(40));
        let mut by_priority: Vec<u32> = (1..=6).collect();
        by_priority.sort_by_key(|&pid| ranker.priority(addrs[pid as usize - 1]).unwrap());
        assert_ne!(by_priority, (1..=6).collect::<Vec<u32>>());

        assert_eq!(
            redundant_peers(&peers, recycle(true, now, &ranker), 6),
            by_priority
        );
        assert_eq!(
            redundant_peers(&peers, recycle(true, now, &ranker), 2),
            by_priority[..2]
        );
        assert_eq!(
            redundant_peers(&peers, recycle(true, now, &PeerRanker::default()), 6),
            (1..=6).collect::<Vec<u32>>()
        );
    }

    #[test]
    fn an_older_redundant_peer_still_goes_before_a_younger_low_priority_one() {
        let ranker = elected_ranker();
        let now = Instant::now();
        let pool = priority_pool(2);
        let (best, worst) = (pool[0], pool[1]);
        let (mut peers, _rxs) = redundant_swarm(&[best, worst], now - Duration::from_secs(40));
        peers.get_mut(&2).unwrap().connected_at =
            now - Duration::from_secs(40) + EVICTION_TIE_WINDOW + Duration::from_secs(1);
        assert_eq!(
            redundant_peers(&peers, recycle(true, now, &ranker), 2),
            vec![1, 2]
        );
        peers.get_mut(&2).unwrap().connected_at =
            now - Duration::from_secs(40) + Duration::from_secs(1);
        assert_eq!(
            redundant_peers(&peers, recycle(true, now, &ranker), 2),
            vec![2, 1]
        );
    }

    #[test]
    fn priority_never_makes_a_productive_peer_recyclable() {
        let ranker = elected_ranker();
        let now = Instant::now();
        let old = now - PEER_USELESS_AFTER - Duration::from_secs(67);
        let pool = priority_pool(5);
        let (mut peers, _rxs) = redundant_swarm(&pool, old);
        for peer in peers.values_mut() {
            peer.upload_only = false;
        }
        peers.get_mut(&5).unwrap().downloaded_total = 1;
        peers.get_mut(&4).unwrap().peer_interested = true;
        peers.get_mut(&3).unwrap().am_interested = true;
        peers.get_mut(&3).unwrap().peer_choking = false;
        let recycled = redundant_peers(&peers, recycle(false, now, &ranker), 8);
        assert_eq!(recycled, vec![2, 1]);
    }

    #[test]
    fn a_candidate_without_a_priority_keeps_the_age_order() {
        let ranker = elected_ranker();
        let now = Instant::now();
        let mut addrs = priority_pool(3);
        addrs.push(SocketAddr::from((
            std::net::Ipv6Addr::new(0x2001, 0xdb8, 2, 0, 0, 0, 0, 5),
            6881,
        )));
        let (peers, _rxs) = redundant_swarm(&addrs, now - Duration::from_secs(40));
        assert_eq!(
            redundant_peers(&peers, recycle(true, now, &ranker), 8),
            vec![1, 2, 3, 4]
        );
    }

    #[test]
    fn an_inbound_arrival_recycles_the_lowest_priority_redundant_peer() {
        let ranker = elected_ranker();
        let now = Instant::now();
        let mut addrs = priority_pool(4);
        addrs.swap(0, 3);
        let (peers, _rxs) = redundant_swarm(&addrs, now - Duration::from_secs(40));
        let mut by_priority: Vec<u32> = (1..=4).collect();
        by_priority.sort_by_key(|&pid| ranker.priority(addrs[pid as usize - 1]).unwrap());
        assert_eq!(
            inbound_eviction_victim(&peers, &[], recycle(true, now, &ranker)),
            Some(by_priority[0])
        );
        assert_eq!(
            inbound_eviction_victim(&peers, &[by_priority[0]], recycle(true, now, &ranker)),
            Some(by_priority[1])
        );
        assert_eq!(
            inbound_eviction_victim(&peers, &[], recycle(false, now, &ranker)),
            None
        );
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
    fn allowed_fast_set_matches_the_bep_6_reference_vector() {
        let hash = Id20::new([0xaa; 20]);
        let set = allowed_fast_set(&hash, "80.4.4.200:6881".parse().unwrap(), 1313);
        assert_eq!(set[..7], [1059, 431, 808, 1217, 287, 376, 1188]);
    }

    #[test]
    fn allowed_fast_set_ignores_the_host_part_of_the_address() {
        let hash = Id20::new([3; 20]);
        let set = |addr: &str| allowed_fast_set(&hash, addr.parse().unwrap(), 5000);
        assert_eq!(set("80.4.4.1:1"), set("80.4.4.200:9"));
        assert_ne!(set("80.4.4.1:1"), set("80.4.5.1:1"));
        assert_eq!(set("[2001:db8:1:2::1]:1"), set("[2001:db8:1:2:ffff::7]:2"));
        assert_ne!(set("[2001:db8:1:2::1]:1"), set("[2001:db8:1:3::1]:1"));
        assert_eq!(set("[::ffff:80.4.4.200]:1"), set("80.4.4.1:1"));
        assert_ne!(set("[::1]:1"), set("0.0.0.1:1"));
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
            state.tiers[0][1].started_sent = true;
            let promoted = state.tiers[0].remove(1);
            state.tiers[0].insert(0, promoted);
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
        let (hung_addr, hung_task) = hung_tracker().await;
        let (live_addr, live_task) = serve_tracker(
            b"d8:intervali1800e5:peers6:\x7f\x00\x00\x01\x1a\xe1e",
            |_| {},
        )
        .await;
        let (tx, mut rx) = mpsc::channel(8);
        let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
        let mut pollers = spawn_tracker_tier_pollers(
            tracker_ctx(
                tx,
                stats,
                Arc::new(Mutex::new(TrackerLifecycle::default())),
                None,
            ),
            vec![
                vec![format!("http://{hung_addr}/announce")],
                vec![format!("http://{live_addr}/announce")],
            ],
            &[Id20::new([1; 20])],
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
        scripted_tracker(b"d8:intervali1800e5:peers0:e").await
    }

    async fn serve_tracker(
        body: &'static [u8],
        on_request: impl Fn(String) + Send + 'static,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let request = String::from_utf8_lossy(&buf[..n]);
                on_request(request.lines().next().unwrap_or_default().to_string());
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(body).await;
            }
        });
        (addr, task)
    }

    async fn scripted_tracker(
        body: &'static [u8],
    ) -> (
        SocketAddr,
        mpsc::UnboundedReceiver<String>,
        tokio::task::JoinHandle<()>,
    ) {
        let (line_tx, line_rx) = mpsc::unbounded_channel();
        let (addr, task) = serve_tracker(body, move |line| {
            let _ = line_tx.send(line);
        })
        .await;
        (addr, line_rx, task)
    }

    async fn hung_tracker() -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        });
        (addr, task)
    }

    const RANKED_TRACKER_PORT: u16 = 51413;

    fn rank_direct_announces_on_the_test_port(route: Route, port: u16) -> PeerRanker {
        if route == Route::Direct && port == RANKED_TRACKER_PORT {
            elected_ranker()
        } else {
            PeerRanker::default()
        }
    }

    #[tokio::test]
    async fn both_tracker_pollers_hand_peers_over_highest_priority_first() {
        for private in [true, false] {
            let pool = priority_pool(12);
            let mut body = b"d8:intervali1800e5:peers72:".to_vec();
            for addr in pool.iter().rev() {
                let SocketAddr::V4(addr) = addr else {
                    panic!("the pool is IPv4 only")
                };
                body.extend_from_slice(&addr.ip().octets());
                body.extend_from_slice(&addr.port().to_be_bytes());
            }
            body.push(b'e');
            let (tracker, _lines, server) =
                scripted_tracker(Box::leak(body.into_boxed_slice())).await;
            let (tx, mut rx) = mpsc::channel(32);
            let (tier_tx, _tier_rx) = mpsc::channel(1);
            let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
            let mut pollers = spawn_tracker_tier_pollers(
                TrackerCtx {
                    port: RANKED_TRACKER_PORT,
                    ranker_for: rank_direct_announces_on_the_test_port,
                    ..tracker_ctx(
                        tx,
                        stats,
                        Arc::new(Mutex::new(TrackerLifecycle::default())),
                        private.then_some(tier_tx),
                    )
                },
                vec![vec![format!("http://{tracker}/announce")]],
                &[Id20::new([1; 20])],
            );
            let mut found = Vec::new();
            while found.len() < pool.len() {
                let envelope = tokio::time::timeout(Duration::from_secs(10), rx.recv())
                    .await
                    .unwrap_or_else(|_| panic!("private={private}: tracker peers never arrived"))
                    .expect("peer channel open");
                found.push(envelope.candidate.addr);
            }
            tokio::time::timeout(Duration::from_secs(10), pollers.shutdown(false))
                .await
                .unwrap();
            server.abort();
            assert_eq!(found, pool, "private={private}");
        }
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
            tracker_ctx(
                tx,
                Arc::clone(&stats),
                Arc::clone(&lifecycle),
                private.then_some(tier_tx),
            ),
            vec![vec![format!("http://{addr}/announce")]],
            &[Id20::new([1; 20])],
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

    async fn failing_tracker() -> (
        SocketAddr,
        Arc<std::sync::atomic::AtomicUsize>,
        tokio::task::JoinHandle<()>,
    ) {
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::clone(&hits);
        let (addr, task) = serve_tracker(b"d14:failure reason4:nopee", move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
        })
        .await;
        (addr, hits, task)
    }

    fn private_pollers(
        tiers: Vec<Vec<String>>,
        tier_tx: TierChangeTx,
    ) -> (TrackerPollers, mpsc::Receiver<PeerEnvelope>) {
        let (tx, rx) = mpsc::channel(8);
        let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
        let pollers = spawn_tracker_tier_pollers(
            tracker_ctx(
                tx,
                stats,
                Arc::new(Mutex::new(TrackerLifecycle::default())),
                Some(tier_tx),
            ),
            tiers,
            &[Id20::new([1; 20])],
        );
        (pollers, rx)
    }

    async fn wait_for_first_hit(hits: &std::sync::atomic::AtomicUsize) {
        eventually("the tracker to be contacted", || {
            hits.load(Ordering::SeqCst) > 0
        })
        .await;
    }

    #[tokio::test]
    async fn failing_private_tracker_backs_off_instead_of_hot_looping() {
        let (addr, hits, server) = failing_tracker().await;
        let (tier_tx, mut tier_rx) = mpsc::channel(8);
        let (mut pollers, _rx) =
            private_pollers(vec![vec![format!("http://{addr}/announce")]], tier_tx);
        wait_for_first_hit(&hits).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        tokio::time::timeout(Duration::from_secs(10), pollers.shutdown(false))
            .await
            .unwrap();
        server.abort();
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert!(tier_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn failing_private_tiers_neither_flip_nor_disconnect_peers() {
        let (first, first_hits, first_server) = failing_tracker().await;
        let (second, second_hits, second_server) = failing_tracker().await;
        let (tier_tx, mut tier_rx) = mpsc::channel(8);
        let (mut pollers, _rx) = private_pollers(
            vec![
                vec![format!("http://{first}/announce")],
                vec![format!("http://{second}/announce")],
            ],
            tier_tx,
        );
        wait_for_first_hit(&first_hits).await;
        wait_for_first_hit(&second_hits).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        tokio::time::timeout(Duration::from_secs(10), pollers.shutdown(false))
            .await
            .unwrap();
        first_server.abort();
        second_server.abort();
        assert_eq!(first_hits.load(Ordering::SeqCst), 1);
        assert_eq!(second_hits.load(Ordering::SeqCst), 1);
        assert!(tier_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn private_tier_leaves_siblings_dormant_after_the_first_tracker_answers() {
        let (first, mut first_lines, first_server) = recording_tracker().await;
        let (second, mut second_lines, second_server) = recording_tracker().await;
        let (tier_tx, _tier_rx) = mpsc::channel(8);
        let (mut pollers, _rx) = private_pollers(
            vec![vec![
                format!("http://{first}/announce"),
                format!("http://{second}/announce"),
            ]],
            tier_tx,
        );
        let first_line = tokio::time::timeout(Duration::from_secs(5), first_lines.recv())
            .await
            .expect("first tracker announce")
            .expect("line");
        assert!(first_line.contains("event=started"), "{first_line}");
        assert!(
            tokio::time::timeout(Duration::from_millis(1000), second_lines.recv())
                .await
                .is_err(),
            "the sibling must stay dormant while the first tracker works"
        );
        tokio::time::timeout(Duration::from_secs(10), pollers.shutdown(false))
            .await
            .unwrap();
        first_server.abort();
        second_server.abort();
    }

    fn run_state_of(tiers: &[&[&str]], now: Instant) -> TrackerRunState {
        TrackerRunState {
            tiers: tiers
                .iter()
                .map(|tier| {
                    let urls: Vec<String> = tier.iter().map(|url| url.to_string()).collect();
                    tracker_url_states(&urls, now)
                })
                .collect(),
            last_successful_tier: None,
        }
    }

    fn ok_response(interval_secs: u64) -> AnnounceResponse {
        AnnounceResponse {
            interval: Duration::from_secs(interval_secs),
            peers: Vec::new(),
            min_interval: None,
            seeders: None,
            leechers: None,
            tracker_id: None,
            warning: None,
            external_ip: None,
        }
    }

    fn settle_ok(state: &mut TrackerRunState, tier: usize, url: &str, now: Instant) -> bool {
        state.settle_tier_success(
            tier,
            url,
            AnnounceEvent::Started,
            false,
            false,
            &ok_response(1800),
            now,
        )
    }

    fn failure() -> TrackerError {
        TrackerError::Http("down".into())
    }

    struct TrackerLogCapture(Arc<Mutex<Vec<String>>>);

    impl tracing::Subscriber for TrackerLogCapture {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }

        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

        fn event(&self, event: &tracing::Event<'_>) {
            struct Message(String);
            impl tracing::field::Visit for Message {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    if field.name() == "message" {
                        self.0 = format!("{value:?}");
                    }
                }
            }
            let mut message = Message(String::new());
            event.record(&mut message);
            if message.0.starts_with("tracker ") {
                self.0.lock().push(message.0);
            }
        }

        fn enter(&self, _: &tracing::span::Id) {}

        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[tokio::test]
    async fn announce_logs_never_carry_tracker_paths_or_query_secrets() {
        let captured = Arc::new(Mutex::new(Vec::new()));
        let capture = tracing::Dispatch::new(TrackerLogCapture(Arc::clone(&captured)));
        // a lone live dispatch makes other threads register new callsites as never interested
        let _second_dispatch = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let _capture = tracing::dispatcher::set_default(&capture);
        let mut hosts = Vec::new();
        for private in [false, true] {
            let (ok, mut ok_lines, ok_server) = recording_tracker().await;
            let (bad, bad_hits, bad_server) = failing_tracker().await;
            hosts.push((ok, bad));
            let (tx, _rx) = mpsc::channel(8);
            let (tier_tx, _tier_rx) = mpsc::channel(8);
            let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
            let lifecycle = Arc::new(Mutex::new(TrackerLifecycle::default()));
            let mut pollers = spawn_tracker_tier_pollers(
                tracker_ctx(
                    tx,
                    Arc::clone(&stats),
                    Arc::clone(&lifecycle),
                    private.then_some(tier_tx),
                ),
                vec![
                    vec!["ws://127.0.0.1:1/SECRETPATH/announce?passkey=SECRETKEY".to_string()],
                    vec![format!(
                        "http://{bad}/SECRETPATH/announce?passkey=SECRETKEY"
                    )],
                    vec![format!("http://{ok}/SECRETPATH/announce?passkey=SECRETKEY")],
                ],
                &[Id20::new([1; 20])],
            );
            tokio::time::timeout(Duration::from_secs(5), ok_lines.recv())
                .await
                .expect("announce")
                .expect("line");
            wait_for_started(&lifecycle).await;
            assert!(bad_hits.load(Ordering::SeqCst) >= 1);
            let failed = format!("ANNOUNCE fail url=http://{bad} ");
            let deadline = Instant::now() + Duration::from_secs(5);
            while !captured.lock().iter().any(|line| line.contains(&failed)) {
                assert!(Instant::now() < deadline, "failure was never logged");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            tokio::time::timeout(Duration::from_secs(15), pollers.shutdown(true))
                .await
                .unwrap();
            ok_server.abort();
            bad_server.abort();
        }
        let logs = captured.lock().clone();
        assert!(logs.iter().all(|line| !line.contains("SECRET")), "{logs:?}");
        for (ok, bad) in hosts {
            let has = |needle: String| logs.iter().any(|line| line.contains(&needle));
            assert!(has(format!("ANNOUNCE ok url=http://{ok} ")), "{logs:?}");
            assert!(has(format!("STOPPED ok url=http://{ok}")), "{logs:?}");
            assert!(has(format!("ANNOUNCE fail url=http://{bad} ")), "{logs:?}");
        }
        assert!(
            logs.iter()
                .any(|line| line.contains("unsupported scheme: ws://127.0.0.1:1")),
            "{logs:?}"
        );
    }

    #[tokio::test]
    async fn failover_to_a_backup_tier_still_delivers_its_first_peers() {
        let (bad, _hits, bad_server) = failing_tracker().await;
        let (good, _lines, good_server) =
            scripted_tracker(b"d8:intervali1800e5:peers6:\x7f\x00\x00\x01\x1a\xe1e").await;
        let tiers = vec![
            vec![format!("http://{bad}/announce")],
            vec![format!("http://{good}/announce")],
        ];
        let hash = Id20::new([1; 20]);
        let lifecycle = Arc::new(Mutex::new(TrackerLifecycle::default()));
        lifecycle
            .lock()
            .run_state(hash, &tiers)
            .last_successful_tier = Some(0);
        let generation = GenerationToken::new();
        let (tx, mut rx) = mpsc::channel(8);
        let (tier_tx, mut tier_rx) = mpsc::channel(4);
        let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
        let mut pollers = spawn_tracker_tier_pollers(
            TrackerCtx {
                generation: generation.clone(),
                ..tracker_ctx(tx, stats, lifecycle, Some(tier_tx))
            },
            tiers,
            &[hash],
        );
        let delivered = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    biased;
                    Some(ack) = tier_rx.recv() => {
                        generation.bump();
                        let _ = ack.send(());
                    }
                    Some(envelope) = rx.recv() => {
                        if generation.is_current(envelope.generation) {
                            return envelope;
                        }
                    }
                }
            }
        })
        .await
        .expect("the backup tier's peers were discarded as stale");
        assert_eq!(delivered.candidate.addr.port(), 6881);
        tokio::time::timeout(Duration::from_secs(10), pollers.shutdown(false))
            .await
            .unwrap();
        bad_server.abort();
        good_server.abort();
    }

    #[tokio::test]
    async fn a_tier_change_queued_behind_others_still_tags_its_peers_with_the_new_generation() {
        let (bad, _hits, bad_server) = failing_tracker().await;
        let (good, mut lines, good_server) =
            scripted_tracker(b"d8:intervali1800e5:peers6:\x7f\x00\x00\x01\x1a\xe1e").await;
        let tiers = vec![
            vec![format!("http://{bad}/announce")],
            vec![format!("http://{good}/announce")],
        ];
        let hash = Id20::new([1; 20]);
        let lifecycle = Arc::new(Mutex::new(TrackerLifecycle::default()));
        lifecycle
            .lock()
            .run_state(hash, &tiers)
            .last_successful_tier = Some(0);
        let generation = GenerationToken::new();
        let (tx, mut rx) = mpsc::channel(8);
        let (tier_tx, mut tier_rx) = mpsc::channel(1);
        let (earlier, _earlier_ack) = oneshot::channel();
        tier_tx.try_send(earlier).unwrap();
        let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
        let mut pollers = spawn_tracker_tier_pollers(
            TrackerCtx {
                generation: generation.clone(),
                ..tracker_ctx(tx, stats, lifecycle, Some(tier_tx))
            },
            tiers,
            &[hash],
        );
        tokio::time::timeout(Duration::from_secs(5), lines.recv())
            .await
            .expect("backup tier announce")
            .expect("line");
        tokio::time::sleep(Duration::from_millis(200)).await;
        let delivered = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                tokio::select! {
                    biased;
                    Some(ack) = tier_rx.recv() => {
                        generation.bump();
                        let _ = ack.send(());
                    }
                    Some(envelope) = rx.recv() => {
                        if generation.is_current(envelope.generation) {
                            return envelope;
                        }
                    }
                }
            }
        })
        .await
        .expect("the backup tier's peers were tagged with a stale generation");
        assert_eq!(delivered.candidate.addr.port(), 6881);
        tokio::time::timeout(Duration::from_secs(10), pollers.shutdown(false))
            .await
            .unwrap();
        bad_server.abort();
        good_server.abort();
    }

    #[tokio::test]
    async fn shutdown_does_not_wait_for_an_unanswered_tier_change() {
        let (bad, _hits, bad_server) = failing_tracker().await;
        let (good, mut lines, good_server) =
            scripted_tracker(b"d8:intervali1800e5:peers6:\x7f\x00\x00\x01\x1a\xe1e").await;
        let tiers = vec![
            vec![format!("http://{bad}/announce")],
            vec![format!("http://{good}/announce")],
        ];
        let hash = Id20::new([1; 20]);
        let lifecycle = Arc::new(Mutex::new(TrackerLifecycle::default()));
        lifecycle
            .lock()
            .run_state(hash, &tiers)
            .last_successful_tier = Some(0);
        let (tx, _rx) = mpsc::channel(8);
        let (tier_tx, _tier_rx) = mpsc::channel(4);
        let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
        let mut pollers = spawn_tracker_tier_pollers(
            tracker_ctx(tx, stats, lifecycle, Some(tier_tx)),
            tiers,
            &[hash],
        );
        tokio::time::timeout(Duration::from_secs(5), lines.recv())
            .await
            .expect("backup tier announce")
            .expect("line");
        tokio::time::sleep(Duration::from_millis(200)).await;
        let started = Instant::now();
        tokio::time::timeout(Duration::from_secs(10), pollers.shutdown(false))
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(2));
        bad_server.abort();
        good_server.abort();
    }

    #[tokio::test]
    async fn trackers_are_marked_for_a_fresh_start_before_a_hung_stop_finishes() {
        let (addr, hung_task) = hung_tracker().await;
        let hash = Id20::new([1; 20]);
        let tiers = vec![vec![format!("http://{addr}/announce")]];
        let lifecycle = Arc::new(Mutex::new(TrackerLifecycle::default()));
        {
            let mut lifecycle = lifecycle.lock();
            let entry = &mut lifecycle.run_state(hash, &tiers).tiers[0][0];
            entry.started_sent = true;
            entry.next_at = Instant::now() + Duration::from_secs(1800);
        }
        let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
        let ctx = tracker_ctx(mpsc::channel(1).0, stats, Arc::clone(&lifecycle), None);
        let stopped = tokio::time::timeout(
            Duration::from_millis(300),
            announce_stopped(&ctx, &tiers, hash),
        )
        .await;
        assert!(stopped.is_err(), "the hung tracker must outlast the window");
        let mut lifecycle = lifecycle.lock();
        let entry = &lifecycle.run_state(hash, &tiers).tiers[0][0];
        assert!(!entry.started_sent);
        assert!(entry.next_at <= Instant::now());
        hung_task.abort();
    }

    #[tokio::test]
    async fn pollers_hold_their_first_announce_until_the_previous_stop_finishes() {
        for private in [false, true] {
            let (addr, mut lines, server) = recording_tracker().await;
            let (tx, _rx) = mpsc::channel(8);
            let (tier_tx, _tier_rx) = mpsc::channel(8);
            let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
            let lifecycle = Arc::new(Mutex::new(TrackerLifecycle::default()));
            let stop_done = lifecycle.lock().begin_stop();
            let mut pollers = spawn_tracker_tier_pollers(
                tracker_ctx(
                    tx,
                    stats,
                    Arc::clone(&lifecycle),
                    private.then_some(tier_tx),
                ),
                vec![vec![format!("http://{addr}/announce")]],
                &[Id20::new([1; 20])],
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(300), lines.recv())
                    .await
                    .is_err(),
                "announced while the previous stop was still running"
            );
            drop(stop_done);
            let line = tokio::time::timeout(Duration::from_secs(5), lines.recv())
                .await
                .expect("no announce after the stop finished")
                .expect("line");
            assert!(line.contains("event=started"), "{line}");
            tokio::time::timeout(Duration::from_secs(15), pollers.shutdown(false))
                .await
                .unwrap();
            server.abort();
        }
    }

    #[tokio::test]
    async fn shutdown_does_not_wait_for_a_pending_stop() {
        for private in [false, true] {
            let (addr, mut lines, server) = recording_tracker().await;
            let (tx, _rx) = mpsc::channel(8);
            let (tier_tx, _tier_rx) = mpsc::channel(8);
            let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
            let lifecycle = Arc::new(Mutex::new(TrackerLifecycle::default()));
            let _stop_done = lifecycle.lock().begin_stop();
            let mut pollers = spawn_tracker_tier_pollers(
                tracker_ctx(tx, stats, lifecycle, private.then_some(tier_tx)),
                vec![vec![format!("http://{addr}/announce")]],
                &[Id20::new([1; 20])],
            );
            tokio::time::timeout(Duration::from_secs(2), pollers.shutdown(true))
                .await
                .expect("pollers stayed stuck behind the pending stop");
            assert!(pollers.is_empty());
            assert!(lines.try_recv().is_err());
            server.abort();
        }
    }

    #[tokio::test]
    async fn a_working_backup_tracker_is_not_abandoned_when_the_primary_backoff_expires() {
        let (bad, hits, bad_server) = failing_tracker().await;
        let (good, mut lines, good_server) = recording_tracker().await;
        let tiers = vec![
            vec![format!("http://{bad}/announce")],
            vec![format!("http://{good}/announce")],
        ];
        let hash = Id20::new([1; 20]);
        let lifecycle = Arc::new(Mutex::new(TrackerLifecycle::default()));
        let (tx, _rx) = mpsc::channel(8);
        let (tier_tx, mut tier_rx) = mpsc::channel(4);
        let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
        let mut pollers = spawn_tracker_tier_pollers(
            tracker_ctx(tx, stats, Arc::clone(&lifecycle), Some(tier_tx)),
            tiers,
            &[hash],
        );
        tokio::time::timeout(Duration::from_secs(5), lines.recv())
            .await
            .expect("backup tier announce")
            .expect("line");
        wait_for_started(&lifecycle).await;
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        lifecycle.lock().retry_failed_now();
        assert!(
            tokio::time::timeout(Duration::from_millis(300), lines.recv())
                .await
                .is_err()
        );
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        tokio::time::timeout(Duration::from_secs(10), pollers.shutdown(false))
            .await
            .unwrap();
        assert!(tier_rx.try_recv().is_err());
        bad_server.abort();
        good_server.abort();
    }

    #[test]
    fn retry_failed_now_makes_failing_trackers_due_without_resetting_their_backoff() {
        let t0 = Instant::now();
        let secs = Duration::from_secs;
        let hash = Id20::new([7; 20]);
        let mut state = run_state_of(&[&["down", "told", "gone", "rej"]], t0);
        for _ in 0..12 {
            state.settle_tier_failure(0, "down", &failure(), t0);
        }
        for _ in 0..9 {
            state.settle_tier_failure(0, "rej", &TrackerError::rejected("no"), t0);
        }
        let told = TrackerError::Rejected {
            reason: "busy".into(),
            retry_in: Some(RetryIn::After(secs(2 * 3600))),
        };
        state.settle_tier_failure(0, "told", &told, t0);
        let never = TrackerError::Rejected {
            reason: "gone".into(),
            retry_in: Some(RetryIn::Never),
        };
        state.settle_tier_failure(0, "gone", &never, t0);
        assert_eq!(state.tiers[0][3].next_at, t0 + secs(3600));

        let mut lifecycle = TrackerLifecycle::default();
        lifecycle.by_info_hash.insert(hash, state);
        let wake = lifecycle.subscribe();
        lifecycle.retry_failed_now();
        assert!(wake.has_changed().unwrap());
        let state = lifecycle.by_info_hash.get_mut(&hash).unwrap();
        let now = Instant::now();
        assert!(state.tiers[0][0].next_at <= now);
        assert!(state.tiers[0][3].next_at <= now);
        assert_eq!(state.tiers[0][0].failures, 12);
        assert_eq!(state.tiers[0][3].failures, 9);
        assert_eq!(state.tiers[0][1].next_at, t0 + secs(2 * 3600));
        assert_eq!(state.pick(now), TrackerPick::Due { tier: 0, index: 0 });
        assert_eq!(
            state.settle_tier_failure(0, "down", &failure(), now),
            Some(secs(600))
        );
        assert_eq!(
            state.settle_tier_failure(0, "rej", &TrackerError::rejected("no"), now),
            Some(secs(3600))
        );
    }

    #[test]
    fn retry_failed_now_leaves_a_working_backup_tier_in_charge() {
        let t0 = Instant::now();
        let hash = Id20::new([8; 20]);
        let mut state = run_state_of(&[&["a"], &["b"]], t0);
        state.settle_tier_failure(0, "a", &failure(), t0);
        settle_ok(&mut state, 1, "b", t0);
        let renew = state.tiers[1][0].next_at;
        let mut lifecycle = TrackerLifecycle::default();
        lifecycle.by_info_hash.insert(hash, state);
        lifecycle.retry_failed_now();
        let state = &lifecycle.by_info_hash[&hash];
        assert!(state.tiers[0][0].next_at <= Instant::now());
        assert_eq!(state.tiers[1][0].next_at, renew);
        assert_eq!(state.pick(Instant::now()), TrackerPick::Idle(Some(renew)));
    }

    #[test]
    fn transport_failures_retry_within_ten_minutes_but_rejections_keep_backing_off() {
        let t0 = Instant::now();
        let secs = Duration::from_secs;
        let mut state = run_state_of(&[&["net", "rej"]], t0);
        let timeout = TrackerError::Timeout;
        let io = TrackerError::Io(std::io::Error::other("reset"));
        let http = failure();
        for failures in 1..=12 {
            let error = match failures % 3 {
                0 => &timeout,
                1 => &io,
                _ => &http,
            };
            state.settle_tier_failure(0, "net", error, t0);
            state.settle_tier_failure(0, "rej", &TrackerError::rejected("no"), t0);
        }
        assert_eq!(state.tiers[0][0].next_at, t0 + secs(600));
        assert_eq!(state.tiers[0][1].next_at, t0 + secs(3600));
        let mut fresh = run_state_of(&[&["net"]], t0);
        assert_eq!(
            fresh.settle_tier_failure(0, "net", &failure(), t0),
            Some(secs(15))
        );
    }

    #[test]
    fn a_tracker_told_to_wait_is_not_retried_early_by_retry_failed_now() {
        let t0 = Instant::now();
        let secs = Duration::from_secs;
        let hash = Id20::new([9; 20]);
        let mut state = run_state_of(&[&["told"]], t0);
        let told = TrackerError::Rejected {
            reason: "busy".into(),
            retry_in: Some(RetryIn::After(secs(600))),
        };
        state.settle_tier_failure(0, "told", &told, t0);
        let mut lifecycle = TrackerLifecycle::default();
        lifecycle.by_info_hash.insert(hash, state);
        lifecycle.retry_failed_now();
        assert_eq!(
            lifecycle.by_info_hash[&hash].tiers[0][0].next_at,
            t0 + secs(600)
        );
    }

    #[test]
    fn tier_pick_walks_in_list_order_and_stops_at_the_first_working_tracker() {
        let t0 = Instant::now();
        let mut state = run_state_of(&[&["a", "b"], &["c"]], t0);
        assert_eq!(state.pick(t0), TrackerPick::Due { tier: 0, index: 0 });
        assert!(!settle_ok(&mut state, 0, "a", t0));
        let renew = t0 + Duration::from_secs(1800);
        assert_eq!(state.pick(t0), TrackerPick::Idle(Some(renew)));
        assert_eq!(
            state.pick(t0 + Duration::from_secs(1799)),
            TrackerPick::Idle(Some(renew))
        );
        assert_eq!(state.pick(renew), TrackerPick::Due { tier: 0, index: 0 });
    }

    #[test]
    fn failed_trackers_fail_over_in_order_and_keep_their_backoff() {
        let t0 = Instant::now();
        let secs = Duration::from_secs;
        let mut state = run_state_of(&[&["a", "b"], &["c"]], t0);
        assert!(state.settle_tier_failure(0, "a", &failure(), t0).is_some());
        assert_eq!(state.pick(t0), TrackerPick::Due { tier: 0, index: 1 });
        state.settle_tier_failure(0, "b", &failure(), t0);
        assert_eq!(state.pick(t0), TrackerPick::Due { tier: 1, index: 0 });
        state.settle_tier_failure(1, "c", &failure(), t0);
        assert_eq!(state.pick(t0), TrackerPick::Idle(Some(t0 + secs(15))));
        assert_eq!(
            state.pick(t0 + secs(14)),
            TrackerPick::Idle(Some(t0 + secs(15)))
        );
        assert_eq!(
            state.pick(t0 + secs(15)),
            TrackerPick::Due { tier: 0, index: 0 }
        );
        let delay = state.settle_tier_failure(0, "a", &failure(), t0 + secs(15));
        assert_eq!(delay, Some(secs(30)));
        assert_eq!(
            state.pick(t0 + secs(15)),
            TrackerPick::Due { tier: 0, index: 1 }
        );
    }

    #[test]
    fn working_backup_tier_keeps_the_swarm_until_it_fails() {
        let t0 = Instant::now();
        let secs = Duration::from_secs;
        let mut state = run_state_of(&[&["a"], &["b"]], t0);
        state.settle_tier_failure(0, "a", &failure(), t0);
        assert_eq!(state.pick(t0), TrackerPick::Due { tier: 1, index: 0 });
        assert!(!settle_ok(&mut state, 1, "b", t0));
        let renew = t0 + secs(1800);
        assert_eq!(state.pick(t0 + secs(1)), TrackerPick::Idle(Some(renew)));
        assert_eq!(state.pick(t0 + secs(16)), TrackerPick::Idle(Some(renew)));
        assert_eq!(state.pick(renew), TrackerPick::Due { tier: 1, index: 0 });
        state.settle_tier_failure(1, "b", &failure(), renew);
        assert_eq!(state.pick(renew), TrackerPick::Due { tier: 0, index: 0 });
        assert!(settle_ok(&mut state, 0, "a", renew));
        assert!(!settle_ok(&mut state, 0, "a", renew + secs(2000)));
    }

    #[test]
    fn a_stopped_backup_tier_hands_the_walk_back_to_the_primary() {
        let t0 = Instant::now();
        let mut state = run_state_of(&[&["a"], &["b"]], t0);
        state.settle_tier_failure(0, "a", &failure(), t0);
        settle_ok(&mut state, 1, "b", t0);
        state.tiers[1][0].started_sent = false;
        assert_eq!(
            state.pick(t0 + Duration::from_secs(16)),
            TrackerPick::Due { tier: 0, index: 0 }
        );
    }

    #[test]
    fn promoted_tracker_leads_its_tier_after_it_answers() {
        let t0 = Instant::now();
        let mut state = run_state_of(&[&["a", "b"]], t0);
        state.settle_tier_failure(0, "a", &failure(), t0);
        settle_ok(&mut state, 0, "b", t0);
        let urls: Vec<_> = state.tiers[0].iter().map(|e| e.url.as_str()).collect();
        assert_eq!(urls, ["b", "a"]);
        assert!(matches!(
            state.pick(t0 + Duration::from_secs(20)),
            TrackerPick::Idle(Some(_))
        ));
    }

    #[test]
    fn retry_in_never_and_unsupported_schemes_disable_a_tracker() {
        let t0 = Instant::now();
        let secs = Duration::from_secs;
        let mut state = run_state_of(&[&["a", "b", "ws://c"]], t0);
        let never = TrackerError::Rejected {
            reason: "gone".into(),
            retry_in: Some(RetryIn::Never),
        };
        assert_eq!(state.settle_tier_failure(0, "a", &never, t0), None);
        assert!(state.tiers[0][0].disabled);
        assert_eq!(state.pick(t0), TrackerPick::Due { tier: 0, index: 1 });
        let hour = TrackerError::Rejected {
            reason: "later".into(),
            retry_in: Some(RetryIn::After(secs(2 * 3600))),
        };
        assert_eq!(
            state.settle_tier_failure(0, "b", &hour, t0),
            Some(secs(2 * 3600))
        );
        let short = TrackerError::Rejected {
            reason: "soon".into(),
            retry_in: Some(RetryIn::After(secs(1))),
        };
        assert_eq!(
            state.settle_tier_failure(0, "b", &short, t0),
            Some(secs(30))
        );
        let scheme = TrackerError::UnsupportedScheme("ws".into());
        assert_eq!(state.settle_tier_failure(0, "ws://c", &scheme, t0), None);
        assert_eq!(
            state.pick(t0 + secs(3600 * 3)),
            TrackerPick::Due { tier: 0, index: 1 }
        );
        state.settle_tier_failure(0, "b", &never, t0);
        assert_eq!(state.pick(t0 + secs(3600 * 3)), TrackerPick::Idle(None));
    }

    #[test]
    fn min_interval_floors_the_next_announce_and_a_forced_one() {
        let t0 = Instant::now();
        let secs = Duration::from_secs;
        let mut state = run_state_of(&[&["a"]], t0);
        let mut resp = ok_response(900);
        resp.min_interval = Some(secs(3600));
        state.settle_tier_success(0, "a", AnnounceEvent::Started, false, false, &resp, t0);
        assert_eq!(state.tiers[0][0].next_at, t0 + secs(3600));
        assert_eq!(state.tiers[0][0].min_next_at, t0 + secs(3600));

        let hash = Id20::new([5; 20]);
        let mut lifecycle = TrackerLifecycle::default();
        lifecycle.by_info_hash.insert(hash, state);
        lifecycle.announce_now(false);
        let entry = &lifecycle.by_info_hash[&hash].tiers[0][0];
        assert_eq!(entry.next_at, t0 + secs(3600));
        lifecycle.announce_now(true);
        let entry = &lifecycle.by_info_hash[&hash].tiers[0][0];
        assert!(entry.next_at <= Instant::now());
    }

    #[test]
    fn swarm_counts_are_the_largest_reported_by_any_tracker() {
        let t0 = Instant::now();
        let mut state = run_state_of(&[&["a", "b"]], t0);
        let mut resp = ok_response(900);
        resp.seeders = Some(4);
        resp.leechers = Some(9);
        state.tiers[0][0].record_success(AnnounceEvent::None, false, false, &resp, t0);
        resp.seeders = Some(7);
        resp.leechers = None;
        state.tiers[0][1].record_success(AnnounceEvent::None, false, false, &resp, t0);
        let mut lifecycle = TrackerLifecycle::default();
        assert_eq!(lifecycle.swarm_counts(), (None, None));
        lifecycle.by_info_hash.insert(Id20::new([6; 20]), state);
        assert_eq!(lifecycle.swarm_counts(), (Some(7), Some(9)));
    }

    #[tokio::test]
    async fn announced_swarm_counts_reach_the_torrent_stats() {
        for private in [false, true] {
            let (addr, mut lines, server) =
                scripted_tracker(b"d8:completei7e10:incompletei3e8:intervali1800e5:peers0:e").await;
            let (tx, _rx) = mpsc::channel(8);
            let (tier_tx, _tier_rx) = mpsc::channel(8);
            let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
            let mut pollers = spawn_tracker_tier_pollers(
                tracker_ctx(
                    tx,
                    Arc::clone(&stats),
                    Arc::new(Mutex::new(TrackerLifecycle::default())),
                    private.then_some(tier_tx),
                ),
                vec![vec![format!("http://{addr}/announce")]],
                &[Id20::new([1; 20])],
            );
            tokio::time::timeout(Duration::from_secs(5), lines.recv())
                .await
                .expect("announce")
                .expect("line");
            let deadline = Instant::now() + Duration::from_secs(5);
            while stats.lock().swarm_seeders.is_none() {
                assert!(Instant::now() < deadline, "swarm counts never published");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(stats.lock().swarm_seeders, Some(7));
            assert_eq!(stats.lock().swarm_leechers, Some(3));
            tokio::time::timeout(Duration::from_secs(10), pollers.shutdown(false))
                .await
                .unwrap();
            server.abort();
        }
    }

    #[tokio::test]
    async fn tracker_id_is_echoed_on_later_and_stopped_announces() {
        for private in [false, true] {
            let (addr, mut lines, server) =
                scripted_tracker(b"d8:intervali1800e10:tracker id3:abc5:peers0:e").await;
            let (tx, _rx) = mpsc::channel(8);
            let (tier_tx, _tier_rx) = mpsc::channel(8);
            let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
            let lifecycle = Arc::new(Mutex::new(TrackerLifecycle::default()));
            let mut pollers = spawn_tracker_tier_pollers(
                tracker_ctx(
                    tx,
                    Arc::clone(&stats),
                    Arc::clone(&lifecycle),
                    private.then_some(tier_tx),
                ),
                vec![vec![format!("http://{addr}/announce")]],
                &[Id20::new([1; 20])],
            );
            let first = tokio::time::timeout(Duration::from_secs(5), lines.recv())
                .await
                .expect("started announce")
                .expect("line");
            assert!(!first.contains("trackerid="), "{first}");
            tokio::time::sleep(Duration::from_millis(100)).await;
            lifecycle.lock().announce_now(false);
            let second = tokio::time::timeout(Duration::from_secs(5), lines.recv())
                .await
                .expect("follow-up announce")
                .expect("line");
            assert!(second.contains("trackerid=abc"), "{second}");
            tokio::time::timeout(Duration::from_secs(15), pollers.shutdown(true))
                .await
                .unwrap();
            let stopped = tokio::time::timeout(Duration::from_secs(5), lines.recv())
                .await
                .expect("stopped announce")
                .expect("line");
            assert!(stopped.contains("event=stopped"), "{stopped}");
            assert!(stopped.contains("trackerid=abc"), "{stopped}");
            server.abort();
        }
    }

    async fn wait_for_started(lifecycle: &Mutex<TrackerLifecycle>) {
        eventually("the announce to settle", || {
            lifecycle
                .lock()
                .by_info_hash
                .values()
                .flat_map(|state| state.tiers.iter().flatten())
                .any(|entry| entry.started_sent)
        })
        .await;
    }

    #[tokio::test]
    async fn stopped_tracker_is_announced_started_again_by_the_next_poller_set() {
        for private in [false, true] {
            let (addr, mut lines, server) = recording_tracker().await;
            let lifecycle = Arc::new(Mutex::new(TrackerLifecycle::default()));
            let stats = Arc::new(Mutex::new(TorrentStats::initial(0, 0, vec![])));
            let tiers = vec![vec![format!("http://{addr}/announce")]];
            let spawn = |tiers: Vec<Vec<String>>| {
                let (tx, _rx) = mpsc::channel(8);
                let (tier_tx, _tier_rx) = mpsc::channel(8);
                spawn_tracker_tier_pollers(
                    tracker_ctx(
                        tx,
                        Arc::clone(&stats),
                        Arc::clone(&lifecycle),
                        private.then_some(tier_tx),
                    ),
                    tiers,
                    &[Id20::new([1; 20])],
                )
            };
            async fn next_line(lines: &mut mpsc::UnboundedReceiver<String>) -> String {
                tokio::time::timeout(Duration::from_secs(5), lines.recv())
                    .await
                    .expect("announce")
                    .expect("line")
            }
            let mut pollers = spawn(tiers.clone());
            assert!(next_line(&mut lines).await.contains("event=started"));
            wait_for_started(&lifecycle).await;
            tokio::time::timeout(Duration::from_secs(15), pollers.shutdown(true))
                .await
                .unwrap();
            assert!(next_line(&mut lines).await.contains("event=stopped"));
            let mut resumed = spawn(tiers);
            assert!(next_line(&mut lines).await.contains("event=started"));
            wait_for_started(&lifecycle).await;
            tokio::time::timeout(Duration::from_secs(15), resumed.shutdown(false))
                .await
                .unwrap();
            server.abort();
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
            holepunch: true,
            listen_port: 6881,
            metadata_size: 100,
            private: false,
            prefers_encryption: true,
        };
        let ip = IpAddr::from([192, 0, 2, 1]);
        assert_eq!(
            ExtHandshake::decode(&ext.build(ip, None).encode())
                .unwrap()
                .upload_only,
            None
        );
        assert_eq!(
            ExtHandshake::decode(&ext.build(ip, Some(true)).encode())
                .unwrap()
                .upload_only,
            Some(true)
        );
    }

    fn ext_handshake_from(entries: Vec<(&str, i64)>, flags: Vec<(&str, i64)>) -> ExtHandshake {
        use crate::bencode::{encode_to_vec, Value};
        let m = entries
            .into_iter()
            .map(|(name, id)| (name.as_bytes().to_vec(), Value::Int(id)))
            .collect();
        let mut dict = vec![(b"m".to_vec(), Value::Dict(m))];
        dict.extend(
            flags
                .into_iter()
                .map(|(key, value)| (key.as_bytes().to_vec(), Value::Int(value))),
        );
        ExtHandshake::decode(&encode_to_vec(&Value::Dict(dict))).unwrap()
    }

    #[test]
    fn follow_up_extended_handshake_only_changes_what_it_mentions() {
        let (tx, _rx) = mpsc::channel(1);
        let mut peer = Peer::connected(test_peer(52_400), tx, 1, 1, 1, true, true, None);
        let full = ext_handshake_from(
            vec![("ut_pex", 4), ("ut_metadata", 2), ("ut_holepunch", 7)],
            vec![("e", 1)],
        );
        merge_ext_handshake(&mut peer, &full);
        assert_eq!(
            (
                peer.their_ut_pex_id,
                peer.their_ut_metadata_id,
                peer.their_ut_holepunch_id
            ),
            (Some(4), Some(2), Some(7))
        );
        assert!(peer.prefers_encryption && !peer.upload_only);

        merge_ext_handshake(
            &mut peer,
            &ext_handshake_from(vec![], vec![("upload_only", 1)]),
        );
        assert_eq!(
            (
                peer.their_ut_pex_id,
                peer.their_ut_metadata_id,
                peer.their_ut_holepunch_id
            ),
            (Some(4), Some(2), Some(7))
        );
        assert!(peer.prefers_encryption && peer.upload_only);

        merge_ext_handshake(&mut peer, &ext_handshake_from(vec![("ut_pex", 0)], vec![]));
        assert_eq!(
            (
                peer.their_ut_pex_id,
                peer.their_ut_metadata_id,
                peer.their_ut_holepunch_id
            ),
            (None, Some(2), Some(7))
        );

        merge_ext_handshake(
            &mut peer,
            &ext_handshake_from(vec![("ut_pex", 9)], vec![("upload_only", 0)]),
        );
        assert_eq!(peer.their_ut_pex_id, Some(9));
        assert!(!peer.upload_only);
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
                    executable: false,
                },
                TorrentMetaInfo {
                    path: vec!["second.bin".into()],
                    length: 7,
                    padding: false,
                    executable: false,
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
                executable: false,
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
    async fn webseed_mirror_survives_a_changing_etag() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        tokio::time::timeout(Duration::from_secs(20), async {
            let payload = b"abcdefgh";
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
                    executable: false,
                }],
                single_file_mode: true,
            });
            let tmp = tempfile::tempdir().unwrap();
            let storage = Arc::new(FilesystemStorage::new(&info, tmp.path()));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                for (round, etag) in ["\"node-a\"", "\"node-b\""].into_iter().enumerate() {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                        let read = socket.read(&mut buf).await.unwrap();
                        assert_ne!(read, 0);
                        request.extend_from_slice(&buf[..read]);
                    }
                    let (start, end) = if round == 0 { (0, 3) } else { (4, 7) };
                    let reply = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: 4\r\nContent-Range: bytes {start}-{end}/8\r\nETag: {etag}\r\nConnection: close\r\n\r\n"
                    );
                    socket.write_all(reply.as_bytes()).await.unwrap();
                    socket.write_all(&payload[start..=end]).await.unwrap();
                }
            });
            let ctx = WebSeedContext {
                client: build_webseed_client(None).unwrap(),
                bases: Arc::new(vec![format!("http://{addr}/")]),
                disabled: Arc::new(Mutex::new(HashSet::new())),
                disabled_files: Arc::new(Mutex::new(HashSet::new())),
                backoff: Arc::new(Mutex::new(HashMap::new())),
                layout: storage.layout().clone(),
                info,
                storage: Arc::clone(&storage),
                verifier: PieceVerifier::V1Sha1 {
                    pieces: Arc::new(piece_hashes),
                },
                lengths: Lengths::new(payload.len() as u64, 4).unwrap(),
                generation: GenerationToken::new(),
                throttle: Throttle::unlimited(),
            };

            let first = fetch_webseed_batch(ctx.clone(), vec![0]).await;
            let second = fetch_webseed_batch(ctx.clone(), vec![1]).await;

            assert!(first.pieces.iter().all(|piece| piece.verify_ok));
            assert!(second.pieces.iter().all(|piece| piece.verify_ok));
            assert_eq!(second.pieces.len(), 1);
            assert!(ctx.mirror_usable(0));
            server.await.unwrap();
        })
        .await
        .expect("webseed ETag test timed out");
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
            executable: false,
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
                    executable: false,
                },
                TorrentMetaInfo {
                    path: vec!["b".into()],
                    length: 15,
                    padding: false,
                    executable: false,
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

    async fn dials_back_a_utp_stranger_turned_away_while_paused(private: bool) -> bool {
        use crate::bencode::{encode_to_vec, Value};
        use crate::session::{AddTorrentOptions, AddTorrentResponse, Session, SessionOptions};

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
        let mut info = vec![
            (b"length".to_vec(), Value::Int(1024)),
            (b"name".to_vec(), Value::Bytes(b"stranger.bin".to_vec())),
            (b"piece length".to_vec(), Value::Int(1024)),
            (b"pieces".to_vec(), Value::Bytes(vec![0u8; 20])),
        ];
        if private {
            info.push((b"private".to_vec(), Value::Int(1)));
        }
        let bytes = encode_to_vec(&Value::Dict(vec![(b"info".to_vec(), Value::Dict(info))]));
        let AddTorrentResponse::Added(_, handle) = session
            .add_from_meta(
                crate::parse_torrent(&bytes).unwrap(),
                AddTorrentOptions::default(),
            )
            .await
            .unwrap()
        else {
            panic!("expected Added");
        };
        session.pause(&handle).await.unwrap();

        let stranger = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (cmd_tx, _cmd_rx) = mpsc::channel(8);
        let (piece_tx, _piece_rx) = mpsc::channel(8);
        let (bind, _bind_rx) = oneshot::channel();
        handle
            .cmd_tx()
            .send(TorrentCommand::AddInboundPeer {
                addr: stranger.local_addr().unwrap(),
                cmd_tx,
                piece_tx,
                bind,
                reserved: [0; 8],
                peer_id: Id20::new([9; 20]),
                io_abort: tokio::spawn(async {}).abort_handle(),
                encrypted: false,
                utp: true,
            })
            .await
            .unwrap();
        session.unpause(&handle).await.unwrap();

        let dialled = tokio::time::timeout(
            if private {
                Duration::from_secs(1)
            } else {
                Duration::from_secs(10)
            },
            stranger.accept(),
        )
        .await
        .is_ok();
        session.shutdown().await;
        dialled
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn public_torrents_dial_back_a_utp_peer_turned_away_while_paused() {
        assert!(dials_back_a_utp_stranger_turned_away_while_paused(false).await);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn private_torrents_never_dial_an_unlisted_utp_peer_turned_away_while_paused() {
        assert!(!dials_back_a_utp_stranger_turned_away_while_paused(true).await);
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
            live: LiveUpload {
                pending: pending.clone(),
                bytes: 16384,
            },
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

    #[tokio::test(start_paused = true)]
    async fn tiny_piece_messages_are_charged_for_their_framing() {
        let piece = 64 * 1024;
        let payload = vec![7u8; piece];
        let (_tmp, storage, pending, tx, mut rx) = upload_fixture(piece, &payload).await;
        let stats = Arc::new(Mutex::new(TorrentStats::initial(piece as u64, 0, vec![])));
        let global = Arc::new(RateLimiter::new(100));
        let limiter = Throttle::new(global, Arc::new(RateLimiter::unlimited()));
        let started = tokio::time::Instant::now();
        let requests = 30u32;
        tokio::time::timeout(Duration::from_secs(120), async {
            for begin in 0..requests {
                let key = (0, begin, 1);
                pending.lock().insert(key);
                serve_upload(UploadJob {
                    pending: pending.clone(),
                    storage: storage.clone(),
                    lane: UploadLane::Piece(tx.clone()),
                    stats: stats.clone(),
                    upload_tick: Arc::new(AtomicU64::new(0)),
                    peer_uploaded: Arc::new(AtomicU64::new(0)),
                    peer_uploaded_total: Arc::new(AtomicU64::new(0)),
                    limiter: limiter.clone(),
                    supports_fast: true,
                    key,
                    offset: u64::from(begin),
                    extent: None,
                    live: LiveUpload {
                        pending: pending.clone(),
                        bytes: 1,
                    },
                })
                .await;
                assert!(matches!(rx.recv().await, Some(Message::Piece { .. })));
            }
        })
        .await
        .expect("uploads did not finish");
        assert_eq!(stats.lock().uploaded_bytes, u64::from(requests));
        assert!(
            started.elapsed() >= Duration::from_secs(15),
            "{requests} one-byte pieces went through a 100 B/s cap in {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn piece_frames_are_charged_their_header_and_a_wire_floor() {
        assert_eq!(piece_frame_cost(1), MIN_PIECE_FRAME_COST);
        assert_eq!(piece_frame_cost(0), MIN_PIECE_FRAME_COST);
        assert_eq!(
            piece_frame_cost(MIN_PIECE_FRAME_COST),
            MIN_PIECE_FRAME_COST + PIECE_FRAME_OVERHEAD
        );
        assert_eq!(piece_frame_cost(16384), 16384 + PIECE_FRAME_OVERHEAD);
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

    #[tokio::test]
    async fn cancelled_uploads_keep_their_slot_while_the_parked_job_lives() {
        let piece = 64 * 1024;
        let payload = vec![7u8; piece];
        let (_tmp, storage, _pending, _tx, _rx) = upload_fixture(piece, &payload).await;
        let lengths = Lengths::new(piece as u64, piece as u32).unwrap();
        let mut piece_tracker = PieceTracker::new(lengths);
        piece_tracker.set_local(lengths.validate_piece(0).unwrap(), true);
        let (cmd_tx, _cmd_rx) = mpsc::channel(8192);
        let addr = "127.0.0.1:52400".parse().unwrap();
        let mut peer = Peer::connected(addr, cmd_tx, 1, 4, 8, true, true, None);
        peer.am_choking = false;
        let stats = Arc::new(Mutex::new(TorrentStats::initial(piece as u64, 0, vec![])));
        let (piece_tx, piece_rx) = mpsc::channel(1);
        assert!(piece_tx.try_send(Message::KeepAlive).is_ok());
        let mut admitted = 0;
        for begin in 0..MAX_PENDING_UPLOAD_REQUESTS as u32 * 4 {
            let key = (0, begin, 1);
            if let Some((offset, _, live)) =
                admit_upload_request(&mut peer, &piece_tracker, &lengths, key)
            {
                admitted += 1;
                tokio::spawn(serve_upload(UploadJob {
                    pending: Arc::clone(&peer.pending_uploads),
                    storage: storage.clone(),
                    lane: UploadLane::Piece(piece_tx.clone()),
                    stats: stats.clone(),
                    upload_tick: Arc::new(AtomicU64::new(0)),
                    peer_uploaded: Arc::new(AtomicU64::new(0)),
                    peer_uploaded_total: Arc::new(AtomicU64::new(0)),
                    limiter: Throttle::unlimited(),
                    supports_fast: true,
                    key,
                    offset: offset + u64::from(begin),
                    extent: None,
                    live,
                }));
                tokio::task::yield_now().await;
            }
            cancel_upload_request(&mut peer, key);
        }
        assert_eq!(admitted, MAX_PENDING_UPLOAD_REQUESTS);
        assert_eq!(
            peer.pending_uploads.lock().live,
            MAX_PENDING_UPLOAD_REQUESTS
        );
        drop(piece_rx);
        tokio::time::timeout(Duration::from_secs(5), async {
            while peer.pending_uploads.lock().live != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("parked jobs end once the lane closes");
        assert_eq!(peer.pending_uploads.lock().live_bytes, 0);
    }
}
