use std::collections::HashMap;
use std::net::SocketAddrV4;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::{interval, Duration, Instant};
use tokio_util::sync::CancellationToken;

use risuko_util::limiter::Throttle;

use super::chunks::{hash_part_from_disk, write_block, ChunkManager};
use super::kad::{routing::is_public_ipv4, KadLookupStatus, KadService, KadState};
use super::partial;
use super::peer::{PeerConnection, PeerEvent};
use super::protocol::{new_user_hash, PeerCaps};
use super::server::{ServerConnection, ServerEvent};
use super::server_list::server_list;
use super::share::{PartMap, ShareContext, SharedFile};
use super::types::*;

const MAX_ACTIVE_PEER_DIALS: usize = 64;
const MAX_PENDING_PEER_SOURCES: usize = 300;
const SERVER_REASK_INTERVAL: Duration = Duration::from_secs(800);
const SERVER_FALLBACK_WAIT: Duration = Duration::from_secs(30);
const KAD_FALLBACK_WAIT: Duration = Duration::from_secs(45);
const PEER_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(20);
const PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const PEER_QUEUE_TIMEOUT: Duration = Duration::from_secs(180);
const PEER_TRANSFER_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_PEER_REJECTED_PACKETS: u32 = 256;
const MAX_SLOT_RETRIES: u32 = 32;
const FILE_REASK_INTERVAL: Duration = Duration::from_secs(29 * 60);
const FILE_REASK_JITTER_SECS: u64 = 120;
const REASK_BACKOFF: Duration = Duration::from_secs(30);
const SOURCE_RETRY_BASE: Duration = Duration::from_secs(2 * 60);
const SOURCE_RETRY_MAX: Duration = Duration::from_secs(30 * 60);
const MAX_SOURCE_ATTEMPTS: u32 = 5;
const MAX_TRACKED_SOURCES: usize = 4096;
const KAD_RELOOKUP_INTERVAL: Duration = Duration::from_secs(30 * 60);
const KAD_IDLE_RELOOKUP_DELAY: Duration = Duration::from_secs(5 * 60);
const KAD_IDLE_CHECK_INTERVAL: Duration = Duration::from_secs(15);
const BAD_HASHSET_ERROR: &str = "peer sent a hashset that does not match the file hash";
const UNREQUESTED_DATA_ERROR: &str = "peer keeps sending data that was not requested";
const CORRUPT_DATA_ERROR: &str = "peer served corrupt data";
const MISCONDUCT_ERRORS: [&str; 3] = [
    BAD_HASHSET_ERROR,
    UNREQUESTED_DATA_ERROR,
    CORRUPT_DATA_ERROR,
];

#[derive(Debug, Clone, Copy)]
pub(crate) struct PeerTimeouts {
    pub(crate) handshake: Duration,
    pub(crate) request: Duration,
    pub(crate) queue: Duration,
    pub(crate) transfer: Duration,
}

impl Default for PeerTimeouts {
    fn default() -> Self {
        Self {
            handshake: PEER_HANDSHAKE_TIMEOUT,
            request: PEER_REQUEST_TIMEOUT,
            queue: PEER_QUEUE_TIMEOUT,
            transfer: PEER_TRANSFER_TIMEOUT,
        }
    }
}

static NEXT_PEER_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceOrigin {
    Link,
    Server,
    Kad,
}

#[derive(Debug, Clone, Copy)]
struct SourceCandidate {
    addr: SocketAddrV4,
    client_id: u32,
    server_ip: u32,
    server_port: u16,
    origin: SourceOrigin,
}

type CandidateDispatcher =
    Arc<dyn Fn(SourceCandidate, OwnedSemaphorePermit) + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy)]
struct QueuedSource {
    candidate: SourceCandidate,
    rank: u16,
    due: Instant,
}

struct QueuedSources {
    sources: parking_lot::Mutex<HashMap<SocketAddrV4, QueuedSource>>,
    limit: usize,
    changed: Notify,
    wake: Arc<Notify>,
}

impl QueuedSources {
    fn new(limit: usize, wake: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            sources: parking_lot::Mutex::new(HashMap::new()),
            limit,
            changed: Notify::new(),
            wake,
        })
    }

    fn add(&self, candidate: SourceCandidate, rank: u16, delay: Duration) -> bool {
        {
            let mut sources = self.sources.lock();
            if sources.len() >= self.limit && !sources.contains_key(&candidate.addr) {
                return false;
            }
            sources.insert(
                candidate.addr,
                QueuedSource {
                    candidate,
                    rank,
                    due: Instant::now() + delay,
                },
            );
        }
        self.changed.notify_one();
        self.wake.notify_one();
        true
    }

    fn is_empty(&self) -> bool {
        self.sources.lock().is_empty()
    }

    fn next_due(&self) -> Option<Instant> {
        self.sources.lock().values().map(|s| s.due).min()
    }

    fn take_due(&self, now: Instant, pending: &AtomicU32) -> Vec<QueuedSource> {
        let mut sources = self.sources.lock();
        let due: Vec<SocketAddrV4> = sources
            .iter()
            .filter(|(_, s)| s.due <= now)
            .map(|(addr, _)| *addr)
            .collect();
        let taken: Vec<QueuedSource> = due.iter().filter_map(|addr| sources.remove(addr)).collect();
        pending.fetch_add(taken.len() as u32, Ordering::Relaxed);
        taken
    }

    fn postpone(&self, mut source: QueuedSource, delay: Duration) {
        source.due = Instant::now() + delay;
        self.sources.lock().insert(source.candidate.addr, source);
    }
}

const VERIFY_RECHECK_DELAY: Duration = Duration::from_secs(30);
const SERVER_LOGIN_TIMEOUT: Duration = Duration::from_secs(30);

fn reask_delay() -> Duration {
    use rand::RngExt;
    FILE_REASK_INTERVAL + Duration::from_secs(rand::rng().random_range(0..=FILE_REASK_JITTER_SECS))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SourceState {
    Live { failures: u32 },
    Failed { failures: u32, retry_at: Instant },
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionEnd {
    Failed,
    Done,
    Requeued,
}

fn source_retry_delay(failures: u32) -> Duration {
    let doublings = failures.saturating_sub(1).min(5);
    (SOURCE_RETRY_BASE * (1 << doublings)).min(SOURCE_RETRY_MAX)
}

struct SourceBook {
    states: HashMap<SocketAddrV4, SourceState>,
    limit: usize,
}

impl SourceBook {
    fn new(limit: usize) -> Self {
        Self {
            states: HashMap::new(),
            limit,
        }
    }

    fn admit(&mut self, addr: SocketAddrV4, now: Instant) -> bool {
        let failures = match self.states.get(&addr).copied() {
            None => {
                if self.states.len() >= self.limit && !self.evict_one() {
                    return false;
                }
                0
            }
            Some(SourceState::Failed { failures, retry_at })
                if failures < MAX_SOURCE_ATTEMPTS && now >= retry_at =>
            {
                failures
            }
            Some(_) => return false,
        };
        self.states.insert(addr, SourceState::Live { failures });
        true
    }

    fn record_end(&mut self, addr: SocketAddrV4, end: SessionEnd, now: Instant) {
        let Some(state) = self.states.get_mut(&addr) else {
            return;
        };
        *state = match end {
            SessionEnd::Requeued => SourceState::Live { failures: 0 },
            SessionEnd::Done => SourceState::Done,
            SessionEnd::Failed => {
                let failures = match *state {
                    SourceState::Live { failures } | SourceState::Failed { failures, .. } => {
                        failures.saturating_add(1)
                    }
                    SourceState::Done => 1,
                };
                SourceState::Failed {
                    failures,
                    retry_at: now + source_retry_delay(failures),
                }
            }
        };
    }

    fn evict_one(&mut self) -> bool {
        let victim = self
            .states
            .iter()
            .filter_map(|(addr, state)| match *state {
                SourceState::Live { .. } => None,
                SourceState::Done => Some(((false, false, None), *addr)),
                SourceState::Failed { failures, retry_at } => Some((
                    (true, failures >= MAX_SOURCE_ATTEMPTS, Some(retry_at)),
                    *addr,
                )),
            })
            .max_by_key(|(rank, _)| *rank)
            .map(|(_, addr)| addr);
        victim.is_some_and(|addr| self.states.remove(&addr).is_some())
    }
}

fn kad_relookup_due(finished: Instant, idle_since: Option<Instant>, now: Instant) -> bool {
    now >= finished + KAD_RELOOKUP_INTERVAL
        || idle_since.is_some_and(|since| now >= since.max(finished) + KAD_IDLE_RELOOKUP_DELAY)
}

struct SourceScheduler {
    sources: Arc<parking_lot::Mutex<SourceBook>>,
    queued: Arc<QueuedSources>,
    candidates: mpsc::Sender<SourceCandidate>,
    cancel_token: CancellationToken,
    pending: Arc<AtomicU32>,
    wake: Arc<Notify>,
}

struct DownloadWorkerGuard(CancellationToken);

impl Drop for DownloadWorkerGuard {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

async fn finish_kad_lookup(
    cancel: &CancellationToken,
    lookup_task: &mut Option<tokio::task::JoinHandle<()>>,
) {
    cancel.cancel();
    if let Some(task) = lookup_task.take() {
        let _ = task.await;
    }
}

fn fallback_wait(kad_lookup_running: bool) -> Duration {
    if kad_lookup_running {
        KAD_FALLBACK_WAIT
    } else {
        SERVER_FALLBACK_WAIT
    }
}

async fn collect_kad_sources(
    mut sources: mpsc::Receiver<super::kad::KadSource>,
    mut status: tokio::sync::watch::Receiver<KadLookupStatus>,
    completion: tokio::task::JoinHandle<Result<(), super::kad::KadError>>,
    scheduler: Arc<SourceScheduler>,
    kad_status: Arc<parking_lot::Mutex<Option<KadLookupStatus>>>,
    local_client_hash: [u8; 16],
) {
    let mut sources_open = true;
    let mut status_open = true;
    while sources_open || status_open {
        tokio::select! {
            source = sources.recv(), if sources_open => match source {
                Some(source) => {
                    if source.client_hash == local_client_hash {
                        continue;
                    }
                    scheduler.submit(SourceCandidate {
                        addr: source.addr,
                        client_id: 0,
                        server_ip: 0,
                        server_port: 0,
                        origin: SourceOrigin::Kad,
                    });
                }
                None => sources_open = false,
            },
            changed = status.changed(), if status_open => {
                if changed.is_ok() {
                    *kad_status.lock() = Some(status.borrow().clone());
                } else {
                    status_open = false;
                }
            }
        }
    }
    let _ = completion.await;
    *kad_status.lock() = Some(status.borrow().clone());
}

async fn repeat_kad_lookups<L, F>(
    mut lookup: L,
    is_idle: impl Fn() -> bool,
    running: &AtomicBool,
    cancel: &CancellationToken,
) where
    L: FnMut() -> F,
    F: std::future::Future<Output = ()>,
{
    while !cancel.is_cancelled() {
        running.store(true, Ordering::Relaxed);
        lookup().await;
        running.store(false, Ordering::Relaxed);
        let finished = Instant::now();
        let mut idle_since = None;
        let mut check = interval(KAD_IDLE_CHECK_INTERVAL);
        check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = check.tick() => {}
            }
            let now = Instant::now();
            idle_since = if is_idle() {
                idle_since.or(Some(now))
            } else {
                None
            };
            if kad_relookup_due(finished, idle_since, now) {
                break;
            }
        }
    }
}

fn kad_lookup_active(task: &Option<tokio::task::JoinHandle<()>>, running: &AtomicBool) -> bool {
    task.as_ref().is_some_and(|task| !task.is_finished()) && running.load(Ordering::Relaxed)
}

impl SourceScheduler {
    fn new(
        client_hash: [u8; 16],
        client_port: u16,
        file_hash: [u8; 16],
        chunks: Arc<Mutex<ChunkManager>>,
        completed: Arc<AtomicU64>,
        cancel_token: CancellationToken,
        peer_count: Arc<AtomicU32>,
        proxy: risuko_http::ProxyConnector,
        throttle: Throttle,
    ) -> Arc<Self> {
        let peer_cancel = cancel_token.clone();
        let peer_proxy = proxy.clone();
        let wake = Arc::new(Notify::new());
        let peer_wake = wake.clone();
        let queued = QueuedSources::new(MAX_PENDING_PEER_SOURCES, wake.clone());
        let peer_queued = queued.clone();
        let sources = Arc::new(parking_lot::Mutex::new(SourceBook::new(
            MAX_TRACKED_SOURCES,
        )));
        let peer_sources = sources.clone();
        let dispatcher: CandidateDispatcher = Arc::new(move |candidate, permit| {
            spawn_peer_task_with_permit(
                candidate,
                peer_queued.clone(),
                peer_sources.clone(),
                candidate.addr,
                client_hash,
                candidate.client_id,
                client_port,
                candidate.server_ip,
                candidate.server_port,
                file_hash,
                chunks.clone(),
                completed.clone(),
                peer_cancel.clone(),
                peer_count.clone(),
                permit,
                peer_proxy.clone(),
                throttle.clone(),
                peer_wake.clone(),
            );
        });
        Self::with_dispatcher_and_wake(
            cancel_token,
            MAX_PENDING_PEER_SOURCES,
            MAX_ACTIVE_PEER_DIALS,
            dispatcher,
            wake,
            queued,
            sources,
        )
    }

    #[cfg(test)]
    fn with_dispatcher(
        cancel_token: CancellationToken,
        queue_capacity: usize,
        max_active_dials: usize,
        dispatcher: CandidateDispatcher,
    ) -> Arc<Self> {
        let wake = Arc::new(Notify::new());
        let queued = QueuedSources::new(queue_capacity, wake.clone());
        Self::with_dispatcher_and_wake(
            cancel_token,
            queue_capacity,
            max_active_dials,
            dispatcher,
            wake,
            queued,
            Arc::new(parking_lot::Mutex::new(SourceBook::new(
                MAX_TRACKED_SOURCES,
            ))),
        )
    }

    fn with_dispatcher_and_wake(
        cancel_token: CancellationToken,
        queue_capacity: usize,
        max_active_dials: usize,
        dispatcher: CandidateDispatcher,
        wake: Arc<Notify>,
        queued: Arc<QueuedSources>,
        sources: Arc<parking_lot::Mutex<SourceBook>>,
    ) -> Arc<Self> {
        let permits = Arc::new(Semaphore::new(max_active_dials));
        let (candidates, mut pending_candidates) = mpsc::channel::<SourceCandidate>(queue_capacity);
        let dispatcher_permits = permits.clone();
        let dispatcher_cancel = cancel_token.clone();
        let pending = Arc::new(AtomicU32::new(0));
        let dispatcher_pending = pending.clone();
        let dispatcher_wake = wake.clone();
        tokio::spawn(async move {
            loop {
                let candidate = tokio::select! {
                    _ = dispatcher_cancel.cancelled() => break,
                    candidate = pending_candidates.recv() => candidate,
                };
                let Some(candidate) = candidate else {
                    break;
                };
                let permit = tokio::select! {
                    _ = dispatcher_cancel.cancelled() => {
                        dispatcher_pending.fetch_sub(1, Ordering::Relaxed);
                        break;
                    }
                    permit = dispatcher_permits.clone().acquire_owned() => match permit {
                        Ok(permit) => permit,
                        Err(_) => {
                            dispatcher_pending.fetch_sub(1, Ordering::Relaxed);
                            break;
                        }
                    },
                };
                if dispatcher_cancel.is_cancelled() {
                    drop(permit);
                    dispatcher_pending.fetch_sub(1, Ordering::Relaxed);
                    break;
                }
                dispatcher_pending.fetch_sub(1, Ordering::Relaxed);
                dispatcher(candidate, permit);
                dispatcher_wake.notify_one();
            }
        });

        let reask_candidates = candidates.clone();
        let reask_queued = queued.clone();
        let reask_cancel = cancel_token.clone();
        let reask_pending = pending.clone();
        tokio::spawn(async move {
            loop {
                let next = reask_queued.next_due();
                tokio::select! {
                    _ = reask_cancel.cancelled() => break,
                    _ = reask_queued.changed.notified() => continue,
                    _ = async {
                        match next {
                            Some(due) => tokio::time::sleep_until(due).await,
                            None => std::future::pending().await,
                        }
                    } => {}
                }
                for source in reask_queued.take_due(Instant::now(), &reask_pending) {
                    if reask_candidates.try_send(source.candidate).is_err() {
                        reask_queued.postpone(source, REASK_BACKOFF);
                        reask_pending.fetch_sub(1, Ordering::Relaxed);
                    } else {
                        tracing::debug!(
                            "[ed2k] Re-asking queued source {} (last rank {})",
                            source.candidate.addr,
                            source.rank
                        );
                    }
                }
            }
        });

        Arc::new(Self {
            sources,
            queued,
            candidates,
            cancel_token,
            pending,
            wake,
        })
    }

    fn submit(&self, candidate: SourceCandidate) -> bool {
        self.submit_at(candidate, Instant::now())
    }

    fn submit_at(&self, candidate: SourceCandidate, now: Instant) -> bool {
        let usable_addr = if candidate.origin == SourceOrigin::Kad {
            usable_source_addr(candidate.addr)
        } else {
            candidate.addr.port() != 0
        };
        if self.cancel_token.is_cancelled() || !usable_addr {
            return false;
        }
        let Ok(slot) = self.candidates.try_reserve() else {
            return false;
        };
        if !self.sources.lock().admit(candidate.addr, now) {
            return false;
        }
        self.pending.fetch_add(1, Ordering::Relaxed);
        slot.send(candidate);
        true
    }

    fn is_idle(&self) -> bool {
        self.queued.is_empty() && self.pending.load(Ordering::Relaxed) == 0
    }
}

fn usable_source_addr(addr: SocketAddrV4) -> bool {
    addr.port() != 0 && is_public_ipv4(*addr.ip())
}

fn is_foreign_high_id_source(ip: u32, port: u16, own_id: u32, own_port: u16) -> bool {
    is_high_id(ip) && (ip, port) != (own_id, own_port)
}

#[derive(Debug)]
pub struct Ed2kCompleted {
    pub path: PathBuf,
    pub hashset: Vec<[u8; 16]>,
    pub claim: partial::PathClaim,
}

pub async fn run_ed2k_download_with_proxy(
    file_link: &Ed2kFileLink,
    target: partial::PathClaim,
    auto_rename: bool,
    ed2k_servers: Vec<String>,
    client_port: u16,
    kad_udp_port: Option<u16>,
    kad: Option<Arc<KadService>>,
    kad_status: Arc<parking_lot::Mutex<Option<KadLookupStatus>>>,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
    cancel_token: CancellationToken,
    proxy: risuko_http::ProxyConnector,
    throttle: Throttle,
    share: Option<ShareContext>,
) -> Result<Ed2kCompleted, String> {
    let part_map = PartMap::new(file_link.file_size);
    run_download(
        file_link,
        target.path(),
        ed2k_servers,
        client_port,
        kad_udp_port,
        kad,
        kad_status,
        total,
        completed,
        speed,
        connections,
        cancel_token,
        proxy,
        throttle,
        share,
        part_map.clone(),
    )
    .await?;
    let hashset = part_map.hashset().unwrap_or_default();
    let claim = tokio::task::spawn_blocking(move || partial::finalize(target, auto_rename))
        .await
        .map_err(|e| format!("Finalize task failed: {e}"))??;
    Ok(Ed2kCompleted {
        path: claim.path().to_path_buf(),
        hashset,
        claim,
    })
}

async fn run_download(
    file_link: &Ed2kFileLink,
    final_path: &Path,
    ed2k_servers: Vec<String>,
    client_port: u16,
    kad_udp_port: Option<u16>,
    kad: Option<Arc<KadService>>,
    kad_status: Arc<parking_lot::Mutex<Option<KadLookupStatus>>>,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
    cancel_token: CancellationToken,
    proxy: risuko_http::ProxyConnector,
    throttle: Throttle,
    share: Option<ShareContext>,
    part_map: PartMap,
) -> Result<PathBuf, String> {
    let worker_cancel = cancel_token.child_token();
    let _worker_guard = DownloadWorkerGuard(worker_cancel.clone());
    let file_hash = file_link.file_hash_bytes;
    let file_path = partial::part_path(final_path);
    let met_path = partial::met_path(final_path);

    total.store(file_link.file_size, Ordering::Relaxed);

    let met = {
        let (met_path, link) = (met_path.clone(), file_link.clone());
        tokio::task::spawn_blocking(move || partial::load_met(&met_path, &link))
            .await
            .map_err(|e| format!("Sidecar load failed: {e}"))?
    };
    if met.is_none() {
        let (target, sidecar) = (final_path.to_path_buf(), met_path.clone());
        let owner = partial::Met::new(file_hash, file_link.file_size, None, Vec::new());
        tokio::task::spawn_blocking(move || {
            if partial::part_path(&target).exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "partial file belongs to another download",
                ));
            }
            partial::save_met(&sidecar, &owner)
        })
        .await
        .map_err(|e| format!("Cleanup task failed: {e}"))?
        .map_err(|e| format!("Failed to write {}: {e}", met_path.display()))?;
    }

    let mut chunks = ChunkManager::new(file_path.clone(), file_link.file_size)
        .with_file_hash(file_hash)
        .with_met(met_path, worker_cancel.clone())
        .with_part_map(part_map.clone());
    chunks.init_file().await?;
    if let Some(met) = &met {
        restore_verified_parts(&mut chunks, met, &worker_cancel).await;
    }
    let restored = chunks.completed_length();
    completed.store(restored, Ordering::Relaxed);

    let chunks = Arc::new(Mutex::new(chunks));
    let peer_count = Arc::new(AtomicU32::new(0));

    let client_hash = share
        .as_ref()
        .map_or_else(new_user_hash, |share| share.user_hash);
    let login_hash = new_user_hash();
    let _partial_share = share.as_ref().map(|share| {
        share.share(SharedFile::partial(
            file_hash,
            file_link.file_name.clone(),
            file_link.file_size,
            file_path.clone(),
            part_map,
        ))
    });
    let scheduler = SourceScheduler::new(
        client_hash,
        client_port,
        file_hash,
        chunks.clone(),
        completed.clone(),
        worker_cancel.clone(),
        peer_count.clone(),
        proxy.clone(),
        throttle,
    );

    let servers = server_list(&ed2k_servers);

    let link_sources: Vec<(u32, u16)> = file_link
        .sources
        .iter()
        .filter_map(|s| {
            let ip: std::net::Ipv4Addr = s.ip.parse().ok()?;
            let octets = ip.octets();
            let ip_le = u32::from_le_bytes(octets);
            Some((ip_le, s.port))
        })
        .collect();

    for &(ip, port) in &link_sources {
        if is_high_id(ip) {
            let peer_addr = SocketAddrV4::new(client_id_to_ip(ip), port);
            scheduler.submit(SourceCandidate {
                addr: peer_addr,
                client_id: 0,
                server_ip: 0,
                server_port: 0,
                origin: SourceOrigin::Link,
            });
        }
    }

    let kad_cancel = worker_cancel.child_token();
    let kad_lookup_running = Arc::new(AtomicBool::new(kad.is_some()));
    let mut kad_task = kad.map(|service| {
        let file_size = file_link.file_size;
        let scheduler = scheduler.clone();
        let kad_status = kad_status.clone();
        let peer_count = peer_count.clone();
        let running = kad_lookup_running.clone();
        let cancel = kad_cancel.clone();
        tokio::spawn(async move {
            let lookup = || {
                let (sources, status, completion) = service
                    .lookup_sources_for_client(file_hash, file_size, client_hash, cancel.clone())
                    .into_parts();
                collect_kad_sources(
                    sources,
                    status,
                    completion,
                    scheduler.clone(),
                    kad_status.clone(),
                    client_hash,
                )
            };
            let is_idle = || peer_count.load(Ordering::Relaxed) == 0 && scheduler.is_idle();
            repeat_kad_lookups(lookup, is_idle, &running, &cancel).await;
        })
    });
    if kad_task.is_none() {
        let mut status = kad_status.lock();
        if status.is_none() {
            *status = Some(KadLookupStatus {
                state: KadState::Disabled,
                ..KadLookupStatus::default()
            });
        }
    }

    let mut progress_tick = interval(Duration::from_secs(1));
    progress_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut prev_completed: u64 = restored;

    let mut last_error = String::from("No servers available");

    for entry in &servers {
        if cancel_token.is_cancelled() {
            finish_kad_lookup(&kad_cancel, &mut kad_task).await;
            return Err("cancelled".to_string());
        }

        {
            let cm = chunks.lock().await;
            if cm.is_complete() {
                finish_kad_lookup(&kad_cancel, &mut kad_task).await;
                return Ok(file_path);
            }
        }

        let addr = match entry.to_socket_addr() {
            Some(a) => a,
            None => continue,
        };

        tracing::info!("[ed2k] Trying server {} ({})", entry.name, addr);
        let mut conn = ServerConnection::new_with_proxy(
            addr,
            login_hash,
            client_port,
            kad_udp_port,
            proxy.clone(),
        );
        let connected = tokio::select! {
            _ = cancel_token.cancelled() => {
                finish_kad_lookup(&kad_cancel, &mut kad_task).await;
                return Err("cancelled".to_string());
            }
            result = conn.connect() => result,
        };
        let (event_rx, _packet_tx) = match connected {
            Ok(pair) => pair,
            Err(e) => {
                tracing::warn!("[ed2k] Failed to connect to {}: {}", entry.name, e);
                last_error = format!("Failed to connect to {}: {}", entry.name, e);
                continue;
            }
        };
        tracing::info!("[ed2k] Connected to server {}", entry.name);

        let server_ip = u32::from_le_bytes(addr.ip().octets());
        let server_port_val = addr.port();
        connections.store(1, Ordering::Relaxed);

        match run_server_session(
            &conn,
            event_rx,
            server_ip,
            server_port_val,
            client_port,
            file_hash,
            &file_path,
            &chunks,
            &completed,
            &speed,
            &worker_cancel,
            &peer_count,
            &connections,
            &scheduler,
            &mut progress_tick,
            &mut prev_completed,
        )
        .await
        {
            Ok(path) => {
                finish_kad_lookup(&kad_cancel, &mut kad_task).await;
                return Ok(path);
            }
            Err(e) if e == "cancelled" => {
                finish_kad_lookup(&kad_cancel, &mut kad_task).await;
                return Err(e);
            }
            Err(e) => {
                tracing::warn!("[ed2k] Server {} session ended: {}", entry.name, e);
                last_error = e;
            }
        }
    }

    tracing::info!("[ed2k] All servers tried, waiting for active peers to finish");
    let fallback_wait = fallback_wait(kad_lookup_active(&kad_task, &kad_lookup_running));
    let deadline = tokio::time::Instant::now() + fallback_wait;
    let mut deadline_passed = false;
    loop {
        if cancel_token.is_cancelled() {
            finish_kad_lookup(&kad_cancel, &mut kad_task).await;
            return Err("cancelled".to_string());
        }
        {
            let cm = chunks.lock().await;
            if cm.is_complete() {
                finish_kad_lookup(&kad_cancel, &mut kad_task).await;
                return Ok(file_path);
            }
        }
        let kad_finished = !kad_lookup_active(&kad_task, &kad_lookup_running);
        let no_work = peer_count.load(Ordering::Relaxed) == 0 && scheduler.is_idle();
        if no_work && (kad_finished || deadline_passed || tokio::time::Instant::now() >= deadline) {
            break;
        }
        tokio::select! {
            _ = cancel_token.cancelled() => {}
            _ = scheduler.wake.notified() => {}
            _ = tokio::time::sleep_until(deadline), if !deadline_passed => {
                deadline_passed = true;
            }
            _ = async {
                if let Some(task) = kad_task.as_mut() {
                    let _ = task.await;
                }
            }, if kad_task.is_some() => {
                kad_task = None;
            }
            _ = progress_tick.tick() => {
                publish_progress(&chunks, &completed, &speed, &connections, &peer_count, 0, &mut prev_completed).await;
            }
        }
    }

    {
        let cm = chunks.lock().await;
        if cm.is_complete() {
            finish_kad_lookup(&kad_cancel, &mut kad_task).await;
            return Ok(file_path);
        }
    }

    finish_kad_lookup(&kad_cancel, &mut kad_task).await;
    Err(last_error)
}

async fn run_server_session(
    server: &ServerConnection,
    mut event_rx: tokio::sync::mpsc::Receiver<ServerEvent>,
    server_ip: u32,
    server_port_val: u16,
    client_port: u16,
    file_hash: [u8; 16],
    file_path: &Path,
    chunks: &Arc<Mutex<ChunkManager>>,
    completed: &Arc<AtomicU64>,
    speed: &Arc<AtomicU64>,
    cancel_token: &CancellationToken,
    peer_count: &Arc<AtomicU32>,
    connections: &Arc<AtomicU32>,
    scheduler: &Arc<SourceScheduler>,
    progress_tick: &mut tokio::time::Interval,
    prev_completed: &mut u64,
) -> Result<PathBuf, String> {
    let mut got_id = false;
    let mut sources_requested = false;
    let mut client_id: u32 = 0;
    let mut source_check = interval(SERVER_REASK_INTERVAL);
    source_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let login_deadline = tokio::time::sleep(SERVER_LOGIN_TIMEOUT);
    tokio::pin!(login_deadline);

    loop {
        if cancel_token.is_cancelled() {
            return Err("cancelled".to_string());
        }

        {
            let cm = chunks.lock().await;
            if cm.is_complete() {
                return Ok(file_path.to_path_buf());
            }
        }

        tokio::select! {
            _ = cancel_token.cancelled() => {
                return Err("cancelled".to_string());
            }
            event = event_rx.recv() => {
                match event {
                    Some(ServerEvent::Connected { client_id: cid, .. }) => {
                        tracing::info!("[ed2k] Got client ID: {} ({})",
                            cid,
                            if is_high_id(cid) { "High" } else { "Low" }
                        );
                        got_id = true;
                        client_id = cid;
                        server.request_sources(&file_hash).await?;
                        sources_requested = true;
                    }
                    Some(ServerEvent::FoundSources { file_hash: fh, sources }) => {
                        if fh == file_hash {
                            tracing::info!("[ed2k] Found {} sources", sources.len());
                            for &(ip, port) in &sources {
                                if !is_foreign_high_id_source(ip, port, client_id, client_port) {
                                    continue;
                                }
                                let peer_addr = SocketAddrV4::new(client_id_to_ip(ip), port);
                                scheduler.submit(SourceCandidate {
                                    addr: peer_addr,
                                    client_id,
                                    server_ip,
                                    server_port: server_port_val,
                                    origin: SourceOrigin::Server,
                                });
                            }
                        }
                    }
                    Some(ServerEvent::ServerMessage(msg)) => {
                        tracing::info!("[ed2k] Server message: {}", msg);
                    }
                    Some(ServerEvent::ServerStatus { users, files }) => {
                        tracing::info!("[ed2k] Server: {} users, {} files", users, files);
                    }
                    Some(ServerEvent::ServerList | ServerEvent::CallbackRequested(_)) => {}
                    Some(ServerEvent::Disconnected(reason)) => {
                        tracing::warn!("[ed2k] Server disconnected: {:?}", reason);
                        return Err(format!("Server disconnected: {:?}", reason));
                    }
                    None => {
                        return Err("Server event channel closed".to_string());
                    }
                }
            }
            _ = source_check.tick() => {
                if got_id && sources_requested {
                    let _ = server.request_sources(&file_hash).await;
                }
            }
            _ = &mut login_deadline, if !got_id => {
                return Err("Server login timed out".to_string());
            }
            _ = progress_tick.tick() => {
                publish_progress(chunks, completed, speed, connections, peer_count, 1, prev_completed).await;
            }
        }
    }
}

async fn publish_progress(
    chunks: &Arc<Mutex<ChunkManager>>,
    completed: &AtomicU64,
    speed: &AtomicU64,
    connections: &AtomicU32,
    peer_count: &AtomicU32,
    server_connections: u32,
    prev_completed: &mut u64,
) {
    let comp = chunks.lock().await.completed_length();
    let delta = comp.saturating_sub(*prev_completed);
    *prev_completed = comp;
    completed.store(comp, Ordering::Relaxed);
    speed.store(delta, Ordering::Relaxed);
    connections.store(
        server_connections + peer_count.load(Ordering::Relaxed),
        Ordering::Relaxed,
    );
}

async fn restore_verified_parts(
    chunks: &mut ChunkManager,
    met: &partial::Met,
    cancel: &CancellationToken,
) {
    if let Some(hashes) = met.decoded_hashset() {
        chunks.set_chunk_hashes(hashes);
    }
    for job in chunks.restore_jobs(&met.verified) {
        if cancel.is_cancelled() {
            return;
        }
        let (index, expected) = (job.index, job.expected);
        if let Ok(Ok(hash)) = tokio::task::spawn_blocking(move || hash_part_from_disk(&job)).await {
            if hash == expected {
                chunks.restore_verified(index);
            }
        }
    }
}

async fn persist_met(chunks: &Arc<Mutex<ChunkManager>>) {
    let Some((path, met)) = chunks.lock().await.take_met_save() else {
        return;
    };
    match tokio::task::spawn_blocking(move || partial::save_met(&path, &met)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!("[ed2k] could not save resume state: {e}"),
        Err(e) => tracing::warn!("[ed2k] resume state task failed: {e}"),
    }
}

async fn verify_ready_parts(
    chunks: &Arc<Mutex<ChunkManager>>,
    completed: &AtomicU64,
) -> Result<(), String> {
    let mut jobs = chunks.lock().await.take_verify_jobs().into_iter();
    while let Some(job) = jobs.next() {
        let index = job.index;
        let expected = job.expected;
        let hashed = tokio::task::spawn_blocking(move || hash_part_from_disk(&job)).await;
        let failure = match &hashed {
            Ok(Ok(_)) => None,
            Ok(Err(e)) => Some(format!("Failed to hash part {}: {}", index, e)),
            Err(e) => Some(format!("Part hash task failed: {}", e)),
        };
        if let Some(failure) = failure {
            let mut manager = chunks.lock().await;
            manager.finish_verify(index, false);
            for skipped in jobs {
                manager.requeue_verify(skipped.index);
            }
            return Err(failure);
        }
        let ok = matches!(hashed, Ok(Ok(hash)) if hash == expected);
        if !ok {
            tracing::warn!(
                "[ed2k] Part {} failed MD4 verification, re-downloading",
                index
            );
        }
        let mut cm = chunks.lock().await;
        cm.finish_verify(index, ok);
        completed.store(cm.completed_length(), Ordering::Relaxed);
    }
    persist_met(chunks).await;
    Ok(())
}

fn spawn_peer_task_with_permit(
    candidate: SourceCandidate,
    queued: Arc<QueuedSources>,
    sources: Arc<parking_lot::Mutex<SourceBook>>,
    addr: SocketAddrV4,
    client_hash: [u8; 16],
    client_id: u32,
    client_port: u16,
    server_ip: u32,
    server_port: u16,
    file_hash: [u8; 16],
    chunks: Arc<Mutex<ChunkManager>>,
    completed: Arc<AtomicU64>,
    cancel_token: CancellationToken,
    peer_count: Arc<AtomicU32>,
    permit: OwnedSemaphorePermit,
    proxy: risuko_http::ProxyConnector,
    throttle: Throttle,
    wake: Arc<Notify>,
) {
    peer_count.fetch_add(1, Ordering::Relaxed);
    tokio::spawn(async move {
        let _permit = permit;
        let result = run_peer_download(
            addr,
            client_hash,
            client_id,
            client_port,
            server_ip,
            server_port,
            &file_hash,
            &chunks,
            &completed,
            &cancel_token,
            proxy,
            &throttle,
        )
        .await;
        let requeued = match result {
            Ok(PeerOutcome::Queued(rank)) => {
                let added = queued.add(candidate, rank, reask_delay());
                if added {
                    tracing::debug!("[ed2k] Peer {} queued us at rank {}", addr, rank);
                }
                added
            }
            Ok(PeerOutcome::Recheck) => queued.add(candidate, u16::MAX, VERIFY_RECHECK_DELAY),
            Ok(PeerOutcome::NothingNeeded) => queued.add(candidate, u16::MAX, reask_delay()),
            _ => false,
        };
        sources
            .lock()
            .record_end(addr, session_end(&result, requeued), Instant::now());
        peer_count.fetch_sub(1, Ordering::Relaxed);
        wake.notify_one();

        if let Err(e) = result {
            tracing::debug!("[ed2k] Peer {} finished: {}", addr, e);
        }
    });
}

fn session_end(result: &Result<PeerOutcome, String>, requeued: bool) -> SessionEnd {
    match (result, requeued) {
        (Err(error), _) if MISCONDUCT_ERRORS.contains(&error.as_str()) => SessionEnd::Done,
        (Err(_), _) => SessionEnd::Failed,
        (Ok(_), true) => SessionEnd::Requeued,
        (Ok(_), false) => SessionEnd::Done,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerOutcome {
    Finished,
    NothingNeeded,
    Queued(u16),
    Recheck,
}

async fn run_peer_download(
    addr: SocketAddrV4,
    client_hash: [u8; 16],
    client_id: u32,
    client_port: u16,
    server_ip: u32,
    server_port: u16,
    file_hash: &[u8; 16],
    chunks: &Arc<Mutex<ChunkManager>>,
    completed: &Arc<AtomicU64>,
    cancel_token: &CancellationToken,
    proxy: risuko_http::ProxyConnector,
    throttle: &Throttle,
) -> Result<PeerOutcome, String> {
    let peer_id = NEXT_PEER_ID.fetch_add(1, Ordering::Relaxed);
    let result = peer_session(
        PeerTimeouts::default(),
        peer_id,
        addr,
        client_hash,
        client_id,
        client_port,
        server_ip,
        server_port,
        file_hash,
        chunks,
        completed,
        cancel_token,
        proxy,
        throttle,
    )
    .await;
    chunks.lock().await.release_peer(peer_id);
    result
}

async fn block_wanted(
    chunks: &Mutex<ChunkManager>,
    peer: u64,
    offset: u64,
    len: usize,
) -> Result<bool, String> {
    let manager = chunks.lock().await;
    let wanted = manager.begin_write(peer, offset, len)?.is_some();
    Ok(wanted)
}

fn can_serve(file_size: u64, caps: &PeerCaps) -> bool {
    !is_large_file(file_size) || caps.large_files()
}

pub(crate) async fn peer_session(
    timeouts: PeerTimeouts,
    peer_id: u64,
    addr: SocketAddrV4,
    client_hash: [u8; 16],
    client_id: u32,
    client_port: u16,
    server_ip: u32,
    server_port: u16,
    file_hash: &[u8; 16],
    chunks: &Arc<Mutex<ChunkManager>>,
    completed: &Arc<AtomicU64>,
    cancel_token: &CancellationToken,
    proxy: risuko_http::ProxyConnector,
    throttle: &Throttle,
) -> Result<PeerOutcome, String> {
    let mut peer = PeerConnection::new_with_proxy(
        addr,
        client_hash,
        client_id,
        client_port,
        server_ip,
        server_port,
        proxy,
    );
    let (mut event_rx, _packet_tx) = tokio::select! {
        _ = cancel_token.cancelled() => return Err("cancelled".to_string()),
        result = peer.connect() => result?,
    };

    let mut got_hello = false;
    let mut large_file = false;
    let mut got_slot = false;
    let mut slot_requested = false;
    let mut peer_parts: Vec<bool> = Vec::new();
    let mut rejected: u32 = 0;
    let mut slot_retries: u32 = 0;
    let mut queue_rank: Option<u16> = None;

    loop {
        let idle = if !got_hello {
            timeouts.handshake
        } else if got_slot {
            timeouts.transfer
        } else if slot_requested {
            timeouts.queue
        } else {
            timeouts.request
        };
        let event = tokio::select! {
            _ = cancel_token.cancelled() => return Err("cancelled".to_string()),
            event = tokio::time::timeout(idle, event_rx.recv()) => match event {
                Ok(event) => event,
                Err(_) => {
                    let (done, ranges) = if got_slot {
                        let mut cm = chunks.lock().await;
                        (cm.is_complete(), cm.assign_blocks(peer_id, &peer_parts, 3))
                    } else {
                        (false, Vec::new())
                    };
                    if done {
                        return Ok(PeerOutcome::Finished);
                    }
                    if let (false, Some(rank)) = (got_slot, queue_rank) {
                        return Ok(PeerOutcome::Queued(rank));
                    }
                    if ranges.is_empty() {
                        return Err("timed out waiting for peer".to_string());
                    }
                    peer.request_parts(file_hash, &ranges, large_file).await?;
                    continue;
                }
            },
        };
        match event {
            Some(PeerEvent::HelloAnswer(caps)) => {
                let file_size = chunks.lock().await.file_size();
                large_file = is_large_file(file_size);
                if !can_serve(file_size, &caps) {
                    return Ok(PeerOutcome::Finished);
                }
                got_hello = true;
                peer.request_file(file_hash).await?;
                peer.request_file_status(file_hash).await?;
                peer.request_hashset(file_hash).await?;
            }
            Some(PeerEvent::FileStatus {
                file_hash: fh,
                parts,
            }) => {
                if fh == *file_hash {
                    let (needs, verifying) = {
                        let manager = chunks.lock().await;
                        (
                            manager.needs_any(&parts),
                            manager.awaits_verification(&parts),
                        )
                    };
                    peer_parts = parts;
                    if needs && got_hello {
                        slot_requested = true;
                        peer.request_slot(file_hash).await?;
                    } else if verifying {
                        return Ok(PeerOutcome::Recheck);
                    } else {
                        return Ok(PeerOutcome::NothingNeeded);
                    }
                }
            }
            Some(PeerEvent::HashsetAnswer {
                file_hash: fh,
                hashes,
            }) => {
                if fh == *file_hash {
                    if !chunks.lock().await.set_chunk_hashes(hashes) {
                        return Err(BAD_HASHSET_ERROR.into());
                    }
                    verify_ready_parts(chunks, completed).await?;
                    if got_slot {
                        let ranges = chunks.lock().await.assign_blocks(peer_id, &peer_parts, 3);
                        if !ranges.is_empty() {
                            peer.request_parts(file_hash, &ranges, large_file).await?;
                        }
                    }
                }
            }
            Some(PeerEvent::SlotGiven) => {
                got_slot = true;
                queue_rank = None;
                let ranges = chunks.lock().await.assign_blocks(peer_id, &peer_parts, 3);
                if !ranges.is_empty() {
                    peer.request_parts(file_hash, &ranges, large_file).await?;
                }
            }
            Some(PeerEvent::DataReceived {
                file_hash: fh,
                start,
                data,
            }) => {
                let wanted =
                    fh == *file_hash && block_wanted(chunks, peer_id, start, data.len()).await?;
                let accepted = if wanted {
                    tokio::select! {
                        _ = cancel_token.cancelled() => return Err("cancelled".to_string()),
                        _ = throttle.acquire(data.len()) => {}
                    }
                    write_block(chunks, peer_id, start, data).await?
                } else {
                    false
                };
                if accepted {
                    rejected = 0;
                    slot_retries = 0;
                } else {
                    rejected += 1;
                    if rejected > MAX_PEER_REJECTED_PACKETS {
                        return Err(UNREQUESTED_DATA_ERROR.into());
                    }
                }
                verify_ready_parts(chunks, completed).await?;

                let (is_complete, ranges) = {
                    let mut cm = chunks.lock().await;
                    completed.store(cm.completed_length(), Ordering::Relaxed);
                    if cm.is_banned(peer_id) {
                        return Err(CORRUPT_DATA_ERROR.into());
                    }
                    if cm.is_complete() {
                        (true, vec![])
                    } else if got_slot {
                        (false, cm.assign_blocks(peer_id, &peer_parts, 3))
                    } else {
                        (false, vec![])
                    }
                };

                if is_complete {
                    return Ok(PeerOutcome::Finished);
                }
                if !ranges.is_empty() {
                    peer.request_parts(file_hash, &ranges, large_file).await?;
                }
            }
            Some(PeerEvent::SlotTaken) => {
                got_slot = false;
                queue_rank = None;
                chunks.lock().await.release_peer(peer_id);
                slot_retries += 1;
                if slot_retries > MAX_SLOT_RETRIES {
                    return Err("peer keeps revoking the upload slot".into());
                }
                slot_requested = true;
                peer.request_slot(file_hash).await?;
            }
            Some(PeerEvent::NoFile) => return Ok(PeerOutcome::Finished),
            Some(PeerEvent::QueueRanking(rank)) => {
                tracing::debug!("[ed2k] Peer {} queue rank: {}", addr, rank);
                if !got_slot {
                    queue_rank = Some(rank);
                }
            }
            Some(PeerEvent::Disconnected(reason)) => {
                if let (false, Some(rank)) = (got_slot, queue_rank) {
                    return Ok(PeerOutcome::Queued(rank));
                }
                return Err(format!("disconnected: {:?}", reason));
            }
            None => {
                return Ok(match (got_slot, queue_rank) {
                    (false, Some(rank)) => PeerOutcome::Queued(rank),
                    _ => PeerOutcome::Finished,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    fn two_part_fixture() -> (Vec<u8>, Vec<[u8; 16]>, [u8; 16]) {
        let size = ED2K_CHUNK_SIZE as usize + 1000;
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let hashes = vec![
            super::super::md4::md4(&data[..ED2K_CHUNK_SIZE as usize]),
            super::super::md4::md4(&data[ED2K_CHUNK_SIZE as usize..]),
        ];
        let concat: Vec<u8> = hashes.iter().flatten().copied().collect();
        (data, hashes, super::super::md4::md4(&concat))
    }

    async fn resumed(
        dir: &Path,
        data: &[u8],
        file_hash: [u8; 16],
        met: &partial::Met,
    ) -> ChunkManager {
        let fin = dir.join("f.bin");
        std::fs::write(partial::part_path(&fin), data).unwrap();
        let mut cm = ChunkManager::new(partial::part_path(&fin), data.len() as u64)
            .with_file_hash(file_hash);
        cm.init_file().await.unwrap();
        restore_verified_parts(&mut cm, met, &CancellationToken::new()).await;
        cm
    }

    #[tokio::test]
    async fn resume_restores_only_parts_listed_as_verified() {
        let (data, hashes, fh) = two_part_fixture();
        let dir = tempfile::tempdir().unwrap();
        let met = partial::Met::new(fh, data.len() as u64, Some(&hashes), vec![0]);
        let cm = resumed(dir.path(), &data, fh, &met).await;
        assert_eq!(cm.completed_length(), ED2K_CHUNK_SIZE);
        assert!(!cm.is_complete());
    }

    #[tokio::test]
    async fn resume_rehashes_and_drops_damaged_verified_parts() {
        let (mut data, hashes, fh) = two_part_fixture();
        data[10] ^= 0xff;
        let dir = tempfile::tempdir().unwrap();
        let met = partial::Met::new(fh, data.len() as u64, Some(&hashes), vec![0, 1]);
        let cm = resumed(dir.path(), &data, fh, &met).await;
        assert_eq!(cm.completed_length(), 1000);
    }

    #[tokio::test]
    async fn resume_with_all_parts_verified_is_complete() {
        let (data, hashes, fh) = two_part_fixture();
        let dir = tempfile::tempdir().unwrap();
        let met = partial::Met::new(fh, data.len() as u64, Some(&hashes), vec![0, 1, 7, 1]);
        let cm = resumed(dir.path(), &data, fh, &met).await;
        assert!(cm.is_complete());
        assert_eq!(cm.completed_length(), data.len() as u64);
    }

    #[tokio::test]
    async fn resume_ignores_a_hashset_that_does_not_chain() {
        let (data, mut hashes, fh) = two_part_fixture();
        hashes[0][0] ^= 1;
        let dir = tempfile::tempdir().unwrap();
        let met = partial::Met::new(fh, data.len() as u64, Some(&hashes), vec![0, 1]);
        let cm = resumed(dir.path(), &data, fh, &met).await;
        assert_eq!(cm.completed_length(), 0);
    }

    #[tokio::test]
    async fn verified_parts_are_persisted_and_survive_a_restart() {
        let (data, hashes, fh) = two_part_fixture();
        let dir = tempfile::tempdir().unwrap();
        let fin = dir.path().join("f.bin");
        let met_path = partial::met_path(&fin);
        let cancel = CancellationToken::new();
        let mut cm = ChunkManager::new(partial::part_path(&fin), data.len() as u64)
            .with_file_hash(fh)
            .with_met(met_path.clone(), cancel.clone());
        cm.init_file().await.unwrap();
        assert!(cm.set_chunk_hashes(hashes));
        cm.restore_verified(1);
        cm.finish_verify(0, true);
        let (path, met) = cm.take_met_save().unwrap();
        assert_eq!(path, met_path);
        assert_eq!(met.verified, vec![0, 1]);
        assert!(cm.take_met_save().is_none());
        cm.finish_verify(0, true);
        cancel.cancel();
        assert!(cm.take_met_save().is_none());
    }

    use super::super::protocol::Ed2kPacket;
    use super::*;
    use std::collections::HashSet;
    use std::net::Ipv4Addr;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Semaphore as TestGate;
    use tokio::time::timeout;

    fn source_candidate(addr: SocketAddrV4, client_id: u32, server_ip: u32) -> SourceCandidate {
        source_candidate_from(SourceOrigin::Server, addr, client_id, server_ip)
    }

    fn source_candidate_from(
        origin: SourceOrigin,
        addr: SocketAddrV4,
        client_id: u32,
        server_ip: u32,
    ) -> SourceCandidate {
        SourceCandidate {
            addr,
            client_id,
            server_ip,
            server_port: 4661,
            origin,
        }
    }

    fn public_addr(index: usize) -> SocketAddrV4 {
        let third = (index / 254 + 1) as u8;
        let fourth = (index % 254 + 1) as u8;
        SocketAddrV4::new(Ipv4Addr::new(8, 8, third, fourth), 4000 + index as u16)
    }

    fn recording_dispatcher() -> (
        CandidateDispatcher,
        tokio::sync::mpsc::UnboundedReceiver<SourceCandidate>,
    ) {
        let (started_tx, started_rx) = mpsc::unbounded_channel();
        let dispatcher: CandidateDispatcher = Arc::new(move |candidate, permit| {
            let _ = started_tx.send(candidate);
            drop(permit);
        });
        (dispatcher, started_rx)
    }

    fn gated_dispatcher(
        gate: Arc<TestGate>,
        active: Arc<AtomicUsize>,
    ) -> (
        CandidateDispatcher,
        tokio::sync::mpsc::UnboundedReceiver<SourceCandidate>,
    ) {
        let (started_tx, started_rx) = mpsc::unbounded_channel();
        let dispatcher: CandidateDispatcher = Arc::new(move |candidate, permit| {
            active.fetch_add(1, Ordering::SeqCst);
            let _ = started_tx.send(candidate);
            let gate = gate.clone();
            let active = active.clone();
            tokio::spawn(async move {
                let _gate_permit = gate
                    .acquire_owned()
                    .await
                    .expect("test gate should remain open");
                active.fetch_sub(1, Ordering::SeqCst);
                drop(permit);
            });
        });
        (dispatcher, started_rx)
    }

    #[tokio::test]
    async fn finish_kad_lookup_cancels_and_joins_the_collector() {
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let mut task = Some(tokio::spawn(async move {
            task_cancel.cancelled().await;
            let _ = finished_tx.send(());
        }));

        finish_kad_lookup(&cancel, &mut task).await;

        assert!(cancel.is_cancelled());
        assert!(task.is_none());
        finished_rx.await.expect("collector completed");
    }

    #[test]
    fn server_sources_skip_low_ids_and_our_own_listener() {
        let own = u32::from_le_bytes([8, 8, 8, 8]);
        assert!(!is_foreign_high_id_source(own, 4662, own, 4662));
        assert!(is_foreign_high_id_source(own, 4663, own, 4662));
        assert!(is_foreign_high_id_source(own, 4662, 1234, 4662));
        assert!(!is_foreign_high_id_source(1234, 4662, own, 4662));
    }

    #[test]
    fn fallback_wait_does_not_extend_after_kad_lookup_finishes() {
        assert_eq!(fallback_wait(false), SERVER_FALLBACK_WAIT);
    }

    #[test]
    fn fallback_wait_extends_only_while_kad_lookup_is_running() {
        assert_eq!(fallback_wait(true), KAD_FALLBACK_WAIT);
    }

    #[tokio::test]
    async fn kad_collector_drains_buffered_sources_after_status_closes() {
        let cancel = CancellationToken::new();
        let (dispatcher, mut started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 1, 1, dispatcher);
        let (source_tx, source_rx) = mpsc::channel(1);
        let (status_tx, status_rx) = tokio::sync::watch::channel(KadLookupStatus::default());
        let addr = public_addr(1);
        source_tx
            .send(super::super::kad::KadSource {
                client_hash: [1; 16],
                addr,
                source_type: 1,
            })
            .await
            .expect("source receiver should be open");
        drop(source_tx);
        drop(status_tx);

        let completion = tokio::spawn(async { Ok::<(), super::super::kad::KadError>(()) });
        let kad_status = Arc::new(parking_lot::Mutex::new(None));
        collect_kad_sources(
            source_rx, status_rx, completion, scheduler, kad_status, [9; 16],
        )
        .await;

        assert_eq!(
            timeout(Duration::from_secs(1), started.recv())
                .await
                .expect("buffered Kad source should be dispatched")
                .expect("dispatcher should stay open")
                .addr,
            addr
        );
        cancel.cancel();
    }

    #[test]
    fn download_worker_guard_cancels_only_its_child_scope() {
        let parent = CancellationToken::new();
        let child = parent.child_token();
        {
            let _guard = DownloadWorkerGuard(child.clone());
        }

        assert!(child.is_cancelled());
        assert!(!parent.is_cancelled());
    }

    async fn run_unreachable(
        link: &Ed2kFileLink,
        target: &Path,
        kad_status: Arc<parking_lot::Mutex<Option<KadLookupStatus>>>,
    ) -> Result<Ed2kCompleted, String> {
        run_ed2k_download_with_proxy(
            link,
            partial::PathClaim::acquire(target).unwrap(),
            true,
            vec!["127.0.0.1:1".into()],
            4662,
            None,
            None,
            kad_status,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            CancellationToken::new(),
            risuko_http::ProxyConnector::direct(),
            Throttle::unlimited(),
            None,
        )
        .await
    }

    #[tokio::test]
    async fn a_new_partial_records_its_owner_before_any_part_verifies() {
        let directory = tempfile::tempdir().unwrap();
        let link = super::super::parse_ed2k_link(
            "ed2k://|file|test.bin|1|0123456789abcdef0123456789abcdef|/",
        )
        .unwrap();
        let fin = directory.path().join("test.bin");
        let kad_status = Arc::new(parking_lot::Mutex::new(None));
        assert!(run_unreachable(&link, &fin, kad_status).await.is_err());
        assert!(partial::part_path(&fin).exists());
        assert!(partial::load_met(&partial::met_path(&fin), &link).is_some());
    }

    #[tokio::test]
    async fn a_partial_without_a_record_is_never_deleted() {
        let directory = tempfile::tempdir().unwrap();
        let link = super::super::parse_ed2k_link(
            "ed2k://|file|test.bin|1|0123456789abcdef0123456789abcdef|/",
        )
        .unwrap();
        let fin = directory.path().join("test.bin");
        std::fs::write(partial::part_path(&fin), b"foreign").unwrap();
        let kad_status = Arc::new(parking_lot::Mutex::new(None));
        let error = run_unreachable(&link, &fin, kad_status).await.unwrap_err();
        assert!(error.contains("belongs to another download"));
        assert_eq!(std::fs::read(partial::part_path(&fin)).unwrap(), b"foreign");
        assert!(!partial::met_path(&fin).exists());
    }

    #[tokio::test]
    async fn unavailable_kad_preserves_the_initial_failure_diagnostic() {
        let directory = tempfile::tempdir().unwrap();
        let link = super::super::parse_ed2k_link(
            "ed2k://|file|test.bin|1|0123456789abcdef0123456789abcdef|/",
        )
        .unwrap();
        let kad_status = Arc::new(parking_lot::Mutex::new(Some(KadLookupStatus {
            state: KadState::Error,
            queried_nodes: 0,
            discovered_sources: 0,
            contacts: 0,
            error: Some("UDP port is already in use".into()),
        })));

        let result = run_unreachable(
            &link,
            &directory.path().join("test.bin"),
            kad_status.clone(),
        )
        .await;

        assert!(result.is_err());
        let status = kad_status.lock();
        assert_eq!(
            status.as_ref().map(|status| status.state),
            Some(KadState::Error)
        );
        assert_eq!(
            status.as_ref().and_then(|status| status.error.as_deref()),
            Some("UDP port is already in use")
        );
    }

    #[tokio::test]
    async fn source_scheduler_deduplicates_link_server_and_kad_candidates() {
        let cancel = CancellationToken::new();
        let (dispatcher, mut started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 8, 64, dispatcher);
        let addr = public_addr(1);

        assert!(scheduler.submit(source_candidate_from(SourceOrigin::Link, addr, 0, 0)));
        assert!(!scheduler.submit(source_candidate_from(
            SourceOrigin::Server,
            addr,
            123,
            0x0102_0304,
        )));
        assert!(!scheduler.submit(source_candidate_from(SourceOrigin::Kad, addr, 0, 0)));

        let dispatched = timeout(Duration::from_secs(1), started.recv())
            .await
            .expect("link source should be dispatched")
            .expect("dispatcher should stay open");
        assert_eq!(dispatched.addr, addr);
        assert!(timeout(Duration::from_millis(25), started.recv())
            .await
            .is_err());

        cancel.cancel();
    }

    #[tokio::test]
    async fn source_scheduler_rejects_non_global_kad_endpoints_only() {
        let cancel = CancellationToken::new();
        let (dispatcher, mut started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 8, 64, dispatcher);
        let invalid = [
            SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 4662),
            SocketAddrV4::new(Ipv4Addr::new(0, 1, 2, 3), 4662),
            SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4662),
            SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 4662),
            SocketAddrV4::new(Ipv4Addr::new(172, 16, 0, 1), 4662),
            SocketAddrV4::new(Ipv4Addr::new(192, 168, 0, 1), 4662),
            SocketAddrV4::new(Ipv4Addr::new(192, 0, 0, 1), 4662),
            SocketAddrV4::new(Ipv4Addr::new(192, 88, 99, 1), 4662),
            SocketAddrV4::new(Ipv4Addr::new(198, 18, 0, 1), 4662),
            SocketAddrV4::new(Ipv4Addr::new(198, 19, 0, 1), 4662),
            SocketAddrV4::new(Ipv4Addr::new(224, 0, 0, 1), 4662),
            SocketAddrV4::new(Ipv4Addr::new(240, 0, 0, 1), 4662),
            SocketAddrV4::new(Ipv4Addr::new(8, 8, 8, 8), 0),
        ];

        for addr in invalid {
            assert!(
                !usable_source_addr(addr),
                "unexpected usable endpoint {addr}"
            );
            assert!(!scheduler.submit(source_candidate_from(SourceOrigin::Kad, addr, 0, 0)));
        }
        assert!(usable_source_addr(SocketAddrV4::new(
            Ipv4Addr::new(8, 8, 8, 8),
            4662,
        )));

        let link_addr = SocketAddrV4::new(Ipv4Addr::new(10, 0, 0, 1), 4662);
        let server_addr = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 4663);
        assert!(scheduler.submit(source_candidate_from(SourceOrigin::Link, link_addr, 0, 0,)));
        assert!(scheduler.submit(source_candidate_from(
            SourceOrigin::Server,
            server_addr,
            1,
            0x0102_0304,
        )));

        let dispatched = [
            timeout(Duration::from_secs(1), started.recv())
                .await
                .expect("link source should be dispatched")
                .expect("dispatcher should stay open")
                .addr,
            timeout(Duration::from_secs(1), started.recv())
                .await
                .expect("server source should be dispatched")
                .expect("dispatcher should stay open")
                .addr,
        ]
        .into_iter()
        .collect::<HashSet<_>>();
        assert_eq!(dispatched, HashSet::from([link_addr, server_addr]));
        cancel.cancel();
    }

    #[tokio::test]
    async fn source_scheduler_allows_retry_after_queue_full() {
        let cancel = CancellationToken::new();
        let gate = Arc::new(TestGate::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let (dispatcher, mut started) = gated_dispatcher(gate.clone(), active.clone());
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 1, 1, dispatcher);
        let first = source_candidate(public_addr(1), 0, 0);

        assert!(scheduler.submit(first));
        assert_eq!(
            timeout(Duration::from_secs(1), started.recv())
                .await
                .expect("first candidate should start")
                .expect("dispatcher should stay open")
                .addr,
            first.addr
        );
        let mut accepted = Vec::new();
        let rejected = loop {
            let candidate = source_candidate(public_addr(accepted.len() + 2), 0, 0);
            if scheduler.submit(candidate) {
                accepted.push(candidate);
            } else {
                break candidate;
            }
        };
        assert!(!accepted.is_empty());

        gate.add_permits(1);
        let accepted_addresses: HashSet<SocketAddrV4> =
            accepted.iter().map(|candidate| candidate.addr).collect();
        for _ in &accepted {
            let started_candidate = timeout(Duration::from_secs(1), started.recv())
                .await
                .expect("queued candidate should start after permit release")
                .expect("dispatcher should stay open");
            assert!(accepted_addresses.contains(&started_candidate.addr));
            gate.add_permits(1);
        }
        timeout(Duration::from_secs(1), async {
            while active.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all accepted candidates should release their permits");

        assert!(scheduler.submit(rejected));
        gate.add_permits(1);
        assert_eq!(
            timeout(Duration::from_secs(1), started.recv())
                .await
                .expect("retried candidate should start")
                .expect("dispatcher should stay open")
                .addr,
            rejected.addr
        );
        gate.add_permits(1);
        cancel.cancel();
    }

    #[tokio::test]
    async fn source_scheduler_caps_active_dials_at_64() {
        let cancel = CancellationToken::new();
        let gate = Arc::new(TestGate::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let (dispatcher, mut started) = gated_dispatcher(gate.clone(), active.clone());
        let scheduler = SourceScheduler::with_dispatcher(
            cancel.clone(),
            MAX_ACTIVE_PEER_DIALS + 1,
            MAX_ACTIVE_PEER_DIALS,
            dispatcher,
        );

        for index in 0..=MAX_ACTIVE_PEER_DIALS {
            assert!(scheduler.submit(source_candidate(public_addr(index), 0, 0)));
        }
        for _ in 0..MAX_ACTIVE_PEER_DIALS {
            timeout(Duration::from_secs(1), started.recv())
                .await
                .expect("each of the first 64 candidates should start")
                .expect("dispatcher should stay open");
        }
        assert_eq!(active.load(Ordering::SeqCst), MAX_ACTIVE_PEER_DIALS);
        assert!(timeout(Duration::from_millis(25), started.recv())
            .await
            .is_err());

        gate.add_permits(MAX_ACTIVE_PEER_DIALS);
        let overflow = timeout(Duration::from_secs(1), started.recv())
            .await
            .expect("the 65th candidate should start after a permit releases")
            .expect("dispatcher should stay open");
        assert_eq!(overflow.addr, public_addr(MAX_ACTIVE_PEER_DIALS));
        gate.add_permits(1);
        cancel.cancel();
    }

    #[tokio::test]
    async fn source_scheduler_cancellation_drops_waiting_candidates() {
        let cancel = CancellationToken::new();
        let gate = Arc::new(TestGate::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let (dispatcher, mut started) = gated_dispatcher(gate.clone(), active);
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 2, 1, dispatcher);
        let first = source_candidate(public_addr(1), 0, 0);
        let waiting = source_candidate(public_addr(2), 0, 0);

        assert!(scheduler.submit(first));
        assert_eq!(
            timeout(Duration::from_secs(1), started.recv())
                .await
                .expect("first candidate should start")
                .expect("dispatcher should stay open")
                .addr,
            first.addr
        );
        assert!(scheduler.submit(waiting));

        tokio::task::yield_now().await;
        cancel.cancel();
        assert!(matches!(
            timeout(Duration::from_millis(100), started.recv()).await,
            Err(_) | Ok(None)
        ));
        gate.add_permits(1);
    }

    #[tokio::test]
    async fn source_scheduler_wakes_the_fallback_wait_after_dispatch() {
        let cancel = CancellationToken::new();
        let (dispatcher, _started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 1, 1, dispatcher);

        assert!(scheduler.submit(source_candidate(public_addr(1), 0, 0)));
        timeout(Duration::from_secs(1), scheduler.wake.notified())
            .await
            .expect("dispatching a source should wake the waiter");
        assert!(scheduler.is_idle());
        cancel.cancel();
    }

    #[tokio::test]
    async fn source_scheduler_keeps_queued_work_visible_until_dispatch() {
        let cancel = CancellationToken::new();
        let (dispatcher, _started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 1, 0, dispatcher);

        assert!(scheduler.submit(source_candidate(public_addr(1), 0, 0)));
        assert!(!scheduler.is_idle());
        tokio::task::yield_now().await;
        assert!(!scheduler.is_idle());

        cancel.cancel();
    }

    #[tokio::test]
    async fn source_scheduler_dispatches_server_and_kad_sources_in_parallel() {
        let cancel = CancellationToken::new();
        let gate = Arc::new(TestGate::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let (dispatcher, mut started) = gated_dispatcher(gate.clone(), active.clone());
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 2, 2, dispatcher);
        let server_source = source_candidate(public_addr(1), 0x0102_0304, 0x0506_0708);
        let kad_source = source_candidate(public_addr(2), 0, 0);

        let (server_accepted, kad_accepted) = tokio::join!(
            {
                let scheduler = scheduler.clone();
                async move { scheduler.submit(server_source) }
            },
            {
                let scheduler = scheduler.clone();
                async move { scheduler.submit(kad_source) }
            },
        );
        assert!(server_accepted);
        assert!(kad_accepted);

        let first = timeout(Duration::from_secs(1), started.recv())
            .await
            .expect("server/Kad candidates should be dispatched")
            .expect("dispatcher should stay open");
        let second = timeout(Duration::from_secs(1), started.recv())
            .await
            .expect("server/Kad candidates should be dispatched in parallel")
            .expect("dispatcher should stay open");
        let addresses = [first.addr, second.addr]
            .into_iter()
            .collect::<HashSet<_>>();
        assert_eq!(addresses.len(), 2);
        assert_eq!(active.load(Ordering::SeqCst), 2);

        gate.add_permits(2);
        cancel.cancel();
    }

    #[test]
    fn large_files_only_go_to_capable_peers() {
        let plain = PeerCaps::default();
        let capable = PeerCaps {
            misc_options2: MISC2_LARGE_FILES,
            ..PeerCaps::default()
        };
        let big = u64::from(u32::MAX) + 1;
        assert!(can_serve(OLD_MAX_FILE_SIZE, &plain));
        assert!(!can_serve(OLD_MAX_FILE_SIZE + 1, &plain));
        assert!(!can_serve(big, &plain));
        assert!(can_serve(big, &capable));
        assert!(can_serve(1000, &capable));
        assert!(!is_large_file(1000));
        assert!(is_large_file(big));
        let r = [(0u64, 100u64)];
        let a = super::super::protocol::build_request_parts(&[0; 16], &r, is_large_file(1000));
        let b = super::super::protocol::build_request_parts(&[0; 16], &r, is_large_file(big));
        assert_eq!(a.unwrap().opcode, OP_REQUEST_PARTS);
        assert_eq!(b.unwrap().opcode, OP_REQUEST_PARTS_I64);
    }

    fn queued_candidate(index: usize) -> SourceCandidate {
        source_candidate(public_addr(index), 0, 0)
    }

    #[tokio::test(start_paused = true)]
    async fn queued_source_is_reasked_after_the_delay_and_not_before() {
        let cancel = CancellationToken::new();
        let (dispatcher, mut started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 4, 4, dispatcher);
        let candidate = queued_candidate(1);
        assert!(scheduler.submit(candidate));
        started.recv().await.expect("first dial");
        assert!(scheduler.is_idle());

        assert!(scheduler.queued.add(candidate, 9, FILE_REASK_INTERVAL));
        assert!(!scheduler.is_idle());
        assert!(!scheduler.submit(candidate));

        tokio::time::sleep(FILE_REASK_INTERVAL - Duration::from_secs(1)).await;
        assert!(started.try_recv().is_err());

        tokio::time::sleep(Duration::from_secs(2)).await;
        let again = started.recv().await.expect("re-ask dial");
        assert_eq!(again.addr, candidate.addr);
        assert!(scheduler.is_idle());
        cancel.cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_with_nothing_needed_is_dialled_again_after_the_reask_delay() {
        let cancel = CancellationToken::new();
        let (dispatcher, mut started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 4, 4, dispatcher);
        let candidate = queued_candidate(1);
        assert!(scheduler.submit(candidate));
        started.recv().await.expect("first dial");

        let delay = reask_delay();
        let requeued = scheduler.queued.add(candidate, u16::MAX, delay);
        let end = session_end(&Ok(PeerOutcome::NothingNeeded), requeued);
        assert_eq!(end, SessionEnd::Requeued);
        end_session(&scheduler, candidate.addr, end, Instant::now());
        assert!(!scheduler.submit(candidate));

        tokio::time::sleep(delay - Duration::from_secs(1)).await;
        assert!(started.try_recv().is_err());

        tokio::time::sleep(Duration::from_secs(2)).await;
        let again = started.recv().await.expect("re-ask dial");
        assert_eq!(again.addr, candidate.addr);
        cancel.cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn queued_sources_are_capped_by_the_source_limit() {
        let cancel = CancellationToken::new();
        let (dispatcher, _started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 2, 2, dispatcher);
        let delay = Duration::from_secs(60);
        assert!(scheduler.queued.add(queued_candidate(1), 1, delay));
        assert!(scheduler.queued.add(queued_candidate(2), 2, delay));
        assert!(!scheduler.queued.add(queued_candidate(3), 3, delay));
        assert!(scheduler.queued.add(queued_candidate(1), 4, delay));
        assert_eq!(scheduler.queued.sources.lock().len(), 2);
        assert_eq!(scheduler.queued.sources.lock()[&public_addr(1)].rank, 4);
        cancel.cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn reask_waits_for_dial_queue_room_without_blocking() {
        let cancel = CancellationToken::new();
        let gate = Arc::new(TestGate::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let (dispatcher, mut started) = gated_dispatcher(gate.clone(), active);
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 1, 1, dispatcher);
        assert!(scheduler.submit(queued_candidate(1)));
        started.recv().await.expect("first dial");
        assert!(scheduler.submit(queued_candidate(2)));
        tokio::task::yield_now().await;
        assert!(scheduler.submit(queued_candidate(3)));

        assert!(scheduler
            .queued
            .add(queued_candidate(4), 1, Duration::from_secs(1)));
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert_eq!(scheduler.queued.sources.lock().len(), 1);

        gate.add_permits(8);
        tokio::time::sleep(REASK_BACKOFF + Duration::from_secs(1)).await;
        let mut dialled = Vec::new();
        while dialled.len() < 3 {
            match timeout(Duration::from_secs(1), started.recv()).await {
                Ok(Some(c)) => dialled.push(c.addr),
                _ => break,
            }
        }
        assert!(dialled.contains(&public_addr(4)));
        assert!(scheduler.queued.is_empty());
        cancel.cancel();
    }

    #[test]
    fn reask_delay_stays_within_emule_filereasktime_plus_jitter() {
        for _ in 0..64 {
            let d = reask_delay();
            assert!(d >= FILE_REASK_INTERVAL);
            assert!(d <= FILE_REASK_INTERVAL + Duration::from_secs(FILE_REASK_JITTER_SECS));
        }
    }

    fn end_session(scheduler: &SourceScheduler, addr: SocketAddrV4, end: SessionEnd, now: Instant) {
        scheduler.sources.lock().record_end(addr, end, now);
    }

    #[tokio::test]
    async fn failed_source_is_accepted_again_after_a_doubling_delay() {
        let cancel = CancellationToken::new();
        let (dispatcher, _started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 16, 16, dispatcher);
        let candidate = queued_candidate(1);
        let second = Duration::from_secs(1);
        let t0 = Instant::now();

        assert!(scheduler.submit_at(candidate, t0));
        end_session(&scheduler, candidate.addr, SessionEnd::Failed, t0);
        assert!(!scheduler.submit_at(candidate, t0));
        assert!(!scheduler.submit_at(candidate, t0 + SOURCE_RETRY_BASE - second));

        let t1 = t0 + SOURCE_RETRY_BASE;
        assert!(scheduler.submit_at(candidate, t1));
        end_session(&scheduler, candidate.addr, SessionEnd::Failed, t1);
        assert!(!scheduler.submit_at(candidate, t1));
        assert!(!scheduler.submit_at(candidate, t1 + 2 * SOURCE_RETRY_BASE - second));
        assert!(scheduler.submit_at(candidate, t1 + 2 * SOURCE_RETRY_BASE));
        cancel.cancel();
    }

    #[test]
    fn source_retry_delay_doubles_up_to_the_cap() {
        assert_eq!(source_retry_delay(1), SOURCE_RETRY_BASE);
        assert_eq!(source_retry_delay(2), SOURCE_RETRY_BASE * 2);
        assert_eq!(source_retry_delay(3), SOURCE_RETRY_BASE * 4);
        for failures in 0..=64 {
            assert!(source_retry_delay(failures) <= SOURCE_RETRY_MAX);
        }
        assert_eq!(source_retry_delay(u32::MAX), SOURCE_RETRY_MAX);
    }

    #[tokio::test]
    async fn source_is_refused_for_good_after_the_last_attempt_fails() {
        let cancel = CancellationToken::new();
        let (dispatcher, _started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 16, 16, dispatcher);
        let candidate = queued_candidate(1);
        let mut now = Instant::now();

        for _ in 0..MAX_SOURCE_ATTEMPTS {
            assert!(scheduler.submit_at(candidate, now));
            end_session(&scheduler, candidate.addr, SessionEnd::Failed, now);
            now += SOURCE_RETRY_MAX;
        }
        assert!(!scheduler.submit_at(candidate, now));
        assert!(!scheduler.submit_at(candidate, now + Duration::from_secs(7 * 24 * 3600)));
        cancel.cancel();
    }

    #[tokio::test]
    async fn a_successful_session_resets_the_failure_count() {
        let cancel = CancellationToken::new();
        let (dispatcher, _started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 16, 16, dispatcher);
        let candidate = queued_candidate(1);
        let mut now = Instant::now();

        for _ in 0..MAX_SOURCE_ATTEMPTS - 1 {
            assert!(scheduler.submit_at(candidate, now));
            end_session(&scheduler, candidate.addr, SessionEnd::Failed, now);
            now += SOURCE_RETRY_MAX;
        }
        assert!(scheduler.submit_at(candidate, now));
        end_session(&scheduler, candidate.addr, SessionEnd::Requeued, now);
        assert!(!scheduler.submit_at(candidate, now + SOURCE_RETRY_MAX));
        end_session(&scheduler, candidate.addr, SessionEnd::Failed, now);
        assert!(scheduler.submit_at(candidate, now + SOURCE_RETRY_BASE));
        end_session(
            &scheduler,
            candidate.addr,
            SessionEnd::Done,
            now + SOURCE_RETRY_BASE,
        );
        assert!(!scheduler.submit_at(candidate, now + SOURCE_RETRY_MAX * 4));
        cancel.cancel();
    }

    #[test]
    fn misconduct_ends_a_source_and_other_errors_count_as_failures() {
        for error in [
            BAD_HASHSET_ERROR,
            UNREQUESTED_DATA_ERROR,
            CORRUPT_DATA_ERROR,
        ] {
            assert_eq!(
                session_end(&Err(error.to_string()), false),
                SessionEnd::Done
            );
        }
        for error in [
            "timed out waiting for peer".to_string(),
            format!("disconnected: {:?}", Some("connection reset")),
            "peer keeps revoking the upload slot".to_string(),
            format!("{CORRUPT_DATA_ERROR} again"),
        ] {
            assert_eq!(session_end(&Err(error), false), SessionEnd::Failed);
        }
        assert_eq!(
            session_end(&Ok(PeerOutcome::Queued(3)), true),
            SessionEnd::Requeued
        );
        assert_eq!(
            session_end(&Ok(PeerOutcome::Queued(3)), false),
            SessionEnd::Done
        );
        assert_eq!(
            session_end(&Ok(PeerOutcome::Finished), false),
            SessionEnd::Done
        );
        assert_eq!(
            session_end(&Ok(PeerOutcome::NothingNeeded), true),
            SessionEnd::Requeued
        );
    }

    #[tokio::test]
    async fn a_misbehaving_source_is_refused_for_the_rest_of_the_download() {
        let cancel = CancellationToken::new();
        let (dispatcher, _started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 16, 16, dispatcher);
        let candidate = queued_candidate(1);
        let now = Instant::now();

        assert!(scheduler.submit_at(candidate, now));
        let end = session_end(&Err(CORRUPT_DATA_ERROR.to_string()), false);
        end_session(&scheduler, candidate.addr, end, now);
        assert!(!scheduler.submit_at(candidate, now + Duration::from_secs(7 * 24 * 3600)));
        cancel.cancel();
    }

    #[tokio::test]
    async fn pending_source_is_refused_however_late_it_is_offered_again() {
        let cancel = CancellationToken::new();
        let (dispatcher, _started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 16, 16, dispatcher);
        let candidate = queued_candidate(1);
        let now = Instant::now();

        assert!(scheduler.submit_at(candidate, now));
        assert!(!scheduler.submit_at(candidate, now + Duration::from_secs(365 * 24 * 3600)));
        cancel.cancel();
    }

    #[test]
    fn source_book_stays_within_its_cap_and_keeps_live_entries() {
        let mut book = SourceBook::new(4);
        let now = Instant::now();
        let [live, done, near, far] = [1, 2, 3, 4].map(public_addr);
        for addr in [live, done, near, far] {
            assert!(book.admit(addr, now));
        }
        book.record_end(done, SessionEnd::Done, now);
        book.record_end(near, SessionEnd::Failed, now);
        book.record_end(far, SessionEnd::Failed, now + Duration::from_secs(60));

        let newcomers = [5, 6, 7].map(public_addr);
        assert!(book.admit(newcomers[0], now));
        assert_eq!(book.states.len(), 4);
        assert!(!book.states.contains_key(&far));
        assert!(book.states.contains_key(&near));

        assert!(book.admit(newcomers[1], now));
        assert_eq!(book.states.len(), 4);
        assert!(!book.states.contains_key(&near));
        assert!(book.states.contains_key(&done));

        assert!(book.admit(newcomers[2], now));
        assert_eq!(book.states.len(), 4);
        assert!(!book.states.contains_key(&done));

        assert!(!book.admit(public_addr(8), now));
        assert_eq!(book.states.len(), 4);
        assert!(book.states.contains_key(&live));
        assert!(newcomers.iter().all(|addr| book.states.contains_key(addr)));
    }

    #[tokio::test]
    async fn a_full_dial_queue_refuses_a_source_without_evicting_another() {
        let cancel = CancellationToken::new();
        let (dispatcher, _started) = recording_dispatcher();
        let scheduler = SourceScheduler::with_dispatcher(cancel.clone(), 1, 0, dispatcher);
        let now = Instant::now();
        assert!(scheduler.submit_at(queued_candidate(1), now));
        {
            let mut book = scheduler.sources.lock();
            let failed = SourceState::Failed {
                failures: 1,
                retry_at: now + SOURCE_RETRY_BASE,
            };
            let mut index = 2;
            while book.states.len() < MAX_TRACKED_SOURCES {
                book.states.insert(public_addr(index), failed);
                index += 1;
            }
        }
        let before = scheduler.sources.lock().states.clone();

        assert!(!scheduler.submit_at(queued_candidate(MAX_TRACKED_SOURCES + 10), now));
        assert_eq!(scheduler.sources.lock().states, before);
        cancel.cancel();
    }

    #[test]
    fn kad_relookup_follows_the_interval_or_sustained_idleness() {
        let finished = Instant::now();
        let second = Duration::from_secs(1);
        let minute = Duration::from_secs(60);

        assert!(!kad_relookup_due(finished, None, finished));
        assert!(!kad_relookup_due(
            finished,
            None,
            finished + KAD_RELOOKUP_INTERVAL - second
        ));
        assert!(kad_relookup_due(
            finished,
            None,
            finished + KAD_RELOOKUP_INTERVAL
        ));

        assert!(!kad_relookup_due(
            finished,
            Some(finished),
            finished + KAD_IDLE_RELOOKUP_DELAY - second
        ));
        assert!(kad_relookup_due(
            finished,
            Some(finished),
            finished + KAD_IDLE_RELOOKUP_DELAY
        ));

        let idle = finished + 10 * minute;
        assert!(!kad_relookup_due(
            finished,
            Some(idle),
            idle + KAD_IDLE_RELOOKUP_DELAY - second
        ));
        assert!(kad_relookup_due(
            finished,
            Some(idle),
            idle + KAD_IDLE_RELOOKUP_DELAY
        ));
        assert!(!kad_relookup_due(
            finished,
            Some(finished - 10 * minute),
            finished + second
        ));
    }

    enum FakePeerEnd {
        Close,
        Hold,
    }

    async fn fake_queueing_peer(rank: u16, end: FakePeerEnd, file_hash: [u8; 16]) -> SocketAddrV4 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = match listener.local_addr().expect("addr") {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!("bound to an IPv4 address"),
        };
        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut scratch = [0u8; 256];
            let _ = stream.read(&mut scratch).await;
            let mut status = file_hash.to_vec();
            status.extend_from_slice(&2u16.to_le_bytes());
            status.push(0b11);
            let packets = [
                Ed2kPacket::new(PROTO_EDONKEY, OP_HELLO_ANSWER, vec![0; 26]),
                Ed2kPacket::new(PROTO_EDONKEY, OP_FILE_STATUS, status),
                Ed2kPacket::new(
                    PROTO_EMULE,
                    OP_EMULE_QUEUE_RANKING,
                    rank.to_le_bytes().to_vec(),
                ),
            ];
            for packet in packets {
                if stream.write_all(&packet.encode()).await.is_err() {
                    return;
                }
            }
            if let FakePeerEnd::Hold = end {
                while stream
                    .read(&mut scratch)
                    .await
                    .map(|n| n > 0)
                    .unwrap_or(false)
                {}
            }
        });
        addr
    }

    async fn fake_sending_peer(file_hash: [u8; 16], blocks: Vec<(u64, Vec<u8>)>) -> SocketAddrV4 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = match listener.local_addr().expect("addr") {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!("bound to an IPv4 address"),
        };
        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut scratch = [0u8; 256];
            let _ = stream.read(&mut scratch).await;
            let mut status = file_hash.to_vec();
            status.extend_from_slice(&2u16.to_le_bytes());
            status.push(0b11);
            let mut packets = vec![
                Ed2kPacket::new(PROTO_EDONKEY, OP_HELLO_ANSWER, vec![0; 26]),
                Ed2kPacket::new(PROTO_EDONKEY, OP_FILE_STATUS, status),
                Ed2kPacket::new(PROTO_EDONKEY, OP_SLOT_GIVEN, Vec::new()),
            ];
            for (start, data) in blocks {
                let end = start + data.len() as u64;
                let mut payload = file_hash.to_vec();
                payload.extend_from_slice(&(start as u32).to_le_bytes());
                payload.extend_from_slice(&(end as u32).to_le_bytes());
                payload.extend_from_slice(&data);
                packets.push(Ed2kPacket::new(PROTO_EDONKEY, OP_SENDING_PART, payload));
            }
            packets.push(Ed2kPacket::new(
                PROTO_EDONKEY,
                OP_FILE_REQ_ANS_NOFILE,
                file_hash.to_vec(),
            ));
            for packet in packets {
                if stream.write_all(&packet.encode()).await.is_err() {
                    return;
                }
            }
            while stream
                .read(&mut scratch)
                .await
                .map(|n| n > 0)
                .unwrap_or(false)
            {}
        });
        addr
    }

    async fn fake_peer_offering(file_hash: [u8; 16], parts: u8) -> SocketAddrV4 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = match listener.local_addr().expect("addr") {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!("bound to an IPv4 address"),
        };
        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut scratch = [0u8; 256];
            let _ = stream.read(&mut scratch).await;
            let mut status = file_hash.to_vec();
            status.extend_from_slice(&2u16.to_le_bytes());
            status.push(parts);
            let packets = [
                Ed2kPacket::new(PROTO_EDONKEY, OP_HELLO_ANSWER, vec![0; 26]),
                Ed2kPacket::new(PROTO_EDONKEY, OP_FILE_STATUS, status),
            ];
            for packet in packets {
                if stream.write_all(&packet.encode()).await.is_err() {
                    return;
                }
            }
            while stream
                .read(&mut scratch)
                .await
                .map(|n| n > 0)
                .unwrap_or(false)
            {}
        });
        addr
    }

    async fn fixture_chunks(
        fixture_dir: &Path,
        size: u64,
        file_hash: [u8; 16],
    ) -> Arc<Mutex<ChunkManager>> {
        let fin = fixture_dir.join("f.bin");
        let mut cm = ChunkManager::new(partial::part_path(&fin), size).with_file_hash(file_hash);
        cm.init_file().await.expect("init file");
        Arc::new(Mutex::new(cm))
    }

    async fn session_with(
        addr: SocketAddrV4,
        chunks: &Arc<Mutex<ChunkManager>>,
        file_hash: [u8; 16],
        timeouts: PeerTimeouts,
        throttle: &Throttle,
    ) -> Result<PeerOutcome, String> {
        let completed = Arc::new(AtomicU64::new(0));
        peer_session(
            timeouts,
            1,
            addr,
            [1; 16],
            0,
            4662,
            0,
            0,
            &file_hash,
            chunks,
            &completed,
            &CancellationToken::new(),
            risuko_http::ProxyConnector::direct(),
            throttle,
        )
        .await
    }

    async fn session_against(
        addr: SocketAddrV4,
        fixture_dir: &Path,
        timeouts: PeerTimeouts,
    ) -> Result<PeerOutcome, String> {
        let (data, _hashes, file_hash) = two_part_fixture();
        let chunks = fixture_chunks(fixture_dir, data.len() as u64, file_hash).await;
        session_with(addr, &chunks, file_hash, timeouts, &Throttle::unlimited()).await
    }

    #[tokio::test]
    async fn unrequested_block_skips_the_limiter_and_requested_block_still_waits() {
        use super::super::chunks::ED2K_BLOCK_SIZE;
        use risuko_util::limiter::RateLimiter;
        let (data, _hashes, file_hash) = two_part_fixture();
        let rate = 32 * 1024;
        let requested = 8 * 1024;
        let unrequested = (4 * ED2K_BLOCK_SIZE, vec![0; ED2K_BLOCK_SIZE as usize]);
        let addr = fake_sending_peer(
            file_hash,
            vec![unrequested, (0, data[..requested].to_vec())],
        )
        .await;
        let dir = tempfile::tempdir().expect("tempdir");
        let chunks = fixture_chunks(dir.path(), data.len() as u64, file_hash).await;
        let started = Instant::now();
        let throttle = Throttle::new(
            Arc::new(RateLimiter::new(rate)),
            Arc::new(RateLimiter::unlimited()),
        );
        throttle.acquire(rate as usize).await;

        let outcome = timeout(
            Duration::from_secs(3),
            session_with(addr, &chunks, file_hash, PeerTimeouts::default(), &throttle),
        )
        .await
        .expect("an unrequested block must not wait on the limiter");
        assert_eq!(outcome, Ok(PeerOutcome::Finished));
        assert_eq!(chunks.lock().await.completed_length(), requested as u64);
        assert!(started.elapsed() >= Duration::from_millis(240));
    }

    #[tokio::test]
    async fn queued_peer_that_disconnects_is_reported_as_queued_with_its_rank() {
        let (_data, _hashes, file_hash) = two_part_fixture();
        let addr = fake_queueing_peer(42, FakePeerEnd::Close, file_hash).await;
        let dir = tempfile::tempdir().expect("tempdir");
        let outcome = session_against(addr, dir.path(), PeerTimeouts::default()).await;
        assert_eq!(outcome, Ok(PeerOutcome::Queued(42)));
    }

    #[tokio::test]
    async fn queued_peer_is_held_through_the_queue_window_then_released_as_queued() {
        let (_data, _hashes, file_hash) = two_part_fixture();
        let addr = fake_queueing_peer(3, FakePeerEnd::Hold, file_hash).await;
        let dir = tempfile::tempdir().expect("tempdir");
        let timeouts = PeerTimeouts {
            queue: Duration::from_millis(300),
            ..PeerTimeouts::default()
        };
        let outcome = session_against(addr, dir.path(), timeouts).await;
        assert_eq!(outcome, Ok(PeerOutcome::Queued(3)));
    }

    #[tokio::test]
    async fn a_peer_with_no_needed_part_is_asked_again_later() {
        let (data, _hashes, file_hash) = two_part_fixture();
        let addr = fake_peer_offering(file_hash, 0b01).await;
        let dir = tempfile::tempdir().expect("tempdir");
        let chunks = fixture_chunks(dir.path(), data.len() as u64, file_hash).await;
        chunks.lock().await.restore_verified(0);
        let outcome = timeout(
            Duration::from_secs(5),
            session_with(
                addr,
                &chunks,
                file_hash,
                PeerTimeouts::default(),
                &Throttle::unlimited(),
            ),
        )
        .await
        .expect("the session ends once the peer's parts are known");
        assert_eq!(outcome, Ok(PeerOutcome::NothingNeeded));
    }

    #[tokio::test]
    async fn silent_peer_without_a_queue_rank_still_times_out_as_an_error() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = match listener.local_addr().expect("addr") {
            std::net::SocketAddr::V4(a) => a,
            _ => unreachable!("bound to an IPv4 address"),
        };
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut scratch = [0u8; 64];
                while stream
                    .read(&mut scratch)
                    .await
                    .map(|n| n > 0)
                    .unwrap_or(false)
                {}
            }
        });
        let dir = tempfile::tempdir().expect("tempdir");
        let timeouts = PeerTimeouts {
            handshake: Duration::from_millis(300),
            ..PeerTimeouts::default()
        };
        let outcome = session_against(addr, dir.path(), timeouts).await;
        assert!(outcome.is_err());
    }
}
