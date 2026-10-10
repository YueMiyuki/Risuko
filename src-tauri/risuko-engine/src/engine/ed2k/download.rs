use std::collections::{HashMap, HashSet};
use std::net::SocketAddrV4;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::{interval, Duration, Instant};
use tokio_util::sync::CancellationToken;

use risuko_bt::limiter::Throttle;

use super::chunks::{hash_part_from_disk, write_block, ChunkManager};
use super::kad::{routing::is_public_ipv4, KadLookupStatus, KadService, KadState};
use super::partial;
use super::peer::{PeerConnection, PeerEvent};
use super::protocol::{new_user_hash, PeerCaps};
use super::server::{ServerConnection, ServerEvent};
use super::server_list::server_list;
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

#[derive(Debug, Clone, Copy)]
struct PeerTimeouts {
    handshake: Duration,
    request: Duration,
    queue: Duration,
    transfer: Duration,
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

fn reask_delay() -> Duration {
    use rand::RngExt;
    FILE_REASK_INTERVAL + Duration::from_secs(rand::rng().random_range(0..=FILE_REASK_JITTER_SECS))
}

struct SourceScheduler {
    seen: parking_lot::Mutex<HashSet<SocketAddrV4>>,
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
        let dispatcher: CandidateDispatcher = Arc::new(move |candidate, permit| {
            spawn_peer_task_with_permit(
                candidate,
                peer_queued.clone(),
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
        )
    }

    fn with_dispatcher_and_wake(
        cancel_token: CancellationToken,
        queue_capacity: usize,
        max_active_dials: usize,
        dispatcher: CandidateDispatcher,
        wake: Arc<Notify>,
        queued: Arc<QueuedSources>,
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
            seen: parking_lot::Mutex::new(HashSet::new()),
            queued,
            candidates,
            cancel_token,
            pending,
            wake,
        })
    }

    fn submit(&self, candidate: SourceCandidate) -> bool {
        let usable_addr = if candidate.origin == SourceOrigin::Kad {
            usable_source_addr(candidate.addr)
        } else {
            candidate.addr.port() != 0
        };
        if self.cancel_token.is_cancelled() || !usable_addr {
            return false;
        }
        let mut seen = self.seen.lock();
        if !seen.insert(candidate.addr) {
            return false;
        }
        self.pending.fetch_add(1, Ordering::Relaxed);
        if self.candidates.try_send(candidate).is_ok() {
            return true;
        }

        self.pending.fetch_sub(1, Ordering::Relaxed);
        seen.remove(&candidate.addr);
        false
    }

    fn is_idle(&self) -> bool {
        self.queued.is_empty() && self.pending.load(Ordering::Relaxed) == 0
    }
}

fn usable_source_addr(addr: SocketAddrV4) -> bool {
    addr.port() != 0 && is_public_ipv4(*addr.ip())
}

pub async fn run_ed2k_download_with_proxy(
    file_link: &Ed2kFileLink,
    dir: &str,
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
) -> Result<PathBuf, String> {
    let final_path = partial::final_path(dir, &file_link.file_name);
    run_download(
        file_link,
        dir,
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
    )
    .await?;
    let target = final_path.clone();
    tokio::task::spawn_blocking(move || partial::finalize(&target))
        .await
        .map_err(|e| format!("Finalize task failed: {e}"))??;
    Ok(final_path)
}

async fn run_download(
    file_link: &Ed2kFileLink,
    dir: &str,
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
) -> Result<PathBuf, String> {
    let worker_cancel = cancel_token.child_token();
    let _worker_guard = DownloadWorkerGuard(worker_cancel.clone());
    let file_hash = file_link.file_hash_bytes;
    let final_path = partial::final_path(dir, &file_link.file_name);
    let file_path = partial::part_path(&final_path);
    let met_path = partial::met_path(&final_path);

    total.store(file_link.file_size, Ordering::Relaxed);

    let met = {
        let (met_path, link) = (met_path.clone(), file_link.clone());
        tokio::task::spawn_blocking(move || partial::load_met(&met_path, &link))
            .await
            .map_err(|e| format!("Sidecar load failed: {e}"))?
    };
    if met.is_none() {
        let target = final_path.clone();
        tokio::task::spawn_blocking(move || partial::remove_partial_files(&target))
            .await
            .map_err(|e| format!("Cleanup task failed: {e}"))?;
    }

    let mut chunks = ChunkManager::new(file_path.clone(), file_link.file_size)
        .with_file_hash(file_hash)
        .with_met(met_path, worker_cancel.clone());
    chunks.init_file().await?;
    if let Some(met) = &met {
        restore_verified_parts(&mut chunks, met, &worker_cancel).await;
    }
    let restored = chunks.completed_length();
    completed.store(restored, Ordering::Relaxed);

    let chunks = Arc::new(Mutex::new(chunks));
    let peer_count = Arc::new(AtomicU32::new(0));

    let client_hash = new_user_hash();
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
    let mut kad_task = kad.map(|service| {
        let lookup = service.lookup_sources_for_client(
            file_hash,
            file_link.file_size,
            client_hash,
            kad_cancel.clone(),
        );
        let scheduler = scheduler.clone();
        let kad_status = kad_status.clone();
        tokio::spawn(async move {
            let (sources, status, completion) = lookup.into_parts();
            collect_kad_sources(
                sources,
                status,
                completion,
                scheduler,
                kad_status,
                client_hash,
            )
            .await;
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
            client_hash,
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
    let fallback_wait = fallback_wait(kad_task.as_ref().is_some_and(|task| !task.is_finished()));
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
        let kad_finished = kad_task.as_ref().is_none_or(|task| task.is_finished());
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
                    Some(ServerEvent::Connected { client_id: cid }) => {
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
                                if !is_high_id(ip) {
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
                    Some(ServerEvent::ServerList) => {}
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
    let jobs = chunks.lock().await.take_verify_jobs();
    for job in jobs {
        let index = job.index;
        let expected = job.expected;
        let hashed = tokio::task::spawn_blocking(move || hash_part_from_disk(&job)).await;
        let ok = match hashed {
            Ok(Ok(hash)) => hash == expected,
            Ok(Err(e)) => {
                chunks.lock().await.finish_verify(index, false);
                return Err(format!("Failed to hash part {}: {}", index, e));
            }
            Err(e) => {
                chunks.lock().await.finish_verify(index, false);
                return Err(format!("Part hash task failed: {}", e));
            }
        };
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
        if let Ok(PeerOutcome::Queued(rank)) = result {
            if queued.add(candidate, rank, reask_delay()) {
                tracing::debug!("[ed2k] Peer {} queued us at rank {}", addr, rank);
            }
        }
        peer_count.fetch_sub(1, Ordering::Relaxed);
        wake.notify_one();

        if let Err(e) = result {
            tracing::debug!("[ed2k] Peer {} finished: {}", addr, e);
        }
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PeerOutcome {
    Finished,
    Queued(u16),
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

const OLD_MAX_EMULE_FILE_SIZE: u64 = 4_290_048_000;

fn is_large_file(file_size: u64) -> bool {
    file_size > OLD_MAX_EMULE_FILE_SIZE
}

fn can_serve(file_size: u64, caps: &PeerCaps) -> bool {
    !is_large_file(file_size) || caps.large_files()
}

async fn peer_session(
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
                    let needs = chunks.lock().await.needs_any(&parts);
                    peer_parts = parts;
                    if needs && got_hello {
                        slot_requested = true;
                        peer.request_slot(file_hash).await?;
                    } else {
                        return Ok(PeerOutcome::Finished);
                    }
                }
            }
            Some(PeerEvent::HashsetAnswer {
                file_hash: fh,
                hashes,
            }) => {
                if fh == *file_hash {
                    if !chunks.lock().await.set_chunk_hashes(hashes) {
                        return Err("peer sent a hashset that does not match the file hash".into());
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
                tokio::select! {
                    _ = cancel_token.cancelled() => return Err("cancelled".to_string()),
                    _ = throttle.acquire(data.len()) => {}
                }
                let accepted =
                    fh == *file_hash && write_block(chunks, peer_id, start, data).await?;
                if accepted {
                    rejected = 0;
                    slot_retries = 0;
                } else {
                    rejected += 1;
                    if rejected > MAX_PEER_REJECTED_PACKETS {
                        return Err("peer keeps sending data that was not requested".into());
                    }
                }
                verify_ready_parts(chunks, completed).await?;

                let (is_complete, ranges) = {
                    let mut cm = chunks.lock().await;
                    completed.store(cm.completed_length(), Ordering::Relaxed);
                    if cm.is_banned(peer_id) {
                        return Err("peer served corrupt data".into());
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

        let result = run_ed2k_download_with_proxy(
            &link,
            directory.path().to_str().unwrap(),
            vec!["127.0.0.1:1".into()],
            4662,
            None,
            None,
            kad_status.clone(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            CancellationToken::new(),
            risuko_http::ProxyConnector::direct(),
            Throttle::unlimited(),
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
        assert!(can_serve(OLD_MAX_EMULE_FILE_SIZE, &plain));
        assert!(!can_serve(OLD_MAX_EMULE_FILE_SIZE + 1, &plain));
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

    async fn session_against(
        addr: SocketAddrV4,
        fixture_dir: &Path,
        timeouts: PeerTimeouts,
    ) -> Result<PeerOutcome, String> {
        let (data, _hashes, file_hash) = two_part_fixture();
        let fin = fixture_dir.join("f.bin");
        let mut cm = ChunkManager::new(partial::part_path(&fin), data.len() as u64)
            .with_file_hash(file_hash);
        cm.init_file().await.expect("init file");
        let chunks = Arc::new(Mutex::new(cm));
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
            &chunks,
            &completed,
            &CancellationToken::new(),
            risuko_http::ProxyConnector::direct(),
            &Throttle::unlimited(),
        )
        .await
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
