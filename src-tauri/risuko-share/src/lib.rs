use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use iroh::{
    address_lookup::MemoryLookup,
    endpoint::{presets, TransportAddrUsage},
    protocol::Router,
    Endpoint, EndpointId, RelayMode,
};
use iroh_blobs::{
    api::{
        blobs::{AddPathOptions, ExportMode, ExportOptions, ImportMode},
        downloader::{DownloadProgressItem, Shuffled},
        Store,
    },
    format::collection::Collection,
    provider::events::{
        ConnectMode, EventMask, EventSender, ProviderMessage, RequestMode, RequestUpdate,
    },
    store::fs::FsStore,
    ticket::BlobTicket,
    BlobFormat, BlobsProtocol, Hash, HashAndFormat,
};
use n0_future::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

struct SendProgressTracker {
    total: u64,
    finished: u64,
    current_end: u64,
    last_emit: Instant,
}

impl SendProgressTracker {
    fn transferred(&self) -> u64 {
        (self.finished + self.current_end).min(self.total)
    }
}

fn watch_send_request(
    mut rx: irpc::channel::mpsc::Receiver<RequestUpdate>,
    id: String,
    events: UnboundedSender<ShareEnvelope>,
    stop: Arc<AtomicBool>,
    tracker: Arc<Mutex<SendProgressTracker>>,
) {
    tokio::spawn(async move {
        while let Ok(Some(update)) = rx.recv().await {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let emit = {
                let mut state = tracker.lock().unwrap();
                let mut force = false;
                match update {
                    RequestUpdate::Started { .. } => {
                        state.current_end = 0;
                    }
                    RequestUpdate::Progress(progress) => {
                        state.current_end = progress.end_offset;
                    }
                    RequestUpdate::Completed(done) => {
                        state.finished += done.stats.payload_bytes_sent;
                        state.current_end = 0;
                        force = true;
                    }
                    RequestUpdate::Aborted(_) => break,
                }
                let now = Instant::now();
                if force || now.duration_since(state.last_emit) >= PROGRESS_EMIT_INTERVAL {
                    state.last_emit = now;
                    Some((state.transferred(), state.total))
                } else {
                    None
                }
            };
            if let Some((transferred, total)) = emit {
                let _ = events.send(ShareEnvelope {
                    id: id.clone(),
                    event: ShareEvent::Progress { transferred, total },
                });
            }
        }
    });
}

fn begin_send_transfer(
    events: &UnboundedSender<ShareEnvelope>,
    id: &str,
    peer: Option<EndpointId>,
    endpoint: Endpoint,
    transferring: &mut bool,
    path_task: &mut Option<tokio::task::JoinHandle<()>>,
    stop: Arc<AtomicBool>,
) {
    if *transferring {
        return;
    }
    *transferring = true;
    let _ = events.send(ShareEnvelope {
        id: id.to_string(),
        event: ShareEvent::Connected,
    });
    if let Some(remote) = peer {
        *path_task = Some(spawn_path_poll(
            endpoint,
            remote,
            id.to_string(),
            events.clone(),
            stop,
        ));
    }
}

const PATH_POLL_INTERVAL: Duration = Duration::from_millis(700);
const ONLINE_TIMEOUT: Duration = Duration::from_secs(6);
const PROGRESS_EMIT_INTERVAL: Duration = Duration::from_millis(100);

async fn bind_endpoint() -> Result<Endpoint> {
    Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Default)
        .bind()
        .await
        .context("failed to bind endpoint")
}

fn spawn_online_warmup(endpoint: Endpoint) {
    tokio::spawn(async move {
        endpoint.online().await;
    });
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMeta {
    pub name: String,
    pub size: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PathKind {
    Direct,
    Relay,
    Mixed,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum ShareEvent {
    Connected,
    Path { path: PathKind },
    Progress { transferred: u64, total: u64 },
    Completed,
    Terminated,
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareEnvelope {
    pub id: String,
    #[serde(flatten)]
    pub event: ShareEvent,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendInfo {
    pub ticket: String,
    pub files: Vec<FileMeta>,
}

struct ActiveTransfer {
    _router: Router,
    store: Store,
    blobs_dir: PathBuf,
    stop: Arc<AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl ActiveTransfer {
    async fn shutdown(self) {
        self.stop.store(true, Ordering::Relaxed);
        for task in self.tasks {
            task.abort();
        }
        self._router.shutdown().await.ok();
        let _ = self.store.shutdown().await;
        let _ = tokio::fs::remove_dir_all(&self.blobs_dir).await;
    }
}

struct ReceiveHandle {
    stop: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

fn stale_dirs(data_dir: &Path) -> Vec<PathBuf> {
    let Ok(read) = std::fs::read_dir(data_dir) else {
        return Vec::new();
    };
    read.flatten()
        .filter(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            (name.starts_with("send-") || name.starts_with("recv-"))
                && entry.file_type().is_ok_and(|t| t.is_dir())
        })
        .map(|entry| entry.path())
        .collect()
}

struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl AbortOnDrop {
    fn abort(&self) {
        self.0.abort();
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub struct ShareManager {
    data_dir: PathBuf,
    events: UnboundedSender<ShareEnvelope>,
    active: Arc<Mutex<HashMap<String, ActiveTransfer>>>,
    receives: Arc<Mutex<HashMap<String, ReceiveHandle>>>,
}

impl ShareManager {
    pub fn new(data_dir: PathBuf, events: UnboundedSender<ShareEnvelope>) -> Self {
        let stale = stale_dirs(&data_dir);
        if !stale.is_empty() {
            std::thread::spawn(move || {
                for dir in stale {
                    let _ = std::fs::remove_dir_all(dir);
                }
            });
        }
        Self {
            data_dir,
            events,
            active: Arc::new(Mutex::new(HashMap::new())),
            receives: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn start_send(&self, id: String, paths: Vec<PathBuf>) -> Result<SendInfo> {
        anyhow::ensure!(!paths.is_empty(), "no files selected");
        let blobs_dir = self.data_dir.join(format!("send-{id}"));
        let result = self.start_send_inner(id, paths, blobs_dir.clone()).await;
        if result.is_err() {
            let _ = tokio::fs::remove_dir_all(&blobs_dir).await;
        }
        result
    }

    async fn start_send_inner(
        &self,
        id: String,
        paths: Vec<PathBuf>,
        blobs_dir: PathBuf,
    ) -> Result<SendInfo> {
        let endpoint_task = tokio::spawn(async {
            let endpoint = bind_endpoint().await?;
            let _ = tokio::time::timeout(ONLINE_TIMEOUT, endpoint.online()).await;
            anyhow::Ok(endpoint)
        });

        tokio::fs::create_dir_all(&blobs_dir).await.ok();

        let mask = EventMask {
            connected: ConnectMode::Notify,
            get: RequestMode::NotifyLog,
            get_many: RequestMode::NotifyLog,
            ..EventMask::DEFAULT
        };
        let (event_tx, mut event_rx) = EventSender::channel(64, mask);

        let store: Store = FsStore::load(&blobs_dir)
            .await
            .context("failed to open blob store")?
            .into();

        let (entries, has_dir) = tokio::task::spawn_blocking(move || {
            let mut entries: Vec<(String, PathBuf, u64)> = Vec::new();
            let mut has_dir = false;
            for path in &paths {
                let abs = std::path::absolute(path)?;
                let meta = std::fs::metadata(&abs).context("failed to stat selection")?;
                if meta.is_dir() {
                    has_dir = true;
                    collect_dir(&abs, &file_name(&abs), &mut entries)?;
                } else {
                    entries.push((file_name(&abs), abs, meta.len()));
                }
            }
            anyhow::Ok((entries, has_dir))
        })
        .await??;
        anyhow::ensure!(!entries.is_empty(), "no files to send");

        let files: Vec<FileMeta> = entries
            .iter()
            .map(|(name, _, size)| FileMeta {
                name: name.clone(),
                size: *size,
            })
            .collect();

        let import_opts = |path: PathBuf| AddPathOptions {
            path,
            mode: ImportMode::TryReference,
            format: BlobFormat::Raw,
        };

        let ticket_hash;
        let ticket_format;

        if entries.len() == 1 && !has_dir {
            let tag = store
                .blobs()
                .add_path_with_opts(import_opts(entries[0].1.clone()))
                .await?;
            ticket_hash = tag.hash;
            ticket_format = tag.format;
        } else {
            let limit = std::thread::available_parallelism().map_or(4, |n| n.get());
            let semaphore = Arc::new(tokio::sync::Semaphore::new(limit));
            let mut imports = tokio::task::JoinSet::new();
            for (idx, (_, abs, _)) in entries.iter().enumerate() {
                let store = store.clone();
                let semaphore = semaphore.clone();
                let opts = import_opts(abs.clone());
                imports.spawn(async move {
                    let _permit = semaphore.acquire_owned().await?;
                    let tag = store.blobs().add_path_with_opts(opts).await?;
                    anyhow::Ok((idx, tag.hash))
                });
            }
            let mut hashes: Vec<Option<Hash>> = vec![None; entries.len()];
            while let Some(joined) = imports.join_next().await {
                let (idx, hash) = joined??;
                hashes[idx] = Some(hash);
            }
            let mut items: Vec<(String, Hash)> = Vec::with_capacity(entries.len());
            for ((name, _, _), hash) in entries.iter().zip(hashes) {
                items.push((name.clone(), hash.context("file import did not finish")?));
            }
            let collection = Collection::from_iter(items);
            let tt = collection.store(&store).await?;
            store.tags().create(tt.hash_and_format()).await?;
            ticket_hash = tt.hash();
            ticket_format = BlobFormat::HashSeq;
        }

        let endpoint = endpoint_task.await??;
        let addr = endpoint.addr();

        let blobs = BlobsProtocol::new(&store, Some(event_tx));
        let router = Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, blobs)
            .spawn();

        let ticket = BlobTicket::new(addr, ticket_hash, ticket_format).to_string();

        let stop = Arc::new(AtomicBool::new(false));

        let events = self.events.clone();
        let active = self.active.clone();
        let endpoint_for_events = router.endpoint().clone();
        let id_for_events = id.clone();
        let stop_for_events = stop.clone();
        let total_bytes: u64 = files.iter().map(|f| f.size).sum();
        let progress_tracker = Arc::new(Mutex::new(SendProgressTracker {
            total: total_bytes,
            finished: 0,
            current_end: 0,
            last_emit: Instant::now(),
        }));
        let event_task = tokio::spawn(async move {
            let mut peer: Option<EndpointId> = None;
            let mut transferring = false;
            let mut completed = false;
            let mut finished = false;
            let mut path_task: Option<tokio::task::JoinHandle<()>> = None;

            while let Some(msg) = event_rx.recv().await {
                if stop_for_events.load(Ordering::Relaxed) {
                    break;
                }
                match msg {
                    ProviderMessage::ClientConnected(m) => {
                        peer = m.endpoint_id;
                    }
                    ProviderMessage::ClientConnectedNotify(m) => {
                        peer = m.endpoint_id;
                    }
                    ProviderMessage::GetRequestReceivedNotify(m) => {
                        begin_send_transfer(
                            &events,
                            &id_for_events,
                            peer,
                            endpoint_for_events.clone(),
                            &mut transferring,
                            &mut path_task,
                            stop_for_events.clone(),
                        );
                        watch_send_request(
                            m.rx,
                            id_for_events.clone(),
                            events.clone(),
                            stop_for_events.clone(),
                            progress_tracker.clone(),
                        );
                    }
                    ProviderMessage::GetManyRequestReceivedNotify(m) => {
                        begin_send_transfer(
                            &events,
                            &id_for_events,
                            peer,
                            endpoint_for_events.clone(),
                            &mut transferring,
                            &mut path_task,
                            stop_for_events.clone(),
                        );
                        watch_send_request(
                            m.rx,
                            id_for_events.clone(),
                            events.clone(),
                            stop_for_events.clone(),
                            progress_tracker.clone(),
                        );
                    }
                    ProviderMessage::ConnectionClosed(_) if transferring && !completed => {
                        completed = true;
                        if stop_for_events.load(Ordering::Relaxed) {
                        } else {
                            let transferred = progress_tracker.lock().unwrap().transferred();
                            if total_bytes == 0 || transferred >= total_bytes {
                                let _ = events.send(ShareEnvelope {
                                    id: id_for_events.clone(),
                                    event: ShareEvent::Progress {
                                        transferred: total_bytes,
                                        total: total_bytes,
                                    },
                                });
                                let _ = events.send(ShareEnvelope {
                                    id: id_for_events.clone(),
                                    event: ShareEvent::Completed,
                                });
                                finished = true;
                            } else {
                                let _ = events.send(ShareEnvelope {
                                    id: id_for_events.clone(),
                                    event: ShareEvent::Terminated,
                                });
                            }
                        }
                    }
                    _ => {}
                }
                if finished {
                    break;
                }
            }

            if let Some(t) = path_task {
                t.abort();
            }
            if finished {
                let transfer = active.lock().unwrap().remove(&id_for_events);
                if let Some(transfer) = transfer {
                    tokio::spawn(transfer.shutdown());
                }
            }
        });

        let transfer = ActiveTransfer {
            _router: router,
            store,
            blobs_dir,
            stop,
            tasks: vec![event_task],
        };
        self.active.lock().unwrap().insert(id, transfer);

        Ok(SendInfo { ticket, files })
    }

    pub fn start_receive(
        &self,
        id: String,
        ticket: String,
        dest_dir: PathBuf,
        files: Vec<FileMeta>,
    ) -> Result<()> {
        let blobs_dir = self.data_dir.join(format!("recv-{id}"));
        let stop = Arc::new(AtomicBool::new(false));
        let events = self.events.clone();
        let receives = self.receives.clone();

        let stop_for_task = stop.clone();
        let id_for_task = id.clone();
        let dir_for_task = blobs_dir.clone();

        let handle = tokio::spawn(async move {
            let result = run_receive(
                &dir_for_task,
                &ticket,
                &dest_dir,
                &files,
                &id_for_task,
                &events,
                stop_for_task,
            )
            .await;

            match result {
                Ok(()) => {
                    let _ = events.send(ShareEnvelope {
                        id: id_for_task.clone(),
                        event: ShareEvent::Completed,
                    });
                }
                Err(err) => {
                    let message = err.to_string();
                    if message == "cancelled" {
                        receives.lock().unwrap().remove(&id_for_task);
                        let _ = tokio::fs::remove_dir_all(&dir_for_task).await;
                        return;
                    }
                    let _ = events.send(ShareEnvelope {
                        id: id_for_task.clone(),
                        event: ShareEvent::Error { message },
                    });
                }
            }

            receives.lock().unwrap().remove(&id_for_task);
            let _ = tokio::fs::remove_dir_all(&dir_for_task).await;
        });
        self.receives
            .lock()
            .unwrap()
            .insert(id, ReceiveHandle { stop, task: handle });
        Ok(())
    }

    pub async fn cancel(&self, id: &str) {
        let transfer = self.active.lock().unwrap().remove(id);
        if let Some(transfer) = transfer {
            transfer.shutdown().await;
        }
        let recv = self.receives.lock().unwrap().remove(id);
        if let Some(recv) = recv {
            recv.stop.store(true, Ordering::Relaxed);
            recv.task.abort();
            let _ = tokio::fs::remove_dir_all(self.data_dir.join(format!("recv-{id}"))).await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_receive(
    blobs_dir: &Path,
    ticket_str: &str,
    dest_dir: &Path,
    files: &[FileMeta],
    id: &str,
    events: &UnboundedSender<ShareEnvelope>,
    stop: Arc<AtomicBool>,
) -> Result<()> {
    tokio::fs::create_dir_all(dest_dir).await.ok();
    tokio::fs::create_dir_all(blobs_dir).await.ok();

    let ticket: BlobTicket = ticket_str.parse().context("invalid share ticket")?;
    let provider = ticket.addr().id;
    let hash = ticket.hash();
    let format = ticket.format();
    let total: u64 = files.iter().map(|f| f.size).sum();

    let store: Store = FsStore::load(blobs_dir)
        .await
        .context("failed to open blob store")?
        .into();

    let lookup = MemoryLookup::new();
    lookup.add_endpoint_info(ticket.addr().clone());
    let endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Default)
        .address_lookup(lookup)
        .bind()
        .await
        .context("failed to bind endpoint")?;
    spawn_online_warmup(endpoint.clone());

    let path_handle = AbortOnDrop(spawn_path_poll(
        endpoint.clone(),
        provider,
        id.to_string(),
        events.clone(),
        stop.clone(),
    ));

    let _ = events.send(ShareEnvelope {
        id: id.to_string(),
        event: ShareEvent::Connected,
    });

    let request = match format {
        BlobFormat::Raw => HashAndFormat::raw(hash),
        _ => HashAndFormat::hash_seq(hash),
    };

    let downloader = store.downloader(&endpoint);
    let mut stream = downloader
        .download(request, Shuffled::new(vec![provider]))
        .stream()
        .await?;

    let mut last_emit: Option<Instant> = None;
    while let Some(item) = stream.next().await {
        if stop.load(Ordering::Relaxed) {
            path_handle.abort();
            anyhow::bail!("cancelled");
        }
        match item {
            DownloadProgressItem::Progress(offset) => {
                emit_receive_progress(events, id, &mut last_emit, offset, total, false);
            }
            DownloadProgressItem::Error(err) => {
                path_handle.abort();
                anyhow::bail!("download failed: {err}");
            }
            DownloadProgressItem::DownloadError => {
                path_handle.abort();
                anyhow::bail!("download failed");
            }
            _ => {}
        }
    }
    emit_receive_progress(events, id, &mut last_emit, total, total, true);
    path_handle.abort();

    match format {
        BlobFormat::Raw => {
            let name = files.first().map(|f| f.name.as_str()).unwrap_or("");
            let target = dedupe_path(dest_dir.join(sanitize_rel_path(name))).await;
            if let Some(parent) = target.parent() {
                tokio::fs::create_dir_all(parent).await.ok();
            }
            store
                .blobs()
                .export_with_opts(ExportOptions {
                    hash,
                    mode: ExportMode::TryReference,
                    target,
                })
                .await?;
        }
        _ => {
            let collection = Collection::load(hash, &store).await?;
            for (name, child) in collection.iter() {
                let target = dest_dir.join(sanitize_rel_path(name));
                if let Some(parent) = target.parent() {
                    tokio::fs::create_dir_all(parent).await.ok();
                }
                let target = dedupe_path(target).await;
                store
                    .blobs()
                    .export_with_opts(ExportOptions {
                        hash: *child,
                        mode: ExportMode::TryReference,
                        target,
                    })
                    .await?;
            }
        }
    }

    let _ = store.shutdown().await;
    Ok(())
}

fn emit_receive_progress(
    events: &UnboundedSender<ShareEnvelope>,
    id: &str,
    last_emit: &mut Option<Instant>,
    transferred: u64,
    total: u64,
    force: bool,
) {
    let now = Instant::now();
    if !force && last_emit.is_some_and(|t| now.duration_since(t) < PROGRESS_EMIT_INTERVAL) {
        return;
    }
    *last_emit = Some(now);
    let _ = events.send(ShareEnvelope {
        id: id.to_string(),
        event: ShareEvent::Progress { transferred, total },
    });
}

fn spawn_path_poll(
    endpoint: Endpoint,
    remote: EndpointId,
    id: String,
    events: UnboundedSender<ShareEnvelope>,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last = PathKind::Unknown;
        loop {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            if let Some(info) = endpoint.remote_info(remote).await {
                let kind = classify_path(&info);
                if kind != PathKind::Unknown && kind != last {
                    last = kind;
                    let _ = events.send(ShareEnvelope {
                        id: id.clone(),
                        event: ShareEvent::Path { path: kind },
                    });
                }
            }
            tokio::time::sleep(PATH_POLL_INTERVAL).await;
        }
    })
}

fn classify_path(info: &iroh::endpoint::RemoteInfo) -> PathKind {
    let mut direct = false;
    let mut relay = false;
    for addr in info.addrs() {
        if !matches!(addr.usage(), TransportAddrUsage::Active) {
            continue;
        }
        if addr.addr().is_ip() {
            direct = true;
        }
        if addr.addr().is_relay() {
            relay = true;
        }
    }
    match (direct, relay) {
        (true, true) => PathKind::Mixed,
        (true, false) => PathKind::Direct,
        (false, true) => PathKind::Relay,
        (false, false) => PathKind::Unknown,
    }
}

fn collect_dir(dir: &Path, prefix: &str, out: &mut Vec<(String, PathBuf, u64)>) -> Result<()> {
    let read = std::fs::read_dir(dir).with_context(|| format!("failed to read {dir:?}"))?;
    for entry in read {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.is_empty() {
            continue;
        }
        let rel = format!("{prefix}/{name}");
        if file_type.is_dir() {
            collect_dir(&entry.path(), &rel, out)?;
        } else if file_type.is_file() {
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            out.push((rel, entry.path(), size));
        }
    }
    Ok(())
}

async fn dedupe_path(target: PathBuf) -> PathBuf {
    if !tokio::fs::try_exists(&target).await.unwrap_or(false) {
        return target;
    }
    let parent = target.parent().map(Path::to_path_buf).unwrap_or_default();
    let stem = target
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "file".to_string());
    let ext = target.extension().map(|s| s.to_string_lossy().to_string());
    for n in 1..=9999 {
        let name = match &ext {
            Some(ext) => format!("{stem} ({n}).{ext}"),
            None => format!("{stem} ({n})"),
        };
        let candidate = parent.join(name);
        if !tokio::fs::try_exists(&candidate).await.unwrap_or(false) {
            return candidate;
        }
    }
    target
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .map(|s| s.to_string_lossy().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "file".to_string())
}

fn sanitize_rel_path(name: &str) -> PathBuf {
    let mut out = PathBuf::new();
    for component in name.split(['/', '\\']) {
        let component = component.trim();
        if component.is_empty() || component == "." || component == ".." || component.contains(':')
        {
            continue;
        }
        if risuko_engine::engine::is_windows_device_name(component) {
            out.push(format!("_{component}"));
            continue;
        }
        out.push(component);
    }
    if out.as_os_str().is_empty() {
        out.push("risuko-received");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rel_path_neutralises_device_names() {
        assert_eq!(sanitize_rel_path("a/CON.txt"), PathBuf::from("a/_CON.txt"));
        assert_eq!(sanitize_rel_path("../x/y"), PathBuf::from("x/y"));
    }

    #[test]
    fn receiver_forces_final_progress_emit() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut last_emit = Some(Instant::now());
        emit_receive_progress(&tx, "t1", &mut last_emit, 100, 100, true);

        let env = rx
            .try_recv()
            .expect("forced final emit must punch through the throttle");
        assert_eq!(env.id, "t1");
        assert!(matches!(
            env.event,
            ShareEvent::Progress {
                transferred: 100,
                total: 100
            }
        ));
    }

    #[test]
    fn sweep_removes_only_transfer_dirs() {
        let dir = tempfile::tempdir().unwrap();
        for name in ["send-a", "recv-b", "keep"] {
            std::fs::create_dir(dir.path().join(name)).unwrap();
            std::fs::write(dir.path().join(name).join("f"), b"x").unwrap();
        }
        let mut stale = stale_dirs(dir.path());
        stale.sort();
        assert_eq!(
            stale,
            [dir.path().join("recv-b"), dir.path().join("send-a")]
        );
    }

    #[tokio::test]
    async fn abort_on_drop_stops_task() {
        let task = tokio::spawn(std::future::pending::<()>());
        let abort = task.abort_handle();
        drop(AbortOnDrop(task));
        tokio::task::yield_now().await;
        assert!(abort.is_finished());
    }
}
