use serde_json::{Map, Value};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use super::cookie_store::CookieStore;
use super::ed2k::kad::{KadConfig, KadHealthSnapshot, KadLookupStatus, KadService, KadState};
use super::error_code::classify_error;
use super::events::{EngineEvent, EventBroadcaster};
use super::http;
use super::media;
use super::options::EngineOptions;
use super::routing::{resolve_routing, strip_part_suffix, TaskRoutingRule};
use super::session::SessionManager;
use super::speed_limiter::{parse_speed_limit, SpeedLimiter};
use super::task::{
    generate_gid, ChunkProgress, DownloadFile, DownloadTask, Ed2kKadTaskStatus, FileUri, PeerInfo,
    TaskKind, TaskPatch, TaskStatus, UpdateTaskOutcome, UsenetRepairFailure, UsenetTaskData,
    UsenetTaskFile, UsenetTaskOptions, UsenetTaskSegment,
};
use super::torrent::{self, TorrentEngine};
use super::upload::UploadFileSnapshot;
use super::usenet::UsenetProviderProfile;
use super::usenet_transport::{ProviderConnectionCapacityRegistry, ProviderConnectionLease};
use super::STARTUP_ONLY_KEYS;
use std::collections::HashSet;

const MAGNET_METADATA_ATTEMPT_TIMEOUT_SECS: u64 = 60;
const MAGNET_METADATA_RETRY_DELAY_SECS: u64 = 15;
const MAGNET_METADATA_RETRY_MAX_DELAY_SECS: u64 = 600;
const P2P_RELOAD_CANCEL_TIMEOUT: Duration = Duration::from_secs(10);
const WORKER_EXIT_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_WORKER_WAIT: Duration = Duration::from_secs(3);

static WORKER_EPOCH: AtomicU64 = AtomicU64::new(1);

fn register_task_limiter(
    registry: &parking_lot::Mutex<HashMap<String, Arc<SpeedLimiter>>>,
    gid: &str,
    limit: u64,
) -> Arc<SpeedLimiter> {
    let limiter = Arc::new(SpeedLimiter::new(limit));
    let mut registry = registry.lock();
    registry.retain(|_, l| Arc::strong_count(l) > 1);
    registry.insert(gid.to_string(), limiter.clone());
    limiter
}

fn next_worker_epoch() -> u64 {
    WORKER_EPOCH.fetch_add(1, Ordering::Relaxed)
}

fn publish_starting_worker(
    starting: &parking_lot::Mutex<HashMap<String, (u64, CancellationToken)>>,
    gid: &str,
    epoch: u64,
    token: CancellationToken,
) {
    starting.lock().insert(gid.to_string(), (epoch, token));
}

fn clear_starting_worker(
    starting: &parking_lot::Mutex<HashMap<String, (u64, CancellationToken)>>,
    gid: &str,
    epoch: u64,
) {
    let mut guard = starting.lock();
    if guard.get(gid).map(|(e, _)| *e) == Some(epoch) {
        guard.remove(gid);
    }
}

async fn register_active_download(
    active: &Arc<RwLock<HashMap<String, ActiveDownload>>>,
    starting: &parking_lot::Mutex<HashMap<String, (u64, CancellationToken)>>,
    gid: String,
    ad: ActiveDownload,
) {
    let epoch = ad.epoch;
    active.write().await.insert(gid.clone(), ad);
    clear_starting_worker(starting, &gid, epoch);
}

fn cancel_starting_worker(
    starting: &parking_lot::Mutex<HashMap<String, (u64, CancellationToken)>>,
    gid: &str,
) {
    if let Some((_, token)) = starting.lock().get(gid).cloned() {
        token.cancel();
    }
}

fn cancel_all_starting_workers(
    starting: &parking_lot::Mutex<HashMap<String, (u64, CancellationToken)>>,
) {
    let tokens: Vec<_> = starting
        .lock()
        .values()
        .map(|(_, token)| token.clone())
        .collect();
    for token in tokens {
        token.cancel();
    }
}

async fn worker_startup_aborted(
    tasks: &Arc<RevLock>,
    gid: &str,
    cancel_token: &CancellationToken,
) -> bool {
    if cancel_token.is_cancelled() {
        return true;
    }
    let still_active = tasks
        .read()
        .await
        .iter()
        .any(|task| task.gid == gid && task.status == TaskStatus::Active);
    if still_active {
        false
    } else {
        cancel_token.cancel();
        true
    }
}

fn decode_thunder_http_uris(uris: Vec<String>) -> Result<Vec<String>, String> {
    uris.into_iter()
        .map(|uri| match torrent::decode_thunder_uri(&uri) {
            Some(decoded)
                if torrent::is_magnet_uri(&decoded)
                    || url::Url::parse(&decoded)
                        .ok()
                        .is_some_and(|parsed| matches!(parsed.scheme(), "http" | "https")) =>
            {
                Ok(decoded)
            }
            Some(_) => Err("Unsupported Thunder URI payload".to_string()),
            None if torrent::is_thunder_uri(&uri) => Err("Invalid Thunder URI".to_string()),
            None => Ok(uri),
        })
        .collect()
}

fn is_supported_http_task_uri(uri: &str) -> bool {
    url::Url::parse(uri)
        .ok()
        .is_some_and(|parsed| matches!(parsed.scheme(), "http" | "https"))
}

fn refresh_persisted_file_paths(task: &mut DownloadTask) {
    let file_count = task.files.len();
    let task_dir = task.dir.clone();
    let task_out = task.out.clone();
    for file in &mut task.files {
        let current_name = Path::new(&file.path)
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        let from_path = current_name
            .strip_suffix(".part")
            .unwrap_or(current_name.as_str());
        let display_out = if file_count == 1 && !task_out.is_empty() {
            task_out.strip_suffix(".part").unwrap_or(task_out.as_str())
        } else {
            from_path
        };
        if display_out.is_empty() {
            continue;
        }
        file.path = format!("{task_dir}/{display_out}");
    }
}

const UPDATE_RESTART_OPTION_KEYS: &[&str] = &[
    "split",
    "max-download-limit",
    "header",
    "all-proxy",
    "proxy",
    "no-proxy",
    "user-agent",
    "referer",
    "cookie",
    "max-connection-per-server",
    "min-split-size",
    "checksum",
    "ftp-user",
    "ftp-passwd",
    "sftp-private-key",
    "sftp-private-key-passphrase",
];

struct AppliedTaskPatch {
    path_changed: bool,
    uris_changed: bool,
    primary_uri_changed: bool,
    options_need_restart: bool,
    tracker_urls_to_add: Vec<String>,
}

fn apply_task_patch(
    task: &mut DownloadTask,
    normalized_uris: Option<Vec<String>>,
    normalized_dir: Option<String>,
    normalized_out: Option<String>,
    normalized_trackers: &[String],
    patch_options: Option<Map<String, Value>>,
) -> AppliedTaskPatch {
    let old_primary = task.uris.first().cloned().unwrap_or_default();
    let mut uris_changed = false;
    let mut primary_uri_changed = false;
    if let Some(uris) = normalized_uris {
        if uris != task.uris {
            uris_changed = true;
            primary_uri_changed = uris.first().cloned().unwrap_or_default() != old_primary;
            task.uris = uris.clone();
            if let Some(file) = task.files.first_mut() {
                file.uris = uris
                    .iter()
                    .enumerate()
                    .map(|(i, u)| FileUri {
                        uri: u.clone(),
                        status: if i == 0 {
                            "used".to_string()
                        } else {
                            "waiting".to_string()
                        },
                    })
                    .collect();
            } else if !uris.is_empty() {
                let display_out = task.out.strip_suffix(".part").unwrap_or(&task.out);
                let path = if !display_out.is_empty() {
                    format!("{}/{}", task.dir, display_out)
                } else {
                    uris.first().cloned().unwrap_or_default()
                };
                task.files.push(DownloadFile {
                    index: "1".into(),
                    path,
                    length: "0".into(),
                    completed_length: "0".into(),
                    selected: "true".into(),
                    uris: uris
                        .iter()
                        .enumerate()
                        .map(|(i, u)| FileUri {
                            uri: u.clone(),
                            status: if i == 0 {
                                "used".to_string()
                            } else {
                                "waiting".to_string()
                            },
                        })
                        .collect(),
                });
            }
        }
    }

    let mut path_changed = false;
    if let Some(dir) = normalized_dir {
        if dir != task.dir {
            path_changed = true;
            task.dir = dir;
        }
    }
    if let Some(out) = normalized_out {
        if out != task.out {
            path_changed = true;
            task.out = out;
        }
    }
    if path_changed {
        refresh_persisted_file_paths(task);
        task.options
            .insert("dir".into(), Value::String(task.dir.clone()));
        if !task.out.is_empty() {
            task.options
                .insert("out".into(), Value::String(task.out.clone()));
        }
    }

    let mut options_need_restart = false;
    if let Some(opts) = patch_options {
        for (k, v) in opts {
            if v.is_null() {
                if UPDATE_RESTART_OPTION_KEYS.contains(&k.as_str()) && task.options.contains_key(&k)
                {
                    options_need_restart = true;
                }
                task.options.remove(&k);
            } else {
                if UPDATE_RESTART_OPTION_KEYS.contains(&k.as_str()) {
                    let changed = task.options.get(&k) != Some(&v);
                    if changed {
                        options_need_restart = true;
                    }
                }
                if k == "dir" {
                    if let Some(s) = v.as_str() {
                        let s = s.trim();
                        if !s.is_empty() && s != task.dir {
                            path_changed = true;
                            task.dir = s.to_string();
                        }
                    }
                } else if k == "out" {
                    if let Some(s) = v.as_str() {
                        let trimmed = s.trim();
                        if !trimmed.is_empty() {
                            let s = http::sanitize_filename(trimmed);
                            if !s.is_empty() && s != task.out {
                                path_changed = true;
                                task.out = s;
                            }
                        }
                    }
                }
                task.options.insert(k, v);
            }
        }
        if path_changed {
            refresh_persisted_file_paths(task);
        }
    }

    let mut tracker_urls_to_add = Vec::new();
    if !normalized_trackers.is_empty() {
        let mut existing: HashSet<String> = HashSet::new();
        for tier in &task.bt_announce_list {
            for url in tier {
                existing.insert(url.clone());
            }
        }
        if let Some(raw) = task.options.get("bt-tracker").and_then(|v| v.as_str()) {
            existing.extend(risuko_bt::torrent::split_tracker_list(raw).map(str::to_string));
        }
        for url in normalized_trackers {
            if existing.insert(url.clone()) {
                tracker_urls_to_add.push(url.clone());
            }
        }
        if !tracker_urls_to_add.is_empty() {
            task.bt_announce_list.push(tracker_urls_to_add.clone());
            let mut merged: Vec<String> = Vec::new();
            let mut seen = HashSet::new();
            for tier in &task.bt_announce_list {
                for url in tier {
                    if seen.insert(url.clone()) {
                        merged.push(url.clone());
                    }
                }
            }
            task.options
                .insert("bt-tracker".into(), Value::String(merged.join("\n")));
        }
    }

    AppliedTaskPatch {
        path_changed,
        uris_changed,
        primary_uri_changed,
        options_need_restart,
        tracker_urls_to_add,
    }
}

fn ed2k_kad_task_status(status: &KadLookupStatus) -> Ed2kKadTaskStatus {
    let state = match status.state {
        KadState::Disabled => "disabled",
        KadState::Bootstrapping => "bootstrapping",
        KadState::Searching => "searching",
        KadState::Ready => "complete",
        KadState::Timeout => "timeout",
        KadState::Error => "error",
        KadState::Stopped => "disabled",
    };
    Ed2kKadTaskStatus {
        state: state.to_string(),
        queried_nodes: status.queried_nodes.min(u32::MAX as usize) as u32,
        discovered_sources: status.discovered_sources.min(u32::MAX as usize) as u32,
        error: status.error.clone(),
    }
}

struct ActiveDownload {
    epoch: u64,
    cancel_token: CancellationToken,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
    chunk_completed: Vec<Arc<AtomicU64>>,
    adopted_filename: Arc<parking_lot::Mutex<Option<String>>>,
    metalink_files: Vec<(usize, Counters)>,
    kad_status: Arc<parking_lot::Mutex<Option<KadLookupStatus>>>,
}

#[derive(Clone)]
struct Counters {
    cancel_token: CancellationToken,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
}

impl Counters {
    fn new(total: u64, connections: u32) -> Self {
        Self {
            cancel_token: CancellationToken::new(),
            total: Arc::new(AtomicU64::new(total)),
            completed: Arc::new(AtomicU64::new(0)),
            speed: Arc::new(AtomicU64::new(0)),
            connections: Arc::new(AtomicU32::new(connections)),
        }
    }

    fn to_active(
        &self,
        epoch: u64,
        chunk_completed: Vec<Arc<AtomicU64>>,
        adopted_filename: Arc<parking_lot::Mutex<Option<String>>>,
    ) -> ActiveDownload {
        ActiveDownload {
            epoch,
            cancel_token: self.cancel_token.clone(),
            total: self.total.clone(),
            completed: self.completed.clone(),
            speed: self.speed.clone(),
            connections: self.connections.clone(),
            chunk_completed,
            adopted_filename,
            metalink_files: Vec::new(),
            kad_status: Arc::new(parking_lot::Mutex::new(None)),
        }
    }
}

fn chunk_progress(chunks: &[Arc<AtomicU64>], total: u64) -> Vec<ChunkProgress> {
    if chunks.is_empty() {
        return Vec::new();
    }
    let split_count = chunks.len() as u64;
    let chunk_size = total / split_count;
    chunks
        .iter()
        .enumerate()
        .map(|(i, cc)| {
            let baseline = if i as u64 == split_count - 1 {
                total - chunk_size * (split_count - 1)
            } else {
                chunk_size
            };
            let completed = cc.load(Ordering::Relaxed);
            ChunkProgress {
                completed,
                total: completed.max(baseline),
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn finish_task(
    tasks: &Arc<RevLock>,
    active: &Arc<RwLock<HashMap<String, ActiveDownload>>>,
    events: &EventBroadcaster,
    gid: &str,
    worker_epoch: u64,
    proto_label: &str,
    counters: &Counters,
    result: Result<std::path::PathBuf, String>,
    on_found: impl FnOnce(&mut DownloadTask),
    on_ok: impl FnOnce(&mut DownloadTask, &Path) -> u64,
    on_err: impl FnOnce(&mut DownloadTask, &str) -> super::error_code::ErrorCode,
) {
    let (is_current, final_kad_status) = {
        let active_guard = active.read().await;
        match active_guard.get(gid) {
            Some(active_download) if active_download.epoch == worker_epoch => {
                (true, active_download.kad_status.lock().clone())
            }
            _ => (false, None),
        }
    };
    let mut tasks_guard = tasks.write().await;
    if let Some(task) = tasks_guard
        .iter_mut()
        .find(|t| t.gid == gid)
        .filter(|_| is_current)
    {
        task.total_length = counters.total.load(Ordering::Relaxed);
        task.completed_length = counters.completed.load(Ordering::Relaxed);
        task.download_speed = 0;
        if let Some(status) = final_kad_status
            .as_ref()
            .filter(|_| task.kind == TaskKind::Ed2k)
        {
            task.ed2k_kad = Some(ed2k_kad_task_status(status));
        }
        on_found(task);

        match result {
            Ok(path) => {
                if task.status != TaskStatus::Active {
                    tracing::debug!(
                        "[task:{}] {} worker finished after status became {:?}; ignoring result",
                        gid,
                        proto_label,
                        task.status
                    );
                } else {
                    let file_completed = on_ok(task, &path);
                    tracing::info!(
                        "[task:{}] {} download complete: {}",
                        gid,
                        proto_label,
                        path.display()
                    );
                    task.status = TaskStatus::Complete;
                    if task.kind != TaskKind::Usenet {
                        task.files = vec![DownloadFile {
                            index: "1".to_string(),
                            path: path.to_string_lossy().to_string(),
                            length: task.total_length.to_string(),
                            completed_length: file_completed.to_string(),
                            selected: "true".to_string(),
                            uris: task
                                .uris
                                .iter()
                                .map(|u| FileUri {
                                    uri: u.clone(),
                                    status: "used".to_string(),
                                })
                                .collect(),
                        }];
                    }
                    events.send(EngineEvent::DownloadComplete {
                        gid: gid.to_string(),
                    });
                }
            }
            Err(e) => {
                if task.status != TaskStatus::Active {
                    tracing::debug!(
                        "[task:{}] {} worker failed after status became {:?}; ignoring error",
                        gid,
                        proto_label,
                        task.status
                    );
                } else if counters.cancel_token.is_cancelled() && e.contains("cancelled") {
                    task.status = TaskStatus::Paused;
                    events.send(EngineEvent::DownloadPause {
                        gid: gid.to_string(),
                    });
                } else {
                    tracing::error!("[{}] Download failed for {}: {}", proto_label, gid, e);
                    task.status = TaskStatus::Error;
                    task.error_code = Some(on_err(task, &e).to_string());
                    task.error_message = Some(e);
                    events.send(EngineEvent::DownloadError {
                        gid: gid.to_string(),
                    });
                }
            }
        }
    }
    drop(tasks_guard);

    let mut active_guard = active.write().await;
    if active_guard.get(gid).map(|ad| ad.epoch) == Some(worker_epoch) {
        active_guard.remove(gid);
    }
}

fn finish_usenet_failure(
    task: &mut DownloadTask,
    error: &str,
    repair_failure: Option<UsenetRepairFailure>,
) -> super::error_code::ErrorCode {
    task.usenet_stage = Some("error".to_string());
    task.usenet_repair_failure = repair_failure;
    classify_error(error, "usenet")
}

fn usenet_stage_rank(stage: &str) -> u8 {
    match stage {
        "connecting" => 0,
        "fetching" => 1,
        "assembling" => 2,
        "repairing" | "verifying" => 3,
        "complete" => 4,
        "error" => 5,
        unknown => {
            tracing::warn!(stage = unknown, "unrecognised Usenet progress stage");
            0
        }
    }
}

fn should_update_usenet_stage(current: Option<&str>, next: &str) -> bool {
    current
        .map(|current| usenet_stage_rank(next) >= usenet_stage_rank(current))
        .unwrap_or(true)
}

fn metalink_all_selected_done(
    task: &DownloadTask,
    results: &[(usize, String, Result<std::path::PathBuf, String>)],
) -> bool {
    let finished: HashSet<usize> = results
        .iter()
        .filter(|(_, _, r)| r.is_ok())
        .map(|(idx, _, _)| *idx)
        .collect();
    let mut selected = task
        .files
        .iter()
        .enumerate()
        .filter(|(_, f)| f.selected != "false");
    let mut any = false;
    let all = selected.all(|(i, f)| {
        any = true;
        let len: u64 = f.length.parse().unwrap_or(0);
        let done: u64 = f.completed_length.parse().unwrap_or(0);
        finished.contains(&i) || (len > 0 && done >= len)
    });
    any && all
}

async fn metalink_finish(
    tasks: &Arc<RevLock>,
    active: &Arc<RwLock<HashMap<String, ActiveDownload>>>,
    events: &EventBroadcaster,
    gid: &str,
    worker_epoch: u64,
    file_counters: Vec<(usize, Counters)>,
    results: Vec<(usize, String, Result<std::path::PathBuf, String>)>,
) {
    // snapshot before taking tasks.write to keep the active -> tasks lock order
    let is_current = active.read().await.get(gid).map(|ad| ad.epoch) == Some(worker_epoch);
    let mut tasks_guard = tasks.write().await;
    if let Some(task) = tasks_guard
        .iter_mut()
        .find(|t| t.gid == gid)
        .filter(|_| is_current)
    {
        for (idx, c) in &file_counters {
            if let Some(f) = task.files.get_mut(*idx) {
                f.completed_length = c.completed.load(Ordering::Relaxed).to_string();
                let t = c.total.load(Ordering::Relaxed);
                if t > 0 {
                    f.length = t.to_string();
                }
            }
        }
        for (idx, _, r) in &results {
            if let (Ok(path), Some(f)) = (r, task.files.get_mut(*idx)) {
                f.path = path.to_string_lossy().to_string();
            }
        }
        metalink_rollup_totals(task);

        if task.status == TaskStatus::Active {
            let failures: Vec<(&str, &String)> = results
                .iter()
                .filter_map(|(_, name, r)| match r {
                    Err(e) if !e.contains("cancelled") => Some((name.as_str(), e)),
                    _ => None,
                })
                .collect();
            task.download_speed = 0;
            let all_done = metalink_all_selected_done(task, &results);
            if failures.is_empty() && all_done {
                task.status = TaskStatus::Complete;
                events.send(EngineEvent::DownloadComplete {
                    gid: gid.to_string(),
                });
            } else if failures.is_empty() {
                task.status = TaskStatus::Paused;
            } else {
                let names: Vec<&str> = failures.iter().map(|(n, _)| *n).collect();
                let first_err = failures[0].1.clone();
                task.status = TaskStatus::Paused;
                task.error_code = Some(classify_error(&first_err, "http").to_string());
                task.error_message = Some(format!(
                    "{} file(s) failed: {} — {}",
                    failures.len(),
                    names.join(", "),
                    first_err
                ));
                events.send(EngineEvent::DownloadError {
                    gid: gid.to_string(),
                });
            }
        }
    }
    drop(tasks_guard);

    let mut active_guard = active.write().await;
    if active_guard.get(gid).map(|ad| ad.epoch) == Some(worker_epoch) {
        active_guard.remove(gid);
    }
}

fn metalink_file_concurrency(options: &Map<String, Value>) -> usize {
    options
        .get("max-concurrent-downloads")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(5)
        .clamp(1, 64) as usize
}

fn metalink_rollup_totals(task: &mut DownloadTask) {
    let mut total = 0u64;
    let mut completed = 0u64;
    for f in task.files.iter() {
        if f.selected == "false" {
            continue;
        }
        total += f.length.parse::<u64>().unwrap_or(0);
        completed += f.completed_length.parse::<u64>().unwrap_or(0);
    }
    task.total_length = total;
    task.completed_length = completed;
}

fn metalink_checksums(options: &Map<String, Value>) -> Vec<String> {
    options
        .get("metalink-checksums")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .map(|v| v.as_str().unwrap_or("").to_string())
                .collect()
        })
        .unwrap_or_default()
}

fn normalize_select_file(value: &Value) -> Result<Option<String>, String> {
    let selection = match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Null => String::new(),
        _ => return Ok(None),
    };
    if !selection.trim().is_empty() && torrent::parse_select_file(&selection).is_none() {
        return Err(format!("Invalid select-file value: {selection}"));
    }
    Ok(Some(selection))
}

fn apply_select_file(files: &mut [DownloadFile], options: &Map<String, Value>) {
    let raw = options
        .get("select-file")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .unwrap_or("");
    if raw.is_empty() {
        for f in files.iter_mut() {
            f.selected = "true".to_string();
        }
        return;
    }
    let wanted: std::collections::HashSet<usize> = torrent::parse_select_file(raw)
        .unwrap_or_default()
        .into_iter()
        .collect();
    for (i, f) in files.iter_mut().enumerate() {
        f.selected = if wanted.contains(&i) { "true" } else { "false" }.to_string();
    }
}

struct RevLock {
    inner: RwLock<Vec<DownloadTask>>,
    rev: AtomicU64,
}

impl RevLock {
    fn new(tasks: Vec<DownloadTask>) -> Self {
        Self {
            inner: RwLock::new(tasks),
            rev: AtomicU64::new(0),
        }
    }

    fn rev(&self) -> u64 {
        self.rev.load(Ordering::Relaxed)
    }

    async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, Vec<DownloadTask>> {
        self.inner.read().await
    }

    async fn write(&self) -> RevWriteGuard<'_> {
        RevWriteGuard {
            guard: self.inner.write().await,
            rev: &self.rev,
        }
    }
}

struct RevWriteGuard<'a> {
    guard: tokio::sync::RwLockWriteGuard<'a, Vec<DownloadTask>>,
    rev: &'a AtomicU64,
}

impl std::ops::Deref for RevWriteGuard<'_> {
    type Target = Vec<DownloadTask>;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl std::ops::DerefMut for RevWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

struct UpdatingGuard {
    set: Arc<parking_lot::Mutex<HashSet<String>>>,
    gid: String,
}

impl Drop for UpdatingGuard {
    fn drop(&mut self) {
        self.set.lock().remove(&self.gid);
    }
}

impl Drop for RevWriteGuard<'_> {
    fn drop(&mut self) {
        self.rev.fetch_add(1, Ordering::Relaxed);
    }
}

pub struct TaskManager {
    config_dir: PathBuf,
    p2p_reload_lock: tokio::sync::Mutex<()>,
    p2p_route_generation: Arc<AtomicU64>,
    torrent_resume_seq: AtomicU64,
    tasks: Arc<RevLock>,
    saved_rev: AtomicU64,
    active_downloads: Arc<RwLock<HashMap<String, ActiveDownload>>>,
    starting_workers: Arc<parking_lot::Mutex<HashMap<String, (u64, CancellationToken)>>>,
    updating_tasks: Arc<parking_lot::Mutex<HashSet<String>>>,
    torrent_ids: Arc<RwLock<HashMap<String, usize>>>,
    pending_magnets: Arc<RwLock<HashSet<String>>>,
    options: Arc<RwLock<EngineOptions>>,
    events: EventBroadcaster,
    session: Arc<SessionManager>,
    save_gate: tokio::sync::Mutex<()>,
    shutting_down: std::sync::atomic::AtomicBool,
    torrent_engine: Arc<RwLock<Option<TorrentEngine>>>,
    global_speed_limiter: Arc<SpeedLimiter>,
    global_upload_limiter: Arc<SpeedLimiter>,
    task_limiters: Arc<parking_lot::Mutex<HashMap<String, Arc<SpeedLimiter>>>>,
    cookie_store: Arc<CookieStore>,
    usenet_connection_capacity: Arc<ProviderConnectionCapacityRegistry>,
    kad_runtime: Arc<parking_lot::RwLock<KadRuntime>>,
}

#[derive(Clone)]
enum KadRuntime {
    Disabled { port: u16 },
    Running(Arc<KadService>),
    Failed { port: u16, error: String },
}

impl KadRuntime {
    fn service(&self) -> Option<Arc<KadService>> {
        match self {
            Self::Running(service) => Some(service.clone()),
            Self::Disabled { .. } | Self::Failed { .. } => None,
        }
    }

    fn udp_port(&self) -> Option<u16> {
        match self {
            Self::Running(service) => service.advertised_udp_port(),
            Self::Disabled { .. } | Self::Failed { .. } => None,
        }
    }
}

fn extract_filename_from_uri(uris: &[String]) -> String {
    if uris.is_empty() {
        return String::new();
    }

    let uri = &uris[0];

    if let Ok(parsed) = url::Url::parse(uri) {
        if let Some(segment) = parsed
            .path_segments()
            .and_then(|mut segs| segs.next_back())
            .filter(|s| !s.is_empty())
        {
            let decoded = percent_encoding::percent_decode_str(segment)
                .decode_utf8_lossy()
                .into_owned();
            if !decoded.is_empty() {
                return decoded;
            }
        }
        return String::new();
    }

    if let Some(start) = uri.find("://") {
        let after_scheme = &uri[start + 3..];
        if let Some(path_start) = after_scheme.find('/') {
            let path = &after_scheme[path_start..];
            if let Some(file_part) = path.split('/').next_back() {
                if !file_part.is_empty() {
                    let file_name = file_part
                        .split('?')
                        .next()
                        .unwrap_or("")
                        .split('#')
                        .next()
                        .unwrap_or("");
                    if !file_name.is_empty() {
                        return file_name.to_string();
                    }
                }
            }
        }
    }

    String::new()
}

fn header_contains_cookie(value: &Value) -> bool {
    let lines: Vec<&str> = if let Some(s) = value.as_str() {
        s.split('\n').collect()
    } else if let Some(arr) = value.as_array() {
        arr.iter().filter_map(|v| v.as_str()).collect()
    } else {
        return false;
    };
    lines.iter().any(|l| {
        let lower = l.trim().to_ascii_lowercase();
        lower.starts_with("cookie:") || lower.starts_with("cookie ")
    })
}

fn task_requests_pause(options: &Map<String, Value>) -> bool {
    options
        .get("pause")
        .and_then(super::options::json_bool)
        .unwrap_or(false)
}

fn magnet_retry_delay(attempt: u32) -> Duration {
    let secs = MAGNET_METADATA_RETRY_DELAY_SECS
        .saturating_mul(1u64 << attempt.min(16))
        .min(MAGNET_METADATA_RETRY_MAX_DELAY_SECS);
    Duration::from_secs(secs)
}

fn is_live_magnet(task: &DownloadTask, gid: &str, uri: &str) -> bool {
    task.gid == gid
        && task.kind == TaskKind::Torrent
        && task.status == TaskStatus::Active
        && task.uris.iter().any(|u| u == uri)
}

fn parse_cf_host(msg: &str) -> Option<String> {
    let key = "host=";
    let start = msg.find(key)? + key.len();
    let rest = &msg[start..];
    let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
    let host = rest[..end].trim();
    if host.is_empty() {
        None
    } else {
        Some(host.to_lowercase())
    }
}

impl TaskManager {
    pub async fn new(
        config_dir: &Path,
        options: EngineOptions,
        events: EventBroadcaster,
    ) -> Result<Self, String> {
        let session = SessionManager::new(config_dir);
        let mut saved_tasks = session.load();

        if options.purge_record_on_start() {
            saved_tasks.retain(|t| !t.status.is_stopped());
        }

        let output_dir = options.dir();
        let p2p_proxy_result = super::torrent::p2p_proxy_from_options(&options.global);
        let p2p_proxy_invalid = p2p_proxy_result.is_err();
        let p2p_proxy = match p2p_proxy_result {
            Ok(proxy) => proxy,
            Err(error) => {
                tracing::warn!("Invalid global P2P proxy (torrent engine disabled): {error}");
                None
            }
        };
        let global_speed_limiter =
            Arc::new(SpeedLimiter::new(options.max_overall_download_limit()));
        let global_upload_limiter =
            Arc::new(SpeedLimiter::new(options.effective_bt_upload_limit()));
        let tuning = super::torrent::BtTuning::from_options(
            &options,
            global_speed_limiter.clone(),
            global_upload_limiter.clone(),
            p2p_proxy,
        );
        let torrent_engine = if p2p_proxy_invalid {
            None
        } else {
            TorrentEngine::new_with_tuning(Path::new(&output_dir), tuning)
                .await
                .map_err(|e| {
                    tracing::warn!("Torrent engine init failed (non-fatal): {}", e);
                    e
                })
                .ok()
        };

        super::dns::apply_from_options(&options.global);

        let kad_runtime = match options.ed2k_kad_port_checked() {
            Err(error) => KadRuntime::Failed {
                port: options.ed2k_kad_port(),
                error,
            },
            Ok(port) if !options.ed2k_enable_kad() => KadRuntime::Disabled { port },
            Ok(port) => match options.p2p_proxy_connector() {
                Err(error) => KadRuntime::Failed { port, error },
                Ok(connector) => {
                    let kad_config =
                        KadConfig::new(config_dir.to_path_buf(), port, options.ed2k_port())
                            .with_proxy(connector.has_proxy().then_some(connector));
                    match KadService::bind(kad_config).await {
                        Ok(service) => KadRuntime::Running(service),
                        Err(error) => KadRuntime::Failed {
                            port,
                            error: error.to_string(),
                        },
                    }
                }
            },
        };

        let manager = Self {
            tasks: Arc::new(RevLock::new(saved_tasks)),
            saved_rev: AtomicU64::new(u64::MAX),
            active_downloads: Arc::new(RwLock::new(HashMap::new())),
            starting_workers: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            updating_tasks: Arc::new(parking_lot::Mutex::new(HashSet::new())),
            torrent_ids: Arc::new(RwLock::new(HashMap::new())),
            pending_magnets: Arc::new(RwLock::new(HashSet::new())),
            options: Arc::new(RwLock::new(options)),
            events,
            session: Arc::new(session),
            save_gate: tokio::sync::Mutex::new(()),
            shutting_down: std::sync::atomic::AtomicBool::new(false),
            torrent_engine: Arc::new(RwLock::new(torrent_engine)),
            global_speed_limiter,
            global_upload_limiter,
            task_limiters: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            cookie_store: Arc::new(CookieStore::new(config_dir)),
            usenet_connection_capacity: Arc::new(ProviderConnectionCapacityRegistry::default()),
            config_dir: config_dir.to_path_buf(),
            p2p_reload_lock: tokio::sync::Mutex::new(()),
            p2p_route_generation: Arc::new(AtomicU64::new(0)),
            torrent_resume_seq: AtomicU64::new(0),
            kad_runtime: Arc::new(parking_lot::RwLock::new(kad_runtime)),
        };

        Ok(manager)
    }

    async fn resolve_routing_for_task(
        &self,
        options: &Map<String, Value>,
        filename_hint: &str,
    ) -> (String, Option<String>) {
        let opts_guard = self.options.read().await;
        let merged = opts_guard.merge_task_options(options);
        let raw_dir = merged
            .get("dir")
            .and_then(|v| v.as_str())
            .unwrap_or(".")
            .to_string();
        let rules = opts_guard.task_routing_rules();
        let file_category_dirs = opts_guard.file_category_dirs();
        drop(opts_guard);

        let decision = resolve_routing(&rules, filename_hint, &raw_dir, &file_category_dirs);
        (decision.dir, decision.tag)
    }

    fn send_download_start(&self, gid: &str) {
        self.events.send(EngineEvent::DownloadStart {
            gid: gid.to_string(),
        });
    }

    async fn enqueue(&self, mut task: DownloadTask) -> Result<String, String> {
        let gid = task.gid.clone();
        let pause = task_requests_pause(&task.options);

        let now = crate::engine::util::now_secs();
        let scheduled = task
            .options
            .get("risuko-start-at")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
            })
            .filter(|&ts| ts > now);
        if let Some(ts) = scheduled {
            task.start_at = Some(ts);
            task.status = TaskStatus::Scheduled;
        } else if pause {
            task.status = TaskStatus::Paused;
        }

        self.tasks.write().await.push(task);
        self.save_session_logged(&gid).await;

        if scheduled.is_none() && !pause {
            self.try_start_next().await;
        } else if scheduled.is_some() {
            tracing::info!(
                "[task:{}] Queued as scheduled (start_at={:?})",
                gid,
                scheduled
            );
        }

        Ok(gid)
    }

    pub async fn add_http_task(
        &self,
        uris: Vec<String>,
        options: Map<String, Value>,
    ) -> Result<String, String> {
        let uris = decode_thunder_http_uris(uris)?;
        if let Some(magnet) = uris.iter().find(|u| torrent::is_magnet_uri(u)) {
            return self.add_magnet_task(magnet, options).await;
        }

        if uris.len() == 1 && super::metalink::url_hints_metalink(&uris[0]) {
            let merged = self.options.read().await.merge_task_options(&options);
            if let Ok(bytes) = http::fetch_for_metalink_probe(&uris[0], &merged).await {
                if std::str::from_utf8(&bytes)
                    .ok()
                    .is_some_and(|text| super::metalink::parse(text).is_ok())
                {
                    tracing::info!("[metalink] following metalink URL: {}", uris[0]);
                    match self.add_metalink_task(bytes, options.clone()).await {
                        Ok(gid) => return Ok(gid),
                        Err(e) => tracing::warn!(
                            "[metalink] {} parsed but task creation failed, falling back to HTTP: {e}",
                            uris[0]
                        ),
                    }
                }
            }
        }

        if let Some(bad) = uris.iter().find(|u| !is_supported_http_task_uri(u)) {
            return Err(format!("Unsupported URI scheme for an HTTP task: {bad}"));
        }

        let gid = generate_gid();
        tracing::info!("[task:{}] Adding HTTP task, uris={:?}", gid, uris);
        let out = options
            .get("out")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let filename_hint = if out.is_empty() {
            extract_filename_from_uri(&uris)
        } else {
            out.clone()
        };

        let (dir, tag) = self
            .resolve_routing_for_task(&options, strip_part_suffix(&filename_hint))
            .await;

        self.enqueue(DownloadTask::new_http(gid, uris, dir, tag, options))
            .await
    }

    pub async fn add_media_task(
        &self,
        uri: &str,
        options: Map<String, Value>,
    ) -> Result<String, String> {
        let gid = generate_gid();
        tracing::info!("[task:{}] Adding media task, uri={}", gid, uri);
        let out = options
            .get("out")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let filename_hint = if out.is_empty() {
            String::new()
        } else {
            out.clone()
        };

        let (dir, tag) = self
            .resolve_routing_for_task(&options, &filename_hint)
            .await;

        self.enqueue(DownloadTask::new_media(
            gid,
            uri.to_string(),
            dir,
            tag,
            options,
        ))
        .await
    }

    pub async fn add_torrent_task(
        &self,
        torrent_data: Vec<u8>,
        options: Map<String, Value>,
    ) -> Result<String, String> {
        let _p2p_reload_guard = self.p2p_reload_lock.lock().await;
        let gid = generate_gid();
        let merged = self.options.read().await.merge_task_options(&options);
        let out = options
            .get("out")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let filename_hint = if out.is_empty() {
            "download.torrent".to_string()
        } else {
            out.clone()
        };

        let (dir, tag) = self
            .resolve_routing_for_task(&options, &filename_hint)
            .await;

        let mut task = DownloadTask::new_torrent(gid.clone(), dir.clone(), tag, options.clone());

        // Drop the engine lock before remember_torrent_id, which also reads torrent_engine
        let add_result = {
            let te_guard = self.torrent_engine.read().await;
            if let Some(ref te) = *te_guard {
                Some(te.add_torrent_bytes(&torrent_data, &merged).await)
            } else {
                None
            }
        };
        match add_result {
            Some(Ok(handle)) => {
                tracing::info!(
                    "Torrent task {} added: id={}, info_hash={:?}",
                    gid,
                    handle.id,
                    handle.info_hash
                );
                if handle.already_managed {
                    let owner = {
                        let ids = self.torrent_ids.read().await;
                        let tasks = self.tasks.read().await;
                        tasks
                            .iter()
                            .find(|t| {
                                t.status != TaskStatus::Removed
                                    && ids.get(&t.gid) == Some(&handle.id)
                            })
                            .map(|t| t.gid.clone())
                    };
                    if let Some(owner) = owner {
                        return Err(format!(
                            "InfoHash {} is already registered by task {owner}",
                            handle.info_hash.as_deref().unwrap_or_default()
                        ));
                    }
                }
                task.info_hash = handle.info_hash.clone();
                self.persist_torrent_file(
                    task.info_hash.as_deref().unwrap_or_default(),
                    &torrent_data,
                )
                .await;
                self.remember_torrent_id(&gid, handle.id).await;
                task.info_hash_v2 = handle.info_hash_v2;
                task.meta_version = handle.meta_version;
                task.status = TaskStatus::Active;
                if task_requests_pause(&options) {
                    let te = self.torrent_engine.read().await.clone();
                    if let Some(te) = te {
                        if let Err(e) = te.pause(handle.id).await {
                            tracing::warn!("[task:{gid}] could not pause new torrent: {e}");
                        }
                    }
                    task.status = TaskStatus::Paused;
                }
            }
            Some(Err(e)) => {
                tracing::error!("Torrent task {} failed to add: {}", gid, e);
                task.status = TaskStatus::Error;
                task.error_code = Some(classify_error(&e, "torrent").to_string());
                task.error_message = Some(e);
            }
            None => {
                tracing::error!("Torrent task {} failed: engine not available", gid);
                task.status = TaskStatus::Error;
                task.error_code =
                    Some(super::error_code::ErrorCode::ENGINE_NOT_RUNNING.to_string());
                task.error_message = Some("Torrent engine not available".to_string());
            }
        }

        let failed = task.status == TaskStatus::Error;
        self.tasks.write().await.push(task);
        self.save_session_logged(&gid).await;
        if !failed {
            self.send_download_start(&gid);
        }

        Ok(gid)
    }

    pub async fn add_metalink_task(
        &self,
        meta4: Vec<u8>,
        options: Map<String, Value>,
    ) -> Result<String, String> {
        let xml = String::from_utf8(meta4).map_err(|e| format!("metalink is not UTF-8: {e}"))?;
        let files = super::metalink::parse(&xml)?;

        let gid = generate_gid();
        let filename_hint = files.first().map(|f| f.name.clone()).unwrap_or_default();
        let (dir, tag) = self
            .resolve_routing_for_task(&options, &filename_hint)
            .await;

        let mut download_files = Vec::with_capacity(files.len());
        let mut checksums = Vec::with_capacity(files.len());
        for (i, f) in files.iter().enumerate() {
            download_files.push(DownloadFile {
                index: (i + 1).to_string(),
                path: format!("{}/{}", dir, f.name),
                length: "0".to_string(),
                completed_length: "0".to_string(),
                selected: "true".to_string(),
                uris: f
                    .uris
                    .iter()
                    .map(|u| FileUri {
                        uri: u.clone(),
                        status: "waiting".to_string(),
                    })
                    .collect(),
            });
            checksums.push(Value::String(
                f.checksum
                    .as_ref()
                    .map(|c| format!("{}:{}", c.algo.name(), c.hex))
                    .unwrap_or_default(),
            ));
        }

        let mut options = options;
        apply_select_file(&mut download_files, &options);
        options.insert("metalink-checksums".to_string(), Value::Array(checksums));

        tracing::info!(
            "[task:{}] Adding Metalink task, {} file(s)",
            gid,
            download_files.len()
        );
        self.enqueue(DownloadTask::new_metalink(
            gid,
            dir,
            tag,
            options,
            download_files,
        ))
        .await
    }

    pub async fn add_nzb_task(
        &self,
        nzb: Vec<u8>,
        options: Map<String, Value>,
    ) -> Result<String, String> {
        let document = super::usenet::parse(&nzb)?;
        let filename_hint = document
            .title
            .clone()
            .or_else(|| document.files.first().map(|file| file.name.clone()))
            .unwrap_or_else(|| "usenet".to_string());
        let (dir, tag) = self
            .resolve_routing_for_task(&options, &filename_hint)
            .await;
        let files: Vec<DownloadFile> = document
            .files
            .iter()
            .enumerate()
            .map(|(index, file)| {
                let length = file
                    .segments
                    .iter()
                    .try_fold(0u64, |total, segment| total.checked_add(segment.bytes))
                    .ok_or_else(|| format!("NZB file {:?} byte count overflowed", file.name))?;
                Ok(DownloadFile {
                    index: (index + 1).to_string(),
                    path: Path::new(&dir)
                        .join(super::util::safe_filename(&file.name, "download"))
                        .to_string_lossy()
                        .to_string(),
                    length: length.to_string(),
                    completed_length: "0".to_string(),
                    selected: "true".to_string(),
                    uris: Vec::new(),
                })
            })
            .collect::<Result<_, String>>()?;
        let gid = generate_gid();
        let mut options = options;
        for key in [
            "username",
            "password",
            "usenet-username",
            "usenet-password",
            "archive-password",
            "usenet-archive-password",
        ] {
            options.remove(key);
        }
        for key in ["usenet-profiles", "usenetProfiles"] {
            if let Some(value) = options.get_mut(key) {
                let profiles: Vec<UsenetProviderProfile> = serde_json::from_value(value.clone())
                    .map_err(|error| format!("Invalid Usenet provider profiles: {error}"))?;
                *value = serde_json::to_value(profiles)
                    .map_err(|error| format!("Invalid Usenet provider profiles: {error}"))?;
            }
        }
        if let Some(value) = options.get("usenet-archive-limits") {
            let defaults = if cfg!(target_os = "android") {
                super::archive_safety::ArchiveLimits::android_defaults()
            } else {
                super::archive_safety::ArchiveLimits::desktop_defaults()
            };
            let confirmed = options
                .get("usenet-archive-limit-override-confirmed")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            super::archive_safety::validate_limits_override_value(defaults, value, confirmed)
                .map_err(|error| format!("Invalid Usenet archive limits: {error:?}"))?;
        }
        options.insert(
            "usenet-nzb-bytes".to_string(),
            Value::Number((nzb.len() as u64).into()),
        );
        options.insert(
            "usenet-title".to_string(),
            document
                .title
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        );
        options.insert(
            "usenet-segment-count".to_string(),
            Value::Number(
                document
                    .files
                    .iter()
                    .map(|file| file.segments.len() as u64)
                    .sum::<u64>()
                    .into(),
            ),
        );
        let usenet_options = UsenetTaskOptions {
            profile_id: options
                .get("usenet-profile-id")
                .and_then(Value::as_str)
                .map(str::to_string),
            cleanup_mode: options
                .get("usenet-cleanup-mode")
                .and_then(Value::as_str)
                .map(str::to_string),
            archive_limits: options.get("usenet-archive-limits").cloned(),
            archive_limit_override_confirmed: options
                .get("usenet-archive-limit-override-confirmed")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        };
        let metadata = UsenetTaskData {
            options: usenet_options,
            files: document
                .files
                .iter()
                .map(|file| UsenetTaskFile {
                    name: file.name.clone(),
                    subject: file.subject.clone(),
                    groups: file.groups.clone(),
                    segments: file
                        .segments
                        .iter()
                        .map(|segment| UsenetTaskSegment {
                            number: segment.number,
                            bytes: segment.bytes,
                            message_id: segment.message_id.clone(),
                        })
                        .collect(),
                })
                .collect(),
        };
        let names: Vec<String> = document
            .files
            .iter()
            .map(|file| super::util::safe_filename(&file.name, "download"))
            .collect();
        let deferred = super::usenet::deferred_par2_volumes(&names);
        let initial_bytes = metadata
            .files
            .iter()
            .enumerate()
            .filter(|(index, _)| !deferred.contains(index))
            .flat_map(|(_, file)| file.segments.iter())
            .fold(0u64, |sum, segment| sum.saturating_add(segment.bytes));
        let mut task = DownloadTask::new_usenet(gid, dir, tag, document.title, options, files)
            .with_usenet_data(metadata);
        task.total_length = initial_bytes;
        self.enqueue(task).await
    }

    pub fn try_acquire_usenet_profile_connection(
        &self,
        profile: &super::usenet::UsenetProviderProfile,
    ) -> Result<Option<ProviderConnectionLease>, String> {
        self.usenet_connection_capacity
            .try_acquire(profile)
            .map_err(|error| error.to_string())
    }

    pub async fn add_magnet_task(
        &self,
        magnet_uri: &str,
        options: Map<String, Value>,
    ) -> Result<String, String> {
        let _p2p_reload_guard = self.p2p_reload_lock.lock().await;
        let gid = generate_gid();
        let merged = self.options.read().await.merge_task_options(&options);
        let out = options
            .get("out")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let filename_hint = if out.is_empty() {
            "download.magnet".to_string()
        } else {
            out.clone()
        };

        let (dir, tag) = self
            .resolve_routing_for_task(&options, &filename_hint)
            .await;

        let mut task = DownloadTask::new_torrent(gid.clone(), dir.clone(), tag, options.clone());
        task.uris = vec![magnet_uri.to_string()];

        let should_spawn_resolver = match torrent::inspect_magnet(magnet_uri) {
            Ok(info) => {
                task.info_hash = Some(info.info_hash);
                task.info_hash_v2 = info.info_hash_v2;
                task.bt_name = info.display_name;
                task.status = if task_requests_pause(&options) {
                    TaskStatus::Paused
                } else {
                    TaskStatus::Active
                };
                true
            }
            Err(e) => {
                task.status = TaskStatus::Error;
                task.error_code = Some(classify_error(&e, "torrent").to_string());
                task.error_message = Some(e);
                false
            }
        };

        let should_start_resolver = should_spawn_resolver && task.status == TaskStatus::Active;
        let failed = task.status == TaskStatus::Error;
        self.tasks.write().await.push(task);
        self.save_session_logged(&gid).await;
        if should_start_resolver {
            self.spawn_magnet_metadata_resolver(
                gid.clone(),
                magnet_uri.to_string(),
                merged.clone(),
            )
            .await;
        }
        if !failed {
            self.send_download_start(&gid);
        }

        Ok(gid)
    }

    pub async fn resolve_magnet_metadata(
        &self,
        magnet_uri: &str,
        options: Map<String, Value>,
        timeout_secs: u64,
    ) -> Result<Vec<torrent::TorrentFileInfo>, String> {
        let (engine, merged, generation) = {
            let _p2p_reload_guard = self.p2p_reload_lock.lock().await;
            let merged = self.options.read().await.merge_task_options(&options);
            let Some(engine) = self.torrent_engine.read().await.clone() else {
                return Err("Torrent engine not available".to_string());
            };
            let generation = engine.magnet_route_generation().await;
            (engine, merged, generation)
        };
        engine
            .resolve_magnet_at_generation(magnet_uri, &merged, timeout_secs, generation)
            .await
    }

    async fn spawn_magnet_metadata_resolver(
        &self,
        gid: String,
        magnet_uri: String,
        options: Map<String, Value>,
    ) {
        {
            let mut guard = self.pending_magnets.write().await;
            if !guard.insert(gid.clone()) {
                return;
            }
        }

        let pending = self.pending_magnets.clone();
        let tasks = self.tasks.clone();
        let torrent_ids = self.torrent_ids.clone();
        let torrent_engine = self.torrent_engine.clone();
        let events = self.events.clone();
        let p2p_route_generation = self.p2p_route_generation.clone();
        let expected_route_generation =
            p2p_route_generation.load(std::sync::atomic::Ordering::Acquire);
        let expected_engine_generation = match torrent_engine.read().await.clone() {
            Some(engine) => Some(engine.magnet_route_generation().await),
            None => None,
        };

        tokio::spawn(async move {
            let mut failed_attempts: u32 = 0;
            loop {
                if p2p_route_generation.load(Ordering::Acquire) != expected_route_generation {
                    break;
                }

                let still_active = {
                    let guard = tasks.read().await;
                    guard
                        .iter()
                        .any(|task| is_live_magnet(task, &gid, &magnet_uri))
                };
                if !still_active {
                    break;
                }

                if p2p_route_generation.load(Ordering::Acquire) != expected_route_generation {
                    break;
                }

                let engine = torrent_engine.read().await.clone();
                let Some(engine) = engine else {
                    let mut guard = tasks.write().await;
                    if let Some(task) = guard
                        .iter_mut()
                        .find(|task| is_live_magnet(task, &gid, &magnet_uri))
                    {
                        task.status = TaskStatus::Error;
                        task.error_code =
                            Some(super::error_code::ErrorCode::ENGINE_NOT_RUNNING.to_string());
                        task.error_message = Some("Torrent engine not available".to_string());
                        events.send(EngineEvent::DownloadError { gid: gid.clone() });
                    }
                    break;
                };

                if p2p_route_generation.load(Ordering::Acquire) != expected_route_generation {
                    break;
                }

                let mut resolve_options = options.clone();
                {
                    let guard = tasks.read().await;
                    if let Some(task) = guard
                        .iter()
                        .find(|task| is_live_magnet(task, &gid, &magnet_uri))
                    {
                        if let Some(trackers) = task.options.get("bt-tracker") {
                            resolve_options.insert("bt-tracker".to_string(), trackers.clone());
                        }
                        match task.options.get("select-file") {
                            Some(selection) => {
                                resolve_options
                                    .insert("select-file".to_string(), selection.clone());
                            }
                            None => {
                                resolve_options.remove("select-file");
                            }
                        }
                    }
                }

                let result = match expected_engine_generation {
                    Some(engine_generation) => {
                        engine
                            .resolve_and_add_magnet_at_generation(
                                &magnet_uri,
                                &resolve_options,
                                MAGNET_METADATA_ATTEMPT_TIMEOUT_SECS,
                                engine_generation,
                            )
                            .await
                    }
                    None => Err("Torrent engine route unavailable".to_string()),
                };

                match result {
                    Ok(handle) => {
                        if p2p_route_generation.load(Ordering::Acquire) != expected_route_generation
                        {
                            if !handle.already_managed {
                                let _ = engine.remove(handle.id, false).await;
                            }
                            break;
                        }

                        if handle.already_managed {
                            let other_gids: Vec<String> = torrent_ids
                                .read()
                                .await
                                .iter()
                                .filter(|(g, id)| **id == handle.id && **g != gid)
                                .map(|(g, _)| g.clone())
                                .collect();
                            let mut guard = tasks.write().await;
                            let duplicate = guard.iter().any(|t| {
                                other_gids.contains(&t.gid) && t.status != TaskStatus::Removed
                            });
                            if duplicate {
                                if let Some(task) = guard
                                    .iter_mut()
                                    .find(|task| is_live_magnet(task, &gid, &magnet_uri))
                                {
                                    let msg = "InfoHash is already registered".to_string();
                                    task.status = TaskStatus::Error;
                                    task.error_code =
                                        Some(classify_error(&msg, "torrent").to_string());
                                    task.error_message = Some(msg);
                                    events.send(EngineEvent::DownloadError { gid: gid.clone() });
                                }
                                break;
                            }
                        }

                        let mut attached = false;
                        {
                            let mut guard = tasks.write().await;
                            if let Some(task) = guard
                                .iter_mut()
                                .find(|task| is_live_magnet(task, &gid, &magnet_uri))
                            {
                                if let Some(info_hash) = handle.info_hash.clone() {
                                    task.info_hash = Some(info_hash);
                                }
                                task.info_hash_v2 = handle.info_hash_v2.clone();
                                task.meta_version = handle.meta_version.clone();
                                task.error_code = None;
                                task.error_message = None;
                                if let Some(selection) = handle.select_file.clone() {
                                    task.options
                                        .entry("select-file".to_string())
                                        .or_insert(Value::String(selection));
                                }
                                attached = true;
                            }
                        }

                        if attached {
                            let still_live = {
                                let guard = tasks.read().await;
                                guard
                                    .iter()
                                    .any(|task| is_live_magnet(task, &gid, &magnet_uri))
                            };
                            if still_live {
                                Self::remember_torrent_id_in(
                                    &torrent_engine,
                                    &torrent_ids,
                                    &tasks,
                                    &gid,
                                    handle.id,
                                )
                                .await;
                                let status_now = tasks
                                    .read()
                                    .await
                                    .iter()
                                    .find(|task| task.gid == gid)
                                    .map(|task| task.status);
                                match status_now {
                                    Some(TaskStatus::Active) => {}
                                    Some(TaskStatus::Paused) => {
                                        let _ = engine.pause(handle.id).await;
                                    }
                                    _ => {
                                        torrent_ids.write().await.remove(&gid);
                                        if !handle.already_managed {
                                            let _ = engine.remove(handle.id, false).await;
                                        }
                                        break;
                                    }
                                }
                                let applied = resolve_options
                                    .get("select-file")
                                    .cloned()
                                    .or_else(|| handle.select_file.clone().map(Value::String));
                                let latest = {
                                    let guard = tasks.read().await;
                                    guard
                                        .iter()
                                        .find(|task| task.gid == gid)
                                        .and_then(|task| task.options.get("select-file").cloned())
                                };
                                if latest != applied {
                                    let raw = latest.as_ref().and_then(Value::as_str);
                                    if let Err(e) = engine.set_select_file(handle.id, raw).await {
                                        tracing::warn!(
                                            "[task:{gid}] could not apply file selection: {e}"
                                        );
                                    }
                                }
                                let tracker_urls = {
                                    let guard = tasks.read().await;
                                    guard
                                        .iter()
                                        .find(|task| task.gid == gid)
                                        .map(|task| {
                                            let mut seen = HashSet::new();
                                            let mut urls = Vec::new();
                                            for tier in &task.bt_announce_list {
                                                for url in tier {
                                                    if seen.insert(url.clone()) {
                                                        urls.push(url.clone());
                                                    }
                                                }
                                            }
                                            urls
                                        })
                                        .unwrap_or_default()
                                };
                                if !tracker_urls.is_empty() {
                                    if let Err(e) =
                                        engine.add_trackers(handle.id, tracker_urls).await
                                    {
                                        tracing::warn!(
                                            "[task:{}] Failed to replay persisted magnet trackers: {e}",
                                            gid
                                        );
                                    }
                                }
                            } else if !handle.already_managed {
                                let _ = engine.remove(handle.id, false).await;
                            }
                        } else if !handle.already_managed {
                            let _ = engine.remove(handle.id, false).await;
                        }
                        break;
                    }
                    Err(e) => {
                        if e == "P2P proxy profile changed; magnet resolution cancelled" {
                            break;
                        }
                        if !is_retryable_magnet_resolution_error(&e) {
                            let mut guard = tasks.write().await;
                            if let Some(task) = guard
                                .iter_mut()
                                .find(|task| is_live_magnet(task, &gid, &magnet_uri))
                            {
                                task.status = TaskStatus::Error;
                                task.error_code = Some(classify_error(&e, "torrent").to_string());
                                task.error_message = Some(e);
                                events.send(EngineEvent::DownloadError { gid: gid.clone() });
                            }
                            break;
                        }
                        let deadline =
                            tokio::time::Instant::now() + magnet_retry_delay(failed_attempts);
                        failed_attempts = failed_attempts.saturating_add(1);
                        while tokio::time::Instant::now() < deadline {
                            tokio::time::sleep(Duration::from_secs(5)).await;
                            let live = tasks
                                .read()
                                .await
                                .iter()
                                .any(|task| is_live_magnet(task, &gid, &magnet_uri));
                            if !live {
                                break;
                            }
                        }
                    }
                }
            }

            pending.write().await.remove(&gid);
        });
    }

    pub async fn add_ed2k_task(
        &self,
        uri: &str,
        options: Map<String, Value>,
    ) -> Result<String, String> {
        let link = super::ed2k::parse_ed2k_link(uri)?;
        let gid = generate_gid();
        let file_name = link.file_name.clone();
        let file_size = link.file_size;
        let (dir, tag) = self.resolve_routing_for_task(&options, &file_name).await;

        self.enqueue(DownloadTask::new_ed2k(
            gid,
            uri.to_string(),
            file_name,
            file_size,
            dir,
            tag,
            options,
        ))
        .await
    }

    pub async fn add_m3u8_task(
        &self,
        uri: &str,
        options: Map<String, Value>,
    ) -> Result<String, String> {
        let gid = generate_gid();

        let out = options
            .get("out")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .unwrap_or_else(|| infer_m3u8_output_name(uri));
        let (dir, tag) = self.resolve_routing_for_task(&options, &out).await;

        self.enqueue(DownloadTask::new_m3u8(
            gid,
            uri.to_string(),
            out,
            dir,
            tag,
            options,
        ))
        .await
    }

    pub async fn add_ftp_task(
        &self,
        uri: &str,
        options: Map<String, Value>,
    ) -> Result<String, String> {
        let gid = generate_gid();
        let out = options
            .get("out")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let filename_hint = if out.is_empty() {
            extract_filename_from_uri(&[uri.to_string()])
        } else {
            out.clone()
        };

        let (dir, tag) = self
            .resolve_routing_for_task(&options, strip_part_suffix(&filename_hint))
            .await;

        self.enqueue(DownloadTask::new_ftp(
            gid,
            uri.to_string(),
            dir,
            tag,
            options,
        ))
        .await
    }

    pub async fn add_legacy_p2p_task(
        &self,
        kind: TaskKind,
        uri: &str,
        options: Map<String, Value>,
    ) -> Result<String, String> {
        let (out_hint, size_hint) = match kind {
            TaskKind::Adc => super::adc::parse_dchub_file_uri(uri)
                .map(|p| (p.file_name, p.file_size))
                .unwrap_or_default(),
            TaskKind::Gnutella => super::gnutella::parse_gnutella_uri(uri)
                .map(|p| (p.file_name, p.file_size))
                .unwrap_or_default(),
            TaskKind::G2 => super::g2::parse_g2_uri(uri)
                .map(|p| (p.file_name, p.file_size))
                .unwrap_or_default(),
            TaskKind::Gift => super::gift::parse_gift_uri(uri)
                .map(|p| (super::gift::extract_gift_name(&p.inner), 0u64))
                .unwrap_or_default(),
            _ => (String::new(), 0),
        };

        let gid = generate_gid();
        let (dir, tag) = self.resolve_routing_for_task(&options, &out_hint).await;

        self.enqueue(DownloadTask::new_simple_protocol(
            gid,
            kind,
            uri.to_string(),
            out_hint,
            size_hint,
            dir,
            tag,
            options,
        ))
        .await
    }

    pub async fn list_routing_rules(&self) -> Vec<TaskRoutingRule> {
        self.options.read().await.task_routing_rules()
    }

    pub async fn add_routing_rule(
        &self,
        mut rule: TaskRoutingRule,
    ) -> Result<TaskRoutingRule, String> {
        if rule.id.is_empty() {
            rule.id = uuid::Uuid::new_v4().to_string();
        }
        let mut opts = self.options.write().await;
        let mut rules = opts.task_routing_rules();
        rules.push(rule.clone());
        let value = serde_json::to_value(rules)
            .map_err(|e| format!("Failed to serialize routing rules: {e}"))?;
        opts.set("task-routing-rules".to_string(), value);
        Ok(rule)
    }

    pub async fn update_routing_rule(&self, rule: TaskRoutingRule) -> Result<(), String> {
        let mut opts = self.options.write().await;
        let mut rules = opts.task_routing_rules();
        let pos = rules.iter().position(|r| r.id == rule.id);
        match pos {
            Some(idx) => {
                rules[idx] = rule;
                let value = serde_json::to_value(rules)
                    .map_err(|e| format!("Failed to serialize routing rules: {e}"))?;
                opts.set("task-routing-rules".to_string(), value);
                Ok(())
            }
            None => Err("Rule not found".to_string()),
        }
    }

    pub async fn remove_routing_rule(&self, id: &str) -> Result<(), String> {
        let mut opts = self.options.write().await;
        let mut rules = opts.task_routing_rules();
        let pos = rules.iter().position(|r| r.id == id);
        match pos {
            Some(idx) => {
                rules.remove(idx);
                let value = serde_json::to_value(rules)
                    .map_err(|e| format!("Failed to serialize routing rules: {e}"))?;
                opts.set("task-routing-rules".to_string(), value);
                Ok(())
            }
            None => Err("Rule not found".to_string()),
        }
    }

    pub async fn preview_routing(&self, filename: &str) -> super::routing::RoutingDecision {
        let opts = self.options.read().await;
        let raw_dir = opts.dir();
        let rules = opts.task_routing_rules();
        let file_category_dirs = opts.file_category_dirs();
        drop(opts);
        resolve_routing(&rules, filename, &raw_dir, &file_category_dirs)
    }

    fn spawn_legacy_p2p_download(&self, task: &DownloadTask) {
        let gid = task.gid.clone();
        let uri = task.uris.first().cloned().unwrap_or_default();
        let dir = task.dir.clone();
        let kind = task.kind;
        let task_options = task.options.clone();
        let events = self.events.clone();
        let tasks = self.tasks.clone();
        let active = self.active_downloads.clone();
        let options = self.options.clone();

        let proto_label = match kind {
            TaskKind::Adc => "adc",
            TaskKind::Gnutella => "gnutella",
            TaskKind::G2 => "g2",
            TaskKind::Gift => "gift",
            _ => "p2p",
        };

        let counters = Counters::new(task.total_length, 0);
        let worker_epoch = next_worker_epoch();
        publish_starting_worker(
            &self.starting_workers,
            &gid,
            worker_epoch,
            counters.cancel_token.clone(),
        );
        let starting = self.starting_workers.clone();
        tokio::spawn(async move {
            register_active_download(
                &active,
                &starting,
                gid.clone(),
                counters.to_active(
                    worker_epoch,
                    Vec::new(),
                    Arc::new(parking_lot::Mutex::new(None)),
                ),
            )
            .await;

            let still_active = tasks
                .read()
                .await
                .iter()
                .any(|task| task.gid == gid && task.status == TaskStatus::Active);
            if !still_active {
                counters.cancel_token.cancel();
                finish_task(
                    &tasks,
                    &active,
                    &events,
                    &gid,
                    worker_epoch,
                    proto_label,
                    &counters,
                    Err("cancelled during P2P proxy reload".to_string()),
                    |_| {},
                    |task, _| task.total_length,
                    |_, e| classify_error(e, proto_label),
                )
                .await;
                return;
            }

            let opts_snapshot = {
                let runtime_opts = options.read().await.clone();
                let merged = runtime_opts.merge_task_options(&task_options);
                EngineOptions { global: merged }
            };

            let c = counters.clone();
            let download_result = match kind {
                TaskKind::Adc => {
                    super::adc::run_adc_download(
                        &uri,
                        &dir,
                        &opts_snapshot,
                        c.total,
                        c.completed,
                        c.speed,
                        c.connections,
                        c.cancel_token,
                    )
                    .await
                }
                TaskKind::Gnutella => {
                    super::gnutella::run_gnutella_download(
                        &uri,
                        &dir,
                        &opts_snapshot,
                        c.total,
                        c.completed,
                        c.speed,
                        c.connections,
                        c.cancel_token,
                    )
                    .await
                }
                TaskKind::G2 => super::g2::run_g2_download(
                    &uri,
                    &dir,
                    &opts_snapshot,
                    c.total,
                    c.completed,
                    c.speed,
                    c.connections,
                    c.cancel_token,
                )
                .await
                .map_err(|e| e.to_string()),
                TaskKind::Gift => {
                    super::gift::run_gift_download(
                        &uri,
                        &dir,
                        &opts_snapshot,
                        c.total,
                        c.completed,
                        c.speed,
                        c.connections,
                        c.cancel_token,
                    )
                    .await
                }
                _ => Err("Unsupported protocol".to_string()),
            };

            finish_task(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                proto_label,
                &counters,
                download_result,
                |_| {},
                |task, _| task.total_length,
                |_, e| classify_error(e, proto_label),
            )
            .await;
        });
    }

    fn spawn_usenet_download(&self, task: &DownloadTask, merged_options: Map<String, Value>) {
        let gid = task.gid.clone();
        let task_snapshot = DownloadTask {
            gid: task.gid.clone(),
            dir: task.dir.clone(),
            usenet: task.usenet.clone(),
            ..Default::default()
        };
        let tasks = self.tasks.clone();
        let active = self.active_downloads.clone();
        let events = self.events.clone();
        let connection_capacity = self.usenet_connection_capacity.clone();
        let throttle = risuko_bt::limiter::Throttle::new(
            self.global_speed_limiter.clone(),
            self.task_download_limiter(
                &gid,
                super::torrent::task_limit(&merged_options, "max-download-limit"),
            ),
        );
        let output_paths = Arc::new(parking_lot::Mutex::new(HashMap::<usize, PathBuf>::new()));
        let output_paths_for_worker = output_paths.clone();
        let counters = Counters::new(task.total_length, 0);
        let worker_epoch = next_worker_epoch();
        publish_starting_worker(
            &self.starting_workers,
            &gid,
            worker_epoch,
            counters.cancel_token.clone(),
        );
        let starting = self.starting_workers.clone();
        tokio::spawn(async move {
            register_active_download(
                &active,
                &starting,
                gid.clone(),
                counters.to_active(
                    worker_epoch,
                    Vec::new(),
                    Arc::new(parking_lot::Mutex::new(None)),
                ),
            )
            .await;
            if worker_startup_aborted(&tasks, &gid, &counters.cancel_token).await {
                finish_task(
                    &tasks,
                    &active,
                    &events,
                    &gid,
                    worker_epoch,
                    "usenet",
                    &counters,
                    Err("cancelled".to_string()),
                    |_| {},
                    |task, _| task.total_length,
                    |task, error| finish_usenet_failure(task, error, None),
                )
                .await;
                return;
            }
            let result = super::usenet_worker::run_usenet_download_with_resolver_and_capacity(
                &task_snapshot,
                &merged_options,
                counters.completed.clone(),
                counters.total.clone(),
                counters.cancel_token.clone(),
                super::usenet_credential_resolver().await,
                connection_capacity,
                throttle,
                Some(Arc::new({
                    let tasks = tasks.clone();
                    let gid = gid.clone();
                    move |stage: &str| {
                        let tasks = tasks.clone();
                        let gid = gid.clone();
                        let stage = stage.to_string();
                        tokio::spawn(async move {
                            let mut tasks = tasks.write().await;
                            if let Some(task) = tasks.iter_mut().find(|task| task.gid == gid) {
                                if task.status == TaskStatus::Active
                                    && should_update_usenet_stage(
                                        task.usenet_stage.as_deref(),
                                        &stage,
                                    )
                                {
                                    task.usenet_stage = Some(stage);
                                }
                            }
                        });
                    }
                })),
            )
            .await;
            let repair_failure = result
                .as_ref()
                .err()
                .and_then(|error| error.repair_failure().cloned());
            let result = result
                .map(|(path, outputs)| {
                    *output_paths_for_worker.lock() = outputs.into_iter().collect();
                    path
                })
                .map_err(|error| error.to_string());
            finish_task(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                "usenet",
                &counters,
                result,
                |_| {},
                |task, path| {
                    task.usenet_repair_failure = None;
                    let output_paths = output_paths.lock().clone();
                    if let Some(metadata) = task.usenet.as_ref() {
                        task.usenet_stage = Some("complete".to_string());
                        task.files = metadata
                            .files
                            .iter()
                            .enumerate()
                            .filter_map(|(index, file)| {
                                let path = output_paths.get(&index).cloned()?;
                                if !path.exists() {
                                    return None;
                                }
                                Some(DownloadFile {
                                    index: (index + 1).to_string(),
                                    path: path.to_string_lossy().to_string(),
                                    length: file
                                        .segments
                                        .iter()
                                        .map(|segment| segment.bytes)
                                        .sum::<u64>()
                                        .to_string(),
                                    completed_length: file
                                        .segments
                                        .iter()
                                        .map(|segment| segment.bytes)
                                        .sum::<u64>()
                                        .to_string(),
                                    selected: "true".to_string(),
                                    uris: Vec::new(),
                                })
                            })
                            .collect();
                    }
                    if task.files.is_empty() {
                        path.metadata().map(|metadata| metadata.len()).unwrap_or(0)
                    } else {
                        task.files
                            .iter()
                            .filter_map(|file| file.completed_length.parse::<u64>().ok())
                            .sum()
                    }
                },
                |task, error| finish_usenet_failure(task, error, repair_failure),
            )
            .await;
        });
    }

    async fn try_start_next(&self) {
        let _p2p_reload_guard = self.p2p_reload_lock.lock().await;
        self.try_start_next_unlocked().await;
    }

    async fn try_start_next_unlocked(&self) {
        if self.shutting_down.load(Ordering::Acquire) {
            return;
        }
        let max_concurrent = self.options.read().await.max_concurrent_downloads();
        let (active_count, busy_gids) = {
            let active = self.active_downloads.read().await;
            let mut busy_gids = active.keys().cloned().collect::<HashSet<_>>();
            busy_gids.extend(self.starting_workers.lock().keys().cloned());
            (busy_gids.len(), busy_gids)
        };

        if active_count >= max_concurrent {
            return;
        }

        let has_startable = {
            let tasks = self.tasks.read().await;
            let updating = self.updating_tasks.lock();
            tasks.iter().any(|t| {
                t.status == TaskStatus::Waiting
                    && !busy_gids.contains(&t.gid)
                    && !updating.contains(&t.gid)
            })
        };
        if !has_startable {
            return;
        }
        let options_snapshot = self.options.read().await.clone();

        let slots = max_concurrent - active_count;
        let mut tasks = self.tasks.write().await;
        let mut started = 0;

        for task in tasks.iter_mut() {
            if started >= slots {
                break;
            }
            if task.status != TaskStatus::Waiting {
                continue;
            }
            if busy_gids.contains(&task.gid) || self.updating_tasks.lock().contains(&task.gid) {
                continue;
            }
            if task.kind == TaskKind::Http && !task.uris.is_empty() {
                task.status = TaskStatus::Active;
                let mut merged = options_snapshot.merge_task_options(&task.options);
                self.apply_stored_cookies(&task.uris, &mut merged);
                self.spawn_http_download(task, merged);
                self.send_download_start(&task.gid);
                started += 1;
            } else if task.kind == TaskKind::Media && !task.uris.is_empty() {
                task.status = TaskStatus::Active;
                let mut merged = options_snapshot.merge_task_options(&task.options);
                self.apply_stored_cookies(&task.uris, &mut merged);
                self.spawn_media_download(task, merged);
                self.send_download_start(&task.gid);
                started += 1;
            } else if task.kind == TaskKind::M3u8 && !task.uris.is_empty() {
                task.status = TaskStatus::Active;
                let merged = options_snapshot.merge_task_options(&task.options);
                self.spawn_m3u8_download(task, merged);
                self.send_download_start(&task.gid);
                started += 1;
            } else if task.kind == TaskKind::Ed2k && !task.uris.is_empty() {
                task.status = TaskStatus::Active;
                self.spawn_ed2k_download(task);
                self.send_download_start(&task.gid);
                started += 1;
            } else if task.kind == TaskKind::Ftp && !task.uris.is_empty() {
                task.status = TaskStatus::Active;
                let merged = options_snapshot.merge_task_options(&task.options);
                self.spawn_ftp_download(task, merged);
                self.send_download_start(&task.gid);
                started += 1;
            } else if task.kind == TaskKind::Metalink && !task.files.is_empty() {
                task.status = TaskStatus::Active;
                let merged = options_snapshot.merge_task_options(&task.options);
                apply_select_file(&mut task.files, &task.options);
                self.spawn_metalink_download(task, merged);
                self.send_download_start(&task.gid);
                started += 1;
            } else if task.kind == TaskKind::Usenet && task.usenet.is_some() {
                task.status = TaskStatus::Active;
                task.usenet_stage = Some("connecting".to_string());
                let merged = options_snapshot.merge_task_options(&task.options);
                self.spawn_usenet_download(task, merged);
                self.send_download_start(&task.gid);
                started += 1;
            } else if matches!(
                task.kind,
                TaskKind::Adc | TaskKind::Gnutella | TaskKind::G2 | TaskKind::Gift
            ) && !task.uris.is_empty()
            {
                task.status = TaskStatus::Active;
                self.spawn_legacy_p2p_download(task);
                self.send_download_start(&task.gid);
                started += 1;
            }
        }
    }

    async fn reconcile_active_set(&self) {
        let max_concurrent = self.options.read().await.max_concurrent_downloads();
        let to_preempt: Vec<String> = {
            let tasks = self.tasks.read().await;
            let mut rank = 0usize;
            let mut preempt = Vec::new();
            for task in tasks.iter() {
                if task.kind == TaskKind::Torrent {
                    continue;
                }
                match task.status {
                    TaskStatus::Active | TaskStatus::Waiting => {
                        rank += 1;
                        if rank > max_concurrent && task.status == TaskStatus::Active {
                            preempt.push(task.gid.clone());
                        }
                    }
                    _ => {}
                }
            }
            preempt
        };
        for gid in &to_preempt {
            self.preempt(gid).await;
        }
        self.try_start_next().await;
    }

    async fn preempt(&self, gid: &str) {
        let should_cancel = {
            let mut tasks = self.tasks.write().await;
            match tasks.iter_mut().find(|t| t.gid == gid) {
                Some(task) if task.status == TaskStatus::Active => {
                    tracing::info!("[task:{}] Preempted, yielding download slot", gid);
                    task.status = TaskStatus::Waiting;
                    task.download_speed = 0;
                    task.upload_speed = 0;
                    self.events.send(EngineEvent::DownloadPause {
                        gid: gid.to_string(),
                    });
                    true
                }
                _ => false,
            }
        };
        if should_cancel {
            let active = self.active_downloads.read().await;
            if let Some(ad) = active.get(gid) {
                ad.cancel_token.cancel();
            }
        }
    }

    fn apply_stored_cookies(&self, uris: &[String], merged: &mut Map<String, Value>) {
        Self::apply_stored_cookies_from(&self.cookie_store, uris, merged);
    }

    fn apply_stored_cookies_from(
        store: &CookieStore,
        uris: &[String],
        merged: &mut Map<String, Value>,
    ) {
        let Some(uri) = uris.first() else {
            return;
        };
        let Some(entry) = store.find_for_url(uri) else {
            tracing::debug!("apply_stored_cookies: no entry for uri={uri}");
            return;
        };

        tracing::debug!(
            "apply_stored_cookies: matched stored entry (browser={}, {} cookie(s))",
            entry.browser_id,
            entry.cookies.len(),
        );

        let has_cookie = merged
            .get("cookie")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty())
            || merged
                .get("header")
                .map(header_contains_cookie)
                .unwrap_or(false);
        if !has_cookie {
            let cookie_header = super::cookie_store::cookies_to_header(&entry.cookies);
            if !cookie_header.is_empty() {
                tracing::debug!(
                    "apply_stored_cookies: injecting cookie header ({} bytes)",
                    cookie_header.len(),
                );
                merged.insert("cookie".to_string(), Value::String(cookie_header));
            }
        } else {
            tracing::debug!(
                "apply_stored_cookies: cookie already set on task, leaving stored entry untouched"
            );
        }

        let has_ua = merged
            .get("user-agent")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty());
        if !has_ua && !entry.user_agent.is_empty() {
            merged.insert(
                "user-agent".to_string(),
                Value::String(entry.user_agent.clone()),
            );
        }

        store.touch(&entry.host);
    }

    pub fn cookie_store(&self) -> &Arc<CookieStore> {
        &self.cookie_store
    }

    fn task_download_limiter(&self, gid: &str, limit: u64) -> Arc<SpeedLimiter> {
        register_task_limiter(&self.task_limiters, gid, limit)
    }

    fn apply_global_limits(&self, options: &EngineOptions) {
        self.global_speed_limiter
            .set_limit(options.max_overall_download_limit());
        self.global_upload_limiter
            .set_limit(options.effective_bt_upload_limit());
    }

    fn spawn_http_download(&self, task: &DownloadTask, merged_options: Map<String, Value>) {
        let gid = task.gid.clone();
        let uris: Vec<String> = task.uris.clone();
        let dir = task.dir.clone();
        let out = task.out.clone();
        let events = self.events.clone();
        let tasks = self.tasks.clone();
        let active = self.active_downloads.clone();
        let cookie_store = self.cookie_store.clone();

        let split: u32 = merged_options
            .get("split")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(1)
            .max(1) as u32;

        let per_task_limit = merged_options
            .get("max-download-limit")
            .map(parse_speed_limit)
            .unwrap_or(0);
        let task_speed_limiter = self.task_download_limiter(&gid, per_task_limit);
        let global_limiter = self.global_speed_limiter.clone();

        let counters = Counters::new(0, split);
        let chunk_completed: Vec<Arc<AtomicU64>> =
            (0..split).map(|_| Arc::new(AtomicU64::new(0))).collect();
        let adopted_filename: Arc<parking_lot::Mutex<Option<String>>> =
            Arc::new(parking_lot::Mutex::new(None));

        let worker_epoch = next_worker_epoch();
        publish_starting_worker(
            &self.starting_workers,
            &gid,
            worker_epoch,
            counters.cancel_token.clone(),
        );
        let starting = self.starting_workers.clone();
        tokio::spawn(async move {
            register_active_download(
                &active,
                &starting,
                gid.clone(),
                counters.to_active(
                    worker_epoch,
                    chunk_completed.clone(),
                    adopted_filename.clone(),
                ),
            )
            .await;

            if worker_startup_aborted(&tasks, &gid, &counters.cancel_token).await {
                finish_task(
                    &tasks,
                    &active,
                    &events,
                    &gid,
                    worker_epoch,
                    "http",
                    &counters,
                    Err("cancelled".to_string()),
                    |_| {},
                    |task, _| task.total_length,
                    |_, e| classify_error(e, "http"),
                )
                .await;
                return;
            }

            let c = counters.clone();
            let download_result = http::run_http_download_multi(
                &uris,
                &dir,
                &out,
                &merged_options,
                c.total,
                c.completed,
                c.speed,
                c.connections,
                c.cancel_token,
                global_limiter,
                task_speed_limiter,
                chunk_completed.clone(),
                adopted_filename,
            )
            .await;

            finish_task(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                "http",
                &counters,
                download_result,
                |task| {
                    let conns = counters.connections.load(Ordering::Relaxed);
                    if chunk_completed.len() > 1 && task.total_length > 0 && conns > 1 {
                        task.chunk_progress = chunk_progress(&chunk_completed, task.total_length);
                    } else {
                        task.chunk_progress.clear();
                    }
                },
                |task, path| {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        task.out = name.to_string();
                    }
                    task.total_length
                },
                |task, e| {
                    let code = classify_error(e, "http");
                    if code == super::error_code::ErrorCode::CLOUDFLARE_CHALLENGE {
                        let lookup_url = parse_cf_host(e)
                            .map(|h| format!("https://{h}/"))
                            .unwrap_or_else(|| {
                                task.uris
                                    .first()
                                    .map(|u| u.as_str().to_owned())
                                    .unwrap_or_default()
                            });
                        if let Some(entry) = cookie_store.find_for_url(&lookup_url) {
                            if let Err(err) = cookie_store.remove(&entry.host) {
                                tracing::warn!(
                                    "[task:{gid}] cookie store remove({}) failed: {err}",
                                    entry.host
                                );
                            }
                        }
                    }
                    code
                },
            )
            .await;
        });
    }

    fn spawn_metalink_download(&self, task: &DownloadTask, merged_options: Map<String, Value>) {
        let gid = task.gid.clone();
        let dir = task.dir.clone();
        let events = self.events.clone();
        let tasks = self.tasks.clone();
        let active = self.active_downloads.clone();
        let global_limiter = self.global_speed_limiter.clone();

        let split: u32 = merged_options
            .get("split")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(1)
            .max(1) as u32;
        let per_task_limit = merged_options
            .get("max-download-limit")
            .map(parse_speed_limit)
            .unwrap_or(0);
        let task_speed_limiter = self.task_download_limiter(&gid, per_task_limit);

        let file_concurrency = metalink_file_concurrency(&merged_options);
        let checksums = metalink_checksums(&task.options);
        let parent = CancellationToken::new();

        struct Spec {
            idx: usize,
            out: String,
            uris: Vec<String>,
            checksum: Option<String>,
            counters: Counters,
            chunk: Vec<Arc<AtomicU64>>,
            adopted: Arc<parking_lot::Mutex<Option<String>>>,
        }
        let mut specs: Vec<Spec> = Vec::new();
        let mut file_counters: Vec<(usize, Counters)> = Vec::new();
        for (i, f) in task.files.iter().enumerate() {
            if f.selected == "false" {
                continue;
            }
            let len: u64 = f.length.parse().unwrap_or(0);
            let done: u64 = f.completed_length.parse().unwrap_or(0);
            if len > 0 && done >= len {
                continue;
            }
            let file_uris: Vec<String> = f.uris.iter().map(|u| u.uri.clone()).collect();
            let out = Path::new(&f.path)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            let mut counters = Counters::new(0, split);
            counters.cancel_token = parent.child_token();
            let chunk: Vec<Arc<AtomicU64>> =
                (0..split).map(|_| Arc::new(AtomicU64::new(0))).collect();
            file_counters.push((i, counters.clone()));
            specs.push(Spec {
                idx: i,
                uris: file_uris,
                out,
                checksum: checksums.get(i).filter(|cs| !cs.is_empty()).cloned(),
                counters,
                chunk,
                adopted: Arc::new(parking_lot::Mutex::new(None)),
            });
        }

        let merged_options = Arc::new(merged_options);
        let cookie_store = self.cookie_store.clone();
        let agg = Counters::new(0, 0);
        let worker_epoch = next_worker_epoch();
        publish_starting_worker(&self.starting_workers, &gid, worker_epoch, parent.clone());
        let starting = self.starting_workers.clone();
        tokio::spawn(async move {
            let mut ad = agg.to_active(
                worker_epoch,
                Vec::new(),
                Arc::new(parking_lot::Mutex::new(None)),
            );
            ad.cancel_token = parent.clone();
            ad.metalink_files = file_counters.clone();
            register_active_download(&active, &starting, gid.clone(), ad).await;

            if worker_startup_aborted(&tasks, &gid, &parent).await {
                metalink_finish(
                    &tasks,
                    &active,
                    &events,
                    &gid,
                    worker_epoch,
                    file_counters,
                    Vec::new(),
                )
                .await;
                return;
            }

            let futs = specs.into_iter().map(|spec| {
                let gl = global_limiter.clone();
                let tl = task_speed_limiter.clone();
                let dir = dir.clone();
                let base_options = merged_options.clone();
                let cookie_store = cookie_store.clone();
                async move {
                    let mut options = (*base_options).clone();
                    Self::apply_stored_cookies_from(&cookie_store, &spec.uris, &mut options);
                    options.insert("out".to_string(), Value::String(spec.out.clone()));
                    if let Some(cs) = spec.checksum {
                        options.insert("checksum".to_string(), Value::String(cs));
                    }
                    let c = spec.counters;
                    let r = http::run_http_download_multi(
                        &spec.uris,
                        &dir,
                        &spec.out,
                        &options,
                        c.total,
                        c.completed,
                        c.speed,
                        c.connections,
                        c.cancel_token,
                        gl,
                        tl,
                        spec.chunk,
                        spec.adopted,
                    )
                    .await;
                    (spec.idx, spec.out, r)
                }
            });
            use futures_util::StreamExt;
            let results: Vec<_> = futures_util::stream::iter(futs)
                .buffer_unordered(file_concurrency)
                .collect()
                .await;

            metalink_finish(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                file_counters,
                results,
            )
            .await;
        });
    }

    fn spawn_ed2k_download(&self, task: &DownloadTask) {
        let gid = task.gid.clone();
        let uri = task.uris.first().cloned().unwrap_or_default();
        let dir = task.dir.clone();
        let task_options = task.options.clone();
        let events = self.events.clone();
        let tasks = self.tasks.clone();
        let active = self.active_downloads.clone();
        let options = self.options.clone();
        let kad_service = self.kad_service();
        let kad_udp_port = self.kad_udp_port();
        let global_limiter = self.global_speed_limiter.clone();
        let task_limiters = self.task_limiters.clone();
        let kad_status = Arc::new(parking_lot::Mutex::new(Some(
            self.kad_initial_task_status(),
        )));

        let counters = Counters::new(task.total_length, 0);
        let worker_epoch = next_worker_epoch();
        publish_starting_worker(
            &self.starting_workers,
            &gid,
            worker_epoch,
            counters.cancel_token.clone(),
        );
        let starting = self.starting_workers.clone();
        tokio::spawn(async move {
            let mut active_download = counters.to_active(
                worker_epoch,
                Vec::new(),
                Arc::new(parking_lot::Mutex::new(None)),
            );
            active_download.kad_status = kad_status.clone();
            register_active_download(&active, &starting, gid.clone(), active_download).await;

            let still_active = tasks
                .read()
                .await
                .iter()
                .any(|task| task.gid == gid && task.status == TaskStatus::Active);
            if !still_active {
                counters.cancel_token.cancel();
                finish_task(
                    &tasks,
                    &active,
                    &events,
                    &gid,
                    worker_epoch,
                    "ed2k",
                    &counters,
                    Err("cancelled during P2P proxy reload".to_string()),
                    |_| {},
                    |task, _| task.total_length,
                    |_, e| classify_error(e, "ed2k"),
                )
                .await;
                return;
            }

            let file_link = super::ed2k::parse_ed2k_link(&uri);
            let effective_options = {
                let options = options.read().await;
                EngineOptions {
                    global: options.merge_task_options(&task_options),
                }
            };
            let ed2k_servers = effective_options.ed2k_servers();
            let ed2k_port = effective_options.ed2k_port();
            let p2p_proxy = effective_options.p2p_proxy_connector();
            let throttle = risuko_bt::limiter::Throttle::new(
                global_limiter,
                register_task_limiter(
                    &task_limiters,
                    &gid,
                    super::torrent::task_limit(&effective_options.global, "max-download-limit"),
                ),
            );

            let c = counters.clone();
            let download_result = match (file_link, p2p_proxy) {
                (Ok(link), Ok(p2p_proxy)) => {
                    super::ed2k::run_ed2k_download_with_proxy(
                        &link,
                        &dir,
                        ed2k_servers,
                        ed2k_port,
                        kad_udp_port,
                        kad_service,
                        kad_status,
                        c.total,
                        c.completed,
                        c.speed,
                        c.connections,
                        c.cancel_token,
                        p2p_proxy,
                        throttle,
                    )
                    .await
                }
                (Err(e), _) => Err(e),
                (_, Err(e)) => Err(e),
            };

            finish_task(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                "ed2k",
                &counters,
                download_result,
                |_| {},
                |task, _| task.total_length,
                |_, e| classify_error(e, "ed2k"),
            )
            .await;
        });
    }

    fn spawn_media_download(&self, task: &DownloadTask, merged_options: Map<String, Value>) {
        let gid = task.gid.clone();
        let uri = task.uris.first().cloned().unwrap_or_default();
        let dir = task.dir.clone();
        let out = task.out.clone();
        let events = self.events.clone();
        let tasks = self.tasks.clone();
        let active = self.active_downloads.clone();
        let global_limiter = self.global_speed_limiter.clone();

        let counters = Counters::new(0, 1);

        let (dest_tx, mut dest_rx) = tokio::sync::watch::channel(String::new());

        let tasks_name = tasks.clone();
        let gid_name = gid.clone();
        tokio::spawn(async move {
            while dest_rx.changed().await.is_ok() {
                let dest = dest_rx.borrow().clone();
                if dest.is_empty() {
                    continue;
                }
                let mut guard = tasks_name.write().await;
                if let Some(t) = guard.iter_mut().find(|t| t.gid == gid_name) {
                    if let Some(f) = t.files.get_mut(0) {
                        f.path = dest;
                    }
                }
            }
        });

        let worker_epoch = next_worker_epoch();
        publish_starting_worker(
            &self.starting_workers,
            &gid,
            worker_epoch,
            counters.cancel_token.clone(),
        );
        let starting = self.starting_workers.clone();
        tokio::spawn(async move {
            register_active_download(
                &active,
                &starting,
                gid.clone(),
                counters.to_active(
                    worker_epoch,
                    Vec::new(),
                    Arc::new(parking_lot::Mutex::new(None)),
                ),
            )
            .await;

            if worker_startup_aborted(&tasks, &gid, &counters.cancel_token).await {
                finish_task(
                    &tasks,
                    &active,
                    &events,
                    &gid,
                    worker_epoch,
                    "media",
                    &counters,
                    Err("cancelled".to_string()),
                    |_| {},
                    |task, _| task.total_length,
                    |_, e| classify_error(e, "media"),
                )
                .await;
                return;
            }

            let global_rate_limit = global_limiter.limit_bps();

            let c = counters.clone();
            let download_result = media::run_media_download(
                &uri,
                &dir,
                &out,
                &merged_options,
                global_rate_limit,
                c.total,
                c.completed,
                c.speed,
                c.connections,
                c.cancel_token,
                dest_tx,
            )
            .await;

            finish_task(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                "media",
                &counters,
                download_result,
                |_| {},
                |task, path| {
                    if let Ok(metadata) = std::fs::metadata(path) {
                        let file_size = metadata.len();
                        task.completed_length = file_size;
                        if task.total_length == 0 {
                            task.total_length = file_size;
                        }
                    }
                    task.completed_length
                },
                |_, e| classify_error(e, "media"),
            )
            .await;
        });
    }

    fn spawn_m3u8_download(&self, task: &DownloadTask, merged_options: Map<String, Value>) {
        let gid = task.gid.clone();
        let uri = task.uris.first().cloned().unwrap_or_default();
        let dir = task.dir.clone();
        let out = task.out.clone();
        let events = self.events.clone();
        let tasks = self.tasks.clone();
        let active = self.active_downloads.clone();

        let per_task_limit = merged_options
            .get("max-download-limit")
            .map(parse_speed_limit)
            .unwrap_or(0);
        let task_speed_limiter = self.task_download_limiter(&gid, per_task_limit);
        let global_limiter = self.global_speed_limiter.clone();

        let counters = Counters::new(0, 0);
        let worker_epoch = next_worker_epoch();
        publish_starting_worker(
            &self.starting_workers,
            &gid,
            worker_epoch,
            counters.cancel_token.clone(),
        );
        let starting = self.starting_workers.clone();
        tokio::spawn(async move {
            register_active_download(
                &active,
                &starting,
                gid.clone(),
                counters.to_active(
                    worker_epoch,
                    Vec::new(),
                    Arc::new(parking_lot::Mutex::new(None)),
                ),
            )
            .await;

            if worker_startup_aborted(&tasks, &gid, &counters.cancel_token).await {
                finish_task(
                    &tasks,
                    &active,
                    &events,
                    &gid,
                    worker_epoch,
                    "m3u8",
                    &counters,
                    Err("cancelled".to_string()),
                    |_| {},
                    |task, _| task.total_length,
                    |_, e| classify_error(e, "m3u8"),
                )
                .await;
                return;
            }

            let c = counters.clone();
            let download_result = super::m3u8::run_m3u8_download(
                &gid,
                &uri,
                &dir,
                &out,
                &merged_options,
                c.total,
                c.completed,
                c.speed,
                c.connections,
                c.cancel_token,
                global_limiter,
                task_speed_limiter,
            )
            .await;

            finish_task(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                "m3u8",
                &counters,
                download_result,
                |_| {},
                |task, _| task.total_length,
                |_, e| classify_error(e, "m3u8"),
            )
            .await;
        });
    }

    fn spawn_ftp_download(&self, task: &DownloadTask, merged_options: Map<String, Value>) {
        let gid = task.gid.clone();
        let uri = task.uris.first().cloned().unwrap_or_default();
        let dir = task.dir.clone();
        let out = task.out.clone();
        let events = self.events.clone();
        let tasks = self.tasks.clone();
        let active = self.active_downloads.clone();

        let per_task_limit = merged_options
            .get("max-download-limit")
            .map(parse_speed_limit)
            .unwrap_or(0);
        let task_speed_limiter = self.task_download_limiter(&gid, per_task_limit);
        let global_limiter = self.global_speed_limiter.clone();

        let counters = Counters::new(0, 1);
        let worker_epoch = next_worker_epoch();
        publish_starting_worker(
            &self.starting_workers,
            &gid,
            worker_epoch,
            counters.cancel_token.clone(),
        );
        let starting = self.starting_workers.clone();
        tokio::spawn(async move {
            register_active_download(
                &active,
                &starting,
                gid.clone(),
                counters.to_active(
                    worker_epoch,
                    Vec::new(),
                    Arc::new(parking_lot::Mutex::new(None)),
                ),
            )
            .await;

            if worker_startup_aborted(&tasks, &gid, &counters.cancel_token).await {
                finish_task(
                    &tasks,
                    &active,
                    &events,
                    &gid,
                    worker_epoch,
                    "ftp",
                    &counters,
                    Err("cancelled".to_string()),
                    |_| {},
                    |task, _| task.total_length,
                    |_, e| classify_error(e, "ftp"),
                )
                .await;
                return;
            }

            let c = counters.clone();
            let download_result = super::ftp::run_ftp_download(
                &uri,
                &dir,
                &out,
                &merged_options,
                c.total,
                c.completed,
                c.speed,
                c.connections,
                c.cancel_token,
                global_limiter,
                task_speed_limiter,
            )
            .await;

            finish_task(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                "ftp",
                &counters,
                download_result,
                |_| {},
                |task, _| task.total_length,
                |_, e| classify_error(e, "ftp"),
            )
            .await;
        });
    }

    pub async fn update_progress(&self) {
        self.enforce_result_cap().await;

        {
            let tasks_ro = self.tasks.read().await;
            let any_work = tasks_ro.iter().any(|t| {
                matches!(
                    t.status,
                    TaskStatus::Active | TaskStatus::Waiting | TaskStatus::Scheduled
                )
            });
            if !any_work {
                return;
            }
        }

        {
            let (g_manual, g_seed_time, g_seed_ratio, bt_create_subfolder_default) = {
                let opts = self.options.read().await;
                let csub_default = opts.get_bool("bt-create-subfolder").unwrap_or(true);
                (
                    opts.keep_seeding(),
                    opts.seed_time(),
                    opts.seed_ratio(),
                    csub_default,
                )
            };

            let active_torrent_gids = {
                let active = self.active_downloads.read().await;
                let mut tasks = self.tasks.write().await;
                let mut active_torrent_gids = Vec::new();

                for task in tasks.iter_mut() {
                    if task.status != TaskStatus::Active {
                        continue;
                    }
                    if task.kind == TaskKind::Torrent {
                        active_torrent_gids.push(task.gid.clone());
                    }
                    if let Some(ad) = active.get(&task.gid) {
                        if task.kind == TaskKind::Metalink {
                            for (idx, c) in &ad.metalink_files {
                                if let Some(f) = task.files.get_mut(*idx) {
                                    f.completed_length =
                                        c.completed.load(Ordering::Relaxed).to_string();
                                    let t = c.total.load(Ordering::Relaxed);
                                    if t > 0 {
                                        f.length = t.to_string();
                                    }
                                }
                            }
                            task.download_speed = ad
                                .metalink_files
                                .iter()
                                .map(|(_, c)| c.speed.load(Ordering::Relaxed))
                                .sum();
                            task.connections = ad
                                .metalink_files
                                .iter()
                                .map(|(_, c)| c.connections.load(Ordering::Relaxed))
                                .sum();
                            metalink_rollup_totals(task);
                            continue;
                        }
                        task.total_length = ad.total.load(Ordering::Relaxed);
                        task.completed_length = ad.completed.load(Ordering::Relaxed);
                        task.download_speed = ad.speed.load(Ordering::Relaxed);
                        task.connections = ad.connections.load(Ordering::Relaxed);
                        if task.kind == TaskKind::Ed2k {
                            task.ed2k_kad = ad.kad_status.lock().as_ref().map(ed2k_kad_task_status);
                        }

                        if let Some(name) = ad.adopted_filename.lock().clone() {
                            if !name.is_empty() && task.out != name {
                                task.out = name.clone();
                                if let Some(f) = task.files.first_mut() {
                                    f.path = format!("{}/{}", task.dir, name);
                                }
                            }
                        }

                        let conns = ad.connections.load(Ordering::Relaxed);
                        if !ad.chunk_completed.is_empty() && task.total_length > 0 && conns > 1 {
                            task.chunk_progress =
                                chunk_progress(&ad.chunk_completed, task.total_length);
                        } else {
                            task.chunk_progress.clear();
                        }

                        if let Some(f) = task.files.first_mut() {
                            f.length = task.total_length.to_string();
                            f.completed_length = task.completed_length.to_string();
                            if looks_like_url(&f.path) {
                                let filename = if !task.out.is_empty() {
                                    task.out.clone()
                                } else if let Some(uri) = task.uris.first() {
                                    let name = http::infer_filename_from_uri(uri);
                                    format!("{name}.part")
                                } else {
                                    String::new()
                                };
                                if !filename.is_empty() {
                                    let display =
                                        filename.strip_suffix(".part").unwrap_or(&filename);
                                    f.path = format!("{}/{}", task.dir, display);
                                }
                            }
                        }
                    }
                }

                active_torrent_gids
            };

            let resume_seq = self.torrent_resume_seq.load(Ordering::SeqCst);
            let torrent_stats_by_gid: HashMap<String, torrent::TorrentStats> =
                if active_torrent_gids.is_empty() {
                    HashMap::new()
                } else {
                    let te_guard = self.torrent_engine.read().await;
                    let tid_guard = self.torrent_ids.read().await;
                    if let Some(ref te) = *te_guard {
                        active_torrent_gids
                            .into_iter()
                            .filter_map(|gid| {
                                let tid = *tid_guard.get(&gid)?;
                                te.get_torrent_stats(tid).map(|stats| (gid, stats))
                            })
                            .collect()
                    } else {
                        HashMap::new()
                    }
                };

            let mut halted = Vec::new();
            let mut write_halted = Vec::new();
            if !torrent_stats_by_gid.is_empty() {
                let mut tasks = self.tasks.write().await;
                let resumed_since_snapshot =
                    self.torrent_resume_seq.load(Ordering::SeqCst) != resume_seq;
                for task in tasks.iter_mut() {
                    if task.kind != TaskKind::Torrent || task.status != TaskStatus::Active {
                        if !task.peers.is_empty() {
                            task.peers = Vec::new();
                        }
                        task.bt_peers_key = None;
                        continue;
                    }
                    if let Some(stats) = torrent_stats_by_gid.get(&task.gid) {
                        task.total_length = stats.total_bytes;
                        task.completed_length = stats.downloaded_bytes;
                        task.upload_length = stats.uploaded_bytes;
                        task.download_speed = stats.download_speed;
                        task.upload_speed = stats.upload_speed;
                        task.connections = stats.num_peers;
                        task.num_seeders = stats.num_seeders;

                        let num_pieces = stats
                            .metadata
                            .as_ref()
                            .map(|m| m.num_pieces)
                            .unwrap_or(task.num_pieces);
                        let peers_key = (stats.peers_seq, num_pieces);
                        if task.bt_peers_key != Some(peers_key) {
                            sync_peer_infos(&mut task.peers, &stats.peers, num_pieces);
                            task.bt_peers_key = Some(peers_key);
                        }

                        if let Some(ref meta) = stats.metadata {
                            task.piece_length = meta.piece_length;
                            task.num_pieces = meta.num_pieces;
                            if task.bt_comment.is_none() {
                                task.bt_comment = meta.comment.clone();
                            }
                            if task.bt_creation_date.is_none() {
                                task.bt_creation_date = meta.creation_date;
                            }
                            if task.bt_announce_list.is_empty() {
                                task.bt_announce_list = meta.announce_list.clone();
                            }
                        }

                        if task.bt_name.is_none() {
                            if let Some(ref name) = stats.name {
                                task.bt_name = Some(name.clone());
                            }
                        }

                        if let Some(ref file_details) = stats.file_details {
                            let torrent_name = task.bt_name.as_deref().unwrap_or("");
                            let create_subfolder = task
                                .options
                                .get("bt-create-subfolder")
                                .and_then(super::options::json_bool)
                                .unwrap_or(bt_create_subfolder_default);
                            let base_dir = if let Some(resolved_root) =
                                stats.resolved_root.as_ref().filter(|s| !s.is_empty())
                            {
                                resolved_root.clone()
                            } else if torrent_name.is_empty()
                                || stats.single_file_mode
                                || !create_subfolder
                            {
                                task.dir.clone()
                            } else {
                                format!("{}/{}", task.dir, torrent::disk_component(torrent_name))
                            };

                            let select_file =
                                task.options.get("select-file").and_then(|v| v.as_str());
                            let sig =
                                torrent_files_signature(&base_dir, select_file, file_details.len());
                            let cached = task
                                .bt_files_cache
                                .filter(|(s, _)| *s == sig)
                                .filter(|_| task.files.len() == file_details.len());
                            let (selected_total, selected_completed, has_selection) =
                                if let Some((_, has_selection)) = cached {
                                    let (t, c) = refresh_torrent_file_progress(
                                        &mut task.files,
                                        file_details,
                                        &stats.file_progress,
                                    );
                                    (t, c, has_selection)
                                } else {
                                    let selected_indices: Option<std::collections::HashSet<usize>> =
                                        select_file
                                            .and_then(torrent::parse_select_file)
                                            .map(|indices| indices.into_iter().collect());
                                    let (t, c) = sync_torrent_files(
                                        &mut task.files,
                                        file_details,
                                        &stats.file_progress,
                                        &base_dir,
                                        selected_indices.as_ref(),
                                    );
                                    task.bt_files_cache = Some((sig, selected_indices.is_some()));
                                    (t, c, selected_indices.is_some())
                                };

                            if has_selection {
                                task.total_length = selected_total;
                                task.completed_length = selected_completed;
                            }
                        } else if task.files.is_empty() {
                            if let Some(ref name) = stats.name {
                                task.files = vec![DownloadFile {
                                    index: "1".to_string(),
                                    path: format!("{}/{}", task.dir, torrent::disk_component(name)),
                                    length: stats.total_bytes.to_string(),
                                    completed_length: stats.downloaded_bytes.to_string(),
                                    selected: "true".to_string(),
                                    uris: Vec::new(),
                                }];
                            }
                        } else {
                            if let Some(f) = task.files.first_mut() {
                                f.length = stats.total_bytes.to_string();
                                f.completed_length = stats.downloaded_bytes.to_string();
                            }
                        }

                        if let Some(error) = stats.error.as_ref() {
                            if task.status == TaskStatus::Active
                                && !task.seeder
                                && !resumed_since_snapshot
                            {
                                write_halted.push(task.gid.clone());
                                task.status = TaskStatus::Error;
                                task.error_code =
                                    Some(classify_error(error, "torrent").to_string());
                                task.error_message = Some(error.clone());
                                task.download_speed = 0;
                                self.events.send(EngineEvent::DownloadError {
                                    gid: task.gid.clone(),
                                });
                            }
                        }

                        let (keep, seed_time_minutes, seed_ratio) =
                            resolve_seed_goal(&task.options, g_manual, g_seed_time, g_seed_ratio);

                        if stats.is_finished && !task.seeder {
                            if keep {
                                task.seeder = true;
                                task.seeding_since = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis()
                                    as u64;
                                task.download_speed = 0;
                                self.events.send(EngineEvent::BtDownloadComplete {
                                    gid: task.gid.clone(),
                                });
                            } else {
                                task.status = TaskStatus::Complete;
                                task.download_speed = 0;
                                task.upload_speed = 0;
                                halted.push(task.gid.clone());
                                self.events.send(EngineEvent::BtDownloadComplete {
                                    gid: task.gid.clone(),
                                });
                                self.events.send(EngineEvent::DownloadComplete {
                                    gid: task.gid.clone(),
                                });
                            }
                        }

                        if task.seeder && task.seeding_since > 0 {
                            let mut should_stop = false;

                            let seed_time_ms = seed_time_minutes * 60 * 1000;
                            if seed_time_ms > 0 {
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis() as u64;
                                if now >= task.seeding_since
                                    && now - task.seeding_since >= seed_time_ms
                                {
                                    should_stop = true;
                                }
                            }

                            if !should_stop && seed_ratio > 0.0 && task.total_length > 0 {
                                let current_ratio =
                                    task.upload_length as f64 / task.total_length as f64;
                                if current_ratio >= seed_ratio {
                                    should_stop = true;
                                }
                            }

                            if should_stop {
                                task.seeder = false;
                                task.seeding_since = 0;
                                task.status = TaskStatus::Complete;
                                task.upload_speed = 0;
                                halted.push(task.gid.clone());
                                self.events.send(EngineEvent::DownloadComplete {
                                    gid: task.gid.clone(),
                                });
                            }
                        }
                    }
                }
            }
            if !halted.is_empty() || !write_halted.is_empty() {
                let (te, tids, halted_tids) = {
                    let tid_guard = self.torrent_ids.read().await;
                    let te_guard = self.torrent_engine.read().await;
                    let lookup = |gids: &[String]| -> Vec<usize> {
                        gids.iter()
                            .filter_map(|gid| tid_guard.get(gid).copied())
                            .collect()
                    };
                    (te_guard.clone(), lookup(&halted), lookup(&write_halted))
                };
                if let Some(te) = te {
                    for tid in tids {
                        let te = te.clone();
                        tokio::spawn(async move {
                            te.pause(tid).await.ok();
                        });
                    }
                    for tid in halted_tids {
                        let te = te.clone();
                        tokio::spawn(async move {
                            te.pause_if_halted(tid).await.ok();
                        });
                    }
                }
            }
        }
        self.ensure_active_magnet_resolvers().await;
        self.check_scheduled_tasks().await;
        self.reconcile_active_set().await;
    }

    async fn ensure_active_magnet_resolvers(&self) {
        let _p2p_reload_guard = self.p2p_reload_lock.lock().await;
        self.ensure_active_magnet_resolvers_unlocked().await;
    }

    async fn ensure_active_magnet_resolvers_unlocked(&self) {
        let jobs = {
            // Same lock order as remove() (torrent_ids -> pending_magnets -> tasks) to avoid deadlock
            let torrent_ids = self.torrent_ids.read().await;
            let pending = self.pending_magnets.read().await;
            let tasks = self.tasks.read().await;
            let options = self.options.read().await;

            tasks
                .iter()
                .filter(|task| {
                    task.kind == TaskKind::Torrent
                        && task.status == TaskStatus::Active
                        && !torrent_ids.contains_key(&task.gid)
                        && !pending.contains(&task.gid)
                })
                .filter_map(|task| {
                    let uri = task
                        .uris
                        .iter()
                        .find(|uri| torrent::is_magnet_uri(uri))?
                        .clone();
                    Some((
                        task.gid.clone(),
                        uri,
                        options.merge_task_options(&task.options),
                    ))
                })
                .collect::<Vec<_>>()
        };

        for (gid, uri, options) in jobs {
            self.spawn_magnet_metadata_resolver(gid, uri, options).await;
        }

        self.restore_saved_torrent_files().await;
    }

    fn saved_torrent_path(&self, info_hash: &str) -> Option<PathBuf> {
        if info_hash.is_empty() || !info_hash.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        Some(
            self.config_dir
                .join("torrents")
                .join(format!("{}.torrent", info_hash.to_ascii_lowercase())),
        )
    }

    async fn persist_torrent_file(&self, info_hash: &str, data: &[u8]) {
        let Some(path) = self.saved_torrent_path(info_hash) else {
            return;
        };
        if let Some(parent) = path.parent() {
            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                tracing::warn!("could not create {}: {e}", parent.display());
                return;
            }
        }
        if let Err(e) = tokio::fs::write(&path, data).await {
            tracing::warn!("could not save {}: {e}", path.display());
        }
    }

    async fn forget_saved_torrent_file(&self, gid: &str) {
        let hash = {
            let tasks = self.tasks.read().await;
            tasks
                .iter()
                .find(|t| t.gid == gid && t.kind == TaskKind::Torrent)
                .and_then(|t| t.info_hash.clone())
        };
        if let Some(path) = hash.and_then(|h| self.saved_torrent_path(&h)) {
            tokio::fs::remove_file(path).await.ok();
        }
    }

    async fn restore_saved_torrent_files(&self) {
        let candidates: Vec<(String, String, Map<String, Value>)> = {
            let torrent_ids = self.torrent_ids.read().await;
            let tasks = self.tasks.read().await;
            let options = self.options.read().await;
            tasks
                .iter()
                .filter(|task| {
                    task.kind == TaskKind::Torrent
                        && task.status == TaskStatus::Active
                        && task.info_hash.as_ref().is_some_and(|h| !h.is_empty())
                        && !torrent_ids.contains_key(&task.gid)
                        && !task.uris.iter().any(|uri| torrent::is_magnet_uri(uri))
                })
                .map(|task| {
                    (
                        task.gid.clone(),
                        task.info_hash.clone().unwrap_or_default(),
                        options.merge_task_options(&task.options),
                    )
                })
                .collect()
        };
        for (gid, info_hash, merged) in candidates {
            let Some(path) = self.saved_torrent_path(&info_hash) else {
                continue;
            };
            let Ok(data) = tokio::fs::read(&path).await else {
                continue;
            };
            let Some(engine) = self.torrent_engine.read().await.clone() else {
                return;
            };
            match engine.add_torrent_bytes(&data, &merged).await {
                Ok(handle) => {
                    tracing::info!("[task:{gid}] re-added saved torrent as id={}", handle.id);
                    self.remember_torrent_id(&gid, handle.id).await;
                    let still_active = self
                        .tasks
                        .read()
                        .await
                        .iter()
                        .any(|t| t.gid == gid && t.status == TaskStatus::Active);
                    if !still_active {
                        self.torrent_ids.write().await.remove(&gid);
                        engine.remove(handle.id, false).await.ok();
                    }
                }
                Err(e) => {
                    tracing::warn!("[task:{gid}] re-adding saved torrent failed: {e}");
                    let mut tasks = self.tasks.write().await;
                    if let Some(task) = tasks
                        .iter_mut()
                        .find(|t| t.gid == gid && t.status == TaskStatus::Active)
                    {
                        task.status = TaskStatus::Error;
                        task.error_code = Some(classify_error(&e, "torrent").to_string());
                        task.error_message = Some(e);
                        task.download_speed = 0;
                        self.events.send(EngineEvent::DownloadError { gid });
                    }
                }
            }
        }
    }

    async fn save_session_logged(&self, gid: &str) {
        if let Err(e) = self.save_session().await {
            tracing::warn!("[task:{gid}] save_session after add failed: {e}");
        }
    }

    async fn discard_torrent_engine(&self) {
        *self.torrent_engine.write().await = None;
        self.torrent_ids.write().await.clear();
    }

    async fn torrent_target(&self, gid: &str) -> Option<(TorrentEngine, usize)> {
        let tid = self.torrent_ids.read().await.get(gid).copied()?;
        let te = self.torrent_engine.read().await.clone()?;
        Some((te, tid))
    }

    async fn torrent_targets<'a>(
        &self,
        gids: impl IntoIterator<Item = &'a String>,
    ) -> Option<(TorrentEngine, Vec<usize>)> {
        let tids: Vec<usize> = {
            let ids = self.torrent_ids.read().await;
            gids.into_iter()
                .filter_map(|g| ids.get(g).copied())
                .collect()
        };
        let te = self.torrent_engine.read().await.clone()?;
        Some((te, tids))
    }

    pub async fn pause(&self, gid: &str) -> Result<(), String> {
        if let Some((_, token)) = self.worker_epoch_and_cancel(gid).await {
            token.cancel();
        }

        let result = {
            let mut tasks = self.tasks.write().await;
            match tasks.iter_mut().find(|t| t.gid == gid) {
                Some(task)
                    if task.status == TaskStatus::Active || task.status == TaskStatus::Waiting =>
                {
                    tracing::info!("[task:{}] Paused (was {:?})", gid, task.status);
                    task.status = TaskStatus::Paused;
                    task.download_speed = 0;
                    task.upload_speed = 0;
                    self.events.send(EngineEvent::DownloadPause {
                        gid: gid.to_string(),
                    });
                    Ok(())
                }
                _ => Err(format!("Task {} not found or not active", gid)),
            }
        };

        if let Some((te, tid)) = self.torrent_target(gid).await {
            te.pause(tid).await.ok();
        }
        result
    }

    pub async fn unpause(&self, gid: &str) -> Result<(), String> {
        tracing::info!("[task:{}] Resuming", gid);
        let resumable =
            |status: &TaskStatus| matches!(status, TaskStatus::Paused | TaskStatus::Error);
        let is_torrent = {
            let tasks = self.tasks.read().await;
            let task = tasks
                .iter()
                .find(|t| t.gid == gid && resumable(&t.status))
                .ok_or_else(|| format!("Task {} not found or not paused", gid))?;
            task.kind == TaskKind::Torrent
        };

        let mut has_torrent = false;
        if is_torrent {
            let tid = self.torrent_ids.read().await.get(gid).copied();
            if let Some(tid) = tid {
                has_torrent = true;
                let te = self.torrent_engine.read().await.clone();
                if let Some(te) = te {
                    te.unpause(tid).await.ok();
                }
            }
            self.torrent_resume_seq.fetch_add(1, Ordering::SeqCst);
        }

        {
            let mut tasks = self.tasks.write().await;
            let task = tasks
                .iter_mut()
                .find(|t| t.gid == gid && resumable(&t.status))
                .ok_or_else(|| format!("Task {} not found or not paused", gid))?;
            task.error_code = None;
            task.error_message = None;
            task.usenet_repair_failure = None;
            task.status = if is_torrent {
                TaskStatus::Active
            } else {
                TaskStatus::Waiting
            };
        }

        if is_torrent {
            if !has_torrent {
                self.ensure_active_magnet_resolvers().await;
            }
            self.send_download_start(gid);
        } else {
            self.try_start_next().await;
        }

        Ok(())
    }

    async fn check_scheduled_tasks(&self) {
        const GRACE_SECS: u64 = 300;
        let now = crate::engine::util::now_secs();
        let mut tasks = self.tasks.write().await;
        for task in tasks.iter_mut() {
            if task.status != TaskStatus::Scheduled || task.schedule_missed {
                continue;
            }
            let Some(ts) = task.start_at else { continue };
            if now < ts {
                continue;
            }
            if now - ts <= GRACE_SECS {
                tracing::info!(
                    "[task:{}] Scheduled task now due, promoting to Waiting (now={}, start_at={}, elapsed={}s)",
                    task.gid,
                    now,
                    ts,
                    now - ts
                );
                task.status = TaskStatus::Waiting;
                task.start_at = None;
            } else {
                tracing::warn!(
                    "[task:{}] Scheduled task missed (now={}, start_at={}, overdue by {}s)",
                    task.gid,
                    now,
                    ts,
                    now - ts
                );
                task.schedule_missed = true;
            }
        }
    }

    pub async fn set_task_schedule(&self, gid: &str, start_at: u64) -> Result<(), String> {
        let now = crate::engine::util::now_secs();
        if start_at <= now {
            return Err("Schedule time must be in the future".to_string());
        }
        tracing::info!(
            "[task:{}] set_task_schedule: start_at={}, now={}, delay={}s",
            gid,
            start_at,
            now,
            start_at.saturating_sub(now)
        );
        {
            let mut tasks = self.tasks.write().await;
            let task = tasks
                .iter_mut()
                .find(|t| t.gid == gid)
                .ok_or_else(|| format!("Task {} not found", gid))?;
            if task.kind == TaskKind::Torrent {
                return Err("Scheduling torrent tasks is not supported".to_string());
            }
            if task.status.is_stopped() {
                return Err(format!("Task {} is already finished", gid));
            }
            task.status = TaskStatus::Scheduled;
            task.start_at = Some(start_at);
            task.schedule_missed = false;
            task.download_speed = 0;
            task.upload_speed = 0;
        }
        {
            let active = self.active_downloads.read().await;
            if let Some(ad) = active.get(gid) {
                ad.cancel_token.cancel();
            }
        }
        self.reconcile_active_set().await;
        Ok(())
    }

    pub async fn start_task_now(&self, gid: &str) -> Result<(), String> {
        tracing::info!(
            "[task:{}] start_task_now: manually starting scheduled task",
            gid
        );
        {
            let mut tasks = self.tasks.write().await;
            let task = tasks
                .iter_mut()
                .find(|t| t.gid == gid)
                .ok_or_else(|| format!("Task {} not found", gid))?;
            if task.status != TaskStatus::Scheduled {
                return Err(format!("Task {} is not scheduled", gid));
            }
            task.status = TaskStatus::Waiting;
            task.start_at = None;
            task.schedule_missed = false;
        }
        self.reconcile_active_set().await;
        Ok(())
    }

    pub async fn move_tasks(
        &self,
        gids: &[String],
        target_gid: &str,
        after: bool,
    ) -> Result<(), String> {
        {
            let mut guard = self.tasks.write().await;
            let move_set: std::collections::HashSet<&str> =
                gids.iter().map(|s| s.as_str()).collect();
            if !guard.iter().any(|t| move_set.contains(t.gid.as_str())) {
                return Err("no matching tasks to move".to_string());
            }
            if !guard
                .iter()
                .any(|t| t.gid == target_gid && !move_set.contains(t.gid.as_str()))
            {
                return Err(format!("target task {target_gid} not found"));
            }
            let mut moved = Vec::new();
            let mut remaining = Vec::new();
            for t in std::mem::take(&mut *guard) {
                if move_set.contains(t.gid.as_str()) {
                    moved.push(t);
                } else {
                    remaining.push(t);
                }
            }
            if moved.is_empty() {
                *guard = remaining;
                return Err("no matching tasks to move".to_string());
            }
            let insert_at = remaining
                .iter()
                .position(|t| t.gid == target_gid)
                .map(|p| if after { p + 1 } else { p })
                .expect("target task was validated before splitting");
            let mut result = Vec::with_capacity(remaining.len() + moved.len());
            result.extend(remaining.drain(..insert_at));
            result.extend(moved);
            result.extend(remaining);
            *guard = result;
        }
        self.reconcile_active_set().await;
        Ok(())
    }

    pub async fn retry_with_cookies(
        &self,
        gid: &str,
        cookie: Option<String>,
        user_agent: Option<String>,
    ) -> Result<(), String> {
        tracing::info!("[task:{}] Retrying with imported cookies", gid);
        let was_active;
        {
            let mut tasks = self.tasks.write().await;
            let task = tasks
                .iter_mut()
                .find(|t| t.gid == gid)
                .ok_or_else(|| format!("Task {} not found", gid))?;
            if task.kind != TaskKind::Http {
                return Err(format!("Task {} is not an HTTP task", gid));
            }
            if let Some(c) = cookie {
                if !c.is_empty() {
                    task.options.insert("cookie".to_string(), Value::String(c));
                }
            }
            if let Some(ua) = user_agent {
                if !ua.is_empty() {
                    task.options
                        .insert("user-agent".to_string(), Value::String(ua));
                }
            }
            task.error_code = None;
            task.error_message = None;
            was_active = task.status == TaskStatus::Active;
            task.status = TaskStatus::Waiting;
        }

        if was_active {
            let active = self.active_downloads.read().await;
            if let Some(ad) = active.get(gid) {
                ad.cancel_token.cancel();
            }
        }
        self.try_start_next().await;
        Ok(())
    }

    pub async fn remove(&self, gid: &str) -> Result<(), String> {
        tracing::info!("[task:{}] Removing", gid);
        if let Some((_, token)) = self.worker_epoch_and_cancel(gid).await {
            token.cancel();
        }

        let found = {
            let mut tasks = self.tasks.write().await;
            match tasks.iter_mut().find(|t| t.gid == gid) {
                Some(task) => {
                    task.status = TaskStatus::Removed;
                    task.download_speed = 0;
                    task.upload_speed = 0;
                    true
                }
                None => false,
            }
        };

        self.forget_saved_torrent_file(gid).await;
        if let Some((te, tid)) = self.torrent_target(gid).await {
            te.remove(tid, false).await.ok();
        }
        self.torrent_ids.write().await.remove(gid);
        self.pending_magnets.write().await.remove(gid);
        self.cleanup_m3u8_temp(gid).await;
        self.cleanup_p2p_partial(gid).await;
        self.cleanup_ed2k_partial(gid).await;

        if found {
            self.events.send(EngineEvent::DownloadStop {
                gid: gid.to_string(),
            });
            return Ok(());
        }
        Err(format!("Task {} not found", gid))
    }

    pub async fn tell_status(&self, gid: &str, keys: &[String]) -> Result<Value, String> {
        let tasks = self.tasks.read().await;
        tasks
            .iter()
            .find(|t| t.gid == gid)
            .map(|t| t.to_rpc_status(keys))
            .ok_or_else(|| format!("GID {} not found", gid))
    }

    pub async fn tell_active(&self, keys: &[String]) -> Value {
        let tasks = self.tasks.read().await;
        let active: Vec<Value> = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Active)
            .map(|t| t.to_rpc_status(keys))
            .collect();
        Value::Array(active)
    }

    pub async fn tell_waiting(&self, offset: i64, num: usize, keys: &[String]) -> Value {
        let tasks = self.tasks.read().await;
        let waiting: Vec<&DownloadTask> = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Waiting || t.status == TaskStatus::Paused)
            .collect();
        let num = num.min(10_000);
        Value::Array(Self::paginate_newest_first(&waiting, offset, num, keys))
    }

    pub async fn tell_stopped(&self, offset: i64, num: usize, keys: &[String]) -> Value {
        let tasks = self.tasks.read().await;
        let stopped: Vec<&DownloadTask> = tasks.iter().filter(|t| t.status.is_stopped()).collect();
        let num = num.min(10_000);
        Value::Array(Self::paginate_newest_first(&stopped, offset, num, keys))
    }

    pub async fn tell_scheduled(&self, offset: i64, num: usize, keys: &[String]) -> Value {
        let tasks = self.tasks.read().await;
        let scheduled: Vec<&DownloadTask> = tasks
            .iter()
            .filter(|t| t.status == TaskStatus::Scheduled)
            .collect();
        let num = num.min(10_000);
        Value::Array(Self::paginate_newest_first(&scheduled, offset, num, keys))
    }

    fn paginate_newest_first(
        items: &[&DownloadTask],
        offset: i64,
        num: usize,
        keys: &[String],
    ) -> Vec<Value> {
        let len = items.len();
        let (start, end) = if offset >= 0 {
            let end = len.saturating_sub(offset as usize);
            let start = end.saturating_sub(num);
            (start, end)
        } else {
            let back = usize::try_from(offset.unsigned_abs()).unwrap_or(usize::MAX);
            let start = len.saturating_sub(back);
            let end = start.saturating_add(num).min(len);
            (start, end)
        };
        items[start..end]
            .iter()
            .map(|t| t.to_rpc_status(keys))
            .collect()
    }

    pub async fn get_global_stat(&self) -> Value {
        let tasks = self.tasks.read().await;
        let mut num_active = 0u64;
        let mut num_waiting = 0u64;
        let mut num_stopped = 0u64;
        let mut num_completed = 0u64;
        let mut num_scheduled = 0u64;
        let mut num_paused = 0u64;
        let mut dl_speed = 0u64;
        let mut ul_speed = 0u64;

        for task in tasks.iter() {
            match task.status {
                TaskStatus::Active => {
                    num_active += 1;
                    dl_speed += task.download_speed;
                    ul_speed += task.upload_speed;
                }
                TaskStatus::Waiting => num_waiting += 1,
                TaskStatus::Paused => {
                    num_waiting += 1;
                    num_paused += 1;
                }
                TaskStatus::Scheduled => {
                    num_waiting += 1;
                    num_scheduled += 1;
                }
                TaskStatus::Complete => {
                    num_stopped += 1;
                    num_completed += 1;
                }
                _ => num_stopped += 1,
            }
        }

        serde_json::json!({
            "numActive": num_active.to_string(),
            "numWaiting": num_waiting.to_string(),
            "numStopped": num_stopped.to_string(),
            "numStoppedTotal": num_stopped.to_string(),
            "numCompleted": num_completed.to_string(),
            "numStoppedError": (num_stopped - num_completed).to_string(),
            "numScheduled": num_scheduled.to_string(),
            "numPaused": num_paused.to_string(),
            "downloadSpeed": dl_speed.to_string(),
            "uploadSpeed": ul_speed.to_string(),
        })
    }

    pub async fn bt_health_snapshot(&self) -> Option<super::torrent::BtHealthSnapshot> {
        let te_guard = self.torrent_engine.read().await;
        te_guard.as_ref().and_then(|te| te.health_snapshot())
    }

    pub async fn kad_health_snapshot(&self) -> KadHealthSnapshot {
        let runtime = self.kad_runtime.read().clone();
        match &runtime {
            KadRuntime::Running(service) => service.health_snapshot().await,
            KadRuntime::Disabled { port } => KadHealthSnapshot::disabled(*port),
            KadRuntime::Failed { port, error } => KadHealthSnapshot {
                enabled: true,
                bound: false,
                state: KadState::Error,
                udp_port: *port,
                node_id: String::new(),
                routing_contacts: 0,
                cached_contacts: 0,
                last_bootstrap_at_ms: None,
                last_lookup_at_ms: None,
                last_lookup_success: None,
                last_error: Some(error.clone()),
            },
        }
    }

    pub fn kad_service(&self) -> Option<Arc<KadService>> {
        self.kad_runtime.read().service()
    }

    pub fn kad_udp_port(&self) -> Option<u16> {
        self.kad_runtime.read().udp_port()
    }

    fn kad_initial_task_status(&self) -> KadLookupStatus {
        let runtime = self.kad_runtime.read().clone();
        match &runtime {
            KadRuntime::Running(_) => KadLookupStatus::default(),
            KadRuntime::Disabled { .. } => KadLookupStatus {
                state: KadState::Disabled,
                ..KadLookupStatus::default()
            },
            KadRuntime::Failed { error, .. } => KadLookupStatus {
                state: KadState::Error,
                error: Some(error.clone()),
                ..KadLookupStatus::default()
            },
        }
    }

    pub async fn list_active_tracker_urls(&self) -> Vec<String> {
        use std::collections::BTreeSet;
        let tasks = self.tasks.read().await;
        let mut seen: BTreeSet<String> = BTreeSet::new();
        for t in tasks.iter() {
            if !matches!(
                t.status,
                TaskStatus::Active | TaskStatus::Waiting | TaskStatus::Paused
            ) {
                continue;
            }
            for tier in &t.bt_announce_list {
                for url in tier {
                    let trimmed = url.trim();
                    if !trimmed.is_empty() {
                        seen.insert(trimmed.to_string());
                    }
                }
            }
        }
        seen.into_iter().collect()
    }

    pub async fn change_position(&self, gid: &str, pos: i64, how: &str) -> Result<Value, String> {
        let mut tasks = self.tasks.write().await;
        let waiting: Vec<usize> = tasks
            .iter()
            .enumerate()
            .filter(|(_, t)| t.status == TaskStatus::Waiting || t.status == TaskStatus::Paused)
            .map(|(i, _)| i)
            .collect();

        let current_waiting_pos = waiting
            .iter()
            .position(|&idx| tasks[idx].gid == gid)
            .ok_or_else(|| format!("GID {} not in waiting queue", gid))?;

        let target_waiting_pos = match how {
            "POS_SET" => pos.max(0) as usize,
            "POS_CUR" => (current_waiting_pos as i64 + pos).max(0) as usize,
            "POS_END" => {
                if pos >= 0 {
                    waiting.len().saturating_sub(1)
                } else {
                    (waiting.len() as i64 - 1 + pos).max(0) as usize
                }
            }
            _ => return Err("Invalid position mode".to_string()),
        };

        let target_waiting_pos = target_waiting_pos.min(waiting.len().saturating_sub(1));

        if current_waiting_pos != target_waiting_pos {
            let task_idx = waiting[current_waiting_pos];
            let task = tasks.remove(task_idx);

            let waiting_after_remove: Vec<usize> = tasks
                .iter()
                .enumerate()
                .filter(|(_, t)| t.status == TaskStatus::Waiting || t.status == TaskStatus::Paused)
                .map(|(i, _)| i)
                .collect();

            let insert_idx = if target_waiting_pos < waiting_after_remove.len() {
                waiting_after_remove[target_waiting_pos]
            } else {
                tasks.len()
            };

            tasks.insert(insert_idx, task);
        }

        Ok(Value::Number(serde_json::Number::from(
            target_waiting_pos as u64,
        )))
    }

    pub async fn get_waiting_gids_in_order(
        &self,
        filter: &std::collections::HashSet<String>,
    ) -> Vec<String> {
        let tasks = self.tasks.read().await;
        tasks
            .iter()
            .filter(|t| {
                (t.status == TaskStatus::Waiting || t.status == TaskStatus::Paused)
                    && filter.contains(&t.gid)
            })
            .map(|t| t.gid.clone())
            .collect()
    }

    async fn worker_epoch_and_cancel(&self, gid: &str) -> Option<(u64, CancellationToken)> {
        let starting = self.starting_workers.lock().get(gid).cloned();
        if let Some(entry) = starting {
            return Some(entry);
        }
        self.active_downloads
            .read()
            .await
            .get(gid)
            .map(|ad| (ad.epoch, ad.cancel_token.clone()))
    }

    async fn wait_for_worker_epoch_exit(&self, gid: &str, epoch: u64) -> bool {
        let deadline = tokio::time::Instant::now() + WORKER_EXIT_TIMEOUT;
        loop {
            let in_active = self
                .active_downloads
                .read()
                .await
                .get(gid)
                .map(|ad| ad.epoch)
                == Some(epoch);
            let in_starting = self.starting_workers.lock().get(gid).map(|(e, _)| *e) == Some(epoch);
            if !in_active && !in_starting {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn update_task(
        &self,
        gid: &str,
        patch: TaskPatch,
    ) -> Result<UpdateTaskOutcome, String> {
        let has_uris = patch.uris.is_some();
        let has_dir = patch.dir.is_some();
        let has_out = patch.out.is_some();
        let has_trackers = patch
            .trackers
            .as_ref()
            .is_some_and(|t| t.iter().any(|s| !s.trim().is_empty()));
        let has_options = patch.options.as_ref().is_some_and(|o| !o.is_empty());
        if !has_uris && !has_dir && !has_out && !has_trackers && !has_options {
            return Err("No changes in patch".to_string());
        }

        if let Some(opts) = patch.options.as_ref() {
            for key in opts.keys() {
                if STARTUP_ONLY_KEYS.contains(&key.as_str()) {
                    return Err(format!(
                        "Option '{key}' can only be changed by restarting the engine"
                    ));
                }
            }
        }

        let mut normalized_uris: Option<Vec<String>> = None;
        if let Some(uris) = patch.uris.as_ref() {
            let cleaned: Vec<String> = uris
                .iter()
                .map(|u| u.trim().to_string())
                .filter(|u| !u.is_empty())
                .collect();
            if cleaned.is_empty() {
                return Err("uris must contain at least one non-empty URL".to_string());
            }
            normalized_uris = Some(decode_thunder_http_uris(cleaned)?);
        }

        let normalized_dir = patch.dir.as_ref().map(|d| d.trim().to_string());
        if let Some(d) = normalized_dir.as_ref() {
            if d.is_empty() {
                return Err("dir must not be empty".to_string());
            }
        }
        let normalized_out = match patch.out.as_ref() {
            Some(o) => {
                let trimmed = o.trim();
                if trimmed.is_empty() {
                    return Err("out must not be empty".to_string());
                }
                Some(http::sanitize_filename(trimmed))
            }
            None => None,
        };
        if let Some(o) = normalized_out.as_ref() {
            if o.is_empty() {
                return Err("out must not be empty".to_string());
            }
        }

        let normalized_trackers: Vec<String> =
            risuko_bt::torrent::split_tracker_lists(patch.trackers.unwrap_or_default());

        let mut pending_apply: Option<DownloadTask> = None;
        let mut updating_guard: Option<UpdatingGuard> = None;
        let (
            kind,
            was_active,
            old_dir,
            old_out,
            new_dir,
            new_out,
            path_changed,
            uris_changed,
            primary_uri_changed,
            options_need_restart,
            tracker_urls_to_add,
            old_primary_uri,
            primary_uri,
        ) = {
            let mut tasks = self.tasks.write().await;
            let task = tasks
                .iter_mut()
                .find(|t| t.gid == gid)
                .ok_or_else(|| format!("GID {gid} not found"))?;

            if task.status.is_stopped() && task.status != TaskStatus::Error {
                return Err(format!(
                    "Task {gid} is finished ({}); edit is not supported",
                    task.status.as_str()
                ));
            }

            if task.kind == TaskKind::Http {
                if let Some(uris) = normalized_uris.as_ref() {
                    if let Some(bad) = uris.iter().find(|uri| !is_supported_http_task_uri(uri)) {
                        return Err(format!("Unsupported URI scheme: {bad}"));
                    }
                }
            }

            if task.kind == TaskKind::Torrent {
                if normalized_uris
                    .as_ref()
                    .is_some_and(|uris| uris != &task.uris)
                {
                    return Err("Cannot change URIs on a torrent task".to_string());
                }
                let option_dir = patch
                    .options
                    .as_ref()
                    .and_then(|o| o.get("dir"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty());
                if normalized_dir.as_ref().is_some_and(|dir| dir != &task.dir)
                    || option_dir.is_some_and(|d| d != task.dir)
                {
                    return Err("Cannot change save path on a torrent task".to_string());
                }
                let option_out = patch
                    .options
                    .as_ref()
                    .and_then(|o| o.get("out"))
                    .and_then(|v| v.as_str())
                    .map(|s| http::sanitize_filename(s.trim()))
                    .filter(|s| !s.is_empty());
                if normalized_out.as_ref().is_some_and(|out| out != &task.out)
                    || option_out.is_some_and(|o| o != task.out)
                {
                    return Err("Cannot change the file name on a torrent task".to_string());
                }
            } else if !normalized_trackers.is_empty() {
                return Err("Trackers can only be added to torrent tasks".to_string());
            }

            let was_active = task.status == TaskStatus::Active;
            let old_dir = task.dir.clone();
            let old_out = task.out.clone();
            let old_primary = task.uris.first().cloned().unwrap_or_default();
            let kind = task.kind;

            let mut staged = task.clone();
            let applied = apply_task_patch(
                &mut staged,
                normalized_uris,
                normalized_dir,
                normalized_out,
                &normalized_trackers,
                patch.options,
            );
            let needs_restart = was_active
                && kind != TaskKind::Torrent
                && (applied.uris_changed || applied.path_changed || applied.options_need_restart);
            let snapshot = (
                kind,
                was_active,
                old_dir,
                old_out,
                staged.dir.clone(),
                staged.out.clone(),
                applied.path_changed,
                applied.uris_changed,
                applied.primary_uri_changed,
                applied.options_need_restart,
                applied.tracker_urls_to_add,
                old_primary,
                staged.uris.first().cloned().unwrap_or_default(),
            );
            if needs_restart {
                tracing::info!("[task:{}] Restarting worker to apply property edits", gid);
                self.updating_tasks.lock().insert(gid.to_string());
                updating_guard = Some(UpdatingGuard {
                    set: self.updating_tasks.clone(),
                    gid: gid.to_string(),
                });
                task.status = TaskStatus::Waiting;
                task.download_speed = 0;
                task.upload_speed = 0;
                self.events.send(EngineEvent::DownloadPause {
                    gid: gid.to_string(),
                });
                pending_apply = Some(staged);
            } else {
                let status = task.status;
                *task = staged;
                task.status = status;
            }
            snapshot
        };

        let mut trackers_added = 0usize;
        if !tracker_urls_to_add.is_empty() {
            let tid = {
                let ids = self.torrent_ids.read().await;
                ids.get(gid).copied()
            };
            if let Some(tid) = tid {
                let te_guard = self.torrent_engine.read().await;
                if let Some(ref te) = *te_guard {
                    match te.add_trackers(tid, tracker_urls_to_add.clone()).await {
                        Ok(n) => trackers_added = n,
                        Err(e) => {
                            tracing::warn!("[task:{}] Failed to inject trackers live: {e}", gid);
                            trackers_added = tracker_urls_to_add.len();
                        }
                    }
                } else {
                    trackers_added = tracker_urls_to_add.len();
                }
            } else {
                trackers_added = tracker_urls_to_add.len();
            }
        }

        let needs_restart = was_active
            && kind != TaskKind::Torrent
            && (uris_changed || path_changed || options_need_restart);

        let mut restarted = false;
        if needs_restart {
            restarted = pending_apply.is_some();
            if let Some((epoch, token)) = self.worker_epoch_and_cancel(gid).await {
                token.cancel();
                if !self.wait_for_worker_epoch_exit(gid, epoch).await {
                    {
                        let mut tasks = self.tasks.write().await;
                        if let Some(task) = tasks.iter_mut().find(|t| t.gid == gid) {
                            if task.status == TaskStatus::Waiting {
                                task.status = TaskStatus::Active;
                            }
                        }
                    }
                    return Err(format!(
                        "Timed out waiting for worker to stop after {WORKER_EXIT_TIMEOUT:?}"
                    ));
                }
            }
            if let Some(mut staged) = pending_apply.take() {
                let mut tasks = self.tasks.write().await;
                if let Some(task) = tasks.iter_mut().find(|t| t.gid == gid) {
                    staged.status = task.status;
                    staged.download_speed = 0;
                    staged.upload_speed = 0;
                    *task = staged;
                }
            }
        }
        drop(updating_guard.take());

        let mut progress_preserved = true;
        if path_changed && kind != TaskKind::Torrent {
            let relocate_old_out = if old_out.is_empty() {
                http::sanitize_filename(&http::infer_filename_from_uri(&old_primary_uri))
            } else {
                old_out.clone()
            };
            let relocate_new_out = if new_out.is_empty() {
                http::sanitize_filename(&http::infer_filename_from_uri(&primary_uri))
            } else {
                new_out.clone()
            };
            if relocate_old_out.is_empty() || relocate_new_out.is_empty() {
                progress_preserved = false;
            } else {
                let (from_dir, to_dir) = (old_dir.clone(), new_dir.clone());
                let (from_out, to_out) = (relocate_old_out.clone(), relocate_new_out.clone());
                let moved = tokio::task::spawn_blocking(move || {
                    http::relocate_partial(&from_dir, &from_out, &to_dir, &to_out)
                })
                .await
                .unwrap_or_else(|e| Err(format!("relocate task failed: {e}")));
                match moved {
                    Ok(moved) => {
                        if !moved && (old_dir != new_dir || relocate_old_out != relocate_new_out) {
                            progress_preserved = false;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("[task:{}] relocate_partial failed: {e}", gid);
                        progress_preserved = false;
                    }
                }
            }
        }
        if primary_uri_changed {
            progress_preserved = false;
        }

        if needs_restart {
            self.reconcile_active_set().await;
        }

        if let Err(e) = self.save_session().await {
            tracing::warn!("[task:{}] save_session after update_task failed: {e}", gid);
        }

        Ok(UpdateTaskOutcome {
            restarted,
            trackers_added,
            progress_preserved,
        })
    }

    pub async fn change_option(
        &self,
        gid: &str,
        mut opts: Map<String, Value>,
    ) -> Result<(), String> {
        let reselect = match opts.remove("select-file") {
            Some(value) => normalize_select_file(&value)?,
            None => None,
        };
        if let Some(selection) = &reselect {
            opts.insert("select-file".to_string(), Value::String(selection.clone()));
        }
        let mut stop_seeding = false;
        let mut tasks = self.tasks.write().await;
        let is_torrent = tasks
            .iter()
            .any(|t| t.gid == gid && t.kind == TaskKind::Torrent);
        if let Some(task) = tasks.iter_mut().find(|t| t.gid == gid) {
            if let Some(v) = opts.get("seed-time") {
                let val = v
                    .as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    .unwrap_or(0);
                if val == 0 && task.seeder {
                    let effective_ratio = opts
                        .get("seed-ratio")
                        .and_then(|r| {
                            r.as_f64()
                                .or_else(|| r.as_str().and_then(|s| s.parse().ok()))
                        })
                        .or_else(|| {
                            task.options.get("seed-ratio").and_then(|r| {
                                r.as_f64()
                                    .or_else(|| r.as_str().and_then(|s| s.parse().ok()))
                            })
                        });
                    if effective_ratio.is_none_or(|r| r <= 0.0) {
                        // The engine torrent is paused after tasks.write() is released to avoid nesting torrent-engine locks
                        stop_seeding = true;
                        task.upload_speed = 0;
                        task.seeder = false;
                        task.seeding_since = 0;
                        task.status = TaskStatus::Complete;
                        self.events.send(EngineEvent::DownloadComplete {
                            gid: task.gid.clone(),
                        });
                    }
                }
            }
            let touches_limits = ["max-download-limit", "max-upload-limit"]
                .iter()
                .any(|key| opts.contains_key(*key));
            for (k, v) in opts {
                task.options.insert(k, v);
            }
            let task_options = touches_limits.then(|| task.options.clone());
            drop(tasks);
            if let Some(task_options) = task_options {
                self.apply_task_limits(gid, &task_options).await;
            }
            if stop_seeding {
                let tid = self.torrent_ids.read().await.get(gid).copied();
                let engine = self.torrent_engine.read().await.clone();
                if let (Some(tid), Some(engine)) = (tid, engine) {
                    engine.pause(tid).await.ok();
                }
            }
            if let (true, Some(selection)) = (is_torrent, reselect) {
                self.apply_torrent_selection(gid, &selection).await;
            }
            return Ok(());
        }
        Err(format!("GID {} not found", gid))
    }

    async fn apply_task_limits(&self, gid: &str, task_options: &Map<String, Value>) {
        let merged = self.options.read().await.merge_task_options(task_options);
        let download = super::torrent::task_limit(&merged, "max-download-limit");
        let upload = super::torrent::task_limit(&merged, "max-upload-limit");
        if let Some(limiter) = self.task_limiters.lock().get(gid) {
            limiter.set_limit(download);
        }
        let Some(tid) = self.torrent_ids.read().await.get(gid).copied() else {
            return;
        };
        if let Some(engine) = self.torrent_engine.read().await.as_ref() {
            engine.set_torrent_limits(tid, download, upload);
        }
    }

    async fn apply_torrent_selection(&self, gid: &str, selection: &str) {
        let Some(tid) = self.torrent_ids.read().await.get(gid).copied() else {
            return;
        };
        let Some(engine) = self.torrent_engine.read().await.clone() else {
            return;
        };
        let raw = (!selection.trim().is_empty()).then_some(selection);
        if let Err(e) = engine.set_select_file(tid, raw).await {
            tracing::warn!("[task:{gid}] could not apply file selection: {e}");
        }
    }

    fn is_p2p_task_kind(kind: TaskKind) -> bool {
        matches!(
            kind,
            TaskKind::Torrent
                | TaskKind::Ed2k
                | TaskKind::Adc
                | TaskKind::Gnutella
                | TaskKind::G2
                | TaskKind::Gift
        )
    }

    pub async fn reload_p2p_profile(&self, new_options: EngineOptions) -> Result<(), String> {
        let _reload_guard = self.p2p_reload_lock.lock().await;
        let (old_proxy, old_bypass, old_udp_proxy, old_udp_bypass, old_route, old_route_available) = {
            let options = self.options.read().await.clone();
            let (old_route, old_route_available) = match options.p2p_proxy_connector() {
                Ok(connector) => (connector.has_proxy().then_some(connector), true),
                Err(error) => {
                    tracing::warn!("previous P2P proxy was invalid during reload: {error}");
                    (None, false)
                }
            };
            (
                options
                    .get_str("p2p-proxy")
                    .unwrap_or("")
                    .trim()
                    .to_string(),
                options.get_str("p2p-no-proxy").unwrap_or("").to_string(),
                options
                    .get_str("p2p-udp-proxy")
                    .unwrap_or("")
                    .trim()
                    .to_string(),
                options
                    .get_str("p2p-udp-no-proxy")
                    .unwrap_or("")
                    .to_string(),
                old_route,
                old_route_available,
            )
        };
        let (new_proxy, new_bypass, new_udp_proxy, new_udp_bypass) = (
            new_options
                .get_str("p2p-proxy")
                .unwrap_or("")
                .trim()
                .to_string(),
            new_options
                .get_str("p2p-no-proxy")
                .unwrap_or("")
                .to_string(),
            new_options
                .get_str("p2p-udp-proxy")
                .unwrap_or("")
                .trim()
                .to_string(),
            new_options
                .get_str("p2p-udp-no-proxy")
                .unwrap_or("")
                .to_string(),
        );
        if old_proxy == new_proxy
            && old_bypass == new_bypass
            && old_udp_proxy == new_udp_proxy
            && old_udp_bypass == new_udp_bypass
        {
            return Ok(());
        }

        self.p2p_route_generation.fetch_add(1, Ordering::AcqRel);

        let active_p2p: Vec<(String, TaskKind)> = {
            let mut tasks = self.tasks.write().await;
            let mut active = Vec::new();
            for task in tasks.iter_mut() {
                if task.status == TaskStatus::Active && Self::is_p2p_task_kind(task.kind) {
                    active.push((task.gid.clone(), task.kind));
                    task.status = TaskStatus::Paused;
                    task.download_speed = 0;
                    task.upload_speed = 0;
                    self.events.send(EngineEvent::DownloadPause {
                        gid: task.gid.clone(),
                    });
                }
            }
            active
        };
        let active_gids: HashSet<String> = active_p2p.iter().map(|(gid, _)| gid.clone()).collect();

        if let Some(engine) = self.torrent_engine.read().await.clone() {
            engine.invalidate_magnet_resolutions().await;
        }

        let deadline = tokio::time::Instant::now() + P2P_RELOAD_CANCEL_TIMEOUT;
        loop {
            let still_running = {
                let active = self.active_downloads.read().await;
                for gid in &active_gids {
                    if let Some(download) = active.get(gid) {
                        download.cancel_token.cancel();
                    }
                    cancel_starting_worker(&self.starting_workers, gid);
                }
                let starting = self.starting_workers.lock();
                active.keys().any(|gid| active_gids.contains(gid))
                    || starting.keys().any(|gid| active_gids.contains(gid))
            };
            if !still_running || tokio::time::Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let still_active = {
            let active = self.active_downloads.read().await;
            let starting = self.starting_workers.lock();
            active.keys().any(|gid| active_gids.contains(gid))
                || starting.keys().any(|gid| active_gids.contains(gid))
        };
        if still_active {
            self.mark_p2p_reload_failed(
                &active_gids,
                "timed out stopping the previous P2P runtime",
            )
            .await;
            return Err("Timed out stopping active P2P tasks for proxy reload".to_string());
        }

        let connector = match new_options.p2p_proxy_connector() {
            Ok(connector) => connector,
            Err(error) => {
                self.mark_p2p_reload_failed(&active_gids, &error).await;
                return Err(error);
            }
        };
        let proxy = connector.has_proxy().then_some(connector);

        let torrent_gids: Vec<String> = active_p2p
            .iter()
            .filter(|(_, kind)| *kind == TaskKind::Torrent)
            .map(|(gid, _)| gid.clone())
            .collect();
        let torrent_targets = {
            let ids = self.torrent_ids.read().await;
            torrent_gids
                .iter()
                .filter_map(|gid| ids.get(gid).copied().map(|id| (gid.clone(), id)))
                .collect::<Vec<_>>()
        };
        let torrent_engine = self.torrent_engine.read().await.clone();
        if let Some(engine) = torrent_engine.as_ref() {
            for (_, id) in torrent_targets {
                if let Err(error) = engine.pause(id).await {
                    self.mark_p2p_reload_failed(
                        &active_gids,
                        &format!("failed to pause torrent runtime: {error}"),
                    )
                    .await;
                    return Err(error);
                }
            }
        }

        let old_dht = risuko_bt::dht::Dht::current_shared().await;

        let engine_was_missing = self.torrent_engine.read().await.is_none();
        let mut dht_swap = match risuko_bt::dht::Dht::prepare_shared_with_proxy(proxy.clone()).await
        {
            Ok(swap) => Some(swap),
            Err(error) => {
                self.mark_p2p_reload_failed(&active_gids, &error).await;
                return Err(format!("failed to rebuild BitTorrent DHT: {error}"));
            }
        };
        let dht = dht_swap.as_ref().and_then(|swap| swap.next());

        if engine_was_missing {
            if let Err(error) = self
                .initialize_torrent_engine(&new_options, proxy.clone())
                .await
            {
                if let Some(swap) = dht_swap.take() {
                    let _ = swap.rollback().await;
                }
                self.mark_p2p_reload_failed(&active_gids, &error).await;
                return Err(error);
            }
        }

        let next_kad = self.build_kad_runtime(&new_options, proxy.clone()).await;
        if let KadRuntime::Failed { error, .. } = &next_kad {
            if let Some(swap) = dht_swap.take() {
                let _ = swap.rollback().await;
            }
            if engine_was_missing {
                self.discard_torrent_engine().await;
            }
            self.mark_p2p_reload_failed(&active_gids, error).await;
            return Err(format!("failed to rebuild Kad runtime: {error}"));
        }
        let old_kad = {
            let mut guard = self.kad_runtime.write();
            std::mem::replace(&mut *guard, next_kad)
        };
        let old_kad_for_rollback = old_kad.clone();

        let torrent_engine = self.torrent_engine.read().await.clone();
        if let Some(engine) = torrent_engine.as_ref() {
            if let Err(error) = engine.reconfigure_p2p_proxy(proxy.clone(), dht).await {
                if engine_was_missing {
                    self.discard_torrent_engine().await;
                }
                let error = self
                    .rollback_p2p_reload(
                        &active_gids,
                        &error,
                        dht_swap.take(),
                        old_kad_for_rollback.clone(),
                        old_route.clone(),
                        old_dht.clone(),
                        old_route_available,
                        &old_proxy,
                        &old_bypass,
                        &old_udp_proxy,
                        &old_udp_bypass,
                    )
                    .await;
                return Err(error);
            }
        }

        {
            let mut options = self.options.write().await;
            options.set("p2p-proxy".to_string(), Value::String(new_proxy));
            options.set("p2p-no-proxy".to_string(), Value::String(new_bypass));
            options.set("p2p-udp-proxy".to_string(), Value::String(new_udp_proxy));
            options.set(
                "p2p-udp-no-proxy".to_string(),
                Value::String(new_udp_bypass),
            );
        }

        let mut torrent_to_resume = Vec::new();
        {
            let mut tasks = self.tasks.write().await;
            for (gid, kind) in &active_p2p {
                let Some(task) = tasks.iter_mut().find(|task| task.gid == *gid) else {
                    continue;
                };
                if task.status != TaskStatus::Paused {
                    continue;
                }
                task.error_code = None;
                task.error_message = None;
                if *kind == TaskKind::Torrent {
                    task.status = TaskStatus::Active;
                    torrent_to_resume.push(gid.clone());
                } else {
                    task.status = TaskStatus::Waiting;
                }
            }
        }
        let torrent_engine = self.torrent_engine.read().await.clone();
        let torrent_ids = self.torrent_ids.read().await.clone();
        if !torrent_to_resume.is_empty() && torrent_engine.is_none() {
            let error = "torrent engine unavailable after P2P proxy reload".to_string();
            if engine_was_missing {
                self.discard_torrent_engine().await;
            }
            let error = self
                .rollback_p2p_reload(
                    &active_gids,
                    &error,
                    dht_swap.take(),
                    old_kad_for_rollback.clone(),
                    old_route.clone(),
                    old_dht.clone(),
                    old_route_available,
                    &old_proxy,
                    &old_bypass,
                    &old_udp_proxy,
                    &old_udp_bypass,
                )
                .await;
            return Err(error);
        }
        if let Some(engine) = torrent_engine.as_ref() {
            for gid in torrent_to_resume {
                if let Some(id) = torrent_ids.get(&gid) {
                    if let Err(error) = engine.unpause(*id).await {
                        if engine_was_missing {
                            self.discard_torrent_engine().await;
                        }
                        let error = self
                            .rollback_p2p_reload(
                                &active_gids,
                                &error,
                                dht_swap.take(),
                                old_kad_for_rollback.clone(),
                                old_route.clone(),
                                old_dht.clone(),
                                old_route_available,
                                &old_proxy,
                                &old_bypass,
                                &old_udp_proxy,
                                &old_udp_bypass,
                            )
                            .await;
                        return Err(error);
                    }
                } else {
                    self.ensure_active_magnet_resolvers_unlocked().await;
                }
                self.send_download_start(&gid);
            }
        }
        if let Some(swap) = dht_swap.take() {
            swap.commit().await;
        }
        if let KadRuntime::Running(service) = old_kad {
            service.shutdown().await;
        }
        self.try_start_next_unlocked().await;
        Ok(())
    }

    async fn initialize_torrent_engine(
        &self,
        options: &EngineOptions,
        proxy: Option<risuko_http::ProxyConnector>,
    ) -> Result<(), String> {
        let output_dir = options.dir();
        let tuning = super::torrent::BtTuning::from_options(
            options,
            self.global_speed_limiter.clone(),
            self.global_upload_limiter.clone(),
            proxy,
        );
        let engine = TorrentEngine::new_with_tuning(Path::new(&output_dir), tuning).await?;
        *self.torrent_engine.write().await = Some(engine);
        Ok(())
    }

    async fn mark_p2p_reload_failed(&self, gids: &HashSet<String>, error: &str) {
        let mut tasks = self.tasks.write().await;
        for task in tasks.iter_mut() {
            if gids.contains(&task.gid)
                && matches!(
                    task.status,
                    TaskStatus::Active | TaskStatus::Waiting | TaskStatus::Paused
                )
            {
                task.status = TaskStatus::Paused;
                task.download_speed = 0;
                task.upload_speed = 0;
                task.error_code = Some("P2P_PROXY_RELOAD_FAILED".to_string());
                task.error_message = Some(format!("P2P proxy reload failed: {error}"));
            }
        }
    }

    async fn rollback_p2p_reload(
        &self,
        gids: &HashSet<String>,
        error: &str,
        dht_swap: Option<risuko_bt::dht::DhtRouteSwap>,
        old_kad: KadRuntime,
        old_route: Option<risuko_http::ProxyConnector>,
        old_dht: Option<Arc<risuko_bt::dht::Dht>>,
        restore_session: bool,
        old_proxy: &str,
        old_bypass: &str,
        old_udp_proxy: &str,
        old_udp_bypass: &str,
    ) -> String {
        let mut surfaced = error.to_string();

        self.mark_p2p_reload_failed(gids, &surfaced).await;
        {
            let active = self.active_downloads.read().await;
            for gid in gids {
                if let Some(download) = active.get(gid) {
                    download.cancel_token.cancel();
                }
                cancel_starting_worker(&self.starting_workers, gid);
            }
        }
        if let Some(engine) = self.torrent_engine.read().await.clone() {
            let torrent_ids = self.torrent_ids.read().await.clone();
            for gid in gids {
                if let Some(id) = torrent_ids.get(gid) {
                    if let Err(pause_error) = engine.pause(*id).await {
                        surfaced.push_str(&format!(
                            "; failed to pause torrent while rolling back: {pause_error}"
                        ));
                    }
                }
            }
        }

        let failed_kad = {
            let mut guard = self.kad_runtime.write();
            std::mem::replace(&mut *guard, old_kad)
        };
        if let KadRuntime::Running(service) = failed_kad {
            service.shutdown().await;
        }

        if restore_session {
            if let Some(engine) = self.torrent_engine.read().await.clone() {
                if let Err(restore_error) = engine
                    .reconfigure_p2p_proxy(old_route, old_dht.clone())
                    .await
                {
                    surfaced.push_str(&format!(
                        "; failed to restore the previous P2P route: {restore_error}"
                    ));
                }
            }
        } else if !old_proxy.trim().is_empty() || !old_udp_proxy.trim().is_empty() {
            self.discard_torrent_engine().await;
        }

        if let Some(swap) = dht_swap {
            let _ = swap.rollback().await;
        }

        {
            let mut options = self.options.write().await;
            options.set(
                "p2p-proxy".to_string(),
                Value::String(old_proxy.to_string()),
            );
            options.set(
                "p2p-no-proxy".to_string(),
                Value::String(old_bypass.to_string()),
            );
            options.set(
                "p2p-udp-proxy".to_string(),
                Value::String(old_udp_proxy.to_string()),
            );
            options.set(
                "p2p-udp-no-proxy".to_string(),
                Value::String(old_udp_bypass.to_string()),
            );
        }

        surfaced
    }

    async fn build_kad_runtime(
        &self,
        options: &EngineOptions,
        connector: Option<risuko_http::ProxyConnector>,
    ) -> KadRuntime {
        let port = match options.ed2k_kad_port_checked() {
            Ok(port) => port,
            Err(error) => {
                return KadRuntime::Failed {
                    port: options.ed2k_kad_port(),
                    error,
                }
            }
        };
        if !options.ed2k_enable_kad() {
            return KadRuntime::Disabled { port };
        }
        let config = KadConfig::new(self.config_dir.clone(), port, options.ed2k_port())
            .with_proxy(connector.filter(|value| value.has_proxy()));
        match KadService::bind(config).await {
            Ok(service) => KadRuntime::Running(service),
            Err(error) => KadRuntime::Failed {
                port,
                error: error.to_string(),
            },
        }
    }

    pub async fn change_global_option(&self, opts: Map<String, Value>) {
        let touches_limits = [
            "max-overall-download-limit",
            "max-overall-upload-limit",
            "bt-upload-rate-limit",
        ]
        .iter()
        .any(|key| opts.contains_key(*key));

        let touches_doh = opts.keys().any(|k| k.starts_with("doh-"));

        let touches_conns = opts.contains_key("bt-max-connections");
        let mut max_connections = None;
        {
            let mut options = self.options.write().await;
            for (k, v) in opts {
                options.set(k, v);
            }
            if touches_conns {
                max_connections = Some(options.bt_max_connections());
            }
            if touches_doh {
                super::dns::apply_from_options(&options.global);
            }
            if touches_limits {
                self.apply_global_limits(&options);
            }
        }
        if let Some(max) = max_connections {
            if let Some(engine) = self.torrent_engine.read().await.as_ref() {
                engine.set_max_connections(Some(max));
            }
        }
    }

    pub async fn get_option(&self, gid: &str) -> Result<Value, String> {
        let tasks = self.tasks.read().await;
        tasks
            .iter()
            .find(|t| t.gid == gid)
            .map(|t| Value::Object(t.options.clone()))
            .ok_or_else(|| format!("GID {} not found", gid))
    }

    pub async fn get_global_option(&self) -> Value {
        Value::Object(self.options.read().await.global.clone())
    }

    pub async fn get_peers(&self, gid: &str) -> Value {
        let tasks = self.tasks.read().await;
        if let Some(task) = tasks.iter().find(|t| t.gid == gid) {
            let peers: Vec<Value> = task
                .peers
                .iter()
                .filter_map(|p| match serde_json::to_value(p) {
                    Ok(mut v) => {
                        if let Some(obj) = v.as_object_mut() {
                            obj.insert(
                                "bitfield".into(),
                                Value::String(hex::encode(p.raw_bitfield.as_ref())),
                            );
                        }
                        Some(v)
                    }
                    Err(e) => {
                        tracing::warn!("[task:{}] failed to serialize peer entry: {e}", gid);
                        None
                    }
                })
                .collect();
            Value::Array(peers)
        } else {
            Value::Array(Vec::new())
        }
    }

    pub async fn set_bt_peer_blocklist(
        &self,
        entries: Vec<String>,
    ) -> Result<risuko_bt::BlocklistApplyResult, String> {
        let te_guard = self.torrent_engine.read().await;
        let Some(engine) = te_guard.as_ref() else {
            return Err("Torrent engine not initialized".to_string());
        };
        engine.set_peer_blocklist(entries).await
    }

    pub async fn get_uris(&self, gid: &str) -> Result<Value, String> {
        let tasks = self.tasks.read().await;
        let task = tasks
            .iter()
            .find(|t| t.gid == gid)
            .ok_or_else(|| format!("GID {} not found", gid))?;
        let uris: Vec<Value> = match task.files.first().filter(|f| !f.uris.is_empty()) {
            Some(file) => file
                .uris
                .iter()
                .map(|u| {
                    serde_json::json!({
                        "uri": u.uri,
                        "status": u.status,
                    })
                })
                .collect(),
            None => task
                .uris
                .iter()
                .enumerate()
                .map(|(i, u)| {
                    serde_json::json!({
                        "uri": u,
                        "status": if i == 0 { "used" } else { "waiting" },
                    })
                })
                .collect(),
        };
        Ok(Value::Array(uris))
    }

    pub async fn get_files(&self, gid: &str) -> Result<Value, String> {
        let tasks = self.tasks.read().await;
        let task = tasks
            .iter()
            .find(|t| t.gid == gid)
            .ok_or_else(|| format!("GID {} not found", gid))?;
        let files: Vec<Value> = task
            .files
            .iter()
            .filter_map(|f| match serde_json::to_value(f) {
                Ok(v) => Some(v),
                Err(e) => {
                    tracing::warn!("[task:{}] failed to serialize file entry: {e}", gid);
                    None
                }
            })
            .collect();
        Ok(Value::Array(files))
    }

    pub async fn files_for_upload(
        &self,
        gid: &str,
    ) -> Option<(Vec<UploadFileSnapshot>, String, Option<String>)> {
        let tasks = self.tasks.read().await;
        let task = tasks.iter().find(|t| t.gid == gid)?;
        let kind = serde_json::to_value(task.kind)
            .ok()
            .and_then(|v| v.as_str().map(String::from))
            .unwrap_or_default();
        let override_sink_id = task
            .options
            .get("upload-sink-id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let dir = std::path::Path::new(&task.dir);
        let snapshots = task
            .files
            .iter()
            .filter(|f| f.selected != "false")
            .filter_map(|f| {
                let local = std::path::PathBuf::from(&f.path);
                let size: u64 = match f.length.parse::<u64>() {
                    Ok(n) => n,
                    Err(e) => {
                        tracing::warn!(
                            "Skipping upload entry with unparseable size: path={} length={:?} err={}",
                            f.path,
                            f.length,
                            e
                        );
                        return None;
                    }
                };
                let rel_path = local
                    .strip_prefix(dir)
                    .map(std::path::PathBuf::from)
                    .unwrap_or_else(|_| {
                        local
                            .file_name()
                            .map(std::path::PathBuf::from)
                            .unwrap_or_default()
                    });
                if rel_path
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
                {
                    tracing::warn!(
                        "Skipping upload entry with parent-directory segment: path={}",
                        f.path
                    );
                    return None;
                }
                let rel = rel_path.to_string_lossy().to_string();
                let rel = if std::path::MAIN_SEPARATOR != '/' {
                    rel.replace(std::path::MAIN_SEPARATOR, "/")
                } else {
                    rel
                };
                if rel.is_empty() {
                    return None;
                }
                let category = local
                    .file_name()
                    .and_then(|s| s.to_str())
                    .and_then(super::upload::resolve_category);
                Some(UploadFileSnapshot {
                    local_path: local,
                    remote_relative: rel,
                    size,
                    category,
                })
            })
            .collect();
        Some((snapshots, kind, override_sink_id))
    }

    pub async fn get_servers(&self, gid: &str) -> Result<Value, String> {
        let tasks = self.tasks.read().await;
        let task = tasks
            .iter()
            .find(|t| t.gid == gid)
            .ok_or_else(|| format!("GID {} not found", gid))?;
        let servers: Vec<Value> = task
            .files
            .iter()
            .map(|f| {
                let svrs: Vec<Value> = f
                    .uris
                    .iter()
                    .map(|u| {
                        serde_json::json!({
                            "uri": u.uri,
                            "currentUri": u.uri,
                            "downloadSpeed": "0",
                        })
                    })
                    .collect();
                serde_json::json!({
                    "index": f.index,
                    "servers": svrs,
                })
            })
            .collect();
        Ok(Value::Array(servers))
    }

    pub async fn save_session(&self) -> Result<(), String> {
        let rev = self.tasks.rev();
        if self.saved_rev.load(Ordering::Relaxed) == rev {
            return Ok(());
        }
        let _gate = self.save_gate.lock().await;
        let (rev, data) = {
            let tasks = self.tasks.read().await;
            (self.tasks.rev(), SessionManager::encode(&tasks)?)
        };
        let session = self.session.clone();
        tokio::task::spawn_blocking(move || session.write(&data))
            .await
            .map_err(|e| format!("session save task failed: {e}"))??;
        self.saved_rev.store(rev, Ordering::Relaxed);
        Ok(())
    }

    const MAX_STOPPED_RESULTS: usize = 1000;

    async fn enforce_result_cap(&self) {
        let to_evict: Vec<String> = {
            let tasks = self.tasks.read().await;
            let stopped = tasks.iter().filter(|t| t.status.is_stopped()).count();
            if stopped <= Self::MAX_STOPPED_RESULTS {
                return;
            }
            let mut excess = stopped - Self::MAX_STOPPED_RESULTS;
            let mut gids = Vec::with_capacity(excess);
            for t in tasks.iter() {
                if excess == 0 {
                    break;
                }
                if t.status.is_stopped() {
                    gids.push(t.gid.clone());
                    excess -= 1;
                }
            }
            gids
        };

        for gid in &to_evict {
            self.drop_torrent_engine_entry(gid).await;
            self.cleanup_m3u8_temp(gid).await;
        }

        let evict: std::collections::HashSet<&str> = to_evict.iter().map(String::as_str).collect();
        let mut tasks = self.tasks.write().await;
        tasks.retain(|t| !(t.status.is_stopped() && evict.contains(t.gid.as_str())));
    }

    async fn cleanup_m3u8_temp(&self, gid: &str) {
        let target = {
            let tasks = self.tasks.read().await;
            tasks
                .iter()
                .find(|t| t.gid == gid && t.kind == TaskKind::M3u8)
                .map(|t| {
                    (
                        t.dir.clone(),
                        t.uris.first().cloned().unwrap_or_default(),
                        t.out.clone(),
                    )
                })
        };
        if let Some((dir, uri, out)) = target {
            let gid = gid.to_string();
            let _ = tokio::task::spawn_blocking(move || {
                super::m3u8::remove_temp_dirs(&gid, &dir, &uri, &out)
            })
            .await;
        }
    }

    async fn cleanup_ed2k_partial(&self, gid: &str) {
        let target = {
            let tasks = self.tasks.read().await;
            tasks
                .iter()
                .find(|t| t.gid == gid && t.kind == TaskKind::Ed2k)
                .and_then(|t| Some((t.dir.clone(), t.uris.first()?.clone())))
        };
        if let Some((dir, uri)) = target {
            let _ = tokio::task::spawn_blocking(move || {
                super::ed2k::partial::remove_partial(&dir, &uri)
            })
            .await;
        }
    }

    async fn cleanup_p2p_partial(&self, gid: &str) {
        let target = {
            let tasks = self.tasks.read().await;
            tasks
                .iter()
                .find(|t| t.gid == gid && matches!(t.kind, TaskKind::Gnutella | TaskKind::G2))
                .and_then(|t| {
                    let uri = t.uris.first()?;
                    super::gnutella::download::partial_path_for_uri(uri, &t.dir)
                })
        };
        if let Some(path) = target {
            if let Err(e) = tokio::fs::remove_file(&path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!("could not delete {}: {e}", path.display());
                }
            }
        }
    }

    pub async fn purge_download_result(&self) {
        let stopped_torrent_gids: Vec<String> = {
            let tasks = self.tasks.read().await;
            tasks
                .iter()
                .filter(|t| t.status.is_stopped() && t.kind == TaskKind::Torrent)
                .map(|t| t.gid.clone())
                .collect()
        };
        for gid in &stopped_torrent_gids {
            self.forget_saved_torrent_file(gid).await;
            self.drop_torrent_engine_entry(gid).await;
        }
        let stopped_m3u8_gids: Vec<String> = {
            let tasks = self.tasks.read().await;
            tasks
                .iter()
                .filter(|t| t.status.is_stopped() && t.kind == TaskKind::M3u8)
                .map(|t| t.gid.clone())
                .collect()
        };
        for gid in &stopped_m3u8_gids {
            self.cleanup_m3u8_temp(gid).await;
        }
        let stopped_ed2k_gids: Vec<String> = {
            let tasks = self.tasks.read().await;
            tasks
                .iter()
                .filter(|t| t.status.is_stopped() && t.kind == TaskKind::Ed2k)
                .map(|t| t.gid.clone())
                .collect()
        };
        for gid in &stopped_ed2k_gids {
            self.cleanup_ed2k_partial(gid).await;
        }
        let mut tasks = self.tasks.write().await;
        tasks.retain(|t| !t.status.is_stopped());
    }

    pub async fn remove_download_result(&self, gid: &str) -> Result<(), String> {
        let is_stopped_torrent = {
            let tasks = self.tasks.read().await;
            tasks
                .iter()
                .any(|t| t.gid == gid && t.status.is_stopped() && t.kind == TaskKind::Torrent)
        };
        if is_stopped_torrent {
            self.forget_saved_torrent_file(gid).await;
            self.drop_torrent_engine_entry(gid).await;
        }
        let is_stopped_m3u8 = {
            let tasks = self.tasks.read().await;
            tasks
                .iter()
                .any(|t| t.gid == gid && t.status.is_stopped() && t.kind == TaskKind::M3u8)
        };
        if is_stopped_m3u8 {
            self.cleanup_m3u8_temp(gid).await;
        }
        let is_stopped_ed2k = {
            let tasks = self.tasks.read().await;
            tasks
                .iter()
                .any(|t| t.gid == gid && t.status.is_stopped() && t.kind == TaskKind::Ed2k)
        };
        if is_stopped_ed2k {
            self.cleanup_ed2k_partial(gid).await;
        }
        let mut tasks = self.tasks.write().await;
        let len_before = tasks.len();
        tasks.retain(|t| !(t.gid == gid && t.status.is_stopped()));
        if tasks.len() < len_before {
            Ok(())
        } else {
            Err(format!("GID {} not found or not stopped", gid))
        }
    }

    async fn remember_torrent_id(&self, gid: &str, torrent_id: usize) {
        Self::remember_torrent_id_in(
            &self.torrent_engine,
            &self.torrent_ids,
            &self.tasks,
            gid,
            torrent_id,
        )
        .await;
    }

    async fn remember_torrent_id_in(
        torrent_engine: &Arc<RwLock<Option<TorrentEngine>>>,
        torrent_ids: &Arc<RwLock<HashMap<String, usize>>>,
        tasks: &Arc<RevLock>,
        gid: &str,
        torrent_id: usize,
    ) {
        let managed: Option<HashSet<usize>> = {
            let te = torrent_engine.read().await;
            te.as_ref().map(|te| {
                te.list_managed_torrents()
                    .into_iter()
                    .map(|(id, _)| id)
                    .collect()
            })
        };
        let dropped: Vec<String> = {
            let mut ids = torrent_ids.write().await;
            let dropped = if let Some(managed) = managed.as_ref() {
                let dropped: Vec<String> = ids
                    .iter()
                    .filter(|(mapped_gid, tid)| *mapped_gid != gid && !managed.contains(tid))
                    .map(|(mapped_gid, _)| mapped_gid.clone())
                    .collect();
                ids.retain(|mapped_gid, tid| mapped_gid == gid || managed.contains(tid));
                dropped
            } else {
                Vec::new()
            };
            ids.insert(gid.to_string(), torrent_id);
            dropped
        };
        if dropped.is_empty() {
            return;
        }
        let mut tasks = tasks.write().await;
        for task in tasks.iter_mut() {
            if dropped.iter().any(|g| g == &task.gid) && task.status == TaskStatus::Active {
                task.seeder = false;
                task.seeding_since = 0;
                task.status = TaskStatus::Complete;
                task.download_speed = 0;
                task.upload_speed = 0;
            }
        }
    }

    async fn drop_torrent_engine_entry(&self, gid: &str) {
        let tid = {
            let tid_guard = self.torrent_ids.read().await;
            tid_guard.get(gid).copied()
        };
        let Some(tid) = tid else { return };

        let te_guard = self.torrent_engine.read().await;
        if let Some(ref te) = *te_guard {
            if let Err(e) = te.remove(tid, false).await {
                tracing::warn!(
                    "[task:{}] failed to drop torrent engine entry (id={}): {}",
                    gid,
                    tid,
                    e
                );
                return;
            }
        }

        self.torrent_ids.write().await.remove(gid);
    }

    pub async fn pause_all(&self) {
        let mut torrent_gids = Vec::new();
        {
            let mut tasks = self.tasks.write().await;
            for task in tasks.iter_mut() {
                if task.status == TaskStatus::Active || task.status == TaskStatus::Waiting {
                    if task.kind == TaskKind::Torrent {
                        torrent_gids.push(task.gid.clone());
                    }
                    task.status = TaskStatus::Paused;
                    task.download_speed = 0;
                    task.upload_speed = 0;
                    self.events.send(EngineEvent::DownloadPause {
                        gid: task.gid.clone(),
                    });
                }
            }
        }
        let active = self.active_downloads.read().await;
        for ad in active.values() {
            ad.cancel_token.cancel();
        }
        drop(active);
        cancel_all_starting_workers(&self.starting_workers);
        if let Some((te, tids)) = self.torrent_targets(&torrent_gids).await {
            futures_util::future::join_all(tids.into_iter().map(|tid| {
                let te = te.clone();
                async move {
                    te.pause(tid).await.ok();
                }
            }))
            .await;
        }
    }

    pub async fn unpause_all(&self) {
        let torrent_gids: HashSet<String> = {
            let tasks = self.tasks.read().await;
            tasks
                .iter()
                .filter(|t| t.status == TaskStatus::Paused && t.kind == TaskKind::Torrent)
                .map(|t| t.gid.clone())
                .collect()
        };
        if let Some((te, tids)) = self.torrent_targets(&torrent_gids).await {
            futures_util::future::join_all(tids.into_iter().map(|tid| {
                let te = te.clone();
                async move {
                    te.unpause(tid).await.ok();
                }
            }))
            .await;
        }
        self.torrent_resume_seq.fetch_add(1, Ordering::SeqCst);
        {
            let mut tasks = self.tasks.write().await;
            for task in tasks.iter_mut() {
                if task.status != TaskStatus::Paused {
                    continue;
                }
                if task.kind != TaskKind::Torrent {
                    task.status = TaskStatus::Waiting;
                } else if torrent_gids.contains(&task.gid) {
                    task.status = TaskStatus::Active;
                    self.send_download_start(&task.gid);
                }
            }
        }
        self.try_start_next().await;
    }

    pub async fn resolve_gid(&self, prefix: &str) -> Result<String, String> {
        let tasks = self.tasks.read().await;

        if tasks.iter().any(|t| t.gid == prefix) {
            return Ok(prefix.to_string());
        }

        if prefix.len() < 4 {
            return Err(format!("GID prefix too short: {prefix} (minimum 4 chars)"));
        }

        let matches: Vec<&str> = tasks
            .iter()
            .filter(|t| t.gid.starts_with(prefix))
            .map(|t| t.gid.as_str())
            .collect();

        match matches.len() {
            0 => Err(format!("Task {prefix} not found")),
            1 => Ok(matches[0].to_string()),
            _ => Err(format!(
                "Ambiguous GID prefix {prefix}, matches: {}",
                matches.join(", ")
            )),
        }
    }

    pub async fn shutdown(&self) {
        tracing::info!("Engine shutting down");
        self.shutting_down.store(true, Ordering::Release);
        let active = self.active_downloads.read().await;
        for ad in active.values() {
            ad.cancel_token.cancel();
        }
        drop(active);
        cancel_all_starting_workers(&self.starting_workers);

        let deadline = tokio::time::Instant::now() + SHUTDOWN_WORKER_WAIT;
        while tokio::time::Instant::now() < deadline {
            let idle = self.active_downloads.read().await.is_empty()
                && self.starting_workers.lock().is_empty();
            if idle {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        if let Err(e) = self.save_session().await {
            tracing::error!("Failed to save session on shutdown: {}", e);
        }

        let mut te_guard = self.torrent_engine.write().await;
        if let Some(mut te) = te_guard.take() {
            te.shutdown().await;
        }

        let runtime = self.kad_runtime.read().clone();
        if let KadRuntime::Running(service) = &runtime {
            service.shutdown().await;
        }
    }
}

fn infer_m3u8_output_name(uri: &str) -> String {
    let path = uri.split('?').next().unwrap_or(uri);
    let path = path.split('#').next().unwrap_or(path);
    let name = path.rsplit('/').next().unwrap_or("download");

    if let Some(stem) = name
        .strip_suffix(".m3u8")
        .or_else(|| name.strip_suffix(".m3u"))
    {
        format!("{stem}.ts")
    } else if name.is_empty() {
        "download.ts".to_string()
    } else {
        format!("{name}.ts")
    }
}

fn looks_like_url(path: &str) -> bool {
    path.starts_with("http://")
        || path.starts_with("https://")
        || path.starts_with("ftp://")
        || path.starts_with("ed2k://")
}

fn is_retryable_magnet_resolution_error(err: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    lower.contains("failed to fetch metadata")
        || lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("no peers")
        || lower.contains("no seeds")
}

fn resolve_seed_goal(
    opts: &Map<String, Value>,
    g_manual: bool,
    g_seed_time: u64,
    g_seed_ratio: f64,
) -> (bool, u64, f64) {
    let manual = opts
        .get("keep-seeding")
        .and_then(super::options::json_bool)
        .unwrap_or(g_manual);
    let seed_time = opts
        .get("seed-time")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
        })
        .unwrap_or(g_seed_time);
    let seed_ratio = opts
        .get("seed-ratio")
        .and_then(|v| {
            v.as_f64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(g_seed_ratio);
    let keep = manual || seed_time > 0 || seed_ratio > 0.0;
    let eff_time = if manual { 0 } else { seed_time };
    let eff_ratio = if manual { 0.0 } else { seed_ratio };
    (keep, eff_time, eff_ratio)
}

fn sync_peer_infos(target: &mut Vec<PeerInfo>, peers: &[torrent::PeerSnapshot], num_pieces: u32) {
    *target = peers
        .iter()
        .map(|p| {
            let percent = if p.seeder {
                100
            } else if num_pieces > 0 {
                let full_bytes = (num_pieces / 8) as usize;
                let mut ones: u64 = p.bitfield[..full_bytes.min(p.bitfield.len())]
                    .iter()
                    .map(|b| u64::from(b.count_ones()))
                    .sum();
                let trailing_bits = num_pieces % 8;
                if trailing_bits > 0 {
                    if let Some(&b) = p.bitfield.get(full_bytes) {
                        let mask = 0xffu8 << (8 - trailing_bits);
                        ones += u64::from((b & mask).count_ones());
                    }
                }
                (ones * 100 / num_pieces as u64) as u8
            } else {
                0
            };
            PeerInfo {
                ip: p.addr.ip().to_string(),
                port: p.addr.port().to_string(),
                percent,
                am_choking: p.am_choking.to_string(),
                peer_choking: p.peer_choking.to_string(),
                seeder: p.seeder.to_string(),
                peer_id: p
                    .peer_id
                    .map(|id| {
                        percent_encoding::percent_encode(&id, percent_encoding::NON_ALPHANUMERIC)
                            .to_string()
                    })
                    .unwrap_or_default(),
                peer_client_name: p.client.clone().unwrap_or_default(),
                am_interested: p.am_interested.to_string(),
                peer_interested: p.peer_interested.to_string(),
                download_speed: p.dl_speed,
                upload_speed: p.up_speed,
                downloaded: p.downloaded,
                uploaded: p.uploaded,
                progress: p.progress,
                incoming: p.incoming,
                snubbed: p.snubbed,
                handshaking: false,
                optimistic_unchoke: p.optimistic_unchoke,
                bitfield: String::new(),
                raw_bitfield: p.bitfield.clone(),
            }
        })
        .collect();
}

fn torrent_files_signature(base_dir: &str, select_file: Option<&str>, file_count: usize) -> u64 {
    use std::hash::{DefaultHasher, Hash, Hasher};
    let mut h = DefaultHasher::new();
    base_dir.hash(&mut h);
    select_file.hash(&mut h);
    file_count.hash(&mut h);
    h.finish()
}

fn refresh_torrent_file_progress(
    target: &mut [DownloadFile],
    file_details: &[torrent::TorrentFileInfo],
    file_progress: &[u64],
) -> (u64, u64) {
    use std::fmt::Write;
    let mut selected_total = 0;
    let mut selected_completed = 0;
    for (f, fd) in target.iter_mut().zip(file_details) {
        let completed = file_progress.get(fd.torrent_index).copied().unwrap_or(0);
        f.completed_length.clear();
        let _ = write!(f.completed_length, "{completed}");
        if f.selected == "true" {
            selected_total += fd.length;
            selected_completed += completed;
        }
    }
    (selected_total, selected_completed)
}

fn sync_torrent_files(
    target: &mut Vec<DownloadFile>,
    file_details: &[torrent::TorrentFileInfo],
    file_progress: &[u64],
    base_dir: &str,
    selected_indices: Option<&std::collections::HashSet<usize>>,
) -> (u64, u64) {
    let mut selected_total = 0;
    let mut selected_completed = 0;
    *target = file_details
        .iter()
        .map(|fd| {
            let completed = file_progress.get(fd.torrent_index).copied().unwrap_or(0);
            let is_selected = selected_indices.is_none_or(|set| set.contains(&fd.index));
            if is_selected {
                selected_total += fd.length;
                selected_completed += completed;
            }
            DownloadFile {
                index: (fd.index + 1).to_string(),
                path: format!("{}/{}", base_dir, torrent::disk_path(&fd.path)),
                length: fd.length.to_string(),
                completed_length: completed.to_string(),
                selected: is_selected.to_string(),
                uris: Vec::new(),
            }
        })
        .collect();

    (selected_total, selected_completed)
}

#[cfg(test)]
mod tests {
    #[test]
    fn metalink_file_concurrency_follows_global_limit() {
        let mut o = Map::new();
        assert_eq!(metalink_file_concurrency(&o), 5);
        o.insert("max-concurrent-downloads".into(), serde_json::json!("3"));
        assert_eq!(metalink_file_concurrency(&o), 3);
        o.insert("max-concurrent-downloads".into(), serde_json::json!(0));
        assert_eq!(metalink_file_concurrency(&o), 1);
    }

    use super::*;
    use base64::Engine as _;

    #[test]
    fn refresh_torrent_file_progress_updates_in_place() {
        let details = vec![
            torrent::TorrentFileInfo {
                index: 0,
                torrent_index: 0,
                path: "a".into(),
                length: 10,
            },
            torrent::TorrentFileInfo {
                index: 1,
                torrent_index: 1,
                path: "b".into(),
                length: 20,
            },
        ];
        let mut files = Vec::new();
        let sel: std::collections::HashSet<usize> = [1].into();
        sync_torrent_files(&mut files, &details, &[1, 2], "/d", Some(&sel));
        let (total, done) = refresh_torrent_file_progress(&mut files, &details, &[5, 7]);
        assert_eq!((total, done), (20, 7));
        assert_eq!(files[0].completed_length, "5");
        assert_eq!(files[1].selected, "true");
        assert_eq!(files[0].selected, "false");
    }

    #[test]
    fn peer_percent_from_bitfield() {
        let snap = torrent::PeerSnapshot {
            addr: "1.2.3.4:6881".parse().unwrap(),
            bitfield: std::sync::Arc::from([0xFFu8, 0xF0].as_slice()),
            am_choking: false,
            am_interested: false,
            peer_choking: false,
            peer_interested: false,
            seeder: false,
            peer_id: Some(*b"-RS0001-0123456789ab"),
            client: Some("risuko-bt".into()),
            downloaded: 100,
            uploaded: 50,
            dl_speed: 10,
            up_speed: 5,
            incoming: false,
            snubbed: false,
            progress: 0.92,
            optimistic_unchoke: false,
        };
        let peers = [snap];
        let mut target = Vec::new();

        sync_peer_infos(&mut target, &peers, 13);
        assert_eq!(target[0].percent, 92);
        assert_eq!(target[0].ip, "1.2.3.4");
        assert_eq!(target[0].port, "6881");
        assert_eq!(target[0].peer_client_name, "risuko-bt");
        assert_eq!(target[0].peer_id, "%2DRS0001%2D0123456789ab");
        assert_eq!(target[0].downloaded, 100);
        assert_eq!(target[0].uploaded, 50);
        assert_eq!(target[0].download_speed, 10);
        assert_eq!(target[0].upload_speed, 5);
        assert!(!target[0].incoming);
        let json = serde_json::to_value(&target[0]).unwrap();
        assert_eq!(json["peerClientName"], "risuko-bt");
        assert_eq!(json["downloadSpeed"], 10);
        assert_eq!(json["incoming"], false);

        sync_peer_infos(&mut target, &peers, 0);
        assert_eq!(target[0].percent, 0);

        let padded = torrent::PeerSnapshot {
            addr: "1.2.3.4:6881".parse().unwrap(),
            bitfield: std::sync::Arc::from([0xFFu8, 0x07].as_slice()),
            am_choking: false,
            am_interested: false,
            peer_choking: false,
            peer_interested: false,
            seeder: false,
            peer_id: None,
            client: None,
            downloaded: 0,
            uploaded: 0,
            dl_speed: 0,
            up_speed: 0,
            incoming: true,
            snubbed: false,
            progress: 0.0,
            optimistic_unchoke: false,
        };
        sync_peer_infos(&mut target, &[padded], 13);
        assert_eq!(target[0].percent, 61);
    }

    #[test]
    fn parse_cf_host_extracts_host_from_marker() {
        assert_eq!(
            parse_cf_host("[cloudflare-challenge] host=www.spigotmc.org status=403").as_deref(),
            Some("www.spigotmc.org")
        );
    }

    #[test]
    fn parse_cf_host_returns_none_when_absent() {
        assert!(parse_cf_host("HTTP error: 403").is_none());
    }

    #[test]
    fn m3u8_name_strips_extension() {
        assert_eq!(
            infer_m3u8_output_name("http://example.com/video.m3u8"),
            "video.ts"
        );
    }

    #[test]
    fn m3u_name_strips_extension() {
        assert_eq!(
            infer_m3u8_output_name("http://example.com/video.m3u"),
            "video.ts"
        );
    }

    #[test]
    fn m3u8_name_ignores_query_string() {
        assert_eq!(
            infer_m3u8_output_name("http://example.com/video.m3u8?token=abc"),
            "video.ts"
        );
    }

    #[test]
    fn m3u8_name_no_extension() {
        assert_eq!(
            infer_m3u8_output_name("http://example.com/video"),
            "video.ts"
        );
    }

    #[test]
    fn m3u8_name_empty_segment() {
        assert_eq!(infer_m3u8_output_name("http://example.com/"), "download.ts");
    }

    #[test]
    fn m3u8_name_bare_name() {
        assert_eq!(infer_m3u8_output_name("download"), "download.ts");
    }

    #[test]
    fn looks_like_url_protocols() {
        assert!(looks_like_url("http://example.com"));
        assert!(looks_like_url("https://example.com"));
        assert!(looks_like_url("ftp://files.example.com"));
        assert!(looks_like_url("ed2k://|file|test|100|hash|/"));
    }

    #[test]
    fn looks_like_url_paths() {
        assert!(!looks_like_url("/path/to/file"));
        assert!(!looks_like_url("file.txt"));
        assert!(!looks_like_url(""));
    }

    #[test]
    fn change_option_select_file_is_normalized() {
        use serde_json::json;
        assert_eq!(
            normalize_select_file(&json!("1-2,4")),
            Ok(Some("1-2,4".into()))
        );
        assert_eq!(normalize_select_file(&json!(3)), Ok(Some("3".into())));
        assert_eq!(normalize_select_file(&json!("")), Ok(Some(String::new())));
        assert_eq!(normalize_select_file(&json!(null)), Ok(Some(String::new())));
        assert_eq!(normalize_select_file(&json!(true)), Ok(None));
        assert_eq!(normalize_select_file(&json!([1])), Ok(None));
        assert_eq!(normalize_select_file(&json!({"a": 1})), Ok(None));
        assert!(normalize_select_file(&json!("x")).is_err());
        assert!(normalize_select_file(&json!(0)).is_err());
        assert!(normalize_select_file(&json!(-2)).is_err());
    }

    #[test]
    fn per_task_seed_goal_overrides_global() {
        let mut opts = Map::new();
        opts.insert("seed-time".into(), serde_json::json!(30));
        let (keep, time, ratio) = resolve_seed_goal(&opts, false, 0, 0.0);
        assert!(keep);
        assert_eq!(time, 30);
        assert_eq!(ratio, 0.0);

        let mut opts = Map::new();
        opts.insert("seed-time".into(), serde_json::json!("45"));
        let (keep, time, _) = resolve_seed_goal(&opts, false, 0, 0.0);
        assert!(keep);
        assert_eq!(time, 45);

        let (keep, time, ratio) = resolve_seed_goal(&Map::new(), false, 10, 1.5);
        assert!(keep);
        assert_eq!(time, 10);
        assert_eq!(ratio, 1.5);

        let mut opts = Map::new();
        opts.insert("keep-seeding".into(), serde_json::json!(true));
        let (keep, time, ratio) = resolve_seed_goal(&opts, false, 99, 9.0);
        assert!(keep);
        assert_eq!(time, 0);
        assert_eq!(ratio, 0.0);

        let mut opts = Map::new();
        opts.insert("keep-seeding".into(), serde_json::json!("true"));
        assert_eq!(resolve_seed_goal(&opts, false, 99, 9.0), (true, 0, 0.0));
        opts.insert("keep-seeding".into(), serde_json::json!("false"));
        assert_eq!(resolve_seed_goal(&opts, true, 0, 0.0), (false, 0, 0.0));

        let mut opts = Map::new();
        opts.insert("seed-ratio".into(), serde_json::json!("2.0"));
        let (keep, _t, ratio) = resolve_seed_goal(&opts, false, 0, 0.0);
        assert!(keep);
        assert_eq!(ratio, 2.0);

        let (keep, _t, _r) = resolve_seed_goal(&Map::new(), false, 0, 0.0);
        assert!(!keep);
    }

    use tokio::sync::RwLock;

    fn make_test_manager(tasks: Vec<DownloadTask>) -> TaskManager {
        let dir = tempfile::TempDir::new().unwrap();
        let session = SessionManager::new(dir.path());
        let options = EngineOptions::from_config(&Map::new(), &Map::new());
        let events = EventBroadcaster::new(16);
        let global_speed_limiter = Arc::new(SpeedLimiter::new(0));
        let cookie_store = Arc::new(CookieStore::new(dir.path()));

        TaskManager {
            config_dir: dir.path().to_path_buf(),
            p2p_reload_lock: tokio::sync::Mutex::new(()),
            p2p_route_generation: Arc::new(AtomicU64::new(0)),
            torrent_resume_seq: AtomicU64::new(0),
            tasks: Arc::new(RevLock::new(tasks)),
            saved_rev: AtomicU64::new(u64::MAX),
            active_downloads: Arc::new(RwLock::new(HashMap::new())),
            starting_workers: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            updating_tasks: Arc::new(parking_lot::Mutex::new(HashSet::new())),
            torrent_ids: Arc::new(RwLock::new(HashMap::new())),
            pending_magnets: Arc::new(RwLock::new(HashSet::new())),
            options: Arc::new(RwLock::new(options)),
            events,
            session: Arc::new(session),
            save_gate: tokio::sync::Mutex::new(()),
            shutting_down: std::sync::atomic::AtomicBool::new(false),
            torrent_engine: Arc::new(RwLock::new(None)),
            global_speed_limiter,
            global_upload_limiter: Arc::new(SpeedLimiter::new(0)),
            task_limiters: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            cookie_store,
            usenet_connection_capacity: Arc::new(ProviderConnectionCapacityRegistry::default()),
            kad_runtime: Arc::new(parking_lot::RwLock::new(KadRuntime::Disabled {
                port: 4672,
            })),
        }
    }

    #[tokio::test]
    async fn finish_task_retains_terminal_ed2k_kad_status_without_progress_tick() {
        let mut task = DownloadTask::new_ed2k(
            "ed2k-kad-finish".into(),
            "ed2k://|file|test.bin|1024|0123456789abcdef0123456789abcdef|/".into(),
            "test.bin".into(),
            1024,
            "/downloads".into(),
            None,
            Map::new(),
        );
        task.status = TaskStatus::Active;
        let manager = make_test_manager(vec![task]);
        let counters = Counters::new(1024, 0);
        let worker_epoch = next_worker_epoch();
        let active_download = counters.to_active(
            worker_epoch,
            Vec::new(),
            Arc::new(parking_lot::Mutex::new(None)),
        );
        *active_download.kad_status.lock() = Some(KadLookupStatus {
            state: KadState::Ready,
            queried_nodes: 9,
            discovered_sources: 4,
            contacts: 3,
            error: None,
        });
        manager
            .active_downloads
            .write()
            .await
            .insert("ed2k-kad-finish".into(), active_download);

        finish_task(
            &manager.tasks,
            &manager.active_downloads,
            &manager.events,
            "ed2k-kad-finish",
            worker_epoch,
            "ed2k",
            &counters,
            Err("server exhausted".into()),
            |_| {},
            |task, _| task.total_length,
            |_, error| classify_error(error, "ed2k"),
        )
        .await;

        let tasks = manager.tasks.read().await;
        let status = tasks[0].ed2k_kad.as_ref().expect("final Kad status");
        assert_eq!(status.state, "complete");
        assert_eq!(status.queried_nodes, 9);
        assert_eq!(status.discovered_sources, 4);
        assert!(manager.active_downloads.read().await.is_empty());
    }

    #[test]
    fn cancelled_ed2k_kad_lookup_is_not_serialized_as_complete() {
        let status = KadLookupStatus {
            state: KadState::Stopped,
            ..KadLookupStatus::default()
        };

        assert_eq!(ed2k_kad_task_status(&status).state, "disabled");
    }

    #[test]
    fn usenet_failure_records_a_structured_par2_summary() {
        let mut task = DownloadTask::new_usenet(
            "ugid".into(),
            "/dl".into(),
            None,
            None,
            Map::new(),
            Vec::new(),
        );
        let repair_failure = UsenetRepairFailure {
            needed_blocks: 184,
            available_blocks: 62,
            partials_retained: true,
        };

        let error_code = finish_usenet_failure(
            &mut task,
            "PAR2 recovery is insufficient: need 184 blocks, have 62",
            Some(repair_failure.clone()),
        );

        assert_eq!(error_code.to_string(), "554");
        assert_eq!(task.usenet_stage.as_deref(), Some("error"));
        assert_eq!(task.usenet_repair_failure, Some(repair_failure));
    }

    #[test]
    fn usenet_stage_updates_cannot_regress() {
        assert!(should_update_usenet_stage(Some("assembling"), "repairing"));
        assert!(!should_update_usenet_stage(Some("verifying"), "assembling"));
        assert!(!should_update_usenet_stage(Some("complete"), "fetching"));
        assert!(!should_update_usenet_stage(Some("error"), "complete"));
        assert!(should_update_usenet_stage(None, "fetching"));
    }

    #[tokio::test]
    async fn save_session_skips_until_a_write_bumps_rev() {
        let mgr = make_test_manager(vec![make_task("g1", TaskStatus::Complete)]);

        mgr.save_session().await.unwrap();
        let rev = mgr.tasks.rev();
        assert_eq!(mgr.saved_rev.load(Ordering::Relaxed), rev);

        mgr.save_session().await.unwrap();
        assert_eq!(mgr.tasks.rev(), rev, "no write must not bump rev");

        mgr.tasks
            .write()
            .await
            .push(make_task("g2", TaskStatus::Complete));
        assert!(mgr.tasks.rev() > rev, "write must bump rev");
        mgr.save_session().await.unwrap();
        assert_eq!(mgr.saved_rev.load(Ordering::Relaxed), mgr.tasks.rev());
    }

    fn make_task(gid: &str, status: TaskStatus) -> DownloadTask {
        let mut task = DownloadTask::new_http(
            gid.into(),
            vec!["http://example.com/f.bin".into()],
            "/dl".into(),
            None,
            Map::new(),
        );
        task.status = status;
        task
    }

    fn make_torrent_task(gid: &str, status: TaskStatus) -> DownloadTask {
        let mut task = make_task(gid, status);
        task.kind = TaskKind::Torrent;
        task
    }

    #[tokio::test]
    async fn unpause_clears_a_failed_torrent_and_marks_the_resume() {
        let mut failed = make_torrent_task("t", TaskStatus::Error);
        failed.error_code = Some("1".into());
        failed.error_message = Some("Disk write failed: No space left on device".into());
        let mgr = make_test_manager(vec![failed]);

        mgr.unpause("t").await.unwrap();

        let tasks = mgr.tasks.read().await;
        assert_eq!(tasks[0].status, TaskStatus::Active);
        assert!(tasks[0].error_message.is_none());
        assert_eq!(mgr.torrent_resume_seq.load(Ordering::SeqCst), 1);
        drop(tasks);
        assert!(mgr.unpause("t").await.is_err());
    }

    #[tokio::test]
    async fn unpause_all_resumes_paused_torrents_and_leaves_failed_ones() {
        let mgr = make_test_manager(vec![
            make_torrent_task("p", TaskStatus::Paused),
            make_torrent_task("e", TaskStatus::Error),
        ]);

        mgr.unpause_all().await;

        let tasks = mgr.tasks.read().await;
        assert_eq!(tasks[0].status, TaskStatus::Active);
        assert_eq!(tasks[1].status, TaskStatus::Error);
        assert_eq!(mgr.torrent_resume_seq.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn move_tasks_moves_the_block_next_to_the_target() {
        let mgr = make_test_manager(vec![
            make_task("a", TaskStatus::Paused),
            make_task("b", TaskStatus::Paused),
            make_task("c", TaskStatus::Paused),
            make_task("d", TaskStatus::Paused),
        ]);

        mgr.move_tasks(&["c".into(), "d".into()], "a", false)
            .await
            .unwrap();

        let order: Vec<String> = mgr
            .tasks
            .read()
            .await
            .iter()
            .map(|t| t.gid.clone())
            .collect();
        assert_eq!(order, vec!["c", "d", "a", "b"]);
    }

    #[tokio::test]
    async fn move_tasks_errors_when_target_is_stale() {
        let mgr = make_test_manager(vec![
            make_task("a", TaskStatus::Paused),
            make_task("b", TaskStatus::Paused),
        ]);

        let err = mgr
            .move_tasks(&["b".into()], "missing", false)
            .await
            .unwrap_err();
        assert!(err.contains("target task missing not found"));

        let order: Vec<String> = mgr
            .tasks
            .read()
            .await
            .iter()
            .map(|t| t.gid.clone())
            .collect();
        assert_eq!(order, vec!["a", "b"]);
    }

    #[tokio::test]
    async fn check_scheduled_promotes_due_and_flags_missed() {
        let now = crate::engine::util::now_secs();
        let mut due = make_task("due", TaskStatus::Scheduled);
        due.start_at = Some(now.saturating_sub(5));
        let mut missed = make_task("missed", TaskStatus::Scheduled);
        missed.start_at = Some(now.saturating_sub(10_000));
        let mut future = make_task("future", TaskStatus::Scheduled);
        future.start_at = Some(now + 10_000);

        let mgr = make_test_manager(vec![due, missed, future]);
        mgr.check_scheduled_tasks().await;

        let tasks = mgr.tasks.read().await;
        let get = |gid: &str| tasks.iter().find(|t| t.gid == gid).unwrap();
        assert_eq!(get("due").status, TaskStatus::Waiting);
        assert!(get("due").start_at.is_none());
        assert_eq!(get("missed").status, TaskStatus::Scheduled);
        assert!(get("missed").schedule_missed);
        assert_eq!(get("future").status, TaskStatus::Scheduled);
        assert!(!get("future").schedule_missed);
    }

    #[tokio::test]
    async fn update_progress_runs_scheduler_when_only_scheduled_tasks_exist() {
        let now = crate::engine::util::now_secs();
        let mut due = make_task("due", TaskStatus::Scheduled);
        due.kind = TaskKind::Torrent;
        due.start_at = Some(now.saturating_sub(5));
        let mgr = make_test_manager(vec![due]);

        mgr.update_progress().await;

        let tasks = mgr.tasks.read().await;
        let task = tasks.iter().find(|t| t.gid == "due").unwrap();
        assert_eq!(task.status, TaskStatus::Waiting);
        assert!(task.start_at.is_none());
    }

    #[tokio::test]
    async fn global_limit_options_retune_every_budget_live() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let patch = |entries: &[(&str, Value)]| {
            entries
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect::<Map<String, Value>>()
        };
        mgr.change_global_option(patch(&[
            ("max-overall-download-limit", Value::from("1M")),
            ("max-overall-upload-limit", Value::from("2M")),
        ]))
        .await;
        assert_eq!(mgr.global_speed_limiter.limit_bps(), 1024 * 1024);
        assert_eq!(mgr.global_upload_limiter.limit_bps(), 2 * 1024 * 1024);

        mgr.change_global_option(patch(&[("bt-upload-rate-limit", Value::from("512K"))]))
            .await;
        assert_eq!(mgr.global_upload_limiter.limit_bps(), 512 * 1024);
        assert_eq!(mgr.global_speed_limiter.limit_bps(), 1024 * 1024);

        mgr.change_global_option(patch(&[
            ("max-overall-download-limit", Value::from(0)),
            ("max-overall-upload-limit", Value::from("256K")),
        ]))
        .await;
        assert_eq!(mgr.global_speed_limiter.limit_bps(), 0);
        assert_eq!(mgr.global_upload_limiter.limit_bps(), 256 * 1024);

        mgr.change_global_option(patch(&[
            ("max-overall-upload-limit", Value::from(0)),
            ("bt-upload-rate-limit", Value::from(0)),
        ]))
        .await;
        assert_eq!(mgr.global_upload_limiter.limit_bps(), 0);
    }

    #[tokio::test]
    async fn bt_max_connections_option_applies_live_and_clamps() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let engine = mgr.torrent_engine.read().await.clone().unwrap();
        let expect = |n: usize| risuko_bt::conn_budget::session_limit(Some(n));
        let set = |v: Value| {
            let mut m = Map::new();
            m.insert("bt-max-connections".to_string(), v);
            m
        };
        assert_eq!(engine.max_connections(), Some(expect(400)));
        mgr.change_global_option(set(Value::from(50))).await;
        assert_eq!(engine.max_connections(), Some(expect(50)));
        mgr.change_global_option(set(Value::from(1))).await;
        assert_eq!(engine.max_connections(), Some(expect(20)));
        mgr.change_global_option(set(Value::from(900_000))).await;
        assert_eq!(engine.max_connections(), Some(expect(5000)));
    }

    #[tokio::test]
    async fn change_option_retunes_the_running_task_limiter() {
        let mut task = DownloadTask::new_http(
            "limited".into(),
            vec!["https://a.example/file.bin".into()],
            "/dl".into(),
            None,
            Map::new(),
        );
        task.status = TaskStatus::Active;
        let mgr = make_test_manager(vec![task]);
        let limiter = mgr.task_download_limiter("limited", 0);
        assert_eq!(limiter.limit_bps(), 0);

        let mut patch = Map::new();
        patch.insert("max-download-limit".into(), Value::from("100K"));
        mgr.change_option("limited", patch).await.unwrap();
        assert_eq!(limiter.limit_bps(), 100 * 1024);

        let mut patch = Map::new();
        patch.insert("max-download-limit".into(), Value::from("0"));
        mgr.change_option("limited", patch).await.unwrap();
        assert_eq!(limiter.limit_bps(), 0);
    }

    #[test]
    fn finished_workers_drop_out_of_the_limiter_registry() {
        let registry = parking_lot::Mutex::new(HashMap::new());
        let kept = register_task_limiter(&registry, "kept", 10);
        drop(register_task_limiter(&registry, "gone", 10));
        let _third = register_task_limiter(&registry, "third", 10);
        let map = registry.lock();
        assert!(map.contains_key("kept"));
        assert!(!map.contains_key("gone"));
        assert!(map.contains_key("third"));
        assert_eq!(kept.limit_bps(), 10);
    }

    async fn make_test_manager_with_engine() -> (TaskManager, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let mut system = Map::new();
        system.insert(
            "dir".to_string(),
            Value::String(dir.path().join("downloads").to_string_lossy().to_string()),
        );
        system.insert("bt-enable-upnp".to_string(), Value::Bool(false));
        system.insert("bt-enable-lsd".to_string(), Value::Bool(false));
        let options = EngineOptions::from_config(&system, &Map::new());
        let manager = TaskManager::new(dir.path(), options, EventBroadcaster::new(16))
            .await
            .unwrap();
        (manager, dir)
    }

    #[tokio::test]
    async fn add_magnet_task_returns_before_metadata_is_resolved() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let uri = "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&dn=Metadata+Later";
        let started = std::time::Instant::now();

        let gid = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            mgr.add_magnet_task(uri, Map::new()),
        )
        .await
        .expect("add_magnet_task should not wait for metadata")
        .expect("valid magnet should create a task");

        assert!(started.elapsed() < std::time::Duration::from_secs(2));
        assert!(!mgr.torrent_ids.read().await.contains_key(&gid));
        assert!(mgr.pending_magnets.read().await.contains(&gid));

        let tasks = mgr.tasks.read().await;
        let task = tasks.iter().find(|task| task.gid == gid).unwrap();
        assert_eq!(task.status, TaskStatus::Active);
        assert_eq!(
            task.info_hash.as_deref(),
            Some("cab507494d02ebb1178b38f2e9d7be299c86b862")
        );
        assert_eq!(task.bt_name.as_deref(), Some("Metadata Later"));
        assert_eq!(task.uris, vec![uri.to_string()]);
    }

    #[tokio::test]
    async fn add_http_task_decodes_raw_thunder_magnet() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let encoded = base64::engine::general_purpose::STANDARD.encode(
            "AAmagnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&amp;dn=ExampleZZ",
        );
        let mut options = Map::new();
        options.insert("pause".to_string(), Value::Bool(true));

        let gid = mgr
            .add_http_task(vec![format!("THUNDER://{encoded}")], options)
            .await
            .expect("raw Thunder magnet should create a task");

        let tasks = mgr.tasks.read().await;
        let task = tasks.iter().find(|task| task.gid == gid).unwrap();
        assert_eq!(task.kind, TaskKind::Torrent);
        assert_eq!(
            task.info_hash.as_deref(),
            Some("cab507494d02ebb1178b38f2e9d7be299c86b862")
        );
        assert_eq!(task.bt_name.as_deref(), Some("Example"));
        assert_eq!(
            task.uris,
            vec![
                "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&dn=Example"
                    .to_string()
            ]
        );
    }

    #[tokio::test]
    async fn add_http_task_decodes_raw_thunder_http_uri() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let encoded = base64::engine::general_purpose::STANDARD
            .encode("AAhttps://example.com/downloads/example.binZZ");
        let mut options = Map::new();
        options.insert("pause".to_string(), Value::Bool(true));

        let gid = mgr
            .add_http_task(vec![format!("THUNDER://{encoded}")], options)
            .await
            .expect("raw Thunder HTTP URI should create a task");

        let tasks = mgr.tasks.read().await;
        let task = tasks.iter().find(|task| task.gid == gid).unwrap();
        assert_eq!(task.kind, TaskKind::Http);
        assert_eq!(
            task.uris,
            vec!["https://example.com/downloads/example.bin".to_string()]
        );
    }

    #[tokio::test]
    async fn add_http_task_rejects_unsupported_thunder_uri_payload() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let encoded = base64::engine::general_purpose::STANDARD.encode("AAfile:///tmp/exampleZZ");

        let error = mgr
            .add_http_task(vec![format!("thunder://{encoded}")], Map::new())
            .await
            .expect_err("Thunder payloads must be magnet or HTTP(S) URIs");

        assert_eq!(error, "Unsupported Thunder URI payload");
        assert!(mgr.tasks.read().await.is_empty());
    }

    #[tokio::test]
    async fn add_http_task_rejects_invalid_thunder_uri() {
        let (mgr, _dir) = make_test_manager_with_engine().await;

        let error = mgr
            .add_http_task(vec!["thunder://not%base64".to_string()], Map::new())
            .await
            .expect_err("invalid Thunder URIs must not create tasks");

        assert_eq!(error, "Invalid Thunder URI");
        assert!(mgr.tasks.read().await.is_empty());
    }

    #[tokio::test]
    async fn add_http_task_rejects_non_http_scheme() {
        let mgr = make_test_manager(Vec::new());
        let error = mgr
            .add_http_task(vec!["gopher://example.com/x".to_string()], Map::new())
            .await
            .expect_err("non-HTTP schemes must not become HTTP tasks");
        assert!(error.contains("Unsupported URI scheme"), "{error}");
        assert!(mgr.tasks.read().await.is_empty());
    }

    #[tokio::test]
    async fn add_http_task_honours_pause_option() {
        let mgr = make_test_manager(Vec::new());
        let mut options = Map::new();
        options.insert("pause".to_string(), Value::String("true".to_string()));
        let gid = mgr
            .add_http_task(vec!["http://example.com/file.bin".to_string()], options)
            .await
            .unwrap();
        let status = mgr
            .tasks
            .read()
            .await
            .iter()
            .find(|t| t.gid == gid)
            .map(|t| t.status);
        assert_eq!(status, Some(TaskStatus::Paused));
    }

    #[test]
    fn metalink_completion_follows_file_state() {
        let file = |selected: &str, len: u64, done: u64| DownloadFile {
            index: "1".to_string(),
            path: "/dl/a".to_string(),
            length: len.to_string(),
            completed_length: done.to_string(),
            selected: selected.to_string(),
            uris: Vec::new(),
        };
        let mut task = DownloadTask::new_http(
            "g".into(),
            vec!["http://example.com/a".into()],
            "/dl".into(),
            None,
            Map::new(),
        );
        task.files = vec![
            file("true", 10, 10),
            file("true", 10, 4),
            file("false", 0, 0),
        ];
        let ok = |idx: usize| (idx, "n".to_string(), Ok(std::path::PathBuf::from("/dl/n")));

        assert!(!metalink_all_selected_done(&task, &[ok(0)]));
        assert!(metalink_all_selected_done(&task, &[ok(1)]));
        task.files[1] = file("true", 10, 10);
        assert!(metalink_all_selected_done(&task, &[]));
        task.files = vec![file("false", 0, 0)];
        assert!(!metalink_all_selected_done(&task, &[]));
    }

    #[test]
    fn magnet_retry_delay_backs_off_and_caps() {
        assert_eq!(magnet_retry_delay(0), Duration::from_secs(15));
        assert_eq!(magnet_retry_delay(1), Duration::from_secs(30));
        assert_eq!(magnet_retry_delay(3), Duration::from_secs(120));
        assert_eq!(
            magnet_retry_delay(60),
            Duration::from_secs(MAGNET_METADATA_RETRY_MAX_DELAY_SECS)
        );
    }

    #[tokio::test]
    async fn shutdown_cancels_active_download_tokens() {
        let mgr = make_test_manager(Vec::new());
        let cancel_token = CancellationToken::new();
        mgr.active_downloads.write().await.insert(
            "gid1".to_string(),
            ActiveDownload {
                epoch: next_worker_epoch(),
                cancel_token: cancel_token.clone(),
                total: Arc::new(AtomicU64::new(0)),
                completed: Arc::new(AtomicU64::new(0)),
                speed: Arc::new(AtomicU64::new(0)),
                connections: Arc::new(AtomicU32::new(0)),
                chunk_completed: Vec::new(),
                adopted_filename: Arc::new(parking_lot::Mutex::new(None)),
                metalink_files: Vec::new(),
                kad_status: Arc::new(parking_lot::Mutex::new(None)),
            },
        );
        let starting_token = CancellationToken::new();
        mgr.starting_workers.lock().insert(
            "gid-starting".to_string(),
            (next_worker_epoch(), starting_token.clone()),
        );

        mgr.shutdown().await;

        assert!(cancel_token.is_cancelled());
        assert!(starting_token.is_cancelled());
    }

    fn mk_metalink_file(index: &str, len: u64, done: u64) -> DownloadFile {
        DownloadFile {
            index: index.to_string(),
            path: format!("/dl/{index}.bin"),
            length: len.to_string(),
            completed_length: done.to_string(),
            selected: "true".to_string(),
            uris: vec![FileUri {
                uri: "http://mirror/".into(),
                status: "waiting".into(),
            }],
        }
    }

    async fn run_metalink_finish(
        mgr: &TaskManager,
        gid: &str,
        results: Vec<(usize, String, Result<std::path::PathBuf, String>)>,
    ) {
        let epoch = next_worker_epoch();
        let fc: Vec<(usize, Counters)> = results
            .iter()
            .map(|(i, _, _)| (*i, Counters::new(0, 1)))
            .collect();
        let mut ad = Counters::new(0, 0).to_active(
            epoch,
            Vec::new(),
            Arc::new(parking_lot::Mutex::new(None)),
        );
        ad.metalink_files = fc.clone();
        mgr.active_downloads
            .write()
            .await
            .insert(gid.to_string(), ad);
        metalink_finish(
            &mgr.tasks,
            &mgr.active_downloads,
            &mgr.events,
            gid,
            epoch,
            fc,
            results,
        )
        .await;
    }

    #[tokio::test]
    async fn metalink_parks_paused_when_a_file_fails() {
        let mut task = DownloadTask::new_metalink(
            "gm".into(),
            "/dl".into(),
            None,
            Map::new(),
            vec![
                mk_metalink_file("1", 100, 100),
                mk_metalink_file("2", 100, 0),
            ],
        );
        task.status = TaskStatus::Active;
        let mgr = make_test_manager(vec![task]);

        run_metalink_finish(
            &mgr,
            "gm",
            vec![
                (0, "a".into(), Ok(std::path::PathBuf::from("/dl/a"))),
                (1, "b".into(), Err("all mirrors failed".into())),
            ],
        )
        .await;

        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "gm").unwrap();
        assert_eq!(t.status, TaskStatus::Paused);
        assert_ne!(t.status, TaskStatus::Complete);
        assert!(t.error_code.is_some());
    }

    #[tokio::test]
    async fn metalink_completes_when_all_files_ok() {
        let mut task = DownloadTask::new_metalink(
            "gm".into(),
            "/dl".into(),
            None,
            Map::new(),
            vec![
                mk_metalink_file("1", 100, 100),
                mk_metalink_file("2", 100, 100),
            ],
        );
        task.status = TaskStatus::Active;
        let mgr = make_test_manager(vec![task]);

        run_metalink_finish(
            &mgr,
            "gm",
            vec![
                (0, "a".into(), Ok(std::path::PathBuf::from("/dl/a"))),
                (1, "b".into(), Ok(std::path::PathBuf::from("/dl/b"))),
            ],
        )
        .await;

        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "gm").unwrap();
        assert_eq!(t.status, TaskStatus::Complete);
        assert!(t.error_code.is_none());
    }

    #[tokio::test]
    async fn metalink_all_cancelled_does_not_complete() {
        let mut task = DownloadTask::new_metalink(
            "gm".into(),
            "/dl".into(),
            None,
            Map::new(),
            vec![mk_metalink_file("1", 100, 0), mk_metalink_file("2", 100, 0)],
        );
        task.status = TaskStatus::Active;
        let mgr = make_test_manager(vec![task]);

        run_metalink_finish(
            &mgr,
            "gm",
            vec![
                (0, "a".into(), Err("download cancelled".into())),
                (1, "b".into(), Err("download cancelled".into())),
            ],
        )
        .await;

        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "gm").unwrap();
        assert_ne!(t.status, TaskStatus::Complete);
        assert_eq!(t.status, TaskStatus::Paused);
        assert!(t.error_code.is_none());
    }

    #[tokio::test]
    async fn tell_active_returns_only_active() {
        let mgr = make_test_manager(vec![
            make_task("a1", TaskStatus::Active),
            make_task("w1", TaskStatus::Waiting),
            make_task("a2", TaskStatus::Active),
        ]);

        let result = mgr.tell_active(&[]).await;
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0].get("gid").unwrap(), "a1");
        assert_eq!(arr[1].get("gid").unwrap(), "a2");
    }

    #[tokio::test]
    async fn tell_waiting_pagination() {
        let mgr = make_test_manager(vec![
            make_task("w1", TaskStatus::Waiting),
            make_task("w2", TaskStatus::Paused),
            make_task("w3", TaskStatus::Waiting),
            make_task("a1", TaskStatus::Active),
        ]);

        let result = mgr.tell_waiting(0, 2, &[]).await;
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0].get("gid").unwrap(), "w2");
        assert_eq!(arr[1].get("gid").unwrap(), "w3");

        let result = mgr.tell_waiting(1, 10, &[]).await;
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0].get("gid").unwrap(), "w1");
        assert_eq!(arr[1].get("gid").unwrap(), "w2");
    }

    #[tokio::test]
    async fn tell_waiting_newest_visible_past_cap() {
        let mut tasks = Vec::new();
        for i in 0..1005 {
            tasks.push(make_task(&format!("w{i}"), TaskStatus::Waiting));
        }
        let mgr = make_test_manager(tasks);

        let result = mgr.tell_waiting(0, 1000, &[]).await;
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 1000);
        assert_eq!(arr.last().unwrap().get("gid").unwrap(), "w1004");
        assert_eq!(arr.first().unwrap().get("gid").unwrap(), "w5");
    }

    #[tokio::test]
    async fn tell_waiting_negative_offset() {
        let mgr = make_test_manager(vec![
            make_task("w1", TaskStatus::Waiting),
            make_task("w2", TaskStatus::Waiting),
            make_task("w3", TaskStatus::Waiting),
        ]);

        let result = mgr.tell_waiting(-1, 10, &[]).await;
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0].get("gid").unwrap(), "w3");
    }

    #[tokio::test]
    async fn tell_stopped_filters_correctly() {
        let mgr = make_test_manager(vec![
            make_task("c1", TaskStatus::Complete),
            make_task("e1", TaskStatus::Error),
            make_task("w1", TaskStatus::Waiting),
            make_task("r1", TaskStatus::Removed),
        ]);

        let result = mgr.tell_stopped(0, 10, &[]).await;
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 3);
    }

    #[tokio::test]
    async fn get_global_stat_counts() {
        let mut active = make_task("a1", TaskStatus::Active);
        active.download_speed = 1000;
        active.upload_speed = 200;

        let mgr = make_test_manager(vec![
            active,
            make_task("w1", TaskStatus::Waiting),
            make_task("w2", TaskStatus::Paused),
            make_task("c1", TaskStatus::Complete),
        ]);

        let stat = mgr.get_global_stat().await;
        assert_eq!(stat.get("numActive").unwrap(), "1");
        assert_eq!(stat.get("numWaiting").unwrap(), "2");
        assert_eq!(stat.get("numStopped").unwrap(), "1");
        assert_eq!(stat.get("numCompleted").unwrap(), "1");
        assert_eq!(stat.get("numStoppedError").unwrap(), "0");
        assert_eq!(stat.get("numScheduled").unwrap(), "0");
        assert_eq!(stat.get("numPaused").unwrap(), "1");
        assert_eq!(stat.get("downloadSpeed").unwrap(), "1000");
        assert_eq!(stat.get("uploadSpeed").unwrap(), "200");
    }

    #[tokio::test]
    async fn get_global_stat_split_counts() {
        let mgr = make_test_manager(vec![
            make_task("s1", TaskStatus::Scheduled),
            make_task("w1", TaskStatus::Waiting),
            make_task("c1", TaskStatus::Complete),
            make_task("e1", TaskStatus::Error),
            make_task("r1", TaskStatus::Removed),
        ]);

        let stat = mgr.get_global_stat().await;
        assert_eq!(stat.get("numWaiting").unwrap(), "2");
        assert_eq!(stat.get("numScheduled").unwrap(), "1");
        assert_eq!(stat.get("numStopped").unwrap(), "3");
        assert_eq!(stat.get("numCompleted").unwrap(), "1");
        assert_eq!(stat.get("numStoppedError").unwrap(), "2");
    }

    #[tokio::test]
    async fn tell_status_found_and_not_found() {
        let mgr = make_test_manager(vec![make_task("gid1", TaskStatus::Active)]);

        let result = mgr.tell_status("gid1", &[]).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().get("gid").unwrap(), "gid1");

        let result = mgr.tell_status("nonexistent", &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn change_position_set() {
        let mgr = make_test_manager(vec![
            make_task("w1", TaskStatus::Waiting),
            make_task("w2", TaskStatus::Waiting),
            make_task("w3", TaskStatus::Waiting),
        ]);

        let result = mgr.change_position("w3", 0, "POS_SET").await;
        assert!(result.is_ok());

        let waiting = mgr.tell_waiting(0, 10, &[]).await;
        let arr = waiting.as_array().unwrap();
        assert_eq!(arr[0].get("gid").unwrap(), "w3");
    }

    #[tokio::test]
    async fn change_position_cur() {
        let mgr = make_test_manager(vec![
            make_task("w1", TaskStatus::Waiting),
            make_task("w2", TaskStatus::Waiting),
            make_task("w3", TaskStatus::Waiting),
        ]);

        let result = mgr.change_position("w1", 1, "POS_CUR").await;
        assert!(result.is_ok());

        let waiting = mgr.tell_waiting(0, 10, &[]).await;
        let arr = waiting.as_array().unwrap();
        assert_eq!(arr[0].get("gid").unwrap(), "w2");
        assert_eq!(arr[1].get("gid").unwrap(), "w1");
    }

    #[tokio::test]
    async fn change_position_end() {
        let mgr = make_test_manager(vec![
            make_task("w1", TaskStatus::Waiting),
            make_task("w2", TaskStatus::Waiting),
            make_task("w3", TaskStatus::Waiting),
        ]);

        let result = mgr.change_position("w1", 0, "POS_END").await;
        assert!(result.is_ok());

        let waiting = mgr.tell_waiting(0, 10, &[]).await;
        let arr = waiting.as_array().unwrap();
        assert_eq!(arr[0].get("gid").unwrap(), "w2");
        assert_eq!(arr[1].get("gid").unwrap(), "w3");
        assert_eq!(arr[2].get("gid").unwrap(), "w1");
    }

    #[tokio::test]
    async fn update_task_applies_uris_dir_out_and_options() {
        let mut task = DownloadTask::new_http(
            "edit1".into(),
            vec!["https://a.example/file.bin".into()],
            "/old".into(),
            None,
            Map::new(),
        );
        task.out = "old.bin".into();
        task.status = TaskStatus::Paused;
        let mgr = make_test_manager(vec![task]);

        let mut opts = Map::new();
        opts.insert("split".into(), Value::from(8));
        opts.insert(
            "all-proxy".into(),
            Value::String("http://proxy:8080".into()),
        );

        let outcome = mgr
            .update_task(
                "edit1",
                TaskPatch {
                    uris: Some(vec![
                        "https://b.example/file.bin".into(),
                        "https://c.example/file.bin".into(),
                    ]),
                    dir: Some("/new".into()),
                    out: Some("new.bin".into()),
                    trackers: None,
                    options: Some(opts),
                },
            )
            .await
            .expect("update_task");

        assert!(!outcome.restarted);
        assert!(!outcome.progress_preserved);
        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "edit1").unwrap();
        assert_eq!(
            t.uris,
            vec![
                "https://b.example/file.bin".to_string(),
                "https://c.example/file.bin".to_string()
            ]
        );
        assert_eq!(t.dir, "/new");
        assert_eq!(t.out, "new.bin");
        assert_eq!(t.options.get("split").and_then(|v| v.as_u64()), Some(8));
        assert_eq!(
            t.options.get("all-proxy").and_then(|v| v.as_str()),
            Some("http://proxy:8080")
        );
        assert_eq!(t.files[0].uris.len(), 2);
        assert_eq!(t.files[0].path, "/new/new.bin");
    }

    #[tokio::test]
    async fn update_task_adding_mirrors_preserves_progress_flag() {
        let mut task = DownloadTask::new_http(
            "mirror1".into(),
            vec!["https://a.example/file.bin".into()],
            "/dl".into(),
            None,
            Map::new(),
        );
        task.out = "file.bin".into();
        task.status = TaskStatus::Paused;
        let mgr = make_test_manager(vec![task]);

        let outcome = mgr
            .update_task(
                "mirror1",
                TaskPatch {
                    uris: Some(vec![
                        "https://a.example/file.bin".into(),
                        "https://b.example/file.bin".into(),
                    ]),
                    ..Default::default()
                },
            )
            .await
            .expect("mirrors");

        assert!(outcome.progress_preserved);
        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "mirror1").unwrap();
        assert_eq!(t.uris.len(), 2);
    }

    #[tokio::test]
    async fn update_task_rejects_uris_on_torrent() {
        let mut task = DownloadTask::new_torrent("bt1".into(), "/dl".into(), None, Map::new());
        task.status = TaskStatus::Paused;
        let mgr = make_test_manager(vec![task]);

        let err = mgr
            .update_task(
                "bt1",
                TaskPatch {
                    uris: Some(vec!["https://example.com/x".into()]),
                    ..Default::default()
                },
            )
            .await
            .expect_err("uris on torrent");
        assert!(err.contains("URI") || err.contains("torrent"));
    }

    #[tokio::test]
    async fn update_task_rejects_dir_on_torrent() {
        let mut task = DownloadTask::new_torrent("bt2".into(), "/dl".into(), None, Map::new());
        task.status = TaskStatus::Paused;
        let mgr = make_test_manager(vec![task]);

        let err = mgr
            .update_task(
                "bt2",
                TaskPatch {
                    dir: Some("/elsewhere".into()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("dir on torrent");
        assert!(err.contains("save path") || err.contains("torrent"));
    }

    #[tokio::test]
    async fn update_task_allows_unchanged_torrent_dir_and_uris() {
        let mut task = DownloadTask::new_torrent("bt-same".into(), "/dl".into(), None, Map::new());
        task.status = TaskStatus::Paused;
        task.uris = vec!["magnet:?xt=urn:btih:abc".into()];
        task.bt_announce_list = vec![vec!["udp://a.example:80/announce".into()]];
        let mgr = make_test_manager(vec![task]);

        let outcome = mgr
            .update_task(
                "bt-same",
                TaskPatch {
                    uris: Some(vec!["magnet:?xt=urn:btih:abc".into()]),
                    dir: Some("/dl".into()),
                    trackers: Some(vec!["http://b.example/announce".into()]),
                    ..Default::default()
                },
            )
            .await
            .expect("restating current torrent fields must be allowed");
        assert_eq!(outcome.trackers_added, 1);
    }

    #[tokio::test]
    async fn update_task_rejects_ftp_uri_on_http_task() {
        let mut task = DownloadTask::new_http(
            "http1".into(),
            vec!["https://a.example/file.bin".into()],
            "/dl".into(),
            None,
            Map::new(),
        );
        task.status = TaskStatus::Paused;
        let mgr = make_test_manager(vec![task]);

        let err = mgr
            .update_task(
                "http1",
                TaskPatch {
                    uris: Some(vec!["ftp://example.com/file.bin".into()]),
                    ..Default::default()
                },
            )
            .await
            .expect_err("ftp on http");
        assert!(err.to_ascii_lowercase().contains("scheme") || err.contains("Unsupported"));
        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "http1").unwrap();
        assert_eq!(t.uris, vec!["https://a.example/file.bin".to_string()]);
    }

    #[tokio::test]
    async fn update_task_rejects_magnet_uri_on_http_task() {
        let mut task = DownloadTask::new_http(
            "http-magnet".into(),
            vec!["https://a.example/file.bin".into()],
            "/dl".into(),
            None,
            Map::new(),
        );
        task.status = TaskStatus::Paused;
        let mgr = make_test_manager(vec![task]);

        let err = mgr
            .update_task(
                "http-magnet",
                TaskPatch {
                    uris: Some(vec![
                        "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862".into(),
                    ]),
                    ..Default::default()
                },
            )
            .await
            .expect_err("magnet on http");
        assert!(err.to_ascii_lowercase().contains("scheme") || err.contains("Unsupported"));
        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "http-magnet").unwrap();
        assert_eq!(t.kind, TaskKind::Http);
        assert_eq!(t.uris, vec!["https://a.example/file.bin".to_string()]);
    }

    #[tokio::test]
    async fn update_task_relocates_every_metalink_file_path() {
        let mut task = DownloadTask::new_http(
            "multi1".into(),
            vec!["https://a.example/a.bin".into()],
            "/old".into(),
            None,
            Map::new(),
        );
        task.out = "a.bin".into();
        task.status = TaskStatus::Paused;
        task.files = vec![
            DownloadFile {
                index: "1".into(),
                path: "/old/a.bin".into(),
                length: "1".into(),
                completed_length: "0".into(),
                selected: "true".into(),
                uris: vec![FileUri {
                    uri: "https://a.example/a.bin".into(),
                    status: "used".into(),
                }],
            },
            DownloadFile {
                index: "2".into(),
                path: "/old/b.bin.part".into(),
                length: "1".into(),
                completed_length: "0".into(),
                selected: "true".into(),
                uris: vec![FileUri {
                    uri: "https://a.example/b.bin".into(),
                    status: "waiting".into(),
                }],
            },
        ];
        let mgr = make_test_manager(vec![task]);

        mgr.update_task(
            "multi1",
            TaskPatch {
                dir: Some("/new".into()),
                ..Default::default()
            },
        )
        .await
        .expect("dir change");

        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "multi1").unwrap();
        assert_eq!(t.files[0].path, "/new/a.bin");
        assert_eq!(t.files[1].path, "/new/b.bin");
    }

    #[tokio::test]
    async fn update_task_appends_trackers_and_persists_bt_tracker() {
        let mut task = DownloadTask::new_torrent("bt3".into(), "/dl".into(), None, Map::new());
        task.status = TaskStatus::Paused;
        task.bt_announce_list = vec![vec!["udp://a.example:80/announce".into()]];
        let mgr = make_test_manager(vec![task]);

        let outcome = mgr
            .update_task(
                "bt3",
                TaskPatch {
                    trackers: Some(vec![
                        "udp://a.example:80/announce".into(),
                        "http://b.example/announce".into(),
                    ]),
                    ..Default::default()
                },
            )
            .await
            .expect("trackers");

        assert_eq!(outcome.trackers_added, 1);
        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "bt3").unwrap();
        assert!(t
            .bt_announce_list
            .iter()
            .flatten()
            .any(|u| u == "http://b.example/announce"));
        let raw = t
            .options
            .get("bt-tracker")
            .and_then(|v| v.as_str())
            .unwrap();
        assert!(raw.contains("http://b.example/announce"));
        assert!(raw.contains("udp://a.example:80/announce"));
    }

    #[tokio::test]
    async fn update_task_rejects_out_on_torrent() {
        let mut task = DownloadTask::new_torrent("bt4".into(), "/dl".into(), None, Map::new());
        task.status = TaskStatus::Paused;
        task.out = "movie.mkv".into();
        let mgr = make_test_manager(vec![task]);

        let err = mgr
            .update_task(
                "bt4",
                TaskPatch {
                    out: Some("renamed.mkv".into()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("out on torrent");
        assert!(err.contains("file name") || err.contains("torrent"));

        let mut opts = Map::new();
        opts.insert("split".into(), Value::from(8));
        opts.insert("out".into(), Value::String("renamed.mkv".into()));
        let err = mgr
            .update_task(
                "bt4",
                TaskPatch {
                    options: Some(opts),
                    ..Default::default()
                },
            )
            .await
            .expect_err("out option on torrent");
        assert!(err.contains("file name") || err.contains("torrent"));

        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "bt4").unwrap();
        assert_eq!(t.out, "movie.mkv");
        assert!(t.options.get("split").is_none());
    }

    async fn spawn_mock_worker(
        mgr: &TaskManager,
        gid: &str,
        on_cancel: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> CancellationToken {
        let counters = Counters::new(0, 0);
        let cancel_token = counters.cancel_token.clone();
        mgr.active_downloads.write().await.insert(
            gid.to_string(),
            counters.to_active(
                next_worker_epoch(),
                Vec::new(),
                Arc::new(parking_lot::Mutex::new(None)),
            ),
        );
        let active = mgr.active_downloads.clone();
        let gid = gid.to_string();
        let token = cancel_token.clone();
        tokio::spawn(async move {
            token.cancelled().await;
            on_cancel.await;
            active.write().await.remove(&gid);
        });
        cancel_token
    }

    #[tokio::test]
    async fn update_task_moves_partial_after_the_worker_stops_writing() {
        let root = tempfile::TempDir::new().unwrap();
        let old_dir = root.path().join("old");
        let new_dir = root.path().join("new");
        std::fs::create_dir_all(&old_dir).unwrap();
        std::fs::create_dir_all(&new_dir).unwrap();
        let old_part = old_dir.join("file.bin.part");
        std::fs::write(&old_part, b"partial").unwrap();

        let mut task = DownloadTask::new_http(
            "move1".into(),
            vec!["https://a.example/file.bin".into()],
            old_dir.to_string_lossy().into_owned(),
            None,
            Map::new(),
        );
        task.out = "file.bin".into();
        task.status = TaskStatus::Active;
        let mgr = make_test_manager(vec![task]);
        let flushed = old_part.clone();
        spawn_mock_worker(&mgr, "move1", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            std::fs::write(&flushed, b"partial+flushed").unwrap();
        })
        .await;

        let outcome = mgr
            .update_task(
                "move1",
                TaskPatch {
                    dir: Some(new_dir.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )
            .await
            .expect("relocate");

        assert!(outcome.restarted);
        assert!(outcome.progress_preserved);
        assert!(!old_part.exists());
        assert_eq!(
            std::fs::read(new_dir.join("file.bin.part")).unwrap(),
            b"partial+flushed"
        );
    }

    #[tokio::test]
    async fn update_task_restarts_active_http_worker() {
        let dl_dir = tempfile::TempDir::new().unwrap();
        let mut task = DownloadTask::new_http(
            "act1".into(),
            vec!["https://a.example/file.bin".into()],
            dl_dir.path().to_string_lossy().into_owned(),
            None,
            Map::new(),
        );
        task.out = "file.bin".into();
        task.status = TaskStatus::Active;
        let mgr = make_test_manager(vec![task]);
        let cancel_token = spawn_mock_worker(&mgr, "act1", async {}).await;

        let mut opts = Map::new();
        opts.insert("split".into(), Value::from(4));
        let outcome = mgr
            .update_task(
                "act1",
                TaskPatch {
                    options: Some(opts),
                    ..Default::default()
                },
            )
            .await
            .expect("restart");

        assert!(outcome.restarted);
        assert!(cancel_token.is_cancelled());
        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "act1").unwrap();
        assert!(
            t.status == TaskStatus::Waiting || t.status == TaskStatus::Active,
            "status={:?}",
            t.status
        );
        assert_eq!(t.options.get("split").and_then(|v| v.as_u64()), Some(4));
    }

    #[tokio::test]
    async fn update_task_rejects_cleared_out_before_sanitize() {
        let mut task = DownloadTask::new_http(
            "empty-out".into(),
            vec!["https://a.example/file.bin".into()],
            "/dl".into(),
            None,
            Map::new(),
        );
        task.out = "file.bin".into();
        task.status = TaskStatus::Paused;
        let mgr = make_test_manager(vec![task]);

        let err = mgr
            .update_task(
                "empty-out",
                TaskPatch {
                    out: Some("   ".into()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("blank out");
        assert!(err.contains("out must not be empty"));
        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "empty-out").unwrap();
        assert_eq!(t.out, "file.bin");
    }

    #[tokio::test]
    async fn update_task_unsetting_restart_option_restarts_active_worker() {
        let mut opts = Map::new();
        opts.insert(
            "all-proxy".into(),
            Value::String("http://proxy:8080".into()),
        );
        let mut task = DownloadTask::new_http(
            "unset1".into(),
            vec!["https://a.example/file.bin".into()],
            "/dl".into(),
            None,
            opts,
        );
        task.out = "file.bin".into();
        task.status = TaskStatus::Active;
        let mgr = make_test_manager(vec![task]);
        let cancel_token = spawn_mock_worker(&mgr, "unset1", async {}).await;

        let mut patch_opts = Map::new();
        patch_opts.insert("all-proxy".into(), Value::Null);
        let outcome = mgr
            .update_task(
                "unset1",
                TaskPatch {
                    options: Some(patch_opts),
                    ..Default::default()
                },
            )
            .await
            .expect("unset");

        assert!(outcome.restarted);
        assert!(cancel_token.is_cancelled());
        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "unset1").unwrap();
        assert!(t.options.get("all-proxy").is_none());
    }

    #[tokio::test]
    async fn nzb_total_leaves_out_unrequested_recovery_volumes() {
        let file = |name: &str, id: &str, bytes: u64| {
            format!(
                r#"<file poster="p" date="1" subject="&quot;{name}&quot; yEnc (1/1)"><groups><group>a.b</group></groups><segments><segment bytes="{bytes}" number="1">{id}</segment></segments></file>"#
            )
        };
        let nzb = format!(
            r#"<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">{}{}{}</nzb>"#,
            file("data.bin", "d@t", 1000),
            file("data.par2", "i@t", 50),
            file("data.vol00+02.par2", "v@t", 400),
        );
        let mgr = make_test_manager(Vec::new());
        let mut options = Map::new();
        options.insert("pause".into(), Value::String("true".into()));

        let gid = mgr.add_nzb_task(nzb.into_bytes(), options).await.unwrap();

        let tasks = mgr.tasks.read().await;
        let task = tasks.iter().find(|t| t.gid == gid).unwrap();
        assert_eq!(task.total_length, 1050);
        assert_eq!(task.files.len(), 3);
    }

    #[tokio::test]
    async fn removing_a_p2p_task_deletes_its_part_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let g2 = dir.path().join("foo.bin.part");
        let gnutella = dir.path().join("bar.bin.part");
        let keep = dir.path().join("other.bin.part");
        for file in [&g2, &gnutella, &keep] {
            std::fs::write(file, b"partial").unwrap();
        }
        let task = |gid: &str, kind: TaskKind, uri: &str| {
            let mut t = DownloadTask::new_simple_protocol(
                gid.into(),
                kind,
                uri.into(),
                String::new(),
                10,
                dir.path().to_string_lossy().into_owned(),
                None,
                Map::new(),
            );
            t.status = TaskStatus::Paused;
            t
        };
        let mgr = make_test_manager(vec![
            task(
                "g2",
                TaskKind::G2,
                "g2://h:6346/sha1/ABCDEF?xl=10&dn=foo.bin",
            ),
            task(
                "gn",
                TaskKind::Gnutella,
                "gnutella://h:6346/?urn=urn:sha1:ABCDEF&dn=bar.bin&xl=10",
            ),
        ]);

        mgr.remove("g2").await.unwrap();
        mgr.remove("gn").await.unwrap();

        assert!(!g2.exists());
        assert!(!gnutella.exists());
        assert!(keep.exists());
    }

    #[tokio::test]
    async fn update_task_relocates_inferred_filename_when_out_is_empty() {
        let root = tempfile::TempDir::new().unwrap();
        let old_dir = root.path().join("old");
        let new_dir = root.path().join("new");
        std::fs::create_dir_all(&old_dir).unwrap();
        std::fs::create_dir_all(&new_dir).unwrap();
        std::fs::write(old_dir.join("file.bin.part"), b"partial").unwrap();

        let mut task = DownloadTask::new_http(
            "infer1".into(),
            vec!["https://a.example/file.bin".into()],
            old_dir.to_string_lossy().into_owned(),
            None,
            Map::new(),
        );
        task.out = String::new();
        task.status = TaskStatus::Paused;
        let mgr = make_test_manager(vec![task]);

        let outcome = mgr
            .update_task(
                "infer1",
                TaskPatch {
                    dir: Some(new_dir.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )
            .await
            .expect("relocate inferred");

        assert!(outcome.progress_preserved);
        assert!(!old_dir.join("file.bin.part").exists());
        assert_eq!(
            std::fs::read(new_dir.join("file.bin.part")).unwrap(),
            b"partial"
        );
    }

    #[tokio::test]
    async fn update_task_relocates_inferred_name_using_old_uri_when_primary_changes() {
        let root = tempfile::TempDir::new().unwrap();
        let old_dir = root.path().join("old");
        let new_dir = root.path().join("new");
        std::fs::create_dir_all(&old_dir).unwrap();
        std::fs::create_dir_all(&new_dir).unwrap();
        std::fs::write(old_dir.join("file.bin.part"), b"partial").unwrap();

        let mut task = DownloadTask::new_http(
            "infer-uri".into(),
            vec!["https://a.example/file.bin".into()],
            old_dir.to_string_lossy().into_owned(),
            None,
            Map::new(),
        );
        task.out = String::new();
        task.status = TaskStatus::Paused;
        let mgr = make_test_manager(vec![task]);

        let outcome = mgr
            .update_task(
                "infer-uri",
                TaskPatch {
                    dir: Some(new_dir.to_string_lossy().into_owned()),
                    uris: Some(vec!["https://b.example/other.bin".into()]),
                    ..Default::default()
                },
            )
            .await
            .expect("relocate inferred with new uri");

        assert!(!old_dir.join("file.bin.part").exists());
        assert!(!new_dir.join("file.bin.part").exists());
        assert_eq!(
            std::fs::read(new_dir.join("other.bin.part")).unwrap(),
            b"partial"
        );
        assert!(!outcome.progress_preserved);
    }

    #[tokio::test(start_paused = true)]
    async fn update_task_errors_when_cancelled_worker_does_not_exit() {
        let mut task = DownloadTask::new_http(
            "stuck1".into(),
            vec!["https://a.example/file.bin".into()],
            "/old".into(),
            None,
            Map::new(),
        );
        task.out = "file.bin".into();
        task.status = TaskStatus::Active;
        let mgr = make_test_manager(vec![task]);
        let tasks = mgr.tasks.clone();
        let counters = Counters::new(0, 0);
        mgr.active_downloads.write().await.insert(
            "stuck1".into(),
            counters.to_active(
                next_worker_epoch(),
                Vec::new(),
                Arc::new(parking_lot::Mutex::new(None)),
            ),
        );

        let update = tokio::spawn(async move {
            mgr.update_task(
                "stuck1",
                TaskPatch {
                    dir: Some("/new".into()),
                    ..Default::default()
                },
            )
            .await
        });
        let mut elapsed = Duration::ZERO;
        while elapsed < WORKER_EXIT_TIMEOUT + Duration::from_secs(1) {
            tokio::task::yield_now().await;
            tokio::time::advance(Duration::from_millis(20)).await;
            elapsed += Duration::from_millis(20);
            if update.is_finished() {
                break;
            }
        }

        let err = update
            .await
            .expect("join")
            .expect_err("stale worker must fail the restart");
        assert!(
            err.contains("Timed out waiting for worker to stop"),
            "err={err}"
        );
        let tasks = tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "stuck1").unwrap();
        assert_eq!(t.dir, "/old");
        assert_eq!(t.status, TaskStatus::Active);
    }

    #[tokio::test]
    async fn update_task_reports_lost_progress_when_partial_cannot_be_moved() {
        let mut task = DownloadTask::new_http(
            "nopath".into(),
            vec!["https://a.example/file.bin".into()],
            "/old".into(),
            None,
            Map::new(),
        );
        task.out = "file.bin".into();
        task.status = TaskStatus::Paused;
        let mgr = make_test_manager(vec![task]);

        let outcome = mgr
            .update_task(
                "nopath",
                TaskPatch {
                    dir: Some("/new".into()),
                    ..Default::default()
                },
            )
            .await
            .expect("dir change");

        assert!(!outcome.progress_preserved);
    }

    #[tokio::test]
    async fn remember_torrent_id_keeps_other_mappings_without_engine() {
        let mut t1 = DownloadTask::new_torrent("g1".into(), "/dl".into(), None, Map::new());
        t1.status = TaskStatus::Active;
        let mut t2 = DownloadTask::new_torrent("g2".into(), "/dl".into(), None, Map::new());
        t2.status = TaskStatus::Active;
        let mgr = make_test_manager(vec![t1, t2]);

        mgr.remember_torrent_id("g1", 1).await;
        mgr.remember_torrent_id("g2", 2).await;

        let ids = mgr.torrent_ids.read().await;
        assert_eq!(ids.get("g1"), Some(&1));
        assert_eq!(ids.get("g2"), Some(&2));
        drop(ids);
        let tasks = mgr.tasks.read().await;
        assert!(tasks.iter().all(|t| t.status == TaskStatus::Active));
    }

    #[test]
    fn saved_torrent_path_rejects_non_hex_hashes() {
        let mgr = make_test_manager(vec![]);
        assert!(mgr.saved_torrent_path("").is_none());
        assert!(mgr.saved_torrent_path("../evil").is_none());
        let p = mgr.saved_torrent_path("ABCDEF0123").unwrap();
        assert!(p.ends_with("torrents/abcdef0123.torrent"));
    }
}
