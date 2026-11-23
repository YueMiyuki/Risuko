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
use super::ed2k::share::{
    load_or_create_user_hash, remember_complete_file, verify_complete_file, ShareContext,
    ShareRegistry, SharedFile, UploadStats, VerifiedFile,
};
use super::ed2k::upload::{Ed2kUploader, UploadConfig};
use super::error_code::classify_error;
use super::events::{EngineEvent, EventBroadcaster};
use super::http;
use super::media;
use super::options::EngineOptions;
use super::part_claim::RESERVED_PARTS_OPTION;
use super::routing::{resolve_routing, strip_part_suffix, TaskRoutingRule};
use super::session::SessionManager;
use super::speed_limiter::{parse_speed_limit, SpeedLimiter};
use super::task::{
    generate_gid, ChunkProgress, DownloadFile, DownloadTask, Ed2kKadTaskStatus, FileUri,
    MirrorStat, PeerInfo, TaskFileStamp, TaskKind, TaskPatch, TaskStatus, UpdateTaskOutcome,
    UsenetRepairFailure, UsenetTaskData, UsenetTaskFile, UsenetTaskOptions, UsenetTaskSegment,
};
use super::torrent::{self, TorrentEngine};
use super::upload::UploadFileSnapshot;
use super::usenet::UsenetProviderProfile;
use super::usenet_transport::{ProviderConnectionCapacityRegistry, ProviderConnectionLease};
use std::collections::HashSet;

const MAGNET_METADATA_ATTEMPT_TIMEOUT_SECS: u64 = 60;
const MAGNET_METADATA_RETRY_DELAY_SECS: u64 = 15;
const MAGNET_METADATA_RETRY_MAX_DELAY_SECS: u64 = 600;
const P2P_RELOAD_CANCEL_TIMEOUT: Duration = Duration::from_secs(10);
const SEED_IN_PLACE_KEY: &str = "risuko-seed-in-place";
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

type StartingWorkers = parking_lot::Mutex<HashMap<String, (u64, CancellationToken)>>;

fn next_worker_epoch() -> u64 {
    WORKER_EPOCH.fetch_add(1, Ordering::Relaxed)
}

fn clear_starting_worker(starting: &StartingWorkers, gid: &str, epoch: u64) {
    let mut guard = starting.lock();
    if guard.get(gid).map(|(e, _)| *e) == Some(epoch) {
        guard.remove(gid);
    }
}

async fn register_active_download(
    active: &Arc<RwLock<HashMap<String, ActiveDownload>>>,
    starting: &StartingWorkers,
    gid: String,
    ad: ActiveDownload,
) {
    let epoch = ad.epoch;
    active.write().await.insert(gid.clone(), ad);
    clear_starting_worker(starting, &gid, epoch);
}

fn cancel_starting_worker(starting: &StartingWorkers, gid: &str) {
    if let Some((_, token)) = starting.lock().get(gid).cloned() {
        token.cancel();
    }
}

fn cancel_all_starting_workers(starting: &StartingWorkers) {
    let tokens: Vec<_> = starting
        .lock()
        .values()
        .map(|(_, token)| token.clone())
        .collect();
    for token in tokens {
        token.cancel();
    }
}

fn resource_key(uri: &str) -> &str {
    uri.split('#').next().unwrap_or(uri)
}

async fn reserved_part_paths(tasks: &Arc<RevLock>, gid: &str, uri: Option<&str>) -> Vec<Value> {
    let own = uri.map(resource_key);
    tasks
        .read()
        .await
        .iter()
        .filter(|task| {
            task.gid != gid
                && task.uris.first().map(|other| resource_key(other)) != own
                && !matches!(task.status, TaskStatus::Complete | TaskStatus::Removed)
        })
        .filter_map(|task| {
            let uri = task.uris.first()?;
            let part = match task.kind {
                TaskKind::Http | TaskKind::Metalink if task.completed_length > 0 => {
                    http::initial_part_path(&task.dir, &task.out, uri)
                }
                TaskKind::Ftp if task.completed_length > 0 => {
                    super::ftp::initial_part_path(&task.dir, &task.out, uri)?
                }
                TaskKind::Ed2k => super::ed2k::partial::part_path(&ed2k_target(task)?.0),
                TaskKind::Gnutella | TaskKind::G2 => {
                    super::gnutella::download::partial_path_for_uri(uri, &task.dir)?
                }
                _ => return None,
            };
            Some(Value::String(part.to_string_lossy().into_owned()))
        })
        .collect()
}

fn is_reserved(reserved: &[Value], path: &Path) -> bool {
    reserved
        .iter()
        .filter_map(Value::as_str)
        .any(|reserved| Path::new(reserved) == path)
}

async fn foreign_partial(tasks: &Arc<RevLock>, gid: &str, uri: &str, dir: &str) -> Option<PathBuf> {
    let path = super::gnutella::download::partial_path_for_uri(uri, dir)?;
    let reserved = reserved_part_paths(tasks, gid, None).await;
    (is_reserved(&reserved, &path) && path.exists()).then_some(path)
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

async fn finish_if_startup_aborted(
    tasks: &Arc<RevLock>,
    active: &Arc<RwLock<HashMap<String, ActiveDownload>>>,
    events: &EventBroadcaster,
    gid: &str,
    worker_epoch: u64,
    proto_label: &'static str,
    counters: &Counters,
) -> bool {
    if !worker_startup_aborted(tasks, gid, &counters.cancel_token).await {
        return false;
    }
    finish_task(
        tasks,
        active,
        events,
        gid,
        worker_epoch,
        proto_label,
        counters,
        Err("cancelled".to_string()),
        |_| {},
        |task, _| task.total_length,
        |_, e| classify_error(e, proto_label),
    )
    .await;
    true
}

async fn replay_magnet_trackers(
    engine: &TorrentEngine,
    tasks: &Arc<RevLock>,
    gid: &str,
    torrent_id: usize,
) {
    let tracker_urls = {
        let guard = tasks.read().await;
        guard
            .iter()
            .find(|task| task.gid == gid)
            .map(|task| unique_tracker_urls(&task.bt_announce_list))
            .unwrap_or_default()
    };
    if let Some(announced) = engine.private_tracker_tiers(torrent_id) {
        if !tracker_urls.is_empty() {
            tracing::warn!(
                "[task:{gid}] Private torrent: not replaying {} added trackers",
                tracker_urls.len()
            );
        }
        let mut guard = tasks.write().await;
        if let Some(task) = guard.iter_mut().find(|task| task.gid == gid) {
            task.options.insert(
                "bt-tracker".into(),
                Value::String(unique_tracker_urls(&announced).join("\n")),
            );
            task.bt_announce_list = announced;
        }
    } else if !tracker_urls.is_empty() {
        if let Err(e) = engine.add_trackers(torrent_id, tracker_urls).await {
            tracing::warn!("[task:{gid}] Failed to replay persisted magnet trackers: {e}");
        }
    }
}

fn unique_tracker_urls(tiers: &[Vec<String>]) -> Vec<String> {
    let mut seen = HashSet::new();
    tiers
        .iter()
        .flatten()
        .filter(|url| seen.insert(url.as_str()))
        .cloned()
        .collect()
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UriKind {
    Magnet,
    M3u8,
    Ed2k,
    Ftp,
    Media,
    LegacyP2p(TaskKind),
    Http,
}

pub fn media_downloads_supported() -> bool {
    !cfg!(target_os = "android")
}

pub fn uri_kind(uri: &str, options: &Map<String, Value>) -> UriKind {
    let uri = uri.trim();
    if torrent::is_magnet_uri(uri) {
        UriKind::Magnet
    } else if super::m3u8::is_m3u8_uri(uri) {
        UriKind::M3u8
    } else if super::ed2k::is_ed2k_uri(uri) {
        UriKind::Ed2k
    } else if super::ftp::is_ftp_uri(uri) {
        UriKind::Ftp
    } else if media::is_media_uri(uri) || media::is_force_ytdlp(options) {
        UriKind::Media
    } else if super::adc::is_adc_uri(uri) {
        UriKind::LegacyP2p(TaskKind::Adc)
    } else if super::gnutella::is_gnutella_uri(uri) {
        UriKind::LegacyP2p(TaskKind::Gnutella)
    } else if super::g2::is_g2_uri(uri) {
        UriKind::LegacyP2p(TaskKind::G2)
    } else if super::gift::is_gift_uri(uri) {
        UriKind::LegacyP2p(TaskKind::Gift)
    } else {
        UriKind::Http
    }
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

enum RetryCookies {
    Given(Option<String>),
    Stored,
}

struct AppliedTaskPatch {
    path_changed: bool,
    uris_changed: bool,
    primary_uri_changed: bool,
    options_need_restart: bool,
    tracker_urls_to_add: Vec<String>,
}

struct TaskPatchInputs {
    uris: Option<Vec<String>>,
    dir: Option<String>,
    out: Option<String>,
    options: Option<Map<String, Value>>,
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
            task.mirror_progress.clear();
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
            task.options.insert(
                "bt-tracker".into(),
                Value::String(unique_tracker_urls(&task.bt_announce_list).join("\n")),
            );
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

fn ed2k_file_name<'a>(out: &'a str, link: &'a super::ed2k::types::Ed2kFileLink) -> &'a str {
    if out.is_empty() {
        &link.file_name
    } else {
        out
    }
}

fn ed2k_target(task: &DownloadTask) -> Option<(PathBuf, super::ed2k::types::Ed2kFileLink)> {
    let link = super::ed2k::parse_ed2k_link(task.uris.first()?).ok()?;
    let path = super::ed2k::partial::final_path(&task.dir, ed2k_file_name(&task.out, &link));
    Some((path, link))
}

async fn record_ed2k_target(tasks: &Arc<RevLock>, gid: &str, path: &Path) {
    let Some(name) = path.file_name().map(|n| n.to_string_lossy().into_owned()) else {
        return;
    };
    let mut tasks = tasks.write().await;
    if let Some(task) = tasks.iter_mut().find(|t| t.gid == gid && t.out != name) {
        task.out = name;
        refresh_persisted_file_paths(task);
    }
}

fn ed2k_partial_users(
    tasks: &[DownloadTask],
    except: &[String],
    live: &HashSet<String>,
) -> HashMap<PathBuf, HashSet<[u8; 16]>> {
    let mut users: HashMap<PathBuf, HashSet<[u8; 16]>> = HashMap::new();
    for (path, link) in tasks
        .iter()
        .filter(|t| {
            t.kind == TaskKind::Ed2k
                && !t.seeder
                && (live.contains(&t.gid)
                    || (!matches!(t.status, TaskStatus::Complete | TaskStatus::Removed)
                        && !except.contains(&t.gid)))
        })
        .filter_map(ed2k_target)
    {
        users.entry(path).or_default().insert(link.file_hash_bytes);
    }
    users
}

fn ed2k_partial_is_ours(
    path: &Path,
    link: &super::ed2k::types::Ed2kFileLink,
    users: Option<&HashSet<[u8; 16]>>,
) -> bool {
    let met = super::ed2k::partial::met_path(path);
    if super::ed2k::partial::load_met(&met, link).is_some() {
        users.is_none_or(|hashes| !hashes.contains(&link.file_hash_bytes))
    } else {
        !met.exists() && users.is_none()
    }
}

fn remove_ed2k_leftovers(
    config_dir: &Path,
    path: &Path,
    link: &super::ed2k::types::Ed2kFileLink,
    users: Option<&HashSet<[u8; 16]>>,
) {
    super::ed2k::share::remove_known(config_dir, &link.file_hash_bytes);
    if ed2k_partial_is_ours(path, link, users) {
        super::ed2k::partial::remove_partial_files(path);
    }
}

fn relocate_ed2k_partial(
    from: &Path,
    to: &Path,
    link: &super::ed2k::types::Ed2kFileLink,
    users: Option<&HashSet<[u8; 16]>>,
) -> Result<bool, String> {
    use super::ed2k::partial::{met_path, part_path, PathClaim};
    let (old_part, new_part) = (part_path(from), part_path(to));
    let (old_met, new_met) = (met_path(from), met_path(to));
    if !old_part.exists() {
        return Ok(false);
    }
    if from == to {
        return Ok(ed2k_partial_is_ours(from, link, users));
    }
    let Ok(_from_claim) = PathClaim::acquire(from) else {
        return Ok(false);
    };
    let _to_claim = PathClaim::acquire(to)?;
    if !old_part.exists() || !ed2k_partial_is_ours(from, link, users) {
        return Ok(false);
    }
    if new_part.exists() || new_met.exists() {
        return Err(format!(
            "Cannot relocate partial: destination already exists ({})",
            new_part.display()
        ));
    }
    if let Some(parent) = new_part.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create destination dir {}: {e}", parent.display()))?;
    }
    http::relocate_file_pair(&old_part, &new_part, &old_met, &new_met)?;
    Ok(true)
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
    ed2k_upload: Option<Arc<UploadStats>>,
    mirrors: Option<Arc<http::MirrorTracker>>,
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

    fn to_active_plain(&self, epoch: u64) -> ActiveDownload {
        self.to_active(epoch, Vec::new(), Arc::new(parking_lot::Mutex::new(None)))
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
            ed2k_upload: None,
            mirrors: None,
        }
    }
}

fn mirror_progress(task: &DownloadTask, tracker: &http::MirrorTracker) -> Vec<MirrorStat> {
    if task.uris.len() < 2 {
        return Vec::new();
    }
    tracker.snapshot()
}

fn settle_mirror_progress(
    task: &mut DownloadTask,
    worker_uris: &[String],
    tracker: &http::MirrorTracker,
) {
    if task.uris == worker_uris {
        task.mirror_progress = mirror_progress(task, tracker);
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
    on_found: impl FnOnce(&mut DownloadTask) + Send,
    on_ok: impl FnOnce(&mut DownloadTask, &Path) -> u64 + Send,
    on_err: impl FnOnce(&mut DownloadTask, &str) -> super::error_code::ErrorCode + Send,
) {
    finish_task_boxed(
        tasks,
        active,
        events,
        gid,
        worker_epoch,
        proto_label,
        counters,
        result,
        Box::new(on_found),
        Box::new(on_ok),
        Box::new(on_err),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn finish_task_boxed(
    tasks: &Arc<RevLock>,
    active: &Arc<RwLock<HashMap<String, ActiveDownload>>>,
    events: &EventBroadcaster,
    gid: &str,
    worker_epoch: u64,
    proto_label: &str,
    counters: &Counters,
    result: Result<std::path::PathBuf, String>,
    on_found: Box<dyn FnOnce(&mut DownloadTask) + Send + '_>,
    on_ok: Box<dyn FnOnce(&mut DownloadTask, &Path) -> u64 + Send + '_>,
    on_err: Box<dyn FnOnce(&mut DownloadTask, &str) -> super::error_code::ErrorCode + Send + '_>,
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
        task.upload_speed = 0;
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
                        task.files = single_file(task, &path, file_completed);
                    }
                    events.send(EngineEvent::DownloadComplete {
                        gid: gid.to_string(),
                        seeded: false,
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
        pause_seed_clock(task, crate::engine::util::now_ms());
    }
    drop(tasks_guard);

    let mut active_guard = active.write().await;
    if active_guard.get(gid).map(|ad| ad.epoch) == Some(worker_epoch) {
        active_guard.remove(gid);
    }
}

fn single_file(task: &DownloadTask, path: &Path, completed: u64) -> Vec<DownloadFile> {
    vec![DownloadFile {
        index: "1".to_string(),
        path: path.to_string_lossy().to_string(),
        length: task.total_length.to_string(),
        completed_length: completed.to_string(),
        selected: "true".to_string(),
        uris: task
            .uris
            .iter()
            .map(|u| FileUri {
                uri: u.clone(),
                status: "used".to_string(),
            })
            .collect(),
    }]
}

async fn seed_ed2k(
    verified: Result<VerifiedFile, String>,
    share: &ShareContext,
    link: &super::ed2k::types::Ed2kFileLink,
    path: &Path,
    cancel: &CancellationToken,
) -> Result<PathBuf, String> {
    let verified = match verified {
        Ok(verified) => verified,
        Err(error) if cancel.is_cancelled() => return Err(error),
        Err(error) => {
            tracing::warn!("[ed2k] Not sharing {}: {error}", path.display());
            return Ok(path.to_path_buf());
        }
    };
    let _handle = share.share(
        SharedFile::complete(
            link.file_hash_bytes,
            link.file_name.clone(),
            link.file_size,
            path.to_path_buf(),
            verified.hashset,
        )
        .with_stamp(verified.stamp),
    );
    cancel.cancelled().await;
    Err("cancelled".to_string())
}

async fn begin_ed2k_seeding(
    tasks: &Arc<RevLock>,
    active: &Arc<RwLock<HashMap<String, ActiveDownload>>>,
    options: &Arc<RwLock<EngineOptions>>,
    events: &EventBroadcaster,
    gid: &str,
    worker_epoch: u64,
    path: &Path,
) -> bool {
    let is_current = active
        .read()
        .await
        .get(gid)
        .is_some_and(|ad| ad.epoch == worker_epoch);
    if !is_current {
        return false;
    }
    let (g_manual, g_seed_time, g_seed_ratio) = {
        let options = options.read().await;
        (
            options.keep_seeding(),
            options.seed_time(),
            options.seed_ratio(),
        )
    };
    let mut tasks = tasks.write().await;
    let Some(task) = tasks
        .iter_mut()
        .find(|t| t.gid == gid && t.status == TaskStatus::Active)
    else {
        return false;
    };
    if !resolve_seed_goal(&task.options, g_manual, g_seed_time, g_seed_ratio).0 {
        return false;
    }
    start_seeding(task);
    task.completed_length = task.total_length;
    task.files = single_file(task, path, task.total_length);
    tracing::info!(
        "[task:{gid}] ed2k download complete, sharing {}",
        path.display()
    );
    events.send(EngineEvent::BtDownloadComplete {
        gid: gid.to_string(),
        seeding: true,
    });
    true
}

fn finish_usenet_success(task: &mut DownloadTask, warning: Option<&'static str>) {
    task.usenet_repair_failure = None;
    task.usenet_warning = warning.map(str::to_string);
}

fn complete_usenet_task(
    task: &mut DownloadTask,
    path: &Path,
    warning: Option<&'static str>,
    output_paths: &HashMap<usize, PathBuf>,
) -> u64 {
    finish_usenet_success(task, warning);
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
    task.drop_usenet_articles();
    if task.files.is_empty() {
        path.metadata().map(|metadata| metadata.len()).unwrap_or(0)
    } else {
        task.files
            .iter()
            .filter_map(|file| file.completed_length.parse::<u64>().ok())
            .sum()
    }
}

fn finish_usenet_failure(
    task: &mut DownloadTask,
    error: &str,
    repair_failure: Option<UsenetRepairFailure>,
) -> super::error_code::ErrorCode {
    task.usenet_stage = Some("error".to_string());
    task.usenet_repair_failure = repair_failure;
    task.usenet_warning = None;
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

type MetalinkFileResult = (
    usize,
    String,
    Result<std::path::PathBuf, String>,
    &'static str,
);

fn metalink_mirror_label(uris: &[String]) -> &'static str {
    if file_transfer_mirror(uris).is_some() {
        "ftp"
    } else {
        "http"
    }
}

fn metalink_all_selected_done(task: &DownloadTask, results: &[MetalinkFileResult]) -> bool {
    let finished: HashSet<usize> = results
        .iter()
        .filter(|(_, _, r, _)| r.is_ok())
        .map(|(idx, _, _, _)| *idx)
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
    results: Vec<MetalinkFileResult>,
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
        for (idx, _, r, _) in &results {
            if let (Ok(path), Some(f)) = (r, task.files.get_mut(*idx)) {
                f.path = path.to_string_lossy().to_string();
            }
        }
        metalink_rollup_totals(task);

        if task.status == TaskStatus::Active {
            let failures: Vec<(&str, &String, &'static str)> = results
                .iter()
                .filter_map(|(_, name, r, label)| match r {
                    Err(e) if !e.contains("cancelled") => Some((name.as_str(), e, *label)),
                    _ => None,
                })
                .collect();
            task.download_speed = 0;
            let all_done = metalink_all_selected_done(task, &results);
            if failures.is_empty() && all_done {
                task.status = TaskStatus::Complete;
                events.send(EngineEvent::DownloadComplete {
                    gid: gid.to_string(),
                    seeded: false,
                });
            } else if failures.is_empty() {
                task.status = TaskStatus::Paused;
            } else {
                let names: Vec<&str> = failures.iter().map(|(n, _, _)| *n).collect();
                let first_err = failures[0].1.clone();
                let first_label = failures[0].2;
                task.status = TaskStatus::Paused;
                task.error_code = Some(classify_error(&first_err, first_label).to_string());
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

fn picks_own_dir(options: &Map<String, Value>, default_dir: &str) -> bool {
    let bare = |dir: &str| dir.trim().trim_end_matches(['/', '\\']).to_string();
    options
        .get("dir")
        .and_then(Value::as_str)
        .map(bare)
        .is_some_and(|dir| !dir.is_empty() && dir != bare(default_dir))
}

fn file_transfer_mirror(uris: &[String]) -> Option<&str> {
    uris.first()
        .map(String::as_str)
        .filter(|uri| super::ftp::is_ftp_uri(uri))
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
    let wanted = torrent::parse_select_file(raw)
        .and_then(|indices| torrent::selection_within(indices, files.len()));
    for (i, f) in files.iter_mut().enumerate() {
        f.selected = wanted
            .as_ref()
            .is_none_or(|set| set.contains(&i))
            .to_string();
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
    starting_workers: Arc<StartingWorkers>,
    updating_tasks: Arc<parking_lot::Mutex<HashSet<String>>>,
    stamp_attempts: Arc<parking_lot::Mutex<HashSet<String>>>,
    torrent_upload_bases: parking_lot::Mutex<HashMap<String, TorrentUploadBase>>,
    torrent_ids: Arc<RwLock<HashMap<String, usize>>>,
    pending_magnets: Arc<RwLock<HashSet<String>>>,
    options: Arc<RwLock<EngineOptions>>,
    events: EventBroadcaster,
    session: Arc<SessionManager>,
    save_gate: tokio::sync::Mutex<()>,
    save_requested: tokio::sync::Notify,
    task_list_changed: tokio::sync::Notify,
    shutting_down: std::sync::atomic::AtomicBool,
    torrent_engine: Arc<RwLock<Option<TorrentEngine>>>,
    global_speed_limiter: Arc<SpeedLimiter>,
    global_upload_limiter: Arc<SpeedLimiter>,
    task_limiters: Arc<parking_lot::Mutex<HashMap<String, Arc<SpeedLimiter>>>>,
    cookie_store: Arc<CookieStore>,
    usenet_connection_capacity: Arc<ProviderConnectionCapacityRegistry>,
    kad_runtime: Arc<parking_lot::RwLock<KadRuntime>>,
    ed2k_shares: Arc<ShareRegistry>,
    ed2k_user_hash: [u8; 16],
    ed2k_upload: parking_lot::RwLock<UploadRuntime>,
    ed2k_port_mapper: parking_lot::Mutex<Option<risuko_bt::session::PortMapper>>,
    task_upload_limiters: Arc<parking_lot::Mutex<HashMap<String, Arc<SpeedLimiter>>>>,
}

#[derive(Clone)]
enum UploadRuntime {
    Disabled,
    Running(Arc<Ed2kUploader>),
    Failed(String),
}

impl UploadRuntime {
    fn uploader(&self) -> Option<Arc<Ed2kUploader>> {
        match self {
            Self::Running(uploader) => Some(uploader.clone()),
            Self::Disabled | Self::Failed(_) => None,
        }
    }

    async fn build(
        user_hash: [u8; 16],
        options: &EngineOptions,
        kad: Option<Arc<KadService>>,
        registry: Arc<ShareRegistry>,
        upload_limit: Arc<SpeedLimiter>,
    ) -> Self {
        if !options.ed2k_enable_upload() {
            return Self::Disabled;
        }
        let proxy = match options.p2p_proxy_connector() {
            Ok(proxy) => proxy,
            Err(error) => {
                tracing::warn!("[ed2k] Uploads disabled, invalid P2P proxy: {error}");
                return Self::Failed(format!("invalid P2P proxy: {error}"));
            }
        };
        let mut config = UploadConfig::new(user_hash, options.ed2k_port());
        config.servers = super::ed2k::server_list::server_list(&options.ed2k_servers());
        config.kad = kad;
        config.proxy = proxy;
        config.upload_limit = upload_limit;
        config.registry = registry;
        match Ed2kUploader::bind(config).await {
            Ok(uploader) => Self::Running(uploader),
            Err(error) => {
                let reason = format!("cannot listen on TCP port {}: {error}", options.ed2k_port());
                tracing::warn!("[ed2k] Uploads disabled, {reason}");
                Self::Failed(reason)
            }
        }
    }

    fn port_mapper(
        &self,
        options: &EngineOptions,
        kad: &KadRuntime,
        previous: Option<risuko_bt::session::PortMapper>,
    ) -> Option<risuko_bt::session::PortMapper> {
        let uploader = self.uploader()?;
        if !options.bt_enable_upnp() {
            return None;
        }
        let mut ports = vec![(uploader.port(), risuko_bt::upnp::MapProto::Tcp)];
        if let Some(port) = kad.udp_port() {
            ports.push((port, risuko_bt::upnp::MapProto::Udp));
        }
        Some(risuko_bt::session::PortMapper::spawn(
            ports,
            options.bt_upnp_lease(),
            previous,
        ))
    }
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

fn seeds_in_place(options: &Map<String, Value>) -> bool {
    options.get(SEED_IN_PLACE_KEY).and_then(Value::as_bool) == Some(true)
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

fn magnet_for_torrent(task: &DownloadTask) -> Option<String> {
    const QUERY_SAFE: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'.')
        .remove(b'_')
        .remove(b'~');
    const MAX_TRACKERS: usize = 20;
    fn is_hex(hash: &str, len: usize) -> bool {
        hash.len() == len && hash.bytes().all(|b| b.is_ascii_hexdigit())
    }
    let topic = if task.meta_version.as_deref() == Some("v2") {
        let v2 = task.info_hash_v2.as_deref().filter(|h| is_hex(h, 64))?;
        format!("urn:btmh:1220{}", v2.to_ascii_lowercase())
    } else {
        let v1 = task.info_hash.as_deref().filter(|h| is_hex(h, 40))?;
        format!("urn:btih:{}", v1.to_ascii_lowercase())
    };
    let mut magnet = format!("magnet:?xt={topic}");
    if let Some(name) = task.bt_name.as_deref().filter(|n| !n.is_empty()) {
        magnet.push_str("&dn=");
        magnet.extend(percent_encoding::utf8_percent_encode(name, QUERY_SAFE));
    }
    let mut seen = HashSet::new();
    for tracker in task.bt_announce_list.iter().flatten().map(|t| t.trim()) {
        if seen.len() == MAX_TRACKERS {
            break;
        }
        if tracker.is_empty() || !seen.insert(tracker) {
            continue;
        }
        magnet.push_str("&tr=");
        magnet.extend(percent_encoding::utf8_percent_encode(tracker, QUERY_SAFE));
    }
    Some(magnet)
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

fn saved_torrent_path_in(config_dir: &Path, info_hash: &str) -> Option<PathBuf> {
    if info_hash.is_empty() || !info_hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    Some(
        config_dir
            .join("torrents")
            .join(format!("{}.torrent", info_hash.to_ascii_lowercase())),
    )
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
            let mut purged_hashes: HashSet<String> = saved_tasks
                .iter()
                .filter(|t| t.status.is_stopped() && t.kind == TaskKind::Torrent)
                .filter_map(|t| t.info_hash.as_deref())
                .map(str::to_ascii_lowercase)
                .collect();
            let purged_m3u8: Vec<_> = saved_tasks
                .iter()
                .filter(|t| t.status.is_stopped() && t.kind == TaskKind::M3u8)
                .map(|t| {
                    (
                        t.gid.clone(),
                        t.dir.clone(),
                        t.uris.first().cloned().unwrap_or_default(),
                        t.out.clone(),
                    )
                })
                .collect();
            let purged_ed2k: Vec<_> = saved_tasks
                .iter()
                .filter(|t| t.status.is_stopped() && t.kind == TaskKind::Ed2k)
                .filter_map(ed2k_target)
                .collect();
            saved_tasks.retain(|t| !t.status.is_stopped());
            for task in saved_tasks.iter().filter(|t| t.kind == TaskKind::Torrent) {
                if let Some(hash) = task.info_hash.as_deref() {
                    purged_hashes.remove(&hash.to_ascii_lowercase());
                }
            }
            for path in purged_hashes
                .iter()
                .filter_map(|hash| saved_torrent_path_in(config_dir, hash))
            {
                if let Err(e) = std::fs::remove_file(&path) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        tracing::warn!("could not delete {}: {e}", path.display());
                    }
                }
            }
            for (gid, dir, uri, out) in &purged_m3u8 {
                super::m3u8::remove_temp_dirs(gid, dir, uri, out);
            }
            let users = ed2k_partial_users(&saved_tasks, &[], &HashSet::new());
            for (path, link) in &purged_ed2k {
                remove_ed2k_leftovers(config_dir, path, link, users.get(path));
            }
        }
        forget_failed_flushes_from_earlier_boots(&mut saved_tasks, super::boot_id::current());
        for task in &mut saved_tasks {
            if task.kind == TaskKind::Usenet && task.status == TaskStatus::Complete {
                task.drop_usenet_articles();
            }
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
        )
        .with_config_dir(config_dir);
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

        let ed2k_shares = ShareRegistry::new();
        let ed2k_user_hash = {
            let dir = config_dir.to_path_buf();
            tokio::task::spawn_blocking(move || load_or_create_user_hash(&dir))
                .await
                .map_err(std::io::Error::other)
                .and_then(|loaded| loaded)
                .unwrap_or_else(|error| {
                    tracing::warn!("[ed2k] Cannot persist the user hash: {error}");
                    super::ed2k::protocol::new_user_hash()
                })
        };
        let ed2k_upload = UploadRuntime::build(
            ed2k_user_hash,
            &options,
            kad_runtime.service(),
            ed2k_shares.clone(),
            global_upload_limiter.clone(),
        )
        .await;
        let ed2k_port_mapper = ed2k_upload.port_mapper(&options, &kad_runtime, None);

        let manager = Self {
            tasks: Arc::new(RevLock::new(saved_tasks)),
            saved_rev: AtomicU64::new(u64::MAX),
            active_downloads: Arc::new(RwLock::new(HashMap::new())),
            starting_workers: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            updating_tasks: Arc::new(parking_lot::Mutex::new(HashSet::new())),
            stamp_attempts: Arc::new(parking_lot::Mutex::new(HashSet::new())),
            torrent_upload_bases: parking_lot::Mutex::new(HashMap::new()),
            torrent_ids: Arc::new(RwLock::new(HashMap::new())),
            pending_magnets: Arc::new(RwLock::new(HashSet::new())),
            options: Arc::new(RwLock::new(options)),
            events,
            session: Arc::new(session),
            save_gate: tokio::sync::Mutex::new(()),
            save_requested: tokio::sync::Notify::new(),
            task_list_changed: tokio::sync::Notify::new(),
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
            ed2k_shares,
            ed2k_user_hash,
            ed2k_upload: parking_lot::RwLock::new(ed2k_upload),
            ed2k_port_mapper: parking_lot::Mutex::new(ed2k_port_mapper),
            task_upload_limiters: Arc::new(parking_lot::Mutex::new(HashMap::new())),
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
        if seeds_in_place(options) {
            return (raw_dir, None);
        }
        let rules = opts_guard.task_routing_rules();
        let file_category_dirs = opts_guard.file_category_dirs();
        let picked = picks_own_dir(options, &opts_guard.dir());
        drop(opts_guard);

        let decision = resolve_routing(&rules, filename_hint, &raw_dir, &file_category_dirs);
        if picked {
            return (raw_dir, decision.tag);
        }
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
        self.request_save();
        self.signal_task_list_change();

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
                    tracing::info!(
                        "[metalink] following metalink URL: {}",
                        super::util::redact_url(&uris[0])
                    );
                    match self.add_metalink_task(bytes, options.clone()).await {
                        Ok(gid) => return Ok(gid),
                        Err(e) => tracing::warn!(
                            "[metalink] {} parsed but task creation failed, falling back to HTTP: {e}",
                            super::util::redact_url(&uris[0])
                        ),
                    }
                }
            }
        }

        if let Some(bad) = uris.iter().find(|u| !is_supported_http_task_uri(u)) {
            return Err(format!(
                "Unsupported URI scheme for an HTTP task: {}",
                super::util::redact_url(bad)
            ));
        }

        let gid = generate_gid();
        tracing::info!(
            "[task:{}] Adding HTTP task, uris={:?}",
            gid,
            super::util::redact_urls(&uris)
        );
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
        if !media_downloads_supported() {
            return Err("task.media-unsupported-android".to_string());
        }
        let gid = generate_gid();
        tracing::info!(
            "[task:{}] Adding media task, uri={}",
            gid,
            super::util::redact_url(uri)
        );
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

    pub fn seed_in_place_options(dir: &str) -> Map<String, Value> {
        let mut options = Map::new();
        options.insert("dir".into(), Value::String(dir.to_string()));
        options.insert("bt-create-subfolder".into(), Value::Bool(true));
        options.insert("keep-seeding".into(), Value::Bool(true));
        options.insert("seed-time".into(), Value::from(0));
        options.insert("seed-ratio".into(), Value::from(0));
        options.insert(
            "risuko-completion-script-enabled".into(),
            Value::Bool(false),
        );
        options.insert(SEED_IN_PLACE_KEY.into(), Value::Bool(true));
        options
    }

    pub async fn add_torrent_task(
        &self,
        torrent_data: Vec<u8>,
        options: Map<String, Value>,
    ) -> Result<String, String> {
        self.add_torrent_task_with(torrent_data, options, Vec::new())
            .await
    }

    pub async fn seed_created_torrent(
        &self,
        torrent_data: Vec<u8>,
        dir: &str,
        verified_files: Vec<risuko_bt::create::FileStamp>,
    ) -> Result<String, String> {
        self.add_torrent_task_with(
            torrent_data,
            Self::seed_in_place_options(dir),
            verified_files,
        )
        .await
    }

    async fn add_torrent_task_with(
        &self,
        torrent_data: Vec<u8>,
        mut options: Map<String, Value>,
        verified_files: Vec<risuko_bt::create::FileStamp>,
    ) -> Result<String, String> {
        let _p2p_reload_guard = self.p2p_reload_lock.lock().await;
        let gid = generate_gid();
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
        options.insert("dir".into(), Value::String(dir.clone()));
        let merged = self.options.read().await.merge_task_options(&options);

        let mut task = DownloadTask::new_torrent(gid.clone(), dir.clone(), tag, options.clone());

        // Drop the engine lock before remember_torrent_id, which also reads torrent_engine
        let add_result = {
            let te_guard = self.torrent_engine.read().await;
            if let Some(ref te) = *te_guard {
                Some(
                    te.add_verified_torrent_bytes(&torrent_data, &merged, verified_files)
                        .await,
                )
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
        self.request_save();
        self.signal_task_list_change();
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
        mut options: Map<String, Value>,
    ) -> Result<String, String> {
        let _p2p_reload_guard = self.p2p_reload_lock.lock().await;
        let gid = generate_gid();
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
        options.insert("dir".into(), Value::String(dir.clone()));
        let merged = self.options.read().await.merge_task_options(&options);

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
        self.request_save();
        self.signal_task_list_change();
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
        let stamp_attempts = self.stamp_attempts.clone();
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
                let mut verified_files = Vec::new();
                {
                    let guard = tasks.read().await;
                    if let Some(task) = guard
                        .iter()
                        .find(|task| is_live_magnet(task, &gid, &magnet_uri))
                    {
                        verified_files = task.verified_files();
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
                                verified_files,
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
                                task.bt_file_stamps.clear();
                                attached = true;
                            }
                        }
                        if attached {
                            stamp_attempts.lock().remove(&gid);
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
                                replay_magnet_trackers(&engine, &tasks, &gid, handle.id).await;
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
        let (worker_epoch, starting) =
            self.publish_starting_worker(&gid, counters.cancel_token.clone());
        tokio::spawn(async move {
            register_active_download(
                &active,
                &starting,
                gid.clone(),
                counters.to_active_plain(worker_epoch),
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

            let foreign = match kind {
                TaskKind::Gnutella | TaskKind::G2 => {
                    foreign_partial(&tasks, &gid, &uri, &dir).await
                }
                _ => None,
            };
            let c = counters.clone();
            let download_result = if let Some(path) = foreign {
                Err(format!(
                    "{} belongs to another unfinished download",
                    path.display()
                ))
            } else {
                match kind {
                    TaskKind::Adc => {
                        super::adc::run_adc_download(&uri, c.total, c.cancel_token).await
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
                }
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
        let usenet_warning = Arc::new(parking_lot::Mutex::new(None::<&'static str>));
        let usenet_warning_for_worker = usenet_warning.clone();
        let counters = Counters::new(task.total_length, 0);
        let (worker_epoch, starting) =
            self.publish_starting_worker(&gid, counters.cancel_token.clone());
        tokio::spawn(async move {
            register_active_download(
                &active,
                &starting,
                gid.clone(),
                counters.to_active_plain(worker_epoch),
            )
            .await;
            if finish_if_startup_aborted(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                "usenet",
                &counters,
            )
            .await
            {
                return;
            }
            let received = Arc::new(AtomicU64::new(0));
            let speed_task = http::spawn_speed_tracker(
                received.clone(),
                counters.speed.clone(),
                counters.cancel_token.clone(),
            );
            let result = super::usenet_worker::run_usenet_download_with_resolver_and_capacity(
                &task_snapshot,
                &merged_options,
                counters.completed.clone(),
                counters.total.clone(),
                received,
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
            speed_task.abort();
            counters.speed.store(0, Ordering::Relaxed);
            let repair_failure = result
                .as_ref()
                .err()
                .and_then(|error| error.repair_failure().cloned());
            let result = result
                .map(|output| {
                    *output_paths_for_worker.lock() = output.outputs.into_iter().collect();
                    *usenet_warning_for_worker.lock() = output.warning;
                    output.path
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
                    let warning = *usenet_warning.lock();
                    let output_paths = output_paths.lock().clone();
                    complete_usenet_task(task, path, warning, &output_paths)
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
        let busy_gids = {
            let active = self.active_downloads.read().await;
            let mut busy_gids = active.keys().cloned().collect::<HashSet<_>>();
            busy_gids.extend(self.starting_workers.lock().keys().cloned());
            busy_gids
        };

        let (active_count, has_startable) = {
            let tasks = self.tasks.read().await;
            let updating = self.updating_tasks.lock();
            let busy_seeders = tasks
                .iter()
                .filter(|t| busy_gids.contains(&t.gid) && !t.holds_download_slot())
                .count();
            let active_count = busy_gids.len() - busy_seeders;
            let has_startable = tasks.iter().any(|t| {
                t.status == TaskStatus::Waiting
                    && !busy_gids.contains(&t.gid)
                    && !updating.contains(&t.gid)
                    && (active_count < max_concurrent || !t.holds_download_slot())
            });
            (active_count, has_startable)
        };
        if !has_startable {
            return;
        }
        let options_snapshot = self.options.read().await.clone();

        let slots = max_concurrent.saturating_sub(active_count);
        let mut tasks = self.tasks.write().await;
        let mut started = 0;

        for task in tasks.iter_mut() {
            let holds_slot = task.holds_download_slot();
            if started >= slots && holds_slot {
                continue;
            }
            if task.status != TaskStatus::Waiting {
                continue;
            }
            if busy_gids.contains(&task.gid) || self.updating_tasks.lock().contains(&task.gid) {
                continue;
            }
            if !task.can_be_started() {
                continue;
            }
            if task.kind == TaskKind::Http {
                task.status = TaskStatus::Active;
                let mut merged = options_snapshot.merge_task_options(&task.options);
                self.apply_stored_cookies(&task.uris, &mut merged);
                self.spawn_http_download(task, merged);
                self.send_download_start(&task.gid);
            } else if task.kind == TaskKind::Media {
                task.status = TaskStatus::Active;
                let mut merged = options_snapshot.merge_task_options(&task.options);
                self.apply_stored_cookies(&task.uris, &mut merged);
                self.spawn_media_download(task, merged);
                self.send_download_start(&task.gid);
            } else if task.kind == TaskKind::M3u8 {
                task.status = TaskStatus::Active;
                let mut merged = options_snapshot.merge_task_options(&task.options);
                self.apply_stored_cookies(&task.uris, &mut merged);
                self.spawn_m3u8_download(task, merged);
                self.send_download_start(&task.gid);
            } else if task.kind == TaskKind::Ed2k {
                task.status = TaskStatus::Active;
                resume_seed_clock(task, crate::engine::util::now_ms());
                self.spawn_ed2k_download(task);
                self.send_download_start(&task.gid);
            } else if task.kind == TaskKind::Ftp {
                task.status = TaskStatus::Active;
                let merged = options_snapshot.merge_task_options(&task.options);
                self.spawn_ftp_download(task, merged);
                self.send_download_start(&task.gid);
            } else if task.kind == TaskKind::Metalink {
                task.status = TaskStatus::Active;
                let merged = options_snapshot.merge_task_options(&task.options);
                apply_select_file(&mut task.files, &task.options);
                self.spawn_metalink_download(task, merged);
                self.send_download_start(&task.gid);
            } else if task.kind == TaskKind::Usenet {
                task.status = TaskStatus::Active;
                task.usenet_stage = Some("connecting".to_string());
                let merged = options_snapshot.merge_task_options(&task.options);
                self.spawn_usenet_download(task, merged);
                self.send_download_start(&task.gid);
            } else if matches!(
                task.kind,
                TaskKind::Adc | TaskKind::Gnutella | TaskKind::G2 | TaskKind::Gift
            ) {
                task.status = TaskStatus::Active;
                self.spawn_legacy_p2p_download(task);
                self.send_download_start(&task.gid);
            }
            if holds_slot && task.status == TaskStatus::Active {
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
                if !task.holds_download_slot() {
                    continue;
                }
                match task.status {
                    TaskStatus::Waiting if !task.can_be_started() => {}
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
            let is_https = uri
                .split_once("://")
                .is_some_and(|(scheme, _)| scheme.eq_ignore_ascii_case("https"));
            let cookie_header =
                super::cookie_store::cookies_to_header_for(&entry.cookies, is_https);
            if !cookie_header.is_empty() {
                tracing::debug!(
                    "apply_stored_cookies: injecting cookie header ({} bytes)",
                    cookie_header.len(),
                );
                if is_https {
                    let plain = super::cookie_store::cookies_to_header_for(&entry.cookies, false);
                    if plain != cookie_header {
                        merged.insert(
                            super::http::COOKIE_HTTP_OPTION.to_string(),
                            Value::String(plain),
                        );
                    }
                }
                merged.insert("cookie".to_string(), Value::String(cookie_header));
                merged.insert(
                    super::http::COOKIE_HOST_OPTION.to_string(),
                    Value::String(entry.host.clone()),
                );
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

    fn publish_starting_worker(
        &self,
        gid: &str,
        token: CancellationToken,
    ) -> (u64, Arc<StartingWorkers>) {
        let epoch = next_worker_epoch();
        self.starting_workers
            .lock()
            .insert(gid.to_string(), (epoch, token));
        (epoch, self.starting_workers.clone())
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
        let mirror_tracker = Arc::new(http::MirrorTracker::default());

        let (worker_epoch, starting) =
            self.publish_starting_worker(&gid, counters.cancel_token.clone());
        tokio::spawn(async move {
            let mut active_download = counters.to_active(
                worker_epoch,
                chunk_completed.clone(),
                adopted_filename.clone(),
            );
            active_download.mirrors = Some(Arc::clone(&mirror_tracker));
            register_active_download(&active, &starting, gid.clone(), active_download).await;

            if finish_if_startup_aborted(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                "http",
                &counters,
            )
            .await
            {
                return;
            }

            let mut merged_options = merged_options;
            let reserved =
                reserved_part_paths(&tasks, &gid, uris.first().map(String::as_str)).await;
            if !reserved.is_empty() {
                merged_options.insert(RESERVED_PARTS_OPTION.into(), Value::Array(reserved));
            }

            let c = counters.clone();
            let download_result = http::run_http_download_tracked(
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
                &mirror_tracker,
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
                    settle_mirror_progress(task, &uris, &mirror_tracker);
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
        let (worker_epoch, starting) = self.publish_starting_worker(&gid, parent.clone());
        tokio::spawn(async move {
            let mut ad = agg.to_active_plain(worker_epoch);
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
                    let label = metalink_mirror_label(&spec.uris);
                    let r = match file_transfer_mirror(&spec.uris) {
                        Some(uri) => {
                            let fetched = super::ftp::run_ftp_download(
                                uri,
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
                                spec.adopted,
                            )
                            .await;
                            match fetched {
                                Ok(path) => http::verify_checksum_option(&path, &options)
                                    .await
                                    .map(|()| path),
                                Err(e) => Err(e),
                            }
                        }
                        None => {
                            http::run_http_download_multi(
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
                            .await
                        }
                    };
                    (spec.idx, spec.out, r, label)
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
        let out = task.out.clone();
        let task_options = task.options.clone();
        let seeding = task.seeder;
        let events = self.events.clone();
        let tasks = self.tasks.clone();
        let active = self.active_downloads.clone();
        let options = self.options.clone();
        let kad_service = self.kad_service();
        let kad_udp_port = self.kad_udp_port();
        let global_limiter = self.global_speed_limiter.clone();
        let task_limiters = self.task_limiters.clone();
        let task_upload_limiters = self.task_upload_limiters.clone();
        let shares = self.ed2k_shares.clone();
        let user_hash = self.ed2k_user_hash;
        let upload_runtime = self.ed2k_upload.read().clone();
        let uploads_enabled = upload_runtime.uploader().is_some();
        let config_dir = self.config_dir.clone();
        let upload_stats = Arc::new(UploadStats::with_uploaded(task.upload_length));
        let kad_status = Arc::new(parking_lot::Mutex::new(Some(
            self.kad_initial_task_status(),
        )));

        let counters = Counters::new(task.total_length, 0);
        if seeding {
            counters
                .completed
                .store(task.total_length, Ordering::Relaxed);
        }
        let (worker_epoch, starting) =
            self.publish_starting_worker(&gid, counters.cancel_token.clone());
        tokio::spawn(async move {
            let mut active_download = counters.to_active_plain(worker_epoch);
            active_download.kad_status = kad_status.clone();
            active_download.ed2k_upload = Some(upload_stats.clone());
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
            let share = ShareContext {
                registry: shares,
                user_hash,
                stats: upload_stats,
                limiter: register_task_limiter(
                    &task_upload_limiters,
                    &gid,
                    super::torrent::task_limit(&effective_options.global, "max-upload-limit"),
                ),
            };

            let c = counters.clone();
            let result = match file_link {
                Ok(link) if seeding => {
                    let path = super::ed2k::partial::final_path(&dir, ed2k_file_name(&out, &link));
                    match upload_runtime {
                        UploadRuntime::Running(_) => {
                            async {
                                let _claim =
                                    super::ed2k::partial::PathClaim::acquire_async(path.clone())
                                        .await?;
                                let verified = verify_complete_file(
                                    config_dir,
                                    path.clone(),
                                    link.file_hash_bytes,
                                    link.file_size,
                                    c.cancel_token.clone(),
                                )
                                .await;
                                seed_ed2k(verified, &share, &link, &path, &c.cancel_token).await
                            }
                            .await
                        }
                        UploadRuntime::Disabled => Ok(path),
                        UploadRuntime::Failed(reason) => {
                            Err(format!("eD2K sharing is unavailable: {reason}"))
                        }
                    }
                }
                Ok(link) => {
                    async {
                        let p2p_proxy = effective_options.p2p_proxy_connector()?;
                        let throttle = risuko_bt::limiter::Throttle::new(
                            global_limiter,
                            register_task_limiter(
                                &task_limiters,
                                &gid,
                                super::torrent::task_limit(
                                    &effective_options.global,
                                    "max-download-limit",
                                ),
                            ),
                        );
                        let auto_rename = effective_options
                            .get_bool("auto-file-renaming")
                            .unwrap_or(true);
                        let target = {
                            let (dir, name, link) = (
                                dir.clone(),
                                ed2k_file_name(&out, &link).to_string(),
                                link.clone(),
                            );
                            tokio::task::spawn_blocking(move || {
                                super::ed2k::partial::reserve(&dir, &name, &link, auto_rename)
                            })
                            .await
                            .map_err(|e| format!("Reserve task failed: {e}"))??
                        };
                        record_ed2k_target(&tasks, &gid, target.path()).await;
                        let downloaded = super::ed2k::run_ed2k_download_with_proxy(
                            &link,
                            target,
                            auto_rename,
                            effective_options.ed2k_servers(),
                            effective_options.ed2k_port(),
                            kad_udp_port,
                            kad_service,
                            kad_status,
                            c.total,
                            c.completed,
                            c.speed,
                            c.connections,
                            c.cancel_token.clone(),
                            p2p_proxy,
                            throttle,
                            uploads_enabled.then(|| share.clone()),
                        )
                        .await;
                        if let Ok(done) = &downloaded {
                            record_ed2k_target(&tasks, &gid, &done.path).await;
                        }
                        let super::ed2k::download::Ed2kCompleted {
                            path,
                            hashset,
                            claim: _claim,
                        } = match downloaded {
                            Ok(done) if uploads_enabled => done,
                            other => return other.map(|done| done.path),
                        };
                        let verified = match remember_complete_file(
                            config_dir,
                            path.clone(),
                            link.file_hash_bytes,
                            hashset,
                        )
                        .await
                        {
                            Ok(verified) => verified,
                            Err(error) => {
                                tracing::warn!("[ed2k] Not sharing {}: {error}", path.display());
                                return Ok(path);
                            }
                        };
                        if !begin_ed2k_seeding(
                            &tasks,
                            &active,
                            &options,
                            &events,
                            &gid,
                            worker_epoch,
                            &path,
                        )
                        .await
                        {
                            return Ok(path);
                        }
                        counters.speed.store(0, Ordering::Relaxed);
                        seed_ed2k(Ok(verified), &share, &link, &path, &c.cancel_token).await
                    }
                    .await
                }
                Err(e) => Err(e),
            };
            let ok = result.is_ok();

            finish_task(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                "ed2k",
                &counters,
                result,
                |task| {
                    if ok && task.seeder && task.status == TaskStatus::Active {
                        task.status = TaskStatus::Complete;
                        clear_seeding(task);
                    }
                },
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

        let (worker_epoch, starting) =
            self.publish_starting_worker(&gid, counters.cancel_token.clone());
        tokio::spawn(async move {
            register_active_download(
                &active,
                &starting,
                gid.clone(),
                counters.to_active_plain(worker_epoch),
            )
            .await;

            if finish_if_startup_aborted(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                "media",
                &counters,
            )
            .await
            {
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
        let (worker_epoch, starting) =
            self.publish_starting_worker(&gid, counters.cancel_token.clone());
        tokio::spawn(async move {
            register_active_download(
                &active,
                &starting,
                gid.clone(),
                counters.to_active_plain(worker_epoch),
            )
            .await;

            if finish_if_startup_aborted(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                "m3u8",
                &counters,
            )
            .await
            {
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

    fn spawn_ftp_download(&self, task: &DownloadTask, mut merged_options: Map<String, Value>) {
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
        let (worker_epoch, starting) =
            self.publish_starting_worker(&gid, counters.cancel_token.clone());
        let adopted_filename: Arc<parking_lot::Mutex<Option<String>>> =
            Arc::new(parking_lot::Mutex::new(None));
        tokio::spawn(async move {
            register_active_download(
                &active,
                &starting,
                gid.clone(),
                counters.to_active(worker_epoch, Vec::new(), adopted_filename.clone()),
            )
            .await;

            if finish_if_startup_aborted(
                &tasks,
                &active,
                &events,
                &gid,
                worker_epoch,
                "ftp",
                &counters,
            )
            .await
            {
                return;
            }

            let reserved = reserved_part_paths(&tasks, &gid, Some(&uri)).await;
            if !reserved.is_empty() {
                merged_options.insert(RESERVED_PARTS_OPTION.into(), Value::Array(reserved));
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
                adopted_filename,
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
                let now = crate::engine::util::now_ms();

                for task in tasks.iter_mut() {
                    checkpoint_seed_clock(task, now);
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
                            if let Some(upload) = &ad.ed2k_upload {
                                task.upload_length = upload.uploaded();
                                task.upload_speed = upload.speed();
                                if task.seeder {
                                    task.connections = upload.peers();
                                    if seeding_should_end(task, g_manual, g_seed_time, g_seed_ratio)
                                    {
                                        task.status = TaskStatus::Complete;
                                        clear_seeding(task);
                                        ad.cancel_token.cancel();
                                        self.events.send(EngineEvent::DownloadComplete {
                                            gid: task.gid.clone(),
                                            seeded: true,
                                        });
                                        continue;
                                    }
                                }
                            }
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
                        if let Some(tracker) = &ad.mirrors {
                            task.mirror_progress = mirror_progress(task, tracker);
                        }

                        if let Some(f) = task
                            .files
                            .first_mut()
                            .filter(|_| task.kind != TaskKind::Usenet)
                        {
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
            let has_active_torrents = !active_torrent_gids.is_empty();
            let torrent_stats_by_gid: HashMap<String, (usize, torrent::TorrentStats)> =
                if !has_active_torrents {
                    HashMap::new()
                } else {
                    let te_guard = self.torrent_engine.read().await;
                    let tid_guard = self.torrent_ids.read().await;
                    if let Some(ref te) = *te_guard {
                        active_torrent_gids
                            .into_iter()
                            .filter_map(|gid| {
                                let tid = *tid_guard.get(&gid)?;
                                te.get_torrent_stats(tid).map(|stats| (gid, (tid, stats)))
                            })
                            .collect()
                    } else {
                        HashMap::new()
                    }
                };

            let mut halted = Vec::new();
            let mut write_halted = Vec::new();
            let mut to_stamp = Vec::new();
            let mut flush_marked = false;
            if has_active_torrents {
                let mut tasks = self.tasks.write().await;
                let resumed_since_snapshot =
                    self.torrent_resume_seq.load(Ordering::SeqCst) != resume_seq;
                for task in tasks.iter_mut() {
                    if task.kind != TaskKind::Torrent || task.status != TaskStatus::Active {
                        if !task.peers.is_empty() {
                            task.peers = Vec::new();
                        }
                        task.bt_peers_key = None;
                        task.swarm_seeders = None;
                        task.swarm_leechers = None;
                        continue;
                    }
                    if let Some((tid, stats)) = torrent_stats_by_gid.get(&task.gid) {
                        task.total_length = stats.total_bytes;
                        task.completed_length = stats.downloaded_bytes;
                        task.upload_length =
                            self.torrent_upload_total(task, *tid, stats.uploaded_bytes);
                        task.download_speed = stats.download_speed;
                        task.upload_speed = stats.upload_speed;
                        task.connections = stats.num_peers;
                        task.num_seeders = stats.num_seeders;
                        task.swarm_seeders = stats.swarm_seeders;
                        task.swarm_leechers = stats.swarm_leechers;

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
                                    let selected_indices = select_file
                                        .and_then(torrent::parse_select_file)
                                        .and_then(|indices| {
                                            torrent::selection_within(indices, file_details.len())
                                        });
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
                            if surfaces_torrent_error(
                                task,
                                stats.is_finished,
                                resumed_since_snapshot,
                                stats.storage_error,
                            ) {
                                write_halted.push(task.gid.clone());
                                task.status = TaskStatus::Error;
                                task.error_code =
                                    Some(classify_error(error, "torrent").to_string());
                                task.error_message = Some(error.clone());
                                task.download_speed = 0;
                                clear_seeding(task);
                                self.events.send(EngineEvent::DownloadError {
                                    gid: task.gid.clone(),
                                });
                            }
                        }

                        flush_marked |= remember_failed_flush(
                            task,
                            stats.flush_failed,
                            super::boot_id::current(),
                        );
                        if wants_file_stamps(
                            task,
                            stats.is_finished,
                            &mut self.stamp_attempts.lock(),
                        ) {
                            to_stamp.push(task.gid.clone());
                        }

                        let (keep, _, _) =
                            resolve_seed_goal(&task.options, g_manual, g_seed_time, g_seed_ratio);
                        let downloaded = !seeds_in_place(&task.options);

                        if stats.is_finished && !task.seeder && task.status == TaskStatus::Active {
                            if keep {
                                start_seeding(task);
                                if downloaded {
                                    self.events.send(EngineEvent::BtDownloadComplete {
                                        gid: task.gid.clone(),
                                        seeding: true,
                                    });
                                }
                            } else {
                                task.status = TaskStatus::Complete;
                                task.download_speed = 0;
                                task.upload_speed = 0;
                                halted.push(task.gid.clone());
                                if downloaded {
                                    self.events.send(EngineEvent::BtDownloadComplete {
                                        gid: task.gid.clone(),
                                        seeding: false,
                                    });
                                    self.events.send(EngineEvent::DownloadComplete {
                                        gid: task.gid.clone(),
                                        seeded: false,
                                    });
                                }
                            }
                        }

                        if task.seeder
                            && seeding_should_end(task, g_manual, g_seed_time, g_seed_ratio)
                        {
                            task.status = TaskStatus::Complete;
                            clear_seeding(task);
                            halted.push(task.gid.clone());
                            if downloaded {
                                self.events.send(EngineEvent::DownloadComplete {
                                    gid: task.gid.clone(),
                                    seeded: true,
                                });
                            }
                        }
                    } else {
                        task.swarm_seeders = None;
                        task.swarm_leechers = None;
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
            self.save_new_flush_marks(flush_marked).await;
            self.record_file_stamps(to_stamp).await;
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
        saved_torrent_path_in(&self.config_dir, info_hash)
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
        let target = path.clone();
        let data = data.to_vec();
        let saved = tokio::task::spawn_blocking(move || {
            crate::traits::write_file_atomically(&target, &data)
        })
        .await
        .unwrap_or_else(|e| Err(e.to_string()));
        if let Err(e) = saved {
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
        let Some(hash) = hash else {
            return;
        };
        let shared = self.tasks.read().await.iter().any(|t| {
            t.gid != gid
                && t.kind == TaskKind::Torrent
                && !matches!(t.status, TaskStatus::Removed)
                && t.info_hash
                    .as_deref()
                    .is_some_and(|other| other.eq_ignore_ascii_case(&hash))
        });
        if shared {
            return;
        }
        if let Some(path) = self.saved_torrent_path(&hash) {
            tokio::fs::remove_file(path).await.ok();
        }
    }

    async fn restore_saved_torrent_files(&self) {
        type Candidate = (
            String,
            String,
            Map<String, Value>,
            Vec<risuko_bt::create::FileStamp>,
            bool,
        );
        let candidates: Vec<Candidate> = {
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
                        task.verified_files(),
                        !task.bt_file_stamps.is_empty(),
                    )
                })
                .collect()
        };
        for (gid, info_hash, merged, verified_files, had_stamps) in candidates {
            let Some(path) = self.saved_torrent_path(&info_hash) else {
                continue;
            };
            let data = match tokio::fs::read(&path).await {
                Ok(data) => data,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.fail_torrent_restore(
                        &gid,
                        "Saved torrent file is missing; add the torrent again".to_string(),
                    )
                    .await;
                    continue;
                }
                Err(_) => continue,
            };
            let Some(engine) = self.torrent_engine.read().await.clone() else {
                return;
            };
            let moved = tokio::task::spawn_blocking({
                let (data, merged) = (data.clone(), merged.clone());
                move || {
                    let renamed = if seeds_in_place(&merged) {
                        0
                    } else {
                        torrent::adopt_legacy_disk_names(&data, &merged)
                    };
                    renamed + torrent::adopt_stripped_v2_layout(&data, &merged)
                }
            })
            .await
            .ok();
            if let Some(moved) = moved.filter(|moved| *moved > 0) {
                tracing::info!("[task:{gid}] moved {moved} file(s) out of an earlier layout");
            }
            let verified_files = if moved == Some(0) {
                verified_files
            } else {
                Vec::new()
            };
            match engine
                .add_verified_torrent_bytes(&data, &merged, verified_files)
                .await
            {
                Ok(handle) => {
                    tracing::info!("[task:{gid}] re-added saved torrent as id={}", handle.id);
                    if had_stamps {
                        self.use_up_file_stamps(&gid).await;
                    }
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
                Err(e) => self.fail_torrent_restore(&gid, e).await,
            }
        }
    }

    async fn use_up_file_stamps(&self, gid: &str) {
        self.stamp_attempts.lock().remove(gid);
        if let Some(task) = self.tasks.write().await.iter_mut().find(|t| t.gid == gid) {
            task.bt_file_stamps.clear();
        }
    }

    async fn record_file_stamps(&self, gids: Vec<String>) {
        if gids.is_empty() {
            return;
        }
        let targets: Vec<(String, usize)> = {
            let ids = self.torrent_ids.read().await;
            gids.into_iter()
                .filter_map(|gid| {
                    let tid = *ids.get(&gid)?;
                    Some((gid, tid))
                })
                .collect()
        };
        let Some(te) = self.torrent_engine.read().await.clone() else {
            return;
        };
        let tasks = self.tasks.clone();
        let attempted = self.stamp_attempts.clone();
        tokio::spawn(async move {
            for (gid, tid) in targets {
                let Some(stamps) = te.complete_file_stamps(tid).await else {
                    continue;
                };
                let stored = TaskFileStamp::from_stamps(&stamps);
                if stored.is_empty() {
                    continue;
                }
                let mut tasks = tasks.write().await;
                if !attempted.lock().contains(&gid) {
                    continue;
                }
                if let Some(task) = tasks.iter_mut().find(|t| takes_late_file_stamps(t, &gid)) {
                    task.bt_file_stamps = stored;
                }
            }
        });
    }

    async fn fail_torrent_restore(&self, gid: &str, error: String) {
        tracing::warn!("[task:{gid}] re-adding saved torrent failed: {error}");
        let mut tasks = self.tasks.write().await;
        if let Some(task) = tasks
            .iter_mut()
            .find(|t| t.gid == gid && t.status == TaskStatus::Active)
        {
            task.status = TaskStatus::Error;
            task.error_code = Some(classify_error(&error, "torrent").to_string());
            task.error_message = Some(error);
            task.download_speed = 0;
            pause_seed_clock(task, crate::engine::util::now_ms());
            self.events.send(EngineEvent::DownloadError {
                gid: gid.to_string(),
            });
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
                    pause_seed_clock(task, crate::engine::util::now_ms());
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
            task.usenet_warning = None;
            task.status = if is_torrent {
                TaskStatus::Active
            } else {
                TaskStatus::Waiting
            };
            if is_torrent {
                resume_seed_clock(task, crate::engine::util::now_ms());
            }
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

    pub async fn restart_task(&self, gid: &str) -> Result<(), String> {
        tracing::info!("[task:{gid}] Restarting");
        let (status, is_torrent, articles_gone) = {
            let tasks = self.tasks.read().await;
            let task = tasks
                .iter()
                .find(|t| t.gid == gid)
                .ok_or_else(|| format!("Task {gid} not found"))?;
            (
                task.status,
                task.kind == TaskKind::Torrent,
                task.kind == TaskKind::Usenet && task.usenet_articles_dropped(),
            )
        };
        match status {
            TaskStatus::Error => return self.unpause(gid).await,
            TaskStatus::Removed if articles_gone => {
                return Err(format!(
                    "Task {gid} cannot be restarted: its article list is gone; add the NZB again"
                ))
            }
            TaskStatus::Removed => {}
            TaskStatus::Complete => {
                return Err(format!(
                    "Task {gid} is complete; add it again to download another copy"
                ))
            }
            _ => return Err(format!("Task {gid} is not stopped")),
        }

        let rebuilt_magnet = if is_torrent {
            self.removed_torrent_magnet(gid).await?
        } else {
            None
        };

        if is_torrent {
            self.torrent_resume_seq.fetch_add(1, Ordering::SeqCst);
        }

        {
            let mut tasks = self.tasks.write().await;
            let task = tasks
                .iter_mut()
                .find(|t| t.gid == gid && t.status == TaskStatus::Removed)
                .ok_or_else(|| format!("Task {gid} is not stopped"))?;
            if let Some(magnet) = rebuilt_magnet {
                task.uris = vec![magnet];
            }
            task.error_code = None;
            task.error_message = None;
            task.usenet_repair_failure = None;
            task.usenet_warning = None;
            task.download_speed = 0;
            task.upload_speed = 0;
            task.status = if is_torrent {
                TaskStatus::Active
            } else {
                TaskStatus::Waiting
            };
            if is_torrent {
                resume_seed_clock(task, crate::engine::util::now_ms());
            }
        }

        if is_torrent {
            self.ensure_active_magnet_resolvers().await;
            self.send_download_start(gid);
        } else {
            self.try_start_next().await;
        }

        Ok(())
    }

    async fn removed_torrent_magnet(&self, gid: &str) -> Result<Option<String>, String> {
        let (has_magnet, info_hash, rebuilt) = {
            let tasks = self.tasks.read().await;
            let task = tasks
                .iter()
                .find(|t| t.gid == gid)
                .ok_or_else(|| format!("Task {gid} not found"))?;
            let info_hash = task.info_hash.clone().filter(|h| !h.is_empty());
            if let Some(hash) = info_hash.as_deref() {
                let duplicate = tasks.iter().any(|t| {
                    t.gid != gid
                        && t.status != TaskStatus::Removed
                        && t.info_hash
                            .as_deref()
                            .is_some_and(|other| other.eq_ignore_ascii_case(hash))
                });
                if duplicate {
                    return Err(format!(
                        "Task {gid} cannot be restarted: the same torrent is already in the list"
                    ));
                }
            }
            (
                task.uris.iter().any(|uri| torrent::is_magnet_uri(uri)),
                info_hash,
                magnet_for_torrent(task),
            )
        };
        if has_magnet {
            return Ok(None);
        }
        if let Some(path) = info_hash
            .as_deref()
            .and_then(|h| self.saved_torrent_path(h))
        {
            if tokio::fs::try_exists(&path).await.unwrap_or(false) {
                return Ok(None);
            }
        }
        rebuilt.map(Some).ok_or_else(|| {
            format!(
                "Task {gid} cannot be restarted: its torrent data is gone; add the torrent again"
            )
        })
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
            pause_seed_clock(task, crate::engine::util::now_ms());
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
        self.retry_http_task(gid, RetryCookies::Given(cookie), user_agent)
            .await
    }

    pub async fn retry_with_stored_cookies(
        &self,
        gid: &str,
        user_agent: Option<String>,
    ) -> Result<(), String> {
        tracing::info!("[task:{}] Retrying with stored cookies", gid);
        self.retry_http_task(gid, RetryCookies::Stored, user_agent)
            .await
    }

    async fn retry_http_task(
        &self,
        gid: &str,
        cookies: RetryCookies,
        user_agent: Option<String>,
    ) -> Result<(), String> {
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
            if matches!(task.status, TaskStatus::Complete | TaskStatus::Removed) {
                return Err(format!("Task {} has already finished", gid));
            }
            match cookies {
                RetryCookies::Given(Some(c)) if !c.is_empty() => {
                    task.options.insert("cookie".to_string(), Value::String(c));
                }
                RetryCookies::Given(_) => {}
                RetryCookies::Stored => {
                    task.options.remove("cookie");
                    task.options.remove(super::http::COOKIE_HOST_OPTION);
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
        let worker = self.worker_epoch_and_cancel(gid).await;
        if let Some((_, token)) = &worker {
            token.cancel();
        }

        let (found, is_ed2k) = {
            let mut tasks = self.tasks.write().await;
            match tasks.iter_mut().find(|t| t.gid == gid) {
                Some(task) => {
                    task.status = TaskStatus::Removed;
                    task.download_speed = 0;
                    task.upload_speed = 0;
                    pause_seed_clock(task, crate::engine::util::now_ms());
                    (true, task.kind == TaskKind::Ed2k)
                }
                None => (false, false),
            }
        };

        self.forget_saved_torrent_file(gid).await;
        if let Some((te, tid)) = self.torrent_target(gid).await {
            te.remove(tid, false).await.ok();
        }
        self.torrent_ids.write().await.remove(gid);
        self.torrent_upload_bases.lock().remove(gid);
        self.pending_magnets.write().await.remove(gid);
        self.cleanup_m3u8_temp(gid).await;
        self.cleanup_p2p_partial(gid).await;
        if let Some((epoch, _)) = worker.filter(|_| is_ed2k) {
            if !self.wait_for_worker_epoch_exit(gid, epoch).await {
                tracing::warn!("[task:{gid}] eD2K worker still running; keeping its partial file");
            }
        }
        self.cleanup_ed2k_partials(&[gid.to_string()]).await;

        if found {
            self.signal_task_list_change();
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
            "POS_CUR" => (current_waiting_pos as i64).saturating_add(pos).max(0) as usize,
            "POS_END" => {
                if pos >= 0 {
                    waiting.len().saturating_sub(1)
                } else {
                    (waiting.len() as i64 - 1).saturating_add(pos).max(0) as usize
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

    async fn torrent_is_private(&self, gid: &str) -> bool {
        let tid = self.torrent_ids.read().await.get(gid).copied();
        if let Some(tid) = tid {
            if let Some(engine) = self.torrent_engine.read().await.clone() {
                return engine.private_tracker_tiers(tid).is_some();
            }
        }
        let info_hash = self
            .tasks
            .read()
            .await
            .iter()
            .find(|task| task.gid == gid)
            .and_then(|task| task.info_hash.clone());
        let Some(path) = info_hash.and_then(|hash| self.saved_torrent_path(&hash)) else {
            return false;
        };
        tokio::fs::read(path)
            .await
            .ok()
            .and_then(|data| risuko_bt::parse_torrent(&data).ok())
            .is_some_and(|meta| meta.info.private)
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
                if crate::config::keys::is_startup_only(key) {
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
        let private_torrent = !normalized_trackers.is_empty() && self.torrent_is_private(gid).await;

        let mut pending_apply: Option<TaskPatchInputs> = None;
        let mut updating_guard: Option<UpdatingGuard> = None;
        let (
            kind,
            was_active,
            mut old_dir,
            mut old_out,
            mut new_dir,
            mut new_out,
            mut path_changed,
            uris_changed,
            mut primary_uri_changed,
            options_need_restart,
            tracker_urls_to_add,
            mut old_primary_uri,
            mut primary_uri,
        ) = {
            let mut tasks = self.tasks.write().await;
            let task = tasks
                .iter_mut()
                .find(|t| t.gid == gid)
                .ok_or_else(|| format!("GID {gid} not found"))?;

            if self.updating_tasks.lock().contains(gid) {
                return Err(format!("Task {gid} is being updated; try again"));
            }

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

            let inputs = TaskPatchInputs {
                uris: normalized_uris.clone(),
                dir: normalized_dir.clone(),
                out: normalized_out.clone(),
                options: patch.options.clone(),
            };
            let mut staged = task.clone();
            let applied = apply_task_patch(
                &mut staged,
                normalized_uris,
                normalized_dir,
                normalized_out,
                &normalized_trackers,
                patch.options,
            );
            if private_torrent && !applied.tracker_urls_to_add.is_empty() {
                return Err(
                    "Private torrents only announce to the trackers in their metainfo".to_string(),
                );
            }
            let needs_restart = was_active
                && kind != TaskKind::Torrent
                && (applied.uris_changed || applied.path_changed || applied.options_need_restart);
            let moves_partial = applied.path_changed && kind != TaskKind::Torrent;
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
            if needs_restart || moves_partial {
                self.updating_tasks.lock().insert(gid.to_string());
                updating_guard = Some(UpdatingGuard {
                    set: self.updating_tasks.clone(),
                    gid: gid.to_string(),
                });
            }
            if needs_restart {
                tracing::info!("[task:{}] Restarting worker to apply property edits", gid);
                task.status = TaskStatus::Waiting;
                task.download_speed = 0;
                task.upload_speed = 0;
                pause_seed_clock(task, crate::engine::util::now_ms());
                self.events.send(EngineEvent::DownloadPause {
                    gid: gid.to_string(),
                });
                pending_apply = Some(inputs);
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
                                resume_seed_clock(task, crate::engine::util::now_ms());
                            }
                        }
                    }
                    return Err(format!(
                        "Timed out waiting for worker to stop after {WORKER_EXIT_TIMEOUT:?}"
                    ));
                }
            }
            if let Some(inputs) = pending_apply.take() {
                let mut tasks = self.tasks.write().await;
                let task = tasks
                    .iter_mut()
                    .find(|t| t.gid == gid)
                    .ok_or_else(|| format!("GID {gid} not found"))?;
                if task.status.is_stopped() && task.status != TaskStatus::Error {
                    return Err(format!(
                        "Task {gid} finished ({}) before the edit could be applied",
                        task.status.as_str()
                    ));
                }
                old_dir = task.dir.clone();
                old_out = task.out.clone();
                old_primary_uri = task.uris.first().cloned().unwrap_or_default();
                let applied = apply_task_patch(
                    task,
                    inputs.uris,
                    inputs.dir,
                    inputs.out,
                    &[],
                    inputs.options,
                );
                task.download_speed = 0;
                task.upload_speed = 0;
                new_dir = task.dir.clone();
                new_out = task.out.clone();
                primary_uri = task.uris.first().cloned().unwrap_or_default();
                path_changed = applied.path_changed;
                primary_uri_changed = applied.primary_uri_changed;
            }
        }
        let mut worker_lingers = false;
        if path_changed && kind != TaskKind::Torrent && !needs_restart {
            if let Some((epoch, token)) = self.worker_epoch_and_cancel(gid).await {
                token.cancel();
                worker_lingers = !self.wait_for_worker_epoch_exit(gid, epoch).await;
            }
        }
        let mut progress_preserved = true;
        let mut ed2k_move = None;
        if kind == TaskKind::Ed2k {
            let live = self.live_worker_gids().await;
            let tasks = self.tasks.read().await;
            if let Ok(link) = super::ed2k::parse_ed2k_link(&old_primary_uri) {
                let from =
                    super::ed2k::partial::final_path(&old_dir, ed2k_file_name(&old_out, &link));
                let users = ed2k_partial_users(&tasks, &[gid.to_string()], &live).remove(&from);
                ed2k_move = Some((link, from, users));
            }
        }
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
            if worker_lingers {
                tracing::warn!(
                    "[task:{}] Partial left in {} because the previous worker had not stopped",
                    gid,
                    old_dir
                );
                progress_preserved = false;
            } else if relocate_old_out.is_empty() || relocate_new_out.is_empty() {
                progress_preserved = false;
            } else {
                let (from_dir, to_dir) = (old_dir.clone(), new_dir.clone());
                let (from_out, to_out) = (relocate_old_out.clone(), relocate_new_out.clone());
                let moved = tokio::task::spawn_blocking(move || match ed2k_move {
                    Some((link, from, users)) => {
                        let to = super::ed2k::partial::final_path(&to_dir, &to_out);
                        relocate_ed2k_partial(&from, &to, &link, users.as_ref())
                    }
                    None => http::relocate_partial(&from_dir, &from_out, &to_dir, &to_out),
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

        let rerun_queue = updating_guard.is_some();
        drop(updating_guard.take());
        if rerun_queue {
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
            if task.seeder && requests_stop_seeding(&opts, &task.options) {
                // The engine torrent is paused after tasks.write() is released to avoid nesting torrent-engine locks
                stop_seeding = true;
                clear_seeding(task);
                task.status = TaskStatus::Complete;
                if !seeds_in_place(&task.options) {
                    self.events.send(EngineEvent::DownloadComplete {
                        gid: task.gid.clone(),
                        seeded: true,
                    });
                }
            }
            let touches_limits = ["max-download-limit", "max-upload-limit"]
                .iter()
                .any(|key| opts.contains_key(*key));
            for (k, v) in opts {
                task.options.insert(k, v);
            }
            if reselect.is_some()
                && matches!(task.kind, TaskKind::Torrent | TaskKind::Metalink)
                && task.status != TaskStatus::Active
                && !task.files.is_empty()
            {
                apply_select_file(&mut task.files, &task.options);
                if task.files.iter().all(|f| f.length.parse::<u64>().is_ok()) {
                    metalink_rollup_totals(task);
                }
            }
            let task_options = touches_limits.then(|| task.options.clone());
            drop(tasks);
            if let Some(task_options) = task_options {
                self.apply_task_limits(gid, &task_options).await;
            }
            if stop_seeding {
                if let Some((_, token)) = self.worker_epoch_and_cancel(gid).await {
                    token.cancel();
                }
                if let Some((te, tid)) = self.torrent_target(gid).await {
                    te.pause(tid).await.ok();
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
        if let Some(limiter) = self.task_upload_limiters.lock().get(gid) {
            limiter.set_limit(upload);
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
            let now = crate::engine::util::now_ms();
            for task in tasks.iter_mut() {
                if task.status == TaskStatus::Active && Self::is_p2p_task_kind(task.kind) {
                    active.push((task.gid.clone(), task.kind));
                    task.status = TaskStatus::Paused;
                    task.download_speed = 0;
                    task.upload_speed = 0;
                    pause_seed_clock(task, now);
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
        let dht_options = self.bt_tuning(&new_options, proxy.clone()).dht_options();
        let mut dht_swap = match risuko_bt::dht::Dht::prepare_shared_with_proxy(
            proxy.clone(),
            dht_options,
        )
        .await
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
                    resume_seed_clock(task, crate::engine::util::now_ms());
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
        self.rebuild_upload_runtime(&new_options).await;
        self.try_start_next_unlocked().await;
        Ok(())
    }

    async fn stop_uploader(&self) -> Option<risuko_bt::session::PortMapper> {
        let mapper = self.ed2k_port_mapper.lock().take();
        let old = std::mem::replace(&mut *self.ed2k_upload.write(), UploadRuntime::Disabled);
        if let UploadRuntime::Running(uploader) = old {
            uploader.shutdown().await;
        }
        mapper
    }

    async fn rebuild_upload_runtime(&self, options: &EngineOptions) {
        let previous_mapper = self.stop_uploader().await;
        let kad = self.kad_runtime.read().clone();
        let next = UploadRuntime::build(
            self.ed2k_user_hash,
            options,
            kad.service(),
            self.ed2k_shares.clone(),
            self.global_upload_limiter.clone(),
        )
        .await;
        *self.ed2k_port_mapper.lock() = next.port_mapper(options, &kad, previous_mapper);
        *self.ed2k_upload.write() = next;
    }

    async fn initialize_torrent_engine(
        &self,
        options: &EngineOptions,
        proxy: Option<risuko_http::ProxyConnector>,
    ) -> Result<(), String> {
        let output_dir = options.dir();
        let tuning = self.bt_tuning(options, proxy);
        let engine = TorrentEngine::new_with_tuning(Path::new(&output_dir), tuning).await?;
        *self.torrent_engine.write().await = Some(engine);
        Ok(())
    }

    fn bt_tuning(
        &self,
        options: &EngineOptions,
        proxy: Option<risuko_http::ProxyConnector>,
    ) -> super::torrent::BtTuning {
        super::torrent::BtTuning::from_options(
            options,
            self.global_speed_limiter.clone(),
            self.global_upload_limiter.clone(),
            proxy,
        )
        .with_config_dir(&self.config_dir)
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
                pause_seed_clock(task, crate::engine::util::now_ms());
                task.error_code =
                    Some(super::error_code::ErrorCode::PROXY_CONNECTION_FAILED.to_string());
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
        let touches_ed2k_servers = opts.contains_key("ed2k-server");
        let mut max_connections = None;
        {
            let mut options = self.options.write().await;
            for (k, v) in opts {
                options.set(k, v);
            }
            if touches_conns {
                max_connections = Some(options.bt_max_connections());
            }
            if touches_ed2k_servers {
                if let Some(uploader) = self.ed2k_upload.read().uploader() {
                    uploader.set_servers(super::ed2k::server_list::server_list(
                        &options.ed2k_servers(),
                    ));
                }
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
                        let speed = task
                            .mirror_progress
                            .iter()
                            .find(|m| m.uri == u.uri)
                            .map(|m| m.speed)
                            .unwrap_or(0);
                        serde_json::json!({
                            "uri": u.uri,
                            "currentUri": u.uri,
                            "downloadSpeed": speed.to_string(),
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

    pub(crate) fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<EngineEvent> {
        self.events.subscribe()
    }

    fn request_save(&self) {
        self.save_requested.notify_one();
    }

    pub(crate) async fn wait_for_save_request(&self) {
        self.save_requested.notified().await;
    }

    fn signal_task_list_change(&self) {
        self.task_list_changed.notify_one();
    }

    pub(crate) async fn wait_for_task_list_change(&self) {
        self.task_list_changed.notified().await;
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
            self.forget_saved_torrent_file(gid).await;
            self.drop_torrent_engine_entry(gid).await;
            self.cleanup_m3u8_temp(gid).await;
        }
        self.cleanup_ed2k_partials(&to_evict).await;

        let evict: std::collections::HashSet<&str> = to_evict.iter().map(String::as_str).collect();
        let mut tasks = self.tasks.write().await;
        let len_before = tasks.len();
        tasks.retain(|t| !(t.status.is_stopped() && evict.contains(t.gid.as_str())));
        if tasks.len() < len_before {
            self.signal_task_list_change();
        }
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

    async fn live_worker_gids(&self) -> HashSet<String> {
        let mut live: HashSet<String> =
            self.active_downloads.read().await.keys().cloned().collect();
        live.extend(self.starting_workers.lock().keys().cloned());
        live
    }

    async fn cleanup_ed2k_partials(&self, gids: &[String]) {
        let live = self.live_worker_gids().await;
        let targets: Vec<_> = {
            let tasks = self.tasks.read().await;
            let users = ed2k_partial_users(&tasks, gids, &live);
            tasks
                .iter()
                .filter(|t| t.kind == TaskKind::Ed2k && gids.contains(&t.gid))
                .filter_map(ed2k_target)
                .map(|(path, link)| {
                    let users = users.get(&path).cloned();
                    (path, link, users)
                })
                .collect()
        };
        if targets.is_empty() {
            return;
        }
        let config_dir = self.config_dir.clone();
        let _ = tokio::task::spawn_blocking(move || {
            for (path, link, users) in targets {
                remove_ed2k_leftovers(&config_dir, &path, &link, users.as_ref());
            }
        })
        .await;
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
        let Some(path) = target else {
            return;
        };
        if is_reserved(&reserved_part_paths(&self.tasks, gid, None).await, &path) {
            tracing::warn!(
                "[task:{gid}] keeping {}: another unfinished download owns it",
                path.display()
            );
            return;
        }
        if let Err(e) = tokio::fs::remove_file(&path).await {
            if e.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!("could not delete {}: {e}", path.display());
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
        self.cleanup_ed2k_partials(&stopped_ed2k_gids).await;
        let mut tasks = self.tasks.write().await;
        let len_before = tasks.len();
        tasks.retain(|t| !t.status.is_stopped());
        if tasks.len() < len_before {
            self.signal_task_list_change();
        }
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
            self.cleanup_ed2k_partials(&[gid.to_string()]).await;
        }
        let mut tasks = self.tasks.write().await;
        let len_before = tasks.len();
        tasks.retain(|t| !(t.gid == gid && t.status.is_stopped()));
        if tasks.len() < len_before {
            self.signal_task_list_change();
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
                clear_seeding(task);
                task.status = TaskStatus::Complete;
                task.download_speed = 0;
            }
        }
    }

    fn torrent_upload_total(&self, task: &DownloadTask, torrent_id: usize, counter: u64) -> u64 {
        let fresh = TorrentUploadBase {
            torrent_id,
            base: task.upload_length,
            last: 0,
        };
        let mut bases = self.torrent_upload_bases.lock();
        let entry = bases.entry(task.gid.clone()).or_insert(fresh);
        if entry.torrent_id != torrent_id {
            *entry = fresh;
        }
        upload_total(&mut entry.base, &mut entry.last, counter)
    }

    async fn drop_torrent_engine_entry(&self, gid: &str) {
        self.torrent_upload_bases.lock().remove(gid);
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
            let now = crate::engine::util::now_ms();
            for task in tasks.iter_mut() {
                if task.status == TaskStatus::Active || task.status == TaskStatus::Waiting {
                    if task.kind == TaskKind::Torrent {
                        torrent_gids.push(task.gid.clone());
                    }
                    task.status = TaskStatus::Paused;
                    task.download_speed = 0;
                    task.upload_speed = 0;
                    pause_seed_clock(task, now);
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
            let now = crate::engine::util::now_ms();
            for task in tasks.iter_mut() {
                if task.status != TaskStatus::Paused {
                    continue;
                }
                if task.kind != TaskKind::Torrent {
                    task.status = TaskStatus::Waiting;
                } else if torrent_gids.contains(&task.gid) {
                    task.status = TaskStatus::Active;
                    resume_seed_clock(task, now);
                    self.send_download_start(&task.gid);
                } else {
                    continue;
                }
                task.error_code = None;
                task.error_message = None;
                task.usenet_repair_failure = None;
                task.usenet_warning = None;
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

    async fn save_new_flush_marks(&self, marked: bool) {
        if !marked {
            return;
        }
        if let Err(e) = self.save_session().await {
            tracing::error!("Failed to save session after a failed flush: {e}");
        }
    }

    async fn remember_flushes_failed_at_shutdown(&self, failed: &[usize]) {
        if failed.is_empty() {
            return;
        }
        let gids: HashSet<String> = self
            .torrent_ids
            .read()
            .await
            .iter()
            .filter(|(_, tid)| failed.contains(tid))
            .map(|(gid, _)| gid.clone())
            .collect();
        if !self
            .tasks
            .read()
            .await
            .iter()
            .any(|task| misses_flush_mark(task, &gids))
        {
            return;
        }
        let changed = mark_flushes_failed_at_shutdown(
            &mut self.tasks.write().await,
            &gids,
            super::boot_id::current(),
        );
        if changed {
            if let Err(e) = self.save_session().await {
                tracing::error!("Failed to save session after a failed flush at shutdown: {e}");
            }
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

        {
            let now = crate::engine::util::now_ms();
            for task in self.tasks.write().await.iter_mut() {
                pause_seed_clock(task, now);
            }
        }
        if let Err(e) = self.save_session().await {
            tracing::error!("Failed to save session on shutdown: {}", e);
        }

        let mut te_guard = self.torrent_engine.write().await;
        let failed = match te_guard.take() {
            Some(mut te) => te.shutdown().await,
            None => Vec::new(),
        };

        drop(self.stop_uploader().await);

        let runtime = self.kad_runtime.read().clone();
        if let KadRuntime::Running(service) = &runtime {
            service.shutdown().await;
        }
        drop(te_guard);
        self.remember_flushes_failed_at_shutdown(&failed).await;
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

fn wants_file_stamps(
    task: &mut DownloadTask,
    finished: bool,
    attempted: &mut HashSet<String>,
) -> bool {
    if !finished {
        attempted.remove(&task.gid);
        task.bt_file_stamps.clear();
        return false;
    }
    if task.bt_flush_failed_boot.is_some() {
        return false;
    }
    task.bt_file_stamps.is_empty() && attempted.insert(task.gid.clone())
}

fn remember_failed_flush(task: &mut DownloadTask, flush_failed: bool, boot: Option<&str>) -> bool {
    if !flush_failed || task.bt_flush_failed_boot.is_some() {
        return false;
    }
    task.bt_flush_failed_boot = Some(boot.unwrap_or(super::boot_id::UNKNOWN).to_string());
    task.bt_file_stamps.clear();
    true
}

fn takes_late_file_stamps(task: &DownloadTask, gid: &str) -> bool {
    task.gid == gid
        && task.kind == TaskKind::Torrent
        && task.bt_file_stamps.is_empty()
        && task.bt_flush_failed_boot.is_none()
}

fn misses_flush_mark(task: &DownloadTask, gids: &HashSet<String>) -> bool {
    task.kind == TaskKind::Torrent
        && task.bt_flush_failed_boot.is_none()
        && gids.contains(&task.gid)
}

fn mark_flushes_failed_at_shutdown(
    tasks: &mut [DownloadTask],
    gids: &HashSet<String>,
    boot: Option<&str>,
) -> bool {
    let mut changed = false;
    for task in tasks
        .iter_mut()
        .filter(|task| misses_flush_mark(task, gids))
    {
        remember_failed_flush(task, true, boot);
        changed = true;
    }
    changed
}

fn forget_failed_flushes_from_earlier_boots(tasks: &mut [DownloadTask], boot: Option<&str>) {
    for task in tasks.iter_mut().filter(|t| t.kind == TaskKind::Torrent) {
        if task
            .bt_flush_failed_boot
            .as_deref()
            .is_some_and(|mark| !super::boot_id::same_boot(mark, boot))
        {
            task.bt_flush_failed_boot = None;
        }
    }
}

fn start_seeding(task: &mut DownloadTask) {
    task.seeder = true;
    resume_seed_clock(task, crate::engine::util::now_ms());
    task.download_speed = 0;
}

fn pause_seed_clock(task: &mut DownloadTask, now: u64) {
    task.seeded_ms = task.seeding_total_ms(now);
    task.seeding_since = 0;
}

fn resume_seed_clock(task: &mut DownloadTask, now: u64) {
    if task.seeder {
        task.seeding_since = now;
    }
}

const SEED_CLOCK_CHECKPOINT_MS: u64 = 5 * 60 * 1000;

fn checkpoint_seed_clock(task: &mut DownloadTask, now: u64) {
    if !(task.seeder && task.status == TaskStatus::Active) {
        if task.seeding_since > 0 {
            pause_seed_clock(task, now);
        }
    } else if task.seeding_since == 0
        || now.saturating_sub(task.seeding_since) >= SEED_CLOCK_CHECKPOINT_MS
    {
        pause_seed_clock(task, now);
        resume_seed_clock(task, now);
    }
}

#[derive(Clone, Copy)]
struct TorrentUploadBase {
    torrent_id: usize,
    base: u64,
    last: u64,
}

fn upload_total(base: &mut u64, last: &mut u64, counter: u64) -> u64 {
    if counter < *last {
        *base = base.saturating_add(*last);
    }
    *last = counter;
    base.saturating_add(counter)
}

fn clear_seeding(task: &mut DownloadTask) {
    pause_seed_clock(task, crate::engine::util::now_ms());
    task.seeder = false;
    task.upload_speed = 0;
}

fn surfaces_torrent_error(
    task: &DownloadTask,
    finished: bool,
    resumed: bool,
    storage_error: bool,
) -> bool {
    task.status == TaskStatus::Active && (!task.seeder || !finished || storage_error) && !resumed
}

fn seed_goal_met(task: &DownloadTask, seed_time_minutes: u64, seed_ratio: f64, now: u64) -> bool {
    let seed_time_ms = seed_time_minutes.saturating_mul(60_000);
    if seed_time_ms > 0 && task.seeding_total_ms(now) >= seed_time_ms {
        return true;
    }
    seed_ratio > 0.0
        && task.total_length > 0
        && task.upload_length as f64 / task.total_length as f64 >= seed_ratio
}

fn seeding_should_end(
    task: &DownloadTask,
    g_manual: bool,
    g_seed_time: u64,
    g_seed_ratio: f64,
) -> bool {
    let (keep, seed_time_minutes, seed_ratio) =
        resolve_seed_goal(&task.options, g_manual, g_seed_time, g_seed_ratio);
    let now = crate::engine::util::now_ms();
    !keep || seed_goal_met(task, seed_time_minutes, seed_ratio, now)
}

fn requests_stop_seeding(change: &Map<String, Value>, own: &Map<String, Value>) -> bool {
    let seed_time = change.get("seed-time").and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
    });
    let keeps_seeding = change
        .get("keep-seeding")
        .and_then(super::options::json_bool)
        == Some(true);
    let ratio = change
        .get("seed-ratio")
        .or_else(|| own.get("seed-ratio"))
        .and_then(|r| {
            r.as_f64()
                .or_else(|| r.as_str().and_then(|s| s.parse().ok()))
        });
    seed_time == Some(0) && !keeps_seeding && ratio.is_none_or(|r| r <= 0.0)
}

fn resolve_seed_goal(
    opts: &Map<String, Value>,
    g_manual: bool,
    g_seed_time: u64,
    g_seed_ratio: f64,
) -> (bool, u64, f64) {
    if seeds_in_place(opts) {
        return (true, 0, 0.0);
    }
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
    (keep, seed_time, seed_ratio)
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
    fn a_selection_that_matches_nothing_shows_every_file_as_selected() {
        let files = || -> Vec<DownloadFile> {
            (0..3)
                .map(|i| DownloadFile {
                    index: (i + 1).to_string(),
                    path: format!("/dl/{i}.bin"),
                    length: "10".to_string(),
                    completed_length: "0".to_string(),
                    selected: "false".to_string(),
                    uris: Vec::new(),
                })
                .collect()
        };
        let selected = |files: &[DownloadFile]| -> Vec<String> {
            files.iter().map(|f| f.selected.clone()).collect()
        };

        let mut opts = Map::new();
        opts.insert("select-file".into(), serde_json::json!("99"));
        let mut list = files();
        apply_select_file(&mut list, &opts);
        assert_eq!(selected(&list), ["true", "true", "true"]);

        opts.insert("select-file".into(), serde_json::json!("2"));
        let mut list = files();
        apply_select_file(&mut list, &opts);
        assert_eq!(selected(&list), ["false", "true", "false"]);
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
        assert_eq!(resolve_seed_goal(&opts, false, 99, 9.0), (true, 99, 9.0));
        assert_eq!(
            resolve_seed_goal(&Map::new(), true, 525600, 1.0),
            (true, 525600, 1.0)
        );
        assert_eq!(resolve_seed_goal(&Map::new(), true, 0, 0.0), (true, 0, 0.0));

        opts.insert("seed-time".into(), serde_json::json!(0));
        opts.insert("seed-ratio".into(), serde_json::json!(0));
        assert_eq!(resolve_seed_goal(&opts, true, 525600, 1.0), (true, 0, 0.0));

        let mut opts = Map::new();
        opts.insert("keep-seeding".into(), serde_json::json!("true"));
        assert_eq!(resolve_seed_goal(&opts, false, 99, 9.0), (true, 99, 9.0));
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

    #[test]
    fn a_torrent_seeded_in_place_seeds_without_limit() {
        assert_eq!(
            resolve_seed_goal(&TaskManager::seed_in_place_options("/x"), true, 525600, 1.0),
            (true, 0, 0.0)
        );
    }

    #[test]
    fn a_torrent_seeded_in_place_ignores_limits_written_into_it() {
        for (time, ratio) in [
            (serde_json::json!(30), serde_json::json!(1.5)),
            (serde_json::json!("30"), serde_json::json!("1.5")),
        ] {
            let mut opts = TaskManager::seed_in_place_options("/x");
            opts.insert("seed-time".into(), time);
            opts.insert("seed-ratio".into(), ratio);
            assert_eq!(resolve_seed_goal(&opts, true, 525600, 1.0), (true, 0, 0.0));
        }
    }

    #[test]
    fn a_torrent_seeded_in_place_by_an_earlier_version_still_seeds_without_limit() {
        let mut opts = TaskManager::seed_in_place_options("/x");
        opts.remove("seed-time");
        opts.remove("seed-ratio");
        assert_eq!(resolve_seed_goal(&opts, true, 525600, 1.0), (true, 0, 0.0));
    }

    #[tokio::test]
    async fn the_keep_seeding_switch_reaches_the_running_engine() {
        let mgr = make_test_manager(Vec::new());
        for (sent, expected) in [("true", true), ("false", false)] {
            let mut opts = Map::new();
            opts.insert("keep-seeding".into(), Value::from(sent));
            mgr.change_global_option(opts).await;
            assert_eq!(mgr.options.read().await.keep_seeding(), expected);
        }
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
            stamp_attempts: Arc::new(parking_lot::Mutex::new(HashSet::new())),
            torrent_upload_bases: parking_lot::Mutex::new(HashMap::new()),
            torrent_ids: Arc::new(RwLock::new(HashMap::new())),
            pending_magnets: Arc::new(RwLock::new(HashSet::new())),
            options: Arc::new(RwLock::new(options)),
            events,
            session: Arc::new(session),
            save_gate: tokio::sync::Mutex::new(()),
            save_requested: tokio::sync::Notify::new(),
            task_list_changed: tokio::sync::Notify::new(),
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
            ed2k_shares: ShareRegistry::new(),
            ed2k_user_hash: [7; 16],
            ed2k_upload: parking_lot::RwLock::new(UploadRuntime::Disabled),
            ed2k_port_mapper: parking_lot::Mutex::new(None),
            task_upload_limiters: Arc::new(parking_lot::Mutex::new(HashMap::new())),
        }
    }

    #[tokio::test]
    async fn the_dht_swap_options_are_the_ones_the_session_tuning_carries() {
        let mgr = make_test_manager(Vec::new());
        let mut system = Map::new();
        system.insert("dht-listen-port".into(), Value::from("26701-26710"));
        let options = EngineOptions::from_config(&system, &Map::new());
        let expected = risuko_bt::dht::DhtOptions {
            port: Some(26701),
            routing_state_path: Some(
                mgr.config_dir
                    .join(super::super::torrent::DHT_ROUTING_STATE_FILENAME),
            ),
        };
        assert_eq!(mgr.bt_tuning(&options, None).dht_options(), expected);
        let from_new = super::super::torrent::BtTuning::from_options(
            &options,
            mgr.global_speed_limiter.clone(),
            mgr.global_upload_limiter.clone(),
            None,
        )
        .with_config_dir(&mgr.config_dir);
        assert_eq!(from_new.dht_options(), expected);
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
        let active_download = counters.to_active_plain(worker_epoch);
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
    fn seeding_stops_at_its_goal_or_when_keep_seeding_is_off() {
        let mut task = DownloadTask::new_ed2k(
            "seed".into(),
            "ed2k://|file|f.bin|1000|0123456789abcdef0123456789abcdef|/".into(),
            "f.bin".into(),
            1000,
            "/dl".into(),
            None,
            Map::new(),
        );
        task.total_length = 1000;
        task.seeding_since = crate::engine::util::now_ms();
        assert!(seeding_should_end(&task, false, 0, 0.0));
        assert!(!seeding_should_end(&task, true, 0, 0.0));
        assert!(!seeding_should_end(&task, false, 60, 0.0));
        task.seeding_since -= 61 * 60 * 1000;
        assert!(seeding_should_end(&task, false, 60, 0.0));
        assert!(!seeding_should_end(&task, false, 0, 1.5));
        task.upload_length = 1500;
        assert!(seeding_should_end(&task, false, 0, 1.5));
        task.options
            .insert("keep-seeding".into(), Value::Bool(true));
        assert!(seeding_should_end(&task, false, 0, 1.5));
        task.options
            .insert("seed-ratio".into(), serde_json::json!(0));
        assert!(!seeding_should_end(&task, false, 0, 1.5));
    }

    fn seeder_with_earlier_seeding(start: u64) -> DownloadTask {
        let mut task = make_torrent_task("s", TaskStatus::Active);
        task.seeder = true;
        task.seeded_ms = 5 * 60_000;
        resume_seed_clock(&mut task, start);
        task
    }

    #[test]
    fn the_seed_clock_counts_each_stretch_and_not_the_time_spent_paused() {
        let start = 1_800_000_000_000;
        let mut task = seeder_with_earlier_seeding(start);
        assert_eq!(task.seeding_total_ms(start + 60_000), 6 * 60_000);

        pause_seed_clock(&mut task, start + 60_000);
        assert_eq!((task.seeded_ms, task.seeding_since), (6 * 60_000, 0));
        assert_eq!(task.seeding_total_ms(start + 3_600_000), 6 * 60_000);

        resume_seed_clock(&mut task, start + 3_600_000);
        assert_eq!(task.seeding_since, start + 3_600_000);
        assert_eq!(
            task.seeding_total_ms(start + 3_630_000),
            6 * 60_000 + 30_000
        );

        clear_seeding(&mut task);
        assert!(!task.seeder);
        assert_eq!(task.seeding_since, 0);
        assert!(task.seeded_ms >= 6 * 60_000);
    }

    #[test]
    fn the_progress_tick_folds_a_running_stretch_only_every_five_minutes() {
        let start = 1_800_000_000_000;
        let mut task = seeder_with_earlier_seeding(start);
        checkpoint_seed_clock(&mut task, start + 4 * 60_000);
        assert_eq!((task.seeded_ms, task.seeding_since), (5 * 60_000, start));

        let at = start + SEED_CLOCK_CHECKPOINT_MS;
        let total = task.seeding_total_ms(at);
        checkpoint_seed_clock(&mut task, at);
        assert_eq!((task.seeded_ms, task.seeding_since), (10 * 60_000, at));
        assert_eq!(task.seeding_total_ms(at), total);
    }

    #[test]
    fn the_progress_tick_closes_a_stray_stretch_and_starts_a_missing_one() {
        let start = 1_800_000_000_000;
        let mut paused = seeder_with_earlier_seeding(start);
        paused.status = TaskStatus::Paused;
        checkpoint_seed_clock(&mut paused, start + 60_000);
        assert_eq!((paused.seeded_ms, paused.seeding_since), (6 * 60_000, 0));

        let mut active = make_torrent_task("a", TaskStatus::Active);
        active.seeder = true;
        checkpoint_seed_clock(&mut active, start);
        assert_eq!((active.seeded_ms, active.seeding_since), (0, start));

        let mut downloading = make_torrent_task("d", TaskStatus::Active);
        checkpoint_seed_clock(&mut downloading, start);
        assert_eq!((downloading.seeded_ms, downloading.seeding_since), (0, 0));
    }

    #[test]
    fn the_seed_time_goal_counts_time_seeded_in_earlier_stretches() {
        let start = 1_800_000_000_000;
        let task = seeder_with_earlier_seeding(start);
        assert!(seed_goal_met(&task, 6, 0.0, start + 60_000));
        assert!(!seed_goal_met(&task, 6, 0.0, start + 59_000));
    }

    async fn seeder_after_a_restart(
        stretch_ms: u64,
        stop: impl AsyncFnOnce(&TaskManager),
    ) -> DownloadTask {
        let dir = tempfile::TempDir::new().unwrap();
        let task = seeder_with_earlier_seeding(crate::engine::util::now_ms() - stretch_ms);
        let mgr = make_test_manager_in(&dir, vec![task]);
        stop(&mgr).await;
        let loaded = SessionManager::new(dir.path()).load().remove(0);
        assert_eq!(
            (loaded.status, loaded.seeding_since),
            (TaskStatus::Paused, 0)
        );
        loaded
    }

    #[tokio::test]
    async fn time_seeded_before_a_restart_counts_toward_the_seed_time() {
        let an_hour_later = || crate::engine::util::now_ms() + 3_600_000;

        let crashed = seeder_after_a_restart(6 * 60_000, async |mgr| {
            mgr.update_progress().await;
            assert!(mgr.tasks.read().await[0].seeding_since > 0);
            mgr.save_session().await.unwrap();
        })
        .await;
        assert!(seed_goal_met(&crashed, 11, 0.0, an_hour_later()));
        assert!(!seed_goal_met(&crashed, 12, 0.0, an_hour_later()));

        let shut_down = seeder_after_a_restart(60_000, async |mgr| mgr.shutdown().await).await;
        assert!(seed_goal_met(&shut_down, 6, 0.0, an_hour_later()));
        assert!(!seed_goal_met(&shut_down, 7, 0.0, an_hour_later()));
    }

    #[tokio::test]
    async fn pausing_a_seeder_stops_its_seed_clock_until_it_resumes() {
        let task = seeder_with_earlier_seeding(crate::engine::util::now_ms() - 60_000);
        let mgr = make_test_manager(vec![task]);

        mgr.pause("s").await.unwrap();
        let paused = mgr.tasks.read().await[0].clone();
        assert_eq!(paused.seeding_since, 0);
        assert!(paused.seeded_ms >= 6 * 60_000);

        mgr.unpause("s").await.unwrap();
        let resumed = mgr.tasks.read().await[0].clone();
        assert_eq!(resumed.status, TaskStatus::Active);
        assert!(resumed.seeding_since > 0);
        assert_eq!(resumed.seeded_ms, paused.seeded_ms);
    }

    async fn install_uploader(mgr: &TaskManager) {
        let mut config = UploadConfig::new(mgr.ed2k_user_hash, 0);
        config.bind_addr = std::net::Ipv4Addr::LOCALHOST;
        config.registry = mgr.ed2k_shares.clone();
        *mgr.ed2k_upload.write() =
            UploadRuntime::Running(Ed2kUploader::bind(config).await.unwrap());
    }

    async fn wait_for(what: &str, mut check: impl AsyncFnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        while !check().await {
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn finished_ed2k_download_seeds_until_its_goal() {
        use super::super::ed2k::share::{hash_file, SharedFile};
        let dir = tempfile::tempdir().unwrap();
        let data: Vec<u8> = (0..9_729_000u32).map(|i| (i % 251) as u8).collect();
        let source_path = dir.path().join("source.bin");
        std::fs::write(&source_path, &data).unwrap();
        let (hash, hashset) = hash_file(&source_path, &CancellationToken::new()).unwrap();

        let mut source_config = UploadConfig::new(super::super::ed2k::protocol::new_user_hash(), 0);
        source_config.bind_addr = std::net::Ipv4Addr::LOCALHOST;
        let source = Ed2kUploader::bind(source_config).await.unwrap();
        let _offer = source.share(SharedFile::complete(
            hash,
            "source.bin",
            data.len() as u64,
            source_path,
            hashset,
        ));

        let mgr = make_test_manager(Vec::new());
        install_uploader(&mgr).await;
        {
            let mut options = mgr.options.write().await;
            options.set("ed2k-server".into(), Value::from("127.0.0.1:1"));
            options.set("keep-seeding".into(), Value::Bool(true));
            let out = dir.path().join("out");
            options.set("dir".into(), Value::from(out.to_str().unwrap()));
        }
        let taken = dir.path().join("out").join("got.bin");
        std::fs::create_dir_all(taken.parent().unwrap()).unwrap();
        std::fs::write(&taken, b"someone else's file").unwrap();
        let uri = format!(
            "ed2k://|file|got.bin|{}|{}|/|sources,127.0.0.1:{}|/",
            data.len(),
            hex::encode(hash),
            source.port()
        );
        let mut events = mgr.events.subscribe();
        let gid = mgr.add_ed2k_task(&uri, Map::new()).await.unwrap();

        wait_for("the task to start seeding", async || {
            mgr.tasks
                .read()
                .await
                .iter()
                .any(|t| t.gid == gid && t.seeder && t.status == TaskStatus::Active)
        })
        .await;
        wait_for("the complete share", async || {
            mgr.ed2k_shares
                .get(&hash)
                .is_some_and(|file| file.is_complete())
        })
        .await;
        let path = dir.path().join("out").join("got.1.bin");
        let claimed = || super::super::ed2k::partial::PathClaim::acquire(&path).is_err();
        assert_eq!(std::fs::read(&path).unwrap(), data);
        assert_eq!(std::fs::read(&taken).unwrap(), b"someone else's file");
        assert!(claimed());
        {
            let tasks = mgr.tasks.read().await;
            let task = tasks.iter().find(|t| t.gid == gid).unwrap();
            assert_eq!(task.out, "got.1.bin");
            assert_eq!(task.files[0].path, path.to_string_lossy());
            assert_eq!(task.completed_length, data.len() as u64);
        }

        mgr.pause(&gid).await.unwrap();
        wait_for("the share to be withdrawn", async || {
            mgr.ed2k_shares.get(&hash).is_none() && mgr.active_downloads.read().await.is_empty()
        })
        .await;
        assert!(!claimed());
        {
            let tasks = mgr.tasks.read().await;
            let task = tasks.iter().find(|t| t.gid == gid).unwrap();
            assert_eq!(task.status, TaskStatus::Paused);
            assert!(task.seeder);
        }

        mgr.unpause(&gid).await.unwrap();
        wait_for("the share to return", async || {
            mgr.ed2k_shares
                .get(&hash)
                .is_some_and(|file| file.is_complete())
        })
        .await;
        mgr.update_progress().await;
        assert!(claimed());
        let full = data.len().to_string();
        {
            let tasks = mgr.tasks.read().await;
            let task = tasks.iter().find(|t| t.gid == gid).unwrap();
            assert_eq!((task.status, task.seeder), (TaskStatus::Active, true));
            assert_eq!(task.completed_length, data.len() as u64);
            assert_eq!(task.files[0].completed_length, full);
        }
        mgr.options
            .write()
            .await
            .set("keep-seeding".into(), Value::Bool(false));
        mgr.update_progress().await;
        wait_for("the share to be withdrawn at the goal", async || {
            mgr.ed2k_shares.get(&hash).is_none() && mgr.active_downloads.read().await.is_empty()
        })
        .await;
        {
            let tasks = mgr.tasks.read().await;
            let task = tasks.iter().find(|t| t.gid == gid).unwrap();
            assert_eq!((task.status, task.seeder), (TaskStatus::Complete, false));
            assert_eq!(task.completed_length, data.len() as u64);
        }
        assert!(!claimed());
        let mut completions = Vec::new();
        while let Ok(event) = events.try_recv() {
            match event {
                EngineEvent::BtDownloadComplete { seeding, .. } => {
                    completions.push(("bt", seeding))
                }
                EngineEvent::DownloadComplete { seeded, .. } => completions.push(("done", seeded)),
                _ => {}
            }
        }
        assert_eq!(completions, [("bt", true), ("done", true)]);
    }

    #[tokio::test]
    async fn resumed_ed2k_seeders_that_cannot_share() {
        let dir = tempfile::tempdir().unwrap();
        let paused_seeder = |gid: &str| {
            let mut task = DownloadTask::new_ed2k(
                gid.into(),
                "ed2k://|file|gone.bin|1000|0123456789abcdef0123456789abcdef|/".into(),
                "gone.bin".into(),
                1000,
                dir.path().to_string_lossy().into(),
                None,
                Map::new(),
            );
            task.total_length = 1000;
            task.completed_length = 1000;
            task.seeder = true;
            task.seeding_since = 1;
            task.status = TaskStatus::Paused;
            task
        };
        let mgr = make_test_manager(vec![
            paused_seeder("off"),
            paused_seeder("failed"),
            paused_seeder("missing"),
        ]);
        let mut events = mgr.events.subscribe();
        let settled = async |gid: &str, status: TaskStatus| {
            mgr.unpause(gid).await.unwrap();
            wait_for(gid, async || {
                mgr.tasks
                    .read()
                    .await
                    .iter()
                    .any(|t| t.gid == gid && t.status == status)
                    && mgr.active_downloads.read().await.is_empty()
            })
            .await;
            mgr.tasks
                .read()
                .await
                .iter()
                .find(|t| t.gid == gid)
                .cloned()
                .unwrap()
        };

        let off = settled("off", TaskStatus::Complete).await;
        assert!(!off.seeder);
        assert_eq!(off.completed_length, 1000);

        *mgr.ed2k_upload.write() = UploadRuntime::Failed("port busy".into());
        let failed = settled("failed", TaskStatus::Error).await;
        assert!(failed.seeder);
        assert!(failed.error_message.unwrap().contains("port busy"));

        install_uploader(&mgr).await;
        let missing = settled("missing", TaskStatus::Complete).await;
        assert!(!missing.seeder);

        while let Ok(event) = events.try_recv() {
            assert!(!matches!(event, EngineEvent::DownloadComplete { .. }));
        }
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
    fn usenet_success_assigns_or_clears_the_warning() {
        let mut task = DownloadTask::new_usenet(
            "ugid".into(),
            "/dl".into(),
            None,
            None,
            Map::new(),
            Vec::new(),
        );
        task.usenet_repair_failure = Some(UsenetRepairFailure {
            needed_blocks: 4,
            available_blocks: 1,
            partials_retained: true,
        });

        finish_usenet_success(&mut task, Some("cleanup-incomplete"));
        assert_eq!(task.usenet_warning.as_deref(), Some("cleanup-incomplete"));
        assert!(task.usenet_repair_failure.is_none());

        finish_usenet_success(&mut task, None);
        assert!(task.usenet_warning.is_none());
    }

    fn usenet_task_with_segments(gid: &str, status: TaskStatus) -> DownloadTask {
        let file = |name: &str, count: u32| UsenetTaskFile {
            name: name.into(),
            subject: format!("{name} yEnc"),
            groups: vec!["alt.binaries.example".into()],
            segments: (1..=count)
                .map(|number| UsenetTaskSegment {
                    number,
                    bytes: 10,
                    message_id: format!("<{name}-{number}@example>"),
                })
                .collect(),
        };
        let mut task =
            DownloadTask::new_usenet(gid.into(), "/dl".into(), None, None, Map::new(), Vec::new())
                .with_usenet_data(UsenetTaskData {
                    options: UsenetTaskOptions::default(),
                    files: vec![file("one.bin", 3), file("two.bin", 2)],
                });
        task.status = status;
        task
    }

    #[test]
    fn a_completed_usenet_task_has_no_article_list() {
        let dir = tempfile::tempdir().unwrap();
        let first = dir.path().join("one.bin");
        let second = dir.path().join("two.bin");
        std::fs::write(&first, b"a").unwrap();
        std::fs::write(&second, b"b").unwrap();
        let output_paths = HashMap::from([(0, first.clone()), (1, second.clone())]);
        let mut task = usenet_task_with_segments("ugid", TaskStatus::Active);

        let completed = complete_usenet_task(&mut task, &first, None, &output_paths);

        assert_eq!(completed, 50);
        assert_eq!(task.usenet_stage.as_deref(), Some("complete"));
        let files: Vec<(String, String, String)> = task
            .files
            .iter()
            .map(|file| {
                (
                    file.path.clone(),
                    file.length.clone(),
                    file.completed_length.clone(),
                )
            })
            .collect();
        assert_eq!(
            files,
            vec![
                (
                    first.to_string_lossy().to_string(),
                    "30".into(),
                    "30".into()
                ),
                (
                    second.to_string_lossy().to_string(),
                    "20".into(),
                    "20".into()
                ),
            ]
        );
        let usenet = task.usenet.as_ref().expect("usenet data is kept");
        let names: Vec<&str> = usenet.files.iter().map(|file| file.name.as_str()).collect();
        assert_eq!(names, ["one.bin", "two.bin"]);
        assert!(usenet.files.iter().all(|file| file.segments.is_empty()));
    }

    #[test]
    fn usenet_failure_clears_the_warning() {
        let mut task = DownloadTask::new_usenet(
            "ugid".into(),
            "/dl".into(),
            None,
            None,
            Map::new(),
            Vec::new(),
        );
        task.usenet_warning = Some("archive-cleanup-unsupported".into());

        finish_usenet_failure(&mut task, "connection refused", None);

        assert!(task.usenet_warning.is_none());
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

    fn bench_http_task(i: usize) -> DownloadTask {
        let mut options = Map::new();
        options.insert("out".into(), Value::from(format!("package-{i}.tar.gz")));
        let mut task = DownloadTask::new_http(
            format!("{i:016x}"),
            vec![format!(
                "https://downloads.example.com/releases/v{i}/package-{i}.tar.gz"
            )],
            "/home/user/Downloads".into(),
            None,
            options,
        );
        task.status = TaskStatus::Complete;
        task.total_length = 52_428_800;
        task.completed_length = 52_428_800;
        task
    }

    fn bench_usenet_task(articles: u32) -> DownloadTask {
        const PER_FILE: u32 = 1000;
        let file_count = articles.div_ceil(PER_FILE);
        let names: Vec<String> = (1..=file_count)
            .map(|n| format!("Some.Release.Name.2026.1080p.WEB.H264-GROUP.part{n:03}.rar"))
            .collect();
        let usenet_files: Vec<UsenetTaskFile> = names
            .iter()
            .enumerate()
            .map(|(f, name)| {
                let count = PER_FILE.min(articles - f as u32 * PER_FILE);
                UsenetTaskFile {
                    name: name.clone(),
                    subject: format!("[{}/{file_count}] - \"{name}\" yEnc (1/{count})", f + 1),
                    groups: vec!["alt.binaries.example".into(), "alt.binaries.test".into()],
                    segments: (1..=count)
                        .map(|number| UsenetTaskSegment {
                            number,
                            bytes: 739_574,
                            message_id: format!(
                                "<Part{number}of{count}.{:016X}{:08X}@powerpost2000AA.local>",
                                (f as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15),
                                number.wrapping_mul(0x85EB_CA6B)
                            ),
                        })
                        .collect(),
                }
            })
            .collect();
        let files = usenet_files
            .iter()
            .enumerate()
            .map(|(index, file)| DownloadFile {
                index: (index + 1).to_string(),
                path: format!("/home/user/Downloads/{}", file.name),
                length: (u64::from(PER_FILE) * 739_574).to_string(),
                completed_length: "0".into(),
                selected: "true".into(),
                uris: Vec::new(),
            })
            .collect();
        let mut task = DownloadTask::new_usenet(
            "usenet0000000000".into(),
            "/home/user/Downloads".into(),
            None,
            Some("Some.Release.Name.2026.1080p.WEB.H264-GROUP".into()),
            Map::new(),
            files,
        )
        .with_usenet_data(UsenetTaskData {
            options: UsenetTaskOptions::default(),
            files: usenet_files,
        });
        task.status = TaskStatus::Paused;
        task.usenet_stage = Some("fetching".into());
        task
    }

    fn bench_torrent_task(file_count: usize) -> DownloadTask {
        let mut task = DownloadTask::new_torrent(
            "torrent000000000".into(),
            "/home/user/Downloads".into(),
            None,
            Map::new(),
        );
        task.status = TaskStatus::Paused;
        task.info_hash = Some("0123456789abcdef0123456789abcdef01234567".into());
        task.bt_name = Some("Some.Large.Collection".into());
        task.bt_announce_list = vec![
            vec!["udp://tracker.example.org:1337/announce".into()],
            vec!["https://tracker.example.net/announce".into()],
        ];
        task.files = (0..file_count)
            .map(|i| DownloadFile {
                index: (i + 1).to_string(),
                path: format!(
                    "/home/user/Downloads/Some.Large.Collection/Disc {:02}/{:05} - Track Title Number {i}.flac",
                    i / 500 + 1,
                    i + 1
                ),
                length: "31457280".into(),
                completed_length: "0".into(),
                selected: "true".into(),
                uris: Vec::new(),
            })
            .collect();
        task
    }

    #[tokio::test]
    #[ignore = "benchmark"]
    async fn measure_what_saving_the_session_costs() {
        const SAVES: u32 = 20;
        let http = || (0..1000).map(bench_http_task).collect::<Vec<_>>();
        let cases = [
            ("1000 http", http()),
            ("1000 http + usenet 70k articles", {
                let mut tasks = http();
                tasks.push(bench_usenet_task(70_000));
                tasks
            }),
            ("1000 http + usenet 70k + torrent 10k files", {
                let mut tasks = http();
                tasks.push(bench_usenet_task(70_000));
                tasks.push(bench_torrent_task(10_000));
                tasks
            }),
        ];
        let build = if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        };
        for (label, tasks) in cases {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join(super::super::session::SESSION_FILENAME);
            let mgr = make_test_manager_in(&dir, tasks);
            mgr.tasks.write().await[0].status = TaskStatus::Active;
            mgr.save_session().await.unwrap();

            let mut saves = Vec::new();
            for _ in 0..SAVES {
                mgr.tasks.write().await[0].completed_length += 1;
                let start = std::time::Instant::now();
                mgr.save_session().await.unwrap();
                saves.push(start.elapsed());
            }
            assert_eq!(mgr.saved_rev.load(Ordering::Relaxed), mgr.tasks.rev());

            let (mut held, mut hash, mut write) = (Duration::ZERO, Duration::ZERO, Duration::ZERO);
            for _ in 0..SAVES {
                mgr.tasks.write().await[0].completed_length += 1;
                let tasks = mgr.tasks.read().await;
                let start = std::time::Instant::now();
                let data = SessionManager::encode(&tasks).unwrap();
                drop(tasks);
                held += start.elapsed();
                let start = std::time::Instant::now();
                let mut hasher = std::hash::DefaultHasher::new();
                std::hash::Hash::hash(&data, &mut hasher);
                std::hint::black_box(std::hash::Hasher::finish(&hasher));
                hash += start.elapsed();
                let start = std::time::Instant::now();
                crate::traits::write_file_atomically(&path, &data).unwrap();
                write += start.elapsed();
            }

            let ms = |d: Duration| d.as_secs_f64() * 1000.0;
            let mean = |d: Duration| ms(d) / f64::from(SAVES);
            let total: Duration = saves.iter().sum();
            saves.sort();
            println!(
                "[{build}] {label}: file {} bytes ({:.2} MB); save_session mean {:.2} ms, median {:.2} ms, min {:.2} ms, max {:.2} ms; encode under read lock mean {:.2} ms; hash mean {:.2} ms; atomic write mean {:.2} ms",
                std::fs::metadata(&path).unwrap().len(),
                std::fs::metadata(&path).unwrap().len() as f64 / 1_000_000.0,
                mean(total),
                ms(saves[saves.len() / 2]),
                ms(saves[0]),
                ms(saves[saves.len() - 1]),
                mean(held),
                mean(hash),
                mean(write),
            );
        }
    }

    fn make_test_manager_in(dir: &tempfile::TempDir, tasks: Vec<DownloadTask>) -> TaskManager {
        let mut mgr = make_test_manager(tasks);
        mgr.config_dir = dir.path().to_path_buf();
        mgr.session = Arc::new(SessionManager::new(dir.path()));
        mgr
    }

    #[tokio::test(start_paused = true)]
    async fn session_is_saved_soon_after_a_task_event() {
        let dir = tempfile::TempDir::new().unwrap();
        let mgr = Arc::new(make_test_manager_in(
            &dir,
            vec![make_task("g1", TaskStatus::Complete)],
        ));
        let (progress, auto_save) = super::super::spawn_maintenance_loops(&mgr);
        tokio::time::sleep(Duration::from_millis(10)).await;

        mgr.tasks
            .write()
            .await
            .push(make_task("g2", TaskStatus::Complete));
        mgr.events.send(EngineEvent::DownloadComplete {
            gid: "g1".into(),
            seeded: false,
        });
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(mgr.saved_rev.load(Ordering::Relaxed), mgr.tasks.rev());

        mgr.tasks
            .write()
            .await
            .push(make_task("g3", TaskStatus::Complete));
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert!(mgr.saved_rev.load(Ordering::Relaxed) < mgr.tasks.rev());

        progress.abort();
        auto_save.abort();
    }

    fn paused_options() -> Map<String, Value> {
        let mut options = Map::new();
        options.insert("pause".into(), Value::from("true"));
        options
    }

    #[tokio::test(start_paused = true)]
    async fn a_paused_add_reaches_the_disk_soon_without_an_engine_event() {
        let dir = tempfile::TempDir::new().unwrap();
        let mgr = Arc::new(make_test_manager_in(&dir, Vec::new()));
        let mut events = mgr.subscribe_events();
        let (progress, auto_save) = super::super::spawn_maintenance_loops(&mgr);
        tokio::time::sleep(Duration::from_millis(10)).await;
        let on_disk = |gid: &str| {
            SessionManager::new(dir.path())
                .load()
                .iter()
                .any(|t| t.gid == gid)
        };

        let gid = mgr
            .add_http_task(vec!["http://example.com/f.bin".into()], paused_options())
            .await
            .unwrap();
        assert!(!on_disk(&gid), "the add itself must not write the session");

        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(on_disk(&gid));
        assert!(events.try_recv().is_err());

        progress.abort();
        auto_save.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn adds_in_a_row_write_the_session_far_fewer_times_than_they_add() {
        const ADDS: usize = 30;
        let dir = tempfile::TempDir::new().unwrap();
        let mgr = Arc::new(make_test_manager_in(&dir, Vec::new()));
        let (progress, auto_save) = super::super::spawn_maintenance_loops(&mgr);
        tokio::time::sleep(Duration::from_millis(10)).await;

        let mut saved = mgr.saved_rev.load(Ordering::Relaxed);
        let mut writes = 0;
        let mut count_write = |writes: &mut usize| {
            let now = mgr.saved_rev.load(Ordering::Relaxed);
            if now != saved {
                *writes += 1;
                saved = now;
            }
        };
        for _ in 0..ADDS {
            mgr.add_http_task(vec!["http://example.com/f.bin".into()], paused_options())
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            count_write(&mut writes);
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
        count_write(&mut writes);

        assert!(
            (2..=5).contains(&writes),
            "{writes} writes for {ADDS} adds over three seconds"
        );
        assert_eq!(mgr.saved_rev.load(Ordering::Relaxed), mgr.tasks.rev());
        assert_eq!(SessionManager::new(dir.path()).load().len(), ADDS);

        progress.abort();
        auto_save.abort();
    }

    #[derive(Default)]
    struct RecordingSink(parking_lot::Mutex<Vec<(String, Value, tokio::time::Instant)>>);

    impl crate::traits::EventSink for RecordingSink {
        fn emit(&self, event: &str, payload: Value) {
            self.0
                .lock()
                .push((event.to_string(), payload, tokio::time::Instant::now()));
        }
    }

    impl RecordingSink {
        fn count(&self) -> usize {
            self.0.lock().len()
        }
    }

    async fn notified_manager(
        dir: &tempfile::TempDir,
        tasks: Vec<DownloadTask>,
    ) -> (
        Arc<TaskManager>,
        Arc<RecordingSink>,
        tokio::task::JoinHandle<()>,
    ) {
        let mgr = Arc::new(make_test_manager_in(dir, tasks));
        let sink = Arc::new(RecordingSink::default());
        let notifier = super::super::spawn_task_list_notifier(&mgr, sink.clone());
        tokio::time::sleep(Duration::from_millis(10)).await;
        (mgr, sink, notifier)
    }

    #[tokio::test(start_paused = true)]
    async fn a_paused_add_tells_the_host_the_task_list_changed() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mgr, sink, notifier) = notified_manager(&dir, Vec::new()).await;
        let mut events = mgr.subscribe_events();
        assert_eq!(sink.count(), 0);

        mgr.add_http_task(vec!["http://example.com/f.bin".into()], paused_options())
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let emitted = sink.0.lock().clone();
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].0, "engine:task-list-changed");
        assert_eq!(emitted[0].1, serde_json::json!({}));
        assert!(events.try_recv().is_err());

        notifier.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn adds_in_a_row_tell_the_host_far_fewer_times_than_they_add() {
        const ADDS: usize = 50;
        let dir = tempfile::TempDir::new().unwrap();
        let (mgr, sink, notifier) = notified_manager(&dir, Vec::new()).await;

        for i in 0..ADDS {
            if i > 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            mgr.add_http_task(vec!["http://example.com/f.bin".into()], paused_options())
                .await
                .unwrap();
        }
        let last_add = tokio::time::Instant::now();
        tokio::time::sleep(Duration::from_secs(3)).await;

        let emitted = sink.0.lock().clone();
        assert!(
            (2..=3).contains(&emitted.len()),
            "{} events for {ADDS} adds",
            emitted.len()
        );
        assert!(emitted.last().unwrap().2 >= last_add);
        assert_eq!(mgr.tasks.read().await.len(), ADDS);

        notifier.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn removing_tasks_tells_the_host_the_task_list_changed() {
        let dir = tempfile::TempDir::new().unwrap();
        let (mgr, sink, notifier) = notified_manager(
            &dir,
            vec![
                make_task("a", TaskStatus::Paused),
                make_task("b", TaskStatus::Complete),
                make_task("c", TaskStatus::Error),
            ],
        )
        .await;

        mgr.remove("a").await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(sink.count(), 1);

        tokio::time::sleep(Duration::from_secs(2)).await;
        mgr.remove_download_result("b").await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(sink.count(), 2);

        tokio::time::sleep(Duration::from_secs(2)).await;
        mgr.purge_download_result().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(sink.count(), 3);
        assert!(mgr.tasks.read().await.is_empty());

        tokio::time::sleep(Duration::from_secs(2)).await;
        mgr.purge_download_result().await;
        assert!(mgr.remove("missing").await.is_err());
        assert!(mgr.remove_download_result("missing").await.is_err());
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(sink.count(), 3);

        notifier.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn the_cap_on_finished_tasks_tells_the_host_when_it_evicts() {
        let dir = tempfile::TempDir::new().unwrap();
        let finished = (0..=TaskManager::MAX_STOPPED_RESULTS)
            .map(|i| make_task(&format!("done-{i}"), TaskStatus::Complete))
            .collect();
        let (mgr, sink, notifier) = notified_manager(&dir, finished).await;

        mgr.enforce_result_cap().await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(sink.count(), 1);
        assert_eq!(
            mgr.tasks.read().await.len(),
            TaskManager::MAX_STOPPED_RESULTS
        );

        tokio::time::sleep(Duration::from_secs(2)).await;
        mgr.enforce_result_cap().await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!(sink.count(), 1);

        notifier.abort();
    }

    async fn limit_downloads_to(mgr: &TaskManager, max: usize) {
        let mut opts = Map::new();
        opts.insert(
            "max-concurrent-downloads".into(),
            Value::String(max.to_string()),
        );
        mgr.change_global_option(opts).await;
    }

    fn queued_task(gid: &str, status: TaskStatus) -> DownloadTask {
        let mut task = make_task(gid, status);
        task.uris = vec!["http://127.0.0.1:1/x".into()];
        task
    }

    async fn park_fake_worker(mgr: &TaskManager, gid: &str) -> CancellationToken {
        let worker = fake_worker(next_worker_epoch());
        let token = worker.cancel_token.clone();
        mgr.active_downloads
            .write()
            .await
            .insert(gid.into(), worker);
        token
    }

    async fn spawn_gated_worker(
        mgr: &TaskManager,
        gid: &str,
    ) -> (CancellationToken, tokio::sync::oneshot::Sender<()>) {
        let (release, released) = tokio::sync::oneshot::channel();
        let token = spawn_mock_worker(mgr, gid, async move {
            released.await.ok();
        })
        .await;
        (token, release)
    }

    async fn wait_for_workers_to_exit(mgr: &TaskManager) {
        wait_for("the workers to exit", async || {
            mgr.active_downloads.read().await.is_empty()
        })
        .await;
    }

    fn queue_outcome(
        mgr: &TaskManager,
        events: &mut tokio::sync::broadcast::Receiver<EngineEvent>,
    ) -> (Vec<String>, Vec<TaskStatus>) {
        cancel_all_starting_workers(&mgr.starting_workers);
        let started = std::iter::from_fn(|| events.try_recv().ok())
            .filter_map(|event| match event {
                EngineEvent::DownloadStart { gid } => Some(gid),
                _ => None,
            })
            .collect();
        // read without awaiting so no spawned worker can run before the snapshot
        let statuses = mgr
            .tasks
            .inner
            .try_read()
            .unwrap()
            .iter()
            .map(|task| task.status)
            .collect();
        (started, statuses)
    }

    #[tokio::test]
    async fn one_free_slot_starts_only_the_first_waiting_task() {
        let mgr = make_test_manager(vec![
            queued_task("a", TaskStatus::Waiting),
            queued_task("b", TaskStatus::Waiting),
        ]);
        limit_downloads_to(&mgr, 1).await;
        let mut events = mgr.events.subscribe();

        mgr.try_start_next().await;

        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert_eq!(started, ["a"]);
        assert_eq!(statuses, [TaskStatus::Active, TaskStatus::Waiting]);
        assert!(mgr.starting_workers.lock().contains_key("a"));
    }

    #[tokio::test]
    async fn a_running_download_in_the_only_slot_keeps_the_next_one_waiting() {
        let mgr = make_test_manager(vec![
            queued_task("run", TaskStatus::Active),
            queued_task("next", TaskStatus::Waiting),
        ]);
        park_fake_worker(&mgr, "run").await;
        limit_downloads_to(&mgr, 1).await;
        let mut events = mgr.events.subscribe();

        mgr.try_start_next().await;

        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert!(started.is_empty());
        assert_eq!(statuses, [TaskStatus::Active, TaskStatus::Waiting]);
    }

    #[tokio::test]
    async fn a_seeding_worker_does_not_take_a_download_slot() {
        let dir = tempfile::tempdir().unwrap();
        let mut seeding = ed2k_task_in(dir.path(), "seed", TaskStatus::Active);
        seeding.seeder = true;
        let mgr = make_test_manager(vec![seeding, queued_task("next", TaskStatus::Waiting)]);
        park_fake_worker(&mgr, "seed").await;
        limit_downloads_to(&mgr, 1).await;
        let mut events = mgr.events.subscribe();

        mgr.try_start_next().await;

        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert_eq!(started, ["next"]);
        assert_eq!(statuses, [TaskStatus::Active, TaskStatus::Active]);
    }

    #[tokio::test]
    async fn a_pushed_back_task_keeps_its_slot_until_its_worker_exits() {
        let mgr = make_test_manager(vec![
            queued_task("stopping", TaskStatus::Waiting),
            queued_task("next", TaskStatus::Waiting),
        ]);
        park_fake_worker(&mgr, "stopping").await;
        limit_downloads_to(&mgr, 1).await;
        let mut events = mgr.events.subscribe();

        mgr.try_start_next().await;
        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert!(started.is_empty());
        assert_eq!(statuses, [TaskStatus::Waiting, TaskStatus::Waiting]);

        mgr.active_downloads.write().await.remove("stopping");
        mgr.try_start_next().await;
        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert_eq!(started, ["stopping"]);
        assert_eq!(statuses, [TaskStatus::Active, TaskStatus::Waiting]);
    }

    #[tokio::test]
    async fn lowering_the_limit_pushes_back_the_last_running_task_on_the_next_tick() {
        let mgr = make_test_manager(vec![
            queued_task("first", TaskStatus::Active),
            queued_task("second", TaskStatus::Active),
        ]);
        let first = park_fake_worker(&mgr, "first").await;
        let second = park_fake_worker(&mgr, "second").await;
        limit_downloads_to(&mgr, 2).await;
        let mut events = mgr.events.subscribe();

        limit_downloads_to(&mgr, 1).await;
        let (_, statuses) = queue_outcome(&mgr, &mut events);
        assert_eq!(statuses, [TaskStatus::Active, TaskStatus::Active]);

        mgr.update_progress().await;
        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert!(started.is_empty());
        assert_eq!(statuses, [TaskStatus::Active, TaskStatus::Waiting]);
        assert!(!first.is_cancelled());
        assert!(second.is_cancelled());
    }

    #[tokio::test]
    async fn start_task_now_turns_down_a_waiting_task() {
        let mgr = make_test_manager(vec![queued_task("w", TaskStatus::Waiting)]);

        assert!(mgr.start_task_now("w").await.is_err());
        assert_eq!(mgr.tasks.read().await[0].status, TaskStatus::Waiting);
    }

    #[tokio::test]
    async fn starting_a_scheduled_task_now_pushes_back_a_running_task_listed_after_it() {
        let mut scheduled = queued_task("sched", TaskStatus::Scheduled);
        scheduled.start_at = Some(crate::engine::util::now_secs() + 3600);
        let mgr = make_test_manager(vec![scheduled, queued_task("run", TaskStatus::Active)]);
        let (run, release) = spawn_gated_worker(&mgr, "run").await;
        limit_downloads_to(&mgr, 1).await;
        let mut events = mgr.events.subscribe();

        mgr.start_task_now("sched").await.unwrap();
        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert!(started.is_empty());
        assert_eq!(statuses, [TaskStatus::Waiting, TaskStatus::Waiting]);
        assert!(run.is_cancelled());

        release.send(()).unwrap();
        wait_for_workers_to_exit(&mgr).await;
        mgr.update_progress().await;
        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert_eq!(started, ["sched"]);
        assert_eq!(statuses, [TaskStatus::Active, TaskStatus::Waiting]);
    }

    #[tokio::test]
    async fn starting_a_scheduled_task_now_behind_a_running_one_waits_for_a_free_slot() {
        let mut scheduled = queued_task("sched", TaskStatus::Scheduled);
        scheduled.start_at = Some(crate::engine::util::now_secs() + 3600);
        let mgr = make_test_manager(vec![queued_task("run", TaskStatus::Active), scheduled]);
        let run = park_fake_worker(&mgr, "run").await;
        limit_downloads_to(&mgr, 1).await;
        let mut events = mgr.events.subscribe();

        mgr.start_task_now("sched").await.unwrap();

        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert!(started.is_empty());
        assert_eq!(statuses, [TaskStatus::Active, TaskStatus::Waiting]);
        assert!(!run.is_cancelled());
    }

    #[tokio::test]
    async fn a_task_being_updated_is_skipped_and_the_slot_goes_to_the_next() {
        let mgr = make_test_manager(vec![
            queued_task("updating", TaskStatus::Waiting),
            queued_task("next", TaskStatus::Waiting),
        ]);
        mgr.updating_tasks.lock().insert("updating".into());
        limit_downloads_to(&mgr, 1).await;
        let mut events = mgr.events.subscribe();

        mgr.try_start_next().await;

        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert_eq!(started, ["next"]);
        assert_eq!(statuses, [TaskStatus::Waiting, TaskStatus::Active]);
    }

    #[tokio::test]
    async fn a_path_edit_keeps_the_queue_from_starting_the_task_until_the_move_is_done() {
        let root = tempfile::TempDir::new().unwrap();
        let old_dir = root.path().join("old");
        let new_dir = root.path().join("new");
        std::fs::create_dir_all(&old_dir).unwrap();
        std::fs::create_dir_all(&new_dir).unwrap();
        std::fs::write(old_dir.join("file.bin.part"), b"partial").unwrap();
        let mut task = DownloadTask::new_http(
            "edit".into(),
            vec!["http://127.0.0.1:1/file.bin".into()],
            old_dir.to_string_lossy().into_owned(),
            None,
            Map::new(),
        );
        task.out = "file.bin".into();
        task.status = TaskStatus::Waiting;
        let mgr = Arc::new(make_test_manager(vec![task]));
        let (seen, status_in_window) = tokio::sync::oneshot::channel();
        let queue = mgr.clone();
        spawn_mock_worker(&mgr, "edit", async move {
            queue.active_downloads.write().await.remove("edit");
            queue.try_start_next().await;
            cancel_all_starting_workers(&queue.starting_workers);
            let tasks = queue.tasks.read().await;
            seen.send(tasks[0].status).ok();
        })
        .await;
        let mut events = mgr.events.subscribe();

        let outcome = mgr
            .update_task(
                "edit",
                TaskPatch {
                    dir: Some(new_dir.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )
            .await
            .expect("relocate");

        assert_eq!(status_in_window.await.unwrap(), TaskStatus::Waiting);
        assert!(outcome.progress_preserved);
        assert_eq!(
            std::fs::read(new_dir.join("file.bin.part")).unwrap(),
            b"partial"
        );
        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert_eq!(started, ["edit"]);
        assert_eq!(statuses, [TaskStatus::Active]);
        assert!(mgr.updating_tasks.lock().is_empty());
    }

    #[tokio::test]
    async fn a_paused_download_frees_its_slot_only_after_its_worker_exits() {
        let mgr = make_test_manager(vec![
            queued_task("run", TaskStatus::Active),
            queued_task("next", TaskStatus::Waiting),
        ]);
        let (run, release) = spawn_gated_worker(&mgr, "run").await;
        limit_downloads_to(&mgr, 1).await;
        let mut events = mgr.events.subscribe();

        mgr.pause("run").await.unwrap();
        mgr.update_progress().await;
        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert!(run.is_cancelled());
        assert!(started.is_empty());
        assert_eq!(statuses, [TaskStatus::Paused, TaskStatus::Waiting]);

        release.send(()).unwrap();
        wait_for_workers_to_exit(&mgr).await;
        mgr.update_progress().await;
        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert_eq!(started, ["next"]);
        assert_eq!(statuses, [TaskStatus::Paused, TaskStatus::Active]);
    }

    #[tokio::test]
    async fn a_waiting_task_the_queue_cannot_start_does_not_push_back_a_running_one() {
        let mut stuck = queued_task("stuck", TaskStatus::Waiting);
        stuck.uris.clear();
        let mgr = make_test_manager(vec![stuck, queued_task("run", TaskStatus::Active)]);
        let run = park_fake_worker(&mgr, "run").await;
        limit_downloads_to(&mgr, 1).await;
        let mut events = mgr.events.subscribe();

        mgr.update_progress().await;
        let (started, statuses) = queue_outcome(&mgr, &mut events);
        assert!(started.is_empty());
        assert_eq!(statuses, [TaskStatus::Waiting, TaskStatus::Active]);
        assert!(!run.is_cancelled());
    }

    #[tokio::test]
    async fn a_torrent_evicted_by_the_cap_loses_its_saved_torrent_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let hash = "aa".repeat(20);
        let mut oldest = make_torrent_task("oldest", TaskStatus::Complete);
        oldest.info_hash = Some(hash.clone());
        let finished = std::iter::once(oldest)
            .chain(
                (1..=TaskManager::MAX_STOPPED_RESULTS)
                    .map(|i| make_task(&format!("done-{i}"), TaskStatus::Complete)),
            )
            .collect();
        let saved = dir.path().join("torrents").join(format!("{hash}.torrent"));
        std::fs::create_dir_all(saved.parent().unwrap()).unwrap();
        std::fs::write(&saved, b"d4:infod4:name4:testee").unwrap();
        let mgr = make_test_manager_in(&dir, finished);

        mgr.enforce_result_cap().await;

        assert!(mgr.tasks.read().await.iter().all(|t| t.gid != "oldest"));
        assert!(!saved.exists());
    }

    #[tokio::test]
    async fn a_failed_ed2k_task_evicted_by_the_cap_loses_its_partial() {
        let dir = tempfile::TempDir::new().unwrap();
        let downloads = dir.path().join("downloads");
        std::fs::create_dir_all(&downloads).unwrap();
        let fin = downloads.join("f.bin");
        write_ed2k_partial(&fin, ED2K_HASH);
        std::fs::write(&fin, b"keep").unwrap();
        let finished = std::iter::once(ed2k_task_in(&downloads, "failed", TaskStatus::Error))
            .chain(
                (1..=TaskManager::MAX_STOPPED_RESULTS)
                    .map(|i| make_task(&format!("done-{i}"), TaskStatus::Complete)),
            )
            .collect();
        let mgr = make_test_manager_in(&dir, finished);

        mgr.enforce_result_cap().await;

        assert!(mgr.tasks.read().await.iter().all(|t| t.gid != "failed"));
        assert!(!downloads.join("f.bin.part").exists());
        assert!(!downloads.join("f.bin.part.met").exists());
        assert!(fin.exists());
    }

    #[test]
    fn upload_total_adds_the_saved_total_and_carries_it_over_counter_restarts() {
        let (mut base, mut last) = (900, 0);
        assert_eq!(upload_total(&mut base, &mut last, 0), 900);
        assert_eq!(upload_total(&mut base, &mut last, 150), 1050);
        assert_eq!(upload_total(&mut base, &mut last, 20), 1070);
        assert_eq!(upload_total(&mut base, &mut last, 60), 1110);
    }

    #[test]
    fn a_torrent_added_again_under_a_new_id_keeps_its_upload_total() {
        let mut task = make_torrent_task("t", TaskStatus::Active);
        task.upload_length = 900;
        let mgr = make_test_manager(Vec::new());
        task.upload_length = mgr.torrent_upload_total(&task, 1, 150);
        assert_eq!(task.upload_length, 1050);
        task.upload_length = mgr.torrent_upload_total(&task, 2, 700);
        assert_eq!(task.upload_length, 1750);
        task.upload_length = mgr.torrent_upload_total(&task, 2, 760);
        assert_eq!(task.upload_length, 1810);
    }

    #[tokio::test]
    async fn a_restored_torrent_reports_its_saved_upload_total() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        let (gid, _pack) = add_complete_torrent(&mgr, dir.path()).await;
        mgr.tasks
            .write()
            .await
            .iter_mut()
            .find(|t| t.gid == gid)
            .unwrap()
            .upload_length = 900;
        let (te, tid) = mgr.torrent_target(&gid).await.unwrap();
        assert_eq!(te.get_torrent_stats(tid).unwrap().uploaded_bytes, 0);

        mgr.update_progress().await;

        let tasks = mgr.tasks.read().await;
        let task = tasks.iter().find(|t| t.gid == gid).unwrap();
        assert!(task.upload_length >= 900);
    }

    async fn switch_seeding(mgr: &TaskManager, on: bool) {
        let mut opts = Map::new();
        opts.insert("keep-seeding".into(), Value::String(on.to_string()));
        opts.insert("seed-time".into(), Value::String("0".into()));
        opts.insert("seed-ratio".into(), Value::String("0".into()));
        mgr.change_global_option(opts).await;
    }

    async fn tick_until_seeding(mgr: &TaskManager, gid: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !mgr
            .tasks
            .read()
            .await
            .iter()
            .any(|t| t.gid == gid && t.seeder)
        {
            assert!(std::time::Instant::now() < deadline, "never seeded");
            mgr.update_progress().await;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    async fn seed_complete_torrent(mgr: &TaskManager, dir: &Path, rule: &[(&str, &str)]) -> String {
        let (gid, _) = add_complete_torrent(mgr, dir).await;
        {
            let mut tasks = mgr.tasks.write().await;
            let task = tasks.iter_mut().find(|t| t.gid == gid).unwrap();
            for key in ["keep-seeding", "seed-time", "seed-ratio"] {
                task.options.remove(key);
            }
            for (key, value) in rule {
                task.options
                    .insert((*key).into(), Value::String((*value).into()));
            }
        }
        tick_until_seeding(mgr, &gid).await;
        gid
    }

    async fn seeding_state(mgr: &TaskManager, gid: &str) -> (TaskStatus, bool) {
        let tasks = mgr.tasks.read().await;
        let task = tasks.iter().find(|t| t.gid == gid).unwrap();
        (task.status, task.seeder)
    }

    async fn resume_after_switching_seeding_off(
        mgr: &TaskManager,
        gid: &str,
    ) -> (TaskStatus, bool) {
        mgr.pause(gid).await.unwrap();
        switch_seeding(mgr, false).await;
        mgr.unpause(gid).await.unwrap();
        mgr.update_progress().await;
        seeding_state(mgr, gid).await
    }

    #[tokio::test]
    async fn a_torrent_seeder_stops_when_its_rule_turns_seeding_off() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        switch_seeding(&mgr, true).await;
        let gid = seed_complete_torrent(&mgr, dir.path(), &[]).await;
        let mut events = mgr.events.subscribe();

        let mut rule = Map::new();
        for (key, value) in [
            ("keep-seeding", "false"),
            ("seed-time", "0"),
            ("seed-ratio", "0"),
        ] {
            rule.insert(key.into(), Value::String(value.into()));
        }
        mgr.update_task(
            &gid,
            TaskPatch {
                options: Some(rule),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        mgr.update_progress().await;

        assert_eq!(
            seeding_state(&mgr, &gid).await,
            (TaskStatus::Complete, false)
        );
        let mut completed = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let EngineEvent::DownloadComplete { gid, seeded } = event {
                completed.push((gid, seeded));
            }
        }
        assert_eq!(completed, [(gid, true)]);
    }

    #[tokio::test]
    async fn a_resumed_torrent_seeder_stops_when_seeding_is_switched_off() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        switch_seeding(&mgr, true).await;
        let gid = seed_complete_torrent(&mgr, dir.path(), &[]).await;

        assert_eq!(
            resume_after_switching_seeding_off(&mgr, &gid).await,
            (TaskStatus::Complete, false)
        );
    }

    #[tokio::test]
    async fn a_torrent_with_its_own_rule_keeps_seeding_when_seeding_is_switched_off() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        switch_seeding(&mgr, true).await;
        let rule = [
            ("keep-seeding", "true"),
            ("seed-time", "0"),
            ("seed-ratio", "0"),
        ];
        let gid = seed_complete_torrent(&mgr, dir.path(), &rule).await;

        assert_eq!(
            resume_after_switching_seeding_off(&mgr, &gid).await,
            (TaskStatus::Active, true)
        );
    }

    #[tokio::test]
    async fn a_torrent_seeded_in_place_keeps_seeding_when_seeding_is_switched_off() {
        use risuko_bt::create::{create_torrent, CreateOptions};
        let (mgr, dir) = make_test_manager_with_engine().await;
        switch_seeding(&mgr, true).await;
        let source = dir.path().join("source");
        std::fs::create_dir_all(&source).unwrap();
        let file = source.join("seeded.bin");
        std::fs::write(&file, "seeded".repeat(5000)).unwrap();
        let created = create_torrent(
            &file,
            &CreateOptions::default(),
            |_, _| {},
            &std::sync::atomic::AtomicBool::new(false),
        )
        .unwrap();
        let gid = mgr
            .seed_created_torrent(created.bytes, &source.to_string_lossy(), created.stamps)
            .await
            .unwrap();
        tick_until_seeding(&mgr, &gid).await;

        assert_eq!(
            resume_after_switching_seeding_off(&mgr, &gid).await,
            (TaskStatus::Active, true)
        );
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

    fn part_in_dl(name: &str) -> Value {
        Value::String(Path::new("/dl").join(name).to_string_lossy().into_owned())
    }

    #[tokio::test]
    async fn only_other_unfinished_downloads_of_another_file_reserve_their_part() {
        let with_progress = |gid: &str, status, uri: &str| {
            let mut task = make_task(gid, status);
            task.uris = vec![uri.into()];
            task.completed_length = 10;
            task
        };
        let mine = "http://example.com/f.bin?id=2";
        let mgr = make_test_manager(vec![
            with_progress(
                "other-host",
                TaskStatus::Paused,
                "http://elsewhere.example/f.bin",
            ),
            with_progress(
                "other-query",
                TaskStatus::Paused,
                "http://example.com/f.bin?id=1",
            ),
            with_progress(
                "restarted-from",
                TaskStatus::Error,
                "http://example.com/f.bin?id=2#x",
            ),
            with_progress("done", TaskStatus::Complete, "http://third.example/f.bin"),
            make_task("never-started", TaskStatus::Paused),
            with_progress("me", TaskStatus::Active, mine),
        ]);

        let reserved = reserved_part_paths(&mgr.tasks, "me", Some(mine)).await;
        assert_eq!(reserved, vec![part_in_dl("f.bin.part"); 2]);
        mgr.tasks
            .write()
            .await
            .retain(|task| !task.gid.starts_with("other-"));
        assert!(reserved_part_paths(&mgr.tasks, "me", Some(mine))
            .await
            .is_empty());
    }

    fn legacy_task(
        gid: &str,
        kind: TaskKind,
        uri: &str,
        dir: &str,
        status: TaskStatus,
    ) -> DownloadTask {
        let mut task = DownloadTask::new_simple_protocol(
            gid.into(),
            kind,
            uri.into(),
            String::new(),
            10,
            dir.into(),
            None,
            Map::new(),
        );
        task.status = status;
        task
    }

    #[tokio::test]
    async fn partials_of_other_protocols_are_reserved_too() {
        let ed2k = |gid: &str, name: &str, status| {
            let mut task = DownloadTask::new_ed2k(
                gid.into(),
                format!("ed2k://|file|{name}|1000|{ED2K_HASH}|/"),
                String::new(),
                1000,
                "/dl".into(),
                None,
                Map::new(),
            );
            task.status = status;
            task
        };
        let gnutella = "gnutella://h:6346/?urn=urn:sha1:ABCDEF&dn=bar.bin&xl=10";
        let g2 = "g2://h:6346/sha1/ABCDEF?xl=10&dn=foo.bin";
        let gone = "gnutella://h:6346/?urn=urn:sha1:ABCDEF&dn=gone.bin&xl=10";
        let mine = "http://example.com/x.iso";
        let mut me = make_task("me", TaskStatus::Active);
        me.uris = vec![mine.into()];
        let mgr = make_test_manager(vec![
            ed2k("ed2k", "x.iso", TaskStatus::Paused),
            legacy_task(
                "gnutella",
                TaskKind::Gnutella,
                gnutella,
                "/dl",
                TaskStatus::Paused,
            ),
            legacy_task("g2", TaskKind::G2, g2, "/dl", TaskStatus::Waiting),
            ed2k("ed2k-done", "done.iso", TaskStatus::Complete),
            legacy_task("gone", TaskKind::Gnutella, gone, "/dl", TaskStatus::Removed),
            me,
        ]);

        let partial = |uri: &str| {
            let path = super::super::gnutella::download::partial_path_for_uri(uri, "/dl").unwrap();
            Value::String(path.to_string_lossy().into_owned())
        };
        let reserved = reserved_part_paths(&mgr.tasks, "me", Some(mine)).await;
        assert_eq!(
            reserved,
            vec![part_in_dl("x.iso.part"), partial(gnutella), partial(g2)]
        );
    }

    #[tokio::test]
    async fn a_legacy_download_refuses_a_partial_that_another_task_owns() {
        let dir = tempfile::TempDir::new().unwrap();
        let dl = dir.path().to_string_lossy().into_owned();
        let part = dir.path().join("bar.bin.part");
        let uri = "gnutella://h:6346/?urn=urn:sha1:ABCDEF&dn=bar.bin&xl=10";
        let mut owner = make_task("owner", TaskStatus::Paused);
        owner.uris = vec!["http://example.com/bar.bin".into()];
        owner.dir = dl.clone();
        owner.completed_length = 10;
        let mgr = make_test_manager(vec![
            owner,
            legacy_task("gn", TaskKind::Gnutella, uri, &dl, TaskStatus::Active),
        ]);

        assert_eq!(foreign_partial(&mgr.tasks, "gn", uri, &dl).await, None);
        std::fs::write(&part, b"theirs").unwrap();
        assert_eq!(
            foreign_partial(&mgr.tasks, "gn", uri, &dl).await,
            Some(part.clone())
        );
        mgr.tasks.write().await[0].status = TaskStatus::Complete;
        assert_eq!(foreign_partial(&mgr.tasks, "gn", uri, &dl).await, None);
    }

    #[test]
    fn only_a_folder_that_differs_from_the_default_counts_as_picked() {
        let with_dir = |dir: &str| {
            let mut options = Map::new();
            options.insert("dir".into(), Value::String(dir.into()));
            options
        };
        assert!(picks_own_dir(&with_dir("/media/films"), "/dl"));
        assert!(!picks_own_dir(&with_dir("/dl"), "/dl"));
        assert!(!picks_own_dir(&with_dir("/dl/"), "/dl"));
        assert!(!picks_own_dir(&with_dir("  "), "/dl"));
        assert!(!picks_own_dir(&Map::new(), "/dl"));
    }

    #[tokio::test]
    async fn a_picked_folder_wins_over_routing_rules_but_keeps_the_tag() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        let default_dir = dir.path().join("default").to_string_lossy().into_owned();
        let routed_dir = dir.path().join("routed").to_string_lossy().into_owned();
        let picked_dir = dir.path().join("picked").to_string_lossy().into_owned();
        {
            let mut options = mgr.options.write().await;
            options
                .global
                .insert("dir".into(), Value::String(default_dir.clone()));
            options.global.insert(
                "task-routing-rules".into(),
                serde_json::json!([{
                    "id": "all",
                    "label": "Everything",
                    "pattern": "*",
                    "dir": routed_dir,
                }]),
            );
        }
        let with_dir = |dir: &str| {
            let mut options = Map::new();
            options.insert("dir".into(), Value::String(dir.into()));
            options
        };

        let picked = mgr
            .resolve_routing_for_task(&with_dir(&picked_dir), "film.mkv")
            .await;
        assert_eq!(picked, (picked_dir, Some("Everything".to_string())));
        let untouched = mgr
            .resolve_routing_for_task(&with_dir(&default_dir), "film.mkv")
            .await;
        assert_eq!(untouched.0, routed_dir);
        let unset = mgr.resolve_routing_for_task(&Map::new(), "film.mkv").await;
        assert_eq!(unset.0, routed_dir);
    }

    #[test]
    fn a_metalink_file_with_only_file_transfer_mirrors_uses_the_first_one() {
        let ftp_only = vec!["sftp://a/f.iso".to_string(), "ftp://b/f.iso".to_string()];
        assert_eq!(file_transfer_mirror(&ftp_only), Some("sftp://a/f.iso"));
        let web = vec!["https://a/f.iso".to_string()];
        assert_eq!(file_transfer_mirror(&web), None);
        assert_eq!(file_transfer_mirror(&[]), None);
    }

    #[tokio::test]
    async fn an_ftp_download_reserves_the_part_its_own_worker_would_write() {
        let mut paused = make_task("ftp", TaskStatus::Paused);
        paused.kind = TaskKind::Ftp;
        paused.uris = vec!["sftp://host/pub/My%20File.iso".into()];
        paused.completed_length = 10;
        let mut renamed = paused.clone();
        renamed.gid = "ftp-renamed".into();
        renamed.out = "My File.1.iso".into();
        let mgr = make_test_manager(vec![paused, renamed]);

        let reserved = reserved_part_paths(&mgr.tasks, "me", Some("ftp://other/x.iso")).await;
        assert_eq!(
            reserved,
            vec![
                part_in_dl("My File.iso.part"),
                part_in_dl("My File.1.iso.part"),
            ]
        );
        assert!(
            reserved_part_paths(&mgr.tasks, "ftp", Some("sftp://host/pub/My%20File.iso"))
                .await
                .is_empty()
        );
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
    async fn unpause_clears_the_usenet_warning() {
        let mut paused = make_torrent_task("t", TaskStatus::Paused);
        paused.usenet_warning = Some("cleanup-incomplete".into());
        let mut other = make_torrent_task("u", TaskStatus::Paused);
        other.usenet_warning = Some("cleanup-incomplete".into());
        let mgr = make_test_manager(vec![paused, other]);

        mgr.unpause("t").await.unwrap();
        assert!(mgr.tasks.read().await[0].usenet_warning.is_none());

        mgr.unpause_all().await;
        assert!(mgr.tasks.read().await[1].usenet_warning.is_none());
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

    fn task_with_options(gid: &str, status: TaskStatus) -> DownloadTask {
        let mut task = make_task(gid, status);
        task.options
            .insert("cookie".into(), Value::String("session=1".into()));
        task.options
            .insert("user-agent".into(), Value::String("Agent/1".into()));
        task.options
            .insert("split".into(), Value::String("8".into()));
        task.error_code = Some("403".into());
        task.error_message = Some("Forbidden".into());
        task
    }

    fn removed_torrent(gid: &str, info_hash: Option<&str>) -> DownloadTask {
        let mut task = DownloadTask::new_torrent(gid.into(), "/dl".into(), None, Map::new());
        task.status = TaskStatus::Removed;
        task.info_hash = info_hash.map(str::to_string);
        task.error_code = Some("1".into());
        task.error_message = Some("Tracker failed".into());
        task
    }

    #[tokio::test]
    async fn restart_task_resumes_a_failed_task_with_its_options() {
        let task = task_with_options("f", TaskStatus::Error);
        let options = task.options.clone();
        let mgr = make_test_manager(vec![task]);
        mgr.shutting_down.store(true, Ordering::Release);

        mgr.restart_task("f").await.expect("restart");

        let tasks = mgr.tasks.read().await;
        let t = &tasks[0];
        assert!(matches!(t.status, TaskStatus::Waiting | TaskStatus::Active));
        assert!(t.error_code.is_none());
        assert!(t.error_message.is_none());
        assert_eq!(t.options, options);
    }

    #[tokio::test]
    async fn restart_task_revives_a_removed_task() {
        let mut task = task_with_options("r", TaskStatus::Removed);
        task.download_speed = 512;
        task.upload_speed = 64;
        let (uris, options) = (task.uris.clone(), task.options.clone());
        let mgr = make_test_manager(vec![task]);
        mgr.shutting_down.store(true, Ordering::Release);

        mgr.restart_task("r").await.expect("restart");

        let tasks = mgr.tasks.read().await;
        let t = &tasks[0];
        assert!(matches!(t.status, TaskStatus::Waiting | TaskStatus::Active));
        assert!(t.error_code.is_none());
        assert!(t.error_message.is_none());
        assert_eq!(t.download_speed, 0);
        assert_eq!(t.upload_speed, 0);
        assert_eq!(t.uris, uris);
        assert_eq!(t.options, options);
    }

    #[tokio::test]
    async fn restart_task_refuses_tasks_that_are_not_failed_or_removed() {
        let statuses = [
            ("c", TaskStatus::Complete),
            ("a", TaskStatus::Active),
            ("p", TaskStatus::Paused),
            ("w", TaskStatus::Waiting),
        ];
        let mgr = make_test_manager(
            statuses
                .iter()
                .map(|(gid, status)| make_task(gid, *status))
                .collect(),
        );
        mgr.shutting_down.store(true, Ordering::Release);

        for (gid, _) in &statuses {
            assert!(mgr.restart_task(gid).await.is_err(), "{gid}");
        }
        assert!(mgr.restart_task("missing").await.is_err());
        let complete = mgr.restart_task("c").await.unwrap_err();
        assert!(complete.contains("complete"), "{complete}");

        let tasks = mgr.tasks.read().await;
        for (task, (_, status)) in tasks.iter().zip(&statuses) {
            assert_eq!(task.status, *status);
        }
    }

    #[tokio::test]
    async fn restart_task_refuses_a_removed_usenet_task_without_articles() {
        let mut completed = usenet_task_with_segments("done", TaskStatus::Active);
        complete_usenet_task(
            &mut completed,
            Path::new("/dl/one.bin"),
            None,
            &HashMap::new(),
        );
        completed.status = TaskStatus::Complete;
        let interrupted = usenet_task_with_segments("cut", TaskStatus::Paused);
        let mgr = make_test_manager(vec![completed, interrupted]);
        mgr.shutting_down.store(true, Ordering::Release);
        mgr.remove("done").await.unwrap();
        mgr.remove("cut").await.unwrap();

        let err = mgr.restart_task("done").await.unwrap_err();
        assert!(err.contains("article list is gone"), "{err}");
        {
            let tasks = mgr.tasks.read().await;
            assert_eq!(tasks[0].status, TaskStatus::Removed);
        }

        mgr.restart_task("cut").await.expect("restart");
        let tasks = mgr.tasks.read().await;
        assert!(matches!(
            tasks[1].status,
            TaskStatus::Waiting | TaskStatus::Active
        ));
        assert!(!tasks[1].usenet_articles_dropped());
    }

    #[tokio::test]
    async fn restart_task_revives_a_removed_torrent_task() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let hash = "cab507494d02ebb1178b38f2e9d7be299c86b862";
        let magnet = format!("magnet:?xt=urn:btih:{hash}&dn=Example");
        let mut task = removed_torrent("bt", Some(hash));
        task.uris = vec![magnet.clone()];
        task.options
            .insert("select-file".into(), Value::String("1,3".into()));
        let options = task.options.clone();
        mgr.tasks.write().await.push(task);

        mgr.restart_task("bt").await.expect("restart");

        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "bt").unwrap();
        assert_eq!(t.status, TaskStatus::Active);
        assert!(t.error_code.is_none());
        assert!(t.error_message.is_none());
        assert_eq!(t.uris, vec![magnet]);
        assert_eq!(t.options, options);
        drop(tasks);
        assert_eq!(mgr.torrent_resume_seq.load(Ordering::SeqCst), 1);
        assert!(mgr.pending_magnets.read().await.contains("bt"));
    }

    #[tokio::test]
    async fn restart_task_rebuilds_a_magnet_for_a_removed_file_torrent() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let hash = "cab507494d02ebb1178b38f2e9d7be299c86b862";
        let mut task = removed_torrent("bt", Some(hash));
        task.bt_name = Some("My Show & Extras".into());
        task.bt_announce_list = vec![
            vec!["udp://tracker.example:6969/announce".into()],
            vec!["https://t.example/announce?passkey=a&b=c".into()],
        ];
        mgr.tasks.write().await.push(task);

        mgr.restart_task("bt").await.expect("restart");

        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "bt").unwrap();
        assert_eq!(t.status, TaskStatus::Active);
        assert_eq!(t.uris.len(), 1);
        let uri = &t.uris[0];
        assert!(
            uri.starts_with(&format!("magnet:?xt=urn:btih:{hash}")),
            "{uri}"
        );
        assert!(uri.contains("&dn=My%20Show%20%26%20Extras"), "{uri}");
        assert!(
            uri.contains("&tr=udp%3A%2F%2Ftracker.example%3A6969%2Fannounce"),
            "{uri}"
        );
        assert!(
            uri.contains("&tr=https%3A%2F%2Ft.example%2Fannounce%3Fpasskey%3Da%26b%3Dc"),
            "{uri}"
        );
        drop(tasks);
        assert!(mgr.pending_magnets.read().await.contains("bt"));
    }

    #[tokio::test]
    async fn restart_task_keeps_a_removed_torrent_whose_metadata_is_saved() {
        let dir = tempfile::TempDir::new().unwrap();
        let hash = "cab507494d02ebb1178b38f2e9d7be299c86b862";
        std::fs::create_dir_all(dir.path().join("torrents")).unwrap();
        std::fs::write(
            dir.path().join("torrents").join(format!("{hash}.torrent")),
            b"d",
        )
        .unwrap();
        let mgr = make_test_manager_in(&dir, vec![removed_torrent("bt", Some(hash))]);

        mgr.restart_task("bt").await.expect("restart");

        let tasks = mgr.tasks.read().await;
        assert!(tasks[0].uris.is_empty());
        assert_eq!(tasks[0].status, TaskStatus::Active);
    }

    #[tokio::test]
    async fn restart_task_refuses_a_removed_torrent_without_hash_or_magnet() {
        let mgr = make_test_manager(vec![
            removed_torrent("none", None),
            removed_torrent("empty", Some("")),
            removed_torrent("long", Some(&"ab".repeat(32))),
        ]);

        for gid in ["none", "empty", "long"] {
            let err = mgr.restart_task(gid).await.unwrap_err();
            assert!(err.contains("gone"), "{gid}: {err}");
        }

        let tasks = mgr.tasks.read().await;
        assert!(tasks.iter().all(|t| t.status == TaskStatus::Removed));
        assert!(tasks.iter().all(|t| t.uris.is_empty()));
        assert_eq!(mgr.torrent_resume_seq.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn restart_task_refuses_a_duplicate_of_a_live_torrent() {
        let hash = "cab507494d02ebb1178b38f2e9d7be299c86b862";
        let mut live = removed_torrent("live", Some(&hash.to_ascii_uppercase()));
        live.status = TaskStatus::Active;
        let mut removed = removed_torrent("old", Some(hash));
        removed.uris = vec![format!("magnet:?xt=urn:btih:{hash}")];
        let mgr = make_test_manager(vec![live, removed]);

        let err = mgr.restart_task("old").await.unwrap_err();
        assert!(err.contains("already in the list"), "{err}");

        let tasks = mgr.tasks.read().await;
        assert_eq!(tasks[1].status, TaskStatus::Removed);
        assert_eq!(tasks[1].error_message.as_deref(), Some("Tracker failed"));
    }

    fn torrent_record(
        info_hash: Option<&str>,
        name: Option<&str>,
        tiers: Vec<Vec<String>>,
    ) -> DownloadTask {
        DownloadTask {
            kind: TaskKind::Torrent,
            info_hash: info_hash.map(str::to_string),
            bt_name: name.map(str::to_string),
            bt_announce_list: tiers,
            ..Default::default()
        }
    }

    #[test]
    fn magnet_for_torrent_encodes_the_name_and_distinct_trackers() {
        let hash = Some("CAB507494D02EBB1178B38F2E9D7BE299C86B862");
        let tiers = vec![
            vec![
                "udp://a.example:80".to_string(),
                " udp://a.example:80 ".into(),
            ],
            vec![String::new(), "http://b.example/announce?k=1&x=y".into()],
        ];

        assert_eq!(
            magnet_for_torrent(&torrent_record(
                hash,
                Some("Ünï code/ä b.txt"),
                tiers.clone()
            ))
            .as_deref(),
            Some(
                "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862\
                 &dn=%C3%9Cn%C3%AF%20code%2F%C3%A4%20b.txt\
                 &tr=udp%3A%2F%2Fa.example%3A80\
                 &tr=http%3A%2F%2Fb.example%2Fannounce%3Fk%3D1%26x%3Dy"
            )
        );
        assert_eq!(
            magnet_for_torrent(&torrent_record(hash, Some(""), Vec::new())).as_deref(),
            Some("magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862")
        );

        let magnet = magnet_for_torrent(&torrent_record(hash, Some("A & B"), tiers)).unwrap();
        let parsed = risuko_bt::Magnet::parse(&magnet).unwrap();
        assert_eq!(parsed.display_name.as_deref(), Some("A & B"));
        assert_eq!(
            parsed.trackers,
            vec![
                "udp://a.example:80".to_string(),
                "http://b.example/announce?k=1&x=y".to_string()
            ]
        );
    }

    #[test]
    fn magnet_for_torrent_caps_trackers_and_rejects_other_hashes() {
        let tiers: Vec<Vec<String>> = (0..30)
            .map(|i| vec![format!("udp://t{i}.example:80")])
            .collect();
        let hash = "ab".repeat(20);
        let magnet = magnet_for_torrent(&torrent_record(Some(&hash), None, tiers)).unwrap();
        assert_eq!(magnet.matches("&tr=").count(), 20);
        assert!(magnet.ends_with("&tr=udp%3A%2F%2Ft19.example%3A80"));
        assert!(!magnet.contains("&dn="));

        for bad in [
            Some("ab".repeat(32)),
            Some("zz".repeat(20)),
            Some(String::new()),
            None,
        ] {
            let record = torrent_record(bad.as_deref(), None, Vec::new());
            assert!(magnet_for_torrent(&record).is_none(), "{bad:?}");
        }
    }

    #[test]
    fn magnet_for_torrent_uses_btmh_only_for_v2_only_torrents() {
        let v1 = "cab507494d02ebb1178b38f2e9d7be299c86b862";
        let v2 = "0123456789abcdef".repeat(4);
        let tiers = vec![vec!["udp://a.example:80".to_string()]];

        let mut v2_only = torrent_record(Some(&v2[..40]), Some("Name"), tiers.clone());
        v2_only.meta_version = Some("v2".into());
        v2_only.info_hash_v2 = Some(v2.to_ascii_uppercase());
        let magnet = magnet_for_torrent(&v2_only).unwrap();
        assert_eq!(
            magnet,
            format!("magnet:?xt=urn:btmh:1220{v2}&dn=Name&tr=udp%3A%2F%2Fa.example%3A80")
        );
        let parsed = risuko_bt::Magnet::parse(&magnet).unwrap();
        assert_eq!(parsed.info_hash_v2().map(|h| h.to_hex()), Some(v2.clone()));
        assert!(parsed.info_hash_v1().is_none());

        let mut hybrid = torrent_record(Some(v1), Some("Name"), tiers);
        hybrid.meta_version = Some("hybrid".into());
        hybrid.info_hash_v2 = Some(v2.clone());
        let magnet = magnet_for_torrent(&hybrid).unwrap();
        assert!(
            magnet.starts_with(&format!("magnet:?xt=urn:btih:{v1}&")),
            "{magnet}"
        );
        assert!(!magnet.contains("btmh"), "{magnet}");

        v2_only.info_hash_v2 = None;
        assert!(magnet_for_torrent(&v2_only).is_none());
        v2_only.info_hash_v2 = Some(v2[..40].to_string());
        assert!(magnet_for_torrent(&v2_only).is_none());
    }

    #[tokio::test]
    async fn restart_task_rebuilds_a_v2_magnet_for_a_removed_v2_only_torrent() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let v2 = "0123456789abcdef".repeat(4);
        let mut task = removed_torrent("bt", Some(&v2[..40]));
        task.meta_version = Some("v2".into());
        task.info_hash_v2 = Some(v2.clone());
        task.bt_name = Some("Only V2".into());
        task.bt_announce_list = vec![vec!["udp://tracker.example:6969/announce".into()]];
        mgr.tasks.write().await.push(task);

        mgr.restart_task("bt").await.expect("restart");

        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "bt").unwrap();
        assert_eq!(t.status, TaskStatus::Active);
        assert_eq!(t.uris.len(), 1);
        let parsed = risuko_bt::Magnet::parse(&t.uris[0]).unwrap();
        assert_eq!(parsed.info_hash_v2().map(|h| h.to_hex()), Some(v2));
        assert!(parsed.info_hash_v1().is_none());
        assert_eq!(parsed.display_name.as_deref(), Some("Only V2"));
        assert_eq!(
            parsed.trackers,
            vec!["udp://tracker.example:6969/announce".to_string()]
        );
        drop(tasks);
        assert!(mgr.pending_magnets.read().await.contains("bt"));
    }

    #[tokio::test]
    async fn restart_task_refuses_a_removed_v2_only_torrent_without_its_full_hash() {
        let v2 = "0123456789abcdef".repeat(4);
        let mut missing = removed_torrent("missing", Some(&v2[..40]));
        missing.meta_version = Some("v2".into());
        let mut short = removed_torrent("short", Some(&v2[..40]));
        short.meta_version = Some("v2".into());
        short.info_hash_v2 = Some(v2[..40].to_string());
        let mgr = make_test_manager(vec![missing, short]);

        for gid in ["missing", "short"] {
            let err = mgr.restart_task(gid).await.unwrap_err();
            assert!(
                err.contains("its torrent data is gone; add the torrent again"),
                "{gid}: {err}"
            );
        }

        let tasks = mgr.tasks.read().await;
        assert!(tasks.iter().all(|t| t.status == TaskStatus::Removed));
        assert!(tasks.iter().all(|t| t.uris.is_empty()));
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

    #[tokio::test]
    async fn change_option_reselects_the_files_of_a_paused_task() {
        let mut task = DownloadTask::new_http(
            "picked".into(),
            vec!["https://a.example/file.bin".into()],
            "/dl".into(),
            None,
            Map::new(),
        );
        task.kind = TaskKind::Torrent;
        task.status = TaskStatus::Paused;
        task.files = [(10, 4), (20, 0), (30, 30)]
            .iter()
            .enumerate()
            .map(|(i, (length, done))| DownloadFile {
                index: (i + 1).to_string(),
                path: format!("/dl/{i}.bin"),
                length: length.to_string(),
                completed_length: done.to_string(),
                selected: "true".into(),
                uris: Vec::new(),
            })
            .collect();
        let mgr = make_test_manager(vec![task]);
        let pick = |selection: &str| {
            let mut m = Map::new();
            m.insert("select-file".into(), Value::from(selection));
            m
        };
        async fn snapshot(mgr: &TaskManager) -> (Vec<String>, u64, u64) {
            let tasks = mgr.tasks.read().await;
            let t = tasks.iter().find(|t| t.gid == "picked").unwrap();
            let flags = t.files.iter().map(|f| f.selected.clone()).collect();
            (flags, t.total_length, t.completed_length)
        }

        mgr.change_option("picked", pick("1,3")).await.unwrap();
        assert_eq!(
            snapshot(&mgr).await,
            (vec!["true".into(), "false".into(), "true".into()], 40, 34)
        );

        mgr.change_option("picked", pick("")).await.unwrap();
        assert_eq!(
            snapshot(&mgr).await,
            (vec!["true".into(), "true".into(), "true".into()], 60, 34)
        );

        {
            let mut tasks = mgr.tasks.write().await;
            let t = tasks.iter_mut().find(|t| t.gid == "picked").unwrap();
            t.kind = TaskKind::Http;
            t.total_length = 99;
        }
        mgr.change_option("picked", pick("2")).await.unwrap();
        assert_eq!(
            snapshot(&mgr).await,
            (vec!["true".into(), "true".into(), "true".into()], 99, 34)
        );
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
        let manager = make_test_manager_with_engine_in(&dir).await;
        (manager, dir)
    }

    async fn make_test_manager_with_engine_in(dir: &tempfile::TempDir) -> TaskManager {
        make_test_manager_with_system_in(dir, Map::new()).await
    }

    async fn make_test_manager_with_system_in(
        dir: &tempfile::TempDir,
        mut system: Map<String, Value>,
    ) -> TaskManager {
        system.insert(
            "dir".to_string(),
            Value::String(dir.path().join("downloads").to_string_lossy().to_string()),
        );
        system.insert("bt-enable-upnp".to_string(), Value::Bool(false));
        system.insert("bt-enable-lsd".to_string(), Value::Bool(false));
        let options = EngineOptions::from_config(&system, &Map::new());
        TaskManager::new(dir.path(), options, EventBroadcaster::new(16))
            .await
            .unwrap()
    }

    fn saved_torrent_task(gid: &str, status: TaskStatus, info_hash: &str) -> DownloadTask {
        let mut task = make_torrent_task(gid, status);
        task.info_hash = Some(info_hash.to_string());
        task
    }

    fn write_saved_torrent_file(dir: &tempfile::TempDir, info_hash: &str) -> PathBuf {
        let path = dir
            .path()
            .join("torrents")
            .join(format!("{info_hash}.torrent"));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"d4:infod4:name4:testee").unwrap();
        path
    }

    fn purge_on_start() -> Map<String, Value> {
        let mut system = Map::new();
        system.insert("purge-record-on-start".to_string(), Value::Bool(true));
        system
    }

    #[tokio::test]
    async fn purge_on_start_removes_the_saved_torrent_files_of_purged_tasks() {
        let dir = tempfile::TempDir::new().unwrap();
        let hash = "aa".repeat(20);
        SessionManager::new(dir.path())
            .save(&[saved_torrent_task("done", TaskStatus::Complete, &hash)])
            .unwrap();
        let saved = write_saved_torrent_file(&dir, &hash);

        let mgr = make_test_manager_with_system_in(&dir, purge_on_start()).await;

        assert!(mgr.tasks.read().await.is_empty());
        assert!(!saved.exists());
    }

    #[tokio::test]
    async fn purge_on_start_keeps_a_saved_torrent_file_a_surviving_task_shares() {
        let dir = tempfile::TempDir::new().unwrap();
        let hash = "bb".repeat(20);
        SessionManager::new(dir.path())
            .save(&[
                saved_torrent_task("failed", TaskStatus::Error, &hash.to_ascii_uppercase()),
                saved_torrent_task("paused", TaskStatus::Paused, &hash),
            ])
            .unwrap();
        let saved = write_saved_torrent_file(&dir, &hash);

        let mgr = make_test_manager_with_system_in(&dir, purge_on_start()).await;

        let gids: Vec<String> = mgr
            .tasks
            .read()
            .await
            .iter()
            .map(|t| t.gid.clone())
            .collect();
        assert_eq!(gids, ["paused"]);
        assert!(saved.exists());
    }

    #[tokio::test]
    async fn purge_on_start_removes_the_partial_of_a_purged_failed_ed2k_task() {
        let dir = tempfile::TempDir::new().unwrap();
        let downloads = dir.path().join("downloads");
        std::fs::create_dir_all(&downloads).unwrap();
        let fin = downloads.join("f.bin");
        write_ed2k_partial(&fin, ED2K_HASH);
        std::fs::write(&fin, b"keep").unwrap();
        SessionManager::new(dir.path())
            .save(&[ed2k_task_in(&downloads, "failed", TaskStatus::Error)])
            .unwrap();

        let mgr = make_test_manager_with_system_in(&dir, purge_on_start()).await;

        assert!(mgr.tasks.read().await.is_empty());
        assert!(!downloads.join("f.bin.part").exists());
        assert!(!downloads.join("f.bin.part.met").exists());
        assert!(fin.exists());
    }

    #[tokio::test]
    async fn purge_on_start_keeps_an_ed2k_partial_a_surviving_task_uses() {
        let dir = tempfile::TempDir::new().unwrap();
        let downloads = dir.path().join("downloads");
        std::fs::create_dir_all(&downloads).unwrap();
        write_ed2k_partial(&downloads.join("f.bin"), ED2K_HASH);
        SessionManager::new(dir.path())
            .save(&[
                ed2k_task_in(&downloads, "failed", TaskStatus::Error),
                ed2k_task_in(&downloads, "paused", TaskStatus::Paused),
            ])
            .unwrap();

        let mgr = make_test_manager_with_system_in(&dir, purge_on_start()).await;

        let gids: Vec<String> = mgr
            .tasks
            .read()
            .await
            .iter()
            .map(|t| t.gid.clone())
            .collect();
        assert_eq!(gids, ["paused"]);
        assert!(downloads.join("f.bin.part").exists());
        assert!(downloads.join("f.bin.part.met").exists());
    }

    #[tokio::test]
    async fn purge_on_start_removes_the_temp_folder_of_a_purged_m3u8_task() {
        let dir = tempfile::TempDir::new().unwrap();
        let downloads = dir.path().join("downloads");
        let m3u8_task = |gid: &str, status: TaskStatus| {
            let mut task = DownloadTask::new_m3u8(
                gid.into(),
                "https://e.com/p.m3u8".into(),
                "video.ts".into(),
                downloads.to_string_lossy().into(),
                None,
                Map::new(),
            );
            task.status = status;
            task
        };
        let purged = downloads.join(".m3u8_video.ts_failed_00000000000000ab");
        let kept = downloads.join(".m3u8_video.ts_paused_00000000000000ab");
        for temp in [&purged, &kept] {
            std::fs::create_dir_all(temp).unwrap();
        }
        SessionManager::new(dir.path())
            .save(&[
                m3u8_task("failed", TaskStatus::Error),
                m3u8_task("paused", TaskStatus::Paused),
            ])
            .unwrap();

        let mgr = make_test_manager_with_system_in(&dir, purge_on_start()).await;

        let gids: Vec<String> = mgr
            .tasks
            .read()
            .await
            .iter()
            .map(|t| t.gid.clone())
            .collect();
        assert_eq!(gids, ["paused"]);
        assert!(!purged.exists());
        assert!(kept.exists());
    }

    #[tokio::test]
    async fn loading_a_session_trims_completed_usenet_tasks() {
        let dir = tempfile::TempDir::new().unwrap();
        SessionManager::new(dir.path())
            .save(&[
                usenet_task_with_segments("done", TaskStatus::Complete),
                usenet_task_with_segments("paused", TaskStatus::Paused),
                usenet_task_with_segments("failed", TaskStatus::Error),
            ])
            .unwrap();

        let mgr = make_test_manager_with_engine_in(&dir).await;

        let tasks = mgr.tasks.read().await;
        let segments = |gid: &str| -> Vec<usize> {
            tasks
                .iter()
                .find(|task| task.gid == gid)
                .and_then(|task| task.usenet.as_ref())
                .expect(gid)
                .files
                .iter()
                .map(|file| file.segments.len())
                .collect()
        };
        assert_eq!(segments("done"), [0, 0]);
        assert_eq!(segments("paused"), [3, 2]);
        assert_eq!(segments("failed"), [3, 2]);
        let done = tasks.iter().find(|task| task.gid == "done").unwrap();
        assert_eq!(done.status, TaskStatus::Complete);
        assert_eq!(done.usenet.as_ref().unwrap().files[0].name, "one.bin");
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

    #[test]
    fn links_are_classified_in_the_same_order_as_the_app() {
        let none = Map::new();
        let mut forced = Map::new();
        forced.insert("force-ytdlp".into(), Value::String("true".into()));
        let cases = [
            (
                "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567",
                &none,
                UriKind::Magnet,
            ),
            ("https://example.com/live/index.m3u8", &none, UriKind::M3u8),
            (
                "ed2k://|file|a.bin|1|0123456789ABCDEF0123456789ABCDEF|/",
                &none,
                UriKind::Ed2k,
            ),
            ("ftp://example.com/f.bin", &none, UriKind::Ftp),
            ("sftp://example.com/f.bin", &none, UriKind::Ftp),
            ("  ftp://example.com/f.bin\n", &none, UriKind::Ftp),
            ("https://www.youtube.com/watch?v=x", &none, UriKind::Media),
            ("https://example.com/f.bin", &forced, UriKind::Media),
            ("https://example.com/live/index.m3u8", &forced, UriKind::M3u8),
            ("ftp://example.com/f.bin", &forced, UriKind::Ftp),
            ("https://example.com/f.bin", &none, UriKind::Http),
            (
                "thunder://QUFodHRwOi8vZXhhbXBsZS5jb20vZi5iaW5aWg==",
                &none,
                UriKind::Http,
            ),
            (
                "dchub://hub.example.com:411/?TTH=ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFG&xl=42&dn=a.bin",
                &none,
                UriKind::LegacyP2p(TaskKind::Adc),
            ),
            (
                "gnutella://h:6346/?urn=urn:sha1:ABCDEF&dn=bar.bin&xl=10",
                &none,
                UriKind::LegacyP2p(TaskKind::Gnutella),
            ),
            (
                "g2://h:6346/sha1/ABCDEF?xl=10&dn=foo.bin",
                &none,
                UriKind::LegacyP2p(TaskKind::G2),
            ),
            (
                "gift://OpenFT/file?dn=x.bin",
                &none,
                UriKind::LegacyP2p(TaskKind::Gift),
            ),
            (
                "gift://OpenFT/file?dn=x.bin",
                &forced,
                UriKind::Media,
            ),
        ];
        for (uri, options, kind) in cases {
            assert_eq!(uri_kind(uri, options), kind, "{uri:?}");
        }
    }

    #[cfg(not(target_os = "android"))]
    #[test]
    fn media_downloads_are_supported_off_android() {
        assert!(media_downloads_supported());
    }

    #[cfg(not(target_os = "android"))]
    #[tokio::test]
    async fn add_media_task_creates_a_task_where_media_downloads_are_supported() {
        let mgr = make_test_manager(Vec::new());
        let mut options = Map::new();
        options.insert("pause".to_string(), Value::String("true".to_string()));
        let gid = mgr
            .add_media_task("https://www.youtube.com/watch?v=x", options)
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

    #[tokio::test]
    async fn add_http_task_rejects_non_http_scheme() {
        let mgr = make_test_manager(Vec::new());
        let error = mgr
            .add_http_task(
                vec!["gopher://example.com/x?token=abc".to_string()],
                Map::new(),
            )
            .await
            .expect_err("non-HTTP schemes must not become HTTP tasks");
        assert!(error.contains("Unsupported URI scheme"), "{error}");
        assert!(!error.contains("token=abc"), "{error}");
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
        let ok = |idx: usize| {
            (
                idx,
                "n".to_string(),
                Ok(std::path::PathBuf::from("/dl/n")),
                "http",
            )
        };

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
                ed2k_upload: None,
                mirrors: None,
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

    #[tokio::test]
    async fn finish_if_startup_aborted_parks_a_worker_cancelled_before_it_started() {
        let mgr = make_test_manager(vec![make_task("g1", TaskStatus::Active)]);
        let counters = Counters::new(0, 0);
        let epoch = next_worker_epoch();
        mgr.active_downloads
            .write()
            .await
            .insert("g1".into(), counters.to_active_plain(epoch));
        let finish = || {
            finish_if_startup_aborted(
                &mgr.tasks,
                &mgr.active_downloads,
                &mgr.events,
                "g1",
                epoch,
                "http",
                &counters,
            )
        };

        assert!(!finish().await);
        assert_eq!(mgr.tasks.read().await[0].status, TaskStatus::Active);
        assert!(mgr.active_downloads.read().await.contains_key("g1"));

        counters.cancel_token.cancel();

        assert!(finish().await);
        assert_eq!(mgr.tasks.read().await[0].status, TaskStatus::Paused);
        assert!(mgr.active_downloads.read().await.is_empty());
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

    async fn run_metalink_finish(mgr: &TaskManager, gid: &str, results: Vec<MetalinkFileResult>) {
        let epoch = next_worker_epoch();
        let fc: Vec<(usize, Counters)> = results
            .iter()
            .map(|(i, _, _, _)| (*i, Counters::new(0, 1)))
            .collect();
        let mut ad = Counters::new(0, 0).to_active_plain(epoch);
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
                (0, "a".into(), Ok(std::path::PathBuf::from("/dl/a")), "http"),
                (1, "b".into(), Err("all mirrors failed".into()), "http"),
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
                (0, "a".into(), Ok(std::path::PathBuf::from("/dl/a")), "http"),
                (1, "b".into(), Ok(std::path::PathBuf::from("/dl/b")), "http"),
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
                (0, "a".into(), Err("download cancelled".into()), "http"),
                (1, "b".into(), Err("download cancelled".into()), "http"),
            ],
        )
        .await;

        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "gm").unwrap();
        assert_ne!(t.status, TaskStatus::Complete);
        assert_eq!(t.status, TaskStatus::Paused);
        assert!(t.error_code.is_none());
    }

    #[test]
    fn the_metalink_error_label_follows_the_mirror_the_worker_uses() {
        let ftp_only = vec!["sftp://a/f.iso".to_string(), "ftp://b/f.iso".to_string()];
        assert_eq!(metalink_mirror_label(&ftp_only), "ftp");
        let web = vec!["https://a/f.iso".to_string(), "http://b/f.iso".to_string()];
        assert_eq!(metalink_mirror_label(&web), "http");
        let web_first = vec!["https://a/f.iso".to_string(), "ftp://b/f.iso".to_string()];
        assert_eq!(metalink_mirror_label(&web_first), "http");
        assert_eq!(metalink_mirror_label(&[]), "http");
    }

    #[test]
    fn an_ftp_login_failure_is_only_recognised_under_the_ftp_label() {
        let text = "FTP login failed: Invalid response: [530] 530 Login incorrect.";
        assert_eq!(
            classify_error(text, "ftp"),
            super::super::error_code::ErrorCode::FTP_LOGIN_FAILED
        );
        assert_ne!(
            classify_error(text, "http"),
            super::super::error_code::ErrorCode::FTP_LOGIN_FAILED
        );
    }

    #[tokio::test]
    async fn metalink_reports_the_login_failure_code_for_an_ftp_mirror() {
        let mut task = DownloadTask::new_metalink(
            "gm".into(),
            "/dl".into(),
            None,
            Map::new(),
            vec![mk_metalink_file("1", 100, 0)],
        );
        task.status = TaskStatus::Active;
        let mgr = make_test_manager(vec![task]);

        run_metalink_finish(
            &mgr,
            "gm",
            vec![(
                0,
                "a".into(),
                Err("FTP login failed: Invalid response: [530] 530 Login incorrect.".into()),
                "ftp",
            )],
        )
        .await;

        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "gm").unwrap();
        assert_eq!(t.status, TaskStatus::Paused);
        assert_eq!(
            t.error_code,
            Some(super::super::error_code::ErrorCode::FTP_LOGIN_FAILED.to_string())
        );
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
    async fn change_position_saturates_extreme_offsets() {
        let mgr = make_test_manager(vec![
            make_task("w1", TaskStatus::Waiting),
            make_task("w2", TaskStatus::Waiting),
            make_task("w3", TaskStatus::Waiting),
        ]);
        let order = || async {
            let waiting = mgr.tell_waiting(0, 10, &[]).await;
            waiting
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t.get("gid").unwrap().as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        };

        let result = mgr.change_position("w2", i64::MAX, "POS_CUR").await;
        assert_eq!(result.unwrap(), 2);
        assert_eq!(order().await, ["w1", "w3", "w2"]);

        let result = mgr.change_position("w2", i64::MIN, "POS_END").await;
        assert_eq!(result.unwrap(), 0);
        assert_eq!(order().await, ["w2", "w1", "w3"]);
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

    fn torrent_file_bytes(name: &str, private: bool) -> Vec<u8> {
        use risuko_bt::bencode::{encode_to_vec, Value as Bencode};
        let mut info = vec![
            (b"length".to_vec(), Bencode::Int(1024)),
            (b"name".to_vec(), Bencode::Bytes(name.as_bytes().to_vec())),
            (b"piece length".to_vec(), Bencode::Int(1024)),
            (b"pieces".to_vec(), Bencode::Bytes(vec![0u8; 20])),
        ];
        if private {
            info.push((b"private".to_vec(), Bencode::Int(1)));
        }
        encode_to_vec(&Bencode::Dict(vec![
            (
                b"announce".to_vec(),
                Bencode::Bytes(b"http://own.example/announce".to_vec()),
            ),
            (b"info".to_vec(), Bencode::Dict(info)),
        ]))
    }

    #[tokio::test]
    async fn seeding_in_place_ignores_routing_rules() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        mgr.options.write().await.global.insert(
            "task-routing-rules".into(),
            serde_json::json!([{
                "id": "all",
                "label": "Everything",
                "pattern": "*",
                "dir": dir.path().join("routed").to_string_lossy(),
            }]),
        );
        let source = dir.path().join("source");
        let mut seed = TaskManager::seed_in_place_options(&source.to_string_lossy());
        seed.insert("pause".into(), Value::Bool(true));
        let mut routed = Map::new();
        routed.insert("pause".into(), Value::Bool(true));
        let seed_gid = mgr
            .add_torrent_task(torrent_file_bytes("seeded", false), seed)
            .await
            .unwrap();
        let routed_gid = mgr
            .add_torrent_task(torrent_file_bytes("routed", false), routed)
            .await
            .unwrap();
        let tasks = mgr.tasks.read().await;
        let find = |gid: &str| tasks.iter().find(|t| t.gid == gid).unwrap();
        assert_eq!(find(&seed_gid).dir, source.to_string_lossy());
        assert_eq!(find(&seed_gid).tag, None);
        assert_eq!(
            find(&seed_gid).options.get("keep-seeding"),
            Some(&Value::Bool(true))
        );
        assert_eq!(
            find(&seed_gid)
                .options
                .get("risuko-completion-script-enabled"),
            Some(&Value::Bool(false))
        );
        assert_eq!(find(&routed_gid).tag.as_deref(), Some("Everything"));
        let routed_dir = dir.path().join("routed").to_string_lossy().into_owned();
        assert_eq!(find(&routed_gid).dir, routed_dir);
        assert_eq!(
            find(&routed_gid).options.get("dir"),
            Some(&Value::String(routed_dir))
        );
    }

    #[tokio::test]
    async fn seeding_in_place_starts_seeding_without_a_completion_event() {
        use risuko_bt::create::{create_torrent, CreateOptions};
        let (mgr, dir) = make_test_manager_with_engine().await;
        let mut events = mgr.events.subscribe();
        let source = dir.path().join("source");
        std::fs::create_dir_all(&source).unwrap();
        let mut gids = Vec::new();
        for (name, seed) in [("seeded.bin", true), ("fetched.bin", false)] {
            std::fs::write(source.join(name), name.repeat(5000)).unwrap();
            let created = create_torrent(
                &source.join(name),
                &CreateOptions::default(),
                |_, _| {},
                &std::sync::atomic::AtomicBool::new(false),
            )
            .unwrap();
            let dir = source.to_string_lossy();
            gids.push(if seed {
                mgr.seed_created_torrent(created.bytes, &dir, created.stamps)
                    .await
                    .unwrap()
            } else {
                let mut options = TaskManager::seed_in_place_options(&dir);
                options.remove(SEED_IN_PLACE_KEY);
                mgr.add_torrent_task(created.bytes, options).await.unwrap()
            });
        }

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while mgr.tasks.read().await.iter().any(|t| !t.seeder) {
            assert!(std::time::Instant::now() < deadline, "tasks never seeded");
            mgr.update_progress().await;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let mut completed = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let EngineEvent::BtDownloadComplete { gid, .. }
            | EngineEvent::DownloadComplete { gid, .. } = event
            {
                completed.push(gid);
            }
        }
        assert_eq!(completed, [gids[1].clone()]);

        for gid in &gids {
            let mut stop = Map::new();
            stop.insert("seed-time".into(), serde_json::json!(0));
            mgr.change_option(gid, stop).await.unwrap();
        }
        let mut stopped = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let EngineEvent::DownloadComplete { gid, seeded } = event {
                stopped.push((gid, seeded));
            }
        }
        assert_eq!(stopped, [(gids[1].clone(), true)]);
        mgr.update_progress().await;
        assert!(mgr
            .tasks
            .read()
            .await
            .iter()
            .all(|t| t.status == TaskStatus::Complete && !t.seeder));
    }

    fn seeding_manager(own: &[(&str, Value)]) -> TaskManager {
        let mut task = make_torrent_task("s", TaskStatus::Active);
        task.seeder = true;
        task.seeding_since = 1;
        task.options.extend(
            own.iter()
                .map(|(key, value)| ((*key).to_string(), value.clone())),
        );
        make_test_manager(vec![task])
    }

    async fn change_seeder(mgr: &TaskManager, change: &[(&str, Value)]) -> DownloadTask {
        let opts = change
            .iter()
            .map(|(key, value)| ((*key).to_string(), value.clone()))
            .collect();
        mgr.change_option("s", opts).await.unwrap();
        mgr.tasks.read().await[0].clone()
    }

    #[tokio::test]
    async fn a_blank_or_null_seed_time_does_not_stop_a_seeder() {
        for seed_time in [Value::Null, serde_json::json!(""), serde_json::json!("abc")] {
            let mgr = seeding_manager(&[]);
            let task = change_seeder(&mgr, &[("seed-time", seed_time.clone())]).await;
            assert_eq!(
                (task.status, task.seeder),
                (TaskStatus::Active, true),
                "{seed_time}"
            );
        }
    }

    #[tokio::test]
    async fn seed_time_zero_with_keep_seeding_on_keeps_a_task_seeding() {
        let mgr = seeding_manager(&[]);
        let rule = [
            ("keep-seeding", serde_json::json!("true")),
            ("seed-time", serde_json::json!("0")),
            ("seed-ratio", serde_json::json!("0")),
        ];
        let task = change_seeder(&mgr, &rule).await;
        assert_eq!((task.status, task.seeder), (TaskStatus::Active, true));
        for (key, value) in rule {
            assert_eq!(task.options.get(key), Some(&value));
        }
    }

    #[tokio::test]
    async fn stop_seeding_ends_a_seeder_that_has_a_ratio_of_its_own() {
        let mgr = seeding_manager(&[("seed-ratio", serde_json::json!("2"))]);
        let task = change_seeder(&mgr, &[("seed-time", serde_json::json!(0))]).await;
        assert_eq!((task.status, task.seeder), (TaskStatus::Active, true));
        let task = change_seeder(
            &mgr,
            &[
                ("seed-time", serde_json::json!(0)),
                ("seed-ratio", serde_json::json!(0)),
            ],
        )
        .await;
        assert_eq!((task.status, task.seeder), (TaskStatus::Complete, false));
    }

    const STAMPED_PIECE: usize = 16 * 1024;

    async fn add_complete_torrent(mgr: &TaskManager, dir: &Path) -> (String, PathBuf) {
        use risuko_bt::create::{create_torrent, CreateOptions};
        let source = dir.join("source");
        let pack = source.join("pack");
        std::fs::create_dir_all(&pack).unwrap();
        std::fs::write(pack.join("a.bin"), vec![1u8; 40_000]).unwrap();
        std::fs::write(pack.join("b.bin"), vec![2u8; 30_000]).unwrap();
        let created = create_torrent(
            &pack,
            &CreateOptions {
                piece_length: Some(STAMPED_PIECE as u32),
                ..Default::default()
            },
            |_, _| {},
            &std::sync::atomic::AtomicBool::new(false),
        )
        .unwrap();
        let mut options = TaskManager::seed_in_place_options(&source.to_string_lossy());
        options.remove(SEED_IN_PLACE_KEY);
        let gid = mgr.add_torrent_task(created.bytes, options).await.unwrap();
        (gid, pack)
    }

    async fn task_stamps(mgr: &TaskManager, gid: &str) -> Vec<TaskFileStamp> {
        mgr.tasks
            .read()
            .await
            .iter()
            .find(|t| t.gid == gid)
            .map(|t| t.bt_file_stamps.clone())
            .unwrap_or_default()
    }

    async fn tick_until_stamped(mgr: &TaskManager, gid: &str) -> Vec<TaskFileStamp> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        loop {
            mgr.update_progress().await;
            let stamps = task_stamps(mgr, gid).await;
            if !stamps.is_empty() {
                return stamps;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the task never recorded file stamps"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    async fn forget_torrent_like_a_restart(mgr: &TaskManager, gid: &str) {
        let (te, tid) = mgr.torrent_target(gid).await.unwrap();
        te.remove(tid, false).await.unwrap();
        mgr.torrent_ids.write().await.remove(gid);
    }

    async fn restored_stats(
        mgr: &TaskManager,
        gid: &str,
        what: &str,
        settled: impl Fn(&torrent::TorrentStats) -> bool,
    ) -> torrent::TorrentStats {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if let Some((te, tid)) = mgr.torrent_target(gid).await {
                if let Some(stats) = te.get_torrent_stats(tid).filter(|stats| settled(stats)) {
                    return stats;
                }
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {what}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    fn stamped_time(stamps: &[TaskFileStamp], name: &str) -> std::time::SystemTime {
        let task = DownloadTask {
            bt_file_stamps: stamps.to_vec(),
            ..Default::default()
        };
        task.verified_files()
            .into_iter()
            .find(|stamp| stamp.path == [name])
            .unwrap()
            .modified
    }

    fn set_modified(file: &Path, time: std::time::SystemTime) {
        std::fs::File::options()
            .write(true)
            .open(file)
            .unwrap()
            .set_modified(time)
            .unwrap();
    }

    #[test]
    fn file_stamps_are_asked_for_once_per_finish() {
        let mut task = DownloadTask::new_torrent("t".into(), "/dl".into(), None, Map::new());
        let mut attempted = HashSet::new();
        assert!(wants_file_stamps(&mut task, true, &mut attempted));
        assert!(!wants_file_stamps(&mut task, true, &mut attempted));

        task.bt_file_stamps = vec![TaskFileStamp::default()];
        assert!(!wants_file_stamps(&mut task, false, &mut attempted));
        assert!(task.bt_file_stamps.is_empty());
        assert!(attempted.is_empty());
        assert!(wants_file_stamps(&mut task, true, &mut attempted));

        attempted.clear();
        task.bt_file_stamps = vec![TaskFileStamp::default()];
        assert!(!wants_file_stamps(&mut task, true, &mut attempted));
    }

    #[test]
    fn a_failed_flush_marks_the_task_once_and_drops_its_stamps() {
        let mut task = make_torrent_task("t", TaskStatus::Active);
        task.bt_file_stamps = vec![TaskFileStamp::default()];
        assert!(!remember_failed_flush(&mut task, false, Some("boot-a")));
        assert_eq!(task.bt_flush_failed_boot, None);
        assert_eq!(task.bt_file_stamps.len(), 1);

        assert!(remember_failed_flush(&mut task, true, Some("boot-a")));
        assert_eq!(task.bt_flush_failed_boot.as_deref(), Some("boot-a"));
        assert!(task.bt_file_stamps.is_empty());

        task.bt_file_stamps = vec![TaskFileStamp::default()];
        assert!(!remember_failed_flush(&mut task, true, Some("boot-b")));
        assert!(!remember_failed_flush(&mut task, false, Some("boot-b")));
        assert_eq!(task.bt_flush_failed_boot.as_deref(), Some("boot-a"));
        assert_eq!(task.bt_file_stamps.len(), 1);

        let mut unknown = make_torrent_task("u", TaskStatus::Active);
        assert!(remember_failed_flush(&mut unknown, true, None));
        assert_eq!(unknown.bt_flush_failed_boot.as_deref(), Some("unknown"));
    }

    #[test]
    fn a_task_marked_by_a_failed_flush_is_never_asked_for_stamps() {
        let mut task = make_torrent_task("t", TaskStatus::Active);
        task.bt_flush_failed_boot = Some("boot-a".into());
        let mut attempted = HashSet::new();
        assert!(!wants_file_stamps(&mut task, true, &mut attempted));
        assert!(!wants_file_stamps(&mut task, false, &mut attempted));
        assert!(!wants_file_stamps(&mut task, true, &mut attempted));
        assert!(attempted.is_empty());
        assert!(!takes_late_file_stamps(&task, "t"));

        task.bt_flush_failed_boot = None;
        assert!(takes_late_file_stamps(&task, "t"));
        assert!(wants_file_stamps(&mut task, true, &mut attempted));
    }

    #[test]
    fn a_flush_failed_at_shutdown_marks_only_the_named_torrent_tasks() {
        let mut failed = make_torrent_task("failed", TaskStatus::Paused);
        failed.bt_file_stamps = vec![TaskFileStamp::default()];
        let mut marked = make_torrent_task("marked", TaskStatus::Active);
        marked.bt_flush_failed_boot = Some("boot-a".into());
        let mut healthy = make_torrent_task("healthy", TaskStatus::Complete);
        healthy.bt_file_stamps = vec![TaskFileStamp::default()];
        let http = make_task("http", TaskStatus::Active);
        let mut tasks = vec![failed, marked, healthy, http];
        let named: HashSet<String> = ["failed", "marked", "http"]
            .into_iter()
            .map(String::from)
            .collect();

        assert!(mark_flushes_failed_at_shutdown(
            &mut tasks,
            &named,
            Some("boot-b")
        ));
        assert_eq!(tasks[0].bt_flush_failed_boot.as_deref(), Some("boot-b"));
        assert!(tasks[0].bt_file_stamps.is_empty());
        assert_eq!(tasks[1].bt_flush_failed_boot.as_deref(), Some("boot-a"));
        assert_eq!(tasks[2].bt_flush_failed_boot, None);
        assert_eq!(tasks[2].bt_file_stamps.len(), 1);
        assert_eq!(tasks[3].bt_flush_failed_boot, None);
        assert!(!tasks.iter().any(|task| misses_flush_mark(task, &named)));

        assert!(!mark_flushes_failed_at_shutdown(
            &mut tasks,
            &named,
            Some("boot-c")
        ));
        assert_eq!(tasks[0].bt_flush_failed_boot.as_deref(), Some("boot-b"));
        assert!(!mark_flushes_failed_at_shutdown(
            &mut tasks,
            &HashSet::new(),
            Some("boot-b")
        ));

        let mut unknown = vec![make_torrent_task("failed", TaskStatus::Error)];
        assert!(misses_flush_mark(&unknown[0], &named));
        assert!(mark_flushes_failed_at_shutdown(&mut unknown, &named, None));
        assert_eq!(unknown[0].bt_flush_failed_boot.as_deref(), Some("unknown"));
    }

    fn flush_marks_after_loading(
        saved: &[(&str, Option<&str>)],
        boot: Option<&str>,
    ) -> Vec<(String, Option<String>)> {
        let dir = tempfile::TempDir::new().unwrap();
        let session = SessionManager::new(dir.path());
        let tasks: Vec<DownloadTask> = saved
            .iter()
            .map(|(gid, mark)| {
                let mut task = make_torrent_task(gid, TaskStatus::Paused);
                task.bt_flush_failed_boot = mark.map(str::to_string);
                task
            })
            .collect();
        session.save(&tasks).unwrap();
        let mut loaded = session.load();
        forget_failed_flushes_from_earlier_boots(&mut loaded, boot);
        loaded
            .into_iter()
            .map(|task| (task.gid, task.bt_flush_failed_boot))
            .collect()
    }

    #[test]
    fn loading_on_the_same_boot_keeps_the_flush_mark() {
        assert_eq!(
            flush_marks_after_loading(&[("t", Some("boot-a"))], Some("boot-a")),
            [("t".to_string(), Some("boot-a".to_string()))]
        );
        assert_eq!(
            flush_marks_after_loading(&[("t", Some("1794000000"))], Some("1794000060")),
            [("t".to_string(), Some("1794000000".to_string()))]
        );
    }

    #[test]
    fn loading_after_a_restart_of_the_machine_clears_the_flush_mark() {
        assert_eq!(
            flush_marks_after_loading(&[("t", Some("boot-a"))], Some("boot-b")),
            [("t".to_string(), None)]
        );
        assert_eq!(
            flush_marks_after_loading(&[("t", Some("1794000000"))], Some("1794086400")),
            [("t".to_string(), None)]
        );
    }

    #[test]
    fn an_unknown_boot_keeps_the_flush_mark() {
        assert_eq!(
            flush_marks_after_loading(
                &[("unknown", Some("unknown")), ("known", Some("boot-a"))],
                Some("boot-b")
            )[0],
            ("unknown".to_string(), Some("unknown".to_string()))
        );
        assert_eq!(
            flush_marks_after_loading(&[("t", Some("boot-a"))], None),
            [("t".to_string(), Some("boot-a".to_string()))]
        );
    }

    #[test]
    fn a_session_file_without_the_flush_mark_loads_unmarked() {
        let task = make_torrent_task("t", TaskStatus::Paused);
        let encoded = serde_json::to_value(&task).unwrap();
        assert!(encoded.get("bt_flush_failed_boot").is_none());
        assert_eq!(
            flush_marks_after_loading(&[("t", None)], Some("boot-a")),
            [("t".to_string(), None)]
        );

        let mut marked = task;
        marked.bt_flush_failed_boot = Some("boot-a".into());
        let encoded = serde_json::to_value(&marked).unwrap();
        assert_eq!(encoded["bt_flush_failed_boot"], serde_json::json!("boot-a"));
        let restored: DownloadTask = serde_json::from_value(encoded).unwrap();
        assert_eq!(restored.bt_flush_failed_boot.as_deref(), Some("boot-a"));
    }

    #[test]
    fn the_flush_mark_is_not_sent_to_clients() {
        let mut task = make_torrent_task("t", TaskStatus::Active);
        task.bt_flush_failed_boot = Some("boot-mark-7f3a".into());
        let status = serde_json::to_string(&task.to_rpc_status(&[])).unwrap();
        assert!(!status.contains("boot-mark-7f3a"));
        assert!(!status.contains("bt_flush_failed_boot"));
        assert!(!status.to_ascii_lowercase().contains("flushfailed"));
    }

    #[tokio::test]
    async fn the_manager_forgets_flush_marks_from_an_earlier_boot_when_it_loads() {
        let dir = tempfile::TempDir::new().unwrap();
        let marked = |gid: &str, mark: &str| {
            let mut task = make_torrent_task(gid, TaskStatus::Paused);
            task.bt_flush_failed_boot = Some(mark.to_string());
            task
        };
        let current = super::super::boot_id::current();
        let mut saved = vec![
            marked("earlier", "a-boot-before-this-one"),
            marked("unknown", super::super::boot_id::UNKNOWN),
        ];
        if let Some(current) = current {
            saved.push(marked("current", current));
        }
        SessionManager::new(dir.path()).save(&saved).unwrap();

        let mgr = make_test_manager_with_engine_in(&dir).await;

        let tasks = mgr.tasks.read().await;
        let mark = |gid: &str| {
            tasks
                .iter()
                .find(|task| task.gid == gid)
                .unwrap()
                .bt_flush_failed_boot
                .clone()
        };
        assert_eq!(mark("unknown").as_deref(), Some("unknown"));
        if let Some(current) = current {
            assert_eq!(mark("earlier"), None);
            assert_eq!(mark("current").as_deref(), Some(current));
        } else {
            assert_eq!(mark("earlier").as_deref(), Some("a-boot-before-this-one"));
        }
    }

    #[test]
    fn a_torrent_error_fails_a_seeder_once_unfinished_or_on_a_storage_failure() {
        let downloading = make_torrent_task("d", TaskStatus::Active);
        let mut seeding = make_torrent_task("s", TaskStatus::Active);
        seeding.seeder = true;
        seeding.seeding_since = 1;

        assert!(surfaces_torrent_error(&downloading, false, false, false));
        assert!(surfaces_torrent_error(&downloading, true, false, false));
        assert!(surfaces_torrent_error(&seeding, false, false, false));
        assert!(!surfaces_torrent_error(&seeding, true, false, false));
        assert!(!surfaces_torrent_error(&downloading, false, true, false));
        assert!(!surfaces_torrent_error(&seeding, false, true, false));
        assert!(surfaces_torrent_error(&seeding, true, false, true));
        assert!(surfaces_torrent_error(&downloading, true, false, true));
        assert!(!surfaces_torrent_error(&seeding, true, true, true));
        for status in [TaskStatus::Paused, TaskStatus::Error, TaskStatus::Complete] {
            for storage_error in [false, true] {
                assert!(!surfaces_torrent_error(
                    &make_torrent_task("x", status),
                    false,
                    false,
                    storage_error
                ));
            }
        }
    }

    #[tokio::test]
    async fn a_seeding_torrent_whose_files_cannot_be_prepared_fails() {
        use risuko_bt::create::{create_torrent, CreateOptions};
        let (mgr, dir) = make_test_manager_with_engine().await;
        let (healthy, _) = add_complete_torrent(&mgr, dir.path()).await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !mgr
            .tasks
            .read()
            .await
            .iter()
            .any(|t| t.gid == healthy && t.seeder)
        {
            assert!(std::time::Instant::now() < deadline, "never seeded");
            mgr.update_progress().await;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let source = dir.path().join("blocked");
        let pack = source.join("pack");
        std::fs::create_dir_all(pack.join("sub")).unwrap();
        std::fs::write(pack.join("a.bin"), vec![1u8; 40_000]).unwrap();
        std::fs::write(pack.join("sub").join("b.bin"), vec![2u8; 30_000]).unwrap();
        let created = create_torrent(
            &pack,
            &CreateOptions {
                version: risuko_bt::core::MetaVersion::V1,
                piece_length: Some(STAMPED_PIECE as u32),
                ..Default::default()
            },
            |_, _| {},
            &std::sync::atomic::AtomicBool::new(false),
        )
        .unwrap();
        std::fs::remove_dir_all(pack.join("sub")).unwrap();
        std::fs::write(pack.join("sub"), b"").unwrap();
        let parts =
            risuko_bt::storage::parts_dir_for(&pack, &created.info_hash_v1.unwrap().to_hex());
        std::fs::create_dir_all(&parts).unwrap();
        for idx in 0..created.file_count {
            std::fs::write(parts.join(idx.to_string()), b"").unwrap();
        }
        let mut options = TaskManager::seed_in_place_options(&source.to_string_lossy());
        options.remove(SEED_IN_PLACE_KEY);
        let gid = mgr.add_torrent_task(created.bytes, options).await.unwrap();
        {
            let mut tasks = mgr.tasks.write().await;
            let task = tasks.iter_mut().find(|t| t.gid == gid).unwrap();
            task.seeder = true;
            task.seeding_since = 1;
        }
        let stats = restored_stats(&mgr, &gid, "the storage error", |stats| {
            stats.error.is_some()
        })
        .await;
        let blocked = pack.join("sub").join("b.bin").display().to_string();
        let error = stats.error.unwrap();
        assert!(error.contains(&blocked), "{error}");
        assert!(!stats.is_finished);

        let mut events = mgr.events.subscribe();
        mgr.update_progress().await;

        {
            let tasks = mgr.tasks.read().await;
            let task = tasks.iter().find(|t| t.gid == gid).unwrap();
            assert_eq!(task.status, TaskStatus::Error);
            assert_eq!(task.error_message.as_deref(), Some(error.as_str()));
            assert!(!task.seeder);
            assert_eq!(task.seeding_since, 0);
            assert_eq!((task.download_speed, task.upload_speed), (0, 0));
            let other = tasks.iter().find(|t| t.gid == healthy).unwrap();
            assert_eq!((other.status, other.seeder), (TaskStatus::Active, true));
            assert!(other.error_message.is_none());
        }
        let mut failed = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let EngineEvent::DownloadError { gid } = event {
                failed.push(gid);
            }
        }
        assert_eq!(failed, [gid]);
    }

    #[tokio::test]
    async fn a_finished_seeder_fails_on_a_storage_error_but_not_a_hash_disagreement() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        let (gid, _) = add_complete_torrent(&mgr, dir.path()).await;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while !mgr
            .tasks
            .read()
            .await
            .iter()
            .any(|t| t.gid == gid && t.seeder)
        {
            assert!(std::time::Instant::now() < deadline, "never seeded");
            mgr.update_progress().await;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let root = restored_stats(&mgr, &gid, "the seeder to finish", |stats| {
            stats.is_finished
        })
        .await
        .resolved_root
        .map(PathBuf::from)
        .unwrap();
        let force = |error: &str, storage: bool| {
            let mut forced = torrent::FORCED_ERRORS.lock();
            forced.retain(|(dir, ..)| *dir != root);
            forced.push((root.clone(), error.to_string(), storage));
        };
        let mut events = mgr.events.subscribe();

        force(
            "Piece 1 matches its v1 hash but not its v2 hash; the torrent's hashes disagree",
            false,
        );
        mgr.update_progress().await;
        {
            let tasks = mgr.tasks.read().await;
            let task = tasks.iter().find(|t| t.gid == gid).unwrap();
            assert_eq!((task.status, task.seeder), (TaskStatus::Active, true));
            assert!(task.error_message.is_none());
        }

        let error = "Disk write failed: No space left on device";
        force(error, true);
        mgr.update_progress().await;
        torrent::FORCED_ERRORS
            .lock()
            .retain(|(dir, ..)| *dir != root);
        {
            let tasks = mgr.tasks.read().await;
            let task = tasks.iter().find(|t| t.gid == gid).unwrap();
            assert_eq!(task.status, TaskStatus::Error);
            assert_eq!(task.error_message.as_deref(), Some(error));
            assert!(!task.seeder);
            assert_eq!(task.seeding_since, 0);
            assert_eq!((task.download_speed, task.upload_speed), (0, 0));
        }
        let mut failed = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let EngineEvent::DownloadError { gid } = event {
                failed.push(gid);
            }
        }
        assert_eq!(failed, [gid]);
    }

    #[tokio::test]
    async fn a_finished_torrent_records_its_file_stamps() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        let (gid, pack) = add_complete_torrent(&mgr, dir.path()).await;
        let stamps = tick_until_stamped(&mgr, &gid).await;
        let mut recorded: Vec<(Vec<String>, u64)> = stamps
            .iter()
            .map(|stamp| (stamp.path.clone(), stamp.length))
            .collect();
        recorded.sort();
        assert_eq!(
            recorded,
            [
                (vec!["a.bin".to_string()], 40_000),
                (vec!["b.bin".to_string()], 30_000),
            ]
        );
        assert_eq!(
            stamped_time(&stamps, "a.bin"),
            std::fs::metadata(pack.join("a.bin"))
                .unwrap()
                .modified()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn a_restored_torrent_with_unchanged_files_does_not_rehash() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        let (gid, pack) = add_complete_torrent(&mgr, dir.path()).await;
        let stored = tick_until_stamped(&mgr, &gid).await;
        let stamped = stamped_time(&stored, "a.bin");
        let a = pack.join("a.bin");

        forget_torrent_like_a_restart(&mgr, &gid).await;
        let mut bytes = std::fs::read(&a).unwrap();
        bytes[..STAMPED_PIECE].fill(0);
        std::fs::write(&a, &bytes).unwrap();
        set_modified(&a, stamped);
        mgr.restore_saved_torrent_files().await;
        let stats = restored_stats(&mgr, &gid, "the restored torrent to finish", |stats| {
            stats.is_finished
        })
        .await;
        assert_eq!(stats.downloaded_bytes, stats.total_bytes);

        forget_torrent_like_a_restart(&mgr, &gid).await;
        set_modified(&a, stamped + std::time::Duration::from_secs(5));
        mgr.tasks
            .write()
            .await
            .iter_mut()
            .find(|t| t.gid == gid)
            .unwrap()
            .bt_file_stamps = stored;
        mgr.restore_saved_torrent_files().await;
        restored_stats(&mgr, &gid, "the recheck to find the damage", |stats| {
            stats.total_bytes > 0
                && stats.downloaded_bytes == stats.total_bytes - STAMPED_PIECE as u64
        })
        .await;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let (te, tid) = mgr.torrent_target(&gid).await.unwrap();
        assert!(!te.get_torrent_stats(tid).unwrap().is_finished);
        mgr.update_progress().await;
        assert!(task_stamps(&mgr, &gid).await.is_empty());
    }

    #[tokio::test]
    async fn restoring_uses_the_stamps_up_and_the_tick_records_new_ones() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        let (gid, _pack) = add_complete_torrent(&mgr, dir.path()).await;
        let stored = tick_until_stamped(&mgr, &gid).await;

        forget_torrent_like_a_restart(&mgr, &gid).await;
        mgr.restore_saved_torrent_files().await;
        assert!(task_stamps(&mgr, &gid).await.is_empty());
        assert!(!mgr.stamp_attempts.lock().contains(&gid));
        restored_stats(&mgr, &gid, "the restored torrent to finish", |stats| {
            stats.is_finished
        })
        .await;
        assert_eq!(tick_until_stamped(&mgr, &gid).await, stored);
    }

    #[tokio::test]
    async fn a_healthy_shutdown_leaves_no_flush_mark() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        let (gid, _pack) = add_complete_torrent(&mgr, dir.path()).await;
        let stamps = tick_until_stamped(&mgr, &gid).await;

        mgr.shutdown().await;

        assert!(mgr.torrent_ids.read().await.contains_key(&gid));
        assert_eq!(mgr.saved_rev.load(Ordering::Relaxed), mgr.tasks.rev());
        {
            let tasks = mgr.tasks.read().await;
            let task = tasks.iter().find(|t| t.gid == gid).unwrap();
            assert_eq!(task.bt_flush_failed_boot, None);
            assert_eq!(task.bt_file_stamps, stamps);
        }
        let saved = SessionManager::new(dir.path()).load();
        let saved = saved.iter().find(|t| t.gid == gid).unwrap();
        assert_eq!(saved.bt_flush_failed_boot, None);
        assert_eq!(saved.bt_file_stamps, stamps);
    }

    #[tokio::test]
    async fn a_paused_torrent_is_still_in_the_engine_when_it_shuts_down() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        let (gid, _pack) = add_complete_torrent(&mgr, dir.path()).await;
        restored_stats(&mgr, &gid, "the torrent to finish", |stats| {
            stats.is_finished
        })
        .await;
        mgr.pause(&gid).await.unwrap();

        let (te, tid) = mgr.torrent_target(&gid).await.unwrap();
        assert!(te.get_torrent_stats(tid).is_some());
        mgr.shutdown().await;
        assert_eq!(mgr.torrent_ids.read().await.get(&gid), Some(&tid));
    }

    #[tokio::test]
    async fn a_flush_failed_at_shutdown_is_saved_once_more_only_when_it_marks_a_task() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        let mut failed = make_torrent_task("failed", TaskStatus::Paused);
        failed.bt_file_stamps = vec![TaskFileStamp::default()];
        let other = make_torrent_task("other", TaskStatus::Complete);
        mgr.tasks.write().await.extend([failed, other]);
        mgr.torrent_ids
            .write()
            .await
            .extend([("failed".to_string(), 5), ("other".to_string(), 6)]);
        mgr.save_session().await.unwrap();
        let rev = mgr.tasks.rev();

        mgr.remember_flushes_failed_at_shutdown(&[7]).await;
        assert_eq!(mgr.tasks.rev(), rev);

        mgr.remember_flushes_failed_at_shutdown(&[5]).await;
        assert_ne!(mgr.tasks.rev(), rev);
        assert_eq!(mgr.saved_rev.load(Ordering::Relaxed), mgr.tasks.rev());
        let saved = SessionManager::new(dir.path()).load();
        let task = |gid: &str| saved.iter().find(|t| t.gid == gid).unwrap();
        assert_eq!(
            task("failed").bt_flush_failed_boot.as_deref(),
            Some(super::super::boot_id::current().unwrap_or("unknown"))
        );
        assert!(task("failed").bt_file_stamps.is_empty());
        assert_eq!(task("other").bt_flush_failed_boot, None);

        let rev = mgr.tasks.rev();
        mgr.remember_flushes_failed_at_shutdown(&[5]).await;
        assert_eq!(mgr.tasks.rev(), rev);
    }

    #[tokio::test]
    async fn a_flush_mark_is_saved_at_once_only_when_it_is_new() {
        let dir = tempfile::TempDir::new().unwrap();
        let mgr = make_test_manager_in(&dir, vec![make_torrent_task("t", TaskStatus::Active)]);
        mgr.save_session().await.unwrap();

        let rev = mgr.saved_rev.load(Ordering::Relaxed);
        let marked = remember_failed_flush(&mut mgr.tasks.write().await[0], false, Some("boot-a"));
        mgr.save_new_flush_marks(marked).await;
        assert_ne!(mgr.tasks.rev(), rev);
        assert_eq!(mgr.saved_rev.load(Ordering::Relaxed), rev);

        let marked = remember_failed_flush(&mut mgr.tasks.write().await[0], true, Some("boot-a"));
        mgr.save_new_flush_marks(marked).await;
        assert_eq!(mgr.saved_rev.load(Ordering::Relaxed), mgr.tasks.rev());
        let saved = SessionManager::new(dir.path()).load();
        assert_eq!(saved[0].bt_flush_failed_boot.as_deref(), Some("boot-a"));

        let rev = mgr.saved_rev.load(Ordering::Relaxed);
        let marked = remember_failed_flush(&mut mgr.tasks.write().await[0], true, Some("boot-b"));
        mgr.save_new_flush_marks(marked).await;
        assert_ne!(mgr.tasks.rev(), rev);
        assert_eq!(mgr.saved_rev.load(Ordering::Relaxed), rev);
    }

    #[tokio::test]
    async fn a_progress_update_without_a_new_flush_mark_does_not_save() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        let (gid, _pack) = add_complete_torrent(&mgr, dir.path()).await;
        tick_until_stamped(&mgr, &gid).await;
        mgr.save_session().await.unwrap();
        let rev = mgr.saved_rev.load(Ordering::Relaxed);

        mgr.update_progress().await;

        assert_ne!(mgr.tasks.rev(), rev);
        assert_eq!(mgr.saved_rev.load(Ordering::Relaxed), rev);
    }

    async fn set_flush_mark(mgr: &TaskManager, gid: &str, mark: Option<&str>) {
        mgr.tasks
            .write()
            .await
            .iter_mut()
            .find(|t| t.gid == gid)
            .unwrap()
            .bt_flush_failed_boot = mark.map(str::to_string);
    }

    #[tokio::test]
    async fn a_finished_torrent_marked_by_a_failed_flush_records_no_stamps() {
        let (mgr, dir) = make_test_manager_with_engine().await;
        let (gid, _pack) = add_complete_torrent(&mgr, dir.path()).await;
        set_flush_mark(&mgr, &gid, Some("boot-a")).await;
        restored_stats(&mgr, &gid, "the torrent to finish", |stats| {
            stats.is_finished
        })
        .await;
        for _ in 0..10 {
            mgr.update_progress().await;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert!(task_stamps(&mgr, &gid).await.is_empty());
        assert!(!mgr.stamp_attempts.lock().contains(&gid));

        mgr.stamp_attempts.lock().insert(gid.clone());
        mgr.record_file_stamps(vec![gid.clone()]).await;
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        assert!(task_stamps(&mgr, &gid).await.is_empty());

        set_flush_mark(&mgr, &gid, None).await;
        mgr.stamp_attempts.lock().clear();
        assert!(!tick_until_stamped(&mgr, &gid).await.is_empty());
    }

    async fn paused_torrent_task(mgr: &TaskManager, name: &str, private: bool) -> String {
        let mut options = Map::new();
        options.insert("pause".to_string(), Value::Bool(true));
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            mgr.add_torrent_task(torrent_file_bytes(name, private), options),
        )
        .await
        .expect("add_torrent_task did not finish")
        .expect("torrent task")
    }

    fn add_tracker_patch(url: &str) -> TaskPatch {
        TaskPatch {
            trackers: Some(vec![url.to_string()]),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn update_task_refuses_new_trackers_on_a_private_torrent() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let gid = paused_torrent_task(&mgr, "private-edit", true).await;

        let err = mgr
            .update_task(&gid, add_tracker_patch("http://b.example/announce"))
            .await
            .expect_err("a private torrent only announces to its metainfo trackers");
        assert!(err.contains("Private torrents"), "{err}");

        let tasks = mgr.tasks.read().await;
        let task = tasks.iter().find(|t| t.gid == gid).unwrap();
        assert!(task.bt_announce_list.is_empty());
        assert!(!task.options.contains_key("bt-tracker"));
    }

    #[tokio::test]
    async fn update_task_refuses_new_trackers_on_a_private_torrent_not_yet_re_added() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let gid = paused_torrent_task(&mgr, "private-restored", true).await;
        mgr.torrent_ids.write().await.remove(&gid);

        let err = mgr
            .update_task(&gid, add_tracker_patch("http://b.example/announce"))
            .await
            .expect_err("the saved metainfo marks the torrent private");
        assert!(err.contains("Private torrents"), "{err}");
    }

    #[tokio::test]
    async fn update_task_still_adds_trackers_to_a_public_torrent_with_an_engine() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let gid = paused_torrent_task(&mgr, "public-edit", false).await;

        let outcome = mgr
            .update_task(&gid, add_tracker_patch("http://b.example/announce"))
            .await
            .expect("public torrents take extra trackers");
        assert_eq!(outcome.trackers_added, 1);

        let tasks = mgr.tasks.read().await;
        let task = tasks.iter().find(|t| t.gid == gid).unwrap();
        assert_eq!(
            task.bt_announce_list,
            vec![vec!["http://b.example/announce".to_string()]]
        );
    }

    #[tokio::test]
    async fn magnet_tracker_replay_leaves_a_private_torrent_with_its_metainfo_trackers() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let gid = paused_torrent_task(&mgr, "private-replay", true).await;
        let tid = mgr.torrent_ids.read().await[&gid];
        let engine = mgr.torrent_engine.read().await.clone().unwrap();
        {
            let mut tasks = mgr.tasks.write().await;
            let task = tasks.iter_mut().find(|t| t.gid == gid).unwrap();
            task.bt_announce_list = vec![vec!["http://b.example/announce".to_string()]];
            task.options.insert(
                "bt-tracker".into(),
                Value::String("http://b.example/announce".into()),
            );
        }

        replay_magnet_trackers(&engine, &mgr.tasks, &gid, tid).await;

        let tasks = mgr.tasks.read().await;
        let task = tasks.iter().find(|t| t.gid == gid).unwrap();
        assert_eq!(
            task.bt_announce_list,
            vec![vec!["http://own.example/announce".to_string()]]
        );
        assert_eq!(
            task.options.get("bt-tracker").and_then(Value::as_str),
            Some("http://own.example/announce")
        );
    }

    #[tokio::test]
    async fn magnet_tracker_replay_adds_the_listed_trackers_to_a_public_torrent() {
        let (mgr, _dir) = make_test_manager_with_engine().await;
        let gid = paused_torrent_task(&mgr, "public-replay", false).await;
        let tid = mgr.torrent_ids.read().await[&gid];
        let engine = mgr.torrent_engine.read().await.clone().unwrap();
        let listed = vec![vec!["http://b.example/announce".to_string()]];
        mgr.tasks
            .write()
            .await
            .iter_mut()
            .find(|t| t.gid == gid)
            .unwrap()
            .bt_announce_list = listed.clone();

        replay_magnet_trackers(&engine, &mgr.tasks, &gid, tid).await;

        assert_eq!(
            engine
                .add_trackers(tid, vec!["http://b.example/announce".to_string()])
                .await,
            Ok(0),
            "the replay already handed the tracker to the torrent"
        );
        let tasks = mgr.tasks.read().await;
        let task = tasks.iter().find(|t| t.gid == gid).unwrap();
        assert_eq!(task.bt_announce_list, listed);
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
    async fn update_task_waits_for_a_stopping_worker_of_a_paused_task() {
        let root = tempfile::TempDir::new().unwrap();
        let old_dir = root.path().join("old");
        let new_dir = root.path().join("new");
        std::fs::create_dir_all(&old_dir).unwrap();
        std::fs::create_dir_all(&new_dir).unwrap();
        let old_part = old_dir.join("file.bin.part");
        std::fs::write(&old_part, b"partial").unwrap();

        let mut task = DownloadTask::new_http(
            "move2".into(),
            vec!["https://a.example/file.bin".into()],
            old_dir.to_string_lossy().into_owned(),
            None,
            Map::new(),
        );
        task.out = "file.bin".into();
        task.status = TaskStatus::Paused;
        let mgr = make_test_manager(vec![task]);
        let flushed = old_part.clone();
        spawn_mock_worker(&mgr, "move2", async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            std::fs::write(&flushed, b"partial+flushed").unwrap();
        })
        .await
        .cancel();

        let outcome = mgr
            .update_task(
                "move2",
                TaskPatch {
                    dir: Some(new_dir.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )
            .await
            .expect("relocate");

        assert!(!outcome.restarted);
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

    async fn make_manager_with_slow_exiting_worker(
        gid: &str,
        dl_dir: &tempfile::TempDir,
    ) -> TaskManager {
        let mut task = DownloadTask::new_http(
            gid.into(),
            vec!["https://a.example/file.bin".into()],
            dl_dir.path().to_string_lossy().into_owned(),
            None,
            Map::new(),
        );
        task.out = "file.bin".into();
        task.status = TaskStatus::Active;
        let mgr = make_test_manager(vec![task]);
        spawn_mock_worker(&mgr, gid, async {
            tokio::time::sleep(Duration::from_millis(300)).await;
        })
        .await;
        mgr
    }

    #[tokio::test]
    async fn update_task_refuses_a_second_edit_while_the_first_restarts() {
        let dl_dir = tempfile::TempDir::new().unwrap();
        let mgr = make_manager_with_slow_exiting_worker("busy1", &dl_dir).await;

        let (first, second) = tokio::join!(
            mgr.update_task(
                "busy1",
                TaskPatch {
                    out: Some("first.bin".into()),
                    ..Default::default()
                },
            ),
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let mut opts = Map::new();
                opts.insert("split".into(), Value::from(8));
                mgr.update_task(
                    "busy1",
                    TaskPatch {
                        out: Some("second.bin".into()),
                        options: Some(opts),
                        ..Default::default()
                    },
                )
                .await
            }
        );

        let err = second.expect_err("second edit must be refused");
        assert!(err.contains("is being updated"), "err={err}");
        assert!(first.expect("first edit").restarted);
        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "busy1").unwrap();
        assert_eq!(t.out, "first.bin");
        assert!(t.options.get("split").is_none());
    }

    #[tokio::test]
    async fn update_task_restart_keeps_changes_made_while_the_worker_stops() {
        let dl_dir = tempfile::TempDir::new().unwrap();
        let mgr = make_manager_with_slow_exiting_worker("keep1", &dl_dir).await;

        let (outcome, ()) = tokio::join!(
            mgr.update_task(
                "keep1",
                TaskPatch {
                    out: Some("renamed.bin".into()),
                    ..Default::default()
                },
            ),
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let mut opts = Map::new();
                opts.insert("max-download-limit".into(), Value::String("12345".into()));
                mgr.change_option("keep1", opts)
                    .await
                    .expect("change option");
                let mut tasks = mgr.tasks.write().await;
                let t = tasks.iter_mut().find(|t| t.gid == "keep1").unwrap();
                t.completed_length = 4242;
            }
        );

        assert!(outcome.expect("edit").restarted);
        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "keep1").unwrap();
        assert_eq!(t.out, "renamed.bin");
        assert_eq!(
            t.options.get("max-download-limit"),
            Some(&Value::String("12345".into()))
        );
        assert_eq!(t.completed_length, 4242);
    }

    #[tokio::test]
    async fn update_task_restart_reports_a_task_that_finished_meanwhile() {
        let dl_dir = tempfile::TempDir::new().unwrap();
        let mgr = make_manager_with_slow_exiting_worker("done1", &dl_dir).await;

        let (outcome, ()) = tokio::join!(
            mgr.update_task(
                "done1",
                TaskPatch {
                    out: Some("renamed.bin".into()),
                    ..Default::default()
                },
            ),
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let mut tasks = mgr.tasks.write().await;
                let t = tasks.iter_mut().find(|t| t.gid == "done1").unwrap();
                t.status = TaskStatus::Complete;
            }
        );

        let err = outcome.expect_err("finished task must not be edited");
        assert!(err.contains("finished"), "err={err}");
        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "done1").unwrap();
        assert_eq!(t.out, "file.bin");
        assert_eq!(t.status, TaskStatus::Complete);
    }

    #[tokio::test]
    async fn update_task_restart_reports_a_task_removed_meanwhile() {
        let dl_dir = tempfile::TempDir::new().unwrap();
        let mgr = make_manager_with_slow_exiting_worker("gone1", &dl_dir).await;

        let (outcome, ()) = tokio::join!(
            mgr.update_task(
                "gone1",
                TaskPatch {
                    out: Some("renamed.bin".into()),
                    ..Default::default()
                },
            ),
            async {
                tokio::time::sleep(Duration::from_millis(50)).await;
                mgr.tasks.write().await.retain(|t| t.gid != "gone1");
            }
        );

        let err = outcome.expect_err("removed task must not be edited");
        assert!(err.contains("not found"), "err={err}");
        assert!(mgr.tasks.read().await.iter().all(|t| t.gid != "gone1"));
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

    #[test]
    fn stored_secure_cookies_are_sent_only_over_https() {
        use super::super::cookie_store::{CookieEntry, StoredCookie};
        let dir = tempfile::tempdir().unwrap();
        let store = CookieStore::new(dir.path());
        let cookie = |name: &str, value: &str, secure: bool| StoredCookie {
            name: name.into(),
            value: value.into(),
            secure,
            ..Default::default()
        };
        store
            .upsert(CookieEntry {
                host: "site.example".into(),
                cookies: vec![cookie("s", "1", true), cookie("p", "2", false)],
                ..Default::default()
            })
            .unwrap();
        let header = |uri: &str| {
            let mut merged = Map::new();
            TaskManager::apply_stored_cookies_from(&store, &[uri.to_string()], &mut merged);
            merged
                .get("cookie")
                .and_then(Value::as_str)
                .map(String::from)
        };

        assert_eq!(
            header("https://site.example/f").as_deref(),
            Some("s=1; p=2")
        );
        assert_eq!(
            header("HTTPS://site.example/f").as_deref(),
            Some("s=1; p=2")
        );
        assert_eq!(header("http://site.example/f").as_deref(), Some("p=2"));

        let http_variant = |uri: &str| {
            let mut merged = Map::new();
            TaskManager::apply_stored_cookies_from(&store, &[uri.to_string()], &mut merged);
            merged
                .get(crate::engine::http::COOKIE_HTTP_OPTION)
                .and_then(Value::as_str)
                .map(String::from)
        };
        assert_eq!(
            http_variant("https://site.example/f").as_deref(),
            Some("p=2")
        );
        assert_eq!(http_variant("http://site.example/f"), None);
    }

    #[test]
    fn changing_uris_drops_stale_mirror_stats() {
        let stat = |uri: &str| MirrorStat {
            uri: uri.into(),
            ..Default::default()
        };
        let mut task = make_task("m1", TaskStatus::Paused);
        task.uris = vec!["https://a.test/f".into(), "https://b.test/f".into()];
        task.mirror_progress = vec![stat("https://a.test/f"), stat("https://b.test/f")];

        let same = task.uris.clone();
        apply_task_patch(&mut task, Some(same), None, None, &[], None);
        assert_eq!(task.mirror_progress.len(), 2);

        let other = vec!["https://c.test/f".into(), "https://d.test/f".into()];
        apply_task_patch(&mut task, Some(other), None, None, &[], None);
        assert!(task.mirror_progress.is_empty());
    }

    #[tokio::test]
    async fn worker_finish_after_a_uri_edit_keeps_mirror_stats_cleared() {
        let stat = |uri: &str| MirrorStat {
            uri: uri.into(),
            ..Default::default()
        };
        let worker_uris: Vec<String> = vec!["https://a.test/f".into(), "https://b.test/f".into()];
        let tracker = crate::engine::http::MirrorTracker::with_stats(vec![
            stat("https://a.test/f"),
            stat("https://b.test/f"),
        ]);
        let mut task = make_task("m2", TaskStatus::Paused);
        task.uris = worker_uris.clone();

        let mut unedited = task.clone();
        settle_mirror_progress(&mut unedited, &worker_uris, &tracker);
        assert_eq!(unedited.mirror_progress.len(), 2);

        let manager = make_test_manager(vec![task]);
        let counters = Counters::new(0, 0);
        let worker_epoch = next_worker_epoch();
        manager
            .active_downloads
            .write()
            .await
            .insert("m2".into(), counters.to_active_plain(worker_epoch));
        {
            let mut tasks = manager.tasks.write().await;
            let edited = vec!["https://c.test/f".into(), "https://d.test/f".into()];
            apply_task_patch(&mut tasks[0], Some(edited), None, None, &[], None);
        }

        finish_task(
            &manager.tasks,
            &manager.active_downloads,
            &manager.events,
            "m2",
            worker_epoch,
            "http",
            &counters,
            Err("cancelled".into()),
            |task| settle_mirror_progress(task, &worker_uris, &tracker),
            |task, _| task.total_length,
            |_, error| classify_error(error, "http"),
        )
        .await;

        assert!(manager.tasks.read().await[0].mirror_progress.is_empty());
    }

    #[tokio::test]
    async fn retry_with_stored_cookies_drops_the_tasks_own_cookie() {
        let mut task = make_task("cf1", TaskStatus::Error);
        task.options
            .insert("cookie".into(), Value::String("old=1".into()));
        task.options.insert(
            crate::engine::http::COOKIE_HOST_OPTION.into(),
            Value::String("example.com".into()),
        );
        task.options
            .insert("user-agent".into(), Value::String("Old/1".into()));
        task.error_code = Some("403".into());
        task.error_message = Some("Forbidden".into());
        let mgr = make_test_manager(vec![task]);
        mgr.shutting_down.store(true, Ordering::Release);

        mgr.retry_with_stored_cookies("cf1", Some("New/2".into()))
            .await
            .expect("retry");

        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "cf1").unwrap();
        assert!(!t.options.contains_key("cookie"));
        assert!(!t
            .options
            .contains_key(crate::engine::http::COOKIE_HOST_OPTION));
        assert_eq!(
            t.options.get("user-agent").and_then(Value::as_str),
            Some("New/2")
        );
        assert!(t.error_code.is_none());
        assert!(t.error_message.is_none());
        assert_eq!(t.status, TaskStatus::Waiting);
    }

    #[tokio::test]
    async fn retry_with_stored_cookies_rejects_finished_and_non_http_tasks() {
        let mut done = make_task("done", TaskStatus::Complete);
        done.options
            .insert("cookie".into(), Value::String("kept=1".into()));
        let mut torrent = DownloadTask::new_torrent("bt".into(), "/dl".into(), None, Map::new());
        torrent.status = TaskStatus::Error;
        let mgr = make_test_manager(vec![done, make_task("gone", TaskStatus::Removed), torrent]);

        for (gid, expected) in [
            ("done", "Task done has already finished"),
            ("gone", "Task gone has already finished"),
            ("bt", "Task bt is not an HTTP task"),
            ("missing", "Task missing not found"),
        ] {
            let stored = mgr
                .retry_with_stored_cookies(gid, None)
                .await
                .expect_err(gid);
            let given = mgr
                .retry_with_cookies(gid, None, None)
                .await
                .expect_err(gid);
            assert_eq!(stored, expected);
            assert_eq!(stored, given);
        }

        let tasks = mgr.tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "done").unwrap();
        assert_eq!(
            t.options.get("cookie").and_then(Value::as_str),
            Some("kept=1")
        );
    }

    #[tokio::test]
    async fn usenet_progress_leaves_per_file_entries_alone() {
        let file = |name: &str, id: &str, bytes: u64| {
            format!(
                r#"<file poster="p" date="1" subject="&quot;{name}&quot; yEnc (1/1)"><groups><group>a.b</group></groups><segments><segment bytes="{bytes}" number="1">{id}</segment></segments></file>"#
            )
        };
        let nzb = format!(
            r#"<?xml version="1.0"?><nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">{}{}</nzb>"#,
            file("one.bin", "o@t", 1000),
            file("two.bin", "t@t", 500),
        );
        let mgr = make_test_manager(Vec::new());
        let mut options = Map::new();
        options.insert("pause".into(), Value::String("true".into()));
        let gid = mgr.add_nzb_task(nzb.into_bytes(), options).await.unwrap();
        mgr.tasks
            .write()
            .await
            .iter_mut()
            .find(|t| t.gid == gid)
            .unwrap()
            .status = TaskStatus::Active;
        let worker = fake_worker(next_worker_epoch());
        worker.total.store(1500, Ordering::Relaxed);
        worker.completed.store(700, Ordering::Relaxed);
        mgr.active_downloads
            .write()
            .await
            .insert(gid.clone(), worker);

        mgr.update_progress().await;

        let tasks = mgr.tasks.read().await;
        let task = tasks.iter().find(|t| t.gid == gid).unwrap();
        assert_eq!(task.total_length, 1500);
        assert_eq!(task.completed_length, 700);
        let lengths: Vec<_> = task
            .files
            .iter()
            .map(|f| (f.length.as_str(), f.completed_length.as_str()))
            .collect();
        assert_eq!(lengths, [("1000", "0"), ("500", "0")]);
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
    async fn removing_a_legacy_task_keeps_a_partial_another_task_owns() {
        let dir = tempfile::TempDir::new().unwrap();
        let dl = dir.path().to_string_lossy().into_owned();
        let owned = dir.path().join("bar.bin.part");
        let own = dir.path().join("foo.bin.part");
        for file in [&owned, &own] {
            std::fs::write(file, b"partial").unwrap();
        }
        let mut owner = DownloadTask::new_ed2k(
            "owner".into(),
            format!("ed2k://|file|bar.bin|1000|{ED2K_HASH}|/"),
            String::new(),
            1000,
            dl.clone(),
            None,
            Map::new(),
        );
        owner.status = TaskStatus::Paused;
        let mgr = make_test_manager(vec![
            owner,
            legacy_task(
                "gn",
                TaskKind::Gnutella,
                "gnutella://h:6346/?urn=urn:sha1:ABCDEF&dn=bar.bin&xl=10",
                &dl,
                TaskStatus::Error,
            ),
            legacy_task(
                "g2",
                TaskKind::G2,
                "g2://h:6346/sha1/ABCDEF?xl=10&dn=foo.bin",
                &dl,
                TaskStatus::Paused,
            ),
        ]);

        mgr.remove("gn").await.unwrap();
        mgr.remove("g2").await.unwrap();

        assert!(owned.exists());
        assert!(!own.exists());
    }

    #[tokio::test]
    async fn ed2k_partials_are_kept_while_another_task_targets_them() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("f.bin.part");
        let met = dir.path().join("f.bin.part.met");
        write_ed2k_partial(&dir.path().join("f.bin"), ED2K_HASH);
        let task = |gid: &str, status: TaskStatus| ed2k_task_in(dir.path(), gid, status);
        let mgr = make_test_manager(vec![
            task("a", TaskStatus::Paused),
            task("b", TaskStatus::Paused),
            task("c", TaskStatus::Error),
            task("d", TaskStatus::Error),
        ]);

        mgr.remove("a").await.unwrap();
        mgr.remove("b").await.unwrap();
        mgr.remove_download_result("c").await.unwrap();
        assert!(part.exists() && met.exists());

        mgr.purge_download_result().await;
        assert!(!part.exists() && !met.exists());
    }

    const ED2K_HASH: &str = "0123456789abcdef0123456789abcdef";
    const OTHER_ED2K_HASH: &str = "fedcba9876543210fedcba9876543210";

    fn ed2k_uri(hash: &str) -> String {
        format!("ed2k://|file|f.bin|1000|{hash}|/")
    }

    fn write_ed2k_partial(final_path: &Path, hash: &str) {
        use super::super::ed2k::partial::{met_path, part_path, save_met, Met};
        let link = super::super::ed2k::parse_ed2k_link(&ed2k_uri(hash)).unwrap();
        std::fs::write(part_path(final_path), b"partial").unwrap();
        let met = Met::new(link.file_hash_bytes, link.file_size, None, Vec::new());
        save_met(&met_path(final_path), &met).unwrap();
    }

    fn ed2k_task_in(dir: &Path, gid: &str, status: TaskStatus) -> DownloadTask {
        ed2k_task_for(dir, gid, ED2K_HASH, status)
    }

    fn ed2k_task_for(dir: &Path, gid: &str, hash: &str, status: TaskStatus) -> DownloadTask {
        let mut t = DownloadTask::new_ed2k(
            gid.into(),
            ed2k_uri(hash),
            "f.bin".into(),
            1000,
            dir.to_string_lossy().into(),
            None,
            Map::new(),
        );
        t.status = status;
        t
    }

    fn fake_worker(epoch: u64) -> ActiveDownload {
        Counters::new(0, 0).to_active_plain(epoch)
    }

    #[tokio::test]
    async fn removing_a_renamed_ed2k_task_cleans_its_own_partial_only() {
        let dir = tempfile::tempdir().unwrap();
        write_ed2k_partial(&dir.path().join("f.bin"), ED2K_HASH);
        write_ed2k_partial(&dir.path().join("f.1.bin"), ED2K_HASH);
        let mut renamed = ed2k_task_in(dir.path(), "a", TaskStatus::Paused);
        renamed.out = "f.1.bin".into();
        let mgr = make_test_manager(vec![renamed]);

        mgr.remove("a").await.unwrap();
        assert!(!dir.path().join("f.1.bin.part").exists());
        assert!(!dir.path().join("f.1.bin.part.met").exists());
        assert!(dir.path().join("f.bin.part").exists());
        assert!(dir.path().join("f.bin.part.met").exists());
    }

    #[tokio::test]
    async fn removing_an_ed2k_task_deletes_only_partials_its_link_owns() {
        let dir = tempfile::tempdir().unwrap();
        let fin = dir.path().join("f.bin");
        let part = dir.path().join("f.bin.part");

        write_ed2k_partial(&fin, ED2K_HASH);
        let mgr = make_test_manager(vec![
            ed2k_task_for(dir.path(), "owner", ED2K_HASH, TaskStatus::Paused),
            ed2k_task_for(dir.path(), "queued", OTHER_ED2K_HASH, TaskStatus::Waiting),
        ]);
        mgr.remove("owner").await.unwrap();
        assert!(!part.exists());

        write_ed2k_partial(&fin, OTHER_ED2K_HASH);
        let mgr = make_test_manager(vec![
            ed2k_task_for(dir.path(), "queued", ED2K_HASH, TaskStatus::Waiting),
            ed2k_task_for(dir.path(), "owner", OTHER_ED2K_HASH, TaskStatus::Paused),
        ]);
        mgr.remove("queued").await.unwrap();
        mgr.remove_download_result("queued").await.unwrap();
        assert!(part.exists());
    }

    #[tokio::test]
    async fn moving_an_ed2k_task_carries_its_sidecar() {
        let root = tempfile::tempdir().unwrap();
        let (old_dir, new_dir) = (root.path().join("old"), root.path().join("new"));
        std::fs::create_dir_all(&old_dir).unwrap();
        write_ed2k_partial(&old_dir.join("f.bin"), ED2K_HASH);
        let mgr = make_test_manager(vec![ed2k_task_in(&old_dir, "a", TaskStatus::Paused)]);

        let outcome = mgr
            .update_task(
                "a",
                TaskPatch {
                    dir: Some(new_dir.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(outcome.progress_preserved);
        assert!(new_dir.join("f.bin.part").exists());
        assert!(new_dir.join("f.bin.part.met").exists());
        assert!(!old_dir.join("f.bin.part.met").exists());
        use super::super::ed2k::partial::PathClaim;
        PathClaim::acquire(&old_dir.join("f.bin")).unwrap();
        PathClaim::acquire(&new_dir.join("f.bin")).unwrap();
    }

    #[tokio::test]
    async fn same_path_ed2k_edit_does_not_adopt_another_links_partial() {
        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("d");
        std::fs::create_dir_all(&dir).unwrap();
        write_ed2k_partial(&dir.join("f.bin"), OTHER_ED2K_HASH);
        let mgr = make_test_manager(vec![ed2k_task_in(&dir, "a", TaskStatus::Paused)]);

        let outcome = mgr
            .update_task(
                "a",
                TaskPatch {
                    dir: Some(format!("{}/", dir.to_string_lossy())),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(!outcome.progress_preserved);
        assert!(dir.join("f.bin.part").exists());
        assert!(dir.join("f.bin.part.met").exists());
    }

    #[tokio::test]
    async fn moving_an_ed2k_task_onto_a_claimed_path_leaves_the_partial() {
        let root = tempfile::tempdir().unwrap();
        let (old_dir, new_dir) = (root.path().join("old"), root.path().join("new"));
        std::fs::create_dir_all(&old_dir).unwrap();
        write_ed2k_partial(&old_dir.join("f.bin"), ED2K_HASH);
        let _held =
            super::super::ed2k::partial::PathClaim::acquire(&new_dir.join("f.bin")).unwrap();
        let mgr = make_test_manager(vec![ed2k_task_in(&old_dir, "a", TaskStatus::Paused)]);

        let outcome = mgr
            .update_task(
                "a",
                TaskPatch {
                    dir: Some(new_dir.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(!outcome.progress_preserved);
        assert!(old_dir.join("f.bin.part").exists());
        assert!(old_dir.join("f.bin.part.met").exists());
        assert!(!new_dir.join("f.bin.part").exists());
        assert!(!new_dir.join("f.bin.part.met").exists());
    }

    #[tokio::test]
    async fn ed2k_partials_are_kept_while_a_removed_worker_unwinds() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("f.bin.part");
        std::fs::write(&part, b"partial").unwrap();
        let mgr = make_test_manager(vec![
            ed2k_task_in(dir.path(), "a", TaskStatus::Removed),
            ed2k_task_in(dir.path(), "b", TaskStatus::Paused),
        ]);
        mgr.active_downloads
            .write()
            .await
            .insert("a".into(), fake_worker(next_worker_epoch()));

        mgr.remove("b").await.unwrap();
        assert!(part.exists());

        mgr.active_downloads.write().await.remove("a");
        mgr.purge_download_result().await;
        assert!(!part.exists());
    }

    #[tokio::test]
    async fn removing_an_ed2k_task_waits_for_its_worker_before_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("f.bin.part");
        std::fs::write(&part, b"partial").unwrap();
        let mgr = make_test_manager(vec![ed2k_task_in(dir.path(), "a", TaskStatus::Active)]);
        let worker = fake_worker(next_worker_epoch());
        let cancel = worker.cancel_token.clone();
        mgr.active_downloads
            .write()
            .await
            .insert("a".into(), worker);
        let active = mgr.active_downloads.clone();
        tokio::spawn(async move {
            cancel.cancelled().await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            active.write().await.remove("a");
        });

        mgr.remove("a").await.unwrap();
        assert!(!part.exists());
    }

    #[tokio::test]
    async fn moving_an_ed2k_task_leaves_a_shared_partial_in_place() {
        let root = tempfile::tempdir().unwrap();
        let (old_dir, new_dir) = (root.path().join("old"), root.path().join("new"));
        std::fs::create_dir_all(&old_dir).unwrap();
        std::fs::create_dir_all(&new_dir).unwrap();
        std::fs::write(old_dir.join("f.bin.part"), b"partial").unwrap();
        let task = |gid: &str| ed2k_task_in(&old_dir, gid, TaskStatus::Paused);
        let mgr = make_test_manager(vec![task("a"), task("b")]);

        let outcome = mgr
            .update_task(
                "a",
                TaskPatch {
                    dir: Some(new_dir.to_string_lossy().into_owned()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(!outcome.progress_preserved);
        assert!(old_dir.join("f.bin.part").exists());
        assert!(!new_dir.join("f.bin.part").exists());
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

    #[tokio::test(start_paused = true)]
    async fn update_task_leaves_the_partial_when_a_paused_task_worker_does_not_stop() {
        let root = tempfile::TempDir::new().unwrap();
        let old_dir = root.path().join("old");
        let new_dir = root.path().join("new");
        std::fs::create_dir_all(&old_dir).unwrap();
        std::fs::create_dir_all(&new_dir).unwrap();
        let old_part = old_dir.join("file.bin.part");
        std::fs::write(&old_part, b"partial").unwrap();

        let mut task = DownloadTask::new_http(
            "stuck2".into(),
            vec!["https://a.example/file.bin".into()],
            old_dir.to_string_lossy().into_owned(),
            None,
            Map::new(),
        );
        task.out = "file.bin".into();
        task.status = TaskStatus::Paused;
        let mgr = make_test_manager(vec![task]);
        let tasks = mgr.tasks.clone();
        let counters = Counters::new(0, 0);
        mgr.active_downloads.write().await.insert(
            "stuck2".into(),
            counters.to_active(
                next_worker_epoch(),
                Vec::new(),
                Arc::new(parking_lot::Mutex::new(None)),
            ),
        );

        let target = new_dir.to_string_lossy().into_owned();
        let update = tokio::spawn(async move {
            mgr.update_task(
                "stuck2",
                TaskPatch {
                    dir: Some(target),
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

        let outcome = update.await.expect("join").expect("the edit still applies");
        assert!(!outcome.restarted);
        assert!(!outcome.progress_preserved);
        assert_eq!(std::fs::read(&old_part).unwrap(), b"partial");
        assert!(!new_dir.join("file.bin.part").exists());
        let tasks = tasks.read().await;
        let t = tasks.iter().find(|t| t.gid == "stuck2").unwrap();
        assert_eq!(t.dir, new_dir.to_string_lossy());
        assert_eq!(t.status, TaskStatus::Paused);
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

    #[tokio::test]
    async fn saved_torrent_files_are_written_atomically() {
        let dir = tempfile::TempDir::new().unwrap();
        let mgr = make_test_manager_in(&dir, vec![]);
        let info_hash = "0123456789abcdef0123456789abcdef01234567";
        let data = b"d4:infod4:name4:testee".to_vec();

        mgr.persist_torrent_file(info_hash, &data).await;

        let path = mgr.saved_torrent_path(info_hash).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), data);
        let entries: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .collect();
        assert_eq!(entries, vec![path]);
    }
}
