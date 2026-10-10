use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::StreamExt;
use risuko_http::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT_ENCODING, ACCEPT_RANGES, CONTENT_ENCODING,
    CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_MATCH, IF_UNMODIFIED_SINCE,
    LAST_MODIFIED, RANGE, TRANSFER_ENCODING,
};
use risuko_http::Client;
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

use super::options::json_bool;
use super::speed_limiter::{parse_speed_limit, SpeedEma, SpeedLimiter, MIN_EMA_SAMPLE_SECS};
use risuko_bt::limiter::Throttle;

const PART_SUFFIX: &str = ".part";
const DEFAULT_MIN_SPLIT_SIZE: u64 = 1024 * 1024;
const CHUNK_MAX_RETRIES: u32 = 5;
pub const PIECE_SIZE: u64 = 1024 * 1024;
const META_VERSION: u32 = 2;
const MAX_PIECE_SIZE: u64 = 16 * 1024 * 1024;
const META_SAVE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(2);
const PARTIAL_ERROR_BACKOFF_STEP: std::time::Duration = std::time::Duration::from_millis(50);
const PARTIAL_ERROR_BACKOFF_MAX: std::time::Duration = std::time::Duration::from_millis(250);
const STALE_PART_REMOVED: &str = "stale partial file removed";
use super::CHUNK_META_SUFFIX;

#[derive(Clone, Copy)]
struct ChunkRange {
    start: u64,
    end: u64,
}

impl ChunkRange {
    fn new(start: u64, end: u64) -> Self {
        debug_assert!(end >= start);
        Self { start, end }
    }

    fn to_range_header_value(self) -> String {
        format!("bytes={}-{}", self.start, self.end)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PieceProgress {
    i: u32,
    c: u32,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ChunkMeta {
    version: u32,
    content_length: u64,
    piece_size: u32,
    etag: Option<String>,
    #[serde(default)]
    last_modified: Option<String>,
    pieces: Vec<PieceProgress>,
}

fn etags_strongly_equal(a: &str, b: &str) -> bool {
    fn is_strong(tag: &str) -> bool {
        let bytes = tag.as_bytes();
        bytes.len() >= 2
            && bytes.first() == Some(&b'"')
            && bytes.last() == Some(&b'"')
            && !tag.starts_with("W/")
    }

    is_strong(a) && is_strong(b) && a == b
}

fn is_strong_etag(tag: &str) -> bool {
    etags_strongly_equal(tag, tag)
}

fn has_resume_validator(etag: &Option<String>, last_modified: &Option<String>) -> bool {
    etag.as_deref().is_some_and(is_strong_etag) || last_modified.is_some()
}

fn choose_piece_size(content_length: u64, workers: usize) -> u64 {
    let target = content_length / (workers.max(1) as u64).saturating_mul(4);
    (target / PIECE_SIZE * PIECE_SIZE).clamp(PIECE_SIZE, MAX_PIECE_SIZE)
}

fn chunk_meta_path(part_path: &Path) -> PathBuf {
    let mut p = part_path.as_os_str().to_owned();
    p.push(CHUNK_META_SUFFIX);
    PathBuf::from(p)
}

fn part_file_path(dir: &Path, out: &str) -> PathBuf {
    let out = sanitize_filename(out);
    let name = if out.ends_with(PART_SUFFIX) {
        out
    } else {
        format!("{out}{PART_SUFFIX}")
    };
    dir.join(name)
}

pub fn relocate_partial(
    old_dir: &str,
    old_out: &str,
    new_dir: &str,
    new_out: &str,
) -> Result<bool, String> {
    if old_out.trim().is_empty() || new_out.trim().is_empty() {
        return Err("Cannot relocate partial: output name is empty".to_string());
    }
    if old_dir == new_dir && old_out == new_out {
        return Ok(false);
    }
    let old_part = part_file_path(Path::new(old_dir), old_out);
    let new_part = part_file_path(Path::new(new_dir), new_out);
    if !old_part.exists() {
        return Ok(false);
    }
    if old_part == new_part {
        return Ok(true);
    }
    let old_meta = chunk_meta_path(&old_part);
    let new_meta = chunk_meta_path(&new_part);
    if new_part.exists() || new_meta.exists() {
        let dest = if new_part.exists() {
            new_part.display().to_string()
        } else {
            new_meta.display().to_string()
        };
        return Err(format!(
            "Cannot relocate partial: destination already exists ({dest})"
        ));
    }
    if let Some(parent) = new_part.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create destination dir {}: {e}", parent.display()))?;
    }
    relocate_file_pair(&old_part, &new_part, &old_meta, &new_meta)?;
    Ok(true)
}

fn is_cross_device_error(e: &std::io::Error) -> bool {
    if e.kind() == std::io::ErrorKind::CrossesDevices {
        return true;
    }
    match e.raw_os_error() {
        #[cfg(unix)]
        Some(18) => true,
        #[cfg(windows)]
        Some(17) => true,
        _ => false,
    }
}

fn sync_path(path: &Path) -> Result<(), String> {
    let file = fs::File::open(path)
        .map_err(|e| format!("Failed to open {} for sync: {e}", path.display()))?;
    file.sync_data()
        .map_err(|e| format!("Failed to sync {}: {e}", path.display()))
}

fn sync_directory(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        let directory = fs::File::open(path)
            .map_err(|e| format!("Failed to open {} for sync: {e}", path.display()))?;
        directory
            .sync_all()
            .map_err(|e| format!("Failed to sync directory {}: {e}", path.display()))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

fn copy_file_durable(src: &Path, dst: &Path, what: &str) -> Result<(), String> {
    fs::copy(src, dst).map_err(|e| {
        format!(
            "Failed to copy {what} {} -> {}: {e}",
            src.display(),
            dst.display()
        )
    })?;
    sync_path(dst)
}

fn relocate_file_pair(
    old_part: &Path,
    new_part: &Path,
    old_meta: &Path,
    new_meta: &Path,
) -> Result<(), String> {
    let meta_exists = old_meta.exists();
    match fs::rename(old_part, new_part) {
        Ok(()) => {
            if meta_exists {
                if let Err(e) = fs::rename(old_meta, new_meta) {
                    let _ = fs::rename(new_part, old_part);
                    return Err(format!(
                        "Failed to move chunk meta {}: {e}",
                        old_meta.display()
                    ));
                }
            }
            Ok(())
        }
        Err(e) if is_cross_device_error(&e) => {
            copy_file_durable(old_part, new_part, "partial").inspect_err(|_| {
                let _ = fs::remove_file(new_part);
            })?;
            if meta_exists {
                if let Err(meta_err) = copy_file_durable(old_meta, new_meta, "chunk meta") {
                    let _ = fs::remove_file(new_meta);
                    let _ = fs::remove_file(new_part);
                    return Err(meta_err);
                }
            }
            if let Some(parent) = new_part.parent() {
                if let Err(e) = sync_directory(parent) {
                    let _ = fs::remove_file(new_part);
                    if meta_exists {
                        let _ = fs::remove_file(new_meta);
                    }
                    return Err(e);
                }
            }
            if let Err(e) = fs::remove_file(old_part) {
                tracing::warn!(
                    "Failed to remove source partial {}: {e}",
                    old_part.display()
                );
            }
            if meta_exists {
                if let Err(e) = fs::remove_file(old_meta) {
                    tracing::warn!(
                        "Failed to remove source chunk meta {}: {e}",
                        old_meta.display()
                    );
                }
            }
            Ok(())
        }
        Err(e) => Err(format!(
            "Failed to move partial {}: {e}",
            old_part.display()
        )),
    }
}

pub fn sanitize_filename(name: &str) -> String {
    let base = Path::new(name)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "download".to_string());
    if base.is_empty() || base == "." || base == ".." {
        "download".to_string()
    } else if cfg!(windows) {
        super::util::safe_filename(&base, "download")
    } else {
        base
    }
}

fn save_piece_meta(
    part_path: &Path,
    queue: &PieceQueue,
    content_length: u64,
    etag: &Option<String>,
    last_modified: &Option<String>,
) {
    if !has_resume_validator(etag, last_modified) {
        return;
    }
    let pieces: Vec<PieceProgress> = queue
        .pieces
        .iter()
        .enumerate()
        .filter_map(|(i, p)| {
            let c = p.completed.load(Ordering::Relaxed);
            if c == 0 {
                None
            } else {
                Some(PieceProgress { i: i as u32, c })
            }
        })
        .collect();
    let meta = ChunkMeta {
        version: META_VERSION,
        content_length,
        piece_size: queue.piece_size as u32,
        etag: etag.clone(),
        last_modified: last_modified.clone(),
        pieces,
    };
    let path = chunk_meta_path(part_path);
    if let Ok(json) = serde_json::to_string(&meta) {
        let _ = crate::traits::write_file_atomically(&path, json.as_bytes());
    }
}

fn load_piece_meta(
    part_path: &Path,
    content_length: u64,
    etag: &Option<String>,
    last_modified: &Option<String>,
) -> Option<ChunkMeta> {
    let path = chunk_meta_path(part_path);
    let data = fs::read_to_string(&path).ok()?;
    let mut meta: ChunkMeta = serde_json::from_str(&data).ok()?;
    if meta.version != META_VERSION
        || meta.content_length != content_length
        || !(PIECE_SIZE..=MAX_PIECE_SIZE).contains(&u64::from(meta.piece_size))
    {
        let _ = fs::remove_file(&path);
        return None;
    }

    let file_len = match fs::metadata(part_path) {
        Ok(m) if m.len() <= content_length => m.len(),
        _ => {
            let _ = fs::remove_file(&path);
            return None;
        }
    };

    let saved_strong = meta.etag.as_deref().is_some_and(is_strong_etag);
    let current_strong = etag.as_deref().is_some_and(is_strong_etag);
    let validators_match = match (saved_strong, current_strong) {
        (true, true) => matches!(
            (&meta.etag, etag),
            (Some(saved), Some(current)) if etags_strongly_equal(saved, current)
        ),
        (false, false) => {
            matches!((&meta.last_modified, last_modified), (Some(a), Some(b)) if a == b)
        }
        _ => false,
    };
    if !validators_match {
        let _ = fs::remove_file(&path);
        return None;
    }

    let piece_size = u64::from(meta.piece_size);
    for pp in &mut meta.pieces {
        let offset = u64::from(pp.i) * piece_size;
        let on_disk = file_len.saturating_sub(offset).min(u64::from(u32::MAX)) as u32;
        pp.c = pp.c.min(on_disk);
    }
    meta.pieces.retain(|pp| pp.c > 0);

    Some(meta)
}

fn delete_chunk_meta(part_path: &Path) {
    let path = chunk_meta_path(part_path);
    let _ = fs::remove_file(&path);
    let mut tmp = path.into_os_string();
    tmp.push(".tmp");
    let _ = fs::remove_file(tmp);
}

const PIECE_FREE: u8 = 0;
const PIECE_INFLIGHT: u8 = 1;
const PIECE_DONE: u8 = 2;

struct Piece {
    offset: u64,
    length: u32,
    completed: Arc<AtomicU32>,
    state: AtomicU8,
}

struct PieceQueue {
    pieces: Vec<Piece>,
    piece_size: u64,
    next_hint: AtomicUsize,
    changed: tokio::sync::Notify,
}

impl PieceQueue {
    #[cfg(test)]
    fn new(content_length: u64) -> Self {
        Self::with_piece_size(content_length, PIECE_SIZE)
    }

    fn with_piece_size(content_length: u64, piece_size: u64) -> Self {
        debug_assert!(content_length > 0);
        let n = content_length.div_ceil(piece_size) as usize;
        let mut pieces = Vec::with_capacity(n);
        for i in 0..n {
            let offset = i as u64 * piece_size;
            let length = std::cmp::min(piece_size, content_length - offset) as u32;
            pieces.push(Piece {
                offset,
                length,
                completed: Arc::new(AtomicU32::new(0)),
                state: AtomicU8::new(PIECE_FREE),
            });
        }
        Self {
            pieces,
            piece_size,
            next_hint: AtomicUsize::new(0),
            changed: tokio::sync::Notify::new(),
        }
    }

    fn claim_next(&self) -> Option<usize> {
        let n = self.pieces.len();
        if n == 0 {
            return None;
        }
        let start = self.next_hint.load(Ordering::Relaxed) % n;
        for di in 0..n {
            let i = (start + di) % n;
            let p = &self.pieces[i];
            if p.state
                .compare_exchange(
                    PIECE_FREE,
                    PIECE_INFLIGHT,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                self.next_hint.store(i + 1, Ordering::Relaxed);
                return Some(i);
            }
        }
        None
    }

    fn release(&self, idx: usize) {
        self.pieces[idx].state.store(PIECE_FREE, Ordering::Release);
        let cur = self.next_hint.load(Ordering::Relaxed);
        if idx < cur {
            self.next_hint.store(idx, Ordering::Relaxed);
        }
        self.changed.notify_waiters();
    }

    fn has_inflight(&self) -> bool {
        self.pieces
            .iter()
            .any(|p| p.state.load(Ordering::Acquire) == PIECE_INFLIGHT)
    }

    fn has_free(&self) -> bool {
        self.pieces
            .iter()
            .any(|p| p.state.load(Ordering::Acquire) == PIECE_FREE)
    }

    fn complete(&self, idx: usize) {
        self.pieces[idx].state.store(PIECE_DONE, Ordering::Release);
        self.changed.notify_waiters();
    }

    async fn wait_for_change(&self) {
        let changed = self.changed.notified();
        tokio::pin!(changed);
        // Register before re-checking so a release in between is not missed
        changed.as_mut().enable();
        if self.has_inflight() && !self.has_free() {
            changed.await;
        }
    }

    fn is_finished(&self) -> bool {
        self.pieces
            .iter()
            .all(|p| p.state.load(Ordering::Acquire) == PIECE_DONE)
    }
}

const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 60;
const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 60;
const DEFAULT_NZB_BODY_TIMEOUT_SECS: u64 = 30;
const DEFAULT_LOWEST_SPEED_TIMEOUT_SECS: u64 = 30;

#[derive(Clone)]
struct StallWatchdog {
    lowest_speed: u64,
    timeout: std::time::Duration,
    flag: Arc<AtomicBool>,
}

impl StallWatchdog {
    fn from_options(options: &Map<String, Value>) -> Self {
        let lowest_speed = options
            .get("lowest-speed-limit")
            .map(parse_speed_limit)
            .unwrap_or(0);
        let timeout = parse_duration_secs_option(
            options.get("lowest-speed-limit-timeout"),
            DEFAULT_LOWEST_SPEED_TIMEOUT_SECS,
        );
        Self {
            lowest_speed,
            timeout,
            flag: Arc::new(AtomicBool::new(false)),
        }
    }
}

fn parse_duration_secs_option(v: Option<&Value>, default: u64) -> std::time::Duration {
    let secs = v
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
        })
        .unwrap_or(default);
    std::time::Duration::from_secs(secs)
}

fn parse_whole_checksum_option(
    options: &Map<String, Value>,
) -> Result<Option<super::hasher::WholeChecksum>, String> {
    match options.get("checksum").and_then(Value::as_str) {
        Some(s) if !s.trim().is_empty() => super::hasher::WholeChecksum::parse(s.trim()).map(Some),
        _ => Ok(None),
    }
}

fn parse_piece_checksums_option(
    options: &Map<String, Value>,
) -> Result<Option<super::hasher::PieceChecksums>, String> {
    match options.get("piece-checksums").and_then(Value::as_str) {
        Some(s) if !s.trim().is_empty() => super::hasher::PieceChecksums::parse(s.trim()).map(Some),
        _ => Ok(None),
    }
}

async fn verify_whole_file(
    path: &Path,
    expected: &super::hasher::WholeChecksum,
) -> Result<(), String> {
    let path = path.to_path_buf();
    let expected = expected.clone();
    tokio::task::spawn_blocking(move || -> Result<(), String> {
        use std::io::Read;
        let mut f = std::fs::File::open(&path).map_err(|e| format!("open for verify: {e}"))?;
        let mut hasher = super::hasher::Hasher::new(expected.algo);
        let mut buf = vec![0u8; 1024 * 1024];
        loop {
            let n = f
                .read(&mut buf)
                .map_err(|e| format!("read for verify: {e}"))?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let got = hasher.finalize_hex();
        if expected.matches(&got) {
            Ok(())
        } else {
            Err(format!(
                "checksum mismatch ({}): expected {}, got {}",
                expected.algo.name(),
                expected.hex,
                got
            ))
        }
    })
    .await
    .map_err(|e| format!("verify task panicked: {e}"))?
}

async fn verify_piece_checksums(
    path: &Path,
    content_length: u64,
    expected: &super::hasher::PieceChecksums,
) -> Result<(), String> {
    let expected_count = content_length.div_ceil(PIECE_SIZE) as usize;
    if expected.hexes.len() != expected_count {
        return Err(format!(
            "piece-checksums count mismatch: got {} hashes for {} pieces",
            expected.hexes.len(),
            expected_count
        ));
    }
    let path = path.to_path_buf();
    let expected = expected.clone();
    tokio::task::spawn_blocking(move || -> Result<(), String> {
        use std::io::Read;
        let mut f = std::fs::File::open(&path).map_err(|e| format!("open for verify: {e}"))?;
        let piece_size = PIECE_SIZE as usize;
        let mut buf = vec![0u8; piece_size];
        for (i, want) in expected.hexes.iter().enumerate() {
            let offset = i as u64 * PIECE_SIZE;
            let remaining = content_length.saturating_sub(offset) as usize;
            let take = remaining.min(piece_size);
            f.read_exact(&mut buf[..take])
                .map_err(|e| format!("read piece {i}: {e}"))?;
            let mut hasher = super::hasher::Hasher::new(expected.algo);
            hasher.update(&buf[..take]);
            let got = hasher.finalize_hex();
            if !want.eq_ignore_ascii_case(&got) {
                return Err(format!(
                    "piece {} checksum mismatch ({}): expected {}, got {}",
                    i,
                    expected.algo.name(),
                    want,
                    got
                ));
            }
        }
        Ok(())
    })
    .await
    .map_err(|e| format!("verify task panicked: {e}"))?
}

async fn verify_output(
    path: &Path,
    content_length: u64,
    piece_checksums: &Option<super::hasher::PieceChecksums>,
    whole_checksum: &Option<super::hasher::WholeChecksum>,
) -> Result<(), String> {
    if let Some(expected) = piece_checksums {
        if let Err(e) = verify_piece_checksums(path, content_length, expected).await {
            tracing::warn!("piece checksum failed, deleting output: {e}");
            let _ = fs::remove_file(path);
            return Err(e);
        }
    }
    if let Some(expected) = whole_checksum {
        if let Err(e) = verify_whole_file(path, expected).await {
            tracing::warn!("integrity check failed, deleting output: {e}");
            let _ = fs::remove_file(path);
            return Err(e);
        }
    }
    Ok(())
}

pub async fn fetch_for_metalink_probe(
    uri: &str,
    options: &Map<String, Value>,
) -> Result<Vec<u8>, String> {
    const CAP: u64 = 4 * 1024 * 1024;
    const PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    tokio::time::timeout(
        PROBE_TIMEOUT,
        fetch_capped(uri, options, CAP, "metalink probe", PROBE_TIMEOUT),
    )
    .await
    .map_err(|_| "metalink probe timed out".to_string())?
}

pub async fn fetch_for_nzb(uri: &str, options: &Map<String, Value>) -> Result<Vec<u8>, String> {
    const CAP: u64 = 16 * 1024 * 1024;
    let body_timeout = parse_duration_secs_option(
        options.get("nzb-body-timeout"),
        DEFAULT_NZB_BODY_TIMEOUT_SECS,
    );
    fetch_capped(uri, options, CAP, "NZB URL", body_timeout).await
}

async fn fetch_capped(
    uri: &str,
    options: &Map<String, Value>,
    cap: u64,
    what: &str,
    body_timeout: std::time::Duration,
) -> Result<Vec<u8>, String> {
    let client = build_client(options, true, false, load_cookie_jar(options))?;
    let mut headers = build_headers(options);
    apply_netrc_auth(&mut headers, uri, options);
    let header_timeout =
        parse_duration_secs_option(options.get("connect-timeout"), DEFAULT_CONNECT_TIMEOUT_SECS);
    let response = tokio::time::timeout(header_timeout, client.get(uri).headers(headers).send())
        .await
        .map_err(|_| format!("{what} response timed out"))?
        .map_err(|e| format!("{what} fetch failed: {e}"))?
        .error_for_status()
        .map_err(|e| format!("{what} returned an error: {e}"))?;

    if response.content_length().is_some_and(|length| length > cap) {
        return Err(format!("{what} payload too large"));
    }

    let mut bytes = Vec::with_capacity(response.content_length().unwrap_or(0).min(cap) as usize);
    let mut stream = response.bytes_stream();
    let mut total = 0u64;
    loop {
        let item = tokio::time::timeout(body_timeout, stream.next())
            .await
            .map_err(|_| format!("{what} response body timed out"))?;
        let Some(item) = item else { break };
        let chunk = item.map_err(|e| format!("{what} body read failed: {e}"))?;
        total = total.saturating_add(chunk.len() as u64);
        if total > cap {
            return Err(format!("{what} payload too large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn build_client(
    options: &Map<String, Value>,
    decompress: bool,
    http1_only: bool,
    cookie_jar: Option<std::sync::Arc<risuko_http::Jar>>,
) -> Result<Client, String> {
    let ua = options
        .get("user-agent")
        .and_then(|v| v.as_str())
        .unwrap_or("Mozilla/5.0");

    let connect_timeout =
        parse_duration_secs_option(options.get("connect-timeout"), DEFAULT_CONNECT_TIMEOUT_SECS);

    let mut builder = Client::builder()
        .user_agent(ua)
        .redirect(risuko_http::redirect::Policy::limited(10))
        .connect_timeout(connect_timeout)
        .tcp_nodelay(true)
        .http1_only(http1_only)
        .tcp_keepalive(std::time::Duration::from_secs(60))
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .pool_max_idle_per_host(64);

    if decompress {
        builder = builder.gzip(true).brotli(true).deflate(true);
    }

    if let Some(proxy_url) = options
        .get("all-proxy")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let proxy = risuko_http::Proxy::all(proxy_url)
            .map_err(|e| format!("Invalid configured proxy: {e}"))?;
        builder = builder.proxy(proxy);
    }

    if let Some(no_proxy) = options
        .get("no-proxy")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let matcher = risuko_http::NoProxy::parse(no_proxy);
        if !matcher.is_empty() {
            builder = builder.no_proxy(matcher);
        }
    }

    if let Some(jar) = cookie_jar {
        builder = builder.cookie_provider(jar);
    }

    builder
        .build()
        .map_err(|e| format!("Failed to build HTTP client: {e}"))
}

pub(crate) fn load_cookie_jar(
    options: &Map<String, Value>,
) -> Option<std::sync::Arc<risuko_http::Jar>> {
    let cookies_path = options
        .get("load-cookies")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())?;
    let f = match std::fs::File::open(cookies_path) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!("Failed to open cookie file {cookies_path}: {e}");
            return None;
        }
    };
    let jar = std::sync::Arc::new(risuko_http::Jar::new());
    match jar.load_netscape(f) {
        Ok(n) => {
            tracing::info!("Loaded {n} cookies from {cookies_path}");
            Some(jar)
        }
        Err(e) => {
            tracing::warn!("Failed to parse cookie file {cookies_path}: {e}");
            None
        }
    }
}

pub(crate) fn build_headers(options: &Map<String, Value>) -> HeaderMap {
    let mut headers = HeaderMap::new();

    if let Some(header_val) = options.get("header") {
        let lines: Vec<&str> = if let Some(s) = header_val.as_str() {
            s.split('\n').collect()
        } else if let Some(arr) = header_val.as_array() {
            arr.iter().filter_map(|v| v.as_str()).collect()
        } else {
            Vec::new()
        };
        for h in lines {
            let trimmed = h.trim();
            if let Some(colon) = trimmed.find(':') {
                let name = trimmed[..colon].trim();
                let value = trimmed[colon + 1..].trim();
                if let (Ok(n), Ok(v)) = (
                    HeaderName::from_bytes(name.as_bytes()),
                    HeaderValue::from_str(value),
                ) {
                    headers.insert(n, v);
                }
            }
        }
    }

    if let Some(referer) = options
        .get("referer")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        if let Ok(v) = HeaderValue::from_str(referer) {
            headers.insert(risuko_http::header::REFERER, v);
        }
    }

    if !headers.contains_key(risuko_http::header::USER_AGENT) {
        if let Some(user_agent) = options
            .get("user-agent")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            if let Ok(v) = HeaderValue::from_str(user_agent) {
                headers.insert(risuko_http::header::USER_AGENT, v);
            }
        }
    }

    if let Some(cookie) = options
        .get("cookie")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
    {
        if let Ok(v) = HeaderValue::from_str(cookie) {
            headers.insert(risuko_http::header::COOKIE, v);
        }
    }

    headers
}

pub(crate) fn apply_netrc_auth(headers: &mut HeaderMap, uri: &str, options: &Map<String, Value>) {
    if headers.contains_key(risuko_http::header::AUTHORIZATION) {
        return;
    }
    if options.get("no-netrc").and_then(json_bool).unwrap_or(false) {
        return;
    }
    let parsed = match url::Url::parse(uri) {
        Ok(u) => u,
        Err(_) => return,
    };
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return;
    }
    let host = match parsed.host_str() {
        Some(h) => h.to_ascii_lowercase(),
        None => return,
    };
    let path = options
        .get("netrc-path")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(std::path::PathBuf::from)
        .or_else(super::netrc::default_netrc_path);
    let Some(path) = path else { return };
    let netrc = match super::netrc::Netrc::from_path(&path) {
        Ok(n) => n,
        Err(e) => {
            tracing::debug!("netrc {} unreadable: {e}", path.display());
            return;
        }
    };
    let Some(entry) = netrc.lookup(&host) else {
        return;
    };
    let user = entry.login.as_deref().unwrap_or("");
    let pass = entry.password.as_deref().unwrap_or("");
    if user.is_empty() && pass.is_empty() {
        return;
    }
    let raw = format!("{user}:{pass}");
    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD.encode(raw.as_bytes());
    let value = format!("Basic {encoded}");
    if let Ok(v) = HeaderValue::from_str(&value) {
        headers.insert(risuko_http::header::AUTHORIZATION, v);
        tracing::debug!("Applied netrc credentials for {host}");
    }
}

fn build_mirror_headers(
    all_uris: &[String],
    base_headers: &HeaderMap,
    options: &Map<String, Value>,
) -> Vec<HeaderMap> {
    all_uris
        .iter()
        .map(|u| {
            let mut h = base_headers.clone();
            apply_netrc_auth(&mut h, u, options);
            h
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub async fn run_http_download_multi(
    uris: &[String],
    dir: &str,
    out: &str,
    options: &Map<String, Value>,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
    cancel_token: CancellationToken,
    global_limiter: Arc<SpeedLimiter>,
    task_limiter: Arc<SpeedLimiter>,
    chunk_completed: Vec<Arc<AtomicU64>>,
    adopted_filename: Arc<parking_lot::Mutex<Option<String>>>,
) -> Result<PathBuf, String> {
    if uris.is_empty() {
        return Err("no URIs provided".to_string());
    }
    let throttle = Throttle::new(global_limiter, task_limiter);
    let strategy = super::uri_selector::strategy_from_options(options);
    let mut stats = super::uri_selector::ServerStats::default();
    let mut tried: Vec<usize> = Vec::new();
    let mut last_err: Option<String> = None;

    loop {
        let idx = match super::uri_selector::pick(strategy, uris, &stats, &tried) {
            Some(i) => i,
            None => {
                return Err(last_err.unwrap_or_else(|| {
                    "all mirrors exhausted with no successful download".to_string()
                }))
            }
        };
        tried.push(idx);
        let uri = &uris[idx];
        if uris.len() > 1 {
            tracing::info!(
                "Mirror {}/{}: {}",
                tried.len(),
                uris.len(),
                super::uri_selector::host_of(uri)
            );
        }

        let started = std::time::Instant::now();
        let start_bytes = completed.load(Ordering::Relaxed);
        let attempt_token = cancel_token.child_token();
        let result = run_single_uri_download(
            uri,
            uris,
            dir,
            out,
            options,
            total.clone(),
            completed.clone(),
            speed.clone(),
            connections.clone(),
            attempt_token,
            throttle.clone(),
            chunk_completed.clone(),
            adopted_filename.clone(),
            false,
        )
        .await;

        match result {
            Ok(path) => {
                let elapsed = started.elapsed().as_secs_f64();
                let bytes = completed
                    .load(Ordering::Relaxed)
                    .saturating_sub(start_bytes);
                stats.record_success(&super::uri_selector::host_of(uri), bytes, elapsed);
                return Ok(path);
            }
            Err(e) => {
                if cancel_token.is_cancelled() {
                    return Err(e);
                }
                stats.record_failure(&super::uri_selector::host_of(uri));
                tracing::warn!("Mirror {} failed: {e}", super::uri_selector::host_of(uri));
                last_err = Some(e);
                completed.store(0, Ordering::Relaxed);
                total.store(0, Ordering::Relaxed);
                speed.store(0, Ordering::Relaxed);
                for cc in &chunk_completed {
                    cc.store(0, Ordering::Relaxed);
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_single_uri_download(
    uri: &str,
    all_uris: &[String],
    dir: &str,
    out: &str,
    options: &Map<String, Value>,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
    cancel_token: CancellationToken,
    throttle: Throttle,
    chunk_completed: Vec<Arc<AtomicU64>>,
    adopted_filename: Arc<parking_lot::Mutex<Option<String>>>,
    is_stale_retry: bool,
) -> Result<PathBuf, String> {
    tracing::info!("Starting download: uri={uri}, dir={dir}, out={out}");
    let dir_path = Path::new(dir);
    fs::create_dir_all(dir_path).map_err(|e| format!("Failed to create dir: {e}"))?;

    let out_was_empty = out.is_empty();
    let mut filename = if out_was_empty {
        infer_filename_from_uri(uri)
    } else {
        out.to_string()
    };
    filename = sanitize_filename(&filename);

    let url_inferred = sanitize_filename(&infer_filename_from_uri(uri));
    let url_inferred_part = format!("{url_inferred}{PART_SUFFIX}");
    let filename_was_url_derived = out_was_empty
        || filename == url_inferred
        || filename == url_inferred_part
        || filename.trim_end_matches(PART_SUFFIX) == url_inferred
        || is_placeholder_download_name(&filename)
        || is_placeholder_download_name(filename.trim_end_matches(PART_SUFFIX));

    let part_name = if filename.ends_with(PART_SUFFIX) {
        filename.clone()
    } else {
        format!("{filename}{PART_SUFFIX}")
    };
    let mut part_path = dir_path.join(&part_name);

    let split = options
        .get("split")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(8)
        .max(1) as usize;
    let max_worker_retries = options
        .get("max-worker-retries")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .map(|v| v.max(1).min(u64::from(u32::MAX)) as u32)
        .unwrap_or(CHUNK_MAX_RETRIES);

    let max_conn_per_server = options
        .get("max-connection-per-server")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .map(|v| v.max(1).min(u64::from(u32::MAX)) as usize)
        .unwrap_or(split);
    let mirror_strategy = super::uri_selector::strategy_from_options(options);

    let min_split_size = parse_size_option(options.get("min-split-size"))
        .unwrap_or(DEFAULT_MIN_SPLIT_SIZE)
        .max(1);

    let cookie_jar = load_cookie_jar(options);
    let client = build_client(options, false, false, cookie_jar.clone())?;
    let range_client = if split > 1 {
        build_client(options, false, true, cookie_jar)?
    } else {
        client.clone()
    };
    let base_headers = build_headers(options);
    let mut headers = base_headers.clone();
    apply_netrc_auth(&mut headers, uri, options);
    let headers = headers;
    let stall = StallWatchdog::from_options(options);
    let falloc_mode = {
        let mode = super::falloc::Mode::from_option(options.get("file-allocation"));
        if cfg!(target_os = "android") && mode == super::falloc::Mode::Falloc {
            super::falloc::Mode::None
        } else {
            mode
        }
    };

    let whole_checksum = parse_whole_checksum_option(options)?;
    let piece_checksums = parse_piece_checksums_option(options)?;

    let use_remote_time = options
        .get("remote-time")
        .and_then(json_bool)
        .unwrap_or(false);

    let auto_rename = options
        .get("auto-file-renaming")
        .and_then(json_bool)
        .unwrap_or(true);

    let existing_size = fs::metadata(&part_path).map(|m| m.len()).unwrap_or(0);

    let mut last_modified_header: Option<String> = None;

    let is_http = uri.starts_with("http://") || uri.starts_with("https://");

    let has_cf_clearance = effective_cookies_have_name(&client, &headers, uri, "cf_clearance");
    let wants_range_probe = split > 1 || filename_was_url_derived;

    let idle_timeout = Some(parse_duration_secs_option(
        options.get("timeout"),
        DEFAULT_IDLE_TIMEOUT_SECS,
    ))
    .filter(|d| !d.is_zero());

    let has_sidecar = existing_size > 0 && chunk_meta_path(&part_path).exists();
    let mut probe_error: Option<String> = None;
    let probe_for_name: Option<ProbeResult> = if is_http && wants_range_probe && !has_cf_clearance {
        let mut attempt: u32 = 0;
        loop {
            match probe_range_support(&range_client, uri, &headers).await {
                Ok(p) => break Some(p),
                Err(e)
                    if has_sidecar
                        && !e.contains(CLOUDFLARE_MARKER)
                        && attempt < max_worker_retries
                        && !cancel_token.is_cancelled() =>
                {
                    attempt += 1;
                    tracing::warn!(
                        "Range probe attempt {attempt}/{max_worker_retries} failed: {e}"
                    );
                    if !cancellable_sleep(
                        &cancel_token,
                        std::time::Duration::from_secs(u64::from(attempt)),
                    )
                    .await
                    {
                        return Err("Download cancelled".to_string());
                    }
                }
                Err(e) => {
                    tracing::warn!("Range probe failed, falling back to single: {e}");
                    if has_sidecar {
                        probe_error = Some(e);
                    }
                    break None;
                }
            }
        }
    } else {
        if is_http && wants_range_probe && has_cf_clearance {
            tracing::debug!("Skipping range probe because cf_clearance is present");
        }
        None
    };

    if filename_was_url_derived {
        if let Some(suggested) = probe_for_name
            .as_ref()
            .and_then(|p| p.suggested_filename.as_ref())
        {
            if let Some((new_name, new_part)) =
                adopt_suggested_filename(suggested, &filename, &part_path, dir_path)
            {
                tracing::info!(
                    "Adopting Content-Disposition filename: {filename:?} -> {new_name:?}"
                );
                *adopted_filename.lock() = Some(new_name.clone());
                filename = new_name;
                part_path = new_part;
            }
        } else if is_http {
            tracing::debug!(
                "No Content-Disposition filename available, keeping URL-derived name {filename:?}"
            );
        }
    }

    if filename_was_url_derived {
        if let Some(content_type) = probe_for_name
            .as_ref()
            .and_then(|p| p.content_type.as_deref())
        {
            if let Some((new_name, new_part)) =
                adopt_content_type_extension(&filename, content_type, &part_path, dir_path)
            {
                tracing::info!("Adding Content-Type extension: {filename:?} -> {new_name:?}");
                *adopted_filename.lock() = Some(new_name.clone());
                filename = new_name;
                part_path = new_part;
            }
        }
    }

    if let Some(e) = probe_error {
        return Err(if e.contains(CLOUDFLARE_MARKER) {
            e
        } else {
            format!("Range probe failed, partial progress kept: {e}")
        });
    }

    let resume_multi = existing_size > 0 && chunk_meta_path(&part_path).exists();
    let sidecar_probe = if is_http && probe_for_name.is_none() && resume_multi {
        probe_from_sidecar(&part_path)
    } else {
        None
    };
    let mut stale_from_multi: Option<String> = None;
    if is_http && (split > 1 || sidecar_probe.is_some()) {
        match probe_for_name.as_ref().or(sidecar_probe.as_ref()) {
            Some(probe)
                if probe.range_supported
                    && probe.content_length > 0
                    && (resume_multi || probe.content_length / min_split_size >= 2) =>
            {
                if use_remote_time {
                    last_modified_header = probe.last_modified.clone();
                }
                let workers = if resume_multi {
                    split
                } else {
                    (probe.content_length / min_split_size).min(split as u64) as usize
                };
                connections.store(workers as u32, Ordering::Relaxed);
                tracing::debug!(
                    "run_single_uri_download calling run_multi_chunk: part_path={part_path:?}, filename={filename:?}"
                );
                let mirror_headers = build_mirror_headers(all_uris, &base_headers, options);
                let result = run_multi_chunk(
                    &range_client,
                    all_uris,
                    mirror_strategy,
                    max_conn_per_server,
                    &part_path,
                    probe.content_length,
                    workers,
                    &mirror_headers,
                    total.clone(),
                    completed.clone(),
                    speed.clone(),
                    cancel_token.clone(),
                    &filename,
                    dir_path,
                    throttle.clone(),
                    probe.etag.clone(),
                    probe.last_modified.clone(),
                    &chunk_completed,
                    stall.clone(),
                    falloc_mode,
                    max_worker_retries,
                    auto_rename,
                    idle_timeout,
                    probe
                        .final_url
                        .as_ref()
                        .map(|f| (uri.to_string(), f.clone())),
                    (&piece_checksums, &whole_checksum),
                )
                .await;
                match result {
                    Err(ref e) if e.contains(STALE_PART_REMOVED) && !is_stale_retry => {
                        stale_from_multi = result.err();
                    }
                    other => {
                        if let Ok(ref path) = other {
                            if let Some(ref lm) = last_modified_header {
                                apply_remote_file_time(path, lm);
                            }
                        }
                        return other;
                    }
                }
            }
            Some(_) => {
                tracing::info!("File too small for multi-chunk, using single connection");
            }
            None => {
                if has_cf_clearance {
                    tracing::info!(
                        "Range probe skipped (cf_clearance present), using single connection"
                    );
                } else {
                    tracing::info!("Server does not support ranges, using single connection");
                }
            }
        }
    }

    if stale_from_multi.is_none() && chunk_meta_path(&part_path).exists() && existing_size > 0 {
        tracing::info!("Removing pre-allocated .part before single-connection fallback");
        let _ = fs::remove_file(&part_path);
        delete_chunk_meta(&part_path);
    }
    connections.store(1, Ordering::Relaxed);

    // Reuse the probe's Range shape; some signed-URL CDNs (e.g. Quark) reject a plain GET with 412
    let probe_confirmed_range = is_http
        && split > 1
        && probe_for_name
            .as_ref()
            .map(|p| p.range_supported)
            .unwrap_or(false);
    let single_client = if probe_confirmed_range {
        &range_client
    } else {
        &client
    };

    let mut single_attempt: u32 = 0;
    let result = if let Some(e) = stale_from_multi {
        Err(e)
    } else {
        loop {
            let attempt = run_single_download(
                single_client,
                uri,
                &part_path,
                &headers,
                total.clone(),
                completed.clone(),
                speed.clone(),
                cancel_token.clone(),
                &filename,
                dir_path,
                throttle.clone(),
                stall.clone(),
                filename_was_url_derived,
                probe_confirmed_range,
                auto_rename,
                idle_timeout,
                (&piece_checksums, &whole_checksum),
            )
            .await;

            match attempt {
                Err(ref e)
                    if single_attempt < max_worker_retries
                        && !cancel_token.is_cancelled()
                        && is_transient_single_error(e) =>
                {
                    single_attempt += 1;
                    let resumed = completed.load(Ordering::Relaxed);
                    tracing::warn!(
                        "Single-connection attempt {single_attempt}/{max_worker_retries} failed \
                     ({e}); resuming from {resumed} bytes"
                    );
                    speed.store(0, Ordering::Relaxed);
                    if !cancellable_sleep(
                        &cancel_token,
                        std::time::Duration::from_secs(single_attempt as u64),
                    )
                    .await
                    {
                        break Err("Download cancelled".to_string());
                    }
                    continue;
                }
                other => break other,
            }
        }
    };

    if let Err(ref e) = result {
        if e.contains(STALE_PART_REMOVED) && !is_stale_retry {
            tracing::info!("Retrying download after stale .part removal");
            delete_chunk_meta(&part_path);
            completed.store(0, Ordering::Relaxed);
            total.store(0, Ordering::Relaxed);
            speed.store(0, Ordering::Relaxed);
            for cc in &chunk_completed {
                cc.store(0, Ordering::Relaxed);
            }
            return Box::pin(run_single_uri_download(
                uri,
                all_uris,
                dir,
                out,
                options,
                total,
                completed,
                speed,
                connections,
                cancel_token,
                throttle,
                chunk_completed,
                adopted_filename,
                true,
            ))
            .await;
        }
    }

    match result {
        Ok((path, lm)) => {
            if use_remote_time {
                if let Some(ref lm_str) = lm {
                    apply_remote_file_time(&path, lm_str);
                }
            }
            Ok(path)
        }
        Err(e) => Err(e),
    }
}

struct ProbeResult {
    content_length: u64,
    etag: Option<String>,
    last_modified: Option<String>,
    suggested_filename: Option<String>,
    content_type: Option<String>,
    range_supported: bool,
    final_url: Option<String>,
}

fn probe_from_sidecar(part_path: &Path) -> Option<ProbeResult> {
    let data = fs::read_to_string(chunk_meta_path(part_path)).ok()?;
    let meta: ChunkMeta = serde_json::from_str(&data).ok()?;
    let file_len = fs::metadata(part_path).ok()?.len();
    if meta.version != META_VERSION
        || meta.content_length == 0
        || file_len > meta.content_length
        || !has_resume_validator(&meta.etag, &meta.last_modified)
    {
        return None;
    }
    Some(ProbeResult {
        content_length: meta.content_length,
        etag: meta.etag,
        last_modified: meta.last_modified,
        suggested_filename: None,
        content_type: None,
        range_supported: true,
        final_url: None,
    })
}

pub const CLOUDFLARE_MARKER: &str = "[cloudflare-challenge]";

fn looks_like_cloudflare_block(headers: &HeaderMap, status: u16) -> bool {
    if !matches!(status, 403 | 429 | 503) {
        return false;
    }
    let header_str = |name: &str| -> Option<String> {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_ascii_lowercase())
    };

    if headers.get("cf-ray").is_some() {
        return true;
    }
    if let Some(server) = header_str("server") {
        if server.contains("cloudflare") {
            return true;
        }
    }
    if let Some(mitigated) = header_str("cf-mitigated") {
        if mitigated.contains("challenge") {
            return true;
        }
    }
    false
}

fn headers_have_cookie_name(headers: &HeaderMap, name: &str) -> bool {
    headers
        .get(risuko_http::header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|raw| cookie_header_has_name(raw, name))
}

fn cookie_header_has_name(raw: &str, name: &str) -> bool {
    raw.split(';')
        .filter_map(|kv| kv.split('=').next())
        .map(str::trim)
        .any(|cookie_name| cookie_name.eq_ignore_ascii_case(name))
}

fn effective_cookies_have_name(
    client: &Client,
    headers: &HeaderMap,
    uri: &str,
    name: &str,
) -> bool {
    if headers.contains_key(risuko_http::header::COOKIE) {
        return headers_have_cookie_name(headers, name);
    }
    url::Url::parse(uri)
        .ok()
        .and_then(|u| client.jar_cookies(&u))
        .and_then(|v| v.to_str().ok().map(|raw| cookie_header_has_name(raw, name)))
        .unwrap_or(false)
}

fn log_cloudflare_diagnostic(
    client: &Client,
    req_headers: &HeaderMap,
    resp_headers: &HeaderMap,
    uri: &str,
) {
    use risuko_http::header::{COOKIE, USER_AGENT};

    let sanitized_uri = match url::Url::parse(uri) {
        Ok(mut u) => {
            u.set_query(None);
            u.set_fragment(None);
            let _ = u.set_username("");
            let _ = u.set_password(None);
            u.to_string()
        }
        Err(_) => {
            let mut s = uri;
            if let Some(pos) = s.find('?') {
                s = &s[..pos];
            }
            if let Some(pos) = s.find('#') {
                s = &s[..pos];
            }
            s.to_string()
        }
    };

    let manual_cookie = req_headers
        .get(COOKIE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let effective_cookie = manual_cookie.or_else(|| {
        url::Url::parse(uri)
            .ok()
            .and_then(|u| client.jar_cookies(&u))
            .and_then(|v| v.to_str().ok().map(str::to_owned))
    });

    let (cookie_names, cookie_len, has_cf_clearance) = match effective_cookie {
        Some(raw) => {
            let names: Vec<&str> = raw
                .split(';')
                .filter_map(|kv| kv.split('=').next())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();
            let has_clearance = names.iter().any(|n| n.eq_ignore_ascii_case("cf_clearance"));
            (names.join(","), raw.len(), has_clearance)
        }
        None => ("<none>".to_string(), 0, false),
    };

    let sent_ua = req_headers
        .get(USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("<client-level>");

    let resp_str = |name: &str| -> String {
        resp_headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<absent>")
            .to_string()
    };

    tracing::warn!(
        "cloudflare-diagnostic uri={sanitized_uri} \
         sent[cf_clearance={has_cf_clearance} cookie_names={cookie_names} \
         cookie_len={cookie_len} req_ua={sent_ua}] \
         resp[cf-ray={} cf-mitigated={} cf-cache-status={} server={}]",
        resp_str("cf-ray"),
        resp_str("cf-mitigated"),
        resp_str("cf-cache-status"),
        resp_str("server"),
    );
}

fn cloudflare_error(uri: &str, status: u16) -> String {
    let host = url::Url::parse(uri)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "unknown".to_string());
    format!("{CLOUDFLARE_MARKER} host={host} status={status}")
}

async fn read_body_prefix(
    resp: risuko_http::Response,
    cap: usize,
    idle: Option<std::time::Duration>,
    cancel_token: &CancellationToken,
) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut stream = resp.bytes_stream();
    while buf.len() < cap {
        match with_idle_timeout(idle, cancel_token, stream.next()).await {
            Ok(Some(Ok(bytes))) => {
                let take = (cap - buf.len()).min(bytes.len());
                buf.extend_from_slice(&bytes[..take]);
            }
            _ => break,
        }
    }
    buf
}

async fn drain_response_body(resp: risuko_http::Response) {
    const MAX_DRAIN_BYTES: usize = 64 * 1024;
    const DRAIN_IDLE: std::time::Duration = std::time::Duration::from_secs(10);
    read_body_prefix(
        resp,
        MAX_DRAIN_BYTES,
        Some(DRAIN_IDLE),
        &CancellationToken::new(),
    )
    .await;
}

async fn cancellable_sleep(cancel_token: &CancellationToken, dur: std::time::Duration) -> bool {
    tokio::select! {
        _ = cancel_token.cancelled() => false,
        _ = tokio::time::sleep(dur) => true,
    }
}

fn same_origin(a: &str, b: &str) -> bool {
    match (url::Url::parse(a), url::Url::parse(b)) {
        (Ok(a), Ok(b)) => {
            a.scheme() == b.scheme()
                && a.host_str().map(str::to_ascii_lowercase)
                    == b.host_str().map(str::to_ascii_lowercase)
                && a.port_or_known_default() == b.port_or_known_default()
        }
        _ => false,
    }
}

fn cross_origin_headers(headers: &HeaderMap) -> HeaderMap {
    let mut out = HeaderMap::new();
    for name in [
        risuko_http::header::USER_AGENT,
        risuko_http::header::REFERER,
    ] {
        if let Some(v) = headers.get(&name) {
            out.insert(name, v.clone());
        }
    }
    out
}

async fn probe_range_support(
    client: &Client,
    uri: &str,
    headers: &HeaderMap,
) -> Result<ProbeResult, String> {
    let resp = client
        .get(uri)
        .headers(headers.clone())
        .header(RANGE, "bytes=0-0")
        .header(ACCEPT_ENCODING, "identity")
        .send()
        .await
        .map_err(|e| format!("Range probe request failed: {e}"))?;

    let status = resp.status().as_u16();

    if looks_like_cloudflare_block(resp.headers(), status) {
        log_cloudflare_diagnostic(client, headers, resp.headers(), uri);
        let err = cloudflare_error(uri, status);
        drain_response_body(resp).await;
        return Err(err);
    }

    tracing::debug!(
        "Range probe: uri={uri}, status={status}, accept-ranges={:?}, content-range={:?}, \
         content-length={:?}, content-encoding={:?}",
        resp.headers().get(ACCEPT_RANGES),
        resp.headers().get(CONTENT_RANGE),
        resp.headers().get(CONTENT_LENGTH),
        resp.headers().get(CONTENT_ENCODING),
    );

    let etag = resp
        .headers()
        .get(ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let last_modified = resp
        .headers()
        .get(LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let suggested_filename = filename_from_content_disposition(resp.headers());
    let content_type = content_type_from_headers(resp.headers());

    let no_range = ProbeResult {
        content_length: 0,
        etag: etag.clone(),
        last_modified: last_modified.clone(),
        suggested_filename: suggested_filename.clone(),
        content_type: content_type.clone(),
        range_supported: false,
        final_url: None,
    };

    let content_encoding = resp
        .headers()
        .get(CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !content_encoding.is_empty() && !content_encoding.eq_ignore_ascii_case("identity") {
        tracing::info!(
            "Range probe: Content-Encoding={content_encoding}; falling back to single connection"
        );
        return Ok(no_range);
    }
    let transfer_encoding = resp
        .headers()
        .get(TRANSFER_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if transfer_encoding
        .split(',')
        .any(|t| t.trim().eq_ignore_ascii_case("chunked"))
    {
        tracing::info!("Range probe: Transfer-Encoding=chunked; falling back to single connection");
        return Ok(no_range);
    }

    if status == 206 {
        let total = resp
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_content_range)
            .and_then(|cr| cr.total);
        if let Some(total) = total {
            return Ok(ProbeResult {
                content_length: total,
                etag,
                last_modified,
                suggested_filename,
                content_type,
                range_supported: true,
                final_url: Some(resp.url().to_string()),
            });
        }
        return Ok(no_range);
    }

    Ok(no_range)
}

fn is_http_scheme(uri: &str) -> bool {
    let lower = uri.get(..8).unwrap_or(uri).to_ascii_lowercase();
    lower.starts_with("http://") || lower.starts_with("https://")
}

fn endpoint_key(uri: &str) -> String {
    match url::Url::parse(uri) {
        Ok(u) => {
            let host = u.host_str().unwrap_or("").to_ascii_lowercase();
            match u.port_or_known_default() {
                Some(p) => format!("{host}:{p}"),
                None if host.is_empty() => uri.to_string(),
                None => host,
            }
        }
        Err(_) => uri.to_string(),
    }
}

struct MirrorPool {
    uris: Vec<String>,
    keys: Vec<String>,
    strategy: super::uri_selector::Strategy,
    stats: parking_lot::Mutex<super::uri_selector::ServerStats>,
    active: parking_lot::Mutex<std::collections::HashMap<String, usize>>,
    max_conn_per_server: usize,
    resolved: parking_lot::Mutex<Vec<Option<String>>>,
    changed: AtomicBool,
}

impl MirrorPool {
    fn new(
        uris: Vec<String>,
        strategy: super::uri_selector::Strategy,
        max_conn_per_server: usize,
        probed: Option<(String, String)>,
    ) -> Self {
        let keys = uris.iter().map(|u| endpoint_key(u)).collect();
        let resolved = uris
            .iter()
            .map(|u| match &probed {
                Some((orig, fin)) if orig == u && fin != u && is_http_scheme(fin) => {
                    Some(fin.clone())
                }
                _ => None,
            })
            .collect();
        Self {
            resolved: parking_lot::Mutex::new(resolved),
            changed: AtomicBool::new(false),
            uris,
            keys,
            strategy,
            stats: parking_lot::Mutex::new(super::uri_selector::ServerStats::default()),
            active: parking_lot::Mutex::new(std::collections::HashMap::new()),
            max_conn_per_server: max_conn_per_server.max(1),
        }
    }

    fn distinct_endpoints(&self) -> usize {
        let mut s = std::collections::HashSet::new();
        for k in &self.keys {
            s.insert(k.as_str());
        }
        s.len().max(1)
    }

    fn has_live(&self) -> bool {
        !self.uris.is_empty()
    }

    fn acquire(&self) -> Option<(usize, String)> {
        let stats = self.stats.lock();
        let mut active = self.active.lock();
        let all_blacklisted = self.keys.iter().all(|k| stats.is_blacklisted(k));
        let eligible: Vec<usize> = (0..self.uris.len())
            .filter(|&i| {
                let k = &self.keys[i];
                let at_cap = active.get(k).copied().unwrap_or(0) >= self.max_conn_per_server;
                let blacklisted = self.strategy != super::uri_selector::Strategy::Inorder
                    && !all_blacklisted
                    && stats.is_blacklisted(k);
                !at_cap && !blacklisted
            })
            .collect();
        let chosen = *eligible.first()?;
        let active_of = |i: usize| active.get(&self.keys[i]).copied().unwrap_or(0);
        let ema_of = |i: usize| stats.get(&self.keys[i]).map(|s| s.ema_bps).unwrap_or(0.0);
        let chosen = match self.strategy {
            super::uri_selector::Strategy::Inorder => eligible[0],
            super::uri_selector::Strategy::Adaptive => *eligible
                .iter()
                .min_by(|&&a, &&b| {
                    active_of(a).cmp(&active_of(b)).then(
                        ema_of(b)
                            .partial_cmp(&ema_of(a))
                            .unwrap_or(std::cmp::Ordering::Equal),
                    )
                })
                .unwrap_or(&chosen),
            super::uri_selector::Strategy::Feedback => *eligible
                .iter()
                .min_by_key(|&&i| (active_of(i), i))
                .unwrap_or(&chosen),
        };
        let key = self.keys[chosen].clone();
        *active.entry(key.clone()).or_insert(0) += 1;
        Some((chosen, key))
    }

    fn target(&self, idx: usize, base: &HeaderMap) -> (String, HeaderMap, bool) {
        let cached = self.resolved.lock().get(idx).cloned().flatten();
        match cached {
            Some(url) => {
                let headers = if same_origin(&self.uris[idx], &url) {
                    base.clone()
                } else {
                    cross_origin_headers(base)
                };
                (url, headers, true)
            }
            None => (self.uris[idx].clone(), base.clone(), false),
        }
    }

    fn record_resolved(&self, idx: usize, final_url: &str) {
        if final_url != self.uris[idx] && is_http_scheme(final_url) {
            if let Some(slot) = self.resolved.lock().get_mut(idx) {
                *slot = Some(final_url.to_string());
            }
        }
    }

    fn forget_resolved(&self, idx: usize) {
        if let Some(slot) = self.resolved.lock().get_mut(idx) {
            *slot = None;
        }
    }

    fn release(&self, key: &str) {
        let mut active = self.active.lock();
        if let Some(c) = active.get_mut(key) {
            *c = c.saturating_sub(1);
        }
    }
    fn record_success(&self, key: &str, bytes: u64, secs: f64) {
        self.stats.lock().record_success(key, bytes, secs);
    }
    fn record_failure(&self, key: &str) {
        self.stats.lock().record_failure(key);
    }
}

async fn run_multi_chunk(
    client: &Client,
    uris: &[String],
    strategy: super::uri_selector::Strategy,
    max_conn_per_server: usize,
    part_path: &Path,
    content_length: u64,
    split: usize,
    mirror_headers: &[HeaderMap],
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    cancel_token: CancellationToken,
    filename: &str,
    dir_path: &Path,
    throttle: Throttle,
    expected_etag: Option<String>,
    expected_last_modified: Option<String>,
    chunk_completed: &[Arc<AtomicU64>],
    stall: StallWatchdog,
    falloc_mode: super::falloc::Mode,
    max_retries: u32,
    auto_rename: bool,
    idle_timeout: Option<std::time::Duration>,
    probed_url: Option<(String, String)>,
    integrity: (
        &Option<super::hasher::PieceChecksums>,
        &Option<super::hasher::WholeChecksum>,
    ),
) -> Result<PathBuf, String> {
    total.store(content_length, Ordering::Relaxed);

    let resumed_meta = load_piece_meta(
        part_path,
        content_length,
        &expected_etag,
        &expected_last_modified,
    );
    let piece_size = resumed_meta
        .as_ref()
        .map(|m| u64::from(m.piece_size))
        .unwrap_or_else(|| choose_piece_size(content_length, split));
    let queue = Arc::new(PieceQueue::with_piece_size(content_length, piece_size));
    tracing::info!(
        "Multi-piece download: {} pieces ({} KiB each), {} workers, {} bytes total",
        queue.pieces.len(),
        piece_size / 1024,
        split,
        content_length
    );

    if let Some(meta) = resumed_meta {
        let mut total_resumed: u64 = 0;
        for pp in &meta.pieces {
            if let Some(p) = queue.pieces.get(pp.i as usize) {
                let c = std::cmp::min(pp.c, p.length);
                p.completed.store(c, Ordering::Relaxed);
                total_resumed += c as u64;
                if c == p.length {
                    p.state.store(PIECE_DONE, Ordering::Release);
                }
            }
        }
        completed.store(total_resumed, Ordering::Relaxed);
        tracing::info!("Resuming multi-piece download: {total_resumed}/{content_length} bytes");
    }

    let file = {
        let f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(part_path)
            .map_err(|e| format!("Failed to create file: {e}"))?;
        let f = tokio::task::spawn_blocking(move || {
            super::falloc::allocate(&f, content_length, falloc_mode).map(|_| f)
        })
        .await
        .map_err(|e| format!("pre-allocation task failed: {e}"))?
        .map_err(|e| format!("Failed to pre-allocate file: {e}"))?;
        Arc::new(f)
    };

    let speed_cancel = cancel_token.clone();
    let speed_completed = completed.clone();
    let speed_val = speed.clone();
    let speed_total = total.clone();
    let speed_stall = stall.clone();
    let speed_task = tokio::spawn(async move {
        run_speed_tracker(
            speed_completed,
            speed_val,
            speed_total,
            speed_cancel,
            speed_stall,
        )
        .await;
    });

    let save_part = part_path.to_path_buf();
    let save_queue = Arc::clone(&queue);
    let save_etag = expected_etag.clone();
    let save_lm = expected_last_modified.clone();
    let save_cancel = cancel_token.clone();
    let save_stop = CancellationToken::new();
    let save_stop_rx = save_stop.clone();
    let save_task = tokio::spawn(async move {
        let progress = |q: &PieceQueue| -> u64 {
            q.pieces
                .iter()
                .map(|p| u64::from(p.completed.load(Ordering::Relaxed)))
                .sum()
        };
        let mut last_saved = progress(&save_queue);
        let mut tick = tokio::time::interval(META_SAVE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        tick.tick().await;
        loop {
            tokio::select! {
                _ = save_cancel.cancelled() => break,
                _ = save_stop_rx.cancelled() => break,
                _ = tick.tick() => {
                    let now = progress(&save_queue);
                    if now == last_saved {
                        continue;
                    }
                    last_saved = now;
                    let (part, queue, etag, lm) = (
                        save_part.clone(),
                        Arc::clone(&save_queue),
                        save_etag.clone(),
                        save_lm.clone(),
                    );
                    let _ = tokio::task::spawn_blocking(move || {
                        save_piece_meta(&part, &queue, content_length, &etag, &lm);
                    })
                    .await;
                }
            }
        }
    });

    let multi_mirror = uris.len() > 1;
    let pool = Arc::new(MirrorPool::new(
        uris.to_vec(),
        strategy,
        max_conn_per_server,
        probed_url,
    ));
    let worker_token = cancel_token.child_token();
    let mirror_headers = Arc::new(mirror_headers.to_vec());
    let piece_etag = if multi_mirror {
        None
    } else {
        expected_etag.clone()
    };
    let piece_last_modified = if multi_mirror {
        None
    } else {
        expected_last_modified.clone()
    };
    let expected_total = Some(content_length);
    let effective_workers = split
        .min(max_conn_per_server.saturating_mul(pool.distinct_endpoints()))
        .max(1);

    let mut workers = futures_util::stream::FuturesUnordered::new();
    for w in 0..effective_workers {
        let client = client.clone();
        let pool = Arc::clone(&pool);
        let mirror_headers = Arc::clone(&mirror_headers);
        let file = Arc::clone(&file);
        let queue = Arc::clone(&queue);
        let completed = Arc::clone(&completed);
        let cancel_token = worker_token.clone();
        let throttle = throttle.clone();
        let etag = piece_etag.clone();
        let lm = piece_last_modified.clone();
        let wc = chunk_completed.get(w).cloned();

        workers.push(tokio::spawn(async move {
            piece_worker(
                w,
                &client,
                pool,
                &mirror_headers,
                file,
                queue,
                completed,
                cancel_token,
                throttle,
                etag,
                lm,
                expected_total,
                wc,
                max_retries,
                idle_timeout,
            )
            .await
        }));
    }

    let mut errors = Vec::new();
    while let Some(result) = workers.next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => errors.push(e),
            Err(e) => errors.push(format!("Worker task panicked: {e}")),
        }
    }

    speed_task.abort();
    // Let the save task finish so no save lands after delete_chunk_meta below
    save_stop.cancel();
    let _ = save_task.await;
    speed.store(0, Ordering::Relaxed);

    if pool.changed.load(Ordering::Acquire) {
        let _ = fs::remove_file(part_path);
        delete_chunk_meta(part_path);
        return Err(format!(
            "Download will retry: {STALE_PART_REMOVED} (server file changed)"
        ));
    }

    if stall.flag.load(Ordering::Acquire) {
        save_piece_meta(
            part_path,
            &queue,
            content_length,
            &expected_etag,
            &expected_last_modified,
        );
        return Err(format!(
            "Download stalled: speed below {} B/s for {}s",
            stall.lowest_speed,
            stall.timeout.as_secs(),
        ));
    }

    if !errors.is_empty() {
        save_piece_meta(
            part_path,
            &queue,
            content_length,
            &expected_etag,
            &expected_last_modified,
        );

        if errors.iter().all(|e| e.contains("cancelled")) {
            return Err("Download cancelled".to_string());
        }
        if !queue.is_finished() {
            let real_errors: Vec<&String> =
                errors.iter().filter(|e| !e.contains("cancelled")).collect();
            if !real_errors.is_empty() {
                return Err(real_errors
                    .iter()
                    .map(|e| e.as_str())
                    .collect::<Vec<_>>()
                    .join("; "));
            }
        }
    }

    // Sync before deleting the sidecar; on failure abort so the sidecar survives
    if let Err(e) = sync_file(&file).await {
        return Err(format!("fsync before rename failed: {e}"));
    }
    if let Err(e) = verify_output(part_path, content_length, integrity.0, integrity.1).await {
        delete_chunk_meta(part_path);
        return Err(e);
    }
    tracing::debug!("run_multi_chunk finalizing: part_path={part_path:?}, filename={filename:?}");
    let path = finalize_download(part_path, filename, dir_path, auto_rename)?;
    delete_chunk_meta(part_path);
    Ok(path)
}

fn http_error_status(error: &str) -> Option<u16> {
    error.strip_prefix("HTTP error: ")?.trim().parse().ok()
}

fn charge_retry_if_no_progress(retry_count: &mut u32, downloaded: u64) -> bool {
    if downloaded > 0 {
        *retry_count = 0;
        false
    } else {
        *retry_count = retry_count.saturating_add(1);
        true
    }
}

fn partial_error_backoff(error_streak: u32) -> std::time::Duration {
    PARTIAL_ERROR_BACKOFF_STEP
        .saturating_mul(error_streak.max(1))
        .min(PARTIAL_ERROR_BACKOFF_MAX)
}

#[derive(Debug, PartialEq, Eq)]
enum PieceDownloadError {
    Cancelled,
    Changed(String),
    Source(String),
    Storage(String),
}

#[allow(clippy::too_many_arguments)]
async fn piece_worker(
    worker_id: usize,
    client: &Client,
    pool: Arc<MirrorPool>,
    mirror_headers: &[HeaderMap],
    file: Arc<std::fs::File>,
    queue: Arc<PieceQueue>,
    completed: Arc<AtomicU64>,
    cancel_token: CancellationToken,
    throttle: Throttle,
    expected_etag: Option<String>,
    expected_last_modified: Option<String>,
    expected_total: Option<u64>,
    worker_completed: Option<Arc<AtomicU64>>,
    max_retries: u32,
    idle_timeout: Option<std::time::Duration>,
) -> Result<(), String> {
    let mut retry_count: u32 = 0;
    let mut source_error_streak: u32 = 0;
    loop {
        if cancel_token.is_cancelled() {
            return Err("Download cancelled".to_string());
        }

        let idx = match queue.claim_next() {
            Some(i) => i,
            None if queue.has_inflight() => {
                tokio::select! {
                    _ = cancel_token.cancelled() => return Err("Download cancelled".to_string()),
                    _ = queue.wait_for_change() => {}
                }
                continue;
            }
            None => return Ok(()),
        };

        let piece_offset = queue.pieces[idx].offset;
        let piece_length = queue.pieces[idx].length;
        let piece_completed = Arc::clone(&queue.pieces[idx].completed);

        let already = piece_completed.load(Ordering::Relaxed);
        if already >= piece_length {
            queue.complete(idx);
            continue;
        }

        let range = ChunkRange::new(
            piece_offset + already as u64,
            piece_offset + piece_length as u64 - 1,
        );

        let (mirror_idx, mirror_key) = loop {
            match pool.acquire() {
                Some(picked) => break picked,
                None => {
                    if !pool.has_live() {
                        queue.release(idx);
                        return Err(format!(
                            "Worker {worker_id}: all mirrors failed on piece {idx}"
                        ));
                    }
                    if cancel_token.is_cancelled() {
                        queue.release(idx);
                        return Err("Download cancelled".to_string());
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        };
        let base_headers = match mirror_headers
            .get(mirror_idx)
            .or_else(|| mirror_headers.first())
        {
            Some(h) => h,
            None => {
                pool.release(&mirror_key);
                queue.release(idx);
                return Err(format!("Worker {worker_id}: no mirror headers available"));
            }
        };
        let (uri, headers, used_resolved) = pool.target(mirror_idx, base_headers);
        let headers = &headers;
        let started = std::time::Instant::now();
        let mut observed_url: Option<String> = None;

        let outcome = download_piece_stream(
            client,
            &uri,
            headers,
            range,
            &file,
            &completed,
            &cancel_token,
            &throttle,
            expected_etag.as_deref(),
            expected_last_modified.as_deref(),
            worker_completed.as_ref(),
            &piece_completed,
            expected_total,
            idle_timeout,
            &mut observed_url,
        )
        .await;
        pool.release(&mirror_key);
        if !used_resolved {
            if let Some(final_url) = observed_url.as_deref() {
                pool.record_resolved(mirror_idx, final_url);
            }
        }

        let now_completed = piece_completed.load(Ordering::Relaxed);
        let downloaded = (now_completed as u64).saturating_sub(already as u64);

        match outcome {
            Ok(()) => {
                source_error_streak = 0;
                if now_completed >= piece_length {
                    pool.record_success(&mirror_key, downloaded, started.elapsed().as_secs_f64());
                    queue.complete(idx);
                    retry_count = 0;
                } else if now_completed > already {
                    pool.record_success(&mirror_key, downloaded, started.elapsed().as_secs_f64());
                    queue.release(idx);
                    retry_count = 0;
                } else {
                    queue.release(idx);
                    charge_retry_if_no_progress(&mut retry_count, downloaded);
                    if retry_count > max_retries {
                        return Err(format!(
                            "Worker {worker_id} failed after {max_retries} retries on \
                             piece {idx}: server closed stream early"
                        ));
                    }
                    tracing::warn!(
                        "Worker {worker_id} piece {idx} attempt \
                         {retry_count}/{max_retries}: early EOF, will retry"
                    );
                    if !cancellable_sleep(
                        &cancel_token,
                        std::time::Duration::from_secs(retry_count as u64),
                    )
                    .await
                    {
                        return Err("Download cancelled".to_string());
                    }
                }
            }
            Err(PieceDownloadError::Cancelled) => {
                queue.release(idx);
                return Err("Download cancelled".to_string());
            }
            Err(PieceDownloadError::Changed(error)) => {
                queue.release(idx);
                pool.changed.store(true, Ordering::Release);
                cancel_token.cancel();
                return Err(format!("Worker {worker_id}: {error}"));
            }
            Err(PieceDownloadError::Storage(error)) => {
                queue.release(idx);
                return Err(format!(
                    "Worker {worker_id} local storage failure on piece {idx}: {error}"
                ));
            }
            Err(PieceDownloadError::Source(error)) => {
                queue.release(idx);
                if used_resolved && matches!(http_error_status(&error), Some(401 | 403 | 404 | 410))
                {
                    tracing::debug!("Cached redirect target rejected ({error}); re-resolving");
                    pool.forget_resolved(mirror_idx);
                    continue;
                }
                source_error_streak = source_error_streak.saturating_add(1);
                if !charge_retry_if_no_progress(&mut retry_count, downloaded) {
                    pool.record_success(&mirror_key, downloaded, started.elapsed().as_secs_f64());
                    let backoff = partial_error_backoff(source_error_streak);
                    tracing::warn!(
                        "Worker {worker_id} piece {idx} on {mirror_key} failed after writing \
                         {downloaded} bytes: {error}; retry budget reset, retrying in {}ms",
                        backoff.as_millis()
                    );
                    if !cancellable_sleep(&cancel_token, backoff).await {
                        return Err("Download cancelled".to_string());
                    }
                    continue;
                }
                pool.record_failure(&mirror_key);
                if retry_count > max_retries {
                    return Err(format!(
                        "Worker {worker_id} failed after {max_retries} retries on \
                         piece {idx}: {error}"
                    ));
                }
                tracing::warn!(
                    "Worker {worker_id} piece {idx} on {mirror_key} attempt \
                     {retry_count}/{max_retries}: {error}, will retry"
                );
                if !cancellable_sleep(
                    &cancel_token,
                    std::time::Duration::from_secs(retry_count as u64),
                )
                .await
                {
                    return Err("Download cancelled".to_string());
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn download_piece_stream(
    client: &Client,
    uri: &str,
    headers: &HeaderMap,
    range: ChunkRange,
    file: &Arc<std::fs::File>,
    completed: &Arc<AtomicU64>,
    cancel_token: &CancellationToken,
    throttle: &Throttle,
    expected_etag: Option<&str>,
    expected_last_modified: Option<&str>,
    worker_completed: Option<&Arc<AtomicU64>>,
    piece_completed: &Arc<AtomicU32>,
    expected_total: Option<u64>,
    idle_timeout: Option<std::time::Duration>,
    final_url: &mut Option<String>,
) -> Result<(), PieceDownloadError> {
    // Identity is forced after user headers so a coded body never lands at decoded offsets
    let mut req = client
        .get(uri)
        .headers(headers.clone())
        .header(ACCEPT_ENCODING, "identity")
        .header(RANGE, range.to_range_header_value());

    let strong_expected_etag = expected_etag.filter(|etag| etags_strongly_equal(etag, etag));

    if let Some(etag) = strong_expected_etag {
        if let Ok(v) = HeaderValue::from_str(etag) {
            req = req.header(IF_MATCH, v);
        }
    }

    if strong_expected_etag.is_none() {
        if let Some(lm) = expected_last_modified {
            if let Ok(v) = HeaderValue::from_str(lm) {
                req = req.header(IF_UNMODIFIED_SINCE, v);
            }
        }
    }

    let resp = with_idle_timeout(idle_timeout, cancel_token, req.send())
        .await
        .map_err(|e| match e {
            IdleError::Cancelled => PieceDownloadError::Cancelled,
            IdleError::TimedOut => {
                PieceDownloadError::Source("HTTP request failed: read timeout".to_string())
            }
        })?
        .map_err(|e| PieceDownloadError::Source(format!("HTTP request failed: {e}")))?;

    let status = resp.status().as_u16();

    if looks_like_cloudflare_block(resp.headers(), status) {
        log_cloudflare_diagnostic(client, headers, resp.headers(), uri);
        let err = cloudflare_error(uri, status);
        drain_response_body(resp).await;
        return Err(PieceDownloadError::Source(err));
    }

    if let Some(expected) = strong_expected_etag {
        let mismatch = resp
            .headers()
            .get(ETAG)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|actual| !etags_strongly_equal(actual, expected));
        if mismatch {
            drop(resp);
            return Err(PieceDownloadError::Changed(
                "Server file changed (ETag mismatch), aborting download".to_string(),
            ));
        }
    }

    if status == 412 && (strong_expected_etag.is_some() || expected_last_modified.is_some()) {
        drop(resp);
        return Err(PieceDownloadError::Changed(
            "Server file changed (precondition failed), aborting download".to_string(),
        ));
    }

    if status >= 400 {
        let safe_headers: String = resp
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                let name_str = name.as_str();
                if matches!(
                    name_str.to_lowercase().as_str(),
                    "authorization" | "set-cookie" | "cookie" | "proxy-authorization"
                ) {
                    None
                } else {
                    Some(format!("{}: {:?}", name_str, value))
                }
            })
            .collect::<Vec<_>>()
            .join(", ");

        let body_len = resp.content_length().unwrap_or(0);
        const MAX_SNIPPET_BYTES: usize = 256;
        let snippet = if body_len > 0 {
            let buf = read_body_prefix(resp, MAX_SNIPPET_BYTES, idle_timeout, cancel_token).await;
            String::from_utf8_lossy(&buf).to_string()
        } else {
            "<empty body>".to_string()
        };

        tracing::debug!(
            "Piece request got HTTP {status} for range {}; safe_headers=[{safe_headers}]; \
             body_len={body_len}; body_snippet={snippet:?}",
            range.to_range_header_value()
        );
        tracing::warn!(
            "Piece request got HTTP {status} for range {}",
            range.to_range_header_value()
        );
        return Err(PieceDownloadError::Source(format!("HTTP error: {status}")));
    }

    if status != 206 {
        return Err(PieceDownloadError::Source(format!(
            "Expected 206 Partial Content with matching Content-Range, got {status}"
        )));
    }
    let Some(cr) = resp
        .headers()
        .get(CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
    else {
        return Err(PieceDownloadError::Source(
            "Expected 206 Partial Content with matching Content-Range, \
             but Content-Range header is missing"
                .to_string(),
        ));
    };
    let Some(parsed) = parse_content_range(cr) else {
        return Err(PieceDownloadError::Source(format!(
            "Malformed Content-Range header: {cr:?}"
        )));
    };
    if parsed.start != range.start {
        return Err(PieceDownloadError::Source(format!(
            "Expected 206 Partial Content with matching Content-Range: \
             requested start {} but got {}",
            range.start, parsed.start
        )));
    }
    if let (Some(want_total), Some(got_total)) = (expected_total, parsed.total) {
        if got_total != want_total {
            return Err(PieceDownloadError::Source(format!(
                "mirror size mismatch: expected total {want_total} but got \
                 {got_total} (serving a different file)"
            )));
        }
    }

    let content_encoding = resp
        .headers()
        .get(CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .unwrap_or("");
    if !content_encoding.is_empty() && !content_encoding.eq_ignore_ascii_case("identity") {
        return Err(PieceDownloadError::Source(format!(
            "Server answered the range request with Content-Encoding: {content_encoding}"
        )));
    }
    *final_url = Some(resp.url().to_string());

    let mut stream = resp.bytes_stream();
    // Cap writes at the range length so an over-sending server cannot pwrite into the next piece
    let max_bytes = range.end - range.start + 1;
    let writer = ChunkWriter::spawn(
        Arc::clone(file),
        range.start,
        Some(max_bytes),
        Arc::clone(completed),
        worker_completed.cloned(),
        Some(Arc::clone(piece_completed)),
    );

    let mut received: u64 = 0;
    loop {
        let next = match with_idle_timeout(idle_timeout, cancel_token, stream.next()).await {
            Ok(next) => next,
            Err(IdleError::Cancelled) => {
                let _ = writer.finish().await;
                return Err(PieceDownloadError::Cancelled);
            }
            Err(IdleError::TimedOut) => {
                finish_piece_writer(writer).await?;
                return Err(PieceDownloadError::Source(
                    "Stream error: read timeout".to_string(),
                ));
            }
        };
        match next {
            Some(Ok(mut bytes)) => {
                let room = max_bytes - received;
                if bytes.len() as u64 > room {
                    bytes.truncate(room as usize);
                }
                let len = bytes.len();
                received += len as u64;
                throttle.acquire(len).await;

                if !writer.send(bytes).await {
                    return match finish_piece_writer(writer).await {
                        Ok(_) => Err(PieceDownloadError::Storage(
                            "Writer task closed unexpectedly".to_string(),
                        )),
                        Err(error) => Err(error),
                    };
                }
                if received >= max_bytes {
                    finish_piece_writer(writer).await?;
                    return Ok(());
                }
            }
            Some(Err(e)) => {
                finish_piece_writer(writer).await?;
                return Err(PieceDownloadError::Source(format!("Stream error: {e}")));
            }
            None => {
                finish_piece_writer(writer).await?;
                return Ok(());
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum IdleError {
    Cancelled,
    TimedOut,
}

async fn with_idle_timeout<T>(
    limit: Option<std::time::Duration>,
    cancel_token: &CancellationToken,
    fut: impl std::future::Future<Output = T>,
) -> Result<T, IdleError> {
    tokio::select! {
        _ = cancel_token.cancelled() => Err(IdleError::Cancelled),
        out = async {
            match limit {
                Some(d) => tokio::time::timeout(d, fut).await.map_err(|_| IdleError::TimedOut),
                None => Ok(fut.await),
            }
        } => out,
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ContentRange {
    start: u64,
    end: u64,
    total: Option<u64>,
}

fn parse_content_range(value: &str) -> Option<ContentRange> {
    let value = value.trim();
    let (unit, rest) = value.split_once(' ')?;
    if !unit.eq_ignore_ascii_case("bytes") {
        return None;
    }
    let (span, total) = rest.trim().split_once('/')?;
    let (start, end) = span.split_once('-')?;
    let start = start.trim().parse::<u64>().ok()?;
    let end = end.trim().parse::<u64>().ok()?;
    if end < start {
        return None;
    }
    let total = match total.trim() {
        "*" => None,
        t => Some(t.parse::<u64>().ok()?),
    };
    Some(ContentRange { start, end, total })
}

fn parse_size_option(value: Option<&Value>) -> Option<u64> {
    let v = value?;
    if let Some(n) = v.as_u64() {
        return Some(n);
    }
    let s = v.as_str()?.trim();
    if s.is_empty() {
        return None;
    }
    let (num, mult): (&str, u64) = match s.as_bytes().last() {
        Some(b'K') | Some(b'k') => (&s[..s.len() - 1], 1024),
        Some(b'M') | Some(b'm') => (&s[..s.len() - 1], 1024 * 1024),
        Some(b'G') | Some(b'g') => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        _ => (s, 1),
    };
    num.trim().parse::<u64>().ok()?.checked_mul(mult)
}

#[cfg(unix)]
fn pwrite_all(file: &std::fs::File, offset: u64, buf: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

#[cfg(windows)]
fn pwrite_all(file: &std::fs::File, offset: u64, buf: &[u8]) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    let mut written = 0usize;
    while written < buf.len() {
        let n = file.seek_write(&buf[written..], offset + written as u64)?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        written += n;
    }
    Ok(())
}

async fn sync_file(file: &Arc<std::fs::File>) -> Result<(), String> {
    let file = Arc::clone(file);
    tokio::task::spawn_blocking(move || file.sync_all())
        .await
        .map_err(|e| format!("sync task join error: {e}"))?
        .map_err(|e| format!("Sync failed: {e}"))
}

struct ChunkWriter {
    tx: tokio::sync::mpsc::Sender<Bytes>,
    join: tokio::task::JoinHandle<Result<u64, String>>,
}

impl ChunkWriter {
    fn spawn(
        file: Arc<std::fs::File>,
        start_offset: u64,
        max_bytes: Option<u64>,
        completed: Arc<AtomicU64>,
        worker_completed: Option<Arc<AtomicU64>>,
        piece_completed: Option<Arc<AtomicU32>>,
    ) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(16);
        let join = tokio::task::spawn_blocking(move || {
            let mut offset = start_offset;
            let mut written: u64 = 0;
            let mut remaining = max_bytes;
            while let Some(mut bytes) = rx.blocking_recv() {
                if let Some(rem) = remaining {
                    if rem == 0 {
                        continue;
                    }
                    if (bytes.len() as u64) > rem {
                        bytes.truncate(rem as usize);
                    }
                }
                if bytes.is_empty() {
                    continue;
                }
                if let Err(e) = pwrite_all(&file, offset, &bytes) {
                    return Err(format!("Write failed: {e}"));
                }
                let n = bytes.len() as u64;
                offset += n;
                written += n;
                if let Some(rem) = remaining.as_mut() {
                    *rem -= n;
                }
                completed.fetch_add(n, Ordering::Relaxed);
                if let Some(ref wc) = worker_completed {
                    wc.fetch_add(n, Ordering::Relaxed);
                }
                if let Some(ref pc) = piece_completed {
                    pc.fetch_add(n as u32, Ordering::Relaxed);
                }
            }
            Ok(written)
        });
        Self { tx, join }
    }

    async fn send(&self, bytes: Bytes) -> bool {
        self.tx.send(bytes).await.is_ok()
    }

    async fn finish(self) -> Result<u64, String> {
        drop(self.tx);
        match self.join.await {
            Ok(r) => r,
            Err(e) => Err(format!("write task join error: {e}")),
        }
    }
}

async fn finish_piece_writer(writer: ChunkWriter) -> Result<u64, PieceDownloadError> {
    writer.finish().await.map_err(PieceDownloadError::Storage)
}

fn is_transient_single_error(e: &str) -> bool {
    if e.contains("cancelled")
        || e.contains(STALE_PART_REMOVED)
        || e.contains("Cloudflare")
        || e.contains("cloudflare")
        || e.contains("checksum")
        || e.contains("stalled")
    {
        return false;
    }
    if e.contains("HTTP error: 412") {
        return true;
    }
    if e.contains("HTTP error:") {
        return false;
    }
    e.contains("Download failed:")
        || e.contains("Stream error")
        || e.contains("error reading a body")
        || e.contains("Writer task")
        || e.contains("connection")
}

async fn run_single_download(
    client: &Client,
    uri: &str,
    part_path: &Path,
    headers: &HeaderMap,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    cancel_token: CancellationToken,
    filename: &str,
    dir_path: &Path,
    throttle: Throttle,
    stall: StallWatchdog,
    filename_was_url_derived: bool,
    force_range: bool,
    auto_rename: bool,
    idle_timeout: Option<std::time::Duration>,
    integrity: (
        &Option<super::hasher::PieceChecksums>,
        &Option<super::hasher::WholeChecksum>,
    ),
) -> Result<(PathBuf, Option<String>), String> {
    let existing_size = if part_path.exists() {
        fs::metadata(part_path).map(|m| m.len()).unwrap_or(0)
    } else {
        0
    };

    completed.store(existing_size, Ordering::Relaxed);

    let mut req = client
        .get(uri)
        .headers(headers.clone())
        .header(ACCEPT_ENCODING, "identity");
    if existing_size > 0 {
        req = req.header(RANGE, format!("bytes={existing_size}-"));
    } else if force_range {
        req = req.header(RANGE, "bytes=0-");
    }
    tracing::debug!(
        "Single download request: uri={uri}, existing={existing_size}, force_range={force_range}"
    );

    let resp = with_idle_timeout(idle_timeout, &cancel_token, req.send())
        .await
        .map_err(|e| match e {
            IdleError::Cancelled => "Download cancelled".to_string(),
            IdleError::TimedOut => "Download failed: read timeout".to_string(),
        })?
        .map_err(|e| {
            if cancel_token.is_cancelled() {
                "Download cancelled".to_string()
            } else {
                format!("Download failed: {e}")
            }
        })?;

    let status = resp.status().as_u16();

    if looks_like_cloudflare_block(resp.headers(), status) {
        log_cloudflare_diagnostic(client, headers, resp.headers(), uri);
        let err = cloudflare_error(uri, status);
        drain_response_body(resp).await;
        return Err(err);
    }

    if status == 416
        && existing_size > 0
        && unsatisfied_range_total(resp.headers()) == Some(existing_size)
    {
        let last_modified = resp
            .headers()
            .get(LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        drain_response_body(resp).await;
        total.store(existing_size, Ordering::Relaxed);
        verify_part(part_path, integrity).await?;
        let final_path = finalize_download(part_path, filename, dir_path, auto_rename)?;
        return Ok((final_path, last_modified));
    }

    if status == 416 && existing_size > 0 {
        tracing::warn!("Got 416 with existing_size={existing_size}, deleting stale .part");
        if let Err(e) = fs::remove_file(part_path) {
            return Err(format!("Failed to delete stale .part: {e}"));
        }
        return Err(format!("Download will retry: {STALE_PART_REMOVED}"));
    }

    if status >= 400 {
        let header_dump = format!("{:?}", resp.headers());
        let body = read_body_prefix(resp, 512, idle_timeout, &cancel_token).await;
        let snippet = String::from_utf8_lossy(&body);
        tracing::warn!(
            "Single download got HTTP {status} for {uri}; force_range={force_range}; \
             headers={header_dump}; body[..512]={snippet:?}"
        );
        return Err(format!("HTTP error: {status}"));
    }

    let write_offset = if existing_size > 0 && status != 206 {
        tracing::warn!(
            "Server ignored Range resume request (status {status}, existing={existing_size}); \
             restarting download from scratch"
        );
        completed.store(0, Ordering::Relaxed);
        0
    } else if existing_size > 0 && status == 206 {
        let range_valid = resp
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_content_range)
            .map(|cr| cr.start)
            == Some(existing_size);

        if !range_valid {
            tracing::warn!(
                "Server returned 206 with mismatched Content-Range (expected start={existing_size}); \
                 deleting stale .part and retrying"
            );
            drain_response_body(resp).await;
            if let Err(e) = fs::remove_file(part_path) {
                return Err(format!("Failed to delete stale .part: {e}"));
            }
            return Err(format!("Download will retry: {STALE_PART_REMOVED}"));
        }
        existing_size
    } else {
        existing_size
    };

    let resp_last_modified = resp
        .headers()
        .get(LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let final_filename = if filename_was_url_derived {
        filename_from_content_disposition(resp.headers())
            .or_else(|| {
                filename_with_content_type_extension(
                    filename,
                    content_type_from_headers(resp.headers()).as_deref(),
                )
            })
            .unwrap_or_else(|| filename.to_string())
    } else {
        filename.to_string()
    };

    if let Some(cl) = resp.content_length() {
        if cl > 0 {
            total.store(write_offset + cl, Ordering::Relaxed);
        }
    }

    let file = {
        let f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(part_path)
            .map_err(|e| format!("Failed to open file: {e}"))?;
        if write_offset == 0 && existing_size > 0 {
            f.set_len(0)
                .map_err(|e| format!("Failed to truncate stale .part: {e}"))?;
        }
        Arc::new(f)
    };

    let speed_cancel = cancel_token.clone();
    let speed_completed = completed.clone();
    let speed_val = speed.clone();
    let speed_total = total.clone();
    let speed_stall = stall.clone();
    let speed_task = tokio::spawn(async move {
        run_speed_tracker(
            speed_completed,
            speed_val,
            speed_total,
            speed_cancel,
            speed_stall,
        )
        .await;
    });

    let mut stream = resp.bytes_stream();
    let writer = ChunkWriter::spawn(
        Arc::clone(&file),
        write_offset,
        None,
        Arc::clone(&completed),
        None,
        None,
    );

    let result: Result<(), String> = loop {
        let next = match with_idle_timeout(idle_timeout, &cancel_token, stream.next()).await {
            Ok(next) => next,
            Err(IdleError::Cancelled) => break Err("Download cancelled".to_string()),
            Err(IdleError::TimedOut) => break Err("Download failed: read timeout".to_string()),
        };
        match next {
            Some(Ok(bytes)) => {
                let len = bytes.len();
                throttle.acquire(len).await;
                if !writer.send(bytes).await {
                    break Err("Writer task closed unexpectedly".to_string());
                }
            }
            Some(Err(e)) => {
                break Err(format!("Download failed: {e}"));
            }
            None => {
                break Ok(());
            }
        }
    };

    let writer_result = writer.finish().await;

    let result = match (result, writer_result) {
        (Ok(()), Ok(_)) => sync_file(&file).await,
        (Ok(()), Err(e)) => Err(e),
        (Err(e), _) => Err(e),
    };

    speed_task.abort();
    speed.store(0, Ordering::Relaxed);

    if stall.flag.load(Ordering::Acquire) {
        return Err(format!(
            "Download stalled: speed below {} B/s for {}s",
            stall.lowest_speed,
            stall.timeout.as_secs(),
        ));
    }

    result?;
    verify_part(part_path, integrity).await?;
    tracing::debug!(
        "run_single_download finalizing: part_path={part_path:?}, final_filename={final_filename:?}"
    );
    let final_path = finalize_download(part_path, &final_filename, dir_path, auto_rename)?;
    Ok((final_path, resp_last_modified))
}

fn unsatisfied_range_total(headers: &HeaderMap) -> Option<u64> {
    let v = headers.get(CONTENT_RANGE)?.to_str().ok()?.trim();
    let rest = v.strip_prefix("bytes")?.trim_start().strip_prefix("*/")?;
    rest.trim().parse().ok()
}

async fn verify_part(
    part_path: &Path,
    integrity: (
        &Option<super::hasher::PieceChecksums>,
        &Option<super::hasher::WholeChecksum>,
    ),
) -> Result<(), String> {
    if integrity.0.is_none() && integrity.1.is_none() {
        return Ok(());
    }
    let len = match fs::metadata(part_path) {
        Ok(meta) => meta.len(),
        Err(e) => {
            let _ = fs::remove_file(part_path);
            return Err(format!("stat for verify: {e}"));
        }
    };
    verify_output(part_path, len, integrity.0, integrity.1).await
}

async fn run_speed_tracker(
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    total: Arc<AtomicU64>,
    cancel_token: CancellationToken,
    stall: StallWatchdog,
) {
    let started_at = tokio::time::Instant::now();
    let mut last_bytes = completed.load(Ordering::Relaxed);
    let mut last_time = started_at;
    let mut ema = SpeedEma::new();
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(250));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut below_since: Option<tokio::time::Instant> = None;
    let watchdog_active = stall.lowest_speed > 0;
    let unknown_len_grace = stall.timeout;

    loop {
        interval.tick().await;
        if cancel_token.is_cancelled() {
            break;
        }

        let now = tokio::time::Instant::now();
        let elapsed = now.duration_since(last_time).as_secs_f64();
        let current = completed.load(Ordering::Relaxed);

        if elapsed >= MIN_EMA_SAMPLE_SECS {
            let delta = current.saturating_sub(last_bytes);
            speed.store(ema.update(delta, elapsed), Ordering::Relaxed);
            last_bytes = current;
            last_time = now;
        }

        let t = total.load(Ordering::Relaxed);
        let armed =
            watchdog_active && (t > 0 || now.duration_since(started_at) >= unknown_len_grace);
        if armed {
            if ema.get() < stall.lowest_speed {
                let started = below_since.get_or_insert(now);
                if now.duration_since(*started) >= stall.timeout {
                    tracing::warn!(
                        "Download stalled: EMA {} B/s below {} B/s for {}s",
                        ema.get(),
                        stall.lowest_speed,
                        stall.timeout.as_secs(),
                    );
                    stall.flag.store(true, Ordering::Release);
                    cancel_token.cancel();
                    break;
                }
            } else {
                below_since = None;
            }
        }

        if t > 0 && current >= t {
            break;
        }
    }
    speed.store(0, Ordering::Relaxed);
}

fn finalize_download(
    part_path: &Path,
    filename: &str,
    dir_path: &Path,
    auto_rename: bool,
) -> Result<PathBuf, String> {
    let final_name = if let Some(stripped) = filename.strip_suffix(PART_SUFFIX) {
        stripped.to_string()
    } else {
        filename.to_string()
    };
    let final_path = if auto_rename {
        super::util::dedup_path(dir_path, &final_name)
    } else {
        dir_path.join(&final_name)
    };
    tracing::debug!(
        "finalize_download: part_path={part_path:?}, filename={filename:?}, final_path={final_path:?}"
    );
    if part_path != final_path {
        fs::rename(part_path, &final_path).map_err(|e| format!("Failed to rename: {e}"))?;
    }
    Ok(final_path)
}

fn apply_remote_file_time(path: &Path, last_modified_str: &str) {
    if let Some(time) = parse_http_date(last_modified_str) {
        if let Err(e) = set_file_mtime(path, time) {
            tracing::warn!("Failed to set remote file time on {}: {e}", path.display());
        } else {
            tracing::info!(
                "Set remote file time on {}: {last_modified_str}",
                path.display()
            );
        }
    } else {
        tracing::warn!("Could not parse Last-Modified header: {last_modified_str}");
    }
}

fn parse_http_date(s: &str) -> Option<std::time::SystemTime> {
    httpdate::parse_http_date(s.trim()).ok()
}

fn set_file_mtime(path: &Path, time: std::time::SystemTime) -> std::io::Result<()> {
    let times = fs::FileTimes::new().set_modified(time);

    #[cfg(windows)]
    let file = {
        use std::os::windows::fs::OpenOptionsExt;

        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x02000000;
        fs::OpenOptions::new()
            .write(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)?
    };

    #[cfg(not(windows))]
    let file = fs::File::open(path).or_else(|_| fs::OpenOptions::new().write(true).open(path))?;

    file.set_times(times)
}

fn adopt_suggested_filename(
    suggested: &str,
    current_filename: &str,
    current_part_path: &Path,
    dir_path: &Path,
) -> Option<(String, std::path::PathBuf)> {
    let candidate = sanitize_filename(suggested);
    tracing::debug!(
        "adopt_suggested_filename: candidate={candidate:?}, current={current_filename:?}, \
         current_part={current_part_path:?}"
    );
    if candidate.is_empty() || candidate == current_filename {
        tracing::debug!("adopt_suggested_filename: rejected (empty or same name)");
        return None;
    }
    let current_has_bytes = current_part_path.exists()
        && fs::metadata(current_part_path)
            .map(|m| m.len() > 0)
            .unwrap_or(false);
    tracing::debug!("adopt_suggested_filename: current_has_bytes={current_has_bytes}");
    if current_has_bytes {
        tracing::debug!("adopt_suggested_filename: rejected (current .part has bytes)");
        return None;
    }
    let new_part = if candidate.ends_with(PART_SUFFIX) {
        dir_path.join(&candidate)
    } else {
        dir_path.join(format!("{candidate}{PART_SUFFIX}"))
    };
    let new_part_has_bytes = new_part != current_part_path
        && new_part.exists()
        && fs::metadata(&new_part)
            .map(|m| m.len() > 0)
            .unwrap_or(false);
    tracing::debug!(
        "adopt_suggested_filename: new_part={new_part:?}, new_part_has_bytes={new_part_has_bytes}"
    );
    if new_part_has_bytes {
        tracing::debug!("adopt_suggested_filename: rejected (target .part has bytes)");
        return None;
    }
    tracing::debug!("adopt_suggested_filename: accepted -> {candidate:?}, {new_part:?}");
    Some((candidate, new_part))
}

fn adopt_content_type_extension(
    current_filename: &str,
    content_type: &str,
    current_part_path: &Path,
    dir_path: &Path,
) -> Option<(String, std::path::PathBuf)> {
    let candidate = filename_with_content_type_extension(current_filename, Some(content_type))?;
    let new_part = dir_path.join(format!("{candidate}{PART_SUFFIX}"));
    if current_part_path.exists()
        && fs::metadata(current_part_path)
            .map(|m| m.len() > 0)
            .unwrap_or(false)
    {
        return Some((candidate, current_part_path.to_path_buf()));
    }
    if new_part != current_part_path
        && new_part.exists()
        && fs::metadata(&new_part)
            .map(|m| m.len() > 0)
            .unwrap_or(false)
    {
        return None;
    }
    Some((candidate, new_part))
}

fn filename_with_content_type_extension(
    filename: &str,
    content_type: Option<&str>,
) -> Option<String> {
    if filename_has_extension(filename) {
        return None;
    }
    let ext = extension_from_content_type(content_type?)?;
    let base = filename.strip_suffix(PART_SUFFIX).unwrap_or(filename);
    Some(format!("{base}.{ext}"))
}

fn filename_has_extension(filename: &str) -> bool {
    let base = filename.strip_suffix(PART_SUFFIX).unwrap_or(filename);
    Path::new(base)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| !ext.trim().is_empty())
        .unwrap_or(false)
}

fn content_type_from_headers(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(CONTENT_TYPE)?.to_str().ok()?;
    let mime = value.split(';').next()?.trim().to_ascii_lowercase();
    if mime.is_empty() {
        None
    } else {
        Some(mime)
    }
}

fn extension_from_content_type(content_type: &str) -> Option<&'static str> {
    match content_type {
        "image/png" => Some("png"),
        "image/jpeg" | "image/jpg" => Some("jpg"),
        "image/gif" => Some("gif"),
        "image/webp" => Some("webp"),
        "image/bmp" => Some("bmp"),
        "image/svg+xml" => Some("svg"),
        "image/avif" => Some("avif"),
        "video/mp4" => Some("mp4"),
        "video/x-matroska" => Some("mkv"),
        "video/webm" => Some("webm"),
        "video/quicktime" => Some("mov"),
        "video/x-msvideo" => Some("avi"),
        "audio/mpeg" => Some("mp3"),
        "audio/aac" => Some("aac"),
        "audio/ogg" => Some("ogg"),
        "audio/wav" | "audio/x-wav" => Some("wav"),
        "audio/flac" => Some("flac"),
        "application/pdf" => Some("pdf"),
        "application/zip" => Some("zip"),
        "application/gzip" => Some("gz"),
        "application/x-7z-compressed" => Some("7z"),
        "application/vnd.rar" => Some("rar"),
        "application/json" => Some("json"),
        "application/xml" | "text/xml" => Some("xml"),
        "application/x-bittorrent" => Some("torrent"),
        "text/plain" => Some("txt"),
        "text/html" => Some("html"),
        "text/css" => Some("css"),
        "text/csv" => Some("csv"),
        "text/vtt" => Some("vtt"),
        _ => None,
    }
}

pub fn infer_filename_from_uri(uri: &str) -> String {
    let without_hash = uri.split('#').next().unwrap_or(uri);
    let without_query = without_hash.split('?').next().unwrap_or(without_hash);
    let candidate = without_query.rsplit('/').next().unwrap_or("").trim();
    if candidate.is_empty() || !candidate.contains('.') {
        "download".to_string()
    } else {
        url_decode(candidate)
    }
}

fn is_placeholder_download_name(name: &str) -> bool {
    if name == "download" {
        return true;
    }
    let Some(rest) = name.strip_prefix("download-") else {
        return false;
    };
    !rest.is_empty() && rest.chars().all(|c| c.is_ascii_hexdigit())
}

pub fn filename_from_content_disposition(headers: &HeaderMap) -> Option<String> {
    let raw_bytes = headers.get("content-disposition")?.as_bytes();
    let raw = String::from_utf8_lossy(raw_bytes);

    let mut star_value: Option<String> = None;
    let mut plain_value: Option<String> = None;

    for part in raw.split(';') {
        let part = part.trim();
        if let Some(rest) = part
            .strip_prefix("filename*=")
            .or_else(|| part.strip_prefix("FILENAME*="))
        {
            if let Some(encoded) = rest
                .split_once('\'')
                .and_then(|(_charset, remainder)| remainder.split_once('\''))
                .map(|(_lang, encoded)| encoded)
            {
                star_value = Some(url_decode(encoded.trim_matches('"')));
            }
        } else if let Some(rest) = part
            .strip_prefix("filename=")
            .or_else(|| part.strip_prefix("FILENAME="))
            .or_else(|| part.strip_prefix("Filename="))
        {
            let val = rest.trim().trim_matches('"').trim_matches('\'');
            if !val.is_empty() {
                plain_value = Some(url_decode(val));
            }
        }
    }

    let candidate = star_value.or(plain_value)?;
    let trimmed = candidate.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(sanitize_filename(trimmed))
    }
}

fn url_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut hm = HeaderMap::new();
        for (k, v) in pairs {
            hm.insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        hm
    }

    #[test]
    fn filename_from_quoted_attachment() {
        let headers = h(&[(
            "content-disposition",
            "attachment; filename=\"StoragePeek.jar\"",
        )]);
        assert_eq!(
            filename_from_content_disposition(&headers).as_deref(),
            Some("StoragePeek.jar")
        );
    }

    #[test]
    fn filename_from_unquoted_attachment() {
        let headers = h(&[("content-disposition", "attachment; filename=Build_v2.zip")]);
        assert_eq!(
            filename_from_content_disposition(&headers).as_deref(),
            Some("Build_v2.zip")
        );
    }

    #[test]
    fn filename_from_rfc5987() {
        let headers = h(&[(
            "content-disposition",
            "attachment; filename*=UTF-8''Storage%20Peek.jar",
        )]);
        assert_eq!(
            filename_from_content_disposition(&headers).as_deref(),
            Some("Storage Peek.jar")
        );
    }

    #[test]
    fn build_headers_parses_header_array() {
        let mut options = Map::new();
        options.insert(
            "header".to_string(),
            Value::Array(vec![
                Value::String("User-Agent: pan.baidu.com".to_string()),
                Value::String("Referer: https://pan.baidu.com/disk/home".to_string()),
                Value::String("Cookie: BDUSS=deadbeef".to_string()),
            ]),
        );
        let headers = build_headers(&options);
        assert_eq!(
            headers.get(risuko_http::header::USER_AGENT).unwrap(),
            "pan.baidu.com"
        );
        assert_eq!(
            headers.get(risuko_http::header::REFERER).unwrap(),
            "https://pan.baidu.com/disk/home"
        );
        assert_eq!(
            headers.get(risuko_http::header::COOKIE).unwrap(),
            "BDUSS=deadbeef"
        );
    }

    #[test]
    fn build_headers_parses_header_string() {
        let mut options = Map::new();
        options.insert(
            "header".to_string(),
            Value::String("User-Agent: pan.baidu.com\nReferer: https://pan.baidu.com/".to_string()),
        );
        let headers = build_headers(&options);
        assert_eq!(
            headers.get(risuko_http::header::USER_AGENT).unwrap(),
            "pan.baidu.com"
        );
        assert_eq!(
            headers.get(risuko_http::header::REFERER).unwrap(),
            "https://pan.baidu.com/"
        );
    }

    #[test]
    fn build_headers_adds_user_agent_option_unless_header_sets_one() {
        let mut options = Map::new();
        options.insert("user-agent".to_string(), Value::String("ua-option".into()));
        let headers = build_headers(&options);
        assert_eq!(
            headers.get(risuko_http::header::USER_AGENT).unwrap(),
            "ua-option"
        );

        options.insert(
            "header".to_string(),
            Value::String("User-Agent: ua-header".into()),
        );
        let headers = build_headers(&options);
        assert_eq!(
            headers.get(risuko_http::header::USER_AGENT).unwrap(),
            "ua-header"
        );
    }

    #[test]
    fn headers_have_cookie_name_matches_case_insensitively() {
        let headers = h(&[("cookie", "a=1; cf_clearance=abc; xf_session=def")]);

        assert!(headers_have_cookie_name(&headers, "CF_CLEARANCE"));
        assert!(headers_have_cookie_name(&headers, "xf_session"));
        assert!(!headers_have_cookie_name(&headers, "missing"));
    }

    #[test]
    fn effective_cookies_have_name_checks_client_jar() {
        let url = url::Url::parse("https://example.com/file.bin").unwrap();
        let jar = Arc::new(risuko_http::Jar::new());
        jar.add_cookie_str("cf_clearance=abc; Path=/; Domain=example.com", &url);
        let client = Client::builder().cookie_provider(jar).build().unwrap();

        assert!(effective_cookies_have_name(
            &client,
            &HeaderMap::new(),
            url.as_str(),
            "cf_clearance",
        ));
    }

    #[test]
    fn effective_cookies_have_name_treats_manual_cookie_as_authoritative() {
        let url = url::Url::parse("https://example.com/file.bin").unwrap();
        let jar = Arc::new(risuko_http::Jar::new());
        jar.add_cookie_str("cf_clearance=abc; Path=/; Domain=example.com", &url);
        let client = Client::builder().cookie_provider(jar).build().unwrap();
        let headers = h(&[("cookie", "session=manual")]);

        assert!(!effective_cookies_have_name(
            &client,
            &headers,
            url.as_str(),
            "cf_clearance",
        ));
    }

    #[test]
    fn filename_from_rfc5987_utf8_multibyte() {
        let headers = h(&[(
            "content-disposition",
            "attachment; filename*=UTF-8''%E4%B8%AD.txt",
        )]);
        assert_eq!(
            filename_from_content_disposition(&headers).as_deref(),
            Some("\u{4e2d}.txt")
        );
    }

    #[test]
    fn filename_strips_path_traversal() {
        let headers = h(&[(
            "content-disposition",
            "attachment; filename=\"../../etc/passwd\"",
        )]);
        let got = filename_from_content_disposition(&headers).unwrap();
        assert!(!got.contains('/'));
        assert!(!got.contains('\\'));
    }

    #[test]
    fn filename_absent_returns_none() {
        let headers = h(&[("content-type", "application/zip")]);
        assert_eq!(filename_from_content_disposition(&headers), None);
    }

    #[test]
    fn content_type_adds_extension_to_extensionless_filename() {
        assert_eq!(
            filename_with_content_type_extension("download", Some("image/png")).as_deref(),
            Some("download.png")
        );
        assert_eq!(
            filename_with_content_type_extension("download.part", Some("image/png")).as_deref(),
            Some("download.png")
        );
    }

    #[test]
    fn content_type_does_not_replace_existing_extension() {
        assert_eq!(
            filename_with_content_type_extension("photo.jpg", Some("image/png")),
            None
        );
    }

    #[test]
    fn content_type_ignores_unknown_mime() {
        assert_eq!(
            filename_with_content_type_extension("download", Some("application/octet-stream")),
            None
        );
    }

    #[test]
    fn cloudflare_detection_cf_ray_403() {
        let headers = h(&[("cf-ray", "abc-IAD"), ("server", "cloudflare")]);
        assert!(looks_like_cloudflare_block(&headers, 403));
    }

    #[test]
    fn cloudflare_detection_503_challenge() {
        let headers = h(&[("server", "cloudflare"), ("cf-mitigated", "challenge")]);
        assert!(looks_like_cloudflare_block(&headers, 503));
    }

    #[test]
    fn cloudflare_detection_429_with_cf_ray() {
        let headers = h(&[("cf-ray", "xyz-LHR")]);
        assert!(looks_like_cloudflare_block(&headers, 429));
    }

    #[test]
    fn cloudflare_detection_skipped_on_2xx() {
        let headers = h(&[("server", "cloudflare"), ("cf-ray", "abc")]);
        assert!(!looks_like_cloudflare_block(&headers, 200));
    }

    #[test]
    fn cloudflare_detection_skipped_on_plain_403() {
        let headers = h(&[("server", "nginx")]);
        assert!(!looks_like_cloudflare_block(&headers, 403));
    }

    #[test]
    fn cloudflare_error_includes_marker_and_host() {
        let msg = cloudflare_error("https://dl.example.com/file.zip", 403);
        assert!(msg.starts_with(CLOUDFLARE_MARKER));
        assert!(msg.contains("host=dl.example.com"));
        assert!(msg.contains("status=403"));
    }

    #[test]
    fn piece_queue_partitions_content_into_1mib_pieces() {
        let total = PIECE_SIZE * 3 + 7;
        let q = PieceQueue::new(total);
        assert_eq!(q.pieces.len(), 4);
        assert_eq!(q.pieces[0].offset, 0);
        assert_eq!(q.pieces[0].length, PIECE_SIZE as u32);
        assert_eq!(q.pieces[3].offset, PIECE_SIZE * 3);
        assert_eq!(q.pieces[3].length, 7);
        let sum: u64 = q.pieces.iter().map(|p| p.length as u64).sum();
        assert_eq!(sum, total);
    }

    #[test]
    fn claim_marks_inflight_and_skips_busy_pieces() {
        let q = PieceQueue::new(PIECE_SIZE * 3);
        let a = q.claim_next().unwrap();
        let b = q.claim_next().unwrap();
        let c = q.claim_next().unwrap();
        assert_ne!(a, b);
        assert_ne!(b, c);
        assert!(q.claim_next().is_none(), "queue should be exhausted");
    }

    #[test]
    fn release_returns_piece_to_pool_for_work_stealing() {
        let q = PieceQueue::new(PIECE_SIZE * 2);
        let a = q.claim_next().unwrap();
        let b = q.claim_next().unwrap();
        assert!(q.claim_next().is_none());
        q.release(a);
        let stolen = q.claim_next().unwrap();
        assert_eq!(stolen, a);
        q.complete(b);
        q.complete(a);
        assert!(q.claim_next().is_none());
    }

    #[test]
    fn complete_prevents_reclaim() {
        let q = PieceQueue::new(PIECE_SIZE * 2);
        let a = q.claim_next().unwrap();
        q.complete(a);
        let b = q.claim_next().unwrap();
        assert_ne!(b, a, "completed piece must not be reclaimed");
        q.complete(b);
        assert!(q.claim_next().is_none());
    }

    #[test]
    fn partial_error_progress_resets_retry_budget() {
        let mut retry_count = 3;

        assert!(!charge_retry_if_no_progress(&mut retry_count, 1));
        assert_eq!(retry_count, 0);
        assert!(charge_retry_if_no_progress(&mut retry_count, 0));
        assert_eq!(retry_count, 1);
    }

    #[test]
    fn productive_source_errors_use_a_bounded_backoff() {
        assert_eq!(
            partial_error_backoff(1),
            std::time::Duration::from_millis(50)
        );
        assert_eq!(
            partial_error_backoff(3),
            std::time::Duration::from_millis(150)
        );
        assert_eq!(partial_error_backoff(u32::MAX), PARTIAL_ERROR_BACKOFF_MAX);
    }

    #[tokio::test]
    async fn piece_writer_failures_are_classified_as_storage_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("read-only.bin");
        std::fs::write(&path, b"existing").unwrap();
        let file = Arc::new(std::fs::File::open(path).unwrap());
        let completed = Arc::new(AtomicU64::new(0));
        let piece_completed = Arc::new(AtomicU32::new(0));
        let writer = ChunkWriter::spawn(
            file,
            0,
            Some(1),
            Arc::clone(&completed),
            None,
            Some(Arc::clone(&piece_completed)),
        );
        let _ = writer.send(Bytes::from_static(b"x")).await;

        let error = finish_piece_writer(writer).await.unwrap_err();

        assert!(matches!(error, PieceDownloadError::Storage(_)));
        assert_eq!(completed.load(Ordering::Relaxed), 0);
        assert_eq!(piece_completed.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn relocate_partial_moves_part_and_chunks_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let old_dir = dir.path().join("old");
        let new_dir = dir.path().join("new");
        std::fs::create_dir_all(&old_dir).unwrap();
        std::fs::create_dir_all(&new_dir).unwrap();

        let old_part = old_dir.join("file.bin.part");
        let old_meta = {
            let mut p = old_part.as_os_str().to_owned();
            p.push(CHUNK_META_SUFFIX);
            std::path::PathBuf::from(p)
        };
        std::fs::write(&old_part, b"partial-bytes").unwrap();
        std::fs::write(&old_meta, b"{\"version\":2}").unwrap();

        let moved = relocate_partial(
            old_dir.to_str().unwrap(),
            "file.bin",
            new_dir.to_str().unwrap(),
            "renamed.bin",
        )
        .expect("relocate");
        assert!(moved);
        assert!(!old_part.exists());
        assert!(!old_meta.exists());
        let new_part = new_dir.join("renamed.bin.part");
        let new_meta = {
            let mut p = new_part.as_os_str().to_owned();
            p.push(CHUNK_META_SUFFIX);
            std::path::PathBuf::from(p)
        };
        assert!(new_part.exists());
        assert!(new_meta.exists());
        assert_eq!(std::fs::read(&new_part).unwrap(), b"partial-bytes");
    }

    #[test]
    fn relocate_partial_noop_when_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let moved = relocate_partial(
            dir.path().to_str().unwrap(),
            "a.bin",
            dir.path().to_str().unwrap(),
            "a.bin",
        )
        .unwrap();
        assert!(!moved);
    }

    #[test]
    fn relocate_partial_strips_path_separators_from_output_names() {
        let dir = tempfile::tempdir().unwrap();
        let old_dir = dir.path().join("old");
        let new_dir = dir.path().join("new");
        std::fs::create_dir_all(&old_dir).unwrap();
        std::fs::create_dir_all(&new_dir).unwrap();
        std::fs::write(old_dir.join("safe.bin.part"), b"partial").unwrap();

        let moved = relocate_partial(
            old_dir.to_str().unwrap(),
            "../safe.bin",
            new_dir.to_str().unwrap(),
            "subdir/renamed.bin",
        )
        .expect("relocate sanitized names");
        assert!(moved);
        assert!(new_dir.join("renamed.bin.part").exists());
        assert!(!new_dir.join("subdir").exists());
        assert!(!dir.path().join("safe.bin.part").exists());
    }

    #[test]
    fn relocate_partial_rejects_orphan_chunks_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let old_dir = dir.path().join("old");
        let new_dir = dir.path().join("new");
        std::fs::create_dir_all(&old_dir).unwrap();
        std::fs::create_dir_all(&new_dir).unwrap();
        std::fs::write(old_dir.join("file.bin.part"), b"partial").unwrap();
        let dest_part = new_dir.join("file.bin.part");
        let mut dest_meta = dest_part.as_os_str().to_owned();
        dest_meta.push(CHUNK_META_SUFFIX);
        std::fs::write(std::path::PathBuf::from(dest_meta), b"{}").unwrap();

        let err = relocate_partial(
            old_dir.to_str().unwrap(),
            "file.bin",
            new_dir.to_str().unwrap(),
            "file.bin",
        )
        .expect_err("orphan sidecar");
        assert!(err.contains("already exists"));
        assert!(old_dir.join("file.bin.part").exists());
    }

    #[test]
    fn detects_cross_device_rename_errors() {
        #[cfg(unix)]
        {
            let err = std::io::Error::from_raw_os_error(18);
            assert!(is_cross_device_error(&err));
        }
        #[cfg(windows)]
        {
            let err = std::io::Error::from_raw_os_error(17);
            assert!(is_cross_device_error(&err));
        }
        let other = std::io::Error::other("permission denied");
        assert!(!is_cross_device_error(&other));
    }

    #[test]
    fn byte_range_resume_requires_matching_strong_etags() {
        assert!(etags_strongly_equal("\"abc\"", "\"abc\""));
        assert!(!etags_strongly_equal("\"abc\"", "\"xyz\""));
        assert!(!etags_strongly_equal("W/\"abc\"", "\"abc\""));
        assert!(!etags_strongly_equal("W/\"abc\"", "W/\"abc\""));
        assert!(!etags_strongly_equal("abc", "abc"));
    }

    #[test]
    fn piece_meta_round_trip_is_sparse() {
        let dir = std::env::temp_dir().join(format!("risuko_piecemeta_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let part = dir.join("file.bin.part");
        std::fs::write(&part, vec![0u8; (PIECE_SIZE * 3) as usize]).unwrap();

        let q = Arc::new(PieceQueue::new(PIECE_SIZE * 3));
        q.pieces[0]
            .completed
            .store(PIECE_SIZE as u32, Ordering::Relaxed);
        q.pieces[0].state.store(PIECE_DONE, Ordering::Release);
        q.pieces[1]
            .completed
            .store(PIECE_SIZE as u32 / 2, Ordering::Relaxed);

        let etag = Some("\"abc\"".to_string());
        save_piece_meta(&part, &q, PIECE_SIZE * 3, &etag, &None);

        let loaded = load_piece_meta(&part, PIECE_SIZE * 3, &etag, &None).expect("meta");
        assert_eq!(loaded.version, META_VERSION);
        assert_eq!(loaded.content_length, PIECE_SIZE * 3);
        assert_eq!(loaded.pieces.len(), 2);
        let p0 = loaded.pieces.iter().find(|p| p.i == 0).unwrap();
        let p1 = loaded.pieces.iter().find(|p| p.i == 1).unwrap();
        assert_eq!(p0.c, PIECE_SIZE as u32);
        assert_eq!(p1.c, PIECE_SIZE as u32 / 2);

        let other = Some("\"xyz\"".to_string());
        assert!(load_piece_meta(&part, PIECE_SIZE * 3, &other, &None).is_none());

        save_piece_meta(&part, &q, PIECE_SIZE * 3, &etag, &None);
        let weak = Some("W/\"abc\"".to_string());
        assert!(load_piece_meta(&part, PIECE_SIZE * 3, &weak, &None).is_none());

        let lm = Some("Wed, 21 Oct 2015 07:28:00 GMT".to_string());
        save_piece_meta(&part, &q, PIECE_SIZE * 3, &None, &lm);
        assert!(load_piece_meta(&part, PIECE_SIZE * 3, &etag, &lm).is_none());

        delete_chunk_meta(&part);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn meta_fixture(
        name: &str,
        file_len: u64,
        content_length: u64,
    ) -> (tempfile::TempDir, PathBuf, Arc<PieceQueue>) {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join(format!("{name}.part"));
        let f = fs::File::create(&part).unwrap();
        f.set_len(file_len).unwrap();
        let q = Arc::new(PieceQueue::new(content_length));
        (dir, part, q)
    }

    #[test]
    fn short_part_file_resumes_and_clamps_progress() {
        let (_dir, part, q) = meta_fixture("short", PIECE_SIZE + PIECE_SIZE / 2, PIECE_SIZE * 4);
        q.pieces[0]
            .completed
            .store(PIECE_SIZE as u32, Ordering::Relaxed);
        q.pieces[1]
            .completed
            .store(PIECE_SIZE as u32, Ordering::Relaxed);
        q.pieces[3].completed.store(10, Ordering::Relaxed);
        let etag = Some("\"abc\"".to_string());
        save_piece_meta(&part, &q, PIECE_SIZE * 4, &etag, &None);

        let meta = load_piece_meta(&part, PIECE_SIZE * 4, &etag, &None).expect("meta");
        let c_of = |i: u32| meta.pieces.iter().find(|p| p.i == i).map(|p| p.c);
        assert_eq!(c_of(0), Some(PIECE_SIZE as u32));
        assert_eq!(c_of(1), Some(PIECE_SIZE as u32 / 2));
        assert_eq!(c_of(3), None, "piece wholly past EOF restarts");
    }

    #[test]
    fn oversized_part_file_discards_sidecar() {
        let (_dir, part, q) = meta_fixture("big", PIECE_SIZE * 2 + 1, PIECE_SIZE * 2);
        let etag = Some("\"abc\"".to_string());
        q.pieces[0].completed.store(1, Ordering::Relaxed);
        save_piece_meta(&part, &q, PIECE_SIZE * 2, &etag, &None);
        assert!(load_piece_meta(&part, PIECE_SIZE * 2, &etag, &None).is_none());
    }

    #[test]
    fn last_modified_resumes_without_strong_etag() {
        let (_dir, part, q) = meta_fixture("lm", PIECE_SIZE * 2, PIECE_SIZE * 2);
        q.pieces[0].completed.store(5, Ordering::Relaxed);
        let lm = Some("Wed, 21 Oct 2015 07:28:00 GMT".to_string());
        let weak = Some("W/\"x\"".to_string());
        save_piece_meta(&part, &q, PIECE_SIZE * 2, &weak, &lm);
        assert!(load_piece_meta(&part, PIECE_SIZE * 2, &None, &lm).is_some());

        let other = Some("Thu, 22 Oct 2015 07:28:00 GMT".to_string());
        save_piece_meta(&part, &q, PIECE_SIZE * 2, &weak, &lm);
        assert!(load_piece_meta(&part, PIECE_SIZE * 2, &weak, &other).is_none());

        delete_chunk_meta(&part);
        save_piece_meta(&part, &q, PIECE_SIZE * 2, &weak, &None);
        assert!(!chunk_meta_path(&part).exists());
    }

    #[test]
    fn piece_size_scales_with_file_and_resume_keeps_saved_size() {
        assert_eq!(choose_piece_size(100 * PIECE_SIZE, 16), PIECE_SIZE);
        assert_eq!(choose_piece_size(1024 * PIECE_SIZE, 16), 16 * PIECE_SIZE);
        assert_eq!(choose_piece_size(10_000 * PIECE_SIZE, 16), MAX_PIECE_SIZE);
        let (_dir, part, _) = meta_fixture("ps", 0, 8 * PIECE_SIZE);
        let q = PieceQueue::with_piece_size(8 * PIECE_SIZE, 4 * PIECE_SIZE);
        assert_eq!(q.pieces.len(), 2);
        q.pieces[0].completed.store(7, Ordering::Relaxed);
        let etag = Some("\"abc\"".to_string());
        save_piece_meta(&part, &q, 8 * PIECE_SIZE, &etag, &None);
        let meta = load_piece_meta(&part, 8 * PIECE_SIZE, &etag, &None).expect("meta");
        assert_eq!(u64::from(meta.piece_size), 4 * PIECE_SIZE);
    }

    #[test]
    fn content_range_parsing() {
        assert_eq!(
            parse_content_range("bytes 5-9/20"),
            Some(ContentRange {
                start: 5,
                end: 9,
                total: Some(20)
            })
        );
        assert_eq!(
            parse_content_range("bytes 0-0/*"),
            Some(ContentRange {
                start: 0,
                end: 0,
                total: None
            })
        );
        assert!(parse_content_range("items 0-1/2").is_none());
        assert!(parse_content_range("bytes 9-5/20").is_none());
        assert!(parse_content_range("bytes */20").is_none());
        assert!(parse_content_range("garbage").is_none());
    }

    #[test]
    fn blacklist_is_a_preference_for_mirror_pool() {
        use super::super::uri_selector::Strategy;
        let pool = MirrorPool::new(
            vec!["http://a.test/f".to_string()],
            Strategy::Feedback,
            4,
            None,
        );
        for _ in 0..5 {
            pool.record_failure("a.test:80");
        }
        assert!(pool.has_live());
        let (idx, key) = pool.acquire().expect("sole mirror stays usable");
        assert_eq!(idx, 0);
        pool.release(&key);

        let pool = MirrorPool::new(
            vec!["http://a.test/f".to_string(), "http://b.test/f".to_string()],
            Strategy::Feedback,
            4,
            None,
        );
        for _ in 0..5 {
            pool.record_failure("a.test:80");
        }
        assert_eq!(pool.acquire().map(|p| p.0), Some(1));
    }

    #[test]
    fn resolved_target_strips_headers_cross_origin() {
        use super::super::uri_selector::Strategy;
        let orig = "https://a.test/f".to_string();
        let pool = MirrorPool::new(
            vec![orig.clone()],
            Strategy::Feedback,
            4,
            Some((orig.clone(), "https://cdn.test/x?sig=1".to_string())),
        );
        let base = h(&[
            ("authorization", "Bearer t"),
            ("cookie", "a=b"),
            ("x-token", "secret"),
            ("user-agent", "ua"),
        ]);
        let (url, headers, cached) = pool.target(0, &base);
        assert!(cached);
        assert_eq!(url, "https://cdn.test/x?sig=1");
        assert!(headers.get("authorization").is_none());
        assert!(headers.get("cookie").is_none());
        assert!(headers.get("x-token").is_none());
        assert_eq!(headers.get("user-agent").unwrap(), "ua");

        pool.record_resolved(0, "https://a.test/other");
        pool.forget_resolved(0);
        let (url, headers, cached) = pool.target(0, &base);
        assert!(!cached);
        assert_eq!(url, orig);
        assert!(headers.get("authorization").is_some());

        pool.record_resolved(0, "https://a.test/real");
        let (_, headers, cached) = pool.target(0, &base);
        assert!(cached);
        assert!(headers.get("authorization").is_some());
    }

    #[test]
    fn unsatisfied_range_total_parses_star_form() {
        let headers = h(&[("content-range", "bytes */1234")]);
        assert_eq!(unsatisfied_range_total(&headers), Some(1234));
        assert_eq!(
            unsatisfied_range_total(&h(&[("content-range", "bytes 0-1/5")])),
            None
        );
    }

    #[test]
    fn chunk_meta_save_is_atomic_and_probe_from_sidecar_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("f.bin.part");
        fs::write(&part, vec![0u8; 10]).unwrap();
        let queue = PieceQueue::new(PIECE_SIZE * 2);
        queue.pieces[0].completed.store(5, Ordering::Relaxed);
        let etag = Some("\"abc\"".to_string());
        save_piece_meta(&part, &queue, PIECE_SIZE * 2, &etag, &None);
        assert!(!dir.path().join("f.bin.part.chunks.tmp").exists());
        let probe = probe_from_sidecar(&part).expect("sidecar usable");
        assert_eq!(probe.content_length, PIECE_SIZE * 2);
        assert_eq!(probe.etag, etag);
        assert!(probe.range_supported);
        delete_chunk_meta(&part);
        assert!(probe_from_sidecar(&part).is_none());
    }

    #[test]
    fn same_origin_compares_scheme_host_port() {
        assert!(same_origin("https://a.test/x", "https://A.test:443/y"));
        assert!(!same_origin("https://a.test/x", "http://a.test/x"));
        assert!(!same_origin("https://a.test/x", "https://b.test/x"));
        assert!(!same_origin("https://a.test:8443/x", "https://a.test/x"));
    }

    #[test]
    fn queue_reports_inflight_pieces() {
        let q = PieceQueue::new(PIECE_SIZE * 2);
        assert!(!q.has_inflight());
        let a = q.claim_next().unwrap();
        assert!(q.has_inflight());
        q.complete(a);
        assert!(!q.has_inflight());
    }

    #[tokio::test]
    async fn idle_worker_wakes_when_the_last_piece_completes() {
        let q = Arc::new(PieceQueue::new(PIECE_SIZE));
        let idx = q.claim_next().unwrap();
        let waiter = tokio::spawn({
            let q = Arc::clone(&q);
            async move { q.wait_for_change().await }
        });
        tokio::task::yield_now().await;
        q.complete(idx);
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("completion wakes the idle worker")
            .unwrap();
        q.wait_for_change().await;
    }

    #[tokio::test]
    async fn idle_timeout_fires_and_cancel_wins() {
        let cancel = CancellationToken::new();
        let r = with_idle_timeout(
            Some(std::time::Duration::from_millis(20)),
            &cancel,
            std::future::pending::<()>(),
        )
        .await;
        assert_eq!(r, Err(IdleError::TimedOut));
        let r = with_idle_timeout(None, &cancel, async { 7 }).await;
        assert_eq!(r, Ok(7));
        cancel.cancel();
        let r = with_idle_timeout(None, &cancel, std::future::pending::<()>()).await;
        assert_eq!(r, Err(IdleError::Cancelled));
    }

    #[test]
    fn adopt_suggested_filename_proceeds_when_finalized_target_exists() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let current_part = dir_path.join("download.part");
        std::fs::write(&current_part, b"").unwrap();
        std::fs::write(dir_path.join("Report.pdf"), b"existing").unwrap();

        let (name, new_part) =
            adopt_suggested_filename("Report.pdf", "download", &current_part, dir_path).unwrap();
        assert_eq!(name, "Report.pdf");
        assert_eq!(new_part, dir_path.join("Report.pdf.part"));
    }

    #[test]
    fn adopt_suggested_filename_skips_when_other_part_in_progress() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let current_part = dir_path.join("download.part");
        std::fs::write(&current_part, b"").unwrap();
        std::fs::write(dir_path.join("Report.pdf.part"), b"halfway").unwrap();

        let result = adopt_suggested_filename("Report.pdf", "download", &current_part, dir_path);
        assert!(
            result.is_none(),
            "must not adopt when another .part is mid-flight"
        );
    }

    #[test]
    fn finalize_download_dedups_when_target_exists() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        std::fs::write(dir_path.join("Report.pdf"), b"original").unwrap();
        let part = dir_path.join("Report.pdf.part");
        std::fs::write(&part, b"second download").unwrap();

        let final_path = finalize_download(&part, "Report.pdf", dir_path, true).unwrap();

        assert_eq!(final_path, dir_path.join("Report.1.pdf"));
        assert_eq!(
            std::fs::read(dir_path.join("Report.pdf")).unwrap(),
            b"original"
        );
        assert_eq!(std::fs::read(&final_path).unwrap(), b"second download");
    }

    #[test]
    fn finalize_download_overwrites_when_renaming_disabled() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        std::fs::write(dir_path.join("Report.pdf"), b"original").unwrap();
        let part = dir_path.join("Report.pdf.part");
        std::fs::write(&part, b"second download").unwrap();

        let final_path = finalize_download(&part, "Report.pdf", dir_path, false).unwrap();

        assert_eq!(final_path, dir_path.join("Report.pdf"));
        assert_eq!(std::fs::read(&final_path).unwrap(), b"second download");
    }

    #[test]
    fn adopt_suggested_filename_proceeds_when_target_clear() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let current_part = dir_path.join("download.part");
        std::fs::write(&current_part, b"").unwrap();

        let (name, new_part) =
            adopt_suggested_filename("Report.pdf", "download", &current_part, dir_path).unwrap();
        assert_eq!(name, "Report.pdf");
        assert_eq!(new_part, dir_path.join("Report.pdf.part"));
    }

    #[test]
    fn apply_remote_file_time_sets_last_modified() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("download.bin");
        std::fs::write(&path, b"content").unwrap();

        let last_modified = "Sun, 06 Nov 1994 08:49:37 GMT";
        let expected = parse_http_date(last_modified).unwrap();
        apply_remote_file_time(&path, last_modified);

        let actual = std::fs::metadata(&path).unwrap().modified().unwrap();
        let difference = actual
            .duration_since(expected)
            .unwrap_or_else(|error| error.duration());
        assert!(
            difference <= std::time::Duration::from_secs(2),
            "expected mtime {expected:?}, got {actual:?}"
        );
    }

    #[test]
    fn apply_remote_file_time_ignores_set_time_errors() {
        let dir = tempfile::tempdir().unwrap();
        let missing_path = dir.path().join("missing.bin");

        apply_remote_file_time(&missing_path, "Sun, 06 Nov 1994 08:49:37 GMT");

        assert!(!missing_path.exists());
    }
}
