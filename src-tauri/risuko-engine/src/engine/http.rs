use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::StreamExt;
use risuko_http::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT_ENCODING, ACCEPT_RANGES, CONTENT_ENCODING,
    CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_MATCH, LAST_MODIFIED, RANGE,
    TRANSFER_ENCODING,
};
use risuko_http::Client;
use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

use super::speed_limiter::{parse_speed_limit, SpeedEma, SpeedLimiter};

const PART_SUFFIX: &str = ".part";
const DEFAULT_MIN_SPLIT_SIZE: u64 = 1024 * 1024;
const CHUNK_MAX_RETRIES: u32 = 5;
pub const PIECE_SIZE: u64 = 1024 * 1024;
const META_VERSION: u32 = 2;
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

/// Per-piece resume entry. Sparse: only pieces with `c > 0` are persisted
#[derive(serde::Serialize, serde::Deserialize)]
struct PieceProgress {
    /// piece index
    i: u32,
    /// bytes completed in this piece (0 < c <= piece length)
    c: u32,
}

/// Resume metadata for a multi-piece download: a JSON sidecar next to the `.part` file; versioned so incompatible versions are discarded and the download restarts from scratch (the file stays pre-allocated but every Range is re-issued)
#[derive(serde::Serialize, serde::Deserialize)]
struct ChunkMeta {
    version: u32,
    content_length: u64,
    piece_size: u32,
    etag: Option<String>,
    /// Sparse list of pieces with progress; a piece is fully done when `c == piece_length` and pieces not listed start fresh at 0
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

/// Move an in-progress `.part` file and its `.chunks` resume sidecar to a new
/// `(dir, out)` location
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

/// Sanitize a user-supplied filename to a single path component
pub fn sanitize_filename(name: &str) -> String {
    let base = Path::new(name)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "download".to_string());
    if base.is_empty() || base == "." || base == ".." {
        "download".to_string()
    } else {
        base
    }
}

fn save_piece_meta(
    part_path: &Path,
    queue: &PieceQueue,
    content_length: u64,
    etag: &Option<String>,
) {
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
        piece_size: PIECE_SIZE as u32,
        etag: etag.clone(),
        pieces,
    };
    let path = chunk_meta_path(part_path);
    if let Ok(json) = serde_json::to_string(&meta) {
        let _ = fs::write(&path, json);
    }
}

fn load_piece_meta(
    part_path: &Path,
    content_length: u64,
    etag: &Option<String>,
) -> Option<ChunkMeta> {
    let path = chunk_meta_path(part_path);
    let data = fs::read_to_string(&path).ok()?;
    let meta: ChunkMeta = serde_json::from_str(&data).ok()?;
    if meta.version != META_VERSION
        || meta.content_length != content_length
        || meta.piece_size != PIECE_SIZE as u32
    {
        let _ = fs::remove_file(&path);
        return None;
    }

    // Verify the .part file exists with the expected pre-allocated size
    match fs::metadata(part_path) {
        Ok(m) if m.len() == content_length => {}
        _ => {
            let _ = fs::remove_file(&path);
            return None;
        }
    }

    // A byte-for-byte resume is safe only when both requests carry the same
    // strong validator. Weak or missing ETags cannot be used with If-Match and
    // do not guarantee that byte ranges are from the same representation
    if !matches!(
        (&meta.etag, etag),
        (Some(saved), Some(current)) if etags_strongly_equal(saved, current)
    ) {
        let _ = fs::remove_file(&path);
        return None;
    }

    Some(meta)
}

fn delete_chunk_meta(part_path: &Path) {
    let _ = fs::remove_file(chunk_meta_path(part_path));
}

// === Piece queue: shared work pool for the worker tasks === Each piece is at most PIECE_SIZE bytes; workers atomically claim a free piece (CAS 0 -> 1), download it, then mark done (-> 2) or release it (-> 0, completed bytes preserved so the next claimant resumes mid-piece). Work stealing comes for free — fast workers pull more, slow ones fewer, and nothing is owned long-term

/// Free / in-flight / done state encoded in a single byte for cheap CAS
const PIECE_FREE: u8 = 0;
const PIECE_INFLIGHT: u8 = 1;
const PIECE_DONE: u8 = 2;

struct Piece {
    /// Absolute byte offset in the output file
    offset: u64,
    /// Piece length in bytes (last piece may be < PIECE_SIZE)
    length: u32,
    /// Bytes confirmed flushed to disk so far, monotonically increasing because positioned writes are issued in order from the writer task; wrapped in Arc so the writer task can hold a clone independently
    completed: Arc<AtomicU32>,
    /// Lifecycle state — see PIECE_* constants
    state: AtomicU8,
}

struct PieceQueue {
    pieces: Vec<Piece>,
    /// Hint for the next free piece, to avoid rescanning from 0 every claim
    next_hint: AtomicUsize,
}

impl PieceQueue {
    fn new(content_length: u64) -> Self {
        debug_assert!(content_length > 0);
        let n = content_length.div_ceil(PIECE_SIZE) as usize;
        let mut pieces = Vec::with_capacity(n);
        for i in 0..n {
            let offset = i as u64 * PIECE_SIZE;
            let length = std::cmp::min(PIECE_SIZE, content_length - offset) as u32;
            pieces.push(Piece {
                offset,
                length,
                completed: Arc::new(AtomicU32::new(0)),
                state: AtomicU8::new(PIECE_FREE),
            });
        }
        Self {
            pieces,
            next_hint: AtomicUsize::new(0),
        }
    }

    /// Try to claim the next free piece, scanning circularly from the hint Returns the piece index, or None if the queue is exhausted
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
                // Move hint past this slot. Best-effort — racy stores are fine
                self.next_hint.store(i + 1, Ordering::Relaxed);
                return Some(i);
            }
        }
        None
    }

    /// Return a piece to the pool with its `completed` count preserved so the next claimant resumes mid-piece via a smaller Range request
    fn release(&self, idx: usize) {
        self.pieces[idx].state.store(PIECE_FREE, Ordering::Release);
        // Bias the hint backwards so this piece gets retried sooner
        let cur = self.next_hint.load(Ordering::Relaxed);
        if idx < cur {
            self.next_hint.store(idx, Ordering::Relaxed);
        }
    }

    fn complete(&self, idx: usize) {
        self.pieces[idx].state.store(PIECE_DONE, Ordering::Release);
    }

    /// True when every piece is in the DONE state; used to decide whether worker errors are fatal, since a stranded retry-exhausted worker is recoverable as long as another worker eventually finished the piece
    fn is_finished(&self) -> bool {
        self.pieces
            .iter()
            .all(|p| p.state.load(Ordering::Acquire) == PIECE_DONE)
    }
}

/// aria2 default: 60s connect timeout when not configured
const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 60;
/// Default maximum idle time between chunks of an NZB response body
const DEFAULT_NZB_BODY_TIMEOUT_SECS: u64 = 30;
/// aria2 default for `--lowest-speed-limit-timeout`: 30s of below-threshold transfer before the worker is considered stalled
const DEFAULT_LOWEST_SPEED_TIMEOUT_SECS: u64 = 30;

/// Configuration + signal for the stalled-transfer watchdog; `lowest_speed` of 0 disables it entirely (matches aria2's default of never enforcing a floor)
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

/// Read a duration option (seconds) with sane fallback
fn parse_duration_secs_option(v: Option<&Value>, default: u64) -> std::time::Duration {
    let secs = v
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
        })
        .unwrap_or(default);
    std::time::Duration::from_secs(secs)
}

/// Pull `checksum` from the options map and parse it; empty string / missing key disables verification (`Ok(None)`), while a non-empty but malformed value is rejected so users see the typo immediately rather than after a long download
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

/// Hash `path` from disk in 1 MiB blocks and verify against `expected`, running on the blocking pool so the async runtime stays responsive even on multi-GB files
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

/// Verify each piece against `expected`; the piece layout must match `PIECE_SIZE` (the engine's fixed multi-piece chunk size), and since aria2-style per-piece lengths aren't carried in the option we fail loudly if the hash count doesn't line up with `(content_length / PIECE_SIZE).ceil()`
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
            // Last piece may be short; read exactly the remaining bytes so we don't hash any pre-allocated zero padding
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

/// Run the optional piece + whole-file integrity checks on the finished output, deleting it on mismatch so a retry can't resume corrupted bytes
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
    let client = build_client(options, true, None)?;
    let fetch = async {
        let resp = client
            .get(uri)
            .send()
            .await
            .map_err(|e| format!("metalink probe request failed: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("metalink probe HTTP {}", resp.status()));
        }
        if let Some(len) = resp
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
        {
            if len > CAP {
                return Err("resource too large to be a metalink".to_string());
            }
        }
        let mut buf: Vec<u8> = Vec::new();
        let mut stream = resp.bytes_stream();
        while let Some(item) = stream.next().await {
            let chunk = item.map_err(|e| format!("metalink probe read failed: {e}"))?;
            if buf.len() as u64 + chunk.len() as u64 > CAP {
                return Err("resource too large to be a metalink".to_string());
            }
            buf.extend_from_slice(&chunk);
        }
        Ok(buf)
    };
    tokio::time::timeout(PROBE_TIMEOUT, fetch)
        .await
        .map_err(|_| "metalink probe timed out".to_string())?
}

/// Fetch an NZB URL using the same HTTP client and request headers as a regular HTTP task; the response is consumed incrementally so chunked responses cannot bypass the payload cap
pub async fn fetch_for_nzb(uri: &str, options: &Map<String, Value>) -> Result<Vec<u8>, String> {
    const CAP: u64 = 16 * 1024 * 1024;
    let client = build_client(options, true, load_cookie_jar(options))?;
    let mut headers = build_headers(options);
    apply_netrc_auth(&mut headers, uri, options);
    let header_timeout =
        parse_duration_secs_option(options.get("connect-timeout"), DEFAULT_CONNECT_TIMEOUT_SECS);
    let response = tokio::time::timeout(header_timeout, client.get(uri).headers(headers).send())
        .await
        .map_err(|_| "NZB URL response timed out".to_string())?
        .map_err(|e| format!("NZB URL fetch failed: {e}"))?
        .error_for_status()
        .map_err(|e| format!("NZB URL returned an error: {e}"))?;

    if response.content_length().is_some_and(|length| length > CAP) {
        return Err("NZB URL payload too large".to_string());
    }

    let mut bytes = Vec::with_capacity(response.content_length().unwrap_or(0).min(CAP) as usize);
    let mut stream = response.bytes_stream();
    let body_timeout = parse_duration_secs_option(
        options.get("nzb-body-timeout"),
        DEFAULT_NZB_BODY_TIMEOUT_SECS,
    );
    let mut total = 0u64;
    loop {
        let item = tokio::time::timeout(body_timeout, stream.next())
            .await
            .map_err(|_| "NZB response body timed out".to_string())?;
        let Some(item) = item else { break };
        let chunk = item.map_err(|e| format!("NZB URL body read failed: {e}"))?;
        total = total.saturating_add(chunk.len() as u64);
        if total > CAP {
            return Err("NZB URL payload too large".to_string());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Build a HTTP Client with common settings applied from options; `decompress` is off for range requests, which must receive raw bytes at exact file offsets, and `cookie_jar` is shared with the range-request client so both see the same cookie store (passing `None` disables the cookie provider for this client)
fn build_client(
    options: &Map<String, Value>,
    decompress: bool,
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
        // Per-request `timeout` option is intentionally NOT applied here: chunked / long-streaming downloads must not be cut off by a wall clock, and stalled-transfer detection is handled separately by the `lowest-speed-limit` watchdog in `run_speed_tracker`
        .tcp_nodelay(true)
        // Long-lived chunk connections benefit from generous keepalive, and a large idle pool keeps every chunk worker on its own TCP stream
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

/// Parse the `load-cookies` file (Netscape format) once and return a shared `Jar`; returns `None` when the option is unset/blank, the file is missing, or parsing fails — all non-fatal conditions logged here so the call sites stay simple
fn load_cookie_jar(options: &Map<String, Value>) -> Option<std::sync::Arc<risuko_http::Jar>> {
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

/// Build custom headers from options
fn build_headers(options: &Map<String, Value>) -> HeaderMap {
    let mut headers = HeaderMap::new();

    // `header` arrives as an aria2 array of strings or a newline-joined string; accept both — string-only previously dropped headers silently (#106)
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

/// Inject `Authorization: Basic` from a `.netrc` lookup when the URI doesn't already carry credentials and the user hasn't disabled netrc via `no-netrc`; honors `netrc-path` for an override location, silently no-ops if the file is missing or unreadable, and never overwrites an existing Authorization header (set via the `header` option)
fn apply_netrc_auth(headers: &mut HeaderMap, uri: &str, options: &Map<String, Value>) {
    if headers.contains_key(risuko_http::header::AUTHORIZATION) {
        return;
    }
    if options
        .get("no-netrc")
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
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

/// Multi-URI entry point that iterates `uris` per the configured selector strategy; a non-cancellation error from one URI bumps that host's fail count and triggers a try with the next viable URI, and the loop terminates on success, on cancellation, or when the selector runs out of candidates
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
        // Existing .part bytes are already in `completed`; exclude them from this mirror's speed EMA
        let start_bytes = completed.load(Ordering::Relaxed);
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
            cancel_token.clone(),
            global_limiter.clone(),
            task_limiter.clone(),
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
                // Cancellation isn't a mirror fault — propagate immediately
                if cancel_token.is_cancelled() {
                    return Err(e);
                }
                stats.record_failure(&super::uri_selector::host_of(uri));
                tracing::warn!("Mirror {} failed: {e}", super::uri_selector::host_of(uri));
                last_err = Some(e);
                // Reset counters before the next mirror so progress accounting doesn't double-count partial bytes; clear `total` too so a failed mirror's content-length can't leak into an unknown-length mirror
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
    global_limiter: Arc<SpeedLimiter>,
    task_limiter: Arc<SpeedLimiter>,
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
    // Sanitize: strip path separators and traversal components
    filename = sanitize_filename(&filename);

    // Detect a URL-derived filename. The Tauri layer pre-fills task.out from the URL path, falling back to "download" / "download-<hash>" for opaque URLs (e.g. /resources/foo/download?version=N). Treat any of those as URL-derived so the engine can replace them when the server suggests a real name via Content-Disposition
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

    // aria2-compatible `max-connection-per-server`
    let max_conn_per_server = options
        .get("max-connection-per-server")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .map(|v| v.max(1).min(u64::from(u32::MAX)) as usize)
        .unwrap_or(split);
    let mirror_strategy = super::uri_selector::strategy_from_options(options);

    // aria2-compatible `min-split-size` (bytes, with K/M suffixes accepted); defaults to 1 MiB so smaller files use a single connection
    let min_split_size = parse_size_option(options.get("min-split-size"))
        .unwrap_or(DEFAULT_MIN_SPLIT_SIZE)
        .max(1);

    // Build a single shared cookie jar so both the decompressing client and the range-request client see the same cookies (the load-cookies file is parsed exactly once here)
    let cookie_jar = load_cookie_jar(options);
    let client = build_client(options, true, cookie_jar.clone())?;
    let range_client = if split > 1 {
        build_client(options, false, cookie_jar)?
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
        // Android fallocate support varies by filesystem/device, so avoid blocking preallocation there
        if cfg!(target_os = "android") && mode == super::falloc::Mode::Falloc {
            super::falloc::Mode::None
        } else {
            mode
        }
    };

    // Optional integrity checks; bad input is rejected up-front so a typo doesn't silently disable verification, letting the user see the error immediately instead of a "downloaded but unverified" success
    let whole_checksum = parse_whole_checksum_option(options)?;
    let piece_checksums = parse_piece_checksums_option(options)?;

    let use_remote_time = options
        .get("remote-time")
        .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
        .unwrap_or(false);

    // aria2-compatible `auto-file-renaming`: on (default) a name collision finalizes as "stem.N.ext", off overwrites the existing file
    let auto_rename = options
        .get("auto-file-renaming")
        .and_then(|v| v.as_bool().or_else(|| v.as_str().map(|s| s == "true")))
        .unwrap_or(true);

    // Check for an existing partial download; a single metadata() covers both the "missing file" and "present file" cases (Err -> 0), avoiding a redundant exists() stat and its TOCTOU window
    let existing_size = fs::metadata(&part_path).map(|m| m.len()).unwrap_or(0);

    let mut last_modified_header: Option<String> = None;

    let is_http = uri.starts_with("http://") || uri.starts_with("https://");

    let has_cf_clearance = effective_cookies_have_name(&client, &headers, uri, "cf_clearance");
    let wants_range_probe = split > 1 || filename_was_url_derived;

    let probe_for_name: Option<ProbeResult> = if is_http && wants_range_probe && !has_cf_clearance {
        match probe_range_support(&range_client, uri, &headers).await {
            Ok(p) => Some(p),
            Err(e) => {
                tracing::warn!("Range probe failed, falling back to single: {e}");
                None
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

    if is_http && split > 1 {
        match probe_for_name.as_ref() {
            Some(probe)
                if probe.range_supported
                    && probe.content_length > min_split_size.saturating_mul(split as u64) =>
            {
                if use_remote_time {
                    last_modified_header = probe.last_modified.clone();
                }
                connections.store(split as u32, Ordering::Relaxed);
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
                    split,
                    &mirror_headers,
                    total,
                    completed,
                    speed,
                    cancel_token,
                    &filename,
                    dir_path,
                    global_limiter,
                    task_limiter,
                    probe.etag.clone(),
                    &chunk_completed,
                    stall.clone(),
                    falloc_mode,
                    max_worker_retries,
                    auto_rename,
                )
                .await;
                if let Ok(ref path) = result {
                    if let Some(ref lm) = last_modified_header {
                        apply_remote_file_time(path, lm);
                    }
                    verify_output(
                        path,
                        probe.content_length,
                        &piece_checksums,
                        &whole_checksum,
                    )
                    .await?;
                }
                return result;
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

    // Single-connection download (fallback, resume, FTP, or small files) If falling through from a failed multi-chunk probe and a pre-allocated .part exists, its size doesn't reflect actual download progress — remove it to avoid a 416. Only remove when chunk metadata confirms this is a multi-chunk artifact
    if chunk_meta_path(&part_path).exists() && existing_size > 0 {
        tracing::info!("Removing pre-allocated .part before single-connection fallback");
        let _ = fs::remove_file(&part_path);
        delete_chunk_meta(&part_path);
    }
    connections.store(1, Ordering::Relaxed);

    // When a probe confirmed Range support but the file was too small for the multi-chunk path, reuse the probe's request shape for the single connection: the range client (HTTP/1.1, identity encoding) plus an explicit `Range: bytes=0-`. Some signed-URL CDNs (e.g. Quark) accept the Range probe but reject a plain full GET with 412 Precondition Failed
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

    // Single-connection downloads auto-retry transient network failures (connection reset, body-read errors, timeouts), resuming in place from the partial `.part`. This mirrors the per-piece retry budget of the multi-chunk path so a flaky link doesn't drop the whole task to Error and force a manual resume. Hard HTTP errors (4xx/5xx), cancellation, and stall trips are NOT retried here — they're handled a level up
    let mut single_attempt: u32 = 0;
    let result = loop {
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
            global_limiter.clone(),
            task_limiter.clone(),
            stall.clone(),
            filename_was_url_derived,
            probe_confirmed_range,
            auto_rename,
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
                tokio::time::sleep(std::time::Duration::from_secs(single_attempt as u64)).await;
                continue;
            }
            other => break other,
        }
    };

    // If a stale .part was removed, retry once from scratch by re-entering the full pipeline (probe, multi-chunk, verify) with fresh counters
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
                global_limiter,
                task_limiter,
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
            // Integrity for the single-connection path. Use the finished file's own size as content_length so the last (short) piece is hashed correctly. Mirrors the multi-chunk path so enforcement is consistent across all download paths
            let content_length = if piece_checksums.is_some() {
                match fs::metadata(&path) {
                    Ok(meta) => meta.len(),
                    Err(e) => {
                        let _ = fs::remove_file(&path);
                        return Err(format!("stat for piece verify: {e}"));
                    }
                }
            } else {
                0
            };
            verify_output(&path, content_length, &piece_checksums, &whole_checksum).await?;
            Ok(path)
        }
        Err(e) => Err(e),
    }
}

/// Result from probing range support: content length + optional ETag
struct ProbeResult {
    content_length: u64,
    etag: Option<String>,
    last_modified: Option<String>,
    /// Filename advertised by `Content-Disposition: attachment; filename=...` when present. Overrides URL-path inference for opaque endpoints like `download?version=N`
    suggested_filename: Option<String>,
    content_type: Option<String>,
    /// True when the response confirms range support (206 with valid Content-Range, or 200 + Accept-Ranges + Content-Length). When false the caller must fall back to a single-connection stream; the other fields can still carry useful headers
    range_supported: bool,
}

/// Marker prefix used in error strings for Cloudflare-blocked downloads. `manager.rs::classify_error` reads this and maps it to `ErrorCode::CLOUDFLARE_CHALLENGE` (315)
pub const CLOUDFLARE_MARKER: &str = "[cloudflare-challenge]";

/// Returns true when the response looks like a Cloudflare bot-protection challenge: a 4xx/5xx status from CF's edge carrying either a `cf-ray` header, `server: cloudflare`, or `cf-mitigated: challenge` Header-only on purpose. Modern CF challenges always set these headers, and reading the body would force buffering before downstream code runs. Skipping the body keeps the streaming path simple
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

/// Build a cloudflare-marker error message the classifier maps to `CLOUDFLARE_CHALLENGE` and the renderer can scan for the host
fn cloudflare_error(uri: &str, status: u16) -> String {
    let host = url::Url::parse(uri)
        .ok()
        .and_then(|u| u.host_str().map(|s| s.to_string()))
        .unwrap_or_else(|| "unknown".to_string());
    format!("{CLOUDFLARE_MARKER} host={host} status={status}")
}

/// Drain a small response body so reqwest can reuse the connection Bounded for early-return paths such as Cloudflare challenges
async fn drain_response_body(resp: risuko_http::Response) {
    use futures_util::StreamExt;
    const MAX_DRAIN_BYTES: usize = 64 * 1024;
    let mut read: usize = 0;
    let mut stream = resp.bytes_stream();
    while let Some(item) = stream.next().await {
        match item {
            Ok(bytes) => {
                read = read.saturating_add(bytes.len());
                if read >= MAX_DRAIN_BYTES {
                    break;
                }
            }
            Err(_) => break,
        }
    }
}

/// Probe whether the server supports Range requests. Returns the response headers we care about regardless of range support; check `range_supported` on the result before slicing
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

    // The fallback we hand back whenever range support isn't confirmed. Carries the filename / ETag / last-modified info so the streaming path can still adopt them
    let no_range = ProbeResult {
        content_length: 0,
        etag: etag.clone(),
        last_modified: last_modified.clone(),
        suggested_filename: suggested_filename.clone(),
        content_type: content_type.clone(),
        range_supported: false,
    };

    // Compatibility short-circuits: a compressed body or chunked transfer-encoding (no length) blocks parallel range workers from slicing the file safely. Range offsets refer to *encoded* bytes when Content-Encoding is present, but workers see decoded bytes; the two address spaces don't line up. Chunked also leaves the total size unknown up front. Drop to the streaming path in either case but keep the filename info we already pulled
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
        // Parse Content-Range: bytes 0-0/TOTAL
        if let Some(cr) = resp
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
        {
            if let Some(slash) = cr.rfind('/') {
                if let Ok(total) = cr[slash + 1..].trim().parse::<u64>() {
                    return Ok(ProbeResult {
                        content_length: total,
                        etag,
                        last_modified,
                        suggested_filename,
                        content_type,
                        range_supported: true,
                    });
                }
            }
        }
        return Ok(no_range);
    }

    // Anything else (including a 200 to a concrete Range request) is not a byte-range response. Keep filename/type metadata, but stay out of multi-chunk mode
    Ok(no_range)
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
}

impl MirrorPool {
    fn new(
        uris: Vec<String>,
        strategy: super::uri_selector::Strategy,
        max_conn_per_server: usize,
    ) -> Self {
        let keys = uris.iter().map(|u| endpoint_key(u)).collect();
        Self {
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
        if self.uris.is_empty() {
            return false;
        }
        if self.strategy == super::uri_selector::Strategy::Inorder {
            return true;
        }
        let stats = self.stats.lock();
        self.keys.iter().any(|k| !stats.is_blacklisted(k))
    }

    fn acquire(&self) -> Option<(usize, String)> {
        let stats = self.stats.lock();
        let mut active = self.active.lock();
        let eligible: Vec<usize> = (0..self.uris.len())
            .filter(|&i| {
                let k = &self.keys[i];
                let at_cap = active.get(k).copied().unwrap_or(0) >= self.max_conn_per_server;
                let blacklisted = self.strategy != super::uri_selector::Strategy::Inorder
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
            // Round-robin: always hand the next connection to the least-loaded mirror. This is what makes the default behavior truly concurrent
            super::uri_selector::Strategy::Feedback => *eligible
                .iter()
                .min_by_key(|&&i| (active_of(i), i))
                .unwrap_or(&chosen),
        };
        let key = self.keys[chosen].clone();
        *active.entry(key.clone()).or_insert(0) += 1;
        Some((chosen, key))
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

/// Multi-chunk parallel download using a piece queue + worker pool. The file is divided into PIECE_SIZE-byte pieces. `split` workers share one queue: each pulls the next free piece, downloads it, then pulls the next. Fast workers naturally pull more pieces (work stealing for free). Resume preserves per-piece byte progress so a SIGKILL never loses more than the in-flight bytes of one piece per worker
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
    global_limiter: Arc<SpeedLimiter>,
    task_limiter: Arc<SpeedLimiter>,
    expected_etag: Option<String>,
    chunk_completed: &[Arc<AtomicU64>],
    stall: StallWatchdog,
    falloc_mode: super::falloc::Mode,
    max_retries: u32,
    auto_rename: bool,
) -> Result<PathBuf, String> {
    total.store(content_length, Ordering::Relaxed);

    let queue = Arc::new(PieceQueue::new(content_length));
    tracing::info!(
        "Multi-piece download: {} pieces ({} MiB each), {} workers, {} bytes total",
        queue.pieces.len(),
        PIECE_SIZE / (1024 * 1024),
        split,
        content_length
    );

    // Restore piece progress from sidecar BEFORE pre-allocating so the file-size check inside load_piece_meta is meaningful
    if let Some(meta) = load_piece_meta(part_path, content_length, &expected_etag) {
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

    // Pre-allocate and open the output file as a single shared std::fs::File. All writers issue positioned writes (pwrite/seek_write) concurrently — no global mutex, no shared file cursor. The allocation strategy is configurable via `file-allocation` (`falloc` | `trunc` | `none`). `falloc` reserves real disk blocks via platform-specific syscalls; on filesystems that don't support it we silently degrade to `trunc` (`set_len`)
    let file = {
        let f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(part_path)
            .map_err(|e| format!("Failed to create file: {e}"))?;
        super::falloc::allocate(&f, content_length, falloc_mode)
            .map_err(|e| format!("Failed to pre-allocate file: {e}"))?;
        Arc::new(f)
    };

    // Speed tracker
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

    // Periodic sidecar save: snapshot piece progress every META_SAVE_INTERVAL so a SIGKILL never loses more than that interval of in-flight bytes
    let save_part = part_path.to_path_buf();
    let save_queue = Arc::clone(&queue);
    let save_etag = expected_etag.clone();
    let save_cancel = cancel_token.clone();
    let save_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(META_SAVE_INTERVAL);
        tick.tick().await; // skip the immediate first tick
        loop {
            tokio::select! {
                _ = save_cancel.cancelled() => break,
                _ = tick.tick() => {
                    save_piece_meta(&save_part, &save_queue, content_length, &save_etag);
                }
            }
        }
    });

    // Build the shared mirror pool
    let multi_mirror = uris.len() > 1;
    let pool = Arc::new(MirrorPool::new(
        uris.to_vec(),
        strategy,
        max_conn_per_server,
    ));
    // Per-mirror headers, indexed parallel to pool.uris. Shared read-only
    let mirror_headers = Arc::new(mirror_headers.to_vec());
    let piece_etag = if multi_mirror {
        None
    } else {
        expected_etag.clone()
    };
    let expected_total = if multi_mirror {
        Some(content_length)
    } else {
        None
    };
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
        let cancel_token = cancel_token.clone();
        let gl = Arc::clone(&global_limiter);
        let tl = Arc::clone(&task_limiter);
        let etag = piece_etag.clone();
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
                gl,
                tl,
                etag,
                expected_total,
                wc,
                max_retries,
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
    save_task.abort();
    // Drain the save task: abort cancels at the next await, but a save iteration mid-flight can still complete its synchronous fs::write. Awaiting here guarantees no save lands on disk after we proceed to delete_chunk_meta below
    let _ = save_task.await;
    speed.store(0, Ordering::Relaxed);

    // Stall watchdog tripped — surface a distinct, retryable error rather than letting the generic "cancelled" classification swallow it. The sidecar still holds piece progress so a retry resumes in place
    if stall.flag.load(Ordering::Acquire) {
        save_piece_meta(part_path, &queue, content_length, &expected_etag);
        return Err(format!(
            "Download stalled: speed below {} B/s for {}s",
            stall.lowest_speed,
            stall.timeout.as_secs(),
        ));
    }

    if !errors.is_empty() {
        // Persist current piece progress so the next attempt resumes correctly
        save_piece_meta(part_path, &queue, content_length, &expected_etag);

        if errors.iter().all(|e| e.contains("cancelled")) {
            return Err("Download cancelled".to_string());
        }
        // Worker errors are only fatal if the queue itself did not finish. A worker that exhausted its retry budget on one piece may have returned Err while another worker later picked the piece up and completed it. In that case the download is actually done
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

    // Ensure all positioned writes are durable before deleting the resume sidecar or renaming. If sync_file fails (ENOSPC, EIO, …) the .part file may be incomplete on disk — abort without finalizing so the sidecar survives and the next attempt can resume
    if let Err(e) = sync_file(&file).await {
        return Err(format!("fsync before rename failed: {e}"));
    }
    delete_chunk_meta(part_path);
    tracing::debug!("run_multi_chunk finalizing: part_path={part_path:?}, filename={filename:?}");
    finalize_download(part_path, filename, dir_path, auto_rename)
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
    Source(String),
    Storage(String),
}

/// Worker loop: claim a piece, pick a mirror, download it, mark it done; repeat. Returns Ok(()) when the queue is exhausted (this worker is finished), or Err on cancellation or after exceeding retry budget on one piece
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
    global_limiter: Arc<SpeedLimiter>,
    task_limiter: Arc<SpeedLimiter>,
    expected_etag: Option<String>,
    expected_total: Option<u64>,
    worker_completed: Option<Arc<AtomicU64>>,
    max_retries: u32,
) -> Result<(), String> {
    // Track consecutive zero-progress failures across piece claims. Work
    // stealing may hand this worker a different piece after each failure, so
    // resetting merely because the index changed would make the retry bound
    // ineffective. Any successful progress resets the budget below
    let mut retry_count: u32 = 0;
    let mut source_error_streak: u32 = 0;
    loop {
        if cancel_token.is_cancelled() {
            return Err("Download cancelled".to_string());
        }

        let idx = match queue.claim_next() {
            Some(i) => i,
            None => return Ok(()), // queue exhausted: this worker is done
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
        let uri = pool.uris[mirror_idx].clone();
        let headers = match mirror_headers
            .get(mirror_idx)
            .or_else(|| mirror_headers.first())
        {
            Some(h) => h,
            None => {
                queue.release(idx);
                return Err(format!("Worker {worker_id}: no mirror headers available"));
            }
        };
        let started = std::time::Instant::now();

        let outcome = download_piece_stream(
            client,
            &uri,
            headers,
            range,
            &file,
            &completed,
            &cancel_token,
            &global_limiter,
            &task_limiter,
            expected_etag.as_deref(),
            worker_completed.as_ref(),
            &piece_completed,
            expected_total,
        )
        .await;
        pool.release(&mirror_key);

        // The writer task already incremented piece_completed per Bytes flushed
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
                    // Early EOF made progress; return the piece with progress intact and reset the retry budget because per-piece resume moves toward piece_length
                    pool.record_success(&mirror_key, downloaded, started.elapsed().as_secs_f64());
                    queue.release(idx);
                    retry_count = 0;
                } else {
                    // Early EOF with zero progress counts against retry budget to avoid worker spin
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
                    tokio::time::sleep(std::time::Duration::from_secs(retry_count as u64)).await;
                }
            }
            Err(PieceDownloadError::Cancelled) => {
                queue.release(idx);
                return Err("Download cancelled".to_string());
            }
            Err(PieceDownloadError::Storage(error)) => {
                queue.release(idx);
                return Err(format!(
                    "Worker {worker_id} local storage failure on piece {idx}: {error}"
                ));
            }
            Err(PieceDownloadError::Source(error)) => {
                queue.release(idx);
                source_error_streak = source_error_streak.saturating_add(1);
                if !charge_retry_if_no_progress(&mut retry_count, downloaded) {
                    // A stream error after bytes were flushed still advanced
                    // the piece. Treat that attempt as mirror progress so a
                    // flaky but productive source is not blacklisted
                    pool.record_success(&mirror_key, downloaded, started.elapsed().as_secs_f64());
                    let backoff = partial_error_backoff(source_error_streak);
                    tracing::warn!(
                        "Worker {worker_id} piece {idx} on {mirror_key} failed after writing \
                         {downloaded} bytes: {error}; retry budget reset, retrying in {}ms",
                        backoff.as_millis()
                    );
                    tokio::time::sleep(backoff).await;
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
                tokio::time::sleep(std::time::Duration::from_secs(retry_count as u64)).await;
            }
        }
    }
}

/// Stream a single piece (or its remaining tail). Always returns the number of bytes flushed to disk, even on error, so the caller can accurately track resume progress via the per-piece atomic counter
#[allow(clippy::too_many_arguments)]
async fn download_piece_stream(
    client: &Client,
    uri: &str,
    headers: &HeaderMap,
    range: ChunkRange,
    file: &Arc<std::fs::File>,
    completed: &Arc<AtomicU64>,
    cancel_token: &CancellationToken,
    global_limiter: &SpeedLimiter,
    task_limiter: &SpeedLimiter,
    expected_etag: Option<&str>,
    worker_completed: Option<&Arc<AtomicU64>>,
    piece_completed: &Arc<AtomicU32>,
    expected_total: Option<u64>,
) -> Result<(), PieceDownloadError> {
    let mut req = client
        .get(uri)
        .headers(headers.clone())
        .header(RANGE, range.to_range_header_value());

    let strong_expected_etag = expected_etag.filter(|etag| {
        // Comparing a tag to itself is a compact validity check for the strong
        // ETag syntax accepted by `etags_strongly_equal`
        etags_strongly_equal(etag, etag)
    });

    if let Some(etag) = strong_expected_etag {
        if let Ok(v) = HeaderValue::from_str(etag) {
            req = req.header(IF_MATCH, v);
        }
    }

    let resp = req
        .send()
        .await
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
            // Do not wait for a potentially stalled response body before the
            // worker can fail over or observe cancellation
            drop(resp);
            return Err(PieceDownloadError::Source(
                "Server file changed (ETag mismatch), aborting download".to_string(),
            ));
        }
    }

    if status >= 400 {
        // Redact sensitive headers before logging
        let safe_headers: String = resp
            .headers()
            .iter()
            .filter_map(|(name, value)| {
                let name_str = name.as_str();
                // Skip sensitive headers
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

        // Read only bounded bytes from response stream to avoid memory issues
        let body_len = resp.content_length().unwrap_or(0);
        const MAX_SNIPPET_BYTES: usize = 256;
        let snippet = if body_len > 0 {
            use futures_util::StreamExt;
            let mut stream = resp.bytes_stream();
            let mut buf = Vec::new();
            while let Some(item) = stream.next().await {
                match item {
                    Ok(bytes) => {
                        let remaining = MAX_SNIPPET_BYTES.saturating_sub(buf.len());
                        if remaining == 0 {
                            break;
                        }
                        if bytes.len() <= remaining {
                            buf.extend_from_slice(&bytes);
                        } else {
                            buf.extend_from_slice(&bytes[..remaining]);
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
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
    if let Some(cr) = resp
        .headers()
        .get(CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
    {
        if let Some(space) = cr.find(' ') {
            if let Some(dash) = cr[space + 1..].find('-') {
                if let Ok(range_start) = cr[space + 1..space + 1 + dash].parse::<u64>() {
                    if range_start != range.start {
                        return Err(PieceDownloadError::Source(format!(
                            "Expected 206 Partial Content with matching Content-Range: \
                             requested start {} but got {range_start}",
                            range.start
                        )));
                    }
                }
            }
        }
        if let Some(want_total) = expected_total {
            if let Some(slash) = cr.rfind('/') {
                let tail = cr[slash + 1..].trim();
                if tail != "*" {
                    if let Ok(got_total) = tail.parse::<u64>() {
                        if got_total != want_total {
                            return Err(PieceDownloadError::Source(format!(
                                "mirror size mismatch: expected total {want_total} but got \
                                 {got_total} (serving a different file)"
                            )));
                        }
                    }
                }
            }
        }
    } else {
        return Err(PieceDownloadError::Source(
            "Expected 206 Partial Content with matching Content-Range, \
             but Content-Range header is missing"
                .to_string(),
        ));
    }

    let mut stream = resp.bytes_stream();
    // Cap writes at the requested range length. A misbehaving server that returns more bytes than asked must NOT pwrite past the piece into the next piece's region in the pre-allocated file
    let max_bytes = range.end - range.start + 1;
    let writer = ChunkWriter::spawn(
        Arc::clone(file),
        range.start,
        Some(max_bytes),
        Arc::clone(completed),
        worker_completed.cloned(),
        Some(Arc::clone(piece_completed)),
    );

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                let _ = writer.finish().await;
                return Err(PieceDownloadError::Cancelled);
            }
            chunk = stream.next() => {
                match chunk {
                    Some(Ok(bytes)) => {
                        let len = bytes.len();
                        global_limiter.acquire(len).await;
                        task_limiter.acquire(len).await;

                        if !writer.send(bytes).await {
                            return match finish_piece_writer(writer).await {
                                Ok(_) => Err(PieceDownloadError::Storage(
                                    "Writer task closed unexpectedly".to_string(),
                                )),
                                Err(error) => Err(error),
                            };
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
    }
}

/// Parse a JSON value as a byte-size: integer bytes, or a string with optional `K`/`M`/`G` suffix (case-insensitive). Returns `None` for missing/invalid
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

/// Cross-platform positioned write: writes the entire buffer at the given offset without touching the shared file cursor. Safe to call concurrently from multiple threads on the same `File` handle
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

/// fsync via blocking thread
async fn sync_file(file: &Arc<std::fs::File>) -> Result<(), String> {
    let file = Arc::clone(file);
    tokio::task::spawn_blocking(move || file.sync_all())
        .await
        .map_err(|e| format!("sync task join error: {e}"))?
        .map_err(|e| format!("Sync failed: {e}"))
}

/// Dedicated chunk writer: a single `spawn_blocking` thread per piece that pulls `Bytes` from an MPSC channel and `pwrite`s them sequentially to the pre-allocated file. No userspace memcpy (Bytes flows straight from HTTP client to pwrite), no per-flush spawn_blocking churn
struct ChunkWriter {
    tx: tokio::sync::mpsc::Sender<Bytes>,
    join: tokio::task::JoinHandle<Result<u64, String>>,
}

impl ChunkWriter {
    /// `max_bytes` caps the total bytes this writer will pwrite. Excess input is silently dropped so a misbehaving server returning more data than the requested Range can never overwrite adjacent pieces in the pre-allocated file. `None` means unlimited (single-connection path)
    fn spawn(
        file: Arc<std::fs::File>,
        start_offset: u64,
        max_bytes: Option<u64>,
        completed: Arc<AtomicU64>,
        worker_completed: Option<Arc<AtomicU64>>,
        piece_completed: Option<Arc<AtomicU32>>,
    ) -> Self {
        // Bounded channel = backpressure. Cap ~4 MiB worth of in-flight Bytes (16 messages * typical 16-256 KiB each) so we never balloon RAM if the disk is briefly slower than the network
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(16);
        let join = tokio::task::spawn_blocking(move || {
            let mut offset = start_offset;
            let mut written: u64 = 0;
            let mut remaining = max_bytes;
            while let Some(mut bytes) = rx.blocking_recv() {
                if let Some(rem) = remaining {
                    if rem == 0 {
                        // Drain extra data without writing — server overran
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

    /// Send a Bytes to the writer. Returns false if the writer has died
    async fn send(&self, bytes: Bytes) -> bool {
        self.tx.send(bytes).await.is_ok()
    }

    /// Close the input side and await the writer thread. Returns total bytes successfully written or the writer's first error
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

/// Classify a single-connection download error as transient (worth an in-place resume retry) vs terminal. Terminal cases: cancellation, the stale-`.part` signal (handled separately by the caller), a hard HTTP status (4xx/5xx — mirror failover handles those a level up), a Cloudflare challenge, integrity failures, and stall-watchdog trips. Everything else — connection resets, body-read errors, timeouts, transient DNS/connect hiccups — is treated as transient and retried with resume
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
    // 412 Precondition Failed on signed URLs (e.g. Quark) is often transient
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

/// Single-connection download Returns (final_path, last_modified_header_value)
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
    global_limiter: Arc<SpeedLimiter>,
    task_limiter: Arc<SpeedLimiter>,
    stall: StallWatchdog,
    filename_was_url_derived: bool,
    force_range: bool,
    auto_rename: bool,
) -> Result<(PathBuf, Option<String>), String> {
    let existing_size = if part_path.exists() {
        fs::metadata(part_path).map(|m| m.len()).unwrap_or(0)
    } else {
        0
    };

    completed.store(existing_size, Ordering::Relaxed);

    let mut req = client.get(uri).headers(headers.clone());
    if existing_size > 0 {
        req = req.header(RANGE, format!("bytes={existing_size}-"));
    } else if force_range {
        // Mirror the successful Range probe's request shape. Some signed-URL CDNs (e.g. Quark) reject a plain full GET with 412 Precondition Failed but serve the identical URL when a Range request is issued
        req = req
            .header(RANGE, "bytes=0-")
            .header(ACCEPT_ENCODING, "identity");
    }
    tracing::debug!(
        "Single download request: uri={uri}, existing={existing_size}, force_range={force_range}"
    );

    let resp = req.send().await.map_err(|e| {
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

    if status == 416 && existing_size > 0 {
        tracing::warn!("Got 416 with existing_size={existing_size}, deleting stale .part");
        if let Err(e) = fs::remove_file(part_path) {
            return Err(format!("Failed to delete stale .part: {e}"));
        }
        return Err(format!("Download will retry: {STALE_PART_REMOVED}"));
    }

    if status >= 400 {
        let header_dump = format!("{:?}", resp.headers());
        let body = resp.text().await.unwrap_or_default();
        let snippet: String = body.chars().take(512).collect();
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
        // Validate Content-Range matches requested offset
        let range_valid = resp
            .headers()
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .and_then(|cr| {
                // Parse "bytes START-END/TOTAL"
                let cr = cr.strip_prefix("bytes ")?;
                let dash = cr.find('-')?;
                let start_str = &cr[..dash];
                start_str.parse::<u64>().ok()
            })
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

    // Update total from Content-Length
    if let Some(cl) = resp.content_length() {
        if cl > 0 {
            total.store(write_offset + cl, Ordering::Relaxed);
        }
    }

    // Open file as a sync handle for positioned writes. We start writing at `write_offset` to resume in place \u2014 no append mode, no shared cursor
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

    // Speed tracking
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
    // Single-connection path also uses the dedicated writer thread: zero-copy Bytes -> pwrite, no userspace memcpy. No max_bytes cap here — the file isn't pre-allocated, so trailing extra bytes are appended rather than corrupting other regions
    let writer = ChunkWriter::spawn(
        Arc::clone(&file),
        write_offset,
        None,
        Arc::clone(&completed),
        None,
        None,
    );

    let result: Result<(), String> = loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                break Err("Download cancelled".to_string());
            }
            chunk = stream.next() => {
                match chunk {
                    Some(Ok(bytes)) => {
                        let len = bytes.len();
                        global_limiter.acquire(len).await;
                        task_limiter.acquire(len).await;
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
            }
        }
    };

    // Always drain the writer so all queued Bytes hit disk before we sync
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
    tracing::debug!(
        "run_single_download finalizing: part_path={part_path:?}, final_filename={final_filename:?}"
    );
    let final_path = finalize_download(part_path, &final_filename, dir_path, auto_rename)?;
    Ok((final_path, resp_last_modified))
}

/// Speed tracker that samples completed bytes every 250ms using EMA. Also implements aria2's `--lowest-speed-limit` watchdog: when the EMA stays below `stall.lowest_speed` (bytes/s) for at least `stall.timeout` seconds, `stall.flag` is raised and `cancel_token` is cancelled so workers exit Both the multi-piece and single-connection paths inspect `stall.flag` after their workers complete to surface the stall as a distinct error
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

    // Watchdog state. `below_since` records when the EMA first dropped below the threshold. Once `total` is known we arm immediately so the user- configured budget kicks in. For chunked / unknown-length responses (`total == 0`) we wait `unknown_len_grace` after start before arming so that pre-flight time (setup, probe, first byte) doesn't count against the budget but truly stalled streams still get killed
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

        if elapsed > 0.0 {
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

/// Rename the .part file to the final filename. With `auto_rename` (the aria2 `auto-file-renaming` default) a finished file already holding that name is never clobbered — the new file gets "stem.N.ext" instead; with it off the existing file is overwritten
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

/// Apply the remote server's Last-Modified time to the downloaded file. `last_modified_str` is the raw HTTP Last-Modified header value (RFC 2822 / RFC 7231)
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

/// Parse an HTTP date string (all three RFC 7231 formats) into a `SystemTime`
fn parse_http_date(s: &str) -> Option<std::time::SystemTime> {
    httpdate::parse_http_date(s.trim()).ok()
}

fn set_file_mtime(path: &Path, time: std::time::SystemTime) -> std::io::Result<()> {
    let times = fs::FileTimes::new().set_modified(time);

    // Preserve platform-specific handle access while delegating the timestamp update itself to Rust's standard library
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

/// Decide whether to switch from the URL-inferred filename to one the server suggests via `Content-Disposition`. Skips the swap when the suggested name is unsafe, identical to what we already have, when an existing .part on disk would have to be moved (resume safety), or when adopting it would trample another `.part` already in progress on disk under the same name. We deliberately do *not* block adoption when a finalized file with the target name already exists — `finalize_download` dedups to "stem.N.ext" at rename time, and rejecting adoption here would silently leave the file under the placeholder name even for legitimate re-downloads. Returns the new (filename, part_path) pair when adoption fires
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
    // Refuse to rename a download that already has bytes on disk
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
    // Don't trample another download that's already mid-flight under the suggested filename. A `.part` with bytes belongs to a different task; leaving it alone preserves their work
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

/// `content_type` is already normalized (lowercased, params stripped) by `content_type_from_headers` at every call site
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

/// Recognize the placeholder names emitted by the Tauri layer for opaque URLs: bare `download` (legacy) and `download-<hexhash>` (new, per-URL unique). These shouldn't be treated as user-chosen filenames when deciding whether to adopt a Content-Disposition suggestion
fn is_placeholder_download_name(name: &str) -> bool {
    if name == "download" {
        return true;
    }
    let Some(rest) = name.strip_prefix("download-") else {
        return false;
    };
    !rest.is_empty() && rest.chars().all(|c| c.is_ascii_hexdigit())
}

/// Parse `Content-Disposition` for a filename. Recognizes: - `attachment; filename="StoragePeek.jar"` (RFC 6266 quoted-string) - `attachment; filename=StoragePeek.jar` (unquoted token) - `attachment; filename*=UTF-8''Storage%20Peek.jar` (RFC 5987 ext-value) Returns `None` when no usable filename is present
pub fn filename_from_content_disposition(headers: &HeaderMap) -> Option<String> {
    let raw_bytes = headers.get("content-disposition")?.as_bytes();
    let raw = String::from_utf8_lossy(raw_bytes);

    // Prefer filename* (RFC 5987) since it can carry non-ASCII names
    let mut star_value: Option<String> = None;
    let mut plain_value: Option<String> = None;

    for part in raw.split(';') {
        let part = part.trim();
        if let Some(rest) = part
            .strip_prefix("filename*=")
            .or_else(|| part.strip_prefix("FILENAME*="))
        {
            // Format: charset'lang'percent-encoded — split on the two single-quotes to isolate the encoded name. A malformed value missing a quote yields None and is silently skipped (the plain `filename=` fallback still applies)
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
        // `中.txt` in UTF-8 = E4 B8 AD 2E 74 78 74. The previous byte-by-byte cast produced mojibake; verify the bytes-then-UTF8 path renders the original Unicode codepoint
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
        // sanitize_filename collapses path components — exact result depends on the helper, but must not contain a slash
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
        // Another worker can pick up the released piece (the essence of stealing)
        let stolen = q.claim_next().unwrap();
        assert_eq!(stolen, a);
        // b is still in flight
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
        // Mark piece 0 fully done, piece 1 half done, piece 2 untouched
        q.pieces[0]
            .completed
            .store(PIECE_SIZE as u32, Ordering::Relaxed);
        q.pieces[0].state.store(PIECE_DONE, Ordering::Release);
        q.pieces[1]
            .completed
            .store(PIECE_SIZE as u32 / 2, Ordering::Relaxed);

        let etag = Some("\"abc\"".to_string());
        save_piece_meta(&part, &q, PIECE_SIZE * 3, &etag);

        let loaded = load_piece_meta(&part, PIECE_SIZE * 3, &etag).expect("meta");
        assert_eq!(loaded.version, META_VERSION);
        assert_eq!(loaded.content_length, PIECE_SIZE * 3);
        // Sparse: only pieces with c > 0 are recorded
        assert_eq!(loaded.pieces.len(), 2);
        let p0 = loaded.pieces.iter().find(|p| p.i == 0).unwrap();
        let p1 = loaded.pieces.iter().find(|p| p.i == 1).unwrap();
        assert_eq!(p0.c, PIECE_SIZE as u32);
        assert_eq!(p1.c, PIECE_SIZE as u32 / 2);

        // ETag mismatch invalidates resume
        let other = Some("\"xyz\"".to_string());
        assert!(load_piece_meta(&part, PIECE_SIZE * 3, &other).is_none());

        // A weak validator must never restore byte-range progress, even when
        // its opaque value matches the previously saved strong validator
        save_piece_meta(&part, &q, PIECE_SIZE * 3, &etag);
        let weak = Some("W/\"abc\"".to_string());
        assert!(load_piece_meta(&part, PIECE_SIZE * 3, &weak).is_none());

        // Likewise, a sidecar created without a validator cannot later be
        // trusted merely because the current probe happens to return one
        save_piece_meta(&part, &q, PIECE_SIZE * 3, &None);
        assert!(load_piece_meta(&part, PIECE_SIZE * 3, &etag).is_none());

        // Cleanup
        delete_chunk_meta(&part);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn adopt_suggested_filename_proceeds_when_finalized_target_exists() {
        // A finalized file with the same name as the CD suggestion is NOT a blocker — finalize_download dedups at rename time, and refusing here would just trap legitimate re-downloads under the placeholder name (regression: re-fetch of `StoragePeek.jar` ended up named `download-<hash>` because a prior copy lived in the directory)
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
        // Another download is partway through under the suggested name
        std::fs::write(dir_path.join("Report.pdf.part"), b"halfway").unwrap();

        let result = adopt_suggested_filename("Report.pdf", "download", &current_part, dir_path);
        assert!(
            result.is_none(),
            "must not adopt when another .part is mid-flight"
        );
    }

    #[test]
    fn finalize_download_dedups_when_target_exists() {
        // Duplicate name must not clobber the finished file — aria2's auto-file-renaming default picks "stem.N.ext" instead
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
