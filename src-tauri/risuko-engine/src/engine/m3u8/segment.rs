use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use futures_util::StreamExt;
use risuko_bt::limiter::Throttle;
use risuko_http::Client;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use super::decrypt::{decrypt_segment, fetch_decryption_key, iv_from_sequence};
use super::download::RequestHeaders;
use super::parser::Segment;
use crate::engine::speed_limiter::SpeedLimiter;

const SEGMENT_MAX_RETRIES: u32 = 5;
const MAX_ENCRYPTED_SEGMENT_BYTES: usize = 512 << 20;
const SEGMENT_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(45);
const KEY_FETCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const PROGRESS_FILENAME: &str = ".m3u8.progress";

pub struct ProgressState {
    pub completed_indices: HashSet<usize>,
    progress_path: PathBuf,
    append_file: Option<std::fs::File>,
}

impl ProgressState {
    pub fn load(temp_dir: &Path) -> Self {
        let progress_path = temp_dir.join(PROGRESS_FILENAME);
        let completed_indices = if progress_path.exists() {
            std::fs::read_to_string(&progress_path)
                .unwrap_or_default()
                .lines()
                .filter_map(|line| line.trim().parse::<usize>().ok())
                .collect()
        } else {
            HashSet::new()
        };
        let append_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&progress_path)
            .ok();
        Self {
            completed_indices,
            progress_path,
            append_file,
        }
    }

    pub fn mark_completed(&mut self, index: usize) {
        if self.completed_indices.insert(index) {
            if let Some(file) = self.append_file.as_mut() {
                use std::io::Write;
                let _ = writeln!(file, "{index}");
            }
        }
    }

    pub fn cleanup(&self) {
        let _ = std::fs::remove_file(&self.progress_path);
    }
}

struct AttemptError {
    message: String,
    retryable: bool,
}

impl AttemptError {
    fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            retryable: false,
        }
    }
}

impl From<String> for AttemptError {
    fn from(message: String) -> Self {
        Self {
            message,
            retryable: true,
        }
    }
}

type KeyCell = Arc<tokio::sync::OnceCell<[u8; 16]>>;

struct KeyCache {
    entries: std::collections::HashMap<String, KeyCell>,
}

impl KeyCache {
    fn new() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
        }
    }

    fn cell(&mut self, key_uri: &str) -> KeyCell {
        self.entries.entry(key_uri.to_string()).or_default().clone()
    }
}

async fn key_from_cell<F>(cell: &KeyCell, fetch: F) -> Result<[u8; 16], String>
where
    F: std::future::Future<Output = Result<[u8; 16], String>>,
{
    cell.get_or_try_init(|| fetch).await.copied()
}

pub type SharedProgress = Arc<StdMutex<ProgressState>>;

pub fn cleanup_progress(progress: &SharedProgress) {
    progress.lock().unwrap_or_else(|e| e.into_inner()).cleanup();
}

struct SegmentStats {
    done_segments: AtomicU64,
    done_bytes: AtomicU64,
}

impl SegmentStats {
    fn record(&self, bytes: u64, total_segments: u64) -> u64 {
        let done_bytes = self.done_bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        let done_segments = self.done_segments.fetch_add(1, Ordering::Relaxed) + 1;
        estimate_total(done_bytes, done_segments, total_segments)
    }
}

fn estimate_total(done_bytes: u64, done_segments: u64, total_segments: u64) -> u64 {
    if done_segments == 0 {
        return 0;
    }
    let estimate = u128::from(done_bytes) * u128::from(total_segments) / u128::from(done_segments);
    u64::try_from(estimate).unwrap_or(u64::MAX)
}

struct SegmentCtx {
    client: Client,
    headers: RequestHeaders,
    throttle: Throttle,
    key_cache: tokio::sync::Mutex<KeyCache>,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
    stats: SegmentStats,
    progress: SharedProgress,
    total_segments: u64,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn download_segments(
    segments: &[Segment],
    media_sequence: u64,
    temp_dir: &Path,
    client: &Client,
    headers: &RequestHeaders,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
    cancel_token: CancellationToken,
    global_limiter: Arc<SpeedLimiter>,
    task_limiter: Arc<SpeedLimiter>,
    max_concurrent: usize,
) -> Result<(Vec<PathBuf>, SharedProgress), String> {
    std::fs::create_dir_all(temp_dir).map_err(|e| format!("Failed to create temp dir: {e}"))?;

    let progress = ProgressState::load(temp_dir);

    let segment_paths: Vec<PathBuf> = (0..segments.len())
        .map(|i| temp_dir.join(format!("seg_{i:06}.ts")))
        .collect();

    let total_segments = segments.len() as u64;
    let stats = SegmentStats {
        done_segments: AtomicU64::new(0),
        done_bytes: AtomicU64::new(0),
    };
    let mut resumed_bytes: u64 = 0;
    let mut pending: Vec<usize> = Vec::new();
    for (index, seg_path) in segment_paths.iter().enumerate() {
        let done = progress.completed_indices.contains(&index);
        match done.then(|| std::fs::metadata(seg_path).ok()).flatten() {
            Some(meta) => {
                resumed_bytes += meta.len();
                stats.done_segments.fetch_add(1, Ordering::Relaxed);
                stats.done_bytes.fetch_add(meta.len(), Ordering::Relaxed);
            }
            None => pending.push(index),
        }
    }
    completed.store(resumed_bytes, Ordering::Relaxed);
    let resumed_estimate = estimate_total(
        stats.done_bytes.load(Ordering::Relaxed),
        stats.done_segments.load(Ordering::Relaxed),
        total_segments,
    );
    if resumed_estimate > 0 {
        total.store(resumed_estimate, Ordering::Relaxed);
    }

    let ctx = Arc::new(SegmentCtx {
        client: client.clone(),
        headers: headers.clone(),
        throttle: Throttle::new(global_limiter, task_limiter),
        key_cache: tokio::sync::Mutex::new(KeyCache::new()),
        total,
        completed,
        connections,
        stats,
        progress: Arc::new(StdMutex::new(progress)),
        total_segments,
    });

    let mut queue = pending.into_iter();
    let mut running: JoinSet<Result<(), String>> = JoinSet::new();

    loop {
        while running.len() < max_concurrent.max(1) {
            let Some(index) = queue.next() else { break };
            let ctx = ctx.clone();
            let segment = segments[index].clone();
            let seg_path = segment_paths[index].clone();
            let seq = media_sequence + index as u64;
            let cancel_token = cancel_token.clone();
            running.spawn(async move {
                ctx.connections.fetch_add(1, Ordering::Relaxed);
                let result = tokio::select! {
                    result = download_single_segment(&ctx, &segment, seq, &seg_path) => result,
                    _ = cancel_token.cancelled() => Err("cancelled".to_string()),
                };
                ctx.connections.fetch_sub(1, Ordering::Relaxed);
                let bytes = result?;
                let estimate = ctx.stats.record(bytes, ctx.total_segments);
                ctx.total.store(
                    estimate.max(ctx.completed.load(Ordering::Relaxed)),
                    Ordering::Relaxed,
                );
                ctx.progress
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .mark_completed(index);
                Ok(())
            });
        }
        if running.is_empty() {
            break;
        }

        let joined = tokio::select! {
            joined = running.join_next() => joined,
            _ = cancel_token.cancelled() => {
                abort_all(&mut running).await;
                return Err("cancelled".to_string());
            }
        };

        match joined {
            None => break,
            Some(Ok(Ok(()))) => {}
            Some(Ok(Err(e))) => {
                if e.contains("cancelled") {
                    abort_all(&mut running).await;
                    return Err("cancelled".to_string());
                }
                cancel_token.cancel();
                abort_all(&mut running).await;
                return Err(e);
            }
            Some(Err(e)) => {
                cancel_token.cancel();
                abort_all(&mut running).await;
                return Err(format!("Segment task panicked: {e}"));
            }
        }
    }

    Ok((segment_paths, ctx.progress.clone()))
}

async fn abort_all(running: &mut JoinSet<Result<(), String>>) {
    running.abort_all();
    while running.join_next().await.is_some() {}
}

async fn download_single_segment(
    ctx: &SegmentCtx,
    segment: &Segment,
    sequence_number: u64,
    output_path: &Path,
) -> Result<u64, String> {
    let mut retries = 0;

    loop {
        let mut added: u64 = 0;
        match attempt_segment_download(ctx, segment, sequence_number, output_path, &mut added).await
        {
            Ok(bytes) => {
                crate::engine::util::atomic_saturating_sub(&ctx.completed, added);
                ctx.completed.fetch_add(bytes, Ordering::Relaxed);
                return Ok(bytes);
            }
            Err(e) => {
                crate::engine::util::atomic_saturating_sub(&ctx.completed, added);
                if !e.retryable {
                    return Err(format!("Segment {} failed: {}", segment.url, e.message));
                }
                retries += 1;
                if retries >= SEGMENT_MAX_RETRIES {
                    return Err(format!(
                        "Segment {} failed after {SEGMENT_MAX_RETRIES} retries: {}",
                        segment.url, e.message
                    ));
                }
                let delay = std::time::Duration::from_millis(500 * 2u64.pow(retries - 1));
                tokio::time::sleep(delay).await;
            }
        }
    }
}

fn expected_body_len(remaining: Option<u64>, content_length: Option<u64>) -> u64 {
    match (remaining, content_length) {
        (Some(left), Some(len)) => left.min(len),
        (Some(left), None) => left,
        (None, len) => len.unwrap_or(0),
    }
}

fn encrypted_segment_too_large() -> AttemptError {
    AttemptError::fatal(format!(
        "Encrypted segment exceeds {} MiB",
        MAX_ENCRYPTED_SEGMENT_BYTES >> 20
    ))
}

async fn attempt_segment_download(
    ctx: &SegmentCtx,
    segment: &Segment,
    sequence_number: u64,
    output_path: &Path,
    added: &mut u64,
) -> Result<u64, AttemptError> {
    let client = &ctx.client;
    let mut request = client
        .get(&segment.url)
        .headers(ctx.headers.for_url(&segment.url));

    let mut ranged: Option<(u64, u64)> = None;
    if let Some(ref br) = segment.byte_range {
        if br.length > 0 {
            let end = br.offset.saturating_add(br.length).saturating_sub(1);
            request = request.header("Range", format!("bytes={}-{}", br.offset, end));
            ranged = Some((br.offset, br.length));
        }
    }

    let resp = tokio::time::timeout(SEGMENT_IDLE_TIMEOUT, request.send())
        .await
        .map_err(|_| "Segment request timed out".to_string())?
        .map_err(|e| format!("Segment request failed: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("Segment HTTP {}", resp.status()).into());
    }

    let mut remaining: Option<u64> = None;
    if let Some((offset, length)) = ranged {
        if resp.status() != risuko_http::StatusCode::PARTIAL_CONTENT {
            if offset != 0 {
                return Err(AttemptError::fatal(format!(
                    "Segment server ignored Range ({})",
                    resp.status()
                )));
            }
            remaining = Some(length);
        }
    }

    let encryption = segment
        .encryption
        .as_ref()
        .filter(|enc| enc.method == "AES-128");

    let expected = expected_body_len(remaining, resp.content_length());
    if encryption.is_some() && expected > MAX_ENCRYPTED_SEGMENT_BYTES as u64 {
        return Err(encrypted_segment_too_large());
    }
    let capacity = expected.min(64 << 20) as usize;
    let mut stream = resp.bytes_stream();
    let mut file = tokio::fs::File::create(output_path)
        .await
        .map_err(|e| format!("Failed to create segment file: {e}"))?;
    let mut ciphertext: Vec<u8> = if encryption.is_some() {
        Vec::with_capacity(capacity)
    } else {
        Vec::new()
    };
    let mut bytes_written: u64 = 0;

    loop {
        let Some(chunk) = tokio::time::timeout(SEGMENT_IDLE_TIMEOUT, stream.next())
            .await
            .map_err(|_| "Segment body stalled".to_string())?
        else {
            break;
        };
        let chunk = chunk.map_err(|e| format!("Failed to read segment body: {e}"))?;
        let mut chunk = &chunk[..];
        if let Some(left) = remaining.as_mut() {
            let keep = chunk
                .len()
                .min(usize::try_from(*left).unwrap_or(usize::MAX));
            chunk = &chunk[..keep];
            *left -= keep as u64;
        }
        let len = chunk.len();
        ctx.throttle.acquire(len).await;

        if encryption.is_some() {
            if ciphertext.len() + len > MAX_ENCRYPTED_SEGMENT_BYTES {
                return Err(encrypted_segment_too_large());
            }
            ciphertext.extend_from_slice(chunk);
        } else {
            file.write_all(chunk)
                .await
                .map_err(|e| format!("Failed to write segment: {e}"))?;
            bytes_written += len as u64;
        }
        ctx.completed.fetch_add(len as u64, Ordering::Relaxed);
        *added += len as u64;
        if remaining == Some(0) {
            break;
        }
    }

    if let Some(enc) = encryption {
        let cell = ctx.key_cache.lock().await.cell(&enc.key_uri);
        let key = key_from_cell(&cell, async {
            tokio::time::timeout(
                KEY_FETCH_TIMEOUT,
                fetch_decryption_key(&enc.key_uri, client, &ctx.headers),
            )
            .await
            .map_err(|_| "Decryption key fetch timed out".to_string())?
        })
        .await?;
        let iv = match &enc.iv {
            Some(iv_bytes) => {
                let mut iv = [0u8; 16];
                let len = iv_bytes.len().min(16);
                iv[16 - len..].copy_from_slice(&iv_bytes[..len]);
                iv
            }
            None => iv_from_sequence(sequence_number),
        };
        let plaintext = decrypt_segment(ciphertext, &key, &iv)?;
        bytes_written = plaintext.len() as u64;
        file.write_all(&plaintext)
            .await
            .map_err(|e| format!("Failed to write segment: {e}"))?;
    }

    file.flush()
        .await
        .map_err(|e| format!("Failed to flush segment: {e}"))?;

    Ok(bytes_written)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_estimate_scales_mean_segment_size() {
        assert_eq!(estimate_total(0, 0, 10), 0);
        assert_eq!(estimate_total(300, 3, 10), 1000);
        assert_eq!(estimate_total(u64::MAX, 1, 4), u64::MAX);
    }

    async fn serve_ok_head(content_length: u64) -> (u16, Arc<std::sync::atomic::AtomicUsize>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let requests = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = requests.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf).await;
                    let head =
                        format!("HTTP/1.1 200 OK\r\nContent-Length: {content_length}\r\n\r\n");
                    let _ = stream.write_all(head.as_bytes()).await;
                    tokio::time::sleep(std::time::Duration::from_secs(120)).await;
                });
            }
        });
        (port, requests)
    }

    async fn fail_one_segment(segment: Segment) -> String {
        let dir = tempfile::tempdir().unwrap();
        let headers = RequestHeaders::new(&serde_json::Map::new(), &segment.url);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            download_segments(
                &[segment],
                0,
                dir.path(),
                &Client::builder().build().unwrap(),
                &headers,
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU64::new(0)),
                Arc::new(AtomicU32::new(0)),
                CancellationToken::new(),
                Arc::new(SpeedLimiter::new(0)),
                Arc::new(SpeedLimiter::new(0)),
                1,
            ),
        )
        .await
        .expect("a segment that can never succeed must not be retried with backoff");
        result.err().expect("the segment must fail")
    }

    #[tokio::test]
    async fn an_oversized_encrypted_segment_fails_at_once_without_retries() {
        let (port, requests) = serve_ok_head(MAX_ENCRYPTED_SEGMENT_BYTES as u64 + 1).await;
        let message = fail_one_segment(Segment {
            url: format!("http://127.0.0.1:{port}/seg.ts"),
            byte_range: None,
            encryption: Some(super::super::parser::EncryptionInfo {
                method: "AES-128".into(),
                key_uri: format!("http://127.0.0.1:{port}/key"),
                iv: None,
            }),
        })
        .await;
        assert!(message.contains("exceeds 512 MiB"), "{message}");
        assert!(!message.contains("retries"), "{message}");
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_server_that_ignores_a_mid_file_range_fails_at_once() {
        let (port, requests) = serve_ok_head(4096).await;
        let message = fail_one_segment(Segment {
            url: format!("http://127.0.0.1:{port}/seg.ts"),
            byte_range: Some(super::super::parser::ByteRange {
                length: 1024,
                offset: 2048,
            }),
            encryption: None,
        })
        .await;
        assert!(message.contains("ignored Range"), "{message}");
        assert!(!message.contains("retries"), "{message}");
        assert_eq!(requests.load(Ordering::SeqCst), 1);
    }

    async fn serve_and_record(body: &'static [u8]) -> (u16, Arc<StdMutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let heads = Arc::new(StdMutex::new(Vec::new()));
        let seen = heads.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut head = Vec::new();
                let mut buf = [0u8; 4096];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                seen.lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&head).to_ascii_lowercase());
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.write_all(body).await;
            }
        });
        (port, heads)
    }

    #[tokio::test]
    async fn segment_and_key_requests_carry_the_credentials_of_their_own_origin() {
        let (segment_port, segment_heads) = serve_and_record(b"").await;
        let (key_port, key_heads) = serve_and_record(&[7u8; 16]).await;
        let mut options = serde_json::Map::new();
        options.insert(
            "header".into(),
            serde_json::json!(["Authorization: Bearer t"]),
        );
        options.insert("cookie".into(), "sid=abc".into());
        options.insert(
            crate::engine::http::COOKIE_HOST_OPTION.into(),
            "127.0.0.1".into(),
        );
        options.insert("no-netrc".into(), true.into());
        let headers =
            RequestHeaders::new(&options, &format!("http://127.0.0.1:{segment_port}/a.m3u8"));
        let segment = Segment {
            url: format!("http://127.0.0.1:{segment_port}/seg.ts"),
            byte_range: None,
            encryption: Some(super::super::parser::EncryptionInfo {
                method: "AES-128".into(),
                key_uri: format!("http://127.0.0.1:{key_port}/key"),
                iv: None,
            }),
        };
        let dir = tempfile::tempdir().unwrap();
        download_segments(
            &[segment],
            0,
            dir.path(),
            &Client::builder().build().unwrap(),
            &headers,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            CancellationToken::new(),
            Arc::new(SpeedLimiter::new(0)),
            Arc::new(SpeedLimiter::new(0)),
            1,
        )
        .await
        .unwrap();

        let segment_head = segment_heads.lock().unwrap()[0].clone();
        let key_head = key_heads.lock().unwrap()[0].clone();
        assert!(
            segment_head.contains("authorization: bearer t"),
            "{segment_head}"
        );
        assert!(segment_head.contains("cookie: sid=abc"), "{segment_head}");
        assert!(!key_head.contains("authorization:"), "{key_head}");
        assert!(key_head.contains("cookie: sid=abc"), "{key_head}");
    }

    #[test]
    fn a_server_that_ignores_range_is_judged_by_the_requested_length() {
        let whole_file = 600 << 20;
        assert_eq!(expected_body_len(Some(1000), Some(whole_file)), 1000);
        assert_eq!(expected_body_len(Some(1000), None), 1000);
        assert_eq!(expected_body_len(None, Some(whole_file)), whole_file);
        assert_eq!(expected_body_len(None, None), 0);
    }

    #[tokio::test]
    async fn a_key_is_fetched_once_for_concurrent_segments() {
        let cache = Arc::new(tokio::sync::Mutex::new(KeyCache::new()));
        let calls = Arc::new(AtomicU32::new(0));
        let ask = |cache: Arc<tokio::sync::Mutex<KeyCache>>, calls: Arc<AtomicU32>| async move {
            let cell = cache.lock().await.cell("http://keys.test/k");
            key_from_cell(&cell, async move {
                calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                Ok([3u8; 16])
            })
            .await
        };
        let (first, second) = tokio::join!(
            ask(cache.clone(), calls.clone()),
            ask(cache.clone(), calls.clone())
        );
        assert_eq!(first.unwrap(), [3u8; 16]);
        assert_eq!(second.unwrap(), [3u8; 16]);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_slow_key_does_not_block_another_key() {
        let cache = Arc::new(tokio::sync::Mutex::new(KeyCache::new()));
        let stuck_cache = cache.clone();
        let stuck = tokio::spawn(async move {
            let cell = stuck_cache.lock().await.cell("http://keys.test/slow");
            key_from_cell(&cell, std::future::pending()).await
        });
        tokio::task::yield_now().await;

        let other = async {
            let cell = cache.lock().await.cell("http://keys.test/fast");
            key_from_cell(&cell, async { Ok([9u8; 16]) }).await
        };
        let key = tokio::time::timeout(std::time::Duration::from_secs(2), other)
            .await
            .expect("a key that is already available must not wait for a slow one")
            .unwrap();
        assert_eq!(key, [9u8; 16]);
        stuck.abort();
    }

    #[test]
    fn sub_saturating_never_underflows() {
        let counter = AtomicU64::new(5);
        crate::engine::util::atomic_saturating_sub(&counter, 9);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
}
