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
use super::parser::Segment;
use crate::engine::speed_limiter::SpeedLimiter;

const SEGMENT_MAX_RETRIES: u32 = 5;
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

struct KeyCache {
    entries: std::collections::HashMap<String, [u8; 16]>,
}

impl KeyCache {
    fn new() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
        }
    }

    async fn get_or_fetch(&mut self, key_uri: &str, client: &Client) -> Result<[u8; 16], String> {
        if let Some(key) = self.entries.get(key_uri) {
            return Ok(*key);
        }
        let key = tokio::time::timeout(KEY_FETCH_TIMEOUT, fetch_decryption_key(key_uri, client))
            .await
            .map_err(|_| "Decryption key fetch timed out".to_string())??;
        self.entries.insert(key_uri.to_string(), key);
        Ok(key)
    }
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
pub async fn download_segments(
    segments: &[Segment],
    media_sequence: u64,
    temp_dir: &Path,
    client: &Client,
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
                retries += 1;
                if retries >= SEGMENT_MAX_RETRIES {
                    return Err(format!(
                        "Segment {} failed after {SEGMENT_MAX_RETRIES} retries: {e}",
                        segment.url
                    ));
                }
                let delay = std::time::Duration::from_millis(500 * 2u64.pow(retries - 1));
                tokio::time::sleep(delay).await;
            }
        }
    }
}

async fn attempt_segment_download(
    ctx: &SegmentCtx,
    segment: &Segment,
    sequence_number: u64,
    output_path: &Path,
    added: &mut u64,
) -> Result<u64, String> {
    let client = &ctx.client;
    let mut request = client.get(&segment.url);

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
        return Err(format!("Segment HTTP {}", resp.status()));
    }

    let mut remaining: Option<u64> = None;
    if let Some((offset, length)) = ranged {
        if resp.status() != risuko_http::StatusCode::PARTIAL_CONTENT {
            if offset != 0 {
                return Err(format!("Segment server ignored Range ({})", resp.status()));
            }
            remaining = Some(length);
        }
    }

    let encryption = segment
        .encryption
        .as_ref()
        .filter(|enc| enc.method == "AES-128");

    let capacity = resp.content_length().unwrap_or(0).min(64 << 20) as usize;
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
        let key = ctx
            .key_cache
            .lock()
            .await
            .get_or_fetch(&enc.key_uri, client)
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

    #[test]
    fn sub_saturating_never_underflows() {
        let counter = AtomicU64::new(5);
        crate::engine::util::atomic_saturating_sub(&counter, 9);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }
}
