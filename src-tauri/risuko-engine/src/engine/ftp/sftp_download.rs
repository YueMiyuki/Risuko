use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures_util::stream::{FuturesUnordered, StreamExt};
use risuko_bt::limiter::Throttle;
use russh::client;
use russh_sftp::client::{error::Error as SftpError, RawSftpSession};
use russh_sftp::protocol::{FileAttributes, OpenFlags, StatusCode};
use serde_json::{Map, Value};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

use super::ftp_download::{
    host_port, http_proxy_from_options, option_str, output_filename, read_timeout_from_options,
    retry_transient,
};
use super::FtpUri;
use crate::engine::options::json_bool;
use crate::engine::speed_limiter::{SpeedEma, SpeedLimiter};
use crate::engine::ssh_known_hosts::TofuHandler;

const PART_SUFFIX: &str = ".part";
const BUF_SIZE: usize = 64 * 1024;
const SFTP_READ_AHEAD: usize = 64;

struct SftpReadChunk {
    offset: u64,
    requested_len: usize,
    data: Vec<u8>,
}

struct OrderedChunkBuffer {
    next_offset: u64,
    pending: BTreeMap<u64, Vec<u8>>,
}

impl OrderedChunkBuffer {
    fn new(next_offset: u64) -> Self {
        Self {
            next_offset,
            pending: BTreeMap::new(),
        }
    }

    fn push(&mut self, chunk: SftpReadChunk) {
        if !chunk.data.is_empty() {
            self.pending.insert(chunk.offset, chunk.data);
        }
    }

    fn pop_ready(&mut self) -> Option<Vec<u8>> {
        let data = self.pending.remove(&self.next_offset)?;
        self.next_offset += data.len() as u64;
        Some(data)
    }
}

fn next_sftp_read_len(file_size: u64, offset: u64, max_len: usize) -> Option<usize> {
    if file_size > 0 {
        if offset >= file_size {
            None
        } else {
            Some(max_len.min((file_size - offset) as usize))
        }
    } else {
        Some(max_len)
    }
}

fn short_read_gap(chunk: &SftpReadChunk, file_size: u64) -> Option<(u64, usize)> {
    let actual_len = chunk.data.len();
    if actual_len == 0 || actual_len >= chunk.requested_len {
        return None;
    }

    let gap_offset = chunk.offset + actual_len as u64;
    if file_size > 0 && gap_offset >= file_size {
        return None;
    }

    let mut gap_len = chunk.requested_len - actual_len;
    if file_size > 0 {
        gap_len = gap_len.min((file_size - gap_offset) as usize);
    }
    (gap_len > 0).then_some((gap_offset, gap_len))
}

async fn read_sftp_range(
    sftp: Arc<RawSftpSession>,
    handle: String,
    offset: u64,
    len: usize,
) -> Result<SftpReadChunk, String> {
    let data = match sftp.read(handle, offset, len as u32).await {
        Ok(data) => data.data,
        Err(SftpError::Status(status)) if status.status_code == StatusCode::Eof => Vec::new(),
        Err(e) => return Err(format!("SFTP read error: {e}")),
    };

    Ok(SftpReadChunk {
        offset,
        requested_len: len,
        data,
    })
}

async fn next_read<S, T>(
    reads: &mut FuturesUnordered<S>,
    timeout: Option<std::time::Duration>,
) -> Result<Option<T>, String>
where
    S: std::future::Future<Output = T>,
{
    match timeout {
        Some(limit) => tokio::time::timeout(limit, reads.next())
            .await
            .map_err(|_| "SFTP read error: timed out".to_string()),
        None => Ok(reads.next().await),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_sftp_download(
    parsed: &FtpUri,
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
) -> Result<PathBuf, String> {
    tracing::info!(
        "Starting SFTP download: host={}, path={}",
        parsed.host,
        parsed.path,
    );

    let dir_path = Path::new(dir);
    fs::create_dir_all(dir_path).map_err(|e| format!("Failed to create dir: {e}"))?;

    let filename = output_filename(&parsed.path, out);

    let part_name = if filename.ends_with(PART_SUFFIX) {
        filename.clone()
    } else {
        format!("{filename}{PART_SUFFIX}")
    };
    let part_path = dir_path.join(&part_name);
    let _part_claim = super::PartClaim::acquire(&part_path)?;

    let user = parsed
        .user
        .clone()
        .or_else(|| option_str(options, "sftp-user"))
        .or_else(|| option_str(options, "ftp-user"))
        .unwrap_or_else(|| "root".to_string());

    let password = parsed
        .password
        .clone()
        .or_else(|| option_str(options, "sftp-passwd"))
        .or_else(|| option_str(options, "ftp-passwd"));

    let private_key_source = option_str(options, "sftp-private-key");
    let key_passphrase = option_str(options, "sftp-private-key-passphrase");

    let throttle = Throttle::new(global_limiter, task_limiter);
    let job = SftpJob {
        parsed,
        options,
        part_path: &part_path,
        user: &user,
        password: password.as_deref(),
        private_key_source: private_key_source.as_deref(),
        key_passphrase: key_passphrase.as_deref(),
        total: &total,
        completed: &completed,
        speed: &speed,
        connections: &connections,
        cancel_token: &cancel_token,
        throttle: &throttle,
        read_timeout: read_timeout_from_options(options),
    };
    retry_transient(options, &cancel_token, || sftp_attempt(&job)).await?;

    let auto_rename = options
        .get("auto-file-renaming")
        .and_then(json_bool)
        .unwrap_or(true);
    let final_path =
        super::ftp_download::finalize_download(&part_path, &filename, dir_path, auto_rename)?;
    tracing::info!("SFTP download complete: {}", final_path.display());
    Ok(final_path)
}

struct SftpJob<'a> {
    parsed: &'a FtpUri,
    options: &'a Map<String, Value>,
    part_path: &'a Path,
    user: &'a str,
    password: Option<&'a str>,
    private_key_source: Option<&'a str>,
    key_passphrase: Option<&'a str>,
    total: &'a Arc<AtomicU64>,
    completed: &'a Arc<AtomicU64>,
    speed: &'a Arc<AtomicU64>,
    connections: &'a Arc<AtomicU32>,
    cancel_token: &'a CancellationToken,
    throttle: &'a Throttle,
    read_timeout: Option<std::time::Duration>,
}

async fn sftp_attempt(job: &SftpJob<'_>) -> Result<(), String> {
    let parsed = job.parsed;
    let options = job.options;
    let part_path = job.part_path;
    let total = job.total;
    let completed = job.completed;
    let speed = job.speed;
    let connections = job.connections;
    let cancel_token = job.cancel_token;
    let throttle = job.throttle;
    let read_timeout = job.read_timeout;
    let user = job.user;
    let password = job.password;
    let private_key_source = job.private_key_source;
    let key_passphrase = job.key_passphrase;

    let config = Arc::new(client::Config {
        window_size: 16 * 1024 * 1024,
        maximum_packet_size: 256 * 1024,
        nodelay: true,
        keepalive_interval: Some(std::time::Duration::from_secs(30)),
        ..Default::default()
    });

    let addr = host_port(&parsed.host, parsed.port);
    let http_proxy = http_proxy_from_options(options)?;
    let session_result = if let Some(proxy) = http_proxy {
        let stream = proxy
            .connect_tcp(&parsed.host, parsed.port)
            .await
            .map_err(|e| format!("SFTP proxy connect failed: {e}"))?;
        client::connect_stream(config, stream, TofuHandler::new(addr.clone())).await
    } else {
        client::connect(config, &addr, TofuHandler::new(addr.clone())).await
    }
    .map_err(|e| format!("SSH connect failed: {e}"));
    let mut session = session_result?;

    let key = match private_key_source.filter(|source| !source.is_empty()) {
        Some(source) => match load_private_key(source, key_passphrase).await {
            Ok(key) => Some(key),
            Err(e) => {
                tracing::warn!("Failed to load SSH key: {e}");
                None
            }
        },
        None => None,
    };
    let authenticated =
        crate::engine::ssh_auth::authenticate(&mut session, user, key, password).await?;

    if !authenticated {
        return Err("SSH authentication failed: no valid credentials".to_string());
    }

    let channel = session
        .channel_open_session()
        .await
        .map_err(|e| format!("SSH channel open failed: {e}"))?;

    channel
        .request_subsystem(true, "sftp")
        .await
        .map_err(|e| format!("SFTP subsystem request failed: {e}"))?;

    let mut sftp = RawSftpSession::new(channel.into_stream());
    let version = sftp
        .init()
        .await
        .map_err(|e| format!("SFTP session init failed: {e}"))?;
    let mut max_read_len = BUF_SIZE;
    if version
        .extensions
        .get(russh_sftp::extensions::LIMITS)
        .is_some_and(|v| v == "1")
    {
        match sftp.limits().await {
            Ok(limits) => {
                if limits.max_read_len > 0 {
                    max_read_len = max_read_len.min(limits.max_read_len as usize);
                }
                sftp.set_limits(limits.into());
            }
            Err(e) => tracing::warn!("SFTP limits extension failed: {e}"),
        }
    }
    let sftp = Arc::new(sftp);

    connections.store(1, Ordering::Relaxed);

    let remote_path = &parsed.path;
    let file_size = match sftp.stat(remote_path).await {
        Ok(attrs) => attrs.attrs.size.unwrap_or(0),
        Err(e) => {
            tracing::warn!("SFTP stat failed (continuing without size): {e}");
            0
        }
    };
    if file_size > 0 {
        total.store(file_size, Ordering::Relaxed);
    }

    let existing_size = if part_path.exists() {
        fs::metadata(part_path).map(|m| m.len()).unwrap_or(0)
    } else {
        0
    };

    let resume_offset = if existing_size > 0 && file_size > 0 && existing_size <= file_size {
        existing_size
    } else {
        0
    };

    if resume_offset > 0 {
        completed.store(resume_offset, Ordering::Relaxed);
        tracing::info!("Resuming SFTP download from byte {resume_offset}");
    }

    let handle = sftp
        .open(remote_path, OpenFlags::READ, FileAttributes::empty())
        .await
        .map_err(|e| format!("SFTP open failed: {e}"))?
        .handle;

    let mut local_file = if resume_offset > 0 {
        tokio::fs::OpenOptions::new()
            .write(true)
            .append(true)
            .open(&part_path)
            .await
            .map_err(|e| format!("Failed to open part file: {e}"))?
    } else {
        tokio::fs::File::create(&part_path)
            .await
            .map_err(|e| format!("Failed to create part file: {e}"))?
    };

    let mut bytes_downloaded = resume_offset;
    let mut last_speed_time = Instant::now();
    let mut interval_bytes: u64 = 0;
    let mut ema = SpeedEma::new();
    let mut next_read_offset = resume_offset;
    let mut saw_eof = false;
    let mut reads = FuturesUnordered::new();
    let mut ordered = OrderedChunkBuffer::new(resume_offset);

    while reads.len() < SFTP_READ_AHEAD {
        let Some(len) = next_sftp_read_len(file_size, next_read_offset, max_read_len) else {
            break;
        };
        reads.push(read_sftp_range(
            sftp.clone(),
            handle.clone(),
            next_read_offset,
            len,
        ));
        next_read_offset += len as u64;
    }

    while !reads.is_empty() {
        if cancel_token.is_cancelled() {
            let _ = sftp.close(handle.clone()).await;
            return Err("Download cancelled".to_string());
        }

        let chunk = match tokio::select! {
            result = next_read(&mut reads, read_timeout) => result?,
            _ = cancel_token.cancelled() => {
                let _ = sftp.close(handle.clone()).await;
                return Err("Download cancelled".to_string());
            }
        } {
            Some(chunk) => chunk,
            None => break,
        };
        let chunk = chunk?;

        if chunk.data.is_empty() {
            saw_eof = true;
        } else {
            if let Some((gap_offset, gap_len)) = short_read_gap(&chunk, file_size) {
                reads.push(read_sftp_range(
                    sftp.clone(),
                    handle.clone(),
                    gap_offset,
                    gap_len,
                ));
            }
            ordered.push(chunk);

            while let Some(data) = ordered.pop_ready() {
                let n = data.len();
                throttle.acquire(n).await;

                local_file
                    .write_all(&data)
                    .await
                    .map_err(|e| format!("Failed to write: {e}"))?;

                bytes_downloaded += n as u64;
                completed.store(bytes_downloaded, Ordering::Relaxed);
                interval_bytes += n as u64;

                let elapsed = last_speed_time.elapsed();
                if elapsed.as_millis() >= 500 {
                    speed.store(
                        ema.update(interval_bytes, elapsed.as_secs_f64()),
                        Ordering::Relaxed,
                    );
                    interval_bytes = 0;
                    last_speed_time = Instant::now();
                }
            }
        }

        while !saw_eof && reads.len() < SFTP_READ_AHEAD {
            let Some(len) = next_sftp_read_len(file_size, next_read_offset, max_read_len) else {
                break;
            };
            reads.push(read_sftp_range(
                sftp.clone(),
                handle.clone(),
                next_read_offset,
                len,
            ));
            next_read_offset += len as u64;
        }
    }

    sftp.close(handle)
        .await
        .map_err(|e| format!("SFTP close failed: {e}"))?;

    if file_size > 0 && bytes_downloaded != file_size {
        let _ = local_file.flush().await;
        return Err(format!(
            "SFTP incomplete transfer: got {bytes_downloaded} of {file_size} bytes"
        ));
    }

    local_file
        .flush()
        .await
        .map_err(|e| format!("Failed to flush: {e}"))?;
    drop(local_file);

    if file_size == 0 {
        total.store(bytes_downloaded, Ordering::Relaxed);
    }
    completed.store(bytes_downloaded, Ordering::Relaxed);
    speed.store(0, Ordering::Relaxed);
    connections.store(0, Ordering::Relaxed);
    Ok(())
}

async fn load_private_key(
    source: &str,
    passphrase: Option<&str>,
) -> Result<russh::keys::PrivateKey, String> {
    let pem_content = if source.contains("----BEGIN") {
        source.to_string()
    } else {
        let path = if source.starts_with('~') {
            if let Some(home) = dirs::home_dir() {
                home.join(&source[2..])
            } else {
                PathBuf::from(source)
            }
        } else {
            PathBuf::from(source)
        };

        tokio::fs::read_to_string(&path)
            .await
            .map_err(|e| format!("Failed to read SSH key file '{}': {e}", path.display()))?
    };

    russh::keys::decode_secret_key(&pem_content, passphrase)
        .map_err(|e| format!("Failed to decode SSH key: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(offset: u64, requested_len: usize, data: &[u8]) -> SftpReadChunk {
        SftpReadChunk {
            offset,
            requested_len,
            data: data.to_vec(),
        }
    }

    #[test]
    fn ordered_chunk_buffer_waits_for_missing_offsets() {
        let mut buffer = OrderedChunkBuffer::new(0);
        buffer.push(chunk(4, 4, b"efgh"));

        assert!(buffer.pop_ready().is_none());

        buffer.push(chunk(0, 4, b"abcd"));
        assert_eq!(buffer.pop_ready().as_deref(), Some(&b"abcd"[..]));
        assert_eq!(buffer.pop_ready().as_deref(), Some(&b"efgh"[..]));
        assert!(buffer.pop_ready().is_none());
    }

    #[test]
    fn short_read_gap_requests_only_the_missing_range() {
        let c = chunk(64, 16, b"abcd");

        assert_eq!(short_read_gap(&c, 100), Some((68, 12)));
        assert_eq!(short_read_gap(&c, 68), None);
        assert_eq!(short_read_gap(&chunk(64, 4, b"abcd"), 100), None);
    }

    #[test]
    fn next_sftp_read_len_caps_to_remaining_known_size() {
        assert_eq!(next_sftp_read_len(100, 0, BUF_SIZE), Some(100));
        assert_eq!(next_sftp_read_len(100, 99, BUF_SIZE), Some(1));
        assert_eq!(next_sftp_read_len(100, 100, BUF_SIZE), None);
        assert_eq!(next_sftp_read_len(0, u64::MAX, BUF_SIZE), Some(BUF_SIZE));
        assert_eq!(next_sftp_read_len(1 << 20, 0, 32 * 1024), Some(32 * 1024));
        assert_eq!(next_sftp_read_len(0, 0, 32 * 1024), Some(32 * 1024));
    }
}
