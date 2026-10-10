use std::fs;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use rand::RngExt;
use serde_json::{Map, Value};
use suppaftp::tokio::{AsyncFtpStream, AsyncRustlsFtpStream};
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{FtpProtocol, FtpUri};
use crate::engine::options::json_bool;
use crate::engine::speed_limiter::{SpeedEma, SpeedLimiter};
use risuko_bt::limiter::Throttle;

const PART_SUFFIX: &str = ".part";
const BUF_SIZE: usize = 64 * 1024;
const PROXY_BRIDGE_ACCEPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const PROXY_BRIDGE_TOKEN_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const PROXY_BRIDGE_TOKEN_LEN: usize = 32;

macro_rules! ftp_transfer {
    ($ftp:expr, $parsed:expr, $part_path:expr, $file_size:expr,
     $total:expr, $completed:expr, $speed:expr,
     $connections:expr, $cancel_token:expr, $throttle:expr, $read_timeout:expr) => {{
        $ftp.transfer_type(suppaftp::types::FileType::Binary)
            .await
            .map_err(|e| format!("Failed to set binary mode: {e}"))?;

        let remote_path = &$parsed.path;
        let remote_size = $ftp.size(remote_path).await.unwrap_or(0) as u64;
        if remote_size > 0 {
            $total.store(remote_size, Ordering::Relaxed);
        }

        $connections.store(1, Ordering::Relaxed);

        let existing_size = if $part_path.exists() {
            fs::metadata(&$part_path).map(|m| m.len()).unwrap_or(0)
        } else {
            0
        };

        let effective_size = if remote_size > 0 {
            remote_size
        } else {
            $file_size
        };
        if existing_size > 0 && effective_size > 0 && existing_size == effective_size {
            $completed.store(existing_size, Ordering::Relaxed);
            tracing::info!("FTP .part already complete ({existing_size} bytes), skipping transfer");
            let _ = $ftp.quit().await;
        } else {
            let resume_offset =
                if existing_size > 0 && (effective_size == 0 || existing_size < effective_size) {
                    match usize::try_from(existing_size) {
                        Ok(off) => match $ftp.resume_transfer(off).await {
                            Ok(()) => {
                                $completed.store(existing_size, Ordering::Relaxed);
                                tracing::info!("Resuming FTP download from byte {existing_size}");
                                existing_size
                            }
                            Err(e) => {
                                tracing::warn!("FTP resume not supported: {e}");
                                0
                            }
                        },
                        Err(_) => {
                            tracing::warn!(
                                "FTP resume offset {existing_size} exceeds usize; \
                                 restarting download from scratch"
                            );
                            0
                        }
                    }
                } else {
                    0
                };

            let mut file = if resume_offset > 0 {
                tokio::fs::OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(&$part_path)
                    .await
                    .map_err(|e| format!("Failed to open part file: {e}"))?
            } else {
                tokio::fs::File::create(&$part_path)
                    .await
                    .map_err(|e| format!("Failed to create part file: {e}"))?
            };

            let mut data_stream = $ftp
                .retr_as_stream(remote_path)
                .await
                .map_err(|e| format!("FTP RETR failed: {e}"))?;

            let mut bytes_downloaded = resume_offset;
            let mut buf = vec![0u8; BUF_SIZE];
            let mut last_speed_time = Instant::now();
            let mut interval_bytes: u64 = 0;
            let mut ema = SpeedEma::new();

            loop {
                if $cancel_token.is_cancelled() {
                    return Err("Download cancelled".to_string());
                }

                let n = tokio::select! {
                    result = read_with_timeout(&mut data_stream, &mut buf, $read_timeout) => {
                        result.map_err(|e| format!("FTP read error: {e}"))?
                    }
                    _ = $cancel_token.cancelled() => {
                        return Err("Download cancelled".to_string());
                    }
                };

                if n == 0 {
                    break;
                }

                $throttle.acquire(n).await;

                file.write_all(&buf[..n])
                    .await
                    .map_err(|e| format!("Failed to write: {e}"))?;

                bytes_downloaded += n as u64;
                $completed.store(bytes_downloaded, Ordering::Relaxed);
                interval_bytes += n as u64;

                let elapsed = last_speed_time.elapsed();
                if elapsed.as_millis() >= 500 {
                    $speed.store(
                        ema.update(interval_bytes, elapsed.as_secs_f64()),
                        Ordering::Relaxed,
                    );
                    interval_bytes = 0;
                    last_speed_time = Instant::now();
                }
            }

            file.flush()
                .await
                .map_err(|e| format!("Failed to flush: {e}"))?;
            drop(file);

            // Idle control connection can fail the closing 226 although every byte arrived
            if let Err(e) = $ftp.finalize_retr_stream(data_stream).await {
                if remote_size > 0 && bytes_downloaded == remote_size {
                    tracing::warn!("FTP finalize failed after a complete transfer: {e}");
                } else {
                    return Err(format!("FTP finalize failed: {e}"));
                }
            }
            if remote_size > 0 && bytes_downloaded != remote_size {
                return Err(format!(
                    "FTP incomplete transfer: got {bytes_downloaded} of {remote_size} bytes"
                ));
            }

            let _ = $ftp.quit().await;
        }
        Ok::<(), String>(())
    }};
}

#[allow(clippy::too_many_arguments)]
pub async fn run_ftp_ftps_download(
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
        "Starting FTP{} download: host={}, path={}",
        if parsed.protocol == FtpProtocol::Ftps {
            "S"
        } else {
            ""
        },
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
        .or_else(|| option_str(options, "ftp-user"))
        .unwrap_or_else(|| "anonymous".to_string());
    let password = parsed
        .password
        .clone()
        .or_else(|| option_str(options, "ftp-passwd"))
        .unwrap_or_else(|| "risuko@".to_string());

    let http_proxy = http_proxy_from_options(options)?;
    let control_proxy = proxy_for_target(http_proxy, &parsed.host, parsed.port);
    let throttle = Throttle::new(global_limiter, task_limiter);
    let read_timeout = read_timeout_from_options(options);

    let job = FtpJob {
        parsed,
        options,
        part_path: &part_path,
        user: &user,
        password: &password,
        control_proxy,
        total: &total,
        completed: &completed,
        speed: &speed,
        connections: &connections,
        cancel_token: &cancel_token,
        throttle: &throttle,
        read_timeout,
    };
    retry_transient(options, &cancel_token, || ftp_attempt(&job)).await?;

    let bytes_done = completed.load(Ordering::Relaxed);
    if total.load(Ordering::Relaxed) == 0 {
        total.store(bytes_done, Ordering::Relaxed);
    }
    speed.store(0, Ordering::Relaxed);
    connections.store(0, Ordering::Relaxed);

    let auto_rename = options
        .get("auto-file-renaming")
        .and_then(json_bool)
        .unwrap_or(true);
    let final_path = finalize_download(&part_path, &filename, dir_path, auto_rename)?;
    tracing::info!("FTP download complete: {}", final_path.display());
    Ok(final_path)
}

struct FtpJob<'a> {
    parsed: &'a FtpUri,
    options: &'a Map<String, Value>,
    part_path: &'a Path,
    user: &'a str,
    password: &'a str,
    control_proxy: Option<risuko_http::ProxyConnector>,
    total: &'a Arc<AtomicU64>,
    completed: &'a Arc<AtomicU64>,
    speed: &'a Arc<AtomicU64>,
    connections: &'a Arc<AtomicU32>,
    cancel_token: &'a CancellationToken,
    throttle: &'a Throttle,
    read_timeout: Option<Duration>,
}

async fn ftp_attempt(job: &FtpJob<'_>) -> Result<(), String> {
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
    let (user, password) = (job.user, job.password);
    let addr = host_port(&parsed.host, parsed.port);
    let file_size = total.load(Ordering::Relaxed);
    let control_proxy = job.control_proxy.clone();
    let ipv6 = parsed.host.contains(':');

    if parsed.protocol == FtpProtocol::Ftps {
        let verify_cert = options
            .get("check-certificate")
            .and_then(json_bool)
            .unwrap_or(true);
        if !verify_cert {
            tracing::warn!(
                "FTPS certificate verification disabled (check-certificate=false) for {}",
                parsed.host
            );
        }
        let connector = super::tls::ftps_tls_connector(!verify_cert);

        if parsed.port == 990 && control_proxy.is_some() {
            return Err("FTPS implicit proxying is unsupported".to_string());
        }
        let proxy_control = if let Some(proxy) = &control_proxy {
            Some(
                proxy_bridge(proxy.clone(), parsed.host.clone(), parsed.port)
                    .await
                    .map_err(|e| format!("FTPS proxy bridge failed: {e}"))?,
            )
        } else {
            None
        };
        let mut ftp = if parsed.port == 990 {
            AsyncRustlsFtpStream::connect_secure_implicit(&addr, connector, &parsed.host)
                .await
                .map_err(|e| format!("FTPS implicit connect failed: {e}"))?
        } else {
            let control = match proxy_control {
                Some(bridge) => bridge
                    .connect()
                    .await
                    .map_err(|e| format!("FTPS proxy connect failed: {e}"))?,
                None => TcpStream::connect(&addr)
                    .await
                    .map_err(|e| format!("FTPS explicit connect failed: {e}"))?,
            };
            AsyncRustlsFtpStream::connect_with_stream(control)
                .await
                .map_err(|e| format!("FTPS explicit connect failed: {e}"))?
                .into_secure(connector, &parsed.host)
                .await
                .map_err(|e| format!("FTPS AUTH TLS failed: {e}"))?
        };

        if ipv6 {
            ftp.set_mode(suppaftp::Mode::ExtendedPassive);
        }
        if let Some(proxy) = control_proxy {
            ftp = ftp.passive_stream_builder(passive_proxy_builder(proxy, parsed.host.clone()));
        }

        ftp.login(user, password)
            .await
            .map_err(|e| format!("FTP login failed: {e}"))?;

        ftp_transfer!(
            ftp,
            parsed,
            part_path,
            file_size,
            total,
            completed,
            speed,
            connections,
            cancel_token,
            throttle,
            read_timeout
        )
    } else {
        let mut ftp = if let Some(proxy) = control_proxy.clone() {
            let control = proxy_bridge(proxy, parsed.host.clone(), parsed.port)
                .await
                .map_err(|e| format!("FTP proxy bridge failed: {e}"))?
                .connect()
                .await
                .map_err(|e| format!("FTP connect failed: {e}"))?;
            AsyncFtpStream::connect_with_stream(control)
                .await
                .map_err(|e| format!("FTP connect failed: {e}"))?
        } else {
            AsyncFtpStream::connect(&addr)
                .await
                .map_err(|e| format!("FTP connect failed: {e}"))?
        };

        if ipv6 {
            ftp.set_mode(suppaftp::Mode::ExtendedPassive);
        }
        if let Some(proxy) = control_proxy {
            ftp = ftp.passive_stream_builder(passive_proxy_builder(proxy, parsed.host.clone()));
        }

        ftp.login(user, password)
            .await
            .map_err(|e| format!("FTP login failed: {e}"))?;

        ftp_transfer!(
            ftp,
            parsed,
            part_path,
            file_size,
            total,
            completed,
            speed,
            connections,
            cancel_token,
            throttle,
            read_timeout
        )
    }
}

type PassiveFuture =
    Pin<Box<dyn Future<Output = suppaftp::FtpResult<TcpStream>> + Send + Sync + 'static>>;

fn passive_proxy_builder(
    proxy: risuko_http::ProxyConnector,
    host: String,
) -> impl Fn(std::net::SocketAddr) -> PassiveFuture + Send + Sync + 'static {
    move |remote| {
        let proxy = proxy.clone();
        let host = host.clone();
        sync_boxed(async move {
            proxy_bridge(proxy, host, remote.port())
                .await
                .map_err(suppaftp::FtpError::ConnectionError)?
                .connect()
                .await
                .map_err(suppaftp::FtpError::ConnectionError)
        })
    }
}

pub(super) fn host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

pub(super) fn output_filename(path: &str, out: &str) -> String {
    let raw = if out.is_empty() {
        basename_from_ftp_path(path)
    } else {
        out.to_string()
    };
    crate::engine::util::safe_filename(&raw, "download")
}

pub(super) fn read_timeout_from_options(options: &Map<String, Value>) -> Option<Duration> {
    let secs = option_u64(options, "timeout").unwrap_or(60);
    (secs > 0).then(|| Duration::from_secs(secs))
}

async fn read_with_timeout<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
    timeout: Option<Duration>,
) -> io::Result<usize> {
    match timeout {
        Some(limit) => tokio::time::timeout(limit, reader.read(buf))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "read timed out"))?,
        None => reader.read(buf).await,
    }
}

pub(super) fn option_u64(options: &Map<String, Value>, key: &str) -> Option<u64> {
    match options.get(key)? {
        Value::Number(n) => n.as_u64(),
        Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

pub(super) fn is_transient_error(error: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "FTP read error",
        "FTP connect failed",
        "FTP proxy",
        "FTP incomplete",
        "FTPS explicit connect failed",
        "FTPS implicit connect failed",
        "FTPS proxy",
        "FTPS AUTH TLS failed",
        "SSH connect failed",
        "SFTP proxy connect failed",
        "SFTP read error",
        "SFTP incomplete",
    ];
    PREFIXES.iter().any(|prefix| error.starts_with(prefix))
}

pub(super) async fn retry_transient<F, Fut>(
    options: &Map<String, Value>,
    cancel_token: &CancellationToken,
    mut attempt: F,
) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let max_tries = option_u64(options, "max-tries").unwrap_or(5);
    let wait = Duration::from_secs(option_u64(options, "retry-wait").unwrap_or(0).max(1));
    let mut tries: u64 = 0;
    loop {
        let error = match attempt().await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        tries += 1;
        if !is_transient_error(&error) || (max_tries != 0 && tries >= max_tries) {
            return Err(error);
        }
        tracing::warn!("Transfer attempt {tries} failed ({error}), retrying in {wait:?}");
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = cancel_token.cancelled() => return Err("Download cancelled".to_string()),
        }
    }
}

pub(super) fn finalize_download(
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
        reserve_unique_path(dir_path, &final_name)?
    } else {
        dir_path.join(&final_name)
    };
    if part_path != final_path {
        if let Err(e) = fs::rename(part_path, &final_path) {
            if auto_rename {
                let _ = fs::remove_file(&final_path);
            }
            return Err(format!("Failed to rename: {e}"));
        }
    }
    Ok(final_path)
}

fn reserve_unique_path(dir: &Path, name: &str) -> Result<PathBuf, String> {
    let (stem, ext) = match name.rfind('.') {
        Some(dot) if dot > 0 => (&name[..dot], &name[dot..]),
        _ => (name, ""),
    };
    for n in 0u32.. {
        let candidate = if n == 0 {
            dir.join(name)
        } else {
            dir.join(format!("{stem}.{n}{ext}"))
        };
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(_) => return Ok(candidate),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(format!("Failed to reserve output file: {e}")),
        }
    }
    Err("Failed to reserve output filename".to_string())
}

pub(super) fn option_str(options: &Map<String, Value>, key: &str) -> Option<String> {
    options
        .get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

pub(super) fn http_proxy_from_options(
    options: &Map<String, Value>,
) -> Result<Option<risuko_http::ProxyConnector>, String> {
    let server = option_str(options, "all-proxy").unwrap_or_default();
    if server.is_empty() {
        return Ok(None);
    }
    let proxy = risuko_http::Proxy::all(&server)
        .map_err(|error| format!("Invalid HTTP profile proxy: {error}"))?;
    let bypass = option_str(options, "no-proxy").unwrap_or_default();
    Ok(Some(
        risuko_http::ProxyConnector::from_proxy(proxy)
            .with_no_proxy(risuko_http::NoProxy::parse(bypass)),
    ))
}

fn proxy_for_target(
    proxy: Option<risuko_http::ProxyConnector>,
    host: &str,
    port: u16,
) -> Option<risuko_http::ProxyConnector> {
    proxy.filter(|connector| {
        !connector
            .no_proxy()
            .is_some_and(|bypass| bypass.matches_host_port(host, Some(port)))
    })
}

async fn proxy_bridge(
    proxy: risuko_http::ProxyConnector,
    host: String,
    port: u16,
) -> io::Result<ProxyBridge> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = listener.local_addr()?;
    let token: [u8; PROXY_BRIDGE_TOKEN_LEN] = rand::rng().random();
    let (ready_tx, ready_rx) = oneshot::channel();
    tokio::spawn(async move {
        let accepted = tokio::time::timeout(PROXY_BRIDGE_ACCEPT_TIMEOUT, async {
            loop {
                let (mut local, _) = listener.accept().await?;
                let mut presented = [0u8; PROXY_BRIDGE_TOKEN_LEN];
                let token_read = tokio::time::timeout(
                    PROXY_BRIDGE_TOKEN_READ_TIMEOUT,
                    local.read_exact(&mut presented),
                )
                .await;
                if matches!(token_read, Ok(Ok(_))) && presented == token {
                    return Ok::<_, io::Error>(local);
                }
            }
        })
        .await;
        match accepted {
            Ok(Ok(mut local)) => match proxy.connect_tcp(&host, port).await {
                Ok(mut tunneled) => {
                    let _ = ready_tx.send(Ok(()));
                    let _ = tokio::io::copy_bidirectional(&mut local, &mut tunneled).await;
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(io::Error::other(error.to_string())));
                }
            },
            Ok(Err(error)) => {
                let _ = ready_tx.send(Err(error));
            }
            Err(_) => {
                let _ = ready_tx.send(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "FTP proxy bridge accept timed out",
                )));
            }
        }
    });
    Ok(ProxyBridge {
        endpoint,
        token,
        ready: ready_rx,
    })
}

pub(super) struct ProxyBridge {
    endpoint: std::net::SocketAddr,
    token: [u8; PROXY_BRIDGE_TOKEN_LEN],
    ready: oneshot::Receiver<io::Result<()>>,
}

impl ProxyBridge {
    pub(super) async fn wait_ready(self) -> io::Result<()> {
        self.ready.await.map_err(|_| {
            io::Error::new(io::ErrorKind::ConnectionAborted, "FTP proxy bridge stopped")
        })??;
        Ok(())
    }

    async fn connect(self) -> io::Result<TcpStream> {
        let mut stream = TcpStream::connect(self.endpoint).await?;
        stream.write_all(&self.token).await?;
        self.wait_ready().await?;
        Ok(stream)
    }
}

struct SyncFuture<T> {
    state: Arc<StdMutex<Option<Pin<Box<dyn Future<Output = T> + Send>>>>>,
}

impl<T> Future for SyncFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut state = this.state.lock().unwrap_or_else(|error| error.into_inner());
        let Some(future) = state.as_mut() else {
            panic!("polled completed proxy future");
        };
        match future.as_mut().poll(cx) {
            Poll::Ready(value) => {
                state.take();
                Poll::Ready(value)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn sync_boxed<T, F>(future: F) -> Pin<Box<dyn Future<Output = T> + Send + Sync>>
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    Box::pin(SyncFuture {
        state: Arc::new(StdMutex::new(Some(Box::pin(future)))),
    })
}

pub(super) fn basename_from_ftp_path(path: &str) -> String {
    let trimmed = path.trim_end_matches('/');
    match trimmed.rfind('/') {
        Some(idx) if !trimmed[idx + 1..].is_empty() => trimmed[idx + 1..].to_string(),
        _ => "download".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn host_port_brackets_ipv6_literals() {
        assert_eq!(host_port("example.com", 21), "example.com:21");
        assert_eq!(host_port("2001:db8::1", 21), "[2001:db8::1]:21");
    }

    #[test]
    fn output_filename_is_a_single_safe_component() {
        let name = output_filename("/x/..\\..\\evil:name.exe", "");
        assert!(!name.contains(['\\', '/', ':']), "{name}");
        assert_eq!(output_filename("/a/b/file.bin", ""), "file.bin");
        assert_eq!(output_filename("/a/b/file.bin", "../x/y.bin"), "_x_y.bin");
        assert_eq!(output_filename("/a/CON", ""), "download");
    }

    #[test]
    fn finalize_never_overwrites_existing_output() {
        let dir = std::env::temp_dir().join(format!("risuko-ftp-fin-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("a.bin"), b"old").unwrap();
        fs::write(dir.join("a.bin.part"), b"new").unwrap();
        let out = finalize_download(&dir.join("a.bin.part"), "a.bin", &dir, true).unwrap();
        assert_eq!(out, dir.join("a.1.bin"));
        assert_eq!(fs::read(dir.join("a.bin")).unwrap(), b"old");
        assert_eq!(fs::read(&out).unwrap(), b"new");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn only_connection_errors_are_retried() {
        assert!(is_transient_error("FTP read error: reset"));
        assert!(!is_transient_error("FTP login failed: 530"));
        assert!(!is_transient_error("Download cancelled"));
    }

    #[tokio::test]
    async fn retry_transient_stops_on_fatal_and_exhausts_tries() {
        let mut options = Map::new();
        options.insert("max-tries".into(), Value::from(2));
        let token = CancellationToken::new();
        let calls = std::sync::atomic::AtomicU32::new(0);
        let result = retry_transient(&options, &token, || {
            calls.fetch_add(1, Ordering::Relaxed);
            async { Err("FTP login failed: 530".to_string()) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn bypassed_target_does_not_use_http_proxy() {
        let proxy = risuko_http::ProxyConnector::from_proxy(
            risuko_http::Proxy::all("http://127.0.0.1:8080").unwrap(),
        )
        .with_no_proxy(risuko_http::NoProxy::parse("bypass.example"));

        assert!(proxy_for_target(Some(proxy.clone()), "bypass.example", 990).is_none());
        assert!(proxy_for_target(Some(proxy), "proxy.example", 990).is_some());

        let port_qualified = risuko_http::ProxyConnector::from_proxy(
            risuko_http::Proxy::all("http://127.0.0.1:8080").unwrap(),
        )
        .with_no_proxy(risuko_http::NoProxy::parse("bypass.example:21"));
        assert!(proxy_for_target(Some(port_qualified.clone()), "bypass.example", 21).is_none());
        assert!(proxy_for_target(Some(port_qualified), "bypass.example", 50000).is_some());
    }

    #[tokio::test]
    async fn stalled_bridge_client_does_not_block_authenticated_client() {
        let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_addr = upstream.local_addr().unwrap();
        let bridge = proxy_bridge(
            risuko_http::ProxyConnector::direct(),
            upstream_addr.ip().to_string(),
            upstream_addr.port(),
        )
        .await
        .unwrap();

        let stalled = TcpStream::connect(bridge.endpoint).await.unwrap();
        let bridged = tokio::time::timeout(std::time::Duration::from_secs(3), bridge.connect())
            .await
            .expect("stalled client blocked the bridge")
            .unwrap();
        let _upstream_connection =
            tokio::time::timeout(std::time::Duration::from_secs(1), upstream.accept())
                .await
                .expect("authenticated bridge client did not reach upstream")
                .unwrap();

        drop(bridged);
        drop(stalled);
    }
}
