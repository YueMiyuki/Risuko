use serde_json::{Map, Value};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{watch, OnceCell};
use tokio_util::sync::CancellationToken;

use super::speed_limiter::parse_speed_limit;

const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

static FFMPEG_PATH: OnceCell<PathBuf> = OnceCell::const_new();
static YT_DLP_READY: OnceCell<()> = OnceCell::const_new();

async fn output_within(mut cmd: Command, limit: Duration) -> std::io::Result<Output> {
    cmd.kill_on_drop(true);
    tokio::time::timeout(limit, cmd.output())
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("timed out after {}s", limit.as_secs_f32()),
            )
        })?
}

pub async fn probe_output(program: impl AsRef<OsStr>, arg: &str) -> std::io::Result<Output> {
    let mut cmd = Command::new(program);
    cmd.arg(arg);
    output_within(cmd, PROBE_TIMEOUT).await
}

pub fn is_youtube_uri(uri: &str) -> bool {
    let trimmed = uri.trim();
    if trimmed.is_empty() {
        return false;
    }

    let parsed = match url::Url::parse(trimmed) {
        Ok(url) => url,
        Err(_) => return false,
    };

    let host = match parsed.host_str() {
        Some(h) => h.to_ascii_lowercase(),
        None => return false,
    };

    host == "youtube.com" || host.ends_with(".youtube.com") || host == "youtu.be"
}

const MEDIA_HOST_SUFFIXES: &[&str] = &[
    "youtube.com",
    "youtu.be",
    "youtube-nocookie.com",
    "twitch.tv",
    "vimeo.com",
    "dailymotion.com",
    "dai.ly",
    "bilibili.com",
    "b23.tv",
    "nicovideo.jp",
    "tiktok.com",
    "twitter.com",
    "x.com",
    "instagram.com",
    "facebook.com",
    "fb.watch",
    "reddit.com",
    "redd.it",
    "soundcloud.com",
    "bandcamp.com",
    "streamable.com",
    "rumble.com",
    "odysee.com",
    "bitchute.com",
    "ted.com",
    "vk.com",
];

pub fn is_media_uri(uri: &str) -> bool {
    let trimmed = uri.trim();
    if trimmed.is_empty() {
        return false;
    }

    let parsed = match url::Url::parse(trimmed) {
        Ok(url) => url,
        Err(_) => return false,
    };

    match parsed.scheme() {
        "http" | "https" => {}
        _ => return false,
    }

    let host = match parsed.host_str() {
        Some(h) => h.to_ascii_lowercase(),
        None => return false,
    };

    MEDIA_HOST_SUFFIXES
        .iter()
        .any(|suffix| host == *suffix || host.ends_with(&format!(".{suffix}")))
}

pub fn is_force_ytdlp(options: &Map<String, Value>) -> bool {
    options
        .get("force-ytdlp")
        .and_then(crate::engine::options::json_bool)
        .unwrap_or(false)
}

pub async fn check_yt_dlp_available() -> Result<(), String> {
    let output = probe_output("yt-dlp", "--version")
        .await
        .map_err(|e| format!("yt-dlp is not available in PATH: {e}"))?;

    if output.status.success() {
        Ok(())
    } else {
        Err("yt-dlp is not available in PATH".to_string())
    }
}

async fn ensure_yt_dlp() -> Result<(), String> {
    YT_DLP_READY
        .get_or_try_init(check_yt_dlp_available)
        .await
        .map(|_| ())
}

async fn kill_tree(child: &mut tokio::process::Child) {
    #[cfg(unix)]
    if let Some(pgid) = child.id().and_then(|pid| libc::pid_t::try_from(pid).ok()) {
        let _ = unsafe { libc::killpg(pgid, libc::SIGKILL) };
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
}

pub async fn find_ffmpeg() -> Option<PathBuf> {
    FFMPEG_PATH
        .get_or_try_init(|| async {
            let candidate = PathBuf::from("ffmpeg");
            match probe_output(&candidate, "-version").await {
                Ok(o) if o.status.success() => Ok(candidate),
                _ => Err(()),
            }
        })
        .await
        .ok()
        .cloned()
}

fn yt_dlp_retries(max_tries: u64) -> String {
    match max_tries {
        0 => "infinite".to_string(),
        n => (n - 1).to_string(),
    }
}

fn parse_na_u64(s: &str) -> Option<u64> {
    if s == "NA" || s.is_empty() {
        return None;
    }
    s.parse::<f64>().ok().map(|f| f.max(0.0) as u64)
}

fn min_nonzero_limit(lhs: u64, rhs: u64) -> u64 {
    match (lhs, rhs) {
        (0, 0) => 0,
        (0, value) | (value, 0) => value,
        (left, right) => left.min(right),
    }
}

fn option_non_empty_str<'a>(options: &'a Map<String, Value>, key: &str) -> Option<&'a str> {
    options
        .get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn apply_yt_dlp_network_options(
    cmd: &mut Command,
    options: &Map<String, Value>,
    target_url: &str,
) -> Result<(), String> {
    if let Some(proxy_url) = option_non_empty_str(options, "all-proxy")
        .or_else(|| option_non_empty_str(options, "proxy"))
    {
        let matcher = risuko_http::NoProxy::parse(
            option_non_empty_str(options, "no-proxy").unwrap_or_default(),
        );
        let bypassed = risuko_http::Url::parse(target_url)
            .ok()
            .is_some_and(|target| matcher.matches_url(&target));
        risuko_http::Proxy::all(proxy_url).map_err(|e| format!("Invalid configured proxy: {e}"))?;
        if !bypassed {
            cmd.arg("--proxy").arg(proxy_url);
        }
    }

    if let Some(user_agent) = option_non_empty_str(options, "user-agent") {
        cmd.arg("--user-agent").arg(user_agent);
    }

    if let Some(referer) = option_non_empty_str(options, "referer") {
        cmd.arg("--referer").arg(referer);
    }

    if let Some(cookies_file) = option_non_empty_str(options, "cookies") {
        cmd.arg("--cookies").arg(cookies_file);
    } else if let Some(cookie_header) = option_non_empty_str(options, "cookie") {
        cmd.arg("--add-header")
            .arg(format!("Cookie: {cookie_header}"));
    }

    Ok(())
}

const YT_DLP_DEFAULT_OUTPUT: &str = "%(title)s [%(id)s].%(ext)s";

fn yt_dlp_expands_to_absolute(path: &str) -> bool {
    Path::new(path).is_absolute()
        || path
            .strip_prefix('~')
            .is_some_and(|rest| rest.chars().next().is_none_or(std::path::is_separator))
}

fn yt_dlp_output_template(
    dir: &str,
    out: &str,
    cwd: impl FnOnce() -> std::io::Result<PathBuf>,
) -> Result<String, String> {
    let template = if out.trim().is_empty() {
        YT_DLP_DEFAULT_OUTPUT
    } else {
        out
    };
    if yt_dlp_expands_to_absolute(template) {
        return Ok(template.to_string());
    }
    let dir = dir.trim();
    let folder = if yt_dlp_expands_to_absolute(dir) {
        PathBuf::from(dir)
    } else {
        cwd()
            .map_err(|e| format!("failed to resolve the download folder: {e}"))?
            .join(dir)
    };
    let folder = folder.to_string_lossy().replace('%', "%%");
    Ok(Path::new(&folder)
        .join(template)
        .to_string_lossy()
        .into_owned())
}

fn yt_dlp_command() -> Result<(Command, tempfile::TempDir), String> {
    let home = tempfile::TempDir::new()
        .map_err(|e| format!("failed to create a working folder for yt-dlp: {e}"))?;
    let mut cmd = Command::new("yt-dlp");
    // yt-dlp reads yt-dlp.conf from its home folder, which is the working directory when -P is not given
    cmd.current_dir(home.path());
    Ok((cmd, home))
}

fn yt_dlp_ffmpeg_location(ffmpeg: Option<&Path>) -> Option<&Path> {
    ffmpeg.filter(|path| path.is_absolute())
}

fn media_download_command(
    url: &str,
    dir: &str,
    out: &str,
    options: &Map<String, Value>,
    global_rate_limit: u64,
    ffmpeg: Option<&Path>,
) -> Result<(Command, tempfile::TempDir), String> {
    let (mut cmd, home) = yt_dlp_command()?;
    cmd.arg("--newline")
        .arg("--no-color")
        .arg("--print")
        .arg("before_dl:__YTDEST__%(filename)s")
        .arg("--print")
        .arg("after_move:__YTPATH__%(filepath)s")
        .arg("--progress")
        .arg("--progress-template")
        .arg("download:__YTPROG__%(progress.downloaded_bytes)s %(progress.total_bytes,progress.total_bytes_estimate)s %(progress.speed)s");

    if is_youtube_uri(url) {
        cmd.arg("--extractor-args")
            .arg("youtube:player_client=web,default");
    }

    apply_yt_dlp_network_options(&mut cmd, options, url)?;

    if let Some(location) = yt_dlp_ffmpeg_location(ffmpeg) {
        cmd.arg("--ffmpeg-location").arg(location);
    }
    if ffmpeg.is_some() {
        let merge_fmt = option_non_empty_str(options, "media-merge-format")
            .or_else(|| option_non_empty_str(options, "youtube-merge-format"))
            .unwrap_or("mp4");
        cmd.arg("--merge-output-format").arg(merge_fmt);
    }

    let format_opt = options
        .get("media-format")
        .or_else(|| options.get("youtube-format"))
        .or_else(|| options.get("yt-format"))
        .and_then(|v| v.as_str())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());
    if let Some(fmt) = format_opt {
        cmd.arg("--format").arg(fmt);
    } else if ffmpeg.is_none() {
        cmd.arg("--format")
            .arg("best[ext=mp4]/best[ext=webm]/best/b");
    }

    let task_rate_limit = options
        .get("max-download-limit")
        .map(parse_speed_limit)
        .unwrap_or(0);
    let rate_limit = min_nonzero_limit(task_rate_limit, global_rate_limit);
    if rate_limit > 0 {
        cmd.arg("--limit-rate").arg(rate_limit.to_string());
    }

    cmd.arg("-o")
        .arg(yt_dlp_output_template(dir, out, std::env::current_dir)?);

    let fragments = options
        .get("split")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
        })
        .unwrap_or(8)
        .clamp(1, 16);
    cmd.arg("--concurrent-fragments").arg(fragments.to_string());

    cmd.arg("--no-playlist");

    if let Some(tries) = options.get("max-tries").and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.trim().parse().ok()))
    }) {
        let retries = yt_dlp_retries(tries);
        cmd.arg("--retries").arg(&retries);
        cmd.arg("--fragment-retries").arg(&retries);
    }

    Ok((cmd, home))
}

pub async fn run_media_download(
    url: &str,
    dir: &str,
    out: &str,
    options: &Map<String, Value>,
    global_rate_limit: u64,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
    cancel_token: CancellationToken,
    dest_tx: watch::Sender<String>,
) -> Result<PathBuf, String> {
    ensure_yt_dlp().await?;

    let ffmpeg = find_ffmpeg().await;
    let (mut cmd, _home) =
        media_download_command(url, dir, out, options, global_rate_limit, ffmpeg.as_deref())?;
    cmd.arg("--").arg(url);
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    cmd.kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);

    let mut child = cmd
        .spawn()
        .map_err(|e| format!("failed to start yt-dlp: {e}"))?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "failed to capture yt-dlp stdout".to_string())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "failed to capture yt-dlp stderr".to_string())?;

    let stderr_task = tokio::spawn(async move {
        let mut lines = BufReader::new(stderr).lines();
        let mut out = Vec::new();
        while let Ok(Some(line)) = lines.next_line().await {
            out.push(line);
            if out.len() > 40 {
                out.remove(0);
            }
        }
        out
    });

    let mut lines = BufReader::new(stdout).lines();
    let mut last_path: Option<PathBuf> = None;

    let mut base_bytes: u64 = 0;
    let mut last_stage_total: u64 = 0;
    let mut last_dl_bytes: u64 = 0;

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                kill_tree(&mut child).await;
                let _ = stderr_task.await;
                return Err("cancelled".to_string());
            }
            next = lines.next_line() => {
                match next {
                    Ok(Some(line)) => {
                        let trimmed = line.trim();

                        if let Some(rest) = trimmed.strip_prefix("__YTDEST__") {
                            let path_str = rest.trim().to_string();
                            if !path_str.is_empty() {
                                last_path = Some(PathBuf::from(&path_str));
                                let _ = dest_tx.send(path_str);
                            }
                        } else if let Some(rest) = trimmed.strip_prefix("__YTPATH__") {
                            let path_str = rest.trim();
                            if !path_str.is_empty() {
                                last_path = Some(PathBuf::from(path_str));
                            }
                        } else if let Some(rest) = trimmed.strip_prefix("__YTPROG__") {
                            let parts: Vec<&str> = rest.split_ascii_whitespace().collect();
                            if parts.len() >= 2 {
                                let dl_bytes = parse_na_u64(parts[0]);
                                let total_bytes = parse_na_u64(parts[1]);
                                let speed_bytes = if parts.len() >= 3 {
                                    parse_na_u64(parts[2])
                                } else {
                                    None
                                };

                                if let Some(dl) = dl_bytes {
                                    if dl < last_dl_bytes && last_dl_bytes > 65536 {
                                        base_bytes += last_stage_total.max(last_dl_bytes);
                                    }
                                    last_dl_bytes = dl;
                                    if let Some(t) = total_bytes {
                                        last_stage_total = t;
                                        total.store(base_bytes + t, Ordering::Relaxed);
                                    }
                                    completed.store(base_bytes + dl, Ordering::Relaxed);
                                    connections.store(1, Ordering::Relaxed);
                                }
                                if let Some(s) = speed_bytes {
                                    speed.store(s, Ordering::Relaxed);
                                }
                            }
                        }
                        else if let Some(rest) = trimmed.strip_prefix("[download] Destination: ") {
                            let path_str = rest.trim().to_string();
                            if !path_str.is_empty() {
                                last_path = Some(PathBuf::from(&path_str));
                                let _ = dest_tx.send(path_str);
                            }
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        kill_tree(&mut child).await;
                        let _ = stderr_task.await;
                        return Err(format!("failed reading yt-dlp output: {e}"));
                    }
                }
            }
        }
    }

    let status = child
        .wait()
        .await
        .map_err(|e| format!("failed waiting for yt-dlp: {e}"))?;

    let stderr_lines = stderr_task.await.unwrap_or_default();
    if !status.success() {
        let stderr_summary = stderr_lines[stderr_lines.len().saturating_sub(5)..].join("\n");
        if stderr_summary.is_empty() {
            return Err(format!("yt-dlp failed with exit code {:?}", status.code()));
        }
        return Err(stderr_summary);
    }

    let resolved = if let Some(path) = last_path {
        if path.is_absolute() || dir.trim().is_empty() {
            path
        } else {
            Path::new(dir).join(path)
        }
    } else if !out.trim().is_empty() {
        if Path::new(out).is_absolute() || dir.trim().is_empty() {
            PathBuf::from(out)
        } else {
            Path::new(dir).join(out)
        }
    } else {
        Path::new(dir).to_path_buf()
    };

    let total_val = total.load(Ordering::Relaxed);
    if total_val > 0 {
        completed.store(total_val, Ordering::Relaxed);
    }
    speed.store(0, Ordering::Relaxed);
    Ok(resolved)
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct MediaFormat {
    pub format_id: String,
    pub ext: String,
    pub resolution: String,
    pub fps: Option<f64>,
    pub vcodec: String,
    pub acodec: String,
    pub filesize: Option<u64>,
    pub filesize_approx: Option<u64>,
    pub tbr: Option<f64>,
    pub abr: Option<f64>,
    pub vbr: Option<f64>,
    pub format_note: String,
    pub audio_only: bool,
    pub video_only: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub struct MediaInfo {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub duration: Option<f64>,
    pub uploader: Option<String>,
    pub upload_date: Option<String>,
    pub thumbnail: Option<String>,
    pub view_count: Option<u64>,
    pub channel: Option<String>,
    pub webpage_url: String,
    pub formats: Vec<MediaFormat>,
    pub is_playlist: bool,
    pub playlist_count: Option<u64>,
}

fn extract_format(obj: &serde_json::Value) -> Option<MediaFormat> {
    let format_id = obj.get("format_id")?.as_str()?.to_string();
    let ext = obj
        .get("ext")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let vcodec = obj
        .get("vcodec")
        .and_then(|v| v.as_str())
        .unwrap_or("none")
        .to_string();
    let acodec = obj
        .get("acodec")
        .and_then(|v| v.as_str())
        .unwrap_or("none")
        .to_string();
    let resolution = obj
        .get("resolution")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            let w = obj.get("width").and_then(|v| v.as_u64());
            let h = obj.get("height").and_then(|v| v.as_u64());
            match (w, h) {
                (Some(w), Some(h)) => format!("{w}x{h}"),
                _ => "audio only".to_string(),
            }
        });
    let fps = obj.get("fps").and_then(|v| v.as_f64());
    let filesize = obj.get("filesize").and_then(|v| v.as_u64());
    let filesize_approx = obj.get("filesize_approx").and_then(|v| v.as_u64());
    let tbr = obj.get("tbr").and_then(|v| v.as_f64());
    let abr = obj.get("abr").and_then(|v| v.as_f64());
    let vbr = obj.get("vbr").and_then(|v| v.as_f64());
    let format_note = obj
        .get("format_note")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let audio_only = vcodec == "none";
    let video_only = acodec == "none";

    Some(MediaFormat {
        format_id,
        ext,
        resolution,
        fps,
        vcodec,
        acodec,
        filesize,
        filesize_approx,
        tbr,
        abr,
        vbr,
        format_note,
        audio_only,
        video_only,
    })
}

fn media_info_command(
    url: &str,
    options: &Map<String, Value>,
) -> Result<(Command, tempfile::TempDir), String> {
    let (mut cmd, home) = yt_dlp_command()?;
    cmd.arg("--dump-json")
        .arg("--no-playlist")
        .arg("--flat-playlist")
        .kill_on_drop(true);

    if is_youtube_uri(url) {
        cmd.arg("--extractor-args")
            .arg("youtube:player_client=web,default");
    }

    apply_yt_dlp_network_options(&mut cmd, options, url)?;
    Ok((cmd, home))
}

pub async fn get_media_info(url: &str, options: &Map<String, Value>) -> Result<MediaInfo, String> {
    ensure_yt_dlp().await?;

    let (mut cmd, _home) = media_info_command(url, options)?;
    cmd.arg("--").arg(url);

    let output = cmd
        .output()
        .await
        .map_err(|e| format!("failed to run yt-dlp: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let lines: Vec<_> = stderr.lines().collect();
        let last = lines[lines.len().saturating_sub(3)..].join("\n");
        return Err(if last.is_empty() {
            format!("yt-dlp failed with exit code {:?}", output.status.code())
        } else {
            last
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let first_line = stdout
        .lines()
        .find(|l| !l.trim().is_empty())
        .ok_or_else(|| "yt-dlp returned no output".to_string())?;

    let json: serde_json::Value = serde_json::from_str(first_line)
        .map_err(|e| format!("failed to parse yt-dlp JSON: {e}"))?;

    let formats: Vec<MediaFormat> = json
        .get("formats")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(extract_format).collect())
        .unwrap_or_default();

    let is_playlist = json.get("_type").and_then(|v| v.as_str()) == Some("playlist");
    let playlist_count = json
        .get("playlist_count")
        .and_then(|v| v.as_u64())
        .or_else(|| {
            json.get("entries")
                .and_then(|v| v.as_array())
                .map(|a| a.len() as u64)
        });

    Ok(MediaInfo {
        id: json
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        title: json
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or(url)
            .to_string(),
        description: json
            .get("description")
            .and_then(|v| v.as_str())
            .map(|s| s.chars().take(500).collect()),
        duration: json.get("duration").and_then(|v| v.as_f64()),
        uploader: json
            .get("uploader")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        upload_date: json
            .get("upload_date")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        thumbnail: json
            .get("thumbnail")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        view_count: json.get("view_count").and_then(|v| v.as_u64()),
        channel: json
            .get("channel")
            .or_else(|| json.get("uploader"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()),
        webpage_url: json
            .get("webpage_url")
            .and_then(|v| v.as_str())
            .unwrap_or(url)
            .to_string(),
        formats,
        is_playlist,
        playlist_count,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn max_tries_maps_to_yt_dlp_retries() {
        assert_eq!(super::yt_dlp_retries(0), "infinite");
        assert_eq!(super::yt_dlp_retries(1), "0");
        assert_eq!(super::yt_dlp_retries(5), "4");
    }

    use super::*;
    use serde_json::json;

    #[test]
    fn youtube_uris_detected() {
        assert!(is_youtube_uri("https://www.youtube.com/watch?v=abc"));
        assert!(is_youtube_uri("https://youtu.be/abc"));
        assert!(is_youtube_uri("https://music.youtube.com/watch?v=abc"));
        assert!(!is_youtube_uri("https://vimeo.com/12345"));
    }

    #[test]
    fn media_allowlist_matches_known_hosts() {
        assert!(is_media_uri("https://www.youtube.com/watch?v=abc"));
        assert!(is_media_uri("https://youtu.be/abc"));
        assert!(is_media_uri("https://vimeo.com/12345"));
        assert!(is_media_uri("https://www.bilibili.com/video/BV1"));
        assert!(is_media_uri("https://m.bilibili.com/video/BV1"));
        assert!(is_media_uri("https://vm.tiktok.com/xyz"));
        assert!(is_media_uri("https://x.com/user/status/1"));
        assert!(is_media_uri("https://twitter.com/user/status/1"));
    }

    #[test]
    fn media_allowlist_rejects_non_media() {
        assert!(!is_media_uri("https://example.com/file.zip"));
        assert!(!is_media_uri("magnet:?xt=urn:btih:abc"));
        assert!(!is_media_uri("ftp://example.com/file.bin"));
        assert!(!is_media_uri("ed2k://|file|x|1|H|/"));
        assert!(!is_media_uri(""));
        assert!(!is_media_uri("https://notyoutube.com/watch"));
        assert!(!is_media_uri("https://youtube.com.evil.test/watch"));
    }

    #[test]
    fn force_ytdlp_option_parsed() {
        let mut bool_opt = Map::new();
        bool_opt.insert("force-ytdlp".into(), json!(true));
        assert!(is_force_ytdlp(&bool_opt));

        let mut str_opt = Map::new();
        str_opt.insert("force-ytdlp".into(), json!("true"));
        assert!(is_force_ytdlp(&str_opt));

        let empty = Map::new();
        assert!(!is_force_ytdlp(&empty));

        let mut false_opt = Map::new();
        false_opt.insert("force-ytdlp".into(), json!(false));
        assert!(!is_force_ytdlp(&false_opt));
    }

    #[cfg(windows)]
    const ROOT: &str = r"C:\";
    #[cfg(not(windows))]
    const ROOT: &str = "/";
    const S: char = std::path::MAIN_SEPARATOR;

    fn work() -> std::io::Result<PathBuf> {
        Ok(PathBuf::from(format!("{ROOT}work")))
    }

    fn no_cwd() -> std::io::Result<PathBuf> {
        Err(std::io::Error::other("no working directory"))
    }

    #[test]
    fn output_template_puts_the_folder_in_front_of_the_name() {
        let dl = format!("{ROOT}Downloads");
        assert_eq!(
            yt_dlp_output_template(&dl, "", no_cwd).unwrap(),
            format!("{dl}{S}%(title)s [%(id)s].%(ext)s")
        );
        assert_eq!(
            yt_dlp_output_template(&format!(" {dl} "), "%(title)s.%(ext)s", no_cwd).unwrap(),
            format!("{dl}{S}%(title)s.%(ext)s")
        );
        assert_eq!(
            yt_dlp_output_template(&dl, "clips/a.mp4", no_cwd).unwrap(),
            format!("{dl}{S}clips/a.mp4")
        );
    }

    #[test]
    fn output_template_keeps_an_absolute_out() {
        let out = format!("{ROOT}elsewhere{S}%(id)s.%(ext)s");
        assert_eq!(
            yt_dlp_output_template(&format!("{ROOT}Downloads"), &out, no_cwd).unwrap(),
            out
        );
        assert_eq!(yt_dlp_output_template("", &out, no_cwd).unwrap(), out);
    }

    #[test]
    fn output_template_escapes_percent_in_the_folder_only() {
        assert_eq!(
            yt_dlp_output_template(&format!("{ROOT}100% done"), "%(id)s.mp4", no_cwd).unwrap(),
            format!("{ROOT}100%% done{S}%(id)s.mp4")
        );
        let percent_cwd = || Ok(PathBuf::from(format!("{ROOT}a%b")));
        assert_eq!(
            yt_dlp_output_template("", "a.mp4", percent_cwd).unwrap(),
            format!("{ROOT}a%%b{S}a.mp4")
        );
    }

    #[test]
    fn output_template_resolves_other_folders_against_the_working_directory() {
        assert_eq!(
            yt_dlp_output_template("", "a.mp4", work).unwrap(),
            format!("{ROOT}work{S}a.mp4")
        );
        assert_eq!(
            yt_dlp_output_template(".", "a.mp4", work).unwrap(),
            format!("{ROOT}work{S}.{S}a.mp4")
        );
        assert_eq!(
            yt_dlp_output_template("videos", "", work).unwrap(),
            format!("{ROOT}work{S}videos{S}%(title)s [%(id)s].%(ext)s")
        );
        assert_eq!(
            yt_dlp_output_template("~videos", "a.mp4", work).unwrap(),
            format!("{ROOT}work{S}~videos{S}a.mp4")
        );
        assert!(yt_dlp_output_template("", "a.mp4", no_cwd).is_err());
    }

    #[test]
    fn output_template_leaves_a_home_folder_to_yt_dlp() {
        assert_eq!(
            yt_dlp_output_template("~/Videos", "", no_cwd).unwrap(),
            format!("~/Videos{S}%(title)s [%(id)s].%(ext)s")
        );
        assert_eq!(
            yt_dlp_output_template("~", "a.mp4", no_cwd).unwrap(),
            format!("~{S}a.mp4")
        );
        assert_eq!(
            yt_dlp_output_template(&format!("{ROOT}Downloads"), "~/a.mp4", no_cwd).unwrap(),
            "~/a.mp4"
        );
    }

    #[cfg(windows)]
    #[test]
    fn output_template_joins_with_backslashes_on_windows() {
        assert_eq!(
            yt_dlp_output_template(r"D:\Users\me\Videos", "%(title)s.%(ext)s", no_cwd).unwrap(),
            r"D:\Users\me\Videos\%(title)s.%(ext)s"
        );
        assert_eq!(
            yt_dlp_output_template(r"D:\Videos", r"E:\a.mp4", no_cwd).unwrap(),
            r"E:\a.mp4"
        );
        assert_eq!(
            yt_dlp_output_template(r"~\Videos", "a.mp4", no_cwd).unwrap(),
            r"~\Videos\a.mp4"
        );
    }

    #[cfg(not(windows))]
    #[test]
    fn output_template_treats_windows_paths_as_relative_elsewhere() {
        assert_eq!(
            yt_dlp_output_template(r"D:\Videos", "a.mp4", work).unwrap(),
            r"/work/D:\Videos/a.mp4"
        );
        assert_eq!(
            yt_dlp_output_template(r"~\Videos", "a.mp4", work).unwrap(),
            r"/work/~\Videos/a.mp4"
        );
    }

    #[test]
    fn yt_dlp_commands_run_in_an_empty_private_folder() {
        let (download, download_home) =
            media_download_command("https://youtu.be/x", "", "", &Map::new(), 0, None).unwrap();
        let (info, info_home) = media_info_command("https://youtu.be/x", &Map::new()).unwrap();
        for (cmd, home) in [(download, download_home), (info, info_home)] {
            assert_eq!(cmd.as_std().get_current_dir(), Some(home.path()));
            assert!(std::fs::read_dir(home.path()).unwrap().next().is_none());
        }
    }

    fn download_args(ffmpeg: Option<&Path>) -> Vec<String> {
        let (cmd, _home) =
            media_download_command("https://youtu.be/x", "", "", &Map::new(), 0, ffmpeg).unwrap();
        cmd.as_std()
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    fn value_after<'a>(args: &'a [String], flag: &str) -> Option<&'a str> {
        let at = args.iter().position(|arg| arg == flag)?;
        args.get(at + 1).map(String::as_str)
    }

    #[test]
    fn ffmpeg_location_is_only_a_path() {
        let absolute = PathBuf::from(format!("{ROOT}usr{S}bin{S}ffmpeg"));
        assert_eq!(
            yt_dlp_ffmpeg_location(Some(&absolute)),
            Some(absolute.as_path())
        );
        assert_eq!(yt_dlp_ffmpeg_location(Some(Path::new("ffmpeg"))), None);
        assert_eq!(yt_dlp_ffmpeg_location(None), None);
    }

    #[test]
    fn a_bare_ffmpeg_name_is_not_passed_as_a_location_but_still_merges() {
        let args = download_args(Some(Path::new("ffmpeg")));
        assert_eq!(value_after(&args, "--ffmpeg-location"), None);
        assert_eq!(value_after(&args, "--merge-output-format"), Some("mp4"));
    }

    #[test]
    fn an_absolute_ffmpeg_path_is_passed_on_and_merges() {
        let absolute = PathBuf::from(format!("{ROOT}usr{S}bin{S}ffmpeg"));
        let args = download_args(Some(&absolute));
        assert_eq!(value_after(&args, "--ffmpeg-location"), absolute.to_str());
        assert_eq!(value_after(&args, "--merge-output-format"), Some("mp4"));
    }

    #[test]
    fn no_ffmpeg_passes_neither_a_location_nor_a_merge_format() {
        let args = download_args(None);
        assert_eq!(value_after(&args, "--ffmpeg-location"), None);
        assert_eq!(value_after(&args, "--merge-output-format"), None);
    }

    const UNREACHABLE_URL: &str = "http://127.0.0.1:9/x";

    fn folder_with_bad_config() -> tempfile::TempDir {
        let folder = tempfile::tempdir().unwrap();
        std::fs::write(
            folder.path().join("yt-dlp.conf"),
            "--risuko-test-no-such-option\n",
        )
        .unwrap();
        folder
    }

    async fn assert_runs_without_the_config(mut cmd: Command, inherited_cwd: &Path) {
        if check_yt_dlp_available().await.is_err() {
            eprintln!("yt-dlp is not installed; skipping the test against the real program");
            return;
        }
        if cmd.as_std().get_current_dir().is_none() {
            cmd.current_dir(inherited_cwd);
        }
        cmd.arg("--simulate").arg("--").arg(UNREACHABLE_URL);
        let output = cmd.output().await.unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!text.contains("no such option"), "{text}");
        assert_ne!(output.status.code(), Some(2), "{text}");
        assert!(text.contains("[generic]"), "{text}");
    }

    #[tokio::test]
    async fn download_ignores_a_config_file_in_the_save_folder() {
        let folder = folder_with_bad_config();
        let dir = folder.path().to_string_lossy();
        let (cmd, _home) =
            media_download_command(UNREACHABLE_URL, &dir, "", &Map::new(), 0, None).unwrap();
        assert_runs_without_the_config(cmd, folder.path()).await;
    }

    #[tokio::test]
    async fn media_info_ignores_a_config_file_in_the_working_directory() {
        let folder = folder_with_bad_config();
        let (cmd, _home) = media_info_command(UNREACHABLE_URL, &Map::new()).unwrap();
        assert_runs_without_the_config(cmd, folder.path()).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_probe_that_outlasts_its_limit_returns_nothing_and_stops_waiting() {
        let mut cmd = Command::new("sleep");
        cmd.arg("5");
        let started = std::time::Instant::now();
        let result = output_within(cmd, Duration::from_millis(100)).await;
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_probe_returns_the_output_of_a_command_that_finishes() {
        let output = probe_output("echo", "hello").await.unwrap();
        assert!(output.status.success());
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "hello");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_probe_of_a_missing_command_returns_nothing() {
        assert!(probe_output("risuko-no-such-command", "--version")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn yt_dlp_finds_an_ffmpeg_that_is_only_on_the_path() {
        if check_yt_dlp_available().await.is_err() {
            eprintln!("yt-dlp is not installed; skipping the test against the real program");
            return;
        }
        let Some(ffmpeg) = find_ffmpeg().await else {
            eprintln!("ffmpeg is not installed; skipping the test against the real program");
            return;
        };
        let folder = tempfile::tempdir().unwrap();
        let dir = folder.path().to_string_lossy();
        let (mut cmd, _home) = media_download_command(
            UNREACHABLE_URL,
            &dir,
            "",
            &Map::new(),
            0,
            Some(ffmpeg.as_path()),
        )
        .unwrap();
        cmd.arg("--simulate")
            .arg("-v")
            .arg("--")
            .arg(UNREACHABLE_URL);
        let output = cmd.output().await.unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!text.contains("Continuing without ffmpeg"), "{text}");
    }
}
