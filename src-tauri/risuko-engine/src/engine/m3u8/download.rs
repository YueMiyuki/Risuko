use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

use super::parser::{self, ParsedPlaylist, Variant};
use super::segment;
use crate::engine::http;
use crate::engine::speed_limiter::{SpeedEma, SpeedLimiter, MIN_EMA_SAMPLE_SECS};

#[allow(clippy::too_many_arguments)]
pub async fn run_m3u8_download(
    gid: &str,
    uri: &str,
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
    tracing::info!("[m3u8] Starting download: uri={uri}, dir={dir}, out={out}");

    let dir_path = Path::new(dir);
    std::fs::create_dir_all(dir_path).map_err(|e| format!("Failed to create dir: {e}"))?;

    let mut client = build_client(options, Some(uri))?;

    let playlist = parser::fetch_and_parse_playlist(uri, &client).await?;

    let (media_playlist_url, media_playlist) = match playlist {
        ParsedPlaylist::Master { variants } => {
            if variants.is_empty() {
                return Err("Master playlist has no variants".to_string());
            }
            let variant = select_variant(&variants, options);
            if variant.separate_audio {
                tracing::warn!(
                    "[m3u8] Variant uses a separate audio rendition, which is not downloaded; the output has no audio"
                );
            }
            tracing::info!(
                "[m3u8] Selected variant: bandwidth={}, url={}",
                variant.bandwidth,
                variant.url
            );
            let media = if same_host(uri, &variant.url) {
                parser::fetch_and_parse_playlist(&variant.url, &client).await?
            } else {
                let plain = build_client(options, None)?;
                parser::fetch_and_parse_playlist(&variant.url, &plain).await?
            };
            (variant.url.clone(), media)
        }
        media @ ParsedPlaylist::Media { .. } => (uri.to_string(), media),
    };

    let ParsedPlaylist::Media {
        segments,
        media_sequence,
        end_list,
    } = media_playlist
    else {
        return Err("Expected media playlist after variant resolution".to_string());
    };

    if !end_list {
        return Err("Live streams (no #EXT-X-ENDLIST) are not supported".to_string());
    }

    if segments.is_empty() {
        return Err("Media playlist has no segments".to_string());
    }

    check_cancelled(&cancel_token)?;

    if leaves_host(uri, &media_playlist_url, &segments) {
        client = build_client(options, None)?;
    }

    let total_segments = segments.len();
    tracing::info!(
        "[m3u8] Downloading {total_segments} segments from {}",
        media_playlist_url
    );

    let split = options
        .get("split")
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
        })
        .unwrap_or(5)
        .max(1) as usize;

    let filename = if out.is_empty() {
        infer_filename_from_uri(uri)
    } else {
        out.to_string()
    };
    let temp_dir_name = temp_dir_name_for(
        gid,
        &filename,
        &media_playlist_url,
        &segments,
        media_sequence,
    );
    let temp_dir = dir_path.join(&temp_dir_name);
    let legacy_name =
        legacy_temp_dir_name_for(&filename, &media_playlist_url, &segments, media_sequence);
    adopt_legacy_temp_dir(dir_path, &legacy_name, &temp_dir_name).await;

    let speed_completed = completed.clone();
    let speed_val = speed.clone();
    let speed_cancel = cancel_token.clone();
    let speed_tracker = tokio::spawn(async move {
        run_speed_tracker(speed_completed, speed_val, speed_cancel).await;
    });

    let segments_result = segment::download_segments(
        &segments,
        media_sequence,
        &temp_dir,
        &client,
        total.clone(),
        completed.clone(),
        connections.clone(),
        cancel_token.clone(),
        global_limiter,
        task_limiter,
        split,
    )
    .await;

    let (seg_paths, progress) = match segments_result {
        Ok(result) => result,
        Err(e) => {
            let cancelled = cancel_token.is_cancelled() && e.contains("cancelled");
            cancel_token.cancel();
            speed.store(0, Ordering::Relaxed);
            speed_tracker.abort();
            if !cancelled {
                let _ = tokio::fs::remove_dir_all(&temp_dir).await;
            }
            return Err(e);
        }
    };

    speed.store(0, Ordering::Relaxed);
    speed_tracker.abort();

    check_cancelled(&cancel_token)?;

    let final_ts_path = concatenate_segments_unique(&seg_paths, dir_path, &filename).await?;

    if let Ok(meta) = tokio::fs::metadata(&final_ts_path).await {
        let file_size = meta.len();
        total.store(file_size, Ordering::Relaxed);
        completed.store(file_size, Ordering::Relaxed);
    }

    let output_format = options
        .get("m3u8-output-format")
        .and_then(|v| v.as_str())
        .unwrap_or("ts");

    let final_path = if output_format == "mp4" {
        match remux_to_mp4(&final_ts_path, &cancel_token).await {
            Ok(mp4_path) => {
                let _ = tokio::fs::remove_file(&final_ts_path).await;
                mp4_path
            }
            Err(e) if e == "cancelled" => return Err(e),
            Err(e) => {
                tracing::warn!("[m3u8] ffmpeg remux failed, keeping .ts output: {e}");
                final_ts_path
            }
        }
    } else {
        final_ts_path
    };

    segment::cleanup_progress(&progress);
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    speed.store(0, Ordering::Relaxed);

    tracing::info!("[m3u8] Download complete: {}", final_path.display());
    Ok(final_path)
}

fn check_cancelled(cancel_token: &CancellationToken) -> Result<(), String> {
    if cancel_token.is_cancelled() {
        return Err("cancelled".to_string());
    }
    Ok(())
}

async fn run_speed_tracker(
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    cancel_token: CancellationToken,
) {
    let mut last_bytes = completed.load(Ordering::Relaxed);
    let mut last_time = tokio::time::Instant::now();
    let mut ema = SpeedEma::new();
    let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

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
    }
}

fn select_variant<'a>(variants: &'a [Variant], options: &Map<String, Value>) -> &'a Variant {
    if let Some(chosen_url) = options.get("m3u8-variant-url").and_then(|v| v.as_str()) {
        if let Some(v) = variants.iter().find(|v| v.url == chosen_url) {
            return v;
        }
    }

    variants
        .iter()
        .max_by_key(|v| v.bandwidth)
        .unwrap_or(&variants[0])
}

fn build_client(
    options: &Map<String, Value>,
    netrc_uri: Option<&str>,
) -> Result<risuko_http::Client, String> {
    let connect_timeout = options
        .get("connect-timeout")
        .and_then(|v| v.as_u64().or_else(|| v.as_str()?.trim().parse().ok()))
        .filter(|secs| *secs > 0)
        .unwrap_or(30);
    let mut builder = risuko_http::Client::builder()
        .redirect(risuko_http::redirect::Policy::limited(10))
        .connect_timeout(std::time::Duration::from_secs(connect_timeout))
        .tcp_nodelay(true)
        .tcp_keepalive(std::time::Duration::from_secs(60));

    let mut headers = http::build_headers(options);
    if let Some(uri) = netrc_uri {
        http::apply_netrc_auth(&mut headers, uri, options);
    }
    if !headers.contains_key(risuko_http::header::USER_AGENT) {
        builder = builder.user_agent("Mozilla/5.0");
    }
    builder = builder.default_headers(headers);

    if let Some(proxy_url) = options
        .get("all-proxy")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let proxy =
            risuko_http::Proxy::all(proxy_url).map_err(|e| format!("Invalid proxy: {e}"))?;
        builder = builder.proxy(proxy);
    }

    if let Some(no_proxy) = options
        .get("no-proxy")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let matcher = risuko_http::NoProxy::parse(no_proxy);
        if !matcher.is_empty() {
            builder = builder.no_proxy(matcher);
        }
    }

    if let Some(jar) = http::load_cookie_jar(options) {
        builder = builder.cookie_provider(jar);
    }

    builder
        .build()
        .map_err(|e| format!("Failed to build HTTP client: {e}"))
}

fn host_of(url: &str) -> Option<String> {
    risuko_http::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_ascii_lowercase))
}

fn same_host(a: &str, b: &str) -> bool {
    host_of(a) == host_of(b)
}

fn leaves_host(uri: &str, media_playlist_url: &str, segments: &[parser::Segment]) -> bool {
    !same_host(uri, media_playlist_url)
        || segments.iter().any(|segment| {
            !same_host(uri, &segment.url)
                || segment
                    .encryption
                    .as_ref()
                    .is_some_and(|enc| !same_host(uri, &enc.key_uri))
        })
}

fn infer_filename_from_uri(uri: &str) -> String {
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

fn playlist_hash(playlist_url: &str, segments: &[parser::Segment], media_sequence: u64) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    playlist_url.hash(&mut hasher);
    media_sequence.hash(&mut hasher);
    for seg in segments {
        seg.url.hash(&mut hasher);
        if let Some(br) = &seg.byte_range {
            br.offset.hash(&mut hasher);
            br.length.hash(&mut hasher);
        }
    }
    hasher.finish()
}

fn gid_component(gid: &str) -> String {
    gid.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
}

fn temp_dir_name_for(
    gid: &str,
    filename: &str,
    playlist_url: &str,
    segments: &[parser::Segment],
    media_sequence: u64,
) -> String {
    format!(
        ".m3u8_{}_{}_{:016x}",
        crate::engine::util::safe_filename(filename, "download"),
        gid_component(gid),
        playlist_hash(playlist_url, segments, media_sequence)
    )
}

fn legacy_temp_dir_name_for(
    filename: &str,
    playlist_url: &str,
    segments: &[parser::Segment],
    media_sequence: u64,
) -> String {
    format!(
        ".m3u8_{}_{:016x}",
        crate::engine::util::safe_filename(filename, "download"),
        playlist_hash(playlist_url, segments, media_sequence)
    )
}

async fn adopt_legacy_temp_dir(dir: &Path, legacy_name: &str, new_name: &str) {
    let legacy = dir.join(legacy_name);
    let new = dir.join(new_name);
    if !legacy.is_dir() || new.exists() {
        return;
    }
    match tokio::fs::rename(&legacy, &new).await {
        Ok(()) => tracing::info!("[m3u8] Adopted legacy temp dir {legacy_name} as {new_name}"),
        Err(e) => tracing::warn!("[m3u8] Failed to adopt legacy temp dir {legacy_name}: {e}"),
    }
}

fn is_hash16(s: &str) -> bool {
    s.len() == 16 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

pub fn remove_temp_dirs(gid: &str, dir: &str, uri: &str, out: &str) {
    let filename = if out.is_empty() {
        infer_filename_from_uri(uri)
    } else {
        out.to_string()
    };
    let legacy_prefix = format!(
        ".m3u8_{}_",
        crate::engine::util::safe_filename(&filename, "download")
    );
    let own_prefix = format!("{legacy_prefix}{}_", gid_component(gid));
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let own = name.strip_prefix(&own_prefix).is_some_and(is_hash16);
        let legacy = name.strip_prefix(&legacy_prefix).is_some_and(is_hash16);
        if !(own || legacy) {
            continue;
        }
        if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            if let Err(e) = std::fs::remove_dir_all(entry.path()) {
                tracing::warn!("[m3u8] Failed to remove {}: {e}", entry.path().display());
            }
        }
    }
}

fn final_path_candidate(dir: &Path, filename: &str, n: u32) -> PathBuf {
    let sanitized = crate::engine::util::safe_filename(filename, "download");
    if n == 0 {
        return dir.join(sanitized);
    }

    let (stem, ext) = match sanitized.rfind('.') {
        Some(dot) if dot > 0 => (&sanitized[..dot], &sanitized[dot..]),
        _ => (sanitized.as_str(), ""),
    };

    let numbered = if ext.is_empty() {
        format!("{stem}.{n}")
    } else {
        format!("{stem}.{n}{ext}")
    };
    dir.join(numbered)
}

async fn concatenate_segments_unique(
    segment_paths: &[PathBuf],
    output_dir: &Path,
    filename: &str,
) -> Result<PathBuf, String> {
    let output_path = reserve_unique_output_path(output_dir, filename).await?;
    if let Err(e) = concatenate_segments(segment_paths, &output_path).await {
        let _ = tokio::fs::remove_file(&output_path).await;
        return Err(e);
    }
    Ok(output_path)
}

async fn concatenate_segments(segment_paths: &[PathBuf], output_path: &Path) -> Result<(), String> {
    let segment_paths = segment_paths.to_vec();
    let output_path = output_path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let output = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&output_path)
            .map_err(|e| format!("Failed to create output file: {e}"))?;
        let mut output = std::io::BufWriter::with_capacity(1 << 20, output);
        for path in &segment_paths {
            let mut seg_file = std::fs::File::open(path)
                .map_err(|e| format!("Failed to open segment {}: {e}", path.display()))?;
            std::io::copy(&mut seg_file, &mut output)
                .map_err(|e| format!("Failed to write to output: {e}"))?;
        }
        std::io::Write::flush(&mut output).map_err(|e| format!("Failed to flush output: {e}"))
    })
    .await
    .map_err(|e| format!("Segment merge task failed: {e}"))?
}

async fn remux_to_mp4(ts_path: &Path, cancel_token: &CancellationToken) -> Result<PathBuf, String> {
    let ffmpeg = crate::engine::media::find_ffmpeg()
        .await
        .ok_or_else(|| "ffmpeg is not available in PATH".to_string())?;
    let parent = ts_path
        .parent()
        .ok_or_else(|| "M3U8 output path has no parent directory".to_string())?;
    let mp4_name = ts_path
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| {
            let mut name = name.to_string();
            if let Some(dot) = name.rfind('.') {
                name.replace_range(dot.., ".mp4");
                name
            } else {
                format!("{name}.mp4")
            }
        })
        .ok_or_else(|| "M3U8 output filename is not valid UTF-8".to_string())?;
    let mp4_path = reserve_unique_output_path(parent, &mp4_name).await?;

    let mut command = tokio::process::Command::new(ffmpeg);
    command
        .arg("-i")
        .arg(ts_path)
        .arg("-c")
        .arg("copy")
        .arg("-movflags")
        .arg("+faststart")
        .arg("-y")
        .arg(&mp4_path)
        .kill_on_drop(true);
    let output = tokio::select! {
        output = command.output() => output.map_err(|e| format!("ffmpeg execution failed: {e}")),
        _ = cancel_token.cancelled() => Err("cancelled".to_string()),
    };
    let output = match output {
        Ok(output) => output,
        Err(e) => {
            let _ = tokio::fs::remove_file(&mp4_path).await;
            return Err(e);
        }
    };

    if !output.status.success() {
        let _ = tokio::fs::remove_file(&mp4_path).await;
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("ffmpeg remux failed: {stderr}"));
    }

    Ok(mp4_path)
}

async fn reserve_unique_output_path(dir: &Path, filename: &str) -> Result<PathBuf, String> {
    for n in 0u32.. {
        let path = final_path_candidate(dir, filename, n);
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
        {
            Ok(_) => return Ok(path),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(format!("Failed to reserve output file: {e}")),
        }
    }

    Err("Failed to reserve output filename".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn headers_come_from_task_options() {
        let mut options = Map::new();
        options.insert(
            "header".into(),
            serde_json::json!(["Authorization: Bearer t", "X-Test: 1"]),
        );
        options.insert("referer".into(), "https://site.example/".into());
        options.insert("cookie".into(), "a=b".into());
        let headers = http::build_headers(&options);
        assert_eq!(headers["authorization"], "Bearer t");
        assert_eq!(headers["referer"], "https://site.example/");
        assert_eq!(headers["cookie"], "a=b");
        assert!(build_client(&options, Some("https://site.example/a.m3u8")).is_ok());
    }

    #[test]
    fn netrc_credentials_apply_and_stay_on_the_task_host() {
        let dir = tempfile::tempdir().unwrap();
        let netrc = dir.path().join("netrc");
        std::fs::write(&netrc, "machine site.example login u password p\n").unwrap();
        let mut options = Map::new();
        options.insert("netrc-path".into(), netrc.to_string_lossy().into());
        let mut headers = http::build_headers(&options);
        http::apply_netrc_auth(&mut headers, "https://site.example/a.m3u8", &options);
        assert!(headers.contains_key(risuko_http::header::AUTHORIZATION));

        let segment = |url: &str| parser::Segment {
            url: url.to_string(),
            byte_range: None,
            encryption: None,
        };
        let uri = "https://site.example/a.m3u8";
        assert!(!leaves_host(
            uri,
            uri,
            &[segment("https://site.example/1.ts")]
        ));
        assert!(leaves_host(
            uri,
            uri,
            &[segment("https://cdn.example/1.ts")]
        ));
    }

    #[test]
    fn test_infer_filename_from_uri() {
        assert_eq!(
            infer_filename_from_uri("https://example.com/video.m3u8"),
            "video.ts"
        );
        assert_eq!(
            infer_filename_from_uri("https://example.com/live/stream.m3u8?token=abc"),
            "stream.ts"
        );
        assert_eq!(
            infer_filename_from_uri("https://example.com/path/"),
            "download.ts"
        );
        assert_eq!(
            infer_filename_from_uri("https://example.com/video.m3u"),
            "video.ts"
        );
    }

    fn seg(url: &str) -> parser::Segment {
        parser::Segment {
            url: url.to_string(),
            byte_range: None,
            encryption: None,
        }
    }

    #[test]
    fn temp_dir_names_are_stable_per_playlist() {
        let one = [seg("https://e.com/1.ts")];
        let a = temp_dir_name_for("g1", "video.ts", "https://e.com/p.m3u8", &one, 0);
        let b = temp_dir_name_for("g1", "video.ts", "https://e.com/p.m3u8", &one, 0);
        let changed = [seg("https://e.com/2.ts")];
        let c = temp_dir_name_for("g1", "video.ts", "https://e.com/p.m3u8", &changed, 0);
        assert!(a.starts_with(".m3u8_video.ts_g1_"));
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn two_tasks_with_same_inputs_get_different_temp_dirs() {
        let one = [seg("https://e.com/1.ts")];
        let a = temp_dir_name_for("aaaa", "video.ts", "https://e.com/p.m3u8", &one, 0);
        let b = temp_dir_name_for("bbbb", "video.ts", "https://e.com/p.m3u8", &one, 0);
        assert_ne!(a, b);
        let legacy = legacy_temp_dir_name_for("video.ts", "https://e.com/p.m3u8", &one, 0);
        assert!(a.ends_with(&legacy[".m3u8_video.ts".len()..]));
        assert_ne!(a, legacy);
    }

    #[test]
    fn remove_temp_dirs_only_matches_own_and_legacy_dirs() {
        let dir = std::env::temp_dir().join(format!("risuko-m3u8-rm-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mine = dir.join(".m3u8_video.ts_gidone_00000000000000ab");
        let legacy = dir.join(".m3u8_video.ts_00000000000000cd");
        let sibling = dir.join(".m3u8_video.ts_gidtwo_00000000000000ab");
        let other = dir.join(".m3u8_other.ts_gidone_00000000000000ab");
        let bad = dir.join(".m3u8_video.ts_gidone_notahash");
        for d in [&mine, &legacy, &sibling, &other, &bad] {
            std::fs::create_dir_all(d).unwrap();
        }
        remove_temp_dirs(
            "gidone",
            dir.to_str().unwrap(),
            "https://e.com/p.m3u8",
            "video.ts",
        );
        assert!(!mine.exists());
        assert!(!legacy.exists());
        assert!(sibling.exists());
        assert!(other.exists());
        assert!(bad.exists());
        let inferred = dir.join(".m3u8_p.ts_gidone_0000000000000001");
        std::fs::create_dir_all(&inferred).unwrap();
        remove_temp_dirs("gidone", dir.to_str().unwrap(), "https://e.com/p.m3u8", "");
        assert!(!inferred.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn legacy_temp_dir_is_adopted_only_when_new_one_is_absent() {
        let dir = std::env::temp_dir().join(format!("risuko-m3u8-adopt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let one = [seg("https://e.com/1.ts")];
        let legacy_name = legacy_temp_dir_name_for("video.ts", "https://e.com/p.m3u8", &one, 0);
        let new_name = temp_dir_name_for("g1", "video.ts", "https://e.com/p.m3u8", &one, 0);
        std::fs::create_dir_all(dir.join(&legacy_name)).unwrap();
        std::fs::write(dir.join(&legacy_name).join("seg0"), b"x").unwrap();

        adopt_legacy_temp_dir(&dir, &legacy_name, &new_name).await;
        assert!(!dir.join(&legacy_name).exists());
        assert!(dir.join(&new_name).join("seg0").exists());

        let other_name = temp_dir_name_for("g2", "video.ts", "https://e.com/p.m3u8", &one, 0);
        adopt_legacy_temp_dir(&dir, &legacy_name, &other_name).await;
        assert!(!dir.join(&other_name).exists());
        assert!(dir.join(&new_name).join("seg0").exists());

        std::fs::create_dir_all(dir.join(&legacy_name)).unwrap();
        adopt_legacy_temp_dir(&dir, &legacy_name, &new_name).await;
        assert!(dir.join(&legacy_name).exists());
        assert!(dir.join(&new_name).join("seg0").exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn concatenate_segments_joins_in_order() {
        let dir = std::env::temp_dir().join(format!("risuko-m3u8-cat-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let (a, b, out) = (dir.join("a"), dir.join("b"), dir.join("out"));
        std::fs::write(&a, b"hello ").unwrap();
        std::fs::write(&b, b"world").unwrap();
        std::fs::write(&out, b"").unwrap();
        concatenate_segments(&[a, b], &out).await.unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), b"hello world");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn final_path_candidate_deduplicates_stem_and_extension() {
        let dir = Path::new("/downloads");
        assert_eq!(
            final_path_candidate(dir, "video.ts", 0),
            dir.join("video.ts")
        );
        assert_eq!(
            final_path_candidate(dir, "video.ts", 1),
            dir.join("video.1.ts")
        );
        assert_eq!(final_path_candidate(dir, "video", 2), dir.join("video.2"));
    }
}
