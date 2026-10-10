use std::path::Path;
use std::{collections::HashMap, collections::HashSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tauri::{AppHandle, Manager};

use risuko_engine::engine;
use risuko_engine::engine::torrent;

const TEMP_DOWNLOAD_SUFFIX: &str = ".part";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LowSpeedTaskInput {
    pub gid: String,
    pub status: String,
    pub download_speed: Value,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LowSpeedEvaluationResult {
    pub strike_map: HashMap<String, u32>,
    pub recover_at_map: HashMap<String, u64>,
    pub recover_gids: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AutoRetryPlanResult {
    pub attempt_map: HashMap<String, u32>,
    pub next_attempt: u32,
    pub delay_ms: u64,
}

fn normalize_non_negative(value: f64) -> u64 {
    if !value.is_finite() || value <= 0.0 {
        return 0;
    }

    let floored = value.floor();
    if floored >= u64::MAX as f64 {
        u64::MAX
    } else {
        floored as u64
    }
}

fn parse_length_like(value: &Value) -> u64 {
    match value {
        Value::Number(number) => {
            if let Some(parsed) = number.as_u64() {
                return parsed;
            }
            if let Some(parsed) = number.as_i64() {
                return u64::try_from(parsed).unwrap_or(0);
            }
            number.as_f64().map(normalize_non_negative).unwrap_or(0)
        }
        Value::String(text) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                return 0;
            }

            if let Ok(parsed) = trimmed.parse::<u64>() {
                return parsed;
            }

            trimmed
                .parse::<f64>()
                .map(normalize_non_negative)
                .unwrap_or(0)
        }
        Value::Bool(flag) if *flag => 1,
        _ => 0,
    }
}

fn parse_counter_like(value: &Value) -> u32 {
    let parsed = parse_length_like(value);
    if parsed >= u32::MAX as u64 {
        u32::MAX
    } else {
        parsed as u32
    }
}

fn parse_retry_attempt_map(values: HashMap<String, Value>) -> HashMap<String, u32> {
    let mut result = HashMap::with_capacity(values.len());
    for (gid, value) in values {
        let key = gid.trim().to_string();
        if key.is_empty() {
            continue;
        }
        result.insert(key, parse_counter_like(&value));
    }
    result
}

fn compute_auto_retry_delay_ms(
    strategy: &str,
    base_delay_ms: u64,
    next_attempt: u32,
    max_delay_ms: u64,
) -> u64 {
    let min_delay_ms = 1000u64;
    let base_delay_ms = base_delay_ms.max(min_delay_ms);
    let max_delay_ms = max_delay_ms.max(min_delay_ms);

    let computed = if strategy.eq_ignore_ascii_case("exponential") {
        let exponent = next_attempt.saturating_sub(1).min(62);
        (base_delay_ms as u128).saturating_mul(1u128 << exponent)
    } else {
        base_delay_ms as u128
    };

    computed.min(max_delay_ms as u128).max(min_delay_ms as u128) as u64
}

fn infer_out_from_uri_inner(uri: &str) -> String {
    let raw = uri.trim();
    if raw.is_empty() {
        return String::new();
    }

    let lower = raw.to_ascii_lowercase();
    let path_part = lower.split('?').next().unwrap_or(&lower);
    let path_part = path_part.split('#').next().unwrap_or(path_part);
    if path_part.ends_with(".m3u8") || path_part.ends_with(".m3u") {
        let without_query = raw.split('?').next().unwrap_or(raw);
        let without_hash = without_query.split('#').next().unwrap_or(without_query);
        let name = without_hash.rsplit('/').next().unwrap_or("");
        if name.is_empty() {
            return "download.ts".to_string();
        }
        let stem = name
            .strip_suffix(".m3u8")
            .or_else(|| name.strip_suffix(".M3U8"))
            .or_else(|| name.strip_suffix(".m3u"))
            .or_else(|| name.strip_suffix(".M3U"))
            .unwrap_or(name);
        if stem.is_empty() {
            return "download.ts".to_string();
        }
        return format!("{}.ts", stem);
    }

    if lower.starts_with("ed2k://") {
        let body = raw
            .trim_start_matches("ed2k://|file|")
            .trim_start_matches("ed2k://|FILE|")
            .trim_end_matches("|/");
        let parts: Vec<&str> = body.split('|').collect();
        if !parts.is_empty() {
            let decoded = crate::commands::file_cmds::percent_decode_strict(parts[0]);
            let name = decoded.replace('_', " ");
            if !name.is_empty() {
                return name;
            }
        }
        return String::new();
    }

    let without_hash = raw.split('#').next().unwrap_or(raw);
    let without_query = without_hash.split('?').next().unwrap_or(without_hash);
    let candidate = without_query.rsplit('/').next().unwrap_or("").trim();
    let decoded_candidate = crate::commands::file_cmds::percent_decode_lossy(candidate);
    let decoded_candidate = decoded_candidate.trim();
    if decoded_candidate.is_empty() || !decoded_candidate.contains('.') {
        return placeholder_download_name(raw);
    }
    if decoded_candidate.contains('/') || decoded_candidate.contains('\\') {
        return placeholder_download_name(raw);
    }
    if decoded_candidate.starts_with('.') || decoded_candidate.ends_with('.') {
        return placeholder_download_name(raw);
    }

    decoded_candidate.to_string()
}

fn placeholder_download_name(uri: &str) -> String {
    use sha1::{Digest, Sha1};
    let mut hasher = Sha1::new();
    hasher.update(uri.as_bytes());
    let digest = hasher.finalize();
    let hex: String = digest.iter().take(4).map(|b| format!("{b:02x}")).collect();
    format!("download-{hex}")
}

fn ensure_temp_download_suffix(value: &str) -> String {
    let normalized = value.trim();
    if normalized.is_empty() {
        return String::new();
    }

    if normalized
        .to_ascii_lowercase()
        .ends_with(TEMP_DOWNLOAD_SUFFIX)
    {
        return normalized.to_string();
    }

    format!("{}{}", normalized, TEMP_DOWNLOAD_SUFFIX)
}

#[tauri::command]
pub fn evaluate_low_speed_tasks(
    tasks: Vec<LowSpeedTaskInput>,
    threshold_bytes: Value,
    strike_threshold: u32,
    cooldown_ms: u64,
    now_ms: u64,
    strike_map: HashMap<String, Value>,
    recover_at_map: HashMap<String, Value>,
) -> Result<LowSpeedEvaluationResult, String> {
    let threshold = parse_length_like(&threshold_bytes);
    let strike_threshold = strike_threshold.max(1);

    let mut next_strike_map = HashMap::with_capacity(strike_map.len());
    for (gid, value) in strike_map {
        let key = gid.trim();
        if key.is_empty() {
            continue;
        }
        next_strike_map.insert(key.to_string(), parse_counter_like(&value));
    }

    let mut next_recover_at_map = HashMap::with_capacity(recover_at_map.len());
    for (gid, value) in recover_at_map {
        let key = gid.trim();
        if key.is_empty() {
            continue;
        }
        next_recover_at_map.insert(key.to_string(), parse_length_like(&value));
    }

    let mut recover_gids = Vec::new();
    let mut active_gids = HashSet::new();

    for task in tasks {
        let gid = task.gid.trim().to_string();
        if gid.is_empty() || !task.status.eq_ignore_ascii_case("active") {
            continue;
        }

        active_gids.insert(gid.clone());

        let speed = parse_length_like(&task.download_speed);
        if speed >= threshold {
            next_strike_map.remove(&gid);
            next_recover_at_map.remove(&gid);
            continue;
        }

        let strike = next_strike_map
            .get(&gid)
            .copied()
            .unwrap_or(0)
            .saturating_add(1);
        next_strike_map.insert(gid.clone(), strike);

        if strike < strike_threshold {
            continue;
        }
        if next_recover_at_map.get(&gid).copied().unwrap_or(0) > now_ms {
            continue;
        }

        next_strike_map.insert(gid.clone(), 0);
        next_recover_at_map.insert(gid.clone(), now_ms.saturating_add(cooldown_ms));
        recover_gids.push(gid);
    }

    next_strike_map.retain(|gid, _| active_gids.contains(gid));
    next_recover_at_map.retain(|gid, _| active_gids.contains(gid));

    Ok(LowSpeedEvaluationResult {
        strike_map: next_strike_map,
        recover_at_map: next_recover_at_map,
        recover_gids,
    })
}

#[tauri::command]
pub fn plan_auto_retry(
    gid: String,
    strategy: String,
    interval_seconds: Value,
    max_delay_ms: Value,
    attempt_map: HashMap<String, Value>,
) -> Result<AutoRetryPlanResult, String> {
    let gid = gid.trim().to_string();
    if gid.is_empty() {
        return Err("Invalid task gid".to_string());
    }

    let mut next_attempt_map = parse_retry_attempt_map(attempt_map);
    let next_attempt = next_attempt_map
        .get(&gid)
        .copied()
        .unwrap_or(0)
        .saturating_add(1)
        .max(1);
    next_attempt_map.insert(gid, next_attempt);

    let interval_seconds = parse_length_like(&interval_seconds).max(1);
    let base_delay_ms = interval_seconds.saturating_mul(1000);
    let max_delay_ms = parse_length_like(&max_delay_ms).max(1000);
    let delay_ms =
        compute_auto_retry_delay_ms(&strategy, base_delay_ms, next_attempt, max_delay_ms);

    Ok(AutoRetryPlanResult {
        attempt_map: next_attempt_map,
        next_attempt,
        delay_ms,
    })
}

#[tauri::command]
pub async fn restart_engine(handle: AppHandle) -> Result<(), String> {
    let config_dir = handle
        .path()
        .app_config_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."));
    let config =
        risuko_engine::config::ConfigManager::with_dir(config_dir).map_err(|e| e.to_string())?;
    let event_sink: std::sync::Arc<dyn risuko_engine::EventSink> =
        std::sync::Arc::new(crate::bridge::TauriEventSink::new(&handle));
    let upload_mgr = Some(
        handle
            .state::<crate::state::AppState>()
            .upload_sinks
            .clone(),
    );
    risuko_engine::engine::restart_engine(&config, event_sink, upload_mgr)
        .await
        .map_err(|e| e.to_string())
}

async fn add_torrent_by_path_inner(path: &str, options: Option<Value>) -> Result<String, String> {
    let path = path.trim();
    if path.is_empty() {
        return Err("task.new-task-torrent-required".to_string());
    }

    let fs_path = Path::new(path);
    let is_torrent = fs_path
        .extension()
        .and_then(|value| value.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("torrent"))
        == Some(true);
    if !is_torrent {
        return Err("task.new-task-torrent-required".to_string());
    }

    const MAX_TORRENT_BYTES: u64 = 16 * 1024 * 1024;
    let meta = tokio::fs::metadata(fs_path)
        .await
        .map_err(|e| e.to_string())?;
    if meta.len() > MAX_TORRENT_BYTES {
        return Err("Torrent file too large".to_string());
    }
    let bytes = tokio::fs::read(fs_path).await.map_err(|e| e.to_string())?;
    if bytes.is_empty() {
        return Err("Torrent payload is empty".to_string());
    }
    let fallback_name = fs_path
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("download");
    let (is_multi_file, torrent_root_name) =
        crate::commands::file_cmds::inspect_torrent_metadata(&bytes, fallback_name)
            .unwrap_or_else(|_| (false, fallback_name.to_string()));

    let options = options.unwrap_or(Value::Object(Map::new()));
    let mut options = match options {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    if is_multi_file {
        options.remove("out");
    } else {
        let has_out = options
            .get("out")
            .and_then(|value| value.as_str())
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false);

        if !has_out {
            options.insert(
                "out".to_string(),
                Value::String(format!("{}{}", torrent_root_name, TEMP_DOWNLOAD_SUFFIX)),
            );
        } else if let Some(current_out) = options.get("out").and_then(|value| value.as_str()) {
            let trimmed = current_out.trim();
            if !trimmed.to_ascii_lowercase().ends_with(TEMP_DOWNLOAD_SUFFIX) {
                options.insert(
                    "out".to_string(),
                    Value::String(format!("{}{}", trimmed, TEMP_DOWNLOAD_SUFFIX)),
                );
            }
        }
    }

    let manager = engine::get_manager().await.ok_or("Engine not running")?;

    manager.add_torrent_task(bytes, options).await
}

#[tauri::command]
pub async fn add_torrent_by_path(
    _handle: AppHandle,
    _state: tauri::State<'_, crate::state::AppState>,
    path: String,
    options: Option<Value>,
) -> Result<String, String> {
    add_torrent_by_path_inner(&path, options).await
}

#[derive(Serialize)]
pub struct BatchAddResult {
    pub path: String,
    pub gid: Option<String>,
    pub error: Option<String>,
}

#[tauri::command]
pub async fn add_torrents_by_paths(
    _handle: AppHandle,
    _state: tauri::State<'_, crate::state::AppState>,
    paths: Vec<String>,
    options: Option<Value>,
) -> Result<Vec<BatchAddResult>, String> {
    if paths.is_empty() {
        return Err("task.new-task-torrent-required".to_string());
    }

    let per_call_options: Option<Value> = if paths.len() > 1 {
        match options.as_ref() {
            Some(Value::Object(map)) => {
                let mut stripped = map.clone();
                stripped.remove("out");
                Some(Value::Object(stripped))
            }
            other => other.cloned(),
        }
    } else {
        options.clone()
    };

    let mut results = Vec::with_capacity(paths.len());
    for path in paths.iter() {
        match add_torrent_by_path_inner(path, per_call_options.clone()).await {
            Ok(gid) => results.push(BatchAddResult {
                path: path.clone(),
                gid: Some(gid),
                error: None,
            }),
            Err(err) => results.push(BatchAddResult {
                path: path.clone(),
                gid: None,
                error: Some(err),
            }),
        }
    }
    Ok(results)
}

async fn add_metalink_by_path_inner(path: &str, options: Option<Value>) -> Result<String, String> {
    let path = path.trim();
    let fs_path = Path::new(path);
    let is_metalink = fs_path
        .extension()
        .and_then(|value| value.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("meta4") || ext.eq_ignore_ascii_case("metalink"))
        == Some(true);
    if path.is_empty() || !is_metalink {
        return Err("Metalink file (.meta4 or .metalink) required".to_string());
    }
    const MAX_METALINK_BYTES: u64 = 4 * 1024 * 1024;
    let meta = tokio::fs::metadata(fs_path)
        .await
        .map_err(|e| e.to_string())?;
    if meta.len() > MAX_METALINK_BYTES {
        return Err("Metalink file too large".to_string());
    }
    let bytes = tokio::fs::read(fs_path).await.map_err(|e| e.to_string())?;
    if bytes.is_empty() {
        return Err("Metalink payload is empty".to_string());
    }
    let mut options = match options.unwrap_or(Value::Object(Map::new())) {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    options.remove("out");

    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.add_metalink_task(bytes, options).await
}

async fn add_nzb_by_path_inner(path: &str, options: Option<Value>) -> Result<String, String> {
    let path = path.trim();
    let fs_path = Path::new(path);
    let is_nzb = fs_path
        .extension()
        .and_then(|value| value.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("nzb"))
        == Some(true);
    if path.is_empty() || !is_nzb {
        return Err("NZB file (.nzb) required".to_string());
    }
    const MAX_NZB_BYTES: u64 = 16 * 1024 * 1024;
    let meta = tokio::fs::metadata(fs_path)
        .await
        .map_err(|e| e.to_string())?;
    if meta.len() > MAX_NZB_BYTES {
        return Err("NZB file too large".to_string());
    }
    let bytes = tokio::fs::read(fs_path).await.map_err(|e| e.to_string())?;
    if bytes.is_empty() {
        return Err("NZB payload is empty".to_string());
    }
    let mut options = match options.unwrap_or(Value::Object(Map::new())) {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    options.insert(
        "usenet-source-name".to_string(),
        Value::String(
            fs_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("download.nzb")
                .to_string(),
        ),
    );
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.add_nzb_task(bytes, options).await
}

fn is_nzb_url(uri: &str) -> bool {
    let Some(scheme_end) = uri.find("://") else {
        return false;
    };
    let scheme = &uri[..scheme_end];
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return false;
    }
    let path = uri.split(['?', '#']).next().unwrap_or(uri);
    path.rsplit('/')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
        .ends_with(".nzb")
}

fn nzb_source_name_from_url(uri: &str) -> String {
    let path = uri.split(['?', '#']).next().unwrap_or(uri);
    let name = path.rsplit('/').next().unwrap_or("").trim();
    if name.is_empty() {
        "download.nzb".to_string()
    } else {
        crate::commands::file_cmds::percent_decode_lossy(name)
    }
}

async fn fetch_nzb_url(uri: &str, options: &Map<String, Value>) -> Result<Vec<u8>, String> {
    engine::http::fetch_for_nzb(uri, options).await
}

#[tauri::command]
pub async fn add_nzbs_by_paths(
    _handle: AppHandle,
    _state: tauri::State<'_, crate::state::AppState>,
    paths: Vec<String>,
    options: Option<Value>,
) -> Result<Vec<BatchAddResult>, String> {
    if paths.is_empty() {
        return Err("NZB file (.nzb) required".to_string());
    }
    let mut results = Vec::with_capacity(paths.len());
    for path in paths.iter() {
        match add_nzb_by_path_inner(path, options.clone()).await {
            Ok(gid) => results.push(BatchAddResult {
                path: path.clone(),
                gid: Some(gid),
                error: None,
            }),
            Err(err) => results.push(BatchAddResult {
                path: path.clone(),
                gid: None,
                error: Some(err),
            }),
        }
    }
    Ok(results)
}

#[tauri::command]
pub async fn add_metalinks_by_paths(
    _handle: AppHandle,
    _state: tauri::State<'_, crate::state::AppState>,
    paths: Vec<String>,
    options: Option<Value>,
) -> Result<Vec<BatchAddResult>, String> {
    if paths.is_empty() {
        return Err("Metalink file (.meta4) required".to_string());
    }
    let mut results = Vec::with_capacity(paths.len());
    for path in paths.iter() {
        match add_metalink_by_path_inner(path, options.clone()).await {
            Ok(gid) => results.push(BatchAddResult {
                path: path.clone(),
                gid: Some(gid),
                error: None,
            }),
            Err(err) => results.push(BatchAddResult {
                path: path.clone(),
                gid: None,
                error: Some(err),
            }),
        }
    }
    Ok(results)
}

fn is_plain_http_mirror_uri(uri: &str, options: &Map<String, Value>) -> bool {
    let is_http = uri.starts_with("http://") || uri.starts_with("https://");
    if !is_http || is_nzb_url(uri) {
        return false;
    }
    if torrent::is_magnet_uri(uri)
        || engine::m3u8::is_m3u8_uri(uri)
        || engine::ed2k::is_ed2k_uri(uri)
        || engine::ftp::is_ftp_uri(uri)
        || engine::adc::is_adc_uri(uri)
        || engine::gnutella::is_gnutella_uri(uri)
        || engine::g2::is_g2_uri(uri)
        || engine::gift::is_gift_uri(uri)
    {
        return false;
    }
    if engine::media::is_media_uri(uri) || engine::media::is_force_ytdlp(options) {
        return false;
    }
    true
}

#[tauri::command]
pub fn decode_thunder_uri(uri: String) -> Option<String> {
    torrent::decode_thunder_uri(&uri)
}

#[tauri::command]
pub async fn add_uri(
    _state: tauri::State<'_, crate::state::AppState>,
    uris: Vec<String>,
    outs: Option<Vec<String>>,
    options: Option<Value>,
) -> Result<Value, String> {
    let normalized_uris: Vec<String> = uris
        .into_iter()
        .map(|uri| {
            let trimmed = uri.trim().to_string();
            torrent::decode_thunder_uri(&trimmed).unwrap_or(trimmed)
        })
        .filter(|uri| !uri.is_empty())
        .collect();

    if normalized_uris.is_empty() {
        return Err("task.new-task-uris-required".to_string());
    }

    let out_list = outs.unwrap_or_default();
    let base_options = match options {
        Some(Value::Object(map)) => map,
        _ => Map::new(),
    };

    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    let global_options = manager
        .get_global_option()
        .await
        .as_object()
        .cloned()
        .unwrap_or_default();

    let mut distinct_outs = out_list
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .collect::<std::collections::HashSet<_>>();
    if let Some(out) = base_options
        .get("out")
        .and_then(|value| value.as_str())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        distinct_outs.insert(out);
    }
    if normalized_uris.len() >= 2
        && distinct_outs.len() <= 1
        && normalized_uris
            .iter()
            .all(|u| is_plain_http_mirror_uri(u, &base_options))
    {
        let mut task_options = base_options.clone();
        let preferred_out = out_list
            .iter()
            .map(|value| value.trim())
            .find(|value| !value.is_empty())
            .map(|value| value.to_string())
            .or_else(|| {
                task_options
                    .get("out")
                    .and_then(|value| value.as_str())
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or_else(|| infer_out_from_uri_inner(&normalized_uris[0]));
        let temp_out = ensure_temp_download_suffix(&preferred_out);
        if !temp_out.is_empty() {
            task_options.insert("out".to_string(), Value::String(temp_out));
        }
        return match manager
            .add_http_task(normalized_uris.clone(), task_options)
            .await
        {
            Ok(gid) => Ok(Value::Array(vec![Value::Array(vec![Value::String(gid)])])),
            Err(e) => Err(e),
        };
    }

    let mut results = Vec::with_capacity(normalized_uris.len());

    for (index, uri) in normalized_uris.iter().enumerate() {
        let mut task_options = base_options.clone();

        let preferred_out = out_list
            .get(index)
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .or_else(|| {
                task_options
                    .get("out")
                    .and_then(|value| value.as_str())
                    .map(|value| value.trim().to_string())
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or_else(|| infer_out_from_uri_inner(uri));

        let is_media =
            engine::media::is_media_uri(uri) || engine::media::is_force_ytdlp(&task_options);
        let is_m3u8 = engine::m3u8::is_m3u8_uri(uri);
        let legacy_kind = if engine::adc::is_adc_uri(uri) {
            Some(engine::task::TaskKind::Adc)
        } else if engine::gnutella::is_gnutella_uri(uri) {
            Some(engine::task::TaskKind::Gnutella)
        } else if engine::g2::is_g2_uri(uri) {
            Some(engine::task::TaskKind::G2)
        } else if engine::gift::is_gift_uri(uri) {
            Some(engine::task::TaskKind::Gift)
        } else {
            None
        };
        let is_legacy_p2p = legacy_kind.is_some();
        if !is_m3u8 && !is_media && !is_legacy_p2p {
            let temp_out = ensure_temp_download_suffix(&preferred_out);
            if !temp_out.is_empty() {
                task_options.insert("out".to_string(), Value::String(temp_out));
            }
        } else if !preferred_out.is_empty() {
            task_options.insert("out".to_string(), Value::String(preferred_out));
        }

        let result = if torrent::is_magnet_uri(uri) {
            manager.add_magnet_task(uri, task_options).await
        } else if is_nzb_url(uri) {
            let mut fetch_options = global_options.clone();
            fetch_options.extend(task_options.clone());
            match fetch_nzb_url(uri, &fetch_options).await {
                Ok(bytes) => {
                    task_options
                        .entry("usenet-source-name".to_string())
                        .or_insert_with(|| Value::String(nzb_source_name_from_url(uri)));
                    manager.add_nzb_task(bytes, task_options).await
                }
                Err(error) => Err(error),
            }
        } else if is_m3u8 {
            manager.add_m3u8_task(uri, task_options).await
        } else if engine::ed2k::is_ed2k_uri(uri) {
            manager.add_ed2k_task(uri, task_options).await
        } else if engine::ftp::is_ftp_uri(uri) {
            manager.add_ftp_task(uri, task_options).await
        } else if is_media {
            manager.add_media_task(uri, task_options).await
        } else if let Some(kind) = legacy_kind {
            manager.add_legacy_p2p_task(kind, uri, task_options).await
        } else {
            manager.add_http_task(vec![uri.clone()], task_options).await
        };
        match result {
            Ok(gid) => results.push(Value::Array(vec![Value::String(gid)])),
            Err(e) => results.push(json!({"code": 1, "message": e})),
        }
    }

    let mut failed_count = 0usize;
    let mut first_error_message: Option<String> = None;

    for item in &results {
        if let Some(obj) = item.as_object() {
            failed_count += 1;
            if first_error_message.is_none() {
                first_error_message = obj.get("message").and_then(Value::as_str).map(String::from);
            }
        }
    }

    if failed_count > 0 {
        let success_count = results.len().saturating_sub(failed_count);
        if success_count == 0 {
            return Err(first_error_message.unwrap_or_else(|| "task.new-task-fail".to_string()));
        }

        tracing::warn!(
            "[Risuko] add_uri partially failed: {} succeeded, {} failed",
            success_count,
            failed_count
        );
    }

    Ok(Value::Array(results))
}

#[tauri::command]
pub async fn get_media_info(
    _state: tauri::State<'_, crate::state::AppState>,
    url: String,
    options: Option<Value>,
) -> Result<engine::media::MediaInfo, String> {
    let normalized = url.trim().to_string();
    if normalized.is_empty() {
        return Err("URL is required".to_string());
    }

    let task_options = match options {
        Some(Value::Object(map)) => map,
        _ => Map::new(),
    };
    if !engine::media::is_media_uri(&normalized) && !engine::media::is_force_ytdlp(&task_options) {
        return Err("Not a supported media URL".to_string());
    }

    let mut merged_options = match engine::get_manager().await {
        Some(manager) => match manager.get_global_option().await {
            Value::Object(map) => map,
            _ => Map::new(),
        },
        None => Map::new(),
    };
    for (key, value) in task_options {
        merged_options.insert(key, value);
    }

    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        engine::media::get_media_info(&normalized, &merged_options),
    )
    .await
    .map_err(|_| "yt-dlp timed out".to_string())?
}

#[tauri::command]
pub async fn add_media(
    _state: tauri::State<'_, crate::state::AppState>,
    url: String,
    options: Option<Value>,
) -> Result<String, String> {
    let normalized_url = url.trim().to_string();
    if normalized_url.is_empty() {
        return Err("task.new-task-uris-required".to_string());
    }

    let task_options = match options {
        Some(Value::Object(map)) => map,
        _ => Map::new(),
    };
    if !engine::media::is_media_uri(&normalized_url)
        && !engine::media::is_force_ytdlp(&task_options)
    {
        return Err("Not a supported media URL".to_string());
    }

    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.add_media_task(&normalized_url, task_options).await
}

const RESOLVE_MAGNET_TIMEOUT_SECS: u64 = 60;

#[tauri::command]
pub async fn resolve_magnet(
    _state: tauri::State<'_, crate::state::AppState>,
    uri: String,
    options: Option<Value>,
) -> Result<Value, String> {
    let magnet_uri = uri.trim().to_string();
    if !torrent::is_magnet_uri(&magnet_uri) {
        return Err("Not a valid magnet URI".to_string());
    }

    let base_options = match options {
        Some(Value::Object(map)) => map,
        _ => Map::new(),
    };

    let manager = engine::get_manager().await.ok_or("Engine not running")?;

    let files = manager
        .resolve_magnet_metadata(&magnet_uri, base_options, RESOLVE_MAGNET_TIMEOUT_SECS)
        .await?;

    let file_count = files.len();
    let result_files: Vec<Value> = files
        .iter()
        .map(|f| {
            let name = std::path::Path::new(&f.path)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_else(|| f.path.clone());
            json!({
                "path": f.path,
                "length": f.length,
                "name": name,
                "index": f.index + 1,
            })
        })
        .collect();

    Ok(json!({
        "files": result_files,
        "fileCount": file_count,
    }))
}

#[tauri::command]
pub async fn reorder_tasks(
    gids: Vec<String>,
    target_gid: String,
    after: bool,
) -> Result<(), String> {
    if gids.is_empty() {
        return Err("no tasks to reorder".to_string());
    }
    if gids.iter().any(|gid| gid == &target_gid) {
        return Err("target task must not be included in the moved tasks".to_string());
    }
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.move_tasks(&gids, &target_gid, after).await?;
    let _ = manager.save_session().await;
    Ok(())
}

#[tauri::command]
pub async fn set_task_schedule(gid: String, start_at: u64) -> Result<(), String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.set_task_schedule(&gid, start_at).await?;
    let _ = manager.save_session().await;
    Ok(())
}

#[tauri::command]
pub async fn start_task_now(gid: String) -> Result<(), String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.start_task_now(&gid).await?;
    let _ = manager.save_session().await;
    Ok(())
}

#[tauri::command]
pub async fn tell_scheduled(
    offset: Option<i64>,
    num: Option<usize>,
    keys: Option<Vec<String>>,
) -> Result<Value, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    Ok(manager
        .tell_scheduled(
            offset.unwrap_or(0),
            num.unwrap_or(5000),
            &keys.unwrap_or_default(),
        )
        .await)
}

const ENGINE_VERSION: &str = concat!("risuko-engine/", env!("CARGO_PKG_VERSION"));

#[tauri::command]
pub async fn tell_status(gid: String, keys: Option<Vec<String>>) -> Result<Value, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.tell_status(&gid, &keys.unwrap_or_default()).await
}

#[tauri::command]
pub async fn tell_active(keys: Option<Vec<String>>) -> Result<Value, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    Ok(manager.tell_active(&keys.unwrap_or_default()).await)
}

#[tauri::command]
pub async fn tell_waiting(
    offset: Option<i64>,
    num: Option<usize>,
    keys: Option<Vec<String>>,
) -> Result<Value, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    Ok(manager
        .tell_waiting(
            offset.unwrap_or(0),
            num.unwrap_or(5000),
            &keys.unwrap_or_default(),
        )
        .await)
}

#[tauri::command]
pub async fn tell_stopped(
    offset: Option<i64>,
    num: Option<usize>,
    keys: Option<Vec<String>>,
) -> Result<Value, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    Ok(manager
        .tell_stopped(
            offset.unwrap_or(0),
            num.unwrap_or(5000),
            &keys.unwrap_or_default(),
        )
        .await)
}

#[tauri::command]
pub async fn pause_task(gid: String) -> Result<String, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.pause(&gid).await?;
    Ok(gid)
}

#[tauri::command]
pub async fn unpause_task(gid: String) -> Result<String, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.unpause(&gid).await?;
    Ok(gid)
}

#[tauri::command]
pub async fn remove_task(gid: String) -> Result<String, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.remove(&gid).await?;
    Ok(gid)
}

#[tauri::command]
pub async fn change_option(gid: String, options: Value) -> Result<(), String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    let opts = match options {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    manager.change_option(&gid, opts).await
}

#[tauri::command]
pub async fn update_task(gid: String, patch: Value) -> Result<Value, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    let patch: engine::task::TaskPatch =
        serde_json::from_value(patch).map_err(|e| format!("Invalid task patch: {e}"))?;
    let outcome = manager.update_task(&gid, patch).await?;
    serde_json::to_value(outcome).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn change_global_option_engine(options: Value) -> Result<(), String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    let opts = match options {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    manager.change_global_option(opts).await;
    Ok(())
}

#[tauri::command]
pub async fn get_option_engine(gid: String) -> Result<Value, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.get_option(&gid).await
}

#[tauri::command]
pub async fn get_global_stat() -> Result<Value, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    Ok(manager.get_global_stat().await)
}

#[tauri::command]
pub async fn save_session() -> Result<(), String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.save_session().await
}

#[tauri::command]
pub async fn get_version() -> Result<Value, String> {
    Ok(json!({
        "version": ENGINE_VERSION,
        "enabledFeatures": [
            "HTTP",
            "HTTPS",
            "FTP",
            "FTPS",
            "SFTP",
            "BitTorrent",
            "JSON-RPC",
        ]
    }))
}

#[tauri::command]
pub async fn pause_all_tasks() -> Result<(), String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.pause_all().await;
    Ok(())
}

#[tauri::command]
pub async fn unpause_all_tasks() -> Result<(), String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.unpause_all().await;
    Ok(())
}

#[tauri::command]
pub async fn purge_download_result(
    state: tauri::State<'_, crate::state::AppState>,
) -> Result<(), String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.purge_download_result().await;
    let stats_result = state.stats.clear().await;
    let _ = manager.save_session().await;
    stats_result
}

#[tauri::command]
pub async fn remove_download_result(gid: String) -> Result<(), String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    manager.remove_download_result(&gid).await?;
    let _ = manager.save_session().await;
    Ok(())
}

#[tauri::command]
pub async fn get_peers(gid: String) -> Result<Value, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    Ok(manager.get_peers(&gid).await)
}

#[tauri::command]
pub async fn multicall_engine(
    method: String,
    gids: Vec<String>,
    options: Option<Value>,
) -> Result<Value, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    let opts = match options {
        Some(Value::Object(map)) => map,
        _ => Map::new(),
    };

    let mut results: Vec<Value> = Vec::with_capacity(gids.len());
    for gid in &gids {
        let result = match method.as_str() {
            "risuko.changeOption" => manager
                .change_option(gid, opts.clone())
                .await
                .map(|_| Value::String("OK".into())),
            "risuko.remove" => manager
                .remove(gid)
                .await
                .map(|_| Value::String(gid.clone())),
            "risuko.pause" | "risuko.forcePause" => {
                manager.pause(gid).await.map(|_| Value::String(gid.clone()))
            }
            "risuko.unpause" => manager
                .unpause(gid)
                .await
                .map(|_| Value::String(gid.clone())),
            _ => Err(format!("Unsupported multicall method: {}", method)),
        };
        match result {
            Ok(v) => results.push(Value::Array(vec![v])),
            Err(e) => results.push(json!({ "code": 1, "message": e })),
        }
    }
    Ok(Value::Array(results))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use serde_json::json;

    #[test]
    fn decode_thunder_uri_exposes_the_engine_decoder() {
        let encoded = base64::engine::general_purpose::STANDARD.encode(
            "AAmagnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&amp;dn=ExampleZZ",
        );

        assert_eq!(
            decode_thunder_uri(format!("thunder://{encoded}")),
            Some(
                "magnet:?xt=urn:btih:cab507494d02ebb1178b38f2e9d7be299c86b862&dn=Example"
                    .to_string()
            )
        );
        assert_eq!(decode_thunder_uri("https://example.com".to_string()), None);
    }

    #[test]
    fn normalize_non_negative_positive_float() {
        assert_eq!(normalize_non_negative(3.7), 3);
        assert_eq!(normalize_non_negative(1.0), 1);
        assert_eq!(normalize_non_negative(0.9), 0);
    }

    #[test]
    fn normalize_non_negative_zero_and_negative() {
        assert_eq!(normalize_non_negative(0.0), 0);
        assert_eq!(normalize_non_negative(-1.0), 0);
        assert_eq!(normalize_non_negative(-0.0), 0);
    }

    #[test]
    fn normalize_non_negative_special_floats() {
        assert_eq!(normalize_non_negative(f64::NAN), 0);
        assert_eq!(normalize_non_negative(f64::INFINITY), 0);
        assert_eq!(normalize_non_negative(f64::NEG_INFINITY), 0);
    }

    #[test]
    fn normalize_non_negative_large_value() {
        assert_eq!(normalize_non_negative(1e20), u64::MAX);
    }

    #[test]
    fn parse_length_like_u64_number() {
        assert_eq!(parse_length_like(&json!(42)), 42);
        assert_eq!(parse_length_like(&json!(0)), 0);
    }

    #[test]
    fn parse_length_like_negative_number() {
        assert_eq!(parse_length_like(&json!(-5)), 0);
    }

    #[test]
    fn parse_length_like_float_number() {
        assert_eq!(parse_length_like(&json!(3.7)), 3);
    }

    #[test]
    fn parse_length_like_string() {
        assert_eq!(parse_length_like(&json!("100")), 100);
        assert_eq!(parse_length_like(&json!("3.9")), 3);
        assert_eq!(parse_length_like(&json!("0")), 0);
        assert_eq!(parse_length_like(&json!("")), 0);
        assert_eq!(parse_length_like(&json!("abc")), 0);
    }

    #[test]
    fn parse_length_like_bool() {
        assert_eq!(parse_length_like(&json!(true)), 1);
        assert_eq!(parse_length_like(&json!(false)), 0);
    }

    #[test]
    fn parse_length_like_null() {
        assert_eq!(parse_length_like(&json!(null)), 0);
    }

    #[test]
    fn parse_counter_like_normal() {
        assert_eq!(parse_counter_like(&json!(10)), 10);
    }

    #[test]
    fn parse_counter_like_overflow() {
        assert_eq!(parse_counter_like(&json!(u64::MAX)), u32::MAX);
        assert_eq!(parse_counter_like(&json!(u32::MAX as u64 + 1)), u32::MAX);
    }

    #[test]
    fn retry_exponential_backoff() {
        assert_eq!(
            compute_auto_retry_delay_ms("exponential", 2000, 1, 60_000),
            2000
        );
        assert_eq!(
            compute_auto_retry_delay_ms("exponential", 2000, 2, 60_000),
            4000
        );
        assert_eq!(
            compute_auto_retry_delay_ms("exponential", 2000, 3, 60_000),
            8000
        );
    }

    #[test]
    fn retry_exponential_capped_by_max() {
        assert_eq!(
            compute_auto_retry_delay_ms("exponential", 2000, 10, 10_000),
            10_000
        );
    }

    #[test]
    fn retry_static_strategy() {
        assert_eq!(compute_auto_retry_delay_ms("static", 5000, 1, 60_000), 5000);
        assert_eq!(compute_auto_retry_delay_ms("static", 5000, 5, 60_000), 5000);
    }

    #[test]
    fn retry_min_delay_clamp() {
        assert_eq!(compute_auto_retry_delay_ms("static", 100, 1, 60_000), 1000);
    }

    #[test]
    fn infer_out_empty() {
        assert_eq!(infer_out_from_uri_inner(""), "");
        assert_eq!(infer_out_from_uri_inner("   "), "");
    }

    #[test]
    fn infer_out_m3u8_uri() {
        assert_eq!(
            infer_out_from_uri_inner("http://example.com/video.m3u8"),
            "video.ts"
        );
        assert_eq!(
            infer_out_from_uri_inner("http://example.com/video.m3u8?token=abc"),
            "video.ts"
        );
        assert_eq!(
            infer_out_from_uri_inner("http://example.com/video.m3u"),
            "video.ts"
        );
    }

    #[test]
    fn infer_out_http_filename() {
        assert_eq!(
            infer_out_from_uri_inner("http://example.com/path/file.zip"),
            "file.zip"
        );
        assert_eq!(
            infer_out_from_uri_inner("http://example.com/path/file.zip?v=1"),
            "file.zip"
        );
    }

    #[test]
    fn infer_out_no_extension_falls_back_to_download() {
        let r1 = infer_out_from_uri_inner("http://example.com/path/noext");
        assert!(
            r1.starts_with("download-"),
            "expected download-<hex>, got {r1}"
        );
        assert_eq!(r1.len(), "download-".len() + 8);
        let r2 = infer_out_from_uri_inner(
            "https://www.spigotmc.org/resources/storagepeek.134712/download?version=638562",
        );
        assert!(
            r2.starts_with("download-"),
            "expected download-<hex>, got {r2}"
        );
        assert_eq!(r2.len(), "download-".len() + 8);
    }

    #[test]
    fn infer_out_ed2k() {
        let uri = "ed2k://|file|test_file.bin|1024|abc123def456abc123def456abc123de|/";
        let result = infer_out_from_uri_inner(uri);
        assert_eq!(result, "test file.bin");
    }

    #[test]
    fn plain_http_urls_are_mirror_eligible() {
        let opts = Map::new();
        assert!(is_plain_http_mirror_uri(
            "http://example.com/file.zip",
            &opts
        ));
        assert!(is_plain_http_mirror_uri(
            "https://mirror.example.org/path/file.zip",
            &opts
        ));
    }

    #[test]
    fn special_schemes_are_not_mirror_eligible() {
        let opts = Map::new();
        assert!(!is_plain_http_mirror_uri(
            "magnet:?xt=urn:btih:abcdef",
            &opts
        ));
        assert!(!is_plain_http_mirror_uri(
            "ed2k://|file|x|1|0123456789ABCDEF0123456789ABCDEF|/",
            &opts
        ));
        assert!(!is_plain_http_mirror_uri(
            "ftp://example.com/file.zip",
            &opts
        ));
        assert!(!is_plain_http_mirror_uri(
            "https://host/stream/playlist.m3u8",
            &opts
        ));
    }

    #[test]
    fn force_ytdlp_disables_mirror_grouping() {
        let mut opts = Map::new();
        opts.insert("force-ytdlp".to_string(), Value::Bool(true));
        assert!(!is_plain_http_mirror_uri(
            "http://example.com/video.mp4",
            &opts
        ));
    }

    #[test]
    fn nzb_url_route_only_matches_http() {
        assert!(is_nzb_url("http://example.com/file.nzb"));
        assert!(is_nzb_url("https://example.com/file.NZB?token=abc"));
        assert!(!is_nzb_url("ftp://example.com/file.nzb"));
        assert!(!is_nzb_url("nntp://example.com/file.nzb"));
    }

    #[test]
    fn nzb_url_source_name_decodes_the_last_path_segment() {
        assert_eq!(
            nzb_source_name_from_url("https://example.com/path/My%20File.nzb?token=abc"),
            "My File.nzb"
        );
    }

    #[test]
    fn nzb_urls_are_not_grouped_as_http_mirrors() {
        let opts = Map::new();
        assert!(!is_plain_http_mirror_uri(
            "https://one.example/file.nzb",
            &opts
        ));
        assert!(!is_plain_http_mirror_uri(
            "https://two.example/file.nzb?token=x",
            &opts
        ));
    }
}
