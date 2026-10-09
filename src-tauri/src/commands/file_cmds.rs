use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use risuko_bt::bencode;
use serde::Serialize;
use serde_json::Value;
use tauri::AppHandle;

const MAX_TORRENT_PREVIEW_FILES: usize = 2_000;
const MAX_TORRENT_PREVIEW_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_TORRENT_PREVIEW_PAGE_SIZE: usize = 300;
const MAX_TORRENT_PREVIEW_PAGE_SIZE: usize = 2_000;
const TEMP_DOWNLOAD_SUFFIX: &str = ".part";
use crate::engine::CHUNK_META_SUFFIX;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedTorrentFile {
    path: String,
    length: i64,
    name: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedTorrentItem {
    path: String,
    length: i64,
    name: String,
    #[serde(rename = "type")]
    item_type: String,
    has_children: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    select_ranges: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolvedTorrentPayload {
    files: Vec<ResolvedTorrentFile>,
    items: Vec<ResolvedTorrentItem>,
    file_count: usize,
    items_total: usize,
    next_offset: usize,
    preview_disabled: bool,
    preview_reason: String,
}

fn normalize_preview_limit(limit: Option<usize>) -> usize {
    let value = limit.unwrap_or(DEFAULT_TORRENT_PREVIEW_PAGE_SIZE);
    value.clamp(1, MAX_TORRENT_PREVIEW_PAGE_SIZE)
}

fn canonicalize_path(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute() {
        return Err("Path must be absolute".to_string());
    }

    std::fs::canonicalize(path).map_err(|e| e.to_string())
}

fn canonicalize_parent_path(path: &Path) -> Result<PathBuf, String> {
    let parent = path
        .parent()
        .ok_or_else(|| "Path has no parent directory".to_string())?;
    canonicalize_path(parent)
}

fn strip_temp_download_suffix(name: &str) -> Option<String> {
    if name.len() <= TEMP_DOWNLOAD_SUFFIX.len() {
        return None;
    }

    let lower = name.to_ascii_lowercase();
    if !lower.ends_with(TEMP_DOWNLOAD_SUFFIX) {
        return None;
    }

    Some(name[..name.len() - TEMP_DOWNLOAD_SUFFIX.len()].to_string())
}

fn ensure_torrent_extension(path: &Path) -> Result<(), String> {
    if path
        .extension()
        .and_then(|value| value.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("torrent"))
        == Some(true)
    {
        Ok(())
    } else {
        Err("Only .torrent files can be read".to_string())
    }
}

#[tauri::command]
pub fn reveal_in_folder(handle: AppHandle, path: String) -> Result<(), String> {
    #[cfg(target_os = "android")]
    {
        // Hand the raw path to Kotlin. `MainActivity.revealFolder` builds the SAF document URI on the UI thread, tries several intent shapes (chooser/no-chooser × dirmime/no-mime), and returns the first that resolves — it can call `queryIntentActivities` to skip hopeless attempts and catch `ActivityNotFoundException`, both awkward through raw JNI from a Tauri worker thread
        let _ = handle;
        return crate::commands::android_intent::reveal_folder(&path);
    }

    #[cfg(not(target_os = "android"))]
    {
        let p = PathBuf::from(&path);
        if !p.exists() {
            return Err("Path does not exist".to_string());
        }

        let is_dir = p.is_dir();

        #[cfg(target_os = "macos")]
        {
            let _ = handle;
            if is_dir {
                std::process::Command::new("open")
                    .arg(&path)
                    .spawn()
                    .map_err(|e| e.to_string())?;
            } else {
                std::process::Command::new("open")
                    .args(["-R", &path])
                    .spawn()
                    .map_err(|e| e.to_string())?;
            }
        }

        #[cfg(target_os = "windows")]
        {
            use std::os::windows::process::CommandExt;
            let _ = handle;

            // Normalize separators so explorer.exe parses the path reliably
            let normalized_path = path.replace('/', "\\");

            if is_dir {
                // Use ShellExecute via `open` to avoid explorer.exe quirks (e.g. non-zero exit codes, race conditions when an Explorer window is already focused on the same directory)
                open::that(&normalized_path).map_err(|e| e.to_string())?;
            } else {
                // explorer.exe parses its command line manually and expects the form: /select,"<path>". Rust's standard argument escaping mangles the embedded quotes, so use raw_arg to pass the command line through verbatim
                let raw = format!("/select,\"{}\"", normalized_path);
                std::process::Command::new("explorer")
                    .raw_arg(raw)
                    .spawn()
                    .map_err(|e| e.to_string())?;
            }
        }

        #[cfg(target_os = "linux")]
        {
            let _ = handle;
            if is_dir {
                open::that(path).map_err(|e| e.to_string())?;
            } else if let Some(parent) = p.parent() {
                open::that(parent.to_string_lossy().as_ref()).map_err(|e| e.to_string())?;
            } else {
                return Err("Path has no parent directory".to_string());
            }
        }

        Ok(())
    }
}

#[tauri::command]
pub async fn select_android_directory() -> Result<Option<String>, String> {
    #[cfg(target_os = "android")]
    {
        crate::commands::android_intent::pick_directory().await
    }
    #[cfg(not(target_os = "android"))]
    {
        Err("Android directory picker is only available on Android".to_string())
    }
}

#[tauri::command]
pub fn stage_android_share_paths(paths: Vec<String>) -> Result<Vec<String>, String> {
    #[cfg(target_os = "android")]
    {
        paths
            .into_iter()
            .map(|path| crate::commands::android_intent::stage_share_path(&path))
            .collect()
    }
    #[cfg(not(target_os = "android"))]
    {
        let _ = paths;
        Err("Android share staging is only available on Android".to_string())
    }
}

#[tauri::command]
pub fn open_path(handle: AppHandle, path: String) -> Result<(), String> {
    #[cfg(target_os = "android")]
    {
        // URI in, intent out For real file paths, guess the MIME type first so Android shows useful viewers
        let _ = handle;
        let mime = guess_android_mime(&path);
        return crate::commands::android_intent::open_file(&path, &mime);
    }
    #[cfg(not(target_os = "android"))]
    {
        let _ = handle;
        open::that(&path).map_err(|e| e.to_string())
    }
}

/// Map a filename extension to a MIME type Android understands Fall back to `*/*` so unknown files still get a chooser
#[cfg(target_os = "android")]
fn guess_android_mime(path: &str) -> String {
    let ext = std::path::Path::new(path)
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase())
        .unwrap_or_default();
    let mime: &str = match ext.as_str() {
        // Video
        "mp4" | "m4v" => "video/mp4",
        "mkv" => "video/x-matroska",
        "webm" => "video/webm",
        "avi" => "video/x-msvideo",
        "mov" => "video/quicktime",
        "wmv" => "video/x-ms-wmv",
        "flv" => "video/x-flv",
        "ts" => "video/mp2t",
        // Audio
        "mp3" => "audio/mpeg",
        "m4a" | "aac" => "audio/aac",
        "ogg" | "opus" => "audio/ogg",
        "wav" => "audio/wav",
        "flac" => "audio/flac",
        // Image
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "svg" => "image/svg+xml",
        // Documents / archives
        "pdf" => "application/pdf",
        "txt" | "log" | "md" => "text/plain",
        "zip" => "application/zip",
        "tar" => "application/x-tar",
        "gz" | "tgz" => "application/gzip",
        "rar" => "application/vnd.rar",
        "7z" => "application/x-7z-compressed",
        // Subtitles
        "srt" => "application/x-subrip",
        "vtt" => "text/vtt",
        // Fallback for generic handlers
        _ => "*/*",
    };
    mime.to_string()
}

#[tauri::command]
pub fn trash_item(path: String) -> Result<bool, String> {
    let p = std::path::Path::new(&path);
    let existed = p.exists();
    if existed {
        delete_path(p)?;
    } else {
        tracing::debug!("trash_item: path does not exist, skipped: {}", path);
    }
    // Clean up multi-chunk resume sidecar alongside .part file
    if path.ends_with(TEMP_DOWNLOAD_SUFFIX) {
        let _ = std::fs::remove_file(format!("{}{}", path, CHUNK_META_SUFFIX));
    }
    Ok(existed)
}

#[tauri::command]
pub fn rename_path(from_path: String, to_path: String) -> Result<(), String> {
    let from_path = from_path.trim();
    let to_path = to_path.trim();
    if from_path.is_empty() || to_path.is_empty() {
        return Err("Invalid path".to_string());
    }
    if from_path == to_path {
        return Ok(());
    }

    let from = PathBuf::from(from_path);
    let to = PathBuf::from(to_path);
    if !from.is_absolute() || !to.is_absolute() {
        return Err("Path must be absolute".to_string());
    }

    if !from.exists() {
        if to.exists() {
            return Ok(());
        }
        return Err("Source path does not exist".to_string());
    }

    // Limit rename_path to in-place temporary suffix finalization to avoid arbitrary moves
    let from_parent = canonicalize_parent_path(&from)?;
    let to_parent = canonicalize_parent_path(&to)?;
    if from_parent != to_parent {
        return Err("Cross-directory rename is not allowed".to_string());
    }
    let from_name = from
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| "Invalid source path".to_string())?;
    let to_name = to
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| "Invalid target path".to_string())?;
    let expected_to_name = strip_temp_download_suffix(from_name)
        .ok_or_else(|| "Only temporary download files can be renamed".to_string())?;
    if expected_to_name != to_name {
        return Err("Invalid rename target".to_string());
    }

    std::fs::rename(from, to).map_err(|e| e.to_string())
}

fn resolve_torrent_fs_path(path: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(path);
    ensure_torrent_extension(&path)?;
    canonicalize_path(&path).map_err(|_| "Path does not exist".to_string())
}

fn as_string(value: &bencode::Value) -> String {
    match value {
        bencode::Value::Bytes(bytes) => String::from_utf8_lossy(bytes).to_string(),
        bencode::Value::Int(value) => value.to_string(),
        _ => String::new(),
    }
}

fn as_length(value: Option<&bencode::Value>) -> i64 {
    match value.and_then(bencode::Value::as_int) {
        Some(value) if value > 0 => value,
        _ => 0,
    }
}

/// BEP 47 padding entry, hidden like the engine does so `select-file` indices match
fn is_padding_entry(item: &bencode::Value) -> bool {
    item.get(b"attr")
        .and_then(bencode::Value::as_bytes)
        .is_some_and(|attr| attr.contains(&b'p'))
}

fn dict_get_first<'a>(dict: &'a bencode::Value, keys: &[&[u8]]) -> Option<&'a bencode::Value> {
    keys.iter().find_map(|key| dict.get(key))
}

fn normalize_torrent_path(path: &str) -> String {
    path.replace('\\', "/").trim_start_matches('/').to_string()
}

pub(crate) fn inspect_torrent_metadata(
    bytes: &[u8],
    fallback: &str,
) -> Result<(bool, String), String> {
    if bytes.is_empty() {
        return Err("Torrent payload is empty".to_string());
    }

    let root = bencode::decode(bytes)
        .map_err(|_| "Invalid torrent metadata".to_string())?
        .value;
    let info = root
        .get(b"info")
        .filter(|value| value.as_dict().is_some())
        .ok_or_else(|| "Invalid torrent metadata".to_string())?;

    let is_multi_file = matches!(
        info.get(b"files").and_then(bencode::Value::as_list),
        Some(files) if !files.is_empty()
    );

    let name = dict_get_first(info, &[b"name.utf-8", b"name"])
        .map(as_string)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback.to_string());
    Ok((is_multi_file, name))
}

fn split_torrent_path_segments(path: &str) -> Vec<String> {
    normalize_torrent_path(path)
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(|segment| segment.to_string())
        .collect()
}

fn normalize_parent_segments(parent_path: Option<&str>, normalized_root_name: &str) -> Vec<String> {
    let raw = normalize_torrent_path(parent_path.unwrap_or(""));
    if raw.is_empty() || raw == normalized_root_name {
        return Vec::new();
    }

    let relative = if !normalized_root_name.is_empty() {
        let prefix = format!("{normalized_root_name}/");
        if raw.starts_with(&prefix) {
            raw[prefix.len()..].to_string()
        } else {
            raw
        }
    } else {
        raw
    };

    split_torrent_path_segments(&relative)
}

fn push_index_to_ranges(ranges: &mut Vec<(usize, usize)>, index: usize) {
    if index == 0 {
        return;
    }

    if let Some((_, end)) = ranges.last_mut() {
        if index <= end.saturating_add(1) {
            if index > *end {
                *end = index;
            }
            return;
        }
    }

    ranges.push((index, index));
}

fn encode_index_ranges(ranges: &[(usize, usize)]) -> Option<String> {
    if ranges.is_empty() {
        return None;
    }

    let encoded = ranges
        .iter()
        .map(|(start, end)| {
            if start == end {
                format!("{start}")
            } else {
                format!("{start}-{end}")
            }
        })
        .collect::<Vec<_>>()
        .join(",");

    if encoded.is_empty() {
        None
    } else {
        Some(encoded)
    }
}

fn collect_direct_children(
    raw_files: &[&bencode::Value],
    normalized_root_name: &str,
    parent_segments: &[String],
) -> Vec<ResolvedTorrentItem> {
    let mut folder_items: BTreeMap<String, ResolvedTorrentItem> = BTreeMap::new();
    let mut file_items: BTreeMap<String, ResolvedTorrentItem> = BTreeMap::new();
    let mut folder_index_ranges: BTreeMap<String, Vec<(usize, usize)>> = BTreeMap::new();

    for (file_index, item) in raw_files.iter().enumerate() {
        let segments = dict_get_first(item, &[b"path.utf-8", b"path"])
            .and_then(bencode::Value::as_list)
            .map(|parts| {
                parts
                    .iter()
                    .map(as_string)
                    .filter(|part| !part.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if segments.is_empty() {
            continue;
        }

        if parent_segments.len() > segments.len() {
            continue;
        }
        let is_child_of_parent = parent_segments
            .iter()
            .zip(segments.iter())
            .all(|(parent, child)| parent == child);
        if !is_child_of_parent {
            continue;
        }

        let remaining = &segments[parent_segments.len()..];
        if remaining.is_empty() {
            continue;
        }

        let child_name = remaining[0].clone();
        let mut full_path_segments: Vec<String> = Vec::new();
        if !normalized_root_name.is_empty() {
            full_path_segments.push(normalized_root_name.to_string());
        }
        full_path_segments.extend(parent_segments.iter().cloned());
        full_path_segments.push(child_name.clone());
        let full_path = normalize_torrent_path(&full_path_segments.join("/"));
        if full_path.is_empty() {
            continue;
        }

        if remaining.len() == 1 {
            let length = as_length(item.get(b"length"));
            file_items
                .entry(full_path.clone())
                .or_insert_with(|| ResolvedTorrentItem {
                    path: full_path,
                    length,
                    name: child_name,
                    item_type: "file".to_string(),
                    has_children: false,
                    index: Some(file_index + 1),
                    select_ranges: None,
                });
        } else {
            folder_items
                .entry(full_path.clone())
                .or_insert_with(|| ResolvedTorrentItem {
                    path: full_path.clone(),
                    length: 0,
                    name: child_name,
                    item_type: "folder".to_string(),
                    has_children: true,
                    index: None,
                    select_ranges: None,
                });
            let ranges = folder_index_ranges.entry(full_path.clone()).or_default();
            push_index_to_ranges(ranges, file_index + 1);
        }
    }

    let mut items = folder_items
        .into_iter()
        .map(|(path, mut item)| {
            item.select_ranges = folder_index_ranges
                .get(&path)
                .and_then(|ranges| encode_index_ranges(ranges.as_slice()));
            item
        })
        .collect::<Vec<_>>();
    items.extend(file_items.into_values());
    items.sort_by(|a, b| {
        let a_folder = a.item_type == "folder";
        let b_folder = b.item_type == "folder";
        if a_folder != b_folder {
            if a_folder {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        } else {
            a.name
                .to_ascii_lowercase()
                .cmp(&b.name.to_ascii_lowercase())
                .then_with(|| a.path.cmp(&b.path))
        }
    });
    items
}

fn resolve_torrent_from_bytes(
    bytes: &[u8],
    file_name: &str,
    force_preview: bool,
    parent_path: Option<&str>,
    offset: usize,
    limit: usize,
) -> Result<ResolvedTorrentPayload, String> {
    if bytes.is_empty() {
        return Err("Torrent payload is empty".to_string());
    }

    if !force_preview && bytes.len() > MAX_TORRENT_PREVIEW_BYTES {
        return Ok(ResolvedTorrentPayload {
            files: Vec::new(),
            items: Vec::new(),
            file_count: 0,
            items_total: 0,
            next_offset: 0,
            preview_disabled: true,
            preview_reason: "size".to_string(),
        });
    }

    let root = bencode::decode(bytes)
        .map_err(|_| "Invalid torrent metadata".to_string())?
        .value;
    let info = root
        .get(b"info")
        .filter(|value| value.as_dict().is_some())
        .ok_or_else(|| "Invalid torrent metadata".to_string())?;

    let root_name = dict_get_first(info, &[b"name.utf-8", b"name"])
        .map(as_string)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| file_name.to_string());
    let normalized_root_name = normalize_torrent_path(&root_name);

    if let Some(files_value) = info.get(b"files") {
        if let Some(raw_files) = files_value.as_list() {
            let visible_files: Vec<&bencode::Value> = raw_files
                .iter()
                .filter(|item| !is_padding_entry(item))
                .collect();
            let raw_files = visible_files.as_slice();
            let file_count = raw_files.len();
            if !force_preview && file_count > MAX_TORRENT_PREVIEW_FILES {
                return Ok(ResolvedTorrentPayload {
                    files: Vec::new(),
                    items: Vec::new(),
                    file_count,
                    items_total: 0,
                    next_offset: 0,
                    preview_disabled: true,
                    preview_reason: "count".to_string(),
                });
            }

            if force_preview {
                let parent_segments = normalize_parent_segments(parent_path, &normalized_root_name);
                let items =
                    collect_direct_children(raw_files, &normalized_root_name, &parent_segments);
                let items_total = items.len();
                let safe_offset = offset.min(items_total);
                let paged_items = items
                    .into_iter()
                    .skip(safe_offset)
                    .take(limit)
                    .collect::<Vec<_>>();
                let next_offset = safe_offset + paged_items.len();
                return Ok(ResolvedTorrentPayload {
                    files: Vec::new(),
                    items: paged_items,
                    file_count,
                    items_total,
                    next_offset,
                    preview_disabled: false,
                    preview_reason: String::new(),
                });
            }

            let mut files = Vec::with_capacity(file_count);
            for item in raw_files {
                if item.as_dict().is_none() {
                    continue;
                }

                let length = as_length(item.get(b"length"));
                let segments = dict_get_first(item, &[b"path.utf-8", b"path"])
                    .and_then(bencode::Value::as_list)
                    .map(|parts| {
                        parts
                            .iter()
                            .map(as_string)
                            .filter(|part| !part.is_empty())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                let relative_path = normalize_torrent_path(&segments.join("/"));
                let full_path = if relative_path.is_empty() {
                    normalized_root_name.clone()
                } else {
                    normalize_torrent_path(&format!("{}/{}", normalized_root_name, relative_path))
                };
                if full_path.is_empty() {
                    continue;
                }

                let name = segments
                    .last()
                    .cloned()
                    .unwrap_or_else(|| root_name.clone());
                files.push(ResolvedTorrentFile {
                    path: full_path,
                    length,
                    name,
                });
            }

            return Ok(ResolvedTorrentPayload {
                files,
                items: Vec::new(),
                file_count,
                items_total: 0,
                next_offset: 0,
                preview_disabled: false,
                preview_reason: String::new(),
            });
        }
    }

    let single_name = if normalized_root_name.is_empty() {
        file_name.to_string()
    } else {
        root_name
    };
    let single_path = normalize_torrent_path(&single_name);
    let length = as_length(info.get(b"length"));

    if force_preview {
        let parent_segments = normalize_parent_segments(parent_path, &normalized_root_name);
        let items = if parent_segments.is_empty() {
            vec![ResolvedTorrentItem {
                path: single_path.clone(),
                length,
                name: single_name.clone(),
                item_type: "file".to_string(),
                has_children: false,
                index: Some(1),
                select_ranges: None,
            }]
        } else {
            Vec::new()
        };
        let items_total = items.len();
        let safe_offset = offset.min(items_total);
        let paged_items = items
            .into_iter()
            .skip(safe_offset)
            .take(limit)
            .collect::<Vec<_>>();
        let next_offset = safe_offset + paged_items.len();
        return Ok(ResolvedTorrentPayload {
            files: Vec::new(),
            items: paged_items,
            file_count: 1,
            items_total,
            next_offset,
            preview_disabled: false,
            preview_reason: String::new(),
        });
    }

    Ok(ResolvedTorrentPayload {
        files: vec![ResolvedTorrentFile {
            path: single_path.clone(),
            length,
            name: single_name,
        }],
        items: Vec::new(),
        file_count: 1,
        items_total: 0,
        next_offset: 0,
        preview_disabled: false,
        preview_reason: String::new(),
    })
}

#[tauri::command]
pub fn resolve_torrent_path(
    path: String,
    file_name: Option<String>,
    force_preview: Option<bool>,
    parent_path: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
) -> Result<ResolvedTorrentPayload, String> {
    let resolved_path = resolve_torrent_fs_path(&path)?;
    let bytes = std::fs::read(&resolved_path).map_err(|e| e.to_string())?;
    let fallback_name = file_name
        .map(|name| name.trim().to_string())
        .filter(|name| !name.is_empty())
        .or_else(|| {
            resolved_path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.to_string())
        })
        .unwrap_or_else(|| "task.torrent".to_string());

    resolve_torrent_from_bytes(
        &bytes,
        &fallback_name,
        force_preview.unwrap_or(false),
        parent_path.as_deref(),
        offset.unwrap_or(0),
        normalize_preview_limit(limit),
    )
}

fn normalize_info_hash(raw: &str) -> String {
    fn decode_base32_btih_to_hex(input: &str) -> Option<String> {
        let normalized = input.trim().to_ascii_uppercase();
        if normalized.len() != 32 {
            return None;
        }

        let mut acc: u64 = 0;
        let mut bits: u8 = 0;
        let mut bytes: Vec<u8> = Vec::with_capacity(20);

        for ch in normalized.chars() {
            let value: u8 = match ch {
                'A'..='Z' => (ch as u8) - b'A',
                '2'..='7' => 26 + (ch as u8 - b'2'),
                _ => return None,
            };

            acc = (acc << 5) | value as u64;
            bits += 5;
            while bits >= 8 {
                let shift = bits - 8;
                let byte = ((acc >> shift) & 0xFF) as u8;
                bytes.push(byte);
                bits -= 8;
                if bits > 0 {
                    acc &= (1u64 << bits) - 1;
                } else {
                    acc = 0;
                }
            }
        }

        if bytes.len() != 20 {
            return None;
        }

        Some(bytes_to_lower_hex(&bytes))
    }

    let value = raw.trim();
    const URN_BTIH_PREFIX: &str = "urn:btih:";
    let stripped = if value
        .get(..URN_BTIH_PREFIX.len())
        .map(|prefix| prefix.eq_ignore_ascii_case(URN_BTIH_PREFIX))
        .unwrap_or(false)
    {
        value.get(URN_BTIH_PREFIX.len()..).unwrap_or("").trim()
    } else {
        value
    };

    let normalized_hex = stripped.to_ascii_lowercase();
    if (normalized_hex.len() == 40 || normalized_hex.len() == 64)
        && normalized_hex.chars().all(|c| c.is_ascii_hexdigit())
    {
        return normalized_hex;
    }

    if let Some(decoded) = decode_base32_btih_to_hex(stripped) {
        return decoded;
    }

    normalized_hex
        .chars()
        .filter(|c| c.is_ascii_hexdigit())
        .collect()
}

fn generated_torrent_hex_stem(file_name: &str) -> Option<String> {
    let lower = file_name.to_ascii_lowercase();
    if !lower.ends_with(".torrent") {
        return None;
    }

    let stem = lower.strip_suffix(".torrent")?;
    let stem = stem.strip_prefix("[metadata]").unwrap_or(stem);
    let is_hex = stem.chars().all(|c| c.is_ascii_hexdigit());
    if !is_hex {
        return None;
    }
    if stem.len() != 40 && stem.len() != 64 {
        return None;
    }

    Some(stem.to_string())
}

fn bytes_to_lower_hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut encoded, "{:02x}", byte);
    }
    encoded
}

fn delete_file_best_effort(path: &Path) -> bool {
    delete_path(path).is_ok() || std::fs::remove_file(path).is_ok()
}

#[cfg(target_os = "android")]
fn delete_path(path: &Path) -> Result<(), String> {
    if path.is_dir() {
        std::fs::remove_dir_all(path).map_err(|e| e.to_string())
    } else {
        std::fs::remove_file(path).map_err(|e| e.to_string())
    }
}

#[cfg(not(target_os = "android"))]
fn delete_path(path: &Path) -> Result<(), String> {
    trash::delete(path).map_err(|e| e.to_string())
}

fn extract_btih_token(input: &str) -> Option<String> {
    let lower = input.to_ascii_lowercase();
    let marker = "urn:btih:";
    let start = lower.find(marker)? + marker.len();
    let tail = &input[start..];
    let token: String = tail
        .chars()
        .take_while(|ch| ch.is_ascii_alphanumeric())
        .collect();
    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

pub(crate) fn percent_decode_lossy(input: &str) -> String {
    percent_encoding::percent_decode(input.as_bytes())
        .decode_utf8_lossy()
        .into_owned()
}

pub(crate) fn percent_decode_strict(input: &str) -> String {
    percent_encoding::percent_decode(input.as_bytes())
        .decode_utf8()
        .map(|s| s.into_owned())
        .unwrap_or_default()
}

fn valid_normalized_info_hash(raw: &str) -> Option<String> {
    let normalized = normalize_info_hash(raw);
    if normalized.len() == 40 || normalized.len() == 64 {
        Some(normalized)
    } else {
        None
    }
}

fn resolve_task_info_hash(task: &Value) -> Option<String> {
    if let Some(value) = task.get("infoHash").and_then(Value::as_str) {
        if let Some(normalized) = valid_normalized_info_hash(value) {
            return Some(normalized);
        }
    }

    if let Some(value) = task
        .get("bittorrent")
        .and_then(Value::as_object)
        .and_then(|bt| bt.get("infoHash"))
        .and_then(Value::as_str)
    {
        if let Some(normalized) = valid_normalized_info_hash(value) {
            return Some(normalized);
        }
    }

    let files = task.get("files").and_then(Value::as_array)?;

    for file in files {
        let Some(uris) = file.get("uris").and_then(Value::as_array) else {
            continue;
        };

        for uri in uris {
            let Some(raw_uri) = uri.get("uri").and_then(Value::as_str) else {
                continue;
            };
            let raw_uri = raw_uri.trim();
            if raw_uri.is_empty() {
                continue;
            }

            let token = extract_btih_token(raw_uri)
                .or_else(|| extract_btih_token(&percent_decode_lossy(raw_uri)));
            if let Some(token) = token {
                if let Some(normalized) = valid_normalized_info_hash(&token) {
                    return Some(normalized);
                }
            }
        }
    }

    None
}

fn push_candidate_dir(
    dirs: &mut Vec<String>,
    seen: &mut BTreeSet<String>,
    candidate: Option<&str>,
) {
    let candidate = candidate.unwrap_or("").trim();
    if candidate.is_empty() {
        return;
    }

    if seen.insert(candidate.to_string()) {
        dirs.push(candidate.to_string());
    }
}

fn resolve_task_candidate_dirs(task: &Value) -> Vec<String> {
    let mut dirs = Vec::new();
    let mut seen = BTreeSet::new();

    push_candidate_dir(
        &mut dirs,
        &mut seen,
        task.get("dir").and_then(Value::as_str),
    );

    if let Some(files) = task.get("files").and_then(Value::as_array) {
        for file in files {
            let Some(path) = file.get("path").and_then(Value::as_str) else {
                continue;
            };
            let path = path.trim();
            if path.is_empty() {
                continue;
            }

            let parent = Path::new(path).parent().and_then(|value| value.to_str());
            push_candidate_dir(&mut dirs, &mut seen, parent);
        }
    }

    dirs
}

fn trash_generated_torrent_sidecars_in_dir(dir: &Path, normalized_info_hash: Option<&str>) -> u32 {
    // Without a target info-hash we cannot tell this task's generated sidecar apart from unrelated .torrent files, so match nothing
    let Some(hash) = normalized_info_hash else {
        return 0;
    };

    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return 0,
    };

    let mut deleted = 0u32;

    for entry in entries {
        let Ok(entry) = entry else {
            continue;
        };

        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !file_name.to_ascii_lowercase().ends_with(".torrent") {
            continue;
        }
        let matched = generated_torrent_hex_stem(file_name).as_deref() == Some(hash);

        if matched && delete_file_best_effort(&path) {
            deleted += 1;
        }
    }

    deleted
}

#[tauri::command]
pub async fn cleanup_generated_torrent_sidecars_for_task(task: Value) -> Result<u32, String> {
    const RETRY_DELAYS_MS: [u64; 3] = [0, 250, 500];

    let dirs = resolve_task_candidate_dirs(&task);
    if dirs.is_empty() {
        return Ok(0);
    }

    let normalized_info_hash = resolve_task_info_hash(&task);
    let mut total_deleted = 0u32;

    for delay_ms in RETRY_DELAYS_MS {
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }

        let mut deleted = 0u32;
        for dir in &dirs {
            let path = PathBuf::from(dir);
            let Ok(path) = canonicalize_path(&path) else {
                continue;
            };
            if !path.is_dir() {
                continue;
            }

            deleted +=
                trash_generated_torrent_sidecars_in_dir(&path, normalized_info_hash.as_deref());
        }

        total_deleted = total_deleted.saturating_add(deleted);

        // The retries exist to catch a sidecar the engine writes slightly after task removal; once we've deleted one there's nothing left to wait for, so stop early instead of re-scanning and sleeping needlessly
        if deleted > 0 {
            break;
        }
    }

    Ok(total_deleted)
}

#[cfg(test)]
mod tests {
    use sha1::{Digest, Sha1};

    use super::*;

    // -- strip_temp_download_suffix --

    #[test]
    fn strip_suffix_removes_part() {
        assert_eq!(
            strip_temp_download_suffix("file.zip.part"),
            Some("file.zip".to_string())
        );
    }

    #[test]
    fn strip_suffix_case_insensitive() {
        assert_eq!(
            strip_temp_download_suffix("file.zip.PART"),
            Some("file.zip".to_string())
        );
    }

    #[test]
    fn strip_suffix_no_match() {
        assert_eq!(strip_temp_download_suffix("file.zip"), None);
    }

    #[test]
    fn strip_suffix_too_short() {
        assert_eq!(strip_temp_download_suffix(".part"), None);
        assert_eq!(strip_temp_download_suffix("a.par"), None);
    }

    // -- push_index_to_ranges --

    #[test]
    fn push_index_ignores_zero() {
        let mut ranges = Vec::new();
        push_index_to_ranges(&mut ranges, 0);
        assert!(ranges.is_empty());
    }

    #[test]
    fn push_index_starts_new_range() {
        let mut ranges = Vec::new();
        push_index_to_ranges(&mut ranges, 3);
        assert_eq!(ranges, vec![(3, 3)]);
    }

    #[test]
    fn push_index_extends_sequential() {
        let mut ranges = vec![(1, 3)];
        push_index_to_ranges(&mut ranges, 4);
        assert_eq!(ranges, vec![(1, 4)]);
    }

    #[test]
    fn push_index_gap_creates_new() {
        let mut ranges = vec![(1, 3)];
        push_index_to_ranges(&mut ranges, 5);
        assert_eq!(ranges, vec![(1, 3), (5, 5)]);
    }

    // -- padding files --

    #[test]
    fn torrent_preview_hides_padding_files() {
        use bencode::Value;
        let file = |path: &[u8], attr: Option<&[u8]>| {
            let mut entry = Vec::new();
            if let Some(attr) = attr {
                entry.push((b"attr".to_vec(), Value::Bytes(attr.to_vec())));
            }
            entry.push((b"length".to_vec(), Value::Int(4)));
            entry.push((
                b"path".to_vec(),
                Value::List(vec![Value::Bytes(path.to_vec())]),
            ));
            Value::Dict(entry)
        };
        let torrent = bencode::encode_to_vec(&Value::Dict(vec![(
            b"info".to_vec(),
            Value::Dict(vec![
                (
                    b"files".to_vec(),
                    Value::List(vec![
                        file(b"a.txt", None),
                        file(b"pad0", Some(b"p")),
                        file(b"b.txt", Some(b"x")),
                    ]),
                ),
                (b"name".to_vec(), Value::Bytes(b"root".to_vec())),
            ]),
        )]));
        let listed = resolve_torrent_from_bytes(&torrent, "t.torrent", false, None, 0, 50).unwrap();
        assert_eq!(listed.file_count, 2);
        let names: Vec<&str> = listed.files.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["a.txt", "b.txt"]);

        // Indices count visible files, as `select-file` does
        let paged = resolve_torrent_from_bytes(&torrent, "t.torrent", true, None, 0, 50).unwrap();
        let mut indices: Vec<(String, Option<usize>)> = paged
            .items
            .iter()
            .map(|item| (item.name.clone(), item.index))
            .collect();
        indices.sort();
        assert_eq!(
            indices,
            vec![
                ("a.txt".to_string(), Some(1)),
                ("b.txt".to_string(), Some(2))
            ]
        );
    }

    // -- encode_index_ranges --

    #[test]
    fn encode_ranges_empty() {
        assert_eq!(encode_index_ranges(&[]), None);
    }

    #[test]
    fn encode_ranges_single_value() {
        assert_eq!(encode_index_ranges(&[(5, 5)]), Some("5".to_string()));
    }

    #[test]
    fn encode_ranges_single_range() {
        assert_eq!(encode_index_ranges(&[(1, 5)]), Some("1-5".to_string()));
    }

    #[test]
    fn encode_ranges_mixed() {
        let ranges = [(1, 3), (5, 5), (7, 9)];
        assert_eq!(encode_index_ranges(&ranges), Some("1-3,5,7-9".to_string()));
    }

    // -- normalize_torrent_path --

    #[test]
    fn normalize_path_backslashes() {
        assert_eq!(
            normalize_torrent_path("folder\\file.txt"),
            "folder/file.txt"
        );
    }

    #[test]
    fn normalize_path_leading_slash() {
        assert_eq!(
            normalize_torrent_path("/folder/file.txt"),
            "folder/file.txt"
        );
    }

    #[test]
    fn normalize_path_already_clean() {
        assert_eq!(normalize_torrent_path("folder/file.txt"), "folder/file.txt");
    }

    // -- normalize_info_hash --

    #[test]
    fn info_hash_sha1_hex() {
        let hex = "aabbccddee11223344556677889900aabbccddee";
        assert_eq!(normalize_info_hash(hex), hex);
    }

    #[test]
    fn info_hash_sha1_uppercase() {
        let hex = "AABBCCDDEE11223344556677889900AABBCCDDEE";
        assert_eq!(normalize_info_hash(hex), hex.to_ascii_lowercase());
    }

    #[test]
    fn info_hash_urn_btih_prefix() {
        let hash = "aabbccddee11223344556677889900aabbccddee";
        let input = format!("urn:btih:{}", hash);
        assert_eq!(normalize_info_hash(&input), hash);
    }

    #[test]
    fn info_hash_base32_decode() {
        // 20 bytes of 0x61 ("aaaa...") = base32 "MFQWCYLBMFQWCYLBMFQWCYLBMFQWCYLB"
        let base32 = "MFQWCYLBMFQWCYLBMFQWCYLBMFQWCYLB";
        let expected = "6161616161616161616161616161616161616161";
        assert_eq!(normalize_info_hash(base32), expected);
    }

    // -- percent_decode_lossy --

    #[test]
    fn decode_space() {
        assert_eq!(percent_decode_lossy("%20"), " ");
    }

    #[test]
    fn decode_utf8_multibyte() {
        // "中" is U+4E2D, UTF-8: E4 B8 AD
        assert_eq!(percent_decode_lossy("%E4%B8%AD"), "中");
    }

    #[test]
    fn decode_invalid_sequence() {
        assert_eq!(percent_decode_lossy("%ZZ"), "%ZZ");
    }

    #[test]
    fn decode_passthrough() {
        assert_eq!(percent_decode_lossy("hello"), "hello");
    }

    #[test]
    fn decode_strict_rejects_invalid_utf8() {
        // %FF%FE is not valid UTF-8 -> strict decode yields "" (old urlencoding::decode(..).unwrap_or_default() behaviour)
        assert_eq!(percent_decode_strict("%FF%FE"), "");
        assert_eq!(percent_decode_strict("%E4%B8%AD"), "中");
    }

    // -- inspect_torrent_metadata --

    #[test]
    fn inspect_empty_bytes() {
        assert!(inspect_torrent_metadata(b"", "fallback").is_err());
    }

    #[test]
    fn inspect_single_file_torrent() {
        let bytes = b"d4:infod6:lengthi1024e4:name8:test.binee";
        let (is_multi, name) = inspect_torrent_metadata(bytes, "fallback").unwrap();
        assert!(!is_multi);
        assert_eq!(name, "test.bin");
    }

    #[test]
    fn inspect_multi_file_torrent() {
        let bytes = b"d4:infod5:filesld6:lengthi512e4:pathl5:a.bineee4:name4:testee";
        let (is_multi, name) = inspect_torrent_metadata(bytes, "fallback").unwrap();
        assert!(is_multi);
        assert_eq!(name, "test");
    }

    #[test]
    fn inspect_fallback_name() {
        // Torrent with info dict but no name key
        let bytes = b"d4:infod6:lengthi100eee";
        let (_, name) = inspect_torrent_metadata(bytes, "my_fallback").unwrap();
        assert_eq!(name, "my_fallback");
    }

    #[test]
    fn cleanup_removes_only_hex_stem_sidecar() {
        let dir = tempfile::tempdir().unwrap();

        let info_dict = b"d6:lengthi123e4:name9:test.filee";
        let hash = bytes_to_lower_hex(Sha1::digest(info_dict).as_ref());
        let mut torrent = Vec::from(*b"d4:info");
        torrent.extend_from_slice(info_dict);
        torrent.push(b'e');
        let user_file = dir.path().join("Some Movie (2026).torrent");
        std::fs::write(&user_file, &torrent).unwrap();

        let sidecar = dir.path().join(format!("{hash}.torrent"));
        std::fs::write(&sidecar, &torrent).unwrap();

        let other = dir.path().join(format!("{}.torrent", "a".repeat(40)));
        std::fs::write(&other, &torrent).unwrap();

        let deleted = trash_generated_torrent_sidecars_in_dir(dir.path(), Some(&hash));
        assert_eq!(deleted, 1);
        assert!(!sidecar.exists(), "hex-stem sidecar should be removed");
        assert!(
            user_file.exists(),
            "user's display-named .torrent must survive even with a matching info-hash"
        );
        assert!(
            other.exists(),
            "hex-stem file of a different torrent must be left alone"
        );
    }

    #[test]
    fn cleanup_without_info_hash_matches_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("anything.torrent");
        std::fs::write(&path, b"d4:infod6:lengthi1eee").unwrap();
        assert_eq!(trash_generated_torrent_sidecars_in_dir(dir.path(), None), 0);
        assert!(path.exists());
    }
}
