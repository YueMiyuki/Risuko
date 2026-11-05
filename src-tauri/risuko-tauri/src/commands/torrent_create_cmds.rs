use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use risuko_bt::core::MetaVersion;
use risuko_bt::create::{
    create_torrent as build_torrent, plan, throttled, CreateError, CreateOptions, FileStamp,
    MAX_PIECE_LENGTH, MIN_PIECE_LENGTH,
};
use risuko_engine::engine;
use serde::{Deserialize, Serialize};
use tauri::ipc::Channel;

const PROGRESS_INTERVAL: Duration = Duration::from_millis(150);
const CANCELLED: &str = "task.create-torrent-cancelled";

#[derive(Default)]
pub struct TorrentCreateState {
    jobs: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTorrentRequest {
    pub path: String,
    pub output_dir: String,
    pub version: MetaVersion,
    pub piece_length: Option<u32>,
    #[serde(default)]
    pub trackers: Vec<Vec<String>>,
    #[serde(default)]
    pub web_seeds: Vec<String>,
    pub comment: Option<String>,
    pub source: Option<String>,
    #[serde(default)]
    pub private: bool,
    #[serde(default)]
    pub seed: bool,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTorrentProgress {
    pub hashed: u64,
    pub total: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TorrentSourceInfo {
    pub total_length: u64,
    pub auto_piece_length: u32,
    pub hybrid_padding: BTreeMap<u32, u64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateTorrentResult {
    pub output: String,
    pub name: String,
    pub info_hash: Option<String>,
    pub info_hash_v2: Option<String>,
    pub magnet: String,
    pub piece_length: u32,
    pub total_length: u64,
    pub file_count: usize,
    pub gid: Option<String>,
    pub seed_error: Option<String>,
}

#[tauri::command]
pub async fn create_torrent(
    id: String,
    request: CreateTorrentRequest,
    on_progress: Channel<CreateTorrentProgress>,
    state: tauri::State<'_, TorrentCreateState>,
) -> Result<CreateTorrentResult, String> {
    let cancel = state
        .jobs
        .lock()
        .map_err(|e| e.to_string())?
        .entry(id.clone())
        .or_default()
        .clone();
    let result = create_and_seed(request, on_progress, &cancel).await;
    if let Ok(mut jobs) = state.jobs.lock() {
        jobs.remove(&id);
    }
    result
}

async fn create_and_seed(
    request: CreateTorrentRequest,
    on_progress: Channel<CreateTorrentProgress>,
    cancel: &Arc<AtomicBool>,
) -> Result<CreateTorrentResult, String> {
    let source = PathBuf::from(&request.path);
    let output_dir = PathBuf::from(request.output_dir.trim());
    if !output_dir.is_dir() {
        return Err(format!("{} is not a folder", output_dir.display()));
    }
    let exclude = std::fs::canonicalize(&source)
        .ok()
        .and_then(|path| Some(earlier_outputs(&output_dir, path.file_name()?.to_str()?)))
        .unwrap_or_default();
    let opts = CreateOptions {
        version: request.version,
        piece_length: request.piece_length,
        trackers: request.trackers,
        web_seeds: request.web_seeds,
        comment: request.comment,
        source: request.source,
        private: request.private,
        exclude,
        ..Default::default()
    };

    let hashing = tokio::task::spawn_blocking({
        let cancel = cancel.clone();
        move || {
            build_torrent(
                &source,
                &opts,
                throttled(PROGRESS_INTERVAL, |hashed, total| {
                    let _ = on_progress.send(CreateTorrentProgress { hashed, total });
                }),
                &cancel,
            )
        }
    })
    .await;
    let created = match hashing.map_err(|e| e.to_string())? {
        Ok(_) if cancel.load(Ordering::Relaxed) => return Err(CANCELLED.to_string()),
        Ok(created) => created,
        Err(CreateError::Cancelled) => return Err(CANCELLED.to_string()),
        Err(e) => return Err(e.to_string()),
    };

    let output = unique_output(&output_dir, &created.name);
    tokio::fs::write(&output, &created.bytes)
        .await
        .map_err(|e| format!("{}: {e}", output.display()))?;

    let (gid, seed_error) = if request.seed {
        match seed_in_place(&created.seed_dir, created.bytes, created.stamps).await {
            Ok(gid) => (Some(gid), None),
            Err(e) => (None, Some(e)),
        }
    } else {
        (None, None)
    };

    Ok(CreateTorrentResult {
        output: output.to_string_lossy().into_owned(),
        name: created.name,
        info_hash: created.info_hash_v1.map(|h| h.to_hex()),
        info_hash_v2: created.info_hash_v2.map(|h| h.to_hex()),
        magnet: created.magnet,
        piece_length: created.piece_length,
        total_length: created.total_length,
        file_count: created.file_count,
        gid,
        seed_error,
    })
}

#[tauri::command]
pub async fn inspect_torrent_source(path: String) -> Result<TorrentSourceInfo, String> {
    let planned =
        tokio::task::spawn_blocking(move || plan(Path::new(&path), &CreateOptions::default()))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
    let hybrid_padding = std::iter::successors(Some(MIN_PIECE_LENGTH), |len| len.checked_mul(2))
        .take_while(|len| *len <= MAX_PIECE_LENGTH)
        .map(|len| (len, planned.hybrid_padding_with(len)))
        .collect();
    Ok(TorrentSourceInfo {
        total_length: planned.total_length,
        auto_piece_length: planned.piece_length,
        hybrid_padding,
    })
}

#[tauri::command]
pub fn cancel_create_torrent(id: String, state: tauri::State<'_, TorrentCreateState>) {
    if let Ok(mut jobs) = state.jobs.lock() {
        jobs.entry(id).or_default().store(true, Ordering::Relaxed);
    }
}

async fn seed_in_place(
    dir: &Path,
    torrent: Vec<u8>,
    stamps: Vec<FileStamp>,
) -> Result<String, String> {
    let manager = engine::get_manager().await.ok_or("Engine not running")?;
    let gid = manager
        .seed_created_torrent(torrent, &dir.to_string_lossy(), stamps)
        .await?;
    let status = manager
        .tell_status(&gid, &["status".into(), "errorMessage".into()])
        .await?;
    match status["status"].as_str() {
        Some("error") => Err(status["errorMessage"]
            .as_str()
            .unwrap_or("error")
            .to_string()),
        _ => Ok(gid),
    }
}

fn output_names<'a>(dir: &'a Path, name: &'a str) -> impl Iterator<Item = PathBuf> + 'a {
    std::iter::once(format!("{name}.torrent"))
        .chain((1..).map(move |n| format!("{name} ({n}).torrent")))
        .map(move |file| dir.join(file))
}

fn earlier_outputs(dir: &Path, name: &str) -> Vec<PathBuf> {
    output_names(dir, name)
        .take_while(|path| path.exists())
        .collect()
}

fn unique_output(dir: &Path, name: &str) -> PathBuf {
    output_names(dir, name)
        .find(|path| !path.exists())
        .unwrap_or_else(|| dir.join(format!("{name}.torrent")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn existing_outputs_get_a_number() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(unique_output(dir.path(), "a"), dir.path().join("a.torrent"));
        std::fs::write(dir.path().join("a.torrent"), b"").unwrap();
        std::fs::write(dir.path().join("a (1).torrent"), b"").unwrap();
        assert_eq!(
            unique_output(dir.path(), "a"),
            dir.path().join("a (2).torrent")
        );
        assert_eq!(
            earlier_outputs(dir.path(), "a"),
            [
                dir.path().join("a.torrent"),
                dir.path().join("a (1).torrent")
            ]
        );
        assert!(earlier_outputs(dir.path(), "b").is_empty());
    }
}
