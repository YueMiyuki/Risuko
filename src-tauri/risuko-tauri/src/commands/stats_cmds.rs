use serde_json::Value;
use tauri::State;

use risuko_engine::engine::stats::{
    DownloadStatsMinuteInput, DownloadStatsQuery, DownloadStatsView,
};

use crate::state::AppState;

#[tauri::command]
pub async fn record_download_stats_minute(
    state: State<'_, AppState>,
    input: DownloadStatsMinuteInput,
) -> Result<(), String> {
    state.stats.record_minute(input).await
}

#[tauri::command]
pub async fn get_download_stats(
    state: State<'_, AppState>,
    query: DownloadStatsQuery,
) -> Result<DownloadStatsView, String> {
    state.stats.query(query).await
}

#[tauri::command]
pub async fn export_download_stats(state: State<'_, AppState>) -> Result<Value, String> {
    Ok(state.stats.export().await)
}

#[tauri::command]
pub async fn merge_download_stats(state: State<'_, AppState>, data: Value) -> Result<(), String> {
    state.stats.merge(data).await
}
