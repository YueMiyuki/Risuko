use std::path::PathBuf;
use std::sync::Arc;

use risuko_share::{FileMeta, SendInfo, ShareManager};

pub struct ShareState {
    manager: Arc<ShareManager>,
}

impl ShareState {
    pub fn new(manager: ShareManager) -> Self {
        Self {
            manager: Arc::new(manager),
        }
    }
}

#[tauri::command]
pub async fn share_start_send(
    id: String,
    paths: Vec<String>,
    state: tauri::State<'_, ShareState>,
) -> Result<SendInfo, String> {
    #[cfg(target_os = "android")]
    crate::commands::android_intent::ensure_ndk_context()?;
    let manager = state.manager.clone();
    let paths = paths.into_iter().map(PathBuf::from).collect();
    manager
        .start_send(id, paths)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn share_start_receive(
    id: String,
    ticket: String,
    dest_dir: String,
    files: Vec<FileMeta>,
    state: tauri::State<'_, ShareState>,
) -> Result<(), String> {
    #[cfg(target_os = "android")]
    crate::commands::android_intent::ensure_ndk_context()?;
    let manager = state.manager.clone();
    manager
        .start_receive(id, ticket, PathBuf::from(dest_dir), files)
        .map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn share_cancel(id: String, state: tauri::State<'_, ShareState>) -> Result<(), String> {
    let manager = state.manager.clone();
    manager.cancel(&id).await;
    Ok(())
}
