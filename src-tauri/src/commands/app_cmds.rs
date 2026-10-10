use std::sync::atomic::Ordering;
use tauri::{AppHandle, Manager};

pub use crate::utils::run_mode::{current_run_mode, update_macos_activation_policy_for_mode};

pub fn show_main_window(handle: &AppHandle) -> Result<(), String> {
    update_macos_activation_policy_for_mode(handle, crate::utils::run_mode::RUN_MODE_STANDARD);
    if let Some(window) = handle.get_webview_window("main") {
        window.show().map_err(|e| e.to_string())?;
        window.set_focus().map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub fn hide_main_window(handle: &AppHandle) -> Result<(), String> {
    let run_mode = current_run_mode(handle);
    if let Some(window) = handle.get_webview_window("main") {
        window.hide().map_err(|e| e.to_string())?;
    }
    update_macos_activation_policy_for_mode(handle, run_mode);
    Ok(())
}

#[tauri::command]
pub async fn quit_app(handle: AppHandle) -> Result<(), String> {
    handle
        .state::<crate::state::AppState>()
        .is_quitting
        .store(true, Ordering::SeqCst);
    risuko_engine::engine::stop_engine()
        .await
        .map_err(|e| e.to_string())?;
    handle.exit(0);
    Ok(())
}

#[tauri::command]
pub async fn relaunch_app(handle: AppHandle) -> Result<(), String> {
    let state = handle.state::<crate::state::AppState>();
    state.is_quitting.store(true, Ordering::SeqCst);
    let stop = risuko_engine::engine::stop_engine();
    match tokio::time::timeout(std::time::Duration::from_secs(15), stop).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            state.is_quitting.store(false, Ordering::SeqCst);
            return Err(e.to_string());
        }
        Err(_) => tracing::warn!("Timed out stopping engine before relaunch"),
    }
    handle.restart()
}

#[tauri::command]
pub fn show_window(handle: AppHandle) -> Result<(), String> {
    show_main_window(&handle)
}

#[tauri::command]
pub fn log_frontend(level: String, message: String) {
    match level.as_str() {
        "error" => tracing::error!("[renderer] {}", message),
        "warn" => tracing::warn!("[renderer] {}", message),
        "info" => tracing::info!("[renderer] {}", message),
        "debug" => tracing::debug!("[renderer] {}", message),
        _ => tracing::info!("[renderer] {}", message),
    }
}

#[tauri::command(async)]
pub fn factory_reset(
    handle: AppHandle,
    state: tauri::State<'_, crate::state::AppState>,
) -> Result<(), String> {
    let mut config = state.config.lock().map_err(|e| e.to_string())?;
    config.reset()?;
    drop(config);
    handle.restart();
}

#[tauri::command]
pub async fn reset_session(handle: AppHandle) -> Result<(), String> {
    risuko_engine::engine::stop_engine()
        .await
        .map_err(|e| e.to_string())?;
    if let Ok(config_dir) = handle.path().app_config_dir() {
        let session_path = config_dir.join(risuko_engine::engine::SESSION_FILENAME);
        let _ = std::fs::remove_file(&session_path);
    }
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
    risuko_engine::engine::start_engine(&config, event_sink, upload_mgr)
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

#[tauri::command]
pub fn toggle_app_menu(handle: AppHandle, hidden: bool) -> Result<(), String> {
    crate::managers::menu::toggle_app_menu(&handle, hidden)
}

#[tauri::command]
pub fn is_opened_at_login() -> bool {
    std::env::args_os().any(|arg| arg == "--opened-at-login=1")
}

#[tauri::command]
pub fn set_android_system_bars(dark_mode: bool) -> Result<(), String> {
    #[cfg(target_os = "android")]
    {
        crate::commands::android_intent::set_system_bars(dark_mode)
    }
    #[cfg(not(target_os = "android"))]
    {
        let _ = dark_mode;
        Ok(())
    }
}

#[tauri::command]
pub fn ensure_android_storage_access() -> Result<bool, String> {
    #[cfg(target_os = "android")]
    {
        crate::commands::android_intent::ensure_all_files_access()
    }
    #[cfg(not(target_os = "android"))]
    {
        Ok(true)
    }
}

#[tauri::command]
pub fn update_android_download_notification(
    progress: u32,
    active_count: u32,
    detail: String,
) -> Result<(), String> {
    #[cfg(target_os = "android")]
    {
        crate::commands::android_intent::show_download_notification(progress, active_count, &detail)
    }
    #[cfg(not(target_os = "android"))]
    {
        let _ = (progress, active_count, detail);
        Ok(())
    }
}

#[tauri::command]
pub fn clear_android_download_notification() -> Result<(), String> {
    #[cfg(target_os = "android")]
    {
        crate::commands::android_intent::hide_download_notification()
    }
    #[cfg(not(target_os = "android"))]
    {
        Ok(())
    }
}

#[tauri::command]
pub async fn shutdown_system(handle: AppHandle) -> Result<(), String> {
    tracing::info!("[Risuko] shutdown_system: initiating OS shutdown");

    #[cfg(target_os = "android")]
    {
        return quit_app(handle).await;
    }

    #[cfg(not(target_os = "android"))]
    {
        let state = handle.state::<crate::state::AppState>();
        state.is_quitting.store(true, Ordering::SeqCst);

        #[cfg(target_os = "windows")]
        let args = ["/s", "/t", "0"];
        #[cfg(not(target_os = "windows"))]
        let args = ["-h", "now"];

        if let Some(manager) = risuko_engine::engine::get_manager().await {
            if let Err(e) = manager.save_session().await {
                tracing::warn!("shutdown_system: failed to save session: {}", e);
            }
        }

        let rollback = || state.is_quitting.store(false, Ordering::SeqCst);
        let status = tauri::async_runtime::spawn_blocking(move || {
            std::process::Command::new("shutdown").args(args).status()
        })
        .await
        .map_err(|e| {
            rollback();
            format!("shutdown failed: {e}")
        })?
        .map_err(|e| {
            rollback();
            format!("shutdown failed: {e}")
        })?;
        if !status.success() {
            rollback();
            return Err(format!("shutdown command failed: {:?}", status));
        }

        Ok(())
    }
}
