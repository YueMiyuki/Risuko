use tauri::{AppHandle, State};

use crate::state::AppState;

#[cfg(not(target_os = "android"))]
pub fn is_download_candidate(uri: &str, exts: &[String]) -> bool {
    use risuko_engine::engine;
    let uri = uri.trim();
    if uri.is_empty() {
        return false;
    }
    let lower = uri.to_ascii_lowercase();
    if engine::torrent::is_magnet_uri(uri)
        || engine::ed2k::is_ed2k_uri(uri)
        || engine::ftp::is_ftp_uri(uri)
        || engine::adc::is_adc_uri(uri)
        || engine::gnutella::is_gnutella_uri(uri)
        || engine::g2::is_g2_uri(uri)
        || engine::gift::is_gift_uri(uri)
        || engine::m3u8::is_m3u8_uri(uri)
        || lower.starts_with("thunder://")
    {
        return true;
    }
    if lower.starts_with("http://") || lower.starts_with("https://") {
        if engine::media::is_media_uri(uri) {
            return false;
        }
        return path_has_allowed_ext(uri, exts);
    }
    false
}

#[cfg(not(target_os = "android"))]
fn path_has_allowed_ext(uri: &str, exts: &[String]) -> bool {
    let after_scheme = uri.split_once("://").map(|(_, rest)| rest).unwrap_or(uri);
    let path = after_scheme.split(['?', '#']).next().unwrap_or("");
    let last = path.rsplit('/').next().unwrap_or("");
    let Some(dot) = last.rfind('.') else {
        return false;
    };
    let ext = last[dot + 1..].to_ascii_lowercase();
    if ext.is_empty() {
        return false;
    }
    exts.iter()
        .any(|e| e.trim_start_matches('.').eq_ignore_ascii_case(&ext))
}

#[cfg(not(target_os = "android"))]
const MAX_CLIPBOARD_BYTES: usize = 8 * 1024;

#[cfg(not(target_os = "android"))]
fn config_value<'a>(
    cfg: &'a risuko_engine::config::ConfigManager,
    key: &str,
) -> Option<&'a serde_json::Value> {
    cfg.get_user_config()
        .get(key)
        .or_else(|| cfg.get_system_config().get(key))
}

#[cfg(not(target_os = "android"))]
pub fn watch_enabled(state: &AppState) -> bool {
    let Ok(cfg) = state.config.lock() else {
        return false;
    };
    config_value(&cfg, "clipboard-watch")
        .and_then(|v| v.as_bool())
        .unwrap_or(true)
}

#[cfg(not(target_os = "android"))]
fn watch_exts(state: &AppState) -> Vec<String> {
    let Ok(cfg) = state.config.lock() else {
        return Vec::new();
    };
    config_value(&cfg, "clipboard-watch-extensions")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(not(target_os = "android"))]
pub fn on_clipboard_update(app: &AppHandle) {
    use tauri::Manager;
    use tauri_plugin_clipboard::Clipboard;

    let state = app.state::<AppState>();
    if !watch_enabled(&state) {
        return;
    }
    let clipboard = app.state::<Clipboard>();
    if !clipboard.has_text().unwrap_or(false) {
        return;
    }
    let Ok(text) = clipboard.read_text() else {
        return;
    };
    let text = text.trim();
    if text.is_empty() || text.len() > MAX_CLIPBOARD_BYTES {
        return;
    }
    let text = text.to_string();
    if let Ok(mut sw) = state.last_clipboard_self_write.lock() {
        if sw.as_deref() == Some(text.as_str()) {
            *sw = None;
            return;
        }
    }
    if let Ok(mut seen) = state.last_clipboard_seen.lock() {
        if seen.as_deref() == Some(text.as_str()) {
            return;
        }
        *seen = Some(text.clone());
    }
    if !is_download_candidate(&text, &watch_exts(&state)) {
        return;
    }
    crate::managers::clip_prompt::show_clip_prompt(app, &text);
}

#[tauri::command]
pub fn mark_clipboard_self_write(state: State<'_, AppState>, text: String) {
    if let Ok(mut sw) = state.last_clipboard_self_write.lock() {
        *sw = Some(text);
    }
}

#[tauri::command]
pub fn start_clipboard_watch(app: AppHandle) -> Result<(), String> {
    #[cfg(not(target_os = "android"))]
    {
        use tauri::Manager;
        use tauri_plugin_clipboard::Clipboard;
        let clipboard = app.state::<Clipboard>();
        if !clipboard.is_monitor_running() {
            clipboard.start_monitor(app.clone())?;
        }
    }
    #[cfg(target_os = "android")]
    let _ = app;
    Ok(())
}

#[tauri::command]
pub fn stop_clipboard_watch(app: AppHandle) -> Result<(), String> {
    #[cfg(not(target_os = "android"))]
    {
        use tauri::Manager;
        use tauri_plugin_clipboard::Clipboard;
        let clipboard = app.state::<Clipboard>();
        if clipboard.is_monitor_running() {
            clipboard.stop_monitor(app.clone())?;
        }
    }
    #[cfg(target_os = "android")]
    let _ = app;
    Ok(())
}

#[tauri::command]
pub fn get_clip_prompt_uri(state: State<'_, AppState>) -> Option<String> {
    state.pending_clip_uri.lock().ok().and_then(|g| g.clone())
}

#[tauri::command]
pub fn clip_prompt_accept(app: AppHandle, state: State<'_, AppState>) -> Result<(), String> {
    let uri = state
        .pending_clip_uri
        .lock()
        .ok()
        .and_then(|mut g| g.take());
    #[cfg(not(target_os = "android"))]
    crate::managers::clip_prompt::hide_clip_prompt(&app);
    if let Some(uri) = uri {
        crate::commands::app_cmds::show_main_window(&app)?;
        if let Some(main) = tauri::Manager::get_webview_window(&app, "main") {
            use tauri::Emitter;
            let _ = main.emit("clipboard-download", uri);
        }
    }
    Ok(())
}

#[tauri::command]
pub fn clip_prompt_dismiss(app: AppHandle, state: State<'_, AppState>) {
    if let Ok(mut g) = state.pending_clip_uri.lock() {
        *g = None;
    }
    #[cfg(not(target_os = "android"))]
    crate::managers::clip_prompt::hide_clip_prompt(&app);
    #[cfg(target_os = "macos")]
    let _ = app.run_on_main_thread(crate::managers::clip_prompt::restore_prev_focus);
    #[cfg(target_os = "android")]
    let _ = app;
}

#[cfg(test)]
mod tests {
    use super::is_download_candidate;

    fn exts() -> Vec<String> {
        ["iso", "zip", "torrent"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn classifies_download_candidates() {
        let e = exts();
        assert!(is_download_candidate("magnet:?xt=urn:btih:0123abc", &e));
        assert!(is_download_candidate("thunder://QUFodHRwOi8v", &e));
        assert!(is_download_candidate(
            "https://mirror.example.com/ubuntu.iso",
            &e
        ));
        assert!(is_download_candidate(
            "https://example.com/a/b.torrent?dl=1",
            &e
        ));
        assert!(!is_download_candidate(
            "https://example.com/blog/post-123",
            &e
        ));
        assert!(!is_download_candidate("https://github.com/user/repo", &e));
        assert!(!is_download_candidate("just some copied text", &e));
        assert!(!is_download_candidate("", &e));
        assert!(!is_download_candidate("https://example.com/page.html", &e));
        assert!(is_download_candidate("magnet:?xt=urn:btih:0123abc", &[]));
        assert!(!is_download_candidate(
            "https://mirror.example.com/ubuntu.iso",
            &[]
        ));
    }
}
