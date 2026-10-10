#[cfg(not(target_os = "android"))]
use super::panel_gate::ShowStep;
#[cfg(not(target_os = "android"))]
use tauri::{AppHandle, Emitter, Manager, WebviewUrl, WebviewWindow, WebviewWindowBuilder};

#[cfg(not(target_os = "android"))]
pub const CLIP_PROMPT_LABEL: &str = "clip-prompt";

#[cfg(not(target_os = "android"))]
const PROMPT_WIDTH: f64 = 384.0;
#[cfg(not(target_os = "android"))]
const PROMPT_HEIGHT: f64 = 156.0;

#[cfg(not(target_os = "android"))]
fn build_prompt(app: &AppHandle) -> Result<(), tauri::Error> {
    if app.get_webview_window(CLIP_PROMPT_LABEL).is_some() {
        return Ok(());
    }

    let builder = WebviewWindowBuilder::new(
        app,
        CLIP_PROMPT_LABEL,
        WebviewUrl::App("clip-prompt.html".into()),
    )
    .title("Risuko")
    .inner_size(PROMPT_WIDTH, PROMPT_HEIGHT)
    .decorations(false)
    .always_on_top(true)
    .skip_taskbar(true)
    .resizable(false)
    .visible(false)
    .focused(false)
    .transparent(true)
    .shadow(false)
    .accept_first_mouse(true);

    #[cfg(target_os = "macos")]
    let builder = builder.visible_on_all_workspaces(true);

    builder.build()?;
    Ok(())
}

#[cfg(not(target_os = "android"))]
fn spawn_build(app: &AppHandle) {
    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        let ok = match build_prompt(&app) {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!("[Risuko] failed to build clip-prompt window: {}", e);
                false
            }
        };
        if let Some(state) = app.try_state::<crate::state::AppState>() {
            if let Ok(mut gate) = state.clip_gate.lock() {
                gate.built(ok);
            }
        }
    });
}

#[cfg(not(target_os = "android"))]
pub fn show_clip_prompt(app: &AppHandle, uri: &str) {
    let window = app.get_webview_window(CLIP_PROMPT_LABEL);
    #[cfg(target_os = "macos")]
    {
        let already_open = window
            .as_ref()
            .and_then(|w| w.is_visible().ok())
            .unwrap_or(false);
        if !already_open {
            prev_focus::remember();
        }
    }
    let Some(state) = app.try_state::<crate::state::AppState>() else {
        return;
    };
    if let Ok(mut pending) = state.pending_clip_uri.lock() {
        *pending = Some(uri.to_string());
    }
    let step = {
        let Ok(mut gate) = state.clip_gate.lock() else {
            return;
        };
        gate.request_show(window.is_some())
    };
    match (step, window) {
        (ShowStep::ShowNow, Some(window)) => present(app, &window, uri),
        (ShowStep::Build, _) => spawn_build(app),
        _ => {}
    }
}

#[cfg(not(target_os = "android"))]
pub fn on_ready(app: &AppHandle) {
    let Some(state) = app.try_state::<crate::state::AppState>() else {
        return;
    };
    let want_show = state
        .clip_gate
        .lock()
        .map(|mut gate| gate.page_ready())
        .unwrap_or(false);
    if !want_show {
        return;
    }
    let uri = state.pending_clip_uri.lock().ok().and_then(|g| g.clone());
    let (Some(uri), Some(window)) = (uri, app.get_webview_window(CLIP_PROMPT_LABEL)) else {
        return;
    };
    present(app, &window, &uri);
}

#[cfg(not(target_os = "android"))]
fn present(app: &AppHandle, window: &WebviewWindow, uri: &str) {
    position_prompt(app, window);
    let _ = window.show();
    let _ = window.set_focus();
    let _ = window.emit("clip-prompt:show", uri.to_string());
}

#[cfg(not(target_os = "android"))]
pub fn hide_clip_prompt(app: &AppHandle) {
    if let Some(state) = app.try_state::<crate::state::AppState>() {
        if let Ok(mut gate) = state.clip_gate.lock() {
            gate.cancel_pending();
        }
    }
    if let Some(window) = app.get_webview_window(CLIP_PROMPT_LABEL) {
        #[cfg(target_os = "macos")]
        crate::managers::flyout::demote_if_ui_hidden(app);
        let _ = window.hide();
    }
}

#[cfg(target_os = "macos")]
pub fn restore_prev_focus() {
    prev_focus::restore();
}

#[cfg(target_os = "macos")]
mod prev_focus {
    use std::sync::atomic::{AtomicI32, Ordering};

    static PREV_APP_PID: AtomicI32 = AtomicI32::new(0);

    pub fn remember() {
        use objc2_app_kit::NSWorkspace;
        let self_pid = std::process::id() as i32;
        let pid = NSWorkspace::sharedWorkspace()
            .frontmostApplication()
            .map(|a| a.processIdentifier())
            .filter(|&p| p != self_pid)
            .unwrap_or(0);
        PREV_APP_PID.store(pid, Ordering::SeqCst);
    }

    pub fn restore() {
        let pid = PREV_APP_PID.swap(0, Ordering::SeqCst);
        if pid == 0 {
            return;
        }
        use objc2_app_kit::{NSApplicationActivationOptions, NSRunningApplication};
        if let Some(app) = NSRunningApplication::runningApplicationWithProcessIdentifier(pid) {
            app.activateWithOptions(NSApplicationActivationOptions::empty());
        }
    }
}

#[cfg(not(target_os = "android"))]
fn position_prompt(app: &AppHandle, window: &WebviewWindow) {
    super::position_near_tray(app, window, PROMPT_WIDTH, PROMPT_HEIGHT);
}
