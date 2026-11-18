use super::panel::Panel;
use super::panel_gate::ShowStep;
use tauri::{AppHandle, Emitter, Manager, WebviewWindow};

pub const CLIP_PROMPT_LABEL: &str = "clip-prompt";

static PROMPT: Panel = Panel {
    label: CLIP_PROMPT_LABEL,
    page: "clip-prompt.html",
    title: "Risuko",
    width: 384.0,
    height: 156.0,
    gate: |state| &state.clip_gate,
};

pub fn show_clip_prompt(app: &AppHandle, uri: &str) {
    let window = PROMPT.window(app);
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
    match (PROMPT.request_show(app, window.is_some()), window) {
        (Some(ShowStep::ShowNow), Some(window)) => present(app, &window, uri),
        (Some(ShowStep::Build), _) => PROMPT.spawn_build(app),
        _ => {}
    }
}

pub fn on_ready(app: &AppHandle) {
    let Some(state) = app.try_state::<crate::state::AppState>() else {
        return;
    };
    if !PROMPT.page_ready(app) {
        return;
    }
    let uri = state.pending_clip_uri.lock().ok().and_then(|g| g.clone());
    let (Some(uri), Some(window)) = (uri, PROMPT.window(app)) else {
        return;
    };
    present(app, &window, &uri);
}

fn present(app: &AppHandle, window: &WebviewWindow, uri: &str) {
    PROMPT.show(app, window);
    let _ = window.emit("clip-prompt:show", uri.to_string());
}

pub fn hide_clip_prompt(app: &AppHandle) {
    PROMPT.with_gate(app, |gate| gate.cancel_pending());
    if let Some(window) = PROMPT.window(app) {
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
