#[cfg(not(target_os = "android"))]
use super::panel_gate::ShowStep;
#[cfg(not(target_os = "android"))]
use tauri::{AppHandle, Emitter, Manager, WebviewUrl, WebviewWindow, WebviewWindowBuilder};

#[cfg(not(target_os = "android"))]
pub const FLYOUT_LABEL: &str = "tray-panel";

#[cfg(not(target_os = "android"))]
const FLYOUT_WIDTH: f64 = 396.0;
#[cfg(not(target_os = "android"))]
const FLYOUT_HEIGHT: f64 = 540.0;

#[cfg(not(target_os = "android"))]
fn build_flyout(app: &AppHandle) -> Result<(), tauri::Error> {
    if app.get_webview_window(FLYOUT_LABEL).is_some() {
        return Ok(());
    }

    let builder = WebviewWindowBuilder::new(app, FLYOUT_LABEL, WebviewUrl::App("tray.html".into()))
        .title("Risuko Quick Panel")
        .inner_size(FLYOUT_WIDTH, FLYOUT_HEIGHT)
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
        let ok = match build_flyout(&app) {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!("[Risuko] failed to build flyout window: {}", e);
                false
            }
        };
        if let Some(state) = app.try_state::<crate::state::AppState>() {
            if let Ok(mut gate) = state.flyout_gate.lock() {
                gate.built(ok);
            }
        }
    });
}

#[cfg(target_os = "macos")]
pub fn demote_if_ui_hidden(app: &AppHandle) {
    let run_mode = crate::utils::run_mode::current_run_mode(app);
    if !crate::utils::run_mode::is_tray_mode(run_mode) {
        return;
    }
    let main_visible = app
        .get_webview_window("main")
        .and_then(|w| w.is_visible().ok())
        .unwrap_or(false);
    if !main_visible {
        let _ = app.set_activation_policy(tauri::ActivationPolicy::Accessory);
    }
}

#[cfg(not(target_os = "android"))]
pub fn cache_tray_rect(app: &AppHandle, rect: &tauri::Rect, scale_factor: f64) {
    let pos = rect.position.to_physical::<f64>(scale_factor);
    let size = rect.size.to_physical::<f64>(scale_factor);
    if let Some(state) = app.try_state::<crate::state::AppState>() {
        if let Ok(mut anchor) = state.tray_anchor.lock() {
            *anchor = Some((pos.x, pos.y, size.width, size.height));
        }
    }
}

#[cfg(not(target_os = "android"))]
pub fn toggle_flyout(app: &AppHandle) {
    let window = app.get_webview_window(FLYOUT_LABEL);

    if let Some(window) = &window {
        if window.is_visible().unwrap_or(false) {
            #[cfg(target_os = "macos")]
            demote_if_ui_hidden(app);
            let _ = window.hide();
            return;
        }
    }

    let Some(state) = app.try_state::<crate::state::AppState>() else {
        return;
    };
    let step = {
        let Ok(mut gate) = state.flyout_gate.lock() else {
            return;
        };
        if gate.cancel_pending() {
            return;
        }
        gate.request_show(window.is_some())
    };
    match (step, window) {
        (ShowStep::ShowNow, Some(window)) => present(app, &window),
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
        .flyout_gate
        .lock()
        .map(|mut gate| gate.page_ready())
        .unwrap_or(false);
    if want_show {
        if let Some(window) = app.get_webview_window(FLYOUT_LABEL) {
            present(app, &window);
        }
    }
}

#[cfg(not(target_os = "android"))]
fn present(app: &AppHandle, window: &WebviewWindow) {
    position_flyout(app, window);
    let _ = window.show();
    let _ = window.set_focus();
    let _ = window.emit("flyout:show", ());
}

#[cfg(not(target_os = "android"))]
fn position_flyout(app: &AppHandle, window: &WebviewWindow) {
    super::position_near_tray(app, window, FLYOUT_WIDTH, FLYOUT_HEIGHT);
}
