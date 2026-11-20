use super::panel::Panel;
use super::panel_gate::ShowStep;
use tauri::{AppHandle, Emitter, Manager, WebviewWindow};

pub const FLYOUT_LABEL: &str = "tray-panel";

static FLYOUT: Panel = Panel {
    label: FLYOUT_LABEL,
    page: "tray.html",
    title: "Risuko Quick Panel",
    width: 396.0,
    height: 540.0,
    gate: |state| &state.flyout_gate,
};

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

pub fn cache_tray_rect(app: &AppHandle, rect: &tauri::Rect, scale_factor: f64) {
    let pos = rect.position.to_physical::<f64>(scale_factor);
    let size = rect.size.to_physical::<f64>(scale_factor);
    if let Some(state) = app.try_state::<crate::state::AppState>() {
        if let Ok(mut anchor) = state.tray_anchor.lock() {
            *anchor = Some((pos.x, pos.y, size.width, size.height));
        }
    }
}

pub fn toggle_flyout(app: &AppHandle) {
    let window = FLYOUT.window(app);

    if let Some(window) = &window {
        if window.is_visible().unwrap_or(false) {
            #[cfg(target_os = "macos")]
            demote_if_ui_hidden(app);
            let _ = window.hide();
            return;
        }
    }

    let step = FLYOUT.with_gate(app, |gate| {
        (!gate.cancel_pending()).then(|| FLYOUT.request_show_locked(app, gate))
    });
    match step.flatten() {
        Some((ShowStep::ShowNow, Some(window))) => present(app, &window),
        Some((ShowStep::Build, _)) => FLYOUT.spawn_build(app),
        _ => {}
    }
}

pub fn on_ready(app: &AppHandle) {
    if FLYOUT.page_ready(app) {
        if let Some(window) = FLYOUT.window(app) {
            present(app, &window);
        }
    }
}

fn present(app: &AppHandle, window: &WebviewWindow) {
    FLYOUT.show(app, window);
    let _ = window.emit("flyout:show", ());
}
