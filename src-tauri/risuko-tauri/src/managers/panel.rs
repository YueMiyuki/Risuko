use std::sync::Mutex;

use tauri::{AppHandle, Manager, WebviewUrl, WebviewWindow, WebviewWindowBuilder};

use super::panel_gate::{PanelGate, ShowStep};
use crate::state::AppState;

pub struct Panel {
    pub label: &'static str,
    pub page: &'static str,
    pub title: &'static str,
    pub width: f64,
    pub height: f64,
    pub gate: fn(&AppState) -> &Mutex<PanelGate>,
}

impl Panel {
    pub fn window(&self, app: &AppHandle) -> Option<WebviewWindow> {
        app.get_webview_window(self.label)
    }

    fn build(&self, app: &AppHandle) -> Result<(), tauri::Error> {
        if self.window(app).is_some() {
            return Ok(());
        }

        let builder = WebviewWindowBuilder::new(app, self.label, WebviewUrl::App(self.page.into()))
            .title(self.title)
            .inner_size(self.width, self.height)
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

    pub fn spawn_build(&'static self, app: &AppHandle) {
        let app = app.clone();
        tauri::async_runtime::spawn(async move {
            let ok = match self.build(&app) {
                Ok(()) => true,
                Err(e) => {
                    tracing::warn!("[Risuko] failed to build {} window: {}", self.label, e);
                    false
                }
            };
            self.with_gate(&app, |gate| gate.built(ok));
        });
    }

    pub fn with_gate<T>(&self, app: &AppHandle, f: impl FnOnce(&mut PanelGate) -> T) -> Option<T> {
        let state = app.try_state::<AppState>()?;
        let mut gate = (self.gate)(&state).lock().ok()?;
        Some(f(&mut gate))
    }

    pub fn request_show_locked(
        &self,
        app: &AppHandle,
        gate: &mut PanelGate,
    ) -> (ShowStep, Option<WebviewWindow>) {
        let window = self.window(app);
        (gate.request_show(window.is_some()), window)
    }

    pub fn request_show(&self, app: &AppHandle) -> Option<(ShowStep, Option<WebviewWindow>)> {
        self.with_gate(app, |gate| self.request_show_locked(app, gate))
    }

    pub fn page_ready(&self, app: &AppHandle) -> bool {
        self.with_gate(app, PanelGate::page_ready).unwrap_or(false)
    }

    pub fn show(&self, app: &AppHandle, window: &WebviewWindow) {
        super::position_near_tray(app, window, self.width, self.height);
        let _ = window.show();
        let _ = window.set_focus();
    }
}
