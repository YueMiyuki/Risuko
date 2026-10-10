use tauri::{
    plugin::{Builder, TauriPlugin},
    Runtime,
};

const PLUGIN_NAME: &str = "webview-upgrade";

pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new(PLUGIN_NAME).build()
}
