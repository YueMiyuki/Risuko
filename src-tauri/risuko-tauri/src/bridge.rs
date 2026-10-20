use std::path::PathBuf;

use serde_json::Value;
use tauri::{AppHandle, Emitter, Manager};
use tauri_plugin_store::StoreExt;

use risuko_engine::traits::{ConfigDirProvider, EventSink, StorageBackend};

pub struct TauriConfigDir {
    config_dir: PathBuf,
}

impl TauriConfigDir {
    pub fn new(handle: &AppHandle) -> Self {
        let config_dir = handle
            .path()
            .app_config_dir()
            .unwrap_or_else(|_| PathBuf::from("."));
        Self { config_dir }
    }
}

impl ConfigDirProvider for TauriConfigDir {
    fn config_dir(&self) -> PathBuf {
        self.config_dir.clone()
    }
}

pub struct TauriEventSink {
    handle: AppHandle,
}

impl TauriEventSink {
    pub fn new(handle: &AppHandle) -> Self {
        Self {
            handle: handle.clone(),
        }
    }
}

impl EventSink for TauriEventSink {
    fn emit(&self, event: &str, payload: Value) {
        if let Err(e) = self.handle.emit(event, payload) {
            tracing::warn!("Failed to emit Tauri event {}: {}", event, e);
        }
    }
}

pub struct TauriStorage {
    handle: AppHandle,
}

impl TauriStorage {
    pub fn new(handle: &AppHandle) -> Self {
        Self {
            handle: handle.clone(),
        }
    }
}

impl StorageBackend for TauriStorage {
    fn load(&self, key: &str) -> Result<Option<Value>, String> {
        let store = self
            .handle
            .store(key)
            .map_err(|e| format!("Failed to open store '{key}': {e}"))?;
        Ok(store.get("data"))
    }

    fn save(&self, key: &str, value: &Value) -> Result<(), String> {
        let store = self
            .handle
            .store(key)
            .map_err(|e| format!("Failed to open store '{key}': {e}"))?;
        store.set("data", value.clone());
        store
            .save()
            .map_err(|e| format!("Failed to save store '{key}': {e}"))?;
        Ok(())
    }
}
