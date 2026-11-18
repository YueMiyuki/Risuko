use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tauri::{AppHandle, Emitter, Manager};

use risuko_engine::traits::{write_file_atomically, ConfigDirProvider, EventSink, StorageBackend};

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
    dir: PathBuf,
}

impl TauriStorage {
    pub fn new(handle: &AppHandle) -> Self {
        let dir = handle
            .path()
            .app_data_dir()
            .unwrap_or_else(|_| PathBuf::from("."));
        Self::with_dir(dir)
    }

    pub(crate) fn with_dir(dir: PathBuf) -> Self {
        Self { dir }
    }

    fn path_for(&self, key: &str) -> Result<PathBuf, String> {
        if key.is_empty() || key == "." || key == ".." || key.contains(['/', '\\', '\0']) {
            return Err(format!("invalid storage key: {key:?}"));
        }
        Ok(self.dir.join(key))
    }
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

impl StorageBackend for TauriStorage {
    fn load(&self, key: &str) -> Result<Option<Value>, String> {
        let path = self.path_for(key)?;
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(format!("Failed to read {}: {e}", path.display())),
        };
        let mut value: Value = match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(e) => {
                let aside = self.dir.join(format!("{key}.corrupt-{}", unix_secs()));
                match std::fs::rename(&path, &aside) {
                    Ok(()) => tracing::warn!(
                        "{} could not be parsed ({e}); moved it to {}",
                        path.display(),
                        aside.display()
                    ),
                    Err(rename_err) => tracing::warn!(
                        "{} could not be parsed ({e}) and could not be moved to {}: {rename_err}",
                        path.display(),
                        aside.display()
                    ),
                }
                return Err(format!("Failed to parse {}: {e}", path.display()));
            }
        };
        Ok(value.get_mut("data").map(Value::take))
    }

    fn save(&self, key: &str, value: &Value) -> Result<(), String> {
        let path = self.path_for(key)?;
        let data = serde_json::to_string_pretty(&serde_json::json!({ "data": value }))
            .map_err(|e| format!("Failed to serialize: {e}"))?;
        write_file_atomically(&path, data.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn read_json(path: &std::path::Path) -> Value {
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }

    #[test]
    fn saved_values_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let storage = TauriStorage::with_dir(dir.path().to_path_buf());
        let value = json!({"data": {"feeds": []}});

        storage.save("rss", &value).unwrap();

        assert_eq!(storage.load("rss").unwrap(), Some(value));
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, vec!["rss".to_string()]);
        assert_eq!(
            read_json(&dir.path().join("rss")),
            json!({"data": {"data": {"feeds": []}}})
        );
    }

    #[test]
    fn files_written_by_the_store_plugin_still_load() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("upload-sinks"),
            br#"{"data": {"data": {"x": 1}}}"#,
        )
        .unwrap();
        let storage = TauriStorage::with_dir(dir.path().to_path_buf());

        assert_eq!(
            storage.load("upload-sinks").unwrap(),
            Some(json!({"data": {"x": 1}}))
        );
    }

    #[test]
    fn missing_file_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let storage = TauriStorage::with_dir(dir.path().to_path_buf());

        assert_eq!(storage.load("download_history_stats").unwrap(), None);
    }

    #[test]
    fn unparsable_file_is_moved_aside_not_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let original = br#"{"data": {"da"#;
        std::fs::write(dir.path().join("rss"), original).unwrap();
        let storage = TauriStorage::with_dir(dir.path().to_path_buf());

        assert!(storage.load("rss").is_err());

        assert!(!dir.path().join("rss").exists());
        let moved: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("rss.corrupt-"))
            })
            .collect();
        assert_eq!(moved.len(), 1);
        assert_eq!(std::fs::read(&moved[0]).unwrap(), original);

        storage
            .save("rss", &json!({"data": {"feeds": []}}))
            .unwrap();
        assert_eq!(std::fs::read(&moved[0]).unwrap(), original);
        assert!(dir.path().join("rss").exists());
    }

    #[test]
    fn invalid_keys_are_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let storage = TauriStorage::with_dir(dir.path().to_path_buf());

        for key in ["", "../x", "a/b", "a\\b"] {
            assert!(storage.load(key).is_err(), "load accepted {key:?}");
            assert!(
                storage.save(key, &json!({})).is_err(),
                "save accepted {key:?}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn saved_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let storage = TauriStorage::with_dir(dir.path().to_path_buf());

        storage
            .save("upload-sinks", &json!({"data": {"sinks": []}}))
            .unwrap();

        let mode = std::fs::metadata(dir.path().join("upload-sinks"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
