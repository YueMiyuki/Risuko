pub mod defaults;

use serde_json::{json, Map, Value};
use std::fs;
use std::path::{Path, PathBuf};

use crate::traits::{cleanup_stale_atomic_write_files, write_file_atomically, ConfigDirProvider};
use risuko_http::NoProxy;

const PROXY_SCOPES: [&str; 3] = ["download", "update-app", "update-trackers"];

pub fn normalize_proxy_config(value: &Value) -> Value {
    let root = value.as_object().cloned().unwrap_or_default();
    let empty_http = Map::new();
    let has_legacy_fields = proxy_has_legacy_fields(value);
    let mut merged_http = if has_legacy_fields {
        root.clone()
    } else {
        Map::new()
    };
    if let Some(nested) = root.get("http").and_then(Value::as_object) {
        merged_http.extend(nested.clone());
    }
    let http_source = if merged_http.is_empty() {
        &empty_http
    } else {
        &merged_http
    };
    let p2p_source = root
        .get("p2p")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    let scopes = normalize_proxy_scopes(http_source.get("scope"));

    let mut http = Map::new();
    http.insert(
        "enable".into(),
        json!(value_as_bool(http_source.get("enable"))),
    );
    http.insert(
        "server".into(),
        json!(http_source
            .get("server")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()),
    );
    http.insert(
        "bypass".into(),
        json!(normalize_proxy_bypass_value(http_source.get("bypass"))),
    );
    http.insert(
        "scope".into(),
        Value::Array(scopes.into_iter().map(Value::String).collect()),
    );

    let mut p2p = Map::new();
    p2p.insert(
        "enable".into(),
        json!(value_as_bool(p2p_source.get("enable"))),
    );
    p2p.insert(
        "server".into(),
        json!(p2p_source
            .get("server")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()),
    );
    p2p.insert(
        "bypass".into(),
        json!(normalize_proxy_bypass_value(p2p_source.get("bypass"))),
    );

    let udp_source = p2p_source
        .get("udp")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut udp = Map::new();
    udp.insert(
        "server".into(),
        json!(udp_source
            .get("server")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()),
    );
    udp.insert(
        "bypass".into(),
        json!(normalize_proxy_bypass_value(udp_source.get("bypass"))),
    );
    p2p.insert("udp".into(), Value::Object(udp));

    json!({ "http": http, "p2p": p2p })
}

/// Whether a proxy value uses the pre-profile, top-level HTTP fields
pub fn proxy_has_legacy_fields(value: &Value) -> bool {
    value.as_object().is_some_and(|root| {
        ["enable", "server", "bypass", "scope"]
            .iter()
            .any(|key| root.contains_key(*key))
    })
}

pub fn proxy_http_profile_is_explicit(value: &Value) -> bool {
    if proxy_has_legacy_fields(value) {
        return true;
    }

    let normalized = normalize_proxy_config(value);
    let Some(http) = normalized.get("http") else {
        return false;
    };
    http != &json!({
        "enable": false,
        "server": "",
        "bypass": "",
        "scope": PROXY_SCOPES
            .iter()
            .map(|scope| Value::String((*scope).to_string()))
            .collect::<Vec<_>>(),
    })
}

/// Whether a proxy value contains a non-default nested P2P profile.  A
/// default profile must not erase legacy/system P2P engine keys while older
/// configurations are being migrated
pub fn proxy_p2p_profile_is_explicit(value: &Value) -> bool {
    let normalized = normalize_proxy_config(value);
    normalized.get("p2p")
        != Some(&json!({
            "enable": false,
            "server": "",
            "bypass": "",
            "udp": {
                "server": "",
                "bypass": ""
            }
        }))
}

fn value_as_bool(value: Option<&Value>) -> bool {
    match value {
        Some(Value::Bool(value)) => *value,
        Some(Value::Number(value)) => value.as_f64().is_some_and(|value| value != 0.0),
        Some(Value::String(value)) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "true" | "1" | "yes" | "on"
        ),
        _ => false,
    }
}

fn normalize_proxy_bypass_value(value: Option<&Value>) -> String {
    let raw = match value {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(","),
        _ => String::new(),
    };
    NoProxy::normalize(raw)
}

fn normalize_user_map(map: &Map<String, Value>) -> Map<String, Value> {
    map.iter()
        .map(|(key, value)| {
            (
                key.clone(),
                if key == "proxy" {
                    normalize_proxy_config(value)
                } else {
                    value.clone()
                },
            )
        })
        .collect()
}

fn normalize_proxy_scopes(value: Option<&Value>) -> Vec<String> {
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return PROXY_SCOPES
            .iter()
            .map(|scope| (*scope).to_string())
            .collect();
    };

    let mut result = Vec::new();
    let mut add = |raw: &str| {
        for scope in raw.split([',', '\r', '\n']) {
            let scope = scope.trim().to_ascii_lowercase();
            if PROXY_SCOPES.contains(&scope.as_str()) && !result.iter().any(|known| known == &scope)
            {
                result.push(scope);
            }
        }
    };
    match value {
        Value::Array(items) => {
            for item in items {
                if let Some(scope) = item.as_str() {
                    add(scope);
                }
            }
        }
        Value::String(scopes) => add(scopes),
        _ => {}
    }
    result
}

pub struct ConfigManager {
    system_config: Map<String, Value>,
    user_config: Map<String, Value>,
    config_dir: PathBuf,
}

impl ConfigManager {
    pub fn new(provider: &dyn ConfigDirProvider) -> Result<Self, Box<dyn std::error::Error>> {
        Self::with_dir(provider.config_dir())
    }

    /// Create a ConfigManager with an explicit config directory path
    pub fn with_dir(config_dir: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        fs::create_dir_all(&config_dir)?;
        cleanup_stale_atomic_write_files(&config_dir);

        let system_config =
            load_or_default(&config_dir.join("system.json"), defaults::system_defaults());
        let user_config = load_or_default(&config_dir.join("user.json"), defaults::user_defaults());

        let mut manager = Self {
            system_config,
            user_config,
            config_dir,
        };

        let raw_proxy = manager.user_config.get("proxy").cloned();
        let migrated_legacy_proxy = raw_proxy.as_ref().is_some_and(proxy_has_legacy_fields);
        let normalized_proxy = normalize_proxy_config(raw_proxy.as_ref().unwrap_or(&Value::Null));
        if manager.user_config.get("proxy") != Some(&normalized_proxy) {
            manager.user_config.insert("proxy".into(), normalized_proxy);
            if let Err(err) = manager.save_user() {
                tracing::warn!("Failed to persist proxy profile migration: {}", err);
            }
        }

        if migrated_legacy_proxy {
            let proxy = manager
                .user_config
                .get("proxy")
                .cloned()
                .unwrap_or(Value::Null);
            let http = proxy.get("http").and_then(Value::as_object);
            let enabled = http
                .and_then(|profile| profile.get("enable"))
                .is_some_and(|value| value_as_bool(Some(value)));
            let server = http
                .and_then(|profile| profile.get("server"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim();
            let download = http
                .and_then(|profile| profile.get("scope"))
                .and_then(Value::as_array)
                .is_some_and(|scopes| {
                    scopes
                        .iter()
                        .any(|scope| scope.as_str() == Some("download"))
                });
            manager.system_config.insert(
                "all-proxy".into(),
                Value::String(if enabled && !server.is_empty() && download {
                    server.to_string()
                } else {
                    String::new()
                }),
            );
            manager.system_config.insert(
                "no-proxy".into(),
                Value::String(if enabled && !server.is_empty() && download {
                    http.and_then(|profile| profile.get("bypass"))
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string()
                } else {
                    String::new()
                }),
            );
            if let Err(err) = manager.save_system() {
                tracing::warn!("Failed to persist legacy proxy route migration: {}", err);
            }
        }

        if manager.migrate_legacy_keep_seeding_defaults() {
            if let Err(err) = manager.save_system() {
                tracing::warn!(
                    "Failed to persist legacy keep-seeding migration; continuing startup: {}",
                    err
                );
            }
        }

        Ok(manager)
    }

    pub fn config_dir(&self) -> &Path {
        &self.config_dir
    }

    pub fn get_system_config(&self) -> &Map<String, Value> {
        &self.system_config
    }

    pub fn get_user_config(&self) -> &Map<String, Value> {
        &self.user_config
    }

    pub fn get_merged_config(&self) -> Value {
        let mut merged = self.system_config.clone();
        merged.extend(self.user_config.clone());

        // Add runtime context without clobbering a user/system key of the same name (insert only when absent)
        merged
            .entry("platform".to_string())
            .or_insert_with(|| json!(std::env::consts::OS));
        merged
            .entry("arch".to_string())
            .or_insert_with(|| json!(std::env::consts::ARCH));

        Value::Object(merged)
    }

    pub fn set_system_config_map(&mut self, map: &Map<String, Value>) -> Result<(), String> {
        for (k, v) in map {
            self.system_config.insert(k.clone(), v.clone());
        }
        self.save_system()
    }

    pub fn remove_system_config_key(&mut self, key: &str) -> Result<(), String> {
        self.system_config.remove(key);
        self.save_system()
    }

    pub fn set_user_config_map(&mut self, map: &Map<String, Value>) -> Result<(), String> {
        self.user_config.extend(normalize_user_map(map));
        self.save_user()
    }

    pub fn replace_config_maps(
        &mut self,
        system: &Map<String, Value>,
        user: &Map<String, Value>,
    ) -> Result<(), String> {
        self.system_config = system.clone();
        self.user_config = normalize_user_map(user);
        self.save_system()?;
        self.save_user()
    }

    pub fn reset(&mut self) -> Result<(), String> {
        self.system_config = defaults::system_defaults();
        self.user_config = defaults::user_defaults();
        self.save_system()?;
        self.save_user()?;
        Ok(())
    }

    fn migrate_legacy_keep_seeding_defaults(&mut self) -> bool {
        let Some(keep_seeding) = parse_keep_seeding_option(self.user_config.get("keep-seeding"))
        else {
            return false;
        };
        if keep_seeding {
            return false;
        }

        let seed_ratio = parse_f64_like(self.system_config.get("seed-ratio"));
        let seed_time = parse_f64_like(self.system_config.get("seed-time"));

        let is_legacy_seed_ratio = matches!(seed_ratio, Some(value) if (value - 2.0).abs() < 1e-6);
        let is_legacy_seed_time = matches!(seed_time, Some(value) if (value - 2880.0).abs() < 1e-6);

        if !is_legacy_seed_ratio || !is_legacy_seed_time {
            return false;
        }

        self.system_config.insert("seed-ratio".into(), json!(0));
        self.system_config.insert("seed-time".into(), json!(0));
        true
    }

    fn save_system(&self) -> Result<(), String> {
        let path = self.config_dir.join("system.json");
        let data = serde_json::to_string_pretty(&self.system_config).map_err(|e| e.to_string())?;
        write_file_atomically(&path, data.as_bytes())
    }

    fn save_user(&self) -> Result<(), String> {
        let path = self.config_dir.join("user.json");
        let data = serde_json::to_string_pretty(&self.user_config).map_err(|e| e.to_string())?;
        write_file_atomically(&path, data.as_bytes())
    }
}

fn load_or_default(path: &Path, defaults: Map<String, Value>) -> Map<String, Value> {
    match fs::read_to_string(path) {
        Ok(data) => {
            if let Ok(Value::Object(mut map)) = serde_json::from_str(&data) {
                // Fill in missing keys from defaults
                for (k, v) in &defaults {
                    if !map.contains_key(k) {
                        map.insert(k.clone(), v.clone());
                    }
                }
                return map;
            }
            // File exists but is corrupt/unparseable: back it up so the user's broken settings aren't silently overwritten, then fall back
            let backup = path.with_extension("json.bak");
            match fs::rename(path, &backup) {
                Ok(()) => tracing::warn!(
                    "Config file {} is corrupt; backed up to {} and using defaults",
                    path.display(),
                    backup.display()
                ),
                Err(err) => tracing::warn!(
                    "Config file {} is corrupt and could not be backed up ({}); using defaults",
                    path.display(),
                    err
                ),
            }
        }
        Err(err) if err.kind() != std::io::ErrorKind::NotFound => {
            tracing::warn!(
                "Failed to read config {}: {}; using defaults",
                path.display(),
                err
            );
        }
        Err(_) => {}
    }
    defaults
}

pub fn parse_keep_seeding_option(value: Option<&Value>) -> Option<bool> {
    match value {
        Some(Value::Bool(v)) => Some(*v),
        Some(Value::Number(v)) => v.as_i64().map(|n| n != 0),
        Some(Value::String(v)) => {
            let normalized = v.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "true" | "1" | "yes" | "on" => Some(true),
                "false" | "0" | "no" | "off" | "" => Some(false),
                _ => None,
            }
        }
        _ => None,
    }
}

fn parse_f64_like(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Number(v)) => v.as_f64(),
        Some(Value::String(v)) => v.trim().parse::<f64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    // -- parse_keep_seeding_option --

    #[test]
    fn keep_seeding_bool() {
        assert_eq!(parse_keep_seeding_option(Some(&json!(true))), Some(true));
        assert_eq!(parse_keep_seeding_option(Some(&json!(false))), Some(false));
    }

    #[test]
    fn keep_seeding_string_truthy() {
        for s in &["true", "1", "yes", "on"] {
            assert_eq!(
                parse_keep_seeding_option(Some(&json!(s))),
                Some(true),
                "expected true for {:?}",
                s
            );
        }
    }

    // -- proxy profile normalization --

    #[test]
    fn normalize_proxy_config_migrates_legacy_fields_into_http() {
        let normalized = normalize_proxy_config(&json!({
            "enable": "true",
            "server": " http://proxy.example:8080 ",
            "bypass": " Example.COM,\n127.0.0.1, example.com ",
            "scope": ["download", "download", "unsupported"]
        }));

        assert_eq!(
            normalized,
            json!({
                "http": {
                    "enable": true,
                    "server": "http://proxy.example:8080",
                    "bypass": "example.com,127.0.0.1",
                    "scope": ["download"]
                },
                "p2p": {
                    "enable": false,
                    "server": "",
                    "bypass": "",
                    "udp": {
                        "server": "",
                        "bypass": ""
                    }
                }
            })
        );
    }

    #[test]
    fn normalize_proxy_config_fills_nested_defaults_independently() {
        let normalized = normalize_proxy_config(&json!({
            "http": {
                "enable": 1,
                "server": "http://proxy.example:8080",
                "bypass": " .Example.com, example.com:443 ",
                "scope": "download, update-trackers"
            },
            "p2p": {
                "enable": "on",
                "server": "socks5h://proxy.example:1080",
                "bypass": "10.0.0.0/8"
            }
        }));

        assert_eq!(
            normalized.get("http").and_then(|v| v.get("scope")),
            Some(&json!(["download", "update-trackers"]))
        );
        assert_eq!(
            normalized.get("http").and_then(|v| v.get("bypass")),
            Some(&json!("example.com,example.com:443"))
        );
        assert_eq!(
            normalized.get("p2p"),
            Some(&json!({
                "enable": true,
                "server": "socks5h://proxy.example:1080",
                "bypass": "10.0.0.0/8",
                "udp": {
                    "server": "",
                    "bypass": ""
                }
            }))
        );
    }

    #[test]
    fn normalize_proxy_config_keeps_an_independent_udp_override() {
        let normalized = normalize_proxy_config(&json!({
            "p2p": {
                "enable": true,
                "server": "http://tcp.example:8080",
                "bypass": "tcp.example",
                "udp": {
                    "server": "socks5h://udp.example:1080",
                    "bypass": "udp.example"
                }
            }
        }));

        assert_eq!(
            normalized.get("p2p").and_then(|p| p.get("udp")),
            Some(&json!({
                "server": "socks5h://udp.example:1080",
                "bypass": "udp.example"
            }))
        );
    }

    #[test]
    fn normalize_proxy_config_accepts_array_bypass_values() {
        let normalized = normalize_proxy_config(&json!({
            "http": { "bypass": [" Example.com ", "127.0.0.1", 42] },
            "p2p": { "bypass": ["10.0.0.0/8", "10.0.0.0/8"] }
        }));

        assert_eq!(
            normalized.get("http").and_then(|v| v.get("bypass")),
            Some(&json!("example.com,127.0.0.1"))
        );
        assert_eq!(
            normalized.get("p2p").and_then(|v| v.get("bypass")),
            Some(&json!("10.0.0.0/8"))
        );
    }

    #[test]
    fn normalize_proxy_config_migrates_legacy_http_fields_alongside_p2p() {
        let normalized = normalize_proxy_config(&json!({
            "enable": true,
            "server": "http://legacy.example:8080",
            "p2p": {
                "enable": true,
                "server": "socks5://p2p.example:1080"
            }
        }));

        assert_eq!(
            normalized.get("http").and_then(|v| v.get("server")),
            Some(&json!("http://legacy.example:8080"))
        );
        assert_eq!(
            normalized.get("p2p").and_then(|v| v.get("server")),
            Some(&json!("socks5://p2p.example:1080"))
        );
    }

    #[test]
    fn normalize_proxy_config_merges_partial_nested_http_over_legacy() {
        let normalized = normalize_proxy_config(&json!({
            "enable": true,
            "server": "http://legacy.example:8080",
            "bypass": "legacy.example",
            "scope": ["download", "update-app"],
            "http": { "bypass": "nested.example" }
        }));

        assert_eq!(
            normalized.get("http"),
            Some(&json!({
                "enable": true,
                "server": "http://legacy.example:8080",
                "bypass": "nested.example",
                "scope": ["download", "update-app"]
            }))
        );
    }

    #[test]
    fn normalize_proxy_config_keeps_explicit_empty_scope_disabled() {
        let normalized = normalize_proxy_config(&json!({
            "http": { "scope": [], "bypass": "invalid host, ," },
            "p2p": { "bypass": "example.com\nexample.com" }
        }));

        assert_eq!(
            normalized.get("http").and_then(|v| v.get("scope")),
            Some(&json!([]))
        );
        assert_eq!(
            normalized.get("http").and_then(|v| v.get("bypass")),
            Some(&json!(""))
        );
        assert_eq!(
            normalized.get("p2p").and_then(|v| v.get("bypass")),
            Some(&json!("example.com"))
        );
    }

    #[test]
    fn normalize_proxy_config_treats_null_scope_as_default() {
        let normalized = normalize_proxy_config(&json!({
            "http": { "scope": null }
        }));

        assert_eq!(
            normalized.get("http").and_then(|http| http.get("scope")),
            Some(&json!(["download", "update-app", "update-trackers"]))
        );
    }

    #[test]
    fn config_manager_persists_legacy_proxy_migration() {
        let dir = TempDir::new().unwrap();
        let legacy = json!({
            "proxy": {
                "enable": true,
                "server": "http://proxy.example:8080",
                "bypass": "localhost",
                "scope": ["download"]
            },
            "theme": "dark"
        });
        std::fs::write(
            dir.path().join("user.json"),
            serde_json::to_vec_pretty(&legacy).unwrap(),
        )
        .unwrap();

        let manager = ConfigManager::with_dir(dir.path().to_path_buf()).unwrap();
        assert_eq!(
            manager
                .get_user_config()
                .get("proxy")
                .and_then(|proxy| proxy.get("http"))
                .and_then(|http| http.get("server")),
            Some(&json!("http://proxy.example:8080"))
        );
        let persisted: Value =
            serde_json::from_str(&std::fs::read_to_string(dir.path().join("user.json")).unwrap())
                .unwrap();
        assert!(persisted
            .get("proxy")
            .and_then(|proxy| proxy.get("http"))
            .is_some());
        assert!(persisted
            .get("proxy")
            .and_then(|proxy| proxy.get("enable"))
            .is_none());
    }

    // -- ConfigManager --

    #[test]
    fn config_manager_with_dir_uses_defaults_when_missing() {
        let dir = TempDir::new().unwrap();
        let mgr = ConfigManager::with_dir(dir.path().to_path_buf()).unwrap();
        // Defaults should be present when files are missing
        assert!(mgr.get_system_config().contains_key("all-proxy"));
        assert!(mgr.get_user_config().contains_key("theme"));
    }

    #[test]
    fn get_merged_config_combines_system_and_user() {
        let dir = TempDir::new().unwrap();
        let mut mgr = ConfigManager::with_dir(dir.path().to_path_buf()).unwrap();
        mgr.set_system_config_map(&serde_json::from_str(r#"{"system-key": "sys-val"}"#).unwrap())
            .unwrap();
        mgr.set_user_config_map(&serde_json::from_str(r#"{"user-key": "user-val"}"#).unwrap())
            .unwrap();

        let merged = mgr.get_merged_config();
        let map = merged.as_object().unwrap();
        assert_eq!(map.get("system-key"), Some(&json!("sys-val")));
        assert_eq!(map.get("user-key"), Some(&json!("user-val")));
        assert!(map.contains_key("platform"));
        assert!(map.contains_key("arch"));
    }

    #[test]
    fn user_config_overrides_system_config() {
        let dir = TempDir::new().unwrap();
        let mut mgr = ConfigManager::with_dir(dir.path().to_path_buf()).unwrap();
        mgr.set_system_config_map(&serde_json::from_str(r#"{"shared": "system"}"#).unwrap())
            .unwrap();
        mgr.set_user_config_map(&serde_json::from_str(r#"{"shared": "user"}"#).unwrap())
            .unwrap();

        let merged = mgr.get_merged_config();
        let map = merged.as_object().unwrap();
        assert_eq!(map.get("shared"), Some(&json!("user")));
    }

    #[test]
    fn set_system_config_map_persists() {
        let dir = TempDir::new().unwrap();
        {
            let mut mgr = ConfigManager::with_dir(dir.path().to_path_buf()).unwrap();
            mgr.set_system_config_map(&serde_json::from_str(r#"{"persisted": true}"#).unwrap())
                .unwrap();
        }
        // Re-open and verify
        let mgr2 = ConfigManager::with_dir(dir.path().to_path_buf()).unwrap();
        assert_eq!(
            mgr2.get_system_config().get("persisted"),
            Some(&json!(true))
        );
    }

    #[test]
    fn set_user_config_map_persists() {
        let dir = TempDir::new().unwrap();
        {
            let mut mgr = ConfigManager::with_dir(dir.path().to_path_buf()).unwrap();
            mgr.set_user_config_map(&serde_json::from_str(r#"{"locale": "zh-CN"}"#).unwrap())
                .unwrap();
        }
        let mgr2 = ConfigManager::with_dir(dir.path().to_path_buf()).unwrap();
        assert_eq!(mgr2.get_user_config().get("locale"), Some(&json!("zh-CN")));
    }

    #[test]
    fn remove_system_config_key() {
        let dir = TempDir::new().unwrap();
        let mut mgr = ConfigManager::with_dir(dir.path().to_path_buf()).unwrap();
        mgr.set_system_config_map(&serde_json::from_str(r#"{"a": 1, "b": 2}"#).unwrap())
            .unwrap();
        mgr.remove_system_config_key("a").unwrap();
        assert!(!mgr.get_system_config().contains_key("a"));
        assert!(mgr.get_system_config().contains_key("b"));
        // Verify persistence by reopening
        let mgr2 = ConfigManager::with_dir(dir.path().to_path_buf()).unwrap();
        assert!(!mgr2.get_system_config().contains_key("a"));
        assert!(mgr2.get_system_config().contains_key("b"));
    }

    #[test]
    fn reset_restores_defaults() {
        let dir = TempDir::new().unwrap();
        let mut mgr = ConfigManager::with_dir(dir.path().to_path_buf()).unwrap();
        mgr.set_user_config_map(&serde_json::from_str(r#"{"locale": "custom"}"#).unwrap())
            .unwrap();
        mgr.set_system_config_map(&serde_json::from_str(r#"{"dir": "/tmp"}"#).unwrap())
            .unwrap();
        mgr.reset().unwrap();

        let merged = mgr.get_merged_config();
        let map = merged.as_object().unwrap();
        assert_eq!(map.get("locale"), Some(&json!("auto")));
    }

    #[test]
    fn load_or_default_fills_missing_keys() {
        let dir = TempDir::new().unwrap();
        let user_path = dir.path().join("user.json");
        fs::write(&user_path, r#"{"locale": "fr-FR"}"#).unwrap();

        let mgr = ConfigManager::with_dir(dir.path().to_path_buf()).unwrap();
        let user = mgr.get_user_config();
        assert_eq!(user.get("locale"), Some(&json!("fr-FR")));
        // theme should be filled from defaults
        assert!(user.contains_key("theme"));
    }
}
