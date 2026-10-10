use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::engine::util::now_secs;

const STORE_FILE: &str = "browser_cookies.json";
const MAX_ENTRIES: usize = 200;
const TOUCH_MIN_INTERVAL_SECS: u64 = 3600;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct StoredCookie {
    pub name: String,
    pub value: String,
    pub domain: String,
    pub path: String,
    pub secure: bool,
    pub http_only: bool,
    pub expires: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CookieEntry {
    pub host: String,
    pub browser_id: String,
    pub user_agent: String,
    pub cookies: Vec<StoredCookie>,
    pub imported_at: u64,
    pub last_validated_at: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StoreFile {
    entries: HashMap<String, CookieEntry>,
}

pub struct CookieStore {
    path: PathBuf,
    state: RwLock<StoreFile>,
}

impl CookieStore {
    pub fn new(config_dir: &Path) -> Self {
        let path = config_dir.join(STORE_FILE);
        let state = match load_from_disk(&path) {
            Ok(s) => s,
            Err(err) => {
                tracing::warn!(
                    "cookie store load failed for {}: {err}. Backing up to .corrupt and starting empty",
                    path.display()
                );
                if path.exists() {
                    let backup = path.with_extension(format!("json.corrupt-{}", now_secs()));
                    if let Err(rename_err) = std::fs::rename(&path, &backup) {
                        tracing::error!(
                            "cookie store backup failed (rename {} -> {}): {rename_err}; refusing to overwrite",
                            path.display(),
                            backup.display()
                        );
                        return Self {
                            path: backup,
                            state: RwLock::new(StoreFile::default()),
                        };
                    }
                    tracing::info!("cookie store backup created at {}", backup.display());
                }
                StoreFile::default()
            }
        };
        Self {
            path,
            state: RwLock::new(state),
        }
    }

    pub fn find_for_url(&self, target_url: &str) -> Option<CookieEntry> {
        let host = host_of(target_url)?;
        let s = self.state.read();
        if let Some(entry) = s.entries.get(&host) {
            return Some(entry.clone());
        }
        let mut best: Option<&CookieEntry> = None;
        for entry in s.entries.values() {
            if host_matches(&host, &entry.host) {
                best = match best {
                    Some(prev) if prev.host.len() >= entry.host.len() => Some(prev),
                    _ => Some(entry),
                };
            }
        }
        best.cloned()
    }

    pub fn upsert(&self, mut entry: CookieEntry) -> Result<(), String> {
        let now = now_secs();
        if entry.imported_at == 0 {
            entry.imported_at = now;
        }
        entry.last_validated_at = now;

        let key = entry.host.to_ascii_lowercase();
        entry.host = key.clone();
        let mut s = self.state.write();
        s.entries.insert(key, entry);

        if s.entries.len() > MAX_ENTRIES {
            let mut by_age: Vec<(String, u64)> = s
                .entries
                .iter()
                .map(|(k, v)| (k.clone(), v.last_validated_at))
                .collect();
            by_age.sort_by_key(|(_, t)| *t);
            let to_drop = s.entries.len() - MAX_ENTRIES;
            for (host, _) in by_age.into_iter().take(to_drop) {
                s.entries.remove(&host);
            }
        }
        write_to_disk(&self.path, &s)
    }

    pub fn touch(&self, host: &str) {
        let host = host.to_ascii_lowercase();
        let mut s = self.state.write();
        if let Some(entry) = s.entries.get_mut(&host) {
            let now = now_secs();
            if now.saturating_sub(entry.last_validated_at) < TOUCH_MIN_INTERVAL_SECS {
                return;
            }
            entry.last_validated_at = now;
            let _ = write_to_disk(&self.path, &s);
        }
    }

    pub fn remove(&self, host: &str) -> Result<bool, String> {
        let host = host.to_ascii_lowercase();
        let mut s = self.state.write();
        let removed = s.entries.remove(&host).is_some();
        if removed {
            write_to_disk(&self.path, &s)?;
        }
        Ok(removed)
    }

    pub fn list(&self) -> Vec<CookieEntry> {
        let s = self.state.read();
        let mut entries: Vec<_> = s.entries.values().cloned().collect();
        entries.sort_by_key(|b| std::cmp::Reverse(b.last_validated_at));
        entries
    }

    pub fn clear(&self) -> Result<(), String> {
        let mut s = self.state.write();
        s.entries.clear();
        write_to_disk(&self.path, &s)
    }
}

fn host_of(target_url: &str) -> Option<String> {
    if let Ok(url) = url::Url::parse(target_url) {
        return url.host_str().map(|s| s.to_ascii_lowercase());
    }
    let trimmed = target_url
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let host = trimmed.split('/').next()?.split(':').next()?;
    if host.is_empty() {
        None
    } else {
        Some(host.to_ascii_lowercase())
    }
}

fn host_matches(request_host: &str, entry_host: &str) -> bool {
    let r = request_host.to_ascii_lowercase();
    let e = entry_host.to_ascii_lowercase();
    if r == e {
        return true;
    }
    r.ends_with(&format!(".{e}"))
}

fn load_from_disk(path: &Path) -> Result<StoreFile, String> {
    if !path.exists() {
        return Ok(StoreFile::default());
    }
    let data =
        std::fs::read_to_string(path).map_err(|e| format!("read cookie store failed: {e}"))?;
    serde_json::from_str(&data).map_err(|e| format!("parse cookie store failed: {e}"))
}

fn write_to_disk(path: &Path, state: &StoreFile) -> Result<(), String> {
    let json = serde_json::to_string_pretty(state)
        .map_err(|e| format!("serialize cookie store failed: {e}"))?;
    crate::traits::write_file_atomically(path, json.as_bytes())
        .map_err(|e| format!("write cookie store failed: {e}"))
}

pub fn eligible_cookies(cookies: &[StoredCookie]) -> Vec<&StoredCookie> {
    let now = now_secs();
    cookies
        .iter()
        .filter(|c| c.expires.is_none_or(|exp| exp > now))
        .filter(|c| is_safe_cookie_field(&c.name) && is_safe_cookie_field(&c.value))
        .collect()
}

pub fn cookies_to_header(cookies: &[StoredCookie]) -> String {
    eligible_cookies(cookies)
        .into_iter()
        .map(|c| format!("{}={}", c.name, c.value))
        .collect::<Vec<_>>()
        .join("; ")
}

fn is_safe_cookie_field(s: &str) -> bool {
    !s.chars().any(|c| c.is_control() || c == ';')
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn dummy_entry(host: &str) -> CookieEntry {
        CookieEntry {
            host: host.to_string(),
            browser_id: "chrome".into(),
            user_agent: "ua".into(),
            cookies: vec![StoredCookie {
                name: "cf_clearance".into(),
                value: "abc".into(),
                domain: format!(".{host}"),
                path: "/".into(),
                secure: true,
                http_only: false,
                expires: None,
            }],
            imported_at: 0,
            last_validated_at: 0,
        }
    }

    #[test]
    fn upsert_and_find_exact() {
        let dir = TempDir::new().unwrap();
        let store = CookieStore::new(dir.path());
        store.upsert(dummy_entry("example.com")).unwrap();
        let hit = store.find_for_url("https://example.com/foo").unwrap();
        assert_eq!(hit.host, "example.com");
        assert_eq!(hit.cookies[0].name, "cf_clearance");
    }

    #[test]
    fn find_for_url_subdomain() {
        let dir = TempDir::new().unwrap();
        let store = CookieStore::new(dir.path());
        store.upsert(dummy_entry("example.com")).unwrap();
        let hit = store.find_for_url("https://dl.example.com/file.zip");
        assert!(hit.is_some());
    }

    #[test]
    fn find_for_url_unrelated_host_misses() {
        let dir = TempDir::new().unwrap();
        let store = CookieStore::new(dir.path());
        store.upsert(dummy_entry("example.com")).unwrap();
        assert!(store.find_for_url("https://attacker.com/foo").is_none());
        assert!(store.find_for_url("https://notexample.com/foo").is_none());
    }

    #[test]
    fn persist_and_reload() {
        let dir = TempDir::new().unwrap();
        {
            let store = CookieStore::new(dir.path());
            store.upsert(dummy_entry("example.com")).unwrap();
        }
        let store = CookieStore::new(dir.path());
        assert!(store.find_for_url("https://example.com").is_some());
    }

    #[test]
    fn remove_works() {
        let dir = TempDir::new().unwrap();
        let store = CookieStore::new(dir.path());
        store.upsert(dummy_entry("example.com")).unwrap();
        assert!(store.remove("example.com").unwrap());
        assert!(!store.remove("example.com").unwrap());
        assert!(store.find_for_url("https://example.com").is_none());
    }

    #[test]
    fn lru_eviction() {
        let dir = TempDir::new().unwrap();
        let store = CookieStore::new(dir.path());
        for i in 0..(MAX_ENTRIES + 5) {
            let mut e = dummy_entry(&format!("h{i}.example.com"));
            e.last_validated_at = i as u64;
            e.imported_at = i as u64;
            store.upsert(e).unwrap();
            store.state.write().entries.values_mut().for_each(|e| {
                if e.host == format!("h{i}.example.com") {
                    e.last_validated_at = i as u64;
                }
            });
        }
        let listed = store.list();
        assert!(listed.len() <= MAX_ENTRIES);
        assert!(store.find_for_url("https://h0.example.com").is_none());
    }

    #[test]
    fn cookies_to_header_format() {
        let cs = vec![
            StoredCookie {
                name: "a".into(),
                value: "1".into(),
                ..Default::default()
            },
            StoredCookie {
                name: "b".into(),
                value: "two".into(),
                ..Default::default()
            },
        ];
        assert_eq!(cookies_to_header(&cs), "a=1; b=two");
    }

    #[test]
    fn cookies_to_header_drops_injection_and_expired() {
        let now = now_secs();
        let cs = vec![
            StoredCookie {
                name: "evil".into(),
                value: "x\r\nX-Injected: 1".into(),
                ..Default::default()
            },
            StoredCookie {
                name: "split".into(),
                value: "a; admin=1".into(),
                ..Default::default()
            },
            StoredCookie {
                name: "stale".into(),
                value: "1".into(),
                expires: Some(now.saturating_sub(10)),
                ..Default::default()
            },
            StoredCookie {
                name: "good".into(),
                value: "ok".into(),
                expires: Some(now + 3600),
                ..Default::default()
            },
        ];
        assert_eq!(cookies_to_header(&cs), "good=ok");
        assert_eq!(
            eligible_cookies(&cs)
                .into_iter()
                .map(|cookie| cookie.name.as_str())
                .collect::<Vec<_>>(),
            vec!["good"]
        );
    }

    #[test]
    fn corrupted_store_is_backed_up_not_overwritten() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(STORE_FILE);
        std::fs::write(&path, b"{not valid json").unwrap();

        let store = CookieStore::new(dir.path());
        assert!(store.list().is_empty());
        assert!(
            !path.exists(),
            "original corrupt file should be moved aside"
        );

        let backups: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with("browser_cookies.json.corrupt-")
            })
            .collect();
        assert_eq!(backups.len(), 1, "expected exactly one .corrupt-* backup");

        store.upsert(dummy_entry("example.com")).unwrap();
        assert!(path.exists());
        assert!(backups[0].path().exists());
    }

    #[test]
    fn touch_only_restamps_stale_entries() {
        let dir = TempDir::new().unwrap();
        let store = CookieStore::new(dir.path());
        store.upsert(dummy_entry("example.com")).unwrap();
        let stamp =
            |store: &CookieStore| store.state.read().entries["example.com"].last_validated_at;
        let fresh = stamp(&store);
        store
            .state
            .write()
            .entries
            .get_mut("example.com")
            .unwrap()
            .last_validated_at = fresh - 10;
        store.touch("example.com");
        assert_eq!(
            stamp(&store),
            fresh - 10,
            "fresh stamp must not be rewritten"
        );

        store
            .state
            .write()
            .entries
            .get_mut("example.com")
            .unwrap()
            .last_validated_at = fresh - 2 * TOUCH_MIN_INTERVAL_SECS;
        store.touch("example.com");
        assert!(stamp(&store) >= fresh);
    }
}
