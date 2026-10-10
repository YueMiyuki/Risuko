use crate::utils::host::{cookie_covers_host, host_key_candidates};
use crate::utils::{paths, time};
use eyre::Result;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone)]
pub struct Cookie {
    pub name: String,
    pub value: String,
    pub domain: String,
    pub path: String,
    pub secure: bool,
    pub http_only: bool,
    pub expires: Option<u64>,
}

pub struct BrowserConfig {
    pub cookie_paths: Vec<&'static str>,
    #[cfg_attr(not(target_os = "macos"), allow(dead_code))]
    pub keychain: (&'static str, &'static str),
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    pub secret_app: &'static str,
}

const HOST_HASH_DB_VERSION: i64 = 24;
const HOST_HASH_LEN: usize = 32;

fn strip_host_hash(mut plain: Vec<u8>, encrypted: &[u8], db_version: i64) -> Option<Vec<u8>> {
    let prefixed = encrypted.len() >= 3 && matches!(&encrypted[..3], b"v10" | b"v11" | b"v20");
    if !prefixed || db_version < HOST_HASH_DB_VERSION {
        return Some(plain);
    }
    if plain.len() < HOST_HASH_LEN {
        return None;
    }
    plain.drain(..HOST_HASH_LEN);
    Some(plain)
}

impl BrowserConfig {
    pub fn chrome() -> Self {
        Self {
            cookie_paths: if cfg!(windows) {
                vec![
                    "%LOCALAPPDATA%/Google/Chrome/User Data/*/Cookies",
                    "%LOCALAPPDATA%/Google/Chrome/User Data/*/Network/Cookies",
                ]
            } else if cfg!(target_os = "macos") {
                vec![
                    "~/Library/Application Support/Google/Chrome/*/Cookies",
                    "~/Library/Application Support/Google/Chrome/*/Network/Cookies",
                ]
            } else {
                vec!["~/.config/google-chrome/*/Cookies"]
            },
            keychain: ("Chrome Safe Storage", "Chrome"),
            secret_app: "chrome",
        }
    }

    pub fn edge() -> Self {
        Self {
            cookie_paths: if cfg!(windows) {
                vec![
                    "%LOCALAPPDATA%/Microsoft/Edge/User Data/*/Cookies",
                    "%LOCALAPPDATA%/Microsoft/Edge/User Data/*/Network/Cookies",
                ]
            } else if cfg!(target_os = "macos") {
                vec![
                    "~/Library/Application Support/Microsoft Edge/*/Cookies",
                    "~/Library/Application Support/Microsoft Edge/*/Network/Cookies",
                ]
            } else {
                vec!["~/.config/microsoft-edge/*/Cookies"]
            },
            keychain: ("Microsoft Edge Safe Storage", "Microsoft Edge"),
            secret_app: "microsoft-edge",
        }
    }

    pub fn brave() -> Self {
        Self {
            cookie_paths: if cfg!(windows) {
                vec!["%LOCALAPPDATA%/BraveSoftware/Brave-Browser/User Data/*/Cookies"]
            } else if cfg!(target_os = "macos") {
                vec![
                    "~/Library/Application Support/BraveSoftware/Brave-Browser/*/Cookies",
                    "~/Library/Application Support/BraveSoftware/Brave-Browser/*/Network/Cookies",
                ]
            } else {
                vec!["~/.config/BraveSoftware/Brave-Browser/*/Cookies"]
            },
            keychain: ("Brave Safe Storage", "Brave"),
            secret_app: "brave",
        }
    }

    pub fn chromium() -> Self {
        Self {
            cookie_paths: if cfg!(windows) {
                vec!["%LOCALAPPDATA%/Chromium/User Data/*/Cookies"]
            } else if cfg!(target_os = "macos") {
                vec![
                    "~/Library/Application Support/Chromium/*/Cookies",
                    "~/Library/Application Support/Chromium/*/Network/Cookies",
                ]
            } else {
                vec!["~/.config/chromium/*/Cookies"]
            },
            keychain: ("Chromium Safe Storage", "Chromium"),
            secret_app: "chromium",
        }
    }

    pub fn vivaldi() -> Self {
        Self {
            cookie_paths: if cfg!(windows) {
                vec!["%LOCALAPPDATA%/Vivaldi/User Data/*/Cookies"]
            } else if cfg!(target_os = "macos") {
                vec![
                    "~/Library/Application Support/Vivaldi/*/Cookies",
                    "~/Library/Application Support/Vivaldi/*/Network/Cookies",
                ]
            } else {
                vec!["~/.config/vivaldi/*/Cookies"]
            },
            keychain: ("Vivaldi Safe Storage", "Vivaldi"),
            secret_app: "vivaldi",
        }
    }

    pub fn opera() -> Self {
        Self {
            cookie_paths: if cfg!(windows) {
                vec!["%APPDATA%/Opera Software/Opera Stable/Cookies"]
            } else if cfg!(target_os = "macos") {
                vec![
                    "~/Library/Application Support/com.operasoftware.Opera/Cookies",
                    "~/Library/Application Support/com.operasoftware.Opera/Network/Cookies",
                ]
            } else {
                vec!["~/.config/opera/Cookies"]
            },
            keychain: ("Opera Safe Storage", "Opera"),
            secret_app: "opera",
        }
    }

    #[cfg(target_os = "macos")]
    pub fn arc() -> Self {
        Self {
            cookie_paths: vec![
                "~/Library/Application Support/Arc/User Data/*/Cookies",
                "~/Library/Application Support/Arc/User Data/*/Network/Cookies",
            ],
            keychain: ("Arc Safe Storage", "Arc"),
            secret_app: "arc",
        }
    }
}

pub fn find_cookie_dbs(config: &BrowserConfig) -> Vec<PathBuf> {
    let mut all_dbs = Vec::new();
    for pattern in &config.cookie_paths {
        if let Ok(paths) = paths::find_matching(pattern) {
            for p in paths {
                if p.exists() {
                    all_dbs.push(p);
                }
            }
        }
    }
    all_dbs
}

pub fn is_available(config: &BrowserConfig) -> bool {
    !find_cookie_dbs(config).is_empty()
}

fn load_master_key(config: &BrowserConfig, ls: &std::path::Path) -> Option<Vec<u8>> {
    #[cfg(target_os = "windows")]
    {
        let _ = config;
        let key = crate::platform::windows::extract_master_key(ls).ok();
        tracing::debug!(target: "risuko_cookies", "chromium: windows master_key len = {:?}", key.as_ref().map(|k| k.len()));
        key
    }
    #[cfg(target_os = "macos")]
    {
        let key = crate::platform::macos::extract_master_key(ls, config.keychain).ok();
        tracing::debug!(target: "risuko_cookies", "chromium: macos master_key len = {:?}", key.as_ref().map(|k| k.len()));
        key
    }
    #[cfg(target_os = "linux")]
    {
        let key = crate::platform::linux::extract_master_key(ls, config.secret_app).ok();
        tracing::debug!(target: "risuko_cookies", "chromium: linux master_key len = {:?}", key.as_ref().map(|k| k.len()));
        key
    }
    #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
    {
        let _ = (config, ls);
        tracing::debug!(target: "risuko_cookies", "chromium: unsupported platform, no master key");
        None
    }
}

pub fn extract_cookies(config: &BrowserConfig, host: Option<&str>) -> Result<Vec<Cookie>> {
    let all_dbs = find_cookie_dbs(config);
    if all_dbs.is_empty() {
        return Err(eyre::eyre!("cookie database not found"));
    }

    let mut keys: HashMap<PathBuf, Option<Vec<u8>>> = HashMap::new();
    let mut all_cookies = Vec::new();
    let mut last_err = None;
    let mut failed = 0usize;
    for cookie_db in &all_dbs {
        tracing::debug!(target: "risuko_cookies", "chromium: using db path {}", cookie_db.display());

        let local_state = paths::find_local_state(cookie_db);
        tracing::debug!(target: "risuko_cookies", "chromium: local_state path = {:?}", local_state.as_ref().map(|p| p.display().to_string()));

        let master_key = match local_state {
            Some(ls) => keys
                .entry(ls.clone())
                .or_insert_with(|| load_master_key(config, &ls))
                .as_deref(),
            None => {
                tracing::debug!(target: "risuko_cookies", "chromium: no local_state found, cookies will be raw");
                None
            }
        };

        match read_cookies_from_db(cookie_db, master_key, host) {
            Ok(mut cookies) => all_cookies.append(&mut cookies),
            Err(e) => {
                tracing::debug!(target: "risuko_cookies", "chromium: skipping db {}: {}", cookie_db.display(), e);
                failed += 1;
                last_err = Some(e);
            }
        }
    }
    if failed == all_dbs.len() {
        if let Some(e) = last_err {
            return Err(e);
        }
    }

    tracing::debug!(target: "risuko_cookies",
        "chromium: scanned {} db(s), host_filter={:?} -> total kept={}",
        all_dbs.len(), host, all_cookies.len()
    );

    Ok(all_cookies)
}

fn read_cookies_from_db(
    db_path: &std::path::Path,
    master_key: Option<&[u8]>,
    host: Option<&str>,
) -> Result<Vec<Cookie>> {
    let temp = paths::copy_db_snapshot(db_path)?;

    let conn = Connection::open(temp.db_path())?;

    let db_version: i64 = conn
        .query_row(
            "SELECT CAST(value AS INTEGER) FROM meta WHERE key = 'version'",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0);

    let total_count: i64 = conn.query_row("SELECT COUNT(*) FROM cookies", [], |row| row.get(0))?;
    tracing::debug!(target: "risuko_cookies", "chromium: total rows in db = {}", total_count);

    let mut sql = String::from(
        "SELECT name, encrypted_value, value, host_key, path, is_secure, is_httponly, expires_utc FROM cookies",
    );
    let candidates = host.map(host_key_candidates).unwrap_or_default();
    if host.is_some() {
        sql.push_str(" WHERE host_key IN (");
        sql.push_str(&vec!["?"; candidates.len()].join(","));
        sql.push(')');
    }
    let mut stmt = conn.prepare(&sql)?;
    let cookie_iter = stmt.query_map(rusqlite::params_from_iter(candidates.iter()), parse_row)?;

    let mut cookies = Vec::new();
    let mut skipped_empty = 0usize;
    let mut skipped_domain = 0usize;
    let mut decrypted_ok = 0usize;
    let mut decrypted_fail = 0usize;
    let mut no_key = 0usize;

    for raw in cookie_iter.flatten() {
        if raw.raw_value.is_empty() && raw.plaintext_value.is_empty() {
            skipped_empty += 1;
            continue;
        }

        if let Some(request_host) = host {
            if !cookie_covers_host(request_host, &raw.domain) {
                tracing::trace!(target: "risuko_cookies", "chromium: skip cookie '{}' host_key={} (does not cover {})", raw.name, raw.domain, request_host);
                skipped_domain += 1;
                continue;
            }
        }

        let value = if !raw.raw_value.is_empty() {
            if let Some(key) = master_key {
                match decrypt_cookie_value(&raw.raw_value, key) {
                    Ok(decrypted) => {
                        let plain = strip_host_hash(decrypted, &raw.raw_value, db_version)
                            .and_then(|p| String::from_utf8(p).ok());
                        match plain {
                            Some(v) => {
                                decrypted_ok += 1;
                                v
                            }
                            None => {
                                tracing::trace!(target: "risuko_cookies", "chromium: invalid plaintext for '{}' host_key={}", raw.name, raw.domain);
                                decrypted_fail += 1;
                                continue;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::trace!(target: "risuko_cookies", "chromium: decrypt failed for '{}' host_key={}: {}", raw.name, raw.domain, e);
                        decrypted_fail += 1;
                        continue;
                    }
                }
            } else {
                no_key += 1;
                if !raw.plaintext_value.is_empty() {
                    raw.plaintext_value
                } else {
                    continue;
                }
            }
        } else {
            decrypted_ok += 1;
            raw.plaintext_value
        };

        cookies.push(Cookie {
            name: raw.name,
            value,
            domain: raw.domain,
            path: raw.path,
            secure: raw.secure,
            http_only: raw.http_only,
            expires: raw.expires,
        });
    }

    tracing::debug!(target: "risuko_cookies",
        "chromium: host_filter={:?} -> kept={} (skipped_empty={} skipped_domain={} decrypted_ok={} decrypted_fail={} no_key={})",
        host, cookies.len(), skipped_empty, skipped_domain, decrypted_ok, decrypted_fail, no_key
    );

    for c in cookies.iter() {
        tracing::trace!(target: "risuko_cookies", "chromium: kept cookie name={} domain={} value_len={}", c.name, c.domain, c.value.len());
    }

    Ok(cookies)
}

struct RawCookie {
    name: String,
    raw_value: Vec<u8>,
    plaintext_value: String,
    domain: String,
    path: String,
    secure: bool,
    http_only: bool,
    expires: Option<u64>,
}

fn parse_row(row: &rusqlite::Row) -> rusqlite::Result<RawCookie> {
    Ok(RawCookie {
        name: row.get(0)?,
        raw_value: row.get::<_, Vec<u8>>(1)?,
        plaintext_value: row.get(2)?,
        domain: row.get(3)?,
        path: row.get(4)?,
        secure: row.get::<_, i32>(5)? != 0,
        http_only: row.get::<_, i32>(6)? != 0,
        expires: {
            let v: i64 = row.get(7)?;
            if v <= 0 {
                None
            } else {
                time::webkit_to_unix(v as u64)
            }
        },
    })
}

fn decrypt_cookie_value(encrypted: &[u8], master_key: &[u8]) -> Result<Vec<u8>> {
    #[cfg(target_os = "windows")]
    {
        crate::platform::windows::decrypt_value(encrypted, master_key)
    }

    #[cfg(target_os = "macos")]
    {
        crate::platform::macos::decrypt_value(encrypted, master_key)
    }

    #[cfg(target_os = "linux")]
    {
        crate::platform::linux::decrypt_value(encrypted, master_key)
    }

    #[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
    {
        let _ = (encrypted, master_key);
        eyre::bail!("unsupported platform")
    }
}

#[cfg(test)]
mod tests {
    use super::strip_host_hash;

    #[test]
    fn host_hash_stripped_for_v24_encrypted() {
        let mut plain = vec![7u8; 32];
        plain.extend_from_slice(b"value");
        assert_eq!(
            strip_host_hash(plain.clone(), b"v10xxxx", 24),
            Some(b"value".to_vec())
        );
        assert_eq!(
            strip_host_hash(plain.clone(), b"v10xxxx", 23),
            Some(plain.clone())
        );
        assert_eq!(strip_host_hash(plain.clone(), b"rawxxxx", 24), Some(plain));
        assert_eq!(strip_host_hash(vec![1; 5], b"v11xxxx", 24), None);
    }
}
