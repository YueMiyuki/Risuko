use crate::browser::chromium::Cookie;
use crate::utils::host::{cookie_covers_host, host_key_candidates};
use crate::utils::paths;
use eyre::{bail, Result};
use rusqlite::Connection;

pub struct BrowserConfig {
    pub cookie_paths: Vec<&'static str>,
}

impl BrowserConfig {
    pub fn firefox() -> Self {
        Self {
            cookie_paths: if cfg!(windows) {
                vec!["%APPDATA%/Mozilla/Firefox/Profiles/*/cookies.sqlite"]
            } else if cfg!(target_os = "macos") {
                vec!["~/Library/Application Support/Firefox/Profiles/*/cookies.sqlite"]
            } else {
                vec!["~/.mozilla/firefox/*/cookies.sqlite"]
            },
        }
    }

    pub fn librewolf() -> Self {
        Self {
            cookie_paths: if cfg!(windows) {
                vec!["%APPDATA%/LibreWolf/Profiles/*/cookies.sqlite"]
            } else if cfg!(target_os = "macos") {
                vec!["~/Library/Application Support/LibreWolf/Profiles/*/cookies.sqlite"]
            } else {
                vec!["~/.librewolf/*/cookies.sqlite"]
            },
        }
    }

    pub fn zen() -> Self {
        Self {
            cookie_paths: if cfg!(windows) {
                vec!["%APPDATA%/Zen/Profiles/*/cookies.sqlite"]
            } else if cfg!(target_os = "macos") {
                vec!["~/Library/Application Support/Zen/Profiles/*/cookies.sqlite"]
            } else {
                vec!["~/.zen/*/cookies.sqlite"]
            },
        }
    }
}

fn find_cookie_dbs(config: &BrowserConfig) -> Vec<std::path::PathBuf> {
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

fn read_db(cookie_db: &std::path::Path, host: Option<&str>) -> Result<Vec<Cookie>> {
    // Copy because Firefox keeps the DB locked
    let temp = paths::copy_db_snapshot(cookie_db)?;
    let conn = Connection::open(temp.db_path())?;

    let mut sql = String::from(
        "SELECT name, value, host, path, isSecure, isHttpOnly, expiry FROM moz_cookies",
    );
    let candidates = host.map(host_key_candidates).unwrap_or_default();
    if host.is_some() {
        sql.push_str(" WHERE host IN (");
        sql.push_str(&vec!["?"; candidates.len()].join(","));
        sql.push(')');
    }
    let mut stmt = conn.prepare(&sql)?;
    let cookie_iter = stmt.query_map(rusqlite::params_from_iter(candidates.iter()), parse_row)?;

    let mut cookies = Vec::new();
    for cookie in cookie_iter.flatten() {
        if let Some(request_host) = host {
            if !cookie_covers_host(request_host, &cookie.domain) {
                tracing::trace!(target: "risuko_cookies", "firefox: skip cookie '{}' host={} (does not cover {})", cookie.name, cookie.domain, request_host);
                continue;
            }
        }
        tracing::trace!(target: "risuko_cookies", "firefox: keep cookie '{}' host={} value_len={}", cookie.name, cookie.domain, cookie.value.len());
        cookies.push(cookie);
    }
    Ok(cookies)
}

pub fn extract_cookies(config: &BrowserConfig, host: Option<&str>) -> Result<Vec<Cookie>> {
    let all_dbs = find_cookie_dbs(config);
    if all_dbs.is_empty() {
        bail!("firefox cookie database not found");
    }

    let mut cookies = Vec::new();
    let mut failed = 0usize;
    let mut last_err = None;
    for cookie_db in &all_dbs {
        tracing::debug!(target: "risuko_cookies", "firefox: using db path {}", cookie_db.display());
        match read_db(cookie_db, host) {
            Ok(mut c) => cookies.append(&mut c),
            Err(e) => {
                tracing::debug!(target: "risuko_cookies", "firefox: skipping db {}: {}", cookie_db.display(), e);
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
        "firefox: scanned {} db(s), host_filter={:?} -> kept={}",
        all_dbs.len(), host, cookies.len()
    );

    Ok(cookies)
}

fn parse_row(row: &rusqlite::Row) -> rusqlite::Result<Cookie> {
    let expiry: i64 = row.get(6)?;
    let expires = if expiry > 0 {
        Some(expiry as u64)
    } else {
        None
    };

    Ok(Cookie {
        name: row.get(0)?,
        value: row.get(1)?,
        domain: row.get(2)?,
        path: row.get(3)?,
        secure: row.get::<_, i32>(4)? != 0,
        http_only: row.get::<_, i32>(5)? != 0,
        expires,
    })
}
