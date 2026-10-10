use std::path::{Path, PathBuf};

pub fn expand(path: &str) -> eyre::Result<PathBuf> {
    let expanded = if cfg!(windows) {
        expand_windows(path)
    } else {
        expand_unix(path)
    };
    Ok(PathBuf::from(expanded))
}

#[cfg(unix)]
fn expand_unix(path: &str) -> String {
    let home = std::env::var("HOME").unwrap_or_default();
    path.replace('~', &home).replace("$HOME", &home)
}

#[cfg(not(unix))]
fn expand_unix(_path: &str) -> String {
    String::new()
}

#[cfg(windows)]
fn expand_windows(path: &str) -> String {
    use regex::Regex;
    let re = Regex::new(r"%([^%]+)%").unwrap();
    let mut result = path.to_string();
    for cap in re.captures_iter(path) {
        if let Ok(val) = std::env::var(&cap[1]) {
            result = result.replace(&cap[0], &val);
        }
    }
    result
}

#[cfg(not(windows))]
fn expand_windows(_path: &str) -> String {
    String::new()
}

pub fn find_matching(pattern: &str) -> eyre::Result<Vec<PathBuf>> {
    let expanded = expand(pattern)?;
    let path_str = expanded
        .to_str()
        .ok_or_else(|| eyre::eyre!("invalid path encoding"))?;

    let mut results = Vec::new();
    for p in glob::glob(path_str)?.flatten() {
        results.push(p);
    }
    Ok(results)
}

#[cfg(target_os = "macos")]
pub fn find_first_existing(patterns: &[&str]) -> Option<PathBuf> {
    for pattern in patterns {
        if let Ok(paths) = find_matching(pattern) {
            for p in paths {
                if p.exists() {
                    return Some(p);
                }
            }
        }
    }
    None
}

pub fn find_local_state(cookie_db: &Path) -> Option<PathBuf> {
    let parent = cookie_db.parent()?;

    for rel in ["../../Local State", "../Local State", "Local State"] {
        let candidate = parent.join(rel);
        if candidate.exists() {
            return candidate.canonicalize().ok();
        }
    }

    None
}

pub struct DbSnapshot {
    dir: tempfile::TempDir,
    name: std::ffi::OsString,
}

impl DbSnapshot {
    pub fn db_path(&self) -> PathBuf {
        self.dir.path().join(&self.name)
    }
}

const SNAPSHOT_ATTEMPTS: usize = 3;

type Fingerprint = Option<(u64, std::time::SystemTime)>;

fn fingerprint(path: &Path) -> Fingerprint {
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.len(), meta.modified().ok()?))
}

pub fn copy_db_snapshot(src: &Path) -> eyre::Result<DbSnapshot> {
    let name = src
        .file_name()
        .ok_or_else(|| eyre::eyre!("invalid db path"))?
        .to_owned();
    let mut wal = name.clone();
    wal.push("-wal");
    let wal_src = src.with_file_name(&wal);
    let dir = tempfile::tempdir()?;
    let db_dst = dir.path().join(&name);
    let wal_dst = dir.path().join(&wal);

    let mut stable = false;
    for _ in 0..SNAPSHOT_ATTEMPTS {
        let before = (fingerprint(src), fingerprint(&wal_src));
        std::fs::copy(src, &db_dst)?;
        let _ = std::fs::remove_file(&wal_dst);
        if wal_src.exists() {
            let _ = std::fs::copy(&wal_src, &wal_dst);
        }
        stable = before == (fingerprint(src), fingerprint(&wal_src));
        if stable {
            break;
        }
    }
    if !stable {
        tracing::debug!(target: "risuko_cookies", "db changed while copying {}", src.display());
    }
    Ok(DbSnapshot { dir, name })
}

#[cfg(test)]
mod tests {
    use super::{copy_db_snapshot, fingerprint};

    #[test]
    fn snapshot_includes_wal_sidecar() {
        let src = tempfile::tempdir().unwrap();
        let db = src.path().join("cookies.sqlite");
        std::fs::write(&db, b"db").unwrap();
        std::fs::write(src.path().join("cookies.sqlite-wal"), b"wal").unwrap();
        let snap = copy_db_snapshot(&db).unwrap();
        let copy = snap.db_path();
        assert_eq!(std::fs::read(&copy).unwrap(), b"db");
        assert_eq!(
            std::fs::read(copy.with_file_name("cookies.sqlite-wal")).unwrap(),
            b"wal"
        );
    }

    #[test]
    fn snapshot_without_wal_has_no_sidecar() {
        let src = tempfile::tempdir().unwrap();
        let db = src.path().join("cookies.sqlite");
        std::fs::write(&db, b"db").unwrap();
        let snap = copy_db_snapshot(&db).unwrap();
        assert!(!snap.db_path().with_file_name("cookies.sqlite-wal").exists());
    }

    #[test]
    fn fingerprint_tracks_size_changes() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("f");
        assert!(fingerprint(&file).is_none());
        std::fs::write(&file, b"a").unwrap();
        let first = fingerprint(&file);
        std::fs::write(&file, b"ab").unwrap();
        assert_ne!(first, fingerprint(&file));
    }
}
