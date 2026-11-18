use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::RESUME_META_SUFFIX;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ResumeMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mtime: Option<i64>,
}

fn differs<T: PartialEq>(saved: &Option<T>, current: &Option<T>) -> bool {
    matches!((saved, current), (Some(saved), Some(current)) if saved != current)
}

impl ResumeMeta {
    pub fn is_empty(&self) -> bool {
        self.etag.is_none()
            && self.last_modified.is_none()
            && self.size.is_none()
            && self.mtime.is_none()
    }

    pub fn conflicts_with(&self, current: &ResumeMeta) -> bool {
        differs(&self.etag, &current.etag)
            || differs(&self.last_modified, &current.last_modified)
            || differs(&self.size, &current.size)
            || differs(&self.mtime, &current.mtime)
    }
}

pub(crate) fn path(part_path: &Path) -> PathBuf {
    let mut p = part_path.as_os_str().to_owned();
    p.push(RESUME_META_SUFFIX);
    PathBuf::from(p)
}

fn remove_if_present(path: &Path) {
    if let Err(e) = fs::remove_file(path) {
        if e.kind() != io::ErrorKind::NotFound {
            tracing::warn!("Failed to remove resume record {}: {e}", path.display());
        }
    }
}

pub(crate) fn load(part_path: &Path) -> Option<ResumeMeta> {
    let data = fs::read(path(part_path)).ok()?;
    serde_json::from_slice(&data).ok()
}

pub(crate) fn save(part_path: &Path, meta: &ResumeMeta) {
    if meta.is_empty() {
        delete(part_path);
        return;
    }
    let record = path(part_path);
    let written = serde_json::to_vec(meta)
        .map_err(|e| e.to_string())
        .and_then(|json| crate::traits::write_file_atomically(&record, &json));
    if let Err(e) = written {
        tracing::warn!("Failed to save resume record {}: {e}", record.display());
    }
}

pub(crate) fn delete(part_path: &Path) {
    let record = path(part_path);
    let mut tmp = record.clone().into_os_string();
    tmp.push(".tmp");
    remove_if_present(&record);
    remove_if_present(Path::new(&tmp));
}

pub(crate) fn relocate(old_part: &Path, new_part: &Path) {
    let from = path(old_part);
    let to = path(new_part);
    if from == to || !from.exists() {
        return;
    }
    remove_if_present(&to);
    if fs::rename(&from, &to).is_ok() {
        return;
    }
    if let Err(e) = fs::copy(&from, &to) {
        tracing::warn!("Failed to move resume record {}: {e}", from.display());
        remove_if_present(&to);
    }
    remove_if_present(&from);
}

pub(crate) fn should_restart(part_path: &Path, existing_size: u64, current: &ResumeMeta) -> bool {
    existing_size > 0 && load(part_path).is_some_and(|saved| saved.conflicts_with(current))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fields() -> [fn(u8) -> ResumeMeta; 4] {
        [
            |v| ResumeMeta {
                etag: Some(format!("\"v{v}\"")),
                ..Default::default()
            },
            |v| ResumeMeta {
                last_modified: Some(format!("Tue, 0{v} Nov 2026 10:00:00 GMT")),
                ..Default::default()
            },
            |v| ResumeMeta {
                size: Some(u64::from(v)),
                ..Default::default()
            },
            |v| ResumeMeta {
                mtime: Some(i64::from(v)),
                ..Default::default()
            },
        ]
    }

    fn full() -> ResumeMeta {
        ResumeMeta {
            etag: Some("\"v1\"".to_string()),
            last_modified: Some("Tue, 01 Nov 2026 10:00:00 GMT".to_string()),
            size: Some(4096),
            mtime: Some(1_793_000_000),
        }
    }

    #[test]
    fn save_then_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("file.bin.part");

        save(&part, &full());

        assert!(dir.path().join("file.bin.part.resume").exists());
        assert_eq!(load(&part), Some(full()));
    }

    #[test]
    fn saving_an_empty_record_deletes_the_existing_one() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("file.bin.part");
        save(&part, &full());
        std::fs::write(dir.path().join("file.bin.part.resume.tmp"), b"{}").unwrap();

        save(&part, &ResumeMeta::default());

        assert!(!path(&part).exists());
        assert!(!dir.path().join("file.bin.part.resume.tmp").exists());
        assert_eq!(load(&part), None);
    }

    #[test]
    fn load_returns_none_for_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("file.bin.part");
        std::fs::write(path(&part), b"not json").unwrap();

        assert_eq!(load(&part), None);
        assert_eq!(load(&dir.path().join("missing.part")), None);
    }

    #[test]
    fn conflicts_with_needs_the_field_on_both_sides() {
        for field in fields() {
            assert!(!field(1).conflicts_with(&ResumeMeta::default()));
            assert!(!ResumeMeta::default().conflicts_with(&field(1)));
        }
    }

    #[test]
    fn conflicts_with_is_false_for_equal_fields() {
        for field in fields() {
            assert!(!field(1).conflicts_with(&field(1)));
        }
        assert!(!full().conflicts_with(&full()));
    }

    #[test]
    fn conflicts_with_is_true_for_each_differing_field() {
        for field in fields() {
            assert!(field(1).conflicts_with(&field(2)));
        }
    }

    #[test]
    fn relocate_moves_the_record_and_replaces_a_stale_one() {
        let dir = tempfile::tempdir().unwrap();
        let old_part = dir.path().join("old.bin.part");
        let new_part = dir.path().join("new.bin.part");
        save(&old_part, &full());
        std::fs::write(path(&new_part), b"{\"etag\":\"\\\"stale\\\"\"}").unwrap();

        relocate(&old_part, &new_part);

        assert!(!path(&old_part).exists());
        assert_eq!(load(&new_part), Some(full()));
    }

    #[test]
    fn should_restart_is_false_without_a_record() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("file.bin.part");

        assert!(!should_restart(&part, 100, &full()));
    }

    #[test]
    fn should_restart_is_false_for_a_matching_record() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("file.bin.part");
        save(&part, &full());

        assert!(!should_restart(&part, 100, &full()));
    }

    #[test]
    fn should_restart_is_true_when_the_size_changed() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("file.bin.part");
        save(&part, &full());
        let current = ResumeMeta {
            size: Some(8192),
            ..full()
        };

        assert!(should_restart(&part, 100, &current));
        assert!(!should_restart(&part, 0, &current));
    }

    #[test]
    fn should_restart_is_true_when_the_mtime_changed() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("file.bin.part");
        save(&part, &full());
        let current = ResumeMeta {
            mtime: Some(1_793_000_001),
            ..full()
        };

        assert!(should_restart(&part, 100, &current));
    }

    #[test]
    fn should_restart_ignores_fields_only_one_side_has() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("file.bin.part");
        save(
            &part,
            &ResumeMeta {
                size: Some(4096),
                ..Default::default()
            },
        );
        let current = ResumeMeta {
            mtime: Some(1_793_000_000),
            ..Default::default()
        };

        assert!(!should_restart(&part, 100, &current));
    }
}
