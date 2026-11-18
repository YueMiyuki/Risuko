use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

const PART_SUFFIX: &str = ".part";
const PART_CLAIM_WAIT: Duration = Duration::from_secs(5);
const PART_CLAIM_POLL: Duration = Duration::from_millis(50);
const MAX_NUMBERED_NAMES: u32 = 1000;
pub(crate) const RESERVED_PARTS_OPTION: &str = "risuko-reserved-parts";

pub(crate) struct PartClaim(PathBuf);

static ACTIVE_PARTS: LazyLock<Mutex<HashSet<PathBuf>>> = LazyLock::new(Default::default);

#[cfg(any(target_os = "macos", windows))]
fn fold_file_name(path: &Path) -> PathBuf {
    match path.file_name() {
        Some(name) => path.with_file_name(name.to_string_lossy().to_lowercase()),
        None => path.to_path_buf(),
    }
}

#[cfg(not(any(target_os = "macos", windows)))]
fn fold_file_name(path: &Path) -> PathBuf {
    path.to_path_buf()
}

fn claim_key(path: &Path) -> PathBuf {
    let path = fold_file_name(path);
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    match (parent.canonicalize(), path.file_name()) {
        (Ok(parent), Some(name)) => parent.join(name),
        _ => path.to_path_buf(),
    }
}

impl PartClaim {
    pub(crate) fn acquire(path: &Path) -> Result<Self, String> {
        let key = claim_key(path);
        let mut active = ACTIVE_PARTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !active.insert(key.clone()) {
            return Err(format!(
                "Another active download is already writing {}",
                path.display()
            ));
        }
        Ok(Self(key))
    }
}

impl Drop for PartClaim {
    fn drop(&mut self) {
        ACTIVE_PARTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.0);
    }
}

pub(crate) struct RenamedPart {
    pub filename: String,
    pub part_path: PathBuf,
}

fn part_reserved_elsewhere(options: &Map<String, Value>, part_path: &Path) -> bool {
    part_path.exists()
        && options
            .get(RESERVED_PARTS_OPTION)
            .and_then(Value::as_array)
            .is_some_and(|parts| {
                parts
                    .iter()
                    .filter_map(Value::as_str)
                    .any(|reserved| Path::new(reserved) == part_path)
            })
}

async fn claim_part(
    part_path: &Path,
    cancel_token: &CancellationToken,
) -> Result<PartClaim, String> {
    let deadline = tokio::time::Instant::now() + PART_CLAIM_WAIT;
    loop {
        match PartClaim::acquire(part_path) {
            Ok(claim) => return Ok(claim),
            Err(e) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(e);
                }
                tokio::select! {
                    _ = cancel_token.cancelled() => return Err(e),
                    _ = tokio::time::sleep(PART_CLAIM_POLL) => {}
                }
            }
        }
    }
}

fn claim_numbered_part(dir_path: &Path, filename: &str) -> Option<(RenamedPart, PartClaim)> {
    let name = filename.strip_suffix(PART_SUFFIX).unwrap_or(filename);
    let (stem, ext) = match name.rfind('.') {
        Some(dot) if dot > 0 => (&name[..dot], &name[dot..]),
        _ => (name, ""),
    };
    (1..=MAX_NUMBERED_NAMES).find_map(|n| {
        let numbered = format!("{stem}.{n}{ext}");
        let part_path = dir_path.join(format!("{numbered}{PART_SUFFIX}"));
        if part_path.exists() || dir_path.join(&numbered).exists() {
            return None;
        }
        let claim = PartClaim::acquire(&part_path).ok()?;
        Some((
            RenamedPart {
                filename: numbered,
                part_path,
            },
            claim,
        ))
    })
}

pub(crate) async fn claim_or_rename(
    dir_path: &Path,
    filename: &str,
    part_path: &Path,
    options: &Map<String, Value>,
    auto_rename: bool,
    cancel_token: &CancellationToken,
) -> Result<(PartClaim, Option<RenamedPart>), String> {
    let claimed = if part_reserved_elsewhere(options, part_path) {
        Err(format!(
            "{} belongs to another unfinished download",
            part_path.display()
        ))
    } else {
        claim_part(part_path, cancel_token).await
    };
    match claimed {
        Ok(claim) => Ok((claim, None)),
        Err(e) if !auto_rename || cancel_token.is_cancelled() => Err(e),
        Err(e) => {
            let (renamed, claim) = claim_numbered_part(dir_path, filename).ok_or(e)?;
            tracing::info!(
                "{filename:?} is being written by another download, saving as {:?}",
                renamed.filename
            );
            Ok((claim, Some(renamed)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn part_claim_refuses_a_second_writer_until_released() {
        let path = PathBuf::from("/claim-test/file.bin.part");
        let first = PartClaim::acquire(&path).unwrap();
        assert!(PartClaim::acquire(&path).is_err());
        assert!(PartClaim::acquire(&PathBuf::from("/claim-test/other.part")).is_ok());
        drop(first);
        assert!(PartClaim::acquire(&path).is_ok());
    }

    #[test]
    fn part_claim_treats_equivalent_paths_as_one() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("downloads");
        std::fs::create_dir(&sub).unwrap();
        let direct = sub.join("file.bin.part");
        let dotted = sub.join("..").join("downloads").join("file.bin.part");
        let first = PartClaim::acquire(&direct).unwrap();
        assert!(PartClaim::acquire(&dotted).is_err());
        drop(first);
        let second = PartClaim::acquire(&dotted).unwrap();
        assert!(PartClaim::acquire(&direct).is_err());
        drop(second);
        assert!(PartClaim::acquire(&direct).is_ok());
    }

    #[test]
    fn part_claim_folds_name_case_where_the_filesystem_does() {
        let dir = tempfile::tempdir().unwrap();
        let first = PartClaim::acquire(&dir.path().join("Setup.exe.part")).unwrap();
        let second = PartClaim::acquire(&dir.path().join("setup.exe.part"));
        assert_eq!(second.is_err(), cfg!(any(target_os = "macos", windows)));
        drop(first);
    }

    #[tokio::test(start_paused = true)]
    async fn a_claimed_part_is_waited_for_then_given_up_on() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("file.bin.part");
        let cancel = CancellationToken::new();

        let held = PartClaim::acquire(&part).unwrap();
        let waiter = tokio::spawn({
            let (part, cancel) = (part.clone(), cancel.clone());
            async move { claim_part(&part, &cancel).await.is_ok() }
        });
        tokio::time::sleep(PART_CLAIM_WAIT / 2).await;
        drop(held);
        assert!(waiter.await.unwrap());

        let _held = PartClaim::acquire(&part).unwrap();
        assert!(claim_part(&part, &cancel).await.is_err());
    }

    #[test]
    fn a_part_is_reserved_only_while_it_exists_and_is_listed() {
        let dir = tempfile::tempdir().unwrap();
        let part = dir.path().join("file.bin.part");
        let mut options = Map::new();
        options.insert(
            RESERVED_PARTS_OPTION.into(),
            serde_json::json!([part.to_string_lossy()]),
        );
        assert!(!part_reserved_elsewhere(&options, &part));
        std::fs::write(&part, b"theirs").unwrap();
        assert!(part_reserved_elsewhere(&options, &part));
        assert!(!part_reserved_elsewhere(&Map::new(), &part));
    }

    #[test]
    fn a_numbered_name_skips_names_that_are_taken() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        std::fs::write(dir_path.join("file.1.bin"), b"done").unwrap();
        std::fs::write(dir_path.join("file.2.bin.part"), b"paused").unwrap();
        let _active = PartClaim::acquire(&dir_path.join("file.3.bin.part")).unwrap();

        let (renamed, _claim) = claim_numbered_part(dir_path, "file.bin").unwrap();
        assert_eq!(renamed.filename, "file.4.bin");
        assert_eq!(renamed.part_path, dir_path.join("file.4.bin.part"));
        assert!(PartClaim::acquire(&renamed.part_path).is_err());

        let (bare, _claim) = claim_numbered_part(dir_path, "download").unwrap();
        assert_eq!(bare.filename, "download.1");
    }

    #[tokio::test(start_paused = true)]
    async fn a_reserved_or_busy_part_is_renamed_or_refused() {
        let dir = tempfile::tempdir().unwrap();
        let dir_path = dir.path();
        let part = dir_path.join("file.bin.part");
        let cancel = CancellationToken::new();
        let none = Map::new();

        let (claim, renamed) = claim_or_rename(dir_path, "file.bin", &part, &none, true, &cancel)
            .await
            .unwrap();
        assert!(renamed.is_none());

        let (_second, renamed) = claim_or_rename(dir_path, "file.bin", &part, &none, true, &cancel)
            .await
            .unwrap();
        assert_eq!(renamed.unwrap().filename, "file.1.bin");
        assert!(
            claim_or_rename(dir_path, "file.bin", &part, &none, false, &cancel)
                .await
                .is_err()
        );
        drop(claim);

        std::fs::write(&part, b"paused").unwrap();
        let mut reserved = Map::new();
        reserved.insert(
            RESERVED_PARTS_OPTION.into(),
            serde_json::json!([part.to_string_lossy()]),
        );
        let (_third, renamed) =
            claim_or_rename(dir_path, "file.bin", &part, &reserved, true, &cancel)
                .await
                .unwrap();
        assert_eq!(renamed.unwrap().filename, "file.2.bin");
        assert!(
            claim_or_rename(dir_path, "file.bin", &part, &reserved, false, &cancel)
                .await
                .is_err()
        );
    }
}
