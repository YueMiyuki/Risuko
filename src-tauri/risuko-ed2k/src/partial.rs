use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::types::Ed2kFileLink;

const MET_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Met {
    pub version: u32,
    pub file_hash: String,
    pub file_size: u64,
    pub hashset: Option<Vec<String>>,
    pub verified: Vec<u32>,
}

impl Met {
    pub fn new(
        file_hash: [u8; 16],
        file_size: u64,
        hashset: Option<&[[u8; 16]]>,
        verified: Vec<u32>,
    ) -> Self {
        Self {
            version: MET_VERSION,
            file_hash: hex::encode(file_hash),
            file_size,
            hashset: hashset.map(|h| h.iter().map(hex::encode).collect()),
            verified,
        }
    }

    pub fn decoded_hashset(&self) -> Option<Vec<[u8; 16]>> {
        decode_hashset(self.hashset.as_ref()?)
    }

    fn matches(&self, link: &Ed2kFileLink) -> bool {
        self.version == MET_VERSION
            && self.file_size == link.file_size
            && self
                .file_hash
                .eq_ignore_ascii_case(&hex::encode(link.file_hash_bytes))
    }
}

pub fn final_path(dir: &str, file_name: &str) -> PathBuf {
    let safe = risuko_util::safe_filename(file_name, "ed2k-download");
    PathBuf::from(dir).join(safe)
}

static CLAIMED: LazyLock<Mutex<HashSet<PathBuf>>> = LazyLock::new(Default::default);

#[derive(Debug)]
pub struct PathClaim {
    path: PathBuf,
    key: PathBuf,
}

impl PathClaim {
    pub fn acquire(final_path: &Path) -> Result<Self, String> {
        let key = claim_key(final_path);
        if !CLAIMED.lock().insert(key.clone()) {
            return Err(format!(
                "{} is already in use by another eD2K task",
                final_path.display()
            ));
        }
        Ok(Self {
            path: final_path.to_path_buf(),
            key,
        })
    }

    pub async fn acquire_async(final_path: PathBuf) -> Result<Self, String> {
        tokio::task::spawn_blocking(move || Self::acquire(&final_path))
            .await
            .map_err(|e| format!("Path claim task failed: {e}"))?
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn claim_key(final_path: &Path) -> PathBuf {
    let (Some(parent), Some(name)) = (final_path.parent(), final_path.file_name()) else {
        return final_path.to_path_buf();
    };
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let _ = std::fs::create_dir_all(parent);
    std::fs::canonicalize(parent)
        .map(|dir| dir.join(name))
        .unwrap_or_else(|_| final_path.to_path_buf())
}

impl Drop for PathClaim {
    fn drop(&mut self) {
        CLAIMED.lock().remove(&self.key);
    }
}

fn numbered(path: &Path, n: u32) -> PathBuf {
    let name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let numbered = match name.rfind('.') {
        Some(dot) if dot > 0 => format!("{}.{n}{}", &name[..dot], &name[dot..]),
        _ => format!("{name}.{n}"),
    };
    path.with_file_name(numbered)
}

fn candidates(first: PathBuf) -> impl Iterator<Item = PathBuf> {
    (0u32..).map(move |n| {
        if n == 0 {
            first.clone()
        } else {
            numbered(&first, n)
        }
    })
}

pub fn reserve(
    dir: &str,
    file_name: &str,
    link: &Ed2kFileLink,
    auto_rename: bool,
) -> Result<PathClaim, String> {
    let first = final_path(dir, file_name);
    if !auto_rename {
        return PathClaim::acquire(&first);
    }
    for (n, path) in candidates(first).enumerate() {
        let Ok(claim) = PathClaim::acquire(&path) else {
            continue;
        };
        let owned = load_met(&met_path(&path), link).is_some();
        let free =
            !path.exists() && !met_path(&path).exists() && (n == 0 || !part_path(&path).exists());
        if owned || free {
            return Ok(claim);
        }
    }
    Err("Failed to reserve an output filename".to_string())
}

pub(crate) fn decode_hashset(hashes: &[String]) -> Option<Vec<[u8; 16]>> {
    hashes
        .iter()
        .map(|h| hex::decode(h).ok()?.try_into().ok())
        .collect()
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(suffix);
    PathBuf::from(s)
}

pub fn part_path(final_path: &Path) -> PathBuf {
    with_suffix(final_path, ".part")
}

pub fn met_path(final_path: &Path) -> PathBuf {
    with_suffix(final_path, ".part.met")
}

pub fn load_met(path: &Path, link: &Ed2kFileLink) -> Option<Met> {
    let bytes = std::fs::read(path).ok()?;
    let met: Met = serde_json::from_slice(&bytes).ok()?;
    met.matches(link).then_some(met)
}

pub fn save_met(path: &Path, met: &Met) -> std::io::Result<()> {
    write_atomic(
        path,
        &serde_json::to_vec(met).map_err(std::io::Error::other)?,
    )
}

pub(crate) fn write_atomic(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let tmp = with_suffix(path, ".tmp");
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}

pub(crate) fn remove_quiet(path: &Path) {
    if let Err(e) = std::fs::remove_file(path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!("could not delete {}: {e}", path.display());
        }
    }
}

pub fn remove_partial_files(final_path: &Path) {
    remove_quiet(&part_path(final_path));
    remove_quiet(&met_path(final_path));
    remove_quiet(&with_suffix(&met_path(final_path), ".tmp"));
}

pub fn finalize(claim: PathClaim, auto_rename: bool) -> Result<PathClaim, String> {
    let original = claim.path().to_path_buf();
    let part = part_path(&original);
    let target = if auto_rename {
        let target = place_final(claim)?;
        std::fs::rename(&part, target.path()).map_err(|e| {
            let _ = std::fs::remove_file(target.path());
            format!("Failed to move completed file into place: {e}")
        })?;
        target
    } else {
        if std::fs::rename(&part, &original).is_err() {
            let _ = std::fs::remove_file(&original);
            std::fs::rename(&part, &original)
                .map_err(|e| format!("Failed to move completed file into place: {e}"))?;
        }
        claim
    };
    remove_quiet(&met_path(&original));
    Ok(target)
}

fn place_final(claim: PathClaim) -> Result<PathClaim, String> {
    for path in candidates(claim.path().to_path_buf()) {
        let next = if path == claim.path() {
            None
        } else {
            match PathClaim::acquire(&path) {
                Ok(next) if !part_path(&path).exists() && !met_path(&path).exists() => Some(next),
                _ => continue,
            }
        };
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(_) => return Ok(next.unwrap_or(claim)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(format!("Failed to reserve output file: {e}")),
        }
    }
    Err("Failed to reserve an output filename".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(size: u64, hash: [u8; 16]) -> Ed2kFileLink {
        Ed2kFileLink {
            file_name: "f.bin".into(),
            file_size: size,
            file_hash: hex::encode(hash),
            file_hash_bytes: hash,
            sources: Vec::new(),
            aich_hash: None,
        }
    }

    #[test]
    fn paths_append_to_the_final_name() {
        let f = Path::new("/d/f.tar.gz");
        assert_eq!(part_path(f), Path::new("/d/f.tar.gz.part"));
        assert_eq!(met_path(f), Path::new("/d/f.tar.gz.part.met"));
    }

    #[test]
    fn met_roundtrips_and_matches_only_its_link() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("f.bin.part.met");
        let hashes = [[3u8; 16], [4u8; 16]];
        let met = Met::new([1; 16], 500, Some(&hashes), vec![0, 1]);
        save_met(&path, &met).unwrap();
        assert_eq!(load_met(&path, &link(500, [1; 16])), Some(met.clone()));
        assert_eq!(met.decoded_hashset().unwrap(), hashes.to_vec());
        assert!(load_met(&path, &link(501, [1; 16])).is_none());
        assert!(load_met(&path, &link(500, [2; 16])).is_none());
    }

    #[test]
    fn corrupt_or_foreign_version_sidecar_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("m");
        std::fs::write(&path, b"{not json").unwrap();
        assert!(load_met(&path, &link(10, [1; 16])).is_none());
        let mut met = Met::new([1; 16], 10, None, vec![]);
        met.version = 99;
        save_met(&path, &met).unwrap();
        assert!(load_met(&path, &link(10, [1; 16])).is_none());
        assert!(load_met(&dir.path().join("missing"), &link(10, [1; 16])).is_none());
    }

    #[test]
    fn a_claimed_path_cannot_be_claimed_again_until_released() {
        let dir = tempfile::tempdir().unwrap();
        let fin = dir.path().join("f.bin");
        let claim = PathClaim::acquire(&fin).unwrap();
        assert!(PathClaim::acquire(&fin).is_err());
        assert!(PathClaim::acquire(&dir.path().join("g.bin")).is_ok());
        drop(claim);
        assert!(PathClaim::acquire(&fin).is_ok());
    }

    #[test]
    fn aliases_of_a_claimed_path_are_rejected() {
        let root = tempfile::tempdir().unwrap();
        let real = root.path().join("real");
        let fin = real.join("f.bin");
        let _claim = PathClaim::acquire(&fin).unwrap();
        assert!(real.is_dir());
        assert!(PathClaim::acquire(&real.join("sub").join("..").join("f.bin")).is_err());
        #[cfg(unix)]
        {
            let link = root.path().join("link");
            std::os::unix::fs::symlink(&real, &link).unwrap();
            assert!(PathClaim::acquire(&link.join("f.bin")).is_err());
        }
    }

    #[test]
    fn remove_partial_files_deletes_part_and_sidecar_only() {
        let dir = tempfile::tempdir().unwrap();
        let fin = dir.path().join("f.bin");
        std::fs::write(part_path(&fin), b"x").unwrap();
        std::fs::write(met_path(&fin), b"x").unwrap();
        std::fs::write(&fin, b"keep").unwrap();
        remove_partial_files(&fin);
        assert!(!part_path(&fin).exists());
        assert!(!met_path(&fin).exists());
        assert!(fin.exists());
    }

    #[test]
    fn numbered_names_keep_the_extension() {
        assert_eq!(
            numbered(Path::new("/d/f.tar.gz"), 2),
            Path::new("/d/f.tar.2.gz")
        );
        assert_eq!(
            numbered(Path::new("/d/README"), 1),
            Path::new("/d/README.1")
        );
        assert_eq!(
            numbered(Path::new("/d/.hidden"), 1),
            Path::new("/d/.hidden.1")
        );
    }

    #[test]
    fn reserve_skips_names_owned_by_other_files() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().to_str().unwrap();
        let ours = link(500, [1; 16]);
        let fin = dir.path().join("f.bin");
        save_met(&met_path(&fin), &Met::new([2; 16], 500, None, vec![])).unwrap();
        std::fs::write(part_path(&fin), b"theirs").unwrap();
        std::fs::write(dir.path().join("f.1.bin"), b"done").unwrap();

        let claim = reserve(d, "f.bin", &ours, true).unwrap();
        assert_eq!(claim.path(), dir.path().join("f.2.bin"));
        assert_eq!(std::fs::read(part_path(&fin)).unwrap(), b"theirs");
        drop(claim);

        save_met(&met_path(&fin), &Met::new([1; 16], 500, None, vec![])).unwrap();
        assert_eq!(reserve(d, "f.bin", &ours, true).unwrap().path(), fin);
        let held = PathClaim::acquire(&fin).unwrap();
        assert_eq!(
            reserve(d, "f.bin", &ours, true).unwrap().path(),
            dir.path().join("f.2.bin")
        );
        assert!(reserve(d, "f.bin", &ours, false).is_err());
        drop(held);
        assert_eq!(reserve(d, "f.bin", &ours, false).unwrap().path(), fin);
    }

    #[test]
    fn reserve_accepts_a_stray_part_only_under_the_first_name() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path().to_str().unwrap();
        let fin = dir.path().join("f.bin");
        std::fs::write(part_path(&fin), b"legacy").unwrap();
        assert_eq!(
            reserve(d, "f.bin", &link(500, [1; 16]), true)
                .unwrap()
                .path(),
            fin
        );

        std::fs::write(&fin, b"done").unwrap();
        std::fs::write(part_path(&dir.path().join("f.1.bin")), b"stray").unwrap();
        assert_eq!(
            reserve(d, "f.bin", &link(500, [1; 16]), true)
                .unwrap()
                .path(),
            dir.path().join("f.2.bin")
        );
    }

    #[test]
    fn finalize_never_replaces_an_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let fin = dir.path().join("f.bin");
        std::fs::write(part_path(&fin), b"new").unwrap();
        std::fs::write(met_path(&fin), b"{}").unwrap();
        std::fs::write(&fin, b"old").unwrap();
        let taken = dir.path().join("f.1.bin");
        let _other = PathClaim::acquire(&taken).unwrap();

        let placed = finalize(PathClaim::acquire(&fin).unwrap(), true).unwrap();
        assert_eq!(placed.path(), dir.path().join("f.2.bin"));
        assert_eq!(std::fs::read(placed.path()).unwrap(), b"new");
        assert_eq!(std::fs::read(&fin).unwrap(), b"old");
        assert!(!taken.exists());
        assert!(!part_path(&fin).exists() && !met_path(&fin).exists());
        assert!(PathClaim::acquire(&fin).is_ok());
        assert!(PathClaim::acquire(placed.path()).is_err());
    }

    #[test]
    fn finalize_keeps_the_name_when_it_is_free() {
        let dir = tempfile::tempdir().unwrap();
        let fin = dir.path().join("f.bin");
        std::fs::write(part_path(&fin), b"new").unwrap();
        let placed = finalize(PathClaim::acquire(&fin).unwrap(), true).unwrap();
        assert_eq!(placed.path(), fin);
        assert_eq!(std::fs::read(&fin).unwrap(), b"new");
    }

    #[test]
    fn finalize_overwrites_when_renaming_is_off() {
        let dir = tempfile::tempdir().unwrap();
        let fin = dir.path().join("f.bin");
        std::fs::write(part_path(&fin), b"new").unwrap();
        std::fs::write(met_path(&fin), b"{}").unwrap();
        std::fs::write(&fin, b"old").unwrap();
        let placed = finalize(PathClaim::acquire(&fin).unwrap(), false).unwrap();
        assert_eq!(placed.path(), fin);
        assert_eq!(std::fs::read(&fin).unwrap(), b"new");
        assert!(!part_path(&fin).exists());
        assert!(!met_path(&fin).exists());
    }
}
