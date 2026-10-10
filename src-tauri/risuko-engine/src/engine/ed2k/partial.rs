use std::path::{Path, PathBuf};

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
        self.hashset
            .as_ref()?
            .iter()
            .map(|h| hex::decode(h).ok()?.try_into().ok())
            .collect()
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
    let safe = crate::engine::util::safe_filename(file_name, "ed2k-download");
    PathBuf::from(dir).join(safe)
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
    let tmp = with_suffix(path, ".tmp");
    let data = serde_json::to_vec(met).map_err(std::io::Error::other)?;
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}

fn remove_quiet(path: &Path) {
    if let Err(e) = std::fs::remove_file(path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            tracing::warn!("could not delete {}: {e}", path.display());
        }
    }
}

pub fn remove_partial(dir: &str, uri: &str) {
    let Ok(link) = super::parse_ed2k_link(uri) else {
        return;
    };
    let final_path = final_path(dir, &link.file_name);
    remove_partial_files(&final_path);
}

pub fn remove_partial_files(final_path: &Path) {
    remove_quiet(&part_path(final_path));
    remove_quiet(&met_path(final_path));
    remove_quiet(&with_suffix(&met_path(final_path), ".tmp"));
}

pub fn finalize(final_path: &Path) -> Result<(), String> {
    let part = part_path(final_path);
    if std::fs::rename(&part, final_path).is_err() {
        let _ = std::fs::remove_file(final_path);
        std::fs::rename(&part, final_path)
            .map_err(|e| format!("Failed to move completed file into place: {e}"))?;
    }
    remove_quiet(&met_path(final_path));
    Ok(())
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
    fn remove_partial_deletes_part_and_sidecar_only() {
        let dir = tempfile::tempdir().unwrap();
        let fin = dir.path().join("f.bin");
        std::fs::write(part_path(&fin), b"x").unwrap();
        std::fs::write(met_path(&fin), b"x").unwrap();
        std::fs::write(&fin, b"keep").unwrap();
        let uri = "ed2k://|file|f.bin|10|0123456789abcdef0123456789abcdef|/";
        remove_partial(dir.path().to_str().unwrap(), uri);
        assert!(!part_path(&fin).exists());
        assert!(!met_path(&fin).exists());
        assert!(fin.exists());
    }

    #[test]
    fn finalize_moves_part_into_place_and_drops_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let fin = dir.path().join("f.bin");
        std::fs::write(part_path(&fin), b"new").unwrap();
        std::fs::write(met_path(&fin), b"{}").unwrap();
        std::fs::write(&fin, b"old").unwrap();
        finalize(&fin).unwrap();
        assert_eq!(std::fs::read(&fin).unwrap(), b"new");
        assert!(!part_path(&fin).exists());
        assert!(!met_path(&fin).exists());
    }
}
