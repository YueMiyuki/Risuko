use crate::engine::archive_safety::ArchiveSafetyError;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub(crate) fn decode_yenc_line(input: &[u8], output: &mut Vec<u8>) -> Result<(), &'static str> {
    let mut index = 0;
    while index < input.len() {
        let mut value = input[index];
        index += 1;
        if value == b'=' {
            if index >= input.len() {
                return Err("truncated yEnc escape");
            }
            value = input[index].wrapping_sub(64);
            index += 1;
        }
        output.push(value.wrapping_sub(42));
    }
    Ok(())
}

#[derive(Debug)]
pub enum ArchivePipelineError {
    Safety(ArchiveSafetyError),
}

impl std::fmt::Display for ArchivePipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Safety(err) => write!(f, "archive safety: {err:?}"),
        }
    }
}

impl std::error::Error for ArchivePipelineError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Par2Outcome {
    Verified,
    Repaired,
    MissingParity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Par2Report {
    pub outcome: Par2Outcome,
    pub recovered_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum CleanupMode {
    KeepAll,
    DeletePar2,
    #[serde(alias = "delete-par2-and-archives")]
    DeletePar2AndVolumes,
}

impl CleanupMode {
    pub fn from_setting(value: Option<&str>) -> Self {
        match value.unwrap_or_default() {
            "delete-par2" => Self::DeletePar2,
            "delete-par2-and-volumes" | "delete-par2-and-archives" => Self::DeletePar2AndVolumes,
            _ => Self::KeepAll,
        }
    }
}

pub fn is_archive_volume_name(name: &str) -> bool {
    let name = Path::new(name)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(name)
        .to_ascii_lowercase();
    let extension = Path::new(&name)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default();
    if matches!(
        extension,
        "zip"
            | "rar"
            | "rev"
            | "7z"
            | "tar"
            | "gz"
            | "tgz"
            | "tbz"
            | "tbz2"
            | "bz2"
            | "xz"
            | "txz"
            | "zst"
            | "tzst"
    ) {
        return true;
    }
    let legacy_volume_digits = extension
        .strip_prefix('r')
        .or_else(|| extension.strip_prefix('z'));
    if legacy_volume_digits.is_some_and(|digits| {
        !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
    }) {
        return true;
    }
    if extension.bytes().all(|byte| byte.is_ascii_digit()) {
        let without_volume = Path::new(&name).with_extension("");
        let container_extension = without_volume
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or_default();
        return matches!(container_extension, "zip" | "7z" | "tar");
    }
    false
}

pub fn cleanup_after_success(
    mode: CleanupMode,
    verified_success: bool,
    par2_files: &[PathBuf],
    archive_volumes: &[PathBuf],
) -> io::Result<()> {
    if !verified_success {
        return Ok(());
    }
    if matches!(
        mode,
        CleanupMode::DeletePar2 | CleanupMode::DeletePar2AndVolumes
    ) {
        remove_existing(par2_files)?;
    }
    if matches!(mode, CleanupMode::DeletePar2AndVolumes) {
        remove_existing(archive_volumes)?;
    }
    Ok(())
}

fn remove_existing(paths: &[PathBuf]) -> io::Result<()> {
    for path in paths {
        if path.exists() {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn cleanup_is_success_gated() {
        let dir = tempdir().unwrap();
        let par2 = dir.path().join("x.par2");
        fs::write(&par2, b"x").unwrap();
        cleanup_after_success(
            CleanupMode::DeletePar2,
            false,
            std::slice::from_ref(&par2),
            &[],
        )
        .unwrap();
        assert!(par2.exists());
        let archive = dir.path().join("x.part01.rar");
        fs::write(&archive, b"x").unwrap();
        cleanup_after_success(
            CleanupMode::DeletePar2AndVolumes,
            false,
            std::slice::from_ref(&par2),
            std::slice::from_ref(&archive),
        )
        .unwrap();
        assert!(par2.exists() && archive.exists());
        cleanup_after_success(
            CleanupMode::DeletePar2,
            true,
            std::slice::from_ref(&par2),
            &[],
        )
        .unwrap();
        assert!(!par2.exists());
    }

    #[test]
    fn cleanup_modes_preserve_sidecars_and_non_archive_auxiliary_files() {
        let dir = tempdir().unwrap();
        let par2 = dir.path().join("release.vol00+01.par2");
        let rar = dir.path().join("release.part01.rar");
        let zip_volume = dir.path().join("release.z01");
        let nfo = dir.path().join("release.nfo");
        let resume = dir.path().join("release.part01.rar.resume.json");
        for path in [&par2, &rar, &zip_volume, &nfo, &resume] {
            fs::write(path, b"input").unwrap();
        }

        cleanup_after_success(
            CleanupMode::KeepAll,
            true,
            std::slice::from_ref(&par2),
            &[rar.clone(), zip_volume.clone()],
        )
        .unwrap();
        assert!(par2.exists() && rar.exists() && zip_volume.exists());

        cleanup_after_success(
            CleanupMode::DeletePar2,
            true,
            std::slice::from_ref(&par2),
            &[rar.clone(), zip_volume.clone()],
        )
        .unwrap();
        assert!(!par2.exists());
        assert!(rar.exists() && zip_volume.exists());

        fs::write(&par2, b"input").unwrap();
        cleanup_after_success(
            CleanupMode::DeletePar2AndVolumes,
            true,
            std::slice::from_ref(&par2),
            &[rar.clone(), zip_volume.clone()],
        )
        .unwrap();
        assert!(!par2.exists() && !rar.exists() && !zip_volume.exists());
        assert!(nfo.exists() && resume.exists());
    }

    #[test]
    fn cleanup_setting_accepts_frontend_archive_spelling() {
        assert_eq!(
            CleanupMode::from_setting(Some("delete-par2-and-archives")),
            CleanupMode::DeletePar2AndVolumes
        );
        assert_eq!(
            CleanupMode::from_setting(Some("delete-par2-and-volumes")),
            CleanupMode::DeletePar2AndVolumes
        );
        assert_eq!(
            CleanupMode::from_setting(Some("unknown")),
            CleanupMode::KeepAll
        );
        assert_eq!(
            serde_json::from_str::<CleanupMode>("\"delete-par2-and-archives\"").unwrap(),
            CleanupMode::DeletePar2AndVolumes
        );
    }

    #[test]
    fn archive_volume_classifier_excludes_auxiliary_files() {
        assert!(is_archive_volume_name("release.part01.rar"));
        assert!(is_archive_volume_name("release.r00"));
        assert!(is_archive_volume_name("release.z01"));
        assert!(is_archive_volume_name("release.7z.001"));
        assert!(is_archive_volume_name("release.tar.gz"));
        assert!(!is_archive_volume_name("release.nfo"));
        assert!(!is_archive_volume_name("release.sfv"));
        assert!(!is_archive_volume_name("release.par2"));
    }
}
