//! Cross-platform file pre-allocation: Linux `fallocate(2)`, macOS `fcntl(F_PREALLOCATE)`+`set_len`, else plain `set_len`; `file-allocation` mode is `falloc` (default, platform fallocate then `set_len` fallback), `trunc` (`set_len` only), or `none` (writes grow the file); `Mode::from_option` reads the options-map JSON value and defaults to `Falloc` on missing/invalid keys

use std::fs::File;
use std::io;

use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    None,
    Trunc,
    Falloc,
}

impl Mode {
    pub fn from_option(v: Option<&Value>) -> Self {
        match v.and_then(Value::as_str) {
            Some("none") => Mode::None,
            Some("trunc") => Mode::Trunc,
            // Accept the canonical `falloc` and the aria2-compatible `prealloc` spelling so users migrating configs aren't surprised
            Some("falloc") | Some("prealloc") => Mode::Falloc,
            _ => Mode::Falloc,
        }
    }
}

/// Pre-allocate `file` to `len` bytes per `mode`; `Falloc` silently degrades to `set_len` on `ENOTSUP`/`EOPNOTSUPP` (network/exotic filesystems) so tmpfs/SMB users don't see spurious errors
pub fn allocate(file: &File, len: u64, mode: Mode) -> io::Result<()> {
    match mode {
        Mode::None => Ok(()),
        Mode::Trunc => file.set_len(len),
        Mode::Falloc => match platform_fallocate(file, len) {
            Ok(()) => Ok(()),
            Err(e) if is_unsupported(&e) => {
                tracing::debug!(
                    "fallocate not supported on this filesystem ({e}), falling back to set_len"
                );
                file.set_len(len)
            }
            Err(e) => Err(e),
        },
    }
}

#[cfg(target_os = "linux")]
fn platform_fallocate(file: &File, len: u64) -> io::Result<()> {
    use nix::fcntl::{fallocate, FallocateFlags};
    // 0 flags == reserve blocks AND extend logical length, matching aria2; `fallocate(2)` takes a signed length, so reject sizes that don't fit rather than pass a negative value (kernel rejects with EINVAL but it would look like a corrupt request)
    let signed_len = i64::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "fallocate length exceeds i64::MAX",
        )
    })?;
    let current_len = file.metadata()?.len();
    if len == 0 {
        return if current_len == 0 {
            Ok(())
        } else {
            file.set_len(0)
        };
    }
    // nix 0.30 expects an `AsFd` implementor — `&File` qualifies and avoids the unsafe-ish round-trip through `RawFd`
    fallocate(file, FallocateFlags::empty(), 0, signed_len)
        .map_err(|e| io::Error::from_raw_os_error(e as i32))?;
    // Empty flags extend a shorter file to `len`, but never shrink a longer
    // one. Avoid an extra metadata-changing truncate on the common grow path
    if current_len > len {
        file.set_len(len)
    } else {
        Ok(())
    }
}

#[cfg(target_os = "macos")]
fn platform_fallocate(file: &File, len: u64) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::io::AsRawFd;
    // F_PREALLOCATE only reserves blocks (logical size still needs set_len); try contiguous first (F_ALLOCATECONTIG), then retry without the hint on fragmented free space so we still get the reserve
    #[repr(C)]
    struct Fstore {
        fst_flags: libc::c_uint,
        fst_posmode: libc::c_int,
        fst_offset: libc::off_t,
        fst_length: libc::off_t,
        fst_bytesalloc: libc::off_t,
    }
    const F_PREALLOCATE: libc::c_int = 42;
    const F_ALLOCATECONTIG: libc::c_uint = 0x2;
    const F_ALLOCATEALL: libc::c_uint = 0x4;
    const F_PEOFPOSMODE: libc::c_int = 3;

    let fd = file.as_raw_fd();
    if file.metadata()?.len() > len {
        file.set_len(len)?;
    }
    let allocated_len = file.metadata()?.blocks().saturating_mul(512);
    if allocated_len >= len {
        return file.set_len(len);
    }
    let needed = len - allocated_len;
    let signed_len = libc::off_t::try_from(needed).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "fallocate length exceeds off_t range",
        )
    })?;
    let mut store = Fstore {
        fst_flags: F_ALLOCATECONTIG | F_ALLOCATEALL,
        fst_posmode: F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: signed_len,
        fst_bytesalloc: 0,
    };
    let rc = unsafe { libc::fcntl(fd, F_PREALLOCATE, &mut store as *mut Fstore) };
    if rc == -1 {
        // Capture the contiguous-attempt error before retrying so it can be surfaced if the second call also fails; otherwise we'd only report the (often less informative) non-contiguous failure
        let first_err = io::Error::last_os_error();
        // Retry without contiguous hint
        store.fst_flags = F_ALLOCATEALL;
        let rc2 = unsafe { libc::fcntl(fd, F_PREALLOCATE, &mut store as *mut Fstore) };
        if rc2 == -1 {
            let second_err = io::Error::last_os_error();
            // Log the contextual message and return `second_err` directly so `raw_os_error()` is preserved — wrapping with `io::Error::new` would erase the OS code and silently disable the `is_unsupported` fallback for filesystems that don't implement F_PREALLOCATE
            tracing::debug!(
                fd = fd,
                first_err = %first_err,
                second_err = %second_err,
                "F_PREALLOCATE failed (Fstore fst_flags=F_ALLOCATEALL): contiguous and non-contiguous attempts both failed"
            );
            return Err(second_err);
        }
    }
    // F_PREALLOCATE only reserves blocks; `set_len` commits the new logical size so writes within `len` see allocated space. On `set_len` failure the reserved blocks stay attached until close/unlink, which is fine because the caller treats the allocation as failed and cleans up the file
    file.set_len(len)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn platform_fallocate(file: &File, len: u64) -> io::Result<()> {
    // No good cross-platform reservation primitive on Windows without Administrator/SeManageVolumePrivilege; fall back to set_len, which is what `Mode::Trunc` does anyway
    file.set_len(len)
}

#[cfg(unix)]
fn is_unsupported(e: &io::Error) -> bool {
    // Documented "unsupported" errnos: ENOTSUP/EOPNOTSUPP/ENOSYS. Older Linux kernels and some filesystems (e.g. FUSE drivers) report EINVAL instead, so on Linux we also treat EINVAL as a fallback signal — but other Unixes (notably macOS' F_PREALLOCATE) use EINVAL for genuine invalid-argument errors, which must not be masked
    let Some(code) = e.raw_os_error() else {
        return false;
    };
    if code == libc::ENOTSUP || code == libc::EOPNOTSUPP || code == libc::ENOSYS {
        return true;
    }
    #[cfg(target_os = "linux")]
    {
        if code == libc::EINVAL {
            return true;
        }
    }
    false
}

#[cfg(not(unix))]
fn is_unsupported(_: &io::Error) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn mode_parses() {
        assert_eq!(Mode::from_option(None), Mode::Falloc);
        assert_eq!(Mode::from_option(Some(&json!("none"))), Mode::None);
        assert_eq!(Mode::from_option(Some(&json!("trunc"))), Mode::Trunc);
        assert_eq!(Mode::from_option(Some(&json!("falloc"))), Mode::Falloc);
        assert_eq!(Mode::from_option(Some(&json!("prealloc"))), Mode::Falloc);
        assert_eq!(Mode::from_option(Some(&json!("garbage"))), Mode::Falloc);
    }

    #[test]
    fn allocate_grows_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob");
        let f = std::fs::File::create(&path).unwrap();
        allocate(&f, 4096, Mode::Falloc).unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), 4096);
    }

    #[test]
    fn allocate_none_leaves_file_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob");
        let f = std::fs::File::create(&path).unwrap();
        allocate(&f, 4096, Mode::None).unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), 0);
    }

    #[test]
    fn allocate_on_resumed_file_reaches_target_len() {
        // Mimic a resumed download: file already holds bytes and we (re)allocate to the full target; final logical size must be exactly `target`, never `existing + target`
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&[0u8; 2048]).unwrap();
        allocate(&f, 8192, Mode::Falloc).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 8192);
    }

    #[test]
    fn allocate_below_current_len_truncates_to_target() {
        // Target smaller than the file's current size should end at `target`
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("blob");
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(&[0u8; 8192]).unwrap();
        allocate(&f, 4096, Mode::Falloc).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 4096);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn falloc_reserves_blocks_for_an_already_sized_sparse_file() {
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse");
        let f = std::fs::File::create(&path).unwrap();
        let target = 1024 * 1024;
        f.set_len(target).unwrap();
        let before = f.metadata().unwrap().blocks();

        allocate(&f, target, Mode::Falloc).unwrap();

        let metadata = f.metadata().unwrap();
        assert_eq!(metadata.len(), target);
        assert!(
            metadata.blocks().saturating_mul(512) >= target,
            "preallocation must reserve the full sparse logical file (before={before} blocks, after={} blocks)",
            metadata.blocks()
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn falloc_remeasures_blocks_after_shrinking_a_sparse_file() {
        use std::io::{Seek, SeekFrom, Write};
        use std::os::unix::fs::MetadataExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse-tail");
        let mut f = std::fs::File::create(&path).unwrap();
        let original_len = 1024 * 1024 * 1024;
        let target = 16 * 1024;
        f.set_len(original_len).unwrap();
        f.seek(SeekFrom::Start(original_len - target)).unwrap();
        f.write_all(&vec![0x5a; target as usize]).unwrap();
        assert!(f.metadata().unwrap().blocks() > 0);

        allocate(&f, target, Mode::Falloc).unwrap();

        let metadata = f.metadata().unwrap();
        assert_eq!(metadata.len(), target);
        assert!(
            metadata.blocks().saturating_mul(512) >= target,
            "preallocation must replace blocks discarded by the shrink"
        );
    }
}
