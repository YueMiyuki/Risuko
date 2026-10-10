use serde_json::Value;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

const ATOMIC_TEMP_PREFIX: &str = ".risuko-atomic-";
const ATOMIC_TEMP_SUFFIX: &str = ".tmp";
const STALE_ATOMIC_TEMP_AGE: Duration = Duration::from_secs(24 * 60 * 60);

pub(crate) fn process_may_be_running(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let Ok(pid) = libc::pid_t::try_from(pid) else {
            return true;
        };
        // SAFETY
        let result = unsafe { libc::kill(pid, 0) };
        result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
    }

    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{
            CloseHandle, GetLastError, ERROR_INVALID_PARAMETER, WAIT_OBJECT_0,
        };
        use windows_sys::Win32::System::Threading::{
            OpenProcess, WaitForSingleObject, PROCESS_SYNCHRONIZE,
        };

        let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, pid) };
        if handle.is_null() {
            (unsafe { GetLastError() }) != ERROR_INVALID_PARAMETER
        } else {
            let state = unsafe { WaitForSingleObject(handle, 0) };
            unsafe {
                CloseHandle(handle);
            }
            state != WAIT_OBJECT_0
        }
    }

    #[cfg(all(not(unix), not(windows)))]
    {
        let _ = pid;
        true
    }
}

fn atomic_temp_pid(name: &str) -> Option<u32> {
    let body = name
        .strip_prefix(ATOMIC_TEMP_PREFIX)?
        .strip_suffix(ATOMIC_TEMP_SUFFIX)?;
    let (pid, random_suffix) = body.split_once('-')?;
    if random_suffix.is_empty() {
        return None;
    }
    pid.parse().ok()
}

pub(crate) fn cleanup_stale_atomic_write_files(directory: &Path) {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            tracing::debug!(
                path = %directory.display(),
                %error,
                "could not scan for stale atomic-write files"
            );
            return;
        }
    };
    let now = SystemTime::now();

    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(pid) = atomic_temp_pid(name) else {
            continue;
        };
        if pid == std::process::id() || process_may_be_running(pid) {
            continue;
        }
        let Ok(metadata) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        let Ok(age) = now.duration_since(modified) else {
            continue;
        };
        if age < STALE_ATOMIC_TEMP_AGE {
            continue;
        }

        if let Err(error) = std::fs::remove_file(entry.path()) {
            tracing::debug!(
                path = %entry.path().display(),
                %error,
                "could not remove stale atomic-write file"
            );
        }
    }
}

pub fn write_file_atomically(path: &Path, data: &[u8]) -> Result<(), String> {
    write_file_atomically_with(path, data, |_| Ok(()))
}

pub fn write_file_atomically_with(
    path: &Path,
    data: &[u8],
    prepare: impl FnOnce(&Path) -> Result<(), String>,
) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("{} has no parent directory", path.display()))?;
    std::fs::create_dir_all(parent)
        .map_err(|e| format!("Failed to create dir {}: {e}", parent.display()))?;

    let mut builder = tempfile::Builder::new();
    let prefix = format!("{ATOMIC_TEMP_PREFIX}{}-", std::process::id());
    builder.prefix(&prefix).suffix(ATOMIC_TEMP_SUFFIX);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        builder.permissions(std::fs::Permissions::from_mode(0o600));
    }
    let mut temp = builder
        .tempfile_in(parent)
        .map_err(|e| format!("Failed to create temp file in {}: {e}", parent.display()))?;
    temp.as_file_mut()
        .write_all(data)
        .map_err(|e| format!("Failed to write temporary file for {}: {e}", path.display()))?;
    temp.as_file()
        .sync_all()
        .map_err(|e| format!("Failed to sync temporary file for {}: {e}", path.display()))?;
    prepare(temp.path())?;
    temp.persist(path)
        .map_err(|e| format!("Failed to persist {}: {}", path.display(), e.error))?;

    if let Ok(directory) = std::fs::File::open(parent) {
        let _ = directory.sync_all();
    }
    Ok(())
}

pub trait ConfigDirProvider: Send + Sync {
    fn config_dir(&self) -> PathBuf;
}

pub trait EventSink: Send + Sync {
    fn emit(&self, event: &str, payload: Value);
}

pub struct NoopEventSink;

impl EventSink for NoopEventSink {
    fn emit(&self, _event: &str, _payload: Value) {}
}

pub trait StorageBackend: Send + Sync {
    fn load(&self, key: &str) -> Result<Option<Value>, String>;
    fn save(&self, key: &str, value: &Value) -> Result<(), String>;
}

pub struct FileStorage {
    dir: PathBuf,
}

impl FileStorage {
    pub fn new(dir: PathBuf) -> Self {
        cleanup_stale_atomic_write_files(&dir);
        Self { dir }
    }

    fn safe_path(&self, key: &str) -> Result<PathBuf, String> {
        if key.is_empty()
            || key == "."
            || key == ".."
            || key.contains('/')
            || key.contains('\\')
            || key.contains('\0')
            || std::path::Path::new(key)
                .components()
                .any(|c| !matches!(c, std::path::Component::Normal(_)))
        {
            return Err(format!("invalid storage key: {key:?}"));
        }
        Ok(self.dir.join(format!("{key}.json")))
    }
}

impl StorageBackend for FileStorage {
    fn load(&self, key: &str) -> Result<Option<Value>, String> {
        let path = self.safe_path(key)?;
        let data = match std::fs::read_to_string(&path) {
            Ok(data) => data,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(format!("Failed to read {}: {e}", path.display())),
        };
        let value: Value = serde_json::from_str(&data)
            .map_err(|e| format!("Failed to parse {}: {e}", path.display()))?;
        Ok(Some(value))
    }

    fn save(&self, key: &str, value: &Value) -> Result<(), String> {
        let path = self.safe_path(key)?;
        let data =
            serde_json::to_string_pretty(value).map_err(|e| format!("Failed to serialize: {e}"))?;
        write_file_atomically(&path, data.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Arc, Barrier};

    #[test]
    fn concurrent_saves_of_one_key_use_independent_temp_files() {
        let dir = tempfile::tempdir().unwrap();
        let storage = Arc::new(FileStorage::new(dir.path().to_path_buf()));
        let barrier = Arc::new(Barrier::new(3));

        let handles: Vec<_> = [json!({"writer": 1}), json!({"writer": 2})]
            .into_iter()
            .map(|value| {
                let storage = Arc::clone(&storage);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    storage.save("shared", &value)
                })
            })
            .collect();
        barrier.wait();

        for handle in handles {
            handle.join().unwrap().unwrap();
        }
        let saved = storage.load("shared").unwrap().unwrap();
        assert!(saved == json!({"writer": 1}) || saved == json!({"writer": 2}));
    }

    #[cfg(unix)]
    #[test]
    fn stale_atomic_temp_files_are_pruned_without_touching_fresh_or_unrelated_files() {
        let dir = tempfile::tempdir().unwrap();
        let mut dead_process = std::process::Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let dead_pid = dead_process.id();
        dead_process.wait().unwrap();
        assert!(!process_may_be_running(dead_pid));
        let mut live_process = std::process::Command::new("/bin/sh")
            .args(["-c", "sleep 30"])
            .spawn()
            .unwrap();
        let live_pid = live_process.id();
        assert!(process_may_be_running(live_pid));
        let stale = dir.path().join(format!(
            "{ATOMIC_TEMP_PREFIX}{dead_pid}-stale{ATOMIC_TEMP_SUFFIX}"
        ));
        let fresh = dir.path().join(format!(
            "{ATOMIC_TEMP_PREFIX}{dead_pid}-fresh{ATOMIC_TEMP_SUFFIX}"
        ));
        let live = dir.path().join(format!(
            "{ATOMIC_TEMP_PREFIX}{live_pid}-live{ATOMIC_TEMP_SUFFIX}"
        ));
        let unrelated = dir.path().join("unrelated.tmp");
        std::fs::write(&stale, b"stale").unwrap();
        std::fs::write(&fresh, b"fresh").unwrap();
        std::fs::write(&live, b"live").unwrap();
        std::fs::write(&unrelated, b"unrelated").unwrap();

        let stale_time = SystemTime::now() - STALE_ATOMIC_TEMP_AGE - Duration::from_secs(60);
        let times = std::fs::FileTimes::new().set_modified(stale_time);
        for candidate in [&stale, &live] {
            std::fs::File::open(candidate)
                .unwrap()
                .set_times(times)
                .unwrap();
        }

        cleanup_stale_atomic_write_files(dir.path());
        let _ = live_process.kill();
        let _ = live_process.wait();

        assert!(!stale.exists());
        assert!(fresh.exists());
        assert!(live.exists());
        assert!(unrelated.exists());
    }

    #[cfg(unix)]
    #[test]
    fn atomic_write_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let atomic = dir.path().join("atomic.json");

        write_file_atomically(&atomic, b"atomic").unwrap();

        let atomic_mode = std::fs::metadata(atomic).unwrap().permissions().mode() & 0o777;
        assert_eq!(atomic_mode & 0o077, 0);
    }
}
