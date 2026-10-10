use serde::{Deserialize, Serialize};
use std::fs;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use super::task::{DownloadTask, TaskStatus};

pub const SESSION_FILENAME: &str = "engine-session.json";

#[derive(Serialize, Deserialize)]
pub struct SessionData {
    pub version: u32,
    pub tasks: Vec<DownloadTask>,
}

pub struct SessionManager {
    path: PathBuf,
    last_hash: Mutex<Option<u64>>,
}

impl SessionManager {
    pub fn new(config_dir: &Path) -> Self {
        let path = config_dir.join(SESSION_FILENAME);
        Self {
            path,
            last_hash: Mutex::new(None),
        }
    }

    pub fn load(&self) -> Vec<DownloadTask> {
        let data = match fs::read_to_string(&self.path) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };

        match serde_json::from_str::<SessionData>(&data) {
            Ok(session) => session
                .tasks
                .into_iter()
                .filter(|t| !matches!(t.status, TaskStatus::Removed))
                .map(|mut t| {
                    if t.status == TaskStatus::Active {
                        t.status = TaskStatus::Paused;
                    }
                    t.download_speed = 0;
                    t.upload_speed = 0;
                    t.connections = 0;
                    if t.seeder && t.seeding_since > 0 {
                        t.seeding_since = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                    }
                    t
                })
                .collect(),
            Err(e) => {
                tracing::warn!("Failed to parse engine session: {}", e);
                Vec::new()
            }
        }
    }

    pub fn encode(tasks: &[DownloadTask]) -> Result<Vec<u8>, String> {
        struct Persisted<'a>(&'a [DownloadTask]);
        impl Serialize for Persisted<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_seq(
                    self.0
                        .iter()
                        .filter(|t| !matches!(t.status, TaskStatus::Removed)),
                )
            }
        }
        #[derive(Serialize)]
        struct Out<'a> {
            version: u32,
            tasks: Persisted<'a>,
        }
        serde_json::to_vec(&Out {
            version: 1,
            tasks: Persisted(tasks),
        })
        .map_err(|e| e.to_string())
    }

    pub fn write(&self, data: &[u8]) -> Result<(), String> {
        let new_hash = {
            let mut hasher = DefaultHasher::new();
            data.hash(&mut hasher);
            hasher.finish()
        };
        if let Ok(guard) = self.last_hash.lock() {
            if *guard == Some(new_hash) && self.path.exists() {
                return Ok(());
            }
        }

        crate::traits::write_file_atomically(&self.path, data)
            .map_err(|e| format!("Failed to write session: {e}"))?;

        if let Ok(mut guard) = self.last_hash.lock() {
            *guard = Some(new_hash);
        }

        Ok(())
    }

    pub fn save(&self, tasks: &[DownloadTask]) -> Result<(), String> {
        self.write(&Self::encode(tasks)?)
    }

    pub fn cleanup_legacy(config_dir: &Path) {
        let legacy = config_dir.join("download.session");
        if legacy.exists() {
            let _ = fs::remove_file(&legacy);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Map;
    use tempfile::TempDir;

    fn make_http_task(gid: &str, status: TaskStatus) -> DownloadTask {
        let mut task = DownloadTask::new_http(
            gid.into(),
            vec!["http://example.com/file.zip".into()],
            "/dl".into(),
            None,
            Map::new(),
        );
        task.status = status;
        task
    }

    #[test]
    fn load_restarts_seed_clock_and_save_leaves_no_temp_files() {
        let dir = TempDir::new().unwrap();
        let mgr = SessionManager::new(dir.path());
        let mut task = make_http_task("gid1", TaskStatus::Active);
        task.seeder = true;
        task.seeding_since = 1;
        mgr.save(&[task]).unwrap();
        let loaded = mgr.load();
        assert!(loaded[0].seeding_since > 1);
        let leftovers: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .filter(|e| e.file_name().to_string_lossy().contains("tmp"))
            .collect();
        assert!(leftovers.is_empty());
    }

    #[test]
    fn save_and_load_round_trip() {
        let dir = TempDir::new().unwrap();
        let mgr = SessionManager::new(dir.path());

        let tasks = vec![
            make_http_task("gid1", TaskStatus::Paused),
            make_http_task("gid2", TaskStatus::Complete),
        ];

        mgr.save(&tasks).unwrap();
        let loaded = mgr.load();

        assert_eq!(loaded.len(), 2);
        assert_eq!(loaded[0].gid, "gid1");
        assert_eq!(loaded[0].status, TaskStatus::Paused);
        assert_eq!(loaded[1].gid, "gid2");
        assert_eq!(loaded[1].status, TaskStatus::Complete);
    }

    #[test]
    fn load_missing_file_returns_empty() {
        let dir = TempDir::new().unwrap();
        let mgr = SessionManager::new(dir.path());
        assert!(mgr.load().is_empty());
    }

    #[test]
    fn load_corrupt_json_returns_empty() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join(SESSION_FILENAME);
        fs::write(&path, "not valid json {{{").unwrap();

        let mgr = SessionManager::new(dir.path());
        assert!(mgr.load().is_empty());
    }

    #[test]
    fn active_tasks_become_paused_on_load() {
        let dir = TempDir::new().unwrap();
        let mgr = SessionManager::new(dir.path());

        let mut task = make_http_task("gid1", TaskStatus::Active);
        task.download_speed = 1000;
        task.connections = 5;

        mgr.save(&[task]).unwrap();
        let loaded = mgr.load();

        assert_eq!(loaded[0].status, TaskStatus::Paused);
        assert_eq!(loaded[0].download_speed, 0);
        assert_eq!(loaded[0].connections, 0);
    }

    #[test]
    fn removed_tasks_filtered_on_save_and_load() {
        let dir = TempDir::new().unwrap();
        let mgr = SessionManager::new(dir.path());

        let tasks = vec![
            make_http_task("kept", TaskStatus::Paused),
            make_http_task("gone", TaskStatus::Removed),
        ];

        mgr.save(&tasks).unwrap();
        let loaded = mgr.load();

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].gid, "kept");
    }

    #[test]
    fn cleanup_legacy_removes_old_session() {
        let dir = TempDir::new().unwrap();
        let legacy = dir.path().join("download.session");
        fs::write(&legacy, "old data").unwrap();
        assert!(legacy.exists());

        SessionManager::cleanup_legacy(dir.path());
        assert!(!legacy.exists());
    }

    #[test]
    fn cleanup_legacy_no_op_when_missing() {
        let dir = TempDir::new().unwrap();
        SessionManager::cleanup_legacy(dir.path());
    }

    #[test]
    fn encode_skips_removed_tasks_and_runtime_peers() {
        let mut live = make_http_task("live", TaskStatus::Active);
        live.peers.push(crate::engine::task::PeerInfo {
            ip: "1.2.3.4".into(),
            port: "1".into(),
            percent: 0,
            am_choking: "true".into(),
            peer_choking: "true".into(),
            seeder: "false".into(),
            peer_id: String::new(),
            peer_client_name: String::new(),
            am_interested: String::new(),
            peer_interested: String::new(),
            download_speed: 0,
            upload_speed: 0,
            downloaded: 0,
            uploaded: 0,
            progress: 0.0,
            incoming: false,
            snubbed: false,
            handshaking: false,
            optimistic_unchoke: false,
            bitfield: String::new(),
            raw_bitfield: std::sync::Arc::from([0xffu8].as_slice()),
        });
        let tasks = vec![live, make_http_task("gone", TaskStatus::Removed)];
        let bytes = SessionManager::encode(&tasks).unwrap();
        let text = String::from_utf8(bytes.clone()).unwrap();
        assert!(!text.contains("gone"));
        assert!(!text.contains("1.2.3.4"));
        let parsed: SessionData = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.tasks.len(), 1);
    }
}
