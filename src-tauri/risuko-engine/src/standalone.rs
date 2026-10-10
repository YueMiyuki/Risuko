use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::config::defaults;
use crate::engine::events::EventBroadcaster;
use crate::engine::manager::TaskManager;
use crate::engine::options::EngineOptions;
use crate::engine::rpc::{RpcCompatMode, RpcServer};

const APP_CONFIG_DIR_NAME: &str = "app.risuko.Risuko";
const LEGACY_CONFIG_DIR_NAME: &str = "dev.risuko.app";

pub fn gui_config_dir() -> PathBuf {
    dirs::config_dir()
        .map(|base| pick_config_dir(&base))
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn standalone_config_dir() -> PathBuf {
    dirs::config_dir()
        .map(|base| pick_standalone_dir(&base))
        .unwrap_or_else(|| PathBuf::from("."))
}

fn pick_standalone_dir(base: &Path) -> PathBuf {
    let legacy_dir = base.join(LEGACY_CONFIG_DIR_NAME);
    if legacy_dir.exists() {
        legacy_dir
    } else {
        base.join(APP_CONFIG_DIR_NAME)
    }
}

fn pick_config_dir(base: &Path) -> PathBuf {
    let app_dir = base.join(APP_CONFIG_DIR_NAME);
    let legacy_dir = base.join(LEGACY_CONFIG_DIR_NAME);
    if !app_dir.exists() && legacy_dir.exists() {
        legacy_dir
    } else {
        app_dir
    }
}

pub fn load_config(path: &Path, defaults: Map<String, Value>) -> Map<String, Value> {
    if let Ok(data) = std::fs::read_to_string(path) {
        if let Ok(Value::Object(mut map)) = serde_json::from_str(&data) {
            for (k, v) in &defaults {
                if !map.contains_key(k) {
                    map.insert(k.clone(), v.clone());
                }
            }
            return map;
        }
    }
    defaults
}

pub fn load_engine_options(config_dir: &Path) -> EngineOptions {
    let system = load_config(&config_dir.join("system.json"), defaults::system_defaults());
    let user = load_config(&config_dir.join("user.json"), defaults::user_defaults());
    EngineOptions::from_config(&system, &user)
}

pub struct StandaloneConfig {
    pub config_dir: PathBuf,
    pub rpc_port: Option<u16>,
    pub enable_rpc: bool,
    pub require_download_dir: bool,
}

pub struct StandaloneEngine {
    pub manager: Arc<TaskManager>,
    pub events: EventBroadcaster,
    pub rpc_secret: Option<String>,
    shutdown_notify: Arc<Notify>,
    rpc_server: RpcServer,
    pbh_rpc_server: Option<RpcServer>,
    progress_task: JoinHandle<()>,
    auto_save_task: JoinHandle<()>,
}

impl StandaloneEngine {
    pub async fn start(cfg: StandaloneConfig) -> Result<Self, String> {
        let config_dir = cfg.config_dir;
        std::fs::create_dir_all(&config_dir)
            .map_err(|e| format!("Failed to create config dir: {e}"))?;
        tracing::debug!("Config directory: {}", config_dir.display());
        crate::engine::set_file_usenet_credential_resolver(config_dir.clone()).await;
        crate::config::ensure_rpc_secret_on_disk(&config_dir);

        let mut options = load_engine_options(&config_dir);
        if let Some(port) = cfg.rpc_port {
            options.set("rpc-listen-port".into(), Value::from(port));
        }

        let dir = options.dir();
        if !dir.is_empty() {
            match std::fs::create_dir_all(&dir) {
                Ok(()) => tracing::debug!("Download directory: {}", dir),
                Err(e) if cfg.require_download_dir => {
                    return Err(format!("Failed to create download directory '{dir}': {e}"));
                }
                Err(_) => {}
            }
        }

        let events = EventBroadcaster::default();
        let rpc_host = options.rpc_host();
        let rpc_port = options.rpc_listen_port();
        let rpc_secret = options.rpc_secret();
        let pbh_config = if cfg.enable_rpc {
            options.pbh_rpc_config(rpc_port)?
        } else {
            None
        };

        let manager = Arc::new(
            TaskManager::new(&config_dir, options, events.clone())
                .await
                .map_err(|e| format!("Failed to create task manager: {e}"))?,
        );

        let (rpc_shutdown_tx, mut rpc_shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

        let mut rpc_server = RpcServer::new(
            rpc_host.clone(),
            rpc_port,
            rpc_secret.clone(),
            manager.clone(),
            events.clone(),
            rpc_shutdown_tx.clone(),
        );
        let mut pbh_rpc_server = None;
        if cfg.enable_rpc {
            if let Err(e) = rpc_server.start().await {
                manager.shutdown().await;
                return Err(format!("Failed to start RPC server: {e}"));
            }
            tracing::info!("RPC server listening on {}:{}", rpc_host, rpc_port);
            if let Some(pbh) = pbh_config {
                let mut server = RpcServer::new_with_compat(
                    rpc_host.clone(),
                    pbh.port,
                    pbh.secret,
                    manager.clone(),
                    events.clone(),
                    rpc_shutdown_tx.clone(),
                    RpcCompatMode::Aria2Next,
                );
                if let Err(e) = server.start().await {
                    rpc_server.stop();
                    manager.shutdown().await;
                    return Err(format!("Failed to start PeerBanHelper RPC server: {e}"));
                }
                tracing::info!(
                    "PeerBanHelper Aria2Next RPC listening on {}:{}",
                    rpc_host,
                    pbh.port
                );
                pbh_rpc_server = Some(server);
            }
        }
        drop(rpc_shutdown_tx);

        let mgr = manager.clone();
        let progress_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                mgr.update_progress().await;
            }
        });

        let mgr = manager.clone();
        let auto_save_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(e) = mgr.save_session().await {
                    tracing::warn!("Auto-save session failed: {}", e);
                }
            }
        });

        // notify_one stores a permit so a late waiter still wakes
        let shutdown_notify = Arc::new(Notify::new());
        let notify = shutdown_notify.clone();
        tokio::spawn(async move {
            if rpc_shutdown_rx.recv().await.is_some() {
                tracing::info!("Shutdown requested via RPC");
                notify.notify_one();
            }
        });

        Ok(Self {
            manager,
            events,
            rpc_secret: Some(rpc_secret).filter(|s| !s.is_empty()),
            shutdown_notify,
            rpc_server,
            pbh_rpc_server,
            progress_task,
            auto_save_task,
        })
    }

    pub fn shutdown_signal(&self) -> Arc<Notify> {
        self.shutdown_notify.clone()
    }

    pub async fn stop(mut self) {
        self.progress_task.abort();
        self.auto_save_task.abort();
        self.rpc_server.stop();
        if let Some(mut pbh) = self.pbh_rpc_server.take() {
            pbh.stop();
        }
        self.manager.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_identifier_matches_tauri_config() {
        let conf: Value = serde_json::from_str(include_str!("../../tauri.conf.json")).unwrap();
        assert_eq!(conf["identifier"], APP_CONFIG_DIR_NAME);
    }

    #[test]
    fn config_dir_prefers_gui_dir_and_falls_back_to_legacy() {
        let base = tempfile::tempdir().unwrap();
        assert_eq!(
            pick_config_dir(base.path()),
            base.path().join(APP_CONFIG_DIR_NAME)
        );
        std::fs::create_dir(base.path().join(LEGACY_CONFIG_DIR_NAME)).unwrap();
        assert_eq!(
            pick_config_dir(base.path()),
            base.path().join(LEGACY_CONFIG_DIR_NAME)
        );
        std::fs::create_dir(base.path().join(APP_CONFIG_DIR_NAME)).unwrap();
        assert_eq!(
            pick_config_dir(base.path()),
            base.path().join(APP_CONFIG_DIR_NAME)
        );
    }

    #[test]
    fn standalone_dir_keeps_an_existing_legacy_dir() {
        let base = tempfile::tempdir().unwrap();
        assert_eq!(
            pick_standalone_dir(base.path()),
            base.path().join(APP_CONFIG_DIR_NAME)
        );
        std::fs::create_dir(base.path().join(LEGACY_CONFIG_DIR_NAME)).unwrap();
        std::fs::create_dir(base.path().join(APP_CONFIG_DIR_NAME)).unwrap();
        assert_eq!(
            pick_standalone_dir(base.path()),
            base.path().join(LEGACY_CONFIG_DIR_NAME)
        );
    }

    #[test]
    fn load_config_fills_defaults_and_survives_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.json");
        let mut defaults = Map::new();
        defaults.insert("a".into(), Value::from(1));
        defaults.insert("b".into(), Value::from(2));

        assert_eq!(load_config(&path, defaults.clone()), defaults);
        std::fs::write(&path, "not json").unwrap();
        assert_eq!(load_config(&path, defaults.clone()), defaults);
        std::fs::write(&path, r#"{"a": 9}"#).unwrap();
        let loaded = load_config(&path, defaults);
        assert_eq!(loaded["a"], 9);
        assert_eq!(loaded["b"], 2);
    }
}
