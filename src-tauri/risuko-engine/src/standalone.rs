use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::{Map, Value};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::config::defaults;
use crate::engine::events::EventBroadcaster;
use crate::engine::headless_policy::PolicyTask;
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

fn require_rpc_secret(options: &mut EngineOptions) {
    if options.rpc_secret().is_empty() {
        tracing::warn!(
            "system.json could not be read or updated; using a temporary RPC secret for this run"
        );
        options.set(
            "rpc-secret".into(),
            Value::from(crate::config::generate_rpc_secret()),
        );
    }
}

fn load_options_with_rpc_secret(config_dir: &Path, enable_rpc: bool) -> EngineOptions {
    let secret_settled = crate::config::ensure_rpc_secret_on_disk(config_dir);
    let mut options = load_engine_options(config_dir);
    if enable_rpc && !secret_settled {
        require_rpc_secret(&mut options);
    }
    options
}

async fn abort_start(policy_task: Option<PolicyTask>, manager: &TaskManager) {
    if let Some(policy) = policy_task {
        policy.stop().await;
    }
    manager.shutdown().await;
}

pub struct StandaloneConfig {
    pub config_dir: PathBuf,
    pub rpc_port: Option<u16>,
    pub enable_rpc: bool,
    pub require_download_dir: bool,
    pub headless_policies: bool,
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
    policy_task: Option<PolicyTask>,
}

impl StandaloneEngine {
    pub async fn start(cfg: StandaloneConfig) -> Result<Self, String> {
        let config_dir = cfg.config_dir;
        std::fs::create_dir_all(&config_dir)
            .map_err(|e| format!("Failed to create config dir: {e}"))?;
        tracing::debug!("Config directory: {}", config_dir.display());
        crate::engine::set_file_usenet_credential_resolver(config_dir.clone()).await;

        let mut options = load_options_with_rpc_secret(&config_dir, cfg.enable_rpc);
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

        let policy_task = cfg.headless_policies.then(|| {
            tracing::info!(
                "headless policies: auto-retry and completion script are handled by the engine"
            );
            crate::engine::headless_policy::spawn(manager.clone(), &events, &config_dir)
        });

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
                abort_start(policy_task, &manager).await;
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
                    abort_start(policy_task, &manager).await;
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

        let (progress_task, auto_save_task) = crate::engine::spawn_maintenance_loops(&manager);

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
            policy_task,
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
        if let Some(policy) = self.policy_task.take() {
            policy.stop().await;
        }
        self.manager.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unreadable_system_config_never_leaves_rpc_without_a_secret() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("system.json"), "{").unwrap();
        assert!(!crate::config::ensure_rpc_secret_on_disk(dir.path()));

        let mut options = load_engine_options(dir.path());
        assert!(options.rpc_secret().is_empty());
        require_rpc_secret(&mut options);
        let secret = options.rpc_secret();
        assert!(!secret.is_empty());
        require_rpc_secret(&mut options);
        assert_eq!(options.rpc_secret(), secret);

        let fresh = tempfile::tempdir().unwrap();
        assert!(crate::config::ensure_rpc_secret_on_disk(fresh.path()));
        assert!(!load_engine_options(fresh.path()).rpc_secret().is_empty());
    }

    #[test]
    fn rpc_gets_a_secret_unless_the_user_cleared_it() {
        let fresh = tempfile::tempdir().unwrap();
        let options = load_options_with_rpc_secret(fresh.path(), true);
        assert_eq!(options.rpc_secret().len(), 64);
        assert_eq!(
            load_engine_options(fresh.path()).rpc_secret(),
            options.rpc_secret()
        );

        let corrupt = tempfile::tempdir().unwrap();
        std::fs::write(corrupt.path().join("system.json"), "{").unwrap();
        assert!(!load_options_with_rpc_secret(corrupt.path(), true)
            .rpc_secret()
            .is_empty());
        assert!(load_options_with_rpc_secret(corrupt.path(), false)
            .rpc_secret()
            .is_empty());
        assert_eq!(
            std::fs::read_to_string(corrupt.path().join("system.json")).unwrap(),
            "{"
        );

        let cleared = tempfile::tempdir().unwrap();
        std::fs::write(
            cleared.path().join("system.json"),
            r#"{"rpc-secret": "", "rpc-secret-initialized": true}"#,
        )
        .unwrap();
        assert!(load_options_with_rpc_secret(cleared.path(), true)
            .rpc_secret()
            .is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn a_secret_that_cannot_be_saved_still_protects_rpc() {
        let root = tempfile::tempdir().unwrap();
        let config_dir = root.path().join("config");
        std::os::unix::fs::symlink(root.path().join("missing"), &config_dir).unwrap();
        assert!(!crate::config::ensure_rpc_secret_on_disk(&config_dir));

        let guarded = load_options_with_rpc_secret(&config_dir, true).rpc_secret();
        assert_eq!(guarded.len(), 64);
        assert!(load_options_with_rpc_secret(&config_dir, false)
            .rpc_secret()
            .is_empty());
        assert!(!root.path().join("missing").exists());
    }

    #[tokio::test]
    async fn a_start_that_fails_on_the_rpc_port_returns_the_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut system = Map::new();
        system.insert(
            "dir".into(),
            Value::from(dir.path().join("downloads").to_string_lossy().to_string()),
        );
        for key in [
            "bt-enable-upnp",
            "bt-enable-lsd",
            "ed2k-enable-kad",
            "ed2k-enable-upload",
        ] {
            system.insert(key.into(), Value::from(false));
        }
        std::fs::write(
            dir.path().join("system.json"),
            Value::Object(system).to_string(),
        )
        .unwrap();
        let taken = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = taken.local_addr().unwrap().port();

        let started = StandaloneEngine::start(StandaloneConfig {
            config_dir: dir.path().to_path_buf(),
            rpc_port: Some(port),
            enable_rpc: true,
            require_download_dir: true,
            headless_policies: true,
        })
        .await;

        let error = started.err().expect("the RPC port is taken");
        assert!(error.contains("Failed to start RPC server"), "{error}");
    }

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
