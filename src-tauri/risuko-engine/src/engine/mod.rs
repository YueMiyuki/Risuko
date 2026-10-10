pub mod cookie_store;
pub mod dns;
pub mod ed2k;
pub mod error_code;
pub mod events;
pub mod falloc;
pub mod ftp;
pub mod hasher;
pub mod http;
pub mod m3u8;
pub mod manager;
pub mod media;
pub mod metalink;
pub mod netrc;
pub mod options;
pub mod routing;
pub mod rpc;
pub mod rss;
pub mod session;
pub mod speed_limiter;
pub(crate) mod ssh_auth;
pub mod ssh_known_hosts;
pub mod stats;
pub mod task;
pub mod torrent;
pub mod upload;
pub mod uri_selector;
pub mod usenet;
pub mod usenet_par2;
pub mod usenet_pipeline;
pub mod usenet_transport;
pub mod usenet_worker;
pub(crate) mod util;

pub mod adc;
pub mod archive_pipeline;
pub mod archive_safety;
pub mod g2;
pub mod gift;
pub mod gnutella;

#[cfg(test)]
mod p2p_tests;

pub use session::SESSION_FILENAME;
pub use util::is_windows_device_name;

pub const CHUNK_META_SUFFIX: &str = ".chunks";

pub const STARTUP_ONLY_KEYS: &[&str] = &[
    "rpc-listen-port",
    "rpc-secret",
    "pbh-enable",
    "pbh-listen-port",
    "pbh-rpc-secret",
    "listen-port",
    "dht-listen-port",
    "ed2k-port",
    "ed2k-enable-kad",
    "ed2k-kad-port",
    "bt-max-peers-per-torrent",
    "bt-max-outstanding-per-peer",
    "bt-ban-corrupt-peers",
    "bt-ban-corrupt-strikes",
    "bt-enable-upnp",
    "bt-upnp-lease",
    "bt-enable-lsd",
    "bt-encryption-policy",
    "bt-listen-v6",
];

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::sync::RwLock;

use crate::config::ConfigManager;
use crate::traits::EventSink;

use self::events::EventBroadcaster;
use self::manager::TaskManager;
use self::options::EngineOptions;
use self::rpc::{RpcCompatMode, RpcServer};
use self::upload::UploadSinkManager;

static ENGINE_INSTANCE: std::sync::LazyLock<Mutex<Option<EngineInstance>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));

static USENET_CREDENTIAL_RESOLVER: std::sync::LazyLock<
    RwLock<Option<std::sync::Arc<dyn usenet::UsenetCredentialResolver>>>,
> = std::sync::LazyLock::new(|| RwLock::new(None));

pub async fn set_usenet_credential_resolver(
    resolver: std::sync::Arc<dyn usenet::UsenetCredentialResolver>,
) {
    *USENET_CREDENTIAL_RESOLVER.write().await = Some(resolver);
}

pub async fn set_file_usenet_credential_resolver(config_dir: impl Into<PathBuf>) {
    set_usenet_credential_resolver(Arc::new(FileUsenetCredentialResolver::new(config_dir))).await;
}

pub async fn ensure_file_usenet_credential_resolver(config_dir: impl Into<PathBuf>) {
    let mut guard = USENET_CREDENTIAL_RESOLVER.write().await;
    if guard.is_none() {
        *guard = Some(Arc::new(FileUsenetCredentialResolver::new(config_dir)));
    }
}

pub fn usenet_credential_fallback_path(config_dir: &Path) -> PathBuf {
    config_dir.join("usenet-credentials.json")
}

pub struct FileUsenetCredentialResolver {
    path: PathBuf,
}

impl FileUsenetCredentialResolver {
    pub fn new(config_dir: impl Into<PathBuf>) -> Self {
        Self {
            path: usenet_credential_fallback_path(&config_dir.into()),
        }
    }
}

#[async_trait::async_trait]
impl usenet::UsenetCredentialResolver for FileUsenetCredentialResolver {
    async fn resolve(&self, profile_id: &str) -> Result<Option<usenet::UsenetCredentials>, String> {
        let path = self.path.clone();
        let text = match tokio::task::spawn_blocking(move || std::fs::read_to_string(path))
            .await
            .map_err(|error| format!("Failed to read Usenet credentials: {error}"))?
        {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(format!("Failed to read Usenet credentials: {error}")),
        };
        let root: serde_json::Value = serde_json::from_str(&text)
            .map_err(|error| format!("Failed to parse Usenet credentials: {error}"))?;
        let Some(entry) = root.get(profile_id).and_then(serde_json::Value::as_object) else {
            return Ok(None);
        };
        let username = entry
            .get("username")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let password = entry
            .get("password")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        if username.is_none() && password.is_none() {
            Ok(None)
        } else {
            Ok(Some(usenet::UsenetCredentials { username, password }))
        }
    }
}

pub async fn usenet_credential_resolver() -> std::sync::Arc<dyn usenet::UsenetCredentialResolver> {
    USENET_CREDENTIAL_RESOLVER
        .read()
        .await
        .clone()
        .unwrap_or_else(|| {
            std::sync::Arc::new(crate::engine::usenet_worker::AnonymousCredentialResolver)
        })
}

static ENGINE_STARTED_AT: std::sync::Mutex<Option<Instant>> = std::sync::Mutex::new(None);

static STARTUP_SNAPSHOT: std::sync::Mutex<
    Option<std::collections::HashMap<String, serde_json::Value>>,
> = std::sync::Mutex::new(None);

pub fn engine_uptime() -> Option<Duration> {
    ENGINE_STARTED_AT
        .lock()
        .ok()
        .and_then(|g| g.as_ref().map(|t| Instant::now().duration_since(*t)))
}

pub fn startup_snapshot() -> Option<std::collections::HashMap<String, serde_json::Value>> {
    STARTUP_SNAPSHOT.lock().ok().and_then(|g| g.clone())
}

struct EngineInstance {
    manager: Arc<TaskManager>,
    rpc_server: RpcServer,
    pbh_rpc_server: Option<RpcServer>,
    progress_task: tokio::task::JoinHandle<()>,
    auto_save_task: tokio::task::JoinHandle<()>,
    event_bridge_task: tokio::task::JoinHandle<()>,
}

fn parse_config_bool(value: Option<&serde_json::Value>) -> bool {
    value.and_then(options::json_bool).unwrap_or(false)
}

pub fn should_start_embedded_engine(config: &ConfigManager) -> bool {
    let external_enabled =
        parse_config_bool(config.get_user_config().get("external-engine-enabled"));

    !external_enabled
}

pub async fn start_engine(
    config: &ConfigManager,
    event_sink: Arc<dyn EventSink>,
    upload_sinks: Option<Arc<UploadSinkManager>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if !should_start_embedded_engine(config) {
        tracing::info!("Embedded engine start skipped because external engine mode is enabled");
        return Ok(());
    }

    {
        let guard = ENGINE_INSTANCE.lock().await;
        if guard.is_some() {
            tracing::info!("Engine already running");
            return Ok(());
        }
    }

    let config_dir = config.config_dir().to_path_buf();
    ensure_file_usenet_credential_resolver(config_dir.clone()).await;
    let system = config.get_system_config();
    let user = config.get_user_config();
    let options = EngineOptions::from_config(system, user);

    std::fs::create_dir_all(&config_dir)?;

    let dir = options.dir();
    if !dir.is_empty() {
        std::fs::create_dir_all(&dir).ok();
    }

    let events = EventBroadcaster::default();
    let rpc_host = options.rpc_host();
    let rpc_port = options.rpc_listen_port();
    let rpc_secret = options.rpc_secret();
    let pbh_config = options.pbh_rpc_config(rpc_port)?;

    tracing::info!("Starting Risuko engine (in-process)");

    let manager = Arc::new(
        TaskManager::new(&config_dir, options, events.clone())
            .await
            .map_err(|e| format!("Failed to create task manager: {}", e))?,
    );

    let (rpc_shutdown_tx, mut rpc_shutdown_rx) = tokio::sync::mpsc::channel::<()>(1);

    let mut rpc_server = RpcServer::new(
        rpc_host.clone(),
        rpc_port,
        rpc_secret,
        manager.clone(),
        events.clone(),
        rpc_shutdown_tx.clone(),
    );
    rpc_server
        .start()
        .await
        .map_err(|e| format!("Failed to start RPC server: {}", e))?;

    let pbh_rpc_server = if let Some(pbh) = pbh_config {
        let pbh_listen_port = pbh.port;
        let mut server = RpcServer::new_with_compat(
            rpc_host,
            pbh.port,
            pbh.secret,
            manager.clone(),
            events.clone(),
            rpc_shutdown_tx,
            RpcCompatMode::Aria2Next,
        );
        if let Err(e) = server.start().await {
            rpc_server.stop();
            manager.shutdown().await;
            return Err(format!("Failed to start PeerBanHelper RPC server: {e}").into());
        }
        tracing::info!(
            "PeerBanHelper Aria2Next RPC listening on port {}",
            pbh_listen_port
        );
        Some(server)
    } else {
        drop(rpc_shutdown_tx);
        None
    };

    let mgr_for_progress = manager.clone();
    let progress_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let mgr = mgr_for_progress.clone();
            if let Err(e) = tokio::spawn(async move { mgr.update_progress().await }).await {
                tracing::error!("Progress update panicked: {}", e);
            }
        }
    });

    let mgr_for_save = manager.clone();
    let auto_save_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let mgr = mgr_for_save.clone();
            match tokio::spawn(async move { mgr.save_session().await }).await {
                Ok(Err(e)) => tracing::warn!("Auto-save session failed: {}", e),
                Err(e) => tracing::error!("Auto-save session panicked: {}", e),
                Ok(Ok(_)) => {}
            }
        }
    });

    let sink = event_sink.clone();
    let mut event_rx = events.subscribe();
    let mgr_for_uploads = manager.clone();
    let upload_mgr = upload_sinks.clone();
    let event_bridge_task = tokio::spawn(async move {
        use events::EngineEvent;
        loop {
            match event_rx.recv().await {
                Ok(event) => {
                    let (name, gid) = match &event {
                        EngineEvent::DownloadStart { gid } => {
                            ("engine:download-start", gid.as_str())
                        }
                        EngineEvent::DownloadPause { gid } => {
                            ("engine:download-pause", gid.as_str())
                        }
                        EngineEvent::DownloadStop { gid } => ("engine:download-stop", gid.as_str()),
                        EngineEvent::DownloadComplete { gid } => {
                            ("engine:download-complete", gid.as_str())
                        }
                        EngineEvent::DownloadError { gid } => {
                            ("engine:download-error", gid.as_str())
                        }
                        EngineEvent::BtDownloadComplete { gid } => {
                            ("engine:bt-download-complete", gid.as_str())
                        }
                    };
                    let payload = serde_json::json!({ "gid": gid });
                    sink.emit(name, payload);

                    if matches!(event, EngineEvent::DownloadComplete { .. }) {
                        if let Some(uploads) = upload_mgr.clone() {
                            let mgr = mgr_for_uploads.clone();
                            let gid_owned = gid.to_string();
                            tokio::spawn(async move {
                                let Some((files, kind, override_id)) =
                                    mgr.files_for_upload(&gid_owned).await
                                else {
                                    return;
                                };
                                for f in files {
                                    uploads
                                        .enqueue_for_file(
                                            &gid_owned,
                                            f.local_path,
                                            f.remote_relative,
                                            f.size,
                                            f.category,
                                            &kind,
                                            override_id.clone(),
                                        )
                                        .await;
                                }
                            });
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("Event bridge lagged by {} events", n);
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    });

    session::SessionManager::cleanup_legacy(&config_dir);

    let instance = EngineInstance {
        manager,
        rpc_server,
        pbh_rpc_server,
        progress_task,
        auto_save_task,
        event_bridge_task,
    };

    *ENGINE_INSTANCE.lock().await = Some(instance);

    if let Ok(mut g) = STARTUP_SNAPSHOT.lock() {
        let mut snap = std::collections::HashMap::new();
        for key in STARTUP_ONLY_KEYS {
            let v = user
                .get(*key)
                .or_else(|| system.get(*key))
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            snap.insert((*key).to_string(), v);
        }
        *g = Some(snap);
    }
    if let Ok(mut g) = ENGINE_STARTED_AT.lock() {
        *g = Some(Instant::now());
    }

    tokio::spawn(async move {
        if rpc_shutdown_rx.recv().await.is_some() {
            tracing::info!("Shutdown requested via RPC");
            if let Err(e) = stop_engine().await {
                tracing::error!("Failed to stop engine via RPC shutdown: {}", e);
            }
        }
    });

    tracing::info!("Risuko engine started on port {}", rpc_port);
    Ok(())
}

pub async fn stop_engine() -> Result<(), Box<dyn std::error::Error>> {
    let mut guard = ENGINE_INSTANCE.lock().await;
    if let Some(mut instance) = guard.take() {
        instance.progress_task.abort();
        instance.auto_save_task.abort();
        instance.event_bridge_task.abort();

        instance.rpc_server.stop();
        if let Some(mut pbh) = instance.pbh_rpc_server {
            pbh.stop();
        }

        instance.manager.shutdown().await;
    }
    drop(guard);

    if let Ok(mut g) = ENGINE_STARTED_AT.lock() {
        *g = None;
    }

    tracing::info!("Risuko engine stopped");
    Ok(())
}

pub async fn restart_engine(
    config: &ConfigManager,
    event_sink: Arc<dyn EventSink>,
    upload_sinks: Option<Arc<UploadSinkManager>>,
) -> Result<(), Box<dyn std::error::Error>> {
    stop_engine().await?;
    if should_start_embedded_engine(config) {
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        start_engine(config, event_sink, upload_sinks).await?;
    }
    Ok(())
}

pub async fn reload_p2p_profile(config: &ConfigManager) -> Result<(), Box<dyn std::error::Error>> {
    if !should_start_embedded_engine(config) {
        tracing::info!("Skipping P2P profile reload because the embedded engine is disabled");
        return Ok(());
    }
    let Some(manager) = get_manager().await else {
        tracing::info!("Skipping P2P profile reload because the engine manager is unavailable");
        return Ok(());
    };
    let options = EngineOptions::from_config(config.get_system_config(), config.get_user_config());
    manager
        .reload_p2p_profile(options)
        .await
        .map_err(|error| error.into())
}

pub async fn get_manager() -> Option<Arc<TaskManager>> {
    let guard = ENGINE_INSTANCE.lock().await;
    guard.as_ref().map(|i| i.manager.clone())
}

#[cfg(test)]
mod credential_tests {
    use super::*;
    use crate::engine::usenet::UsenetCredentialResolver;
    use serde_json::json;
    use tempfile::TempDir;

    #[tokio::test]
    async fn file_resolver_reads_durable_credentials() {
        let dir = TempDir::new().unwrap();
        let path = usenet_credential_fallback_path(dir.path());
        std::fs::write(
            &path,
            serde_json::to_vec(&json!({
                "primary": { "username": "alice", "password": "secret" }
            }))
            .unwrap(),
        )
        .unwrap();

        let resolver = FileUsenetCredentialResolver::new(dir.path());
        let credentials = resolver.resolve("primary").await.unwrap().unwrap();
        assert_eq!(credentials.username.as_deref(), Some("alice"));
        assert_eq!(credentials.password.as_deref(), Some("secret"));
        assert!(resolver.resolve("missing").await.unwrap().is_none());
    }
}
