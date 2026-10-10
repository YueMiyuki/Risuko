use std::path::PathBuf;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use serde_json::{Map, Value};
use tokio::sync::Mutex;

use risuko_engine::engine::manager::TaskManager;
use risuko_engine::standalone::{standalone_config_dir, StandaloneConfig, StandaloneEngine};

struct NapiEngine {
    engine: StandaloneEngine,
    event_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

static ENGINE: std::sync::LazyLock<Mutex<Option<NapiEngine>>> =
    std::sync::LazyLock::new(|| Mutex::new(None));

#[napi(object)]
pub struct EngineConfig {
    /// Custom config directory (default: OS config dir / app.risuko.Risuko, or the legacy dev.risuko.app when only that exists)
    pub config_dir: Option<String>,
    /// RPC listen port
    pub rpc_port: Option<u16>,
    /// Whether to start the RPC server (default: true)
    pub enable_rpc: Option<bool>,
}

#[napi]
pub async fn start_engine(config: Option<EngineConfig>) -> Result<()> {
    let mut guard = ENGINE.lock().await;
    if guard.is_some() {
        return Err(Error::from_reason("Engine already running"));
    }

    let config_dir = config
        .as_ref()
        .and_then(|c| c.config_dir.as_deref())
        .map(PathBuf::from)
        .unwrap_or_else(standalone_config_dir);

    let engine = StandaloneEngine::start(StandaloneConfig {
        config_dir,
        rpc_port: config.as_ref().and_then(|c| c.rpc_port),
        enable_rpc: config.as_ref().and_then(|c| c.enable_rpc).unwrap_or(true),
        require_download_dir: false,
    })
    .await
    .map_err(Error::from_reason)?;

    let shutdown_signal = engine.shutdown_signal();
    *guard = Some(NapiEngine {
        engine,
        event_task: Arc::new(Mutex::new(None)),
    });
    drop(guard);

    tokio::spawn(async move {
        shutdown_signal.notified().await;
        tracing::info!("Shutdown requested via RPC (napi)");
        if let Err(e) = stop_engine().await {
            tracing::error!("Failed to stop engine via RPC shutdown: {}", e);
        }
    });

    Ok(())
}

#[napi]
pub async fn stop_engine() -> Result<()> {
    let mut guard = ENGINE.lock().await;
    let engine = guard
        .take()
        .ok_or_else(|| Error::from_reason("Engine not running"))?;
    if let Some(handle) = engine.event_task.lock().await.take() {
        handle.abort();
    }
    engine.engine.stop().await;
    Ok(())
}

async fn with_manager<F, Fut, T>(f: F) -> Result<T>
where
    F: FnOnce(Arc<TaskManager>) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let manager = {
        let guard = ENGINE.lock().await;
        let engine = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Engine not running"))?;
        engine.engine.manager.clone()
    };
    f(manager).await
}

#[napi]
pub async fn add_uri(uris: Vec<String>, options: Option<serde_json::Value>) -> Result<String> {
    with_manager(|mgr| async move {
        let opts = to_map(options);
        mgr.add_http_task(uris, opts)
            .await
            .map_err(Error::from_reason)
    })
    .await
}

#[napi]
pub async fn add_torrent(data: Buffer, options: Option<serde_json::Value>) -> Result<String> {
    with_manager(|mgr| async move {
        let opts = to_map(options);
        mgr.add_torrent_task(data.to_vec(), opts)
            .await
            .map_err(Error::from_reason)
    })
    .await
}

#[napi]
pub async fn add_magnet(uri: String, options: Option<serde_json::Value>) -> Result<String> {
    with_manager(|mgr| async move {
        let opts = to_map(options);
        mgr.add_magnet_task(&uri, opts)
            .await
            .map_err(Error::from_reason)
    })
    .await
}

#[napi]
pub async fn add_ed2k(uri: String, options: Option<serde_json::Value>) -> Result<String> {
    with_manager(|mgr| async move {
        let opts = to_map(options);
        mgr.add_ed2k_task(&uri, opts)
            .await
            .map_err(Error::from_reason)
    })
    .await
}

#[napi]
pub async fn add_m3u8(uri: String, options: Option<serde_json::Value>) -> Result<String> {
    with_manager(|mgr| async move {
        let opts = to_map(options);
        mgr.add_m3u8_task(&uri, opts)
            .await
            .map_err(Error::from_reason)
    })
    .await
}

#[napi]
pub async fn add_ftp(uri: String, options: Option<serde_json::Value>) -> Result<String> {
    with_manager(|mgr| async move {
        let opts = to_map(options);
        mgr.add_ftp_task(&uri, opts)
            .await
            .map_err(Error::from_reason)
    })
    .await
}

#[napi]
pub async fn pause(gid: String) -> Result<()> {
    with_manager(|mgr| async move { mgr.pause(&gid).await.map_err(Error::from_reason) }).await
}

#[napi]
pub async fn unpause(gid: String) -> Result<()> {
    with_manager(|mgr| async move { mgr.unpause(&gid).await.map_err(Error::from_reason) }).await
}

#[napi]
pub async fn remove(gid: String) -> Result<()> {
    with_manager(|mgr| async move { mgr.remove(&gid).await.map_err(Error::from_reason) }).await
}

#[napi]
pub async fn pause_all() -> Result<()> {
    with_manager(|mgr| async move {
        mgr.pause_all().await;
        Ok(())
    })
    .await
}

#[napi]
pub async fn unpause_all() -> Result<()> {
    with_manager(|mgr| async move {
        mgr.unpause_all().await;
        Ok(())
    })
    .await
}

#[napi]
pub async fn tell_status(gid: String, keys: Option<Vec<String>>) -> Result<serde_json::Value> {
    with_manager(|mgr| async move {
        let k = keys.unwrap_or_default();
        mgr.tell_status(&gid, &k).await.map_err(Error::from_reason)
    })
    .await
}

#[napi]
pub async fn tell_active(keys: Option<Vec<String>>) -> Result<serde_json::Value> {
    with_manager(|mgr| async move {
        let k = keys.unwrap_or_default();
        Ok(mgr.tell_active(&k).await)
    })
    .await
}

#[napi]
pub async fn tell_waiting(
    offset: i32,
    num: u32,
    keys: Option<Vec<String>>,
) -> Result<serde_json::Value> {
    with_manager(|mgr| async move {
        let k = keys.unwrap_or_default();
        Ok(mgr.tell_waiting(offset as i64, num as usize, &k).await)
    })
    .await
}

#[napi]
pub async fn tell_stopped(
    offset: i32,
    num: u32,
    keys: Option<Vec<String>>,
) -> Result<serde_json::Value> {
    with_manager(|mgr| async move {
        let k = keys.unwrap_or_default();
        Ok(mgr.tell_stopped(offset as i64, num as usize, &k).await)
    })
    .await
}

#[napi]
pub async fn get_global_stat() -> Result<serde_json::Value> {
    with_manager(|mgr| async move { Ok(mgr.get_global_stat().await) }).await
}

#[napi]
pub async fn get_files(gid: String) -> Result<serde_json::Value> {
    with_manager(|mgr| async move { mgr.get_files(&gid).await.map_err(Error::from_reason) }).await
}

#[napi]
pub async fn get_peers(gid: String) -> Result<serde_json::Value> {
    with_manager(|mgr| async move { Ok(mgr.get_peers(&gid).await) }).await
}

#[napi]
pub async fn get_uris(gid: String) -> Result<serde_json::Value> {
    with_manager(|mgr| async move { mgr.get_uris(&gid).await.map_err(Error::from_reason) }).await
}

#[napi]
pub async fn get_option(gid: String) -> Result<serde_json::Value> {
    with_manager(|mgr| async move { mgr.get_option(&gid).await.map_err(Error::from_reason) }).await
}

#[napi]
pub async fn get_global_option() -> Result<serde_json::Value> {
    with_manager(|mgr| async move { Ok(mgr.get_global_option().await) }).await
}

#[napi]
pub async fn change_option(gid: String, options: serde_json::Value) -> Result<()> {
    with_manager(|mgr| async move {
        let opts = value_to_map(options);
        mgr.change_option(&gid, opts)
            .await
            .map_err(Error::from_reason)
    })
    .await
}

#[napi]
pub async fn update_task(gid: String, patch: serde_json::Value) -> Result<serde_json::Value> {
    with_manager(|mgr| async move {
        let patch: risuko_engine::engine::task::TaskPatch = serde_json::from_value(patch)
            .map_err(|e| Error::from_reason(format!("Invalid task patch: {e}")))?;
        let outcome = mgr
            .update_task(&gid, patch)
            .await
            .map_err(Error::from_reason)?;
        serde_json::to_value(outcome).map_err(|e| Error::from_reason(e.to_string()))
    })
    .await
}

#[napi]
pub async fn change_global_option(options: serde_json::Value) -> Result<()> {
    with_manager(|mgr| async move {
        let opts = value_to_map(options);
        mgr.change_global_option(opts).await;
        Ok(())
    })
    .await
}

#[napi]
pub async fn save_session() -> Result<()> {
    with_manager(|mgr| async move { mgr.save_session().await.map_err(Error::from_reason) }).await
}

#[napi]
pub async fn purge_download_result() -> Result<()> {
    with_manager(|mgr| async move {
        mgr.purge_download_result().await;
        Ok(())
    })
    .await
}

#[napi]
pub async fn remove_download_result(gid: String) -> Result<()> {
    with_manager(|mgr| async move {
        mgr.remove_download_result(&gid)
            .await
            .map_err(Error::from_reason)
    })
    .await
}

type EventCallback = napi::threadsafe_function::ThreadsafeFunction<
    FnArgs<(String, String)>,
    napi::bindgen_prelude::Unknown<'static>,
    FnArgs<(String, String)>,
    napi::Status,
    false,
>;

/// Subscribe to engine events; the callback receives (eventName, gid) and returns `Result<()>` on success
#[napi(ts_args_type = "callback: (eventName: string, gid: string) => void")]
pub async fn on_event(callback: EventCallback) -> Result<()> {
    let (event_task, rx) = {
        let guard = ENGINE.lock().await;
        let engine = guard
            .as_ref()
            .ok_or_else(|| Error::from_reason("Engine not running"))?;
        let event_task = Arc::clone(&engine.event_task);
        let rx = engine.engine.events.subscribe();
        (event_task, rx)
    };
    let mut slot = event_task.lock().await;
    if let Some(prev) = slot.take() {
        prev.abort();
    }
    let mut rx = rx;
    let handle = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    let name = event.method_name().to_string();
                    let gid = event.gid().to_string();
                    callback.call(
                        (name, gid).into(),
                        napi::threadsafe_function::ThreadsafeFunctionCallMode::NonBlocking,
                    );
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Err(_) => continue,
            }
        }
    });
    *slot = Some(handle);
    Ok(())
}

fn to_map(val: Option<serde_json::Value>) -> Map<String, Value> {
    match val {
        Some(Value::Object(m)) => m,
        _ => Map::new(),
    }
}

fn value_to_map(val: serde_json::Value) -> Map<String, Value> {
    match val {
        Value::Object(m) => m,
        _ => Map::new(),
    }
}
