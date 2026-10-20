use serde_json::{json, Value};
use std::sync::Arc;
use tauri::State;

use risuko_engine::engine::upload::{SinkConfig, UploadRule, UploadSinkManager, UploadSinkRecord};

use crate::managers::vault::VaultManager;
use crate::state::AppState;

fn extract_sink_secrets(config: &SinkConfig) -> Option<Value> {
    let mut obj = serde_json::Map::new();
    match config {
        SinkConfig::Webdav(c) => {
            if !c.password.is_empty() {
                obj.insert("password".into(), json!(c.password));
            }
        }
        SinkConfig::S3(c) => {
            if !c.secret_access_key.is_empty() {
                obj.insert("secretAccessKey".into(), json!(c.secret_access_key));
            }
        }
        SinkConfig::Sftp(c) => {
            if !c.password.is_empty() {
                obj.insert("password".into(), json!(c.password));
            }
            if !c.private_key.is_empty() {
                obj.insert("privateKey".into(), json!(c.private_key));
            }
        }
        SinkConfig::Ftp(c) => {
            if !c.password.is_empty() {
                obj.insert("password".into(), json!(c.password));
            }
        }
    }
    if obj.is_empty() {
        None
    } else {
        Some(Value::Object(obj))
    }
}

fn apply_sink_secrets(config: &mut SinkConfig, secrets: &Value) {
    let obj = match secrets.as_object() {
        Some(o) => o,
        None => return,
    };
    let s = |k: &str| {
        obj.get(k)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };
    match config {
        SinkConfig::Webdav(c) => {
            if c.password.is_empty() {
                let v = s("password");
                if !v.is_empty() {
                    c.password = v;
                }
            }
        }
        SinkConfig::S3(c) => {
            if c.secret_access_key.is_empty() {
                let v = s("secretAccessKey");
                if !v.is_empty() {
                    c.secret_access_key = v;
                }
            }
        }
        SinkConfig::Sftp(c) => {
            if c.password.is_empty() {
                let pw = s("password");
                if !pw.is_empty() {
                    c.password = pw;
                }
            }
            if c.private_key.is_empty() {
                let pk = s("privateKey");
                if !pk.is_empty() {
                    c.private_key = pk;
                }
            }
        }
        SinkConfig::Ftp(c) => {
            if c.password.is_empty() {
                let v = s("password");
                if !v.is_empty() {
                    c.password = v;
                }
            }
        }
    }
}

async fn fill_from_vault(
    vault: &VaultManager,
    mgr: &UploadSinkManager,
    id: &str,
    config: &mut SinkConfig,
) {
    if vault.enabled() {
        match vault.get_sink(id) {
            Ok(Some(secrets)) => {
                apply_sink_secrets(config, &secrets);
                return;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!("Failed to load vault entry for sink {id}: {e}, trying fallback");
            }
        }
    }
    if let Some(secrets) = mgr.get_sink_secret_fallback(id).await {
        apply_sink_secrets(config, &secrets);
    }
}

async fn persist_sink_secrets(
    vault: &VaultManager,
    mgr: &UploadSinkManager,
    id: &str,
    config: &SinkConfig,
) {
    match extract_sink_secrets(config) {
        Some(v) => {
            if vault.enabled() {
                match vault.put_sink(id, &v) {
                    Ok(()) => {
                        if let Err(e) = mgr.remove_sink_secret_fallback(id).await {
                            tracing::warn!("Failed to clear stale fallback for sink {id}: {e}");
                        }
                        return;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Failed to store sink secrets in vault for {id}: {e}, falling back"
                        );
                    }
                }
            }
            if let Err(e) = mgr.put_sink_secret_fallback(id, &v).await {
                tracing::warn!("Failed to store sink secrets in fallback for {id}: {e}");
            }
        }
        None => {
            if vault.enabled() {
                if let Err(e) = vault.remove_sink(id) {
                    tracing::warn!("Failed to clear vault entry for sink {id}: {e}");
                }
            }
            if let Err(e) = mgr.remove_sink_secret_fallback(id).await {
                tracing::warn!("Failed to clear fallback secrets for sink {id}: {e}");
            }
        }
    }
}

pub async fn rehydrate_upload_sinks(mgr: &UploadSinkManager, vault: &Arc<VaultManager>) {
    let sinks = mgr.list_sinks().await;
    for mut record in sinks {
        let mut secrets: Option<Value> = None;
        let lookup = {
            let vault = vault.clone();
            let id = record.id.clone();
            tokio::task::spawn_blocking(move || vault.enabled().then(|| vault.get_sink(&id))).await
        };
        match lookup {
            Ok(Some(Ok(Some(v)))) => secrets = Some(v),
            Ok(Some(Ok(None))) | Ok(None) => {}
            Ok(Some(Err(e))) => {
                tracing::warn!(
                    "Failed to load vault entry for sink {}: {e}, trying fallback",
                    record.id
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Vault lookup for sink {} did not complete: {e}, trying fallback",
                    record.id
                );
            }
        }
        if secrets.is_none() {
            secrets = mgr.get_sink_secret_fallback(&record.id).await;
        }
        let Some(secrets) = secrets else { continue };
        let id = record.id.clone();
        apply_sink_secrets(&mut record.config, &secrets);
        if let Err(e) = mgr.update_sink(record).await {
            tracing::warn!("Failed to inject secrets into upload manager for sink {id}: {e}");
        }
    }
}

#[tauri::command]
pub async fn list_upload_sinks(state: State<'_, AppState>) -> Result<Value, String> {
    let mgr = state.upload_sinks.clone();
    let sinks = mgr.list_sinks().await;
    serde_json::to_value(sinks).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn add_upload_sink(
    state: State<'_, AppState>,
    record: UploadSinkRecord,
) -> Result<Value, String> {
    let mgr = state.upload_sinks.clone();
    let vault = state.vault.clone();
    let created = mgr.add_sink(record).await?;
    persist_sink_secrets(&vault, &mgr, &created.id, &created.config).await;
    serde_json::to_value(created).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn update_upload_sink(
    state: State<'_, AppState>,
    mut record: UploadSinkRecord,
) -> Result<(), String> {
    let mgr = state.upload_sinks.clone();
    let vault = state.vault.clone();
    fill_from_vault(&vault, &mgr, &record.id, &mut record.config).await;
    let id = record.id.clone();
    let config_for_vault = record.config.clone();
    mgr.update_sink(record).await?;
    persist_sink_secrets(&vault, &mgr, &id, &config_for_vault).await;
    Ok(())
}

#[tauri::command]
pub async fn remove_upload_sink(state: State<'_, AppState>, id: String) -> Result<(), String> {
    let mgr = state.upload_sinks.clone();
    let vault = state.vault.clone();
    mgr.remove_sink(&id).await?;
    if let Err(e) = vault.remove_sink(&id) {
        tracing::warn!("Failed to remove vault entry for sink {id}: {e}");
    }
    Ok(())
}

#[tauri::command]
pub async fn test_upload_sink(state: State<'_, AppState>, id: String) -> Result<(), String> {
    let mgr = state.upload_sinks.clone();
    mgr.test_sink(&id).await
}

#[tauri::command]
pub async fn get_default_upload_sink(state: State<'_, AppState>) -> Result<Option<String>, String> {
    let mgr = state.upload_sinks.clone();
    Ok(mgr.default_sink_id().await)
}

#[tauri::command]
pub async fn set_default_upload_sink(
    state: State<'_, AppState>,
    id: Option<String>,
) -> Result<(), String> {
    let mgr = state.upload_sinks.clone();
    mgr.set_default_sink(id).await
}

#[tauri::command]
pub async fn list_upload_rules(state: State<'_, AppState>) -> Result<Value, String> {
    let mgr = state.upload_sinks.clone();
    let rules = mgr.list_rules().await;
    serde_json::to_value(rules).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn add_upload_rule(
    state: State<'_, AppState>,
    rule: UploadRule,
) -> Result<Value, String> {
    let mgr = state.upload_sinks.clone();
    let created = mgr.add_rule(rule).await?;
    serde_json::to_value(created).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn update_upload_rule(
    state: State<'_, AppState>,
    rule: UploadRule,
) -> Result<(), String> {
    let mgr = state.upload_sinks.clone();
    mgr.update_rule(rule).await
}

#[tauri::command]
pub async fn remove_upload_rule(state: State<'_, AppState>, id: String) -> Result<(), String> {
    let mgr = state.upload_sinks.clone();
    mgr.remove_rule(&id).await
}

#[tauri::command]
pub async fn list_upload_jobs(state: State<'_, AppState>) -> Result<Value, String> {
    let mgr = state.upload_sinks.clone();
    let jobs = mgr.list_jobs().await;
    serde_json::to_value(jobs).map_err(|e| e.to_string())
}

#[tauri::command]
pub async fn cancel_upload_job(state: State<'_, AppState>, id: String) -> Result<(), String> {
    let mgr = state.upload_sinks.clone();
    mgr.cancel_job(&id).await
}

#[tauri::command]
pub async fn clear_upload_history(state: State<'_, AppState>) -> Result<(), String> {
    let mgr = state.upload_sinks.clone();
    mgr.clear_history().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use risuko_engine::engine::upload::{FtpConfig, S3Config, SftpConfig, WebdavConfig};

    fn sftp(password: &str, private_key: &str) -> SinkConfig {
        SinkConfig::Sftp(SftpConfig {
            host: "h".into(),
            port: 22,
            username: "u".into(),
            password: password.into(),
            private_key: private_key.into(),
            base_path: String::new(),
        })
    }

    #[test]
    fn extract_returns_none_when_empty() {
        assert!(extract_sink_secrets(&sftp("", "")).is_none());
        assert!(extract_sink_secrets(&SinkConfig::Ftp(FtpConfig {
            host: "h".into(),
            port: 21,
            username: String::new(),
            password: String::new(),
            base_path: String::new(),
            secure: false,
            insecure: false,
        }))
        .is_none());
    }

    #[test]
    fn extract_picks_up_sftp_secrets() {
        let v = extract_sink_secrets(&sftp("p", "k")).unwrap();
        assert_eq!(v["password"], "p");
        assert_eq!(v["privateKey"], "k");
    }

    #[test]
    fn extract_picks_up_s3_secret() {
        let cfg = SinkConfig::S3(S3Config {
            endpoint: "e".into(),
            region: String::new(),
            bucket: "b".into(),
            access_key_id: "a".into(),
            secret_access_key: "shh".into(),
            prefix: String::new(),
            force_path_style: false,
        });
        let v = extract_sink_secrets(&cfg).unwrap();
        assert_eq!(v["secretAccessKey"], "shh");
    }

    #[test]
    fn apply_round_trips_sftp() {
        let mut cfg = sftp("", "");
        let payload = serde_json::json!({"password": "p", "privateKey": "k"});
        apply_sink_secrets(&mut cfg, &payload);
        match cfg {
            SinkConfig::Sftp(c) => {
                assert_eq!(c.password, "p");
                assert_eq!(c.private_key, "k");
            }
            _ => panic!("expected sftp"),
        }
    }

    #[test]
    fn apply_does_not_clobber_with_empty() {
        let mut cfg = sftp("existing", "");
        let payload = serde_json::json!({"password": "", "privateKey": ""});
        apply_sink_secrets(&mut cfg, &payload);
        match cfg {
            SinkConfig::Sftp(c) => assert_eq!(c.password, "existing"),
            _ => panic!("expected sftp"),
        }
    }

    #[test]
    fn apply_ignores_unrelated_fields() {
        let mut cfg = SinkConfig::Webdav(WebdavConfig {
            endpoint: "e".into(),
            base_path: String::new(),
            username: String::new(),
            password: String::new(),
            insecure: false,
        });
        let payload = serde_json::json!({"privateKey": "x"});
        apply_sink_secrets(&mut cfg, &payload);
        match cfg {
            SinkConfig::Webdav(c) => assert_eq!(c.password, ""),
            _ => panic!("expected webdav"),
        }
    }

    #[tokio::test]
    async fn fill_from_vault_falls_back_when_vault_misses() {
        use risuko_engine::traits::{FileStorage, NoopEventSink};
        use std::sync::Arc;

        let dir = tempfile::TempDir::new().unwrap();
        let storage: Arc<dyn risuko_engine::traits::StorageBackend> =
            Arc::new(FileStorage::new(dir.path().to_path_buf()));
        let event_sink: Arc<dyn risuko_engine::traits::EventSink> = Arc::new(NoopEventSink);
        let mgr = UploadSinkManager::new(storage, event_sink);

        let id = format!("test-sink-{}-{}", std::process::id(), uuid_like());
        let stored = json!({"password": "from-fallback", "privateKey": "pk"});
        mgr.put_sink_secret_fallback(&id, &stored).await.unwrap();

        let vault = crate::managers::vault::VaultManager::for_test(true);
        assert!(vault.enabled());

        let mut cfg = sftp("", "");
        fill_from_vault(&vault, &mgr, &id, &mut cfg).await;
        match cfg {
            SinkConfig::Sftp(c) => {
                assert_eq!(c.password, "from-fallback", "fallback password not applied");
                assert_eq!(c.private_key, "pk", "fallback private key not applied");
            }
            _ => panic!("expected sftp"),
        }
    }

    fn uuid_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        format!(
            "{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        )
    }
}
