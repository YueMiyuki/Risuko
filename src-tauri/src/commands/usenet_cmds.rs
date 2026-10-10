use fs4::FileExt;
use risuko_engine::engine::options::EngineOptions;
use risuko_engine::engine::usenet::{UsenetCredentialResolver, UsenetProviderProfile};
use risuko_engine::engine::usenet_transport::NntpConnection;
use serde_json::{json, Map, Value};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tauri::State;

use crate::state::AppState;

fn credential_key(profile_id: &str) -> String {
    format!("usenet:{profile_id}")
}

pub struct VaultCredentialResolver {
    vault: Arc<crate::managers::vault::VaultManager>,
    fallback: risuko_engine::engine::FileUsenetCredentialResolver,
}

impl VaultCredentialResolver {
    pub fn with_config_dir(
        vault: Arc<crate::managers::vault::VaultManager>,
        config_dir: PathBuf,
    ) -> Self {
        Self {
            vault,
            fallback: risuko_engine::engine::FileUsenetCredentialResolver::new(config_dir),
        }
    }
}

#[async_trait::async_trait]
impl risuko_engine::engine::usenet::UsenetCredentialResolver for VaultCredentialResolver {
    async fn resolve(
        &self,
        profile_id: &str,
    ) -> Result<Option<risuko_engine::engine::usenet::UsenetCredentials>, String> {
        let lookup = {
            let vault = self.vault.clone();
            let key = credential_key(profile_id);
            tokio::task::spawn_blocking(move || vault.enabled().then(|| vault.get(&key))).await
        };
        match lookup {
            Ok(Some(Ok(Some(value)))) => {
                let username = value
                    .get("username")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                let password = value
                    .get("password")
                    .and_then(|value| value.as_str())
                    .map(str::to_string);
                return Ok(Some(risuko_engine::engine::usenet::UsenetCredentials {
                    username,
                    password,
                }));
            }
            Ok(Some(Ok(None))) | Ok(None) => {}
            Ok(Some(Err(error))) => {
                tracing::warn!(
                    "Failed to load Usenet credentials for {profile_id}: {error}; trying fallback"
                );
            }
            Err(error) => {
                tracing::warn!(
                    "Usenet credential lookup for {profile_id} did not complete: {error}; trying fallback"
                );
            }
        }
        self.fallback.resolve(profile_id).await
    }
}

fn load_fallback(path: &Path) -> Result<Map<String, Value>, String> {
    match std::fs::read_to_string(path) {
        Ok(data) => serde_json::from_str(&data).map_err(|e| e.to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Map::new()),
        Err(error) => Err(error.to_string()),
    }
}

static FALLBACK_MUTEX: std::sync::LazyLock<std::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

fn with_fallback_lock<T>(
    path: &Path,
    operation: impl FnOnce() -> Result<T, String>,
) -> Result<T, String> {
    let _process_guard = FALLBACK_MUTEX
        .lock()
        .map_err(|_| "Credential fallback lock poisoned".to_string())?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let lock_path = path.with_file_name(format!(
        ".{}.lock",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("credentials")
    ));
    let lock_file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|e| e.to_string())?;
    FileExt::lock(&lock_file).map_err(|e| e.to_string())?;
    let result = operation();
    let _ = FileExt::unlock(&lock_file);
    result
}

fn write_fallback_atomic(path: &Path, data: &str) -> Result<(), String> {
    risuko_engine::traits::write_file_atomically_with(
        path,
        data.as_bytes(),
        set_owner_only_permissions,
    )
}

#[cfg(unix)]
fn set_owner_only_permissions(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, PermissionsExt::from_mode(0o600)).map_err(|e| e.to_string())
}

#[cfg(windows)]
fn set_owner_only_permissions(path: &Path) -> Result<(), String> {
    let username = std::env::var("USERNAME")
        .map_err(|_| "Windows user name is unavailable for credential ACL".to_string())?;
    let status = std::process::Command::new("icacls")
        .arg(path)
        .args(["/inheritance:r", "/grant:r"])
        .arg(format!("{username}:F"))
        .status()
        .map_err(|e| format!("apply credential ACL: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("icacls exited with status {status}"))
    }
}

#[cfg(not(any(unix, windows)))]
fn set_owner_only_permissions(_path: &Path) -> Result<(), String> {
    Ok(())
}

fn save_fallback(path: &Path, profile_id: &str, credentials: &Value) -> Result<(), String> {
    with_fallback_lock(path, || {
        let mut entries = load_fallback(path)?;
        entries.insert(profile_id.to_string(), credentials.clone());
        let data = serde_json::to_string_pretty(&entries).map_err(|e| e.to_string())?;
        write_fallback_atomic(path, &data)
    })
}

fn remove_fallback(path: &Path, profile_id: &str) -> Result<(), String> {
    with_fallback_lock(path, || {
        let mut entries = load_fallback(path)?;
        entries.remove(profile_id);
        if entries.is_empty() {
            match std::fs::remove_file(path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.to_string()),
            }
        } else {
            let data = serde_json::to_string_pretty(&entries).map_err(|e| e.to_string())?;
            write_fallback_atomic(path, &data)
        }
    })
}

fn resolver_for_state(state: &AppState) -> Result<VaultCredentialResolver, String> {
    let config_dir = state
        .config
        .lock()
        .map_err(|_| "Configuration lock poisoned".to_string())?
        .config_dir()
        .to_path_buf();
    Ok(VaultCredentialResolver::with_config_dir(
        state.vault.clone(),
        config_dir,
    ))
}

#[tauri::command(async)]
pub fn usenet_save_credentials(
    state: State<'_, AppState>,
    profile_id: String,
    username: Option<String>,
    password: Option<String>,
) -> Result<(), String> {
    if profile_id.trim().is_empty() {
        return Err("Usenet profile id is required".into());
    }
    let credentials = json!({
        "username": username.filter(|value| !value.is_empty()),
        "password": password.filter(|value| !value.is_empty()),
    });
    let config_dir = state
        .config
        .lock()
        .map_err(|_| "Configuration lock poisoned".to_string())?
        .config_dir()
        .to_path_buf();
    let fallback_path = risuko_engine::engine::usenet_credential_fallback_path(&config_dir);
    if state.vault.enabled() {
        match state.vault.put(&credential_key(&profile_id), &credentials) {
            Ok(()) => {
                if let Err(error) = remove_fallback(&fallback_path, &profile_id) {
                    tracing::warn!(
                        "Failed to clear stale Usenet credential fallback for {profile_id}: {error}"
                    );
                }
                return Ok(());
            }
            Err(error) => tracing::warn!(
                "Failed to store Usenet credentials in vault for {profile_id}: {error}; using fallback"
            ),
        }
    }
    save_fallback(&fallback_path, &profile_id, &credentials)
}

#[tauri::command(async)]
pub fn usenet_remove_credentials(
    state: State<'_, AppState>,
    profile_id: String,
) -> Result<(), String> {
    let config_dir = state
        .config
        .lock()
        .map_err(|_| "Configuration lock poisoned".to_string())?
        .config_dir()
        .to_path_buf();
    let fallback_path = risuko_engine::engine::usenet_credential_fallback_path(&config_dir);
    let vault_error = if state.vault.enabled() {
        state.vault.remove(&credential_key(&profile_id)).err()
    } else {
        None
    };
    let fallback_result = remove_fallback(&fallback_path, &profile_id);
    if let Some(error) = vault_error {
        return Err(error);
    }
    fallback_result
}

#[tauri::command(async)]
pub fn usenet_has_credentials(
    state: State<'_, AppState>,
    profile_id: String,
) -> Result<bool, String> {
    let config_dir = state
        .config
        .lock()
        .map_err(|_| "Configuration lock poisoned".to_string())?
        .config_dir()
        .to_path_buf();
    if state.vault.enabled() {
        match state.vault.get(&credential_key(&profile_id)) {
            Ok(Some(_)) => return Ok(true),
            Ok(None) => {}
            Err(error) => tracing::warn!(
                "Failed to inspect Usenet credentials for {profile_id}: {error}; trying fallback"
            ),
        }
    }
    let fallback_path = risuko_engine::engine::usenet_credential_fallback_path(&config_dir);
    Ok(load_fallback(&fallback_path)?.contains_key(&profile_id))
}

#[tauri::command]
pub async fn usenet_test_profile(
    state: State<'_, AppState>,
    profile: UsenetProviderProfile,
) -> Result<String, String> {
    let credentials = resolver_for_state(&state)?.resolve(&profile.id).await?;
    let _capacity_lease = match risuko_engine::engine::get_manager().await {
        Some(manager) => Some(
            manager
                .try_acquire_usenet_profile_connection(&profile)?
                .ok_or_else(|| {
                    "The provider's configured connection capacity is currently in use".to_string()
                })?,
        ),
        None => None,
    };
    let http_proxy = {
        let config = state
            .config
            .lock()
            .map_err(|_| "Configuration lock poisoned".to_string())?;
        let options =
            EngineOptions::from_config(config.get_system_config(), config.get_user_config());
        let server = options.get_str("all-proxy").unwrap_or("").trim();
        if server.is_empty() {
            None
        } else {
            let proxy = risuko_http::Proxy::all(server)
                .map_err(|error| format!("Invalid HTTP profile proxy: {error}"))?;
            let bypass = options.get_str("no-proxy").unwrap_or("");
            Some(
                risuko_http::ProxyConnector::from_proxy(proxy)
                    .with_no_proxy(risuko_http::NoProxy::parse(bypass)),
            )
        }
    };
    let mut connection =
        NntpConnection::connect_with_proxy(&profile, credentials, http_proxy.as_ref())
            .await
            .map_err(|error| error.to_string())?;
    let _ = connection
        .capabilities()
        .await
        .map_err(|error| error.to_string())?;
    Ok("ok".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn fallback_credentials_round_trip_and_remove() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("usenet-credentials.json");
        let credentials = json!({"username": "alice", "password": "secret"});

        save_fallback(&path, "primary", &credentials).unwrap();
        let entries = load_fallback(&path).unwrap();
        assert_eq!(entries.get("primary"), Some(&credentials));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }

        remove_fallback(&path, "primary").unwrap();
        assert!(!path.exists());
    }
}
