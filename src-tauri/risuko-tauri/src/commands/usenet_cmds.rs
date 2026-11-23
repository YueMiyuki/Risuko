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
        let fallback = self.fallback.resolve(profile_id).await;
        if let Ok(Some(credentials)) = fallback {
            return Ok(Some(credentials));
        }
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
                tracing::warn!("Failed to load Usenet credentials for {profile_id}: {error}");
            }
            Err(error) => {
                tracing::warn!(
                    "Usenet credential lookup for {profile_id} did not complete: {error}"
                );
            }
        }
        fallback
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
        if entries.remove(profile_id).is_none() && !entries.is_empty() {
            return Ok(());
        }
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

fn clear_stale_fallback(path: &Path, profile_id: &str) -> Result<(), String> {
    remove_fallback(path, profile_id).map_err(|error| {
        tracing::warn!("Failed to clear stale Usenet credential fallback for {profile_id}: {error}");
        format!(
            "The older fallback copy of these credentials could not be cleared, so nothing was changed: {error}"
        )
    })
}

fn save_in_order(
    vault_enabled: bool,
    clear_fallback: impl FnOnce() -> Result<(), String>,
    put_vault: impl FnOnce() -> Result<(), String>,
    save_fallback: impl FnOnce() -> Result<(), String>,
) -> Result<(), String> {
    if vault_enabled {
        clear_fallback()?;
        match put_vault() {
            Ok(()) => return Ok(()),
            Err(error) => tracing::warn!(
                "Failed to store Usenet credentials in vault: {error}; using fallback"
            ),
        }
    }
    save_fallback()
}

fn saved_server_matches(
    saved: &[UsenetProviderProfile],
    requested: &UsenetProviderProfile,
) -> bool {
    saved.iter().any(|profile| {
        profile.id == requested.id
            && profile.deleted_at.is_none()
            && profile
                .host
                .trim()
                .eq_ignore_ascii_case(requested.host.trim())
            && profile.port == requested.port
            && profile.security_mode == requested.security_mode
    })
}

fn saved_profiles(state: &AppState) -> Result<Vec<UsenetProviderProfile>, String> {
    let config = state
        .config
        .lock()
        .map_err(|_| "Configuration lock poisoned".to_string())?;
    Ok(
        risuko_engine::engine::usenet_worker::profiles_from_options(config.get_user_config())
            .unwrap_or_default(),
    )
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
    save_in_order(
        state.vault.enabled(),
        || clear_stale_fallback(&fallback_path, &profile_id),
        || {
            state
                .vault
                .put(&credential_key(&profile_id), &credentials)
                .map_err(|error| format!("{profile_id}: {error}"))
        },
        || save_fallback(&fallback_path, &profile_id, &credentials),
    )
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
    let credentials = if saved_server_matches(&saved_profiles(&state)?, &profile) {
        resolver_for_state(&state)?.resolve(&profile.id).await?
    } else {
        None
    };
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
    use std::cell::RefCell;
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

    #[test]
    fn a_failed_cleanup_is_reported_and_never_rewrites_the_fallback() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("usenet-credentials.json");
        save_fallback(&path, "primary", &json!({"password": "old"})).unwrap();
        save_fallback(&path, "backup", &json!({"password": "other"})).unwrap();
        let before = std::fs::read(&path).unwrap();

        let lock = dir.path().join(".usenet-credentials.json.lock");
        std::fs::remove_file(&lock).unwrap();
        std::fs::create_dir(&lock).unwrap();
        let error = clear_stale_fallback(&path, "primary").unwrap_err();

        assert!(error.contains("could not be cleared"), "{error}");
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[cfg(unix)]
    #[test]
    fn clearing_a_profile_without_an_entry_does_not_touch_the_file() {
        use std::os::unix::fs::PermissionsExt;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("usenet-credentials.json");
        save_fallback(&path, "backup", &json!({"password": "other"})).unwrap();
        let before = std::fs::read(&path).unwrap();

        std::fs::set_permissions(dir.path(), PermissionsExt::from_mode(0o500)).unwrap();
        let result = clear_stale_fallback(&path, "primary");
        std::fs::set_permissions(dir.path(), PermissionsExt::from_mode(0o700)).unwrap();

        result.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn a_failed_cleanup_stops_before_the_vault_is_written() {
        let calls = RefCell::new(Vec::new());
        let result = save_in_order(
            true,
            || {
                calls.borrow_mut().push("clear");
                Err("clear failed".to_string())
            },
            || {
                calls.borrow_mut().push("vault");
                Ok(())
            },
            || {
                calls.borrow_mut().push("fallback");
                Ok(())
            },
        );

        assert_eq!(result, Err("clear failed".to_string()));
        assert_eq!(*calls.borrow(), ["clear"]);
    }

    #[test]
    fn a_vault_failure_falls_back_to_the_file() {
        let calls = RefCell::new(Vec::new());
        let result = save_in_order(
            true,
            || {
                calls.borrow_mut().push("clear");
                Ok(())
            },
            || {
                calls.borrow_mut().push("vault");
                Err("vault failed".to_string())
            },
            || {
                calls.borrow_mut().push("fallback");
                Err("fallback failed".to_string())
            },
        );

        assert_eq!(result, Err("fallback failed".to_string()));
        assert_eq!(*calls.borrow(), ["clear", "vault", "fallback"]);
    }

    #[test]
    fn a_working_vault_leaves_no_fallback() {
        let calls = RefCell::new(Vec::new());
        let result = save_in_order(
            true,
            || {
                calls.borrow_mut().push("clear");
                Ok(())
            },
            || {
                calls.borrow_mut().push("vault");
                Ok(())
            },
            || {
                calls.borrow_mut().push("fallback");
                Ok(())
            },
        );

        assert_eq!(result, Ok(()));
        assert_eq!(*calls.borrow(), ["clear", "vault"]);
    }

    #[test]
    fn without_a_vault_only_the_file_is_written() {
        let calls = RefCell::new(Vec::new());
        let result = save_in_order(
            false,
            || {
                calls.borrow_mut().push("clear");
                Ok(())
            },
            || {
                calls.borrow_mut().push("vault");
                Ok(())
            },
            || {
                calls.borrow_mut().push("fallback");
                Ok(())
            },
        );

        assert_eq!(result, Ok(()));
        assert_eq!(*calls.borrow(), ["fallback"]);
    }

    fn saved_profile(
        id: &str,
        host: &str,
        port: u16,
        security_mode: &str,
    ) -> UsenetProviderProfile {
        UsenetProviderProfile {
            id: id.into(),
            name: id.into(),
            host: host.into(),
            port,
            security_mode: security_mode.into(),
            enabled: true,
            priority: 0,
            max_connections: 4,
            allow_plain: security_mode == "plain",
            deleted_at: None,
            updated_at: None,
        }
    }

    fn saved() -> [UsenetProviderProfile; 1] {
        [saved_profile(
            "primary",
            "news.example",
            563,
            "implicit-tls",
        )]
    }

    #[test]
    fn the_saved_server_may_use_the_stored_login() {
        let requested = saved_profile("primary", "news.example", 563, "implicit-tls");

        assert!(saved_server_matches(&saved(), &requested));
    }

    #[test]
    fn the_host_is_compared_without_letter_case() {
        let saved = [saved_profile(
            "primary",
            "News.Example",
            563,
            "implicit-tls",
        )];
        let requested = saved_profile("primary", "news.EXAMPLE", 563, "implicit-tls");

        assert!(saved_server_matches(&saved, &requested));
    }

    #[test]
    fn another_host_does_not_get_the_stored_login() {
        let requested = saved_profile("primary", "evil.example", 563, "implicit-tls");

        assert!(!saved_server_matches(&saved(), &requested));
    }

    #[test]
    fn another_port_does_not_get_the_stored_login() {
        let requested = saved_profile("primary", "news.example", 443, "implicit-tls");

        assert!(!saved_server_matches(&saved(), &requested));
    }

    #[test]
    fn another_tls_setting_does_not_get_the_stored_login() {
        let requested = saved_profile("primary", "news.example", 563, "plain");

        assert!(!saved_server_matches(&saved(), &requested));
    }

    #[test]
    fn an_id_that_is_not_saved_does_not_get_a_stored_login() {
        let requested = saved_profile("backup", "news.example", 563, "implicit-tls");

        assert!(!saved_server_matches(&saved(), &requested));
        assert!(!saved_server_matches(&[], &requested));
    }

    #[test]
    fn a_removed_profile_does_not_get_a_stored_login() {
        let mut removed = saved_profile("primary", "news.example", 563, "implicit-tls");
        removed.deleted_at = Some(1);
        let requested = saved_profile("primary", "news.example", 563, "implicit-tls");

        assert!(!saved_server_matches(&[removed], &requested));
    }

    #[test]
    fn a_cleanup_that_works_drops_only_that_profile() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("usenet-credentials.json");
        save_fallback(&path, "primary", &json!({"password": "old"})).unwrap();
        save_fallback(&path, "backup", &json!({"password": "other"})).unwrap();

        clear_stale_fallback(&path, "primary").unwrap();
        let entries = load_fallback(&path).unwrap();
        assert!(!entries.contains_key("primary"));
        assert!(entries.contains_key("backup"));
    }
}
