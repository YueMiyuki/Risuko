//! OS-keychain-backed credential vault

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Once;

use keyring_core::{Entry, Error};
use serde_json::Value;

const SERVICE: &str = "risuko-credentials";
const SINK_SERVICE: &str = "risuko-sinks";
const PROBE_ACCOUNT: &str = "__probe__";

fn ensure_default_store() {
    static INIT: Once = Once::new();
    INIT.call_once(|| match build_default_store() {
        Ok(store) => keyring_core::set_default_store(store),
        Err(e) => tracing::warn!("Credential vault: failed to init keystore: {e}"),
    });
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
fn build_default_store() -> Result<std::sync::Arc<keyring_core::CredentialStore>, Error> {
    apple_native_keyring_store::keychain::Store::new()
        .map(|s| s as std::sync::Arc<keyring_core::CredentialStore>)
}

#[cfg(target_os = "windows")]
fn build_default_store() -> Result<std::sync::Arc<keyring_core::CredentialStore>, Error> {
    windows_native_keyring_store::Store::new()
        .map(|s| s as std::sync::Arc<keyring_core::CredentialStore>)
}

#[cfg(all(
    unix,
    not(any(target_os = "macos", target_os = "ios", target_os = "android"))
))]
fn build_default_store() -> Result<std::sync::Arc<keyring_core::CredentialStore>, Error> {
    dbus_secret_service_keyring_store::Store::new()
        .map(|s| s as std::sync::Arc<keyring_core::CredentialStore>)
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "windows",
    all(
        unix,
        not(any(target_os = "macos", target_os = "ios", target_os = "android"))
    ),
)))]
fn build_default_store() -> Result<std::sync::Arc<keyring_core::CredentialStore>, Error> {
    Err(Error::NoDefaultStore)
}

pub struct VaultManager {
    enabled: AtomicBool,
}

impl VaultManager {
    pub fn new() -> Self {
        ensure_default_store();
        let mgr = Self {
            enabled: AtomicBool::new(false),
        };
        mgr.probe();
        mgr
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Build a manager with a forced `enabled` flag, skipping the OS probe
    #[cfg(test)]
    pub(crate) fn for_test(enabled: bool) -> Self {
        Self {
            enabled: AtomicBool::new(enabled),
        }
    }

    /// Non-destructive reachability check: confirm the keyring backend opens
    /// and lookups succeed
    fn probe(&self) {
        let ok = match Entry::new(SERVICE, PROBE_ACCOUNT) {
            Ok(entry) => match entry.get_password() {
                Ok(_) | Err(Error::NoEntry) => true,
                Err(_) => false,
            },
            Err(_) => false,
        };
        self.enabled.store(ok, Ordering::Relaxed);
        if ok {
            tracing::info!("Credential vault: OS keychain available");
        } else {
            tracing::warn!("Credential vault: OS keychain unavailable, falling back to plaintext");
        }
    }

    /// Store the secret JSON object for the given credential id
    pub fn put(&self, id: &str, secrets: &Value) -> Result<(), String> {
        self.put_at(SERVICE, id, secrets)
    }

    /// Retrieve the secret JSON object for the given credential id
    pub fn get(&self, id: &str) -> Result<Option<Value>, String> {
        self.get_at(SERVICE, id)
    }

    /// Best-effort delete; missing entries are not an error
    pub fn remove(&self, id: &str) -> Result<(), String> {
        self.remove_at(SERVICE, id)
    }

    /// Store secrets for an upload sink (separate keychain namespace)
    pub fn put_sink(&self, id: &str, secrets: &Value) -> Result<(), String> {
        self.put_at(SINK_SERVICE, id, secrets)
    }

    /// Retrieve secrets for an upload sink
    pub fn get_sink(&self, id: &str) -> Result<Option<Value>, String> {
        self.get_at(SINK_SERVICE, id)
    }

    /// Best-effort delete of an upload sink's secrets
    pub fn remove_sink(&self, id: &str) -> Result<(), String> {
        self.remove_at(SINK_SERVICE, id)
    }

    fn put_at(&self, service: &str, account: &str, secrets: &Value) -> Result<(), String> {
        if !self.enabled() {
            return Err("vault not available".to_string());
        }
        let json = serde_json::to_string(secrets).map_err(|e| e.to_string())?;
        let entry = Entry::new(service, account).map_err(|e| e.to_string())?;
        entry.set_password(&json).map_err(|e| e.to_string())
    }

    fn get_at(&self, service: &str, account: &str) -> Result<Option<Value>, String> {
        if !self.enabled() {
            return Ok(None);
        }
        let entry = Entry::new(service, account).map_err(|e| e.to_string())?;
        match entry.get_password() {
            Ok(json) => {
                let v: Value = serde_json::from_str(&json).map_err(|e| e.to_string())?;
                Ok(Some(v))
            }
            Err(Error::NoEntry) => Ok(None),
            Err(e) => Err(e.to_string()),
        }
    }

    fn remove_at(&self, service: &str, account: &str) -> Result<(), String> {
        if !self.enabled() {
            return Ok(());
        }
        let entry = Entry::new(service, account).map_err(|e| e.to_string())?;
        match entry.delete_credential() {
            Ok(()) | Err(Error::NoEntry) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn disabled_put_is_err() {
        let m = VaultManager::for_test(false);
        assert!(!m.enabled());
        let err = m.put("any-id", &json!({"ftpPasswd": "x"})).unwrap_err();
        assert!(err.contains("not available"), "unexpected error: {err}");
    }

    #[test]
    fn disabled_get_returns_none() {
        let m = VaultManager::for_test(false);
        assert_eq!(m.get("any-id").unwrap(), None);
    }

    #[test]
    fn disabled_remove_is_ok() {
        let m = VaultManager::for_test(false);
        // Should silently succeed so callers can blindly remove on cleanup
        m.remove("any-id").unwrap();
    }

    #[test]
    fn enabled_round_trip_real_keychain() {
        if std::env::var("RISUKO_VAULT_INTEGRATION_TEST")
            .ok()
            .as_deref()
            != Some("1")
        {
            return;
        }
        let m = VaultManager::new();
        if !m.enabled() {
            // No usable backend on this host; treat as a skip
            return;
        }

        let id = format!("risuko-test-{}", std::process::id());
        let secrets = json!({
            "ftpUser": "alice",
            "ftpPasswd": "s3cret!",
            "authorization": "Bearer abc",
        });

        m.put(&id, &secrets).expect("put");
        let fetched = m.get(&id).expect("get").expect("entry exists");
        assert_eq!(fetched, secrets);

        m.remove(&id).expect("remove");
        assert_eq!(m.get(&id).expect("get after remove"), None);

        // Removing a missing entry must remain a no-op
        m.remove(&id).expect("idempotent remove");
    }
}
