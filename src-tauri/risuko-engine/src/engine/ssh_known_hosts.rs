use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::LazyLock;

use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
use parking_lot::Mutex;
use russh::client;
use sha2::{Digest, Sha256};

struct KnownHosts {
    path: PathBuf,
    map: HashMap<String, String>,
    readable: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum KeyDecision {
    Accept,
    Mismatch,
    Pin,
    StoreUnreadable,
}

fn decide(store: &KnownHosts, host_key: &str, fingerprint: &str) -> KeyDecision {
    if !store.readable {
        return KeyDecision::StoreUnreadable;
    }
    match store.map.get(host_key) {
        Some(existing) if existing == fingerprint => KeyDecision::Accept,
        Some(_) => KeyDecision::Mismatch,
        None => KeyDecision::Pin,
    }
}

impl KnownHosts {
    fn load() -> Self {
        let path = dirs::config_dir()
            .unwrap_or_else(std::env::temp_dir)
            .join("risuko")
            .join("sftp_known_hosts.json");
        Self::load_from(path)
    }

    fn load_from(path: PathBuf) -> Self {
        let result = match std::fs::read_to_string(&path) {
            Ok(s) => serde_json::from_str::<HashMap<String, String>>(&s).map_err(|e| e.to_string()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HashMap::new()),
            Err(e) => Err(e.to_string()),
        };
        match result {
            Ok(map) => Self {
                path,
                map,
                readable: true,
            },
            Err(e) => {
                tracing::error!(
                    "SFTP known_hosts at {} is unreadable: {}",
                    path.display(),
                    e
                );
                Self {
                    path,
                    map: HashMap::new(),
                    readable: false,
                }
            }
        }
    }

    fn write_to_disk(path: &std::path::Path, map: &HashMap<String, String>) -> Result<(), String> {
        let s =
            serde_json::to_string_pretty(map).map_err(|e| format!("serialize known_hosts: {e}"))?;
        // A partial write must never truncate existing pinned hosts
        crate::traits::write_file_atomically(path, s.as_bytes())
    }
}

static KNOWN_HOSTS: LazyLock<Mutex<KnownHosts>> = LazyLock::new(|| Mutex::new(KnownHosts::load()));

pub fn fingerprint(key: &russh::keys::PublicKey) -> Result<String, russh::Error> {
    let mut h = Sha256::new();
    h.update(key.to_bytes()?);
    Ok(format!("SHA256:{}", STANDARD_NO_PAD.encode(h.finalize())))
}

pub struct TofuHandler {
    pub host_key: String,
}

impl TofuHandler {
    pub fn new(host_key: String) -> Self {
        Self { host_key }
    }
}

impl client::Handler for TofuHandler {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        let fp = match fingerprint(&key.public_key()) {
            Ok(fp) => fp,
            Err(e) => {
                tracing::warn!(
                    "SFTP host key fingerprint failed for {}: {} — refusing connection",
                    self.host_key,
                    e
                );
                return Ok(false);
            }
        };
        let mut store = KNOWN_HOSTS.lock();
        match decide(&store, &self.host_key, &fp) {
            KeyDecision::Accept => Ok(true),
            KeyDecision::Mismatch => {
                tracing::warn!(
                    "SFTP host key mismatch for {}: stored {} got {}",
                    self.host_key,
                    store
                        .map
                        .get(&self.host_key)
                        .map(String::as_str)
                        .unwrap_or_default(),
                    fp
                );
                Ok(false)
            }
            KeyDecision::StoreUnreadable => {
                tracing::error!(
                    "SFTP host-key store at {} could not be read, refusing connection to {}; repair or remove the file",
                    store.path.display(),
                    self.host_key
                );
                Ok(false)
            }
            KeyDecision::Pin => {
                let path = store.path.clone();
                let mut next = store.map.clone();
                next.insert(self.host_key.clone(), fp.clone());
                if let Err(e) = KnownHosts::write_to_disk(&path, &next) {
                    tracing::error!(
                        "SFTP TOFU: refusing to trust {} because known_hosts persist failed: {}",
                        self.host_key,
                        e
                    );
                    return Ok(false);
                }
                tracing::info!("SFTP TOFU: pinning {} -> {}", self.host_key, fp);
                store.map.insert(self.host_key.clone(), fp);
                Ok(true)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(readable: bool, entries: &[(&str, &str)]) -> KnownHosts {
        KnownHosts {
            path: PathBuf::from("sftp_known_hosts.json"),
            map: entries
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            readable,
        }
    }

    #[test]
    fn missing_file_means_no_pins_yet() {
        let dir = tempfile::tempdir().unwrap();
        let loaded = KnownHosts::load_from(dir.path().join("sftp_known_hosts.json"));
        assert!(loaded.map.is_empty());
        assert!(loaded.readable);
    }

    #[test]
    fn valid_file_is_loaded() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sftp_known_hosts.json");
        std::fs::write(&path, r#"{"host:22":"SHA256:abc"}"#).unwrap();
        let loaded = KnownHosts::load_from(path);
        assert_eq!(loaded.map.len(), 1);
        assert_eq!(
            loaded.map.get("host:22").map(String::as_str),
            Some("SHA256:abc")
        );
        assert!(loaded.readable);
    }

    #[test]
    fn damaged_file_is_not_treated_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sftp_known_hosts.json");
        std::fs::write(&path, r#"{"host:22":"#).unwrap();
        let loaded = KnownHosts::load_from(path);
        assert!(!loaded.readable);
    }

    #[test]
    fn unreadable_store_refuses_every_host() {
        let unreadable = store(false, &[]);
        assert_eq!(
            decide(&unreadable, "new:22", "SHA256:abc"),
            KeyDecision::StoreUnreadable
        );
        let unreadable = store(false, &[("known:22", "SHA256:abc")]);
        assert_eq!(
            decide(&unreadable, "known:22", "SHA256:abc"),
            KeyDecision::StoreUnreadable
        );
        assert_eq!(
            decide(&unreadable, "other:22", "SHA256:abc"),
            KeyDecision::StoreUnreadable
        );
    }

    #[test]
    fn unknown_host_in_readable_store_is_pinned() {
        let readable = store(true, &[("known:22", "SHA256:abc")]);
        assert_eq!(decide(&readable, "new:22", "SHA256:def"), KeyDecision::Pin);
    }

    #[test]
    fn matching_pin_is_accepted() {
        let readable = store(true, &[("known:22", "SHA256:abc")]);
        assert_eq!(
            decide(&readable, "known:22", "SHA256:abc"),
            KeyDecision::Accept
        );
    }

    #[test]
    fn different_pin_is_a_mismatch() {
        let readable = store(true, &[("known:22", "SHA256:abc")]);
        assert_eq!(
            decide(&readable, "known:22", "SHA256:def"),
            KeyDecision::Mismatch
        );
    }
}
