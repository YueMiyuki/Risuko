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
}

impl KnownHosts {
    fn load() -> Self {
        let path = dirs::config_dir()
            .unwrap_or_else(std::env::temp_dir)
            .join("risuko")
            .join("sftp_known_hosts.json");
        let map = std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| serde_json::from_str::<HashMap<String, String>>(&s).ok())
            .unwrap_or_default();
        Self { path, map }
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
        key: &russh::keys::PublicKey,
    ) -> Result<bool, Self::Error> {
        let fp = match fingerprint(key) {
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
        match store.map.get(&self.host_key) {
            Some(existing) if existing == &fp => Ok(true),
            Some(existing) => {
                tracing::warn!(
                    "SFTP host key mismatch for {}: stored {} got {}",
                    self.host_key,
                    existing,
                    fp
                );
                Ok(false)
            }
            None => {
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
