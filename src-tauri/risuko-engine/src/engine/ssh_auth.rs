use std::sync::Arc;
use std::time::Duration;

use russh::client;
use russh::keys::{PrivateKey, PrivateKeyWithHashAlg};

use super::ssh_known_hosts::TofuHandler;

const RSA_HASH_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) async fn authenticate(
    session: &mut client::Handle<TofuHandler>,
    user: &str,
    key: Option<PrivateKey>,
    password: Option<&str>,
) -> Result<bool, String> {
    if let Some(key) = key {
        // RSA keys need rsa-sha2-* since OpenSSH 8.8 rejects ssh-rsa (SHA-1)
        let hash_alg = if key.algorithm().is_rsa() {
            tokio::time::timeout(RSA_HASH_TIMEOUT, session.best_supported_rsa_hash())
                .await
                .ok()
                .and_then(|r| r.ok())
                .flatten()
                .flatten()
        } else {
            None
        };
        let key_with_alg = PrivateKeyWithHashAlg::new(Arc::new(key), hash_alg);
        match session.authenticate_publickey(user, key_with_alg).await {
            Ok(auth) if auth.success() => {
                tracing::info!("SSH key authentication successful");
                return Ok(true);
            }
            Ok(_) => tracing::warn!("SSH key authentication rejected by server"),
            Err(e) => tracing::warn!("SSH key authentication error: {e}"),
        }
    }

    if let Some(pass) = password {
        match session.authenticate_password(user, pass).await {
            Ok(auth) if auth.success() => {
                tracing::info!("SSH password authentication successful");
                return Ok(true);
            }
            Ok(_) => tracing::warn!("SSH password authentication rejected"),
            Err(e) => tracing::warn!("SSH password authentication error: {e}"),
        }
    }
    Ok(false)
}
