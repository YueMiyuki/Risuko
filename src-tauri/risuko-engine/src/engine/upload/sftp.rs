use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use russh::client;
use russh_sftp::client::SftpSession;
use russh_sftp::protocol::OpenFlags;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::sink::{SftpConfig, UploadControl, UploadFile, UploadSink};
use crate::engine::ssh_auth::authenticate;
use crate::engine::ssh_known_hosts::TofuHandler;

const COPY_BUF: usize = 256 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

pub struct SftpSink {
    cfg: SftpConfig,
}

impl SftpSink {
    pub fn new(cfg: SftpConfig) -> Result<Self, String> {
        if cfg.host.trim().is_empty() {
            return Err("SFTP host is empty".into());
        }
        if cfg.username.trim().is_empty() {
            return Err("SFTP username is empty".into());
        }
        if cfg.password.is_empty() && cfg.private_key.is_empty() {
            return Err("SFTP requires a password or a private key".into());
        }
        Ok(Self { cfg })
    }

    async fn connect(&self) -> Result<(client::Handle<TofuHandler>, SftpSession), String> {
        let config = Arc::new(client::Config::default());
        let addr = format!("{}:{}", self.cfg.host, self.cfg.port);
        let handler = TofuHandler::new(addr.clone());
        let mut session = client::connect(config, &addr, handler)
            .await
            .map_err(|e| format!("SSH connect failed: {e}"))?;

        let key = if self.cfg.private_key.is_empty() {
            None
        } else {
            match russh::keys::decode_secret_key(&self.cfg.private_key, None) {
                Ok(key) => Some(key),
                Err(e) => {
                    tracing::warn!("SFTP key decode error: {e}");
                    None
                }
            }
        };
        let password = (!self.cfg.password.is_empty()).then_some(self.cfg.password.as_str());
        if !authenticate(&mut session, &self.cfg.username, key, password).await? {
            return Err("SFTP authentication failed".into());
        }

        let channel = session
            .channel_open_session()
            .await
            .map_err(|e| format!("SSH channel open failed: {e}"))?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .map_err(|e| format!("SFTP subsystem request failed: {e}"))?;

        let sftp = SftpSession::new(channel.into_stream())
            .await
            .map_err(|e| format!("SFTP session init failed: {e}"))?;
        Ok((session, sftp))
    }

    async fn ensure_parent_dirs(&self, sftp: &SftpSession, full_path: &str) -> Result<(), String> {
        let parent = match full_path.rsplit_once('/') {
            Some((p, _)) if !p.is_empty() => p.to_string(),
            _ => return Ok(()),
        };

        let absolute = parent.starts_with('/');
        let mut accum = String::new();
        if absolute {
            accum.push('/');
        }
        for seg in parent.split('/').filter(|s| !s.is_empty()) {
            if !accum.is_empty() && !accum.ends_with('/') {
                accum.push('/');
            }
            accum.push_str(seg);
            match sftp.try_exists(&accum).await {
                Ok(true) => continue,
                Ok(false) => {
                    if let Err(e) = sftp.create_dir(&accum).await {
                        tracing::debug!("SFTP mkdir {accum} ignored: {e}");
                    }
                }
                Err(_) => {
                    let _ = sftp.create_dir(&accum).await;
                }
            }
        }
        Ok(())
    }

    fn full_remote_path(&self, remote_relative: &str) -> String {
        let rel = remote_relative.trim_start_matches('/');
        if self.cfg.base_path == "/" {
            return format!("/{rel}");
        }
        let base = self.cfg.base_path.trim_end_matches('/');
        if base.is_empty() {
            rel.to_string()
        } else {
            format!("{base}/{rel}")
        }
    }
}

#[async_trait]
impl UploadSink for SftpSink {
    async fn upload(&self, file: &UploadFile, ctl: &UploadControl) -> Result<String, String> {
        if ctl.cancel.is_cancelled() {
            return Err("cancelled".into());
        }

        let (_ssh, sftp) = tokio::time::timeout(CONNECT_TIMEOUT, self.connect())
            .await
            .map_err(|_| "SFTP connect timed out".to_string())?
            .inspect_err(|e| tracing::error!("SFTP connect failed: {e}"))?;
        let remote = self.full_remote_path(&file.remote_relative);
        tracing::debug!("SFTP upload starting: remote={remote}");
        self.ensure_parent_dirs(&sftp, &remote)
            .await
            .inspect_err(|e| tracing::debug!("SFTP ensure_parent_dirs({remote}) failed: {e}"))?;

        let remote_tmp = format!("{remote}.part");
        let mut remote_file = sftp
            .open_with_flags(
                &remote_tmp,
                OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::TRUNCATE,
            )
            .await
            .map_err(|e| {
                tracing::debug!("SFTP open {remote_tmp}: {e}");
                tracing::error!("SFTP open failed: {e}");
                format!("SFTP open failed: {e}")
            })?;

        let local: PathBuf = file.local_path.clone();
        let mut local_file = tokio::fs::File::open(&local)
            .await
            .map_err(|e| format!("open {}: {e}", local.display()))?;

        let mut buf = vec![0u8; COPY_BUF];
        let mut sent: u64 = 0;

        async fn discard_partial(
            mut remote_file: russh_sftp::client::fs::File,
            sftp: &SftpSession,
            remote_tmp: &str,
        ) {
            let _ = remote_file.shutdown().await;
            if let Err(e) = sftp.remove_file(remote_tmp).await {
                tracing::debug!("SFTP cleanup of partial {remote_tmp} ignored: {e}");
            }
        }

        loop {
            if ctl.cancel.is_cancelled() {
                discard_partial(remote_file, &sftp, &remote_tmp).await;
                return Err("cancelled".into());
            }

            let n = match local_file.read(&mut buf).await {
                Ok(n) => n,
                Err(e) => {
                    discard_partial(remote_file, &sftp, &remote_tmp).await;
                    return Err(format!("local read: {e}"));
                }
            };
            if n == 0 {
                break;
            }
            if let Err(e) = remote_file.write_all(&buf[..n]).await {
                discard_partial(remote_file, &sftp, &remote_tmp).await;
                return Err(format!("SFTP write: {e}"));
            }
            sent += n as u64;
            ctl.report(sent, file.size.max(sent));
        }

        if let Err(e) = remote_file.shutdown().await {
            if let Err(re) = sftp.remove_file(&remote_tmp).await {
                tracing::debug!("SFTP cleanup after close failure ignored: {re}");
            }
            return Err(format!("SFTP close: {e}"));
        }

        // Some servers' SFTPv3 `rename` fails if the destination exists
        if let Err(e) = sftp.rename(&remote_tmp, &remote).await {
            tracing::debug!(
                "SFTP rename {remote_tmp} -> {remote} failed ({e}); retrying after unlink"
            );
            let _ = sftp.remove_file(&remote).await;
            if let Err(e2) = sftp.rename(&remote_tmp, &remote).await {
                let _ = sftp.remove_file(&remote_tmp).await;
                return Err(format!("SFTP rename {remote_tmp} -> {remote}: {e2}"));
            }
        }

        ctl.report(file.size, file.size);
        let remote_for_url = remote.trim_start_matches('/');
        Ok(format!(
            "sftp://{}@{}:{}/{}",
            self.cfg.username, self.cfg.host, self.cfg.port, remote_for_url
        ))
    }

    async fn test(&self) -> Result<(), String> {
        tokio::time::timeout(Duration::from_secs(20), async {
            let (_ssh, sftp) = self.connect().await?;
            let probe = if self.cfg.base_path.trim().is_empty() {
                "/".to_string()
            } else {
                self.cfg.base_path.clone()
            };
            sftp.try_exists(&probe)
                .await
                .map_err(|e| format!("stat {probe}: {e}"))?;
            Ok::<_, String>(())
        })
        .await
        .map_err(|_| "SFTP test timed out".to_string())?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(host: &str, user: &str, pass: &str, key: &str, base: &str) -> SftpConfig {
        SftpConfig {
            host: host.into(),
            port: 22,
            username: user.into(),
            password: pass.into(),
            private_key: key.into(),
            base_path: base.into(),
        }
    }

    #[test]
    fn rejects_empty_host() {
        assert!(SftpSink::new(cfg("", "u", "p", "", "")).is_err());
        assert!(SftpSink::new(cfg("   ", "u", "p", "", "")).is_err());
    }

    #[test]
    fn rejects_empty_username() {
        assert!(SftpSink::new(cfg("h", "", "p", "", "")).is_err());
    }

    #[test]
    fn rejects_no_credentials() {
        assert!(SftpSink::new(cfg("h", "u", "", "", "")).is_err());
    }

    #[test]
    fn accepts_password_only() {
        assert!(SftpSink::new(cfg("h", "u", "secret", "", "")).is_ok());
    }

    #[test]
    fn accepts_private_key_only() {
        assert!(SftpSink::new(cfg("h", "u", "", "PEM-DATA", "")).is_ok());
    }

    #[test]
    fn full_remote_no_base() {
        let s = SftpSink::new(cfg("h", "u", "p", "", "")).unwrap();
        assert_eq!(s.full_remote_path("foo/bar.bin"), "foo/bar.bin");
        assert_eq!(s.full_remote_path("/foo/bar.bin"), "foo/bar.bin");
    }

    #[test]
    fn full_remote_with_base() {
        let s = SftpSink::new(cfg("h", "u", "p", "", "/data/uploads")).unwrap();
        assert_eq!(
            s.full_remote_path("foo/bar.bin"),
            "/data/uploads/foo/bar.bin"
        );
    }

    #[test]
    fn full_remote_strips_trailing_base_slash() {
        let s = SftpSink::new(cfg("h", "u", "p", "", "/data/")).unwrap();
        assert_eq!(s.full_remote_path("file.bin"), "/data/file.bin");
    }

    #[test]
    fn full_remote_strips_leading_relative_slash() {
        let s = SftpSink::new(cfg("h", "u", "p", "", "/data")).unwrap();
        assert_eq!(s.full_remote_path("/file.bin"), "/data/file.bin");
    }
}
