use std::time::Duration;

use async_trait::async_trait;
use suppaftp::tokio::{AsyncFtpStream, AsyncRustlsConnector, AsyncRustlsFtpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::sink::{
    write_or_stall, FtpConfig, UploadControl, UploadError, UploadFile, UploadSink,
    UPLOAD_CLEANUP_TIMEOUT, UPLOAD_STALL_TIMEOUT,
};

const COPY_BUF: usize = 64 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

fn ftp_error(context: &str, error: &suppaftp::FtpError) -> UploadError {
    use suppaftp::FtpError;
    let message = format!("{context}: {error}");
    match error {
        FtpError::UnexpectedResponse(reply) if reply.body.first() == Some(&b'5') => {
            UploadError::fatal(message)
        }
        FtpError::UnexpectedResponse(_) | FtpError::ConnectionError(_) | FtpError::BadResponse => {
            UploadError::transient(message)
        }
        FtpError::SecureError(_)
        | FtpError::InvalidAddress(_)
        | FtpError::DataConnectionAlreadyOpen => UploadError::fatal(message),
    }
}

pub struct FtpSink {
    cfg: FtpConfig,
}

impl FtpSink {
    pub fn new(cfg: FtpConfig) -> Result<Self, String> {
        if cfg.host.trim().is_empty() {
            return Err("FTP host is empty".into());
        }
        Ok(Self { cfg })
    }

    fn user(&self) -> String {
        if self.cfg.username.trim().is_empty() {
            "anonymous".to_string()
        } else {
            self.cfg.username.clone()
        }
    }

    fn pass(&self) -> String {
        if self.cfg.password.is_empty() {
            "risuko@".to_string()
        } else {
            self.cfg.password.clone()
        }
    }

    fn full_remote(&self, remote_relative: &str) -> String {
        let base = self.cfg.base_path.trim_end_matches('/');
        let rel = remote_relative.trim_start_matches('/');
        if base.is_empty() {
            rel.to_string()
        } else {
            format!("{base}/{rel}")
        }
    }

    fn result_url(&self, remote: &str) -> String {
        let scheme = if self.cfg.secure { "ftps" } else { "ftp" };
        format!(
            "{scheme}://{}@{}:{}/{}",
            self.user(),
            self.cfg.host,
            self.cfg.port,
            remote.trim_start_matches('/')
        )
    }

    fn tls_connector(&self) -> AsyncRustlsConnector {
        crate::engine::ftp::tls::ftps_tls_connector(self.cfg.insecure)
    }

    async fn connect_secure(&self) -> Result<AsyncRustlsFtpStream, UploadError> {
        let addr = format!("{}:{}", self.cfg.host, self.cfg.port);
        let connector = self.tls_connector();
        let mut ftp = if is_implicit_ftps_port(self.cfg.port) {
            AsyncRustlsFtpStream::connect_secure_implicit(&addr, connector, &self.cfg.host)
                .await
                .map_err(|e| ftp_error("FTPS connect failed", &e))?
        } else {
            AsyncRustlsFtpStream::connect(&addr)
                .await
                .map_err(|e| ftp_error("FTPS connect failed", &e))?
                .into_secure(connector, &self.cfg.host)
                .await
                .map_err(|e| ftp_error("FTPS AUTH TLS failed", &e))?
        };
        ftp.set_passive_nat_workaround(true);
        Ok(ftp)
    }
}

fn is_implicit_ftps_port(port: u16) -> bool {
    port == 990
}

macro_rules! ensure_dirs {
    ($ftp:expr, $full_path:expr) => {{
        let parent = match $full_path.rsplit_once('/') {
            Some((p, _)) if !p.is_empty() => p.to_string(),
            _ => String::new(),
        };
        if !parent.is_empty() {
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
                let _ = $ftp.mkdir(&accum).await;
            }
        }
    }};
}

macro_rules! copy_to {
    ($ftp:expr, $local:expr, $remote:expr, $size:expr, $ctl:expr) => {{
        let mut local_file = tokio::fs::File::open(&$local)
            .await
            .map_err(|e| UploadError::fatal(format!("open {}: {e}", $local.display())))?;

        let part_remote = format!("{}.part", $remote);
        let mut writer = $ftp
            .put_with_stream(&part_remote)
            .await
            .map_err(|e| ftp_error(&format!("FTP STOR {}", part_remote), &e))?;

        let mut buf = vec![0u8; COPY_BUF];
        let mut sent: u64 = 0;
        let copy_res: Result<(), UploadError> = async {
            loop {
                if $ctl.cancel.is_cancelled() {
                    return Err(UploadError::fatal("cancelled"));
                }
                let n = local_file
                    .read(&mut buf)
                    .await
                    .map_err(|e| UploadError::fatal(format!("local read: {e}")))?;
                if n == 0 {
                    break;
                }
                write_or_stall(&mut writer, &buf[..n], &$ctl.cancel, "FTP").await?;
                sent += n as u64;
                $ctl.report(sent, $size.max(sent));
            }
            tokio::time::timeout(UPLOAD_STALL_TIMEOUT, writer.flush())
                .await
                .map_err(|_| UploadError::transient("FTP flush timed out"))?
                .map_err(|e| UploadError::transient(format!("FTP flush: {e}")))?;
            Ok(())
        }
        .await;

        if let Err(e) = copy_res {
            let _ = tokio::time::timeout(UPLOAD_CLEANUP_TIMEOUT, async {
                let _ = writer.finish().await;
                let _ = $ftp.rm(part_remote.as_str()).await;
            })
            .await;
            return Err(e);
        }
        writer
            .finish()
            .await
            .map_err(|e| ftp_error("FTP finalize", &e))?;

        if $ftp
            .rename(part_remote.as_str(), $remote.as_str())
            .await
            .is_err()
        {
            let _ = $ftp.rm($remote.as_str()).await;
            $ftp.rename(part_remote.as_str(), $remote.as_str())
                .await
                .map_err(|e| {
                    ftp_error(&format!("FTP rename {} -> {}", part_remote, $remote), &e)
                })?;
        }
    }};
}

#[async_trait]
impl UploadSink for FtpSink {
    async fn upload(&self, file: &UploadFile, ctl: &UploadControl) -> Result<String, UploadError> {
        if ctl.cancel.is_cancelled() {
            return Err(UploadError::fatal("cancelled"));
        }

        let addr = format!("{}:{}", self.cfg.host, self.cfg.port);
        let remote = self.full_remote(&file.remote_relative);
        let size = file.size;
        let local = file.local_path.clone();
        let user = self.user();
        let pass = self.pass();

        if self.cfg.secure {
            let mut ftp = tokio::time::timeout(CONNECT_TIMEOUT, self.connect_secure())
                .await
                .map_err(|_| UploadError::transient("FTPS connect timed out"))??;
            ftp.login(&user, &pass)
                .await
                .map_err(|e| ftp_error("FTP login failed", &e))?;
            ftp.transfer_type(suppaftp::types::FileType::Binary)
                .await
                .map_err(|e| ftp_error("FTP binary mode", &e))?;
            ensure_dirs!(ftp, remote);
            copy_to!(ftp, local, remote, size, ctl);
            let _ = ftp.quit().await;
        } else {
            let mut ftp = tokio::time::timeout(CONNECT_TIMEOUT, AsyncFtpStream::connect(&addr))
                .await
                .map_err(|_| UploadError::transient("FTP connect timed out"))?
                .map_err(|e| ftp_error("FTP connect failed", &e))?;
            ftp.set_passive_nat_workaround(true);
            ftp.login(&user, &pass)
                .await
                .map_err(|e| ftp_error("FTP login failed", &e))?;
            ftp.transfer_type(suppaftp::types::FileType::Binary)
                .await
                .map_err(|e| ftp_error("FTP binary mode", &e))?;
            ensure_dirs!(ftp, remote);
            copy_to!(ftp, local, remote, size, ctl);
            let _ = ftp.quit().await;
        }

        ctl.report(size, size);
        Ok(self.result_url(&remote))
    }

    async fn test(&self) -> Result<(), String> {
        let addr = format!("{}:{}", self.cfg.host, self.cfg.port);
        let user = self.user();
        let pass = self.pass();
        let secure = self.cfg.secure;

        tokio::time::timeout(Duration::from_secs(20), async move {
            if secure {
                let mut ftp = self.connect_secure().await.map_err(|e| e.message)?;
                ftp.login(&user, &pass)
                    .await
                    .map_err(|e| format!("FTP login failed: {e}"))?;
                ftp.pwd().await.map_err(|e| format!("FTP PWD: {e}"))?;
                let _ = ftp.quit().await;
            } else {
                let mut ftp = AsyncFtpStream::connect(&addr)
                    .await
                    .map_err(|e| format!("FTP connect failed: {e}"))?;
                ftp.login(&user, &pass)
                    .await
                    .map_err(|e| format!("FTP login failed: {e}"))?;
                ftp.pwd().await.map_err(|e| format!("FTP PWD: {e}"))?;
                let _ = ftp.quit().await;
            }
            Ok::<_, String>(())
        })
        .await
        .map_err(|_| "FTP test timed out".to_string())?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(host: &str, user: &str, pass: &str, base: &str, secure: bool) -> FtpConfig {
        FtpConfig {
            host: host.into(),
            port: if secure { 990 } else { 21 },
            username: user.into(),
            password: pass.into(),
            base_path: base.into(),
            secure,
            insecure: false,
        }
    }

    fn reply(code: u32) -> suppaftp::FtpError {
        suppaftp::FtpError::UnexpectedResponse(suppaftp::types::Response {
            status: suppaftp::Status::from(code),
            body: format!("{code} reply").into_bytes(),
        })
    }

    #[test]
    fn permanent_replies_are_fatal_and_temporary_ones_retry() {
        for code in [550, 553, 552, 530, 532, 534, 554] {
            assert!(!ftp_error("x", &reply(code)).retryable, "{code}");
        }
        for code in [421, 425, 426, 451, 431] {
            assert!(ftp_error("x", &reply(code)).retryable, "{code}");
        }
    }

    #[test]
    fn dropped_connections_retry_and_bad_addresses_do_not() {
        let dropped = suppaftp::FtpError::ConnectionError(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset,
        ));
        assert!(ftp_error("x", &dropped).retryable);
        assert!(ftp_error("x", &suppaftp::FtpError::BadResponse).retryable);
        let invalid = "nope".parse::<std::net::IpAddr>().unwrap_err();
        assert!(!ftp_error("x", &suppaftp::FtpError::InvalidAddress(invalid)).retryable);
    }

    #[test]
    fn tls_failures_are_not_retried() {
        let rejected = suppaftp::FtpError::SecureError("invalid peer certificate".into());
        assert!(!ftp_error("x", &rejected).retryable);
    }

    #[test]
    fn only_port_990_is_implicit_ftps() {
        assert!(is_implicit_ftps_port(990));
        assert!(!is_implicit_ftps_port(21));
        assert!(!is_implicit_ftps_port(2121));
    }

    #[test]
    fn rejects_empty_host() {
        assert!(FtpSink::new(cfg("", "u", "p", "", false)).is_err());
        assert!(FtpSink::new(cfg("   ", "u", "p", "", false)).is_err());
    }

    #[test]
    fn accepts_anonymous() {
        assert!(FtpSink::new(cfg("ftp.example.com", "", "", "", false)).is_ok());
    }

    #[test]
    fn user_defaults_to_anonymous() {
        let s = FtpSink::new(cfg("h", "", "", "", false)).unwrap();
        assert_eq!(s.user(), "anonymous");
    }

    #[test]
    fn user_from_config() {
        let s = FtpSink::new(cfg("h", "alice", "", "", false)).unwrap();
        assert_eq!(s.user(), "alice");
    }

    #[test]
    fn pass_defaults_to_email_form() {
        let s = FtpSink::new(cfg("h", "u", "", "", false)).unwrap();
        assert_eq!(s.pass(), "risuko@");
    }

    #[test]
    fn pass_from_config() {
        let s = FtpSink::new(cfg("h", "u", "secret", "", false)).unwrap();
        assert_eq!(s.pass(), "secret");
    }

    #[test]
    fn full_remote_no_base() {
        let s = FtpSink::new(cfg("h", "u", "p", "", false)).unwrap();
        assert_eq!(s.full_remote("foo/bar.bin"), "foo/bar.bin");
        assert_eq!(s.full_remote("/foo/bar.bin"), "foo/bar.bin");
    }

    #[test]
    fn full_remote_with_base() {
        let s = FtpSink::new(cfg("h", "u", "p", "/uploads", false)).unwrap();
        assert_eq!(s.full_remote("file.bin"), "/uploads/file.bin");
    }

    #[test]
    fn full_remote_strips_extra_slashes() {
        let s = FtpSink::new(cfg("h", "u", "p", "/uploads/", false)).unwrap();
        assert_eq!(s.full_remote("/file.bin"), "/uploads/file.bin");
    }

    #[test]
    fn result_url_ftp_scheme() {
        let s = FtpSink::new(cfg("ftp.example.com", "alice", "p", "", false)).unwrap();
        let u = s.result_url("/uploads/file.bin");
        assert_eq!(u, "ftp://alice@ftp.example.com:21/uploads/file.bin");
    }

    #[test]
    fn result_url_ftps_scheme() {
        let s = FtpSink::new(cfg("ftp.example.com", "alice", "p", "", true)).unwrap();
        let u = s.result_url("/uploads/file.bin");
        assert_eq!(u, "ftps://alice@ftp.example.com:990/uploads/file.bin");
    }

    #[test]
    fn result_url_anonymous() {
        let s = FtpSink::new(cfg("ftp.example.com", "", "", "", false)).unwrap();
        let u = s.result_url("file.bin");
        assert_eq!(u, "ftp://anonymous@ftp.example.com:21/file.bin");
    }
}
