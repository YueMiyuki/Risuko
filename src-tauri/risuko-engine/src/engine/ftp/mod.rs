mod ftp_download;
mod sftp_download;
pub(crate) mod tls;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;

use serde_json::{Map, Value};
use tokio_util::sync::CancellationToken;

use super::speed_limiter::SpeedLimiter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FtpProtocol {
    Ftp,
    Ftps,
    Sftp,
}

#[derive(Debug, Clone)]
pub struct FtpUri {
    pub protocol: FtpProtocol,
    pub user: Option<String>,
    pub password: Option<String>,
    pub host: String,
    pub port: u16,
    pub path: String,
}

pub fn is_ftp_uri(uri: &str) -> bool {
    detect_ftp_protocol(uri).is_some()
}

pub fn detect_ftp_protocol(uri: &str) -> Option<FtpProtocol> {
    let lower = uri.trim().to_lowercase();
    if lower.starts_with("sftp://") {
        Some(FtpProtocol::Sftp)
    } else if lower.starts_with("ftps://") {
        Some(FtpProtocol::Ftps)
    } else if lower.starts_with("ftp://") {
        Some(FtpProtocol::Ftp)
    } else {
        None
    }
}

pub fn parse_ftp_uri(uri: &str) -> Result<FtpUri, String> {
    let protocol = detect_ftp_protocol(uri).ok_or("Not an FTP/FTPS/SFTP URI")?;

    let default_port = match protocol {
        FtpProtocol::Ftp => 21,
        FtpProtocol::Ftps => 990,
        FtpProtocol::Sftp => 22,
    };

    let parsed = url::Url::parse(uri.trim()).map_err(|e| format!("Invalid URI: {e}"))?;

    let decode = |s: &str| -> Result<String, String> {
        percent_encoding::percent_decode_str(s)
            .decode_utf8()
            .map(|c| c.into_owned())
            .map_err(|e| e.to_string())
    };

    let host = match parsed.host_str() {
        Some(h) if !h.is_empty() => h.trim_start_matches('[').trim_end_matches(']').to_string(),
        _ => return Err("Empty host in URI".to_string()),
    };

    let user = match parsed.username() {
        "" => None,
        u => Some(decode(u)?),
    };
    let password = parsed.password().map(decode).transpose()?;

    let path = decode(parsed.path())?;

    Ok(FtpUri {
        protocol,
        user,
        password,
        host,
        port: parsed.port().unwrap_or(default_port),
        path: if path.is_empty() {
            "/".to_string()
        } else {
            path
        },
    })
}

#[allow(clippy::too_many_arguments)]
pub async fn run_ftp_download(
    uri: &str,
    dir: &str,
    out: &str,
    options: &Map<String, Value>,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
    cancel_token: CancellationToken,
    global_limiter: Arc<SpeedLimiter>,
    task_limiter: Arc<SpeedLimiter>,
) -> Result<PathBuf, String> {
    let parsed = parse_ftp_uri(uri)?;

    match parsed.protocol {
        FtpProtocol::Ftp | FtpProtocol::Ftps => {
            ftp_download::run_ftp_ftps_download(
                &parsed,
                dir,
                out,
                options,
                total,
                completed,
                speed,
                connections,
                cancel_token,
                global_limiter,
                task_limiter,
            )
            .await
        }
        FtpProtocol::Sftp => {
            sftp_download::run_sftp_download(
                &parsed,
                dir,
                out,
                options,
                total,
                completed,
                speed,
                connections,
                cancel_token,
                global_limiter,
                task_limiter,
            )
            .await
        }
    }
}

pub(crate) struct PartClaim(PathBuf);

static ACTIVE_PARTS: std::sync::LazyLock<std::sync::Mutex<std::collections::HashSet<PathBuf>>> =
    std::sync::LazyLock::new(Default::default);

fn claim_key(path: &std::path::Path) -> PathBuf {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    match (parent.canonicalize(), path.file_name()) {
        (Ok(parent), Some(name)) => parent.join(name),
        _ => path.to_path_buf(),
    }
}

impl PartClaim {
    pub(crate) fn acquire(path: &std::path::Path) -> Result<Self, String> {
        let key = claim_key(path);
        let mut active = ACTIVE_PARTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !active.insert(key.clone()) {
            return Err(format!(
                "Another active download is already writing {}",
                path.display()
            ));
        }
        Ok(Self(key))
    }
}

impl Drop for PartClaim {
    fn drop(&mut self) {
        ACTIVE_PARTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn part_claim_refuses_a_second_writer_until_released() {
        let path = PathBuf::from("/claim-test/file.bin.part");
        let first = PartClaim::acquire(&path).unwrap();
        assert!(PartClaim::acquire(&path).is_err());
        assert!(PartClaim::acquire(&PathBuf::from("/claim-test/other.part")).is_ok());
        drop(first);
        assert!(PartClaim::acquire(&path).is_ok());
    }

    #[test]
    fn part_claim_treats_equivalent_paths_as_one() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("downloads");
        std::fs::create_dir(&sub).unwrap();
        let direct = sub.join("file.bin.part");
        let dotted = sub.join("..").join("downloads").join("file.bin.part");
        let first = PartClaim::acquire(&direct).unwrap();
        assert!(PartClaim::acquire(&dotted).is_err());
        drop(first);
        let second = PartClaim::acquire(&dotted).unwrap();
        assert!(PartClaim::acquire(&direct).is_err());
        drop(second);
        assert!(PartClaim::acquire(&direct).is_ok());
    }

    #[test]
    fn test_detect_ftp_protocol() {
        assert_eq!(
            detect_ftp_protocol("ftp://host/file"),
            Some(FtpProtocol::Ftp)
        );
        assert_eq!(
            detect_ftp_protocol("ftps://host/file"),
            Some(FtpProtocol::Ftps)
        );
        assert_eq!(
            detect_ftp_protocol("sftp://host/file"),
            Some(FtpProtocol::Sftp)
        );
        assert_eq!(detect_ftp_protocol("http://host/file"), None);
        assert_eq!(
            detect_ftp_protocol("FTP://HOST/file"),
            Some(FtpProtocol::Ftp)
        );
    }

    #[test]
    fn test_parse_ftp_uri_simple() {
        let uri = parse_ftp_uri("ftp://example.com/pub/file.zip").unwrap();
        assert_eq!(uri.protocol, FtpProtocol::Ftp);
        assert_eq!(uri.host, "example.com");
        assert_eq!(uri.port, 21);
        assert_eq!(uri.path, "/pub/file.zip");
        assert!(uri.user.is_none());
        assert!(uri.password.is_none());
    }

    #[test]
    fn test_parse_ftp_uri_with_credentials() {
        let uri = parse_ftp_uri("ftp://user:p%40ss@host:2121/dir/file.zip").unwrap();
        assert_eq!(uri.user.as_deref(), Some("user"));
        assert_eq!(uri.password.as_deref(), Some("p@ss"));
        assert_eq!(uri.host, "host");
        assert_eq!(uri.port, 2121);
        assert_eq!(uri.path, "/dir/file.zip");
    }

    #[test]
    fn test_parse_sftp_uri() {
        let uri = parse_ftp_uri("sftp://myuser@server.com/home/file.tar.gz").unwrap();
        assert_eq!(uri.protocol, FtpProtocol::Sftp);
        assert_eq!(uri.user.as_deref(), Some("myuser"));
        assert!(uri.password.is_none());
        assert_eq!(uri.host, "server.com");
        assert_eq!(uri.port, 22);
        assert_eq!(uri.path, "/home/file.tar.gz");
    }

    #[test]
    fn test_parse_ftps_uri() {
        let uri = parse_ftp_uri("ftps://host:990/file.bin").unwrap();
        assert_eq!(uri.protocol, FtpProtocol::Ftps);
        assert_eq!(uri.port, 990);
    }

    #[test]
    fn test_basename_from_ftp_path() {
        assert_eq!(
            ftp_download::basename_from_ftp_path("/dir/file.zip"),
            "file.zip"
        );
        assert_eq!(ftp_download::basename_from_ftp_path("/"), "download");
        assert_eq!(ftp_download::basename_from_ftp_path(""), "download");
    }

    #[test]
    fn test_is_ftp_uri() {
        assert!(is_ftp_uri("ftp://host/file"));
        assert!(is_ftp_uri("ftps://host/file"));
        assert!(is_ftp_uri("sftp://host/file"));
        assert!(!is_ftp_uri("http://host/file"));
        assert!(!is_ftp_uri("magnet:?xt=urn:btih:abc"));
    }
}
