use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct UploadFile {
    pub local_path: PathBuf,
    pub remote_relative: String,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct UploadProgress {
    pub uploaded: u64,
    pub total: u64,
}

#[derive(Clone)]
pub struct UploadControl {
    pub cancel: CancellationToken,
    pub progress: watch::Sender<UploadProgress>,
}

impl UploadControl {
    pub fn report(&self, uploaded: u64, total: u64) {
        let _ = self.progress.send(UploadProgress { uploaded, total });
    }
}

pub(super) const UPLOAD_STALL_TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Debug)]
pub(super) struct Heartbeat {
    start: Instant,
    last_ms: AtomicU64,
}

impl Heartbeat {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            start: Instant::now(),
            last_ms: AtomicU64::new(0),
        })
    }

    pub(super) fn touch(&self) {
        let ms = self.start.elapsed().as_millis() as u64;
        self.last_ms.store(ms, Ordering::Relaxed);
    }

    fn idle(&self) -> Duration {
        let now = self.start.elapsed().as_millis() as u64;
        Duration::from_millis(now.saturating_sub(self.last_ms.load(Ordering::Relaxed)))
    }
}

pub(super) async fn run_with_stall<F, T>(
    fut: F,
    hb: &Heartbeat,
    stall: Duration,
) -> Result<T, String>
where
    F: std::future::Future<Output = Result<T, String>>,
{
    tokio::pin!(fut);
    loop {
        let idle = hb.idle();
        if idle >= stall {
            return Err(format!(
                "upload stalled: no data moved for {}s",
                stall.as_secs()
            ));
        }
        tokio::select! {
            r = &mut fut => return r,
            _ = tokio::time::sleep(stall - idle) => {}
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum SinkConfig {
    Webdav(WebdavConfig),
    S3(S3Config),
    Sftp(SftpConfig),
    Ftp(FtpConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WebdavConfig {
    pub endpoint: String,
    #[serde(default)]
    pub base_path: String,
    #[serde(default)]
    pub username: String,
    #[serde(default, skip_serializing)]
    pub password: String,
    #[serde(default)]
    pub insecure: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct S3Config {
    pub endpoint: String,
    #[serde(default)]
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    #[serde(default, skip_serializing)]
    pub secret_access_key: String,
    #[serde(default)]
    pub prefix: String,
    #[serde(default)]
    pub force_path_style: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SftpConfig {
    pub host: String,
    #[serde(default = "default_sftp_port")]
    pub port: u16,
    pub username: String,
    #[serde(default, skip_serializing)]
    pub password: String,
    #[serde(default, skip_serializing)]
    pub private_key: String,
    #[serde(default)]
    pub base_path: String,
}

fn default_sftp_port() -> u16 {
    22
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct FtpConfig {
    pub host: String,
    #[serde(default = "default_ftp_port")]
    pub port: u16,
    #[serde(default)]
    pub username: String,
    #[serde(default, skip_serializing)]
    pub password: String,
    #[serde(default)]
    pub base_path: String,
    #[serde(default)]
    pub secure: bool,
    #[serde(default)]
    pub insecure: bool,
}

fn default_ftp_port() -> u16 {
    21
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
#[derive(Default)]
pub enum PostUploadAction {
    #[default]
    Keep,
    Trash,
    Move,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadSinkRecord {
    pub id: String,
    pub label: String,
    pub config: SinkConfig,
    #[serde(default)]
    pub post_action: PostUploadAction,
    #[serde(default)]
    pub move_target: Option<PathBuf>,
    pub created_at: u64,
    #[serde(default)]
    pub last_used_at: Option<u64>,
}

#[async_trait]
pub trait UploadSink: Send + Sync {
    async fn upload(&self, file: &UploadFile, ctl: &UploadControl) -> Result<String, String>;

    async fn test(&self) -> Result<(), String>;
}

pub type BoxedSink = Arc<dyn UploadSink>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn webdav_config_round_trip_camel_case() {
        let cfg = SinkConfig::Webdav(WebdavConfig {
            endpoint: "https://dav.example.com".into(),
            base_path: "uploads".into(),
            username: "u".into(),
            password: String::new(),
            insecure: true,
        });
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(json.contains("\"kind\":\"webdav\""));
        assert!(json.contains("\"basePath\":\"uploads\""));
        let back: SinkConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, back);
    }

    #[test]
    fn s3_config_serializes_camel_case() {
        let cfg = SinkConfig::S3(S3Config {
            endpoint: "https://s3.amazonaws.com".into(),
            region: "us-west-2".into(),
            bucket: "mybucket".into(),
            access_key_id: "AKIA".into(),
            secret_access_key: String::new(),
            prefix: "folder".into(),
            force_path_style: true,
        });
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(json.contains("\"kind\":\"s3\""));
        assert!(json.contains("\"accessKeyId\":\"AKIA\""));
        assert!(json.contains("\"forcePathStyle\":true"));
        let back: SinkConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(cfg, back);
    }

    #[test]
    fn sftp_default_port_22() {
        let json = r#"{"kind":"sftp","host":"h","username":"u"}"#;
        let cfg: SinkConfig = serde_json::from_str(json).unwrap();
        match cfg {
            SinkConfig::Sftp(c) => {
                assert_eq!(c.port, 22);
                assert_eq!(c.password, "");
                assert_eq!(c.private_key, "");
                assert_eq!(c.base_path, "");
            }
            _ => panic!("expected sftp"),
        }
    }

    #[test]
    fn ftp_default_port_21() {
        let json = r#"{"kind":"ftp","host":"h"}"#;
        let cfg: SinkConfig = serde_json::from_str(json).unwrap();
        match cfg {
            SinkConfig::Ftp(c) => {
                assert_eq!(c.port, 21);
                assert!(!c.secure);
                assert_eq!(c.username, "");
            }
            _ => panic!("expected ftp"),
        }
    }

    #[test]
    fn post_action_serde_kebab_case() {
        assert_eq!(
            serde_json::to_string(&PostUploadAction::Keep).unwrap(),
            "\"keep\""
        );
        assert_eq!(
            serde_json::to_string(&PostUploadAction::Trash).unwrap(),
            "\"trash\""
        );
        assert_eq!(
            serde_json::to_string(&PostUploadAction::Move).unwrap(),
            "\"move\""
        );
        assert_eq!(PostUploadAction::default(), PostUploadAction::Keep);
    }

    #[test]
    fn upload_sink_record_round_trip() {
        let rec = UploadSinkRecord {
            id: "id-1".into(),
            label: "My Sink".into(),
            config: SinkConfig::Webdav(WebdavConfig {
                endpoint: "https://e".into(),
                base_path: String::new(),
                username: String::new(),
                password: String::new(),
                insecure: false,
            }),
            post_action: PostUploadAction::Trash,
            move_target: None,
            created_at: 1_700_000_000,
            last_used_at: Some(1_700_000_100),
        };
        let json = serde_json::to_string(&rec).unwrap();
        assert!(json.contains("\"postAction\":\"trash\""));
        assert!(json.contains("\"createdAt\":1700000000"));
        let back: UploadSinkRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, rec.id);
        assert_eq!(back.post_action, rec.post_action);
    }

    #[tokio::test]
    async fn upload_control_report_propagates() {
        let (tx, rx) = watch::channel(UploadProgress {
            uploaded: 0,
            total: 0,
        });
        let ctl = UploadControl {
            cancel: CancellationToken::new(),
            progress: tx,
        };
        ctl.report(50, 100);
        let snap = rx.borrow().clone();
        assert_eq!(snap.uploaded, 50);
        assert_eq!(snap.total, 100);
    }

    #[tokio::test(start_paused = true)]
    async fn run_with_stall_fails_when_idle() {
        let hb = Heartbeat::new();
        let r: Result<(), String> =
            run_with_stall(std::future::pending(), &hb, Duration::from_secs(10)).await;
        assert!(r.unwrap_err().contains("stalled"));
    }

    #[tokio::test(start_paused = true)]
    async fn run_with_stall_survives_while_progressing() {
        let hb = Heartbeat::new();
        let hb2 = hb.clone();
        let work = async move {
            for _ in 0..10 {
                tokio::time::sleep(Duration::from_secs(8)).await;
                hb2.touch();
            }
            Ok::<_, String>(7)
        };
        let r = run_with_stall(work, &hb, Duration::from_secs(10)).await;
        assert_eq!(r.unwrap(), 7);
    }
}
