use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use risuko_http::{Client, ClientBuilder, StatusCode, Url};

use super::sink::{
    run_with_stall, Heartbeat, UploadControl, UploadFile, UploadSink, WebdavConfig,
    UPLOAD_STALL_TIMEOUT,
};
use crate::engine::util::ERROR_SNIPPET_BYTES;

pub struct WebdavSink {
    cfg: WebdavConfig,
    client: Client,
    base_url: Url,
}

impl WebdavSink {
    pub fn new(cfg: WebdavConfig) -> Result<Self, String> {
        let endpoint = cfg.endpoint.trim_end_matches('/').to_string();
        if endpoint.is_empty() {
            return Err("WebDAV endpoint is empty".into());
        }

        let base_url = Url::parse(&format!("{endpoint}/"))
            .map_err(|e| format!("Invalid WebDAV endpoint URL: {e}"))?;

        let client = ClientBuilder::new()
            .connect_timeout(Duration::from_secs(30))
            .danger_accept_invalid_certs(cfg.insecure)
            .user_agent("risuko/upload")
            .build()
            .map_err(|e| format!("Failed to build HTTP client: {e}"))?;

        Ok(Self {
            cfg,
            client,
            base_url,
        })
    }

    fn auth_header(&self) -> Option<String> {
        if self.cfg.username.is_empty() && self.cfg.password.is_empty() {
            return None;
        }
        let raw = format!("{}:{}", self.cfg.username, self.cfg.password);
        Some(format!("Basic {}", STANDARD.encode(raw)))
    }

    fn resolve(&self, remote_relative: &str) -> Result<Url, String> {
        use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
        const PATH_SAFE: &AsciiSet = &NON_ALPHANUMERIC
            .remove(b'-')
            .remove(b'_')
            .remove(b'.')
            .remove(b'~')
            .remove(b'/');

        let combined = if self.cfg.base_path.trim_matches('/').is_empty() {
            remote_relative.trim_start_matches('/').to_string()
        } else {
            format!(
                "{}/{}",
                self.cfg.base_path.trim_matches('/'),
                remote_relative.trim_start_matches('/')
            )
        };
        if remote_relative
            .trim_start_matches('/')
            .split('/')
            .any(|seg| seg == "." || seg == "..")
        {
            return Err(format!(
                "Path traversal not allowed in remote path: '{remote_relative}'"
            ));
        }
        let encoded = utf8_percent_encode(&combined, PATH_SAFE).to_string();
        self.base_url
            .join(&encoded)
            .map_err(|e| format!("Invalid remote path '{combined}': {e}"))
    }

    async fn put_file(
        &self,
        target: &Url,
        file: &UploadFile,
        ctl: &UploadControl,
    ) -> Result<risuko_http::Response, String> {
        let path: PathBuf = file.local_path.clone();
        let total = file.size;
        let progress = ctl.clone();
        let hb = Heartbeat::new();
        let hb_cb = hb.clone();
        let body = risuko_http::file_stream_body_with_progress(
            path.clone(),
            total,
            move |sent| {
                hb_cb.touch();
                progress.report(sent.min(total), total)
            },
            Some(ctl.cancel.clone()),
        );

        let mut req = self
            .client
            .put(target.as_str())
            .stream_body(body)
            .header("content-length", file.size.to_string());
        if let Some(auth) = self.auth_header() {
            req = req.header("authorization", auth);
        }

        let send_fut = async { req.send().await.map_err(|e| format!("PUT failed: {e}")) };
        let resp = tokio::select! {
            _ = ctl.cancel.cancelled() => return Err("cancelled".into()),
            r = run_with_stall(send_fut, &hb, UPLOAD_STALL_TIMEOUT) => r?,
        };

        Ok(resp)
    }

    async fn ensure_parent_dirs(&self, target: &Url) -> Result<(), String> {
        let base_path = self.base_url.path().trim_end_matches('/');
        let target_path = target.path();
        let rel = target_path.strip_prefix(base_path).unwrap_or(target_path);
        let mut segments: Vec<&str> = rel.split('/').filter(|s| !s.is_empty()).collect();
        segments.pop();
        if segments.is_empty() {
            return Ok(());
        }

        let mut accum = String::new();
        for seg in &segments {
            accum.push('/');
            accum.push_str(seg);
            let mut url = self.base_url.clone();
            url.set_path(&format!("{base_path}{accum}/"));

            let mut req = self
                .client
                .request(
                    risuko_http::Method::from_bytes(b"MKCOL").expect("MKCOL is a valid HTTP token"),
                    url.as_str(),
                )
                .timeout(Duration::from_secs(30));
            if let Some(auth) = self.auth_header() {
                req = req.header("authorization", auth);
            }
            let resp = req.send().await.map_err(|e| format!("MKCOL: {e}"))?;
            let status = resp.status();
            if !matches!(
                status,
                StatusCode::CREATED | StatusCode::METHOD_NOT_ALLOWED | StatusCode::OK
            ) {
                let body = resp.snippet(ERROR_SNIPPET_BYTES).await;
                return Err(format!(
                    "MKCOL {} failed with status {}: {}",
                    url, status, body
                ));
            }
        }
        Ok(())
    }
}

#[async_trait]
impl UploadSink for WebdavSink {
    async fn upload(&self, file: &UploadFile, ctl: &UploadControl) -> Result<String, String> {
        let target = self.resolve(&file.remote_relative)?;

        if ctl.cancel.is_cancelled() {
            return Err("cancelled".into());
        }

        let mut created_parents = false;
        loop {
            let resp = match self.put_file(&target, file, ctl).await {
                Ok(r) => r,
                Err(e) if !created_parents && !ctl.cancel.is_cancelled() => {
                    created_parents = true;
                    if self.ensure_parent_dirs(&target).await.is_err() {
                        return Err(e);
                    }
                    continue;
                }
                Err(e) => return Err(e),
            };
            let status = resp.status();
            if matches!(status, StatusCode::CONFLICT | StatusCode::NOT_FOUND) && !created_parents {
                created_parents = true;
                self.ensure_parent_dirs(&target).await?;
                continue;
            }
            if !status.is_success() {
                let body = resp.snippet(ERROR_SNIPPET_BYTES).await;
                return Err(format!("PUT {} returned {}: {}", target, status, body));
            }
            break;
        }

        ctl.report(file.size, file.size);

        Ok(target.to_string())
    }

    async fn test(&self) -> Result<(), String> {
        let mut req = self
            .client
            .request(risuko_http::Method::OPTIONS, self.base_url.as_str())
            .timeout(Duration::from_secs(15));
        if let Some(auth) = self.auth_header() {
            req = req.header("authorization", auth);
        }
        let resp = req.send().await.map_err(|e| format!("OPTIONS: {e}"))?;
        let status = resp.status();
        if !status.is_success() {
            return Err(format!("WebDAV server returned {}", status));
        }
        if !resp.headers().contains_key("dav") {
            tracing::warn!("WebDAV test: server reachable but missing DAV header");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(endpoint: &str, base: &str) -> WebdavConfig {
        WebdavConfig {
            endpoint: endpoint.into(),
            base_path: base.into(),
            username: String::new(),
            password: String::new(),
            insecure: false,
        }
    }

    #[test]
    fn empty_endpoint_rejected() {
        let res = WebdavSink::new(cfg("", ""));
        assert!(res.is_err());
        assert!(res.err().unwrap().to_lowercase().contains("endpoint"));
    }

    #[test]
    fn malformed_endpoint_rejected() {
        let res = WebdavSink::new(cfg("not a url", ""));
        assert!(res.is_err());
        assert!(res.err().unwrap().to_lowercase().contains("invalid"));
    }

    #[test]
    fn resolve_no_base_path() {
        let s = WebdavSink::new(cfg("https://dav.example.com", "")).unwrap();
        let u = s.resolve("foo/bar.bin").unwrap();
        assert_eq!(u.as_str(), "https://dav.example.com/foo/bar.bin");
    }

    #[test]
    fn resolve_with_base_path() {
        let s = WebdavSink::new(cfg("https://dav.example.com", "uploads")).unwrap();
        let u = s.resolve("a/b.bin").unwrap();
        assert_eq!(u.as_str(), "https://dav.example.com/uploads/a/b.bin");
    }

    #[test]
    fn resolve_strips_leading_and_trailing_slashes() {
        let s = WebdavSink::new(cfg("https://dav.example.com/", "/uploads/")).unwrap();
        let u = s.resolve("/file.bin").unwrap();
        assert_eq!(u.as_str(), "https://dav.example.com/uploads/file.bin");
    }

    #[test]
    fn resolve_percent_encodes_special_chars() {
        let s = WebdavSink::new(cfg("https://dav.example.com", "")).unwrap();
        let u = s.resolve("dir/hello world.mp4").unwrap();
        assert!(
            u.as_str().contains("hello%20world.mp4"),
            "got {}",
            u.as_str()
        );
    }

    #[test]
    fn resolve_preserves_path_separators() {
        let s = WebdavSink::new(cfg("https://dav.example.com", "")).unwrap();
        let u = s.resolve("a/b/c.bin").unwrap();
        assert_eq!(u.as_str(), "https://dav.example.com/a/b/c.bin");
    }

    #[test]
    fn auth_header_none_when_no_credentials() {
        let s = WebdavSink::new(cfg("https://dav.example.com", "")).unwrap();
        assert!(s.auth_header().is_none());
    }

    #[test]
    fn auth_header_basic_encoding() {
        let mut c = cfg("https://dav.example.com", "");
        c.username = "Aladdin".into();
        c.password = "open sesame".into();
        let s = WebdavSink::new(c).unwrap();
        assert_eq!(
            s.auth_header().unwrap(),
            "Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="
        );
    }
}
