use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::engine::gnutella::types::url_decode;
use crate::engine::options::EngineOptions;

const MAX_LINE: usize = 64 * 1024;
const FIRST_STATUS_TIMEOUT: Duration = Duration::from_secs(30);
const STATUS_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

async fn remove_transfer<W: tokio::io::AsyncWrite + Unpin>(wr: &mut W, id: Option<&str>) {
    if let Some(id) = id {
        let _ = wr
            .write_all(format!("TRANSFER REMOVE id={id}\n").as_bytes())
            .await;
    }
}

pub struct GiftLink {
    pub inner: String,
}

pub fn is_gift_uri(uri: &str) -> bool {
    uri.trim().to_ascii_lowercase().starts_with("gift://")
}

pub fn parse_gift_uri(uri: &str) -> Option<GiftLink> {
    let s = uri.trim();
    let lower = s.to_ascii_lowercase();
    if lower.starts_with("gift://") {
        Some(GiftLink {
            inner: s[7..].to_string(),
        })
    } else {
        None
    }
}

pub async fn run_gift_download(
    uri: &str,
    dir: &str,
    opts: &EngineOptions,
    total: Arc<AtomicU64>,
    completed: Arc<AtomicU64>,
    speed: Arc<AtomicU64>,
    connections: Arc<AtomicU32>,
    cancel_token: CancellationToken,
) -> Result<PathBuf, String> {
    if !is_gift_uri(uri) {
        return Err(format!("not a giFT URI: {uri}"));
    }
    let link = parse_gift_uri(uri).ok_or_else(|| "invalid gift URI".to_string())?;
    if link
        .inner
        .chars()
        .any(|c| c == '"' || c == '\n' || c == '\r' || c == '\0')
    {
        return Err("giFT inner URI contains forbidden control characters".into());
    }
    if !opts.get_bool("gift-enabled").unwrap_or(false) {
        return Err("giFT bridge is disabled in preferences".into());
    }
    let host = opts.get_str("gift-host").unwrap_or("127.0.0.1").to_string();
    let port: u16 = match opts.get_u64("gift-port") {
        Some(n) if (1..=u16::MAX as u64).contains(&n) => n as u16,
        Some(n) => {
            return Err(format!("invalid gift-port {n}: expected 1..={}", u16::MAX));
        }
        None => 1213,
    };

    let proxy = opts.p2p_proxy_connector()?;
    let stream = timeout(Duration::from_secs(5), proxy.connect_tcp(&host, port))
        .await
        .map_err(|_| "giftd connect timeout".to_string())?
        .map_err(|e| format!("giftd connect: {e}"))?;
    let (rd, mut wr) = tokio::io::split(stream);
    let mut reader = BufReader::new(rd);

    wr.write_all(b"ATTACH risuko\n")
        .await
        .map_err(|e| e.to_string())?;

    let out_dir = PathBuf::from(dir);
    tokio::fs::create_dir_all(&out_dir)
        .await
        .map_err(|e| e.to_string())?;

    let safe_name =
        crate::engine::util::safe_filename(&extract_gift_name(&link.inner), "gift-download");
    let out_path = out_dir.join(&safe_name);
    let out_path_str = out_path.display().to_string();
    if out_path_str
        .chars()
        .any(|c| matches!(c, '"' | '\n' | '\r' | '\0'))
    {
        return Err("output path contains forbidden control characters".into());
    }
    let cmd = format!(
        "TRANSFER ADD url=\"{}\" path=\"{}\"\n",
        link.inner,
        out_path.display()
    );
    wr.write_all(cmd.as_bytes())
        .await
        .map_err(|e| e.to_string())?;

    let mut transfer_id: Option<String> = None;
    let mut last_progress: Option<(tokio::time::Instant, u64)> = None;
    let mut line: Vec<u8> = Vec::new();
    let mut last_status = tokio::time::Instant::now();
    let mut seen_status = false;
    loop {
        let limit = if seen_status {
            STATUS_IDLE_TIMEOUT
        } else {
            FIRST_STATUS_TIMEOUT
        };
        let remaining = limit.saturating_sub(last_status.elapsed());
        if remaining.is_zero() {
            remove_transfer(&mut wr, transfer_id.as_deref()).await;
            return Err("giftd sent no transfer status in time".into());
        }
        // read_until keeps partial data in `line` across timeouts and cancellation
        let room = (MAX_LINE + 1).saturating_sub(line.len()) as u64;
        let read = tokio::select! {
            _ = cancel_token.cancelled() => {
                remove_transfer(&mut wr, transfer_id.as_deref()).await;
                return Err("cancelled".into());
            }
            r = timeout(remaining, async {
                (&mut reader).take(room).read_until(b'\n', &mut line).await
            }) => r,
        };
        match read {
            Ok(Ok(0)) if line.len() <= MAX_LINE => return Err("giftd disconnected".into()),
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(e.to_string()),
            Err(_) => continue,
        }
        if line.len() > MAX_LINE {
            return Err("giftd line too long".into());
        }
        if line.last() != Some(&b'\n') {
            continue;
        }
        let text = String::from_utf8_lossy(&line).into_owned();
        line.clear();
        let trimmed = text.trim();
        if trimmed.starts_with("ERROR") {
            return Err(format!("giftd: {trimmed}"));
        }
        if let Some(rest) = trimmed.strip_prefix("TRANSFER STATUS ") {
            let mut id = None;
            let mut t = 0u64;
            let mut d = 0u64;
            let mut state = String::new();
            for kv in rest.split_whitespace() {
                if let Some(v) = kv.strip_prefix("id=") {
                    id = Some(v.to_string());
                } else if let Some(v) = kv.strip_prefix("total=") {
                    t = v.parse().unwrap_or(0);
                } else if let Some(v) = kv.strip_prefix("done=") {
                    d = v.parse().unwrap_or(0);
                } else if let Some(v) = kv.strip_prefix("state=") {
                    state = v.to_string();
                }
            }
            last_status = tokio::time::Instant::now();
            seen_status = true;
            if let Some(i) = id {
                transfer_id = Some(i);
            }
            if t > 0 {
                total.store(t, Ordering::Relaxed);
            }
            completed.store(d, Ordering::Relaxed);
            let now = tokio::time::Instant::now();
            if let Some((prev_time, prev_done)) = last_progress {
                let elapsed_ms = now.duration_since(prev_time).as_millis() as u64;
                let delta = d.saturating_sub(prev_done);
                if let Some(rate) = delta.saturating_mul(1000).checked_div(elapsed_ms) {
                    speed.store(rate, Ordering::Relaxed);
                }
            }
            last_progress = Some((now, d));
            if matches!(state.as_str(), "active" | "complete") {
                connections.store(1, Ordering::Relaxed);
            }
            match state.as_str() {
                "complete" => return Ok(out_path),
                "cancelled" | "error" | "failed" => return Err(format!("giftd transfer {state}")),
                _ => {}
            }
        }
    }
}

pub fn extract_gift_name(inner: &str) -> String {
    let (path, query) = inner.split_once('?').unwrap_or((inner, ""));
    let dn = query
        .split('&')
        .find_map(|part| part.strip_prefix("dn="))
        .map(url_decode)
        .map(|n| n.trim().to_string())
        .filter(|n| !n.is_empty());
    if let Some(name) = dn {
        return name;
    }
    match path.rsplit('/').next() {
        Some(name) if !name.is_empty() => name.to_string(),
        _ => "gift-download".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    #[test]
    fn detects() {
        assert!(is_gift_uri("gift://FastTrack/sha1/ABC"));
        assert!(!is_gift_uri("gnutella://"));
    }
    #[test]
    fn parses() {
        let l = parse_gift_uri("gift://OpenFT/file?dn=foo&xl=42").unwrap();
        assert_eq!(l.inner, "OpenFT/file?dn=foo&xl=42");
    }

    async fn run_against(chunks: Vec<&'static [u8]>) -> Result<PathBuf, String> {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut b = [0u8; 1024];
            let _ = sock.read(&mut b).await;
            for c in chunks {
                sock.write_all(c).await.unwrap();
                sock.flush().await.unwrap();
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        });
        let mut cfg = serde_json::Map::new();
        cfg.insert("gift-enabled".into(), true.into());
        cfg.insert("gift-port".into(), port.into());
        let opts = EngineOptions::from_config(&cfg, &serde_json::Map::new());
        let dir = tempfile::tempdir().unwrap();
        run_gift_download(
            "gift://OpenFT/file?dn=x.bin",
            dir.path().to_str().unwrap(),
            &opts,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            CancellationToken::new(),
        )
        .await
    }

    #[tokio::test]
    async fn keeps_partial_status_line_across_reads() {
        let res = run_against(vec![
            b"TRANSFER STATUS id=1 total=10 ",
            b"done=10 state=complete\n",
        ])
        .await;
        assert!(res.unwrap().ends_with("x.bin"));
    }

    #[tokio::test]
    async fn surfaces_error_lines() {
        let err = run_against(vec![b"ERROR bad request\n"]).await.unwrap_err();
        assert!(err.contains("ERROR bad request"), "{err}");
    }

    #[tokio::test]
    async fn rejects_overlong_line() {
        let long: &'static [u8] = Box::leak(vec![b'a'; MAX_LINE + 10].into_boxed_slice());
        let err = run_against(vec![long]).await.unwrap_err();
        assert!(err.contains("too long"), "{err}");
    }
}
