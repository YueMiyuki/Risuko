use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use sha1::{Digest, Sha1};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use super::types::GnutellaError;

pub async fn fetch_by_urn(
    host: &str,
    port: u16,
    n2r_path: &str,
    urn: &str,
    file_size: u64,
    out_path: &Path,
    completed: Arc<AtomicU64>,
    cancel: CancellationToken,
) -> Result<(), GnutellaError> {
    fetch_by_urn_with_proxy(
        host,
        port,
        n2r_path,
        urn,
        file_size,
        out_path,
        completed,
        cancel,
        risuko_http::ProxyConnector::direct(),
    )
    .await
}

pub async fn fetch_by_urn_with_proxy(
    host: &str,
    port: u16,
    n2r_path: &str,
    urn: &str,
    file_size: u64,
    out_path: &Path,
    completed: Arc<AtomicU64>,
    cancel: CancellationToken,
    proxy: risuko_http::ProxyConnector,
) -> Result<(), GnutellaError> {
    let part_path = part_path_for(out_path);
    let existing = match tokio::fs::metadata(&part_path).await {
        Ok(m) if m.is_file() && m.len() > 0 && m.len() < file_size => m.len(),
        _ => 0,
    };
    let stream = timeout(Duration::from_secs(15), proxy.connect_tcp(host, port))
        .await
        .map_err(|_| GnutellaError::Network("peer connect timeout".into()))?
        .map_err(|error| GnutellaError::Network(format!("peer connect: {error}")))?;
    let (rd, mut wr) = tokio::io::split(stream);
    let range = if existing > 0 {
        format!("Range: bytes={existing}-\r\n")
    } else {
        String::new()
    };
    let host_hdr = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    let req = format!(
        "GET {}?{} HTTP/1.1\r\nHost: {}:{}\r\nUser-Agent: Risuko/0.1\r\nX-Gnutella-Content-URN: {}\r\n{}Accept: */*\r\nConnection: close\r\n\r\n",
        n2r_path, urn, host_hdr, port, urn, range
    );
    wr.write_all(req.as_bytes()).await?;

    let mut reader = BufReader::new(rd);
    let mut header_buf = Vec::new();
    timeout(Duration::from_secs(30), async {
        loop {
            let start = header_buf.len();
            let room = (MAX_HEAD + 1).saturating_sub(start) as u64;
            let n = (&mut reader)
                .take(room)
                .read_until(b'\n', &mut header_buf)
                .await?;
            if header_buf.len() > MAX_HEAD {
                return Err(GnutellaError::Network("header too large".into()));
            }
            if n == 0 {
                return Err(GnutellaError::Network("eof in headers".into()));
            }
            if start > 0 && matches!(&header_buf[start..], b"\n" | b"\r\n") {
                return Ok::<(), GnutellaError>(());
            }
        }
    })
    .await
    .map_err(|_| GnutellaError::Network("header read timeout".into()))??;
    let head = parse_head(&String::from_utf8_lossy(&header_buf))?;
    let offset = plan_body(&head, existing, file_size)?;

    if let Some(parent) = out_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let file = if offset > 0 {
        tokio::fs::OpenOptions::new()
            .append(true)
            .open(&part_path)
            .await?
    } else {
        tokio::fs::File::create(&part_path).await?
    };
    let mut f = BufWriter::with_capacity(256 * 1024, file);
    completed.store(offset, Ordering::Relaxed);
    let copied = copy_body(&mut reader, &mut f, file_size - offset, &completed, &cancel).await;
    let flushed = f.flush().await;
    copied?;
    flushed?;
    drop(f);

    if let Some(expected) = expected_sha1(urn) {
        if hash_file(&part_path, &cancel).await?.as_slice() != expected {
            let _ = tokio::fs::remove_file(&part_path).await;
            return Err(GnutellaError::Network("sha1 mismatch".into()));
        }
    }
    tokio::fs::rename(&part_path, out_path).await?;
    Ok(())
}

const MAX_HEAD: usize = 32 * 1024;

pub(crate) fn part_path_for(out_path: &Path) -> PathBuf {
    let mut name = out_path.as_os_str().to_owned();
    name.push(".part");
    PathBuf::from(name)
}

async fn copy_body<R, W>(
    reader: &mut R,
    f: &mut W,
    len: u64,
    completed: &AtomicU64,
    cancel: &CancellationToken,
) -> Result<(), GnutellaError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buf = vec![0u8; 64 * 1024];
    let mut remaining = len;
    while remaining > 0 {
        let take = buf.len().min(remaining as usize);
        let n = tokio::select! {
            read_res = timeout(Duration::from_secs(30), reader.read(&mut buf[..take])) => {
                match read_res {
                    Ok(Ok(n)) => n,
                    Ok(Err(e)) => return Err(GnutellaError::Io(e)),
                    Err(_) => return Err(GnutellaError::Network("read timeout".into())),
                }
            }
            _ = cancel.cancelled() => {
                return Err(GnutellaError::Network("cancelled".into()));
            }
        };
        if n == 0 {
            return Err(GnutellaError::Network(
                "unexpected EOF while reading file".into(),
            ));
        }
        f.write_all(&buf[..n]).await?;
        remaining -= n as u64;
        completed.fetch_add(n as u64, Ordering::Relaxed);
    }
    let mut eof_probe = [0u8; 1];
    let probe = timeout(Duration::from_secs(30), reader.read(&mut eof_probe))
        .await
        .map_err(|_| GnutellaError::Network("final read timeout".into()))??;
    if probe > 0 {
        return Err(GnutellaError::Network(
            "unexpected extra bytes after expected content length".into(),
        ));
    }
    Ok(())
}

struct ResponseHead {
    status_line: String,
    code: Option<u16>,
    content_length: Option<u64>,
    content_range: Option<(u64, u64, Option<u64>)>,
    chunked: bool,
}

fn parse_head(head: &str) -> Result<ResponseHead, GnutellaError> {
    let mut lines = head.lines();
    let status_line = lines.next().unwrap_or("").to_string();
    let code = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|t| t.parse().ok());
    if !matches!(code, Some(200) | Some(206)) {
        return Err(GnutellaError::Network(format!("HTTP: {status_line}")));
    }
    let mut out = ResponseHead {
        status_line,
        code,
        content_length: None,
        content_range: None,
        chunked: false,
    };
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => out.content_length = value.parse().ok(),
            "content-range" => out.content_range = parse_content_range(value),
            "transfer-encoding" => {
                out.chunked = value.to_ascii_lowercase().contains("chunked");
            }
            _ => {}
        }
    }
    Ok(out)
}

fn parse_content_range(value: &str) -> Option<(u64, u64, Option<u64>)> {
    let spec = value.trim().strip_prefix("bytes")?.trim_start();
    let (range, total) = spec.split_once('/')?;
    let (first, last) = range.split_once('-')?;
    Some((
        first.trim().parse().ok()?,
        last.trim().parse().ok()?,
        total.trim().parse().ok(),
    ))
}

fn plan_body(head: &ResponseHead, requested: u64, file_size: u64) -> Result<u64, GnutellaError> {
    if head.chunked {
        return Err(GnutellaError::Network(
            "chunked transfer-encoding is not supported".into(),
        ));
    }
    let offset = if head.code == Some(206) {
        let (first, last, total) = head
            .content_range
            .ok_or_else(|| GnutellaError::Network("206 without content-range".into()))?;
        if requested == 0 || first != requested || last < first {
            return Err(GnutellaError::Network(format!(
                "unexpected content-range: {first}-{last}"
            )));
        }
        if total.is_some_and(|t| t != file_size) || last + 1 != file_size {
            return Err(GnutellaError::Network(format!(
                "content-range mismatch: expected size {file_size}, got {first}-{last}/{total:?}"
            )));
        }
        first
    } else {
        0
    };
    let expected = file_size - offset;
    if head.content_length.is_some_and(|l| l != expected) {
        return Err(GnutellaError::Network(format!(
            "content-length mismatch: expected {expected}, got {} ({})",
            head.content_length.unwrap_or(0),
            head.status_line
        )));
    }
    Ok(offset)
}

fn expected_sha1(urn: &str) -> Option<[u8; 20]> {
    let lower = urn.trim().to_ascii_lowercase();
    let rest = lower
        .strip_prefix("urn:sha1:")
        .or_else(|| lower.strip_prefix("urn:bitprint:"))?;
    let b32 = rest.split('.').next()?;
    if b32.len() != 32 {
        return None;
    }
    let mut out = [0u8; 20];
    let (mut acc, mut bits, mut idx) = (0u32, 0u32, 0usize);
    for b in b32.bytes() {
        let v = match b {
            b'a'..=b'z' => b - b'a',
            b'2'..=b'7' => b - b'2' + 26,
            _ => return None,
        };
        acc = (acc << 5) | v as u32;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out[idx] = (acc >> bits) as u8;
            idx += 1;
            acc &= (1 << bits) - 1;
        }
    }
    Some(out)
}

async fn hash_file(path: &Path, cancel: &CancellationToken) -> Result<[u8; 20], GnutellaError> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha1::new();
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        if cancel.is_cancelled() {
            return Err(GnutellaError::Network("cancelled".into()));
        }
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    fn b32(bytes: &[u8]) -> String {
        const A: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
        let (mut acc, mut bits, mut out) = (0u32, 0u32, String::new());
        for &b in bytes {
            acc = (acc << 8) | b as u32;
            bits += 8;
            while bits >= 5 {
                bits -= 5;
                out.push(A[((acc >> bits) & 31) as usize] as char);
            }
        }
        out
    }

    fn urn_of(data: &[u8]) -> String {
        format!("urn:sha1:{}", b32(&Sha1::digest(data)))
    }

    async fn serve(response: Vec<u8>) -> (u16, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let task = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut req = Vec::new();
            let mut b = [0u8; 1024];
            while !req.ends_with(b"\r\n\r\n") {
                let n = sock.read(&mut b).await.unwrap();
                if n == 0 {
                    break;
                }
                req.extend_from_slice(&b[..n]);
            }
            sock.write_all(&response).await.unwrap();
            sock.shutdown().await.ok();
            String::from_utf8_lossy(&req).into_owned()
        });
        (port, task)
    }

    async fn fetch(port: u16, urn: &str, size: u64, out: &Path) -> Result<(), GnutellaError> {
        fetch_by_urn(
            "127.0.0.1",
            port,
            "/uri-res/N2R",
            urn,
            size,
            out,
            Arc::new(AtomicU64::new(0)),
            CancellationToken::new(),
        )
        .await
    }

    #[test]
    fn base32_sha1_round_trip() {
        let digest = Sha1::digest(b"hello");
        let urn = format!("urn:bitprint:{}.TIGERPART", b32(&digest));
        assert_eq!(expected_sha1(&urn).unwrap().as_slice(), digest.as_slice());
        assert!(expected_sha1("urn:sha1:SHORT").is_none());
    }

    #[test]
    fn content_range_parsing() {
        assert_eq!(parse_content_range("bytes 5-9/10"), Some((5, 9, Some(10))));
        assert_eq!(parse_content_range("bytes 5-9/*"), Some((5, 9, None)));
        assert_eq!(parse_content_range("items 5-9/10"), None);
    }

    #[tokio::test]
    async fn accepts_lf_only_headers_and_verifies_hash() {
        let data = b"gnutella payload".to_vec();
        let mut resp = format!("HTTP/1.1 200 OK\nContent-Length: {}\n\n", data.len()).into_bytes();
        resp.extend_from_slice(&data);
        let (port, srv) = serve(resp).await;
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("f.bin");
        fetch(port, &urn_of(&data), data.len() as u64, &out)
            .await
            .unwrap();
        srv.await.unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), data);
        assert!(!part_path_for(&out).exists());
    }

    #[tokio::test]
    async fn rejects_short_length_and_hash_mismatch() {
        let data = b"0123456789".to_vec();
        let mut resp = b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\n012345678".to_vec();
        let (port, _srv) = serve(std::mem::take(&mut resp)).await;
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("f.bin");
        let err = fetch(port, &urn_of(&data), 10, &out).await.unwrap_err();
        assert!(err.to_string().contains("content-length mismatch"), "{err}");
        assert!(!out.exists());

        let mut resp = b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n".to_vec();
        resp.extend_from_slice(b"9876543210");
        let (port, _srv) = serve(resp).await;
        let err = fetch(port, &urn_of(&data), 10, &out).await.unwrap_err();
        assert!(err.to_string().contains("sha1 mismatch"), "{err}");
        assert!(!out.exists() && !part_path_for(&out).exists());
    }

    #[tokio::test]
    async fn resumes_from_part_file_with_range() {
        let data = b"0123456789abcdef".to_vec();
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("f.bin");
        std::fs::write(part_path_for(&out), &data[..6]).unwrap();
        let mut resp = b"HTTP/1.1 206 Partial Content\r\nContent-Length: 10\r\nContent-Range: bytes 6-15/16\r\n\r\n".to_vec();
        resp.extend_from_slice(&data[6..]);
        let (port, srv) = serve(resp).await;
        let completed = Arc::new(AtomicU64::new(0));
        fetch_by_urn(
            "127.0.0.1",
            port,
            "/uri-res/N2R",
            &urn_of(&data),
            16,
            &out,
            completed.clone(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert!(srv.await.unwrap().contains("Range: bytes=6-"));
        assert_eq!(std::fs::read(&out).unwrap(), data);
        assert_eq!(completed.load(Ordering::Relaxed), 16);
    }

    #[tokio::test]
    async fn restarts_when_peer_ignores_range() {
        let data = b"0123456789abcdef".to_vec();
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("f.bin");
        std::fs::write(part_path_for(&out), b"stale!").unwrap();
        let mut resp = b"HTTP/1.1 200 OK\r\nContent-Length: 16\r\n\r\n".to_vec();
        resp.extend_from_slice(&data);
        let (port, _srv) = serve(resp).await;
        fetch(port, &urn_of(&data), 16, &out).await.unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), data);
    }

    #[tokio::test]
    async fn reads_close_delimited_body() {
        let data = b"no length header".to_vec();
        let mut resp = b"HTTP/1.1 200 OK\r\n\r\n".to_vec();
        resp.extend_from_slice(&data);
        let (port, _srv) = serve(resp).await;
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("f.bin");
        fetch(port, &urn_of(&data), data.len() as u64, &out)
            .await
            .unwrap();
        assert_eq!(std::fs::read(&out).unwrap(), data);
    }
}
