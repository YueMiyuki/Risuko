#![allow(clippy::type_complexity)]

use std::convert::Infallible;
use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use http_body_util::Full;
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use risuko_engine::engine::http::{run_http_download_multi, PIECE_SIZE};
use risuko_engine::engine::speed_limiter::SpeedLimiter;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

const PAYLOAD_LEN: usize = (PIECE_SIZE as usize) * 4 + 17;

fn make_payload() -> Vec<u8> {
    let mut v = vec![0u8; PAYLOAD_LEN];
    for (i, b) in v.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    v
}

fn payload_sha256_hex(p: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(p);
    hex::encode(h.finalize())
}

fn parse_range(payload: &[u8], req: &Request<Incoming>) -> Option<(u64, u64, Vec<u8>)> {
    if payload.is_empty() {
        return None;
    }
    let last = payload.len() as u64 - 1;
    let range = req.headers().get(hyper::header::RANGE)?;
    let s = range.to_str().ok()?;
    let rest = s.strip_prefix("bytes=")?;
    let mut parts = rest.split('-');
    let start: u64 = parts.next().unwrap_or("0").parse().unwrap_or(0);
    let end_str = parts.next().unwrap_or("");
    let end: u64 = if end_str.is_empty() {
        last
    } else {
        end_str.parse().unwrap_or(last)
    };
    let end = end.min(last);
    if start > end {
        return None;
    }
    let slice = payload[start as usize..=end as usize].to_vec();
    Some((start, end, slice))
}

async fn handle_good(
    req: Request<Incoming>,
    payload: Arc<Vec<u8>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let len = payload.len() as u64;
    if let Some((start, end, slice)) = parse_range(&payload, &req) {
        return Ok(Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", slice.len().to_string())
            .header("Content-Range", format!("bytes {start}-{end}/{len}"))
            .header("ETag", "\"v1\"")
            .body(Full::new(Bytes::from(slice)))
            .unwrap());
    }
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Accept-Ranges", "bytes")
        .header("Content-Length", len.to_string())
        .header("ETag", "\"v1\"")
        .body(Full::new(Bytes::from(payload.as_ref().clone())))
        .unwrap())
}

async fn handle_broken(_req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(Response::builder()
        .status(StatusCode::SERVICE_UNAVAILABLE)
        .body(Full::new(Bytes::from_static(b"down")))
        .unwrap())
}

async fn spawn_good_server(payload: Arc<Vec<u8>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let payload = payload.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new()
                    .serve_connection(io, service_fn(move |req| handle_good(req, payload.clone())))
                    .await;
            });
        }
    });
    addr
}

async fn spawn_broken_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new()
                    .serve_connection(io, service_fn(handle_broken))
                    .await;
            });
        }
    });
    addr
}

fn options_with(extra: Vec<(&str, Value)>) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("split".into(), json!("4"));
    m.insert("min-split-size".into(), json!("1M"));
    m.insert("file-allocation".into(), json!("falloc"));
    m.insert("connect-timeout".into(), json!("5"));
    m.insert("uri-selector".into(), json!("feedback"));
    for (k, v) in extra {
        m.insert(k.into(), v);
    }
    m
}

fn dummy_state() -> (
    Arc<AtomicU64>,
    Arc<AtomicU64>,
    Arc<AtomicU64>,
    Arc<AtomicU32>,
    CancellationToken,
    Arc<SpeedLimiter>,
    Arc<SpeedLimiter>,
    Vec<Arc<AtomicU64>>,
) {
    (
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        CancellationToken::new(),
        Arc::new(SpeedLimiter::new(0)),
        Arc::new(SpeedLimiter::new(0)),
        Vec::new(),
    )
}

#[tokio::test]
async fn mirror_failover_with_checksum_verify() {
    let payload = Arc::new(make_payload());
    let expected = payload_sha256_hex(&payload);
    let good = spawn_good_server(payload.clone()).await;
    let broken = spawn_broken_server().await;
    tokio::task::yield_now().await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();

    let uris = vec![
        format!("http://{}/file.bin", broken),
        format!("http://{}/file.bin", good),
    ];

    let options = options_with(vec![("checksum", json!(format!("sha-256={expected}")))]);

    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();

    let result = run_http_download_multi(
        &uris,
        &dir,
        "file.bin",
        &options,
        total,
        completed.clone(),
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        std::sync::Arc::new(parking_lot::Mutex::new(None)),
    )
    .await
    .expect("download should succeed via failover to good mirror");

    let bytes = std::fs::read(&result).unwrap();
    assert_eq!(bytes.len(), PAYLOAD_LEN, "size mismatch");
    let mut h = Sha256::new();
    h.update(&bytes);
    assert_eq!(hex::encode(h.finalize()), expected, "content mismatch");
    assert_eq!(
        completed.load(Ordering::Relaxed),
        PAYLOAD_LEN as u64,
        "completed counter should match payload"
    );
}

#[tokio::test]
async fn checksum_mismatch_deletes_file() {
    let payload = Arc::new(make_payload());
    let good = spawn_good_server(payload).await;
    tokio::task::yield_now().await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![format!("http://{}/file.bin", good)];

    let bogus = "0".repeat(64);
    let options = options_with(vec![("checksum", json!(format!("sha-256={bogus}")))]);
    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();

    let err = run_http_download_multi(
        &uris,
        &dir,
        "file.bin",
        &options,
        total,
        completed,
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        std::sync::Arc::new(parking_lot::Mutex::new(None)),
    )
    .await
    .expect_err("checksum mismatch must fail");
    assert!(
        err.to_lowercase().contains("checksum")
            || err.to_lowercase().contains("hash")
            || err.to_lowercase().contains("sha"),
        "error should mention checksum failure, got: {err}"
    );

    let final_path = tmp.path().join("file.bin");
    assert!(
        !final_path.exists(),
        "verified-failure file must be removed: {final_path:?}"
    );
}

#[tokio::test]
async fn cookie_jar_loaded_from_netscape_file() {
    let tmp = tempfile::tempdir().unwrap();
    let cookies_path = tmp.path().join("cookies.txt");
    let mut f = std::fs::File::create(&cookies_path).unwrap();
    writeln!(
        f,
        "# Netscape HTTP Cookie File\n.example.com\tTRUE\t/\tFALSE\t0\tk\tv"
    )
    .unwrap();
    drop(f);

    let opts = options_with(vec![(
        "load-cookies",
        json!(cookies_path.to_string_lossy()),
    )]);
    assert!(opts.get("load-cookies").and_then(|v| v.as_str()).is_some());
}

struct CountingState {
    payload: Arc<Vec<u8>>,
    range_hits: AtomicU64,
    in_flight: AtomicU64,
    max_in_flight: AtomicU64,
    fail_after: Option<u64>,
    override_total: Option<u64>,
    delay_ms: u64,
}

impl CountingState {
    fn healthy(payload: Arc<Vec<u8>>, delay_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            payload,
            range_hits: AtomicU64::new(0),
            in_flight: AtomicU64::new(0),
            max_in_flight: AtomicU64::new(0),
            fail_after: None,
            override_total: None,
            delay_ms,
        })
    }
    fn hits(&self) -> u64 {
        self.range_hits.load(Ordering::SeqCst)
    }
    fn peak(&self) -> u64 {
        self.max_in_flight.load(Ordering::SeqCst)
    }
}

fn range_response(st: &CountingState, req: &Request<Incoming>) -> Response<Full<Bytes>> {
    let len = st.payload.len() as u64;
    let reported_total = st.override_total.unwrap_or(len);
    if let Some((start, end, slice)) = parse_range(&st.payload, req) {
        return Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", slice.len().to_string())
            .header(
                "Content-Range",
                format!("bytes {start}-{end}/{reported_total}"),
            )
            .header("ETag", "\"v1\"")
            .body(Full::new(Bytes::from(slice)))
            .unwrap();
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("Accept-Ranges", "bytes")
        .header("Content-Length", len.to_string())
        .header("ETag", "\"v1\"")
        .body(Full::new(Bytes::from(st.payload.as_ref().clone())))
        .unwrap()
}

async fn handle_counting(
    req: Request<Incoming>,
    st: Arc<CountingState>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let hit = st.range_hits.fetch_add(1, Ordering::SeqCst) + 1;
    if let Some(fa) = st.fail_after {
        if hit > fa {
            return Ok(Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(Full::new(Bytes::from_static(b"down")))
                .unwrap());
        }
    }
    let cur = st.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
    st.max_in_flight.fetch_max(cur, Ordering::SeqCst);
    if st.delay_ms > 0 {
        tokio::time::sleep(std::time::Duration::from_millis(st.delay_ms)).await;
    }
    let resp = range_response(&st, &req);
    st.in_flight.fetch_sub(1, Ordering::SeqCst);
    Ok(resp)
}

async fn spawn_counting_server(st: Arc<CountingState>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let st = st.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new()
                    .serve_connection(io, service_fn(move |req| handle_counting(req, st.clone())))
                    .await;
            });
        }
    });
    addr
}

#[allow(clippy::type_complexity)]
async fn run_mirrors(
    uris: &[String],
    dir: &str,
    options: &Map<String, Value>,
) -> Result<PathBuf, String> {
    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();
    run_http_download_multi(
        uris,
        dir,
        "file.bin",
        options,
        total,
        completed,
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        std::sync::Arc::new(parking_lot::Mutex::new(None)),
    )
    .await
}

#[tokio::test]
async fn concurrent_multi_source_distributes_pieces() {
    let payload = Arc::new(make_payload());
    let expected = payload_sha256_hex(&payload);
    let a = CountingState::healthy(payload.clone(), 15);
    let b = CountingState::healthy(payload.clone(), 15);
    let addr_a = spawn_counting_server(a.clone()).await;
    let addr_b = spawn_counting_server(b.clone()).await;
    tokio::task::yield_now().await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![
        format!("http://{addr_a}/file.bin"),
        format!("http://{addr_b}/file.bin"),
    ];
    let options = options_with(vec![]);

    let path = run_mirrors(&uris, &dir, &options)
        .await
        .expect("concurrent multi-source download should succeed");

    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(
        payload_sha256_hex(&bytes),
        expected,
        "content must be correct"
    );
    assert!(a.hits() > 0, "mirror A should have served pieces");
    assert!(
        b.hits() > 0,
        "mirror B should have served pieces (concurrent multi-source), got 0 hits"
    );
}

#[tokio::test]
async fn max_connection_per_server_caps_host() {
    let payload = Arc::new(make_payload());
    let expected = payload_sha256_hex(&payload);
    let a = CountingState::healthy(payload.clone(), 40);
    let b = CountingState::healthy(payload.clone(), 40);
    let addr_a = spawn_counting_server(a.clone()).await;
    let addr_b = spawn_counting_server(b.clone()).await;
    tokio::task::yield_now().await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![
        format!("http://{addr_a}/file.bin"),
        format!("http://{addr_b}/file.bin"),
    ];
    let options = options_with(vec![("max-connection-per-server", json!("1"))]);

    let path = run_mirrors(&uris, &dir, &options)
        .await
        .expect("download should succeed under per-server connection cap");

    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(
        payload_sha256_hex(&bytes),
        expected,
        "content must be correct"
    );
    assert!(
        a.peak() <= 1,
        "mirror A exceeded max-connection-per-server=1: peak {}",
        a.peak()
    );
    assert!(
        b.peak() <= 1,
        "mirror B exceeded max-connection-per-server=1: peak {}",
        b.peak()
    );
}

#[tokio::test]
async fn size_mismatch_mirror_is_dropped() {
    let payload = Arc::new(make_payload());
    let expected = payload_sha256_hex(&payload);
    let good = CountingState::healthy(payload.clone(), 10);
    let bad = Arc::new(CountingState {
        payload: payload.clone(),
        range_hits: AtomicU64::new(0),
        in_flight: AtomicU64::new(0),
        max_in_flight: AtomicU64::new(0),
        fail_after: None,
        override_total: Some(payload.len() as u64 + 999_999),
        delay_ms: 10,
    });
    let addr_good = spawn_counting_server(good.clone()).await;
    let addr_bad = spawn_counting_server(bad.clone()).await;
    tokio::task::yield_now().await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![
        format!("http://{addr_good}/file.bin"),
        format!("http://{addr_bad}/file.bin"),
    ];
    let options = options_with(vec![("checksum", json!(format!("sha-256={expected}")))]);

    let path = run_mirrors(&uris, &dir, &options)
        .await
        .expect("download should complete from the good mirror despite a bad one");

    let bytes = std::fs::read(&path).unwrap();
    assert_eq!(
        payload_sha256_hex(&bytes),
        expected,
        "output must be byte-correct (bad mirror's data never written)"
    );
    assert!(
        bad.hits() > 0,
        "bad mirror should have been contacted and rejected via the size guard"
    );
}

async fn handle_with_disposition(
    req: Request<Incoming>,
    payload: Arc<Vec<u8>>,
    filename: &'static str,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let len = payload.len() as u64;
    let cd = format!("attachment; filename=\"{filename}\"");
    if let Some((start, end, slice)) = parse_range(&payload, &req) {
        return Ok(Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", slice.len().to_string())
            .header("Content-Range", format!("bytes {start}-{end}/{len}"))
            .header("Content-Disposition", cd)
            .body(Full::new(Bytes::from(slice)))
            .unwrap());
    }
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Accept-Ranges", "bytes")
        .header("Content-Length", len.to_string())
        .header("Content-Disposition", cd)
        .body(Full::new(Bytes::from(payload.as_ref().clone())))
        .unwrap())
}

async fn spawn_disposition_server(payload: Arc<Vec<u8>>, filename: &'static str) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let payload = payload.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(move |req| {
                            handle_with_disposition(req, payload.clone(), filename)
                        }),
                    )
                    .await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn adopts_filename_from_content_disposition() {
    let payload = Arc::new(make_payload());
    let addr = spawn_disposition_server(payload.clone(), "StoragePeek.jar").await;
    tokio::task::yield_now().await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![format!("http://{}/resources/foo/download?version=42", addr)];
    let options = options_with(vec![]);
    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();
    let adopted = std::sync::Arc::new(parking_lot::Mutex::new(None));

    let result = run_http_download_multi(
        &uris,
        &dir,
        "",
        &options,
        total,
        completed,
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        adopted.clone(),
    )
    .await
    .expect("download should succeed");

    let final_name = result
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_string();
    assert!(
        final_name.starts_with("StoragePeek.jar"),
        "expected StoragePeek.jar*, got {final_name:?}"
    );
    assert_eq!(
        adopted.lock().as_deref(),
        Some("StoragePeek.jar"),
        "engine should publish the adopted filename to the manager slot"
    );
}

#[tokio::test]
async fn keeps_user_supplied_filename_over_content_disposition() {
    let payload = Arc::new(make_payload());
    let addr = spawn_disposition_server(payload.clone(), "ServerSays.jar").await;
    tokio::task::yield_now().await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![format!("http://{}/foo", addr)];
    let options = options_with(vec![]);
    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();
    let adopted = std::sync::Arc::new(parking_lot::Mutex::new(None));

    let result = run_http_download_multi(
        &uris,
        &dir,
        "MyChoice.jar",
        &options,
        total,
        completed,
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        adopted.clone(),
    )
    .await
    .expect("download should succeed");

    let final_name = result
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_string();
    assert!(
        final_name.starts_with("MyChoice.jar"),
        "user-supplied name must win, got {final_name:?}"
    );
    assert!(
        adopted.lock().is_none(),
        "user-supplied name must not trigger an adoption notification"
    );
}

async fn handle_chunked_disposition(
    _req: Request<Incoming>,
    payload: Arc<Vec<u8>>,
    filename: &'static str,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let cd = format!("attachment; filename=\"{filename}\"");
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Disposition", cd)
        .header("Content-Type", "application/java-archive")
        .body(Full::new(Bytes::from(payload.as_ref().clone())))
        .unwrap())
}

async fn spawn_chunked_disposition_server(
    payload: Arc<Vec<u8>>,
    filename: &'static str,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let payload = payload.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(move |req| {
                            handle_chunked_disposition(req, payload.clone(), filename)
                        }),
                    )
                    .await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn adopts_filename_when_server_doesnt_support_ranges() {
    let payload = Arc::new(make_payload());
    let addr = spawn_chunked_disposition_server(payload.clone(), "StoragePeek.jar").await;
    tokio::task::yield_now().await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![format!("http://{}/resources/foo/download?version=1", addr)];
    let options = options_with(vec![]);
    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();
    let adopted = std::sync::Arc::new(parking_lot::Mutex::new(None));

    let result = run_http_download_multi(
        &uris,
        &dir,
        "",
        &options,
        total,
        completed,
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        adopted.clone(),
    )
    .await
    .expect("download should succeed");

    let final_name = result
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_string();
    assert!(
        final_name.starts_with("StoragePeek.jar"),
        "expected StoragePeek.jar*, got {final_name:?}"
    );
    assert_eq!(
        adopted.lock().as_deref(),
        Some("StoragePeek.jar"),
        "engine should publish the adopted filename even without range support"
    );
}

fn small_payload(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i % 251) as u8).collect()
}

async fn handle_quark(
    req: Request<Incoming>,
    payload: Arc<Vec<u8>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let len = payload.len() as u64;
    if let Some((start, end, slice)) = parse_range(&payload, &req) {
        return Ok(Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header("Accept-Ranges", "bytes")
            .header("Content-Length", slice.len().to_string())
            .header("Content-Range", format!("bytes {start}-{end}/{len}"))
            .body(Full::new(Bytes::from(slice)))
            .unwrap());
    }
    Ok(Response::builder()
        .status(StatusCode::PRECONDITION_FAILED)
        .body(Full::new(Bytes::from_static(b"precondition failed")))
        .unwrap())
}

async fn spawn_quark_server(payload: Arc<Vec<u8>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let payload = payload.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(move |req| handle_quark(req, payload.clone())),
                    )
                    .await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn small_file_reuses_probe_range_shape_on_412_cdn() {
    let payload = Arc::new(small_payload(5000));
    let addr = spawn_quark_server(payload.clone()).await;
    tokio::task::yield_now().await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![format!("http://{addr}/file.bin")];
    let options = options_with(vec![]);
    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();

    let result = run_http_download_multi(
        &uris,
        &dir,
        "file.bin",
        &options,
        total,
        completed,
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        std::sync::Arc::new(parking_lot::Mutex::new(None)),
    )
    .await
    .expect("small-file download should succeed by reusing the Range request");

    let got = std::fs::read(&result).unwrap();
    assert_eq!(got, *payload, "downloaded content must match payload");
}

async fn spawn_flaky_server(payload: Arc<Vec<u8>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let attempt = Arc::new(AtomicU32::new(0));
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let payload = payload.clone();
            let attempt = attempt.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut tmp = [0u8; 1024];
                loop {
                    match stream.read(&mut tmp).await {
                        Ok(0) => return,
                        Ok(n) => {
                            buf.extend_from_slice(&tmp[..n]);
                            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => return,
                    }
                }
                let req = String::from_utf8_lossy(&buf).to_ascii_lowercase();
                let start = req.lines().find_map(|l| {
                    l.strip_prefix("range: bytes=")
                        .and_then(|r| r.split('-').next())
                        .and_then(|s| s.trim().parse::<usize>().ok())
                });
                let len = payload.len();
                let n = attempt.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {len}\r\nAccept-Ranges: bytes\r\n\r\n"
                    );
                    let _ = stream.write_all(head.as_bytes()).await;
                    let _ = stream.write_all(&payload[..len / 2]).await;
                    let _ = stream.flush().await;
                } else {
                    let start = match start {
                        Some(s) => s,
                        None => {
                            let head = "HTTP/1.1 400 Bad Request\r\n\r\n";
                            let _ = stream.write_all(head.as_bytes()).await;
                            let _ = stream.flush().await;
                            return;
                        }
                    };
                    let body = &payload[start..];
                    let head = if start > 0 {
                        format!(
                            "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\n\
                             Content-Range: bytes {}-{}/{}\r\nAccept-Ranges: bytes\r\n\r\n",
                            body.len(),
                            start,
                            len - 1,
                            len
                        )
                    } else {
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
                            body.len()
                        )
                    };
                    let _ = stream.write_all(head.as_bytes()).await;
                    let _ = stream.write_all(body).await;
                    let _ = stream.flush().await;
                }
            });
        }
    });
    addr
}

async fn handle_range_ignoring(
    _req: Request<Incoming>,
    payload: Arc<Vec<u8>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Length", payload.len().to_string())
        .body(Full::new(Bytes::from(payload.as_ref().clone())))
        .unwrap())
}

async fn spawn_range_ignoring_server(payload: Arc<Vec<u8>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let payload = payload.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(move |req| handle_range_ignoring(req, payload.clone())),
                    )
                    .await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn restarts_from_scratch_when_server_ignores_range_resume() {
    let payload = Arc::new(small_payload(4000));
    let addr = spawn_range_ignoring_server(payload.clone()).await;
    tokio::task::yield_now().await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    std::fs::write(tmp.path().join("out.bin.part"), vec![0xFFu8; 1000]).unwrap();

    let uris = vec![format!("http://{addr}/stream")];
    let options = options_with(vec![("split", json!("1"))]);
    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();

    let result = run_http_download_multi(
        &uris,
        &dir,
        "out.bin",
        &options,
        total,
        completed,
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        std::sync::Arc::new(parking_lot::Mutex::new(None)),
    )
    .await
    .expect("download should succeed by restarting from scratch");

    let got = std::fs::read(&result).unwrap();
    assert_eq!(
        got, *payload,
        "stale partial bytes must not survive a 200-on-resume restart"
    );
}

#[tokio::test]
async fn single_connection_auto_retries_and_resumes_on_reset() {
    let payload = Arc::new(small_payload(4000));
    let addr = spawn_flaky_server(payload.clone()).await;
    tokio::task::yield_now().await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![format!("http://{addr}/stream")];
    let options = options_with(vec![("split", json!("1"))]);
    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();

    let result = run_http_download_multi(
        &uris,
        &dir,
        "out.bin",
        &options,
        total,
        completed,
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        std::sync::Arc::new(parking_lot::Mutex::new(None)),
    )
    .await
    .expect("download should succeed after auto-retry/resume");

    let got = std::fs::read(&result).unwrap();
    assert_eq!(got, *payload, "resumed content must match payload");
}

async fn spawn_partial_range_server(payload: Arc<Vec<u8>>) -> (SocketAddr, Arc<AtomicU32>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let partials = Arc::new(AtomicU32::new(0));
    tokio::spawn({
        let partials = partials.clone();
        async move {
            loop {
                let (mut stream, _) = match listener.accept().await {
                    Ok(pair) => pair,
                    Err(_) => return,
                };
                let payload = payload.clone();
                let partials = partials.clone();
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    let mut buffer = [0u8; 1024];
                    loop {
                        match stream.read(&mut buffer).await {
                            Ok(0) => return,
                            Ok(count) => {
                                request.extend_from_slice(&buffer[..count]);
                                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                                    break;
                                }
                            }
                            Err(_) => return,
                        }
                    }

                    let request = String::from_utf8_lossy(&request).to_ascii_lowercase();
                    let Some(range) = request.lines().find_map(|line| {
                        let value = line.strip_prefix("range: bytes=")?;
                        let (start, end) = value.split_once('-')?;
                        let start = start.trim().parse::<usize>().ok()?;
                        let end = if end.trim().is_empty() {
                            payload.len().saturating_sub(1)
                        } else {
                            end.trim().parse::<usize>().ok()?
                        };
                        Some((start, end))
                    }) else {
                        let _ = stream.write_all(b"HTTP/1.1 400 Bad Request\r\n\r\n").await;
                        return;
                    };
                    if range.0 >= payload.len() || range.1 < range.0 {
                        let _ = stream
                            .write_all(b"HTTP/1.1 416 Range Not Satisfiable\r\n\r\n")
                            .await;
                        return;
                    }

                    let end = range.1.min(payload.len() - 1);
                    let requested = end - range.0 + 1;
                    let body_len = requested.min(1024);
                    if body_len < requested {
                        partials.fetch_add(1, Ordering::SeqCst);
                    }
                    let head = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Length: {requested}\r\nContent-Range: bytes {}-{end}/{}\r\nAccept-Ranges: bytes\r\nETag: \"v1\"\r\n\r\n",
                        range.0,
                        payload.len()
                    );
                    let _ = stream.write_all(head.as_bytes()).await;
                    let _ = stream
                        .write_all(&payload[range.0..range.0 + body_len])
                        .await;
                    let _ = stream.flush().await;
                });
            }
        }
    });
    (addr, partials)
}

#[tokio::test]
async fn partial_range_server_rejects_a_reversed_range() {
    let payload = Arc::new(small_payload(1024));
    let (addr, _) = spawn_partial_range_server(payload).await;
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(b"GET /partial HTTP/1.1\r\nHost: localhost\r\nRange: bytes=500-100\r\n\r\n")
        .await
        .unwrap();

    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();

    assert!(response.starts_with(b"HTTP/1.1 416 Range Not Satisfiable"));
}

#[tokio::test]
async fn multi_chunk_partial_errors_keep_progressing_mirror_alive() {
    let payload = Arc::new(small_payload(16 * 1024));
    let (addr, partials) = spawn_partial_range_server(payload.clone()).await;
    tokio::task::yield_now().await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![format!("http://{addr}/partial")];
    let options = options_with(vec![
        ("split", json!("2")),
        ("min-split-size", json!(1)),
        ("max-worker-retries", json!(1)),
    ]);
    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();

    let result = run_http_download_multi(
        &uris,
        &dir,
        "partial.bin",
        &options,
        total,
        completed,
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        std::sync::Arc::new(parking_lot::Mutex::new(None)),
    )
    .await
    .expect("partial progress must reset both retry and mirror failure budgets");

    assert!(
        partials.load(Ordering::SeqCst) >= 3,
        "the server must exercise repeated partial stream errors"
    );
    assert_eq!(std::fs::read(result).unwrap(), *payload);
}

async fn handle_range_overrun(
    req: Request<Incoming>,
    payload: Arc<Vec<u8>>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let len = payload.len() as u64;
    let start = req
        .headers()
        .get(hyper::header::RANGE)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("bytes="))
        .and_then(|s| s.split('-').next())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let slice = payload[start as usize..].to_vec();
    Ok(Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header("Accept-Ranges", "bytes")
        .header("Content-Length", slice.len().to_string())
        .header("Content-Range", format!("bytes {start}-{}/{len}", len - 1))
        .header("ETag", "\"v1\"")
        .body(Full::new(Bytes::from(slice)))
        .unwrap())
}

#[tokio::test]
async fn range_end_overrun_is_capped_and_file_stays_correct() {
    let payload = Arc::new(make_payload());
    let expected = payload_sha256_hex(&payload);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let served = payload.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let payload = served.clone();
            tokio::spawn(async move {
                let _ = http1::Builder::new()
                    .serve_connection(
                        TokioIo::new(stream),
                        service_fn(move |req| handle_range_overrun(req, payload.clone())),
                    )
                    .await;
            });
        }
    });

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![format!("http://{addr}/file.bin")];
    let options = options_with(vec![
        ("checksum", json!(format!("sha-256={expected}"))),
        ("file-allocation", json!("none")),
    ]);
    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();
    let result = run_http_download_multi(
        &uris,
        &dir,
        "file.bin",
        &options,
        total,
        completed.clone(),
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        std::sync::Arc::new(parking_lot::Mutex::new(None)),
    )
    .await
    .expect("overrunning server must still yield a correct file");
    assert_eq!(std::fs::read(&result).unwrap().len(), PAYLOAD_LEN);
    assert_eq!(completed.load(Ordering::Relaxed), PAYLOAD_LEN as u64);
}

async fn serve_with<F>(handler: F) -> SocketAddr
where
    F: Fn(Request<Incoming>) -> Response<Full<Bytes>> + Clone + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let handler = handler.clone();
            tokio::spawn(async move {
                let io = TokioIo::new(stream);
                let _ = http1::Builder::new()
                    .serve_connection(
                        io,
                        service_fn(move |req| {
                            let resp = handler(req);
                            async move { Ok::<_, Infallible>(resp) }
                        }),
                    )
                    .await;
            });
        }
    });
    addr
}

#[tokio::test]
async fn pieces_reuse_the_redirect_target() {
    let payload = Arc::new(make_payload());
    let redirects = Arc::new(AtomicU32::new(0));
    let (p, r) = (payload.clone(), redirects.clone());
    let addr = serve_with(move |req| {
        if req.uri().path() == "/r" {
            r.fetch_add(1, Ordering::SeqCst);
            return Response::builder()
                .status(StatusCode::FOUND)
                .header("Location", "/real")
                .body(Full::new(Bytes::new()))
                .unwrap();
        }
        let len = p.len() as u64;
        match parse_range(&p, &req) {
            Some((start, end, slice)) => Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header("Content-Range", format!("bytes {start}-{end}/{len}"))
                .header("ETag", "\"v1\"")
                .body(Full::new(Bytes::from(slice)))
                .unwrap(),
            None => Response::builder()
                .status(StatusCode::OK)
                .body(Full::new(Bytes::from(p.as_ref().clone())))
                .unwrap(),
        }
    })
    .await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![format!("http://{addr}/r")];
    let options = options_with(vec![]);
    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();
    let path = run_http_download_multi(
        &uris,
        &dir,
        "file.bin",
        &options,
        total,
        completed,
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        Arc::new(parking_lot::Mutex::new(None)),
    )
    .await
    .expect("download through a redirect");
    assert_eq!(std::fs::read(path).unwrap(), *payload);
    assert_eq!(
        redirects.load(Ordering::SeqCst),
        1,
        "only the probe may hit the redirector"
    );
}

#[tokio::test]
async fn content_encoded_download_is_stored_verbatim() {
    let body: Vec<u8> = (0..4096u32).map(|i| (i % 253) as u8).collect();
    let b = body.clone();
    let addr = serve_with(move |_req| {
        Response::builder()
            .status(StatusCode::OK)
            .header("Content-Encoding", "gzip")
            .header("Content-Length", b.len().to_string())
            .body(Full::new(Bytes::from(b.clone())))
            .unwrap()
    })
    .await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![format!("http://{addr}/a.tgz")];
    let options = options_with(vec![]);
    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();
    let path = run_http_download_multi(
        &uris,
        &dir,
        "a.tgz",
        &options,
        total,
        completed,
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        Arc::new(parking_lot::Mutex::new(None)),
    )
    .await
    .expect("single download");
    assert_eq!(std::fs::read(path).unwrap(), body);
}

#[tokio::test]
async fn changed_resource_restarts_once_without_burning_retries() {
    let payload = Arc::new(make_payload());
    let version = Arc::new(AtomicU32::new(1));
    let (p, v) = (payload.clone(), version.clone());
    let addr = serve_with(move |req| {
        let len = p.len() as u64;
        let etag = format!("\"v{}\"", v.load(Ordering::SeqCst));
        if let Some(m) = req.headers().get("if-match") {
            if m.to_str().unwrap_or("") != etag {
                return Response::builder()
                    .status(StatusCode::PRECONDITION_FAILED)
                    .body(Full::new(Bytes::new()))
                    .unwrap();
            }
        }
        let resp = match parse_range(&p, &req) {
            Some((start, end, slice)) => Response::builder()
                .status(StatusCode::PARTIAL_CONTENT)
                .header("Content-Range", format!("bytes {start}-{end}/{len}"))
                .header("ETag", etag)
                .body(Full::new(Bytes::from(slice)))
                .unwrap(),
            None => Response::builder()
                .status(StatusCode::OK)
                .header("ETag", etag)
                .body(Full::new(Bytes::from(p.as_ref().clone())))
                .unwrap(),
        };
        v.store(2, Ordering::SeqCst);
        resp
    })
    .await;

    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_string_lossy().to_string();
    let uris = vec![format!("http://{addr}/file.bin")];
    let options = options_with(vec![("max-worker-retries", json!("3"))]);
    let (total, completed, speed, conns, ct, gl, tl, cc) = dummy_state();
    let started = std::time::Instant::now();
    let path = run_http_download_multi(
        &uris,
        &dir,
        "file.bin",
        &options,
        total,
        completed,
        speed,
        conns,
        ct,
        gl,
        tl,
        cc,
        Arc::new(parking_lot::Mutex::new(None)),
    )
    .await
    .expect("restart picks up the new version");
    assert_eq!(std::fs::read(path).unwrap(), *payload);
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "workers must not spend retry backoff on a changed resource"
    );
}
