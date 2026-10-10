use std::path::PathBuf;

use risuko_bt::bencode::{encode_to_vec, Value};
use risuko_bt::session::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, ListenerOptions, Session, SessionOptions,
};
use sha1::{Digest, Sha1};

fn make_torrent_bytes(name: &str) -> Vec<u8> {
    let pieces = vec![0u8; 20];
    let info = Value::Dict(vec![
        (b"length".to_vec(), Value::Int(1024)),
        (b"name".to_vec(), Value::Bytes(name.as_bytes().to_vec())),
        (b"piece length".to_vec(), Value::Int(1024)),
        (b"pieces".to_vec(), Value::Bytes(pieces)),
    ]);
    let top = Value::Dict(vec![(b"info".to_vec(), info)]);
    encode_to_vec(&top)
}

fn make_hashed_torrent(name: &str, payload: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(payload);
    let digest: [u8; 20] = hasher.finalize().into();
    let info = Value::Dict(vec![
        (b"length".to_vec(), Value::Int(payload.len() as i64)),
        (b"name".to_vec(), Value::Bytes(name.as_bytes().to_vec())),
        (b"piece length".to_vec(), Value::Int(payload.len() as i64)),
        (b"pieces".to_vec(), Value::Bytes(digest.to_vec())),
    ]);
    encode_to_vec(&Value::Dict(vec![(b"info".to_vec(), info)]))
}

fn quiet_session_opts() -> SessionOptions {
    SessionOptions {
        disable_dht: true,
        disable_local_service_discovery: true,
        ..Default::default()
    }
}

async fn wait_until_finished(handle: &risuko_bt::ManagedTorrent) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let stats = handle.stats();
        if stats.finished {
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "timed out waiting for finished: progress={} / {}",
                stats.progress_bytes, stats.total_bytes
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_same_torrent_twice_returns_already_managed() {
    let tmp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(PathBuf::from(tmp.path()), SessionOptions::default())
        .await
        .unwrap();

    let bytes = make_torrent_bytes("dedup-test");

    let first = session
        .add_torrent(
            AddTorrent::TorrentFileBytes(bytes.clone().into()),
            Some(AddTorrentOptions::default()),
        )
        .await
        .expect("first add");
    let first_id = match first {
        AddTorrentResponse::Added(id, _) => id,
        AddTorrentResponse::AlreadyManaged(..) => panic!("expected Added"),
    };

    let second = session
        .add_torrent(
            AddTorrent::TorrentFileBytes(bytes.into()),
            Some(AddTorrentOptions::default()),
        )
        .await
        .expect("second add");
    match second {
        AddTorrentResponse::AlreadyManaged(id, handle) => {
            assert_eq!(id, first_id);
            assert_eq!(handle.id, first_id);
        }
        _ => panic!("expected AlreadyManaged on duplicate add"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_stops_every_torrent_and_clears_the_session() {
    let tmp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(PathBuf::from(tmp.path()), quiet_session_opts())
        .await
        .unwrap();
    let mut handles = Vec::new();
    for name in ["shutdown-a", "shutdown-b", "shutdown-c"] {
        let added = session
            .add_torrent(
                AddTorrent::TorrentFileBytes(make_torrent_bytes(name).into()),
                Some(AddTorrentOptions::default()),
            )
            .await
            .expect("add");
        match added {
            AddTorrentResponse::Added(_, handle) => handles.push(handle),
            AddTorrentResponse::AlreadyManaged(..) => panic!("expected Added"),
        }
    }
    assert_eq!(session.with_torrents(|iter| iter.count()), 3);

    tokio::time::timeout(std::time::Duration::from_secs(15), session.shutdown())
        .await
        .expect("shutdown did not finish");

    assert_eq!(session.with_torrents(|iter| iter.count()), 0);
    for handle in handles {
        assert!(session.pause(&handle).await.is_err());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_starts_with_listen_ipv6_enabled() {
    let tmp = tempfile::tempdir().unwrap();
    let opts = SessionOptions {
        listen: Some(ListenerOptions {
            listen_addr: None,
            enable_upnp_port_forwarding: false,
            upnp_lease: None,
            listen_ipv6: true,
        }),
        disable_local_service_discovery: true,
        ..Default::default()
    };
    let session = Session::new_with_opts(PathBuf::from(tmp.path()), opts)
        .await
        .expect("session start with listen_ipv6=true");
    drop(session);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readd_after_deleting_payload_does_not_stay_complete() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let payload = vec![7u8; 1024];
    let bytes = make_hashed_torrent("payload.bin", &payload);
    std::fs::write(dir.join("payload.bin"), &payload).unwrap();

    let session = Session::new_with_opts(dir.to_path_buf(), quiet_session_opts())
        .await
        .unwrap();

    let first = session
        .add_torrent(
            AddTorrent::TorrentFileBytes(bytes.clone().into()),
            Some(AddTorrentOptions::default()),
        )
        .await
        .expect("first add");
    let (id1, handle1) = match first {
        AddTorrentResponse::Added(id, handle) => (id, handle),
        AddTorrentResponse::AlreadyManaged(..) => panic!("expected Added"),
    };
    wait_until_finished(&handle1).await;

    std::fs::remove_file(dir.join("payload.bin")).unwrap();

    let second = session
        .add_torrent(
            AddTorrent::TorrentFileBytes(bytes.into()),
            Some(AddTorrentOptions::default()),
        )
        .await
        .expect("second add");
    match second {
        AddTorrentResponse::Added(id, handle) => {
            assert_ne!(id, id1, "stale complete torrent must be replaced");
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            assert!(
                !handle.stats().finished,
                "missing payload must not be marked complete"
            );
            assert_eq!(handle.stats().progress_bytes, 0);
        }
        AddTorrentResponse::AlreadyManaged(_, handle) => {
            panic!(
                "re-add returned stale handle finished={}",
                handle.stats().finished
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readd_of_complete_torrent_rehashes_existing_files() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let payload = vec![9u8; 1024];
    let bytes = make_hashed_torrent("payload.bin", &payload);
    std::fs::write(dir.join("payload.bin"), &payload).unwrap();

    let session = Session::new_with_opts(dir.to_path_buf(), quiet_session_opts())
        .await
        .unwrap();

    let first = session
        .add_torrent(
            AddTorrent::TorrentFileBytes(bytes.clone().into()),
            Some(AddTorrentOptions::default()),
        )
        .await
        .expect("first add");
    let id1 = match first {
        AddTorrentResponse::Added(id, handle) => {
            wait_until_finished(&handle).await;
            id
        }
        AddTorrentResponse::AlreadyManaged(..) => panic!("expected Added"),
    };

    let second = session
        .add_torrent(
            AddTorrent::TorrentFileBytes(bytes.into()),
            Some(AddTorrentOptions::default()),
        )
        .await
        .expect("second add");
    match second {
        AddTorrentResponse::Added(id, handle) => {
            assert_ne!(id, id1);
            wait_until_finished(&handle).await;
        }
        AddTorrentResponse::AlreadyManaged(_, _) => {
            panic!("complete torrent should be respawned so disk is rehashed");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn adding_first_tracker_to_trackerless_torrent_starts_polling() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tracker_addr = listener.local_addr().unwrap();
    let (seen_tx, seen_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut buf = vec![0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap_or(0);
        let request = String::from_utf8_lossy(&buf[..n]);
        let body = b"d8:intervali60e5:peers0:e";
        let header = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(header.as_bytes()).await;
        let _ = stream.write_all(body).await;
        let _ = seen_tx.send(request.contains("info_hash"));
    });

    let tmp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(tmp.path().to_path_buf(), quiet_session_opts())
        .await
        .unwrap();
    let bytes = make_torrent_bytes("no-trackers");
    let added = session
        .add_torrent(
            AddTorrent::TorrentFileBytes(bytes.into()),
            Some(AddTorrentOptions::default()),
        )
        .await
        .expect("add trackerless torrent");
    let handle = match added {
        AddTorrentResponse::Added(_, handle) => handle,
        AddTorrentResponse::AlreadyManaged(..) => panic!("expected Added"),
    };

    let url = format!("http://{tracker_addr}/announce");
    let added = session
        .add_trackers(handle.info_hash(), vec![url])
        .await
        .expect("add first tracker");
    assert_eq!(added, 1);

    let announced = tokio::time::timeout(std::time::Duration::from_secs(3), seen_rx)
        .await
        .expect("tracker poller should announce immediately")
        .expect("tracker request observed");
    assert!(announced, "announce query should include info_hash");
}
