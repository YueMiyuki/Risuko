mod common;

use std::path::PathBuf;
use std::time::Duration;

use risuko_bt::bencode::{encode_to_vec, Value};
use risuko_bt::session::{AddTorrent, AddTorrentOptions, AddTorrentResponse, Session};
use risuko_bt::TorrentIdOrHash;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use common::quiet_session_opts;

fn torrent_with_announce(name: &str, announce: &str, announce_list: Option<Vec<&str>>) -> Vec<u8> {
    let info = common::tiny_info(name, false);
    let mut top = vec![
        (
            b"announce".to_vec(),
            Value::Bytes(announce.as_bytes().to_vec()),
        ),
        (b"info".to_vec(), info),
    ];
    if let Some(urls) = announce_list {
        top.push((
            b"announce-list".to_vec(),
            Value::List(vec![Value::List(
                urls.into_iter()
                    .map(|url| Value::Bytes(url.as_bytes().to_vec()))
                    .collect(),
            )]),
        ));
    }
    encode_to_vec(&Value::Dict(top))
}

#[derive(Clone, Copy)]
enum Reply {
    Answer,
    Drop,
    Hang,
}

async fn fake_tracker(
    script: impl Fn(usize) -> Reply + Send + 'static,
) -> (
    String,
    mpsc::UnboundedReceiver<String>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let (line_tx, line_rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let mut held = Vec::new();
        let mut served = 0;
        while let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = [0u8; 4096];
            let n = socket.read(&mut buf).await.unwrap_or(0);
            let request = String::from_utf8_lossy(&buf[..n]);
            let _ = line_tx.send(request.lines().next().unwrap_or_default().to_string());
            let reply = script(served);
            served += 1;
            match reply {
                Reply::Answer => {
                    let body: &[u8] = b"d8:intervali1800e5:peers0:e";
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                }
                Reply::Drop => {}
                Reply::Hang => held.push(socket),
            }
        }
    });
    (url, line_rx, task)
}

async fn next_announce(lines: &mut mpsc::UnboundedReceiver<String>, what: &str) -> String {
    tokio::time::timeout(Duration::from_secs(10), lines.recv())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"))
        .unwrap_or_else(|| panic!("tracker closed before {what}"))
}

async fn add(
    session: &std::sync::Arc<Session>,
    bytes: Vec<u8>,
    opts: AddTorrentOptions,
) -> (usize, std::sync::Arc<risuko_bt::ManagedTorrent>) {
    match session
        .add_torrent(AddTorrent::TorrentFileBytes(bytes.into()), Some(opts))
        .await
        .expect("add")
    {
        AddTorrentResponse::Added(id, handle) => (id, handle),
        AddTorrentResponse::AlreadyManaged(..) => panic!("expected Added"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconfiguring_the_p2p_route_keeps_the_tracker_pollers_running() {
    let tmp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(PathBuf::from(tmp.path()), quiet_session_opts())
        .await
        .unwrap();
    let (url, mut lines, server) = fake_tracker(|_| Reply::Answer).await;
    let (id, _handle) = add(
        &session,
        torrent_with_announce("reconfigure-keeps-trackers", &url, None),
        AddTorrentOptions::default(),
    )
    .await;
    let started = next_announce(&mut lines, "started announce").await;
    assert!(started.contains("event=started"), "{started}");
    tokio::time::sleep(Duration::from_millis(200)).await;

    tokio::time::timeout(
        Duration::from_secs(15),
        session.reconfigure_p2p_proxy(None, None),
    )
    .await
    .expect("reconfigure did not finish")
    .expect("reconfigure");

    tokio::time::timeout(
        Duration::from_secs(20),
        session.delete(TorrentIdOrHash::Id(id), false),
    )
    .await
    .expect("delete did not finish")
    .expect("delete");
    let stopped = next_announce(&mut lines, "stopped announce").await;
    assert!(stopped.contains("event=stopped"), "{stopped}");
    server.abort();
    session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pausing_a_torrent_tells_its_trackers_and_resuming_starts_again() {
    let tmp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(PathBuf::from(tmp.path()), quiet_session_opts())
        .await
        .unwrap();
    let (url, mut lines, server) = fake_tracker(|_| Reply::Answer).await;
    let (_id, handle) = add(
        &session,
        torrent_with_announce("pause-stops-trackers", &url, None),
        AddTorrentOptions::default(),
    )
    .await;
    let started = next_announce(&mut lines, "started announce").await;
    assert!(started.contains("event=started"), "{started}");
    tokio::time::sleep(Duration::from_millis(200)).await;

    tokio::time::timeout(Duration::from_secs(20), session.pause(&handle))
        .await
        .expect("pause did not finish")
        .expect("pause");
    let stopped = next_announce(&mut lines, "stopped announce").await;
    assert!(stopped.contains("event=stopped"), "{stopped}");

    tokio::time::timeout(Duration::from_secs(20), session.unpause(&handle))
        .await
        .expect("unpause did not finish")
        .expect("unpause");
    let resumed = next_announce(&mut lines, "announce after resume").await;
    assert!(resumed.contains("event=started"), "{resumed}");
    server.abort();
    session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extra_trackers_do_not_replace_the_torrents_own_announce_url() {
    let tmp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(PathBuf::from(tmp.path()), quiet_session_opts())
        .await
        .unwrap();
    let (url, mut lines, server) = fake_tracker(|_| Reply::Answer).await;
    let (_id, handle) = add(
        &session,
        torrent_with_announce("extras-keep-announce", &url, None),
        AddTorrentOptions {
            trackers: Some(vec!["http://127.0.0.1:1/announce".to_string()]),
            ..Default::default()
        },
    )
    .await;
    let started = next_announce(&mut lines, "announce to the torrent's own tracker").await;
    assert!(started.contains("event=started"), "{started}");
    let announced = handle
        .metadata
        .load()
        .as_ref()
        .map(|meta| meta.announce_list.clone())
        .unwrap_or_default();
    assert_eq!(announced.len(), 2, "{announced:?}");
    assert_eq!(announced[0], [url]);
    server.abort();
    session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pausing_does_not_wait_for_a_tracker_that_stopped_answering() {
    let tmp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(PathBuf::from(tmp.path()), quiet_session_opts())
        .await
        .unwrap();
    let (url, mut lines, server) = fake_tracker(|served| {
        if served == 0 {
            Reply::Answer
        } else {
            Reply::Hang
        }
    })
    .await;
    let (_id, handle) = add(
        &session,
        torrent_with_announce("pause-ignores-hung-tracker", &url, None),
        AddTorrentOptions::default(),
    )
    .await;
    let started = next_announce(&mut lines, "started announce").await;
    assert!(started.contains("event=started"), "{started}");
    tokio::time::sleep(Duration::from_millis(200)).await;

    tokio::time::timeout(Duration::from_secs(3), session.pause(&handle))
        .await
        .expect("pause waited for the unresponsive tracker")
        .expect("pause");
    let stopped = next_announce(&mut lines, "stopped announce").await;
    assert!(stopped.contains("event=stopped"), "{stopped}");
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resuming_does_not_wait_for_a_tracker_that_stopped_answering() {
    let tmp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(PathBuf::from(tmp.path()), quiet_session_opts())
        .await
        .unwrap();
    let (url, mut lines, server) = fake_tracker(|served| {
        if served == 0 {
            Reply::Answer
        } else {
            Reply::Hang
        }
    })
    .await;
    let (_id, handle) = add(
        &session,
        torrent_with_announce("resume-ignores-hung-stop", &url, None),
        AddTorrentOptions::default(),
    )
    .await;
    let started = next_announce(&mut lines, "started announce").await;
    assert!(started.contains("event=started"), "{started}");
    tokio::time::sleep(Duration::from_millis(200)).await;

    tokio::time::timeout(Duration::from_secs(3), session.pause(&handle))
        .await
        .expect("pause waited for the unresponsive tracker")
        .expect("pause");
    let stopped = next_announce(&mut lines, "stopped announce").await;
    assert!(stopped.contains("event=stopped"), "{stopped}");
    tokio::time::timeout(Duration::from_secs(2), session.unpause(&handle))
        .await
        .expect("resume waited for the unfinished stop announce")
        .expect("unpause");
    server.abort();
    tokio::time::timeout(Duration::from_secs(20), session.shutdown())
        .await
        .expect("shutdown did not finish");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resuming_retries_a_tracker_that_is_backing_off() {
    let tmp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(PathBuf::from(tmp.path()), quiet_session_opts())
        .await
        .unwrap();
    let (url, mut lines, server) = fake_tracker(|served| {
        if served == 0 {
            Reply::Drop
        } else {
            Reply::Answer
        }
    })
    .await;
    let (_id, handle) = add(
        &session,
        torrent_with_announce("resume-retries-failed-tracker", &url, None),
        AddTorrentOptions::default(),
    )
    .await;
    let failed = next_announce(&mut lines, "announce that gets dropped").await;
    assert!(failed.contains("event=started"), "{failed}");
    tokio::time::sleep(Duration::from_millis(300)).await;

    tokio::time::timeout(Duration::from_secs(20), session.pause(&handle))
        .await
        .expect("pause did not finish")
        .expect("pause");
    tokio::time::timeout(Duration::from_secs(20), session.unpause(&handle))
        .await
        .expect("unpause did not finish")
        .expect("unpause");
    let retried = next_announce(&mut lines, "retry after resume").await;
    assert!(retried.contains("event=started"), "{retried}");
    server.abort();
    session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reconfiguring_the_p2p_route_retries_a_tracker_that_is_backing_off() {
    let tmp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(PathBuf::from(tmp.path()), quiet_session_opts())
        .await
        .unwrap();
    let (url, mut lines, server) = fake_tracker(|served| {
        if served == 0 {
            Reply::Drop
        } else {
            Reply::Answer
        }
    })
    .await;
    let (_id, _handle) = add(
        &session,
        torrent_with_announce("reconfigure-retries-failed-tracker", &url, None),
        AddTorrentOptions::default(),
    )
    .await;
    let failed = next_announce(&mut lines, "announce that gets dropped").await;
    assert!(failed.contains("event=started"), "{failed}");
    tokio::time::sleep(Duration::from_millis(300)).await;

    tokio::time::timeout(
        Duration::from_secs(15),
        session.reconfigure_p2p_proxy(None, None),
    )
    .await
    .expect("reconfigure did not finish")
    .expect("reconfigure");
    let retried = next_announce(&mut lines, "retry after reconfigure").await;
    assert!(retried.contains("event=started"), "{retried}");
    server.abort();
    session.shutdown().await;
}
