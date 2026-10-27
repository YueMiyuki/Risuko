mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use risuko_bt::bencode::{encode_to_vec, Value};
use risuko_bt::core::metainfo::parse_torrent;
use risuko_bt::magnet::resolve_with_peers;
use risuko_bt::peer::EncryptionPolicy;
use risuko_bt::session::{AddTorrentOptions, AddTorrentResponse, Session};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use common::loopback_session;

fn torrent_bytes(name: &str, private: bool) -> Vec<u8> {
    encode_to_vec(&Value::Dict(vec![(
        b"info".to_vec(),
        common::tiny_info(name, private),
    )]))
}

async fn tracker_returning(peer: SocketAddr) -> (String, tokio::task::JoinHandle<()>) {
    let SocketAddr::V4(peer) = peer else {
        panic!("loopback seed is ipv4")
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let mut body = b"d8:intervali600e5:peers6:".to_vec();
    body.extend_from_slice(&peer.ip().octets());
    body.extend_from_slice(&peer.port().to_be_bytes());
    body.push(b'e');
    let task = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let body = body.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&body).await;
            });
        }
    });
    (url, task)
}

struct Seed {
    _dir: tempfile::TempDir,
    session: Arc<Session>,
    addr: SocketAddr,
    magnet: String,
}

async fn metadata_seed(name: &str) -> Seed {
    seed_with_privacy(name, false).await
}

async fn seed_with_privacy(name: &str, private: bool) -> Seed {
    let dir = tempfile::tempdir().unwrap();
    let session = loopback_session(dir.path(), false).await;
    let meta = parse_torrent(&torrent_bytes(name, private)).unwrap();
    let magnet = format!("magnet:?xt=urn:btih:{}", meta.info_hash.to_hex());
    session
        .add_from_meta(meta, AddTorrentOptions::default())
        .await
        .expect("seed add");
    let addr = format!("127.0.0.1:{}", session.listen_port())
        .parse()
        .unwrap();
    Seed {
        _dir: dir,
        session,
        addr,
        magnet,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extra_trackers_resolve_peers_but_stay_out_of_the_resolved_trackers() {
    let seed = metadata_seed("extras-stay-out").await;
    let (tracker, server) = tracker_returning(seed.addr).await;

    let resolved = tokio::time::timeout(
        Duration::from_secs(30),
        resolve_with_peers(
            &seed.magnet,
            &[tracker],
            &[],
            Duration::from_secs(20),
            EncryptionPolicy::PlaintextOnly,
        ),
    )
    .await
    .expect("resolve did not finish")
    .expect("magnet resolves through the extra tracker's peer");

    assert!(resolved.trackers.is_empty(), "{:?}", resolved.trackers);
    assert!(
        resolved.tracker_peers.is_empty(),
        "{:?}",
        resolved.tracker_peers
    );
    assert!(resolved.peers.contains(&seed.addr));
    server.abort();
    seed.session.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trackers_named_by_the_magnet_stay_in_the_resolved_trackers() {
    let seed = metadata_seed("own-trackers-stay").await;
    let (tracker, server) = tracker_returning(seed.addr).await;
    let magnet = format!(
        "{}&tr={}",
        seed.magnet,
        percent_encoding::utf8_percent_encode(&tracker, percent_encoding::NON_ALPHANUMERIC)
    );

    let resolved = tokio::time::timeout(
        Duration::from_secs(30),
        resolve_with_peers(
            &magnet,
            &[],
            &[],
            Duration::from_secs(20),
            EncryptionPolicy::PlaintextOnly,
        ),
    )
    .await
    .expect("resolve did not finish")
    .expect("magnet resolves through its own tracker's peer");

    assert_eq!(resolved.trackers, [tracker]);
    assert_eq!(resolved.tracker_peers, [seed.addr]);
    server.abort();
    seed.session.shutdown().await;
}

async fn add_trackers_ack(private: bool, name: &str) -> usize {
    let dir = tempfile::tempdir().unwrap();
    let session = loopback_session(dir.path(), false).await;
    let meta = parse_torrent(&torrent_bytes(name, private)).unwrap();
    let info_hash = meta.info_hash;
    match session
        .add_from_meta(meta, AddTorrentOptions::default())
        .await
        .expect("add")
    {
        AddTorrentResponse::Added(..) => {}
        AddTorrentResponse::AlreadyManaged(..) => panic!("expected Added"),
    }
    let added = tokio::time::timeout(
        Duration::from_secs(10),
        session.add_trackers(info_hash, vec!["udp://203.0.113.9:1/announce".to_string()]),
    )
    .await
    .expect("add_trackers did not answer")
    .expect("add_trackers");
    session.shutdown().await;
    added
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_torrents_refuse_live_added_trackers() {
    assert_eq!(add_trackers_ack(true, "private-refuses-trackers").await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_torrents_accept_live_added_trackers() {
    assert_eq!(add_trackers_ack(false, "public-accepts-trackers").await, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn private_seeder_serves_a_leecher_its_tracker_has_not_listed_yet() {
    use sha1::{Digest, Sha1};

    let payload = vec![0x5Au8; 1024];
    let info = Value::Dict(vec![
        (b"length".to_vec(), Value::Int(1024)),
        (
            b"name".to_vec(),
            Value::Bytes(b"private-inbound.bin".to_vec()),
        ),
        (b"piece length".to_vec(), Value::Int(1024)),
        (
            b"pieces".to_vec(),
            Value::Bytes(Sha1::digest(&payload).to_vec()),
        ),
        (b"private".to_vec(), Value::Int(1)),
    ]);
    let bytes = encode_to_vec(&Value::Dict(vec![(b"info".to_vec(), info)]));
    let meta = parse_torrent(&bytes).unwrap();
    assert!(meta.info.private);

    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    std::fs::write(seed_dir.path().join("private-inbound.bin"), &payload).unwrap();
    let seed = loopback_session(seed_dir.path(), false).await;
    let leech = loopback_session(leech_dir.path(), false).await;
    seed.add_from_meta(meta.clone(), AddTorrentOptions::default())
        .await
        .expect("seed add");
    let seed_addr: SocketAddr = format!("127.0.0.1:{}", seed.listen_port()).parse().unwrap();
    let leech_handle = match leech
        .add_from_meta(
            meta,
            AddTorrentOptions {
                initial_tracker_peers: vec![seed_addr],
                ..Default::default()
            },
        )
        .await
        .expect("leech add")
    {
        AddTorrentResponse::Added(_, handle) => handle,
        AddTorrentResponse::AlreadyManaged(..) => panic!("expected Added"),
    };

    tokio::time::timeout(Duration::from_secs(20), async {
        while !leech_handle.stats().finished {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the private seeder refused an inbound peer it had not been told about");
    assert_eq!(
        std::fs::read(leech_dir.path().join("private-inbound.bin")).unwrap(),
        payload
    );
    seed.shutdown().await;
    leech.shutdown().await;
}

async fn resolved_web_seeds(private: bool, name: &str) -> Vec<String> {
    let seed = seed_with_privacy(name, private).await;
    let magnet = format!(
        "{}&ws=http%3A%2F%2Fmirror.example%2Fpayload.bin&ws=ftp%3A%2F%2Fmirror.example%2Fpayload.bin",
        seed.magnet
    );
    let resolved = tokio::time::timeout(
        Duration::from_secs(30),
        resolve_with_peers(
            &magnet,
            &[],
            &[seed.addr],
            Duration::from_secs(20),
            EncryptionPolicy::PlaintextOnly,
        ),
    )
    .await
    .expect("resolve did not finish")
    .expect("magnet resolves");
    let meta = parse_torrent(&risuko_bt::magnet::synth_torrent_bytes(
        &resolved.info_bytes,
        &resolved.trackers,
        &resolved.web_seeds,
        &resolved.piece_layers,
    ))
    .unwrap();
    assert_eq!(meta.info.private, private);
    seed.session.shutdown().await;
    meta.url_list
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn magnet_web_seeds_reach_the_synthesized_torrent_of_a_public_swarm() {
    assert_eq!(
        resolved_web_seeds(false, "public-ws").await,
        ["http://mirror.example/payload.bin"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn magnet_web_seeds_are_dropped_for_private_torrents() {
    assert!(resolved_web_seeds(true, "private-ws").await.is_empty());
}
