use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use risuko_bt::core::hash::Id20;
use risuko_bt::core::metainfo::{TorrentMeta, TorrentMetaInfo, ValidatedTorrentMetaV1Info};
use risuko_bt::limiter::RateLimiter;
use risuko_bt::session::{AddTorrentOptions, AddTorrentResponse, Session, SessionOptions};
use sha1::{Digest, Sha1};

const PIECE_LEN: u32 = 64 * 1024;
const TOTAL: u64 = 256 * 1024;

fn make_payload() -> Vec<u8> {
    let mut out = Vec::with_capacity(TOTAL as usize);
    let mut x: u32 = 0xDEADBEEF;
    for _ in 0..TOTAL {
        x = x.wrapping_mul(1664525).wrapping_add(1013904223);
        out.push((x >> 24) as u8);
    }
    out
}

fn build_meta(payload: &[u8]) -> TorrentMeta {
    let pieces: Vec<u8> = payload
        .chunks(PIECE_LEN as usize)
        .flat_map(|c| {
            let mut h = Sha1::new();
            h.update(c);
            let d: [u8; 20] = h.finalize().into();
            d.into_iter()
        })
        .collect();
    let info = ValidatedTorrentMetaV1Info {
        name: "payload.bin".to_string(),
        piece_length: PIECE_LEN,
        pieces,
        private: false,
        files: vec![TorrentMetaInfo {
            path: vec!["payload.bin".to_string()],
            length: payload.len() as u64,
            padding: false,
        }],
        single_file_mode: true,
    };
    let mut h = Sha1::new();
    h.update(&info.name);
    h.update((info.files[0].length as u64).to_be_bytes());
    h.update(&info.pieces);
    let ih: [u8; 20] = h.finalize().into();
    TorrentMeta {
        info,
        announce: None,
        announce_list: Vec::new(),
        obfuscate_announce_list: Vec::new(),
        url_list: Vec::new(),
        bootstrap_nodes: Vec::new(),
        bootstrap_hosts: Vec::new(),
        comment: None,
        creation_date: None,
        info_hash: Id20::new(ih),
        info_v2: None,
        info_hash_v2: None,
        meta_version: risuko_bt::core::MetaVersion::V1,
        piece_layers: std::collections::BTreeMap::new(),
        info_bytes: Vec::new(),
    }
}

fn write_payload(dir: &std::path::Path, name: &str, payload: &[u8]) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join(name), payload).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leecher_downloads_from_seeder() {
    let _ = env_logger::builder().is_test(true).try_init();

    let payload = make_payload();
    let meta = build_meta(&payload);

    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();

    write_payload(seed_dir.path(), &meta.info.name, &payload);

    let seed = Session::new_with_opts(
        seed_dir.path().to_path_buf(),
        SessionOptions {
            disable_dht: true,
            listen: Some(risuko_bt::session::ListenerOptions {
                listen_addr: Some("127.0.0.1:0".parse().unwrap()),
                enable_upnp_port_forwarding: false,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let leech = Session::new_with_opts(
        leech_dir.path().to_path_buf(),
        SessionOptions {
            disable_dht: true,
            listen: Some(risuko_bt::session::ListenerOptions {
                listen_addr: Some("127.0.0.1:0".parse().unwrap()),
                enable_upnp_port_forwarding: false,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let _seed_handle = match seed
        .add_from_meta(meta.clone(), AddTorrentOptions::default())
        .await
        .expect("seed add")
    {
        AddTorrentResponse::Added(_, h) => h,
        _ => panic!("expected Added"),
    };

    let leech_handle = match leech
        .add_from_meta(meta.clone(), AddTorrentOptions::default())
        .await
        .expect("leech add")
    {
        AddTorrentResponse::Added(_, h) => h,
        _ => panic!("expected Added"),
    };

    let seed_addr: SocketAddr = format!("127.0.0.1:{}", seed.listen_port()).parse().unwrap();
    leech
        .add_peer(meta.info_hash, seed_addr)
        .await
        .expect("add peer");

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let s = leech_handle.stats();
        if s.finished {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "timed out: progress={} / {} bytes (peers={})",
                s.progress_bytes,
                s.total_bytes,
                s.live.map(|l| l.snapshot.peer_stats.live).unwrap_or(0),
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let got = std::fs::read(leech_dir.path().join(&meta.info.name)).unwrap();
    assert_eq!(got.len(), payload.len(), "length mismatch");
    assert_eq!(got, payload, "payload mismatch");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pause_if_halted_leaves_a_healthy_torrent_running() {
    let _ = env_logger::builder().is_test(true).try_init();

    let payload = make_payload();
    let meta = build_meta(&payload);

    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();

    write_payload(seed_dir.path(), &meta.info.name, &payload);

    let seed = Session::new_with_opts(
        seed_dir.path().to_path_buf(),
        SessionOptions {
            disable_dht: true,
            listen: Some(risuko_bt::session::ListenerOptions {
                listen_addr: Some("127.0.0.1:0".parse().unwrap()),
                enable_upnp_port_forwarding: false,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let leech = Session::new_with_opts(
        leech_dir.path().to_path_buf(),
        SessionOptions {
            disable_dht: true,
            listen: Some(risuko_bt::session::ListenerOptions {
                listen_addr: Some("127.0.0.1:0".parse().unwrap()),
                enable_upnp_port_forwarding: false,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let _seed_handle = match seed
        .add_from_meta(meta.clone(), AddTorrentOptions::default())
        .await
        .expect("seed add")
    {
        AddTorrentResponse::Added(_, h) => h,
        _ => panic!("expected Added"),
    };

    let leech_handle = match leech
        .add_from_meta(meta.clone(), AddTorrentOptions::default())
        .await
        .expect("leech add")
    {
        AddTorrentResponse::Added(_, h) => h,
        _ => panic!("expected Added"),
    };

    leech
        .pause_if_halted(&leech_handle)
        .await
        .expect("pause if halted");

    let seed_addr: SocketAddr = format!("127.0.0.1:{}", seed.listen_port()).parse().unwrap();
    leech
        .add_peer(meta.info_hash, seed_addr)
        .await
        .expect("add peer");

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let s = leech_handle.stats();
        if s.finished {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "timed out: progress={} / {} bytes (peers={})",
                s.progress_bytes,
                s.total_bytes,
                s.live.map(|l| l.snapshot.peer_stats.live).unwrap_or(0),
            );
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let got = std::fs::read(leech_dir.path().join(&meta.info.name)).unwrap();
    assert_eq!(got.len(), payload.len(), "length mismatch");
    assert_eq!(got, payload, "payload mismatch");
}

async fn loopback_session(dir: &std::path::Path) -> std::sync::Arc<Session> {
    Session::new_with_opts(
        dir.to_path_buf(),
        SessionOptions {
            disable_dht: true,
            listen: Some(risuko_bt::session::ListenerOptions {
                listen_addr: Some("127.0.0.1:0".parse().unwrap()),
                enable_upnp_port_forwarding: false,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap()
}

async fn timed_transfer(
    seed_opts: SessionOptions,
    seed_add: AddTorrentOptions,
    leech_add: AddTorrentOptions,
    tweak: impl FnOnce(&Session, &Session, &risuko_bt::ManagedTorrent),
) -> Duration {
    let payload = make_payload();
    let meta = build_meta(&payload);
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    write_payload(seed_dir.path(), &meta.info.name, &payload);

    let seed = Session::new_with_opts(
        seed_dir.path().to_path_buf(),
        SessionOptions {
            disable_dht: true,
            listen: Some(risuko_bt::session::ListenerOptions {
                listen_addr: Some("127.0.0.1:0".parse().unwrap()),
                enable_upnp_port_forwarding: false,
                ..Default::default()
            }),
            ..seed_opts
        },
    )
    .await
    .unwrap();
    let leech = loopback_session(leech_dir.path()).await;
    match seed.add_from_meta(meta.clone(), seed_add).await.unwrap() {
        AddTorrentResponse::Added(..) => {}
        _ => panic!("expected Added"),
    }
    let leech_handle = match leech.add_from_meta(meta.clone(), leech_add).await.unwrap() {
        AddTorrentResponse::Added(_, h) => h,
        _ => panic!("expected Added"),
    };
    let seed_addr: SocketAddr = format!("127.0.0.1:{}", seed.listen_port()).parse().unwrap();
    let started = std::time::Instant::now();
    leech.add_peer(meta.info_hash, seed_addr).await.unwrap();

    let mut tweak = Some(tweak);
    let deadline = started + Duration::from_secs(40);
    while !leech_handle.stats().finished {
        assert!(std::time::Instant::now() < deadline, "transfer timed out");
        if started.elapsed() >= Duration::from_secs(1) {
            if let Some(f) = tweak.take() {
                f(&seed, &leech, &leech_handle);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let elapsed = started.elapsed();
    let got = std::fs::read(leech_dir.path().join(&meta.info.name)).unwrap();
    assert_eq!(got, payload, "payload mismatch");
    elapsed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_connection_budget_caps_live_peers() {
    let payload = make_payload();
    let meta = build_meta(&payload);
    let mut seeds = Vec::new();
    let mut seed_addrs = Vec::new();
    for _ in 0..2 {
        let dir = tempfile::tempdir().unwrap();
        write_payload(dir.path(), &meta.info.name, &payload);
        let seed = loopback_session(dir.path()).await;
        seed.add_from_meta(meta.clone(), AddTorrentOptions::default())
            .await
            .unwrap();
        seed_addrs.push(
            format!("127.0.0.1:{}", seed.listen_port())
                .parse::<SocketAddr>()
                .unwrap(),
        );
        seeds.push((dir, seed));
    }

    let leech_dir = tempfile::tempdir().unwrap();
    let leech = Session::new_with_opts(
        leech_dir.path().to_path_buf(),
        SessionOptions {
            disable_dht: true,
            max_connections: Some(1),
            listen: Some(risuko_bt::session::ListenerOptions {
                listen_addr: Some("127.0.0.1:0".parse().unwrap()),
                enable_upnp_port_forwarding: false,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let leech_handle = match leech
        .add_from_meta(
            meta.clone(),
            AddTorrentOptions {
                download_limit: 16 * 1024,
                ..Default::default()
            },
        )
        .await
        .unwrap()
    {
        AddTorrentResponse::Added(_, h) => h,
        _ => panic!("expected Added"),
    };
    for addr in &seed_addrs {
        leech.add_peer(meta.info_hash, *addr).await.unwrap();
    }

    let mut max_live = 0;
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while std::time::Instant::now() < deadline {
        let live = leech_handle
            .stats()
            .live
            .map_or(0, |live| live.snapshot.peer_stats.live);
        max_live = max_live.max(live);
        assert!(live <= 1, "budget of 1 exceeded: {live} live peers");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(max_live, 1, "the single budgeted peer should connect");
    drop(seeds);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_torrent_download_cap_paces_the_leecher() {
    let elapsed = timed_transfer(
        SessionOptions::default(),
        AddTorrentOptions::default(),
        AddTorrentOptions {
            download_limit: 64 * 1024,
            ..Default::default()
        },
        |_, _, _| {},
    )
    .await;
    assert!(elapsed >= Duration::from_millis(1500), "took {elapsed:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_upload_cap_paces_the_seeder() {
    let elapsed = timed_transfer(
        SessionOptions {
            upload_limiter: Some(std::sync::Arc::new(RateLimiter::new(64 * 1024))),
            ..Default::default()
        },
        AddTorrentOptions::default(),
        AddTorrentOptions::default(),
        |_, _, _| {},
    )
    .await;
    assert!(elapsed >= Duration::from_millis(1500), "took {elapsed:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_cap_can_be_lifted_while_running() {
    let elapsed = timed_transfer(
        SessionOptions::default(),
        AddTorrentOptions::default(),
        AddTorrentOptions {
            download_limit: 8 * 1024,
            ..Default::default()
        },
        |_, _, handle| handle.set_download_limit(0),
    )
    .await;
    assert!(elapsed < Duration::from_secs(15), "took {elapsed:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_caps_can_be_lifted_while_running() {
    let elapsed = timed_transfer(
        SessionOptions {
            upload_limiter: Some(std::sync::Arc::new(RateLimiter::new(8 * 1024))),
            ..Default::default()
        },
        AddTorrentOptions::default(),
        AddTorrentOptions::default(),
        |seed, _, _| seed.set_upload_limit(0),
    )
    .await;
    assert!(elapsed < Duration::from_secs(15), "took {elapsed:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leecher_downloads_only_selected_file() {
    let payload = make_payload();
    let mut meta = build_meta(&payload);
    let half = payload.len() / 2;
    meta.info.single_file_mode = false;
    meta.info.name = "pair".to_string();
    meta.info.files = vec![
        TorrentMetaInfo {
            path: vec!["first.bin".to_string()],
            length: half as u64,
            padding: false,
        },
        TorrentMetaInfo {
            path: vec!["second.bin".to_string()],
            length: (payload.len() - half) as u64,
            padding: false,
        },
    ];

    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    write_payload(seed_dir.path(), "first.bin", &payload[..half]);
    write_payload(seed_dir.path(), "second.bin", &payload[half..]);
    let seed = loopback_session(seed_dir.path()).await;
    let leech = loopback_session(leech_dir.path()).await;

    let flat = |only_files| AddTorrentOptions {
        create_subfolder: false,
        only_files,
        ..Default::default()
    };
    let AddTorrentResponse::Added(_, _seed_handle) =
        seed.add_from_meta(meta.clone(), flat(None)).await.unwrap()
    else {
        panic!("expected Added");
    };
    let AddTorrentResponse::Added(_, leech_handle) = leech
        .add_from_meta(meta.clone(), flat(Some(vec![1])))
        .await
        .unwrap()
    else {
        panic!("expected Added");
    };
    let seed_addr: SocketAddr = format!("127.0.0.1:{}", seed.listen_port()).parse().unwrap();
    leech.add_peer(meta.info_hash, seed_addr).await.unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !leech_handle.stats().finished {
        assert!(
            std::time::Instant::now() < deadline,
            "selective download timed out: progress={}",
            leech_handle.stats().progress_bytes
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let stats = leech_handle.stats();
    assert_eq!(stats.left_bytes, 0);
    assert_eq!(*stats.file_progress, vec![0, (payload.len() - half) as u64]);
    let got = std::fs::read(leech_dir.path().join("second.bin")).unwrap();
    assert!(got == payload[half..], "selected file corrupted");
    assert!(
        !leech_dir.path().join("first.bin").exists(),
        "unselected file must not be created"
    );

    leech_handle.set_only_files(None).await.unwrap();
    assert!(!leech_handle.stats().finished);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !leech_handle.stats().finished {
        assert!(
            std::time::Instant::now() < deadline,
            "reselected download timed out"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let got = std::fs::read(leech_dir.path().join("first.bin")).unwrap();
    assert!(got == payload[..half], "reselected file corrupted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unselected_neighbour_of_a_boundary_piece_stays_in_a_part_file() {
    let payload = make_payload();
    let mut meta = build_meta(&payload);
    let split = 100 * 1024;
    meta.info.single_file_mode = false;
    meta.info.name = "pair".to_string();
    meta.info.files = vec![
        TorrentMetaInfo {
            path: vec!["a.bin".to_string()],
            length: split as u64,
            padding: false,
        },
        TorrentMetaInfo {
            path: vec!["b.bin".to_string()],
            length: (payload.len() - split) as u64,
            padding: false,
        },
    ];
    let seed_dir = tempfile::tempdir().unwrap();
    let leech_dir = tempfile::tempdir().unwrap();
    write_payload(seed_dir.path(), "a.bin", &payload[..split]);
    write_payload(seed_dir.path(), "b.bin", &payload[split..]);
    let seed = loopback_session(seed_dir.path()).await;
    let leech = loopback_session(leech_dir.path()).await;
    let flat = |only_files| AddTorrentOptions {
        create_subfolder: false,
        only_files,
        ..Default::default()
    };
    let AddTorrentResponse::Added(_, _seed_handle) =
        seed.add_from_meta(meta.clone(), flat(None)).await.unwrap()
    else {
        panic!("expected Added");
    };
    let AddTorrentResponse::Added(_, leech_handle) = leech
        .add_from_meta(meta.clone(), flat(Some(vec![1])))
        .await
        .unwrap()
    else {
        panic!("expected Added");
    };
    let seed_addr: SocketAddr = format!("127.0.0.1:{}", seed.listen_port()).parse().unwrap();
    leech.add_peer(meta.info_hash, seed_addr).await.unwrap();

    let wait_finished = |what: &'static str| {
        let handle = leech_handle.clone();
        async move {
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            while !handle.stats().finished {
                assert!(std::time::Instant::now() < deadline, "{what} timed out");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    };
    wait_finished("selected download").await;
    assert!(
        !leech_dir.path().join("a.bin").exists(),
        "boundary bytes of the unselected file belong in the part file"
    );
    let got = std::fs::read(leech_dir.path().join("b.bin")).unwrap();
    assert!(got == payload[split..]);

    leech_handle.set_only_files(None).await.unwrap();
    wait_finished("full download").await;
    let got = std::fs::read(leech_dir.path().join("a.bin")).unwrap();
    assert!(got == payload[..split], "promoted file corrupted");
    assert!(!leech_dir.path().join(".risuko-parts").exists());
}

#[test]
fn sanity_bytes_type_exists() {
    let _ = Bytes::from_static(b"");
    let _ = PathBuf::new();
}

mod v2 {
    use super::*;
    use risuko_bt::bencode::{encode_to_vec, Value};
    use risuko_bt::core::merkle::{compute_root_padded, hash_block, piece_layer_root, BLOCK_SIZE};
    use risuko_bt::core::metainfo::parse_torrent;
    use risuko_bt::core::Id32;

    const V2_PIECE_LEN: u32 = 64 * 1024;
    const V2_TOTAL: u64 = 256 * 1024;

    fn make_v2_payload() -> Vec<u8> {
        let mut out = Vec::with_capacity(V2_TOTAL as usize);
        let mut x: u32 = 0xCAFEBABE;
        for _ in 0..V2_TOTAL {
            x = x.wrapping_mul(1664525).wrapping_add(1013904223);
            out.push((x >> 16) as u8);
        }
        out
    }

    fn piece_root(piece_bytes: &[u8], blocks_per_piece: u32) -> Id32 {
        let mut leaves: Vec<Id32> = piece_bytes
            .chunks(BLOCK_SIZE as usize)
            .map(hash_block)
            .collect();
        let target = blocks_per_piece as usize;
        if leaves.len() < target {
            leaves.resize(target, Id32([0u8; 32]));
        }
        if target == 1 {
            leaves[0]
        } else {
            compute_root_padded(&leaves, target.next_power_of_two())
        }
    }

    fn build_pure_v2_torrent(name: &str, payload: &[u8]) -> Vec<u8> {
        let blocks_per_piece = V2_PIECE_LEN / BLOCK_SIZE;
        let pieces: Vec<Id32> = payload
            .chunks(V2_PIECE_LEN as usize)
            .map(|p| piece_root(p, blocks_per_piece))
            .collect();
        let pieces_root = if pieces.len() == 1 {
            pieces[0]
        } else {
            piece_layer_root(&pieces, blocks_per_piece)
        };
        let mut layer_bytes = Vec::with_capacity(pieces.len() * 32);
        for r in &pieces {
            layer_bytes.extend_from_slice(&r.0);
        }

        let file_leaf = Value::Dict(vec![(
            b"".to_vec(),
            Value::Dict(vec![
                (b"length".to_vec(), Value::Int(payload.len() as i64)),
                (
                    b"pieces root".to_vec(),
                    Value::Bytes(pieces_root.0.to_vec()),
                ),
            ]),
        )]);
        let info = Value::Dict(vec![
            (
                b"file tree".to_vec(),
                Value::Dict(vec![(name.as_bytes().to_vec(), file_leaf)]),
            ),
            (b"meta version".to_vec(), Value::Int(2)),
            (b"name".to_vec(), Value::Bytes(name.as_bytes().to_vec())),
            (b"piece length".to_vec(), Value::Int(V2_PIECE_LEN as i64)),
        ]);
        let layers = Value::Dict(vec![(pieces_root.0.to_vec(), Value::Bytes(layer_bytes))]);
        let top = Value::Dict(vec![
            (b"info".to_vec(), info),
            (b"piece layers".to_vec(), layers),
        ]);
        encode_to_vec(&top)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pure_v2_leecher_downloads_from_seeder() {
        let _ = env_logger::builder().is_test(true).try_init();

        let payload = make_v2_payload();
        let bytes = build_pure_v2_torrent("payload-v2.bin", &payload);
        let meta = parse_torrent(&bytes).expect("pure-v2 torrent parses");
        assert!(meta.info_hash_v2.is_some(), "v2 info-hash present");

        let seed_dir = tempfile::tempdir().unwrap();
        let leech_dir = tempfile::tempdir().unwrap();

        write_payload(seed_dir.path(), &meta.info.name, &payload);

        let seed = Session::new_with_opts(
            seed_dir.path().to_path_buf(),
            SessionOptions {
                disable_dht: true,
                listen: Some(risuko_bt::session::ListenerOptions {
                    listen_addr: Some("127.0.0.1:0".parse().unwrap()),
                    enable_upnp_port_forwarding: false,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let leech = Session::new_with_opts(
            leech_dir.path().to_path_buf(),
            SessionOptions {
                disable_dht: true,
                listen: Some(risuko_bt::session::ListenerOptions {
                    listen_addr: Some("127.0.0.1:0".parse().unwrap()),
                    enable_upnp_port_forwarding: false,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let _seed_handle = match seed
            .add_from_meta(meta.clone(), AddTorrentOptions::default())
            .await
            .expect("seed add v2")
        {
            AddTorrentResponse::Added(_, h) => h,
            _ => panic!("expected Added"),
        };
        let leech_handle = match leech
            .add_from_meta(meta.clone(), AddTorrentOptions::default())
            .await
            .expect("leech add v2")
        {
            AddTorrentResponse::Added(_, h) => h,
            _ => panic!("expected Added"),
        };

        let seed_addr: SocketAddr = format!("127.0.0.1:{}", seed.listen_port()).parse().unwrap();
        leech
            .add_peer(meta.info_hash, seed_addr)
            .await
            .expect("add peer");

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let s = leech_handle.stats();
            if s.finished {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!(
                    "pure-v2 timed out: progress={} / {} bytes (peers={})",
                    s.progress_bytes,
                    s.total_bytes,
                    s.live.map(|l| l.snapshot.peer_stats.live).unwrap_or(0),
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let got = std::fs::read(leech_dir.path().join(&meta.info.name)).unwrap();
        assert_eq!(got.len(), payload.len(), "v2 length mismatch");
        assert_eq!(got, payload, "v2 payload mismatch");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pure_v2_magnet_leecher_resolves_and_downloads_from_seeder() {
        let _ = env_logger::builder().is_test(true).try_init();

        let payload = make_v2_payload();
        let bytes = build_pure_v2_torrent("payload-v2-magnet.bin", &payload);
        let meta = parse_torrent(&bytes).expect("pure-v2 torrent parses");
        let v2_hash = meta.info_hash_v2.expect("v2 hash present");

        let seed_dir = tempfile::tempdir().unwrap();
        let leech_dir = tempfile::tempdir().unwrap();

        write_payload(seed_dir.path(), &meta.info.name, &payload);

        let seed = Session::new_with_opts(
            seed_dir.path().to_path_buf(),
            SessionOptions {
                disable_dht: true,
                listen: Some(risuko_bt::session::ListenerOptions {
                    listen_addr: Some("127.0.0.1:0".parse().unwrap()),
                    enable_upnp_port_forwarding: false,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let _seed_handle = match seed
            .add_from_meta(meta.clone(), AddTorrentOptions::default())
            .await
            .expect("seed add v2")
        {
            AddTorrentResponse::Added(_, h) => h,
            _ => panic!("expected Added"),
        };

        let seed_addr: SocketAddr = format!("127.0.0.1:{}", seed.listen_port()).parse().unwrap();
        let magnet_uri = format!("magnet:?xt=urn:btmh:1220{}", hex::encode(v2_hash.0));

        let resolved = risuko_bt::magnet::resolve_with_peers(
            &magnet_uri,
            &[],
            &[seed_addr],
            Duration::from_secs(30),
            risuko_bt::peer::EncryptionPolicy::PlaintextOnly,
        )
        .await
        .expect("magnet resolves");
        assert_eq!(resolved.info_hash_v2, Some(v2_hash));
        assert!(
            !resolved.piece_layers.is_empty(),
            "piece layers fetched from seeder"
        );

        let torrent_bytes = risuko_bt::magnet::synth_torrent_bytes(
            &resolved.info_bytes,
            &resolved.trackers,
            &resolved.piece_layers,
        );
        let leech_meta = parse_torrent(&torrent_bytes).expect("synthesised torrent parses");
        assert_eq!(leech_meta.info_hash_v2, Some(v2_hash));

        let leech = Session::new_with_opts(
            leech_dir.path().to_path_buf(),
            SessionOptions {
                disable_dht: true,
                listen: Some(risuko_bt::session::ListenerOptions {
                    listen_addr: Some("127.0.0.1:0".parse().unwrap()),
                    enable_upnp_port_forwarding: false,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let leech_handle = match leech
            .add_from_meta(leech_meta.clone(), AddTorrentOptions::default())
            .await
            .expect("leech add v2 magnet")
        {
            AddTorrentResponse::Added(_, h) => h,
            _ => panic!("expected Added"),
        };
        leech
            .add_peer(leech_meta.info_hash, seed_addr)
            .await
            .expect("add peer");

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let s = leech_handle.stats();
            if s.finished {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!(
                    "pure-v2 magnet timed out: progress={} / {} bytes (peers={})",
                    s.progress_bytes,
                    s.total_bytes,
                    s.live.map(|l| l.snapshot.peer_stats.live).unwrap_or(0),
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let got = std::fs::read(leech_dir.path().join(&leech_meta.info.name)).unwrap();
        assert_eq!(got.len(), payload.len(), "v2 magnet length mismatch");
        assert_eq!(got, payload, "v2 magnet payload mismatch");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn seed_answers_leaf_layer_hash_requests() {
        use risuko_bt::core::merkle::hash_pair;
        use risuko_bt::peer::{connect, EncryptionPolicy, PeerCommand, PeerEvent, SpawnPeer};
        use risuko_bt::wire::Message;

        let payload = make_v2_payload();
        let bytes = build_pure_v2_torrent("leaf-v2.bin", &payload);
        let meta = parse_torrent(&bytes).unwrap();
        let root = meta.info_v2.as_ref().unwrap().files[0].pieces_root;
        let seed_dir = tempfile::tempdir().unwrap();
        write_payload(seed_dir.path(), &meta.info.name, &payload);
        let seed = super::loopback_session(seed_dir.path()).await;
        let AddTorrentResponse::Added(_, seed_handle) = seed
            .add_from_meta(meta.clone(), AddTorrentOptions::default())
            .await
            .unwrap()
        else {
            panic!("expected Added");
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !seed_handle.stats().finished {
            assert!(
                std::time::Instant::now() < deadline,
                "seed never verified its data"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        let (peer, mut events) = connect(SpawnPeer {
            addr: format!("127.0.0.1:{}", seed.listen_port()).parse().unwrap(),
            info_hash: meta.info_hash,
            our_peer_id: Id20::new([0x77; 20]),
            connect_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(5),
            encryption: EncryptionPolicy::PlaintextOnly,
            advertise_v2: true,
            advertise_dht: false,
            ext_handshake_builder: None,
            proxy: None,
            deferred: false,
        })
        .await
        .unwrap();
        peer.tx
            .send(PeerCommand::Send(Message::HashRequest {
                pieces_root: root.0,
                base_layer: 0,
                index: 4,
                length: 2,
                proof_layers: 3,
            }))
            .await
            .unwrap();
        let hashes = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match events.recv().await.expect("connection closed") {
                    PeerEvent::Message(Message::Hashes { hashes, .. }) => break hashes,
                    PeerEvent::Message(Message::HashReject { .. }) => {
                        panic!("leaf request rejected")
                    }
                    _ => continue,
                }
            }
        })
        .await
        .expect("no hashes reply");
        let hashes: Vec<Id32> = hashes
            .chunks_exact(32)
            .map(|c| Id32::from_slice(c).unwrap())
            .collect();
        assert_eq!(hashes.len(), 5);
        assert_eq!(
            hashes[0],
            hash_block(&payload[4 * BLOCK_SIZE as usize..5 * BLOCK_SIZE as usize])
        );
        let mut node = hash_pair(&hashes[0], &hashes[1]);
        let mut position: u32 = 4 / 2;
        for uncle in &hashes[2..] {
            node = if position.is_multiple_of(2) {
                hash_pair(&node, uncle)
            } else {
                hash_pair(uncle, &node)
            };
            position /= 2;
        }
        assert_eq!(node, root);
    }
}
