//! End-to-end swarm test for the in-tree BitTorrent implementation Spins up two `Session`s sharing a 256 KiB random payload. The seeder pre-populates storage and we verify the leecher receives a byte-exact copy via direct peer connection (no tracker / DHT)

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use risuko_bt::core::hash::Id20;
use risuko_bt::core::metainfo::{TorrentMeta, TorrentMetaInfo, ValidatedTorrentMetaV1Info};
use risuko_bt::session::{AddTorrentOptions, AddTorrentResponse, Session, SessionOptions};
use sha1::{Digest, Sha1};

const PIECE_LEN: u32 = 64 * 1024;
const TOTAL: u64 = 256 * 1024;

fn make_payload() -> Vec<u8> {
    // Deterministic non-trivial pattern: LCG output. Not uniform random, but each piece is distinct which is all we need
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
        }],
        single_file_mode: true,
    };
    // Build a deterministic info-hash by SHA-1ing a concat of name+length+pieces — the value doesn't matter for peer-direct tests as long as both sessions agree. Handshake compares this byte-for-byte
    let mut h = Sha1::new();
    h.update(&info.name);
    h.update((info.files[0].length as u64).to_be_bytes());
    h.update(&info.pieces);
    let ih: [u8; 20] = h.finalize().into();
    TorrentMeta {
        info,
        announce: None,
        announce_list: Vec::new(),
        url_list: Vec::new(),
        bootstrap_nodes: Vec::new(),
        bootstrap_hosts: Vec::new(),
        comment: None,
        created_by: None,
        creation_date: None,
        encoding: None,
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

    // Seed side: file is already on disk so scan_existing_pieces marks them
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

    // Add to seeder
    let _seed_handle = match seed
        .add_from_meta(meta.clone(), AddTorrentOptions::default())
        .await
        .expect("seed add")
    {
        AddTorrentResponse::Added(_, h) => h,
        _ => panic!("expected Added"),
    };

    // Add to leecher
    let leech_handle = match leech
        .add_from_meta(meta.clone(), AddTorrentOptions::default())
        .await
        .expect("leech add")
    {
        AddTorrentResponse::Added(_, h) => h,
        _ => panic!("expected Added"),
    };

    // Dial seeder from leecher
    let seed_addr: SocketAddr = format!("127.0.0.1:{}", seed.listen_port()).parse().unwrap();
    leech
        .add_peer(meta.info_hash, seed_addr)
        .await
        .expect("add peer");

    // Poll for completion
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

    // Verify byte equality
    let got = std::fs::read(leech_dir.path().join(&meta.info.name)).unwrap();
    assert_eq!(got.len(), payload.len(), "length mismatch");
    assert_eq!(got, payload, "payload mismatch");
}

#[test]
fn sanity_bytes_type_exists() {
    // Exercise the Bytes re-export so unused-import lints don't trip
    let _ = Bytes::from_static(b"");
    let _ = PathBuf::new();
}

/// BEP 52 (v2) pure-v2 swarm test
mod v2 {
    use super::*;
    use risuko_bt::bencode::{encode_to_vec, Value};
    use risuko_bt::core::merkle::{compute_root, compute_root_padded, hash_block, BLOCK_SIZE};
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

    /// Compute the per-piece root (SHA-256 Merkle subtree over 16 KiB blocks, zero-padded to `blocks_per_piece`)
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

    /// Build a real BEP 52 pure-v2 .torrent for a single-file payload. Returns (bencode_bytes, pieces_root, layer_bytes) so the test can assert the parsed `info_hash_v2` matches expectations
    fn build_pure_v2_torrent(name: &str, payload: &[u8]) -> Vec<u8> {
        let blocks_per_piece = V2_PIECE_LEN / BLOCK_SIZE;
        let pieces: Vec<Id32> = payload
            .chunks(V2_PIECE_LEN as usize)
            .map(|p| piece_root(p, blocks_per_piece))
            .collect();
        let pieces_root = if pieces.len() == 1 {
            pieces[0]
        } else {
            compute_root(&pieces)
        };
        let mut layer_bytes = Vec::with_capacity(pieces.len() * 32);
        for r in &pieces {
            layer_bytes.extend_from_slice(&r.0);
        }

        // file tree: { name: { "": { length, pieces root } } }
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

        // Seed-side payload pre-populated; scan_existing_pieces will mark it local via the V2Merkle verifier
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

    // NOTE: an end-to-end pure-v2 magnet swarm test (leecher resolves `magnet:?xt=urn:btmh:...` against a risuko seeder) requires the seeder side to also serve the info dict over BEP 9 ut_metadata. risuko's torrent loop currently does not implement an ut_metadata responder (info dicts are loaded from the .torrent on both sides). Adding one is its own feature; in the meantime the BEP 52 piece- layer responder is exercised by the unit test `core::merkle::tests::full_piece_layer_serve_and_verify_round_trip` and by `torrent::tests::build_hash_response_serves_full_piece_layer`

    /// End-to-end pure-v2 magnet test: 1. Build a real BEP 52 .torrent + payload, seed it from session A 2. Construct a pure-v2 magnet URI (`urn:btmh:1220<sha256>`) 3. Have session B resolve the magnet by dialing session A's listener directly. The resolver must: a. Receive the info dict via BEP-9 `ut_metadata` b. Receive every file's piece-layer rows via BEP 52 `HASH_REQUEST` / `Hashes` 4. Synthesize the .torrent bytes from the resolved info + piece layers, attach to session B, dial seeder, download to completion, and assert byte equality with the original payload Exercises the full pure-v2 magnet path: ut_metadata responder (added in this test's enabling commit), HASH_REQUEST responder, and the leecher's piece-layer fetcher
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

        // Resolve via direct peer dial (no tracker / DHT). This drives both ut_metadata fetch and BEP 52 piece-layer fetch through the seeder
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

        // Reconstruct a .torrent blob from the resolved pieces and parse
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
}
