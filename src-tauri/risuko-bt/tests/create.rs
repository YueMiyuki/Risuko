mod common;

use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use common::{loopback_session, wait_until_finished};
use risuko_bt::core::MetaVersion;
use risuko_bt::create::{create_torrent, CreateOptions};
use risuko_bt::session::{AddTorrent, AddTorrentOptions, AddTorrentResponse};

fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[1] ^ seed)
        .collect()
}

fn snapshot(root: &Path) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                let rel = path
                    .strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned();
                out.push((rel, std::fs::read(&path).unwrap()));
            }
        }
    }
    out.sort();
    out
}

async fn added(response: AddTorrentResponse) -> std::sync::Arc<risuko_bt::ManagedTorrent> {
    match response {
        AddTorrentResponse::Added(_, handle) => handle,
        AddTorrentResponse::AlreadyManaged(..) => panic!("expected Added"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_created_torrent_seeds_in_place_and_downloads_elsewhere() {
    for version in [MetaVersion::V1, MetaVersion::V2, MetaVersion::Hybrid] {
        let seed_dir = tempfile::tempdir().unwrap();
        let leech_dir = tempfile::tempdir().unwrap();
        let source = seed_dir.path().join("Season 1");
        for (i, (path, len)) in [
            ("ep1.mkv", 100_003usize),
            ("extras/notes.txt", 17),
            ("extras/empty", 0),
            ("ep2.mkv", 65_536),
        ]
        .into_iter()
        .enumerate()
        {
            let path = source.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, pattern(len, i as u8)).unwrap();
        }
        let before = snapshot(&source);

        let created = create_torrent(
            &source,
            &CreateOptions {
                version,
                piece_length: Some(32 * 1024),
                ..Default::default()
            },
            |_, _| {},
            &AtomicBool::new(false),
        )
        .unwrap();

        let seed = loopback_session(seed_dir.path(), false).await;
        let seed_handle = added(
            seed.add_torrent(
                AddTorrent::TorrentFileBytes(created.bytes.clone().into()),
                Some(AddTorrentOptions {
                    output_folder: Some(seed_dir.path().to_string_lossy().into_owned()),
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        )
        .await;
        wait_until_finished(&seed_handle, Duration::from_secs(10)).await;
        assert_eq!(
            snapshot(&source),
            before,
            "{version:?} seeding touched the source"
        );

        let leech = loopback_session(leech_dir.path(), false).await;
        let leech_handle = added(
            leech
                .add_torrent(
                    AddTorrent::TorrentFileBytes(created.bytes.clone().into()),
                    Some(AddTorrentOptions::default()),
                )
                .await
                .unwrap(),
        )
        .await;
        let seed_addr = format!("127.0.0.1:{}", seed.listen_port()).parse().unwrap();
        leech
            .add_peer(leech_handle.info_hash(), seed_addr)
            .await
            .unwrap();
        wait_until_finished(&leech_handle, Duration::from_secs(30)).await;
        assert_eq!(
            snapshot(&leech_dir.path().join("Season 1")),
            before,
            "{version:?} download differs from the source"
        );
        seed.shutdown().await;
        leech.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn creation_stamps_skip_the_recheck_until_a_file_changes() {
    const PIECE: u32 = 32 * 1024;
    for version in [MetaVersion::V1, MetaVersion::V2, MetaVersion::Hybrid] {
        for keep_mtime in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let source = dir.path().join("pack");
            std::fs::create_dir_all(source.join("b")).unwrap();
            std::fs::write(source.join("a.bin"), pattern(100_003, 1)).unwrap();
            std::fs::write(source.join("b/c.bin"), pattern(40_000, 2)).unwrap();
            let created = create_torrent(
                &source,
                &CreateOptions {
                    version,
                    piece_length: Some(PIECE),
                    ..Default::default()
                },
                |_, _| {},
                &AtomicBool::new(false),
            )
            .unwrap();
            assert_eq!(created.stamps.len(), 2);

            let first = source.join("a.bin");
            let modified = std::fs::metadata(&first).unwrap().modified().unwrap();
            let mut bytes = std::fs::read(&first).unwrap();
            bytes[0] ^= 1;
            std::fs::write(&first, &bytes).unwrap();
            let stamp = if keep_mtime {
                modified
            } else {
                modified + Duration::from_secs(5)
            };
            std::fs::File::options()
                .write(true)
                .open(&first)
                .unwrap()
                .set_modified(stamp)
                .unwrap();

            let session = loopback_session(dir.path(), false).await;
            let handle = added(
                session
                    .add_torrent(
                        AddTorrent::TorrentFileBytes(created.bytes.clone().into()),
                        Some(AddTorrentOptions {
                            output_folder: Some(dir.path().to_string_lossy().into_owned()),
                            verified_files: created.stamps.clone(),
                            ..Default::default()
                        }),
                    )
                    .await
                    .unwrap(),
            )
            .await;
            if keep_mtime {
                wait_until_finished(&handle, Duration::from_secs(10)).await;
            } else {
                let expected = handle.stats().total_bytes - PIECE as u64;
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                while handle.stats().progress_bytes != expected {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "{version:?} scan never settled"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                assert!(!handle.stats().finished, "{version:?}");
            }
            session.shutdown().await;
        }
    }
}
