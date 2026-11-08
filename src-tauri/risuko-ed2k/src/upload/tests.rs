use std::net::{Ipv4Addr, SocketAddrV4};
use std::path::Path;
use std::sync::atomic::AtomicU64;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use super::session::{packet_spans, Mode};
use super::*;
use crate::chunks::ChunkManager;
use crate::download::{peer_session, PeerOutcome, PeerTimeouts};
use crate::md4::md4;
use crate::protocol::*;
use crate::share::{hash_file, PartMap, ShareRegistry, SharedFile};
use crate::types::*;
use risuko_util::limiter::Throttle;

fn fixture(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

async fn uploader(max_slots: usize) -> Arc<Ed2kUploader> {
    let mut config = UploadConfig::new(new_user_hash(), 0);
    config.bind_addr = Ipv4Addr::LOCALHOST;
    config.max_slots = max_slots;
    Ed2kUploader::bind(config).await.expect("bind uploader")
}

fn share_complete(up: &Ed2kUploader, dir: &Path, data: &[u8]) -> (ShareHandle, [u8; 16]) {
    let path = dir.join("shared.bin");
    std::fs::write(&path, data).unwrap();
    let (hash, hashset) = hash_file(&path, &CancellationToken::new()).unwrap();
    let file = SharedFile::complete(hash, "shared.bin", data.len() as u64, path, hashset);
    (up.share(file), hash)
}

async fn download(
    up: &Ed2kUploader,
    dir: &Path,
    name: &str,
    hash: [u8; 16],
    size: u64,
    client: u8,
    timeouts: PeerTimeouts,
) -> (Result<PeerOutcome, String>, Arc<Mutex<ChunkManager>>) {
    let path = dir.join(name);
    let mut cm = ChunkManager::new(path, size).with_file_hash(hash);
    cm.init_file().await.unwrap();
    let chunks = Arc::new(Mutex::new(cm));
    let user = mark_user_hash([client; 16]);
    let result = peer_session(
        timeouts,
        u64::from(client),
        SocketAddrV4::new(Ipv4Addr::LOCALHOST, up.port()),
        user,
        0,
        0,
        0,
        0,
        &hash,
        &chunks,
        &Arc::new(AtomicU64::new(0)),
        &CancellationToken::new(),
        risuko_http::ProxyConnector::direct(),
        &Throttle::unlimited(),
    )
    .await;
    (result, chunks)
}

async fn wait_for_free_slots(up: &Ed2kUploader, why: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while up.shared.queue.lock().slots() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect(why);
}

async fn assert_downloaded(chunks: &Arc<Mutex<ChunkManager>>, dir: &Path, name: &str, data: &[u8]) {
    assert!(chunks.lock().await.is_complete());
    assert_eq!(std::fs::read(dir.join(name)).unwrap(), data);
}

#[tokio::test]
async fn risuko_downloads_a_two_part_file_from_the_listener() {
    let dir = tempfile::tempdir().unwrap();
    let up = uploader(4).await;
    let data = fixture(ED2K_CHUNK_SIZE as usize + 1000);
    let (handle, hash) = share_complete(&up, dir.path(), &data);
    let (result, chunks) = download(
        &up,
        dir.path(),
        "got.bin",
        hash,
        data.len() as u64,
        1,
        PeerTimeouts::default(),
    )
    .await;
    assert_eq!(result, Ok(PeerOutcome::Finished));
    assert_downloaded(&chunks, dir.path(), "got.bin", &data).await;
    assert_eq!(handle.file().stats().uploaded(), data.len() as u64);
    up.shutdown().await;
}

#[tokio::test]
async fn exact_chunk_multiple_serves_the_trailing_empty_part_hash() {
    let dir = tempfile::tempdir().unwrap();
    let up = uploader(4).await;
    let data = fixture(ED2K_CHUNK_SIZE as usize);
    let (_handle, hash) = share_complete(&up, dir.path(), &data);
    assert_eq!(hash, md4(&[md4(&data), md4(b"")].concat()));
    let (result, chunks) = download(
        &up,
        dir.path(),
        "got.bin",
        hash,
        data.len() as u64,
        1,
        PeerTimeouts::default(),
    )
    .await;
    assert_eq!(result, Ok(PeerOutcome::Finished));
    assert_downloaded(&chunks, dir.path(), "got.bin", &data).await;
    up.shutdown().await;
}

#[tokio::test]
async fn partial_share_serves_only_verified_parts() {
    let dir = tempfile::tempdir().unwrap();
    let up = uploader(4).await;
    let data = fixture(ED2K_CHUNK_SIZE as usize * 2 + 500);
    let path = dir.path().join("partial.bin");
    std::fs::write(&path, &data).unwrap();
    let (hash, hashset) = hash_file(&path, &CancellationToken::new()).unwrap();
    let parts = PartMap::new(data.len() as u64);
    parts.set_hashset(&hashset);
    parts.mark_verified(1);
    let _handle = up.share(SharedFile::partial(
        hash,
        "partial.bin",
        data.len() as u64,
        path,
        parts,
    ));
    let timeouts = PeerTimeouts {
        transfer: Duration::from_millis(500),
        ..PeerTimeouts::default()
    };
    let (_result, chunks) = download(
        &up,
        dir.path(),
        "got.bin",
        hash,
        data.len() as u64,
        1,
        timeouts,
    )
    .await;
    let cm = chunks.lock().await;
    assert!(!cm.is_complete());
    assert_eq!(cm.completed_length(), ED2K_CHUNK_SIZE);
    let got = std::fs::read(dir.path().join("got.bin")).unwrap();
    let part = ED2K_CHUNK_SIZE as usize..ED2K_CHUNK_SIZE as usize * 2;
    assert_eq!(got[part.clone()], data[part]);
    drop(cm);
    up.shutdown().await;
}

#[tokio::test]
async fn a_queued_client_gets_the_slot_when_the_holder_leaves() {
    let dir = tempfile::tempdir().unwrap();
    let up = uploader(1).await;
    let data = fixture(ED2K_CHUNK_SIZE as usize + 1000);
    let (_handle, hash) = share_complete(&up, dir.path(), &data);
    let size = data.len() as u64;
    let (first, second) = tokio::join!(
        download(
            &up,
            dir.path(),
            "a.bin",
            hash,
            size,
            1,
            PeerTimeouts::default()
        ),
        download(
            &up,
            dir.path(),
            "b.bin",
            hash,
            size,
            2,
            PeerTimeouts::default()
        ),
    );
    assert_eq!(first.0, Ok(PeerOutcome::Finished));
    assert_eq!(second.0, Ok(PeerOutcome::Finished));
    assert_downloaded(&first.1, dir.path(), "a.bin", &data).await;
    assert_downloaded(&second.1, dir.path(), "b.bin", &data).await;
    wait_for_free_slots(&up, "slot released after the client left").await;
    up.shutdown().await;
}

async fn raw_client(up: &Ed2kUploader, user: [u8; 16]) -> tokio::net::TcpStream {
    let mut stream = tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, up.port()))
        .await
        .unwrap();
    let hello = build_hello(false, &user, 0, 0, 0, 0);
    stream.write_all(&hello.encode()).await.unwrap();
    stream
}

async fn next_packet(
    stream: &mut tokio::net::TcpStream,
    buf: &mut bytes::BytesMut,
) -> Option<Ed2kPacket> {
    loop {
        if let Some(packet) = Ed2kPacket::decode(buf).ok()? {
            return Some(packet);
        }
        let read = tokio::time::timeout(Duration::from_secs(5), stream.read_buf(buf))
            .await
            .ok()?
            .ok()?;
        if read == 0 {
            return None;
        }
    }
}

#[tokio::test]
async fn hello_is_answered_and_unknown_files_get_no_file() {
    let up = uploader(4).await;
    let mut stream = raw_client(&up, [3; 16]).await;
    let mut buf = bytes::BytesMut::new();
    let answer = next_packet(&mut stream, &mut buf).await.unwrap();
    assert_eq!(answer.opcode, OP_HELLO_ANSWER);
    let info = parse_hello(&answer.payload, true).unwrap();
    assert_eq!(info.user_hash, up.shared.user_hash);
    assert_eq!(info.port, up.port());
    assert!(info.caps.large_files());
    stream
        .write_all(&build_file_status_request(&[9; 16]).encode())
        .await
        .unwrap();
    let reply = next_packet(&mut stream, &mut buf).await.unwrap();
    assert_eq!(
        (reply.opcode, reply.payload),
        (OP_FILE_REQ_ANS_NOFILE, vec![9; 16])
    );
    up.shutdown().await;
}

#[tokio::test]
async fn connections_carrying_our_own_user_hash_are_dropped() {
    let up = uploader(4).await;
    let mut stream = raw_client(&up, up.shared.user_hash).await;
    assert!(next_packet(&mut stream, &mut bytes::BytesMut::new())
        .await
        .is_none());
    up.shutdown().await;
}

#[tokio::test]
async fn requests_before_hello_close_the_connection() {
    let up = uploader(4).await;
    let mut stream = tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, up.port()))
        .await
        .unwrap();
    stream
        .write_all(&build_slot_request(&[1; 16]).encode())
        .await
        .unwrap();
    assert!(next_packet(&mut stream, &mut bytes::BytesMut::new())
        .await
        .is_none());
    up.shutdown().await;
}

#[test]
fn packets_follow_the_emule_split() {
    assert_eq!(packet_spans(0, 100), vec![(0, 100)]);
    assert_eq!(packet_spans(0, 12_840), vec![(0, 12_840)]);
    let spans = packet_spans(0, 184_320);
    assert_eq!(spans.first(), Some(&(0, 10_240)));
    assert_eq!(spans.last().unwrap().1, 184_320);
    assert!(spans.windows(2).all(|w| w[0].1 == w[1].0));
    assert!(spans.iter().all(|(s, e)| e - s <= 20_480));
    let spans = packet_spans(5, 25_000);
    assert_eq!(spans, vec![(5, 10_245), (10_245, 25_000)]);
}

#[test]
fn wide_offsets_use_the_i64_part_packet() {
    let small = build_sending_part(&[1; 16], 10, &[0; 4]);
    assert_eq!(
        (small.protocol, small.opcode, small.payload.len()),
        (PROTO_EDONKEY, OP_SENDING_PART, 28)
    );
    let big = 5_000_000_000u64;
    let wide = build_sending_part(&[1; 16], big, &[0; 4]);
    assert_eq!(
        (wide.protocol, wide.opcode),
        (PROTO_EMULE, OP_SENDING_PART_I64)
    );
    assert_eq!(
        parse_sending_part_header_i64(&wide.payload).unwrap(),
        ([1; 16], big, big + 4)
    );
    let req = build_request_parts(&[2; 16], &[(big, big + 9), (1, 2)], true).unwrap();
    assert_eq!(
        parse_request_parts(&req.payload, true),
        Some(([2; 16], [(big, big + 9), (1, 2), (0, 0)]))
    );
    let req = build_request_parts(&[2; 16], &[(3, 4)], false).unwrap();
    assert_eq!(
        parse_request_parts(&req.payload, false),
        Some(([2; 16], [(3, 4), (0, 0), (0, 0)]))
    );
}

#[test]
fn offer_packet_carries_name_size_and_high_size_tags() {
    let entries = [OfferEntry {
        file_hash: [4; 16],
        client_id: FILE_TEST_ID,
        client_port: 4662,
        name: "a",
        size: (5u64 << 32) | 7,
    }];
    let packet = build_offer_files(&entries);
    assert_eq!(packet.opcode, OP_OFFER_FILES);
    let p = &packet.payload;
    assert_eq!(&p[..4], &1u32.to_le_bytes());
    assert_eq!(&p[4..20], &[4; 16]);
    assert_eq!(&p[20..24], &FILE_TEST_ID.to_le_bytes());
    assert_eq!(&p[26..30], &3u32.to_le_bytes());
    assert!(p
        .windows(8)
        .any(|w| w == [0x03, 1, 0, FT_FILESIZE, 7, 0, 0, 0]));
    assert!(p
        .windows(8)
        .any(|w| w == [0x03, 1, 0, FT_FILESIZE_HI, 5, 0, 0, 0]));
}

const FILE_TEST_ID: u32 = 0x0a0b_0c0d;

#[test]
fn callback_requests_parse_the_requester_address() {
    assert_eq!(
        parse_callback_requested(&[1, 2, 3, 4, 0x36, 0x12]),
        Some(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 0x1236))
    );
    assert_eq!(parse_callback_requested(&[1, 2, 3, 4, 0, 0]), None);
    assert_eq!(parse_callback_requested(&[1, 2]), None);
}

#[tokio::test]
async fn a_download_shares_its_partial_and_returns_the_hashset() {
    use crate::share::{ShareContext, UploadStats};
    use std::sync::atomic::AtomicU32;

    let dir = tempfile::tempdir().unwrap();
    let source = uploader(4).await;
    let data = fixture(ED2K_CHUNK_SIZE as usize * 2 + 10);
    let (_handle, hash) = share_complete(&source, dir.path(), &data);
    let (_, expected) =
        hash_file(&dir.path().join("shared.bin"), &CancellationToken::new()).unwrap();
    let link = crate::types::Ed2kFileLink {
        file_name: "dl.bin".into(),
        file_size: data.len() as u64,
        file_hash: hex::encode(hash),
        file_hash_bytes: hash,
        sources: vec![crate::types::Ed2kSource {
            ip: "127.0.0.1".into(),
            port: source.port(),
        }],
        aich_hash: None,
    };
    let registry = ShareRegistry::new();
    let changes = registry.subscribe();
    let user_hash = mark_user_hash([5; 16]);
    let share = ShareContext {
        registry: registry.clone(),
        user_hash,
        stats: Arc::new(UploadStats::default()),
        limiter: Arc::new(RateLimiter::unlimited()),
    };
    let out = dir.path().join("out");
    let done = crate::run_ed2k_download_with_proxy(
        &link,
        out.to_str().unwrap(),
        vec!["127.0.0.1:1".into()],
        0,
        None,
        None,
        Arc::new(parking_lot::Mutex::new(None)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        CancellationToken::new(),
        risuko_http::ProxyConnector::direct(),
        Throttle::unlimited(),
        Some(share),
    )
    .await
    .expect("download from the local listener");
    assert_eq!(std::fs::read(&done.path).unwrap(), data);
    assert_eq!(done.hashset, expected);
    assert!(registry.files().is_empty());
    assert_eq!(*changes.borrow(), 2);
    source.shutdown().await;
}

#[test]
fn verified_parts_reach_the_share_map() {
    let size = ED2K_CHUNK_SIZE * 2;
    let parts = PartMap::new(size);
    let mut cm = ChunkManager::new(std::path::PathBuf::from("unused"), size)
        .with_file_hash([1; 16])
        .with_part_map(parts.clone());
    let file = SharedFile::partial([1; 16], "f", size, std::path::PathBuf::new(), parts.clone());
    assert!(!file.is_offerable());
    cm.restore_verified(1);
    assert!(file.is_offerable());
    assert_eq!(file.status_payload()[18], 0b10);
    assert!(file.range_available(ED2K_CHUNK_SIZE, ED2K_CHUNK_SIZE + 10));
    assert!(file.hashset().is_none());
}

async fn expect_op(
    stream: &mut tokio::net::TcpStream,
    buf: &mut bytes::BytesMut,
    opcode: u8,
) -> Ed2kPacket {
    loop {
        let packet = next_packet(stream, buf)
            .await
            .unwrap_or_else(|| panic!("no 0x{opcode:02x} before the connection ended"));
        if packet.opcode == opcode {
            return packet;
        }
    }
}

#[tokio::test]
async fn repeated_part_requests_are_served_once() {
    let dir = tempfile::tempdir().unwrap();
    let up = uploader(4).await;
    let data = fixture(5000);
    let (handle, hash) = share_complete(&up, dir.path(), &data);
    let mut stream = raw_client(&up, [3; 16]).await;
    let mut buf = bytes::BytesMut::new();
    expect_op(&mut stream, &mut buf, OP_HELLO_ANSWER).await;
    stream
        .write_all(&build_slot_request(&hash).encode())
        .await
        .unwrap();
    expect_op(&mut stream, &mut buf, OP_SLOT_GIVEN).await;
    let request = build_request_parts(&hash, &[(0, 1000), (0, 1000)], false).unwrap();
    stream.write_all(&request.encode()).await.unwrap();
    let part = expect_op(&mut stream, &mut buf, OP_SENDING_PART).await;
    assert_eq!(&part.payload[24..], &data[..1000]);
    stream.write_all(&request.encode()).await.unwrap();
    let second = build_request_parts(&hash, &[(1000, 1500)], false).unwrap();
    stream.write_all(&second.encode()).await.unwrap();
    let part = expect_op(&mut stream, &mut buf, OP_SENDING_PART).await;
    assert_eq!(parse_sending_part_header(&part.payload).unwrap().1, 1000);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(handle.file().stats().uploaded(), 1500);
    up.shutdown().await;
}

#[tokio::test]
async fn withdrawing_a_share_revokes_the_slot_but_keeps_the_connection() {
    let dir = tempfile::tempdir().unwrap();
    let up = uploader(4).await;
    let (handle, hash) = share_complete(&up, dir.path(), &fixture(5000));
    let mut stream = raw_client(&up, [3; 16]).await;
    let mut buf = bytes::BytesMut::new();
    expect_op(&mut stream, &mut buf, OP_HELLO_ANSWER).await;
    stream
        .write_all(&build_slot_request(&hash).encode())
        .await
        .unwrap();
    expect_op(&mut stream, &mut buf, OP_SLOT_GIVEN).await;
    drop(handle);
    let request = build_request_parts(&hash, &[(0, 1000)], false).unwrap();
    stream.write_all(&request.encode()).await.unwrap();
    expect_op(&mut stream, &mut buf, OP_SLOT_TAKEN).await;
    wait_for_free_slots(&up, "slot released after the share was withdrawn").await;
    stream
        .write_all(&build_file_status_request(&hash).encode())
        .await
        .unwrap();
    expect_op(&mut stream, &mut buf, OP_FILE_REQ_ANS_NOFILE).await;
    up.shutdown().await;
}

#[tokio::test]
async fn a_second_connection_from_the_same_user_takes_over_the_slot() {
    let dir = tempfile::tempdir().unwrap();
    let up = uploader(4).await;
    let data = fixture(5000);
    let (handle, hash) = share_complete(&up, dir.path(), &data);
    let slot_request = build_slot_request(&hash).encode();
    let mut first = raw_client(&up, [3; 16]).await;
    let mut first_buf = bytes::BytesMut::new();
    expect_op(&mut first, &mut first_buf, OP_HELLO_ANSWER).await;
    first.write_all(&slot_request).await.unwrap();
    expect_op(&mut first, &mut first_buf, OP_SLOT_GIVEN).await;

    let mut second = raw_client(&up, [3; 16]).await;
    let mut second_buf = bytes::BytesMut::new();
    expect_op(&mut second, &mut second_buf, OP_HELLO_ANSWER).await;
    second.write_all(&slot_request).await.unwrap();
    expect_op(&mut second, &mut second_buf, OP_SLOT_GIVEN).await;

    let request = build_request_parts(&hash, &[(0, 1000)], false)
        .unwrap()
        .encode();
    first.write_all(&request).await.unwrap();
    loop {
        let packet = next_packet(&mut first, &mut first_buf).await.unwrap();
        assert_ne!(packet.opcode, OP_SENDING_PART);
        if packet.opcode == OP_SLOT_TAKEN {
            break;
        }
    }
    second.write_all(&request).await.unwrap();
    let part = expect_op(&mut second, &mut second_buf, OP_SENDING_PART).await;
    assert_eq!(&part.payload[24..], &data[..1000]);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(handle.file().stats().uploaded(), 1000);
    assert_eq!(up.shared.queue.lock().slots(), 1);
    up.shutdown().await;
}

#[tokio::test]
async fn an_undialable_waiter_does_not_hold_up_connected_ones() {
    let dir = tempfile::tempdir().unwrap();
    let up = uploader(1).await;
    let (_handle, hash) = share_complete(&up, dir.path(), &fixture(5000));
    let slot_request = build_slot_request(&hash).encode();
    let mut holder = raw_client(&up, [3; 16]).await;
    let mut holder_buf = bytes::BytesMut::new();
    expect_op(&mut holder, &mut holder_buf, OP_HELLO_ANSWER).await;
    holder.write_all(&slot_request).await.unwrap();
    expect_op(&mut holder, &mut holder_buf, OP_SLOT_GIVEN).await;

    let busy = Ipv4Addr::new(127, 0, 0, 2);
    let offline = queue::Waiter {
        user: [5; 16],
        file: hash,
        dial: Some(SocketAddrV4::new(busy, 4662)),
        session: None,
        seen: tokio::time::Instant::now(),
    };
    assert_eq!(
        up.shared.queue.lock().request(offline),
        queue::Admission::Queued(1)
    );
    let _guards: Vec<_> = (0..MAX_SESSIONS_PER_IP)
        .map(|_| up.shared.admit(busy).unwrap())
        .collect();

    let mut waiting = raw_client(&up, [4; 16]).await;
    let mut waiting_buf = bytes::BytesMut::new();
    expect_op(&mut waiting, &mut waiting_buf, OP_HELLO_ANSWER).await;
    waiting.write_all(&slot_request).await.unwrap();
    expect_op(&mut waiting, &mut waiting_buf, OP_EMULE_QUEUE_RANKING).await;

    holder
        .write_all(&Ed2kPacket::new(PROTO_EDONKEY, OP_CANCEL_TRANSFER, Vec::new()).encode())
        .await
        .unwrap();
    expect_op(&mut waiting, &mut waiting_buf, OP_SLOT_GIVEN).await;
    assert_eq!(up.shared.queue.lock().rank(&[5; 16]), Some(1));
    up.shutdown().await;
}

#[tokio::test]
async fn a_busy_host_does_not_hold_up_dials_to_other_waiters() {
    let dir = tempfile::tempdir().unwrap();
    let up = uploader(1).await;
    let (_handle, hash) = share_complete(&up, dir.path(), &fixture(5000));
    let mut holder = raw_client(&up, [3; 16]).await;
    let mut holder_buf = bytes::BytesMut::new();
    expect_op(&mut holder, &mut holder_buf, OP_HELLO_ANSWER).await;
    holder
        .write_all(&build_slot_request(&hash).encode())
        .await
        .unwrap();
    expect_op(&mut holder, &mut holder_buf, OP_SLOT_GIVEN).await;

    let busy = Ipv4Addr::new(127, 0, 0, 2);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    for (user, addr) in [
        ([5; 16], SocketAddrV4::new(busy, 4662)),
        ([6; 16], local_addr(&listener)),
    ] {
        up.shared.queue.lock().request(queue::Waiter {
            user,
            file: hash,
            dial: Some(addr),
            session: None,
            seen: tokio::time::Instant::now(),
        });
    }
    let _guards: Vec<_> = (0..MAX_SESSIONS_PER_IP)
        .map(|_| up.shared.admit(busy).unwrap())
        .collect();

    holder
        .write_all(&Ed2kPacket::new(PROTO_EDONKEY, OP_CANCEL_TRANSFER, Vec::new()).encode())
        .await
        .unwrap();
    let (mut stream, mut buf, _) = accept_dial(&listener).await;
    stream
        .write_all(&build_hello(true, &[6; 16], 0, 4662, 0, 0).encode())
        .await
        .unwrap();
    expect_op(&mut stream, &mut buf, OP_SLOT_GIVEN).await;
    assert_eq!(up.shared.queue.lock().rank(&[5; 16]), Some(1));
    up.shutdown().await;
}

async fn accept_dial(
    listener: &tokio::net::TcpListener,
) -> (tokio::net::TcpStream, bytes::BytesMut, HelloInfo) {
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("dial in time")
        .unwrap();
    let mut buf = bytes::BytesMut::new();
    let hello = expect_op(&mut stream, &mut buf, OP_HELLO_CLIENT).await;
    let info = parse_hello(&hello.payload, false).expect("hello with hash size prefix");
    (stream, buf, info)
}

fn local_addr(listener: &tokio::net::TcpListener) -> SocketAddrV4 {
    match listener.local_addr().unwrap() {
        std::net::SocketAddr::V4(addr) => addr,
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn server_callbacks_dial_out_and_serve_the_requester() {
    let dir = tempfile::tempdir().unwrap();
    let up = uploader(4).await;
    let (_handle, hash) = share_complete(&up, dir.path(), &fixture(5000));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    assert!(up
        .shared
        .spawn_dial(local_addr(&listener), Mode::Callback)
        .is_ok());
    let (mut stream, mut buf, info) = accept_dial(&listener).await;
    assert_eq!(
        (info.user_hash, info.port),
        (up.shared.user_hash, up.port())
    );
    stream
        .write_all(&build_hello(true, &[4; 16], 0, 4662, 0, 0).encode())
        .await
        .unwrap();
    stream
        .write_all(&build_file_status_request(&hash).encode())
        .await
        .unwrap();
    let status = expect_op(&mut stream, &mut buf, OP_FILE_STATUS).await;
    assert_eq!(status.payload, [hash.as_slice(), &[0, 0]].concat());
    up.shutdown().await;
}

fn reserve_dial_slot(up: &Ed2kUploader, user: [u8; 16], file: [u8; 16], addr: SocketAddrV4) {
    let mut queue = up.shared.queue.lock();
    let admission = queue.request(queue::Waiter {
        user,
        file,
        dial: Some(addr),
        session: Some(u64::MAX),
        seen: tokio::time::Instant::now(),
    });
    assert_eq!(admission, queue::Admission::Granted);
    queue.detach(&user);
}

#[tokio::test]
async fn dial_back_grants_the_slot_only_to_the_queued_user() {
    let dir = tempfile::tempdir().unwrap();
    let up = uploader(1).await;
    let (_handle, hash) = share_complete(&up, dir.path(), &fixture(5000));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = local_addr(&listener);

    reserve_dial_slot(&up, [6; 16], hash, addr);
    assert!(up
        .shared
        .spawn_dial(
            addr,
            Mode::Grant {
                user: [6; 16],
                file: hash
            }
        )
        .is_ok());
    let (mut stream, mut buf, _) = accept_dial(&listener).await;
    stream
        .write_all(&build_hello(true, &[6; 16], 0, 4662, 0, 0).encode())
        .await
        .unwrap();
    expect_op(&mut stream, &mut buf, OP_SLOT_GIVEN).await;
    assert_eq!(up.shared.queue.lock().slots(), 1);
    drop(stream);
    wait_for_free_slots(&up, "slot released after the peer left").await;

    reserve_dial_slot(&up, [6; 16], hash, addr);
    assert!(up
        .shared
        .spawn_dial(
            addr,
            Mode::Grant {
                user: [6; 16],
                file: hash
            }
        )
        .is_ok());
    let (mut stream, _, _) = accept_dial(&listener).await;
    stream
        .write_all(&build_hello(true, &[7; 16], 0, 4662, 0, 0).encode())
        .await
        .unwrap();
    let mut rest = Vec::new();
    let closed = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut rest)).await;
    assert!(closed.is_ok());
    assert!(Ed2kPacket::decode(&mut bytes::BytesMut::from(&rest[..]))
        .unwrap()
        .is_none_or(|p| p.opcode != OP_SLOT_GIVEN));
    wait_for_free_slots(&up, "slot released after a hash mismatch").await;
    up.shutdown().await;
}

#[tokio::test]
async fn withdrawing_the_last_share_drops_the_server_login_at_once() {
    let dir = tempfile::tempdir().unwrap();
    let server = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = UploadConfig::new(new_user_hash(), 0);
    config.bind_addr = Ipv4Addr::LOCALHOST;
    config.servers = crate::server_list::server_list(&[local_addr(&server).to_string()]);
    let up = Ed2kUploader::bind(config).await.unwrap();
    let (handle, hash) = share_complete(&up, dir.path(), &fixture(5000));

    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), server.accept())
        .await
        .expect("login in time")
        .unwrap();
    let mut buf = bytes::BytesMut::new();
    expect_op(&mut stream, &mut buf, OP_HELLO_SERVER).await;
    let id_change = Ed2kPacket::new(PROTO_EDONKEY, OP_ID_CHANGE, 1234u32.to_le_bytes().to_vec());
    stream.write_all(&id_change.encode()).await.unwrap();
    let offer = loop {
        let packet = expect_op(&mut stream, &mut buf, OP_OFFER_FILES).await;
        if packet.payload[..4] != [0; 4] {
            break packet;
        }
    };
    assert_eq!(&offer.payload[..4], &1u32.to_le_bytes());
    assert_eq!(&offer.payload[4..20], &hash);
    assert_eq!(&offer.payload[20..26], &[0; 6]);

    drop(handle);
    let mut rest = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut rest))
        .await
        .expect("server login closed soon after the last share was withdrawn")
        .unwrap();
    up.shutdown().await;
}
