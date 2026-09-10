//! Magnet URI → info-dict resolution
use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::RngExt;
use sha1::{Digest, Sha1};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tokio::task::JoinSet;

use super::core::hash::sha256;
use super::core::merkle::MerkleProofTable;
use super::core::{
    generate_peer_id, parse_info_v2_from_bytes, Id20, Id32, Magnet, ValidatedTorrentMetaV2Info,
};
use super::dht::Dht;
use super::peer::{connect_with_utp_fallback, PeerCommand, PeerEvent, SpawnPeer};
use super::tracker::{AnnounceEvent, AnnounceRequest};
use super::wire::extended::{
    parse_ut_metadata, ut_metadata_request, ut_metadata_type, ExtHandshake, EXT_HANDSHAKE_ID,
};
use super::wire::{Message, MessageEncoder};

const META_PIECE_SIZE: usize = 16 * 1024;
const MAX_METADATA_SIZE: usize = 32 * 1024 * 1024;
const OUR_UT_METADATA_ID: u8 = 3;
const OUR_UT_PEX_ID: u8 = 4;
const TRACKER_TIMEOUT: Duration = Duration::from_secs(10);
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PEER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const PEER_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONCURRENT_PEERS: usize = 128;
pub const ERR_PIECE_LAYERS_UNAVAILABLE: &str = "piece layers unavailable";

/// Resolution result
pub struct Resolved {
    pub info_hash: Id20,
    pub info_hash_v2: Option<Id32>,
    pub info_bytes: Vec<u8>,
    pub trackers: Vec<String>,
    pub piece_layers: BTreeMap<Id32, Vec<u8>>,
    pub peers: Vec<SocketAddr>,
    pub tracker_peers: Vec<SocketAddr>,
}

const DEFAULT_LISTEN_PORT: u16 = 6881;

/// Resolve a magnet URI to its raw info dict
pub async fn resolve(
    magnet_uri: &str,
    extra_trackers: &[String],
    budget: Duration,
    encryption: crate::peer::EncryptionPolicy,
) -> Result<Resolved, String> {
    resolve_with_port(
        magnet_uri,
        extra_trackers,
        DEFAULT_LISTEN_PORT,
        budget,
        encryption,
    )
    .await
}

/// Resolve a magnet while advertising the caller's real peer-listen port
pub async fn resolve_with_port(
    magnet_uri: &str,
    extra_trackers: &[String],
    listen_port: u16,
    budget: Duration,
    encryption: crate::peer::EncryptionPolicy,
) -> Result<Resolved, String> {
    resolve_with_port_and_utp(
        magnet_uri,
        extra_trackers,
        listen_port,
        budget,
        encryption,
        None,
    )
    .await
}

pub async fn resolve_with_port_and_utp(
    magnet_uri: &str,
    extra_trackers: &[String],
    listen_port: u16,
    budget: Duration,
    encryption: crate::peer::EncryptionPolicy,
    utp: Option<Arc<crate::utp::UtpSocket>>,
) -> Result<Resolved, String> {
    resolve_with_port_and_utp_and_proxy(
        magnet_uri,
        extra_trackers,
        listen_port,
        budget,
        encryption,
        utp,
        None,
    )
    .await
}

pub async fn resolve_with_port_and_utp_and_proxy(
    magnet_uri: &str,
    extra_trackers: &[String],
    listen_port: u16,
    budget: Duration,
    encryption: crate::peer::EncryptionPolicy,
    utp: Option<Arc<crate::utp::UtpSocket>>,
    proxy: Option<risuko_http::ProxyConnector>,
) -> Result<Resolved, String> {
    resolve_with_peers_and_port_and_utp_and_proxy(
        magnet_uri,
        extra_trackers,
        &[],
        listen_port,
        budget,
        encryption,
        utp,
        proxy,
    )
    .await
}

pub async fn resolve_with_peers(
    magnet_uri: &str,
    extra_trackers: &[String],
    extra_peers: &[SocketAddr],
    budget: Duration,
    encryption: crate::peer::EncryptionPolicy,
) -> Result<Resolved, String> {
    resolve_with_peers_and_port(
        magnet_uri,
        extra_trackers,
        extra_peers,
        DEFAULT_LISTEN_PORT,
        budget,
        encryption,
    )
    .await
}

pub async fn resolve_with_peers_and_port(
    magnet_uri: &str,
    extra_trackers: &[String],
    extra_peers: &[SocketAddr],
    listen_port: u16,
    budget: Duration,
    encryption: crate::peer::EncryptionPolicy,
) -> Result<Resolved, String> {
    resolve_with_peers_and_port_and_utp(
        magnet_uri,
        extra_trackers,
        extra_peers,
        listen_port,
        budget,
        encryption,
        None,
    )
    .await
}

pub async fn resolve_with_peers_and_port_and_utp(
    magnet_uri: &str,
    extra_trackers: &[String],
    extra_peers: &[SocketAddr],
    listen_port: u16,
    budget: Duration,
    encryption: crate::peer::EncryptionPolicy,
    utp: Option<Arc<crate::utp::UtpSocket>>,
) -> Result<Resolved, String> {
    resolve_with_peers_and_port_and_utp_and_proxy(
        magnet_uri,
        extra_trackers,
        extra_peers,
        listen_port,
        budget,
        encryption,
        utp,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn resolve_with_peers_and_port_and_utp_and_proxy(
    magnet_uri: &str,
    extra_trackers: &[String],
    extra_peers: &[SocketAddr],
    listen_port: u16,
    budget: Duration,
    encryption: crate::peer::EncryptionPolicy,
    utp: Option<Arc<crate::utp::UtpSocket>>,
    proxy: Option<risuko_http::ProxyConnector>,
) -> Result<Resolved, String> {
    let magnet = Magnet::parse(magnet_uri).map_err(|e| e.to_string())?;
    let info_hash = magnet.info_hash();
    let want_v1 = magnet.info_hash_v1();
    let want_v2 = magnet.info_hash_v2();
    let advertise_v2 = want_v1.is_none() && want_v2.is_some();

    let mut trackers: Vec<String> = Vec::new();
    for t in magnet.trackers.iter().chain(extra_trackers.iter()) {
        for part in expand_tracker_entries(t) {
            if !trackers.iter().any(|x| x == &part) {
                trackers.push(part);
            }
        }
    }

    let our_peer_id = generate_peer_id();
    // Announce as a leecher (`left != 0`); `left: 0` marks us as a seeder and trackers then return other leechers, useless for metadata fetch and the peer list we hand off; size is unknown until the info dict arrives, so use a large sentinel (common BT client practice)
    let req = metadata_announce_request(info_hash, our_peer_id, listen_port);

    let deadline = Instant::now() + budget;
    let started = Instant::now();

    #[derive(Clone, Copy)]
    enum PeerSource {
        Tracker,
        Dht,
        Manual,
    }

    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel::<(SocketAddr, PeerSource)>();
    let tracker_peers: Arc<Mutex<HashSet<SocketAddr>>> = Arc::new(Mutex::new(HashSet::new()));

    // Fire off all tracker announces in parallel; each feeds peer_tx with its own bounded timeout, so a slow tracker never gates the others
    let mut tracker_set: JoinSet<()> = JoinSet::new();
    for url in trackers.clone() {
        let req = req.clone();
        let tx = peer_tx.clone();
        let tracker_peers = tracker_peers.clone();
        let per_tracker = budget.min(TRACKER_TIMEOUT);
        let tracker_proxy = proxy.clone();
        tracker_set.spawn(async move {
            match super::tracker::announce_with_proxy(
                &url,
                &req,
                per_tracker,
                tracker_proxy.as_ref(),
            )
            .await
            {
                Ok(r) => {
                    tracing::debug!("tracker {url} returned {} peers", r.peers.len());
                    for p in r.peers {
                        if is_dialable_peer_addr(p) {
                            tracker_peers.lock().insert(p);
                        }
                        let _ = tx.send((p, PeerSource::Tracker));
                    }
                }
                Err(e) => tracing::debug!("tracker {url} failed: {e}"),
            }
        });
    }

    // Fire up DHT in parallel
    let dht_handle: Option<tokio::task::JoinHandle<()>> = match Dht::current_shared().await {
        Some(dht) => {
            let tx = peer_tx.clone();
            let dht_budget = budget.min(Duration::from_secs(60));
            let mut dht_rx = dht.get_peers_stream(info_hash, dht_budget, None);
            Some(tokio::spawn(async move {
                while let Some(p) = dht_rx.recv().await {
                    if tx.send((p, PeerSource::Dht)).is_err() {
                        break;
                    }
                }
            }))
        }
        None => {
            tracing::debug!("dht unavailable for magnet resolution");
            None
        }
    };
    // Caller-supplied peers go in first so the driver can begin contacting them immediately, without waiting for any tracker / DHT round-trip
    for p in extra_peers {
        let _ = peer_tx.send((*p, PeerSource::Manual));
    }
    drop(peer_tx);

    // First successful (info, piece_layers, winner_addr) triple wins via this oneshot
    type ResolvedPayload = (Vec<u8>, BTreeMap<Id32, Vec<u8>>, SocketAddr);
    let (result_tx, result_rx) = oneshot::channel::<ResolvedPayload>();
    let result_tx: Arc<Mutex<Option<oneshot::Sender<ResolvedPayload>>>> =
        Arc::new(Mutex::new(Some(result_tx)));

    // Tracks whether *any* peer delivered the info dict but failed to furnish all required piece layers; exhausting the deadline with info-yes / layers-no lets us surface a typed error rather than a generic "no metadata" failure
    let layers_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Bound fan-out so we don't open thousands of sockets
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_PEERS));
    // Shared so we can hand discovered peers to the download after resolve; a fan-in task owns `peer_rx` and records every addr before dialing
    let discovered: Arc<Mutex<HashSet<SocketAddr>>> = Arc::new(Mutex::new(HashSet::new()));
    let (dial_tx, mut dial_rx) = mpsc::unbounded_channel::<SocketAddr>();
    let fan_discovered = discovered.clone();
    let fan_in = tokio::spawn(async move {
        while let Some((addr, _source)) = peer_rx.recv().await {
            if !is_dialable_peer_addr(addr) {
                continue;
            }
            if fan_discovered.lock().insert(addr) {
                let _ = dial_tx.send(addr);
            }
        }
    });

    // Driver: consume peer addresses and spawn bounded fetch tasks
    let driver = {
        let result_tx = result_tx.clone();
        let sem = sem.clone();
        let layers_failed = layers_failed.clone();
        let utp = utp.clone();
        let proxy = proxy.clone();
        async move {
            let mut joinset: JoinSet<()> = JoinSet::new();

            loop {
                // If a winner already published, stop spawning
                if result_tx.lock().is_none() {
                    break;
                }
                tokio::select! {
                    maybe = dial_rx.recv() => {
                        let Some(addr) = maybe else {
                            // Trackers drained; wait for in-flight to finish
                            while joinset.join_next().await.is_some() {
                                if result_tx.lock().is_none() { break; }
                            }
                            break;
                        };

                        let permit = match Arc::clone(&sem).acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => break,
                        };
                        let result_tx = result_tx.clone();
                        let layers_failed = layers_failed.clone();
                        let utp = utp.clone();
                        let peer_proxy = proxy.clone();
                        joinset.spawn(async move {
                            let _permit = permit;
                            let fetched = tokio::time::timeout(
                                PEER_TOTAL_TIMEOUT,
                                try_fetch_from_peer(
                                    addr,
                                    info_hash,
                                    our_peer_id,
                                    encryption,
                                    advertise_v2,
                                    utp,
                                    peer_proxy,
                                ),
                            )
                            .await
                            .ok()
                            .flatten();
                            let Some((bytes, layers, layers_complete)) = fetched else { return };

                            let digest = Sha1::digest(&bytes);
                            let sha1_ok = Id20::from_slice(digest.as_slice())
                                .map(|h| match want_v1 {
                                    // Hybrid / pure-v1: must match declared v1
                                    Some(v1) => h == v1,
                                    // Pure-v2 magnet: peer cannot deliver a v1 dict by definition, so trust the v2 check below and skip the v1 gate
                                    None => true,
                                })
                                .unwrap_or(false);
                            // BEP 52: pure-v2 / hybrid magnets must also pass SHA-256 cross-validation against urn:btmh (hybrid info dicts hash identically under both algorithms)
                            let sha256_ok = match want_v2 {
                                Some(v2) => sha256(&bytes) == v2,
                                None => true,
                            };
                            if !sha1_ok || !sha256_ok {
                                tracing::debug!(
                                    "peer {addr}: info hash mismatch (sha1_ok={sha1_ok} sha256_ok={sha256_ok})"
                                );
                                return;
                            }
                            if !layers_complete {
                                if can_use_v1_metadata_without_piece_layers(want_v1, &bytes) {
                                    if let Some(tx) = result_tx.lock().take() {
                                        let _ = tx.send((bytes, BTreeMap::new(), addr));
                                    }
                                    return;
                                }
                                // Hash-validated info dict but the peer could not serve every required piece layer; let another peer try
                                layers_failed.store(true, std::sync::atomic::Ordering::Relaxed);
                                tracing::debug!("peer {addr}: piece layers incomplete; will try other peers");
                                return;
                            }
                            if let Some(tx) = result_tx.lock().take() {
                                let _ = tx.send((bytes, layers, addr));
                            }
                        });
                    }
                    Some(_done) = joinset.join_next(), if !joinset.is_empty() => {
                        // Reap completed tasks; slot is implicitly freed by permit drop
                    }
                }
            }
        }
    };

    // Race the driver against the overall deadline and the oneshot winner
    let overall = deadline.saturating_duration_since(Instant::now());
    let winner = tokio::select! {
        biased;
        got = result_rx => got.ok(),
        _ = driver => None,
        _ = tokio::time::sleep(overall) => None,
    };

    tracker_set.abort_all();
    while tracker_set.join_next().await.is_some() {}
    if let Some(h) = dht_handle {
        h.abort();
    }
    // Brief wait so fan-in can record peers already in-flight before senders drop
    let _ = tokio::time::timeout(Duration::from_millis(200), fan_in).await;

    let peers: Vec<SocketAddr> = discovered.lock().iter().copied().collect();

    match winner {
        Some((info_bytes, piece_layers, winner_addr)) => {
            // Prefer the peer that already served metadata — they are a proven live contact for the subsequent download
            let mut peers = peers;
            if let Some(pos) = peers.iter().position(|a| *a == winner_addr) {
                peers.swap(0, pos);
            } else if is_dialable_peer_addr(winner_addr) {
                peers.insert(0, winner_addr);
            }
            tracing::info!(
                "Resolved magnet in {:?} ({} peers discovered, winner={winner_addr})",
                started.elapsed(),
                peers.len()
            );
            Ok(Resolved {
                info_hash,
                info_hash_v2: want_v2,
                info_bytes,
                trackers,
                piece_layers,
                peers,
                tracker_peers: tracker_peers.lock().iter().copied().collect(),
            })
        }
        None => {
            // Distinguish "no peer ever delivered the info dict" (generic metadata failure) from "every peer that delivered the info dict refused to serve piece layers" (pure-v2 specific — surface a typed error code so the UI can suggest importing a .torrent instead)
            if layers_failed.load(std::sync::atomic::Ordering::Relaxed) {
                Err(format!(
                    "{ERR_PIECE_LAYERS_UNAVAILABLE}: no peer served the BEP 52 piece-layer hashes for this magnet"
                ))
            } else {
                Err("failed to fetch metadata from any peer".into())
            }
        }
    }
}

fn metadata_announce_request(info_hash: Id20, peer_id: Id20, listen_port: u16) -> AnnounceRequest {
    AnnounceRequest {
        info_hash,
        peer_id,
        key: rand::rng().random(),
        port: listen_port,
        uploaded: 0,
        downloaded: 0,
        left: u64::MAX / 2,
        event: AnnounceEvent::Started,
        num_want: 200,
    }
}

fn expand_tracker_entries(raw: &str) -> Vec<String> {
    raw.split([',', '\n', '\r'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

pub fn is_dialable_peer_addr(addr: SocketAddr) -> bool {
    if addr.port() == 0 {
        return false;
    }
    match addr.ip() {
        std::net::IpAddr::V4(ip) => {
            if ip.is_unspecified() || ip.is_broadcast() || ip.is_multicast() || ip.is_link_local() {
                return false;
            }
            // Cloudflare anycast shows up via PEX/DHT as "peers" that always time out and burn dial slots (162.158/15, 172.64/13, …)
            !is_cloudflare_v4(ip)
        }
        std::net::IpAddr::V6(ip) => {
            !(ip.is_unspecified() || ip.is_multicast() || ip.is_unicast_link_local())
        }
    }
}

fn is_cloudflare_v4(ip: std::net::Ipv4Addr) -> bool {
    let o = ip.octets();
    matches!(
        (o[0], o[1]),
        (162, 158 | 159) // 162.158.0.0/15
            | (172, 64..=71) // 172.64.0.0/13
            | (104, 16..=31) // 104.16.0.0/12 (covers /13 published ranges)
            | (173, 245) // 173.245.48.0/20 — coarse; rare false positives OK
            | (108, 162) // 108.162.192.0/18
            | (141, 101) // 141.101.64.0/18
            | (188, 114) // 188.114.96.0/20
            | (190, 93) // 190.93.240.0/20
            | (197, 234) // 197.234.240.0/22
            | (198, 41) // 198.41.128.0/17
    ) || matches!(
        (o[0], o[1], o[2]),
        (103, 21, 244..=247) | (103, 22, 200..=203) | (103, 31, 4..=7) | (131, 0, 72..=75)
    )
}

fn can_use_v1_metadata_without_piece_layers(want_v1: Option<Id20>, info_bytes: &[u8]) -> bool {
    if want_v1.is_none() {
        return false;
    }

    let Ok(value) = crate::bencode::decode_all(info_bytes) else {
        return false;
    };
    let Some(dict) = value.as_dict() else {
        return false;
    };

    dict.iter().any(|(key, value)| {
        key == b"pieces"
            && value
                .as_bytes()
                .is_some_and(|pieces| !pieces.is_empty() && pieces.len() % Id20::LEN == 0)
    })
}

/// Outcome of a single-peer fetch attempt: raw info dict bytes, any validated piece layers, and whether the layers cover every file that requires them; `(_, _, false)` means the metadata is v2 but at least one file's layer was rejected/missing, so the caller should try another peer rather than committing this partial result
async fn try_fetch_from_peer(
    addr: SocketAddr,
    info_hash: Id20,
    our_peer_id: Id20,
    encryption: crate::peer::EncryptionPolicy,
    advertise_v2: bool,
    utp: Option<Arc<crate::utp::UtpSocket>>,
    proxy: Option<risuko_http::ProxyConnector>,
) -> Option<(Vec<u8>, BTreeMap<Id32, Vec<u8>>, bool)> {
    // Build a per-peer extended-handshake builder; the connection layer invokes it once with the peer's IP so `yourip` matches that peer (some swarms, notably CN BT clients, only engage with remotes that populate this), and metadata size is unknown until the peer replies so we leave it `None` here
    let ext_handshake_builder: crate::peer::ExtHandshakeBuilder =
        std::sync::Arc::new(|peer_ip: std::net::IpAddr| {
            let hs = ExtHandshake::new_outgoing(OUR_UT_METADATA_ID, OUR_UT_PEX_ID, None)
                .with_yourip(peer_ip);
            MessageEncoder::encode(&Message::Extended {
                ext_id: EXT_HANDSHAKE_ID,
                payload: hs.encode(),
            })
        });
    let (handle, rx) = connect_with_utp_fallback(
        SpawnPeer {
            addr,
            info_hash,
            our_peer_id,
            connect_timeout: PEER_CONNECT_TIMEOUT,
            read_timeout: PEER_READ_TIMEOUT,
            encryption,
            advertise_v2,
            advertise_dht: true,
            ext_handshake_builder: Some(ext_handshake_builder),
            proxy,
        },
        utp,
    )
    .await
    .ok()?;

    // Run the protocol inside a helper so every exit path disconnects the peer actor below; otherwise timed-out or rejected probes leak the socket and reader task
    let result = try_fetch_from_peer_inner(&handle, rx).await;
    let _ = handle.tx.send(PeerCommand::Disconnect).await;
    result
}

async fn try_fetch_from_peer_inner(
    handle: &super::peer::PeerHandle,
    mut rx: tokio::sync::mpsc::Receiver<PeerEvent>,
) -> Option<(Vec<u8>, BTreeMap<Id32, Vec<u8>>, bool)> {
    // Collect Handshook and the peer's extended handshake from a single receive loop; the extended handshake message can arrive before the BT handshake event under some orderings, and draining two sequential loops would drop whichever arrives first in the other arm
    let mut peer_supports_ext: Option<bool> = None;
    let mut peer_supports_v2 = false;
    let peer_ext = loop {
        match rx.recv().await? {
            PeerEvent::Handshook { reserved, .. } => {
                let supports = reserved[5] & 0x10 != 0;
                if !supports {
                    return None;
                }
                // BEP 52 v2 capability bit (reserved byte 7, bit 0x08)
                peer_supports_v2 = reserved[7] & 0x08 != 0;
                peer_supports_ext = Some(true);
            }
            PeerEvent::Message(Message::Extended { ext_id: 0, payload }) => {
                let h = ExtHandshake::decode(&payload)?;
                if peer_supports_ext.is_none() {
                    // Extended handshake must be preceded by Handshook with the extension bit set; keep looping until Handshook confirms support, but stash the decoded dict
                    match wait_for_handshook(&mut rx).await {
                        Some((true, v2)) => {
                            peer_supports_v2 = v2;
                            break h;
                        }
                        _ => return None,
                    }
                }
                break h;
            }
            PeerEvent::Disconnected { .. } => return None,
            _ => continue,
        }
    };

    let their_ut_metadata_id = peer_ext.ut_metadata_id()?;
    let total_size = peer_ext.metadata_size? as usize;
    if total_size == 0 || total_size > MAX_METADATA_SIZE {
        return None;
    }
    let num_pieces = total_size.div_ceil(META_PIECE_SIZE);
    let mut pieces: Vec<Option<Vec<u8>>> = vec![None; num_pieces];

    for i in 0..num_pieces {
        let payload = ut_metadata_request(i as i64);
        handle
            .tx
            .send(PeerCommand::Send(Message::Extended {
                ext_id: their_ut_metadata_id,
                payload,
            }))
            .await
            .ok()?;
    }

    let mut remaining = num_pieces;
    while remaining > 0 {
        match rx.recv().await? {
            PeerEvent::Message(Message::Extended { ext_id, payload })
                if ext_id == OUR_UT_METADATA_ID =>
            {
                let msg = parse_ut_metadata(payload)?;
                if msg.msg_type == ut_metadata_type::DATA {
                    let idx = msg.piece as usize;
                    if idx < num_pieces && pieces[idx].is_none() {
                        let expected = if idx + 1 == num_pieces {
                            total_size - idx * META_PIECE_SIZE
                        } else {
                            META_PIECE_SIZE
                        };
                        if msg.block.len() == expected {
                            pieces[idx] = Some(msg.block.to_vec());
                            remaining -= 1;
                        }
                    }
                } else if msg.msg_type == ut_metadata_type::REJECT {
                    return None;
                }
            }
            PeerEvent::Disconnected { .. } => return None,
            _ => continue,
        }
    }

    let mut info_bytes = Vec::with_capacity(total_size);
    for p in pieces {
        info_bytes.extend_from_slice(&p?);
    }
    if info_bytes.len() != total_size {
        return None;
    }

    // If the info dict is v2, attempt to fetch each file's piece layer on the same connection via BEP 52 HASH_REQUEST; a peer that has the info but cannot serve layers (HashReject / no v2 support) yields `(_, _, false)` so the driver tries another peer
    let v2 = match parse_info_v2_from_bytes(&info_bytes) {
        Ok(v) => v,
        Err(_) => return None,
    };
    let Some(v2) = v2 else {
        return Some((info_bytes, BTreeMap::new(), true));
    };
    if !peer_supports_v2 {
        tracing::debug!("peer does not advertise v2; cannot serve piece layers");
        return Some((info_bytes, BTreeMap::new(), false));
    }
    let layers = fetch_piece_layers(handle, &mut rx, &v2).await;
    let complete = layers
        .as_ref()
        .map(|m| {
            v2.files
                .iter()
                .filter(|f| f.length > v2.piece_length as u64)
                .all(|f| m.contains_key(&f.pieces_root))
        })
        .unwrap_or(false);
    Some((info_bytes, layers.unwrap_or_default(), complete))
}

/// Issue a `HASH_REQUEST` for every file's full piece layer in `v2` and collect the validated responses keyed by `pieces_root`; returns `None` only on connection-level errors (peer disappears), while a `HashReject` for any file is reported as a missing entry the caller treats as "incomplete"
async fn fetch_piece_layers(
    handle: &super::peer::PeerHandle,
    rx: &mut tokio::sync::mpsc::Receiver<PeerEvent>,
    v2: &ValidatedTorrentMetaV2Info,
) -> Option<BTreeMap<Id32, Vec<u8>>> {
    let piece_length = v2.piece_length;
    // base_layer for piece-aligned requests = log2(piece_length / 16 KiB)
    let base_layer = (piece_length / super::core::merkle::BLOCK_SIZE).trailing_zeros();

    // Group files by `pieces_root` — duplicates can appear when the same file content is referenced more than once; send one request per distinct root that requires a layer (file > piece_length)
    let mut wanted: BTreeMap<Id32, (u64, u32)> = BTreeMap::new();
    for f in &v2.files {
        if f.length <= piece_length as u64 {
            continue;
        }
        wanted
            .entry(f.pieces_root)
            .or_insert((f.length, piece_length));
    }
    if wanted.is_empty() {
        return Some(BTreeMap::new());
    }

    // Send all requests up front so the peer can pipeline its responses
    for (root, (file_len, plen)) in &wanted {
        let piece_count = file_len.div_ceil(*plen as u64) as u32;
        let length = (piece_count as usize).next_power_of_two().max(2) as u32;
        let req = Message::HashRequest {
            pieces_root: root.0,
            base_layer,
            index: 0,
            length,
            proof_layers: 0,
        };
        if handle.tx.send(PeerCommand::Send(req)).await.is_err() {
            return None;
        }
    }

    let mut out: BTreeMap<Id32, Vec<u8>> = BTreeMap::new();
    let mut remaining = wanted.len();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while remaining > 0 {
        let timeout_at = deadline.saturating_duration_since(tokio::time::Instant::now());
        if timeout_at.is_zero() {
            break;
        }
        let next = match tokio::time::timeout(timeout_at, rx.recv()).await {
            Ok(Some(ev)) => ev,
            Ok(None) | Err(_) => break,
        };
        match next {
            PeerEvent::Message(Message::Hashes {
                pieces_root,
                base_layer: rb,
                index: 0,
                length: rl,
                proof_layers: 0,
                hashes,
            }) => {
                let root = Id32(pieces_root);
                let Some((file_len, plen)) = wanted.get(&root).copied() else {
                    continue;
                };
                if rb != base_layer {
                    continue;
                }
                let piece_count = file_len.div_ceil(plen as u64) as u32;
                let expected_padded = (piece_count as usize).next_power_of_two().max(2) as u32;
                if rl != expected_padded || hashes.len() != expected_padded as usize * 32 {
                    continue;
                }
                match MerkleProofTable::verify_full_piece_layer_response(
                    root, file_len, plen, &hashes,
                ) {
                    Ok(canonical) => {
                        if out.insert(root, canonical).is_none() {
                            remaining -= 1;
                        }
                    }
                    Err(e) => {
                        tracing::debug!("piece-layer verify failed for {root:?}: {e}");
                    }
                }
            }
            PeerEvent::Message(Message::HashReject { pieces_root, .. }) => {
                let root = Id32(pieces_root);
                if wanted.contains_key(&root) && !out.contains_key(&root) {
                    // Single rejection is terminal for this peer — the caller will fall back to another seeder
                    tracing::debug!("peer rejected piece-layer request for {root:?}");
                    return Some(out);
                }
            }
            PeerEvent::Disconnected { .. } => return None,
            _ => continue,
        }
    }
    Some(out)
}

async fn wait_for_handshook(
    rx: &mut tokio::sync::mpsc::Receiver<PeerEvent>,
) -> Option<(bool, bool)> {
    loop {
        match rx.recv().await? {
            PeerEvent::Handshook { reserved, .. } => {
                let supports_ext = reserved[5] & 0x10 != 0;
                let supports_v2 = reserved[7] & 0x08 != 0;
                return Some((supports_ext, supports_v2));
            }
            PeerEvent::Disconnected { .. } => return None,
            _ => continue,
        }
    }
}

/// Build a minimal `.torrent` blob from a raw info dict, optional trackers, and (for BEP 52 v2 metadata) any piece layers fetched out-of-band via `HASH_REQUEST`; the result is suitable for feeding back into [`crate::parse_torrent`]
pub fn synth_torrent_bytes(
    info_bytes: &[u8],
    trackers: &[String],
    piece_layers: &BTreeMap<Id32, Vec<u8>>,
) -> Vec<u8> {
    // Top-level dict keys must appear in lexicographic order: announce, announce-list, info, piece layers
    let mut out = Vec::with_capacity(info_bytes.len() + 64);
    out.push(b'd');
    if !trackers.is_empty() {
        // announce: use first as primary
        let primary = trackers[0].as_bytes();
        out.extend_from_slice(b"8:announce");
        out.extend_from_slice(format!("{}:", primary.len()).as_bytes());
        out.extend_from_slice(primary);
        // announce-list: list of tiers, each a list of URLs
        out.extend_from_slice(b"13:announce-listl");
        for t in trackers {
            out.push(b'l');
            let b = t.as_bytes();
            out.extend_from_slice(format!("{}:", b.len()).as_bytes());
            out.extend_from_slice(b);
            out.push(b'e');
        }
        out.push(b'e');
    }
    out.extend_from_slice(b"4:info");
    out.extend_from_slice(info_bytes);
    if !piece_layers.is_empty() {
        // BEP 52 `piece layers` dict, keys are 32-byte SHA-256 roots; BTreeMap iteration order matches the bencode lexicographic requirement on dict keys
        out.extend_from_slice(b"12:piece layersd");
        for (root, layer) in piece_layers {
            out.extend_from_slice(b"32:");
            out.extend_from_slice(&root.0);
            out.extend_from_slice(format!("{}:", layer.len()).as_bytes());
            out.extend_from_slice(layer);
        }
        out.push(b'e');
    }
    out.push(b'e');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_announce_uses_supplied_listen_port() {
        let info_hash = Id20::from_slice(&[1u8; 20]).unwrap();
        let peer_id = Id20::from_slice(&[2u8; 20]).unwrap();
        let request = metadata_announce_request(info_hash, peer_id, 51_234);

        assert_eq!(request.port, 51_234);
        assert_eq!(request.info_hash, info_hash);
        assert_eq!(request.peer_id, peer_id);
        assert_eq!(request.event, AnnounceEvent::Started);
    }

    #[test]
    fn synth_torrent_round_trips_through_parse() {
        use crate::bencode::{encode_to_vec, Value};
        let pieces = vec![0u8; 20];
        let info = Value::Dict(vec![
            (b"length".to_vec(), Value::Int(1024)),
            (b"name".to_vec(), Value::Bytes(b"hello".to_vec())),
            (b"piece length".to_vec(), Value::Int(1024)),
            (b"pieces".to_vec(), Value::Bytes(pieces)),
        ]);
        let info_bytes = encode_to_vec(&info);
        let trackers = vec!["http://tracker.example/announce".to_string()];
        let torrent_bytes = synth_torrent_bytes(&info_bytes, &trackers, &BTreeMap::new());
        let meta = crate::parse_torrent(&torrent_bytes).unwrap();
        assert_eq!(meta.info.name, "hello");
        assert_eq!(meta.info.total_length(), 1024);
        assert_eq!(
            meta.announce.as_deref(),
            Some("http://tracker.example/announce")
        );
    }

    #[test]
    fn expand_tracker_entries_splits_newlines_and_commas() {
        let parts = expand_tracker_entries(
            "udp://a:1/announce\n\nudp://b:2/announce,http://c:3/announce\r\n",
        );
        assert_eq!(
            parts,
            vec![
                "udp://a:1/announce".to_string(),
                "udp://b:2/announce".to_string(),
                "http://c:3/announce".to_string(),
            ]
        );
    }

    #[test]
    fn v1_info_can_be_used_without_piece_layers() {
        use crate::bencode::{encode_to_vec, Value};
        let pieces = vec![0u8; 20];
        let info = Value::Dict(vec![
            (b"length".to_vec(), Value::Int(1024)),
            (b"name".to_vec(), Value::Bytes(b"hello".to_vec())),
            (b"piece length".to_vec(), Value::Int(1024)),
            (b"pieces".to_vec(), Value::Bytes(pieces)),
        ]);
        let info_bytes = encode_to_vec(&info);
        let want_v1 = Some(Id20::from_slice(&[1u8; 20]).unwrap());

        assert!(can_use_v1_metadata_without_piece_layers(
            want_v1,
            &info_bytes
        ));
        assert!(!can_use_v1_metadata_without_piece_layers(None, &info_bytes));
    }

    #[test]
    fn hybrid_info_round_trips_without_piece_layers_for_v1_download() {
        use crate::bencode::{encode_to_vec, Value};
        let length = 64 * 1024;
        let piece_length = 16 * 1024;
        let file_leaf = Value::Dict(vec![(
            Vec::new(),
            Value::Dict(vec![
                (b"length".to_vec(), Value::Int(length)),
                (b"pieces root".to_vec(), Value::Bytes(vec![1u8; 32])),
            ]),
        )]);
        let file_tree = Value::Dict(vec![(b"hello.bin".to_vec(), file_leaf)]);
        let info = Value::Dict(vec![
            (b"file tree".to_vec(), file_tree),
            (b"length".to_vec(), Value::Int(length)),
            (b"meta version".to_vec(), Value::Int(2)),
            (b"name".to_vec(), Value::Bytes(b"hello".to_vec())),
            (b"piece length".to_vec(), Value::Int(piece_length)),
            (b"pieces".to_vec(), Value::Bytes(vec![0u8; 4 * Id20::LEN])),
        ]);
        let info_bytes = encode_to_vec(&info);
        let want_v1 = Some(Id20::from_slice(&[1u8; 20]).unwrap());

        assert!(can_use_v1_metadata_without_piece_layers(
            want_v1,
            &info_bytes
        ));

        let torrent_bytes = synth_torrent_bytes(&info_bytes, &[], &BTreeMap::new());
        let meta = crate::parse_torrent(&torrent_bytes).unwrap();
        assert_eq!(meta.meta_version.as_str(), "hybrid");
        assert!(meta.piece_layers.is_empty());
        // Wire-bit advertisement still applies because the metadata carries v2 hashes, so peers that gate engagement on the V2 reserved bit see us as v2-aware; serving piece layers / announcing v2 info-hashes remains gated on `supports_v2_wire` (false here), so the runtime falls back to the v1 download path
        assert!(meta.info_v2.is_some());
        assert!(!crate::core::supports_v2_wire(&meta));
    }

    #[test]
    fn dialable_filter_drops_cloudflare_anycast() {
        use std::net::{Ipv4Addr, SocketAddr};
        let cf: SocketAddr = (Ipv4Addr::new(162, 158, 179, 174), 6881).into();
        let cf2: SocketAddr = (Ipv4Addr::new(172, 71, 219, 4), 60329).into();
        let ok: SocketAddr = (Ipv4Addr::new(111, 193, 237, 96), 34000).into();
        let loopback: SocketAddr = (Ipv4Addr::LOCALHOST, 6881).into();
        assert!(!is_dialable_peer_addr(cf));
        assert!(!is_dialable_peer_addr(cf2));
        assert!(is_dialable_peer_addr(ok));
        assert!(is_dialable_peer_addr(loopback));
    }
}
