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
    parse_info_v2_from_bytes, Id20, Id32, Magnet, TorrentInfoHashes, ValidatedTorrentMetaV2Info,
};
use super::dht::Dht;
use super::peer::{connect_with_utp_fallback, PeerCommand, PeerEvent, SpawnPeer};
use super::tracker::{AnnounceEvent, AnnounceRequest};
use super::wire::extended::{
    parse_ut_metadata, ut_metadata_request, ut_metadata_type, ExtHandshake, EXT_HANDSHAKE_ID,
};
use super::wire::handshake::reserved as handshake_reserved;
use super::wire::{Message, MessageEncoder};

const META_PIECE_SIZE: usize = 16 * 1024;
const MAX_METADATA_SIZE: usize = 32 * 1024 * 1024;
const MAX_PIECE_LAYER_HASHES: u64 = 1 << 21;
const OUR_UT_METADATA_ID: u8 = 3;
const OUR_UT_PEX_ID: u8 = 4;
const TRACKER_TIMEOUT: Duration = Duration::from_secs(10);
const STOPPED_TIMEOUT: Duration = Duration::from_secs(5);
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PEER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const PEER_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_CONCURRENT_PEERS: usize = 128;
const PEER_METADATA_WINDOW: usize = 16;
pub const ERR_PIECE_LAYERS_UNAVAILABLE: &str = "piece layers unavailable";

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
    resolve_with_peers_and_port_and_utp_and_proxy(
        magnet_uri,
        extra_trackers,
        extra_peers,
        DEFAULT_LISTEN_PORT,
        budget,
        encryption,
        None,
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
    resolve_with_conn_budget(
        magnet_uri,
        extra_trackers,
        extra_peers,
        listen_port,
        budget,
        encryption,
        utp,
        proxy,
        None,
    )
    .await
}

const BUDGET_POLL: Duration = Duration::from_millis(250);

#[allow(clippy::too_many_arguments)]
pub async fn resolve_with_conn_budget(
    magnet_uri: &str,
    extra_trackers: &[String],
    extra_peers: &[SocketAddr],
    listen_port: u16,
    budget: Duration,
    encryption: crate::peer::EncryptionPolicy,
    utp: Option<Arc<crate::utp::UtpSocket>>,
    proxy: Option<risuko_http::ProxyConnector>,
    conn_budget: Option<Arc<crate::conn_budget::ConnBudget>>,
) -> Result<Resolved, String> {
    let magnet = Magnet::parse(magnet_uri).map_err(|e| e.to_string())?;
    let info_hash = magnet.info_hash();
    let want_v1 = magnet.info_hash_v1();
    let want_v2 = magnet.info_hash_v2();
    let advertise_v2 = want_v1.is_none() && want_v2.is_some();

    let trackers =
        super::torrent::split_tracker_lists(magnet.trackers.iter().chain(extra_trackers.iter()));

    let our_peer_id = crate::core::peer_id::session_peer_id_or_new(listen_port);
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
    for p in extra_peers {
        let _ = peer_tx.send((*p, PeerSource::Manual));
    }
    if !magnet.peers.is_empty() {
        let tx = peer_tx.clone();
        let magnet_peers = magnet.clone();
        tokio::spawn(async move {
            use futures_util::StreamExt;
            let mut addrs = std::pin::pin!(magnet_peers.resolve_peers());
            while let Some(addr) = addrs.next().await {
                if tx.send((addr, PeerSource::Manual)).is_err() {
                    break;
                }
            }
        });
    }
    drop(peer_tx);

    type ResolvedPayload = (Vec<u8>, BTreeMap<Id32, Vec<u8>>, SocketAddr);
    let (result_tx, result_rx) = oneshot::channel::<ResolvedPayload>();
    let result_tx: Arc<Mutex<Option<oneshot::Sender<ResolvedPayload>>>> =
        Arc::new(Mutex::new(Some(result_tx)));

    let layers_failed = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_PEERS));
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

    let share = Arc::new(MetadataShare::default());
    let driver = {
        let result_tx = result_tx.clone();
        let sem = sem.clone();
        let layers_failed = layers_failed.clone();
        let utp = utp.clone();
        let proxy = proxy.clone();
        let conn_budget = conn_budget.clone();
        async move {
            let mut joinset: JoinSet<()> = JoinSet::new();

            loop {
                if result_tx.lock().is_none() {
                    break;
                }
                tokio::select! {
                    maybe = dial_rx.recv() => {
                        let Some(addr) = maybe else {
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
                        let share = share.clone();
                        let conn_budget = conn_budget.clone();
                        joinset.spawn(async move {
                            let _permit = permit;
                            let _slot = match &conn_budget {
                                Some(b) => {
                                    let Some(slot) = b
                                        .acquire_waiting(BUDGET_POLL, || result_tx.lock().is_none())
                                        .await
                                    else {
                                        return;
                                    };
                                    Some(slot)
                                }
                                None => None,
                            };
                            let fetched = tokio::time::timeout(
                                PEER_TOTAL_TIMEOUT,
                                try_fetch_from_peer(
                                    addr,
                                    info_hash,
                                    TorrentInfoHashes {
                                        v1: want_v1,
                                        v2: want_v2,
                                    },
                                    our_peer_id,
                                    encryption,
                                    advertise_v2,
                                    utp,
                                    peer_proxy,
                                    share,
                                ),
                            )
                            .await
                            .ok()
                            .flatten();
                            let Some((bytes, layers, layers_complete)) = fetched else { return };
                            if !layers_complete {
                                if can_use_v1_metadata_without_piece_layers(want_v1, &bytes) {
                                    if let Some(tx) = result_tx.lock().take() {
                                        let _ = tx.send((bytes, BTreeMap::new(), addr));
                                    }
                                    return;
                                }
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
                    }
                }
            }
        }
    };

    let overall = deadline.saturating_duration_since(Instant::now());
    let winner = tokio::select! {
        biased;
        got = result_rx => got.ok(),
        _ = driver => None,
        _ = tokio::time::sleep(overall) => None,
    };

    tracker_set.abort_all();
    while tracker_set.join_next().await.is_some() {}
    send_stopped(&trackers, &req, proxy.clone());
    if let Some(h) = dht_handle {
        h.abort();
    }
    let _ = tokio::time::timeout(Duration::from_millis(200), fan_in).await;

    let peers: Vec<SocketAddr> = discovered.lock().iter().copied().collect();

    match winner {
        Some((info_bytes, piece_layers, winner_addr)) => {
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

fn send_stopped(
    trackers: &[String],
    started: &AnnounceRequest,
    proxy: Option<risuko_http::ProxyConnector>,
) {
    if trackers.is_empty() {
        return;
    }
    let mut stopped = started.clone();
    stopped.event = AnnounceEvent::Stopped;
    stopped.num_want = 0;
    let trackers = trackers.to_vec();
    tokio::spawn(async move {
        let mut set: JoinSet<()> = JoinSet::new();
        for url in trackers {
            let stopped = stopped.clone();
            let proxy = proxy.clone();
            set.spawn(async move {
                if let Err(e) = super::tracker::announce_with_proxy(
                    &url,
                    &stopped,
                    STOPPED_TIMEOUT,
                    proxy.as_ref(),
                )
                .await
                {
                    tracing::debug!("tracker {url} stopped announce failed: {e}");
                }
            });
        }
        while set.join_next().await.is_some() {}
    });
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
        obfuscate: false,
    }
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
            // Cloudflare anycast peers always time out and burn dial slots
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
        (162, 158 | 159)
            | (172, 64..=71)
            | (104, 16..=31)
            | (173, 245)
            | (108, 162)
            | (141, 101)
            | (188, 114)
            | (190, 93)
            | (197, 234)
            | (198, 41)
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

fn info_matches_magnet(info_bytes: &[u8], want: TorrentInfoHashes) -> bool {
    let sha1_ok = want.v1.is_none_or(|v1| {
        Id20::from_slice(Sha1::digest(info_bytes).as_slice()).is_ok_and(|h| h == v1)
    });
    sha1_ok && want.v2.is_none_or(|v2| sha256(info_bytes) == v2)
}

struct Assembly {
    size: usize,
    state: Mutex<AssemblyState>,
}

struct AssemblyState {
    pieces: Vec<Option<Vec<u8>>>,
    claimed: Vec<u8>,
}

impl Assembly {
    fn new(size: usize) -> Self {
        let n = size.div_ceil(META_PIECE_SIZE);
        Self {
            size,
            state: Mutex::new(AssemblyState {
                pieces: vec![None; n],
                claimed: vec![0; n],
            }),
        }
    }

    fn num_pieces(&self) -> usize {
        self.size.div_ceil(META_PIECE_SIZE)
    }

    fn piece_len(&self, idx: usize) -> usize {
        if idx + 1 == self.num_pieces() {
            self.size - idx * META_PIECE_SIZE
        } else {
            META_PIECE_SIZE
        }
    }

    fn missing_all_in(&self, set: &HashSet<usize>) -> bool {
        let st = self.state.lock();
        st.pieces
            .iter()
            .enumerate()
            .all(|(i, p)| p.is_some() || set.contains(&i))
    }

    fn is_complete(&self) -> bool {
        self.state.lock().pieces.iter().all(Option::is_some)
    }

    fn claim(&self, n: usize, skip: &HashSet<usize>) -> Vec<usize> {
        let mut st = self.state.lock();
        let mut out = Vec::new();
        for want_unclaimed in [true, false] {
            for idx in 0..st.pieces.len() {
                if out.len() >= n {
                    break;
                }
                if st.pieces[idx].is_none()
                    && !skip.contains(&idx)
                    && !out.contains(&idx)
                    && (st.claimed[idx] == 0) == want_unclaimed
                {
                    out.push(idx);
                }
            }
        }
        for &idx in &out {
            st.claimed[idx] = st.claimed[idx].saturating_add(1);
        }
        out
    }

    fn unclaim(&self, idx: usize) {
        let mut st = self.state.lock();
        if let Some(c) = st.claimed.get_mut(idx) {
            *c = c.saturating_sub(1);
        }
    }

    fn store(&self, idx: usize, block: &[u8]) {
        let mut st = self.state.lock();
        if let Some(slot) = st.pieces.get_mut(idx) {
            if slot.is_none() {
                *slot = Some(block.to_vec());
            }
        }
    }

    fn assemble(&self) -> Option<Vec<u8>> {
        let st = self.state.lock();
        let mut out = Vec::with_capacity(self.size);
        for p in &st.pieces {
            out.extend_from_slice(p.as_ref()?);
        }
        Some(out)
    }

    fn reset(&self) {
        let mut st = self.state.lock();
        st.pieces.iter_mut().for_each(|p| *p = None);
    }
}

enum Assembled {
    Incomplete,
    Mismatch,
    Verified(Vec<u8>),
}

fn assemble_checked(assembly: &Assembly, want: TorrentInfoHashes) -> Assembled {
    match assembly.assemble() {
        None => Assembled::Incomplete,
        Some(bytes) if bytes.len() == assembly.size && info_matches_magnet(&bytes, want) => {
            Assembled::Verified(bytes)
        }
        Some(_) => Assembled::Mismatch,
    }
}

struct Claims {
    assembly: Arc<Assembly>,
    mine: HashSet<usize>,
}

impl Claims {
    fn release(&mut self, idx: usize) {
        if self.mine.remove(&idx) {
            self.assembly.unclaim(idx);
        }
    }
}

impl Drop for Claims {
    fn drop(&mut self) {
        for idx in self.mine.drain() {
            self.assembly.unclaim(idx);
        }
    }
}

#[derive(Default)]
struct MetadataShare {
    current: Mutex<Option<Arc<Assembly>>>,
}

impl MetadataShare {
    fn attach(&self, size: usize) -> Arc<Assembly> {
        let mut cur = self.current.lock();
        match cur.as_ref() {
            Some(a) if a.size == size => a.clone(),
            Some(_) => Arc::new(Assembly::new(size)),
            None => {
                let a = Arc::new(Assembly::new(size));
                *cur = Some(a.clone());
                a
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn try_fetch_from_peer(
    addr: SocketAddr,
    info_hash: Id20,
    want: TorrentInfoHashes,
    our_peer_id: Id20,
    encryption: crate::peer::EncryptionPolicy,
    advertise_v2: bool,
    utp: Option<Arc<crate::utp::UtpSocket>>,
    proxy: Option<risuko_http::ProxyConnector>,
    share: Arc<MetadataShare>,
) -> Option<(Vec<u8>, BTreeMap<Id32, Vec<u8>>, bool)> {
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
            deferred: false,
        },
        utp,
    )
    .await
    .ok()?;

    let result = try_fetch_from_peer_inner(&handle, rx, want, &share).await;
    let _ = handle.tx.send(PeerCommand::Disconnect).await;
    result
}

async fn try_fetch_from_peer_inner(
    handle: &super::peer::PeerHandle,
    mut rx: tokio::sync::mpsc::Receiver<PeerEvent>,
    want: TorrentInfoHashes,
    share: &MetadataShare,
) -> Option<(Vec<u8>, BTreeMap<Id32, Vec<u8>>, bool)> {
    let mut peer_supports_ext: Option<bool> = None;
    let mut peer_supports_v2 = false;
    let peer_ext = loop {
        match rx.recv().await? {
            PeerEvent::Handshook { reserved, .. } => {
                if !reserved_bit(&reserved, handshake_reserved::EXT_PROTOCOL) {
                    return None;
                }
                peer_supports_v2 = reserved_bit(&reserved, handshake_reserved::V2);
                peer_supports_ext = Some(true);
            }
            PeerEvent::Message(Message::Extended { ext_id: 0, payload }) => {
                let h = ExtHandshake::decode(&payload)?;
                if peer_supports_ext.is_none() {
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
    let mut assembly = share.attach(total_size);
    let mut private = false;
    let mut rejected: HashSet<usize> = HashSet::new();

    let info_bytes = loop {
        let mut claims = Claims {
            assembly: assembly.clone(),
            mine: HashSet::new(),
        };
        while !assembly.is_complete() {
            let room = PEER_METADATA_WINDOW.saturating_sub(claims.mine.len());
            if room > 0 {
                let skip: HashSet<usize> = claims.mine.union(&rejected).copied().collect();
                for idx in assembly.claim(room, &skip) {
                    claims.mine.insert(idx);
                    handle
                        .tx
                        .send(PeerCommand::Send(Message::Extended {
                            ext_id: their_ut_metadata_id,
                            payload: ut_metadata_request(idx as i64),
                        }))
                        .await
                        .ok()?;
                }
            }
            if claims.mine.is_empty() && assembly.missing_all_in(&rejected) {
                return None;
            }
            let event = match tokio::time::timeout(Duration::from_millis(250), rx.recv()).await {
                Ok(event) => event?,
                Err(_) => continue,
            };
            match event {
                PeerEvent::Message(Message::Extended { ext_id, payload })
                    if ext_id == OUR_UT_METADATA_ID =>
                {
                    let msg = parse_ut_metadata(payload)?;
                    let idx = msg.piece as usize;
                    if msg.msg_type == ut_metadata_type::DATA {
                        if claims.mine.contains(&idx) && msg.block.len() == assembly.piece_len(idx)
                        {
                            assembly.store(idx, &msg.block);
                            claims.release(idx);
                        }
                    } else if msg.msg_type == ut_metadata_type::REJECT && claims.mine.contains(&idx)
                    {
                        claims.release(idx);
                        rejected.insert(idx);
                    }
                }
                PeerEvent::Disconnected { .. } => return None,
                _ => continue,
            }
        }
        drop(claims);

        match assemble_checked(&assembly, want) {
            Assembled::Verified(bytes) => break bytes,
            Assembled::Incomplete => {}
            Assembled::Mismatch if !private => {
                tracing::debug!(
                    "peer {}: info hash mismatch; refetching without shared blocks",
                    handle.addr
                );
                assembly.reset();
                assembly = Arc::new(Assembly::new(total_size));
                private = true;
            }
            Assembled::Mismatch => {
                tracing::debug!("peer {}: info hash mismatch", handle.addr);
                return None;
            }
        }
    };

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

async fn fetch_piece_layers(
    handle: &super::peer::PeerHandle,
    rx: &mut tokio::sync::mpsc::Receiver<PeerEvent>,
    v2: &ValidatedTorrentMetaV2Info,
) -> Option<BTreeMap<Id32, Vec<u8>>> {
    use super::core::merkle::{pad_hash, piece_layer_requests, BLOCK_SIZE};

    let piece_length = v2.piece_length;
    let base_layer = (piece_length / BLOCK_SIZE).trailing_zeros();

    struct LayerFetch {
        file_len: u64,
        pending: Vec<(u32, u32)>,
        layer: Vec<u8>,
    }

    let pad = pad_hash(piece_length / BLOCK_SIZE);
    let mut wanted: BTreeMap<Id32, LayerFetch> = BTreeMap::new();
    let mut total_hashes = 0u64;
    for f in &v2.files {
        if f.length <= piece_length as u64 || wanted.contains_key(&f.pieces_root) {
            continue;
        }
        let piece_count = f.length.div_ceil(piece_length as u64);
        let padded = piece_count
            .max(2)
            .checked_next_power_of_two()
            .unwrap_or(u64::MAX);
        total_hashes = total_hashes.saturating_add(padded);
        if total_hashes > MAX_PIECE_LAYER_HASHES {
            tracing::debug!("piece layers exceed {MAX_PIECE_LAYER_HASHES} hashes; not fetching");
            return Some(BTreeMap::new());
        }
        wanted.insert(
            f.pieces_root,
            LayerFetch {
                file_len: f.length,
                pending: piece_layer_requests(piece_count as u32),
                layer: pad.0.repeat(padded as usize),
            },
        );
    }
    if wanted.is_empty() {
        return Some(BTreeMap::new());
    }

    for (root, fetch) in &wanted {
        for &(index, length) in &fetch.pending {
            let req = Message::HashRequest {
                pieces_root: root.0,
                base_layer,
                index,
                length,
                proof_layers: 0,
            };
            if handle.tx.send(PeerCommand::Send(req)).await.is_err() {
                return None;
            }
        }
    }

    let mut out: BTreeMap<Id32, Vec<u8>> = BTreeMap::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while out.len() < wanted.len() {
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
                index,
                length,
                proof_layers: 0,
                hashes,
            }) => {
                let root = Id32(pieces_root);
                if rb != base_layer || out.contains_key(&root) {
                    continue;
                }
                let Some(fetch) = wanted.get_mut(&root) else {
                    continue;
                };
                let Some(pos) = fetch
                    .pending
                    .iter()
                    .position(|&chunk| chunk == (index, length))
                else {
                    continue;
                };
                if hashes.len() != length as usize * 32 {
                    continue;
                }
                fetch.pending.swap_remove(pos);
                let start = index as usize * 32;
                fetch.layer[start..start + hashes.len()].copy_from_slice(&hashes);
                if !fetch.pending.is_empty() {
                    continue;
                }
                match MerkleProofTable::verify_full_piece_layer_response(
                    root,
                    fetch.file_len,
                    piece_length,
                    &fetch.layer,
                ) {
                    Ok(canonical) => {
                        out.insert(root, canonical);
                    }
                    Err(e) => {
                        tracing::debug!("piece-layer verify failed for {root:?}: {e}");
                        return Some(out);
                    }
                }
            }
            PeerEvent::Message(Message::HashReject { pieces_root, .. }) => {
                let root = Id32(pieces_root);
                if wanted.contains_key(&root) && !out.contains_key(&root) {
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

fn reserved_bit(reserved: &[u8; 8], (byte, mask): (usize, u8)) -> bool {
    reserved[byte] & mask != 0
}

async fn wait_for_handshook(
    rx: &mut tokio::sync::mpsc::Receiver<PeerEvent>,
) -> Option<(bool, bool)> {
    loop {
        match rx.recv().await? {
            PeerEvent::Handshook { reserved, .. } => {
                let supports_ext = reserved_bit(&reserved, handshake_reserved::EXT_PROTOCOL);
                let supports_v2 = reserved_bit(&reserved, handshake_reserved::V2);
                return Some((supports_ext, supports_v2));
            }
            PeerEvent::Disconnected { .. } => return None,
            _ => continue,
        }
    }
}

pub fn synth_torrent_bytes(
    info_bytes: &[u8],
    trackers: &[String],
    piece_layers: &BTreeMap<Id32, Vec<u8>>,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(info_bytes.len() + 64);
    out.push(b'd');
    if !trackers.is_empty() {
        let primary = trackers[0].as_bytes();
        out.extend_from_slice(b"8:announce");
        out.extend_from_slice(format!("{}:", primary.len()).as_bytes());
        out.extend_from_slice(primary);
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
    #[test]
    fn assembly_shares_pieces_and_regrants_claimed_tail() {
        let size = META_PIECE_SIZE * 2 + 10;
        let a = Assembly::new(size);
        let none = HashSet::new();
        assert_eq!(a.claim(2, &none), vec![0, 1]);
        assert_eq!(a.claim(2, &none), vec![2, 0]);
        a.store(0, &vec![1u8; META_PIECE_SIZE]);
        a.store(1, &vec![2u8; META_PIECE_SIZE]);
        assert!(!a.is_complete());
        assert_eq!(a.piece_len(2), 10);
        a.store(2, &[3u8; 10]);
        assert!(a.is_complete());
        assert_eq!(a.assemble().unwrap().len(), size);
        a.reset();
        assert!(!a.is_complete());
        let skip: HashSet<usize> = [0, 1].into();
        assert!(a.missing_all_in(&[0, 1, 2].into()));
        assert!(!a.missing_all_in(&skip));
    }

    fn filler_info() -> Vec<u8> {
        let filler = vec![b'x'; META_PIECE_SIZE + 100];
        let mut info = format!("d1:a{}:", filler.len()).into_bytes();
        info.extend_from_slice(&filler);
        info.push(b'e');
        info
    }

    fn hashes_of(info: &[u8]) -> TorrentInfoHashes {
        TorrentInfoHashes {
            v1: Some(Id20::from_slice(Sha1::digest(info).as_slice()).unwrap()),
            v2: None,
        }
    }

    fn metadata_peer(
        info: Vec<u8>,
        garbage_piece: Option<usize>,
    ) -> (super::super::peer::PeerHandle, mpsc::Receiver<PeerEvent>) {
        use super::super::wire::extended::ut_metadata_data;
        let (tx, mut cmd_rx) = mpsc::channel::<PeerCommand>(64);
        let (event_tx, rx) = mpsc::channel(64);
        let handle = super::super::peer::PeerHandle {
            addr: "127.0.0.1:6881".parse().unwrap(),
            tx,
            piece_tx: mpsc::channel(1).0,
            io_abort: tokio::spawn(async {}).abort_handle(),
            gate: Default::default(),
            bind: None,
        };
        tokio::spawn(async move {
            let mut reserved = [0u8; 8];
            reserved[handshake_reserved::EXT_PROTOCOL.0] |= handshake_reserved::EXT_PROTOCOL.1;
            let _ = event_tx
                .send(PeerEvent::Handshook {
                    peer_id: Id20([7u8; 20]),
                    reserved,
                    info_hash: Id20([8u8; 20]),
                    encrypted: false,
                    utp: false,
                })
                .await;
            let hs = ExtHandshake::new_outgoing(2, 3, Some(info.len() as u64));
            let _ = event_tx
                .send(PeerEvent::Message(Message::Extended {
                    ext_id: EXT_HANDSHAKE_ID,
                    payload: hs.encode(),
                }))
                .await;
            while let Some(cmd) = cmd_rx.recv().await {
                let PeerCommand::Send(Message::Extended { ext_id: 2, payload }) = cmd else {
                    continue;
                };
                let Some(msg) = parse_ut_metadata(payload) else {
                    continue;
                };
                let idx = msg.piece as usize;
                let start = idx * META_PIECE_SIZE;
                let mut block = info[start..(start + META_PIECE_SIZE).min(info.len())].to_vec();
                if garbage_piece == Some(idx) {
                    block.fill(0xAA);
                }
                let reply = Message::Extended {
                    ext_id: OUR_UT_METADATA_ID,
                    payload: ut_metadata_data(idx as i64, info.len() as i64, &block),
                };
                if event_tx.send(PeerEvent::Message(reply)).await.is_err() {
                    return;
                }
            }
        });
        (handle, rx)
    }

    #[test]
    fn poisoned_assembly_is_a_mismatch_until_refilled() {
        let info = filler_info();
        let want = hashes_of(&info);
        let a = Assembly::new(info.len());
        assert!(matches!(assemble_checked(&a, want), Assembled::Incomplete));
        a.store(0, &vec![0xAA; META_PIECE_SIZE]);
        a.store(1, &info[META_PIECE_SIZE..]);
        assert!(matches!(assemble_checked(&a, want), Assembled::Mismatch));
        a.reset();
        a.store(0, &info[..META_PIECE_SIZE]);
        a.store(1, &info[META_PIECE_SIZE..]);
        assert!(matches!(assemble_checked(&a, want), Assembled::Verified(b) if b == info));
    }

    #[tokio::test]
    async fn honest_peer_recovers_from_a_poisoned_shared_block() {
        let info = filler_info();
        let want = hashes_of(&info);
        let share = MetadataShare::default();
        share
            .attach(info.len())
            .store(0, &vec![0xAA; META_PIECE_SIZE]);

        let (handle, rx) = metadata_peer(info.clone(), None);
        let got = try_fetch_from_peer_inner(&handle, rx, want, &share).await;
        assert_eq!(
            got.map(|(bytes, _, complete)| (bytes, complete)),
            Some((info, true))
        );
    }

    #[tokio::test]
    async fn peer_serving_a_bad_block_is_dropped() {
        let info = filler_info();
        let want = hashes_of(&info);
        let share = MetadataShare::default();

        let (handle, rx) = metadata_peer(info, Some(0));
        let got = tokio::time::timeout(
            Duration::from_secs(10),
            try_fetch_from_peer_inner(&handle, rx, want, &share),
        )
        .await;
        assert!(matches!(got, Ok(None)));
    }

    #[test]
    fn share_gives_mismatched_size_a_private_assembly() {
        let share = MetadataShare::default();
        let a = share.attach(100);
        assert!(Arc::ptr_eq(&a, &share.attach(100)));
        assert!(!Arc::ptr_eq(&a, &share.attach(200)));
    }

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

    #[tokio::test]
    async fn metadata_attempt_sends_stopped_to_trackers_it_announced_to() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/announce", listener.local_addr().unwrap());
        let (events_tx, mut events_rx) = mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let events_tx = events_tx.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let Ok(n) = socket.read(&mut buf).await else {
                        return;
                    };
                    let head = String::from_utf8_lossy(&buf[..n]).to_string();
                    let event = head
                        .split("event=")
                        .nth(1)
                        .and_then(|rest| rest.split(['&', ' ']).next())
                        .unwrap_or("")
                        .to_string();
                    let _ = events_tx.send(event);
                    let body = b"d8:intervali600e5:peers0:e";
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                    let _ = socket.write_all(body).await;
                });
            }
        });

        let magnet = format!("magnet:?xt=urn:btih:{}", "ab".repeat(20));
        let resolved = resolve_with_peers(
            &magnet,
            &[url],
            &[],
            Duration::from_secs(2),
            crate::peer::EncryptionPolicy::PlaintextOnly,
        )
        .await;
        assert!(resolved.is_err(), "no peers, so nothing resolves");

        let mut seen = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !(seen.contains(&"started".to_string()) && seen.contains(&"stopped".to_string()))
            {
                match events_rx.recv().await {
                    Some(event) => seen.push(event),
                    None => break,
                }
            }
        })
        .await
        .unwrap_or_default();
        assert!(seen.contains(&"started".to_string()), "{seen:?}");
        assert!(seen.contains(&"stopped".to_string()), "{seen:?}");
    }

    fn spawn_resolve(
        peer: SocketAddr,
        budget: Arc<crate::conn_budget::ConnBudget>,
    ) -> tokio::task::JoinHandle<Result<Resolved, String>> {
        let magnet = format!("magnet:?xt=urn:btih:{}", "cd".repeat(20));
        tokio::spawn(async move {
            resolve_with_conn_budget(
                &magnet,
                &[],
                &[peer],
                6881,
                Duration::from_secs(20),
                crate::peer::EncryptionPolicy::PlaintextOnly,
                None,
                None,
                Some(budget),
            )
            .await
        })
    }

    #[tokio::test]
    async fn metadata_fetch_waits_for_budget_instead_of_failing() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let budget = crate::conn_budget::ConnBudget::new(10);
        let held: Vec<_> = (0..10).map(|_| budget.try_acquire().unwrap()).collect();
        assert!(budget.try_acquire().is_none());
        let task = spawn_resolve(peer, budget.clone());

        let early = tokio::time::timeout(Duration::from_millis(800), listener.accept()).await;
        assert!(early.is_err(), "dialled without a budget slot");
        assert!(!task.is_finished(), "the magnet must keep waiting");

        drop(held);
        let (_sock, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("dials once a slot frees")
            .unwrap();
        assert_eq!(budget.used(), 1, "the connection holds exactly one slot");
        task.abort();
    }

    #[tokio::test]
    async fn metadata_fetch_uses_the_reserve_when_torrents_fill_the_rest() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = listener.local_addr().unwrap();
        let budget = crate::conn_budget::ConnBudget::new(40);
        let mut lease = crate::conn_budget::BudgetLease::new(budget.clone());
        while lease.try_reserve() {}
        assert!(budget.is_full());
        let task = spawn_resolve(peer, budget.clone());
        let (_sock, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
            .await
            .expect("reserve lets the dial through")
            .unwrap();
        task.abort();
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
        assert!(meta.info_v2.is_some());
        assert!(!crate::core::supports_v2_wire(&meta));
    }

    #[test]
    fn info_must_match_every_declared_hash() {
        let info = b"d4:name5:helloe";
        let v1 = Id20::from_slice(Sha1::digest(info).as_slice()).unwrap();
        let v2 = sha256(info);
        let other_v1 = Id20([9u8; 20]);
        let other_v2 = Id32([9u8; 32]);
        let want = |v1, v2| TorrentInfoHashes { v1, v2 };

        assert!(info_matches_magnet(info, want(Some(v1), None)));
        assert!(info_matches_magnet(info, want(None, Some(v2))));
        assert!(info_matches_magnet(info, want(Some(v1), Some(v2))));
        assert!(!info_matches_magnet(info, want(Some(other_v1), None)));
        assert!(!info_matches_magnet(info, want(None, Some(other_v2))));
        assert!(!info_matches_magnet(info, want(Some(v1), Some(other_v2))));
    }

    #[tokio::test]
    async fn oversized_piece_layers_are_refused_before_requesting() {
        let (tx, mut cmd_rx) = mpsc::channel(8);
        let handle = super::super::peer::PeerHandle {
            addr: "127.0.0.1:6881".parse().unwrap(),
            tx,
            piece_tx: mpsc::channel(1).0,
            io_abort: tokio::spawn(async {}).abort_handle(),
            gate: Default::default(),
            bind: None,
        };
        let (_event_tx, mut rx) = mpsc::channel(1);
        let piece_length = 16 * 1024;
        let v2 = ValidatedTorrentMetaV2Info {
            name: "huge".into(),
            piece_length,
            private: false,
            files: vec![crate::core::metainfo::TorrentMetaInfoV2 {
                path: vec!["huge".into()],
                length: (MAX_PIECE_LAYER_HASHES + 1) * piece_length as u64,
                pieces_root: Id32([1u8; 32]),
            }],
            empty_files: Vec::new(),
        };

        let layers = fetch_piece_layers(&handle, &mut rx, &v2).await;
        assert!(layers.is_some_and(|l| l.is_empty()));
        assert!(cmd_rx.try_recv().is_err(), "no hash request may be sent");
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
