use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Weak};
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
use super::external_ip::{ExternalIp, Route};
use super::peer::{connect_with_utp_fallback, PeerCommand, PeerEvent, SpawnPeer};
use super::tracker::{AnnounceEvent, AnnounceRequest};
use super::wire::extended::{
    parse_ut_metadata, ut_metadata_reject, ut_metadata_request, ut_metadata_type, ExtHandshake,
    EXT_HANDSHAKE_ID,
};
use super::wire::handshake::reserved as handshake_reserved;
use super::wire::{Message, MessageEncoder};

const META_PIECE_SIZE: usize = 16 * 1024;
const MAX_METADATA_SIZE: usize = 32 * 1024 * 1024;
const MAX_INFLIGHT_METADATA: usize = 2 * MAX_METADATA_SIZE;
const MAX_PIECE_LAYER_HASHES: u64 = 1 << 21;
const MAX_INFLIGHT_PIECE_LAYERS: usize = MAX_PIECE_LAYER_HASHES as usize * 32;
const OUR_UT_METADATA_ID: u8 = 3;
const OUR_UT_PEX_ID: u8 = 4;
const TRACKER_TIMEOUT: Duration = Duration::from_secs(10);
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PEER_READ_TIMEOUT: Duration = Duration::from_secs(10);
const PEER_TOTAL_TIMEOUT: Duration = Duration::from_secs(30);
const LAYER_FETCH_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_CONCURRENT_PEERS: usize = 128;
const PEER_METADATA_WINDOW: usize = 16;
pub const ERR_PIECE_LAYERS_UNAVAILABLE: &str = "piece layers unavailable";

pub struct Resolved {
    pub info_hash: Id20,
    pub info_hash_v2: Option<Id32>,
    pub info_bytes: Vec<u8>,
    pub trackers: Vec<String>,
    pub web_seeds: Vec<String>,
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
    tracker_source: Option<SocketAddr>,
) -> Result<Resolved, String> {
    let magnet = Magnet::parse(magnet_uri).map_err(|e| e.to_string())?;
    let info_hash = magnet.info_hash();
    let want_v1 = magnet.info_hash_v1();
    let want_v2 = magnet.info_hash_v2();
    let advertise_v2 = want_v1.is_none() && want_v2.is_some();

    let own_trackers = super::torrent::split_tracker_lists(magnet.trackers.iter());
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
        let tracker_peers = own_trackers.contains(&url).then(|| tracker_peers.clone());
        let per_tracker = budget.min(TRACKER_TIMEOUT);
        let tracker_proxy = proxy.clone();
        tracker_set.spawn(async move {
            match super::tracker::announce_with_proxy_and_source(
                &url,
                &req,
                per_tracker,
                tracker_proxy.as_ref(),
                tracker_source,
            )
            .await
            {
                Ok(r) => {
                    tracing::debug!(
                        "tracker {} returned {} peers",
                        super::tracker::redacted_tracker_url(&url),
                        r.peers.len()
                    );
                    for p in r.peers {
                        if let Some(tracker_peers) = &tracker_peers {
                            if is_dialable_peer_addr(p) {
                                tracker_peers.lock().insert(p);
                            }
                        }
                        let _ = tx.send((p, PeerSource::Tracker));
                    }
                }
                Err(e) => tracing::debug!(
                    "tracker {} failed: {e}",
                    super::tracker::redacted_tracker_url(&url)
                ),
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
    send_stopped(&trackers, &req, proxy.clone(), tracker_source);
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
            let web_seeds = if info_is_private(&info_bytes) {
                Vec::new()
            } else {
                magnet.web_seeds.clone()
            };
            Ok(Resolved {
                info_hash,
                info_hash_v2: want_v2,
                info_bytes,
                trackers: own_trackers,
                web_seeds,
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
    tracker_source: Option<SocketAddr>,
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
                if let Err(e) = super::tracker::announce_with_proxy_and_source(
                    &url,
                    &stopped,
                    super::tracker::STOPPED_ANNOUNCE_TIMEOUT,
                    proxy.as_ref(),
                    tracker_source,
                )
                .await
                {
                    tracing::debug!(
                        "tracker {} stopped announce failed: {e}",
                        super::tracker::redacted_tracker_url(&url)
                    );
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
        tracker_id: None,
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

const CLOUDFLARE_V4: [([u8; 4], u32); 15] = [
    ([173, 245, 48, 0], 20),
    ([103, 21, 244, 0], 22),
    ([103, 22, 200, 0], 22),
    ([103, 31, 4, 0], 22),
    ([141, 101, 64, 0], 18),
    ([108, 162, 192, 0], 18),
    ([190, 93, 240, 0], 20),
    ([188, 114, 96, 0], 20),
    ([197, 234, 240, 0], 22),
    ([198, 41, 128, 0], 17),
    ([162, 158, 0, 0], 15),
    ([104, 16, 0, 0], 13),
    ([104, 24, 0, 0], 14),
    ([172, 64, 0, 0], 13),
    ([131, 0, 72, 0], 22),
];

fn is_cloudflare_v4(ip: std::net::Ipv4Addr) -> bool {
    let ip = u32::from(ip);
    CLOUDFLARE_V4
        .iter()
        .any(|&(net, len)| ip >> (32 - len) == u32::from_be_bytes(net) >> (32 - len))
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

fn info_is_private(info_bytes: &[u8]) -> bool {
    crate::bencode::decode_all(info_bytes)
        .is_ok_and(|info| crate::core::metainfo::info_private(&info))
}

fn info_matches_magnet(info_bytes: &[u8], want: TorrentInfoHashes) -> bool {
    let sha1_ok = want.v1.is_none_or(|v1| {
        Id20::from_slice(Sha1::digest(info_bytes).as_slice()).is_ok_and(|h| h == v1)
    });
    sha1_ok && want.v2.is_none_or(|v2| sha256(info_bytes) == v2)
}

struct ByteBudget {
    limit: usize,
    used: Mutex<usize>,
}

impl ByteBudget {
    fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: Mutex::new(0),
        })
    }

    fn reserve(&self, bytes: usize) -> bool {
        let mut used = self.used.lock();
        match used.checked_add(bytes) {
            Some(total) if total <= self.limit => {
                *used = total;
                true
            }
            _ => false,
        }
    }

    fn release(&self, bytes: usize) {
        let mut used = self.used.lock();
        *used = used.saturating_sub(bytes);
    }

    fn available(&self) -> usize {
        self.limit.saturating_sub(*self.used.lock())
    }
}

struct Assembly {
    size: usize,
    state: Mutex<AssemblyState>,
}

struct AssemblyState {
    pieces: Vec<Option<Vec<u8>>>,
    claimed: Vec<u8>,
    reserved: ReservedBytes,
}

impl Assembly {
    fn new(size: usize, budget: Arc<ByteBudget>) -> Self {
        let n = size.div_ceil(META_PIECE_SIZE);
        Self {
            size,
            state: Mutex::new(AssemblyState {
                pieces: vec![None; n],
                claimed: vec![0; n],
                reserved: ReservedBytes::new(budget),
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

    fn store(&self, idx: usize, block: &[u8]) -> bool {
        let mut st = self.state.lock();
        let AssemblyState {
            pieces, reserved, ..
        } = &mut *st;
        match pieces.get_mut(idx) {
            Some(slot) if slot.is_none() => {
                if !reserved.reserve(block.len()) {
                    return false;
                }
                *slot = Some(block.to_vec());
                true
            }
            _ => true,
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
        let held = st.reserved.held;
        st.reserved.release(held);
    }

    fn evictable_bytes(&self) -> usize {
        let held = self.state.lock().reserved.held;
        if held >= self.size {
            0
        } else {
            held
        }
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

struct MetadataShare {
    current: Mutex<Option<Arc<Assembly>>>,
    live: Mutex<Vec<Weak<Assembly>>>,
    budget: Arc<ByteBudget>,
    layer_budget: Arc<ByteBudget>,
}

impl Default for MetadataShare {
    fn default() -> Self {
        Self::with_budget(MAX_INFLIGHT_METADATA)
    }
}

impl MetadataShare {
    fn with_budget(limit: usize) -> Self {
        Self {
            current: Mutex::default(),
            live: Mutex::default(),
            budget: ByteBudget::new(limit),
            layer_budget: ByteBudget::new(MAX_INFLIGHT_PIECE_LAYERS),
        }
    }

    fn private_assembly(&self, size: usize) -> Arc<Assembly> {
        let assembly = Arc::new(Assembly::new(size, self.budget.clone()));
        let mut live = self.live.lock();
        live.retain(|other| other.strong_count() > 0);
        live.push(Arc::downgrade(&assembly));
        assembly
    }

    fn attach(&self, size: usize) -> Arc<Assembly> {
        let mut cur = self.current.lock();
        match cur.as_ref() {
            Some(a) if a.size == size => a.clone(),
            Some(_) => self.private_assembly(size),
            None => {
                let a = self.private_assembly(size);
                *cur = Some(a.clone());
                a
            }
        }
    }

    fn heaviest_other(&self, requester: &Assembly) -> Option<Arc<Assembly>> {
        self.live
            .lock()
            .iter()
            .filter_map(Weak::upgrade)
            .filter(|other| !std::ptr::eq(Arc::as_ptr(other), requester))
            .map(|other| (other.evictable_bytes(), other))
            .filter(|(held, _)| *held > 0)
            .max_by_key(|(held, _)| *held)
            .map(|(_, other)| other)
    }

    fn store(&self, assembly: &Assembly, idx: usize, block: &[u8]) -> bool {
        while !assembly.store(idx, block) {
            let Some(victim) = self.heaviest_other(assembly) else {
                return false;
            };
            victim.reset();
        }
        true
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
    let route = Route::of(proxy.as_ref());
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

    let result = try_fetch_from_peer_inner(&handle, rx, want, &share, &|hs| {
        crate::torrent::vote_yourip(ExternalIp::shared(), addr.ip(), hs, route)
    })
    .await;
    let _ = handle.tx.send(PeerCommand::Disconnect).await;
    result
}

async fn try_fetch_from_peer_inner(
    handle: &super::peer::PeerHandle,
    mut rx: tokio::sync::mpsc::Receiver<PeerEvent>,
    want: TorrentInfoHashes,
    share: &MetadataShare,
    observe: &(dyn Fn(&ExtHandshake) + Sync),
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
                announce_empty_availability(handle, &reserved).await?;
            }
            PeerEvent::Message(Message::Extended { ext_id: 0, payload }) => {
                let h = ExtHandshake::decode(&payload)?;
                if peer_supports_ext.is_none() {
                    match wait_for_handshook(&mut rx).await {
                        Some(reserved)
                            if reserved_bit(&reserved, handshake_reserved::EXT_PROTOCOL) =>
                        {
                            peer_supports_v2 = reserved_bit(&reserved, handshake_reserved::V2);
                            announce_empty_availability(handle, &reserved).await?;
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

    observe(&peer_ext);
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
                            if !share.store(&assembly, idx, &msg.block) {
                                tracing::debug!(
                                    "peer {}: metadata memory budget exhausted",
                                    handle.addr
                                );
                                return None;
                            }
                            claims.release(idx);
                        }
                    } else if msg.msg_type == ut_metadata_type::REJECT && claims.mine.contains(&idx)
                    {
                        claims.release(idx);
                        rejected.insert(idx);
                    } else if msg.msg_type == ut_metadata_type::REQUEST {
                        handle
                            .tx
                            .send(PeerCommand::Send(Message::Extended {
                                ext_id: their_ut_metadata_id,
                                payload: ut_metadata_reject(msg.piece),
                            }))
                            .await
                            .ok()?;
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
                assembly = share.private_assembly(total_size);
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
    let layers = fetch_piece_layers(handle, &mut rx, &v2, &share.layer_budget).await;
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

struct ReservedBytes {
    budget: Arc<ByteBudget>,
    held: usize,
}

impl ReservedBytes {
    fn new(budget: Arc<ByteBudget>) -> Self {
        Self { budget, held: 0 }
    }

    fn reserve(&mut self, bytes: usize) -> bool {
        let granted = self.budget.reserve(bytes);
        if granted {
            self.held += bytes;
        }
        granted
    }

    fn release(&mut self, bytes: usize) {
        let bytes = bytes.min(self.held);
        self.budget.release(bytes);
        self.held -= bytes;
    }
}

impl Drop for ReservedBytes {
    fn drop(&mut self) {
        self.budget.release(self.held);
    }
}

struct LayerFetch {
    file_len: u64,
    piece_count: u32,
    pending: Vec<(u32, u32)>,
    chunks: BTreeMap<u32, Vec<u8>>,
}

impl LayerFetch {
    fn new(file_len: u64, piece_count: u32) -> Self {
        Self {
            file_len,
            piece_count,
            pending: super::core::merkle::piece_layer_requests(piece_count),
            chunks: BTreeMap::new(),
        }
    }

    fn held(&self) -> usize {
        self.chunks.values().map(Vec::len).sum()
    }

    fn restart(&mut self) -> usize {
        self.chunks.clear();
        self.pending = super::core::merkle::piece_layer_requests(self.piece_count);
        self.pending
            .iter()
            .map(|&(_, length)| length as usize * 32)
            .sum()
    }
}

async fn send_layer_requests(
    handle: &super::peer::PeerHandle,
    base_layer: u32,
    wanted: &BTreeMap<Id32, LayerFetch>,
) -> bool {
    for (root, fetch) in wanted {
        for &(index, length) in &fetch.pending {
            let req = Message::HashRequest {
                pieces_root: root.0,
                base_layer,
                index,
                length,
                proof_layers: 0,
            };
            if handle.tx.send(PeerCommand::Send(req)).await.is_err() {
                return false;
            }
        }
    }
    true
}

async fn fetch_piece_layers(
    handle: &super::peer::PeerHandle,
    rx: &mut tokio::sync::mpsc::Receiver<PeerEvent>,
    v2: &ValidatedTorrentMetaV2Info,
    budget: &Arc<ByteBudget>,
) -> Option<BTreeMap<Id32, Vec<u8>>> {
    use super::core::merkle::BLOCK_SIZE;
    use tokio::sync::mpsc::error::TryRecvError;

    let piece_length = v2.piece_length;
    let base_layer = (piece_length / BLOCK_SIZE).trailing_zeros();

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
        wanted.insert(f.pieces_root, LayerFetch::new(f.length, piece_count as u32));
    }
    if wanted.is_empty() {
        return Some(BTreeMap::new());
    }

    if !send_layer_requests(handle, base_layer, &wanted).await {
        return None;
    }

    let mut reserved = ReservedBytes::new(Arc::clone(budget));
    let mut starved = false;
    let mut out: BTreeMap<Id32, Vec<u8>> = BTreeMap::new();
    let mut deadline = tokio::time::Instant::now() + LAYER_FETCH_TIMEOUT;
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
                if !reserved.reserve(hashes.len()) {
                    if starved {
                        tracing::debug!("piece layers exceed the in-flight memory budget");
                        return Some(out);
                    }
                    starved = true;
                    let mut needed = 0;
                    for (root, fetch) in &mut wanted {
                        if !out.contains_key(root) {
                            reserved.release(fetch.held());
                            needed += fetch.restart();
                        }
                    }
                    if reserved.held + needed > budget.limit {
                        tracing::debug!("piece layers exceed the in-flight memory budget");
                        return Some(out);
                    }
                    let give_up_at = tokio::time::Instant::now() + LAYER_FETCH_TIMEOUT;
                    while budget.available() < needed {
                        if tokio::time::Instant::now() >= give_up_at {
                            tracing::debug!("piece layers waited out the in-flight memory budget");
                            return Some(out);
                        }
                        tokio::time::sleep(BUDGET_POLL).await;
                        loop {
                            match rx.try_recv() {
                                Ok(PeerEvent::Disconnected { .. })
                                | Err(TryRecvError::Disconnected) => return None,
                                Ok(_) => {}
                                Err(TryRecvError::Empty) => break,
                            }
                        }
                    }
                    if !send_layer_requests(handle, base_layer, &wanted).await {
                        return None;
                    }
                    deadline = tokio::time::Instant::now() + LAYER_FETCH_TIMEOUT;
                    continue;
                }
                fetch.pending.swap_remove(pos);
                fetch.chunks.insert(index, hashes.to_vec());
                if !fetch.pending.is_empty() {
                    continue;
                }
                let received = std::mem::take(&mut fetch.chunks);
                let slices: Vec<&[u8]> = received.values().map(Vec::as_slice).collect();
                match MerkleProofTable::verify_piece_layer_chunks(
                    root,
                    fetch.file_len,
                    piece_length,
                    &slices,
                ) {
                    Ok(()) => {
                        let keep = fetch.piece_count as usize * 32;
                        let mut canonical = Vec::with_capacity(keep);
                        for chunk in received.into_values() {
                            let room = keep - canonical.len();
                            canonical.extend_from_slice(&chunk[..chunk.len().min(room)]);
                        }
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

async fn wait_for_handshook(rx: &mut tokio::sync::mpsc::Receiver<PeerEvent>) -> Option<[u8; 8]> {
    loop {
        match rx.recv().await? {
            PeerEvent::Handshook { reserved, .. } => return Some(reserved),
            PeerEvent::Disconnected { .. } => return None,
            _ => continue,
        }
    }
}

async fn announce_empty_availability(
    handle: &super::peer::PeerHandle,
    remote_reserved: &[u8; 8],
) -> Option<()> {
    if reserved_bit(remote_reserved, handshake_reserved::FAST) {
        handle
            .tx
            .send(PeerCommand::Send(Message::HaveNone))
            .await
            .ok()?;
    }
    Some(())
}

pub fn synth_torrent_bytes(
    info_bytes: &[u8],
    trackers: &[String],
    web_seeds: &[String],
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
    if !web_seeds.is_empty() {
        out.extend_from_slice(b"8:url-listl");
        for url in web_seeds {
            out.extend_from_slice(format!("{}:", url.len()).as_bytes());
            out.extend_from_slice(url.as_bytes());
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
        let a = Assembly::new(size, ByteBudget::new(usize::MAX));
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

    #[derive(Default)]
    struct MockPeer {
        garbage_piece: Option<usize>,
        fast: bool,
        ask_for_piece: Option<i64>,
        yourip: Option<std::net::IpAddr>,
    }

    type CommandLog = Arc<Mutex<Vec<String>>>;

    fn metadata_peer(
        info: Vec<u8>,
        garbage_piece: Option<usize>,
    ) -> (super::super::peer::PeerHandle, mpsc::Receiver<PeerEvent>) {
        let (handle, rx, _) = metadata_peer_with(
            info,
            MockPeer {
                garbage_piece,
                ..Default::default()
            },
        );
        (handle, rx)
    }

    fn metadata_peer_with(
        info: Vec<u8>,
        mock: MockPeer,
    ) -> (
        super::super::peer::PeerHandle,
        mpsc::Receiver<PeerEvent>,
        CommandLog,
    ) {
        use super::super::wire::extended::ut_metadata_data;
        let (tx, mut cmd_rx) = mpsc::channel::<PeerCommand>(64);
        let (event_tx, rx) = mpsc::channel(64);
        let log: CommandLog = Arc::default();
        let task_log = log.clone();
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
            if mock.fast {
                reserved[handshake_reserved::FAST.0] |= handshake_reserved::FAST.1;
            }
            let _ = event_tx
                .send(PeerEvent::Handshook {
                    peer_id: Id20([7u8; 20]),
                    reserved,
                    info_hash: Id20([8u8; 20]),
                    encrypted: false,
                    utp: false,
                })
                .await;
            let mut hs = ExtHandshake::new_outgoing(2, 3, Some(info.len() as u64));
            hs.yourip = mock.yourip;
            let _ = event_tx
                .send(PeerEvent::Message(Message::Extended {
                    ext_id: EXT_HANDSHAKE_ID,
                    payload: hs.encode(),
                }))
                .await;
            if let Some(piece) = mock.ask_for_piece {
                let _ = event_tx
                    .send(PeerEvent::Message(Message::Extended {
                        ext_id: OUR_UT_METADATA_ID,
                        payload: ut_metadata_request(piece),
                    }))
                    .await;
            }
            while let Some(cmd) = cmd_rx.recv().await {
                let PeerCommand::Send(msg) = cmd else {
                    continue;
                };
                let Message::Extended { ext_id: 2, payload } = msg else {
                    if matches!(msg, Message::HaveNone) {
                        task_log.lock().push("have_none".into());
                    }
                    continue;
                };
                let Some(msg) = parse_ut_metadata(payload) else {
                    continue;
                };
                if msg.msg_type == ut_metadata_type::REJECT {
                    task_log.lock().push(format!("reject:{}", msg.piece));
                    continue;
                }
                task_log.lock().push(format!("request:{}", msg.piece));
                let idx = msg.piece as usize;
                let start = idx * META_PIECE_SIZE;
                let mut block = info[start..(start + META_PIECE_SIZE).min(info.len())].to_vec();
                if mock.garbage_piece == Some(idx) {
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
        (handle, rx, log)
    }

    #[test]
    fn poisoned_assembly_is_a_mismatch_until_refilled() {
        let info = filler_info();
        let want = hashes_of(&info);
        let a = Assembly::new(info.len(), ByteBudget::new(usize::MAX));
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
        let got = try_fetch_from_peer_inner(&handle, rx, want, &share, &|_| {}).await;
        assert_eq!(
            got.map(|(bytes, _, complete)| (bytes, complete)),
            Some((info, true))
        );
    }

    #[tokio::test]
    async fn the_peers_yourip_reaches_the_observer_before_metadata_is_requested() {
        let info = filler_info();
        let want = hashes_of(&info);
        let share = MetadataShare::default();
        let seen = std::net::IpAddr::from([93, 184, 216, 34]);
        let external = ExternalIp::new();
        let (handle, rx, _) = metadata_peer_with(
            info.clone(),
            MockPeer {
                yourip: Some(seen),
                ..Default::default()
            },
        );
        let got = tokio::time::timeout(
            Duration::from_secs(10),
            try_fetch_from_peer_inner(&handle, rx, want, &share, &|hs| {
                crate::torrent::vote_yourip(
                    &external,
                    std::net::IpAddr::from([23, 1, 0, 9]),
                    hs,
                    Route::Direct,
                )
            }),
        )
        .await
        .expect("fetch did not finish");
        assert!(got.is_some());
        assert_eq!(external.external_v4(), None);
        for octet in 2..=3u8 {
            external.vote(
                seen,
                crate::external_ip::Source::Peer,
                std::net::IpAddr::from([23, octet, 0, 9]),
                Route::Direct,
            );
        }
        assert_eq!(
            external.external_v4(),
            Some("93.184.216.34".parse().unwrap())
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
            try_fetch_from_peer_inner(&handle, rx, want, &share, &|_| {}),
        )
        .await;
        assert!(matches!(got, Ok(None)));
    }

    async fn fetch_with(mock: MockPeer) -> (Option<Vec<u8>>, CommandLog) {
        let info = filler_info();
        let want = hashes_of(&info);
        let share = MetadataShare::default();
        let (handle, rx, log) = metadata_peer_with(info, mock);
        let got = tokio::time::timeout(
            Duration::from_secs(10),
            try_fetch_from_peer_inner(&handle, rx, want, &share, &|_| {}),
        )
        .await
        .expect("fetch did not finish");
        (got.map(|(bytes, _, _)| bytes), log)
    }

    #[tokio::test]
    async fn fast_peers_hear_have_none_before_any_metadata_request() {
        let (got, log) = fetch_with(MockPeer {
            fast: true,
            ..Default::default()
        })
        .await;
        assert!(got.is_some());
        let log = log.lock().clone();
        assert_eq!(
            log.first().map(String::as_str),
            Some("have_none"),
            "{log:?}"
        );
        assert_eq!(log.iter().filter(|e| *e == "have_none").count(), 1);
    }

    #[tokio::test]
    async fn peers_without_the_fast_bit_are_not_sent_have_none() {
        let (got, log) = fetch_with(MockPeer::default()).await;
        assert!(got.is_some());
        assert!(!log.lock().iter().any(|e| e == "have_none"));
    }

    #[tokio::test]
    async fn metadata_requests_from_the_remote_are_rejected_while_resolving() {
        let (got, log) = fetch_with(MockPeer {
            ask_for_piece: Some(1),
            ..Default::default()
        })
        .await;
        assert!(got.is_some());
        tokio::time::timeout(Duration::from_secs(5), async {
            while !log.lock().iter().any(|e| e == "reject:1") {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("no reject for the remote's request");
    }

    #[test]
    fn metadata_budget_is_shared_across_assemblies_and_released_on_drop() {
        let share = MetadataShare::with_budget(META_PIECE_SIZE * 3);
        let shared = share.attach(META_PIECE_SIZE * 4);
        let liar = share.attach(META_PIECE_SIZE * 5);
        let block = vec![1u8; META_PIECE_SIZE];
        assert!(shared.store(0, &block));
        assert!(shared.store(1, &block));
        assert!(shared.store(1, &block), "a held piece costs nothing twice");
        assert!(liar.store(0, &block));
        assert!(!liar.store(1, &block));
        assert!(!shared.store(2, &block));
        drop(liar);
        assert!(shared.store(2, &block));
        shared.reset();
        let again = share.private_assembly(META_PIECE_SIZE * 3);
        assert!(again.store(0, &block) && again.store(1, &block) && again.store(2, &block));
    }

    #[test]
    fn a_late_peer_evicts_the_heaviest_hoard_instead_of_failing() {
        let share = MetadataShare::with_budget(META_PIECE_SIZE * 3);
        let block = vec![1u8; META_PIECE_SIZE];
        let big = share.attach(META_PIECE_SIZE * 5);
        let small = share.private_assembly(META_PIECE_SIZE * 4);
        assert!(share.store(&big, 0, &block) && share.store(&big, 1, &block));
        assert!(share.store(&small, 0, &block));
        let late = share.private_assembly(META_PIECE_SIZE * 2);
        assert!(share.store(&late, 0, &block));
        assert_eq!(big.evictable_bytes(), 0);
        assert_eq!(small.evictable_bytes(), META_PIECE_SIZE);
        assert_eq!(late.evictable_bytes(), META_PIECE_SIZE);
    }

    #[test]
    fn a_peer_with_nobody_to_evict_is_refused() {
        let share = MetadataShare::with_budget(META_PIECE_SIZE * 2);
        let block = vec![1u8; META_PIECE_SIZE];
        let alone = share.attach(META_PIECE_SIZE * 4);
        assert!(share.store(&alone, 0, &block) && share.store(&alone, 1, &block));
        assert!(!share.store(&alone, 2, &block));
        assert_eq!(alone.evictable_bytes(), META_PIECE_SIZE * 2);
    }

    #[test]
    fn complete_assemblies_are_never_evicted() {
        let share = MetadataShare::with_budget(META_PIECE_SIZE * 3);
        let block = vec![1u8; META_PIECE_SIZE];
        let done = share.private_assembly(META_PIECE_SIZE * 2);
        assert!(share.store(&done, 0, &block) && share.store(&done, 1, &block));
        let late = share.private_assembly(META_PIECE_SIZE * 4);
        assert!(share.store(&late, 0, &block));
        assert!(!share.store(&late, 1, &block));
        assert!(done.is_complete());
    }

    #[tokio::test]
    async fn honest_peer_fetches_while_two_sybils_hold_the_whole_budget() {
        let info = filler_info();
        let want = hashes_of(&info);
        let share = MetadataShare::with_budget(META_PIECE_SIZE * 4);
        let block = vec![1u8; META_PIECE_SIZE];
        let shared_sybil = share.attach(META_PIECE_SIZE * 8 + 1);
        let private_sybil = share.attach(META_PIECE_SIZE * 8 + 2);
        for idx in 0..2 {
            assert!(share.store(&shared_sybil, idx, &block));
            assert!(share.store(&private_sybil, idx, &block));
        }
        assert!(!shared_sybil.is_complete() && !private_sybil.is_complete());

        let (handle, rx) = metadata_peer(info.clone(), None);
        let got = tokio::time::timeout(
            Duration::from_secs(10),
            try_fetch_from_peer_inner(&handle, rx, want, &share, &|_| {}),
        )
        .await
        .expect("fetch did not finish");
        assert_eq!(got.map(|(bytes, _, _)| bytes), Some(info));
    }

    #[tokio::test]
    async fn peer_is_dropped_when_the_metadata_budget_runs_out() {
        let info = filler_info();
        let want = hashes_of(&info);
        let share = MetadataShare::with_budget(META_PIECE_SIZE);
        let (handle, rx) = metadata_peer(info, None);
        let got = tokio::time::timeout(
            Duration::from_secs(10),
            try_fetch_from_peer_inner(&handle, rx, want, &share, &|_| {}),
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
        let torrent_bytes = synth_torrent_bytes(&info_bytes, &trackers, &[], &BTreeMap::new());
        let meta = crate::parse_torrent(&torrent_bytes).unwrap();
        assert_eq!(meta.info.name, "hello");
        assert_eq!(meta.info.total_length(), 1024);
        assert_eq!(
            meta.announce.as_deref(),
            Some("http://tracker.example/announce")
        );
    }

    #[test]
    fn synth_torrent_carries_web_seeds_in_url_list() {
        let info =
            b"d6:lengthi1024e4:name5:hello12:piece lengthi1024e6:pieces20:aaaaaaaaaaaaaaaaaaaae";
        let seeds = vec![
            "http://seed.example/files/".to_string(),
            "https://other.example/hello".to_string(),
        ];
        let bytes = synth_torrent_bytes(info, &[], &seeds, &BTreeMap::new());
        let meta = crate::parse_torrent(&bytes).unwrap();
        assert_eq!(meta.url_list, seeds);

        let bytes = synth_torrent_bytes(info, &[], &[], &BTreeMap::new());
        assert!(crate::parse_torrent(&bytes).unwrap().url_list.is_empty());
    }

    #[test]
    fn info_privacy_is_read_from_the_private_flag() {
        assert!(info_is_private(b"d4:name1:x7:privatei1ee"));
        assert!(info_is_private(b"d4:name1:x7:privatei2ee"));
        assert!(info_is_private(b"d4:name1:x7:privatei-1ee"));
        assert!(!info_is_private(b"d4:name1:x7:privatei0ee"));
        assert!(!info_is_private(b"d4:name1:xe"));
        assert!(!info_is_private(b"not bencode"));
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

    async fn datagrams_reaching_udp_tracker(tracker_source: Option<SocketAddr>) -> usize {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let url = format!("udp://{}/announce", socket.local_addr().unwrap());
        let magnet = format!("magnet:?xt=urn:btih:{}", "ef".repeat(20));
        let resolved = resolve_with_conn_budget(
            &magnet,
            &[url],
            &[],
            6881,
            Duration::from_millis(1500),
            crate::peer::EncryptionPolicy::PlaintextOnly,
            None,
            None,
            None,
            tracker_source,
        )
        .await;
        assert!(resolved.is_err(), "no peers, so nothing resolves");
        let mut buf = [0u8; 64];
        let mut count = 0;
        while let Ok(Ok(_)) =
            tokio::time::timeout(Duration::from_millis(100), socket.recv_from(&mut buf)).await
        {
            count += 1;
        }
        count
    }

    #[tokio::test]
    async fn magnet_tracker_announces_leave_from_the_configured_source_address() {
        assert!(datagrams_reaching_udp_tracker(None).await > 0);
        let unowned = SocketAddr::from(([192, 0, 2, 1], 0));
        assert_eq!(datagrams_reaching_udp_tracker(Some(unowned)).await, 0);
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
                None,
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
        let file_tree = Value::Dict(vec![(b"hello".to_vec(), file_leaf)]);
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

        let torrent_bytes = synth_torrent_bytes(&info_bytes, &[], &[], &BTreeMap::new());
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

    const LAYER_PIECE_LENGTH: u32 = 16 * 1024;

    fn layer_handle() -> (super::super::peer::PeerHandle, mpsc::Receiver<PeerCommand>) {
        let (tx, cmd_rx) = mpsc::channel(4096);
        let handle = super::super::peer::PeerHandle {
            addr: "127.0.0.1:6881".parse().unwrap(),
            tx,
            piece_tx: mpsc::channel(1).0,
            io_abort: tokio::spawn(async {}).abort_handle(),
            gate: Default::default(),
            bind: None,
        };
        (handle, cmd_rx)
    }

    fn layer_torrent(piece_count: usize) -> (ValidatedTorrentMetaV2Info, Vec<Id32>) {
        use crate::core::merkle::piece_layer_root;
        let pieces: Vec<Id32> = (0..piece_count as u32)
            .map(|i| sha256(&i.to_be_bytes()))
            .collect();
        let root = piece_layer_root(&pieces, LAYER_PIECE_LENGTH / 16384);
        let v2 = ValidatedTorrentMetaV2Info {
            name: "layers".into(),
            piece_length: LAYER_PIECE_LENGTH,
            private: false,
            files: vec![crate::core::metainfo::TorrentMetaInfoV2 {
                path: vec!["layers".into()],
                length: piece_count as u64 * LAYER_PIECE_LENGTH as u64,
                pieces_root: root,
                executable: false,
            }],
            empty_files: Vec::new(),
        };
        (v2, pieces)
    }

    fn layer_reply(
        v2: &ValidatedTorrentMetaV2Info,
        pieces: &[Id32],
        index: u32,
        length: u32,
    ) -> Message {
        let pad = crate::core::merkle::pad_hash(LAYER_PIECE_LENGTH / 16384);
        let hashes: Vec<u8> = (index..index + length)
            .flat_map(|i| pieces.get(i as usize).copied().unwrap_or(pad).0)
            .collect();
        Message::Hashes {
            pieces_root: v2.files[0].pieces_root.0,
            base_layer: 0,
            index,
            length,
            proof_layers: 0,
            hashes: hashes.into(),
        }
    }

    async fn answer_layer_requests(
        cmd_rx: &mut mpsc::Receiver<PeerCommand>,
        event_tx: &mpsc::Sender<PeerEvent>,
        v2: &ValidatedTorrentMetaV2Info,
        pieces: &[Id32],
        expected: usize,
        reverse: bool,
    ) {
        let mut asked = Vec::new();
        while asked.len() < expected {
            if let Some(PeerCommand::Send(Message::HashRequest { index, length, .. })) =
                cmd_rx.recv().await
            {
                asked.push((index, length));
            }
        }
        if reverse {
            asked.reverse();
        }
        for (index, length) in asked {
            let reply = layer_reply(v2, pieces, index, length);
            event_tx.send(PeerEvent::Message(reply)).await.unwrap();
        }
    }

    fn reserved_layer_bytes(budget: &ByteBudget) -> usize {
        *budget.used.lock()
    }

    #[tokio::test]
    async fn piece_layers_assemble_from_out_of_order_chunks_that_overshoot_the_piece_count() {
        use crate::core::merkle::piece_layer_requests;
        let (v2, pieces) = layer_torrent(1100);
        let expected = piece_layer_requests(1100).len();
        assert_eq!(expected, 3);
        let (handle, mut cmd_rx) = layer_handle();
        let (event_tx, mut rx) = mpsc::channel(16);
        let budget = ByteBudget::new(MAX_INFLIGHT_PIECE_LAYERS);
        let fetch = fetch_piece_layers(&handle, &mut rx, &v2, &budget);
        let serve = answer_layer_requests(&mut cmd_rx, &event_tx, &v2, &pieces, expected, true);
        let (layers, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(fetch, serve)
        })
        .await
        .expect("layer fetch did not finish");
        let layers = layers.expect("peer stayed connected");
        let canonical: Vec<u8> = pieces.iter().flat_map(|p| p.0).collect();
        assert_eq!(layers.get(&v2.files[0].pieces_root), Some(&canonical));
        assert_eq!(reserved_layer_bytes(&budget), 0);
    }

    #[tokio::test]
    async fn piece_layers_beyond_the_in_flight_budget_are_given_up_without_leaking_it() {
        let (v2, pieces) = layer_torrent(1100);
        let (handle, mut cmd_rx) = layer_handle();
        let (event_tx, mut rx) = mpsc::channel(16);
        let chunk_bytes = 512 * 32;
        let budget = ByteBudget::new(chunk_bytes * 2);
        let fetch = fetch_piece_layers(&handle, &mut rx, &v2, &budget);
        let serve = answer_layer_requests(&mut cmd_rx, &event_tx, &v2, &pieces, 3, false);
        let (layers, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(fetch, serve)
        })
        .await
        .expect("layer fetch did not finish");
        assert!(layers.is_some_and(|layers| layers.is_empty()));
        assert_eq!(reserved_layer_bytes(&budget), 0);
        assert!(budget.reserve(chunk_bytes * 2));
    }

    #[tokio::test]
    async fn tampered_piece_layers_fail_verification_and_release_their_bytes() {
        let (v2, mut pieces) = layer_torrent(5);
        let (handle, mut cmd_rx) = layer_handle();
        let (event_tx, mut rx) = mpsc::channel(16);
        let budget = ByteBudget::new(MAX_INFLIGHT_PIECE_LAYERS);
        pieces[2] = Id32([0xAA; 32]);
        let fetch = fetch_piece_layers(&handle, &mut rx, &v2, &budget);
        let serve = answer_layer_requests(&mut cmd_rx, &event_tx, &v2, &pieces, 1, false);
        let (layers, ()) = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::join!(fetch, serve)
        })
        .await
        .expect("layer fetch did not finish");
        assert!(layers.is_some_and(|layers| layers.is_empty()));
        assert_eq!(reserved_layer_bytes(&budget), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_peer_reserves_nothing_for_a_huge_declared_piece_layer() {
        let (mut v2, _) = layer_torrent(2);
        v2.files[0].length = (MAX_PIECE_LAYER_HASHES) * LAYER_PIECE_LENGTH as u64;
        let (handle, mut cmd_rx) = layer_handle();
        let (_event_tx, mut rx) = mpsc::channel(1);
        let budget = ByteBudget::new(MAX_INFLIGHT_PIECE_LAYERS);
        let fetch = fetch_piece_layers(&handle, &mut rx, &v2, &budget);
        let watch = async {
            let mut requests = 0;
            while let Some(PeerCommand::Send(Message::HashRequest { .. })) = cmd_rx.recv().await {
                requests += 1;
                assert_eq!(reserved_layer_bytes(&budget), 0);
                if requests == 4096 {
                    break;
                }
            }
            requests
        };
        let (layers, requests) = tokio::time::timeout(Duration::from_secs(120), async {
            tokio::join!(fetch, watch)
        })
        .await
        .expect("layer fetch did not finish");
        assert_eq!(requests, 4096);
        assert!(layers.is_some_and(|layers| layers.is_empty()));
        assert_eq!(reserved_layer_bytes(&budget), 0);
    }

    const LAYER_CHUNK_BYTES: usize = 512 * 32;

    #[tokio::test(start_paused = true)]
    async fn a_starved_piece_layer_fetch_waits_for_room_then_requests_again() {
        let (v2, pieces) = layer_torrent(1100);
        let (handle, mut cmd_rx) = layer_handle();
        let (event_tx, mut rx) = mpsc::channel(16);
        let budget = ByteBudget::new(LAYER_CHUNK_BYTES * 3);
        assert!(budget.reserve(LAYER_CHUNK_BYTES));
        let fetch = fetch_piece_layers(&handle, &mut rx, &v2, &budget);
        let serve = async {
            answer_layer_requests(&mut cmd_rx, &event_tx, &v2, &pieces, 3, false).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
            assert!(
                cmd_rx.try_recv().is_err(),
                "the fetch asked again before the budget had room"
            );
            budget.release(LAYER_CHUNK_BYTES);
            answer_layer_requests(&mut cmd_rx, &event_tx, &v2, &pieces, 3, true).await;
        };
        let (layers, ()) = tokio::time::timeout(Duration::from_secs(60), async {
            tokio::join!(fetch, serve)
        })
        .await
        .expect("layer fetch did not finish");
        let layers = layers.expect("peer stayed connected");
        let canonical: Vec<u8> = pieces.iter().flat_map(|p| p.0).collect();
        assert_eq!(layers.get(&v2.files[0].pieces_root), Some(&canonical));
        assert_eq!(reserved_layer_bytes(&budget), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_starved_piece_layer_fetch_gives_up_when_the_budget_never_frees() {
        let (v2, pieces) = layer_torrent(1100);
        let (handle, mut cmd_rx) = layer_handle();
        let (event_tx, mut rx) = mpsc::channel(16);
        let budget = ByteBudget::new(LAYER_CHUNK_BYTES * 3);
        assert!(budget.reserve(LAYER_CHUNK_BYTES));
        let started = tokio::time::Instant::now();
        let fetch = fetch_piece_layers(&handle, &mut rx, &v2, &budget);
        let serve = answer_layer_requests(&mut cmd_rx, &event_tx, &v2, &pieces, 3, false);
        let (layers, ()) = tokio::time::timeout(Duration::from_secs(120), async {
            tokio::join!(fetch, serve)
        })
        .await
        .expect("layer fetch did not finish");
        assert!(layers.is_some_and(|layers| layers.is_empty()));
        assert!(started.elapsed() >= LAYER_FETCH_TIMEOUT);
        assert_eq!(reserved_layer_bytes(&budget), LAYER_CHUNK_BYTES);
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_that_drops_while_the_fetch_waits_for_room_ends_the_fetch() {
        let (v2, pieces) = layer_torrent(1100);
        let (handle, mut cmd_rx) = layer_handle();
        let (event_tx, mut rx) = mpsc::channel(16);
        let budget = ByteBudget::new(LAYER_CHUNK_BYTES * 3);
        assert!(budget.reserve(LAYER_CHUNK_BYTES));
        let fetch = fetch_piece_layers(&handle, &mut rx, &v2, &budget);
        let serve = async {
            answer_layer_requests(&mut cmd_rx, &event_tx, &v2, &pieces, 3, false).await;
            event_tx
                .send(PeerEvent::Disconnected {
                    reason: "gone".into(),
                })
                .await
                .unwrap();
        };
        let (layers, ()) = tokio::time::timeout(Duration::from_secs(120), async {
            tokio::join!(fetch, serve)
        })
        .await
        .expect("layer fetch did not finish");
        assert!(layers.is_none());
        assert_eq!(reserved_layer_bytes(&budget), LAYER_CHUNK_BYTES);
    }

    #[test]
    fn a_fresh_layer_fetch_buffers_nothing_however_large_the_file_is() {
        let mut fetch = LayerFetch::new(
            MAX_PIECE_LAYER_HASHES * 16384,
            MAX_PIECE_LAYER_HASHES as u32,
        );
        assert!(fetch.chunks.is_empty());
        assert_eq!(fetch.held(), 0);
        assert_eq!(fetch.pending.len(), 4096);
        assert_eq!(fetch.restart(), MAX_INFLIGHT_PIECE_LAYERS);
        assert!(fetch.chunks.is_empty());

        let mut small = LayerFetch::new(1100 * 16384, 1100);
        small.chunks.insert(0, vec![0; LAYER_CHUNK_BYTES]);
        assert_eq!(small.held(), LAYER_CHUNK_BYTES);
        assert_eq!(small.restart(), LAYER_CHUNK_BYTES * 3);
        assert_eq!(small.held(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_huge_declared_piece_layer_reserves_only_the_bytes_that_arrive() {
        let (mut v2, pieces) = layer_torrent(2);
        v2.files[0].length = MAX_PIECE_LAYER_HASHES * LAYER_PIECE_LENGTH as u64;
        let (handle, mut cmd_rx) = layer_handle();
        let (event_tx, mut rx) = mpsc::channel(4);
        let budget = ByteBudget::new(MAX_INFLIGHT_PIECE_LAYERS);
        let fetch = fetch_piece_layers(&handle, &mut rx, &v2, &budget);
        let drive = async {
            let Some(PeerCommand::Send(Message::HashRequest { index, length, .. })) =
                cmd_rx.recv().await
            else {
                panic!("expected a hash request");
            };
            assert_eq!((index, length), (0, 512));
            let reply = layer_reply(&v2, &pieces, index, length);
            event_tx.send(PeerEvent::Message(reply)).await.unwrap();
            for _ in 0..1000 {
                if reserved_layer_bytes(&budget) != 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(reserved_layer_bytes(&budget), LAYER_CHUNK_BYTES);
        };
        let (layers, ()) = tokio::time::timeout(Duration::from_secs(120), async {
            tokio::join!(fetch, drive)
        })
        .await
        .expect("layer fetch did not finish");
        assert!(layers.is_some_and(|layers| layers.is_empty()));
        assert_eq!(reserved_layer_bytes(&budget), 0);
    }

    #[tokio::test]
    async fn oversized_piece_layers_are_refused_before_requesting() {
        let (handle, mut cmd_rx) = layer_handle();
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
                executable: false,
            }],
            empty_files: Vec::new(),
        };

        let budget = ByteBudget::new(MAX_INFLIGHT_PIECE_LAYERS);
        let layers = fetch_piece_layers(&handle, &mut rx, &v2, &budget).await;
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

    #[test]
    fn dialable_filter_drops_only_the_published_cloudflare_ranges() {
        use std::net::{Ipv4Addr, SocketAddr};
        let dialable =
            |a, b, c, d| is_dialable_peer_addr(SocketAddr::from((Ipv4Addr::new(a, b, c, d), 6881)));
        for (a, b, c, d) in [
            (173, 245, 48, 1),
            (173, 245, 63, 254),
            (141, 101, 70, 1),
            (108, 162, 255, 1),
            (188, 114, 100, 5),
            (190, 93, 240, 9),
            (197, 234, 243, 9),
            (198, 41, 200, 1),
            (104, 27, 0, 1),
            (131, 0, 75, 1),
        ] {
            assert!(!dialable(a, b, c, d), "{a}.{b}.{c}.{d} is Cloudflare");
        }
        for (a, b, c, d) in [
            (173, 245, 47, 255),
            (173, 245, 64, 0),
            (141, 101, 10, 1),
            (108, 162, 10, 1),
            (188, 114, 20, 5),
            (188, 114, 112, 0),
            (190, 93, 239, 9),
            (197, 234, 244, 9),
            (198, 41, 127, 1),
            (104, 28, 0, 1),
            (104, 31, 255, 1),
            (131, 0, 76, 1),
        ] {
            assert!(dialable(a, b, c, d), "{a}.{b}.{c}.{d} is not Cloudflare");
        }
    }
}
