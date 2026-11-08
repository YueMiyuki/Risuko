use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;
use tokio::time::{interval, timeout, Duration, Instant};

use risuko_util::limiter::Throttle;

use super::queue::{Admission, SessionId, Waiter};
use super::{ConnectionGuard, Shared};
use crate::protocol::*;
use crate::share::SharedFile;
use crate::types::*;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);
const IDLE_TIMEOUT: Duration = Duration::from_secs(120);
const IDLE_CHECK: Duration = Duration::from_secs(10);
const GRANT_TIMEOUT: Duration = Duration::from_secs(40);
const WRITE_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_PENDING_BLOCKS: usize = 12;
const SERVED_MEMORY: usize = 32;
const PACKET_SIZE: u64 = 10_240;

#[derive(Debug, Clone, Copy)]
pub(super) enum Mode {
    Inbound,
    Callback,
    Grant { user: [u8; 16], file: [u8; 16] },
}

type SlotId = u64;

static NEXT_SLOT: AtomicU64 = AtomicU64::new(1);

pub(super) enum SessionCmd {
    Grant([u8; 16]),
    Rotate(SlotId),
    Revoke(SlotId),
    Failed(SlotId),
}

struct Blocks {
    pending: VecDeque<(u64, u64)>,
    current: Option<(u64, u64)>,
    served: VecDeque<(u64, u64)>,
    last_progress: Instant,
}

impl Blocks {
    fn knows(&self, range: (u64, u64)) -> bool {
        self.current == Some(range) || self.pending.contains(&range) || self.served.contains(&range)
    }
}

struct BlockQueue {
    state: parking_lot::Mutex<Blocks>,
    ready: Notify,
    busy: AtomicBool,
}

impl BlockQueue {
    fn stalled(&self, now: Instant) -> bool {
        let state = self.state.lock();
        state.current.is_none()
            && state.pending.is_empty()
            && now.saturating_duration_since(state.last_progress) >= GRANT_TIMEOUT
    }
}

struct SlotRun {
    id: SlotId,
    file: Arc<SharedFile>,
    blocks: Arc<BlockQueue>,
    task: JoinHandle<()>,
}

impl Drop for SlotRun {
    fn drop(&mut self) {
        self.task.abort();
        self.file.stats().peer_left();
    }
}

struct Peer {
    user: [u8; 16],
    ext: bool,
    large_files: bool,
    dial: Option<SocketAddrV4>,
}

struct Session {
    id: SessionId,
    ip: Ipv4Addr,
    shared: Arc<Shared>,
    out: mpsc::Sender<Ed2kPacket>,
    cmd_tx: mpsc::Sender<SessionCmd>,
    peer: Option<Peer>,
    slot: Option<SlotRun>,
    pending_grant: Option<([u8; 16], [u8; 16])>,
}

pub(super) async fn run<S>(
    stream: S,
    ip: Ipv4Addr,
    shared: Arc<Shared>,
    mode: Mode,
    guard: ConnectionGuard,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let id = shared.next_session_id();
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (out, mut out_rx) = mpsc::channel::<Ed2kPacket>(8);
    let mut writer_task = tokio::spawn(async move {
        while let Some(packet) = out_rx.recv().await {
            if !matches!(
                timeout(WRITE_TIMEOUT, writer.write_all(&packet.encode())).await,
                Ok(Ok(()))
            ) {
                break;
            }
        }
        drop(out_rx);
        let _ = writer.shutdown().await;
    });
    let (cmd_tx, mut cmd_rx) = mpsc::channel(16);
    shared.sessions.lock().insert(id, cmd_tx.clone());

    let mut session = Session {
        id,
        ip,
        shared: shared.clone(),
        out,
        cmd_tx,
        peer: None,
        slot: None,
        pending_grant: match mode {
            Mode::Grant { user, file } => Some((user, file)),
            _ => None,
        },
    };

    let outbound = !matches!(mode, Mode::Inbound);
    let opened = !outbound || session.send_hello(false).await;
    if opened {
        session.drive(&mut reader, &mut cmd_rx).await;
    }

    shared.sessions.lock().remove(&id);
    drop(guard);
    session.slot = None;
    let mut released = false;
    if let Some(peer) = &session.peer {
        released = shared.queue.lock().session_closed(&peer.user, id);
    }
    if let Some((user, _)) = session.pending_grant {
        released |= shared.queue.lock().abandon_grant(&user);
    }
    if released {
        shared.dispatch();
    }
    drop(session);
    if timeout(WRITE_TIMEOUT, &mut writer_task).await.is_err() {
        writer_task.abort();
    }
}

impl Session {
    async fn drive<R: AsyncRead + Unpin>(
        &mut self,
        reader: &mut R,
        cmd_rx: &mut mpsc::Receiver<SessionCmd>,
    ) {
        let started = Instant::now();
        let mut last_rx = started;
        let mut buf = BytesMut::with_capacity(16 * 1024);
        let mut idle_check = interval(IDLE_CHECK);
        idle_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = self.shared.shutdown.cancelled() => return,
                _ = self.out.closed() => return,
                read = reader.read_buf(&mut buf) => {
                    match read {
                        Ok(0) | Err(_) => return,
                        Ok(_) => last_rx = Instant::now(),
                    }
                    loop {
                        match Ed2kPacket::decode(&mut buf) {
                            Ok(Some(packet)) => {
                                if !self.handle(packet).await {
                                    return;
                                }
                            }
                            Ok(None) => break,
                            Err(_) => return,
                        }
                    }
                }
                cmd = cmd_rx.recv() => {
                    let Some(cmd) = cmd else { return };
                    if !self.command(cmd).await {
                        return;
                    }
                }
                _ = idle_check.tick() => {
                    let now = Instant::now();
                    if self.peer.is_none() && now - started >= HANDSHAKE_TIMEOUT {
                        return;
                    }
                    if !self.check_slot(now).await {
                        return;
                    }
                    let sending = self
                        .slot
                        .as_ref()
                        .is_some_and(|slot| slot.blocks.busy.load(Ordering::Relaxed));
                    if !sending && now - last_rx >= IDLE_TIMEOUT {
                        return;
                    }
                }
            }
        }
    }

    async fn send(&self, packet: Ed2kPacket) -> bool {
        self.out.send(packet).await.is_ok()
    }

    async fn send_hello(&self, answer: bool) -> bool {
        let identity = self.shared.identity();
        let (server_ip, server_port) = identity
            .server
            .map_or((0, 0), |s| (u32::from_le_bytes(s.ip().octets()), s.port()));
        self.send(build_hello(
            answer,
            &self.shared.user_hash,
            identity.client_id,
            self.shared.port,
            server_ip,
            server_port,
        ))
        .await
    }

    fn file_hash(payload: &[u8]) -> Option<[u8; 16]> {
        payload.first_chunk::<16>().copied()
    }

    fn shareable(&self, hash: &[u8; 16]) -> Option<Arc<SharedFile>> {
        self.shared
            .registry
            .get(hash)
            .filter(|file| file.is_offerable())
    }

    async fn handle(&mut self, packet: Ed2kPacket) -> bool {
        match (packet.protocol, packet.opcode) {
            (PROTO_EDONKEY, OP_HELLO_CLIENT) => self.on_hello(&packet.payload, false).await,
            (PROTO_EDONKEY, OP_HELLO_ANSWER) => self.on_hello(&packet.payload, true).await,
            _ if self.peer.is_none() => false,
            (PROTO_EDONKEY, OP_FILE_REQUEST) => {
                let Some(hash) = Self::file_hash(&packet.payload) else {
                    return false;
                };
                match self.shareable(&hash) {
                    Some(file) => self.send(build_file_name_answer(&hash, file.name())).await,
                    None => self.send(build_no_file(&hash)).await,
                }
            }
            (PROTO_EDONKEY, OP_FILE_STATUS_REQUEST) => {
                let Some(hash) = Self::file_hash(&packet.payload) else {
                    return false;
                };
                match self.shareable(&hash) {
                    Some(file) => {
                        self.send(Ed2kPacket::new(
                            PROTO_EDONKEY,
                            OP_FILE_STATUS,
                            file.status_payload(),
                        ))
                        .await
                    }
                    None => self.send(build_no_file(&hash)).await,
                }
            }
            (PROTO_EDONKEY, OP_HASHSET_REQUEST) => {
                let Some(hash) = Self::file_hash(&packet.payload) else {
                    return false;
                };
                match self.shareable(&hash).and_then(|file| file.hashset()) {
                    Some(hashes) => self.send(build_hashset_answer(&hash, &hashes)).await,
                    None => true,
                }
            }
            (PROTO_EDONKEY, OP_SLOT_REQUEST) => {
                let Some(hash) = Self::file_hash(&packet.payload) else {
                    return false;
                };
                self.on_slot_request(hash).await
            }
            (PROTO_EDONKEY, OP_REQUEST_PARTS) => {
                self.on_request_parts(&packet.payload, false).await
            }
            (PROTO_EMULE, OP_REQUEST_PARTS_I64) => {
                self.on_request_parts(&packet.payload, true).await
            }
            (PROTO_EDONKEY, OP_CANCEL_TRANSFER) => {
                self.end_slot();
                true
            }
            (PROTO_EDONKEY, OP_END_OF_DOWNLOAD) => {
                let current = self.slot.as_ref().map(|slot| slot.file.hash());
                if current.is_some() && current == Self::file_hash(&packet.payload) {
                    self.end_slot();
                }
                true
            }
            (PROTO_EMULE, OP_PUBLIC_IP_REQ) => self.send(build_public_ip_answer(self.ip)).await,
            _ => true,
        }
    }

    async fn on_hello(&mut self, payload: &[u8], answer: bool) -> bool {
        let Some(hello) = parse_hello(payload, answer) else {
            return false;
        };
        if hello.user_hash == self.shared.user_hash {
            return false;
        }
        if let Some(peer) = &self.peer {
            if peer.user != hello.user_hash {
                return false;
            }
        }
        let dial = (is_high_id(hello.client_id) && hello.port != 0)
            .then(|| SocketAddrV4::new(self.ip, hello.port));
        self.peer = Some(Peer {
            user: hello.user_hash,
            ext: hello.caps.emule_version.is_some(),
            large_files: hello.caps.large_files(),
            dial,
        });
        if !answer {
            return self.send_hello(true).await;
        }
        let Some((user, file)) = self.pending_grant.take() else {
            return true;
        };
        if user != hello.user_hash || !self.shared.queue.lock().attach(&user, self.id) {
            if self.shared.queue.lock().abandon_grant(&user) {
                self.shared.dispatch();
            }
            return false;
        }
        self.start_slot(file).await
    }

    async fn on_slot_request(&mut self, hash: [u8; 16]) -> bool {
        let Some(file) = self.shareable(&hash) else {
            return true;
        };
        let Some(peer) = &self.peer else {
            return false;
        };
        if file.is_large() && !peer.large_files {
            return true;
        }
        let (user, ext, dial) = (peer.user, peer.ext, peer.dial);
        if let Some(slot) = &self.slot {
            if slot.file.hash() == hash {
                return self
                    .send(Ed2kPacket::new(PROTO_EDONKEY, OP_SLOT_GIVEN, Vec::new()))
                    .await;
            }
            self.end_slot();
        }
        let waiter = Waiter {
            user,
            file: hash,
            dial,
            session: Some(self.id),
            seen: Instant::now(),
        };
        let admission = self.shared.queue.lock().request(waiter);
        let ok = match admission {
            Admission::Granted => self.start_slot(hash).await,
            Admission::Queued(rank) if ext => self.send(build_queue_ranking(rank)).await,
            Admission::Queued(_) | Admission::Full => true,
        };
        self.shared.dispatch();
        ok
    }

    async fn command(&mut self, cmd: SessionCmd) -> bool {
        let Some(peer) = &self.peer else {
            return false;
        };
        match cmd {
            SessionCmd::Grant(file) => {
                if !self.shared.queue.lock().holds_slot(&peer.user, self.id) {
                    return true;
                }
                self.start_slot(file).await
            }
            SessionCmd::Rotate(id) => self.rotate(id).await,
            SessionCmd::Revoke(id) => self.revoke(id).await,
            SessionCmd::Failed(id) => {
                if !self.is_current_slot(id) {
                    return true;
                }
                self.end_slot();
                false
            }
        }
    }

    fn is_current_slot(&self, id: SlotId) -> bool {
        self.slot.as_ref().is_some_and(|slot| slot.id == id)
    }

    async fn check_slot(&mut self, now: Instant) -> bool {
        let (Some(slot), Some(peer)) = (&self.slot, &self.peer) else {
            return true;
        };
        let id = slot.id;
        let (held, expired) = {
            let mut queue = self.shared.queue.lock();
            let held = queue.holds_slot(&peer.user, self.id);
            (held, held && queue.record_sent(&peer.user, 0, now))
        };
        if !held || slot.file.is_withdrawn() || slot.blocks.stalled(now) {
            return self.revoke(id).await;
        }
        if expired {
            return self.rotate(id).await;
        }
        true
    }

    async fn revoke(&mut self, id: SlotId) -> bool {
        if !self.is_current_slot(id) {
            return true;
        }
        self.end_slot();
        self.send(Ed2kPacket::new(PROTO_EDONKEY, OP_SLOT_TAKEN, Vec::new()))
            .await
    }

    async fn rotate(&mut self, id: SlotId) -> bool {
        let (Some(slot), Some(peer)) = (&self.slot, &self.peer) else {
            return true;
        };
        if slot.id != id {
            return true;
        }
        let ext = peer.ext;
        let waiter = Waiter {
            user: peer.user,
            file: slot.file.hash(),
            dial: peer.dial,
            session: Some(self.id),
            seen: Instant::now(),
        };
        self.slot = None;
        let rank = self.shared.queue.lock().requeue(waiter);
        self.shared.dispatch();
        if !self
            .send(Ed2kPacket::new(PROTO_EDONKEY, OP_SLOT_TAKEN, Vec::new()))
            .await
        {
            return false;
        }
        match rank {
            Some(rank) if ext => self.send(build_queue_ranking(rank)).await,
            _ => true,
        }
    }

    async fn start_slot(&mut self, hash: [u8; 16]) -> bool {
        let Some(file) = self.shareable(&hash) else {
            self.end_slot();
            return true;
        };
        let Some(peer) = &self.peer else {
            return false;
        };
        let id = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
        let blocks = Arc::new(BlockQueue {
            state: parking_lot::Mutex::new(Blocks {
                pending: VecDeque::new(),
                current: None,
                served: VecDeque::new(),
                last_progress: Instant::now(),
            }),
            ready: Notify::new(),
            busy: AtomicBool::new(false),
        });
        let task = tokio::spawn(upload_loop(
            self.shared.clone(),
            id,
            self.id,
            file.clone(),
            peer.user,
            blocks.clone(),
            self.out.clone(),
            self.cmd_tx.clone(),
        ));
        file.stats().peer_joined();
        self.slot = Some(SlotRun {
            id,
            file,
            blocks,
            task,
        });
        self.send(Ed2kPacket::new(PROTO_EDONKEY, OP_SLOT_GIVEN, Vec::new()))
            .await
    }

    fn end_slot(&mut self) {
        let Some(peer) = &self.peer else {
            return;
        };
        let held = self.shared.queue.lock().holds_slot(&peer.user, self.id);
        self.slot = None;
        if held && self.shared.queue.lock().release(&peer.user) {
            self.shared.dispatch();
        }
    }

    async fn on_request_parts(&mut self, payload: &[u8], wide: bool) -> bool {
        let Some(slot) = &self.slot else {
            return true;
        };
        let Some((hash, ranges)) = parse_request_parts(payload, wide) else {
            return true;
        };
        if hash != slot.file.hash() {
            return true;
        }
        if slot.file.is_withdrawn() {
            return self.revoke(slot.id).await;
        }
        let mut blocks = slot.blocks.state.lock();
        let mut added = false;
        for (start, end) in ranges {
            if end <= start
                || blocks.pending.len() >= MAX_PENDING_BLOCKS
                || blocks.knows((start, end))
                || !slot.file.range_available(start, end)
            {
                continue;
            }
            blocks.pending.push_back((start, end));
            blocks.last_progress = Instant::now();
            added = true;
        }
        drop(blocks);
        if added {
            slot.blocks.ready.notify_one();
        }
        true
    }
}

pub(super) fn packet_spans(start: u64, end: u64) -> Vec<(u64, u64)> {
    let mut spans = Vec::new();
    let mut togo = end.saturating_sub(start);
    let mut size = if togo <= PACKET_SIZE + 2600 {
        togo
    } else {
        PACKET_SIZE
    };
    while togo > 0 {
        if togo < size * 2 {
            size = togo;
        }
        togo -= size;
        let stop = end - togo;
        spans.push((stop - size, stop));
    }
    spans
}

#[allow(clippy::too_many_arguments)]
async fn upload_loop(
    shared: Arc<Shared>,
    id: SlotId,
    session: SessionId,
    file: Arc<SharedFile>,
    user: [u8; 16],
    blocks: Arc<BlockQueue>,
    out: mpsc::Sender<Ed2kPacket>,
    cmd_tx: mpsc::Sender<SessionCmd>,
) {
    let cmd = serve_blocks(&shared, id, session, &file, &user, &blocks, &out).await;
    blocks.busy.store(false, Ordering::Relaxed);
    if let Some(cmd) = cmd {
        let _ = cmd_tx.send(cmd).await;
    }
}

async fn serve_blocks(
    shared: &Shared,
    id: SlotId,
    session: SessionId,
    file: &Arc<SharedFile>,
    user: &[u8; 16],
    blocks: &BlockQueue,
    out: &mpsc::Sender<Ed2kPacket>,
) -> Option<SessionCmd> {
    let throttle = Throttle::new(shared.upload_limit.clone(), file.limiter().clone());
    let hash = file.hash();
    loop {
        let next = {
            let mut state = blocks.state.lock();
            state.current = state.pending.pop_front();
            state.current
        };
        let Some((start, end)) = next else {
            blocks.busy.store(false, Ordering::Relaxed);
            blocks.ready.notified().await;
            continue;
        };
        if file.is_withdrawn() || !shared.queue.lock().holds_slot(user, session) {
            return Some(SessionCmd::Revoke(id));
        }
        blocks.busy.store(true, Ordering::Relaxed);
        let reader = file.clone();
        match tokio::task::spawn_blocking(move || reader.read_range(start, end)).await {
            Ok(Ok(data)) => {
                for (s, e) in packet_spans(start, end) {
                    let chunk = &data[(s - start) as usize..(e - start) as usize];
                    throttle.acquire(chunk.len()).await;
                    out.send(build_sending_part(&hash, s, chunk)).await.ok()?;
                    file.stats().add(chunk.len() as u64);
                }
            }
            Ok(Err(e)) => {
                tracing::warn!("[ed2k] Cannot read {} for upload: {e}", file.name());
                return Some(SessionCmd::Failed(id));
            }
            Err(_) => return Some(SessionCmd::Failed(id)),
        }
        {
            let mut state = blocks.state.lock();
            state.current = None;
            state.last_progress = Instant::now();
            state.served.push_back((start, end));
            if state.served.len() > SERVED_MEMORY {
                state.served.pop_front();
            }
        }
        let rotate = {
            let mut queue = shared.queue.lock();
            if !queue.holds_slot(user, session) {
                return Some(SessionCmd::Revoke(id));
            }
            queue.record_sent(user, end - start, Instant::now())
        };
        if rotate {
            return Some(SessionCmd::Rotate(id));
        }
    }
}
