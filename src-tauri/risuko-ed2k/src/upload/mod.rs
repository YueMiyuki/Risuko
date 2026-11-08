mod announce;
mod queue;
mod session;

use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{interval, timeout, Duration, Instant};
use tokio_util::sync::CancellationToken;

use risuko_util::limiter::RateLimiter;

use super::kad::KadService;
use super::server_list::ServerEntry;
use super::share::{ShareHandle, ShareRegistry, SharedFile};
use queue::{SessionId, UploadQueue, Waiter};
use session::{Mode, SessionCmd};

const MAX_UPLOAD_SLOTS: usize = 4;
const MAX_WAITING: usize = 500;
const MAX_SESSIONS: usize = 256;
const MAX_SESSIONS_PER_IP: usize = 4;
const DIAL_TIMEOUT: Duration = Duration::from_secs(15);
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(1);
const PURGE_EVERY_TICKS: u32 = 60;

pub struct UploadConfig {
    pub user_hash: [u8; 16],
    pub bind_addr: Ipv4Addr,
    pub port: u16,
    pub servers: Vec<ServerEntry>,
    pub kad: Option<Arc<KadService>>,
    pub proxy: risuko_http::ProxyConnector,
    pub upload_limit: Arc<RateLimiter>,
    pub max_slots: usize,
    pub registry: Arc<ShareRegistry>,
}

impl UploadConfig {
    pub fn new(user_hash: [u8; 16], port: u16) -> Self {
        Self {
            user_hash,
            bind_addr: Ipv4Addr::UNSPECIFIED,
            port,
            servers: Vec::new(),
            kad: None,
            proxy: risuko_http::ProxyConnector::direct(),
            upload_limit: Arc::new(RateLimiter::unlimited()),
            max_slots: MAX_UPLOAD_SLOTS,
            registry: ShareRegistry::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Identity {
    client_id: u32,
    server: Option<SocketAddrV4>,
}

struct Shared {
    registry: Arc<ShareRegistry>,
    servers: parking_lot::RwLock<Vec<ServerEntry>>,
    user_hash: [u8; 16],
    port: u16,
    identity: parking_lot::Mutex<Identity>,
    queue: parking_lot::Mutex<UploadQueue>,
    sessions: parking_lot::Mutex<HashMap<SessionId, mpsc::Sender<SessionCmd>>>,
    connections: parking_lot::Mutex<HashMap<Ipv4Addr, usize>>,
    upload_limit: Arc<RateLimiter>,
    proxy: risuko_http::ProxyConnector,
    shutdown: CancellationToken,
    next_session: AtomicU64,
}

struct ConnectionGuard {
    shared: Arc<Shared>,
    ip: Ipv4Addr,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        let mut connections = self.shared.connections.lock();
        if let Some(count) = connections.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                connections.remove(&self.ip);
            }
        }
    }
}

impl Shared {
    fn identity(&self) -> Identity {
        *self.identity.lock()
    }

    fn set_identity(&self, identity: Identity) {
        *self.identity.lock() = identity;
    }

    fn admit(self: &Arc<Self>, ip: Ipv4Addr) -> Option<ConnectionGuard> {
        let mut connections = self.connections.lock();
        let total: usize = connections.values().sum();
        let count = connections.get(&ip).copied().unwrap_or(0);
        if total >= MAX_SESSIONS || count >= MAX_SESSIONS_PER_IP {
            return None;
        }
        connections.insert(ip, count + 1);
        Some(ConnectionGuard {
            shared: self.clone(),
            ip,
        })
    }

    fn next_session_id(&self) -> SessionId {
        self.next_session.fetch_add(1, Ordering::Relaxed)
    }

    fn dispatch(self: &Arc<Self>) {
        let mut can_dial = true;
        loop {
            let Some(mut grant) = self.queue.lock().next_grant(Instant::now(), can_dial) else {
                return;
            };
            if self.deliver(&grant) {
                continue;
            }
            let Some(addr) = grant.dial else {
                self.queue.lock().release(&grant.user);
                continue;
            };
            self.queue.lock().detach(&grant.user);
            let mode = Mode::Grant {
                user: grant.user,
                file: grant.file,
            };
            if !can_dial || !self.spawn_dial(addr, mode) {
                grant.session = None;
                self.queue.lock().defer_grant(grant);
                can_dial = false;
            }
        }
    }

    fn deliver(&self, grant: &Waiter) -> bool {
        let Some(session) = grant.session else {
            return false;
        };
        let Some(tx) = self.sessions.lock().get(&session).cloned() else {
            return false;
        };
        tx.try_send(SessionCmd::Grant(grant.file)).is_ok()
    }

    fn spawn_dial(self: &Arc<Self>, addr: SocketAddrV4, mode: Mode) -> bool {
        let Some(guard) = self.admit(*addr.ip()) else {
            return false;
        };
        let shared = self.clone();
        tokio::spawn(async move {
            let host = addr.ip().to_string();
            let connected = tokio::select! {
                _ = shared.shutdown.cancelled() => return,
                result = timeout(DIAL_TIMEOUT, shared.proxy.connect_tcp(&host, addr.port())) => result,
            };
            match connected {
                Ok(Ok(stream)) => {
                    session::run(stream, *addr.ip(), shared.clone(), mode, guard).await;
                }
                Ok(Err(e)) => {
                    drop(guard);
                    tracing::debug!("[ed2k] Upload dial to {addr} failed: {e}");
                    shared.dial_failed(mode);
                }
                Err(_) => {
                    drop(guard);
                    tracing::debug!("[ed2k] Upload dial to {addr} timed out");
                    shared.dial_failed(mode);
                }
            }
        });
        true
    }

    fn dial_failed(self: &Arc<Self>, mode: Mode) {
        if let Mode::Grant { user, .. } = mode {
            if self.queue.lock().abandon_grant(&user) {
                self.dispatch();
            }
        }
    }
}

pub struct Ed2kUploader {
    shared: Arc<Shared>,
    tasks: parking_lot::Mutex<Vec<JoinHandle<()>>>,
}

impl Ed2kUploader {
    pub async fn bind(config: UploadConfig) -> std::io::Result<Arc<Self>> {
        let listener = TcpListener::bind(SocketAddrV4::new(config.bind_addr, config.port)).await?;
        let port = listener.local_addr()?.port();
        let shared = Arc::new(Shared {
            registry: config.registry,
            servers: parking_lot::RwLock::new(config.servers),
            user_hash: config.user_hash,
            port,
            identity: parking_lot::Mutex::new(Identity::default()),
            queue: parking_lot::Mutex::new(UploadQueue::new(config.max_slots, MAX_WAITING)),
            sessions: parking_lot::Mutex::new(HashMap::new()),
            connections: parking_lot::Mutex::new(HashMap::new()),
            upload_limit: config.upload_limit,
            proxy: config.proxy,
            shutdown: CancellationToken::new(),
            next_session: AtomicU64::new(1),
        });
        let kad_udp_port = config
            .kad
            .as_ref()
            .and_then(|kad| kad.advertised_udp_port());
        let mut tasks = vec![
            tokio::spawn(accept_loop(listener, shared.clone())),
            tokio::spawn(housekeeping(shared.clone())),
            tokio::spawn(announce::run_server_announcer(shared.clone(), kad_udp_port)),
        ];
        if let Some(kad) = config.kad {
            tasks.push(tokio::spawn(announce::run_kad_publisher(
                shared.clone(),
                kad,
            )));
        }
        tracing::info!("[ed2k] Upload listener on TCP port {port}");
        Ok(Arc::new(Self {
            shared,
            tasks: parking_lot::Mutex::new(tasks),
        }))
    }

    pub fn port(&self) -> u16 {
        self.shared.port
    }

    pub fn share(&self, file: SharedFile) -> ShareHandle {
        self.shared.registry.insert(file)
    }

    pub fn set_servers(&self, servers: Vec<ServerEntry>) {
        *self.shared.servers.write() = servers;
    }

    pub async fn shutdown(&self) {
        self.shared.shutdown.cancel();
        let tasks: Vec<_> = self.tasks.lock().drain(..).collect();
        for task in tasks {
            let _ = task.await;
        }
    }
}

impl Drop for Ed2kUploader {
    fn drop(&mut self) {
        self.shared.shutdown.cancel();
        for task in self.tasks.lock().drain(..) {
            task.abort();
        }
    }
}

async fn accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    loop {
        let accepted = tokio::select! {
            _ = shared.shutdown.cancelled() => return,
            accepted = listener.accept() => accepted,
        };
        let (stream, addr) = match accepted {
            Ok(pair) => pair,
            Err(e) => {
                tracing::debug!("[ed2k] Upload accept failed: {e}");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let std::net::SocketAddr::V4(addr) = addr else {
            continue;
        };
        let Some(guard) = shared.admit(*addr.ip()) else {
            continue;
        };
        let _ = stream.set_nodelay(true);
        tokio::spawn(session::run(
            stream,
            *addr.ip(),
            shared.clone(),
            Mode::Inbound,
            guard,
        ));
    }
}

async fn housekeeping(shared: Arc<Shared>) {
    let mut tick = interval(HOUSEKEEPING_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut ticks = 0u32;
    loop {
        tokio::select! {
            _ = shared.shutdown.cancelled() => return,
            _ = tick.tick() => {}
        }
        shared.registry.sample_stats();
        ticks += 1;
        if ticks.is_multiple_of(PURGE_EVERY_TICKS) {
            shared.queue.lock().purge(Instant::now());
            shared.dispatch();
        }
    }
}

#[cfg(test)]
mod tests;
