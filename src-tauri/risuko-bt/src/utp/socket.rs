use std::collections::{BTreeMap, HashMap};
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use parking_lot::{Mutex, RwLock};
use rand::RngExt;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};

use risuko_http::{NoProxy, ProxyAssociation, ProxyConnector, ProxyDatagramSource};

use super::dontfrag::UdpSender;
use super::packet::{PacketType, UtpHeader};
use super::slots::{Denied, IpSlot, IpSlots};
use super::stream::{self, DatagramTransport, DriverConfig, Inbound, Role, RoleKind, UtpStream};

pub(crate) type ConnKey = (SocketAddr, u16);

#[derive(Clone)]
pub(crate) struct ConnectionToken(Arc<()>);

impl ConnectionToken {
    fn new() -> Self {
        Self(Arc::new(()))
    }

    pub(crate) fn matches(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Clone)]
pub(crate) struct ConnRegistration {
    pub(crate) sender: mpsc::Sender<(UtpHeader, Bytes)>,
    pub(crate) token: ConnectionToken,
}

#[derive(Clone)]
pub(crate) struct ProxyConnRegistration {
    pub(crate) key: ConnKey,
    pub(crate) token: ConnectionToken,
}

pub(crate) type ConnRegistry = Arc<Mutex<HashMap<ConnKey, ConnRegistration>>>;
pub(crate) type ProxyConnRegistry = Arc<Mutex<HashMap<u16, ProxyConnRegistration>>>;

pub(crate) fn remove_connection_registration(
    registry: &ConnRegistry,
    key: ConnKey,
    token: &ConnectionToken,
) {
    let mut registry = registry.lock();
    if registry
        .get(&key)
        .is_some_and(|entry| entry.token.matches(token))
    {
        registry.remove(&key);
    }
}

pub(crate) fn remove_proxy_connection_registration(
    proxy_registry: &ProxyConnRegistry,
    connection_id: u16,
    token: &ConnectionToken,
) {
    let mut proxy_registry = proxy_registry.lock();
    if proxy_registry
        .get(&connection_id)
        .is_some_and(|entry| entry.token.matches(token))
    {
        proxy_registry.remove(&connection_id);
    }
}

const MAX_DATAGRAM: usize = 2048;
const ROUTER_READ_SLAB: usize = MAX_DATAGRAM * 64;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const ACCEPT_BACKLOG: usize = 128;
const MAX_PENDING_INBOUND: usize = 256;
const MAX_PENDING_INBOUND_PER_IP: usize = 8;
const MAX_LIVE_INBOUND: usize = 1024;
const MAX_LIVE_INBOUND_PER_IP: usize = 16;

type InboundFilter = Arc<dyn Fn(IpAddr) -> bool + Send + Sync>;

struct InboundGate {
    pending: Arc<IpSlots>,
    live: Arc<IpSlots>,
    filter: RwLock<Option<InboundFilter>>,
    half_open: Mutex<HalfOpenSet>,
}

#[derive(Default)]
struct HalfOpenSet {
    next: u64,
    entries: BTreeMap<u64, HalfOpen>,
}

struct HalfOpen {
    key: ConnKey,
    token: ConnectionToken,
    _slot: IpSlot,
}

pub(crate) struct HalfOpenTicket {
    gate: Arc<InboundGate>,
    seq: u64,
}

impl Drop for HalfOpenTicket {
    fn drop(&mut self) {
        self.gate.half_open.lock().entries.remove(&self.seq);
    }
}

impl InboundGate {
    fn new() -> Arc<Self> {
        Self::with_limits(
            (MAX_PENDING_INBOUND, MAX_PENDING_INBOUND_PER_IP),
            (MAX_LIVE_INBOUND, MAX_LIVE_INBOUND_PER_IP),
        )
    }

    fn with_limits(pending: (usize, usize), live: (usize, usize)) -> Arc<Self> {
        Arc::new(Self {
            pending: IpSlots::new(pending.0, pending.1),
            live: IpSlots::new(live.0, live.1),
            filter: RwLock::new(None),
            half_open: Mutex::new(HalfOpenSet::default()),
        })
    }

    fn admit(
        self: &Arc<Self>,
        accept_tx: &mpsc::Sender<UtpStream>,
        ip: IpAddr,
        key: ConnKey,
        token: &ConnectionToken,
        registry: &mut HashMap<ConnKey, ConnRegistration>,
    ) -> Option<Inbound> {
        if let Some(filter) = self.filter.read().as_ref() {
            if !filter(ip) {
                return None;
            }
        }
        let live = self.live.try_acquire(ip)?;
        let mut half_open = self.half_open.lock();
        let slot = match self.pending.acquire(ip) {
            Ok(slot) => slot,
            Err(Denied::PerIp) => return None,
            Err(Denied::Total) => {
                let (_, oldest) = half_open.entries.pop_first()?;
                let evicted = registry
                    .get(&oldest.key)
                    .is_some_and(|entry| entry.token.matches(&oldest.token));
                if evicted {
                    registry.remove(&oldest.key);
                }
                drop(oldest);
                self.pending.acquire(ip).ok()?
            }
        };
        let seq = half_open.next;
        half_open.next += 1;
        half_open.entries.insert(
            seq,
            HalfOpen {
                key,
                token: token.clone(),
                _slot: slot,
            },
        );
        drop(half_open);
        Some(Inbound {
            accept_tx: accept_tx.clone(),
            half_open: HalfOpenTicket {
                gate: self.clone(),
                seq,
            },
            live,
        })
    }
}

pub struct UtpSocket {
    sender: Arc<UdpSender>,
    registry: ConnRegistry,
    gate: Arc<InboundGate>,
    proxy_registry: ProxyConnRegistry,
    local_addr: SocketAddr,
    accept_rx: tokio::sync::Mutex<mpsc::Receiver<UtpStream>>,
    router_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    proxy_router_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    outbound: RwLock<OutboundRoute>,
    reconfigure_lock: AsyncMutex<()>,
    reconfigure_generation: AtomicU64,
    v6: OnceLock<Arc<UtpSocket>>,
}

#[derive(Clone)]
enum OutboundRoute {
    Direct,
    Proxy(Arc<ProxyAssociation>),
    Blocked { error: String, bypass: NoProxy },
}

async fn build_outbound_route(proxy: Option<ProxyConnector>) -> OutboundRoute {
    match proxy {
        None => OutboundRoute::Direct,
        Some(connector) if connector.udp_proxy().is_none() => OutboundRoute::Direct,
        Some(connector) => {
            let bypass = connector.udp_no_proxy().unwrap_or_default();
            let with_bypass = !bypass.is_empty();
            let bind = if with_bypass {
                connector.bind_udp_with_bypass().await
            } else {
                connector.bind_udp().await
            };
            match bind {
                Ok(datagram) => OutboundRoute::Proxy(Arc::new(ProxyAssociation::new(
                    connector,
                    with_bypass,
                    datagram,
                ))),
                Err(error) => OutboundRoute::Blocked {
                    error: error.to_string(),
                    bypass,
                },
            }
        }
    }
}

const SOCKET_BUFFER_BYTES: usize = 4 * 1024 * 1024;

fn grow_socket_buffers(udp: &UdpSocket) {
    let sock = socket2::SockRef::from(udp);
    if let Err(e) = sock.set_recv_buffer_size(SOCKET_BUFFER_BYTES) {
        tracing::debug!("uTP: set SO_RCVBUF failed: {e}");
    }
    if let Err(e) = sock.set_send_buffer_size(SOCKET_BUFFER_BYTES) {
        tracing::debug!("uTP: set SO_SNDBUF failed: {e}");
    }
}

fn family_mismatch() -> io::Error {
    io::Error::new(
        io::ErrorKind::AddrNotAvailable,
        "no utp socket for the peer's address family",
    )
}

impl UtpSocket {
    pub async fn bind(addr: SocketAddr) -> io::Result<Arc<Self>> {
        let udp = UdpSocket::bind(addr).await?;
        Ok(Self::with_udp(Arc::new(udp), true))
    }

    pub async fn bind_with_proxy(
        addr: SocketAddr,
        proxy: Option<ProxyConnector>,
    ) -> io::Result<Arc<Self>> {
        let udp = Arc::new(UdpSocket::bind(addr).await?);
        let socket = Self::with_udp(udp, true);
        socket.reconfigure_proxy(proxy).await;
        Ok(socket)
    }

    pub(crate) async fn bind_v6_only_with_proxy(
        port: u16,
        proxy: Option<ProxyConnector>,
    ) -> io::Result<Arc<Self>> {
        let udp = crate::net::bind_udp_v6_only(port)?;
        let socket = Self::with_udp(Arc::new(udp), true);
        socket.reconfigure_proxy(proxy).await;
        Ok(socket)
    }

    pub(crate) fn attach_v6(&self, sibling: Arc<UtpSocket>) {
        *sibling.gate.filter.write() = self.gate.filter.read().clone();
        let _ = self.v6.set(sibling);
    }

    pub(crate) fn v6_socket(&self) -> Option<Arc<UtpSocket>> {
        self.v6.get().cloned()
    }

    pub(crate) fn v6_local_addr(&self) -> Option<SocketAddr> {
        if self.local_addr.is_ipv6() {
            Some(self.local_addr)
        } else {
            self.v6.get().map(|socket| socket.local_addr)
        }
    }

    #[cfg(test)]
    pub fn from_udp(udp: Arc<UdpSocket>) -> Arc<Self> {
        Self::with_udp(udp, false)
    }

    fn with_udp(udp: Arc<UdpSocket>, exclusive: bool) -> Arc<Self> {
        grow_socket_buffers(&udp);
        let local_addr = udp
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
        let registry: ConnRegistry = Arc::new(Mutex::new(HashMap::new()));
        let proxy_registry: ProxyConnRegistry = Arc::new(Mutex::new(HashMap::new()));
        let (accept_tx, accept_rx) = mpsc::channel(ACCEPT_BACKLOG);
        let sender = Arc::new(UdpSender::new(udp.clone(), exclusive));
        let gate = InboundGate::new();
        let router_handle = tokio::spawn(router(
            udp,
            sender.clone(),
            registry.clone(),
            gate.clone(),
            accept_tx,
        ));
        Arc::new(Self {
            sender,
            registry,
            gate,
            proxy_registry,
            local_addr,
            accept_rx: tokio::sync::Mutex::new(accept_rx),
            router_handle: Mutex::new(Some(router_handle)),
            proxy_router_handle: Mutex::new(None),
            outbound: RwLock::new(OutboundRoute::Direct),
            reconfigure_lock: AsyncMutex::new(()),
            reconfigure_generation: AtomicU64::new(0),
            v6: OnceLock::new(),
        })
    }

    pub async fn reconfigure_proxy(&self, proxy: Option<ProxyConnector>) {
        match self.v6.get() {
            Some(v6) => {
                tokio::join!(
                    self.reconfigure_route(proxy.clone()),
                    v6.reconfigure_route(proxy)
                );
            }
            None => self.reconfigure_route(proxy).await,
        }
    }

    async fn reconfigure_route(&self, proxy: Option<ProxyConnector>) {
        let generation = self
            .reconfigure_generation
            .fetch_add(1, Ordering::AcqRel)
            .wrapping_add(1);
        let route = build_outbound_route(proxy).await;
        let _reconfigure_guard = self.reconfigure_lock.lock().await;
        if self.reconfigure_generation.load(Ordering::Acquire) != generation {
            return;
        }

        {
            let mut proxy_registry = self.proxy_registry.lock();
            let old_connections = proxy_registry.values().cloned().collect::<Vec<_>>();
            let mut registry = self.registry.lock();
            for connection in old_connections {
                if registry
                    .get(&connection.key)
                    .is_some_and(|entry| entry.token.matches(&connection.token))
                {
                    registry.remove(&connection.key);
                }
            }
            proxy_registry.clear();
        }
        if let Some(handle) = self.proxy_router_handle.lock().take() {
            handle.abort();
        }

        if let OutboundRoute::Proxy(datagram) = &route {
            let router_datagram = datagram.clone();
            let registry = self.registry.clone();
            let proxy_registry = self.proxy_registry.clone();
            let handle = tokio::spawn(async move {
                proxy_router(router_datagram, registry, proxy_registry).await
            });
            *self.proxy_router_handle.lock() = Some(handle);
        }
        *self.outbound.write() = route;
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) fn set_inbound_filter(
        &self,
        filter: impl Fn(IpAddr) -> bool + Send + Sync + 'static,
    ) {
        let filter: InboundFilter = Arc::new(filter);
        if let Some(v6) = self.v6.get() {
            *v6.gate.filter.write() = Some(filter.clone());
        }
        *self.gate.filter.write() = Some(filter);
    }

    pub fn refuse_inbound(&self) {
        self.set_inbound_filter(|_| false);
    }

    pub async fn connect(&self, remote: SocketAddr) -> io::Result<UtpStream> {
        self.connect_timeout(remote, DEFAULT_CONNECT_TIMEOUT).await
    }

    pub async fn connect_timeout(
        &self,
        remote: SocketAddr,
        timeout: Duration,
    ) -> io::Result<UtpStream> {
        let remote = crate::net::canonical_addr(remote);
        let proxied = matches!(*self.outbound.read(), OutboundRoute::Proxy(_));
        let socket = match (remote.is_ipv6(), self.local_addr.is_ipv6()) {
            _ if proxied => self,
            (true, false) => self.v6.get().map(Arc::as_ref).ok_or_else(family_mismatch)?,
            (false, true) => return Err(family_mismatch()),
            _ => self,
        };
        socket.dial(remote, timeout).await
    }

    async fn dial(&self, remote: SocketAddr, timeout: Duration) -> io::Result<UtpStream> {
        let reconfigure_guard = self.reconfigure_lock.lock().await;
        let route = self.outbound.read().clone();
        let uses_proxy = matches!(route, OutboundRoute::Proxy(_));

        let (key, token, inc_rx) = {
            let mut proxy_registry = self.proxy_registry.lock();
            let mut reg = self.registry.lock();
            let mut id: u16 = rand::rng().random();
            let mut tries = 0;
            while reg.contains_key(&(remote, id))
                || (uses_proxy && proxy_registry.contains_key(&id))
            {
                id = id.wrapping_add(1);
                tries += 1;
                if tries > 64 {
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        "no free utp connection id for peer",
                    ));
                }
            }
            let key = (remote, id);
            let token = ConnectionToken::new();
            let (inc_tx, inc_rx) = mpsc::channel(stream::INCOMING_QUEUE_PACKETS);
            reg.insert(
                key,
                ConnRegistration {
                    sender: inc_tx,
                    token: token.clone(),
                },
            );
            if uses_proxy {
                proxy_registry.insert(
                    id,
                    ProxyConnRegistration {
                        key,
                        token: token.clone(),
                    },
                );
            }
            (key, token, inc_rx)
        };
        let recv_id = key.1;
        let send_id = recv_id.wrapping_add(1);

        let transport = match route {
            OutboundRoute::Direct => DatagramTransport::Direct(self.sender.clone()),
            OutboundRoute::Proxy(proxy) => DatagramTransport::Proxy(proxy),
            OutboundRoute::Blocked { error, bypass } => {
                if bypass.matches_host_port(&remote.ip().to_string(), Some(remote.port())) {
                    DatagramTransport::Direct(self.sender.clone())
                } else {
                    remove_connection_registration(&self.registry, key, &token);
                    remove_proxy_connection_registration(&self.proxy_registry, recv_id, &token);
                    return Err(io::Error::new(io::ErrorKind::Unsupported, error));
                }
            }
        };

        let (done_tx, done_rx) = oneshot::channel();
        let shared = stream::new_shared(remote, send_id, RoleKind::Initiator);
        let cfg = DriverConfig {
            transport,
            remote,
            incoming: inc_rx,
            registry: self.registry.clone(),
            key,
            token: token.clone(),
            proxy_registry: uses_proxy.then(|| self.proxy_registry.clone()),
        };
        let driver_shared = shared.clone();
        tokio::spawn(stream::drive(driver_shared, cfg, Role::Initiator(done_tx)));
        drop(reconfigure_guard);

        match tokio::time::timeout(timeout, done_rx).await {
            Ok(Ok(Ok(()))) => Ok(UtpStream::new(shared)),
            Ok(Ok(Err(e))) => Err(e),
            Ok(Err(_)) => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "utp driver exited before handshake",
            )),
            Err(_) => {
                {
                    let mut st = shared.state.lock();
                    st.force_close();
                }
                shared.nudge.notify_one();
                remove_connection_registration(&self.registry, key, &token);
                remove_proxy_connection_registration(&self.proxy_registry, recv_id, &token);
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "utp connect timed out",
                ))
            }
        }
    }

    pub async fn accept(&self) -> io::Result<UtpStream> {
        let mut rx = self.accept_rx.lock().await;
        rx.recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "utp socket closed"))
    }

    pub fn shutdown(&self) {
        if let Some(v6) = self.v6.get() {
            v6.shutdown();
        }
        if let Some(handle) = self.router_handle.lock().take() {
            handle.abort();
        }
        if let Some(handle) = self.proxy_router_handle.lock().take() {
            handle.abort();
        }
        self.registry.lock().clear();
        self.proxy_registry.lock().clear();
    }
}

impl Drop for UtpSocket {
    fn drop(&mut self) {
        self.shutdown();
    }
}

async fn router(
    udp: Arc<UdpSocket>,
    sender: Arc<UdpSender>,
    registry: ConnRegistry,
    gate: Arc<InboundGate>,
    accept_tx: mpsc::Sender<UtpStream>,
) {
    let mut buf = BytesMut::with_capacity(ROUTER_READ_SLAB);
    buf.resize(ROUTER_READ_SLAB, 0);
    loop {
        if buf.len() <= MAX_DATAGRAM {
            buf.resize(ROUTER_READ_SLAB, 0);
        }
        let (n, src) = match udp.recv_from(&mut buf[..=MAX_DATAGRAM]).await {
            Ok(x) => x,
            // ICMP port-unreachable on some platforms must not kill the endpoint
            Err(_) => continue,
        };
        if n > MAX_DATAGRAM {
            continue;
        }
        let Ok((header, payload)) = UtpHeader::decode(&buf[..n]) else {
            continue;
        };
        let payload_offset = n - payload.len();
        let payload = buf.split_to(n).freeze().slice(payload_offset..);
        let key = (src, header.connection_id);
        {
            let reg = registry.lock();
            if header.packet_type == PacketType::Reset {
                for id in [
                    header.connection_id,
                    header.connection_id.wrapping_sub(1),
                    header.connection_id.wrapping_add(1),
                ] {
                    if let Some(entry) = reg.get(&(src, id)) {
                        let _ = entry.sender.try_send((header.clone(), payload.clone()));
                    }
                }
                continue;
            }
            if let Some(entry) = reg.get(&key) {
                let _ = entry.sender.try_send((header, payload));
                continue;
            }
        }
        match header.packet_type {
            PacketType::Syn => open_inbound(&sender, &registry, &gate, &accept_tx, src, &header),
            _ => {
                let _ = sender.try_send_to(&reset_for(&header), src);
            }
        }
    }
}

pub(crate) fn reset_for(header: &UtpHeader) -> Vec<u8> {
    UtpHeader {
        packet_type: PacketType::Reset,
        connection_id: header.connection_id,
        timestamp_micros: super::now_micros(),
        timestamp_diff_micros: 0,
        wnd_size: 0,
        seq_nr: rand::rng().random(),
        ack_nr: header.seq_nr,
        selective_ack: None,
    }
    .encode(&[])
}

fn open_inbound(
    sender: &Arc<UdpSender>,
    registry: &ConnRegistry,
    gate: &Arc<InboundGate>,
    accept_tx: &mpsc::Sender<UtpStream>,
    src: SocketAddr,
    syn: &UtpHeader,
) {
    let send_id = syn.connection_id;
    let recv_id = send_id.wrapping_add(1);
    let key = (src, recv_id);

    let (token, inc_rx, inbound) = {
        let mut reg = registry.lock();
        if let Some(existing) = reg.get(&key) {
            let _ = existing.sender.try_send((syn.clone(), Bytes::new()));
            return;
        }
        let token = ConnectionToken::new();
        let Some(inbound) = gate.admit(accept_tx, src.ip(), key, &token, &mut reg) else {
            return;
        };
        let (inc_tx, inc_rx) = mpsc::channel(stream::INCOMING_QUEUE_PACKETS);
        reg.insert(
            key,
            ConnRegistration {
                sender: inc_tx,
                token: token.clone(),
            },
        );
        (token, inc_rx, inbound)
    };

    let shared = stream::new_shared(src, send_id, RoleKind::Responder);
    shared.state.lock().seed_responder(syn);

    let cfg = DriverConfig {
        transport: DatagramTransport::Direct(sender.clone()),
        remote: src,
        incoming: inc_rx,
        registry: registry.clone(),
        key,
        token,
        proxy_registry: None,
    };
    tokio::spawn(stream::drive(shared, cfg, Role::Responder(inbound)));
}

fn route_proxied(
    registry: &ConnRegistry,
    proxy_registry: &ProxyConnRegistry,
    src: SocketAddr,
    id: u16,
    packet: (UtpHeader, Bytes),
) {
    let Some(connection) = proxy_registry.lock().get(&id).cloned() else {
        return;
    };
    let key = (src, id);
    if connection.key != key {
        return;
    }
    if let Some(entry) = registry.lock().get(&key) {
        if entry.token.matches(&connection.token) {
            let _ = entry.sender.try_send(packet);
        }
    }
}

async fn proxy_router(
    datagram: Arc<ProxyAssociation>,
    registry: ConnRegistry,
    proxy_registry: ProxyConnRegistry,
) {
    let mut buf = BytesMut::with_capacity(ROUTER_READ_SLAB);
    buf.resize(ROUTER_READ_SLAB, 0);
    loop {
        if buf.len() <= MAX_DATAGRAM {
            buf.resize(ROUTER_READ_SLAB, 0);
        }
        let (n, src) = match datagram.recv_from_target(&mut buf[..=MAX_DATAGRAM]).await {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!("uTP proxy receive loop terminated: {error}");
                return;
            }
        };
        if n > MAX_DATAGRAM {
            continue;
        }
        let Ok((header, payload)) = UtpHeader::decode(&buf[..n]) else {
            continue;
        };
        let payload_offset = n - payload.len();
        let payload = buf.split_to(n).freeze().slice(payload_offset..);
        match src {
            ProxyDatagramSource::Ip(src) => {
                if header.packet_type == PacketType::Reset {
                    let id = header.connection_id;
                    for id in [id, id.wrapping_sub(1), id.wrapping_add(1)] {
                        let packet = (header.clone(), payload.clone());
                        route_proxied(&registry, &proxy_registry, src, id, packet);
                    }
                } else {
                    let id = header.connection_id;
                    route_proxied(&registry, &proxy_registry, src, id, (header, payload));
                }
            }
            ProxyDatagramSource::Host(host, port) => {
                let connection = proxy_registry.lock().get(&header.connection_id).cloned();
                let Some(connection) = connection else {
                    continue;
                };
                if connection.key.0.port() != port
                    || !risuko_http::datagram_source_matches(
                        &ProxyDatagramSource::Host(host, port),
                        connection.key.0,
                    )
                {
                    continue;
                }
                if let Some(entry) = registry.lock().get(&connection.key) {
                    if entry.token.matches(&connection.token) {
                        let _ = entry.sender.try_send((header, payload));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{eventually, ipv6_loopback_available};
    use std::net::Ipv6Addr;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn loopback_pair() -> (Arc<UtpSocket>, Arc<UtpSocket>) {
        let a = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let b = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        (a, b)
    }

    #[tokio::test]
    async fn proxy_router_survives_a_closed_association() {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        let (assoc_tx, mut assoc_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            loop {
                let (mut control, _) = listener.accept().await.unwrap();
                let relay = UdpSocket::bind("127.0.0.1:0").await.unwrap();
                let relay_port = relay.local_addr().unwrap().port();
                crate::test_util::socks5_grant_udp_associate(&mut control, relay_port).await;
                let _ = assoc_tx.send((relay, control));
            }
        });
        let connector = ProxyConnector::from_proxy(
            risuko_http::Proxy::all(format!("socks5h://{proxy_addr}")).unwrap(),
        )
        .connect_timeout(Duration::from_secs(2));
        let socket = UtpSocket::bind_with_proxy("127.0.0.1:0".parse().unwrap(), Some(connector))
            .await
            .unwrap();
        let (_relay, control) = assoc_rx.recv().await.unwrap();
        drop(control);
        tokio::time::timeout(Duration::from_secs(5), assoc_rx.recv())
            .await
            .expect("association was not rebuilt")
            .unwrap();
        let finished = socket
            .proxy_router_handle
            .lock()
            .as_ref()
            .is_some_and(|handle| handle.is_finished());
        assert!(
            !finished,
            "proxy router exited after the association closed"
        );
    }

    #[tokio::test]
    async fn a_proxied_v4_socket_relays_ipv6_dials_through_the_proxy() {
        use tokio::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        let relay = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let relay_port = relay.local_addr().unwrap().port();
        let control = tokio::spawn(async move {
            let (mut control, _) = listener.accept().await.unwrap();
            crate::test_util::socks5_grant_udp_associate(&mut control, relay_port).await;
            let _held = control;
            std::future::pending::<()>().await;
        });
        let connector = ProxyConnector::from_proxy(
            risuko_http::Proxy::all(format!("socks5h://{proxy_addr}")).unwrap(),
        )
        .connect_timeout(Duration::from_secs(2));
        let socket = UtpSocket::bind_with_proxy("127.0.0.1:0".parse().unwrap(), Some(connector))
            .await
            .unwrap();
        assert!(socket.v6_socket().is_none());
        let remote: SocketAddr = "[2001:db8::7]:6881".parse().unwrap();
        let dialing = Arc::clone(&socket);
        let dial = tokio::spawn(async move {
            dialing
                .connect_timeout(remote, Duration::from_secs(2))
                .await
        });
        let mut frame = [0u8; 2048];
        let (n, _) = tokio::time::timeout(Duration::from_secs(5), relay.recv_from(&mut frame))
            .await
            .expect("the IPv6 dial never reached the proxy relay")
            .unwrap();
        assert!(n > 22);
        assert_eq!(frame[3], 4, "the relay header must name an IPv6 target");
        dial.abort();
        control.abort();
    }

    #[tokio::test]
    async fn handshake_then_echo() {
        let (client_sock, server_sock) = loopback_pair().await;
        let server_addr = server_sock.local_addr();

        let server = tokio::spawn(async move {
            let mut s = server_sock.accept().await.unwrap();
            let mut buf = [0u8; 5];
            s.read_exact(&mut buf).await.unwrap();
            s.write_all(&buf).await.unwrap();
            s.flush().await.unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        tokio::time::timeout(Duration::from_secs(5), async move {
            let mut c = client_sock.connect(server_addr).await.unwrap();
            c.write_all(b"hello").await.unwrap();
            c.flush().await.unwrap();
            let mut echo = [0u8; 5];
            c.read_exact(&mut echo).await.unwrap();
            assert_eq!(&echo, b"hello");
        })
        .await
        .expect("echo round trip timed out");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn bulk_transfer_preserves_bytes() {
        let (client_sock, server_sock) = loopback_pair().await;
        let server_addr = server_sock.local_addr();
        const N: usize = 256 * 1024;
        let data: Vec<u8> = (0..N).map(|i| (i % 251) as u8).collect();
        let expected = data.clone();

        let server = tokio::spawn(async move {
            let mut s = server_sock.accept().await.unwrap();
            let mut got = Vec::new();
            s.read_to_end(&mut got).await.unwrap();
            got
        });

        tokio::time::timeout(Duration::from_secs(20), async move {
            let mut c = client_sock.connect(server_addr).await.unwrap();
            c.write_all(&data).await.unwrap();
            c.shutdown().await.unwrap();
        })
        .await
        .expect("bulk send timed out");

        let got = tokio::time::timeout(Duration::from_secs(20), server)
            .await
            .expect("bulk recv timed out")
            .unwrap();
        assert_eq!(got.len(), expected.len());
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn connect_to_dead_peer_times_out() {
        let sock = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let err = sock
            .connect_timeout(dead, Duration::from_millis(600))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
    }

    async fn dual_stack_pair() -> Option<(Arc<UtpSocket>, Arc<UtpSocket>)> {
        if !ipv6_loopback_available() {
            eprintln!("skipping: no IPv6 loopback on this host");
            return None;
        }
        let v4 = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let v6 = UtpSocket::bind_v6_only_with_proxy(0, None).await.unwrap();
        v4.attach_v6(v6.clone());
        Some((v4, v6))
    }

    async fn dial_and_accept(
        dialer: &UtpSocket,
        listener: &Arc<UtpSocket>,
        target: SocketAddr,
    ) -> UtpStream {
        let listener = listener.clone();
        let accept = tokio::spawn(async move { listener.accept().await });
        let mut out = tokio::time::timeout(Duration::from_secs(5), dialer.connect(target))
            .await
            .expect("dial timed out")
            .expect("dial failed");
        out.write_all(b"hi").await.unwrap();
        out.flush().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), accept)
            .await
            .expect("no inbound connection")
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn dials_pick_the_socket_that_matches_the_peers_address_family() {
        let Some((v4, v6)) = dual_stack_pair().await else {
            return;
        };
        let peer4 = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let peer6 = UtpSocket::bind("[::1]:0".parse().unwrap()).await.unwrap();

        let via4 = dial_and_accept(&v4, &peer4, peer4.local_addr()).await;
        assert_eq!(via4.peer_addr().port(), v4.local_addr().port());
        let via6 = dial_and_accept(&v4, &peer6, peer6.local_addr()).await;
        assert_eq!(via6.peer_addr().port(), v6.local_addr().port());
        assert!(via6.peer_addr().is_ipv6());
    }

    #[tokio::test]
    async fn a_v4_mapped_peer_address_is_dialled_over_v4() {
        let Some((v4, _v6)) = dual_stack_pair().await else {
            return;
        };
        let peer = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let mapped = SocketAddr::new(
            IpAddr::V6(std::net::Ipv4Addr::LOCALHOST.to_ipv6_mapped()),
            peer.local_addr().port(),
        );
        assert!(mapped.is_ipv6());
        let inbound = dial_and_accept(&v4, &peer, mapped).await;
        assert!(inbound.peer_addr().is_ipv4());
    }

    #[tokio::test]
    async fn address_families_without_a_socket_are_refused_not_misrouted() {
        let Some((_v4, v6)) = dual_stack_pair().await else {
            return;
        };
        let v4_only = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let wanted = [
            (&v4_only, "[::1]:9"),
            (&v6, "127.0.0.1:9"),
            (&v4_only, "[2001:db8::1]:9"),
        ];
        for (socket, addr) in wanted {
            let err = socket
                .connect_timeout(addr.parse().unwrap(), Duration::from_secs(1))
                .await
                .expect_err("dial must be refused");
            assert_eq!(err.kind(), io::ErrorKind::AddrNotAvailable, "{addr}");
        }
    }

    #[tokio::test]
    async fn v6_dials_follow_the_proxy_route_of_the_primary_socket() {
        let Some((v4, _v6)) = dual_stack_pair().await else {
            return;
        };
        let peer6 = UtpSocket::bind("[::1]:0".parse().unwrap()).await.unwrap();
        let target = peer6.local_addr();
        dial_and_accept(&v4, &peer6, target).await;

        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = closed.local_addr().unwrap();
        drop(closed);
        let connector = ProxyConnector::from_proxy(
            risuko_http::Proxy::all(format!("socks5h://{proxy_addr}")).unwrap(),
        )
        .connect_timeout(Duration::from_secs(2));
        tokio::time::timeout(
            Duration::from_secs(10),
            v4.reconfigure_proxy(Some(connector)),
        )
        .await
        .expect("reconfigure timed out");

        let err = tokio::time::timeout(Duration::from_secs(5), v4.connect(target))
            .await
            .expect("blocked dial must fail fast")
            .expect_err("v6 dial escaped the proxy");
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);

        tokio::time::timeout(Duration::from_secs(10), v4.reconfigure_proxy(None))
            .await
            .expect("reconfigure timed out");
        dial_and_accept(&v4, &peer6, target).await;
    }

    #[tokio::test]
    async fn the_inbound_filter_and_shutdown_reach_the_v6_sibling() {
        let Some((v4, v6)) = dual_stack_pair().await else {
            return;
        };
        let seen: Arc<Mutex<Vec<IpAddr>>> = Arc::new(Mutex::new(Vec::new()));
        let recorded = seen.clone();
        v4.set_inbound_filter(move |ip| {
            recorded.lock().push(ip);
            false
        });
        let raw = UdpSocket::bind("[::1]:0").await.unwrap();
        let v6_target = SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), v6.local_addr().port());
        let syn = raw_header(PacketType::Syn, 920, 1, 0);
        raw.send_to(&syn.encode(&[]), v6_target).await.unwrap();
        eventually("the filter to see the v6 SYN", || !seen.lock().is_empty()).await;
        assert!(received_types(&raw, Duration::from_millis(200))
            .await
            .is_empty());
        assert!(v6.registry.lock().is_empty());
        assert_eq!(*seen.lock(), vec![IpAddr::V6(Ipv6Addr::LOCALHOST)]);

        let late = UtpSocket::bind_v6_only_with_proxy(0, None).await.unwrap();
        assert!(late.gate.filter.read().is_none());
        let primary = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        primary.refuse_inbound();
        primary.attach_v6(late.clone());
        assert!(late.gate.filter.read().is_some());

        v4.shutdown();
        assert!(v6.router_handle.lock().is_none());
    }

    #[tokio::test]
    async fn drop_clears_registry_to_close_driver_channels() {
        let udp = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let sock = UtpSocket::from_udp(udp);
        let registry = sock.registry.clone();
        let key: ConnKey = ("127.0.0.1:9".parse().unwrap(), 7);
        let (tx, mut rx) = mpsc::channel(8);
        registry.lock().insert(
            key,
            ConnRegistration {
                sender: tx,
                token: ConnectionToken::new(),
            },
        );

        drop(sock);

        assert!(registry.lock().is_empty());
        let received = tokio::time::timeout(Duration::from_millis(100), rx.recv())
            .await
            .expect("registry sender kept the driver channel open");
        assert!(received.is_none());
    }

    #[tokio::test]
    async fn externally_shared_socket_skips_path_mtu_discovery() {
        let udp = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        assert!(!UtpSocket::from_udp(udp).sender.can_probe());
    }

    #[test]
    fn stale_cleanup_preserves_reused_connection_registration() {
        let registry: ConnRegistry = Arc::new(Mutex::new(HashMap::new()));
        let proxy_registry: ProxyConnRegistry = Arc::new(Mutex::new(HashMap::new()));
        let key: ConnKey = ("127.0.0.1:9".parse().unwrap(), 7);
        let stale = ConnectionToken::new();
        let current = ConnectionToken::new();
        let (tx, _rx) = mpsc::channel(8);

        registry.lock().insert(
            key,
            ConnRegistration {
                sender: tx,
                token: current.clone(),
            },
        );
        proxy_registry.lock().insert(
            key.1,
            ProxyConnRegistration {
                key,
                token: current.clone(),
            },
        );

        remove_connection_registration(&registry, key, &stale);
        remove_proxy_connection_registration(&proxy_registry, key.1, &stale);

        assert!(registry.lock().contains_key(&key));
        assert!(proxy_registry.lock().contains_key(&key.1));
    }

    async fn lossy_relay(
        server: SocketAddr,
        drop_c2s: fn(usize) -> bool,
        drop_s2c: fn(usize) -> bool,
    ) -> SocketAddr {
        let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let back = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let front_addr = front.local_addr().unwrap();
        let client: Arc<Mutex<Option<SocketAddr>>> = Arc::new(Mutex::new(None));
        {
            let (front, back, client) = (front.clone(), back.clone(), client.clone());
            tokio::spawn(async move {
                let mut buf = [0u8; MAX_DATAGRAM];
                let mut n_seen = 0usize;
                while let Ok((n, from)) = front.recv_from(&mut buf).await {
                    *client.lock() = Some(from);
                    let drop = drop_c2s(n_seen);
                    n_seen += 1;
                    if !drop {
                        let _ = back.send_to(&buf[..n], server).await;
                    }
                }
            });
        }
        tokio::spawn(async move {
            let mut buf = [0u8; MAX_DATAGRAM];
            let mut n_seen = 0usize;
            while let Ok((n, _)) = back.recv_from(&mut buf).await {
                let drop = drop_s2c(n_seen);
                n_seen += 1;
                let target = *client.lock();
                if let (false, Some(target)) = (drop, target) {
                    let _ = front.send_to(&buf[..n], target).await;
                }
            }
        });
        front_addr
    }

    #[tokio::test]
    async fn bulk_transfer_survives_loss_and_a_lost_syn_ack() {
        let (client_sock, server_sock) = loopback_pair().await;
        let relay = lossy_relay(
            server_sock.local_addr(),
            |i| i > 0 && i % 7 == 0,
            |i| i == 0,
        )
        .await;
        const N: usize = 200 * 1024;
        let data: Vec<u8> = (0..N).map(|i| (i % 253) as u8).collect();
        let expected = data.clone();

        let server = tokio::spawn(async move {
            let mut s = server_sock.accept().await.unwrap();
            let mut got = Vec::new();
            s.read_to_end(&mut got).await.unwrap();
            got
        });
        tokio::time::timeout(Duration::from_secs(30), async move {
            let mut c = client_sock.connect(relay).await.unwrap();
            c.write_all(&data).await.unwrap();
            c.shutdown().await.unwrap();
        })
        .await
        .expect("lossy send timed out");
        let got = tokio::time::timeout(Duration::from_secs(30), server)
            .await
            .expect("lossy recv timed out")
            .unwrap();
        assert_eq!(got.len(), expected.len());
        assert!(got == expected, "payload corrupted across retransmissions");
    }

    #[tokio::test]
    async fn unknown_connection_is_answered_with_reset() {
        let sock = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let raw = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let stray = UtpHeader {
            packet_type: PacketType::Data,
            connection_id: 4242,
            timestamp_micros: 1,
            timestamp_diff_micros: 0,
            wnd_size: 1000,
            seq_nr: 77,
            ack_nr: 5,
            selective_ack: None,
        };
        raw.send_to(&stray.encode(b"hi"), sock.local_addr())
            .await
            .unwrap();
        let mut buf = [0u8; 64];
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), raw.recv_from(&mut buf))
            .await
            .expect("no reset received")
            .unwrap();
        let (reset, _) = UtpHeader::decode(&buf[..n]).unwrap();
        assert_eq!(reset.packet_type, PacketType::Reset);
        assert_eq!(reset.connection_id, 4242);
        assert_eq!(reset.ack_nr, 77);
    }

    async fn assert_reset_aborts_connect(
        reset_id: fn(u16) -> u16,
        reset_ack: Option<fn(u16) -> u16>,
    ) {
        let client_sock = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let raw = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let raw_addr = raw.local_addr().unwrap();
        let responder = {
            let raw = raw.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; MAX_DATAGRAM];
                let (n, client) = raw.recv_from(&mut buf).await.unwrap();
                let (syn, _) = UtpHeader::decode(&buf[..n]).unwrap();
                assert_eq!(syn.packet_type, PacketType::Syn);
                let mut state = syn.clone();
                state.packet_type = PacketType::State;
                state.seq_nr = 1000;
                state.ack_nr = syn.seq_nr;
                raw.send_to(&state.encode(&[]), client).await.unwrap();
                let mut reset = state;
                reset.packet_type = PacketType::Reset;
                reset.connection_id = reset_id(syn.connection_id);
                if let Some(ack) = reset_ack {
                    reset.ack_nr = ack(syn.seq_nr);
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
                raw.send_to(&reset.encode(&[]), client).await.unwrap();
            })
        };
        let mut stream = client_sock.connect(raw_addr).await.unwrap();
        responder.await.unwrap();
        let mut buf = [0u8; 8];
        let err = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .expect("reset not observed")
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
    }

    #[tokio::test]
    async fn reset_addressed_to_send_id_aborts_the_connection() {
        assert_reset_aborts_connect(|id| id.wrapping_add(1), None).await;
    }

    #[tokio::test]
    async fn reset_answering_a_state_packet_aborts_the_connection() {
        let next_seq = |syn_seq: u16| syn_seq.wrapping_add(1);
        assert_reset_aborts_connect(|id| id, Some(next_seq)).await;
        assert_reset_aborts_connect(|id| id.wrapping_add(1), Some(next_seq)).await;
    }

    #[tokio::test]
    async fn oversized_datagrams_are_dropped_instead_of_truncated() {
        let client_sock = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let raw = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let raw_addr = raw.local_addr().unwrap();
        let peer = {
            let raw = raw.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; MAX_DATAGRAM];
                let (n, client) = raw.recv_from(&mut buf).await.unwrap();
                let (syn, _) = UtpHeader::decode(&buf[..n]).unwrap();
                let state = UtpHeader {
                    packet_type: PacketType::State,
                    connection_id: syn.connection_id,
                    timestamp_micros: 1,
                    timestamp_diff_micros: 0,
                    wnd_size: 65536,
                    seq_nr: 1000,
                    ack_nr: syn.seq_nr,
                    selective_ack: None,
                };
                raw.send_to(&state.encode(&[]), client).await.unwrap();
                let data = UtpHeader {
                    packet_type: PacketType::Data,
                    ..state
                };
                raw.send_to(&data.encode(&[7u8; 3000]), client)
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(200)).await;
                raw.send_to(&data.encode(b"ok"), client).await.unwrap();
            })
        };
        let mut stream = client_sock.connect(raw_addr).await.unwrap();
        peer.await.unwrap();
        let mut buf = [0u8; 4096];
        let n = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .expect("no data delivered")
            .unwrap();
        assert!(&buf[..n] == b"ok", "read {n} bytes");
    }

    #[tokio::test]
    async fn path_mtu_discovery_raises_packet_size_on_a_roomy_path() {
        let (client_sock, server_sock) = loopback_pair().await;
        if !client_sock.sender.can_probe() {
            return;
        }
        let server_addr = server_sock.local_addr();
        let data: Vec<u8> = (0..512 * 1024).map(|i| (i % 249) as u8).collect();
        let expected = data.clone();
        let server = tokio::spawn(async move {
            let mut s = server_sock.accept().await.unwrap();
            let mut got = Vec::new();
            s.read_to_end(&mut got).await.unwrap();
            got
        });
        let floor = tokio::time::timeout(Duration::from_secs(20), async move {
            let mut c = client_sock.connect(server_addr).await.unwrap();
            c.write_all(&data).await.unwrap();
            c.flush().await.unwrap();
            let floor = c.mtu_floor();
            c.shutdown().await.unwrap();
            floor
        })
        .await
        .expect("transfer timed out");
        assert_eq!(server.await.unwrap(), expected);
        assert!(
            floor > 1254,
            "loopback carries more than the starting size (floor {floor})"
        );
    }

    #[tokio::test]
    async fn path_mtu_discovery_settles_below_a_narrow_path() {
        const PATH_MTU: usize = 1300;
        let (client_sock, server_sock) = loopback_pair().await;
        if !client_sock.sender.can_probe() {
            return;
        }
        let server_addr = server_sock.local_addr();
        let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let back = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let relay = front.local_addr().unwrap();
        let client_addr: Arc<Mutex<Option<SocketAddr>>> = Arc::new(Mutex::new(None));
        {
            let (front, back, client_addr) = (front.clone(), back.clone(), client_addr.clone());
            tokio::spawn(async move {
                let mut seen = std::collections::HashSet::new();
                let mut buf = [0u8; MAX_DATAGRAM];
                while let Ok((n, from)) = front.recv_from(&mut buf).await {
                    *client_addr.lock() = Some(from);
                    let seq = u16::from_be_bytes([buf[16], buf[17]]);
                    if n > PATH_MTU && seen.insert(seq) {
                        continue;
                    }
                    let _ = back.send_to(&buf[..n], server_addr).await;
                }
            });
        }
        tokio::spawn(async move {
            let mut buf = [0u8; MAX_DATAGRAM];
            while let Ok((n, _)) = back.recv_from(&mut buf).await {
                let target = *client_addr.lock();
                if let Some(target) = target {
                    let _ = front.send_to(&buf[..n], target).await;
                }
            }
        });

        let data: Vec<u8> = (0..512 * 1024).map(|i| (i % 241) as u8).collect();
        let expected = data.clone();
        let server = tokio::spawn(async move {
            let mut s = server_sock.accept().await.unwrap();
            let mut got = Vec::new();
            s.read_to_end(&mut got).await.unwrap();
            got
        });
        let floor = tokio::time::timeout(Duration::from_secs(30), async move {
            let mut c = client_sock.connect(relay).await.unwrap();
            c.write_all(&data).await.unwrap();
            c.flush().await.unwrap();
            let floor = c.mtu_floor();
            c.shutdown().await.unwrap();
            floor
        })
        .await
        .expect("transfer over the narrow path timed out");
        assert!(server.await.unwrap() == expected, "payload corrupted");
        assert!(
            floor > 1254 && floor <= PATH_MTU,
            "floor {floor} should settle between the start size and the path MTU"
        );
    }

    fn raw_header(
        packet_type: PacketType,
        connection_id: u16,
        seq_nr: u16,
        ack_nr: u16,
    ) -> UtpHeader {
        UtpHeader {
            packet_type,
            connection_id,
            timestamp_micros: 0,
            timestamp_diff_micros: 0,
            wnd_size: 65536,
            seq_nr,
            ack_nr,
            selective_ack: None,
        }
    }

    async fn raw_syn(raw: &UdpSocket, server: SocketAddr, id: u16) -> UtpHeader {
        let syn = raw_header(PacketType::Syn, id, 1, 0);
        raw.send_to(&syn.encode(&[]), server).await.unwrap();
        let mut buf = [0u8; 64];
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), raw.recv_from(&mut buf))
            .await
            .expect("no syn-ack")
            .unwrap();
        let (state, _) = UtpHeader::decode(&buf[..n]).unwrap();
        assert_eq!(state.packet_type, PacketType::State);
        state
    }

    async fn raw_data(raw: &UdpSocket, server: SocketAddr, id: u16, state: &UtpHeader, p: &[u8]) {
        let data = raw_header(
            PacketType::Data,
            id.wrapping_add(1),
            2,
            state.seq_nr.wrapping_sub(1),
        );
        raw.send_to(&data.encode(p), server).await.unwrap();
    }

    async fn received_types(raw: &UdpSocket, window: Duration) -> Vec<PacketType> {
        received_until(raw, None, window).await
    }

    async fn received_until(
        raw: &UdpSocket,
        wanted: Option<PacketType>,
        limit: Duration,
    ) -> Vec<PacketType> {
        let deadline = tokio::time::Instant::now() + limit;
        let mut types = Vec::new();
        let mut buf = [0u8; 2048];
        while let Ok(Ok((n, _))) = tokio::time::timeout_at(deadline, raw.recv_from(&mut buf)).await
        {
            let ty = UtpHeader::decode(&buf[..n]).unwrap().0.packet_type;
            types.push(ty);
            if Some(ty) == wanted {
                break;
            }
        }
        types
    }

    #[tokio::test]
    async fn a_syn_flood_is_bounded_and_nothing_is_accepted_without_data() {
        let socket = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let raw = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for id in 0..5000u16 {
            let syn = raw_header(PacketType::Syn, id.wrapping_mul(2), 1, 0);
            raw.send_to(&syn.encode(&[]), socket.local_addr())
                .await
                .unwrap();
        }
        eventually("the half-open connections", || {
            socket.registry.lock().len() >= MAX_PENDING_INBOUND_PER_IP
        })
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            socket.registry.lock().len(),
            MAX_PENDING_INBOUND_PER_IP,
            "one source address may hold only its share of half-open connections"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), socket.accept())
                .await
                .is_err(),
            "a connection that sent no data must not reach accept"
        );
        let replies = received_types(&raw, Duration::from_millis(500)).await;
        assert!(replies.len() <= MAX_PENDING_INBOUND_PER_IP);
        assert!(replies.iter().all(|ty| *ty == PacketType::State));
    }

    type Registry = HashMap<ConnKey, ConnRegistration>;

    fn gate_ip(i: usize) -> IpAddr {
        IpAddr::from([10, 0, (i / 250) as u8, (i % 250) as u8 + 1])
    }

    fn gate_key(ip: IpAddr, id: u16) -> ConnKey {
        (SocketAddr::new(ip, 6881), id)
    }

    fn admit_one(
        gate: &Arc<InboundGate>,
        registry: &mut Registry,
        tx: &mpsc::Sender<UtpStream>,
        ip: IpAddr,
        id: u16,
    ) -> Option<Inbound> {
        let key = gate_key(ip, id);
        let token = ConnectionToken::new();
        let inbound = gate.admit(tx, ip, key, &token, registry)?;
        let (sender, _rx) = mpsc::channel(1);
        registry.insert(key, ConnRegistration { sender, token });
        Some(inbound)
    }

    #[tokio::test]
    async fn a_full_half_open_pool_evicts_the_oldest_instead_of_refusing_new_syns() {
        let gate = InboundGate::new();
        let (tx, _rx) = mpsc::channel(1);
        let mut registry = Registry::new();
        let mut held: Vec<_> = (0..MAX_PENDING_INBOUND)
            .map(|i| {
                admit_one(&gate, &mut registry, &tx, gate_ip(i), i as u16)
                    .expect("room for another source")
            })
            .collect();
        assert_eq!(gate.pending.in_use(), MAX_PENDING_INBOUND);

        let newcomer_ip = gate_ip(MAX_PENDING_INBOUND);
        let newcomer = admit_one(&gate, &mut registry, &tx, newcomer_ip, 9999)
            .expect("a new SYN must displace the oldest half-open connection");
        assert!(!registry.contains_key(&gate_key(gate_ip(0), 0)));
        assert!(registry.contains_key(&gate_key(gate_ip(1), 1)));
        assert!(registry.contains_key(&gate_key(newcomer_ip, 9999)));
        assert_eq!(gate.pending.in_use(), MAX_PENDING_INBOUND);
        assert_eq!(gate.half_open.lock().entries.len(), MAX_PENDING_INBOUND);

        held.remove(0);
        assert_eq!(gate.pending.in_use(), MAX_PENDING_INBOUND);
        held.pop();
        assert_eq!(gate.pending.in_use(), MAX_PENDING_INBOUND - 1);
        drop(held);
        drop(newcomer);
        assert_eq!(gate.pending.in_use(), 0);
        assert!(gate.half_open.lock().entries.is_empty());
    }

    #[tokio::test]
    async fn a_source_over_its_own_share_cannot_evict_other_half_open_connections() {
        let gate = InboundGate::new();
        let (tx, _rx) = mpsc::channel(1);
        let mut registry = Registry::new();
        let noisy = gate_ip(0);
        let mut held = Vec::new();
        for id in 0..MAX_PENDING_INBOUND_PER_IP as u16 {
            held.push(admit_one(&gate, &mut registry, &tx, noisy, id).unwrap());
        }
        for i in 1..=MAX_PENDING_INBOUND - MAX_PENDING_INBOUND_PER_IP {
            held.push(admit_one(&gate, &mut registry, &tx, gate_ip(i), 1000 + i as u16).unwrap());
        }
        assert_eq!(gate.pending.in_use(), MAX_PENDING_INBOUND);
        let before = registry.len();
        assert!(admit_one(&gate, &mut registry, &tx, noisy, 5000).is_none());
        assert_eq!(registry.len(), before);
        assert_eq!(gate.pending.in_use(), MAX_PENDING_INBOUND);
    }

    #[tokio::test]
    async fn half_open_connections_from_one_ipv6_prefix_share_one_source_share() {
        let gate = InboundGate::new();
        let (tx, _rx) = mpsc::channel(1);
        let mut registry = Registry::new();
        let in_prefix = |i: usize| -> IpAddr {
            format!("2001:db8:7:9:{:x}::{:x}", i + 1, i + 1)
                .parse()
                .unwrap()
        };
        let mut held = Vec::new();
        for i in 0..MAX_PENDING_INBOUND_PER_IP {
            let admitted = admit_one(&gate, &mut registry, &tx, in_prefix(i), i as u16);
            held.push(admitted.expect("room for another address in the prefix"));
        }
        let last = MAX_PENDING_INBOUND_PER_IP;
        assert!(admit_one(&gate, &mut registry, &tx, in_prefix(last), last as u16).is_none());
        assert_eq!(gate.pending.in_use(), MAX_PENDING_INBOUND_PER_IP);
        let other_prefix: IpAddr = "2001:db8:7:a::1".parse().unwrap();
        assert!(admit_one(&gate, &mut registry, &tx, other_prefix, 4000).is_some());
    }

    #[test]
    fn live_connections_from_one_ipv6_prefix_share_one_source_share() {
        let gate = InboundGate::new();
        let in_prefix = |i: usize| -> IpAddr {
            format!("2001:db8:7:9:{:x}::{:x}", i + 1, i + 1)
                .parse()
                .unwrap()
        };
        let held: Vec<_> = (0..MAX_LIVE_INBOUND_PER_IP)
            .map(|i| {
                gate.live
                    .try_acquire(in_prefix(i))
                    .expect("room in the prefix")
            })
            .collect();
        assert!(gate
            .live
            .try_acquire(in_prefix(MAX_LIVE_INBOUND_PER_IP))
            .is_none());
        assert!(gate
            .live
            .try_acquire("2001:db8:7:a::1".parse().unwrap())
            .is_some());
        drop(held);
    }

    #[tokio::test]
    async fn an_evicted_half_open_connection_is_dropped_without_a_reset() {
        let udp = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let sender = Arc::new(UdpSender::new(udp.clone(), false));
        let registry: ConnRegistry = Arc::new(Mutex::new(HashMap::new()));
        let gate = InboundGate::with_limits((2, 3), (16, 16));
        let (tx, _rx) = mpsc::channel(8);
        let mut peers = Vec::new();
        for i in 0..3u16 {
            let raw = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let syn = raw_header(PacketType::Syn, 100 + 2 * i, 1, 0);
            open_inbound(
                &sender,
                &registry,
                &gate,
                &tx,
                raw.local_addr().unwrap(),
                &syn,
            );
            peers.push(raw);
        }
        assert_eq!(registry.lock().len(), 2);
        eventually("the evicted driver to release its slots", || {
            gate.live.in_use() == 2 && gate.pending.in_use() == 2
        })
        .await;
        let window = Duration::from_millis(300);
        let seen = tokio::join!(
            received_types(&peers[0], window),
            received_types(&peers[1], window),
            received_types(&peers[2], window),
        );
        assert_eq!(seen.0, vec![PacketType::State]);
        assert_eq!(seen.1, vec![PacketType::State]);
        assert_eq!(seen.2, vec![PacketType::State]);
    }

    #[tokio::test]
    async fn the_first_data_packet_hands_the_stream_to_accept() {
        let socket = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let raw = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let state = raw_syn(&raw, socket.local_addr(), 300).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(200), socket.accept())
                .await
                .is_err()
        );
        raw_data(&raw, socket.local_addr(), 300, &state, b"hello").await;
        let mut stream = tokio::time::timeout(Duration::from_secs(2), socket.accept())
            .await
            .expect("stream not delivered")
            .unwrap();
        let mut buf = [0u8; 16];
        let n = stream.read(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"hello");
    }

    #[tokio::test]
    async fn aborting_an_accepted_stream_sends_one_reset_and_no_fin() {
        let socket = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let raw = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let state = raw_syn(&raw, socket.local_addr(), 500).await;
        raw_data(&raw, socket.local_addr(), 500, &state, b"x").await;
        let stream = tokio::time::timeout(Duration::from_secs(2), socket.accept())
            .await
            .expect("stream not delivered")
            .unwrap();
        stream.abort();
        let seen = received_types(&raw, Duration::from_millis(2500)).await;
        assert_eq!(
            seen.iter().filter(|ty| **ty == PacketType::Reset).count(),
            1,
            "{seen:?}"
        );
        assert!(!seen.contains(&PacketType::Fin), "{seen:?}");
        assert!(socket.registry.lock().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_abort_racing_the_driver_still_delivers_its_reset() {
        let socket = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        for round in 0..100u16 {
            let raw = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let id = 100 + round * 4;
            let state = raw_syn(&raw, socket.local_addr(), id).await;
            raw_data(&raw, socket.local_addr(), id, &state, b"x").await;
            let stream = tokio::time::timeout(Duration::from_secs(2), socket.accept())
                .await
                .expect("stream not delivered")
                .unwrap();
            stream.abort();
            let deadline = tokio::time::Instant::now() + Duration::from_millis(1500);
            let mut buf = [0u8; 2048];
            let mut reset = false;
            while !reset {
                let Ok(Ok((n, _))) =
                    tokio::time::timeout_at(deadline, raw.recv_from(&mut buf)).await
                else {
                    break;
                };
                reset = UtpHeader::decode(&buf[..n]).unwrap().0.packet_type == PacketType::Reset;
            }
            assert!(reset, "round {round}");
        }
    }

    #[tokio::test]
    async fn a_dropped_stream_still_closes_with_a_fin() {
        let socket = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let raw = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let state = raw_syn(&raw, socket.local_addr(), 700).await;
        raw_data(&raw, socket.local_addr(), 700, &state, b"x").await;
        let stream = tokio::time::timeout(Duration::from_secs(2), socket.accept())
            .await
            .expect("stream not delivered")
            .unwrap();
        drop(stream);
        let seen = received_until(&raw, Some(PacketType::Fin), Duration::from_secs(5)).await;
        assert!(seen.contains(&PacketType::Fin), "{seen:?}");
        assert!(!seen.contains(&PacketType::Reset), "{seen:?}");
    }

    #[tokio::test]
    async fn refused_sources_get_no_reply_and_leave_no_state() {
        let socket = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let seen: Arc<Mutex<Vec<IpAddr>>> = Arc::new(Mutex::new(Vec::new()));
        let recorded = seen.clone();
        socket.set_inbound_filter(move |ip| {
            recorded.lock().push(ip);
            false
        });
        let raw = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let syn = raw_header(PacketType::Syn, 900, 1, 0);
        raw.send_to(&syn.encode(&[]), socket.local_addr())
            .await
            .unwrap();
        eventually("the filter to see the SYN", || !seen.lock().is_empty()).await;
        assert!(received_types(&raw, Duration::from_millis(200))
            .await
            .is_empty());
        assert!(socket.registry.lock().is_empty());
        assert_eq!(*seen.lock(), vec![IpAddr::from([127, 0, 0, 1])]);
    }

    #[tokio::test]
    async fn a_socket_told_to_refuse_inbound_never_answers_a_syn() {
        let socket = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        socket.refuse_inbound();
        let raw = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let syn = raw_header(PacketType::Syn, 910, 1, 0);
        raw.send_to(&syn.encode(&[]), socket.local_addr())
            .await
            .unwrap();
        let probe = raw_header(PacketType::Data, 911, 2, 0);
        raw.send_to(&probe.encode(b"x"), socket.local_addr())
            .await
            .unwrap();
        let seen = received_until(&raw, Some(PacketType::Reset), Duration::from_secs(5)).await;
        assert_eq!(
            seen,
            vec![PacketType::Reset],
            "only the stray data packet is answered"
        );
        let late = received_types(&raw, Duration::from_millis(200)).await;
        assert!(
            !late.contains(&PacketType::State),
            "the SYN must never be acknowledged"
        );
        assert!(socket.registry.lock().is_empty());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), socket.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn initiators_start_from_unpredictable_sequence_numbers() {
        let client = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = silent.local_addr().unwrap();
        let mut attempts = Vec::new();
        for _ in 0..8 {
            let client = client.clone();
            attempts.push(tokio::spawn(async move {
                let _ = client
                    .connect_timeout(target, Duration::from_millis(300))
                    .await;
            }));
        }
        let mut seqs = std::collections::HashSet::new();
        let mut buf = [0u8; 64];
        for _ in 0..8 {
            let (n, _) = tokio::time::timeout(Duration::from_secs(2), silent.recv_from(&mut buf))
                .await
                .expect("a SYN never arrived")
                .unwrap();
            let (syn, _) = UtpHeader::decode(&buf[..n]).unwrap();
            assert_eq!(syn.packet_type, PacketType::Syn);
            seqs.insert(syn.seq_nr);
        }
        for attempt in attempts {
            let _ = attempt.await;
        }
        assert!(
            seqs.len() > 1,
            "every SYN started from the same number: {seqs:?}"
        );
    }
}
