use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use parking_lot::{Mutex, RwLock};
use rand::RngExt;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};

use risuko_http::{NoProxy, ProxyAssociation, ProxyConnector, ProxyDatagramSource};

use super::dontfrag::UdpSender;
use super::packet::{PacketType, UtpHeader};
use super::stream::{self, DatagramTransport, DriverConfig, Role, RoleKind, UtpStream};

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

pub struct UtpSocket {
    sender: Arc<UdpSender>,
    registry: ConnRegistry,
    proxy_registry: ProxyConnRegistry,
    local_addr: SocketAddr,
    accept_rx: tokio::sync::Mutex<mpsc::Receiver<UtpStream>>,
    router_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    proxy_router_handle: Mutex<Option<tokio::task::JoinHandle<()>>>,
    outbound: RwLock<OutboundRoute>,
    reconfigure_lock: AsyncMutex<()>,
    reconfigure_generation: AtomicU64,
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
        let router_handle = tokio::spawn(router(udp, sender.clone(), registry.clone(), accept_tx));
        Arc::new(Self {
            sender,
            registry,
            proxy_registry,
            local_addr,
            accept_rx: tokio::sync::Mutex::new(accept_rx),
            router_handle: Mutex::new(Some(router_handle)),
            proxy_router_handle: Mutex::new(None),
            outbound: RwLock::new(OutboundRoute::Direct),
            reconfigure_lock: AsyncMutex::new(()),
            reconfigure_generation: AtomicU64::new(0),
        })
    }

    pub async fn reconfigure_proxy(&self, proxy: Option<ProxyConnector>) {
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

    pub async fn connect(&self, remote: SocketAddr) -> io::Result<UtpStream> {
        self.connect_timeout(remote, DEFAULT_CONNECT_TIMEOUT).await
    }

    pub async fn connect_timeout(
        &self,
        remote: SocketAddr,
        timeout: Duration,
    ) -> io::Result<UtpStream> {
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
        if let Some(handle) = self.router_handle.get_mut().take() {
            handle.abort();
        }
        if let Some(handle) = self.proxy_router_handle.get_mut().take() {
            handle.abort();
        }
        self.registry.lock().clear();
        self.proxy_registry.lock().clear();
    }
}

async fn router(
    udp: Arc<UdpSocket>,
    sender: Arc<UdpSender>,
    registry: ConnRegistry,
    accept_tx: mpsc::Sender<UtpStream>,
) {
    let mut buf = BytesMut::with_capacity(ROUTER_READ_SLAB);
    buf.resize(ROUTER_READ_SLAB, 0);
    loop {
        if buf.len() < MAX_DATAGRAM {
            buf.resize(ROUTER_READ_SLAB, 0);
        }
        let (n, src) = match udp.recv_from(&mut buf[..MAX_DATAGRAM]).await {
            Ok(x) => x,
            // ICMP port-unreachable on some platforms must not kill the endpoint
            Err(_) => continue,
        };
        let Ok((header, payload)) = UtpHeader::decode(&buf[..n]) else {
            continue;
        };
        let payload_offset = n - payload.len();
        let payload = buf.split_to(n).freeze().slice(payload_offset..);
        let key = (src, header.connection_id);
        {
            let reg = registry.lock();
            if let Some(entry) = reg.get(&key) {
                let _ = entry.sender.try_send((header, payload));
                continue;
            }
            if header.packet_type == PacketType::Reset {
                for id in [
                    header.connection_id.wrapping_sub(1),
                    header.connection_id.wrapping_add(1),
                ] {
                    if let Some(entry) = reg.get(&(src, id)) {
                        let _ = entry.sender.try_send((header.clone(), payload.clone()));
                        break;
                    }
                }
                continue;
            }
        }
        match header.packet_type {
            PacketType::Syn => open_inbound(&sender, &registry, &accept_tx, src, &header),
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
    accept_tx: &mpsc::Sender<UtpStream>,
    src: SocketAddr,
    syn: &UtpHeader,
) {
    let send_id = syn.connection_id;
    let recv_id = send_id.wrapping_add(1);
    let key = (src, recv_id);

    let (token, inc_rx, permit) = {
        let mut reg = registry.lock();
        if let Some(existing) = reg.get(&key) {
            let _ = existing.sender.try_send((syn.clone(), Bytes::new()));
            return;
        }
        let Ok(permit) = accept_tx.try_reserve() else {
            return;
        };
        let (inc_tx, inc_rx) = mpsc::channel(stream::INCOMING_QUEUE_PACKETS);
        let token = ConnectionToken::new();
        reg.insert(
            key,
            ConnRegistration {
                sender: inc_tx,
                token: token.clone(),
            },
        );
        (token, inc_rx, permit)
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
    tokio::spawn(stream::drive(shared.clone(), cfg, Role::Responder));
    permit.send(UtpStream::new(shared));
}

async fn proxy_router(
    datagram: Arc<ProxyAssociation>,
    registry: ConnRegistry,
    proxy_registry: ProxyConnRegistry,
) {
    let mut buf = BytesMut::with_capacity(ROUTER_READ_SLAB);
    buf.resize(ROUTER_READ_SLAB, 0);
    loop {
        if buf.len() < MAX_DATAGRAM {
            buf.resize(ROUTER_READ_SLAB, 0);
        }
        let (n, src) = match datagram.recv_from_target(&mut buf[..MAX_DATAGRAM]).await {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!("uTP proxy receive loop terminated: {error}");
                return;
            }
        };
        let Ok((header, payload)) = UtpHeader::decode(&buf[..n]) else {
            continue;
        };
        let payload_offset = n - payload.len();
        let payload = buf.split_to(n).freeze().slice(payload_offset..);
        match src {
            ProxyDatagramSource::Ip(src) => {
                let candidates: &[u16] = if header.packet_type == PacketType::Reset {
                    &[
                        header.connection_id,
                        header.connection_id.wrapping_sub(1),
                        header.connection_id.wrapping_add(1),
                    ]
                } else {
                    &[header.connection_id]
                };
                let Some((key, connection)) = candidates.iter().find_map(|id| {
                    let key = (src, *id);
                    let connection = proxy_registry.lock().get(id).cloned()?;
                    (connection.key == key).then_some((key, connection))
                }) else {
                    continue;
                };
                if let Some(entry) = registry.lock().get(&key) {
                    if entry.token.matches(&connection.token) {
                        let _ = entry.sender.try_send((header, payload));
                    }
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
                let relay_port = relay.local_addr().unwrap().port().to_be_bytes();
                let mut greeting = [0u8; 3];
                control.read_exact(&mut greeting).await.unwrap();
                control.write_all(&[5, 0]).await.unwrap();
                let mut associate = [0u8; 10];
                control.read_exact(&mut associate).await.unwrap();
                control
                    .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, relay_port[0], relay_port[1]])
                    .await
                    .unwrap();
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

    #[tokio::test]
    async fn reset_addressed_to_send_id_aborts_the_connection() {
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
                reset.connection_id = syn.connection_id.wrapping_add(1);
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

    #[tokio::test]
    async fn unaccepted_syns_stop_at_the_backlog() {
        let socket = UtpSocket::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let raw = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        for id in 0..(ACCEPT_BACKLOG as u16 + 50) {
            let syn = UtpHeader {
                packet_type: PacketType::Syn,
                connection_id: id.wrapping_mul(2),
                timestamp_micros: 0,
                timestamp_diff_micros: 0,
                wnd_size: 65536,
                seq_nr: 1,
                ack_nr: 0,
                selective_ack: None,
            };
            raw.send_to(&syn.encode(&[]), socket.local_addr())
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(socket.registry.lock().len(), ACCEPT_BACKLOG);
    }
}
