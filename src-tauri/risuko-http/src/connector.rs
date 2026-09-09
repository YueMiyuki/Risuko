//! TCP / TLS / SOCKS5 / HTTP-proxy connector

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};

use http::Uri;
use hyper::rt::ReadBufCursor;
use hyper_util::client::legacy::connect::{Connected, Connection};
use hyper_util::rt::TokioIo;
use rustls::pki_types::ServerName;
use rustls::ClientConfig;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpStream, UdpSocket};
use tokio::sync::Mutex;
use tokio_rustls::client::TlsStream;
use tokio_rustls::TlsConnector;
use tower_service::Service;

use crate::error::{Error, Result as HttpResult};
use crate::proxy::{NoProxy, Proxy, ProxyScheme};
use crate::resolver::{GlobalResolver, Resolve, SharedResolver};

#[derive(Clone)]
pub(crate) struct Connector {
    pub(crate) tls: Arc<ClientConfig>,
    pub(crate) resolver: SharedResolver,
    pub(crate) proxy: Option<Arc<Proxy>>,
    pub(crate) no_proxy: Option<Arc<NoProxy>>,
    pub(crate) connect_timeout: Option<Duration>,
    pub(crate) tcp_nodelay: bool,
    pub(crate) tcp_keepalive: Option<Duration>,
    pub(crate) local_addr: Option<SocketAddr>,
}

impl Connector {
    fn tls_connector(&self) -> TlsConnector {
        TlsConnector::from(self.tls.clone())
    }
}

#[derive(Clone)]
pub struct ProxyConnector {
    inner: Connector,
    udp_inner: Option<Connector>,
}

impl fmt::Debug for ProxyConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyConnector")
            .field(
                "proxy",
                &self.inner.proxy.as_deref().map(redacted_proxy_url),
            )
            .field("no_proxy", &self.inner.no_proxy.as_deref())
            .field(
                "udp_proxy",
                &self.udp_inner().proxy.as_deref().map(redacted_proxy_url),
            )
            .field("udp_no_proxy", &self.udp_inner().no_proxy.as_deref())
            .finish()
    }
}

fn redacted_proxy_url(proxy: &Proxy) -> String {
    proxy.redacted_url()
}

impl ProxyConnector {
    pub fn new(proxy: Option<Proxy>) -> Self {
        let no_proxy = proxy
            .as_ref()
            .and_then(|proxy| proxy.no_proxy.as_deref().cloned())
            .unwrap_or_default();
        Self {
            inner: Connector {
                tls: Arc::new(empty_tls_config()),
                resolver: Arc::new(GlobalResolver),
                proxy: proxy.map(Arc::new),
                no_proxy: Some(Arc::new(no_proxy)),
                connect_timeout: None,
                tcp_nodelay: true,
                tcp_keepalive: None,
                local_addr: None,
            },
            udp_inner: None,
        }
    }

    /// Construct a direct-only connector
    pub fn direct() -> Self {
        Self::new(None)
    }

    /// Construct a connector for one proxy URL
    pub fn from_proxy(proxy: Proxy) -> Self {
        Self::new(Some(proxy))
    }

    /// Return the configured proxy, if any
    pub fn proxy(&self) -> Option<Proxy> {
        self.inner.proxy.as_deref().cloned()
    }

    pub fn udp_proxy(&self) -> Option<Proxy> {
        self.udp_inner().proxy.as_deref().cloned()
    }

    pub fn has_proxy(&self) -> bool {
        self.inner.proxy.is_some() || self.udp_inner().proxy.is_some()
    }

    /// Whether this route can carry UDP datagrams
    pub fn supports_udp(&self) -> bool {
        self.udp_inner()
            .proxy
            .as_deref()
            .is_none_or(|proxy| !matches!(proxy.scheme(), ProxyScheme::Http))
    }

    /// Return the configured bypass matcher, if any
    pub fn no_proxy(&self) -> Option<NoProxy> {
        self.inner.no_proxy.as_deref().cloned()
    }

    pub fn udp_no_proxy(&self) -> Option<NoProxy> {
        self.udp_inner().no_proxy.as_deref().cloned()
    }

    /// Replace the bypass matcher used for both TCP and UDP destinations
    pub fn with_no_proxy(mut self, no_proxy: NoProxy) -> Self {
        let no_proxy = Arc::new(no_proxy);
        self.inner.no_proxy = Some(no_proxy.clone());
        if let Some(udp_inner) = self.udp_inner.as_mut() {
            udp_inner.no_proxy = Some(no_proxy);
        }
        self
    }

    pub fn with_udp_no_proxy(mut self, no_proxy: NoProxy) -> Self {
        if let Some(udp_inner) = self.udp_inner.as_mut() {
            udp_inner.no_proxy = Some(Arc::new(no_proxy));
        } else {
            self.inner.no_proxy = Some(Arc::new(no_proxy));
        }
        self
    }

    pub fn with_udp_proxy(mut self, udp: Option<ProxyConnector>) -> Self {
        self.udp_inner = udp.map(|connector| connector.udp_inner.unwrap_or(connector.inner));
        self
    }

    /// Set the timeout for DNS/TCP/proxy handshakes
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.inner.connect_timeout = Some(timeout);
        if let Some(udp_inner) = self.udp_inner.as_mut() {
            udp_inner.connect_timeout = Some(timeout);
        }
        self
    }

    /// Set TCP_NODELAY for direct and tunneled TCP streams
    pub fn tcp_nodelay(mut self, enabled: bool) -> Self {
        self.inner.tcp_nodelay = enabled;
        if let Some(udp_inner) = self.udp_inner.as_mut() {
            udp_inner.tcp_nodelay = enabled;
        }
        self
    }

    /// Set TCP keepalive for direct and proxy control connections
    pub fn tcp_keepalive(mut self, keepalive: Option<Duration>) -> Self {
        self.inner.tcp_keepalive = keepalive;
        if let Some(udp_inner) = self.udp_inner.as_mut() {
            udp_inner.tcp_keepalive = keepalive;
        }
        self
    }

    pub fn resolver<R>(mut self, resolver: R) -> Self
    where
        R: Resolve + 'static,
    {
        let resolver = Arc::new(resolver);
        self.inner.resolver = resolver.clone();
        if let Some(udp_inner) = self.udp_inner.as_mut() {
            udp_inner.resolver = resolver;
        }
        self
    }

    /// Use a shared resolver object.
    pub fn resolver_arc(mut self, resolver: Arc<dyn Resolve>) -> Self {
        self.inner.resolver = resolver.clone();
        if let Some(udp_inner) = self.udp_inner.as_mut() {
            udp_inner.resolver = resolver;
        }
        self
    }

    fn udp_inner(&self) -> &Connector {
        self.udp_inner.as_ref().unwrap_or(&self.inner)
    }

    pub async fn connect_tcp(&self, host: &str, port: u16) -> HttpResult<BoxedIo> {
        let operation = self.connect_tcp_inner(host, port);
        match self.inner.connect_timeout {
            Some(timeout) => tokio::time::timeout(timeout, operation)
                .await
                .map_err(|_| Error::ProxyTimeout(format!("TCP connection to {host}:{port}")))?,
            None => operation.await,
        }
    }

    async fn connect_tcp_inner(&self, host: &str, port: u16) -> HttpResult<BoxedIo> {
        let host = normalize_target_host(host);
        let bypass = self
            .inner
            .no_proxy
            .as_deref()
            .is_some_and(|matcher| matcher.matches_host_port(&host, Some(port)));

        let Some(proxy) = self.inner.proxy.as_deref() else {
            return Ok(BoxedIo::new(self.inner.direct(&host, port).await?));
        };
        if bypass {
            return Ok(BoxedIo::new(self.inner.direct(&host, port).await?));
        }

        self.inner.via_proxy(proxy, &host, port, true).await
    }

    pub async fn bind_udp(&self) -> HttpResult<ProxyDatagram> {
        let connector = self.udp_inner().clone();
        let timeout = connector.connect_timeout;
        let operation = Self::bind_udp_inner(connector);
        match timeout {
            Some(timeout) => tokio::time::timeout(timeout, operation)
                .await
                .map_err(|_| Error::ProxyTimeout("UDP association".into()))?,
            None => operation.await,
        }
    }

    pub async fn bind_udp_with_bypass(&self) -> HttpResult<ProxyDatagram> {
        match self.bind_udp().await {
            Ok(datagram) => Ok(datagram),
            Err(error)
                if self.udp_inner().proxy.is_some()
                    && self
                        .udp_inner()
                        .no_proxy
                        .as_deref()
                        .is_some_and(|no_proxy| !no_proxy.is_empty()) =>
            {
                let no_proxy = self
                    .udp_inner()
                    .no_proxy
                    .as_deref()
                    .cloned()
                    .unwrap_or_default();
                ProxyDatagram::blocked(self.udp_inner().clone(), no_proxy, error).await
            }
            Err(error) => Err(error),
        }
    }

    async fn bind_udp_inner(connector: Connector) -> HttpResult<ProxyDatagram> {
        match connector.proxy.as_deref() {
            None => ProxyDatagram::direct(connector).await,
            Some(proxy) if matches!(proxy.scheme(), ProxyScheme::Http) => {
                Err(Error::Socks5Required)
            }
            Some(proxy) => {
                ProxyDatagram::socks5(
                    connector.clone(),
                    proxy.clone(),
                    connector.no_proxy.as_deref().cloned().unwrap_or_default(),
                )
                .await
            }
        }
    }
}

impl Default for ProxyConnector {
    fn default() -> Self {
        Self::direct()
    }
}

fn empty_tls_config() -> ClientConfig {
    ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth()
}

fn normalize_target_host(host: &str) -> String {
    host.trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .to_string()
}

pub struct ProxyDatagram {
    socket: Arc<UdpSocket>,
    secondary_socket: Option<Arc<UdpSocket>>,
    route: DatagramRoute,
    direct_targets: Arc<std::sync::Mutex<HashSet<SocketAddr>>>,
}

const SOCKS5_UDP_MAX_ADDRESS_OVERHEAD: usize = 3 + 1 + 1 + 255 + 2;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyDatagramSource {
    Ip(SocketAddr),
    Host(String, u16),
}

/// Match a datagram source against the endpoint used for an outstanding
/// request without resolving a proxy-supplied hostname locally.
pub fn datagram_source_matches(source: &ProxyDatagramSource, target: SocketAddr) -> bool {
    match source {
        ProxyDatagramSource::Ip(source) => *source == target,
        ProxyDatagramSource::Host(host, port) => {
            if *port != target.port() {
                return false;
            }
            host.parse::<IpAddr>()
                .is_ok_and(|ip| SocketAddr::new(ip, *port) == target)
        }
    }
}

fn direct_datagram_source_allowed(no_proxy: &NoProxy, source: SocketAddr) -> bool {
    no_proxy.matches_host_port(&source.ip().to_string(), Some(source.port()))
}

fn clone_route_error(error: &Error) -> Error {
    match error {
        Error::Url(message) => Error::Url(message.clone()),
        Error::Header(message) => Error::Header(message.clone()),
        Error::Builder(message) => Error::Builder(message.clone()),
        Error::Connect(message) => Error::Connect(message.clone()),
        Error::Socks5Required => Error::Socks5Required,
        Error::ProxyAuthentication(message) => Error::ProxyAuthentication(message.clone()),
        Error::ProxyProtocol(message) => Error::ProxyProtocol(message.clone()),
        Error::ProxyTimeout(message) => Error::ProxyTimeout(message.clone()),
        Error::Request(message) => Error::Request(message.clone()),
        Error::Redirect(message) => Error::Redirect(message.clone()),
        Error::Body(message) => Error::Body(message.clone()),
        Error::Decode(message) => Error::Decode(message.clone()),
        Error::Encode(message) => Error::Encode(message.clone()),
        Error::NoRecords => Error::NoRecords,
        Error::Timeout => Error::Timeout,
        Error::Status(status) => Error::Status(*status),
        Error::Io(error) => match error.raw_os_error() {
            Some(code) => Error::Io(std::io::Error::from_raw_os_error(code)),
            None => Error::Io(std::io::Error::new(error.kind(), error.to_string())),
        },
        Error::Tls(message) => Error::Tls(message.clone()),
    }
}

enum DatagramRoute {
    Direct {
        connector: Connector,
    },
    Socks5 {
        connector: Connector,
        relay: SocketAddr,
        no_proxy: NoProxy,
        resolve_locally: bool,
        _control: Arc<Mutex<BoxedIo>>,
    },
    Blocked {
        connector: Connector,
        no_proxy: NoProxy,
        error: Arc<Error>,
    },
}

impl ProxyDatagram {
    async fn direct(connector: Connector) -> HttpResult<Self> {
        let (socket, secondary_socket) = bind_udp_socket_pair(false).await?;
        Ok(Self {
            socket,
            secondary_socket,
            route: DatagramRoute::Direct { connector },
            direct_targets: Arc::new(std::sync::Mutex::new(HashSet::new())),
        })
    }

    async fn blocked(connector: Connector, no_proxy: NoProxy, error: Error) -> HttpResult<Self> {
        let (socket, secondary_socket) = bind_udp_socket_pair(false).await?;
        Ok(Self {
            socket,
            secondary_socket,
            route: DatagramRoute::Blocked {
                connector,
                no_proxy,
                error: Arc::new(error),
            },
            direct_targets: Arc::new(std::sync::Mutex::new(HashSet::new())),
        })
    }

    async fn socks5(connector: Connector, proxy: Proxy, no_proxy: NoProxy) -> HttpResult<Self> {
        let (control, relay, resolve_locally) = socks5_udp_associate(&connector, &proxy).await?;
        let (socket, secondary_socket) = bind_udp_socket_pair(relay.is_ipv6()).await?;
        Ok(Self {
            socket,
            secondary_socket,
            route: DatagramRoute::Socks5 {
                connector,
                relay,
                no_proxy,
                resolve_locally,
                _control: Arc::new(Mutex::new(control)),
            },
            direct_targets: Arc::new(std::sync::Mutex::new(HashSet::new())),
        })
    }

    fn socket_for_target(&self, target: SocketAddr) -> Arc<UdpSocket> {
        let primary_is_ipv6 = self
            .socket
            .local_addr()
            .map(|address| address.is_ipv6())
            .unwrap_or(false);
        if target.is_ipv6() == primary_is_ipv6 {
            self.socket.clone()
        } else {
            self.secondary_socket
                .clone()
                .unwrap_or_else(|| self.socket.clone())
        }
    }

    async fn recv_from_any(
        &self,
        primary: &mut [u8],
        secondary: &mut [u8],
    ) -> std::io::Result<(usize, SocketAddr, bool)> {
        let Some(secondary_socket) = &self.secondary_socket else {
            let (length, source) = self.socket.recv_from(primary).await?;
            return Ok((length, source, true));
        };

        tokio::select! {
            result = self.socket.recv_from(primary) => {
                result.map(|(length, source)| (length, source, true))
            }
            result = secondary_socket.recv_from(secondary) => {
                result.map(|(length, source)| (length, source, false))
            }
        }
    }

    fn remember_direct_target(&self, target: SocketAddr) {
        const MAX_DIRECT_TARGETS: usize = 4096;
        let Ok(mut targets) = self.direct_targets.lock() else {
            return;
        };
        if targets.len() >= MAX_DIRECT_TARGETS {
            targets.clear();
        }
        targets.insert(target);
    }

    fn allows_direct_source(&self, no_proxy: &NoProxy, source: SocketAddr) -> bool {
        direct_datagram_source_allowed(no_proxy, source)
            || self
                .direct_targets
                .lock()
                .is_ok_and(|targets| targets.contains(&source))
    }

    /// Return the local UDP address
    pub fn local_addr(&self) -> HttpResult<SocketAddr> {
        self.socket.local_addr().map_err(Error::Io)
    }

    /// Send a datagram to a resolved destination
    pub async fn send_to(&self, payload: &[u8], target: SocketAddr) -> HttpResult<usize> {
        match &self.route {
            DatagramRoute::Direct { .. } => self
                .socket_for_target(target)
                .send_to(payload, target)
                .await
                .map_err(Error::Io),
            DatagramRoute::Socks5 {
                relay, no_proxy, ..
            } if no_proxy.matches_host_port(&target.ip().to_string(), Some(target.port())) => self
                .socket_for_target(target)
                .send_to(payload, target)
                .await
                .inspect(|_| {
                    self.remember_direct_target(target);
                })
                .map_err(Error::Io),
            DatagramRoute::Socks5 { relay, .. } => {
                let frame = socks5_udp_frame(payload, &ProxyTarget::Ip(target))?;
                let n = self
                    .socket
                    .send_to(&frame, relay)
                    .await
                    .map_err(Error::Io)?;
                if n == frame.len() {
                    Ok(payload.len())
                } else {
                    Err(Error::ProxyProtocol("short SOCKS5 UDP write".into()))
                }
            }
            DatagramRoute::Blocked {
                connector: _,
                no_proxy,
                error,
            } => {
                if no_proxy.matches_host_port(&target.ip().to_string(), Some(target.port())) {
                    let result = self
                        .socket_for_target(target)
                        .send_to(payload, target)
                        .await
                        .map_err(Error::Io);
                    if result.is_ok() {
                        self.remember_direct_target(target);
                    }
                    result
                } else {
                    Err(clone_route_error(error))
                }
            }
        }
    }

    pub async fn send_to_host(&self, payload: &[u8], host: &str, port: u16) -> HttpResult<usize> {
        let host = normalize_target_host(host);
        match &self.route {
            DatagramRoute::Direct { connector } => {
                let target = resolve_first(connector, &host, port).await?;
                self.socket_for_target(target)
                    .send_to(payload, target)
                    .await
                    .map_err(Error::Io)
            }
            DatagramRoute::Socks5 {
                connector,
                relay,
                no_proxy,
                resolve_locally,
                ..
            } => {
                if no_proxy.matches_host_port(&host, Some(port)) {
                    let target = resolve_first(connector, &host, port).await?;
                    let result = self
                        .socket_for_target(target)
                        .send_to(payload, target)
                        .await
                        .map_err(Error::Io);
                    if result.is_ok() {
                        self.remember_direct_target(target);
                    }
                    return result;
                }
                let target = if let Ok(ip) = host.parse::<IpAddr>() {
                    ProxyTarget::Ip(SocketAddr::new(ip, port))
                } else if *resolve_locally {
                    ProxyTarget::Ip(resolve_first(connector, &host, port).await?)
                } else {
                    ProxyTarget::Host(host, port)
                };
                let frame = socks5_udp_frame(payload, &target)?;
                let n = self
                    .socket
                    .send_to(&frame, relay)
                    .await
                    .map_err(Error::Io)?;
                if n == frame.len() {
                    Ok(payload.len())
                } else {
                    Err(Error::ProxyProtocol("short SOCKS5 UDP write".into()))
                }
            }
            DatagramRoute::Blocked {
                connector,
                no_proxy,
                error,
            } => {
                if !no_proxy.matches_host_port(&host, Some(port)) {
                    return Err(clone_route_error(error));
                }
                let target = resolve_first(connector, &host, port).await?;
                let result = self
                    .socket_for_target(target)
                    .send_to(payload, target)
                    .await
                    .map_err(Error::Io);
                if result.is_ok() {
                    self.remember_direct_target(target);
                }
                result
            }
        }
    }

    pub async fn send_to_addr(&self, payload: &[u8], target: SocketAddr) -> HttpResult<usize> {
        self.send_to(payload, target).await
    }

    pub async fn recv_from(&self, buffer: &mut [u8]) -> HttpResult<(usize, SocketAddr)> {
        let (n, source) = self.recv_from_target(buffer).await?;
        let source = match source {
            ProxyDatagramSource::Ip(addr) => addr,
            ProxyDatagramSource::Host(_, port) => {
                SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), port)
            }
        };
        Ok((n, source))
    }

    /// Receive one datagram while preserving a domain-form SOCKS5 source
    pub async fn recv_from_target(
        &self,
        buffer: &mut [u8],
    ) -> HttpResult<(usize, ProxyDatagramSource)> {
        let mut packet = vec![0u8; buffer.len().saturating_add(SOCKS5_UDP_MAX_ADDRESS_OVERHEAD)];
        let mut secondary_packet = vec![0u8; packet.len()];
        loop {
            let (n, from, primary) = self
                .recv_from_any(&mut packet, &mut secondary_packet)
                .await
                .map_err(Error::Io)?;
            let received = if primary {
                &packet[..n]
            } else {
                &secondary_packet[..n]
            };
            match &self.route {
                DatagramRoute::Direct { .. } => {
                    let copy_len = n.min(buffer.len());
                    buffer[..copy_len].copy_from_slice(&received[..copy_len]);
                    return Ok((copy_len, ProxyDatagramSource::Ip(from)));
                }
                DatagramRoute::Socks5 {
                    relay, no_proxy, ..
                } if from != *relay => {
                    if !self.allows_direct_source(no_proxy, from) {
                        continue;
                    }
                    let copy_len = n.min(buffer.len());
                    buffer[..copy_len].copy_from_slice(&received[..copy_len]);
                    return Ok((copy_len, ProxyDatagramSource::Ip(from)));
                }
                DatagramRoute::Socks5 { .. } => {
                    let (payload, target) = parse_socks5_udp_packet(received)?;
                    if payload.len() > buffer.len() {
                        return Err(Error::ProxyProtocol(
                            "SOCKS5 UDP payload exceeds receive buffer".into(),
                        ));
                    }
                    buffer[..payload.len()].copy_from_slice(&payload);
                    return Ok((payload.len(), target));
                }
                DatagramRoute::Blocked { no_proxy, .. } => {
                    if !self.allows_direct_source(no_proxy, from) {
                        continue;
                    }
                    let copy_len = n.min(buffer.len());
                    buffer[..copy_len].copy_from_slice(&received[..copy_len]);
                    return Ok((copy_len, ProxyDatagramSource::Ip(from)));
                }
            }
        }
    }
}

async fn bind_udp_socket_pair(
    primary_ipv6: bool,
) -> HttpResult<(Arc<UdpSocket>, Option<Arc<UdpSocket>>)> {
    let primary_addr = if primary_ipv6 { "[::]:0" } else { "0.0.0.0:0" };
    let secondary_addr = if primary_ipv6 { "0.0.0.0:0" } else { "[::]:0" };
    let primary = Arc::new(UdpSocket::bind(primary_addr).await.map_err(Error::Io)?);
    let secondary = match UdpSocket::bind(secondary_addr).await {
        Ok(socket) => Some(Arc::new(socket)),
        Err(error) => {
            tracing::debug!(
                address = secondary_addr,
                "opposite-family UDP socket unavailable: {error}"
            );
            None
        }
    };
    Ok((primary, secondary))
}

impl std::fmt::Debug for ProxyDatagram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyDatagram")
            .field("local_addr", &self.local_addr().ok())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
enum ProxyTarget {
    Ip(SocketAddr),
    Host(String, u16),
}

async fn socks5_udp_associate(
    connector: &Connector,
    proxy: &Proxy,
) -> HttpResult<(BoxedIo, SocketAddr, bool)> {
    let phost = proxy
        .url()
        .host_str()
        .ok_or_else(|| Error::Url("proxy missing host".into()))?
        .to_string();
    let pport = proxy.url().port().unwrap_or(1080);
    let stream = connector.direct(&phost, pport).await?;
    connector.tune(&stream)?;
    let peer = stream
        .peer_addr()
        .map_err(|error| Error::Connect(format!("proxy peer address: {error}")))?;
    let resolve_locally = matches!(
        proxy.scheme(),
        ProxyScheme::Socks5 {
            resolve_locally: true
        }
    );
    let future = socks5_udp_associate_inner(
        stream,
        proxy,
        peer,
        resolve_locally,
        &connector.resolver,
        connector.connect_timeout,
    );
    let (stream, relay) = match connector.connect_timeout {
        Some(timeout) => tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| Error::ProxyTimeout("SOCKS5 UDP associate".into()))??,
        None => future.await?,
    };
    Ok((stream, relay, resolve_locally))
}

async fn socks5_udp_associate_inner(
    mut stream: TcpStream,
    proxy: &Proxy,
    proxy_peer: SocketAddr,
    _resolve_locally: bool,
    resolver: &SharedResolver,
    resolve_timeout: Option<Duration>,
) -> HttpResult<(BoxedIo, SocketAddr)> {
    socks5_negotiate(&mut stream, proxy).await?;

    stream
        .write_all(&[5, 3, 0, 1, 0, 0, 0, 0, 0, 0])
        .await
        .map_err(|error| Error::ProxyProtocol(format!("UDP ASSOCIATE write: {error}")))?;

    let mut header = [0u8; 4];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|error| Error::ProxyProtocol(format!("UDP ASSOCIATE response: {error}")))?;
    if header[0] != 5 {
        return Err(Error::ProxyProtocol("invalid SOCKS5 version".into()));
    }
    if header[1] != 0 {
        return Err(Error::ProxyProtocol(format!(
            "UDP ASSOCIATE rejected (reply {})",
            header[1]
        )));
    }
    if header[2] != 0 {
        return Err(Error::ProxyProtocol("invalid SOCKS5 reserved byte".into()));
    }
    let mut relay = read_socks_address(&mut stream, header[3], resolver, resolve_timeout).await?;
    if relay.ip().is_unspecified() {
        relay.set_ip(proxy_peer.ip());
    }
    Ok((BoxedIo::new(stream), relay))
}

async fn socks5_negotiate(stream: &mut TcpStream, proxy: &Proxy) -> HttpResult<()> {
    let user = percent_decode_str(proxy.url().username());
    let pass = percent_decode_str(proxy.url().password().unwrap_or_default());
    socks5_negotiate_auth(
        stream,
        (!user.is_empty()).then_some((user.as_str(), pass.as_str())),
    )
    .await
}

async fn socks5_negotiate_auth(
    stream: &mut TcpStream,
    auth: Option<(&str, &str)>,
) -> HttpResult<()> {
    let has_credentials = auth.is_some();
    let methods: &[u8] = if has_credentials { &[0, 2] } else { &[0] };
    let method_count = u8::try_from(methods.len())
        .map_err(|_| Error::ProxyProtocol("too many SOCKS5 auth methods".into()))?;
    stream
        .write_all(&[5, method_count])
        .await
        .map_err(|error| Error::ProxyProtocol(format!("SOCKS5 greeting: {error}")))?;
    stream
        .write_all(methods)
        .await
        .map_err(|error| Error::ProxyProtocol(format!("SOCKS5 greeting: {error}")))?;

    let mut selected = [0u8; 2];
    stream
        .read_exact(&mut selected)
        .await
        .map_err(|error| Error::ProxyProtocol(format!("SOCKS5 greeting response: {error}")))?;
    if selected[0] != 5 {
        return Err(Error::ProxyProtocol("invalid SOCKS5 version".into()));
    }
    match selected[1] {
        0 => Ok(()),
        2 if has_credentials => {
            let (user, pass) = auth.expect("checked has_credentials");
            let username = user.as_bytes();
            let password = pass.as_bytes();
            let username_len = u8::try_from(username.len())
                .map_err(|_| Error::ProxyAuthentication("username exceeds 255 bytes".into()))?;
            let password_len = u8::try_from(password.len())
                .map_err(|_| Error::ProxyAuthentication("password exceeds 255 bytes".into()))?;
            let mut request = Vec::with_capacity(3 + username.len() + password.len());
            request.extend_from_slice(&[1, username_len]);
            request.extend_from_slice(username);
            request.push(password_len);
            request.extend_from_slice(password);
            stream
                .write_all(&request)
                .await
                .map_err(|error| Error::ProxyAuthentication(error.to_string()))?;
            let mut response = [0u8; 2];
            stream
                .read_exact(&mut response)
                .await
                .map_err(|error| Error::ProxyAuthentication(error.to_string()))?;
            if response[1] == 0 {
                Ok(())
            } else {
                Err(Error::ProxyAuthentication(format!(
                    "proxy rejected credentials (status {})",
                    response[1]
                )))
            }
        }
        2 => Err(Error::ProxyAuthentication(
            "proxy requires username/password authentication".into(),
        )),
        0xff => Err(Error::ProxyAuthentication(
            "proxy offered no supported authentication method".into(),
        )),
        method => Err(Error::ProxyProtocol(format!(
            "unsupported SOCKS5 authentication method {method}"
        ))),
    }
}

fn socks5_udp_frame(payload: &[u8], target: &ProxyTarget) -> HttpResult<Vec<u8>> {
    let mut frame = Vec::with_capacity(payload.len() + 32);
    frame.extend_from_slice(&[0, 0, 0]); // RSV, RSV, FRAG
    match target {
        ProxyTarget::Ip(addr) => match addr.ip() {
            IpAddr::V4(ip) => {
                frame.push(1);
                frame.extend_from_slice(&ip.octets());
            }
            IpAddr::V6(ip) => {
                frame.push(4);
                frame.extend_from_slice(&ip.octets());
            }
        },
        ProxyTarget::Host(host, _) => {
            let host = normalize_target_host(host);
            let bytes = host.as_bytes();
            let len = u8::try_from(bytes.len())
                .map_err(|_| Error::ProxyProtocol("SOCKS5 hostname exceeds 255 bytes".into()))?;
            if bytes.is_empty() {
                return Err(Error::ProxyProtocol(
                    "empty SOCKS5 destination hostname".into(),
                ));
            }
            frame.push(3);
            frame.push(len);
            frame.extend_from_slice(bytes);
        }
    }
    let port = match target {
        ProxyTarget::Ip(addr) => addr.port(),
        ProxyTarget::Host(_, port) => *port,
    };
    frame.extend_from_slice(&port.to_be_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn parse_socks5_udp_packet(packet: &[u8]) -> HttpResult<(Vec<u8>, ProxyDatagramSource)> {
    if packet.len() < 4 || packet[0] != 0 || packet[1] != 0 || packet[2] != 0 {
        return Err(Error::ProxyProtocol("invalid SOCKS5 UDP header".into()));
    }
    let mut cursor = 3;
    let (target, next) = parse_socks_address(packet, cursor)?;
    cursor = next;
    if cursor > packet.len() {
        return Err(Error::ProxyProtocol("truncated SOCKS5 UDP payload".into()));
    }
    let source = match target {
        ProxyTarget::Ip(addr) => ProxyDatagramSource::Ip(addr),
        ProxyTarget::Host(host, port) => ProxyDatagramSource::Host(host, port),
    };
    Ok((packet[cursor..].to_vec(), source))
}

fn parse_socks_address(packet: &[u8], mut cursor: usize) -> HttpResult<(ProxyTarget, usize)> {
    let atyp = *packet
        .get(cursor)
        .ok_or_else(|| Error::ProxyProtocol("missing SOCKS5 address type".into()))?;
    cursor += 1;
    let target = match atyp {
        1 => {
            let bytes = packet
                .get(cursor..cursor + 4)
                .ok_or_else(|| Error::ProxyProtocol("truncated SOCKS5 IPv4 address".into()))?;
            cursor += 4;
            let ip = std::net::Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3]);
            let port = read_be_port(packet, &mut cursor)?;
            ProxyTarget::Ip(SocketAddr::new(IpAddr::V4(ip), port))
        }
        3 => {
            let len =
                usize::from(*packet.get(cursor).ok_or_else(|| {
                    Error::ProxyProtocol("missing SOCKS5 hostname length".into())
                })?);
            cursor += 1;
            let bytes = packet
                .get(cursor..cursor + len)
                .ok_or_else(|| Error::ProxyProtocol("truncated SOCKS5 hostname".into()))?;
            cursor += len;
            let host = std::str::from_utf8(bytes)
                .map_err(|error| Error::ProxyProtocol(format!("invalid SOCKS5 hostname: {error}")))?
                .to_string();
            let port = read_be_port(packet, &mut cursor)?;
            ProxyTarget::Host(host, port)
        }
        4 => {
            let bytes = packet
                .get(cursor..cursor + 16)
                .ok_or_else(|| Error::ProxyProtocol("truncated SOCKS5 IPv6 address".into()))?;
            cursor += 16;
            let mut octets = [0u8; 16];
            octets.copy_from_slice(bytes);
            let ip = std::net::Ipv6Addr::from(octets);
            let port = read_be_port(packet, &mut cursor)?;
            ProxyTarget::Ip(SocketAddr::new(IpAddr::V6(ip), port))
        }
        atyp => {
            return Err(Error::ProxyProtocol(format!(
                "unsupported SOCKS5 UDP address type {atyp}"
            )))
        }
    };
    Ok((target, cursor))
}

fn read_be_port(packet: &[u8], cursor: &mut usize) -> HttpResult<u16> {
    let bytes = packet
        .get(*cursor..*cursor + 2)
        .ok_or_else(|| Error::ProxyProtocol("truncated SOCKS5 port".into()))?;
    *cursor += 2;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

async fn read_socks_target<R: AsyncRead + Unpin>(
    reader: &mut R,
    atyp: u8,
) -> HttpResult<ProxyTarget> {
    match atyp {
        1 => {
            let mut bytes = [0u8; 4];
            reader
                .read_exact(&mut bytes)
                .await
                .map_err(|error| Error::ProxyProtocol(error.to_string()))?;
            let mut port = [0u8; 2];
            reader
                .read_exact(&mut port)
                .await
                .map_err(|error| Error::ProxyProtocol(error.to_string()))?;
            Ok(ProxyTarget::Ip(SocketAddr::new(
                IpAddr::V4(std::net::Ipv4Addr::from(bytes)),
                u16::from_be_bytes(port),
            )))
        }
        3 => {
            let mut len = [0u8; 1];
            reader
                .read_exact(&mut len)
                .await
                .map_err(|error| Error::ProxyProtocol(error.to_string()))?;
            let mut bytes = vec![0u8; usize::from(len[0])];
            reader
                .read_exact(&mut bytes)
                .await
                .map_err(|error| Error::ProxyProtocol(error.to_string()))?;
            let host = String::from_utf8(bytes).map_err(|error| {
                Error::ProxyProtocol(format!("invalid relay hostname: {error}"))
            })?;
            let mut port = [0u8; 2];
            reader
                .read_exact(&mut port)
                .await
                .map_err(|error| Error::ProxyProtocol(error.to_string()))?;
            let port = u16::from_be_bytes(port);
            Ok(ProxyTarget::Host(host, port))
        }
        4 => {
            let mut bytes = [0u8; 16];
            reader
                .read_exact(&mut bytes)
                .await
                .map_err(|error| Error::ProxyProtocol(error.to_string()))?;
            let mut port = [0u8; 2];
            reader
                .read_exact(&mut port)
                .await
                .map_err(|error| Error::ProxyProtocol(error.to_string()))?;
            Ok(ProxyTarget::Ip(SocketAddr::new(
                IpAddr::V6(std::net::Ipv6Addr::from(bytes)),
                u16::from_be_bytes(port),
            )))
        }
        other => Err(Error::ProxyProtocol(format!(
            "unsupported SOCKS5 relay address type {other}"
        ))),
    }
}

async fn read_socks_address<R: AsyncRead + Unpin>(
    reader: &mut R,
    atyp: u8,
    resolver: &SharedResolver,
    resolve_timeout: Option<Duration>,
) -> HttpResult<SocketAddr> {
    match read_socks_target(reader, atyp).await? {
        ProxyTarget::Ip(addr) => Ok(addr),
        ProxyTarget::Host(host, port) => {
            let resolving = resolver.resolve(&host);
            let mut addrs = match resolve_timeout {
                Some(timeout) => tokio::time::timeout(timeout, resolving)
                    .await
                    .map_err(|_| Error::ProxyTimeout(format!("DNS resolution for {host}")))??,
                None => resolving.await?,
            };
            addrs
                .next()
                .map(|addr| SocketAddr::new(addr.ip(), port))
                .ok_or_else(|| Error::Connect(format!("no addresses for relay {host}")))
        }
    }
}

async fn resolve_first(connector: &Connector, host: &str, port: u16) -> HttpResult<SocketAddr> {
    connector
        .resolve_addrs(host)
        .await?
        .into_iter()
        .next()
        .map(|addr| SocketAddr::new(addr.ip(), port))
        .ok_or_else(|| Error::Connect(format!("no addresses for {host}")))
}

pub(crate) enum MaybeTls {
    Plain(BoxedIo),
    Tls(Box<TlsStream<BoxedIo>>),
}

/// Type-erased AsyncRead+AsyncWrite stream so TLS sits on plain TCP or a SOCKS-tunneled TCP without enum gymnastics
pub struct BoxedIo {
    inner: Box<dyn AsyncReadWrite + Send + Unpin>,
}

trait AsyncReadWrite: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite> AsyncReadWrite for T {}

impl BoxedIo {
    pub fn new<T: AsyncRead + AsyncWrite + Send + Unpin + 'static>(t: T) -> Self {
        Self { inner: Box::new(t) }
    }
}

impl From<TcpStream> for BoxedIo {
    fn from(stream: TcpStream) -> Self {
        Self::new(stream)
    }
}

struct PrefixedIo<T> {
    prefix: io::Cursor<Vec<u8>>,
    inner: T,
}

impl<T> PrefixedIo<T> {
    fn new(prefix: Vec<u8>, inner: T) -> Self {
        Self {
            prefix: io::Cursor::new(prefix),
            inner,
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for PrefixedIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.prefix.position() < self.prefix.get_ref().len() as u64 {
            let position = self.prefix.position();
            let remaining = &self.prefix.get_ref()[position as usize..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            self.prefix.set_position(position + n as u64);
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for PrefixedIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl AsyncRead for BoxedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for BoxedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Hyper-compatible IO wrapper around `MaybeTls`
pub struct ConnIo {
    io: TokioIo<MaybeTls>,
    negotiated_h2: bool,
    proxied: bool,
}

impl Connection for ConnIo {
    fn connected(&self) -> Connected {
        let connected = Connected::new().proxy(self.proxied);
        if self.negotiated_h2 {
            connected.negotiated_h2()
        } else {
            connected
        }
    }
}

impl hyper::rt::Read for ConnIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for ConnIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

impl AsyncRead for MaybeTls {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_read(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTls {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_write(cx, buf),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_flush(cx),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(s) => Pin::new(s).poll_shutdown(cx),
            MaybeTls::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

impl Service<Uri> for Connector {
    type Response = ConnIo;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<ConnIo, Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let this = self.clone();
        Box::pin(async move { this.connect(dst).await })
    }
}

impl Connector {
    async fn connect(self, dst: Uri) -> Result<ConnIo, Error> {
        let scheme = dst.scheme_str().unwrap_or("http").to_string();
        let host = dst
            .host()
            .ok_or_else(|| Error::Url("missing host".into()))?
            .to_string();
        let port = dst.port_u16().unwrap_or(match scheme.as_str() {
            "https" => 443,
            _ => 80,
        });
        let is_https = scheme == "https";

        let bypass = self
            .no_proxy
            .as_deref()
            .is_some_and(|matcher| matcher.matches_host_port(&host, Some(port)));
        let proxied = !bypass
            && !is_https
            && self
                .proxy
                .as_deref()
                .is_some_and(|proxy| matches!(proxy.scheme(), ProxyScheme::Http));
        let stream = match (self.proxy.as_deref(), bypass) {
            (Some(p), false) => self.via_proxy(p, &host, port, is_https).await?,
            (None, _) | (Some(_), true) => BoxedIo::new(self.direct(&host, port).await?),
        };

        let (final_io, negotiated_h2) = if is_https {
            let server_name = ServerName::try_from(host.clone())
                .map_err(|e| Error::Tls(format!("invalid server name: {e}")))?;
            let handshake = self.tls_connector().connect(server_name, stream);
            let tls = match self.connect_timeout {
                Some(d) => tokio::time::timeout(d, handshake)
                    .await
                    .map_err(|_| Error::Tls("TLS handshake timeout".into()))?
                    .map_err(|e| Error::Tls(e.to_string()))?,
                None => handshake.await.map_err(|e| Error::Tls(e.to_string()))?,
            };
            let negotiated_h2 = tls.get_ref().1.alpn_protocol() == Some(b"h2".as_slice());
            (MaybeTls::Tls(Box::new(tls)), negotiated_h2)
        } else {
            (MaybeTls::Plain(stream), false)
        };

        Ok(ConnIo {
            io: TokioIo::new(final_io),
            negotiated_h2,
            proxied,
        })
    }

    async fn resolve_addrs(&self, host: &str) -> Result<Vec<SocketAddr>, Error> {
        let resolving = self.resolver.resolve(host);
        let addrs = match self.connect_timeout {
            Some(timeout) => tokio::time::timeout(timeout, resolving)
                .await
                .map_err(|_| Error::ProxyTimeout(format!("DNS resolution for {host}")))??,
            None => resolving.await?,
        };
        Ok(addrs.collect())
    }

    async fn direct(&self, host: &str, port: u16) -> Result<TcpStream, Error> {
        let addrs = self.resolve_addrs(host).await?;
        // RFC 8305 (Happy Eyeballs v2)
        let ordered =
            interleave_by_family(addrs.into_iter().map(|a| SocketAddr::new(a.ip(), port)));
        if ordered.is_empty() {
            return Err(Error::Connect(format!("no addresses for {host}")));
        }
        let local_addr = self.local_addr;
        let ordered: Vec<_> = ordered
            .into_iter()
            .filter(|target| local_addr.is_none_or(|source| source.is_ipv4() == target.is_ipv4()))
            .collect();
        if ordered.is_empty() {
            return Err(Error::Connect(format!(
                "no addresses for {host} match the configured local address"
            )));
        }
        let connecting = self.happy_eyeballs(host, ordered, local_addr);
        match self.connect_timeout {
            Some(timeout) => tokio::time::timeout(timeout, connecting)
                .await
                .map_err(|_| Error::ProxyTimeout(format!("TCP connection to {host}:{port}")))?,
            None => connecting.await,
        }
    }

    /// Staggered-parallel connect: each address starts `ATTEMPT_DELAY` after the previous, first to connect wins, rest dropped
    async fn happy_eyeballs(
        &self,
        host: &str,
        addrs: Vec<SocketAddr>,
        local_addr: Option<SocketAddr>,
    ) -> Result<TcpStream, Error> {
        /// Connection Attempt Delay
        const ATTEMPT_DELAY: Duration = Duration::from_millis(300);

        let timeout = self.connect_timeout;
        let mut in_flight = FuturesUnordered::new();
        let mut remaining = addrs.into_iter();
        let mut last: Option<io::Error> = None;

        // Prime the first attempt
        if let Some(addr) = remaining.next() {
            in_flight.push(connect_one(addr, timeout, local_addr));
        }

        let stagger = tokio::time::sleep(ATTEMPT_DELAY);
        tokio::pin!(stagger);

        loop {
            // Wait for whichever comes first: next in-flight attempt finishing or the stagger timer firing
            tokio::select! {
                biased;
                finished = in_flight.next(), if !in_flight.is_empty() => {
                    match finished {
                        Some(Ok((stream, addr))) => {
                            self.tune(&stream)?;
                            tracing::debug!(
                                "connected host={host} peer={addr} family={}",
                                if addr.is_ipv6() { "v6" } else { "v4" },
                            );
                            return Ok(stream);
                        }
                        Some(Err(e)) => {
                            last = Some(e);
                            // That attempt failed
                            if in_flight.is_empty() {
                                if let Some(addr) = remaining.next() {
                                    in_flight.push(connect_one(addr, timeout, local_addr));
                                } else {
                                    break;
                                }
                            }
                        }
                        None => {
                            // No attempts in flight and the stream drained
                            if let Some(addr) = remaining.next() {
                                in_flight.push(connect_one(addr, timeout, local_addr));
                            } else {
                                break;
                            }
                        }
                    }
                }
                _ = &mut stagger => {
                    // Stagger elapsed without a winner
                    if let Some(addr) = remaining.next() {
                        in_flight.push(connect_one(addr, timeout, local_addr));
                    } else if in_flight.is_empty() {
                        break;
                    }
                    stagger.as_mut().reset(tokio::time::Instant::now() + ATTEMPT_DELAY);
                }
            }
        }

        Err(Error::Connect(
            last.map(|e| e.to_string())
                .unwrap_or_else(|| format!("no addresses for {host}")),
        ))
    }

    fn tune(&self, s: &TcpStream) -> Result<(), Error> {
        if self.tcp_nodelay {
            let _ = s.set_nodelay(true);
        }
        if let Some(d) = self.tcp_keepalive {
            let sock = socket2::SockRef::from(s);
            let ka = socket2::TcpKeepalive::new().with_time(d);
            let _ = sock.set_tcp_keepalive(&ka);
        }
        Ok(())
    }

    async fn via_proxy(
        &self,
        proxy: &Proxy,
        host: &str,
        port: u16,
        is_https: bool,
    ) -> Result<BoxedIo, Error> {
        match proxy.scheme() {
            ProxyScheme::Http => {
                let phost = proxy
                    .url()
                    .host_str()
                    .ok_or_else(|| Error::Url("proxy missing host".into()))?
                    .to_string();
                let pport = proxy.url().port().unwrap_or(80);
                let stream = self.direct_proxy_connection(&phost, pport).await?;
                if is_https {
                    http_connect(stream, host, port, proxy, self.connect_timeout).await
                } else {
                    Ok(BoxedIo::new(stream))
                }
            }
            ProxyScheme::Socks5 { resolve_locally } => {
                let phost = proxy
                    .url()
                    .host_str()
                    .ok_or_else(|| Error::Url("proxy missing host".into()))?
                    .to_string();
                let pport = proxy.url().port().unwrap_or(1080);
                // Percent-decode URL creds (RFC 3986) before SOCKS5 user/pass auth or HTTP `Basic` so `@`, `:`, spaces authenticate correctly
                let user_raw = proxy.url().username();
                let pass_raw = proxy.url().password().unwrap_or("");
                let user = percent_decode_str(user_raw);
                let pass = percent_decode_str(pass_raw);
                let auth = (!user.is_empty()).then_some((user.as_str(), pass.as_str()));
                let proxy_stream = self.direct_proxy_connection(&phost, pport).await?;
                self.tune(&proxy_stream)?;
                let stream = match self.connect_timeout {
                    Some(d) => tokio::time::timeout(
                        d,
                        socks5_connect(
                            proxy_stream,
                            host,
                            port,
                            *resolve_locally,
                            auth,
                            &self.resolver,
                            self.connect_timeout,
                        ),
                    )
                    .await
                    .map_err(|_| Error::ProxyTimeout("SOCKS5 connect".into()))??,
                    None => {
                        socks5_connect(
                            proxy_stream,
                            host,
                            port,
                            *resolve_locally,
                            auth,
                            &self.resolver,
                            self.connect_timeout,
                        )
                        .await?
                    }
                };
                Ok(BoxedIo::new(stream))
            }
        }
    }

    /// Proxy control connections intentionally do not inherit a direct
    /// destination source address. A proxy chooses the tracker-facing source.
    async fn direct_proxy_connection(&self, host: &str, port: u16) -> Result<TcpStream, Error> {
        let mut connector = self.clone();
        connector.local_addr = None;
        connector.direct(host, port).await
    }
}

async fn http_connect(
    stream: TcpStream,
    host: &str,
    port: u16,
    proxy: &Proxy,
    timeout: Option<Duration>,
) -> Result<BoxedIo, Error> {
    let fut = http_connect_inner(stream, host, port, proxy);
    match timeout {
        Some(d) => tokio::time::timeout(d, fut)
            .await
            .map_err(|_| Error::ProxyTimeout("proxy CONNECT".into()))?,
        None => fut.await,
    }
}

async fn http_connect_inner(
    mut stream: TcpStream,
    host: &str,
    port: u16,
    proxy: &Proxy,
) -> Result<BoxedIo, Error> {
    let authority = format_socket_endpoint(host, port);
    let mut req = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some(auth) = proxy.http_basic_authorization() {
        req.push_str(&format!("Proxy-Authorization: {auth}\r\n"));
    }
    req.push_str("\r\n");
    stream
        .write_all(req.as_bytes())
        .await
        .map_err(|e| Error::ProxyProtocol(format!("CONNECT write: {e}")))?;

    let mut buf = Vec::with_capacity(1024);
    loop {
        let mut chunk = [0u8; 1024];
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|e| Error::ProxyProtocol(format!("CONNECT read: {e}")))?;
        if n == 0 {
            return Err(Error::ProxyProtocol("proxy closed during CONNECT".into()));
        }
        buf.extend_from_slice(&chunk[..n]);
        let header_end = find_header_end(&buf);
        if header_end.is_some() {
            break;
        }
        if buf.len() > 16 * 1024 {
            return Err(Error::ProxyProtocol("CONNECT response too large".into()));
        }
    }
    let header_end = find_header_end(&buf)
        .ok_or_else(|| Error::ProxyProtocol("CONNECT response missing header terminator".into()))?;
    let leftover = buf.split_off(header_end);
    let head = std::str::from_utf8(&buf)
        .map_err(|e| Error::ProxyProtocol(format!("invalid CONNECT response: {e}")))?;
    let status_line = head.lines().next().unwrap_or("");
    let mut parts = status_line.split_whitespace();
    let version = parts.next().unwrap_or("");
    let code = parts.next().unwrap_or("");
    let status = code.parse::<u16>().ok();
    if !version.starts_with("HTTP/") || status.is_none() {
        return Err(Error::ProxyProtocol(format!(
            "invalid CONNECT status line: {status_line}"
        )));
    }
    let status = status.expect("checked above");
    if !(200..300).contains(&status) {
        if status == 407 {
            return Err(Error::ProxyAuthentication(format!(
                "HTTP proxy rejected CONNECT credentials (status {status})"
            )));
        }
        return Err(Error::ProxyProtocol(format!(
            "CONNECT failed: {status_line}"
        )));
    }
    Ok(BoxedIo::new(PrefixedIo::new(leftover, stream)))
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|pos| pos + 4)
}

/// Connect to a single address with the optional per-attempt timeout
async fn connect_one(
    addr: SocketAddr,
    timeout: Option<Duration>,
    local_addr: Option<SocketAddr>,
) -> Result<(TcpStream, SocketAddr), io::Error> {
    let fut = async move {
        if let Some(local_addr) = local_addr {
            let socket = if addr.is_ipv4() {
                tokio::net::TcpSocket::new_v4()?
            } else {
                tokio::net::TcpSocket::new_v6()?
            };
            socket.bind(local_addr)?;
            socket.connect(addr).await
        } else {
            TcpStream::connect(addr).await
        }
    };
    let stream = match timeout {
        Some(d) => tokio::time::timeout(d, fut)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timeout"))??,
        None => fut.await?,
    };
    Ok((stream, addr))
}

/// Interleave addresses by family with IPv6 first
fn interleave_by_family(addrs: impl Iterator<Item = SocketAddr>) -> Vec<SocketAddr> {
    let mut v6: std::collections::VecDeque<SocketAddr> = std::collections::VecDeque::new();
    let mut v4: std::collections::VecDeque<SocketAddr> = std::collections::VecDeque::new();
    for a in addrs {
        if a.is_ipv6() {
            v6.push_back(a);
        } else {
            v4.push_back(a);
        }
    }
    let mut out = Vec::with_capacity(v6.len() + v4.len());
    // IPv6 first, then alternate families
    while !v6.is_empty() || !v4.is_empty() {
        if let Some(a) = v6.pop_front() {
            out.push(a);
        }
        if let Some(a) = v4.pop_front() {
            out.push(a);
        }
    }
    out
}

/// Percent-decode a URL component (lossy on invalid UTF-8): `Url::username()`/`password()` return raw encoded, proxy auth needs decoded bytes
fn percent_decode_str(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

fn format_socket_endpoint(host: &str, port: u16) -> String {
    let host = host.trim_matches(['[', ']']);
    if host.parse::<std::net::Ipv6Addr>().is_ok() {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// SOCKS5 connect over an already-established proxy stream
async fn socks5_connect(
    mut stream: TcpStream,
    host: &str,
    port: u16,
    resolve_locally: bool,
    auth: Option<(&str, &str)>,
    resolver: &SharedResolver,
    resolve_timeout: Option<Duration>,
) -> Result<TcpStream, Error> {
    socks5_negotiate_auth(&mut stream, auth).await?;

    let normalized_host = normalize_target_host(host);
    let target = if let Ok(ip) = normalized_host.parse::<IpAddr>() {
        ProxyTarget::Ip(SocketAddr::new(ip, port))
    } else if resolve_locally {
        let resolving = resolver.resolve(host);
        let mut addrs = match resolve_timeout {
            Some(timeout) => tokio::time::timeout(timeout, resolving)
                .await
                .map_err(|_| Error::ProxyTimeout(format!("DNS resolution for {host}")))??,
            None => resolving.await?,
        };
        let target_ip = addrs
            .next()
            .ok_or_else(|| Error::Connect(format!("no addrs for {host}")))?
            .ip();
        ProxyTarget::Ip(SocketAddr::new(target_ip, port))
    } else {
        if normalized_host.is_empty() {
            return Err(Error::ProxyProtocol(
                "empty SOCKS5 destination hostname".into(),
            ));
        }
        ProxyTarget::Host(normalized_host, port)
    };

    let mut request = Vec::with_capacity(7 + host.len());
    request.extend_from_slice(&[5, 1, 0]); // VER, CONNECT, RSV
    append_socks_address(&mut request, &target)?;
    stream
        .write_all(&request)
        .await
        .map_err(|error| Error::ProxyProtocol(format!("SOCKS5 CONNECT write: {error}")))?;

    let mut response = [0u8; 4];
    stream
        .read_exact(&mut response)
        .await
        .map_err(|error| Error::ProxyProtocol(format!("SOCKS5 CONNECT response: {error}")))?;
    if response[0] != 5 {
        return Err(Error::ProxyProtocol("invalid SOCKS5 version".into()));
    }
    if response[2] != 0 {
        return Err(Error::ProxyProtocol("invalid SOCKS5 reserved byte".into()));
    }
    if response[1] != 0 {
        return Err(socks5_reply_error(response[1]));
    }
    let _bound = read_socks_target(&mut stream, response[3]).await?;
    Ok(stream)
}

fn append_socks_address(frame: &mut Vec<u8>, target: &ProxyTarget) -> HttpResult<()> {
    match target {
        ProxyTarget::Ip(addr) => match addr.ip() {
            IpAddr::V4(ip) => {
                frame.push(1);
                frame.extend_from_slice(&ip.octets());
            }
            IpAddr::V6(ip) => {
                frame.push(4);
                frame.extend_from_slice(&ip.octets());
            }
        },
        ProxyTarget::Host(host, _) => {
            let bytes = host.as_bytes();
            let len = u8::try_from(bytes.len())
                .map_err(|_| Error::ProxyProtocol("SOCKS5 hostname exceeds 255 bytes".into()))?;
            if bytes.is_empty() {
                return Err(Error::ProxyProtocol(
                    "empty SOCKS5 destination hostname".into(),
                ));
            }
            frame.push(3);
            frame.push(len);
            frame.extend_from_slice(bytes);
        }
    }
    let port = match target {
        ProxyTarget::Ip(addr) => addr.port(),
        ProxyTarget::Host(_, port) => *port,
    };
    frame.extend_from_slice(&port.to_be_bytes());
    Ok(())
}

fn socks5_reply_error(reply: u8) -> Error {
    let description = match reply {
        1 => "general SOCKS server failure",
        2 => "connection not allowed by ruleset",
        3 => "network unreachable",
        4 => "host unreachable",
        5 => "connection refused",
        6 => "TTL expired",
        7 => "command not supported",
        8 => "address type not supported",
        _ => "unknown SOCKS5 reply",
    };
    Error::ProxyProtocol(format!("SOCKS5 CONNECT failed ({reply}): {description}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::sync::Arc;
    use tokio::net::TcpListener;

    fn v4(n: u8) -> SocketAddr {
        SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::new(10, 0, 0, n)), 443)
    }
    fn v6(n: u16) -> SocketAddr {
        SocketAddr::new(
            std::net::IpAddr::V6(Ipv6Addr::new(0x2606, 0, 0, 0, 0, 0, 0, n)),
            443,
        )
    }

    #[test]
    fn interleave_puts_ipv6_first() {
        let input = vec![v4(1), v4(2), v6(1), v6(2)];
        let out = interleave_by_family(input.into_iter());
        // v6, v4, v6, v4
        assert_eq!(out, vec![v6(1), v4(1), v6(2), v4(2)]);
    }

    #[test]
    fn interleave_v4_only_preserves_order() {
        let input = vec![v4(1), v4(2), v4(3)];
        let out = interleave_by_family(input.into_iter());
        assert_eq!(out, vec![v4(1), v4(2), v4(3)]);
    }

    #[test]
    fn interleave_v6_only_preserves_order() {
        let input = vec![v6(1), v6(2)];
        let out = interleave_by_family(input.into_iter());
        assert_eq!(out, vec![v6(1), v6(2)]);
    }

    #[test]
    fn interleave_uneven_drains_remainder() {
        // More v6 than v4: after the pair runs out, remaining v6 trail
        let input = vec![v6(1), v6(2), v6(3), v4(1)];
        let out = interleave_by_family(input.into_iter());
        assert_eq!(out, vec![v6(1), v4(1), v6(2), v6(3)]);
    }

    #[test]
    fn socks_proxy_endpoint_brackets_ipv6_hosts() {
        assert_eq!(format_socket_endpoint("::1", 1080), "[::1]:1080");
        assert_eq!(format_socket_endpoint("[::1]", 1080), "[::1]:1080");
        assert_eq!(
            format_socket_endpoint("proxy.example", 1080),
            "proxy.example:1080"
        );
    }

    fn test_connector() -> Connector {
        let roots = rustls::RootCertStore::empty();
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Connector {
            tls: Arc::new(tls),
            resolver: Arc::new(crate::resolver::GaiResolver),
            proxy: None,
            no_proxy: None,
            connect_timeout: Some(Duration::from_secs(2)),
            tcp_nodelay: true,
            tcp_keepalive: None,
            local_addr: None,
        }
    }

    #[test]
    fn conn_io_reports_negotiated_h2() {
        let (client, _server) = tokio::io::duplex(64);
        let conn = ConnIo {
            io: TokioIo::new(MaybeTls::Plain(BoxedIo::new(client))),
            negotiated_h2: true,
            proxied: false,
        };
        assert!(conn.connected().is_negotiated_h2());
    }

    #[test]
    fn conn_io_reports_plain_http_proxy_state() {
        let (client, _server) = tokio::io::duplex(64);
        let conn = ConnIo {
            io: TokioIo::new(MaybeTls::Plain(BoxedIo::new(client))),
            negotiated_h2: false,
            proxied: true,
        };
        assert!(conn.connected().is_proxied());
    }

    #[tokio::test]
    async fn happy_eyeballs_connects_to_listener() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let c = test_connector();
        let stream = c
            .happy_eyeballs("localhost", vec![addr], None)
            .await
            .unwrap();
        assert_eq!(stream.peer_addr().unwrap(), addr);
    }

    #[tokio::test]
    async fn happy_eyeballs_fails_over_to_second_address() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live = listener.local_addr().unwrap();
        let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let c = test_connector();
        let stream = c
            .happy_eyeballs("localhost", vec![dead, live], None)
            .await
            .unwrap();
        assert_eq!(stream.peer_addr().unwrap(), live);
    }

    #[tokio::test]
    async fn happy_eyeballs_all_fail_returns_error() {
        let dead1: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let dead2: SocketAddr = "127.0.0.1:2".parse().unwrap();
        let c = test_connector();
        let res = c
            .happy_eyeballs("localhost", vec![dead1, dead2], None)
            .await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn http_connect_preserves_bytes_read_after_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut req = [0u8; 256];
            let n = socket.read(&mut req).await.unwrap();
            assert!(std::str::from_utf8(&req[..n])
                .unwrap()
                .starts_with("CONNECT example.com:443 HTTP/1.1"));
            socket
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\nhello")
                .await
                .unwrap();
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let proxy = Proxy::all("http://127.0.0.1:8080").unwrap();
        let mut tunneled = http_connect(stream, "example.com", 443, &proxy, None)
            .await
            .unwrap();
        let mut out = [0u8; 5];
        tunneled.read_exact(&mut out).await.unwrap();

        assert_eq!(&out, b"hello");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn proxy_connector_http_uses_connect_for_plain_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 1024];
            loop {
                let n = socket.read(&mut chunk).await.unwrap();
                request.extend_from_slice(&chunk[..n]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let text = std::str::from_utf8(&request).unwrap();
            assert!(text.starts_with("CONNECT peer.example:6881 HTTP/1.1"));
            socket
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .unwrap();
            let mut payload = [0u8; 4];
            socket.read_exact(&mut payload).await.unwrap();
            assert_eq!(&payload, b"ping");
            socket.write_all(b"pong").await.unwrap();
        });

        let connector =
            ProxyConnector::from_proxy(Proxy::all(format!("http://{proxy_addr}")).unwrap())
                .connect_timeout(Duration::from_secs(2));
        let mut stream = connector.connect_tcp("peer.example", 6881).await.unwrap();
        stream.write_all(b"ping").await.unwrap();
        let mut response = [0u8; 4];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"pong");
        server.await.unwrap();
    }

    #[test]
    fn proxy_connector_can_split_tcp_and_udp_routes() {
        let tcp = ProxyConnector::from_proxy(
            Proxy::all("http://tcp-proxy.example:8080")
                .unwrap()
                .with_bypass("tcp.example"),
        );
        let udp = ProxyConnector::from_proxy(
            Proxy::all("socks5h://udp-proxy.example:1080")
                .unwrap()
                .with_bypass("udp.example"),
        );
        let connector = tcp.with_udp_proxy(Some(udp));

        assert_eq!(connector.proxy().unwrap().url().scheme(), "http");
        assert_eq!(connector.udp_proxy().unwrap().url().scheme(), "socks5h");
        assert!(connector
            .no_proxy()
            .unwrap()
            .matches_host_port("tcp.example", Some(80)));
        assert!(connector
            .udp_no_proxy()
            .unwrap()
            .matches_host_port("udp.example", Some(80)));
        assert!(connector.supports_udp());
    }

    #[test]
    fn proxy_connector_with_no_proxy_updates_both_routes() {
        let tcp = ProxyConnector::from_proxy(Proxy::all("http://tcp-proxy.example:8080").unwrap());
        let udp =
            ProxyConnector::from_proxy(Proxy::all("socks5://udp-proxy.example:1080").unwrap());
        let connector = tcp
            .with_udp_proxy(Some(udp))
            .with_no_proxy(NoProxy::parse("bypass.example"));

        assert!(connector
            .no_proxy()
            .unwrap()
            .matches_host_port("bypass.example", Some(80)));
        assert!(connector
            .udp_no_proxy()
            .unwrap()
            .matches_host_port("bypass.example", Some(80)));
    }

    #[tokio::test]
    async fn proxy_connector_socks5_tcp_supports_password_authentication() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut greeting = [0u8; 2];
            socket.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting[0], 5);
            let mut methods = vec![0u8; usize::from(greeting[1])];
            socket.read_exact(&mut methods).await.unwrap();
            assert!(methods.contains(&2));
            socket.write_all(&[5, 2]).await.unwrap();

            let mut auth_head = [0u8; 2];
            socket.read_exact(&mut auth_head).await.unwrap();
            assert_eq!(auth_head[0], 1);
            let mut user = vec![0u8; usize::from(auth_head[1])];
            socket.read_exact(&mut user).await.unwrap();
            let mut pass_len = [0u8; 1];
            socket.read_exact(&mut pass_len).await.unwrap();
            let mut pass = vec![0u8; usize::from(pass_len[0])];
            socket.read_exact(&mut pass).await.unwrap();
            assert_eq!(&user, b"user");
            assert_eq!(&pass, b"pass");
            socket.write_all(&[1, 0]).await.unwrap();

            let mut request = [0u8; 4];
            socket.read_exact(&mut request).await.unwrap();
            assert_eq!(&request[..4], &[5, 1, 0, 3]);
            let mut target_len = [0u8; 1];
            socket.read_exact(&mut target_len).await.unwrap();
            let mut target = vec![0u8; usize::from(target_len[0]) + 2];
            socket.read_exact(&mut target).await.unwrap();
            assert_eq!(&target[..usize::from(target_len[0])], b"peer.invalid");
            socket
                .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
                .await
                .unwrap();
            let mut payload = [0u8; 4];
            socket.read_exact(&mut payload).await.unwrap();
            socket.write_all(&payload).await.unwrap();
        });

        let connector = ProxyConnector::from_proxy(
            Proxy::all(format!("socks5h://user:pass@{proxy_addr}")).unwrap(),
        )
        .connect_timeout(Duration::from_secs(2));
        let mut stream = connector.connect_tcp("peer.invalid", 6881).await.unwrap();
        stream.write_all(b"data").await.unwrap();
        let mut response = [0u8; 4];
        stream.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"data");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn proxy_connector_socks5h_udp_associate_wraps_frames() {
        let relay = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let relay_addr = relay.local_addr().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut control, _) = listener.accept().await.unwrap();
            let mut greeting = [0u8; 2];
            control.read_exact(&mut greeting).await.unwrap();
            let mut methods = vec![0u8; usize::from(greeting[1])];
            control.read_exact(&mut methods).await.unwrap();
            control.write_all(&[5, 0]).await.unwrap();
            let mut associate = [0u8; 10];
            control.read_exact(&mut associate).await.unwrap();
            assert_eq!(&associate[..4], &[5, 3, 0, 1]);
            let relay_port = relay_addr.port().to_be_bytes();
            control
                .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, relay_port[0], relay_port[1]])
                .await
                .unwrap();

            let mut frame = [0u8; 512];
            let (n, source) = relay.recv_from(&mut frame).await.unwrap();
            assert_eq!(&frame[..3], &[0, 0, 0]);
            assert_eq!(frame[3], 3);
            let host_len = usize::from(frame[4]);
            assert_eq!(&frame[5..5 + host_len], b"peer.invalid");
            let payload_start = 7 + host_len;
            assert_eq!(&frame[payload_start..n], b"ping");
            let mut response = vec![0, 0, 0, 1, 203, 0, 113, 7, 0x1a, 0x39];
            response.extend_from_slice(b"pong");
            relay.send_to(&response, source).await.unwrap();

            // Keep the association control stream alive while the caller reads
            // the relayed response.
            let mut one = [0u8; 1];
            let _ = control.read(&mut one).await;
        });

        let connector =
            ProxyConnector::from_proxy(Proxy::all(format!("socks5h://{proxy_addr}")).unwrap())
                .connect_timeout(Duration::from_secs(2));
        let datagram = connector.bind_udp().await.unwrap();
        datagram
            .send_to_host(b"ping", "peer.invalid", 6969)
            .await
            .unwrap();
        let mut buffer = [0u8; 16];
        let (n, _) = tokio::time::timeout(Duration::from_secs(2), datagram.recv_from(&mut buffer))
            .await
            .expect("SOCKS5 UDP response timed out")
            .unwrap();
        assert_eq!(&buffer[..n], b"pong");
        drop(datagram);
        server.await.unwrap();
    }

    #[test]
    fn socks5_udp_frame_accepts_maximum_domain_length() {
        let host = "a".repeat(255);
        let payload = vec![0x5a; 32];
        let frame = socks5_udp_frame(&payload, &ProxyTarget::Host(host.clone(), 65535)).unwrap();
        assert_eq!(frame.len(), payload.len() + SOCKS5_UDP_MAX_ADDRESS_OVERHEAD);

        let (decoded, source) = parse_socks5_udp_packet(&frame).unwrap();
        assert_eq!(decoded, payload);
        assert_eq!(source, ProxyDatagramSource::Host(host, 65535));
    }

    #[tokio::test]
    async fn http_connect_timeout_is_typed_as_proxy_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let proxy = Proxy::all("http://proxy.example:8080").unwrap();
        let error = match http_connect(
            stream,
            "target.example",
            443,
            &proxy,
            Some(Duration::from_millis(20)),
        )
        .await
        {
            Ok(_) => panic!("CONNECT unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(matches!(error, Error::ProxyTimeout(message) if message.contains("CONNECT")));
        server.abort();
    }

    #[tokio::test]
    async fn http_proxy_udp_returns_socks5_required_without_direct_fallback() {
        let connector = ProxyConnector::from_proxy(Proxy::all("http://127.0.0.1:1").unwrap());
        let error = connector.bind_udp().await.unwrap_err();
        assert!(matches!(error, Error::Socks5Required));
        assert!(!connector.supports_udp());
    }

    #[tokio::test]
    async fn udp_bypass_survives_unreachable_socks5_proxy() {
        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = receiver.local_addr().unwrap();
        let connector = ProxyConnector::from_proxy(
            Proxy::all("socks5://127.0.0.1:1")
                .unwrap()
                .with_bypass("127.0.0.1"),
        );
        let datagram = connector.bind_udp_with_bypass().await.unwrap();
        datagram
            .send_to_host(b"direct", "127.0.0.1", target.port())
            .await
            .unwrap();
        let mut buffer = [0u8; 16];
        let (length, _) =
            tokio::time::timeout(Duration::from_secs(1), receiver.recv_from(&mut buffer))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&buffer[..length], b"direct");
        let error = datagram
            .send_to_host(b"proxied", "192.0.2.1", target.port())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Io(_) | Error::Connect(_)));
    }

    struct HangingResolver;

    impl Resolve for HangingResolver {
        fn resolve(&self, _host: &str) -> crate::resolver::Resolving {
            Box::pin(async {
                tokio::time::sleep(Duration::from_secs(60)).await;
                Ok(Box::new(std::iter::empty()) as crate::resolver::Addrs)
            })
        }
    }

    struct ProxyOnlyResolver(SocketAddr);

    impl Resolve for ProxyOnlyResolver {
        fn resolve(&self, host: &str) -> crate::resolver::Resolving {
            let result = if host == "proxy.invalid" {
                Ok(Box::new(std::iter::once(self.0)) as crate::resolver::Addrs)
            } else {
                Err(Error::Connect(format!("unexpected DNS lookup for {host}")))
            };
            Box::pin(async move { result })
        }
    }

    struct TargetResolver(SocketAddr);

    impl Resolve for TargetResolver {
        fn resolve(&self, host: &str) -> crate::resolver::Resolving {
            let result = if host == "tracker.example" {
                Ok(Box::new(std::iter::once(self.0)) as crate::resolver::Addrs)
            } else {
                Err(Error::Connect(format!("unexpected DNS lookup for {host}")))
            };
            Box::pin(async move { result })
        }
    }

    #[tokio::test]
    async fn socks5_connect_does_not_resolve_domain_form_bound_metadata() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut greeting = [0u8; 2];
            socket.read_exact(&mut greeting).await.unwrap();
            let mut methods = vec![0u8; usize::from(greeting[1])];
            socket.read_exact(&mut methods).await.unwrap();
            socket.write_all(&[5, 0]).await.unwrap();

            let mut request = [0u8; 4];
            socket.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, &[5, 1, 0, 3]);
            let mut host_len = [0u8; 1];
            socket.read_exact(&mut host_len).await.unwrap();
            let mut host_and_port = vec![0u8; usize::from(host_len[0]) + 2];
            socket.read_exact(&mut host_and_port).await.unwrap();
            assert_eq!(
                &host_and_port[..usize::from(host_len[0])],
                b"target.invalid"
            );

            let bound_host = b"bound.invalid";
            let mut response = vec![5, 0, 0, 3, bound_host.len() as u8];
            response.extend_from_slice(bound_host);
            response.extend_from_slice(&443u16.to_be_bytes());
            socket.write_all(&response).await.unwrap();
        });

        let connector = ProxyConnector::from_proxy(
            Proxy::all(format!("socks5h://proxy.invalid:{}", proxy_addr.port())).unwrap(),
        )
        .resolver(ProxyOnlyResolver(proxy_addr))
        .connect_timeout(Duration::from_secs(2));
        let _stream = connector.connect_tcp("target.invalid", 443).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn proxy_connector_timeout_covers_dns_resolution() {
        let connector = ProxyConnector::direct()
            .resolver_arc(Arc::new(HangingResolver))
            .connect_timeout(Duration::from_millis(20));
        let error = match connector.connect_tcp("dns.example", 443).await {
            Ok(_) => panic!("DNS resolution unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(
            matches!(error, Error::ProxyTimeout(message) if message.contains("DNS resolution"))
        );
    }

    #[tokio::test]
    async fn http_connect_authentication_failure_is_typed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0u8; 512];
            loop {
                let n = socket.read(&mut chunk).await.unwrap();
                request.extend_from_slice(&chunk[..n]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            socket
                .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
                .await
                .unwrap();
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let proxy = Proxy::all("http://user:pass@proxy.example").unwrap();
        let error = match http_connect(stream, "target.example", 443, &proxy, None).await {
            Ok(_) => panic!("CONNECT unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(matches!(error, Error::ProxyAuthentication(_)));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn socks5_authentication_failure_is_typed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut greeting = [0u8; 2];
            socket.read_exact(&mut greeting).await.unwrap();
            let mut methods = vec![0u8; usize::from(greeting[1])];
            socket.read_exact(&mut methods).await.unwrap();
            socket.write_all(&[5, 2]).await.unwrap();
            let mut auth_head = [0u8; 2];
            socket.read_exact(&mut auth_head).await.unwrap();
            let mut auth = vec![0u8; usize::from(auth_head[1]) + 1];
            socket.read_exact(&mut auth).await.unwrap();
            socket.write_all(&[1, 1]).await.unwrap();
        });

        let connector =
            ProxyConnector::from_proxy(Proxy::all(format!("socks5://user:pass@{addr}")).unwrap())
                .connect_timeout(Duration::from_secs(2));
        let error = match connector.connect_tcp("target.invalid", 443).await {
            Ok(_) => panic!("SOCKS5 authentication unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(matches!(error, Error::ProxyAuthentication(_)));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn socks5_reply_failure_is_typed() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut greeting = [0u8; 2];
            socket.read_exact(&mut greeting).await.unwrap();
            let mut methods = vec![0u8; usize::from(greeting[1])];
            socket.read_exact(&mut methods).await.unwrap();
            socket.write_all(&[5, 0]).await.unwrap();
            let mut request = [0u8; 4];
            socket.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, &[5, 1, 0, 1]);
            let mut target = [0u8; 6];
            socket.read_exact(&mut target).await.unwrap();
            assert_eq!(&target[..4], &[198, 51, 100, 7]);
            socket
                .write_all(&[5, 5, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .unwrap();
        });

        let connector = ProxyConnector::from_proxy(Proxy::all(format!("socks5://{addr}")).unwrap())
            .connect_timeout(Duration::from_secs(2));
        let error = match connector.connect_tcp("198.51.100.7", 443).await {
            Ok(_) => panic!("SOCKS5 connection unexpectedly succeeded"),
            Err(error) => error,
        };
        assert!(
            matches!(error, Error::ProxyProtocol(message) if message.contains("connection refused"))
        );
        server.await.unwrap();
    }

    #[test]
    fn socks5_udp_domain_source_is_not_locally_resolved() {
        let mut packet = vec![0, 0, 0, 3, 10];
        packet.extend_from_slice(b"relay.test");
        packet.extend_from_slice(&6881u16.to_be_bytes());
        packet.extend_from_slice(b"payload");

        let (payload, source) = parse_socks5_udp_packet(&packet).unwrap();
        assert_eq!(payload, b"payload");
        assert_eq!(
            source,
            ProxyDatagramSource::Host("relay.test".to_string(), 6881)
        );
    }

    #[test]
    fn domain_datagram_source_never_triggers_local_resolution() {
        let target: SocketAddr = "127.0.0.1:6881".parse().unwrap();
        assert!(!datagram_source_matches(
            &ProxyDatagramSource::Host("localhost".to_string(), 6881),
            target,
        ));
        assert!(datagram_source_matches(
            &ProxyDatagramSource::Host("127.0.0.1".to_string(), 6881),
            target,
        ));
    }

    #[test]
    fn proxied_udp_direct_sources_must_match_bypass() {
        let bypass = NoProxy::parse("198.51.100.7:6881");
        assert!(direct_datagram_source_allowed(
            &bypass,
            "198.51.100.7:6881".parse().unwrap()
        ));
        assert!(!direct_datagram_source_allowed(
            &bypass,
            "198.51.100.8:6881".parse().unwrap()
        ));
        assert!(!direct_datagram_source_allowed(
            &bypass,
            "127.0.0.1:6881".parse().unwrap()
        ));
        let explicit = NoProxy::parse("127.0.0.1");
        assert!(direct_datagram_source_allowed(
            &explicit,
            "127.0.0.1:6881".parse().unwrap()
        ));
    }

    #[tokio::test]
    async fn socks5h_ip_literals_use_native_socks_address_types() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut greeting = [0u8; 2];
            socket.read_exact(&mut greeting).await.unwrap();
            let mut methods = vec![0u8; usize::from(greeting[1])];
            socket.read_exact(&mut methods).await.unwrap();
            socket.write_all(&[5, 0]).await.unwrap();

            let mut request = [0u8; 4];
            socket.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, &[5, 1, 0, 4]);
            let mut target = [0u8; 18];
            socket.read_exact(&mut target).await.unwrap();
            assert_eq!(
                &target[..16],
                &"2001:db8::7"
                    .parse::<std::net::Ipv6Addr>()
                    .unwrap()
                    .octets()
            );
            assert_eq!(u16::from_be_bytes([target[16], target[17]]), 6881);
            socket
                .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0])
                .await
                .unwrap();
        });

        let connector =
            ProxyConnector::from_proxy(Proxy::all(format!("socks5h://{proxy_addr}")).unwrap())
                .connect_timeout(Duration::from_secs(2));
        let _stream = connector.connect_tcp("2001:db8::7", 6881).await.unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn http_proxy_udp_allows_only_explicit_bypass_destinations() {
        let destination = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let destination_addr = destination.local_addr().unwrap();
        let proxy = Proxy::all("http://127.0.0.1:1")
            .unwrap()
            .with_no_proxy(NoProxy::parse("127.0.0.1"));
        let connector = ProxyConnector::from_proxy(proxy);
        let datagram = connector.bind_udp_with_bypass().await.unwrap();

        datagram.send_to(b"ping", destination_addr).await.unwrap();
        let mut received = [0u8; 8];
        let (n, _) = destination.recv_from(&mut received).await.unwrap();
        assert_eq!(&received[..n], b"ping");

        let error = datagram
            .send_to(b"blocked", "198.51.100.7:6881".parse().unwrap())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Socks5Required));
    }

    #[tokio::test]
    async fn http_proxy_udp_ipv6_bypass_uses_opposite_family_socket() {
        let Ok(destination) = UdpSocket::bind("[::1]:0").await else {
            // Some CI hosts disable IPv6; the dual-family path is covered on
            // hosts where an IPv6 loopback socket is available.
            return;
        };
        let destination_addr = destination.local_addr().unwrap();
        let proxy = Proxy::all("http://127.0.0.1:1")
            .unwrap()
            .with_no_proxy(NoProxy::parse("::1"));
        let connector = ProxyConnector::from_proxy(proxy);
        let datagram = connector.bind_udp_with_bypass().await.unwrap();

        datagram.send_to(b"ping", destination_addr).await.unwrap();
        let mut received = [0u8; 8];
        let (n, _) = destination.recv_from(&mut received).await.unwrap();
        assert_eq!(&received[..n], b"ping");
    }

    #[tokio::test]
    async fn http_proxy_udp_hostname_bypass_accepts_resolved_replies() {
        let destination = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let destination_addr = destination.local_addr().unwrap();
        let proxy = Proxy::all("http://127.0.0.1:1")
            .unwrap()
            .with_no_proxy(NoProxy::parse("tracker.example"));
        let connector =
            ProxyConnector::from_proxy(proxy).resolver(TargetResolver(destination_addr));
        let datagram = connector.bind_udp_with_bypass().await.unwrap();

        datagram
            .send_to_host(b"ping", "tracker.example", destination_addr.port())
            .await
            .unwrap();
        let mut received = [0u8; 8];
        let (n, source) = destination.recv_from(&mut received).await.unwrap();
        assert_eq!(&received[..n], b"ping");
        destination.send_to(b"pong", source).await.unwrap();

        let mut response = [0u8; 8];
        let (n, _) = tokio::time::timeout(
            Duration::from_secs(2),
            datagram.recv_from_target(&mut response),
        )
        .await
        .expect("hostname bypass response timed out")
        .unwrap();
        assert_eq!(&response[..n], b"pong");
    }
}
