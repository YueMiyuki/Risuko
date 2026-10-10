use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::RngExt;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::core::Id20;

const LSD_PORT: u16 = 6771;
const LSD_V4: Ipv4Addr = Ipv4Addr::new(239, 192, 152, 143);
const LSD_V6: Ipv6Addr = Ipv6Addr::new(0xff15, 0, 0, 0, 0, 0, 0xefc0, 0x988f);
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(5 * 60);
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(1);
const MAX_DATAGRAM: usize = 1280;

pub struct LocalServiceDiscovery {
    inner: Arc<LsdInner>,
    join_handles: Mutex<Vec<JoinHandle<()>>>,
}

struct LsdInner {
    cookie: String,
    announce_port: u16,
    info_hashes: Mutex<HashSet<Id20>>,
    pending: Mutex<Vec<Id20>>,
    rate: Mutex<HashMap<(Id20, IpAddr), Instant>>,
    tx_out: mpsc::Sender<(Id20, SocketAddr)>,
    wake_tx: mpsc::Sender<()>,
}

impl LocalServiceDiscovery {
    #[allow(clippy::type_complexity)]
    pub fn spawn(
        announce_port: u16,
        info_hashes: Vec<Id20>,
    ) -> std::io::Result<(Arc<Self>, mpsc::Receiver<(Id20, SocketAddr)>)> {
        let (tx_out, rx_out) = mpsc::channel(256);
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let cookie = random_cookie();

        let inner = Arc::new(LsdInner {
            cookie,
            announce_port,
            info_hashes: Mutex::new(info_hashes.into_iter().collect()),
            pending: Mutex::new(Vec::new()),
            rate: Mutex::new(HashMap::new()),
            tx_out,
            wake_tx,
        });

        let mut handles = Vec::new();

        let v4 = match bind_v4() {
            Ok(s) => Some(Arc::new(s)),
            Err(e) => {
                tracing::warn!("lsd: failed to bind IPv4 socket: {e}");
                None
            }
        };
        let v6 = match bind_v6() {
            Ok(s) => Some(Arc::new(s)),
            Err(e) => {
                tracing::warn!("lsd: failed to bind IPv6 socket: {e}");
                None
            }
        };

        if v4.is_none() && v6.is_none() {
            return Err(std::io::Error::other(
                "lsd: no multicast sockets could be bound",
            ));
        }

        if let Some(sock) = v4.clone() {
            let inner2 = inner.clone();
            handles.push(tokio::spawn(recv_loop(sock, inner2)));
        }
        if let Some(sock) = v6.clone() {
            let inner2 = inner.clone();
            handles.push(tokio::spawn(recv_loop(sock, inner2)));
        }

        let inner3 = inner.clone();
        handles.push(tokio::spawn(announce_loop(v4, v6, inner3, wake_rx)));

        Ok((
            Arc::new(Self {
                inner,
                join_handles: Mutex::new(handles),
            }),
            rx_out,
        ))
    }

    pub fn add_infohash(&self, ih: Id20) {
        let added = self.inner.info_hashes.lock().insert(ih);
        if added {
            self.inner.pending.lock().push(ih);
            let _ = self.inner.wake_tx.try_send(());
        }
    }

    pub fn remove_infohash(&self, ih: Id20) {
        self.inner.info_hashes.lock().remove(&ih);
        self.inner.pending.lock().retain(|h| *h != ih);
    }
}

impl Drop for LocalServiceDiscovery {
    fn drop(&mut self) {
        for h in self.join_handles.lock().drain(..) {
            h.abort();
        }
    }
}

fn bind_v4() -> std::io::Result<UdpSocket> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    #[cfg(unix)]
    sock.set_reuse_port(true)?;
    sock.set_nonblocking(true)?;
    sock.bind(&SocketAddr::from(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, LSD_PORT)).into())?;
    sock.join_multicast_v4(&LSD_V4, &Ipv4Addr::UNSPECIFIED)?;
    sock.set_multicast_ttl_v4(1)?;
    sock.set_multicast_loop_v4(false)?;
    UdpSocket::from_std(sock.into())
}

fn bind_v6() -> std::io::Result<UdpSocket> {
    let sock = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_only_v6(true)?;
    sock.set_reuse_address(true)?;
    #[cfg(unix)]
    sock.set_reuse_port(true)?;
    sock.set_nonblocking(true)?;
    sock.bind(&SocketAddr::from(SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, LSD_PORT, 0, 0)).into())?;
    sock.join_multicast_v6(&LSD_V6, 0)?;
    sock.set_multicast_hops_v6(1)?;
    sock.set_multicast_loop_v6(false)?;
    UdpSocket::from_std(sock.into())
}

const WAKE_MIN_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Default)]
struct LanIfaces {
    v4: Vec<Ipv4Addr>,
    v6: Vec<u32>,
}

impl LanIfaces {
    fn refresh(&mut self, v4: Option<&UdpSocket>, v6: Option<&UdpSocket>) {
        use network_interface::{Addr, NetworkInterface, NetworkInterfaceConfig};
        let mut found4 = Vec::new();
        let mut found6 = Vec::new();
        for nic in NetworkInterface::show().unwrap_or_default() {
            for addr in &nic.addr {
                match addr {
                    Addr::V4(a) if !a.ip.is_loopback() && !found4.contains(&a.ip) => {
                        found4.push(a.ip)
                    }
                    Addr::V6(a)
                        if !a.ip.is_loopback()
                            && nic.index != 0
                            && !found6.contains(&nic.index) =>
                    {
                        found6.push(nic.index)
                    }
                    _ => {}
                }
            }
        }
        if let Some(sock) = v4 {
            for ip in &found4 {
                if !self.v4.contains(ip) {
                    let _ = socket2::SockRef::from(sock).join_multicast_v4(&LSD_V4, ip);
                }
            }
        }
        if let Some(sock) = v6 {
            for idx in &found6 {
                if !self.v6.contains(idx) {
                    let _ = socket2::SockRef::from(sock).join_multicast_v6(&LSD_V6, *idx);
                }
            }
        }
        self.v4 = found4;
        self.v6 = found6;
    }
}

async fn announce_loop(
    v4: Option<Arc<UdpSocket>>,
    v6: Option<Arc<UdpSocket>>,
    inner: Arc<LsdInner>,
    mut wake: mpsc::Receiver<()>,
) {
    let mut tick = tokio::time::interval(ANNOUNCE_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let _ = tick.tick().await;
    let mut ifaces = LanIfaces::default();
    let mut full = true;
    loop {
        ifaces.refresh(v4.as_deref(), v6.as_deref());
        let hashes: Vec<Id20> = if full {
            inner.pending.lock().clear();
            inner.info_hashes.lock().iter().copied().collect()
        } else {
            std::mem::take(&mut *inner.pending.lock())
        };
        do_announce(v4.as_deref(), v6.as_deref(), &inner, &ifaces, &hashes).await;
        let last = Instant::now();
        tokio::select! {
            _ = tick.tick() => full = true,
            _ = wake.recv() => {
                full = false;
                let due = last + WAKE_MIN_INTERVAL;
                tokio::select! {
                    _ = tokio::time::sleep_until(due.into()) => {}
                    _ = tick.tick() => full = true,
                }
                while wake.try_recv().is_ok() {}
            }
        }
    }
}

async fn do_announce(
    v4: Option<&UdpSocket>,
    v6: Option<&UdpSocket>,
    inner: &LsdInner,
    ifaces: &LanIfaces,
    hashes: &[Id20],
) {
    if hashes.is_empty() {
        return;
    }
    let chunk = 20;
    for slice in hashes.chunks(chunk) {
        if let Some(sock) = v4 {
            let msg = build_announce(
                SocketAddrV4::new(LSD_V4, LSD_PORT).into(),
                inner.announce_port,
                slice,
                &inner.cookie,
            );
            if msg.len() <= MAX_DATAGRAM {
                let target = SocketAddrV4::new(LSD_V4, LSD_PORT);
                let sock_ref = socket2::SockRef::from(sock);
                if ifaces.v4.is_empty() {
                    let _ = sock.send_to(msg.as_bytes(), target).await;
                }
                for ip in &ifaces.v4 {
                    if sock_ref.set_multicast_if_v4(ip).is_ok() {
                        let _ = sock.send_to(msg.as_bytes(), target).await;
                    }
                }
            }
        }
        if let Some(sock) = v6 {
            let target = SocketAddrV6::new(LSD_V6, LSD_PORT, 0, 0);
            let msg = build_announce(target.into(), inner.announce_port, slice, &inner.cookie);
            if msg.len() <= MAX_DATAGRAM {
                let sock_ref = socket2::SockRef::from(sock);
                if ifaces.v6.is_empty() {
                    let _ = sock.send_to(msg.as_bytes(), target).await;
                }
                for idx in &ifaces.v6 {
                    if sock_ref.set_multicast_if_v6(*idx).is_ok() {
                        let _ = sock.send_to(msg.as_bytes(), target).await;
                    }
                }
            }
        }
    }
}

fn build_announce(host: SocketAddr, port: u16, hashes: &[Id20], cookie: &str) -> String {
    let mut s = String::with_capacity(128 + hashes.len() * 50);
    s.push_str("BT-SEARCH * HTTP/1.1\r\n");
    s.push_str(&format!("Host: {host}\r\n"));
    s.push_str(&format!("Port: {port}\r\n"));
    for h in hashes {
        s.push_str(&format!(
            "Infohash: {}\r\n",
            h.to_hex().to_ascii_uppercase()
        ));
    }
    s.push_str(&format!("cookie: {cookie}\r\n"));
    s.push_str("\r\n\r\n");
    s
}

async fn recv_loop(sock: Arc<UdpSocket>, inner: Arc<LsdInner>) {
    let mut buf = vec![0u8; 2048];
    loop {
        let Ok((n, from)) = sock.recv_from(&mut buf).await else {
            return;
        };
        let Some(parsed) = parse_announce(&buf[..n]) else {
            continue;
        };
        if parsed.cookie.as_deref() == Some(&inner.cookie) {
            continue;
        }
        let port = parsed.port;
        if port == 0 {
            continue;
        }
        let peer_addr = SocketAddr::new(from.ip(), port);
        // Snapshot under the lock, send outside it
        let matched: Vec<Id20> = {
            let our = inner.info_hashes.lock();
            parsed
                .info_hashes
                .into_iter()
                .filter(|ih| our.contains(ih))
                .collect()
        };
        for ih in matched {
            let key = (ih, from.ip());
            let now = Instant::now();
            {
                let mut rate = inner.rate.lock();
                if rate.len() > 4096 {
                    rate.retain(|_, t| now.duration_since(*t) < RATE_LIMIT_WINDOW * 4);
                }
                if rate.len() > 8192 {
                    continue;
                }
                match rate.get(&key) {
                    Some(&t) if now.duration_since(t) < RATE_LIMIT_WINDOW => continue,
                    _ => {
                        rate.insert(key, now);
                    }
                }
            }
            let _ = inner.tx_out.try_send((ih, peer_addr));
        }
    }
}

#[derive(Default)]
struct AnnounceMsg {
    port: u16,
    info_hashes: Vec<Id20>,
    cookie: Option<String>,
}

fn parse_announce(buf: &[u8]) -> Option<AnnounceMsg> {
    let mut headers = [httparse::EMPTY_HEADER; 32];
    let mut req = httparse::Request::new(&mut headers);
    req.parse(buf).ok()?;
    if req.method? != "BT-SEARCH" {
        return None;
    }
    let mut out = AnnounceMsg::default();
    for h in req.headers.iter() {
        let v = std::str::from_utf8(h.value).unwrap_or("").trim();
        if h.name.eq_ignore_ascii_case("Port") {
            if let Ok(p) = v.parse::<u16>() {
                out.port = p;
            }
        } else if h.name.eq_ignore_ascii_case("Infohash") {
            if let Ok(bytes) = hex::decode(v) {
                if let Ok(id) = Id20::from_slice(&bytes) {
                    out.info_hashes.push(id);
                }
            }
        } else if h.name.eq_ignore_ascii_case("cookie") {
            out.cookie = Some(v.to_string());
        }
    }
    if out.info_hashes.is_empty() {
        return None;
    }
    Some(out)
}

fn random_cookie() -> String {
    let mut rng = rand::rng();
    let n: u64 = rng.random();
    format!("{n:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ok() {
        let ih = "0123456789ABCDEF0123456789ABCDEF01234567";
        let msg = format!(
            "BT-SEARCH * HTTP/1.1\r\n\
             Host: 239.192.152.143:6771\r\n\
             Port: 51413\r\n\
             Infohash: {ih}\r\n\
             cookie: abcd\r\n\
             \r\n\r\n"
        );
        let p = parse_announce(msg.as_bytes()).unwrap();
        assert_eq!(p.port, 51413);
        assert_eq!(p.info_hashes.len(), 1);
        assert_eq!(p.cookie.as_deref(), Some("abcd"));
    }

    #[test]
    fn build_contains_headers() {
        let h = Id20([0x11; 20]);
        let s = build_announce(
            SocketAddrV4::new(LSD_V4, LSD_PORT).into(),
            51413,
            &[h],
            "cafe",
        );
        assert!(s.starts_with("BT-SEARCH * HTTP/1.1\r\n"));
        assert!(s.contains("Port: 51413"));
        assert!(s.contains("Infohash: 1111111111111111111111111111111111111111"));
        assert!(s.contains("cookie: cafe"));
    }
}
