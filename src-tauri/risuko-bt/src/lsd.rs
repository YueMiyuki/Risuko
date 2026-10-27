use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rand::RngExt;
use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::core::Id20;

const LSD_PORT: u16 = 6771;
const LSD_V4: Ipv4Addr = Ipv4Addr::new(239, 192, 152, 143);
const LSD_V6: Ipv6Addr = Ipv6Addr::new(0xff15, 0, 0, 0, 0, 0, 0xefc0, 0x988f);
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(5 * 60);
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(1);
const MAX_DATAGRAM: usize = 1280;
const RECV_ERROR_BACKOFF: Duration = Duration::from_millis(200);
const MAX_RECV_ERROR_BACKOFF: Duration = Duration::from_secs(30);

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
    local_nets: Mutex<Vec<LocalNet>>,
    nets_scanned: Mutex<Option<Instant>>,
    tx_out: mpsc::Sender<(Id20, SocketAddr)>,
    wake_tx: mpsc::Sender<()>,
}

impl LsdInner {
    fn set_local_nets(&self, nets: Vec<LocalNet>) {
        *self.local_nets.lock() = nets;
        *self.nets_scanned.lock() = Some(Instant::now());
    }

    fn accepts_source(&self, ip: IpAddr) -> bool {
        is_local_source(ip, &self.local_nets.lock())
    }

    fn claim_rescan(&self, ip: IpAddr) -> bool {
        if self.accepts_source(ip) {
            return false;
        }
        let mut scanned = self.nets_scanned.lock();
        let now = Instant::now();
        if scanned.is_some_and(|at| now.duration_since(at) < NETS_RESCAN_INTERVAL) {
            return false;
        }
        *scanned = Some(now);
        true
    }
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
            local_nets: Mutex::new(Vec::new()),
            nets_scanned: Mutex::new(None),
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

const WAKE_MIN_INTERVAL: Duration = Duration::from_secs(60);
const NETS_RESCAN_INTERVAL: Duration = Duration::from_secs(10);
const CHUNK_PACING: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LocalNet {
    addr: IpAddr,
    prefix: u8,
}

#[derive(Default)]
struct LanIfaces {
    v4: Vec<Ipv4Addr>,
    v6: Vec<u32>,
    nets: Vec<LocalNet>,
}

impl LanIfaces {
    fn refresh(&mut self, v4: Option<&UdpSocket>, v6: Option<&UdpSocket>) {
        use network_interface::{Addr, NetworkInterface, NetworkInterfaceConfig};
        let mut found4 = Vec::new();
        let mut found6 = Vec::new();
        let mut nets = Vec::new();
        for nic in NetworkInterface::show().unwrap_or_default() {
            for addr in &nic.addr {
                match addr {
                    Addr::V4(a) if !a.ip.is_loopback() => {
                        if !found4.contains(&a.ip) {
                            found4.push(a.ip);
                        }
                        if let Some(mask) = a.netmask {
                            nets.extend(local_net(a.ip.into(), u32::from(mask).leading_ones()));
                        }
                    }
                    Addr::V6(a) if !a.ip.is_loopback() => {
                        if nic.index != 0 && !found6.contains(&nic.index) {
                            found6.push(nic.index);
                        }
                        if let Some(mask) = a.netmask {
                            nets.extend(local_net(a.ip.into(), u128::from(mask).leading_ones()));
                        }
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
        self.nets = nets;
    }
}

fn scan_local_nets() -> Vec<LocalNet> {
    let mut ifaces = LanIfaces::default();
    ifaces.refresh(None, None);
    ifaces.nets
}

fn local_net(addr: IpAddr, prefix: u32) -> Option<LocalNet> {
    (!addr.is_unspecified() && prefix > 0).then_some(LocalNet {
        addr,
        prefix: prefix as u8,
    })
}

fn in_subnet(ip: IpAddr, net: &LocalNet) -> bool {
    use crate::net::{ipv4_mask, ipv6_mask};
    match (ip, net.addr) {
        _ if net.prefix == 0 => false,
        (IpAddr::V4(a), IpAddr::V4(b)) => {
            (u32::from(a) ^ u32::from(b)) & ipv4_mask(net.prefix) == 0
        }
        (IpAddr::V6(a), IpAddr::V6(b)) => {
            (u128::from(a) ^ u128::from(b)) & ipv6_mask(net.prefix) == 0
        }
        _ => false,
    }
}

fn is_local_source(ip: IpAddr, nets: &[LocalNet]) -> bool {
    if ip.is_loopback() {
        return true;
    }
    let mut family_nets = nets
        .iter()
        .filter(|net| net.prefix > 0 && net.addr.is_ipv4() == ip.is_ipv4())
        .peekable();
    if family_nets.peek().is_some() {
        return family_nets.any(|net| in_subnet(ip, net));
    }
    crate::net::is_lan(ip)
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
    let mut last = Instant::now()
        .checked_sub(WAKE_MIN_INTERVAL)
        .unwrap_or_else(Instant::now);
    loop {
        ifaces.refresh(v4.as_deref(), v6.as_deref());
        inner.set_local_nets(ifaces.nets.clone());
        let hashes: Vec<Id20> = if full {
            inner.pending.lock().clear();
            inner.info_hashes.lock().iter().copied().collect()
        } else {
            std::mem::take(&mut *inner.pending.lock())
        };
        do_announce(v4.as_deref(), v6.as_deref(), &inner, &ifaces, &hashes).await;
        if !hashes.is_empty() {
            last = Instant::now();
        }
        full = next_round(&mut wake, &mut tick, last).await;
    }
}

async fn next_round(
    wake: &mut mpsc::Receiver<()>,
    tick: &mut tokio::time::Interval,
    last: Instant,
) -> bool {
    tokio::select! {
        _ = tick.tick() => true,
        _ = wake.recv() => {
            let mut full = false;
            tokio::select! {
                _ = tokio::time::sleep_until(last + WAKE_MIN_INTERVAL) => {}
                _ = tick.tick() => full = true,
            }
            while wake.try_recv().is_ok() {}
            full
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
    for (i, slice) in hashes.chunks(chunk).enumerate() {
        if i > 0 {
            tokio::time::sleep(CHUNK_PACING).await;
        }
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

trait DatagramSource: Send + Sync + 'static {
    fn recv(
        &self,
        buf: &mut [u8],
    ) -> impl std::future::Future<Output = std::io::Result<(usize, SocketAddr)>> + Send;
}

impl DatagramSource for UdpSocket {
    fn recv(
        &self,
        buf: &mut [u8],
    ) -> impl std::future::Future<Output = std::io::Result<(usize, SocketAddr)>> + Send {
        self.recv_from(buf)
    }
}

async fn recv_loop<S: DatagramSource>(sock: Arc<S>, inner: Arc<LsdInner>) {
    let mut buf = vec![0u8; 2048];
    let mut backoff = RECV_ERROR_BACKOFF;
    loop {
        match sock.recv(&mut buf).await {
            Ok((n, from)) => {
                backoff = RECV_ERROR_BACKOFF;
                if inner.claim_rescan(from.ip()) {
                    if let Ok(nets) = tokio::task::spawn_blocking(scan_local_nets).await {
                        *inner.local_nets.lock() = nets;
                    }
                }
                handle_datagram(&inner, &buf[..n], from)
            }
            Err(e) => {
                tracing::debug!("lsd: recv failed: {e}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_RECV_ERROR_BACKOFF);
            }
        }
    }
}

fn handle_datagram(inner: &LsdInner, data: &[u8], from: SocketAddr) {
    if !inner.accepts_source(from.ip()) {
        return;
    }
    let Some(parsed) = parse_announce(data) else {
        return;
    };
    if parsed.cookie.as_deref() == Some(&inner.cookie) {
        return;
    }
    let port = parsed.port;
    if port == 0 {
        return;
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

    type ScriptedRecv = std::io::Result<(Vec<u8>, SocketAddr)>;

    struct ScriptedSource {
        script: Mutex<std::collections::VecDeque<ScriptedRecv>>,
    }

    impl ScriptedSource {
        fn new(script: Vec<ScriptedRecv>) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(script.into()),
            })
        }
    }

    impl DatagramSource for ScriptedSource {
        fn recv(
            &self,
            buf: &mut [u8],
        ) -> impl std::future::Future<Output = std::io::Result<(usize, SocketAddr)>> + Send
        {
            let next = self.script.lock().pop_front();
            async move {
                match next {
                    Some(Ok((data, from))) => {
                        buf[..data.len()].copy_from_slice(&data);
                        Ok((data.len(), from))
                    }
                    Some(Err(e)) => Err(e),
                    None => std::future::pending().await,
                }
            }
        }
    }

    fn test_inner(ih: Id20) -> (Arc<LsdInner>, mpsc::Receiver<(Id20, SocketAddr)>) {
        let (tx_out, rx_out) = mpsc::channel(8);
        let (wake_tx, _wake_rx) = mpsc::channel(1);
        let inner = Arc::new(LsdInner {
            cookie: "own".into(),
            announce_port: 51413,
            info_hashes: Mutex::new(HashSet::from([ih])),
            pending: Mutex::new(Vec::new()),
            rate: Mutex::new(HashMap::new()),
            local_nets: Mutex::new(Vec::new()),
            nets_scanned: Mutex::new(None),
            tx_out,
            wake_tx,
        });
        (inner, rx_out)
    }

    fn announce_bytes(ih: Id20) -> Vec<u8> {
        build_announce(
            SocketAddrV4::new(LSD_V4, LSD_PORT).into(),
            51413,
            &[ih],
            "peer",
        )
        .into_bytes()
    }

    fn net(addr: &str, prefix: u8) -> LocalNet {
        LocalNet {
            addr: addr.parse().unwrap(),
            prefix,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn recv_loop_survives_a_receive_error() {
        let ih = Id20([0x22; 20]);
        let (inner, mut rx_out) = test_inner(ih);
        let from: SocketAddr = "192.168.1.20:6771".parse().unwrap();
        let source = ScriptedSource::new(vec![
            Err(std::io::ErrorKind::ConnectionReset.into()),
            Ok((announce_bytes(ih), from)),
        ]);
        let task = tokio::spawn(recv_loop(source, inner));
        let got = tokio::time::timeout(Duration::from_secs(5), rx_out.recv())
            .await
            .expect("announce after the error was never delivered");
        assert_eq!(got, Some((ih, "192.168.1.20:51413".parse().unwrap())));
        assert!(!task.is_finished());
        task.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_receive_errors_back_off_instead_of_spinning() {
        let (inner, _rx_out) = test_inner(Id20([0x27; 20]));
        let errors = 20;
        let source = ScriptedSource::new(
            (0..errors)
                .map(|_| Err(std::io::ErrorKind::ConnectionReset.into()))
                .collect(),
        );
        let task = tokio::spawn(recv_loop(Arc::clone(&source), inner));
        tokio::time::sleep(Duration::from_secs(5)).await;
        let attempts = errors - source.script.lock().len();
        assert!(attempts <= 6, "{attempts} receive attempts in 5 s");
        task.abort();
    }

    #[test]
    fn local_source_check_accepts_on_link_hosts_only() {
        let nets = [net("192.168.1.5", 24), net("2001:db8:1::5", 64)];
        for ip in ["192.168.1.20", "127.0.0.1", "2001:db8:1::99", "::1"] {
            assert!(is_local_source(ip.parse().unwrap(), &nets), "rejected {ip}");
        }
        for ip in [
            "169.254.9.9",
            "fe80::1",
            "10.1.2.3",
            "192.168.2.20",
            "fd00::1",
            "8.8.8.8",
            "203.0.113.7",
            "2001:db8:2::1",
            "2606:4700::1111",
        ] {
            assert!(
                !is_local_source(ip.parse().unwrap(), &nets),
                "accepted {ip}"
            );
        }
        assert!(is_local_source(
            "203.0.113.7".parse().unwrap(),
            &[net("203.0.113.9", 24)]
        ));
        assert!(!is_local_source(
            "203.0.114.7".parse().unwrap(),
            &[net("203.0.113.9", 24)]
        ));
        assert!(is_local_source(
            "203.0.113.7".parse().unwrap(),
            &[net("203.0.113.7", 32)]
        ));
        assert!(!is_local_source(
            "203.0.113.8".parse().unwrap(),
            &[net("203.0.113.7", 32)]
        ));
    }

    #[test]
    fn link_local_sources_must_share_a_known_interface_subnet() {
        let metadata = "169.254.169.254".parse().unwrap();
        assert!(!is_local_source(metadata, &[net("192.168.1.5", 24)]));
        assert!(!is_local_source(
            metadata,
            &[net("192.168.1.5", 24), net("fe80::5", 64)]
        ));
        assert!(is_local_source(metadata, &[net("169.254.7.7", 16)]));
        assert!(is_local_source(
            "169.254.1.1".parse().unwrap(),
            &[net("192.168.1.5", 24), net("169.254.7.7", 16)]
        ));
        let spoofed = "fe80::1".parse().unwrap();
        assert!(!is_local_source(spoofed, &[net("2001:db8:1::5", 64)]));
        assert!(!is_local_source(
            spoofed,
            &[net("2001:db8:1::5", 64), net("fe80:0:0:7::5", 64)]
        ));
        assert!(is_local_source(spoofed, &[net("fe80::5", 64)]));
        assert!(is_local_source(
            "::1".parse().unwrap(),
            &[net("fe80::5", 64)]
        ));
        assert!(is_local_source(
            "127.0.0.1".parse().unwrap(),
            &[net("192.168.1.5", 24)]
        ));
    }

    #[test]
    fn private_ranges_are_trusted_only_for_a_family_without_interface_data() {
        for ip in [
            "10.1.2.3",
            "172.16.0.9",
            "192.168.5.5",
            "fd00::1",
            "fe80::2",
            "169.254.9.9",
        ] {
            assert!(is_local_source(ip.parse().unwrap(), &[]), "rejected {ip}");
        }
        for ip in ["8.8.8.8", "2606:4700::1111"] {
            assert!(!is_local_source(ip.parse().unwrap(), &[]), "accepted {ip}");
        }
        let v6_only = [net("2001:db8:1::5", 64)];
        assert!(is_local_source("10.1.2.3".parse().unwrap(), &v6_only));
        assert!(!is_local_source("fd00::1".parse().unwrap(), &v6_only));
        let v4_only = [net("192.168.1.5", 24)];
        assert!(is_local_source("fd00::1".parse().unwrap(), &v4_only));
        assert!(!is_local_source("10.1.2.3".parse().unwrap(), &v4_only));
    }

    #[test]
    fn interfaces_without_a_usable_prefix_never_widen_the_source_filter() {
        let any = net("0.0.0.0", 0);
        assert!(!in_subnet("8.8.8.8".parse().unwrap(), &any));
        assert!(!is_local_source("8.8.8.8".parse().unwrap(), &[any]));
        assert_eq!(local_net("0.0.0.0".parse().unwrap(), 24), None);
        assert_eq!(local_net("::".parse().unwrap(), 64), None);
        assert_eq!(local_net("192.168.1.5".parse().unwrap(), 0), None);
        assert_eq!(
            local_net("192.168.1.5".parse().unwrap(), 24),
            Some(net("192.168.1.5", 24))
        );
    }

    #[tokio::test]
    async fn announces_from_off_link_sources_are_ignored() {
        let ih = Id20([0x23; 20]);
        let (inner, mut rx_out) = test_inner(ih);
        inner.set_local_nets(vec![net("192.168.1.5", 24)]);
        let data = announce_bytes(ih);
        handle_datagram(&inner, &data, "8.8.8.8:6771".parse().unwrap());
        assert!(rx_out.try_recv().is_err());
        handle_datagram(&inner, &data, "192.168.1.20:6771".parse().unwrap());
        assert_eq!(
            rx_out.try_recv().unwrap(),
            (ih, "192.168.1.20:51413".parse().unwrap())
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_off_link_source_triggers_a_rate_limited_rescan_of_the_local_networks() {
        let (inner, _rx_out) = test_inner(Id20([0x25; 20]));
        let new_lan = "10.0.0.7".parse().unwrap();
        inner.set_local_nets(vec![net("192.168.1.5", 24)]);
        assert!(!inner.claim_rescan(new_lan));
        tokio::time::sleep(NETS_RESCAN_INTERVAL).await;
        assert!(inner.claim_rescan(new_lan));
        *inner.local_nets.lock() = vec![net("10.0.0.2", 24)];
        assert!(inner.accepts_source(new_lan));
        assert!(!inner.claim_rescan(new_lan));
        let public = "8.8.8.8".parse().unwrap();
        assert!(!inner.claim_rescan(public));
        tokio::time::sleep(NETS_RESCAN_INTERVAL).await;
        assert!(inner.claim_rescan(public));
        assert!(!inner.claim_rescan(public));
        assert!(!inner.accepts_source(public));
    }

    #[tokio::test(start_paused = true)]
    async fn the_first_off_link_source_scans_without_waiting() {
        let (inner, _rx_out) = test_inner(Id20([0x26; 20]));
        *inner.local_nets.lock() = vec![net("192.168.1.5", 24)];
        assert!(inner.claim_rescan("10.0.0.7".parse().unwrap()));
    }

    #[tokio::test(start_paused = true)]
    async fn adds_within_a_minute_share_one_announce_round() {
        let (wake_tx, mut wake_rx) = mpsc::channel(1);
        let mut tick = tokio::time::interval(ANNOUNCE_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        let last = Instant::now();
        wake_tx.try_send(()).unwrap();
        let round = tokio::spawn(async move {
            let full = next_round(&mut wake_rx, &mut tick, last).await;
            (full, wake_rx, tick)
        });
        tokio::time::sleep(Duration::from_secs(10)).await;
        let _ = wake_tx.try_send(());
        tokio::time::sleep(Duration::from_secs(40)).await;
        assert!(!round.is_finished());
        tokio::time::sleep(Duration::from_secs(11)).await;
        assert!(round.is_finished());
        let (full, mut wake_rx, mut tick) = round.await.unwrap();
        assert!(!full);
        let next = tokio::spawn(async move { next_round(&mut wake_rx, &mut tick, last).await });
        tokio::time::sleep(Duration::from_secs(120)).await;
        assert!(!next.is_finished());
        next.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn torrents_added_after_an_empty_first_round_are_announced_promptly() {
        let ih = Id20([0x24; 20]);
        let (tx_out, _rx_out) = mpsc::channel(8);
        let (wake_tx, wake_rx) = mpsc::channel(1);
        let inner = Arc::new(LsdInner {
            cookie: "own".into(),
            announce_port: 51413,
            info_hashes: Mutex::new(HashSet::new()),
            pending: Mutex::new(Vec::new()),
            rate: Mutex::new(HashMap::new()),
            local_nets: Mutex::new(Vec::new()),
            nets_scanned: Mutex::new(None),
            tx_out,
            wake_tx: wake_tx.clone(),
        });
        let round = tokio::spawn(announce_loop(None, None, inner.clone(), wake_rx));
        tokio::time::sleep(Duration::from_secs(1)).await;
        inner.info_hashes.lock().insert(ih);
        inner.pending.lock().push(ih);
        wake_tx.try_send(()).unwrap();
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(
            inner.pending.lock().is_empty(),
            "the new torrent waited out the wake interval of an empty round"
        );
        round.abort();
    }
}
