use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rand::RngExt;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::external_ip::ExternalIp;
use super::upnp::{next_pass_delay, report_external_address, ForwarderHandle, MapProto};

const SERVER_PORT: u16 = 5351;
const PCP_VERSION: u8 = 2;
const NATPMP_VERSION: u8 = 0;
const PCP_OPCODE_MAP: u8 = 1;
const NATPMP_OPCODE_EXTERNAL: u8 = 0;
const RESPONSE_BIT: u8 = 0x80;
const PCP_HEADER_LEN: usize = 24;
const PCP_MAP_LEN: usize = PCP_HEADER_LEN + 36;
const NATPMP_MAP_RESPONSE_LEN: usize = 16;
const NATPMP_EXTERNAL_RESPONSE_LEN: usize = 12;
const FIRST_WAIT: Duration = Duration::from_millis(250);
const ATTEMPTS: u32 = 3;
const RELEASE_WAIT: Duration = Duration::from_millis(500);
const DEFAULT_LIFETIME: u32 = 7200;
const MIN_RENEWAL: Duration = Duration::from_secs(30);

type GatewayLookup = Arc<dyn Fn() -> Option<SocketAddr> + Send + Sync>;

pub(crate) type NatPmpHandle = ForwarderHandle<ActiveMapping>;

pub(crate) fn spawn(
    ports: Vec<(u16, MapProto)>,
    lease: Duration,
    after: Option<JoinHandle<()>>,
) -> NatPmpHandle {
    spawn_with(ports, lease, Arc::new(default_gateway), after)
}

fn spawn_with(
    ports: Vec<(u16, MapProto)>,
    lease: Duration,
    gateway: GatewayLookup,
    after: Option<JoinHandle<()>>,
) -> NatPmpHandle {
    let mapper = Mapper::new(ports, lease);
    let active = mapper.active.clone();
    ForwarderHandle::spawn(active, after, move |attempts, shutdown| {
        run_forever(mapper, gateway, attempts, shutdown)
    })
}

fn default_gateway() -> Option<SocketAddr> {
    let usable = |ip: &Ipv4Addr| !ip.is_unspecified() && !ip.is_loopback();
    let ip = netdev::get_default_gateway()
        .ok()
        .and_then(|device| device.ipv4.into_iter().find(usable))
        .or_else(|| {
            netdev::get_interfaces().into_iter().find_map(|iface| {
                let gateway = iface.gateway?;
                gateway
                    .ipv4
                    .into_iter()
                    .filter(usable)
                    .find(|ip| iface.ipv4.iter().any(|net| net.contains(ip)))
            })
        })?;
    Some(SocketAddr::from((ip, SERVER_PORT)))
}

async fn run_forever(
    mut mapper: Mapper,
    gateway: GatewayLookup,
    attempts: Arc<AtomicUsize>,
    mut shutdown: mpsc::Receiver<()>,
) {
    let mut failures = 0u32;
    let mut delay = Duration::ZERO;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(delay) => {
                let lookup = gateway.clone();
                let found = tokio::task::spawn_blocking(move || lookup()).await.ok().flatten();
                let renewal = match found {
                    Some(addr) => mapper.pass(addr).await.unwrap_or_else(|e| {
                        tracing::debug!("nat-pmp/pcp pass failed: {e}");
                        None
                    }),
                    None => {
                        tracing::debug!("nat-pmp/pcp: no IPv4 default gateway");
                        None
                    }
                };
                if renewal.is_none() {
                    mapper.active.lock().clear();
                }
                failures = if renewal.is_some() { 0 } else { failures.saturating_add(1) };
                attempts.fetch_add(1, Ordering::Relaxed);
                delay = next_pass_delay(failures, renewal.unwrap_or_default());
            }
            _ = shutdown.recv() => break,
        }
    }
    mapper.release().await;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dialect {
    Pcp,
    NatPmp,
}

#[derive(Debug, Clone)]
pub(crate) struct ActiveMapping {
    port: u16,
    proto: MapProto,
    nonce: [u8; 12],
    dialect: Dialect,
    lifetime: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Granted {
    external_port: u16,
    lifetime: u32,
    external_ip: Option<Ipv4Addr>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reply {
    Granted(Granted),
    Refused(u16),
    Downgrade,
}

struct Mapper {
    ports: Vec<(u16, MapProto, [u8; 12])>,
    lifetime: u32,
    gateway: Option<SocketAddr>,
    dialect: Option<Dialect>,
    active: Arc<Mutex<Vec<ActiveMapping>>>,
}

impl Mapper {
    fn new(ports: Vec<(u16, MapProto)>, lease: Duration) -> Self {
        let lifetime = match u32::try_from(lease.as_secs()).unwrap_or(u32::MAX) {
            0 => DEFAULT_LIFETIME,
            secs => secs,
        };
        Self {
            ports: ports
                .into_iter()
                .map(|(port, proto)| (port, proto, rand::rng().random()))
                .collect(),
            lifetime,
            gateway: None,
            dialect: None,
            active: Arc::new(Mutex::new(Vec::new())),
        }
    }

    async fn pass(&mut self, gateway: SocketAddr) -> std::io::Result<Option<Duration>> {
        if self.gateway != Some(gateway) {
            self.release().await;
            self.gateway = Some(gateway);
            self.dialect = None;
        }
        let (sock, client) = connect(gateway).await?;
        let mut mapped = Vec::new();
        let mut external_ip = None;
        for (port, proto, nonce) in self.ports.clone() {
            let reply = match self
                .request_map(&sock, client, port, proto, nonce, self.lifetime)
                .await
            {
                Ok(reply) => reply,
                Err(e) if mapped.is_empty() => return Err(e),
                Err(e) => {
                    tracing::debug!("nat-pmp/pcp map {} {port}: {e}", proto.as_str());
                    continue;
                }
            };
            match reply {
                Reply::Granted(granted) if granted.lifetime == 0 => {}
                Reply::Granted(granted) if granted.external_port != port => {
                    tracing::debug!(
                        "nat-pmp/pcp gave external {} port {} for {port}, releasing it",
                        proto.as_str(),
                        granted.external_port
                    );
                    let _ = self.request_map(&sock, client, port, proto, nonce, 0).await;
                }
                Reply::Granted(granted) => {
                    external_ip = external_ip.or(granted.external_ip);
                    let dialect = self.dialect.unwrap_or(Dialect::Pcp);
                    tracing::info!(
                        "{} mapped {} {port} via {gateway} for {}s",
                        dialect_name(dialect),
                        proto.as_str(),
                        granted.lifetime
                    );
                    mapped.push(ActiveMapping {
                        port,
                        proto,
                        nonce,
                        dialect,
                        lifetime: Duration::from_secs(granted.lifetime.into()),
                    });
                }
                Reply::Refused(code) => {
                    tracing::warn!(
                        "{} refused {} {port} via {gateway}: result {code}",
                        dialect_name(self.dialect.unwrap_or(Dialect::Pcp)),
                        proto.as_str()
                    );
                }
                Reply::Downgrade => {}
            }
        }
        if external_ip.is_none() && self.dialect == Some(Dialect::NatPmp) && !mapped.is_empty() {
            external_ip = natpmp_external_address(&sock).await;
        }
        if let (Some(ip), IpAddr::V4(gateway_ip)) = (external_ip, gateway.ip()) {
            report_external_address(ExternalIp::shared(), gateway_ip, ip);
        }
        let renewal = mapped
            .iter()
            .map(|m| m.lifetime / 2)
            .min()
            .map(|d| d.max(MIN_RENEWAL));
        *self.active.lock() = mapped;
        Ok(renewal)
    }

    async fn request_map(
        &mut self,
        sock: &UdpSocket,
        client: Ipv4Addr,
        port: u16,
        proto: MapProto,
        nonce: [u8; 12],
        lifetime: u32,
    ) -> std::io::Result<Reply> {
        if self.dialect != Some(Dialect::NatPmp) {
            let request = pcp_map_request(&nonce, proto, port, client, lifetime);
            let outcome = transact(sock, &request, FIRST_WAIT, ATTEMPTS, |buf| {
                parse_pcp_map_response(buf, &nonce, proto, port)
            })
            .await;
            match outcome {
                Ok(Reply::Downgrade) => self.dialect = Some(Dialect::NatPmp),
                Ok(reply) => {
                    self.dialect = Some(Dialect::Pcp);
                    return Ok(reply);
                }
                Err(e) if self.dialect.is_some() => return Err(e),
                Err(e) if e.kind() != std::io::ErrorKind::TimedOut => return Err(e),
                Err(_) => {}
            }
        }
        let external_port = if lifetime == 0 { 0 } else { port };
        let request = natpmp_map_request(proto, port, external_port, lifetime);
        let reply = transact(sock, &request, FIRST_WAIT, ATTEMPTS, |buf| {
            parse_natpmp_map_response(buf, proto, port)
        })
        .await?;
        self.dialect = Some(Dialect::NatPmp);
        Ok(reply)
    }

    async fn release(&self) {
        let mappings: Vec<ActiveMapping> = self.active.lock().drain(..).collect();
        let Some(gateway) = self.gateway else {
            return;
        };
        if mappings.is_empty() {
            return;
        }
        let Ok((sock, client)) = connect(gateway).await else {
            return;
        };
        for m in mappings {
            let request = match m.dialect {
                Dialect::Pcp => pcp_map_request(&m.nonce, m.proto, m.port, client, 0).to_vec(),
                Dialect::NatPmp => natpmp_map_request(m.proto, m.port, 0, 0).to_vec(),
            };
            let _ = transact(&sock, &request, RELEASE_WAIT, 1, |_| Some(())).await;
        }
    }
}

async fn connect(gateway: SocketAddr) -> std::io::Result<(UdpSocket, Ipv4Addr)> {
    let sock = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;
    sock.connect(gateway).await?;
    match sock.local_addr()?.ip() {
        IpAddr::V4(ip) if !ip.is_unspecified() => Ok((sock, ip)),
        other => Err(std::io::Error::other(format!(
            "no usable IPv4 source address towards {gateway} ({other})"
        ))),
    }
}

async fn transact<T>(
    sock: &UdpSocket,
    request: &[u8],
    first_wait: Duration,
    attempts: u32,
    accept: impl Fn(&[u8]) -> Option<T>,
) -> std::io::Result<T> {
    let mut wait = first_wait;
    let mut buf = [0u8; 1100];
    for _ in 0..attempts {
        sock.send(request).await?;
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            match tokio::time::timeout_at(deadline, sock.recv(&mut buf)).await {
                Ok(Ok(n)) => {
                    if let Some(reply) = accept(&buf[..n]) {
                        return Ok(reply);
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => break,
            }
        }
        wait = wait.saturating_mul(2);
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::TimedOut,
        "no NAT-PMP/PCP response",
    ))
}

async fn natpmp_external_address(sock: &UdpSocket) -> Option<Ipv4Addr> {
    transact(
        sock,
        &[NATPMP_VERSION, NATPMP_OPCODE_EXTERNAL],
        FIRST_WAIT,
        ATTEMPTS,
        parse_natpmp_external_response,
    )
    .await
    .ok()
    .flatten()
}

fn proto_number(proto: MapProto) -> u8 {
    match proto {
        MapProto::Tcp => 6,
        MapProto::Udp => 17,
    }
}

fn natpmp_opcode(proto: MapProto) -> u8 {
    match proto {
        MapProto::Udp => 1,
        MapProto::Tcp => 2,
    }
}

fn dialect_name(dialect: Dialect) -> &'static str {
    match dialect {
        Dialect::Pcp => "pcp",
        Dialect::NatPmp => "nat-pmp",
    }
}

fn pcp_map_request(
    nonce: &[u8; 12],
    proto: MapProto,
    port: u16,
    client: Ipv4Addr,
    lifetime: u32,
) -> [u8; PCP_MAP_LEN] {
    let mut buf = [0u8; PCP_MAP_LEN];
    buf[0] = PCP_VERSION;
    buf[1] = PCP_OPCODE_MAP;
    buf[4..8].copy_from_slice(&lifetime.to_be_bytes());
    buf[8..24].copy_from_slice(&client.to_ipv6_mapped().octets());
    buf[24..36].copy_from_slice(nonce);
    buf[36] = proto_number(proto);
    buf[40..42].copy_from_slice(&port.to_be_bytes());
    let suggested = if lifetime == 0 { 0 } else { port };
    buf[42..44].copy_from_slice(&suggested.to_be_bytes());
    buf[44..60].copy_from_slice(&Ipv4Addr::UNSPECIFIED.to_ipv6_mapped().octets());
    buf
}

fn parse_pcp_map_response(
    buf: &[u8],
    nonce: &[u8; 12],
    proto: MapProto,
    port: u16,
) -> Option<Reply> {
    if buf.first() == Some(&NATPMP_VERSION) && buf.len() >= 4 {
        return Some(Reply::Downgrade);
    }
    if buf.len() < PCP_MAP_LEN
        || buf[0] != PCP_VERSION
        || buf[1] != (RESPONSE_BIT | PCP_OPCODE_MAP)
        || &buf[24..36] != nonce
    {
        return None;
    }
    let result = buf[3];
    if result != 0 {
        return Some(Reply::Refused(result.into()));
    }
    if buf[36] != proto_number(proto) || buf[40..42] != port.to_be_bytes() {
        return None;
    }
    let lifetime = u32::from_be_bytes(buf[4..8].try_into().ok()?);
    let external_port = u16::from_be_bytes([buf[42], buf[43]]);
    let external_ip = Ipv6Addr::from(<[u8; 16]>::try_from(&buf[44..60]).ok()?).to_ipv4_mapped();
    Some(Reply::Granted(Granted {
        external_port,
        lifetime,
        external_ip,
    }))
}

fn natpmp_map_request(proto: MapProto, port: u16, external_port: u16, lifetime: u32) -> [u8; 12] {
    let mut buf = [0u8; 12];
    buf[0] = NATPMP_VERSION;
    buf[1] = natpmp_opcode(proto);
    buf[4..6].copy_from_slice(&port.to_be_bytes());
    buf[6..8].copy_from_slice(&external_port.to_be_bytes());
    buf[8..12].copy_from_slice(&lifetime.to_be_bytes());
    buf
}

fn parse_natpmp_map_response(buf: &[u8], proto: MapProto, port: u16) -> Option<Reply> {
    if buf.len() < 4 || buf[0] != NATPMP_VERSION || buf[1] != RESPONSE_BIT | natpmp_opcode(proto) {
        return None;
    }
    let result = u16::from_be_bytes([buf[2], buf[3]]);
    if result != 0 {
        return Some(Reply::Refused(result));
    }
    if buf.len() < NATPMP_MAP_RESPONSE_LEN || buf[8..10] != port.to_be_bytes() {
        return None;
    }
    Some(Reply::Granted(Granted {
        external_port: u16::from_be_bytes([buf[10], buf[11]]),
        lifetime: u32::from_be_bytes(buf[12..16].try_into().ok()?),
        external_ip: None,
    }))
}

fn parse_natpmp_external_response(buf: &[u8]) -> Option<Option<Ipv4Addr>> {
    if buf.len() < 4 || buf[0] != NATPMP_VERSION || buf[1] != RESPONSE_BIT | NATPMP_OPCODE_EXTERNAL
    {
        return None;
    }
    if u16::from_be_bytes([buf[2], buf[3]]) != 0 {
        return Some(None);
    }
    if buf.len() < NATPMP_EXTERNAL_RESPONSE_LEN {
        return None;
    }
    Some(Some(Ipv4Addr::new(buf[8], buf[9], buf[10], buf[11])))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    enum Mode {
        Pcp { port_shift: u16 },
        NatPmpOnly,
        Silent,
    }

    struct FakeGateway {
        addr: SocketAddr,
        requests: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    async fn fake_gateway(mode: Mode) -> FakeGateway {
        let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = sock.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1100];
            loop {
                let Ok((n, from)) = sock.recv_from(&mut buf).await else {
                    return;
                };
                let req = buf[..n].to_vec();
                seen.lock().push(req.clone());
                if let Some(resp) = respond(mode, &req) {
                    let _ = sock.send_to(&resp, from).await;
                }
            }
        });
        FakeGateway { addr, requests }
    }

    fn respond(mode: Mode, req: &[u8]) -> Option<Vec<u8>> {
        match mode {
            Mode::Silent => None,
            Mode::Pcp { port_shift } => {
                let mut resp = vec![0u8; PCP_MAP_LEN];
                resp[0] = PCP_VERSION;
                resp[1] = RESPONSE_BIT | req[1];
                let requested = u32::from_be_bytes(req[4..8].try_into().unwrap());
                resp[4..8].copy_from_slice(&requested.min(120).to_be_bytes());
                resp[24..42].copy_from_slice(&req[24..42]);
                let internal = u16::from_be_bytes([req[40], req[41]]);
                resp[42..44].copy_from_slice(&(internal + port_shift).to_be_bytes());
                resp[44..60].copy_from_slice(&Ipv4Addr::new(10, 0, 0, 1).to_ipv6_mapped().octets());
                Some(resp)
            }
            Mode::NatPmpOnly => {
                if req[0] != NATPMP_VERSION {
                    return Some(vec![0, RESPONSE_BIT | req[1], 0, 1, 0, 0, 0, 9]);
                }
                if req[1] == NATPMP_OPCODE_EXTERNAL {
                    return Some(vec![0, RESPONSE_BIT, 0, 0, 0, 0, 0, 9, 10, 0, 0, 1]);
                }
                let mut resp = vec![0, RESPONSE_BIT | req[1], 0, 0, 0, 0, 0, 9];
                resp.extend_from_slice(&req[4..6]);
                resp.extend_from_slice(&req[4..6]);
                let requested = u32::from_be_bytes(req[8..12].try_into().unwrap());
                resp.extend_from_slice(&requested.min(120).to_be_bytes());
                Some(resp)
            }
        }
    }

    fn ports() -> Vec<(u16, MapProto)> {
        vec![(41_000, MapProto::Tcp), (41_000, MapProto::Udp)]
    }

    #[test]
    fn pcp_map_request_layout() {
        let nonce = [7u8; 12];
        let req = pcp_map_request(
            &nonce,
            MapProto::Udp,
            6881,
            Ipv4Addr::new(192, 168, 1, 9),
            300,
        );
        assert_eq!(req.len(), 60);
        assert_eq!(&req[..4], &[2, 1, 0, 0]);
        assert_eq!(&req[4..8], &300u32.to_be_bytes());
        assert_eq!(
            &req[8..24],
            &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 192, 168, 1, 9]
        );
        assert_eq!(&req[24..36], &nonce);
        assert_eq!(&req[36..40], &[17, 0, 0, 0]);
        assert_eq!(&req[40..44], &[0x1a, 0xe1, 0x1a, 0xe1]);
        assert_eq!(
            &req[44..60],
            &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, 0, 0, 0, 0]
        );
        let delete = pcp_map_request(&nonce, MapProto::Tcp, 6881, Ipv4Addr::LOCALHOST, 0);
        assert_eq!(&delete[4..8], &[0, 0, 0, 0]);
        assert_eq!(delete[36], 6);
        assert_eq!(&delete[42..44], &[0, 0]);
    }

    #[test]
    fn natpmp_map_request_layout() {
        assert_eq!(
            natpmp_map_request(MapProto::Tcp, 6881, 6881, 7200),
            [0, 2, 0, 0, 0x1a, 0xe1, 0x1a, 0xe1, 0, 0, 0x1c, 0x20]
        );
        assert_eq!(
            natpmp_map_request(MapProto::Udp, 6881, 0, 0),
            [0, 1, 0, 0, 0x1a, 0xe1, 0, 0, 0, 0, 0, 0]
        );
    }

    #[test]
    fn pcp_responses_must_echo_the_request() {
        let nonce = [3u8; 12];
        let req = pcp_map_request(&nonce, MapProto::Tcp, 6881, Ipv4Addr::LOCALHOST, 300);
        let ok = respond(Mode::Pcp { port_shift: 0 }, &req).unwrap();
        assert_eq!(
            parse_pcp_map_response(&ok, &nonce, MapProto::Tcp, 6881),
            Some(Reply::Granted(Granted {
                external_port: 6881,
                lifetime: 120,
                external_ip: Some(Ipv4Addr::new(10, 0, 0, 1)),
            }))
        );
        assert_eq!(
            parse_pcp_map_response(&ok, &[4u8; 12], MapProto::Tcp, 6881),
            None
        );
        assert_eq!(
            parse_pcp_map_response(&ok, &nonce, MapProto::Udp, 6881),
            None
        );
        assert_eq!(
            parse_pcp_map_response(&ok, &nonce, MapProto::Tcp, 6882),
            None
        );
        assert_eq!(
            parse_pcp_map_response(&ok[..40], &nonce, MapProto::Tcp, 6881),
            None
        );
        let mut refused = ok.clone();
        refused[3] = 2;
        assert_eq!(
            parse_pcp_map_response(&refused, &nonce, MapProto::Tcp, 6881),
            Some(Reply::Refused(2))
        );
        assert_eq!(
            parse_pcp_map_response(&[0, 0x81, 0, 1, 0, 0, 0, 0], &nonce, MapProto::Tcp, 6881),
            Some(Reply::Downgrade)
        );
    }

    #[test]
    fn natpmp_responses_must_match_the_opcode_and_port() {
        let req = natpmp_map_request(MapProto::Udp, 6881, 6881, 300);
        let ok = respond(Mode::NatPmpOnly, &req).unwrap();
        assert_eq!(
            parse_natpmp_map_response(&ok, MapProto::Udp, 6881),
            Some(Reply::Granted(Granted {
                external_port: 6881,
                lifetime: 120,
                external_ip: None,
            }))
        );
        assert_eq!(parse_natpmp_map_response(&ok, MapProto::Tcp, 6881), None);
        assert_eq!(parse_natpmp_map_response(&ok, MapProto::Udp, 6882), None);
        assert_eq!(
            parse_natpmp_map_response(&[0, 0x81, 0, 2, 0, 0, 0, 0], MapProto::Udp, 6881),
            Some(Reply::Refused(2))
        );
        assert_eq!(
            parse_natpmp_external_response(&[0, 0x80, 0, 0, 0, 0, 0, 1, 203, 0, 113, 7]),
            Some(Some(Ipv4Addr::new(203, 0, 113, 7)))
        );
        assert_eq!(
            parse_natpmp_external_response(&[0, 0x80, 0, 3, 0, 0, 0, 1]),
            Some(None)
        );
    }

    #[tokio::test]
    async fn pcp_gateway_maps_and_renews_at_half_the_granted_lifetime() {
        let gw = fake_gateway(Mode::Pcp { port_shift: 0 }).await;
        let mut mapper = Mapper::new(ports(), Duration::from_secs(300));
        let renewal = mapper.pass(gw.addr).await.unwrap();
        assert_eq!(renewal, Some(Duration::from_secs(60)));
        assert_eq!(mapper.dialect, Some(Dialect::Pcp));
        assert_eq!(mapper.active.lock().len(), 2);
        let first: Vec<Vec<u8>> = gw.requests.lock().clone();
        mapper.pass(gw.addr).await.unwrap();
        let second: Vec<Vec<u8>> = gw.requests.lock()[2..].to_vec();
        assert_eq!(first.len(), 2);
        assert_eq!(first[0][24..36], second[0][24..36]);
        assert_eq!(first[1][24..36], second[1][24..36]);
        assert_ne!(first[0][24..36], first[1][24..36]);
    }

    #[tokio::test]
    async fn natpmp_only_gateway_is_used_after_a_version_downgrade() {
        let gw = fake_gateway(Mode::NatPmpOnly).await;
        let mut mapper = Mapper::new(ports(), Duration::from_secs(300));
        let renewal = mapper.pass(gw.addr).await.unwrap();
        assert_eq!(renewal, Some(Duration::from_secs(60)));
        assert_eq!(mapper.dialect, Some(Dialect::NatPmp));
        assert_eq!(mapper.active.lock().len(), 2);
        let requests = gw.requests.lock().clone();
        assert_eq!(requests[0][0], PCP_VERSION);
        assert!(requests[1..].iter().all(|r| r[0] == NATPMP_VERSION));
        assert_eq!(requests.last().unwrap(), &vec![0, 0]);
    }

    #[tokio::test]
    async fn a_different_external_port_is_released() {
        let gw = fake_gateway(Mode::Pcp { port_shift: 1 }).await;
        let mut mapper = Mapper::new(vec![(41_000, MapProto::Tcp)], Duration::from_secs(300));
        assert_eq!(mapper.pass(gw.addr).await.unwrap(), None);
        assert!(mapper.active.lock().is_empty());
        let requests = gw.requests.lock().clone();
        assert_eq!(requests.len(), 2);
        assert_eq!(&requests[1][4..8], &[0, 0, 0, 0]);
        assert_eq!(requests[0][24..36], requests[1][24..36]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_gateway_fails_the_pass() {
        let gw = fake_gateway(Mode::Silent).await;
        let mut mapper = Mapper::new(ports(), Duration::from_secs(300));
        let err = mapper.pass(gw.addr).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(mapper.dialect, None);
        for _ in 0..100 {
            if gw.requests.lock().len() == 2 * ATTEMPTS as usize {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
            tokio::task::yield_now().await;
        }
        assert_eq!(gw.requests.lock().len(), 2 * ATTEMPTS as usize);
    }

    #[tokio::test]
    async fn dropping_the_handle_deletes_the_mappings() {
        let gw = fake_gateway(Mode::Pcp { port_shift: 0 }).await;
        let addr = gw.addr;
        let handle = spawn_with(
            ports(),
            Duration::from_secs(300),
            Arc::new(move || Some(addr)),
            None,
        );
        for _ in 0..100 {
            if handle.mapping_count() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(handle.mapping_count(), 2);
        assert_eq!(handle.attempts(), 1);
        drop(handle);
        for _ in 0..100 {
            if gw.requests.lock().len() == 4 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let requests = gw.requests.lock().clone();
        assert_eq!(requests.len(), 4);
        assert!(requests[2..].iter().all(|r| r[4..8] == [0, 0, 0, 0]));
        assert_eq!(requests[0][24..36], requests[2][24..36]);
        assert_eq!(requests[1][24..36], requests[3][24..36]);
    }

    async fn wait_for(mut done: impl FnMut() -> bool) {
        for _ in 0..250 {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn a_gateway_that_stops_answering_leaves_nothing_counted() {
        let addr = UdpSocket::bind("127.0.0.1:0")
            .await
            .unwrap()
            .local_addr()
            .unwrap();
        let mut mapper = Mapper::new(ports(), Duration::from_secs(300));
        mapper.gateway = Some(addr);
        mapper.dialect = Some(Dialect::Pcp);
        *mapper.active.lock() = ports()
            .into_iter()
            .map(|(port, proto)| ActiveMapping {
                port,
                proto,
                nonce: [0; 12],
                dialect: Dialect::Pcp,
                lifetime: Duration::from_secs(120),
            })
            .collect();
        let active = mapper.active.clone();
        let (_shutdown, rx) = mpsc::channel(1);
        let run = tokio::spawn(run_forever(
            mapper,
            Arc::new(move || Some(addr)),
            Arc::new(AtomicUsize::new(0)),
            rx,
        ));
        wait_for(|| active.lock().is_empty()).await;
        assert!(active.lock().is_empty());
        run.abort();
    }

    #[tokio::test]
    async fn a_new_gateway_gets_the_mappings_after_the_old_one_releases_them() {
        let old = fake_gateway(Mode::Pcp { port_shift: 0 }).await;
        let new = fake_gateway(Mode::Pcp { port_shift: 0 }).await;
        let mut mapper = Mapper::new(ports(), Duration::from_secs(300));
        mapper.pass(old.addr).await.unwrap();
        mapper.pass(new.addr).await.unwrap();
        let released = old.requests.lock().clone();
        assert_eq!(released.len(), 4);
        assert!(released[2..].iter().all(|r| r[4..8] == [0, 0, 0, 0]));
        assert_eq!(new.requests.lock().len(), 2);
        assert_eq!(mapper.active.lock().len(), 2);
    }

    #[tokio::test]
    async fn a_replacement_forwarder_maps_only_after_the_old_one_released() {
        let gw = fake_gateway(Mode::Pcp { port_shift: 0 }).await;
        let addr = gw.addr;
        let lookup: GatewayLookup = Arc::new(move || Some(addr));
        let first = spawn_with(ports(), Duration::from_secs(300), lookup.clone(), None);
        wait_for(|| first.mapping_count() == 2).await;
        let second = spawn_with(ports(), Duration::from_secs(300), lookup, first.retire());
        wait_for(|| second.mapping_count() == 2).await;
        assert_eq!(second.mapping_count(), 2);
        let lifetimes: Vec<u32> = gw
            .requests
            .lock()
            .iter()
            .map(|r| u32::from_be_bytes(r[4..8].try_into().unwrap()))
            .collect();
        assert_eq!(lifetimes, [300, 300, 0, 0, 300, 300]);
    }
}
