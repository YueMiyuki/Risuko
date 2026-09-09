//! BEP-10 extension protocol: handshake dict + ut_metadata (BEP-9) and ut_pex (BEP-11) payloads; framed as `Message::Extended` where `ext_id == 0` is the extended-handshake and higher ids map to types negotiated via the handshake's `m` dict

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use bytes::Bytes;

use super::super::bencode::{
    decode_all_external, decode_external, encode_to_vec, DecodeLimits, Value,
};

pub const EXT_HANDSHAKE_ID: u8 = 0;

const EXTENSION_DECODE_LIMITS: DecodeLimits =
    DecodeLimits::new(super::MAX_MESSAGE_BYTES, 32, 65_536);

pub const EXT_NAME_UT_METADATA: &[u8] = b"ut_metadata";
pub const EXT_NAME_UT_PEX: &[u8] = b"ut_pex";
pub const EXT_NAME_UT_HOLEPUNCH: &[u8] = b"ut_holepunch";

pub mod ut_metadata_type {
    pub const REQUEST: i64 = 0;
    pub const DATA: i64 = 1;
    pub const REJECT: i64 = 2;
}

pub mod holepunch_type {
    pub const RENDEZVOUS: u8 = 0;
    pub const CONNECT: u8 = 1;
    pub const ERROR: u8 = 2;
}


pub mod holepunch_err {
    pub const NO_SUCH_PEER: u32 = 1;
    pub const NOT_CONNECTED: u32 = 2;
    pub const NO_SUPPORT: u32 = 3;
    pub const NO_SELF: u32 = 4;
}

#[derive(Debug, Clone, Default)]
pub struct ExtHandshake {
    pub supported: HashMap<Vec<u8>, u8>,
    pub metadata_size: Option<u64>,
    pub client: Option<String>,
    pub yourip: Option<IpAddr>,
    pub reqq: Option<u32>,
    pub port: Option<u16>,
    pub ipv4: Option<Ipv4Addr>,
    pub ipv6: Option<Ipv6Addr>,
}

impl ExtHandshake {
    /// Build our outgoing extended handshake; caller supplies the message ids peers should use to send us each extension
    pub fn new_outgoing(ut_metadata_id: u8, ut_pex_id: u8, metadata_size: Option<u64>) -> Self {
        let mut supported = HashMap::new();
        supported.insert(EXT_NAME_UT_METADATA.to_vec(), ut_metadata_id);
        supported.insert(EXT_NAME_UT_PEX.to_vec(), ut_pex_id);
        Self {
            supported,
            metadata_size,
            client: Some(format!(
                "{} {}",
                env!("CARGO_PKG_NAME"),
                env!("CARGO_PKG_VERSION")
            )),
            yourip: None,
            reqq: None,
            port: None,
            ipv4: None,
            ipv6: None,
        }
    }

    /// Set `yourip` to the peer's address (compact-encoded on the wire); builder helper used by the connection layer per dial / accept
    pub fn with_yourip(mut self, ip: IpAddr) -> Self {
        self.yourip = Some(ip);
        self
    }

    pub fn with_holepunch(mut self, ut_holepunch_id: u8) -> Self {
        self.supported
            .insert(EXT_NAME_UT_HOLEPUNCH.to_vec(), ut_holepunch_id);
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn with_ipv4(mut self, addr: Ipv4Addr) -> Self {
        self.ipv4 = Some(addr);
        self
    }

    pub fn with_ipv6(mut self, addr: Ipv6Addr) -> Self {
        self.ipv6 = Some(addr);
        self
    }

    pub fn encode(&self) -> Bytes {
        let mut m_entries: Vec<(Vec<u8>, Value)> = self
            .supported
            .iter()
            .map(|(k, v)| (k.clone(), Value::Int(*v as i64)))
            .collect();
        m_entries.sort_by(|a, b| a.0.cmp(&b.0));
        // Bencode dicts must be lexicographically sorted by key; `m`, `metadata_size`, `v`, `yourip` are distinct and pushed in that fixed sorted order
        let mut dict = vec![(b"m".to_vec(), Value::Dict(m_entries))];
        if let Some(sz) = self.metadata_size {
            dict.push((b"metadata_size".to_vec(), Value::Int(sz as i64)));
        }
        if let Some(v) = &self.client {
            dict.push((b"v".to_vec(), Value::Bytes(v.as_bytes().to_vec())));
        }
        if let Some(ip) = &self.yourip {
            let bytes = match ip {
                IpAddr::V4(v4) => v4.octets().to_vec(),
                IpAddr::V6(v6) => v6.octets().to_vec(),
            };
            dict.push((b"yourip".to_vec(), Value::Bytes(bytes)));
        }
        if let Some(port) = self.port {
            dict.push((b"p".to_vec(), Value::Int(port as i64)));
        }
        if let Some(ip) = &self.ipv4 {
            dict.push((b"ipv4".to_vec(), Value::Bytes(ip.octets().to_vec())));
        }
        if let Some(ip) = &self.ipv6 {
            dict.push((b"ipv6".to_vec(), Value::Bytes(ip.octets().to_vec())));
        }
        Bytes::from(encode_to_vec(&Value::Dict(dict)))
    }

    pub fn decode(payload: &[u8]) -> Option<Self> {
        let value = decode_all_external(payload, EXTENSION_DECODE_LIMITS).ok()?;
        let dict = value.as_dict()?;
        let mut supported = HashMap::new();
        if let Some((_, m)) = dict.iter().find(|(k, _)| k == b"m") {
            if let Some(m_dict) = m.as_dict() {
                for (k, v) in m_dict {
                    if let Some(id) = v.as_int() {
                        // Extension ID 0 is reserved for the BEP-10 handshake itself and must not appear in the `m` dict
                        if (1..=255).contains(&id) {
                            supported.insert(k.clone(), id as u8);
                        }
                    }
                }
            }
        }
        let metadata_size = dict
            .iter()
            .find(|(k, _)| k == b"metadata_size")
            .and_then(|(_, v)| v.as_int())
            .and_then(|n| if n >= 0 { Some(n as u64) } else { None });
        let client = dict
            .iter()
            .find(|(k, _)| k == b"v")
            .and_then(|(_, v)| v.as_str().map(String::from));
        let yourip = dict
            .iter()
            .find(|(k, _)| k == b"yourip")
            .and_then(|(_, v)| v.as_bytes())
            .and_then(|bytes| match bytes.len() {
                4 => {
                    let arr: [u8; 4] = bytes.try_into().ok()?;
                    Some(IpAddr::V4(std::net::Ipv4Addr::from(arr)))
                }
                16 => {
                    let arr: [u8; 16] = bytes.try_into().ok()?;
                    Some(IpAddr::V6(std::net::Ipv6Addr::from(arr)))
                }
                _ => None,
            });
        let reqq = dict
            .iter()
            .find(|(k, _)| k == b"reqq")
            .and_then(|(_, v)| v.as_int())
            .and_then(|n| if n > 0 { Some(n as u32) } else { None });
        let port = dict
            .iter()
            .find(|(k, _)| k == b"p")
            .and_then(|(_, v)| v.as_int())
            .and_then(|n| u16::try_from(n).ok());
        let ipv4 = dict
            .iter()
            .find(|(k, _)| k == b"ipv4")
            .and_then(|(_, v)| v.as_bytes())
            .and_then(|b| <[u8; 4]>::try_from(b).ok())
            .map(Ipv4Addr::from);
        let ipv6 = dict
            .iter()
            .find(|(k, _)| k == b"ipv6")
            .and_then(|(_, v)| v.as_bytes())
            .and_then(|b| <[u8; 16]>::try_from(b).ok())
            .map(Ipv6Addr::from);
        Some(Self {
            supported,
            metadata_size,
            client,
            yourip,
            reqq,
            port,
            ipv4,
            ipv6,
        })
    }

    pub fn ut_metadata_id(&self) -> Option<u8> {
        self.supported.get(EXT_NAME_UT_METADATA).copied()
    }

    pub fn ut_pex_id(&self) -> Option<u8> {
        self.supported.get(EXT_NAME_UT_PEX).copied()
    }

    pub fn ut_holepunch_id(&self) -> Option<u8> {
        self.supported.get(EXT_NAME_UT_HOLEPUNCH).copied()
    }
}

/// Build a `ut_metadata` request for a given piece index
pub fn ut_metadata_request(piece: i64) -> Bytes {
    let dict = Value::Dict(vec![
        (b"msg_type".to_vec(), Value::Int(ut_metadata_type::REQUEST)),
        (b"piece".to_vec(), Value::Int(piece)),
    ]);
    Bytes::from(encode_to_vec(&dict))
}

/// Build a `ut_metadata` data response
pub fn ut_metadata_data(piece: i64, total_size: i64, block: &[u8]) -> Bytes {
    let header = Value::Dict(vec![
        (b"msg_type".to_vec(), Value::Int(ut_metadata_type::DATA)),
        (b"piece".to_vec(), Value::Int(piece)),
        (b"total_size".to_vec(), Value::Int(total_size)),
    ]);
    let mut out = encode_to_vec(&header);
    out.extend_from_slice(block);
    Bytes::from(out)
}

/// Build a `ut_metadata` reject response (out-of-range pieces or when we cannot serve the request)
pub fn ut_metadata_reject(piece: i64) -> Bytes {
    let dict = Value::Dict(vec![
        (b"msg_type".to_vec(), Value::Int(ut_metadata_type::REJECT)),
        (b"piece".to_vec(), Value::Int(piece)),
    ]);
    Bytes::from(encode_to_vec(&dict))
}

/// Parse a `ut_metadata` message, returning the parsed header and any trailing data block (for DATA messages)
pub struct UtMetadataMsg {
    pub msg_type: i64,
    pub piece: i64,
    pub total_size: Option<i64>,
    pub block: Bytes,
}

pub fn parse_ut_metadata(payload: Bytes) -> Option<UtMetadataMsg> {
    // ut_metadata carries a bencoded dict followed by raw data for DATA messages; we need how many bytes the dict consumed
    let mut p = decode_external(&payload, EXTENSION_DECODE_LIMITS).ok()?;
    let block = payload.slice(p.span.end..);
    let dict = match &mut p.value {
        Value::Dict(d) => std::mem::take(&mut *d),
        _ => return None,
    };
    let msg_type = dict
        .iter()
        .find(|(k, _)| k == b"msg_type")
        .and_then(|(_, v)| v.as_int())?;
    let piece = dict
        .iter()
        .find(|(k, _)| k == b"piece")
        .and_then(|(_, v)| v.as_int())?;
    let total_size = dict
        .iter()
        .find(|(k, _)| k == b"total_size")
        .and_then(|(_, v)| v.as_int());
    Some(UtMetadataMsg {
        msg_type,
        piece,
        total_size,
        block,
    })
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UtPex {
    pub added: Vec<(SocketAddr, u8)>,
    pub dropped: Vec<SocketAddr>,
}

fn valid_pex_endpoint(addr: SocketAddr) -> bool {
    addr.port() != 0 && !addr.ip().is_unspecified() && !addr.ip().is_multicast()
}

fn parse_compact_pex(bytes: &[u8], v6: bool) -> Vec<SocketAddr> {
    let width = if v6 { 18 } else { 6 };
    bytes
        .chunks_exact(width)
        .filter_map(|chunk| {
            let addr = if v6 {
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&chunk[..16]);
                SocketAddr::from((
                    std::net::Ipv6Addr::from(octets),
                    u16::from_be_bytes([chunk[16], chunk[17]]),
                ))
            } else {
                SocketAddr::from((
                    std::net::Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]),
                    u16::from_be_bytes([chunk[4], chunk[5]]),
                ))
            };
            valid_pex_endpoint(addr).then_some(addr)
        })
        .collect()
}

fn parse_compact_pex_flagged(
    bytes: &[u8],
    flags: &[u8],
    v6: bool,
    remaining: usize,
) -> Vec<(SocketAddr, u8)> {
    let width = if v6 { 18 } else { 6 };
    bytes
        .chunks_exact(width)
        .enumerate()
        .filter_map(|(index, chunk)| {
            let addr = if v6 {
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&chunk[..16]);
                SocketAddr::from((
                    std::net::Ipv6Addr::from(octets),
                    u16::from_be_bytes([chunk[16], chunk[17]]),
                ))
            } else {
                SocketAddr::from((
                    std::net::Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]),
                    u16::from_be_bytes([chunk[4], chunk[5]]),
                ))
            };
            valid_pex_endpoint(addr).then_some((addr, flags.get(index).copied().unwrap_or(0)))
        })
        .take(remaining)
        .collect()
}

pub fn parse_ut_pex_full(payload: &[u8]) -> Option<UtPex> {
    let value = decode_all_external(payload, EXTENSION_DECODE_LIMITS).ok()?;
    let dict = value.as_dict()?;
    let bytes = |key: &[u8]| {
        dict.iter()
            .find(|(k, _)| k.as_slice() == key)
            .and_then(|(_, v)| v.as_bytes())
            .unwrap_or_default()
    };
    let flags = |key: &[u8]| {
        dict.iter()
            .find(|(k, _)| k.as_slice() == key)
            .and_then(|(_, v)| v.as_bytes())
            .unwrap_or_default()
    };
    let mut added = Vec::new();
    added.extend(parse_compact_pex_flagged(
        bytes(b"added"),
        flags(b"added.f"),
        false,
        50,
    ));
    if added.len() < 50 {
        added.extend(parse_compact_pex_flagged(
            bytes(b"added6"),
            flags(b"added6.f"),
            true,
            50 - added.len(),
        ));
    }
    let mut dropped = parse_compact_pex(bytes(b"dropped"), false);
    dropped.extend(parse_compact_pex(bytes(b"dropped6"), true));
    dropped.truncate(50);
    Some(UtPex { added, dropped })
}

pub fn parse_ut_pex(
    payload: &[u8],
) -> Option<(Vec<std::net::SocketAddr>, Vec<std::net::SocketAddr>)> {
    let pex = parse_ut_pex_full(payload)?;
    let mut v4 = Vec::new();
    let mut v6 = Vec::new();
    for (addr, _) in pex.added {
        if addr.is_ipv4() {
            v4.push(addr);
        } else {
            v6.push(addr);
        }
    }
    Some((v4, v6))
}

pub fn build_ut_pex(added: &[SocketAddr], dropped: &[SocketAddr]) -> Bytes {
    fn split(addrs: &[SocketAddr]) -> (Vec<u8>, Vec<u8>) {
        let mut v4 = Vec::new();
        let mut v6 = Vec::new();
        for a in addrs {
            match a.ip() {
                IpAddr::V4(ip) => {
                    v4.extend_from_slice(&ip.octets());
                    v4.extend_from_slice(&a.port().to_be_bytes());
                }
                IpAddr::V6(ip) => {
                    v6.extend_from_slice(&ip.octets());
                    v6.extend_from_slice(&a.port().to_be_bytes());
                }
            }
        }
        (v4, v6)
    }
    let added: Vec<_> = added
        .iter()
        .copied()
        .filter(|addr| valid_pex_endpoint(*addr))
        .take(50)
        .collect();
    let dropped: Vec<_> = dropped
        .iter()
        .copied()
        .filter(|addr| valid_pex_endpoint(*addr))
        .take(50)
        .collect();
    let (added4, added6) = split(&added);
    let (dropped4, dropped6) = split(&dropped);
    let dict = vec![
        (b"added".to_vec(), Value::Bytes(added4.clone())),
        (
            b"added.f".to_vec(),
            Value::Bytes(vec![0u8; added4.len() / 6]),
        ),
        (b"added6".to_vec(), Value::Bytes(added6.clone())),
        (
            b"added6.f".to_vec(),
            Value::Bytes(vec![0u8; added6.len() / 18]),
        ),
        (b"dropped".to_vec(), Value::Bytes(dropped4)),
        (b"dropped6".to_vec(), Value::Bytes(dropped6)),
    ];
    Bytes::from(encode_to_vec(&Value::Dict(dict)))
}

/// A decoded BEP-55 ut_holepunch message
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HolepunchMsg {
    pub msg_type: u8,
    pub addr: SocketAddr,
    pub err_code: u32,
}

pub fn build_holepunch(msg_type: u8, addr: SocketAddr, err_code: u32) -> Bytes {
    let mut buf = Vec::with_capacity(24);
    buf.push(msg_type);
    match addr.ip() {
        IpAddr::V4(v4) => {
            buf.push(0);
            buf.extend_from_slice(&v4.octets());
        }
        IpAddr::V6(v6) => {
            buf.push(1);
            buf.extend_from_slice(&v6.octets());
        }
    }
    buf.extend_from_slice(&addr.port().to_be_bytes());
    buf.extend_from_slice(
        &(if msg_type == holepunch_type::ERROR {
            err_code
        } else {
            0
        })
        .to_be_bytes(),
    );
    Bytes::from(buf)
}

/// Decode a BEP-55 ut_holepunch message; `None` on a malformed/short payload or an unknown address family
pub fn parse_holepunch(payload: &[u8]) -> Option<HolepunchMsg> {
    if payload.len() < 2 {
        return None;
    }
    let msg_type = payload[0];
    let addr_type = payload[1];
    let (ip, port_off): (IpAddr, usize) = match addr_type {
        0 => {
            if payload.len() < 2 + 4 + 2 {
                return None;
            }
            let arr: [u8; 4] = payload[2..6].try_into().ok()?;
            (IpAddr::V4(Ipv4Addr::from(arr)), 6)
        }
        1 => {
            if payload.len() < 2 + 16 + 2 {
                return None;
            }
            let arr: [u8; 16] = payload[2..18].try_into().ok()?;
            (IpAddr::V6(Ipv6Addr::from(arr)), 18)
        }
        _ => return None,
    };
    let port = u16::from_be_bytes([payload[port_off], payload[port_off + 1]]);
    let eo = port_off + 2;
    if payload.len() != eo + 4 {
        return None;
    }
    let err_code = u32::from_be_bytes(payload[eo..eo + 4].try_into().ok()?);
    if msg_type != holepunch_type::ERROR && err_code != 0 {
        return None;
    }
    Some(HolepunchMsg {
        msg_type,
        addr: SocketAddr::new(ip, port),
        err_code,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ut_pex_encode_parse_round_trip() {
        let v4a: SocketAddr = "10.1.2.3:6881".parse().unwrap();
        let v6a: SocketAddr = "[2001:db8::7]:51413".parse().unwrap();
        let gone: SocketAddr = "10.9.9.9:1000".parse().unwrap();
        let payload = build_ut_pex(&[v4a, v6a], &[gone]);
        let (v4, v6) = parse_ut_pex(&payload).unwrap();
        assert_eq!(v4, vec![v4a]);
        assert_eq!(v6, vec![v6a]);
    }

    #[test]
    fn ut_pex_full_retains_flags_and_dropped_contacts() {
        let mut added = vec![1, 2, 3, 4, 0x1a, 0xe1];
        let dropped = vec![5, 6, 7, 8, 0x1a, 0xe2];
        let payload = encode_to_vec(&Value::Dict(vec![
            (b"added".to_vec(), Value::Bytes(std::mem::take(&mut added))),
            (b"added.f".to_vec(), Value::Bytes(vec![0x02])),
            (b"dropped".to_vec(), Value::Bytes(dropped)),
        ]));
        let parsed = parse_ut_pex_full(&payload).unwrap();
        assert_eq!(parsed.added, vec![("1.2.3.4:6881".parse().unwrap(), 0x02)]);
        assert_eq!(parsed.dropped, vec!["5.6.7.8:6882".parse().unwrap()]);
    }

    #[test]
    fn handshake_round_trip() {
        let out = ExtHandshake::new_outgoing(3, 4, Some(1024));
        let bytes = out.encode();
        let parsed = ExtHandshake::decode(&bytes).unwrap();
        assert_eq!(parsed.ut_metadata_id(), Some(3));
        assert_eq!(parsed.ut_pex_id(), Some(4));
        assert_eq!(parsed.metadata_size, Some(1024));
        assert!(parsed.client.is_some());
    }

    #[test]
    fn handshake_parses_reqq() {
        let payload = encode_to_vec(&Value::Dict(vec![
            (
                b"m".to_vec(),
                Value::Dict(vec![(b"ut_metadata".to_vec(), Value::Int(2))]),
            ),
            (b"reqq".to_vec(), Value::Int(250)),
        ]));
        let parsed = ExtHandshake::decode(&payload).unwrap();
        assert_eq!(parsed.reqq, Some(250));
        assert_eq!(parsed.ut_metadata_id(), Some(2));
    }

    #[test]
    fn ut_metadata_request_parse() {
        let bytes = ut_metadata_request(5);
        let parsed = parse_ut_metadata(bytes).unwrap();
        assert_eq!(parsed.msg_type, ut_metadata_type::REQUEST);
        assert_eq!(parsed.piece, 5);
        assert!(parsed.block.is_empty());
    }

    #[test]
    fn ut_metadata_data_parse() {
        let data = vec![0xaau8; 200];
        let bytes = ut_metadata_data(2, 1_000_000, &data);
        let parsed = parse_ut_metadata(bytes).unwrap();
        assert_eq!(parsed.msg_type, ut_metadata_type::DATA);
        assert_eq!(parsed.piece, 2);
        assert_eq!(parsed.total_size, Some(1_000_000));
        assert_eq!(parsed.block.as_ref(), data.as_slice());
    }

    #[test]
    fn ut_metadata_reject_parse() {
        let bytes = ut_metadata_reject(7);
        let parsed = parse_ut_metadata(bytes).unwrap();
        assert_eq!(parsed.msg_type, ut_metadata_type::REJECT);
        assert_eq!(parsed.piece, 7);
        assert!(parsed.block.is_empty());
    }

    #[test]
    fn yourip_ipv4_round_trip() {
        let ip = std::net::IpAddr::V4("192.168.1.42".parse().unwrap());
        let out = ExtHandshake::new_outgoing(3, 4, None).with_yourip(ip);
        let bytes = out.encode();
        let parsed = ExtHandshake::decode(&bytes).unwrap();
        assert_eq!(parsed.yourip, Some(ip));
    }

    #[test]
    fn yourip_ipv6_round_trip() {
        let ip = std::net::IpAddr::V6("2001:db8::1".parse().unwrap());
        let out = ExtHandshake::new_outgoing(3, 4, None).with_yourip(ip);
        let bytes = out.encode();
        let parsed = ExtHandshake::decode(&bytes).unwrap();
        assert_eq!(parsed.yourip, Some(ip));
    }

    #[test]
    fn advertised_addresses_and_port_round_trip() {
        let out = ExtHandshake::new_outgoing(3, 4, None)
            .with_port(51413)
            .with_ipv4("198.51.100.7".parse().unwrap())
            .with_ipv6("2001:db8::7".parse().unwrap());
        let parsed = ExtHandshake::decode(&out.encode()).unwrap();
        assert_eq!(parsed.port, Some(51413));
        assert_eq!(parsed.ipv4, Some("198.51.100.7".parse().unwrap()));
        assert_eq!(parsed.ipv6, Some("2001:db8::7".parse().unwrap()));
    }

    #[test]
    fn holepunch_advertised_in_handshake() {
        let out = ExtHandshake::new_outgoing(3, 4, None).with_holepunch(5);
        let bytes = out.encode();
        let parsed = ExtHandshake::decode(&bytes).unwrap();
        assert_eq!(parsed.ut_holepunch_id(), Some(5));
        // Existing extensions remain advertised alongside it
        assert_eq!(parsed.ut_metadata_id(), Some(3));
        assert_eq!(parsed.ut_pex_id(), Some(4));
    }

    #[test]
    fn holepunch_handshake_absent_when_not_advertised() {
        let out = ExtHandshake::new_outgoing(3, 4, None);
        let parsed = ExtHandshake::decode(&out.encode()).unwrap();
        assert_eq!(parsed.ut_holepunch_id(), None);
    }

    #[test]
    fn holepunch_connect_v4_round_trip() {
        let addr: SocketAddr = "203.0.113.7:51413".parse().unwrap();
        let bytes = build_holepunch(holepunch_type::CONNECT, addr, 0);
        assert_eq!(bytes.len(), 12);
        let parsed = parse_holepunch(&bytes).unwrap();
        assert_eq!(parsed.msg_type, holepunch_type::CONNECT);
        assert_eq!(parsed.addr, addr);
        assert_eq!(parsed.err_code, 0);
    }

    #[test]
    fn holepunch_rendezvous_v6_round_trip() {
        let addr: SocketAddr = "[2001:db8::dead:beef]:6881".parse().unwrap();
        let bytes = build_holepunch(holepunch_type::RENDEZVOUS, addr, 0);
        assert_eq!(bytes.len(), 24);
        let parsed = parse_holepunch(&bytes).unwrap();
        assert_eq!(parsed.msg_type, holepunch_type::RENDEZVOUS);
        assert_eq!(parsed.addr, addr);
    }

    #[test]
    fn holepunch_error_carries_code() {
        let addr: SocketAddr = "198.51.100.9:1337".parse().unwrap();
        let bytes = build_holepunch(holepunch_type::ERROR, addr, holepunch_err::NO_SUCH_PEER);
        assert_eq!(bytes.len(), 12); // ...+ err_code(4)
        let parsed = parse_holepunch(&bytes).unwrap();
        assert_eq!(parsed.msg_type, holepunch_type::ERROR);
        assert_eq!(parsed.addr, addr);
        assert_eq!(parsed.err_code, holepunch_err::NO_SUCH_PEER);
    }

    #[test]
    fn holepunch_rejects_truncated_and_unknown_family() {
        assert!(parse_holepunch(&[]).is_none());
        assert!(parse_holepunch(&[holepunch_type::CONNECT]).is_none());
        assert!(parse_holepunch(&[holepunch_type::CONNECT, 0, 1, 2, 3, 4]).is_none());
        // unknown address family
        assert!(parse_holepunch(&[holepunch_type::CONNECT, 9, 1, 2, 3, 4, 0, 0]).is_none());
    }
}
