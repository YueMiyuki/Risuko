use std::fmt;
use std::net::{Ipv4Addr, SocketAddrV4};

use super::routing::{Contact, KadId};

pub const KAD_PROTOCOL: u8 = 0xe4;
pub const KAD_PROTOCOL_COMPRESSED: u8 = 0xe5;
pub const MAX_DATAGRAM_SIZE: usize = 64 * 1024;
const MAX_INFLATED_SIZE: usize = 64 * 1024;

pub const OP_BOOTSTRAP_REQ: u8 = 0x01;
pub const OP_BOOTSTRAP_RES: u8 = 0x09;
pub const OP_HELLO_REQ: u8 = 0x11;
pub const OP_HELLO_RES: u8 = 0x19;
pub const OP_HELLO_RES_ACK: u8 = 0x22;
pub const OP_ROUTING_REQ: u8 = 0x21;
pub const OP_ROUTING_RES: u8 = 0x29;
pub const OP_SEARCH_SOURCE_REQ: u8 = 0x34;
pub const OP_SEARCH_RES: u8 = 0x3b;
pub const OP_PING: u8 = 0x60;
pub const OP_PONG: u8 = 0x61;

pub const TAG_SOURCE_IP: u8 = 0xfe;
pub const TAG_SOURCE_PORT: u8 = 0xfd;
pub const TAG_SOURCE_UDP_PORT: u8 = 0xfc;
pub const TAG_SOURCE_TYPE: u8 = 0xff;

fn id_to_wire(id: &KadId) -> KadId {
    let mut wire = *id;
    for chunk in wire.chunks_exact_mut(4) {
        chunk.reverse();
    }
    wire
}

fn id_from_wire(id: &KadId) -> KadId {
    id_to_wire(id)
}

pub const TAGTYPE_HASH16: u8 = 0x01;
pub const TAGTYPE_STRING: u8 = 0x02;
pub const TAGTYPE_UINT32: u8 = 0x03;
pub const TAGTYPE_FLOAT32: u8 = 0x04;
pub const TAGTYPE_BOOL: u8 = 0x05;
pub const TAGTYPE_BOOLARRAY: u8 = 0x06;
pub const TAGTYPE_BLOB: u8 = 0x07;
pub const TAGTYPE_UINT16: u8 = 0x08;
pub const TAGTYPE_UINT8: u8 = 0x09;
pub const TAGTYPE_BSOB: u8 = 0x0a;
pub const TAGTYPE_UINT64: u8 = 0x0b;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KadPacket {
    pub opcode: u8,
    pub payload: Vec<u8>,
}

impl KadPacket {
    pub fn new(opcode: u8, payload: Vec<u8>) -> Self {
        Self { opcode, payload }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.payload.len() + 2);
        out.push(KAD_PROTOCOL);
        out.push(self.opcode);
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(data: &[u8]) -> Result<Self, WireError> {
        if data.len() < 2 {
            return Err(WireError::Truncated {
                context: "Kad packet header",
            });
        }
        if data.len() > MAX_DATAGRAM_SIZE {
            return Err(WireError::TooLarge(data.len()));
        }
        match data[0] {
            KAD_PROTOCOL => Ok(Self::new(data[1], data[2..].to_vec())),
            KAD_PROTOCOL_COMPRESSED => {
                use std::io::Read;
                let mut inflated = Vec::new();
                flate2::read::ZlibDecoder::new(&data[2..])
                    .take(MAX_INFLATED_SIZE as u64 + 1)
                    .read_to_end(&mut inflated)
                    .map_err(|_| WireError::InvalidValue("compressed Kad packet"))?;
                if inflated.len() > MAX_INFLATED_SIZE {
                    return Err(WireError::TooLarge(inflated.len()));
                }
                Ok(Self::new(data[1], inflated))
            }
            protocol => Err(WireError::InvalidProtocol(protocol)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireError {
    Truncated { context: &'static str },
    InvalidProtocol(u8),
    TooLarge(usize),
    InvalidValue(&'static str),
    InvalidTagType(u8),
    InvalidUtf8,
    InvalidCount { context: &'static str, count: usize },
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated { context } => write!(f, "truncated {context}"),
            Self::InvalidProtocol(value) => write!(f, "invalid Kad protocol 0x{value:02x}"),
            Self::TooLarge(size) => write!(f, "Kad datagram is too large ({size} bytes)"),
            Self::InvalidValue(context) => write!(f, "invalid {context}"),
            Self::InvalidTagType(value) => write!(f, "unsupported tag type 0x{value:02x}"),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8 in Kad tag"),
            Self::InvalidCount { context, count } => {
                write!(f, "invalid {context} count {count}")
            }
        }
    }
}

impl std::error::Error for WireError {}

#[derive(Debug, Clone, PartialEq)]
pub enum KadTagValue {
    UInt(u64),
    String(String),
    Hash([u8; 16]),
    Float(f32),
    Bool(bool),
    BoolArray(Vec<bool>),
    Blob(Vec<u8>),
    Bsob(Vec<u8>),
}

impl Eq for KadTagValue {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KadTag {
    pub name: KadTagName,
    pub value: KadTagValue,
    pub wire_type: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KadTagName {
    Id(u8),
    Text(String),
}

impl KadTag {
    pub fn id(name: u8, value: KadTagValue) -> Self {
        Self {
            name: KadTagName::Id(name),
            value,
            wire_type: None,
        }
    }

    pub fn uint(name: u8, value: u64) -> Self {
        Self::id(name, KadTagValue::UInt(value))
    }

    pub fn get_uint(&self) -> Option<u64> {
        match self.value {
            KadTagValue::UInt(value) => Some(value),
            _ => None,
        }
    }

    pub fn id_value(&self, id: u8) -> Option<&KadTagValue> {
        matches!(self.name, KadTagName::Id(value) if value == id).then_some(&self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KadWireContact {
    pub id: KadId,
    pub ip: Ipv4Addr,
    pub udp_port: u16,
    pub tcp_port: u16,
    pub version: u8,
}

impl KadWireContact {
    pub fn to_contact(&self) -> Contact {
        Contact::new(
            self.id,
            SocketAddrV4::new(self.ip, self.udp_port),
            self.tcp_port,
            self.version,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingResponse {
    pub target: KadId,
    pub contacts: Vec<KadWireContact>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSearchResponse {
    pub target: KadId,
    pub sources: Vec<KadSourceRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KadSourceRecord {
    pub id: KadId,
    pub tags: Vec<KadTag>,
}

impl KadSourceRecord {
    pub fn source_type(&self) -> Option<u8> {
        self.tag_uint(TAG_SOURCE_TYPE)
            .and_then(|value| u8::try_from(value).ok())
    }

    pub fn ip(&self) -> Option<Ipv4Addr> {
        let raw = u32::try_from(self.tag_uint(TAG_SOURCE_IP)?).ok()?;
        Some(Ipv4Addr::from(raw.to_be_bytes()))
    }

    pub fn tcp_port(&self) -> Option<u16> {
        self.tag_uint(TAG_SOURCE_PORT)
            .and_then(|value| u16::try_from(value).ok())
    }

    pub fn udp_port(&self) -> Option<u16> {
        self.tag_uint(TAG_SOURCE_UDP_PORT)
            .and_then(|value| u16::try_from(value).ok())
    }

    pub fn direct_addr(&self) -> Option<SocketAddrV4> {
        Some(SocketAddrV4::new(self.ip()?, self.tcp_port()?))
    }

    fn tag_uint(&self, id: u8) -> Option<u64> {
        let mut value = None;
        for tag in &self.tags {
            if !matches!(tag.name, KadTagName::Id(name) if name == id) {
                continue;
            }
            let KadTagValue::UInt(candidate) = &tag.value else {
                return None;
            };
            if value.replace(*candidate).is_some() {
                return None;
            }
        }
        value
    }
}

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn take(&mut self, len: usize, context: &'static str) -> Result<&'a [u8], WireError> {
        if len > self.remaining() {
            return Err(WireError::Truncated { context });
        }
        let start = self.pos;
        self.pos += len;
        Ok(&self.data[start..self.pos])
    }

    fn u8(&mut self, context: &'static str) -> Result<u8, WireError> {
        Ok(self.take(1, context)?[0])
    }

    fn u16(&mut self, context: &'static str) -> Result<u16, WireError> {
        Ok(u16::from_le_bytes(
            self.take(2, context)?.try_into().unwrap(),
        ))
    }

    fn u32(&mut self, context: &'static str) -> Result<u32, WireError> {
        Ok(u32::from_le_bytes(
            self.take(4, context)?.try_into().unwrap(),
        ))
    }

    fn u64(&mut self, context: &'static str) -> Result<u64, WireError> {
        Ok(u64::from_le_bytes(
            self.take(8, context)?.try_into().unwrap(),
        ))
    }

    fn id(&mut self, context: &'static str) -> Result<KadId, WireError> {
        let wire_id: KadId = self.take(16, context)?.try_into().unwrap();
        Ok(id_from_wire(&wire_id))
    }

    fn finish(&self, context: &'static str) -> Result<(), WireError> {
        if self.remaining() != 0 {
            return Err(WireError::InvalidValue(context));
        }
        Ok(())
    }
}

fn encode_name(out: &mut Vec<u8>, name: &KadTagName, wire_type: u8) {
    out.push(wire_type);
    match name {
        KadTagName::Id(id) => {
            out.extend_from_slice(&1u16.to_le_bytes());
            out.push(*id);
        }
        KadTagName::Text(text) => {
            let bytes = text.as_bytes();
            let len = u16::try_from(bytes.len()).unwrap_or(u16::MAX);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&bytes[..usize::from(len)]);
        }
    }
}

fn natural_wire_type(value: &KadTagValue) -> u8 {
    match value {
        KadTagValue::Hash(_) => TAGTYPE_HASH16,
        KadTagValue::String(_) => TAGTYPE_STRING,
        KadTagValue::UInt(value) => {
            if *value <= u8::MAX as u64 {
                TAGTYPE_UINT8
            } else if *value <= u16::MAX as u64 {
                TAGTYPE_UINT16
            } else if *value <= u32::MAX as u64 {
                TAGTYPE_UINT32
            } else {
                TAGTYPE_UINT64
            }
        }
        KadTagValue::Float(_) => TAGTYPE_FLOAT32,
        KadTagValue::Bool(_) => TAGTYPE_BOOL,
        KadTagValue::BoolArray(_) => TAGTYPE_BOOLARRAY,
        KadTagValue::Blob(_) => TAGTYPE_BLOB,
        KadTagValue::Bsob(_) => TAGTYPE_BSOB,
    }
}

pub fn encode_tag(out: &mut Vec<u8>, tag: &KadTag) {
    let wire_type = tag
        .wire_type
        .unwrap_or_else(|| natural_wire_type(&tag.value));
    encode_name(out, &tag.name, wire_type);
    match (&tag.value, wire_type & 0x7f) {
        (KadTagValue::Hash(value), TAGTYPE_HASH16) => out.extend_from_slice(value),
        (KadTagValue::String(value), TAGTYPE_STRING) => {
            let bytes = value.as_bytes();
            let len = u16::try_from(bytes.len()).unwrap_or(u16::MAX);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(&bytes[..usize::from(len)]);
        }
        (KadTagValue::UInt(value), TAGTYPE_UINT8) => out.push(*value as u8),
        (KadTagValue::UInt(value), TAGTYPE_UINT16) => {
            out.extend_from_slice(&(*value as u16).to_le_bytes())
        }
        (KadTagValue::UInt(value), TAGTYPE_UINT32) => {
            out.extend_from_slice(&(*value as u32).to_le_bytes())
        }
        (KadTagValue::UInt(value), TAGTYPE_UINT64) => out.extend_from_slice(&value.to_le_bytes()),
        (KadTagValue::Float(value), TAGTYPE_FLOAT32) => out.extend_from_slice(&value.to_le_bytes()),
        (KadTagValue::Bool(value), TAGTYPE_BOOL) => out.push(u8::from(*value)),
        (KadTagValue::BoolArray(values), TAGTYPE_BOOLARRAY) => {
            let len = u16::try_from(values.len()).unwrap_or(u16::MAX);
            out.extend_from_slice(&len.to_le_bytes());
            // eMule's reader consumes one sentinel byte per eight booleans (`len / 8 + 1`), even for empty arrays
            let bytes = values.len() / 8 + 1;
            for byte in 0..bytes {
                let mut value = 0u8;
                for bit in 0..8 {
                    let index = byte * 8 + bit;
                    if values.get(index).copied().unwrap_or(false) {
                        value |= 1 << bit;
                    }
                }
                out.push(value);
            }
        }
        (KadTagValue::Blob(value), TAGTYPE_BLOB) => {
            let len = u32::try_from(value.len()).unwrap_or(u32::MAX);
            out.extend_from_slice(&len.to_le_bytes());
            out.extend_from_slice(
                &value[..usize::try_from(len).unwrap_or(value.len()).min(value.len())],
            );
        }
        (KadTagValue::Bsob(value), TAGTYPE_BSOB) => {
            debug_assert!(
                value.len() <= usize::from(u8::MAX),
                "BSOB value exceeds single-byte length and will be truncated"
            );
            let len = value.len().min(usize::from(u8::MAX));
            out.push(len as u8);
            out.extend_from_slice(&value[..len]);
        }
        _ => {}
    }
}

fn decode_tag(cursor: &mut Cursor<'_>) -> Result<KadTag, WireError> {
    let wire_type = cursor.u8("tag type")?;
    let len = cursor.u16("tag name length")? as usize;
    if len == 0 || len > 4096 {
        return Err(WireError::InvalidValue("tag name length"));
    }
    let name = if len == 1 {
        KadTagName::Id(cursor.u8("numeric tag name")?)
    } else {
        let bytes = cursor.take(len, "tag name")?;
        KadTagName::Text(String::from_utf8(bytes.to_vec()).map_err(|_| WireError::InvalidUtf8)?)
    };

    let value = match wire_type {
        TAGTYPE_HASH16 => KadTagValue::Hash(cursor.take(16, "hash tag")?.try_into().unwrap()),
        TAGTYPE_STRING => {
            let len = cursor.u16("string tag length")? as usize;
            if len > 64 * 1024 {
                return Err(WireError::InvalidValue("string tag length"));
            }
            KadTagValue::String(
                String::from_utf8(cursor.take(len, "string tag")?.to_vec())
                    .map_err(|_| WireError::InvalidUtf8)?,
            )
        }
        TAGTYPE_UINT32 => KadTagValue::UInt(cursor.u32("uint32 tag")? as u64),
        TAGTYPE_FLOAT32 => KadTagValue::Float(f32::from_le_bytes(
            cursor.take(4, "float tag")?.try_into().unwrap(),
        )),
        TAGTYPE_BOOL => KadTagValue::Bool(cursor.u8("bool tag")? != 0),
        TAGTYPE_BOOLARRAY => {
            let count = cursor.u16("bool array count")? as usize;
            if count > MAX_DATAGRAM_SIZE * 8 {
                return Err(WireError::InvalidCount {
                    context: "bool array",
                    count,
                });
            }
            let bytes = count / 8 + 1;
            let raw = cursor.take(bytes, "bool array")?;
            let values = (0..count)
                .map(|index| raw[index / 8] & (1 << (index % 8)) != 0)
                .collect();
            KadTagValue::BoolArray(values)
        }
        TAGTYPE_BLOB => {
            let len = cursor.u32("blob length")? as usize;
            if len > MAX_DATAGRAM_SIZE || len > cursor.remaining() {
                return Err(WireError::InvalidValue("blob length"));
            }
            let value = cursor.take(len, "blob value")?.to_vec();
            KadTagValue::Blob(value)
        }
        TAGTYPE_BSOB => {
            let len = cursor.u8("bsob length")? as usize;
            if len > cursor.remaining() {
                return Err(WireError::InvalidValue("bsob length"));
            }
            KadTagValue::Bsob(cursor.take(len, "bsob value")?.to_vec())
        }
        TAGTYPE_UINT16 => KadTagValue::UInt(cursor.u16("uint16 tag")? as u64),
        TAGTYPE_UINT8 => KadTagValue::UInt(cursor.u8("uint8 tag")? as u64),
        TAGTYPE_UINT64 => KadTagValue::UInt(cursor.u64("uint64 tag")?),
        compressed if (0x11..=0x26).contains(&compressed) => {
            let len = usize::from(compressed - 0x10);
            KadTagValue::String(
                String::from_utf8(cursor.take(len, "compressed string tag")?.to_vec())
                    .map_err(|_| WireError::InvalidUtf8)?,
            )
        }
        other => return Err(WireError::InvalidTagType(other)),
    };

    Ok(KadTag {
        name,
        value,
        wire_type: Some(wire_type),
    })
}

fn decode_tags(cursor: &mut Cursor<'_>) -> Result<Vec<KadTag>, WireError> {
    let count = cursor.u8("tag count")? as usize;
    if count > 128 {
        return Err(WireError::InvalidCount {
            context: "tag",
            count,
        });
    }
    let mut tags = Vec::with_capacity(count);
    for _ in 0..count {
        tags.push(decode_tag(cursor)?);
    }
    Ok(tags)
}

fn encode_tags(out: &mut Vec<u8>, tags: &[KadTag]) {
    let count = u8::try_from(tags.len()).unwrap_or(u8::MAX);
    out.push(count);
    for tag in tags.iter().take(usize::from(count)) {
        encode_tag(out, tag);
    }
}

pub fn build_bootstrap_request() -> KadPacket {
    KadPacket::new(OP_BOOTSTRAP_REQ, Vec::new())
}

pub fn parse_bootstrap_response(
    payload: &[u8],
) -> Result<(KadWireContact, Vec<KadWireContact>), WireError> {
    let mut cursor = Cursor::new(payload);
    let id = cursor.id("bootstrap node id")?;
    let tcp_port = cursor.u16("bootstrap tcp port")?;
    let version = cursor.u8("bootstrap version")?;
    let count = cursor.u16("bootstrap contact count")? as usize;
    if count > 64 {
        return Err(WireError::InvalidCount {
            context: "bootstrap contact",
            count,
        });
    }
    let mut contacts = Vec::with_capacity(count);
    for _ in 0..count {
        contacts.push(read_contact(&mut cursor)?);
    }
    cursor.finish("bootstrap response trailing bytes")?;
    Ok((
        KadWireContact {
            id,
            ip: Ipv4Addr::UNSPECIFIED,
            udp_port: 0,
            tcp_port,
            version,
        },
        contacts,
    ))
}

pub fn build_hello_request(
    node_id: &KadId,
    udp_port: u16,
    tcp_port: u16,
    version: u8,
) -> KadPacket {
    let mut payload = Vec::with_capacity(64);
    payload.extend_from_slice(&id_to_wire(node_id));
    payload.extend_from_slice(&tcp_port.to_le_bytes());
    payload.push(version);
    let tags = [KadTag::uint(TAG_SOURCE_UDP_PORT, udp_port as u64)];
    encode_tags(&mut payload, &tags);
    KadPacket::new(OP_HELLO_REQ, payload)
}

pub fn parse_hello(payload: &[u8]) -> Result<(KadId, u16, u8, Vec<KadTag>), WireError> {
    let mut cursor = Cursor::new(payload);
    let id = cursor.id("hello node id")?;
    let tcp_port = cursor.u16("hello tcp port")?;
    let version = cursor.u8("hello version")?;
    let tags = decode_tags(&mut cursor)?;
    cursor.finish("hello trailing bytes")?;
    Ok((id, tcp_port, version, tags))
}

pub fn build_hello_response(
    node_id: &KadId,
    udp_port: u16,
    tcp_port: u16,
    version: u8,
) -> KadPacket {
    build_hello_request(node_id, udp_port, tcp_port, version).with_opcode(OP_HELLO_RES)
}

impl KadPacket {
    fn with_opcode(mut self, opcode: u8) -> Self {
        self.opcode = opcode;
        self
    }
}

pub fn build_routing_request(kind: u8, target: &KadId, requester: &KadId) -> KadPacket {
    let mut payload = Vec::with_capacity(33);
    payload.push(kind & 0x1f);
    payload.extend_from_slice(&id_to_wire(target));
    payload.extend_from_slice(&id_to_wire(requester));
    KadPacket::new(OP_ROUTING_REQ, payload)
}

pub fn parse_routing_request(payload: &[u8]) -> Result<(u8, KadId, KadId), WireError> {
    let mut cursor = Cursor::new(payload);
    let kind = cursor.u8("routing request kind")? & 0x1f;
    let target = cursor.id("routing request target")?;
    let requester = cursor.id("routing request requester")?;
    cursor.finish("routing request trailing bytes")?;
    Ok((kind, target, requester))
}

pub fn build_routing_response(target: &KadId, contacts: &[KadWireContact]) -> KadPacket {
    let mut payload = Vec::with_capacity(17 + contacts.len() * 25);
    payload.extend_from_slice(&id_to_wire(target));
    let count = u8::try_from(contacts.len().min(32)).unwrap_or(32);
    payload.push(count);
    for contact in contacts.iter().take(usize::from(count)) {
        write_contact(&mut payload, contact);
    }
    KadPacket::new(OP_ROUTING_RES, payload)
}

pub fn parse_routing_response(payload: &[u8]) -> Result<RoutingResponse, WireError> {
    parse_routing_response_with_limit(payload, 32)
}

pub fn parse_routing_response_with_limit(
    payload: &[u8],
    max_contacts: usize,
) -> Result<RoutingResponse, WireError> {
    let mut cursor = Cursor::new(payload);
    let target = cursor.id("routing target")?;
    let count = cursor.u8("routing contact count")? as usize;
    if count > 32 || count > max_contacts {
        return Err(WireError::InvalidCount {
            context: "routing contact",
            count,
        });
    }
    let mut contacts = Vec::with_capacity(count);
    for _ in 0..count {
        contacts.push(read_contact(&mut cursor)?);
    }
    cursor.finish("routing response trailing bytes")?;
    Ok(RoutingResponse { target, contacts })
}

pub fn build_source_search_request(file_hash: &KadId, file_size: u64, start: u16) -> KadPacket {
    let mut payload = Vec::with_capacity(26);
    payload.extend_from_slice(&id_to_wire(file_hash));
    payload.extend_from_slice(&(start & 0x7fff).to_le_bytes());
    payload.extend_from_slice(&file_size.to_le_bytes());
    KadPacket::new(OP_SEARCH_SOURCE_REQ, payload)
}

pub fn parse_source_search_request(payload: &[u8]) -> Result<(KadId, u16, u64), WireError> {
    let mut cursor = Cursor::new(payload);
    let file_hash = cursor.id("source request hash")?;
    let start = cursor.u16("source request start")? & 0x7fff;
    let file_size = cursor.u64("source request file size")?;
    cursor.finish("source request trailing bytes")?;
    Ok((file_hash, start, file_size))
}

pub fn build_source_search_response(target: &KadId, sources: &[KadSourceRecord]) -> KadPacket {
    let mut payload = Vec::with_capacity(18 + sources.len() * 64);
    payload.extend_from_slice(&id_to_wire(target));
    let count = u16::try_from(sources.len().min(300)).unwrap_or(300);
    payload.extend_from_slice(&count.to_le_bytes());
    for source in sources.iter().take(usize::from(count)) {
        payload.extend_from_slice(&id_to_wire(&source.id));
        encode_tags(&mut payload, &source.tags);
    }
    KadPacket::new(OP_SEARCH_RES, payload)
}

pub fn parse_source_search_response(payload: &[u8]) -> Result<SourceSearchResponse, WireError> {
    let mut cursor = Cursor::new(payload);
    let target = cursor.id("source response target")?;
    let count = cursor.u16("source response count")? as usize;
    if count > 300 {
        return Err(WireError::InvalidCount {
            context: "source response",
            count,
        });
    }
    let mut sources = Vec::with_capacity(count);
    for _ in 0..count {
        let id = cursor.id("source id")?;
        let tags = decode_tags(&mut cursor)?;
        sources.push(KadSourceRecord { id, tags });
    }
    cursor.finish("source response trailing bytes")?;
    Ok(SourceSearchResponse { target, sources })
}

pub fn build_ping() -> KadPacket {
    KadPacket::new(OP_PING, Vec::new())
}

pub fn parse_pong(payload: &[u8]) -> Result<u16, WireError> {
    let mut cursor = Cursor::new(payload);
    let observed_port = cursor.u16("pong UDP port")?;
    cursor.finish("pong trailing bytes")?;
    Ok(observed_port)
}

fn read_contact(cursor: &mut Cursor<'_>) -> Result<KadWireContact, WireError> {
    let id = cursor.id("contact id")?;
    let raw_ip = cursor.u32("contact ip")?;
    let ip = Ipv4Addr::from(raw_ip.to_be_bytes());
    let udp_port = cursor.u16("contact udp port")?;
    let tcp_port = cursor.u16("contact tcp port")?;
    let version = cursor.u8("contact version")?;
    Ok(KadWireContact {
        id,
        ip,
        udp_port,
        tcp_port,
        version,
    })
}

fn write_contact(out: &mut Vec<u8>, contact: &KadWireContact) {
    out.extend_from_slice(&id_to_wire(&contact.id));
    out.extend_from_slice(&u32::from_be_bytes(contact.ip.octets()).to_le_bytes());
    out.extend_from_slice(&contact.udp_port.to_le_bytes());
    out.extend_from_slice(&contact.tcp_port.to_le_bytes());
    out.push(contact.version);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: u8) -> KadId {
        [value; 16]
    }

    #[test]
    fn packet_rejects_compressed_and_oversized_datagrams() {
        assert!(KadPacket::decode(&[KAD_PROTOCOL_COMPRESSED, OP_PING, 0x78]).is_err());
        let mut data = vec![KAD_PROTOCOL, OP_PING];
        data.resize(MAX_DATAGRAM_SIZE + 1, 0);
        assert!(matches!(
            KadPacket::decode(&data),
            Err(WireError::TooLarge(_))
        ));
    }

    fn zlib(opcode: u8, data: &[u8]) -> Vec<u8> {
        use std::io::Write;
        let mut enc = flate2::write::ZlibEncoder::new(
            vec![KAD_PROTOCOL_COMPRESSED, opcode],
            flate2::Compression::fast(),
        );
        enc.write_all(data).unwrap();
        enc.finish().unwrap()
    }

    #[test]
    fn compressed_packet_is_inflated_and_bombs_are_rejected() {
        let packet = KadPacket::decode(&zlib(OP_SEARCH_RES, &[0xab; 300])).unwrap();
        assert_eq!(packet.opcode, OP_SEARCH_RES);
        assert_eq!(packet.payload, vec![0xab; 300]);

        let bomb = zlib(OP_SEARCH_RES, &vec![0u8; MAX_INFLATED_SIZE + 10]);
        assert!(bomb.len() < MAX_DATAGRAM_SIZE);
        assert!(matches!(
            KadPacket::decode(&bomb),
            Err(WireError::TooLarge(_))
        ));
        assert!(KadPacket::decode(&zlib(OP_PING, &[]))
            .unwrap()
            .payload
            .is_empty());
    }

    #[test]
    fn source_request_uses_little_endian_fields() {
        let packet = build_source_search_request(&id(0x11), 0x0102_0304_0506_0708, 0x1234);
        assert_eq!(packet.encode()[..2], [KAD_PROTOCOL, OP_SEARCH_SOURCE_REQ]);
        assert_eq!(&packet.payload[16..18], &[0x34, 0x12]);
        assert_eq!(&packet.payload[18..26], &[8, 7, 6, 5, 4, 3, 2, 1]);
        assert_eq!(
            parse_source_search_request(&packet.payload).unwrap(),
            (id(0x11), 0x1234, 0x0102_0304_0506_0708)
        );
    }

    #[test]
    fn kad_id_fields_reverse_each_little_endian_u32_chunk_on_wire() {
        let canonical: KadId = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ];
        let wire = [
            0x04, 0x03, 0x02, 0x01, 0x08, 0x07, 0x06, 0x05, 0x0c, 0x0b, 0x0a, 0x09, 0x10, 0x0f,
            0x0e, 0x0d,
        ];

        let hello = build_hello_request(&canonical, 4672, 4662, 8);
        assert_eq!(&hello.payload[..16], &wire);
        assert_eq!(parse_hello(&hello.payload).unwrap().0, canonical);

        let routing = build_routing_request(0, &canonical, &canonical);
        assert_eq!(&routing.payload[1..17], &wire);
        assert_eq!(&routing.payload[17..33], &wire);
        assert_eq!(
            parse_routing_request(&routing.payload).unwrap(),
            (0, canonical, canonical)
        );

        let source_request = build_source_search_request(&canonical, 42, 0);
        assert_eq!(&source_request.payload[..16], &wire);
        assert_eq!(
            parse_source_search_request(&source_request.payload)
                .unwrap()
                .0,
            canonical
        );

        let mut bootstrap = Vec::new();
        bootstrap.extend_from_slice(&wire);
        bootstrap.extend_from_slice(&4662u16.to_le_bytes());
        bootstrap.push(8);
        bootstrap.extend_from_slice(&0u16.to_le_bytes());
        assert_eq!(
            parse_bootstrap_response(&bootstrap).unwrap().0.id,
            canonical
        );

        let source = KadSourceRecord {
            id: canonical,
            tags: Vec::new(),
        };
        let response = build_source_search_response(&canonical, &[source]);
        assert_eq!(&response.payload[..16], &wire);
        assert_eq!(&response.payload[16..18], &[1, 0]);
        assert_eq!(&response.payload[18..34], &wire);
        let parsed = parse_source_search_response(&response.payload).unwrap();
        assert_eq!(parsed.target, canonical);
        assert_eq!(parsed.sources[0].id, canonical);
    }

    #[test]
    fn routing_contacts_reverse_ids_without_reversing_ipv4_octets() {
        let canonical: KadId = [
            0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80, 0x90, 0xa0, 0xb0, 0xc0, 0xd0, 0xe0,
            0xf0, 0xff,
        ];
        let contact = KadWireContact {
            id: canonical,
            ip: Ipv4Addr::new(1, 2, 3, 4),
            udp_port: 4672,
            tcp_port: 4662,
            version: 8,
        };
        let packet = build_routing_response(&canonical, std::slice::from_ref(&contact));
        assert_eq!(&packet.payload[..16], &id_to_wire(&canonical));
        assert_eq!(&packet.payload[17..33], &id_to_wire(&canonical));
        let parsed = parse_routing_response(&packet.payload).unwrap();
        assert_eq!(parsed.target, canonical);
        assert_eq!(parsed.contacts[0], contact);
        assert_eq!(parsed.contacts[0].ip, Ipv4Addr::new(1, 2, 3, 4));
    }

    #[test]
    fn routing_contact_round_trip() {
        let contact = KadWireContact {
            id: id(7),
            ip: Ipv4Addr::new(1, 2, 3, 4),
            udp_port: 4672,
            tcp_port: 4662,
            version: 8,
        };
        let packet = build_routing_response(&id(9), std::slice::from_ref(&contact));
        let parsed = parse_routing_response(&packet.payload).unwrap();
        assert_eq!(parsed.target, id(9));
        assert_eq!(parsed.contacts, vec![contact]);
    }

    #[test]
    fn kad_contact_ip_uses_a_little_endian_host_order_scalar() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&id(9));
        payload.push(1);
        payload.extend_from_slice(&id(7));
        payload.extend_from_slice(&[4, 3, 2, 1]);
        payload.extend_from_slice(&4672u16.to_le_bytes());
        payload.extend_from_slice(&4662u16.to_le_bytes());
        payload.push(8);
        let parsed = parse_routing_response(&payload).unwrap();
        assert_eq!(parsed.contacts[0].ip, Ipv4Addr::new(1, 2, 3, 4));
    }

    #[test]
    fn bootstrap_and_hello_layouts_round_trip() {
        let contact = KadWireContact {
            id: id(2),
            ip: Ipv4Addr::new(9, 8, 7, 6),
            udp_port: 4672,
            tcp_port: 4662,
            version: 8,
        };
        let mut bootstrap = Vec::new();
        bootstrap.extend_from_slice(&id(1));
        bootstrap.extend_from_slice(&4662u16.to_le_bytes());
        bootstrap.push(8);
        bootstrap.extend_from_slice(&1u16.to_le_bytes());
        write_contact(&mut bootstrap, &contact);
        let (sender, contacts) = parse_bootstrap_response(&bootstrap).unwrap();
        assert_eq!(sender.id, id(1));
        assert_eq!(sender.tcp_port, 4662);
        assert_eq!(contacts, vec![contact]);

        let hello = build_hello_request(&id(3), 4672, 4662, 8);
        assert_eq!(
            &hello.payload[19..],
            &[1, TAGTYPE_UINT16, 1, 0, TAG_SOURCE_UDP_PORT, 0x40, 0x12,]
        );
        let (hello_id, tcp_port, version, tags) = parse_hello(&hello.payload).unwrap();
        assert_eq!(hello_id, id(3));
        assert_eq!((tcp_port, version), (4662, 8));
        assert_eq!(
            tags[0]
                .id_value(TAG_SOURCE_UDP_PORT)
                .and_then(|_| tags[0].get_uint()),
            Some(4672)
        );
    }

    #[test]
    fn source_records_keep_direct_endpoint_tags() {
        let record = KadSourceRecord {
            id: id(7),
            tags: vec![
                KadTag::uint(TAG_SOURCE_TYPE, 1),
                KadTag::uint(TAG_SOURCE_IP, u32::from_be_bytes([1, 2, 3, 4]) as u64),
                KadTag::uint(TAG_SOURCE_PORT, 4662),
            ],
        };
        let packet = build_source_search_response(&id(9), &[record]);
        let parsed = parse_source_search_response(&packet.payload).unwrap();
        assert_eq!(parsed.sources[0].source_type(), Some(1));
        assert_eq!(
            parsed.sources[0].direct_addr(),
            Some(SocketAddrV4::new(Ipv4Addr::new(1, 2, 3, 4), 4662))
        );
    }

    #[test]
    fn decoded_u64_ip_tag_is_not_truncated_to_ipv4() {
        let oversized_ip = (1u64 << 32) | u64::from(u32::from_be_bytes([1, 2, 3, 4]));
        let record = KadSourceRecord {
            id: id(7),
            tags: vec![
                KadTag::uint(TAG_SOURCE_TYPE, 1),
                KadTag::uint(TAG_SOURCE_IP, oversized_ip),
                KadTag::uint(TAG_SOURCE_PORT, 4662),
            ],
        };
        let packet = build_source_search_response(&id(9), &[record]);
        let parsed = parse_source_search_response(&packet.payload).unwrap();
        let source = &parsed.sources[0];

        assert_eq!(source.tags[1].wire_type, Some(TAGTYPE_UINT64));
        assert_eq!(source.tag_uint(TAG_SOURCE_IP), Some(oversized_ip));
        assert_eq!(source.direct_addr(), None);
    }

    #[test]
    fn source_endpoint_tags_reject_boolean_values() {
        let record = KadSourceRecord {
            id: id(7),
            tags: vec![
                KadTag::uint(TAG_SOURCE_TYPE, 1),
                KadTag::uint(TAG_SOURCE_IP, u32::from_be_bytes([1, 2, 3, 4]) as u64),
                KadTag::id(TAG_SOURCE_PORT, KadTagValue::Bool(true)),
            ],
        };

        assert_eq!(record.tags[2].get_uint(), None);
        assert_eq!(record.tcp_port(), None);
        assert_eq!(record.direct_addr(), None);
    }

    #[test]
    fn source_endpoint_tags_reject_duplicates() {
        let record = KadSourceRecord {
            id: id(7),
            tags: vec![
                KadTag::uint(TAG_SOURCE_TYPE, 1),
                KadTag::uint(TAG_SOURCE_TYPE, 1),
                KadTag::uint(TAG_SOURCE_IP, u32::from_be_bytes([1, 2, 3, 4]) as u64),
                KadTag::uint(TAG_SOURCE_PORT, 4662),
            ],
        };

        assert_eq!(record.source_type(), None);
    }

    #[test]
    fn typed_tags_round_trip_and_unknown_type_is_rejected() {
        let tags = vec![
            KadTag::uint(TAG_SOURCE_PORT, 4662),
            KadTag::id(
                TAG_SOURCE_IP,
                KadTagValue::UInt(u32::from_be_bytes([10, 20, 30, 40]) as u64),
            ),
            KadTag::id(0x42, KadTagValue::String("x".into())),
        ];
        let mut payload = vec![tags.len() as u8];
        for tag in &tags {
            encode_tag(&mut payload, tag);
        }
        let mut cursor = Cursor::new(&payload);
        let decoded = decode_tags(&mut cursor).unwrap();
        cursor.finish("test tags").unwrap();
        assert_eq!(decoded.len(), 3);
        assert_eq!(decoded[0].get_uint(), Some(4662));

        let mut numeric = Vec::new();
        encode_tag(&mut numeric, &KadTag::uint(TAG_SOURCE_PORT, 4662));
        assert_eq!(&numeric[..4], &[TAGTYPE_UINT16, 1, 0, TAG_SOURCE_PORT]);

        let bad = vec![1, 0x0f, 1, 0, TAG_SOURCE_PORT];
        let mut cursor = Cursor::new(&bad);
        assert!(matches!(
            decode_tags(&mut cursor),
            Err(WireError::InvalidTagType(_))
        ));
    }

    #[test]
    fn kad_tags_reject_generic_ed2k_compact_names() {
        let compact_ed2k_tag = vec![1, TAGTYPE_UINT16 | 0x80, 1, 0, TAG_SOURCE_PORT, 0x34, 0x12];
        let mut cursor = Cursor::new(&compact_ed2k_tag);

        assert_eq!(
            decode_tags(&mut cursor),
            Err(WireError::InvalidTagType(TAGTYPE_UINT16 | 0x80))
        );
    }

    #[test]
    fn bsob_tags_use_a_one_byte_length_fixture() {
        let tag = KadTag::id(0x42, KadTagValue::Bsob(vec![0xde, 0xad, 0xbe]));
        let mut encoded = Vec::new();
        encode_tag(&mut encoded, &tag);
        assert_eq!(encoded, vec![TAGTYPE_BSOB, 1, 0, 0x42, 3, 0xde, 0xad, 0xbe]);

        let fixture = vec![1, TAGTYPE_BSOB, 1, 0, 0x42, 3, 0xde, 0xad, 0xbe];
        let mut cursor = Cursor::new(&fixture);
        let tags = decode_tags(&mut cursor).unwrap();
        cursor.finish("bsob fixture").unwrap();
        assert_eq!(tags[0].name, tag.name);
        assert_eq!(tags[0].value, tag.value);
        assert_eq!(tags[0].wire_type, Some(TAGTYPE_BSOB));

        let truncated = vec![1, TAGTYPE_BSOB, 1, 0, 0x42, 3, 0xde, 0xad];
        let mut cursor = Cursor::new(&truncated);
        assert!(matches!(
            decode_tags(&mut cursor),
            Err(WireError::InvalidValue("bsob length"))
        ));
    }

    #[test]
    fn find_value_response_limit_rejects_extra_contacts() {
        let contacts = (1..=3)
            .map(|index| KadWireContact {
                id: id(index),
                ip: Ipv4Addr::new(8, 8, 8, index),
                udp_port: 4672,
                tcp_port: 4662,
                version: 8,
            })
            .collect::<Vec<_>>();
        let packet = build_routing_response(&id(9), &contacts);

        assert_eq!(
            parse_routing_response(&packet.payload)
                .unwrap()
                .contacts
                .len(),
            3
        );
        assert!(matches!(
            parse_routing_response_with_limit(&packet.payload, 2),
            Err(WireError::InvalidCount {
                context: "routing contact",
                count: 3,
            })
        ));
    }

    #[test]
    fn compressed_string_tag_types_after_str22_are_rejected() {
        let invalid_compact_string = vec![1, 0x27, 1, 0, TAG_SOURCE_PORT];
        let mut cursor = Cursor::new(&invalid_compact_string);

        assert_eq!(
            decode_tags(&mut cursor),
            Err(WireError::InvalidTagType(0x27))
        );
    }

    #[test]
    fn bool_array_consumes_emule_sentinel_byte_before_following_tags() {
        let tags = vec![
            KadTag {
                name: KadTagName::Id(0x40),
                value: KadTagValue::BoolArray(vec![
                    true, false, true, false, true, false, true, false,
                ]),
                wire_type: None,
            },
            KadTag::uint(TAG_SOURCE_PORT, 4662),
        ];
        let mut payload = Vec::new();
        encode_tags(&mut payload, &tags);
        let mut cursor = Cursor::new(&payload);
        let decoded = decode_tags(&mut cursor).unwrap();
        cursor.finish("bool array fixture").unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[1].get_uint(), Some(4662));
    }

    #[test]
    fn source_response_rejects_trailing_and_truncated_records() {
        let response = build_source_search_response(&id(2), &[]);
        assert!(parse_source_search_response(&response.payload).is_ok());
        let mut truncated = response.payload.clone();
        truncated.pop();
        assert!(parse_source_search_response(&truncated).is_err());
        let mut trailing = response.payload;
        trailing.push(1);
        assert!(parse_source_search_response(&trailing).is_err());
    }

    #[test]
    fn pong_requires_its_observed_udp_port() {
        assert_eq!(parse_pong(&4672u16.to_le_bytes()).unwrap(), 4672);
        assert!(parse_pong(&[]).is_err());
        assert!(parse_pong(&[1]).is_err());
        assert!(parse_pong(&[0, 0, 1]).is_err());
    }
}
