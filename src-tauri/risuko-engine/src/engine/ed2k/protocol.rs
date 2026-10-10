use super::types::*;
use bytes::{Buf, BytesMut};
use std::io;

pub const MAX_FRAME_LEN: usize = 2_000_000;

#[derive(Debug, Clone)]
pub struct Ed2kPacket {
    pub protocol: u8,
    pub opcode: u8,
    pub payload: Vec<u8>,
}

impl Ed2kPacket {
    pub fn new(protocol: u8, opcode: u8, payload: Vec<u8>) -> Self {
        Self {
            protocol,
            opcode,
            payload,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let data_len = 1 + self.payload.len();
        let mut buf = Vec::with_capacity(5 + data_len);
        buf.push(self.protocol);
        buf.extend_from_slice(&(data_len as u32).to_le_bytes());
        buf.push(self.opcode);
        buf.extend_from_slice(&self.payload);
        buf
    }

    pub fn decode(buf: &mut BytesMut) -> Result<Option<Self>, io::Error> {
        if buf.len() < 5 {
            return Ok(None);
        }

        let protocol = buf[0];
        if !matches!(protocol, PROTO_EDONKEY | PROTO_EMULE | PROTO_PACKED) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Unknown ed2k protocol byte",
            ));
        }
        let data_len = u32::from_le_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;

        if data_len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Zero-length ed2k packet",
            ));
        }
        if data_len > MAX_FRAME_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Oversized ed2k packet",
            ));
        }

        let total_len = 5 + data_len;
        if buf.len() < total_len {
            return Ok(None);
        }

        buf.advance(5);
        let opcode = buf[0];
        buf.advance(1);

        let payload_len = data_len - 1;
        let payload = buf.split_to(payload_len).to_vec();

        Ok(Some(Self {
            protocol,
            opcode,
            payload,
        }))
    }
}

pub fn build_hello_server(
    client_hash: &[u8; 16],
    client_port: u16,
    kad_udp_port: Option<u16>,
) -> Ed2kPacket {
    let mut payload = Vec::with_capacity(64);
    payload.extend_from_slice(client_hash);
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(&client_port.to_le_bytes());

    let mut tags = vec![
        MetaTag::string(TAG_NAME, "Risuko"),
        MetaTag::u32(TAG_VERSION, 0x3c),
        MetaTag::u32(TAG_PORT, client_port as u32),
    ];
    if let Some(port) = kad_udp_port {
        tags.push(MetaTag::u32(TAG_EMULE_UDP_PORT, port as u32));
    }

    let tag_count = tags.len() as u32;
    payload.extend_from_slice(&tag_count.to_le_bytes());
    for tag in &tags {
        tag.encode(&mut payload);
    }

    Ed2kPacket::new(PROTO_EDONKEY, OP_HELLO_SERVER, payload)
}

pub fn build_get_sources(file_hash: &[u8; 16]) -> Ed2kPacket {
    Ed2kPacket::new(PROTO_EDONKEY, OP_GET_SOURCES, file_hash.to_vec())
}

pub fn build_offer_files_empty() -> Ed2kPacket {
    let mut payload = Vec::new();
    payload.extend_from_slice(&0u32.to_le_bytes());
    Ed2kPacket::new(PROTO_EDONKEY, OP_OFFER_FILES, payload)
}

pub fn parse_id_change(payload: &[u8]) -> Result<u32, String> {
    if payload.len() < 4 {
        return Err("ID Change packet too short".to_string());
    }
    Ok(u32::from_le_bytes([
        payload[0], payload[1], payload[2], payload[3],
    ]))
}

pub fn parse_server_status(payload: &[u8]) -> Result<(u32, u32), String> {
    if payload.len() < 8 {
        return Err("Server Status packet too short".to_string());
    }
    let users = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
    let files = u32::from_le_bytes([payload[4], payload[5], payload[6], payload[7]]);
    Ok((users, files))
}

pub fn parse_server_message(payload: &[u8]) -> Result<String, String> {
    if payload.len() < 2 {
        return Err("Server Message packet too short".to_string());
    }
    let len = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    if payload.len() < 2 + len {
        return Err("Server Message truncated".to_string());
    }
    String::from_utf8(payload[2..2 + len].to_vec())
        .map_err(|_| "Server Message contains invalid UTF-8".to_string())
}

pub fn parse_found_sources(payload: &[u8]) -> Result<([u8; 16], Vec<(u32, u16)>), String> {
    if payload.len() < 17 {
        return Err("Found Sources packet too short".to_string());
    }

    let mut hash = [0u8; 16];
    hash.copy_from_slice(&payload[0..16]);

    let count = payload[16] as usize;
    let expected_len = 17 + count * 6;
    if payload.len() < expected_len {
        return Err("Found Sources truncated".to_string());
    }

    let mut sources = Vec::with_capacity(count);
    let mut offset = 17;
    for _ in 0..count {
        let ip = u32::from_le_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]);
        let port = u16::from_le_bytes([payload[offset + 4], payload[offset + 5]]);
        sources.push((ip, port));
        offset += 6;
    }

    Ok((hash, sources))
}

pub fn parse_hashset_answer(payload: &[u8]) -> Result<([u8; 16], Vec<[u8; 16]>), String> {
    if payload.len() < 18 {
        return Err("Hashset Answer too short".to_string());
    }

    let mut file_hash = [0u8; 16];
    file_hash.copy_from_slice(&payload[0..16]);

    let count = u16::from_le_bytes([payload[16], payload[17]]) as usize;
    let expected = 18 + count * 16;
    if payload.len() < expected {
        return Err("Hashset Answer truncated".to_string());
    }

    let mut hashes = Vec::with_capacity(count);
    let mut offset = 18;
    for _ in 0..count {
        let mut h = [0u8; 16];
        h.copy_from_slice(&payload[offset..offset + 16]);
        hashes.push(h);
        offset += 16;
    }

    Ok((file_hash, hashes))
}

pub fn parse_file_status(payload: &[u8]) -> Result<([u8; 16], Vec<bool>), String> {
    if payload.len() < 18 {
        return Err("File Status too short".to_string());
    }

    let mut file_hash = [0u8; 16];
    file_hash.copy_from_slice(&payload[0..16]);

    let part_count = u16::from_le_bytes([payload[16], payload[17]]) as usize;
    let byte_count = part_count.div_ceil(8);
    if payload.len() < 18 + byte_count {
        return Err("File Status bitmap truncated".to_string());
    }

    let mut parts = Vec::with_capacity(part_count);
    for i in 0..part_count {
        let byte_idx = i / 8;
        let bit_idx = i % 8;
        let has_part = (payload[18 + byte_idx] >> bit_idx) & 1 == 1;
        parts.push(has_part);
    }

    Ok((file_hash, parts))
}

pub fn parse_sending_part_header(payload: &[u8]) -> Result<([u8; 16], u32, u32), String> {
    if payload.len() < 24 {
        return Err("Sending Part header too short".to_string());
    }

    let mut file_hash = [0u8; 16];
    file_hash.copy_from_slice(&payload[0..16]);

    let start = u32::from_le_bytes([payload[16], payload[17], payload[18], payload[19]]);
    let end = u32::from_le_bytes([payload[20], payload[21], payload[22], payload[23]]);

    Ok((file_hash, start, end))
}

#[derive(Debug, Clone)]
pub enum MetaTag {
    String { name_id: u8, value: String },
    U32 { name_id: u8, value: u32 },
}

impl MetaTag {
    pub fn string(name_id: u8, value: &str) -> Self {
        Self::String {
            name_id,
            value: value.to_string(),
        }
    }

    pub fn u32(name_id: u8, value: u32) -> Self {
        Self::U32 { name_id, value }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Self::String { name_id, value } => {
                buf.push(0x02);
                buf.extend_from_slice(&1u16.to_le_bytes());
                buf.push(*name_id);
                let bytes = value.as_bytes();
                buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
                buf.extend_from_slice(bytes);
            }
            Self::U32 { name_id, value } => {
                buf.push(0x03);
                buf.extend_from_slice(&1u16.to_le_bytes());
                buf.push(*name_id);
                buf.extend_from_slice(&value.to_le_bytes());
            }
        }
    }
}

pub fn build_hello_client(
    client_hash: &[u8; 16],
    client_id: u32,
    client_port: u16,
    server_ip: u32,
    server_port: u16,
) -> Ed2kPacket {
    let mut payload = Vec::with_capacity(64);
    payload.push(0x10);
    payload.extend_from_slice(client_hash);
    payload.extend_from_slice(&client_id.to_le_bytes());
    payload.extend_from_slice(&client_port.to_le_bytes());

    let tags = vec![
        MetaTag::string(TAG_NAME, "Risuko"),
        MetaTag::u32(TAG_VERSION, 0x3c),
        MetaTag::u32(TAG_PORT, client_port as u32),
        MetaTag::u32(CT_EMULE_VERSION, OUR_EMULE_VERSION),
        MetaTag::u32(CT_EMULE_MISCOPTIONS1, OUR_MISC_OPTIONS1),
        MetaTag::u32(CT_EMULE_MISCOPTIONS2, OUR_MISC_OPTIONS2),
    ];

    let tag_count = tags.len() as u32;
    payload.extend_from_slice(&tag_count.to_le_bytes());
    for tag in &tags {
        tag.encode(&mut payload);
    }

    payload.extend_from_slice(&server_ip.to_le_bytes());
    payload.extend_from_slice(&server_port.to_le_bytes());

    Ed2kPacket::new(PROTO_EDONKEY, OP_HELLO_CLIENT, payload)
}

pub fn build_file_request(file_hash: &[u8; 16]) -> Ed2kPacket {
    Ed2kPacket::new(PROTO_EDONKEY, OP_FILE_REQUEST, file_hash.to_vec())
}

pub fn build_file_status_request(file_hash: &[u8; 16]) -> Ed2kPacket {
    Ed2kPacket::new(PROTO_EDONKEY, OP_FILE_STATUS_REQUEST, file_hash.to_vec())
}

pub fn build_hashset_request(file_hash: &[u8; 16]) -> Ed2kPacket {
    Ed2kPacket::new(PROTO_EDONKEY, OP_HASHSET_REQUEST, file_hash.to_vec())
}

pub fn build_slot_request(file_hash: &[u8; 16]) -> Ed2kPacket {
    Ed2kPacket::new(PROTO_EDONKEY, OP_SLOT_REQUEST, file_hash.to_vec())
}

pub fn build_request_parts(
    file_hash: &[u8; 16],
    ranges: &[(u64, u64)],
    large: bool,
) -> Result<Ed2kPacket, String> {
    if ranges.len() > 3 {
        return Err("at most 3 ranges per request".to_string());
    }
    let width = if large { 8 } else { 4 };
    let mut payload = Vec::with_capacity(16 + 6 * width);
    payload.extend_from_slice(file_hash);

    for field in [0usize, 1] {
        for i in 0..3 {
            let r = ranges.get(i).copied().unwrap_or((0, 0));
            let v = if field == 0 { r.0 } else { r.1 };
            if large {
                payload.extend_from_slice(&v.to_le_bytes());
            } else {
                let v = u32::try_from(v)
                    .map_err(|_| "range needs a peer with large-file support".to_string())?;
                payload.extend_from_slice(&v.to_le_bytes());
            }
        }
    }

    Ok(if large {
        Ed2kPacket::new(PROTO_EMULE, OP_REQUEST_PARTS_I64, payload)
    } else {
        Ed2kPacket::new(PROTO_EDONKEY, OP_REQUEST_PARTS, payload)
    })
}

pub fn parse_sending_part_header_i64(payload: &[u8]) -> Result<([u8; 16], u64, u64), String> {
    if payload.len() < 32 {
        return Err("Sending Part I64 header too short".to_string());
    }
    let mut file_hash = [0u8; 16];
    file_hash.copy_from_slice(&payload[0..16]);
    let mut word = [0u8; 8];
    word.copy_from_slice(&payload[16..24]);
    let start = u64::from_le_bytes(word);
    word.copy_from_slice(&payload[24..32]);
    let end = u64::from_le_bytes(word);
    Ok((file_hash, start, end))
}

pub const OUR_EMULE_VERSION: u32 = (0xff << 24) | (50 << 10);
pub const OUR_MISC_OPTIONS1: u32 = 0;
pub const OUR_MISC_OPTIONS2: u32 = MISC2_LARGE_FILES;

pub fn new_user_hash() -> [u8; 16] {
    let mut hash: [u8; 16] = rand::random();
    hash[5] = 14;
    hash[14] = 111;
    hash
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerCaps {
    pub emule_version: Option<u32>,
    pub misc_options1: u32,
    pub misc_options2: u32,
}

impl PeerCaps {
    pub fn large_files(&self) -> bool {
        self.misc_options2 & MISC2_LARGE_FILES != 0
    }
}

pub fn parse_hello_answer_caps(payload: &[u8]) -> PeerCaps {
    let mut caps = PeerCaps::default();
    let Some(rest) = payload.get(22..) else {
        return caps;
    };
    let Some((count, mut rest)) = rest.split_first_chunk::<4>() else {
        return caps;
    };
    for _ in 0..u32::from_le_bytes(*count) {
        let Some((id, value, tail)) = read_tag(rest) else {
            break;
        };
        rest = tail;
        match (id, value) {
            (Some(CT_EMULE_VERSION), Some(v)) => caps.emule_version = Some(v),
            (Some(CT_EMULE_MISCOPTIONS1), Some(v)) => caps.misc_options1 = v,
            (Some(CT_EMULE_MISCOPTIONS2), Some(v)) => caps.misc_options2 = v,
            _ => {}
        }
    }
    caps
}

type TagRead<'a> = (Option<u8>, Option<u32>, &'a [u8]);

fn read_tag(buf: &[u8]) -> Option<TagRead<'_>> {
    let (&ty, buf) = buf.split_first()?;
    let (id, buf) = if ty & 0x80 != 0 {
        let (&id, buf) = buf.split_first()?;
        (Some(id), buf)
    } else {
        let (len, buf) = buf.split_first_chunk::<2>()?;
        let len = u16::from_le_bytes(*len) as usize;
        let name = buf.get(..len)?;
        let id = (len == 1).then(|| name[0]);
        (id, &buf[len..])
    };
    let take = |n: usize| -> Option<(&[u8], &[u8])> { (buf.len() >= n).then(|| buf.split_at(n)) };
    let int = |n: usize| -> Option<(Option<u32>, &[u8])> {
        let (b, tail) = take(n)?;
        let mut w = [0u8; 8];
        w[..n].copy_from_slice(b);
        Some((u32::try_from(u64::from_le_bytes(w)).ok(), tail))
    };
    let (value, tail) = match ty & 0x7f {
        0x01 => (None, take(16)?.1),
        0x02 => {
            let (l, t) = take(2)?;
            let len = u16::from_le_bytes([l[0], l[1]]) as usize;
            (None, t.get(len..)?)
        }
        0x03 => int(4)?,
        0x04 => (None, take(4)?.1),
        0x05 | 0x09 => int(1)?,
        0x06 => {
            let (l, t) = take(2)?;
            let bits = u16::from_le_bytes([l[0], l[1]]) as usize;
            (None, t.get(bits.div_ceil(8)..)?)
        }
        0x07 => {
            let (l, t) = take(4)?;
            let len = u32::from_le_bytes([l[0], l[1], l[2], l[3]]) as usize;
            (None, t.get(len..)?)
        }
        0x08 => int(2)?,
        0x0a => {
            let (l, t) = take(1)?;
            (None, t.get(l[0] as usize..)?)
        }
        0x0b => int(8)?,
        t @ 0x11..=0x20 => (None, take((t - 0x10) as usize)?.1),
        _ => return None,
    };
    Some((id, value, tail))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_rejects_oversized_and_unknown_frames() {
        let mut oversized = BytesMut::from(&[PROTO_EDONKEY, 0xff, 0xff, 0xff, 0xff][..]);
        assert!(Ed2kPacket::decode(&mut oversized).is_err());
        let mut http = BytesMut::from(&b"HTTP/1.1 200 OK"[..]);
        assert!(Ed2kPacket::decode(&mut http).is_err());
        let mut ok = BytesMut::from(&Ed2kPacket::new(PROTO_EDONKEY, 0x42, vec![1, 2]).encode()[..]);
        let packet = Ed2kPacket::decode(&mut ok).unwrap().unwrap();
        assert_eq!((packet.opcode, packet.payload), (0x42, vec![1, 2]));
    }

    #[test]
    fn server_hello_advertises_kad_udp_port_only_when_kad_is_running() {
        let without_kad = build_hello_server(&[0; 16], 4662, None);
        let with_kad = build_hello_server(&[0; 16], 4662, Some(4672));
        let kad_tag = [0x03, 0x01, 0x00, TAG_EMULE_UDP_PORT, 0x40, 0x12, 0x00, 0x00];

        assert_eq!(
            u32::from_le_bytes(without_kad.payload[22..26].try_into().unwrap()),
            3
        );
        assert_eq!(
            u32::from_le_bytes(with_kad.payload[22..26].try_into().unwrap()),
            4
        );
        assert!(!without_kad
            .payload
            .windows(kad_tag.len())
            .any(|window| window == kad_tag));
        assert!(with_kad
            .payload
            .windows(kad_tag.len())
            .any(|window| window == kad_tag));
    }

    const BIG: u64 = 6_000_000_000;

    #[test]
    fn request_parts_uses_32_bit_form_for_plain_peers() {
        let p = build_request_parts(&[9; 16], &[(1, 2), (3, 4)], false).unwrap();
        assert_eq!(
            (p.protocol, p.opcode, p.payload.len()),
            (PROTO_EDONKEY, OP_REQUEST_PARTS, 40)
        );
        assert_eq!(&p.payload[16..20], &1u32.to_le_bytes());
        assert_eq!(&p.payload[28..32], &2u32.to_le_bytes());
        assert!(build_request_parts(&[9; 16], &[(BIG, BIG + 1)], false).is_err());
    }

    #[test]
    fn request_parts_i64_encodes_wide_offsets() {
        let p = build_request_parts(&[9; 16], &[(BIG, BIG + 184_320), (7, 8)], true).unwrap();
        assert_eq!(
            (p.protocol, p.opcode, p.payload.len()),
            (PROTO_EMULE, OP_REQUEST_PARTS_I64, 64)
        );
        assert_eq!(&p.payload[16..24], &BIG.to_le_bytes());
        assert_eq!(&p.payload[24..32], &7u64.to_le_bytes());
        assert_eq!(&p.payload[40..48], &(BIG + 184_320).to_le_bytes());
        assert!(build_request_parts(&[9; 16], &[(0, 1); 4], true).is_err());
        let mut buf = BytesMut::from(&p.encode()[..]);
        let back = Ed2kPacket::decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            (back.opcode, back.payload),
            (OP_REQUEST_PARTS_I64, p.payload)
        );
    }

    #[test]
    fn sending_part_i64_header_roundtrip() {
        let mut payload = vec![5u8; 16];
        payload.extend_from_slice(&BIG.to_le_bytes());
        payload.extend_from_slice(&(BIG + 10).to_le_bytes());
        assert_eq!(
            parse_sending_part_header_i64(&payload).unwrap(),
            ([5; 16], BIG, BIG + 10)
        );
        assert!(parse_sending_part_header_i64(&payload[..31]).is_err());
    }

    fn answer_with(tags: &[MetaTag]) -> Vec<u8> {
        let mut payload = vec![0u8; 22];
        payload.extend_from_slice(&(tags.len() as u32).to_le_bytes());
        for t in tags {
            t.encode(&mut payload);
        }
        payload
    }

    #[test]
    fn hello_advertises_only_large_file_support() {
        let hello = build_hello_client(&[0; 16], 0, 4662, 0, 0);
        let caps = parse_hello_answer_caps(&hello.payload[1..]);
        assert!(caps.large_files());
        assert_eq!(caps.misc_options1, 0);
        assert_eq!(caps.misc_options2, MISC2_LARGE_FILES);
        assert_eq!(caps.emule_version, Some(OUR_EMULE_VERSION));
    }

    #[test]
    fn caps_parse_from_answer_tags() {
        let payload = answer_with(&[
            MetaTag::string(TAG_NAME, "peer"),
            MetaTag::u32(CT_EMULE_MISCOPTIONS1, 0x1234),
            MetaTag::u32(CT_EMULE_MISCOPTIONS2, 0x10 | 0x3),
            MetaTag::u32(CT_EMULE_VERSION, 0x4200),
        ]);
        let caps = parse_hello_answer_caps(&payload);
        assert_eq!(
            caps,
            PeerCaps {
                emule_version: Some(0x4200),
                misc_options1: 0x1234,
                misc_options2: 0x13
            }
        );
        let without =
            parse_hello_answer_caps(&answer_with(&[MetaTag::u32(CT_EMULE_MISCOPTIONS2, 3)]));
        assert!(!without.large_files());
    }

    #[test]
    fn caps_parse_tolerates_short_names_and_garbage() {
        let mut payload = vec![0u8; 22];
        payload.extend_from_slice(&2u32.to_le_bytes());
        payload.extend_from_slice(&[0x12, 0x07, b'x']);
        payload.extend_from_slice(&[0x83, CT_EMULE_MISCOPTIONS2]);
        payload.extend_from_slice(&MISC2_LARGE_FILES.to_le_bytes());
        assert!(!parse_hello_answer_caps(&payload).large_files());

        let mut ok = vec![0u8; 22];
        ok.extend_from_slice(&2u32.to_le_bytes());
        ok.extend_from_slice(&[0x91, 0x07, b'x']);
        ok.extend_from_slice(&[0x83, CT_EMULE_MISCOPTIONS2]);
        ok.extend_from_slice(&MISC2_LARGE_FILES.to_le_bytes());
        assert!(parse_hello_answer_caps(&ok).large_files());
        assert_eq!(parse_hello_answer_caps(&[1, 2, 3]), PeerCaps::default());
        let mut bad = vec![0u8; 22];
        bad.extend_from_slice(&1u32.to_le_bytes());
        bad.extend_from_slice(&[0xff, 0x01]);
        assert_eq!(parse_hello_answer_caps(&bad), PeerCaps::default());
    }

    #[test]
    fn user_hash_carries_emule_markers() {
        for _ in 0..8 {
            let h = new_user_hash();
            assert_eq!((h[5], h[14]), (14, 111));
        }
    }
}
