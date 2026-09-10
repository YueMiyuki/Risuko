//! BitTorrent handshake (BEP-3), 68 bytes: 1 len + 19 "BitTorrent protocol" + 8 reserved (BEP-10 ext, BEP-5 DHT) + 20 info-hash + 20 peer-id

use super::super::core::Id20;

pub const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";
pub const HANDSHAKE_LEN: usize = 1 + 19 + 8 + 20 + 20;

/// Reserved bit flags we care about; byte indices are 0-based from the start of the 8-byte reserved area
pub mod reserved {
    /// Bit 20 (byte 5, bit 0x10) — BEP-10 Extension Protocol
    pub const EXT_PROTOCOL: (usize, u8) = (5, 0x10);
    /// Bit 3 of byte 7 (0x08) — BEP 52 v2 capable; matches libtorrent for interop; bit not yet ratified, flip this constant if it shifts
    pub const V2: (usize, u8) = (7, 0x08);
    pub const DHT: (usize, u8) = (7, 0x01);
    pub const FAST: (usize, u8) = (7, 0x04);
}

#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    #[error("bad protocol string length {0}")]
    BadPstrLen(u8),
    #[error("bad protocol string")]
    BadProtocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Handshake {
    pub reserved: [u8; 8],
    pub info_hash: Id20,
    pub peer_id: Id20,
}

impl Handshake {
    pub fn new_with_v2(info_hash: Id20, peer_id: Id20, advertise_v2: bool) -> Self {
        Self::new_with_features(info_hash, peer_id, advertise_v2, true, true)
    }

    pub fn new_with_features(
        info_hash: Id20,
        peer_id: Id20,
        advertise_v2: bool,
        advertise_dht: bool,
        advertise_fast: bool,
    ) -> Self {
        let mut reserved = [0u8; 8];
        let (b, m) = reserved::EXT_PROTOCOL;
        reserved[b] |= m;
        if advertise_dht {
            let (b, m) = reserved::DHT;
            reserved[b] |= m;
        }
        if advertise_fast {
            let (b, m) = reserved::FAST;
            reserved[b] |= m;
        }
        if advertise_v2 {
            let (b, m) = reserved::V2;
            reserved[b] |= m;
        }
        Self {
            reserved,
            info_hash,
            peer_id,
        }
    }

    pub fn has_ext_protocol(&self) -> bool {
        let (b, m) = reserved::EXT_PROTOCOL;
        self.reserved[b] & m != 0
    }

    /// True if the peer advertises BEP 52 (BitTorrent v2) capability
    pub fn has_v2(&self) -> bool {
        let (b, m) = reserved::V2;
        self.reserved[b] & m != 0
    }

    pub fn to_bytes(&self) -> [u8; HANDSHAKE_LEN] {
        let mut out = [0u8; HANDSHAKE_LEN];
        out[0] = 19;
        out[1..20].copy_from_slice(PROTOCOL);
        out[20..28].copy_from_slice(&self.reserved);
        out[28..48].copy_from_slice(self.info_hash.as_bytes());
        out[48..68].copy_from_slice(self.peer_id.as_bytes());
        out
    }

    pub fn parse(buf: &[u8; HANDSHAKE_LEN]) -> Result<Self, HandshakeError> {
        if buf[0] != 19 {
            return Err(HandshakeError::BadPstrLen(buf[0]));
        }
        if &buf[1..20] != PROTOCOL {
            return Err(HandshakeError::BadProtocol);
        }
        let mut reserved = [0u8; 8];
        reserved.copy_from_slice(&buf[20..28]);
        let info_hash = Id20::from_slice(&buf[28..48]).unwrap();
        let peer_id = Id20::from_slice(&buf[48..68]).unwrap();
        Ok(Self {
            reserved,
            info_hash,
            peer_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_advertises_ext_and_optionally_v2() {
        let hs = Handshake::new_with_v2(Id20([0xaau8; 20]), Id20([0xbbu8; 20]), true);
        let bytes = hs.to_bytes();
        let parsed = Handshake::parse(&bytes).unwrap();
        assert_eq!(hs, parsed);
        assert!(parsed.has_ext_protocol());
        // `advertise_v2 = true` carries the v2 capability bit
        assert!(parsed.has_v2());
    }

    #[test]
    fn dht_reserved_bit_is_set() {
        let hs = Handshake::new_with_v2(Id20([0u8; 20]), Id20([1u8; 20]), false);
        let (b, m) = reserved::DHT;
        assert_ne!(hs.reserved[b] & m, 0, "BEP-5 DHT bit must be advertised");
    }

    #[test]
    fn fast_reserved_bit_is_set() {
        let hs = Handshake::new_with_v2(Id20([0u8; 20]), Id20([1u8; 20]), false);
        let (b, m) = reserved::FAST;
        assert_ne!(hs.reserved[b] & m, 0, "BEP 6 fast bit must be advertised");
    }

    #[test]
    fn v2_reserved_bit_is_caller_controlled() {
        let hs_off = Handshake::new_with_v2(Id20([0xaau8; 20]), Id20([0xbbu8; 20]), false);
        let hs_on = Handshake::new_with_v2(Id20([0xaau8; 20]), Id20([0xbbu8; 20]), true);
        assert!(hs_off.has_ext_protocol());
        assert!(!hs_off.has_v2());
        assert!(hs_on.has_v2());
    }

    #[test]
    fn feature_flags_can_disable_dht_for_private_torrents() {
        let hs =
            Handshake::new_with_features(Id20([0xaau8; 20]), Id20([0xbbu8; 20]), true, false, true);
        assert!(hs.has_ext_protocol());
        assert!(hs.has_v2());
        assert_eq!(hs.reserved[reserved::DHT.0] & reserved::DHT.1, 0);
        assert_ne!(hs.reserved[reserved::FAST.0] & reserved::FAST.1, 0);
    }

    #[test]
    fn rejects_bad_protocol() {
        let mut bytes = [0u8; HANDSHAKE_LEN];
        bytes[0] = 19;
        bytes[1..20].copy_from_slice(b"NotBitTorrentProto!");
        assert!(matches!(
            Handshake::parse(&bytes),
            Err(HandshakeError::BadProtocol)
        ));
    }

    #[test]
    fn rejects_bad_pstrlen() {
        let mut bytes = [0u8; HANDSHAKE_LEN];
        bytes[0] = 42;
        assert!(matches!(
            Handshake::parse(&bytes),
            Err(HandshakeError::BadPstrLen(42))
        ));
    }
}
