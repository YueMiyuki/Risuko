use std::fmt;
use std::str::FromStr;

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Id20(pub [u8; 20]);

impl Id20 {
    pub const LEN: usize = 20;

    pub fn new(bytes: [u8; 20]) -> Self {
        Self(bytes)
    }

    pub fn from_slice(b: &[u8]) -> Result<Self, HashParseError> {
        if b.len() != 20 {
            return Err(HashParseError::BadLength(b.len()));
        }
        let mut v = [0u8; 20];
        v.copy_from_slice(b);
        Ok(Self(v))
    }

    pub fn as_bytes(&self) -> &[u8; 20] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn distance(&self, other: &Self) -> Self {
        let mut d = [0u8; 20];
        for (i, slot) in d.iter_mut().enumerate() {
            *slot = self.0[i] ^ other.0[i];
        }
        Self(d)
    }
}

impl fmt::Debug for Id20 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Id20({})", self.to_hex())
    }
}

impl fmt::Display for Id20 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum HashParseError {
    #[error("expected 20 bytes, got {0}")]
    BadLength(usize),
    #[error("hex decode failed")]
    BadHex,
    #[error("base32 decode failed")]
    BadBase32,
}

impl FromStr for Id20 {
    type Err = HashParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.len() {
            40 => {
                let raw = hex::decode(s).map_err(|_| HashParseError::BadHex)?;
                Self::from_slice(&raw)
            }
            32 => {
                let decoded = base32_decode_upper(s.as_bytes()).ok_or(HashParseError::BadBase32)?;
                Self::from_slice(&decoded)
            }
            n => Err(HashParseError::BadLength(n)),
        }
    }
}

fn base32_decode_upper(input: &[u8]) -> Option<Vec<u8>> {
    const ALPHA: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let input: Vec<u8> = input
        .iter()
        .copied()
        .filter(|b| *b != b'=')
        .map(|b| b.to_ascii_uppercase())
        .collect();

    let mut out = Vec::with_capacity(input.len() * 5 / 8);
    let mut buf: u32 = 0;
    let mut bits: u32 = 0;
    for c in input {
        let v = ALPHA.iter().position(|&a| a == c)? as u32;
        buf = (buf << 5) | v;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push(((buf >> bits) & 0xFF) as u8);
        }
    }
    Some(out)
}

pub fn sha1(data: &[u8]) -> Id20 {
    use sha1::Digest;
    let mut h = sha1::Sha1::new();
    h.update(data);
    let out = h.finalize();
    Id20(out.into())
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Id32(pub [u8; 32]);

impl Id32 {
    pub fn from_slice(b: &[u8]) -> Result<Self, HashParseError> {
        if b.len() != 32 {
            return Err(HashParseError::BadLength(b.len()));
        }
        let mut v = [0u8; 32];
        v.copy_from_slice(b);
        Ok(Self(v))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn truncate_to_id20(&self) -> Id20 {
        let mut out = [0u8; 20];
        out.copy_from_slice(&self.0[..20]);
        Id20(out)
    }
}

impl fmt::Debug for Id32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Id32({})", self.to_hex())
    }
}

impl fmt::Display for Id32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl FromStr for Id32 {
    type Err = HashParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.len() {
            64 => {
                let raw = hex::decode(s).map_err(|_| HashParseError::BadHex)?;
                Self::from_slice(&raw)
            }
            n => Err(HashParseError::BadLength(n)),
        }
    }
}

pub fn sha256(data: &[u8]) -> Id32 {
    use sha2::Digest;
    let mut h = sha2::Sha256::new();
    h.update(data);
    let out = h.finalize();
    Id32(out.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trip() {
        let id = Id20([0xabu8; 20]);
        let hex = id.to_hex();
        assert_eq!(hex.len(), 40);
        let parsed: Id20 = hex.parse().unwrap();
        assert_eq!(parsed, id);
    }

    #[test]
    fn base32_parse_known() {
        let encoded = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
        let parsed: Id20 = encoded.parse().unwrap();
        assert_eq!(parsed.0, [0u8; 20]);
    }

    #[test]
    fn xor_distance() {
        let a = Id20([0xffu8; 20]);
        let b = Id20([0x0fu8; 20]);
        let d = a.distance(&b);
        assert_eq!(d.0, [0xf0u8; 20]);
    }

    #[test]
    fn sha1_matches_known() {
        let empty = sha1(b"");
        assert_eq!(empty.to_hex(), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn sha256_matches_known() {
        let empty = sha256(b"");
        assert_eq!(
            empty.to_hex(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn id32_truncate() {
        let mut raw = [0u8; 32];
        for (i, b) in raw.iter_mut().enumerate() {
            *b = i as u8;
        }
        let id = Id32(raw);
        let trunc = id.truncate_to_id20();
        assert_eq!(trunc.0[..], raw[..20]);
    }

    #[test]
    fn id32_hex_round_trip() {
        let id = Id32([0x5au8; 32]);
        let parsed: Id32 = id.to_hex().parse().unwrap();
        assert_eq!(parsed, id);
    }
}
