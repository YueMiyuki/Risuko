//! Peer id generation (BEP 20): Azureus-style `-RS<version>-` plus 12 random bytes

use rand::Rng;

use super::hash::Id20;

/// `-RS`, three version characters from `CARGO_PKG_VERSION`, `0-`
pub const PEER_ID_PREFIX: [u8; 8] = [
    b'-',
    b'R',
    b'S',
    version_char(parse_version_part(env!("CARGO_PKG_VERSION_MAJOR"))),
    version_char(parse_version_part(env!("CARGO_PKG_VERSION_MINOR"))),
    version_char(parse_version_part(env!("CARGO_PKG_VERSION_PATCH"))),
    b'0',
    b'-',
];

/// Client name + version advertised in the BEP 10 `v` key
pub const CLIENT_VERSION: &str = concat!("Risuko ", env!("CARGO_PKG_VERSION"));

const fn parse_version_part(part: &str) -> u32 {
    let bytes = part.as_bytes();
    let mut value = 0u32;
    let mut i = 0;
    while i < bytes.len() {
        assert!(bytes[i].is_ascii_digit(), "non-numeric version component");
        value = value * 10 + (bytes[i] - b'0') as u32;
        i += 1;
    }
    value
}

/// Version digit: 0-9, then A-Z and a-z (libtorrent's encoding), saturating at `z`
const fn version_char(value: u32) -> u8 {
    match value {
        0..=9 => b'0' + value as u8,
        10..=35 => b'A' + (value - 10) as u8,
        36..=61 => b'a' + (value - 36) as u8,
        _ => b'z',
    }
}

/// Generate a fresh peer id for this client instance
pub fn generate_peer_id() -> Id20 {
    let mut raw = [0u8; 20];
    raw[..PEER_ID_PREFIX.len()].copy_from_slice(&PEER_ID_PREFIX);
    rand::rng().fill_bytes(&mut raw[PEER_ID_PREFIX.len()..]);
    Id20(raw)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_matches() {
        let id = generate_peer_id();
        assert!(id.0.starts_with(&PEER_ID_PREFIX));
    }

    #[test]
    fn randomness_differs() {
        let a = generate_peer_id();
        let b = generate_peer_id();
        // Prefix bytes match, suffix should (almost certainly) differ
        assert_ne!(a, b);
    }

    #[test]
    fn prefix_tracks_crate_version() {
        let digit = |part: &str| version_char(part.parse().unwrap());
        let expected = [
            b'-',
            b'R',
            b'S',
            digit(env!("CARGO_PKG_VERSION_MAJOR")),
            digit(env!("CARGO_PKG_VERSION_MINOR")),
            digit(env!("CARGO_PKG_VERSION_PATCH")),
            b'0',
            b'-',
        ];
        assert_eq!(PEER_ID_PREFIX, expected);
        assert_eq!(version_char(0), b'0');
        assert_eq!(version_char(9), b'9');
        assert_eq!(version_char(10), b'A');
        assert_eq!(version_char(36), b'a');
        assert_eq!(version_char(99), b'z');
    }
}
