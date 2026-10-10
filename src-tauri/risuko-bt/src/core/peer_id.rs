use rand::Rng;

use super::hash::Id20;

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

const fn version_char(value: u32) -> u8 {
    match value {
        0..=9 => b'0' + value as u8,
        10..=35 => b'A' + (value - 10) as u8,
        36..=61 => b'a' + (value - 36) as u8,
        _ => b'z',
    }
}

pub fn generate_peer_id() -> Id20 {
    let mut raw = [0u8; 20];
    raw[..PEER_ID_PREFIX.len()].copy_from_slice(&PEER_ID_PREFIX);
    rand::rng().fill_bytes(&mut raw[PEER_ID_PREFIX.len()..]);
    Id20(raw)
}

static SESSION_PEER_ID: parking_lot::Mutex<Option<(u16, Id20)>> = parking_lot::Mutex::new(None);

pub(crate) fn set_session_peer_id(listen_port: u16, id: Id20) {
    *SESSION_PEER_ID.lock() = Some((listen_port, id));
}

static FALLBACK_PEER_ID: std::sync::OnceLock<Id20> = std::sync::OnceLock::new();

pub(crate) fn session_peer_id_or_new(listen_port: u16) -> Id20 {
    match *SESSION_PEER_ID.lock() {
        Some((port, id)) if port == listen_port => id,
        _ => *FALLBACK_PEER_ID.get_or_init(generate_peer_id),
    }
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

    #[test]
    fn session_peer_id_is_reused_once_recorded() {
        let id = generate_peer_id();
        set_session_peer_id(54321, id);
        assert_eq!(session_peer_id_or_new(54321), id);
        assert_ne!(session_peer_id_or_new(54322), id);
        assert_eq!(session_peer_id_or_new(54322), session_peer_id_or_new(54323));
    }
}
