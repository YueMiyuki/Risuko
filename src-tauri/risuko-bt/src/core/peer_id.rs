use std::collections::BTreeMap;

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

pub(crate) const fn parse_version_part(part: &str) -> u32 {
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

const AZUREUS_CLIENTS: [(&[u8; 2], &str); 17] = [
    (b"AZ", "Azureus"),
    (b"BC", "BitComet"),
    (b"BI", "BiglyBT"),
    (b"BT", "BitTorrent"),
    (b"DE", "Deluge"),
    (b"FD", "Free Download Manager"),
    (b"KT", "KTorrent"),
    (b"LT", "libtorrent"),
    (b"lt", "libTorrent"),
    (b"qB", "qBittorrent"),
    (b"RS", "Risuko"),
    (b"SD", "Thunder"),
    (b"TR", "Transmission"),
    (b"UM", "\u{b5}Torrent Mac"),
    (b"UT", "\u{b5}Torrent"),
    (b"WW", "WebTorrent"),
    (b"XL", "Xunlei"),
];

const SHADOW_CLIENTS: [(u8, &str); 7] = [
    (b'A', "ABC"),
    (b'O', "Osprey Permaseed"),
    (b'Q', "BTQueue"),
    (b'R', "Tribler"),
    (b'S', "Shadow"),
    (b'T', "BitTornado"),
    (b'U', "UPnP NAT BitTorrent"),
];

fn version_value(byte: u8) -> Option<u32> {
    match byte {
        b'0'..=b'9' => Some(u32::from(byte - b'0')),
        b'A'..=b'Z' => Some(u32::from(byte - b'A') + 10),
        b'a'..=b'z' => Some(u32::from(byte - b'a') + 36),
        _ => None,
    }
}

fn named_version(name: &str, digits: &[u8]) -> String {
    let Some(mut parts) = digits
        .iter()
        .map(|&byte| version_value(byte))
        .collect::<Option<Vec<u32>>>()
    else {
        return name.to_string();
    };
    while parts.len() > 2 && parts.last() == Some(&0) {
        parts.pop();
    }
    let version: Vec<String> = parts.iter().map(u32::to_string).collect();
    format!("{name} {}", version.join("."))
}

pub(crate) fn client_name_from_peer_id(id: &[u8; 20]) -> Option<String> {
    if id[0] == b'-' && id[7] == b'-' {
        let (_, name) = AZUREUS_CLIENTS
            .iter()
            .find(|(code, _)| **code == id[1..3])?;
        return Some(named_version(name, &id[3..7]));
    }
    let (_, name) = SHADOW_CLIENTS.iter().find(|(code, _)| *code == id[0])?;
    (id[4..8] == *b"----" && id[1..4].iter().all(u8::is_ascii_alphanumeric))
        .then(|| named_version(name, &id[1..4]))
}

static SESSION_PEER_IDS: parking_lot::Mutex<BTreeMap<u16, Id20>> =
    parking_lot::Mutex::new(BTreeMap::new());

pub(crate) fn set_session_peer_id(listen_port: u16, id: Id20) {
    SESSION_PEER_IDS.lock().insert(listen_port, id);
}

pub(crate) fn clear_session_peer_id(listen_port: u16, id: Id20) {
    let mut ids = SESSION_PEER_IDS.lock();
    if ids.get(&listen_port) == Some(&id) {
        ids.remove(&listen_port);
    }
}

static FALLBACK_PEER_ID: std::sync::OnceLock<Id20> = std::sync::OnceLock::new();

pub(crate) fn session_peer_id_or_new(listen_port: u16) -> Id20 {
    let recorded = SESSION_PEER_IDS.lock().get(&listen_port).copied();
    recorded.unwrap_or_else(|| *FALLBACK_PEER_ID.get_or_init(generate_peer_id))
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

    fn id_from(prefix: &[u8]) -> [u8; 20] {
        let mut id = [b'x'; 20];
        id[..prefix.len()].copy_from_slice(prefix);
        id
    }

    #[test]
    fn azureus_style_peer_ids_name_the_client() {
        for (prefix, expected) in [
            (&b"-qB4620-"[..], "qBittorrent 4.6.2"),
            (b"-TR4000-", "Transmission 4.0"),
            (b"-UT3550-", "\u{b5}Torrent 3.5.5"),
            (b"-LT1200-", "libtorrent 1.2"),
            (b"-lt0D80-", "libTorrent 0.13.8"),
            (b"-DE2110-", "Deluge 2.1.1"),
            (b"-BC0210-", "BitComet 0.2.1"),
            (b"-AZ5750-", "Azureus 5.7.5"),
            (b"-RS1030-", "Risuko 1.0.3"),
        ] {
            assert_eq!(
                client_name_from_peer_id(&id_from(prefix)).as_deref(),
                Some(expected),
                "{}",
                String::from_utf8_lossy(prefix)
            );
        }
    }

    #[test]
    fn shadow_style_peer_ids_name_the_client() {
        assert_eq!(
            client_name_from_peer_id(&id_from(b"T03B-----")).as_deref(),
            Some("BitTornado 0.3.11")
        );
        assert_eq!(
            client_name_from_peer_id(&id_from(b"S58B-----")).as_deref(),
            Some("Shadow 5.8.11")
        );
    }

    #[test]
    fn unrecognised_peer_ids_have_no_client_name() {
        assert_eq!(client_name_from_peer_id(&[0u8; 20]), None);
        assert_eq!(client_name_from_peer_id(&id_from(b"-ZZ1234-")), None);
        assert_eq!(client_name_from_peer_id(&id_from(b"-qB46")), None);
        assert_eq!(
            client_name_from_peer_id(&id_from(b"-qB4_20-")).as_deref(),
            Some("qBittorrent")
        );
    }

    #[test]
    fn own_peer_ids_are_recognised() {
        let name = client_name_from_peer_id(&generate_peer_id().0).unwrap();
        assert!(name.starts_with("Risuko "), "{name}");
    }

    #[test]
    fn session_peer_id_is_reused_once_recorded() {
        let id = generate_peer_id();
        set_session_peer_id(54321, id);
        assert_eq!(session_peer_id_or_new(54321), id);
        assert_ne!(session_peer_id_or_new(54322), id);
        assert_eq!(session_peer_id_or_new(54322), session_peer_id_or_new(54323));
    }

    #[test]
    fn concurrent_sessions_keep_their_own_peer_ids() {
        let first = generate_peer_id();
        let second = generate_peer_id();
        set_session_peer_id(54331, first);
        set_session_peer_id(54332, second);
        assert_eq!(session_peer_id_or_new(54331), first);
        assert_eq!(session_peer_id_or_new(54332), second);
        clear_session_peer_id(54331, second);
        assert_eq!(session_peer_id_or_new(54331), first);
        clear_session_peer_id(54331, first);
        assert_ne!(session_peer_id_or_new(54331), first);
        assert_eq!(session_peer_id_or_new(54332), second);
    }
}
