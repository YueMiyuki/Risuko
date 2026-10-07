//! BEP 8 tracker peer obfuscation: `sha_ih` announces and peer lists XORed with RC4-drop768 keyed by the infohash (or `SHA-1(info_hash || iv)`)

use super::super::core::hash::sha1;
use super::super::core::Id20;
use super::super::wire::rc4::Rc4;

const DISCARD: usize = 768;
/// Cap on the tracker-declared keystream length, in peers
const MAX_KEYSTREAM_PEERS: u32 = 100_000;

/// `sha_ih` announce parameter: the infohash hashed again with SHA-1
pub fn sha_ih(info_hash: &Id20) -> Id20 {
    sha1(info_hash.as_bytes())
}

/// RC4 positioned at the first reserved byte (768 bytes discarded)
fn keystream(info_hash: &Id20, iv: Option<&[u8]>) -> Rc4 {
    let key = match iv {
        Some(iv) => {
            let mut material = info_hash.as_bytes().to_vec();
            material.extend_from_slice(iv);
            sha1(&material).0.to_vec()
        }
        None => info_hash.as_bytes().to_vec(),
    };
    let mut rc4 = Rc4::new(&key);
    rc4.apply_keystream(&mut [0u8; DISCARD]);
    rc4
}

fn take(rc4: &mut Rc4, n: usize) -> Vec<u8> {
    let mut out = vec![0u8; n];
    rc4.apply_keystream(&mut out);
    out
}

/// Obscure the announce `port` with the first keystream bytes after the reserved ones; BEP 8 names no offset and no implementation exists to match
pub fn obscure_port(info_hash: &Id20, port: u16) -> u16 {
    let mut rc4 = keystream(info_hash, None);
    let _reserved = take(&mut rc4, 8);
    let pad = take(&mut rc4, 2);
    port ^ u16::from_be_bytes([pad[0], pad[1]])
}

/// Decrypt a peer list field (`stride` 6 or 18); with `i` and `n` the keystream is `n` peers long, wraps, and starts `i` peers in
pub fn deobfuscate_peers(
    info_hash: &Id20,
    iv: Option<&[u8]>,
    i: Option<u32>,
    n: Option<u32>,
    data: &[u8],
    stride: usize,
) -> Option<Vec<u8>> {
    let mut rc4 = keystream(info_hash, iv);
    let reserved = take(&mut rc4, 8);
    let x = u32::from_be_bytes(reserved[0..4].try_into().ok()?);
    let y = u32::from_be_bytes(reserved[4..8].try_into().ok()?);
    let (start, pseudo) = match (i, n) {
        (Some(i), Some(n)) => {
            let (i, n) = (i ^ x, n ^ y);
            if n == 0 || n > MAX_KEYSTREAM_PEERS {
                return None;
            }
            // The keystream wraps every `n` peers, so reducing `i` first keeps the offset small
            (
                (i % n) as usize * stride,
                take(&mut rc4, n as usize * stride),
            )
        }
        _ => (0, take(&mut rc4, data.len())),
    };
    if pseudo.is_empty() {
        return Some(Vec::new());
    }
    Some(
        data.iter()
            .enumerate()
            .map(|(k, byte)| byte ^ pseudo[(start + k) % pseudo.len()])
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ih() -> Id20 {
        // sha1("hello"), the BEP's example infohash
        Id20::from_slice(&hex::decode("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d").unwrap()).unwrap()
    }

    #[test]
    fn sha_ih_matches_the_bep_example() {
        // The BEP's url-encoded example
        assert_eq!(
            sha_ih(&ih()).to_hex(),
            "6b4f89a54e2d27ecd7e8da05b4ab8fd9d1d8b119"
        );
    }

    #[test]
    fn port_obscuring_is_an_involution() {
        let obscured = obscure_port(&ih(), 6881);
        assert_ne!(obscured, 6881);
        assert_eq!(obscure_port(&ih(), obscured), 6881);
    }

    /// The BEP's reference tracker: encrypt with an `n`-peer keystream, return a window from peer `i`
    fn tracker_window(iv: &[u8], list: &[u8], n: u32, i: u32, count: usize) -> (Vec<u8>, u32, u32) {
        let mut rc4 = keystream(&ih(), Some(iv));
        let reserved = take(&mut rc4, 8);
        let x = u32::from_be_bytes(reserved[0..4].try_into().unwrap());
        let y = u32::from_be_bytes(reserved[4..8].try_into().unwrap());
        let pseudo = take(&mut rc4, n as usize * 6);
        let encrypted: Vec<u8> = list
            .iter()
            .enumerate()
            .map(|(k, b)| b ^ pseudo[k % pseudo.len()])
            .collect();
        let window = encrypted[i as usize * 6..(i as usize + count) * 6].to_vec();
        (window, i ^ x, n ^ y)
    }

    #[test]
    fn windowed_peer_list_round_trips_through_a_wrapping_keystream() {
        // Three peers, two peers' worth of keystream, window of peers 1..3
        let list = hex::decode("d048c1561ae1d151ad0f37f180d506081ae1").unwrap();
        let (window, i, n) = tracker_window(b"iv-bytes", &list, 2, 1, 2);
        let plain =
            deobfuscate_peers(&ih(), Some(b"iv-bytes"), Some(i), Some(n), &window, 6).unwrap();
        assert_eq!(plain, list[6..]);
    }

    #[test]
    fn window_start_past_the_keystream_wraps_like_the_reduced_index() {
        let list = hex::decode("d048c1561ae1d151ad0f37f180d506081ae1").unwrap();
        let (window, i, n) = tracker_window(b"iv-bytes", &list, 2, 1, 2);
        // `i ^ 1` recovers the mask; `u32::MAX` is peer 1 modulo the 2-peer keystream
        let far = i ^ 1 ^ u32::MAX;
        let plain =
            deobfuscate_peers(&ih(), Some(b"iv-bytes"), Some(far), Some(n), &window, 6).unwrap();
        assert_eq!(plain, list[6..]);
    }

    #[test]
    fn whole_list_without_window_parameters() {
        let list = hex::decode("d048c1561ae1d151ad0f37f1").unwrap();
        let mut rc4 = keystream(&ih(), None);
        let _ = take(&mut rc4, 8);
        let pseudo = take(&mut rc4, list.len());
        let encrypted: Vec<u8> = list.iter().zip(&pseudo).map(|(a, b)| a ^ b).collect();
        assert_eq!(
            deobfuscate_peers(&ih(), None, None, None, &encrypted, 6).unwrap(),
            list
        );
    }

    #[test]
    fn rejects_absurd_windows() {
        let mut rc4 = keystream(&ih(), None);
        let reserved = take(&mut rc4, 8);
        let y = u32::from_be_bytes(reserved[4..8].try_into().unwrap());
        // n decodes to zero
        assert!(deobfuscate_peers(&ih(), None, Some(0), Some(y), &[0u8; 6], 6).is_none());
    }
}
