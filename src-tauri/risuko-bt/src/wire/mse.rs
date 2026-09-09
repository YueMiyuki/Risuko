//! Message Stream Encryption (MSE) / Protocol Encryption (PE)

use std::io;
use std::sync::OnceLock;

use num_bigint::BigUint;
use rand::{Rng, RngExt};
use sha1::{Digest, Sha1};

use super::rc4::Rc4;

const P_HEX: &str = concat!(
    "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD1",
    "29024E088A67CC74020BBEA63B139B22514A08798E3404DD",
    "EF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245",
    "E485B576625E7EC6F44C42E9A63A3620FFFFFFFFFFFFFFFF"
);
const G: u64 = 2;
pub const DH_LEN: usize = 96;
pub const VC: [u8; 8] = [0u8; 8];

pub mod crypto {
    pub const PLAINTEXT: u32 = 0x01;
    pub const RC4: u32 = 0x02;
}

fn modulus() -> &'static BigUint {
    static MODULUS: OnceLock<BigUint> = OnceLock::new();
    MODULUS.get_or_init(|| BigUint::parse_bytes(P_HEX.as_bytes(), 16).expect("valid hex modulus"))
}

fn to_fixed_be(n: &BigUint, len: usize) -> Vec<u8> {
    let bytes = n.to_bytes_be();
    if bytes.len() >= len {
        bytes[bytes.len() - len..].to_vec()
    } else {
        let mut out = vec![0u8; len - bytes.len()];
        out.extend_from_slice(&bytes);
        out
    }
}

/// A freshly generated DH private/public pair
pub struct DhKeys {
    pub private: BigUint,
    pub public_be: [u8; DH_LEN],
}

impl DhKeys {
    /// Generate a random 160-bit exponent X and derive Y = G^X mod P
    pub fn generate() -> Self {
        let mut rng = rand::rng();
        // 160-bit private key is plenty per spec (saves CPU vs a full 768-bit one)
        let mut x_bytes = [0u8; 20];
        rng.fill_bytes(&mut x_bytes);
        // Force the low bit of the most-significant byte so X is a large, non-zero exponent
        x_bytes[0] |= 0x01;
        let x = BigUint::from_bytes_be(&x_bytes);
        let p = modulus();
        let y = BigUint::from(G).modpow(&x, p);
        let mut public_be = [0u8; DH_LEN];
        public_be.copy_from_slice(&to_fixed_be(&y, DH_LEN));
        Self {
            private: x,
            public_be,
        }
    }

    /// Compute the shared secret S = Y_other^X mod P; errors if the peer's public key is invalid (0, 1, or >= p-1)
    pub fn shared_secret(&self, peer_public_be: &[u8; DH_LEN]) -> io::Result<[u8; DH_LEN]> {
        let y = BigUint::from_bytes_be(peer_public_be);
        let p = modulus();
        let one = BigUint::from(1u32);
        let p_minus_1 = p - &one;
        if y <= one || y >= p_minus_1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid DH public key",
            ));
        }
        let s = y.modpow(&self.private, p);
        let mut out = [0u8; DH_LEN];
        out.copy_from_slice(&to_fixed_be(&s, DH_LEN));
        Ok(out)
    }
}

pub fn sha1_many(parts: &[&[u8]]) -> [u8; 20] {
    let mut h = Sha1::new();
    for p in parts {
        h.update(p);
    }
    let out = h.finalize();
    let mut r = [0u8; 20];
    r.copy_from_slice(&out);
    r
}

pub fn xor20(a: &[u8; 20], b: &[u8; 20]) -> [u8; 20] {
    let mut out = [0u8; 20];
    for i in 0..20 {
        out[i] = a[i] ^ b[i];
    }
    out
}

pub fn req1(s: &[u8; DH_LEN]) -> [u8; 20] {
    sha1_many(&[b"req1", s])
}
pub fn req2(skey: &[u8; 20]) -> [u8; 20] {
    sha1_many(&[b"req2", skey])
}
pub fn req3(s: &[u8; DH_LEN]) -> [u8; 20] {
    sha1_many(&[b"req3", s])
}

pub fn rc4_key(tag: &[u8; 4], s: &[u8; DH_LEN], skey: &[u8; 20]) -> [u8; 20] {
    sha1_many(&[tag, s, skey])
}

pub type MseRc4 = Rc4;

pub fn init_rc4(key: &[u8; 20]) -> MseRc4 {
    let mut c = Rc4::new(key);
    let mut sink = [0u8; 1024];
    c.apply_keystream(&mut sink);
    c
}

/// Generate 0..=max_len random padding bytes
pub fn gen_pad(max_len: usize) -> Vec<u8> {
    let mut rng = rand::rng();
    let len = (rng.random::<u32>() as usize) % (max_len + 1);
    let mut v = vec![0u8; len];
    rng.fill_bytes(&mut v);
    v
}

/// Errors from the byte-level MSE handshake
#[derive(Debug, thiserror::Error)]
pub enum MseError {
    #[error("pad length too large: {0}")]
    PadTooLarge(u16),
    #[error("initial-payload length too large: {0}")]
    IaTooLarge(u16),
}

/// First occurrence of `needle` in `hay` at or after `start`, else `None`; used by the incoming side to locate `HASH('req1',S)`
pub fn find_subsequence_from(hay: &[u8], needle: &[u8], start: usize) -> Option<usize> {
    if needle.is_empty() || needle.len() > hay.len() {
        return None;
    }
    let max_start = hay.len() - needle.len();
    if start > max_start {
        return None;
    }
    hay[start..]
        .windows(needle.len())
        .position(|w| w == needle)
        .map(|off| off + start)
}

/// First candidate offset to rescan after appending bytes to a searched buffer; backs up by `needle_len - 1` to keep matches that cross the old/new boundary
pub fn scan_start_after_append(previous_len: usize, needle_len: usize) -> usize {
    previous_len.saturating_sub(needle_len.saturating_sub(1))
}

/// Build the initiator's third message body (plaintext, RC4-encrypted by the caller): `VC || crypto_provide || len(PadC) || PadC || len(IA) || IA`
pub fn build_initiator_payload(
    crypto_provide: u32,
    pad_c: &[u8],
    ia: &[u8],
) -> Result<Vec<u8>, MseError> {
    if pad_c.len() > u16::MAX as usize {
        return Err(MseError::PadTooLarge(pad_c.len() as u16));
    }
    if ia.len() > u16::MAX as usize {
        return Err(MseError::IaTooLarge(ia.len() as u16));
    }
    let mut out = Vec::with_capacity(8 + 4 + 2 + pad_c.len() + 2 + ia.len());
    out.extend_from_slice(&VC);
    out.extend_from_slice(&crypto_provide.to_be_bytes());
    out.extend_from_slice(&(pad_c.len() as u16).to_be_bytes());
    out.extend_from_slice(pad_c);
    out.extend_from_slice(&(ia.len() as u16).to_be_bytes());
    out.extend_from_slice(ia);
    Ok(out)
}

/// Build the responder's reply body (plaintext, RC4-encrypted by the caller): `VC || crypto_select || len(PadD) || PadD`
pub fn build_responder_payload(crypto_select: u32, pad_d: &[u8]) -> Result<Vec<u8>, MseError> {
    if pad_d.len() > u16::MAX as usize {
        return Err(MseError::PadTooLarge(pad_d.len() as u16));
    }
    let mut out = Vec::with_capacity(8 + 4 + 2 + pad_d.len());
    out.extend_from_slice(&VC);
    out.extend_from_slice(&crypto_select.to_be_bytes());
    out.extend_from_slice(&(pad_d.len() as u16).to_be_bytes());
    out.extend_from_slice(pad_d);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dh_roundtrip_shared_secret() {
        let a = DhKeys::generate();
        let b = DhKeys::generate();
        let s_a = a.shared_secret(&b.public_be).unwrap();
        let s_b = b.shared_secret(&a.public_be).unwrap();
        assert_eq!(s_a, s_b);
    }

    #[test]
    fn rc4_encrypt_decrypt_matches_with_key_a_key_b() {
        let s = [0xAAu8; DH_LEN];
        let skey = [0xBBu8; 20];
        let kab = rc4_key(b"keyA", &s, &skey);
        let kba = rc4_key(b"keyB", &s, &skey);
        let mut enc_out = init_rc4(&kab);
        let mut dec_in = init_rc4(&kab);
        let mut buf = *b"hello bittorrent mse";
        enc_out.apply_keystream(&mut buf);
        assert_ne!(&buf, b"hello bittorrent mse");
        dec_in.apply_keystream(&mut buf);
        assert_eq!(&buf, b"hello bittorrent mse");
        // keyB stream is independent from keyA stream
        let mut other = init_rc4(&kba);
        let mut probe = [0u8; 4];
        other.apply_keystream(&mut probe);
        let mut again = init_rc4(&kab);
        let mut probe2 = [0u8; 4];
        again.apply_keystream(&mut probe2);
        assert_ne!(probe, probe2);
    }

    #[test]
    fn req1_req2_req3_differ() {
        let s = [0u8; DH_LEN];
        let skey = [0u8; 20];
        let a = req1(&s);
        let b = req2(&skey);
        let c = req3(&s);
        assert_ne!(a, b);
        assert_ne!(a, c);
        assert_ne!(b, c);
    }

    #[test]
    fn payload_round_trip() {
        let p =
            build_initiator_payload(crypto::RC4 | crypto::PLAINTEXT, &[1, 2, 3], b"ia").unwrap();
        assert_eq!(&p[0..8], &VC);
        let provide = u32::from_be_bytes([p[8], p[9], p[10], p[11]]);
        assert_eq!(provide, 0x03);
        let padlen = u16::from_be_bytes([p[12], p[13]]) as usize;
        assert_eq!(padlen, 3);
        let ia_off = 14 + padlen;
        let ia_len = u16::from_be_bytes([p[ia_off], p[ia_off + 1]]) as usize;
        assert_eq!(ia_len, 2);
        assert_eq!(&p[ia_off + 2..ia_off + 2 + ia_len], b"ia");
    }

    #[test]
    fn find_subsequence_works() {
        let hay = b"aaaabbbbccccddddeeeeffff";
        assert_eq!(find_subsequence_from(hay, b"ccccdddd", 0), Some(8));
        assert_eq!(find_subsequence_from(hay, b"zzzz", 0), None);
        assert_eq!(find_subsequence_from(hay, b"", 0), None);
    }

    #[test]
    fn cursor_search_finds_boundary_spanning_subsequence() {
        let needle = b"needle";
        let mut hay = b"aaaa nee".to_vec();
        let start = scan_start_after_append(hay.len(), needle.len());
        hay.extend_from_slice(b"dle zzzz");

        assert_eq!(find_subsequence_from(&hay, needle, start), Some(5));
    }

    #[test]
    fn cursor_search_skips_already_scanned_prefix() {
        let hay = b"needle xxxx needle";

        assert_eq!(find_subsequence_from(hay, b"needle", 1), Some(12));
    }
}
