use md5::Md5;
use sha1::Sha1;
use sha2::digest::Digest;
use sha2::Sha256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algo {
    Sha256,
    Sha1,
    Md5,
}

impl Algo {
    pub fn parse(name: &str) -> Option<Self> {
        let norm = name.trim().to_ascii_lowercase().replace('-', "");
        match norm.as_str() {
            "sha256" => Some(Algo::Sha256),
            "sha1" => Some(Algo::Sha1),
            "md5" => Some(Algo::Md5),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Algo::Sha256 => "sha-256",
            Algo::Sha1 => "sha-1",
            Algo::Md5 => "md5",
        }
    }

    pub fn hex_len(self) -> usize {
        match self {
            Algo::Sha256 => 64,
            Algo::Sha1 => 40,
            Algo::Md5 => 32,
        }
    }
}

pub enum Hasher {
    Sha256(Sha256),
    Sha1(Sha1),
    Md5(Md5),
}

impl Hasher {
    pub fn new(algo: Algo) -> Self {
        match algo {
            Algo::Sha256 => Hasher::Sha256(Sha256::new()),
            Algo::Sha1 => Hasher::Sha1(Sha1::new()),
            Algo::Md5 => Hasher::Md5(Md5::new()),
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        match self {
            Hasher::Sha256(h) => h.update(data),
            Hasher::Sha1(h) => h.update(data),
            Hasher::Md5(h) => h.update(data),
        }
    }

    pub fn finalize_hex(self) -> String {
        match self {
            Hasher::Sha256(h) => hex::encode(h.finalize()),
            Hasher::Sha1(h) => hex::encode(h.finalize()),
            Hasher::Md5(h) => hex::encode(h.finalize()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WholeChecksum {
    pub algo: Algo,
    pub hex: String,
}

impl WholeChecksum {
    pub fn parse(s: &str) -> Result<Self, String> {
        let body = s
            .trim()
            .strip_prefix("checksum=")
            .unwrap_or_else(|| s.trim());
        let (algo_part, hex_part) = body
            .split_once(':')
            .or_else(|| body.split_once('='))
            .ok_or_else(|| "checksum must be `<algo>:<hex>`".to_string())?;
        let algo = Algo::parse(algo_part)
            .ok_or_else(|| format!("unknown checksum algorithm: {algo_part}"))?;
        let hex = hex_part.trim().to_ascii_lowercase();
        if hex.len() != algo.hex_len() {
            return Err(format!(
                "{} digest must be {} hex chars, got {}",
                algo.name(),
                algo.hex_len(),
                hex.len()
            ));
        }
        if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err("checksum contains non-hex characters".to_string());
        }
        Ok(Self { algo, hex })
    }

    pub fn matches(&self, computed_hex: &str) -> bool {
        self.hex.eq_ignore_ascii_case(computed_hex)
    }
}

#[derive(Debug, Clone)]
pub struct PieceChecksums {
    pub algo: Algo,
    pub hexes: Vec<String>,
}

impl PieceChecksums {
    pub fn parse(s: &str) -> Result<Self, String> {
        let body = s
            .trim()
            .strip_prefix("piece-checksums=")
            .unwrap_or_else(|| s.trim());
        let (algo_part, list_part) = body
            .split_once(':')
            .or_else(|| body.split_once('='))
            .ok_or_else(|| "piece-checksums must be `<algo>:<hex>,...`".to_string())?;
        let algo = Algo::parse(algo_part)
            .ok_or_else(|| format!("unknown checksum algorithm: {algo_part}"))?;
        let hexes: Vec<String> = list_part
            .split(',')
            .map(|h| h.trim().to_ascii_lowercase())
            .filter(|h| !h.is_empty())
            .collect();
        for h in &hexes {
            if h.len() != algo.hex_len() || !h.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(format!("invalid {} digest: {}", algo.name(), h));
            }
        }
        if hexes.is_empty() {
            return Err("piece-checksums list is empty".to_string());
        }
        Ok(Self { algo, hexes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn algo_parse() {
        assert_eq!(Algo::parse("sha-256"), Some(Algo::Sha256));
        assert_eq!(Algo::parse("SHA256"), Some(Algo::Sha256));
        assert_eq!(Algo::parse("sha1"), Some(Algo::Sha1));
        assert_eq!(Algo::parse("MD5"), Some(Algo::Md5));
        assert_eq!(Algo::parse("crc32"), None);
    }

    #[test]
    fn whole_checksum_parse() {
        let c = WholeChecksum::parse(
            "sha-256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )
        .unwrap();
        assert_eq!(c.algo, Algo::Sha256);
        assert!(c.matches("E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855"));
    }

    #[test]
    fn whole_checksum_rejects_short_hex() {
        assert!(WholeChecksum::parse("sha-256:abc").is_err());
    }

    #[test]
    fn piece_checksums_parse() {
        let p = PieceChecksums::parse("sha-1:da39a3ee5e6b4b0d3255bfef95601890afd80709,da39a3ee5e6b4b0d3255bfef95601890afd80709")
            .unwrap();
        assert_eq!(p.algo, Algo::Sha1);
        assert_eq!(p.hexes.len(), 2);
    }

    #[test]
    fn streaming_matches_oneshot() {
        let mut h = Hasher::new(Algo::Sha256);
        h.update(b"hello ");
        h.update(b"world");
        assert_eq!(
            h.finalize_hex(),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }
}
