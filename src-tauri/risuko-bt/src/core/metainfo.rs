//! `.torrent` metainfo parsing (BEP-3) Produces [`TorrentMeta`] with the raw `info` dict bytes preserved so the info-hash can be recomputed. [`ValidatedTorrentMetaV1Info`] wraps a parsed info dict with an enumerator over per-file details, matching the API shape that `engine::torrent` consumes from librqbit

use std::collections::HashSet;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use super::super::bencode::{decode_dict_field_raw, Value};
use super::hash::{sha1, sha256, Id20, Id32};

#[derive(Debug, thiserror::Error)]
pub enum MetaError {
    #[error("bencode: {0}")]
    Bencode(#[from] super::super::bencode::Error),
    #[error("metainfo: missing `info` dict")]
    MissingInfo,
    #[error("metainfo: malformed info dict: {0}")]
    BadInfo(&'static str),
    #[error("metainfo: invalid UTF-8 in {field}")]
    BadUtf8 { field: &'static str },
    #[error("metainfo: torrent has zero length")]
    ZeroLength,
    #[error("metainfo: pieces field is not a multiple of 20 bytes")]
    BadPieces,
    #[error("metainfo: malformed v2 info dict: {0}")]
    BadInfoV2(&'static str),
    #[error("metainfo: missing required piece layers for one or more files")]
    MissingPieceLayers,
}

/// Which BEP versions a `.torrent` declares
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaVersion {
    /// BEP-3 only (`pieces` SHA-1 list, no `meta version`)
    V1,
    /// BEP 52 only (`meta version=2` with `file tree`, no v1 `pieces`/`files`)
    V2,
    /// Both v1 and v2 dicts present in the same `info` block
    Hybrid,
}

impl MetaVersion {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
            Self::Hybrid => "hybrid",
        }
    }

    pub fn has_v1(&self) -> bool {
        matches!(self, Self::V1 | Self::Hybrid)
    }
}

/// Pair of optional v1/v2 info-hashes identifying a torrent. At least one is always populated. Hybrid torrents populate both
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TorrentInfoHashes {
    pub v1: Option<Id20>,
    pub v2: Option<Id32>,
}

impl TorrentInfoHashes {
    /// All 20-byte hashes to announce on for this torrent. Hybrid returns both the v1 hash and the truncated v2 hash so peers from either swarm are reachable; v1-only or v2-only returns a single hash
    pub fn announce_infohashes(&self) -> Vec<Id20> {
        let mut out = Vec::with_capacity(2);
        if let Some(v1) = self.v1 {
            out.push(v1);
        }
        if let Some(v2) = self.v2 {
            let trunc = v2.truncate_to_id20();
            // For pure-v2 the truncated hash IS the wire infohash, no dup
            if !out.contains(&trunc) {
                out.push(trunc);
            }
        }
        out
    }
}

/// Raw top-level `.torrent` metadata
#[derive(Debug, Clone)]
pub struct TorrentMeta {
    pub info: ValidatedTorrentMetaV1Info,
    pub announce: Option<String>,
    pub announce_list: Vec<Vec<String>>,
    pub url_list: Vec<String>,
    pub bootstrap_nodes: Vec<(Id20, SocketAddr)>,
    pub bootstrap_hosts: Vec<(Id20, String, u16)>,
    pub comment: Option<String>,
    pub created_by: Option<String>,
    pub creation_date: Option<i64>,
    pub encoding: Option<String>,
    pub info_hash: Id20,
    pub info_v2: Option<ValidatedTorrentMetaV2Info>,
    pub info_hash_v2: Option<Id32>,
    pub meta_version: MetaVersion,
    pub piece_layers: std::collections::BTreeMap<Id32, Vec<u8>>,
    pub info_bytes: Vec<u8>,
}

impl TorrentMeta {
    /// Identity bundle (v1/v2 hashes) for this torrent
    pub fn info_hashes(&self) -> TorrentInfoHashes {
        TorrentInfoHashes {
            v1: self.meta_version.has_v1().then_some(self.info_hash),
            v2: self.info_hash_v2,
        }
    }

    /// All 20-byte hashes to announce on (BEP-3 trackers, v1 DHT, LSD) Hybrid torrents return both the v1 hash and the truncated v2 hash
    pub fn announce_infohashes(&self) -> Vec<Id20> {
        self.info_hashes().announce_infohashes()
    }
}

/// Parsed and validated `info` dictionary
#[derive(Debug, Clone)]
pub struct ValidatedTorrentMetaV1Info {
    pub name: String,
    pub piece_length: u32,
    /// SHA-1 hash per piece, in order. Length == `piece_count * 20`
    pub pieces: Vec<u8>,
    pub private: bool,
    pub files: Vec<TorrentMetaInfo>,
    /// True if the torrent described a single file (no `files` list)
    pub single_file_mode: bool,
}

/// A single file entry as viewed by the rest of the engine
#[derive(Debug, Clone)]
pub struct TorrentMetaInfo {
    /// Components relative to the torrent root (never absolute, never `..`)
    pub path: Vec<String>,
    pub length: u64,
}

/// Aggregated file view returned by [`ValidatedTorrentMetaV1Info::iter_file_details`]
#[derive(Debug, Clone)]
pub struct FileDetails {
    /// Joined path, suitable for display. Slashes are used regardless of OS
    pub filename: String,
    pub len: u64,
}

impl ValidatedTorrentMetaV1Info {
    pub fn iter_file_details(&self) -> impl Iterator<Item = FileDetails> + '_ {
        self.files.iter().map(|f| FileDetails {
            filename: f.path.join("/"),
            len: f.length,
        })
    }

    pub fn piece_count(&self) -> u32 {
        if self.pieces.is_empty() {
            // Pure-v2 facade: derive piece count from total length and piece length. v2 verification doesn't use the SHA-1 `pieces` blob
            let total = self.total_length();
            if self.piece_length == 0 || total == 0 {
                return 0;
            }
            total.div_ceil(self.piece_length as u64) as u32
        } else {
            (self.pieces.len() / 20) as u32
        }
    }

    pub fn total_length(&self) -> u64 {
        self.files.iter().map(|f| f.length).sum()
    }
}

// BEP 52 (v2) info dict

/// One file entry derived from the v2 `file tree` (BEP 52). Path components are sanitised the same way as v1
#[derive(Debug, Clone)]
pub struct TorrentMetaInfoV2 {
    pub path: Vec<String>,
    pub length: u64,
    /// Per-file Merkle-tree root (SHA-256) over 16 KiB leaves
    pub pieces_root: Id32,
}

/// Recursive view of the BEP 52 `file tree` dict
#[derive(Debug, Clone)]
pub enum FileTreeNode {
    /// Directory, keyed by component name
    Dir(std::collections::BTreeMap<String, FileTreeNode>),
    /// File leaf carrying length and (for non-empty files) Merkle root
    File {
        length: u64,
        pieces_root: Option<Id32>,
    },
}

/// Parsed BEP 52 v2 `info` view
#[derive(Debug, Clone)]
pub struct ValidatedTorrentMetaV2Info {
    pub name: String,
    pub piece_length: u32,
    /// BEP 27 private flag at info-dict level (same semantics as v1)
    pub private: bool,
    /// Flat file list in stable order matching `file tree` traversal
    pub files: Vec<TorrentMetaInfoV2>,
}

impl ValidatedTorrentMetaV2Info {
    pub fn total_length(&self) -> u64 {
        self.files.iter().map(|f| f.length).sum()
    }
}

/// Parse a raw `info` dict (as fetched via BEP-9 `ut_metadata`) and return its v2 view if the dict declares `meta version=2` with a `file tree`. Returns `Ok(None)` for v1-only info dicts. Used by the magnet resolver to identify the v2 file roots that need piece-layer hashes
pub fn parse_info_v2_from_bytes(
    info_bytes: &[u8],
) -> Result<Option<ValidatedTorrentMetaV2Info>, MetaError> {
    let value = super::super::bencode::decode_all(info_bytes)?;
    value.as_dict().ok_or(MetaError::BadInfo("info not dict"))?;
    let has_v2 = matches!(value.get(b"meta version").and_then(Value::as_int), Some(2))
        && value.get(b"file tree").is_some();
    if !has_v2 {
        return Ok(None);
    }
    Ok(Some(validate_info_v2(&value)?))
}

/// Parse a `.torrent` blob
pub fn parse_torrent(bytes: &[u8]) -> Result<TorrentMeta, MetaError> {
    let value = super::super::bencode::decode_all(bytes)?;
    value
        .as_dict()
        .ok_or(MetaError::BadInfo("top-level not dict"))?;

    let announce = get_str(&value, b"announce");
    let announce_list = value
        .get(b"announce-list")
        .and_then(Value::as_list)
        .map(|tiers| {
            tiers
                .iter()
                .filter_map(|tier| tier.as_list())
                .map(|urls| {
                    urls.iter()
                        .filter_map(|u| u.as_str().map(String::from))
                        .collect()
                })
                .collect::<Vec<Vec<String>>>()
        })
        .unwrap_or_default();
    let url_list = parse_url_list(&value);
    let (bootstrap_nodes, bootstrap_hosts) = parse_bootstrap_nodes(&value);
    let comment = get_str(&value, b"comment");
    let created_by = get_str(&value, b"created by");
    let encoding = get_str(&value, b"encoding");
    let creation_date = value.get(b"creation date").and_then(Value::as_int);

    // Recover raw bytes of the `info` field to compute the info-hash
    let (info_value, info_raw) =
        decode_dict_field_raw(bytes, b"info")?.ok_or(MetaError::MissingInfo)?;

    info_value
        .as_dict()
        .ok_or(MetaError::BadInfo("info not dict"))?;

    // Detect v1/v2 presence
    let has_v1 = info_value.get(b"pieces").is_some();
    let has_v2 = matches!(
        info_value.get(b"meta version").and_then(Value::as_int),
        Some(2)
    ) && info_value.get(b"file tree").is_some();

    let meta_version = match (has_v1, has_v2) {
        (true, true) => MetaVersion::Hybrid,
        (true, false) => MetaVersion::V1,
        (false, true) => MetaVersion::V2,
        (false, false) => return Err(MetaError::BadInfo("info dict has neither v1 nor v2")),
    };

    if !has_v1 {
        // Pure-v2: synthesize a v1-shaped facade so the rest of the engine can iterate files / piece counts uniformly. Verification is routed through `PieceVerifier::V2Merkle` instead of SHA-1
        let v2 = validate_info_v2(&info_value)?;
        let info_hash_v2_val = sha256(info_raw);
        let piece_layers = parse_piece_layers(&value)?;
        let info = synthesize_v1_facade_from_v2(&v2);
        // Pure-v2 from a `.torrent` requires `piece layers` for every file larger than one piece (otherwise per-piece verification has no anchor). Reject up front rather than fail mid-download
        for file in &v2.files {
            if file.length <= v2.piece_length as u64 {
                continue;
            }
            let root = file.pieces_root;
            if !piece_layers.contains_key(&root) {
                return Err(MetaError::MissingPieceLayers);
            }
        }
        // Pure-v2 wire infohash is the truncated SHA-256
        let wire_hash = info_hash_v2_val.truncate_to_id20();
        return Ok(TorrentMeta {
            info,
            announce,
            announce_list,
            url_list,
            bootstrap_nodes,
            bootstrap_hosts,
            comment,
            created_by,
            creation_date,
            encoding,
            info_hash: wire_hash,
            info_v2: Some(v2),
            info_hash_v2: Some(info_hash_v2_val),
            meta_version,
            piece_layers,
            info_bytes: info_raw.to_vec(),
        });
    }

    let info_hash = sha1(info_raw);
    let mut info = validate_info(&info_value)?;

    let (info_v2, info_hash_v2) = if has_v2 {
        let v2 = validate_info_v2(&info_value)?;
        info.private |= v2.private;
        let h = sha256(info_raw);
        (Some(v2), Some(h))
    } else {
        (None, None)
    };

    let piece_layers = if has_v2 {
        parse_piece_layers(&value)?
    } else {
        std::collections::BTreeMap::new()
    };

    Ok(TorrentMeta {
        info,
        announce,
        announce_list,
        url_list,
        bootstrap_nodes,
        bootstrap_hosts,
        comment,
        created_by,
        creation_date,
        encoding,
        info_hash,
        info_v2,
        info_hash_v2,
        meta_version,
        piece_layers,
        info_bytes: info_raw.to_vec(),
    })
}

fn parse_url_list(value: &Value) -> Vec<String> {
    value
        .get(b"url-list")
        .map(crate::webseed::parse_url_list)
        .unwrap_or_default()
}

fn parse_bootstrap_nodes(value: &Value) -> (Vec<(Id20, SocketAddr)>, Vec<(Id20, String, u16)>) {
    const MAX_BOOTSTRAP_NODES: usize = 4096;
    let mut out = Vec::new();
    let mut hosts = Vec::new();
    let mut seen_addrs = HashSet::new();
    let mut seen_hosts = HashSet::new();
    for (key, width, v6) in [
        (b"nodes".as_slice(), 26usize, false),
        (b"nodes6".as_slice(), 38usize, true),
    ] {
        let Some(nodes) = value.get(key) else {
            continue;
        };
        if let Some(raw) = nodes.as_bytes() {
            for chunk in raw.chunks_exact(width) {
                if out.len() + hosts.len() >= MAX_BOOTSTRAP_NODES {
                    break;
                }
                let Ok(id) = Id20::from_slice(&chunk[..20]) else {
                    continue;
                };
                let addr = if v6 {
                    let mut bytes = [0u8; 16];
                    bytes.copy_from_slice(&chunk[20..36]);
                    let ip = Ipv6Addr::from(bytes);
                    let port = u16::from_be_bytes([chunk[36], chunk[37]]);
                    if ip.is_unspecified() || port == 0 {
                        continue;
                    }
                    SocketAddr::new(ip.into(), port)
                } else {
                    let ip = Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]);
                    let port = u16::from_be_bytes([chunk[24], chunk[25]]);
                    if ip.is_unspecified() || port == 0 {
                        continue;
                    }
                    SocketAddr::new(ip.into(), port)
                };
                if seen_addrs.insert(addr) {
                    out.push((id, addr));
                }
            }
            continue;
        }

        let Some(entries) = nodes.as_list() else {
            continue;
        };
        for entry in entries {
            if out.len() + hosts.len() >= MAX_BOOTSTRAP_NODES {
                break;
            }
            let Some(pair) = entry.as_list() else {
                continue;
            };
            let Some(host) = pair.first().and_then(Value::as_str) else {
                continue;
            };
            let Some(port) = pair
                .get(1)
                .and_then(Value::as_int)
                .and_then(|p| u16::try_from(p).ok())
                .filter(|p| *p != 0)
            else {
                continue;
            };
            let host = host.trim();
            if host.is_empty() || host.contains('\0') {
                continue;
            }
            let id = {
                use sha1::{Digest, Sha1};
                let mut digest = Sha1::new();
                digest.update(host.as_bytes());
                digest.update(port.to_be_bytes());
                Id20::from_slice(&digest.finalize()[..20]).expect("sha1 is 20 bytes")
            };
            if let Ok(ip) = host.parse::<std::net::IpAddr>() {
                if ip.is_unspecified() || (v6 && !ip.is_ipv6()) {
                    continue;
                }
                if !seen_hosts.insert((host.to_string(), port)) {
                    continue;
                }
            } else if !seen_hosts.insert((host.to_string(), port)) {
                continue;
            }
            {
                hosts.push((id, host.to_string(), port));
            }
        }
    }
    (out, hosts)
}

fn get_str(value: &Value, key: &[u8]) -> Option<String> {
    value.get(key).and_then(|v| v.as_str().map(String::from))
}

fn validate_info(value: &Value) -> Result<ValidatedTorrentMetaV1Info, MetaError> {
    value.as_dict().ok_or(MetaError::BadInfo("info not dict"))?;

    let piece_length = value
        .get(b"piece length")
        .and_then(Value::as_int)
        .ok_or(MetaError::BadInfo("piece length missing"))?;
    if !(0..=u32::MAX as i64).contains(&piece_length) || piece_length == 0 {
        return Err(MetaError::BadInfo("piece length out of range"));
    }
    let piece_length = piece_length as u32;

    let pieces_bytes = value
        .get(b"pieces")
        .and_then(Value::as_bytes)
        .ok_or(MetaError::BadInfo("pieces missing"))?;
    if pieces_bytes.is_empty() || pieces_bytes.len() % 20 != 0 {
        return Err(MetaError::BadPieces);
    }

    let name = value
        .get(b"name")
        .and_then(Value::as_str)
        .ok_or(MetaError::BadUtf8 { field: "name" })?
        .to_string();
    // Sanitize against directory traversal; apply the same rules as file path components
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return Err(MetaError::BadInfo("torrent name unsafe"));
    }

    let private = value
        .get(b"private")
        .and_then(Value::as_int)
        .is_some_and(|n| n != 0);

    let (files, single_file_mode) = if let Some(list) = value.get(b"files").and_then(Value::as_list)
    {
        let mut files = Vec::with_capacity(list.len());
        for entry in list {
            entry
                .as_dict()
                .ok_or(MetaError::BadInfo("files entry not dict"))?;
            let length = entry
                .get(b"length")
                .and_then(Value::as_int)
                .ok_or(MetaError::BadInfo("file length missing"))?;
            if length < 0 {
                return Err(MetaError::BadInfo("file length negative"));
            }
            let path_list = entry
                .get(b"path")
                .and_then(Value::as_list)
                .ok_or(MetaError::BadInfo("file path missing"))?;
            let mut path_components = Vec::with_capacity(path_list.len());
            for c in path_list {
                let s = c
                    .as_str()
                    .ok_or(MetaError::BadUtf8 { field: "file path" })?;
                if s.is_empty() || s == "." || s == ".." || s.contains('/') || s.contains('\\') {
                    return Err(MetaError::BadInfo("file path component unsafe"));
                }
                path_components.push(s.to_string());
            }
            files.push(TorrentMetaInfo {
                path: path_components,
                length: length as u64,
            });
        }
        (files, false)
    } else {
        let length = value
            .get(b"length")
            .and_then(Value::as_int)
            .ok_or(MetaError::BadInfo("single-file length missing"))?;
        if length <= 0 {
            return Err(MetaError::ZeroLength);
        }
        (
            vec![TorrentMetaInfo {
                path: vec![name.clone()],
                length: length as u64,
            }],
            true,
        )
    };

    let mut total_len: u64 = 0;
    for f in &files {
        total_len = total_len
            .checked_add(f.length)
            .ok_or(MetaError::BadInfo("total file length overflow"))?;
    }
    if total_len == 0 {
        return Err(MetaError::ZeroLength);
    }

    Ok(ValidatedTorrentMetaV1Info {
        name,
        piece_length,
        pieces: pieces_bytes.to_vec(),
        private,
        files,
        single_file_mode,
    })
}

// BEP 52 v2 parsing

/// Build a v1-shaped facade from a parsed v2 info view, so the rest of the engine — which iterates `files`, `name`, `piece_length` and `single_file_mode` — keeps working uniformly. The synthesized facade has `pieces` empty and `private` defaulted to false: piece verification on pure-v2 torrents routes through `PieceVerifier::V2Merkle` instead of the SHA-1 `pieces` blob. Path-component reconciliation: the BEP 52 `file tree` includes the torrent name as the root key for multi-file torrents, while v1's per-file `path` lists are relative to that name. We normalize the v2 paths to match v1 semantics so storage layout helpers stay version-agnostic
fn synthesize_v1_facade_from_v2(v2: &ValidatedTorrentMetaV2Info) -> ValidatedTorrentMetaV1Info {
    let single_file_mode =
        v2.files.len() == 1 && v2.files[0].path.len() == 1 && v2.files[0].path[0] == v2.name;

    let files = v2
        .files
        .iter()
        .map(|f| {
            // For multi-file torrents v2 paths start with the torrent name; strip it to match the v1 `info.files[].path` convention
            let path = if !single_file_mode && f.path.first().is_some_and(|p| p == &v2.name) {
                f.path[1..].to_vec()
            } else {
                f.path.clone()
            };
            TorrentMetaInfo {
                path,
                length: f.length,
            }
        })
        .collect();
    ValidatedTorrentMetaV1Info {
        name: v2.name.clone(),
        piece_length: v2.piece_length,
        pieces: Vec::new(),
        private: v2.private,
        files,
        single_file_mode,
    }
}

/// Parse the v2 view of an `info` dict. Caller has already verified the presence of `meta version=2` and `file tree`
fn validate_info_v2(value: &Value) -> Result<ValidatedTorrentMetaV2Info, MetaError> {
    value
        .as_dict()
        .ok_or(MetaError::BadInfoV2("info not dict"))?;

    let meta_version = value
        .get(b"meta version")
        .and_then(Value::as_int)
        .ok_or(MetaError::BadInfoV2("meta version missing"))?;
    if meta_version != 2 {
        return Err(MetaError::BadInfoV2("unsupported meta version"));
    }

    let piece_length = value
        .get(b"piece length")
        .and_then(Value::as_int)
        .ok_or(MetaError::BadInfoV2("piece length missing"))?;
    if !(0..=u32::MAX as i64).contains(&piece_length) || piece_length == 0 {
        return Err(MetaError::BadInfoV2("piece length out of range"));
    }
    let piece_length = piece_length as u32;
    // BEP 52: piece length must be a power of two >= 16 KiB
    if piece_length < 16 * 1024 || !piece_length.is_power_of_two() {
        return Err(MetaError::BadInfoV2(
            "piece length must be a power of two and >= 16 KiB",
        ));
    }

    let name = value
        .get(b"name")
        .and_then(Value::as_str)
        .ok_or(MetaError::BadUtf8 { field: "v2 name" })?
        .to_string();
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return Err(MetaError::BadInfoV2("v2 name unsafe"));
    }

    let file_tree_value = value
        .get(b"file tree")
        .ok_or(MetaError::BadInfoV2("file tree missing"))?;

    let file_tree = parse_file_tree(file_tree_value)?;
    let mut files = Vec::new();
    flatten_file_tree(&file_tree, &mut Vec::new(), &mut files)?;
    if files.is_empty() {
        return Err(MetaError::BadInfoV2("file tree has no files"));
    }
    let total: u64 = files
        .iter()
        .try_fold(0u64, |acc, f| acc.checked_add(f.length))
        .ok_or(MetaError::BadInfoV2("total file length overflow"))?;
    if total == 0 {
        return Err(MetaError::ZeroLength);
    }

    // BEP 27 private flag at info-dict level. Same int-as-bool decoding as v1
    let private = value
        .get(b"private")
        .and_then(Value::as_int)
        .map(|i| i != 0)
        .unwrap_or(false);

    Ok(ValidatedTorrentMetaV2Info {
        name,
        piece_length,
        private,
        files,
    })
}

/// Recursively decode the `file tree` dict. Each leaf is a child dict with a single key `""` (empty bencoded string) holding `{length, pieces root?}`
fn parse_file_tree(value: &Value) -> Result<FileTreeNode, MetaError> {
    let dict = value
        .as_dict()
        .ok_or(MetaError::BadInfoV2("file tree node not dict"))?;

    // Leaf: a dict with the single empty-string key
    if dict.len() == 1 && dict[0].0.is_empty() {
        let leaf = &dict[0].1;
        leaf.as_dict()
            .ok_or(MetaError::BadInfoV2("file tree leaf payload not dict"))?;
        let length = leaf
            .get(b"length")
            .and_then(Value::as_int)
            .ok_or(MetaError::BadInfoV2("file tree leaf length missing"))?;
        if length < 0 {
            return Err(MetaError::BadInfoV2("file tree leaf length negative"));
        }
        let pieces_root = leaf
            .get(b"pieces root")
            .and_then(Value::as_bytes)
            .map(Id32::from_slice)
            .transpose()
            .map_err(|_| MetaError::BadInfoV2("pieces root not 32 bytes"))?;
        // Files of length > 0 must have a pieces_root
        if length > 0 && pieces_root.is_none() {
            return Err(MetaError::BadInfoV2(
                "file tree leaf missing pieces root for non-empty file",
            ));
        }
        return Ok(FileTreeNode::File {
            length: length as u64,
            pieces_root,
        });
    }

    // Directory: each entry's key is a path component
    let mut children = std::collections::BTreeMap::new();
    for (k, v) in dict {
        let name = std::str::from_utf8(k)
            .map_err(|_| MetaError::BadUtf8 {
                field: "file tree key",
            })?
            .to_string();
        if name.is_empty()
            || name == "."
            || name == ".."
            || name.contains('/')
            || name.contains('\\')
        {
            return Err(MetaError::BadInfoV2("file tree key unsafe"));
        }
        children.insert(name, parse_file_tree(v)?);
    }
    Ok(FileTreeNode::Dir(children))
}

fn flatten_file_tree(
    node: &FileTreeNode,
    path: &mut Vec<String>,
    out: &mut Vec<TorrentMetaInfoV2>,
) -> Result<(), MetaError> {
    match node {
        FileTreeNode::Dir(children) => {
            for (name, child) in children {
                path.push(name.clone());
                flatten_file_tree(child, path, out)?;
                path.pop();
            }
            Ok(())
        }
        FileTreeNode::File {
            length,
            pieces_root,
        } => {
            // Skip zero-length files in the flat list (BEP 52 allows them and they have no Merkle root)
            if *length == 0 {
                return Ok(());
            }
            let root =
                pieces_root.ok_or(MetaError::BadInfoV2("non-empty file missing pieces root"))?;
            out.push(TorrentMetaInfoV2 {
                path: path.clone(),
                length: *length,
                pieces_root: root,
            });
            Ok(())
        }
    }
}

/// Parse top-level `piece layers` dict (BEP 52). Each key is a 32-byte pieces-root; each value is the concatenated SHA-256 hashes of the piece-aligned chunks below it (one 32-byte hash per piece)
fn parse_piece_layers(top: &Value) -> Result<std::collections::BTreeMap<Id32, Vec<u8>>, MetaError> {
    let Some(value) = top.get(b"piece layers") else {
        return Ok(std::collections::BTreeMap::new());
    };
    let dict = value
        .as_dict()
        .ok_or(MetaError::BadInfoV2("piece layers not dict"))?;
    let mut out = std::collections::BTreeMap::new();
    for (k, v) in dict {
        let root = Id32::from_slice(k)
            .map_err(|_| MetaError::BadInfoV2("piece layers key not 32 bytes"))?;
        let bytes = v
            .as_bytes()
            .ok_or(MetaError::BadInfoV2("piece layers value not bytes"))?;
        if bytes.len() % 32 != 0 {
            return Err(MetaError::BadInfoV2(
                "piece layers value length not multiple of 32",
            ));
        }
        out.insert(root, bytes.to_vec());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn synth_single_file(name: &str, piece_len: u32, data_len: u64) -> Vec<u8> {
        // Craft a minimal valid single-file .torrent (announce-less). Piece hashes are zeroed; sufficient for parser tests
        let piece_count = data_len.div_ceil(piece_len as u64);
        let pieces = vec![0u8; (piece_count * 20) as usize];
        // Build info dict via the encoder to guarantee canonical output
        let info = Value::Dict(vec![
            (b"length".to_vec(), Value::Int(data_len as i64)),
            (b"name".to_vec(), Value::Bytes(name.as_bytes().to_vec())),
            (b"piece length".to_vec(), Value::Int(piece_len as i64)),
            (b"pieces".to_vec(), Value::Bytes(pieces)),
        ]);
        let top = Value::Dict(vec![
            (
                b"announce".to_vec(),
                Value::Bytes(b"http://tracker/announce".to_vec()),
            ),
            (b"info".to_vec(), info),
        ]);
        super::super::super::bencode::encode_to_vec(&top)
    }

    #[test]
    fn parse_single_file() {
        let bytes = synth_single_file("hello.bin", 16 * 1024, 100_000);
        let meta = parse_torrent(&bytes).unwrap();
        assert_eq!(meta.info.name, "hello.bin");
        assert_eq!(meta.info.piece_length, 16 * 1024);
        assert_eq!(meta.info.files.len(), 1);
        assert_eq!(meta.info.files[0].length, 100_000);
        assert!(meta.info.single_file_mode);
        assert_eq!(meta.announce.as_deref(), Some("http://tracker/announce"));
        // info-hash must be stable regardless of re-encode order (dict keys are sorted by the encoder already)
        assert_ne!(meta.info_hash.to_hex(), "0".repeat(40));
    }

    #[test]
    fn parse_multi_file_rejects_unsafe_path() {
        let info = Value::Dict(vec![
            (
                b"files".to_vec(),
                Value::List(vec![Value::Dict(vec![
                    (b"length".to_vec(), Value::Int(10)),
                    (
                        b"path".to_vec(),
                        Value::List(vec![
                            Value::Bytes(b"..".to_vec()),
                            Value::Bytes(b"escape".to_vec()),
                        ]),
                    ),
                ])]),
            ),
            (b"name".to_vec(), Value::Bytes(b"multi".to_vec())),
            (b"piece length".to_vec(), Value::Int(16 * 1024)),
            (b"pieces".to_vec(), Value::Bytes(vec![0; 20])),
        ]);
        let top = Value::Dict(vec![(b"info".to_vec(), info)]);
        let bytes = super::super::super::bencode::encode_to_vec(&top);
        assert!(matches!(parse_torrent(&bytes), Err(MetaError::BadInfo(_))));
    }

    /// Build a hybrid v1+v2 .torrent with a single-file `file tree`. Hashes are zero-filled; sufficient for parser-shape tests
    fn synth_hybrid_single_file(name: &str, piece_len: u32, data_len: u64) -> Vec<u8> {
        let piece_count = data_len.div_ceil(piece_len as u64);
        let v1_pieces = vec![0u8; (piece_count * 20) as usize];
        let pieces_root = Id32([0x11u8; 32]);
        // file tree: { name: { "": { length, pieces root } } }
        let file_leaf = Value::Dict(vec![(
            b"".to_vec(),
            Value::Dict(vec![
                (b"length".to_vec(), Value::Int(data_len as i64)),
                (
                    b"pieces root".to_vec(),
                    Value::Bytes(pieces_root.0.to_vec()),
                ),
            ]),
        )]);
        let file_tree = Value::Dict(vec![(name.as_bytes().to_vec(), file_leaf)]);
        let info = Value::Dict(vec![
            (b"file tree".to_vec(), file_tree),
            (b"length".to_vec(), Value::Int(data_len as i64)),
            (b"meta version".to_vec(), Value::Int(2)),
            (b"name".to_vec(), Value::Bytes(name.as_bytes().to_vec())),
            (b"piece length".to_vec(), Value::Int(piece_len as i64)),
            (b"pieces".to_vec(), Value::Bytes(v1_pieces)),
        ]);
        // piece layers entry only required when file > one piece
        let layers = if piece_count > 1 {
            let bytes = vec![0u8; (piece_count * 32) as usize];
            Value::Dict(vec![(pieces_root.0.to_vec(), Value::Bytes(bytes))])
        } else {
            Value::Dict(vec![])
        };
        let top = Value::Dict(vec![
            (b"info".to_vec(), info),
            (b"piece layers".to_vec(), layers),
        ]);
        super::super::super::bencode::encode_to_vec(&top)
    }

    #[test]
    fn parse_hybrid_torrent() {
        let bytes = synth_hybrid_single_file("hybrid.bin", 16 * 1024, 100_000);
        let meta = parse_torrent(&bytes).unwrap();
        assert_eq!(meta.meta_version, MetaVersion::Hybrid);
        assert!(meta.info_hash_v2.is_some());
        let v2 = meta.info_v2.as_ref().expect("v2 info present");
        assert_eq!(v2.name, "hybrid.bin");
        assert_eq!(v2.piece_length, 16 * 1024);
        assert_eq!(v2.files.len(), 1);
        assert_eq!(v2.files[0].length, 100_000);
        assert_eq!(v2.files[0].path, vec!["hybrid.bin"]);
        // 100_000 bytes / 16 KiB pieces = 7 pieces; piece layers must contain 7 * 32 bytes for the file's pieces_root
        let layer = meta
            .piece_layers
            .get(&v2.files[0].pieces_root)
            .expect("piece layers entry present");
        assert_eq!(layer.len(), 7 * 32);
    }

    #[test]
    fn parse_hybrid_torrent_small_file_no_layers() {
        // Single-piece file => piece layers may be empty for that root
        let bytes = synth_hybrid_single_file("tiny.bin", 16 * 1024, 1024);
        let meta = parse_torrent(&bytes).unwrap();
        assert_eq!(meta.meta_version, MetaVersion::Hybrid);
        assert!(meta.piece_layers.is_empty());
    }

    #[test]
    fn pure_v2_small_file_parses_without_layers() {
        // Pure-v2 .torrent: single file <= piece_length needs no piece-layer entries (BEP 52: only files larger than one piece have layers)
        let pieces_root = Id32([0x22u8; 32]);
        let file_leaf = Value::Dict(vec![(
            b"".to_vec(),
            Value::Dict(vec![
                (b"length".to_vec(), Value::Int(2048)),
                (
                    b"pieces root".to_vec(),
                    Value::Bytes(pieces_root.0.to_vec()),
                ),
            ]),
        )]);
        let info = Value::Dict(vec![
            (
                b"file tree".to_vec(),
                Value::Dict(vec![(b"a".to_vec(), file_leaf)]),
            ),
            (b"meta version".to_vec(), Value::Int(2)),
            (b"name".to_vec(), Value::Bytes(b"a".to_vec())),
            (b"piece length".to_vec(), Value::Int(16 * 1024)),
        ]);
        let top = Value::Dict(vec![(b"info".to_vec(), info)]);
        let bytes = super::super::super::bencode::encode_to_vec(&top);
        let meta = parse_torrent(&bytes).expect("pure-v2 .torrent should parse");
        assert_eq!(meta.meta_version, MetaVersion::V2);
        assert!(meta.info_v2.is_some());
        assert!(meta.info_hash_v2.is_some());
    }

    #[test]
    fn pure_v2_large_file_without_layers_rejected() {
        // Pure-v2 .torrent missing required piece-layer entries for a file larger than one piece must be rejected (BEP 52); only magnet-v2 can defer this via HASH_REQUEST
        let pieces_root = Id32([0x55u8; 32]);
        let file_leaf = Value::Dict(vec![(
            b"".to_vec(),
            Value::Dict(vec![
                // length > piece_length forces piece-layer requirement
                (b"length".to_vec(), Value::Int(64 * 1024)),
                (
                    b"pieces root".to_vec(),
                    Value::Bytes(pieces_root.0.to_vec()),
                ),
            ]),
        )]);
        let info = Value::Dict(vec![
            (
                b"file tree".to_vec(),
                Value::Dict(vec![(b"a".to_vec(), file_leaf)]),
            ),
            (b"meta version".to_vec(), Value::Int(2)),
            (b"name".to_vec(), Value::Bytes(b"a".to_vec())),
            (b"piece length".to_vec(), Value::Int(16 * 1024)),
        ]);
        let top = Value::Dict(vec![(b"info".to_vec(), info)]);
        let bytes = super::super::super::bencode::encode_to_vec(&top);
        assert!(matches!(
            parse_torrent(&bytes),
            Err(MetaError::MissingPieceLayers)
        ));
    }

    #[test]
    fn v2_rejects_non_power_of_two_piece_length() {
        // Build a hybrid-shaped torrent with a 24 KiB piece length to trigger the v2 power-of-two validation before v1 (validate_info accepts any)
        let bad_piece_len: u32 = 24 * 1024;
        let pieces_root = Id32([0x33u8; 32]);
        let file_leaf = Value::Dict(vec![(
            b"".to_vec(),
            Value::Dict(vec![
                (b"length".to_vec(), Value::Int(bad_piece_len as i64)),
                (
                    b"pieces root".to_vec(),
                    Value::Bytes(pieces_root.0.to_vec()),
                ),
            ]),
        )]);
        let info = Value::Dict(vec![
            (
                b"file tree".to_vec(),
                Value::Dict(vec![(b"f".to_vec(), file_leaf)]),
            ),
            (b"length".to_vec(), Value::Int(bad_piece_len as i64)),
            (b"meta version".to_vec(), Value::Int(2)),
            (b"name".to_vec(), Value::Bytes(b"f".to_vec())),
            (b"piece length".to_vec(), Value::Int(bad_piece_len as i64)),
            (b"pieces".to_vec(), Value::Bytes(vec![0u8; 20])),
        ]);
        let top = Value::Dict(vec![
            (b"info".to_vec(), info),
            (b"piece layers".to_vec(), Value::Dict(vec![])),
        ]);
        let bytes = super::super::super::bencode::encode_to_vec(&top);
        let err = parse_torrent(&bytes).unwrap_err();
        assert!(matches!(err, MetaError::BadInfoV2(_)), "got {err:?}");
    }

    #[test]
    fn announce_infohashes_dedup_pure_v2() {
        let v2 = Id32([0xccu8; 32]);
        // Pure-v2: returns only one hash (the truncated v2)
        let h = TorrentInfoHashes {
            v1: None,
            v2: Some(v2),
        };
        assert_eq!(h.announce_infohashes().len(), 1);
        assert_eq!(h.announce_infohashes()[0], v2.truncate_to_id20());

        // Hybrid: returns both
        let v1 = Id20([0x11u8; 20]);
        let h = TorrentInfoHashes {
            v1: Some(v1),
            v2: Some(v2),
        };
        let all = h.announce_infohashes();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0], v1);
        assert_eq!(all[1], v2.truncate_to_id20());

        // v1-only: returns one
        let h = TorrentInfoHashes {
            v1: Some(v1),
            v2: None,
        };
        assert_eq!(h.announce_infohashes(), vec![v1]);
    }

    #[test]
    fn parse_bep5_list_bootstrap_nodes() {
        let info = Value::Dict(vec![
            (b"length".to_vec(), Value::Int(1)),
            (b"name".to_vec(), Value::Bytes(b"x".to_vec())),
            (b"piece length".to_vec(), Value::Int(1)),
            (b"pieces".to_vec(), Value::Bytes(vec![0u8; 20])),
        ]);
        let nodes = Value::List(vec![
            Value::List(vec![Value::Bytes(b"127.0.0.1".to_vec()), Value::Int(6881)]),
            Value::List(vec![
                Value::Bytes(b"2001:db8::1".to_vec()),
                Value::Int(6882),
            ]),
            Value::List(vec![
                Value::Bytes(b"router.example.org".to_vec()),
                Value::Int(6883),
            ]),
        ]);
        let top = Value::Dict(vec![(b"info".to_vec(), info), (b"nodes".to_vec(), nodes)]);
        let bytes = super::super::super::bencode::encode_to_vec(&top);
        let meta = parse_torrent(&bytes).unwrap();
        assert!(meta.bootstrap_nodes.is_empty());
        assert_eq!(meta.bootstrap_hosts.len(), 3);
        let v4_id = {
            let mut data = b"127.0.0.1".to_vec();
            data.extend_from_slice(&6881u16.to_be_bytes());
            sha1(&data)
        };
        let v6_id = {
            let mut data = b"2001:db8::1".to_vec();
            data.extend_from_slice(&6882u16.to_be_bytes());
            sha1(&data)
        };
        assert!(meta.bootstrap_hosts.iter().any(|(id, host, port)| {
            *id == v4_id && host == "127.0.0.1" && *port == 6881
        }));
        assert!(meta.bootstrap_hosts.iter().any(|(id, host, port)| {
            *id == v6_id && host == "2001:db8::1" && *port == 6882
        }));
        let host_id = {
            let mut data = b"router.example.org".to_vec();
            data.extend_from_slice(&6883u16.to_be_bytes());
            sha1(&data)
        };
        assert!(meta.bootstrap_hosts.contains(&(
            host_id,
            "router.example.org".to_string(),
            6883,
        )));
    }
}
