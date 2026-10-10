use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use super::super::bencode::Value;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetaVersion {
    V1,
    V2,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TorrentInfoHashes {
    pub v1: Option<Id20>,
    pub v2: Option<Id32>,
}

impl TorrentInfoHashes {
    pub fn announce_infohashes(&self) -> Vec<Id20> {
        let mut out = Vec::with_capacity(2);
        if let Some(v1) = self.v1 {
            out.push(v1);
        }
        if let Some(v2) = self.v2 {
            let trunc = v2.truncate_to_id20();
            if !out.contains(&trunc) {
                out.push(trunc);
            }
        }
        out
    }
}

#[derive(Debug, Clone)]
pub struct TorrentMeta {
    pub info: ValidatedTorrentMetaV1Info,
    pub announce: Option<String>,
    pub announce_list: Vec<Vec<String>>,
    pub obfuscate_announce_list: Vec<Vec<String>>,
    pub url_list: Vec<String>,
    pub bootstrap_nodes: Vec<(Id20, SocketAddr)>,
    pub bootstrap_hosts: Vec<(String, u16)>,
    pub comment: Option<String>,
    pub creation_date: Option<i64>,
    pub info_hash: Id20,
    pub info_v2: Option<ValidatedTorrentMetaV2Info>,
    pub info_hash_v2: Option<Id32>,
    pub meta_version: MetaVersion,
    pub piece_layers: std::collections::BTreeMap<Id32, Vec<u8>>,
    pub info_bytes: Vec<u8>,
}

impl TorrentMeta {
    pub fn info_hashes(&self) -> TorrentInfoHashes {
        TorrentInfoHashes {
            v1: self.meta_version.has_v1().then_some(self.info_hash),
            v2: self.info_hash_v2,
        }
    }

    pub fn announce_infohashes(&self) -> Vec<Id20> {
        self.info_hashes().announce_infohashes()
    }
}

#[derive(Debug, Clone)]
pub struct ValidatedTorrentMetaV1Info {
    pub name: String,
    pub piece_length: u32,
    pub pieces: Vec<u8>,
    pub private: bool,
    pub files: Vec<TorrentMetaInfo>,
    pub single_file_mode: bool,
}

#[derive(Debug, Clone)]
pub struct TorrentMetaInfo {
    pub path: Vec<String>,
    pub length: u64,
    pub padding: bool,
}

#[derive(Debug, Clone)]
pub struct FileDetails {
    pub filename: String,
    pub len: u64,
    pub padding: bool,
}

impl ValidatedTorrentMetaV1Info {
    pub fn iter_file_details(&self) -> impl Iterator<Item = FileDetails> + '_ {
        self.files.iter().map(|f| FileDetails {
            filename: f.path.join("/"),
            len: f.length,
            padding: f.padding,
        })
    }

    pub fn selectable_file_indices(&self) -> Vec<usize> {
        self.files
            .iter()
            .enumerate()
            .filter(|(_, f)| !f.padding)
            .map(|(idx, _)| idx)
            .collect()
    }

    pub fn piece_count(&self) -> u32 {
        if self.pieces.is_empty() {
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

#[derive(Debug, Clone)]
pub struct TorrentMetaInfoV2 {
    pub path: Vec<String>,
    pub length: u64,
    pub pieces_root: Id32,
}

#[derive(Debug, Clone)]
pub enum FileTreeNode {
    Dir(Vec<(String, FileTreeNode)>),
    File {
        length: u64,
        pieces_root: Option<Id32>,
    },
}

#[derive(Debug, Clone)]
pub struct ValidatedTorrentMetaV2Info {
    pub name: String,
    pub piece_length: u32,
    pub private: bool,
    pub files: Vec<TorrentMetaInfoV2>,
    pub empty_files: Vec<Vec<String>>,
}

impl ValidatedTorrentMetaV2Info {
    pub fn total_length(&self) -> u64 {
        self.files.iter().map(|f| f.length).sum()
    }
}

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

pub fn parse_torrent(bytes: &[u8]) -> Result<TorrentMeta, MetaError> {
    let value = super::super::bencode::decode_all(bytes)?;
    value
        .as_dict()
        .ok_or(MetaError::BadInfo("top-level not dict"))?;

    let announce = get_str(&value, b"announce");
    let tier_list = |key: &[u8]| {
        value
            .get(key)
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
            .unwrap_or_default()
    };
    let announce_list = tier_list(b"announce-list");
    let obfuscate_announce_list = tier_list(b"obfuscate-announce-list");
    let url_list = parse_url_list(&value);
    let (bootstrap_nodes, bootstrap_hosts) = parse_bootstrap_nodes(&value);
    let comment = get_str(&value, b"comment");
    let creation_date = value.get(b"creation date").and_then(Value::as_int);

    let info_raw = super::super::bencode::top_level_field_span(bytes, b"info")
        .map(|span| &bytes[span])
        .ok_or(MetaError::MissingInfo)?;
    let info_value = value.get(b"info").ok_or(MetaError::MissingInfo)?;

    info_value
        .as_dict()
        .ok_or(MetaError::BadInfo("info not dict"))?;

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
        let v2 = validate_info_v2(info_value)?;
        let info_hash_v2_val = sha256(info_raw);
        let piece_layers = parse_piece_layers(&value)?;
        let info = synthesize_v1_facade_from_v2(&v2);
        for file in &v2.files {
            if file.length <= v2.piece_length as u64 {
                continue;
            }
            let root = file.pieces_root;
            if !piece_layers.contains_key(&root) {
                return Err(MetaError::MissingPieceLayers);
            }
        }
        let wire_hash = info_hash_v2_val.truncate_to_id20();
        return Ok(TorrentMeta {
            info,
            announce,
            announce_list,
            obfuscate_announce_list,
            url_list,
            bootstrap_nodes,
            bootstrap_hosts,
            comment,
            creation_date,
            info_hash: wire_hash,
            info_v2: Some(v2),
            info_hash_v2: Some(info_hash_v2_val),
            meta_version,
            piece_layers,
            info_bytes: info_raw.to_vec(),
        });
    }

    let info_hash = sha1(info_raw);
    let mut info = validate_info(info_value)?;

    let (info_v2, info_hash_v2) = if has_v2 {
        let v2 = validate_info_v2(info_value)?;
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
        obfuscate_announce_list,
        url_list,
        bootstrap_nodes,
        bootstrap_hosts,
        comment,
        creation_date,
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

type BootstrapNodes = (Vec<(Id20, SocketAddr)>, Vec<(String, u16)>);

fn parse_bootstrap_nodes(value: &Value) -> BootstrapNodes {
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
                    if !crate::dht::public_dht_endpoint(SocketAddr::new(ip.into(), port)) {
                        continue;
                    }
                    SocketAddr::new(ip.into(), port)
                } else {
                    let ip = Ipv4Addr::new(chunk[20], chunk[21], chunk[22], chunk[23]);
                    let port = u16::from_be_bytes([chunk[24], chunk[25]]);
                    if !crate::dht::public_dht_endpoint(SocketAddr::new(ip.into(), port)) {
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
            if let Ok(ip) = host.parse::<IpAddr>() {
                if !crate::dht::public_dht_endpoint(SocketAddr::new(ip, port))
                    || (v6 && !ip.is_ipv6())
                {
                    continue;
                }
                if !seen_hosts.insert((host.to_string(), port)) {
                    continue;
                }
            } else if !seen_hosts.insert((host.to_string(), port)) {
                continue;
            }
            hosts.push((host.to_string(), port));
        }
    }
    (out, hosts)
}

fn text_with_utf8_alt(value: &Value, alt: &[u8], key: &[u8]) -> Option<String> {
    if let Some(s) = value.get(alt).and_then(Value::as_str) {
        return Some(s.to_string());
    }
    value
        .get(key)
        .and_then(Value::as_bytes)
        .map(|b| String::from_utf8_lossy(b).into_owned())
}

fn get_str(value: &Value, key: &[u8]) -> Option<String> {
    value.get(key).and_then(|v| v.as_str().map(String::from))
}

const MAX_PIECES: u64 = 1 << 22;
const MAX_PIECE_LENGTH: u32 = 256 * 1024 * 1024;

fn check_piece_count(total_len: u64, piece_length: u32) -> Result<(), MetaError> {
    if piece_length > MAX_PIECE_LENGTH {
        return Err(MetaError::BadInfo("piece length too large"));
    }
    if total_len.div_ceil(piece_length as u64) > MAX_PIECES {
        return Err(MetaError::BadInfo("too many pieces"));
    }
    Ok(())
}

pub(crate) fn is_safe_component(s: &str) -> bool {
    if s.is_empty() || s == "." || s == ".." || s.contains(['/', '\\', '\0']) {
        return false;
    }
    let b = s.as_bytes();
    !(cfg!(windows) && b.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':')
}

pub fn sanitize_windows_component(s: &str) -> String {
    let mut out: String = s
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*') {
                '_'
            } else {
                c
            }
        })
        .collect();
    out.truncate(out.trim_end_matches(['.', ' ']).len());
    if out.is_empty() {
        return "_".into();
    }
    let stem = out.split('.').next().unwrap_or("").to_ascii_uppercase();
    let reserved = matches!(stem.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || ((stem.starts_with("COM") || stem.starts_with("LPT"))
            && stem.len() == 4
            && matches!(stem.as_bytes()[3], b'1'..=b'9'));
    if reserved {
        out.insert(0, '_');
    }
    out
}

pub fn fs_component(s: &str) -> std::borrow::Cow<'_, str> {
    if cfg!(windows) {
        std::borrow::Cow::Owned(sanitize_windows_component(s))
    } else {
        std::borrow::Cow::Borrowed(s)
    }
}

fn disk_path_key(path: &[String]) -> String {
    let joined = path
        .iter()
        .map(|c| fs_component(c).into_owned())
        .collect::<Vec<_>>()
        .join("/");
    if cfg!(any(windows, target_os = "macos")) {
        joined.to_lowercase()
    } else {
        joined
    }
}

fn numbered_name(name: &str, n: usize) -> String {
    match name.rfind('.') {
        Some(dot) if dot > 0 => format!("{} ({n}){}", &name[..dot], &name[dot..]),
        _ => format!("{name} ({n})"),
    }
}

pub fn dedupe_paths(paths: &mut [Vec<String>], skip: &[bool]) {
    use std::collections::HashSet;
    let skipped_at: Vec<bool> = (0..paths.len())
        .map(|i| skip.get(i).copied().unwrap_or(false) || paths[i].is_empty())
        .collect();
    let skipped = |i: usize| skipped_at[i];
    let mut taken: HashSet<String> = (0..paths.len())
        .filter(|&i| !skipped(i))
        .map(|i| disk_path_key(&paths[i]))
        .collect();
    let mut seen: HashSet<String> = HashSet::with_capacity(taken.len());
    for (i, &is_skipped) in skipped_at.iter().enumerate() {
        if is_skipped {
            continue;
        }
        let key = disk_path_key(&paths[i]);
        if seen.insert(key) {
            continue;
        }
        let Some(last) = paths[i].last().cloned() else {
            continue;
        };
        for n in 1usize.. {
            let mut candidate = paths[i].clone();
            if let Some(slot) = candidate.last_mut() {
                *slot = numbered_name(&last, n);
            }
            let ck = disk_path_key(&candidate);
            if taken.insert(ck.clone()) {
                seen.insert(ck);
                paths[i] = candidate;
                break;
            }
        }
    }
}

fn dedupe_file_list(files: &mut [TorrentMetaInfo]) {
    let skip: Vec<bool> = files.iter().map(|f| f.padding).collect();
    let mut paths: Vec<Vec<String>> = files
        .iter_mut()
        .map(|f| std::mem::take(&mut f.path))
        .collect();
    dedupe_paths(&mut paths, &skip);
    for (f, p) in files.iter_mut().zip(paths) {
        f.path = p;
    }
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

    let name = text_with_utf8_alt(value, b"name.utf-8", b"name")
        .ok_or(MetaError::BadUtf8 { field: "name" })?;
    if !is_safe_component(&name) {
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
            let path_list = entry
                .get(b"path.utf-8")
                .and_then(Value::as_list)
                .filter(|l| l.len() == path_list.len() && l.iter().all(|c| c.as_str().is_some()))
                .unwrap_or(path_list);
            let mut path_components = Vec::with_capacity(path_list.len());
            for c in path_list {
                let s = c
                    .as_bytes()
                    .map(String::from_utf8_lossy)
                    .ok_or(MetaError::BadUtf8 { field: "file path" })?;
                if !is_safe_component(&s) {
                    return Err(MetaError::BadInfo("file path component unsafe"));
                }
                path_components.push(s.into_owned());
            }
            let padding = entry
                .get(b"attr")
                .and_then(Value::as_bytes)
                .is_some_and(|attr| attr.contains(&b'p'));
            files.push(TorrentMetaInfo {
                path: path_components,
                length: length as u64,
                padding,
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
                padding: false,
            }],
            true,
        )
    };

    let mut files = files;
    dedupe_file_list(&mut files);

    let mut total_len: u64 = 0;
    for f in &files {
        total_len = total_len
            .checked_add(f.length)
            .ok_or(MetaError::BadInfo("total file length overflow"))?;
    }
    if total_len == 0 {
        return Err(MetaError::ZeroLength);
    }
    check_piece_count(total_len, piece_length)?;
    if pieces_bytes.len() as u64 != total_len.div_ceil(piece_length as u64) * 20 {
        return Err(MetaError::BadPieces);
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

fn synthesize_v1_facade_from_v2(v2: &ValidatedTorrentMetaV2Info) -> ValidatedTorrentMetaV1Info {
    let single_file_mode = v2.empty_files.is_empty()
        && v2.files.len() == 1
        && v2.files[0].path.len() == 1
        && v2.files[0].path[0] == v2.name;

    let mut files = Vec::with_capacity(v2.files.len());
    let mut offset = 0u64;
    for f in &v2.files {
        let pad = v2_alignment_padding(offset, f.length, v2.piece_length);
        if pad > 0 {
            files.push(TorrentMetaInfo {
                path: vec![".pad".into(), pad.to_string()],
                length: pad,
                padding: true,
            });
            offset += pad;
        }
        let path = if !single_file_mode && f.path.first().is_some_and(|p| p == &v2.name) {
            f.path[1..].to_vec()
        } else {
            f.path.clone()
        };
        files.push(TorrentMetaInfo {
            path,
            length: f.length,
            padding: false,
        });
        offset += f.length;
    }
    for path in &v2.empty_files {
        let path = if !single_file_mode && path.first().is_some_and(|p| p == &v2.name) {
            path[1..].to_vec()
        } else {
            path.clone()
        };
        files.push(TorrentMetaInfo {
            path,
            length: 0,
            padding: false,
        });
    }
    dedupe_file_list(&mut files);
    ValidatedTorrentMetaV1Info {
        name: v2.name.clone(),
        piece_length: v2.piece_length,
        pieces: Vec::new(),
        private: v2.private,
        files,
        single_file_mode,
    }
}

pub(crate) fn v2_alignment_padding(offset: u64, file_length: u64, piece_length: u32) -> u64 {
    if file_length == 0 || piece_length == 0 {
        return 0;
    }
    match offset % piece_length as u64 {
        0 => 0,
        rem => piece_length as u64 - rem,
    }
}

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
    if piece_length < 16 * 1024 || !piece_length.is_power_of_two() {
        return Err(MetaError::BadInfoV2(
            "piece length must be a power of two and >= 16 KiB",
        ));
    }

    let name = text_with_utf8_alt(value, b"name.utf-8", b"name")
        .ok_or(MetaError::BadUtf8 { field: "v2 name" })?;
    if !is_safe_component(&name) {
        return Err(MetaError::BadInfoV2("v2 name unsafe"));
    }

    let file_tree_value = value
        .get(b"file tree")
        .ok_or(MetaError::BadInfoV2("file tree missing"))?;

    let file_tree = parse_file_tree(file_tree_value)?;
    let mut files = Vec::new();
    let mut empty_files = Vec::new();
    flatten_file_tree(&file_tree, &mut Vec::new(), &mut files, &mut empty_files)?;
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
    check_piece_count(total, piece_length)?;

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
        empty_files,
    })
}

fn parse_file_tree(value: &Value) -> Result<FileTreeNode, MetaError> {
    let dict = value
        .as_dict()
        .ok_or(MetaError::BadInfoV2("file tree node not dict"))?;

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

    let mut children: Vec<(String, FileTreeNode)> = Vec::with_capacity(dict.len());
    let mut used = std::collections::HashSet::<String>::with_capacity(dict.len());
    for (k, v) in dict {
        let name = String::from_utf8_lossy(k).into_owned();
        if !is_safe_component(&name) {
            return Err(MetaError::BadInfoV2("file tree key unsafe"));
        }
        let mut name = name;
        let base = name.clone();
        let mut n = 0usize;
        while used.contains(&name) {
            n += 1;
            name = numbered_name(&base, n);
        }
        used.insert(name.clone());
        children.push((name, parse_file_tree(v)?));
    }
    Ok(FileTreeNode::Dir(children))
}

fn flatten_file_tree(
    node: &FileTreeNode,
    path: &mut Vec<String>,
    out: &mut Vec<TorrentMetaInfoV2>,
    empty: &mut Vec<Vec<String>>,
) -> Result<(), MetaError> {
    match node {
        FileTreeNode::Dir(children) => {
            for (name, child) in children {
                path.push(name.clone());
                flatten_file_tree(child, path, out, empty)?;
                path.pop();
            }
            Ok(())
        }
        FileTreeNode::File {
            length,
            pieces_root,
        } => {
            if *length == 0 {
                empty.push(path.clone());
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
        let piece_count = data_len.div_ceil(piece_len as u64);
        let pieces = vec![0u8; (piece_count * 20) as usize];
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
    fn rejects_drive_prefix_names() {
        for name in ["C:", "c:x", "Z:evil.dll"] {
            let bytes = synth_single_file(name, 16 * 1024, 100_000);
            assert_eq!(parse_torrent(&bytes).is_err(), cfg!(windows), "{name}");
        }
        let bytes = synth_single_file("a\0b", 16 * 1024, 100_000);
        assert!(parse_torrent(&bytes).is_err());
        let bytes = synth_single_file("Ep 1: Title.mkv", 16 * 1024, 100_000);
        assert!(parse_torrent(&bytes).is_ok());
    }

    #[test]
    fn sanitizes_windows_components() {
        assert_eq!(sanitize_windows_component("Ep 1: T?.mkv"), "Ep 1_ T_.mkv");
        assert_eq!(sanitize_windows_component("con.txt"), "_con.txt");
        assert_eq!(sanitize_windows_component("LPT3"), "_LPT3");
        assert_eq!(sanitize_windows_component("a. ."), "a");
        assert_eq!(sanitize_windows_component("..."), "_");
        assert_eq!(sanitize_windows_component("COM10"), "COM10");
    }

    #[test]
    fn rejects_piece_count_mismatch_and_huge_counts() {
        let info = |len: i64, pl: i64, pieces: usize| {
            Value::Dict(vec![
                (b"length".to_vec(), Value::Int(len)),
                (b"name".to_vec(), Value::Bytes(b"x".to_vec())),
                (b"piece length".to_vec(), Value::Int(pl)),
                (b"pieces".to_vec(), Value::Bytes(vec![0u8; pieces])),
            ])
        };
        assert!(validate_info(&info(4_294_967_295, 1, 20)).is_err());
        assert!(validate_info(&info(100, 10, 20)).is_err());
        assert!(validate_info(&info(100, 10, 200)).is_ok());
        assert!(validate_info(&info(100, 10, 220)).is_err());
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

    fn synth_hybrid_single_file(name: &str, piece_len: u32, data_len: u64) -> Vec<u8> {
        let piece_count = data_len.div_ceil(piece_len as u64);
        let v1_pieces = vec![0u8; (piece_count * 20) as usize];
        let pieces_root = Id32([0x11u8; 32]);
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
        let layer = meta
            .piece_layers
            .get(&v2.files[0].pieces_root)
            .expect("piece layers entry present");
        assert_eq!(layer.len(), 7 * 32);
    }

    #[test]
    fn parse_hybrid_torrent_small_file_no_layers() {
        let bytes = synth_hybrid_single_file("tiny.bin", 16 * 1024, 1024);
        let meta = parse_torrent(&bytes).unwrap();
        assert_eq!(meta.meta_version, MetaVersion::Hybrid);
        assert!(meta.piece_layers.is_empty());
    }

    #[test]
    fn pure_v2_small_file_parses_without_layers() {
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
        let pieces_root = Id32([0x55u8; 32]);
        let file_leaf = Value::Dict(vec![(
            b"".to_vec(),
            Value::Dict(vec![
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
        let h = TorrentInfoHashes {
            v1: None,
            v2: Some(v2),
        };
        assert_eq!(h.announce_infohashes().len(), 1);
        assert_eq!(h.announce_infohashes()[0], v2.truncate_to_id20());

        let v1 = Id20([0x11u8; 20]);
        let h = TorrentInfoHashes {
            v1: Some(v1),
            v2: Some(v2),
        };
        let all = h.announce_infohashes();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0], v1);
        assert_eq!(all[1], v2.truncate_to_id20());

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
            Value::List(vec![Value::Bytes(b"8.8.8.8".to_vec()), Value::Int(6884)]),
            Value::List(vec![
                Value::Bytes(b"router.example.org".to_vec()),
                Value::Int(6883),
            ]),
        ]);
        let top = Value::Dict(vec![(b"info".to_vec(), info), (b"nodes".to_vec(), nodes)]);
        let bytes = super::super::super::bencode::encode_to_vec(&top);
        let meta = parse_torrent(&bytes).unwrap();
        assert!(meta.bootstrap_nodes.is_empty());
        assert_eq!(meta.bootstrap_hosts.len(), 2);
        assert!(meta
            .bootstrap_hosts
            .contains(&("8.8.8.8".to_string(), 6884)));
        assert!(meta
            .bootstrap_hosts
            .contains(&("router.example.org".to_string(), 6883)));
    }

    #[test]
    fn pure_v2_multi_file_aligns_each_file_to_a_piece_boundary() {
        use crate::core::merkle::{hash_block, PieceVerifier};
        const PIECE: usize = 16 * 1024;
        let a: Vec<u8> = (0..10 * 1024).map(|i| (i % 251) as u8).collect();
        let b: Vec<u8> = (0..5 * 1024).map(|i| (i % 241) as u8).collect();
        let leaf = |data: &[u8]| {
            Value::Dict(vec![(
                b"".to_vec(),
                Value::Dict(vec![
                    (b"length".to_vec(), Value::Int(data.len() as i64)),
                    (
                        b"pieces root".to_vec(),
                        Value::Bytes(hash_block(data).0.to_vec()),
                    ),
                ]),
            )])
        };
        let info = Value::Dict(vec![
            (
                b"file tree".to_vec(),
                Value::Dict(vec![(b"a".to_vec(), leaf(&a)), (b"b".to_vec(), leaf(&b))]),
            ),
            (b"meta version".to_vec(), Value::Int(2)),
            (b"name".to_vec(), Value::Bytes(b"root".to_vec())),
            (b"piece length".to_vec(), Value::Int(PIECE as i64)),
        ]);
        let top = Value::Dict(vec![(b"info".to_vec(), info)]);
        let meta = parse_torrent(&super::super::super::bencode::encode_to_vec(&top)).unwrap();

        let layout: Vec<(String, u64, bool)> = meta
            .info
            .files
            .iter()
            .map(|f| (f.path.join("/"), f.length, f.padding))
            .collect();
        assert_eq!(
            layout,
            [
                ("a".to_string(), a.len() as u64, false),
                (".pad/6144".to_string(), (PIECE - a.len()) as u64, true),
                ("b".to_string(), b.len() as u64, false),
            ]
        );
        assert_eq!(meta.info.piece_count(), 2);

        let verifier = PieceVerifier::from_meta(&meta).unwrap();
        let mut piece0 = a.clone();
        piece0.resize(PIECE, 0);
        verifier.verify(0, &piece0).unwrap();
        verifier.verify(1, &b).unwrap();
        assert!(verifier.verify(1, &piece0[..b.len()]).is_err());
    }

    #[test]
    fn pure_v2_keeps_empty_files_in_layout() {
        let data = vec![7u8; 2048];
        let leaf = Value::Dict(vec![(
            b"".to_vec(),
            Value::Dict(vec![
                (b"length".to_vec(), Value::Int(2048)),
                (
                    b"pieces root".to_vec(),
                    Value::Bytes(crate::core::merkle::hash_block(&data).0.to_vec()),
                ),
            ]),
        )]);
        let empty = Value::Dict(vec![(
            b"".to_vec(),
            Value::Dict(vec![(b"length".to_vec(), Value::Int(0))]),
        )]);
        let info = Value::Dict(vec![
            (
                b"file tree".to_vec(),
                Value::Dict(vec![(b"a".to_vec(), leaf), (b".keep".to_vec(), empty)]),
            ),
            (b"meta version".to_vec(), Value::Int(2)),
            (b"name".to_vec(), Value::Bytes(b"root".to_vec())),
            (b"piece length".to_vec(), Value::Int(16 * 1024)),
        ]);
        let top = Value::Dict(vec![(b"info".to_vec(), info)]);
        let meta = parse_torrent(&super::super::super::bencode::encode_to_vec(&top)).unwrap();
        let layout: Vec<(String, u64)> = meta
            .info
            .files
            .iter()
            .map(|f| (f.path.join("/"), f.length))
            .collect();
        assert_eq!(layout, [("a".to_string(), 2048), (".keep".to_string(), 0)]);
        assert_eq!(meta.info_v2.unwrap().files.len(), 1);
    }

    #[test]
    fn non_utf8_names_use_alternates_or_lossy() {
        let mut info = vec![
            (b"length".to_vec(), Value::Int(10)),
            (b"name".to_vec(), Value::Bytes(vec![0xb9, 0xfe, b'x'])),
            (b"piece length".to_vec(), Value::Int(16 * 1024)),
            (b"pieces".to_vec(), Value::Bytes(vec![0u8; 20])),
        ];
        let lossy = validate_info(&Value::Dict(info.clone())).unwrap();
        assert!(lossy.name.ends_with('x'));
        info.push((b"name.utf-8".to_vec(), Value::Bytes(b"good".to_vec())));
        let preferred = validate_info(&Value::Dict(info)).unwrap();
        assert_eq!(preferred.name, "good");
    }

    fn names(files: &[TorrentMetaInfo]) -> Vec<String> {
        files.iter().map(|f| f.path.join("/")).collect()
    }

    fn v1_multi(entries: &[(&[&[u8]], u64, bool)]) -> Vec<u8> {
        let total: u64 = entries.iter().map(|e| e.1).sum();
        let files = entries
            .iter()
            .map(|(path, len, pad)| {
                let mut d = vec![
                    (b"length".to_vec(), Value::Int(*len as i64)),
                    (
                        b"path".to_vec(),
                        Value::List(path.iter().map(|c| Value::Bytes(c.to_vec())).collect()),
                    ),
                ];
                if *pad {
                    d.insert(0, (b"attr".to_vec(), Value::Bytes(b"p".to_vec())));
                }
                Value::Dict(d)
            })
            .collect();
        let pieces = vec![0u8; (total.div_ceil(16384) * 20) as usize];
        let info = Value::Dict(vec![
            (b"files".to_vec(), Value::List(files)),
            (b"name".to_vec(), Value::Bytes(b"root".to_vec())),
            (b"piece length".to_vec(), Value::Int(16384)),
            (b"pieces".to_vec(), Value::Bytes(pieces)),
        ]);
        super::super::super::bencode::encode_to_vec(&Value::Dict(vec![(b"info".to_vec(), info)]))
    }

    #[test]
    fn dedupe_numbers_later_duplicates_and_skips_taken_names() {
        let mut paths: Vec<Vec<String>> = [
            "d/a.txt",
            "d/a.txt",
            "d/a (1).txt",
            "d/a.txt",
            ".hid",
            ".hid",
        ]
        .iter()
        .map(|p| p.split('/').map(String::from).collect())
        .collect();
        let skip = vec![false; paths.len()];
        dedupe_paths(&mut paths, &skip);
        let got: Vec<String> = paths.iter().map(|p| p.join("/")).collect();
        assert_eq!(
            got,
            [
                "d/a.txt",
                "d/a (2).txt",
                "d/a (1).txt",
                "d/a (3).txt",
                ".hid",
                ".hid (1)"
            ]
        );
        let mut again = paths.clone();
        dedupe_paths(&mut again, &skip);
        assert_eq!(again, paths);
    }

    #[test]
    fn dedupe_never_counts_padding() {
        let mut paths = vec![
            vec![".pad".to_string(), "5".to_string()],
            vec![".pad".to_string(), "5".to_string()],
        ];
        dedupe_paths(&mut paths, &[true, true]);
        assert_eq!(paths[0], paths[1]);
    }

    #[test]
    fn dedupe_folds_case_only_where_the_filesystem_does() {
        let mut paths = vec![vec!["A.txt".to_string()], vec!["a.txt".to_string()]];
        dedupe_paths(&mut paths, &[false, false]);
        if cfg!(any(windows, target_os = "macos")) {
            assert_eq!(paths[1], vec!["a (1).txt".to_string()]);
        } else {
            assert_eq!(paths[1], vec!["a.txt".to_string()]);
        }
    }

    #[test]
    fn v1_lossy_name_collisions_get_unique_paths() {
        let bytes = v1_multi(&[
            (&[b"d", b"a\xff.txt"], 100, false),
            (&[b".pad", b"1"], 10, true),
            (&[b".pad", b"1"], 10, true),
            (&[b"d", b"a\xfe.txt"], 100, false),
        ]);
        let meta = parse_torrent(&bytes).unwrap();
        assert_eq!(
            names(&meta.info.files),
            ["d/a\u{fffd}.txt", ".pad/1", ".pad/1", "d/a\u{fffd} (1).txt"]
        );
        let details: Vec<String> = meta.info.iter_file_details().map(|d| d.filename).collect();
        assert_eq!(details[3], "d/a\u{fffd} (1).txt");
        let again = parse_torrent(&bytes).unwrap();
        assert_eq!(names(&again.info.files), names(&meta.info.files));
    }

    fn v2_leaf(len: i64) -> Value {
        Value::Dict(vec![(
            b"".to_vec(),
            Value::Dict(vec![
                (b"length".to_vec(), Value::Int(len)),
                (b"pieces root".to_vec(), Value::Bytes(vec![7u8; 32])),
            ]),
        )])
    }

    #[test]
    fn pure_v2_lossy_name_collisions_get_unique_paths() {
        let info = Value::Dict(vec![
            (
                b"file tree".to_vec(),
                Value::Dict(vec![
                    (b"a\xfe.txt".to_vec(), v2_leaf(100)),
                    (b"a\xff.txt".to_vec(), v2_leaf(200)),
                ]),
            ),
            (b"meta version".to_vec(), Value::Int(2)),
            (b"name".to_vec(), Value::Bytes(b"root".to_vec())),
            (b"piece length".to_vec(), Value::Int(16384)),
        ]);
        let top = Value::Dict(vec![(b"info".to_vec(), info)]);
        let meta = parse_torrent(&super::super::super::bencode::encode_to_vec(&top)).unwrap();
        let shown: Vec<String> = meta
            .info
            .iter_file_details()
            .filter(|d| !d.padding)
            .map(|d| d.filename)
            .collect();
        assert_eq!(shown, ["a\u{fffd}.txt", "a\u{fffd} (1).txt"]);
    }

    #[test]
    fn hybrid_uses_the_deduped_v1_layout() {
        let mut v1 = parse_torrent(&v1_multi(&[
            (&[b"x\xfe"], 100, false),
            (&[b"x\xff"], 100, false),
        ]))
        .unwrap();
        let top = crate::bencode::decode_all(&v1.info_bytes).unwrap();
        let mut dict = top.as_dict().unwrap().to_vec();
        dict.push((b"meta version".to_vec(), Value::Int(2)));
        dict.push((
            b"file tree".to_vec(),
            Value::Dict(vec![(
                b"root".to_vec(),
                Value::Dict(vec![
                    (b"p".to_vec(), v2_leaf(100)),
                    (b"q".to_vec(), v2_leaf(100)),
                ]),
            )]),
        ));
        dict.sort_by(|a, b| a.0.cmp(&b.0));
        let outer = Value::Dict(vec![(b"info".to_vec(), Value::Dict(dict))]);
        v1 = parse_torrent(&super::super::super::bencode::encode_to_vec(&outer)).unwrap();
        assert!(matches!(v1.meta_version, MetaVersion::Hybrid));
        assert_eq!(names(&v1.info.files), ["x\u{fffd}", "x\u{fffd} (1)"]);
    }
}
