use std::collections::BTreeMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant, SystemTime};

use parking_lot::Mutex;

use percent_encoding::utf8_percent_encode;
use sha1::{Digest, Sha1};

use super::bencode::{encode_to_vec, Value};
use super::core::hash::{sha1, sha256, Id20, Id32};
use super::core::merkle::{blocks_root, piece_layer_root, piece_root, BLOCK_SIZE};
use super::core::metainfo::{is_safe_component, parse_torrent, MetaVersion};
use super::core::peer_id::CLIENT_VERSION;
use super::tracker::http::QUERY_SAFE;

pub const MIN_PIECE_LENGTH: u32 = 16 * 1024;
pub const MAX_PIECE_LENGTH: u32 = 64 * 1024 * 1024;
const AUTO_MAX_PIECE_LENGTH: u32 = 16 * 1024 * 1024;
const TARGET_PIECES: u64 = 1500;
const SKIPPED_NAMES: [&str; 3] = [".DS_Store", "Thumbs.db", "desktop.ini"];
const MAX_HASH_THREADS: usize = 8;
const HASH_BYTES_IN_FLIGHT: usize = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum CreateError {
    #[error("{}: {source}", path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{} has a name that is not valid UTF-8 or not allowed in a torrent", .0.display())]
    BadName(PathBuf),
    #[error("there are no files to add")]
    Empty,
    #[error("piece length must be a power of two between 16 KiB and 64 MiB")]
    BadPieceLength,
    #[error("{} changed while it was being hashed", .0.display())]
    Changed(PathBuf),
    #[error("cancelled")]
    Cancelled,
    #[error("created torrent did not parse back: {0}")]
    Invalid(String),
}

#[derive(Debug, Clone)]
pub struct CreateOptions {
    pub version: MetaVersion,
    pub piece_length: Option<u32>,
    pub trackers: Vec<Vec<String>>,
    pub web_seeds: Vec<String>,
    pub comment: Option<String>,
    pub source: Option<String>,
    pub private: bool,
    pub created_by: Option<String>,
    pub creation_date: Option<i64>,
    pub exclude: Vec<PathBuf>,
}

impl Default for CreateOptions {
    fn default() -> Self {
        Self {
            version: MetaVersion::Hybrid,
            piece_length: None,
            trackers: Vec::new(),
            web_seeds: Vec::new(),
            comment: None,
            source: None,
            private: false,
            created_by: Some(CLIENT_VERSION.to_string()),
            creation_date: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|d| i64::try_from(d.as_secs()).ok()),
            exclude: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreatedTorrent {
    pub bytes: Vec<u8>,
    pub seed_dir: PathBuf,
    pub name: String,
    pub info_hash_v1: Option<Id20>,
    pub info_hash_v2: Option<Id32>,
    pub piece_length: u32,
    pub total_length: u64,
    pub file_count: usize,
    pub magnet: String,
    pub stamps: Vec<FileStamp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStamp {
    pub path: Vec<String>,
    pub length: u64,
    pub modified: SystemTime,
}

#[derive(Debug)]
struct SourceFile {
    path: PathBuf,
    components: Vec<String>,
    length: u64,
    executable: bool,
}

pub fn auto_piece_length(total_length: u64) -> u32 {
    let target = total_length / TARGET_PIECES;
    let mut length = MIN_PIECE_LENGTH;
    while (length as u64) < target && length < AUTO_MAX_PIECE_LENGTH {
        length *= 2;
    }
    length
}

pub fn throttled(interval: Duration, mut report: impl FnMut(u64, u64)) -> impl FnMut(u64, u64) {
    let mut last: Option<Instant> = None;
    move |done, total| {
        if done == total || last.is_none_or(|t| t.elapsed() >= interval) {
            last = Some(Instant::now());
            report(done, total);
        }
    }
}

pub struct Plan {
    root: PathBuf,
    name: String,
    files: Vec<SourceFile>,
    single: bool,
    pub piece_length: u32,
    pub total_length: u64,
}

pub fn plan(root: &Path, opts: &CreateOptions) -> Result<Plan, CreateError> {
    let (mut root, name, files, mut single) = collect_files(root, &opts.exclude)?;
    if opts.version == MetaVersion::V2
        && !single
        && matches!(files.as_slice(), [only] if only.components == [name.as_str()])
    {
        root = files[0].path.clone();
        single = true;
    }
    let total_length: u64 = files.iter().map(|f| f.length).sum();
    if total_length == 0 {
        return Err(CreateError::Empty);
    }
    let piece_length = match opts.piece_length {
        Some(len)
            if len.is_power_of_two() && (MIN_PIECE_LENGTH..=MAX_PIECE_LENGTH).contains(&len) =>
        {
            len
        }
        Some(_) => return Err(CreateError::BadPieceLength),
        None => auto_piece_length(total_length),
    };
    Ok(Plan {
        root,
        name,
        files,
        single,
        piece_length,
        total_length,
    })
}

pub fn create_torrent(
    root: &Path,
    opts: &CreateOptions,
    progress: impl FnMut(u64, u64),
    cancel: &AtomicBool,
) -> Result<CreatedTorrent, CreateError> {
    plan(root, opts)?.create(opts, progress, cancel)
}

fn padding_after(length: u64, piece_length: u32) -> u64 {
    let piece = piece_length as u64;
    (piece - length % piece) % piece
}

impl Plan {
    pub fn hybrid_padding(&self) -> u64 {
        self.hybrid_padding_with(self.piece_length)
    }

    pub fn hybrid_padding_with(&self, piece_length: u32) -> u64 {
        self.files.split_last().map_or(0, |(_, rest)| {
            rest.iter()
                .map(|f| padding_after(f.length, piece_length))
                .sum()
        })
    }

    pub fn create(
        self,
        opts: &CreateOptions,
        progress: impl FnMut(u64, u64),
        cancel: &AtomicBool,
    ) -> Result<CreatedTorrent, CreateError> {
        self.create_on(hash_threads(), opts, progress, cancel)
    }

    fn create_on(
        self,
        threads: usize,
        opts: &CreateOptions,
        mut progress: impl FnMut(u64, u64),
        cancel: &AtomicBool,
    ) -> Result<CreatedTorrent, CreateError> {
        let Plan {
            root,
            name,
            files,
            single,
            piece_length,
            total_length,
        } = self;
        let has_v1 = opts.version.has_v1();
        let has_v2 = opts.version.has_v2();
        let pad_files = opts.version == MetaVersion::Hybrid;
        let blocks_per_piece = piece_length / BLOCK_SIZE;
        progress(0, total_length);

        let (job_tx, job_rx) = mpsc::channel::<HashJob>();
        let (done_tx, done_rx) = mpsc::channel::<Hashed>();
        let job_rx = Mutex::new(job_rx);
        let (hashes, chunks, stamps) = std::thread::scope(|scope| {
            for _ in 0..threads.max(1) {
                let job_rx = &job_rx;
                let done_tx = done_tx.clone();
                scope.spawn(move || loop {
                    let job = job_rx.lock().recv();
                    let Ok(job) = job else { break };
                    if done_tx.send(job.hash(blocks_per_piece)).is_err() {
                        break;
                    }
                });
            }
            drop(done_tx);
            let mut pipeline = Pipeline {
                jobs: job_tx,
                done: done_rx,
                limit: (HASH_BYTES_IN_FLIGHT / piece_length as usize).clamp(1, 2 * threads.max(1)),
                pending: 0,
                results: Vec::new(),
                spare: Vec::new(),
                hashed: 0,
                total: total_length,
                progress: &mut progress,
            };
            let mut chunks = Vec::with_capacity(files.len());
            let mut stamps = Vec::with_capacity(files.len());
            let mut stream = Vec::new();
            for (index, file) in files.iter().enumerate() {
                let io_err = |source| CreateError::Io {
                    path: file.path.clone(),
                    source,
                };
                let mut reader = File::open(&file.path).map_err(io_err)?;
                let modified = reader.metadata().and_then(|m| m.modified()).ok();
                let small = file.length <= piece_length as u64;
                let pad = padding_after(file.length, piece_length);
                let first = pipeline.results.len();
                let mut remaining = file.length;
                while remaining > 0 {
                    if cancel.load(Ordering::Relaxed) {
                        return Err(CreateError::Cancelled);
                    }
                    let mut buf = if has_v2 {
                        pipeline.buffer()
                    } else {
                        std::mem::take(&mut stream)
                    };
                    let want = remaining.min((piece_length as usize - buf.len()) as u64) as usize;
                    let start = buf.len();
                    buf.resize(start + want, 0);
                    reader.read_exact(&mut buf[start..]).map_err(|e| {
                        if e.kind() == std::io::ErrorKind::UnexpectedEof {
                            CreateError::Changed(file.path.clone())
                        } else {
                            io_err(e)
                        }
                    })?;
                    remaining -= want as u64;
                    if has_v2 {
                        let v1_pad = if remaining == 0 && pad_files && index + 1 < files.len() {
                            pad
                        } else {
                            0
                        };
                        pipeline.submit(HashJob {
                            seq: 0,
                            data: buf,
                            v1: has_v1,
                            v1_pad,
                            v2: Some(small),
                        });
                    } else if buf.len() == piece_length as usize {
                        pipeline.submit(HashJob::v1(buf));
                        stream = pipeline.buffer();
                    } else {
                        stream = buf;
                    }
                }
                if reader.read(&mut [0u8; 1]).map_err(io_err)? != 0
                    || reader.metadata().and_then(|m| m.modified()).ok() != modified
                {
                    return Err(CreateError::Changed(file.path.clone()));
                }
                chunks.push(first..pipeline.results.len());
                if let Some(modified) = modified {
                    stamps.push(FileStamp {
                        path: if single {
                            vec![name.clone()]
                        } else {
                            file.components.clone()
                        },
                        length: file.length,
                        modified,
                    });
                }
            }
            if !stream.is_empty() {
                pipeline.submit(HashJob::v1(stream));
            }
            Ok((pipeline.finish(), chunks, stamps))
        })?;
        if cancel.load(Ordering::Relaxed) {
            return Err(CreateError::Cancelled);
        }

        let pieces: Vec<u8> = hashes.iter().filter_map(|(v1, _)| *v1).flatten().collect();
        let mut v1_files = Vec::with_capacity(files.len());
        let mut tree = TreeDir::default();
        let mut piece_layers = BTreeMap::new();
        for (index, (file, chunks)) in files.iter().zip(chunks).enumerate() {
            v1_files.push(file_entry(file));
            let pad = padding_after(file.length, piece_length);
            if pad_files && pad > 0 && index + 1 < files.len() {
                v1_files.push(padding_entry(pad));
            }
            if has_v2 {
                let roots: Vec<Id32> = hashes[chunks].iter().filter_map(|(_, v2)| *v2).collect();
                let small = file.length <= piece_length as u64;
                let pieces_root = match roots.as_slice() {
                    [] => None,
                    [root] if small => Some(*root),
                    roots => {
                        let root = piece_layer_root(roots, blocks_per_piece);
                        piece_layers.entry(root.0.to_vec()).or_insert_with(|| {
                            Value::Bytes(roots.iter().flat_map(|h| h.0).collect())
                        });
                        Some(root)
                    }
                };
                let components = if single {
                    vec![name.clone()]
                } else {
                    file.components.clone()
                };
                tree.insert(
                    &components,
                    v2_file_entry(file.length, pieces_root, file.executable),
                );
            }
        }

        let mut info = vec![
            entry("name", bytes(&name)),
            entry("piece length", Value::Int(piece_length.into())),
        ];
        if opts.private {
            info.push(entry("private", Value::Int(1)));
        }
        if let Some(source) = non_empty(&opts.source) {
            info.push(entry("source", bytes(source)));
        }
        if has_v1 {
            info.push(entry("pieces", Value::Bytes(pieces)));
            if single {
                info.push(entry("length", Value::Int(total_length as i64)));
                if files[0].executable {
                    info.push(entry("attr", bytes("x")));
                }
            } else {
                info.push(entry("files", Value::List(v1_files)));
            }
        }
        if has_v2 {
            info.push(entry("meta version", Value::Int(2)));
            info.push(entry("file tree", tree.into_value()));
        }
        let info = Value::Dict(info);
        let info_bytes = encode_to_vec(&info);
        let info_hash_v1 = has_v1.then(|| sha1(&info_bytes));
        let info_hash_v2 = has_v2.then(|| sha256(&info_bytes));

        let trackers: Vec<Vec<String>> = opts
            .trackers
            .iter()
            .map(|tier| {
                tier.iter()
                    .map(|t| t.trim().to_string())
                    .filter(|t| !t.is_empty())
                    .collect::<Vec<_>>()
            })
            .filter(|tier| !tier.is_empty())
            .collect();
        let web_seeds: Vec<String> = opts
            .web_seeds
            .iter()
            .map(|w| w.trim().to_string())
            .filter(|w| !w.is_empty())
            .collect();

        let mut top = vec![entry("info", info)];
        if let Some(first) = trackers.first().and_then(|tier| tier.first()) {
            top.push(entry("announce", bytes(first)));
        }
        if trackers.iter().map(Vec::len).sum::<usize>() > 1 {
            top.push(entry(
                "announce-list",
                Value::List(
                    trackers
                        .iter()
                        .map(|tier| Value::List(tier.iter().map(bytes).collect()))
                        .collect(),
                ),
            ));
        }
        if !web_seeds.is_empty() {
            top.push(entry(
                "url-list",
                Value::List(web_seeds.iter().map(bytes).collect()),
            ));
        }
        if let Some(comment) = non_empty(&opts.comment) {
            top.push(entry("comment", bytes(comment)));
        }
        if let Some(created_by) = non_empty(&opts.created_by) {
            top.push(entry("created by", bytes(created_by)));
        }
        if let Some(date) = opts.creation_date {
            top.push(entry("creation date", Value::Int(date)));
        }
        if !piece_layers.is_empty() {
            top.push(entry(
                "piece layers",
                Value::Dict(piece_layers.into_iter().collect()),
            ));
        }
        let bytes = encode_to_vec(&Value::Dict(top));
        parse_torrent(&bytes).map_err(|e| CreateError::Invalid(e.to_string()))?;

        let magnet = magnet_link(
            info_hash_v1,
            info_hash_v2,
            &name,
            trackers.iter().flatten(),
            &web_seeds,
        );
        Ok(CreatedTorrent {
            bytes,
            name,
            info_hash_v1,
            info_hash_v2,
            piece_length,
            total_length,
            file_count: files.len(),
            magnet,
            seed_dir: root.parent().unwrap_or(&root).to_path_buf(),
            stamps,
        })
    }
}

fn collect_files(
    root: &Path,
    exclude: &[PathBuf],
) -> Result<(PathBuf, String, Vec<SourceFile>, bool), CreateError> {
    let io_err = |path: &Path| {
        let path = path.to_path_buf();
        move |source| CreateError::Io { path, source }
    };
    let root = dunce::canonicalize(root).map_err(io_err(root))?;
    let name = root
        .file_name()
        .and_then(|n| n.to_str())
        .filter(|n| is_safe_component(n))
        .ok_or_else(|| CreateError::BadName(root.clone()))?
        .to_string();
    let meta = std::fs::metadata(&root).map_err(io_err(&root))?;
    if meta.is_file() {
        let file = SourceFile {
            executable: is_executable(&meta),
            length: meta.len(),
            components: Vec::new(),
            path: root.clone(),
        };
        return Ok((root, name, vec![file], true));
    }
    let exclude: Vec<PathBuf> = exclude
        .iter()
        .map(|p| dunce::canonicalize(p).unwrap_or_else(|_| p.clone()))
        .collect();
    let mut files = Vec::new();
    walk(&root, &mut Vec::new(), &exclude, &mut files)?;
    if files.is_empty() {
        return Err(CreateError::Empty);
    }
    files.sort_by(|a, b| a.components.cmp(&b.components));
    Ok((root, name, files, false))
}

fn walk(
    dir: &Path,
    prefix: &mut Vec<String>,
    exclude: &[PathBuf],
    out: &mut Vec<SourceFile>,
) -> Result<(), CreateError> {
    let io_err = |source| CreateError::Io {
        path: dir.to_path_buf(),
        source,
    };
    for entry in std::fs::read_dir(dir).map_err(io_err)? {
        let entry = entry.map_err(io_err)?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(io_err)?;
        if file_type.is_symlink() || exclude.contains(&path) {
            continue;
        }
        let name = entry
            .file_name()
            .to_str()
            .filter(|n| is_safe_component(n))
            .ok_or_else(|| CreateError::BadName(path.clone()))?
            .to_string();
        if file_type.is_dir() {
            prefix.push(name);
            walk(&path, prefix, exclude, out)?;
            prefix.pop();
        } else if file_type.is_file() && !SKIPPED_NAMES.contains(&name.as_str()) {
            let meta = entry.metadata().map_err(io_err)?;
            let mut components = prefix.clone();
            components.push(name);
            out.push(SourceFile {
                path,
                components,
                length: meta.len(),
                executable: is_executable(&meta),
            });
        }
    }
    Ok(())
}

#[cfg(unix)]
fn is_executable(meta: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    meta.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_meta: &std::fs::Metadata) -> bool {
    false
}

struct HashJob {
    seq: usize,
    data: Vec<u8>,
    v1: bool,
    v1_pad: u64,
    v2: Option<bool>,
}

type PieceHashes = (Option<[u8; 20]>, Option<Id32>);

struct Hashed {
    seq: usize,
    hashes: PieceHashes,
    data: Vec<u8>,
}

impl HashJob {
    fn v1(data: Vec<u8>) -> Self {
        Self {
            seq: 0,
            data,
            v1: true,
            v1_pad: 0,
            v2: None,
        }
    }

    fn hash(self, blocks_per_piece: u32) -> Hashed {
        let v1 = self.v1.then(|| {
            let mut hasher = Sha1::new();
            hasher.update(&self.data);
            let zeros = [0u8; 16 * 1024];
            let mut pad = self.v1_pad;
            while pad > 0 {
                let take = pad.min(zeros.len() as u64) as usize;
                hasher.update(&zeros[..take]);
                pad -= take as u64;
            }
            hasher.finalize().into()
        });
        let v2 = self.v2.map(|small| {
            if small {
                blocks_root(&self.data)
            } else {
                piece_root(&self.data, blocks_per_piece)
            }
        });
        Hashed {
            seq: self.seq,
            hashes: (v1, v2),
            data: self.data,
        }
    }
}

struct Pipeline<'p, P> {
    jobs: mpsc::Sender<HashJob>,
    done: mpsc::Receiver<Hashed>,
    limit: usize,
    pending: usize,
    results: Vec<Option<PieceHashes>>,
    spare: Vec<Vec<u8>>,
    hashed: u64,
    total: u64,
    progress: &'p mut P,
}

impl<P: FnMut(u64, u64)> Pipeline<'_, P> {
    fn buffer(&mut self) -> Vec<u8> {
        self.spare.pop().unwrap_or_default()
    }

    fn submit(&mut self, mut job: HashJob) {
        while self.pending >= self.limit {
            self.collect();
        }
        job.seq = self.results.len();
        self.results.push(None);
        self.pending += 1;
        self.jobs.send(job).expect("hash workers stopped");
    }

    fn collect(&mut self) {
        let mut done = self.done.recv().expect("hash workers stopped");
        self.pending -= 1;
        self.hashed += done.data.len() as u64;
        (self.progress)(self.hashed, self.total);
        self.results[done.seq] = Some(done.hashes);
        done.data.clear();
        self.spare.push(done.data);
    }

    fn finish(mut self) -> Vec<PieceHashes> {
        while self.pending > 0 {
            self.collect();
        }
        self.results.into_iter().flatten().collect()
    }
}

fn hash_threads() -> usize {
    std::thread::available_parallelism()
        .map_or(1, |n| n.get().saturating_sub(1))
        .clamp(1, MAX_HASH_THREADS)
}

#[derive(Default)]
struct TreeDir(BTreeMap<String, TreeNode>);

enum TreeNode {
    Dir(TreeDir),
    File(Value),
}

impl TreeDir {
    fn insert(&mut self, components: &[String], file: Value) {
        let Some((last, dirs)) = components.split_last() else {
            return;
        };
        let mut dir = self;
        for component in dirs {
            let node = dir
                .0
                .entry(component.clone())
                .or_insert_with(|| TreeNode::Dir(TreeDir::default()));
            dir = match node {
                TreeNode::Dir(child) => child,
                TreeNode::File(_) => return,
            };
        }
        dir.0.insert(last.clone(), TreeNode::File(file));
    }

    fn into_value(self) -> Value {
        Value::Dict(
            self.0
                .into_iter()
                .map(|(name, node)| {
                    let value = match node {
                        TreeNode::Dir(dir) => dir.into_value(),
                        TreeNode::File(file) => file,
                    };
                    (name.into_bytes(), value)
                })
                .collect(),
        )
    }
}

fn entry(key: &str, value: Value) -> (Vec<u8>, Value) {
    (key.as_bytes().to_vec(), value)
}

fn bytes(text: impl AsRef<str>) -> Value {
    Value::Bytes(text.as_ref().as_bytes().to_vec())
}

fn non_empty(value: &Option<String>) -> Option<&str> {
    value.as_deref().map(str::trim).filter(|v| !v.is_empty())
}

fn file_entry(file: &SourceFile) -> Value {
    let mut fields = vec![
        entry("length", Value::Int(file.length as i64)),
        entry(
            "path",
            Value::List(file.components.iter().map(bytes).collect()),
        ),
    ];
    if file.executable {
        fields.push(entry("attr", bytes("x")));
    }
    Value::Dict(fields)
}

fn padding_entry(len: u64) -> Value {
    Value::Dict(vec![
        entry("attr", bytes("p")),
        entry("length", Value::Int(len as i64)),
        entry(
            "path",
            Value::List(vec![bytes(".pad"), bytes(len.to_string())]),
        ),
    ])
}

fn v2_file_entry(length: u64, pieces_root: Option<Id32>, executable: bool) -> Value {
    let mut fields = vec![entry("length", Value::Int(length as i64))];
    if let Some(root) = pieces_root {
        fields.push(entry("pieces root", Value::Bytes(root.0.to_vec())));
    }
    if executable {
        fields.push(entry("attr", bytes("x")));
    }
    Value::Dict(vec![(Vec::new(), Value::Dict(fields))])
}

fn magnet_link<'a>(
    v1: Option<Id20>,
    v2: Option<Id32>,
    name: &str,
    trackers: impl Iterator<Item = &'a String>,
    web_seeds: &[String],
) -> String {
    let mut parts = Vec::new();
    if let Some(v1) = v1 {
        parts.push(format!("xt=urn:btih:{}", v1.to_hex()));
    }
    if let Some(v2) = v2 {
        parts.push(format!("xt=urn:btmh:1220{}", v2.to_hex()));
    }
    parts.push(format!("dn={}", utf8_percent_encode(name, QUERY_SAFE)));
    for tracker in trackers {
        parts.push(format!("tr={}", utf8_percent_encode(tracker, QUERY_SAFE)));
    }
    for seed in web_seeds {
        parts.push(format!("ws={}", utf8_percent_encode(seed, QUERY_SAFE)));
    }
    format!("magnet:?{}", parts.join("&"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::merkle::PieceVerifier;

    const PIECE: u32 = 32 * 1024;

    fn pattern(len: usize, seed: u8) -> Vec<u8> {
        (0..len)
            .map(|i| (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[1] ^ seed)
            .collect()
    }

    fn opts(version: MetaVersion) -> CreateOptions {
        CreateOptions {
            version,
            piece_length: Some(PIECE),
            creation_date: Some(1_700_000_000),
            ..Default::default()
        }
    }

    fn create(root: &Path, opts: &CreateOptions) -> CreatedTorrent {
        create_torrent(root, opts, |_, _| {}, &AtomicBool::new(false)).unwrap()
    }

    fn assert_every_piece_verifies(created: &CreatedTorrent, root: &Path, version: MetaVersion) {
        let meta = parse_torrent(&created.bytes).unwrap();
        assert_eq!(meta.meta_version, version);
        assert_eq!(meta.info.piece_length, created.piece_length);
        assert_eq!(meta.info_hashes().v1, created.info_hash_v1);
        assert_eq!(meta.info_hash_v2, created.info_hash_v2);
        let mut stream = Vec::new();
        for file in &meta.info.files {
            if file.padding {
                stream.resize(stream.len() + file.length as usize, 0);
            } else if meta.info.single_file_mode {
                stream.extend(std::fs::read(root).unwrap());
            } else {
                stream.extend(std::fs::read(root.join(file.path.join("/"))).unwrap());
            }
        }
        let verifier = PieceVerifier::from_meta(&meta).unwrap();
        if version == MetaVersion::Hybrid {
            assert!(matches!(verifier, PieceVerifier::Hybrid { .. }));
        }
        let pieces = stream.len().div_ceil(created.piece_length as usize);
        assert_eq!(pieces as u32, meta.info.piece_count());
        for (index, piece) in stream.chunks(created.piece_length as usize).enumerate() {
            verifier
                .verify(index as u32, piece)
                .unwrap_or_else(|e| panic!("{version:?} piece {index}: {e}"));
        }
        let mut corrupt = stream[..stream.len().min(created.piece_length as usize)].to_vec();
        corrupt[0] ^= 1;
        assert!(verifier.verify(0, &corrupt).is_err());
    }

    fn write_tree(root: &Path, files: &[(&str, usize)]) {
        for (i, (path, len)) in files.iter().enumerate() {
            let path = root.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, pattern(*len, i as u8)).unwrap();
        }
    }

    const VERSIONS: [MetaVersion; 3] = [MetaVersion::V1, MetaVersion::V2, MetaVersion::Hybrid];

    #[test]
    fn single_files_of_awkward_sizes_round_trip() {
        let block = BLOCK_SIZE as usize;
        let piece = PIECE as usize;
        for len in [
            1,
            block - 1,
            block,
            block + 1,
            piece,
            piece + 1,
            3 * piece + 5,
        ] {
            let tmp = tempfile::tempdir().unwrap();
            let file = tmp.path().join("payload.bin");
            std::fs::write(&file, pattern(len, 7)).unwrap();
            for version in VERSIONS {
                let created = create(&file, &opts(version));
                assert_eq!(created.name, "payload.bin");
                assert_eq!(created.total_length, len as u64);
                assert_eq!(created.file_count, 1);
                assert_every_piece_verifies(&created, &file, version);
            }
        }
    }

    #[test]
    fn directories_round_trip_in_file_tree_order() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("album");
        let block = BLOCK_SIZE as usize;
        let piece = PIECE as usize;
        write_tree(
            &root,
            &[
                ("a-b", 10),
                ("a/b", block + 3),
                ("a/c/deep.bin", 2 * piece + 1),
                ("empty", 0),
                ("z.bin", piece),
                ("m/one", 1),
                ("m/exact", block),
            ],
        );
        for version in VERSIONS {
            let created = create(&root, &opts(version));
            assert_eq!(created.name, "album");
            assert_eq!(created.file_count, 7);
            assert_every_piece_verifies(&created, &root, version);
            let meta = parse_torrent(&created.bytes).unwrap();
            let paths: Vec<String> = meta
                .info
                .files
                .iter()
                .filter(|f| !f.padding)
                .map(|f| f.path.join("/"))
                .collect();
            let expected: &[&str] = if version == MetaVersion::V2 {
                &[
                    "a/b",
                    "a/c/deep.bin",
                    "a-b",
                    "m/exact",
                    "m/one",
                    "z.bin",
                    "empty",
                ]
            } else {
                &[
                    "a/b",
                    "a/c/deep.bin",
                    "a-b",
                    "empty",
                    "m/exact",
                    "m/one",
                    "z.bin",
                ]
            };
            assert_eq!(paths, expected, "{version:?}");
            if version == MetaVersion::V1 {
                assert!(meta.info.files.iter().all(|f| !f.padding));
            }
        }
    }

    #[test]
    fn hybrid_pads_every_file_but_the_last_to_a_piece_boundary() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("set");
        write_tree(&root, &[("a", 100), ("b", 200), ("c", 300)]);
        let created = create(&root, &opts(MetaVersion::Hybrid));
        let meta = parse_torrent(&created.bytes).unwrap();
        let layout: Vec<(bool, u64)> = meta
            .info
            .files
            .iter()
            .map(|f| (f.padding, f.length))
            .collect();
        let pad = |len: u64| (true, PIECE as u64 - len);
        assert_eq!(
            layout,
            [(false, 100), pad(100), (false, 200), pad(200), (false, 300)]
        );
        let planned = plan(&root, &opts(MetaVersion::Hybrid)).unwrap();
        assert_eq!(planned.hybrid_padding(), 2 * PIECE as u64 - 300);
        assert_eq!(
            planned.hybrid_padding_with(2 * PIECE),
            4 * PIECE as u64 - 300
        );
    }

    #[test]
    fn piece_layers_cover_only_files_longer_than_a_piece() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("layers");
        let piece = PIECE as usize;
        write_tree(&root, &[("big", 3 * piece), ("exact", piece), ("small", 5)]);
        let created = create(&root, &opts(MetaVersion::V2));
        let meta = parse_torrent(&created.bytes).unwrap();
        assert_eq!(meta.piece_layers.len(), 1);
        let big = &meta.info_v2.as_ref().unwrap().files[0];
        assert_eq!(big.path.last().map(String::as_str), Some("big"));
        assert_eq!(meta.piece_layers[&big.pieces_root].len(), 3 * 32);
    }

    #[test]
    fn identical_files_share_one_piece_layer() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("twins");
        let data = pattern(3 * PIECE as usize, 1);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a.bin"), &data).unwrap();
        std::fs::write(root.join("b.bin"), &data).unwrap();
        for version in [MetaVersion::V2, MetaVersion::Hybrid] {
            let created = create(&root, &opts(version));
            assert_every_piece_verifies(&created, &root, version);
            assert_eq!(parse_torrent(&created.bytes).unwrap().piece_layers.len(), 1);
        }
    }

    #[test]
    fn v2_keeps_a_top_folder_named_like_the_torrent() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("Pack");
        write_tree(&root, &[("Pack/a.bin", 1000)]);
        let created = create(&root, &opts(MetaVersion::V2));
        assert_every_piece_verifies(&created, &root, MetaVersion::V2);
        let meta = parse_torrent(&created.bytes).unwrap();
        assert!(!meta.info.single_file_mode);
        assert_eq!(meta.info.files[0].path, ["Pack", "a.bin"]);
    }

    #[test]
    fn v2_folder_holding_only_its_namesake_seeds_from_the_folder() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("Same");
        write_tree(&root, &[("Same", 1000)]);
        let file = root.join("Same");
        let created = create(&root, &opts(MetaVersion::V2));
        assert!(parse_torrent(&created.bytes).unwrap().info.single_file_mode);
        assert_eq!(
            created.seed_dir.join(&created.name),
            dunce::canonicalize(&file).unwrap()
        );
        assert_every_piece_verifies(&created, &file, MetaVersion::V2);
        for version in [MetaVersion::V1, MetaVersion::Hybrid] {
            let created = create(&root, &opts(version));
            assert!(!parse_torrent(&created.bytes).unwrap().info.single_file_mode);
            assert_eq!(created.seed_dir, dunce::canonicalize(tmp.path()).unwrap());
        }
    }

    #[test]
    fn metadata_options_are_written() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("f.bin");
        std::fs::write(&file, pattern(1000, 1)).unwrap();
        let created = create(
            &file,
            &CreateOptions {
                trackers: vec![
                    vec!["udp://t1.example:80/announce".into(), " ".into()],
                    vec![],
                    vec!["https://t2.example/announce".into()],
                ],
                web_seeds: vec!["https://cdn.example/f.bin".into()],
                comment: Some("hello".into()),
                source: Some("SRC".into()),
                private: true,
                ..opts(MetaVersion::Hybrid)
            },
        );
        let meta = parse_torrent(&created.bytes).unwrap();
        assert_eq!(
            meta.announce.as_deref(),
            Some("udp://t1.example:80/announce")
        );
        assert_eq!(
            meta.announce_list,
            [
                vec!["udp://t1.example:80/announce".to_string()],
                vec!["https://t2.example/announce".to_string()]
            ]
        );
        assert_eq!(meta.url_list, ["https://cdn.example/f.bin"]);
        assert_eq!(meta.comment.as_deref(), Some("hello"));
        assert_eq!(meta.creation_date, Some(1_700_000_000));
        assert!(meta.info.private);
        let info = crate::bencode::decode_all(&meta.info_bytes).unwrap();
        assert_eq!(info.get(b"source").and_then(Value::as_str), Some("SRC"));
        let magnet = crate::core::Magnet::parse(&created.magnet).unwrap();
        assert_eq!(magnet.info_hash_v1(), created.info_hash_v1);
        assert_eq!(magnet.info_hash_v2(), created.info_hash_v2);
        assert_eq!(magnet.display_name.as_deref(), Some("f.bin"));
        assert_eq!(magnet.trackers.len(), 2);
    }

    #[test]
    fn the_same_input_gives_the_same_info_hash() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("same");
        write_tree(&root, &[("x", 70_000), ("y/z", 3)]);
        let a = create(&root, &opts(MetaVersion::Hybrid));
        let b = create(&root, &opts(MetaVersion::Hybrid));
        assert_eq!(a.bytes, b.bytes);
        let v1 = create(&root, &opts(MetaVersion::V1));
        assert_eq!(v1.info_hash_v2, None);
        assert_ne!(v1.info_hash_v1, a.info_hash_v1);
    }

    #[cfg(unix)]
    #[test]
    fn symlinks_junk_and_excluded_files_are_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("pack");
        write_tree(&root, &[("keep", 10), (".DS_Store", 4), ("out.torrent", 9)]);
        std::os::unix::fs::symlink(root.join("keep"), root.join("link")).unwrap();
        std::os::unix::fs::symlink(tmp.path(), root.join("loop")).unwrap();
        let created = create(
            &root,
            &CreateOptions {
                exclude: vec![root.join("out.torrent")],
                ..opts(MetaVersion::Hybrid)
            },
        );
        assert_eq!(created.file_count, 1);
        assert_eq!(created.total_length, 10);
    }

    #[cfg(unix)]
    #[test]
    fn executable_files_carry_the_x_attribute() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("bin");
        write_tree(&root, &[("run.sh", 10), ("data", 10)]);
        std::fs::set_permissions(root.join("run.sh"), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        for version in VERSIONS {
            let meta = parse_torrent(&create(&root, &opts(version)).bytes).unwrap();
            let exec: Vec<(String, bool)> = meta
                .info
                .files
                .iter()
                .filter(|f| !f.padding)
                .map(|f| (f.path.join("/"), f.executable))
                .collect();
            assert_eq!(
                exec,
                [("data".to_string(), false), ("run.sh".to_string(), true)],
                "{version:?}"
            );
        }
    }

    #[test]
    fn empty_inputs_and_bad_piece_lengths_are_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let empty_dir = tmp.path().join("nothing");
        std::fs::create_dir(&empty_dir).unwrap();
        let cancel = AtomicBool::new(false);
        let err = |root: &Path, opts: &CreateOptions| {
            create_torrent(root, opts, |_, _| {}, &cancel).unwrap_err()
        };
        assert!(matches!(
            err(&empty_dir, &opts(MetaVersion::V1)),
            CreateError::Empty
        ));
        let zero = tmp.path().join("zero");
        std::fs::write(&zero, b"").unwrap();
        assert!(matches!(
            err(&zero, &opts(MetaVersion::V2)),
            CreateError::Empty
        ));
        let file = tmp.path().join("f");
        std::fs::write(&file, b"abc").unwrap();
        for bad in [0, 8 * 1024, 48 * 1024, 128 * 1024 * 1024] {
            let o = CreateOptions {
                piece_length: Some(bad),
                ..opts(MetaVersion::V1)
            };
            assert!(
                matches!(err(&file, &o), CreateError::BadPieceLength),
                "{bad}"
            );
        }
    }

    #[test]
    fn cancelling_stops_hashing() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("f");
        let total = 64 * PIECE as u64;
        std::fs::write(&file, pattern(total as usize, 0)).unwrap();
        let cancel = AtomicBool::new(false);
        let mut calls = Vec::new();
        let result = create_torrent(
            &file,
            &opts(MetaVersion::Hybrid),
            |done, total| {
                calls.push((done, total));
                if done >= PIECE as u64 {
                    cancel.store(true, Ordering::Relaxed);
                }
            },
            &cancel,
        );
        assert!(matches!(result, Err(CreateError::Cancelled)));
        assert_eq!(calls[0], (0, total));
        assert!(calls.iter().all(|&(done, _)| done < total), "{calls:?}");
    }

    #[test]
    fn hashing_threads_do_not_change_the_torrent() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("mixed");
        let piece = PIECE as usize;
        write_tree(
            &root,
            &[
                ("a", piece / 3),
                ("b/c", 2 * piece + 7),
                ("b/d", 1),
                ("e", 0),
                ("f", piece),
                ("g", 3 * piece - 1),
                ("h", piece / 2),
            ],
        );
        for version in VERSIONS {
            let made = |threads| {
                plan(&root, &opts(version))
                    .unwrap()
                    .create_on(threads, &opts(version), |_, _| {}, &AtomicBool::new(false))
                    .unwrap()
            };
            let one = made(1);
            for threads in [2, 4] {
                assert_eq!(one.bytes, made(threads).bytes, "{version:?} {threads}");
            }
            assert_every_piece_verifies(&one, &root, version);
        }
    }

    #[test]
    fn info_hashes_match_libtorrent() {
        let tmp = tempfile::tempdir().unwrap();
        let single = tmp.path().join("single.bin");
        std::fs::write(&single, pattern(100_003, 6)).unwrap();
        let album = tmp.path().join("album2");
        for (path, len, seed) in [
            ("a/b", 16_387, 1),
            ("a/c/deep.bin", 65_537, 2),
            ("m/exact", 16_384, 3),
            ("m/one", 1, 4),
            ("z.bin", 32_768, 5),
            ("m/empty", 0, 0),
        ] {
            let path = album.join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, pattern(len, seed)).unwrap();
        }
        let hashes = |root: &Path, version| {
            let created = create(root, &opts(version));
            (
                created.info_hash_v1.map(|h| h.to_hex()),
                created.info_hash_v2.map(|h| h.to_hex()),
            )
        };
        let expect = |v1: Option<&str>, v2: &str| (v1.map(String::from), Some(v2.to_string()));
        assert_eq!(
            hashes(&single, MetaVersion::V2),
            expect(
                None,
                "9a5fb2b7d62a2109571c9cd445f5217a55b38a6fd7339f53d81c040fff49b80c"
            )
        );
        assert_eq!(
            hashes(&single, MetaVersion::Hybrid),
            expect(
                Some("de15f4e0d7d8eca1fdcb2f933985975a40789246"),
                "118b0f647d9344dc53495863899e7c950ccaf6892434a80f22ec1885761d20b6"
            )
        );
        assert_eq!(
            hashes(&album, MetaVersion::V2),
            expect(
                None,
                "f8ac6394dac28154c2431521e565e655fb8cfab4160d37a6518ad90fed210976"
            )
        );
        assert_eq!(
            hashes(&album, MetaVersion::Hybrid),
            expect(
                Some("3b78f378b81a089bb1a2d8c742b9bcc81fd194ff"),
                "b1c3c4c7083c5f818279c02d7f438dc572c81d9fc71d1b1d4e1739e0b6843532"
            )
        );
    }

    #[test]
    fn auto_piece_length_stays_in_range() {
        assert_eq!(auto_piece_length(1), MIN_PIECE_LENGTH);
        assert_eq!(auto_piece_length(1500 * 256 * 1024), 256 * 1024);
        assert_eq!(auto_piece_length(1501 * 256 * 1024), 512 * 1024);
        assert_eq!(auto_piece_length(u64::MAX), AUTO_MAX_PIECE_LENGTH);
    }
}
