use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::UNIX_EPOCH;

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use risuko_util::limiter::RateLimiter;

use super::chunks::ED2K_BLOCK_SIZE;
use super::md4::{md4, Md4};
use super::protocol::{is_user_hash, new_user_hash};
use super::types::*;

pub const MAX_REQUEST_SPAN: u64 = ED2K_BLOCK_SIZE * 3;

const USER_HASH_FILENAME: &str = "ed2k-userhash";
const KNOWN_DIR: &str = "ed2k-known";
const KNOWN_VERSION: u32 = 1;

#[derive(Clone)]
pub struct PartMap(Arc<RwLock<PartState>>);

struct PartState {
    verified: Vec<bool>,
    hashset: Option<Vec<[u8; 16]>>,
}

impl PartMap {
    pub fn new(file_size: u64) -> Self {
        Self(Arc::new(RwLock::new(PartState {
            verified: vec![false; chunk_count(file_size) as usize],
            hashset: None,
        })))
    }

    pub fn mark_verified(&self, part: usize) {
        if let Some(slot) = self.0.write().verified.get_mut(part) {
            *slot = true;
        }
    }

    pub fn set_hashset(&self, hashes: &[[u8; 16]]) {
        let mut state = self.0.write();
        if state.hashset.is_none() {
            state.hashset = Some(hashes.to_vec());
        }
    }

    pub fn hashset(&self) -> Option<Vec<[u8; 16]>> {
        self.0.read().hashset.clone()
    }
}

enum Availability {
    Complete(Vec<[u8; 16]>),
    Partial(PartMap),
}

#[derive(Default)]
pub struct UploadStats {
    uploaded: AtomicU64,
    speed: AtomicU64,
    sampled: AtomicU64,
    peers: AtomicU32,
}

impl UploadStats {
    pub fn with_uploaded(uploaded: u64) -> Self {
        Self {
            uploaded: AtomicU64::new(uploaded),
            sampled: AtomicU64::new(uploaded),
            ..Self::default()
        }
    }

    pub fn uploaded(&self) -> u64 {
        self.uploaded.load(Ordering::Relaxed)
    }

    pub fn speed(&self) -> u64 {
        self.speed.load(Ordering::Relaxed)
    }

    pub fn peers(&self) -> u32 {
        self.peers.load(Ordering::Relaxed)
    }

    pub(crate) fn add(&self, bytes: u64) {
        self.uploaded.fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn peer_joined(&self) {
        self.peers.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn peer_left(&self) {
        self.peers.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn sample(&self) {
        let now = self.uploaded();
        let before = self.sampled.swap(now, Ordering::Relaxed);
        self.speed
            .store(now.saturating_sub(before), Ordering::Relaxed);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileStamp {
    pub len: u64,
    pub mtime_secs: u64,
    pub mtime_nanos: u32,
}

impl FileStamp {
    pub fn of(path: &Path) -> io::Result<Self> {
        Self::from_metadata(&std::fs::metadata(path)?)
    }

    fn from_metadata(meta: &std::fs::Metadata) -> io::Result<Self> {
        let modified = meta
            .modified()?
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        Ok(Self {
            len: meta.len(),
            mtime_secs: modified.as_secs(),
            mtime_nanos: modified.subsec_nanos(),
        })
    }
}

pub struct SharedFile {
    hash: [u8; 16],
    name: String,
    size: u64,
    path: PathBuf,
    availability: Availability,
    stamp: Option<FileStamp>,
    stats: Arc<UploadStats>,
    limiter: Arc<RateLimiter>,
    handle: Mutex<Option<Arc<std::fs::File>>>,
    withdrawn: AtomicBool,
}

impl SharedFile {
    pub fn complete(
        hash: [u8; 16],
        name: impl Into<String>,
        size: u64,
        path: PathBuf,
        hashset: Vec<[u8; 16]>,
    ) -> Self {
        Self::new(
            hash,
            name.into(),
            size,
            path,
            Availability::Complete(hashset),
        )
    }

    pub fn partial(
        hash: [u8; 16],
        name: impl Into<String>,
        size: u64,
        path: PathBuf,
        parts: PartMap,
    ) -> Self {
        Self::new(hash, name.into(), size, path, Availability::Partial(parts))
    }

    fn new(
        hash: [u8; 16],
        name: String,
        size: u64,
        path: PathBuf,
        availability: Availability,
    ) -> Self {
        Self {
            hash,
            name,
            size,
            path,
            availability,
            stamp: None,
            stats: Arc::new(UploadStats::default()),
            limiter: Arc::new(RateLimiter::unlimited()),
            handle: Mutex::new(None),
            withdrawn: AtomicBool::new(false),
        }
    }

    pub fn with_stamp(mut self, stamp: FileStamp) -> Self {
        self.stamp = Some(stamp);
        self
    }

    pub fn hash(&self) -> [u8; 16] {
        self.hash
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn is_complete(&self) -> bool {
        matches!(self.availability, Availability::Complete(_))
    }

    pub fn is_large(&self) -> bool {
        is_large_file(self.size)
    }

    pub fn stats(&self) -> &Arc<UploadStats> {
        &self.stats
    }

    pub fn limiter(&self) -> &Arc<RateLimiter> {
        &self.limiter
    }

    pub fn is_withdrawn(&self) -> bool {
        self.withdrawn.load(Ordering::Relaxed)
    }

    pub fn is_offerable(&self) -> bool {
        if self.is_withdrawn() {
            return false;
        }
        match &self.availability {
            Availability::Complete(_) => self.size > 0,
            Availability::Partial(parts) => {
                self.size > ED2K_CHUNK_SIZE && parts.0.read().verified.iter().any(|v| *v)
            }
        }
    }

    pub fn status_payload(&self) -> Vec<u8> {
        let mut out = self.hash.to_vec();
        match &self.availability {
            Availability::Complete(_) => out.extend_from_slice(&0u16.to_le_bytes()),
            Availability::Partial(parts) => {
                let state = parts.0.read();
                let count = ed2k_part_count(self.size);
                out.extend_from_slice(&count.to_le_bytes());
                let mut bits = vec![0u8; usize::from(count).div_ceil(8)];
                for (i, verified) in state.verified.iter().enumerate() {
                    if *verified && i < usize::from(count) {
                        bits[i / 8] |= 1 << (i % 8);
                    }
                }
                out.extend_from_slice(&bits);
            }
        }
        out
    }

    pub fn hashset(&self) -> Option<Vec<[u8; 16]>> {
        if self.size < ED2K_CHUNK_SIZE {
            return None;
        }
        match &self.availability {
            Availability::Complete(hashes) => (!hashes.is_empty()).then(|| hashes.clone()),
            Availability::Partial(parts) => parts.0.read().hashset.clone(),
        }
    }

    pub fn range_available(&self, start: u64, end: u64) -> bool {
        if start >= end || end > self.size || end - start > MAX_REQUEST_SPAN {
            return false;
        }
        match &self.availability {
            Availability::Complete(_) => true,
            Availability::Partial(parts) => {
                let state = parts.0.read();
                let first = (start / ED2K_CHUNK_SIZE) as usize;
                let last = ((end - 1) / ED2K_CHUNK_SIZE) as usize;
                (first..=last).all(|part| state.verified.get(part).copied().unwrap_or(false))
            }
        }
    }

    pub fn read_range(&self, start: u64, end: u64) -> io::Result<Vec<u8>> {
        if !self.range_available(start, end) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "requested range is not shared",
            ));
        }
        let file = self.open()?;
        if let Some(stamp) = self.stamp {
            if FileStamp::from_metadata(&file.metadata()?)? != stamp {
                return Err(io::Error::other("shared file changed on disk"));
            }
        }
        let mut buf = vec![0u8; (end - start) as usize];
        read_exact_at(&file, &mut buf, start)?;
        Ok(buf)
    }

    fn open(&self) -> io::Result<Arc<std::fs::File>> {
        let mut handle = self.handle.lock();
        if let Some(file) = handle.as_ref() {
            return Ok(file.clone());
        }
        let file = Arc::new(std::fs::File::open(&self.path)?);
        if file.metadata()?.len() < self.size {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "shared file is shorter than expected",
            ));
        }
        *handle = Some(file.clone());
        Ok(file)
    }
}

#[derive(Clone)]
pub struct ShareContext {
    pub registry: Arc<ShareRegistry>,
    pub user_hash: [u8; 16],
    pub stats: Arc<UploadStats>,
    pub limiter: Arc<RateLimiter>,
}

impl ShareContext {
    pub fn share(&self, mut file: SharedFile) -> ShareHandle {
        file.stats = self.stats.clone();
        file.limiter = self.limiter.clone();
        self.registry.insert(file)
    }
}

fn ed2k_part_count(file_size: u64) -> u16 {
    u16::try_from(part_hash_count(file_size)).unwrap_or(u16::MAX)
}

#[cfg(unix)]
fn read_exact_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

#[cfg(windows)]
fn read_exact_at(file: &std::fs::File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        let n = file.seek_read(buf, offset)?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        buf = &mut buf[n..];
        offset += n as u64;
    }
    Ok(())
}

pub struct ShareRegistry {
    files: RwLock<HashMap<[u8; 16], Vec<Arc<SharedFile>>>>,
    changed: watch::Sender<u64>,
}

fn preferred(shares: &[Arc<SharedFile>]) -> Option<&Arc<SharedFile>> {
    shares
        .iter()
        .max_by_key(|file| (file.is_offerable(), file.is_complete()))
}

impl ShareRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            files: RwLock::new(HashMap::new()),
            changed: watch::channel(0).0,
        })
    }

    pub fn insert(self: &Arc<Self>, file: SharedFile) -> ShareHandle {
        let file = Arc::new(file);
        self.files
            .write()
            .entry(file.hash)
            .or_default()
            .push(file.clone());
        self.bump();
        ShareHandle {
            registry: Arc::downgrade(self),
            file,
        }
    }

    pub fn get(&self, hash: &[u8; 16]) -> Option<Arc<SharedFile>> {
        self.files
            .read()
            .get(hash)
            .and_then(|shares| preferred(shares))
            .cloned()
    }

    pub fn files(&self) -> Vec<Arc<SharedFile>> {
        self.files
            .read()
            .values()
            .filter_map(|shares| preferred(shares))
            .cloned()
            .collect()
    }

    pub(crate) fn sample_stats(&self) {
        for file in self.files.read().values().flatten() {
            file.stats.sample();
        }
    }

    pub fn has_offerable(&self) -> bool {
        self.files
            .read()
            .values()
            .flatten()
            .any(|f| f.is_offerable())
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    fn remove(&self, file: &Arc<SharedFile>) {
        let removed = {
            let mut files = self.files.write();
            let Some(shares) = files.get_mut(&file.hash) else {
                return;
            };
            let before = shares.len();
            shares.retain(|f| !Arc::ptr_eq(f, file));
            let removed = shares.len() != before;
            if shares.is_empty() {
                files.remove(&file.hash);
            }
            removed
        };
        if removed {
            self.bump();
        }
    }

    fn bump(&self) {
        self.changed.send_modify(|generation| *generation += 1);
    }
}

pub struct ShareHandle {
    registry: Weak<ShareRegistry>,
    file: Arc<SharedFile>,
}

impl ShareHandle {
    pub fn file(&self) -> &Arc<SharedFile> {
        &self.file
    }
}

impl Drop for ShareHandle {
    fn drop(&mut self) {
        self.file.withdrawn.store(true, Ordering::Relaxed);
        if let Some(registry) = self.registry.upgrade() {
            registry.remove(&self.file);
        }
    }
}

pub fn hash_file(path: &Path, cancel: &CancellationToken) -> io::Result<([u8; 16], Vec<[u8; 16]>)> {
    use std::io::Read;
    let mut file = std::fs::File::open(path)?;
    let size = file.metadata()?.len();
    let mut buf = vec![0u8; 256 * 1024];
    let count = part_hash_count(size);
    let mut hashes = Vec::with_capacity(count as usize);
    let mut remaining = size;
    for _ in 0..count {
        let mut hasher = Md4::new();
        let mut left = remaining.min(ED2K_CHUNK_SIZE);
        remaining -= left;
        while left > 0 {
            if cancel.is_cancelled() {
                return Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled"));
            }
            let want = left.min(buf.len() as u64) as usize;
            file.read_exact(&mut buf[..want])?;
            hasher.update(&buf[..want]);
            left -= want as u64;
        }
        hashes.push(hasher.finalize());
    }
    if size < ED2K_CHUNK_SIZE {
        return Ok((hashes[0], Vec::new()));
    }
    Ok((md4(&hashes.concat()), hashes))
}

pub(crate) fn hashset_matches(file_hash: &[u8; 16], size: u64, hashset: &[[u8; 16]]) -> bool {
    if size < ED2K_CHUNK_SIZE {
        return hashset.is_empty();
    }
    hashset.len() as u64 == part_hash_count(size) && md4(&hashset.concat()) == *file_hash
}

#[derive(Serialize, Deserialize)]
struct KnownRecord {
    version: u32,
    file_hash: String,
    path: String,
    stamp: FileStamp,
    hashset: Vec<String>,
}

fn known_path(config_dir: &Path, hash: &[u8; 16]) -> PathBuf {
    config_dir
        .join(KNOWN_DIR)
        .join(format!("{}.json", hex::encode(hash)))
}

fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    super::partial::write_atomic(path, data)
}

fn save_known(
    config_dir: &Path,
    file_hash: &[u8; 16],
    path: &Path,
    stamp: FileStamp,
    hashset: &[[u8; 16]],
) -> io::Result<()> {
    let path = path
        .to_str()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "path is not UTF-8"))?;
    let record = KnownRecord {
        version: KNOWN_VERSION,
        file_hash: hex::encode(file_hash),
        path: path.to_string(),
        stamp,
        hashset: hashset.iter().map(hex::encode).collect(),
    };
    let data = serde_json::to_vec(&record).map_err(io::Error::other)?;
    write_atomic(&known_path(config_dir, file_hash), &data)
}

fn load_known(
    config_dir: &Path,
    file_hash: &[u8; 16],
    path: &Path,
    stamp: FileStamp,
) -> Option<Vec<[u8; 16]>> {
    let bytes = std::fs::read(known_path(config_dir, file_hash)).ok()?;
    let record: KnownRecord = serde_json::from_slice(&bytes).ok()?;
    if record.version != KNOWN_VERSION
        || !record
            .file_hash
            .eq_ignore_ascii_case(&hex::encode(file_hash))
        || Some(record.path.as_str()) != path.to_str()
        || record.stamp != stamp
    {
        return None;
    }
    let hashset = super::partial::decode_hashset(&record.hashset)?;
    hashset_matches(file_hash, stamp.len, &hashset).then_some(hashset)
}

pub fn remove_known(config_dir: &Path, file_hash: &[u8; 16]) {
    super::partial::remove_quiet(&known_path(config_dir, file_hash));
}

#[derive(Debug, Clone)]
pub struct VerifiedFile {
    pub hashset: Vec<[u8; 16]>,
    pub stamp: FileStamp,
}

fn stamp_of(path: &Path) -> Result<FileStamp, String> {
    FileStamp::of(path).map_err(|e| format!("Cannot read {}: {e}", path.display()))
}

fn remember(
    config_dir: &Path,
    path: &Path,
    file_hash: &[u8; 16],
    stamp: FileStamp,
    hashset: Vec<[u8; 16]>,
) -> VerifiedFile {
    if let Err(e) = save_known(config_dir, file_hash, path, stamp, &hashset) {
        tracing::warn!("[ed2k] could not save known-file record: {e}");
    }
    VerifiedFile { hashset, stamp }
}

pub async fn remember_complete_file(
    config_dir: PathBuf,
    path: PathBuf,
    file_hash: [u8; 16],
    hashset: Vec<[u8; 16]>,
) -> Result<VerifiedFile, String> {
    tokio::task::spawn_blocking(move || {
        let stamp = stamp_of(&path)?;
        if !hashset_matches(&file_hash, stamp.len, &hashset) {
            return Err("Part hashes do not match the file hash".to_string());
        }
        Ok(remember(&config_dir, &path, &file_hash, stamp, hashset))
    })
    .await
    .map_err(|e| format!("Known-file task failed: {e}"))?
}

pub async fn verify_complete_file(
    config_dir: PathBuf,
    path: PathBuf,
    file_hash: [u8; 16],
    file_size: u64,
    cancel: CancellationToken,
) -> Result<VerifiedFile, String> {
    tokio::task::spawn_blocking(move || {
        let stamp = stamp_of(&path)?;
        if stamp.len != file_size {
            return Err(format!(
                "{} is {} bytes, expected {}",
                path.display(),
                stamp.len,
                file_size
            ));
        }
        if let Some(hashset) = load_known(&config_dir, &file_hash, &path, stamp) {
            return Ok(VerifiedFile { hashset, stamp });
        }
        let (hash, hashset) = hash_file(&path, &cancel).map_err(|e| {
            if e.kind() == io::ErrorKind::Interrupted {
                "cancelled".to_string()
            } else {
                format!("Cannot hash {}: {e}", path.display())
            }
        })?;
        if hash != file_hash {
            return Err(format!(
                "{} no longer matches its eD2K hash",
                path.display()
            ));
        }
        if stamp_of(&path)? != stamp {
            return Err(format!("{} changed while it was hashed", path.display()));
        }
        Ok(remember(&config_dir, &path, &file_hash, stamp, hashset))
    })
    .await
    .map_err(|e| format!("Hash task failed: {e}"))?
}

pub fn load_or_create_user_hash(config_dir: &Path) -> io::Result<[u8; 16]> {
    let path = config_dir.join(USER_HASH_FILENAME);
    let stored = std::fs::read_to_string(&path)
        .ok()
        .and_then(|text| hex::decode(text.trim()).ok())
        .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
        .filter(is_user_hash);
    if let Some(hash) = stored {
        return Ok(hash);
    }
    let hash = new_user_hash();
    write_atomic(&path, hex::encode(hash).as_bytes())?;
    Ok(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn write(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }

    #[test]
    fn hash_file_follows_the_emule_part_convention() {
        let dir = tempfile::tempdir().unwrap();
        let cancel = CancellationToken::new();

        let small = data(1000);
        let (hash, set) = hash_file(&write(dir.path(), "s", &small), &cancel).unwrap();
        assert_eq!((hash, set.len()), (md4(&small), 0));

        let exact = data(ED2K_CHUNK_SIZE as usize);
        let (hash, set) = hash_file(&write(dir.path(), "e", &exact), &cancel).unwrap();
        assert_eq!(set, vec![md4(&exact), md4(b"")]);
        assert_eq!(hash, md4(&[md4(&exact), md4(b"")].concat()));
        assert!(hashset_matches(&hash, exact.len() as u64, &set));

        let two = data(ED2K_CHUNK_SIZE as usize + 10);
        let (hash, set) = hash_file(&write(dir.path(), "t", &two), &cancel).unwrap();
        assert_eq!(set.len(), 2);
        assert_eq!(set[1], md4(&two[ED2K_CHUNK_SIZE as usize..]));
        assert!(hashset_matches(&hash, two.len() as u64, &set));
        assert!(!hashset_matches(&hash, two.len() as u64, &set[..1]));
    }

    #[test]
    fn partial_status_lists_verified_parts_with_the_ed2k_part_count() {
        let size = ED2K_CHUNK_SIZE * 2;
        let parts = PartMap::new(size);
        let file = SharedFile::partial([1; 16], "f", size, PathBuf::from("x"), parts.clone());
        assert!(!file.is_offerable());
        parts.mark_verified(1);
        assert!(file.is_offerable());
        let status = file.status_payload();
        assert_eq!(&status[16..18], &3u16.to_le_bytes());
        assert_eq!(status[18], 0b010);
        assert!(file.range_available(ED2K_CHUNK_SIZE, ED2K_CHUNK_SIZE + 100));
        assert!(!file.range_available(ED2K_CHUNK_SIZE - 1, ED2K_CHUNK_SIZE + 1));
        assert!(!file.range_available(0, 10));
        assert!(!file.range_available(size - 1, size + 1));
        assert!(!file.range_available(ED2K_CHUNK_SIZE, ED2K_CHUNK_SIZE + MAX_REQUEST_SPAN + 1));

        let complete = SharedFile::complete([1; 16], "f", 10, PathBuf::from("x"), Vec::new());
        assert_eq!(&complete.status_payload()[16..], &[0, 0]);
        assert!(complete.hashset().is_none());
    }

    #[test]
    fn single_part_partials_are_never_offered() {
        let parts = PartMap::new(1000);
        parts.mark_verified(0);
        let file = SharedFile::partial([1; 16], "f", 1000, PathBuf::from("x"), parts);
        assert!(!file.is_offerable());
    }

    #[test]
    fn reads_refuse_a_file_that_changed_since_it_was_stamped() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = data(5000);
        let path = write(dir.path(), "f", &bytes);
        let stamp = FileStamp::of(&path).unwrap();
        let file = SharedFile::complete(md4(&bytes), "f", 5000, path.clone(), Vec::new())
            .with_stamp(stamp);
        assert_eq!(file.read_range(10, 20).unwrap(), bytes[10..20].to_vec());
        let later = std::time::SystemTime::now() + std::time::Duration::from_secs(5);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(later)
            .unwrap();
        assert!(file.read_range(10, 20).is_err());
    }

    #[test]
    fn registry_handles_only_remove_their_own_entry() {
        let registry = ShareRegistry::new();
        let complete = |name| SharedFile::complete([7; 16], name, 1, PathBuf::new(), Vec::new());
        let first = registry.insert(complete("a"));
        let second = registry.insert(complete("b"));
        assert_eq!(registry.get(&[7; 16]).unwrap().name(), "b");
        assert_eq!(registry.files().len(), 1);
        drop(first);
        assert_eq!(registry.get(&[7; 16]).unwrap().name(), "b");
        drop(second);
        assert!(registry.get(&[7; 16]).is_none());

        let older = registry.insert(complete("a"));
        let newer = registry.insert(complete("b"));
        drop(newer);
        assert_eq!(registry.get(&[7; 16]).unwrap().name(), "a");
        assert!(registry.has_offerable());
        drop(older);
        assert!(registry.files().is_empty());
    }

    #[test]
    fn a_complete_share_wins_over_a_newer_partial_of_the_same_file() {
        let registry = ShareRegistry::new();
        let size = ED2K_CHUNK_SIZE * 2;
        let _complete = registry.insert(SharedFile::complete(
            [7; 16],
            "done",
            size,
            PathBuf::new(),
            Vec::new(),
        ));
        let parts = PartMap::new(size);
        let _partial = registry.insert(SharedFile::partial(
            [7; 16],
            "part",
            size,
            PathBuf::new(),
            parts.clone(),
        ));
        assert_eq!(registry.get(&[7; 16]).unwrap().name(), "done");
        parts.mark_verified(0);
        assert_eq!(registry.get(&[7; 16]).unwrap().name(), "done");
    }

    #[tokio::test]
    async fn known_record_skips_rehash_until_the_file_changes() {
        let dir = tempfile::tempdir().unwrap();
        let bytes = data(ED2K_CHUNK_SIZE as usize + 77);
        let path = write(dir.path(), "f.bin", &bytes);
        let (hash, set) = hash_file(&path, &CancellationToken::new()).unwrap();
        let config = dir.path().join("cfg");

        let first = verify_complete_file(
            config.clone(),
            path.clone(),
            hash,
            bytes.len() as u64,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        assert_eq!(first.hashset, set);
        let stamp = FileStamp::of(&path).unwrap();
        assert_eq!(load_known(&config, &hash, &path, stamp), Some(set.clone()));

        let mut changed = bytes.clone();
        changed[5] ^= 1;
        std::fs::write(&path, &changed).unwrap();
        let later = std::time::SystemTime::now() + std::time::Duration::from_secs(5);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(later)
            .unwrap();
        let err = verify_complete_file(
            config.clone(),
            path.clone(),
            hash,
            bytes.len() as u64,
            CancellationToken::new(),
        )
        .await
        .unwrap_err();
        assert!(err.contains("no longer matches"));
        remove_known(&config, &hash);
        assert!(!known_path(&config, &hash).exists());
    }

    #[test]
    fn user_hash_is_created_once_and_reused() {
        let dir = tempfile::tempdir().unwrap();
        let first = load_or_create_user_hash(dir.path()).unwrap();
        assert_eq!(load_or_create_user_hash(dir.path()).unwrap(), first);
        std::fs::write(dir.path().join(USER_HASH_FILENAME), "garbage").unwrap();
        let replaced = load_or_create_user_hash(dir.path()).unwrap();
        assert_eq!((replaced[5], replaced[14]), (14, 111));
    }
}
