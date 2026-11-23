use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::Mutex;
use tokio::task;

use super::core::metainfo::ValidatedTorrentMetaV1Info;

pub mod file_info;

pub use file_info::{FileInfo, FileSet};

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("offset {offset} out of torrent range ({total})")]
    OutOfRange { offset: u64, total: u64 },
}

#[cfg(unix)]
fn executable_bits(mode: u32) -> u32 {
    0o100 | ((mode & 0o040) >> 2) | ((mode & 0o004) >> 2)
}

#[cfg(unix)]
fn set_executable(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let meta = file.metadata()?;
    let mode = meta.permissions().mode() & 0o7777;
    if !meta.is_file() || mode & 0o111 != 0 {
        return Ok(());
    }
    file.set_permissions(std::fs::Permissions::from_mode(
        mode | executable_bits(mode),
    ))
}

const MAX_OPEN_HANDLES: usize = 64;
const HANDLE_BUDGET_FLOOR: usize = 8;
const EVICTION_MARGIN: usize = 16;
const EVICTION_PASSES: usize = 2;

static HANDLE_TICK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static CACHED_HANDLES: AtomicUsize = AtomicUsize::new(0);
static HANDLE_BUDGET: AtomicUsize = AtomicUsize::new(512);
static HANDLE_REGISTRY: HandleRegistry = Mutex::new(Vec::new());

pub fn set_handle_budget(budget: usize) {
    HandleQuota::process().set_limit(budget);
}

pub fn cached_handle_count() -> usize {
    HandleQuota::process().cached()
}

const MIN_HANDLE_BUDGET: u64 = 64;
const MAX_HANDLE_BUDGET: u64 = 4_096;
const UNLIMITED_HANDLE_BUDGET: usize = 8_192;

pub(crate) fn handle_budget_for(nofile_soft: Option<u64>) -> usize {
    match nofile_soft {
        Some(soft) => (soft / 4).clamp(MIN_HANDLE_BUDGET, MAX_HANDLE_BUDGET) as usize,
        None => UNLIMITED_HANDLE_BUDGET,
    }
}

#[derive(Clone, Copy)]
struct HandleQuota {
    cached: &'static AtomicUsize,
    limit: &'static AtomicUsize,
    registry: &'static HandleRegistry,
}

impl HandleQuota {
    fn process() -> Self {
        Self {
            cached: &CACHED_HANDLES,
            limit: &HANDLE_BUDGET,
            registry: &HANDLE_REGISTRY,
        }
    }

    fn set_limit(&self, budget: usize) {
        self.limit
            .store(budget.max(HANDLE_BUDGET_FLOOR), Ordering::Relaxed);
    }

    fn cached(&self) -> usize {
        self.cached.load(Ordering::Relaxed)
    }

    fn over_budget(&self) -> bool {
        self.cached() > self.limit.load(Ordering::Relaxed)
    }

    fn count(&self) -> Arc<CountedHandle> {
        self.cached.fetch_add(1, Ordering::Relaxed);
        Arc::new(CountedHandle(self.cached))
    }

    fn register(&self, caches: [&Arc<HandleCache>; 2], unsynced: &Arc<UnsyncedPaths>) {
        self.registry
            .lock()
            .extend(caches.map(|slots| RegisteredCache {
                slots: Arc::downgrade(slots),
                unsynced: Arc::downgrade(unsynced),
            }));
    }

    fn deregister(&self, unsynced: &Arc<UnsyncedPaths>) {
        self.registry.lock().retain(|entry| {
            entry.slots.strong_count() > 0
                && !std::ptr::eq(entry.unsynced.as_ptr(), Arc::as_ptr(unsynced))
        });
    }

    fn evict_oldest(&self, keep: &HandleCache, keep_idx: usize) {
        if !self.over_budget() {
            return;
        }
        let registry = self.registry.lock();
        if !self.over_budget() {
            return;
        }
        let target = eviction_target(self.limit.load(Ordering::Relaxed));
        let mut remaining = self.cached().saturating_sub(target);
        let caches: Vec<LiveCache> = registry
            .iter()
            .filter_map(RegisteredCache::upgrade)
            .collect();
        for _ in 0..EVICTION_PASSES {
            if remaining == 0 {
                break;
            }
            let evicted = evict_globally_oldest(&caches, keep, keep_idx, remaining);
            if evicted == 0 {
                break;
            }
            remaining = remaining.saturating_sub(evicted);
        }
    }
}

fn eviction_target(budget: usize) -> usize {
    let margin = (budget / 32).max(EVICTION_MARGIN).min(budget / 4).max(1);
    budget.saturating_sub(margin)
}

type UnsyncedPaths = Mutex<HashSet<PathBuf>>;
type HandleRegistry = Mutex<Vec<RegisteredCache>>;

struct RegisteredCache {
    slots: Weak<HandleCache>,
    unsynced: Weak<UnsyncedPaths>,
}

impl RegisteredCache {
    fn upgrade(&self) -> Option<LiveCache> {
        Some(LiveCache {
            slots: self.slots.upgrade()?,
            unsynced: self.unsynced.upgrade()?,
        })
    }
}

struct LiveCache {
    slots: Arc<HandleCache>,
    unsynced: Arc<UnsyncedPaths>,
}

fn evict_globally_oldest(
    caches: &[LiveCache],
    keep: &HandleCache,
    keep_idx: usize,
    count: usize,
) -> usize {
    let mut candidates: Vec<(u64, usize, usize)> = Vec::new();
    for (c, cache) in caches.iter().enumerate() {
        let kept = std::ptr::eq(Arc::as_ptr(&cache.slots), keep).then_some(keep_idx);
        candidates.extend(
            cache
                .slots
                .lock()
                .iter()
                .enumerate()
                .filter(|(idx, _)| Some(*idx) != kept)
                .filter_map(|(idx, slot)| slot.as_ref().map(|slot| (slot.used, c, idx))),
        );
    }
    if candidates.len() > count {
        candidates.select_nth_unstable(count);
        candidates.truncate(count);
    }
    candidates.sort_unstable_by_key(|&(_, c, idx)| (c, idx));
    let mut evicted = 0;
    for victims in candidates.chunk_by(|a, b| a.1 == b.1) {
        let cache = &caches[victims[0].1];
        let taken: Vec<HandleSlot> = {
            let mut slots = cache.slots.lock();
            let taken: Vec<HandleSlot> = victims
                .iter()
                .filter_map(|&(used, _, idx)| {
                    let slot = slots.get_mut(idx)?;
                    if slot.as_ref()?.used == used {
                        slot.take()
                    } else {
                        None
                    }
                })
                .collect();
            cache.unsynced.lock().extend(
                taken
                    .iter()
                    .filter(|slot| slot.writable)
                    .map(|slot| slot.path.clone()),
            );
            taken
        };
        evicted += taken.len();
    }
    evicted
}

struct CountedHandle(&'static AtomicUsize);

impl Drop for CountedHandle {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct HandleSlot {
    file: Arc<std::fs::File>,
    path: PathBuf,
    writable: bool,
    used: u64,
    _counted: Arc<CountedHandle>,
}

type HandleCache = Mutex<Vec<Option<HandleSlot>>>;

fn next_tick() -> u64 {
    HANDLE_TICK.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

static FAILED_FLUSH_PATHS: std::sync::LazyLock<Mutex<HashSet<PathBuf>>> =
    std::sync::LazyLock::new(|| Mutex::new(HashSet::new()));

#[cfg(test)]
pub(crate) static FAIL_SYNC_UNDER: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

pub const PARTS_DIR: &str = ".risuko-parts";

pub fn parts_dir_for(root: &Path, info_hash_hex: &str) -> PathBuf {
    root.join(PARTS_DIR).join(info_hash_hex)
}

pub struct FilesystemStorage {
    layout: FileSet,
    piece_length: u64,
    handles: Arc<HandleCache>,
    parts_dir: Option<PathBuf>,
    shadowed: Mutex<Vec<bool>>,
    shadow_handles: Arc<HandleCache>,
    unsynced: Arc<UnsyncedPaths>,
    quota: HandleQuota,
    io_gate: tokio::sync::RwLock<()>,
    dirty: std::sync::atomic::AtomicBool,
    flush_failed: std::sync::atomic::AtomicBool,
    read_cache: super::read_cache::ReadCache,
    #[cfg(test)]
    write_hold: Mutex<Option<WriteHold>>,
}

#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct WriteHold {
    pub(crate) reached: Arc<tokio::sync::Notify>,
    pub(crate) release: Arc<tokio::sync::Notify>,
}

impl FilesystemStorage {
    pub fn new(info: &ValidatedTorrentMetaV1Info, root: &Path) -> Self {
        Self::with_quota(info, root, HandleQuota::process())
    }

    fn with_quota(info: &ValidatedTorrentMetaV1Info, root: &Path, quota: HandleQuota) -> Self {
        let layout = FileSet::from_meta(info, root);
        let files = layout.files().len();
        let handles = Arc::new(Mutex::new(vec![None; files]));
        let shadow_handles = Arc::new(Mutex::new(vec![None; files]));
        let unsynced = Arc::new(Mutex::new(HashSet::new()));
        quota.register([&handles, &shadow_handles], &unsynced);
        Self {
            layout,
            piece_length: info.piece_length as u64,
            handles,
            parts_dir: None,
            shadowed: Mutex::new(vec![false; files]),
            shadow_handles,
            unsynced,
            quota,
            io_gate: tokio::sync::RwLock::new(()),
            dirty: std::sync::atomic::AtomicBool::new(false),
            flush_failed: std::sync::atomic::AtomicBool::new(false),
            read_cache: super::read_cache::ReadCache::default(),
            #[cfg(test)]
            write_hold: Mutex::new(None),
        }
    }

    pub fn flush_has_failed(&self) -> bool {
        if self.flush_failed.load(Ordering::Relaxed) {
            return true;
        }
        let failed = FAILED_FLUSH_PATHS.lock();
        !failed.is_empty() && self.payload_paths().any(|path| failed.contains(&path))
    }

    fn remember_failed_flush(&self) {
        self.flush_failed.store(true, Ordering::Relaxed);
        FAILED_FLUSH_PATHS.lock().extend(self.payload_paths());
    }

    fn payload_paths(&self) -> impl Iterator<Item = PathBuf> + '_ {
        self.layout
            .files()
            .iter()
            .enumerate()
            .filter(|(_, file)| !file.padding)
            .flat_map(|(idx, file)| std::iter::once(file.path.clone()).chain(self.shadow_path(idx)))
    }

    #[cfg(test)]
    pub(crate) fn hold_next_write(&self) -> WriteHold {
        let hold = WriteHold::default();
        *self.write_hold.lock() = Some(hold.clone());
        hold
    }

    #[cfg(test)]
    async fn pause_at_write_hold(&self) {
        let hold = self.write_hold.lock().take();
        if let Some(hold) = hold {
            hold.reached.notify_one();
            hold.release.notified().await;
        }
    }

    pub fn with_parts_dir(mut self, dir: PathBuf) -> Self {
        self.parts_dir = Some(dir);
        self
    }

    fn shadow_path(&self, idx: usize) -> Option<PathBuf> {
        self.parts_dir.as_ref().map(|dir| dir.join(idx.to_string()))
    }

    fn is_shadowed(&self, idx: usize) -> bool {
        self.shadowed.lock().get(idx).copied().unwrap_or(false)
    }

    pub async fn set_selection(&self, selected: Option<&HashSet<usize>>) -> Vec<usize> {
        let Some(parts_dir) = self.parts_dir.clone() else {
            return Vec::new();
        };
        // Exclusive so no in-flight write lands in a real file being rerouted
        let _exclusive = self.io_gate.write().await;
        self.read_cache.invalidate_all();
        let candidates: Vec<(usize, PathBuf, bool)> = self
            .layout
            .files()
            .iter()
            .enumerate()
            .filter(|(idx, f)| !f.padding && f.length > 0 && !self.is_shadowed(*idx))
            .map(|(idx, f)| {
                let unselected = selected.is_some_and(|set| !set.contains(&idx));
                (idx, f.path.clone(), unselected)
            })
            .collect();
        let newly_shadowed = task::spawn_blocking(move || {
            candidates
                .into_iter()
                .filter(|(idx, path, unselected)| {
                    !path.exists() && (*unselected || parts_dir.join(idx.to_string()).exists())
                })
                .map(|(idx, _, _)| idx)
                .collect::<Vec<_>>()
        })
        .await
        .unwrap_or_default();
        let mut shadowed = self.shadowed.lock();
        for idx in newly_shadowed {
            shadowed[idx] = true;
        }
        shadowed
            .iter()
            .enumerate()
            .filter(|(idx, is_shadowed)| {
                **is_shadowed && selected.is_none_or(|set| set.contains(idx))
            })
            .map(|(idx, _)| idx)
            .collect()
    }

    pub async fn promote_file(&self, idx: usize) -> Result<(), StorageError> {
        let _exclusive = self.io_gate.write().await;
        self.read_cache.invalidate_all();
        if !self.is_shadowed(idx) {
            return Ok(());
        }
        let Some(shadow_path) = self.shadow_path(idx) else {
            return Ok(());
        };
        let file = &self.layout.files()[idx];
        let ranges = boundary_ranges(file.offset, file.length, self.piece_length);
        let copy_from = shadow_path.clone();
        let shadow = task::spawn_blocking(move || open_part_file(&copy_from))
            .await
            .map_err(|e| io::Error::other(e.to_string()))??;
        let real = self.handle(idx, true).await?;
        let target = [(idx, Arc::clone(&real))];
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        #[cfg(test)]
        self.pause_at_write_hold().await;
        let copied = task::spawn_blocking(move || -> io::Result<()> {
            let Some(shadow) = shadow else {
                return Ok(());
            };
            let shadow_len = shadow.metadata()?.len();
            for (start, len) in ranges {
                let end = (start + len).min(shadow_len);
                if end <= start {
                    continue;
                }
                let mut buf = vec![0u8; (end - start) as usize];
                pread_exact(&shadow, start, &mut buf)?;
                pwrite_all(&real, start, &buf)?;
            }
            Ok(())
        })
        .await;
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        copied.map_err(|e| io::Error::other(e.to_string()))??;
        self.shadowed.lock()[idx] = false;
        self.shadow_handles.lock()[idx] = None;
        self.queue_evicted_targets(&target);
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        let parts_dir = self.parts_dir.clone();
        let _ = task::spawn_blocking(move || {
            let _ = std::fs::remove_file(&shadow_path);
            if let Some(dir) = parts_dir {
                if std::fs::remove_dir(&dir).is_ok() {
                    if let Some(parent) = dir.parent() {
                        let _ = std::fs::remove_dir(parent);
                    }
                }
            }
        })
        .await;
        Ok(())
    }

    pub fn layout(&self) -> &FileSet {
        &self.layout
    }

    pub(crate) async fn files_completed(&self, files: &[usize]) {
        let paths: Vec<PathBuf> = files
            .iter()
            .filter_map(|&idx| {
                self.layout
                    .files()
                    .get(idx)
                    .filter(|f| f.executable && f.length > 0 && !self.is_shadowed(idx))
                    .map(|f| f.path.clone())
            })
            .collect();
        if paths.is_empty() {
            return;
        }
        #[cfg(unix)]
        let _ = task::spawn_blocking(move || {
            for path in paths {
                if let Err(e) = set_executable(&path) {
                    tracing::debug!("could not mark {} executable: {e}", path.display());
                }
            }
        })
        .await;
    }

    pub(crate) async fn settle_promoted_files(&self, promoted: &[usize], file_progress: &[u64]) {
        let complete: Vec<usize> = promoted
            .iter()
            .copied()
            .filter(|&idx| {
                self.layout.files().get(idx).is_some_and(|file| {
                    file_progress
                        .get(idx)
                        .is_some_and(|done| *done >= file.length)
                })
            })
            .collect();
        self.files_completed(&complete).await;
    }

    pub async fn has_existing_payload_files(&self) -> bool {
        let paths: Vec<_> = self
            .layout
            .files()
            .iter()
            .filter(|f| f.length > 0 && !f.padding)
            .map(|f| f.path.clone())
            .collect();
        let parts_dir = self.parts_dir.clone();
        task::spawn_blocking(move || {
            let shadows = parts_dir
                .and_then(|dir| std::fs::read_dir(dir).ok())
                .is_some_and(|mut entries| entries.next().is_some());
            shadows
                || (!paths.is_empty()
                    && paths
                        .iter()
                        .any(|p| std::fs::metadata(p).is_ok_and(|m| has_payload(&m))))
        })
        .await
        .unwrap_or(false)
    }

    pub async fn write_at_owned(&self, offset: u64, buf: bytes::Bytes) -> Result<(), StorageError> {
        let total = self.layout.total_length();
        let end = offset
            .checked_add(buf.len() as u64)
            .ok_or(StorageError::OutOfRange { offset, total })?;
        if end > total {
            return Err(StorageError::OutOfRange { offset, total });
        }
        let spans: Vec<_> = self.layout.spans_for(offset, buf.len() as u64).collect();
        if spans.is_empty() {
            return Ok(());
        }
        let _shared = self.io_gate.read().await;
        let mut jobs = Vec::with_capacity(spans.len());
        let mut targets = Vec::with_capacity(spans.len());
        let mut cursor = 0usize;
        for span in spans {
            let len = span.len as usize;
            if self.is_padding(span.file_index) {
                cursor += len;
                continue;
            }
            let handle = self.span_handle(span.file_index, true).await?;
            targets.push((span.file_index, Arc::clone(&handle)));
            jobs.push((handle, span.file_offset, cursor, len));
            cursor += len;
        }
        if jobs.is_empty() {
            return Ok(());
        }
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        #[cfg(test)]
        self.pause_at_write_hold().await;
        let written = buf.len() as u64;
        let res = task::spawn_blocking(move || -> io::Result<()> {
            for (handle, file_offset, cursor, len) in jobs {
                pwrite_all(&handle, file_offset, &buf[cursor..cursor + len])?;
            }
            Ok(())
        })
        .await;
        self.queue_evicted_targets(&targets);
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        self.read_cache.invalidate_range(offset, written);
        res.map_err(|e| io::Error::other(e.to_string()))??;
        Ok(())
    }

    fn is_padding(&self, idx: usize) -> bool {
        self.layout.files()[idx].padding
    }

    fn queue_evicted_targets(&self, targets: &[(usize, Arc<std::fs::File>)]) {
        for (idx, handle) in targets {
            let (cache, path) = match self.shadow_path(*idx).filter(|_| self.is_shadowed(*idx)) {
                Some(path) => (&self.shadow_handles, path),
                None => (&self.handles, self.layout.files()[*idx].path.clone()),
            };
            let cached = cache
                .lock()
                .get(*idx)
                .and_then(Option::as_ref)
                .is_some_and(|slot| Arc::ptr_eq(&slot.file, handle));
            if !cached {
                self.unsynced.lock().insert(path);
            }
        }
    }

    async fn span_handle(
        &self,
        idx: usize,
        write: bool,
    ) -> Result<Arc<std::fs::File>, StorageError> {
        if self.is_shadowed(idx) {
            if let Some(path) = self.shadow_path(idx) {
                return open_cached(
                    &self.shadow_handles,
                    &self.unsynced,
                    self.quota,
                    idx,
                    &path,
                    write,
                )
                .await;
            }
        }
        self.handle(idx, write).await
    }

    async fn handle(&self, idx: usize, write: bool) -> Result<Arc<std::fs::File>, StorageError> {
        open_cached(
            &self.handles,
            &self.unsynced,
            self.quota,
            idx,
            &self.layout.files()[idx].path,
            write,
        )
        .await
    }

    pub async fn close_handles(&self) -> Result<(), StorageError> {
        let snapshot: Vec<Arc<std::fs::File>> = [&self.handles, &self.shadow_handles]
            .into_iter()
            .flat_map(|cache| {
                let mut guard = cache.lock();
                writable_handles(guard.iter_mut().filter_map(Option::take))
            })
            .collect();
        #[cfg(test)]
        if self.sync_fails_in_test() {
            let evicted = self.take_unsynced();
            return self.sync_all(snapshot, evicted).await;
        }
        if snapshot.is_empty() && self.unsynced.lock().is_empty() {
            return Ok(());
        }
        if !self.dirty.swap(false, std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        let evicted = self.take_unsynced();
        self.sync_all(snapshot, evicted).await
    }

    #[cfg(test)]
    fn sync_fails_in_test(&self) -> bool {
        let Some(first) = self.layout.files().first() else {
            return false;
        };
        FAIL_SYNC_UNDER
            .lock()
            .iter()
            .any(|dir| first.path.starts_with(dir))
    }

    fn take_unsynced(&self) -> Vec<PathBuf> {
        std::mem::take(&mut *self.unsynced.lock())
            .into_iter()
            .collect()
    }

    async fn sync_all(
        &self,
        handles: Vec<Arc<std::fs::File>>,
        evicted: Vec<PathBuf>,
    ) -> Result<(), StorageError> {
        #[cfg(test)]
        if self.sync_fails_in_test() {
            self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
            self.remember_failed_flush();
            return Err(StorageError::Io(io::Error::other("injected sync failure")));
        }
        let joins: Vec<_> = handles
            .into_iter()
            .map(|handle| task::spawn_blocking(move || handle.sync_data()))
            .chain(
                evicted
                    .into_iter()
                    .map(|path| task::spawn_blocking(move || sync_path(&path))),
            )
            .collect();
        let mut first_error: Option<io::Error> = None;
        for join in joins {
            let res = join.await.map_err(|e| io::Error::other(e.to_string()));
            if let Some(e) = res.and_then(|r| r).err() {
                first_error.get_or_insert(e);
            }
        }
        match first_error {
            Some(e) => {
                self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
                self.remember_failed_flush();
                Err(StorageError::Io(e))
            }
            None => Ok(()),
        }
    }

    #[cfg(test)]
    pub async fn preallocate(&self) -> Result<(), StorageError> {
        self.preallocate_selected(None).await
    }

    pub async fn preallocate_selected(
        &self,
        selected: Option<&std::collections::HashSet<usize>>,
    ) -> Result<(), StorageError> {
        for (_, f) in self
            .layout
            .files()
            .iter()
            .enumerate()
            .filter(|(idx, f)| !f.padding && selected.is_none_or(|set| set.contains(idx)))
        {
            let path = f.path.clone();
            let target_len = f.length;
            #[cfg(unix)]
            let executable = f.executable;
            task::spawn_blocking(move || -> io::Result<()> {
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                let file = std::fs::OpenOptions::new()
                    .create(true)
                    .read(true)
                    .write(true)
                    .truncate(false)
                    .open(&path)?;
                mark_sparse(&file);
                let current = file.metadata()?.len();
                if current != target_len {
                    file.set_len(target_len)?;
                }
                #[cfg(unix)]
                if executable && target_len == 0 {
                    if let Err(e) = set_executable(&path) {
                        tracing::debug!("could not mark {} executable: {e}", path.display());
                    }
                }
                Ok(())
            })
            .await
            .map_err(|e| io::Error::other(e.to_string()))??;
        }
        Ok(())
    }

    #[cfg(test)]
    pub async fn write_at(&self, offset: u64, buf: &[u8]) -> Result<(), StorageError> {
        self.write_at_owned(offset, bytes::Bytes::copy_from_slice(buf))
            .await
    }

    pub async fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), StorageError> {
        let data = self.read_owned(offset, buf.len()).await?;
        buf.copy_from_slice(&data);
        Ok(())
    }

    pub async fn read_owned(&self, offset: u64, len: usize) -> Result<Vec<u8>, StorageError> {
        let total = self.layout.total_length();
        let end = offset
            .checked_add(len as u64)
            .ok_or(StorageError::OutOfRange { offset, total })?;
        if end > total {
            return Err(StorageError::OutOfRange { offset, total });
        }
        let spans: Vec<_> = self.layout.spans_for(offset, len as u64).collect();
        let _shared = self.io_gate.read().await;
        let mut jobs = Vec::with_capacity(spans.len());
        let mut cursor = 0usize;
        for span in spans {
            let n = span.len as usize;
            if !self.is_padding(span.file_index) {
                let handle = self.span_handle(span.file_index, false).await?;
                jobs.push((handle, span.file_offset, cursor, n));
            }
            cursor += n;
        }
        let out = task::spawn_blocking(move || -> io::Result<Vec<u8>> {
            let mut out = vec![0u8; len];
            for (handle, file_offset, cursor, n) in jobs {
                pread_exact(&handle, file_offset, &mut out[cursor..cursor + n])?;
            }
            Ok(out)
        })
        .await
        .map_err(|e| io::Error::other(e.to_string()))??;
        Ok(out)
    }

    pub async fn read_block_cached(
        &self,
        ext_start: u64,
        ext_len: usize,
        offset: u64,
        len: usize,
    ) -> Result<bytes::Bytes, StorageError> {
        if !self.read_cache.admits(ext_start, ext_len) {
            return self.read_owned(offset, len).await.map(bytes::Bytes::from);
        }
        self.read_cache
            .get(ext_start, ext_len, offset, len, || {
                self.read_owned(ext_start, ext_len)
            })
            .await
    }

    pub fn trim_read_cache(&self) {
        self.read_cache.trim_idle();
    }

    pub fn invalidate_read_cache(&self) {
        self.read_cache.invalidate_all();
    }

    pub fn read_cache_loads(&self) -> u64 {
        self.read_cache.loads()
    }

    pub async fn sync_written(&self) -> Result<(), StorageError> {
        if !self.dirty.swap(false, std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        let snapshot: Vec<_> = [&self.handles, &self.shadow_handles]
            .into_iter()
            .flat_map(|cache| writable_handles(cache.lock().iter().flatten().cloned()))
            .collect();
        let evicted = self.take_unsynced();
        self.sync_all(snapshot, evicted).await
    }
}

impl Drop for FilesystemStorage {
    fn drop(&mut self) {
        self.quota.deregister(&self.unsynced);
    }
}

async fn open_cached(
    cache: &HandleCache,
    unsynced: &Mutex<HashSet<PathBuf>>,
    quota: HandleQuota,
    idx: usize,
    path: &Path,
    write: bool,
) -> Result<Arc<std::fs::File>, StorageError> {
    if let Some(Some(slot)) = cache.lock().get_mut(idx) {
        if slot.writable || !write {
            slot.used = next_tick();
            return Ok(slot.file.clone());
        }
    }
    let path = path.to_path_buf();
    let slot_path = path.clone();
    let file = task::spawn_blocking(move || -> io::Result<std::fs::File> {
        let mut opts = std::fs::OpenOptions::new();
        opts.read(true);
        if !write {
            return opts.open(&path);
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = opts.create(true).write(true).truncate(false).open(&path)?;
        if file.metadata().is_ok_and(|m| m.len() == 0) {
            mark_sparse(&file);
        }
        Ok(file)
    })
    .await
    .map_err(|e| io::Error::other(e.to_string()))??;
    let arc = Arc::new(file);
    {
        let mut guard = cache.lock();
        if let Some(Some(existing)) = guard.get_mut(idx) {
            if existing.writable || !write {
                existing.used = next_tick();
                return Ok(existing.file.clone());
            }
        }
        if let Some(slot) = guard.get_mut(idx) {
            *slot = Some(HandleSlot {
                file: arc.clone(),
                path: slot_path,
                writable: write,
                used: next_tick(),
                _counted: quota.count(),
            });
        }
        unsynced
            .lock()
            .extend(evict_lru(&mut guard, idx, MAX_OPEN_HANDLES));
    }
    quota.evict_oldest(cache, idx);
    Ok(arc)
}

fn writable_handles(slots: impl IntoIterator<Item = HandleSlot>) -> Vec<Arc<std::fs::File>> {
    slots
        .into_iter()
        .filter(|slot| slot.writable)
        .map(|slot| slot.file)
        .collect()
}

fn sync_path(path: &Path) -> io::Result<()> {
    match std::fs::OpenOptions::new().write(true).open(path) {
        Ok(file) => file.sync_data(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn evict_lru(slots: &mut [Option<HandleSlot>], keep: usize, cap: usize) -> Vec<PathBuf> {
    if slots.len() <= cap {
        return Vec::new();
    }
    let open = slots.iter().flatten().count();
    if open <= cap {
        return Vec::new();
    }
    let mut by_age: Vec<(u64, usize)> = slots
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != keep)
        .filter_map(|(i, s)| s.as_ref().map(|s| (s.used, i)))
        .collect();
    by_age.sort_unstable();
    by_age
        .into_iter()
        .take(open - cap)
        .filter_map(|(_, i)| slots[i].take())
        .filter(|slot| slot.writable)
        .map(|slot| slot.path)
        .collect()
}

#[cfg(windows)]
fn mark_sparse(file: &std::fs::File) {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::System::{Ioctl::FSCTL_SET_SPARSE, IO::DeviceIoControl};
    let mut returned = 0u32;
    // SAFETY: the handle is valid for the call and the null buffers match the zero sizes passed
    unsafe {
        DeviceIoControl(
            file.as_raw_handle() as _,
            FSCTL_SET_SPARSE,
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
            0,
            &mut returned,
            std::ptr::null_mut(),
        );
    }
}

#[cfg(not(windows))]
fn mark_sparse(_file: &std::fs::File) {}

fn has_payload(meta: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        std::os::unix::fs::MetadataExt::blocks(meta) > 0
    }
    #[cfg(not(unix))]
    {
        meta.len() > 0
    }
}

fn boundary_ranges(offset: u64, length: u64, piece_length: u64) -> Vec<(u64, u64)> {
    if length == 0 || piece_length == 0 {
        return Vec::new();
    }
    let first_end = (piece_length - offset % piece_length).min(length);
    let last_start = ((offset + length - 1) / piece_length * piece_length).saturating_sub(offset);
    if last_start <= first_end {
        vec![(0, length)]
    } else {
        vec![(0, first_end), (last_start, length - last_start)]
    }
}

fn pwrite_all(file: &std::fs::File, mut offset: u64, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        #[cfg(unix)]
        let r = std::os::unix::fs::FileExt::write_at(file, buf, offset);
        #[cfg(windows)]
        let r = std::os::windows::fs::FileExt::seek_write(file, buf, offset);
        let n = match r {
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "pwrite returned 0",
            ));
        }
        buf = &buf[n..];
        offset += n as u64;
    }
    Ok(())
}

fn pread_exact(file: &std::fs::File, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
    while !buf.is_empty() {
        #[cfg(unix)]
        let r = std::os::unix::fs::FileExt::read_at(file, buf, offset);
        #[cfg(windows)]
        let r = std::os::windows::fs::FileExt::seek_read(file, buf, offset);
        let n = match r {
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "pread at EOF"));
        }
        buf = &mut buf[n..];
        offset += n as u64;
    }
    Ok(())
}

fn open_part_file(path: &Path) -> io::Result<Option<std::fs::File>> {
    match std::fs::File::open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bencode::{encode_to_vec, Value};
    use crate::core::metainfo::parse_torrent;

    fn build_multi_file_torrent() -> Vec<u8> {
        let info = Value::Dict(vec![
            (
                b"files".to_vec(),
                Value::List(vec![
                    Value::Dict(vec![
                        (b"length".to_vec(), Value::Int(10)),
                        (
                            b"path".to_vec(),
                            Value::List(vec![Value::Bytes(b"a.txt".to_vec())]),
                        ),
                    ]),
                    Value::Dict(vec![
                        (b"length".to_vec(), Value::Int(20)),
                        (
                            b"path".to_vec(),
                            Value::List(vec![
                                Value::Bytes(b"sub".to_vec()),
                                Value::Bytes(b"b.bin".to_vec()),
                            ]),
                        ),
                    ]),
                ]),
            ),
            (b"name".to_vec(), Value::Bytes(b"root".to_vec())),
            (b"piece length".to_vec(), Value::Int(16 * 1024)),
            (b"pieces".to_vec(), Value::Bytes(vec![0; 20])),
        ]);
        let top = Value::Dict(vec![(b"info".to_vec(), info)]);
        encode_to_vec(&top)
    }

    #[test]
    fn boundary_ranges_cover_first_and_last_piece() {
        assert_eq!(boundary_ranges(10, 50, 16), vec![(0, 6), (38, 12)]);
        assert_eq!(boundary_ranges(3, 5, 16), vec![(0, 5)]);
        assert_eq!(boundary_ranges(10, 20, 16), vec![(0, 20)]);
        assert!(boundary_ranges(0, 0, 16).is_empty());
    }

    #[tokio::test]
    async fn unselected_file_bytes_wait_in_a_part_file_until_selected() {
        let file = |len: i64, name: &[u8]| {
            Value::Dict(vec![
                (b"length".to_vec(), Value::Int(len)),
                (
                    b"path".to_vec(),
                    Value::List(vec![Value::Bytes(name.to_vec())]),
                ),
            ])
        };
        let info = Value::Dict(vec![
            (
                b"files".to_vec(),
                Value::List(vec![file(10, b"a.bin"), file(22, b"b.bin")]),
            ),
            (b"name".to_vec(), Value::Bytes(b"root".to_vec())),
            (b"piece length".to_vec(), Value::Int(16)),
            (b"pieces".to_vec(), Value::Bytes(vec![0; 40])),
        ]);
        let meta =
            parse_torrent(&encode_to_vec(&Value::Dict(vec![(b"info".to_vec(), info)]))).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let parts = parts_dir_for(&root, "abcd");
        let storage = FilesystemStorage::new(&meta.info, &root).with_parts_dir(parts.clone());

        let only_b: HashSet<usize> = [1].into_iter().collect();
        {
            let _io = storage.io_gate.read().await;
            let rerouted = tokio::time::timeout(
                std::time::Duration::from_millis(50),
                storage.set_selection(Some(&only_b)),
            );
            assert!(rerouted.await.is_err());
        }
        assert!(storage.set_selection(Some(&only_b)).await.is_empty());
        let piece0: Vec<u8> = (1u8..=16).collect();
        storage.write_at(0, &piece0).await.unwrap();
        assert!(
            !root.join("a.bin").exists(),
            "unselected file must not appear"
        );
        assert!(parts.join("0").exists());
        let mut back = vec![0u8; 16];
        storage.read_at(0, &mut back).await.unwrap();
        assert_eq!(
            back, piece0,
            "boundary piece reads back through the part file"
        );
        storage.sync_written().await.unwrap();
        storage.close_handles().await.unwrap();
        assert!(
            storage.shadow_handles.lock().iter().all(Option::is_none),
            "close_handles must release part-file descriptors too"
        );

        assert_eq!(storage.set_selection(None).await, vec![0]);
        assert!(!storage.dirty.load(std::sync::atomic::Ordering::Relaxed));
        storage.promote_file(0).await.unwrap();
        assert!(storage.dirty.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(
            tokio::fs::read(root.join("a.bin")).await.unwrap(),
            piece0[..10]
        );
        assert!(!parts.exists() && !root.join(PARTS_DIR).exists());
        storage.read_at(0, &mut back).await.unwrap();
        assert_eq!(back, piece0);
    }

    #[tokio::test]
    async fn bep47_padding_files_stay_off_disk_and_read_as_zeros() {
        let file = |len: i64, path: &[u8], attr: Option<&[u8]>| {
            let mut entry = Vec::new();
            if let Some(attr) = attr {
                entry.push((b"attr".to_vec(), Value::Bytes(attr.to_vec())));
            }
            entry.push((b"length".to_vec(), Value::Int(len)));
            entry.push((
                b"path".to_vec(),
                Value::List(
                    path.split(|b| *b == b'/')
                        .map(|c| Value::Bytes(c.to_vec()))
                        .collect(),
                ),
            ));
            Value::Dict(entry)
        };
        let info = Value::Dict(vec![
            (
                b"files".to_vec(),
                Value::List(vec![
                    file(10, b"a.txt", None),
                    file(6, b".pad/6", Some(b"p")),
                    file(4, b"b.txt", Some(b"x")),
                ]),
            ),
            (b"name".to_vec(), Value::Bytes(b"root".to_vec())),
            (b"piece length".to_vec(), Value::Int(16 * 1024)),
            (b"pieces".to_vec(), Value::Bytes(vec![0; 20])),
        ]);
        let meta =
            parse_torrent(&encode_to_vec(&Value::Dict(vec![(b"info".to_vec(), info)]))).unwrap();
        let padding: Vec<bool> = meta.info.files.iter().map(|f| f.padding).collect();
        assert_eq!(padding, [false, true, false]);

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let storage = FilesystemStorage::new(&meta.info, &root);
        storage.preallocate().await.unwrap();
        let payload: Vec<u8> = (1u8..=20).collect();
        storage.write_at(0, &payload).await.unwrap();
        storage.sync_written().await.unwrap();

        let mut out = vec![0xffu8; 20];
        storage.read_at(0, &mut out).await.unwrap();
        assert_eq!(out[..10], payload[..10]);
        assert_eq!(out[10..16], [0u8; 6]);
        assert_eq!(out[16..], payload[16..]);
        assert!(!root.join(".pad").exists());
        assert_eq!(
            tokio::fs::read(root.join("b.txt")).await.unwrap(),
            payload[16..]
        );
    }

    #[tokio::test]
    async fn a_file_without_a_path_never_touches_the_torrent_root() {
        use crate::core::metainfo::{TorrentMetaInfo, ValidatedTorrentMetaV1Info};
        let info = ValidatedTorrentMetaV1Info {
            name: "root".into(),
            piece_length: 16 * 1024,
            pieces: vec![0; 20],
            private: false,
            files: vec![
                TorrentMetaInfo {
                    path: Vec::new(),
                    length: 0,
                    padding: false,
                    executable: false,
                },
                TorrentMetaInfo {
                    path: vec!["x".into()],
                    length: 1,
                    padding: false,
                    executable: false,
                },
            ],
            single_file_mode: false,
        };
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        tokio::fs::write(&root, vec![7u8; 100]).await.unwrap();
        let storage = FilesystemStorage::new(&info, &root);
        let _ = storage.preallocate().await;
        assert_eq!(tokio::fs::metadata(&root).await.unwrap().len(), 100);
        assert!(storage.layout().files()[0].padding);
    }

    #[tokio::test]
    async fn round_trip_spanning_files() {
        let bytes = build_multi_file_torrent();
        let meta = parse_torrent(&bytes).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");

        let storage = FilesystemStorage::new(&meta.info, &root);
        storage.preallocate().await.unwrap();

        let payload: Vec<u8> = (0u8..30).collect();
        storage.write_at(0, &payload).await.unwrap();
        storage.sync_written().await.unwrap();

        let mut out = vec![0u8; 30];
        storage.read_at(0, &mut out).await.unwrap();
        assert_eq!(out, payload);

        let a = tokio::fs::read(root.join("a.txt")).await.unwrap();
        assert_eq!(a, payload[..10]);
        let b = tokio::fs::read(root.join("sub").join("b.bin"))
            .await
            .unwrap();
        assert_eq!(b, payload[10..]);
    }

    #[tokio::test]
    async fn read_of_missing_file_does_not_create_it() {
        let bytes = build_multi_file_torrent();
        let meta = parse_torrent(&bytes).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let storage = FilesystemStorage::new(&meta.info, &root);
        let mut buf = [0u8; 4];
        assert!(storage.read_at(0, &mut buf).await.is_err());
        assert!(!root.exists());
        storage.write_at(0, &[1, 2, 3, 4]).await.unwrap();
        storage.read_at(0, &mut buf).await.unwrap();
        assert_eq!(buf, [1, 2, 3, 4]);
    }

    #[test]
    fn handle_cache_evicts_least_recently_used() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("f");
        let file = Arc::new(std::fs::File::create(&path).unwrap());
        let quota = private_quota(usize::MAX);
        let mut slots: Vec<Option<HandleSlot>> = (0..MAX_OPEN_HANDLES + 5)
            .map(|i| {
                Some(HandleSlot {
                    file: file.clone(),
                    path: PathBuf::from(i.to_string()),
                    writable: i % 2 == 0,
                    used: i as u64,
                    _counted: quota.count(),
                })
            })
            .collect();
        let mut evicted = evict_lru(&mut slots, 0, MAX_OPEN_HANDLES);
        evicted.sort();
        assert_eq!(slots.iter().flatten().count(), MAX_OPEN_HANDLES);
        assert!(slots[0].is_some(), "kept index survives");
        assert!(slots[1].is_none() && slots[5].is_none());
        assert!(slots[6].is_some());
        assert_eq!(evicted, [PathBuf::from("2"), PathBuf::from("4")]);
    }

    #[tokio::test]
    async fn evicted_writable_handles_are_still_synced_on_close() {
        let files = MAX_OPEN_HANDLES + 6;
        let entries: Vec<_> = (0..files)
            .map(|i| {
                Value::Dict(vec![
                    (b"length".to_vec(), Value::Int(1)),
                    (
                        b"path".to_vec(),
                        Value::List(vec![Value::Bytes(format!("f{i}").into_bytes())]),
                    ),
                ])
            })
            .collect();
        let info = Value::Dict(vec![
            (b"files".to_vec(), Value::List(entries)),
            (b"name".to_vec(), Value::Bytes(b"root".to_vec())),
            (b"piece length".to_vec(), Value::Int(16 * 1024)),
            (b"pieces".to_vec(), Value::Bytes(vec![0; 20])),
        ]);
        let meta =
            parse_torrent(&encode_to_vec(&Value::Dict(vec![(b"info".to_vec(), info)]))).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let storage = FilesystemStorage::new(&meta.info, &tmp.path().join("root"));
        for i in 0..files {
            storage.write_at(i as u64, &[7]).await.unwrap();
        }
        assert_eq!(
            storage.handles.lock().iter().flatten().count(),
            MAX_OPEN_HANDLES
        );
        assert_eq!(storage.unsynced.lock().len(), files - MAX_OPEN_HANDLES);
        storage.close_handles().await.unwrap();
        assert!(storage.unsynced.lock().is_empty());
        assert!(!storage.dirty.load(std::sync::atomic::Ordering::Relaxed));
    }

    fn private_quota(limit: usize) -> HandleQuota {
        let quota = HandleQuota {
            cached: Box::leak(Box::new(AtomicUsize::new(0))),
            limit: Box::leak(Box::new(AtomicUsize::new(0))),
            registry: Box::leak(Box::new(Mutex::new(Vec::new()))),
        };
        quota.set_limit(limit);
        quota
    }

    fn one_byte_files(root: &Path, files: usize, quota: HandleQuota) -> FilesystemStorage {
        let entries: Vec<_> = (0..files)
            .map(|i| {
                Value::Dict(vec![
                    (b"length".to_vec(), Value::Int(1)),
                    (
                        b"path".to_vec(),
                        Value::List(vec![Value::Bytes(format!("f{i}").into_bytes())]),
                    ),
                ])
            })
            .collect();
        let info = Value::Dict(vec![
            (b"files".to_vec(), Value::List(entries)),
            (b"name".to_vec(), Value::Bytes(b"root".to_vec())),
            (b"piece length".to_vec(), Value::Int(16 * 1024)),
            (b"pieces".to_vec(), Value::Bytes(vec![0; 20])),
        ]);
        let meta =
            parse_torrent(&encode_to_vec(&Value::Dict(vec![(b"info".to_vec(), info)]))).unwrap();
        FilesystemStorage::with_quota(&meta.info, root, quota)
    }

    fn cached_in(storage: &FilesystemStorage) -> usize {
        storage.handles.lock().iter().flatten().count()
    }

    #[test]
    fn the_handle_budget_is_a_quarter_of_the_soft_limit_within_bounds() {
        assert_eq!(handle_budget_for(Some(100)), 64);
        assert_eq!(handle_budget_for(Some(256)), 64);
        assert_eq!(handle_budget_for(Some(1_024)), 256);
        assert_eq!(handle_budget_for(Some(8_192)), 2_048);
        assert_eq!(handle_budget_for(Some(1_048_576)), 4_096);
        assert_eq!(handle_budget_for(None), 8_192);
    }

    #[tokio::test]
    async fn new_storages_charge_the_process_wide_count() {
        let meta = parse_torrent(&build_multi_file_torrent()).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let storage = FilesystemStorage::new(&meta.info, &tmp.path().join("root"));
        assert!(std::ptr::eq(storage.quota.cached, &CACHED_HANDLES));
        assert!(std::ptr::eq(storage.quota.limit, &HANDLE_BUDGET));
        storage.write_at(0, &[1u8; 30]).await.unwrap();
        assert_eq!(cached_in(&storage), 2);
        assert!(cached_handle_count() >= 2);
    }

    #[tokio::test]
    async fn cached_handles_are_counted_once_per_slot() {
        let quota = private_quota(usize::MAX);
        let tmp = tempfile::tempdir().unwrap();
        let storage = one_byte_files(&tmp.path().join("root"), 5, quota);
        for i in 0..5u64 {
            storage.write_at(i, &[7]).await.unwrap();
        }
        assert_eq!(quota.cached(), 5);

        let clones: Vec<HandleSlot> = storage.handles.lock().iter().flatten().cloned().collect();
        assert_eq!(quota.cached(), 5);
        storage.sync_written().await.unwrap();
        drop(clones);
        assert_eq!(quota.cached(), 5);

        storage.close_handles().await.unwrap();
        assert_eq!(quota.cached(), 0);

        let mut byte = [0u8; 1];
        storage.read_at(0, &mut byte).await.unwrap();
        assert_eq!(quota.cached(), 1);
        storage.write_at(0, &[8]).await.unwrap();
        assert!(storage.handles.lock()[0].as_ref().unwrap().writable);
        assert_eq!(quota.cached(), 1);
        drop(storage);
        assert_eq!(quota.cached(), 0);
    }

    fn held_in(storage: &FilesystemStorage) -> Vec<usize> {
        storage
            .handles
            .lock()
            .iter()
            .enumerate()
            .filter(|(_, slot)| slot.is_some())
            .map(|(idx, _)| idx)
            .collect()
    }

    #[tokio::test]
    async fn an_idle_torrent_gives_handles_back() {
        let quota = private_quota(8);
        let tmp = tempfile::tempdir().unwrap();
        let idle = one_byte_files(&tmp.path().join("idle"), 6, quota);
        for i in 0..6u8 {
            idle.write_at(u64::from(i), &[0xa0 + i]).await.unwrap();
        }
        let busy = one_byte_files(&tmp.path().join("busy"), 10, quota);
        for i in 0..10u8 {
            busy.write_at(u64::from(i), &[i]).await.unwrap();
            assert!(
                quota.cached() <= 8,
                "{} cached after file {i}",
                quota.cached()
            );
        }
        let held = held_in(&busy);
        assert!(!held.is_empty());
        assert_eq!(held, (10 - held.len()..10).collect::<Vec<_>>());
        assert!(cached_in(&idle) < 6);

        let mut byte = [0u8; 1];
        idle.read_at(0, &mut byte).await.unwrap();
        assert_eq!(byte, [0xa0]);
        assert!(idle.handles.lock()[0].is_some());
        assert!(quota.cached() <= 8);
    }

    #[tokio::test]
    async fn the_globally_oldest_handle_goes_first() {
        let quota = private_quota(8);
        let budget = quota.limit.load(Ordering::Relaxed);
        let tmp = tempfile::tempdir().unwrap();
        let storages: Vec<_> = (0..3)
            .map(|s| one_byte_files(&tmp.path().join(format!("t{s}")), 3, quota))
            .collect();
        let mut order = vec![
            (0, 0),
            (1, 0),
            (2, 0),
            (0, 1),
            (1, 1),
            (2, 1),
            (0, 2),
            (1, 2),
        ];
        assert_eq!(order.len(), budget);
        for &(s, f) in &order {
            storages[s].write_at(f as u64, &[1]).await.unwrap();
        }
        assert_eq!(quota.cached(), budget);

        let mut byte = [0u8; 1];
        storages[0].read_at(0, &mut byte).await.unwrap();
        order.remove(0);
        order.push((0, 0));
        storages[2].write_at(2, &[1]).await.unwrap();
        order.push((2, 2));

        let present: Vec<bool> = order
            .iter()
            .map(|&(s, f)| storages[s].handles.lock()[f].is_some())
            .collect();
        let gone = present.iter().take_while(|p| !**p).count();
        assert!(gone >= 1, "nothing was evicted: {present:?}");
        assert!(
            present[gone..].iter().all(|p| *p),
            "evicted out of order: {present:?}"
        );
        assert!(gone < budget, "the recent handles went too: {present:?}");
        assert!(quota.cached() <= budget);
    }

    #[tokio::test]
    async fn a_writable_handle_evicted_from_another_torrent_is_still_flushed() {
        let quota = private_quota(8);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("a");
        let a = one_byte_files(&root, 1, quota);
        a.write_at(0, &[5]).await.unwrap();
        let b = one_byte_files(&tmp.path().join("b"), 10, quota);
        for i in 0..10u64 {
            b.write_at(i, &[6]).await.unwrap();
        }
        assert!(a.handles.lock()[0].is_none());
        assert!(a.unsynced.lock().contains(&root.join("f0")));
        assert!(a.dirty.load(Ordering::Relaxed));

        a.sync_written().await.unwrap();
        assert!(!a.dirty.load(Ordering::Relaxed));
        assert!(a.unsynced.lock().is_empty());
        let mut byte = [0u8; 1];
        a.read_at(0, &mut byte).await.unwrap();
        assert_eq!(byte, [5]);
    }

    #[tokio::test]
    async fn a_promotion_whose_handle_is_evicted_mid_copy_is_flushed_next_time() {
        let file = |len: i64, name: &[u8]| {
            Value::Dict(vec![
                (b"length".to_vec(), Value::Int(len)),
                (
                    b"path".to_vec(),
                    Value::List(vec![Value::Bytes(name.to_vec())]),
                ),
            ])
        };
        let info = Value::Dict(vec![
            (
                b"files".to_vec(),
                Value::List(vec![file(10, b"a.bin"), file(22, b"b.bin")]),
            ),
            (b"name".to_vec(), Value::Bytes(b"root".to_vec())),
            (b"piece length".to_vec(), Value::Int(16)),
            (b"pieces".to_vec(), Value::Bytes(vec![0; 40])),
        ]);
        let meta =
            parse_torrent(&encode_to_vec(&Value::Dict(vec![(b"info".to_vec(), info)]))).unwrap();
        let quota = private_quota(8);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let storage = Arc::new(
            FilesystemStorage::with_quota(&meta.info, &root, quota)
                .with_parts_dir(parts_dir_for(&root, "abcd")),
        );
        let only_b: HashSet<usize> = [1].into_iter().collect();
        storage.set_selection(Some(&only_b)).await;
        let piece0: Vec<u8> = (1u8..=16).collect();
        storage.write_at(0, &piece0).await.unwrap();
        assert_eq!(storage.set_selection(None).await, vec![0]);
        storage.sync_written().await.unwrap();

        let hold = storage.hold_next_write();
        let promotion = tokio::spawn({
            let storage = Arc::clone(&storage);
            async move { storage.promote_file(0).await }
        });
        hold.reached.notified().await;
        assert!(storage.handles.lock()[0].is_some());
        let other = one_byte_files(&tmp.path().join("other"), 10, quota);
        for i in 0..10u64 {
            other.write_at(i, &[1]).await.unwrap();
        }
        assert!(storage.handles.lock()[0].is_none());
        storage.sync_written().await.unwrap();
        assert!(storage.unsynced.lock().is_empty());
        assert!(!storage.dirty.load(Ordering::Relaxed));

        hold.release.notify_one();
        promotion.await.unwrap().unwrap();
        assert!(storage.dirty.load(Ordering::Relaxed));
        assert!(storage.unsynced.lock().contains(&root.join("a.bin")));
        assert_eq!(
            tokio::fs::read(root.join("a.bin")).await.unwrap(),
            piece0[..10]
        );
        storage.sync_written().await.unwrap();
        assert!(storage.unsynced.lock().is_empty());
        assert!(!storage.dirty.load(Ordering::Relaxed));
    }

    #[test]
    fn a_missing_part_file_is_nothing_to_copy() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("0");
        assert!(open_part_file(&path).unwrap().is_none());
        std::fs::write(&path, b"part").unwrap();
        assert!(open_part_file(&path).unwrap().is_some());
    }

    #[cfg(unix)]
    fn mode_bits_are_enforced(unreadable: &Path) -> bool {
        let enforced = std::fs::File::open(unreadable).is_err();
        if !enforced {
            eprintln!(
                "SKIPPED: this process opens files whatever their mode (running as root?), so nothing was checked"
            );
        }
        enforced
    }

    #[cfg(unix)]
    #[test]
    fn an_unreadable_part_file_is_an_error() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let path = file_with_mode(tmp.path(), "0", 0o000);
        let enforced = mode_bits_are_enforced(&path);
        let result = open_part_file(&path);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        if !enforced {
            return;
        }
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_promotion_that_cannot_open_its_part_file_fails_and_keeps_it() {
        use std::os::unix::fs::PermissionsExt;
        let file = |len: i64, name: &[u8]| {
            Value::Dict(vec![
                (b"length".to_vec(), Value::Int(len)),
                (
                    b"path".to_vec(),
                    Value::List(vec![Value::Bytes(name.to_vec())]),
                ),
            ])
        };
        let info = Value::Dict(vec![
            (
                b"files".to_vec(),
                Value::List(vec![file(10, b"a.bin"), file(22, b"b.bin")]),
            ),
            (b"name".to_vec(), Value::Bytes(b"root".to_vec())),
            (b"piece length".to_vec(), Value::Int(16)),
            (b"pieces".to_vec(), Value::Bytes(vec![0; 40])),
        ]);
        let meta =
            parse_torrent(&encode_to_vec(&Value::Dict(vec![(b"info".to_vec(), info)]))).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let parts = parts_dir_for(&root, "abcd");
        let storage = FilesystemStorage::new(&meta.info, &root).with_parts_dir(parts.clone());
        let only_b: HashSet<usize> = [1].into_iter().collect();
        storage.set_selection(Some(&only_b)).await;
        let piece0: Vec<u8> = (1u8..=16).collect();
        storage.write_at(0, &piece0).await.unwrap();
        storage.sync_written().await.unwrap();
        storage.close_handles().await.unwrap();
        let part = parts.join("0");
        assert!(part.exists());

        std::fs::set_permissions(&part, std::fs::Permissions::from_mode(0o000)).unwrap();
        if !mode_bits_are_enforced(&part) {
            std::fs::set_permissions(&part, std::fs::Permissions::from_mode(0o600)).unwrap();
            return;
        }
        assert_eq!(storage.set_selection(None).await, vec![0]);
        let failed = storage.promote_file(0).await;
        let _ = std::fs::set_permissions(&part, std::fs::Permissions::from_mode(0o600));
        assert!(failed.is_err());
        assert!(storage.is_shadowed(0));
        assert!(part.exists());
        assert!(!root.join("a.bin").exists());

        storage.promote_file(0).await.unwrap();
        assert_eq!(
            tokio::fs::read(root.join("a.bin")).await.unwrap(),
            piece0[..10]
        );
        assert!(!part.exists());
    }

    #[tokio::test]
    async fn the_count_never_exceeds_the_budget_after_an_open() {
        let quota = private_quota(8);
        let budget = quota.limit.load(Ordering::Relaxed);
        let tmp = tempfile::tempdir().unwrap();
        let files = 5u64;
        let storages: Vec<_> = (0..3)
            .map(|s| one_byte_files(&tmp.path().join(format!("t{s}")), files as usize, quota))
            .collect();
        let value = |s: usize, f: u64| (s as u64 * files + f + 1) as u8;
        for round in 0..3u64 {
            for step in 0..files {
                for (s, storage) in storages.iter().enumerate() {
                    let f = (step + round + s as u64) % files;
                    if round == 0 {
                        storage.write_at(f, &[value(s, f)]).await.unwrap();
                    } else {
                        let mut byte = [0u8; 1];
                        storage.read_at(f, &mut byte).await.unwrap();
                        assert_eq!(byte, [value(s, f)]);
                    }
                    assert!(
                        quota.cached() <= budget,
                        "{} cached after round {round}, torrent {s}, file {f}",
                        quota.cached()
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn a_dropped_storage_leaves_the_registry() {
        let quota = private_quota(usize::MAX);
        let tmp = tempfile::tempdir().unwrap();
        let kept = one_byte_files(&tmp.path().join("kept"), 2, quota);
        let dropped = one_byte_files(&tmp.path().join("dropped"), 3, quota);
        for i in 0..2u64 {
            kept.write_at(i, &[1]).await.unwrap();
        }
        for i in 0..3u64 {
            dropped.write_at(i, &[2]).await.unwrap();
        }
        assert_eq!(quota.registry.lock().len(), 4);
        assert_eq!(quota.cached(), 5);

        let gone = Arc::as_ptr(&dropped.unsynced);
        drop(dropped);
        {
            let registry = quota.registry.lock();
            assert_eq!(registry.len(), 2);
            assert!(registry.iter().all(|entry| entry.slots.strong_count() > 0
                && !std::ptr::eq(entry.unsynced.as_ptr(), gone)));
        }
        assert_eq!(quota.cached(), 2);
        let mut byte = [0u8; 1];
        kept.read_at(1, &mut byte).await.unwrap();
        assert_eq!(byte, [1]);
    }

    #[tokio::test]
    async fn a_budget_smaller_than_the_active_torrents_still_works() {
        let quota = private_quota(0);
        let budget = quota.limit.load(Ordering::Relaxed);
        let tmp = tempfile::tempdir().unwrap();
        let storages: Vec<_> = (0..budget + 4)
            .map(|s| one_byte_files(&tmp.path().join(format!("t{s}")), 2, quota))
            .collect();
        let value = |round: usize, s: usize, f: usize| (round * 64 + s * 2 + f + 1) as u8;
        for round in 0..3 {
            for (s, storage) in storages.iter().enumerate() {
                for f in 0..2 {
                    storage
                        .write_at(f as u64, &[value(round, s, f)])
                        .await
                        .unwrap();
                }
            }
            storages[round].sync_written().await.unwrap();
            for (s, storage) in storages.iter().enumerate().rev() {
                for f in 0..2 {
                    let mut byte = [0u8; 1];
                    storage.read_at(f as u64, &mut byte).await.unwrap();
                    assert_eq!(byte, [value(round, s, f)], "round {round}, torrent {s}");
                }
            }
        }
        assert!(quota.cached() <= budget, "{} cached", quota.cached());
        for storage in &storages {
            storage.close_handles().await.unwrap();
            assert!(!storage.dirty.load(Ordering::Relaxed));
        }
        assert_eq!(quota.cached(), 0);
    }

    #[test]
    fn concurrent_io_across_torrents_under_a_tiny_budget_finishes() {
        const FILES: usize = 6;
        fn value(s: usize, f: usize) -> u8 {
            (s * 16 + f + 1) as u8
        }
        let quota = private_quota(0);
        let budget = quota.limit.load(Ordering::Relaxed);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .unwrap();
            let cached = rt.block_on(async move {
                let storages: Arc<Vec<FilesystemStorage>> = Arc::new(
                    (0..4)
                        .map(|s| one_byte_files(&root.join(format!("t{s}")), FILES, quota))
                        .collect(),
                );
                for (s, storage) in storages.iter().enumerate() {
                    for f in 0..FILES {
                        storage.write_at(f as u64, &[value(s, f)]).await.unwrap();
                    }
                }
                let workers: Vec<_> = (0..8usize)
                    .map(|task| {
                        let storages = Arc::clone(&storages);
                        tokio::spawn(async move {
                            for op in 0..50usize {
                                let s = (task + op) % storages.len();
                                let f = (task * 5 + op * 7) % FILES;
                                let storage = &storages[s];
                                match (task + op) % 5 {
                                    0 => storage.sync_written().await.unwrap(),
                                    1 | 2 => {
                                        storage.write_at(f as u64, &[value(s, f)]).await.unwrap()
                                    }
                                    _ => {
                                        let mut byte = [0u8; 1];
                                        storage.read_at(f as u64, &mut byte).await.unwrap();
                                        assert_eq!(byte, [value(s, f)], "torrent {s}, file {f}");
                                    }
                                }
                            }
                        })
                    })
                    .collect();
                for worker in workers {
                    worker.await.unwrap();
                }
                for (s, storage) in storages.iter().enumerate() {
                    for f in 0..FILES {
                        let mut byte = [0u8; 1];
                        storage.read_at(f as u64, &mut byte).await.unwrap();
                        assert_eq!(byte, [value(s, f)], "torrent {s}, file {f}");
                    }
                }
                quota.cached()
            });
            let _ = done.send(cached);
        });
        let cached = finished
            .recv_timeout(std::time::Duration::from_secs(60))
            .expect("the workload hung or panicked");
        assert!(cached <= budget, "{cached} cached");
    }

    #[tokio::test]
    async fn under_budget_the_cache_keeps_its_usual_size() {
        let quota = private_quota(10_000);
        let tmp = tempfile::tempdir().unwrap();
        let storage = one_byte_files(&tmp.path().join("root"), 70, quota);
        for i in 0..70u64 {
            storage.write_at(i, &[3]).await.unwrap();
        }
        assert_eq!(cached_in(&storage), MAX_OPEN_HANDLES);
        assert_eq!(quota.cached(), MAX_OPEN_HANDLES);
    }

    #[tokio::test]
    async fn evicted_writable_paths_are_still_queued_for_syncing() {
        let quota = private_quota(4);
        let budget = quota.limit.load(Ordering::Relaxed);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let files = 12;
        let storage = one_byte_files(&root, files, quota);
        for i in 0..files as u64 {
            storage.write_at(i, &[9]).await.unwrap();
        }
        let cached = cached_in(&storage);
        assert!(cached <= budget);
        let oldest: HashSet<PathBuf> = (0..files - cached)
            .map(|i| root.join(format!("f{i}")))
            .collect();
        assert_eq!(*storage.unsynced.lock(), oldest);
        assert!(oldest.contains(&root.join("f0")));

        storage.close_handles().await.unwrap();
        assert!(storage.unsynced.lock().is_empty());
        assert!(!storage.dirty.load(Ordering::Relaxed));
        assert_eq!(quota.cached(), 0);
    }

    #[tokio::test]
    async fn a_write_that_lands_after_a_flush_is_flushed_next_time() {
        let meta = parse_torrent(&build_multi_file_torrent()).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let storage = Arc::new(FilesystemStorage::new(&meta.info, &tmp.path().join("root")));
        let hold = storage.hold_next_write();
        let writer = tokio::spawn({
            let storage = Arc::clone(&storage);
            async move { storage.write_at(0, &[7u8; 10]).await }
        });
        hold.reached.notified().await;
        storage.sync_written().await.unwrap();
        hold.release.notify_one();
        writer.await.unwrap().unwrap();

        assert!(storage.dirty.load(Ordering::Relaxed));
        FAIL_SYNC_UNDER.lock().push(tmp.path().to_path_buf());
        let next = storage.sync_written().await;
        FAIL_SYNC_UNDER.lock().retain(|dir| dir != tmp.path());
        assert!(next.is_err(), "the next flush skipped the late write");
    }

    #[tokio::test]
    async fn a_write_whose_handle_was_evicted_during_a_flush_is_queued_again() {
        let quota = private_quota(usize::MAX);
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let storage = Arc::new(one_byte_files(&root, MAX_OPEN_HANDLES + 1, quota));
        let hold = storage.hold_next_write();
        let writer = tokio::spawn({
            let storage = Arc::clone(&storage);
            async move { storage.write_at(0, &[1]).await }
        });
        hold.reached.notified().await;
        for i in 1..=MAX_OPEN_HANDLES as u64 {
            storage.write_at(i, &[2]).await.unwrap();
        }
        assert!(storage.unsynced.lock().contains(&root.join("f0")));
        storage.sync_written().await.unwrap();
        assert!(storage.unsynced.lock().is_empty());

        hold.release.notify_one();
        writer.await.unwrap().unwrap();
        assert!(storage.unsynced.lock().contains(&root.join("f0")));
        assert!(storage.dirty.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn a_failed_flush_is_remembered_after_later_flushes_succeed() {
        let meta = parse_torrent(&build_multi_file_torrent()).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let storage = FilesystemStorage::new(&meta.info, &tmp.path().join("root"));
        storage.write_at(0, &[7u8; 16]).await.unwrap();
        storage.sync_written().await.unwrap();
        assert!(!storage.flush_has_failed());

        storage.write_at(0, &[8u8; 16]).await.unwrap();
        FAIL_SYNC_UNDER.lock().push(tmp.path().to_path_buf());
        let failed = storage.sync_written().await;
        FAIL_SYNC_UNDER.lock().retain(|dir| dir != tmp.path());
        assert!(failed.is_err());
        storage.sync_written().await.unwrap();
        storage.close_handles().await.unwrap();
        assert!(storage.flush_has_failed());
    }

    async fn fail_a_flush_under(root: &Path) {
        let meta = parse_torrent(&build_multi_file_torrent()).unwrap();
        let storage = FilesystemStorage::new(&meta.info, root);
        storage.write_at(0, &[7u8; 16]).await.unwrap();
        FAIL_SYNC_UNDER.lock().push(root.to_path_buf());
        let failed = storage.sync_written().await;
        FAIL_SYNC_UNDER.lock().retain(|dir| dir != root);
        assert!(failed.is_err());
    }

    #[tokio::test]
    async fn a_failed_flush_disqualifies_a_new_storage_for_the_same_files() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        fail_a_flush_under(&root).await;
        let meta = parse_torrent(&build_multi_file_torrent()).unwrap();
        let rebuilt = FilesystemStorage::new(&meta.info, &root);
        assert!(rebuilt.flush_has_failed());
    }

    #[tokio::test]
    async fn a_failed_flush_elsewhere_does_not_disqualify_a_storage() {
        let tmp = tempfile::tempdir().unwrap();
        fail_a_flush_under(&tmp.path().join("root")).await;
        let other = tempfile::tempdir().unwrap();
        let meta = parse_torrent(&build_multi_file_torrent()).unwrap();
        let elsewhere = FilesystemStorage::new(&meta.info, &other.path().join("root"));
        assert!(!elsewhere.flush_has_failed());
    }

    #[tokio::test]
    async fn close_handles_releases_cached_descriptors() {
        let bytes = build_multi_file_torrent();
        let meta = parse_torrent(&bytes).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");

        let storage = FilesystemStorage::new(&meta.info, &root);
        storage.preallocate().await.unwrap();

        let payload: Vec<u8> = (0u8..30).collect();
        storage.write_at(0, &payload).await.unwrap();
        assert!(
            storage.handles.lock().iter().any(|h| h.is_some()),
            "expected at least one cached handle after a write"
        );

        storage.close_handles().await.unwrap();
        assert!(
            storage.handles.lock().iter().all(|h| h.is_none()),
            "close_handles must clear all cached file handles"
        );

        let mut out = vec![0u8; 30];
        storage.read_at(0, &mut out).await.unwrap();
        assert_eq!(out, payload);
    }

    #[tokio::test]
    async fn close_handles_reports_a_failed_sync_once() {
        let bytes = build_multi_file_torrent();
        let meta = parse_torrent(&bytes).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let storage = FilesystemStorage::new(&meta.info, &tmp.path().join("root"));
        storage.write_at(0, &[7u8; 16]).await.unwrap();

        FAIL_SYNC_UNDER.lock().push(tmp.path().to_path_buf());
        let first = storage.close_handles().await;
        FAIL_SYNC_UNDER.lock().retain(|dir| dir != tmp.path());
        assert!(first.is_err());
        storage.close_handles().await.unwrap();
    }

    #[tokio::test]
    async fn close_handles_syncs_only_writable_handles() {
        let bytes = build_multi_file_torrent();
        let meta = parse_torrent(&bytes).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let storage = FilesystemStorage::new(&meta.info, &tmp.path().join("root"));
        storage.preallocate().await.unwrap();
        let mut head = [0u8; 10];
        storage.read_at(0, &mut head).await.unwrap();
        storage.write_at(10, &[7u8; 20]).await.unwrap();

        let slots: Vec<HandleSlot> = storage.handles.lock().iter().flatten().cloned().collect();
        assert_eq!(
            slots.iter().map(|slot| slot.writable).collect::<Vec<_>>(),
            [false, true]
        );
        let synced = writable_handles(slots.clone());
        assert_eq!(synced.len(), 1);
        assert!(Arc::ptr_eq(&synced[0], &slots[1].file));

        drop(slots);
        storage.close_handles().await.unwrap();
        assert!(storage.handles.lock().iter().all(Option::is_none));
        assert!(!storage.dirty.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[tokio::test]
    async fn rejects_out_of_range() {
        let bytes = build_multi_file_torrent();
        let meta = parse_torrent(&bytes).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let storage = FilesystemStorage::new(&meta.info, &tmp.path().join("root"));
        let err = storage.write_at(25, &[0u8; 10]).await.unwrap_err();
        assert!(matches!(err, StorageError::OutOfRange { .. }));
    }

    #[tokio::test]
    async fn existing_payload_detection_ignores_missing_and_empty_files() {
        let bytes = build_multi_file_torrent();
        let meta = parse_torrent(&bytes).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        let storage = FilesystemStorage::new(&meta.info, &root);

        assert!(!storage.has_existing_payload_files().await);

        tokio::fs::create_dir_all(root.join("sub")).await.unwrap();
        tokio::fs::File::create(root.join("a.txt")).await.unwrap();
        assert!(!storage.has_existing_payload_files().await);

        tokio::fs::write(root.join("sub").join("b.bin"), b"x")
            .await
            .unwrap();
        assert!(
            storage.has_existing_payload_files().await,
            "partial payload must still trigger a recovery scan"
        );

        tokio::fs::write(root.join("a.txt"), b"hello")
            .await
            .unwrap();
        assert!(storage.has_existing_payload_files().await);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn preallocated_holes_are_not_existing_payload() {
        let bytes = build_multi_file_torrent();
        let meta = parse_torrent(&bytes).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let storage = FilesystemStorage::new(&meta.info, &tmp.path().join("root"));
        storage.preallocate().await.unwrap();
        assert!(!storage.has_existing_payload_files().await);
        storage.write_at(0, b"x").await.unwrap();
        assert!(storage.has_existing_payload_files().await);
    }

    #[tokio::test]
    async fn cached_blocks_of_one_piece_cost_one_storage_read() {
        let bytes = build_multi_file_torrent();
        let meta = parse_torrent(&bytes).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let storage = FilesystemStorage::new(&meta.info, &tmp.path().join("root"));
        let payload: Vec<u8> = (0u8..30).collect();
        storage.write_at(0, &payload).await.unwrap();
        for i in 0..6u64 {
            let got = storage.read_block_cached(0, 30, i * 5, 5).await.unwrap();
            assert_eq!(&got[..], &payload[(i * 5) as usize..(i * 5 + 5) as usize]);
        }
        assert_eq!(storage.read_cache_loads(), 1);
        storage.write_at(3, &[99]).await.unwrap();
        let got = storage.read_block_cached(0, 30, 0, 5).await.unwrap();
        assert_eq!(got[3], 99);
        assert_eq!(storage.read_cache_loads(), 2);
        storage.invalidate_read_cache();
        storage.read_block_cached(0, 30, 0, 5).await.unwrap();
        assert_eq!(storage.read_cache_loads(), 3);
    }

    #[tokio::test]
    async fn read_owned_spans_files_in_one_buffer() {
        let bytes = build_multi_file_torrent();
        let meta = parse_torrent(&bytes).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let storage = FilesystemStorage::new(&meta.info, &tmp.path().join("root"));
        let payload: Vec<u8> = (0u8..30).collect();
        storage.write_at(0, &payload).await.unwrap();
        assert_eq!(storage.read_owned(5, 10).await.unwrap(), &payload[5..15]);
        assert!(storage.read_owned(25, 10).await.is_err());
    }

    #[cfg(unix)]
    fn mode_of(path: &Path) -> u32 {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path).unwrap().permissions().mode() & 0o7777
    }

    #[cfg(unix)]
    fn file_with_mode(dir: &Path, name: &str, mode: u32) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(&path, b"data").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
        path
    }

    #[cfg(unix)]
    #[test]
    fn executable_bits_follow_the_read_bits() {
        assert_eq!(executable_bits(0o644), 0o111);
        assert_eq!(executable_bits(0o600), 0o100);
        assert_eq!(executable_bits(0o640), 0o110);
        assert_eq!(executable_bits(0o604), 0o101);
        assert_eq!(executable_bits(0o000), 0o100);
    }

    #[cfg(unix)]
    #[test]
    fn set_executable_adds_x_wherever_read_is_granted() {
        let tmp = tempfile::tempdir().unwrap();
        for (before, after) in [
            (0o644, 0o755),
            (0o600, 0o700),
            (0o640, 0o750),
            (0o664, 0o775),
            (0o400, 0o500),
        ] {
            let path = file_with_mode(tmp.path(), &format!("m{before:o}"), before);
            set_executable(&path).unwrap();
            assert_eq!(mode_of(&path), after, "from {before:o}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn set_executable_leaves_a_file_that_already_has_exec_bits_alone() {
        let tmp = tempfile::tempdir().unwrap();
        let path = file_with_mode(tmp.path(), "tool", 0o754);
        set_executable(&path).unwrap();
        assert_eq!(mode_of(&path), 0o754);
        let owner_only = file_with_mode(tmp.path(), "mine", 0o710);
        set_executable(&owner_only).unwrap();
        assert_eq!(mode_of(&owner_only), 0o710);
    }

    #[cfg(unix)]
    #[test]
    fn set_executable_never_follows_a_symlink_or_touches_a_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let target = file_with_mode(tmp.path(), "target", 0o644);
        let link = tmp.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(set_executable(&link).is_err());
        assert_eq!(mode_of(&target), 0o644);

        let dir = tmp.path().join("dir");
        std::fs::create_dir(&dir).unwrap();
        let before = mode_of(&dir);
        set_executable(&dir).unwrap();
        assert_eq!(mode_of(&dir), before);
    }

    #[cfg(unix)]
    fn executable_fixture() -> (tempfile::TempDir, FilesystemStorage, PathBuf) {
        use crate::bencode::{encode_to_vec, Value};
        let file = |len: i64, name: &[u8], attr: Option<&[u8]>| {
            let mut entry = Vec::new();
            if let Some(attr) = attr {
                entry.push((b"attr".to_vec(), Value::Bytes(attr.to_vec())));
            }
            entry.push((b"length".to_vec(), Value::Int(len)));
            entry.push((
                b"path".to_vec(),
                Value::List(vec![Value::Bytes(name.to_vec())]),
            ));
            Value::Dict(entry)
        };
        let info = Value::Dict(vec![
            (
                b"files".to_vec(),
                Value::List(vec![
                    file(10, b"plain.txt", None),
                    file(4, b"run.sh", Some(b"x")),
                    file(0, b"empty.sh", Some(b"x")),
                ]),
            ),
            (b"name".to_vec(), Value::Bytes(b"root".to_vec())),
            (b"piece length".to_vec(), Value::Int(16 * 1024)),
            (b"pieces".to_vec(), Value::Bytes(vec![0; 20])),
        ]);
        let meta =
            parse_torrent(&encode_to_vec(&Value::Dict(vec![(b"info".to_vec(), info)]))).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");
        (tmp, FilesystemStorage::new(&meta.info, &root), root)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn completing_an_x_file_sets_its_exec_bits_and_leaves_siblings_alone() {
        let (_tmp, storage, root) = executable_fixture();
        storage.preallocate().await.unwrap();
        storage.write_at(0, &[7u8; 14]).await.unwrap();
        storage.sync_written().await.unwrap();

        storage.files_completed(&[0, 1]).await;
        assert_ne!(mode_of(&root.join("run.sh")) & 0o111, 0);
        assert_eq!(mode_of(&root.join("plain.txt")) & 0o111, 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn preallocation_marks_empty_x_files_executable_at_creation() {
        let (_tmp, storage, root) = executable_fixture();
        storage.preallocate().await.unwrap();
        assert_ne!(mode_of(&root.join("empty.sh")) & 0o111, 0);
        assert_eq!(mode_of(&root.join("run.sh")) & 0o111, 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn settling_only_touches_promoted_x_files_that_are_fully_downloaded() {
        let (_tmp, storage, root) = executable_fixture();
        storage.preallocate().await.unwrap();
        let run = root.join("run.sh");

        storage.settle_promoted_files(&[0, 1, 2], &[10, 3, 0]).await;
        assert_eq!(mode_of(&run) & 0o111, 0, "run.sh is not fully downloaded");

        storage.settle_promoted_files(&[0, 2], &[10, 4, 0]).await;
        assert_eq!(mode_of(&run) & 0o111, 0, "run.sh was not promoted");

        storage.settle_promoted_files(&[0, 1, 2], &[10, 4, 0]).await;
        assert_ne!(mode_of(&run) & 0o111, 0);
        assert_eq!(mode_of(&root.join("plain.txt")) & 0o111, 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn marking_a_missing_or_unknown_file_neither_panics_nor_creates_it() {
        let (_tmp, storage, root) = executable_fixture();
        storage.files_completed(&[1, 99]).await;
        storage.settle_promoted_files(&[1, 99], &[10, 4]).await;
        assert!(!root.join("run.sh").exists());
    }
}
