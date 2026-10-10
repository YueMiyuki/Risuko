use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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

const MAX_OPEN_HANDLES: usize = 64;

static HANDLE_TICK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[derive(Clone)]
struct HandleSlot {
    file: Arc<std::fs::File>,
    path: PathBuf,
    writable: bool,
    used: u64,
}

type HandleCache = Mutex<Vec<Option<HandleSlot>>>;

fn next_tick() -> u64 {
    HANDLE_TICK.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

pub const PARTS_DIR: &str = ".risuko-parts";

pub fn parts_dir_for(root: &Path, info_hash_hex: &str) -> PathBuf {
    root.join(PARTS_DIR).join(info_hash_hex)
}

pub struct FilesystemStorage {
    layout: FileSet,
    piece_length: u64,
    handles: HandleCache,
    parts_dir: Option<PathBuf>,
    shadowed: Mutex<Vec<bool>>,
    shadow_handles: HandleCache,
    unsynced: Mutex<HashSet<PathBuf>>,
    io_gate: tokio::sync::RwLock<()>,
    dirty: std::sync::atomic::AtomicBool,
    read_cache: super::read_cache::ReadCache,
}

impl FilesystemStorage {
    pub fn new(info: &ValidatedTorrentMetaV1Info, root: &Path) -> Self {
        let layout = FileSet::from_meta(info, root);
        let files = layout.files().len();
        Self {
            layout,
            piece_length: info.piece_length as u64,
            handles: Mutex::new(vec![None; files]),
            parts_dir: None,
            shadowed: Mutex::new(vec![false; files]),
            shadow_handles: Mutex::new(vec![None; files]),
            unsynced: Mutex::new(HashSet::new()),
            io_gate: tokio::sync::RwLock::new(()),
            dirty: std::sync::atomic::AtomicBool::new(false),
            read_cache: super::read_cache::ReadCache::default(),
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
        let real = self.handle(idx, true).await?;
        let copy_from = shadow_path.clone();
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        task::spawn_blocking(move || -> io::Result<()> {
            let Ok(shadow) = std::fs::File::open(&copy_from) else {
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
        .await
        .map_err(|e| io::Error::other(e.to_string()))??;
        self.shadowed.lock()[idx] = false;
        self.shadow_handles.lock()[idx] = None;
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
        let mut cursor = 0usize;
        for span in spans {
            let len = span.len as usize;
            if self.is_padding(span.file_index) {
                cursor += len;
                continue;
            }
            let handle = self.span_handle(span.file_index, true).await?;
            jobs.push((handle, span.file_offset, cursor, len));
            cursor += len;
        }
        if jobs.is_empty() {
            return Ok(());
        }
        self.dirty.store(true, std::sync::atomic::Ordering::Relaxed);
        let written = buf.len() as u64;
        let res = task::spawn_blocking(move || -> io::Result<()> {
            for (handle, file_offset, cursor, len) in jobs {
                pwrite_all(&handle, file_offset, &buf[cursor..cursor + len])?;
            }
            Ok(())
        })
        .await;
        self.read_cache.invalidate_range(offset, written);
        res.map_err(|e| io::Error::other(e.to_string()))??;
        Ok(())
    }

    fn is_padding(&self, idx: usize) -> bool {
        self.layout.files()[idx].padding
    }

    async fn span_handle(
        &self,
        idx: usize,
        write: bool,
    ) -> Result<Arc<std::fs::File>, StorageError> {
        if self.is_shadowed(idx) {
            if let Some(path) = self.shadow_path(idx) {
                return open_cached(&self.shadow_handles, &self.unsynced, idx, &path, write).await;
            }
        }
        self.handle(idx, write).await
    }

    async fn handle(&self, idx: usize, write: bool) -> Result<Arc<std::fs::File>, StorageError> {
        open_cached(
            &self.handles,
            &self.unsynced,
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
                guard
                    .iter_mut()
                    .filter_map(Option::take)
                    .map(|slot| slot.file)
                    .collect::<Vec<_>>()
            })
            .collect();
        let evicted = self.take_unsynced();
        if (snapshot.is_empty() && evicted.is_empty())
            || !self.dirty.swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            return Ok(());
        }
        self.sync_all(snapshot, evicted).await
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

    #[cfg(test)]
    pub async fn flush(&self) -> Result<(), StorageError> {
        let snapshot: Vec<_> = [&self.handles, &self.shadow_handles]
            .into_iter()
            .flat_map(|cache| {
                cache
                    .lock()
                    .iter()
                    .flatten()
                    .map(|slot| slot.file.clone())
                    .collect::<Vec<_>>()
            })
            .collect();
        let evicted = self.take_unsynced();
        if !self.dirty.swap(false, std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        self.sync_all(snapshot, evicted).await
    }
}

async fn open_cached(
    cache: &HandleCache,
    unsynced: &Mutex<HashSet<PathBuf>>,
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
        });
    }
    unsynced.lock().extend(evict_lru(&mut guard, idx));
    Ok(arc)
}

fn sync_path(path: &Path) -> io::Result<()> {
    match std::fs::OpenOptions::new().write(true).open(path) {
        Ok(file) => file.sync_data(),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

fn evict_lru(slots: &mut [Option<HandleSlot>], keep: usize) -> Vec<PathBuf> {
    if slots.len() <= MAX_OPEN_HANDLES {
        return Vec::new();
    }
    let open = slots.iter().flatten().count();
    if open <= MAX_OPEN_HANDLES {
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
        .take(open - MAX_OPEN_HANDLES)
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
        storage.flush().await.unwrap();
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
        storage.flush().await.unwrap();

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
    async fn round_trip_spanning_files() {
        let bytes = build_multi_file_torrent();
        let meta = parse_torrent(&bytes).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");

        let storage = FilesystemStorage::new(&meta.info, &root);
        storage.preallocate().await.unwrap();

        let payload: Vec<u8> = (0u8..30).collect();
        storage.write_at(0, &payload).await.unwrap();
        storage.flush().await.unwrap();

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
        let mut slots: Vec<Option<HandleSlot>> = (0..MAX_OPEN_HANDLES + 5)
            .map(|i| {
                Some(HandleSlot {
                    file: file.clone(),
                    path: PathBuf::from(i.to_string()),
                    writable: i % 2 == 0,
                    used: i as u64,
                })
            })
            .collect();
        let mut evicted = evict_lru(&mut slots, 0);
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
}
