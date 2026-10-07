//! Storage backend: maps piece/byte offsets to underlying files with async read/write primitives; [`FilesystemStorage`] uses positional `pread`/`pwrite` via `spawn_blocking`, so non-overlapping reads/writes to one file need no internal lock (a per-file `Mutex<File>` would serialize chunk writes and cap single-file torrents at one peer)

use std::io;
use std::path::Path;
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

/// Filesystem backed storage using the torrent's file layout
pub struct FilesystemStorage {
    layout: FileSet,
    /// Open handles per file, lazily opened on first access; `Arc<std::fs::File>` is safely shareable because we only use positional I/O (`read_at`/`write_at` on Unix, `seek_read`/`seek_write` on Windows), which do not touch the shared file cursor
    handles: Mutex<Vec<Option<Arc<std::fs::File>>>>,
}

impl FilesystemStorage {
    pub fn new(info: &ValidatedTorrentMetaV1Info, root: &Path) -> Self {
        let layout = FileSet::from_meta(info, root);
        let handles = Mutex::new(vec![None; layout.files().len()]);
        Self { layout, handles }
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
        task::spawn_blocking(move || {
            !paths.is_empty()
                && paths
                    .iter()
                    .any(|p| std::fs::metadata(p).is_ok_and(|m| m.len() > 0))
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
        let mut tasks = Vec::with_capacity(spans.len());
        let mut cursor = 0usize;
        for span in spans {
            let len = span.len as usize;
            if self.is_padding(span.file_index) {
                cursor += len;
                continue;
            }
            let handle = self.handle(span.file_index).await?;
            let chunk = buf.slice(cursor..cursor + len);
            let file_offset = span.file_offset;
            tasks.push(task::spawn_blocking(move || {
                pwrite_all(&handle, file_offset, &chunk)
            }));
            cursor += len;
        }
        for t in tasks {
            t.await.map_err(|e| io::Error::other(e.to_string()))??;
        }
        Ok(())
    }

    fn is_padding(&self, idx: usize) -> bool {
        self.layout.files()[idx].padding
    }

    async fn handle(&self, idx: usize) -> Result<Arc<std::fs::File>, StorageError> {
        if let Some(h) = self.handles.lock().get(idx).and_then(|s| s.clone()) {
            return Ok(h);
        }

        let path = self.layout.files()[idx].path.clone();
        let file = task::spawn_blocking(move || -> io::Result<std::fs::File> {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&path)
        })
        .await
        .map_err(|e| io::Error::other(e.to_string()))??;
        let arc = Arc::new(file);

        let mut guard = self.handles.lock();
        if let Some(Some(existing)) = guard.get(idx).cloned() {
            return Ok(existing);
        }
        if let Some(slot) = guard.get_mut(idx) {
            *slot = Some(arc.clone());
        }
        Ok(arc)
    }

    /// Flush buffered writes and drop every cached file handle
    pub async fn close_handles(&self) -> Result<(), StorageError> {
        let snapshot: Vec<Arc<std::fs::File>> = {
            let mut guard = self.handles.lock();
            let snap = guard.iter().filter_map(|h| h.clone()).collect();
            for slot in guard.iter_mut() {
                *slot = None;
            }
            snap
        };
        let mut first_error: Option<io::Error> = None;
        for handle in snapshot {
            match task::spawn_blocking(move || handle.sync_data())
                .await
                .map_err(|e| io::Error::other(e.to_string()))
            {
                Ok(Ok(())) => {}
                Ok(Err(e)) | Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                    }
                }
            }
        }
        if let Some(e) = first_error {
            Err(StorageError::Io(e))
        } else {
            Ok(())
        }
    }

    /// Allocate all files (sparse) on disk if they don't yet exist
    pub async fn preallocate(&self) -> Result<(), StorageError> {
        // Open each file just long enough to preallocate, then drop the handle; caching handles here would defeat lazy opening and can exhaust the process open-file limit on torrents with many files
        for f in self.layout.files().iter().filter(|f| !f.padding) {
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
                // Sparse allocation via set_len reserves size in directory metadata without writing data (APFS/ext4/NTFS support sparse files), avoiding upfront I/O while still surfacing ENOSPC on later writes; truncating oversized files too so stale tail bytes from a previous larger allocation don't survive a "complete" download
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

    /// Write `buf` starting at absolute torrent offset `offset`
    pub async fn write_at(&self, offset: u64, buf: &[u8]) -> Result<(), StorageError> {
        // Generic path: copies into a Bytes once; hot piece writes use the zero-copy `write_at_owned` instead
        self.write_at_owned(offset, bytes::Bytes::copy_from_slice(buf))
            .await
    }

    /// Read `buf.len()` bytes starting at absolute torrent offset `offset`
    pub async fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<(), StorageError> {
        let total = self.layout.total_length();
        let end = offset
            .checked_add(buf.len() as u64)
            .ok_or(StorageError::OutOfRange { offset, total })?;
        if end > total {
            return Err(StorageError::OutOfRange { offset, total });
        }

        // Start all span reads first, then copy each owned buffer into place; `buf` cannot cross spawned tasks, so handles carry cursor and length
        let spans: Vec<_> = self.layout.spans_for(offset, buf.len() as u64).collect();
        let mut tasks = Vec::with_capacity(spans.len());
        let mut cursor = 0usize;
        for span in spans {
            let len = span.len as usize;
            if self.is_padding(span.file_index) {
                buf[cursor..cursor + len].fill(0);
                cursor += len;
                continue;
            }
            let handle = self.handle(span.file_index).await?;
            let file_offset = span.file_offset;
            let join = task::spawn_blocking(move || -> io::Result<Vec<u8>> {
                let mut out = vec![0u8; len];
                pread_exact(&handle, file_offset, &mut out)?;
                Ok(out)
            });
            tasks.push((cursor, len, join));
            cursor += len;
        }
        for (cursor, len, join) in tasks {
            let chunk = join.await.map_err(|e| io::Error::other(e.to_string()))??;
            buf[cursor..cursor + len].copy_from_slice(&chunk);
        }
        Ok(())
    }

    /// Flush in-flight writes
    pub async fn flush(&self) -> Result<(), StorageError> {
        let snapshot: Vec<_> = {
            let g = self.handles.lock();
            g.iter().filter_map(|h| h.clone()).collect()
        };
        for handle in snapshot {
            task::spawn_blocking(move || handle.sync_data())
                .await
                .map_err(|e| io::Error::other(e.to_string()))??;
        }
        Ok(())
    }
}

/// Positional write-all, retrying short writes until `buf` is drained; uses `pwrite` on Unix / `seek_write` on Windows so concurrent callers don't fight over a shared file cursor
fn pwrite_all(file: &std::fs::File, mut offset: u64, mut buf: &[u8]) -> io::Result<()> {
    while !buf.is_empty() {
        #[cfg(unix)]
        let n = std::os::unix::fs::FileExt::write_at(file, buf, offset)?;
        #[cfg(windows)]
        let n = std::os::windows::fs::FileExt::seek_write(file, buf, offset)?;
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

/// Positional read-exact, retrying short reads
fn pread_exact(file: &std::fs::File, mut offset: u64, mut buf: &mut [u8]) -> io::Result<()> {
    while !buf.is_empty() {
        #[cfg(unix)]
        let n = std::os::unix::fs::FileExt::read_at(file, buf, offset)?;
        #[cfg(windows)]
        let n = std::os::windows::fs::FileExt::seek_read(file, buf, offset)?;
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

        // Write a pattern that spans the first file (10 bytes) into the second
        let payload: Vec<u8> = (0u8..30).collect();
        storage.write_at(0, &payload).await.unwrap();
        storage.flush().await.unwrap();

        let mut out = vec![0u8; 30];
        storage.read_at(0, &mut out).await.unwrap();
        assert_eq!(out, payload);

        // Verify underlying files actually got the right bytes
        let a = tokio::fs::read(root.join("a.txt")).await.unwrap();
        assert_eq!(a, payload[..10]);
        let b = tokio::fs::read(root.join("sub").join("b.bin"))
            .await
            .unwrap();
        assert_eq!(b, payload[10..]);
    }

    #[tokio::test]
    async fn close_handles_releases_cached_descriptors() {
        let bytes = build_multi_file_torrent();
        let meta = parse_torrent(&bytes).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("root");

        let storage = FilesystemStorage::new(&meta.info, &root);
        storage.preallocate().await.unwrap();

        // Writing both files lazily opens and caches a handle per file
        let payload: Vec<u8> = (0u8..30).collect();
        storage.write_at(0, &payload).await.unwrap();
        assert!(
            storage.handles.lock().iter().any(|h| h.is_some()),
            "expected at least one cached handle after a write"
        );

        // Releasing must drop every cached descriptor
        storage.close_handles().await.unwrap();
        assert!(
            storage.handles.lock().iter().all(|h| h.is_none()),
            "close_handles must clear all cached file handles"
        );

        // Data persisted and reads transparently reopen handles afterwards
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
}
