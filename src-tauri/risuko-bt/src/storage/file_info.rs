//! File layout: maps absolute torrent offsets to (file_index, file_offset, len)

use std::path::{Path, PathBuf};

use super::super::core::metainfo::ValidatedTorrentMetaV1Info;

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: PathBuf,
    pub length: u64,
    /// Absolute offset of this file's first byte within the torrent
    pub offset: u64,
    /// BEP 47 padding: reads yield zeros, writes are dropped, nothing exists on disk
    pub padding: bool,
}

#[derive(Debug, Clone)]
pub struct FileSet {
    files: Vec<FileInfo>,
    total_length: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct Span {
    pub file_index: usize,
    pub file_offset: u64,
    pub len: u64,
}

impl FileSet {
    pub fn from_meta(info: &ValidatedTorrentMetaV1Info, root: &Path) -> Self {
        let mut offset = 0u64;
        let files = info
            .files
            .iter()
            .map(|f| {
                let mut path = root.to_path_buf();
                // For single-file torrents the parsed file already has the torrent name as its sole path component — don't double it
                for c in &f.path {
                    path.push(c);
                }
                let info = FileInfo {
                    path,
                    length: f.length,
                    offset,
                    padding: f.padding,
                };
                offset += f.length;
                info
            })
            .collect();
        Self {
            files,
            total_length: offset,
        }
    }

    pub fn files(&self) -> &[FileInfo] {
        &self.files
    }

    pub fn total_length(&self) -> u64 {
        self.total_length
    }

    /// Iterate the contiguous file spans touched by a range
    pub fn spans_for(&self, offset: u64, len: u64) -> SpansIter<'_> {
        SpansIter {
            files: &self.files,
            cursor: offset,
            remaining: len,
            idx: self.file_index_for(offset),
        }
    }

    fn file_index_for(&self, offset: u64) -> usize {
        // Binary search for the file whose range covers `offset`
        let r = self.files.binary_search_by(|f| {
            if offset < f.offset {
                std::cmp::Ordering::Greater
            } else if offset >= f.offset + f.length {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        });
        r.unwrap_or_else(|i| i.min(self.files.len().saturating_sub(1)))
    }
}

pub struct SpansIter<'a> {
    files: &'a [FileInfo],
    cursor: u64,
    remaining: u64,
    idx: usize,
}

impl Iterator for SpansIter<'_> {
    type Item = Span;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        // Skip over zero-length files iteratively to avoid unbounded recursion on torrents containing many empty entries
        while self.idx < self.files.len() && self.files[self.idx].length == 0 {
            self.idx += 1;
        }
        if self.idx >= self.files.len() {
            return None;
        }
        let f = &self.files[self.idx];
        let file_offset = self.cursor - f.offset;
        let available = f.length - file_offset;
        let take = available.min(self.remaining);
        let span = Span {
            file_index: self.idx,
            file_offset,
            len: take,
        };
        self.cursor += take;
        self.remaining -= take;
        if self.cursor >= f.offset + f.length {
            self.idx += 1;
        }
        Some(span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::metainfo::TorrentMetaInfo;

    fn fixture() -> ValidatedTorrentMetaV1Info {
        ValidatedTorrentMetaV1Info {
            name: "root".into(),
            piece_length: 16 * 1024,
            pieces: vec![0; 20],
            private: false,
            files: vec![
                TorrentMetaInfo {
                    path: vec!["a".into()],
                    length: 10,
                    padding: false,
                },
                TorrentMetaInfo {
                    path: vec!["b".into()],
                    length: 20,
                    padding: false,
                },
                TorrentMetaInfo {
                    path: vec!["c".into()],
                    length: 5,
                    padding: false,
                },
            ],
            single_file_mode: false,
        }
    }

    #[test]
    fn contiguous_spans() {
        let set = FileSet::from_meta(&fixture(), Path::new("/tmp"));
        // offset 8, len 20 — ends at byte 28, within file 1 (which covers 10..30)
        let spans: Vec<_> = set.spans_for(8, 20).collect();
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].file_index, 0);
        assert_eq!(spans[0].file_offset, 8);
        assert_eq!(spans[0].len, 2);
        assert_eq!(spans[1].file_index, 1);
        assert_eq!(spans[1].file_offset, 0);
        assert_eq!(spans[1].len, 18);
    }

    #[test]
    fn spans_three_files() {
        let set = FileSet::from_meta(&fixture(), Path::new("/tmp"));
        // Range 5..33 spans all three files
        let spans: Vec<_> = set.spans_for(5, 28).collect();
        assert_eq!(spans.len(), 3);
        assert_eq!(spans[0].len, 5);
        assert_eq!(spans[1].len, 20);
        assert_eq!(spans[2].len, 3);
    }

    #[test]
    fn partial_single_file_span() {
        let set = FileSet::from_meta(&fixture(), Path::new("/tmp"));
        let spans: Vec<_> = set.spans_for(12, 3).collect();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].file_index, 1);
        assert_eq!(spans[0].file_offset, 2);
        assert_eq!(spans[0].len, 3);
    }
}
