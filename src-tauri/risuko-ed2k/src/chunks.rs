use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::OpenOptions;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use super::md4::{md4, Md4};
use super::partial::Met;
use super::types::*;

pub const ED2K_BLOCK_SIZE: u64 = 184_320;

const MAX_DUPLICATE_REQUESTERS: u16 = 2;

#[derive(Debug, Clone, Copy)]
struct Block {
    start: u64,
    end: u64,
    part: usize,
}

pub struct VerifyJob {
    pub index: usize,
    pub path: PathBuf,
    pub start: u64,
    pub end: u64,
    pub expected: [u8; 16],
}

const MAX_PEER_STRIKES: u32 = 2;

pub struct WriteTicket {
    file: Arc<std::fs::File>,
    part: usize,
    offset: u64,
    end: u64,
    in_flight: Arc<parking_lot::Mutex<Vec<(u64, u64)>>>,
}

impl Drop for WriteTicket {
    fn drop(&mut self) {
        let mut held = self.in_flight.lock();
        if let Some(i) = held.iter().position(|&r| r == (self.offset, self.end)) {
            held.swap_remove(i);
        }
    }
}

#[cfg(unix)]
fn write_all_at(file: &std::fs::File, data: &[u8], offset: u64) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(data, offset)
}

#[cfg(windows)]
fn write_all_at(file: &std::fs::File, mut data: &[u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !data.is_empty() {
        let n = file.seek_write(data, offset)?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        data = &data[n..];
        offset += n as u64;
    }
    Ok(())
}

pub fn hash_part_from_disk(job: &VerifyJob) -> std::io::Result<[u8; 16]> {
    use std::io::{Read, Seek};
    let mut file = std::fs::File::open(&job.path)?;
    file.seek(std::io::SeekFrom::Start(job.start))?;
    let mut hasher = Md4::new();
    let mut remaining = job.end - job.start;
    let mut buf = vec![0u8; 64 * 1024];
    while remaining > 0 {
        let want = remaining.min(buf.len() as u64) as usize;
        file.read_exact(&mut buf[..want])?;
        hasher.update(&buf[..want]);
        remaining -= want as u64;
    }
    Ok(hasher.finalize())
}

pub struct ChunkManager {
    file_path: PathBuf,
    file_size: u64,
    file_hash: [u8; 16],
    chunk_count: u64,
    chunk_hashes: Option<Vec<[u8; 16]>>,
    chunk_status: Vec<ChunkStatus>,
    hashing: Vec<bool>,
    covered: Vec<Vec<(u64, u64)>>,
    part_bytes: Vec<u64>,
    blocks: Vec<Block>,
    first_block: Vec<usize>,
    req_count: Vec<u16>,
    peer_blocks: HashMap<u64, Vec<usize>>,
    completed_length: u64,
    file: Option<Arc<std::fs::File>>,
    part_sources: Vec<Vec<u64>>,
    strikes: HashMap<u64, u32>,
    in_flight: Arc<parking_lot::Mutex<Vec<(u64, u64)>>>,
    met_path: Option<PathBuf>,
    met_dirty: bool,
    cancel: Option<CancellationToken>,
}

pub async fn write_block(
    chunks: &Mutex<ChunkManager>,
    peer: u64,
    offset: u64,
    data: Vec<u8>,
) -> Result<bool, String> {
    let Some(ticket) = chunks.lock().await.begin_write(peer, offset, data.len())? else {
        return Ok(false);
    };
    let (ticket, result) = tokio::task::spawn_blocking(move || {
        let result = write_all_at(&ticket.file, &data, offset);
        (ticket, result)
    })
    .await
    .map_err(|e| format!("Write task failed: {}", e))?;
    result.map_err(|e| format!("Write failed: {}", e))?;
    Ok(chunks.lock().await.commit_write(peer, &ticket))
}

fn insert_range(ranges: &mut Vec<(u64, u64)>, s: u64, e: u64) -> u64 {
    let mut ns = s;
    let mut ne = e;
    let mut overlap = 0;
    let mut out = Vec::with_capacity(ranges.len() + 1);
    let mut placed = false;
    for &(a, b) in ranges.iter() {
        if b < s || a > e {
            if a > e && !placed {
                out.push((ns, ne));
                placed = true;
            }
            out.push((a, b));
        } else {
            overlap += b.min(e).saturating_sub(a.max(s));
            ns = ns.min(a);
            ne = ne.max(b);
        }
    }
    if !placed {
        out.push((ns, ne));
    }
    *ranges = out;
    (e - s) - overlap
}

fn covers(ranges: &[(u64, u64)], s: u64, e: u64) -> bool {
    ranges.iter().any(|&(a, b)| a <= s && b >= e)
}

impl ChunkManager {
    pub fn new(file_path: PathBuf, file_size: u64) -> Self {
        let count = chunk_count(file_size);
        let mut blocks = Vec::new();
        let mut first_block = Vec::with_capacity(count as usize + 1);
        for part in 0..count {
            first_block.push(blocks.len());
            let ps = part * ED2K_CHUNK_SIZE;
            let pe = (ps + ED2K_CHUNK_SIZE).min(file_size);
            let mut s = ps;
            while s < pe {
                let e = (s + ED2K_BLOCK_SIZE).min(pe);
                blocks.push(Block {
                    start: s,
                    end: e,
                    part: part as usize,
                });
                s = e;
            }
        }
        first_block.push(blocks.len());
        Self {
            file_path,
            file_size,
            file_hash: [0; 16],
            chunk_count: count,
            chunk_hashes: None,
            chunk_status: vec![ChunkStatus::Missing; count as usize],
            hashing: vec![false; count as usize],
            covered: vec![Vec::new(); count as usize],
            part_bytes: vec![0; count as usize],
            req_count: vec![0; blocks.len()],
            blocks,
            first_block,
            peer_blocks: HashMap::new(),
            completed_length: 0,
            file: None,
            part_sources: vec![Vec::new(); count as usize],
            strikes: HashMap::new(),
            in_flight: Arc::new(parking_lot::Mutex::new(Vec::new())),
            met_path: None,
            met_dirty: false,
            cancel: None,
        }
    }

    pub fn with_met(mut self, met_path: PathBuf, cancel: CancellationToken) -> Self {
        self.met_path = Some(met_path);
        self.cancel = Some(cancel);
        self
    }

    pub fn with_file_hash(mut self, file_hash: [u8; 16]) -> Self {
        self.file_hash = file_hash;
        self
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn completed_length(&self) -> u64 {
        self.completed_length
    }

    pub fn is_complete(&self) -> bool {
        self.chunk_status
            .iter()
            .all(|s| *s == ChunkStatus::Downloaded)
    }

    pub fn set_chunk_hashes(&mut self, hashes: Vec<[u8; 16]>) -> bool {
        if self.chunk_hashes.is_some() || self.file_size < ED2K_CHUNK_SIZE {
            return true;
        }
        let expected_len = (self.file_size / ED2K_CHUNK_SIZE + 1) as usize;
        if hashes.len() != expected_len {
            return false;
        }
        let concat: Vec<u8> = hashes.iter().flatten().copied().collect();
        if md4(&concat) != self.file_hash {
            return false;
        }
        self.chunk_hashes = Some(hashes);
        self.met_dirty = true;
        true
    }

    pub fn restore_jobs(&self, claimed: &[u32]) -> Vec<VerifyJob> {
        let mut seen = std::collections::HashSet::new();
        let mut jobs = Vec::new();
        for &part in claimed {
            let index = part as usize;
            if index >= self.chunk_count as usize
                || self.chunk_status[index] != ChunkStatus::Missing
                || !seen.insert(index)
            {
                continue;
            }
            let Some(expected) = self.expected_part_hash(index) else {
                continue;
            };
            let (start, end) = self.chunk_range(index as u64);
            jobs.push(VerifyJob {
                index,
                path: self.file_path.clone(),
                start,
                end,
                expected,
            });
        }
        jobs
    }

    pub fn restore_verified(&mut self, part: usize) {
        if part >= self.chunk_count as usize || self.chunk_status[part] != ChunkStatus::Missing {
            return;
        }
        let (ps, pe) = self.chunk_range(part as u64);
        self.covered[part] = vec![(ps, pe)];
        self.part_bytes[part] = pe - ps;
        self.completed_length += pe - ps;
        self.chunk_status[part] = ChunkStatus::Downloaded;
    }

    pub fn met_snapshot(&self) -> Met {
        let verified = (0..self.chunk_count as usize)
            .filter(|&i| self.chunk_status[i] == ChunkStatus::Downloaded)
            .map(|i| i as u32)
            .collect();
        Met::new(
            self.file_hash,
            self.file_size,
            self.chunk_hashes.as_deref(),
            verified,
        )
    }

    pub fn take_met_save(&mut self) -> Option<(PathBuf, Met)> {
        if !self.met_dirty || self.cancel.as_ref().is_some_and(|c| c.is_cancelled()) {
            return None;
        }
        let path = self.met_path.clone()?;
        self.met_dirty = false;
        Some((path, self.met_snapshot()))
    }

    fn expected_part_hash(&self, index: usize) -> Option<[u8; 16]> {
        if self.file_size < ED2K_CHUNK_SIZE {
            return Some(self.file_hash);
        }
        self.chunk_hashes.as_ref()?.get(index).copied()
    }

    pub fn chunk_range(&self, index: u64) -> (u64, u64) {
        let start = index * ED2K_CHUNK_SIZE;
        let end = std::cmp::min(start + ED2K_CHUNK_SIZE, self.file_size);
        (start, end)
    }

    fn peer_has(peer_parts: &[bool], part: usize) -> bool {
        peer_parts.is_empty() || peer_parts.get(part).copied().unwrap_or(false)
    }

    pub fn needs_any(&self, peer_parts: &[bool]) -> bool {
        (0..self.chunk_count as usize)
            .any(|i| self.chunk_status[i] == ChunkStatus::Missing && Self::peer_has(peer_parts, i))
    }

    fn block_done(&self, idx: usize) -> bool {
        let b = self.blocks[idx];
        covers(&self.covered[b.part], b.start, b.end)
    }

    fn part_started(&self, part: usize) -> bool {
        self.part_bytes[part] > 0
            || (self.first_block[part]..self.first_block[part + 1]).any(|i| self.req_count[i] > 0)
    }

    pub fn assign_blocks(&mut self, peer: u64, peer_parts: &[bool], max: usize) -> Vec<(u64, u64)> {
        let mine = self.peer_blocks.remove(&peer).unwrap_or_default();
        let (done, pending): (Vec<usize>, Vec<usize>) =
            mine.into_iter().partition(|&i| self.block_done(i));
        for i in done {
            self.req_count[i] = self.req_count[i].saturating_sub(1);
        }
        let mine = pending;
        if !mine.is_empty() {
            self.peer_blocks.insert(peer, mine);
            return Vec::new();
        }

        let mut picked: Vec<usize> = Vec::with_capacity(max);
        let parts = self.chunk_count as usize;
        'fresh: for pass in 0..2 {
            for part in 0..parts {
                if self.chunk_status[part] != ChunkStatus::Missing
                    || !Self::peer_has(peer_parts, part)
                    || self.part_started(part) != (pass == 0)
                {
                    continue;
                }
                for i in self.first_block[part]..self.first_block[part + 1] {
                    if self.req_count[i] == 0 && !self.block_done(i) {
                        picked.push(i);
                        if picked.len() >= max {
                            break 'fresh;
                        }
                    }
                }
            }
        }
        if picked.is_empty() {
            'endgame: for part in 0..parts {
                if self.chunk_status[part] != ChunkStatus::Missing
                    || !Self::peer_has(peer_parts, part)
                {
                    continue;
                }
                for i in self.first_block[part]..self.first_block[part + 1] {
                    if self.req_count[i] > 0
                        && self.req_count[i] < MAX_DUPLICATE_REQUESTERS
                        && !self.block_done(i)
                    {
                        picked.push(i);
                        if picked.len() >= max {
                            break 'endgame;
                        }
                    }
                }
            }
        }
        if picked.is_empty() {
            return Vec::new();
        }
        let mut ranges = Vec::with_capacity(picked.len());
        for &i in &picked {
            self.req_count[i] = self.req_count[i].saturating_add(1);
            ranges.push((self.blocks[i].start, self.blocks[i].end));
        }
        self.peer_blocks.insert(peer, picked);
        ranges
    }

    pub fn release_peer(&mut self, peer: u64) {
        if let Some(held) = self.peer_blocks.remove(&peer) {
            for i in held {
                self.req_count[i] = self.req_count[i].saturating_sub(1);
            }
        }
    }

    pub async fn init_file(&mut self) -> Result<(), String> {
        if let Some(parent) = self.file_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| format!("Failed to create dir: {}", e))?;
        }

        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.file_path)
            .await
            .map_err(|e| format!("Failed to create file: {}", e))?;

        file.set_len(self.file_size)
            .await
            .map_err(|e| format!("Failed to pre-allocate: {}", e))?;

        self.file = Some(Arc::new(file.into_std().await));
        Ok(())
    }

    pub fn begin_write(
        &self,
        peer: u64,
        offset: u64,
        len: usize,
    ) -> Result<Option<WriteTicket>, String> {
        if len == 0 {
            return Ok(None);
        }
        let end = offset.saturating_add(len as u64);
        if end > self.file_size {
            return Err(format!(
                "Write out of bounds: offset {} + len {} exceeds file size {}",
                offset, len, self.file_size
            ));
        }
        let Some(part) = self.writable_part(peer, offset, end) else {
            return Ok(None);
        };
        let file = self
            .file
            .clone()
            .ok_or_else(|| "Output file not open".to_string())?;
        let mut held = self.in_flight.lock();
        if held.iter().any(|&(a, b)| a < end && offset < b) {
            return Ok(None);
        }
        held.push((offset, end));
        drop(held);
        Ok(Some(WriteTicket {
            file,
            part,
            offset,
            end,
            in_flight: self.in_flight.clone(),
        }))
    }

    fn writable_part(&self, peer: u64, offset: u64, end: u64) -> Option<usize> {
        let block = self.peer_blocks.get(&peer).and_then(|held| {
            held.iter()
                .map(|&i| self.blocks[i])
                .find(|b| b.start <= offset && end <= b.end)
        })?;
        let part = block.part;
        (self.chunk_status[part] == ChunkStatus::Missing
            && !covers(&self.covered[part], offset, end))
        .then_some(part)
    }

    pub fn commit_write(&mut self, peer: u64, ticket: &WriteTicket) -> bool {
        let (offset, end) = (ticket.offset, ticket.end);
        if self.writable_part(peer, offset, end) != Some(ticket.part) {
            return false;
        }
        let part = ticket.part;
        let added = insert_range(&mut self.covered[part], offset, end);
        self.part_bytes[part] += added;
        self.completed_length += added;
        if !self.part_sources[part].contains(&peer) {
            self.part_sources[part].push(peer);
        }

        let (ps, pe) = self.chunk_range(part as u64);
        if self.part_bytes[part] >= pe - ps {
            self.chunk_status[part] = ChunkStatus::Verifying;
        }
        true
    }

    pub fn is_banned(&self, peer: u64) -> bool {
        self.strikes.get(&peer).copied().unwrap_or(0) >= MAX_PEER_STRIKES
    }

    pub fn take_verify_jobs(&mut self) -> Vec<VerifyJob> {
        let mut jobs = Vec::new();
        for part in 0..self.chunk_count as usize {
            if self.chunk_status[part] != ChunkStatus::Verifying || self.hashing[part] {
                continue;
            }
            let Some(expected) = self.expected_part_hash(part) else {
                continue;
            };
            let (start, end) = self.chunk_range(part as u64);
            self.hashing[part] = true;
            jobs.push(VerifyJob {
                index: part,
                path: self.file_path.clone(),
                start,
                end,
                expected,
            });
        }
        jobs
    }

    pub fn finish_verify(&mut self, part: usize, ok: bool) {
        if part >= self.chunk_status.len() {
            return;
        }
        self.hashing[part] = false;
        let sources = std::mem::take(&mut self.part_sources[part]);
        if ok {
            self.chunk_status[part] = ChunkStatus::Downloaded;
            self.met_dirty = true;
            return;
        }
        if let [peer] = sources.as_slice() {
            *self.strikes.entry(*peer).or_insert(0) += 1;
        }
        self.completed_length = self.completed_length.saturating_sub(self.part_bytes[part]);
        self.part_bytes[part] = 0;
        self.covered[part].clear();
        self.chunk_status[part] = ChunkStatus::Missing;
        let (lo, hi) = (self.first_block[part], self.first_block[part + 1]);
        for held in self.peer_blocks.values_mut() {
            held.retain(|&i| !(lo..hi).contains(&i));
        }
        for c in &mut self.req_count[lo..hi] {
            *c = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager(size: u64, hash: [u8; 16]) -> (ChunkManager, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let cm = ChunkManager::new(dir.path().join("f.bin"), size).with_file_hash(hash);
        (cm, dir)
    }

    fn put(cm: &mut ChunkManager, peer: u64, offset: u64, data: &[u8]) -> bool {
        let Some(ticket) = cm.begin_write(peer, offset, data.len()).unwrap() else {
            return false;
        };
        write_all_at(&ticket.file, data, offset).unwrap();
        cm.commit_write(peer, &ticket)
    }

    #[test]
    fn insert_range_counts_only_new_bytes() {
        let mut r = Vec::new();
        assert_eq!(insert_range(&mut r, 10, 20), 10);
        assert_eq!(insert_range(&mut r, 15, 25), 5);
        assert_eq!(insert_range(&mut r, 10, 25), 0);
        assert_eq!(insert_range(&mut r, 30, 40), 10);
        assert_eq!(insert_range(&mut r, 25, 30), 5);
        assert_eq!(r, vec![(10, 40)]);
    }

    #[tokio::test]
    async fn duplicate_bytes_do_not_advance_progress_or_complete_parts() {
        let data = vec![7u8; 400_000];
        let (mut cm, _dir) = manager(data.len() as u64, md4(&data));
        cm.init_file().await.unwrap();
        let a = cm.assign_blocks(1, &[], 3);
        assert_eq!(
            a,
            vec![(0, 184_320), (184_320, 368_640), (368_640, 400_000)]
        );
        assert_eq!(cm.assign_blocks(2, &[], 3).len(), 3);

        assert!(put(&mut cm, 1, 0, &data[..100_000]));
        assert!(!put(&mut cm, 1, 0, &data[..100_000]));
        assert_eq!(cm.completed_length(), 100_000);
        assert!(!cm.is_complete());
    }

    #[tokio::test]
    async fn rejects_data_outside_the_peers_request() {
        let data = vec![1u8; 400_000];
        let (mut cm, _dir) = manager(data.len() as u64, md4(&data));
        cm.init_file().await.unwrap();
        assert!(!put(&mut cm, 9, 0, &data[..10]));
        cm.assign_blocks(1, &[], 1);
        assert!(!put(&mut cm, 1, 184_320, &data[..10]));
        assert!(!put(&mut cm, 2, 0, &data[..10]));
        assert_eq!(cm.completed_length(), 0);
    }

    #[tokio::test]
    async fn part_completes_only_after_matching_hash() {
        let data: Vec<u8> = (0..400_000u32).map(|i| i as u8).collect();
        let (mut cm, _dir) = manager(data.len() as u64, md4(&data));
        cm.init_file().await.unwrap();
        let ranges = cm.assign_blocks(1, &[], 3);
        for (s, e) in ranges {
            assert!(put(&mut cm, 1, s, &data[s as usize..e as usize]));
        }
        assert_eq!(cm.completed_length(), 400_000);
        assert!(!cm.is_complete());
        let jobs = cm.take_verify_jobs();
        assert_eq!(jobs.len(), 1);
        assert!(cm.take_verify_jobs().is_empty());
        let got = hash_part_from_disk(&jobs[0]).unwrap();
        assert_eq!(got, jobs[0].expected);
        cm.finish_verify(0, got == jobs[0].expected);
        assert!(cm.is_complete());
    }

    #[tokio::test]
    async fn corrupt_part_is_reset_and_refetched() {
        let data = vec![3u8; 100_000];
        let (mut cm, _dir) = manager(data.len() as u64, md4(&data));
        cm.init_file().await.unwrap();
        let bad = vec![4u8; 100_000];
        assert_eq!(cm.assign_blocks(1, &[], 3).len(), 1);
        assert!(put(&mut cm, 1, 0, &bad));
        let jobs = cm.take_verify_jobs();
        let got = hash_part_from_disk(&jobs[0]).unwrap();
        cm.finish_verify(0, got == jobs[0].expected);
        assert!(!cm.is_complete());
        assert_eq!(cm.completed_length(), 0);
        assert_eq!(cm.assign_blocks(1, &[], 3), vec![(0, 100_000)]);
        assert!(put(&mut cm, 1, 0, &data));
        let jobs = cm.take_verify_jobs();
        let got = hash_part_from_disk(&jobs[0]).unwrap();
        cm.finish_verify(0, got == jobs[0].expected);
        assert!(cm.is_complete());
    }

    #[tokio::test]
    async fn sole_contributor_of_a_corrupt_part_is_struck_and_banned() {
        let data = vec![3u8; 100_000];
        let bad = vec![4u8; 100_000];
        let (mut cm, _dir) = manager(data.len() as u64, md4(&data));
        cm.init_file().await.unwrap();
        for round in 0..MAX_PEER_STRIKES {
            assert!(!cm.is_banned(1), "banned before round {round}");
            cm.assign_blocks(1, &[], 3);
            assert!(put(&mut cm, 1, 0, &bad));
            let jobs = cm.take_verify_jobs();
            let got = hash_part_from_disk(&jobs[0]).unwrap();
            cm.finish_verify(0, got == jobs[0].expected);
        }
        assert!(cm.is_banned(1));
        assert!(!cm.is_banned(2));
    }

    #[tokio::test]
    async fn shared_corrupt_part_blames_nobody() {
        let size = ED2K_BLOCK_SIZE * 2;
        let good = vec![5u8; size as usize];
        let (mut cm, _dir) = manager(size, md4(&good));
        cm.init_file().await.unwrap();
        cm.assign_blocks(1, &[], 1);
        cm.assign_blocks(2, &[], 1);
        assert!(put(&mut cm, 1, 0, &vec![9u8; ED2K_BLOCK_SIZE as usize]));
        assert!(put(
            &mut cm,
            2,
            ED2K_BLOCK_SIZE,
            &good[ED2K_BLOCK_SIZE as usize..]
        ));
        let jobs = cm.take_verify_jobs();
        let got = hash_part_from_disk(&jobs[0]).unwrap();
        cm.finish_verify(0, got == jobs[0].expected);
        assert!(!cm.is_banned(1) && !cm.is_banned(2));
        assert_eq!(cm.strikes.len(), 0);
    }

    #[tokio::test]
    async fn write_block_writes_outside_the_lock_and_rejects_unrequested() {
        let data = vec![8u8; 50_000];
        let (mut cm, _dir) = manager(data.len() as u64, md4(&data));
        cm.init_file().await.unwrap();
        cm.assign_blocks(1, &[], 3);
        let chunks = Mutex::new(cm);
        assert!(write_block(&chunks, 1, 0, data.clone()).await.unwrap());
        assert!(!write_block(&chunks, 2, 0, data.clone()).await.unwrap());
        assert_eq!(chunks.lock().await.completed_length(), 50_000);
    }

    #[tokio::test]
    async fn overlapping_write_is_refused_while_one_is_in_flight() {
        let data = vec![8u8; 50_000];
        let (mut cm, _dir) = manager(data.len() as u64, md4(&data));
        cm.init_file().await.unwrap();
        cm.assign_blocks(1, &[], 3);
        cm.assign_blocks(2, &[], 3);
        let first = cm.begin_write(1, 0, 1000).unwrap().unwrap();
        assert!(cm.begin_write(2, 500, 1000).unwrap().is_none());
        drop(first);
        assert!(cm.begin_write(2, 500, 1000).unwrap().is_some());
    }

    #[test]
    fn hashset_must_chain_to_file_hash() {
        let size = ED2K_CHUNK_SIZE + 10;
        let h = [[1u8; 16], [2u8; 16]];
        let root = md4(&h.concat());
        let (mut cm, _d) = manager(size, root);
        assert!(!cm.set_chunk_hashes(vec![[1u8; 16], [9u8; 16]]));
        assert!(!cm.set_chunk_hashes(vec![[1u8; 16]]));
        assert!(cm.set_chunk_hashes(h.to_vec()));
        assert_eq!(cm.expected_part_hash(1), Some([2u8; 16]));
    }

    #[test]
    fn peers_are_spread_across_blocks_and_released_blocks_return() {
        let (mut cm, _d) = manager(ED2K_CHUNK_SIZE * 2, [0; 16]);
        let a = cm.assign_blocks(1, &[], 3);
        let b = cm.assign_blocks(2, &[], 3);
        assert_eq!(a.len(), 3);
        assert_eq!(b.len(), 3);
        assert!(a.iter().all(|r| !b.contains(r)));
        assert!(cm.assign_blocks(1, &[], 3).is_empty());
        cm.release_peer(1);
        let c = cm.assign_blocks(3, &[], 3);
        assert_eq!(c, a);
    }

    #[test]
    fn partial_sources_only_get_parts_they_have() {
        let (mut cm, _d) = manager(ED2K_CHUNK_SIZE * 2, [0; 16]);
        let r = cm.assign_blocks(1, &[false, true], 1);
        assert_eq!(r[0].0, ED2K_CHUNK_SIZE);
        assert!(cm.needs_any(&[false, true]));
        assert!(!cm.needs_any(&[false, false]));
    }

    #[test]
    fn ranges_past_4_gib_keep_full_offsets() {
        let size = 5 * 1024 * 1024 * 1024 + 123_456u64;
        let mut cm = ChunkManager::new(PathBuf::from("unused"), size);
        assert_eq!(cm.file_size(), size);
        let last = chunk_count(size) - 1;
        let (s, e) = cm.chunk_range(last);
        assert_eq!(e, size);
        assert!(s > u64::from(u32::MAX) && e - s <= ED2K_CHUNK_SIZE);
        let mut parts = vec![false; chunk_count(size) as usize];
        parts[last as usize] = true;
        let ranges = cm.assign_blocks(1, &parts, 3);
        assert_eq!(ranges.len(), 3);
        assert_eq!(ranges[0].0, s);
        assert!(ranges
            .iter()
            .all(|r| r.0 > u64::from(u32::MAX) && r.1 > r.0));
        assert!(ranges.windows(2).all(|w| w[0].1 == w[1].0));
    }
}
