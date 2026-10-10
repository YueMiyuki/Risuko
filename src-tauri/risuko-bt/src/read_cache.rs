use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::storage::StorageError;

pub const DEFAULT_CAPACITY: usize = 8 * 1024 * 1024;
pub const PROCESS_CAPACITY: usize = 64 * 1024 * 1024;
pub const MAX_IDLE: Duration = Duration::from_secs(30);

static TOTAL_BYTES: AtomicUsize = AtomicUsize::new(0);

fn reserve_total(total: &AtomicUsize, len: usize, cap: usize) -> bool {
    let prev = total.fetch_add(len, Ordering::Relaxed);
    if prev.saturating_add(len) > cap {
        total.fetch_sub(len, Ordering::Relaxed);
        return false;
    }
    true
}

pub const WINDOW: u64 = 128 * 1024;

pub fn extent_for(
    piece_offset: u64,
    piece_len: u64,
    begin: u64,
    length: u64,
) -> Option<(u64, usize)> {
    let start = begin & !(WINDOW - 1);
    let end = (start + WINDOW).min(piece_len);
    (begin + length <= end && end - start > length)
        .then(|| (piece_offset + start, (end - start) as usize))
}

struct Entry {
    data: Bytes,
    used: u64,
    last: Instant,
}

struct Inflight {
    id: u64,
    len: usize,
    waiters: Vec<oneshot::Sender<Bytes>>,
}

#[derive(Default)]
struct Inner {
    generation: u64,
    tick: u64,
    next_id: u64,
    bytes: usize,
    entries: HashMap<u64, Entry>,
    lru: BTreeMap<u64, u64>,
    inflight: HashMap<u64, Inflight>,
}

pub struct ReadCache {
    capacity: usize,
    total: &'static AtomicUsize,
    process_cap: usize,
    inner: Mutex<Inner>,
    active: AtomicBool,
    loads: AtomicU64,
}

impl Drop for ReadCache {
    fn drop(&mut self) {
        let held = self.inner.get_mut().bytes;
        self.release(held);
    }
}

impl Default for ReadCache {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }
}

struct LeaderGuard<'a> {
    cache: &'a ReadCache,
    start: u64,
    id: u64,
}

impl Drop for LeaderGuard<'_> {
    fn drop(&mut self) {
        let mut inner = self.cache.inner.lock();
        if inner
            .inflight
            .get(&self.start)
            .is_some_and(|f| f.id == self.id)
        {
            inner.inflight.remove(&self.start);
        }
        self.cache.refresh_active(&inner);
    }
}

impl ReadCache {
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_limits(capacity, &TOTAL_BYTES, PROCESS_CAPACITY)
    }

    fn with_limits(capacity: usize, total: &'static AtomicUsize, process_cap: usize) -> Self {
        Self {
            capacity,
            total,
            process_cap,
            inner: Mutex::new(Inner::default()),
            active: AtomicBool::new(false),
            loads: AtomicU64::new(0),
        }
    }

    fn release(&self, len: usize) {
        if len > 0 {
            self.total.fetch_sub(len, Ordering::Relaxed);
        }
    }

    pub fn admits(&self, start: u64, len: usize) -> bool {
        if len > self.capacity {
            return false;
        }
        let inner = self.inner.lock();
        inner
            .entries
            .get(&start)
            .is_some_and(|e| e.data.len() == len)
            || inner.inflight.get(&start).is_some_and(|f| f.len == len)
            || self
                .total
                .load(Ordering::Relaxed)
                .saturating_sub(inner.bytes)
                + len
                <= self.process_cap
    }

    pub fn loads(&self) -> u64 {
        self.loads.load(Ordering::Relaxed)
    }

    pub fn bytes(&self) -> usize {
        self.inner.lock().bytes
    }

    fn refresh_active(&self, inner: &Inner) {
        self.active.store(
            !inner.entries.is_empty() || !inner.inflight.is_empty(),
            Ordering::Release,
        );
    }

    pub fn invalidate_all(&self) {
        if !self.active.load(Ordering::Acquire) {
            return;
        }
        let mut inner = self.inner.lock();
        inner.generation += 1;
        inner.entries.clear();
        inner.lru.clear();
        self.release(inner.bytes);
        inner.bytes = 0;
        inner.inflight.clear();
        self.refresh_active(&inner);
    }

    pub fn invalidate_range(&self, offset: u64, len: u64) {
        if !self.active.load(Ordering::Acquire) {
            return;
        }
        let end = offset.saturating_add(len);
        let overlaps = |start: u64, n: usize| start < end && offset < start + n as u64;
        let mut inner = self.inner.lock();
        let before = inner.entries.len() + inner.inflight.len();
        let mut freed = 0;
        let mut gone = Vec::new();
        inner.entries.retain(|&start, e| {
            let keep = !overlaps(start, e.data.len());
            if !keep {
                freed += e.data.len();
                gone.push(e.used);
            }
            keep
        });
        for used in gone {
            inner.lru.remove(&used);
        }
        inner.bytes -= freed;
        self.release(freed);
        inner.inflight.retain(|&start, f| !overlaps(start, f.len));
        if inner.entries.len() + inner.inflight.len() != before {
            inner.generation += 1;
        }
        self.refresh_active(&inner);
    }

    pub fn trim_idle(&self) {
        if !self.active.load(Ordering::Acquire) {
            return;
        }
        let now = Instant::now();
        let mut inner = self.inner.lock();
        let mut freed = 0;
        let mut gone = Vec::new();
        inner.entries.retain(|_, e| {
            let keep = now.duration_since(e.last) < MAX_IDLE;
            if !keep {
                freed += e.data.len();
                gone.push(e.used);
            }
            keep
        });
        for used in gone {
            inner.lru.remove(&used);
        }
        inner.bytes -= freed;
        self.release(freed);
        self.refresh_active(&inner);
    }

    pub async fn get<F, Fut>(
        &self,
        start: u64,
        len: usize,
        block_off: u64,
        block_len: usize,
        load: F,
    ) -> Result<Bytes, StorageError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<u8>, StorageError>>,
    {
        let rel = (block_off - start) as usize;
        let slice = |b: &Bytes| b.slice(rel..rel + block_len);
        enum Role {
            Hit(Bytes),
            Wait(oneshot::Receiver<Bytes>),
            Lead(u64, u64),
        }
        let role = {
            let mut locked = self.inner.lock();
            let inner = &mut *locked;
            inner.tick += 1;
            let tick = inner.tick;
            if let Some(e) = inner
                .entries
                .get_mut(&start)
                .filter(|e| e.data.len() == len)
            {
                inner.lru.remove(&e.used);
                e.used = tick;
                e.last = Instant::now();
                inner.lru.insert(tick, start);
                Role::Hit(e.data.clone())
            } else if let Some(f) = inner.inflight.get_mut(&start).filter(|f| f.len == len) {
                let (tx, rx) = oneshot::channel();
                f.waiters.push(tx);
                Role::Wait(rx)
            } else {
                inner.next_id += 1;
                let id = inner.next_id;
                let generation = inner.generation;
                inner.inflight.insert(
                    start,
                    Inflight {
                        id,
                        len,
                        waiters: Vec::new(),
                    },
                );
                self.refresh_active(inner);
                Role::Lead(id, generation)
            }
        };
        match role {
            Role::Hit(data) => Ok(slice(&data)),
            Role::Wait(rx) => match rx.await {
                Ok(data) => Ok(slice(&data)),
                Err(_) => Err(StorageError::Io(io::Error::other("shared read failed"))),
            },
            Role::Lead(id, generation) => {
                let guard = LeaderGuard {
                    cache: self,
                    start,
                    id,
                };
                self.loads.fetch_add(1, Ordering::Relaxed);
                let result = load().await;
                let data = match result {
                    Ok(v) if v.len() == len => Bytes::from(v),
                    Ok(_) => {
                        self.invalidate_all();
                        return Err(StorageError::Io(io::Error::other("short extent read")));
                    }
                    Err(e) => {
                        self.invalidate_all();
                        return Err(e);
                    }
                };
                {
                    let mut locked = self.inner.lock();
                    let inner = &mut *locked;
                    let ours = inner.inflight.get(&start).is_some_and(|f| f.id == id);
                    if ours {
                        if let Some(f) = inner.inflight.remove(&start) {
                            for tx in f.waiters {
                                let _ = tx.send(data.clone());
                            }
                        }
                        if inner.generation == generation && len <= self.capacity {
                            if let Some(old) = inner.entries.remove(&start) {
                                inner.lru.remove(&old.used);
                                inner.bytes -= old.data.len();
                                self.release(old.data.len());
                            }
                            let fits_process = |inner: &Inner| {
                                self.total
                                    .load(Ordering::Relaxed)
                                    .saturating_sub(inner.bytes)
                                    + len
                                    <= self.process_cap
                            };
                            if fits_process(inner) {
                                while inner.bytes + len > self.capacity
                                    || self.total.load(Ordering::Relaxed) + len > self.process_cap
                                {
                                    let Some((_, k)) = inner.lru.pop_first() else {
                                        break;
                                    };
                                    if let Some(e) = inner.entries.remove(&k) {
                                        inner.bytes -= e.data.len();
                                        self.release(e.data.len());
                                    }
                                }
                            }
                            if inner.bytes + len <= self.capacity
                                && reserve_total(self.total, len, self.process_cap)
                            {
                                inner.tick += 1;
                                let used = inner.tick;
                                inner.lru.insert(used, start);
                                inner.entries.insert(
                                    start,
                                    Entry {
                                        data: data.clone(),
                                        used,
                                        last: Instant::now(),
                                    },
                                );
                                inner.bytes += len;
                            }
                        }
                    }
                    self.refresh_active(inner);
                }
                drop(guard);
                Ok(slice(&data))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn data(len: usize, tag: u8) -> Vec<u8> {
        (0..len).map(|i| (i as u8).wrapping_add(tag)).collect()
    }

    #[test]
    fn process_total_refuses_past_the_cap() {
        let total = AtomicUsize::new(0);
        assert!(reserve_total(&total, 60, 100));
        assert!(!reserve_total(&total, 50, 100));
        assert_eq!(total.load(Ordering::Relaxed), 60);
        assert!(reserve_total(&total, 40, 100));
        assert_eq!(total.load(Ordering::Relaxed), 100);
    }

    fn leaked_total() -> &'static AtomicUsize {
        Box::leak(Box::new(AtomicUsize::new(0)))
    }

    #[tokio::test]
    async fn a_full_process_cap_keeps_own_entries_and_turns_misses_into_block_reads() {
        let total = leaked_total();
        let other = ReadCache::with_limits(DEFAULT_CAPACITY, total, 320 * 1024);
        let own = ReadCache::with_limits(DEFAULT_CAPACITY, total, 320 * 1024);
        for i in 0..2u64 {
            let start = i * WINDOW;
            other
                .get(start, WINDOW as usize, start, 16, || async {
                    Ok(data(WINDOW as usize, 1))
                })
                .await
                .unwrap();
        }
        own.get(0, 64 * 1024, 0, 16, || async { Ok(data(64 * 1024, 2)) })
            .await
            .unwrap();
        assert_eq!(total.load(Ordering::Relaxed), 320 * 1024);
        assert!(!own.admits(1 << 20, WINDOW as usize));
        assert!(own.admits(0, 64 * 1024));
        own.get(1 << 20, WINDOW as usize, 1 << 20, 16, || async {
            Ok(data(WINDOW as usize, 3))
        })
        .await
        .unwrap();
        assert_eq!(own.bytes(), 64 * 1024);
        assert_eq!(total.load(Ordering::Relaxed), 320 * 1024);
        drop(other);
        assert_eq!(total.load(Ordering::Relaxed), 64 * 1024);
        assert!(own.admits(1 << 20, WINDOW as usize));
    }

    #[tokio::test]
    async fn round_robin_over_many_pieces_reads_each_window_once() {
        const MIB: u64 = 1 << 20;
        const BLOCK: u64 = 16 * 1024;
        let cache = ReadCache::with_limits(DEFAULT_CAPACITY, leaked_total(), PROCESS_CAPACITY);
        let loaded = Arc::new(AtomicU64::new(0));
        let mut served = 0u64;
        for b in 0..(MIB / BLOCK) {
            for p in 0..16u64 {
                let (start, len) = extent_for(p * MIB, MIB, b * BLOCK, BLOCK).unwrap();
                assert!(cache.admits(start, len));
                let l = loaded.clone();
                let got = cache
                    .get(
                        start,
                        len,
                        p * MIB + b * BLOCK,
                        BLOCK as usize,
                        move || async move {
                            l.fetch_add(len as u64, Ordering::Relaxed);
                            Ok(data(len, p as u8))
                        },
                    )
                    .await
                    .unwrap();
                served += got.len() as u64;
            }
        }
        assert_eq!(served, 16 * MIB);
        assert_eq!(loaded.load(Ordering::Relaxed), served);
    }

    #[tokio::test(start_paused = true)]
    async fn idle_entries_are_trimmed() {
        let cache = ReadCache::default();
        let piece = data(256 * 1024, 5);
        let p = piece.clone();
        cache
            .get(0, piece.len(), 0, 16384, move || async move { Ok(p) })
            .await
            .unwrap();
        assert_eq!(cache.bytes(), piece.len());
        tokio::time::advance(MAX_IDLE / 2).await;
        cache.trim_idle();
        assert_eq!(cache.bytes(), piece.len());
        tokio::time::advance(MAX_IDLE).await;
        cache.trim_idle();
        assert_eq!(cache.bytes(), 0);
    }

    #[test]
    fn extents_follow_piece_size() {
        assert_eq!(
            extent_for(1000, 262_144, 16_384, 16_384),
            Some((1000, 131_072))
        );
        assert_eq!(
            extent_for(1000, 262_144, 147_456, 16_384),
            Some((132_072, 131_072))
        );
        let big = 4 * 1024 * 1024;
        assert_eq!(
            extent_for(0, big, 300_000, 16_384),
            Some((262_144, 131_072))
        );
        assert_eq!(extent_for(0, big, 262_144 - 8_192, 16_384), None);
        assert_eq!(extent_for(0, 16_384, 0, 16_384), None);
        let odd = 1024 * 1024 + 100_000;
        assert_eq!(
            extent_for(0, odd, 1024 * 1024 + 10, 100),
            Some((1024 * 1024, 100_000))
        );
    }

    #[tokio::test]
    async fn one_load_serves_every_block() {
        let cache = ReadCache::default();
        let piece = data(256 * 1024, 3);
        let loads = Arc::new(AtomicU64::new(0));
        for blk in 0..16usize {
            let p = piece.clone();
            let l = loads.clone();
            let got = cache
                .get(
                    0,
                    piece.len(),
                    (blk * 16384) as u64,
                    16384,
                    move || async move {
                        l.fetch_add(1, Ordering::Relaxed);
                        Ok(p)
                    },
                )
                .await
                .unwrap();
            assert_eq!(&got[..], &piece[blk * 16384..(blk + 1) * 16384]);
        }
        assert_eq!(loads.load(Ordering::Relaxed), 1);
        assert_eq!(cache.loads(), 1);
    }

    #[tokio::test]
    async fn concurrent_misses_share_one_load() {
        let cache = Arc::new(ReadCache::default());
        let piece = data(64 * 1024, 9);
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let gate_rx = Arc::new(tokio::sync::Mutex::new(Some(gate_rx)));
        let mut tasks = Vec::new();
        for blk in 0..4usize {
            let c = cache.clone();
            let p = piece.clone();
            let g = gate_rx.clone();
            tasks.push(tokio::spawn(async move {
                c.get(
                    0,
                    p.len(),
                    (blk * 16384) as u64,
                    16384,
                    move || async move {
                        let rx = g.lock().await.take();
                        if let Some(rx) = rx {
                            let _ = rx.await;
                        }
                        Ok(p)
                    },
                )
                .await
            }));
        }
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        let _ = gate_tx.send(());
        for (blk, t) in tasks.into_iter().enumerate() {
            let got = t.await.unwrap().unwrap();
            assert_eq!(&got[..], &piece[blk * 16384..(blk + 1) * 16384]);
        }
        assert_eq!(cache.loads(), 1);
    }

    #[tokio::test]
    async fn memory_never_exceeds_the_bound() {
        let cap = 64 * 1024;
        let cache = ReadCache::with_capacity(cap);
        for i in 0..20u64 {
            let start = i * 32 * 1024;
            cache
                .get(start, 32 * 1024, start, 1024, || async {
                    Ok(data(32 * 1024, 1))
                })
                .await
                .unwrap();
            assert!(cache.bytes() <= cap);
        }
        assert_eq!(cache.bytes(), cap);
        let got = cache
            .get(10_000_000, cap * 2, 10_000_000, 16, || async {
                Ok(data(cap * 2, 0))
            })
            .await
            .unwrap();
        assert_eq!(got.len(), 16);
        assert!(cache.bytes() <= cap);
    }

    #[tokio::test]
    async fn eviction_is_least_recently_used() {
        let cache = ReadCache::with_capacity(64 * 1024);
        let load = |tag| move || async move { Ok(data(32 * 1024, tag)) };
        cache.get(0, 32768, 0, 16, load(0)).await.unwrap();
        cache.get(32768, 32768, 32768, 16, load(1)).await.unwrap();
        cache.get(0, 32768, 0, 16, load(0)).await.unwrap();
        cache.get(65536, 32768, 65536, 16, load(2)).await.unwrap();
        assert_eq!(cache.loads(), 3);
        cache.get(0, 32768, 0, 16, load(0)).await.unwrap();
        assert_eq!(cache.loads(), 3);
        cache.get(32768, 32768, 32768, 16, load(1)).await.unwrap();
        assert_eq!(cache.loads(), 4);
    }

    #[tokio::test]
    async fn overlapping_write_invalidates_and_errors_are_not_kept() {
        let cache = ReadCache::default();
        let load = || async { Ok(data(1024, 0)) };
        cache.get(0, 1024, 0, 16, load).await.unwrap();
        cache.invalidate_range(5000, 10);
        cache.get(0, 1024, 0, 16, load).await.unwrap();
        assert_eq!(cache.loads(), 1);
        cache.invalidate_range(1000, 10);
        cache.get(0, 1024, 0, 16, load).await.unwrap();
        assert_eq!(cache.loads(), 2);
        cache.invalidate_all();
        assert_eq!(cache.bytes(), 0);
        let err = cache
            .get(0, 1024, 0, 16, || async {
                Err(StorageError::Io(io::Error::other("boom")))
            })
            .await;
        assert!(err.is_err());
        assert_eq!(cache.bytes(), 0);
    }

    #[tokio::test]
    async fn read_straddling_an_invalidation_is_not_inserted() {
        let cache = Arc::new(ReadCache::default());
        let (gate_tx, gate_rx) = oneshot::channel::<()>();
        let c = cache.clone();
        let t = tokio::spawn(async move {
            c.get(0, 1024, 0, 16, move || async move {
                let _ = gate_rx.await;
                Ok(data(1024, 0))
            })
            .await
        });
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        cache.invalidate_range(0, 1);
        let _ = gate_tx.send(());
        t.await.unwrap().unwrap();
        assert_eq!(cache.bytes(), 0);
    }
}
