use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;

const BURST_WINDOW_US: u64 = 2_000_000;

pub struct RateLimiter {
    limit_bps: AtomicU64,
    next_avail_us: AtomicU64,
    epoch: AtomicU64,
    retuned: Notify,
    start: Instant,
}

struct Charge {
    epoch: u64,
    deadline: Option<Instant>,
}

impl fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RateLimiter")
            .field("limit_bps", &self.limit_bps())
            .finish()
    }
}

impl RateLimiter {
    pub fn new(limit_bps: u64) -> Self {
        let now = Instant::now();
        let start = now.checked_sub(Duration::from_secs(1)).unwrap_or(now);
        Self {
            limit_bps: AtomicU64::new(limit_bps),
            next_avail_us: AtomicU64::new(0),
            epoch: AtomicU64::new(0),
            retuned: Notify::new(),
            start,
        }
    }

    pub fn unlimited() -> Self {
        Self::new(0)
    }

    pub fn set_limit(&self, bps: u64) {
        if self.limit_bps.swap(bps, Ordering::AcqRel) != bps {
            self.next_avail_us.store(0, Ordering::Release);
            self.epoch.fetch_add(1, Ordering::AcqRel);
            self.retuned.notify_waiters();
        }
    }

    pub fn limit_bps(&self) -> u64 {
        self.limit_bps.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub fn is_unlimited(&self) -> bool {
        self.limit_bps() == 0
    }

    fn now_us(&self) -> u64 {
        Instant::now()
            .saturating_duration_since(self.start)
            .as_micros() as u64
    }

    fn reserve(&self, bytes: u64, limit: u64) -> u64 {
        let cost_us = (bytes as u128 * 1_000_000).div_ceil(limit as u128) as u64;
        loop {
            let now_us = self.now_us();
            let cur = self.next_avail_us.load(Ordering::Acquire);
            let base = cur.max(now_us.saturating_sub(BURST_WINDOW_US));
            let new_va = base.saturating_add(cost_us);
            if self
                .next_avail_us
                .compare_exchange_weak(cur, new_va, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return new_va.saturating_sub(now_us);
            }
        }
    }

    fn charge(&self, bytes: u64) -> Charge {
        let epoch = self.epoch.load(Ordering::Acquire);
        let limit = self.limit_bps.load(Ordering::Acquire);
        if limit == 0 {
            return Charge {
                epoch,
                deadline: None,
            };
        }
        let wait_us = self.reserve(bytes, limit);
        Charge {
            epoch,
            deadline: (wait_us > 0).then(|| Instant::now() + Duration::from_micros(wait_us)),
        }
    }

    pub async fn acquire(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        'reserve: loop {
            let epoch = self.epoch.load(Ordering::Acquire);
            let limit = self.limit_bps.load(Ordering::Acquire);
            if limit == 0 {
                return;
            }
            let wait_us = self.reserve(bytes as u64, limit);
            if wait_us == 0 {
                return;
            }
            let deadline = Instant::now() + Duration::from_micros(wait_us);
            loop {
                let notified = self.retuned.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.epoch.load(Ordering::Acquire) != epoch {
                    continue 'reserve;
                }
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => return,
                    _ = &mut notified => {}
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Throttle {
    global: Arc<RateLimiter>,
    task: Arc<RateLimiter>,
}

impl Throttle {
    pub fn new(global: Arc<RateLimiter>, task: Arc<RateLimiter>) -> Self {
        Self { global, task }
    }

    pub fn unlimited() -> Self {
        Self::new(
            Arc::new(RateLimiter::unlimited()),
            Arc::new(RateLimiter::unlimited()),
        )
    }

    pub fn global(&self) -> &Arc<RateLimiter> {
        &self.global
    }

    pub fn task(&self) -> &Arc<RateLimiter> {
        &self.task
    }

    pub async fn acquire(&self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let mut g = self.global.charge(bytes as u64);
        let mut t = self.task.charge(bytes as u64);
        loop {
            let Some(deadline) = g.deadline.max(t.deadline) else {
                return;
            };
            let gn = self.global.retuned.notified();
            let tn = self.task.retuned.notified();
            tokio::pin!(gn, tn);
            gn.as_mut().enable();
            tn.as_mut().enable();
            if self.global.epoch.load(Ordering::Acquire) != g.epoch {
                g = self.global.charge(bytes as u64);
                continue;
            }
            if self.task.epoch.load(Ordering::Acquire) != t.epoch {
                t = self.task.charge(bytes as u64);
                continue;
            }
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => return,
                _ = &mut gn => {}
                _ = &mut tn => {}
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct TorrentLimits {
    pub down: Throttle,
    pub up: Throttle,
}

impl TorrentLimits {
    pub fn unlimited() -> Self {
        Self {
            down: Throttle::unlimited(),
            up: Throttle::unlimited(),
        }
    }
}

pub fn tightest(a: u64, b: u64) -> u64 {
    match (a, b) {
        (0, x) | (x, 0) => x,
        (a, b) => a.min(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KIB: usize = 1024;

    #[tokio::test(start_paused = true)]
    async fn paces_to_configured_rate() {
        let l = RateLimiter::new(100 * KIB as u64);
        let before = Instant::now();
        for _ in 0..10 {
            l.acquire(100 * KIB).await;
        }
        let waited = before.elapsed();
        assert!(waited >= Duration::from_millis(8900), "waited {waited:?}");
        assert!(waited <= Duration::from_millis(9200), "waited {waited:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn idle_time_allows_a_short_burst() {
        let l = RateLimiter::new(100 * KIB as u64);
        l.acquire(100 * KIB).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
        let before = Instant::now();
        l.acquire(200 * KIB).await;
        assert!(before.elapsed() < Duration::from_millis(50));
        l.acquire(100 * KIB).await;
        assert!(before.elapsed() >= Duration::from_millis(900));
    }

    #[tokio::test(start_paused = true)]
    async fn oversized_acquire_paces_before_returning() {
        let l = RateLimiter::new(16 * KIB as u64);
        let before = Instant::now();
        l.acquire(64 * KIB).await;
        let waited = before.elapsed();
        assert!(waited >= Duration::from_millis(2900), "waited {waited:?}");
        assert!(waited <= Duration::from_millis(3100), "waited {waited:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn unlimited_adds_no_waiting() {
        let l = RateLimiter::unlimited();
        let before = Instant::now();
        for _ in 0..1000 {
            l.acquire(64 * 1024 * KIB).await;
        }
        assert_eq!(before.elapsed(), Duration::ZERO);
        assert!(l.is_unlimited());

        let t = Throttle::unlimited();
        t.acquire(usize::MAX / 2).await;
        assert_eq!(before.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn retune_to_unlimited_releases_waiters() {
        let l = Arc::new(RateLimiter::new(KIB as u64));
        l.acquire(KIB).await;
        let waiter = {
            let l = l.clone();
            tokio::spawn(async move { l.acquire(600 * KIB).await })
        };
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!waiter.is_finished());
        l.set_limit(0);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("waiter released by retune")
            .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn retune_applies_new_rate() {
        let l = RateLimiter::new(10 * KIB as u64);
        l.acquire(10 * KIB).await;
        l.set_limit(1000 * KIB as u64);
        let before = Instant::now();
        for _ in 0..10 {
            l.acquire(100 * KIB).await;
        }
        assert!(before.elapsed() < Duration::from_millis(100));
        assert_eq!(l.limit_bps(), 1000 * KIB as u64);

        l.set_limit(10 * KIB as u64);
        l.acquire(20 * KIB).await;
        let before = Instant::now();
        l.acquire(10 * KIB).await;
        assert!(before.elapsed() >= Duration::from_millis(900));
    }

    #[tokio::test(start_paused = true)]
    async fn retune_to_a_lower_rate_still_charges_waiters() {
        let l = Arc::new(RateLimiter::new(10 * KIB as u64));
        l.acquire(10 * KIB).await;
        let before = Instant::now();
        let waiter = {
            let l = l.clone();
            tokio::spawn(async move { l.acquire(20 * KIB).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        l.set_limit(5 * KIB as u64);
        waiter.await.unwrap();
        assert!(
            before.elapsed() >= Duration::from_millis(1900),
            "{:?}",
            before.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn waiters_are_served_in_order_with_one_sleep_each() {
        let l = Arc::new(RateLimiter::new(10 * KIB as u64));
        l.acquire(10 * KIB).await;
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let before = Instant::now();
        let mut tasks = Vec::new();
        for i in 0..100usize {
            let l = l.clone();
            let order = order.clone();
            tasks.push(tokio::spawn(async move {
                l.acquire(KIB).await;
                order.lock().push(i);
            }));
            tokio::task::yield_now().await;
        }
        for t in tasks {
            t.await.unwrap();
        }
        let got = order.lock().clone();
        assert_eq!(got, (0..100).collect::<Vec<_>>());
        let waited = before.elapsed();
        assert!(waited >= Duration::from_millis(9900), "waited {waited:?}");
        assert!(waited <= Duration::from_millis(10200), "waited {waited:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn tighter_budget_wins_in_both_orders() {
        for (global, task) in [(1000u64, 100u64), (100, 1000)] {
            let t = Throttle::new(
                Arc::new(RateLimiter::new(global * KIB as u64)),
                Arc::new(RateLimiter::new(task * KIB as u64)),
            );
            t.acquire(100 * KIB).await;
            let before = Instant::now();
            for _ in 0..10 {
                t.acquire(100 * KIB).await;
            }
            let waited = before.elapsed();
            assert!(
                waited >= Duration::from_millis(9900),
                "{global}/{task}: {waited:?}"
            );
            assert!(
                waited <= Duration::from_millis(10200),
                "{global}/{task}: {waited:?}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn shared_global_budget_splits_between_tasks() {
        let global = Arc::new(RateLimiter::new(100 * KIB as u64));
        let a = Throttle::new(global.clone(), Arc::new(RateLimiter::unlimited()));
        let b = Throttle::new(global, Arc::new(RateLimiter::unlimited()));
        a.acquire(100 * KIB).await;
        let before = Instant::now();
        let run = |t: Throttle| async move {
            for _ in 0..5 {
                t.acquire(100 * KIB).await;
            }
        };
        tokio::join!(run(a), run(b));
        let waited = before.elapsed();
        assert!(waited >= Duration::from_millis(9900), "waited {waited:?}");
        assert!(waited <= Duration::from_millis(10200), "waited {waited:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn throttle_waits_overlap_instead_of_adding() {
        let t = Throttle::new(
            Arc::new(RateLimiter::new(10 * KIB as u64)),
            Arc::new(RateLimiter::new(10 * KIB as u64)),
        );
        t.acquire(10 * KIB).await;
        let before = Instant::now();
        t.acquire(10 * KIB).await;
        let waited = before.elapsed();
        assert!(waited >= Duration::from_millis(900), "waited {waited:?}");
        assert!(waited <= Duration::from_millis(1100), "waited {waited:?}");
    }

    #[test]
    fn tightest_picks_smaller_non_zero() {
        assert_eq!(tightest(0, 0), 0);
        assert_eq!(tightest(0, 5), 5);
        assert_eq!(tightest(5, 0), 5);
        assert_eq!(tightest(3, 5), 3);
        assert_eq!(tightest(5, 3), 3);
    }
}
