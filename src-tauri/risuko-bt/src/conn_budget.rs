use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub const DEFAULT_CONNECTION_LIMIT: usize = 400;

pub const FD_EXHAUSTED_REASON: &str = "connect: out of file descriptors";

pub fn is_fd_exhaustion(err: &std::io::Error) -> bool {
    let Some(code) = err.raw_os_error() else {
        return false;
    };
    #[cfg(unix)]
    {
        code == libc::EMFILE || code == libc::ENFILE
    }
    #[cfg(windows)]
    {
        code == 10024
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = code;
        false
    }
}

fn nofile_soft_limit() -> Option<u64> {
    #[cfg(unix)]
    {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: `lim` is a valid out pointer for the duration of the call
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } != 0 {
            return None;
        }
        #[allow(clippy::unnecessary_cast)]
        Some(lim.rlim_cur as u64)
    }
    #[cfg(not(unix))]
    {
        None
    }
}

pub fn resolve_limit(requested: Option<usize>, nofile_soft: Option<u64>) -> usize {
    let wanted = requested.unwrap_or(DEFAULT_CONNECTION_LIMIT);
    let capped = match nofile_soft {
        Some(soft) => wanted.min(usize::try_from(soft / 2).unwrap_or(usize::MAX)),
        None => wanted,
    };
    capped.max(1)
}

pub fn session_limit(requested: Option<usize>) -> usize {
    resolve_limit(requested, nofile_soft_limit())
}

const METADATA_RESERVE_MIN: usize = 8;
const METADATA_RESERVE_MAX_FRACTION: usize = 4;

pub struct ConnBudget {
    limit: AtomicUsize,
    used: AtomicUsize,
}

impl ConnBudget {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit: AtomicUsize::new(limit.max(1)),
            used: AtomicUsize::new(0),
        })
    }

    pub fn unlimited() -> Arc<Self> {
        Self::new(usize::MAX)
    }

    pub fn limit(&self) -> usize {
        self.limit.load(Ordering::Relaxed)
    }

    pub fn set_limit(&self, limit: usize) {
        self.limit.store(limit.max(1), Ordering::Relaxed);
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    pub fn metadata_reserve(&self) -> usize {
        let limit = self.limit();
        METADATA_RESERVE_MIN
            .max(limit / 50)
            .min(limit / METADATA_RESERVE_MAX_FRACTION)
    }

    fn torrent_cap(&self) -> usize {
        self.limit() - self.metadata_reserve()
    }

    pub fn is_full(&self) -> bool {
        self.used() >= self.torrent_cap()
    }

    pub fn try_acquire(self: &Arc<Self>) -> Option<BudgetGuard> {
        if self.try_add_up_to(self.limit()) {
            Some(BudgetGuard {
                budget: self.clone(),
            })
        } else {
            None
        }
    }

    pub async fn acquire_waiting(
        self: &Arc<Self>,
        poll: Duration,
        give_up: impl Fn() -> bool,
    ) -> Option<BudgetGuard> {
        loop {
            if give_up() {
                return None;
            }
            if let Some(guard) = self.try_acquire() {
                return Some(guard);
            }
            tokio::time::sleep(poll).await;
        }
    }

    fn try_add(&self) -> bool {
        self.try_add_up_to(self.torrent_cap())
    }

    fn try_add_up_to(&self, cap: usize) -> bool {
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            if current >= cap {
                return false;
            }
            match self.used.compare_exchange_weak(
                current,
                current + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }
}

pub struct BudgetGuard {
    budget: Arc<ConnBudget>,
}

impl Drop for BudgetGuard {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct BudgetLease {
    budget: Arc<ConnBudget>,
    held: usize,
}

impl BudgetLease {
    pub fn new(budget: Arc<ConnBudget>) -> Self {
        Self { budget, held: 0 }
    }

    pub fn is_full(&self) -> bool {
        self.budget.is_full()
    }

    pub fn try_reserve(&mut self) -> bool {
        if self.budget.try_add() {
            self.held += 1;
            true
        } else {
            false
        }
    }

    pub fn sync(&mut self, current: usize) {
        if current > self.held {
            self.budget
                .used
                .fetch_add(current - self.held, Ordering::Relaxed);
        } else if current < self.held {
            self.budget
                .used
                .fetch_sub(self.held - current, Ordering::Relaxed);
        }
        self.held = current;
    }
}

impl Drop for BudgetLease {
    fn drop(&mut self) {
        self.sync(0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_defaults_and_respects_half_of_nofile() {
        assert_eq!(resolve_limit(None, None), DEFAULT_CONNECTION_LIMIT);
        assert_eq!(resolve_limit(Some(50), Some(10_000)), 50);
        assert_eq!(resolve_limit(None, Some(256)), 128);
        assert_eq!(resolve_limit(Some(1000), Some(1024)), 512);
        assert_eq!(resolve_limit(Some(0), None), 1);
    }

    #[test]
    fn leases_share_one_budget_and_release_on_drop() {
        let budget = ConnBudget::new(3);
        let mut a = BudgetLease::new(budget.clone());
        let mut b = BudgetLease::new(budget.clone());
        assert!(a.try_reserve());
        assert!(a.try_reserve());
        assert!(b.try_reserve());
        assert!(!b.try_reserve());
        assert!(budget.is_full());
        a.sync(1);
        assert_eq!(budget.used(), 2);
        assert!(b.try_reserve());
        drop(a);
        drop(b);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn lowering_the_limit_keeps_connections_and_blocks_new_ones() {
        let budget = ConnBudget::new(3);
        let mut lease = BudgetLease::new(budget.clone());
        for _ in 0..3 {
            assert!(lease.try_reserve());
        }
        budget.set_limit(2);
        assert_eq!(budget.limit(), 2);
        assert_eq!(budget.used(), 3);
        assert!(budget.is_full());
        assert!(!lease.try_reserve());
        lease.sync(1);
        assert!(lease.try_reserve());
        assert!(!lease.try_reserve());
        budget.set_limit(0);
        assert_eq!(budget.limit(), 1);
        budget.set_limit(10);
        assert!(lease.try_reserve());
    }

    #[test]
    fn sync_can_exceed_the_cap_for_connections_already_made() {
        let budget = ConnBudget::new(2);
        let mut lease = BudgetLease::new(budget.clone());
        lease.sync(5);
        assert_eq!(budget.used(), 5);
        assert!(!lease.try_reserve());
        lease.sync(0);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn reserve_scales_with_the_cap_and_never_starves_small_ones() {
        assert_eq!(ConnBudget::new(400).metadata_reserve(), 8);
        assert_eq!(ConnBudget::new(1000).metadata_reserve(), 20);
        assert_eq!(ConnBudget::new(16).metadata_reserve(), 4);
        assert_eq!(ConnBudget::new(3).metadata_reserve(), 0);
    }

    #[test]
    fn torrents_stop_at_the_reserve_but_metadata_can_use_it() {
        let budget = ConnBudget::new(40);
        assert_eq!(budget.metadata_reserve(), 8);
        let mut lease = BudgetLease::new(budget.clone());
        for _ in 0..32 {
            assert!(lease.try_reserve());
        }
        assert!(!lease.try_reserve());
        assert!(budget.is_full());
        let guards: Vec<_> = (0..8).map(|_| budget.try_acquire().unwrap()).collect();
        assert!(budget.try_acquire().is_none());
        assert_eq!(budget.used(), 40);
        drop(guards);
        assert_eq!(budget.used(), 32);
        assert!(!lease.try_reserve());
    }

    #[test]
    fn metadata_guards_count_against_torrents() {
        let budget = ConnBudget::new(40);
        let _guards: Vec<_> = (0..10).map(|_| budget.try_acquire().unwrap()).collect();
        let mut lease = BudgetLease::new(budget.clone());
        for _ in 0..22 {
            assert!(lease.try_reserve());
        }
        assert!(!lease.try_reserve());
    }

    #[tokio::test(start_paused = true)]
    async fn acquire_waiting_retries_until_a_slot_frees_or_the_caller_gives_up() {
        let budget = ConnBudget::new(2);
        let a = budget.try_acquire().unwrap();
        let b = budget.try_acquire().unwrap();
        let waiter = {
            let budget = budget.clone();
            tokio::spawn(async move {
                budget
                    .acquire_waiting(Duration::from_millis(100), || false)
                    .await
                    .is_some()
            })
        };
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(!waiter.is_finished());
        drop(a);
        assert!(waiter.await.unwrap());
        assert_eq!(budget.used(), 1);
        drop(b);

        let full = ConnBudget::new(1);
        let _held = full.try_acquire().unwrap();
        assert!(full
            .acquire_waiting(Duration::from_millis(10), || true)
            .await
            .is_none());
    }

    #[test]
    fn fd_exhaustion_is_recognised() {
        #[cfg(unix)]
        {
            assert!(is_fd_exhaustion(&std::io::Error::from_raw_os_error(
                libc::EMFILE
            )));
            assert!(is_fd_exhaustion(&std::io::Error::from_raw_os_error(
                libc::ENFILE
            )));
            assert!(!is_fd_exhaustion(&std::io::Error::from_raw_os_error(
                libc::ECONNREFUSED
            )));
        }
        assert!(!is_fd_exhaustion(&std::io::Error::other("x")));
    }
}
