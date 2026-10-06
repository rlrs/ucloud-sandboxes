//! `pause_tier.ReclaimBudget`: the node's one budget for paused reclaim, a
//! shared rate by virtual scheduling and a background CPU priority for the
//! kernel write. The scheduler (the reclaim tick) keeps at most
//! `concurrency` reclaims in flight.

use std::sync::{Arc, Mutex};

use super::Clock;
use super::plan::{RECLAIM_BYTES_PER_SECOND, RECLAIM_CONCURRENCY};

/// The calling thread's nice value. Linux applies `setpriority(PRIO_PROCESS,
/// 0)` to the calling thread only, which is what lets a reclaim worker run
/// one write at nice 19 while the rest of the daemon keeps its priority.
pub trait Nice: Send + Sync {
    fn get(&self) -> std::io::Result<i32>;
    fn set(&self, nice: i32) -> std::io::Result<()>;
}

/// The kernel's per-thread nice, through raw syscalls.
pub struct ThreadNice;

impl Nice for ThreadNice {
    fn get(&self) -> std::io::Result<i32> {
        // SAFETY: getpriority has no memory arguments. The raw syscall returns
        // 20 - nice (1..=40), never a negative success value.
        let raw = unsafe { libc::syscall(libc::SYS_getpriority, libc::PRIO_PROCESS, 0) };
        if raw < 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(20 - raw as i32)
    }

    fn set(&self, nice: i32) -> std::io::Result<()> {
        // SAFETY: setpriority has no memory arguments.
        if unsafe { libc::syscall(libc::SYS_setpriority, libc::PRIO_PROCESS, 0, nice) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
}

/// `_may_raise_priority`: whether this process may lower a nice value again
/// (CAP_SYS_NICE). Without it a write at nice 19 could never return.
fn may_raise_priority(nice: &dyn Nice) -> bool {
    let Ok(before) = nice.get() else { return false };
    if nice.set(before - 1).is_err() {
        return false;
    }
    let _ = nice.set(before);
    true
}

pub type Sleep = Arc<dyn Fn(f64) + Send + Sync>;

pub struct ReclaimBudget {
    pub concurrency: usize,
    pub bytes_per_second: f64,
    clock: Arc<dyn Clock>,
    sleep: Sleep,
    /// Where the last reservation ends (monotonic seconds).
    next: Mutex<f64>,
    nice: Arc<dyn Nice>,
    restorable: bool,
}

impl ReclaimBudget {
    /// The production budget: RECLAIM_CONCURRENCY at RECLAIM_BYTES_PER_SECOND.
    pub fn node(clock: Arc<dyn Clock>) -> Self {
        let sleep: Sleep = Arc::new(|seconds| std::thread::sleep(std::time::Duration::from_secs_f64(seconds)));
        Self::new(RECLAIM_CONCURRENCY, RECLAIM_BYTES_PER_SECOND as f64, clock, sleep, Arc::new(ThreadNice))
    }

    pub fn new(concurrency: usize, bytes_per_second: f64, clock: Arc<dyn Clock>, sleep: Sleep, nice: Arc<dyn Nice>) -> Self {
        assert!(concurrency >= 1 && bytes_per_second > 0.0, "reclaim budget must be positive");
        let restorable = may_raise_priority(&*nice);
        ReclaimBudget { concurrency, bytes_per_second, clock, sleep, next: Mutex::new(0.0), nice, restorable }
    }

    /// Wait for this window's share of the rate; false once superseded. A
    /// cancelled wait forfeits its reservation: at most one window.
    pub fn admit(&self, amount: u64, is_current: &dyn Fn() -> bool) -> bool {
        let start = {
            let mut next = self.next.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let start = self.clock.monotonic().max(*next);
            *next = start + amount as f64 / self.bytes_per_second;
            start
        };
        loop {
            let remaining = start - self.clock.monotonic();
            if remaining <= 0.0 {
                return true;
            }
            if !is_current() {
                return false;
            }
            (self.sleep)(remaining.min(0.05));
        }
    }

    /// Run one kernel write at the lowest CPU weight (nice 19): memory.reclaim,
    /// and zswap compression, run in the writer's context. Not SCHED_IDLE: a
    /// starved writer can sit on kernel reclaim locks. Without CAP_SYS_NICE
    /// the priority could not be restored, so the write keeps it.
    pub fn background<T>(&self, write: impl FnOnce() -> T) -> T {
        if !self.restorable {
            return write();
        }
        let Ok(before) = self.nice.get() else { return write() };
        let lowered = self.nice.set(19).is_ok();
        let result = write();
        if lowered {
            let _ = self.nice.set(before);
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn set_restorable(&mut self, restorable: bool) {
        self.restorable = restorable;
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::pause_policy::tests::FakeClock;
    use std::time::Instant;

    /// A process nice that may be raised again, recording every set.
    pub(crate) struct FakeNice {
        pub value: Mutex<i32>,
        pub calls: Mutex<Vec<i32>>,
    }

    impl FakeNice {
        pub fn new() -> Arc<FakeNice> {
            Arc::new(FakeNice { value: Mutex::new(0), calls: Mutex::new(Vec::new()) })
        }
    }

    impl Nice for FakeNice {
        fn get(&self) -> std::io::Result<i32> {
            Ok(*self.value.lock().unwrap())
        }
        fn set(&self, nice: i32) -> std::io::Result<()> {
            self.calls.lock().unwrap().push(nice);
            *self.value.lock().unwrap() = nice;
            Ok(())
        }
    }

    fn real(bytes_per_second: f64) -> ReclaimBudget {
        let clock: Arc<dyn Clock> = Arc::new(crate::pause_policy::SystemClock::new());
        let sleep: Sleep = Arc::new(|seconds| std::thread::sleep(std::time::Duration::from_secs_f64(seconds)));
        ReclaimBudget::new(RECLAIM_CONCURRENCY, bytes_per_second, clock, sleep, FakeNice::new())
    }

    #[test]
    fn concurrent_reclaims_share_one_node_rate() {
        let budget = Arc::new(real(3000.0));
        let started = Instant::now();
        let threads: Vec<_> = (0..2)
            .map(|_| {
                let budget = budget.clone();
                std::thread::spawn(move || {
                    for _ in 0..3 {
                        assert!(budget.admit(100, &|| true)); // Three 100-byte windows.
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        // 600 bytes at the one 3000 B/s rate: the sixth window starts at 167 ms.
        assert!(started.elapsed().as_secs_f64() >= 0.16);
    }

    #[test]
    fn budget_reservations_queue_and_a_cancelled_wait_returns_promptly() {
        let clock = FakeClock::at(10.0);
        let advance = clock.clone();
        let sleep: Sleep = Arc::new(move |seconds| advance.advance(seconds));
        let budget = ReclaimBudget::new(2, 100.0, clock.clone(), sleep, FakeNice::new());
        assert!(budget.admit(100, &|| true)); // Idle budget: immediate.
        assert_eq!(clock.monotonic(), 10.0);
        assert!(budget.admit(50, &|| true)); // Starts where the last ends.
        assert!((clock.monotonic() - 11.0).abs() < 1e-9);
        let checks = std::sync::atomic::AtomicUsize::new(0);
        assert!(!budget.admit(100, &|| {
            checks.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            false
        }));
        assert_eq!(checks.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!((clock.monotonic() - 11.0).abs() < 1e-9);
        let zero = std::panic::catch_unwind(|| ReclaimBudget::new(0, 1.0, clock.clone(), Arc::new(|_| {}), FakeNice::new()));
        assert!(zero.is_err());
    }

    #[test]
    fn only_the_kernel_write_runs_at_background_priority() {
        let nice = FakeNice::new();
        let mut budget = ReclaimBudget::new(2, 1e12, FakeClock::new(), Arc::new(|_| {}), nice.clone());
        nice.calls.lock().unwrap().clear(); // The capability probe's -1 and 0.
        let during = budget.background(|| *nice.value.lock().unwrap());
        assert_eq!((during, nice.calls.lock().unwrap().clone(), *nice.value.lock().unwrap()), (19, vec![19, 0], 0));
        budget.set_restorable(false);
        nice.calls.lock().unwrap().clear();
        assert_eq!(budget.background(|| 7), 7);
        assert!(nice.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn the_real_thread_nice_reads_back_what_it_sets() {
        // Raising nice needs no capability; the thread exits with it.
        std::thread::spawn(|| {
            let before = ThreadNice.get().unwrap();
            if before < 19 {
                ThreadNice.set(before + 1).unwrap();
                assert_eq!(ThreadNice.get().unwrap(), before + 1);
            }
        })
        .join()
        .unwrap();
    }
}
