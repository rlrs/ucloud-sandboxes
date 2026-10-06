//! The pre-created netns+veth pool, as direct_network.py's: a background
//! thread keeps `size` slots in the durable `state["pool"]`, each with a
//! configured pair in namespace `ucloud-pool-<slot>`, and a create takes the
//! lowest ready one. A slot is in `leases` or `pool`, never both, and moves
//! in the write that records its lease, so a pair has one owner. A pooled
//! name a crash leaves behind belongs to the slot's lease: its next ensure
//! attaches it, or release drops it.
//!
//! One process owns the pool (the `network-slots.json.lock.pool` flock), and
//! hands out only slots it configured while owning it; any other process
//! treats pooled slots as used. Under `--rust-creates` that is this daemon:
//! the agent leaves `state["pool"]` alone. On taking ownership, every durable
//! pool slot, whoever configured it, is rechecked before it is handed out,
//! and stray `ucloud-pool-<slot>` names of unowned slots join the pool to be
//! rechecked or trimmed.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};

use super::{MAX_SLOTS, NetworkError, NetworkManager, pool_slots};
use crate::fsutil::FileLock;

/// One burst of the 32 concurrent creates the create gate measures.
pub const DEFAULT_POOL_SIZE: usize = 32;
pub const MAX_POOL_SIZE: usize = 1024;
/// The refill defers to in-flight ensures for at most this long per pair, so
/// a sustained create stream still refills slowly instead of never.
const YIELD: Duration = Duration::from_secs(1);
const RETRY: Duration = Duration::from_secs(5);

#[derive(Default)]
struct Signal {
    woken: bool,
    stopping: bool,
}

pub(super) struct Pool {
    size: usize,
    /// Slots this process configured while owning the pool (Python `_pool_ready`).
    ready: Mutex<BTreeSet<u32>>,
    /// Ensures in flight; the refill defers to them.
    foreground: AtomicUsize,
    signal: Mutex<Signal>,
    changed: Condvar,
    pub(super) thread: Mutex<Option<JoinHandle<()>>>,
    pub(super) retry: Duration,
}

/// Counts one ensure as foreground work while alive.
pub(super) struct Foreground<'a>(&'a AtomicUsize);

impl Drop for Foreground<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Pool {
    pub(super) fn new(size: usize) -> Pool {
        Pool {
            size,
            ready: Mutex::new(BTreeSet::new()),
            foreground: AtomicUsize::new(0),
            signal: Mutex::new(Signal::default()),
            changed: Condvar::new(),
            thread: Mutex::new(None),
            retry: RETRY,
        }
    }

    pub(super) fn foreground(&self) -> Foreground<'_> {
        self.foreground.fetch_add(1, Ordering::SeqCst);
        Foreground(&self.foreground)
    }

    /// Python `_claim_pooled`: move the lowest ready slot out of
    /// `state["pool"]`. Called under the global state flock.
    pub(super) fn claim(&self, state: &mut Map<String, Value>) -> Option<u32> {
        let mut pool = pool_slots(state);
        let slot = {
            let mut ready = self.ready.lock().expect("not poisoned");
            // A ready slot no longer pooled was leased by an older release.
            ready.retain(|slot| pool.contains(&u64::from(*slot)));
            let slot = ready.first().copied()?;
            ready.remove(&slot);
            slot
        };
        pool.retain(|item| *item != u64::from(slot));
        state.insert("pool".into(), json!(pool));
        self.wake();
        Some(slot)
    }

    fn wake(&self) {
        self.signal.lock().expect("not poisoned").woken = true;
        self.changed.notify_all();
    }

    /// Wait for a wake-up (consumed) or `timeout`; whether the pool is stopping.
    fn wait(&self, timeout: Option<Duration>) -> bool {
        let deadline = timeout.map(|timeout| Instant::now() + timeout);
        let mut signal = self.signal.lock().expect("not poisoned");
        while !signal.woken && !signal.stopping {
            signal = match deadline {
                None => self.changed.wait(signal).expect("not poisoned"),
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        break;
                    }
                    self.changed.wait_timeout(signal, left).expect("not poisoned").0
                }
            };
        }
        signal.woken = false;
        signal.stopping
    }

    /// Sleep up to `timeout` unless stopping; whether the pool is stopping.
    fn pause(&self, timeout: Duration) -> bool {
        let signal = self.signal.lock().expect("not poisoned");
        if signal.stopping {
            return true;
        }
        self.changed.wait_timeout(signal, timeout).expect("not poisoned").0.stopping
    }

    fn stopping(&self) -> bool {
        self.signal.lock().expect("not poisoned").stopping
    }
}

enum Work {
    Fill(u32),
    Trim(u32),
}

impl NetworkManager {
    /// Start the low-priority refill (or the trim of a pool that should be
    /// empty). Call after startup recovery, so it never competes with it.
    pub fn start_pool(self: &Arc<Self>) {
        let mut thread = self.pool.thread.lock().expect("not poisoned");
        if thread.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return;
        }
        self.pool.signal.lock().expect("not poisoned").stopping = false;
        let manager = self.clone();
        let spawned = std::thread::Builder::new().name("noded-network-pool".into()).spawn(move || manager.pool_loop());
        match spawned {
            Ok(handle) => *thread = Some(handle),
            Err(error) => eprintln!("ucloud-noded: cannot start the direct network pool: {error}"),
        }
    }

    /// Stop refilling and wait for the thread. Pooled slots stay durable and
    /// are rechecked by the next owner.
    pub fn stop_pool(&self) {
        self.pool.signal.lock().expect("not poisoned").stopping = true;
        self.pool.changed.notify_all();
        let handle = self.pool.thread.lock().expect("not poisoned").take();
        if let Some(handle) = handle {
            let _ = handle.join();
        }
    }

    /// The slots ready to hand out, lowest first.
    pub fn pool_ready(&self) -> Vec<u32> {
        self.pool.ready.lock().expect("not poisoned").iter().copied().collect()
    }

    fn owner_lock_path(&self) -> PathBuf {
        PathBuf::from(format!("{}.pool", self.lock_path.display()))
    }

    fn pool_loop(self: Arc<Self>) {
        let owner = self.owner_lock_path();
        let mut waiting = false;
        let _owner = loop {
            let attempt = owner.parent().map_or(Ok(()), |parent| {
                use std::os::unix::fs::DirBuilderExt;
                std::fs::DirBuilder::new().recursive(true).mode(0o700).create(parent)
            });
            match attempt.and_then(|()| FileLock::try_acquire(&owner, false)) {
                Ok(Some(lock)) => break lock,
                // An agent from before --rust-creates handed the pool over
                // may still be trimming its own; take over once it is done.
                Ok(None) if !waiting => {
                    eprintln!("ucloud-noded: another process owns the direct network pool; waiting for it");
                    waiting = true;
                }
                Ok(None) => {}
                Err(error) => eprintln!("ucloud-noded: cannot lock {}: {error}", owner.display()),
            }
            if self.pool.pause(self.pool.retry) {
                return;
            }
        };
        if let Err(error) = self.adopt_stray_pool_namespaces() {
            eprintln!("ucloud-noded: cannot reconcile stray pool namespaces: {error}");
        }
        while !self.pool.stopping() {
            match self.refill_one() {
                Ok(true) => {}
                Ok(false) if self.pool.size == 0 => break,
                Ok(false) => {
                    if self.pool.wait(None) {
                        break;
                    }
                }
                Err(error) => {
                    eprintln!("ucloud-noded: direct network pool refill failed; retrying: {error}");
                    if self.pool.wait(Some(self.pool.retry)) {
                        break;
                    }
                }
            }
        }
        // Hand-outs end before another owner starts.
        self.pool.ready.lock().expect("not poisoned").clear();
    }

    /// `ucloud-pool-<slot>` names whose slot is neither leased nor pooled
    /// (a state file that lost a pool-only write, or an agent's pool that
    /// was not trimmed) join the pool, which rechecks or trims them. A name
    /// whose slot is leased belongs to that lease.
    fn adopt_stray_pool_namespaces(&self) -> Result<(), NetworkError> {
        let entries = match std::fs::read_dir(&self.namespace_root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let named: BTreeSet<u64> = entries
            .filter_map(|entry| entry.ok()?.file_name().to_str()?.strip_prefix("ucloud-pool-")?.parse::<u64>().ok())
            .filter(|slot| (1..=u64::from(MAX_SLOTS)).contains(slot))
            .collect();
        if named.is_empty() {
            return Ok(());
        }
        let _state_lock = Self::locked(&self.lock_path)?;
        let mut state = self.load()?;
        let mut owned: BTreeSet<u64> = pool_slots(&state).into_iter().collect();
        let pool = owned.clone();
        owned.extend(state["leases"].as_object().into_iter().flat_map(|leases| leases.values().filter_map(Value::as_u64)));
        let stray: Vec<u64> = named.difference(&owned).copied().collect();
        if stray.is_empty() {
            return Ok(());
        }
        eprintln!("ucloud-noded: pooling stray network namespaces for slots {stray:?}");
        let pool: BTreeSet<u64> = pool.into_iter().chain(stray).collect();
        state.insert("pool".into(), json!(pool));
        self.store(&state, true)
    }

    /// Python `_refill_one`: fill, recheck or trim one pooled slot; false
    /// once the pool is settled.
    fn refill_one(&self) -> Result<bool, NetworkError> {
        let deadline = Instant::now() + YIELD;
        while self.pool.foreground.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
            if self.pool.pause(Duration::from_millis(10)) {
                return Ok(false);
            }
        }
        let work = {
            let _state_lock = Self::locked(&self.lock_path)?;
            let mut state = self.load()?;
            let pool: Vec<u32> = pool_slots(&state).into_iter().map(|slot| slot as u32).collect();
            let mut ready = self.pool.ready.lock().expect("not poisoned");
            let pending: Vec<u32> = pool.iter().copied().filter(|slot| !ready.contains(slot)).collect();
            if pool.len() > self.pool.size {
                // Under the state lock, so no hand-out holds it.
                let slot = *pending.last().or(pool.last()).expect("a pool larger than its size is not empty");
                ready.remove(&slot);
                Work::Trim(slot)
            } else if let Some(slot) = pending.first() {
                Work::Fill(*slot)
            } else {
                drop(ready);
                if pool.len() == self.pool.size {
                    return Ok(false);
                }
                let mut used: BTreeSet<u64> = pool.iter().map(|slot| u64::from(*slot)).collect();
                used.extend(state["leases"].as_object().into_iter().flat_map(|leases| leases.values().filter_map(Value::as_u64)));
                let Some(slot) = (1..=MAX_SLOTS).find(|slot| !used.contains(&u64::from(*slot))) else {
                    return Ok(false);
                };
                // Written before any kernel object exists, so none is orphaned.
                let mut grown = pool.clone();
                grown.push(slot);
                grown.sort_unstable();
                state.insert("pool".into(), json!(grown));
                self.store(&state, true)?;
                Work::Fill(slot)
            }
        };
        match work {
            Work::Fill(slot) => {
                // Recreated unless complete; configuration is idempotent.
                self.ensure_kernel_lease(&self.pool_lease(slot))?;
                self.pool.ready.lock().expect("not poisoned").insert(slot);
            }
            Work::Trim(slot) => {
                self.cleanup_kernel_lease(&self.pool_lease(slot));
                let _state_lock = Self::locked(&self.lock_path)?;
                let mut state = self.load()?;
                let pool: Vec<u64> = pool_slots(&state).into_iter().filter(|item| *item != u64::from(slot)).collect();
                state.insert("pool".into(), json!(pool));
                self.store(&state, true)?;
            }
        }
        Ok(true)
    }
}
