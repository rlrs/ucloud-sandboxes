//! The pause tier's mechanism (C1.1; ucloud_sandboxes/direct_warden.py
//! `pause`/`_pause_locked`/`thaw`/`_thaw_locked`/`_prefetch_memory` and
//! ucloud_sandboxes/pause_tier.py): pause a LIVE runtime in place with
//! `runsc pause`, and thaw it with an optional read-back of its swapped
//! application memory before `runsc resume`.
//!
//! A pause changes no ownership: the journal stays RUNNING/LIVE, the route
//! stays running. The cgroup is never frozen (`runsc pause` leaves
//! `cgroup.frozen 0`; a frozen cgroup would be a second state for every crash
//! window to undo). Pause markers are the truth (`marker`); everything else
//! here is disposable.
//!
//! Locks, in the order every path takes them (phase-3 spec §6.2): the caller's
//! T and A exec-fence locks, then the per-incarnation warden flock
//! (`Warden::lock`, shared with the Python agent), then the marker flock a
//! thaw holds. `pause` and `thaw` take the warden flock themselves; the
//! `_locked` variants are for a caller that already holds it (a keep-paused
//! read: thaw without prefetch, act, re-pause, under one hold).
//!
//! Drive `pause` and `thaw` to completion. A dropped future leaves the states
//! a crash leaves (a marker on a running or paused runtime), which the
//! protocol tolerates and the next thaw repairs.

mod cgroup;
pub mod marker;
pub mod prefetch;
pub mod stats;
#[cfg(test)]
pub(crate) mod tests;

use std::collections::HashMap;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

pub use cgroup::{ZSWAP_SHARE_OF_BOUND, cap_zswap, cgroup_swap_bytes};
pub use prefetch::{PREFETCH_MIN_SWAP_BYTES, PREFETCH_NODE_THREADS, PREFETCH_SECONDS, Slots, memory_pieces};
pub use stats::{Counter, PauseStats};

use crate::fsutil::{FileLock, atomic_write, ensure_private_dir, euid};
use crate::journal::is_safe_id;
use crate::memory_backing::{ActiveMode, MemoryBackingStore};
use crate::runsc::{self, SentryOwner};
use crate::warden::{MemoryMode, Sandbox, Warden, WardenConfig, WardenError};
use marker::ThawHold;
use prefetch::ReadAt;

/// The active application memory file in a sandbox's memory directory.
pub const ACTIVE_APPLICATION_MEMORY: &str = "application_memory.active";

fn fail(message: impl Into<String>) -> WardenError {
    WardenError::Warden(message.into())
}

async fn blocking<T: Send + 'static>(work: impl FnOnce() -> Result<T, WardenError> + Send + 'static) -> Result<T, WardenError> {
    tokio::task::spawn_blocking(work).await.map_err(|error| fail(format!("pause worker failed: {error}")))?
}

/// Where an incarnation's application memory lives (Python
/// `application_memory_mode`'s first source). `None`: not known to this
/// process; the tier then assumes RAM when a RAM root is configured.
pub trait ModeSource: Send + Sync {
    fn application_memory_mode(&self, sandbox_id: &str, generation: u64) -> Option<MemoryMode>;
}

impl ModeSource for MemoryBackingStore {
    fn application_memory_mode(&self, sandbox_id: &str, generation: u64) -> Option<MemoryMode> {
        self.active_mode(sandbox_id, generation).map(|mode| match mode {
            ActiveMode::Ram => MemoryMode::Ram,
            ActiveMode::File => MemoryMode::File,
        })
    }
}

#[derive(Clone, Debug)]
pub struct PauseConfig {
    /// The node's Warden configuration (runsc, roots, proc root, timeout).
    pub warden: WardenConfig,
    /// `sandbox.direct_pause_tier`; `pause` refuses without it.
    pub enabled: bool,
    pub cgroup_root: PathBuf,
    /// `/sys/module/zswap/parameters/enabled`.
    pub zswap_enabled: PathBuf,
    /// A thaw's prefetch time budget.
    pub prefetch_seconds: Duration,
}

impl PauseConfig {
    pub fn new(warden: WardenConfig, enabled: bool) -> Self {
        PauseConfig {
            warden,
            enabled,
            cgroup_root: PathBuf::from("/sys/fs/cgroup"),
            zswap_enabled: PathBuf::from("/sys/module/zswap/parameters/enabled"),
            prefetch_seconds: PREFETCH_SECONDS,
        }
    }
}

/// The node's one pause tier. Cheap to clone; every clone shares the stats,
/// the reader slots and the in-flight prefetches.
#[derive(Clone)]
pub struct PauseTier {
    inner: Arc<Inner>,
}

struct Inner {
    config: PauseConfig,
    warden: Warden,
    modes: Option<Arc<dyn ModeSource>>,
    stats: PauseStats,
    slots: Slots,
    /// Cancel flags of this process's running prefetches (a delete's cancel).
    prefetches: Mutex<HashMap<(String, u64), Arc<AtomicBool>>>,
    read_at: Arc<ReadAt>,
}

/// The journal fields a pause needs: (sentry pid, its start ticks).
fn sentry(record: &Map<String, Value>) -> (Option<u32>, Option<u64>) {
    let pid = record.get("sentry_pid").and_then(Value::as_u64).and_then(|pid| u32::try_from(pid).ok());
    (pid, record.get("sentry_start_time_ticks").and_then(Value::as_u64))
}

fn is_running(record: &Map<String, Value>) -> bool {
    record.get("state").and_then(Value::as_str) == Some("running")
}

impl PauseTier {
    /// `modes`: the memory-backing store when this node has one.
    pub fn new(config: PauseConfig, modes: Option<Arc<dyn ModeSource>>) -> Self {
        Self::with_reader(config, modes, Arc::new(prefetch::pread))
    }

    pub(crate) fn with_reader(config: PauseConfig, modes: Option<Arc<dyn ModeSource>>, read_at: Arc<ReadAt>) -> Self {
        let warden = Warden::new(config.warden.clone());
        PauseTier {
            inner: Arc::new(Inner {
                config,
                warden,
                modes,
                stats: PauseStats::new(),
                slots: Slots::new(PREFETCH_NODE_THREADS),
                prefetches: Mutex::new(HashMap::new()),
                read_at,
            }),
        }
    }

    pub fn config(&self) -> &PauseConfig {
        &self.inner.config
    }

    /// `pause_stats`: pauses and thaws counted here; reclaim adds its own.
    pub fn stats(&self) -> &PauseStats {
        &self.inner.stats
    }

    /// The marker path, or `None` for an id that cannot name one.
    pub fn marker_path(&self, sandbox_id: &str, generation: u64) -> Option<PathBuf> {
        is_safe_id(sandbox_id).then(|| marker::marker_path(&self.inner.config.warden.runtime_root, sandbox_id, generation))
    }

    fn marker(&self, sandbox: &Sandbox) -> Result<PathBuf, WardenError> {
        self.marker_path(&sandbox.sandbox_id, sandbox.generation).ok_or_else(|| fail("sandbox id cannot name a pause marker"))
    }

    /// Python `is_paused`: the marker exists. Observation only.
    pub fn is_paused(&self, sandbox_id: &str, generation: u64) -> bool {
        self.marker_path(sandbox_id, generation).is_some_and(|path| marker::exists(&path))
    }

    /// Python `thawing`: a thaw in any process is working on this pause (from
    /// before its first prefetch read until after the marker's unlink).
    pub fn thawing(&self, sandbox_id: &str, generation: u64) -> bool {
        self.marker_path(sandbox_id, generation).is_some_and(|path| marker::thawing(&path))
    }

    /// `is_paused and not thawing`, as one probe that cannot mistake a just
    /// finished thaw for a settled pause. Paused reclaim checks it before every
    /// window.
    pub fn settled(&self, sandbox_id: &str, generation: u64) -> bool {
        self.marker_path(sandbox_id, generation).is_some_and(|path| marker::settled(&path))
    }

    /// Python `paused_keys`: every marker's `(sandbox_id, generation)`.
    pub fn paused_keys(&self) -> std::io::Result<Vec<(String, u64)>> {
        marker::paused_keys(&self.inner.config.warden.runtime_root)
    }

    /// The per-incarnation warden flock (shared with the Python agent).
    pub async fn lock(&self, sandbox: &Sandbox) -> Result<FileLock, WardenError> {
        self.inner.warden.lock(sandbox).await
    }

    /// The incarnation's journal record. Call it under `lock`.
    pub async fn journal_record(&self, sandbox: &Sandbox) -> Result<Option<Map<String, Value>>, WardenError> {
        let (inner, id, generation) = (self.inner.clone(), sandbox.sandbox_id.clone(), sandbox.generation);
        blocking(move || Ok(inner.warden.journal().load(&id, generation)?)).await
    }

    /// Stop a delete's wait for this incarnation's prefetch; whether one ran.
    pub fn cancel_prefetch(&self, sandbox: &Sandbox) -> bool {
        let prefetches = self.inner.prefetches.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        prefetches.get(&(sandbox.sandbox_id.clone(), sandbox.generation)).map(|cancel| cancel.store(true, Ordering::SeqCst)).is_some()
    }

    fn argv(&self, verb: &str, sandbox: &Sandbox) -> Vec<String> {
        let config = &self.inner.config.warden;
        vec![
            config.runsc.display().to_string(),
            format!("--root={}", config.runtime_root.display()),
            verb.to_string(),
            sandbox.container_id.clone(),
        ]
    }

    /// Python `pause`: pause a LIVE runtime in place. `Ok(false)` when its
    /// journal is not RUNNING (nothing changed). The caller holds T and A
    /// exclusively (spec §6.2 item 1).
    pub async fn pause(&self, sandbox: &Sandbox) -> Result<bool, WardenError> {
        if !self.inner.config.enabled {
            return Err(fail("the pause tier is disabled on this node"));
        }
        let lock = self.lock(sandbox).await?;
        match self.journal_record(sandbox).await? {
            Some(record) if is_running(&record) => {
                self.pause_locked(sandbox, &lock, &record).await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Python `_pause_locked`, under the warden flock `_lock`, for the RUNNING
    /// `record`: marker, zswap cap, `runsc pause`. A failure leaves the runtime
    /// running and the marker gone, unless `runsc state` proves that a crashed
    /// predecessor already paused this very sentry.
    pub async fn pause_locked(&self, sandbox: &Sandbox, lock: &FileLock, record: &Map<String, Value>) -> Result<(), WardenError> {
        let path = self.marker(sandbox)?;
        let (pid, ticks) = sentry(record);
        let (inner, container_id) = (self.inner.clone(), sandbox.container_id.clone());
        blocking(move || {
            let directory = path.parent().expect("a marker has a directory");
            ensure_private_dir(directory)?;
            atomic_write(&path, container_id.as_bytes())?;
            // Before any reclaim: zswap may hold only a bounded share.
            if let Some(pid) = pid {
                let config = &inner.config;
                cap_zswap(pid, &config.warden.proc_root, &config.cgroup_root, &config.zswap_enabled);
            }
            Ok(())
        })
        .await?;
        let result = runsc::run(&self.argv("pause", sandbox), self.inner.config.warden.command_timeout).await?;
        if result.returncode != 0 {
            let (state_pid, state_ticks, status) = self.state_identity_status(sandbox).await?;
            if (Some(state_pid), Some(state_ticks), status.as_str()) != (pid, ticks, "paused") {
                self.thaw_locked(sandbox, lock, false).await?;
                return Err(fail(format!("runsc pause failed: {}", result.stderr)));
            }
        }
        self.inner.stats.add(&[(Counter::Pauses, 1.0)]);
        Ok(())
    }

    /// Python `thaw`: resume a paused runtime; the time taken, or `None` when
    /// it was not paused (one lstat, no lock). The caller holds A shared
    /// (spec §6.2 item 2).
    pub async fn thaw(&self, sandbox: &Sandbox, prefetch: bool) -> Result<Option<Duration>, WardenError> {
        if !self.is_paused(&sandbox.sandbox_id, sandbox.generation) {
            return Ok(None);
        }
        let lock = self.lock(sandbox).await?;
        self.thaw_locked(sandbox, &lock, prefetch).await
    }

    /// Python `_thaw_locked`, under the warden flock: hold the marker's flock,
    /// optionally prefetch, `runsc resume`, unlink the marker, release. A crash
    /// anywhere before the unlink leaves a paused runtime with its marker, or
    /// a running one with a harmless marker; prefetch only reads.
    pub async fn thaw_locked(&self, sandbox: &Sandbox, _lock: &FileLock, prefetch: bool) -> Result<Option<Duration>, WardenError> {
        let path = self.marker(sandbox)?;
        let hold = match ThawHold::try_acquire(&path)? {
            Ok(None) => return Ok(None),
            Ok(Some(hold)) => hold,
            Err(()) => {
                // A reclaim probe holds it shared for microseconds.
                let busy = path.clone();
                match blocking(move || Ok(ThawHold::acquire(&busy)?)).await? {
                    Some(hold) => hold,
                    None => return Ok(None),
                }
            }
        };
        let started = Instant::now();
        if prefetch {
            self.prefetch_memory(sandbox, &hold).await?;
        }
        let result = runsc::run(&self.argv("resume", sandbox), self.inner.config.warden.command_timeout).await?;
        if result.returncode != 0 && self.state_identity_status(sandbox).await?.2 != "running" {
            return Err(fail(format!("runsc resume of a paused sandbox failed: {}", result.stderr)));
        }
        match std::fs::remove_file(&path) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
            _ => {}
        }
        drop(hold); // After the unlink: its descriptor names an orphan inode now.
        let elapsed = started.elapsed();
        let ms = elapsed.as_secs_f64() * 1000.0;
        self.inner.stats.add(&[(Counter::Thaws, 1.0), (Counter::ThawMsTotal, ms), (Counter::ThawMsMax, ms)]);
        Ok(Some(elapsed))
    }

    /// Python `_state_identity_status`: `runsc state` of a live runtime and its
    /// verified sentry: (pid, start ticks, "running" | "paused").
    async fn state_identity_status(&self, sandbox: &Sandbox) -> Result<(u32, u64, String), WardenError> {
        let result = runsc::checked(&self.argv("state", sandbox), self.inner.config.warden.command_timeout).await?;
        let (pid, status) = runsc::parse_state(&result.stdout)?;
        if status != "running" && status != "paused" {
            return Err(fail(format!("runsc state is not live: {status}")));
        }
        let (inner, sandbox) = (self.inner.clone(), sandbox.clone());
        let ticks = blocking(move || {
            let config = &inner.config.warden;
            let owner = SentryOwner {
                proc_root: &config.proc_root,
                runsc: &config.runsc,
                runtime_root: &config.runtime_root,
                bundle: &sandbox.bundle,
                container_id: &sandbox.container_id,
            };
            runsc::sentry_identity(&owner, pid, None).map_err(|_| fail("cannot read sentry process identity"))
        })
        .await?;
        Ok((pid, ticks, status))
    }

    /// Python `_prefetch_memory`. Its readers keep a duplicate of the marker
    /// hold, so "thawing" stays visible until the last of them exits, even if
    /// this future is dropped. Only a journal error fails the thaw; the
    /// rest is best effort and the guest faults in whatever is left.
    async fn prefetch_memory(&self, sandbox: &Sandbox, hold: &ThawHold) -> Result<(), WardenError> {
        let shared = match hold.share() {
            Ok(shared) => shared,
            Err(error) => {
                eprintln!("ucloud-noded: thaw prefetch of {} skipped: {error}", sandbox.sandbox_id);
                return Ok(());
            }
        };
        let (inner, sandbox) = (self.inner.clone(), sandbox.clone());
        blocking(move || {
            let _hold = shared;
            let record = inner.warden.journal().load(&sandbox.sandbox_id, sandbox.generation)?;
            if let Some(record) = record {
                inner.prefetch_memory(&sandbox, &record);
            }
            Ok(())
        })
        .await
    }
}

impl Inner {
    /// Python `application_memory_mode`.
    fn mode(&self, sandbox: &Sandbox) -> MemoryMode {
        let known = self.modes.as_ref().and_then(|modes| modes.application_memory_mode(&sandbox.sandbox_id, sandbox.generation));
        known.unwrap_or(if self.config.warden.application_memory_root.is_some() { MemoryMode::Ram } else { MemoryMode::File })
    }

    /// Python `_active_memory_root(...) / _ACTIVE_APPLICATION_MEMORY`.
    fn active_memory_file(&self, sandbox: &Sandbox) -> Result<PathBuf, WardenError> {
        let root = match self.mode(sandbox) {
            MemoryMode::Ram => self.config.warden.application_memory_root.clone()
                .ok_or_else(|| fail("RAM memory placement has no configured backing root"))?,
            MemoryMode::File => self.config.warden.memory_root.clone(),
        };
        let mut components = Path::new(&sandbox.memory_directory).components();
        if !matches!((components.next(), components.next()), (Some(Component::Normal(_)), None)) {
            return Err(fail("active memory directory escaped its root"));
        }
        Ok(root.join(&sandbox.memory_directory).join(ACTIVE_APPLICATION_MEMORY))
    }

    /// The gates, then the read-back. RAM (tmpfs) memory only: a host read
    /// charges page cache to the reader's cgroup, whereas a swapped tmpfs page
    /// returns to the cgroup its swap entry names, the sandbox's. Skipped
    /// unless that cgroup holds PREFETCH_MIN_SWAP_BYTES of swap.
    fn prefetch_memory(&self, sandbox: &Sandbox, record: &Map<String, Value>) {
        let (Some(pid), ticks) = sentry(record) else { return };
        if self.mode(sandbox) != MemoryMode::Ram {
            return;
        }
        // Unlike Python, the journal's sentry must still be this pid (its
        // start ticks bracket the cgroup read), so a reused pid cannot name
        // another cgroup.
        let proc_root = &self.config.warden.proc_root;
        let same_sentry = || ticks.is_some_and(|ticks| runsc::start_time_ticks(proc_root, pid).is_ok_and(|now| now == ticks));
        if !same_sentry() {
            return;
        }
        let swapped = cgroup_swap_bytes(pid, proc_root, &self.config.cgroup_root);
        if !same_sentry() || swapped.is_none_or(|swapped| swapped < PREFETCH_MIN_SWAP_BYTES) {
            return;
        }
        let (started, cancel) = (Instant::now(), Arc::new(AtomicBool::new(false)));
        let key = (sandbox.sandbox_id.clone(), sandbox.generation);
        self.prefetches.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).insert(key.clone(), cancel.clone());
        let outcome = self.read_back(sandbox, &cancel, started);
        {
            let mut prefetches = self.prefetches.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if prefetches.get(&key).is_some_and(|current| Arc::ptr_eq(current, &cancel)) {
                prefetches.remove(&key);
            }
        }
        match outcome {
            Ok(Some(read)) => self.stats.add(&[
                (Counter::ThawPrefetches, 1.0),
                (Counter::ThawPrefetchedBytes, read as f64),
                (Counter::ThawPrefetchMsTotal, started.elapsed().as_secs_f64() * 1000.0),
            ]),
            Ok(None) => {}
            Err(error) => eprintln!("ucloud-noded: thaw prefetch of {} failed: {error}", sandbox.sandbox_id),
        }
    }

    /// The bytes read back, or `None` for a memory file that is not ours alone.
    fn read_back(&self, sandbox: &Sandbox, cancel: &AtomicBool, started: Instant) -> Result<Option<u64>, WardenError> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(self.active_memory_file(sandbox)?)?;
        let meta = file.metadata()?;
        if !meta.is_file() || meta.uid() != euid() || meta.mode() & 0o077 != 0 {
            return Ok(None); // Only ever a privately owned memory file.
        }
        let pieces = memory_pieces(&file, prefetch::PREFETCH_MAX_BYTES, prefetch::PREFETCH_PIECE_BYTES)?;
        let cancelled = prefetch::deadline(cancel, started, self.config.prefetch_seconds);
        let read = prefetch::prefetch_with(&file, &pieces, &self.slots, &cancelled, prefetch::PREFETCH_THREADS, &*self.read_at, &|_| true)?;
        Ok(Some(read))
    }
}
