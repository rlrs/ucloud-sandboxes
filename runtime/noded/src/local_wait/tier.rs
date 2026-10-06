//! Local waits through the node's pause tier and the exec fence
//! (`node_runtime.py` `pause_model_wait`, `_thaw_model_wait`,
//! `_local_wait_candidates`; phase-3 spec §6.2).
//!
//! - A pause takes T then A exclusively, both non-blocking, and skips when
//!   either is busy; then the warden flock; then rechecks the registration,
//!   the marker, the journal (RUNNING, live authority, its sentry) and the
//!   call, and pauses under that one hold.
//! - A thaw takes T then A shared, both non-blocking, and releases T (an
//!   exec's fence); then the warden flock and, inside the tier, the marker
//!   flock. A transition in the way, or a runtime that is not RUNNING and
//!   live, goes to the agent's wake route instead.

use std::collections::HashMap;
use std::io;
use std::net::Ipv4Addr;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value};

use super::packet::Ipv4Net;
use super::scheduler::{Actions, BoxFuture, CandidateSource, LocalWaitEvents, Outcome, Recheck, WaitCandidate, request_id};
use crate::exec_fence::{ExecFence, Fenced};
use crate::pause::PauseTier;
use crate::registry::{Phase, Registration, Registry};
use crate::warden::Sandbox;

/// Open and flock without blocking; `None` when busy. Retries while the lock
/// landed on an inode the path no longer names (delete unlinks A, then T).
#[cfg(test)]
pub(crate) fn try_lock(path: &Path, operation: libc::c_int) -> io::Result<Option<std::fs::File>> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    loop {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)?;
        // SAFETY: a valid descriptor.
        if unsafe { libc::flock(file.as_raw_fd(), operation | libc::LOCK_NB) } != 0 {
            let error = io::Error::last_os_error();
            return match error.raw_os_error() {
                Some(libc::EWOULDBLOCK) => Ok(None),
                _ => Err(error),
            };
        }
        let held = file.metadata()?;
        match std::fs::symlink_metadata(path) {
            Ok(current) if (current.dev(), current.ino()) == (held.dev(), held.ino()) => return Ok(Some(file)),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
}

/// A pause's hold: T and A exclusively. Released on drop (A first).
pub type Exclusive = crate::pause_policy::fence::ExclusiveHold;

/// The phase-2a exec fence's files (`<runtime_root>/warden-locks/.<id>.transition`
/// and `.<id>.activity`), as a pauser takes them.
#[derive(Clone, Debug)]
pub struct WaitFence {
    pause: crate::pause_policy::fence::PauseFence,
    exec: ExecFence,
}

impl WaitFence {
    /// `directory`: the warden lock directory (`ExecConfig::warden_locks_dir`).
    pub fn new(directory: impl Into<PathBuf>) -> WaitFence {
        let directory = directory.into();
        WaitFence { exec: ExecFence::new(&directory), pause: crate::pause_policy::fence::PauseFence::new(directory) }
    }

    /// T `LOCK_EX|LOCK_NB`, then A `LOCK_EX|LOCK_NB`; `None` when either is
    /// busy (Python's `SandboxBusyError`). The pause policy's fence: A is
    /// never created by a pause, since its mtime is the activity clock.
    pub fn try_exclusive(&self, sandbox_id: &str) -> io::Result<Option<Exclusive>> {
        self.pause.try_exclusive(sandbox_id)
    }

    /// An exec's fence: T shared then A shared, non-blocking, T released.
    pub fn try_shared(&self, sandbox_id: &str) -> io::Result<Fenced> {
        self.exec.acquire(sandbox_id)
    }
}

/// The journal says RUNNING with live authority and its sentry is the
/// process it names (pid and start ticks).
fn live_running(record: &Map<String, Value>, proc_root: &Path) -> bool {
    let field = |name: &str| record.get(name);
    if field("state").and_then(Value::as_str) != Some("running") || field("authority").and_then(Value::as_str) != Some("live") {
        return false;
    }
    let pid = field("sentry_pid").and_then(Value::as_u64).and_then(|pid| u32::try_from(pid).ok());
    let (Some(pid), Some(ticks)) = (pid, field("sentry_start_time_ticks").and_then(Value::as_u64)) else { return false };
    crate::runsc::start_time_ticks(proc_root, pid).is_ok_and(|now| now == ticks)
}

/// Pause and thaw through the pause tier, under the fence.
pub(crate) struct TierActions {
    pub tier: PauseTier,
    pub fence: WaitFence,
    pub candidates: Arc<dyn CandidateSource>,
    pub events: Arc<dyn LocalWaitEvents>,
}

impl TierActions {
    async fn pause_fenced(&self, wait: WaitCandidate, still_waiting: Recheck) -> Outcome {
        let sandbox = &wait.sandbox;
        let (id, generation) = (sandbox.sandbox_id.as_str(), sandbox.generation);
        if !self.tier.config().enabled {
            return Outcome::Failed("the pause tier is disabled on this node".into());
        }
        let _fence = match self.fence.try_exclusive(id) {
            Ok(Some(held)) => held,
            Ok(None) => return Outcome::Busy,
            Err(error) => return Outcome::Failed(format!("exec fence: {error}")),
        };
        if self.tier.is_paused(id, generation) || !self.candidates.current(id, generation) {
            return Outcome::Done;
        }
        // Never wait for the warden flock while holding T and A exclusively: a
        // Python download or commit export holds it for a guest command, and
        // every operation on this sandbox would queue behind the pause. The
        // 10 ms tick tries again.
        let lock = match self.tier.try_lock(sandbox).await {
            Ok(Some(lock)) => lock,
            Ok(None) => return Outcome::Busy,
            Err(error) => return Outcome::Failed(error.to_string()),
        };
        if self.tier.is_paused(id, generation) {
            return Outcome::Done;
        }
        let record = match self.tier.journal_record(sandbox).await {
            Ok(Some(record)) => record,
            Ok(None) => return Outcome::Done,
            Err(error) => return Outcome::Failed(error.to_string()),
        };
        // A dead sentry or a half transition is the agent's to reconcile.
        if !live_running(&record, &self.tier.config().warden.proc_root) || !still_waiting() {
            return Outcome::Done;
        }
        match self.tier.pause_locked(sandbox, &lock, &record).await {
            Ok(()) => Outcome::Acted,
            Err(error) => Outcome::Failed(error.to_string()),
        }
    }

    async fn thaw_fenced(&self, wait: WaitCandidate) -> Outcome {
        let sandbox = &wait.sandbox;
        let (id, generation) = (sandbox.sandbox_id.as_str(), sandbox.generation);
        if !self.tier.is_paused(id, generation) {
            return Outcome::Done;
        }
        let lease = match self.fence.try_shared(id) {
            Ok(Fenced::Held(lease)) => lease,
            Ok(Fenced::Busy) => return self.delegate(sandbox).await,
            Err(error) => return Outcome::Failed(format!("exec fence: {error}")),
        };
        let lock = match self.tier.lock(sandbox).await {
            Ok(lock) => lock,
            Err(error) => return Outcome::Failed(error.to_string()),
        };
        if !self.tier.is_paused(id, generation) {
            return Outcome::Done;
        }
        let live = match self.tier.journal_record(sandbox).await {
            Ok(record) => record.is_some_and(|record| live_running(&record, &self.tier.config().warden.proc_root)),
            Err(error) => return Outcome::Failed(error.to_string()),
        };
        if !live {
            drop((lock, lease));
            return self.delegate(sandbox).await;
        }
        match self.tier.thaw_locked(sandbox, &lock, true).await {
            Ok(Some(_)) => {
                lease.touch(); // Python's wake marks activity.
                Outcome::Acted
            }
            Ok(None) => Outcome::Done,
            Err(error) => Outcome::Failed(error.to_string()),
        }
    }

    async fn delegate(&self, sandbox: &Sandbox) -> Outcome {
        let operation_id = request_id("local-wake");
        match self.events.wake(sandbox.sandbox_id.clone(), sandbox.generation, operation_id).await {
            Ok(()) => Outcome::Delegated,
            Err(error) => Outcome::Failed(format!("agent wake: {error}")),
        }
    }
}

impl Actions for Arc<TierActions> {
    fn is_paused(&self, sandbox_id: &str, generation: u64) -> bool {
        self.tier.is_paused(sandbox_id, generation)
    }

    fn pause(&self, wait: WaitCandidate, still_waiting: Recheck) -> BoxFuture<Outcome> {
        let me = self.clone();
        Box::pin(async move { me.pause_fenced(wait, still_waiting).await })
    }

    fn thaw(&self, wait: WaitCandidate) -> BoxFuture<Outcome> {
        let me = self.clone();
        Box::pin(async move { me.thaw_fenced(wait).await })
    }
}

/// Guest addresses by incarnation from the direct network state file
/// (`network-slots.json`: `{"leases": {"<id>\0<generation>": slot}}`).
/// The file is replaced atomically, so it is read without the state lock.
pub fn read_leases(path: &Path, network: Ipv4Net) -> Result<HashMap<(String, u64), Ipv4Addr>, String> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(error) => return Err(format!("direct network state: {error}")),
    };
    let invalid = || "direct network state is invalid".to_string();
    let state: Value = serde_json::from_str(&text).map_err(|_| invalid())?;
    let leases = state.get("leases").and_then(Value::as_object).ok_or_else(invalid)?;
    let base = u32::from(network.address());
    let mut result = HashMap::new();
    for (key, slot) in leases {
        let (sandbox_id, generation) = key.rsplit_once('\0').ok_or_else(invalid)?;
        let generation: u64 = generation.parse().map_err(|_| invalid())?;
        let slot = slot.as_u64().filter(|slot| (1..=32_767).contains(slot)).ok_or_else(invalid)? as u32;
        result.insert((sandbox_id.to_string(), generation), Ipv4Addr::from(base + slot * 2 + 1));
    }
    Ok(result)
}

/// `/sys/fs/cgroup/<linux.cgroupsPath>/cpu.stat` from the bundle's config.json.
pub fn cpu_stat_path(bundle: &Path, cgroup_root: &Path) -> Option<PathBuf> {
    let config: Value = serde_json::from_slice(&std::fs::read(bundle.join("config.json")).ok()?).ok()?;
    let relative = Path::new(config.get("linux")?.get("cgroupsPath")?.as_str()?.trim_matches('/'));
    if relative.as_os_str().is_empty() || !relative.components().all(|part| matches!(part, Component::Normal(_))) {
        return None;
    }
    Some(cgroup_root.join(relative).join("cpu.stat"))
}

fn sandbox_of(registration: &Registration) -> Sandbox {
    Sandbox {
        sandbox_id: registration.sandbox_id().to_string(),
        generation: registration.sandbox_generation as u64,
        container_id: registration.container_id.clone(),
        bundle: PathBuf::from(&registration.bundle),
        memory_directory: registration.memory_directory.clone(),
        spec_sha256: registration.spec_sha256(),
    }
}

/// Python `_local_wait_candidates`: owned parkable managed sandboxes with a
/// network lease (relay agents), from the daemon's registry index.
pub struct RegistryCandidates {
    registry: Arc<Registry>,
    network_state: PathBuf,
    network: Ipv4Net,
    cgroup_root: PathBuf,
    paths: Mutex<HashMap<(String, u64), PathBuf>>,
}

impl RegistryCandidates {
    /// `network_state`: `<state_root>/network-slots.json`.
    pub fn new(registry: Arc<Registry>, network_state: PathBuf, network: Ipv4Net, cgroup_root: PathBuf) -> RegistryCandidates {
        RegistryCandidates { registry, network_state, network, cgroup_root, paths: Mutex::new(HashMap::new()) }
    }
}

fn managed_process(registration: &Registration) -> bool {
    registration.spec.to_dict().get("managed_process").and_then(Value::as_bool).unwrap_or(false)
}

impl CandidateSource for RegistryCandidates {
    fn candidates(&self) -> Result<Vec<WaitCandidate>, String> {
        let leases = read_leases(&self.network_state, self.network)?;
        let snapshot = self.registry.snapshot().map_err(|error| error.to_string())?;
        let cached = std::mem::take(&mut *self.paths.lock().unwrap_or_else(|poisoned| poisoned.into_inner()));
        let mut paths = HashMap::new();
        let mut candidates = Vec::new();
        for item in &snapshot.records {
            if item.phase != Phase::Owned || !item.spec.parkable() || !managed_process(item) {
                continue;
            }
            let key = (item.sandbox_id().to_string(), item.sandbox_generation as u64);
            let Some(path) = cached.get(&key).cloned().or_else(|| cpu_stat_path(Path::new(&item.bundle), &self.cgroup_root)) else {
                continue;
            };
            if let Some(guest) = leases.get(&key) {
                paths.insert(key, path.clone());
                candidates.push(WaitCandidate { sandbox: sandbox_of(item), guest: *guest, cpu_stat: path });
            }
        }
        *self.paths.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = paths;
        Ok(candidates)
    }

    fn current(&self, sandbox_id: &str, generation: u64) -> bool {
        matches!(self.registry.get(sandbox_id), Ok(Some(registration))
            if registration.phase == Phase::Owned && registration.sandbox_generation as u64 == generation)
    }
}
